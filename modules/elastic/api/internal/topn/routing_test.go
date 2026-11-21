package topn

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/modules/elastic/common"
)

func TestRoutingHintExpiryAndCapacity(t *testing.T) {
	h := routingHints{expires: make(map[routingKey]time.Time)}
	now := time.Unix(0, 0)
	for i := 0; i < pitHintCapacity; i++ {
		h.remember(routingKey{width: i}, now)
	}
	extra := routingKey{width: pitHintCapacity}
	h.remember(extra, now)
	if h.contains(extra, now) || len(h.expires) != pitHintCapacity {
		t.Fatal("hint limit exceeded")
	}
	if !h.contains(routingKey{}, now.Add(pitHintTTL-time.Nanosecond)) || h.contains(routingKey{}, now.Add(pitHintTTL)) {
		t.Fatal("invalid expiry")
	}
	h.remember(extra, now.Add(pitHintTTL))
	if !h.contains(extra, now.Add(pitHintTTL)) || len(h.expires) != 1 {
		t.Fatal("expired entries not pruned")
	}
}

func TestRoutingHintLearnsOnlyFromCandidateLimit(t *testing.T) {
	q := Query{Source: "node_stats", Cluster: t.Name(), storeScope: "monitor", Min: 1000, Max: 1999, Width: 1, N: 5}
	metric := Metric{Item: &common.MetricItem{Key: "cpu"}, Field: "cpu"}
	api := pitFor(elastic.Version{Distribution: elastic.Elasticsearch, Number: "8.16.0"})
	key, _ := nodeRoutingKey(q, metric, api, "raw", false)
	defer pitHints.forget(key)
	var calls []string
	count := 194 // Oversized response, not a valid candidate sentinel.
	failOpen := false
	call := func(_ context.Context, method, path string, _ dsl) ([]byte, error) {
		switch {
		case strings.HasPrefix(path, "/raw/_pit?"):
			calls = append(calls, "open")
			if failOpen {
				return nil, fmt.Errorf("cannot open PIT")
			}
			return []byte(`{"id":"fixed",` + allShards + `}`), nil
		case method == "DELETE":
			calls = append(calls, "close")
			return []byte(`{}`), nil
		case strings.HasPrefix(path, "/_search?"):
			calls = append(calls, "search")
			return result(`{"buckets":[]}`), nil
		default:
			calls = append(calls, "single")
			var rows []string
			for i := 0; i < count; i++ {
				rows = append(rows, fmt.Sprintf(`{"key":"node-%03d","dates":{"buckets":[]}}`, i))
			}
			return []byte(`{` + allShards + `,"timed_out":false,"aggregations":{"scan":{"entities":{"buckets":[` + strings.Join(rows, ",") + `]}}}}`), nil
		}
	}
	run := func(query Query, pit pitAPI, want ...string) {
		t.Helper()
		calls = nil
		pred, _ := query.filter()
		if err := runQuery(context.Background(), call, pit, "raw", pred, query, metric, false); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(calls, want) {
			t.Fatalf("got %v, want %v", calls, want)
		}
	}
	run(q, api, "single", "open", "search", "close")
	if pitHints.contains(key, time.Now()) {
		t.Fatal("invalid response trained a hint")
	}
	count = 193
	run(q, api, "single", "open", "search", "close")
	sliding := q
	sliding.Min += 1000
	sliding.Max += 1000
	run(sliding, api, "open", "search", "close")
	count = 0
	other := q
	other.storeScope = "different monitor"
	run(other, api, "single")
	run(q, pitAPI{}, "single")
	failOpen = true
	run(q, api, "open", "single")
	if pitHints.contains(key, time.Now()) {
		t.Fatal("failed PIT did not evict hint")
	}
}
