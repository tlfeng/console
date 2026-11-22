package topn

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	cfg "infini.sh/framework/core/config"
)

func TestOnDemandConfig(t *testing.T) {
	raw := []byte("enabled: true\nwriter: false\n")
	input, err := cfg.NewConfigWithYAML(raw, "test")
	if err != nil {
		t.Fatal(err)
	}
	var c materializedConfig
	if err := input.Unpack(&c); err != nil {
		t.Fatal(err)
	}
	if c.writes() || !c.enabled("biz", queryLatencyKey) || len(c.metricKeys()) != 4 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	m, err := c.store("raw")
	if err != nil || m.index != ".console-topn-index-rate-v2" {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestOnDemandQueue(t *testing.T) {
	jobs := summaryQueue{jobs: make(map[string]*summaryJob), running: make(chan struct{}, 1)}
	m := materializedMetric(writeRateKey)
	store := materializedStore{raw: "raw", index: ".console-topn-test"}
	q := Query{Source: "index_stats", Cluster: "biz", Min: 1000, Max: 2000, Width: 1, N: 1, Indices: []string{"b", "a"}}
	key := summaryJobKey(store, q, m)
	q.Indices = []string{"a", "b"}
	if key != summaryJobKey(store, q, m) {
		t.Fatal("different ACL ordering must not change the key")
	}
	q.Indices = []string{"b"}
	if key == summaryJobKey(store, q, m) {
		t.Fatal("narrowed ACL must change the key")
	}
	release := make(chan struct{})
	work := func(ctx context.Context, metric Metric) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer close(release)
	for i := range summaryJobLimit {
		k := key + string(rune('a'+i))
		if err := jobs.submit(k, m, work); !errors.Is(err, errSummaryPending) {
			t.Fatal(err)
		}
		if found, err := jobs.lookup(k, m); !found || !errors.Is(err, errSummaryPending) {
			t.Fatal(found, err)
		}
		if err := jobs.submit(k, m, work); !errors.Is(err, errSummaryPending) {
			t.Fatal(err)
		}
	}
	if err := jobs.submit("overflow", m, work); !errors.Is(err, errSummaryBusy) {
		t.Fatal(err)
	}
}

func TestSummaryWithoutRawPolicyDoesNotCreateIndex(t *testing.T) {
	m := materializedStore{index: ".console-topn-test", raw: "raw"}
	call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
		if method != http.MethodGet {
			t.Fatalf("unexpected mutation: %s %s", method, path)
		}
		if path == "/"+m.index+"/_mapping" {
			return nil, monitoringHTTPError(404)
		}
		return []byte(`{"raw":{"settings":{"index.uuid":"original"}}}`), nil
	}
	if err := ensureSummaryIndex(context.Background(), call, m, pitAPI{}); !errors.Is(err, errSummaryNoPolicy) {
		t.Fatal(err)
	}
}

func TestOnDemandEmptyDoesNotPublish(t *testing.T) {
	q := Query{Source: "index_stats", Cluster: "biz", Min: 1000, Max: 2999, Width: 1, N: 1, AllIndices: true}
	m := materializedStore{raw: "raw", index: ".console-topn-test"}
	calls := 0
	call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
		if method == http.MethodGet {
			return []byte(`{"raw":{"settings":{"index.uuid":"uuid"}}}`), nil
		}
		if method == http.MethodPut {
			t.Fatal("empty bucket must not be persisted")
		}
		if method == http.MethodDelete {
			return []byte(`{"pits":[{"pit_id":"p","successful":true}]}`), nil
		}
		if body == nil {
			return []byte(`{"pit_id":"p","_shards":{"total":1,"successful":1,"failed":0}}`), nil
		}
		calls++
		return []byte(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"aggregations":{"entities":{"buckets":[]}}}`), nil
	}
	metric := materializedMetric(writeRateKey)
	if err := computeMissingSummaries(context.Background(), call, pitAPI{path: "/_pit", idField: "pit_id"}, m, q, metric, time.Unix(2000, 0), true); err != nil || len(metric.Item.Lines) != 0 || calls != 1 {
		t.Fatal(err, calls)
	}
}
