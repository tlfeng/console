package topn

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestSharedTopNFieldsAndScores(t *testing.T) {
	keys := []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey}
	q := Query{Source: "index_stats", Cluster: "biz", AllIndices: true, Width: 1, Min: 2000, Max: 3999, N: 5}
	var batches []transitionBatch
	for _, key := range keys {
		m := summaryMetric(key)
		route := q
		route.Width = 10800
		route.Max = route.Min + 15*86400000
		if !useSummary(route, m) {
			t.Fatalf("missing route for %s", key)
		}
		m.Field += ".wrong"
		if useSummary(route, m) {
			t.Fatalf("wrong source field accepted for %s", key)
		}
		batches = append(batches, transitionBatch{summaryMetric(key), []int64{2000, 3000}})
	}
	calls := 0
	s := snapshot{id: "ok", shards: 1, call: func(_ context.Context, _, _ string, body dsl) ([]byte, error) {
		calls++
		aggs := body["aggs"].(dsl)["entities"].(dsl)["aggs"].(dsl)["dates"].(dsl)["aggs"].(dsl)
		if len(aggs) != 7 {
			t.Fatalf("want two maxima, two paired hits and three evidence aggregations, got %v", aggs)
		}
		for name, want := range map[string]dsl{
			"value": maximum(writeCounterField), "other": maximum(queryCounterField),
			"pair": latencyPair(summaryMetric(writeLatencyKey)), "second_pair": latencyPair(summaryMetric(queryLatencyKey)),
		} {
			if !reflect.DeepEqual(aggs[name], want) {
				t.Fatalf("wrong %s: %v", name, aggs[name])
			}
		}
		var points []dsl
		for i, counters := range [][6]int{{10, 100, 10, 200, 100, 500}, {99, 105, 12, 210, 105, 530}, {99, 106, 12, 210, 106, 531}} {
			p := dsl{"key": (i + 1) * 1000, "doc_count": 1, "identity": dsl{"buckets": []dsl{{"key": "id", "doc_count": 1}}},
				"value": dsl{"value": counters[0]}, "other": dsl{"value": counters[1]},
				"pair":        latencyHit(dsl{writeCounterField: []int{counters[2]}, writeTimeField: []int{counters[3]}}),
				"second_pair": latencyHit(dsl{queryCounterField: []int{counters[4]}, queryTimeField: []int{counters[5]}}),
			}
			points = append(points, p)
		}
		return json.Marshal(dsl{"timed_out": false, "_shards": dsl{"total": 1, "successful": 1, "failed": 0}, "aggregations": dsl{"entities": dsl{"buckets": []dsl{{"key": dsl{"entity": "x"}, "dates": dsl{"buckets": points}}}}}})
	}}
	got, err := readTransitionBatches(context.Background(), &s, q, batches)
	if err != nil || calls != 1 {
		t.Fatalf("err=%v searches=%d", err, calls)
	}
	for i, want := range [][][]float64{{{89}, {0}}, {{5}, {1}}, {{5}, {}}, {{6}, {1}}} {
		for j, w := range want {
			if !reflect.DeepEqual(got[i][j].Scores, w) {
				t.Fatalf("%s at %d got %v want %v", keys[i], j, got[i][j].Scores, w)
			}
		}
	}
}

func TestTopNSummaryMetricCoverage(t *testing.T) {
	m := summaryStore{raw: "raw", index: ".console-topn-test"}
	q := Query{Cluster: "biz", Width: 10800}
	writeID := m.metricID(q, writeRateKey, 2000)
	// Fixed output of the original version-1 indexing-rate ID recipe.
	if writeID != "51dda89026ae254ee06cce4c587e443d37a464e074105ba78df3b2bc0f98a06f" {
		t.Fatalf("write-rate ID changed: %s", writeID)
	}
	for _, key := range []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey} {
		oldID := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("1\x00raw\x00biz\x00%s\x0010800\x002000", key))))
		isLatency := key == writeLatencyKey || key == queryLatencyKey
		if (m.metricID(q, key, 2000) != oldID) != isLatency {
			t.Fatalf("only latency IDs should change: %s", key)
		}
	}
	q.Width = 1
	proof := rawBucketProof{1000: {"i": 1}, 2000: {"i": 1}}
	digest := proof.transition(2000, 1000)
	writeID = m.provenID(q, writeRateKey, 2000, digest)
	queryID := m.provenID(q, queryRateKey, 2000, digest)
	missing, err := missingSummaries(context.Background(), func(_ context.Context, _, _ string, body dsl) ([]byte, error) {
		if !reflect.DeepEqual(body["query"], dsl{"ids": dsl{"values": []string{writeID, queryID}}}) {
			t.Fatalf("wrong ids: %v", body)
		}
		return []byte(fmt.Sprintf(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":%q}]}}`, writeID)), nil
	}, m, q, []string{writeRateKey, queryRateKey}, []int64{2000}, proof)
	if err != nil || !reflect.DeepEqual(missing, map[string][]int64{queryRateKey: {2000}}) {
		t.Fatalf("%v %v", missing, err)
	}
	yes := true
	c := summaryConfig{ReadOverride: &yes, Clusters: []string{"biz"}}
	if !c.enabled("biz", writeRateKey) || c.enabled("biz", queryRateKey) {
		t.Fatal("old config must only read write-rate")
	}
	c.Metrics = []string{writeRateKey, queryRateKey}
	c.ReadMetrics = []string{writeRateKey}
	if c.enabled("biz", queryRateKey) || !reflect.DeepEqual(c.metricKeys(), c.Metrics) {
		t.Fatal("backfill must not enable reads")
	}
}
