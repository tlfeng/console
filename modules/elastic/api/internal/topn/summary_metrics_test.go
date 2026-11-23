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
	q := Query{Source: "index_stats", Cluster: "biz", AllIndices: true, Width: 1, Min: 2000, Max: 3999, N: 5}
	type series struct {
		counter [][2]int // (write, query) counter per bucket
		time    [][2]int // (write, query) time per bucket
	}
	rows := series{
		counter: [][2]int{{10, 100}, {99, 105}, {99, 106}},
		time:    [][2]int{{200, 500}, {210, 530}, {210, 531}},
	}
	s := snapshot{id: "ok", shards: 1, call: func(_ context.Context, _, _ string, body dsl) ([]byte, error) {
		aggs := body["aggs"].(dsl)["entities"].(dsl)["aggs"].(dsl)["dates"].(dsl)["aggs"].(dsl)
		var points []dsl
		if value, ok := aggs["value"]; ok {
			field := value.(dsl)["max"].(dsl)["field"].(string)
			column := 0
			if field == queryCounterField {
				column = 1
			}
			for i, ts := range []int{1000, 2000, 3000} {
				points = append(points, dsl{"key": ts, "doc_count": 1,
					"value":    dsl{"value": rows.counter[i][column]},
					"identity": dsl{"buckets": []dsl{{"key": "id", "doc_count": 1}}}})
			}
		} else {
			fields := aggs["pair"].(dsl)["top_hits"].(dsl)["docvalue_fields"].([]string)
			firstTime := fields[0] == writeTimeField
			for i, ts := range []int{1000, 2000, 3000} {
				counter, when := rows.counter[i][0], rows.time[i][0]
				if !firstTime {
					counter, when = rows.counter[i][1], rows.time[i][1]
				}
				points = append(points, dsl{"key": ts, "doc_count": 1,
					"pair":     latencyHit(dsl{fields[0]: []int{when}, fields[1]: []int{counter}}),
					"identity": dsl{"buckets": []dsl{{"key": "id", "doc_count": 1}}}})
			}
		}
		return json.Marshal(dsl{"timed_out": false, "_shards": dsl{"total": 1, "successful": 1, "failed": 0}, "aggregations": dsl{"entities": dsl{"buckets": []dsl{{"key": dsl{"entity": "x"}, "dates": dsl{"buckets": points}}}}}})
	}}
	for _, tc := range []struct {
		key  string
		want [][]float64
	}{
		{writeRateKey, [][]float64{{89}, {0}}},
		{queryRateKey, [][]float64{{5}, {1}}},
		{writeLatencyKey, [][]float64{{5}, {}}},
		{queryLatencyKey, [][]float64{{6}, {1}}},
	} {
		metric := summaryMetric(tc.key)
		q.Width = 10800
		q.Max = q.Min + 15*86400000
		if !useSummary(q, metric) {
			t.Fatalf("missing route for %s", tc.key)
		}
		metric.Field += ".wrong"
		if useSummary(q, metric) {
			t.Fatalf("wrong source field accepted for %s", tc.key)
		}
		metric = summaryMetric(tc.key)
		q.Width = 1
		q.Max = 3999
		got, err := readTransitions(context.Background(), &s, q, metric, []int64{2000, 3000})
		if err != nil {
			t.Fatalf("%s: %v", tc.key, err)
		}
		for j, w := range tc.want {
			if !reflect.DeepEqual(got[j].Scores, w) {
				t.Fatalf("%s at %d got %v want %v", tc.key, j, got[j].Scores, w)
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
	// Omitted metrics cover all four charts; omitted clusters cover every cluster.
	c := summaryConfig{Enabled: true}
	for _, key := range []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey} {
		if !c.enabled("biz", key) || !c.writes() {
			t.Fatalf("default config must cover and write %s", key)
		}
	}
	if !reflect.DeepEqual(c.metricKeys(), []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey}) {
		t.Fatalf("unexpected default metric keys: %v", c.metricKeys())
	}
	c.Clusters = []string{"other"}
	if c.enabled("biz", writeRateKey) {
		t.Fatal("cluster allowlist must exclude other clusters")
	}
	// Metrics selects the write set; ReadMetrics can narrow the read set below it.
	c = summaryConfig{Enabled: true, Metrics: []string{writeRateKey, queryRateKey}, ReadMetrics: []string{writeRateKey}}
	if !c.enabled("biz", writeRateKey) || c.enabled("biz", queryRateKey) {
		t.Fatal("read metrics must follow ReadMetrics")
	}
	if !reflect.DeepEqual(c.metricKeys(), c.Metrics) {
		t.Fatalf("metrics must select the write set: %v", c.metricKeys())
	}
	yes := true
	if !(summaryConfig{ReadOverride: &yes}).reads() {
		t.Fatal("read override must force reads on")
	}
}
