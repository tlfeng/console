package topn

import (
	"reflect"
	"testing"

	"infini.sh/framework/modules/elastic/common"
)

func TestMetricModes(t *testing.T) {
	for _, tc := range []struct {
		key, second string
		derivative  bool
		mode        metricMode
		reverse     bool
	}{
		{key: "cpu", mode: peak},
		{key: "indexing_bytes", derivative: true, mode: rate},
		{key: "jvm_young_gc_latency", derivative: true, mode: rate},
		{key: "query_latency", second: "count", derivative: true, mode: latency},
		{key: "os_used_swap", second: "total", mode: ratio},
		{key: "disk", second: "available", mode: ratio, reverse: true},
	} {
		metric, ok := NewMetric(&common.MetricItem{Key: tc.key}, "value", tc.second, "", "", tc.derivative)
		if !ok || metric.mode != tc.mode || metric.reverseRatio != tc.reverse {
			t.Errorf("%s: %+v, %v", tc.key, metric, ok)
		}
	}
}

func TestScoreSeries(t *testing.T) {
	q := Query{Source: "index_stats", Min: 1001, Max: 4999, Width: 1, N: 1}
	mk := func(ts int64, value float64) samples {
		return samples{Time: &ts, Value: &scalar{Value: &value}}
	}
	row, err := scoreSeries(q, Metric{}, "entity", []samples{mk(1000, 0), mk(3000, 4)}, false)
	want := [][]interface{}{{int64(1000), 0.0}, {int64(2000), nil}, {int64(3000), 4.0}}
	if err != nil || row.score != 4 || !reflect.DeepEqual(row.points, want) {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	const identity = `,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]}}`
	points := []samples{
		parseSample(t, `{"key":0,"value":{"value":60}`+identity),
		parseSample(t, `{"key":1000,"value":{"value":30}`+identity),
		parseSample(t, `{"key":2000,"value":{"value":40}`+identity),
	}
	row, err = scoreSeries(q, Metric{mode: rate}, "index", points, false)
	if err != nil || row.score != 10 || !reflect.DeepEqual(row.points, [][]interface{}{{int64(2000), 10.0}}) {
		t.Fatalf("row=%+v err=%v", row, err)
	}
}

func TestRatioAggregation(t *testing.T) {
	q := Query{Min: 1000, Max: 1999, Width: 1}
	metric := Metric{mode: ratio, Field: "total", Field2: "available", reverseRatio: true}
	aggs := metricAggs(q, metric)
	script := aggs["value"].(dsl)["max"].(dsl)["script"].(dsl)
	if !reflect.DeepEqual(script["params"], dsl{"first": "available", "second": "total"}) {
		t.Fatalf("disk must use available / total: %v", script)
	}
	for _, tc := range []struct {
		name, raw string
		score     float64
	}{
		{"dense bucket", `{"key":1000,"doc_count":360,"value":{"value":90}}`, 90},
		{"invalid observations", `{"key":1000,"doc_count":360,"value":{"value":-1}}`, -1},
		{"measured zero", `{"key":1000,"doc_count":360,"value":{"value":0}}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row, err := scoreSeries(q, metric, "node", []samples{parseSample(t, tc.raw)}, false)
			if err != nil || row.score != tc.score || (len(row.points) != 0) != (tc.score >= 0) {
				t.Fatalf("row=%+v err=%v", row, err)
			}
		})
	}
	if _, err := scoreSeries(q, metric, "node", []samples{parseSample(t, `{"key":1000,"doc_count":360}`)}, false); err == nil {
		t.Fatal("missing ratio result accepted")
	}
}

func TestScoreSeriesApproximatesRollupLatency(t *testing.T) {
	q := Query{Source: "index_stats", Min: 1001, Max: 3999, Width: 1, N: 1}
	metric := Metric{mode: latency, Field: "elapsed", Field2: "ops"}
	const identity = `,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]}`
	points := []samples{
		parseSample(t, `{"key":1000,"first":{"value":100},"second":{"value":10}`+identity+`}`),
		parseSample(t, `{"key":2000,"first":{"value":160},"second":{"value":12}`+identity+`}`),
		parseSample(t, `{"key":3000,"first":{"value":180},"second":{"value":13}`+identity+`}`),
	}
	// The first visible point needs the pre-read predecessor; from then on the
	// maxima differences render 30 and 20 ms per operation.
	row, err := scoreSeries(q, metric, "index", points, true)
	want := [][]interface{}{{int64(2000), 30.0}, {int64(3000), 20.0}}
	if err != nil || row.score != 30 || !reflect.DeepEqual(row.points, want) {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	if _, err := scoreSeries(q, metric, "index", points, false); err == nil {
		t.Fatal("raw page must still reject a missing paired observation")
	}
}
