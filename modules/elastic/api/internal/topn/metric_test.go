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
		{key: "indexing_bytes", derivative: true, mode: storageGrowth},
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
	mk := func(ts int64, value, other *float64) samples {
		return samples{Time: &ts, Value: &scalar{Value: value}, Other: &scalar{Value: other}}
	}
	val := func(v float64) *float64 { return &v }
	for _, tc := range []struct {
		name    string
		metric  Metric
		samples []samples
		want    [][]interface{}
		score   float64
	}{
		{"peak gap", Metric{}, []samples{mk(1000, val(0), nil), mk(3000, val(4), nil)}, [][]interface{}{{int64(1000), 0.0}, {int64(2000), nil}, {int64(3000), 4.0}}, 4},
		{"disk available", Metric{mode: ratio, reverseRatio: true}, []samples{mk(1000, val(100), val(0)), mk(2000, val(0), val(1)), mk(3000, val(100), nil), mk(4000, val(200), val(100))}, [][]interface{}{{int64(1000), 0.0}, {int64(2000), nil}, {int64(4000), 50.0}}, 50},
		{"ratio missing", Metric{mode: ratio}, []samples{mk(1000, nil, val(200)), mk(4000, val(1), val(0))}, nil, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row, err := scoreSeries(q, tc.metric, "entity", tc.samples)
			if err != nil || row.score != tc.score || !reflect.DeepEqual(row.points, tc.want) {
				t.Fatalf("row=%+v err=%v", row, err)
			}
		})
	}
	const identity = `,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]}}`
	points := []samples{
		parseSample(t, `{"key":0,"value":{"value":60}`+identity),
		parseSample(t, `{"key":1000,"value":{"value":30}`+identity),
		parseSample(t, `{"key":2000,"value":{"value":40}`+identity),
	}
	row, err := scoreSeries(q, Metric{mode: storageGrowth}, "index", points)
	if err != nil || row.score != 10 || !reflect.DeepEqual(row.points, [][]interface{}{{int64(2000), 10.0}}) {
		t.Fatalf("row=%+v err=%v", row, err)
	}
}
