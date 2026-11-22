package topn

import (
	"errors"
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

func TestObservedRatio(t *testing.T) {
	metric := Metric{mode: ratio, Field: "total", Field2: "available", reverseRatio: true}
	for _, tc := range []struct {
		name, raw string
		want      float64
		valid     bool
		err       error
	}{
		{"paired maxima", `{"doc_count":2,"ratio_samples":{"hits":{"hits":[{"fields":{"total":[100],"available":[90]}},{"fields":{"total":[200],"available":[100]}}]}}}`, 90, true, nil},
		{"missing or zero divisor", `{"doc_count":3,"ratio_samples":{"hits":{"hits":[{"fields":{"total":[100]}},{"fields":{"available":[90]}},{"fields":{"total":[0],"available":[90]}}]}}}`, -1, false, nil},
		{"zero available", `{"doc_count":1,"ratio_samples":{"hits":{"hits":[{"fields":{"total":[100],"available":[0]}}]}}}`, 0, true, nil},
		{"over capacity", `{"doc_count":101}`, 0, false, errBudget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, valid, err := observedRatio(parseSample(t, tc.raw), metric)
			if !errors.Is(err, tc.err) || valid != tc.valid || got != tc.want {
				t.Fatalf("got %v %v %v", got, valid, err)
			}
		})
	}
	if _, _, err := observedRatio(parseSample(t, `{"doc_count":2,"ratio_samples":{"hits":{"hits":[]}}}`), metric); err == nil {
		t.Fatal("truncated observations accepted")
	}
}
