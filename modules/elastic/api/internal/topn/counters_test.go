package topn

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"infini.sh/framework/modules/elastic/common"
)

func parseSample(t *testing.T, raw string) samples {
	t.Helper()
	var p samples
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func latencyHit(fields dsl) dsl {
	return dsl{"hits": dsl{"hits": []dsl{{"fields": fields}}}}
}

func TestCounterPoint(t *testing.T) {
	previous := parseSample(t, `{"key":1000,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":40},"pair":{"hits":{"hits":[{"fields":{"elapsed":[40],"ops":[5]}}]}}}`)
	current := parseSample(t, `{"key":2000,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":60},"pair":{"hits":{"hits":[{"fields":{"elapsed":[60],"ops":[7]}}]}}}`)
	paired := Metric{mode: latency, Field: "elapsed", Field2: "ops"}
	rateMetric := Metric{mode: rate}
	if score, ok, err := counterPoint(previous, current, 1000, paired); err != nil || !ok || score != 10 {
		t.Fatalf("got %v, %v, %v", score, ok, err)
	}
	if _, ok, err := counterPoint(previous, current, 2000, rateMetric); ok || err != nil {
		t.Fatal("gap produced a point or error")
	}

	missing := current
	missing.Pair = nil
	if _, _, err := counterPoint(previous, missing, 1000, paired); err == nil {
		t.Fatal("missing paired aggregation accepted")
	}
	fields := current.Pair.Hits.Rows[0].Fields
	for _, ops := range [][]float64{nil, {7, 9}, {5}} {
		fields["ops"] = ops
		if _, ok, err := counterPoint(previous, current, 1000, paired); ok || err != nil {
			t.Fatal("missing, multivalued, or unchanged operations produced a latency point")
		}
	}
	current.Value = &scalar{Value: new(float64)}
	if _, ok, err := counterPoint(previous, current, 1000, rateMetric); ok || err != nil {
		t.Fatal("counter reset produced a rate")
	}
	current.Value = previous.Value
	if score, ok, err := counterPoint(previous, current, 1000, rateMetric); err != nil || !ok || score != 0 {
		t.Fatal("legal zero rate dropped")
	}
	current.Count++
	if _, ok, err := counterPoint(previous, current, 1000, rateMetric); ok || err != nil {
		t.Fatal("incomplete identity produced a rate")
	}
}

func TestCounterPointRejectsTopologyChanges(t *testing.T) {
	// A constant counter isolates the topology guard: the only valid point is a zero rate.
	const same = `"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":10}`
	const topology = `,"shards":{"value":3},"replicas":{"value":1}`
	previous := parseSample(t, `{"key":1000,`+same+topology+`}`)
	stable := parseSample(t, `{"key":2000,`+same+topology+`}`)
	if score, ok, err := counterPoint(previous, stable, 1000, Metric{mode: rate}); err != nil || !ok || score != 0 {
		t.Fatalf("unchanged topology must keep scoring: %v %v %v", score, ok, err)
	}
	for name, raw := range map[string]string{
		"reshard":             `{"key":2000,` + same + `,"shards":{"value":5},"replicas":{"value":1}}`,
		"replica change":      `{"key":2000,` + same + `,"shards":{"value":3},"replicas":{"value":2}}`,
		"evidence disappears": `{"key":2000,` + same + `}`,
	} {
		if _, ok, err := counterPoint(previous, parseSample(t, raw), 1000, Metric{mode: rate}); ok || err != nil {
			t.Fatalf("%s produced a rate", name)
		}
	}
	// Node samples never carry topology evidence; absence on both sides is not a change.
	node := parseSample(t, `{"key":2000,`+same+`}`)
	if score, ok, err := counterPoint(parseSample(t, `{"key":1000,`+same+`}`), node, 1000, Metric{mode: rate}); err != nil || !ok || score != 0 {
		t.Fatalf("node transition without topology evidence: %v %v %v", score, ok, err)
	}
	// Evidence appearing on one side only is a change, whichever side it is.
	for _, pair := range [][2]samples{{previous, node}, {node, previous}} {
		if _, ok, err := counterPoint(pair[0], pair[1], 1000, Metric{mode: rate}); ok || err != nil {
			t.Fatal("one-sided topology evidence produced a rate")
		}
	}
}

func TestKeepWinner(t *testing.T) {
	type row struct {
		name  string
		score float64
	}
	names := func(best []winner) []string {
		got := make([]string, len(best))
		for i, w := range best {
			got[i] = w.name
		}
		return got
	}
	for _, tc := range []struct {
		n      int
		insert []row
		want   []string
	}{
		{n: 3, insert: []row{{"b", 2}, {"a", 2}}, want: []string{"a", "b"}},
		{n: 3, insert: []row{{"c", 1}, {"a", 3}, {"b", 2}}, want: []string{"a", "b", "c"}},
		{n: 2, insert: []row{{"a", 3}, {"b", 2}, {"c", 1}}, want: []string{"a", "b"}},
		{n: 2, insert: []row{{"c", 1}, {"b", 2}, {"a", 3}}, want: []string{"a", "b"}},
		{n: 1, insert: []row{{"z", 5}, {"a", 5}}, want: []string{"a"}},
		{n: 3, insert: []row{{"gap", -1}, {"b", 0}}, want: []string{"b"}},
	} {
		best := make([]winner, 0, tc.n)
		for _, r := range tc.insert {
			best = keepWinner(best, winner{name: r.name, score: r.score}, tc.n)
		}
		if got := names(best); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%v: got %v want %v", tc.insert, got, tc.want)
		}
	}
}

func TestCompositePageFailureDoesNotPublish(t *testing.T) {
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1001, Max: 2999, Width: 1, N: 1, AllIndices: true}
	for _, cancelSecond := range []bool{false, true} {
		pred, _ := q.filter()
		m := Metric{Item: &common.MetricItem{Key: "query_times"}, Field: "counter", mode: rate}
		calls := 0
		ctx, cancel := context.WithCancel(context.Background())
		s := snapshot{id: "test", shards: 3, call: func(context.Context, string, string, dsl) ([]byte, error) {
			calls++
			if calls == 1 {
				return result(`{"after_key":{"entity":"a"},"buckets":[{"key":{"entity":"a"},"dates":{"buckets":[{"key":1000,"doc_count":1,"value":{"value":10},"identity":{"buckets":[{"key":"id","doc_count":1}]}},{"key":2000,"doc_count":1,"value":{"value":20},"identity":{"buckets":[{"key":"id","doc_count":1}]}}]}}]}`), nil
			}
			if cancelSecond {
				cancel()
				return nil, ctx.Err()
			}
			return result(`{"after_key":{"entity":"a"},"buckets":[{"key":{"entity":"a"},"dates":{"buckets":[]}}]}`), nil
		}}
		err := paged(ctx, &s, q, pred, m)
		cancel()
		if err == nil || (cancelSecond && !errors.Is(err, context.Canceled)) || calls != 2 || len(m.Item.Lines) != 0 {
			t.Fatalf("err=%v calls=%d lines=%d", err, calls, len(m.Item.Lines))
		}
	}
}

func TestCounterScopeDoesNotReadHiddenPartOfFirstBucket(t *testing.T) {
	q := Query{Min: 3500, Max: 5999, Width: 1}
	got := q.counterScope()["bool"].(dsl)["should"].([]dsl)
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	for i, want := range [][2]int64{{3500, 5999}, {2000, 2999}} {
		r := got[i]["range"].(dsl)["timestamp"].(dsl)
		if r["gte"] != want[0] || r["lte"] != want[1] {
			t.Fatalf("got %v", r)
		}
	}
}

func TestCounterValuesApproximation(t *testing.T) {
	latency := Metric{Item: &common.MetricItem{Key: writeLatencyKey}, Field: writeTimeField, Field2: writeCounterField, mode: latency}
	var pairless samples
	if _, _, ok, err := pairless.counterValues(latency, true); ok || err != nil {
		t.Fatalf("approximate scoring must skip a missing pair: ok=%v err=%v", ok, err)
	}
	if _, _, _, err := pairless.counterValues(latency, false); err == nil {
		t.Fatal("strict scoring must reject a missing pair")
	}
}
