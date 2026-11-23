package topn

import (
	"context"
	"encoding/json"
	"errors"
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
	previous := parseSample(t, `{"key":1000,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":40},"pair":{"hits":{"hits":[{"fields":{"elapsed":[40],"ops":[5]}}]}},"jvm_start_count":{"value":1},"jvm_start_min":{"value":10000},"jvm_start_max":{"value":10000}}`)
	current := parseSample(t, `{"key":2000,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":60},"pair":{"hits":{"hits":[{"fields":{"elapsed":[60],"ops":[7]}}]}},"jvm_start_count":{"value":1},"jvm_start_min":{"value":10100},"jvm_start_max":{"value":10100}}`)
	paired := Metric{mode: latency, Field: "elapsed", Field2: "ops"}
	rateMetric := Metric{mode: rate}
	if score, ok, err := counterPoint(previous, current, 1000, paired, true); err != nil || !ok || score != 10 {
		t.Fatalf("got %v, %v, %v", score, ok, err)
	}
	if _, ok, err := counterPoint(previous, current, 2000, rateMetric, false); ok || err != nil {
		t.Fatal("gap produced a point or error")
	}
	truncated := current
	truncated.Count = 2
	if sameJVMLifetime(previous, truncated) || sameJVMLifetime(truncated, current) {
		t.Fatal("truncated JVM evidence must invalidate both neighboring differences")
	}

	missing := current
	missing.Pair = nil
	if _, _, err := counterPoint(previous, missing, 1000, paired, true); err == nil {
		t.Fatal("missing paired aggregation accepted")
	}
	fields := current.Pair.Hits.Rows[0].Fields
	for _, ops := range [][]float64{nil, {7, 9}, {5}} {
		fields["ops"] = ops
		if _, ok, err := counterPoint(previous, current, 1000, paired, true); ok || err != nil {
			t.Fatal("missing, multivalued, or unchanged operations produced a latency point")
		}
	}
	*current.JVMMin.Value, *current.JVMMax.Value = 20000, 20000
	if _, ok, err := counterPoint(previous, current, 1000, rateMetric, true); ok || err != nil {
		t.Fatal("process restart produced a rate")
	}
	current.Value = &scalar{Value: new(float64)}
	if _, ok, err := counterPoint(previous, current, 1000, rateMetric, false); ok || err != nil {
		t.Fatal("counter reset produced a rate")
	}
	current.Value = previous.Value
	if score, ok, err := counterPoint(previous, current, 1000, rateMetric, false); err != nil || !ok || score != 0 {
		t.Fatal("legal zero rate dropped")
	}
	current.Count++
	if _, ok, err := counterPoint(previous, current, 1000, rateMetric, false); ok || err != nil {
		t.Fatal("incomplete identity produced a rate")
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

func TestMixedJVMLifetime(t *testing.T) {
	const summary = `"doc_count":70,"jvm_start_min":{"value":10000},"jvm_start_max":{"value":10050},"jvm_start_count":{"value":69},`
	for _, tc := range []struct {
		name   string
		legacy string
		start  int64
	}{
		{"one legacy sample with 69 summarized", `"jvm_legacy":{"doc_count":1,"jvm_lifetime":{"hits":{"hits":[{"fields":{"` + jvmTimestamp + `":[11000],"` + jvmUptime + `":[900]}}]}}}`, 10000},
		{"missing legacy sample", `"jvm_legacy":{"doc_count":1,"jvm_lifetime":{"hits":{"hits":[]}}}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, hi, ok := parseSample(t, "{"+summary+tc.legacy+"}").jvmLifetime()
			if got != tc.start || ok != (tc.start != 0) || (ok && hi != 10100) {
				t.Fatalf("got %d..%d valid=%v", got, hi, ok)
			}
		})
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
