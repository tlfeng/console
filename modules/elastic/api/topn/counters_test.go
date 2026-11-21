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

func TestCounterPoint(t *testing.T) {
	previous := parseSample(t, `{"key":1000,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":40},"other":{"value":5},"uptime":{"value":900}}`)
	current := parseSample(t, `{"key":2000,"doc_count":1,"identity":{"buckets":[{"key":"id","doc_count":1}]},"value":{"value":60},"other":{"value":7},"uptime":{"value":1900},"first_uptime":{"value":1900}}`)
	if score, ok := counterPoint(previous, current, 1000, true, true); !ok || score != 10 {
		t.Fatalf("got %v, %v", score, ok)
	}
	if _, ok := counterPoint(previous, current, 2000, false, false); ok {
		t.Fatal("gap produced a point")
	}
	current.Other = previous.Other
	if _, ok := counterPoint(previous, current, 1000, true, true); ok {
		t.Fatal("zero requests produced a latency point")
	}
	current.FirstUptime = previous.Uptime
	if _, ok := counterPoint(previous, current, 1000, false, true); ok {
		t.Fatal("process restart produced a rate")
	}
	current.Value = &scalar{Value: new(float64)}
	if _, ok := counterPoint(previous, current, 1000, false, false); ok {
		t.Fatal("counter reset produced a rate")
	}
	current.Value = previous.Value
	if score, ok := counterPoint(previous, current, 1000, false, false); !ok || score != 0 {
		t.Fatal("legal zero rate dropped")
	}
	current.Count++
	if _, ok := counterPoint(previous, current, 1000, false, false); ok {
		t.Fatal("incomplete identity produced a rate")
	}
}

func TestCompositePageFailureDoesNotPublish(t *testing.T) {
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1001, Max: 2999, Width: 1, N: 1, AllIndices: true}
	for _, cancelSecond := range []bool{false, true} {
		pred, _ := q.filter()
		m := Metric{Item: &common.MetricItem{Key: "query_times"}, Field: "counter", Counter: true}
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
		err := counters(ctx, &s, q, pred, m)
		cancel()
		if err == nil || (cancelSecond && !errors.Is(err, context.Canceled)) || calls != 2 || len(m.Item.Lines) != 0 {
			t.Fatalf("err=%v calls=%d lines=%d", err, calls, len(m.Item.Lines))
		}
	}
}
