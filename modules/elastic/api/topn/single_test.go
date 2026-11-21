package topn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"infini.sh/framework/modules/elastic/common"
)

func TestSingleSearchCompleteOrReject(t *testing.T) {
	q := Query{Source: "node_stats", Min: 1000, Max: 1999, Width: 1, N: 2}
	for _, tc := range []struct {
		name            string
		count, coverage int
		partial         bool
		want            error
	}{
		{name: "complete at limit", count: 192, coverage: 1},
		{name: "empty", coverage: 1},
		{name: "sentinel", count: 193, coverage: 1, want: errBudget},
		{name: "history unavailable", count: 1, want: errRawHistory},
		{name: "partial shards", count: 1, coverage: 1, partial: true, want: errors.New("incomplete monitoring search")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
				calls++
				if method != "POST" || !strings.HasPrefix(path, "/raw/_search?") || body["pit"] != nil {
					t.Fatalf("unexpected search: %s %s", method, path)
				}
				var entities []string
				for i := 0; i < tc.count; i++ {
					entities = append(entities, fmt.Sprintf(`{"key":"node-%03d","dates":{"buckets":[{"key":1000,"value":{"value":0}}]}}`, i))
				}
				shards := allShards
				if tc.partial {
					shards = `"_shards":{"total":3,"successful":2,"failed":0}`
				}
				return []byte(fmt.Sprintf(`{%s,"timed_out":false,"aggregations":{"coverage":{"doc_count":%d},"scan":{"entities":{"buckets":[%s]}}}}`, shards, tc.coverage, strings.Join(entities, ","))), nil
			}
			pred, _ := q.filter()
			metric := Metric{Item: &common.MetricItem{Key: "cpu"}, Field: "cpu"}
			err := singleSearch(context.Background(), call, "raw", pred, q, metric, q.Min, true)
			if calls != 1 {
				t.Fatalf("got %d requests", calls)
			}
			if tc.want != nil {
				if err == nil || err.Error() != tc.want.Error() || len(metric.Item.Lines) != 0 {
					t.Fatalf("error=%v lines=%v", err, metric.Item.Lines)
				}
				return
			}
			if err != nil || len(metric.Item.Lines) != min(q.N, tc.count) {
				t.Fatalf("error=%v lines=%v", err, metric.Item.Lines)
			}
			if tc.count != 0 && (metric.Item.Lines[0].Metric.Label != "node-000" || metric.Item.Lines[1].Metric.Label != "node-001") {
				t.Fatal("zero scores were dropped or ties were reordered")
			}
		})
	}
}
