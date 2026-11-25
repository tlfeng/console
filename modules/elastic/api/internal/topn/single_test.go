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
		name    string
		count   int
		rollup  bool
		partial bool
		want    error
	}{
		{name: "complete at limit", count: 192},
		{name: "empty"},
		{name: "sentinel", count: 193, want: errCandidateLimit},
		{name: "accept rollup approximation", count: 1, rollup: true},
		{name: "partial shards", count: 1, partial: true, want: errors.New("incomplete monitoring search")},
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
				if tc.rollup {
					shards += `,"origin":"rollup"`
				}
				return []byte(fmt.Sprintf(`{%s,"timed_out":false,"aggregations":{"scan":{"entities":{"buckets":[%s]}}}}`, shards, strings.Join(entities, ","))), nil
			}
			pred, _ := q.filter()
			metric := Metric{Item: &common.MetricItem{Key: "cpu"}, Field: "cpu"}
			err := singleSearch(context.Background(), call, "raw", pred, q, metric)
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
			if tc.count >= 2 && (metric.Item.Lines[0].Metric.Label != "node-000" || metric.Item.Lines[1].Metric.Label != "node-001") {
				t.Fatal("zero scores were dropped or ties were reordered")
			}
		})
	}
}
