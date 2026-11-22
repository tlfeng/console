package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestMaterializedScopes(t *testing.T) {
	q := Query{Min: 1500, Max: 5000, Width: 1}
	sealed, edges := materializedScopes(q, 6000)
	if !reflect.DeepEqual(sealed, []int64{3000, 4000}) || !reflect.DeepEqual(edges, map[int64]bool{1000: true, 2000: true, 5000: true}) {
		t.Fatalf("%v %v", sealed, edges)
	}
	q.Min, q.Max = 2000, 4999
	sealed, edges = materializedScopes(q, 4000)
	if !reflect.DeepEqual(sealed, []int64{2000, 3000}) || !reflect.DeepEqual(edges, map[int64]bool{4000: true}) {
		t.Fatalf("%v %v", sealed, edges)
	}
}

func TestMaterializedRank(t *testing.T) {
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1000, Max: 5000, Width: 1, N: 2, Indices: []string{"a*", "b*"}}
	m := materializedMetric(writeRateKey)
	buckets := []scoreBucket{
		{materializedVersion, 5000, []string{"alpha", "beta"}, []float64{2, 0}},
		{materializedVersion, 1000, []string{"alpha", "beta", "restricted"}, []float64{5, 5, 99}},
	}
	if err := materializedRank(q, m, buckets); err != nil {
		t.Fatal(err)
	}
	if len(m.Item.Lines) != 2 || m.Item.Lines[0].Metric.Label != "alpha" || m.Item.Lines[1].Metric.Label != "beta" {
		t.Fatalf("wrong rank: %+v", m.Item.Lines)
	}
	want := [][]interface{}{{int64(1000), float64(5)}, {int64(2000), nil}, {int64(5000), float64(0)}}
	if !reflect.DeepEqual(m.Item.Lines[1].Data, want) {
		t.Fatalf("got %v want %v", m.Item.Lines[1].Data, want)
	}
}

func TestMaterializedRequiresEveryBucket(t *testing.T) {
	ctx := context.Background()
	m := materializedStore{raw: "raw", index: ".console-topn-test", gaps: []coverageGap{{3000, 4000}}}
	q := Query{Cluster: "biz", Source: "index_stats", Min: 2000, Max: 4999, Width: 1, N: 1, AllIndices: true}
	sealed, edges := summaryPlan(m, q, time.UnixMilli(9999999))
	if !reflect.DeepEqual(sealed, []int64{2000}) || len(edges) != 0 {
		t.Fatalf("declared gap must only suppress affected transitions: %v %v", sealed, edges)
	}
	s := snapshot{id: "ok", shards: 1, call: func(context.Context, string, string, dsl) ([]byte, error) {
		return []byte(fmt.Sprintf(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[{"_id":%q,"_source":{"v":1,"t":2000,"n":[],"s":[]}}]}}`, m.metricID(q, writeRateKey, 2000))), nil
	}}
	if _, err := readMetricScoreBuckets(ctx, &s, q, m, writeRateKey, []int64{2000, 3000}); !errors.Is(err, errSummaryHistory) {
		t.Fatal(err)
	}
}

func TestMaterializedFailedSearchDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	q := Query{Cluster: "biz", Source: "index_stats", Min: 2000, Max: 2999, Width: 1, N: 1, AllIndices: true}
	m := materializedStore{index: ".console-topn-test", raw: "raw"}
	var writes, closes int
	call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
		switch {
		case method == http.MethodDelete:
			closes++
			return []byte(`{"pits":[{"pit_id":"p","successful":true}]}`), nil
		case method == http.MethodPut:
			writes++
			return nil, nil
		case body == nil:
			return []byte(`{"pit_id":"p","_shards":{"total":1,"successful":1,"failed":0}}`), nil
		default:
			return nil, errors.New("search failed")
		}
	}
	if err := publishTransitionBatches(ctx, call, pitAPI{path: "/_pit", idField: "pit_id"}, m, q, []transitionBatch{{materializedMetric(writeRateKey), []int64{2000}}}); err == nil || writes != 0 || closes != 1 {
		t.Fatalf("err=%v writes=%d closes=%d", err, writes, closes)
	}
}

func TestMaterializedTransitionClipsFirstAndLast(t *testing.T) {
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1500, Max: 3500, Width: 1, N: 1, AllIndices: true}
	s := snapshot{id: "ok", shards: 1, call: func(_ context.Context, _, _ string, body dsl) ([]byte, error) {
		raw, _ := json.Marshal(body["query"])
		var query struct {
			Bool struct{ Filter []json.RawMessage }
		}
		_ = json.Unmarshal(raw, &query)
		var scope struct {
			Bool struct {
				Should []struct {
					Range map[string]struct{ Gte, Lte int64 }
				}
			}
		}
		_ = json.Unmarshal(query.Bool.Filter[len(query.Bool.Filter)-1], &scope)
		got := map[int64]int64{}
		for _, term := range scope.Bool.Should {
			r := term.Range["timestamp"]
			got[r.Gte] = r.Lte
		}
		want := map[int64]int64{0: 999, 1500: 1999, 2000: 2999, 3000: 3500}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
		parts := make([]dsl, 0, 4)
		for i, v := range []int{10, 12, 18, 18} {
			parts = append(parts, dsl{"key": i * 1000, "doc_count": 1, "value": dsl{"value": v}, "identity": dsl{"buckets": []dsl{{"key": "uuid", "doc_count": 1}}}})
		}
		return json.Marshal(dsl{"timed_out": false, "_shards": dsl{"total": 1, "successful": 1, "failed": 0}, "aggregations": dsl{"entities": dsl{"buckets": []dsl{{"key": dsl{"entity": "x"}, "dates": dsl{"buckets": parts}}}}}})
	}}
	got, err := readTransitions(context.Background(), &s, q, materializedMetric(writeRateKey), []int64{1000, 2000, 3000})
	if err != nil {
		t.Fatal(err)
	}
	for i, x := range []float64{2, 6, 0} {
		if !reflect.DeepEqual(got[i].Scores, []float64{x}) {
			t.Fatalf("bucket %d: %v", i, got[i])
		}
	}
}
