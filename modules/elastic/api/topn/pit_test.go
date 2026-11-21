package topn

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/modules/elastic/common"
)

func TestPITDialects(t *testing.T) {
	for _, tc := range []struct {
		version               elastic.Version
		path, strict, idField string
		closeBody             dsl
	}{
		{elastic.Version{Distribution: elastic.Easysearch}, "/_pit", "allow_partial_pit_creation", "pit_id", dsl{"pit_id": []string{"new"}}},
		{elastic.Version{Distribution: elastic.Opensearch, Number: "2.4.0"}, "/_search/point_in_time", "allow_partial_pit_creation", "pit_id", dsl{"pit_id": []string{"new"}}},
		{elastic.Version{Distribution: elastic.Elasticsearch, Number: "8.16.0"}, "/_pit", "allow_partial_search_results", "id", dsl{"id": "new"}},
	} {
		t.Run(tc.version.Distribution, func(t *testing.T) {
			api := pitFor(tc.version)
			calls := 0
			call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
				calls++
				switch calls {
				case 1:
					if method != http.MethodPost || !strings.HasPrefix(path, "/raw"+tc.path+"?") || !strings.Contains(path, "&"+tc.strict+"=false&") {
						t.Fatalf("create: %s %s", method, path)
					}
					return []byte(`{"` + tc.idField + `":"old",` + allShards + `}`), nil
				case 2:
					if method != http.MethodPost || !strings.HasPrefix(path, "/_search?") || body["pit"].(dsl)["id"] != "old" {
						t.Fatalf("search: %s %s %v", method, path, body)
					}
					return []byte(`{"pit_id":"new",` + allShards + `,"timed_out":false,"aggregations":{"entities":{}}}`), nil
				case 3:
					if method != http.MethodDelete || path != tc.path || !reflect.DeepEqual(body, tc.closeBody) {
						t.Fatalf("close: %s %s %v", method, path, body)
					}
					return []byte(`{}`), nil
				}
				t.Fatalf("unexpected request %s %s", method, path)
				return nil, nil
			}
			s, err := open(context.Background(), call, "raw", api)
			if err != nil {
				t.Fatal(err)
			}
			var value struct{}
			if err := s.search(context.Background(), dsl{}, dsl{}, &value); err != nil {
				t.Fatal(err)
			}
			s.close()
			if calls != 3 {
				t.Fatal(calls)
			}
		})
	}
	for _, v := range []elastic.Version{{Distribution: elastic.Elasticsearch, Number: "8.15.5"}, {Distribution: elastic.Opensearch, Number: "2.3.0"}, {Distribution: "other", Number: "9.0.0"}} {
		if pitFor(v).path != "" {
			t.Fatalf("selected PIT for %+v", v)
		}
	}
}

func TestQueryRouting(t *testing.T) {
	api := pitFor(elastic.Version{Distribution: elastic.Elasticsearch, Number: "8.16.0"})
	for _, tc := range []struct {
		name, source                    string
		usePIT, failSingle, partialOpen bool
		want                            []string
	}{
		{"node single", "node_stats", true, false, false, []string{"single"}},
		{"node PIT fallback", "node_stats", true, true, false, []string{"single", "open", "search", "close"}},
		{"index PIT", "index_stats", true, false, false, []string{"open", "search", "close"}},
		{"index incomplete PIT", "index_stats", true, false, true, []string{"open", "close", "single"}},
		{"index without PIT", "index_stats", false, false, false, []string{"single"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := Query{Source: tc.source, Min: 1000, Max: 1999, Width: 1, N: 5, AllIndices: true}
			pred, _ := q.filter()
			metric := Metric{Item: &common.MetricItem{Key: "storage"}, Field: "store"}
			var calls []string
			call := func(_ context.Context, method, path string, _ dsl) ([]byte, error) {
				switch {
				case strings.HasPrefix(path, "/raw/_pit?"):
					calls = append(calls, "open")
					if tc.partialOpen {
						return []byte(`{"id":"partial","_shards":{"total":3,"successful":2,"failed":0}}`), nil
					}
					return []byte(`{"id":"fixed",` + allShards + `}`), nil
				case method == http.MethodDelete && path == "/_pit":
					calls = append(calls, "close")
					return []byte(`{}`), nil
				case strings.HasPrefix(path, "/_search?"):
					calls = append(calls, "search")
					return result(`{"buckets":[]}`), nil
				case strings.HasPrefix(path, "/raw/_search?"):
					calls = append(calls, "single")
					if tc.failSingle {
						return nil, errors.New("single search unavailable")
					}
					return []byte(`{` + allShards + `,"timed_out":false,"aggregations":{"scan":{"entities":{"buckets":[]}}}}`), nil
				}
				t.Fatalf("unexpected request %s %s", method, path)
				return nil, nil
			}
			selected := pitAPI{}
			if tc.usePIT {
				selected = api
			}
			if err := runQuery(context.Background(), call, selected, "raw", pred, q, metric, q.Min, false); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(calls, tc.want) {
				t.Fatalf("calls=%v want %v", calls, tc.want)
			}
		})
	}
}
