package topn

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"infini.sh/framework/modules/elastic/common"
)

func indexingRateMetric(t *testing.T) Metric {
	t.Helper()
	metric, supported := NewMetric(&common.MetricItem{Key: "indexing_rate"}, "metric", "", "", "", true)
	if !supported {
		t.Fatal("indexing_rate must support exact TopN")
	}
	return metric
}

func TestSelectEntities(t *testing.T) {
	q := Query{Source: "index_stats", Indices: []string{"logs-*", "archive"}}
	q.selected = selectEntities(q, "logs-z,private,archive,logs-z,,_all,logs-a")
	want := []string{"logs-z", "archive", "logs-a"}
	if !reflect.DeepEqual(q.selected, want) {
		t.Fatalf("got %v want %v", q.selected, want)
	}
	pred, ok := q.filter()
	filters := pred["filter"].([]dsl)
	if !ok || !reflect.DeepEqual(filters[len(filters)-1], dsl{"terms": dsl{q.entity(): want}}) || q.authorizer()("logs-new") {
		t.Fatalf("not an exact selection: %v", pred)
	}
}

func TestServeSelectedAllDenied(t *testing.T) {
	q := Query{Source: "index_stats", Width: 1, Indices: []string{"public-*"}}
	r := httptest.NewRequest("GET", "/?min=1000&max=2000&top=not-used", nil)
	w := httptest.NewRecorder()
	ServeSelected(w, r, q, indexingRateMetric(t), "private,_all")
	if got := w.Body.String(); !strings.Contains(got, `"metrics":{"indexing_rate":`) || strings.Contains(got, `"topn_status"`) {
		t.Fatalf("want ordinary empty chart, got %s", got)
	}
}

func TestSelectedSearchUsesFullBucketCapacity(t *testing.T) {
	q := Query{Source: "index_stats", Min: 1000, Max: 512000, Width: 1, AllIndices: true}
	q.selected = []string{"idx-0", "idx-1", "idx-2"}
	pred, _ := q.filter()
	m := indexingRateMetric(t)
	calls := 0
	call := func(_ context.Context, _, _ string, body dsl) ([]byte, error) {
		calls++
		entities := body["aggs"].(dsl)["entities"].(dsl)
		if got := entities["terms"].(dsl)["size"]; got != 3 {
			t.Fatalf("selected query requested %v terms, want 3", got)
		}
		return []byte(`{` + allShards + `,"timed_out":false,"aggregations":{"entities":{"buckets":[]}}}`), nil
	}
	if err := singleSearch(context.Background(), call, "raw", pred, q, m); err != nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	q.selected = append(q.selected, "idx-3")
	pred, _ = q.filter()
	if err := singleSearch(context.Background(), call, "raw", pred, q, m); !errors.Is(err, errBudget) || calls != 1 {
		t.Fatalf("over-capacity selection: err=%v calls=%d", err, calls)
	}
}
