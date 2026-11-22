package topn

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

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
	ServeSelected(w, r, q, materializedMetric(writeRateKey), "private,_all")
	if got := w.Body.String(); !strings.Contains(got, `"metrics":{"indexing_rate":`) || strings.Contains(got, `"topn_status"`) {
		t.Fatalf("want ordinary empty chart, got %s", got)
	}
}

func TestSelectedSearchBudgetRejectsBeforeQuery(t *testing.T) {
	q := Query{Source: "index_stats", Min: 1000, Max: 512000, Width: 1, AllIndices: true}
	for i := range 5 {
		q.selected = append(q.selected, "idx-"+strconv.Itoa(i))
	}
	pred, _ := q.filter()
	call := func(context.Context, string, string, dsl) ([]byte, error) {
		t.Fatal("unexpected query")
		return nil, nil
	}
	m := materializedMetric(writeRateKey)
	if err := singleSearch(context.Background(), call, "raw", pred, q, m); !errors.Is(err, errBudget) || len(m.Item.Lines) != 0 {
		t.Fatalf("err=%v lines=%v", err, m.Item.Lines)
	}
}
