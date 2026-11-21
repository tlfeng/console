package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/modules/elastic/common"
)

const allShards = `"_shards":{"total":3,"successful":3,"failed":0}`

func result(entities string) []byte {
	return []byte(fmt.Sprintf(`{%s,"timed_out":false,"aggregations":{"entities":%s}}`, allShards, entities))
}

func TestPeakSnapshot(t *testing.T) {
	calls := 0
	call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
		calls++
		switch calls {
		case 1:
			if method != "POST" || path[:len("/raw/_pit?")] != "/raw/_pit?" {
				t.Fatalf("open %s %s", method, path)
			}
			return []byte(`{"pit_id":"fixed",` + allShards + `}`), nil
		case 2:
			if body["pit"].(dsl)["id"] != "fixed" {
				t.Fatal(body)
			}
			return result(`{"buckets":[{"key":"inactive","peak":{"value":90}},{"key":"active","peak":{"value":0}}]}`), nil
		case 3:
			if body["pit"].(dsl)["id"] != "fixed" {
				t.Fatal(body)
			}
			return result(`{"buckets":[{"key":"active","dates":{"buckets":[{"key":1000,"value":{"value":0}}]}},{"key":"inactive","dates":{"buckets":[{"key":1000,"value":{"value":80}},{"key":2000,"value":{"value":90}}]}}]}`), nil
		case 4:
			if method != "DELETE" || path != "/_pit" || !reflect.DeepEqual(body["pit_id"], []string{"fixed"}) {
				t.Fatalf("close %s %s %v", method, path, body)
			}
			return []byte(`{}`), nil
		}
		t.Fatal("unexpected call")
		return nil, nil
	}
	ctx := context.Background()
	s, err := open(ctx, call, "raw", pitFor(elastic.Version{Distribution: elastic.Easysearch}))
	if err != nil {
		t.Fatal(err)
	}
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1001, Max: 2001, Width: 1, N: 2, Indices: []string{"in*", "active"}}
	pred, ok := q.filter()
	if !ok {
		t.Fatal("valid authorization rejected")
	}
	m := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	if err := peaks(ctx, s, q, pred, m); err != nil {
		t.Fatal(err)
	}
	s.close()
	if calls != 4 || len(m.Item.Lines) != 2 || m.Item.Lines[0].Metric.Label != "inactive" || m.Item.Lines[1].Metric.Label != "active" {
		t.Fatal(m.Item.Lines)
	}
	if got := m.Item.Lines[0].Data; !reflect.DeepEqual(got, [][]interface{}{{int64(1000), 80.0}, {int64(2000), 90.0}}) {
		t.Fatal(got)
	}
}

func TestStrictWindowAndAuthorization(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/?min=1001&max=2002&top=20", nil)
	min, max, n, ok := Window(r, 1)
	if !ok || min != 1001 || max != 2002 || n != 20 {
		t.Fatal(min, max, n, ok)
	}
	if _, _, _, ok := Window(httptest.NewRequest(http.MethodGet, "/?min=bad&max=2002", nil), 1); ok {
		t.Fatal("invalid time accepted")
	}
	for _, rules := range [][]string{nil, {`secret\*`}, {`abc?`}} {
		if _, ok := (Query{Source: "index_stats", Indices: rules}).filter(); ok {
			t.Fatal("untranslatable authorization accepted", rules)
		}
	}
}

func TestIncompleteSnapshotSearch(t *testing.T) {
	s := snapshot{id: "fixed", shards: 3, call: func(context.Context, string, string, dsl) ([]byte, error) {
		return []byte(`{"pit_id":"rotated","_shards":{"total":3,"successful":2,"failed":0},"timed_out":false,"aggregations":{"entities":{"buckets":[]}}}`), nil
	}}
	var rows struct {
		Buckets []bucket `json:"buckets"`
	}
	if err := s.search(context.Background(), dsl{}, dsl{}, &rows); err == nil || s.id != "rotated" {
		t.Fatalf("err=%v pit=%s", err, s.id)
	}
}

func TestFailureStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{errRawHistory, "history_unavailable"},
		{fmt.Errorf("search: %w", errBudget), "budget_exceeded"},
		{context.DeadlineExceeded, "timed_out"},
		{errUnavailable, "unavailable"},
		{errors.New("backend details"), "query_failed"},
	} {
		w := httptest.NewRecorder()
		WriteStatus(w, failureStatus(tc.err))
		var got map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got) != 1 || got["topn_status"] != tc.want {
			t.Fatalf("%v: response %s, want only status %q", tc.err, w.Body, tc.want)
		}
	}
}

func TestCompleteEmptyResult(t *testing.T) {
	w := httptest.NewRecorder()
	writeMetrics(w, Metric{Item: &common.MetricItem{Key: "cpu", Lines: []*common.MetricLine{}}})
	var got struct {
		Status  string                        `json:"topn_status"`
		Metrics map[string]*common.MetricItem `json:"metrics"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Status != "" || got.Metrics["cpu"] == nil || len(got.Metrics["cpu"].Lines) != 0 {
		t.Fatalf("expected successful empty chart, got %s", w.Body)
	}
}
