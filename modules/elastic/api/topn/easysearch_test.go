package topn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"infini.sh/framework/modules/elastic/common"
)

// Use a disposable loopback Easysearch cluster. Never point this at a real monitoring store.
func TestEasysearchSnapshot(t *testing.T) {
	if os.Getenv("CONSOLE_TOPN_INTEGRATION") != "1" {
		t.Skip("set CONSOLE_TOPN_INTEGRATION=1 for a disposable Easysearch on localhost:19219")
	}
	const base = "http://127.0.0.1:19219"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	call := func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		if res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: %s", method, path, raw)
		}
		return raw, nil
	}
	raw, err := call(ctx, "GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	var info struct {
		Cluster string `json:"cluster_name"`
	}
	if json.Unmarshal(raw, &info) != nil || info.Cluster != "console-topn-local-bench" {
		t.Fatal("not the disposable TopN test cluster")
	}
	index := fmt.Sprintf("console-topn-test-%d", time.Now().UnixNano())
	must := func(method, path string, body dsl) {
		t.Helper()
		if _, err := call(ctx, method, path, body); err != nil {
			t.Fatal(err)
		}
	}
	must("PUT", "/"+index, dsl{"settings": dsl{"number_of_shards": 3, "number_of_replicas": 0}, "mappings": dsl{"properties": dsl{
		"timestamp": dsl{"type": "date"}, "metric": dsl{"type": "long"}, "metadata": dsl{"properties": dsl{
			"name": dsl{"type": "keyword"}, "category": dsl{"type": "keyword"}, "labels": dsl{"properties": dsl{"cluster_id": dsl{"type": "keyword"}, "index_name": dsl{"type": "keyword"}}},
		}}}}})
	defer func() { _, _ = call(context.Background(), "DELETE", "/"+index, nil) }()
	add := func(id, name string, ts, val int) {
		must("PUT", "/"+index+"/_doc/"+id+"?refresh=true", dsl{"timestamp": ts, "metric": val, "metadata": dsl{"name": "index_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "biz", "index_name": name}}})
	}
	add("a1", "a", 1001, 10)
	add("a2", "a", 2001, 50)
	add("b1", "b", 1001, 50)
	add("secret", "secret", 1001, 99)
	s, err := open(ctx, call, index)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	add("late", "b", 2001, 999)
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1001, Max: 2999, Width: 1, N: 1, Indices: []string{"a", "b"}}
	pred, ok := q.filter()
	if !ok {
		t.Fatal("valid permissions rejected")
	}
	m := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	if err := peaks(ctx, s, q, pred, m); err != nil {
		t.Fatal(err)
	}
	if len(m.Item.Lines) != 1 || m.Item.Lines[0].Metric.Label != "a" || len(m.Item.Lines[0].Data.([][]interface{})) != 2 {
		t.Fatalf("unexpected ranking: %+v", m.Item.Lines)
	}
}
