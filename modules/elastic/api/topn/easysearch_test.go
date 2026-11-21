package topn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/modules/elastic/common"
)

// Use a disposable loopback Easysearch cluster. Never point this at a real monitoring store.
func TestEasysearchSnapshot(t *testing.T) {
	if os.Getenv("CONSOLE_TOPN_INTEGRATION") != "1" {
		t.Skip("set CONSOLE_TOPN_INTEGRATION=1 for a disposable Easysearch on localhost:19219")
	}
	const epoch = 1780000000000
	const base = "http://127.0.0.1:19219"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
			"name": dsl{"type": "keyword"}, "category": dsl{"type": "keyword"}, "labels": dsl{"properties": dsl{"cluster_id": dsl{"type": "keyword"}, "index_name": dsl{"type": "keyword"}, "index_uuid": dsl{"type": "keyword"}, "transport_address": dsl{"type": "keyword"}, "node_id": dsl{"type": "keyword"}}},
		}}}}})
	defer func() { _, _ = call(context.Background(), "DELETE", "/"+index, nil) }()
	add := func(id, name string, ts, val int) {
		must("PUT", "/"+index+"/_doc/"+id+"?refresh=true", dsl{"timestamp": epoch + ts, "metric": val, "metadata": dsl{"name": "index_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "biz", "index_name": name, "index_uuid": "id-" + name}}})
	}
	add("a1", "a", 1001, 10)
	add("a2", "a", 2001, 50)
	add("b1", "b", 1001, 50)
	add("secret", "secret", 1001, 99)
	alias := index + "-raw"
	must("POST", "/_aliases", dsl{"actions": []dsl{{"add": dsl{"index": index, "alias": alias}}}})
	s, err := open(ctx, call, alias, pitFor(elastic.Version{Distribution: elastic.Easysearch}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	add("late", "b", 2001, 999)
	q := Query{Cluster: "biz", Source: "index_stats", Min: epoch + 1001, Max: epoch + 2999, Width: 1, N: 1, Indices: []string{"a", "b"}}
	pred, ok := q.filter()
	if !ok {
		t.Fatal("valid permissions rejected")
	}
	for _, tc := range []struct {
		boundary int64
		want     bool
	}{{q.Min, true}, {q.Min - 1, false}} {
		got, err := s.retainedAt(ctx, pred, tc.boundary)
		if err != nil || got != tc.want {
			t.Fatalf("retainedAt(%d) = %v, %v; want %v", tc.boundary, got, err, tc.want)
		}
	}

	m := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	if err := peaks(ctx, s, q, pred, m); err != nil {
		t.Fatal(err)
	}
	if len(m.Item.Lines) != 1 || m.Item.Lines[0].Metric.Label != "a" || len(m.Item.Lines[0].Data.([][]interface{})) != 2 {
		t.Fatalf("unexpected ranking: %+v", m.Item.Lines)
	}
	single := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	pred, _ = q.filter()
	if err := singleSearch(ctx, call, alias, pred, q, single, q.Min, true); err != nil {
		t.Fatal(err)
	}
	if len(single.Item.Lines) != 1 || single.Item.Lines[0].Metric.Label != "b" || !reflect.DeepEqual(single.Item.Lines[0].Data, [][]interface{}{{int64(epoch + 1000), 50.0}, {int64(epoch + 2000), 999.0}}) {
		t.Fatalf("expected single search to see the late write, got %+v", single.Item.Lines)
	}

	for i := 0; i < 65; i++ {
		name := fmt.Sprintf("page-%03d", i)
		add(name+"-1", name, 1001, 1)
		add(name+"-2", name, 2001, i+2)
	}
	pageSnapshot, err := open(ctx, call, index, pitFor(elastic.Version{Distribution: elastic.Easysearch}))
	if err != nil {
		t.Fatal(err)
	}
	defer pageSnapshot.close()
	q.Indices = []string{"page-*"}
	pred, ok = q.filter()
	if !ok {
		t.Fatal("valid wildcard authorization rejected")
	}
	m = Metric{Item: &common.MetricItem{Key: "query_times"}, Field: "metric", mode: rate}
	if err := paged(ctx, pageSnapshot, q, pred, m); err != nil {
		t.Fatal(err)
	}
	if len(m.Item.Lines) != 1 || m.Item.Lines[0].Metric.Label != "page-064" || !reflect.DeepEqual(m.Item.Lines[0].Data, [][]interface{}{{int64(epoch + 2000), 65.0}}) {
		t.Fatalf("expected the later-page winner and its rate, got %+v", m.Item.Lines)
	}

	pred, _ = q.filter()
	single = Metric{Item: &common.MetricItem{Key: "query_times"}, Field: "metric", mode: rate}
	if err := singleSearch(ctx, call, alias, pred, q, single, q.previousBucket(), false); err != nil {
		t.Fatal(err)
	}
	if len(single.Item.Lines) != len(m.Item.Lines) || single.Item.Lines[0].Metric.Label != m.Item.Lines[0].Metric.Label || !reflect.DeepEqual(single.Item.Lines[0].Data, m.Item.Lines[0].Data) {
		t.Fatal("single search and PIT pagination disagree")
	}

	for _, row := range []struct {
		name             string
		total, available int
	}{{"large", 1000, 100}, {"small", 100, 90}, {"full", 50, 0}} {
		must("PUT", "/"+index+"/_doc/disk-"+row.name+"?refresh=true", dsl{
			"timestamp": epoch + 2001, "total": row.total, "available": row.available,
			"metadata": dsl{"name": "node_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "biz", "transport_address": row.name}},
		})
	}
	q.Source, q.N = "node_stats", 3
	for _, usePIT := range []bool{false, true} {
		pred, _ := q.filter()
		disk, _ := NewMetric(&common.MetricItem{Key: "disk"}, "total", "available", "%", "ratio", false)
		var err error
		if usePIT {
			snap, openErr := open(ctx, call, alias, pitFor(elastic.Version{Distribution: elastic.Easysearch}))
			if openErr != nil {
				t.Fatal(openErr)
			}
			err = paged(ctx, snap, q, pred, disk)
			snap.close()
		} else {
			err = singleSearch(ctx, call, alias, pred, q, disk, q.Min, false)
		}
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range []struct {
			name  string
			score float64
		}{{"small", 90}, {"large", 10}, {"full", 0}} {
			if len(disk.Item.Lines) != 3 || disk.Item.Lines[i].Metric.Label != want.name || !reflect.DeepEqual(disk.Item.Lines[i].Data, [][]interface{}{{int64(epoch + 2000), want.score}}) {
				t.Fatalf("PIT=%v: incorrect disk ranking or points", usePIT)
			}
		}
	}

	t.Run("restart inside bucket", func(t *testing.T) {
		// One address and node ID, but two JVM lifetimes in the middle bucket.
		for i, row := range []struct{ offset, uptime, count int }{
			{1000, 1000, 10}, {20100, 20100, 50}, {24000, 100, 1}, {59000, 35100, 150}, {79000, 55100, 190},
		} {
			must("PUT", fmt.Sprintf("/%s/_doc/restart-%d?refresh=true", index, i), dsl{
				"timestamp": epoch + row.offset, "metric": row.count, "elapsed": row.count * 3,
				"metadata": dsl{"name": "node_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "restart", "transport_address": "n1", "node_id": "same-node"}},
				"payload":  dsl{"elasticsearch": dsl{"node_stats": dsl{"jvm": dsl{"timestamp": epoch + row.offset, "uptime_in_millis": row.uptime}}}},
			})
		}
		restart := Query{Cluster: "restart", Source: "node_stats", Min: epoch, Max: epoch + 79999, Width: 20, N: 1}
		for _, tc := range []struct {
			key, field, second string
			mode               metricMode
			want               float64
		}{{"query_rate", "metric", "", rate, 2}, {"query_latency", "elapsed", "metric", latency, 3}} {
			for _, usePIT := range []bool{false, true} {
				pred, _ := restart.filter()
				metric := Metric{Item: &common.MetricItem{Key: tc.key}, Field: tc.field, Field2: tc.second, mode: tc.mode}
				if usePIT {
					s, err := open(ctx, call, alias, pitFor(elastic.Version{Distribution: elastic.Easysearch}))
					if err != nil {
						t.Fatal(err)
					}
					defer s.close()
					if err := paged(ctx, s, restart, pred, metric); err != nil {
						t.Fatal(err)
					}
				} else if err := singleSearch(ctx, call, alias, pred, restart, metric, restart.Min, false); err != nil {
					t.Fatal(err)
				}
				if len(metric.Item.Lines) != 1 || !reflect.DeepEqual(metric.Item.Lines[0].Data, [][]interface{}{{int64(epoch + 60000), tc.want}}) {
					t.Fatalf("%s PIT=%v: expected only the recovery point at %v, got %+v", tc.key, usePIT, tc.want, metric.Item.Lines)
				}
			}
		}
	})

	t.Run("raw alias with an interior retention gap", func(t *testing.T) {
		backing := []string{index + "-old", index + "-middle", index + "-new"}
		historyAlias := index + "-history"
		for i, name := range backing {
			must("PUT", "/"+name, dsl{"settings": dsl{"number_of_shards": 1, "number_of_replicas": 0}, "aliases": dsl{historyAlias: dsl{}}, "mappings": dsl{"properties": dsl{
				"timestamp": dsl{"type": "date"}, "metric": dsl{"type": "long"}, "metadata": dsl{"properties": dsl{
					"name": dsl{"type": "keyword"}, "category": dsl{"type": "keyword"}, "labels": dsl{"properties": dsl{"cluster_id": dsl{"type": "keyword"}, "index_name": dsl{"type": "keyword"}}},
				}},
			}}})
			t.Cleanup(func() { _, _ = call(context.Background(), "DELETE", "/"+name, nil) })
			must("PUT", "/"+name+"/_doc/row?refresh=true", dsl{
				"timestamp": epoch + 1 + i*1000, "metric": []int{1, 100, 10}[i],
				"metadata": dsl{"name": "index_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "history", "index_name": []string{"old", "middle", "new"}[i]}},
			})
		}
		window := Query{Cluster: "history", Source: "index_stats", Min: epoch + 1000, Max: epoch + 2999, Width: 1, N: 1, AllIndices: true}
		search := func(usePIT bool, min int64) (string, error) {
			q := window
			q.Min = min
			pred, _ := q.filter()
			metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
			var api pitAPI
			if usePIT {
				api = pitFor(elastic.Version{Distribution: elastic.Easysearch})
			}
			if err := runQuery(ctx, call, api, historyAlias, pred, q, metric, min, true); err != nil {
				return "", err
			}
			if len(metric.Item.Lines) == 0 {
				return "", nil
			}
			return metric.Item.Lines[0].Metric.Label, nil
		}
		for _, usePIT := range []bool{false, true} {
			if got, err := search(usePIT, window.Min); err != nil || got != "middle" {
				t.Fatalf("PIT=%v complete history: got %q, %v", usePIT, got, err)
			}
			if _, err := search(usePIT, epoch); !errors.Is(err, errRawHistory) {
				t.Fatalf("PIT=%v before first raw sample: got %v", usePIT, err)
			}
		}
		must("POST", "/_aliases", dsl{"actions": []dsl{{"remove": dsl{"index": backing[1], "alias": historyAlias}}}})
		for _, usePIT := range []bool{false, true} {
			// Characterize the current false acceptance of a window with a gap.
			if got, err := search(usePIT, window.Min); err != nil || got != "new" {
				t.Fatalf("PIT=%v gap behavior changed; re-evaluate this known-issue fixture: got %q, %v", usePIT, got, err)
			}
		}
		t.Log("reproduced: earlier raw data makes both paths accept a window whose missing middle backing index contained the winner")
	})

}
