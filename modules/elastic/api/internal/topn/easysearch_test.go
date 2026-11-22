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
	"strings"
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
	if err := singleSearch(ctx, call, alias, pred, q, single, true); err != nil {
		t.Fatal(err)
	}
	if len(single.Item.Lines) != 1 || single.Item.Lines[0].Metric.Label != "b" || !reflect.DeepEqual(single.Item.Lines[0].Data, [][]interface{}{{int64(epoch + 1000), 50.0}, {int64(epoch + 2000), 999.0}}) {
		t.Fatalf("expected single search to see the late write, got %+v", single.Item.Lines)
	}

	t.Run("selected indexing rate matches automatic points", func(t *testing.T) {
		for _, p := range []struct {
			id, name  string
			ts, value int
		}{
			{"a0", "a", 1, 10}, {"a4", "a", 4001, 75}, {"a5", "a", 5001, 80},
		} {
			add(p.id, p.name, p.ts, p.value)
		}
		base := Query{Cluster: "biz", Source: "index_stats", Min: epoch + 1001, Max: epoch + 5999, Width: 1, N: 2, Indices: []string{"a", "b", "missing"}}
		makeMetric := func() Metric {
			return Metric{Item: &common.MetricItem{Key: "indexing_rate"}, Field: "metric", mode: rate}
		}
		pred, _ := base.filter()
		auto := makeMetric()
		if err := runQuery(ctx, call, pitFor(elastic.Version{Distribution: elastic.Easysearch}), alias, pred, base, auto, true); err != nil {
			t.Fatal(err)
		}
		if len(auto.Item.Lines) != 2 || auto.Item.Lines[0].Metric.Label != "b" {
			t.Fatalf("unexpected automatic order: %+v", auto.Item.Lines)
		}
		base.selected = selectEntities(base, "a,secret,missing,b,a,_all")
		base.N = 1 // Must not truncate the selected curves.
		pred, _ = base.filter()
		manual := makeMetric()
		if err := singleSearch(ctx, call, alias, pred, base, manual, true); err != nil {
			t.Fatal(err)
		}
		if len(manual.Item.Lines) != 2 || manual.Item.Lines[0].Metric.Label != "a" || manual.Item.Lines[1].Metric.Label != "b" {
			t.Fatalf("unexpected manual selection: %+v", manual.Item.Lines)
		}
		for i, line := range manual.Item.Lines {
			if !reflect.DeepEqual(line.Data, auto.Item.Lines[1-i].Data) {
				t.Fatalf("%s: manual %v != automatic %v", line.Metric.Label, line.Data, auto.Item.Lines[1-i].Data)
			}
		}
		want := [][]interface{}{{int64(epoch + 1000), 0.0}, {int64(epoch + 2000), 40.0}, {int64(epoch + 3000), nil}, {int64(epoch + 5000), 5.0}}
		if !reflect.DeepEqual(manual.Item.Lines[0].Data, want) {
			t.Fatalf("missing pre-read, zero or gap: got %v want %v", manual.Item.Lines[0].Data, want)
		}
	})

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
	if err := singleSearch(ctx, call, alias, pred, q, single, false); err != nil {
		t.Fatal(err)
	}
	if len(single.Item.Lines) != len(m.Item.Lines) || single.Item.Lines[0].Metric.Label != m.Item.Lines[0].Metric.Label || !reflect.DeepEqual(single.Item.Lines[0].Data, m.Item.Lines[0].Data) {
		t.Fatal("single search and PIT pagination disagree")
	}

	for i, row := range []struct {
		name             string
		total, available int
	}{{"large", 1000, 100}, {"small", 100, 90}, {"small", 200, 100}, {"middle", 100, 60}, {"full", 50, 0}} {
		must("PUT", "/"+index+fmt.Sprintf("/_doc/disk-%d?refresh=true", i), dsl{
			"timestamp": epoch + 2001, "total": row.total, "available": row.available,
			"metadata": dsl{"name": "node_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "biz", "transport_address": row.name}},
		})
	}
	q.Source, q.N = "node_stats", 4
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
			err = singleSearch(ctx, call, alias, pred, q, disk, false)
		}
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range []struct {
			name  string
			score float64
		}{{"small", 90}, {"middle", 60}, {"large", 10}, {"full", 0}} {
			if len(disk.Item.Lines) != 4 || disk.Item.Lines[i].Metric.Label != want.name || !reflect.DeepEqual(disk.Item.Lines[i].Data, [][]interface{}{{int64(epoch + 2000), want.score}}) {
				t.Fatalf("PIT=%v: incorrect disk ranking or points", usePIT)
			}
		}
	}

	t.Run("selected node gauges and ratios", func(t *testing.T) {
		for _, key := range []string{"cpu", "disk"} {
			makeMetric := func() Metric {
				if key == "disk" {
					metric, _ := NewMetric(&common.MetricItem{Key: key}, "total", "available", "%", "ratio", false)
					return metric
				}
				return Metric{Item: &common.MetricItem{Key: key}, Field: "available"}
			}
			auto, manual := makeMetric(), makeMetric()
			pred, _ := q.filter()
			if err := singleSearch(ctx, call, alias, pred, q, auto, false); err != nil {
				t.Fatal(err)
			}
			selected := q
			selected.N = 1
			selected.selected = selectEntities(q, "full,small,missing,full")
			pred, _ = selected.filter()
			if err := runSelected(ctx, call, alias, pred, selected, manual, false); err != nil {
				t.Fatal(err)
			}
			if len(manual.Item.Lines) != 2 {
				t.Fatalf("%s: missing selected curves", key)
			}
			for i, name := range []string{"full", "small"} {
				line := manual.Item.Lines[i]
				if line.Metric.Label != name {
					t.Fatalf("%s: got %s, want %s", key, line.Metric.Label, name)
				}
				matched := false
				for _, other := range auto.Item.Lines {
					if other.Metric.Label == name && reflect.DeepEqual(other.Data, line.Data) {
						matched = true
					}
				}
				if !matched {
					t.Fatalf("%s: %s disagrees with automatic curve", key, name)
				}
			}
		}
	})

	t.Run("restart inside bucket", func(t *testing.T) {
		for _, history := range []string{"legacy", "mixed", "summary"} {
			t.Run(history, func(t *testing.T) {
				// One address and node ID, but two JVM lifetimes in the middle bucket.
				for i, row := range []struct{ offset, uptime, count int }{
					{1000, 1000, 10}, {20100, 20100, 50}, {24000, 100, 1}, {59000, 35100, 150}, {79000, 55100, 190},
				} {
					jvm := dsl{"timestamp": epoch + row.offset, "uptime_in_millis": row.uptime}
					if history == "summary" || (history == "mixed" && i%2 == 0) {
						jvm["start_time_estimate_in_millis"] = epoch + int64(row.offset-row.uptime)
					}
					must("PUT", fmt.Sprintf("/%s/_doc/restart-%s-%d?refresh=true", index, history, i), dsl{
						"timestamp": epoch + row.offset, "metric": row.count, "elapsed": row.count * 3,
						"metadata": dsl{"name": "node_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": history, "transport_address": "n1", "node_id": "same-node"}},
						"payload":  dsl{"elasticsearch": dsl{"node_stats": dsl{"jvm": jvm}}},
					})
				}
				restart := Query{Cluster: history, Source: "node_stats", Min: epoch, Max: epoch + 79999, Width: 20, N: 1}
				for _, tc := range []struct {
					key, field, second string
					mode               metricMode
					want               float64
				}{{"query_rate", "metric", "", rate, 2}, {"query_latency", "elapsed", "metric", latency, 3}} {
					selected := restart
					selected.selected = selectEntities(selected, "n1")
					metric := Metric{Item: &common.MetricItem{Key: tc.key}, Field: tc.field, Field2: tc.second, mode: tc.mode}
					pred, _ := selected.filter()
					if err := runSelected(ctx, call, alias, pred, selected, metric, false); err != nil {
						t.Fatal(err)
					}
					if len(metric.Item.Lines) != 1 || !reflect.DeepEqual(metric.Item.Lines[0].Data, [][]interface{}{{int64(epoch + 60000), tc.want}}) {
						t.Fatalf("%s selected: wrong recovery point", tc.key)
					}
					for _, usePIT := range []bool{false, true} {
						pred, _ := restart.filter()
						metric := Metric{Item: &common.MetricItem{Key: tc.key}, Field: tc.field, Field2: tc.second, mode: tc.mode}
						var api pitAPI
						if usePIT {
							api = pitFor(elastic.Version{Distribution: elastic.Easysearch})
						}
						if err := runQuery(ctx, call, api, alias, pred, restart, metric, false); err != nil {
							t.Fatal(err)
						}
						if len(metric.Item.Lines) != 1 || !reflect.DeepEqual(metric.Item.Lines[0].Data, [][]interface{}{{int64(epoch + 60000), tc.want}}) {
							t.Fatalf("%s PIT=%v: expected only the recovery point at %v, got %+v", tc.key, usePIT, tc.want, metric.Item.Lines)
						}
					}
				}
			})
		}
	})

	t.Run("raw history starts after optional pre-read", func(t *testing.T) {
		for _, tc := range []struct{ seconds, width int }{{900, 20}, {86400, 600}, {7 * 86400, 5400}} {
			for _, usePIT := range []bool{false, true} {
				q := Query{Cluster: "biz", Source: "index_stats", Min: epoch + 1001, Max: epoch + 1001 + int64(tc.seconds)*1000, Width: tc.width, N: 1, Indices: []string{"a"}}
				pred, _ := q.filter()
				metric := Metric{Item: &common.MetricItem{Key: "query_times"}, Field: "metric", mode: rate}
				var api pitAPI
				if usePIT {
					api = pitFor(elastic.Version{Distribution: elastic.Easysearch})
				}
				if err := runQuery(ctx, call, api, alias, pred, q, metric, true); err != nil {
					t.Fatalf("seconds=%d PIT=%v: optional pre-read must not cause history rejection: %v", tc.seconds, usePIT, err)
				}
				if len(metric.Item.Lines) != 0 {
					t.Fatalf("seconds=%d PIT=%v: sparse data must not produce a fake difference", tc.seconds, usePIT)
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
			if err := runQuery(ctx, call, api, historyAlias, pred, q, metric, true); err != nil {
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
		}
		// A window ending before the first sample still rejects.
		if _, err := search(false, epoch-1000); !errors.Is(err, errRawHistory) {
			t.Fatalf("window predating raw evidence: got %v", err)
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

// Verify the actual 6.8 search parser, not just the generated DSL.
func TestElasticsearch68SingleSearch(t *testing.T) {
	if os.Getenv("CONSOLE_TOPN_ES68_INTEGRATION") != "1" {
		t.Skip("set CONSOLE_TOPN_ES68_INTEGRATION=1 for disposable Elasticsearch 6.8 on localhost:19220")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	call := func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:19220"+path, bytes.NewReader(b))
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
	must := func(method, path string, body dsl) []byte {
		t.Helper()
		raw, err := call(ctx, method, path, body)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	var info struct {
		Cluster string `json:"cluster_name"`
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if json.Unmarshal(must("GET", "/", nil), &info) != nil || info.Cluster != "console-topn-local-bench" || !strings.HasPrefix(info.Version.Number, "6.8.") {
		t.Fatal("not the disposable Elasticsearch 6.8 test cluster")
	}
	index := fmt.Sprintf(".console-topn-es68-%d", time.Now().UnixNano())
	must("PUT", "/"+index, dsl{"settings": dsl{"number_of_shards": 2, "number_of_replicas": 0}, "mappings": dsl{"_doc": dsl{"properties": dsl{
		"timestamp": dsl{"type": "date"},
		"metadata": dsl{"properties": dsl{"category": dsl{"type": "keyword"}, "name": dsl{"type": "keyword"}, "labels": dsl{"properties": dsl{
			"cluster_id": dsl{"type": "keyword"}, "index_name": dsl{"type": "keyword"}, "index_uuid": dsl{"type": "keyword"},
		}}}},
	}}}})
	defer func() { _, _ = call(ctx, "DELETE", "/"+index, nil) }()
	const epoch int64 = 1780000000000
	for _, row := range []struct {
		name            string
		offset          int64
		stored, counter int
	}{{"a", 0, 50, 0}, {"a", 1000, 50, 1}, {"b", 0, 20, 0}, {"b", 1000, 20, 8}} {
		must("POST", "/"+index+"/_doc?refresh=true", dsl{"timestamp": epoch + row.offset, "store": row.stored, "counter": row.counter, "metadata": dsl{"category": "elasticsearch", "name": "index_stats", "labels": dsl{"cluster_id": "es68", "index_name": row.name, "index_uuid": row.name}}})
	}
	version := elastic.Version{Distribution: elastic.Elasticsearch, Number: info.Version.Number}
	for _, tc := range []struct {
		key, field, name string
		derived          bool
		value            float64
		at               int64
	}{{"index_storage", "store", "a", false, 50, epoch}, {"indexing_rate", "counter", "b", true, 8, epoch + 1000}} {
		q := Query{Cluster: "es68", Source: "index_stats", Min: epoch, Max: epoch + 1999, Width: 1, N: 1, AllIndices: true}
		q.setSearchVersion(version)
		pred, _ := q.filter()
		metric, ok := NewMetric(&common.MetricItem{Key: tc.key}, tc.field, "", "", "", tc.derived)
		if !ok {
			t.Fatal(tc.key)
		}
		if err := runQuery(ctx, call, pitFor(version), index, pred, q, metric, true); err != nil {
			t.Fatal(err)
		}
		lines := metric.Item.Lines
		if len(lines) != 1 || lines[0].Metric.Label != tc.name {
			t.Fatalf("%s: unexpected winner: %+v", tc.key, lines)
		}
		points := lines[0].Data.([][]interface{})
		if len(points) == 0 || !reflect.DeepEqual(points[0], []interface{}{tc.at, tc.value}) {
			t.Fatalf("%s: unexpected points: %v", tc.key, points)
		}
	}
}
