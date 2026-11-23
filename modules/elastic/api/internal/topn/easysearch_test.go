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
	"slices"
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

	m := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	if err := peaks(ctx, s, q, pred, m); err != nil {
		t.Fatal(err)
	}
	if len(m.Item.Lines) != 1 || m.Item.Lines[0].Metric.Label != "a" || len(m.Item.Lines[0].Data.([][]interface{})) != 2 {
		t.Fatalf("unexpected ranking: %+v", m.Item.Lines)
	}
	single := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	pred, _ = q.filter()
	if err := singleSearch(ctx, call, alias, pred, q, single); err != nil {
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
		if err := runQuery(ctx, call, pitFor(elastic.Version{Distribution: elastic.Easysearch}), alias, pred, base, auto); err != nil {
			t.Fatal(err)
		}
		if len(auto.Item.Lines) != 2 || auto.Item.Lines[0].Metric.Label != "b" {
			t.Fatalf("unexpected automatic order: %+v", auto.Item.Lines)
		}
		base.selected = selectEntities(base, "a,secret,missing,b,a,_all")
		base.N = 1 // Must not truncate the selected curves.
		pred, _ = base.filter()
		manual := makeMetric()
		if err := singleSearch(ctx, call, alias, pred, base, manual); err != nil {
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
	if err := singleSearch(ctx, call, alias, pred, q, single); err != nil {
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
	// Keep this in one bucket: older code rejected even the manual curve at 101 samples.
	for i := 0; i < 101; i++ {
		must("PUT", "/"+index+fmt.Sprintf("/_doc/dense-disk-%d", i), dsl{
			"timestamp": epoch + 2001, "total": 100, "available": 20,
			"metadata": dsl{"name": "node_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "biz", "transport_address": "small"}},
		})
	}
	must("POST", "/"+index+"/_refresh", nil)
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
			err = singleSearch(ctx, call, alias, pred, q, disk)
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
			if err := singleSearch(ctx, call, alias, pred, q, auto); err != nil {
				t.Fatal(err)
			}
			selected := q
			selected.N = 1
			selected.selected = selectEntities(q, "full,small,missing,full")
			pred, _ = selected.filter()
			if err := runSelected(ctx, call, alias, pred, selected, manual); err != nil {
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
					doc := dsl{
						"timestamp": epoch + row.offset, "metric": row.count, "ops": row.count, "elapsed": row.count * 3,
						"metadata": dsl{"name": "node_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": history, "transport_address": "n1", "node_id": "same-node"}},
						"payload":  dsl{"elasticsearch": dsl{"node_stats": dsl{"jvm": jvm}}},
					}
					must("PUT", fmt.Sprintf("/%s/_doc/restart-%s-%d?refresh=true", index, history, i), doc)
					if row.offset == 79000 {
						// Same identity and lifetime, but unusable operation counts must not hide the valid pair.
						doc["ops"], doc["elapsed"] = []int{900, 999}, 9990
						must("PUT", fmt.Sprintf("/%s/_doc/restart-%s-decoy?refresh=true", index, history), doc)
					}
				}
				restart := Query{Cluster: history, Source: "node_stats", Min: epoch, Max: epoch + 79999, Width: 20, N: 1}
				for _, tc := range []struct {
					key, field, second string
					mode               metricMode
					want               float64
				}{{"query_rate", "metric", "", rate, 2}, {"query_latency", "elapsed", "ops", latency, 3}} {
					selected := restart
					selected.selected = selectEntities(selected, "n1")
					metric := Metric{Item: &common.MetricItem{Key: tc.key}, Field: tc.field, Field2: tc.second, mode: tc.mode}
					pred, _ := selected.filter()
					if err := runSelected(ctx, call, alias, pred, selected, metric); err != nil {
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
						if err := runQuery(ctx, call, api, alias, pred, restart, metric); err != nil {
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
				if err := runQuery(ctx, call, api, alias, pred, q, metric); err != nil {
					t.Fatalf("seconds=%d PIT=%v: optional pre-read must not cause history rejection: %v", tc.seconds, usePIT, err)
				}
				if len(metric.Item.Lines) != 0 {
					t.Fatalf("seconds=%d PIT=%v: sparse data must not produce a fake difference", tc.seconds, usePIT)
				}
			}
		}
	})

	t.Run("reads and deduplicates summary rollover generations", func(t *testing.T) {
		alias := ".console-topn-" + index + "-rollover"
		raw := index
		m := summaryStore{raw: raw, index: alias}
		t.Cleanup(func() {
			for _, name := range []string{alias + "-000001", alias + "-000002"} {
				_, _ = call(context.Background(), "DELETE", "/"+name, nil)
			}
			_, _ = call(context.Background(), "DELETE", "/_index_template/"+alias+"-rollover", nil)
		})
		api := pitFor(elastic.Version{Distribution: elastic.Easysearch})
		mapping := dsl{"dynamic": false, "_meta": dsl{"console_topn": summaryVersion}}
		must("PUT", "/_index_template/"+alias+"-rollover", dsl{"index_patterns": []string{alias + "-*"}, "priority": 1000, "template": dsl{"mappings": mapping}})
		must("PUT", "/"+alias+"-000001", dsl{"mappings": mapping, "aliases": dsl{alias: dsl{"is_write_index": true}}})
		q := Query{Cluster: "biz", Width: 1}
		proof := rawBucketProof{1000: {"one": 1}, 2000: {"one": 1}}
		digest := proof.transition(2000, 1000)
		id := m.provenID(q, writeRateKey, 2000, digest)
		score := dsl{"v": summaryVersion, "t": 2000, "n": []string{"a"}, "s": []float64{3}, "p": digest}
		must("PUT", "/"+alias+"/_create/"+id+"?refresh=true&require_alias=true", score)
		must("POST", "/"+alias+"/_rollover", dsl{})
		must("PUT", "/"+alias+"/_create/"+id+"?refresh=true&require_alias=true", score)
		if err := checkSummaryIndex(ctx, call, m); err != nil {
			t.Fatal(err)
		}
		snap, err := open(ctx, call, alias, api)
		if err != nil {
			t.Fatal(err)
		}
		defer snap.close()
		rows, err := readMetricScoreBuckets(ctx, snap, q, m, writeRateKey, []int64{2000}, proof)
		if err != nil || len(rows) != 1 || rows[0].Scores[0] != 3 {
			t.Fatalf("%v %v", rows, err)
		}
	})

	t.Run("summary scores follow raw backing expiry", func(t *testing.T) {
		old, recent, tail := index+"-expiry-old", index+"-expiry-recent", index+"-expiry-tail"
		alias, summary := index+"-expiry-raw", ".console-topn-"+index+"-expiry-summary"
		for _, name := range []string{old, recent, tail} {
			must("PUT", "/"+name, dsl{"settings": dsl{"number_of_shards": 1, "number_of_replicas": 0}, "aliases": dsl{alias: dsl{}}, "mappings": dsl{"properties": dsl{"timestamp": dsl{"type": "date"}, "metadata": dsl{"properties": dsl{"name": dsl{"type": "keyword"}, "category": dsl{"type": "keyword"}, "labels": dsl{"properties": dsl{"cluster_id": dsl{"type": "keyword"}, "index_name": dsl{"type": "keyword"}, "index_uuid": dsl{"type": "keyword"}}}}}}}})
		}
		t.Cleanup(func() {
			_, _ = call(context.Background(), "DELETE", "/"+tail, nil)
			_, _ = call(context.Background(), "DELETE", "/"+recent, nil)
			_, _ = call(context.Background(), "DELETE", "/"+old, nil)
		})
		must("PUT", "/"+summary, dsl{"settings": dsl{"number_of_shards": 1, "number_of_replicas": 0}, "mappings": dsl{"dynamic": false, "_meta": dsl{"console_topn": summaryVersion}}})
		t.Cleanup(func() { _, _ = call(context.Background(), "DELETE", "/"+summary, nil) })
		for i, r := range []struct {
			target          string
			offset, counter int
		}{{old, 1, 10}, {recent, 2, 5}, {recent, 1001, 20}, {recent, 2001, 25}, {tail, 3001, 27}} {
			must("PUT", fmt.Sprintf("/%s/_doc/%d?refresh=true", r.target, i), dsl{"timestamp": epoch + r.offset, "metric": r.counter, "elapsed": r.counter * 3, "metadata": dsl{"name": "index_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "expiry", "index_name": "a", "index_uuid": "a-uuid"}}})
		}
		m := summaryStore{index: summary, raw: alias}
		q := Query{Cluster: "expiry", Source: "index_stats", Min: epoch + 1000, Max: epoch + 3999, Width: 1, N: 1, AllIndices: true}
		starts := []int64{epoch + 1000, epoch + 2000, epoch + 3000}
		api := pitFor(elastic.Version{Distribution: elastic.Easysearch})
		makeMetric := func() Metric { metric := summaryMetric(writeRateKey); metric.Field = "metric"; return metric }
		now := time.UnixMilli(epoch + 86400000)
		makeLatency := func() Metric {
			metric := summaryMetric(writeLatencyKey)
			metric.Field, metric.Field2 = "elapsed", "metric"
			return metric
		}
		if err := publishTransitionBatches(ctx, call, api, m, q, []transitionBatch{{makeMetric(), starts}, {makeLatency(), starts}}); err != nil {
			t.Fatal(err)
		}
		must("POST", "/"+summary+"/_refresh", nil)
		before := makeMetric()
		if err := readSummary(ctx, call, api, m, q, before, now); err != nil {
			t.Fatal(err)
		}
		if got := before.Item.Lines[0].Data; !reflect.DeepEqual(got, [][]interface{}{{int64(epoch + 1000), 10.0}, {int64(epoch + 2000), 5.0}, {int64(epoch + 3000), 2.0}}) {
			t.Fatal(got)
		}
		paired := makeLatency()
		if err := readSummary(ctx, call, api, m, q, paired, now); err != nil {
			t.Fatal(err)
		}
		wantLatency := [][]interface{}{{int64(epoch + 1000), 3.0}, {int64(epoch + 2000), 3.0}, {int64(epoch + 3000), 3.0}}
		if len(paired.Item.Lines) != 1 || !reflect.DeepEqual(paired.Item.Lines[0].Data, wantLatency) {
			t.Fatalf("incorrect latency summary: %+v", paired.Item.Lines)
		}
		original, err := openRawSummary(ctx, call, alias, api)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := rawSummaryProof(ctx, original, q, starts)
		original.close()
		if err != nil {
			t.Fatal(err)
		}
		digest := proof.digest()
		before.rawProof = &digest
		key := summaryJobKey(m, q, before)
		done := make(chan struct{})
		close(done)
		onDemandSummaries.mu.Lock()
		onDemandSummaries.jobs[key] = &summaryJob{done: done, expires: time.Now().Add(time.Minute), metric: before}
		onDemandSummaries.mu.Unlock()
		t.Cleanup(func() {
			onDemandSummaries.mu.Lock()
			delete(onDemandSummaries.jobs, key)
			onDemandSummaries.mu.Unlock()
		})
		if found, err := lookupRetainedJob(ctx, call, api, m, q, makeMetric(), now, key); !found || err != nil {
			t.Fatalf("valid cached result: %v %v", found, err)
		}
		t.Run("clipped job proof ignores observations outside the window", func(t *testing.T) {
			clipped := q
			clipped.Max = epoch + 3500
			cached := makeMetric()
			cached.rawProof = new(string)
			if err := computeMissingSummaries(ctx, call, api, m, clipped, cached, now, false); err != nil {
				t.Fatal(err)
			}
			key := summaryJobKey(m, clipped, cached)
			onDemandSummaries.mu.Lock()
			onDemandSummaries.jobs[key] = &summaryJob{done: done, expires: time.Now().Add(time.Minute), metric: cached}
			onDemandSummaries.mu.Unlock()
			t.Cleanup(func() {
				onDemandSummaries.mu.Lock()
				delete(onDemandSummaries.jobs, key)
				onDemandSummaries.mu.Unlock()
			})
			addTail := func(id string, offset int) {
				must("PUT", "/"+tail+"/_doc/"+id+"?refresh=true", dsl{"timestamp": epoch + offset, "metric": 30, "metadata": dsl{"name": "index_stats", "category": "elasticsearch", "labels": dsl{"cluster_id": "expiry", "index_name": "a", "index_uuid": "a-uuid"}}})
			}
			addTail("outside", 3800)
			got := makeMetric()
			found, err := lookupRetainedJob(ctx, call, api, m, clipped, got, now, key)
			if !found || err != nil || !reflect.DeepEqual(got.Item.Lines, cached.Item.Lines) {
				t.Fatalf("outside-window sample invalidated result: %v %v", found, err)
			}
			addTail("inside", 3400)
			got = makeMetric()
			if found, err := lookupRetainedJob(ctx, call, api, m, clipped, got, now, key); found || err != nil || len(got.Item.Lines) != 0 {
				t.Fatalf("inside-window sample did not invalidate: %v %v", found, err)
			}
			must("DELETE", "/"+tail+"/_doc/inside?refresh=true", nil)
			must("DELETE", "/"+tail+"/_doc/outside?refresh=true", nil)
		})
		t.Run("verified empty transition is readable", func(t *testing.T) {
			makeEmpty := func() Metric {
				metric := summaryMetric(queryLatencyKey)
				metric.Field = "metric"
				metric.Field2 = "absent_counter"
				return metric
			}
			if err := publishTransitionBatches(ctx, call, api, m, q, []transitionBatch{{makeEmpty(), starts}}); err != nil {
				t.Fatal(err)
			}
			must("POST", "/"+summary+"/_refresh", nil)
			metric := makeEmpty()
			if err := readSummary(ctx, call, api, m, q, metric, now); err != nil || len(metric.Item.Lines) != 0 {
				t.Fatalf("proved empty result: %v %v", err, metric.Item.Lines)
			}
		})
		must("DELETE", "/"+old, nil)
		cached := makeMetric()
		if found, err := lookupRetainedJob(ctx, call, api, m, q, cached, now, key); found || err != nil || len(cached.Item.Lines) != 0 {
			t.Fatalf("expired cached result: %v %v %v", found, err, cached.Item.Lines)
		}
		if err := readSummary(ctx, call, api, m, q, makeMetric(), now); !errors.Is(err, errSummaryMissing) {
			t.Fatalf("partly expired predecessor reused: %v", err)
		}
		stale := newScoreBucket(starts[0])
		stale.Names = []string{"a"}
		stale.Scores = []float64{10}
		denyWrite := func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
			if strings.Contains(path, "/_create/") {
				t.Fatal("stale snapshot must not be published")
			}
			return call(ctx, method, path, body)
		}
		if err := publishProvenBuckets(ctx, denyWrite, api, m, q, [][]scoreBucket{{stale}}, []transitionBatch{{makeMetric(), starts[:1]}}, proof); err != nil {
			t.Fatal(err)
		}
		after := makeMetric()
		if err := computeMissingSummaries(ctx, call, api, m, q, after, now, true); err != nil {
			t.Fatal(err)
		}
		if got := after.Item.Lines[0].Data; !reflect.DeepEqual(got, [][]interface{}{{int64(epoch + 1000), 15.0}, {int64(epoch + 2000), 5.0}, {int64(epoch + 3000), 2.0}}) {
			t.Fatal(got)
		}
		must("DELETE", "/"+recent, nil)
		expired := makeMetric()
		if err := readSummary(ctx, call, api, m, q, expired, now); err != nil || len(expired.Item.Lines) != 0 {
			t.Fatalf("expired points must not rank: %v %v", err, expired.Item.Lines)
		}
		writes := 0
		tracked := func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
			if strings.Contains(path, "/_create/") {
				writes++
			}
			return call(ctx, method, path, body)
		}
		if err := computeMissingSummaries(ctx, tracked, api, m, q, makeMetric(), now, true); err != nil || writes != 0 {
			t.Fatalf("expired buckets must not be written: %v writes=%d", err, writes)
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
			if err := runQuery(ctx, call, api, historyAlias, pred, q, metric); err != nil {
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
		for _, usePIT := range []bool{false, true} {
			if got, err := search(usePIT, epoch-1000); err != nil || got != "middle" {
				t.Fatalf("missing beginning must not reject retained rows: %s %v", got, err)
			}
		}
		for _, engine := range []pitAPI{{}, pitFor(elastic.Version{Distribution: elastic.Easysearch})} {
			empty := window
			empty.Min, empty.Max = epoch-3000, epoch-1000
			pred, _ := empty.filter()
			metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
			if err := runQuery(ctx, call, engine, historyAlias, pred, empty, metric); err != nil || len(metric.Item.Lines) != 0 {
				t.Fatalf("complete empty window: %v %v", err, metric.Item.Lines)
			}
		}
		must("POST", "/_aliases", dsl{"actions": []dsl{{"remove": dsl{"index": backing[1], "alias": historyAlias}}}})
		for _, usePIT := range []bool{false, true} {
			// Rank the retained observations, not the removed backing index.
			if got, err := search(usePIT, window.Min); err != nil || got != "new" {
				t.Fatalf("PIT=%v gap behavior changed; re-evaluate this known-issue fixture: got %q, %v", usePIT, got, err)
			}
		}
		t.Log("both paths rank retained observations after the middle backing index is removed")
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
		key, field, second, name string
		derived                  bool
		value                    float64
		at                       int64
	}{
		{"index_storage", "store", "", "a", false, 50, epoch},
		{"indexing_rate", "counter", "", "b", true, 8, epoch + 1000},
		{"os_used_swap", "counter", "store", "b", false, 40, epoch + 1000},
		{"query_latency", "store", "counter", "a", true, 0, epoch + 1000},
	} {
		q := Query{Cluster: "es68", Source: "index_stats", Min: epoch, Max: epoch + 1999, Width: 1, N: 1, AllIndices: true}
		q.setSearchVersion(version)
		pred, _ := q.filter()
		metric, ok := NewMetric(&common.MetricItem{Key: tc.key}, tc.field, tc.second, "", "", tc.derived)
		if !ok {
			t.Fatal(tc.key)
		}
		if err := runQuery(ctx, call, pitFor(version), index, pred, q, metric); err != nil {
			t.Fatal(err)
		}
		lines := metric.Item.Lines
		if len(lines) != 1 || lines[0].Metric.Label != tc.name {
			t.Fatalf("%s: unexpected winner: %+v", tc.key, lines)
		}
		points := lines[0].Data.([][]interface{})
		if !slices.ContainsFunc(points, func(point []interface{}) bool {
			return reflect.DeepEqual(point, []interface{}{tc.at, tc.value})
		}) {
			t.Fatalf("%s: unexpected points: %v", tc.key, points)
		}
	}
}
