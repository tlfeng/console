package topn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/util"
	"infini.sh/framework/modules/elastic/common"
)

// This file measures the background chart path against the persistent
// fixture engine (127.0.0.1:19219, cluster console-topn-local-bench,
// easysearch 2.1.2) and its topn-fixture-v2-index-2592000 index: 1,396
// entities, five-minute sampling, exactly 30 days. Run with
// CONSOLE_TOPN_INTEGRATION=1 go test ./modules/elastic/api/internal/topn -run TestLongWindowAsync1396 -count=1 -v

const benchEpoch int64 = 1800000000000
const benchEntities = 1396

func benchHTTP(t *testing.T, method, path string) []byte {
	t.Helper()
	c := http.Client{Timeout: 20 * time.Second}
	r, err := http.NewRequest(method, "http://127.0.0.1:19219"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("%s %s HTTP %d: %v %.400s", method, path, resp.StatusCode, err, raw)
	}
	return raw
}

func benchEngine(t *testing.T) {
	t.Helper()
	var info struct {
		Cluster string `json:"cluster_name"`
		UUID    string `json:"cluster_uuid"`
		Version struct {
			Distribution string
			Number       string
		} `json:"version"`
	}
	if err := json.Unmarshal(benchHTTP(t, http.MethodGet, "/"), &info); err != nil ||
		info.Cluster != "console-topn-local-bench" || info.UUID != "VoDDE4j_T-S7C-sGd906MQ" ||
		info.Version.Distribution != "easysearch" {
		t.Fatalf("unexpected fixture engine: %v", info)
	}
}

type benchTraffic struct {
	searches, bytes atomic.Int64
}

func (f *benchTraffic) call(ctx context.Context, method, path string, body dsl) ([]byte, error) {
	var wire []byte
	if body != nil {
		wire, _ = json.Marshal(body)
	}
	r := util.NewRequest(method, "http://127.0.0.1:19219"+path)
	r.Context = ctx
	r.Body = wire
	r.SetContentType("application/json")
	res, err := util.ExecuteRequest(r)
	if strings.Contains(path, "/_search") {
		f.searches.Add(1)
		if res != nil {
			f.bytes.Add(int64(len(res.Body)))
		}
	}
	if res == nil {
		return nil, err
	}
	if err == nil && res.StatusCode >= 300 {
		return res.Body, fmt.Errorf("HTTP %d: %.600s", res.StatusCode, res.Body)
	}
	return res.Body, err
}

type benchResult struct {
	body   []byte
	status string
}

// serveBench runs one request through the long-window path and reports the
// chart body or the failure status.
func serveBench(q Query, metric Metric, waitCtx context.Context, cfg config, store func() (caller, elastic.Version)) *benchResult {
	w := httptest.NewRecorder()
	serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
	res := &benchResult{body: w.Body.Bytes()}
	var decoded map[string]interface{}
	if err := json.Unmarshal(res.body, &decoded); err == nil {
		if s, ok := decoded["topn_status"].(string); ok {
			res.status = s
		}
	}
	return res
}

// benchCheck verifies the ranking and every point of all N lines against the
// fixture's declared truth: counter rates grow as (entities-entity) per tick
// and latencies equal entity+1, so rank order follows directly.
func benchCheck(t *testing.T, q Query, metric Metric) {
	t.Helper()
	if len(metric.Item.Lines) != q.N {
		t.Fatalf("got %d lines, want %d", len(metric.Item.Lines), q.N)
	}
	w := int64(q.Width) * 1000
	first := q.Min / w * w
	for rank, line := range metric.Item.Lines {
		entity := rank
		if metric.Field2 != "" {
			entity = benchEntities - 1 - rank
		}
		want := fmt.Sprintf("node-%03d", entity)
		if line.Metric.Label != want {
			t.Fatalf("rank %d got %s want %s", rank, line.Metric.Label, want)
		}
		totalTicks := int64((q.Max-benchEpoch)/300000 + 1) // data runs to Max inclusive
		points, ok := line.Data.([][]interface{})
		if !ok || len(points) != int(q.Max/w-q.Min/w)+1 {
			t.Fatalf("%s: got %d points", line.Metric.Label, len(points))
		}
		for j, p := range points {
			ts, okT := p[0].(int64)
			v, okV := p[1].(float64)
			bucketTicks := int64(96)
			if rest := totalTicks - int64(j)*96; rest < 96 {
				bucketTicks = rest
			}
			wantV := float64(bucketTicks) * float64(benchEntities-entity) / float64(q.Width)
			if metric.Field2 != "" {
				wantV = float64(entity + 1)
			}
			if !okT || !okV || ts != first+int64(j)*w || math.Abs(v-wantV) > 1e-9 {
				t.Fatalf("%s point %d got %v want [%d %g ticks=%d]", line.Metric.Label, j, p, first+int64(j)*w, wantV, bucketTicks)
			}
		}
	}
}

func TestLongWindowAsync1396(t *testing.T) {
	if os.Getenv("CONSOLE_TOPN_INTEGRATION") == "" {
		t.Skip("integration test: set CONSOLE_TOPN_INTEGRATION=1 with the fixture engine on 127.0.0.1:19219")
	}
	benchEngine(t)
	const days, width = 30, 28800 // 30 days at the 8-hour default granularity
	seconds := days * 86400
	var count struct{ Count int }
	if err := json.Unmarshal(benchHTTP(t, http.MethodGet, "/topn-fixture-v2-index-2592000/_count"), &count); err != nil || count.Count != 12196852 {
		t.Fatalf("fixture count %d: %v", count.Count, err)
	}
	var cfg config
	cfg.TopN.RawIndex = "topn-fixture-v2-index-2592000"
	version := elastic.Version{Distribution: elastic.Easysearch, Number: "2.1.2"}

	for _, spec := range []struct{ key, field, other string }{
		{"indexing_rate", "write_counter", ""},
		{"query_times", "counter", ""},
		{"indexing_latency", "elapsed", "write_counter"},
		{"query_latency", "elapsed", "counter"},
	} {
		t.Run(spec.key, func(t *testing.T) {
			q := Query{Cluster: "synthetic", Source: "index_stats", AllIndices: true,
				Min: benchEpoch, Max: benchEpoch + int64(seconds)*1000, Width: width, N: 5}
			makeMetric := func() Metric {
				m, ok := NewMetric(&common.MetricItem{Key: spec.key}, spec.field, spec.other, "", "", true)
				if !ok {
					t.Fatal(spec)
				}
				return m
			}

			// Control: the interactive path must still refuse this scale.
			{
				traffic := &benchTraffic{}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				start := time.Now()
				err := calculateWithConfig(ctx, ctx, q, makeMetric(), cfg, func() (caller, elastic.Version) {
					return traffic.call, version
				})
				cancel()
				t.Logf("sync control: err=%v duration=%s searches=%d bytes=%d",
					err, time.Since(start).Round(time.Millisecond), traffic.searches.Load(), traffic.bytes.Load())
				if err == nil {
					t.Fatal("interactive path unexpectedly ranked this scale; the async comparison is meaningless")
				}
			}

			// First request: waits for the background scan and serves the chart.
			{
				traffic := &benchTraffic{}
				metric := makeMetric()
				waitCtx, cancel := context.WithTimeout(context.Background(), queryWaitTimeout(""))
				start := time.Now()
				res := serveBench(q, metric, waitCtx, cfg, func() (caller, elastic.Version) { return traffic.call, version })
				cancel()
				if res.status != "" {
					t.Fatalf("first request got %s", res.status)
				}
				if len(metric.Item.Lines) == 0 {
					t.Fatal("no lines served")
				}
				benchCheck(t, q, metric)
				t.Logf("async first metric=%s duration=%s searches=%d bytes=%d",
					spec.key, time.Since(start).Round(time.Millisecond), traffic.searches.Load(), traffic.bytes.Load())
			}

			// Second request: served from the retained result without reads.
			{
				traffic := &benchTraffic{}
				metric := makeMetric()
				waitCtx, cancel := context.WithTimeout(context.Background(), queryWaitTimeout(""))
				start := time.Now()
				res := serveBench(q, metric, waitCtx, cfg, func() (caller, elastic.Version) { return traffic.call, version })
				cancel()
				if res.status != "" {
					t.Fatalf("repeat request got %s", res.status)
				}
				if len(res.body) == 0 || !strings.Contains(string(res.body), `"lines":[{`) {
					t.Fatalf("repeat request did not serve a chart: %.200s", res.body)
				}
				if n := traffic.searches.Load(); n != 0 {
					t.Fatalf("repeat request re-read the store: %d searches", n)
				}
				t.Logf("cache hit metric=%s duration=%s body=%d bytes", spec.key, time.Since(start).Round(time.Microsecond), len(res.body))
			}

			// Concurrent first refreshes share one job and one result.
			{
				// Cold key with identical semantics: reset the registry so the
				// four readers race for one fresh computation of the same chart.
				charts.Lock()
				charts.jobs = nil
				charts.Unlock()
				traffic := &benchTraffic{}
				var wg sync.WaitGroup
				results := make([]*benchResult, 4)
				begin := time.Now()
				for i := range results {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						waitCtx, cancel := context.WithTimeout(context.Background(), queryWaitTimeout(""))
						defer cancel()
						results[i] = serveBench(q, makeMetric(), waitCtx, cfg, func() (caller, elastic.Version) { return traffic.call, version })
					}(i)
				}
				wg.Wait()
				for i, res := range results {
					if res.status != "" {
						t.Fatalf("reader %d got %s", i, res.status)
					}
					if len(res.body) == 0 || string(res.body) != string(results[0].body) {
						t.Fatalf("reader %d diverged", i)
					}
				}
				if n := traffic.searches.Load(); n < 40 {
					t.Fatalf("four cold readers must share one scan: searches=%d", n)
				}
				t.Logf("concurrent x4 metric=%s duration=%s searches=%d bytes=%d",
					spec.key, time.Since(begin).Round(time.Millisecond), traffic.searches.Load(), traffic.bytes.Load())
			}
		})
	}
}
