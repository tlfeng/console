package topn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/modules/elastic/common"
)

func longWindowQuery() Query {
	span := int64(24 * 3600 * 1000) // exactly longWindow
	return Query{Cluster: "biz", Source: "index_stats", AllIndices: true,
		Min: span, Max: 2 * span, Width: 86400, N: 2}
}

// isPITOpen reports the snapshot open request (path carries query parameters).
func isPITOpen(method, path string) bool {
	return method == "POST" && strings.HasPrefix(path, "/raw/_pit?")
}

// peakChartStore serves the four-call snapshot flow of one peak chart.
func peakChartStore(calls *atomic.Int64, release <-chan struct{}) func() (caller, elastic.Version) {
	return func() (caller, elastic.Version) {
		return func(_ context.Context, method, path string, _ dsl) ([]byte, error) {
			n := calls.Add(1)
			switch {
			case isPITOpen(method, path):
				if release != nil {
					<-release
				}
				return []byte(`{"pit_id":"fixed",` + allShards + `}`), nil
			case method == "DELETE":
				return []byte(`{}`), nil
			case n == 2:
				return result(`{"buckets":[{"key":"inactive","peak":{"value":90}},{"key":"active","peak":{"value":0}}]}`), nil
			default:
				return result(`{"buckets":[{"key":"active","dates":{"buckets":[{"key":86400000,"value":{"value":0}}]}},{"key":"inactive","dates":{"buckets":[{"key":86400000,"value":{"value":90}}]}}]}`), nil
			}
		}, elastic.Version{Distribution: elastic.Easysearch, Number: "2.1.2"}
	}
}

func TestLongWindowServedBySingleFlight(t *testing.T) {
	isolateChartRegistry(t)
	var calls atomic.Int64
	store := peakChartStore(&calls, nil)
	var cfg config
	cfg.TopN.RawIndex = "raw"
	q := longWindowQuery()
	q.Cluster = "fingerprint-single-flight"

	bodies := make([][]byte, 3)
	var joined sync.WaitGroup
	start := make(chan struct{})
	for i := range bodies {
		joined.Add(1)
		go func(i int) {
			defer joined.Done()
			<-start
			metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
			w := httptest.NewRecorder()
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
			bodies[i] = w.Body.Bytes()
		}(i)
	}
	close(start)
	joined.Wait()
	for i, body := range bodies {
		if body == nil || bytes.Contains(body, []byte(`topn_status`)) {
			t.Fatalf("request %d did not get a chart: %s", i, body)
		}
	}
	if !bytes.Equal(bodies[0], bodies[1]) || !bytes.Equal(bodies[1], bodies[2]) {
		t.Fatalf("concurrent requests diverged: %s / %s / %s", bodies[0], bodies[1], bodies[2])
	}
	// Every joiner reads the same computed item; the job ran exactly once.
	if got := calls.Load(); got != 4 {
		t.Fatalf("store calls=%d, want 4 (open, rank, curves, close)", got)
	}
	// A later request is served from the retained result without new reads.
	metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	w := httptest.NewRecorder()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
	if calls.Load() != 4 || !bytes.Equal(w.Body.Bytes(), bodies[0]) {
		t.Fatalf("cache miss after completion: calls=%d body=%s", calls.Load(), w.Body)
	}
}

func TestLongWindowBusyThenReused(t *testing.T) {
	isolateChartRegistry(t)
	release := make(chan struct{})
	var calls atomic.Int64
	store := peakChartStore(&calls, release)
	var cfg config
	cfg.TopN.RawIndex = "raw"
	q := longWindowQuery()
	q.Cluster = "fingerprint-busy"

	metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	w := httptest.NewRecorder()
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
	if !bytes.Contains(w.Body.Bytes(), []byte(`"topn_status":"busy"`)) {
		t.Fatalf("unfinished job must answer busy, got %s", w.Body)
	}
	close(release)

	w = httptest.NewRecorder()
	waitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
	if bytes.Contains(w.Body.Bytes(), []byte(`topn_status`)) || calls.Load() != 4 {
		t.Fatalf("finished job must serve the chart: calls=%d body=%s", calls.Load(), w.Body)
	}
}

func requestLongWindow(q Query, cfg config, store func() (caller, elastic.Version)) string {
	metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
	w := httptest.NewRecorder()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
	return w.Body.String()
}

// Keep chart entries (and their done channels) inside the synctest bubble
// that created them. These tests do not run in parallel with other tests.
func isolateChartRegistry(t *testing.T) {
	t.Helper()
	charts.Lock()
	previous := charts.jobs
	charts.jobs = nil
	charts.Unlock()
	t.Cleanup(func() {
		charts.Lock()
		charts.jobs = previous
		charts.Unlock()
	})
}

func TestLongWindowBackgroundAdmission(t *testing.T) {
	isolateChartRegistry(t)
	synctest.Test(t, func(t *testing.T) {
		var cfg config
		cfg.TopN.RawIndex = "raw"
		q1, q2, q3 := longWindowQuery(), longWindowQuery(), longWindowQuery()
		q1.Cluster, q2.Cluster, q3.Cluster = "admission-first", "admission-second", "admission-third"
		var calls1, calls2, calls3 atomic.Int64
		release1, release2 := make(chan struct{}), make(chan struct{})
		defer close(release2)
		var releaseOnce sync.Once
		finishFirst := func() { releaseOnce.Do(func() { close(release1) }) }
		defer finishFirst()
		store1, store2, store3 := peakChartStore(&calls1, release1), peakChartStore(&calls2, release2), peakChartStore(&calls3, nil)

		first, second, joiner := make(chan string, 1), make(chan string, 1), make(chan string, 1)
		go func() { first <- requestLongWindow(q1, cfg, store1) }()
		go func() { second <- requestLongWindow(q2, cfg, store2) }()
		synctest.Wait()
		if calls1.Load() != 1 || calls2.Load() != 1 {
			t.Fatalf("two background scans should be active: store calls=%d, %d", calls1.Load(), calls2.Load())
		}

		go func() { joiner <- requestLongWindow(q1, cfg, store1) }()
		synctest.Wait()
		select {
		case body := <-joiner:
			t.Fatalf("same key must join the running job at capacity, got %s", body)
		default:
		}

		if body := requestLongWindow(q3, cfg, store3); !strings.Contains(body, `"topn_status":"busy"`) || calls3.Load() != 0 {
			t.Fatalf("third background job must be rejected without store reads: calls=%d body=%s", calls3.Load(), body)
		}
		// Both other shared slots must still be available to foreground work.
		if err := withSlot(func() error { return withSlot(func() error { return nil }) }); err != nil {
			t.Fatalf("two shared slots must remain available: %v", err)
		}

		finishFirst()
		body := <-first
		if joined := <-joiner; joined != body || body == "" || strings.Contains(body, `topn_status`) || calls1.Load() != 4 {
			t.Fatalf("joiner must receive the first result without a second scan: calls=%d first=%s joiner=%s", calls1.Load(), body, joined)
		}
		if retry := requestLongWindow(q3, cfg, store3); retry == "" || strings.Contains(retry, `topn_status`) || calls3.Load() != 4 {
			t.Fatalf("rejected key must retry immediately after a slot is freed: calls=%d body=%s", calls3.Load(), retry)
		}
	})
}

func TestLongWindowSharedSlotBusyIsNotCached(t *testing.T) {
	isolateChartRegistry(t)
	synctest.Test(t, func(t *testing.T) {
		var cfg config
		cfg.TopN.RawIndex = "raw"
		q := longWindowQuery()
		q.Cluster = "admission-shared-saturation"
		var calls atomic.Int64
		store := peakChartStore(&calls, nil)

		func() {
			for i := 0; i < cap(slots); i++ {
				slots <- struct{}{}
				defer func() { <-slots }()
			}
			if body := requestLongWindow(q, cfg, store); !strings.Contains(body, `"topn_status":"busy"`) || calls.Load() != 0 {
				t.Fatalf("shared slot saturation must return busy without store reads: calls=%d body=%s", calls.Load(), body)
			}
		}()

		if body := requestLongWindow(q, cfg, store); body == "" || strings.Contains(body, `topn_status`) || calls.Load() != 4 {
			t.Fatalf("shared slot busy must not be retained: calls=%d body=%s", calls.Load(), body)
		}
	})
}

func TestLongWindowFailureNegativeCache(t *testing.T) {
	isolateChartRegistry(t)
	previousTTL := asyncErrorTTL
	asyncErrorTTL = 50 * time.Millisecond
	t.Cleanup(func() { asyncErrorTTL = previousTTL })

	var calls atomic.Int64
	store := func() (caller, elastic.Version) {
		return func(_ context.Context, method, path string, _ dsl) ([]byte, error) {
			calls.Add(1)
			switch {
			case isPITOpen(method, path):
				return []byte(`{"pit_id":"fixed",` + allShards + `}`), nil
			case method == "DELETE":
				return []byte(`{}`), nil
			default:
				// A raw page with a foreign shard layout fails the chart.
				return []byte(`{"_shards":{"total":1,"successful":1,"failed":0},"timed_out":false,"aggregations":{"entities":{"buckets":[]}}}`), nil
			}
		}, elastic.Version{Distribution: elastic.Easysearch, Number: "2.1.2"}
	}
	var cfg config
	cfg.TopN.RawIndex = "raw"
	q := longWindowQuery()
	q.Cluster = "fingerprint-broken"

	run := func() string {
		metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "metric"}
		w := httptest.NewRecorder()
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, store)
		body := w.Body.String()
		if marker := `"topn_status":"`; strings.Contains(body, marker) {
			start := strings.Index(body, marker) + len(marker)
			return body[start : start+strings.IndexByte(body[start:], '"')]
		}
		return ""
	}
	if first := run(); first != "query_failed" {
		t.Fatalf("broken store must fail the chart, got %q", first)
	}
	if second := run(); second != "query_failed" {
		t.Fatalf("negative cache must repeat the status, got %q", second)
	}
	// open, search, close: the negative cache served the second run.
	if got := calls.Load(); got != 3 {
		t.Fatalf("negative cache must not re-read the store: calls=%d, want 3", got)
	}
	time.Sleep(60 * time.Millisecond)
	_ = run()
	if calls.Load() <= 3 {
		t.Fatalf("expired failure must recompute: calls=%d", calls.Load())
	}
}

func TestAsyncPageBudgetExtendsCompositePagination(t *testing.T) {
	q := Query{Cluster: "biz", Source: "index_stats", Min: 1001, Max: 2999, Width: 1, N: 1, AllIndices: true}
	entityPage := func(name string, after bool) []byte {
		transition := func(ts int, v int) string {
			return fmt.Sprintf(`{"key":%d,"doc_count":1,"value":{"value":%d},`+
				`"identity":{"buckets":[{"key":"id","doc_count":1}]}}`, ts, v)
		}
		bucket := fmt.Sprintf(`{"key":{"entity":%q},"dates":{"buckets":[%s,%s]}}`,
			name, transition(1000, 10), transition(2000, 20))
		buckets := `"buckets":[` + bucket + `]`
		if after {
			return result(`{"after_key":{"entity":"` + name + `"},` + buckets + `}`)
		}
		return result(`{` + buckets + `}`)
	}
	run := func(budget int) (error, int) {
		var calls atomic.Int64
		s := snapshot{id: "test", shards: 3, call: func(_ context.Context, _, _ string, _ dsl) ([]byte, error) {
			switch calls.Add(1) {
			case 1:
				return entityPage("a", true), nil
			case 2:
				return entityPage("b", true), nil
			default:
				return entityPage("c", false), nil
			}
		}}
		pred, _ := q.filter()
		m := Metric{Item: &common.MetricItem{Key: "query_times"}, Field: "counter", mode: rate}
		scan := q
		scan.pageBudget = budget
		err := paged(context.Background(), &s, scan, pred, m)
		return err, int(calls.Load())
	}
	if err, calls := run(2); !errors.Is(err, errBudget) || calls != 2 {
		t.Fatalf("budget=2: err=%v calls=%d, want errBudget after 2 pages", err, calls)
	}
	if err, calls := run(3); err != nil || calls != 3 {
		t.Fatalf("budget=3: err=%v calls=%d, want success after 3 pages", err, calls)
	}
}
