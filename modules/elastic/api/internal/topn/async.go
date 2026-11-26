package topn

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/env"
)

const (
	// asyncMaxPages bounds one background scan's composite pagination. Each
	// page keeps the synchronous size and bucket cost, so no single search is
	// larger than an interactive attempt; only the page count grows, because a
	// wide window over many entities needs more pages than one request may
	// hold open.
	asyncMaxPages = 512
	// asyncJobTimeout bounds one background scan end to end. A finished scan
	// is normally a few seconds; the budget only absorbs a struggling store.
	asyncJobTimeout = 5 * time.Minute
	// chartJobLimit bounds retained chart entries in memory.
	chartJobLimit = 256
)

var (
	// Completed charts are served from memory briefly; failed charts are
	// retried sooner so a transient store error does not stick. Variables so
	// tests can shorten them.
	asyncResultTTL = time.Minute
	asyncErrorTTL  = 5 * time.Second
)

// Background scans also use the shared query slots. Limit new background
// jobs separately so they cannot occupy more than two of the four shared slots.
var asyncSlots = make(chan struct{}, 2)

type chartKey [sha256.Size]byte

// chartEntry is one shared in-memory chart computation. The job goroutine
// writes the fields before closing done; readers only touch them afterwards,
// so the close publishes every write.
type chartEntry struct {
	done           chan struct{}
	holdsAsyncSlot bool
	err            error
	metric         Metric
	expires        time.Time
}

func (e *chartEntry) finished() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

func (e *chartEntry) ready() bool {
	return e.finished() && time.Now().Before(e.expires)
}

type chartRegistry struct {
	sync.Mutex
	jobs map[chartKey]*chartEntry
}

var charts chartRegistry

// acquire returns the entry for key, spawning the computation when none is
// running and no retained result exists. Only new jobs take a background slot;
// if either the registry or the background slots are full, ok is false.
func (reg *chartRegistry) acquire(key chartKey, spawn func(*chartEntry)) (*chartEntry, bool) {
	reg.Lock()
	defer reg.Unlock()
	if reg.jobs == nil {
		reg.jobs = map[chartKey]*chartEntry{}
	}
	if e, ok := reg.jobs[key]; ok {
		if e.ready() || !e.finished() {
			return e, true
		}
		delete(reg.jobs, key)
	}
	for k, e := range reg.jobs {
		if e.finished() && !e.ready() {
			delete(reg.jobs, k)
		}
	}
	if len(reg.jobs) >= chartJobLimit {
		return nil, false
	}
	select {
	case asyncSlots <- struct{}{}:
	default:
		return nil, false
	}
	e := &chartEntry{done: make(chan struct{}), holdsAsyncSlot: true}
	reg.jobs[key] = e
	// The job must never run under the registry lock: a slow computation
	// would otherwise block every other acquire, including its own waiter.
	go spawn(e)
	return e, true
}

// chartFingerprint covers everything that changes the computed chart: the
// query identity, the metric identity, the authorization scope, the engine
// dialect and the trusted raw source.
func chartFingerprint(q Query, metric Metric, index string) chartKey {
	parts := []string{
		q.Cluster, q.Source, metric.Item.Key,
		strconv.FormatInt(q.Min, 10), strconv.FormatInt(q.Max, 10),
		strconv.Itoa(q.Width), strconv.Itoa(q.N),
		strconv.FormatBool(q.AllIndices), strconv.FormatBool(q.legacyInterval),
		metric.Field, metric.Field2, strconv.Itoa(int(metric.mode)),
		index,
	}
	patterns := append([]string(nil), q.Indices...)
	sort.Strings(patterns)
	parts = append(parts, patterns...)
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return chartKey(h)
}

// Serve owns the response for supported Advanced automatic charts. A failed
// calculation must not be presented as a successful legacy activity ranking.
func Serve(w http.ResponseWriter, r *http.Request, q Query, metric Metric) {
	timeout, err := queryTimeout(r.URL.Query().Get("timeout"))
	if err != nil {
		WriteStatus(w, "invalid_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	// Long automatic charts route to the shared background registry; waitCtx
	// bounds how long one request waits for its shared scan instead of the
	// five-second query budget.
	waitCtx, cancelWait := context.WithTimeout(r.Context(), queryWaitTimeout(r.URL.Query().Get("timeout")))
	defer cancelWait()
	w.Header().Set("Cache-Control", "no-store")
	if q.longAutomaticWindow() {
		serveLongWindow(w, q, metric, waitCtx)
		return
	}
	err = calculate(ctx, q, metric)
	if r.Context().Err() != nil {
		return
	}
	if err == nil {
		writeMetrics(w, metric)
		return
	}
	status := failureStatus(err)
	if status == "query_failed" {
		log.Warnf("automatic monitoring TopN: %v", err)
	}
	WriteStatus(w, status)
}

// serveLongWindow serves a long automatic chart from the shared in-memory
// registry. Concurrent and repeated requests reuse one background scan whose
// per-search protections are unchanged. The requesting goroutine waits inside
// its budget; a scan that outlives it answers busy, and the next request
// joins or reads the retained result.
func serveLongWindow(w http.ResponseWriter, q Query, metric Metric, waitCtx context.Context) {
	var cfg config
	if _, err := env.ParseConfig("insight", &cfg); err != nil {
		WriteStatus(w, failureStatus(errUnavailable))
		return
	}
	serveLongWindowWithConfig(w, q, metric, waitCtx, cfg, monitoredStore)
}

func serveLongWindowWithConfig(w http.ResponseWriter, q Query, metric Metric, waitCtx context.Context, cfg config, store func() (caller, elastic.Version)) {
	index, err := cfg.rawSource()
	if err != nil {
		WriteStatus(w, failureStatus(err))
		return
	}
	if q.Max/(int64(q.Width)*1000)-q.Min/(int64(q.Width)*1000)+1 > 512 {
		WriteStatus(w, failureStatus(errBudget))
		return
	}
	key := chartFingerprint(q, metric, index)
	entry, ok := charts.acquire(key, func(e *chartEntry) {
		runChartJob(e, q, metric, cfg, index, store)
	})
	if !ok {
		WriteStatus(w, failureStatus(errBusy))
		return
	}
	select {
	case <-entry.done:
		if entry.err != nil {
			if failureStatus(entry.err) == "query_failed" {
				log.Warnf("automatic monitoring TopN: %v", entry.err)
			}
			WriteStatus(w, failureStatus(entry.err))
			return
		}
		writeMetrics(w, entry.metric)
	case <-waitCtx.Done():
		// A disconnected client gets no response; the job keeps running.
		if waitCtx.Err() == context.Canceled {
			return
		}
		WriteStatus(w, failureStatus(errBusy))
	}
}

// runChartJob computes one chart outside any request context. It uses the
// same bounded raw path; only the page budget and the deadline grow, so no
// single search is larger than an interactive attempt.
func runChartJob(entry *chartEntry, q Query, metric Metric, cfg config, index string, store func() (caller, elastic.Version)) {
	defer close(entry.done)
	if entry.holdsAsyncSlot {
		// Return the slot before waking waiters so an immediate retry can enter.
		defer func() { <-asyncSlots }()
	}
	scan := q
	scan.pageBudget = asyncMaxPages
	jobCtx, cancel := context.WithTimeout(context.Background(), asyncJobTimeout)
	defer cancel()
	err := calculateWithConfig(jobCtx, scan, metric, cfg, store)
	entry.metric = metric
	entry.err = err
	if errors.Is(err, errBusy) {
		// Publish busy to current waiters, but let the next acquire retry.
		return
	}
	if err != nil {
		entry.expires = time.Now().Add(asyncErrorTTL)
		return
	}
	entry.expires = time.Now().Add(asyncResultTTL)
}
