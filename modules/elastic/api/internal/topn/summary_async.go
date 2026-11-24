package topn

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"sync"
	"time"

	log "github.com/cihub/seelog"
)

const (
	summaryBatch        = 8
	summaryJobLimit     = 8
	summaryJobTTL       = 2 * time.Minute
	summaryJobTimeout   = time.Minute
	summaryQueueLimit   = 64
	summaryQueueTimeout = 25 * time.Second
	summaryErrorTTL     = 5 * time.Second
)

var (
	errSummaryTargetMissing = errors.New("TopN summary target not found")
	errSummaryBusy          = errors.New("TopN processing capacity is full")
)

func summaryPlan(m summaryStore, q Query, now time.Time) (sealed, edges []int64) {
	full, edgeSet := summaryScopes(q, now.Add(-summaryLag-2*time.Minute).UnixMilli())
	w := int64(q.Width) * 1000
	for _, st := range full {
		if m.covered(st-w, st+w-1) {
			sealed = append(sealed, st)
		}
	}
	first := q.Min / w * w
	for st := first; st <= q.Max; st += w {
		if !edgeSet[st] {
			continue
		}
		lo := st - w
		if st == first+w {
			lo = max(lo, q.Min)
		}
		if m.covered(max(0, lo), min(st+w-1, q.Max)) {
			edges = append(edges, st)
		}
	}
	return sealed, edges
}

func loadSummaries(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, starts []int64, proof rawBucketProof) ([]scoreBucket, error) {
	if len(starts) == 0 {
		return nil, nil
	}
	s, err := open(ctx, call, m.index, api)
	if errors.Is(err, monitoringHTTPError(404)) {
		return nil, errSummaryTargetMissing
	}
	if err != nil {
		return nil, err
	}
	defer s.close()
	return readMetricScoreBuckets(ctx, s, q, m, metric.Item.Key, starts, proof)
}

// computeMissingSummaries fills the sealed buckets that are absent from the
// cache, using the same view the cache was read from. It optionally publishes
// the computed buckets for later requests.
func computeMissingSummaries(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time, publish bool) error {
	if publish && m.index == "" {
		return errSummaryTargetMissing
	}
	sc, err := beginSummary(ctx, call, api, m, q, now)
	if err != nil {
		return err
	}
	defer sc.close()
	all, missing, err := sc.read(ctx, call, api, m, q, metric, !publish)
	if err != nil {
		return err
	}
	slices.Sort(missing)
	var newlyRead []scoreBucket
	width := int64(q.Width) * 1000
	for len(missing) > 0 {
		n := 1
		for n < len(missing) && n < summaryBatch && missing[n]-missing[0] < summaryBatch*width {
			n++
		}
		step, cancel := context.WithTimeout(ctx, 8*time.Second)
		rows, err := readTransitions(step, sc.s, q, metric, missing[:n])
		cancel()
		if err != nil {
			return err
		}
		newlyRead = append(newlyRead, rows...)
		missing = missing[n:]
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	all = append(all, newlyRead...)
	if err := summaryRank(q, metric, all); err != nil {
		return err
	}
	if publish && q.AllIndices {
		if err := publishProvenBuckets(ctx, call, api, m, q, newlyRead, metric.Item.Key, sc.proof); err != nil {
			log.Warnf("publish on-demand TopN summary: %v", err)
		}
	}
	if metric.rawProof != nil {
		*metric.rawProof = sc.proof.digest()
	}
	return nil
}

type summaryJob struct {
	done    chan struct{}
	expires time.Time
	metric  Metric
	err     error
}

type summaryQueue struct {
	mu      sync.Mutex
	jobs    map[string]*summaryJob
	running chan struct{}
}

var onDemandSummaries = summaryQueue{jobs: make(map[string]*summaryJob), running: make(chan struct{}, 1)}

func summaryJobKey(m summaryStore, q Query, metric Metric) string {
	acl := append([]string(nil), q.Indices...)
	if q.AllIndices {
		acl = nil
	}
	sort.Strings(acl)
	values := []interface{}{q.storeScope, m.index, m.raw, q.Cluster, q.Source, q.Min, q.Max, q.Width, q.N, q.AllIndices, acl, metric.Item.Key, metric.Field, metric.Field2}
	for _, g := range m.gaps {
		values = append(values, g.from, g.until)
	}
	key, _ := json.Marshal(values)
	return string(key)
}

// retained copies a finished, unexpired result whose raw view is unchanged.
// Only results that cannot be published are kept in memory; published
// summaries are read back from the store instead.
func (jobs *summaryQueue) retained(key, digest string, metric Metric) (bool, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	job := jobs.jobs[key]
	if job == nil {
		return false, nil
	}
	select {
	case <-job.done:
		if time.Now().After(job.expires) || job.metric.rawProof == nil || *job.metric.rawProof != digest {
			delete(jobs.jobs, key)
			return false, nil
		}
		if job.err == nil {
			metric.Item.Lines = job.metric.Item.Lines
			if metric.rawProof != nil {
				*metric.rawProof = *job.metric.rawProof
			}
		}
		return true, job.err
	default:
		return false, nil
	}
}

// forget drops a finished job that is not a retained read-only result.
func (jobs *summaryQueue) forget(key string, job *summaryJob) {
	jobs.mu.Lock()
	if jobs.jobs[key] == job {
		delete(jobs.jobs, key)
	}
	jobs.mu.Unlock()
}

// Submit returns the running job for this key, or starts one. A finished job
// is not reused: publishable results come from the store, read-only results
// from retained above.
func (jobs *summaryQueue) submit(key string, metric Metric, work func(context.Context, Metric) error) (*summaryJob, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if job := jobs.jobs[key]; job != nil {
		select {
		case <-job.done:
			delete(jobs.jobs, key)
		default:
			return job, nil
		}
	}
	now := time.Now()
	active := 0
	for k, j := range jobs.jobs {
		select {
		case <-j.done:
			if now.After(j.expires) {
				delete(jobs.jobs, k)
			}
		default:
			active++
		}
	}
	if active >= summaryJobLimit || len(jobs.jobs) >= summaryQueueLimit {
		return nil, errSummaryBusy
	}
	item := *metric.Item
	item.Lines = nil
	metric.Item = &item
	metric.rawProof = new(string)
	job := &summaryJob{metric: metric, done: make(chan struct{})}
	jobs.jobs[key] = job
	go func() {
		queued, cancelQueue := context.WithTimeout(context.Background(), summaryQueueTimeout)
		defer cancelQueue()
		defer func() {
			if recover() != nil {
				job.err = errors.New("TopN background query failed")
			}
			ttl := summaryJobTTL
			if job.err != nil {
				ttl = summaryErrorTTL
			}
			job.expires = time.Now().Add(ttl)
			close(job.done)
		}()
		select {
		case jobs.running <- struct{}{}:
			defer func() { <-jobs.running }()
		case <-queued.Done():
			job.err = errSummaryBusy
			return
		}
		if queued.Err() != nil {
			job.err = errSummaryBusy
			return
		}
		cancelQueue()
		ctx, cancel := context.WithTimeout(context.Background(), summaryJobTimeout)
		defer cancel()
		job.err = work(ctx, job.metric)
	}()
	return job, nil
}

func canPublishSummary(cfg summaryConfig, q Query, metric Metric) bool {
	return cfg.writes() && q.AllIndices && slices.Contains(cfg.metricKeys(), metric.Item.Key)
}

// readOnDemand serves the stored summaries and fills missing sealed buckets
// through one background job per raw view. The request joins that job instead
// of polling; results that can be published are then read back from the store.
func readOnDemand(ctx, waitCtx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time, cfg summaryConfig) error {
	key := summaryJobKey(m, q, metric)
	publish := canPublishSummary(cfg, q, metric)
	var all []scoreBucket
	var missing []int64
	var served bool
	err := withSlot(func() error {
		sc, err := beginSummary(ctx, call, api, m, q, now)
		if err != nil {
			return err
		}
		defer sc.close()
		if !publish {
			if found, err := onDemandSummaries.retained(key, sc.digest(), metric); found || err != nil {
				served = true
				return err
			}
		}
		all, missing, err = sc.read(ctx, call, api, m, q, metric, true)
		if err != nil {
			return err
		}
		if publish && sc.missingTarget {
			// Setup did not provision the summary alias. Compute transiently
			// and keep that result in this process instead of attempting writes.
			publish = false
			if found, err := onDemandSummaries.retained(key, sc.digest(), metric); found || err != nil {
				served = true
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if served {
		return nil
	}
	if len(missing) == 0 {
		return summaryRank(q, metric, all)
	}
	if !cfg.Enabled || !cfg.writes() {
		return errSummaryMissing
	}
	job, err := onDemandSummaries.submit(key, metric, func(ctx context.Context, mx Metric) error {
		return computeMissingSummaries(ctx, call, api, m, q, mx, now, publish)
	})
	if err != nil {
		return err
	}
	select {
	case <-job.done:
		if publish {
			// Published results are read back from the store, so the lines are
			// not retained in process memory. A transient fallback is recomputed
			// on the next request.
			onDemandSummaries.forget(key, job)
		}
	case <-waitCtx.Done():
		// The job keeps running; a retry joins it.
		return errSummaryBusy
	}
	if job.err == nil {
		metric.Item.Lines = job.metric.Item.Lines
		if metric.rawProof != nil && job.metric.rawProof != nil {
			*metric.rawProof = *job.metric.rawProof
		}
	}
	return job.err
}
