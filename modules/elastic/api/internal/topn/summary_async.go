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

// Each job pins one original view, including the predecessor of each segment.
// A missing source point is not a failed page and is never persisted as coverage.
func computeMissingSummaries(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time, publish bool) error {
	if publish && m.index == "" {
		return errSummaryTargetMissing
	}
	sealed, edges := summaryPlan(m, q, now)
	s, err := openRawSummary(ctx, call, m.raw, api)
	if err != nil {
		return err
	}
	defer s.close()
	proof, err := summaryProof(ctx, s, q, summaryProofStarts(sealed, edges), true)
	if err != nil {
		return err
	}
	var all []scoreBucket
	if m.index != "" {
		all, err = loadSummaries(ctx, call, api, m, q, metric, sealed, proof)
	}
	// A transient calculation can fill an absent cache from the raw PIT.
	if err != nil && !errors.Is(err, errSummaryMissing) && (publish || !errors.Is(err, errSummaryTargetMissing)) {
		return err
	}
	present := make(map[int64]bool, len(all))
	for _, b := range all {
		present[b.Start] = true
	}
	todo := append([]int64(nil), edges...)
	for _, st := range sealed {
		if !present[st] && proof.transition(st, int64(q.Width)*1000) != "" {
			todo = append(todo, st)
		}
	}
	slices.Sort(todo)
	var newlyRead []scoreBucket
	w := int64(q.Width) * 1000
	for len(todo) > 0 {
		n := 1
		for n < len(todo) && n < summaryBatch && todo[n]-todo[0] < summaryBatch*w {
			n++
		}
		step, cancel := context.WithTimeout(ctx, 8*time.Second)
		rows, err := readTransitions(step, s, q, metric, todo[:n])
		cancel()
		if err != nil {
			return err
		}
		newlyRead = append(newlyRead, rows...)
		todo = todo[n:]
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	all = append(all, newlyRead...)
	if err := summaryRank(q, metric, all); err != nil {
		return err
	}
	if publish && q.AllIndices {
		var eligible []scoreBucket
		for _, b := range newlyRead {
			if slices.Contains(sealed, b.Start) {
				eligible = append(eligible, b)
			}
		}
		if err := publishProvenBuckets(ctx, call, api, m, q, eligible, metric.Item.Key, proof); err != nil {
			log.Warnf("publish on-demand TopN summary: %v", err)
		}
	}
	if metric.rawProof != nil {
		*metric.rawProof = proof.digest()
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

// Lookup copies a finished, unexpired job's immutable lines. A running job is
// reported as absent; the caller joins it through submit below.
func (jobs *summaryQueue) lookup(key string, metric Metric) (bool, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	job := jobs.jobs[key]
	if job == nil {
		return false, nil
	}
	select {
	case <-job.done:
		if time.Now().After(job.expires) {
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

// Submit returns the existing job for the key, or starts one. The caller waits
// on the returned job; the result is also retained for later requests.
func (jobs *summaryQueue) submit(key string, metric Metric, work func(context.Context, Metric) error) (*summaryJob, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if job := jobs.jobs[key]; job != nil {
		select {
		case <-job.done:
			if time.Now().After(job.expires) {
				delete(jobs.jobs, key)
			} else {
				return job, nil
			}
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

func lookupRetainedJob(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time, key string) (bool, error) {
	var expected string
	copyItem := *metric.Item
	cached := metric
	cached.Item = &copyItem
	cached.rawProof = &expected
	found, err := onDemandSummaries.lookup(key, cached)
	if !found || err != nil {
		return found, err
	}
	usable := true
	err = withSlot(func() error {
		s, err := openRawSummary(ctx, call, m.raw, api)
		if err != nil {
			return err
		}
		defer s.close()
		sealed, edges := summaryPlan(m, q, now)
		proof, err := summaryProof(ctx, s, q, summaryProofStarts(sealed, edges), true)
		if err != nil {
			return err
		}
		if expected == "" || expected != proof.digest() {
			onDemandSummaries.mu.Lock()
			job := onDemandSummaries.jobs[key]
			if job != nil {
				select {
				case <-job.done:
					if *job.metric.rawProof == expected {
						delete(onDemandSummaries.jobs, key)
					}
				default:
				}
			}
			onDemandSummaries.mu.Unlock()
			usable = false
			return nil
		}
		metric.Item.Lines = cached.Item.Lines
		return nil
	})
	if !usable {
		return false, nil
	}
	return true, err
}

func canPublishSummary(cfg summaryConfig, q Query, metric Metric) bool {
	return cfg.writes() && q.AllIndices && slices.Contains(cfg.metricKeys(), metric.Item.Key)
}

// readOnDemand tries the stored summaries first. Missing work is computed by
// one background job per window; the request waits for it instead of polling.
func readOnDemand(ctx, waitCtx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time, cfg summaryConfig) error {
	err := withSlot(func() error {
		return readSummary(ctx, call, api, m, q, metric, now)
	})
	if !cfg.writes() {
		return err
	}
	if !errors.Is(err, errSummaryTargetMissing) && !errors.Is(err, errSummaryMissing) {
		return err
	}
	publish := canPublishSummary(cfg, q, metric)
	job, err := onDemandSummaries.submit(summaryJobKey(m, q, metric), metric, func(ctx context.Context, mx Metric) error {
		return computeOnDemand(ctx, call, api, m, q, mx, now, publish)
	})
	if err != nil {
		return err
	}
	select {
	case <-job.done:
	case <-waitCtx.Done():
		// The job keeps running and stays retained; a retry joins it.
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

func computeOnDemand(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time, publish bool) error {
	if publish {
		if err := ensureSummaryIndex(ctx, call, m, api); errors.Is(err, errSummaryNoPolicy) {
			publish = false
		} else if err != nil {
			return err
		}
	}
	return computeMissingSummaries(ctx, call, api, m, q, metric, now, publish)
}
