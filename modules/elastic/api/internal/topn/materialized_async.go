package topn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	log "github.com/cihub/seelog"
)

const (
	summaryJobLimit   = 8
	summaryJobTTL     = 2 * time.Minute
	summaryJobTimeout = time.Minute
	summaryJobBytes   = 96 << 20
)

var (
	errSummaryPending = errors.New("TopN is being calculated")
	errSummaryBusy    = errors.New("TopN background queue is full")
)

func ensureSummaryIndex(ctx context.Context, call caller, m materializedStore) error {
	err := checkMaterializedIndex(ctx, call, m)
	if !errors.Is(err, monitoringHTTPError(404)) {
		return err
	}
	body := dsl{"settings": dsl{"number_of_shards": 1}, "mappings": dsl{"dynamic": false, "_meta": dsl{"console_topn": materializedVersion}, "properties": dsl{"t": dsl{"type": "date", "format": "epoch_millis"}}}}
	if _, err = call(ctx, http.MethodPut, "/"+m.index, body); err != nil {
		// Another initializer may have created it; never overwrite an existing mapping.
		if !errors.Is(err, monitoringHTTPError(400)) && !errors.Is(err, monitoringHTTPError(409)) {
			return err
		}
	}
	return checkMaterializedIndex(ctx, call, m)
}

func summaryPlan(m materializedStore, q Query, now time.Time) (sealed, edges []int64) {
	full, edgeSet := materializedScopes(q, now.Add(-materializedLag-2*time.Minute).UnixMilli())
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

func loadSummaries(ctx context.Context, call caller, api pitAPI, m materializedStore, q Query, metric Metric, starts []int64) ([]scoreBucket, error) {
	if len(starts) == 0 {
		return nil, nil
	}
	s, err := open(ctx, call, m.index, api)
	if err != nil {
		return nil, err
	}
	defer s.close()
	return readMetricScoreBuckets(ctx, s, q, m, metric.Item.Key, starts)
}

// Each job pins one original view, including the predecessor of each segment.
// A missing source point is not a failed page and is never persisted as coverage.
func computeMissingSummaries(ctx context.Context, call caller, api pitAPI, m materializedStore, q Query, metric Metric, now time.Time, publish bool) error {
	call = budgetedCaller(call, summaryJobBytes)
	sealed, edges := summaryPlan(m, q, now)
	all, err := loadSummaries(ctx, call, api, m, q, metric, sealed)
	if err != nil && !errors.Is(err, errSummaryMissing) {
		return err
	}
	present := make(map[int64]bool, len(all))
	for _, b := range all {
		present[b.Start] = true
	}
	todo := append([]int64(nil), edges...)
	for _, st := range sealed {
		if !present[st] {
			todo = append(todo, st)
		}
	}
	slices.Sort(todo)
	var newlyRead []scoreBucket
	if len(todo) > 0 {
		s, err := open(ctx, call, m.raw, api)
		if err != nil {
			return err
		}
		s.responseLimit = summaryJobBytes
		defer s.close()
		for len(todo) > 0 {
			w := int64(q.Width) * 1000
			n := 1
			for n < len(todo) && n < materializedBatch && todo[n]-todo[0] < materializedBatch*w {
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
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	all = append(all, newlyRead...)
	if err := materializedRank(q, metric, all); err != nil {
		return err
	}
	if publish && q.AllIndices {
		for _, b := range newlyRead {
			if len(b.Names) == 0 || !slices.Contains(sealed, b.Start) {
				continue
			}
			path := "/" + m.index + "/_create/" + m.metricID(q, metric.Item.Key, b.Start)
			_, err := call(ctx, http.MethodPut, path, dsl{"v": b.Version, "t": b.Start, "n": b.Names, "s": b.Scores})
			if err != nil && !errors.Is(err, monitoringHTTPError(409)) {
				log.Warnf("publish on-demand TopN summary: %v", err)
				break
			}
		}
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

func summaryJobKey(m materializedStore, q Query, metric Metric) string {
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

// Lookup returns immutable lines only after the job has finished. Each request
// derives the key from the permissions evaluated again by the normal handler.
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
		}
		return true, job.err
	default:
		return true, errSummaryPending
	}
}

func (jobs *summaryQueue) submit(key string, metric Metric, work func(context.Context, Metric) error) error {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if jobs.jobs[key] != nil {
		return errSummaryPending
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
	if active >= summaryJobLimit || len(jobs.jobs) >= 64 {
		return errSummaryBusy
	}
	item := *metric.Item
	item.Lines = nil
	metric.Item = &item
	job := &summaryJob{metric: metric, done: make(chan struct{})}
	jobs.jobs[key] = job
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), summaryJobTimeout)
		defer cancel()
		defer func() {
			if recover() != nil {
				job.err = errors.New("TopN background query failed")
			}
			job.expires = time.Now().Add(summaryJobTTL)
			close(job.done)
		}()
		select {
		case jobs.running <- struct{}{}:
			defer func() { <-jobs.running }()
		case <-ctx.Done():
			job.err = ctx.Err()
			return
		}
		if err := ctx.Err(); err != nil {
			job.err = err
			return
		}
		job.err = work(ctx, job.metric)
	}()
	return errSummaryPending
}

func readOnDemand(ctx context.Context, call caller, api pitAPI, m materializedStore, q Query, metric Metric, now time.Time, writer, poll bool) error {
	if api.path == "" {
		return errUnavailable
	}
	key := summaryJobKey(m, q, metric)
	if poll {
		if found, err := onDemandSummaries.lookup(key, metric); found {
			return err
		}
	}
	err := readMaterialized(ctx, call, api, m, q, metric, now)
	if !poll || !writer || q.Max > now.UnixMilli() {
		return err
	}
	if errors.Is(err, monitoringHTTPError(404)) {
		// Validate or initialize the summary target, not the original data source.
		if initErr := ensureSummaryIndex(ctx, budgetedCaller(call, responseLimit), m); initErr != nil {
			return initErr
		}
		err = readMaterialized(ctx, call, api, m, q, metric, now)
	}
	if !errors.Is(err, errSummaryMissing) {
		return err
	}
	return onDemandSummaries.submit(key, metric, func(ctx context.Context, mx Metric) error {
		if err := checkMaterializedIndex(ctx, budgetedCaller(call, responseLimit), m); err != nil {
			return err
		}
		return computeMissingSummaries(ctx, call, api, m, q, mx, now, true)
	})
}
