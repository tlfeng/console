package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/env"
	"infini.sh/framework/core/task"
)

const (
	materializedBatch = 8
	materializedTask  = "console-topn-materialized-v1"
)

func newScoreBucket(start int64) scoreBucket {
	return scoreBucket{Version: materializedVersion, Start: start, Names: []string{}, Scores: []float64{}}
}

// readTransitions builds only requested transitions, using each original interval
// once even when multiple transitions depend on it.
func readTransitions(ctx context.Context, s *snapshot, q Query, metric Metric, starts []int64) ([]scoreBucket, error) {
	sets, err := readTransitionBatches(ctx, s, q, []transitionBatch{{metric, starts}})
	if err != nil {
		return nil, err
	}
	return sets[0], nil
}

func readTransitionBatches(ctx context.Context, s *snapshot, q Query, batches []transitionBatch) ([][]scoreBucket, error) {
	w := int64(q.Width) * 1000
	first := q.Min / w * w
	out := make([][]scoreBucket, len(batches))
	ranges := make(map[int64]dsl)
	for j, batch := range batches {
		out[j] = make([]scoreBucket, len(batch.starts))
		for i, ts := range batch.starts {
			out[j][i] = newScoreBucket(ts)
			if ts < first || ts > q.Max || ts%w != 0 || (i > 0 && batch.starts[i-1] >= ts) {
				return nil, errors.New("invalid TopN transition range")
			}
			for _, start := range []int64{ts - w, ts} {
				if start < 0 || (start < first && start < q.rawFrom) {
					continue
				}
				lo, hi := start, min(q.Max, start+w-1)
				if start >= first {
					lo = max(q.Min, lo)
				}
				ranges[start] = filterRange(lo, hi)
			}
		}
	}
	if len(ranges) == 0 {
		return out, nil
	}
	scopes := make([]dsl, 0, len(ranges))
	for _, r := range ranges {
		scopes = append(scopes, r)
	}
	pred, ok := q.filter()
	if !ok {
		return nil, errUnavailable
	}
	aggs, projections, err := transitionAggs(q, batches)
	if err != nil {
		return nil, err
	}
	scope := dsl{"bool": dsl{"should": scopes, "minimum_should_match": 1}}
	err = visitBucketAggs(ctx, s, q, pred, aggs, scope, 3*len(ranges), func(name string, points []samples) error {
		byTime := make(map[int64]samples, len(points))
		for _, p := range points {
			if p.Time == nil || ranges[*p.Time] == nil || byTime[*p.Time].Time != nil || p.Count <= 0 {
				return errors.New("invalid TopN transition bucket")
			}
			byTime[*p.Time] = p
		}
		for j, batch := range batches {
			projection := projections[j]
			for i, ts := range batch.starts {
				if v, ok := counterPoint(projection.sample(byTime[ts-w]), projection.sample(byTime[ts]), w, batch.metric.mode, false); ok {
					bucket := &out[j][i]
					if len(bucket.Names) >= materializedEntityLimit {
						return errBudget
					}
					bucket.Names = append(bucket.Names, name)
					bucket.Scores = append(bucket.Scores, v)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, set := range out {
		for _, b := range set {
			if !b.valid(b.Start) {
				return nil, errors.New("invalid TopN transition score")
			}
		}
	}
	return out, nil
}

func readMaterialized(ctx context.Context, call caller, api pitAPI, m materializedStore, q Query, metric Metric, now time.Time) error {
	if api.path == "" {
		return errUnavailable
	}
	if !q.AllIndices && len(q.Indices) == 0 {
		return nil
	}
	if q.Max > now.UnixMilli() || !m.covered(q.Min, q.Max) {
		return errSummaryHistory
	}
	// Keep the newest settled bucket on the raw path for two extra ticks,
	// allowing the writer to publish and the index to refresh.
	sealed, edgeSet := materializedScopes(q, now.Add(-materializedLag-2*time.Minute).UnixMilli())
	if len(edgeSet) > 5 {
		return errSummaryHistory
	}
	w := int64(q.Width) * 1000
	for _, st := range sealed {
		if !m.covered(st-w, st+w-1) {
			return errSummaryHistory
		}
	}
	call = budgetedCaller(call, responseLimit)
	var all []scoreBucket
	if len(sealed) > 0 {
		s, err := open(ctx, call, m.index, api)
		if err != nil {
			return err
		}
		defer s.close()
		all, err = readMetricScoreBuckets(ctx, s, q, m, metric.Item.Key, sealed)
		if err != nil {
			return err
		}
	}
	if len(edgeSet) > 0 {
		starts := make([]int64, 0, len(edgeSet))
		for st := q.Min / w * w; st <= q.Max; st += w {
			if edgeSet[st] {
				starts = append(starts, st)
			}
		}
		for _, st := range starts {
			lo := st - w
			if lo < q.Min {
				lo = max(q.Min, lo)
				if q.Min/w*w-w >= q.rawFrom && st == q.Min/w*w {
					lo = q.Min/w*w - w
				}
			}
			if !m.covered(lo, min(st+w-1, q.Max)) {
				return errSummaryHistory
			}
		}
		s, err := open(ctx, call, m.raw, api)
		if err != nil {
			return err
		}
		defer s.close()
		edges, err := readTransitions(ctx, s, q, metric, starts)
		if err != nil {
			return err
		}
		all = append(all, edges...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return materializedRank(q, metric, all)
}

// Only an administrator-provisioned concrete index with this mapping marker
// may be written by the worker. In particular, an alias is not a target.
func checkMaterializedIndex(ctx context.Context, call caller, m materializedStore) error {
	raw, err := call(ctx, http.MethodGet, "/"+m.index+"/_mapping", nil)
	if err != nil {
		return err
	}
	var mapping map[string]struct {
		Mappings struct {
			Meta struct {
				Version int `json:"console_topn"`
			} `json:"_meta"`
		}
	}
	if err = json.Unmarshal(raw, &mapping); err != nil || len(mapping) != 1 || mapping[m.index].Mappings.Meta.Version != materializedVersion {
		return errors.New("unexpected TopN summary index mapping")
	}
	return nil
}

func publishTransitionBatches(ctx context.Context, call caller, api pitAPI, m materializedStore, q Query, batches []transitionBatch) error {
	w := int64(q.Width) * 1000
	if len(batches) == 0 {
		return nil
	}
	for _, batch := range batches {
		for _, st := range batch.starts {
			if !m.covered(st-w, st+w-1) {
				return errSummaryHistory
			}
		}
	}
	s, err := open(ctx, budgetedCaller(call, responseLimit), m.raw, api)
	if err != nil {
		return err
	}
	defer s.close()
	sets, err := readTransitionBatches(ctx, s, q, batches)
	if err != nil {
		return err
	}
	for i, set := range sets {
		for _, bucket := range set {
			path := "/" + m.index + "/_create/" + m.metricID(q, batches[i].metric.Item.Key, bucket.Start)
			body := dsl{"v": bucket.Version, "t": bucket.Start, "n": bucket.Names, "s": bucket.Scores}
			if _, err = call(ctx, http.MethodPut, path, body); err != nil {
				return err
			}
		}
	}
	return nil
}

func missingMaterialized(ctx context.Context, call caller, m materializedStore, q Query, keys []string, starts []int64) (map[string][]int64, error) {
	ids := make([]string, 0, len(starts)*len(keys))
	for _, key := range keys {
		for _, st := range starts {
			ids = append(ids, m.metricID(q, key, st))
		}
	}
	raw, err := call(ctx, http.MethodPost, "/"+m.index+"/_mget?_source=false", dsl{"ids": ids})
	if err != nil {
		return nil, err
	}
	var result struct {
		Docs []struct {
			ID    string `json:"_id"`
			Found *bool  `json:"found"`
		}
	}
	if err = json.Unmarshal(raw, &result); err != nil || len(result.Docs) != len(ids) {
		return nil, errors.New("incomplete TopN summary coverage lookup")
	}
	missing := make(map[string][]int64)
	for i, d := range result.Docs {
		if d.Found == nil || d.ID != ids[i] {
			return nil, errors.New("invalid TopN summary coverage lookup")
		}
		if !*d.Found {
			key := keys[i/len(starts)]
			missing[key] = append(missing[key], starts[i%len(starts)])
		}
	}
	return missing, nil
}

func materializedTick(ctx context.Context, call caller, api pitAPI, m materializedStore, clusters, keys []string, now time.Time) error {
	if api.path == "" {
		return errUnavailable
	}
	call = budgetedCaller(call, 64<<20)
	if err := checkMaterializedIndex(ctx, call, m); err != nil {
		return err
	}
	var batchErrors []error
	for _, cluster := range clusters {
		for _, width := range []int{10800, 14400, 21600, 28800} {
			w := int64(width) * 1000
			end := now.Add(-materializedLag).UnixMilli() / w * w
			begin := max(now.AddDate(0, 0, -materializedDays).UnixMilli()/w*w, (m.from+w-1)/w*w+w)
			q := Query{Cluster: cluster, Source: "index_stats", Width: width, AllIndices: true, rawFrom: m.from}
			var starts []int64
			for st := end - w; st >= begin; st -= w {
				if m.covered(st-w, st+w-1) {
					starts = append(starts, st)
				}
			}
			if len(starts) == 0 {
				continue
			}
			missing, err := missingMaterialized(ctx, call, m, q, keys, starts)
			if err != nil {
				return err
			}
			// Select one bounded recent span across metrics. Each metric publishes
			// only the missing buckets from that span, not existing documents.
			var latest int64
			for _, values := range missing {
				if len(values) > 0 {
					latest = max(latest, values[0])
				}
			}
			if latest == 0 {
				continue
			}
			first := latest - (materializedBatch-1)*w
			var batches []transitionBatch
			for _, key := range keys {
				selected := missing[key]
				for len(selected) > 0 && selected[len(selected)-1] < first {
					selected = selected[:len(selected)-1]
				}
				if len(selected) == 0 {
					continue
				}
				slices.Reverse(selected)
				batches = append(batches, transitionBatch{materializedMetric(key), selected})
			}
			q.Min, q.Max = first, latest+w-1
			if err = publishTransitionBatches(ctx, call, api, m, q, batches); err != nil {
				batchErrors = append(batchErrors, fmt.Errorf("TopN %s/%ds: %w", cluster, width, err))
			}
		}
	}
	return errors.Join(batchErrors...)
}

func StartMaterialized() {
	var cfg config
	if _, err := env.ParseConfig("insight", &cfg); err != nil || !cfg.TopN.Materialized.Writer || len(cfg.TopN.Materialized.Clusters) == 0 {
		return
	}
	if len(cfg.TopN.Materialized.Clusters) > 4 {
		log.Warn("TopN materialization supports at most four configured clusters")
		return
	}
	task.RegisterScheduleTask(task.ScheduleTask{ID: materializedTask, Interval: "1m", Singleton: true, Task: func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		index, from, err := cfg.rawSource()
		if err == nil {
			var m materializedStore
			m, err = cfg.TopN.Materialized.store(index, from)
			if err == nil {
				call, version, _ := monitoredStore()
				if call == nil {
					err = errUnavailable
				} else {
					err = materializedTick(ctx, call, pitFor(version), m, cfg.TopN.Materialized.Clusters, cfg.TopN.Materialized.metricKeys(), time.Now().UTC())
				}
			}
		}
		if err != nil {
			log.Warnf("materialize monitoring TopN: %v", err)
		}
	}})
}
