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
	summaryBatch  = 8
	summaryTaskID = "console-topn-summary-v1"
)

func newScoreBucket(start int64) scoreBucket {
	return scoreBucket{Version: summaryVersion, Start: start, Names: []string{}, Scores: []float64{}}
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
				if lo, hi, ok := transitionBounds(q, start); ok {
					ranges[start] = filterRange(lo, hi)
				}
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
				a, b := byTime[ts-w].project(batch.metric, projection), byTime[ts].project(batch.metric, projection)
				v, ok, err := counterPoint(a, b, w, batch.metric, false)
				if err != nil {
					return err
				}
				if ok {
					bucket := &out[j][i]
					if len(bucket.Names) >= summaryEntityLimit {
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

func readSummary(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, now time.Time) error {
	if api.path == "" {
		return errUnavailable
	}
	if !q.AllIndices && len(q.Indices) == 0 {
		return nil
	}
	if q.Max > now.UnixMilli() {
		return errSummaryHistory
	}
	sealed, edges := summaryPlan(m, q, now)
	if len(edges) > 5 {
		return errBudget
	}
	s, err := openRawSummary(ctx, call, m.raw, api)
	if err != nil {
		return err
	}
	defer s.close()
	proof, err := rawSummaryProof(ctx, s, q, sealed)
	if err != nil {
		return err
	}
	all, err := loadSummaries(ctx, call, api, m, q, metric, sealed, proof)
	if err != nil {
		return err
	}
	if len(edges) > 0 {
		scores, err := readTransitions(ctx, s, q, metric, edges)
		if err != nil {
			return err
		}
		all = append(all, scores...)
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	return summaryRank(q, metric, all)
}

func publishTransitionBatches(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, batches []transitionBatch) error {
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
	s, err := openRawSummary(ctx, call, m.raw, api)
	if err != nil {
		return err
	}
	defer s.close()
	var starts []int64
	for _, batch := range batches {
		starts = append(starts, batch.starts...)
	}
	proof, err := rawSummaryProof(ctx, s, q, starts)
	if err != nil {
		return err
	}
	sets, err := readTransitionBatches(ctx, s, q, batches)
	if err != nil {
		return err
	}
	return publishProvenBuckets(ctx, call, api, m, q, sets, batches, proof)
}

func missingSummaries(ctx context.Context, call caller, m summaryStore, q Query, keys []string, starts []int64, proof rawBucketProof) (map[string][]int64, error) {
	eligible := make([]int64, 0, len(starts))
	for _, st := range starts {
		if proof.transition(st, int64(q.Width)*1000) != "" {
			eligible = append(eligible, st)
		}
	}
	starts = eligible
	if len(starts) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(starts)*len(keys))
	for _, key := range keys {
		for _, st := range starts {
			ids = append(ids, m.provenID(q, key, st, proof.transition(st, int64(q.Width)*1000)))
		}
	}
	found := make(map[string]bool, len(ids))
	for offset := 0; offset < len(ids); offset += 128 {
		chunk := ids[offset:min(len(ids), offset+128)]
		body := dsl{"size": len(chunk) * 32, "track_total_hits": true, "_source": false, "query": dsl{"ids": dsl{"values": chunk}}}
		if err := searchTimeout(ctx, body); err != nil {
			return nil, err
		}
		raw, err := call(ctx, http.MethodPost, "/"+m.index+"/_search?allow_partial_search_results=false", body)
		if err != nil {
			return nil, err
		}
		var result struct {
			searchResponse
			Hits struct {
				Total struct {
					Value    int    `json:"value"`
					Relation string `json:"relation"`
				} `json:"total"`
				Rows []struct {
					ID string `json:"_id"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err = json.Unmarshal(raw, &result); err != nil {
			return nil, err
		}
		if !result.complete() || result.Origin != "" || result.Hits.Rows == nil || result.Hits.Total.Relation != "eq" || result.Hits.Total.Value != len(result.Hits.Rows) {
			return nil, errors.New("incomplete TopN summary coverage lookup")
		}
		for _, row := range result.Hits.Rows {
			if !slices.Contains(chunk, row.ID) {
				return nil, errors.New("invalid TopN summary coverage lookup")
			}
			found[row.ID] = true
		}
	}
	missing := make(map[string][]int64)
	for i, id := range ids {
		if !found[id] {
			key := keys[i/len(starts)]
			missing[key] = append(missing[key], starts[i%len(starts)])
		}
	}

	return missing, nil
}

func summaryTick(ctx context.Context, call caller, api pitAPI, m summaryStore, clusters, keys []string, now time.Time) error {
	if api.path == "" {
		return errUnavailable
	}
	if err := checkSummaryIndex(ctx, call, m); err != nil {
		return err
	}
	var batchErrors []error
	for _, cluster := range clusters {
		for _, width := range []int{10800, 14400, 21600, 28800} {
			w := int64(width) * 1000
			end := now.Add(-summaryLag).UnixMilli() / w * w
			begin := max(now.AddDate(0, 0, -summaryDays).UnixMilli()/w*w, w)
			q := Query{Cluster: cluster, Source: "index_stats", Width: width, AllIndices: true}
			var starts []int64
			for st := end - w; st >= begin; st -= w {
				if m.covered(st-w, st+w-1) {
					starts = append(starts, st)
				}
			}
			if len(starts) == 0 {
				continue
			}
			rawSnapshot, err := openRawSummary(ctx, call, m.raw, api)
			if err != nil {
				return err
			}
			proof, err := rawSummaryProof(ctx, rawSnapshot, q, starts)
			rawSnapshot.close()
			if err != nil {
				return err
			}
			missing, err := missingSummaries(ctx, call, m, q, keys, starts, proof)
			if err != nil {
				return err
			}
			// Keep the script-free rates independent of latency script failures.
			var rateBatches, latencyBatches []transitionBatch
			for _, key := range keys {
				selected := missing[key]
				if len(selected) == 0 {
					continue
				}
				batch := transitionBatch{summaryMetric(key), selected}
				if batch.metric.mode == latency {
					latencyBatches = append(latencyBatches, batch)
				} else {
					rateBatches = append(rateBatches, batch)
				}
			}
			for _, batches := range [][]transitionBatch{rateBatches, latencyBatches} {
				if len(batches) == 0 {
					continue
				}
				var latest int64
				for _, batch := range batches {
					latest = max(latest, batch.starts[0])
				}
				first := latest - (summaryBatch-1)*w
				selected := batches[:0]
				for _, batch := range batches {
					for len(batch.starts) > 0 && batch.starts[len(batch.starts)-1] < first {
						batch.starts = batch.starts[:len(batch.starts)-1]
					}
					if len(batch.starts) > 0 {
						slices.Reverse(batch.starts)
						selected = append(selected, batch)
					}
				}
				q.Min, q.Max = first, latest+w-1
				if err = publishTransitionBatches(ctx, call, api, m, q, selected); err != nil {
					batchErrors = append(batchErrors, fmt.Errorf("TopN %s/%ds: %w", cluster, width, err))
				}
			}
		}
	}
	return errors.Join(batchErrors...)
}

func (c summaryConfig) validateBackfillClusters() error {
	if !c.Enabled && len(c.Clusters) > 4 {
		return errors.New("TopN scheduled backfill supports at most four configured clusters")
	}
	return nil
}

func StartSummary() {
	var cfg config
	if _, err := env.ParseConfig("insight", &cfg); err != nil {
		return
	}
	sc := cfg.TopN.Summary
	if !sc.writes() || (!sc.Enabled && len(sc.Clusters) == 0) {
		return
	}
	if err := sc.validateBackfillClusters(); err != nil {
		log.Warn(err)
		return
	}
	task.RegisterScheduleTask(task.ScheduleTask{ID: summaryTaskID, Interval: "1m", Singleton: true, Task: func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		index, err := cfg.rawSource()
		if err == nil {
			var m summaryStore
			m, err = sc.store(index)
			if err == nil {
				call, version, _ := monitoredStore()
				if call == nil {
					err = errUnavailable
				} else if api := pitFor(version); api.path != "" {
					if sc.Enabled {
						err = ensureSummaryIndex(ctx, call, m, api)
					} else {
						err = summaryTick(ctx, call, api, m, sc.Clusters, sc.metricKeys(), time.Now().UTC())
					}
				}
			}
		}
		if err != nil && !errors.Is(err, errSummaryNoPolicy) {
			log.Warnf("maintain TopN summaries: %v", err)
		}
	}})
}
