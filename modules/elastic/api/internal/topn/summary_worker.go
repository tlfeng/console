package topn

import (
	"context"
	"errors"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/env"
	"infini.sh/framework/core/task"
)

const summaryTaskID = "console-topn-summary-v1"

func newScoreBucket(start int64) scoreBucket {
	return scoreBucket{Version: summaryVersion, Start: start, Names: []string{}, Scores: []float64{}}
}

// readTransitions builds only requested transitions, reading each original
// interval once even when a transition and its predecessor overlap.
func readTransitions(ctx context.Context, s *snapshot, q Query, metric Metric, starts []int64) ([]scoreBucket, error) {
	w := int64(q.Width) * 1000
	first := q.Min / w * w
	out := make([]scoreBucket, len(starts))
	ranges := make(map[int64]dsl)
	for i, ts := range starts {
		out[i] = newScoreBucket(ts)
		if ts < first || ts > q.Max || ts%w != 0 || (i > 0 && starts[i-1] >= ts) {
			return nil, errors.New("invalid TopN transition range")
		}
		for _, start := range []int64{ts - w, ts} {
			if lo, hi, ok := transitionBounds(q, start); ok {
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
	scope := dsl{"bool": dsl{"should": scopes, "minimum_should_match": 1}}
	err := visitBucketAggs(ctx, s, q, pred, metricAggs(q, metric), scope, 3*len(ranges), func(name string, points []samples) error {
		byTime := make(map[int64]samples, len(points))
		for _, p := range points {
			if p.Time == nil || ranges[*p.Time] == nil || byTime[*p.Time].Time != nil || p.Count <= 0 {
				return errors.New("invalid TopN transition bucket")
			}
			byTime[*p.Time] = p
		}
		for i, ts := range starts {
			v, ok, err := counterPoint(byTime[ts-w], byTime[ts], w, metric)
			if err != nil {
				return err
			}
			if ok {
				bucket := &out[i]
				if len(bucket.Names) >= summaryEntityLimit {
					return errBudget
				}
				bucket.Names = append(bucket.Names, name)
				bucket.Scores = append(bucket.Scores, v)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, b := range out {
		if !b.valid(b.Start) {
			return nil, errors.New("invalid TopN transition score")
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

// StartSummary maintains the summary rollover target on the writer instance:
// it provisions the bootstrap generation, binds the raw store's lifecycle
// policy by name, and validates every generation against that binding.
func StartSummary() {
	var cfg config
	if _, err := env.ParseConfig("insight", &cfg); err != nil {
		return
	}
	sc := cfg.TopN.Summary
	if !sc.writes() {
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
					err = ensureSummaryIndex(ctx, call, m, api)
				}
			}
		}
		if err != nil && !errors.Is(err, errSummaryNoPolicy) {
			log.Warnf("maintain TopN summaries: %v", err)
		}
	}})
}
