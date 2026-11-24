package topn

import (
	"context"
	"errors"
	"time"
)

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

// A summaryScan is one validated raw view of the requested window. The proof
// binds both the cached summaries and the raw transitions to the same backing
// indices, so a read never mixes generations.
type summaryScan struct {
	s             *snapshot
	proof         rawBucketProof
	sealed        []int64
	edges         []int64
	missingTarget bool
}

func beginSummary(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, now time.Time) (*summaryScan, error) {
	if q.Max > now.UnixMilli() {
		return nil, errSummaryHistory
	}
	sealed, edges := summaryPlan(m, q, now)
	if len(edges) > 5 {
		return nil, errBudget
	}
	s, err := openRawSummary(ctx, call, m.raw, api)
	if err != nil {
		return nil, err
	}
	proof, err := summaryProof(ctx, s, q, summaryProofStarts(sealed, edges), true)
	if err != nil {
		s.close()
		return nil, err
	}
	return &summaryScan{s: s, proof: proof, sealed: sealed, edges: edges}, nil
}

func (sc *summaryScan) close() { sc.s.close() }

func (sc *summaryScan) digest() string { return sc.proof.digest() }

// read returns the buckets this view can serve and the sealed buckets that
// are still missing. Edges always come from raw. A missing summary index is
// tolerated only when the caller can compute the buckets instead.
func (sc *summaryScan) read(ctx context.Context, call caller, api pitAPI, m summaryStore, q Query, metric Metric, tolerateMissingTarget bool) ([]scoreBucket, []int64, error) {
	var all []scoreBucket
	if m.index != "" && len(sc.sealed) > 0 {
		cached, err := loadSummaries(ctx, call, api, m, q, metric, sc.sealed, sc.proof)
		switch {
		case err == nil, errors.Is(err, errSummaryMissing):
			all = append(all, cached...)
		case errors.Is(err, errSummaryTargetMissing):
			sc.missingTarget = true
			if !tolerateMissingTarget {
				return nil, nil, err
			}
		default:
			return nil, nil, err
		}
	}
	present := make(map[int64]bool, len(all))
	for _, b := range all {
		present[b.Start] = true
	}
	width := int64(q.Width) * 1000
	var missing []int64
	for _, st := range sc.sealed {
		if !present[st] && sc.proof.transition(st, width) != "" {
			missing = append(missing, st)
		}
	}
	if len(sc.edges) > 0 {
		edges, err := readTransitions(ctx, sc.s, q, metric, sc.edges)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, edges...)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return all, missing, nil
}
