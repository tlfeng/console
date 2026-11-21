package topn

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

const (
	pageSize  = 64
	maxPages  = 24
	maxPoints = 150000
)

type scalar struct {
	Value *float64 `json:"value"`
}
type samples struct {
	Time        *int64  `json:"key"`
	Count       int     `json:"doc_count"`
	Value       *scalar `json:"value"`
	Other       *scalar `json:"other"`
	Uptime      *scalar `json:"uptime"`
	FirstUptime *scalar `json:"first_uptime"`
	Shards      *scalar `json:"shards"`
	Replicas    *scalar `json:"replicas"`
	Identity    *struct {
		Buckets []struct {
			Key   string `json:"key"`
			Count int    `json:"doc_count"`
		} `json:"buckets"`
	} `json:"identity"`
}

func (p samples) entity() string {
	if p.Identity == nil || len(p.Identity.Buckets) != 1 || p.Identity.Buckets[0].Count != p.Count {
		return ""
	}
	return p.Identity.Buckets[0].Key
}

func counterPoint(previous, current samples, width int64, ratio, node bool) (float64, bool) {
	if previous.Time == nil || current.Time == nil || *current.Time-*previous.Time != width || previous.Value == nil || current.Value == nil || previous.Value.Value == nil || current.Value.Value == nil {
		return 0, false
	}
	id := previous.entity()
	if id == "" || id != current.entity() {
		return 0, false
	}
	if node && (previous.Uptime == nil || current.Uptime == nil || previous.Uptime.Value == nil || current.Uptime.Value == nil || current.FirstUptime == nil || current.FirstUptime.Value == nil || *current.FirstUptime.Value <= *previous.Uptime.Value) {
		return 0, false
	}
	if !node && (changed(previous.Shards, current.Shards) || changed(previous.Replicas, current.Replicas)) {
		return 0, false
	}

	a, b := *previous.Value.Value, *current.Value.Value
	if a < 0 || b < a {
		return 0, false
	}
	if !ratio {
		return (b - a) / (float64(width) / 1000), true
	}
	if previous.Other == nil || current.Other == nil || previous.Other.Value == nil || current.Other.Value == nil {
		return 0, false
	}
	c, d := *previous.Other.Value, *current.Other.Value
	if c < 0 || d <= c {
		return 0, false
	}
	return (b - a) / (d - c), true
}

func changed(a, b *scalar) bool {
	if a == nil || a.Value == nil {
		return b != nil && b.Value != nil
	}
	return b == nil || b.Value == nil || *a.Value != *b.Value
}

type winner struct {
	name   string
	score  float64
	points [][]interface{}
}

func (q Query) previousBucket() int64 {
	w := int64(q.Width) * 1000
	return q.Min/w*w - w
}

func counters(ctx context.Context, s *snapshot, q Query, pred dsl, metric Metric) error {
	width := int64(q.Width) * 1000
	first := q.Min / width * width
	// Read the complete preceding bucket, but never include the hidden part of the first visible bucket.
	scope := []dsl{filterRange(q.Min, q.Max)}
	if first > 0 {
		scope = append(scope, filterRange(first-width, first-1))
	}
	pred["filter"] = append(pred["filter"].([]dsl), dsl{"bool": dsl{"should": scope, "minimum_should_match": 1}})
	node := q.Source == "node_stats"
	identity := "metadata.labels.index_uuid"
	if node {
		identity = "metadata.labels.node_id"
	}
	buckets := dsl{"value": maximum(metric.Field), "identity": dsl{"terms": terms(identity, 2)}}
	if node {
		const uptime = "payload.elasticsearch.node_stats.jvm.uptime_in_millis"
		buckets["uptime"] = maximum(uptime)
		buckets["first_uptime"] = dsl{"min": dsl{"field": uptime}}
	} else {
		buckets["shards"] = maximum("payload.elasticsearch.index_stats.index_info.shards")
		buckets["replicas"] = maximum("payload.elasticsearch.index_stats.index_info.replicas")
	}
	if metric.Field2 != "" {
		buckets["other"] = maximum(metric.Field2)
	}
	size := pageSize
	// Leave headroom for per-point identity buckets plus their histogram parents.
	if limit := 6000 / (3*int(q.Max/width-q.Min/width+2) + 1); limit < size {
		size = limit
	}
	if size < 1 {
		return errBudget
	}
	composite := dsl{"size": size, "sources": []dsl{{"entity": dsl{"terms": dsl{"field": q.entity()}}}}}
	agg := dsl{"composite": composite, "aggs": dsl{"dates": histogram(q.Width, buckets)}}
	best := make([]winner, 0, q.N)
	var after, last string
	total := 0
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var response struct {
			After *struct {
				Entity string `json:"entity"`
			} `json:"after_key"`
			Buckets []struct {
				Key struct {
					Entity string `json:"entity"`
				} `json:"key"`
				Dates *struct {
					Buckets []samples `json:"buckets"`
				} `json:"dates"`
			} `json:"buckets"`
		}
		if err := s.search(ctx, pred, agg, &response); err != nil {
			return err
		}
		if response.Buckets == nil || len(response.Buckets) > size {
			return errors.New("invalid TopN page")
		}
		for _, b := range response.Buckets {
			name := b.Key.Entity
			if name == "" || name <= last || name <= after || b.Dates == nil || len(b.Dates.Buckets) == 0 {
				return errors.New("missing or out-of-order TopN entity")
			}
			last = name
			total += len(b.Dates.Buckets)
			if total > maxPoints {
				return errBudget
			}
			candidate := winner{name: name, score: -1}
			for _, p := range b.Dates.Buckets {
				if p.Time == nil {
					return fmt.Errorf("missing time bucket key for %q", name)
				}
			}
			sort.Slice(b.Dates.Buckets, func(i, j int) bool { return *b.Dates.Buckets[i].Time < *b.Dates.Buckets[j].Time })
			var previous samples
			for _, p := range b.Dates.Buckets {
				if p.Time == nil || *p.Time < first-width || *p.Time > q.Max || *p.Time%width != 0 || (previous.Time != nil && *p.Time <= *previous.Time) {
					return fmt.Errorf("invalid time bucket %d for %q", *p.Time, name)
				}
				if *p.Time >= first {
					if v, valid := counterPoint(previous, p, width, metric.Field2 != "", node); valid {
						if previous.Time != nil && len(candidate.points) > 0 {
							lastPoint := candidate.points[len(candidate.points)-1][0].(int64)
							if *p.Time-lastPoint > width {
								candidate.points = append(candidate.points, []interface{}{lastPoint + width, nil})
							}
						}
						candidate.points = append(candidate.points, []interface{}{*p.Time, v})
						if v > candidate.score {
							candidate.score = v
						}
					}
				}
				previous = p
			}
			if candidate.score < 0 {
				continue
			}
			// N is at most 20. Keep the bounded list ordered, never retain whole candidate pages.
			i := 0
			for i < len(best) && (best[i].score > candidate.score || (best[i].score == candidate.score && best[i].name < candidate.name)) {
				i++
			}
			if i < q.N {
				if len(best) < q.N {
					best = append(best, winner{})
				}
				copy(best[i+1:], best[i:])
				best[i] = candidate
			}
		}
		if len(response.Buckets) == 0 || response.After == nil {
			if len(response.Buckets) == size && response.After == nil {
				return errors.New("missing TopN cursor")
			}
			for _, row := range best {
				display(q, metric, row.name, row.points)
			}
			return nil
		}
		next := response.After.Entity
		if next == "" || next <= after || next < last {
			return errors.New("TopN cursor did not advance")
		}
		after = next
		composite["after"] = dsl{"entity": after}
	}
	return errBudget
}
