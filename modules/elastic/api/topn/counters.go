package topn

import (
	"context"
	"errors"
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
	Time  *int64  `json:"key"`
	Count int     `json:"doc_count"`
	Value *scalar `json:"value"`
	Other *scalar `json:"other"`
	JVM   *struct {
		Hits struct {
			Rows []struct {
				Fields map[string][]int64 `json:"fields"`
			} `json:"hits"`
		} `json:"hits"`
	} `json:"jvm_lifetime"`
	Shards   *scalar `json:"shards"`
	Replicas *scalar `json:"replicas"`
	Identity *struct {
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

func counterPoint(previous, current samples, width int64, mode metricMode, node bool) (float64, bool) {
	if previous.Time == nil || current.Time == nil || *current.Time-*previous.Time != width || previous.Value == nil || current.Value == nil || previous.Value.Value == nil || current.Value.Value == nil {
		return 0, false
	}
	id := previous.entity()
	if id == "" || id != current.entity() {
		return 0, false
	}
	if node && !sameJVMLifetime(previous, current) {
		return 0, false
	}
	if !node && (changed(previous.Shards, current.Shards) || changed(previous.Replicas, current.Replicas)) {
		return 0, false
	}

	a, b := *previous.Value.Value, *current.Value.Value
	if a < 0 || b < a {
		return 0, false
	}
	if mode != latency {
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

const (
	jvmTimestamp   = "payload.elasticsearch.node_stats.jvm.timestamp"
	jvmUptime      = "payload.elasticsearch.node_stats.jvm.uptime_in_millis"
	jvmSampleLimit = 64
)

// The node's own timestamp and uptime share a clock. Reject a bucket if
// its lifetime evidence is missing or truncated, even as a baseline.
func (p samples) jvmLifetime() (int64, int64, bool) {
	if p.Count <= 0 || p.JVM == nil || len(p.JVM.Hits.Rows) != p.Count {
		return 0, 0, false
	}
	var lo, hi int64
	for i, row := range p.JVM.Hits.Rows {
		ts, uptime := row.Fields[jvmTimestamp], row.Fields[jvmUptime]
		if len(ts) != 1 || len(uptime) != 1 || uptime[0] < 0 || ts[0] <= uptime[0] {
			return 0, 0, false
		}
		start := ts[0] - uptime[0]
		if i == 0 {
			lo, hi = start, start
		} else {
			lo, hi = min(lo, start), max(hi, start)
		}
	}
	return lo, hi, true
}

func sameJVMLifetime(previous, current samples) bool {
	const toleranceMS = 1000
	a, b, ok := previous.jvmLifetime()
	c, d, otherOK := current.jvmLifetime()
	return ok && otherOK && max(b, d)-min(a, c) <= toleranceMS
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

func (q Query) counterScope() dsl {
	width := int64(q.Width) * 1000
	first := q.Min / width * width
	// Read the complete preceding bucket, but never include the hidden part of the first visible bucket.
	scope := []dsl{filterRange(q.Min, q.Max)}
	if first > 0 {
		scope = append(scope, filterRange(first-width, first-1))
	}
	return dsl{"bool": dsl{"should": scope, "minimum_should_match": 1}}
}

func metricAggs(q Query, metric Metric) dsl {
	buckets := dsl{"value": maximum(metric.Field)}
	if metric.Field2 != "" {
		buckets["other"] = maximum(metric.Field2)
	}
	if !metric.mode.difference() {
		return buckets
	}
	node := q.Source == "node_stats"
	identity := "metadata.labels.index_uuid"
	if node {
		identity = "metadata.labels.node_id"
	}
	buckets["identity"] = dsl{"terms": terms(identity, 2)}
	if node {
		buckets["jvm_lifetime"] = dsl{"top_hits": dsl{
			"size": jvmSampleLimit, "_source": false, "stored_fields": "_none_", "sort": []string{"_doc"},
			"docvalue_fields": []string{jvmTimestamp, jvmUptime},
		}}
	} else {
		buckets["shards"] = maximum("payload.elasticsearch.index_stats.index_info.shards")
		buckets["replicas"] = maximum("payload.elasticsearch.index_stats.index_info.replicas")
	}
	return buckets
}

func keepWinner(best []winner, candidate winner, n int) []winner {
	if candidate.score < 0 {
		return best
	}
	// N is at most 20. Keep the bounded list ordered, never retain whole candidate pages.
	i := 0
	for i < len(best) && (best[i].score > candidate.score || (best[i].score == candidate.score && best[i].name < candidate.name)) {
		i++
	}
	if i < n {
		if len(best) < n {
			best = append(best, winner{})
		}
		copy(best[i+1:], best[i:])
		best[i] = candidate
	}
	return best
}

func paged(ctx context.Context, s *snapshot, q Query, pred dsl, metric Metric) error {
	pred["filter"] = append(pred["filter"].([]dsl), metricScope(q, metric))
	buckets := metricAggs(q, metric)
	size := pageSize
	// Leave headroom for per-point identity buckets plus their histogram parents.
	if limit := 6000 / (metricBucketCost(q, metric) + 1); limit < size {
		size = limit
	}
	if size < 1 {
		return errBudget
	}
	composite := dsl{"size": size, "sources": []dsl{{"entity": dsl{"terms": dsl{"field": q.entity()}}}}}
	agg := dsl{"composite": composite, "aggs": dsl{"dates": histogram(q.Width, buckets)}}
	best := make([]winner, 0, q.N)
	allowed := q.authorizer()
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
			if !allowed(name) || name == "" || name <= last || name <= after || b.Dates == nil || len(b.Dates.Buckets) == 0 {
				return errors.New("missing or out-of-order TopN entity")
			}
			last = name
			total += len(b.Dates.Buckets)
			if total > maxPoints {
				return errBudget
			}
			candidate, err := scoreSeries(q, metric, name, b.Dates.Buckets)
			if err != nil {
				return err
			}
			best = keepWinner(best, candidate, q.N)
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
