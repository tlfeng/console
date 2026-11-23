package topn

import (
	"context"
	"errors"
	"math"
)

const (
	searchBucketLimit = 6000
	pageSize          = 64
	maxPages          = 48
	maxPoints         = 150000
)

type scalar struct {
	Value *float64 `json:"value"`
}
type samples struct {
	Time       *int64     `json:"key"`
	Count      int        `json:"doc_count"`
	Value      *scalar    `json:"value"`
	Other      *scalar    `json:"other"`
	Pair       *pairedHit `json:"pair"`
	SecondPair *pairedHit `json:"second_pair"`
	Shards     *scalar    `json:"shards"`
	Replicas   *scalar    `json:"replicas"`
	Identity   *struct {
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

type pairedHit struct {
	Hits struct {
		Rows []struct {
			Fields map[string][]float64 `json:"fields"`
		} `json:"hits"`
	} `json:"hits"`
}

func (p samples) counterValues(metric Metric) (value, operations float64, ok bool, err error) {
	if metric.mode != latency {
		value, ok = number(p.Value)
		return value, 0, ok, nil
	}
	if p.Pair == nil || len(p.Pair.Hits.Rows) != 1 {
		return 0, 0, false, errors.New("incomplete TopN latency observation")
	}
	fields := p.Pair.Hits.Rows[0].Fields
	a, b := fields[metric.Field], fields[metric.Field2]
	if len(a) != 1 || len(b) != 1 || !validCounter(a[0]) || !validCounter(b[0]) {
		return 0, 0, false, nil
	}
	return a[0], b[0], true, nil
}

func validCounter(v float64) bool {
	return v >= 0 && !math.IsInf(v, 0)
}

// Index transitions also carry shard and replica evidence; node transitions
// never do, so their topology guard is always vacuous.
func counterPoint(previous, current samples, width int64, metric Metric) (float64, bool, error) {
	if previous.Time == nil || current.Time == nil || *current.Time-*previous.Time != width {
		return 0, false, nil
	}
	id := previous.entity()
	if id == "" || id != current.entity() {
		return 0, false, nil
	}
	if changed(previous.Shards, current.Shards) || changed(previous.Replicas, current.Replicas) {
		return 0, false, nil
	}

	a, c, ok, err := previous.counterValues(metric)
	if err != nil || !ok {
		return 0, false, err
	}
	b, d, ok, err := current.counterValues(metric)
	if err != nil || !ok || b < a {
		return 0, false, err
	}
	if metric.mode != latency {
		return (b - a) / (float64(width) / 1000), true, nil
	}
	if d <= c {
		return 0, false, nil
	}
	return (b - a) / (d - c), true, nil
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

func (q Query) counterScope() dsl {
	width := int64(q.Width) * 1000
	first := q.Min / width * width
	// Read the complete preceding bucket, but never include the hidden part of the first visible bucket.
	scope := []dsl{filterRange(q.Min, q.Max)}
	if first >= width {
		scope = append(scope, filterRange(first-width, first-1))
	}
	return dsl{"bool": dsl{"should": scope, "minimum_should_match": 1}}
}

// Both aggregations work on doc values from the same observation. A numeric
// sort after the validity flag keeps large operation counts in their native type.
const pairedNumbers = `
if (!doc.containsKey(params.first) || !doc.containsKey(params.second) ||
    doc[params.first].size() != 1 || doc[params.second].size() != 1) return -1;
double a = doc[params.first].value;
double b = doc[params.second].value;
if (!(a >= 0 && a <= 1.7976931348623157E308 && b >= 0 && b <= 1.7976931348623157E308)) return -1;
`

func pairedScript(first, second, source string) dsl {
	return dsl{"lang": "painless", "source": pairedNumbers + source, "params": dsl{"first": first, "second": second}}
}

func ratioMaximum(metric Metric) dsl {
	a, b := metric.Field, metric.Field2
	if metric.reverseRatio {
		a, b = b, a
	}
	return dsl{"max": dsl{"script": pairedScript(a, b, `
if (b <= 0) return -1;
double value = a / b * 100.0;
return value <= 1.7976931348623157E308 ? value : -1;
`)}}
}

func latencyPair(metric Metric) dsl {
	return dsl{"top_hits": dsl{
		"size": 1, "_source": false, "stored_fields": "_none_",
		"docvalue_fields": []string{metric.Field, metric.Field2},
		"sort": []dsl{
			{"_script": dsl{"type": "number", "order": "desc", "script": pairedScript(metric.Field, metric.Field2, "return 1;")}},
			{metric.Field2: dsl{"order": "desc", "unmapped_type": "long"}},
			{"timestamp": dsl{"order": "desc", "unmapped_type": "date"}},
			{metric.Field: dsl{"order": "desc", "unmapped_type": "long"}},
		},
	}}
}

func metricAggs(q Query, metric Metric) dsl {
	switch metric.mode {
	case ratio:
		return dsl{"value": ratioMaximum(metric)}
	case peak:
		return dsl{"value": maximum(metric.Field)}
	}
	buckets := counterEvidence(q)
	if metric.mode == latency {
		buckets["pair"] = latencyPair(metric)
	} else {
		buckets["value"] = maximum(metric.Field)
	}
	return buckets
}

func counterEvidence(q Query) dsl {
	buckets := dsl{}
	node := q.Source == "node_stats"
	identity := "metadata.labels.index_uuid"
	if node {
		identity = "metadata.labels.node_id"
	}
	buckets["identity"] = dsl{"terms": terms(identity, 2)}
	if !node {
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
	best := make([]winner, 0, q.N)
	err := visitBucketAggs(ctx, s, q, pred, metricAggs(q, metric), metricScope(q, metric), metricBucketCost(q, metric), func(name string, points []samples) error {
		candidate, err := scoreSeries(q, metric, name, points)
		if err == nil {
			best = keepWinner(best, candidate, q.N)
		}
		return err
	})
	if err != nil {
		return err
	}
	for _, row := range best {
		display(q, metric, row.name, row.points)
	}
	return nil
}

func visitBucketAggs(ctx context.Context, s *snapshot, q Query, pred, buckets, scope dsl, cost int, visit func(string, []samples) error) error {
	pred["filter"] = append(pred["filter"].([]dsl), scope)
	// Leave headroom for per-point identity buckets plus their histogram parents.
	size := min(pageSize, searchBucketLimit/(cost+1))
	if size < 1 {
		return errBudget
	}
	composite := dsl{"size": size, "sources": []dsl{{"entity": dsl{"terms": dsl{"field": q.entity()}}}}}
	agg := dsl{"composite": composite, "aggs": dsl{"dates": q.histogram(buckets)}}
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
			if err := visit(name, b.Dates.Buckets); err != nil {
				return err
			}
		}
		if len(response.Buckets) == 0 || response.After == nil {
			if len(response.Buckets) == size && response.After == nil {
				return errors.New("missing TopN cursor")
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
