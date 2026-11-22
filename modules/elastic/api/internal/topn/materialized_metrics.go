package topn

import (
	"fmt"
	"slices"

	"infini.sh/framework/modules/elastic/common"
)

const (
	queryRateKey      = "query_times"
	writeLatencyKey   = "indexing_latency"
	queryLatencyKey   = "query_latency"
	writeTimeField    = "payload.elasticsearch.index_stats.primaries.indexing.index_time_in_millis"
	queryCounterField = "payload.elasticsearch.index_stats.total.search.query_total"
	queryTimeField    = "payload.elasticsearch.index_stats.total.search.query_time_in_millis"
)

type materializedDefinition struct {
	field, other string
	mode         metricMode
}

var materializedMetrics = map[string]materializedDefinition{
	writeRateKey:    {writeCounterField, "", rate},
	queryRateKey:    {queryCounterField, "", rate},
	writeLatencyKey: {writeTimeField, writeCounterField, latency},
	queryLatencyKey: {queryTimeField, queryCounterField, latency},
}

func (c materializedConfig) metricKeys() []string {
	if len(c.Metrics) == 0 {
		if c.Enabled {
			return []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey}
		}
		return []string{writeRateKey}
	}
	return c.Metrics
}

func (c materializedConfig) readMetricKeys() []string {
	if len(c.ReadMetrics) == 0 {
		return c.metricKeys()
	}
	return c.ReadMetrics
}

func (c materializedConfig) validateMetrics() error {
	for _, keys := range [][]string{c.metricKeys(), c.readMetricKeys()} {
		for i, key := range keys {
			if _, ok := materializedMetrics[key]; !ok {
				return fmt.Errorf("unsupported TopN summary metric %q", key)
			}
			if slices.Contains(keys[:i], key) {
				return fmt.Errorf("duplicate TopN summary metric %q", key)
			}
		}
	}
	return nil
}

func materializedMetric(key string) Metric {
	def := materializedMetrics[key]
	return Metric{Item: &common.MetricItem{Key: key}, Field: def.field, Field2: def.other, mode: def.mode}
}

type transitionBatch struct {
	metric Metric
	starts []int64
}

type transitionProjection struct {
	value, other int
}

// All four metrics have the same identity/topology requirements. Assign one
// aggregation slot to each distinct counter, including latency denominators.
func transitionAggs(q Query, batches []transitionBatch) (dsl, []transitionProjection, error) {
	aggs := metricAggs(q, Metric{mode: rate})
	delete(aggs, "value")
	fields := make(map[string]int)
	slots := []string{"value", "other", "extra1", "extra2"}
	slot := func(field string) int {
		if field == "" {
			return -1
		}
		if i, ok := fields[field]; ok {
			return i
		}
		i := len(fields)
		fields[field] = i
		if i < len(slots) {
			aggs[slots[i]] = maximum(field)
		}
		return i
	}
	projections := make([]transitionProjection, len(batches))
	for i, b := range batches {
		projections[i] = transitionProjection{slot(b.metric.Field), slot(b.metric.Field2)}
	}
	if len(fields) > len(slots) {
		return nil, nil, fmt.Errorf("too many shared TopN counters: %d", len(fields))
	}
	return aggs, projections, nil
}

func (p transitionProjection) sample(s samples) samples {
	values := [4]*scalar{s.Value, s.Other, s.Extra1, s.Extra2}
	s.Value, s.Other = nil, nil
	if p.value >= 0 {
		s.Value = values[p.value]
	}
	if p.other >= 0 {
		s.Other = values[p.other]
	}
	return s
}
