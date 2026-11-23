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

type summaryMetricDefinition struct {
	field, other string
	mode         metricMode
}

var summaryMetrics = map[string]summaryMetricDefinition{
	writeRateKey:    {writeCounterField, "", rate},
	queryRateKey:    {queryCounterField, "", rate},
	writeLatencyKey: {writeTimeField, writeCounterField, latency},
	queryLatencyKey: {queryTimeField, queryCounterField, latency},
}

func (c summaryConfig) metricKeys() []string {
	if len(c.Metrics) == 0 {
		if c.Enabled {
			return []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey}
		}
		return []string{writeRateKey}
	}
	return c.Metrics
}

func (c summaryConfig) readMetricKeys() []string {
	if len(c.ReadMetrics) == 0 {
		return c.metricKeys()
	}
	return c.ReadMetrics
}

func (c summaryConfig) validateMetrics() error {
	for _, keys := range [][]string{c.metricKeys(), c.readMetricKeys()} {
		for i, key := range keys {
			if _, ok := summaryMetrics[key]; !ok {
				return fmt.Errorf("unsupported TopN summary metric %q", key)
			}
			if slices.Contains(keys[:i], key) {
				return fmt.Errorf("duplicate TopN summary metric %q", key)
			}
		}
	}
	return nil
}

func summaryMetric(key string) Metric {
	def := summaryMetrics[key]
	return Metric{Item: &common.MetricItem{Key: key}, Field: def.field, Field2: def.other, mode: def.mode}
}

type transitionBatch struct {
	metric Metric
	starts []int64
}

// Rates need one maximum; latency needs two counters from one document.
// Share the scan and its identity/topology evidence across all requested metrics.
func transitionAggs(q Query, batches []transitionBatch) (dsl, []int, error) {
	aggs := counterEvidence(q)
	rateSlots := [...]string{"value", "other"}
	pairSlots := [...]string{"pair", "second_pair"}
	rates := make(map[string]int, len(rateSlots))
	pairs := make(map[[2]string]int, len(pairSlots))
	projections := make([]int, len(batches))
	for i, batch := range batches {
		metric := batch.metric
		if metric.mode == latency {
			key := [2]string{metric.Field, metric.Field2}
			slot, ok := pairs[key]
			if !ok {
				slot = len(pairs)
				if slot == len(pairSlots) {
					return nil, nil, fmt.Errorf("too many shared TopN latency pairs")
				}
				pairs[key] = slot
				aggs[pairSlots[slot]] = latencyPair(metric)
			}
			projections[i] = slot
			continue
		}
		slot, ok := rates[metric.Field]
		if !ok {
			slot = len(rates)
			if slot == len(rateSlots) {
				return nil, nil, fmt.Errorf("too many shared TopN rate counters")
			}
			rates[metric.Field] = slot
			aggs[rateSlots[slot]] = maximum(metric.Field)
		}
		projections[i] = slot
	}
	return aggs, projections, nil
}

func (s samples) project(metric Metric, slot int) samples {
	if slot == 1 {
		if metric.mode == latency {
			s.Pair = s.SecondPair
		} else {
			s.Value = s.Other
		}
	}
	return s
}
