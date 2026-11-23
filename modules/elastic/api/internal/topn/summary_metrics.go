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
		return []string{writeRateKey, queryRateKey, writeLatencyKey, queryLatencyKey}
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
