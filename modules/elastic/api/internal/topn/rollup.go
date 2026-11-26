package topn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// Use the native Easysearch rewrite only for the benchmarked nonnegative
// gauge. Easysearch decides whether the source can be rewritten.
func tryRollupPeak(ctx context.Context, call caller, index string, q Query, metric Metric) bool {
	const minute = 60_000
	if !q.easysearch || !q.longAutomaticWindow() || q.Source != "index_stats" || metric.mode != peak ||
		metric.Item.Key != "index_storage" || metric.Field != "payload.elasticsearch.index_stats.total.store.size_in_bytes" ||
		q.Min%minute != 0 || q.Max%minute != minute-1 || q.Width <= 0 || q.Width%60 != 0 {
		return false
	}
	filters := []dsl{
		{"term": dsl{"metadata.labels.cluster_id": q.Cluster}},
		{"term": dsl{"metadata.category": "elasticsearch"}},
		{"range": dsl{"timestamp": dsl{"gte": q.Min, "lte": q.Max}}},
	}
	if !q.AllIndices {
		if len(q.Indices) == 0 {
			return false
		}
		for _, name := range q.Indices {
			if !aclPattern.MatchString(name) || strings.Contains(name, "*") {
				return false
			}
		}
		filters = append(filters, dsl{"terms": dsl{q.entity(): q.Indices}})
	}
	pred := dsl{
		"must":     []dsl{{"term": dsl{"metadata.name": q.Source}}},
		"filter":   filters,
		"must_not": []dsl{{"term": dsl{q.entity(): "_all"}}},
	}
	search := func(ctx context.Context, pred, agg dsl, result interface{}) error {
		body := dsl{"size": 0, "track_total_hits": false, "query": dsl{"bool": pred}, "aggs": dsl{"entities": agg}}
		if err := searchTimeout(ctx, body); err != nil {
			return err
		}
		raw, err := call(ctx, http.MethodPost, "/"+index+"/_search?enable_rollup=true&allow_partial_search_results=false", body)
		if err != nil {
			return err
		}
		var response searchResponse
		if err := json.Unmarshal(raw, &response); err != nil {
			return err
		}
		if err := response.validate(); err != nil {
			return err
		}
		// Mixed responses can mask failures in their live or rollup half.
		if response.Origin != "rollup" {
			return errors.New("search did not use pure rollup data")
		}
		return json.Unmarshal(response.Aggs["entities"], result)
	}
	// peakSeries publishes only after both searches and every curve validate.
	before := len(metric.Item.Lines)
	return peakSeries(ctx, search, q, pred, metric) == nil && len(metric.Item.Lines) > before
}
