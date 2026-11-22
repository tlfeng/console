package topn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

func singleSearch(ctx context.Context, call caller, index string, pred dsl, q Query, metric Metric, checkHistory bool) error {
	budget := 5998 // Reserve the scan and history filter buckets.
	if metric.jvmSummary {
		budget-- // The same-search JVM availability filter.
	}
	limit := min(192, budget/(metricBucketCost(q, metric)+1)-1)
	if limit < 1 {
		return errBudget
	}
	entityTerms := terms(q.entity(), limit+1)
	entityTerms["order"] = dsl{"_key": "asc"}
	// Every shard containing a globally first L+1 key also has it in its
	// local first L+1. Fewer than L+1 results therefore prove completeness.
	aggs := dsl{"scan": dsl{"filter": metricScope(q, metric), "aggs": dsl{"entities": dsl{
		"terms": entityTerms, "aggs": dsl{"dates": q.histogram(metricAggs(q, metric))},
	}}}}
	if checkHistory {
		aggs["coverage"] = dsl{"filter": filterRange(0, q.Min)}
	}
	if metric.jvmSummary {
		// Check availability in the same read as the data, not in a separate live search.
		aggs["missing_jvm_start"] = missingJVMStarts(q)
	}
	// The target is a concrete index or alias. ES 6.8 does not support hidden wildcard expansion.
	path := "/" + index + "/_search?allow_partial_search_results=false&allow_no_indices=false&ignore_unavailable=false"
	request := dsl{"size": 0, "track_total_hits": false, "query": dsl{"bool": pred}, "aggs": aggs}
	if err := searchTimeout(ctx, request); err != nil {
		return err
	}
	raw, err := call(ctx, http.MethodPost, path, request)
	if err != nil {
		return err
	}
	if len(raw) > 16<<20 {
		return errBudget
	}
	var response searchResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.Origin != "" {
		return errRawHistory
	}
	if response.TimedOut != nil && *response.TimedOut {
		return context.DeadlineExceeded
	}
	if !response.complete() {
		return errors.New("incomplete monitoring search")
	}
	if checkHistory {
		var coverage struct {
			Count *int64 `json:"doc_count"`
		}
		if err := json.Unmarshal(response.Aggs["coverage"], &coverage); err != nil {
			return err
		}
		if coverage.Count == nil || *coverage.Count == 0 {
			return errRawHistory
		}
	}
	if metric.jvmSummary {
		missing, err := hasMissingJVMStarts(response.Aggs["missing_jvm_start"])
		if err != nil {
			return err
		}
		if missing {
			return errLegacyJVM
		}
	}
	var scan struct {
		Entities struct {
			Buckets []struct {
				Key   string `json:"key"`
				Dates struct {
					Buckets []samples `json:"buckets"`
				} `json:"dates"`
			} `json:"buckets"`
		} `json:"entities"`
	}
	if err := json.Unmarshal(response.Aggs["scan"], &scan); err != nil {
		return err
	}
	entities := scan.Entities.Buckets
	if entities == nil {
		return errors.New("missing TopN entities")
	}
	if len(entities) > limit+1 {
		return errors.New("oversized TopN entity response")
	}
	best := make([]winner, 0, q.N)
	allowed := q.authorizer()
	last := ""
	for _, b := range entities {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !allowed(b.Key) || b.Key <= last {
			return errors.New("invalid TopN entity buckets")
		}
		last = b.Key
		if len(entities) == limit+1 {
			continue
		}
		candidate, err := scoreSeries(q, metric, b.Key, b.Dates.Buckets)
		if err != nil {
			return err
		}
		best = keepWinner(best, candidate, q.N)
	}
	if len(entities) == limit+1 {
		return errCandidateLimit
	}
	for _, row := range best {
		display(q, metric, row.name, row.points)
	}
	return nil
}
