package topn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

func singleSearch(ctx context.Context, call caller, index string, pred dsl, q Query, metric Metric) error {
	budget := searchBucketLimit - 2 // Reserve the scan and history filter buckets.
	const maxCandidates = 192
	capacity := budget / (metricBucketCost(q, metric) + 1)
	selectedOnly := q.selected != nil
	// Automatic ranking needs one extra candidate to detect truncation.
	maxEntities := min(maxCandidates+1, capacity)
	if selectedOnly {
		maxEntities = len(q.selected)
		if maxEntities == 0 {
			return nil
		}
		if maxEntities > min(maxCandidates, capacity) {
			return errBudget
		}
	} else if maxEntities < 2 {
		return errBudget
	}
	entityTerms := terms(q.entity(), maxEntities)
	entityTerms["order"] = dsl{"_key": "asc"}
	// The first maxEntities keys are visible across shards, so a full automatic
	// response cannot prove that every candidate was ranked.
	aggs := dsl{"scan": dsl{"filter": metricScope(q, metric), "aggs": dsl{"entities": dsl{
		"terms": entityTerms, "aggs": dsl{"dates": q.histogram(metricAggs(q, metric))},
	}}}}
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
	var selected map[string]winner
	if selectedOnly {
		selected = make(map[string]winner, maxEntities)
	}
	if len(entities) > maxEntities {
		return errors.New("oversized TopN entity response")
	}
	truncated := !selectedOnly && len(entities) == maxEntities
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
		if truncated {
			continue
		}
		candidate, err := scoreSeries(q, metric, b.Key, b.Dates.Buckets)
		if err != nil {
			return err
		}
		if selected != nil {
			if len(candidate.points) > 0 {
				selected[b.Key] = candidate
			}
		} else {
			best = keepWinner(best, candidate, q.N)
		}
	}
	if truncated {
		return errCandidateLimit
	}
	if selected != nil {
		for _, name := range q.selected {
			if row, ok := selected[name]; ok {
				display(q, metric, row.name, row.points)
			}
		}
		return nil
	}
	for _, row := range best {
		display(q, metric, row.name, row.points)
	}
	return nil
}
