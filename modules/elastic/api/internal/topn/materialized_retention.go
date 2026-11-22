package topn

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
)

// A proof describes observations in a raw PIT, not a claim that collection was
// continuous. Include storage index names so rollover and partial expiry cannot
// be hidden by an unchanged date range or a single surviving sample.
type rawBucketProof map[int64]map[string]int64

func (p rawBucketProof) digest() string {
	wire, _ := json.Marshal(p)
	return fmt.Sprintf("%x", sha256.Sum256(wire))
}

func rawIndexUUIDs(ctx context.Context, call caller, raw string) (map[string]string, error) {
	wire, err := call(ctx, http.MethodGet, "/"+raw+"/_settings/index.uuid?flat_settings=true&allow_no_indices=false&ignore_unavailable=false&expand_wildcards=open,hidden", nil)
	if err != nil {
		return nil, err
	}
	var response map[string]struct {
		Settings map[string]string `json:"settings"`
	}
	if err = json.Unmarshal(wire, &response); err != nil {
		return nil, err
	}
	if len(response) == 0 || len(response) > 32 {
		return nil, errors.New("invalid TopN raw index metadata")
	}
	out := make(map[string]string, len(response))
	for name, row := range response {
		uuid := row.Settings["index.uuid"]
		if uuid == "" {
			return nil, errors.New("missing TopN raw index UUID")
		}
		out[name] = uuid
	}
	return out, nil
}

// PIT IDs do not expose their backing manifest. Resolve identities on both
// sides of opening the PIT; refuse a concurrent alias or index change.
func openRawSummary(ctx context.Context, call caller, raw string, api pitAPI) (*snapshot, error) {
	before, err := rawIndexUUIDs(ctx, call, raw)
	if err != nil {
		return nil, err
	}
	s, err := open(ctx, call, raw, api)
	if err != nil {
		return nil, err
	}
	after, err := rawIndexUUIDs(ctx, call, raw)
	if err != nil || !maps.Equal(before, after) {
		s.close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("TopN raw backing indices changed while opening snapshot")
	}
	s.rawIndexUUIDs = after
	return s, nil
}

// transitionBounds is shared by scoring and job provenance. The bucket before
// the first visible bucket is optional and read whole, not through q.Min.
func transitionBounds(q Query, start int64) (lo, hi int64, ok bool) {
	width := int64(q.Width) * 1000
	if start < 0 {
		return 0, 0, false
	}
	lo, hi = start, min(q.Max, start+width-1)
	if start >= q.Min/width*width {
		lo = max(lo, q.Min)
	}
	return lo, hi, lo <= hi
}

func rawSummaryProof(ctx context.Context, s *snapshot, q Query, starts []int64) (rawBucketProof, error) {
	return summaryProof(ctx, s, q, starts, false)
}

func summaryProof(ctx context.Context, s *snapshot, q Query, starts []int64, clipped bool) (rawBucketProof, error) {
	proof := make(rawBucketProof)
	if len(starts) == 0 {
		return proof, nil
	}
	uuids := s.rawIndexUUIDs
	if len(uuids) == 0 {
		return nil, errors.New("missing TopN raw snapshot metadata")
	}
	width := int64(q.Width) * 1000
	wanted := make(map[int64]bool)
	for _, st := range starts {
		wanted[st] = true
		if st >= width {
			wanted[st-width] = true
		}
	}
	keys := make([]int64, 0, len(wanted))
	for st := range wanted {
		keys = append(keys, st)
	}
	slices.Sort(keys)
	// Bound the number of date × index buckets, including a 32-shard PIT.
	for offset := 0; offset < len(keys); offset += 64 {
		chunk := keys[offset:min(len(keys), offset+64)]
		ranges := make([]dsl, 0, len(chunk))
		var lo, hi int64
		for i, start := range chunk {
			nextLo, nextHi := start, start+width-1
			if clipped {
				nextLo, nextHi, _ = transitionBounds(q, start)
			}
			if i == 0 {
				lo, hi = nextLo, nextHi
				continue
			}
			if nextLo == hi+1 {
				hi = nextHi
				continue
			}
			ranges = append(ranges, filterRange(lo, hi))
			lo, hi = nextLo, nextHi
		}
		ranges = append(ranges, filterRange(lo, hi))
		scope := q
		scope.AllIndices, scope.Indices = true, nil
		pred, _ := scope.filter()
		pred["filter"] = append(pred["filter"].([]dsl), dsl{"bool": dsl{"should": ranges, "minimum_should_match": 1}})
		var result struct {
			Buckets []struct {
				Key     *int64 `json:"key"`
				Count   int64  `json:"doc_count"`
				Storage struct {
					Other   *int64 `json:"sum_other_doc_count"`
					Buckets []struct {
						Name  string `json:"key"`
						Count int64  `json:"doc_count"`
					} `json:"buckets"`
				} `json:"storage"`
			} `json:"buckets"`
		}
		agg := q.histogram(dsl{"storage": dsl{"terms": terms("_index", s.shards)}})
		if err := s.search(ctx, pred, agg, &result); err != nil {
			return nil, err
		}
		if result.Buckets == nil {
			return nil, errors.New("missing TopN raw dependency buckets")
		}
		for _, bucket := range result.Buckets {
			if bucket.Key == nil || !slices.Contains(chunk, *bucket.Key) || proof[*bucket.Key] != nil || bucket.Count <= 0 || bucket.Storage.Other == nil || *bucket.Storage.Other != 0 {
				return nil, errors.New("invalid TopN raw dependency bucket")
			}
			counts := make(map[string]int64, len(bucket.Storage.Buckets))
			var total int64
			for _, row := range bucket.Storage.Buckets {
				uuid := uuids[row.Name]
				if uuid == "" {
					return nil, errors.New("TopN raw backing index changed during read")
				}
				key := row.Name + "\x00" + uuid
				if row.Name == "" || row.Count <= 0 || counts[key] != 0 || row.Count > bucket.Count-total {
					return nil, errors.New("invalid TopN storage dependency")
				}
				counts[key] = row.Count
				total += row.Count
			}
			if total != bucket.Count {
				return nil, errors.New("incomplete TopN storage dependencies")
			}
			proof[*bucket.Key] = counts
		}
	}
	return proof, nil
}

func (p rawBucketProof) transition(start, width int64) string {
	previous, current := p[start-width], p[start]
	if len(previous) == 0 || len(current) == 0 {
		return ""
	}
	wire, _ := json.Marshal([2]map[string]int64{previous, current})
	return fmt.Sprintf("%x", sha256.Sum256(wire))
}

func summaryProofStarts(sealed, edges []int64) []int64 {
	return append(append([]int64(nil), sealed...), edges...)
}

// Recheck with a fresh raw PIT before publishing; an old PIT can still hold
// references to a backing index removed from the live alias during computation.
func publishProvenBuckets(ctx context.Context, call caller, api pitAPI, m materializedStore, q Query, sets [][]scoreBucket, batches []transitionBatch, original rawBucketProof) error {
	width := int64(q.Width) * 1000
	var starts []int64
	for _, set := range sets {
		for _, b := range set {
			if original.transition(b.Start, width) != "" {
				starts = append(starts, b.Start)
			}
		}
	}
	if len(starts) == 0 {
		return nil
	}
	live, err := openRawSummary(ctx, call, m.raw, api)
	if err != nil {
		return err
	}
	defer live.close()
	current, err := rawSummaryProof(ctx, live, q, starts)
	if err != nil {
		return err
	}
	suffix, err := summaryWriteSuffix(ctx, call, m)
	if err != nil {
		return err
	}
	for i, set := range sets {
		for _, b := range set {
			b.Proof = original.transition(b.Start, width)
			if b.Proof == "" || b.Proof != current.transition(b.Start, width) {
				continue
			}
			path := "/" + m.index + "/_create/" + m.provenID(q, batches[i].metric.Item.Key, b.Start, b.Proof) + suffix
			body := dsl{"v": b.Version, "t": b.Start, "n": b.Names, "s": b.Scores, "p": b.Proof}
			if _, err := call(ctx, http.MethodPut, path, body); err != nil && !errors.Is(err, monitoringHTTPError(409)) {
				return err
			}
		}
	}
	return nil
}
