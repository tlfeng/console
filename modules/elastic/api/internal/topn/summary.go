package topn

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	writeRateKey       = "indexing_rate"
	writeCounterField  = "payload.elasticsearch.index_stats.primaries.indexing.index_total"
	summaryVersion     = 1
	summaryEntityLimit = 4096
	summaryLag         = 15 * time.Minute
	summaryDays        = 32
)

var errSummaryHistory = errors.New("automatic TopN summary coverage unavailable")
var errSummaryMissing = fmt.Errorf("missing summary bucket: %w", errSummaryHistory)

type summaryConfig struct {
	Enabled        bool     `config:"enabled"`
	ReadOverride   *bool    `config:"read"`
	WriterOverride *bool    `config:"writer"`
	Index          string   `config:"index"`
	Clusters       []string `config:"clusters"`
	Metrics        []string `config:"metrics"`
	ReadMetrics    []string `config:"read_metrics"`
	Gaps           []struct {
		From  string `config:"from"`
		Until string `config:"until"`
	} `config:"raw_gaps"`
}

type coverageGap struct{ from, until int64 }

type summaryStore struct {
	index, raw string
	gaps       []coverageGap
}

func (c summaryConfig) reads() bool {
	if c.ReadOverride != nil {
		return *c.ReadOverride
	}
	return c.Enabled
}

func (c summaryConfig) writes() bool {
	if c.WriterOverride != nil {
		return *c.WriterOverride
	}
	return c.Enabled
}

func (c summaryConfig) enabled(cluster, key string) bool {
	return cluster != "" && c.reads() && slices.Contains(c.readMetricKeys(), key) &&
		(c.Enabled && len(c.Clusters) == 0 || slices.Contains(c.Clusters, cluster))
}

func (c summaryConfig) store(raw string) (summaryStore, error) {
	if c.Enabled && c.Index == "" {
		c.Index = ".console-topn-index-rate-v2"
	}
	// Explicit concrete index outside the original monitoring and rollup namespace.
	if !rawName.MatchString(c.Index) || !strings.HasPrefix(c.Index, ".console-topn-") || c.Index == raw {
		return summaryStore{}, errSummaryHistory
	}
	if err := c.validateMetrics(); err != nil {
		return summaryStore{}, err
	}
	m := summaryStore{index: c.Index, raw: raw}
	for _, gap := range c.Gaps {
		a, e := parseTime(gap.From)
		b, f := parseTime(gap.Until)
		if e != nil || f != nil || a < 0 || a >= b {
			return summaryStore{}, errSummaryHistory
		}
		m.gaps = append(m.gaps, coverageGap{a, b})
	}
	return m, nil
}

func (m summaryStore) covered(from, max int64) bool {
	for _, gap := range m.gaps {
		if from < gap.until && max >= gap.from {
			return false
		}
	}
	return true
}

func useSummary(q Query, metric Metric) bool {
	if q.Source != "index_stats" || metric.Item == nil {
		return false
	}
	def, ok := summaryMetrics[metric.Item.Key]
	if !ok || metric.Field != def.field || metric.Field2 != def.other || metric.mode != def.mode {
		return false
	}
	var days int64
	switch q.Width {
	case 10800, 14400:
		days = 15
	case 21600, 28800:
		days = 30
	default:
		return false
	}
	delta := q.Max - q.Min - days*86400000
	return delta >= -3600000 && delta <= 3600000
}

// One immutable document is both the complete batch and its coverage marker.
// Parallel arrays keep the ranking input small; an empty batch is representable.
type scoreBucket struct {
	Version int       `json:"v"`
	Start   int64     `json:"t"`
	Names   []string  `json:"n"`
	Scores  []float64 `json:"s"`
	Proof   string    `json:"p,omitempty"`
}

func (m summaryStore) metricID(q Query, key string, start int64) string {
	identity := fmt.Sprintf("%d\x00%s\x00%s\x00%s\x00%d\x00%d", summaryVersion, m.raw, q.Cluster, key, q.Width, start)
	if key == writeLatencyKey || key == queryLatencyKey {
		identity += "\x00paired-latency"
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
}

func (m summaryStore) provenID(q Query, key string, start int64, proof string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(m.metricID(q, key, start)+":"+proof)))
}

func (b scoreBucket) valid(start int64) bool {
	if b.Version != summaryVersion || b.Start != start || b.Names == nil || b.Scores == nil || len(b.Names) != len(b.Scores) || len(b.Names) > summaryEntityLimit {
		return false
	}
	for i, name := range b.Names {
		if name == "" || name == "_all" || (i > 0 && b.Names[i-1] >= name) || b.Scores[i] < 0 || math.IsInf(b.Scores[i], 0) || math.IsNaN(b.Scores[i]) {
			return false
		}
	}
	return true
}

func summaryScopes(q Query, seal int64) ([]int64, map[int64]bool) {
	w := int64(q.Width) * 1000
	first, last := q.Min/w*w, q.Max/w*w
	var sealed []int64
	edges := map[int64]bool{}
	for ts := first; ts <= last; ts += w {
		// A clipped first bucket also changes the next transition's baseline.
		if (q.Min != first && ts <= first+w) || ts+w-1 > q.Max || ts+w > seal || ts-w < 0 {
			edges[ts] = true
		} else {
			sealed = append(sealed, ts)
		}
	}
	return sealed, edges
}

func readMetricScoreBuckets(ctx context.Context, s *snapshot, q Query, m summaryStore, key string, starts []int64, proof rawBucketProof) ([]scoreBucket, error) {
	if len(starts) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(starts))
	want := make(map[string]int64, len(starts))
	for _, ts := range starts {
		digest := proof.transition(ts, int64(q.Width)*1000)
		if digest == "" {
			continue
		}
		id := m.provenID(q, key, ts, digest)
		ids = append(ids, id)
		want[id] = ts
	}
	if len(ids) == 0 {
		return nil, nil
	}
	body := dsl{"size": len(ids) * s.shards, "track_total_hits": true, "query": dsl{"ids": dsl{"values": ids}}, "pit": dsl{"id": s.id, "keep_alive": "30s"}}
	if err := searchTimeout(ctx, body); err != nil {
		return nil, err
	}
	raw, err := s.call(ctx, http.MethodPost, "/_search?allow_partial_search_results=false", body)
	if err != nil {
		return nil, err
	}
	var response struct {
		searchResponse
		Hits struct {
			Total struct {
				Value    int    `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			Rows []struct {
				ID     string      `json:"_id"`
				Source scoreBucket `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err = json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	if response.ID != "" {
		s.id = response.ID
	}
	if response.TimedOut != nil && *response.TimedOut {
		return nil, context.DeadlineExceeded
	}
	if !response.complete() || response.Shards.Total != s.shards || response.Origin != "" || response.Hits.Rows == nil || response.Hits.Total.Relation != "eq" || response.Hits.Total.Value != len(response.Hits.Rows) {
		return nil, errors.New("incomplete TopN summary search")
	}
	out := make([]scoreBucket, 0, len(ids))
	seen := make(map[string]scoreBucket)
	for _, row := range response.Hits.Rows {
		ts, ok := want[row.ID]
		if previous, duplicate := seen[row.ID]; duplicate {
			if previous.Proof != row.Source.Proof || previous.Start != row.Source.Start || previous.Version != row.Source.Version || !slices.Equal(previous.Names, row.Source.Names) || !slices.Equal(previous.Scores, row.Source.Scores) {
				return nil, errors.New("conflicting TopN summary generations")
			}
			continue
		}
		if !ok || !row.Source.valid(ts) || row.Source.Proof != proof.transition(ts, int64(q.Width)*1000) {
			return nil, errors.New("invalid TopN summary document")
		}
		seen[row.ID] = row.Source
		delete(want, row.ID)
		out = append(out, row.Source)
	}
	if len(want) != 0 {
		return out, errSummaryMissing
	}
	return out, nil
}

func summaryRank(q Query, metric Metric, buckets []scoreBucket) error {
	allowed := q.authorizer()
	type point struct {
		ts    int64
		value float64
	}
	lists := make(map[string][]point)
	// At most about 120 buckets per request; a simple time sort avoids relying
	// on hit order, which is not stable across shard layouts.
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Start < buckets[j].Start })
	for _, b := range buckets {
		for i, name := range b.Names {
			if allowed(name) {
				lists[name] = append(lists[name], point{b.Start, b.Scores[i]})
				if len(lists) > summaryEntityLimit {
					return errBudget
				}
			}
		}
	}
	best := make([]winner, 0, q.N)
	for name, points := range lists {
		candidate := winner{name: name, score: -1}
		for _, p := range points {
			candidate.score = max(candidate.score, p.value)
		}
		best = keepWinner(best, candidate, q.N)
	}
	w := int64(q.Width) * 1000
	for _, row := range best {
		curve := make([][]interface{}, 0, len(lists[row.name]))
		var prev int64
		for i, p := range lists[row.name] {
			if i > 0 && p.ts-prev > w {
				curve = append(curve, []interface{}{prev + w, nil})
			}
			curve = append(curve, []interface{}{p.ts, p.value})
			prev = p.ts
		}
		display(q, metric, row.name, curve)
	}
	return nil
}
