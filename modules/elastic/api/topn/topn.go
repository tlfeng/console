package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/env"
	"infini.sh/framework/core/global"
	"infini.sh/framework/core/util"
	"infini.sh/framework/modules/elastic/common"
)

const (
	Unsupported        = "unsupported"
	HistoryUnavailable = "history_unavailable"
	InvalidRequest     = "invalid_request"
	BudgetExceeded     = "budget_exceeded"
	QueryFailed        = "query_failed"
	NoData             = "no_data"
)

var errBudget = errors.New("automatic TopN budget exceeded")

type config struct {
	TopN struct {
		Clusters     []string `config:"clusters"`
		DisabledKeys []string `config:"disabled_keys"`
		RawIndex     string   `config:"raw_index"`
		RawFrom      string   `config:"raw_from"`
	} `config:"topn"`
}

// Policy leaves non-enrolled clusters and callers outside Advanced unchanged.
func Policy(req *http.Request, cluster string) (config, bool) {
	var cfg config
	if req.URL.Query().Get("advanced_topn") != "true" {
		return cfg, false
	}
	if _, err := env.ParseConfig("insight", &cfg); err != nil {
		return cfg, false
	}
	for _, id := range cfg.TopN.Clusters {
		if cluster == id {
			return cfg, true
		}
	}
	return cfg, false
}

func Status(w http.ResponseWriter, status string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"topn_status": status})
}

func Disabled(cfg config, source, key string) bool {
	for _, disabled := range cfg.TopN.DisabledKeys {
		if disabled == source+":"+key {
			return true
		}
	}
	return false
}

// Window preserves the exact client bounds rather than the legacy seconds conversion.
func Window(req *http.Request, width int) (int64, int64, int, bool) {
	q := req.URL.Query()
	min, err := parseTime(q.Get("min"))
	if err != nil {
		return 0, 0, 0, false
	}
	max, err := parseTime(q.Get("max"))
	if err != nil || min < 0 || max <= min || width <= 0 || width > 86400*365 {
		return 0, 0, 0, false
	}
	n := 5
	if raw := q.Get("top"); raw != "" {
		n, err = strconv.Atoi(raw)
		if err != nil || n < 1 || n > 20 {
			return 0, 0, 0, false
		}
	}
	if raw := q.Get("bucket_size"); raw != "" {
		d, err := util.ParseDuration(raw)
		if err != nil || d < time.Second || d%time.Second != 0 {
			return 0, 0, 0, false
		}
	}
	return min, max, n, true
}

func parseTime(raw string) (int64, error) {
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return ms, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

type Metric struct {
	Item                         *common.MetricItem
	Field, Field2, Units, Format string
	Counter                      bool
}

type Query struct {
	Cluster, Source string
	Min, Max        int64
	Width, N        int
	AllIndices      bool
	Indices         []string
}

type dsl = map[string]interface{}

// Index authorization comes from Console's radix patterns, not a query_string.
// Translate the ordinary unescaped '*' subset; reject patterns we cannot translate.
func (q Query) filter() (dsl, bool) {
	filters := []dsl{
		{"term": dsl{"metadata.labels.cluster_id": q.Cluster}},
		{"term": dsl{"metadata.category": "elasticsearch"}},
		{"term": dsl{"metadata.name": q.Source}},
	}
	pred := dsl{"filter": filters}
	if q.Source == "index_stats" {
		pred["must_not"] = []dsl{{"term": dsl{q.entity(): "_all"}}}
		if !q.AllIndices {
			if len(q.Indices) == 0 || len(q.Indices) > 1024 {
				return nil, false
			}
			allowed := make([]dsl, 0, len(q.Indices))
			for _, pattern := range q.Indices {
				if !aclPattern.MatchString(pattern) || strings.Contains(pattern, "**") {
					return nil, false
				}
				allowed = append(allowed, dsl{"wildcard": dsl{q.entity(): pattern}})
			}
			pred["filter"] = append(filters, dsl{"bool": dsl{"should": allowed, "minimum_should_match": 1}})
		}
	}
	return pred, true
}

func (q Query) entity() string {
	if q.Source == "index_stats" {
		return "metadata.labels.index_name"
	}
	return "metadata.labels.transport_address"
}

func filterRange(min, max int64) dsl {
	return dsl{"range": dsl{"timestamp": dsl{"gte": min, "lte": max, "format": "epoch_millis"}}}
}
func maximum(field string) dsl { return dsl{"max": dsl{"field": field}} }
func histogram(width int, aggs dsl) dsl {
	return dsl{"date_histogram": dsl{"field": "timestamp", "fixed_interval": fmt.Sprintf("%ds", width), "min_doc_count": 1}, "aggs": aggs}
}

var aclPattern = regexp.MustCompile(`^[a-z0-9._+*\-]+$`)

var rawName = regexp.MustCompile(`^[.a-z0-9_][.a-z0-9_\-]*$`)

type requester interface {
	GetEndpoint() string
	Request(context.Context, string, string, []byte) (*util.Result, error)
}

type caller func(context.Context, string, string, dsl) ([]byte, error)

func monitoredStore() (caller, bool) {
	id, _ := global.Lookup(elastic.GlobalSystemElasticsearchID).(string)
	if id == "" {
		return nil, false
	}
	client := elastic.GetClientNoPanic(id)
	if client == nil || client.GetVersion().Distribution != elastic.Easysearch {
		return nil, false
	}
	transport, ok := client.(requester)
	if !ok {
		return nil, false
	}
	base := strings.TrimRight(transport.GetEndpoint(), "/")
	if base == "" {
		return nil, false
	}
	return func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
		var wire []byte
		if body != nil {
			var err error
			wire, err = json.Marshal(body)
			if err != nil {
				return nil, err
			}
		}
		res, err := transport.Request(ctx, method, base+path, wire)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if res == nil {
			return nil, errors.New("missing monitoring response")
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("monitoring store returned HTTP %d", res.StatusCode)
		}
		return res.Body, nil
	}, true
}

type shardCounts struct {
	Total      int  `json:"total"`
	Successful int  `json:"successful"`
	Failed     *int `json:"failed"`
}

func (s shardCounts) complete() bool {
	return s.Total > 0 && s.Successful == s.Total && s.Failed != nil && *s.Failed == 0
}

type snapshot struct {
	call   caller
	id     string
	shards int
	bytes  int
}

func open(ctx context.Context, call caller, index string) (*snapshot, error) {
	const path = "/_pit?keep_alive=30s&allow_partial_pit_creation=false&allow_no_indices=false&ignore_unavailable=false&expand_wildcards=open,hidden"
	raw, err := call(ctx, http.MethodPost, "/"+index+path, nil)
	if err != nil {
		return nil, err
	}
	var response struct {
		ID     string      `json:"pit_id"`
		Shards shardCounts `json:"_shards"`
	}
	err = json.Unmarshal(raw, &response)
	s := &snapshot{call: call, id: response.ID, shards: response.Shards.Total}
	if err != nil || s.id == "" || !response.Shards.complete() {
		if s.id != "" {
			s.close()
		}
		return nil, errors.New("incomplete monitoring snapshot")
	}
	if s.shards > 32 {
		s.close()
		return nil, errBudget
	}
	return s, nil
}

func (s *snapshot) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.call(ctx, http.MethodDelete, "/_pit", dsl{"pit_id": []string{s.id}}); err != nil {
		log.Warnf("close monitoring TopN snapshot: %v", err)
	}
}

type point struct {
	Time  int64 `json:"key"`
	Value *struct {
		Value *float64 `json:"value"`
	} `json:"value"`
}

type bucket struct {
	Key  string `json:"key"`
	Peak *struct {
		Value *float64 `json:"value"`
	} `json:"peak"`
	Dates *struct {
		Buckets []point `json:"buckets"`
	} `json:"dates"`
}

func (s *snapshot) search(ctx context.Context, pred dsl, agg dsl, result interface{}) error {
	body := dsl{"size": 0, "track_total_hits": false, "query": dsl{"bool": pred}, "aggs": dsl{"entities": agg}, "pit": dsl{"id": s.id, "keep_alive": "30s"}}
	raw, err := s.call(ctx, http.MethodPost, "/_search?allow_partial_search_results=false", body)
	if err != nil {
		return err
	}
	var response struct {
		ID         string      `json:"pit_id"`
		Shards     shardCounts `json:"_shards"`
		TimedOut   *bool       `json:"timed_out"`
		Terminated bool        `json:"terminated_early"`
		Aggs       struct {
			Entities json.RawMessage `json:"entities"`
		} `json:"aggregations"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.ID != "" {
		s.id = response.ID
	}
	s.bytes += len(raw)
	if s.bytes > 16<<20 {
		return errBudget
	}
	if !response.Shards.complete() || response.Shards.Total != s.shards || response.TimedOut == nil || *response.TimedOut || response.Terminated {
		return errors.New("incomplete monitoring search")
	}
	return json.Unmarshal(response.Aggs.Entities, result)
}

func terms(field string, n int) dsl { return dsl{"field": field, "size": n, "shard_size": n} }

func display(q Query, metric Metric, name string, data [][]interface{}) {
	line := metric.Item.AddLine(metric.Item.Key, name, "", "group1", metric.Field, "max", fmt.Sprintf("%ds", q.Width), metric.Units, metric.Format, "0.[00]", "0.[00]", false, metric.Counter)
	line.TimeRange = common.TimeRange{Min: q.Min, Max: q.Max}
	line.Data = data
}

func peaks(ctx context.Context, s *snapshot, q Query, pred dsl, metric Metric) error {
	filters := pred["filter"].([]dsl)
	pred["filter"] = append(filters, filterRange(q.Min, q.Max), dsl{"range": dsl{metric.Field: dsl{"gte": 0}}})
	t := terms(q.entity(), q.N)
	t["order"] = []dsl{{"peak": "desc"}, {"_key": "asc"}}
	var rank struct {
		Buckets []bucket `json:"buckets"`
	}
	if err := s.search(ctx, pred, dsl{"terms": t, "aggs": dsl{"peak": maximum(metric.Field)}}, &rank); err != nil {
		return err
	}
	if rank.Buckets == nil || len(rank.Buckets) > q.N {
		return errors.New("missing or oversized TopN ranking")
	}
	if len(rank.Buckets) == 0 {
		return nil
	}
	names := make([]string, 0, len(rank.Buckets))
	scores := make(map[string]float64, len(rank.Buckets))
	for _, b := range rank.Buckets {
		if _, exists := scores[b.Key]; exists || b.Key == "" || b.Peak == nil || b.Peak.Value == nil {
			return errors.New("invalid TopN ranking")
		}
		names = append(names, b.Key)
		scores[b.Key] = *b.Peak.Value
	}
	pred["filter"] = append(pred["filter"].([]dsl), dsl{"terms": dsl{q.entity(): names}})
	var curves struct {
		Buckets []bucket `json:"buckets"`
	}
	if err := s.search(ctx, pred, dsl{"terms": terms(q.entity(), len(names)), "aggs": dsl{"dates": histogram(q.Width, dsl{"value": maximum(metric.Field)})}}, &curves); err != nil {
		return err
	}
	if len(curves.Buckets) != len(names) {
		return errors.New("missing TopN curves")
	}
	data := make(map[string][][]interface{}, len(names))
	for _, b := range curves.Buckets {
		score, ok := scores[b.Key]
		if !ok || data[b.Key] != nil || b.Dates == nil || len(b.Dates.Buckets) == 0 {
			return errors.New("invalid TopN curve")
		}
		points := make([][]interface{}, 0, len(b.Dates.Buckets))
		peak := -1.0
		for _, p := range b.Dates.Buckets {
			if p.Value == nil || p.Value.Value == nil {
				return errors.New("missing peak sample")
			}
			v := *p.Value.Value
			points = append(points, []interface{}{p.Time, v})
			if v > peak {
				peak = v
			}
		}
		if peak != score {
			return errors.New("ranking does not match curve")
		}
		data[b.Key] = points
	}
	for _, name := range names {
		display(q, metric, name, data[name])
	}
	return nil
}

var slots = make(chan struct{}, 4)

func Run(w http.ResponseWriter, r *http.Request, cfg config, q Query, metric Metric) {
	pred, ok := q.filter()
	if !ok {
		Status(w, Unsupported)
		return
	}
	min := q.Min
	if metric.Counter && q.previousBucket() >= 0 {
		min = q.previousBucket()
	}
	from, err := parseTime(cfg.TopN.RawFrom)
	if err != nil || from < 0 || min < from || !rawName.MatchString(cfg.TopN.RawIndex) || cfg.TopN.RawIndex == "_all" {
		Status(w, HistoryUnavailable)
		return
	}
	if q.Max/(int64(q.Width)*1000)-q.Min/(int64(q.Width)*1000)+1 > 512 {
		Status(w, BudgetExceeded)
		return
	}
	call, ok := monitoredStore()
	if !ok {
		Status(w, Unsupported)
		return
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		Status(w, BudgetExceeded)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	err = func() error {
		s, err := open(ctx, call, cfg.TopN.RawIndex)
		if err != nil {
			return err
		}
		defer s.close()
		if metric.Counter {
			return counters(ctx, s, q, pred, metric)
		}
		return peaks(ctx, s, q, pred, metric)
	}()
	if err != nil {
		if errors.Is(err, errBudget) || errors.Is(err, context.DeadlineExceeded) {
			Status(w, BudgetExceeded)
		} else {
			log.Warnf("automatic monitoring TopN: %v", err)
			Status(w, QueryFailed)
		}
		return
	}
	if len(metric.Item.Lines) == 0 {
		Status(w, NoData)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"metrics": map[string]*common.MetricItem{metric.Item.Key: metric.Item}})
}
