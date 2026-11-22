package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/env"
	"infini.sh/framework/core/event"
	"infini.sh/framework/core/global"
	"infini.sh/framework/core/orm"
	"infini.sh/framework/core/radix"
	"infini.sh/framework/core/util"
	"infini.sh/framework/modules/elastic/common"
)

var (
	errBudget         = errors.New("automatic TopN budget exceeded")
	errCandidateLimit = fmt.Errorf("single search candidate limit: %w", errBudget)
	errLegacyJVM      = errors.New("some node samples still require individual JVM evidence")
	errRawHistory     = errors.New("automatic TopN raw history unavailable")
)

type config struct {
	TopN struct {
		RawIndex     string             `config:"raw_index"`
		RawFrom      string             `config:"raw_from"`
		Materialized materializedConfig `config:"materialized"`
	} `config:"topn"`
}

// Window preserves the exact client bounds rather than the legacy seconds conversion.
func Window(req *http.Request, width int) (int64, int64, int, bool) {
	min, max, ok := windowBounds(req, width)
	if !ok {
		return 0, 0, 0, false
	}
	n := 5
	if raw := req.URL.Query().Get("top"); raw != "" {
		var err error
		n, err = strconv.Atoi(raw)
		if err != nil || n < 1 || n > 20 {
			return 0, 0, 0, false
		}
	}
	return min, max, n, true
}

func windowBounds(req *http.Request, width int) (int64, int64, bool) {
	q := req.URL.Query()
	min, err := parseTime(q.Get("min"))
	if err != nil {
		return 0, 0, false
	}
	max, err := parseTime(q.Get("max"))
	if err != nil || min < 0 || max <= min || width <= 0 || width > 86400*365 {
		return 0, 0, false
	}
	if raw := q.Get("bucket_size"); raw != "" {
		d, err := util.ParseDuration(raw)
		if err != nil || d < time.Second || d%time.Second != 0 {
			return 0, 0, false
		}
	}
	return min, max, true
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

type Query struct {
	Cluster, Source string
	Min, Max        int64
	Width, N        int
	AllIndices      bool
	Indices         []string
	selected        []string
	rawFrom         int64
	storeScope      string
	legacyInterval  bool
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
	}
	if q.selected != nil {
		pred["filter"] = append(filters, dsl{"terms": dsl{q.entity(): q.selected}})
	} else if q.Source == "index_stats" && !q.AllIndices {
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
	return pred, true
}

// The engine's wildcard filter is a candidate filter. Verify returned keys
// with the same matcher used by Console's existing index authorization.
func (q Query) authorizer() func(string) bool {
	if q.selected != nil {
		return func(name string) bool { return slices.Contains(q.selected, name) }
	}
	if q.Source != "index_stats" || q.AllIndices {
		return func(string) bool { return true }
	}
	return radix.Compile(q.Indices...).Match
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
func (q Query) histogram(aggs dsl) dsl {
	field := "fixed_interval"
	if q.legacyInterval {
		field = "interval"
	}
	return dsl{"date_histogram": dsl{"field": "timestamp", field: fmt.Sprintf("%ds", q.Width), "min_doc_count": 1}, "aggs": aggs}
}

func (q *Query) setSearchVersion(version elastic.Version) {
	if version.Distribution != elastic.Elasticsearch && version.Distribution != "" {
		return
	}
	if cmp, err := util.VersionCompare(version.Number, "7.2.0"); err == nil {
		q.legacyInterval = cmp < 0
	}
}

var aclPattern = regexp.MustCompile(`^[a-z0-9._+*\-]+$`)

var rawName = regexp.MustCompile(`^[.a-z0-9_][.a-z0-9_\-]*$`)

type requester interface {
	GetEndpoint() string
	Request(context.Context, string, string, []byte) (*util.Result, error)
}

type caller func(context.Context, string, string, dsl) ([]byte, error)

func monitoredStore() (caller, elastic.Version, string) {
	id, _ := global.Lookup(elastic.GlobalSystemElasticsearchID).(string)
	if id == "" {
		return nil, elastic.Version{}, ""
	}
	client := elastic.GetClientNoPanic(id)
	if client == nil {
		return nil, elastic.Version{}, ""
	}
	transport, ok := client.(requester)
	if !ok {
		return nil, elastic.Version{}, ""
	}
	base := strings.TrimRight(transport.GetEndpoint(), "/")
	if base == "" {
		return nil, elastic.Version{}, ""
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
		if res == nil {
			return nil, errors.New("missing monitoring response")
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return res.Body, fmt.Errorf("monitoring store returned HTTP %d", res.StatusCode)
		}
		return res.Body, nil
	}, client.GetVersion(), id + "\x00" + base
}

type shardCounts struct {
	Total      int  `json:"total"`
	Successful int  `json:"successful"`
	Failed     *int `json:"failed"`
}

func (s shardCounts) complete() bool {
	return s.Total > 0 && s.Successful == s.Total && s.Failed != nil && *s.Failed == 0
}

type pitAPI struct {
	path, strictParam, idField string
}

func pitFor(version elastic.Version) pitAPI {
	supports := func(floor string) bool {
		cmp, err := util.VersionCompare(version.Number, floor)
		return err == nil && cmp >= 0
	}
	switch version.Distribution {
	case elastic.Easysearch:
		return pitAPI{"/_pit", "allow_partial_pit_creation", "pit_id"}
	case elastic.Opensearch:
		if supports("2.4.0") {
			return pitAPI{"/_search/point_in_time", "allow_partial_pit_creation", "pit_id"}
		}
	case elastic.Elasticsearch, "":
		if supports("8.16.0") {
			return pitAPI{"/_pit", "allow_partial_search_results", "id"}
		}
	}
	return pitAPI{}
}

type snapshot struct {
	call   caller
	api    pitAPI
	id     string
	shards int
	bytes  int
}

func open(ctx context.Context, call caller, index string, api pitAPI) (*snapshot, error) {
	path := "/" + index + api.path + "?keep_alive=30s&" + api.strictParam + "=false&allow_no_indices=false&ignore_unavailable=false&expand_wildcards=open,hidden"
	raw, err := call(ctx, http.MethodPost, path, nil)
	if err != nil && len(raw) == 0 {
		return nil, err
	}
	var response struct {
		ID     string      `json:"id"`
		PITID  string      `json:"pit_id"`
		Shards shardCounts `json:"_shards"`
	}
	decodeErr := json.Unmarshal(raw, &response)
	id := response.PITID
	if api.idField == "id" {
		id = response.ID
	}
	s := &snapshot{call: call, api: api, id: id, shards: response.Shards.Total}
	if err != nil || decodeErr != nil || s.id == "" || !response.Shards.complete() {
		if s.id != "" {
			s.close()
		}
		if err != nil {
			return nil, err
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
	body := dsl{"pit_id": []string{s.id}}
	if s.api.idField == "id" {
		body = dsl{"id": s.id}
	}
	raw, err := s.call(ctx, http.MethodDelete, s.api.path, body)
	if err == nil {
		err = checkPITClose(raw, s.id, s.api.idField == "id")
	}
	if err != nil {
		log.Warnf("close monitoring TopN snapshot: %v", err)
	}
}

func checkPITClose(raw []byte, id string, scalarID bool) error {
	var response struct {
		Succeeded bool `json:"succeeded"`
		PITs      []struct {
			ID         string `json:"pit_id"`
			Successful bool   `json:"successful"`
		} `json:"pits"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if scalarID && response.Succeeded {
		return nil
	}
	if !scalarID {
		for _, pit := range response.PITs {
			if pit.ID == id && pit.Successful {
				return nil
			}
		}
	}
	return errors.New("monitoring store did not acknowledge PIT deletion")
}

type bucket struct {
	Key  string `json:"key"`
	Peak *struct {
		Value *float64 `json:"value"`
	} `json:"peak"`
	Dates *struct {
		Buckets []samples `json:"buckets"`
	} `json:"dates"`
}

type searchResponse struct {
	Origin     string                     `json:"origin"`
	ID         string                     `json:"pit_id"`
	Shards     shardCounts                `json:"_shards"`
	TimedOut   *bool                      `json:"timed_out"`
	Terminated bool                       `json:"terminated_early"`
	Aggs       map[string]json.RawMessage `json:"aggregations"`
}

func (r searchResponse) complete() bool {
	return r.Shards.complete() && r.TimedOut != nil && !*r.TimedOut && !r.Terminated
}

func (s *snapshot) search(ctx context.Context, pred dsl, agg dsl, result interface{}) error {
	body := dsl{"size": 0, "track_total_hits": false, "query": dsl{"bool": pred}, "aggs": dsl{"entities": agg}, "pit": dsl{"id": s.id, "keep_alive": "30s"}}
	if err := searchTimeout(ctx, body); err != nil {
		return err
	}
	raw, err := s.call(ctx, http.MethodPost, "/_search?allow_partial_search_results=false", body)
	if err != nil && len(raw) == 0 {
		return err
	}
	if len(raw) > responseLimit-s.bytes {
		return errBudget
	}
	var response searchResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.ID != "" {
		s.id = response.ID
	}
	if err != nil {
		return err
	}
	s.bytes += len(raw)
	if response.TimedOut != nil && *response.TimedOut {
		return context.DeadlineExceeded
	}
	if !response.complete() || response.Shards.Total != s.shards {
		return errors.New("incomplete monitoring search")
	}
	return json.Unmarshal(response.Aggs["entities"], result)
}

func terms(field string, n int) dsl { return dsl{"field": field, "size": n, "shard_size": n} }

func display(q Query, metric Metric, name string, data [][]interface{}) {
	line := metric.Item.AddLine(metric.Item.Key, name, "", "group1", metric.Field, "max", fmt.Sprintf("%ds", q.Width), metric.Units, metric.Format, "0.[00]", "0.[00]", false, metric.mode.difference())
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
	allowed := q.authorizer()
	for _, b := range rank.Buckets {
		if !allowed(b.Key) {
			return errors.New("unexpected entity in TopN authorization filter")
		}
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
	if err := s.search(ctx, pred, dsl{"terms": terms(q.entity(), len(names)), "aggs": dsl{"dates": q.histogram(dsl{"value": maximum(metric.Field)})}}, &curves); err != nil {
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
		row, err := scoreSeries(q, metric, b.Key, b.Dates.Buckets)
		if err != nil {
			return err
		}
		if row.score != score {
			return errors.New("ranking does not match curve")
		}
		data[b.Key] = row.points
	}
	for _, name := range names {
		display(q, metric, name, data[name])
	}
	return nil
}

// retainedAt checks for original samples at or before the visible window starts,
// not at the start of the optional counter pre-read bucket. It uses
// the same snapshot. This is a history guard, not a guarantee of collection
// completeness or of retention for every entity.
func (s *snapshot) retainedAt(ctx context.Context, pred dsl, min int64) (bool, error) {
	coverage := dsl{
		"filter": filterRange(0, min),
	}
	var result struct {
		Count *int64 `json:"doc_count"`
	}
	if err := s.search(ctx, pred, coverage, &result); err != nil {
		return false, err
	}
	if result.Count == nil {
		return false, errors.New("missing raw sample coverage result")
	}
	return *result.Count > 0, nil
}

var slots = make(chan struct{}, 4)

var errUnavailable = errors.New("automatic TopN unavailable")

// Serve owns the response for supported Advanced automatic charts. A failed
// calculation must not be presented as a successful legacy activity ranking.
func Serve(w http.ResponseWriter, r *http.Request, q Query, metric Metric) {
	timeout, err := queryTimeout(r.URL.Query().Get("timeout"))
	if err != nil {
		WriteStatus(w, "invalid_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	err = calculate(ctx, q, metric)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if r.Context().Err() != nil {
		return
	}
	if err == nil {
		writeMetrics(w, metric)
		return
	}
	status := failureStatus(err)
	if status == "query_failed" {
		log.Warnf("automatic monitoring TopN: %v", err)
	}
	WriteStatus(w, status)
}

func failureStatus(err error) string {
	switch {
	case errors.Is(err, errSummaryHistory):
		return "summary_unavailable"
	case errors.Is(err, errRawHistory):
		return "history_unavailable"
	case errors.Is(err, errBudget):
		return "budget_exceeded"
	case errors.Is(err, context.DeadlineExceeded):
		return "timed_out"
	case errors.Is(err, errUnavailable):
		return "unavailable"
	default:
		return "query_failed"
	}
}

func WriteStatus(w http.ResponseWriter, status string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"topn_status": status})
}

func (cfg config) rawSource() (string, int64, error) {
	index := cfg.TopN.RawIndex
	if index == "" {
		// Use the original write alias, not the legacy search wildcard that
		// can also match summary indices.
		index = orm.GetIndexName(event.Event{})
	}
	if !rawName.MatchString(index) || index == "_all" {
		return "", 0, errRawHistory
	}
	if cfg.TopN.RawFrom == "" {
		return index, 0, nil
	}
	from, err := parseTime(cfg.TopN.RawFrom)
	if err != nil || from < 0 {
		return "", 0, errRawHistory
	}
	return index, from, nil
}

func calculate(ctx context.Context, q Query, metric Metric) error {
	if q.selected != nil && len(q.selected) == 0 {
		return nil
	}
	var cfg config
	if _, err := env.ParseConfig("insight", &cfg); err != nil {
		return errUnavailable
	}
	materialized := q.selected == nil && useMaterialized(q, metric) && cfg.TopN.Materialized.enabled(q.Cluster, metric.Item.Key)
	if materialized && !q.AllIndices && len(q.Indices) == 0 {
		return nil
	}
	pred, ok := q.filter()
	if !ok {
		return errUnavailable
	}
	index, from, err := cfg.rawSource()
	if err != nil {
		return err
	}
	if q.Min < from {
		return errRawHistory
	}
	q.rawFrom = from
	if q.Max/(int64(q.Width)*1000)-q.Min/(int64(q.Width)*1000)+1 > 512 {
		return errBudget
	}
	call, version, scope := monitoredStore()
	if call == nil {
		return errUnavailable
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		return errBudget
	}
	q.setSearchVersion(version)
	q.storeScope = scope
	if q.selected != nil {
		return runSelected(ctx, call, index, pred, q, metric, cfg.TopN.RawFrom == "")
	}
	if materialized {
		m, err := cfg.TopN.Materialized.store(index, from)
		if err != nil {
			return err
		}
		return readMaterialized(ctx, call, pitFor(version), m, q, metric, time.Now())
	}
	return runQuery(ctx, call, pitFor(version), index, pred, q, metric, cfg.TopN.RawFrom == "")
}

func withoutJVMStart() dsl {
	return dsl{"bool": dsl{"must_not": []dsl{{"range": dsl{jvmStart: dsl{"gt": 0}}}}}}
}

func missingJVMStarts(q Query) dsl {
	return dsl{"filter": dsl{"bool": dsl{"filter": []dsl{q.counterScope(), withoutJVMStart()}}}}
}

func hasMissingJVMStarts(raw json.RawMessage) (bool, error) {
	var result struct {
		Count *int64 `json:"doc_count"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return false, err
	}
	if result.Count == nil {
		return false, errors.New("missing JVM evidence count")
	}
	return *result.Count > 0, nil
}

func runQuery(ctx context.Context, call caller, api pitAPI, index string, pred dsl, q Query, metric Metric, checkHistory bool) error {
	call = budgetedCaller(call, responseLimit)
	nodeDifference := q.Source == "node_stats" && metric.mode.difference()
	single := func() error {
		m := metric
		m.jvmSummary = nodeDifference
		err := singleSearch(ctx, call, index, pred, q, m, checkHistory)
		if errors.Is(err, errLegacyJVM) && api.path == "" && ctx.Err() == nil {
			// The second single search collects all its own evidence, so it need
			// not share a view with the unsuccessful summary search.
			return singleSearch(ctx, call, index, pred, q, metric, checkHistory)
		}
		return err
	}
	hint, canLearn := nodeRoutingKey(q, metric, api, index, checkHistory)
	learned := canLearn && pitHints.contains(hint, time.Now())
	var singleErr error
	singleFirst := (q.Source == "node_stats" && !learned) || api.path == ""
	if singleFirst {
		singleErr = single()
		if singleErr == nil || errors.Is(singleErr, errRawHistory) || errors.Is(singleErr, errResponseBudget) || api.path == "" || ctx.Err() != nil {
			return singleErr
		}
	}
	s, err := open(ctx, call, index, api)
	if err != nil {
		if learned {
			pitHints.forget(hint)
		}
		if ctx.Err() == nil {
			if !singleFirst {
				singleErr = single()
			}
			if errors.Is(singleErr, errLegacyJVM) {
				return singleSearch(ctx, call, index, pred, q, metric, checkHistory)
			}
			if !singleFirst {
				return singleErr
			}
		}
		return err
	}
	defer s.close()
	if checkHistory {
		covered, err := s.retainedAt(ctx, pred, q.Min)
		if err != nil {
			return err
		}
		if !covered {
			return errRawHistory
		}
	}
	if nodeDifference {
		var raw json.RawMessage
		if err := s.search(ctx, pred, missingJVMStarts(q), &raw); err != nil {
			return err
		}
		missing, err := hasMissingJVMStarts(raw)
		if err != nil {
			return err
		}
		metric.jvmSummary = !missing
	}
	if metric.mode != peak {
		err = paged(ctx, s, q, pred, metric)
	} else {
		err = peaks(ctx, s, q, pred, metric)
	}
	if err == nil && canLearn && errors.Is(singleErr, errCandidateLimit) && ctx.Err() == nil {
		pitHints.remember(hint, time.Now())
	}
	return err
}

func writeMetrics(w http.ResponseWriter, metric Metric) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"metrics": map[string]*common.MetricItem{metric.Item.Key: metric.Item}})
}
