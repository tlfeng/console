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
	errBusy           = errors.New("automatic TopN query slots are busy")
	errBudget         = errors.New("automatic TopN budget exceeded")
	errCandidateLimit = fmt.Errorf("single search candidate limit: %w", errBudget)
	errRawHistory     = errors.New("automatic TopN raw history unavailable")
)

type config struct {
	TopN struct {
		RawIndex string `config:"raw_index"`
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
	legacyInterval  bool
	easysearch      bool
	// pageBudget overrides the interactive composite page cap for background
	// scans; zero keeps the interactive budget.
	pageBudget int
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
	q.easysearch = version.Distribution == elastic.Easysearch
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

type monitoringHTTPError int

func (e monitoringHTTPError) Error() string {
	return fmt.Sprintf("monitoring store returned HTTP %d", e)
}

func monitoredStore() (caller, elastic.Version) {
	id, _ := global.Lookup(elastic.GlobalSystemElasticsearchID).(string)
	if id == "" {
		return nil, elastic.Version{}
	}
	client := elastic.GetClientNoPanic(id)
	if client == nil {
		return nil, elastic.Version{}
	}
	transport, ok := client.(requester)
	if !ok {
		return nil, elastic.Version{}
	}
	base := strings.TrimRight(transport.GetEndpoint(), "/")
	if base == "" {
		return nil, elastic.Version{}
	}
	return func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var wire []byte
		if body != nil {
			var err error
			wire, err = json.Marshal(body)
			if err != nil {
				return nil, err
			}
		}
		res, err := transport.Request(ctx, method, base+path, wire)
		// Keep any returned PIT ID available for cleanup even on cancellation.
		if ctxErr := ctx.Err(); ctxErr != nil {
			if err == nil && res != nil {
				return res.Body, ctxErr
			}
			return nil, ctxErr
		}
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, errors.New("missing monitoring response")
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return res.Body, monitoringHTTPError(res.StatusCode)
		}
		return res.Body, nil
	}, client.GetVersion()
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

func (r searchResponse) validate() error {
	if r.TimedOut != nil && *r.TimedOut {
		return context.DeadlineExceeded
	}
	if r.TimedOut == nil || r.Terminated || !r.Shards.complete() {
		return errors.New("incomplete monitoring search")
	}
	return nil
}

type aggregateSearch func(context.Context, dsl, dsl, interface{}) error

func (s *snapshot) search(ctx context.Context, pred dsl, agg dsl, result interface{}) error {
	body := dsl{"size": 0, "track_total_hits": false, "query": dsl{"bool": pred}, "aggs": dsl{"entities": agg}, "pit": dsl{"id": s.id, "keep_alive": "30s"}}
	if err := searchTimeout(ctx, body); err != nil {
		return err
	}
	raw, err := s.call(ctx, http.MethodPost, "/_search?allow_partial_search_results=false", body)
	var response searchResponse
	decodeErr := json.Unmarshal(raw, &response)
	if response.ID != "" {
		s.id = response.ID
	}
	if err != nil {
		return err
	}
	if decodeErr != nil {
		return decodeErr
	}
	if err := response.validate(); err != nil {
		return err
	}
	if response.Origin != "" || response.Shards.Total != s.shards {
		return errors.New("unexpected monitoring snapshot source")
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
	return peakSeries(ctx, s.search, q, pred, metric)
}

func peakSeries(ctx context.Context, search aggregateSearch, q Query, pred dsl, metric Metric) error {
	t := terms(q.entity(), q.N)
	t["order"] = []dsl{{"peak": "desc"}, {"_key": "asc"}}
	var rank struct {
		Buckets []bucket `json:"buckets"`
	}
	if err := search(ctx, pred, dsl{"terms": t, "aggs": dsl{"peak": maximum(metric.Field)}}, &rank); err != nil {
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
		if !allowed(b.Key) || (q.Source == "index_stats" && b.Key == "_all") {
			return errors.New("unexpected entity in TopN authorization filter")
		}
		if _, exists := scores[b.Key]; exists || b.Key == "" || b.Peak == nil || b.Peak.Value == nil || !validCounter(*b.Peak.Value) {
			return errors.New("invalid TopN ranking")
		}
		names = append(names, b.Key)
		scores[b.Key] = *b.Peak.Value
	}
	pred["filter"] = append(pred["filter"].([]dsl), dsl{"terms": dsl{q.entity(): names}})
	var curves struct {
		Buckets []bucket `json:"buckets"`
	}
	err := search(ctx, pred, dsl{"terms": terms(q.entity(), len(names)), "aggs": dsl{"dates": q.histogram(dsl{"value": maximum(metric.Field)})}}, &curves)
	if err != nil {
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

var slots = make(chan struct{}, 4)

// longWindow is the span at which an automatic chart routes to the shared
// background registry: beyond it, scanning is the only way to produce the
// chart, so the interactive deadline no longer applies.
const longWindow = 24 * time.Hour

func (q Query) longAutomaticWindow() bool {
	// The query includes both endpoint milliseconds.
	return q.selected == nil && q.Max-q.Min >= longWindow.Milliseconds()-1
}

var errUnavailable = errors.New("automatic TopN unavailable")

func failureStatus(err error) string {
	switch {
	case errors.Is(err, errBusy):
		return "busy"
	case errors.Is(err, errRawHistory):
		return "history_unavailable"
	// A store-side timeout is the same user advice as an exceeded budget.
	case errors.Is(err, errBudget), errors.Is(err, context.DeadlineExceeded):
		return "budget_exceeded"
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

func (cfg config) rawSource() (string, error) {
	index := cfg.TopN.RawIndex
	if index == "" {
		// Use the original write alias rather than the legacy search wildcard.
		index = orm.GetIndexName(event.Event{})
	}
	if !rawName.MatchString(index) || index == "_all" {
		return "", errRawHistory
	}
	return index, nil
}

func calculate(ctx context.Context, q Query, metric Metric) error {
	if q.selected != nil && len(q.selected) == 0 {
		return nil
	}
	var cfg config
	if _, err := env.ParseConfig("insight", &cfg); err != nil {
		return errUnavailable
	}
	return calculateWithConfig(ctx, q, metric, cfg, monitoredStore)
}

// calculateWithConfig serves the interactive paths only: windows shorter than
// longWindow and hand-picked selections. Longer automatic charts are computed
// by the background registry in async.go, so no deadline juggling happens here.
func calculateWithConfig(ctx context.Context, q Query, metric Metric, cfg config, store func() (caller, elastic.Version)) error {
	pred, ok := q.filter()
	if !ok {
		return errUnavailable
	}
	index, err := cfg.rawSource()
	if err != nil {
		return err
	}
	if q.Max/(int64(q.Width)*1000)-q.Min/(int64(q.Width)*1000)+1 > 512 {
		return errBudget
	}
	call, version := store()
	if call == nil {
		return errUnavailable
	}
	q.setSearchVersion(version)
	if q.selected != nil {
		return withSlot(func() error { return runSelected(ctx, call, index, pred, q, metric) })
	}
	return withSlot(func() error {
		if tryRollupPeak(ctx, call, index, q, metric) {
			return nil
		}
		return runQuery(ctx, call, pitFor(version), index, pred, q, metric)
	})
}

func withSlot(work func() error) error {
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		return errBusy
	}
	return work()
}

func runQuery(ctx context.Context, call caller, api pitAPI, index string, pred dsl, q Query, metric Metric) error {
	if api.path == "" {
		return singleSearch(ctx, call, index, pred, q, metric)
	}
	// Node and index charts prefer one strict snapshot; the bounded single
	// search is the fallback when the store cannot open one.
	s, err := open(ctx, call, index, api)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, errBudget) {
			return err
		}
		return singleSearch(ctx, call, index, pred, q, metric)
	}
	defer s.close()
	if metric.mode != peak {
		return paged(ctx, s, q, pred, metric)
	}
	return peaks(ctx, s, q, pred, metric)
}

func writeMetrics(w http.ResponseWriter, metric Metric) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"metrics": map[string]*common.MetricItem{metric.Item.Key: metric.Item}})
}
