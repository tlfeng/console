package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	cfg "infini.sh/framework/core/config"
)

func TestOnDemandConfig(t *testing.T) {
	raw := []byte("topn:\n  summary:\n    enabled: true\n    writer: false\n")
	input, err := cfg.NewConfigWithYAML(raw, "test")
	if err != nil {
		t.Fatal(err)
	}
	var settings config
	if err := input.Unpack(&settings); err != nil {
		t.Fatal(err)
	}
	c := settings.TopN.Summary
	if c.writes() || !c.enabled("biz", queryLatencyKey) || len(c.metricKeys()) != 4 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	m, err := c.store("raw")
	if err != nil || m.index != ".console-topn-index-rate-v2" {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestOnDemandQueue(t *testing.T) {
	jobs := summaryQueue{jobs: make(map[string]*summaryJob), running: make(chan struct{}, 1)}
	m := summaryMetric(writeRateKey)
	store := summaryStore{raw: "raw", index: ".console-topn-test"}
	q := Query{Source: "index_stats", Cluster: "biz", Min: 1000, Max: 2000, Width: 1, N: 1, Indices: []string{"b", "a"}}
	key := summaryJobKey(store, q, m)
	q.Indices = []string{"a", "b"}
	if key != summaryJobKey(store, q, m) {
		t.Fatal("different ACL ordering must not change the key")
	}
	q.Indices = []string{"b"}
	if key == summaryJobKey(store, q, m) {
		t.Fatal("narrowed ACL must change the key")
	}
	release := make(chan struct{})
	work := func(ctx context.Context, metric Metric) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer close(release)
	for i := range summaryJobLimit {
		k := key + string(rune('a'+i))
		job, err := jobs.submit(k, m, work)
		if err != nil || job == nil {
			t.Fatal(err)
		}
		if found, _ := jobs.lookup(k, m); found {
			t.Fatal("a running job must not be served")
		}
		again, err := jobs.submit(k, m, work)
		if err != nil || again != job {
			t.Fatal("a running job must be joined, not duplicated", err)
		}
	}
	if _, err := jobs.submit("overflow", m, work); !errors.Is(err, errSummaryBusy) {
		t.Fatal(err)
	}
}

func TestRetainedJobSlotSaturation(t *testing.T) {
	for i := 0; i < cap(slots); i++ {
		slots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(slots); i++ {
			<-slots
		}
	}()
	const key = "retained-slot-test"
	m := summaryMetric(writeRateKey)
	saved := summaryMetric(writeRateKey)
	saved.rawProof = new(string)
	job := &summaryJob{done: make(chan struct{}), expires: time.Now().Add(time.Minute), metric: saved}
	onDemandSummaries.mu.Lock()
	onDemandSummaries.jobs[key] = job
	onDemandSummaries.mu.Unlock()
	defer func() {
		onDemandSummaries.mu.Lock()
		delete(onDemandSummaries.jobs, key)
		onDemandSummaries.mu.Unlock()
	}()
	lookup := func() (bool, error) {
		return lookupRetainedJob(context.Background(), nil, pitAPI{}, summaryStore{}, Query{}, m, time.Now(), key)
	}
	if found, err := lookup(); found || err != nil {
		t.Fatalf("a running job must be reported as absent: found=%v, err=%v", found, err)
	}
	close(job.done)
	if found, err := lookup(); !found || !errors.Is(err, errSummaryBusy) {
		t.Fatalf("completed job must report busy when validation cannot get a slot: found=%v, err=%v", found, err)
	}
}

func TestReadOnDemandWithoutWriteAllowance(t *testing.T) {
	for _, tc := range []struct {
		name, key  string
		allIndices bool
	}{
		{"read-only metric", queryRateKey, true},
		{"restricted ACL", writeRateKey, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := summaryConfig{Enabled: true, Metrics: []string{writeRateKey}, ReadMetrics: []string{writeRateKey, queryRateKey}}
			q := Query{Source: "index_stats", Cluster: "biz", Min: 2000, Max: 3999, Width: 1, N: 1, AllIndices: tc.allIndices, storeScope: tc.name}
			if !q.AllIndices {
				q.Indices = []string{"x"}
			}
			m := summaryStore{raw: "raw", index: ".console-topn-test"}
			metric := summaryMetric(tc.key)
			api := pitAPI{path: "/_pit", idField: "pit_id"}
			now := time.Unix(2000, 0)
			const shard = `"_shards":{"total":1,"successful":1,"failed":0}`
			proof := rawBucketProof{1000: {"raw\x00uuid": 1}, 2000: {"raw\x00uuid": 1}, 3000: {"raw\x00uuid": 1}}
			var summaryReads, rawTransitions int
			call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
				switch {
				case method == http.MethodGet && strings.HasPrefix(path, "/raw/_settings/index.uuid?"):
					return []byte(`{"raw":{"settings":{"index.uuid":"uuid"}}}`), nil
				case method == http.MethodPost && (strings.HasPrefix(path, "/raw/_pit?") || strings.HasPrefix(path, "/"+m.index+"/_pit?")):
					return []byte(`{"pit_id":"p",` + shard + `}`), nil
				case method == http.MethodDelete && path == "/_pit":
					return []byte(`{"pits":[{"pit_id":"p","successful":true}]}`), nil
				case method == http.MethodPost && strings.HasPrefix(path, "/_search?"):
					if query := body["query"].(dsl); query["ids"] != nil {
						summaryReads++
						digest := proof.transition(2000, 1000)
						id := m.provenID(q, tc.key, 2000, digest)
						return []byte(fmt.Sprintf(`{"timed_out":false,%s,"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":%q,"_source":{"v":%d,"t":2000,"n":["x"],"s":[12],"p":%q}}]}}`, shard, id, summaryVersion, digest)), nil
					}
					entity := body["aggs"].(dsl)["entities"].(dsl)
					if entity["composite"] == nil {
						var points []dsl
						for _, ts := range []int64{1000, 2000, 3000} {
							points = append(points, dsl{"key": ts, "doc_count": 1, "storage": dsl{"sum_other_doc_count": 0, "buckets": []dsl{{"key": "raw", "doc_count": 1}}}})
						}
						return json.Marshal(dsl{"timed_out": false, "_shards": dsl{"total": 1, "successful": 1, "failed": 0}, "aggregations": dsl{"entities": dsl{"buckets": points}}})
					}
					rawTransitions++
					return []byte(`{"timed_out":false,` + shard + `,"aggregations":{"entities":{"buckets":[{"key":{"entity":"x"},"dates":{"buckets":[{"key":2000,"doc_count":1,"value":{"value":10},"identity":{"buckets":[{"key":"uuid","doc_count":1}]}},{"key":3000,"doc_count":1,"value":{"value":15},"identity":{"buckets":[{"key":"uuid","doc_count":1}]}}]}}]}}}`), nil
				}
				return nil, fmt.Errorf("unexpected provisioning or publish request: %s %s", method, path)
			}
			// The request joins the background job and returns its result directly.
			if err := readOnDemand(context.Background(), context.Background(), call, api, m, q, metric, now, cfg); err != nil {
				t.Fatalf("synchronous wait must return the finished job: %v", err)
			}
			key := summaryJobKey(m, q, metric)
			t.Cleanup(func() {
				onDemandSummaries.mu.Lock()
				delete(onDemandSummaries.jobs, key)
				onDemandSummaries.mu.Unlock()
			})
			if summaryReads != 2 || rawTransitions != 1 {
				t.Fatalf("summary reads=%d raw scans=%d", summaryReads, rawTransitions)
			}
			want := [][]interface{}{{int64(2000), 12.0}, {int64(3000), 5.0}}
			if len(metric.Item.Lines) != 1 || !reflect.DeepEqual(metric.Item.Lines[0].Data, want) {
				t.Fatalf("expected reused summary and recomputed transition, got %v", metric.Item.Lines)
			}
			found, err := lookupRetainedJob(context.Background(), call, api, m, q, metric, now, key)
			if !found || err != nil {
				t.Fatalf("retained result must validate: found=%v err=%v", found, err)
			}
		})
	}
}

func TestMissingSummaryTargetOnlyAllowedWithoutPublishing(t *testing.T) {
	m := summaryStore{raw: "raw", index: ".console-topn-test"}
	q := Query{Source: "index_stats", Cluster: "biz", Min: 2000, Max: 2999, Width: 1, N: 1, AllIndices: true}
	api := pitAPI{path: "/_pit", idField: "pit_id"}
	for _, tc := range []struct {
		publish bool
		status  monitoringHTTPError
		want    error
	}{
		{false, 404, nil},
		{true, 404, errSummaryTargetMissing},
		{false, 403, monitoringHTTPError(403)},
	} {
		t.Run(fmt.Sprintf("publish=%v HTTP=%d", tc.publish, tc.status), func(t *testing.T) {
			var searches int
			call := func(_ context.Context, method, path string, _ dsl) ([]byte, error) {
				switch {
				case method == http.MethodGet && strings.HasPrefix(path, "/raw/_settings/index.uuid?"):
					return []byte(`{"raw":{"settings":{"index.uuid":"uuid"}}}`), nil
				case method == http.MethodPost && strings.HasPrefix(path, "/raw/_pit?"):
					return []byte(`{"pit_id":"p",` + allShards + `}`), nil
				case method == http.MethodDelete && path == "/_pit":
					return []byte(`{"pits":[{"pit_id":"p","successful":true}]}`), nil
				case method == http.MethodPost && strings.HasPrefix(path, "/"+m.index+"/_pit?"):
					return nil, tc.status
				case method == http.MethodPost && strings.HasPrefix(path, "/_search?"):
					searches++
					if searches == 1 {
						const rows = `{"buckets":[{"key":1000,"doc_count":1,"storage":{"sum_other_doc_count":0,"buckets":[{"key":"raw","doc_count":1}]}},{"key":2000,"doc_count":1,"storage":{"sum_other_doc_count":0,"buckets":[{"key":"raw","doc_count":1}]}}]}`
						return result(rows), nil
					}
					return result(`{"buckets":[]}`), nil
				}
				t.Fatalf("unexpected request: %s %s", method, path)
				return nil, nil
			}
			err := computeMissingSummaries(context.Background(), call, api, m, q, summaryMetric(writeRateKey), time.Unix(2000, 0), tc.publish)
			wantSearches := 1
			if tc.want == nil {
				wantSearches++ // The transient path recomputes the missing sealed transition.
			}
			if !errors.Is(err, tc.want) || searches != wantSearches {
				t.Fatalf("got %v and %d searches, want %v and %d searches", err, searches, tc.want, wantSearches)
			}
		})
	}
}

func TestSummaryWithoutRawPolicyDoesNotCreateIndex(t *testing.T) {
	m := summaryStore{index: ".console-topn-test", raw: "raw"}
	call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
		if method != http.MethodGet {
			t.Fatalf("unexpected mutation: %s %s", method, path)
		}
		if path == "/"+m.index+"/_mapping" {
			return nil, monitoringHTTPError(404)
		}
		return []byte(`{"raw":{"settings":{"index.uuid":"original"}}}`), nil
	}
	if err := ensureSummaryIndex(context.Background(), call, m, pitAPI{}); !errors.Is(err, errSummaryNoPolicy) {
		t.Fatal(err)
	}
}

func TestOnDemandEmptyDoesNotPublish(t *testing.T) {
	q := Query{Source: "index_stats", Cluster: "biz", Min: 1000, Max: 2999, Width: 1, N: 1, AllIndices: true}
	m := summaryStore{raw: "raw", index: ".console-topn-test"}
	calls := 0
	call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
		if method == http.MethodGet {
			return []byte(`{"raw":{"settings":{"index.uuid":"uuid"}}}`), nil
		}
		if method == http.MethodPut {
			t.Fatal("empty bucket must not be persisted")
		}
		if method == http.MethodDelete {
			return []byte(`{"pits":[{"pit_id":"p","successful":true}]}`), nil
		}
		if body == nil {
			return []byte(`{"pit_id":"p","_shards":{"total":1,"successful":1,"failed":0}}`), nil
		}
		calls++
		return []byte(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"aggregations":{"entities":{"buckets":[]}}}`), nil
	}
	metric := summaryMetric(writeRateKey)
	if err := computeMissingSummaries(context.Background(), call, pitAPI{path: "/_pit", idField: "pit_id"}, m, q, metric, time.Unix(2000, 0), true); err != nil || len(metric.Item.Lines) != 0 || calls != 1 {
		t.Fatal(err, calls)
	}
}
