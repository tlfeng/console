package topn

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"infini.sh/framework/core/elastic"
	"infini.sh/framework/modules/elastic/common"
)

func TestRollupPeakRouting(t *testing.T) {
	const good = `"origin":"rollup","timed_out":false,` + allShards
	for _, tc := range []struct {
		name, acl, curveMeta, key string
		shift                     int64
		wantRollup, wantRaw       int
	}{
		{name: "pure", acl: "logs", curveMeta: good, wantRollup: 2},
		{name: "miss", acl: "logs", curveMeta: `"timed_out":false,` + allShards, wantRollup: 2, wantRaw: 1},
		{name: "mixed", acl: "logs", curveMeta: `"origin":"rollup,live_took:1,rollup_took:1","timed_out":false,` + allShards, wantRollup: 2, wantRaw: 1},
		{name: "failed shards", acl: "logs", curveMeta: `"origin":"rollup","timed_out":false,"_shards":{"total":2,"successful":1,"failed":1}`, wantRollup: 2, wantRaw: 1},
		{name: "missing timeout flag", acl: "logs", curveMeta: `"origin":"rollup",` + allShards, wantRollup: 2, wantRaw: 1},
		{name: "early termination", acl: "logs", curveMeta: good + `,"terminated_early":true`, wantRollup: 2, wantRaw: 1},
		{name: "unexpected winner", acl: "logs", key: "private", wantRollup: 1, wantRaw: 1},
		{name: "wildcard", acl: "log*", wantRaw: 1},
		{name: "unaligned", acl: "logs", shift: 1, wantRaw: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := Query{Cluster: "biz", Source: "index_stats", Min: 86400000 + tc.shift, Max: 172799999, Width: 3600, N: 1, Indices: []string{tc.acl}}
			metric := Metric{Item: &common.MetricItem{Key: "index_storage"}, Field: "payload.elasticsearch.index_stats.total.store.size_in_bytes"}
			var cfg config
			cfg.TopN.RawIndex = "monitor"
			var rollupCalls, rawCalls int
			call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
				if strings.Contains(path, "enable_rollup=true") {
					rollupCalls++
					wire, err := json.Marshal(body)
					if err != nil {
						t.Fatal(err)
					}
					for _, want := range []string{`"must":[{"term":{"metadata.name":"index_stats"}}]`, `"terms":{"metadata.labels.index_name":["logs"]}`, `"gte":86400000`, `"lte":172799999`} {
						if !strings.Contains(string(wire), want) {
							t.Fatalf("missing %s in %s", want, wire)
						}
					}
					if body["pit"] != nil || strings.Contains(string(wire), `"gte":0`) {
						t.Fatalf("unexpected rollup DSL: %s", wire)
					}
					if rollupCalls == 1 {
						key := tc.key
						if key == "" {
							key = "logs"
						}
						return []byte(`{` + good + `,"aggregations":{"entities":{"buckets":[{"key":"` + key + `","peak":{"value":19.5}}]}}}`), nil
					}
					return []byte(`{` + tc.curveMeta + `,"aggregations":{"entities":{"buckets":[{"key":"logs","dates":{"buckets":[{"key":86400000,"value":{"value":19.5}},{"key":93600000,"value":{"value":10}}]}}]}}}`), nil
				}
				switch {
				case strings.HasPrefix(path, "/monitor/_pit?"):
					return []byte(`{"pit_id":"fixed",` + allShards + `}`), nil
				case method == "DELETE" && path == "/_pit":
					return []byte(`{"pits":[{"pit_id":"fixed","successful":true}]}`), nil
				case strings.HasPrefix(path, "/_search?"):
					rawCalls++
					wire, _ := json.Marshal(body["query"])
					if !strings.Contains(string(wire), `"gte":0`) || strings.Contains(string(wire), `"must":`) {
						t.Fatalf("raw predicate changed: %s", wire)
					}
					return result(`{"buckets":[]}`), nil
				default:
					t.Fatalf("unexpected request: %s %s", method, path)
					return nil, nil
				}
			}
			store := func() (caller, elastic.Version) { return call, elastic.Version{Distribution: elastic.Easysearch} }
			if err := calculateWithConfig(context.Background(), q, metric, cfg, store); err != nil {
				t.Fatal(err)
			}
			if rollupCalls != tc.wantRollup || rawCalls != tc.wantRaw || (len(metric.Item.Lines) > 0) != (tc.wantRaw == 0) {
				t.Fatalf("rollup=%d raw=%d lines=%d", rollupCalls, rawCalls, len(metric.Item.Lines))
			}
			if tc.name == "pure" {
				want := [][]interface{}{{int64(86400000), 19.5}, {int64(90000000), nil}, {int64(93600000), 10.0}}
				if got := metric.Item.Lines[0].Data; !reflect.DeepEqual(got, want) {
					t.Fatalf("rollup curve = %v, want %v", got, want)
				}
			}
		})
	}
}
