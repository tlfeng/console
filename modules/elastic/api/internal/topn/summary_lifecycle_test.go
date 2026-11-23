package topn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"infini.sh/framework/core/elastic"
)

func TestSummaryReusesRawPolicy(t *testing.T) {
	for _, distribution := range []string{elastic.Easysearch, elastic.Elasticsearch, elastic.Opensearch} {
		t.Run(distribution, func(t *testing.T) {
			ctx := context.Background()
			m := summaryStore{raw: "raw", index: ".console-topn-test"}
			generation := m.index + "-000001"
			created, attached := false, false
			var template dsl
			rawPolicy := "shared-policy"
			call := func(_ context.Context, method, path string, body dsl) ([]byte, error) {
				switch {
				case method == http.MethodGet && path == "/"+m.index+"/_mapping":
					if !created {
						return nil, monitoringHTTPError(404)
					}
					return []byte(fmt.Sprintf(`{%q:{"mappings":{"_meta":{"console_topn":1}}}}`, generation)), nil
				case method == http.MethodGet && strings.HasPrefix(path, "/raw?"):
					if strings.Contains(path, "filter_path=") {
						t.Fatal("flat settings must not use nested filters")
					}
					return []byte(fmt.Sprintf(`{"raw-000001":{"settings":{"index.lifecycle.name":"old"},"aliases":{"raw":{"is_write_index":false}}},"raw-000002":{"settings":{"index.lifecycle.name":%q},"aliases":{"raw":{"is_write_index":true}}}}`, rawPolicy)), nil
				case method == http.MethodGet && path == "/_plugins/_ism/explain/raw-000002":
					return []byte(fmt.Sprintf(`{"raw-000002":{"policy_id":%q},"total_managed_indices":1}`, rawPolicy)), nil
				case method == http.MethodHead && path == "/"+generation:
					return nil, monitoringHTTPError(404)
				case method == http.MethodPut && path == "/_index_template/"+m.index+"-rollover":
					template = body["template"].(dsl)
					if template["aliases"] != nil {
						t.Fatal("template must not set every generation as writer")
					}
				case method == http.MethodPost && path == "/_index_template/_simulate_index/"+m.index+"-000002":
					return json.Marshal(dsl{"template": template})
				case method == http.MethodPut && path == "/"+generation:
					settings := body["settings"].(dsl)
					if distribution == elastic.Opensearch {
						if settings["index.plugins.index_state_management.rollover_alias"] != m.index {
							t.Fatal(settings)
						}
					} else if settings["index.lifecycle.name"] != "shared-policy" || settings["index.lifecycle.rollover_alias"] != m.index {
						t.Fatal(settings)
					}
					if template == nil || body["aliases"].(dsl)[m.index].(dsl)["is_write_index"] != true {
						t.Fatalf("invalid bootstrap: %v", body)
					}
					created = true
				case method == http.MethodGet && strings.HasPrefix(path, "/"+m.index+"?"):
					settings := make(map[string]string)
					for key, value := range template["settings"].(dsl) {
						settings[key] = fmt.Sprint(value)
					}
					return json.Marshal(dsl{generation: dsl{"settings": settings, "aliases": dsl{m.index: dsl{"is_write_index": true}}}})
				case method == http.MethodGet && path == "/_plugins/_ism/explain/"+generation:
					if attached {
						return []byte(fmt.Sprintf(`{%q:{"policy_id":"shared-policy"}}`, generation)), nil
					}
					return []byte(fmt.Sprintf(`{%q:{"policy_id":null}}`, generation)), nil
				case method == http.MethodPost && path == "/_plugins/_ism/add/"+generation:
					if body["policy_id"] != "shared-policy" || attached {
						t.Fatalf("unexpected ISM attachment: %v", body)
					}
					attached = true
					return []byte(`{"updated_indices":1,"failures":false}`), nil
				default:
					t.Fatalf("unexpected request: %s %s", method, path)
				}
				return nil, nil
			}
			api := pitFor(elastic.Version{Distribution: distribution, Number: "8.16.0"})
			for range 2 {
				if err := ensureSummaryIndex(ctx, call, m, api); err != nil {
					t.Fatal(err)
				}
			}
			if template == nil || !created || attached != (distribution == elastic.Opensearch) {
				t.Fatal(template, created, attached)
			}
			rawPolicy = "replacement"
			if err := ensureSummaryIndex(ctx, call, m, api); !errors.Is(err, errSummaryPolicyChanged) {
				t.Fatal(err)
			}
		})
	}
}
