package topn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
)

var generationSuffix = regexp.MustCompile(`^[0-9]{6,}$`)

func summaryGeneration(alias, name string) bool {
	suffix, ok := strings.CutPrefix(name, alias+"-")
	return ok && generationSuffix.MatchString(suffix)
}

func checkMaterializedIndex(ctx context.Context, call caller, m materializedStore) error {
	raw, err := call(ctx, http.MethodGet, "/"+m.index+"/_mapping", nil)
	if err != nil {
		return err
	}
	var mapping map[string]struct {
		Mappings struct {
			Meta struct {
				Version int `json:"console_topn"`
			} `json:"_meta"`
		}
	}
	if err = json.Unmarshal(raw, &mapping); err != nil || len(mapping) == 0 || len(mapping) > 32 {
		return errors.New("unexpected TopN summary index mapping")
	}
	for name, row := range mapping {
		if (name != m.index && !summaryGeneration(m.index, name)) || row.Mappings.Meta.Version != materializedVersion {
			return errors.New("unexpected TopN summary index mapping")
		}
	}
	return nil
}

func backingSettings(ctx context.Context, call caller, target string) (map[string]backingIndex, error) {
	raw, err := call(ctx, http.MethodGet, "/"+target+"?flat_settings=true&allow_no_indices=false&ignore_unavailable=false&expand_wildcards=open,hidden", nil)
	if err != nil {
		return nil, err
	}
	var indices map[string]backingIndex
	if err = json.Unmarshal(raw, &indices); err != nil {
		return nil, err
	}
	if len(indices) == 0 || len(indices) > 32 {
		return nil, errors.New("invalid monitoring backing indices")
	}
	return indices, nil
}

type backingIndex struct {
	Settings map[string]string
	Aliases  map[string]struct {
		Write *bool `json:"is_write_index"`
	}
}

func writeBacking(indices map[string]backingIndex, target string) (string, error) {
	var writer string
	for name, index := range indices {
		if !rawName.MatchString(name) {
			return "", errors.New("invalid monitoring backing index name")
		}
		alias, linked := index.Aliases[target]
		if name == target && len(indices) == 1 || linked && (alias.Write != nil && *alias.Write || alias.Write == nil && len(indices) == 1) {
			if writer != "" {
				return "", errors.New("multiple monitoring write indices")
			}
			writer = name
		}
	}
	if writer == "" {
		return "", errors.New("monitoring source has no identified write index")
	}
	return writer, nil
}

// Prevent a disappeared rollover alias from being auto-created as a concrete
// index by a score write. Existing concrete caches keep their original path.
func summaryWriteSuffix(ctx context.Context, call caller, m materializedStore) (string, error) {
	targets, err := backingSettings(ctx, call, m.index)
	if err != nil {
		return "", err
	}
	writer, err := writeBacking(targets, m.index)
	if err != nil {
		return "", err
	}
	if writer == m.index {
		return "", nil
	}
	for name := range targets {
		if !summaryGeneration(m.index, name) {
			return "", errors.New("invalid TopN summary write alias")
		}
	}
	return "?require_alias=true", nil
}

func ismPolicy(ctx context.Context, call caller, name string) (string, error) {
	raw, err := call(ctx, http.MethodGet, "/_plugins/_ism/explain/"+name, nil)
	if err != nil {
		return "", err
	}
	var rows map[string]json.RawMessage
	if err = json.Unmarshal(raw, &rows); err != nil {
		return "", err
	}
	row, ok := rows[name]
	if !ok || len(row) == 0 || row[0] != '{' {
		return "", errors.New("missing ISM explain result")
	}
	var result struct {
		Policy string `json:"policy_id"`
	}
	if err = json.Unmarshal(row, &result); err != nil {
		return "", err
	}
	return result.Policy, nil
}

var errSummaryNoPolicy = errors.New("raw monitoring index has no lifecycle policy")
var errSummaryPolicyChanged = errors.New("monitoring lifecycle policy changed; use a new TopN summary name")

func ensureSummaryIndex(ctx context.Context, call caller, m materializedStore, api pitAPI) error {
	err := checkMaterializedIndex(ctx, call, m)
	missing := errors.Is(err, monitoringHTTPError(404))
	if err != nil && !missing {
		return err
	}
	var targets map[string]backingIndex
	if !missing {
		targets, err = backingSettings(ctx, call, m.index)
		if err != nil {
			return err
		}
		if _, concrete := targets[m.index]; concrete && len(targets) == 1 {
			return nil
		}
	}
	raw, err := backingSettings(ctx, call, m.raw)
	if err != nil {
		return err
	}
	writer, err := writeBacking(raw, m.raw)
	if err != nil {
		return err
	}
	ism := api.path == "/_search/point_in_time"
	policy := raw[writer].Settings["index.lifecycle.name"]
	if ism {
		policy, err = ismPolicy(ctx, call, writer)
		if err != nil {
			return err
		}
	}
	if policy == "" {
		if missing {
			return errSummaryNoPolicy
		}
		return errSummaryPolicyChanged
	}
	settings := dsl{"number_of_shards": 1}
	aliasSetting := "index.lifecycle.rollover_alias"
	if ism {
		aliasSetting = "index.plugins.index_state_management.rollover_alias"
	} else {
		settings["index.lifecycle.name"] = policy
	}
	settings[aliasSetting] = m.index
	// Bind the bootstrap to the discovered policy, and persist that identity in
	// every generation's mapping. ISM attaches the policy separately.
	mapping := dsl{"dynamic": false, "_meta": dsl{"console_topn": materializedVersion, "raw_policy": policy}, "properties": dsl{"t": dsl{"type": "date", "format": "epoch_millis"}}}
	if missing {
		bootstrap := m.index + "-000001"
		if _, err = call(ctx, http.MethodHead, "/"+bootstrap, nil); !errors.Is(err, monitoringHTTPError(404)) {
			if err != nil {
				return err
			}
			return errors.New("TopN summary bootstrap index exists without its alias")
		}
		template := dsl{"index_patterns": []string{m.index + "-0*"}, "priority": 1000, "template": dsl{"settings": settings, "mappings": mapping}}
		if _, err = call(ctx, http.MethodPut, "/_index_template/"+m.index+"-rollover", template); err != nil {
			return err
		}
		if err = verifySummaryTemplate(ctx, call, m, aliasSetting, policy, ism); err != nil {
			return err
		}
		body := dsl{"settings": settings, "mappings": mapping, "aliases": dsl{m.index: dsl{"is_write_index": true}}}
		if _, err = call(ctx, http.MethodPut, "/"+bootstrap, body); err != nil && !errors.Is(err, monitoringHTTPError(400)) && !errors.Is(err, monitoringHTTPError(409)) {
			return err
		}
		if err = checkMaterializedIndex(ctx, call, m); err != nil {
			return err
		}
		targets, err = backingSettings(ctx, call, m.index)
		if err != nil {
			return err
		}
	} else if err = verifySummaryTemplate(ctx, call, m, aliasSetting, policy, ism); err != nil {
		return err
	}
	if _, err = writeBacking(targets, m.index); err != nil {
		return err
	}
	for name, target := range targets {
		if !summaryGeneration(m.index, name) || target.Settings[aliasSetting] != m.index {
			return errors.New("invalid TopN summary rollover target")
		}
		if !ism {
			if target.Settings["index.lifecycle.name"] != policy {
				return errSummaryPolicyChanged
			}
			continue
		}
		bound, err := ismPolicy(ctx, call, name)
		if err != nil {
			return err
		}
		if bound == policy {
			continue
		}
		if bound != "" {
			return errSummaryPolicyChanged
		}
		response, err := call(ctx, http.MethodPost, "/_plugins/_ism/add/"+name, dsl{"policy_id": policy})
		if err != nil {
			return err
		}
		var result struct {
			Updated  int   `json:"updated_indices"`
			Failures *bool `json:"failures"`
		}
		if json.Unmarshal(response, &result) != nil || result.Failures == nil || *result.Failures || result.Updated != 1 {
			return errors.New("failed to attach TopN summary ISM policy")
		}
	}
	return nil
}

func verifySummaryTemplate(ctx context.Context, call caller, m materializedStore, aliasSetting, policy string, ism bool) error {
	wire, err := call(ctx, http.MethodPost, "/_index_template/_simulate_index/"+m.index+"-000002", nil)
	if err != nil {
		return err
	}
	var response struct {
		Template struct {
			Settings dsl
			Mappings struct {
				Meta struct {
					Version int    `json:"console_topn"`
					Policy  string `json:"raw_policy"`
				} `json:"_meta"`
			}
		}
	}
	if err = json.Unmarshal(wire, &response); err != nil {
		return err
	}
	meta := response.Template.Mappings.Meta
	if meta.Policy != policy {
		return errSummaryPolicyChanged
	}
	if meta.Version != materializedVersion || settingValue(response.Template.Settings, aliasSetting) != m.index || !ism && settingValue(response.Template.Settings, "index.lifecycle.name") != policy {
		return errors.New("effective TopN rollover template does not inherit the monitoring policy")
	}
	return nil
}

// The simulate endpoint may return nested settings instead of flat keys.
func settingValue(settings dsl, key string) string {
	if value, ok := settings[key].(string); ok {
		return value
	}
	prefix, rest, ok := strings.Cut(key, ".")
	if !ok {
		return ""
	}
	child, _ := settings[prefix].(map[string]interface{})
	return settingValue(child, rest)
}
