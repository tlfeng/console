package topn

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// ServeSelected shows only authorized explicit names, in selection order.
// Unlike Window, it deliberately does not interpret the automatic top setting.
func ServeSelected(w http.ResponseWriter, r *http.Request, q Query, metric Metric, names string) {
	min, max, ok := windowBounds(r, q.Width)
	if !ok {
		WriteStatus(w, "invalid_request")
		return
	}
	q.Min, q.Max = min, max
	q.selected = selectEntities(q, names)
	Serve(w, r, q, metric)
}

func selectEntities(q Query, names string) []string {
	allowed := q.authorizer()
	selected := make([]string, 0)
	seen := make(map[string]bool)
	for _, name := range strings.Split(names, ",") {
		if name == "" || (q.Source == "index_stats" && name == "_all") || seen[name] || !allowed(name) {
			continue
		}
		seen[name] = true
		selected = append(selected, name)
	}
	return selected
}

func runSelected(ctx context.Context, call caller, index string, pred dsl, q Query, metric Metric, checkHistory bool) error {
	call = budgetedCaller(call, responseLimit)
	if q.Source == "node_stats" && metric.mode.difference() {
		summary := metric
		summary.jvmSummary = true
		err := singleSearch(ctx, call, index, pred, q, summary, checkHistory)
		if !errors.Is(err, errLegacyJVM) || ctx.Err() != nil {
			return err
		}
	}
	return singleSearch(ctx, call, index, pred, q, metric, checkHistory)
}
