package topn

import (
	"sync"
	"time"
)

const (
	pitHintCapacity = 128
	pitHintTTL      = 30 * time.Second
)

// Hints only skip a known unproductive attempt; they never cache query results.
var pitHints = routingHints{expires: make(map[routingKey]time.Time)}

type routingKey struct {
	store, cluster, index      string
	field, field2              string
	mode                       metricMode
	width                      int
	duration, buckets, rawFrom int64
	checkHistory               bool
}

func nodeRoutingKey(q Query, metric Metric, api pitAPI, index string, checkHistory bool) (routingKey, bool) {
	// Node authorization is cluster-wide. Index requests have a separate
	// per-index authorization filter and never participate in this cache.
	if q.Width <= 0 || q.storeScope == "" || q.Source != "node_stats" || api.path == "" {
		return routingKey{}, false
	}
	width := int64(q.Width) * 1000
	return routingKey{
		store: q.storeScope, cluster: q.Cluster, index: index,
		field: metric.Field, field2: metric.Field2, mode: metric.mode,
		width: q.Width, duration: q.Max - q.Min, buckets: q.Max/width - q.Min/width + 1,
		rawFrom: q.rawFrom, checkHistory: checkHistory,
	}, true
}

type routingHints struct {
	mu      sync.Mutex
	expires map[routingKey]time.Time
}

func (h *routingHints) contains(key routingKey, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	until, ok := h.expires[key]
	if ok && !now.Before(until) {
		delete(h.expires, key)
		return false
	}
	return ok
}

func (h *routingHints) forget(key routingKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.expires, key)
}

func (h *routingHints) remember(key routingKey, now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, until := range h.expires {
		if !now.Before(until) {
			delete(h.expires, k)
		}
	}
	if _, ok := h.expires[key]; !ok && len(h.expires) < pitHintCapacity {
		h.expires[key] = now.Add(pitHintTTL)
	}
}
