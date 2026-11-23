package topn

import (
	"fmt"
	"math"
	"sort"

	"infini.sh/framework/modules/elastic/common"
)

type metricMode uint8

const (
	peak metricMode = iota
	rate
	latency
	ratio
)

func (m metricMode) difference() bool {
	return m == rate || m == latency
}

type Metric struct {
	rawProof                     *string
	Item                         *common.MetricItem
	Field, Field2, Units, Format string
	mode                         metricMode
	reverseRatio                 bool
	jvmSummary                   bool
}

// NewMetric uses the chart's fields and units. Only exceptions to the ordinary
// max, per-second difference, and time-per-operation formulas are named here.
func NewMetric(item *common.MetricItem, field, field2, units, format string, derivative bool) (Metric, bool) {
	if item == nil || field == "" {
		return Metric{}, false
	}
	m := Metric{Item: item, Field: field, Field2: field2, Units: units, Format: format}
	switch {
	case !derivative && field2 == "":
		m.mode = peak
	case !derivative:
		m.mode = ratio
		switch item.Key {
		case "disk":
			m.reverseRatio = true
		case "os_used_swap", "open_file_percent":
		default:
			return Metric{}, false
		}
	case field2 != "":
		m.mode = latency
	default:
		// Store-size growth also uses the positive per-second difference.
		m.mode = rate
	}
	return m, true
}

func metricScope(q Query, metric Metric) dsl {
	if metric.mode.difference() {
		return q.counterScope()
	}
	if metric.mode == ratio {
		return filterRange(q.Min, q.Max)
	}
	return dsl{"bool": dsl{"filter": []dsl{filterRange(q.Min, q.Max), {"range": dsl{metric.Field: dsl{"gte": 0}}}}}}
}

func metricBucketCost(q Query, metric Metric) int {
	w := int64(q.Width) * 1000
	n := int(q.Max/w-q.Min/w) + 1
	if metric.mode.difference() {
		// Date bucket plus up to two identity buckets and an optional legacy filter.
		perPoint := 3
		if q.Source == "node_stats" && !metric.jvmSummary {
			perPoint++
		}
		return perPoint * (n + 1)
	}
	return n
}

func number(s *scalar) (float64, bool) {
	if s == nil || s.Value == nil || *s.Value < 0 {
		return 0, false
	}
	return *s.Value, true
}

func scoreSeries(q Query, metric Metric, name string, points []samples) (winner, error) {
	width := int64(q.Width) * 1000
	first := q.Min / width * width
	earliest := first
	if metric.mode.difference() {
		earliest = max(0, first-width)
	}
	row := winner{name: name, score: -1}
	for _, p := range points {
		if p.Time == nil {
			return winner{}, fmt.Errorf("missing time bucket key for %q", name)
		}
	}
	sort.Slice(points, func(i, j int) bool { return *points[i].Time < *points[j].Time })
	var previous samples
	for _, p := range points {
		ts := *p.Time
		if ts < earliest || ts > q.Max || ts%width != 0 || (previous.Time != nil && ts <= *previous.Time) {
			return winner{}, fmt.Errorf("invalid time bucket %d for %q", ts, name)
		}
		v, valid := number(p.Value)
		switch metric.mode {
		case ratio:
			if p.Value == nil || p.Value.Value == nil {
				return winner{}, fmt.Errorf("missing ratio aggregation for %q", name)
			}
		case peak:
			if !valid {
				return winner{}, fmt.Errorf("missing peak sample for %q", name)
			}
		default:
			var err error
			v, valid, err = counterPoint(previous, p, width, metric, q.Source == "node_stats")
			if err != nil {
				return winner{}, err
			}
		}
		previous = p
		if ts < first || !valid || math.IsInf(v, 0) || math.IsNaN(v) {
			continue
		}
		if n := len(row.points); n > 0 {
			last := row.points[n-1][0].(int64)
			if ts-last > width {
				row.points = append(row.points, []interface{}{last + width, nil})
			}
		}
		row.points = append(row.points, []interface{}{ts, v})
		row.score = max(row.score, v)
	}
	return row, nil
}
