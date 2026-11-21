package v1

import (
	"infini.sh/console/modules/elastic/api/topn"
	"testing"
)

func TestAdvancedTopNMetricCoverage(t *testing.T) {
	keys := []string{
		IndexStorageMetricKey,
		SegmentCountMetricKey,
		DocCountMetricKey,
		DocsDeletedMetricKey,
		QueryTimesMetricKey,
		FetchTimesMetricKey,
		ScrollTimesMetricKey,
		MergeTimesMetricKey,
		RefreshTimesMetricKey,
		FlushTimesMetricKey,
		IndexingRateMetricKey,
		IndexingBytesMetricKey,
		IndexingLatencyMetricKey,
		QueryLatencyMetricKey,
		FetchLatencyMetricKey,
		MergeLatencyMetricKey,
		RefreshLatencyMetricKey,
		ScrollLatencyMetricKey,
		FlushLatencyMetricKey,
		QueryCacheMetricKey,
		RequestCacheMetricKey,
		RequestCacheHitMetricKey,
		RequestCacheMissMetricKey,
		QueryCacheCountMetricKey,
		QueryCacheHitMetricKey,
		QueryCacheMissMetricKey,
		FielddataCacheMetricKey,
		SegmentMemoryMetricKey,
		SegmentDocValuesMemoryMetricKey,
		SegmentTermsMemoryMetricKey,
		SegmentFieldsMemoryMetricKey,
		SegmentIndexWriterMemoryMetricKey,
		SegmentTermVectorsMemoryMetricKey,
		SegmentNormsMetricKey,
		SegmentPointsMetricKey,
		VersionMapMetricKey,
		FixedBitSetMetricKey,
	}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			t.Fatalf("duplicate chart key %q", key)
		}
		seen[key] = true
		defs := indexMetricItemsForKey(key)
		if len(defs) != 1 {
			t.Fatalf("%s: expected one definition, got %d", key, len(defs))
		}
		d := defs[0]
		if _, ok := topn.NewMetric(d.MetricItem, d.Field, d.Field2, d.Units, d.FormatType, d.IsDerivative); !ok {
			t.Errorf("unsupported chart %q", key)
		}
	}
}
