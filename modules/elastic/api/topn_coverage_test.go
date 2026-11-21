package api

import (
	"infini.sh/console/modules/elastic/api/topn"
	"testing"
)

func TestAdvancedTopNMetricCoverage(t *testing.T) {
	keys := []string{
		NodeProcessCPUMetricKey,
		NodeOSCPUMetricKey,
		OSUsedMemoryMetricKey,
		OSLoadAverage1mMetricKey,
		OSUsedSwapMetricKey,
		OpenFileMetricKey,
		OpenFilePercentMetricKey,
		TotalDiskMetricKey,
		IndexingRateMetricKey,
		IndexingBytesMetricKey,
		IndexingLatencyMetricKey,
		QueryRateMetricKey,
		QueryLatencyMetricKey,
		FetchRateMetricKey,
		ScrollRateMetricKey,
		RefreshRateMetricKey,
		FlushRateMetricKey,
		MergesRateMetricKey,
		FetchLatencyMetricKey,
		ScrollLatencyMetricKey,
		MergeLatencyMetricKey,
		RefreshLatencyMetricKey,
		FlushLatencyMetricKey,
		QueryCacheMetricKey,
		RequestCacheMetricKey,
		RequestCacheHitMetricKey,
		RequestCacheMissMetricKey,
		QueryCacheCountMetricKey,
		QueryCacheHitMetricKey,
		QueryCacheMissMetricKey,
		FielddataCacheMetricKey,
		HttpConnectNumMetricKey,
		HttpRateMetricKey,
		SegmentCountMetricKey,
		SegmentMemoryMetricKey,
		SegmentStoredFieldsMemoryMetricKey,
		SegmentTermsMemoryMetricKey,
		SegmentDocValuesMemoryMetricKey,
		SegmentIndexWriterMemoryMetricKey,
		SegmentTermVectorsMemoryMetricKey,
		DocsCountMetricKey,
		DocsDeletedMetricKey,
		IndexStorageMetricKey,
		JVMHeapUsedPercentMetricKey,
		JVMMemYoungUsedMetricKey,
		JVMMemYoungPeakUsedMetricKey,
		JVMMemOldUsedMetricKey,
		JVMMemOldPeakUsedMetricKey,
		JVMUsedHeapMetricKey,
		JVMYoungGCRateMetricKey,
		JVMYoungGCLatencyMetricKey,
		JVMOldGCRateMetricKey,
		JVMOldGCLatencyMetricKey,
		TransportTXRateMetricKey,
		TransportRXRateMetricKey,
		TransportTXBytesMetricKey,
		TransportRXBytesMetricKey,
		TransportTCPOutboundMetricKey,
		TotalIOOperationsMetricKey,
		TotalReadIOOperationsMetricKey,
		TotalWriteIOOperationsMetricKey,
		ScrollOpenContextsMetricKey,
		ParentBreakerMetricKey,
		AccountingBreakerMetricKey,
		FielddataBreakerMetricKey,
		RequestBreakerMetricKey,
		InFlightRequestsBreakerMetricKey,
		ModelInferenceBreakerMetricKey,
		EQLSequenceMetricKey,
		InFlightRequestsMetricKey,
	}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			t.Fatalf("duplicate chart key %q", key)
		}
		seen[key] = true
		defs := nodeMetricItemsForKey(key)
		if len(defs) != 1 {
			t.Fatalf("%s: expected one definition, got %d", key, len(defs))
		}
		d := defs[0]
		if _, ok := topn.NewMetric(d.MetricItem, d.Field, d.Field2, d.Units, d.FormatType, d.IsDerivative); !ok {
			t.Errorf("unsupported chart %q", key)
		}
	}
}
