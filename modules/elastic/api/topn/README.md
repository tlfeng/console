# Agentless Advanced TopN rollout

Exact automatic ranking is opt-in and currently uses the Easysearch PIT API on the **system monitoring store**, not the monitored cluster. Manual selections are unchanged. Configure Console's `insight` section:

```yaml
insight:
  topn:
    clusters: [monitored-cluster-id]
    raw_index: .infini_metrics
    raw_from: "2026-01-01T00:00:00Z" # example only: replace with the verified raw coverage boundary
    disabled_keys: [] # e.g. ["node_stats:cpu", "index_stats:index_storage"]
```

`raw_index` must be one trusted raw monitoring index or alias (no patterns), covering all enrolled clusters. `raw_from` is an operator assertion that original monitoring data has not been removed from that event-time boundary onward. Update it before data expires or original backing indices are removed. Do not infer it from the rollup threshold or retention policy alone. The assertion concerns retained originals, not whether every collector sample arrived. Windows outside that coverage do not return an automatic ranking. Only explicit `cluster_id` matches are accepted; older UUID-only samples are not attributed to an enrolled cluster.

The initial allowlist covers index storage, document count and segment count, and node process/OS CPU, JVM heap percent and index storage. Missing values do not rank; legal zero does. An enrolled cluster's unsupported or disabled automatic charts retain manual selection rather than falling back to activity-based ranking. To roll back a metric without restoring legacy automatic ranking, leave the cluster enrolled and add the metric to `disabled_keys`.

The composite allowlist includes node query rate and query latency, and index query and primary indexing rates. The previous complete bucket may be read to evaluate the first visible point; that bucket also needs raw coverage. Missing or non-adjacent buckets, index UUID/node ID changes, detectable topology changes and counter resets, and zero-request latency points are not scored. This is a ranking of retained valid monitoring samples, not proof that collection or counter continuity was perfect.

The initial limits are deliberately conservative: 512 visible time buckets, four concurrent exact requests per Console process, up to 32 storage shards per PIT, a five-second query deadline, and 16 MiB of cumulative search response data. Composite scans also stop at 24 search pages (including a possible terminal probe) or 150,000 entity-time buckets. The page size is reduced for wider windows. These are protections, not production performance guarantees. Budget rejection never displays a partial or legacy ranking.

Run `go test ./modules/elastic/api/...` with a compatible Framework checkout. The opt-in snapshot integration test requires a disposable Easysearch instance at `127.0.0.1:19219` named `console-topn-local-bench`; run `CONSOLE_TOPN_INTEGRATION=1 go test ./modules/elastic/api/topn -count=1`. Test fixtures use a newly named temporary index and do not target existing monitoring data.
