# TSDB Optimization Analysis (Current State)

This analysis reflects the current implementation and highlights remaining high-impact work.

## Implemented Optimizations

### 1. Sharded hot structures
- Latest-value state is sharded (`stateShards`).
- Active ingest buckets are sharded (`activeShards`).
- This reduces lock contention under concurrent writers.

### 2. Slot-based bucket accumulation
- Active buckets use fixed slot arrays (`values`, `filled`, `touched`) instead of per-point maps.
- Same-slot writes are O(1), with deterministic last-write-wins behavior.

### 3. Object pooling
- `bucketAccumulator` objects are pooled.
- Compactor work buffers are pooled.
- This reduces allocation and GC pressure during high ingest.

### 4. Batched writer with thresholds
- Writer flushes periodically and also when either threshold is reached:
  - `WriterFlushMaxEntries`
  - `WriterFlushMaxBytes`
- This balances throughput and write latency.

### 5. Fixed rollup chain
- Compaction path is fixed: `5m -> 1h -> 4h -> 24h`.
- Rollups are generated without deleting source-tier chunks.

### 6. Query fast path for unflushed data
- `5m` queries merge disk + in-memory active/pending data.
- Pending buckets provide short overlap to bridge packer-to-writer visibility gaps.

## Remaining Bottlenecks / Opportunities

### 1. Compaction transaction scope
Current compaction scans and groups in a DB update transaction. Splitting scan/group and write phases, or partitioning by keyspace, can reduce long transaction pressure.

### 2. Late-data chunk rewrite cost
Late points targeting already-written windows can rewrite whole chunks. Workload-dependent mitigation options:
- bounded late-arrival window
- append-log + reconcile model
- per-slot patch writes (complexity tradeoff)

### 3. Query decode overhead for wide ranges
Large historical scans decode full chunks. Potential improvements:
- chunk decode cache (LRU)
- selective decode shortcuts for fully out-of-range spans
- tighter key pruning for site/test scans

### 4. Storage engine evaluation
Badger is functional and stable, but ingestion-heavy workloads may benefit from testing Pebble with the same workload shape. This needs apples-to-apples benchmarking (same batching, same rollup load, same durability settings).

### 5. Benchmark modernization
Existing benchmark files contain legacy assumptions (for example custom `CompactionTiers` naming not used by fixed rollup execution). Benchmarks should be updated to measure:
- raw `5m` ingest only
- ingest + auto-compaction + retention
- mixed read/write latency percentiles

## Suggested Next Steps (Highest ROI)

1. Add a repeatable end-to-end benchmark suite for `5m + rollups` and publish p50/p95/p99 ingest latency.
2. Profile compaction CPU + transaction time under 7d and 30d workloads.
3. Add optional chunk decode cache behind config flag for read-heavy dashboards.
4. Evaluate Badger vs Pebble using the same benchmark harness before engine migration decisions.
