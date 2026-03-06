# TSDB Performance Results

This file documents how to measure performance for the current engine. Older historical numbers were removed because architecture and defaults changed.

## Scope

Measure these separately:
1. Ingestion-only (`Add` path and raw `5m` persistence).
2. End-to-end ingestion with rollups (`5m -> 1h -> 4h -> 24h`).
3. Mixed read/write workloads (`GetLatest*` and historical `Get*Range`).

## Current Default-Relevant Settings

Defaults applied by `NewEngine` when config values are `<= 0`:
- `ChunkFlushInterval`: `5m`
- `IntervalResolution`: `5m`
- `CompactorWorkers`: `1`
- `WriterFlushMaxEntries`: `4096`
- `WriterFlushMaxBytes`: `8 MiB`
- Retention: `3d / 30d / 90d / 365d` for `5m / 1h / 4h / 24h`

For throughput testing, explicitly set config values to the workload you want to model.

## Recommended Benchmark Commands

Microbenchmarks in repository:

```bash
go test -run '^$' -bench . -benchmem ./...
```

Focused ingestion benchmarks:

```bash
go test -run '^$' -bench 'BenchmarkSingleThreadedIngestion|BenchmarkMultiThreadedIngestion|BenchmarkSustainedLoad' -benchmem ./...
```

Focused query benchmark while writing:

```bash
go test -run '^$' -bench 'BenchmarkRealTimeQueryWhileWriting' -benchmem ./...
```

## End-to-End Measurement Guidance

To measure production-like ingestion throughput, include:
- multiple sites/tests
- realistic timestamp cadence
- auto-compaction enabled
- retention cleanup enabled
- enough runtime to cross flush and compaction windows

Track at minimum:
- points/sec ingested
- p50/p95/p99 `Add` latency
- writer batch flush frequency
- compaction cycle duration
- read latency by scale (`5m`, `1h`, `4h`, `24h`)

## Notes on Interpreting Results

- Very high short-burst rates can overstate steady-state performance if disk flush/compaction is not included.
- Non-`5m` query performance depends on rollup availability; before rollups are populated, queries aggregate from `5m`.
- Performance is highly sensitive to key cardinality (`siteID`, `testID` spread), flush interval, and storage medium.

## Reporting Template

When sharing new results, include:
- CPU model, core count, RAM, storage device
- Go version
- full `Config` used
- benchmark command and duration
- whether rollups and retention were enabled
