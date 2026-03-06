# High-Throughput Go TSDB Engine

A pipeline-based time-series engine in Go backed by [BadgerDB](https://github.com/dgraph-io/badger).

The engine keeps latest values in memory for real-time reads and stores historical chunks on disk. It supports fixed query scales:
- `5m` (raw)
- `1h` (rollup average)
- `4h` (rollup average)
- `24h` (rollup average)

## Overview

Write path:
1. `Add()` updates in-memory latest state immediately.
2. `Add()` also writes into active in-memory buckets (slot-based, last-write-wins per slot).
3. `runPacker` periodically seals active buckets and sends them to compaction/encoding workers.
4. `runWriter` batches compressed chunks into Badger.

Rollup path:
- `performCompaction()` builds fixed rollups: `5m -> 1h -> 4h -> 24h`.
- Source-tier data is preserved (rollups do not delete lower tiers).

Read path:
- Real-time APIs read from in-memory state.
- Historical APIs read Badger and merge in-memory unflushed points for `5m` queries.
- If a non-`5m` tier has no data yet, queries fall back to `5m` and aggregate in memory.

## Installation

```bash
go get github.com/dgraph-io/badger/v4
```

## Quick Start

```go
package main

import (
	"log"
	"time"

	"yourproject/tsdb" // replace with your module path
)

func main() {
	cfg := tsdb.Config{
		DBPath:                "./data/badger",
		ChunkFlushInterval:    5 * time.Minute,
		IntervalResolution:    5 * time.Minute,
		IngestBufferSize:      100000,
		CompactorWorkers:      4,
		EnableAutoCompaction:  true,
		CompactionInterval:    1 * time.Minute,
		Retention5m:           3 * 24 * time.Hour,
		Retention1h:           30 * 24 * time.Hour,
		Retention4h:           90 * 24 * time.Hour,
		Retention24h:          365 * 24 * time.Hour,
		RetentionInterval:     10 * time.Minute,
		WriterFlushMaxEntries: 4096,
		WriterFlushMaxBytes:   8 * 1024 * 1024,
	}

	engine, err := tsdb.NewEngine(cfg)
	if err != nil {
		log.Fatalf("failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	engine.Add(time.Now().Unix(), 101, 5001, 245)
}
```

## Config Defaults

`NewEngine` applies defaults when values are `<= 0`:

- `ChunkFlushInterval`: `5m`
- `IntervalResolution`: `5m`
- `IngestBufferSize`: `10000`
- `CompactorWorkers`: `1`
- `CompactionInterval`: `1m`
- `Retention5m`: `3d`
- `Retention1h`: `30d`
- `Retention4h`: `90d`
- `Retention24h`: `365d`
- `RetentionInterval`: `10m`
- `WriterFlushMaxEntries`: `4096`
- `WriterFlushMaxBytes`: `8388608` (8 MiB)

Config notes:
- `DBPath` should be set to your Badger directory.
- `BadgerOptions` is optional; when nil, `badger.DefaultOptions(DBPath)` is used.
- `CompactionTiers` is optional metadata. Rollup execution is fixed to `5m -> 1h -> 4h -> 24h`.
- Retention defaults are applied when retention values are `<= 0`, so `0` at construction time does not mean “keep forever”.

## Public API

Engine lifecycle:
- `Start()`
- `Stop() error` (idempotent)

Ingestion:
- `Add(timestamp, siteID, testID, value int64)`

Real-time reads:
- `GetLatestTest(siteID, testID int64) (DataPoint, bool)`
- `GetLatestSite(siteID int64) map[int64]DataPoint`

Historical reads:
- `GetTestRange(testID, start, end int64, scale string) ([]DataPoint, error)`
- `GetSiteTestRange(siteID, testID, start, end int64, scale string) ([]DataPoint, error)`
- `GetSiteRange(siteID, start, end int64, scale string) ([]DataPoint, error)`

Accepted `scale` values:
- `tsdb.Scale5m`
- `tsdb.Scale1h`
- `tsdb.Scale4h`
- `tsdb.Scale24h`

If `scale == ""`, the engine uses `Scale5m`.

## Query Examples

```go
startTime := time.Now().Add(-7 * 24 * time.Hour).Unix()
endTime := time.Now().Unix()

points5m, err := engine.GetTestRange(5001, startTime, endTime, tsdb.Scale5m)
if err != nil {
	log.Fatalf("GetTestRange 5m failed: %v", err)
}

points1h, err := engine.GetTestRange(5001, startTime, endTime, tsdb.Scale1h)
if err != nil {
	log.Fatalf("GetTestRange 1h failed: %v", err)
}

sitePoints4h, err := engine.GetSiteTestRange(101, 5001, startTime, endTime, tsdb.Scale4h)
if err != nil {
	log.Fatalf("GetSiteTestRange 4h failed: %v", err)
}

siteAll24h, err := engine.GetSiteRange(101, startTime, endTime, tsdb.Scale24h)
if err != nil {
	log.Fatalf("GetSiteRange 24h failed: %v", err)
}

_ = points5m
_ = points1h
_ = sitePoints4h
_ = siteAll24h
```

## Fake Data Generation

Types:
- `ProbeWithSite{ProbeID, SiteID}`
- `SiteProbe{SiteID, ProbeID}`

Functions:
- `GenerateFakeData(siteIDs []int64, probeIDs []ProbeWithSite, numberOfDays int, interval time.Duration) (int, error)`
- `GenerateFakeDataRange(siteIDs []int64, probeIDs []ProbeWithSite, startTime, endTime int64, interval time.Duration) (int, error)`
- `GenerateFakeDataForSeries(series []SiteProbe, days int, interval time.Duration) (int, error)`
- `GenerateFakeDataRangeForSeries(series []SiteProbe, startTime, endTime int64, interval time.Duration) (int, error)`

Behavior:
- Fake generation writes raw points, forces bucket flush/writer flush, and triggers compaction.
- Closed windows become queryable in `5m`, `1h`, `4h`, and `24h`.

## Retention

Retention runs in a background routine (`RetentionInterval`) and deletes by scale:
- `Retention5m`
- `Retention1h`
- `Retention4h`
- `Retention24h`

Deletion is chunk-based in Badger (test key and matching site key). Only chunks strictly older than the cutoff are removed.

## Key Format

Badger keys:
- `test-{scale}!{TestIDHex16}!{TimestampHex16}`
- `site-{scale}!{SiteIDHex16}!{TimestampHex16}!{TestIDHex16}`

Examples:
- `test-1h!0000000000001389!0000000001a2b3c4`
- `site-24h!0000000000000065!0000000001a2b3c4!0000000000001389`
