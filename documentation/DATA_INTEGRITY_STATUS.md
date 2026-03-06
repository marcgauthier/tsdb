# Data Integrity and Durability Status

This document reflects the current engine behavior in `main.go` and `fake_data.go`.

## Current Guarantees

### 1. Ingestion visibility
- `Add(timestamp, siteID, testID, value)` updates active in-memory buckets immediately.
- For the same `(siteID, testID, time-slot)`, last write wins.

### 2. Disk persistence model
- Data is flushed from active buckets to compactor on each `ChunkFlushInterval` tick.
- Compactor writes raw chunks to the `5m` collection.
- Writer persists batched writes to Badger with periodic and threshold-based flushes.

### 3. Query consistency
- `5m` historical queries merge:
  - flushed disk data
  - active in-memory buckets
  - pending buckets (short TTL bridge after flush)
- This prevents short visibility gaps between packer and writer.

### 4. Rollup integrity
- Auto-compaction path is fixed: `5m -> 1h -> 4h -> 24h`.
- Source-tier data is preserved; rollups do not delete lower tiers.
- Non-`5m` queries fall back to aggregating `5m` if a target rollup tier is empty.

### 5. Retention integrity
- Retention is applied per scale:
  - `Retention5m`
  - `Retention1h`
  - `Retention4h`
  - `Retention24h`
- Deletion is chunk-based and removes both test-key and site-key entries.
- Only chunks strictly older than the cutoff are deleted.

## Shutdown Behavior

`Stop()` is idempotent (`sync.Once`) and performs orderly shutdown:
1. Close ingest channel (packer stop signal).
2. Cancel context (background loops exit).
3. Wait for goroutines to finish.
4. Close BadgerDB.

This is designed to flush pending pipeline work before return.

## Important Edge Behaviors (Intentional)

### Delayed / out-of-order points
A late point for an already persisted `(siteID, testID, windowStart)` can rewrite that chunk key when re-flushed. Tests should explicitly assert expected behavior for your workload.

### Slot overwrite semantics
Multiple points in the same slot are not appended; the latest value overwrites the previous slot value.

### Retention defaults
At `NewEngine` time, retention values `<= 0` are defaulted (not treated as "keep forever").

## Default Retention Values

When config fields are `<= 0`, `NewEngine` applies:
- `5m`: 3 days
- `1h`: 30 days
- `4h`: 90 days
- `24h`: 365 days

## Recommended Validation Commands

```bash
go test ./...
go test -run 'Retention|GenerateFakeDataRange_GeneratesAllScales' -v ./...
```
