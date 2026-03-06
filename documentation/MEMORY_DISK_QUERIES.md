# Memory + Disk Query Semantics

This document describes how historical and real-time queries work in the current engine.

## Historical API Signatures

- `GetTestRange(testID, start, end int64, scale string) ([]DataPoint, error)`
- `GetSiteTestRange(siteID, testID, start, end int64, scale string) ([]DataPoint, error)`
- `GetSiteRange(siteID, start, end int64, scale string) ([]DataPoint, error)`

Valid scales:
- `Scale5m`
- `Scale1h`
- `Scale4h`
- `Scale24h`

If `scale == ""`, it defaults to `Scale5m`.

## Real-Time APIs

- `GetLatestTest(siteID, testID int64) (DataPoint, bool)`
- `GetLatestSite(siteID int64) map[int64]DataPoint`

These read only from in-memory latest-state mirrors.

## Historical Read Path by Scale

### Scale5m
`5m` queries combine both storage layers:
1. Scan Badger (`test-5m` / `site-5m`) for flushed chunks.
2. Read unflushed points from active buckets.
3. Read short-lived pending buckets (TTL bridge after flush).
4. Merge, deduplicate, sort.

Dedup rules:
- `GetTestRange` / `GetSiteTestRange`: key = timestamp
- `GetSiteRange`: key = `(timestamp, testID)`
- Memory entries override disk entries on key collision.

### Scale1h / Scale4h / Scale24h
1. Read requested scale from Badger.
2. If no points exist for that tier yet, fallback to `5m` and aggregate in memory.
3. Return one averaged point per requested window.

## Example

```go
start := time.Now().Add(-7 * 24 * time.Hour).Unix()
end := time.Now().Unix()

raw, err := engine.GetTestRange(5001, start, end, tsdb.Scale5m)
if err != nil {
	log.Fatal(err)
}

hourly, err := engine.GetTestRange(5001, start, end, tsdb.Scale1h)
if err != nil {
	log.Fatal(err)
}

site4h, err := engine.GetSiteTestRange(101, 5001, start, end, tsdb.Scale4h)
if err != nil {
	log.Fatal(err)
}

siteDaily, err := engine.GetSiteRange(101, start, end, tsdb.Scale24h)
if err != nil {
	log.Fatal(err)
}

_ = raw
_ = hourly
_ = site4h
_ = siteDaily
```

## Timing Notes

- Data written by `Add()` is immediately visible to real-time queries.
- For historical queries, `5m` includes in-memory points before disk flush.
- Rollup tiers become available after compaction has processed closed source windows.
