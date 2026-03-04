# Memory + Disk Query Feature

## Overview

All query functions now automatically combine **unflushed data from memory** with **flushed data from disk**, providing complete visibility into your time-series data regardless of flush status.

## How It Works

### Write Path
```
Add() → activeBuckets (memory) → [after ChunkFlushInterval] → BadgerDB (disk)
```

### Query Path (Memory + Disk)
```
GetTestRange() → {
    1. Query BadgerDB (disk) for flushed data
    2. Query activeBuckets (memory) for unflushed data
    3. Merge and deduplicate
    4. Sort by timestamp
    5. Return combined results
}
```

## Example Scenario

### Scenario: Last 7 Days Query
- **Last 4 days**: Flushed to disk (old data)
- **Today**: Still in memory (unflushed, recent data)

```go
// Query for all 7 days
results, err := engine.GetTestRange(testID, startTime, endTime)

// Results automatically include:
// - 4 days from disk (flushed data)
// - Today from memory (unflushed data)
// Total: 7 days of complete data
```

## Deduplication Behavior

If the same timestamp exists in both memory and disk, **memory data takes precedence** (newer data overrides older):

```go
// Write to disk
engine.Add(1000, 100, 5001, 42)
time.Sleep(3 * time.Second) // Wait for flush

// Write same timestamp again (to memory)
engine.Add(1000, 100, 5001, 99)

// Query immediately
results, _ := engine.GetTestRange(5001, 1000, 1000)
fmt.Println(results[0].Value) // Output: 99 (memory value)
```

## Affected Functions

All query functions now support memory+disk:

### 1. GetTestRange
```go
// Returns all data for a specific test in time range
// Memory + Disk combined
results, err := engine.GetTestRange(testID, startTime, endTime)
```

### 2. GetSiteTestRange
```go
// Returns all data for a specific site/test combination in time range
// Memory + Disk combined
results, err := engine.GetSiteTestRange(siteID, testID, startTime, endTime)
```

### 3. GetSiteRange
```go
// Returns all data for all tests at a specific site in time range
// Memory + Disk combined
results, err := engine.GetSiteRange(siteID, startTime, endTime)
```

### 4. GetLatestTest (unchanged)
```go
// Still memory-only (by design - real-time queries)
point, exists := engine.GetLatestTest(siteID, testID)
```

## Performance Characteristics

### Memory Query Cost
- **O(B × P)** where:
  - B = number of active buckets
  - P = average points per bucket
- Typically negligible compared to disk I/O
- Protected by RWMutex (concurrent-read safe)

### Disk Query Cost
- **O(C × D)** where:
  - C = number of chunks in range
  - D = decompression cost per chunk
- Dominant cost in most queries

### Combined Query Cost
```
Total = Disk Cost + Memory Cost + Merge Cost
      ≈ Disk Cost (memory overhead is minimal)
```

## Thread Safety

All query operations are thread-safe:
- **activeBuckets**: Protected by `bucketsMu` (RWMutex)
- **latestValues**: Protected by `stateMu` (RWMutex)
- **BadgerDB**: Natively thread-safe with MVCC

Multiple goroutines can query simultaneously without blocking each other.

## Configuration Tuning

### For Real-Time Queries
```go
Config{
    ChunkFlushInterval: 5 * time.Second, // Shorter = fresher disk data
    IntervalResolution: 1 * time.Second,
}
```
- Shorter flush interval = less data in memory
- More frequent disk writes
- Lower memory usage

### For High Write Throughput
```go
Config{
    ChunkFlushInterval: 10 * time.Second, // Longer = more buffering
    IntervalResolution: 100 * time.Millisecond,
}
```
- Longer flush interval = more data in memory
- Fewer disk writes (higher throughput)
- Higher memory usage

## Test Results

```
=== Memory + Disk Query Test ===
Writing old data (timestamps 1000-1003)...
Waiting 3 seconds for data to flush to disk...
Writing new data (timestamps 1004-1006, NOT flushed)...

Querying for full range [1000, 1006] BEFORE flush...
Got 7 results (should be 7: 4 from disk + 3 from memory)
  [0] ts=1000, val=10000 ✓
  [1] ts=1001, val=10010 ✓
  [2] ts=1002, val=10020 ✓
  [3] ts=1003, val=10030 ✓
  [4] ts=1004, val=10040 ✓  ← from memory
  [5] ts=1005, val=10050 ✓  ← from memory
  [6] ts=1006, val=10060 ✓  ← from memory

✅ All memory+disk query tests passed!
```

## Benefits

1. **Complete Data Visibility**: Never miss recent unflushed data in queries
2. **No Flush Required**: Query anytime without waiting for flush
3. **Automatic Merging**: Transparent to users - just works
4. **Real-Time Analytics**: Combine historical trends with live data
5. **Zero Configuration**: Works out of the box with all queries

## Migration from Disk-Only Queries

No changes required! Existing code automatically benefits:

```go
// Before: Only returned flushed data
results, _ := engine.GetTestRange(testID, start, end)

// After: Automatically includes unflushed data
results, _ := engine.GetTestRange(testID, start, end)
// Same API, better results!
```

## Summary

✅ **Queries now return memory + disk data automatically**
✅ **No code changes required**
✅ **Memory data overrides disk data for same timestamps**
✅ **Thread-safe and concurrent-read optimized**
✅ **Minimal performance overhead**

Your query for "last 7 days" will now correctly return all 7 days of data, regardless of which portions have been flushed to disk!
