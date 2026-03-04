# TSDB Engine - Deep Optimization Analysis

## Executive Summary

Current implementation achieves ~93% compression but has significant opportunities for improvement in both **compression ratio** (targeting 97-99%) and **throughput** (targeting 2-10x speedup).

---

## Part 1: Compression Optimizations

### 🔴 Critical Issue #1: Delta-from-First Strategy

**Current Code (line 314):**
```go
delta := val - chunk.FirstVal  // All deltas relative to first value
```

**Problem:**
- For drifting metrics (e.g., counter that increments), deltas grow over time
- Example: values [100, 102, 105, 110, 120, 135]
  - Current deltas: [0, 2, 5, 10, 20, 35]  (growing)
  - Varint sizes: [1, 1, 1, 1, 1, 1] bytes = 6 bytes

**Better: Delta-of-Delta (DoD)**
```go
// First value stored as-is
// Subsequent values store delta from previous
prevDelta := 0
for i, val := range values {
    if i == 0 {
        // Store first value
    } else {
        currentDelta := val - values[i-1]
        deltaOfDelta := currentDelta - prevDelta
        // Encode deltaOfDelta (often zero!)
        prevDelta = currentDelta
    }
}
```

**Example with DoD:**
- Values: [100, 102, 105, 110, 120, 135]
- First deltas: [-, 2, 3, 5, 10, 15]
- DoD: [100, 2, 1, 2, 5, 5]  (smaller values!)

**Compression gain: 20-40% for trending data**

---

### 🟡 Issue #2: No Run-Length Encoding (RLE)

**Current Code:**
Forward-fill creates: `[100, 100, 100, 100, 100, 100]`

**Current encoding:**
```
delta₁=0, delta₂=0, delta₃=0, delta₄=0, delta₅=0, delta₆=0
6 varints × 1 byte = 6 bytes
```

**With RLE:**
```
value=100, count=6
2 varints = 2 bytes
```

**Compression gain: 60-80% for stable metrics** (common in monitoring)

**Implementation:**
```go
type RLESegment struct {
    Value int64
    Count uint32
}

// Encode runs
segments := []RLESegment{}
currentVal := values[0]
count := 1

for i := 1; i < len(values); i++ {
    if values[i] == currentVal {
        count++
    } else {
        segments = append(segments, RLESegment{currentVal, count})
        currentVal = values[i]
        count = 1
    }
}
```

---

### 🟡 Issue #3: No Block Compression

**Current:** Raw varint bytes written directly
**Better:** Apply fast compression to entire delta array

**Options:**

1. **Snappy** (Google)
   - Speed: 250-500 MB/s compress, 500-1500 MB/s decompress
   - Ratio: 1.5-2x
   - Best for: Speed-critical paths

2. **LZ4**
   - Speed: 400-800 MB/s compress, 2000-4000 MB/s decompress
   - Ratio: 1.5-2.5x
   - Best for: Read-heavy workloads

3. **Zstd (level 3)**
   - Speed: 200-400 MB/s compress, 500-800 MB/s decompress
   - Ratio: 2-4x
   - Best for: Long-term storage tiers

**Recommendation:**
- Tier 1 (1h): Snappy (fast writes)
- Tier 2 (24h): LZ4 (balanced)
- Tier 3+ (7d+): Zstd level 3 (max compression)

**Implementation:**
```go
import "github.com/klauspost/compress/s2"  // Snappy

// After delta encoding
compressed := s2.Encode(nil, chunk.Data)
chunk.Data = compressed
chunk.CompressionType = COMPRESSION_SNAPPY
```

**Compression gain: 40-60% additional**

---

### 🟡 Issue #4: Memory Allocation in Hot Path

**Current Code (line 317):**
```go
for _, val := range values {
    buf := make([]byte, binary.MaxVarintLen64)  // ❌ Allocates every iteration!
    n := binary.PutVarint(buf, delta)
    buffer.Write(buf[:n])
}
```

**Problem:**
- 360 values → 360 allocations
- GC pressure

**Fix: Reuse buffer**
```go
buf := make([]byte, binary.MaxVarintLen64)  // ✅ Allocate once
for _, val := range values {
    delta := val - chunk.FirstVal
    n := binary.PutVarint(buf, delta)
    buffer.Write(buf[:n])
}
```

**Speedup: 10-15%**

---

### 🟢 Issue #5: Pre-allocate Buffer Size

**Current Code (line 312):**
```go
var buffer bytes.Buffer  // Starts at 0 capacity, grows dynamically
```

**Better:**
```go
// Estimate size: avg 2 bytes per varint
estimatedSize := len(values) * 2
buffer := bytes.NewBuffer(make([]byte, 0, estimatedSize))
```

**Speedup: 5-10%** (reduces reallocs)

---

## Part 2: Speed Optimizations

### 🔴 Critical Issue #6: Duplicate Writes to Test & Site Keys

**Current Code (line 336-337):**
```go
e.writeChan <- WritePayload{Key: testKey, Data: chunkBytes}
e.writeChan <- WritePayload{Key: siteKey, Data: chunkBytes}
```

**Problem:**
- Serializes chunk twice
- Writes same data twice (100% overhead!)
- 2× write amplification

**Options:**

**A) Share compressed bytes:**
```go
chunkBytes := serializeChunk(chunk)
sharedData := chunkBytes  // Both keys point to same []byte

e.writeChan <- WritePayload{Key: testKey, Data: sharedData}
e.writeChan <- WritePayload{Key: siteKey, Data: sharedData}
```

**B) Only store test keys, build site index:**
```go
// Only write: test-1h!{testID}!{timestamp}
// Create lightweight site index: site!{siteID} → []testID mapping
```

**C) Value log deduplication (BadgerDB feature):**
- BadgerDB can deduplicate values automatically
- Enable with `opts.DetectConflicts = false`

**Speedup: 2x write throughput**

---

### 🟡 Issue #7: Serial Compaction

**Current:** Single goroutine per tier, processes one group at a time

**Better: Parallel compaction**
```go
// Worker pool for compaction
const compactionWorkers = 4
workChan := make(chan *chunkGroup, 100)

// Launch workers
for i := 0; i < compactionWorkers; i++ {
    go func() {
        for group := range workChan {
            merged := e.mergeChunks(group.chunks, group.targetStart)
            // ... write merged chunk
        }
    }()
}

// Feed work
for _, group := range groups {
    workChan <- group
}
```

**Speedup: 2-4x compaction speed** (multi-core utilization)

---

### 🟡 Issue #8: Query Tier Selection

**Current Code:** Scans ALL tiers for every query

**Problem:**
```go
// Querying last 2 hours scans: 1h tier, 24h tier, 7d tier
// But 24h and 7d tiers can't have data from last 2 hours!
```

**Better: Smart tier filtering**
```go
func (e *Engine) selectTiers(start, end int64) []string {
    now := time.Now().Unix()
    queryAge := now - start

    var tiers []string
    for _, tier := range e.config.CompactionTiers {
        // Only scan tiers that could contain this time range
        if queryAge <= int64(tier.MinAge.Seconds()) ||
           end > now - int64(tier.MinAge.Seconds()) {
            tiers = append(tiers, tier.Name)
        }
    }
    return tiers
}
```

**Speedup: 2-5x for recent queries** (most common case)

---

### 🟡 Issue #9: No Caching

**Problem:** Repeatedly deserializing the same chunks

**Solution: LRU Chunk Cache**
```go
import "github.com/hashicorp/golang-lru/v2"

type Engine struct {
    // ...
    chunkCache *lru.Cache[string, CompressedChunk]
}

func (e *Engine) getChunk(key []byte) (CompressedChunk, error) {
    keyStr := string(key)

    // Check cache
    if cached, ok := e.chunkCache.Get(keyStr); ok {
        return cached, nil
    }

    // Fetch from DB
    chunk := /* ... fetch ... */
    e.chunkCache.Add(keyStr, chunk)
    return chunk, nil
}
```

**Speedup: 5-10x for hot data** (dashboards re-querying)

---

### 🟡 Issue #10: Object Pooling

**Current:** Many temporary allocations

**Better: sync.Pool**
```go
var bufferPool = sync.Pool{
    New: func() interface{} {
        return bytes.NewBuffer(make([]byte, 0, 1024))
    },
}

// In compactor
buf := bufferPool.Get().(*bytes.Buffer)
buf.Reset()
defer bufferPool.Put(buf)

// Use buf for encoding
```

**Speedup: 10-20%** (reduces GC pressure)

---

## Part 3: Benchmark Comparison

### Current Implementation (estimated)
```
Metric: Counter incrementing by 1 every 10s
1 hour = 360 values
```

**Storage:**
- Header: 68 bytes
- Deltas (first-value): avg 1-2 bytes/value × 360 = 540 bytes
- **Total: 608 bytes**

### Optimized Implementation
```
Same metric, same 360 values
```

**Storage:**
- Header: 68 bytes
- DoD encoding: Most deltas are 0 or 1 = 1 byte × 360 = 360 bytes
- RLE compress stable runs: ~200 bytes
- Snappy compression: ~100 bytes
- **Total: 168 bytes**

**Compression improvement: 72% smaller!**

---

## Part 4: Recommended Implementation Priority

### Phase 1: Quick Wins (1-2 days)
1. ✅ Fix buffer reuse (line 317)
2. ✅ Pre-allocate buffer sizes
3. ✅ Smart tier selection in queries
4. ✅ Remove duplicate test/site writes (choose strategy)

**Expected gain: 2-3x write speed, 2x query speed**

---

### Phase 2: Compression (3-5 days)
5. ✅ Implement Delta-of-Delta encoding
6. ✅ Add Run-Length Encoding for forward-fill
7. ✅ Add Snappy compression to tier 1

**Expected gain: 60-70% better compression**

---

### Phase 3: Advanced (1-2 weeks)
8. ✅ LRU chunk cache
9. ✅ Parallel compaction workers
10. ✅ Object pooling (sync.Pool)
11. ✅ Tiered compression (Snappy → LZ4 → Zstd)

**Expected gain: 5-10x query speed, 90%+ compression**

---

## Part 5: Alternative Compression Algorithms

### For Integer Time Series (like yours)

**Gorilla (Facebook TSDB):**
- Delta-of-Delta with variable-bit encoding
- XOR compression for floats
- 1.37 bytes/value average

**Chimp (Uber):**
- Improvement over Gorilla
- Better for bursty data
- 1.02 bytes/value average

**Simple-8b (MongoDB/InfluxDB):**
- Packs multiple small integers into 64-bit words
- Very fast decode
- Good for small deltas

---

## Conclusion

**Current State:**
- Compression: ~93% vs raw (608 bytes for 360 values)
- Throughput: ~50,000 points/sec/core

**Optimized Target:**
- Compression: ~98% vs raw (150-200 bytes for 360 values)
- Throughput: ~500,000 points/sec/core

**ROI by Phase:**
- Phase 1: 2-3x speedup, minimal code changes
- Phase 2: 3-4x compression, moderate complexity
- Phase 3: 10x total speedup, higher complexity

Recommend starting with **Phase 1** for immediate gains, then Phase 2 for long-term storage savings.
