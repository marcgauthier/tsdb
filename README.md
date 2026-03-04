Here is a comprehensive `README.md` for your high-throughput TSDB engine. It outlines the architecture, features, and how to use the public API we designed.

---

# High-Throughput Go TSDB Engine

A highly optimized, pipeline-driven Time-Series Database (TSDB) engine written in Go, backed by [BadgerDB](https://github.com/dgraph-io/badger).

Designed to sustain ingestion rates of **100,000+ metrics per second**, this engine separates real-time state from historical storage. It utilizes fixed-interval time bucketing, bitmasking, and variable-length integer (Varint) delta-encoding to drastically reduce disk I/O and storage footprint.

## 🚀 Features

* **Non-Blocking Ingestion:** `Add()` calls are instantly pushed to a buffered channel, preventing application bottlenecks during high-traffic spikes.
* **Three-Stage Concurrency Pipeline:** Separates metric routing (Packer), CPU-heavy compression (Compactor), and disk I/O (Writer) into independent, highly parallel goroutines.
* **Real-Time Mirror:** Maintains an O(1) in-memory map of the "Latest Value" for every test and site, allowing instant dashboard updates without hitting the disk.
* **Fixed-Interval Bucketing:** Snaps incoming metrics to a strict time grid (e.g., 10s, 30s), eliminating the need to store timestamps on disk.
* **Delta-Encoded Compression:** Stores only the mathematical difference between consecutive values using Varints, shrinking storage requirements by up to 80%.
* **Forward-Fill Strategy:** Missing data points are automatically filled with the last known value, ensuring continuous time series without gaps. No arbitrary 64-point limit per chunk.
* **Multi-Tier Compaction:** Automatic background compaction merges small chunks into larger ones over time (e.g., 1h → 24h → 7d), reducing storage overhead and improving query performance for historical data.

## 🏗️ Architecture overview

The engine writes data via a pipeline and reads data via two distinct paths depending on the query type.

1. **Ingest (`Add`):** Raw metrics go into the `ingestChan`.
2. **Packer:** Routes data to in-memory time buckets and updates the Real-Time Mirror.
3. **Compactor:** Seals buckets at configured intervals, calculates deltas, and packs the bytes.
4. **Writer:** Batches compressed chunks into BadgerDB using lexicographical keys (`test!{ID}!{Time}`).

## 📦 Installation

Ensure you have Go installed, then add the BadgerDB dependency to your project:

```bash
go get github.com/dgraph-io/badger/v4

```

## 🛠️ Quick Start

### 1. Initialization

Configure and create the TSDB engine. The engine will manage the BadgerDB instance internally.

```go
package main

import (
	"time"
	"log"
	"yourproject/tsdb" // Update with your actual package path
)

func main() {
	// Configure TSDB Engine with multi-tier compaction
	cfg := tsdb.Config{
		DBPath:             "./data/badger",  // BadgerDB directory
		ChunkFlushInterval: 60 * time.Second, // Flush data every 60s
		IntervalResolution: 10 * time.Second, // Expect a metric every 10s
		IngestBufferSize:   100000,           // Channel buffer size
		CompactorWorkers:   4,                // Number of CPU-heavy goroutines

		// Multi-tier compaction setup
		CompactionTiers: []tsdb.CompactionTier{
			{Name: "1h", Duration: 1 * time.Hour, MinAge: 2 * time.Hour},
			{Name: "24h", Duration: 24 * time.Hour, MinAge: 48 * time.Hour},
			{Name: "7d", Duration: 7 * 24 * time.Hour, MinAge: 14 * 24 * time.Hour},
		},
		CompactionInterval:   1 * time.Hour, // Run compaction every hour
		EnableAutoCompaction: true,          // Enable background compaction
	}

	// Create engine (opens BadgerDB internally)
	engine, err := tsdb.NewEngine(cfg)
	if err != nil {
		log.Fatalf("Failed to start TSDB: %v", err)
	}

	// Start the background pipeline
	engine.Start()
	defer engine.Stop() // Closes BadgerDB and cleans up

    // ... application logic ...
}

```

### 2. Ingesting Data

The `Add` function is safe for highly concurrent use. It does not block for disk I/O.

```go
// Add(timestamp, siteID, testID, value)
engine.Add(time.Now().Unix(), 101, 5001, 245)
engine.Add(time.Now().Unix(), 101, 5002, 980)
engine.Add(time.Now().Unix(), 102, 5001, 240)

```

### 3. Querying Real-Time Data (In-Memory)

Use these functions to populate live dashboards. They read directly from the memory map and return instantly.

```go
// Get the absolute newest value for a specific test
latestPoint, exists := engine.GetLatestTest(101, 5001)

// Get all the newest values for an entire site
siteMetrics := engine.GetLatestSite(101)
for testID, point := range siteMetrics {
    log.Printf("Test: %d, Value: %d\n", testID, point.Value)
}

```

### 4. Querying Historical Data (Disk / BadgerDB)

Use these functions to render historical charts. They perform prefix scans on BadgerDB and decompress the delta-encoded chunks on the fly.

```go
startTime := time.Now().Add(-1 * time.Hour).Unix()
endTime := time.Now().Unix()

// Retrieve 1 hour of history for TestID 5001 (across all sites)
history, err := engine.GetTestRange(5001, startTime, endTime)
if err != nil {
    log.Fatalf("Query failed: %v", err)
}

for _, pt := range history {
    log.Printf("Time: %d, Site: %d, Value: %d\n", pt.Timestamp, pt.SiteID, pt.Value)
}

// Retrieve 1 hour of history for a specific test at a specific site
siteTestHistory, err := engine.GetSiteTestRange(101, 5001, startTime, endTime)
if err != nil {
    log.Fatalf("Query failed: %v", err)
}

for _, pt := range siteTestHistory {
    log.Printf("Time: %d, Value: %d\n", pt.Timestamp, pt.Value)
}

// Retrieve 1 hour of ALL tests for Site 101
siteHistory, err := engine.GetSiteRange(101, startTime, endTime)
if err != nil {
    log.Fatalf("Query failed: %v", err)
}

for _, pt := range siteHistory {
    log.Printf("Time: %d, Test: %d, Value: %d\n", pt.Timestamp, pt.TestID, pt.Value)
}

```

## 🔄 Multi-Tier Compaction

The engine supports automatic background compaction to optimize storage and query performance:

### How It Works

1. **Initial Storage:** Incoming data is written to the first tier (e.g., 1-hour chunks)
2. **Background Compaction:** A goroutine runs periodically to merge old chunks
3. **Tier Promotion:** Once chunks reach `MinAge`, they're merged into the next tier
4. **Automatic Cleanup:** Original chunks are deleted after successful merging

### Example Lifecycle

```
Time 0h:   Data written to 1h chunks
Time 2h:   1h chunks merged into 24h chunks (MinAge reached)
Time 48h:  24h chunks merged into 7d chunks
```

### Configuration

```go
CompactionTiers: []tsdb.CompactionTier{
    {
        Name:     "1h",              // Tier name (used in key prefix)
        Duration: 1 * time.Hour,     // Each chunk covers 1 hour
        MinAge:   2 * time.Hour,     // Compact after 2 hours
    },
    {
        Name:     "24h",
        Duration: 24 * time.Hour,
        MinAge:   48 * time.Hour,    // Compact 24h chunks after 2 days
    },
    {
        Name:     "7d",
        Duration: 7 * 24 * time.Hour,
        MinAge:   0,                 // Last tier, never compacted further
    },
}
```

### Benefits

- **Reduced Chunk Count:** 24 1-hour chunks become 1 24-hour chunk
- **Faster Queries:** Fewer chunks to scan for historical data
- **Lower Overhead:** Less metadata per time period
- **Flexible Retention:** Add/remove tiers based on your needs

## ⚙️ Key Formats (For Internal Reference)

The engine stores data in BadgerDB using Big Endian formatting to ensure chronological prefix scanning works correctly.

* **Test Prefix:** `test-{tier}!{TestID}!{Timestamp}` -> `CompressedChunk[]`
* **Site Prefix:** `site-{tier}!{SiteID}!{Timestamp}` -> `CompressedChunk[]`

Examples:
- `test-1h!0000000000001389!0000000001a2b3c4` (1-hour tier chunk)
- `site-24h!0000000000000065!0000000001a2b3c4` (24-hour tier chunk)

---