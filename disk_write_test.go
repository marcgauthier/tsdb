package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestActualDiskWriteThroughput(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   200000,
		CompactorWorkers:   4,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()

	fmt.Println("\n=== Disk Write Throughput Test ===")

	// Test 1: Burst without waiting for disk
	{
		count := 100000
		now := time.Now().Unix()
		start := time.Now()

		for i := 0; i < count; i++ {
			engine.Add(now+int64(i/1000), 100, 5001, int64(i))
		}

		elapsed := time.Since(start)
		rate := float64(count) / elapsed.Seconds()
		fmt.Printf("\nBurst (in-memory only):          %10.0f inserts/sec (%v)\n", rate, elapsed.Round(time.Millisecond))
	}

	// Test 2: Wait for flush to disk
	{
		fmt.Println("\nWaiting 3 seconds for data to flush to disk...")
		time.Sleep(3 * time.Second)

		count := 100000
		now := time.Now().Unix()
		start := time.Now()

		for i := 0; i < count; i++ {
			engine.Add(now+int64(i/1000), 200, 5002, int64(i))
		}

		insertElapsed := time.Since(start)

		// Wait for all data to be written
		fmt.Println("Waiting for data to flush to disk...")
		time.Sleep(3 * time.Second)

		totalElapsed := time.Since(start)

		insertRate := float64(count) / insertElapsed.Seconds()
		diskRate := float64(count) / totalElapsed.Seconds()

		fmt.Printf("Insert rate (before flush):      %10.0f inserts/sec\n", insertRate)
		fmt.Printf("Disk write rate (after flush):   %10.0f inserts/sec\n", diskRate)
	}

	// Test 3: Sustained with guaranteed disk writes
	{
		duration := 10 * time.Second
		count := 0
		now := time.Now().Unix()
		start := time.Now()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		done := time.After(duration)
		i := 0

	loop:
		for {
			select {
			case <-done:
				break loop
			case <-ticker.C:
				// Add batch
				for j := 0; j < 1000; j++ {
					engine.Add(now+int64(i), 300, 5003, int64(i))
					i++
					count++
				}
			}
		}

		// Wait for final flush
		time.Sleep(2 * time.Second)

		totalElapsed := time.Since(start)
		rate := float64(count) / totalElapsed.Seconds()

		fmt.Printf("\nSustained (10s + flush):         %10.0f inserts/sec (%d total)\n",
			rate, count)
	}

	// Test 4: Check what's actually on disk
	{
		engine.Stop() // Ensure everything is flushed

		// Re-open to verify
		engine2, _ := NewEngine(cfg)
		defer engine2.Stop()

		// Count keys in database
		keyCount := 0
		engine2.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				keyCount++
			}
			return nil
		})

		fmt.Printf("\nKeys actually written to disk:   %10d\n", keyCount)
	}

	fmt.Println("")
}
