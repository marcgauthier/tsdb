package tsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"github.com/dgraph-io/badger/v4"
	"testing"
	"time"
)

func TestRealisticDiskThroughput(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 500 * time.Millisecond, // More frequent flushes
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   50000,
		CompactorWorkers:   4,
		CompactionTiers: []CompactionTier{
			{Name: "500ms", Duration: 500 * time.Millisecond, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	fmt.Println("\n=== Realistic Disk Write Throughput ===")

	// Test 1: Single-threaded with disk sync
	{
		fmt.Println("\n[1] Single-threaded burst (with disk flush)")
		count := 50000
		now := time.Now().Unix()
		start := time.Now()

		for i := 0; i < count; i++ {
			engine.Add(now+int64(i/100), 100, 5001, int64(i))
		}

		// Wait for writer to flush (1s periodic + safety margin)
		time.Sleep(2 * time.Second)

		elapsed := time.Since(start)
		rate := float64(count) / elapsed.Seconds()
		fmt.Printf("  Rate: %10.0f inserts/sec (%d inserts in %v)\n", 
			rate, count, elapsed.Round(time.Millisecond))
	}

	// Test 2: Multi-threaded with disk sync
	{
		fmt.Println("\n[2] Multi-threaded (4 threads, with disk flush)")
		numThreads := 4
		insertsPerThread := 25000
		totalInserts := numThreads * insertsPerThread

		var wg sync.WaitGroup
		now := time.Now().Unix()
		start := time.Now()

		for g := 0; g < numThreads; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				siteID := int64(200 + id)
				testID := int64(6000 + id)

				for i := 0; i < insertsPerThread; i++ {
					engine.Add(now+int64(i/100), siteID, testID, int64(i))
				}
			}(g)
		}

		wg.Wait()
		time.Sleep(2 * time.Second) // Wait for flush

		elapsed := time.Since(start)
		rate := float64(totalInserts) / elapsed.Seconds()
		fmt.Printf("  Rate: %10.0f inserts/sec (%d inserts in %v)\n",
			rate, totalInserts, elapsed.Round(time.Millisecond))
	}

	// Test 3: Continuous sustained load
	{
		fmt.Println("\n[3] Sustained load (30 seconds, 8 threads)")
		duration := 30 * time.Second
		numThreads := 8
		var counter int64
		stop := make(chan bool)

		now := time.Now().Unix()
		start := time.Now()

		var wg sync.WaitGroup
		for g := 0; g < numThreads; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				siteID := int64(300 + id)
				testID := int64(7000 + id)
				i := int64(0)

				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						// Write batch
						for j := 0; j < 10; j++ {
							engine.Add(now+i, siteID, testID, i)
							i++
							atomic.AddInt64(&counter, 1)
						}
					}
				}
			}(g)
		}

		time.Sleep(duration)
		close(stop)
		wg.Wait()

		// Wait for final flush
		time.Sleep(2 * time.Second)

		elapsed := time.Since(start)
		count := atomic.LoadInt64(&counter)
		rate := float64(count) / elapsed.Seconds()

		fmt.Printf("  Rate: %10.0f inserts/sec (%d total in %v)\n",
			rate, count, elapsed.Round(time.Second))
	}

	// Verify disk writes
	{
		fmt.Println("\n[4] Verification")
		engine.Stop()

		// Check database size
		var dbSize int64
		filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				dbSize += info.Size()
			}
			return nil
		})

		// Reopen and count
		engine2, _ := NewEngine(cfg)
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
		engine2.Stop()

		fmt.Printf("  Keys on disk: %d\n", keyCount)
		fmt.Printf("  Database size: %.2f MB\n", float64(dbSize)/(1024*1024))
	}

	fmt.Println("")
}
