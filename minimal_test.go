package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestMinimalWriteRead(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   10000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   60 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}

	engine.Start()

	fmt.Println("\n=== Minimal Write/Read Test ===")

	// Write ONE value
	timestamp := int64(10000)
	engine.Add(timestamp, 100, 5001, 42)
	fmt.Printf("\nWrote: timestamp=%d, site=100, test=5001, value=42\n", timestamp)

	// Wait for flush
	fmt.Println("Waiting 3 seconds for flush...")
	time.Sleep(3 * time.Second)

	// Check database directly
	keyCount := 0
	engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			fmt.Printf("  DB Key: %s\n", key)
			keyCount++
		}
		return nil
	})
	fmt.Printf("Keys in database: %d\n", keyCount)

	// Try real-time query
	fmt.Println("\nTrying real-time query...")
	pt, exists := engine.GetLatestTest(100, 5001)
	if exists {
		fmt.Printf("  Real-time: value=%d ✓\n", pt.Value)
	} else {
		fmt.Println("  Real-time: not found ❌")
	}

	// Try range query
	fmt.Println("\nTrying range query...")
	results, err := engine.GetTestRange(5001, timestamp-5, timestamp+5, Scale5m)
	if err != nil {
		fmt.Printf("  Query error: %v\n", err)
	} else {
		fmt.Printf("  Query returned %d results\n", len(results))
		for i, pt := range results {
			if i < 15 {
				fmt.Printf("    [%d] ts=%d, val=%d\n", i, pt.Timestamp, pt.Value)
			}
		}
	}

	engine.Stop()
	fmt.Println("")
}
