package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestVerifyCoreFunctionality(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 1 * time.Second,
		IngestBufferSize:   10000,
		CompactorWorkers:   2,
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

	fmt.Println("\n=== Core Functionality Test ===")

	// Test 1: Single value write/read
	engine.Add(1000, 100, 5001, 42)
	fmt.Println("✓ Wrote single value")

	// Wait for flush (need > ChunkFlushInterval + writer flush interval)
	time.Sleep(3 * time.Second)

	// Real-time query
	pt, exists := engine.GetLatestTest(100, 5001)
	if !exists || pt.Value != 42 {
		t.Errorf("Real-time query failed: exists=%v, value=%d", exists, pt.Value)
	} else {
		fmt.Println("✓ Real-time query works")
	}

	// Check database before query
	keyCount := 0
	engine.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			fmt.Printf("  DB Key: %s\n", it.Item().Key())
			keyCount++
		}
		return nil
	})
	fmt.Printf("Database has %d keys\n", keyCount)

	// Historical query
	fmt.Println("Querying for testID=5001, range [999, 1001]...")
	results, err := engine.GetTestRange(5001, 999, 1001)
	if err != nil {
		t.Fatalf("Historical query error: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("❌ Historical query returned 0 results")
	}

	found := false
	for _, r := range results {
		if r.Timestamp == 1000 && r.Value == 42 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("❌ Expected value not found in results. Got %d results", len(results))
		for i, r := range results {
			fmt.Printf("  [%d] ts=%d, val=%d\n", i, r.Timestamp, r.Value)
		}
	} else {
		fmt.Printf("✓ Historical query works (%d results)\n", len(results))
	}

	// Test 2: Multiple values
	for i := int64(2000); i < 2005; i++ {
		engine.Add(i, 200, 5002, i*10)
	}
	fmt.Println("✓ Wrote 5 sequential values")

	time.Sleep(3 * time.Second)

	// Check database
	keyCount2 := 0
	engine.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			if key[0:4] == "test" {
				fmt.Printf("  DB Key: %s\n", key)
				keyCount2++
			}
		}
		return nil
	})
	fmt.Printf("Database has %d test keys\n", keyCount2)

	results2, err := engine.GetTestRange(5002, 2000, 2004)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(results2) < 5 {
		t.Errorf("❌ Expected at least 5 results, got %d", len(results2))
	} else {
		fmt.Printf("✓ Multi-value query works (%d results)\n", len(results2))
	}

	// Test 3: Persistence
	engine.Stop()
	fmt.Println("✓ Engine stopped")

	engine2, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	engine2.Start()
	defer engine2.Stop()

	results3, err := engine2.GetTestRange(5001, 999, 1001)
	if err != nil {
		t.Fatalf("Query after restart error: %v", err)
	}
	if len(results3) == 0 {
		t.Fatal("❌ No data after restart")
	}
	fmt.Printf("✓ Persistence works (%d results after restart)\n", len(results3))

	fmt.Println("\n=== All Core Tests Passed ===")
}
