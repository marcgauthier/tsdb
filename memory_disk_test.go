package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryDiskQuery(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 2 * time.Second, // 2 second flush interval
		IntervalResolution: 1 * time.Second,
		IngestBufferSize:   10000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "2s", Duration: 2 * time.Second, MinAge: 10 * time.Second},
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

	fmt.Println("\n=== Memory + Disk Query Test ===")

	// Write "old" data (simulating days 1-4)
	fmt.Println("Writing old data (timestamps 1000-1003)...")
	for i := int64(1000); i <= 1003; i++ {
		engine.Add(i, 100, 5001, i*10)
	}

	// Wait for flush to disk
	fmt.Println("Waiting 3 seconds for data to flush to disk...")
	time.Sleep(3 * time.Second)

	// Write "new" data (simulating today - not yet flushed)
	fmt.Println("Writing new data (timestamps 1004-1006, NOT flushed)...")
	for i := int64(1004); i <= 1006; i++ {
		engine.Add(i, 100, 5001, i*10)
	}

	// Query immediately (before new data is flushed)
	fmt.Println("\nQuerying for full range [1000, 1006] BEFORE flush...")
	results, err := engine.GetTestRange(5001, 1000, 1006, Scale5m)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Got %d results (should be 7: 4 from disk + 3 from memory)\n", len(results))

	// Verify all timestamps are present
	expectedTimestamps := []int64{1000, 1001, 1002, 1003, 1004, 1005, 1006}
	if len(results) != len(expectedTimestamps) {
		t.Errorf("Expected %d results, got %d", len(expectedTimestamps), len(results))
	}

	// Check each timestamp
	for i, expected := range expectedTimestamps {
		if i >= len(results) {
			t.Errorf("Missing timestamp %d", expected)
			continue
		}
		if results[i].Timestamp != expected {
			t.Errorf("Result[%d]: expected timestamp %d, got %d", i, expected, results[i].Timestamp)
		}
		expectedValue := expected * 10
		if results[i].Value != expectedValue {
			t.Errorf("Result[%d]: expected value %d, got %d", i, expectedValue, results[i].Value)
		}
		fmt.Printf("  [%d] ts=%d, val=%d ✓\n", i, results[i].Timestamp, results[i].Value)
	}

	// Verify queries split correctly
	fmt.Println("\nVerifying partial queries...")

	// Query only disk data
	diskResults, err := engine.GetTestRange(5001, 1000, 1003, Scale5m)
	if err != nil {
		t.Fatalf("Disk query failed: %v", err)
	}
	fmt.Printf("Disk-only query [1000-1003]: %d results\n", len(diskResults))
	if len(diskResults) != 4 {
		t.Errorf("Expected 4 disk results, got %d", len(diskResults))
	}

	// Query only memory data
	memResults, err := engine.GetTestRange(5001, 1004, 1006, Scale5m)
	if err != nil {
		t.Fatalf("Memory query failed: %v", err)
	}
	fmt.Printf("Memory-only query [1004-1006]: %d results\n", len(memResults))
	if len(memResults) != 3 {
		t.Errorf("Expected 3 memory results, got %d", len(memResults))
	}

	// Test overlap scenario: write same timestamp to both disk and memory
	fmt.Println("\nTesting memory override (same timestamp in disk and memory)...")

	// Wait for flush
	time.Sleep(3 * time.Second)

	// Write again to timestamp 1003 (should override)
	engine.Add(1003, 100, 5001, 99999)

	// Query immediately
	overrideResults, err := engine.GetTestRange(5001, 1003, 1003, Scale5m)
	if err != nil {
		t.Fatalf("Override query failed: %v", err)
	}

	if len(overrideResults) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(overrideResults))
	}

	if overrideResults[0].Value != 99999 {
		t.Errorf("Expected memory value (99999) to override disk value, got %d", overrideResults[0].Value)
	} else {
		fmt.Printf("Memory value correctly overrode disk value: ts=%d, val=%d ✓\n",
			overrideResults[0].Timestamp, overrideResults[0].Value)
	}

	fmt.Println("\n✅ All memory+disk query tests passed!")
}
