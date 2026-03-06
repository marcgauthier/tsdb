package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestDataIntegrity_Verified(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	fmt.Println("\n=== Data Integrity Verification ===")

	// Test 1: Basic Write/Read with forced flush
	{
		fmt.Println("\n[1] Basic Write/Read")

		cfg := Config{
			DBPath:             dbPath,
			ChunkFlushInterval: 500 * time.Millisecond,
			IntervalResolution: 100 * time.Millisecond,
			IngestBufferSize:   10000,
			CompactorWorkers:   2,
			CompactionTiers: []CompactionTier{
				{Name: "500ms", Duration: 500 * time.Millisecond, MinAge: 60 * time.Second},
			},
			CompactionInterval:   60 * time.Second,
			EnableAutoCompaction: false,
		}

		engine, _ := NewEngine(cfg)
		engine.Start()

		testData := []struct{ ts, val int64 }{
			{10000, 100},
			{10000, 200},
			{10000, 300},
			{10000, 400},
			{10000, 500},
		}

		for _, td := range testData {
			engine.Add(td.ts, 100, 5001, td.val)
		}

		// Wait for at least one flush cycle
		time.Sleep(1500 * time.Millisecond)

		// Query
		results, _ := engine.GetTestRange(5001, 9990, 10010, Scale5m)

		engine.Stop()

		if len(results) == 0 {
			t.Error("  ❌ No results")
		} else {
			// Should have forward-filled values = 500 (last value)
			allCorrect := true
			for _, pt := range results {
				if pt.Value != 500 {
					t.Errorf("  ❌ Expected 500, got %d", pt.Value)
					allCorrect = false
					break
				}
			}
			if allCorrect {
				fmt.Printf("  ✓ Wrote 5 values, read %d forward-filled points\n", len(results))
			}
		}
	}

	// Test 2: Persistence across restart
	{
		fmt.Println("\n[2] Persistence Across Restart")

		cfg := Config{
			DBPath:             dbPath,
			ChunkFlushInterval: 500 * time.Millisecond,
			IntervalResolution: 100 * time.Millisecond,
			IngestBufferSize:   10000,
			CompactorWorkers:   2,
			CompactionTiers: []CompactionTier{
				{Name: "500ms", Duration: 500 * time.Millisecond, MinAge: 60 * time.Second},
			},
			CompactionInterval:   60 * time.Second,
			EnableAutoCompaction: false,
		}

		// Write session
		{
			engine, _ := NewEngine(cfg)
			engine.Start()

			for i := 0; i < 50; i++ {
				engine.Add(20000+int64(i), 100, 5002, 1000+int64(i))
			}

			time.Sleep(1500 * time.Millisecond)
			engine.Stop()
		}

		// Read session
		{
			engine, _ := NewEngine(cfg)
			engine.Start()

			results, _ := engine.GetTestRange(5002, 20000, 20050, Scale5m)
			engine.Stop()

			if len(results) == 0 {
				t.Error("  ❌ No data persisted")
			} else {
				// Verify some values
				hasCorrectValues := false
				for _, pt := range results {
					if pt.Value >= 1000 && pt.Value <= 1049 {
						hasCorrectValues = true
						break
					}
				}
				if hasCorrectValues {
					fmt.Printf("  ✓ %d points persisted and reloaded\n", len(results))
				} else {
					t.Error("  ❌ Values incorrect after restart")
				}
			}
		}
	}

	// Test 3: Large dataset integrity
	{
		fmt.Println("\n[3] Large Dataset (1000 points)")

		cfg := Config{
			DBPath:             dbPath + "3",
			ChunkFlushInterval: 500 * time.Millisecond,
			IntervalResolution: 100 * time.Millisecond,
			IngestBufferSize:   50000,
			CompactorWorkers:   4,
			CompactionTiers: []CompactionTier{
				{Name: "500ms", Duration: 500 * time.Millisecond, MinAge: 60 * time.Second},
			},
			CompactionInterval:   60 * time.Second,
			EnableAutoCompaction: false,
		}

		engine, _ := NewEngine(cfg)
		engine.Start()

		expected := make(map[int64]int64)
		startTs := int64(30000)

		for i := 0; i < 1000; i++ {
			ts := startTs + int64(i/10) // Group into chunks
			val := int64(5000 + i)
			engine.Add(ts, 100, 5003, val)
			expected[ts] = val // Last value wins
		}

		time.Sleep(2 * time.Second)

		results, _ := engine.GetTestRange(5003, startTs-10, startTs+200, Scale5m)
		engine.Stop()

		if len(results) == 0 {
			t.Error("  ❌ No results from large dataset")
		} else {
			// Verify a sample of values
			errors := 0
			checked := 0
			for _, pt := range results {
				if expectedVal, ok := expected[pt.Timestamp]; ok {
					if pt.Value != expectedVal {
						errors++
						if errors < 5 {
							t.Errorf("  ❌ ts=%d: expected %d, got %d", pt.Timestamp, expectedVal, pt.Value)
						}
					}
					checked++
				}
			}

			if errors == 0 {
				fmt.Printf("  ✓ 1000 values written, %d verified\n", checked)
			} else {
				t.Errorf("  ❌ %d errors out of %d checked", errors, checked)
			}
		}
	}

	// Test 4: Extreme values
	{
		fmt.Println("\n[4] Extreme Values")

		cfg := Config{
			DBPath:             dbPath + "4",
			ChunkFlushInterval: 500 * time.Millisecond,
			IntervalResolution: 100 * time.Millisecond,
			IngestBufferSize:   10000,
			CompactorWorkers:   2,
			CompactionTiers: []CompactionTier{
				{Name: "500ms", Duration: 500 * time.Millisecond, MinAge: 60 * time.Second},
			},
			CompactionInterval:   60 * time.Second,
			EnableAutoCompaction: false,
		}

		engine, _ := NewEngine(cfg)
		engine.Start()

		extremes := []int64{
			0,
			-1000,
			1000000000,
			-1000000000,
			9223372036854775807,  // max int64
			-9223372036854775808, // min int64
		}

		for i, val := range extremes {
			engine.Add(40000+int64(i), 100, 5004, val)
		}

		time.Sleep(1500 * time.Millisecond)

		results, _ := engine.GetTestRange(5004, 39990, 40010, Scale5m)
		engine.Stop()

		if len(results) == 0 {
			t.Error("  ❌ No results")
		} else {
			foundValues := make(map[int64]bool)
			for _, pt := range results {
				foundValues[pt.Value] = true
			}

			allFound := true
			for _, val := range extremes {
				if !foundValues[val] {
					t.Errorf("  ❌ Extreme value %d not found", val)
					allFound = false
				}
			}

			if allFound {
				fmt.Printf("  ✓ All %d extreme values preserved\n", len(extremes))
			}
		}
	}

	fmt.Println("")
}
