package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestQuickIntegrityCheck(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 10 * time.Second, // Long enough for all test data
		IntervalResolution: 1 * time.Second,  // 1-second intervals
		IngestBufferSize:   10000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "10s", Duration: 10 * time.Second, MinAge: 60 * time.Second},
		},
		CompactionInterval:   60 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()

	fmt.Println("\n=== Quick Data Integrity Check ===")

	// Test 1: Write and read exact values
	{
		fmt.Println("[1] Basic Write/Read Test")
		now := time.Now().Unix()
		testData := map[int64]int64{
			now + 0: 100,
			now + 1: 200,
			now + 2: 300,
			now + 3: 400,
			now + 4: 500,
		}

		for ts, val := range testData {
			engine.Add(ts, 100, 5001, val)
		}

		time.Sleep(2 * time.Second) // Wait for flush

		results, _ := engine.GetTestRange(5001, now, now+10, Scale5m)

		if len(results) == 0 {
			t.Error("  ❌ No results returned")
		} else {
			matches := 0
			for _, pt := range results {
				if expected, ok := testData[pt.Timestamp]; ok {
					if pt.Value == expected {
						matches++
					} else {
						t.Errorf("  ❌ Timestamp %d: expected %d, got %d", pt.Timestamp, expected, pt.Value)
					}
				}
			}
			if matches == len(testData) {
				fmt.Printf("  ✓ All %d values match\n", matches)
			} else {
				t.Errorf("  ❌ Only %d/%d values match", matches, len(testData))
			}
		}
	}

	// Test 2: Multiple series isolation
	{
		fmt.Println("\n[2] Series Isolation Test")
		now := time.Now().Unix()

		// Write to different series
		for i := 0; i < 10; i++ {
			engine.Add(now+int64(i), 100, 5001, 1000+int64(i))
			engine.Add(now+int64(i), 100, 5002, 2000+int64(i))
			engine.Add(now+int64(i), 101, 5001, 3000+int64(i))
		}

		time.Sleep(2 * time.Second)

		// Query each series
		series1, _ := engine.GetSiteTestRange(100, 5001, now, now+10, Scale5m)
		series2, _ := engine.GetSiteTestRange(100, 5002, now, now+10, Scale5m)
		series3, _ := engine.GetSiteTestRange(101, 5001, now, now+10, Scale5m)

		errors := 0

		// Verify series1 has only its values (1000-1009)
		for _, pt := range series1 {
			if pt.Value < 1000 || pt.Value > 1009 {
				t.Errorf("  ❌ Series1 contaminated: got value %d", pt.Value)
				errors++
			}
		}

		// Verify series2 has only its values (2000-2009)
		for _, pt := range series2 {
			if pt.Value < 2000 || pt.Value > 2009 {
				t.Errorf("  ❌ Series2 contaminated: got value %d", pt.Value)
				errors++
			}
		}

		// Verify series3 has only its values (3000-3009)
		for _, pt := range series3 {
			if pt.Value < 3000 || pt.Value > 3009 {
				t.Errorf("  ❌ Series3 contaminated: got value %d", pt.Value)
				errors++
			}
		}

		if errors == 0 {
			fmt.Printf("  ✓ 3 series isolated correctly\n")
		}
	}

	// Test 3: Extreme values
	{
		fmt.Println("\n[3] Extreme Values Test")
		now := time.Now().Unix()

		extremes := []int64{
			0,
			-1000,
			1000000000,
			-1000000000,
		}

		for i, val := range extremes {
			engine.Add(now+int64(i), 100, 5003, val)
		}

		time.Sleep(2 * time.Second)

		results, _ := engine.GetTestRange(5003, now, now+10, Scale5m)
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

	// Test 4: Persistence
	{
		fmt.Println("\n[4] Persistence Test")
		now := time.Now().Unix()

		for i := 0; i < 20; i++ {
			engine.Add(now+int64(i), 100, 5004, 7000+int64(i))
		}

		time.Sleep(2 * time.Second)
		engine.Stop()

		// Reopen
		engine2, err := NewEngine(cfg)
		if err != nil {
			t.Fatalf("Failed to reopen: %v", err)
		}
		engine2.Start()
		defer engine2.Stop()

		results, _ := engine2.GetTestRange(5004, now, now+30, Scale5m)

		if len(results) == 0 {
			t.Error("  ❌ No data persisted")
		} else {
			// Check if values are in expected range
			validCount := 0
			for _, pt := range results {
				if pt.Value >= 7000 && pt.Value <= 7019 {
					validCount++
				}
			}
			if validCount > 0 {
				fmt.Printf("  ✓ %d values survived restart\n", validCount)
			} else {
				t.Error("  ❌ No valid values after restart")
			}
		}
	}

	fmt.Println("")
}
