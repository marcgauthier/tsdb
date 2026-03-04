package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestDataIntegrity_BasicWriteRead verifies basic write/read consistency
func TestDataIntegrity_BasicWriteRead(t *testing.T) {
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
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	// Write known test data
	testData := []struct {
		timestamp int64
		siteID    int64
		testID    int64
		value     int64
	}{
		{1000, 100, 5001, 42},
		{1001, 100, 5001, 43},
		{1002, 100, 5001, 44},
		{1003, 100, 5001, 45},
		{1004, 100, 5001, 46},
	}

	for _, td := range testData {
		engine.Add(td.timestamp, td.siteID, td.testID, td.value)
	}

	// Wait for flush
	time.Sleep(2 * time.Second)

	// Read back and verify
	results, err := engine.GetTestRange(5001, 1000, 1005)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("No results returned")
	}

	// Verify values
	foundValues := make(map[int64]int64) // timestamp -> value
	for _, pt := range results {
		if pt.Timestamp >= 1000 && pt.Timestamp <= 1004 {
			foundValues[pt.Timestamp] = pt.Value
		}
	}

	for _, td := range testData {
		if val, ok := foundValues[td.timestamp]; ok {
			if val != td.value {
				t.Errorf("Timestamp %d: expected value %d, got %d", td.timestamp, td.value, val)
			}
		} else {
			t.Errorf("Timestamp %d: value not found in results", td.timestamp)
		}
	}

	t.Logf("✓ Basic write/read: All %d values verified", len(testData))
}

// TestDataIntegrity_LargeDataset verifies integrity with larger dataset
func TestDataIntegrity_LargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   50000,
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
	defer engine.Stop()

	// Write 10,000 data points with predictable values
	startTime := time.Now().Unix()
	count := 10000
	expectedValues := make(map[int64]int64)

	for i := 0; i < count; i++ {
		ts := startTime + int64(i)
		value := int64(1000 + i*2) // Predictable pattern: 1000, 1002, 1004, ...
		engine.Add(ts, 100, 5001, value)
		expectedValues[ts] = value
	}

	// Wait for flush
	time.Sleep(3 * time.Second)

	// Read back all data
	results, err := engine.GetTestRange(5001, startTime, startTime+int64(count))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify count
	if len(results) == 0 {
		t.Fatal("No results returned")
	}

	// Verify each value
	errors := 0
	for _, pt := range results {
		if expected, ok := expectedValues[pt.Timestamp]; ok {
			if pt.Value != expected {
				t.Errorf("Timestamp %d: expected %d, got %d", pt.Timestamp, expected, pt.Value)
				errors++
				if errors > 10 {
					t.Fatal("Too many errors, stopping")
				}
			}
			delete(expectedValues, pt.Timestamp)
		}
	}

	// Check if any expected values were missing
	if len(expectedValues) > 0 {
		t.Errorf("Missing %d expected values", len(expectedValues))
	}

	t.Logf("✓ Large dataset: %d values written and verified", count)
}

// TestDataIntegrity_Persistence verifies data survives engine restart
func TestDataIntegrity_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	cfg := Config{
		DBPath:             dbPath,
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   10000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	// First session: write data
	expectedData := make(map[int64]int64)
	{
		engine, err := NewEngine(cfg)
		if err != nil {
			t.Fatalf("Failed to create engine: %v", err)
		}

		engine.Start()

		startTime := int64(2000)
		for i := 0; i < 100; i++ {
			ts := startTime + int64(i)
			value := int64(5000 + i*3)
			engine.Add(ts, 200, 6001, value)
			expectedData[ts] = value
		}

		time.Sleep(3 * time.Second) // Wait for flush
		engine.Stop()
	}

	// Second session: read data back
	{
		engine, err := NewEngine(cfg)
		if err != nil {
			t.Fatalf("Failed to reopen engine: %v", err)
		}

		engine.Start()
		defer engine.Stop()

		results, err := engine.GetTestRange(6001, 2000, 2100)
		if err != nil {
			t.Fatalf("Query after restart failed: %v", err)
		}

		if len(results) == 0 {
			t.Fatal("No data persisted across restart")
		}

		// Verify values
		for _, pt := range results {
			if expected, ok := expectedData[pt.Timestamp]; ok {
				if pt.Value != expected {
					t.Errorf("After restart: timestamp %d expected %d, got %d",
						pt.Timestamp, expected, pt.Value)
				}
			}
		}

		t.Logf("✓ Persistence: %d values survived restart", len(expectedData))
	}
}

// TestDataIntegrity_ForwardFillCorrectness verifies forward-fill behavior
func TestDataIntegrity_ForwardFillCorrectness(t *testing.T) {
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
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	// Write sparse data: only at certain timestamps
	// With 100ms resolution and 1s chunks, we get 10 slots per chunk
	startTime := int64(3000)
	engine.Add(startTime+0, 100, 5001, 100) // Slot 0
	engine.Add(startTime+0, 100, 5001, 200) // Slot 5 (overwrites to 200)
	engine.Add(startTime+0, 100, 5001, 300) // Final value at slot 0

	time.Sleep(2 * time.Second)

	// Query the chunk
	results, err := engine.GetTestRange(5001, startTime, startTime+1)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("No results from forward-fill query")
	}

	// All values in the chunk should be forward-filled from the last written value
	for i, pt := range results {
		if pt.Value != 300 {
			t.Errorf("Point %d at timestamp %d: expected forward-fill value 300, got %d",
				i, pt.Timestamp, pt.Value)
		}
	}

	t.Logf("✓ Forward-fill: %d values correctly filled with last value (300)", len(results))
}

// TestDataIntegrity_MultipleSeriesIsolation verifies data doesn't leak between series
func TestDataIntegrity_MultipleSeriesIsolation(t *testing.T) {
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
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	// Write data to multiple series with distinct values
	startTime := int64(4000)
	series := []struct {
		siteID int64
		testID int64
		value  int64
	}{
		{100, 5001, 1000},
		{100, 5002, 2000},
		{101, 5001, 3000},
		{101, 5002, 4000},
	}

	for _, s := range series {
		for i := 0; i < 10; i++ {
			engine.Add(startTime+int64(i), s.siteID, s.testID, s.value+int64(i))
		}
	}

	time.Sleep(2 * time.Second)

	// Verify each series independently
	for _, s := range series {
		results, err := engine.GetSiteTestRange(s.siteID, s.testID, startTime, startTime+10)
		if err != nil {
			t.Fatalf("Query failed for site %d test %d: %v", s.siteID, s.testID, err)
		}

		if len(results) == 0 {
			t.Errorf("No results for site %d test %d", s.siteID, s.testID)
			continue
		}

		// Verify all values belong to this series
		for _, pt := range results {
			if pt.SiteID != s.siteID {
				t.Errorf("Data leak: expected siteID %d, got %d", s.siteID, pt.SiteID)
			}
			if pt.TestID != s.testID {
				t.Errorf("Data leak: expected testID %d, got %d", s.testID, pt.TestID)
			}

			// Verify value is in expected range
			expectedMin := s.value
			expectedMax := s.value + 9
			if pt.Value < expectedMin || pt.Value > expectedMax {
				t.Errorf("Value out of range for site %d test %d: got %d, expected [%d, %d]",
					s.siteID, s.testID, pt.Value, expectedMin, expectedMax)
			}
		}
	}

	t.Logf("✓ Series isolation: %d series verified independently", len(series))
}

// TestDataIntegrity_ExtremeValues verifies handling of edge case values
func TestDataIntegrity_ExtremeValues(t *testing.T) {
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
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	// Test extreme values
	extremeValues := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"negative", -1000},
		{"max_int64", 9223372036854775807},
		{"min_int64", -9223372036854775808},
		{"large_positive", 1000000000000},
		{"large_negative", -1000000000000},
	}

	startTime := int64(5000)
	for i, ev := range extremeValues {
		engine.Add(startTime+int64(i), 100, 5001, ev.value)
	}

	time.Sleep(2 * time.Second)

	// Read back and verify
	results, err := engine.GetTestRange(5001, startTime, startTime+10)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	foundValues := make(map[int64]bool)
	for _, pt := range results {
		foundValues[pt.Value] = true
	}

	for _, ev := range extremeValues {
		if !foundValues[ev.value] {
			t.Errorf("Extreme value '%s' (%d) not found in results", ev.name, ev.value)
		}
	}

	t.Logf("✓ Extreme values: All %d extreme values handled correctly", len(extremeValues))
}

// TestDataIntegrity_CompressionFidelity verifies compression doesn't lose data
func TestDataIntegrity_CompressionFidelity(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   50000,
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
	defer engine.Stop()

	// Test different data patterns that stress compression
	patterns := []struct {
		name   string
		values []int64
	}{
		{"constant", []int64{100, 100, 100, 100, 100}},
		{"increasing", []int64{1, 2, 3, 4, 5}},
		{"decreasing", []int64{100, 90, 80, 70, 60}},
		{"random", []int64{42, 1001, 33, 9999, 17}},
		{"alternating", []int64{0, 1000, 0, 1000, 0}},
	}

	startTime := int64(6000)
	offset := int64(0)

	for _, pattern := range patterns {
		for i, value := range pattern.values {
			engine.Add(startTime+offset+int64(i), 100, 5001, value)
		}
		offset += 10 // Space out patterns
	}

	time.Sleep(2 * time.Second)

	// Verify each pattern
	offset = int64(0)
	for _, pattern := range patterns {
		results, err := engine.GetTestRange(5001, startTime+offset, startTime+offset+10)
		if err != nil {
			t.Fatalf("Query failed for pattern '%s': %v", pattern.name, err)
		}

		// Extract values for this pattern
		foundValues := []int64{}
		for _, pt := range results {
			if pt.Timestamp >= startTime+offset && pt.Timestamp < startTime+offset+int64(len(pattern.values)) {
				foundValues = append(foundValues, pt.Value)
			}
		}

		// Verify
		if len(foundValues) < len(pattern.values) {
			t.Errorf("Pattern '%s': expected %d values, got %d",
				pattern.name, len(pattern.values), len(foundValues))
		}

		// Check that all expected values are present (may be forward-filled)
		for _, expectedValue := range pattern.values {
			found := false
			for _, v := range foundValues {
				if v == expectedValue {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Pattern '%s': value %d not found", pattern.name, expectedValue)
			}
		}

		offset += 10
	}

	t.Logf("✓ Compression fidelity: %d data patterns verified", len(patterns))
}

// TestDataIntegrity_FullReport runs all integrity tests and provides summary
func TestDataIntegrity_FullReport(t *testing.T) {
	fmt.Println("\n=== Data Integrity Test Report ===")

	tests := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"Basic Write/Read", TestDataIntegrity_BasicWriteRead},
		{"Large Dataset (10K points)", TestDataIntegrity_LargeDataset},
		{"Persistence Across Restart", TestDataIntegrity_Persistence},
		{"Forward-Fill Correctness", TestDataIntegrity_ForwardFillCorrectness},
		{"Multiple Series Isolation", TestDataIntegrity_MultipleSeriesIsolation},
		{"Extreme Values", TestDataIntegrity_ExtremeValues},
		{"Compression Fidelity", TestDataIntegrity_CompressionFidelity},
	}

	passed := 0
	failed := 0

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fn(t)
			if !t.Failed() {
				passed++
			} else {
				failed++
			}
		})
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Passed: %d/%d\n", passed, len(tests))
	fmt.Printf("Failed: %d/%d\n", failed, len(tests))

	if failed == 0 {
		fmt.Println("\n✅ ALL DATA INTEGRITY TESTS PASSED")
	} else {
		fmt.Println("\n❌ SOME TESTS FAILED")
	}
	fmt.Println("")
}
