package tsdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Helper function to create a test engine with cleanup
func setupTestEngine(t *testing.T) (*Engine, func()) {
	t.Helper()
	tmpDir := t.TempDir()

	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 1 * time.Second,        // Flush every second
		IntervalResolution: 100 * time.Millisecond, // 100ms intervals
		IngestBufferSize:   1000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 3 * time.Second},
			{Name: "10s", Duration: 10 * time.Second, MinAge: 20 * time.Second},
		},
		CompactionInterval:   1 * time.Second,
		EnableAutoCompaction: false, // Disable for predictable testing
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	cleanup := func() {
		engine.Stop()
		os.RemoveAll(tmpDir)
	}

	return engine, cleanup
}

// Test basic ingestion and recent visibility through range queries.
func TestBasicIngestion(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)
	testID := int64(5001)

	// Add some data points
	engine.Add(now, siteID, testID, 100)
	engine.Add(now+1, siteID, testID, 105)
	engine.Add(now+2, siteID, testID, 110)

	results, err := engine.GetSiteTestRange(siteID, testID, now, now+2, Scale5m)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("Expected data points to exist")
	}

	last := results[len(results)-1]
	if last.Value != 110 {
		t.Errorf("Expected latest value 110, got %d", last.Value)
	}
	if last.SiteID != siteID {
		t.Errorf("Expected siteID %d, got %d", siteID, last.SiteID)
	}
	if last.TestID != testID {
		t.Errorf("Expected testID %d, got %d", testID, last.TestID)
	}
}

// Test range query for all tests at a site.
func TestGetSiteRange_AllTestsVisible(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)

	// Add data for multiple tests
	engine.Add(now, siteID, 5001, 100)
	engine.Add(now, siteID, 5002, 200)
	engine.Add(now, siteID, 5003, 300)

	siteData, err := engine.GetSiteRange(siteID, now, now, Scale5m)
	if err != nil {
		t.Fatalf("site range query failed: %v", err)
	}
	if len(siteData) != 3 {
		t.Errorf("Expected 3 points, got %d", len(siteData))
	}

	got := map[int64]int64{}
	for _, pt := range siteData {
		got[pt.TestID] = pt.Value
	}
	if got[5001] != 100 {
		t.Errorf("Expected test 5001 value 100, got %d", got[5001])
	}
	if got[5002] != 200 {
		t.Errorf("Expected test 5002 value 200, got %d", got[5002])
	}
	if got[5003] != 300 {
		t.Errorf("Expected test 5003 value 300, got %d", got[5003])
	}
}

// Test forward-fill behavior
func TestForwardFill(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)
	testID := int64(5001)

	// Add sparse data points
	engine.Add(now, siteID, testID, 100)
	engine.Add(now+5, siteID, testID, 200) // Gap of 5 seconds

	// Wait for chunk to be flushed
	time.Sleep(2 * time.Second)

	// Query historical data
	results, err := engine.GetTestRange(testID, now, now+10, Scale5m)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results, got none")
	}

	// Check that gap was forward-filled
	foundForwardFill := false
	for _, pt := range results {
		if pt.Timestamp > now && pt.Timestamp < now+5 && pt.Value == 100 {
			foundForwardFill = true
			break
		}
	}

	if !foundForwardFill {
		t.Error("Expected forward-fill values in the gap")
	}
}

// Test compression and decompression
func TestCompressionDecompression(t *testing.T) {
	values := []int64{100, 100, 100, 105, 105, 110, 120, 135, 135, 135}
	firstVal := values[0]

	// Encode
	encoded := encodeValuesOptimized(values, firstVal)

	if len(encoded) == 0 {
		t.Fatal("Encoding produced no data")
	}

	// Decode
	decoded := decodeValuesOptimized(encoded, firstVal, len(values), CompressionSnappy)

	if len(decoded) != len(values) {
		t.Fatalf("Expected %d values, got %d", len(values), len(decoded))
	}

	// Verify values match
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

// Test RLE efficiency for stable values
func TestRLECompression(t *testing.T) {
	// Create array with lots of repeated values (good for RLE)
	stableValues := make([]int64, 100)
	for i := range stableValues {
		stableValues[i] = 100
	}

	stableEncoded := encodeValuesOptimized(stableValues, 100)

	// Create array with constantly changing values (bad for RLE)
	changingValues := make([]int64, 100)
	for i := range changingValues {
		changingValues[i] = int64(100 + i)
	}

	changingEncoded := encodeValuesOptimized(changingValues, 100)

	// RLE should compress stable values much better
	if len(stableEncoded) >= len(changingEncoded) {
		t.Errorf("Expected stable values (%d bytes) to compress better than changing values (%d bytes)",
			len(stableEncoded), len(changingEncoded))
	}

	t.Logf("Stable values: %d bytes, Changing values: %d bytes, Ratio: %.2fx",
		len(stableEncoded), len(changingEncoded), float64(len(changingEncoded))/float64(len(stableEncoded)))
}

// Test historical queries
func TestHistoricalQueries(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)
	testID := int64(5001)

	// Add data over a time range
	for i := int64(0); i < 10; i++ {
		engine.Add(now+i, siteID, testID, 100+i)
	}

	// Wait for flush
	time.Sleep(2 * time.Second)

	// Query a subset
	results, err := engine.GetTestRange(testID, now+3, now+7, Scale5m)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results from query")
	}

	// Verify all results are in range
	for _, pt := range results {
		if pt.Timestamp < now+3 || pt.Timestamp > now+7 {
			t.Errorf("Result timestamp %d outside query range [%d, %d]",
				pt.Timestamp, now+3, now+7)
		}
	}
}

// Test site-based queries
func TestSiteQueries(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)

	// Add data for multiple tests at same site
	for testID := int64(5001); testID <= 5003; testID++ {
		for i := int64(0); i < 5; i++ {
			engine.Add(now+i, siteID, testID, 100+i)
		}
	}

	// Wait for flush
	time.Sleep(2 * time.Second)

	// Query all tests at site
	results, err := engine.GetSiteRange(siteID, now, now+10, Scale5m)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results from site query")
	}

	// Verify all results are for correct site
	uniqueTests := make(map[int64]bool)
	for _, pt := range results {
		if pt.SiteID != siteID {
			t.Errorf("Result has wrong siteID: expected %d, got %d", siteID, pt.SiteID)
		}
		uniqueTests[pt.TestID] = true
	}

	if len(uniqueTests) != 3 {
		t.Errorf("Expected data from 3 tests, got %d", len(uniqueTests))
	}
}

// Test specific site+test query
func TestSiteTestQuery(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)
	targetTestID := int64(5002)

	// Add data for multiple tests
	for testID := int64(5001); testID <= 5003; testID++ {
		for i := int64(0); i < 5; i++ {
			engine.Add(now+i, siteID, testID, int64(testID)+i)
		}
	}

	time.Sleep(2 * time.Second)

	// Query specific test at site
	results, err := engine.GetSiteTestRange(siteID, targetTestID, now, now+10, Scale5m)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results")
	}

	// Verify all results match target
	for _, pt := range results {
		if pt.SiteID != siteID {
			t.Errorf("Wrong siteID: expected %d, got %d", siteID, pt.SiteID)
		}
		if pt.TestID != targetTestID {
			t.Errorf("Wrong testID: expected %d, got %d", targetTestID, pt.TestID)
		}
	}
}

// Test concurrent ingestion
func TestConcurrentIngestion(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()

	// Spawn multiple goroutines writing data
	done := make(chan bool, 10)
	for g := 0; g < 10; g++ {
		go func(goroutineID int) {
			siteID := int64(100 + goroutineID)
			testID := int64(5000 + goroutineID)

			for i := 0; i < 100; i++ {
				engine.Add(now+int64(i), siteID, testID, int64(i))
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify data for each goroutine
	for g := 0; g < 10; g++ {
		siteID := int64(100 + g)
		testID := int64(5000 + g)

		results, err := engine.GetSiteTestRange(siteID, testID, now, now+99, Scale5m)
		if err != nil {
			t.Errorf("Goroutine %d: query failed: %v", g, err)
			continue
		}
		if len(results) == 0 {
			t.Errorf("Goroutine %d: expected data to exist", g)
			continue
		}

		last := results[len(results)-1]
		if last.Value != 99 {
			t.Errorf("Goroutine %d: expected value 99, got %d", g, last.Value)
		}
	}
}

// Test multi-tier compaction
func TestMultiTierCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 100 * time.Millisecond,
		IntervalResolution: 10 * time.Millisecond,
		IngestBufferSize:   1000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "100ms", Duration: 100 * time.Millisecond, MinAge: 200 * time.Millisecond},
			{Name: "500ms", Duration: 500 * time.Millisecond, MinAge: 1 * time.Second},
		},
		CompactionInterval:   100 * time.Millisecond,
		EnableAutoCompaction: true,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	now := time.Now().Unix()
	siteID := int64(100)
	testID := int64(5001)

	// Add data
	for i := int64(0); i < 50; i++ {
		engine.Add(now+i, siteID, testID, 100+i)
	}

	// Wait for initial flush
	time.Sleep(500 * time.Millisecond)

	// Wait for tier 1 compaction (MinAge = 200ms)
	time.Sleep(500 * time.Millisecond)

	// Verify we can still query the data
	results, err := engine.GetTestRange(testID, now, now+50, Scale5m)
	if err != nil {
		t.Fatalf("Query after compaction failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results after compaction")
	}

	// Verify data integrity
	for _, pt := range results {
		if pt.TestID != testID {
			t.Errorf("TestID mismatch after compaction")
		}
	}
}

// Test smart tier selection
func TestSmartTierSelection(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "test.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   1000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1h", Duration: 1 * time.Hour, MinAge: 24 * time.Hour},
			{Name: "24h", Duration: 24 * time.Hour, MinAge: 7 * 24 * time.Hour},
			{Name: "7d", Duration: 7 * 24 * time.Hour, MinAge: 0},
		},
		CompactionInterval:   1 * time.Hour,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Stop()

	now := time.Now().Unix()

	// Query last 2 hours (should only select "1h" tier)
	tiers := engine.selectTiersForQuery(now-2*3600, now)
	if len(tiers) != 1 || tiers[0] != "1h" {
		t.Errorf("Expected only 1h tier for recent query, got %v", tiers)
	}

	// Query last 48 hours (should select "1h" and "24h" tiers)
	tiers = engine.selectTiersForQuery(now-48*3600, now)
	if len(tiers) < 2 {
		t.Errorf("Expected at least 2 tiers for 48h query, got %v", tiers)
	}

	// Query last 30 days (should select all tiers)
	tiers = engine.selectTiersForQuery(now-30*24*3600, now)
	if len(tiers) != 3 {
		t.Errorf("Expected all 3 tiers for 30-day query, got %v", tiers)
	}
}

// Test edge case: empty results
func TestEmptyResults(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	// Query non-existent data
	results, err := engine.GetTestRange(99999, 0, 100, Scale5m)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected empty results, got %d points", len(results))
	}

	siteResults, err := engine.GetSiteTestRange(99999, 99999, 0, 100, Scale5m)
	if err != nil {
		t.Fatalf("Site test query failed: %v", err)
	}
	if len(siteResults) != 0 {
		t.Errorf("Expected empty site/test results, got %d points", len(siteResults))
	}
}

// Test edge case: single data point
func TestSingleDataPoint(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	now := time.Now().Unix()
	siteID := int64(100)
	testID := int64(5001)

	engine.Add(now, siteID, testID, 42)

	results, err := engine.GetSiteTestRange(siteID, testID, now, now, Scale5m)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(results))
	}
	if results[0].Value != 42 {
		t.Errorf("Expected value 42, got %d", results[0].Value)
	}
}

// Benchmark ingestion throughput
func BenchmarkIngestion(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   100000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 2 * time.Second},
		},
		CompactionInterval:   1 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	now := time.Now().Unix()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Add(now+int64(i), 100, 5001, int64(i))
	}
}

// Benchmark query performance
func BenchmarkQuery(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   10000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 2 * time.Second},
		},
		CompactionInterval:   1 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	now := time.Now().Unix()

	// Populate with data
	for i := int64(0); i < 1000; i++ {
		engine.Add(now+i, 100, 5001, 100+i)
	}

	time.Sleep(2 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.GetTestRange(5001, now, now+1000, Scale5m)
	}
}

// Benchmark compression
func BenchmarkCompression(b *testing.B) {
	values := make([]int64, 360)
	for i := range values {
		values[i] = 100 + int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encodeValuesOptimized(values, values[0])
	}
}

// Benchmark decompression
func BenchmarkDecompression(b *testing.B) {
	values := make([]int64, 360)
	for i := range values {
		values[i] = 100 + int64(i)
	}

	encoded := encodeValuesOptimized(values, values[0])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = decodeValuesOptimized(encoded, values[0], len(values), CompressionSnappy)
	}
}
