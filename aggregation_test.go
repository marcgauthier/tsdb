package tsdb

import (
	"testing"
	"time"
)

func TestGetTestRangeAggregation(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	siteID := int64(101)
	testID := int64(5001)
	start := int64(3600 * 500000) // aligned to 1-hour windows

	// 24 points at 5-minute spacing -> 2 hourly buckets (12 points each)
	for i := 0; i < 12; i++ {
		engine.Add(start+int64(i*300), siteID, testID, int64(10+i))
	}
	for i := 0; i < 12; i++ {
		engine.Add(start+3600+int64(i*300), siteID, testID, int64(30+i))
	}

	time.Sleep(200 * time.Millisecond)

	results, err := engine.GetTestRange(testID, start, start+7200, Scale1h)
	if err != nil {
		t.Fatalf("GetTestRange failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 aggregated points, got %d", len(results))
	}

	if results[0].Timestamp != start || results[0].Value != 15 {
		t.Fatalf("hour 1 mismatch: got timestamp=%d value=%d", results[0].Timestamp, results[0].Value)
	}

	if results[1].Timestamp != start+3600 || results[1].Value != 35 {
		t.Fatalf("hour 2 mismatch: got timestamp=%d value=%d", results[1].Timestamp, results[1].Value)
	}
}

func TestGetSiteTestRangeAggregation(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	siteID := int64(101)
	testID := int64(5001)
	start := int64(3600 * 500001) // aligned to 1-hour windows

	for i := 0; i < 12; i++ {
		engine.Add(start+int64(i*300), siteID, testID, int64(50+i))
	}

	time.Sleep(200 * time.Millisecond)

	results, err := engine.GetSiteTestRange(siteID, testID, start, start+3600, Scale1h)
	if err != nil {
		t.Fatalf("GetSiteTestRange failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 aggregated point, got %d", len(results))
	}

	if results[0].Timestamp != start || results[0].Value != 55 {
		t.Fatalf("aggregated value mismatch: got timestamp=%d value=%d", results[0].Timestamp, results[0].Value)
	}
}

func TestGetSiteRangeAggregationPerTest(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	siteID := int64(101)
	testID1 := int64(5001)
	testID2 := int64(5002)
	start := int64(3600 * 500002) // aligned to 1-hour windows

	for i := 0; i < 12; i++ {
		ts := start + int64(i*300)
		engine.Add(ts, siteID, testID1, int64(i))     // avg = 5
		engine.Add(ts, siteID, testID2, int64(100+i)) // avg = 105
	}

	time.Sleep(200 * time.Millisecond)

	results, err := engine.GetSiteRange(siteID, start, start+3600, Scale1h)
	if err != nil {
		t.Fatalf("GetSiteRange failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 aggregated points, got %d", len(results))
	}

	if results[0].Timestamp != start || results[0].TestID != testID1 || results[0].Value != 5 {
		t.Fatalf("testID1 aggregate mismatch: got timestamp=%d testID=%d value=%d", results[0].Timestamp, results[0].TestID, results[0].Value)
	}

	if results[1].Timestamp != start || results[1].TestID != testID2 || results[1].Value != 105 {
		t.Fatalf("testID2 aggregate mismatch: got timestamp=%d testID=%d value=%d", results[1].Timestamp, results[1].TestID, results[1].Value)
	}
}

func TestRangeQueriesRejectInvalidScale(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	engine.Start()

	if _, err := engine.GetTestRange(5001, 0, 10, "bad"); err == nil {
		t.Fatal("expected GetTestRange to reject invalid scale")
	}

	if _, err := engine.GetSiteTestRange(101, 5001, 0, 10, "bad"); err == nil {
		t.Fatal("expected GetSiteTestRange to reject invalid scale")
	}

	if _, err := engine.GetSiteRange(101, 0, 10, "bad"); err == nil {
		t.Fatal("expected GetSiteRange to reject invalid scale")
	}
}
