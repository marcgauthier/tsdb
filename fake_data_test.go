package tsdb

import (
	"testing"
	"time"
)

func TestGenerateFakeDataRange_CountAndCoverage(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()
	engine.Start()

	siteIDs := []int64{100, 101}
	probeIDs := []ProbeWithSite{
		{ProbeID: 5001, SiteID: 100},
		{ProbeID: 5002, SiteID: 101},
	}

	start := int64(10_000)
	end := int64(10_000 + 6*3600) // 6 hours
	interval := 30 * time.Minute

	written, err := engine.GenerateFakeDataRange(siteIDs, probeIDs, start, end, interval)
	if err != nil {
		t.Fatalf("GenerateFakeDataRange failed: %v", err)
	}

	steps := int((end-start)/int64(interval/time.Second)) + 1
	expectedWrites := steps * len(probeIDs)
	if written != expectedWrites {
		t.Fatalf("Expected %d writes, got %d", expectedWrites, written)
	}

	for _, p := range probeIDs {
		results, err := engine.GetSiteTestRange(p.SiteID, p.ProbeID, start, end)
		if err != nil {
			t.Fatalf("Query failed for site=%d probe=%d: %v", p.SiteID, p.ProbeID, err)
		}
		if len(results) != steps {
			t.Fatalf("Expected %d points for site=%d probe=%d, got %d", steps, p.SiteID, p.ProbeID, len(results))
		}
		if results[0].Timestamp != start {
			t.Fatalf("First timestamp mismatch: expected %d got %d", start, results[0].Timestamp)
		}
		if results[len(results)-1].Timestamp != end {
			t.Fatalf("Last timestamp mismatch: expected %d got %d", end, results[len(results)-1].Timestamp)
		}
	}
}

func TestGenerateFakeDataRange_Validation(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()
	engine.Start()

	_, err := engine.GenerateFakeDataRange(nil, nil, 100, 200, 5*time.Minute)
	if err == nil {
		t.Fatal("Expected error for empty probeIDs")
	}

	_, err = engine.GenerateFakeDataRange([]int64{1}, []ProbeWithSite{{ProbeID: 2, SiteID: 1}}, 200, 100, 5*time.Minute)
	if err == nil {
		t.Fatal("Expected error for invalid time range")
	}

	_, err = engine.GenerateFakeDataRange([]int64{1}, []ProbeWithSite{{ProbeID: 2, SiteID: 1}}, 100, 200, 500*time.Millisecond)
	if err == nil {
		t.Fatal("Expected error for sub-second interval")
	}

	_, err = engine.GenerateFakeDataRange([]int64{999}, []ProbeWithSite{{ProbeID: 2, SiteID: 1}}, 100, 200, 5*time.Minute)
	if err == nil {
		t.Fatal("Expected error when no probes match siteIDs")
	}
}
