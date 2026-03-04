package tsdb

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRoundTripPersistedQueryReturnsWrittenPoints(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "roundtrip.db")

	cfg := Config{
		DBPath:             dbPath,
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 1 * time.Second,
		IngestBufferSize:   1000,
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	start := time.Now().Unix()
	siteID := int64(777)
	testID := int64(888)

	expected := map[int64]int64{
		start + 0: 101,
		start + 1: 99,
		start + 2: 1234,
		start + 3: -55,
		start + 4: 0,
	}

	{
		engine, err := NewEngine(cfg)
		if err != nil {
			t.Fatalf("Failed to create engine: %v", err)
		}

		engine.Start()
		for ts, value := range expected {
			engine.Add(ts, siteID, testID, value)
		}

		// Noise series to ensure query filtering stays strict.
		engine.Add(start+1, siteID, testID+1, 4444)
		engine.Add(start+1, siteID+1, testID, 5555)

		if err := engine.Stop(); err != nil {
			t.Fatalf("Failed to stop engine: %v", err)
		}
	}

	{
		engine, err := NewEngine(cfg)
		if err != nil {
			t.Fatalf("Failed to reopen engine: %v", err)
		}
		engine.Start()
		defer engine.Stop()

		results, err := engine.GetSiteTestRange(siteID, testID, start, start+4)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(results) != len(expected) {
			t.Fatalf("Expected %d points, got %d", len(expected), len(results))
		}

		got := make(map[int64]int64, len(results))
		for _, pt := range results {
			if pt.SiteID != siteID || pt.TestID != testID {
				t.Fatalf("Unexpected series in results: site=%d test=%d", pt.SiteID, pt.TestID)
			}
			got[pt.Timestamp] = pt.Value
		}

		for ts, want := range expected {
			gotVal, ok := got[ts]
			if !ok {
				t.Fatalf("Missing timestamp %d", ts)
			}
			if gotVal != want {
				t.Fatalf("Timestamp %d: expected %d, got %d", ts, want, gotVal)
			}
		}
	}
}
