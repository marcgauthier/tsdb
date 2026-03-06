package tsdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestGenerateFakeDataRange_CountAndCoverage(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "fake-count.db"),
		ChunkFlushInterval:   5 * time.Minute,
		IntervalResolution:   5 * time.Minute,
		IngestBufferSize:     1000,
		CompactorWorkers:     1,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	defer engine.Stop()

	siteIDs := []int64{100, 101}
	probeIDs := []ProbeWithSite{
		{ProbeID: 5001, SiteID: 100},
		{ProbeID: 5002, SiteID: 101},
	}

	start := int64(10_200)        // aligned to 5m
	end := int64(10_200 + 6*3600) // 6 hours
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
		results, err := engine.GetSiteTestRange(p.SiteID, p.ProbeID, start, end, Scale5m)
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

func TestGenerateFakeDataRange_GeneratesAllScales(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()
	engine.Start()

	siteIDs := []int64{100}
	probeIDs := []ProbeWithSite{
		{ProbeID: 5001, SiteID: 100},
	}

	// Use historical closed windows so 1h/4h/24h rollups are eligible.
	end := time.Now().Add(-48 * time.Hour).Unix()
	start := end - int64((5*24*time.Hour)/time.Second)

	written, err := engine.GenerateFakeDataRange(siteIDs, probeIDs, start, end, 5*time.Minute)
	if err != nil {
		t.Fatalf("GenerateFakeDataRange failed: %v", err)
	}
	if written == 0 {
		t.Fatal("expected fake data writes")
	}

	countScaleKeys := func(scale string) int {
		prefix := []byte(fmt.Sprintf("test-%s!%016x!", scale, probeIDs[0].ProbeID))
		count := 0
		_ = engine.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				count++
			}
			return nil
		})
		return count
	}

	if n := countScaleKeys(Scale5m); n == 0 {
		t.Fatal("expected 5m keys to exist")
	}
	if n := countScaleKeys(Scale1h); n == 0 {
		t.Fatal("expected 1h keys to be generated")
	}
	if n := countScaleKeys(Scale4h); n == 0 {
		t.Fatal("expected 4h keys to be generated")
	}
	if n := countScaleKeys(Scale24h); n == 0 {
		t.Fatal("expected 24h keys to be generated")
	}
}
