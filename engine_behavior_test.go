package tsdb

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestEngineLifecycleStopIdempotentAndFlushesPipeline(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "lifecycle.db"),
		ChunkFlushInterval:   1 * time.Second,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     2000,
		CompactorWorkers:     2,
		EnableAutoCompaction: false,
	}

	before := runtime.NumGoroutine()
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()

	siteID := int64(101)
	testID := int64(5001)
	startTs := time.Now().Unix()
	total := 200
	for i := 0; i < total; i++ {
		engine.Add(startTs+int64(i), siteID, testID, int64(i))
	}

	stopErr1 := engine.Stop()
	stopErr2 := engine.Stop()
	if (stopErr1 == nil) != (stopErr2 == nil) {
		t.Fatalf("stop idempotency mismatch: first=%v second=%v", stopErr1, stopErr2)
	}
	if stopErr1 != nil && stopErr2 != nil && stopErr1.Error() != stopErr2.Error() {
		t.Fatalf("stop returned different errors across calls: %v vs %v", stopErr1, stopErr2)
	}

	time.Sleep(300 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+6 {
		t.Fatalf("possible goroutine leak: before=%d after=%d", before, after)
	}

	reopen, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer reopen.Stop()

	results, err := reopen.GetTestRange(testID, startTs, startTs+int64(total)-1, Scale5m)
	if err != nil {
		t.Fatalf("reopen query failed: %v", err)
	}
	if len(results) != total {
		t.Fatalf("expected %d flushed points after stop, got %d", total, len(results))
	}
}

func TestOutOfOrderDelayedPointOverwritesWindowChunk(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "late-point.db"),
		ChunkFlushInterval:   2 * time.Second,
		IntervalResolution:   1 * time.Second,
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

	siteID := int64(101)
	testID := int64(5001)
	base := time.Now().Unix() - 120
	if base%2 != 0 {
		base--
	}

	engine.Add(base, siteID, testID, 10)
	engine.Add(base+1, siteID, testID, 20)
	time.Sleep(3 * time.Second)

	initial, err := engine.GetTestRange(testID, base, base+1, Scale5m)
	if err != nil {
		t.Fatalf("initial query failed: %v", err)
	}
	if len(initial) != 2 || initial[0].Value != 10 || initial[1].Value != 20 {
		t.Fatalf("unexpected initial points: %#v", initial)
	}

	engine.Add(base+1, siteID, testID, 99) // delayed point for an already flushed window
	time.Sleep(3 * time.Second)

	after, err := engine.GetTestRange(testID, base, base+1, Scale5m)
	if err != nil {
		t.Fatalf("post-delay query failed: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("expected 2 points after delayed overwrite, got %d", len(after))
	}
	for _, pt := range after {
		if pt.Value != 99 {
			t.Fatalf("expected delayed overwrite to produce value 99 for full window, got %#v", after)
		}
	}
}

func TestIntraSlotLastWriteWins(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "last-write-wins.db"),
		ChunkFlushInterval:   1 * time.Second,
		IntervalResolution:   1 * time.Second,
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

	siteID := int64(101)
	testID := int64(5001)
	ts := time.Now().Unix() - 30

	engine.Add(ts, siteID, testID, 11)
	engine.Add(ts, siteID, testID, 77)
	time.Sleep(2 * time.Second)

	results, err := engine.GetTestRange(testID, ts, ts, Scale5m)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 point for slot, got %d", len(results))
	}
	if results[0].Value != 77 {
		t.Fatalf("expected last write to win (77), got %d", results[0].Value)
	}
}

func TestNewEngineAppliesDefaultsForMissingAndNegativeValues(t *testing.T) {
	cfg := Config{
		DBPath:             filepath.Join(t.TempDir(), "defaults.db"),
		ChunkFlushInterval: -1,
		IntervalResolution: -1,
		IngestBufferSize:   -1,
		CompactorWorkers:   -1,
		CompactionInterval: -1,
		RetentionInterval:  -1,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Stop()

	if engine.config.ChunkFlushInterval != 5*time.Minute {
		t.Fatalf("expected default ChunkFlushInterval 5m, got %v", engine.config.ChunkFlushInterval)
	}
	if engine.config.IntervalResolution != 5*time.Minute {
		t.Fatalf("expected default IntervalResolution 5m, got %v", engine.config.IntervalResolution)
	}
	if engine.config.IngestBufferSize != 10000 {
		t.Fatalf("expected default IngestBufferSize 10000, got %d", engine.config.IngestBufferSize)
	}
	if engine.config.CompactorWorkers != 1 {
		t.Fatalf("expected default CompactorWorkers 1, got %d", engine.config.CompactorWorkers)
	}
	if engine.config.CompactionInterval != 1*time.Minute {
		t.Fatalf("expected default CompactionInterval 1m, got %v", engine.config.CompactionInterval)
	}
	if engine.config.RetentionInterval != 10*time.Minute {
		t.Fatalf("expected default RetentionInterval 10m, got %v", engine.config.RetentionInterval)
	}
	if engine.config.Retention5m != 3*24*time.Hour {
		t.Fatalf("expected default Retention5m 3d, got %v", engine.config.Retention5m)
	}
	if engine.config.Retention1h != 30*24*time.Hour {
		t.Fatalf("expected default Retention1h 30d, got %v", engine.config.Retention1h)
	}
	if engine.config.Retention4h != 90*24*time.Hour {
		t.Fatalf("expected default Retention4h 90d, got %v", engine.config.Retention4h)
	}
	if engine.config.Retention24h != 365*24*time.Hour {
		t.Fatalf("expected default Retention24h 365d, got %v", engine.config.Retention24h)
	}
	if len(engine.config.CompactionTiers) != 4 {
		t.Fatalf("expected 4 default tiers, got %d", len(engine.config.CompactionTiers))
	}
	if engine.config.CompactionTiers[0].Name != Scale5m ||
		engine.config.CompactionTiers[1].Name != Scale1h ||
		engine.config.CompactionTiers[2].Name != Scale4h ||
		engine.config.CompactionTiers[3].Name != Scale24h {
		t.Fatalf("unexpected default tier names: %+v", engine.config.CompactionTiers)
	}
}

func TestNewEngineReturnsErrorForInvalidDBPath(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create path fixture: %v", err)
	}

	_, err := NewEngine(Config{DBPath: filePath})
	if err == nil {
		t.Fatal("expected NewEngine to fail for DBPath that is a file")
	}
}

func TestPendingBucketsTTLVisibilityGap(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "pending-ttl.db"),
		ChunkFlushInterval:   200 * time.Millisecond,
		IntervalResolution:   1 * time.Second,
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

	ts := time.Now().Unix()
	testID := int64(5001)
	engine.Add(ts, 101, testID, 42)

	deadline := time.Now().Add(1500 * time.Millisecond)
	visible := false
	for time.Now().Before(deadline) {
		if len(engine.getUnflushedPoints(testID, ts, ts)) > 0 {
			visible = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !visible {
		t.Fatal("expected point to be visible from pending buckets after flush")
	}

	time.Sleep(2500 * time.Millisecond)
	if n := len(engine.getUnflushedPoints(testID, ts, ts)); n != 0 {
		t.Fatalf("expected pending point to expire from memory, still found %d", n)
	}
}

func TestDatabaseClosedOperationsFailGracefully(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath: filepath.Join(tmpDir, "db-closed-ops.db"),
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	_ = engine.db.Close()

	_, err = engine.GetTestRange(5001, 0, 10, Scale5m)
	if err == nil {
		t.Fatal("expected query on closed DB to return an error")
	}

	// These should not panic even if the DB is closed.
	engine.applyRetentionPolicies()
	engine.performCompaction()

	// Stop must return promptly and be idempotent even after manual DB close.
	_ = engine.Stop()
	_ = engine.Stop()
}

func TestCompactionPreservesSourceTierData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "preserve-source.db"),
		ChunkFlushInterval:   1 * time.Second,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     1000,
		CompactorWorkers:     1,
		CompactionInterval:   1 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	defer engine.Stop()

	siteID := int64(101)
	testID := int64(5001)
	base := time.Now().Add(-2 * time.Hour).Unix()
	for i := 0; i < 12; i++ {
		engine.Add(base+int64(i*300), siteID, testID, int64(100+i))
	}
	time.Sleep(2 * time.Second)

	rawBefore, err := engine.GetSiteTestRange(siteID, testID, base, base+3600, Scale5m)
	if err != nil {
		t.Fatalf("raw query before compaction failed: %v", err)
	}
	if len(rawBefore) == 0 {
		t.Fatal("expected raw data before compaction")
	}

	engine.performCompaction()

	rawAfter, err := engine.GetSiteTestRange(siteID, testID, base, base+3600, Scale5m)
	if err != nil {
		t.Fatalf("raw query after compaction failed: %v", err)
	}
	if len(rawAfter) != len(rawBefore) {
		t.Fatalf("raw data count changed after compaction: before=%d after=%d", len(rawBefore), len(rawAfter))
	}
	for i := range rawBefore {
		if rawBefore[i].Timestamp != rawAfter[i].Timestamp || rawBefore[i].Value != rawAfter[i].Value {
			t.Fatalf("raw data changed at index %d: before=%+v after=%+v", i, rawBefore[i], rawAfter[i])
		}
	}

	var count5m, count1h int
	err = engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix5m := []byte("test-5m!")
		for it.Seek(prefix5m); it.ValidForPrefix(prefix5m); it.Next() {
			count5m++
		}

		prefix1h := []byte("test-1h!")
		for it.Seek(prefix1h); it.ValidForPrefix(prefix1h); it.Next() {
			count1h++
		}

		return nil
	})
	if err != nil {
		t.Fatalf("failed scanning keys: %v", err)
	}
	if count5m == 0 {
		t.Fatal("expected source tier (5m) keys to remain after compaction")
	}
	if count1h == 0 {
		t.Fatal("expected target tier (1h) keys to be generated after compaction")
	}
}

func TestIdleEngineDoesNotCreateEmptyChunks(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "idle.db"),
		ChunkFlushInterval:   100 * time.Millisecond,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     1000,
		CompactorWorkers:     1,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	time.Sleep(700 * time.Millisecond)
	if err := engine.Stop(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("unexpected stop error: %v", err)
	}

	reopen, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer reopen.Stop()

	keyCount := 0
	err = reopen.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keyCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed counting keys: %v", err)
	}
	if keyCount != 0 {
		t.Fatalf("expected 0 keys for idle engine, found %d", keyCount)
	}
}

func TestQueryExactBoundaryAndOffByOne(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "boundary.db"),
		ChunkFlushInterval:   1 * time.Second,
		IntervalResolution:   1 * time.Second,
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

	siteID := int64(101)
	testID := int64(5001)
	t1 := time.Now().Unix() - 200
	t2 := t1 + 5
	t3 := t1 + 10

	engine.Add(t1, siteID, testID, 100)
	engine.Add(t2, siteID, testID, 200)
	engine.Add(t3, siteID, testID, 300)
	time.Sleep(2 * time.Second)

	exactStart, err := engine.GetTestRange(testID, t1, t1, Scale5m)
	if err != nil {
		t.Fatalf("exact start query failed: %v", err)
	}
	if len(exactStart) != 1 || exactStart[0].Timestamp != t1 {
		t.Fatalf("expected exact start boundary to include point at %d, got %#v", t1, exactStart)
	}

	exactEnd, err := engine.GetTestRange(testID, t3, t3, Scale5m)
	if err != nil {
		t.Fatalf("exact end query failed: %v", err)
	}
	if len(exactEnd) != 1 || exactEnd[0].Timestamp != t3 {
		t.Fatalf("expected exact end boundary to include point at %d, got %#v", t3, exactEnd)
	}

	between, err := engine.GetTestRange(testID, t1+1, t2-1, Scale5m)
	if err != nil {
		t.Fatalf("between query failed: %v", err)
	}
	if len(between) != 0 {
		t.Fatalf("expected no points for gap boundaries, got %#v", between)
	}

	full, err := engine.GetTestRange(testID, t1, t3, Scale5m)
	if err != nil {
		t.Fatalf("full query failed: %v", err)
	}
	if len(full) != 3 {
		t.Fatalf("expected 3 boundary-inclusive points, got %d", len(full))
	}
}
