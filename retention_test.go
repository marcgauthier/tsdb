package tsdb

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func writeSingleChunkForScale(t *testing.T, e *Engine, scale string, siteID, testID, ts, val int64) {
	t.Helper()

	d, ok := scaleDurations[scale]
	if !ok {
		t.Fatalf("unknown scale %s", scale)
	}
	intervalSec := int64(d / time.Second)
	if intervalSec <= 0 {
		intervalSec = 1
	}

	chunk := CompressedChunk{
		SiteID:          siteID,
		TestID:          testID,
		StartTime:       ts,
		EndTime:         ts + intervalSec,
		Interval:        d,
		Count:           1,
		FirstVal:        val,
		LastVal:         val,
		CompressionType: CompressionSnappy,
		Data:            encodeValuesOptimized([]int64{val}, val),
	}
	encoded := serializeChunk(chunk)

	err := e.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(makeTestKey(testID, ts, scale), encoded); err != nil {
			return err
		}
		if err := txn.Set(makeSiteKey(siteID, testID, ts, scale), encoded); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed writing chunk fixture: %v", err)
	}
}

func writeTestChunkOnly(t *testing.T, e *Engine, scale string, siteID, testID, ts, val int64) {
	t.Helper()

	d, ok := scaleDurations[scale]
	if !ok {
		t.Fatalf("unknown scale %s", scale)
	}
	intervalSec := int64(d / time.Second)
	if intervalSec <= 0 {
		intervalSec = 1
	}
	chunk := CompressedChunk{
		SiteID:          siteID,
		TestID:          testID,
		StartTime:       ts,
		EndTime:         ts + intervalSec,
		Interval:        d,
		Count:           1,
		FirstVal:        val,
		LastVal:         val,
		CompressionType: CompressionSnappy,
		Data:            encodeValuesOptimized([]int64{val}, val),
	}
	encoded := serializeChunk(chunk)

	err := e.db.Update(func(txn *badger.Txn) error {
		return txn.Set(makeTestKey(testID, ts, scale), encoded)
	})
	if err != nil {
		t.Fatalf("failed writing test-only fixture: %v", err)
	}
}

func writeSiteChunkOnly(t *testing.T, e *Engine, scale string, siteID, testID, ts, val int64) {
	t.Helper()

	d, ok := scaleDurations[scale]
	if !ok {
		t.Fatalf("unknown scale %s", scale)
	}
	intervalSec := int64(d / time.Second)
	if intervalSec <= 0 {
		intervalSec = 1
	}
	chunk := CompressedChunk{
		SiteID:          siteID,
		TestID:          testID,
		StartTime:       ts,
		EndTime:         ts + intervalSec,
		Interval:        d,
		Count:           1,
		FirstVal:        val,
		LastVal:         val,
		CompressionType: CompressionSnappy,
		Data:            encodeValuesOptimized([]int64{val}, val),
	}
	encoded := serializeChunk(chunk)

	err := e.db.Update(func(txn *badger.Txn) error {
		return txn.Set(makeSiteKey(siteID, testID, ts, scale), encoded)
	})
	if err != nil {
		t.Fatalf("failed writing site-only fixture: %v", err)
	}
}

func TestRetentionCleanupDeletesWholeExpiredChunk(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "retention.db"),
		ChunkFlushInterval:   1 * time.Second,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     1000,
		CompactorWorkers:     1,
		EnableAutoCompaction: false,
		Retention5m:          2 * time.Second,
		RetentionInterval:    200 * time.Millisecond,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	siteID := int64(101)
	testID := int64(5001)
	now := time.Now().Unix()
	oldTs := now - 100
	freshTs := now + 100

	engine.Add(oldTs, siteID, testID, 10)
	engine.Add(freshTs, siteID, testID, 20)

	// Wait for packer flush + writer flush.
	time.Sleep(2 * time.Second)

	engine.applyRetentionPolicies()

	oldTestKey := makeTestKey(testID, oldTs, Scale5m)
	freshTestKey := makeTestKey(testID, freshTs, Scale5m)
	oldSiteKey := makeSiteKey(siteID, testID, oldTs, Scale5m)
	freshSiteKey := makeSiteKey(siteID, testID, freshTs, Scale5m)

	err = engine.db.View(func(txn *badger.Txn) error {
		if _, getErr := txn.Get(oldTestKey); !errors.Is(getErr, badger.ErrKeyNotFound) {
			return errors.New("old test chunk key was not deleted")
		}
		if _, getErr := txn.Get(oldSiteKey); !errors.Is(getErr, badger.ErrKeyNotFound) {
			return errors.New("old site chunk key was not deleted")
		}

		if _, getErr := txn.Get(freshTestKey); getErr != nil {
			return errors.New("fresh test chunk key was deleted unexpectedly")
		}
		if _, getErr := txn.Get(freshSiteKey); getErr != nil {
			return errors.New("fresh site chunk key was deleted unexpectedly")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetentionBoundaryCutoffBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewEngine(Config{
		DBPath: filepath.Join(tmpDir, "retention-boundary.db"),
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Stop()

	siteID := int64(101)
	testID := int64(5001)
	cutoff := time.Now().Unix() - 10

	olderTs := cutoff - 1
	exactTs := cutoff
	newerTs := cutoff + 1

	writeSingleChunkForScale(t, engine, Scale5m, siteID, testID, olderTs, 11)
	writeSingleChunkForScale(t, engine, Scale5m, siteID, testID, exactTs, 22)
	writeSingleChunkForScale(t, engine, Scale5m, siteID, testID, newerTs, 33)

	engine.deleteExpiredChunksBefore(Scale5m, cutoff)

	err = engine.db.View(func(txn *badger.Txn) error {
		if _, getErr := txn.Get(makeTestKey(testID, olderTs, Scale5m)); !errors.Is(getErr, badger.ErrKeyNotFound) {
			return errors.New("expected older test chunk to be deleted")
		}
		if _, getErr := txn.Get(makeSiteKey(siteID, testID, olderTs, Scale5m)); !errors.Is(getErr, badger.ErrKeyNotFound) {
			return errors.New("expected older site chunk to be deleted")
		}

		if _, getErr := txn.Get(makeTestKey(testID, exactTs, Scale5m)); getErr != nil {
			return errors.New("expected exact-cutoff test chunk to be kept")
		}
		if _, getErr := txn.Get(makeSiteKey(siteID, testID, exactTs, Scale5m)); getErr != nil {
			return errors.New("expected exact-cutoff site chunk to be kept")
		}

		if _, getErr := txn.Get(makeTestKey(testID, newerTs, Scale5m)); getErr != nil {
			return errors.New("expected newer test chunk to be kept")
		}
		if _, getErr := txn.Get(makeSiteKey(siteID, testID, newerTs, Scale5m)); getErr != nil {
			return errors.New("expected newer site chunk to be kept")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetentionZeroMeansKeepForever(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewEngine(Config{
		DBPath: filepath.Join(tmpDir, "retention-zero.db"),
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Stop()

	// Explicitly disable retention at runtime for this scale.
	engine.config.Retention5m = 0
	engine.config.Retention1h = 0
	engine.config.Retention4h = 0
	engine.config.Retention24h = 0

	siteID := int64(101)
	testID := int64(5001)
	oldTs := time.Now().Unix() - 100000
	writeSingleChunkForScale(t, engine, Scale5m, siteID, testID, oldTs, 99)

	engine.applyRetentionPolicies()

	err = engine.db.View(func(txn *badger.Txn) error {
		if _, getErr := txn.Get(makeTestKey(testID, oldTs, Scale5m)); getErr != nil {
			return errors.New("retention=0 should keep old test chunk")
		}
		if _, getErr := txn.Get(makeSiteKey(siteID, testID, oldTs, Scale5m)); getErr != nil {
			return errors.New("retention=0 should keep old site chunk")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetentionHandlesOrphanTestKey(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewEngine(Config{
		DBPath: filepath.Join(tmpDir, "retention-orphan-test.db"),
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Stop()

	siteID := int64(101)
	testID := int64(5001)
	oldTs := time.Now().Unix() - 100
	cutoff := time.Now().Unix() - 10

	writeTestChunkOnly(t, engine, Scale5m, siteID, testID, oldTs, 12)
	engine.deleteExpiredChunksBefore(Scale5m, cutoff)

	err = engine.db.View(func(txn *badger.Txn) error {
		if _, getErr := txn.Get(makeTestKey(testID, oldTs, Scale5m)); !errors.Is(getErr, badger.ErrKeyNotFound) {
			return errors.New("orphan test key should be deleted by retention scan")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetentionLeavesSiteOnlyOrphanUntouched(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewEngine(Config{
		DBPath: filepath.Join(tmpDir, "retention-orphan-site.db"),
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Stop()

	siteID := int64(101)
	testID := int64(5001)
	oldTs := time.Now().Unix() - 100
	cutoff := time.Now().Unix() - 10

	writeSiteChunkOnly(t, engine, Scale5m, siteID, testID, oldTs, 34)
	engine.deleteExpiredChunksBefore(Scale5m, cutoff)

	err = engine.db.View(func(txn *badger.Txn) error {
		if _, getErr := txn.Get(makeSiteKey(siteID, testID, oldTs, Scale5m)); getErr != nil {
			return errors.New("site-only orphan key should remain (not scanned by retention)")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
