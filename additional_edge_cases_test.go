package tsdb

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestGetTestRangeSameTimestampDifferentSitesCollision(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "same-ts-sites.db"),
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

	testID := int64(5001)
	ts := time.Now().Unix() - 200

	engine.Add(ts, 101, testID, 11)
	engine.Add(ts, 102, testID, 22)
	time.Sleep(4 * time.Second) // Ensure pending buckets are expired and disk state is authoritative.

	allSites, err := engine.GetTestRange(testID, ts, ts, Scale5m)
	if err != nil {
		t.Fatalf("GetTestRange failed: %v", err)
	}
	if len(allSites) != 1 {
		t.Fatalf("expected 1 point due test-key collision, got %d", len(allSites))
	}
	if !((allSites[0].SiteID == 101 && allSites[0].Value == 11) || (allSites[0].SiteID == 102 && allSites[0].Value == 22)) {
		t.Fatalf("expected one winner from colliding sites (101/11 or 102/22), got site=%d value=%d", allSites[0].SiteID, allSites[0].Value)
	}

	site101, err := engine.GetSiteTestRange(101, testID, ts, ts, Scale5m)
	if err != nil {
		t.Fatalf("site 101 query failed: %v", err)
	}
	if len(site101) != 1 || site101[0].Value != 11 {
		t.Fatalf("expected site 101 data preserved in site index, got %#v", site101)
	}

	site102, err := engine.GetSiteTestRange(102, testID, ts, ts, Scale5m)
	if err != nil {
		t.Fatalf("site 102 query failed: %v", err)
	}
	if len(site102) != 1 || site102[0].Value != 22 {
		t.Fatalf("expected site 102 data preserved in site index, got %#v", site102)
	}
}

func TestStopReturnsWhenDBClosedDuringWriterActivity(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "writer-close.db"),
		ChunkFlushInterval:   100 * time.Millisecond,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     20000,
		CompactorWorkers:     2,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()

	now := time.Now().Unix()
	for i := 0; i < 3000; i++ {
		engine.Add(now+int64(i), 101, 5001, int64(i))
	}
	time.Sleep(150 * time.Millisecond)

	_ = engine.db.Close() // Force write failures in writer loop.

	done := make(chan error, 1)
	go func() {
		done <- engine.Stop()
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Stop timed out after DB close during writer activity")
	case <-done:
	}
}

func TestCompactionRepeatedPassesRemainIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "idempotent-compaction.db"),
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
	start := floorToWindow(time.Now().Add(-3*time.Hour).Unix(), 3600)

	for i := 0; i < 12; i++ {
		engine.Add(start+int64(i*300), siteID, testID, int64(10+i)) // avg 15
	}
	for i := 0; i < 12; i++ {
		engine.Add(start+3600+int64(i*300), siteID, testID, int64(30+i)) // avg 35
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < 3; i++ {
		engine.performCompaction()
	}

	rolled, err := engine.GetSiteTestRange(siteID, testID, start, start+7200, Scale1h)
	if err != nil {
		t.Fatalf("scale 1h query failed: %v", err)
	}
	if len(rolled) != 2 {
		t.Fatalf("expected 2 rolled points after repeated compactions, got %d", len(rolled))
	}
	if rolled[0].Value != 15 || rolled[1].Value != 35 {
		t.Fatalf("unexpected rolled values after repeated compactions: %#v", rolled)
	}

	prefix := []byte(fmt.Sprintf("test-%s!%016x!", Scale1h, testID))
	keyCount := 0
	err = engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed counting 1h keys: %v", err)
	}
	if keyCount != 2 {
		t.Fatalf("expected exactly 2 1h keys (idempotent overwrite), got %d", keyCount)
	}
}

func TestAggregationExtremeValuesPrecision(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "extreme-values.db"),
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
	start := floorToWindow(time.Now().Add(-2*time.Hour).Unix(), 3600)
	maxInt := int64(^uint64(0) >> 1)
	minInt := -maxInt - 1

	engine.Add(start, siteID, testID, maxInt)
	engine.Add(start+300, siteID, testID, minInt+1) // sum = 0, avg = 0
	time.Sleep(2 * time.Second)

	rolled, err := engine.GetSiteTestRange(siteID, testID, start, start+3600, Scale1h)
	if err != nil {
		t.Fatalf("scale 1h query failed: %v", err)
	}
	if len(rolled) != 1 {
		t.Fatalf("expected 1 rolled point, got %d", len(rolled))
	}
	if rolled[0].Value != 0 {
		t.Fatalf("expected precise mean 0 for extreme pair, got %d", rolled[0].Value)
	}
}

func TestScaleFallbackCoverageAcrossAPIs(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "scale-fallback.db"),
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
	testID1 := int64(5001)
	testID2 := int64(5002)
	start := floorToWindow(time.Now().Add(-2*time.Hour).Unix(), 3600)

	engine.Add(start, siteID, testID1, 10)
	engine.Add(start+300, siteID, testID1, 20) // avg 15
	engine.Add(start, siteID, testID2, 30)
	engine.Add(start+300, siteID, testID2, 50) // avg 40
	time.Sleep(2 * time.Second)

	// No 1h keys were generated (auto compaction off); queries must fallback to 5m aggregation.
	testPrefix1h := []byte(fmt.Sprintf("test-%s!", Scale1h))
	count1h := 0
	_ = engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(testPrefix1h); it.ValidForPrefix(testPrefix1h); it.Next() {
			count1h++
		}
		return nil
	})
	if count1h != 0 {
		t.Fatalf("expected no persisted 1h keys before fallback test, found %d", count1h)
	}

	defScale, err := engine.GetTestRange(testID1, start, start+3600, "")
	if err != nil {
		t.Fatalf("default-scale query failed: %v", err)
	}
	expRaw, err := engine.GetTestRange(testID1, start, start+3600, Scale5m)
	if err != nil {
		t.Fatalf("5m query failed: %v", err)
	}
	if len(defScale) != len(expRaw) {
		t.Fatalf("empty-scale default should behave as 5m: %d vs %d", len(defScale), len(expRaw))
	}

	testRange1h, err := engine.GetTestRange(testID1, start, start+3600, Scale1h)
	if err != nil {
		t.Fatalf("GetTestRange 1h fallback failed: %v", err)
	}
	if len(testRange1h) != 1 || testRange1h[0].Value != 15 {
		t.Fatalf("unexpected test fallback aggregation: %#v", testRange1h)
	}

	siteTest1h, err := engine.GetSiteTestRange(siteID, testID1, start, start+3600, Scale1h)
	if err != nil {
		t.Fatalf("GetSiteTestRange 1h fallback failed: %v", err)
	}
	if len(siteTest1h) != 1 || siteTest1h[0].Value != 15 {
		t.Fatalf("unexpected site-test fallback aggregation: %#v", siteTest1h)
	}

	siteRange1h, err := engine.GetSiteRange(siteID, start, start+3600, Scale1h)
	if err != nil {
		t.Fatalf("GetSiteRange 1h fallback failed: %v", err)
	}
	if len(siteRange1h) != 2 {
		t.Fatalf("expected 2 per-test rollups from site fallback, got %d", len(siteRange1h))
	}
	sort.Slice(siteRange1h, func(i, j int) bool { return siteRange1h[i].TestID < siteRange1h[j].TestID })
	if siteRange1h[0].TestID != testID1 || siteRange1h[0].Value != 15 ||
		siteRange1h[1].TestID != testID2 || siteRange1h[1].Value != 40 {
		t.Fatalf("unexpected site fallback values: %#v", siteRange1h)
	}
}

func TestFakeDataWrappersAreQueryable(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "fake-wrappers.db"),
		ChunkFlushInterval:   1 * time.Second,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     10000,
		CompactorWorkers:     1,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	defer engine.Stop()

	series := []SiteProbe{
		{SiteID: 100, ProbeID: 5001},
		{SiteID: 101, ProbeID: 5002},
	}
	probes := []ProbeWithSite{
		{SiteID: 100, ProbeID: 5001},
		{SiteID: 101, ProbeID: 5002},
	}

	w1, err := engine.GenerateFakeDataForSeries(series, 1, 24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateFakeDataForSeries failed: %v", err)
	}
	if w1 == 0 {
		t.Fatal("expected GenerateFakeDataForSeries to write points")
	}

	start := time.Now().Add(-2 * time.Hour).Unix()
	end := time.Now().Unix()
	w2, err := engine.GenerateFakeDataRangeForSeries(series, start, end, time.Hour)
	if err != nil {
		t.Fatalf("GenerateFakeDataRangeForSeries failed: %v", err)
	}
	if w2 == 0 {
		t.Fatal("expected GenerateFakeDataRangeForSeries to write points")
	}

	w3, err := engine.GenerateFakeData([]int64{100, 101}, probes, 1, 24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateFakeData failed: %v", err)
	}
	if w3 == 0 {
		t.Fatal("expected GenerateFakeData to write points")
	}

	time.Sleep(2 * time.Second)

	results, err := engine.GetSiteTestRange(100, 5001, start-24*3600, end, Scale5m)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected wrapper-generated data to be queryable")
	}
}

func TestConcurrentAddAndQueryUnderCompactionAndRetention(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:               filepath.Join(tmpDir, "concurrency-stress.db"),
		ChunkFlushInterval:   100 * time.Millisecond,
		IntervalResolution:   1 * time.Second,
		IngestBufferSize:     50000,
		CompactorWorkers:     2,
		CompactionInterval:   100 * time.Millisecond,
		EnableAutoCompaction: true,
		Retention5m:          365 * 24 * time.Hour,
		Retention1h:          365 * 24 * time.Hour,
		Retention4h:          365 * 24 * time.Hour,
		Retention24h:         365 * 24 * time.Hour,
		RetentionInterval:    100 * time.Millisecond,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()

	start := time.Now().Unix()
	done := make(chan struct{})
	writerDone := make(chan struct{})

	for w := 0; w < 4; w++ {
		go func(worker int) {
			siteID := int64(100 + worker)
			testID := int64(5000 + worker)
			for i := 0; i < 1500; i++ {
				engine.Add(start+int64(i), siteID, testID, int64(i+worker))
			}
			writerDone <- struct{}{}
		}(w)
	}

	queryDone := make(chan struct{})
	for q := 0; q < 4; q++ {
		go func() {
			for {
				select {
				case <-done:
					queryDone <- struct{}{}
					return
				default:
					_, _ = engine.GetTestRange(5001, start, start+2000, Scale5m)
					_, _ = engine.GetSiteTestRange(101, 5001, start, start+2000, Scale5m)
					_, _ = engine.GetSiteRange(101, start, start+2000, Scale1h)
				}
			}
		}()
	}

	for i := 0; i < 4; i++ {
		<-writerDone
	}
	close(done)
	for i := 0; i < 4; i++ {
		<-queryDone
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- engine.Stop()
	}()

	select {
	case <-time.After(6 * time.Second):
		t.Fatal("Stop timed out under concurrent load")
	case <-stopDone:
	}
}
