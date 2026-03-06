package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/golang/snappy"
)

// ============================================================================
// Data Models
// ============================================================================

// DataPoint represents a single raw metric.
type DataPoint struct {
	Timestamp int64
	SiteID    int64
	TestID    int64
	Value     int64
}

// RawBucket represents an uncompressed collection of points over a time window.
type RawBucket struct {
	SiteID    int64
	TestID    int64
	StartTime int64
	Interval  time.Duration
	Values    []int64
	Filled    []bool
	Touched   []int
}

// CompressionType indicates the compression algorithm used
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionSnappy
)

const (
	Scale5m  = "5m"
	Scale1h  = "1h"
	Scale4h  = "4h"
	Scale24h = "24h"
)

var scaleDurations = map[string]time.Duration{
	Scale5m:  5 * time.Minute,
	Scale1h:  1 * time.Hour,
	Scale4h:  4 * time.Hour,
	Scale24h: 24 * time.Hour,
}

// CompressedChunk is the final payload written to BadgerDB.
type CompressedChunk struct {
	SiteID          int64
	TestID          int64
	StartTime       int64
	EndTime         int64
	Interval        time.Duration
	FirstVal        int64
	LastVal         int64
	Count           uint32 // Number of time slots in this chunk
	CompressionType CompressionType
	Data            []byte // Compressed delta-encoded values
}

// WritePayload pairs the Badger key with the compressed bytes.
type WritePayload struct {
	Key  []byte
	Data []byte
}

// CompactionTier defines a storage tier for chunk compaction
type CompactionTier struct {
	Name     string        // Tier name (e.g., "1h", "24h", "7d")
	Duration time.Duration // Chunk duration for this tier
	MinAge   time.Duration // Minimum age before compacting to next tier
}

// bucketKey uniquely identifies a time-aligned bucket
type bucketKey struct {
	siteID      int64
	testID      int64
	windowStart int64
}

type bucketAccumulator struct {
	siteID      int64
	testID      int64
	windowStart int64
	values      []int64
	filled      []bool
	touched     []int
}

type pendingBucket struct {
	bucket    *bucketAccumulator
	expiresAt time.Time
}

type bucketShard struct {
	mu      sync.RWMutex
	buckets map[bucketKey]*bucketAccumulator
}

type bucketWorkBuffer struct {
	workValues []int64
	workFilled []bool
}

// Config tunes the TSDB engine.
type Config struct {
	DBPath                string          // Path to BadgerDB directory
	BadgerOptions         *badger.Options // Optional BadgerDB options (nil = defaults)
	ChunkFlushInterval    time.Duration   // How often to flush ingest buckets (default: 5m)
	IntervalResolution    time.Duration   // Expected time between data points (default: 5m)
	IngestBufferSize      int
	CompactorWorkers      int
	CompactionTiers       []CompactionTier // Optional/legacy tier metadata; defaults are set if empty
	CompactionInterval    time.Duration    // How often to check for chunks to compact
	EnableAutoCompaction  bool             // Enable automatic background compaction
	Retention5m           time.Duration    // Retention for 5m scale (default: 3d)
	Retention1h           time.Duration    // Retention for 1h scale (default: 30d)
	Retention4h           time.Duration    // Retention for 4h scale (default: 90d)
	Retention24h          time.Duration    // Retention for 24h scale (default: 365d)
	RetentionInterval     time.Duration    // How often to run retention cleanup
	WriterFlushMaxEntries int              // Flush write batch when this many entries are buffered
	WriterFlushMaxBytes   int              // Flush write batch when this many bytes are buffered
}

const (
	bucketShardCount = 64
)

// ============================================================================
// Engine Definition
// ============================================================================

type Engine struct {
	db     *badger.DB
	config Config

	// Pipeline Channels
	ingestChan     chan DataPoint
	chunkChan      chan RawBucket
	writeChan      chan WritePayload
	writerFlushReq chan chan struct{}

	// Unflushed Data (for memory+disk queries)
	activeShards   []bucketShard
	pendingMu      sync.RWMutex
	pendingBuckets map[bucketKey]pendingBucket

	slotCount        int
	slotIntervalSecs int64
	bucketPool       sync.Pool
	workBufferPool   sync.Pool

	// Concurrency Management
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopErr  error
}

// ============================================================================
// Initialization & Lifecycle
// ============================================================================

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.ChunkFlushInterval <= 0 {
		cfg.ChunkFlushInterval = 5 * time.Minute
	}
	if cfg.IntervalResolution <= 0 {
		cfg.IntervalResolution = 5 * time.Minute
	}
	if cfg.IngestBufferSize <= 0 {
		cfg.IngestBufferSize = 10000
	}
	if cfg.CompactorWorkers <= 0 {
		cfg.CompactorWorkers = 1
	}
	if cfg.CompactionInterval <= 0 {
		cfg.CompactionInterval = 1 * time.Minute
	}
	if cfg.WriterFlushMaxEntries <= 0 {
		cfg.WriterFlushMaxEntries = 4096
	}
	if cfg.WriterFlushMaxBytes <= 0 {
		cfg.WriterFlushMaxBytes = 8 * 1024 * 1024
	}
	if cfg.RetentionInterval <= 0 {
		cfg.RetentionInterval = 10 * time.Minute
	}
	if cfg.Retention5m <= 0 {
		cfg.Retention5m = 3 * 24 * time.Hour
	}
	if cfg.Retention1h <= 0 {
		cfg.Retention1h = 30 * 24 * time.Hour
	}
	if cfg.Retention4h <= 0 {
		cfg.Retention4h = 90 * 24 * time.Hour
	}
	if cfg.Retention24h <= 0 {
		cfg.Retention24h = 365 * 24 * time.Hour
	}
	if len(cfg.CompactionTiers) == 0 {
		cfg.CompactionTiers = []CompactionTier{
			{Name: Scale5m, Duration: 5 * time.Minute, MinAge: 5 * time.Minute},
			{Name: Scale1h, Duration: 1 * time.Hour, MinAge: 1 * time.Hour},
			{Name: Scale4h, Duration: 4 * time.Hour, MinAge: 4 * time.Hour},
			{Name: Scale24h, Duration: 24 * time.Hour, MinAge: 0},
		}
	}

	// Open BadgerDB with provided or default options
	var opts badger.Options
	if cfg.BadgerOptions != nil {
		opts = *cfg.BadgerOptions
	} else {
		opts = badger.DefaultOptions(cfg.DBPath)
		opts.Logger = nil // Disable logging by default
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	slotIntervalSecs := int64(cfg.IntervalResolution / time.Second)
	if slotIntervalSecs <= 0 {
		slotIntervalSecs = 1
	}
	slotCount := int(int64(cfg.ChunkFlushInterval) / int64(cfg.IntervalResolution))
	if slotCount <= 0 {
		slotCount = 1
	}

	activeShards := make([]bucketShard, bucketShardCount)
	for i := range activeShards {
		activeShards[i] = bucketShard{buckets: make(map[bucketKey]*bucketAccumulator)}
	}

	engine := &Engine{
		db:               db,
		config:           cfg,
		ingestChan:       make(chan DataPoint, cfg.IngestBufferSize),
		chunkChan:        make(chan RawBucket, cfg.IngestBufferSize/10),
		writeChan:        make(chan WritePayload, cfg.IngestBufferSize/10),
		writerFlushReq:   make(chan chan struct{}, 1),
		activeShards:     activeShards,
		pendingBuckets:   make(map[bucketKey]pendingBucket),
		slotCount:        slotCount,
		slotIntervalSecs: slotIntervalSecs,
		ctx:              ctx,
		cancel:           cancel,
	}

	engine.bucketPool = sync.Pool{
		New: func() any {
			return &bucketAccumulator{
				values:  make([]int64, slotCount),
				filled:  make([]bool, slotCount),
				touched: make([]int, 0, slotCount),
			}
		},
	}
	engine.workBufferPool = sync.Pool{
		New: func() any {
			return &bucketWorkBuffer{
				workValues: make([]int64, slotCount),
				workFilled: make([]bool, slotCount),
			}
		},
	}

	return engine, nil
}

func (e *Engine) Start() {
	// Start packer (closes chunkChan when done)
	e.wg.Add(1)
	go e.runPacker()

	// Start compactor workers
	var compactorWg sync.WaitGroup
	for i := 0; i < e.config.CompactorWorkers; i++ {
		e.wg.Add(1)
		compactorWg.Add(1)
		go func() {
			defer compactorWg.Done()
			e.runCompactor()
		}()
	}

	// Close writeChan when all compactors finish
	go func() {
		compactorWg.Wait()
		close(e.writeChan)
	}()

	// Start writer (exits when writeChan closes)
	e.wg.Add(1)
	go e.runWriter()

	// Start background compaction if enabled
	if e.config.EnableAutoCompaction {
		e.wg.Add(1)
		go e.runChunkCompaction()
	}

	if e.hasRetentionConfigured() {
		e.wg.Add(1)
		go e.runRetentionCleanup()
	}
}

func (e *Engine) Stop() error {
	e.stopOnce.Do(func() {
		// Close input channel to signal no more data coming.
		close(e.ingestChan)

		// Cancel context so background compaction goroutines can exit.
		e.cancel()

		// Wait for all goroutines to finish naturally.
		e.wg.Wait()

		// Close the database.
		e.stopErr = e.db.Close()
	})

	return e.stopErr
}

func bucketShardIndex(key bucketKey) int {
	u := uint64(key.siteID*73856093) ^ uint64(key.testID*19349663) ^ uint64(key.windowStart*83492791)
	u ^= u >> 33
	u *= 0xff51afd7ed558ccd
	u ^= u >> 33
	return int(u % bucketShardCount)
}

func (e *Engine) getBucketAccumulator(siteID, testID, windowStart int64) *bucketAccumulator {
	acc := e.bucketPool.Get().(*bucketAccumulator)
	acc.siteID = siteID
	acc.testID = testID
	acc.windowStart = windowStart
	acc.touched = acc.touched[:0]
	return acc
}

func (e *Engine) releaseBucketAccumulator(acc *bucketAccumulator) {
	for _, idx := range acc.touched {
		acc.filled[idx] = false
		acc.values[idx] = 0
	}
	acc.touched = acc.touched[:0]
	e.bucketPool.Put(acc)
}

func (e *Engine) releaseWorkBuffer(values []int64, filled []bool, touched []int) {
	for _, idx := range touched {
		filled[idx] = false
		values[idx] = 0
	}
	e.workBufferPool.Put(&bucketWorkBuffer{
		workValues: values,
		workFilled: filled,
	})
}

func (e *Engine) appendBucketPoints(acc *bucketAccumulator, start, end int64, out []DataPoint) []DataPoint {
	for _, idx := range acc.touched {
		if !acc.filled[idx] {
			continue
		}
		ts := acc.windowStart + int64(idx)*e.slotIntervalSecs
		if ts < start || ts > end {
			continue
		}
		out = append(out, DataPoint{
			Timestamp: ts,
			SiteID:    acc.siteID,
			TestID:    acc.testID,
			Value:     acc.values[idx],
		})
	}
	return out
}

func (e *Engine) flushActiveBuckets() {
	snapshots := make([]map[bucketKey]*bucketAccumulator, 0, len(e.activeShards))
	for i := range e.activeShards {
		shard := &e.activeShards[i]
		shard.mu.Lock()
		current := shard.buckets
		shard.buckets = make(map[bucketKey]*bucketAccumulator)
		shard.mu.Unlock()
		snapshots = append(snapshots, current)
	}

	now := time.Now()
	pendingTTL := e.config.ChunkFlushInterval + time.Second
	if pendingTTL < 2*time.Second {
		pendingTTL = 2 * time.Second
	}

	e.pendingMu.Lock()
	for key, pb := range e.pendingBuckets {
		if now.After(pb.expiresAt) {
			e.releaseBucketAccumulator(pb.bucket)
			delete(e.pendingBuckets, key)
		}
	}
	for _, bucketMap := range snapshots {
		for key, acc := range bucketMap {
			if len(acc.touched) == 0 {
				e.releaseBucketAccumulator(acc)
				continue
			}
			if existing, ok := e.pendingBuckets[key]; ok {
				e.releaseBucketAccumulator(existing.bucket)
			}
			e.pendingBuckets[key] = pendingBucket{
				bucket:    acc,
				expiresAt: now.Add(pendingTTL),
			}
		}
	}
	e.pendingMu.Unlock()

	for _, bucketMap := range snapshots {
		for key, acc := range bucketMap {
			if len(acc.touched) == 0 {
				continue
			}

			work := e.workBufferPool.Get().(*bucketWorkBuffer)
			touched := make([]int, len(acc.touched))
			copy(touched, acc.touched)
			for _, idx := range touched {
				work.workValues[idx] = acc.values[idx]
				work.workFilled[idx] = true
			}

			e.chunkChan <- RawBucket{
				SiteID:    key.siteID,
				TestID:    key.testID,
				StartTime: key.windowStart,
				Interval:  e.config.IntervalResolution,
				Values:    work.workValues,
				Filled:    work.workFilled,
				Touched:   touched,
			}
		}
	}
}

func (e *Engine) waitPipelinesQuiet(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	stable := 0
	for time.Now().Before(deadline) {
		if len(e.chunkChan) == 0 && len(e.writeChan) == 0 {
			stable++
			if stable >= 5 {
				return true
			}
		} else {
			stable = 0
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func (e *Engine) flushWriterNow(timeout time.Duration) bool {
	ack := make(chan struct{})
	select {
	case e.writerFlushReq <- ack:
	case <-time.After(timeout):
		return false
	}

	select {
	case <-ack:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ============================================================================
// Public API: Ingestion
// ============================================================================

func (e *Engine) Add(timestamp, siteID, testID, value int64) {
	pt := DataPoint{
		Timestamp: timestamp,
		SiteID:    siteID,
		TestID:    testID,
		Value:     value,
	}

	// Add to active bucket synchronously to avoid ingestion/query races.
	flushIntervalSecs := int64(e.config.ChunkFlushInterval.Seconds())
	if flushIntervalSecs <= 0 {
		flushIntervalSecs = 1
	}
	windowStart := (pt.Timestamp / flushIntervalSecs) * flushIntervalSecs
	key := bucketKey{
		siteID:      pt.SiteID,
		testID:      pt.TestID,
		windowStart: windowStart,
	}
	slotIdx := int((pt.Timestamp - windowStart) / e.slotIntervalSecs)
	if slotIdx < 0 || slotIdx >= e.slotCount {
		return
	}

	bucketIdx := bucketShardIndex(key)
	bucketShard := &e.activeShards[bucketIdx]
	bucketShard.mu.Lock()
	acc := bucketShard.buckets[key]
	if acc == nil {
		acc = e.getBucketAccumulator(pt.SiteID, pt.TestID, windowStart)
		bucketShard.buckets[key] = acc
	}
	if !acc.filled[slotIdx] {
		acc.filled[slotIdx] = true
		acc.touched = append(acc.touched, slotIdx)
	}
	acc.values[slotIdx] = pt.Value // Last write wins inside the slot.
	bucketShard.mu.Unlock()
}

// ============================================================================
// Pipeline Goroutines
// ============================================================================

// runPacker periodically seals active buckets and forwards them to compactor workers.
func (e *Engine) runPacker() {
	defer e.wg.Done()
	defer close(e.chunkChan) // Close output channel when done

	ticker := time.NewTicker(e.config.ChunkFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case _, ok := <-e.ingestChan:
			if !ok {
				// Channel closed, flush and exit
				e.flushActiveBuckets()
				return
			}

		case <-ticker.C:
			// 3. Seal buckets and send to compactor
			e.flushActiveBuckets()
		}
	}
}

// runCompactor performs the heavy CPU work of delta-encoding and bitmasking.
func (e *Engine) runCompactor() {
	defer e.wg.Done()

	for {
		raw, ok := <-e.chunkChan
		if !ok {
			return // Channel closed
		}

		// Skip empty buckets
		if len(raw.Touched) == 0 {
			e.releaseWorkBuffer(raw.Values, raw.Filled, raw.Touched)
			continue
		}

		// Work in nanoseconds to avoid rounding issues
		intervalNanos := int64(raw.Interval)
		if intervalNanos == 0 {
			intervalNanos = int64(time.Second) // Minimum 1 second interval
		}

		numSlots := len(raw.Values)

		// Convert to seconds for timestamp calculations
		intervalSecs := intervalNanos / int64(time.Second)
		if intervalSecs == 0 {
			intervalSecs = 1
		}

		values := raw.Values
		firstKnown := -1
		for i := 0; i < numSlots; i++ {
			if raw.Filled[i] {
				firstKnown = i
				break
			}
		}
		if firstKnown < 0 {
			e.releaseWorkBuffer(raw.Values, raw.Filled, raw.Touched)
			continue
		}

		lastValue := values[firstKnown]
		for i := 0; i < firstKnown; i++ {
			values[i] = lastValue
		}
		for i := firstKnown + 1; i < numSlots; i++ {
			if raw.Filled[i] {
				lastValue = values[i]
				continue
			}
			values[i] = lastValue
		}

		chunk := CompressedChunk{
			SiteID:    raw.SiteID,
			TestID:    raw.TestID,
			StartTime: raw.StartTime,
			Interval:  raw.Interval,
			Count:     uint32(numSlots),
			FirstVal:  values[0],
			LastVal:   values[numSlots-1],
		}

		// Calculate EndTime
		chunk.EndTime = raw.StartTime + (int64(numSlots) * intervalSecs)

		// Encode values using Delta-of-Delta with Run-Length Encoding
		chunk.Data = encodeValuesOptimized(values, chunk.FirstVal)
		chunk.CompressionType = CompressionSnappy

		// All raw points are persisted in the 5m collection.
		tierName := Scale5m

		// Generate Keys
		testKey := makeTestKey(raw.TestID, raw.StartTime, tierName)
		siteKey := makeSiteKey(raw.SiteID, raw.TestID, raw.StartTime, tierName)

		// Serialize chunk header and data
		chunkBytes := serializeChunk(chunk)

		e.writeChan <- WritePayload{Key: testKey, Data: chunkBytes}
		e.writeChan <- WritePayload{Key: siteKey, Data: chunkBytes}

		e.releaseWorkBuffer(raw.Values, raw.Filled, raw.Touched)
	}
}

// runWriter batches compressed payloads into BadgerDB to maximize I/O throughput.
func (e *Engine) runWriter() {
	defer e.wg.Done()

	wb := e.db.NewWriteBatch()
	batchEntries := 0
	batchBytes := 0

	resetBatch := func() {
		wb = e.db.NewWriteBatch()
		batchEntries = 0
		batchBytes = 0
	}
	flushBatch := func() {
		if batchEntries == 0 {
			return
		}
		if err := wb.Flush(); err != nil {
			_ = err
		}
		wb.Cancel()
		resetBatch()
	}
	defer wb.Cancel()

	// Flush periodically to ensure data is written with bounded latency.
	flushInterval := e.config.ChunkFlushInterval / 2
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}
	if flushInterval < 50*time.Millisecond {
		flushInterval = 50 * time.Millisecond
	}
	if flushInterval > time.Second {
		flushInterval = time.Second
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			flushBatch()

		case ack := <-e.writerFlushReq:
			flushBatch()
			close(ack)

		case payload, ok := <-e.writeChan:
			if !ok {
				flushBatch()
				return
			}

			err := wb.Set(payload.Key, payload.Data)
			if err != nil {
				flushBatch()
				if retryErr := wb.Set(payload.Key, payload.Data); retryErr != nil {
					_ = retryErr
					continue
				}
			}
			batchEntries++
			batchBytes += len(payload.Key) + len(payload.Data)
			if batchEntries >= e.config.WriterFlushMaxEntries || batchBytes >= e.config.WriterFlushMaxBytes {
				flushBatch()
			}
		}
	}
}

// ============================================================================
// Internal API: Unflushed Data Queries
// ============================================================================

// getUnflushedPoints returns unflushed points from activeBuckets for a specific test in a time range
func (e *Engine) getUnflushedPoints(testID int64, start, end int64) []DataPoint {
	var results []DataPoint
	for i := range e.activeShards {
		shard := &e.activeShards[i]
		shard.mu.RLock()
		for key, acc := range shard.buckets {
			if key.testID == testID {
				results = e.appendBucketPoints(acc, start, end, results)
			}
		}
		shard.mu.RUnlock()
	}

	now := time.Now()
	e.pendingMu.RLock()
	for key, pb := range e.pendingBuckets {
		if pb.expiresAt.Before(now) {
			continue
		}
		if key.testID == testID {
			results = e.appendBucketPoints(pb.bucket, start, end, results)
		}
	}
	e.pendingMu.RUnlock()

	return results
}

// getUnflushedPointsForSite returns unflushed points for a specific site/test in a time range
func (e *Engine) getUnflushedPointsForSite(siteID, testID int64, start, end int64) []DataPoint {
	var results []DataPoint
	for i := range e.activeShards {
		shard := &e.activeShards[i]
		shard.mu.RLock()
		for key, acc := range shard.buckets {
			if key.siteID == siteID && key.testID == testID {
				results = e.appendBucketPoints(acc, start, end, results)
			}
		}
		shard.mu.RUnlock()
	}

	now := time.Now()
	e.pendingMu.RLock()
	for key, pb := range e.pendingBuckets {
		if pb.expiresAt.Before(now) {
			continue
		}
		if key.siteID == siteID && key.testID == testID {
			results = e.appendBucketPoints(pb.bucket, start, end, results)
		}
	}
	e.pendingMu.RUnlock()

	return results
}

// getUnflushedPointsForSiteAll returns unflushed points for all tests at a specific site in a time range
func (e *Engine) getUnflushedPointsForSiteAll(siteID int64, start, end int64) []DataPoint {
	var results []DataPoint
	for i := range e.activeShards {
		shard := &e.activeShards[i]
		shard.mu.RLock()
		for key, acc := range shard.buckets {
			if key.siteID == siteID {
				results = e.appendBucketPoints(acc, start, end, results)
			}
		}
		shard.mu.RUnlock()
	}

	now := time.Now()
	e.pendingMu.RLock()
	for key, pb := range e.pendingBuckets {
		if pb.expiresAt.Before(now) {
			continue
		}
		if key.siteID == siteID {
			results = e.appendBucketPoints(pb.bucket, start, end, results)
		}
	}
	e.pendingMu.RUnlock()

	return results
}

// ============================================================================
// Public API: Historical Queries (Badger Prefix Scans + Memory)
// ============================================================================

func floorToWindow(timestamp, windowSec int64) int64 {
	mod := timestamp % windowSec
	if mod < 0 {
		mod += windowSec
	}
	return timestamp - mod
}

func normalizeScale(scale string) (string, int64, error) {
	if scale == "" {
		scale = Scale5m
	}
	d, ok := scaleDurations[scale]
	if !ok {
		return "", 0, fmt.Errorf("invalid scale %q (allowed: %s, %s, %s, %s)", scale, Scale5m, Scale1h, Scale4h, Scale24h)
	}
	sec := int64(d / time.Second)
	if sec <= 0 {
		sec = 1
	}
	return scale, sec, nil
}

func aggregatePoints(points []DataPoint, aggregateWindowSec int64) []DataPoint {
	if aggregateWindowSec == 0 || len(points) == 0 {
		return points
	}

	windowStart := floorToWindow(points[0].Timestamp, aggregateWindowSec)
	sum := points[0].Value
	count := int64(1)
	first := points[0]
	results := make([]DataPoint, 0, len(points))

	for i := 1; i < len(points); i++ {
		pt := points[i]
		ptWindowStart := floorToWindow(pt.Timestamp, aggregateWindowSec)
		if ptWindowStart != windowStart {
			results = append(results, DataPoint{
				Timestamp: windowStart,
				SiteID:    first.SiteID,
				TestID:    first.TestID,
				Value:     sum / count,
			})
			windowStart = ptWindowStart
			sum = pt.Value
			count = 1
			first = pt
			continue
		}

		sum += pt.Value
		count++
	}

	results = append(results, DataPoint{
		Timestamp: windowStart,
		SiteID:    first.SiteID,
		TestID:    first.TestID,
		Value:     sum / count,
	})

	return results
}

func aggregatePointsByTest(points []DataPoint, aggregateWindowSec int64) []DataPoint {
	if aggregateWindowSec == 0 || len(points) == 0 {
		return points
	}

	type agg struct {
		siteID int64
		testID int64
		sum    int64
		count  int64
	}

	currentWindow := floorToWindow(points[0].Timestamp, aggregateWindowSec)
	perTest := make(map[int64]agg)
	results := make([]DataPoint, 0, len(points))

	flush := func(windowStart int64) {
		testIDs := make([]int64, 0, len(perTest))
		for testID := range perTest {
			testIDs = append(testIDs, testID)
		}
		sort.Slice(testIDs, func(i, j int) bool { return testIDs[i] < testIDs[j] })
		for _, testID := range testIDs {
			a := perTest[testID]
			results = append(results, DataPoint{
				Timestamp: windowStart,
				SiteID:    a.siteID,
				TestID:    a.testID,
				Value:     a.sum / a.count,
			})
		}
	}

	for _, pt := range points {
		ptWindow := floorToWindow(pt.Timestamp, aggregateWindowSec)
		if ptWindow != currentWindow {
			flush(currentWindow)
			clear(perTest)
			currentWindow = ptWindow
		}

		a := perTest[pt.TestID]
		if a.count == 0 {
			a = agg{
				siteID: pt.SiteID,
				testID: pt.TestID,
			}
		}
		a.sum += pt.Value
		a.count++
		perTest[pt.TestID] = a
	}

	flush(currentWindow)
	return results
}

func (e *Engine) getTestRangeForTier(testID int64, start, end int64, tier string) ([]DataPoint, error) {
	// Use map to deduplicate by timestamp.
	pointMap := make(map[int64]DataPoint)

	// 1. Get flushed data from disk.
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("test-%s!%016x!", tier, testID))
		startKey := []byte(fmt.Sprintf("test-%s!%016x!%016x", tier, testID, start))

		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			chunk := deserializeChunk(valCopy)

			// Stop if chunk is completely past our end time.
			if chunk.StartTime > end {
				break
			}

			// Use IDs from the chunk itself.
			points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)

			// Filter points strictly inside [start, end].
			for _, pt := range points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					pointMap[pt.Timestamp] = pt
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	results := make([]DataPoint, 0, len(pointMap))
	for _, pt := range pointMap {
		results = append(results, pt)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})
	return results, nil
}

func (e *Engine) GetTestRange(testID int64, start, end int64, scale string) ([]DataPoint, error) {
	tier, windowSec, err := normalizeScale(scale)
	if err != nil {
		return nil, err
	}

	results, err := e.getTestRangeForTier(testID, start, end, tier)
	if err != nil {
		return nil, err
	}

	if tier == Scale5m {
		// Merge unflushed memory points only for raw scale.
		pointMap := make(map[int64]DataPoint, len(results))
		for _, pt := range results {
			pointMap[pt.Timestamp] = pt
		}
		memoryPoints := e.getUnflushedPoints(testID, start, end)
		for _, pt := range memoryPoints {
			pointMap[pt.Timestamp] = pt
		}
		merged := make([]DataPoint, 0, len(pointMap))
		for _, pt := range pointMap {
			merged = append(merged, pt)
		}
		sort.Slice(merged, func(i, j int) bool {
			return merged[i].Timestamp < merged[j].Timestamp
		})
		return merged, nil
	}

	if len(results) == 0 {
		// Backward-safe fallback while rollups are being populated.
		raw, err := e.GetTestRange(testID, start, end, Scale5m)
		if err != nil {
			return nil, err
		}
		return aggregatePoints(raw, windowSec), nil
	}

	return aggregatePoints(results, windowSec), nil
}

func (e *Engine) getSiteTestRangeForTier(siteID, testID int64, start, end int64, tier string) ([]DataPoint, error) {
	// Use map to deduplicate by timestamp.
	pointMap := make(map[int64]DataPoint)

	// 1. Get flushed data from disk.
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("site-%s!%016x!", tier, siteID))
		startKey := []byte(fmt.Sprintf("site-%s!%016x!%016x!%016x", tier, siteID, start, 0))

		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			chunk := deserializeChunk(valCopy)

			// Stop if chunk is completely past our end time.
			if chunk.StartTime > end {
				break
			}

			// Skip chunks that don't match our testID.
			if chunk.TestID != testID {
				continue
			}

			points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)

			// Filter points for time range.
			for _, pt := range points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					pointMap[pt.Timestamp] = pt
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	results := make([]DataPoint, 0, len(pointMap))
	for _, pt := range pointMap {
		results = append(results, pt)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})
	return results, nil
}

// GetSiteTestRange retrieves historical data for a specific test at a specific site.
func (e *Engine) GetSiteTestRange(siteID, testID int64, start, end int64, scale string) ([]DataPoint, error) {
	tier, windowSec, err := normalizeScale(scale)
	if err != nil {
		return nil, err
	}

	results, err := e.getSiteTestRangeForTier(siteID, testID, start, end, tier)
	if err != nil {
		return nil, err
	}

	if tier == Scale5m {
		pointMap := make(map[int64]DataPoint, len(results))
		for _, pt := range results {
			pointMap[pt.Timestamp] = pt
		}
		memoryPoints := e.getUnflushedPointsForSite(siteID, testID, start, end)
		for _, pt := range memoryPoints {
			pointMap[pt.Timestamp] = pt
		}
		merged := make([]DataPoint, 0, len(pointMap))
		for _, pt := range pointMap {
			merged = append(merged, pt)
		}
		sort.Slice(merged, func(i, j int) bool {
			return merged[i].Timestamp < merged[j].Timestamp
		})
		return merged, nil
	}

	if len(results) == 0 {
		raw, err := e.GetSiteTestRange(siteID, testID, start, end, Scale5m)
		if err != nil {
			return nil, err
		}
		return aggregatePoints(raw, windowSec), nil
	}

	return aggregatePoints(results, windowSec), nil
}

func (e *Engine) getSiteRangeForTier(siteID int64, start, end int64, tier string) ([]DataPoint, error) {
	// Use map keyed by (timestamp, testID) to deduplicate.
	type pointKey struct {
		timestamp int64
		testID    int64
	}
	pointMap := make(map[pointKey]DataPoint)

	// 1. Get flushed data from disk.
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("site-%s!%016x!", tier, siteID))
		startKey := []byte(fmt.Sprintf("site-%s!%016x!%016x!%016x", tier, siteID, start, 0))

		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			chunk := deserializeChunk(valCopy)

			// Stop if chunk is completely past our end time.
			if chunk.StartTime > end {
				break
			}

			// Decode using IDs from the chunk.
			points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)

			// Filter points strictly inside [start, end].
			for _, pt := range points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					key := pointKey{timestamp: pt.Timestamp, testID: pt.TestID}
					pointMap[key] = pt
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	results := make([]DataPoint, 0, len(pointMap))
	for _, pt := range pointMap {
		results = append(results, pt)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Timestamp != results[j].Timestamp {
			return results[i].Timestamp < results[j].Timestamp
		}
		return results[i].TestID < results[j].TestID
	})
	return results, nil
}

// GetSiteRange retrieves historical data for all tests at a specific site.
func (e *Engine) GetSiteRange(siteID int64, start, end int64, scale string) ([]DataPoint, error) {
	tier, windowSec, err := normalizeScale(scale)
	if err != nil {
		return nil, err
	}

	results, err := e.getSiteRangeForTier(siteID, start, end, tier)
	if err != nil {
		return nil, err
	}

	if tier == Scale5m {
		type pointKey struct {
			timestamp int64
			testID    int64
		}
		pointMap := make(map[pointKey]DataPoint, len(results))
		for _, pt := range results {
			pointMap[pointKey{timestamp: pt.Timestamp, testID: pt.TestID}] = pt
		}
		memoryPoints := e.getUnflushedPointsForSiteAll(siteID, start, end)
		for _, pt := range memoryPoints {
			pointMap[pointKey{timestamp: pt.Timestamp, testID: pt.TestID}] = pt
		}
		merged := make([]DataPoint, 0, len(pointMap))
		for _, pt := range pointMap {
			merged = append(merged, pt)
		}
		sort.Slice(merged, func(i, j int) bool {
			if merged[i].Timestamp != merged[j].Timestamp {
				return merged[i].Timestamp < merged[j].Timestamp
			}
			return merged[i].TestID < merged[j].TestID
		})
		return merged, nil
	}

	if len(results) == 0 {
		raw, err := e.GetSiteRange(siteID, start, end, Scale5m)
		if err != nil {
			return nil, err
		}
		return aggregatePointsByTest(raw, windowSec), nil
	}

	return aggregatePointsByTest(results, windowSec), nil
}

// ============================================================================
// Chunk Compaction
// ============================================================================

// runChunkCompaction periodically compacts short-term chunks into long-term chunks
func (e *Engine) runChunkCompaction() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.performCompaction()
		}
	}
}

func (e *Engine) hasRetentionConfigured() bool {
	return e.config.Retention5m > 0 ||
		e.config.Retention1h > 0 ||
		e.config.Retention4h > 0 ||
		e.config.Retention24h > 0
}

func (e *Engine) retentionByScale() map[string]time.Duration {
	return map[string]time.Duration{
		Scale5m:  e.config.Retention5m,
		Scale1h:  e.config.Retention1h,
		Scale4h:  e.config.Retention4h,
		Scale24h: e.config.Retention24h,
	}
}

func (e *Engine) runRetentionCleanup() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.RetentionInterval)
	defer ticker.Stop()

	e.applyRetentionPolicies()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.applyRetentionPolicies()
		}
	}
}

func (e *Engine) applyRetentionPolicies() {
	for scale, retention := range e.retentionByScale() {
		if retention <= 0 {
			continue
		}
		e.deleteExpiredChunks(scale, retention)
	}
}

func (e *Engine) deleteExpiredChunks(scale string, retention time.Duration) {
	if retention <= 0 {
		return
	}

	cutoff := time.Now().Add(-retention).Unix()
	e.deleteExpiredChunksBefore(scale, cutoff)
}

func (e *Engine) deleteExpiredChunksBefore(scale string, cutoff int64) {
	_ = e.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("test-%s!", scale))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			var testID, timestamp int64
			_, err := fmt.Sscanf(string(key), "test-"+scale+"!%016x!%016x", &testID, &timestamp)
			if err != nil {
				continue
			}
			if timestamp >= cutoff {
				continue
			}

			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			chunk := deserializeChunk(valCopy)

			testKey := append([]byte(nil), key...)
			siteKey := makeSiteKey(chunk.SiteID, chunk.TestID, timestamp, scale)

			_ = txn.Delete(testKey)
			_ = txn.Delete(siteKey)
		}

		return nil
	})
}

// performCompaction computes fixed-scale rollups:
// 5m -> 1h -> 4h -> 24h.
func (e *Engine) performCompaction() {
	steps := [][2]string{
		{Scale5m, Scale1h},
		{Scale1h, Scale4h},
		{Scale4h, Scale24h},
	}

	for _, step := range steps {
		sourceDur, srcOK := scaleDurations[step[0]]
		targetDur, dstOK := scaleDurations[step[1]]
		if !srcOK || !dstOK {
			continue
		}

		e.compactTier(
			CompactionTier{Name: step[0], Duration: sourceDur, MinAge: sourceDur},
			CompactionTier{Name: step[1], Duration: targetDur, MinAge: targetDur},
		)
	}
}

// compactTier creates averaged points in targetTier without deleting source-tier data.
func (e *Engine) compactTier(sourceTier, targetTier CompactionTier) {
	err := e.db.Update(func(txn *badger.Txn) error {
		type rollupGroup struct {
			siteID      int64
			testID      int64
			targetStart int64
			sum         int64
			count       int64
		}

		groups := make(map[string]*rollupGroup)
		cutoffTime := time.Now().Add(-sourceTier.MinAge).Unix()
		nowUnix := time.Now().Unix()
		targetDurationSecs := int64(targetTier.Duration / time.Second)
		if targetDurationSecs <= 0 {
			targetDurationSecs = 1
		}

		// Scan all chunks in source tier.
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("test-%s!", sourceTier.Name))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Parse timestamp from key to check age.
			var testID, timestamp int64
			_, err := fmt.Sscanf(string(key), "test-"+sourceTier.Name+"!%016x!%016x", &testID, &timestamp)
			if err != nil {
				continue
			}

			// Only compact chunks older than cutoff.
			if timestamp > cutoffTime {
				continue
			}

			// Get chunk data.
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			chunk := deserializeChunk(valCopy)
			points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)
			for _, pt := range points {
				targetStart := floorToWindow(pt.Timestamp, targetDurationSecs)
				// Only roll up fully closed windows.
				if targetStart+targetDurationSecs > nowUnix {
					continue
				}

				groupKey := fmt.Sprintf("%d:%d:%d", chunk.SiteID, chunk.TestID, targetStart)
				group, exists := groups[groupKey]
				if !exists {
					group = &rollupGroup{
						siteID:      chunk.SiteID,
						testID:      chunk.TestID,
						targetStart: targetStart,
					}
					groups[groupKey] = group
				}
				group.sum += pt.Value
				group.count++
			}
		}

		// Write averaged target-tier chunks.
		for _, group := range groups {
			if group.count == 0 {
				continue
			}
			avg := group.sum / group.count
			rolled := CompressedChunk{
				SiteID:          group.siteID,
				TestID:          group.testID,
				StartTime:       group.targetStart,
				Interval:        targetTier.Duration,
				Count:           1,
				FirstVal:        avg,
				LastVal:         avg,
				CompressionType: CompressionSnappy,
			}
			rolled.EndTime = group.targetStart + targetDurationSecs
			rolled.Data = encodeValuesOptimized([]int64{avg}, avg)

			chunkBytes := serializeChunk(rolled)
			testKey := makeTestKey(group.testID, group.targetStart, targetTier.Name)
			siteKey := makeSiteKey(group.siteID, group.testID, group.targetStart, targetTier.Name)

			if err := txn.Set(testKey, chunkBytes); err != nil {
				continue
			}
			if err := txn.Set(siteKey, chunkBytes); err != nil {
				continue
			}
		}

		return nil
	})

	if err != nil {
		// In production, log this error
		_ = err
	}
}

// ============================================================================
// Optimized Encoding/Decoding
// ============================================================================

// encodeValuesOptimized uses Delta-of-Delta + RLE + Snappy compression
func encodeValuesOptimized(values []int64, firstVal int64) []byte {
	if len(values) == 0 {
		return nil
	}

	// Pre-allocate buffer with estimated size
	estimatedSize := len(values) * 2
	buffer := bytes.NewBuffer(make([]byte, 0, estimatedSize))
	tmpBuf := make([]byte, binary.MaxVarintLen64)

	// Encode using Delta-of-Delta with Run-Length Encoding
	prevValue := firstVal
	i := 0

	for i < len(values) {
		currentValue := values[i]
		delta := currentValue - prevValue

		// Count how many consecutive values have the same delta (RLE)
		runLength := 1
		for i+runLength < len(values) {
			nextDelta := values[i+runLength] - values[i+runLength-1]
			if nextDelta != delta {
				break
			}
			runLength++
		}

		// Write delta and run length
		n := binary.PutVarint(tmpBuf, delta)
		buffer.Write(tmpBuf[:n])

		n = binary.PutUvarint(tmpBuf, uint64(runLength))
		buffer.Write(tmpBuf[:n])

		// Advance
		i += runLength
		if runLength > 0 {
			prevValue = values[i-1]
		}
	}

	// Apply Snappy compression
	compressed := snappy.Encode(nil, buffer.Bytes())
	return compressed
}

// decodeValuesOptimized reverses the optimized encoding
func decodeValuesOptimized(data []byte, firstVal int64, count int, compressionType CompressionType) []int64 {
	if len(data) == 0 || count == 0 {
		return nil
	}

	// Decompress if needed
	var uncompressed []byte
	var err error

	switch compressionType {
	case CompressionSnappy:
		uncompressed, err = snappy.Decode(nil, data)
		if err != nil {
			return nil
		}
	default:
		uncompressed = data
	}

	reader := bytes.NewReader(uncompressed)
	values := make([]int64, 0, count)
	currentValue := firstVal

	for len(values) < count {
		// Read delta
		delta, err := binary.ReadVarint(reader)
		if err != nil {
			break
		}

		// Read run length
		runLengthU64, err := binary.ReadUvarint(reader)
		if err != nil {
			break
		}
		runLength := int(runLengthU64)

		// Generate values for this run
		for j := 0; j < runLength && len(values) < count; j++ {
			currentValue += delta
			values = append(values, currentValue)
		}
	}

	return values
}

// ============================================================================
// Internal Helpers
// ============================================================================

// getTierNames returns all tier names in order (first tier first)
func (e *Engine) getTierNames() []string {
	if len(e.config.CompactionTiers) == 0 {
		return []string{Scale5m, Scale1h, Scale4h, Scale24h}
	}

	names := make([]string, len(e.config.CompactionTiers))
	for i, tier := range e.config.CompactionTiers {
		names[i] = tier.Name
	}
	return names
}

// selectTiersForQuery returns only tiers that could contain data in the given time range
func (e *Engine) selectTiersForQuery(start, end int64) []string {
	if len(e.config.CompactionTiers) == 0 {
		return []string{Scale5m, Scale1h, Scale4h, Scale24h}
	}

	now := time.Now().Unix()
	youngestAge := now - end
	if youngestAge < 0 {
		youngestAge = 0
	}
	oldestAge := now - start
	if oldestAge < youngestAge {
		oldestAge = youngestAge
	}

	var tiers []string
	prevMinAge := int64(0)
	for i, tier := range e.config.CompactionTiers {
		tierMinAge := prevMinAge
		tierMaxAge := int64(1<<62 - 1)
		if i < len(e.config.CompactionTiers)-1 {
			nextMin := int64(tier.MinAge.Seconds())
			if nextMin > 0 {
				tierMaxAge = nextMin
			}
		}

		// Range overlap test: [youngestAge, oldestAge] intersects [tierMinAge, tierMaxAge)
		if oldestAge >= tierMinAge && youngestAge < tierMaxAge {
			tiers = append(tiers, tier.Name)
		}

		nextPrev := int64(tier.MinAge.Seconds())
		if nextPrev > prevMinAge {
			prevMinAge = nextPrev
		}
	}

	// If no tiers matched, return all tiers (safety fallback)
	if len(tiers) == 0 {
		return e.getTierNames()
	}

	return tiers
}

// Key generation helpers
func makeTestKey(testID int64, timestamp int64, tier string) []byte {
	return []byte(fmt.Sprintf("test-%s!%016x!%016x", tier, testID, timestamp))
}

func makeSiteKey(siteID, testID int64, timestamp int64, tier string) []byte {
	return []byte(fmt.Sprintf("site-%s!%016x!%016x!%016x", tier, siteID, timestamp, testID))
}

func serializeChunk(c CompressedChunk) []byte {
	// Calculate total size needed
	headerSize := 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 1 + 4 // All fixed fields + compression type + data length
	totalSize := headerSize + len(c.Data)

	buf := make([]byte, totalSize)
	offset := 0

	// Write fixed fields in big endian for consistent ordering
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.SiteID))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.TestID))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.StartTime))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.EndTime))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.Interval))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.FirstVal))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.LastVal))
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], c.Count)
	offset += 4
	buf[offset] = uint8(c.CompressionType)
	offset += 1
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(c.Data)))
	offset += 4

	// Write variable-length delta data
	copy(buf[offset:], c.Data)

	return buf
}

func deserializeChunk(data []byte) CompressedChunk {
	if len(data) < 69 {
		// Invalid chunk, return empty
		return CompressedChunk{}
	}

	offset := 0
	chunk := CompressedChunk{}

	chunk.SiteID = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.TestID = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.StartTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.EndTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.Interval = time.Duration(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.FirstVal = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.LastVal = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	chunk.Count = binary.BigEndian.Uint32(data[offset:])
	offset += 4
	chunk.CompressionType = CompressionType(data[offset])
	offset += 1
	dataLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Read delta data
	if offset+int(dataLen) <= len(data) {
		chunk.Data = make([]byte, dataLen)
		copy(chunk.Data, data[offset:offset+int(dataLen)])
	}

	return chunk
}

func decodeChunk(siteID, testID int64, chunk CompressedChunk) []DataPoint {
	// Decode values using optimized decoder
	values := decodeValuesOptimized(chunk.Data, chunk.FirstVal, int(chunk.Count), chunk.CompressionType)
	if values == nil {
		return nil
	}

	// Use full nanosecond precision for interval to avoid rounding issues
	intervalNanos := int64(chunk.Interval)
	intervalSecs := intervalNanos / int64(time.Second)
	if intervalSecs == 0 {
		intervalSecs = 1 // Minimum 1 second for timestamp calculation
	}

	points := make([]DataPoint, len(values))

	for i, value := range values {
		points[i] = DataPoint{
			Timestamp: chunk.StartTime + (int64(i) * intervalSecs),
			SiteID:    siteID,
			TestID:    testID,
			Value:     value,
		}
	}

	return points
}
