package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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
	Points    []DataPoint
}

// CompressionType indicates the compression algorithm used
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionSnappy
)

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

type pendingBucket struct {
	points    []DataPoint
	expiresAt time.Time
}

// Config tunes the TSDB engine.
type Config struct {
	DBPath               string          // Path to BadgerDB directory
	BadgerOptions        *badger.Options // Optional BadgerDB options (nil = defaults)
	ChunkFlushInterval   time.Duration   // How often to flush data to initial chunks
	IntervalResolution   time.Duration   // Expected time between data points
	IngestBufferSize     int
	CompactorWorkers     int
	CompactionTiers      []CompactionTier // Ordered list of compaction tiers
	CompactionInterval   time.Duration    // How often to check for chunks to compact
	EnableAutoCompaction bool             // Enable automatic background compaction
}

// ============================================================================
// Engine Definition
// ============================================================================

type Engine struct {
	db     *badger.DB
	config Config

	// Pipeline Channels
	ingestChan chan DataPoint
	chunkChan  chan RawBucket
	writeChan  chan WritePayload

	// Real-Time State Keeper
	stateMu      sync.RWMutex
	latestValues map[int64]map[int64]DataPoint // map[SiteID]map[TestID]DataPoint

	// Unflushed Data (for memory+disk queries)
	bucketsMu      sync.RWMutex
	activeBuckets  map[bucketKey][]DataPoint
	pendingMu      sync.RWMutex
	pendingBuckets map[bucketKey]pendingBucket

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

	return &Engine{
		db:             db,
		config:         cfg,
		ingestChan:     make(chan DataPoint, cfg.IngestBufferSize),
		chunkChan:      make(chan RawBucket, cfg.IngestBufferSize/10),
		writeChan:      make(chan WritePayload, cfg.IngestBufferSize/10),
		latestValues:   make(map[int64]map[int64]DataPoint),
		activeBuckets:  make(map[bucketKey][]DataPoint),
		pendingBuckets: make(map[bucketKey]pendingBucket),
		ctx:            ctx,
		cancel:         cancel,
	}, nil
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

	// Update real-time mirror synchronously for immediate query visibility.
	e.stateMu.Lock()
	if _, exists := e.latestValues[pt.SiteID]; !exists {
		e.latestValues[pt.SiteID] = make(map[int64]DataPoint)
	}
	e.latestValues[pt.SiteID][pt.TestID] = pt
	e.stateMu.Unlock()

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
	e.bucketsMu.Lock()
	e.activeBuckets[key] = append(e.activeBuckets[key], pt)
	e.bucketsMu.Unlock()

	// Keep the pipeline signal for compaction/write flow.
	e.ingestChan <- pt
}

// ============================================================================
// Pipeline Goroutines
// ============================================================================

// runPacker reads individual points, updates the real-time map, and buckets them.
func (e *Engine) runPacker() {
	defer e.wg.Done()
	defer close(e.chunkChan) // Close output channel when done

	ticker := time.NewTicker(e.config.ChunkFlushInterval)
	defer ticker.Stop()

	flushBuckets := func() {
		// Lock and snapshot current buckets
		e.bucketsMu.Lock()
		bucketsToFlush := e.activeBuckets
		e.activeBuckets = make(map[bucketKey][]DataPoint)
		e.bucketsMu.Unlock()

		// Keep flushed buckets queryable for a short period to avoid
		// visibility gaps between packer flush and disk commit.
		now := time.Now()
		pendingTTL := e.config.ChunkFlushInterval + time.Second
		if pendingTTL < 2*time.Second {
			pendingTTL = 2 * time.Second
		}

		e.pendingMu.Lock()
		for key, points := range bucketsToFlush {
			if len(points) == 0 {
				continue
			}
			e.pendingBuckets[key] = pendingBucket{
				points:    points,
				expiresAt: now.Add(pendingTTL),
			}
		}
		for key, pb := range e.pendingBuckets {
			if now.After(pb.expiresAt) {
				delete(e.pendingBuckets, key)
			}
		}
		e.pendingMu.Unlock()

		// Send snapshots to compactor
		for key, points := range bucketsToFlush {
			if len(points) > 0 {
				e.chunkChan <- RawBucket{
					SiteID:    key.siteID,
					TestID:    key.testID,
					StartTime: key.windowStart,
					Interval:  e.config.IntervalResolution,
					Points:    points,
				}
			}
		}
	}

	for {
		select {
		case _, ok := <-e.ingestChan:
			if !ok {
				// Channel closed, flush and exit
				flushBuckets()
				return
			}

		case <-ticker.C:
			// 3. Seal buckets and send to compactor
			flushBuckets()
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
		if len(raw.Points) == 0 {
			continue
		}

		// Work in nanoseconds to avoid rounding issues
		intervalNanos := int64(raw.Interval)
		if intervalNanos == 0 {
			intervalNanos = int64(time.Second) // Minimum 1 second interval
		}

		// Calculate number of time slots in this chunk
		numSlots := int(int64(e.config.ChunkFlushInterval) / intervalNanos)
		if numSlots == 0 {
			numSlots = 1
		}

		// Convert to seconds for timestamp calculations
		intervalSecs := intervalNanos / int64(time.Second)
		if intervalSecs == 0 {
			intervalSecs = 1
		}

		// Create a map of timestamp -> value for quick lookup
		pointMap := make(map[int64]int64)
		for _, pt := range raw.Points {
			// Calculate which slot this point belongs to
			offset := pt.Timestamp - raw.StartTime
			index := offset / intervalSecs
			slotTimestamp := raw.StartTime + (index * intervalSecs)
			pointMap[slotTimestamp] = pt.Value
		}

		// Build forward-filled array of values
		values := make([]int64, numSlots)
		lastValue := raw.Points[0].Value // Start with first actual value

		for i := 0; i < numSlots; i++ {
			slotTimestamp := raw.StartTime + (int64(i) * intervalSecs)
			if val, exists := pointMap[slotTimestamp]; exists {
				values[i] = val
				lastValue = val // Update last known value
			} else {
				// Forward-fill: use previous value
				values[i] = lastValue
			}
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

		// Determine tier name for initial storage
		tierName := "raw"
		if len(e.config.CompactionTiers) > 0 {
			tierName = e.config.CompactionTiers[0].Name
		}

		// Generate Keys
		testKey := makeTestKey(raw.TestID, raw.StartTime, tierName)
		siteKey := makeSiteKey(raw.SiteID, raw.TestID, raw.StartTime, tierName)

		// Serialize chunk header and data
		chunkBytes := serializeChunk(chunk)

		e.writeChan <- WritePayload{Key: testKey, Data: chunkBytes}
		e.writeChan <- WritePayload{Key: siteKey, Data: chunkBytes}
	}
}

// runWriter batches compressed payloads into BadgerDB to maximize I/O throughput.
func (e *Engine) runWriter() {
	defer e.wg.Done()

	wb := e.db.NewWriteBatch()
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
			// Periodic flush to ensure data is committed
			if err := wb.Flush(); err != nil {
				_ = err
			}
			wb = e.db.NewWriteBatch()

		case payload, ok := <-e.writeChan:
			if !ok {
				// Channel closed, flush remaining data and exit
				if err := wb.Flush(); err != nil {
					_ = err
				}
				return
			}

			err := wb.Set(payload.Key, payload.Data)
			if err != nil {
				// Try to flush current batch
				if flushErr := wb.Flush(); flushErr != nil {
					// In production, log this critical error
					_ = flushErr
				}

				// Create new batch and retry
				wb = e.db.NewWriteBatch()
				if retryErr := wb.Set(payload.Key, payload.Data); retryErr != nil {
					// In production, log this critical error - data may be lost
					_ = retryErr
				}
			}
		}
	}
}

// ============================================================================
// Public API: Real-Time Queries (O(1) Map Lookups)
// ============================================================================

func (e *Engine) GetLatestTest(siteID, testID int64) (DataPoint, bool) {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()

	if site, ok := e.latestValues[siteID]; ok {
		if pt, exists := site[testID]; exists {
			return pt, true
		}
	}
	return DataPoint{}, false
}

func (e *Engine) GetLatestSite(siteID int64) map[int64]DataPoint {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()

	result := make(map[int64]DataPoint)
	if site, ok := e.latestValues[siteID]; ok {
		for testID, pt := range site {
			result[testID] = pt
		}
	}
	return result
}

// ============================================================================
// Internal API: Unflushed Data Queries
// ============================================================================

// getUnflushedPoints returns unflushed points from activeBuckets for a specific test in a time range
func (e *Engine) getUnflushedPoints(testID int64, start, end int64) []DataPoint {
	e.bucketsMu.RLock()

	var results []DataPoint
	for key, points := range e.activeBuckets {
		if key.testID == testID {
			// Check if this bucket's time window overlaps with query range
			for _, pt := range points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					results = append(results, pt)
				}
			}
		}
	}
	e.bucketsMu.RUnlock()

	now := time.Now()
	e.pendingMu.RLock()
	for key, pb := range e.pendingBuckets {
		if pb.expiresAt.Before(now) {
			continue
		}
		if key.testID == testID {
			for _, pt := range pb.points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					results = append(results, pt)
				}
			}
		}
	}
	e.pendingMu.RUnlock()

	return results
}

// getUnflushedPointsForSite returns unflushed points for a specific site/test in a time range
func (e *Engine) getUnflushedPointsForSite(siteID, testID int64, start, end int64) []DataPoint {
	e.bucketsMu.RLock()

	var results []DataPoint
	for key, points := range e.activeBuckets {
		if key.siteID == siteID && key.testID == testID {
			// Check if this bucket's time window overlaps with query range
			for _, pt := range points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					results = append(results, pt)
				}
			}
		}
	}
	e.bucketsMu.RUnlock()

	now := time.Now()
	e.pendingMu.RLock()
	for key, pb := range e.pendingBuckets {
		if pb.expiresAt.Before(now) {
			continue
		}
		if key.siteID == siteID && key.testID == testID {
			for _, pt := range pb.points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					results = append(results, pt)
				}
			}
		}
	}
	e.pendingMu.RUnlock()

	return results
}

// getUnflushedPointsForSiteAll returns unflushed points for all tests at a specific site in a time range
func (e *Engine) getUnflushedPointsForSiteAll(siteID int64, start, end int64) []DataPoint {
	e.bucketsMu.RLock()

	var results []DataPoint
	for key, points := range e.activeBuckets {
		if key.siteID == siteID {
			// Check if this bucket's time window overlaps with query range
			for _, pt := range points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					results = append(results, pt)
				}
			}
		}
	}
	e.bucketsMu.RUnlock()

	now := time.Now()
	e.pendingMu.RLock()
	for key, pb := range e.pendingBuckets {
		if pb.expiresAt.Before(now) {
			continue
		}
		if key.siteID == siteID {
			for _, pt := range pb.points {
				if pt.Timestamp >= start && pt.Timestamp <= end {
					results = append(results, pt)
				}
			}
		}
	}
	e.pendingMu.RUnlock()

	return results
}

// ============================================================================
// Public API: Historical Queries (Badger Prefix Scans + Memory)
// ============================================================================

func (e *Engine) GetTestRange(testID int64, start, end int64) ([]DataPoint, error) {
	// Use map to deduplicate by timestamp (memory data overrides disk data)
	pointMap := make(map[int64]DataPoint)

	// 1. Get flushed data from disk
	err := e.db.View(func(txn *badger.Txn) error {
		// Smart tier selection based on query time range
		tiers := e.selectTiersForQuery(start, end)

		for _, tier := range tiers {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)

			prefix := []byte(fmt.Sprintf("test-%s!%016x!", tier, testID))
			startKey := []byte(fmt.Sprintf("test-%s!%016x!%016x", tier, testID, start))

			for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()

				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}

				chunk := deserializeChunk(valCopy)

				// Stop if chunk is completely past our end time
				if chunk.StartTime > end {
					break
				}

				// Use IDs from the chunk itself
				points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)

				// Filter points strictly inside [start, end]
				for _, pt := range points {
					if pt.Timestamp >= start && pt.Timestamp <= end {
						pointMap[pt.Timestamp] = pt
					}
				}
			}
			it.Close()
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// 2. Get unflushed data from memory (overrides disk data if same timestamp)
	memoryPoints := e.getUnflushedPoints(testID, start, end)
	for _, pt := range memoryPoints {
		pointMap[pt.Timestamp] = pt
	}

	// 3. Convert map to sorted slice
	results := make([]DataPoint, 0, len(pointMap))
	for _, pt := range pointMap {
		results = append(results, pt)
	}

	// Sort by timestamp
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].Timestamp > results[j].Timestamp {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results, nil
}

// GetSiteTestRange retrieves historical data for a specific test at a specific site.
func (e *Engine) GetSiteTestRange(siteID, testID int64, start, end int64) ([]DataPoint, error) {
	// Use map to deduplicate by timestamp (memory data overrides disk data)
	pointMap := make(map[int64]DataPoint)

	// 1. Get flushed data from disk
	err := e.db.View(func(txn *badger.Txn) error {
		// Smart tier selection based on query time range
		tiers := e.selectTiersForQuery(start, end)

		for _, tier := range tiers {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)

			prefix := []byte(fmt.Sprintf("site-%s!%016x!", tier, siteID))
			startKey := []byte(fmt.Sprintf("site-%s!%016x!%016x!%016x", tier, siteID, start, 0))

			for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()

				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}

				chunk := deserializeChunk(valCopy)

				// Stop if chunk is completely past our end time
				if chunk.StartTime > end {
					break
				}

				// Skip chunks that don't match our testID
				if chunk.TestID != testID {
					continue
				}

				points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)

				// Filter points for time range
				for _, pt := range points {
					if pt.Timestamp >= start && pt.Timestamp <= end {
						pointMap[pt.Timestamp] = pt
					}
				}
			}
			it.Close()
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// 2. Get unflushed data from memory (overrides disk data if same timestamp)
	memoryPoints := e.getUnflushedPointsForSite(siteID, testID, start, end)
	for _, pt := range memoryPoints {
		pointMap[pt.Timestamp] = pt
	}

	// 3. Convert map to sorted slice
	results := make([]DataPoint, 0, len(pointMap))
	for _, pt := range pointMap {
		results = append(results, pt)
	}

	// Sort by timestamp
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].Timestamp > results[j].Timestamp {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results, nil
}

// GetSiteRange retrieves historical data for all tests at a specific site.
func (e *Engine) GetSiteRange(siteID int64, start, end int64) ([]DataPoint, error) {
	// Use map keyed by (timestamp, testID) to deduplicate
	type pointKey struct {
		timestamp int64
		testID    int64
	}
	pointMap := make(map[pointKey]DataPoint)

	// 1. Get flushed data from disk
	err := e.db.View(func(txn *badger.Txn) error {
		// Smart tier selection based on query time range
		tiers := e.selectTiersForQuery(start, end)

		for _, tier := range tiers {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)

			prefix := []byte(fmt.Sprintf("site-%s!%016x!", tier, siteID))
			startKey := []byte(fmt.Sprintf("site-%s!%016x!%016x!%016x", tier, siteID, start, 0))

			for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()

				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}

				chunk := deserializeChunk(valCopy)

				// Stop if chunk is completely past our end time
				if chunk.StartTime > end {
					break
				}

				// Decode using IDs from the chunk
				points := decodeChunk(chunk.SiteID, chunk.TestID, chunk)

				// Filter points strictly inside [start, end]
				for _, pt := range points {
					if pt.Timestamp >= start && pt.Timestamp <= end {
						key := pointKey{timestamp: pt.Timestamp, testID: pt.TestID}
						pointMap[key] = pt
					}
				}
			}
			it.Close()
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// 2. Get unflushed data from memory (overrides disk data if same timestamp+testID)
	memoryPoints := e.getUnflushedPointsForSiteAll(siteID, start, end)
	for _, pt := range memoryPoints {
		key := pointKey{timestamp: pt.Timestamp, testID: pt.TestID}
		pointMap[key] = pt
	}

	// 3. Convert map to sorted slice
	results := make([]DataPoint, 0, len(pointMap))
	for _, pt := range pointMap {
		results = append(results, pt)
	}

	// Sort by timestamp, then testID
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].Timestamp > results[j].Timestamp ||
				(results[i].Timestamp == results[j].Timestamp && results[i].TestID > results[j].TestID) {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results, nil
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

// performCompaction finds and merges eligible chunks across all tiers
func (e *Engine) performCompaction() {
	if len(e.config.CompactionTiers) < 2 {
		return // Need at least 2 tiers to compact
	}

	// Compact each tier level (0->1, 1->2, etc.)
	for i := 0; i < len(e.config.CompactionTiers)-1; i++ {
		sourceTier := e.config.CompactionTiers[i]
		targetTier := e.config.CompactionTiers[i+1]

		e.compactTier(sourceTier, targetTier)
	}
}

// compactTier merges chunks from sourceTier into targetTier
func (e *Engine) compactTier(sourceTier, targetTier CompactionTier) {
	err := e.db.Update(func(txn *badger.Txn) error {
		// Track chunks to compact by (siteID, testID, target period)
		type chunkGroup struct {
			siteID      int64
			testID      int64
			targetStart int64
			chunks      []CompressedChunk
			timestamps  []int64
		}

		groups := make(map[string]*chunkGroup)
		cutoffTime := time.Now().Add(-sourceTier.MinAge).Unix()

		// Scan all chunks in source tier
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("test-%s!", sourceTier.Name))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Parse timestamp from key to check age
			var testID, timestamp int64
			_, err := fmt.Sscanf(string(key), "test-"+sourceTier.Name+"!%016x!%016x", &testID, &timestamp)
			if err != nil {
				continue
			}

			// Only compact chunks older than cutoff
			if timestamp > cutoffTime {
				continue
			}

			// Get chunk data
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			chunk := deserializeChunk(valCopy)

			// Timestamps are second-based, so normalize target duration to whole seconds.
			// Sub-second tiers collapse to 1 second to avoid division by zero.
			targetDurationSecs := int64(targetTier.Duration / time.Second)
			if targetDurationSecs <= 0 {
				targetDurationSecs = 1
			}
			targetStart := (timestamp / targetDurationSecs) * targetDurationSecs
			groupKey := fmt.Sprintf("%d:%d:%d", chunk.SiteID, chunk.TestID, targetStart)

			if _, exists := groups[groupKey]; !exists {
				groups[groupKey] = &chunkGroup{
					siteID:      chunk.SiteID,
					testID:      chunk.TestID,
					targetStart: targetStart,
				}
			}
			groups[groupKey].chunks = append(groups[groupKey].chunks, chunk)
			groups[groupKey].timestamps = append(groups[groupKey].timestamps, timestamp)
		}

		// Merge and write target tier chunks
		for _, group := range groups {
			if len(group.chunks) == 0 {
				continue
			}

			merged := e.mergeChunks(group.chunks, group.targetStart)

			// Write target tier chunk
			chunkBytes := serializeChunk(merged)
			testKey := makeTestKey(group.testID, group.targetStart, targetTier.Name)
			siteKey := makeSiteKey(group.siteID, group.testID, group.targetStart, targetTier.Name)

			if err := txn.Set(testKey, chunkBytes); err != nil {
				continue
			}
			if err := txn.Set(siteKey, chunkBytes); err != nil {
				continue
			}

			// Delete old source tier chunks
			for _, ts := range group.timestamps {
				testKey := makeTestKey(group.testID, ts, sourceTier.Name)
				siteKey := makeSiteKey(group.siteID, group.testID, ts, sourceTier.Name)
				txn.Delete(testKey)
				txn.Delete(siteKey)
			}
		}

		return nil
	})

	if err != nil {
		// In production, log this error
		_ = err
	}
}

// mergeChunks combines multiple chunks into a single larger chunk
func (e *Engine) mergeChunks(chunks []CompressedChunk, newStartTime int64) CompressedChunk {
	if len(chunks) == 0 {
		return CompressedChunk{}
	}

	// Collect all values from all chunks
	var allValues []int64

	for _, chunk := range chunks {
		// Decode the chunk using optimized decoder
		values := decodeValuesOptimized(chunk.Data, chunk.FirstVal, int(chunk.Count), chunk.CompressionType)
		allValues = append(allValues, values...)
	}

	if len(allValues) == 0 {
		return CompressedChunk{}
	}

	intervalSecs := int64(chunks[0].Interval.Seconds())
	if intervalSecs == 0 {
		intervalSecs = 1
	}

	// Create merged chunk
	merged := CompressedChunk{
		SiteID:          chunks[0].SiteID,
		TestID:          chunks[0].TestID,
		StartTime:       newStartTime,
		Interval:        chunks[0].Interval,
		Count:           uint32(len(allValues)),
		FirstVal:        allValues[0],
		LastVal:         allValues[len(allValues)-1],
		CompressionType: CompressionSnappy,
	}

	merged.EndTime = newStartTime + (int64(len(allValues)) * intervalSecs)

	// Encode using optimized encoding
	merged.Data = encodeValuesOptimized(allValues, merged.FirstVal)

	return merged
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
		return []string{"raw"}
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
		return []string{"raw"}
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
