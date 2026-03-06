package tsdb

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkSingleThreadedIngestion measures single-threaded write throughput
func BenchmarkSingleThreadedIngestion(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   100000,
		CompactorWorkers:   4,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
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
	start := time.Now()

	for i := 0; i < b.N; i++ {
		engine.Add(now+int64(i), 100, 5001, int64(i))
	}

	elapsed := time.Since(start)
	rate := float64(b.N) / elapsed.Seconds()

	b.ReportMetric(rate, "inserts/sec")
	b.Logf("Single-threaded: %.0f inserts/sec", rate)
}

// BenchmarkMultiThreadedIngestion measures concurrent write throughput
func BenchmarkMultiThreadedIngestion(b *testing.B) {
	for _, numGoroutines := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", numGoroutines), func(b *testing.B) {
			tmpDir := b.TempDir()
			cfg := Config{
				DBPath:             filepath.Join(tmpDir, "bench.db"),
				ChunkFlushInterval: 1 * time.Second,
				IntervalResolution: 100 * time.Millisecond,
				IngestBufferSize:   200000,
				CompactorWorkers:   4,
				CompactionTiers: []CompactionTier{
					{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
				},
				CompactionInterval:   10 * time.Second,
				EnableAutoCompaction: false,
			}

			engine, err := NewEngine(cfg)
			if err != nil {
				b.Fatalf("Failed to create engine: %v", err)
			}

			engine.Start()
			defer engine.Stop()

			now := time.Now().Unix()
			var counter int64

			b.ResetTimer()
			start := time.Now()

			var wg sync.WaitGroup
			insertsPerGoroutine := b.N / numGoroutines

			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					siteID := int64(100 + goroutineID)
					testID := int64(5000 + goroutineID)

					for i := 0; i < insertsPerGoroutine; i++ {
						ts := now + int64(i)
						engine.Add(ts, siteID, testID, int64(i))
						atomic.AddInt64(&counter, 1)
					}
				}(g)
			}

			wg.Wait()
			elapsed := time.Since(start)
			rate := float64(counter) / elapsed.Seconds()

			b.ReportMetric(rate, "inserts/sec")
			b.Logf("%d goroutines: %.0f inserts/sec (%.2fx vs single-threaded)",
				numGoroutines, rate, rate/50000.0)
		})
	}
}

// BenchmarkSustainedLoad measures throughput over longer duration
func BenchmarkSustainedLoad(b *testing.B) {
	durations := []time.Duration{5 * time.Second, 10 * time.Second}

	for _, duration := range durations {
		b.Run(fmt.Sprintf("duration=%s", duration), func(b *testing.B) {
			tmpDir := b.TempDir()
			cfg := Config{
				DBPath:             filepath.Join(tmpDir, "bench.db"),
				ChunkFlushInterval: 1 * time.Second,
				IntervalResolution: 100 * time.Millisecond,
				IngestBufferSize:   200000,
				CompactorWorkers:   4,
				CompactionTiers: []CompactionTier{
					{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
				},
				CompactionInterval:   10 * time.Second,
				EnableAutoCompaction: false,
			}

			engine, err := NewEngine(cfg)
			if err != nil {
				b.Fatalf("Failed to create engine: %v", err)
			}

			engine.Start()
			defer engine.Stop()

			var counter int64
			now := time.Now().Unix()
			stop := make(chan bool)

			// 8 concurrent writers
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					siteID := int64(100 + goroutineID)
					testID := int64(5000 + goroutineID)
					i := int64(0)

					for {
						select {
						case <-stop:
							return
						default:
							ts := now + i
							engine.Add(ts, siteID, testID, i)
							atomic.AddInt64(&counter, 1)
							i++
						}
					}
				}(g)
			}

			// Run for specified duration
			b.ResetTimer()
			start := time.Now()
			time.Sleep(duration)
			close(stop)
			wg.Wait()
			elapsed := time.Since(start)

			count := atomic.LoadInt64(&counter)
			rate := float64(count) / elapsed.Seconds()

			b.ReportMetric(rate, "inserts/sec")
			b.Logf("Sustained load (%s): %d total inserts, %.0f inserts/sec",
				duration, count, rate)
		})
	}
}

// BenchmarkWithBackpressure measures behavior when channel buffers fill
func BenchmarkWithBackpressure(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   10000, // Smaller buffer to create backpressure
		CompactorWorkers:   2,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
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
	start := time.Now()

	// Write more than buffer size to test backpressure
	for i := 0; i < b.N; i++ {
		engine.Add(now+int64(i), 100, 5001, int64(i))
	}

	elapsed := time.Since(start)
	rate := float64(b.N) / elapsed.Seconds()

	b.ReportMetric(rate, "inserts/sec")
	b.Logf("With backpressure: %.0f inserts/sec", rate)
}

// BenchmarkRealTimeQueryWhileWriting measures query performance during writes
func BenchmarkRealTimeQueryWhileWriting(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   100000,
		CompactorWorkers:   4,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	now := time.Now().Unix()
	stop := make(chan bool)

	// Background writer
	var writeCounter int64
	go func() {
		i := int64(0)
		for {
			select {
			case <-stop:
				return
			default:
				engine.Add(now+i, 100, 5001, i)
				atomic.AddInt64(&writeCounter, 1)
				i++
			}
		}
	}()

	// Let some data accumulate
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	// Benchmark short-range queries while writes are in flight.
	for i := 0; i < b.N; i++ {
		_, _ = engine.GetSiteTestRange(100, 5001, now, now+1, Scale5m)
	}

	elapsed := time.Since(start)
	close(stop)

	queryRate := float64(b.N) / elapsed.Seconds()
	writeRate := float64(atomic.LoadInt64(&writeCounter)) / elapsed.Seconds()

	b.ReportMetric(queryRate, "queries/sec")
	b.Logf("Short-range queries: %.0f queries/sec while writing at %.0f inserts/sec",
		queryRate, writeRate)
}

// TestThroughputReport generates a comprehensive throughput report
func TestThroughputReport(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	tmpDir := t.TempDir()
	cfg := Config{
		DBPath:             filepath.Join(tmpDir, "bench.db"),
		ChunkFlushInterval: 1 * time.Second,
		IntervalResolution: 100 * time.Millisecond,
		IngestBufferSize:   200000,
		CompactorWorkers:   4,
		CompactionTiers: []CompactionTier{
			{Name: "1s", Duration: 1 * time.Second, MinAge: 10 * time.Second},
		},
		CompactionInterval:   10 * time.Second,
		EnableAutoCompaction: false,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer engine.Stop()

	fmt.Println("\n=== TSDB Throughput Report ===")

	// Test 1: Single-threaded burst
	{
		now := time.Now().Unix()
		count := 100000
		start := time.Now()

		for i := 0; i < count; i++ {
			engine.Add(now+int64(i), 100, 5001, int64(i))
		}

		elapsed := time.Since(start)
		rate := float64(count) / elapsed.Seconds()
		fmt.Printf("Single-threaded burst:     %10.0f inserts/sec (%d inserts in %v)\n",
			rate, count, elapsed.Round(time.Millisecond))
	}

	// Test 2: Multi-threaded burst
	{
		now := time.Now().Unix()
		numGoroutines := 8
		insertsPerGoroutine := 50000
		totalInserts := numGoroutines * insertsPerGoroutine

		var wg sync.WaitGroup
		start := time.Now()

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				siteID := int64(200 + goroutineID)
				testID := int64(6000 + goroutineID)

				for i := 0; i < insertsPerGoroutine; i++ {
					engine.Add(now+int64(i), siteID, testID, int64(i))
				}
			}(g)
		}

		wg.Wait()
		elapsed := time.Since(start)
		rate := float64(totalInserts) / elapsed.Seconds()
		fmt.Printf("Multi-threaded burst (8):  %10.0f inserts/sec (%d inserts in %v)\n",
			rate, totalInserts, elapsed.Round(time.Millisecond))
	}

	// Test 3: Sustained load
	{
		duration := 10 * time.Second
		numGoroutines := 8
		var counter int64
		now := time.Now().Unix()
		stop := make(chan bool)

		var wg sync.WaitGroup
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				siteID := int64(300 + goroutineID)
				testID := int64(7000 + goroutineID)
				i := int64(0)

				for {
					select {
					case <-stop:
						return
					default:
						engine.Add(now+i, siteID, testID, i)
						atomic.AddInt64(&counter, 1)
						i++
					}
				}
			}(g)
		}

		start := time.Now()
		time.Sleep(duration)
		close(stop)
		wg.Wait()
		elapsed := time.Since(start)

		count := atomic.LoadInt64(&counter)
		rate := float64(count) / elapsed.Seconds()
		fmt.Printf("Sustained load (10s, 8T):  %10.0f inserts/sec (%d total inserts)\n",
			rate, count)
	}

	// Test 4: Query throughput
	{
		// Add some data first
		now := time.Now().Unix()
		for i := 0; i < 1000; i++ {
			engine.Add(now+int64(i), 400, 8001, int64(i))
		}
		time.Sleep(100 * time.Millisecond)

		count := 100000
		start := time.Now()

		for i := 0; i < count; i++ {
			_, _ = engine.GetSiteTestRange(400, 8001, now, now+999, Scale5m)
		}

		elapsed := time.Since(start)
		rate := float64(count) / elapsed.Seconds()
		fmt.Printf("Historical query rate:     %10.0f queries/sec\n", rate)
	}

	fmt.Println("\n=== Configuration ===")
	fmt.Printf("IngestBufferSize:   %d\n", cfg.IngestBufferSize)
	fmt.Printf("CompactorWorkers:   %d\n", cfg.CompactorWorkers)
	fmt.Printf("ChunkFlushInterval: %v\n", cfg.ChunkFlushInterval)
	fmt.Printf("IntervalResolution: %v\n", cfg.IntervalResolution)
	fmt.Println("")
}
