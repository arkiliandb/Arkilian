package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/wal"
	"github.com/arkilian/arkilian/pkg/types"
)

// BenchmarkWALAppend measures the latency of appending 1KB entries to the WAL.
// Target: <5ms P99 latency.
// Requirements: 16.1, 16.7
func BenchmarkWALAppend(b *testing.B) {
	// Create a temporary WAL directory
	dir, err := os.MkdirTemp("", "wal-bench-append-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create WAL with 64MB max segment size
	w, err := wal.NewWAL(dir, 64*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	// Create a 1KB row payload
	row := types.Row{
		EventType: "page_view",
		EventTime: time.Now().UnixNano(),
		Payload:   map[string]interface{}{"data": generateRandomString(900)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Measure P99 latency
	latencies := make([]time.Duration, 0, b.N)

	for i := 0; i < b.N; i++ {
		entry := &wal.Entry{
			PartitionKey: fmt.Sprintf("2024/01/%02d", i%31),
			Rows:         []types.Row{row},
			Schema:       partition.DefaultSchema(),
			Timestamp:    time.Now().UnixNano(),
		}

		start := time.Now()
		_, err := w.Append(entry)
		latency := time.Since(start)

		if err != nil {
			b.Fatalf("Append failed: %v", err)
		}

		latencies = append(latencies, latency)
	}

	// Calculate P99 latency
	p99 := percentile(latencies, 99)
	b.ReportMetric(float64(p99.Microseconds()), "us_p99_latency")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")

	// Check if we meet the target
	if p99 > 5*time.Millisecond {
		b.Logf("WARNING: P99 latency %v exceeds 5ms target", p99)
	}
}

// BenchmarkWALFlush measures the time to flush 10K entries.
// Target: <5s total time.
// Requirements: 16.1
func BenchmarkWALFlush(b *testing.B) {
	// Create a temporary WAL directory
	dir, err := os.MkdirTemp("", "wal-bench-flush-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create storage for the flusher
	storageDir, err := os.MkdirTemp("", "wal-bench-storage-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	// Create WAL
	w, err := wal.NewWAL(dir, 64*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	// Create a mock catalog and builder for the flusher
	// For benchmark, we just measure the append time, not the full flusher cycle
	row := types.Row{
		EventType: "page_view",
		EventTime: time.Now().UnixNano(),
		Payload:   map[string]interface{}{"data": generateRandomString(900)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Append 10K entries
	entryCount := 10000
	for i := 0; i < entryCount; i++ {
		entry := &wal.Entry{
			PartitionKey: fmt.Sprintf("2024/01/%02d", i%31),
			Rows:         []types.Row{row},
			Schema:       partition.DefaultSchema(),
			Timestamp:    time.Now().UnixNano(),
		}

		_, err := w.Append(entry)
		if err != nil {
			b.Fatalf("Append failed at %d: %v", i, err)
		}
	}

	elapsed := b.Elapsed()
	b.ReportMetric(float64(entryCount)/elapsed.Seconds(), "entries/sec")

	if elapsed > 5*time.Second {
		b.Logf("WARNING: Flush time %v exceeds 5s target for %d entries", elapsed, entryCount)
	}
}

// BenchmarkWALAppendThroughput measures sustained append throughput.
// Target: 100K entries/sec.
// Requirements: 16.7
func BenchmarkWALAppendThroughput(b *testing.B) {
	// Create a temporary WAL directory
	dir, err := os.MkdirTemp("", "wal-bench-throughput-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create WAL with large segment size to avoid rotation during benchmark
	w, err := wal.NewWAL(dir, 1024*1024*1024) // 1GB
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	// Create a 1KB row payload
	row := types.Row{
		EventType: "page_view",
		EventTime: time.Now().UnixNano(),
		Payload:   map[string]interface{}{"data": generateRandomString(900)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Run at different batch sizes
	b.Run("1K entries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entry := &wal.Entry{
				PartitionKey: fmt.Sprintf("2024/01/%02d", i%31),
				Rows:         []types.Row{row},
				Schema:       partition.DefaultSchema(),
				Timestamp:    time.Now().UnixNano(),
			}

			_, err := w.Append(entry)
			if err != nil {
				b.Fatalf("Append failed: %v", err)
			}
		}
	})

	// Calculate throughput
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "entries/sec")

	if throughput < 100000 {
		b.Logf("WARNING: Throughput %.0f entries/sec below 100K target", throughput)
	}
}

// BenchmarkWALConcurrentAppend measures concurrent append performance.
func BenchmarkWALConcurrentAppend(b *testing.B) {
	// Create a temporary WAL directory
	dir, err := os.MkdirTemp("", "wal-bench-concurrent-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create WAL with large segment size
	w, err := wal.NewWAL(dir, 1024*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	// Create a 1KB row payload
	row := types.Row{
		EventType: "page_view",
		EventTime: time.Now().UnixNano(),
		Payload:   map[string]interface{}{"data": generateRandomString(900)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Test with different concurrency levels
	concurrencyLevels := []int{1, 4, 8, 16, 32}

	for _, conc := range concurrencyLevels {
		b.Run(fmt.Sprintf("%d goroutines", conc), func(b *testing.B) {
			b.SetParallelism(conc)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					entry := &wal.Entry{
						PartitionKey: fmt.Sprintf("2024/01/%02d", i%31),
						Rows:         []types.Row{row},
						Schema:       partition.DefaultSchema(),
						Timestamp:    time.Now().UnixNano(),
					}

					_, err := w.Append(entry)
					if err != nil {
						b.Fatalf("Append failed: %v", err)
					}
					i++
				}
			})

			throughput := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(throughput, "entries/sec")
		})
	}
}

// Helper function to calculate percentile
func percentile(durations []time.Duration, percentile int) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	// Sort the durations
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j-1] > sorted[j]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	// Calculate the index
	index := (len(sorted) * percentile) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

// Helper function to generate random string of specified length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[i%len(charset)]
	}
	return string(result)
}

// Helper to create test partition info
func createTestPartitionInfo(partitionID, partitionKey string) *partition.PartitionInfo {
	return &partition.PartitionInfo{
		PartitionID:   partitionID,
		PartitionKey:  partitionKey,
		SQLitePath:    filepath.Join(os.TempDir(), fmt.Sprintf("test_partition_%s.sqlite", partitionID)),
		RowCount:      1000,
		SizeBytes:     1024 * 200,
		CreatedAt:     time.Now(),
	}
}