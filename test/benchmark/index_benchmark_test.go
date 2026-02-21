package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/index"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/storage"
)

// BenchmarkIndexLookup measures the latency of looking up values in an index.
// Target: <10ms per lookup with 1M entries.
// Requirements: 16.3
func BenchmarkIndexLookup(b *testing.B) {
	// Create a temporary directory for the index
	dir, err := os.MkdirTemp("", "index-bench-lookup-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create storage
	storageDir, err := os.MkdirTemp("", "index-bench-storage-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create catalog
	catalogDir, err := os.MkdirTemp("", "index-bench-catalog-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(catalogDir)

	catalog, err := manifest.NewCatalog(filepath.Join(catalogDir, "manifest.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	// Create index builder
	builder := index.NewBuilder(st, catalog, dir, 64)

	// Create test partitions with known values
	numPartitions := 100
	numValuesPerPartition := 10000 // 1M total entries
	partitions := make([]*index.PartitionInfo, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)

		// Create SQLite file with test data
		dbPath := filepath.Join(dir, fmt.Sprintf("data_%s.sqlite", partitionID))
		if err := createTestDataPartition(dbPath, i, numValuesPerPartition); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		// Upload to storage
		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), dbPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, &index.PartitionInfo{
			PartitionID: partitionID,
			ObjectPath:  objectPath,
			RowCount:    int64(numValuesPerPartition),
		})
	}

	// Build the index
	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	indexInfos, err := builder.BuildIndex(context.Background(), "events", "device_id", partitions)
	buildTime := time.Since(start)

	if err != nil {
		b.Fatalf("Failed to build index: %v", err)
	}

	b.Logf("Index build time: %v for %d partitions with %d entries each", buildTime, numPartitions, numValuesPerPartition)
	b.Logf("Index partitions created: %d", len(indexInfos))

	// Now benchmark lookups
	lookup := index.NewLookup(st, catalog, dir, 64)

	// Pre-warm the cache
	for i := 0; i < 100; i++ {
		value := fmt.Sprintf("device_%06d", i%numValuesPerPartition)
		_, err := lookup.FindPartitions(context.Background(), "events", "device_id", value)
		if err != nil {
			b.Fatalf("Pre-warm lookup failed: %v", err)
		}
	}

	// Benchmark lookups
	b.ResetTimer()
	b.ReportAllocs()

	latencies := make([]time.Duration, 0, b.N)

	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("device_%06d", i%numValuesPerPartition)

		start := time.Now()
		partitionIDs, err := lookup.FindPartitions(context.Background(), "events", "device_id", value)
		latency := time.Since(start)

		if err != nil {
			b.Fatalf("Lookup failed: %v", err)
		}

		if len(partitionIDs) == 0 {
			b.Logf("No partitions found for %s", value)
		}

		latencies = append(latencies, latency)
	}

	// Calculate P50, P90, P99 latencies
	p50 := percentile(latencies, 50)
	p90 := percentile(latencies, 90)
	p99 := percentile(latencies, 99)

	b.ReportMetric(float64(p50.Microseconds()), "us_p50_latency")
	b.ReportMetric(float64(p90.Microseconds()), "us_p90_latency")
	b.ReportMetric(float64(p99.Microseconds()), "us_p99_latency")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lookups_per_sec")

	if p99 > 10*time.Millisecond {
		b.Logf("WARNING: P99 latency %v exceeds 10ms target", p99)
	}
}

// BenchmarkIndexBuild measures the time to build an index for 1K partitions.
// Target: <60s.
// Requirements: 16.3
func BenchmarkIndexBuild(b *testing.B) {
	// Create a temporary directory for the index
	dir, err := os.MkdirTemp("", "index-bench-build-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create storage
	storageDir, err := os.MkdirTemp("", "index-bench-storage-build-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create catalog
	catalogDir, err := os.MkdirTemp("", "index-bench-catalog-build-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(catalogDir)

	catalog, err := manifest.NewCatalog(filepath.Join(catalogDir, "manifest.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	// Create index builder
	builder := index.NewBuilder(st, catalog, dir, 64)

	// Create test partitions
	numPartitions := 1000
	numValuesPerPartition := 100
	partitions := make([]*index.PartitionInfo, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)

		// Create SQLite file with test data
		dbPath := filepath.Join(dir, fmt.Sprintf("data_%s.sqlite", partitionID))
		if err := createTestDataPartition(dbPath, i, numValuesPerPartition); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		// Upload to storage
		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), dbPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, &index.PartitionInfo{
			PartitionID: partitionID,
			ObjectPath:  objectPath,
			RowCount:    int64(numValuesPerPartition),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	indexInfos, err := builder.BuildIndex(context.Background(), "events", "device_id", partitions)
	elapsed := time.Since(start)

	if err != nil {
		b.Fatalf("Failed to build index: %v", err)
	}

	b.ReportMetric(elapsed.Seconds(), "s_total_time")
	b.ReportMetric(float64(len(partitions))/elapsed.Seconds(), "partitions_per_sec")
	b.Logf("Index build completed in %v for %d partitions", elapsed, numPartitions)
	b.Logf("Index partitions created: %d", len(indexInfos))

	if elapsed > 60*time.Second {
		b.Logf("WARNING: Index build time %v exceeds 60s target", elapsed)
	}
}

// BenchmarkIndexLookupCacheHit measures lookup performance with cached index.
func BenchmarkIndexLookupCacheHit(b *testing.B) {
	// Create a temporary directory for the index
	dir, err := os.MkdirTemp("", "index-bench-cache-hit-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create storage
	storageDir, err := os.MkdirTemp("", "index-bench-storage-cache-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create catalog
	catalogDir, err := os.MkdirTemp("", "index-bench-catalog-cache-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(catalogDir)

	catalog, err := manifest.NewCatalog(filepath.Join(catalogDir, "manifest.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	// Create index builder
	builder := index.NewBuilder(st, catalog, dir, 64)

	// Create test partitions
	numPartitions := 100
	numValuesPerPartition := 10000
	partitions := make([]*index.PartitionInfo, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)

		dbPath := filepath.Join(dir, fmt.Sprintf("data_%s.sqlite", partitionID))
		if err := createTestDataPartition(dbPath, i, numValuesPerPartition); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), dbPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, &index.PartitionInfo{
			PartitionID: partitionID,
			ObjectPath:  objectPath,
			RowCount:    int64(numValuesPerPartition),
		})
	}

	// Build the index
	_, err = builder.BuildIndex(context.Background(), "events", "device_id", partitions)
	if err != nil {
		b.Fatalf("Failed to build index: %v", err)
	}

	// Create lookup with cache
	lookup := index.NewLookup(st, catalog, dir, 64)

	// Pre-warm the cache completely
	for i := 0; i < numValuesPerPartition; i++ {
		value := fmt.Sprintf("device_%06d", i)
		lookup.FindPartitions(context.Background(), "events", "device_id", value)
	}

	// Now benchmark with cache hits
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("device_%06d", i%numValuesPerPartition)
		_, err := lookup.FindPartitions(context.Background(), "events", "device_id", value)
		if err != nil {
			b.Fatalf("Lookup failed: %v", err)
		}
	}

	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "lookups_per_sec")
	b.Logf("Cache hit lookup throughput: %.0f lookups/sec", throughput)
}

// Helper function to create a test data partition with device_id values
func createTestDataPartition(dbPath string, partitionIndex, numValues int) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY,
			device_id TEXT,
			event_type TEXT,
			timestamp INTEGER,
			payload TEXT
		)
	`)
	if err != nil {
		return err
	}

	// Insert test data
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO events (device_id, event_type, timestamp, payload) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < numValues; i++ {
		deviceID := fmt.Sprintf("device_%06d", (i+partitionIndex*numValues)%1000000)
		_, err := stmt.Exec(deviceID, "page_view", time.Now().UnixNano(), `{"key": "value"}`)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// Helper to create test row
func createTestRow(deviceID string) map[string]interface{} {
	return map[string]interface{}{
		"device_id":  deviceID,
		"event_type": "page_view",
		"timestamp":  time.Now().UnixNano(),
		"payload":    map[string]interface{}{"key": "value", "data": "test data for benchmark"},
	}
}