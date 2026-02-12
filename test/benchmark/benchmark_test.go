// Package benchmark provides performance benchmarks for Project Arkilian
package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// BenchmarkRowIngestion measures row ingestion throughput
// Target: 50,000 rows per second per node (Requirement 16.1)
func BenchmarkRowIngestion(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "arkilian-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	builder := partition.NewBuilder(tmpDir, 0)
	ctx := context.Background()

	// Generate test rows
	rows := generateTestRows(1000)

	b.ResetTimer()
	b.ReportAllocs()

	totalRows := 0
	for i := 0; i < b.N; i++ {
		key := types.PartitionKey{
			Strategy: "time",
			Value:    fmt.Sprintf("2026020%d", i%10),
		}
		_, err := builder.Build(ctx, rows, key)
		if err != nil {
			b.Fatal(err)
		}
		totalRows += len(rows)
	}

	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkSQLParsing measures SQL parsing performance
func BenchmarkSQLParsing(b *testing.B) {
	queries := []string{
		"SELECT * FROM events WHERE tenant_id = 'acme'",
		"SELECT user_id, COUNT(*) FROM events WHERE tenant_id = 'acme' GROUP BY user_id",
		"SELECT * FROM events WHERE event_time BETWEEN 1000 AND 2000 ORDER BY event_time LIMIT 100",
		"SELECT tenant_id, user_id, SUM(amount) FROM events WHERE event_type = 'purchase' GROUP BY tenant_id, user_id",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := parser.Parse(query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBloomFilterLookup measures bloom filter lookup performance
func BenchmarkBloomFilterLookup(b *testing.B) {
	// Create a bloom filter with 10,000 items
	filter := bloom.New(100000, 7)
	for i := 0; i < 10000; i++ {
		filter.Add([]byte(fmt.Sprintf("tenant_%d", i)))
	}

	testKey := []byte("tenant_5000")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		filter.Contains(testKey)
	}
}

// BenchmarkBloomFilterFalsePositiveRate measures actual FPR
// Target: 1% or less (Requirement 3.4)
func BenchmarkBloomFilterFalsePositiveRate(b *testing.B) {
	// Create filter with 10,000 items targeting 1% FPR
	numItems := 10000
	targetFPR := 0.01
	numBits, numHashes := bloom.OptimalParameters(numItems, targetFPR)
	filter := bloom.New(numBits, numHashes)

	// Add items
	for i := 0; i < numItems; i++ {
		filter.Add([]byte(fmt.Sprintf("item_%d", i)))
	}

	// Test false positives with non-member items
	falsePositives := 0
	testCount := 100000
	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("nonmember_%d", i))
		if filter.Contains(key) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testCount)
	b.ReportMetric(actualFPR*100, "FPR%")

	if actualFPR > 0.011 { // Allow 10% margin
		b.Errorf("False positive rate %.4f exceeds target 1.1%%", actualFPR)
	}
}

// BenchmarkManifestPruning measures partition pruning performance
func BenchmarkManifestPruning(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "arkilian-bench-manifest-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "manifest.db")
	catalog, err := manifest.NewCatalog(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Register 1000 partitions
	for i := 0; i < 1000; i++ {
		minTime := int64(i * 1000)
		maxTime := int64((i + 1) * 1000)
		minUser := int64(i * 100)
		maxUser := int64((i + 1) * 100)

		info := &partition.PartitionInfo{
			PartitionID:  fmt.Sprintf("part_%d", i),
			PartitionKey: fmt.Sprintf("2026020%d", i%10),
			RowCount:     1000,
			SizeBytes:    1024 * 1024,
			MinMaxStats: map[string]partition.MinMax{
				"event_time": {Min: minTime, Max: maxTime},
				"user_id":    {Min: minUser, Max: maxUser},
			},
			SchemaVersion: 1,
			CreatedAt:     time.Now(),
		}
		err := catalog.RegisterPartition(ctx, info, fmt.Sprintf("s3://bucket/part_%d.sqlite", i))
		if err != nil {
			b.Fatal(err)
		}
	}

	predicates := []manifest.Predicate{
		{Column: "event_time", Operator: ">=", Value: int64(500000)},
		{Column: "event_time", Operator: "<=", Value: int64(510000)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := catalog.FindPartitions(ctx, predicates)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMetadataSerialization measures metadata JSON serialization
func BenchmarkMetadataSerialization(b *testing.B) {
	minTime := int64(1000)
	maxTime := int64(2000)
	minUser := int64(100)
	maxUser := int64(200)
	minTenant := "acme"
	maxTenant := "zeta"

	meta := &partition.MetadataSidecar{
		PartitionID:   "events:20260206:001",
		SchemaVersion: 1,
		Stats: partition.PartitionStats{
			RowCount:     10000,
			SizeBytes:    12400000,
			MinEventTime: &minTime,
			MaxEventTime: &maxTime,
			MinUserID:    &minUser,
			MaxUserID:    &maxUser,
			MinTenantID:  &minTenant,
			MaxTenantID:  &maxTenant,
		},
		BloomFilters: map[string]*partition.BloomFilterMeta{
			"tenant_id": {
				Algorithm:  "murmur3_128",
				NumBits:    100000,
				NumHashes:  7,
				Base64Data: "SGVsbG8gV29ybGQ=",
			},
			"user_id": {
				Algorithm:  "murmur3_128",
				NumBits:    100000,
				NumHashes:  7,
				Base64Data: "SGVsbG8gV29ybGQ=",
			},
		},
		CreatedAt: time.Now().Unix(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(meta)
		if err != nil {
			b.Fatal(err)
		}
		var decoded partition.MetadataSidecar
		if err := json.Unmarshal(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkULIDGeneration measures ULID generation throughput
func BenchmarkULIDGeneration(b *testing.B) {
	gen := types.NewULIDGenerator()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := gen.Generate()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLocalStorageUploadDownload measures storage operations
func BenchmarkLocalStorageUploadDownload(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "arkilian-bench-storage-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	localStorage, err := storage.NewLocalStorage(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create a test file
	testFile := filepath.Join(tmpDir, "test_source.dat")
	testData := make([]byte, 1024*1024) // 1MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		objectPath := fmt.Sprintf("test_%d.dat", i)
		if err := localStorage.Upload(ctx, testFile, objectPath); err != nil {
			b.Fatal(err)
		}

		downloadPath := filepath.Join(tmpDir, fmt.Sprintf("download_%d.dat", i))
		if err := localStorage.Download(ctx, objectPath, downloadPath); err != nil {
			b.Fatal(err)
		}
	}
}

// generateTestRows creates test rows for benchmarking
func generateTestRows(count int) []types.Row {
	gen := types.NewULIDGenerator()
	rows := make([]types.Row, count)
	baseTime := time.Now().Unix()

	for i := 0; i < count; i++ {
		ulid, _ := gen.Generate()
		rows[i] = types.Row{
			EventID:   ulid.Bytes(),
			TenantID:  fmt.Sprintf("tenant_%d", i%100),
			UserID:    int64(i % 1000),
			EventTime: baseTime + int64(i),
			EventType: "test_event",
			Payload:   map[string]interface{}{"index": i, "data": "test payload data"},
		}
	}
	return rows
}
