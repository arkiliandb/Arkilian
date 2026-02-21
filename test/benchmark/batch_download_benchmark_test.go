package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/storage"
)

// BenchmarkBatchDownload measures the performance of batch downloading partitions.
// Target: <2s for 30 partitions with concurrency 10 using local storage.
// Requirements: 16.4
func BenchmarkBatchDownload(b *testing.B) {
	// Create a temporary directory for downloads
	downloadDir, err := os.MkdirTemp("", "batch-download-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(downloadDir)

	// Create storage with test data
	storageDir, err := os.MkdirTemp("", "batch-download-storage-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create test partition files in storage
	numPartitions := 30
	partitionSize := 1024 * 1024 // 1MB per partition
	partitions := make([]string, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)
		partitionPath := filepath.Join(storageDir, fmt.Sprintf("%s.sqlite", partitionID))

		// Create a test file of specified size
		if err := createTestFile(partitionPath, partitionSize); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		// Upload to storage (for local storage, this just copies to the storage dir)
		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), partitionPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, objectPath)
	}

	// Create batch downloader
	cacheDir, err := os.MkdirTemp("", "batch-download-cache-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	downloader := storage.NewBatchDownloader(st, 10, cacheDir, nil)

	b.ResetTimer()
	b.ReportAllocs()

	// Run the batch download benchmark
	for i := 0; i < b.N; i++ {
		// Create request with all partitions
		req := &storage.BatchRequest{
			ObjectPaths: partitions,
			Priority:    make([]int, len(partitions)),
		}

		// Set priority: first 10 are critical (0), rest are prefetch (1)
		for j := range req.Priority {
			if j < 10 {
				req.Priority[j] = 0 // critical
			} else {
				req.Priority[j] = 1 // prefetch
			}
		}

		result, err := downloader.Download(context.Background(), req)
		if err != nil {
			b.Fatalf("Batch download failed: %v", err)
		}

		if len(result.LocalPaths) != numPartitions {
			b.Fatalf("Expected %d downloads, got %d", numPartitions, len(result.LocalPaths))
		}

		if len(result.Errors) > 0 {
			b.Fatalf("Download errors: %v", result.Errors)
		}
	}

	elapsed := b.Elapsed()
	throughput := float64(numPartitions*b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "partitions_per_sec")
	b.ReportMetric(elapsed.Seconds(), "s_total_time")
	b.Logf("Batch download throughput: %.2f partitions/sec", throughput)

	if elapsed > 2*time.Second {
		b.Logf("WARNING: Batch download time %v exceeds 2s target for %d partitions", elapsed, numPartitions)
	}
}

// BenchmarkBatchDownloadWithCache measures performance when files are already cached.
func BenchmarkBatchDownloadWithCache(b *testing.B) {
	// Create a temporary directory for downloads
	downloadDir, err := os.MkdirTemp("", "batch-download-cache-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(downloadDir)

	// Create storage with test data
	storageDir, err := os.MkdirTemp("", "batch-download-storage-cache-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create test partition files in storage
	numPartitions := 30
	partitionSize := 1024 * 1024 // 1MB per partition
	partitions := make([]string, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)
		partitionPath := filepath.Join(storageDir, fmt.Sprintf("%s.sqlite", partitionID))

		if err := createTestFile(partitionPath, partitionSize); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), partitionPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, objectPath)
	}

	// Create cache directory and pre-populate it
	cacheDir, err := os.MkdirTemp("", "batch-download-prepopulated-cache-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	// Pre-populate cache with all files
	for _, objectPath := range partitions {
		// Download each file to cache using the same path calculation as the batch downloader
		localPath := filepath.Join(cacheDir, hashFileName(objectPath))
		if err := st.Download(context.Background(), objectPath, localPath); err != nil {
			b.Fatalf("Failed to pre-populate cache: %v", err)
		}
	}

	// Create batch downloader with pre-populated cache
	downloader := storage.NewBatchDownloader(st, 10, cacheDir, nil)

	b.ResetTimer()
	b.ReportAllocs()

	// Run the benchmark - should be all cache hits
	for i := 0; i < b.N; i++ {
		req := &storage.BatchRequest{
			ObjectPaths: partitions,
			Priority:    make([]int, len(partitions)),
		}

		result, err := downloader.Download(context.Background(), req)
		if err != nil {
			b.Fatalf("Batch download failed: %v", err)
		}

		if result.CacheHits != numPartitions {
			b.Fatalf("Expected %d cache hits, got %d", numPartitions, result.CacheHits)
		}
	}

	elapsed := b.Elapsed()
	throughput := float64(numPartitions*b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "partitions_per_sec")
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "batches_per_sec")
	b.Logf("Cache hit batch download throughput: %.2f partitions/sec", throughput)
	b.Logf("Cache hits per batch: %d", numPartitions)
}

// BenchmarkBatchDownloadConcurrency measures performance at different concurrency levels.
func BenchmarkBatchDownloadConcurrency(b *testing.B) {
	// Create storage with test data
	storageDir, err := os.MkdirTemp("", "batch-download-concurrency-storage-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create test partition files
	numPartitions := 50
	partitionSize := 1024 * 1024 // 1MB per partition
	partitions := make([]string, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)
		partitionPath := filepath.Join(storageDir, fmt.Sprintf("%s.sqlite", partitionID))

		if err := createTestFile(partitionPath, partitionSize); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), partitionPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, objectPath)
	}

	// Test different concurrency levels
	concurrencyLevels := []int{1, 5, 10, 20, 50}

	for _, conc := range concurrencyLevels {
		b.Run(fmt.Sprintf("%d concurrent", conc), func(b *testing.B) {
			cacheDir, err := os.MkdirTemp("", fmt.Sprintf("batch-download-concurrency-%d-*", conc))
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(cacheDir)

			downloader := storage.NewBatchDownloader(st, conc, cacheDir, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				req := &storage.BatchRequest{
					ObjectPaths: partitions,
					Priority:    make([]int, len(partitions)),
				}

				result, err := downloader.Download(context.Background(), req)
				if err != nil {
					b.Fatalf("Batch download failed: %v", err)
				}

				if len(result.LocalPaths) != numPartitions {
					b.Fatalf("Expected %d downloads, got %d", numPartitions, len(result.LocalPaths))
				}
			}

			elapsed := b.Elapsed()
			throughput := float64(numPartitions*b.N) / elapsed.Seconds()
			b.ReportMetric(throughput, "partitions_per_sec")
			b.ReportMetric(elapsed.Seconds(), "s_total_time")
		})
	}
}

// BenchmarkBatchDownloadPartialFailure measures behavior when some downloads fail.
func BenchmarkBatchDownloadPartialFailure(b *testing.B) {
	// Create storage with test data
	storageDir, err := os.MkdirTemp("", "batch-download-partial-storage-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(storageDir)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create some valid partitions and some invalid paths
	numValidPartitions := 20
	partitionSize := 1024 * 1024 // 1MB per partition
	partitions := make([]string, 0, numValidPartitions+5) // 5 invalid paths

	for i := 0; i < numValidPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%06d", i)
		partitionPath := filepath.Join(storageDir, fmt.Sprintf("%s.sqlite", partitionID))

		if err := createTestFile(partitionPath, partitionSize); err != nil {
			b.Fatalf("Failed to create test partition: %v", err)
		}

		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		if err := st.Upload(context.Background(), partitionPath, objectPath); err != nil {
			b.Fatalf("Failed to upload partition: %v", err)
		}

		partitions = append(partitions, objectPath)
	}

	// Add some invalid paths
	partitions = append(partitions, "nonexistent/partition_999999.sqlite")
	partitions = append(partitions, "invalid/path.sqlite")
	partitions = append(partitions, "missing/file.sqlite")
	partitions = append(partitions, "another/missing.sqlite")
	partitions = append(partitions, "still/more/missing.sqlite")

	cacheDir, err := os.MkdirTemp("", "batch-download-partial-cache-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	downloader := storage.NewBatchDownloader(st, 10, cacheDir, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := &storage.BatchRequest{
			ObjectPaths: partitions,
			Priority:    make([]int, len(partitions)),
		}

		result, err := downloader.Download(context.Background(), req)
		if err != nil {
			b.Fatalf("Batch download returned error: %v", err)
		}

		// Should have successful downloads and errors
		if len(result.LocalPaths) != numValidPartitions {
			b.Fatalf("Expected %d successful downloads, got %d", numValidPartitions, len(result.LocalPaths))
		}

		if len(result.Errors) != 5 {
			b.Fatalf("Expected 5 errors, got %d", len(result.Errors))
		}
	}

	elapsed := b.Elapsed()
	throughput := float64((numValidPartitions)*b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "partitions_per_sec")
	b.Logf("Partial failure test: %d successful, %d failed per batch", numValidPartitions, 5)
}

// Helper function to create a test file of specified size
func createTestFile(path string, size int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write random-ish data to fill the file
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	written := 0
	for written < size {
		toWrite := size - written
		if toWrite > len(data) {
			toWrite = len(data)
		}
		if _, err := file.Write(data[:toWrite]); err != nil {
			return err
		}
		written += toWrite
	}

	return nil
}

// Helper to create test partition info
func createTestPartitionInfos(count int) []string {
	partitions := make([]string, count)
	for i := 0; i < count; i++ {
		partitions[i] = fmt.Sprintf("partitions/2024/01/%02d/partition_%06d.sqlite", i%31, i)
	}
	return partitions
}

// hashFileName creates a unique filename from an object path (copied from batch_downloader.go)
func hashFileName(objectPath string) string {
	// Replace / with _ for short paths
	result := filepath.FromSlash(objectPath)
	if len(result) <= 100 {
		return result
	}
	// Use hash for long paths
	return fmt.Sprintf("%x", hashString(objectPath))
}

func hashString(s string) uint64 {
	var h uint64
	for _, c := range s {
		h = h*31 + uint64(c)
	}
	return h
}