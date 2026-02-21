package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestBatchDownloader_BasicDownload(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	cacheDir := t.TempDir()
	downloader := NewBatchDownloader(storage, 3, cacheDir, nil)

	ctx := context.Background()

	// Create test files in storage
	paths := []string{"obj1.txt", "obj2.txt", "obj3.txt", "obj4.txt", "obj5.txt",
		"obj6.txt", "obj7.txt", "obj8.txt", "obj9.txt", "obj10.txt"}
	content := []byte("test content")

	for _, p := range paths {
		srcPath := filepath.Join(cacheDir, "src_"+p)
		if err := os.WriteFile(srcPath, content, 0644); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
		if err := storage.Upload(ctx, srcPath, p); err != nil {
			t.Fatalf("Upload failed for %s: %v", p, err)
		}
	}

	// Download all files
	req := &BatchRequest{
		ObjectPaths: paths,
		Priority:    make([]int, len(paths)), // all priority 0
	}

	result, err := downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// Verify all downloads succeeded
	if len(result.LocalPaths) != len(paths) {
		t.Errorf("expected %d local paths, got %d", len(paths), len(result.LocalPaths))
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %v", result.Errors)
	}
	if result.CacheHits != 0 {
		t.Errorf("expected 0 cache hits, got %d", result.CacheHits)
	}
	if result.Downloads != len(paths) {
		t.Errorf("expected %d downloads, got %d", len(paths), result.Downloads)
	}

	// Verify content
	for p, localPath := range result.LocalPaths {
		downloaded, err := os.ReadFile(localPath)
		if err != nil {
			t.Errorf("failed to read downloaded file %s: %v", p, err)
			continue
		}
		if string(downloaded) != string(content) {
			t.Errorf("content mismatch for %s", p)
		}
	}
}

func TestBatchDownloader_CacheHit(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	cacheDir := t.TempDir()
	downloader := NewBatchDownloader(storage, 3, cacheDir, nil)

	ctx := context.Background()

	// Create and upload a test file
	objectPath := "test/object.txt"
	srcPath := filepath.Join(cacheDir, "src.txt")
	content := []byte("cache hit test")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	if err := storage.Upload(ctx, srcPath, objectPath); err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// First download - should be a cache miss
	req := &BatchRequest{
		ObjectPaths: []string{objectPath},
		Priority:    []int{0},
	}

	result, err := downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("First download failed: %v", err)
	}
	if result.CacheHits != 0 {
		t.Errorf("first download: expected 0 cache hits, got %d", result.CacheHits)
	}
	if result.Downloads != 1 {
		t.Errorf("first download: expected 1 download, got %d", result.Downloads)
	}

	// Second download - should be a cache hit
	result, err = downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("Second download failed: %v", err)
	}
	if result.CacheHits != 1 {
		t.Errorf("second download: expected 1 cache hit, got %d", result.CacheHits)
	}
	if result.Downloads != 0 {
		t.Errorf("second download: expected 0 downloads, got %d", result.Downloads)
	}
}

func TestBatchDownloader_PartialFailure(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	cacheDir := t.TempDir()
	downloader := NewBatchDownloader(storage, 3, cacheDir, nil)

	ctx := context.Background()

	// Create some files in storage
	paths := []string{"exists1.txt", "exists2.txt", "exists3.txt", "nonexistent1.txt", "nonexistent2.txt"}
	content := []byte("partial failure test")

	for _, p := range paths[:3] {
		srcPath := filepath.Join(cacheDir, "src_"+p)
		if err := os.WriteFile(srcPath, content, 0644); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
		if err := storage.Upload(ctx, srcPath, p); err != nil {
			t.Fatalf("Upload failed for %s: %v", p, err)
		}
	}

	// Download - some files don't exist
	req := &BatchRequest{
		ObjectPaths: paths,
		Priority:    make([]int, len(paths)),
	}

	result, err := downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// Verify successful downloads
	if len(result.LocalPaths) != 3 {
		t.Errorf("expected 3 successful downloads, got %d", len(result.LocalPaths))
	}
	if len(result.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(result.Errors))
	}
	if result.CacheHits != 0 {
		t.Errorf("expected 0 cache hits, got %d", result.CacheHits)
	}
	if result.Downloads != 3 {
		t.Errorf("expected 3 downloads, got %d", result.Downloads)
	}

	// Verify errors are for the right paths
	for _, p := range paths[3:] {
		if _, exists := result.Errors[p]; !exists {
			t.Errorf("expected error for path %s", p)
		}
	}
}

func TestBatchDownloader_PriorityOrdering(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	cacheDir := t.TempDir()
	downloader := NewBatchDownloader(storage, 1, cacheDir, nil) // concurrency 1 for deterministic order

	ctx := context.Background()

	// Create test files
	paths := []string{"critical1.txt", "prefetch1.txt", "critical2.txt", "prefetch2.txt"}
	content := []byte("priority test")

	for _, p := range paths {
		srcPath := filepath.Join(cacheDir, "src_"+p)
		if err := os.WriteFile(srcPath, content, 0644); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
		if err := storage.Upload(ctx, srcPath, p); err != nil {
			t.Fatalf("Upload failed for %s: %v", p, err)
		}
	}

	// Request with mixed priorities: critical first, then prefetch
	req := &BatchRequest{
		ObjectPaths: []string{"prefetch1.txt", "critical1.txt", "prefetch2.txt", "critical2.txt"},
		Priority:    []int{1, 0, 1, 0}, // prefetch, critical, prefetch, critical
	}

	result, err := downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// Verify all downloaded
	if len(result.LocalPaths) != 4 {
		t.Errorf("expected 4 local paths, got %d", len(result.LocalPaths))
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %v", result.Errors)
	}
}

func TestBatchDownloader_EmptyRequest(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	cacheDir := t.TempDir()
	downloader := NewBatchDownloader(storage, 3, cacheDir, nil)

	ctx := context.Background()

	req := &BatchRequest{
		ObjectPaths: []string{},
		Priority:    []int{},
	}

	result, err := downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	if len(result.LocalPaths) != 0 {
		t.Errorf("expected 0 local paths, got %d", len(result.LocalPaths))
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(result.Errors))
	}
}

func TestBatchDownloader_NoCacheDir(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	// No cache directory
	downloader := NewBatchDownloader(storage, 3, "", nil)

	ctx := context.Background()

	// Create test file
	objectPath := "test.txt"
	srcPath := filepath.Join(baseDir, "src.txt")
	content := []byte("no cache test")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	if err := storage.Upload(ctx, srcPath, objectPath); err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	req := &BatchRequest{
		ObjectPaths: []string{objectPath},
		Priority:    []int{0},
	}

	result, err := downloader.Download(ctx, req)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	if len(result.LocalPaths) != 1 {
		t.Errorf("expected 1 local path, got %d", len(result.LocalPaths))
	}
	if result.CacheHits != 0 {
		t.Errorf("expected 0 cache hits, got %d", result.CacheHits)
	}
}

func TestBatchDownloader_PriorityMismatch(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	cacheDir := t.TempDir()
	downloader := NewBatchDownloader(storage, 3, cacheDir, nil)

	ctx := context.Background()

	req := &BatchRequest{
		ObjectPaths: []string{"a.txt", "b.txt"},
		Priority:    []int{0}, // wrong length
	}

	result, err := downloader.Download(ctx, req)
	if err == nil {
		t.Errorf("expected error for priority mismatch, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %v", result)
	}
}
