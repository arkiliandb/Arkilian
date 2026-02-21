package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNVMeCache_PutGet(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Create a test file
	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Put the file in cache
	objectPath := "test/partition.sqlite"
	if err := cache.Put(objectPath, sourcePath, 12); err != nil {
		t.Fatalf("failed to put in cache: %v", err)
	}

	// Get should return the cached path
	localPath, ok := cache.Get(objectPath)
	if !ok {
		t.Fatal("expected cache hit, got miss")
	}

	if localPath == "" {
		t.Fatal("expected non-empty local path")
	}

	// Verify file content
	content, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("failed to read cached file: %v", err)
	}

	if string(content) != "test content" {
		t.Errorf("expected 'test content', got '%s'", string(content))
	}
}

func TestNVMeCache_Eviction(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 100) // 100 bytes max
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()

	// Add files that exceed capacity
	for i := 0; i < 5; i++ {
		sourcePath := filepath.Join(sourceDir, "file")
		content := make([]byte, 30) // 30 bytes each
		for j := range content {
			content[j] = byte(i)
		}
		if err := os.WriteFile(sourcePath, content, 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		objectPath := "test/partition"
		if err := cache.Put(objectPath, sourcePath, 30); err != nil {
			t.Fatalf("failed to put in cache: %v", err)
		}
	}

	// Wait for async eviction
	time.Sleep(100 * time.Millisecond)

	// Should have evicted some entries
	if cache.Size() > 100 {
		t.Errorf("expected size <= 100, got %d", cache.Size())
	}
}

func TestNVMeCache_Pinned(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 100) // 100 bytes max
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()

	// Add a pinned entry
	sourcePath1 := filepath.Join(sourceDir, "file1")
	if err := os.WriteFile(sourcePath1, []byte("pinned content"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	objectPath1 := "test/pinned"
	if err := cache.Put(objectPath1, sourcePath1, 14); err != nil {
		t.Fatalf("failed to put in cache: %v", err)
	}
	cache.Pin(objectPath1)

	// Add more entries to trigger eviction
	for i := 0; i < 4; i++ {
		sourcePath := filepath.Join(sourceDir, "file")
		content := make([]byte, 25)
		if err := os.WriteFile(sourcePath, content, 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		objectPath := "test/partition"
		if err := cache.Put(objectPath, sourcePath, 25); err != nil {
			t.Fatalf("failed to put in cache: %v", err)
		}
	}

	// Wait for async eviction
	time.Sleep(100 * time.Millisecond)

	// Pinned entry should still be present
	_, ok := cache.Get(objectPath1)
	if !ok {
		t.Fatal("pinned entry was evicted")
	}
}

func TestNVMeCache_Concurrent(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			objectPath := "test/partition"
			if id%2 == 0 {
				cache.Get(objectPath)
			} else {
				cache.Put(objectPath, sourcePath, 12)
			}
		}(i)
	}

	wg.Wait()
	// No race should be detected
}

func TestNVMeCache_RebuildIndex(t *testing.T) {
	dir := t.TempDir()

	// Create some files directly in cache dir
	file1 := filepath.Join(dir, "file1")
	if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	file2 := filepath.Join(dir, "file2")
	if err := os.WriteFile(file2, []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Should find existing files
	_, ok := cache.Get("file1")
	if !ok {
		t.Error("expected to find file1 in index")
	}

	_, ok = cache.Get("file2")
	if !ok {
		t.Error("expected to find file2 in index")
	}
}

func TestNVMeCache_Metrics(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	objectPath := "test/partition"
	cache.Get(objectPath)  // Miss
	cache.Put(objectPath, sourcePath, 12)
	cache.Get(objectPath)  // Hit
	cache.Get(objectPath)  // Hit

	hits, misses, _, entries, size := cache.Metrics()
	if hits != 2 {
		t.Errorf("expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("expected 1 miss, got %d", misses)
	}
	if entries != 1 {
		t.Errorf("expected 1 entry, got %d", entries)
	}
	if size != 12 {
		t.Errorf("expected size 12, got %d", size)
	}

	hitRate := cache.HitRate()
	if hitRate < 66 || hitRate > 67 {  // 2/3 ≈ 66.67%
		t.Errorf("expected hit rate ~66.67, got %.2f", hitRate)
	}
}

func TestNVMeCache_Miss(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Get non-existent entry
	_, ok := cache.Get("nonexistent/path")
	if ok {
		t.Error("expected cache miss for non-existent path")
	}
}

func TestNVMeCache_Remove(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	objectPath := "test/partition"
	cache.Put(objectPath, sourcePath, 12)

	// Remove
	if !cache.Remove(objectPath) {
		t.Error("expected remove to succeed")
	}

	// Should be gone
	_, ok := cache.Get(objectPath)
	if ok {
		t.Error("expected cache miss after remove")
	}
}

func TestNVMeCache_Clear(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()

	for i := 0; i < 5; i++ {
		sourcePath := filepath.Join(sourceDir, fmt.Sprintf("testfile%d", i))
		if err := os.WriteFile(sourcePath, []byte("test content"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		cache.Put(fmt.Sprintf("test/partition%d", i), sourcePath, 12)
	}

	cache.Clear()
	time.Sleep(100 * time.Millisecond)  // Wait for async operations

	if cache.Count() != 0 {
		t.Errorf("expected 0 entries after clear, got %d", cache.Count())
	}
}

func TestNVMeCache_Usage(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1000)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	if cache.Usage() != 0 {
		t.Errorf("expected 0%% usage, got %.2f", cache.Usage())
	}

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("1234567890"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	cache.Put("test/partition", sourcePath, 10)

	usage := cache.Usage()
	if usage < 0.9 || usage > 1.1 {  // ~1%
		t.Errorf("expected usage ~1%%, got %.2f", usage)
	}
}

func TestNVMeCache_Capacity(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	if cache.Capacity() != 1024*1024 {
		t.Errorf("expected capacity %d, got %d", 1024*1024, cache.Capacity())
	}
}

func TestNVMeCache_Size(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	if cache.Size() != 0 {
		t.Errorf("expected size 0, got %d", cache.Size())
	}
}

func TestNVMeCache_Unpin(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024) // Large cache to avoid eviction
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	content := []byte("test content") // 12 bytes
	if err := os.WriteFile(sourcePath, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	objectPath := "test/partition"
	cache.Put(objectPath, sourcePath, int64(len(content)))
	
	if cache.Count() != 1 {
		t.Errorf("expected 1 entry after Put, got %d", cache.Count())
	}
	
	cache.Pin(objectPath)
	cache.Unpin(objectPath)
	
	// Entry should still exist (no eviction with large cache)
	_, ok := cache.Get(objectPath)
	if !ok {
		t.Error("expected entry to exist after unpin")
	}
}

func TestNVMeCache_Close(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Close should not panic
	cache.Close()

	// After close, operations should fail gracefully
	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Put after close - may fail due to closed stopChan
	cache.Put("test/partition", sourcePath, 4)
}

func TestNVMeCache_ZeroMaxBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := NewNVMeCache(dir, 0)
	if err == nil {
		t.Error("expected error for zero maxBytes")
	}
}

func TestNVMeCache_NegativeMaxBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := NewNVMeCache(dir, -100)
	if err == nil {
		t.Error("expected error for negative maxBytes")
	}
}

func TestNVMeCache_ZeroSizeBytes(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	err = cache.Put("test/partition", sourcePath, 0)
	if err == nil {
		t.Error("expected error for zero sizeBytes")
	}
}

func TestNVMeCache_SizeMismatch(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "testfile")
	if err := os.WriteFile(sourcePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Wrong size
	err = cache.Put("test/partition", sourcePath, 100)
	if err == nil {
		t.Error("expected error for size mismatch")
	}
}

func TestNVMeCache_InvalidSource(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewNVMeCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	err = cache.Put("test/partition", "/nonexistent/path", 100)
	if err == nil {
		t.Error("expected error for invalid source")
	}
}