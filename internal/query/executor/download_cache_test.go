package executor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadCache_HitAndMiss(t *testing.T) {
	dir := t.TempDir()

	cache := NewDownloadCache(1024 * 1024) // 1MB

	// Miss on empty cache
	if got := cache.Get("obj/a.sqlite"); got != "" {
		t.Fatalf("expected miss, got %q", got)
	}

	// Create a small file and put it in cache
	path := filepath.Join(dir, "a.sqlite")
	if err := os.WriteFile(path, make([]byte, 100), 0644); err != nil {
		t.Fatal(err)
	}

	cache.Put("obj/a.sqlite", path)

	// Hit
	if got := cache.Get("obj/a.sqlite"); got != path {
		t.Fatalf("expected %q, got %q", path, got)
	}

	if cache.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", cache.Len())
	}
}

func TestDownloadCache_LRUEviction(t *testing.T) {
	dir := t.TempDir()

	// Cache that can hold ~250 bytes
	cache := NewDownloadCache(250)

	// Add 3 files of 100 bytes each → should evict oldest
	for i, name := range []string{"a", "b", "c"} {
		path := filepath.Join(dir, name+".sqlite")
		if err := os.WriteFile(path, make([]byte, 100), 0644); err != nil {
			t.Fatal(err)
		}
		cache.Put("obj/"+name, path)
		_ = i
	}

	// "a" should have been evicted (LRU), "b" and "c" should remain
	if got := cache.Get("obj/a"); got != "" {
		t.Fatalf("expected eviction of 'a', but got %q", got)
	}
	if got := cache.Get("obj/b"); got == "" {
		t.Fatal("expected 'b' to be cached")
	}
	if got := cache.Get("obj/c"); got == "" {
		t.Fatal("expected 'c' to be cached")
	}
}

func TestDownloadCache_StaleFileEvicted(t *testing.T) {
	dir := t.TempDir()

	cache := NewDownloadCache(1024 * 1024)

	path := filepath.Join(dir, "x.sqlite")
	if err := os.WriteFile(path, make([]byte, 50), 0644); err != nil {
		t.Fatal(err)
	}
	cache.Put("obj/x", path)

	// Delete the file on disk
	os.Remove(path)

	// Get should detect the missing file and return miss
	if got := cache.Get("obj/x"); got != "" {
		t.Fatalf("expected miss for deleted file, got %q", got)
	}

	if cache.Len() != 0 {
		t.Fatalf("expected 0 entries after stale eviction, got %d", cache.Len())
	}
}
