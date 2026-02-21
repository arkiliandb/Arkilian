// Package cache provides tiered caching and predictive prefetch for hot partitions.
package cache

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds cache statistics for observability.
type Metrics struct {
	Hits       atomic.Int64
	Misses     atomic.Int64
	Evictions  atomic.Int64
	Entries    atomic.Int64
	SizeBytes  atomic.Int64
}

// NVMeCache provides a local NVMe-backed cache tier for hot partitions.
type NVMeCache struct {
	dir        string
	maxBytes   int64
	metrics    Metrics
	index      sync.Map  // objectPath → *CacheEntry
	evictChan  chan string  // channels entries for async eviction
	wg         sync.WaitGroup
	stopChan   chan struct{}
}

// CacheEntry represents a cached file entry.
type CacheEntry struct {
	LocalPath   string
	SizeBytes   int64
	LastAccess  atomic.Int64  // Unix nanos
	AccessCount atomic.Int64
	Pinned      bool
}

// NewNVMeCache creates a new NVMe cache instance.
func NewNVMeCache(dir string, maxBytes int64) (*NVMeCache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be positive, got %d", maxBytes)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache dir: %w", err)
	}

	cache := &NVMeCache{
		dir:       dir,
		maxBytes:  maxBytes,
		evictChan: make(chan string, 1000),  // Buffered to avoid blocking
		stopChan:  make(chan struct{}),
	}

	// Scan existing files to rebuild index
	if err := cache.scanExistingFiles(); err != nil {
		return nil, fmt.Errorf("failed to scan existing files: %w", err)
	}

	// Start async eviction worker
	cache.wg.Add(1)
	go cache.evictionWorker()

	return cache, nil
}

// Close shuts down the cache and waits for pending operations.
func (c *NVMeCache) Close() {
	close(c.stopChan)
	c.wg.Wait()
}

// Metrics returns current cache metrics.
func (c *NVMeCache) Metrics() (hits, misses, evictions, entries, size int64) {
	return c.metrics.Hits.Load(), c.metrics.Misses.Load(), c.metrics.Evictions.Load(),
		c.metrics.Entries.Load(), c.metrics.SizeBytes.Load()
}

// HitRate returns the cache hit rate as a percentage.
func (c *NVMeCache) HitRate() float64 {
	hits := c.metrics.Hits.Load()
	misses := c.metrics.Misses.Load()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

// scanExistingFiles scans the cache directory and rebuilds the index.
func (c *NVMeCache) scanExistingFiles() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue  // Skip inaccessible files
		}

		objectPath := entry.Name()
		sizeBytes := info.Size()

		cacheEntry := &CacheEntry{
			LocalPath:   filepath.Join(c.dir, entry.Name()),
			SizeBytes:   sizeBytes,
			LastAccess:  atomic.Int64{},
			AccessCount: atomic.Int64{},
			Pinned:      false,
		}
		cacheEntry.LastAccess.Store(time.Now().UnixNano())

		c.index.Store(objectPath, cacheEntry)
		c.metrics.SizeBytes.Add(sizeBytes)
		c.metrics.Entries.Add(1)
	}

	return nil
}

// Get retrieves a cached entry by objectPath.
func (c *NVMeCache) Get(objectPath string) (string, bool) {
	entry, ok := c.index.Load(objectPath)
	if !ok {
		c.metrics.Misses.Add(1)
		return "", false
	}

	c.metrics.Hits.Add(1)
	cacheEntry := entry.(*CacheEntry)
	cacheEntry.LastAccess.Store(time.Now().UnixNano())
	cacheEntry.AccessCount.Add(1)

	return cacheEntry.LocalPath, true
}

// Put adds a file to the cache.
func (c *NVMeCache) Put(objectPath, sourcePath string, sizeBytes int64) error {
	if sizeBytes <= 0 {
		return fmt.Errorf("sizeBytes must be positive, got %d", sizeBytes)
	}

	// Copy file to cache directory
	sanitizedName := sanitizeFileName(objectPath)
	destPath := filepath.Join(c.dir, sanitizedName)

	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create dest file: %w", err)
	}
	defer dst.Close()

	written, err := src.WriteTo(dst)
	if err != nil {
		os.Remove(destPath)
		return fmt.Errorf("failed to copy file: %w", err)
	}

	if written != sizeBytes {
		os.Remove(destPath)
		return fmt.Errorf("size mismatch: expected %d, got %d", sizeBytes, written)
	}

	// Add to index
	cacheEntry := &CacheEntry{
		LocalPath:   destPath,
		SizeBytes:   sizeBytes,
		LastAccess:  atomic.Int64{},
		AccessCount: atomic.Int64{},
		Pinned:      false,
	}
	cacheEntry.LastAccess.Store(time.Now().UnixNano())
	cacheEntry.AccessCount.Store(1)

	c.index.Store(objectPath, cacheEntry)
	c.metrics.SizeBytes.Add(sizeBytes)
	c.metrics.Entries.Add(1)

	// Check if eviction needed and trigger async
	if c.metrics.SizeBytes.Load() > c.maxBytes {
		select {
		case c.evictChan <- objectPath:
		default:
			// Channel full, eviction will happen on next check
		}
	}

	return nil
}

// evictionWorker processes eviction requests asynchronously.
func (c *NVMeCache) evictionWorker() {
	defer c.wg.Done()

	evictTicker := time.NewTicker(5 * time.Second)
	defer evictTicker.Stop()

	for {
		select {
		case <-c.stopChan:
			// Final eviction before shutdown
			c.performEviction()
			return
		case objectPath := <-c.evictChan:
			// Check if still over capacity and evict this entry
			if c.metrics.SizeBytes.Load() > c.maxBytes {
				c.tryEvictOne(objectPath)
			}
		case <-evictTicker.C:
			// Periodic cleanup
			c.performEviction()
		}
	}
}

// performEviction runs a full eviction pass.
func (c *NVMeCache) performEviction() {
	targetSize := int64(float64(c.maxBytes) * 0.9)
	if c.metrics.SizeBytes.Load() <= targetSize {
		return
	}

	// Collect candidates (non-pinned, oldest access first)
	type evictCandidate struct {
		path       string
		entry      *CacheEntry
		accessTime int64
		count      int64
	}
	var candidates []evictCandidate

	c.index.Range(func(key, value interface{}) bool {
		objectPath := key.(string)
		cacheEntry := value.(*CacheEntry)
		if !cacheEntry.Pinned {
			candidates = append(candidates, evictCandidate{
				path:       objectPath,
				entry:      cacheEntry,
				accessTime: cacheEntry.LastAccess.Load(),
				count:      cacheEntry.AccessCount.Load(),
			})
		}
		return true
	})

	// Sort by access count, then by last access (LRU)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].count != candidates[j].count {
			return candidates[i].count < candidates[j].count
		}
		return candidates[i].accessTime < candidates[j].accessTime
	})

	// Evict until under target
	for _, cand := range candidates {
		if c.metrics.SizeBytes.Load() <= targetSize {
			break
		}
		c.tryEvictOne(cand.path)
	}
}

// tryEvictOne attempts to evict a single entry.
func (c *NVMeCache) tryEvictOne(objectPath string) {
	entry, ok := c.index.Load(objectPath)
	if !ok {
		return
	}

	cacheEntry := entry.(*CacheEntry)
	if cacheEntry.Pinned {
		return
	}

	if err := os.Remove(cacheEntry.LocalPath); err == nil {
		c.metrics.SizeBytes.Add(-cacheEntry.SizeBytes)
		c.metrics.Entries.Add(-1)
		c.index.Delete(objectPath)
		c.metrics.Evictions.Add(1)
		log.Printf("cache: evicted %s (freed %d bytes)", objectPath, cacheEntry.SizeBytes)
	}
}

// Pin marks an entry as non-evictable.
func (c *NVMeCache) Pin(objectPath string) {
	entry, ok := c.index.Load(objectPath)
	if !ok {
		return
	}
	cacheEntry := entry.(*CacheEntry)
	cacheEntry.Pinned = true
}

// Unpin marks an entry as evictable.
func (c *NVMeCache) Unpin(objectPath string) {
	entry, ok := c.index.Load(objectPath)
	if !ok {
		return
	}
	cacheEntry := entry.(*CacheEntry)
	cacheEntry.Pinned = false
}

// Remove deletes an entry from the cache.
func (c *NVMeCache) Remove(objectPath string) bool {
	entry, ok := c.index.LoadAndDelete(objectPath)
	if !ok {
		return false
	}
	cacheEntry := entry.(*CacheEntry)
	if err := os.Remove(cacheEntry.LocalPath); err == nil {
		c.metrics.SizeBytes.Add(-cacheEntry.SizeBytes)
		c.metrics.Entries.Add(-1)
		return true
	}
	return false
}

// Clear removes all entries from the cache.
func (c *NVMeCache) Clear() {
	c.index.Range(func(key, value interface{}) bool {
		objectPath := key.(string)
		c.Remove(objectPath)
		return true
	})
}

// Size returns the current cache size in bytes.
func (c *NVMeCache) Size() int64 {
	return c.metrics.SizeBytes.Load()
}

// Count returns the number of entries in the cache.
func (c *NVMeCache) Count() int64 {
	return c.metrics.Entries.Load()
}

// Capacity returns the maximum cache size in bytes.
func (c *NVMeCache) Capacity() int64 {
	return c.maxBytes
}

// Usage returns the cache usage as a percentage.
func (c *NVMeCache) Usage() float64 {
	return float64(c.metrics.SizeBytes.Load()) / float64(c.maxBytes) * 100
}

// sanitizeFileName creates a safe filename from an object path.
func sanitizeFileName(objectPath string) string {
	result := filepath.FromSlash(objectPath)
	if len(result) > 200 || containsInvalidChars(result) {
		return fmt.Sprintf("%x.sqlite", hashString(objectPath))
	}
	return result
}

func containsInvalidChars(s string) bool {
	for _, c := range s {
		if c == '/' || c == '\\' || c == ':' || c == 0 {
			return true
		}
	}
	return false
}

func hashString(s string) uint64 {
	var h uint64
	for _, c := range s {
		h = h*31 + uint64(c)
	}
	return h
}