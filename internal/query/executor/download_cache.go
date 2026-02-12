// Package executor provides query execution across partitions.
package executor

import (
	"container/list"
	"os"
	"sync"
)

// DownloadCache is an LRU cache for downloaded partition files.
// It tracks files by objectPath and evicts least-recently-used entries
// when the total cached size exceeds the configured maximum.
type DownloadCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64

	// items maps objectPath → list element (whose value is *cacheEntry)
	items map[string]*list.Element
	order *list.List // front = most recently used
}

type cacheEntry struct {
	objectPath string
	localPath  string
	sizeBytes  int64
}

// NewDownloadCache creates a new LRU download cache.
// maxBytes is the maximum total size of cached files (default 10GB).
func NewDownloadCache(maxBytes int64) *DownloadCache {
	if maxBytes <= 0 {
		maxBytes = 10 * 1024 * 1024 * 1024 // 10 GB
	}
	return &DownloadCache{
		maxBytes: maxBytes,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get returns the local path for a cached partition, or "" if not cached.
// On hit, the entry is promoted to most-recently-used.
func (c *DownloadCache) Get(objectPath string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[objectPath]
	if !ok {
		return ""
	}

	entry := elem.Value.(*cacheEntry)

	// Verify the file still exists and matches expected size
	info, err := os.Stat(entry.localPath)
	if err != nil || info.Size() != entry.sizeBytes {
		// File gone or corrupted — evict entry
		c.removeLocked(elem)
		return ""
	}

	// Promote to front (most recently used)
	c.order.MoveToFront(elem)
	return entry.localPath
}

// Put records a downloaded partition in the cache.
// If adding this entry exceeds maxBytes, LRU entries are evicted.
func (c *DownloadCache) Put(objectPath, localPath string) {
	info, err := os.Stat(localPath)
	if err != nil {
		return // can't cache what we can't stat
	}
	sizeBytes := info.Size()

	c.mu.Lock()
	defer c.mu.Unlock()

	// If already cached, update and promote
	if elem, ok := c.items[objectPath]; ok {
		old := elem.Value.(*cacheEntry)
		c.curBytes -= old.sizeBytes
		old.localPath = localPath
		old.sizeBytes = sizeBytes
		c.curBytes += sizeBytes
		c.order.MoveToFront(elem)
	} else {
		entry := &cacheEntry{
			objectPath: objectPath,
			localPath:  localPath,
			sizeBytes:  sizeBytes,
		}
		elem := c.order.PushFront(entry)
		c.items[objectPath] = elem
		c.curBytes += sizeBytes
	}

	// Evict LRU entries until under limit
	for c.curBytes > c.maxBytes && c.order.Len() > 1 {
		c.evictOldestLocked()
	}
}

// evictOldestLocked removes the least-recently-used entry.
// Caller must hold c.mu.
func (c *DownloadCache) evictOldestLocked() {
	back := c.order.Back()
	if back == nil {
		return
	}
	c.removeLocked(back)
}

// removeLocked removes a specific element from the cache and deletes the file.
// Caller must hold c.mu.
func (c *DownloadCache) removeLocked(elem *list.Element) {
	entry := elem.Value.(*cacheEntry)
	c.order.Remove(elem)
	delete(c.items, entry.objectPath)
	c.curBytes -= entry.sizeBytes

	// Best-effort delete of the cached file
	os.Remove(entry.localPath)
}

// Size returns the current total cached size in bytes.
func (c *DownloadCache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// Len returns the number of cached entries.
func (c *DownloadCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Clear removes all entries and deletes cached files.
func (c *DownloadCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.order.Len() > 0 {
		c.evictOldestLocked()
	}
}
