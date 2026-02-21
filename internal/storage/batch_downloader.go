package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/arkilian/arkilian/internal/cache"
	"golang.org/x/sync/semaphore"
)

// BatchDownloader coordinates parallel downloads from object storage.
// It supports priority-based ordering and caching to avoid redundant downloads.
type BatchDownloader struct {
	storage     ObjectStorage
	concurrency int
	cacheDir    string
	nvmeCache   *cache.NVMeCache  // nil = disabled
}

// BatchRequest specifies which objects to download with optional priorities.
type BatchRequest struct {
	ObjectPaths []string
	Priority    []int // 0=critical, 1=prefetch
}

// BatchResult contains the outcome of a batch download operation.
type BatchResult struct {
	LocalPaths map[string]string
	Errors     map[string]error
	CacheHits  int
	Downloads  int
}

// NewBatchDownloader creates a new batch downloader.
// storage: the ObjectStorage implementation to download from
// concurrency: maximum number of parallel downloads
// cacheDir: directory to cache downloaded files (empty = no caching)
// nvmeCache: optional NVMe cache for hot partitions (nil = disabled)
func NewBatchDownloader(storage ObjectStorage, concurrency int, cacheDir string, nvmeCache *cache.NVMeCache) *BatchDownloader {
	return &BatchDownloader{
		storage:     storage,
		concurrency: concurrency,
		cacheDir:    cacheDir,
		nvmeCache:   nvmeCache,
	}
}

// Download downloads multiple objects in parallel with priority ordering.
// Returns a map of objectPath to localPath for successful downloads,
// and a separate map of objectPath to error for failed downloads.
func (b *BatchDownloader) Download(ctx context.Context, req *BatchRequest) (*BatchResult, error) {
	if len(req.ObjectPaths) == 0 {
		return &BatchResult{
			LocalPaths: make(map[string]string),
			Errors:     make(map[string]error),
		}, nil
	}

	// Validate priority array matches object paths count
	priority := req.Priority
	if len(priority) == 0 {
		// Default all to priority 0 if not specified
		priority = make([]int, len(req.ObjectPaths))
	} else if len(priority) != len(req.ObjectPaths) {
		return nil, fmt.Errorf("priority array length must match object paths count")
	}

	// Group paths by priority
	type pathWithPriority struct {
		path      string
		priority  int
		localPath string
	}
	paths := make([]pathWithPriority, len(req.ObjectPaths))
	for i, p := range req.ObjectPaths {
		paths[i] = pathWithPriority{
			path:      p,
			priority:  priority[i],
			localPath: b.localPath(p),
		}
	}

	// Sort by priority (0 first, then 1, etc.)
	sort.Slice(paths, func(i, j int) bool {
		return paths[i].priority < paths[j].priority
	})

	// Initialize result
	result := &BatchResult{
		LocalPaths: make(map[string]string),
		Errors:     make(map[string]error),
	}

	// Separate cache hits from downloads
	var downloadQueue []pathWithPriority
	sem := semaphore.NewWeighted(int64(b.concurrency))

	for _, p := range paths {
		// Check NVMe cache first
		if b.nvmeCache != nil {
			if localPath, ok := b.nvmeCache.Get(p.path); ok {
				result.LocalPaths[p.path] = localPath
				result.CacheHits++
				continue
			}
		}

		// Check filesystem cache
		if b.cacheDir != "" {
			if _, err := os.Stat(p.localPath); err == nil {
				result.LocalPaths[p.path] = p.localPath
				result.CacheHits++
				continue
			}
		}

		downloadQueue = append(downloadQueue, p)
	}

	// Process downloads with semaphore
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, p := range downloadQueue {
		if err := sem.Acquire(ctx, 1); err != nil {
			// Context cancelled or semaphore failed
			mu.Lock()
			result.Errors[p.path] = fmt.Errorf("semaphore acquire failed: %w", err)
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(path string, local string) {
			defer sem.Release(1)
			defer wg.Done()

			if err := b.storage.Download(ctx, path, local); err != nil {
				mu.Lock()
				result.Errors[path] = err
				mu.Unlock()
				return
			}

			// Get file size for NVMe cache
			var sizeBytes int64
			if info, err := os.Stat(local); err == nil {
				sizeBytes = info.Size()
			}

			// Add to NVMe cache if enabled
			if b.nvmeCache != nil && sizeBytes > 0 {
				b.nvmeCache.Put(path, local, sizeBytes)
			}

			mu.Lock()
			result.LocalPaths[path] = local
			result.Downloads++
			mu.Unlock()
		}(p.path, p.localPath)
	}

	wg.Wait()

	return result, nil
}

// localPath returns the local filesystem path for an object.
// It uses a hash for long paths to avoid filename length limits and collisions.
func (b *BatchDownloader) localPath(objectPath string) string {
	if b.cacheDir == "" {
		return hashFileName(objectPath)
	}
	return filepath.Join(b.cacheDir, hashFileName(objectPath))
}

// hashFileName creates a unique filename from an object path.
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
