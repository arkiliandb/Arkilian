// Package wal provides a write-ahead log for durable write acknowledgment before asynchronous S3 upload.
package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
)

// Recovery is responsible for recovering unflushed WAL entries after a crash.
type Recovery struct {
	wal       *WAL
	flusher   *Flusher
	catalog   manifest.Catalog
}

// NewRecovery creates a new recovery instance.
func NewRecovery(wal *WAL, flusher *Flusher, catalog manifest.Catalog) *Recovery {
	return &Recovery{
		wal:       wal,
		flusher:   flusher,
		catalog:   catalog,
	}
}

// Recover replays unflushed WAL entries after a crash.
// It returns the count of recovered entries.
func (r *Recovery) Recover(ctx context.Context) (int, error) {
	startTime := time.Now()

	// Step 1: List all WAL segment files sorted lexicographically
	segmentFiles, err := r.listSegmentFiles()
	if err != nil {
		return 0, fmt.Errorf("recovery: failed to list segment files: %w", err)
	}

	// Step 2: Determine highest flushed LSN from manifest
	// DO NOT use flusher.FlushedLSN() - it's in-memory and lost on crash
	flushedLSN, err := r.findHighestFlushedLSN(ctx)
	if err != nil {
		return 0, fmt.Errorf("recovery: failed to find highest flushed LSN: %w", err)
	}

	// Step 3-5: Read entries, filter by LSN, and group by partition key
	unflushedEntries, err := r.readUnflushedEntries(ctx, segmentFiles, flushedLSN)
	if err != nil {
		return 0, fmt.Errorf("recovery: failed to read unflushed entries: %w", err)
	}

	if len(unflushedEntries) == 0 {
		fmt.Printf("wal: no unflushed entries to recover\n")
		return 0, nil
	}

	// Group entries by PartitionKey
	groups := make(map[string][]*Entry)
	for _, entry := range unflushedEntries {
		groups[entry.PartitionKey] = append(groups[entry.PartitionKey], entry)
	}

	// Step 6: Replay each group through flushGroup
	var recoveredCount int
	var highestReplayedLSN uint64 = flushedLSN

	for partitionKey, groupEntries := range groups {
		if err := r.flusher.FlushGroup(ctx, partitionKey, groupEntries); err != nil {
			fmt.Printf("recovery: failed to flush partition key %s: %v\n", partitionKey, err)
			continue
		}

		recoveredCount += len(groupEntries)

		// Track highest LSN replayed
		for _, entry := range groupEntries {
			if entry.LSN > highestReplayedLSN {
				highestReplayedLSN = entry.LSN
			}
		}
	}

	// Step 7: Update flusher's flushedTo to highest replayed LSN
	r.flusher.mu.Lock()
	if highestReplayedLSN > r.flusher.flushedTo {
		r.flusher.flushedTo = highestReplayedLSN
	}
	r.flusher.mu.Unlock()

	// Step 8: Log recovery stats
	elapsed := time.Since(startTime)
	fmt.Printf("wal: recovered %d entries in %v\n", recoveredCount, elapsed)

	// Step 9: Return count
	return recoveredCount, nil
}

// listSegmentFiles lists all WAL segment files sorted lexicographically.
func (r *Recovery) listSegmentFiles() ([]string, error) {
	files, err := os.ReadDir(r.wal.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var segmentFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if len(name) < 24 || name[:4] != "wal_" {
			continue
		}
		segmentFiles = append(segmentFiles, filepath.Join(r.wal.dir, name))
	}

	// Sort lexicographically (which is also chronological for our naming scheme)
	sort.Strings(segmentFiles)

	return segmentFiles, nil
}

// findHighestFlushedLSN queries the manifest to find the highest flushed LSN.
// After a crash, flusher.FlushedLSN() returns 0 (in-memory value lost).
// We query the idempotency_keys table for entries matching prefix "wal-lsn-".
func (r *Recovery) findHighestFlushedLSN(ctx context.Context) (uint64, error) {
	// Call catalog.FindHighestIdempotencyLSN with prefix "wal-lsn-"
	// This method is implemented in Task 13.5 for both SQLiteCatalog and ShardedCatalog
	return r.catalog.FindHighestIdempotencyLSN(ctx, "wal-lsn-")
}

// readUnflushedEntries reads all WAL entries from segment files where LSN > flushedLSN.
func (r *Recovery) readUnflushedEntries(ctx context.Context, segmentFiles []string, flushedLSN uint64) ([]*Entry, error) {
	var allEntries []*Entry

	for _, segmentPath := range segmentFiles {
		entries, err := ReadEntries(segmentPath)
		if err != nil {
			fmt.Printf("recovery: failed to read segment %s: %v\n", segmentPath, err)
			continue
		}

		// Filter entries where entry.LSN > flushedLSN
		for _, entry := range entries {
			if entry.LSN > flushedLSN {
				allEntries = append(allEntries, entry)
			}
		}
	}

	return allEntries, nil
}
