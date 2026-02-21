package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/router"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// Flusher is responsible for asynchronously flushing WAL entries to SQLite partitions
// and uploading them to object storage.
type Flusher struct {
	wal       *WAL
	builder   partition.PartitionBuilder
	storage   storage.ObjectStorage
	catalog   manifest.Catalog
	metaGen   *partition.MetadataGenerator
	interval  time.Duration
	batchSize int
	flushedTo uint64
	notifier  *router.Notifier
	mu        sync.Mutex
}

// NewFlusher creates a new flusher instance.
func NewFlusher(wal *WAL, builder partition.PartitionBuilder, storage storage.ObjectStorage, catalog manifest.Catalog, metaGen *partition.MetadataGenerator, interval time.Duration, batchSize int) *Flusher {
	return &Flusher{
		wal:       wal,
		builder:   builder,
		storage:   storage,
		catalog:   catalog,
		metaGen:   metaGen,
		interval:  interval,
		batchSize: batchSize,
		flushedTo: 0,
	}
}

// Run starts the background flush loop.
func (f *Flusher) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: flush all remaining entries
			f.FlushUpTo(ctx, f.wal.CurrentLSN())
			return
		case <-ticker.C:
			if err := f.flushOnce(ctx); err != nil {
				// Log error but continue - don't crash the flusher
				fmt.Printf("WAL flusher: error during flush: %v\n", err)
			}
		}
	}
}

// flushOnce reads unflushed WAL entries and processes them.
func (f *Flusher) flushOnce(ctx context.Context) error {
	f.mu.Lock()
	currentFlushedTo := f.flushedTo
	f.mu.Unlock()

	currentLSN := f.wal.CurrentLSN()
	if currentFlushedTo >= currentLSN {
		// Nothing to flush
		return nil
	}

	// Step 1: Read all WAL entries from flushedTo+1 to currentLSN
	entries, err := f.readUnflushedEntries(ctx, currentFlushedTo+1, currentLSN)
	if err != nil {
		return fmt.Errorf("flusher: failed to read unflushed entries: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	// Step 2: Group entries by PartitionKey
	groups := make(map[string][]*Entry)
	for _, entry := range entries {
		groups[entry.PartitionKey] = append(groups[entry.PartitionKey], entry)
	}

	// Step 3: Process each group
	var highestFlushed uint64 = currentFlushedTo
	for partitionKey, groupEntries := range groups {
		if err := f.flushGroup(ctx, partitionKey, groupEntries); err != nil {
			// CRITICAL: If flushGroup fails for one partition key, continue with others
			fmt.Printf("WAL flusher: failed to flush partition key %s: %v\n", partitionKey, err)
			continue
		}

		// Track highest LSN successfully flushed
		for _, entry := range groupEntries {
			if entry.LSN > highestFlushed {
				highestFlushed = entry.LSN
			}
		}
	}

	// Step 4: Advance flushedTo to highest successfully flushed LSN
	f.mu.Lock()
	if highestFlushed > f.flushedTo {
		f.flushedTo = highestFlushed
	}
	f.mu.Unlock()

	// Step 5: Delete fully-flushed segments older than retention period
	f.deleteOldSegments(ctx)

	return nil
}

// readUnflushedEntries reads all WAL entries from startLSN to endLSN.
func (f *Flusher) readUnflushedEntries(ctx context.Context, startLSN, endLSN uint64) ([]*Entry, error) {
	// List all WAL segment files
	files, err := os.ReadDir(f.wal.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var allEntries []*Entry
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if len(file.Name()) < 24 || file.Name()[:4] != "wal_" {
			continue
		}

		segmentPath := filepath.Join(f.wal.dir, file.Name())
		entries, err := ReadEntries(segmentPath)
		if err != nil {
			fmt.Printf("WAL flusher: failed to read segment %s: %v\n", segmentPath, err)
			continue
		}

		// Filter entries within LSN range
		for _, entry := range entries {
			if entry.LSN >= startLSN && entry.LSN <= endLSN {
				allEntries = append(allEntries, entry)
			}
		}
	}

	return allEntries, nil
}

// flushGroup builds a partition from entries, uploads to storage, and registers in catalog.
func (f *Flusher) flushGroup(ctx context.Context, partitionKey string, entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// Collect all rows from entries into a single slice
	var allRows []types.Row
	for _, entry := range entries {
		allRows = append(allRows, entry.Rows...)
	}

	// Build partition key
	key := types.PartitionKey{
		Strategy: types.StrategyTime,
		Value:    partitionKey,
	}

	// Build partition using the appropriate builder method based on schema
	var info *partition.PartitionInfo
	var err error

	// Check if WAL entry has a non-zero Schema for schema evolution support
	if entries[0].Schema.Version > 0 {
		info, err = f.builder.BuildWithSchema(ctx, allRows, key, entries[0].Schema, nil)
	} else {
		info, err = f.builder.Build(ctx, allRows, key)
	}
	if err != nil {
		return fmt.Errorf("flusher: failed to build partition: %w", err)
	}

	// Generate metadata
	sidecar, err := f.metaGen.Generate(info, allRows)
	if err != nil {
		return fmt.Errorf("flusher: failed to generate metadata: %w", err)
	}

	// Derive metadata path
	metaPath := partition.GenerateMetadataPath(info.SQLitePath)

	// Write metadata to file
	if err := sidecar.WriteToFile(metaPath); err != nil {
		return fmt.Errorf("flusher: failed to write metadata: %w", err)
	}

	// Set metadata path on info
	info.MetadataPath = metaPath

	// Upload SQLite to S3
	objectPath := fmt.Sprintf("partitions/%s/%s.sqlite", partitionKey, info.PartitionID)
	if _, err := f.storage.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
		return fmt.Errorf("flusher: failed to upload SQLite: %w", err)
	}

	// Upload metadata to S3
	metaObjectPath := fmt.Sprintf("partitions/%s/%s.meta.json", partitionKey, info.PartitionID)
	if err := f.storage.Upload(ctx, metaPath, metaObjectPath); err != nil {
		return fmt.Errorf("flusher: failed to upload metadata: %w", err)
	}

	// Register in catalog with idempotency key (uses LAST entry's LSN)
	idempotencyKey := fmt.Sprintf("wal-lsn-%d", entries[len(entries)-1].LSN)
	if _, err := f.catalog.RegisterPartitionWithIdempotencyKey(ctx, info, objectPath, idempotencyKey); err != nil {
		return fmt.Errorf("flusher: failed to register partition: %w", err)
	}

	// Update zone maps (best-effort, don't fail the flush)
	if err := f.updateZoneMaps(ctx, info, allRows); err != nil {
		fmt.Printf("WAL flusher: failed to update zone maps: %v\n", err)
	}

	// Publish notification if notifier is enabled
	if f.notifier != nil {
		f.notifier.Publish(router.Notification{
			Type:         router.PartitionCreated,
			PartitionKey: partitionKey,
			PartitionID:  info.PartitionID,
			LSN:          entries[len(entries)-1].LSN,
			Timestamp:    time.Now().UnixNano(),
		})
	}

	return nil
}

// updateZoneMaps updates zone maps for the newly flushed partition (best-effort).
func (f *Flusher) updateZoneMaps(ctx context.Context, info *partition.PartitionInfo, rows []types.Row) error {
	// This follows the same pattern as internal/api/http/ingest.go updateZoneMaps
	// Zone map updates are best-effort - failures don't fail the flush
	// The actual zone map update is handled by the catalog when partitions are registered
	return nil
}

// FlushUpTo forces flush of all entries up to the specified LSN.
func (f *Flusher) FlushUpTo(ctx context.Context, lsn uint64) error {
	f.mu.Lock()
	currentFlushedTo := f.flushedTo
	f.mu.Unlock()

	if lsn <= currentFlushedTo {
		return nil
	}

	// Read all entries up to the specified LSN
	entries, err := f.readUnflushedEntries(ctx, currentFlushedTo+1, lsn)
	if err != nil {
		return fmt.Errorf("flusher: failed to read entries for flush up to: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	// Group by partition key
	groups := make(map[string][]*Entry)
	for _, entry := range entries {
		groups[entry.PartitionKey] = append(groups[entry.PartitionKey], entry)
	}

	// Process each group
	var highestFlushed uint64 = currentFlushedTo
	for partitionKey, groupEntries := range groups {
		if err := f.flushGroup(ctx, partitionKey, groupEntries); err != nil {
			fmt.Printf("WAL flusher: failed to flush partition key %s during FlushUpTo: %v\n", partitionKey, err)
			continue
		}

		for _, entry := range groupEntries {
			if entry.LSN > highestFlushed {
				highestFlushed = entry.LSN
			}
		}
	}

	// Advance flushedTo
	f.mu.Lock()
	if highestFlushed > f.flushedTo {
		f.flushedTo = highestFlushed
	}
	f.mu.Unlock()

	return nil
}

// FlushedLSN returns the current flushed LSN.
func (f *Flusher) FlushedLSN() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushedTo
}

// deleteOldSegments removes WAL segments that are fully flushed and older than retention period.
func (f *Flusher) deleteOldSegments(ctx context.Context) {
	// TODO: Implement segment deletion based on retention period
	// For now, this is a no-op - retention policy can be added later
	// The retention period should come from WALConfig.RetentionTime
}
