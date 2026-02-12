package compaction

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/storage"
)

// CompactionConfig holds configuration for the compaction daemon.
type CompactionConfig struct {
	// MinPartitionSize is the threshold below which partitions are compaction candidates (default: 8MB).
	MinPartitionSize int64

	// MaxPartitionsPerKey is the max partitions per key per day before compaction triggers (default: 100).
	MaxPartitionsPerKey int64

	// TTLDays is the number of days before compacted source partitions are garbage collected (default: 7).
	TTLDays int

	// CheckInterval is how often the daemon checks for compaction candidates.
	CheckInterval time.Duration

	// WorkDir is the temporary directory for merge operations.
	WorkDir string
}

// DefaultConfig returns the default compaction configuration.
func DefaultConfig() CompactionConfig {
	return CompactionConfig{
		MinPartitionSize:    DefaultMaxPartitionSize,
		MaxPartitionsPerKey: DefaultMaxPartitionsPerKey,
		TTLDays:             7,
		CheckInterval:       5 * time.Minute,
		WorkDir:             os.TempDir(),
	}
}

// Daemon manages background compaction operations.
type Daemon struct {
	config    CompactionConfig
	catalog   manifest.Catalog
	storage   storage.ObjectStorage
	finder    *CandidateFinder
	merger    *Merger
	validator *Validator
	gc        *GarbageCollector

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewDaemon creates a new compaction daemon.
func NewDaemon(config CompactionConfig, catalog manifest.Catalog, store storage.ObjectStorage) *Daemon {
	workDir := filepath.Join(config.WorkDir, "arkilian_compaction")

	return &Daemon{
		config:    config,
		catalog:   catalog,
		storage:   store,
		finder:    NewCandidateFinder(catalog, config.MinPartitionSize, config.MaxPartitionsPerKey),
		merger:    NewMerger(store, workDir),
		validator: NewValidator(),
		gc:        NewGarbageCollector(catalog, store, time.Duration(config.TTLDays)*24*time.Hour),
	}
}

// Start begins the compaction loop. It runs until the context is cancelled or Stop is called.
func (d *Daemon) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return fmt.Errorf("compaction: daemon is already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.running = true
	d.done = make(chan struct{})
	d.mu.Unlock()

	go d.run(ctx)
	return nil
}

// Stop gracefully stops the compaction daemon.
func (d *Daemon) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	d.cancel()
	<-d.done
	d.running = false
	return nil
}

// run is the main compaction loop.
func (d *Daemon) run(ctx context.Context) {
	defer close(d.done)

	// Recover any incomplete compactions from previous crashes
	if err := d.RecoverIncompleteCompactions(ctx); err != nil {
		log.Printf("compaction: recovery failed: %v", err)
	}

	// Run immediately on start
	d.runOnce(ctx)

	ticker := time.NewTicker(d.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runOnce(ctx)
		}
	}
}

// runOnce performs a single compaction cycle: find candidates, merge, validate, and GC.
func (d *Daemon) runOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	// Find compaction candidates
	groups, err := d.finder.FindCandidates(ctx)
	if err != nil {
		log.Printf("compaction: failed to find candidates: %v", err)
		return
	}

	// Process each candidate group
	for _, group := range groups {
		if ctx.Err() != nil {
			return
		}

		if err := d.compactGroup(ctx, group); err != nil {
			log.Printf("compaction: failed to compact group %s: %v", group.PartitionKey, err)
			// Continue with other groups — don't halt on individual failures
		}
	}

	// Run garbage collection
	if ctx.Err() != nil {
		return
	}
	if err := d.gc.CollectGarbage(ctx); err != nil {
		log.Printf("compaction: garbage collection failed: %v", err)
	}
}

// RecoverIncompleteCompactions scans for partitions where compacted_into references
// a target partition that doesn't exist in storage, and resets them.
// This handles the case where the daemon crashed after manifest update but before
// upload completed (or upload was lost).
// RecoverIncompleteCompactions scans for incomplete compactions and recovers:
// 1. Checks compaction_intents table for incomplete two-phase commits
//    - If target exists in storage: complete Phase 2
//    - If target doesn't exist: clean up intent and retry later
// 2. Checks for partitions where compacted_into references a target not in storage
//    - Resets compacted_into to NULL (rollback)
func (d *Daemon) RecoverIncompleteCompactions(ctx context.Context) error {
	catalog, ok := d.catalog.(*manifest.SQLiteCatalog)
	if !ok {
		log.Printf("compaction: recovery skipped — catalog is not SQLiteCatalog")
		return nil
	}

	// Phase 1: Recover from compaction intents
	intents, err := catalog.FindCompactionIntents(ctx)
	if err != nil {
		return fmt.Errorf("compaction recovery: failed to find intents: %w", err)
	}

	for _, intent := range intents {
		exists, err := d.storage.Exists(ctx, intent.TargetObjectPath)
		if err != nil {
			log.Printf("compaction recovery: failed to check storage for intent target %s: %v", intent.TargetPartitionID, err)
			continue
		}

		if exists {
			// Target exists in storage — complete Phase 2
			log.Printf("compaction recovery: completing interrupted compaction for target %s", intent.TargetPartitionID)

			// We need the partition info to complete. Look up the target to see if it's already registered.
			targetPartition, err := d.catalog.GetPartition(ctx, intent.TargetPartitionID)
			if err == nil && targetPartition != nil {
				// Target already registered — just mark sources and delete intent
				if err := d.catalog.MarkCompacted(ctx, intent.SourcePartitionIDs, intent.TargetPartitionID); err != nil {
					log.Printf("compaction recovery: failed to mark sources for %s: %v", intent.TargetPartitionID, err)
					continue
				}
				if err := catalog.DeleteCompactionIntent(ctx, intent.TargetPartitionID); err != nil {
					log.Printf("compaction recovery: failed to delete intent for %s: %v", intent.TargetPartitionID, err)
				}
			} else {
				// Target not registered yet — we can't easily reconstruct PartitionInfo
				// Clean up the intent and let the next compaction cycle handle it
				log.Printf("compaction recovery: target %s exists in storage but not in manifest, cleaning up intent", intent.TargetPartitionID)
				if err := catalog.DeleteCompactionIntent(ctx, intent.TargetPartitionID); err != nil {
					log.Printf("compaction recovery: failed to delete intent for %s: %v", intent.TargetPartitionID, err)
				}
			}
		} else {
			// Target doesn't exist in storage — clean up intent
			log.Printf("compaction recovery: target %s not found in storage, removing stale intent", intent.TargetPartitionID)
			if err := catalog.DeleteCompactionIntent(ctx, intent.TargetPartitionID); err != nil {
				log.Printf("compaction recovery: failed to delete intent for %s: %v", intent.TargetPartitionID, err)
			}
		}
	}

	// Phase 2: Recover from dangling compacted_into references
	compacted, err := catalog.FindCompactedPartitions(ctx)
	if err != nil {
		return fmt.Errorf("compaction recovery: failed to find compacted partitions: %w", err)
	}

	// Group source partitions by their compacted_into target
	targetSources := make(map[string][]string)
	for _, p := range compacted {
		if p.CompactedInto != nil {
			targetSources[*p.CompactedInto] = append(targetSources[*p.CompactedInto], p.PartitionID)
		}
	}

	var resetIDs []string
	for targetID, sourceIDs := range targetSources {
		targetPartition, err := d.catalog.GetPartition(ctx, targetID)
		if err != nil {
			log.Printf("compaction recovery: target partition %s not found in manifest, resetting %d source partitions", targetID, len(sourceIDs))
			resetIDs = append(resetIDs, sourceIDs...)
			continue
		}

		exists, err := d.storage.Exists(ctx, targetPartition.ObjectPath)
		if err != nil {
			log.Printf("compaction recovery: failed to check storage for target %s: %v", targetID, err)
			continue
		}

		if !exists {
			log.Printf("compaction recovery: target partition %s not found in storage at %s, resetting %d source partitions",
				targetID, targetPartition.ObjectPath, len(sourceIDs))
			resetIDs = append(resetIDs, sourceIDs...)
		}
	}

	if len(resetIDs) > 0 {
		log.Printf("compaction recovery: resetting compacted_into for %d partitions", len(resetIDs))
		if err := catalog.ResetCompactedInto(ctx, resetIDs); err != nil {
			return fmt.Errorf("compaction recovery: failed to reset partitions: %w", err)
		}
		log.Printf("compaction recovery: successfully rolled back incomplete compactions for partitions: %v", resetIDs)
	}

	return nil
}


// compactGroup performs the full compaction workflow for a single candidate group:
// merge → validate → upload → update manifest → cleanup.
// compactGroup performs the full compaction workflow for a single candidate group
// using a two-phase commit for crash safety:
// Phase 1: merge → validate → upload → verify → write intent
// Phase 2: register target + mark sources + delete intent (single transaction)
func (d *Daemon) compactGroup(ctx context.Context, group *CandidateGroup) error {
	log.Printf("compaction: starting compaction for key=%s, partitions=%d, reason=%s",
		group.PartitionKey, len(group.Partitions), group.Reason)

	// Step 1: Merge source partitions into a new compacted partition
	result, err := d.merger.Merge(ctx, group)
	if err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Ensure cleanup of temporary files
	mergeDir := filepath.Dir(result.SQLitePath)
	defer func() {
		if cleanErr := d.merger.Cleanup(mergeDir); cleanErr != nil {
			log.Printf("compaction: cleanup warning: %v", cleanErr)
		}
	}()

	// Step 2: Validate the compacted partition
	validationResult, err := d.validator.Validate(ctx, result, group.Partitions)
	if err != nil {
		return fmt.Errorf("validation error: %w", err)
	}
	if !validationResult.Valid {
		// Halt compaction — source partitions remain unchanged (Req 10.2, 10.5)
		return fmt.Errorf("validation failed: %v", validationResult.Errors)
	}

	// Step 3: Upload new partition to storage BEFORE updating manifest (Req 9.4)
	objectPathSQLite := fmt.Sprintf("partitions/%s/%s.sqlite", group.PartitionKey, result.PartitionInfo.PartitionID)
	objectPathMeta := fmt.Sprintf("partitions/%s/%s.meta.json", group.PartitionKey, result.PartitionInfo.PartitionID)

	if _, err := d.storage.UploadMultipart(ctx, result.SQLitePath, objectPathSQLite); err != nil {
		return fmt.Errorf("failed to upload compacted partition: %w", err)
	}

	if err := d.storage.Upload(ctx, result.MetadataPath, objectPathMeta); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// Step 3b: Verify uploads exist in storage before proceeding
	sqliteExists, err := d.storage.Exists(ctx, objectPathSQLite)
	if err != nil {
		return fmt.Errorf("failed to verify compacted partition upload: %w", err)
	}
	if !sqliteExists {
		return fmt.Errorf("upload verification failed: compacted partition not found in storage at %s", objectPathSQLite)
	}

	metaExists, err := d.storage.Exists(ctx, objectPathMeta)
	if err != nil {
		return fmt.Errorf("failed to verify metadata upload: %w", err)
	}
	if !metaExists {
		return fmt.Errorf("upload verification failed: metadata not found in storage at %s", objectPathMeta)
	}

	// Step 4: Two-phase commit for crash-safe manifest update
	if catalog, ok := d.catalog.(*manifest.SQLiteCatalog); ok {
		// Phase 1: Write compaction intent
		intent := &manifest.CompactionIntent{
			TargetPartitionID:  result.PartitionInfo.PartitionID,
			SourcePartitionIDs: result.SourceIDs,
			TargetObjectPath:   objectPathSQLite,
			TargetMetaPath:     objectPathMeta,
			CreatedAt:          time.Now(),
		}
		if err := catalog.WriteCompactionIntent(ctx, intent); err != nil {
			return fmt.Errorf("failed to write compaction intent: %w", err)
		}

		// Phase 2: Register target, mark sources, delete intent — all in one transaction
		if err := catalog.CompleteCompaction(ctx, result.PartitionInfo, objectPathSQLite, result.SourceIDs, result.PartitionInfo.PartitionID); err != nil {
			return fmt.Errorf("failed to complete compaction transaction: %w", err)
		}
	} else {
		// Fallback for non-SQLite catalogs: use original two-step approach
		if err := d.catalog.RegisterPartition(ctx, result.PartitionInfo, objectPathSQLite); err != nil {
			return fmt.Errorf("failed to register compacted partition: %w", err)
		}
		if err := d.catalog.MarkCompacted(ctx, result.SourceIDs, result.PartitionInfo.PartitionID); err != nil {
			return fmt.Errorf("failed to mark sources as compacted: %w", err)
		}
	}

	log.Printf("compaction: completed for key=%s, merged %d partitions into %s (%d rows)",
		group.PartitionKey, len(group.Partitions), result.PartitionInfo.PartitionID, result.TotalRows)

	return nil
}

// TriggerCompaction manually triggers compaction for a specific partition key.
func (d *Daemon) TriggerCompaction(ctx context.Context, partitionKey string) error {
	// Find candidates for this specific key
	allGroups, err := d.finder.FindCandidates(ctx)
	if err != nil {
		return fmt.Errorf("compaction: failed to find candidates: %w", err)
	}

	for _, group := range allGroups {
		if group.PartitionKey == partitionKey {
			return d.compactGroup(ctx, group)
		}
	}

	return fmt.Errorf("compaction: no candidates found for partition key %s", partitionKey)
}

// RunOnce performs a single compaction cycle (useful for testing).
func (d *Daemon) RunOnce(ctx context.Context) {
	d.runOnce(ctx)
}
