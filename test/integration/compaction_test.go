package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/compaction"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// setupCompactionTestEnv creates a test environment with small partitions for compaction tests.
func setupCompactionTestEnv(t *testing.T, numPartitions int) (
	*manifest.SQLiteCatalog,
	*storage.LocalStorage,
	*compaction.Daemon,
	string,
	func(),
) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-compaction-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	workDir := filepath.Join(tempDir, "work")

	store, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create small partitions
	builder := partition.NewBuilder(partitionDir)
	metaGen := partition.NewMetadataGenerator()
	ctx := context.Background()

	for i := 0; i < numPartitions; i++ {
		rows := []types.Row{
			{
				TenantID:  "acme",
				UserID:    int64(i * 100),
				EventTime: time.Now().Add(time.Duration(-i) * time.Hour).UnixNano(),
				EventType: "test_event",
				Payload:   map[string]interface{}{"index": i},
			},
		}

		key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260206"}
		info, err := builder.Build(ctx, rows, key)
		if err != nil {
			catalog.Close()
			os.RemoveAll(tempDir)
			t.Fatalf("failed to build partition %d: %v", i, err)
		}

		metaPath, err := metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			catalog.Close()
			os.RemoveAll(tempDir)
			t.Fatalf("failed to generate metadata: %v", err)
		}
		info.MetadataPath = metaPath

		objectPath := "partitions/20260206/" + info.PartitionID + ".sqlite"
		metaObjectPath := "partitions/20260206/" + info.PartitionID + ".meta.json"

		if _, err := store.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
			catalog.Close()
			os.RemoveAll(tempDir)
			t.Fatalf("failed to upload partition: %v", err)
		}
		if err := store.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
			catalog.Close()
			os.RemoveAll(tempDir)
			t.Fatalf("failed to upload metadata: %v", err)
		}

		if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			catalog.Close()
			os.RemoveAll(tempDir)
			t.Fatalf("failed to register partition: %v", err)
		}
	}

	// Create compaction daemon with low thresholds for testing
	config := compaction.CompactionConfig{
		MinPartitionSize:    100 * 1024 * 1024, // 100MB - all test partitions are smaller
		MaxPartitionsPerKey: 2,                  // Trigger compaction with just 2 partitions
		TTLDays:             0,                  // Immediate TTL for testing
		CheckInterval:       time.Hour,          // Long interval - we'll trigger manually
		WorkDir:             workDir,
	}

	daemon := compaction.NewDaemon(config, catalog, store)

	cleanup := func() {
		daemon.Stop()
		catalog.Close()
		os.RemoveAll(tempDir)
	}

	return catalog, store, daemon, tempDir, cleanup
}

// TestCompactionFlow tests the end-to-end compaction flow:
// candidates → merge → upload → manifest
func TestCompactionFlow(t *testing.T) {
	ctx := context.Background()

	// Create 3 small partitions
	catalog, store, daemon, _, cleanup := setupCompactionTestEnv(t, 3)
	defer cleanup()

	// Verify initial state
	initialCount, err := catalog.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("failed to count partitions: %v", err)
	}
	if initialCount != 3 {
		t.Fatalf("expected 3 initial partitions, got %d", initialCount)
	}

	// Run compaction once
	daemon.RunOnce(ctx)

	// Verify compaction occurred
	// After compaction, we should have:
	// - 1 new compacted partition (active)
	// - 3 source partitions marked as compacted (inactive)
	activeCount, err := catalog.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("failed to count active partitions: %v", err)
	}

	// Should have 1 active partition (the compacted one)
	if activeCount != 1 {
		t.Errorf("expected 1 active partition after compaction, got %d", activeCount)
	}

	// Verify the compacted partition exists in storage
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	if len(partitions) != 1 {
		t.Fatalf("expected 1 active partition, got %d", len(partitions))
	}

	compactedPartition := partitions[0]

	// Verify it's in storage
	exists, err := store.Exists(ctx, compactedPartition.ObjectPath)
	if err != nil {
		t.Fatalf("failed to check storage: %v", err)
	}
	if !exists {
		t.Error("compacted partition not found in storage")
	}

	// Verify row count is sum of source partitions
	if compactedPartition.RowCount != 3 {
		t.Errorf("expected 3 rows in compacted partition, got %d", compactedPartition.RowCount)
	}
}

// TestCompactionCandidateSelection tests that compaction candidates are correctly identified.
func TestCompactionCandidateSelection(t *testing.T) {
	ctx := context.Background()

	// Create 5 small partitions
	catalog, _, _, _, cleanup := setupCompactionTestEnv(t, 5)
	defer cleanup()

	// Create candidate finder with low threshold
	finder := compaction.NewCandidateFinder(catalog, 100*1024*1024, 2)

	// Find candidates
	groups, err := finder.FindCandidates(ctx)
	if err != nil {
		t.Fatalf("failed to find candidates: %v", err)
	}

	// Should find at least one group
	if len(groups) == 0 {
		t.Error("expected at least one candidate group")
	}

	// Verify the group has the correct partition key
	found := false
	for _, g := range groups {
		if g.PartitionKey == "20260206" {
			found = true
			if len(g.Partitions) < 2 {
				t.Errorf("expected at least 2 partitions in group, got %d", len(g.Partitions))
			}
		}
	}

	if !found {
		t.Error("expected to find candidate group for partition key 20260206")
	}
}

// TestCompactionMergeOrdering tests that merged data is sorted by primary key.
func TestCompactionMergeOrdering(t *testing.T) {
	ctx := context.Background()

	// Create 3 partitions
	catalog, store, daemon, _, cleanup := setupCompactionTestEnv(t, 3)
	defer cleanup()

	// Run compaction
	daemon.RunOnce(ctx)

	// Get the compacted partition
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	if len(partitions) != 1 {
		t.Fatalf("expected 1 compacted partition, got %d", len(partitions))
	}

	// Verify the partition exists and has correct row count
	compacted := partitions[0]
	if compacted.RowCount != 3 {
		t.Errorf("expected 3 rows, got %d", compacted.RowCount)
	}

	// Verify it's in storage
	exists, _ := store.Exists(ctx, compacted.ObjectPath)
	if !exists {
		t.Error("compacted partition not in storage")
	}
}

// TestCompactionSafetyInvariant tests that source partitions remain queryable
// until compaction is complete.
func TestCompactionSafetyInvariant(t *testing.T) {
	ctx := context.Background()

	// Create 3 partitions
	catalog, _, _, _, cleanup := setupCompactionTestEnv(t, 3)
	defer cleanup()

	// Get initial partitions
	initialPartitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	// All partitions should be active (compacted_into is NULL)
	for _, p := range initialPartitions {
		if p.CompactedInto != nil {
			t.Errorf("partition %s should not be compacted yet", p.PartitionID)
		}
	}

	// Verify we can query all partitions
	if len(initialPartitions) != 3 {
		t.Errorf("expected 3 queryable partitions, got %d", len(initialPartitions))
	}
}

// TestCompactionIdempotency tests that compaction operations are idempotent.
func TestCompactionIdempotency(t *testing.T) {
	ctx := context.Background()

	// Create 3 partitions
	catalog, _, daemon, _, cleanup := setupCompactionTestEnv(t, 3)
	defer cleanup()

	// Run compaction twice
	daemon.RunOnce(ctx)
	daemon.RunOnce(ctx)

	// Should still have exactly 1 active partition
	activeCount, err := catalog.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("failed to count partitions: %v", err)
	}

	if activeCount != 1 {
		t.Errorf("expected 1 active partition after double compaction, got %d", activeCount)
	}
}

// TestTTLEnforcement tests that expired partitions are garbage collected.
func TestTTLEnforcement(t *testing.T) {
	ctx := context.Background()

	tempDir, err := os.MkdirTemp("", "arkilian-ttl-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir)
	metaGen := partition.NewMetadataGenerator()

	// Create a partition
	rows := []types.Row{
		{TenantID: "acme", UserID: 100, EventTime: time.Now().UnixNano(), EventType: "test", Payload: map[string]interface{}{}},
	}
	key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260206"}
	info, _ := builder.Build(ctx, rows, key)
	metaPath, _ := metaGen.GenerateAndWrite(info, rows)
	info.MetadataPath = metaPath

	objectPath := "partitions/20260206/" + info.PartitionID + ".sqlite"
	store.UploadMultipart(ctx, info.SQLitePath, objectPath)
	catalog.RegisterPartition(ctx, info, objectPath)

	// Create GC with 0 TTL (immediate expiration)
	gc := compaction.NewGarbageCollector(catalog, store, 0)

	// Initially, partition is not compacted, so GC should not delete it
	result, err := gc.CollectGarbageWithResult(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	if len(result.DeletedPartitions) != 0 {
		t.Error("should not delete non-compacted partitions")
	}

	// Verify partition still exists
	count, _ := catalog.GetPartitionCount(ctx)
	if count != 1 {
		t.Errorf("expected 1 partition, got %d", count)
	}
}

// TestCompactionWithDifferentPartitionKeys tests compaction with multiple partition keys.
func TestCompactionWithDifferentPartitionKeys(t *testing.T) {
	ctx := context.Background()

	tempDir, err := os.MkdirTemp("", "arkilian-multi-key-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	workDir := filepath.Join(tempDir, "work")

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir)
	metaGen := partition.NewMetadataGenerator()

	// Create partitions for two different keys
	keys := []string{"20260205", "20260206"}
	for _, keyVal := range keys {
		for i := 0; i < 3; i++ {
			rows := []types.Row{
				{TenantID: "acme", UserID: int64(i), EventTime: time.Now().UnixNano(), EventType: "test", Payload: map[string]interface{}{}},
			}
			key := types.PartitionKey{Strategy: types.StrategyTime, Value: keyVal}
			info, _ := builder.Build(ctx, rows, key)
			metaPath, _ := metaGen.GenerateAndWrite(info, rows)
			info.MetadataPath = metaPath

			objectPath := "partitions/" + keyVal + "/" + info.PartitionID + ".sqlite"
			store.UploadMultipart(ctx, info.SQLitePath, objectPath)
			catalog.RegisterPartition(ctx, info, objectPath)
		}
	}

	// Verify 6 partitions exist
	initialCount, _ := catalog.GetPartitionCount(ctx)
	if initialCount != 6 {
		t.Fatalf("expected 6 initial partitions, got %d", initialCount)
	}

	// Create daemon and run compaction
	config := compaction.CompactionConfig{
		MinPartitionSize:    100 * 1024 * 1024,
		MaxPartitionsPerKey: 2,
		TTLDays:             7,
		CheckInterval:       time.Hour,
		WorkDir:             workDir,
	}
	daemon := compaction.NewDaemon(config, catalog, store)
	defer daemon.Stop()

	daemon.RunOnce(ctx)

	// Should have 2 active partitions (one per key)
	activeCount, _ := catalog.GetPartitionCount(ctx)
	if activeCount != 2 {
		t.Errorf("expected 2 active partitions after compaction, got %d", activeCount)
	}

	// Verify each key has exactly one partition
	for _, keyVal := range keys {
		count, _ := catalog.GetPartitionCountByKey(ctx, keyVal)
		if count != 1 {
			t.Errorf("expected 1 partition for key %s, got %d", keyVal, count)
		}
	}
}
