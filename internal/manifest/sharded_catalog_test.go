package manifest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/partition"
)

func newTestShardedCatalog(t *testing.T, shardCount int) *ShardedCatalog {
	t.Helper()
	dir := t.TempDir()
	sc, err := NewShardedCatalog(dir, shardCount)
	if err != nil {
		t.Fatalf("NewShardedCatalog: %v", err)
	}
	t.Cleanup(func() { sc.Close() })
	return sc
}

func makePartitionInfo(id, key string, sizeBytes int64) *partition.PartitionInfo {
	return &partition.PartitionInfo{
		PartitionID:  id,
		PartitionKey: key,
		RowCount:     100,
		SizeBytes:    sizeBytes,
		SchemaVersion: 1,
		CreatedAt:    time.Now(),
		MinMaxStats: map[string]partition.MinMax{
			"event_time": {Min: int64(1000), Max: int64(2000)},
			"user_id":    {Min: int64(1), Max: int64(100)},
		},
	}
}

func TestShardedCatalog_RegisterAndGet(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	info := makePartitionInfo("p1", "20260205", 1024)
	if err := sc.RegisterPartition(ctx, info, "s3://bucket/p1.sqlite"); err != nil {
		t.Fatalf("RegisterPartition: %v", err)
	}

	record, err := sc.GetPartition(ctx, "p1")
	if err != nil {
		t.Fatalf("GetPartition: %v", err)
	}
	if record.PartitionID != "p1" {
		t.Errorf("got ID %q, want %q", record.PartitionID, "p1")
	}
	if record.PartitionKey != "20260205" {
		t.Errorf("got key %q, want %q", record.PartitionKey, "20260205")
	}
}

func TestShardedCatalog_FindPartitions(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	// Register partitions across different keys (will land in different shards)
	keys := []string{"20260201", "20260202", "20260203", "20260204", "20260205"}
	for i, key := range keys {
		info := makePartitionInfo(fmt.Sprintf("p%d", i), key, 1024)
		if err := sc.RegisterPartition(ctx, info, fmt.Sprintf("s3://bucket/p%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition: %v", err)
		}
	}

	// FindPartitions with no predicates should return all
	records, err := sc.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("FindPartitions: %v", err)
	}
	if len(records) != 5 {
		t.Errorf("got %d records, want 5", len(records))
	}
}

func TestShardedCatalog_PartitionCount(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("202602%02d", i+1)
		info := makePartitionInfo(fmt.Sprintf("p%d", i), key, 1024)
		if err := sc.RegisterPartition(ctx, info, fmt.Sprintf("s3://bucket/p%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition: %v", err)
		}
	}

	count, err := sc.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("GetPartitionCount: %v", err)
	}
	if count != 10 {
		t.Errorf("got count %d, want 10", count)
	}
}

func TestShardedCatalog_SameKeyGoesToSameShard(t *testing.T) {
	sc := newTestShardedCatalog(t, 8)
	ctx := context.Background()

	// Register multiple partitions with the same key
	for i := 0; i < 5; i++ {
		info := makePartitionInfo(fmt.Sprintf("p%d", i), "20260205", int64(1024*(i+1)))
		if err := sc.RegisterPartition(ctx, info, fmt.Sprintf("s3://bucket/p%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition: %v", err)
		}
	}

	// All should be in the same shard
	shard := sc.Shard("20260205")
	count, err := shard.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("GetPartitionCount: %v", err)
	}
	if count != 5 {
		t.Errorf("shard has %d partitions, want 5 (all same key should be in same shard)", count)
	}
}

func TestShardedCatalog_CompactionCandidates(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	// Register small partitions for the same key
	for i := 0; i < 3; i++ {
		info := makePartitionInfo(fmt.Sprintf("p%d", i), "20260205", 1024) // 1KB, well under 8MB
		if err := sc.RegisterPartition(ctx, info, fmt.Sprintf("s3://bucket/p%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition: %v", err)
		}
	}

	candidates, err := sc.GetCompactionCandidates(ctx, "20260205", 8*1024*1024)
	if err != nil {
		t.Fatalf("GetCompactionCandidates: %v", err)
	}
	if len(candidates) != 3 {
		t.Errorf("got %d candidates, want 3", len(candidates))
	}
}

func TestShardedCatalog_DeleteExpired(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	// Register and compact partitions across different keys
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("202602%02d", i+1)
		// Source partition
		src := makePartitionInfo(fmt.Sprintf("src%d", i), key, 1024)
		src.CreatedAt = time.Now().Add(-48 * time.Hour) // 2 days old
		if err := sc.RegisterPartition(ctx, src, fmt.Sprintf("s3://bucket/src%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition src: %v", err)
		}
		// Target partition
		tgt := makePartitionInfo(fmt.Sprintf("tgt%d", i), key, 2048)
		if err := sc.RegisterPartition(ctx, tgt, fmt.Sprintf("s3://bucket/tgt%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition tgt: %v", err)
		}
		// Mark compacted
		shard := sc.Shard(key)
		if err := shard.MarkCompacted(ctx, []string{fmt.Sprintf("src%d", i)}, fmt.Sprintf("tgt%d", i)); err != nil {
			t.Fatalf("MarkCompacted: %v", err)
		}
	}

	// Delete with 24h TTL — the 2-day-old compacted sources should be deleted
	deleted, err := sc.DeleteExpired(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if len(deleted) != 4 {
		t.Errorf("got %d deleted, want 4", len(deleted))
	}
}

func TestShardedCatalog_IdempotencyKey(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	info := makePartitionInfo("p1", "20260205", 1024)

	id1, err := sc.RegisterPartitionWithIdempotencyKey(ctx, info, "s3://bucket/p1.sqlite", "idem-key-1")
	if err != nil {
		t.Fatalf("first register: %v", err)
	}

	// Second call with same idempotency key should return same ID
	id2, err := sc.RegisterPartitionWithIdempotencyKey(ctx, info, "s3://bucket/p1.sqlite", "idem-key-1")
	if err != nil {
		t.Fatalf("second register: %v", err)
	}
	if id1 != id2 {
		t.Errorf("idempotency failed: got %q then %q", id1, id2)
	}
}

func TestShardedCatalog_DistinctKeys(t *testing.T) {
	sc := newTestShardedCatalog(t, 4)
	ctx := context.Background()

	keys := []string{"20260201", "20260202", "20260203"}
	for i, key := range keys {
		info := makePartitionInfo(fmt.Sprintf("p%d", i), key, 1024)
		if err := sc.RegisterPartition(ctx, info, fmt.Sprintf("s3://bucket/p%d.sqlite", i)); err != nil {
			t.Fatalf("RegisterPartition: %v", err)
		}
	}

	distinct, err := sc.GetDistinctPartitionKeys(ctx)
	if err != nil {
		t.Fatalf("GetDistinctPartitionKeys: %v", err)
	}
	if len(distinct) != 3 {
		t.Errorf("got %d distinct keys, want 3", len(distinct))
	}
}

func TestShardedCatalog_ShardFilesCreated(t *testing.T) {
	dir := t.TempDir()
	sc, err := NewShardedCatalog(dir, 4)
	if err != nil {
		t.Fatalf("NewShardedCatalog: %v", err)
	}
	defer sc.Close()

	for i := 0; i < 4; i++ {
		path := filepath.Join(dir, fmt.Sprintf("manifest_shard_%04d.db", i))
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("shard file %s does not exist", path)
		}
	}
}

func TestShardedCatalog_ImplementsCatalogInterface(t *testing.T) {
	// Compile-time check that ShardedCatalog implements Catalog
	var _ Catalog = (*ShardedCatalog)(nil)
}

func TestShardedCatalog_ImplementsCatalogReader(t *testing.T) {
	// Compile-time check that ShardedCatalog implements CatalogReader
	var _ CatalogReader = (*ShardedCatalog)(nil)
	// And SQLiteCatalog too
	var _ CatalogReader = (*SQLiteCatalog)(nil)
}
