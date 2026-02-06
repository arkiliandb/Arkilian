package manifest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/partition"
)

func TestCatalog_RegisterAndGetPartition(t *testing.T) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "manifest_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Create catalog
	catalog, err := NewCatalog(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Create partition info
	info := &partition.PartitionInfo{
		PartitionID:   "test-partition-001",
		PartitionKey:  "20260205",
		RowCount:      1000,
		SizeBytes:     1024000,
		SchemaVersion: 1,
		CreatedAt:     time.Now(),
		MinMaxStats: map[string]partition.MinMax{
			"user_id":    {Min: int64(100), Max: int64(500)},
			"event_time": {Min: int64(1738713600), Max: int64(1738800000)},
			"tenant_id":  {Min: "acme", Max: "zebra"},
		},
	}

	// Register partition
	err = catalog.RegisterPartition(ctx, info, "s3://bucket/test-partition-001.sqlite")
	if err != nil {
		t.Fatalf("failed to register partition: %v", err)
	}

	// Get partition
	record, err := catalog.GetPartition(ctx, "test-partition-001")
	if err != nil {
		t.Fatalf("failed to get partition: %v", err)
	}

	// Verify fields
	if record.PartitionID != info.PartitionID {
		t.Errorf("partition_id mismatch: got %s, want %s", record.PartitionID, info.PartitionID)
	}
	if record.PartitionKey != info.PartitionKey {
		t.Errorf("partition_key mismatch: got %s, want %s", record.PartitionKey, info.PartitionKey)
	}
	if record.RowCount != info.RowCount {
		t.Errorf("row_count mismatch: got %d, want %d", record.RowCount, info.RowCount)
	}
	if record.SizeBytes != info.SizeBytes {
		t.Errorf("size_bytes mismatch: got %d, want %d", record.SizeBytes, info.SizeBytes)
	}
	if *record.MinUserID != int64(100) {
		t.Errorf("min_user_id mismatch: got %d, want 100", *record.MinUserID)
	}
	if *record.MaxUserID != int64(500) {
		t.Errorf("max_user_id mismatch: got %d, want 500", *record.MaxUserID)
	}
}

func TestCatalog_FindPartitions(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "manifest_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	catalog, err := NewCatalog(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Register multiple partitions
	partitions := []*partition.PartitionInfo{
		{
			PartitionID: "p1", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(1), Max: int64(100)}},
		},
		{
			PartitionID: "p2", PartitionKey: "20260205", RowCount: 200, SizeBytes: 2000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(101), Max: int64(200)}},
		},
		{
			PartitionID: "p3", PartitionKey: "20260206", RowCount: 300, SizeBytes: 3000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(201), Max: int64(300)}},
		},
	}

	for _, p := range partitions {
		if err := catalog.RegisterPartition(ctx, p, "s3://bucket/"+p.PartitionID+".sqlite"); err != nil {
			t.Fatalf("failed to register partition %s: %v", p.PartitionID, err)
		}
	}

	// Find partitions with user_id = 150 (should match p2)
	records, err := catalog.FindPartitions(ctx, []Predicate{
		{Column: "user_id", Operator: "=", Value: int64(150)},
	})
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 partition, got %d", len(records))
	}
	if len(records) > 0 && records[0].PartitionID != "p2" {
		t.Errorf("expected partition p2, got %s", records[0].PartitionID)
	}
}

func TestCatalog_MarkCompacted(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "manifest_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	catalog, err := NewCatalog(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Register source partitions
	sources := []string{"src1", "src2"}
	for _, id := range sources {
		info := &partition.PartitionInfo{
			PartitionID: id, PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
		}
		if err := catalog.RegisterPartition(ctx, info, "s3://bucket/"+id+".sqlite"); err != nil {
			t.Fatalf("failed to register partition %s: %v", id, err)
		}
	}

	// Register target partition
	target := &partition.PartitionInfo{
		PartitionID: "target", PartitionKey: "20260205", RowCount: 200, SizeBytes: 2000, SchemaVersion: 1, CreatedAt: time.Now(),
	}
	if err := catalog.RegisterPartition(ctx, target, "s3://bucket/target.sqlite"); err != nil {
		t.Fatalf("failed to register target partition: %v", err)
	}

	// Mark sources as compacted
	if err := catalog.MarkCompacted(ctx, sources, "target"); err != nil {
		t.Fatalf("failed to mark compacted: %v", err)
	}

	// Verify sources are marked as compacted
	for _, id := range sources {
		record, err := catalog.GetPartition(ctx, id)
		if err != nil {
			t.Fatalf("failed to get partition %s: %v", id, err)
		}
		if record.CompactedInto == nil || *record.CompactedInto != "target" {
			t.Errorf("partition %s should be compacted into target", id)
		}
	}

	// Verify compacted partitions are excluded from FindPartitions
	records, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("expected 1 active partition, got %d", len(records))
	}
	if len(records) > 0 && records[0].PartitionID != "target" {
		t.Errorf("expected target partition, got %s", records[0].PartitionID)
	}
}

func TestCatalog_IdempotencyKey(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "manifest_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	catalog, err := NewCatalog(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()

	info := &partition.PartitionInfo{
		PartitionID: "p1", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
	}

	// First registration with idempotency key
	id1, err := catalog.RegisterPartitionWithIdempotencyKey(ctx, info, "s3://bucket/p1.sqlite", "idem-key-001")
	if err != nil {
		t.Fatalf("failed to register with idempotency key: %v", err)
	}
	if id1 != "p1" {
		t.Errorf("expected partition ID p1, got %s", id1)
	}

	// Second registration with same idempotency key should return existing partition
	info2 := &partition.PartitionInfo{
		PartitionID: "p2", PartitionKey: "20260205", RowCount: 200, SizeBytes: 2000, SchemaVersion: 1, CreatedAt: time.Now(),
	}
	id2, err := catalog.RegisterPartitionWithIdempotencyKey(ctx, info2, "s3://bucket/p2.sqlite", "idem-key-001")
	if err != nil {
		t.Fatalf("failed to register with same idempotency key: %v", err)
	}
	if id2 != "p1" {
		t.Errorf("expected existing partition ID p1, got %s", id2)
	}

	// Verify only one partition exists
	count, err := catalog.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("failed to get partition count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 partition, got %d", count)
	}
}

func TestCatalog_GetCompactionCandidates(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "manifest_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	catalog, err := NewCatalog(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Register partitions with different sizes
	partitions := []*partition.PartitionInfo{
		{PartitionID: "small1", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000000, SchemaVersion: 1, CreatedAt: time.Now()},   // 1MB
		{PartitionID: "small2", PartitionKey: "20260205", RowCount: 200, SizeBytes: 2000000, SchemaVersion: 1, CreatedAt: time.Now()},   // 2MB
		{PartitionID: "large1", PartitionKey: "20260205", RowCount: 1000, SizeBytes: 10000000, SchemaVersion: 1, CreatedAt: time.Now()}, // 10MB
	}

	for _, p := range partitions {
		if err := catalog.RegisterPartition(ctx, p, "s3://bucket/"+p.PartitionID+".sqlite"); err != nil {
			t.Fatalf("failed to register partition %s: %v", p.PartitionID, err)
		}
	}

	// Get compaction candidates (partitions < 8MB)
	candidates, err := catalog.GetCompactionCandidates(ctx, "20260205", 8000000)
	if err != nil {
		t.Fatalf("failed to get compaction candidates: %v", err)
	}

	if len(candidates) != 2 {
		t.Errorf("expected 2 compaction candidates, got %d", len(candidates))
	}
}
