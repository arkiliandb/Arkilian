package manifest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/partition"
)

func TestPruner_PruneByTimeRange(t *testing.T) {
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
	pruner := NewPruner(catalog)

	// Register partitions with different time ranges
	partitions := []*partition.PartitionInfo{
		{
			PartitionID: "p1", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"event_time": {Min: int64(1000), Max: int64(2000)}},
		},
		{
			PartitionID: "p2", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"event_time": {Min: int64(3000), Max: int64(4000)}},
		},
		{
			PartitionID: "p3", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"event_time": {Min: int64(5000), Max: int64(6000)}},
		},
	}

	for _, p := range partitions {
		if err := catalog.RegisterPartition(ctx, p, "s3://bucket/"+p.PartitionID+".sqlite"); err != nil {
			t.Fatalf("failed to register partition %s: %v", p.PartitionID, err)
		}
	}

	// Query for time range [2500, 3500] - should match p2
	records, err := pruner.PruneByTimeRange(ctx, 2500, 3500)
	if err != nil {
		t.Fatalf("failed to prune by time range: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 partition, got %d", len(records))
	}
	if len(records) > 0 && records[0].PartitionID != "p2" {
		t.Errorf("expected partition p2, got %s", records[0].PartitionID)
	}
}

func TestPruner_PruneByUserID(t *testing.T) {
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
	pruner := NewPruner(catalog)

	// Register partitions with different user ID ranges
	partitions := []*partition.PartitionInfo{
		{
			PartitionID: "p1", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(1), Max: int64(100)}},
		},
		{
			PartitionID: "p2", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(101), Max: int64(200)}},
		},
	}

	for _, p := range partitions {
		if err := catalog.RegisterPartition(ctx, p, "s3://bucket/"+p.PartitionID+".sqlite"); err != nil {
			t.Fatalf("failed to register partition %s: %v", p.PartitionID, err)
		}
	}

	// Query for user_id = 150 - should match p2
	records, err := pruner.PruneByUserID(ctx, 150)
	if err != nil {
		t.Fatalf("failed to prune by user ID: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 partition, got %d", len(records))
	}
	if len(records) > 0 && records[0].PartitionID != "p2" {
		t.Errorf("expected partition p2, got %s", records[0].PartitionID)
	}
}

func TestPruner_PruneByTenantID(t *testing.T) {
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
	pruner := NewPruner(catalog)

	// Register partitions with different tenant ID ranges
	partitions := []*partition.PartitionInfo{
		{
			PartitionID: "p1", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"tenant_id": {Min: "aaa", Max: "ccc"}},
		},
		{
			PartitionID: "p2", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"tenant_id": {Min: "ddd", Max: "fff"}},
		},
	}

	for _, p := range partitions {
		if err := catalog.RegisterPartition(ctx, p, "s3://bucket/"+p.PartitionID+".sqlite"); err != nil {
			t.Fatalf("failed to register partition %s: %v", p.PartitionID, err)
		}
	}

	// Query for tenant_id = "eee" - should match p2
	records, err := pruner.PruneByTenantID(ctx, "eee")
	if err != nil {
		t.Fatalf("failed to prune by tenant ID: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 partition, got %d", len(records))
	}
	if len(records) > 0 && records[0].PartitionID != "p2" {
		t.Errorf("expected partition p2, got %s", records[0].PartitionID)
	}
}

func TestPruner_CompactedPartitionsExcluded(t *testing.T) {
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
	pruner := NewPruner(catalog)

	// Register source and target partitions
	source := &partition.PartitionInfo{
		PartitionID: "src", PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
		MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(1), Max: int64(100)}},
	}
	target := &partition.PartitionInfo{
		PartitionID: "tgt", PartitionKey: "20260205", RowCount: 100, SizeBytes: 2000, SchemaVersion: 1, CreatedAt: time.Now(),
		MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(1), Max: int64(100)}},
	}

	if err := catalog.RegisterPartition(ctx, source, "s3://bucket/src.sqlite"); err != nil {
		t.Fatalf("failed to register source: %v", err)
	}
	if err := catalog.RegisterPartition(ctx, target, "s3://bucket/tgt.sqlite"); err != nil {
		t.Fatalf("failed to register target: %v", err)
	}

	// Mark source as compacted
	if err := catalog.MarkCompacted(ctx, []string{"src"}, "tgt"); err != nil {
		t.Fatalf("failed to mark compacted: %v", err)
	}

	// Query should only return target
	records, err := pruner.PruneByUserID(ctx, 50)
	if err != nil {
		t.Fatalf("failed to prune: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 partition, got %d", len(records))
	}
	if len(records) > 0 && records[0].PartitionID != "tgt" {
		t.Errorf("expected target partition, got %s", records[0].PartitionID)
	}
}

func TestPruner_PruneWithPredicates(t *testing.T) {
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
	pruner := NewPruner(catalog)

	// Register 10 partitions
	for i := 0; i < 10; i++ {
		p := &partition.PartitionInfo{
			PartitionID: "p" + string(rune('0'+i)), PartitionKey: "20260205", RowCount: 100, SizeBytes: 1000, SchemaVersion: 1, CreatedAt: time.Now(),
			MinMaxStats: map[string]partition.MinMax{"user_id": {Min: int64(i * 100), Max: int64((i + 1) * 100)}},
		}
		if err := catalog.RegisterPartition(ctx, p, "s3://bucket/"+p.PartitionID+".sqlite"); err != nil {
			t.Fatalf("failed to register partition: %v", err)
		}
	}

	// Query for user_id = 550 - should match p5
	result, err := pruner.PruneWithPredicates(ctx, []Predicate{
		{Column: "user_id", Operator: "=", Value: int64(550)},
	})
	if err != nil {
		t.Fatalf("failed to prune with predicates: %v", err)
	}

	if result.TotalScanned != 10 {
		t.Errorf("expected 10 total scanned, got %d", result.TotalScanned)
	}
	if len(result.Partitions) != 1 {
		t.Errorf("expected 1 matching partition, got %d", len(result.Partitions))
	}
	if result.TotalPruned != 9 {
		t.Errorf("expected 9 pruned, got %d", result.TotalPruned)
	}
	if result.PruningRatio < 0.89 || result.PruningRatio > 0.91 {
		t.Errorf("expected pruning ratio ~0.9, got %f", result.PruningRatio)
	}
}

func TestOverlapCheck(t *testing.T) {
	oc := OverlapCheck{}

	// Test time range overlaps
	if !oc.TimeRangeOverlaps(100, 200, 150, 250) {
		t.Error("expected overlap for [100,200] and [150,250]")
	}
	if oc.TimeRangeOverlaps(100, 200, 300, 400) {
		t.Error("expected no overlap for [100,200] and [300,400]")
	}

	// Test point in range
	if !oc.PointInRange(150, 100, 200) {
		t.Error("expected 150 to be in range [100,200]")
	}
	if oc.PointInRange(50, 100, 200) {
		t.Error("expected 50 to not be in range [100,200]")
	}

	// Test string range overlaps
	if !oc.StringRangeOverlaps("aaa", "ccc", "bbb", "ddd") {
		t.Error("expected overlap for [aaa,ccc] and [bbb,ddd]")
	}
	if oc.StringRangeOverlaps("aaa", "bbb", "ccc", "ddd") {
		t.Error("expected no overlap for [aaa,bbb] and [ccc,ddd]")
	}
}
