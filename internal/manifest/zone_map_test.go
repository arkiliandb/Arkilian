package manifest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/arkilian/arkilian/internal/bloom"
)

func setupTestCatalogForZoneMaps(t *testing.T) (*SQLiteCatalog, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "zonemap_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dbPath := filepath.Join(dir, "manifest.db")
	catalog, err := NewCatalog(dbPath)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create catalog: %v", err)
	}
	return catalog, func() {
		catalog.Close()
		os.RemoveAll(dir)
	}
}

func TestUpsertAndGetZoneMap(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	bf := bloom.NewWithEstimates(100, 0.01)
	bf.Add([]byte("tenant_abc"))
	bf.Add([]byte("tenant_xyz"))

	entry := &ZoneMapEntry{
		PartitionKey:  "20250101",
		ColumnName:    "tenant_id",
		BloomFilter:   bf,
		ItemCount:     2,
		DistinctCount: 2,
	}

	if err := catalog.UpsertZoneMap(ctx, entry); err != nil {
		t.Fatalf("UpsertZoneMap failed: %v", err)
	}

	got, err := catalog.GetZoneMap(ctx, "20250101", "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMap failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected zone map entry, got nil")
	}
	if got.ItemCount != 2 {
		t.Errorf("expected ItemCount=2, got %d", got.ItemCount)
	}
	if got.DistinctCount != 2 {
		t.Errorf("expected DistinctCount=2, got %d", got.DistinctCount)
	}
	if !got.BloomFilter.Contains([]byte("tenant_abc")) {
		t.Error("bloom filter should contain tenant_abc")
	}
	if !got.BloomFilter.Contains([]byte("tenant_xyz")) {
		t.Error("bloom filter should contain tenant_xyz")
	}
}

func TestGetZoneMapNotFound(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	got, err := catalog.GetZoneMap(ctx, "nonexistent", "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMap failed: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil for nonexistent zone map")
	}
}

func TestGetZoneMapsForKeys(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	// Insert zone maps for 3 keys
	for _, key := range []string{"20250101", "20250102", "20250103"} {
		bf := bloom.NewWithEstimates(10, 0.01)
		bf.Add([]byte("val_" + key))
		entry := &ZoneMapEntry{
			PartitionKey:  key,
			ColumnName:    "tenant_id",
			BloomFilter:   bf,
			ItemCount:     1,
			DistinctCount: 1,
		}
		if err := catalog.UpsertZoneMap(ctx, entry); err != nil {
			t.Fatalf("UpsertZoneMap failed for %s: %v", key, err)
		}
	}

	// Query 2 of the 3 keys
	result, err := catalog.GetZoneMapsForKeys(ctx, []string{"20250101", "20250103"}, "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMapsForKeys failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if _, ok := result["20250101"]; !ok {
		t.Error("missing key 20250101")
	}
	if _, ok := result["20250103"]; !ok {
		t.Error("missing key 20250103")
	}

	// Query with a missing key
	result, err = catalog.GetZoneMapsForKeys(ctx, []string{"20250101", "nonexistent"}, "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMapsForKeys failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
}

func TestMergeIntoZoneMap(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	// First merge — creates the zone map
	bf1 := bloom.NewWithEstimates(100, 0.01)
	bf1.Add([]byte("tenant_a"))
	if err := catalog.MergeIntoZoneMap(ctx, "20250101", "tenant_id", bf1, 1); err != nil {
		t.Fatalf("first MergeIntoZoneMap failed: %v", err)
	}

	// Second merge — OR's into existing
	bf2 := bloom.NewWithEstimates(100, 0.01)
	bf2.Add([]byte("tenant_b"))
	if err := catalog.MergeIntoZoneMap(ctx, "20250101", "tenant_id", bf2, 1); err != nil {
		t.Fatalf("second MergeIntoZoneMap failed: %v", err)
	}

	// Verify merged filter contains both values
	got, err := catalog.GetZoneMap(ctx, "20250101", "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMap failed: %v", err)
	}
	if !got.BloomFilter.Contains([]byte("tenant_a")) {
		t.Error("merged filter should contain tenant_a")
	}
	if !got.BloomFilter.Contains([]byte("tenant_b")) {
		t.Error("merged filter should contain tenant_b")
	}
	if got.DistinctCount != 2 {
		t.Errorf("expected DistinctCount=2, got %d", got.DistinctCount)
	}
}

func TestDeleteZoneMap(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	// Insert zone maps for two columns
	for _, col := range []string{"tenant_id", "user_id"} {
		bf := bloom.NewWithEstimates(10, 0.01)
		bf.Add([]byte("val"))
		entry := &ZoneMapEntry{
			PartitionKey:  "20250101",
			ColumnName:    col,
			BloomFilter:   bf,
			ItemCount:     1,
			DistinctCount: 1,
		}
		if err := catalog.UpsertZoneMap(ctx, entry); err != nil {
			t.Fatalf("UpsertZoneMap failed: %v", err)
		}
	}

	// Delete all zone maps for the key
	if err := catalog.DeleteZoneMap(ctx, "20250101"); err != nil {
		t.Fatalf("DeleteZoneMap failed: %v", err)
	}

	// Verify both columns are gone
	for _, col := range []string{"tenant_id", "user_id"} {
		got, err := catalog.GetZoneMap(ctx, "20250101", col)
		if err != nil {
			t.Fatalf("GetZoneMap failed: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for deleted zone map column %s", col)
		}
	}
}

func TestUpdateZoneMapsFromMetadata(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	// Simulate bloom filters from a metadata sidecar
	tenantBF := bloom.NewWithEstimates(10, 0.01)
	tenantBF.Add([]byte("t1"))
	tenantBF.Add([]byte("t2"))

	userBF := bloom.NewWithEstimates(10, 0.01)
	userBF.Add([]byte{0, 0, 0, 0, 0, 0, 0, 42}) // user_id=42 big-endian

	filters := map[string]*bloom.BloomFilter{
		"tenant_id": tenantBF,
		"user_id":   userBF,
	}
	distinctCounts := map[string]int{
		"tenant_id": 2,
		"user_id":   1,
	}

	if err := catalog.UpdateZoneMapsFromMetadata(ctx, "20250101", filters, distinctCounts); err != nil {
		t.Fatalf("UpdateZoneMapsFromMetadata failed: %v", err)
	}

	// Verify tenant_id zone map
	got, err := catalog.GetZoneMap(ctx, "20250101", "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMap failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected tenant_id zone map")
	}
	if !got.BloomFilter.Contains([]byte("t1")) {
		t.Error("tenant_id zone map should contain t1")
	}

	// Verify user_id zone map
	got, err = catalog.GetZoneMap(ctx, "20250101", "user_id")
	if err != nil {
		t.Fatalf("GetZoneMap failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected user_id zone map")
	}
}

func TestRebuildZoneMap(t *testing.T) {
	catalog, cleanup := setupTestCatalogForZoneMaps(t)
	defer cleanup()
	ctx := context.Background()

	// Insert a zone map
	bf := bloom.NewWithEstimates(10, 0.01)
	bf.Add([]byte("val"))
	entry := &ZoneMapEntry{
		PartitionKey:  "20250101",
		ColumnName:    "tenant_id",
		BloomFilter:   bf,
		ItemCount:     1,
		DistinctCount: 1,
	}
	if err := catalog.UpsertZoneMap(ctx, entry); err != nil {
		t.Fatalf("UpsertZoneMap failed: %v", err)
	}

	// Rebuild (deletes existing)
	if err := catalog.RebuildZoneMap(ctx, "20250101"); err != nil {
		t.Fatalf("RebuildZoneMap failed: %v", err)
	}

	// Verify it's gone
	got, err := catalog.GetZoneMap(ctx, "20250101", "tenant_id")
	if err != nil {
		t.Fatalf("GetZoneMap failed: %v", err)
	}
	if got != nil {
		t.Error("expected nil after rebuild")
	}
}
