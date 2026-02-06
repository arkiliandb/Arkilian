package manifest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
)

func setupReconciliationTest(t *testing.T) (*SQLiteCatalog, *storage.LocalStorage, string, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "reconciliation-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "manifest.db")
	catalog, err := NewCatalog(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create catalog: %v", err)
	}

	storagePath := filepath.Join(tmpDir, "storage")
	store, err := storage.NewLocalStorage(storagePath)
	if err != nil {
		catalog.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	cleanup := func() {
		catalog.Close()
		os.RemoveAll(tmpDir)
	}

	return catalog, store, storagePath, cleanup
}

// createFakeObject creates a small file in local storage to simulate an uploaded partition.
func createFakeObject(t *testing.T, store *storage.LocalStorage, storagePath, objectPath string) {
	t.Helper()
	fullPath := filepath.Join(storagePath, objectPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("fake-partition-data"), 0644); err != nil {
		t.Fatalf("failed to write fake object: %v", err)
	}
}

func registerPartition(t *testing.T, catalog *SQLiteCatalog, id, key, objectPath string) {
	t.Helper()
	info := &partition.PartitionInfo{
		PartitionID:   id,
		PartitionKey:  key,
		RowCount:      100,
		SizeBytes:     1024,
		SchemaVersion: 1,
		CreatedAt:     time.Now(),
		MinMaxStats:   map[string]partition.MinMax{},
	}
	if err := catalog.RegisterPartition(context.Background(), info, objectPath); err != nil {
		t.Fatalf("failed to register partition: %v", err)
	}
}

func TestReconcile_NoIssues(t *testing.T) {
	catalog, store, storagePath, cleanup := setupReconciliationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register a partition and create the corresponding storage object.
	registerPartition(t, catalog, "p1", "20260205", "partitions/p1.sqlite")
	createFakeObject(t, store, storagePath, "partitions/p1.sqlite")

	report, err := Reconcile(ctx, catalog, store, "partitions")
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if report.HasIssues() {
		t.Errorf("expected no issues, got %d dangling, %d orphaned",
			len(report.DanglingEntries), len(report.OrphanedObjects))
	}
	if report.TotalManifestEntries != 1 {
		t.Errorf("expected 1 manifest entry, got %d", report.TotalManifestEntries)
	}
}

func TestReconcile_DanglingEntry(t *testing.T) {
	catalog, store, _, cleanup := setupReconciliationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register a partition but don't create the storage object.
	registerPartition(t, catalog, "p1", "20260205", "partitions/p1.sqlite")

	report, err := Reconcile(ctx, catalog, store, "partitions")
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if len(report.DanglingEntries) != 1 {
		t.Fatalf("expected 1 dangling entry, got %d", len(report.DanglingEntries))
	}
	if report.DanglingEntries[0].PartitionID != "p1" {
		t.Errorf("expected dangling partition p1, got %s", report.DanglingEntries[0].PartitionID)
	}
}

func TestReconcile_OrphanedObject(t *testing.T) {
	catalog, store, storagePath, cleanup := setupReconciliationTest(t)
	defer cleanup()

	ctx := context.Background()

	// Register a partition with its object, plus create an extra orphaned object.
	registerPartition(t, catalog, "p1", "20260205", "partitions/p1.sqlite")
	createFakeObject(t, store, storagePath, "partitions/p1.sqlite")
	createFakeObject(t, store, storagePath, "partitions/orphan.sqlite")

	report, err := Reconcile(ctx, catalog, store, "partitions")
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if len(report.OrphanedObjects) != 1 {
		t.Fatalf("expected 1 orphaned object, got %d", len(report.OrphanedObjects))
	}
	if report.OrphanedObjects[0] != "partitions/orphan.sqlite" {
		t.Errorf("expected orphan path partitions/orphan.sqlite, got %s", report.OrphanedObjects[0])
	}
}

func TestReconcile_EmptyManifestAndStorage(t *testing.T) {
	catalog, store, _, cleanup := setupReconciliationTest(t)
	defer cleanup()

	ctx := context.Background()

	report, err := Reconcile(ctx, catalog, store, "partitions")
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if report.HasIssues() {
		t.Error("expected no issues for empty state")
	}
	if report.TotalManifestEntries != 0 || report.TotalStorageObjects != 0 {
		t.Errorf("expected 0/0, got %d/%d", report.TotalManifestEntries, report.TotalStorageObjects)
	}
}
