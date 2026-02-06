package manifest

import (
	"context"
	"os"
	"testing"

	"github.com/arkilian/arkilian/pkg/types"
)

func newTestCatalog(t *testing.T) (*SQLiteCatalog, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "schema_version_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()

	catalog, err := NewCatalog(tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("failed to create catalog: %v", err)
	}

	return catalog, func() {
		catalog.Close()
		os.Remove(tmpFile.Name())
	}
}

func TestSchemaVersionManager_RegisterAndGet(t *testing.T) {
	catalog, cleanup := newTestCatalog(t)
	defer cleanup()

	mgr := NewSchemaVersionManager(catalog)
	ctx := context.Background()

	schema := types.Schema{
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
		},
	}

	// Register first schema
	v, err := mgr.RegisterSchema(ctx, schema)
	if err != nil {
		t.Fatalf("failed to register schema: %v", err)
	}
	if v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}

	// Retrieve it
	record, err := mgr.GetSchemaVersion(ctx, 1)
	if err != nil {
		t.Fatalf("failed to get schema version: %v", err)
	}
	if len(record.Schema.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(record.Schema.Columns))
	}
	if record.Schema.Columns[0].Name != "event_id" {
		t.Errorf("expected first column event_id, got %s", record.Schema.Columns[0].Name)
	}
}

func TestSchemaVersionManager_IncrementOnChange(t *testing.T) {
	catalog, cleanup := newTestCatalog(t)
	defer cleanup()

	mgr := NewSchemaVersionManager(catalog)
	ctx := context.Background()

	schemaV1 := types.Schema{
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
		},
	}

	v1, err := mgr.RegisterSchema(ctx, schemaV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}
	if v1 != 1 {
		t.Errorf("expected version 1, got %d", v1)
	}

	// Register same schema again — should not increment
	v1Again, err := mgr.RegisterSchema(ctx, schemaV1)
	if err != nil {
		t.Fatalf("failed to re-register v1: %v", err)
	}
	if v1Again != 1 {
		t.Errorf("expected version 1 (no change), got %d", v1Again)
	}

	// Register a changed schema — should increment to 2
	schemaV2 := types.Schema{
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
			{Name: "region", Type: "TEXT", Nullable: true},
		},
	}

	v2, err := mgr.RegisterSchema(ctx, schemaV2)
	if err != nil {
		t.Fatalf("failed to register v2: %v", err)
	}
	if v2 != 2 {
		t.Errorf("expected version 2, got %d", v2)
	}

	// Verify current version
	current, err := mgr.GetCurrentVersion(ctx)
	if err != nil {
		t.Fatalf("failed to get current version: %v", err)
	}
	if current != 2 {
		t.Errorf("expected current version 2, got %d", current)
	}
}

func TestSchemaVersionManager_GetColumnDiff(t *testing.T) {
	catalog, cleanup := newTestCatalog(t)
	defer cleanup()

	mgr := NewSchemaVersionManager(catalog)
	ctx := context.Background()

	schemaV1 := types.Schema{
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
		},
	}
	schemaV2 := types.Schema{
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
			{Name: "region", Type: "TEXT", Nullable: true},
			{Name: "priority", Type: "INTEGER", Nullable: true},
		},
	}

	mgr.RegisterSchema(ctx, schemaV1)
	mgr.RegisterSchema(ctx, schemaV2)

	diff, err := mgr.GetColumnDiff(ctx, 1, 2)
	if err != nil {
		t.Fatalf("failed to get column diff: %v", err)
	}
	if len(diff) != 2 {
		t.Fatalf("expected 2 new columns, got %d", len(diff))
	}
	if diff[0].Name != "region" {
		t.Errorf("expected first diff column 'region', got %s", diff[0].Name)
	}
	if diff[1].Name != "priority" {
		t.Errorf("expected second diff column 'priority', got %s", diff[1].Name)
	}
}

func TestSchemaVersionManager_ListVersions(t *testing.T) {
	catalog, cleanup := newTestCatalog(t)
	defer cleanup()

	mgr := NewSchemaVersionManager(catalog)
	ctx := context.Background()

	// Register 3 different schemas
	for i := 0; i < 3; i++ {
		cols := make([]types.ColumnDef, i+1)
		for j := 0; j <= i; j++ {
			cols[j] = types.ColumnDef{Name: string(rune('a' + j)), Type: "TEXT"}
		}
		if _, err := mgr.RegisterSchema(ctx, types.Schema{Columns: cols}); err != nil {
			t.Fatalf("failed to register schema %d: %v", i+1, err)
		}
	}

	versions, err := mgr.ListVersions(ctx)
	if err != nil {
		t.Fatalf("failed to list versions: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
	for i, v := range versions {
		if v.Version != i+1 {
			t.Errorf("expected version %d, got %d", i+1, v.Version)
		}
	}
}
