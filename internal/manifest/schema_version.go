package manifest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
)

// SchemaVersionManager tracks schema versions in the manifest catalog.
// It supports registering new schema versions and detecting changes that
// require a version increment (Requirements 13.1, 13.2).
type SchemaVersionManager struct {
	db *sql.DB
}

// NewSchemaVersionManager creates a new schema version manager using the catalog's database.
func NewSchemaVersionManager(catalog *SQLiteCatalog) *SchemaVersionManager {
	return &SchemaVersionManager{db: catalog.db}
}

// SchemaVersionRecord represents a stored schema version.
type SchemaVersionRecord struct {
	Version   int
	Schema    types.Schema
	CreatedAt time.Time
}

// GetCurrentVersion returns the latest schema version number.
// Returns 0 if no schema versions have been registered.
func (m *SchemaVersionManager) GetCurrentVersion(ctx context.Context) (int, error) {
	var version int
	err := m.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0) FROM schema_versions",
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("schema_version: failed to get current version: %w", err)
	}
	return version, nil
}

// GetSchemaVersion retrieves a specific schema version record.
func (m *SchemaVersionManager) GetSchemaVersion(ctx context.Context, version int) (*SchemaVersionRecord, error) {
	var schemaJSON string
	var createdAtUnix int64

	err := m.db.QueryRowContext(ctx,
		"SELECT version, schema_json, created_at FROM schema_versions WHERE version = ?",
		version,
	).Scan(&version, &schemaJSON, &createdAtUnix)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("schema_version: version %d not found", version)
		}
		return nil, fmt.Errorf("schema_version: failed to get version %d: %w", version, err)
	}

	var schema types.Schema
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return nil, fmt.Errorf("schema_version: failed to unmarshal schema for version %d: %w", version, err)
	}

	return &SchemaVersionRecord{
		Version:   version,
		Schema:    schema,
		CreatedAt: time.Unix(createdAtUnix, 0),
	}, nil
}

// RegisterSchema registers a new schema. If the schema differs from the current
// version, a new version is created with an incremented version number.
// If the schema matches the current version, the existing version is returned.
func (m *SchemaVersionManager) RegisterSchema(ctx context.Context, schema types.Schema) (int, error) {
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return 0, err
	}

	// If there's an existing version, check if schema has changed
	if currentVersion > 0 {
		currentRecord, err := m.GetSchemaVersion(ctx, currentVersion)
		if err != nil {
			return 0, err
		}

		if schemasEqual(currentRecord.Schema, schema) {
			return currentVersion, nil
		}
	}

	// Schema changed (or first registration) — increment version
	newVersion := currentVersion + 1

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return 0, fmt.Errorf("schema_version: failed to marshal schema: %w", err)
	}

	_, err = m.db.ExecContext(ctx,
		"INSERT INTO schema_versions (version, schema_json, created_at) VALUES (?, ?, ?)",
		newVersion, string(schemaJSON), time.Now().Unix(),
	)
	if err != nil {
		return 0, fmt.Errorf("schema_version: failed to insert version %d: %w", newVersion, err)
	}

	return newVersion, nil
}

// ListVersions returns all registered schema versions ordered by version number.
func (m *SchemaVersionManager) ListVersions(ctx context.Context) ([]SchemaVersionRecord, error) {
	rows, err := m.db.QueryContext(ctx,
		"SELECT version, schema_json, created_at FROM schema_versions ORDER BY version ASC",
	)
	if err != nil {
		return nil, fmt.Errorf("schema_version: failed to list versions: %w", err)
	}
	defer rows.Close()

	var records []SchemaVersionRecord
	for rows.Next() {
		var version int
		var schemaJSON string
		var createdAtUnix int64

		if err := rows.Scan(&version, &schemaJSON, &createdAtUnix); err != nil {
			return nil, fmt.Errorf("schema_version: failed to scan version: %w", err)
		}

		var schema types.Schema
		if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
			return nil, fmt.Errorf("schema_version: failed to unmarshal schema: %w", err)
		}

		records = append(records, SchemaVersionRecord{
			Version:   version,
			Schema:    schema,
			CreatedAt: time.Unix(createdAtUnix, 0),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("schema_version: error iterating versions: %w", err)
	}

	return records, nil
}

// GetColumnsForVersion returns the column names for a given schema version.
func (m *SchemaVersionManager) GetColumnsForVersion(ctx context.Context, version int) ([]string, error) {
	record, err := m.GetSchemaVersion(ctx, version)
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(record.Schema.Columns))
	for i, col := range record.Schema.Columns {
		columns[i] = col.Name
	}
	return columns, nil
}

// GetColumnDiff returns columns present in newVersion but absent in oldVersion.
func (m *SchemaVersionManager) GetColumnDiff(ctx context.Context, oldVersion, newVersion int) ([]types.ColumnDef, error) {
	oldRecord, err := m.GetSchemaVersion(ctx, oldVersion)
	if err != nil {
		return nil, err
	}

	newRecord, err := m.GetSchemaVersion(ctx, newVersion)
	if err != nil {
		return nil, err
	}

	oldCols := make(map[string]bool)
	for _, col := range oldRecord.Schema.Columns {
		oldCols[col.Name] = true
	}

	var diff []types.ColumnDef
	for _, col := range newRecord.Schema.Columns {
		if !oldCols[col.Name] {
			diff = append(diff, col)
		}
	}

	return diff, nil
}

// schemasEqual compares two schemas for structural equality (ignoring version field).
func schemasEqual(a, b types.Schema) bool {
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i].Name != b.Columns[i].Name ||
			a.Columns[i].Type != b.Columns[i].Type ||
			a.Columns[i].Nullable != b.Columns[i].Nullable ||
			a.Columns[i].PrimaryKey != b.Columns[i].PrimaryKey {
			return false
		}
	}

	if len(a.Indexes) != len(b.Indexes) {
		return false
	}
	for i := range a.Indexes {
		if a.Indexes[i].Name != b.Indexes[i].Name || a.Indexes[i].Unique != b.Indexes[i].Unique {
			return false
		}
		if len(a.Indexes[i].Columns) != len(b.Indexes[i].Columns) {
			return false
		}
		for j := range a.Indexes[i].Columns {
			if a.Indexes[i].Columns[j] != b.Indexes[i].Columns[j] {
				return false
			}
		}
	}

	return true
}
