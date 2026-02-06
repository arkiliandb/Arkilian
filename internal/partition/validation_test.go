package partition

import (
	"testing"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
)

func TestSchemaValidator_ValidateRow(t *testing.T) {
	validator := DefaultValidator()

	tests := []struct {
		name      string
		row       types.Row
		wantError bool
		errField  string
	}{
		{
			name: "valid row",
			row: types.Row{
				TenantID:  "acme",
				UserID:    12345,
				EventTime: time.Now().UnixNano(),
				EventType: "page_view",
				Payload:   map[string]interface{}{"page": "/home"},
			},
			wantError: false,
		},
		{
			name: "empty tenant_id",
			row: types.Row{
				TenantID:  "",
				UserID:    12345,
				EventTime: time.Now().UnixNano(),
				EventType: "page_view",
				Payload:   map[string]interface{}{},
			},
			wantError: true,
			errField:  "tenant_id",
		},
		{
			name: "empty event_type",
			row: types.Row{
				TenantID:  "acme",
				UserID:    12345,
				EventTime: time.Now().UnixNano(),
				EventType: "",
				Payload:   map[string]interface{}{},
			},
			wantError: true,
			errField:  "event_type",
		},
		{
			name: "invalid event_time",
			row: types.Row{
				TenantID:  "acme",
				UserID:    12345,
				EventTime: 0,
				EventType: "test",
				Payload:   map[string]interface{}{},
			},
			wantError: true,
			errField:  "event_time",
		},
		{
			name: "nil payload",
			row: types.Row{
				TenantID:  "acme",
				UserID:    12345,
				EventTime: time.Now().UnixNano(),
				EventType: "test",
				Payload:   nil,
			},
			wantError: true,
			errField:  "payload",
		},
		{
			name: "invalid event_id length",
			row: types.Row{
				EventID:   []byte{1, 2, 3}, // Should be 16 bytes
				TenantID:  "acme",
				UserID:    12345,
				EventTime: time.Now().UnixNano(),
				EventType: "test",
				Payload:   map[string]interface{}{},
			},
			wantError: true,
			errField:  "event_id",
		},
		{
			name: "valid event_id",
			row: types.Row{
				EventID:   make([]byte, 16),
				TenantID:  "acme",
				UserID:    12345,
				EventTime: time.Now().UnixNano(),
				EventType: "test",
				Payload:   map[string]interface{}{},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := validator.ValidateRow(tt.row, 0)
			if tt.wantError {
				if len(errors) == 0 {
					t.Errorf("expected validation error for field %s", tt.errField)
				} else {
					found := false
					for _, err := range errors {
						if err.Field == tt.errField {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected error for field %s, got errors: %v", tt.errField, errors)
					}
				}
			} else {
				if len(errors) > 0 {
					t.Errorf("unexpected validation errors: %v", errors)
				}
			}
		})
	}
}

func TestSchemaValidator_ValidateRows(t *testing.T) {
	validator := DefaultValidator()

	rows := []types.Row{
		{
			TenantID:  "acme",
			UserID:    12345,
			EventTime: time.Now().UnixNano(),
			EventType: "test",
			Payload:   map[string]interface{}{},
		},
		{
			TenantID:  "", // Invalid
			UserID:    67890,
			EventTime: time.Now().UnixNano(),
			EventType: "test",
			Payload:   map[string]interface{}{},
		},
		{
			TenantID:  "beta",
			UserID:    11111,
			EventTime: 0, // Invalid
			EventType: "",  // Invalid
			Payload:   map[string]interface{}{},
		},
	}

	errors := validator.ValidateRows(rows)
	if len(errors) != 3 {
		t.Errorf("expected 3 validation errors, got %d", len(errors))
	}
}

func TestValidateSchema(t *testing.T) {
	tests := []struct {
		name      string
		schema    types.Schema
		wantError bool
	}{
		{
			name:      "valid default schema",
			schema:    DefaultSchema(),
			wantError: false,
		},
		{
			name: "invalid version",
			schema: types.Schema{
				Version: 0,
				Columns: []types.ColumnDef{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
				},
			},
			wantError: true,
		},
		{
			name: "no columns",
			schema: types.Schema{
				Version: 1,
				Columns: []types.ColumnDef{},
			},
			wantError: true,
		},
		{
			name: "no primary key",
			schema: types.Schema{
				Version: 1,
				Columns: []types.ColumnDef{
					{Name: "id", Type: "INTEGER", PrimaryKey: false},
				},
			},
			wantError: true,
		},
		{
			name: "invalid column type",
			schema: types.Schema{
				Version: 1,
				Columns: []types.ColumnDef{
					{Name: "id", Type: "INVALID", PrimaryKey: true},
				},
			},
			wantError: true,
		},
		{
			name: "duplicate column name",
			schema: types.Schema{
				Version: 1,
				Columns: []types.ColumnDef{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "id", Type: "TEXT"},
				},
			},
			wantError: true,
		},
		{
			name: "index references unknown column",
			schema: types.Schema{
				Version: 1,
				Columns: []types.ColumnDef{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
				},
				Indexes: []types.IndexDef{
					{Name: "idx_unknown", Columns: []string{"unknown_col"}},
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSchema(tt.schema)
			if tt.wantError && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
