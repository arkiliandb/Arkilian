package schema

import (
	"testing"

	"github.com/arkilian/arkilian/internal/observability"
)

func TestGetMaterializedColumns(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")
	stats.RecordJSONPath("$.user_agent")
	stats.RecordJSONPath("$.user_agent")
	stats.RecordJSONPath("$.device.id")
	stats.RecordJSONPath("$.device.id")

	materializer := NewMaterializer(stats, 2) // threshold = 2

	columns := materializer.GetMaterializedColumns("test_collection")

	if len(columns) != 2 {
		t.Errorf("Expected 2 materialized columns, got %d", len(columns))
	}

	// Check that user_agent is included (frequency = 3 >= threshold = 2)
	found := false
	for _, col := range columns {
		if col.JSONPath == "$.user_agent" {
			found = true
			if col.ColumnName != "payload_user_agent" {
				t.Errorf("Expected column name 'payload_user_agent', got '%s'", col.ColumnName)
			}
			if col.SQLiteType != "TEXT" {
				t.Errorf("Expected SQLite type 'TEXT', got '%s'", col.SQLiteType)
			}
		}
	}
	if !found {
		t.Error("Expected to find materialized column for $.user_agent")
	}

	// Check that device.id is included (frequency = 2 >= threshold = 2)
	found = false
	for _, col := range columns {
		if col.JSONPath == "$.device.id" {
			found = true
			if col.ColumnName != "payload_device_id" {
				t.Errorf("Expected column name 'payload_device_id', got '%s'", col.ColumnName)
			}
		}
	}
	if !found {
		t.Error("Expected to find materialized column for $.device.id")
	}
}

func TestGetMaterializedColumnsBelowThreshold(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent") // frequency = 1

	materializer := NewMaterializer(stats, 2) // threshold = 2

	columns := materializer.GetMaterializedColumns("test_collection")

	if len(columns) != 0 {
		t.Errorf("Expected 0 materialized columns (below threshold), got %d", len(columns))
	}
}

func TestGetMaterializedColumnsNilStats(t *testing.T) {
	materializer := NewMaterializer(nil, 2)

	columns := materializer.GetMaterializedColumns("test_collection")

	if len(columns) != 0 {
		t.Errorf("Expected 0 materialized columns when stats is nil, got %d", len(columns))
	}
}

func TestJsonPathToColumnName(t *testing.T) {
	tests := []struct {
		jsonPath string
		expected string
	}{
		{"$.user_agent", "payload_user_agent"},
		{"$.device.id", "payload_device_id"},
		{"$.a.b.c.d", "payload_a_b_c_d"},
		{"$.path", "payload_path"},
	}

	for _, tt := range tests {
		result := jsonPathToColumnName(tt.jsonPath)
		if result != tt.expected {
			t.Errorf("jsonPathToColumnName(%q) = %q, want %q", tt.jsonPath, result, tt.expected)
		}
	}
}

func TestInferSQLiteType(t *testing.T) {
	// Currently, all types default to TEXT
	result := inferSQLiteType("$.user_agent")
	if result != "TEXT" {
		t.Errorf("Expected TEXT, got %s", result)
	}
}