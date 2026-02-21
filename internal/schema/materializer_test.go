package schema

import (
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/observability"
)

func TestGetMaterializedColumns(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")
	stats.RecordJSONPath("$.user_agent")
	stats.RecordJSONPath("$.user_agent")
	stats.RecordJSONPath("$.device.id")
	stats.RecordJSONPath("$.device.id")

	materializer := NewMaterializer(stats, 2, 20) // threshold = 2, maxColumns = 20

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

	materializer := NewMaterializer(stats, 2, 20) // threshold = 2

	columns := materializer.GetMaterializedColumns("test_collection")

	if len(columns) != 0 {
		t.Errorf("Expected 0 materialized columns (below threshold), got %d", len(columns))
	}
}

func TestGetMaterializedColumnsNilStats(t *testing.T) {
	materializer := NewMaterializer(nil, 2, 20)

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
	tests := []struct {
		jsonPath string
		expected string
	}{
		// Basic TEXT cases
		{"$.user_agent", "TEXT"},
		{"$.name", "TEXT"},
		{"$.email", "TEXT"},
		{"$.timestamp", "TEXT"},
		{"$.latency_ms", "TEXT"},
		{"$.duration_sec", "TEXT"},
		{"$.location_latitude", "TEXT"},
		{"$.location_longitude", "TEXT"},
		
		// INTEGER cases (ending with _id, _count, _at, etc.)
		{"$.device_id", "INTEGER"},
		{"$.device_count", "INTEGER"},
		{"$.created_at", "INTEGER"},
		{"$.device_timestamp", "INTEGER"},
		
		// REAL cases (ending with _lat, _lon, _price, _latency, _duration, etc.)
		{"$.location_lat", "REAL"},
		{"$.location_lon", "REAL"},
		{"$.product_price", "REAL"},
		{"$.request_latency", "REAL"},
		{"$.processing_duration", "REAL"},
	}

	for _, tt := range tests {
		result := inferSQLiteType(tt.jsonPath)
		if result != tt.expected {
			t.Errorf("inferSQLiteType(%q) = %q, want %q", tt.jsonPath, result, tt.expected)
		}
	}
}

func TestMaterializerMetrics(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")

	materializer := NewMaterializer(stats, 1, 20)
	defer materializer.Close()

	// First call
	materializer.GetMaterializedColumns("test")
	metrics := materializer.Metrics()
	if metrics.Calls != 1 {
		t.Errorf("Expected 1 call, got %d", metrics.Calls)
	}
	if metrics.ColumnsReturned != 1 {
		t.Errorf("Expected 1 column returned, got %d", metrics.ColumnsReturned)
	}

	// Second call (should hit cache)
	materializer.GetMaterializedColumns("test")
	metrics = materializer.Metrics()
	if metrics.Calls != 2 {
		t.Errorf("Expected 2 calls, got %d", metrics.Calls)
	}
	if metrics.CacheHits != 1 {
		t.Errorf("Expected 1 cache hit, got %d", metrics.CacheHits)
	}
}

func TestMaterializerCaching(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")

	materializer := NewMaterializer(stats, 1, 20)
	defer materializer.Close()

	// First call
	columns1 := materializer.GetMaterializedColumns("test")

	// Add more paths (should not affect cached result)
	stats.RecordJSONPath("$.device_id")
	stats.RecordJSONPath("$.device_id")

	// Second call (should return cached result without new columns)
	columns2 := materializer.GetMaterializedColumns("test")

	if len(columns1) != len(columns2) {
		t.Errorf("Cached result should not change, got %d vs %d", len(columns1), len(columns2))
	}
}

func TestMaterializerInvalidateCache(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")

	materializer := NewMaterializer(stats, 1, 20)
	defer materializer.Close()

	// First call
	materializer.GetMaterializedColumns("test")

	// Add more paths
	stats.RecordJSONPath("$.device_id")
	stats.RecordJSONPath("$.device_id")

	// Invalidate cache
	materializer.InvalidateCache()

	// Now should get new columns
	columns := materializer.GetMaterializedColumns("test")
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns after cache invalidation, got %d", len(columns))
	}
}

func TestMaterializerMaxColumns(t *testing.T) {
	stats := observability.NewQueryStats(1)

	// Add 25 paths above threshold
	for i := 0; i < 25; i++ {
		for j := 0; j < 5; j++ { // frequency = 5
			stats.RecordJSONPath("$.path" + string(rune('a'+i)))
		}
	}

	// Max columns = 10
	materializer := NewMaterializer(stats, 2, 10)
	defer materializer.Close()

	columns := materializer.GetMaterializedColumns("test")
	if len(columns) > 10 {
		t.Errorf("Expected at most 10 columns, got %d", len(columns))
	}
}

func TestMaterializerSetThreshold(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent") // frequency = 1

	materializer := NewMaterializer(stats, 10, 20) // threshold = 10
	defer materializer.Close()

	// Should return 0 (below threshold)
	columns := materializer.GetMaterializedColumns("test")
	if len(columns) != 0 {
		t.Errorf("Expected 0 columns below threshold, got %d", len(columns))
	}

	// Lower threshold
	materializer.SetThreshold(1)
	columns = materializer.GetMaterializedColumns("test")
	if len(columns) != 1 {
		t.Errorf("Expected 1 column after lowering threshold, got %d", len(columns))
	}
}

func TestMaterializerThresholdAccessor(t *testing.T) {
	stats := observability.NewQueryStats(1)
	materializer := NewMaterializer(stats, 50, 20)
	defer materializer.Close()

	if materializer.Threshold() != 50 {
		t.Errorf("Expected threshold 50, got %d", materializer.Threshold())
	}
}

func TestMaterializerClose(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")

	materializer := NewMaterializer(stats, 1, 20)

	// Close should not panic
	materializer.Close()

	// After close, should still work
	columns := materializer.GetMaterializedColumns("test")
	if len(columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(columns))
	}
}

func TestMaterializerDefaultThreshold(t *testing.T) {
	// Zero threshold should use default
	stats := observability.NewQueryStats(1)
	materializer := NewMaterializer(stats, 0, 20)
	defer materializer.Close()

	if materializer.Threshold() != 50 {
		t.Errorf("Expected default threshold 50, got %d", materializer.Threshold())
	}
}

func TestMaterializerDefaultMaxColumns(t *testing.T) {
	stats := observability.NewQueryStats(1)
	materializer := NewMaterializer(stats, 1, 0)
	defer materializer.Close()

	// Should use default maxColumns
	stats.RecordJSONPath("$.user_agent")
	columns := materializer.GetMaterializedColumns("test")
	if len(columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(columns))
	}
}

func TestValidateColumnName(t *testing.T) {
	valid := []string{
		"payload_user_agent",
		"payload_device_id",
		"_test",
		"abc123",
		"payload_a_b_c_d",
	}

	invalid := []string{
		"",
		"123abc",        // starts with number
		"payload.user_agent", // contains dot
		"payload-user_agent", // contains hyphen
		"payload user agent", // contains space
	}

	for _, name := range valid {
		if !ValidateColumnName(name) {
			t.Errorf("Expected %q to be valid", name)
		}
	}

	for _, name := range invalid {
		if ValidateColumnName(name) {
			t.Errorf("Expected %q to be invalid", name)
		}
	}
}

func TestCacheExpiry(t *testing.T) {
	stats := observability.NewQueryStats(1)
	stats.RecordJSONPath("$.user_agent")

	materializer := NewMaterializer(stats, 1, 20)
	defer materializer.Close()

	// First call
	materializer.GetMaterializedColumns("test")

	// Add more paths
	stats.RecordJSONPath("$.device_id")
	stats.RecordJSONPath("$.device_id")

	// Manually expire cache
	materializer.mu.Lock()
	materializer.cacheUntil = time.Now().Add(-1 * time.Second)
	materializer.mu.Unlock()

	// Should get new columns after cache expires
	columns := materializer.GetMaterializedColumns("test")
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns after cache expiry, got %d", len(columns))
	}
}