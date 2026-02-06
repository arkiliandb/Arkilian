package partition

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
	"github.com/golang/snappy"
	_ "github.com/mattn/go-sqlite3"
)

func TestBuilder_Build(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "arkilian-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	builder := NewBuilder(tmpDir)

	// Create test rows
	rows := []types.Row{
		{
			TenantID:  "acme",
			UserID:    12345,
			EventTime: time.Now().UnixNano(),
			EventType: "page_view",
			Payload:   map[string]interface{}{"page": "/home"},
		},
		{
			TenantID:  "acme",
			UserID:    67890,
			EventTime: time.Now().UnixNano(),
			EventType: "purchase",
			Payload:   map[string]interface{}{"amount": 99.99},
		},
		{
			TenantID:  "beta",
			UserID:    11111,
			EventTime: time.Now().UnixNano(),
			EventType: "signup",
			Payload:   map[string]interface{}{"source": "organic"},
		},
	}

	key := types.PartitionKey{
		Strategy: types.StrategyTime,
		Value:    "20260205",
	}

	// Build partition
	info, err := builder.Build(context.Background(), rows, key)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify partition info
	if info.RowCount != 3 {
		t.Errorf("expected RowCount=3, got %d", info.RowCount)
	}
	if info.PartitionKey != "20260205" {
		t.Errorf("expected PartitionKey=20260205, got %s", info.PartitionKey)
	}
	if info.SizeBytes == 0 {
		t.Error("expected SizeBytes > 0")
	}

	// Verify SQLite file exists
	if _, err := os.Stat(info.SQLitePath); os.IsNotExist(err) {
		t.Errorf("SQLite file does not exist: %s", info.SQLitePath)
	}

	// Verify min/max stats
	if stat, ok := info.MinMaxStats["user_id"]; ok {
		if stat.Min.(int64) != 11111 {
			t.Errorf("expected min user_id=11111, got %v", stat.Min)
		}
		if stat.Max.(int64) != 67890 {
			t.Errorf("expected max user_id=67890, got %v", stat.Max)
		}
	} else {
		t.Error("missing user_id stats")
	}

	if stat, ok := info.MinMaxStats["tenant_id"]; ok {
		if stat.Min.(string) != "acme" {
			t.Errorf("expected min tenant_id=acme, got %v", stat.Min)
		}
		if stat.Max.(string) != "beta" {
			t.Errorf("expected max tenant_id=beta, got %v", stat.Max)
		}
	} else {
		t.Error("missing tenant_id stats")
	}

	// Verify SQLite content
	db, err := sql.Open("sqlite3", info.SQLitePath)
	if err != nil {
		t.Fatalf("failed to open SQLite: %v", err)
	}
	defer db.Close()

	// Check row count
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows in SQLite, got %d", count)
	}

	// Verify Snappy compression
	var payload []byte
	if err := db.QueryRow("SELECT payload FROM events LIMIT 1").Scan(&payload); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}
	decompressed, err := snappy.Decode(nil, payload)
	if err != nil {
		t.Fatalf("failed to decompress payload: %v", err)
	}
	var payloadMap map[string]interface{}
	if err := json.Unmarshal(decompressed, &payloadMap); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}
}

func TestBuilder_EmptyRows(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arkilian-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	builder := NewBuilder(tmpDir)
	key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260205"}

	_, err = builder.Build(context.Background(), []types.Row{}, key)
	if err == nil {
		t.Error("expected error for empty rows")
	}
}

func TestBuilder_ValidationError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arkilian-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	builder := NewBuilder(tmpDir)
	key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260205"}

	// Row with missing required fields
	rows := []types.Row{
		{
			TenantID:  "", // Empty - should fail validation
			UserID:    12345,
			EventTime: time.Now().UnixNano(),
			EventType: "test",
			Payload:   map[string]interface{}{},
		},
	}

	_, err = builder.Build(context.Background(), rows, key)
	if err == nil {
		t.Error("expected validation error for empty tenant_id")
	}
}

func TestMetadataGenerator_Generate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arkilian-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	builder := NewBuilder(tmpDir)
	metaGen := NewMetadataGenerator()

	rows := []types.Row{
		{
			TenantID:  "acme",
			UserID:    100,
			EventTime: 1000000000,
			EventType: "test",
			Payload:   map[string]interface{}{"key": "value"},
		},
		{
			TenantID:  "beta",
			UserID:    200,
			EventTime: 2000000000,
			EventType: "test",
			Payload:   map[string]interface{}{"key": "value2"},
		},
	}

	key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260205"}
	info, err := builder.Build(context.Background(), rows, key)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Generate metadata
	sidecar, err := metaGen.Generate(info, rows)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Verify sidecar fields
	if sidecar.PartitionID != info.PartitionID {
		t.Errorf("partition_id mismatch")
	}
	if sidecar.Stats.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", sidecar.Stats.RowCount)
	}
	if sidecar.Stats.MinUserID == nil || *sidecar.Stats.MinUserID != 100 {
		t.Errorf("expected min_user_id=100")
	}
	if sidecar.Stats.MaxUserID == nil || *sidecar.Stats.MaxUserID != 200 {
		t.Errorf("expected max_user_id=200")
	}

	// Verify bloom filters exist
	if _, ok := sidecar.BloomFilters["tenant_id"]; !ok {
		t.Error("missing tenant_id bloom filter")
	}
	if _, ok := sidecar.BloomFilters["user_id"]; !ok {
		t.Error("missing user_id bloom filter")
	}

	// Write and read back
	metaPath := filepath.Join(tmpDir, "test.meta.json")
	if err := sidecar.WriteToFile(metaPath); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	readBack, err := ReadMetadataFromFile(metaPath)
	if err != nil {
		t.Fatalf("ReadMetadataFromFile failed: %v", err)
	}

	if readBack.PartitionID != sidecar.PartitionID {
		t.Error("round-trip partition_id mismatch")
	}
	if readBack.Stats.RowCount != sidecar.Stats.RowCount {
		t.Error("round-trip row_count mismatch")
	}
}
