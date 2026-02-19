// Package integration provides end-to-end integration tests for Project Arkilian.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	apihttp "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// TestIngestFlow tests the end-to-end ingest flow:
// API → partition → storage → manifest
func TestIngestFlow(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	tempDir, err := os.MkdirTemp("", "arkilian-ingest-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Initialize components
	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	store, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, nil)

	// Apply middleware
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Create test request
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows: []map[string]interface{}{
			{
				"tenant_id":  "acme",
				"user_id":    float64(12345),
				"event_time": float64(time.Now().UnixNano()),
				"event_type": "page_view",
				"payload":    map[string]interface{}{"page": "/home"},
			},
			{
				"tenant_id":  "acme",
				"user_id":    float64(67890),
				"event_time": float64(time.Now().UnixNano()),
				"event_type": "click",
				"payload":    map[string]interface{}{"button": "signup"},
			},
		},
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	// Verify response
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify response fields
	if resp.PartitionID == "" {
		t.Error("expected partition_id in response")
	}
	if resp.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", resp.RowCount)
	}
	if resp.SizeBytes <= 0 {
		t.Error("expected positive size_bytes")
	}
	if resp.RequestID == "" {
		t.Error("expected request_id in response")
	}

	// Verify partition was registered in manifest
	record, err := catalog.GetPartition(ctx, resp.PartitionID)
	if err != nil {
		t.Fatalf("failed to get partition from catalog: %v", err)
	}

	if record.PartitionKey != "20260206" {
		t.Errorf("expected partition_key=20260206, got %s", record.PartitionKey)
	}
	if record.RowCount != 2 {
		t.Errorf("expected row_count=2 in catalog, got %d", record.RowCount)
	}

	// Verify files exist in storage
	sqliteExists, _ := store.Exists(ctx, record.ObjectPath)
	if !sqliteExists {
		t.Error("SQLite file not found in storage")
	}
}

// TestIngestIdempotencyKey tests idempotency key handling at the catalog level.
// Note: The current implementation checks idempotency at catalog registration time,
// so the partition is built before the check. This test verifies the catalog-level
// idempotency behavior.
func TestIngestIdempotencyKey(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	tempDir, err := os.MkdirTemp("", "arkilian-idempotency-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)

	// Build a partition
	rows := []types.Row{
		{TenantID: "acme", UserID: 12345, EventTime: time.Now().UnixNano(), EventType: "page_view", Payload: map[string]interface{}{}},
	}
	key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260206"}
	info1, err := builder.Build(ctx, rows, key)
	if err != nil {
		t.Fatalf("failed to build partition: %v", err)
	}

	// Register with idempotency key
	partitionID1, err := catalog.RegisterPartitionWithIdempotencyKey(ctx, info1, "path/to/partition1.sqlite", "batch-001")
	if err != nil {
		t.Fatalf("failed to register partition: %v", err)
	}

	// Build another partition
	info2, err := builder.Build(ctx, rows, key)
	if err != nil {
		t.Fatalf("failed to build second partition: %v", err)
	}

	// Try to register with same idempotency key - should return existing partition ID
	partitionID2, err := catalog.RegisterPartitionWithIdempotencyKey(ctx, info2, "path/to/partition2.sqlite", "batch-001")
	if err != nil {
		t.Fatalf("failed to register with same idempotency key: %v", err)
	}

	// Both should return the same partition ID
	if partitionID1 != partitionID2 {
		t.Errorf("idempotency failed: expected same partition ID, got %s vs %s", partitionID1, partitionID2)
	}

	// Verify only one partition exists in catalog
	count, err := catalog.GetPartitionCount(ctx)
	if err != nil {
		t.Fatalf("failed to count partitions: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 partition, got %d", count)
	}
}

// TestIngestValidation tests request validation.
func TestIngestValidation(t *testing.T) {
	// Setup test environment
	tempDir, err := os.MkdirTemp("", "arkilian-validation-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	tests := []struct {
		name       string
		body       interface{}
		wantStatus int
	}{
		{
			name:       "missing partition_key",
			body:       map[string]interface{}{"rows": []interface{}{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty rows",
			body:       map[string]interface{}{"partition_key": "20260206", "rows": []interface{}{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing tenant_id",
			body: map[string]interface{}{
				"partition_key": "20260206",
				"rows": []interface{}{
					map[string]interface{}{
						"user_id":    float64(123),
						"event_time": float64(time.Now().UnixNano()),
						"event_type": "test",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid json",
			body:       "not json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			if s, ok := tt.body.(string); ok {
				body = []byte(s)
			} else {
				body, _ = json.Marshal(tt.body)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			wrappedHandler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d: %s", tt.wantStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestIngestRequestID tests that request_id is present in all responses.
func TestIngestRequestID(t *testing.T) {
	// Setup test environment
	tempDir, err := os.MkdirTemp("", "arkilian-reqid-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Test with custom request ID
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows: []map[string]interface{}{
			{
				"tenant_id":  "acme",
				"user_id":    float64(12345),
				"event_time": float64(time.Now().UnixNano()),
				"event_type": "test",
				"payload":    map[string]interface{}{},
			},
		},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", "custom-request-123")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	// Verify request ID in response header
	if rec.Header().Get("X-Request-ID") != "custom-request-123" {
		t.Errorf("expected X-Request-ID header to be custom-request-123, got %s",
			rec.Header().Get("X-Request-ID"))
	}

	// Verify request ID in response body
	var resp apihttp.IngestResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.RequestID != "custom-request-123" {
		t.Errorf("expected request_id=custom-request-123, got %s", resp.RequestID)
	}
}

// TestIngestMultipleBatches tests ingesting multiple batches to the same partition key.
func TestIngestMultipleBatches(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	tempDir, err := os.MkdirTemp("", "arkilian-multi-batch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest 3 batches
	for i := 0; i < 3; i++ {
		reqBody := apihttp.IngestRequest{
			PartitionKey: "20260206",
			Rows: []map[string]interface{}{
				{
					"tenant_id":  "acme",
					"user_id":    float64(i * 1000),
					"event_time": float64(time.Now().UnixNano()),
					"event_type": "batch_event",
					"payload":    map[string]interface{}{"batch": i},
				},
			},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		wrappedHandler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("batch %d failed: %d - %s", i, rec.Code, rec.Body.String())
		}
	}

	// Verify 3 partitions exist
	count, err := catalog.GetPartitionCountByKey(ctx, "20260206")
	if err != nil {
		t.Fatalf("failed to count partitions: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 partitions, got %d", count)
	}
}
