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
	"github.com/arkilian/arkilian/internal/wal"
)

// TestIngestFlow tests the end-to-end ingest flow with WAL:
// API → WAL → flusher → partition → storage → manifest
func TestIngestFlow(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	tempDir, err := os.MkdirTemp("", "arkilian-ingest-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	walDir := filepath.Join(tempDir, "wal")

	for _, dir := range []string{partitionDir, storageDir, walDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

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

	// Create WAL
	walInstance, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	// Create flusher
	flusher := wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 100*time.Millisecond, 1000)

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, walInstance, nil)

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
				"payload":    map[string]interface{}{"button": "submit"},
			},
		},
	}
	body, _ := json.Marshal(reqBody)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify response fields (v2: LSN and status)
	if resp.LSN == 0 {
		t.Error("expected LSN > 0 in response")
	}
	if resp.Status != "accepted" {
		t.Errorf("expected status='accepted', got %s", resp.Status)
	}
	if resp.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", resp.RowCount)
	}
	if resp.RequestID == "" {
		t.Error("expected request_id in response")
	}

	// Start flusher and wait for flush
	flusherCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Wait for partition to be registered
	time.Sleep(2 * time.Second)

	// Find the partition in manifest by partition key
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to get partitions from catalog: %v", err)
	}

	var record *manifest.PartitionRecord
	for _, p := range partitions {
		if p.PartitionKey == "20260206" {
			record = p
			break
		}
	}
	if record == nil {
		t.Fatal("expected partition with key 20260206 to be registered after flush")
	}

	if record.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", record.RowCount)
	}

	t.Logf("Ingest flow test passed: LSN=%d, partition_id=%s", resp.LSN, record.PartitionID)
}

// TestIngestMultipleBatches tests ingesting multiple batches sequentially.
func TestIngestMultipleBatches(t *testing.T) {
	ctx := context.Background()

	tempDir, err := os.MkdirTemp("", "arkilian-batch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	walDir := filepath.Join(tempDir, "wal")

	for _, dir := range []string{partitionDir, storageDir, walDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	walInstance, _ := wal.NewWAL(walDir, 64*1024*1024)
	defer walInstance.Close()

	flusher := wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 100*time.Millisecond, 1000)

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, walInstance, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest 3 batches
	for i := 0; i < 3; i++ {
		reqBody := apihttp.IngestRequest{
			PartitionKey: "20260206",
			Rows: []map[string]interface{}{
				{
					"tenant_id":  "acme",
					"user_id":    float64(1000 + i),
					"event_time": float64(time.Now().UnixNano()),
					"event_type": "page_view",
					"payload":    map[string]interface{}{"page": "/home"},
				},
			},
		}
		body, _ := json.Marshal(reqBody)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		wrappedHandler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("batch %d failed: %d - %s", i, rec.Code, rec.Body.String())
		}

		var resp apihttp.IngestResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)

		if resp.LSN == 0 {
			t.Fatalf("batch %d: expected LSN > 0, got 0", i)
		}
	}

	// Wait for flush
	flusherCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)
	time.Sleep(2 * time.Second)

	// Verify all partitions registered
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to get partitions: %v", err)
	}

	// Should have at least 1 partition for 20260206
	var found bool
	for _, p := range partitions {
		if p.PartitionKey == "20260206" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected partition for 20260206 to be registered")
	}

	t.Logf("Multiple batches test passed: %d partitions found", len(partitions))
}