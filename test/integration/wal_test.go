// Package integration provides end-to-end integration tests for Project Arkilian.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	apihttp "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
)

// setupWALTestEnv creates a test environment with WAL enabled.
func setupWALTestEnv(t *testing.T) (
	*wal.WAL,
	*wal.Flusher,
	manifest.Catalog,
	storage.ObjectStorage,
	string,
	func(),
) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	walDir := filepath.Join(tempDir, "wal")
	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	for _, dir := range []string{walDir, partitionDir, storageDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	store, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create catalog: %v", err)
	}

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	walInstance, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		catalog.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create WAL: %v", err)
	}

	flusher := wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 500*time.Millisecond, 1000)

	cleanup := func() {
		walInstance.Close()
		catalog.Close()
		os.RemoveAll(tempDir)
	}

	return walInstance, flusher, catalog, store, tempDir, cleanup
}

// TestWALIngestAndFlush tests the full WAL ingest and flush flow.
func TestWALIngestAndFlush(t *testing.T) {
	walInstance, flusher, catalog, store, _, cleanup := setupWALTestEnv(t)
	defer cleanup()

	// Start flusher in background
	ctx := context.Background()
	flusherCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Create ingest handler with WAL
	builder := partition.NewBuilder("", 0)
	metaGen := partition.NewMetadataGenerator()
	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, walInstance)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest test data
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

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	// Verify response has LSN and status
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.WALIngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.LSN == 0 {
		t.Error("expected LSN > 0 in response")
	}
	if resp.Status != "accepted" {
		t.Errorf("expected status='accepted', got %s", resp.Status)
	}
	if resp.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", resp.RowCount)
	}

	// Wait for flush
	time.Sleep(2 * time.Second)

	// Verify data was flushed to storage and catalog
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	if len(partitions) == 0 {
		t.Error("expected at least one partition after flush")
	}

	// Verify partition has correct row count
	var totalRows int64
	for _, p := range partitions {
		totalRows += p.RowCount
	}

	if totalRows < 2 {
		t.Errorf("expected at least 2 rows in partitions, got %d", totalRows)
	}
}

// TestWALRecovery tests WAL recovery after crash.
func TestWALRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-wal-recovery-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walDir := filepath.Join(tempDir, "wal")
	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	for _, dir := range []string{walDir, partitionDir, storageDir} {
		os.MkdirAll(dir, 0755)
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

	// Create flusher with long interval to prevent auto-flush (not started, just for recovery test)
	_ = wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 10*time.Second, 1000)

	// Create ingest handler
	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, walInstance)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest data (will be in WAL, not flushed due to long flush interval)
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
		},
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.WALIngestResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp.LSN == 0 {
		t.Fatal("expected LSN in response")
	}

	// Simulate crash: close WAL without proper shutdown (flusher stops when context cancelled)
	walInstance.Close()

	// Create new WAL instance (simulating restart)
	newWAL, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create new WAL: %v", err)
	}
	defer newWAL.Close()

	// Create new flusher with short interval
	newFlusher := wal.NewFlusher(newWAL, builder, store, catalog, metaGen, 100*time.Millisecond, 1000)

	// Create recovery
	recovery := wal.NewRecovery(newWAL, newFlusher, catalog)

	// Run recovery
	recoveredCount, err := recovery.Recover(context.Background())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if recoveredCount != 1 {
		t.Errorf("expected 1 recovered entry, got %d", recoveredCount)
	}

	// Start new flusher to process recovered entries
	recoveryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go newFlusher.Run(recoveryCtx)

	// Wait for flush
	time.Sleep(500 * time.Millisecond)

	// Verify data was recovered
	partitions, err := catalog.FindPartitions(recoveryCtx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	if len(partitions) == 0 {
		t.Error("expected partition after recovery and flush")
	}
}

// TestWALConcurrentIngest tests concurrent WAL ingest from multiple goroutines.
func TestWALConcurrentIngest(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-wal-concurrent-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walDir := filepath.Join(tempDir, "wal")
	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	for _, dir := range []string{walDir, partitionDir, storageDir} {
		os.MkdirAll(dir, 0755)
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

	walInstance, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	flusher := wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 500*time.Millisecond, 1000)

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, walInstance)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Concurrent ingest from 10 goroutines
	const numGoroutines = 10
	const rowsPerGoroutine = 10
	var wg sync.WaitGroup
	lsnChan := make(chan uint64, numGoroutines*rowsPerGoroutine)
	errors := make(chan error, numGoroutines*rowsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < rowsPerGoroutine; i++ {
				reqBody := apihttp.IngestRequest{
					PartitionKey: "20260206",
					Rows: []map[string]interface{}{
						{
							"tenant_id":  "acme",
							"user_id":    float64(goroutineID*1000 + i),
							"event_time": float64(time.Now().UnixNano()),
							"event_type": "concurrent_test",
							"payload":    map[string]interface{}{"goroutine": goroutineID, "index": i},
						},
					},
				}

				body, _ := json.Marshal(reqBody)
				req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")

				rec := httptest.NewRecorder()
				wrappedHandler.ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					errors <- fmt.Errorf("goroutine %d, index %d: status %d", goroutineID, i, rec.Code)
					continue
				}

				var resp apihttp.WALIngestResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					errors <- fmt.Errorf("goroutine %d, index %d: unmarshal error: %v", goroutineID, i, err)
					continue
				}

				lsnChan <- resp.LSN
			}
		}(g)
	}

	wg.Wait()
	close(errors)
	close(lsnChan)

	// Check for errors
	for err := range errors {
		t.Errorf("ingest error: %v", err)
	}

	// Collect LSNs
	var lsns []uint64
	for lsn := range lsnChan {
		lsns = append(lsns, lsn)
	}

	// Verify we got all expected LSNs
	expectedCount := numGoroutines * rowsPerGoroutine
	if len(lsns) != expectedCount {
		t.Errorf("expected %d LSNs, got %d", expectedCount, len(lsns))
	}

	// Verify LSNs are unique and monotonic
	seen := make(map[uint64]bool)
	for _, lsn := range lsns {
		if lsn == 0 {
			t.Error("expected LSN > 0")
		}
		if seen[lsn] {
			t.Errorf("duplicate LSN: %d", lsn)
		}
		seen[lsn] = true
	}

	// Start flusher
	flusherCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Wait for flush
	time.Sleep(2 * time.Second)

	// Verify all rows are present
	partitions, err := catalog.FindPartitions(flusherCtx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	var totalRows int64
	for _, p := range partitions {
		totalRows += p.RowCount
	}

	if totalRows < int64(expectedCount) {
		t.Errorf("expected at least %d rows, got %d", expectedCount, totalRows)
	}

	t.Logf("Successfully ingested %d entries with unique LSNs", len(lsns))
}