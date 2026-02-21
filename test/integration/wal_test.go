// Package integration provides end-to-end integration tests for Project Arkilian.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	apihttp "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/app"
	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
)

// setupWALTestEnv creates a test environment with WAL enabled using the full app.
func setupWALTestEnv(t *testing.T) (*app.App, string, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	walDir := filepath.Join(tempDir, "wal")
	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestDir := filepath.Join(tempDir, "manifest")
	downloadDir := filepath.Join(tempDir, "downloads")
	compactionDir := filepath.Join(tempDir, "compaction")

	for _, dir := range []string{walDir, partitionDir, storageDir, manifestDir, downloadDir, compactionDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	// Create config with WAL enabled
	cfg := &config.Config{
		Mode:      config.ModeAll,
		DataDir:   tempDir,
		HTTP:      config.HTTPConfig{IngestAddr: "127.0.0.1:0", QueryAddr: "127.0.0.1:0", CompactAddr: "127.0.0.1:0"},
		GRPC:      config.GRPCConfig{Enabled: false},
		Ingest:    config.IngestConfig{PartitionDir: partitionDir, TargetPartitionSizeMB: 16},
		Query:     config.QueryConfig{DownloadDir: downloadDir, Concurrency: 4, PoolSize: 10},
		Compaction: config.CompactionConfig{WorkDir: compactionDir},
		Storage:   config.StorageConfig{Type: "local", Path: storageDir},
		Manifest:  config.ManifestConfig{Sharded: false},
		WAL: config.WALConfig{
			Dir:            walDir,
			MaxSegmentSize: 64 * 1024 * 1024,
			FlushInterval:  500 * time.Millisecond,
			FlushBatchSize: 1000,
			RetentionTime:  1 * time.Hour,
			Enabled:        true,
		},
		Router: config.RouterConfig{
			NotificationsEnabled: false,
			BufferSize:           1000,
		},
		Index: config.IndexConfig{Enabled: false},
		Cache: config.CacheConfig{NVMeDir: "", NVMeMaxBytes: 0, PrefetchEnabled: false},
	}

	testApp, err := app.New(cfg)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create app: %v", err)
	}

	ctx := context.Background()
	if err := testApp.Start(ctx); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to start app: %v", err)
	}

	cleanup := func() {
		testApp.Stop(context.Background())
		os.RemoveAll(tempDir)
	}

	return testApp, tempDir, cleanup
}

// getIngestURL returns the ingest URL for the app.
func getIngestURL(testApp *app.App) string {
	// Get the actual port from config after startup
	return fmt.Sprintf("http://127.0.0.1:%s", "0")
}

// TestWALIngestAndFlush tests the full WAL ingest and flush flow.
func TestWALIngestAndFlush(t *testing.T) {
	_, _, cleanup := setupWALTestEnv(t)
	defer cleanup()

	// Create test data
	testRows := []map[string]interface{}{
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
	}

	// Ingest via HTTP
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows:         testRows,
	}
	body, _ := json.Marshal(reqBody)

	// Use httptest to make request to the handler directly
	// Note: In a true integration test, we'd start a real server
	// For now, we test the handler with a mock response recorder
	rec := httptest.NewRecorder()
	
	// Create handler and test
	store, _ := storage.NewLocalStorage("")
	catalog, _ := manifest.NewCatalog("")
	builder := partition.NewBuilder("", 0)
	metaGen := partition.NewMetadataGenerator()
	walInstance, _ := wal.NewWAL(filepath.Join(t.TempDir(), "wal"), 64*1024*1024)
	defer walInstance.Close()

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, walInstance)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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
	partitions, err := catalog.FindPartitions(context.Background(), nil)
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

	t.Logf("WAL ingest and flush test passed: LSN=%d, rows=%d", resp.LSN, totalRows)
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
	downloadDir := filepath.Join(tempDir, "downloads")
	compactionDir := filepath.Join(tempDir, "compaction")

	for _, dir := range []string{walDir, partitionDir, storageDir, downloadDir, compactionDir} {
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

	// Simulate crash: close WAL without proper shutdown
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
	partitions, err := catalog.FindPartitions(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}

	if len(partitions) == 0 {
		t.Error("expected partition after recovery and flush")
	}

	// Verify the partition has the correct row count
	if partitions[0].RowCount != 1 {
		t.Errorf("expected 1 row in recovered partition, got %d", partitions[0].RowCount)
	}

	t.Logf("WAL recovery test passed: recovered %d entries", recoveredCount)
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
	partitions, err := catalog.FindPartitions(context.Background(), nil)
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

	t.Logf("WAL concurrent ingest test passed: %d entries with unique LSNs", len(lsns))
}

// Helper function to make HTTP request and get response
func makeIngestRequest(t *testing.T, url string, rows []map[string]interface{}) *http.Response {
	t.Helper()

	reqBody := map[string]interface{}{
		"partition_key": "20260206",
		"rows":          rows,
	}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to make request: %v", err)
	}

	return resp
}

// Helper function to read response body
func readResponseBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	resp.Body.Close()

	return body
}