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
	"github.com/arkilian/arkilian/internal/cache"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/observability"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/router"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
)

// featureFlagEnv holds shared test infrastructure for feature flag tests.
type featureFlagEnv struct {
	catalog      *manifest.SQLiteCatalog
	store        storage.ObjectStorage
	builder      partition.PartitionBuilder
	metaGen      *partition.MetadataGenerator
	tempDir      string
	partitionDir string
	storageDir   string
	downloadDir  string
	walDir       string
}

func setupFeatureFlagTestEnv(t *testing.T) (*featureFlagEnv, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-feature-flag-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	downloadDir := filepath.Join(tempDir, "downloads")
	walDir := filepath.Join(tempDir, "wal")

	for _, dir := range []string{partitionDir, storageDir, downloadDir, walDir} {
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

	env := &featureFlagEnv{
		catalog:      catalog,
		store:        store,
		builder:      builder,
		metaGen:      metaGen,
		tempDir:      tempDir,
		partitionDir: partitionDir,
		storageDir:   storageDir,
		downloadDir:  downloadDir,
		walDir:       walDir,
	}

	cleanup := func() {
		catalog.Close()
		os.RemoveAll(tempDir)
	}

	return env, cleanup
}

func testIngestRows() []map[string]interface{} {
	return []map[string]interface{}{
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
	}
}

// TestWALEnabled_IngestReturnsLSN verifies that WAL is always enabled in v2
// and ingest returns LSN with status "accepted".
func TestWALEnabled_IngestReturnsLSN(t *testing.T) {
	env, cleanup := setupFeatureFlagTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Create WAL
	walInstance, err := wal.NewWAL(env.walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	// Create flusher without notifier
	flusher := wal.NewFlusher(walInstance, env.builder, env.store, env.catalog, env.metaGen, 200*time.Millisecond, 1000)

	// Create ingest handler with WAL
	handler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, walInstance, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest data
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows:         testIngestRows(),
	}
	body, _ := json.Marshal(reqBody)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify IngestResponse with LSN and status "accepted"
	var resp apihttp.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.LSN == 0 {
		t.Error("expected LSN > 0")
	}
	if resp.Status != "accepted" {
		t.Errorf("expected status 'accepted', got %q", resp.Status)
	}
	if resp.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", resp.RowCount)
	}

	// Start flusher and wait for flush
	flusherCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)
	time.Sleep(1 * time.Second)

	// Verify partition was registered
	partitions, err := env.catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to get partitions: %v", err)
	}

	var found bool
	for _, p := range partitions {
		if p.PartitionKey == "20260206" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected partition to be registered after flush")
	}

	t.Logf("WAL enabled test passed: LSN=%d", resp.LSN)
}

// TestAllFeaturesEnabled tests all v2 features working together.
func TestAllFeaturesEnabled(t *testing.T) {
	env, cleanup := setupFeatureFlagTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// WAL
	walInstance, err := wal.NewWAL(env.walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	// Notifications
	notifier := router.NewNotifier(1000)
	defer notifier.Close()

	// Flusher with notifier
	flusher := wal.NewFlusher(walInstance, env.builder, env.store, env.catalog, env.metaGen, 200*time.Millisecond, 1000)
	flusher.SetNotifier(notifier)

	subscriber := notifier.Subscribe("all-features-sub", []string{""})
	defer notifier.Unsubscribe("all-features-sub")

	// NVMe cache
	nvmeDir := filepath.Join(env.tempDir, "nvme")
	os.MkdirAll(nvmeDir, 0755)
	nvmeCache, err := cache.NewNVMeCache(nvmeDir, 100*1024*1024)
	if err != nil {
		t.Fatalf("failed to create NVMe cache: %v", err)
	}
	defer nvmeCache.Close()

	// Co-access graph
	coAccessGraph := cache.NewCoAccessGraph(0.95, 0.70, 10)

	// Query stats
	queryStats := observability.NewQueryStats(1 * time.Hour)

	// Ingest handler with WAL
	ingestHandler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, walInstance, nil)
	wrappedIngestHandler := apihttp.DefaultMiddleware()(ingestHandler)

	// Start flusher
	flusherCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Ingest via WAL
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows:         testIngestRows(),
	}
	body, _ := json.Marshal(reqBody)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	wrappedIngestHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
	}

	var walResp apihttp.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &walResp); err != nil {
		t.Fatalf("failed to unmarshal WAL response: %v", err)
	}
	if walResp.LSN == 0 {
		t.Error("expected LSN > 0")
	}

	// Wait for notification
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case notif := <-subscriber.Ch:
		if notif.Type != router.PartitionCreated {
			t.Errorf("expected PartitionCreated notification, got %d", notif.Type)
		}
		t.Logf("All-enabled: notification received for LSN=%d", notif.LSN)
	case <-timer.C:
		t.Error("expected notification within 500ms")
	}

	// Create planner with notifier for write visibility
	queryPlanner := planner.NewPlannerWithNotifier(env.catalog, notifier)
	exec, err := executor.NewParallelExecutor(queryPlanner, env.store, executor.ExecutorConfig{
		Concurrency: 4,
		DownloadDir: env.downloadDir,
	}, nil, coAccessGraph)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}
	defer exec.Close()

	queryHandler := apihttp.NewQueryHandler(exec, queryStats)
	wrappedQueryHandler := apihttp.DefaultMiddleware()(queryHandler)

	// Query
	queryReqBody := apihttp.QueryRequest{SQL: "SELECT tenant_id, user_id FROM events"}
	queryBody, _ := json.Marshal(queryReqBody)

	qRec := httptest.NewRecorder()
	qReq := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(queryBody))
	qReq.Header.Set("Content-Type", "application/json")
	wrappedQueryHandler.ServeHTTP(qRec, qReq)

	if qRec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", qRec.Code, qRec.Body.String())
	}

	var queryResp apihttp.QueryResponse
	if err := json.Unmarshal(qRec.Body.Bytes(), &queryResp); err != nil {
		t.Fatalf("failed to unmarshal query response: %v", err)
	}
	if len(queryResp.Rows) == 0 {
		t.Error("expected rows from query with all features enabled")
	}

	// Verify NVMe cache and co-access graph didn't cause errors
	_, cacheFound := nvmeCache.Get("nonexistent")
	if cacheFound {
		t.Error("expected cache miss for nonexistent key")
	}

	t.Logf("All-enabled: ingested LSN=%d, queried %d rows, NVMe+CoAccess wired without errors",
		walResp.LSN, len(queryResp.Rows))
}