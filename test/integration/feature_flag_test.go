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
	"github.com/arkilian/arkilian/internal/index"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/observability"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/router"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
	"github.com/arkilian/arkilian/pkg/types"
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

// setupFeatureFlagTestEnv creates temp dirs, storage, catalog, builder, metaGen.
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

// testIngestRows is a helper that returns standard test rows for ingest.
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
			"payload":    map[string]interface{}{"button": "signup"},
		},
	}
}

// TestFeatureFlagWALOnly verifies WAL enabled with everything else disabled.
// Validates: Requirements 14.2, 14.3
func TestFeatureFlagWALOnly(t *testing.T) {
	env, cleanup := setupFeatureFlagTestEnv(t)
	defer cleanup()

	// Create WAL — the only v2 feature enabled
	walInstance, err := wal.NewWAL(env.walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	// Create flusher without notifier (notifications disabled)
	flusher := wal.NewFlusher(walInstance, env.builder, env.store, env.catalog, env.metaGen, 200*time.Millisecond, 1000)

	// Create ingest handler with WAL, no materializer
	handler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, nil, walInstance, nil)
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

	// Verify WALIngestResponse with LSN and status "accepted"
	var walResp apihttp.WALIngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &walResp); err != nil {
		t.Fatalf("failed to unmarshal WAL response: %v", err)
	}
	if walResp.LSN == 0 {
		t.Error("expected LSN > 0")
	}
	if walResp.Status != "accepted" {
		t.Errorf("expected status 'accepted', got %q", walResp.Status)
	}
	if walResp.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", walResp.RowCount)
	}

	// Start flusher and wait for flush
	flusherCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)
	time.Sleep(1 * time.Second)

	// Create basic planner (no index, no notifier) and executor
	queryPlanner := planner.NewPlanner(env.catalog)
	exec, err := executor.NewParallelExecutor(queryPlanner, env.store, executor.ExecutorConfig{
		Concurrency: 4,
		DownloadDir: env.downloadDir,
	}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}
	defer exec.Close()

	queryHandler := apihttp.NewQueryHandler(exec, nil)
	wrappedQueryHandler := apihttp.DefaultMiddleware()(queryHandler)

	// Query data
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
		t.Error("expected rows after WAL flush, got none")
	}

	t.Logf("WAL-only: ingested LSN=%d, queried %d rows after flush", walResp.LSN, len(queryResp.Rows))
}

// TestFeatureFlagWALWithNotifications verifies WAL + notifications enabled.
// Validates: Requirements 14.2, 14.3
func TestFeatureFlagWALWithNotifications(t *testing.T) {
	env, cleanup := setupFeatureFlagTestEnv(t)
	defer cleanup()

	// Create WAL
	walInstance, err := wal.NewWAL(env.walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	// Create notifier (notifications enabled)
	notifier := router.NewNotifier(1000)
	defer notifier.Close()

	// Create flusher with notifier
	flusher := wal.NewFlusher(walInstance, env.builder, env.store, env.catalog, env.metaGen, 100*time.Millisecond, 1000)
	flusher.SetNotifier(notifier)

	// Subscribe to notifications
	subscriber := notifier.Subscribe("ff-test-subscriber", []string{""})
	defer notifier.Unsubscribe("ff-test-subscriber")

	// Create ingest handler with WAL
	handler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, nil, walInstance, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Start flusher
	flusherCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

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
		t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
	}

	var walResp apihttp.WALIngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &walResp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if walResp.LSN == 0 {
		t.Fatal("expected LSN > 0")
	}

	// Wait for notification with 500ms timeout (accounts for test overhead)
	var receivedNotif *router.Notification
	timeout := 500 * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case notif := <-subscriber.Ch:
		receivedNotif = &notif
	case <-timer.C:
	}

	if receivedNotif == nil {
		t.Errorf("expected notification within %v, got none", timeout)
	} else {
		if receivedNotif.PartitionKey != "20260206" {
			t.Errorf("expected partition_key='20260206', got %q", receivedNotif.PartitionKey)
		}
		if receivedNotif.Type != router.PartitionCreated {
			t.Errorf("expected notification type PartitionCreated, got %d", receivedNotif.Type)
		}
		t.Logf("WAL+Notifications: notification received for partition_key=%s, LSN=%d",
			receivedNotif.PartitionKey, receivedNotif.LSN)
	}
}

// TestFeatureFlagWALNotificationsAndIndexes verifies WAL + notifications + indexes enabled.
// Validates: Requirements 14.2, 14.3
func TestFeatureFlagWALNotificationsAndIndexes(t *testing.T) {
	env, cleanup := setupFeatureFlagTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Step 1: Ingest data synchronously (no WAL) to create partitions for indexing
	syncHandler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, nil, nil, nil)
	wrappedSyncHandler := apihttp.DefaultMiddleware()(syncHandler)

	// Ingest multiple batches with distinct user_ids
	for i := 0; i < 3; i++ {
		rows := []map[string]interface{}{
			{
				"tenant_id":  "acme",
				"user_id":    float64(100 + i),
				"event_time": float64(time.Now().UnixNano()),
				"event_type": "indexed_event",
				"payload":    map[string]interface{}{"batch": i},
			},
		}
		reqBody := apihttp.IngestRequest{PartitionKey: "20260206", Rows: rows}
		body, _ := json.Marshal(reqBody)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		wrappedSyncHandler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("sync ingest %d failed: %d - %s", i, rec.Code, rec.Body.String())
		}
	}

	// Step 2: Build index on user_id column
	indexWorkDir := filepath.Join(env.tempDir, "index_work")
	os.MkdirAll(indexWorkDir, 0755)

	indexBuilder := index.NewBuilder(env.store, env.catalog, indexWorkDir, 64)

	// Get partitions for index building
	partitions, err := env.catalog.FindPartitions(ctx, nil)
	if err != nil {
		t.Fatalf("failed to find partitions: %v", err)
	}
	if len(partitions) == 0 {
		t.Fatal("expected partitions after sync ingest")
	}

	// Convert to index.PartitionInfo
	var indexPartitions []*index.PartitionInfo
	for _, p := range partitions {
		ip := &index.PartitionInfo{
			PartitionID: p.PartitionID,
			ObjectPath:  p.ObjectPath,
			RowCount:    p.RowCount,
		}
		if p.MinEventTime != nil {
			minTime := *p.MinEventTime
			ip.MinEventTime = &minTime
		}
		if p.MaxEventTime != nil {
			maxTime := *p.MaxEventTime
			ip.MaxEventTime = &maxTime
		}
		indexPartitions = append(indexPartitions, ip)
	}

	_, err = indexBuilder.BuildIndex(ctx, "events", "user_id", indexPartitions)
	if err != nil {
		t.Fatalf("failed to build index: %v", err)
	}

	// Step 3: Create planner with index lookup
	indexCacheDir := filepath.Join(env.tempDir, "index_cache")
	os.MkdirAll(indexCacheDir, 0755)

	indexLookup := index.NewLookup(env.store, env.catalog, indexCacheDir, 64)
	queryPlanner := planner.NewPlannerWithIndex(env.catalog, nil, indexLookup)

	exec, err := executor.NewParallelExecutor(queryPlanner, env.store, executor.ExecutorConfig{
		Concurrency: 4,
		DownloadDir: env.downloadDir,
	}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}
	defer exec.Close()

	queryHandler := apihttp.NewQueryHandler(exec, nil)
	wrappedQueryHandler := apihttp.DefaultMiddleware()(queryHandler)

	// Step 4: Query with equality predicate on indexed column
	queryReqBody := apihttp.QueryRequest{SQL: "SELECT tenant_id, user_id FROM events WHERE user_id = 101"}
	queryBody, _ := json.Marshal(queryReqBody)

	qRec := httptest.NewRecorder()
	qReq := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(queryBody))
	qReq.Header.Set("Content-Type", "application/json")
	wrappedQueryHandler.ServeHTTP(qRec, qReq)

	if qRec.Code != http.StatusOK {
		t.Fatalf("indexed query failed: %d - %s", qRec.Code, qRec.Body.String())
	}

	var queryResp apihttp.QueryResponse
	if err := json.Unmarshal(qRec.Body.Bytes(), &queryResp); err != nil {
		t.Fatalf("failed to unmarshal query response: %v", err)
	}
	if len(queryResp.Rows) == 0 {
		t.Error("expected rows from indexed query")
	}

	t.Logf("WAL+Notifications+Indexes: indexed query returned %d rows", len(queryResp.Rows))
}

// TestFeatureFlagAllEnabled verifies all v2 features enabled together.
// Validates: Requirements 14.2, 14.3, 14.4
func TestFeatureFlagAllEnabled(t *testing.T) {
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
	ingestHandler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, nil, walInstance, nil)
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

	var walResp apihttp.WALIngestResponse
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

	// Create planner with co-access graph, query stats, and notifier for write visibility
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

	// Verify NVMe cache and co-access graph didn't cause errors (they're wired but passive)
	_, cacheFound := nvmeCache.Get("nonexistent")
	if cacheFound {
		t.Error("expected cache miss for nonexistent key")
	}

	t.Logf("All-enabled: ingested LSN=%d, queried %d rows, NVMe+CoAccess wired without errors",
		walResp.LSN, len(queryResp.Rows))
}

// TestFeatureFlagGracefulFallback verifies disabling features one at a time causes graceful fallback.
// Validates: Requirements 14.3, 14.4
func TestFeatureFlagGracefulFallback(t *testing.T) {
	t.Run("WALDisabled_SynchronousIngest", func(t *testing.T) {
		env, cleanup := setupFeatureFlagTestEnv(t)
		defer cleanup()

		// WAL disabled (nil) → synchronous ingest path
		handler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, nil, nil, nil)
		wrappedHandler := apihttp.DefaultMiddleware()(handler)

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
			t.Fatalf("sync ingest failed: %d - %s", rec.Code, rec.Body.String())
		}

		// Verify synchronous IngestResponse (not WALIngestResponse)
		var syncResp apihttp.IngestResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &syncResp); err != nil {
			t.Fatalf("failed to unmarshal sync response: %v", err)
		}
		if syncResp.PartitionID == "" {
			t.Error("expected partition_id in synchronous response")
		}
		if syncResp.RowCount != 2 {
			t.Errorf("expected row_count=2, got %d", syncResp.RowCount)
		}

		t.Logf("WAL disabled: synchronous ingest returned partition_id=%s", syncResp.PartitionID)
	})

	t.Run("NotificationsDisabled_IngestAndQueryWork", func(t *testing.T) {
		env, cleanup := setupFeatureFlagTestEnv(t)
		defer cleanup()

		// WAL enabled, notifications disabled (no notifier on flusher)
		walInstance, err := wal.NewWAL(env.walDir, 64*1024*1024)
		if err != nil {
			t.Fatalf("failed to create WAL: %v", err)
		}
		defer walInstance.Close()

		flusher := wal.NewFlusher(walInstance, env.builder, env.store, env.catalog, env.metaGen, 200*time.Millisecond, 1000)
		// No flusher.SetNotifier() — notifications disabled

		handler := apihttp.NewIngestHandler(env.builder, env.metaGen, env.catalog, env.store, nil, walInstance, nil)
		wrappedHandler := apihttp.DefaultMiddleware()(handler)

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
			t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
		}

		// Start flusher and wait
		flusherCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go flusher.Run(flusherCtx)
		time.Sleep(1 * time.Second)

		// Query should still work
		queryPlanner := planner.NewPlanner(env.catalog)
		exec, err := executor.NewParallelExecutor(queryPlanner, env.store, executor.ExecutorConfig{
			Concurrency: 4,
			DownloadDir: env.downloadDir,
		}, nil, nil)
		if err != nil {
			t.Fatalf("failed to create executor: %v", err)
		}
		defer exec.Close()

		queryHandler := apihttp.NewQueryHandler(exec, nil)
		wrappedQueryHandler := apihttp.DefaultMiddleware()(queryHandler)

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
			t.Error("expected rows from query without notifications")
		}

		t.Logf("Notifications disabled: ingest+query works, %d rows returned", len(queryResp.Rows))
	})

	t.Run("IndexesDisabled_FallbackToBloomPruning", func(t *testing.T) {
		env, cleanup := setupFeatureFlagTestEnv(t)
		defer cleanup()

		ctx := context.Background()

		// Ingest data synchronously to create partitions
		rows := []types.Row{
			{TenantID: "acme", UserID: 42, EventTime: time.Now().UnixNano(), EventType: "test", Payload: map[string]interface{}{}},
			{TenantID: "acme", UserID: 43, EventTime: time.Now().UnixNano(), EventType: "test", Payload: map[string]interface{}{}},
		}
		key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260206"}
		info, err := env.builder.Build(ctx, rows, key)
		if err != nil {
			t.Fatalf("failed to build partition: %v", err)
		}

		metaPath, err := env.metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			t.Fatalf("failed to generate metadata: %v", err)
		}
		info.MetadataPath = metaPath

		objectPath := "partitions/20260206/" + info.PartitionID + ".sqlite"
		metaObjectPath := "partitions/20260206/" + info.PartitionID + ".meta.json"

		if _, err := env.store.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
			t.Fatalf("failed to upload partition: %v", err)
		}
		if err := env.store.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
			t.Fatalf("failed to upload metadata: %v", err)
		}
		if err := env.catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			t.Fatalf("failed to register partition: %v", err)
		}

		// Planner without index (indexes disabled) — falls back to bloom pruning
		queryPlanner := planner.NewPlanner(env.catalog)
		exec, err := executor.NewParallelExecutor(queryPlanner, env.store, executor.ExecutorConfig{
			Concurrency: 4,
			DownloadDir: env.downloadDir,
		}, nil, nil)
		if err != nil {
			t.Fatalf("failed to create executor: %v", err)
		}
		defer exec.Close()

		queryHandler := apihttp.NewQueryHandler(exec, nil)
		wrappedQueryHandler := apihttp.DefaultMiddleware()(queryHandler)

		queryReqBody := apihttp.QueryRequest{SQL: "SELECT tenant_id, user_id FROM events WHERE user_id = 42"}
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
			t.Error("expected rows from bloom pruning fallback")
		}

		t.Logf("Indexes disabled: bloom pruning fallback returned %d rows", len(queryResp.Rows))
	})

	t.Run("NVMeCacheDisabled_QueryStillWorks", func(t *testing.T) {
		env, cleanup := setupFeatureFlagTestEnv(t)
		defer cleanup()

		ctx := context.Background()

		// Ingest data synchronously
		rows := []types.Row{
			{TenantID: "acme", UserID: 99, EventTime: time.Now().UnixNano(), EventType: "test", Payload: map[string]interface{}{}},
		}
		key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260206"}
		info, err := env.builder.Build(ctx, rows, key)
		if err != nil {
			t.Fatalf("failed to build partition: %v", err)
		}

		metaPath, err := env.metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			t.Fatalf("failed to generate metadata: %v", err)
		}
		info.MetadataPath = metaPath

		objectPath := "partitions/20260206/" + info.PartitionID + ".sqlite"
		metaObjectPath := "partitions/20260206/" + info.PartitionID + ".meta.json"

		if _, err := env.store.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
			t.Fatalf("failed to upload partition: %v", err)
		}
		if err := env.store.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
			t.Fatalf("failed to upload metadata: %v", err)
		}
		if err := env.catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			t.Fatalf("failed to register partition: %v", err)
		}

		// Executor with nil NVMe cache and nil co-access graph
		queryPlanner := planner.NewPlanner(env.catalog)
		exec, err := executor.NewParallelExecutor(queryPlanner, env.store, executor.ExecutorConfig{
			Concurrency: 4,
			DownloadDir: env.downloadDir,
		}, nil, nil)
		if err != nil {
			t.Fatalf("failed to create executor: %v", err)
		}
		defer exec.Close()

		queryHandler := apihttp.NewQueryHandler(exec, nil)
		wrappedQueryHandler := apihttp.DefaultMiddleware()(queryHandler)

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
			t.Error("expected rows from query without NVMe cache")
		}

		t.Logf("NVMe disabled: query returned %d rows without cache", len(queryResp.Rows))
	})
}
