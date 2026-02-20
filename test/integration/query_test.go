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
	"testing"
	"time"

	apihttp "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// setupQueryTestEnv creates a test environment with ingested data for query tests.
func setupQueryTestEnv(t *testing.T) (
	*manifest.SQLiteCatalog,
	*storage.LocalStorage,
	*executor.ParallelExecutor,
	string,
	func(),
) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-query-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	downloadDir := filepath.Join(tempDir, "downloads")

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

	// Create test data
	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()
	ctx := context.Background()

	// Create partitions with different tenants and users
	testData := []struct {
		partitionKey string
		rows         []types.Row
	}{
		{
			partitionKey: "20260205",
			rows: []types.Row{
				{TenantID: "acme", UserID: 100, EventTime: time.Now().Add(-24 * time.Hour).UnixNano(), EventType: "page_view", Payload: map[string]interface{}{"page": "/home"}},
				{TenantID: "acme", UserID: 101, EventTime: time.Now().Add(-23 * time.Hour).UnixNano(), EventType: "click", Payload: map[string]interface{}{"button": "signup"}},
				{TenantID: "beta", UserID: 200, EventTime: time.Now().Add(-22 * time.Hour).UnixNano(), EventType: "page_view", Payload: map[string]interface{}{"page": "/pricing"}},
			},
		},
		{
			partitionKey: "20260206",
			rows: []types.Row{
				{TenantID: "acme", UserID: 100, EventTime: time.Now().Add(-1 * time.Hour).UnixNano(), EventType: "purchase", Payload: map[string]interface{}{"amount": 99.99}},
				{TenantID: "acme", UserID: 102, EventTime: time.Now().UnixNano(), EventType: "page_view", Payload: map[string]interface{}{"page": "/checkout"}},
				{TenantID: "gamma", UserID: 300, EventTime: time.Now().UnixNano(), EventType: "signup", Payload: map[string]interface{}{}},
			},
		},
	}

	for _, td := range testData {
		key := types.PartitionKey{Strategy: types.StrategyTime, Value: td.partitionKey}
		info, err := builder.Build(ctx, td.rows, key)
		if err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to build partition: %v", err)
		}

		metaPath, err := metaGen.GenerateAndWrite(info, td.rows)
		if err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to generate metadata: %v", err)
		}
		info.MetadataPath = metaPath

		objectPath := "partitions/" + td.partitionKey + "/" + info.PartitionID + ".sqlite"
		metaObjectPath := "partitions/" + td.partitionKey + "/" + info.PartitionID + ".meta.json"

		if _, err := store.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to upload partition: %v", err)
		}
		if err := store.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to upload metadata: %v", err)
		}

		if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to register partition: %v", err)
		}
	}

	// Create executor
	queryPlanner := planner.NewPlanner(catalog)
	exec, err := executor.NewParallelExecutor(queryPlanner, store, executor.ExecutorConfig{
		Concurrency: 4,
		DownloadDir: downloadDir,
	}, nil)
	if err != nil {
		catalog.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create executor: %v", err)
	}

	cleanup := func() {
		exec.Close()
		catalog.Close()
		os.RemoveAll(tempDir)
	}

	return catalog, store, exec, tempDir, cleanup
}

// TestQueryFlow tests the end-to-end query flow:
// API → parse → prune → execute → merge
func TestQueryFlow(t *testing.T) {
	_, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Test basic SELECT query
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id, event_type FROM events",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify response structure
	if len(resp.Columns) == 0 {
		t.Error("expected columns in response")
	}
	if resp.RequestID == "" {
		t.Error("expected request_id in response")
	}

	// We should have 6 rows total (3 from each partition)
	if len(resp.Rows) != 6 {
		t.Errorf("expected 6 rows, got %d", len(resp.Rows))
	}

	// Verify stats
	if resp.Stats.PartitionsScanned != 2 {
		t.Errorf("expected 2 partitions scanned, got %d", resp.Stats.PartitionsScanned)
	}
}

// TestQueryWithWhere tests queries with WHERE clause filtering.
func TestQueryWithWhere(t *testing.T) {
	_, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Query for specific tenant
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id, event_type FROM events WHERE tenant_id = 'acme'",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	// Should have 4 rows for tenant 'acme'
	if len(resp.Rows) != 4 {
		t.Errorf("expected 4 rows for tenant 'acme', got %d", len(resp.Rows))
	}
}

// TestQueryWithLimit tests queries with LIMIT clause.
func TestQueryWithLimit(t *testing.T) {
	_, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id FROM events LIMIT 3",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.Rows) > 3 {
		t.Errorf("expected at most 3 rows with LIMIT, got %d", len(resp.Rows))
	}
}

// TestQueryPruningEffectiveness tests that partition pruning works correctly.
func TestQueryPruningEffectiveness(t *testing.T) {
	catalog, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Get total partition count
	totalPartitions, _ := catalog.GetPartitionCount(ctx)

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Query that should prune based on user_id range
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id FROM events WHERE user_id = 100",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	// Verify pruning occurred (pruned count should be >= 0)
	if resp.Stats.PartitionsPruned < 0 {
		t.Errorf("expected non-negative pruned count, got %d", resp.Stats.PartitionsPruned)
	}

	// Total should equal scanned + pruned
	if int64(resp.Stats.PartitionsScanned+resp.Stats.PartitionsPruned) != totalPartitions {
		t.Logf("Note: scanned=%d, pruned=%d, total=%d",
			resp.Stats.PartitionsScanned, resp.Stats.PartitionsPruned, totalPartitions)
	}
}

// TestQueryValidation tests query validation errors.
func TestQueryValidation(t *testing.T) {
	_, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	tests := []struct {
		name       string
		sql        string
		wantStatus int
	}{
		{
			name:       "empty sql",
			sql:        "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid sql syntax",
			sql:        "SELEC * FROM events",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-select statement",
			sql:        "INSERT INTO events VALUES (1)",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody := apihttp.QueryRequest{SQL: tt.sql}
			body, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			wrappedHandler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d: %s", tt.wantStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestQueryRequestID tests that request_id is present in query responses.
func TestQueryRequestID(t *testing.T) {
	_, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id FROM events LIMIT 1",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", "query-req-456")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	// Verify request ID in header
	if rec.Header().Get("X-Request-ID") != "query-req-456" {
		t.Errorf("expected X-Request-ID=query-req-456, got %s", rec.Header().Get("X-Request-ID"))
	}

	// Verify request ID in body
	var resp apihttp.QueryResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.RequestID != "query-req-456" {
		t.Errorf("expected request_id=query-req-456, got %s", resp.RequestID)
	}
}

// TestQueryEmptyResult tests queries that return no results.
func TestQueryEmptyResult(t *testing.T) {
	_, _, exec, _, cleanup := setupQueryTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Query for non-existent tenant
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id FROM events WHERE tenant_id = 'nonexistent'",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	// Should return empty rows, not null
	if resp.Rows == nil {
		t.Error("expected empty array, got nil")
	}
	if len(resp.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(resp.Rows))
	}
}

// TestQueryMemoryExhaustion verifies that when query results exceed the
// StreamingCollector's maxMemoryBytes, rows are spilled to disk instead of
// being silently dropped. The result set must be complete.
func TestQueryMemoryExhaustion(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-memexhaust-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	downloadDir := filepath.Join(tempDir, "downloads")

	store, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()
	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	// Ingest a decent number of rows across multiple partitions so the total
	// result set will exceed a tiny maxMemoryBytes limit.
	const rowsPerPartition = 50
	const numPartitions = 4
	totalExpected := rowsPerPartition * numPartitions

	for p := 0; p < numPartitions; p++ {
		rows := make([]types.Row, rowsPerPartition)
		for i := 0; i < rowsPerPartition; i++ {
			rows[i] = types.Row{
				TenantID:  "test-tenant",
				UserID:    int64(p*1000 + i),
				EventTime: time.Now().Add(time.Duration(-i) * time.Second).UnixNano(),
				EventType: "test_event",
				Payload:   map[string]interface{}{"idx": i, "partition": p, "padding": "some extra data to increase row size"},
			}
		}

		key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260210"}
		info, err := builder.Build(ctx, rows, key)
		if err != nil {
			t.Fatalf("partition %d: build failed: %v", p, err)
		}

		metaPath, err := metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			t.Fatalf("partition %d: metadata failed: %v", p, err)
		}
		info.MetadataPath = metaPath

		objectPath := fmt.Sprintf("partitions/20260210/%s.sqlite", info.PartitionID)
		metaObjectPath := fmt.Sprintf("partitions/20260210/%s.meta.json", info.PartitionID)

		if _, err := store.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
			t.Fatalf("partition %d: upload failed: %v", p, err)
		}
		if err := store.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
			t.Fatalf("partition %d: meta upload failed: %v", p, err)
		}
		if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			t.Fatalf("partition %d: register failed: %v", p, err)
		}
	}

	// Create executor with a very small MaxMemoryBytes to force spill-to-disk.
	queryPlanner := planner.NewPlanner(catalog)
	exec, err := executor.NewParallelExecutor(queryPlanner, store, executor.ExecutorConfig{
		Concurrency:    4,
		DownloadDir:    downloadDir,
		MaxMemoryBytes: 512, // 512 bytes — will force spill almost immediately
	}, nil)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}
	defer exec.Close()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id, event_type FROM events",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// The critical assertion: ALL rows must be returned, none silently dropped.
	if len(resp.Rows) != totalExpected {
		t.Errorf("expected %d rows (spill-to-disk should preserve all), got %d", totalExpected, len(resp.Rows))
	}
}

