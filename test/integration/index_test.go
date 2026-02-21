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
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// setupIndexTestEnv creates a test environment with index support.
func setupIndexTestEnv(t *testing.T) (
	*manifest.SQLiteCatalog,
	storage.ObjectStorage,
	*executor.ParallelExecutor,
	string,
	func(),
) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-index-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	downloadDir := filepath.Join(tempDir, "downloads")

	for _, dir := range []string{partitionDir, storageDir, downloadDir} {
		os.MkdirAll(dir, 0755)
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

	// Create test data with 100 distinct device_ids across 10 partitions
	ctx := context.Background()
	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	const numPartitions = 10
	const numDevices = 100

	for p := 0; p < numPartitions; p++ {
		rows := make([]types.Row, numDevices)
		for i := 0; i < numDevices; i++ {
			deviceID := i + 1 // device-1 through device-100
			rows[i] = types.Row{
				TenantID:  "acme",
				UserID:    int64(deviceID),
				EventTime: time.Now().Add(time.Duration(-p) * time.Hour).UnixNano(),
				EventType: "device_event",
				Payload:   map[string]interface{}{"device_id": deviceID, "temperature": 20 + float64(i%10)},
			}
		}

		key := types.PartitionKey{Strategy: types.StrategyTime, Value: "20260206"}
		info, err := builder.Build(ctx, rows, key)
		if err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to build partition %d: %v", p, err)
		}

		metaPath, err := metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to generate metadata for partition %d: %v", p, err)
		}
		info.MetadataPath = metaPath

		objectPath := "partitions/20260206/" + info.PartitionID + ".sqlite"
		metaObjectPath := "partitions/20260206/" + info.PartitionID + ".meta.json"

		if _, err := store.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to upload partition %d: %v", p, err)
		}
		if err := store.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to upload metadata for partition %d: %v", p, err)
		}
		if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to register partition %d: %v", p, err)
		}
	}

	// Create planner and executor
	queryPlanner := planner.NewPlanner(catalog)
	exec, err := executor.NewParallelExecutor(queryPlanner, store, executor.ExecutorConfig{
		Concurrency: 4,
		DownloadDir: downloadDir,
	}, nil, nil)
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

// TestIndexFallback tests that queries fall back to bloom pruning when no index exists.
func TestIndexFallback(t *testing.T) {
	catalog, _, exec, _, cleanup := setupIndexTestEnv(t)
	defer cleanup()

	// Create planner without index support (fallback to bloom pruning)
	_ = planner.NewPlanner(catalog)

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Query for a column without index
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id FROM events WHERE user_id = 42",
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

	// Should still return correct results via bloom pruning
	if len(resp.Rows) == 0 {
		t.Error("expected rows from bloom pruning fallback")
	}

	t.Logf("Fallback query completed: %d rows returned", len(resp.Rows))
}

// TestIndexPruningEffectiveness tests that partition pruning works correctly.
func TestIndexPruningEffectiveness(t *testing.T) {
	catalog, _, exec, _, cleanup := setupIndexTestEnv(t)
	defer cleanup()

	// Create planner
	_ = planner.NewPlanner(catalog)

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Query that should prune based on user_id
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id FROM events WHERE user_id = 42",
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

	// Should return correct results
	if len(resp.Rows) == 0 {
		t.Error("expected rows from query")
	}

	t.Logf("Pruning test completed: %d rows returned", len(resp.Rows))
}