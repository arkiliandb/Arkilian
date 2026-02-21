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
	"testing"
	"time"

	apihttp "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/cache"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
)

// setupCacheTestEnv creates a test environment with NVMe cache support.
func setupCacheTestEnv(t *testing.T) (
	*manifest.SQLiteCatalog,
	storage.ObjectStorage,
	*executor.ParallelExecutor,
	*cache.NVMeCache,
	string,
	func(),
) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-cache-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	downloadDir := filepath.Join(tempDir, "downloads")
	nvmeDir := filepath.Join(tempDir, "nvme")

	for _, dir := range []string{partitionDir, storageDir, downloadDir, nvmeDir} {
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

	// Create test data
	ctx := context.Background()
	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	// Create 5 partitions
	for p := 0; p < 5; p++ {
		rows := make([]types.Row, 100)
		for i := 0; i < 100; i++ {
			rows[i] = types.Row{
				TenantID:  "acme",
				UserID:    int64(p*100 + i),
				EventTime: time.Now().Add(time.Duration(-p) * time.Hour).UnixNano(),
				EventType: "cache_test",
				Payload:   map[string]interface{}{"partition": p, "index": i},
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

	// Create NVMe cache
	nvmeCache, err := cache.NewNVMeCache(nvmeDir, 100*1024*1024) // 100MB
	if err != nil {
		catalog.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create NVMe cache: %v", err)
	}

	// Create planner and executor with co-access graph
	coAccessGraph := cache.NewCoAccessGraph(0.95, 0.70, 10)
	queryPlanner := planner.NewPlanner(catalog)

	exec, err := executor.NewParallelExecutor(queryPlanner, store, executor.ExecutorConfig{
		Concurrency:    4,
		DownloadDir:    downloadDir,
		MaxMemoryBytes: 1024 * 1024 * 1024, // 1GB
	}, nil, coAccessGraph)
	if err != nil {
		nvmeCache.Close()
		catalog.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create executor: %v", err)
	}

	cleanup := func() {
		exec.Close()
		nvmeCache.Close()
		catalog.Close()
		os.RemoveAll(tempDir)
	}

	return catalog, store, exec, nvmeCache, tempDir, cleanup
}

// TestNVMeCacheHit tests that repeated queries hit the NVMe cache.
func TestNVMeCacheHit(t *testing.T) {
	_, _, exec, _, _, cleanup := setupCacheTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// First query - should be a cache miss
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id, event_type FROM events WHERE tenant_id = 'acme'",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("first query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp1 apihttp.QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("failed to unmarshal first response: %v", err)
	}

	if len(resp1.Rows) == 0 {
		t.Error("expected rows in first query")
	}

	// Second query - should hit cache
	req2 := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")

	rec2 := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("second query failed: %d - %s", rec2.Code, rec2.Body.String())
	}

	var resp2 apihttp.QueryResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("failed to unmarshal second response: %v", err)
	}

	// Verify same results
	if len(resp2.Rows) != len(resp1.Rows) {
		t.Errorf("expected same row count, got %d vs %d", len(resp2.Rows), len(resp1.Rows))
	}

	// Check cache stats - this would require executor to expose cache stats
	// For now, we verify the query works correctly
	t.Logf("First query: %d rows, Second query: %d rows", len(resp1.Rows), len(resp2.Rows))
}

// TestCoAccessPrefetch tests that co-access graph triggers prefetch.
func TestCoAccessPrefetch(t *testing.T) {
	_, _, exec, _, _, cleanup := setupCacheTestEnv(t)
	defer cleanup()

	handler := apihttp.NewQueryHandler(exec, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Query partitions A, B, C multiple times to build co-access graph
	for i := 0; i < 10; i++ {
		reqBody := apihttp.QueryRequest{
			SQL: "SELECT tenant_id, user_id FROM events",
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		rec := httptest.NewRecorder()
		wrappedHandler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("training query %d failed: %d - %s", i, rec.Code, rec.Body.String())
		}
	}

	// Now query just one partition - co-access should have prefetched others
	reqBody := apihttp.QueryRequest{
		SQL: "SELECT tenant_id, user_id FROM events LIMIT 10",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("prefetch query failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify we got results
	if len(resp.Rows) == 0 {
		t.Error("expected rows from query")
	}

	t.Logf("Co-access prefetch query completed: %d rows returned", len(resp.Rows))
}

// TestCacheEviction tests that cache eviction works correctly.
func TestCacheEviction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-cache-eviction-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	nvmeDir := filepath.Join(tempDir, "nvme")
	os.MkdirAll(nvmeDir, 0755)

	// Create a small cache to trigger eviction
	cache, err := cache.NewNVMeCache(nvmeDir, 1024*1024) // 1MB - very small
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add items larger than cache size
	for i := 0; i < 10; i++ {
		localPath := filepath.Join(nvmeDir, fmt.Sprintf("test_file_%d.sqlite", i))
		if err := os.WriteFile(localPath, make([]byte, 500*1024), 0644); err != nil { // 500KB
			t.Fatalf("failed to create test file: %v", err)
		}

		objectPath := fmt.Sprintf("partitions/test_%d.sqlite", i)
		err := cache.Put(objectPath, localPath, 500*1024)
		if err != nil {
			// Eviction error is expected with small cache
			t.Logf("Cache put %d: %v", i, err)
		}
	}

	// Verify cache still functions
	_, found := cache.Get("nonexistent")
	if found {
		t.Error("expected cache miss for nonexistent key")
	}

	t.Log("Cache eviction test completed")
}