package server

import (
	"bytes"
	"encoding/xml"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func setupTestServer(t *testing.T) (*httptest.Server, *StorageEngine, string) {
	tempDir, err := os.MkdirTemp("", "arkilian-s3-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	storage, err := NewStorageEngine(tempDir)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}

	verifier := NewSigV4Verifier("test-access-key", "test-secret-key", "us-east-1")
	s3Handler := NewS3Handler(storage, verifier, false)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	ts := httptest.NewUnstartedServer(s3Handler)
	ts.Listener = l
	ts.Start()

	return ts, storage, tempDir
}

func TestS3PutAndGetObject(t *testing.T) {
	ts, _, tempDir := setupTestServer(t)
	defer ts.Close()
	defer os.RemoveAll(tempDir)

	client := ts.Client()
	bucket := "mybucket"
	key := "test-snapshot.sqlite"
	content := []byte("SQLITE FORMAT 3 -- Test Arkilian Snapshot Data")

	// 1. PUT Object
	putReq, err := http.NewRequest(http.MethodPut, ts.URL+"/"+bucket+"/"+key, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Failed to create PUT request: %v", err)
	}
	putReq.Header.Set("Content-Type", "application/x-sqlite3")

	putResp, err := client.Do(putReq)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected PUT 200 OK, got %d", putResp.StatusCode)
	}
	etag := putResp.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("Expected non-empty ETag header")
	}

	// 2. HEAD Object
	headReq, err := http.NewRequest(http.MethodHead, ts.URL+"/"+bucket+"/"+key, nil)
	if err != nil {
		t.Fatalf("Failed to create HEAD request: %v", err)
	}
	headResp, err := client.Do(headReq)
	if err != nil {
		t.Fatalf("HEAD request failed: %v", err)
	}
	defer headResp.Body.Close()

	if headResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected HEAD 200 OK, got %d", headResp.StatusCode)
	}
	if headResp.Header.Get("Content-Length") != "46" {
		t.Fatalf("Expected Content-Length 46, got %s", headResp.Header.Get("Content-Length"))
	}

	// 3. GET Object
	getReq, err := http.NewRequest(http.MethodGet, ts.URL+"/"+bucket+"/"+key, nil)
	if err != nil {
		t.Fatalf("Failed to create GET request: %v", err)
	}
	getResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected GET 200 OK, got %d", getResp.StatusCode)
	}
	gotContent, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("Failed to read GET body: %v", err)
	}
	if !bytes.Equal(gotContent, content) {
		t.Fatalf("Content mismatch: expected %q, got %q", string(content), string(gotContent))
	}

	// 4. GET Partial Range Request
	rangeReq, err := http.NewRequest(http.MethodGet, ts.URL+"/"+bucket+"/"+key, nil)
	if err != nil {
		t.Fatalf("Failed to create Range GET request: %v", err)
	}
	rangeReq.Header.Set("Range", "bytes=0-14")
	rangeResp, err := client.Do(rangeReq)
	if err != nil {
		t.Fatalf("Range GET request failed: %v", err)
	}
	defer rangeResp.Body.Close()

	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("Expected 206 Partial Content, got %d", rangeResp.StatusCode)
	}
	rangeContent, err := io.ReadAll(rangeResp.Body)
	if err != nil {
		t.Fatalf("Failed to read range body: %v", err)
	}
	if string(rangeContent) != "SQLITE FORMAT 3" {
		t.Fatalf("Range mismatch: expected 'SQLITE FORMAT 3', got %q", string(rangeContent))
	}

	// 5. List Objects
	listReq, err := http.NewRequest(http.MethodGet, ts.URL+"/"+bucket+"?prefix=test-", nil)
	if err != nil {
		t.Fatalf("Failed to create List request: %v", err)
	}
	listResp, err := client.Do(listReq)
	if err != nil {
		t.Fatalf("List request failed: %v", err)
	}
	defer listResp.Body.Close()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected List 200 OK, got %d", listResp.StatusCode)
	}
	var listResult ListBucketResult
	if err := xml.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatalf("Failed to decode ListBucketResult XML: %v", err)
	}
	if len(listResult.Contents) != 1 || listResult.Contents[0].Key != key {
		t.Fatalf("List result mismatch: expected key %s", key)
	}

	// 6. DELETE Object
	delReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/"+bucket+"/"+key, nil)
	if err != nil {
		t.Fatalf("Failed to create DELETE request: %v", err)
	}
	delResp, err := client.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE request failed: %v", err)
	}
	defer delResp.Body.Close()

	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("Expected DELETE 204 No Content, got %d", delResp.StatusCode)
	}

	// 7. Verify GET after DELETE returns 404
	getAfterDelResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("GET after DELETE request failed: %v", err)
	}
	defer getAfterDelResp.Body.Close()
	if getAfterDelResp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected GET after DELETE 404 Not Found, got %d", getAfterDelResp.StatusCode)
	}
}

func TestPathTraversalProtection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-path-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := NewStorageEngine(tempDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Test path traversal attempt
	_, err = storage.PutObject("../bucket", "../../etc/passwd", "text/plain", bytes.NewReader([]byte("test")), nil)
	if err == nil || err != ErrInvalidPath {
		t.Fatalf("Expected ErrInvalidPath for traversal attempt, got %v", err)
	}

	// Verify no file was written outside rootDir
	outsidePath := filepath.Join(tempDir, "..", "etc", "passwd")
	if _, err := os.Stat(outsidePath); !os.IsNotExist(err) {
		t.Fatalf("Path traversal succeeded in writing file outside rootDir!")
	}
}
