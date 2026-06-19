// Arkilian Control Plane — server tests
//
// Run:  go test -v ./...

package main

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func init() {
	authToken = "test-token"
}

// ── Health ───────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	handleHealth(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]bool
	json.NewDecoder(w.Body).Decode(&resp)
	if !resp["ok"] {
		t.Fatal("expected ok:true")
	}
}

// ── Upload request ──────────────────────────────────────────────────

func TestUploadRequest(t *testing.T) {
	// Setup in-memory SQLite
	var err error
	db, err = initTestDB()
	if err != nil {
		t.Fatalf("init test db: %v", err)
	}
	defer db.Close()

	body := `{"token":"test-token","db_id":"test-db","event_count":100,"lsn_start":1,"lsn_end":100}`
	req := httptest.NewRequest("POST", "/v1/upload/request", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handleUploadRequest(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp UploadResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.UploadURL == "" {
		t.Fatal("expected non-empty upload_url")
	}
	if resp.ExpiresAt == 0 {
		t.Fatal("expected non-zero expires_at")
	}
	if !strings.Contains(resp.UploadURL, "test-db") {
		t.Fatalf("expected upload_url to contain db_id, got: %s", resp.UploadURL)
	}
	t.Logf("upload_url: %s", resp.UploadURL)
}

func TestUploadRequestUnauthorized(t *testing.T) {
	var err error
	db, err = initTestDB()
	if err != nil {
		t.Fatalf("init test db: %v", err)
	}
	defer db.Close()

	body := `{"token":"test-token","db_id":"x","event_count":1,"lsn_start":1,"lsn_end":1}`
	req := httptest.NewRequest("POST", "/v1/upload/request", strings.NewReader(body))
	// No auth header

	w := httptest.NewRecorder()
	handleUploadRequest(w, req)

	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestUploadRequestBadMethod(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/upload/request", nil)
	w := httptest.NewRecorder()
	handleUploadRequest(w, req)
	if w.Code != 405 {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// ── Snapshot register ───────────────────────────────────────────────

func TestSnapshotRegister(t *testing.T) {
	var err error
	db, err = initTestDB()
	if err != nil {
		t.Fatalf("init test db: %v", err)
	}
	defer db.Close()

	body := `{"baseline_lsn":42,"s3_key":"db_x/snapshots/snap_42.db"}`
	req := httptest.NewRequest("POST", "/v1/snapshot/register", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handleSnapshotRegister(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

// ── Hydrate plan ────────────────────────────────────────────────────

func TestHydratePlanNoSnapshot(t *testing.T) {
	var err error
	db, err = initTestDB()
	if err != nil {
		t.Fatalf("init test db: %v", err)
	}
	defer db.Close()

	req := httptest.NewRequest("GET", "/v1/hydrate/plan", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	w := httptest.NewRecorder()
	handleHydratePlan(w, req)

	if w.Code != 404 {
		t.Fatalf("expected 404 (no snapshot), got %d", w.Code)
	}
}

func TestHydratePlanWithSnapshot(t *testing.T) {
	var err error
	db, err = initTestDB()
	if err != nil {
		t.Fatalf("init test db: %v", err)
	}
	defer db.Close()

	// Register a snapshot
	db.Exec("INSERT INTO snapshots (baseline_lsn, s3_key) VALUES (100, 'db_x/snapshots/snap_100.db')")

	// Register two chunks after the snapshot
	db.Exec("INSERT INTO chunks (lsn_start, lsn_end, s3_key) VALUES (101, 5000, 'db_x/chunks/chunk_101_5000.zst')")
	db.Exec("INSERT INTO chunks (lsn_start, lsn_end, s3_key) VALUES (5001, 8000, 'db_x/chunks/chunk_5001_8000.zst')")

	// Also register a chunk BEFORE the snapshot (should be filtered out)
	db.Exec("INSERT INTO chunks (lsn_start, lsn_end, s3_key) VALUES (1, 50, 'db_x/chunks/chunk_1_50.zst')")

	req := httptest.NewRequest("GET", "/v1/hydrate/plan", nil)
	req.Header.Set("Authorization", "Bearer test-token")

	w := httptest.NewRecorder()
	handleHydratePlan(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var plan HydratePlanResponse
	if err := json.NewDecoder(w.Body).Decode(&plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}

	if plan.SnapshotURL == "" {
		t.Fatal("expected non-empty snapshot_url")
	}
	if plan.BaselineLSN != 100 {
		t.Fatalf("expected baseline_lsn=100, got %d", plan.BaselineLSN)
	}
	if len(plan.Chunks) != 2 {
		t.Fatalf("expected 2 chunks (after snapshot), got %d", len(plan.Chunks))
	}
	if plan.Chunks[0].LSNStart != 101 || plan.Chunks[0].LSNEnd != 5000 {
		t.Fatalf("chunk 0 range wrong: %d-%d", plan.Chunks[0].LSNStart, plan.Chunks[0].LSNEnd)
	}
	if plan.Chunks[1].LSNStart != 5001 || plan.Chunks[1].LSNEnd != 8000 {
		t.Fatalf("chunk 1 range wrong: %d-%d", plan.Chunks[1].LSNStart, plan.Chunks[1].LSNEnd)
	}

	t.Logf("snapshot_url: %s", plan.SnapshotURL)
	t.Logf("chunks: %d", len(plan.Chunks))
}

func TestHydratePlanUnauthorized(t *testing.T) {
	var err error
	db, err = initTestDB()
	if err != nil {
		t.Fatalf("init test db: %v", err)
	}
	defer db.Close()

	req := httptest.NewRequest("GET", "/v1/hydrate/plan", nil)
	// No auth header
	w := httptest.NewRecorder()
	handleHydratePlan(w, req)

	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

// ── Auth ────────────────────────────────────────────────────────────

func TestCheckAuthNoToken(t *testing.T) {
	old := authToken
	authToken = ""
	defer func() { authToken = old }()

	req := httptest.NewRequest("GET", "/", nil)
	if !checkAuth(req) {
		t.Fatal("expected auth to pass when no token required")
	}
}

func TestCheckAuthValid(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	if !checkAuth(req) {
		t.Fatal("expected valid bearer token to pass")
	}
}

func TestCheckAuthInvalid(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	if checkAuth(req) {
		t.Fatal("expected wrong bearer token to fail")
	}
}

func TestCheckAuthMissing(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	if checkAuth(req) {
		t.Fatal("expected missing auth header to fail")
	}
}

// ── Signed URL format ───────────────────────────────────────────────

func TestSignedURLFormat(t *testing.T) {
	s3Endpoint = "https://s3.amazonaws.com"
	s3Bucket = "my-bucket"
	s3AccessKey = "AKIAIOSFODNN7EXAMPLE"

	url, expires := signedURL("GET", "path/to/object.db", 3600*time.Second)

	if url == "" {
		t.Fatal("expected non-empty url")
	}
	if expires == 0 {
		t.Fatal("expected non-zero expires")
	}
	if !strings.Contains(url, "my-bucket") {
		t.Fatalf("expected url to contain bucket name, got: %s", url)
	}
	if !strings.Contains(url, "path/to/object.db") {
		t.Fatalf("expected url to contain key, got: %s", url)
	}
	if !strings.Contains(url, "X-Amz-Algorithm=AWS4-HMAC-SHA256") {
		t.Fatal("expected sig v4 algorithm in URL")
	}

	t.Logf("signed GET URL: %s", url)
}

func TestSignedURLOptions(t *testing.T) {
	s3Endpoint = "http://minio:9000"
	s3Bucket = "arkilian-test"

	url1, _ := signedURL("PUT", "uploads/chunk.zst", 600*time.Second)
	if !strings.Contains(url1, "X-Amz-Expires=600") {
		t.Fatalf("expected 600s expiry, got: %s", url1)
	}

	url2, _ := signedURL("GET", "snapshots/base.db", 3600*time.Second)
	if !strings.Contains(url2, "X-Amz-Expires=3600") {
		t.Fatalf("expected 3600s expiry, got: %s", url2)
	}
}

// ── Helpers ─────────────────────────────────────────────────────────

// initTestDB creates an in-memory SQLite database for testing.
func initTestDB() (*sql.DB, error) {
	return initTestDBPath(":memory:")
}

func initTestDBPath(path string) (*sql.DB, error) {
	var err error
	db, err = sql.Open("sqlite3", path+"?_journal_mode=MEMORY&_synchronous=OFF")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			baseline_lsn INTEGER NOT NULL,
			s3_key TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS chunks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			lsn_start INTEGER NOT NULL,
			lsn_end INTEGER NOT NULL,
			s3_key TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS db_registry (
			db_id TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL DEFAULT (unixepoch())
		);
	`)
	return db, err
}
