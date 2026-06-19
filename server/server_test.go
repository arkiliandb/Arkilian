// Arkilian Control Plane — server tests (multi-tenant, per-db API keys)

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func init() {
	authToken = ""
	jwtSecret = []byte("test-secret")
}

func setupTestDB(t *testing.T) {
	t.Helper()
	var err error
	db, err = sql.Open("sqlite3", ":memory:?_journal_mode=MEMORY&_synchronous=OFF")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			email         TEXT UNIQUE NOT NULL,
			password_hash TEXT NOT NULL,
			created_at    INTEGER DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS databases (
			db_id     TEXT PRIMARY KEY,
			user_id   INTEGER NOT NULL REFERENCES users(id),
			name      TEXT NOT NULL,
			api_key   TEXT UNIQUE NOT NULL,
			created_at INTEGER DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS wal_entries (
			lsn         INTEGER PRIMARY KEY AUTOINCREMENT,
			db_id       TEXT NOT NULL REFERENCES databases(db_id),
			ts          INTEGER NOT NULL,
			op          INTEGER NOT NULL,
			table_id    INTEGER NOT NULL,
			pk          INTEGER NOT NULL,
			sql         TEXT,
			received_at INTEGER DEFAULT (unixepoch())
		);
		CREATE INDEX IF NOT EXISTS idx_wal_db ON wal_entries(db_id, lsn);
		CREATE TABLE IF NOT EXISTS snapshots (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			db_id        TEXT NOT NULL REFERENCES databases(db_id),
			baseline_lsn INTEGER NOT NULL,
			s3_key       TEXT NOT NULL,
			created_at   INTEGER DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS chunks (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			db_id      TEXT NOT NULL REFERENCES databases(db_id),
			lsn_start  INTEGER NOT NULL,
			lsn_end    INTEGER NOT NULL,
			s3_key     TEXT NOT NULL,
			created_at  INTEGER DEFAULT (unixepoch())
		);
	`)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
}

// register + login helper.  Returns session JWT.
func registerAndLogin(t *testing.T) string {
	t.Helper()

	// Register
	body := `{"email":"test@arkilian.com","password":"secret123"}`
	req := httptest.NewRequest("POST", "/v1/auth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handleRegister(w, req)
	if w.Code != 201 {
		t.Fatalf("register: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// Login
	req = httptest.NewRequest("POST", "/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handleLogin(w, req)
	if w.Code != 200 {
		t.Fatalf("login: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp LoginResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Token == "" {
		t.Fatal("login: expected non-empty token")
	}
	return resp.Token
}

// createDB helper.  Returns db_id + api_key.
func createDB(t *testing.T, sessionToken string, name string) (string, string) {
	t.Helper()
	body := fmt.Sprintf(`{"name":"%s"}`, name)
	req := httptest.NewRequest("POST", "/v1/db/create", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sessionToken)
	w := httptest.NewRecorder()
	handleDBCreate(w, req)
	if w.Code != 201 {
		t.Fatalf("create db: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp CreateDBResponse
	json.NewDecoder(w.Body).Decode(&resp)
	return resp.DBID, resp.APIKey
}

// ── Tests ───────────────────────────────────────────────────────────

func TestRegisterAndLogin(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	if token == "" {
		t.Fatal("expected non-empty session token")
	}
}

func TestRegisterDuplicate(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	body := `{"email":"dup@test.com","password":"secret123"}`
	req := httptest.NewRequest("POST", "/v1/auth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handleRegister(w, req)
	if w.Code != 201 {
		t.Fatalf("first register: %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/auth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handleRegister(w, req)
	if w.Code != 409 {
		t.Fatalf("expected 409 conflict, got %d: %s", w.Code, w.Body.String())
	}
}

func TestLoginInvalid(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	registerAndLogin(t)

	body := `{"email":"test@arkilian.com","password":"wrongpass"}`
	req := httptest.NewRequest("POST", "/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handleLogin(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestDBCreateAndList(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)

	// Create 3 databases
	createDB(t, token, "production")
	createDB(t, token, "staging")
	createDB(t, token, "analytics")

	// List databases
	req := httptest.NewRequest("GET", "/v1/db/list", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	handleDBList(w, req)
	if w.Code != 200 {
		t.Fatalf("list: %d", w.Code)
	}

	var dbs []DBInfo
	json.NewDecoder(w.Body).Decode(&dbs)
	if len(dbs) != 3 {
		t.Fatalf("expected 3 databases, got %d", len(dbs))
	}
	// Verify all expected names exist (order may vary with same-second timestamps)
	names := map[string]bool{"production": false, "staging": false, "analytics": false}
	for _, d := range dbs {
		names[d.Name] = true
	}
	for name, found := range names {
		if !found {
			t.Fatalf("missing database: %s", name)
		}
	}
}

func TestDBKeyRetrieval(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	dbID, apiKey := createDB(t, token, "my-app")

	// Get key for this database
	req := httptest.NewRequest("GET", "/v1/db/"+dbID+"/key", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	handleDBKey(w, req)
	if w.Code != 200 {
		t.Fatalf("get key: %d: %s", w.Code, w.Body.String())
	}

	var kr KeyResponse
	json.NewDecoder(w.Body).Decode(&kr)
	if kr.APIKey != apiKey {
		t.Fatalf("api key mismatch: expected %s, got %s", apiKey, kr.APIKey)
	}
}

func TestDBKeyWrongUser(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	// User 1 creates DB
	token1 := registerAndLogin(t)
	dbID, _ := createDB(t, token1, "user1-db")

	// Register user 2 and try to access user 1's DB key
	body := `{"email":"user2@test.com","password":"secret123"}`
	req := httptest.NewRequest("POST", "/v1/auth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handleRegister(w, req)
	if w.Code != 201 {
		t.Fatalf("register user2: %d", w.Code)
	}
	req = httptest.NewRequest("POST", "/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handleLogin(w, req)
	var lr LoginResponse
	json.NewDecoder(w.Body).Decode(&lr)
	token2 := lr.Token

	// User 2 tries to get user 1's DB key
	req = httptest.NewRequest("GET", "/v1/db/"+dbID+"/key", nil)
	req.Header.Set("Authorization", "Bearer "+token2)
	w = httptest.NewRecorder()
	handleDBKey(w, req)
	if w.Code != 404 {
		t.Fatalf("expected 404 (db not found for user2), got %d", w.Code)
	}
}

func TestWALPushWithAPIKey(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	_, apiKey := createDB(t, token, "my-db")

	// Push WAL entries using the API key
	entries := `[
		{"ts":100,"op":1,"table_id":1,"pk":1,"sql":"INSERT INTO t VALUES (1)"},
		{"ts":101,"op":1,"table_id":1,"pk":2,"sql":"INSERT INTO t VALUES (2)"}
	]`
	req := httptest.NewRequest("POST", "/v1/wal/push", strings.NewReader(entries))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()
	handleWALPush(w, req)
	if w.Code != 200 {
		t.Fatalf("wal push: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify entries in DB
	var count int
	db.QueryRow("SELECT COUNT(*) FROM wal_entries").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2 wal entries, got %d", count)
	}
}

func TestWALPushInvalidAPIKey(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	req := httptest.NewRequest("POST", "/v1/wal/push",
		strings.NewReader(`[{"ts":1,"op":1,"table_id":1,"pk":1,"sql":""}]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer invalid-key")
	w := httptest.NewRecorder()
	handleWALPush(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestWALPushSessionTokenRejected(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	createDB(t, token, "my-db")

	// Try to push WAL using the session token (not the API key)
	req := httptest.NewRequest("POST", "/v1/wal/push",
		strings.NewReader(`[{"ts":1,"op":1,"table_id":1,"pk":1,"sql":""}]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	handleWALPush(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401 (session token not valid for WAL push), got %d", w.Code)
	}
}

func TestHydratePlanWithAPIKey(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	dbID, apiKey := createDB(t, token, "my-db")

	// Register a snapshot
	db.Exec(`INSERT INTO snapshots (db_id, baseline_lsn, s3_key)
		VALUES (?, 100, 'snaps/snap_100.db')`, dbID)

	// Register chunks
	db.Exec(`INSERT INTO chunks (db_id, lsn_start, lsn_end, s3_key)
		VALUES (?, 101, 5000, 'chunks/chunk_101_5000.zst')`, dbID)

	// Hydrate plan with API key
	req := httptest.NewRequest("GET", "/v1/hydrate/plan", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()
	handleHydratePlan(w, req)
	if w.Code != 200 {
		t.Fatalf("hydrate plan: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var plan HydratePlanResponse
	json.NewDecoder(w.Body).Decode(&plan)
	if plan.BaselineLSN != 100 {
		t.Fatalf("expected baseline_lsn=100, got %d", plan.BaselineLSN)
	}
	if len(plan.Chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(plan.Chunks))
	}
}

func TestUploadRequestWithAPIKey(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	_, apiKey := createDB(t, token, "my-db")

	body := `{"db_id":"x","event_count":100,"lsn_start":1,"lsn_end":100}`
	req := httptest.NewRequest("POST", "/v1/upload/request", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()
	handleUploadRequest(w, req)
	if w.Code != 200 {
		t.Fatalf("upload request: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp UploadResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.UploadURL == "" {
		t.Fatal("expected non-empty upload_url")
	}
}

func TestDBManagementRequiresSession(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	// Try to list DBs without auth
	req := httptest.NewRequest("GET", "/v1/db/list", nil)
	w := httptest.NewRecorder()
	handleDBList(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}

	// Try to create DB without auth
	req = httptest.NewRequest("POST", "/v1/db/create",
		strings.NewReader(`{"name":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handleDBCreate(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

// ── Helpers ─────────────────────────────────────────────────────────
