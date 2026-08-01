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

	_, err = db.Exec(schemaSQL)
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

// ── Monitoring ──────────────────────────────────────────────────────

func TestWALPushUpdatesMonitorStats(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	dbID, apiKey := createDB(t, token, "stats-db")

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

	var total, today int64
	var lastSeen int64
	err := db.QueryRow(
		"SELECT total_entries, entries_today, last_seen FROM db_stats WHERE db_id = ?",
		dbID).Scan(&total, &today, &lastSeen)
	if err != nil {
		t.Fatalf("db_stats row missing after push: %v", err)
	}
	if total != 2 || today != 2 {
		t.Fatalf("expected total=2 today=2, got total=%d today=%d", total, today)
	}
	if lastSeen == 0 {
		t.Fatal("expected last_seen to be set after push")
	}

	// Daily series must contain today's bucket.
	var dayEntries int64
	err = db.QueryRow(
		"SELECT entries FROM db_daily_stats WHERE db_id = ? AND day = ?",
		dbID, dayBucket(lastSeen)).Scan(&dayEntries)
	if err != nil || dayEntries != 2 {
		t.Fatalf("daily stats: expected 2 entries today, got %d (err=%v)", dayEntries, err)
	}
}

func TestMonitorSummary(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	dbID, apiKey := createDB(t, token, "monitor-db")

	req := httptest.NewRequest("POST", "/v1/wal/push",
		strings.NewReader(`{"ts":1,"op":1,"table_id":1,"pk":1,"sql":"INSERT"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()
	handleWALPush(w, req)
	if w.Code != 200 {
		t.Fatalf("wal push: %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/monitor/summary", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handleMonitorSummary(w, req)
	if w.Code != 200 {
		t.Fatalf("summary: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var dbs []MonitorDB
	json.NewDecoder(w.Body).Decode(&dbs)
	if len(dbs) != 1 {
		t.Fatalf("expected 1 database in summary, got %d", len(dbs))
	}
	d := dbs[0]
	if d.DBID != dbID || d.Name != "monitor-db" {
		t.Fatalf("summary mismatch: %+v", d)
	}
	if d.TotalEntries != 1 || d.EntriesToday != 1 {
		t.Fatalf("expected total=1 today=1, got total=%d today=%d", d.TotalEntries, d.EntriesToday)
	}
	if d.Status != "active" {
		t.Fatalf("expected status active after push, got %q", d.Status)
	}
	if len(d.Last7) != 1 {
		t.Fatalf("expected 1 daily point in last7, got %d", len(d.Last7))
	}
}

func TestMonitorDetail(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token := registerAndLogin(t)
	dbID, apiKey := createDB(t, token, "detail-db")

	req := httptest.NewRequest("POST", "/v1/wal/push",
		strings.NewReader(`[{"ts":1,"op":1,"table_id":1,"pk":1,"sql":"INSERT INTO t VALUES (1)"},{"ts":2,"op":2,"table_id":2,"pk":7,"sql":"UPDATE t SET v=2 WHERE id=7"}]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()
	handleWALPush(w, req)
	if w.Code != 200 {
		t.Fatalf("wal push: %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/monitor/db/"+dbID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handleMonitorDetail(w, req)
	if w.Code != 200 {
		t.Fatalf("detail: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var d MonitorDetail
	json.NewDecoder(w.Body).Decode(&d)
	if d.DBID != dbID {
		t.Fatalf("detail db_id mismatch: %s", d.DBID)
	}
	if len(d.RecentEntries) != 2 {
		t.Fatalf("expected 2 recent entries, got %d", len(d.RecentEntries))
	}
	if d.RecentEntries[0].SQL != "UPDATE t SET v=2 WHERE id=7" {
		t.Fatalf("expected newest entry first, got %q", d.RecentEntries[0].SQL)
	}
	if len(d.Daily) != 1 {
		t.Fatalf("expected 1 daily point, got %d", len(d.Daily))
	}
}

func TestMonitorRequiresSession(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	req := httptest.NewRequest("GET", "/v1/monitor/summary", nil)
	w := httptest.NewRecorder()
	handleMonitorSummary(w, req)
	if w.Code != 401 {
		t.Fatalf("summary without auth: expected 401, got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/monitor/db/db_x", nil)
	w = httptest.NewRecorder()
	handleMonitorDetail(w, req)
	if w.Code != 401 {
		t.Fatalf("detail without auth: expected 401, got %d", w.Code)
	}
}

func TestMonitorDetailWrongUser(t *testing.T) {
	setupTestDB(t)
	defer db.Close()

	token1 := registerAndLogin(t)
	dbID, _ := createDB(t, token1, "user1-db")

	body := `{"email":"user2@test.com","password":"secret123"}`
	req := httptest.NewRequest("POST", "/v1/auth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handleRegister(w, req)
	req = httptest.NewRequest("POST", "/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handleLogin(w, req)
	var lr LoginResponse
	json.NewDecoder(w.Body).Decode(&lr)

	req = httptest.NewRequest("GET", "/v1/monitor/db/"+dbID, nil)
	req.Header.Set("Authorization", "Bearer "+lr.Token)
	w = httptest.NewRecorder()
	handleMonitorDetail(w, req)
	if w.Code != 404 {
		t.Fatalf("detail as wrong user: expected 404, got %d", w.Code)
	}
}

// ── Helpers ─────────────────────────────────────────────────────────
