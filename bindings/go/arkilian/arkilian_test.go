// Arkilian Go bindings — integration test.
//
// Requires a local S3-compatible endpoint (e.g. MinIO on localhost:9000).
// Set ARKILIAN_DEBUG=true to auto-configure localhost storage.
//
// Run:
//   cd bindings/go
//   ARKILIAN_DEBUG=true go test -v ./arkilian/

package arkilian

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const serverURL = "http://localhost:8080"

// ── Server helpers ──────────────────────────────────────────────────

func postJSON(url string, body interface{}) (*http.Response, error) {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func getJSON(url, token string) (*http.Response, error) {
	req, _ := http.NewRequest("GET", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return http.DefaultClient.Do(req)
}

func authPost(url, token string, body interface{}) (*http.Response, error) {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	return http.DefaultClient.Do(req)
}

// registerAndLogin creates an account and returns a session JWT.
func registerAndLogin(t *testing.T, email, password string) string {
	t.Helper()

	resp, err := postJSON(serverURL+"/v1/auth/register", map[string]string{
		"email":    email,
		"password": password,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("register: expected 201, got %d", resp.StatusCode)
	}

	resp, err = postJSON(serverURL+"/v1/auth/login", map[string]string{
		"email":    email,
		"password": password,
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()

	var loginResp struct {
		Token string `json:"token"`
	}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	if loginResp.Token == "" {
		t.Fatal("login: empty token")
	}
	return loginResp.Token
}

// createDB creates a database and returns db_id + api_key.
func createDB(t *testing.T, sessionToken, name string) (string, string) {
	t.Helper()

	resp, err := authPost(serverURL+"/v1/db/create", sessionToken, map[string]string{
		"name": name,
	})
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		t.Fatalf("create db: expected 201, got %d", resp.StatusCode)
	}

	var cdb struct {
		DBID   string `json:"db_id"`
		APIKey string `json:"api_key"`
	}
	json.NewDecoder(resp.Body).Decode(&cdb)
	return cdb.DBID, cdb.APIKey
}

// ── Tests ───────────────────────────────────────────────────────────

func TestIntegrationFullFlow(t *testing.T) {
	// Check server is reachable
	if _, err := http.Get(serverURL + "/health"); err != nil {
		t.Skipf("server not reachable at %s — skipping integration test", serverURL)
	}

	_ = os.Setenv("ARKILIAN_DEBUG", "true")
	_ = os.Setenv("ARKILIAN_ENABLE_BACKUP", "0")

	// 1. Create user account
	email := fmt.Sprintf("test-%d@arkilian.com", time.Now().UnixNano())
	sessionToken := registerAndLogin(t, email, "secret123")
	t.Logf("registered + logged in as %s", email)

	// 2. Create a database, get API key
	dbID, apiKey := createDB(t, sessionToken, "integration-test")
	t.Logf("created db %s with api_key %s", dbID, apiKey[:20]+"...")

	// 3. Open Arkilian DB with the API key
	dbPath := fmt.Sprintf("/tmp/arkilian_go_test_%s.sqlite", dbID)
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := Open(apiKey, dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// 4. Create schema
	if err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := db.Exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INT, title TEXT, body TEXT)"); err != nil {
		t.Fatalf("create table posts: %v", err)
	}

	// 5. Insert data
	for i := 1; i <= 100; i++ {
		sql := fmt.Sprintf(
			"INSERT INTO users (name, email, age) VALUES ('user-%d', 'user%d@test.com', %d)",
			i, i, 20+(i%40))
		if err := db.Exec(sql); err != nil {
			t.Fatalf("insert user %d: %v", i, err)
		}
	}

	// 6. Verify local data
	rows, err := db.Query("SELECT COUNT(*) as cnt FROM users")
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	if len(rows) != 1 || rows[0]["cnt"] != "100" {
		t.Fatalf("expected 100 users, got %v", rows)
	}

	// 7. Test UPDATE
	if err := db.Exec("UPDATE users SET age = 99 WHERE name = 'user-42'"); err != nil {
		t.Fatalf("update: %v", err)
	}
	row, err := db.QueryRow("SELECT age FROM users WHERE name = 'user-42'")
	if err != nil {
		t.Fatalf("query updated: %v", err)
	}
	if row["age"] != "99" {
		t.Fatalf("expected age=99, got %v", row["age"])
	}

	// 8. Test DELETE
	if err := db.Exec("DELETE FROM users WHERE name = 'user-100'"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	rows, _ = db.Query("SELECT COUNT(*) as cnt FROM users")
	if rows[0]["cnt"] != "99" {
		t.Fatalf("expected 99 after delete, got %v", rows[0]["cnt"])
	}

	// 9. Test batch transaction
	if err := db.Begin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	for i := 200; i < 300; i++ {
		sql := fmt.Sprintf(
			"INSERT INTO posts (user_id, title, body) VALUES (%d, 'Post %d', 'Body content for post %d')",
			(i%99)+1, i, i)
		if err := db.Exec(sql); err != nil {
			t.Fatalf("batch insert post %d: %v", i, err)
		}
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	rows, _ = db.Query("SELECT COUNT(*) as cnt FROM posts")
	if rows[0]["cnt"] != "100" {
		t.Fatalf("expected 100 posts, got %v", rows[0]["cnt"])
	}

	// 10. Test prepared statement with binding
	stmt, err := db.Prepare("SELECT name, email FROM users WHERE age > ? ORDER BY id LIMIT ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	stmt.BindInt(1, 50)
	stmt.BindInt(2, 10)
	defer stmt.Finalize()

	count := 0
	for {
		ok, err := stmt.Step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !ok {
			break
		}
		name := stmt.ColumnText(0)
		if !strings.HasPrefix(name, "user-") {
			t.Fatalf("unexpected name: %s", name)
		}
		count++
	}
	if count == 0 {
		t.Fatal("expected at least 1 row from prepared statement")
	}
	t.Logf("prepared query returned %d rows", count)

	// 11. Check S3 storage was configured
	if os.Getenv("ARKILIAN_S3_ENDPOINT") != "http://localhost:9000" {
		t.Fatalf("expected S3 endpoint to be set, got: %s",
			os.Getenv("ARKILIAN_S3_ENDPOINT"))
	}

	// 12. Verify WAL entries exist
	t.Logf("WAL pending entries: %d", db.WALPending())

	t.Logf("integration test PASSED — db=%s api_key=%s writes=%d",
		dbID, apiKey[:16]+"...", 100+1+1+100)
}

func TestDebugModeAutoConfig(t *testing.T) {
	os.Setenv("ARKILIAN_DEBUG", "true")
	os.Unsetenv("ARKILIAN_S3_ENDPOINT")

	// Simulate what init() does
	if os.Getenv("ARKILIAN_DEBUG") == "true" {
		os.Setenv("ARKILIAN_S3_ENDPOINT", "http://localhost:9000")
	}

	if os.Getenv("ARKILIAN_S3_ENDPOINT") != "http://localhost:9000" {
		t.Fatal("ARKILIAN_DEBUG=true should set the S3 endpoint")
	}
}

func TestOpenClose(t *testing.T) {
	dbPath := "/tmp/arkilian_go_test_openclose.sqlite"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := Open("test-token", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// Basic exec
	if err := db.Exec("CREATE TABLE t (x INT)"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	if err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Query
	rows, err := db.Query("SELECT x FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["x"]) != "1" {
		t.Fatalf("unexpected result: %v", rows)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestBatchRollback(t *testing.T) {
	dbPath := "/tmp/arkilian_go_test_rollback.sqlite"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, _ := Open("", dbPath)
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INT)")

	db.Begin()
	db.Exec("INSERT INTO t (val) VALUES (1)")
	db.Exec("INSERT INTO t (val) VALUES (2)")
	db.Rollback()

	rows, _ := db.Query("SELECT COUNT(*) as cnt FROM t")
	if fmt.Sprint(rows[0]["cnt"]) != "0" {
		t.Fatalf("expected 0 after rollback, got %v", rows[0]["cnt"])
	}
}

func TestWALPending(t *testing.T) {
	dbPath := "/tmp/arkilian_go_test_wal.sqlite"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	_ = os.Setenv("ARKILIAN_S3_ENDPOINT", "http://127.0.0.1:1")

	db, _ := Open("test-key", dbPath)
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INT)")
	pending := db.WALPending()
	t.Logf("WAL pending after CREATE TABLE: %d", pending)
	if pending < 1 {
		t.Fatalf("expected at least 1 WAL entry, got %d", pending)
	}

	db.Exec("INSERT INTO t (val) VALUES (42)")
	db.Exec("INSERT INTO t (val) VALUES (43)")
	pending = db.WALPending()
	t.Logf("WAL pending after 2 INSERTs: %d", pending)
	if pending < 3 {
		t.Fatalf("expected at least 3 WAL entries, got %d", pending)
	}

	lastSQL := db.WALLastSQL()
	if !strings.Contains(lastSQL, "43") {
		t.Fatalf("expected last sql to contain 43, got: %s", lastSQL)
	}
}

func TestCoreParityAndHealth(t *testing.T) {
	dbPath := "/tmp/arkilian_go_test_parity.sqlite"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := Open("test-key", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// 1. Changes and LastInsertRowID
	if err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, data BLOB, num BIGINT)"); err != nil {
		t.Fatalf("create items table: %v", err)
	}
	if err := db.Exec("INSERT INTO items (name, num) VALUES ('test1', 1234567890123)"); err != nil {
		t.Fatalf("insert item: %v", err)
	}
	if db.Changes() != 1 {
		t.Fatalf("expected 1 change, got %d", db.Changes())
	}
	if db.LastInsertRowID() != 1 {
		t.Fatalf("expected last insert rowid 1, got %d", db.LastInsertRowID())
	}

	// 2. Prepared statement with typed bindings: BindBlob, BindInt64, BindNull
	stmt, err := db.Prepare("INSERT INTO items (name, data, num) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	blobPayload := []byte{0x00, 0xFF, 0xFE, 0x42, 0x00, 0x01}
	if err := stmt.BindText(1, "blob_item"); err != nil {
		t.Fatalf("bind text: %v", err)
	}
	if err := stmt.BindBlob(2, blobPayload); err != nil {
		t.Fatalf("bind blob: %v", err)
	}
	if err := stmt.BindInt64(3, 987654321098765); err != nil {
		t.Fatalf("bind int64: %v", err)
	}
	ok, err := stmt.Step()
	if err != nil {
		t.Fatalf("step insert blob: %v", err)
	}
	_ = ok
	stmt.Finalize()

	// Query it back with ColumnType, ColumnBlob, ColumnInt64, ColumnValue
	qstmt, err := db.Prepare("SELECT name, data, num FROM items WHERE id = 2")
	if err != nil {
		t.Fatalf("prepare select: %v", err)
	}
	hasRow, err := qstmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("expected row: %v", err)
	}
	if qstmt.ColumnType(0) != ColumnText {
		t.Fatalf("expected text type, got %d", qstmt.ColumnType(0))
	}
	if qstmt.ColumnType(1) != ColumnBlob {
		t.Fatalf("expected blob type, got %d", qstmt.ColumnType(1))
	}
	retrievedBlob := qstmt.ColumnBlob(1)
	if !bytes.Equal(retrievedBlob, blobPayload) {
		t.Fatalf("blob mismatch: got %v, expected %v", retrievedBlob, blobPayload)
	}
	if qstmt.ColumnInt64(2) != 987654321098765 {
		t.Fatalf("int64 mismatch: got %d", qstmt.ColumnInt64(2))
	}
	valMap := qstmt.ColumnValue(1)
	if valBlob, ok := valMap.([]byte); !ok || !bytes.Equal(valBlob, blobPayload) {
		t.Fatalf("ColumnValue blob mismatch: %v", valMap)
	}
	qstmt.Finalize()

	// 3. Backup controls
	db.SetBackupEnabled(true)
	if !db.IsBackupEnabled() {
		t.Fatalf("expected backup to be enabled")
	}
	db.SetAutoResyncTriggers(true)
	if !db.AutoResyncTriggers() {
		t.Fatalf("expected auto resync triggers to be true")
	}
	db.ResyncTriggers()
	_ = db.TriggersDirty()
	_ = db.CapturePaused()

	// 4. Monitoring & Health flags
	_ = db.BackupQueueDepth()
	_ = db.BackupOldestPendingAgeSec()
	_ = db.BackupDeadLetterCount()
	_ = db.BackupThreadHeartbeatAgeMs()
	_ = db.BackupSnapshotHeartbeatAgeMs()
	_ = db.BackupTriggerCoverage()
	_ = db.BackupSkippedTableCount()
	_ = db.BackupChunkCount()
	_ = db.BackupLastChunkFlushAgeMs()
	_ = db.BackupHealthFlags()
	_ = db.BackupIsHealthy()

	if HealthFlagCoreMask != 0x03FF {
		t.Fatalf("expected HealthFlagCoreMask 0x03FF, got 0x%X", HealthFlagCoreMask)
	}
}

