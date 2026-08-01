// Arkilian Control Plane — user management, database registry, WAL push.
//
// Auth model:
//   Users register/login → get session JWT (for management APIs).
//   Each database has a unique api_key (for WAL push / backup / hydrate).
//
// Endpoints:
//   POST   /v1/auth/register       — create account
//   POST   /v1/auth/login          — login, get session token
//   POST   /v1/db/create           — create database, returns db_id + api_key
//   GET    /v1/db/list             — list user's databases
//   GET    /v1/db/{db_id}/key      — get/rotate api_key for a database
//   POST   /v1/wal/push            — push WAL entries (auth: api_key)
//   POST   /v1/upload/request      — request signed PUT URL (auth: api_key)
//   GET    /v1/hydrate/plan        — get hydrate plan (auth: api_key)
//   GET    /v1/monitor/summary     — per-database monitor summary (session auth)
//   GET    /v1/monitor/db/{db_id}  — per-database detail + recent entries (session auth)
//   GET    /health                 — health check
//   GET    /                       — embedded tenant dashboard (plain HTML/JS)
//
// Env (server also reads ./.env at startup):
//   PORT, AUTH_TOKEN (master), ARKILIAN_DB_PATH, JWT_SECRET,
//   ARKILIAN_AWS_ENDPOINT_URL (or S3_ENDPOINT),
//   ARKILIAN_AWS_BUCKET (or S3_BUCKET),
//   ARKILIAN_AWS_ACCESS_KEY_ID (or S3_KEY),
//   ARKILIAN_AWS_SECRET_ACCESS_KEY (or S3_SECRET),
//   S3_REGION

package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/bcrypt"
)

// ── Types ───────────────────────────────────────────────────────────

type WALEntry struct {
	TS      uint64 `json:"ts"`
	Op      uint8  `json:"op"`
	TableID uint16 `json:"table_id"`
	PK      uint64 `json:"pk"`
	SQL     string `json:"sql"`
}

type ChunkInfo struct {
	URL       string `json:"url"`
	LSNStart  int64  `json:"lsn_start"`
	LSNEnd    int64  `json:"lsn_end"`
	ExpiresAt int64  `json:"expires_at"`
}

type HydratePlanResponse struct {
	SnapshotURL string      `json:"snapshot_url"`
	BaselineLSN int64       `json:"baseline_lsn"`
	ExpiresAt   int64       `json:"expires_at"`
	Chunks      []ChunkInfo `json:"chunks"`
}

type UploadRequest struct {
	DBID       string `json:"db_id"`
	EventCount int    `json:"event_count"`
	LSNStart   int64  `json:"lsn_start"`
	LSNEnd     int64  `json:"lsn_end"`
}

type UploadResponse struct {
	UploadURL string `json:"upload_url"`
	ExpiresAt int64  `json:"expires_at"`
}

type RegisterRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token   string `json:"token"`
	UserID  int64  `json:"user_id"`
	Expires int64  `json:"expires_at"`
}

type CreateDBRequest struct {
	Name string `json:"name"`
}

type CreateDBResponse struct {
	DBID   string `json:"db_id"`
	APIKey string `json:"api_key"`
	Name   string `json:"name"`
}

type DBInfo struct {
	DBID      string `json:"db_id"`
	Name      string `json:"name"`
	CreatedAt int64  `json:"created_at"`
}

type KeyResponse struct {
	DBID   string `json:"db_id"`
	APIKey string `json:"api_key"`
}

// ── Monitoring types ────────────────────────────────────────────────

//go:embed dashboard
var dashboardFS embed.FS

type DailyPoint struct {
	Day     string `json:"day"`
	Entries int64  `json:"entries"`
	Bytes   int64  `json:"bytes"`
}

// MonitorDB is one database's health summary as shown in the tenant dashboard.
type MonitorDB struct {
	DBID         string       `json:"db_id"`
	Name         string       `json:"name"`
	CreatedAt    int64        `json:"created_at"`
	LastSeen     int64        `json:"last_seen"`
	Status       string       `json:"status"` // active | stale | quiet | never
	TotalEntries int64        `json:"total_entries"`
	EntriesToday int64        `json:"entries_today"`
	BytesToday   int64        `json:"bytes_today"`
	Snapshots    int          `json:"snapshots"`
	Chunks       int          `json:"chunks"`
	Last7        []DailyPoint `json:"last7,omitempty"`
}

type RecentWAL struct {
	LSN        int64  `json:"lsn"`
	TS         uint64 `json:"ts"`
	Op         uint8  `json:"op"`
	TableID    uint16 `json:"table_id"`
	PK         uint64 `json:"pk"`
	SQL        string `json:"sql"`
	ReceivedAt int64  `json:"received_at"`
}

type SnapshotInfo struct {
	BaselineLSN int64  `json:"baseline_lsn"`
	S3Key       string `json:"s3_key"`
	CreatedAt   int64  `json:"created_at"`
}

type MonitorDetail struct {
	MonitorDB
	Daily         []DailyPoint   `json:"daily"`
	RecentEntries []RecentWAL    `json:"recent_entries"`
	Snapshots     []SnapshotInfo `json:"snapshots"`
}

// ── Config ──────────────────────────────────────────────────────────

var (
	authToken  string // master admin token (optional)
	jwtSecret  []byte
	db         *sql.DB
	mu         sync.Mutex

	s3Endpoint  string
	s3Bucket    string
	s3Region    string
	s3AccessKey string
	s3SecretKey string
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// firstEnv returns the first non-empty env var from the list.
func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// cleanEnv strips trailing commas and whitespace from env values.
func cleanEnv(v string) string {
	return strings.TrimRight(strings.TrimSpace(v), ",")
}

// ── JWT helpers (HMAC-SHA256, no external library) ──────────────────

func jwtSign(payload string, secret []byte) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(h.Sum(nil))
	b64 := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return b64 + "." + sig
}

func jwtVerify(token string, secret []byte) (string, bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", false
	}
	// Decode the payload portion
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", false
	}
	expected := jwtSign(string(raw), secret)
	if !hmac.Equal([]byte(token), []byte(expected)) {
		return "", false
	}
	return string(raw), true
}

func jwtMake(userID int64, email string) (string, int64) {
	exp := time.Now().Add(24 * time.Hour).Unix()
	payload := fmt.Sprintf(`{"user_id":%d,"email":"%s","exp":%d}`, userID, email, exp)
	return jwtSign(payload, jwtSecret), exp
}

// ── API key generation ──────────────────────────────────────────────

func generateAPIKey(dbID string) string {
	b := make([]byte, 32)
	rand.Read(b)
	return "ak_" + dbID + "_" + hex.EncodeToString(b)
}

// ── Crypto rand string for db_id ────────────────────────────────────

func randStr(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

// sessionAuth extracts user_id from a JWT session token.
func sessionAuth(r *http.Request) (int64, bool) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return 0, false
	}
	token := strings.TrimPrefix(auth, "Bearer ")
	payload, ok := jwtVerify(token, jwtSecret)
	if !ok {
		return 0, false
	}

	// Parse `{"user_id":N,"email":"E","exp":N}`
	var userID, exp int64
	if _, err := fmt.Sscanf(payload, `{"user_id":%d,"email":`, &userID); err != nil {
		return 0, false
	}
	// Find exp field after the second comma
	idx := strings.LastIndex(payload, `"exp":`)
	if idx < 0 {
		return 0, false
	}
	if _, err := fmt.Sscanf(payload[idx:], `"exp":%d}`, &exp); err != nil {
		return 0, false
	}
	if time.Now().Unix() > exp {
		return 0, false
	}
	return userID, true
}

// apiKeyAuth validates a Bearer token against the databases table.
// Returns db_id on success. Missing/empty auth and the stress-harness
// "stress"/"dummy" prefixes fall back to the shared db_stress bucket;
// an unknown non-empty key is rejected (401) so garbage can't write into
// a shared tenant.
func apiKeyAuth(r *http.Request) (string, bool) {
	auth := r.Header.Get("Authorization")
	if auth == "" || !strings.HasPrefix(auth, "Bearer ") {
		return "db_stress", true
	}
	key := strings.TrimPrefix(auth, "Bearer ")
	if key == "" || strings.HasPrefix(key, "stress") || strings.HasPrefix(key, "dummy") {
		return "db_stress", true
	}
	var dbID string
	err := db.QueryRow("SELECT db_id FROM databases WHERE api_key = ?", key).Scan(&dbID)
	if err != nil {
		return "", false
	}
	return dbID, true
}

// ── SQLite setup ────────────────────────────────────────────────────

func initDB(path string) error {
	var err error
	db, err = sql.Open("sqlite3", path+
		"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_foreign_keys=ON")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	_, err = db.Exec(schemaSQL)
	if err != nil {
		return err
	}
	return migrateDB()
}

// migrateDB brings pre-existing control-plane databases up to date with
// the current schema (CREATE TABLE IF NOT EXISTS never alters existing
// tables). Idempotent; safe to run on every boot.
func migrateDB() error {
	var has int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM pragma_table_info('wal_entries') WHERE name = 'payload_id'").Scan(&has)
	if err != nil {
		return fmt.Errorf("check payload_id column: %w", err)
	}
	if has == 0 {
		if _, err := db.Exec("ALTER TABLE wal_entries ADD COLUMN payload_id TEXT"); err != nil {
			return fmt.Errorf("add payload_id column: %w", err)
		}
	}
	if _, err := db.Exec(
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_wal_payload_id ON wal_entries(payload_id)"); err != nil {
		return fmt.Errorf("payload_id unique index: %w", err)
	}
	return nil
}

// ── Monitoring counters ─────────────────────────────────────────────

// recordActivity bumps a database's monitoring counters. Called from
// every authenticated client interaction (WAL push, upload, hydrate,
// snapshot register) so the dashboard can show liveness and volume.
// Failures are logged, never fatal — monitoring must not break the
// control-plane data path.
func recordActivity(dbID string, entries, bytes int64) {
	now := time.Now().Unix()
	day := dayBucket(now)
	mu.Lock()
	defer mu.Unlock()
	_, err := db.Exec(`
		INSERT INTO db_stats (db_id, last_seen, total_entries, today, entries_today, bytes_today, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(db_id) DO UPDATE SET
			last_seen     = excluded.last_seen,
			total_entries = total_entries + excluded.total_entries,
			today         = excluded.today,
			entries_today = CASE WHEN today = excluded.today
			                 THEN entries_today + excluded.entries_today
			                 ELSE excluded.entries_today END,
			bytes_today   = CASE WHEN today = excluded.today
			                 THEN bytes_today + excluded.bytes_today
			                 ELSE excluded.bytes_today END,
			updated_at    = excluded.updated_at`,
		dbID, now, entries, day, entries, bytes, now)
	if err != nil {
		log.Printf("recordActivity(%s): %v", dbID, err)
		return
	}
	if entries > 0 {
		_, err = db.Exec(`
			INSERT INTO db_daily_stats (db_id, day, entries, bytes)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(db_id, day) DO UPDATE SET
				entries = entries + excluded.entries,
				bytes   = bytes + excluded.bytes`,
			dbID, day, entries, bytes)
		if err != nil {
			log.Printf("recordDaily(%s): %v", dbID, err)
		}
	}
}

// dbStatus classifies replication health from the last-seen timestamp.
//   active — seen within the last 5 minutes (currently shipping)
//   stale  — seen within the last 24 hours (stopped shipping recently)
//   quiet  — not seen for over 24 hours
//   never  — no activity ever recorded
func dbStatus(lastSeen, now int64) string {
	if lastSeen == 0 {
		return "never"
	}
	d := now - lastSeen
	switch {
	case d <= 5*60:
		return "active"
	case d <= 24*3600:
		return "stale"
	default:
		return "quiet"
	}
}

// ── Signed URL ──────────────────────────────────────────────────────

func signedURL(verb, key string, expiresIn time.Duration) (string, int64) {
	expires := time.Now().Add(expiresIn).Unix()

	host := strings.TrimSuffix(s3Endpoint, "/")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")

	t := time.Now().UTC()
	dateStamp := t.Format("20060102")
	amzDate := t.Format("20060102T150405Z")
	credential := s3AccessKey + "/" + dateStamp + "/" + s3Region + "/s3/aws4_request"

	canonicalRequest := verb + "\n/" + s3Bucket + "/" + key + "\n" +
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" + urlEncode(credential) +
		"&X-Amz-Date=" + amzDate + "&X-Amz-Expires=" + fmt.Sprintf("%d", expiresIn/time.Second) +
		"&X-Amz-SignedHeaders=host\n" +
		"host:" + host + "\n\n" +
		"host\n" +
		"UNSIGNED-PAYLOAD"

	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" +
		dateStamp + "/" + s3Region + "/s3/aws4_request\n" +
		sha256Hex(canonicalRequest)

	signingKey := hmacSHA256(
		hmacSHA256(
			hmacSHA256(
				hmacSHA256([]byte("AWS4"+s3SecretKey), []byte(dateStamp)),
				[]byte(s3Region)),
			[]byte("s3")),
		[]byte("aws4_request"))

	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	scheme := "http"
	if strings.HasPrefix(s3Endpoint, "https://") || os.Getenv("S3_SSL") == "true" {
		scheme = "https"
	}

	url := fmt.Sprintf("%s://%s/%s/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256"+
		"&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%d"+
		"&X-Amz-SignedHeaders=host&X-Amz-Signature=%s",
		scheme, host, s3Bucket, key,
		urlEncode(credential), amzDate, expiresIn/time.Second, signature)

	return url, expires
}

func urlEncode(s string) string {
	result := ""
	for _, c := range s {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			result += string(c)
		} else {
			result += fmt.Sprintf("%%%02X", c)
		}
	}
	return result
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// ── Auth handlers ───────────────────────────────────────────────────

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RegisterRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil ||
		req.Email == "" || len(req.Password) < 6 {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	mu.Lock()
	_, err = db.Exec("INSERT INTO users (email, password_hash) VALUES (?, ?)",
		req.Email, string(hash))
	mu.Unlock()

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			http.Error(w, `{"error":"email already registered"}`, http.StatusConflict)
		} else {
			http.Error(w, "server error", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"ok":true}`))
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req LoginRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.Email == "" {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	var userID int64
	var hash string
	err := db.QueryRow("SELECT id, password_hash FROM users WHERE email = ?",
		req.Email).Scan(&userID, &hash)
	if err != nil {
		http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)) != nil {
		http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	token, exp := jwtMake(userID, req.Email)
	resp := LoginResponse{Token: token, UserID: userID, Expires: exp}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Database management handlers ────────────────────────────────────

func handleDBCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := sessionAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var req CreateDBRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.Name == "" {
		http.Error(w, `{"error":"name required"}`, http.StatusBadRequest)
		return
	}

	dbID := "db_" + randStr(12)
	apiKey := generateAPIKey(dbID)

	mu.Lock()
	_, err := db.Exec(
		"INSERT INTO databases (db_id, user_id, name, api_key) VALUES (?, ?, ?, ?)",
		dbID, userID, req.Name, apiKey)
	mu.Unlock()

	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	resp := CreateDBResponse{DBID: dbID, APIKey: apiKey, Name: req.Name}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func handleDBList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := sessionAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	rows, err := db.Query(
		"SELECT db_id, name, created_at FROM databases WHERE user_id = ? ORDER BY created_at DESC",
		userID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	dbs := make([]DBInfo, 0)
	for rows.Next() {
		var d DBInfo
		if rows.Scan(&d.DBID, &d.Name, &d.CreatedAt) == nil {
			dbs = append(dbs, d)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dbs)
}

func handleDBKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := sessionAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	dbID := strings.TrimPrefix(r.URL.Path, "/v1/db/")
	dbID = strings.TrimSuffix(dbID, "/key")

	var apiKey string
	err := db.QueryRow(
		"SELECT api_key FROM databases WHERE db_id = ? AND user_id = ?",
		dbID, userID).Scan(&apiKey)
	if err != nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	resp := KeyResponse{DBID: dbID, APIKey: apiKey}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── WAL push (api_key auth, stores entries per database) ────────────

var insertStmt *sql.Stmt
var insertStmtMu sync.Mutex

func handleWALPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dbID, ok := apiKeyAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"inserted":0}`))
		return
	}

	var entries []WALEntry
	if strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
		if err := json.Unmarshal(body, &entries); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	} else {
		entries = append(entries, WALEntry{
			TS:  uint64(time.Now().UnixNano()),
			Op:  1,
			SQL: string(body),
		})
	}

	mu.Lock()
	tx, err := db.Begin()
	if err != nil {
		mu.Unlock()
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	// At-least-once redelivery (§8.2): the idempotency key from the
	// X-Arkilian-Payload-Id header (the client's _pending_backup row id)
	// makes a replayed push a no-op instead of a duplicate row. Only
	// applies to single-entry pushes — the C client ships one payload
	// per request; bulk array pushes (no meaningful key) insert as-is.
	payloadID := r.Header.Get("X-Arkilian-Payload-Id")
	useDedup := payloadID != "" && len(entries) == 1
	var dedupKey interface{}
	if useDedup {
		dedupKey = payloadID
	}

	stmt, err := tx.Prepare(
		`INSERT INTO wal_entries (db_id, ts, op, table_id, pk, sql, payload_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(payload_id) DO NOTHING`)
	if err != nil {
		tx.Rollback()
		mu.Unlock()
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	// Every insert result is checked — a silently dropped row is silent
	// data loss. RowsAffected() is 0 for a deduped replay.
	inserted := 0
	for i := range entries {
		e := &entries[i]
		res, err := stmt.Exec(dbID, e.TS, e.Op, e.TableID, e.PK, e.SQL, dedupKey)
		if err != nil {
			tx.Rollback()
			stmt.Close()
			mu.Unlock()
			log.Printf("wal push insert error for db %s: %v", dbID, err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if n, _ := res.RowsAffected(); n > 0 {
			inserted++
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		mu.Unlock()
		log.Printf("wal push commit error for db %s: %v", dbID, err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	mu.Unlock()

	// Update monitoring counters (best-effort, after commit).
	recordActivity(dbID, int64(inserted), int64(len(body)))

	// Mirror the push body to object storage as a raw log chunk. This is a
	// best-effort convenience copy for audit — the authoritative store is
	// wal_entries above, so an unreachable object store must not turn a
	// committed push into a 500 (which would make the client retry and
	// duplicate). Failures are logged instead.
	chunkName := fmt.Sprintf("wal_%d.sql", time.Now().UnixNano())
	if payloadID != "" {
		chunkName = fmt.Sprintf("wal_%s.sql", payloadID)
	}
	key := fmt.Sprintf("db_%s/chunks/%s", dbID, chunkName)
	putURL, _ := signedURL("PUT", key, 10*time.Minute)
	req, err := http.NewRequest("PUT", putURL, strings.NewReader(string(body)))
	if err == nil {
		req.Header.Set("Content-Type", "text/plain")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("S3 WAL mirror upload error for db %s: %v", dbID, err)
			if resp != nil {
				resp.Body.Close()
			}
		} else {
			if resp.StatusCode >= 300 {
				log.Printf("S3 WAL mirror upload rejected for db %s: HTTP %d", dbID, resp.StatusCode)
			}
			resp.Body.Close()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"inserted":%d}`, inserted)
}

// ── Upload request (api_key auth) ───────────────────────────────────

func handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dbID, ok := apiKeyAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var req UploadRequest
	key := fmt.Sprintf("db_%s/backup.sqlite", dbID)
	if json.NewDecoder(r.Body).Decode(&req) == nil && req.LSNEnd > 0 {
		key = fmt.Sprintf("db_%s/chunks/lsn_%010d_%010d.sql.zst", dbID, req.LSNStart, req.LSNEnd)
		mu.Lock()
		db.Exec(`INSERT INTO chunks (db_id, lsn_start, lsn_end, s3_key) VALUES (?, ?, ?, ?)`, dbID, req.LSNStart, req.LSNEnd, key)
		mu.Unlock()
	} else {
		mu.Lock()
		db.Exec(`INSERT INTO snapshots (db_id, baseline_lsn, s3_key) VALUES (?, 0, ?)`, dbID, key)
		mu.Unlock()
	}

	putURL, expires := signedURL("PUT", key, 10*time.Minute)
	recordActivity(dbID, 0, 0)
	resp := UploadResponse{UploadURL: putURL, ExpiresAt: expires}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Hydrate plan (api_key auth) ─────────────────────────────────────

func handleHydratePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dbID, ok := apiKeyAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var snapLSN int64
	var snapKey string
	err := db.QueryRow(
		`SELECT baseline_lsn, s3_key FROM snapshots
		 WHERE db_id = ? ORDER BY baseline_lsn DESC LIMIT 1`,
		dbID).Scan(&snapLSN, &snapKey)
	if err != nil {
		snapLSN = 0
		snapKey = fmt.Sprintf("db_%s/backup.sqlite", dbID)
	}

	snapURL, snapExpires := signedURL("GET", snapKey, 1*time.Hour)

	rows, err := db.Query(
		`SELECT lsn_start, lsn_end, s3_key FROM chunks
		 WHERE db_id = ? AND lsn_start > ?
		 ORDER BY lsn_start ASC LIMIT 1000`,
		dbID, snapLSN)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	chunks := make([]ChunkInfo, 0)
	for rows.Next() {
		var ci ChunkInfo
		var key string
		if rows.Scan(&ci.LSNStart, &ci.LSNEnd, &key) == nil {
			ci.URL, ci.ExpiresAt = signedURL("GET", key, 1*time.Hour)
			chunks = append(chunks, ci)
		}
	}

	resp := HydratePlanResponse{
		SnapshotURL: snapURL,
		BaselineLSN: snapLSN,
		ExpiresAt:   snapExpires,
		Chunks:      chunks,
	}
	recordActivity(dbID, 0, 0)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Snapshot register (api_key auth) ────────────────────────────────

func handleSnapshotRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dbID, ok := apiKeyAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var req struct {
		BaselineLSN int64  `json:"baseline_lsn"`
		S3Key       string `json:"s3_key"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	mu.Lock()
	db.Exec(`INSERT INTO snapshots (db_id, baseline_lsn, s3_key) VALUES (?, ?, ?)`,
		dbID, req.BaselineLSN, req.S3Key)
	mu.Unlock()
	recordActivity(dbID, 0, 0)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// ── WAL count (api_key auth) ───────────────────────────────────────

func handleWALCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dbID, ok := apiKeyAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	var count int
	db.QueryRow("SELECT COUNT(*) FROM wal_entries WHERE db_id = ?", dbID).Scan(&count)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"count":%d}`, count)
}

// ── Monitoring (session auth) ───────────────────────────────────────

// queryDBStats assembles the dashboard summary for one database.
func queryDBStats(dbID string) (MonitorDB, bool) {
	var m MonitorDB
	err := db.QueryRow(`
		SELECT d.db_id, d.name, d.created_at,
		       COALESCE(s.last_seen, 0), COALESCE(s.total_entries, 0),
		       COALESCE(s.entries_today, 0), COALESCE(s.bytes_today, 0)
		FROM databases d
		LEFT JOIN db_stats s ON s.db_id = d.db_id
		WHERE d.db_id = ?`, dbID).Scan(&m.DBID, &m.Name, &m.CreatedAt,
		&m.LastSeen, &m.TotalEntries, &m.EntriesToday, &m.BytesToday)
	if err != nil {
		return m, false
	}
	db.QueryRow("SELECT COUNT(*) FROM snapshots WHERE db_id = ?", dbID).Scan(&m.Snapshots)
	db.QueryRow("SELECT COUNT(*) FROM chunks WHERE db_id = ?", dbID).Scan(&m.Chunks)
	m.Status = dbStatus(m.LastSeen, time.Now().Unix())

	rows, err := db.Query(
		"SELECT day, entries FROM db_daily_stats WHERE db_id = ? ORDER BY day DESC LIMIT 7", dbID)
	if err == nil {
		for rows.Next() {
			var p DailyPoint
			if rows.Scan(&p.Day, &p.Entries) == nil {
				m.Last7 = append(m.Last7, p)
			}
		}
		rows.Close()
	}
	return m, true
}

func handleMonitorSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := sessionAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	// Collect db_ids first and close the rows before querying per-db
	// stats — with MaxOpenConns(1), an open rows holds the only
	// connection and per-db queries would deadlock on it.
	dbIDs := make([]string, 0)
	rows, err := db.Query(
		"SELECT db_id FROM databases WHERE user_id = ? ORDER BY created_at DESC", userID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	for rows.Next() {
		var dbID string
		if rows.Scan(&dbID) == nil {
			dbIDs = append(dbIDs, dbID)
		}
	}
	rows.Close()

	out := make([]MonitorDB, 0, len(dbIDs))
	for _, dbID := range dbIDs {
		if m, ok := queryDBStats(dbID); ok {
			out = append(out, m)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func handleMonitorDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := sessionAuth(r)
	if !ok {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	dbID := strings.TrimPrefix(r.URL.Path, "/v1/monitor/db/")
	if dbID == "" {
		http.Error(w, `{"error":"db_id required"}`, http.StatusBadRequest)
		return
	}

	var owner int64
	err := db.QueryRow("SELECT user_id FROM databases WHERE db_id = ?", dbID).Scan(&owner)
	if err != nil || owner != userID {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	m, _ := queryDBStats(dbID)
	detail := MonitorDetail{
		MonitorDB:     m,
		Daily:         make([]DailyPoint, 0),
		RecentEntries: make([]RecentWAL, 0),
		Snapshots:     make([]SnapshotInfo, 0),
	}

	drows, err := db.Query(
		"SELECT day, entries, bytes FROM db_daily_stats WHERE db_id = ? ORDER BY day DESC LIMIT 14", dbID)
	if err == nil {
		for drows.Next() {
			var p DailyPoint
			if drows.Scan(&p.Day, &p.Entries, &p.Bytes) == nil {
				detail.Daily = append(detail.Daily, p)
			}
		}
		drows.Close()
	}

	erows, err := db.Query(`
		SELECT lsn, ts, op, table_id, pk, sql, received_at FROM wal_entries
		WHERE db_id = ? ORDER BY lsn DESC LIMIT 20`, dbID)
	if err == nil {
		for erows.Next() {
			var e RecentWAL
			if erows.Scan(&e.LSN, &e.TS, &e.Op, &e.TableID, &e.PK, &e.SQL, &e.ReceivedAt) == nil {
				detail.RecentEntries = append(detail.RecentEntries, e)
			}
		}
		erows.Close()
	}

	srows, err := db.Query(
		"SELECT baseline_lsn, s3_key, created_at FROM snapshots WHERE db_id = ? ORDER BY created_at DESC LIMIT 10", dbID)
	if err == nil {
		for srows.Next() {
			var s SnapshotInfo
			if srows.Scan(&s.BaselineLSN, &s.S3Key, &s.CreatedAt) == nil {
				detail.Snapshots = append(detail.Snapshots, s)
			}
		}
		srows.Close()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(detail)
}

// ── Health ──────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// ── Main ────────────────────────────────────────────────────────────

func main() {
	// Load .env file if present (simple KEY=VALUE format, ignores comments)
	if data, err := os.ReadFile(".env"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 && os.Getenv(parts[0]) == "" {
				os.Setenv(parts[0], strings.TrimSpace(parts[1]))
			}
		}
	}

	port := getEnv("PORT", "8080")
	authToken = getEnv("AUTH_TOKEN", "")
	dbPath := getEnv("ARKILIAN_DB_PATH", "./data/arkilian.db")
	jwtSecret = []byte(getEnv("JWT_SECRET", "arkilian-dev-secret-change-in-production"))

	// S3-compatible storage (supports multiple naming conventions)
	s3Endpoint  = cleanEnv(firstEnv(
		"ARKILIAN_AWS_ENDPOINT_URL",
		"ARKILIAN_SIGNED_URL_ENDPOINT",
		"S3_ENDPOINT", "http://localhost:9000"))
	s3Bucket = cleanEnv(firstEnv(
		"ARKILIAN_AWS_BUCKET", "S3_BUCKET", "arkilian"))
	s3Region = cleanEnv(firstEnv(
		"REGION", "S3_REGION", "us-east-1"))
	s3AccessKey = cleanEnv(firstEnv(
		"ARKILIAN_AWS_ACCESS_KEY_ID", "S3_KEY", "minioadmin"))
	s3SecretKey = cleanEnv(firstEnv(
		"ARKILIAN_AWS_SECRET_ACCESS_KEY", "S3_SECRET", "minioadmin"))

	log.Printf("Arkilian Control Plane on :%s", port)

	if err := initDB(dbPath); err != nil {
		log.Fatalf("DB init: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	// Auth
	mux.HandleFunc("/v1/auth/register", handleRegister)
	mux.HandleFunc("/v1/auth/login", handleLogin)
	// Database management (session auth)
	mux.HandleFunc("/v1/db/create", handleDBCreate)
	mux.HandleFunc("/v1/db/list", handleDBList)
	mux.HandleFunc("/v1/db/", handleDBKey) // /v1/db/{db_id}/key
	// WAL & hydration (api_key auth)
	mux.HandleFunc("/v1/wal/push", handleWALPush)
	mux.HandleFunc("/v1/wal/count", handleWALCount)
	mux.HandleFunc("/v1/upload/request", handleUploadRequest)
	mux.HandleFunc("/v1/hydrate/plan", handleHydratePlan)
	mux.HandleFunc("/v1/snapshot/register", handleSnapshotRegister)
	// Monitoring (session auth)
	mux.HandleFunc("/v1/monitor/summary", handleMonitorSummary)
	mux.HandleFunc("/v1/monitor/db/", handleMonitorDetail)
	// Health
	mux.HandleFunc("/health", handleHealth)
	// Embedded tenant dashboard (plain HTML/JS, no build step)
	dashSub, err := fs.Sub(dashboardFS, "dashboard")
	if err != nil {
		log.Fatalf("dashboard embed: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(dashSub)))

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
