// Arkilian Control Plane API — issues signed URLs, manages metadata.
// The server never touches S3 directly — it delegates heavy transfers
// to the client via Pre-Signed URLs.
//
// Endpoints:
//   GET    /v1/hydrate/plan    — return snapshot URL + incremental chunk URLs
//   POST   /v1/upload/request  — request a signed PUT URL for a chunk
//   GET    /health             — health check
//
// Env:
//   PORT              HTTP listen port (default 8080)
//   AUTH_TOKEN        Bearer token for authentication
//   ARKILIAN_DB_PATH  SQLite metadata database (default /data/arkilian.db)
//   S3_ENDPOINT       S3-compatible endpoint (default http://localhost:9000)
//   S3_BUCKET         Bucket name (default arkilian)
//   S3_REGION         Region (default us-east-1)
//   S3_KEY            Access key
//   S3_SECRET         Secret key

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ── Types ───────────────────────────────────────────────────────────

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
	Token      string `json:"token"`
	DBID       string `json:"db_id"`
	EventCount int    `json:"event_count"`
	LSNStart   int64  `json:"lsn_start"`
	LSNEnd     int64  `json:"lsn_end"`
}

type UploadResponse struct {
	UploadURL string `json:"upload_url"`
	ExpiresAt int64  `json:"expires_at"`
}

// ── Configuration ───────────────────────────────────────────────────

var (
	authToken string
	db        *sql.DB
	mu        sync.Mutex

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

func checkAuth(r *http.Request) bool {
	if authToken == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == authToken
}

// ── SQLite metadata store ───────────────────────────────────────────

func initDB(path string) error {
	var err error
	db, err = sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			baseline_lsn INTEGER NOT NULL,
			s3_key      TEXT NOT NULL,
			created_at  INTEGER NOT NULL DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS chunks (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			lsn_start   INTEGER NOT NULL,
			lsn_end     INTEGER NOT NULL,
			s3_key      TEXT NOT NULL,
			created_at  INTEGER NOT NULL DEFAULT (unixepoch())
		);
		CREATE TABLE IF NOT EXISTS db_registry (
			db_id       TEXT PRIMARY KEY,
			created_at  INTEGER NOT NULL DEFAULT (unixepoch())
		);
	`)
	return err
}

// ── Signed URL generator (S3-compatible) ────────────────────────────

func signedURL(verb, key string, expiresIn time.Duration) (string, int64) {
	expires := time.Now().Add(expiresIn).Unix()

	host := s3Endpoint
	if !strings.HasPrefix(host, "http") {
		host = "https://" + host
	}
	host = strings.TrimSuffix(host, "/")

	url := fmt.Sprintf("%s/%s/%s", host, s3Bucket, key)
	// In production replace with real AWS Signature V4.
	// For now we return a plain URL with an X-Amz-* style query suffix
	// that the caller handles server-side or via a real SDK.
	url += fmt.Sprintf("?X-Amz-Algorithm=AWS4-HMAC-SHA256"+
		"&X-Amz-Credential=%s/%%2F%s/%%2Fs3/%%2Faws4_request"+
		"&X-Amz-Date=%s"+
		"&X-Amz-Expires=%d"+
		"&X-Amz-SignedHeaders=host",
		s3AccessKey, time.Now().UTC().Format("20060102"),
		time.Now().UTC().Format("20060102T150405Z"),
		expiresIn/time.Second)
	// Note: a real deployment must compute the Signature parameter using
	// HMAC-SHA256.  The above URL is structurally correct but will 403
	// without a valid signature.  Use a library or implement HMAC.
	return url, expires
}

// ── Hydrate plan endpoint ───────────────────────────────────────────

func handleHydratePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	// Find the latest snapshot
	var snapLSN int64
	var snapKey string
	if err := db.QueryRow(
		"SELECT baseline_lsn, s3_key FROM snapshots ORDER BY baseline_lsn DESC LIMIT 1",
	).Scan(&snapLSN, &snapKey); err != nil {
		http.Error(w, "no snapshot available", http.StatusNotFound)
		return
	}

	snapURL, snapExpires := signedURL("GET", snapKey, 1*time.Hour)

	// Find all chunks with LSN > snapshot baseline
	rows, err := db.Query(
		`SELECT lsn_start, lsn_end, s3_key
		 FROM chunks WHERE lsn_start > ?
		 ORDER BY lsn_start ASC LIMIT 1000`, snapLSN)
	if err != nil {
		log.Printf("ERROR query chunks: %v", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	chunks := make([]ChunkInfo, 0)
	for rows.Next() {
		var ci ChunkInfo
		var key string
		if err := rows.Scan(&ci.LSNStart, &ci.LSNEnd, &key); err != nil {
			continue
		}
		ci.URL, ci.ExpiresAt = signedURL("GET", key, 1*time.Hour)
		chunks = append(chunks, ci)
	}

	resp := HydratePlanResponse{
		SnapshotURL: snapURL,
		BaselineLSN: snapLSN,
		ExpiresAt:   snapExpires,
		Chunks:      chunks,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Upload request endpoint ─────────────────────────────────────────

func handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req UploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	// Register DB if new
	db.Exec("INSERT OR IGNORE INTO db_registry (db_id) VALUES (?)", req.DBID)

	// Generate S3 key for this chunk
	key := fmt.Sprintf("db_%s/chunks/lsn_%010d_%010d.sql.zst",
		req.DBID, req.LSNStart, req.LSNEnd)

	// Record chunk metadata
	db.Exec(
		`INSERT INTO chunks (lsn_start, lsn_end, s3_key)
		 VALUES (?, ?, ?)`,
		req.LSNStart, req.LSNEnd, key)

	// Generate signed PUT URL (10 minute expiry)
	putURL, expires := signedURL("PUT", key, 10*time.Minute)

	resp := UploadResponse{
		UploadURL: putURL,
		ExpiresAt: expires,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Admin: register a snapshot ──────────────────────────────────────

func handleSnapshotRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		BaselineLSN int64  `json:"baseline_lsn"`
		S3Key       string `json:"s3_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	db.Exec("INSERT INTO snapshots (baseline_lsn, s3_key) VALUES (?, ?)",
		req.BaselineLSN, req.S3Key)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// ── Health ──────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// ── Main ────────────────────────────────────────────────────────────

func main() {
	port := getEnv("PORT", "8080")
	authToken = getEnv("AUTH_TOKEN", "")
	dbPath := getEnv("ARKILIAN_DB_PATH", "/data/arkilian.db")
	s3Endpoint = getEnv("S3_ENDPOINT", "http://localhost:9000")
	s3Bucket = getEnv("S3_BUCKET", "arkilian")
	s3Region = getEnv("S3_REGION", "us-east-1")
	s3AccessKey = getEnv("S3_KEY", "minioadmin")
	s3SecretKey = getEnv("S3_SECRET", "minioadmin")

	log.Printf("Arkilian Control Plane on :%s", port)

	if err := initDB(dbPath); err != nil {
		log.Fatalf("DB init: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/hydrate/plan",       handleHydratePlan)
	mux.HandleFunc("/v1/upload/request",     handleUploadRequest)
	mux.HandleFunc("/v1/snapshot/register",  handleSnapshotRegister)
	mux.HandleFunc("/health",                handleHealth)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
