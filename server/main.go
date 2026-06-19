// Arkilian WAL server — receives WAL pushes, serves snapshots + WAL frames.
// Deployed on Fly.io, single-instance SQLite.
//
// Endpoints:
//   POST   /v1/wal/push          — receive WAL entries from clients
//   GET    /v1/snapshot/latest   — download latest compacted snapshot
//   GET    /v1/wal/frames?after= — download WAL entries after LSN (JSON)
//   POST   /v1/admin/compact     — trigger checkpoint + snapshot build
//   GET    /health               — health check
//
// Build:  CGO_ENABLED=1 go build -o arkilian-server .
// Run:    ./arkilian-server
//
// Env:
//   PORT              HTTP listen port (default 8080)
//   ARKILIAN_DB_PATH  SQLite database path (default /data/arkilian.db)
//   AUTH_TOKEN        Bearer token for authentication (required)

package main

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// WALEntry is a single write-ahead log entry.
type WALEntry struct {
	TS      uint64 `json:"ts"`
	Op      uint8  `json:"op"`
	TableID uint16 `json:"table_id"`
	PK      uint64 `json:"pk"`
	SQL     string `json:"sql"`
}

// ── Configuration ────────────────────────────────────────────────────

var (
	authToken  string
	db         *sql.DB
	dbPath     string
	insertStmt *sql.Stmt
	snapshotMu sync.Mutex
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── Auth ─────────────────────────────────────────────────────────────

func checkAuth(r *http.Request) bool {
	if authToken == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == authToken
}

// ── SQLite setup ─────────────────────────────────────────────────────

func initDB(path string) error {
	var err error
	dbPath = path
	db, err = sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_foreign_keys=ON")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS wal_entries (
			lsn         INTEGER PRIMARY KEY AUTOINCREMENT,
			ts          INTEGER NOT NULL,
			op          INTEGER NOT NULL,
			table_id    INTEGER NOT NULL,
			pk          INTEGER NOT NULL,
			sql         TEXT,
			received_at INTEGER NOT NULL DEFAULT (unixepoch())
		);
		CREATE INDEX IF NOT EXISTS idx_wal_ts ON wal_entries(ts);
		CREATE INDEX IF NOT EXISTS idx_wal_table ON wal_entries(table_id);
	`)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	insertStmt, err = db.Prepare(
		`INSERT INTO wal_entries (ts, op, table_id, pk, sql) VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}

	return nil
}

// ── WAL push (unchanged) ─────────────────────────────────────────────

func handleWALPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	defer r.Body.Close()

	var entries []WALEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if len(entries) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"inserted":0}`))
		return
	}

	snapshotMu.Lock()
	defer snapshotMu.Unlock()

	tx, err := db.Begin()
	if err != nil {
		log.Printf("ERROR begin tx: %v", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	for i := range entries {
		e := &entries[i]
		if _, err := tx.Stmt(insertStmt).Exec(e.TS, e.Op, e.TableID, e.PK, e.SQL); err != nil {
			log.Printf("ERROR insert wal: %v", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("ERROR commit: %v", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"inserted":%d}`, len(entries))
}

// ── Snapshot serving ─────────────────────────────────────────────────

func snapshotPath() string {
	return filepath.Join(filepath.Dir(dbPath), "arkilian_snapshot.db")
}

func handleSnapshotLatest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	sp := snapshotPath()
	data, err := os.ReadFile(sp)
	if err != nil {
		http.Error(w, "no snapshot available", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)
}

// ── WAL frames serving ───────────────────────────────────────────────

func handleWALFrames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	afterStr := r.URL.Query().Get("after")
	afterLSN, _ := strconv.Atoi(afterStr)
	if afterLSN < 0 {
		afterLSN = 0
	}

	rows, err := db.Query(
		`SELECT lsn, ts, op, table_id, pk, sql
		 FROM wal_entries WHERE lsn > ?
		 ORDER BY lsn ASC LIMIT 100000`, afterLSN)
	if err != nil {
		log.Printf("ERROR query wal: %v", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	entries := make([]WALEntry, 0, 1024)
	for rows.Next() {
		var e WALEntry
		var lsn int64
		if err := rows.Scan(&lsn, &e.TS, &e.Op, &e.TableID, &e.PK, &e.SQL); err != nil {
			log.Printf("ERROR scan wal: %v", err)
			continue
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		log.Printf("ERROR rows wal: %v", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// ── Compaction ───────────────────────────────────────────────────────

func handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	snapshotMu.Lock()
	defer snapshotMu.Unlock()

	// 1. Checkpoint WAL (truncate to keep the file small)
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Printf("ERROR checkpoint: %v", err)
		http.Error(w, "checkpoint failed", http.StatusInternalServerError)
		return
	}

	// 2. Get the current max LSN
	var maxLSN uint32
	if err := db.QueryRow("SELECT COALESCE(MAX(lsn), 0) FROM wal_entries").Scan(&maxLSN); err != nil {
		maxLSN = 0
	}

	// 3. Read the current .db file
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		log.Printf("ERROR read db: %v", err)
		http.Error(w, "read db failed", http.StatusInternalServerError)
		return
	}

	// 4. Build snapshot: 4-byte LSN prefix + raw db
	sp := snapshotPath()
	f, err := os.Create(sp)
	if err != nil {
		log.Printf("ERROR create snapshot: %v", err)
		http.Error(w, "create snapshot failed", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	// Write LSN (4 bytes, little-endian)
	var lsnBuf [4]byte
	binary.LittleEndian.PutUint32(lsnBuf[:], maxLSN)
	f.Write(lsnBuf[:])

	// Write database bytes
	f.Write(dbData)

	log.Printf("Compaction complete: snapshot at LSN %d, size %d bytes", maxLSN, 4+len(dbData))

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"snapshot_lsn":%d,"snapshot_bytes":%d}`, maxLSN, 4+len(dbData))
}

// ── Health ────────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// ── Main ─────────────────────────────────────────────────────────────

func main() {
	port := getEnv("PORT", "8080")
	authToken = getEnv("AUTH_TOKEN", "")
	dbPath := getEnv("ARKILIAN_DB_PATH", "/data/arkilian.db")

	log.Printf("Arkilian WAL server starting on :%s", port)
	log.Printf("DB path: %s", dbPath)

	if err := initDB(dbPath); err != nil {
		log.Fatalf("DB init failed: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/wal/push",         handleWALPush)
	mux.HandleFunc("/v1/snapshot/latest",  handleSnapshotLatest)
	mux.HandleFunc("/v1/wal/frames",       handleWALFrames)
	mux.HandleFunc("/v1/admin/compact",    handleCompact)
	mux.HandleFunc("/health",              handleHealth)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 120 * time.Second, // large snapshots need time
		IdleTimeout:  60 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
