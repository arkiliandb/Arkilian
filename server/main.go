// Arkilian WAL server — receives WAL pushes from Arkilian clients.
// Deployed on Fly.io, single-instance SQLite.
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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// WALEntry is a single write-ahead log entry received from a client.
type WALEntry struct {
	TS      uint64 `json:"ts"`
	Op      uint8  `json:"op"`
	TableID uint16 `json:"table_id"`
	PK      uint64 `json:"pk"`
	SQL     string `json:"sql"`
}

// ── Configuration ────────────────────────────────────────────────────

var (
	authToken string
	db        *sql.DB
	insertStmt *sql.Stmt
	stmtMu     sync.Mutex
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── SQLite setup ─────────────────────────────────────────────────────

func initDB(path string) error {
	var err error
	db, err = sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_foreign_keys=ON")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	// Performance tuning
	db.SetMaxOpenConns(1) // SQLite is single-writer
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Create schema
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

	// Prepare insert statement once
	insertStmt, err = db.Prepare(
		`INSERT INTO wal_entries (ts, op, table_id, pk, sql)
		 VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}

	return nil
}

// ── WAL push handler ─────────────────────────────────────────────────

func handleWALPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Auth
	if authToken != "" {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Read body with limit
	r.Body = http.MaxBytesReader(w, r.Body, 32<<20) // 32MB max
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	defer r.Body.Close()

	// Parse JSON
	var entries []WALEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if len(entries) == 0 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true,"inserted":0}`))
		return
	}

	// Insert in a single transaction for speed (max 100k entries)
	stmtMu.Lock()
	defer stmtMu.Unlock()

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
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true,"inserted":%d}`, len(entries))
}

// ── Health check ──────────────────────────────────────────────────────

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
	mux.HandleFunc("/v1/wal/push", handleWALPush)
	mux.HandleFunc("/health", handleHealth)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
