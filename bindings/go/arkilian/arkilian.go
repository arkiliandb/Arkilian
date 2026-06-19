// Package arkilian provides Go bindings for the Arkilian SQLite wrapper.
//
// The C library (src/class.c + src/deps/sqlite/sqlite3.c) is compiled
// via CGo and linked statically.  No external SQLite installation needed.
//
// Usage:
//
//	db, _ := arkilian.Open("my-token", "mydb.sqlite")
//	defer db.Close()
//	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
//	db.Exec("INSERT INTO t (val) VALUES ('hello')")
//
// In debug mode (ARKILIAN_DEBUG=true in env), the WAL push URL and
// server endpoints are set to localhost:8080 automatically.

package arkilian

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src -I${SRCDIR}/../../../src/deps/sqlite
#cgo darwin CFLAGS: -D_DARWIN_C_SOURCE
#cgo LDFLAGS: -lcurl -lpthread
#cgo darwin LDFLAGS: -framework CoreFoundation -framework Security
#include "class.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"os"
	"unsafe"
)

const (
	SQLITE_OK   = 0
	SQLITE_ROW  = 100
	SQLITE_DONE = 101
	SQLITE_BUSY = 5
)

// DB wraps an open Arkilian database connection.
type DB struct {
	ptr *C.arkilian
}

func init() {
	// Debug mode: set localhost endpoints automatically
	if os.Getenv("ARKILIAN_DEBUG") == "true" {
		os.Setenv("ARKILIAN_WAL_PUSH_URL", "http://localhost:8080/v1/wal/push")
	}
	// Disable hourly backup by default in Go binding
	if os.Getenv("ARKILIAN_ENABLE_BACKUP") == "" {
		os.Setenv("ARKILIAN_ENABLE_BACKUP", "0")
	}
}

// Open initializes an Arkilian database.  token is the API key for
// WAL push / hydration.  dbPath is the local SQLite file path.
func Open(token, dbPath string) (*DB, error) {
	cPath := C.CString(dbPath)
	defer C.free(unsafe.Pointer(cPath))

	var ptr *C.arkilian
	rc := C.db_init(&ptr, cPath)
	if rc != 0 {
		return nil, fmt.Errorf("arkilian: db_init failed (rc=%d)", rc)
	}

	db := &DB{ptr: ptr}
	if token != "" {
		db.SetToken(token)
	}
	return db, nil
}

// Close shuts down the database connection.
func (db *DB) Close() error {
	if db.ptr == nil {
		return nil
	}
	C.db_close(db.ptr)
	db.ptr = nil
	return nil
}

// SetToken updates the authentication token used for WAL push / backup.
func (db *DB) SetToken(token string) error {
	cToken := C.CString(token)
	defer C.free(unsafe.Pointer(cToken))
	rc := C.db_set_token(db.ptr, cToken)
	if rc != 0 {
		return fmt.Errorf("arkilian: set_token failed (rc=%d)", rc)
	}
	return nil
}

// Exec runs a SQL statement (INSERT, UPDATE, DELETE, DDL).  For SELECT
// use Query or Prepare/Step.
func (db *DB) Exec(sql string) error {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	rc := C.db_exec(db.ptr, cSQL)
	if rc != C.SQLITE_DONE && rc != C.SQLITE_OK && rc != C.SQLITE_ROW {
		return fmt.Errorf("arkilian: %s", C.GoString(C.db_errmsg(db.ptr)))
	}
	return nil
}

// ── Batch transactions ──────────────────────────────────────────────

// Begin starts a batch transaction.  All subsequent Exec calls share
// this transaction until Commit or Rollback is called.
func (db *DB) Begin() error {
	rc := C.db_begin(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_begin failed (rc=%d)", rc)
	}
	return nil
}

// Commit ends a batch transaction and flushes pending WAL entries.
func (db *DB) Commit() error {
	rc := C.db_commit(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_commit failed (rc=%d)", rc)
	}
	return nil
}

// Rollback aborts a batch transaction.
func (db *DB) Rollback() error {
	rc := C.db_rollback(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_rollback failed (rc=%d)", rc)
	}
	return nil
}

// ── Prepared statements ─────────────────────────────────────────────

// Stmt is a prepared statement.
type Stmt struct {
	db  *DB
	idx int
}

// Prepare compiles a SQL statement for repeated execution.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	rc := C.db_prepare(db.ptr, cSQL)
	if rc != C.SQLITE_OK {
		return nil, fmt.Errorf("arkilian: %s", C.GoString(C.db_errmsg(db.ptr)))
	}
	return &Stmt{db: db, idx: int(C.db_stmt_count(db.ptr)) - 1}, nil
}

// Step advances the prepared statement to the next row.
// Returns true if a row is available, false when done.
func (s *Stmt) Step() (bool, error) {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_step(s.db.ptr)
	if rc == C.SQLITE_ROW {
		return true, nil
	}
	if rc == C.SQLITE_DONE {
		return false, nil
	}
	return false, fmt.Errorf("arkilian: step failed (rc=%d)", int(rc))
}

// Finalize destroys the prepared statement.
func (s *Stmt) Finalize() error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_finalize(s.db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: finalize failed (rc=%d)", int(rc))
	}
	return nil
}

// Reset resets the statement for re-execution with new bindings.
func (s *Stmt) Reset() error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_reset(s.db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: reset failed (rc=%d)", int(rc))
	}
	return nil
}

// ── Column access ───────────────────────────────────────────────────

func (s *Stmt) ColumnCount() int {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return int(C.db_column_count(s.db.ptr))
}

func (s *Stmt) ColumnName(col int) string {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return C.GoString(C.db_column_name(s.db.ptr, C.int(col)))
}

func (s *Stmt) ColumnText(col int) string {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return C.GoString(C.db_column_text(s.db.ptr, C.int(col)))
}

func (s *Stmt) ColumnInt(col int) int {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return int(C.db_column_int(s.db.ptr, C.int(col)))
}

func (s *Stmt) ColumnInt64(col int) int64 {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return int64(C.db_column_int64(s.db.ptr, C.int(col)))
}

func (s *Stmt) ColumnDouble(col int) float64 {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return float64(C.db_column_double(s.db.ptr, C.int(col)))
}

// ── Binding ─────────────────────────────────────────────────────────

func (s *Stmt) BindText(idx int, val string) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	cVal := C.CString(val)
	defer C.free(unsafe.Pointer(cVal))
	rc := C.db_bind_text(s.db.ptr, C.int(idx), cVal)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_text failed")
	}
	return nil
}

func (s *Stmt) BindInt(idx, val int) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_int(s.db.ptr, C.int(idx), C.int(val))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_int failed")
	}
	return nil
}

func (s *Stmt) BindInt64(idx int, val int64) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_int64(s.db.ptr, C.int(idx), C.sqlite3_int64(val))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_int64 failed")
	}
	return nil
}

func (s *Stmt) BindDouble(idx int, val float64) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_double(s.db.ptr, C.int(idx), C.double(val))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_double failed")
	}
	return nil
}

func (s *Stmt) BindNull(idx int) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_null(s.db.ptr, C.int(idx))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_null failed")
	}
	return nil
}

// ── Query helpers ───────────────────────────────────────────────────

// QueryRow runs a SELECT and returns the first row as a map.
func (db *DB) QueryRow(sql string) (map[string]interface{}, error) {
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no rows")
	}

	return stmt.rowMap(), nil
}

// Query runs a SELECT and returns all rows as a slice of maps.
func (db *DB) Query(sql string) ([]map[string]interface{}, error) {
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	var rows []map[string]interface{}
	for {
		ok, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		rows = append(rows, stmt.rowMap())
	}
	return rows, nil
}

func (s *Stmt) rowMap() map[string]interface{} {
	n := s.ColumnCount()
	m := make(map[string]interface{}, n)
	for i := 0; i < n; i++ {
		m[s.ColumnName(i)] = s.ColumnText(i)
	}
	return m
}

// ── WAL status ──────────────────────────────────────────────────────

// WALPending returns the number of entries waiting in the double-buffer.
func (db *DB) WALPending() int {
	return int(C.db_wal_pending(db.ptr))
}

// LastInsertRowID returns the rowid of the last inserted row.
func (db *DB) LastInsertRowID() int64 {
	return int64(C.db_last_insert_rowid(db.ptr))
}

// Changes returns the number of rows modified by the last statement.
func (db *DB) Changes() int {
	return int(C.db_changes(db.ptr))
}

// Error returns the last error message.
func (db *DB) Error() string {
	return C.GoString(C.db_errmsg(db.ptr))
}
