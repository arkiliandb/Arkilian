// Package arkilian provides Go bindings for the Arkilian SQLite wrapper.
//
// The C library is compiled via CGo and linked statically.
// No external SQLite installation needed.
//
// Storage is S3-compatible and configured via ARKILIAN_S3_* environment
// variables (or a ./.env file).
package arkilian

/*
#cgo CFLAGS: -I${SRCDIR} -I${SRCDIR}/../../../src -I${SRCDIR}/../../../src/deps/sqlite
#cgo darwin CFLAGS: -D_DARWIN_C_SOURCE
#cgo LDFLAGS: -lcurl -lpthread
#cgo darwin LDFLAGS: -framework CoreFoundation -framework Security
#include "class.h"
#include "hydration.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"os"
	"unsafe"
)

// SQLite result codes
const (
	SQLITE_OK    = 0
	SQLITE_ERROR = 1
	SQLITE_BUSY  = 5
	SQLITE_ROW   = 100
	SQLITE_DONE  = 101
)

// SQLite column data types
const (
	ColumnInteger = 1
	ColumnFloat   = 2
	ColumnText    = 3
	ColumnBlob    = 4
	ColumnNull    = 5
)

// Health state machine flags (ARK_HF_*)
const (
	HealthFlagBackupEnabled    uint32 = 1 << 0
	HealthFlagDestConfigured   uint32 = 1 << 1
	HealthFlagFlushAlive       uint32 = 1 << 2
	HealthFlagSnapshotAlive    uint32 = 1 << 3
	HealthFlagQueueBelowCap    uint32 = 1 << 4
	HealthFlagSchemaInSync     uint32 = 1 << 5
	HealthFlagNoDeadLetter     uint32 = 1 << 6
	HealthFlagManifestResolved uint32 = 1 << 7
	HealthFlagNoCaptureGap     uint32 = 1 << 8
	HealthFlagDurableCapture   uint32 = 1 << 9
	HealthFlagAllCore          uint32 = 0x3ff
	HealthFlagCoreMask         uint32 = HealthFlagAllCore
)

// Hydration error codes
const (
	HydrationOK          = 0
	HydrationErrNet      = -1
	HydrationErrDisk     = -2
	HydrationErrMem      = -3
	HydrationErrProto    = -4
	HydrationErrSQL      = -5
	HydrationErrDecomp   = -6
	HydrationErrExpired  = -7
	HydrationErrNotFound = -8
	HydrationErrNewer    = -9
	HydrationErrBusy     = -10
)

// S3Config holds connection parameters for cold-start S3 hydration.
type S3Config struct {
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	Prefix    string
}

// HydrateS3 performs a cold-start recovery of a database from S3-compatible storage.
// Must be called from a cold process before opening the database with Open/OpenDB.
func HydrateS3(dbPath string, cfg S3Config) error {
	cPath := C.CString(dbPath)
	defer C.free(unsafe.Pointer(cPath))

	cEndpoint := C.CString(cfg.Endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	cBucket := C.CString(cfg.Bucket)
	defer C.free(unsafe.Pointer(cBucket))

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	cRegion := C.CString(region)
	defer C.free(unsafe.Pointer(cRegion))

	cAccessKey := C.CString(cfg.AccessKey)
	defer C.free(unsafe.Pointer(cAccessKey))

	cSecretKey := C.CString(cfg.SecretKey)
	defer C.free(unsafe.Pointer(cSecretKey))

	cPrefix := C.CString(cfg.Prefix)
	defer C.free(unsafe.Pointer(cPrefix))

	rc := C.arkilian_hydrate_s3(cPath, cEndpoint, cBucket, cRegion, cAccessKey, cSecretKey, cPrefix, nil, nil)
	if rc != C.HYDRATION_OK {
		return fmt.Errorf("arkilian: hydration failed with code %d", int(rc))
	}
	return nil
}

// DB wraps an open Arkilian database connection.
type DB struct {
	ptr *C.arkilian
}

func init() {
	if os.Getenv("ARKILIAN_DEBUG") == "true" {
		os.Setenv("ARKILIAN_S3_ENDPOINT", "http://localhost:9000")
		os.Setenv("ARKILIAN_S3_BUCKET", "arkilian-backups")
		os.Setenv("ARKILIAN_S3_REGION", "us-east-1")
		os.Setenv("ARKILIAN_S3_ACCESS_KEY", "minioadmin")
		os.Setenv("ARKILIAN_S3_SECRET_KEY", "minioadmin")
		os.Setenv("ARKILIAN_S3_PREFIX", "db_default")
	}
	if os.Getenv("ARKILIAN_ENABLE_BACKUP") == "" {
		os.Setenv("ARKILIAN_ENABLE_BACKUP", "0")
	}
}

// Open initializes an Arkilian database.
func Open(token, dbPath string) (*DB, error) {
	db, err := OpenDB(dbPath)
	if err != nil {
		return nil, err
	}
	if token != "" {
		_ = db.SetToken(token)
	}
	return db, nil
}

// OpenDB initializes an Arkilian database given the database path.
func OpenDB(dbPath string) (*DB, error) {
	cPath := C.CString(dbPath)
	defer C.free(unsafe.Pointer(cPath))

	var ptr *C.arkilian
	rc := C.db_init(&ptr, cPath)
	if rc != 0 {
		err := "initialization failed"
		if ptr != nil {
			err = C.GoString(C.db_errmsg(ptr))
			C.db_close(ptr)
		}
		return nil, fmt.Errorf("arkilian: db_init failed (rc=%d): %s", rc, err)
	}
	if ptr == nil {
		return nil, fmt.Errorf("arkilian: db handle is null")
	}

	return &DB{ptr: ptr}, nil
}

// Close shuts down the database connection and stops background threads.
func (db *DB) Close() error {
	if db.ptr == nil {
		return nil
	}
	C.db_close(db.ptr)
	db.ptr = nil
	return nil
}

// SetToken is a backward-compatible no-op for older callers.
func (db *DB) SetToken(token string) error {
	_ = token
	return nil
}

// Exec runs a SQL statement (INSERT, UPDATE, DELETE, DDL).
func (db *DB) Exec(sql string) error {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	rc := C.db_exec(db.ptr, cSQL)
	if rc != C.SQLITE_DONE && rc != C.SQLITE_OK && rc != C.SQLITE_ROW {
		return fmt.Errorf("arkilian: %s", C.GoString(C.db_errmsg(db.ptr)))
	}
	return nil
}

// Changes returns the number of database rows that were changed or inserted or deleted.
func (db *DB) Changes() int {
	return int(C.db_changes(db.ptr))
}

// LastInsertRowID returns the rowid of the most recent successful INSERT.
func (db *DB) LastInsertRowID() int64 {
	return int64(C.db_last_insert_rowid(db.ptr))
}

// ── Batch transactions ──────────────────────────────────────────────

// Begin starts an explicit batch transaction.
func (db *DB) Begin() error {
	rc := C.db_begin(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_begin failed (rc=%d): %s", rc, db.Error())
	}
	return nil
}

// Commit commits the active batch transaction and flushes pending WAL entries.
func (db *DB) Commit() error {
	rc := C.db_commit(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_commit failed (rc=%d): %s", rc, db.Error())
	}
	return nil
}

// Rollback aborts the active batch transaction.
func (db *DB) Rollback() error {
	rc := C.db_rollback(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_rollback failed (rc=%d): %s", rc, db.Error())
	}
	return nil
}

// ── WAL buffer & shipping ───────────────────────────────────────────

// WALPending returns the number of entries waiting in the double-buffer.
func (db *DB) WALPending() int {
	return int(C.db_wal_pending(db.ptr))
}

// FlushWAL forces the WAL double-buffer to flush immediately.
func (db *DB) FlushWAL() {
	C.db_wal_flush(db.ptr)
}

// WALLastSQL returns the last recorded SQL statement in the WAL buffer.
func (db *DB) WALLastSQL() string {
	ptr := C.db_wal_last_sql(db.ptr)
	if ptr == nil {
		return ""
	}
	return C.GoString(ptr)
}

// ── Backup controls & trigger synchronization ───────────────────────

// SetBackupEnabled toggles runtime backup shipping without a restart.
func (db *DB) SetBackupEnabled(enabled bool) {
	val := C.int(0)
	if enabled {
		val = 1
	}
	C.db_backup_set_enabled(db.ptr, val)
}

// IsBackupEnabled returns whether backup shipping is currently enabled.
func (db *DB) IsBackupEnabled() bool {
	return C.db_backup_is_enabled(db.ptr) != 0
}

// ResyncTriggers re-scans live tables and regenerates capture triggers.
func (db *DB) ResyncTriggers() error {
	rc := C.db_resync_triggers(db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: db_resync_triggers failed: %s", db.Error())
	}
	return nil
}

// SetAutoResyncTriggers enables or disables automatic trigger resync on schema changes.
func (db *DB) SetAutoResyncTriggers(enabled bool) {
	val := C.int(0)
	if enabled {
		val = 1
	}
	C.db_set_auto_resync_triggers(db.ptr, val)
}

// AutoResyncTriggers returns whether automatic trigger resync is enabled.
func (db *DB) AutoResyncTriggers() bool {
	return C.db_get_auto_resync_triggers(db.ptr) != 0
}

// TriggersDirty returns true when raw-handle DDL has desynchronized triggers.
func (db *DB) TriggersDirty() bool {
	return C.db_backup_triggers_dirty(db.ptr) != 0
}

// CapturePaused returns true if the outbox has reached the capacity ceiling and CDC rows are dropped.
func (db *DB) CapturePaused() bool {
	return C.db_backup_capture_paused(db.ptr) != 0
}

// ── Monitoring & Health Metrics ─────────────────────────────────────

func (db *DB) BackupQueueDepth() int {
	return int(C.db_backup_queue_depth(db.ptr))
}

func (db *DB) BackupOldestPendingAgeSec() int64 {
	return int64(C.db_backup_oldest_pending_age_sec(db.ptr))
}

func (db *DB) BackupDeadLetterCount() int {
	return int(C.db_backup_dead_letter_count(db.ptr))
}

func (db *DB) BackupThreadHeartbeatAgeMs() int64 {
	return int64(C.db_backup_thread_heartbeat_age_ms(db.ptr))
}

func (db *DB) BackupSnapshotHeartbeatAgeMs() int64 {
	return int64(C.db_backup_snapshot_heartbeat_age_ms(db.ptr))
}

func (db *DB) BackupTriggerCoverage() int {
	return int(C.db_backup_trigger_coverage(db.ptr))
}

func (db *DB) BackupSkippedTableCount() int {
	return int(C.db_backup_skipped_table_count(db.ptr))
}

func (db *DB) BackupChunkCount() int {
	return int(C.db_backup_chunk_count(db.ptr))
}

func (db *DB) BackupLastChunkFlushAgeMs() int64 {
	return int64(C.db_backup_last_chunk_flush_age_ms(db.ptr))
}

func (db *DB) BackupHealthFlags() uint32 {
	return uint32(C.db_backup_health_flags(db.ptr))
}

func (db *DB) BackupIsHealthy() bool {
	return C.db_backup_is_healthy(db.ptr) != 0
}

// Error returns the last error message recorded by the database handle.
func (db *DB) Error() string {
	return C.GoString(C.db_errmsg(db.ptr))
}

// ── Prepared Statements ─────────────────────────────────────────────

// Stmt represents a prepared statement.
type Stmt struct {
	db  *DB
	idx int
}

// Prepare compiles a SQL query into a prepared statement.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	rc := C.db_prepare(db.ptr, cSQL)
	if rc != C.SQLITE_OK {
		return nil, fmt.Errorf("arkilian: %s", C.GoString(C.db_errmsg(db.ptr)))
	}
	return &Stmt{db: db, idx: int(C.db_stmt_count(db.ptr)) - 1}, nil
}

// Step advances to the next result row. Returns true if a row is available.
func (s *Stmt) Step() (bool, error) {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_step(s.db.ptr)
	if rc == C.SQLITE_ROW {
		return true, nil
	}
	if rc == C.SQLITE_DONE {
		return false, nil
	}
	return false, fmt.Errorf("arkilian: step failed (rc=%d): %s", int(rc), s.db.Error())
}

// Finalize frees statement resources.
func (s *Stmt) Finalize() error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_finalize(s.db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: finalize failed (rc=%d)", int(rc))
	}
	return nil
}

// Reset resets the statement for re-execution.
func (s *Stmt) Reset() error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_reset(s.db.ptr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: reset failed (rc=%d)", int(rc))
	}
	return nil
}

// ── Column Access & Introspection ───────────────────────────────────

func (s *Stmt) ColumnCount() int {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return int(C.db_column_count(s.db.ptr))
}

func (s *Stmt) ColumnName(col int) string {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return C.GoString(C.db_column_name(s.db.ptr, C.int(col)))
}

func (s *Stmt) ColumnType(col int) int {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return int(C.db_column_type(s.db.ptr, C.int(col)))
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

func (s *Stmt) ColumnBlob(col int) []byte {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	ptr := C.db_column_blob(s.db.ptr, C.int(col))
	nbytes := int(C.db_column_bytes(s.db.ptr, C.int(col)))
	if ptr == nil || nbytes <= 0 {
		return []byte{}
	}
	return C.GoBytes(ptr, C.int(nbytes))
}

func (s *Stmt) ColumnBytes(col int) int {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	return int(C.db_column_bytes(s.db.ptr, C.int(col)))
}

func (s *Stmt) ColumnValue(col int) interface{} {
	switch s.ColumnType(col) {
	case ColumnNull:
		return nil
	case ColumnInteger:
		return s.ColumnInt64(col)
	case ColumnFloat:
		return s.ColumnDouble(col)
	case ColumnBlob:
		return s.ColumnBlob(col)
	default:
		return s.ColumnText(col)
	}
}

// ── Parameter Binding ───────────────────────────────────────────────

func (s *Stmt) BindText(idx int, val string) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	cVal := C.CString(val)
	defer C.free(unsafe.Pointer(cVal))
	rc := C.db_bind_text(s.db.ptr, C.int(idx), cVal)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_text failed: %s", s.db.Error())
	}
	return nil
}

func (s *Stmt) BindInt(idx, val int) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_int(s.db.ptr, C.int(idx), C.int(val))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_int failed: %s", s.db.Error())
	}
	return nil
}

func (s *Stmt) BindInt64(idx int, val int64) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_int64(s.db.ptr, C.int(idx), C.sqlite3_int64(val))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_int64 failed: %s", s.db.Error())
	}
	return nil
}

func (s *Stmt) BindDouble(idx int, val float64) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_double(s.db.ptr, C.int(idx), C.double(val))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_double failed: %s", s.db.Error())
	}
	return nil
}

func (s *Stmt) BindNull(idx int) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	rc := C.db_bind_null(s.db.ptr, C.int(idx))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_null failed: %s", s.db.Error())
	}
	return nil
}

func (s *Stmt) BindBlob(idx int, val []byte) error {
	C.db_use_stmt(s.db.ptr, C.int(s.idx))
	var ptr unsafe.Pointer
	if len(val) > 0 {
		ptr = unsafe.Pointer(&val[0])
	}
	rc := C.db_bind_blob(s.db.ptr, C.int(idx), ptr, C.int(len(val)))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("arkilian: bind_blob failed: %s", s.db.Error())
	}
	return nil
}

func (s *Stmt) BindParam(idx int, val interface{}) error {
	if val == nil {
		return s.BindNull(idx)
	}
	switch v := val.(type) {
	case string:
		return s.BindText(idx, v)
	case int:
		return s.BindInt64(idx, int64(v))
	case int64:
		return s.BindInt64(idx, v)
	case int32:
		return s.BindInt(idx, int(v))
	case float64:
		return s.BindDouble(idx, v)
	case float32:
		return s.BindDouble(idx, float64(v))
	case bool:
		if v {
			return s.BindInt(idx, 1)
		}
		return s.BindInt(idx, 0)
	case []byte:
		return s.BindBlob(idx, v)
	default:
		return s.BindText(idx, fmt.Sprintf("%v", v))
	}
}

// ── Query Helpers ───────────────────────────────────────────────────

func (db *DB) QueryRow(sql string, params ...interface{}) (map[string]interface{}, error) {
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	for i, p := range params {
		if err := stmt.BindParam(i+1, p); err != nil {
			return nil, err
		}
	}

	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no rows")
	}

	return stmt.rowMap(), nil
}

func (db *DB) Query(sql string, params ...interface{}) ([]map[string]interface{}, error) {
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	for i, p := range params {
		if err := stmt.BindParam(i+1, p); err != nil {
			return nil, err
		}
	}

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
		m[s.ColumnName(i)] = s.ColumnValue(i)
	}
	return m
}
