//! Raw FFI bindings to the Arkilian database library.
//!
//! This crate provides unsafe C function declarations for the Arkilian API
//! and the subset of SQLite3 needed for query execution.

#![allow(non_camel_case_types)]

use std::os::raw::{c_char, c_double, c_int, c_void};

// ── Opaque types ──────────────────────────────────────────────────────────

/// Opaque handle to an Arkilian database context.
pub enum arkilian {}

/// Opaque handle to a SQLite3 database connection.
pub enum sqlite3 {}

/// Opaque handle to a SQLite3 prepared statement.
pub enum sqlite3_stmt {}

// ── SQLite constants ──────────────────────────────────────────────────────

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;

pub const SQLITE_INTEGER: c_int = 1;
pub const SQLITE_FLOAT: c_int = 2;
pub const SQLITE_TEXT: c_int = 3;
pub const SQLITE_BLOB: c_int = 4;
pub const SQLITE_NULL: c_int = 5;

/// SQLITE_TRANSIENT: tells SQLite to make its own copy of the data.
pub const SQLITE_TRANSIENT: isize = -1;

// ── Arkilian Health Flags ──────────────────────────────────────────────────

pub const ARK_HF_BACKUP_ENABLED: u32 = 1 << 0;
pub const ARK_HF_DEST_CONFIGURED: u32 = 1 << 1;
pub const ARK_HF_FLUSH_ALIVE: u32 = 1 << 2;
pub const ARK_HF_SNAPSHOT_ALIVE: u32 = 1 << 3;
pub const ARK_HF_QUEUE_BELOW_CAP: u32 = 1 << 4;
pub const ARK_HF_SCHEMA_IN_SYNC: u32 = 1 << 5;
pub const ARK_HF_NO_DEAD_LETTER: u32 = 1 << 6;
pub const ARK_HF_MANIFEST_RESOLVED: u32 = 1 << 7;
pub const ARK_HF_NO_CAPTURE_GAP: u32 = 1 << 8;
pub const ARK_HF_DURABLE_CAPTURE: u32 = 1 << 9;
pub const ARK_HF_ALL_CORE: u32 = 0x3ff;

// ── Hydration Error Codes ─────────────────────────────────────────────────

pub const HYDRATION_OK: c_int = 0;
pub const HYDRATION_ERR_NET: c_int = -1;
pub const HYDRATION_ERR_DISK: c_int = -2;
pub const HYDRATION_ERR_MEM: c_int = -3;
pub const HYDRATION_ERR_PROTO: c_int = -4;
pub const HYDRATION_ERR_SQL: c_int = -5;
pub const HYDRATION_ERR_DECOMP: c_int = -6;
pub const HYDRATION_ERR_EXPIRED: c_int = -7;
pub const HYDRATION_ERR_NOT_FOUND: c_int = -8;
pub const HYDRATION_ERR_NEWER: c_int = -9;
pub const HYDRATION_ERR_BUSY: c_int = -10;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct arkilian_s3_config {
    pub endpoint: *const c_char,
    pub region: *const c_char,
    pub bucket: *const c_char,
    pub access_key_id: *const c_char,
    pub secret_access_key: *const c_char,
    pub session_token: *const c_char,
    pub use_ssl: c_int,
    pub timeout_ms: c_int,
}

// ── Arkilian API ──────────────────────────────────────────────────────────

extern "C" {
    // Lifecycle
    pub fn db_init(db: *mut *mut arkilian, filename: *const c_char) -> c_int;
    pub fn db_close(db: *mut arkilian);
    pub fn db_errmsg(db: *mut arkilian) -> *const c_char;
    pub fn db_get_handle(db: *mut arkilian) -> *mut sqlite3;

    // Transactions & Execution
    pub fn db_exec(db: *mut arkilian, sql: *const c_char) -> c_int;
    pub fn db_begin(db: *mut arkilian) -> c_int;
    pub fn db_commit(db: *mut arkilian) -> c_int;
    pub fn db_rollback(db: *mut arkilian) -> c_int;
    pub fn db_changes(db: *mut arkilian) -> c_int;
    pub fn db_last_insert_rowid(db: *mut arkilian) -> i64;

    // Prepared statements
    pub fn db_prepare(db: *mut arkilian, sql: *const c_char) -> c_int;
    pub fn db_use_stmt(db: *mut arkilian, idx: c_int) -> c_int;
    pub fn db_stmt_count(db: *mut arkilian) -> c_int;
    pub fn db_step(db: *mut arkilian) -> c_int;
    pub fn db_finalize(db: *mut arkilian) -> c_int;
    pub fn db_reset(db: *mut arkilian) -> c_int;

    // Columns
    pub fn db_column_count(db: *mut arkilian) -> c_int;
    pub fn db_column_name(db: *mut arkilian, col: c_int) -> *const c_char;
    pub fn db_column_type(db: *mut arkilian, col: c_int) -> c_int;
    pub fn db_column_text(db: *mut arkilian, col: c_int) -> *const c_char;
    pub fn db_column_int(db: *mut arkilian, col: c_int) -> c_int;
    pub fn db_column_int64(db: *mut arkilian, col: c_int) -> i64;
    pub fn db_column_double(db: *mut arkilian, col: c_int) -> c_double;
    pub fn db_column_blob(db: *mut arkilian, col: c_int) -> *const c_void;
    pub fn db_column_bytes(db: *mut arkilian, col: c_int) -> c_int;

    // Parameter binding
    pub fn db_bind_text(db: *mut arkilian, idx: c_int, val: *const c_char) -> c_int;
    pub fn db_bind_int(db: *mut arkilian, idx: c_int, val: c_int) -> c_int;
    pub fn db_bind_int64(db: *mut arkilian, idx: c_int, val: i64) -> c_int;
    pub fn db_bind_double(db: *mut arkilian, idx: c_int, val: c_double) -> c_int;
    pub fn db_bind_null(db: *mut arkilian, idx: c_int) -> c_int;
    pub fn db_bind_blob(db: *mut arkilian, idx: c_int, val: *const c_void, n: c_int) -> c_int;

    // WAL & Shipping
    pub fn db_wal_pending(db: *mut arkilian) -> c_int;
    pub fn db_wal_flush(db: *mut arkilian);
    pub fn db_wal_last_sql(db: *mut arkilian) -> *const c_char;

    // Backup & Triggers Controls
    pub fn db_backup_set_enabled(db: *mut arkilian, enabled: c_int);
    pub fn db_backup_is_enabled(db: *mut arkilian) -> c_int;
    pub fn db_resync_triggers(db: *mut arkilian) -> c_int;
    pub fn db_set_auto_resync_triggers(db: *mut arkilian, enabled: c_int);
    pub fn db_get_auto_resync_triggers(db: *mut arkilian) -> c_int;
    pub fn db_backup_triggers_dirty(db: *mut arkilian) -> c_int;
    pub fn db_backup_capture_paused(db: *mut arkilian) -> c_int;

    // Monitoring & Health State Machine
    pub fn db_backup_queue_depth(db: *mut arkilian) -> c_int;
    pub fn db_backup_oldest_pending_age_sec(db: *mut arkilian) -> c_int;
    pub fn db_backup_dead_letter_count(db: *mut arkilian) -> c_int;
    pub fn db_backup_thread_heartbeat_age_ms(db: *mut arkilian) -> c_int;
    pub fn db_backup_snapshot_heartbeat_age_ms(db: *mut arkilian) -> c_int;
    pub fn db_backup_trigger_coverage(db: *mut arkilian) -> c_double;
    pub fn db_backup_skipped_table_count(db: *mut arkilian) -> c_int;
    pub fn db_backup_chunk_count(db: *mut arkilian) -> c_int;
    pub fn db_backup_last_chunk_flush_age_ms(db: *mut arkilian) -> c_int;
    pub fn db_backup_health_flags(db: *mut arkilian) -> u32;
    pub fn db_backup_is_healthy(db: *mut arkilian) -> c_int;

    // Hydration
    pub fn arkilian_hydrate_s3(
        local_db_path: *const c_char,
        db_id: *const c_char,
        s3: *const arkilian_s3_config,
        err_buf: *mut c_char,
        err_buf_cap: usize,
    ) -> c_int;
}

// ── SQLite3 API (subset) ──────────────────────────────────────────────────

extern "C" {
    pub fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: *const c_void,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;

    pub fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const c_char,
        n_byte: c_int,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const c_char,
    ) -> c_int;

    pub fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;

    pub fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_column_name(stmt: *mut sqlite3_stmt, col: c_int) -> *const c_char;
    pub fn sqlite3_column_type(stmt: *mut sqlite3_stmt, col: c_int) -> c_int;
    pub fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, col: c_int) -> i64;
    pub fn sqlite3_column_double(stmt: *mut sqlite3_stmt, col: c_int) -> c_double;
    pub fn sqlite3_column_text(stmt: *mut sqlite3_stmt, col: c_int) -> *const c_char;
    pub fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, col: c_int) -> *const c_void;
    pub fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, col: c_int) -> c_int;

    pub fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    pub fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, val: i64) -> c_int;
    pub fn sqlite3_bind_double(stmt: *mut sqlite3_stmt, idx: c_int, val: c_double) -> c_int;
    pub fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        idx: c_int,
        val: *const c_char,
        n: c_int,
        destructor: isize,
    ) -> c_int;
    pub fn sqlite3_bind_blob(
        stmt: *mut sqlite3_stmt,
        idx: c_int,
        val: *const c_void,
        n: c_int,
        destructor: isize,
    ) -> c_int;

    pub fn sqlite3_changes(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64;
    pub fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    pub fn sqlite3_free(ptr: *mut c_void);
}
