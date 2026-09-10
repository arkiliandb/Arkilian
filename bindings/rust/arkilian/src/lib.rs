//! High-level Rust bindings to Arkilian database library.
//!
//! Provides a safe, idiomatic Rust API on top of the Arkilian C core,
//! including transactions, WAL inspection/flushing, automated replication controls,
//! deep health monitoring metrics, bitmask flags, typed column access, and S3 cold hydration.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

pub mod ffi {
    include!("bindings.rs");
}

// ── SQLite Result Codes ───────────────────────────────────────────────────

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ERROR: i32 = 1;
pub const SQLITE_BUSY: i32 = 5;
pub const SQLITE_ROW: i32 = 100;
pub const SQLITE_DONE: i32 = 101;

// ── SQLite Column Types ───────────────────────────────────────────────────

pub const SQLITE_INTEGER: i32 = 1;
pub const SQLITE_FLOAT: i32 = 2;
pub const SQLITE_TEXT: i32 = 3;
pub const SQLITE_BLOB: i32 = 4;
pub const SQLITE_NULL: i32 = 5;

// ── Health State Machine Flags (ARK_HF_*) ─────────────────────────────────

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

pub const HYDRATION_OK: i32 = 0;
pub const HYDRATION_ERR_NET: i32 = -1;
pub const HYDRATION_ERR_DISK: i32 = -2;
pub const HYDRATION_ERR_MEM: i32 = -3;
pub const HYDRATION_ERR_PROTO: i32 = -4;
pub const HYDRATION_ERR_SQL: i32 = -5;
pub const HYDRATION_ERR_DECOMP: i32 = -6;
pub const HYDRATION_ERR_EXPIRED: i32 = -7;
pub const HYDRATION_ERR_NOT_FOUND: i32 = -8;
pub const HYDRATION_ERR_NEWER: i32 = -9;
pub const HYDRATION_ERR_BUSY: i32 = -10;

// ── Hydration Configuration ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub use_ssl: bool,
    pub timeout_ms: i32,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: "us-east-1".to_string(),
            bucket: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            use_ssl: false,
            timeout_ms: 10_000,
        }
    }
}

/// Cold-start database recovery from S3 storage.
pub fn hydrate_s3(local_db_path: &str, db_id: &str, s3: &S3Config) -> Result<(), String> {
    let c_local = CString::new(local_db_path).map_err(|e| e.to_string())?;
    let c_db_id = CString::new(db_id).map_err(|e| e.to_string())?;
    let c_endpoint = CString::new(s3.endpoint.as_str()).map_err(|e| e.to_string())?;
    let c_region = CString::new(s3.region.as_str()).map_err(|e| e.to_string())?;
    let c_bucket = CString::new(s3.bucket.as_str()).map_err(|e| e.to_string())?;
    let c_key = CString::new(s3.access_key_id.as_str()).map_err(|e| e.to_string())?;
    let c_secret = CString::new(s3.secret_access_key.as_str()).map_err(|e| e.to_string())?;
    let c_token = match &s3.session_token {
        Some(t) => Some(CString::new(t.as_str()).map_err(|e| e.to_string())?),
        None => None,
    };

    let s3_raw = ffi::arkilian_s3_config {
        endpoint: c_endpoint.as_ptr(),
        region: c_region.as_ptr(),
        bucket: c_bucket.as_ptr(),
        access_key_id: c_key.as_ptr(),
        secret_access_key: c_secret.as_ptr(),
        session_token: c_token.as_ref().map_or(ptr::null(), |t| t.as_ptr()),
        use_ssl: if s3.use_ssl { 1 } else { 0 },
        timeout_ms: s3.timeout_ms,
    };

    let mut err_buf = vec![0u8; 1024];
    let rc = unsafe {
        ffi::arkilian_hydrate_s3(
            c_local.as_ptr(),
            c_db_id.as_ptr(),
            &s3_raw,
            err_buf.as_mut_ptr() as *mut c_char,
            err_buf.len(),
        )
    };

    if rc != HYDRATION_OK {
        let msg = unsafe { CStr::from_ptr(err_buf.as_ptr() as *const c_char) }
            .to_string_lossy()
            .into_owned();
        return Err(format!("Hydration failed (code {}): {}", rc, msg));
    }
    Ok(())
}

// ── Database ──────────────────────────────────────────────────────────────

pub struct Database {
    ptr: *mut ffi::arkilian,
}

impl Database {
    /// Opens or creates an Arkilian database at the specified path.
    pub fn new(path: &str) -> Result<Self, String> {
        let c_path = CString::new(path).map_err(|_| "Invalid path")?;
        let mut db_ptr: *mut ffi::arkilian = ptr::null_mut();

        let result = unsafe { ffi::db_init(&mut db_ptr, c_path.as_ptr()) };

        if result != SQLITE_OK {
            let err = unsafe {
                if !db_ptr.is_null() {
                    CStr::from_ptr(ffi::db_errmsg(db_ptr))
                        .to_string_lossy()
                        .into_owned()
                } else {
                    "Failed to initialize database".to_string()
                }
            };
            return Err(err);
        }

        Ok(Database { ptr: db_ptr })
    }

    /// Opens an Arkilian database with an optional API token.
    pub fn open(token: &str, path: &str) -> Result<Self, String> {
        let db = Self::new(path)?;
        if !token.is_empty() {
            db.set_token(token)?;
        }
        Ok(db)
    }

    /// Returns the raw SQLite3 database handle.
    pub fn handle(&self) -> *mut ffi::sqlite3 {
        unsafe { ffi::db_get_handle(self.ptr) }
    }

    /// Closes the database and flushes remaining state.
    pub fn close(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::db_close(self.ptr) };
            self.ptr = ptr::null_mut();
        }
    }

    /// Configures the API token (no-op in v2; preserved for backward compatibility).
    pub fn set_token(&self, _token: &str) -> Result<(), String> {
        Ok(())
    }

    /// Executes raw SQL commands (DDL / DML).
    pub fn exec(&self, sql: &str) -> Result<i32, String> {
        let c_sql = CString::new(sql).map_err(|_| "Invalid SQL")?;
        let result = unsafe { ffi::db_exec(self.ptr, c_sql.as_ptr()) };

        if result != SQLITE_OK && result != SQLITE_DONE {
            return Err(self.last_error());
        }
        Ok(result)
    }

    // ── Transactions ─────────────────────────────────────────────────────

    pub fn begin(&self) -> Result<(), String> {
        let rc = unsafe { ffi::db_begin(self.ptr) };
        if rc != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(())
    }

    pub fn commit(&self) -> Result<(), String> {
        let rc = unsafe { ffi::db_commit(self.ptr) };
        if rc != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(())
    }

    pub fn rollback(&self) -> Result<(), String> {
        let rc = unsafe { ffi::db_rollback(self.ptr) };
        if rc != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(())
    }

    // ── Changes & Row ID ─────────────────────────────────────────────────

    pub fn changes(&self) -> i32 {
        unsafe { ffi::db_changes(self.ptr) }
    }

    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::db_last_insert_rowid(self.ptr) }
    }

    // ── Prepared Statements ──────────────────────────────────────────────

    pub fn prepare(&self, sql: &str) -> Result<i32, String> {
        let c_sql = CString::new(sql).map_err(|_| "Invalid SQL")?;
        let result = unsafe { ffi::db_prepare(self.ptr, c_sql.as_ptr()) };

        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn use_stmt(&self, index: i32) -> Result<(), String> {
        let result = unsafe { ffi::db_use_stmt(self.ptr, index) };
        if result != SQLITE_OK {
            return Err("Invalid statement index or statement already finalized".to_string());
        }
        Ok(())
    }

    pub fn stmt_count(&self) -> i32 {
        unsafe { ffi::db_stmt_count(self.ptr) }
    }

    pub fn step(&self) -> i32 {
        unsafe { ffi::db_step(self.ptr) }
    }

    pub fn finalize(&self) -> Result<i32, String> {
        let result = unsafe { ffi::db_finalize(self.ptr) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn reset(&self) -> Result<i32, String> {
        let result = unsafe { ffi::db_reset(self.ptr) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    // ── Columns ──────────────────────────────────────────────────────────

    pub fn column_count(&self) -> i32 {
        unsafe { ffi::db_column_count(self.ptr) }
    }

    pub fn column_name(&self, col: i32) -> Option<String> {
        unsafe {
            let ptr = ffi::db_column_name(self.ptr, col);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
            }
        }
    }

    pub fn column_type(&self, col: i32) -> i32 {
        unsafe { ffi::db_column_type(self.ptr, col) }
    }

    pub fn column_text(&self, col: i32) -> Option<String> {
        unsafe {
            let ptr = ffi::db_column_text(self.ptr, col);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
            }
        }
    }

    pub fn column_int(&self, col: i32) -> i32 {
        unsafe { ffi::db_column_int(self.ptr, col) }
    }

    pub fn column_int64(&self, col: i32) -> i64 {
        unsafe { ffi::db_column_int64(self.ptr, col) }
    }

    pub fn column_double(&self, col: i32) -> f64 {
        unsafe { ffi::db_column_double(self.ptr, col) }
    }

    pub fn column_blob(&self, col: i32) -> Vec<u8> {
        unsafe {
            let ptr = ffi::db_column_blob(self.ptr, col);
            let len = ffi::db_column_bytes(self.ptr, col);
            if ptr.is_null() || len <= 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(ptr as *const u8, len as usize).to_vec()
            }
        }
    }

    pub fn column_bytes(&self, col: i32) -> i32 {
        unsafe { ffi::db_column_bytes(self.ptr, col) }
    }

    // ── Parameter Binding ────────────────────────────────────────────────

    pub fn bind_text(&self, idx: i32, value: &str) -> Result<i32, String> {
        let c_val = CString::new(value).map_err(|_| "Invalid value")?;
        let result = unsafe { ffi::db_bind_text(self.ptr, idx, c_val.as_ptr()) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn bind_int(&self, idx: i32, value: i32) -> Result<i32, String> {
        let result = unsafe { ffi::db_bind_int(self.ptr, idx, value) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn bind_int64(&self, idx: i32, value: i64) -> Result<i32, String> {
        let result = unsafe { ffi::db_bind_int64(self.ptr, idx, value) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn bind_double(&self, idx: i32, value: f64) -> Result<i32, String> {
        let result = unsafe { ffi::db_bind_double(self.ptr, idx, value) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn bind_null(&self, idx: i32) -> Result<i32, String> {
        let result = unsafe { ffi::db_bind_null(self.ptr, idx) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    pub fn bind_blob(&self, idx: i32, value: &[u8]) -> Result<i32, String> {
        let ptr = if value.is_empty() { ptr::null() } else { value.as_ptr() as *const c_void };
        let result = unsafe { ffi::db_bind_blob(self.ptr, idx, ptr, value.len() as c_int) };
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }

    // ── WAL & Shipping ───────────────────────────────────────────────────

    pub fn wal_pending(&self) -> i32 {
        unsafe { ffi::db_wal_pending(self.ptr) }
    }

    pub fn wal_flush(&self) {
        unsafe { ffi::db_wal_flush(self.ptr) };
    }

    pub fn wal_last_sql(&self) -> Option<String> {
        unsafe {
            let ptr = ffi::db_wal_last_sql(self.ptr);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
            }
        }
    }

    // ── Backup & Trigger Controls ────────────────────────────────────────

    pub fn backup_set_enabled(&self, enabled: bool) {
        unsafe { ffi::db_backup_set_enabled(self.ptr, if enabled { 1 } else { 0 }) };
    }

    pub fn backup_is_enabled(&self) -> bool {
        unsafe { ffi::db_backup_is_enabled(self.ptr) != 0 }
    }

    pub fn resync_triggers(&self) -> Result<i32, String> {
        let rc = unsafe { ffi::db_resync_triggers(self.ptr) };
        if rc != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(rc)
    }

    pub fn set_auto_resync_triggers(&self, enabled: bool) {
        unsafe { ffi::db_set_auto_resync_triggers(self.ptr, if enabled { 1 } else { 0 }) };
    }

    pub fn auto_resync_triggers(&self) -> bool {
        unsafe { ffi::db_get_auto_resync_triggers(self.ptr) != 0 }
    }

    pub fn backup_triggers_dirty(&self) -> bool {
        unsafe { ffi::db_backup_triggers_dirty(self.ptr) != 0 }
    }

    pub fn backup_capture_paused(&self) -> bool {
        unsafe { ffi::db_backup_capture_paused(self.ptr) != 0 }
    }

    // ── Monitoring & Health State Machine ────────────────────────────────

    pub fn backup_queue_depth(&self) -> i32 {
        unsafe { ffi::db_backup_queue_depth(self.ptr) }
    }

    pub fn backup_oldest_pending_age_sec(&self) -> i32 {
        unsafe { ffi::db_backup_oldest_pending_age_sec(self.ptr) }
    }

    pub fn backup_dead_letter_count(&self) -> i32 {
        unsafe { ffi::db_backup_dead_letter_count(self.ptr) }
    }

    pub fn backup_thread_heartbeat_age_ms(&self) -> i32 {
        unsafe { ffi::db_backup_thread_heartbeat_age_ms(self.ptr) }
    }

    pub fn backup_snapshot_heartbeat_age_ms(&self) -> i32 {
        unsafe { ffi::db_backup_snapshot_heartbeat_age_ms(self.ptr) }
    }

    pub fn backup_trigger_coverage(&self) -> f64 {
        unsafe { ffi::db_backup_trigger_coverage(self.ptr) }
    }

    pub fn backup_skipped_table_count(&self) -> i32 {
        unsafe { ffi::db_backup_skipped_table_count(self.ptr) }
    }

    pub fn backup_chunk_count(&self) -> i32 {
        unsafe { ffi::db_backup_chunk_count(self.ptr) }
    }

    pub fn backup_last_chunk_flush_age_ms(&self) -> i32 {
        unsafe { ffi::db_backup_last_chunk_flush_age_ms(self.ptr) }
    }

    pub fn backup_health_flags(&self) -> u32 {
        unsafe { ffi::db_backup_health_flags(self.ptr) }
    }

    pub fn backup_is_healthy(&self) -> bool {
        unsafe { ffi::db_backup_is_healthy(self.ptr) != 0 }
    }

    // ── High-level execution helpers ─────────────────────────────────────

    pub fn run(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<(), String> {
        self.prepare(sql)?;

        for (i, param) in params.iter().enumerate() {
            param.bind(self, (i + 1) as i32)?;
        }

        let _ = self.step();
        self.finalize()?;

        Ok(())
    }

    pub fn all(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Vec<(String, String)>>, String> {
        self.prepare(sql)?;

        for (i, param) in params.iter().enumerate() {
            param.bind(self, (i + 1) as i32)?;
        }

        let col_count = self.column_count();
        let column_names: Vec<String> = (0..col_count)
            .filter_map(|i| self.column_name(i))
            .collect();

        let mut results = Vec::new();

        while self.step() == SQLITE_ROW {
            let mut row = Vec::new();
            for (i, name) in column_names.iter().enumerate() {
                let value = self.column_text(i as i32).unwrap_or_default();
                row.push((name.clone(), value));
            }
            results.push(row);
        }

        self.finalize()?;

        Ok(results)
    }

    pub fn errmsg(&self) -> String {
        self.last_error()
    }

    fn last_error(&self) -> String {
        unsafe {
            if self.ptr.is_null() {
                return "Database closed".to_string();
            }
            let err_ptr = ffi::db_errmsg(self.ptr);
            if err_ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

// ── ToSql Trait ───────────────────────────────────────────────────────────

pub trait ToSql {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String>;
}

impl ToSql for str {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_text(idx, self)
    }
}

impl ToSql for String {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_text(idx, self.as_str())
    }
}

impl ToSql for i32 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_int(idx, *self)
    }
}

impl ToSql for i64 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_int64(idx, *self)
    }
}

impl ToSql for f64 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_double(idx, *self)
    }
}

impl ToSql for &[u8] {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_blob(idx, self)
    }
}

impl ToSql for Vec<u8> {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_blob(idx, self.as_slice())
    }
}

impl ToSql for &str {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_text(idx, self)
    }
}

impl ToSql for &i32 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_int(idx, **self)
    }
}

impl ToSql for &i64 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_int64(idx, **self)
    }
}

impl ToSql for &f64 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_double(idx, **self)
    }
}