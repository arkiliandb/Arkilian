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

// ── Arkilian API ──────────────────────────────────────────────────────────

extern "C" {
    pub fn db_init(db: *mut *mut arkilian, filename: *const c_char) -> c_int;
    pub fn db_close(db: *mut arkilian);
    pub fn db_errmsg(db: *mut arkilian) -> *const c_char;
    pub fn db_get_handle(db: *mut arkilian) -> *mut sqlite3;
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
