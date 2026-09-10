from .binding import lib, ffi

# SQLite Result Codes
SQLITE_OK = 0
SQLITE_ROW = 100
SQLITE_DONE = 101
SQLITE_ERROR = 1
SQLITE_BUSY = 5

# SQLite Column Types
SQLITE_INTEGER = 1
SQLITE_FLOAT = 2
SQLITE_TEXT = 3
SQLITE_BLOB = 4
SQLITE_NULL = 5

# Health State Machine Flags (ARK_HF_*)
ARK_HF_BACKUP_ENABLED = 1 << 0
ARK_HF_DEST_CONFIGURED = 1 << 1
ARK_HF_FLUSH_ALIVE = 1 << 2
ARK_HF_SNAPSHOT_ALIVE = 1 << 3
ARK_HF_QUEUE_BELOW_CAP = 1 << 4
ARK_HF_SCHEMA_IN_SYNC = 1 << 5
ARK_HF_NO_DEAD_LETTER = 1 << 6
ARK_HF_MANIFEST_RESOLVED = 1 << 7
ARK_HF_NO_CAPTURE_GAP = 1 << 8
ARK_HF_DURABLE_CAPTURE = 1 << 9
ARK_HF_ALL_CORE = 0x1FF

# Log Levels
ARK_LOG_ERROR = 0
ARK_LOG_WARN = 1
ARK_LOG_INFO = 2
ARK_LOG_DEBUG = 3

# Hydration Error Codes
HYDRATION_OK = 0
HYDRATION_ERR_NET = -1
HYDRATION_ERR_DISK = -2
HYDRATION_ERR_MEM = -3
HYDRATION_ERR_PROTO = -4
HYDRATION_ERR_SQL = -5
HYDRATION_ERR_DECOMP = -6
HYDRATION_ERR_EXPIRED = -7
HYDRATION_ERR_NOTFOUND = -8
HYDRATION_ERR_NEWER = -9
HYDRATION_ERR_BUSY = -10


class Arkilian:
    """Arkilian - SQLite with automated S3-compatible cloud replication and durability engine."""

    def __init__(self, db_path="app.sqlite", token=None):
        self._db = ffi.new("arkilian**")
        resolved_path = db_path.encode() if isinstance(db_path, str) else b"app.sqlite"
        result = lib.db_init(self._db, resolved_path)
        if result != 0:
            err = self.last_error if self._db[0] else f"rc={result}"
            if self._db[0]:
                lib.db_close(self._db[0])
                self._db = None
            raise RuntimeError(f"Failed to initialize database: {err}")
        if self._db[0] is None:
            raise RuntimeError("Database handle is null")
        if token is not None:
            self.set_token(token)

    @classmethod
    def open(cls, db_path="app.sqlite", token=None):
        return cls(db_path, token)

    @staticmethod
    def hydrate_s3(db_path, endpoint, bucket, region="us-east-1", access_key="", secret_key="", prefix=""):
        """Cold-start restore from S3-compatible storage. Call before db_init on a cold process."""
        rc = lib.arkilian_hydrate_s3(
            db_path.encode(),
            endpoint.encode(),
            bucket.encode(),
            region.encode(),
            access_key.encode(),
            secret_key.encode(),
            prefix.encode(),
            ffi.NULL,
            ffi.NULL,
        )
        if rc != HYDRATION_OK:
            raise RuntimeError(f"Hydration failed with error code {rc}")
        return True

    def set_token(self, token):
        # Arkilian v2 uses S3 SigV4 credentials configured via environment variables.
        # Maintained for backward compatibility.
        return self

    def close(self):
        if self._db and self._db[0]:
            lib.db_close(self._db[0])
            self._db = None

    def exec(self, sql):
        result = lib.db_exec(self._db[0], sql.encode())
        if result != SQLITE_OK and result != SQLITE_DONE:
            raise RuntimeError(self.last_error)
        return result

    # ── Batch transactions ──────────────────────────────────────────
    def begin(self):
        result = lib.db_begin(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(f"db_begin failed: {self.last_error}")
        return self

    def commit(self):
        result = lib.db_commit(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(f"db_commit failed: {self.last_error}")
        return self

    def rollback(self):
        result = lib.db_rollback(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(f"db_rollback failed: {self.last_error}")
        return self

    # ── Changes & RowID ─────────────────────────────────────────────
    @property
    def changes(self):
        return lib.db_changes(self._db[0])

    @property
    def last_insert_rowid(self):
        return lib.db_last_insert_rowid(self._db[0])

    # ── WAL double-buffer & shipping ────────────────────────────────
    @property
    def wal_pending(self):
        return lib.db_wal_pending(self._db[0])

    def wal_flush(self):
        lib.db_wal_flush(self._db[0])
        return self

    @property
    def wal_last_sql(self):
        ptr = lib.db_wal_last_sql(self._db[0])
        return ffi.string(ptr).decode("utf-8", errors="replace") if ptr else None

    # ── Runtime backup kill-switch & trigger resync ──────────────────
    def set_backup_enabled(self, enabled):
        lib.db_backup_set_enabled(self._db[0], 1 if enabled else 0)
        return self

    @property
    def backup_enabled(self):
        return bool(lib.db_backup_is_enabled(self._db[0]))

    def resync_triggers(self):
        rc = lib.db_resync_triggers(self._db[0])
        if rc != SQLITE_OK:
            raise RuntimeError(f"db_resync_triggers failed: {self.last_error}")
        return self

    def set_auto_resync_triggers(self, enabled):
        lib.db_set_auto_resync_triggers(self._db[0], 1 if enabled else 0)
        return self

    @property
    def auto_resync_triggers(self):
        return bool(lib.db_get_auto_resync_triggers(self._db[0]))

    @property
    def triggers_dirty(self):
        return bool(lib.db_backup_triggers_dirty(self._db[0]))

    @property
    def capture_paused(self):
        return bool(lib.db_backup_capture_paused(self._db[0]))

    # ── Monitoring & Health ──────────────────────────────────────────
    @property
    def backup_queue_depth(self):
        return lib.db_backup_queue_depth(self._db[0])

    @property
    def backup_oldest_pending_age_sec(self):
        return lib.db_backup_oldest_pending_age_sec(self._db[0])

    @property
    def backup_dead_letter_count(self):
        return lib.db_backup_dead_letter_count(self._db[0])

    @property
    def backup_thread_heartbeat_age_ms(self):
        return lib.db_backup_thread_heartbeat_age_ms(self._db[0])

    @property
    def backup_snapshot_heartbeat_age_ms(self):
        return lib.db_backup_snapshot_heartbeat_age_ms(self._db[0])

    @property
    def backup_trigger_coverage(self):
        return lib.db_backup_trigger_coverage(self._db[0])

    @property
    def backup_skipped_table_count(self):
        return lib.db_backup_skipped_table_count(self._db[0])

    @property
    def backup_chunk_count(self):
        return lib.db_backup_chunk_count(self._db[0])

    @property
    def backup_last_chunk_flush_age_ms(self):
        return lib.db_backup_last_chunk_flush_age_ms(self._db[0])

    @property
    def backup_health_flags(self):
        return lib.db_backup_health_flags(self._db[0])

    @property
    def backup_healthy(self):
        return bool(lib.db_backup_is_healthy(self._db[0]))

    @property
    def is_healthy(self):
        return self.backup_healthy

    # ── Statement management ────────────────────────────────────────
    def prepare(self, sql):
        result = lib.db_prepare(self._db[0], sql.encode())
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def use_stmt(self, index):
        result = lib.db_use_stmt(self._db[0], index)
        if result != SQLITE_OK:
            raise RuntimeError("Invalid statement index or statement already finalized")
        return self

    def stmt_count(self):
        return lib.db_stmt_count(self._db[0])

    def step(self):
        return lib.db_step(self._db[0])

    def finalize(self):
        result = lib.db_finalize(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def reset(self):
        result = lib.db_reset(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def column_count(self):
        return lib.db_column_count(self._db[0])

    def column_name(self, col):
        return ffi.string(lib.db_column_name(self._db[0], col)).decode()

    def column_type(self, col):
        return lib.db_column_type(self._db[0], col)

    def column_text(self, col):
        ptr = lib.db_column_text(self._db[0], col)
        return ffi.string(ptr).decode("utf-8", errors="replace") if ptr else None

    def column_int(self, col):
        return lib.db_column_int(self._db[0], col)

    def column_int64(self, col):
        return lib.db_column_int64(self._db[0], col)

    def column_double(self, col):
        return lib.db_column_double(self._db[0], col)

    def column_blob(self, col):
        ptr = lib.db_column_blob(self._db[0], col)
        nbytes = lib.db_column_bytes(self._db[0], col)
        return bytes(ffi.buffer(ptr, nbytes)) if ptr and nbytes > 0 else b""

    def column_bytes(self, col):
        return lib.db_column_bytes(self._db[0], col)

    def column_value(self, col):
        t = self.column_type(col)
        if t == SQLITE_NULL:
            return None
        elif t == SQLITE_INTEGER:
            return self.column_int64(col)
        elif t == SQLITE_FLOAT:
            return self.column_double(col)
        elif t == SQLITE_BLOB:
            return self.column_blob(col)
        else:
            return self.column_text(col)

    def bind_text(self, idx, value):
        result = lib.db_bind_text(self._db[0], idx, value.encode() if isinstance(value, str) else bytes(value))
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_int(self, idx, value):
        result = lib.db_bind_int(self._db[0], idx, value)
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_int64(self, idx, value):
        result = lib.db_bind_int64(self._db[0], idx, value)
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_double(self, idx, value):
        result = lib.db_bind_double(self._db[0], idx, float(value))
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_null(self, idx):
        result = lib.db_bind_null(self._db[0], idx)
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_blob(self, idx, value):
        buf = bytes(value) if not isinstance(value, (bytes, bytearray, memoryview)) else value
        c_buf = ffi.from_buffer(buf)
        result = lib.db_bind_blob(self._db[0], idx, c_buf, len(buf))
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def _bind_param(self, idx, val):
        if val is None:
            self.bind_null(idx)
        elif isinstance(val, bool):
            self.bind_int(idx, 1 if val else 0)
        elif isinstance(val, int):
            self.bind_int64(idx, val)
        elif isinstance(val, float):
            self.bind_double(idx, val)
        elif isinstance(val, (bytes, bytearray, memoryview)):
            self.bind_blob(idx, val)
        else:
            self.bind_text(idx, str(val))

    def run(self, sql, params=None):
        self.prepare(sql)
        if params:
            for i, p in enumerate(params):
                self._bind_param(i + 1, p)
        self.step()
        self.finalize()
        return self

    def all(self, sql, params=None):
        results = []
        self.prepare(sql)
        if params:
            for i, p in enumerate(params):
                self._bind_param(i + 1, p)
        col_count = self.column_count()
        columns = [self.column_name(i) for i in range(col_count)]
        while self.step() == SQLITE_ROW:
            row = {}
            for i, col in enumerate(columns):
                row[col] = self.column_value(i)
            results.append(row)
        self.finalize()
        return results

    @property
    def last_error(self):
        return ffi.string(lib.db_errmsg(self._db[0])).decode() if self._db and self._db[0] else ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False