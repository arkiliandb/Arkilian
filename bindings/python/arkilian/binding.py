import os
import sys
import cffi

ffi = cffi.FFI()

ffi.cdef("""
typedef struct arkilian arkilian;
typedef int64_t sqlite3_int64;

int db_init(arkilian **db, const char *connection_url);
void db_close(arkilian *db);
const char* db_errmsg(arkilian *db);

int db_exec(arkilian *db, const char *sql);
int db_begin(arkilian *db);
int db_commit(arkilian *db);
int db_rollback(arkilian *db);
int db_changes(arkilian *db);
sqlite3_int64 db_last_insert_rowid(arkilian *db);

int db_wal_pending(arkilian *db);
void db_wal_flush(arkilian *db);
const char* db_wal_last_sql(arkilian *db);

void db_backup_set_enabled(arkilian *db, int enabled);
int db_backup_is_enabled(arkilian *db);
int db_resync_triggers(arkilian *db);
void db_set_auto_resync_triggers(arkilian *db, int enabled);
int db_get_auto_resync_triggers(arkilian *db);
int db_backup_triggers_dirty(arkilian *db);
int db_backup_capture_paused(arkilian *db);

int db_backup_queue_depth(arkilian *db);
long long db_backup_oldest_pending_age_sec(arkilian *db);
int db_backup_dead_letter_count(arkilian *db);
long long db_backup_thread_heartbeat_age_ms(arkilian *db);
long long db_backup_snapshot_heartbeat_age_ms(arkilian *db);
int db_backup_trigger_coverage(arkilian *db);
int db_backup_skipped_table_count(arkilian *db);
int db_backup_chunk_count(arkilian *db);
long long db_backup_last_chunk_flush_age_ms(arkilian *db);
unsigned db_backup_health_flags(arkilian *db);
int db_backup_is_healthy(arkilian *db);

int db_prepare(arkilian *db, const char *sql);
int db_use_stmt(arkilian *db, int index);
int db_stmt_count(arkilian *db);
int db_step(arkilian *db);
int db_finalize(arkilian *db);
int db_reset(arkilian *db);

int db_column_count(arkilian *db);
const char* db_column_name(arkilian *db, int col);
int db_column_type(arkilian *db, int col);
const char* db_column_text(arkilian *db, int col);
int db_column_int(arkilian *db, int col);
sqlite3_int64 db_column_int64(arkilian *db, int col);
double db_column_double(arkilian *db, int col);
const void* db_column_blob(arkilian *db, int col);
int db_column_bytes(arkilian *db, int col);

int db_bind_text(arkilian *db, int idx, const char *val);
int db_bind_int(arkilian *db, int idx, int val);
int db_bind_int64(arkilian *db, int idx, sqlite3_int64 val);
int db_bind_double(arkilian *db, int idx, double val);
int db_bind_null(arkilian *db, int idx);
int db_bind_blob(arkilian *db, int idx, const void *val, int n);

typedef void (*hydration_progress_cb)(int phase, int current, int total, void *user_data);

int arkilian_hydrate_s3(const char *db_path,
                         const char *s3_endpoint,
                         const char *s3_bucket,
                         const char *s3_region,
                         const char *s3_access_key,
                         const char *s3_secret_key,
                         const char *s3_prefix,
                         hydration_progress_cb progress,
                         void *user_data);

typedef enum {
  ARK_LOG_ERROR = 0,
  ARK_LOG_WARN  = 1,
  ARK_LOG_INFO  = 2,
  ARK_LOG_DEBUG = 3
} ark_log_level_t;

typedef void (*ark_log_fn_t)(ark_log_level_t level, const char *msg, void *ctx);
void db_set_log_callback(arkilian *db, ark_log_fn_t fn, void *ctx);
""")

this_dir = os.path.dirname(os.path.abspath(__file__))

if sys.platform == "darwin":
    lib_names = ["libarkilian.dylib", "libarkilian.1.dylib", "libarkilian.1.0.0.dylib"]
elif sys.platform == "win32":
    lib_names = ["arkilian.dll", "libarkilian.dll"]
else:
    lib_names = ["libarkilian.so", "libarkilian.so.1", "libarkilian.1.0.0.so"]

candidate_paths = []

# 1. Explicit env var override
if "ARKILIAN_LIB_PATH" in os.environ and os.path.exists(os.environ["ARKILIAN_LIB_PATH"]):
    candidate_paths.append(os.environ["ARKILIAN_LIB_PATH"])

# 2. Bundled inside package directory (e.g. from binary wheel)
for name in lib_names:
    candidate_paths.append(os.path.join(this_dir, name))
    candidate_paths.append(os.path.join(this_dir, "lib", name))

# 3. Standard build output directories (dev/repo build)
repo_build_dirs = [
    os.path.join(this_dir, "..", "..", "..", "build"),
    os.path.join(this_dir, "..", "..", "..", "build", "Release"),
    os.path.join(this_dir, "..", "..", "..", "build-c"),
    os.path.join(this_dir, "..", "..", "..", "cmake-build-release"),
    os.path.join(this_dir, "..", "..", "build"),
    os.path.join(this_dir, "..", "build"),
]
for b_dir in repo_build_dirs:
    for name in lib_names:
        candidate_paths.append(os.path.join(b_dir, name))

# 4. Standard system search paths
system_dirs = ["/usr/local/lib", "/usr/lib", "/opt/homebrew/lib", "/lib"]
for s_dir in system_dirs:
    for name in lib_names:
        candidate_paths.append(os.path.join(s_dir, name))

resolved_path = None
for p in candidate_paths:
    if os.path.exists(p):
        resolved_path = p
        break

if not resolved_path:
    # Try system loader lookup as final fallback
    for name in lib_names:
        try:
            lib = ffi.dlopen(name)
            resolved_path = name
            break
        except Exception:
            pass

if not resolved_path:
    raise RuntimeError(
        f"Arkilian shared library not found ({', '.join(lib_names)}). "
        "Set ARKILIAN_LIB_PATH or compile via 'cmake --build build'."
    )

if "lib" not in locals():
    lib = ffi.dlopen(resolved_path)

__all__ = ["ffi", "lib"]