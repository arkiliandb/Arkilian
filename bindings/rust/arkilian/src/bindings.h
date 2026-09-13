#ifndef ARKILIAN_BINDINGS_H
#define ARKILIAN_BINDINGS_H

#include <stddef.h>

typedef struct arkilian arkilian;
typedef struct sqlite3 sqlite3;

/* SQLite Result Codes */
#define SQLITE_OK           0
#define SQLITE_ERROR        1
#define SQLITE_BUSY         5
#define SQLITE_ROW          100
#define SQLITE_DONE         101

/* SQLite Column Types */
#define SQLITE_INTEGER      1
#define SQLITE_FLOAT        2
#define SQLITE_TEXT         3
#define SQLITE_BLOB         4
#define SQLITE_NULL         5

/* Health state machine flags (ARK_HF_*) */
#define ARK_HF_BACKUP_ENABLED      (1u << 0)
#define ARK_HF_DEST_CONFIGURED     (1u << 1)
#define ARK_HF_FLUSH_ALIVE         (1u << 2)
#define ARK_HF_SNAPSHOT_ALIVE      (1u << 3)
#define ARK_HF_QUEUE_BELOW_CAP     (1u << 4)
#define ARK_HF_SCHEMA_IN_SYNC      (1u << 5)
#define ARK_HF_NO_DEAD_LETTER      (1u << 6)
#define ARK_HF_MANIFEST_RESOLVED   (1u << 7)
#define ARK_HF_NO_CAPTURE_GAP      (1u << 8)
#define ARK_HF_DURABLE_CAPTURE     (1u << 9)
#define ARK_HF_ALL_CORE            (0x3ff)

/* Hydration error codes */
#define HYDRATION_OK            0
#define HYDRATION_ERR_NET      -1
#define HYDRATION_ERR_DISK     -2
#define HYDRATION_ERR_MEM      -3
#define HYDRATION_ERR_PROTO    -4
#define HYDRATION_ERR_SQL      -5
#define HYDRATION_ERR_DECOMP   -6
#define HYDRATION_ERR_EXPIRED  -7
#define HYDRATION_ERR_NOT_FOUND -8
#define HYDRATION_ERR_NEWER    -9
#define HYDRATION_ERR_BUSY     -10

typedef struct {
    const char *endpoint;
    const char *region;
    const char *bucket;
    const char *access_key_id;
    const char *secret_access_key;
    const char *session_token;
    int use_ssl;
    int timeout_ms;
} arkilian_s3_config;

/* Lifecycle */
int db_init(arkilian **db, const char *connection_url);
void db_close(arkilian *db);
const char* db_errmsg(arkilian *db);
sqlite3* db_get_handle(arkilian *db);

/* Transactions & Execution */
int db_exec(arkilian *db, const char *sql);
int db_begin(arkilian *db);
int db_commit(arkilian *db);
int db_rollback(arkilian *db);
int db_changes(arkilian *db);
long long db_last_insert_rowid(arkilian *db);

/* Prepared Statements */
int db_prepare(arkilian *db, const char *sql);
int db_use_stmt(arkilian *db, int index);
int db_stmt_count(arkilian *db);
int db_step(arkilian *db);
int db_finalize(arkilian *db);
int db_reset(arkilian *db);

/* Columns */
int db_column_count(arkilian *db);
const char* db_column_name(arkilian *db, int col);
int db_column_type(arkilian *db, int col);
const char* db_column_text(arkilian *db, int col);
int db_column_int(arkilian *db, int col);
long long db_column_int64(arkilian *db, int col);
double db_column_double(arkilian *db, int col);
const void* db_column_blob(arkilian *db, int col);
int db_column_bytes(arkilian *db, int col);

/* Parameter Binding */
int db_bind_text(arkilian *db, int idx, const char *val);
int db_bind_int(arkilian *db, int idx, int val);
int db_bind_int64(arkilian *db, int idx, long long val);
int db_bind_double(arkilian *db, int idx, double val);
int db_bind_null(arkilian *db, int idx);
int db_bind_blob(arkilian *db, int idx, const void *val, int n);

/* WAL & Shipping */
int db_wal_pending(arkilian *db);
void db_wal_flush(arkilian *db);
const char* db_wal_last_sql(arkilian *db);

/* Backup & Triggers Controls */
void db_backup_set_enabled(arkilian *db, int enabled);
int db_backup_is_enabled(arkilian *db);
int db_resync_triggers(arkilian *db);
void db_set_auto_resync_triggers(arkilian *db, int enabled);
int db_get_auto_resync_triggers(arkilian *db);
int db_backup_triggers_dirty(arkilian *db);
int db_backup_capture_paused(arkilian *db);

/* Monitoring & Health State Machine */
int db_backup_queue_depth(arkilian *db);
int db_backup_oldest_pending_age_sec(arkilian *db);
int db_backup_dead_letter_count(arkilian *db);
int db_backup_thread_heartbeat_age_ms(arkilian *db);
int db_backup_snapshot_heartbeat_age_ms(arkilian *db);
double db_backup_trigger_coverage(arkilian *db);
int db_backup_skipped_table_count(arkilian *db);
int db_backup_chunk_count(arkilian *db);
int db_backup_last_chunk_flush_age_ms(arkilian *db);
unsigned int db_backup_health_flags(arkilian *db);
int db_backup_is_healthy(arkilian *db);

/* Hydration */
int arkilian_hydrate_s3(
    const char *db_path,
    const char *s3_endpoint,
    const char *s3_bucket,
    const char *s3_region,
    const char *s3_access_key,
    const char *s3_secret_key,
    const char *s3_prefix,
    void *progress,
    void *user_data
);

#endif