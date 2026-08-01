#ifndef ARKILIAN_H
#define ARKILIAN_H

#include "deps/sqlite/sqlite3.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Public C API for Arkilian Managed SQLite Database Engine
typedef struct arkilian arkilian;

int db_init(arkilian **db, const char *connection_url);
void db_close(arkilian *db);
const char* db_errmsg(arkilian *db);
sqlite3* db_get_handle(arkilian *db);
int db_set_token(arkilian *db, const char *token);

int db_exec(arkilian *db, const char *sql);
int db_begin(arkilian *db);
int db_commit(arkilian *db);
int db_rollback(arkilian *db);
int db_wal_pending(arkilian *db);
void db_wal_flush(arkilian *db);

// Runtime kill-switch (spec §1): disable/enable all outbound backup
// activity without a restart. Capture keeps running while disabled —
// rows accumulate in _pending_backup (nothing is deleted, attempts stay
// 0) and shipping resumes exactly where it left off on re-enable.
void db_backup_set_enabled(arkilian *db, int enabled);
int db_backup_is_enabled(arkilian *db);

// Statement management — multiple statements can coexist.
int db_prepare(arkilian *db, const char *sql);
int db_use_stmt(arkilian *db, int index);
int db_stmt_count(arkilian *db);

int db_step(arkilian *db);
int db_finalize(arkilian *db);
int db_reset(arkilian *db);
int db_column_count(arkilian *db);
const char* db_column_name(arkilian *db, int col);
const char* db_column_text(arkilian *db, int col);
int db_column_int(arkilian *db, int col);
double db_column_double(arkilian *db, int col);
int db_bind_text(arkilian *db, int idx, const char *val);
int db_bind_int(arkilian *db, int idx, int val);
int db_bind_int64(arkilian *db, int idx, sqlite3_int64 val);
int db_bind_double(arkilian *db, int idx, double val);
int db_bind_null(arkilian *db, int idx);

int db_column_type(arkilian *db, int col);
sqlite3_int64 db_column_int64(arkilian *db, int col);
const void* db_column_blob(arkilian *db, int col);
int db_column_bytes(arkilian *db, int col);

int db_changes(arkilian *db);
sqlite3_int64 db_last_insert_rowid(arkilian *db);
const char* db_wal_last_sql(arkilian *db);

// Auto-generate SQL backup triggers for live tables
int sync_backup_triggers(sqlite3 *db, char **err_out);

#ifdef __cplusplus
}
#endif

#endif // ARKILIAN_H
