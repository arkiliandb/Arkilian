#ifndef ARKILIAN_H
#define ARKILIAN_H

#include "deps/sqlite/sqlite3.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// public header
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

// Statement management — multiple statements can coexist.
// db_prepare pushes a new statement and makes it "current" (index = count-1).
// db_use_stmt switches which statement is current by index.
// db_stmt_count returns how many live statements exist.
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
int db_bind_double(arkilian *db, int idx, double val);

#ifdef __cplusplus
}
#endif

#endif
