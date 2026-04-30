// Arkilian SQLite Wrapper - Usage Example
#include "class.h"
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>

int main(void) {
  arkilian *db = NULL;

  if (db_init(&db, "app.sqlite") != 0) {
    fprintf(stderr, "Failed to open database: %s\n",
           db ? db_errmsg(db) : "Memory allocation failed");
    if (db)
      db_close(db);
    return 1;
  }

  sqlite3 *raw = db_get_handle(db);

  const char *create_sql =
      "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);";
  char *err_msg = NULL;
  if (sqlite3_exec(raw, create_sql, 0, 0, &err_msg) != SQLITE_OK) {
    fprintf(stderr, "Failed to create table: %s\n", err_msg);
    sqlite3_free(err_msg);
    db_close(db);
    return 1;
  }

  sqlite3_stmt *stmt;
  const char *sql = "INSERT INTO users (name) VALUES (?);";

  if (sqlite3_prepare_v2(raw, sql, -1, &stmt, 0) != SQLITE_OK) {
    fprintf(stderr, "Prepare failed: %s\n", db_errmsg(db));
    db_close(db);
    return 1;
  }

  sqlite3_bind_text(stmt, 1, "Bob", -1, SQLITE_STATIC);
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    fprintf(stderr, "Insert failed: %s\n", db_errmsg(db));
  } else {
    printf("Inserted Bob successfully!\n");
  }
  
  sqlite3_finalize(stmt);
  db_close(db);

  return 0;
}
