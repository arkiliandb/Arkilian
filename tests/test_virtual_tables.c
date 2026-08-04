// Arkilian Virtual-Table Regression Test (spec §0/§1)
//
// Virtual tables (FTS5, rtree) appear in sqlite_master as type='table'
// but SQLite REJECTS `CREATE TRIGGER ... ON <virtual table>`. Before
// this test existed, any schema containing a virtual table made
// sync_backup_triggers fail and db_init refuse to start — the game
// could not boot with backup "broken". The scan must skip virtual and
// shadow tables so the game always starts; only real tables get capture
// triggers.
//
// Compile (must enable FTS5 — it ships inside the amalgamation):
//   cc tests/test_virtual_tables.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread \
//      -DSQLITE_ENABLE_FTS5 -o test_virtual_tables

#include "class.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void cleanup(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
}

static void test_fts5_virtual_table_does_not_break_init(void) {
  cleanup("test_fts.db");
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_API_KEY", "test-key", 1);
  setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  setenv("ARKILIAN_CONTROL_URL", "http://127.0.0.1:1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_fts.db") == 0);

  // The killer schema: an FTS5 virtual table + shadow tables.
  int rc = db_exec(db, "CREATE VIRTUAL TABLE docs USING fts5(title, body)");
  assert(rc == SQLITE_OK);

  // Game still works: plain-table DDL + writes + FTS writes.
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO t (v) VALUES ('x')") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO docs (title, body) VALUES ('hello', 'world')") == SQLITE_OK);

  // Coverage counts only the real table (1 table -> 3 triggers).
  assert(db_backup_trigger_coverage(db) == 0);

  // Real tables still capture; the virtual table never gets triggers
  // (SQLite would reject them).
  assert(db_wal_pending(db) >= 3);
  db_prepare(db, "SELECT COUNT(*) FROM sqlite_master "
                 "WHERE type='trigger' AND name LIKE 'trg\\_%' ESCAPE '\\'");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 3); // only t's triggers, no docs triggers
  db_finalize(db);

  // FTS content remains queryable.
  db_prepare(db, "SELECT COUNT(*) FROM docs WHERE docs MATCH 'hello'");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 1);
  db_finalize(db);

  db_close(db);
  cleanup("test_fts.db");
}

int main(void) {
  printf("=== Arkilian Virtual-Table Tests ===\n\n");
  printf("  [01] %-52s ", "test_fts5_virtual_table_does_not_break_init");
  test_fts5_virtual_table_does_not_break_init();
  printf("PASS\n");
  printf("\n=== Results: 1/1 passed ===\n");
  return 0;
}
