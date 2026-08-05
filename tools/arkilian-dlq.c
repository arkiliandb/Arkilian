// Arkilian Dead-Letter Queue Tool — inspect and replay _dead_backup rows.
//
// Every row in _dead_backup is a captured change that failed to ship
// after MAX_ATTEMPTS retries. Before replaying, resolve why the
// destination rejected it (destination down, auth, invalid payload).
//
// Usage:
//   arkilian-dlq <db.sqlite> --count
//   arkilian-dlq <db.sqlite> --list [--limit N] [--id N]
//   arkilian-dlq <db.sqlite> --replay [--id N] [--dry-run]
//
// Replay moves rows back into _pending_backup (original ids preserved,
// attempts reset to 0) so the running flush thread re-ships them — or a
// fresh process picks them up on next start. Idempotent: rows already
// present in _pending_backup are skipped, and only successfully
// re-queued rows are removed from _dead_backup.
//
// Compile (macOS/Linux):
//   cc tools/arkilian-dlq.c src/deps/sqlite/sqlite3.c \
//      -Isrc/deps/sqlite -o arkilian-dlq

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

static void usage(void) {
  fprintf(stderr,
      "usage: arkilian-dlq <db.sqlite> --count\n"
      "       arkilian-dlq <db.sqlite> --list [--limit N] [--id N]\n"
      "       arkilian-dlq <db.sqlite> --replay [--id N] [--dry-run]\n");
  exit(2);
}

static int open_db(const char *path, sqlite3 **db, int readonly) {
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  if (readonly) flags = SQLITE_OPEN_READONLY;
  int rc = sqlite3_open_v2(path, db, flags, NULL);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "cannot open %s: %s\n", path, sqlite3_errmsg(*db));
    return 1;
  }
  return 0;
}

static int has_outbox_tables(sqlite3 *db) {
  sqlite3_stmt *st = NULL;
  int ok = 0;
  if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN "
        "('_pending_backup','_dead_backup')", -1, &st, NULL) == SQLITE_OK) {
    if (sqlite3_step(st) == SQLITE_ROW) ok = sqlite3_column_int(st, 0) == 2;
    sqlite3_finalize(st);
  }
  return ok;
}

static int cmd_count(const char *path) {
  sqlite3 *db = NULL;
  if (open_db(path, &db, 1)) return 1;
  if (!has_outbox_tables(db)) {
    fprintf(stderr, "no backup outbox tables in %s\n", path);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_stmt *st = NULL;
  int n = -1;
  if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _dead_backup", -1, &st, NULL) == SQLITE_OK &&
      sqlite3_step(st) == SQLITE_ROW) {
    n = sqlite3_column_int(st, 0);
  }
  sqlite3_finalize(st);
  sqlite3_close(db);
  printf("%d\n", n);
  return n < 0;
}

static int cmd_list(const char *path, int limit, long long only_id) {
  sqlite3 *db = NULL;
  if (open_db(path, &db, 1)) return 1;
  if (!has_outbox_tables(db)) {
    fprintf(stderr, "no backup outbox tables in %s\n", path);
    sqlite3_close(db);
    return 1;
  }
  const char *sql =
      "SELECT id, attempts, failed_reason, created_at, dead_lettered_at, payload "
      "FROM _dead_backup WHERE 1=1";
  char extra[128] = "";
  if (only_id > 0) snprintf(extra, sizeof(extra), " AND id = %lld", only_id);
  char limit_clause[64] = "";
  if (limit > 0) snprintf(limit_clause, sizeof(limit_clause), " LIMIT %d", limit);
  char *full = sqlite3_mprintf("%s%s%s", sql, extra, limit_clause);

  sqlite3_stmt *st = NULL;
  int rc = sqlite3_prepare_v2(db, full, -1, &st, NULL);
  sqlite3_free(full);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "query failed: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }
  printf("%-8s %-9s %-24s %-12s %-12s  %s\n",
         "id", "attempts", "failed_reason", "created_at", "dead_at", "payload");
  while (sqlite3_step(st) == SQLITE_ROW) {
    const char *reason = (const char *)sqlite3_column_text(st, 2);
    const char *payload = (const char *)sqlite3_column_text(st, 5);
    printf("%-8lld %-9d %-24.24s %-12lld %-12lld  %.80s\n",
           sqlite3_column_int64(st, 0), sqlite3_column_int(st, 1),
           reason ? reason : "(null)", sqlite3_column_int64(st, 3),
           sqlite3_column_int64(st, 4), payload ? payload : "(null)");
  }
  sqlite3_finalize(st);
  sqlite3_close(db);
  return 0;
}

static int cmd_replay(const char *path, long long only_id, int dry_run) {
  sqlite3 *db = NULL;
  if (open_db(path, &db, 0)) return 1;
  if (!has_outbox_tables(db)) {
    fprintf(stderr, "no backup outbox tables in %s\n", path);
    sqlite3_close(db);
    return 1;
  }

  // Two filter fragments: the replay SELECT has no WHERE clause, so it
  // needs the "WHERE" form; count_sql and cleanup_sql already have a
  // WHERE clause, so they take the "AND" form. Using "AND" for the replay
  // SELECT produced "FROM _dead_backup AND id = ?" — a SQL syntax error
  // that broke `--replay --id N` entirely (regression caught at launch
  // verification). Verified before/after with the test_dlq suite.
  const char *where_replay = only_id > 0 ? " WHERE id = ?" : "";
  const char *where_extra  = only_id > 0 ? " AND id = ?" : "";
  char *count_sql = sqlite3_mprintf(
      "SELECT COUNT(*) FROM _dead_backup "
      "WHERE id NOT IN (SELECT id FROM _pending_backup) %s", where_extra);

  if (dry_run) {
    sqlite3_stmt *cst = NULL;
    int would = 0;
    int rc = sqlite3_prepare_v2(db, count_sql, -1, &cst, NULL);
    if (rc == SQLITE_OK && only_id > 0) sqlite3_bind_int64(cst, 1, only_id);
    if (rc == SQLITE_OK && sqlite3_step(cst) == SQLITE_ROW) would = sqlite3_column_int(cst, 0);
    sqlite3_finalize(cst);
    sqlite3_free(count_sql);
    printf("%d row(s) would be replayed\n", would);
    sqlite3_close(db);
    return 0;
  }
  sqlite3_free(count_sql);

  char *replay_sql = sqlite3_mprintf(
      "INSERT OR IGNORE INTO _pending_backup (id, payload, attempts, created_at) "
      "SELECT id, payload, 0, created_at FROM _dead_backup%s", where_replay);
  char *cleanup_sql = sqlite3_mprintf(
      "DELETE FROM _dead_backup WHERE id IN ("
      "  SELECT id FROM _pending_backup WHERE id IN (SELECT id FROM _dead_backup)) %s",
      where_extra);

  sqlite3_stmt *st = NULL;
  int rc;
  if (only_id > 0) {
    rc = sqlite3_prepare_v2(db, replay_sql, -1, &st, NULL);
    if (rc == SQLITE_OK) sqlite3_bind_int64(st, 1, only_id);
  } else {
    rc = sqlite3_prepare_v2(db, replay_sql, -1, &st, NULL);
  }
  if (rc != SQLITE_OK) {
    fprintf(stderr, "replay prepare failed: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }
  int replayed = 0;
  if (sqlite3_step(st) == SQLITE_DONE) replayed = sqlite3_changes(db);
  sqlite3_finalize(st);

  if (only_id > 0) {
    rc = sqlite3_prepare_v2(db, cleanup_sql, -1, &st, NULL);
    if (rc == SQLITE_OK) sqlite3_bind_int64(st, 1, only_id);
  } else {
    rc = sqlite3_prepare_v2(db, cleanup_sql, -1, &st, NULL);
  }
  int removed = 0;
  if (rc == SQLITE_OK && sqlite3_step(st) == SQLITE_DONE) removed = sqlite3_changes(db);
  sqlite3_finalize(st);

  printf("replayed %d row(s) to _pending_backup, removed %d from _dead_backup\n",
         replayed, removed);
  sqlite3_close(db);
  return 0;
}

int main(int argc, char **argv) {
  if (argc < 3) usage();
  const char *path = argv[1];
  const char *cmd = argv[2];
  int limit = 0;
  long long only_id = 0;
  int dry_run = 0;

  for (int i = 3; i < argc; i++) {
    if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) limit = atoi(argv[++i]);
    else if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) only_id = atoll(argv[++i]);
    else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
    else usage();
  }

  if (strcmp(cmd, "--count") == 0) return cmd_count(path);
  if (strcmp(cmd, "--list") == 0) return cmd_list(path, limit, only_id);
  if (strcmp(cmd, "--replay") == 0) return cmd_replay(path, only_id, dry_run);
  usage();
  return 2;
}
