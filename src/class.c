// Arkilian SQLite Wrapper — Production Realtime Backup Engine
// Implements realtime SQL trigger capture, non-blocking wake signals,
// and dedicated background WAL shipping.

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif

#include "class.h"
#include <curl/curl.h>

#ifdef _WIN32
#include <windows.h>
#define strncasecmp _strnicmp
#define strdup _strdup
#else
#include <pthread.h>
#include <strings.h>
#include <unistd.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <time.h>

#include "deps/sqlite/sqlite3.h"

// ── Config Defaults ─────────────────────────────────────────────────

#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600
#define DEFAULT_SIGNED_URL_ENDPOINT "https://api.arkilian.com/get-signed-url"

#define BATCH_SIZE 100
#define MAX_ATTEMPTS 10
#define POLL_INTERVAL_MS 2000

// ── Struct Definitions ──────────────────────────────────────────────

struct arkilian {
  sqlite3 *handle;            // Primary connection (game / application thread)
  sqlite3 *backup_db;         // Dedicated secondary connection (backup thread)
  char *db_path;
  int is_open;
  int last_error_code;
  char last_error_msg[256];

  // Statement pool for caller
  sqlite3_stmt **stmts;
  int stmt_count;
  int stmt_capacity;
  int stmt_current;

  // Configuration
  char *backup_path;
  char *signed_url_endpoint;
  char *database_token;
  int backup_interval;
  int backup_enabled;

  // Background thread tracking & synchronization
  volatile int shutdown_requested;
#ifdef _WIN32
  HANDLE backup_thread_handle;
  HANDLE flush_thread_handle;
  CRITICAL_SECTION wake_mutex;
  CONDITION_VARIABLE wake_cond;
#else
  pthread_t backup_thread_id;
  int backup_thread_running;
  pthread_t flush_thread_id;
  int flush_thread_running;
  pthread_mutex_t wake_mutex;
  pthread_cond_t wake_cond;
#endif

  volatile int wake_flag;
  char last_shipped_payload[1024];
  char wal_last_buf[1024];
#ifdef _WIN32
  CRITICAL_SECTION payload_mutex;
#else
  pthread_mutex_t payload_mutex;
#endif

  // Transaction state tracking
  int in_batch_txn;
  sqlite3_stmt *begin_stmt;
  sqlite3_stmt *commit_stmt;
  sqlite3_stmt *rollback_stmt;
};

// ── Helper Prototypes ───────────────────────────────────────────────

static void load_env(void);
static const char *get_env_default(const char *env_var, const char *default_val);
static int get_env_int_default(const char *env_var, int default_val);
#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg);
DWORD WINAPI run_wal_flush(LPVOID arg);
#else
void *run_hourly_backup(void *arg);
void *run_wal_flush(void *arg);
#endif
static size_t curl_discard_cb(void *data, size_t sz, size_t nmemb, void *userp);

// ── Environment Loader ──────────────────────────────────────────────

static const char *get_env_default(const char *env_var, const char *default_val) {
  const char *val = getenv(env_var);
  return (val && strlen(val) > 0) ? val : default_val;
}

static int get_env_int_default(const char *env_var, int default_val) {
  const char *val = getenv(env_var);
  if (val && strlen(val) > 0) return atoi(val);
  return default_val;
}

static void load_env(void) {
  FILE *fp = fopen(".env", "r");
  if (!fp) return;
  char line[256];
  while (fgets(line, sizeof(line), fp)) {
    // Discard the remainder of overlong lines so a truncated
    // fragment is never parsed as a separate KEY=VALUE pair.
    size_t len = strlen(line);
    if (len > 0 && line[len - 1] != '\n' && !feof(fp)) {
      int c;
      while ((c = fgetc(fp)) != EOF && c != '\n') { /* drain */ }
    }
    char *key = strtok(line, "=");
    char *val = strtok(NULL, "\n\r");
    if (key && val) {
#ifdef _WIN32
      _putenv_s(key, val);
#else
      setenv(key, val, 1);
#endif
    }
  }
  fclose(fp);
}

// ── Small Shared Helpers ────────────────────────────────────────────

// Escape a string for embedding inside a SQL single-quoted literal
// (doubles every single quote).  Caller frees.
static char *sql_literal_escape(const char *s) {
  size_t len = 0;
  for (const char *p = s; *p; p++) len += (*p == '\'') ? 2 : 1;
  char *out = malloc(len + 1);
  if (!out) return NULL;
  char *w = out;
  for (const char *p = s; *p; p++) {
    if (*p == '\'') *w++ = '\'';
    *w++ = *p;
  }
  *w = '\0';
  return out;
}

// Skip leading whitespace and SQL comments so DDL verbs are detected
// even when the statement doesn't start at column 0.
static const char *skip_sql_prefix(const char *sql) {
  for (;;) {
    while (*sql && isspace((unsigned char)*sql)) sql++;
    if (sql[0] == '-' && sql[1] == '-') {
      while (*sql && *sql != '\n') sql++;
      continue;
    }
    if (sql[0] == '/' && sql[1] == '*') {
      const char *e = strstr(sql + 2, "*/");
      sql = e ? e + 2 : sql + strlen(sql);
      continue;
    }
    return sql;
  }
}

// Pre-signed object-storage URLs carry their own credentials in the
// query string — attaching our bearer token both leaks the credential
// to the storage host and breaks signature validation.
static int url_is_presigned(const char *url) {
  if (!url) return 0;
  return strstr(url, "X-Amz-Signature=") != NULL ||
         strstr(url, "X-Amz-Credential=") != NULL ||
         strstr(url, "X-Goog-Signature=") != NULL ||
         strstr(url, "X-Goog-Credential=") != NULL ||
         strstr(url, "sig=") != NULL; /* Azure SAS */
}

// ── Trigger Auto-Generator ──────────────────────────────────────────

static const char *RESERVED_TABLES[] = {
    "_pending_backup", "_dead_backup", "_arkilian_meta", "sqlite_sequence", NULL
};

static int is_reserved_table(const char *name) {
  if (!name) return 1;
  if (strncmp(name, "sqlite_", 7) == 0) return 1;
  for (int i = 0; RESERVED_TABLES[i]; i++) {
    if (strcmp(name, RESERVED_TABLES[i]) == 0) return 1;
  }
  return 0;
}

// Returns 1 if the named table is WITHOUT ROWID, 0 otherwise (including
// when the check itself fails — older SQLite lacks pragma_table_list).
static int table_is_without_rowid(sqlite3 *db, const char *table) {
  int wr = 0;
  char *q = sqlite3_mprintf(
      "SELECT wr FROM pragma_table_list WHERE name = %Q", table);
  if (!q) return 0;
  sqlite3_stmt *st = NULL;
  if (sqlite3_prepare_v2(db, q, -1, &st, NULL) == SQLITE_OK) {
    if (sqlite3_step(st) == SQLITE_ROW) wr = sqlite3_column_int(st, 0);
  }
  sqlite3_finalize(st);
  sqlite3_free(q);
  return wr;
}

int sync_backup_triggers(sqlite3 *db, char **err_out) {
  if (!db) return SQLITE_ERROR;
  int rc;
  char *errmsg = NULL;
  int began = 0;

  // Only open our own transaction when the connection is in autocommit
  // mode.  When the caller already holds a transaction (e.g. db_begin
  // batch), join it and leave commit/rollback to the caller.
  if (sqlite3_get_autocommit(db)) {
    rc = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
      if (err_out) *err_out = errmsg;
      else sqlite3_free(errmsg);
      return rc;
    }
    began = 1;
  }

  // Ensure internal outbox & metadata tables exist
  static const char *const kInternalDDL[] = {
    "CREATE TABLE IF NOT EXISTS _pending_backup ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  payload TEXT NOT NULL,"
    "  attempts INTEGER NOT NULL DEFAULT 0,"
    "  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),"
    "  last_attempt_at INTEGER"
    ");",
    "CREATE TABLE IF NOT EXISTS _dead_backup ("
    "  id INTEGER PRIMARY KEY,"
    "  payload TEXT NOT NULL,"
    "  attempts INTEGER NOT NULL,"
    "  failed_reason TEXT,"
    "  created_at INTEGER NOT NULL,"
    "  dead_lettered_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))"
    ");",
    "CREATE TABLE IF NOT EXISTS _arkilian_meta ("
    "  k TEXT PRIMARY KEY,"
    "  v TEXT"
    ");",
    NULL
  };
  for (int i = 0; kInternalDDL[i]; i++) {
    rc = sqlite3_exec(db, kInternalDDL[i], NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
      if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
      if (err_out) *err_out = errmsg;
      else sqlite3_free(errmsg);
      return rc;
    }
  }

  sqlite3_stmt *table_stmt = NULL;
  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM sqlite_master WHERE type = 'table'", -1, &table_stmt, NULL);
  if (rc != SQLITE_OK) {
    if (err_out) *err_out = sqlite3_mprintf("prepare table list: %s", sqlite3_errmsg(db));
    if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return rc;
  }

  while ((rc = sqlite3_step(table_stmt)) == SQLITE_ROW) {
    const char *table = (const char *)sqlite3_column_text(table_stmt, 0);
    if (!table || is_reserved_table(table)) continue;

    int without_rowid = table_is_without_rowid(db, table);

    char *pragma_sql = sqlite3_mprintf("PRAGMA table_xinfo(\"%w\");", table);
    if (!pragma_sql) { rc = SQLITE_NOMEM; goto oom; }
    sqlite3_stmt *col_stmt = NULL;
    rc = sqlite3_prepare_v2(db, pragma_sql, -1, &col_stmt, NULL);
    sqlite3_free(pragma_sql);

    if (rc != SQLITE_OK) {
      // Build the message BEFORE finalizing table_stmt — `table` points
      // into table_stmt's row buffer and is invalid after finalize.
      if (err_out) *err_out = sqlite3_mprintf("prepare table_info(%s): %s", table, sqlite3_errmsg(db));
      sqlite3_finalize(table_stmt);
      if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
      return rc;
    }

    // Column collection (names + primary-key ranks)
    char **cols = NULL;
    int *pk_ranks = NULL;
    int ncols = 0, cap = 0;

    while ((rc = sqlite3_step(col_stmt)) == SQLITE_ROW) {
      const char *col = (const char *)sqlite3_column_text(col_stmt, 1);
      if (!col) continue;
      // table_xinfo's 7th column flags hidden (1) and generated (2/3)
      // columns — those can never appear in INSERT/REPLACE column lists,
      // so capturing them would produce payloads that fail on replay.
      if (sqlite3_column_count(col_stmt) > 6 &&
          sqlite3_column_int(col_stmt, 6) != 0) continue;
      if (ncols == cap) {
        int ncap = cap ? cap * 2 : 8;
        char **nc = realloc(cols, (size_t)ncap * sizeof(char *));
        int *nr = realloc(pk_ranks, (size_t)ncap * sizeof(int));
        // A failed realloc leaves the original block intact; a successful
        // one moved it — adopt each result independently to stay consistent.
        if (nc) cols = nc;
        if (nr) pk_ranks = nr;
        if (!nc || !nr) {
          sqlite3_finalize(col_stmt);
          goto oom;
        }
        cap = ncap;
      }
      cols[ncols] = strdup(col);
      pk_ranks[ncols] = sqlite3_column_int(col_stmt, 5);
      if (!cols[ncols]) { sqlite3_finalize(col_stmt); goto oom; }
      ncols++;
    }
    sqlite3_finalize(col_stmt);
    if (rc != SQLITE_DONE) goto fail;

    if (ncols == 0) goto next_table;

    // Raw identifier list: "c1", "c2"  and  NEW-value expression:
    // quote(NEW."c1") || ', ' || quote(NEW."c2")
    char *raw_cols = NULL;
    char *new_vals = NULL;
    for (int i = 0; i < ncols; i++) {
      char *next_cols, *next_vals;
      if (i == 0) {
        next_cols = sqlite3_mprintf("\"%w\"", cols[i]);
        next_vals = sqlite3_mprintf("quote(NEW.\"%w\")", cols[i]);
      } else {
        next_cols = sqlite3_mprintf("%s, \"%w\"", raw_cols, cols[i]);
        next_vals = sqlite3_mprintf("%s || ', ' || quote(NEW.\"%w\")", new_vals, cols[i]);
      }
      if (!next_cols || !next_vals) {
        sqlite3_free(next_cols); sqlite3_free(next_vals);
        goto oom;
      }
      sqlite3_free(raw_cols); sqlite3_free(new_vals);
      raw_cols = next_cols; new_vals = next_vals;
    }

    // Payload SQL texts.  Literals get a second escape pass so single
    // quotes inside identifiers survive embedding in the trigger's
    // string literal; expressions stay raw.
    char *replace_lit_raw = sqlite3_mprintf("REPLACE INTO \"%w\" (%s) VALUES (", table, raw_cols);
    char *delete_prefix_raw = sqlite3_mprintf("DELETE FROM \"%w\" WHERE ", table);
    char *replace_lit = sql_literal_escape(replace_lit_raw ? replace_lit_raw : "");
    char *delete_prefix = sql_literal_escape(delete_prefix_raw ? delete_prefix_raw : "");
    sqlite3_free(replace_lit_raw); sqlite3_free(delete_prefix_raw);
    if (!replace_lit || !delete_prefix) { free(replace_lit); free(delete_prefix); goto oom; }

    // WITHOUT ROWID tables have no rowid — the DELETE payload must
    // match on primary-key columns instead of OLD.rowid.
    char *delete_expr = NULL;
    if (!without_rowid) {
      delete_expr = sqlite3_mprintf("'%srowid = ' || OLD.rowid", delete_prefix);
    } else {
      int pk_seen = 0;
      char *lit_accum = strdup(delete_prefix);
      if (!lit_accum) goto oom;
      for (int i = 0; i < ncols; i++) {
        if (pk_ranks[i] == 0) continue;
        char *piece = sqlite3_mprintf(pk_seen == 0 ? "\"%w\" = " : " AND \"%w\" = ", cols[i]);
        if (!piece) { free(lit_accum); goto oom; }
        char *new_accum = malloc(strlen(lit_accum) + strlen(piece) + 1);
        if (!new_accum) { sqlite3_free(piece); free(lit_accum); goto oom; }
        strcpy(new_accum, lit_accum);
        strcat(new_accum, piece);
        free(lit_accum); sqlite3_free(piece);
        lit_accum = new_accum;

        char *esc = sql_literal_escape(lit_accum);
        char *expr = sqlite3_mprintf("quote(OLD.\"%w\")", cols[i]);
        if (!esc || !expr) { free(esc); sqlite3_free(expr); free(lit_accum); goto oom; }
        char *next_expr = delete_expr
          ? sqlite3_mprintf("%s || '%s' || %s", delete_expr, esc, expr)
          : sqlite3_mprintf("'%s' || %s", esc, expr);
        free(esc); sqlite3_free(expr);
        sqlite3_free(delete_expr);
        delete_expr = next_expr;
        if (!delete_expr) { free(lit_accum); goto oom; }
        lit_accum[0] = '\0';
        pk_seen++;
      }
      free(lit_accum);
      if (pk_seen == 0) {
        errmsg = sqlite3_mprintf("table %s is WITHOUT ROWID but has no PRIMARY KEY", table);
        free(replace_lit); free(delete_prefix);
        sqlite3_free(raw_cols); sqlite3_free(new_vals);
        goto fail_with_errmsg;
      }
    }
    if (!delete_expr) { free(replace_lit); free(delete_prefix); sqlite3_free(raw_cols); sqlite3_free(new_vals); goto oom; }

    {
      const char *ops[3][2] = {
          {"ai", "INSERT"}, {"au", "UPDATE"}, {"ad", "DELETE"}
      };

      for (int i = 0; i < 3; i++) {
        char *drop_sql = sqlite3_mprintf("DROP TRIGGER IF EXISTS \"trg_%w_%s\";", table, ops[i][0]);
        if (!drop_sql) goto trigger_oom;
        rc = sqlite3_exec(db, drop_sql, NULL, NULL, &errmsg);
        sqlite3_free(drop_sql);
        if (rc != SQLITE_OK) goto trigger_fail;

        char *create_sql = NULL;
        if (i == 2) {
          create_sql = sqlite3_mprintf(
              "CREATE TRIGGER \"trg_%w_ad\" AFTER DELETE ON \"%w\" BEGIN "
              "INSERT INTO _pending_backup (payload) VALUES (%s); END;",
              table, table, delete_expr);
        } else {
          create_sql = sqlite3_mprintf(
              "CREATE TRIGGER \"trg_%w_%s\" AFTER %s ON \"%w\" BEGIN "
              "INSERT INTO _pending_backup (payload) VALUES ("
              "'%s' || %s || ')'); END;",
              table, ops[i][0], ops[i][1], table, replace_lit, new_vals);
        }
        if (!create_sql) goto trigger_oom;

        rc = sqlite3_exec(db, create_sql, NULL, NULL, &errmsg);
        sqlite3_free(create_sql);
        if (rc != SQLITE_OK) goto trigger_fail;
      }
      goto triggers_done;

trigger_oom:
      rc = SQLITE_NOMEM;
trigger_fail:
      free(replace_lit); free(delete_prefix); sqlite3_free(delete_expr);
      sqlite3_free(raw_cols); sqlite3_free(new_vals);
      goto fail;
    }

triggers_done:
    free(replace_lit); free(delete_prefix); sqlite3_free(delete_expr);
    sqlite3_free(raw_cols); sqlite3_free(new_vals);

next_table:
    for (int i = 0; i < ncols; i++) free(cols[i]);
    free(cols); free(pk_ranks);
    continue;

oom:
    rc = SQLITE_NOMEM;
    if (!errmsg) errmsg = sqlite3_mprintf("out of memory");
    goto fail_with_errmsg;

fail:
    if (!errmsg) errmsg = sqlite3_mprintf("trigger sync failed (rc=%d)", rc);
fail_with_errmsg:
    for (int i = 0; i < ncols; i++) free(cols[i]);
    free(cols); free(pk_ranks);
    sqlite3_finalize(table_stmt);
    if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    if (err_out) *err_out = errmsg;
    else sqlite3_free(errmsg);
    return rc;
  }
  sqlite3_finalize(table_stmt);

  if (rc != SQLITE_DONE) {
    if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    if (err_out) *err_out = sqlite3_mprintf("table scan: %s", sqlite3_errmsg(db));
    return rc;
  }

  if (began) {
    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
      if (err_out) *err_out = errmsg;
      else sqlite3_free(errmsg);
      return rc;
    }
  }

  return SQLITE_OK;
}

// ── Wake Signal Update Hook ─────────────────────────────────────────

static void on_db_update(void *user_data, int op_type, char const *db_name,
                          char const *table_name, sqlite3_int64 row_id) {
  arkilian *db = (arkilian *)user_data;
  (void)op_type; (void)db_name; (void)row_id;
  if (!db || is_reserved_table(table_name)) return;

  // The flag and the signal must be issued under the mutex — otherwise
  // the flush thread can miss the wakeup between its predicate check
  // and pthread_cond_timedwait().
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  db->wake_flag = 1;
  pthread_cond_signal(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);
#else
  EnterCriticalSection(&db->wake_mutex);
  db->wake_flag = 1;
  WakeConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);
#endif
}

// ── Backup Shipping & Delivery Thread ───────────────────────────────

static size_t curl_discard_cb(void *data, size_t sz, size_t nmemb, void *userp) {
  (void)data; (void)userp;
  return sz * nmemb;
}

typedef enum { SHIP_OK = 0, SHIP_RETRY = 1 } ship_result_t;

static ship_result_t ship_to_backup(arkilian *db, sqlite3_int64 id, const char *payload) {
  if (!payload || strlen(payload) == 0) return SHIP_OK;

#ifndef _WIN32
  pthread_mutex_lock(&db->payload_mutex);
#else
  EnterCriticalSection(&db->payload_mutex);
#endif
  strncpy(db->last_shipped_payload, payload, sizeof(db->last_shipped_payload) - 1);
  db->last_shipped_payload[sizeof(db->last_shipped_payload) - 1] = '\0';
#ifndef _WIN32
  pthread_mutex_unlock(&db->payload_mutex);
#else
  LeaveCriticalSection(&db->payload_mutex);
#endif

  const char *push_url = getenv("ARKILIAN_WAL_PUSH_URL");
  if (!push_url || strlen(push_url) == 0) {
    push_url = db->signed_url_endpoint;
  }

  if (!push_url || strlen(push_url) == 0 || strcmp(push_url, DEFAULT_SIGNED_URL_ENDPOINT) == 0) {
    return SHIP_OK;
  }

  CURL *curl = curl_easy_init();
  if (!curl) return SHIP_RETRY;

  curl_easy_setopt(curl, CURLOPT_URL, push_url);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_discard_cb);

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/sql");
  // Idempotency key lets the receiver deduplicate retries of the same row.
  char id_header[64];
  snprintf(id_header, sizeof(id_header), "X-Arkilian-Payload-Id: %lld", (long long)id);
  headers = curl_slist_append(headers, id_header);
  // Never attach our bearer token to a pre-signed storage URL — the
  // signature IS the credential, and the token would leak to the host.
  if (db->database_token && strlen(db->database_token) > 0 && !url_is_presigned(push_url)) {
    char auth[512];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", db->database_token);
    headers = curl_slist_append(headers, auth);
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  long http_code = 0;
  if (res == CURLE_OK) {
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return (res == CURLE_OK && (http_code == 200 || http_code == 201)) ? SHIP_OK : SHIP_RETRY;
}

static int drain_batch(arkilian *db, sqlite3_stmt *select_stmt, sqlite3_stmt *delete_stmt,
                        sqlite3_stmt *update_attempts_stmt, sqlite3_stmt *dead_letter_stmt) {
  if (!db || !db->backup_db) return 0;

  sqlite3_reset(select_stmt);
  sqlite3_clear_bindings(select_stmt);
  sqlite3_bind_int(select_stmt, 1, BATCH_SIZE);

  int processed_any = 0;

  for (;;) {
    int rc = sqlite3_step(select_stmt);
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) break;

    sqlite3_int64 id = sqlite3_column_int64(select_stmt, 0);
    const unsigned char *payload = sqlite3_column_text(select_stmt, 1);
    int attempts = sqlite3_column_int(select_stmt, 2);
    if (!payload) continue;

    char *payload_copy = strdup((const char *)payload);
    if (!payload_copy) break;

    ship_result_t result = ship_to_backup(db, id, payload_copy);
    free(payload_copy);

    if (result == SHIP_OK) {
      sqlite3_reset(delete_stmt);
      sqlite3_clear_bindings(delete_stmt);
      sqlite3_bind_int64(delete_stmt, 1, id);
      sqlite3_step(delete_stmt);
      processed_any = 1;
      continue;
    }

    int new_attempts = attempts + 1;
    if (new_attempts >= MAX_ATTEMPTS) {
      // `payload` (select_stmt's column text) is still valid here —
      // select_stmt has not been stepped or reset since the read.
      fprintf(stderr,
              "arkilian: payload id=%lld dead-lettered after %d attempts "
              "(moved to _dead_backup): %.120s\n",
              (long long)id, new_attempts, (const char *)payload);
      sqlite3_reset(dead_letter_stmt);
      sqlite3_clear_bindings(dead_letter_stmt);
      sqlite3_bind_int(dead_letter_stmt, 1, new_attempts);
      sqlite3_bind_text(dead_letter_stmt, 2, "max attempts exceeded", -1, SQLITE_STATIC);
      sqlite3_bind_int64(dead_letter_stmt, 3, id);
      if (sqlite3_step(dead_letter_stmt) == SQLITE_DONE) {
        sqlite3_reset(delete_stmt);
        sqlite3_clear_bindings(delete_stmt);
        sqlite3_bind_int64(delete_stmt, 1, id);
        sqlite3_step(delete_stmt);
      }
      processed_any = 1;
      continue;
    } else {
      sqlite3_reset(update_attempts_stmt);
      sqlite3_clear_bindings(update_attempts_stmt);
      sqlite3_bind_int(update_attempts_stmt, 1, new_attempts);
      sqlite3_bind_int64(update_attempts_stmt, 2, id);
      sqlite3_step(update_attempts_stmt);
      // Back off: report "no work drained" so the flush loop waits one
      // poll interval before retrying instead of hot-spinning on a
      // failing endpoint and burning through MAX_ATTEMPTS instantly.
      processed_any = 0;
      break;
    }
  }

  return processed_any;
}

#ifdef _WIN32
DWORD WINAPI run_wal_flush(LPVOID arg) {
#else
void *run_wal_flush(void *arg) {
#endif
  arkilian *db = (arkilian *)arg;
  if (!db || !db->backup_db) {
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
  }

  sqlite3_stmt *select_stmt = NULL;
  sqlite3_stmt *delete_stmt = NULL;
  sqlite3_stmt *update_attempts_stmt = NULL;
  sqlite3_stmt *dead_letter_stmt = NULL;

  sqlite3_prepare_v2(db->backup_db,
      "SELECT id, payload, attempts FROM _pending_backup ORDER BY id LIMIT ?1",
      -1, &select_stmt, NULL);
  sqlite3_prepare_v2(db->backup_db,
      "DELETE FROM _pending_backup WHERE id = ?1",
      -1, &delete_stmt, NULL);
  sqlite3_prepare_v2(db->backup_db,
      "UPDATE _pending_backup SET attempts = ?1, last_attempt_at = strftime('%s','now') WHERE id = ?2",
      -1, &update_attempts_stmt, NULL);
  sqlite3_prepare_v2(db->backup_db,
      "INSERT INTO _dead_backup (id, payload, attempts, failed_reason, created_at) "
      "SELECT id, payload, ?1, ?2, created_at FROM _pending_backup WHERE id = ?3",
      -1, &dead_letter_stmt, NULL);

  int stmts_ok = select_stmt && delete_stmt && update_attempts_stmt && dead_letter_stmt;
  if (!stmts_ok) {
    fprintf(stderr, "arkilian: flush thread failed to prepare outbox statements "
                    "(outbox tables missing or OOM) — shipping disabled\n");
  }

  while (!db->shutdown_requested) {
    int drained = 0;
    if (stmts_ok) {
      drained = drain_batch(db, select_stmt, delete_stmt,
                            update_attempts_stmt, dead_letter_stmt);
    }

    if (!drained) {
#ifndef _WIN32
      pthread_mutex_lock(&db->wake_mutex);
      if (!db->wake_flag && !db->shutdown_requested) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += POLL_INTERVAL_MS / 1000;
        pthread_cond_timedwait(&db->wake_cond, &db->wake_mutex, &ts);
      }
      db->wake_flag = 0;
      pthread_mutex_unlock(&db->wake_mutex);
#else
      EnterCriticalSection(&db->wake_mutex);
      if (!db->wake_flag && !db->shutdown_requested) {
        SleepConditionVariableCS(&db->wake_cond, &db->wake_mutex, POLL_INTERVAL_MS);
      }
      db->wake_flag = 0;
      LeaveCriticalSection(&db->wake_mutex);
#endif
    }
  }

  if (select_stmt) sqlite3_finalize(select_stmt);
  if (delete_stmt) sqlite3_finalize(delete_stmt);
  if (update_attempts_stmt) sqlite3_finalize(update_attempts_stmt);
  if (dead_letter_stmt) sqlite3_finalize(dead_letter_stmt);

#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}

// ── Database Lifecycle: db_init / db_close ──────────────────────────

int db_init(arkilian **db_ptr, const char *filename) {
  if (!db_ptr) return 1;

  arkilian *db = malloc(sizeof(arkilian));
  if (!db) return 1;
  memset(db, 0, sizeof(arkilian));

  load_env();

  const char *path = (filename != NULL) ? filename :
    get_env_default("ARKILIAN_DB_PATH", DEFAULT_DB_PATH);

  db->db_path = malloc(strlen(path) + 1);
  if (db->db_path) strcpy(db->db_path, path);

  const char *backup_path_tmp = get_env_default("ARKILIAN_BACKUP_PATH", DEFAULT_BACKUP_PATH);
  db->backup_path = malloc(strlen(backup_path_tmp) + 1);
  if (db->backup_path) strcpy(db->backup_path, backup_path_tmp);

  const char *signed_url_tmp = getenv("ARKILIAN_WAL_PUSH_URL");
  if (!signed_url_tmp || strlen(signed_url_tmp) == 0) {
    signed_url_tmp = get_env_default("ARKILIAN_SIGNED_URL_ENDPOINT", DEFAULT_SIGNED_URL_ENDPOINT);
  }
  db->signed_url_endpoint = malloc(strlen(signed_url_tmp) + 1);
  if (db->signed_url_endpoint) strcpy(db->signed_url_endpoint, signed_url_tmp);

  const char *token_tmp = get_env_default("ARKILIAN_DATABASE_TOKEN", "");
  db->database_token = malloc(strlen(token_tmp) + 1);
  if (db->database_token) strcpy(db->database_token, token_tmp);

  db->backup_interval = get_env_int_default("ARKILIAN_BACKUP_INTERVAL", DEFAULT_BACKUP_INTERVAL);
  db->backup_enabled = get_env_int_default("ARKILIAN_ENABLE_BACKUP", 1);

#ifndef _WIN32
  pthread_mutex_init(&db->wake_mutex, NULL);
  pthread_cond_init(&db->wake_cond, NULL);
  pthread_mutex_init(&db->payload_mutex, NULL);
#else
  InitializeCriticalSection(&db->wake_mutex);
  InitializeConditionVariable(&db->wake_cond);
  InitializeCriticalSection(&db->payload_mutex);
#endif

  // Open primary connection
  int rc = sqlite3_open_v2(
      path, &db->handle,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);

  if (rc != SQLITE_OK) {
    const char *err = sqlite3_errstr(rc);
    strncpy(db->last_error_msg, err, sizeof(db->last_error_msg) - 1);
    db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
    if (db->handle) sqlite3_close(db->handle);
    db->handle = NULL;
    *db_ptr = db;
    return 1;
  }

  // Open secondary backup connection
  rc = sqlite3_open_v2(
      path, &db->backup_db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "backup connection: %s", sqlite3_errstr(rc));
    if (db->backup_db) sqlite3_close(db->backup_db);
    sqlite3_close(db->handle);
    db->handle = NULL;
    db->backup_db = NULL;
    *db_ptr = db;
    return 1;
  }

  sqlite3_busy_timeout(db->handle, 5000);
  sqlite3_busy_timeout(db->backup_db, 5000);

  sqlite3_exec(db->handle, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA cache_size=-64000;", NULL, NULL, NULL);

  sqlite3_exec(db->backup_db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
  sqlite3_exec(db->backup_db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);

  // Register non-blocking update hook
  sqlite3_update_hook(db->handle, on_db_update, db);

  // Sync backup triggers — a failure here means writes are NOT being
  // captured for backup. Surface it loudly and return error.
  char *trigger_err = NULL;
  int sync_rc = sync_backup_triggers(db->handle, &trigger_err);
  if (sync_rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "backup trigger sync failed: %s",
             trigger_err ? trigger_err : "unknown error");
    fprintf(stderr, "arkilian error: %s\n", db->last_error_msg);
    if (trigger_err) sqlite3_free(trigger_err);
    if (db->backup_db) sqlite3_close(db->backup_db);
    sqlite3_close(db->handle);
    db->handle = NULL;
    db->backup_db = NULL;
    *db_ptr = db;
    return sync_rc;
  }
  if (trigger_err) sqlite3_free(trigger_err);

  // Transaction statements
  sqlite3_prepare_v2(db->handle, "BEGIN;", -1, &db->begin_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "COMMIT;", -1, &db->commit_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "ROLLBACK;", -1, &db->rollback_stmt, NULL);
  if (!db->begin_stmt || !db->commit_stmt || !db->rollback_stmt) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "failed to prepare transaction statements: %s", sqlite3_errmsg(db->handle));
    *db_ptr = db;
    return 1;
  }

  db->is_open = 1;
  db->shutdown_requested = 0;
  *db_ptr = db;

  // Read configuration environment variables
  const char *env_backup = getenv("ARKILIAN_ENABLE_BACKUP");
  if (env_backup && (strcmp(env_backup, "1") == 0 || strcasecmp(env_backup, "true") == 0)) {
    db->backup_enabled = 1;
  }
  const char *env_url = getenv("ARKILIAN_WAL_PUSH_URL");
  if (env_url && strlen(env_url) > 0) {
    if (db->signed_url_endpoint) free(db->signed_url_endpoint);
    db->signed_url_endpoint = strdup(env_url);
  }
  const char *env_token = getenv("ARKILIAN_DATABASE_TOKEN");
  if (env_token && strlen(env_token) > 0) {
    db_set_token(db, env_token);
  }
  const char *env_interval = getenv("ARKILIAN_BACKUP_INTERVAL");
  if (env_interval && atoi(env_interval) > 0) {
    db->backup_interval = atoi(env_interval);
  }

  // Start WAL flusher thread
#ifndef _WIN32
  db->flush_thread_running = 0;
  if (pthread_create(&db->flush_thread_id, NULL, run_wal_flush, db) == 0)
    db->flush_thread_running = 1;
#else
  db->flush_thread_handle = CreateThread(NULL, 0, run_wal_flush, db, 0, NULL);
#endif

  // Start backup thread
  if (db->backup_enabled) {
#ifdef _WIN32
    db->backup_thread_handle = CreateThread(NULL, 0, run_hourly_backup, db, 0, NULL);
#else
    db->backup_thread_running = 0;
    if (pthread_create(&db->backup_thread_id, NULL, run_hourly_backup, db) == 0)
      db->backup_thread_running = 1;
#endif
  }

  return 0;
}

void db_close(arkilian *db) {
  if (!db) return;

  // Wake BOTH sleeper threads (flush + hourly backup) under the mutex
  // so neither can miss the shutdown signal.
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  db->shutdown_requested = 1;
  db->wake_flag = 1;
  pthread_cond_broadcast(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);

  if (db->flush_thread_running) {
    pthread_join(db->flush_thread_id, NULL);
    db->flush_thread_running = 0;
  }
  if (db->backup_thread_running) {
    pthread_join(db->backup_thread_id, NULL);
    db->backup_thread_running = 0;
  }
  pthread_mutex_destroy(&db->wake_mutex);
  pthread_cond_destroy(&db->wake_cond);
  pthread_mutex_destroy(&db->payload_mutex);
#else
  EnterCriticalSection(&db->wake_mutex);
  db->shutdown_requested = 1;
  db->wake_flag = 1;
  WakeAllConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);

  if (db->flush_thread_handle) {
    WaitForSingleObject(db->flush_thread_handle, INFINITE);
    CloseHandle(db->flush_thread_handle);
    db->flush_thread_handle = NULL;
  }
  if (db->backup_thread_handle) {
    WaitForSingleObject(db->backup_thread_handle, INFINITE);
    CloseHandle(db->backup_thread_handle);
    db->backup_thread_handle = NULL;
  }
  DeleteCriticalSection(&db->wake_mutex);
  DeleteCriticalSection(&db->payload_mutex);
#endif

  for (int i = 0; i < db->stmt_count; i++) {
    if (db->stmts && db->stmts[i]) sqlite3_finalize(db->stmts[i]);
  }
  if (db->stmts) free(db->stmts);

  if (db->begin_stmt) sqlite3_finalize(db->begin_stmt);
  if (db->commit_stmt) sqlite3_finalize(db->commit_stmt);
  if (db->rollback_stmt) sqlite3_finalize(db->rollback_stmt);

  if (db->backup_db) {
    sqlite3_close(db->backup_db);
    db->backup_db = NULL;
  }

  if (db->handle) {
    sqlite3_close(db->handle);
    db->handle = NULL;
  }

  if (db->db_path) free(db->db_path);
  if (db->backup_path) free(db->backup_path);
  if (db->signed_url_endpoint) free(db->signed_url_endpoint);
  if (db->database_token) free(db->database_token);

  free(db);
}

// ── Query Execution ─────────────────────────────────────────────────

const char *db_errmsg(arkilian *db) {
  if (!db) return "Invalid database handle";
  if (db->last_error_msg[0] != '\0') return db->last_error_msg;
  if (db->handle) return sqlite3_errmsg(db->handle);
  return "Unknown error";
}

sqlite3 *db_get_handle(arkilian *db) { return db ? db->handle : NULL; }

int db_exec(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql) return SQLITE_ERROR;

  char *errmsg = NULL;
  int rc = sqlite3_exec(db->handle, sql, NULL, NULL, &errmsg);
  if (rc != SQLITE_OK) {
    if (errmsg) {
      strncpy(db->last_error_msg, errmsg, sizeof(db->last_error_msg) - 1);
      db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
      sqlite3_free(errmsg);
    } else {
      snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s", sqlite3_errmsg(db->handle));
    }
    return rc;
  }

  const char *sql_verb = skip_sql_prefix(sql);
  if (strncasecmp(sql_verb, "CREATE", 6) == 0 ||
      strncasecmp(sql_verb, "ALTER", 5) == 0 ||
      strncasecmp(sql_verb, "DROP", 4) == 0) {
    char *terr = NULL;
    int sync_rc = sync_backup_triggers(db->handle, &terr);
    if (sync_rc != SQLITE_OK) {
      // The DDL succeeded but its table is not captured for backup —
      // make the failure visible instead of silent.
      snprintf(db->last_error_msg, sizeof(db->last_error_msg),
               "backup trigger sync failed after DDL: %s", terr ? terr : "unknown error");
      fprintf(stderr, "arkilian: %s\n", db->last_error_msg);
    }
    if (terr) sqlite3_free(terr);

    sqlite3_stmt *ddl_stmt = NULL;
    if (sqlite3_prepare_v2(db->handle, "INSERT INTO _pending_backup (payload) VALUES (?)", -1, &ddl_stmt, NULL) == SQLITE_OK) {
      sqlite3_bind_text(ddl_stmt, 1, sql, -1, SQLITE_TRANSIENT);
      sqlite3_step(ddl_stmt);
      sqlite3_finalize(ddl_stmt);
    }
  }

  return SQLITE_DONE;
}

int db_prepare(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql) return SQLITE_ERROR;

  if (db->stmt_count >= db->stmt_capacity) {
    int new_cap = (db->stmt_capacity == 0) ? 8 : db->stmt_capacity * 2;
    sqlite3_stmt **new_arr = realloc(db->stmts, (size_t)new_cap * sizeof(sqlite3_stmt *));
    if (!new_arr) return SQLITE_NOMEM;
    db->stmts = new_arr;
    db->stmt_capacity = new_cap;
  }

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db->handle, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s", sqlite3_errmsg(db->handle));
    return rc;
  }
  if (!stmt) {
    // Empty/whitespace-only SQL prepares OK but yields a NULL statement —
    // storing it would create a ghost slot in the pool.
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "empty SQL statement");
    return SQLITE_ERROR;
  }

  db->stmts[db->stmt_count] = stmt;
  db->stmt_current = db->stmt_count;
  db->stmt_count++;
  return rc;
}

int db_use_stmt(arkilian *db, int index) {
  if (!db || index < 0 || index >= db->stmt_count) return SQLITE_ERROR;
  if (!db->stmts[index]) return SQLITE_ERROR;
  db->stmt_current = index;
  return SQLITE_OK;
}

int db_stmt_count(arkilian *db) {
  return db ? db->stmt_count : 0;
}

static sqlite3_stmt *get_current_stmt(arkilian *db) {
  if (!db || db->stmt_current < 0 || db->stmt_current >= db->stmt_count) return NULL;
  return db->stmts[db->stmt_current];
}

int db_step(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_step(stmt);
}

int db_finalize(arkilian *db) {
  if (!db) return SQLITE_ERROR;
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (stmt) {
    sqlite3_finalize(stmt);
    db->stmts[db->stmt_current] = NULL;
  }
  return SQLITE_OK;
}

int db_reset(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_reset(stmt);
}

// ── Column & Binding Accessors ──────────────────────────────────────

int db_column_count(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_count(stmt) : 0;
}

const char *db_column_name(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? (const char *)sqlite3_column_name(stmt, col) : NULL;
}

const char *db_column_text(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? (const char *)sqlite3_column_text(stmt, col) : NULL;
}

int db_column_int(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_int(stmt, col) : 0;
}

double db_column_double(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_double(stmt, col) : 0.0;
}

sqlite3_int64 db_column_int64(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_int64(stmt, col) : 0;
}

int db_column_type(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_type(stmt, col) : SQLITE_NULL;
}

const void *db_column_blob(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_blob(stmt, col) : NULL;
}

int db_column_bytes(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_bytes(stmt, col) : 0;
}

int db_bind_text(arkilian *db, int idx, const char *val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt || !val) return SQLITE_ERROR;
  return sqlite3_bind_text(stmt, idx, val, -1, SQLITE_TRANSIENT);
}

int db_bind_int(arkilian *db, int idx, int val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_int(stmt, idx, val) : SQLITE_ERROR;
}

int db_bind_int64(arkilian *db, int idx, sqlite3_int64 val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_int64(stmt, idx, val) : SQLITE_ERROR;
}

int db_bind_double(arkilian *db, int idx, double val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_double(stmt, idx, val) : SQLITE_ERROR;
}

int db_bind_null(arkilian *db, int idx) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_null(stmt, idx) : SQLITE_ERROR;
}

int db_changes(arkilian *db) {
  return (db && db->handle) ? sqlite3_changes(db->handle) : 0;
}

sqlite3_int64 db_last_insert_rowid(arkilian *db) {
  return (db && db->handle) ? sqlite3_last_insert_rowid(db->handle) : 0;
}

int db_set_token(arkilian *db, const char *token) {
  if (!db || !token) return 1;
  if (db->database_token) free(db->database_token);
  db->database_token = malloc(strlen(token) + 1);
  if (!db->database_token) return 1;
  strcpy(db->database_token, token);
  return 0;
}

// ── Transaction Control ─────────────────────────────────────────────

int db_begin(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (db->in_batch_txn) return SQLITE_BUSY;
  int rc = sqlite3_step(db->begin_stmt);
  sqlite3_reset(db->begin_stmt);
  if (rc == SQLITE_DONE) {
    db->in_batch_txn = 1;
    return SQLITE_OK;
  }
  return rc;
}

int db_commit(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (!db->in_batch_txn) return SQLITE_ERROR;
  int rc = sqlite3_step(db->commit_stmt);
  sqlite3_reset(db->commit_stmt);
  db->in_batch_txn = 0;
  return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

int db_rollback(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (!db->in_batch_txn) return SQLITE_ERROR;
  sqlite3_step(db->rollback_stmt);
  sqlite3_reset(db->rollback_stmt);
  db->in_batch_txn = 0;
  return SQLITE_OK;
}

// ── Introspection & Diagnostics ─────────────────────────────────────

int db_wal_pending(arkilian *db) {
  if (!db || !db->handle) return 0;
  sqlite3_stmt *stmt = NULL;
  int count = 0;
  if (sqlite3_prepare_v2(db->handle, "SELECT COUNT(*) FROM _pending_backup", -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
  }
  return count;
}

const char *db_wal_last_sql(arkilian *db) {
  if (!db || !db->handle) return NULL;
  // Per-instance buffer — a static buffer would race and leak data
  // across database instances.
  db->wal_last_buf[0] = '\0';
  sqlite3_stmt *stmt = NULL;
  if (sqlite3_prepare_v2(db->handle, "SELECT payload FROM _pending_backup ORDER BY id DESC LIMIT 1", -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      const char *p = (const char *)sqlite3_column_text(stmt, 0);
      if (p) {
        strncpy(db->wal_last_buf, p, sizeof(db->wal_last_buf) - 1);
        db->wal_last_buf[sizeof(db->wal_last_buf) - 1] = '\0';
      }
    }
    sqlite3_finalize(stmt);
  }
  if (db->wal_last_buf[0] != '\0') return db->wal_last_buf;
#ifndef _WIN32
  pthread_mutex_lock(&db->payload_mutex);
#else
  EnterCriticalSection(&db->payload_mutex);
#endif
  if (db->last_shipped_payload[0] != '\0') {
    strncpy(db->wal_last_buf, db->last_shipped_payload, sizeof(db->wal_last_buf) - 1);
    db->wal_last_buf[sizeof(db->wal_last_buf) - 1] = '\0';
  }
#ifndef _WIN32
  pthread_mutex_unlock(&db->payload_mutex);
#else
  LeaveCriticalSection(&db->payload_mutex);
#endif
  return db->wal_last_buf[0] != '\0' ? db->wal_last_buf : NULL;
}

void db_wal_flush(arkilian *db) {
  if (!db) return;
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  db->wake_flag = 1;
  pthread_cond_signal(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);
#else
  EnterCriticalSection(&db->wake_mutex);
  db->wake_flag = 1;
  WakeConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);
#endif
}

// ── Hourly Backup Implementation ────────────────────────────────────

int backup_database(sqlite3 *pSource, const char *zFilename) {
  if (!pSource) return SQLITE_ERROR;
  sqlite3 *pDest = NULL;
  const char *actualPath = (zFilename != NULL) ? zFilename : DEFAULT_BACKUP_PATH;
  int rc = sqlite3_open_v2(actualPath, &pDest, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  if (rc != SQLITE_OK) {
    if (pDest) sqlite3_close(pDest);
    return rc;
  }

  sqlite3_backup *pBackup = sqlite3_backup_init(pDest, "main", pSource, "main");
  if (!pBackup) {
    rc = sqlite3_errcode(pDest);
    sqlite3_close(pDest);
    return rc;
  }

  int retry_count = 0;
  do {
    rc = sqlite3_backup_step(pBackup, 5);
    if (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
      if (rc != SQLITE_OK) sqlite3_sleep(100);
    }
  } while ((rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && ++retry_count < 6000);

  int finish_rc = sqlite3_backup_finish(pBackup);
  if (finish_rc != SQLITE_OK && rc == SQLITE_DONE) rc = finish_rc;
  else if (rc == SQLITE_DONE) rc = SQLITE_OK;
  sqlite3_close(pDest);
  return rc;
}

struct Memory {
  char *response;
  size_t size;
  int *shutdown_flag;
};

static size_t write_cb(void *data, size_t size, size_t nmemb, void *userp) {
  struct Memory *mem = (struct Memory *)userp;
  if (mem->shutdown_flag && *(mem->shutdown_flag)) return 0;
  size_t realsize = size * nmemb;
  char *ptr = realloc(mem->response, mem->size + realsize + 1);
  if (!ptr) return 0;
  mem->response = ptr;
  memcpy(&(mem->response[mem->size]), data, realsize);
  mem->size += realsize;
  mem->response[mem->size] = 0;
  return realsize;
}

char *get_signed_url(const char *api_endpoint, const char *token, int *shutdown_flag) {
  CURL *curl = curl_easy_init();
  struct Memory chunk;
  chunk.response = malloc(1);
  chunk.size = 0;
  chunk.shutdown_flag = shutdown_flag;
  if (!chunk.response) return NULL;

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, api_endpoint);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);

    struct curl_slist *headers = NULL;
    if (token && strlen(token) > 0) {
      char auth_header[512];
      snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
      headers = curl_slist_append(headers, auth_header);
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    if (res == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    // Only a 200 response is a valid answer — an error body must never
    // be mistaken for an upload URL.
    if (res == CURLE_OK && http_code == 200 && chunk.response) {
      char *url_key = strstr(chunk.response, "\"upload_url\":\"");
      if (url_key) {
        url_key += strlen("\"upload_url\":\"");
        char *end_quote = strchr(url_key, '"');
        if (end_quote) {
          size_t url_len = end_quote - url_key;
          char *clean_url = malloc(url_len + 1);
          if (!clean_url) { free(chunk.response); return NULL; }
          memcpy(clean_url, url_key, url_len);
          clean_url[url_len] = '\0';
          free(chunk.response);
          return clean_url;
        }
      }
      // Fallback: some control planes return the URL as a plain-text body.
      if (strncmp(chunk.response, "http://", 7) == 0 ||
          strncmp(chunk.response, "https://", 8) == 0) {
        return chunk.response;
      }
    }
  }
  free(chunk.response);
  return NULL;
}

int upload_to_s3(const char *signed_url, const char *file_path, const char *token) {
  CURL *curl = curl_easy_init();
  if (!curl) return 1;
  FILE *fd = fopen(file_path, "rb");
  if (!fd) {
    curl_easy_cleanup(curl);
    return 1;
  }

  fseek(fd, 0L, SEEK_END);
  long file_size = ftell(fd);
  rewind(fd);

  curl_easy_setopt(curl, CURLOPT_URL, signed_url);
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_READDATA, fd);
  curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_size);

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/x-sqlite3");
  // Pre-signed URLs must NOT carry our bearer token — it leaks the
  // credential to the storage host and S3 rejects requests that mix
  // an Authorization header with a query-string signature.
  if (token && strlen(token) > 0 && !url_is_presigned(signed_url)) {
    char auth_header[512];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
    headers = curl_slist_append(headers, auth_header);
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

  CURLcode res = curl_easy_perform(curl);
  long http_code = 0;
  if (res == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

  fclose(fd);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  // A completed transfer is not success — check the HTTP status so
  // rejected uploads (4xx/5xx) are reported rather than swallowed.
  int ok = (res == CURLE_OK && http_code >= 200 && http_code < 300);
  if (!ok) {
    fprintf(stderr, "arkilian: backup upload failed (curl_rc=%d http=%ld)\n",
            (int)res, http_code);
  }
  return ok ? 0 : 1;
}

#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg) {
#else
void *run_hourly_backup(void *arg) {
#endif
  arkilian *db = (arkilian *)arg;
  // First backup runs immediately, then every backup_interval seconds.
  // The wait is interruptible so db_close() never blocks on a sleeping
  // backup interval (previously close could hang for up to an hour).
  time_t next_backup = time(NULL);

  while (1) {
#ifndef _WIN32
    pthread_mutex_lock(&db->wake_mutex);
    while (!db->shutdown_requested) {
      time_t now = time(NULL);
      if (now >= next_backup) break;
      struct timespec ts;
      ts.tv_sec = next_backup;
      ts.tv_nsec = 0;
      pthread_cond_timedwait(&db->wake_cond, &db->wake_mutex, &ts);
    }
    int shutdown = db->shutdown_requested;
    pthread_mutex_unlock(&db->wake_mutex);
#else
    EnterCriticalSection(&db->wake_mutex);
    while (!db->shutdown_requested) {
      time_t now = time(NULL);
      if (now >= next_backup) break;
      DWORD remaining_ms = (DWORD)((next_backup - now) * 1000);
      SleepConditionVariableCS(&db->wake_cond, &db->wake_mutex, remaining_ms);
    }
    int shutdown = db->shutdown_requested;
    LeaveCriticalSection(&db->wake_mutex);
#endif

    if (shutdown || !db->is_open || !db->handle) break;
    next_backup = time(NULL) + db->backup_interval;

    int status = backup_database(db->handle, db->backup_path);

    // Skip the remote upload when no endpoint has been configured —
    // never phone home to the vendor default with a customer database.
    int endpoint_configured = db->signed_url_endpoint &&
        strlen(db->signed_url_endpoint) > 0 &&
        strcmp(db->signed_url_endpoint, DEFAULT_SIGNED_URL_ENDPOINT) != 0;

    if (status == SQLITE_OK && endpoint_configured) {
      char *signed_url = get_signed_url(db->signed_url_endpoint, db->database_token, (int *)&db->shutdown_requested);
      if (signed_url && strlen(signed_url) > 5) {
        if (upload_to_s3(signed_url, db->backup_path, db->database_token) != 0) {
          fprintf(stderr, "arkilian: scheduled backup upload failed\n");
        }
      }
      free(signed_url);
    }
  }
#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}
