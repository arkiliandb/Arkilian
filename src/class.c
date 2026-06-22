// Arkilian SQLite Wrapper - C API

// Enable POSIX features for portable functions
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
#else
#include <pthread.h>
#include <strings.h>
#include <unistd.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
// deps
#include "deps/sqlite/sqlite3.h"

#ifdef _WIN32
#define strncasecmp _strnicmp
#define strdup _strdup
#endif

// ── Double-buffer for out-of-band WAL shipping ────────────────────
// Active buffer: writers push here lock-free (serialized by write_mutex).
// Flush buffer: background thread drains, POSTs, then clears.
// Swap happens only when active buffer fills up (every 100k writes).

#define WAL_BUF_CAPACITY 100000

struct wal_entry {
  uint64_t ts;
  uint8_t  op;         // 1=INSERT, 2=UPDATE, 3=DELETE, 0=DDL
  uint16_t table_id;
  uint64_t pk;
  char     *sql;
  uint32_t rk;
};

struct wal_double_buf {
  struct wal_entry *entries[2];  // [0] = active, [1] = flushing (or vice versa)
  int count[2];                  // entries in each buffer
  int active;                    // index of the currently-active buffer
  int shutdown;
  int allocated;
#ifndef _WIN32
  pthread_mutex_t swap_mutex;
  pthread_cond_t  flush_cond;
  int             is_flushing;   // 1 when flush buffer has work
#else
  HANDLE swap_mutex;
  HANDLE flush_event;
  LONG   is_flushing;            // Interlocked
#endif
};

static void wal_dbuf_init(struct wal_double_buf *b) {
  b->count[0]  = 0;
  b->count[1]  = 0;
  b->active    = 0;
  b->shutdown  = 0;
  b->is_flushing = 0;
  b->allocated = 0;
  b->entries[0] = NULL;
  b->entries[1] = NULL;

  const char *push_url = getenv("ARKILIAN_WAL_PUSH_URL");
  if (push_url && strlen(push_url) > 0) {
    b->entries[0] = malloc((size_t)WAL_BUF_CAPACITY * sizeof(struct wal_entry));
    b->entries[1] = malloc((size_t)WAL_BUF_CAPACITY * sizeof(struct wal_entry));
    if (!b->entries[0] || !b->entries[1]) {
      free(b->entries[0]);
      free(b->entries[1]);
      b->entries[0] = NULL;
      b->entries[1] = NULL;
    } else {
      b->allocated = 1;
    }
  }

#ifndef _WIN32
  pthread_mutex_init(&b->swap_mutex, NULL);
  pthread_cond_init(&b->flush_cond, NULL);
#else
  b->swap_mutex  = CreateMutex(NULL, FALSE, NULL);
  b->flush_event = CreateEvent(NULL, FALSE, FALSE, NULL);
#endif
}

static void wal_entries_free_sql(struct wal_entry *entries, int count) {
  for (int i = 0; i < count; i++) {
    free(entries[i].sql);
    entries[i].sql = NULL;
  }
}

static void wal_dbuf_destroy(struct wal_double_buf *b) {
  if (b->allocated) {
    wal_entries_free_sql(b->entries[0], b->count[0]);
    wal_entries_free_sql(b->entries[1], b->count[1]);
    free(b->entries[0]);
    free(b->entries[1]);
    b->entries[0] = NULL;
    b->entries[1] = NULL;
  }
#ifndef _WIN32
  pthread_mutex_destroy(&b->swap_mutex);
  pthread_cond_destroy(&b->flush_cond);
#else
  CloseHandle(b->swap_mutex);
  CloseHandle(b->flush_event);
#endif
}

// Push one entry into the active buffer.  Caller holds write_mutex.
// Swaps buffers if active is full.  If no push URL is configured, this
// is a no-op (no background thread to drain, no reason to accumulate).
static void wal_dbuf_push(struct wal_double_buf *b, struct wal_entry *e) {
  const char *push_url = getenv("ARKILIAN_WAL_PUSH_URL");
  if (!push_url || strlen(push_url) == 0) {
    free(e->sql);
    return;
  }
  if (!b->allocated) {
    free(e->sql);
    return;
  }

#ifndef _WIN32
  pthread_mutex_lock(&b->swap_mutex);
#else
  WaitForSingleObject(b->swap_mutex, INFINITE);
#endif

  int a = b->active;

  if (b->count[a] < WAL_BUF_CAPACITY) {
    b->entries[a][b->count[a]++] = *e;
#ifndef _WIN32
    pthread_mutex_unlock(&b->swap_mutex);
#else
    ReleaseMutex(b->swap_mutex);
#endif
    return;
  }

  // Active buffer full — try to swap
#ifndef _WIN32
  while (b->is_flushing && !b->shutdown) {
    pthread_mutex_unlock(&b->swap_mutex);
    usleep(100); // brief backoff
    pthread_mutex_lock(&b->swap_mutex);
  }
  if (b->shutdown) { pthread_mutex_unlock(&b->swap_mutex); free(e->sql); return; }

  // Swap: old active becomes flushing, old flushing becomes active.
  // Push to the newly-active buffer BEFORE signalling — the flush thread
  // must not read the buffer until our write is visible.
  b->active = 1 - a;
  b->entries[b->active][b->count[b->active]++] = *e;
  b->is_flushing = 1;
  pthread_cond_signal(&b->flush_cond);
  pthread_mutex_unlock(&b->swap_mutex);
#else
  while (b->is_flushing && !b->shutdown) {
    ReleaseMutex(b->swap_mutex);
    Sleep(1);
    WaitForSingleObject(b->swap_mutex, INFINITE);
  }
  if (b->shutdown) { ReleaseMutex(b->swap_mutex); free(e->sql); return; }
  b->active = 1 - a;
  b->entries[b->active][b->count[b->active]++] = *e;
  b->is_flushing = 1;
  SetEvent(b->flush_event);
  ReleaseMutex(b->swap_mutex);
#endif
}

// Called by the flush thread.  Blocks until a buffer is ready to flush
// or shutdown is requested.  Returns the number of entries to flush.
// The caller MUST call wal_dbuf_flush_done after POST.
static int wal_dbuf_acquire_flush(struct wal_double_buf *b,
                                   struct wal_entry **out_entries) {
  if (!b->allocated) return 0;
#ifndef _WIN32
  pthread_mutex_lock(&b->swap_mutex);
  while (!b->is_flushing && !b->shutdown)
    pthread_cond_wait(&b->flush_cond, &b->swap_mutex);
  if (b->shutdown) {
    pthread_mutex_unlock(&b->swap_mutex);
    return 0;
  }
  int flush_idx = 1 - b->active;
  *out_entries = b->entries[flush_idx];
  int n = b->count[flush_idx];
  pthread_mutex_unlock(&b->swap_mutex);
  return n;
#else
  WaitForSingleObject(b->swap_mutex, INFINITE);
  while (!b->is_flushing && !b->shutdown) {
    ReleaseMutex(b->swap_mutex);
    WaitForSingleObject(b->flush_event, INFINITE);
    WaitForSingleObject(b->swap_mutex, INFINITE);
  }
  if (b->shutdown) {
    ReleaseMutex(b->swap_mutex);
    return 0;
  }
  int flush_idx = 1 - b->active;
  *out_entries = b->entries[flush_idx];
  int n = b->count[flush_idx];
  ReleaseMutex(b->swap_mutex);
  return n;
#endif
}

// Called by the flush thread after POST completes.
static void wal_dbuf_flush_done(struct wal_double_buf *b) {
  if (!b->allocated) return;
#ifndef _WIN32
  pthread_mutex_lock(&b->swap_mutex);
  int flush_idx = 1 - b->active;
  wal_entries_free_sql(b->entries[flush_idx], b->count[flush_idx]);
  b->count[flush_idx] = 0;
  b->is_flushing = 0;
  pthread_mutex_unlock(&b->swap_mutex);
#else
  WaitForSingleObject(b->swap_mutex, INFINITE);
  int flush_idx = 1 - b->active;
  wal_entries_free_sql(b->entries[flush_idx], b->count[flush_idx]);
  b->count[flush_idx] = 0;
  b->is_flushing = 0;
  ReleaseMutex(b->swap_mutex);
#endif
}

// ── SQL helpers ─────────────────────────────────────────────────────

static uint16_t table_name_hash(const char *s) {
  unsigned long hash = 5381;
  int c;
  while ((c = (unsigned char)*s++))
    hash = ((hash << 5) + hash) + c;
  return (uint16_t)(hash & 0xFFFF);
}

static int extract_pk_from_sql(const char *sql, uint64_t *pk) {
  // Walk backwards to find the last `= <number>` — this is most likely
  // the WHERE-clause filter rather than a SET assignment.
  const char *last = NULL;
  const char *p = sql;
  while ((p = strchr(p, '=')) != NULL) {
    const char *num = p + 1;
    while (*num == ' ') num++;
    if (*num >= '0' && *num <= '9') {
      char *end;
      unsigned long long v = strtoull(num, &end, 10);
      if (v > 0 && (*end == ' ' || *end == ';' || *end == '\0' ||
                    *end == ')' || *end == ','))
        last = p;
    }
    p++;
  }
  if (last) {
    const char *num = last + 1;
    while (*num == ' ') num++;
    *pk = (uint64_t)strtoull(num, NULL, 10);
    return 1;
  }
  return 0;
}

static void parse_sql_meta(const char *sql, uint8_t *op_out, char *tbl, size_t tblsz,
                            uint16_t *tid_out, uint64_t *pk_out) {
  uint8_t op = 0; // DDL
  tbl[0] = '\0';

  // Skip whitespace and leading comments
  while (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r') sql++;
  while ((*sql == '-' && *(sql+1) == '-') || (*sql == '/' && *(sql+1) == '*')) {
    if (*sql == '-') {
      while (*sql && *sql != '\n') sql++;
      if (*sql == '\n') sql++;
    } else {
      sql += 2;
      while (*sql && !(*sql == '*' && *(sql+1) == '/')) sql++;
      if (*sql == '*') sql += 2;
    }
    while (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r') sql++;
  }
  if (*sql == '\0') { *op_out = 0; *tid_out = 0; *pk_out = 0; return; }

#define MATCH(s, literal) ( \
  strncasecmp(s, literal, strlen(literal)) == 0 && \
  (s[strlen(literal)] == ' '  || s[strlen(literal)] == '\t' || \
   s[strlen(literal)] == '\n' || s[strlen(literal)] == '\r' || \
   s[strlen(literal)] == '('  || s[strlen(literal)] == ';'  || \
   s[strlen(literal)] == '\0') \
)

  if (MATCH(sql, "INSERT")) {
    op = 1; sql += 6; while (*sql == ' ') sql++;
    if (MATCH(sql, "INTO")) { sql += 4; while (*sql == ' ') sql++; }
    if (MATCH(sql, "OR")) { sql += 2; while (*sql == ' ') sql++;
      if (MATCH(sql,"REPLACE")||MATCH(sql,"ROLLBACK")||MATCH(sql,"ABORT")||MATCH(sql,"FAIL")||MATCH(sql,"IGNORE"))
        { while (*sql && *sql!=' ') sql++; while (*sql==' ') sql++; }
    }
  } else if (MATCH(sql, "UPDATE")) {
    op = 2; sql += 6; while (*sql == ' ') sql++;
    if (MATCH(sql, "OR")) { sql += 2; while (*sql == ' ') sql++;
      while (*sql && *sql!=' ') sql++;
      while (*sql==' ') sql++; }
  } else if (MATCH(sql, "DELETE")) {
    op = 3; sql += 6; while (*sql == ' ') sql++;
    if (MATCH(sql, "FROM")) { sql += 4; while (*sql == ' ') sql++; }
  } else if (MATCH(sql, "REPLACE")) {
    op = 1; sql += 7; while (*sql == ' ') sql++;
    if (MATCH(sql, "INTO")) { sql += 4; while (*sql == ' ') sql++; }
  } else {
    op = 0;
    if (MATCH(sql,"CREATE")) { sql+=6; while (*sql==' ') sql++;
      if (MATCH(sql,"TABLE")||MATCH(sql,"INDEX")||MATCH(sql,"VIEW")||MATCH(sql,"TRIGGER"))
        { while (*sql&&*sql!=' ') sql++; while (*sql==' ') sql++; }
    } else if (MATCH(sql,"DROP")) { sql+=4; while (*sql==' ') sql++;
      if (MATCH(sql,"TABLE")||MATCH(sql,"INDEX")||MATCH(sql,"VIEW")||MATCH(sql,"TRIGGER"))
        { while (*sql&&*sql!=' ') sql++; while (*sql==' ') sql++; }
      if (MATCH(sql,"IF")) {
        while (*sql&&*sql!=' ') sql++;
        while (*sql==' ') sql++;
        while (*sql&&*sql!=' ') sql++;
        while (*sql==' ') sql++;
      }
    } else if (MATCH(sql,"ALTER")) { sql+=5; while (*sql==' ') sql++;
      if (MATCH(sql,"TABLE")) { sql+=5; while (*sql==' ') sql++; }
    }
  }
#undef MATCH

  // Extract table name
  if (*sql == '`' || *sql == '"' || *sql == '[') {
    char quote = (*sql == '[') ? ']' : *sql;
    sql++;
    size_t i = 0;
    while (*sql && *sql != quote && i < tblsz - 1) tbl[i++] = *sql++;
    tbl[i] = '\0';
  } else {
    size_t i = 0;
    while (*sql && *sql != ' ' && *sql != '\t' && *sql != '\n' &&
           *sql != '(' && *sql != ';' && i < tblsz - 1) tbl[i++] = *sql++;
    tbl[i] = '\0';
  }

  *op_out  = op;
  *tid_out = table_name_hash(tbl);
  if (!extract_pk_from_sql(sql, pk_out)) *pk_out = 0;
}

// Build a wal_entry from the SQL string
static void build_wal_entry(struct wal_entry *e, const char *sql) {
  char tbl[128];
  e->ts = (uint64_t)time(NULL);
  parse_sql_meta(sql, &e->op, tbl, sizeof(tbl), &e->table_id, &e->pk);
  e->sql = strdup(sql ? sql : "");
}

// ── Arkilian struct ─────────────────────────────────────────────────

struct arkilian {
  sqlite3 *handle;
  int last_error_code;
  char last_error_msg[256];
  int is_open;
  int has_new_writes;
  sqlite3_stmt **stmts;
  int stmt_count;
  int stmt_capacity;
  int stmt_current;
  // Backup config
  char *backup_path;
  char *signed_url_endpoint;
  char *database_token;
  int backup_interval;
  int backup_enabled;
  // Backup thread tracking
  int shutdown_requested;
#ifdef _WIN32
  HANDLE backup_thread_handle;
#else
  pthread_t backup_thread_id;
  int backup_thread_running;
#endif
  // Write interception
#ifndef _WIN32
  pthread_mutex_t write_mutex;
#else
  HANDLE write_mutex;
#endif
  int in_write_txn;
  int write_stmt_index;
  int in_batch_txn;
  char current_write_sql[1024];
  // Update hook state
  uint8_t  update_hook_op;
  char     update_hook_table[128];
  uint64_t update_hook_rowid;
  int      update_hook_fired;
  int last_step_rc;
  // Cached statements
  sqlite3_stmt *begin_stmt;
  sqlite3_stmt *commit_stmt;
  sqlite3_stmt *rollback_stmt;
  // Double-buffer for out-of-band WAL shipping
  struct wal_double_buf wal;
  // Flush thread
#ifndef _WIN32
  pthread_t flush_thread_id;
  int flush_thread_running;
#else
  HANDLE flush_thread_handle;
#endif
};

static void ar_update_hook(void *user_data, int op_type, char const *db_name,
                           char const *table_name, sqlite3_int64 row_id) {
  arkilian *db = (arkilian *)user_data;
  (void)db_name;
  db->update_hook_fired = 1;
  db->update_hook_op = (op_type == SQLITE_INSERT) ? 1 :
                       (op_type == SQLITE_UPDATE) ? 2 :
                       (op_type == SQLITE_DELETE) ? 3 : 0;
  if (table_name) {
    strncpy(db->update_hook_table, table_name, sizeof(db->update_hook_table) - 1);
    db->update_hook_table[sizeof(db->update_hook_table) - 1] = '\0';
  } else {
    db->update_hook_table[0] = '\0';
  }
  db->update_hook_rowid = (uint64_t)row_id;
}

static void check_update_hook_before_push(arkilian *db, struct wal_entry *e) {
  if (db->update_hook_fired) {
    e->op = db->update_hook_op;
    e->table_id = table_name_hash(db->update_hook_table);
    // sqlite3_update_hook fires per row, but we ship one entry per statement.
    // pk is the last row touched; rk carries the real row count. by design.
    e->pk = db->update_hook_rowid;
    db->update_hook_fired = 0;
  }
}

struct Memory {
  char *response;
  size_t size;
  int *shutdown_flag;
};

// ── Config defaults ─────────────────────────────────────────────────

#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600
#define DEFAULT_SIGNED_URL_ENDPOINT "https://api.arkilian.com/get-signed-url"

// ── Helpers ─────────────────────────────────────────────────────────

static const char *get_env_default(const char *env_var,
                                   const char *default_val) {
  const char *val = getenv(env_var);
  return (val && strlen(val) > 0) ? val : default_val;
}

static int get_env_int_default(const char *env_var, int default_val) {
  const char *val = getenv(env_var);
  if (val && strlen(val) > 0) return atoi(val);
  return default_val;
}

void load_env(void) {
  const char *file = ".env";
  FILE *fp = fopen(file, "r");
  if (!fp) return;
  char line[256];
  while (fgets(line, sizeof(line), fp)) {
    char *key = strtok(line, "=");
    char *val = strtok(NULL, "\n");
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

// ── Forward declarations ────────────────────────────────────────────

int backup_database(sqlite3 *pSource, const char *zFilename);
#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg);
DWORD WINAPI run_wal_flush(LPVOID arg);
#else
void *run_hourly_backup(void *arg);
void *run_wal_flush(void *arg);
#endif
char *get_signed_url(const char *api_endpoint, const char *token,
                     int *shutdown_flag);
int upload_to_s3(const char *signed_url, const char *file_path,
                 const char *token);

// ── Background WAL flush thread ─────────────────────────────────────

#ifdef _WIN32
DWORD WINAPI run_wal_flush(LPVOID arg) {
#else
void *run_wal_flush(void *arg) {
#endif
  arkilian *db = (arkilian *)arg;
  const char *push_url = getenv("ARKILIAN_WAL_PUSH_URL");

  while (1) {
    const char *token = db->database_token;
    struct wal_entry *batch;
    int n = wal_dbuf_acquire_flush(&db->wal, &batch);
    if (n == 0) break;

    int pushed = 0;

    if (push_url && strlen(push_url) > 0) {
      // Build JSON payload
      size_t json_cap = 64;
      for (int i = 0; i < n; i++) json_cap += strlen(batch[i].sql) * 6 + 96;
      char *json = malloc(json_cap);
      if (json) {
        size_t off = 0;
        off += (size_t)snprintf(json + off, json_cap - off, "[");
        for (int i = 0; i < n; i++) {
          struct wal_entry *e = &batch[i];
          if (off + 32 >= json_cap) break;
          off += (size_t)snprintf(json + off, json_cap - off,
            "{\"ts\":%llu,\"op\":%u,\"table_id\":%u,\"pk\":%llu,\"rk\":%u,\"sql\":\"",
            (unsigned long long)e->ts, e->op, e->table_id,
            (unsigned long long)e->pk, e->rk);
          for (char *s = e->sql ? e->sql : ""; *s; s++) {
            if (off + 8 >= json_cap) break;
            switch (*s) {
            case '"':  json[off++] = '\\'; json[off++] = '"';  break;
            case '\\': json[off++] = '\\'; json[off++] = '\\'; break;
            case '\n': json[off++] = '\\'; json[off++] = 'n';  break;
            case '\r': json[off++] = '\\'; json[off++] = 'r';  break;
            case '\t': json[off++] = '\\'; json[off++] = 't';  break;
            case '\b': json[off++] = '\\'; json[off++] = 'b';  break;
            case '\f': json[off++] = '\\'; json[off++] = 'f';  break;
            default:
              if ((unsigned char)*s < 0x20) {
                int w = snprintf(json + off, 8, "\\u%04x", (unsigned char)*s);
                if (w > 0) off += (size_t)w;
                continue;
              }
              json[off++] = *s;
              break;
            }
          }
          if (off + 16 >= json_cap) break;
          off += (size_t)snprintf(json + off, json_cap - off,
            "\"}%s", (i < n - 1) ? "," : "");
        }
        if (off + 4 < json_cap)
          off += (size_t)snprintf(json + off, json_cap - off, "]");

        CURL *curl = curl_easy_init();
        if (curl) {
          curl_easy_setopt(curl, CURLOPT_URL, push_url);
          curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json);
          curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
          curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);

          struct curl_slist *headers = NULL;
          headers = curl_slist_append(headers, "Content-Type: application/json");
          if (token && strlen(token) > 0) {
            char auth[512];
            snprintf(auth, sizeof(auth), "Authorization: Bearer %s", token);
            headers = curl_slist_append(headers, auth);
          }
          curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

          CURLcode res = curl_easy_perform(curl);
          long http_code = 0;
          if (res == CURLE_OK)
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

          if (res == CURLE_OK && http_code == 200) {
            pushed = 1;
          } else {
            fprintf(stderr, "WAL push failed (HTTP %ld, %s) — retrying next cycle\n",
                    http_code, curl_easy_strerror(res));
          }

          curl_slist_free_all(headers);
          curl_easy_cleanup(curl);
        }
        free(json);
      }
    }

    if (pushed) {
      wal_dbuf_flush_done(&db->wal);
    } else {
      // Push failed — entries stay in the flush buffer.  is_flushing
      // is still 1, so the next acquire_flush will return them again.
      // Brief backoff before retry to avoid tight spin.
#ifndef _WIN32
      usleep(500000);
#else
      Sleep(500);
#endif
    }
  }

#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}

// ── db_init / db_close ──────────────────────────────────────────────

int db_init(arkilian **db_ptr, const char *filename) {
  if (!db_ptr) return 1;
  arkilian *db = malloc(sizeof(arkilian));
  if (!db) return 1;
  memset(db, 0, sizeof(arkilian));
  db->is_open = 0;
  db->has_new_writes = 0;
  db->last_error_msg[0] = '\0';
  db->stmts = NULL;
  db->stmt_count = 0;
  db->stmt_capacity = 0;
  db->stmt_current = -1;
  load_env();

  const char *db_path = (filename != NULL) ? filename :
    get_env_default("ARKILIAN_DB_PATH", DEFAULT_DB_PATH);

  const char *backup_path_tmp =
    get_env_default("ARKILIAN_BACKUP_PATH", DEFAULT_BACKUP_PATH);
  db->backup_path = malloc(strlen(backup_path_tmp) + 1);
  if (db->backup_path) strcpy(db->backup_path, backup_path_tmp);

  const char *signed_url_tmp = get_env_default("ARKILIAN_SIGNED_URL_ENDPOINT",
                                                DEFAULT_SIGNED_URL_ENDPOINT);
  db->signed_url_endpoint = malloc(strlen(signed_url_tmp) + 1);
  if (db->signed_url_endpoint) strcpy(db->signed_url_endpoint, signed_url_tmp);

  const char *token_tmp = get_env_default("ARKILIAN_DATABASE_TOKEN", "");
  db->database_token = malloc(strlen(token_tmp) + 1);
  if (db->database_token) strcpy(db->database_token, token_tmp);

  db->backup_interval =
    get_env_int_default("ARKILIAN_BACKUP_INTERVAL", DEFAULT_BACKUP_INTERVAL);
  db->backup_enabled = get_env_int_default("ARKILIAN_ENABLE_BACKUP", 1);

  // Write interception state
#ifndef _WIN32
  pthread_mutex_init(&db->write_mutex, NULL);
#else
  db->write_mutex = CreateMutex(NULL, FALSE, NULL);
#endif
  db->in_write_txn = 0;
  db->write_stmt_index = -1;
  db->in_batch_txn = 0;
  db->current_write_sql[0] = '\0';
  db->last_step_rc = 0;

  int rc = sqlite3_open_v2(
      db_path, &db->handle,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);

  if (rc != SQLITE_OK) {
    db->handle = NULL;
    const char *err = sqlite3_errstr(rc);
    strncpy(db->last_error_msg, err, sizeof(db->last_error_msg) - 1);
    db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
    *db_ptr = db;
    return 1;
  }

  // PRAGMAs
  sqlite3_exec(db->handle, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA busy_timeout=5000;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

  // Register update hook for robust write metadata capture
  sqlite3_update_hook(db->handle, ar_update_hook, db);
  db->update_hook_fired = 0;

  // Internal tables
  sqlite3_exec(db->handle,
    "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
    NULL, NULL, NULL);

  // Cached transaction statements
  if (sqlite3_prepare_v2(db->handle, "BEGIN;", -1, &db->begin_stmt, NULL) != SQLITE_OK)
    db->begin_stmt = NULL;
  if (sqlite3_prepare_v2(db->handle, "COMMIT;", -1, &db->commit_stmt, NULL) != SQLITE_OK)
    db->commit_stmt = NULL;
  if (sqlite3_prepare_v2(db->handle, "ROLLBACK;", -1, &db->rollback_stmt, NULL) != SQLITE_OK)
    db->rollback_stmt = NULL;

  // Double-buffer for WAL shipping
  wal_dbuf_init(&db->wal);

  db->is_open = 1;
  db->shutdown_requested = 0;
  *db_ptr = db;

  // Start flush thread (only if a push URL is configured)
  {
    const char *url = getenv("ARKILIAN_WAL_PUSH_URL");
    if (url && strlen(url) > 0) {
#ifndef _WIN32
      db->flush_thread_running = 0;
      if (pthread_create(&db->flush_thread_id, NULL, run_wal_flush, db) == 0)
        db->flush_thread_running = 1;
#else
      db->flush_thread_handle = CreateThread(NULL, 0, run_wal_flush, db, 0, NULL);
#endif
    }
#ifndef _WIN32
    else { db->flush_thread_running = 0; }
#endif
  }

  // Start backup thread if enabled
  if (db->backup_enabled) {
#ifdef _WIN32
    db->backup_thread_handle =
        CreateThread(NULL, 0, run_hourly_backup, db, 0, NULL);
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

  db->shutdown_requested = 1;

  // Flush any pending WAL entries before joining the flush thread.
  // If the active buffer has unflushed writes, force a swap so the
  // flush thread drains them.  Spin-wait until both buffers are empty
  // or a timeout is reached.
  {
    int active = db->wal.active;
    if (db->wal.count[active] > 0) {
      db_wal_flush(db);
    }
    int waited = 0;
    while ((db->wal.count[0] > 0 || db->wal.count[1] > 0) && waited < 100) {
#ifndef _WIN32
      usleep(100000);
#else
      Sleep(100);
#endif
      if (db->wal.count[db->wal.active] > 0)
        db_wal_flush(db);
      waited++;
    }
  }

  // Signal double-buffer shutdown to wake flush thread
  db->wal.shutdown = 1;
#ifndef _WIN32
  pthread_cond_signal(&db->wal.flush_cond);
#else
  SetEvent(db->wal.flush_event);
#endif

  // Wait for flush thread (if running)
#ifndef _WIN32
  if (db->flush_thread_running) {
    pthread_join(db->flush_thread_id, NULL);
    db->flush_thread_running = 0;
  }
#else
  if (db->flush_thread_handle != NULL) {
    WaitForSingleObject(db->flush_thread_handle, INFINITE);
    CloseHandle(db->flush_thread_handle);
    db->flush_thread_handle = NULL;
  }
#endif

  // Wait for backup thread
#ifndef _WIN32
  if (db->backup_thread_running) {
    pthread_join(db->backup_thread_id, NULL);
    db->backup_thread_running = 0;
  }
#else
  if (db->backup_thread_handle != NULL) {
    WaitForSingleObject(db->backup_thread_handle, INFINITE);
    CloseHandle(db->backup_thread_handle);
    db->backup_thread_handle = NULL;
  }
#endif

  // Clean up user statements
  for (int i = 0; i < db->stmt_count; i++) {
    if (db->stmts[i]) sqlite3_finalize(db->stmts[i]);
  }
  free(db->stmts);
  db->stmts = NULL;
  db->stmt_count = 0;
  db->stmt_capacity = 0;
  db->stmt_current = -1;

  if (db->is_open && db->handle) {
    // Rollback any open transactions
    if (db->in_write_txn || db->in_batch_txn) {
      if (db->in_write_txn) {
        sqlite3_step(db->rollback_stmt);
        sqlite3_reset(db->rollback_stmt);
        db->in_write_txn = 0;
        db->write_stmt_index = -1;
      }
      if (db->in_batch_txn) {
        sqlite3_step(db->rollback_stmt);
        sqlite3_reset(db->rollback_stmt);
        db->in_batch_txn = 0;
      }
#ifndef _WIN32
      pthread_mutex_unlock(&db->write_mutex);
#else
      ReleaseMutex(db->write_mutex);
#endif
    }

    // Finalize cached statements
    if (db->begin_stmt)    { sqlite3_finalize(db->begin_stmt);    db->begin_stmt = NULL; }
    if (db->commit_stmt)   { sqlite3_finalize(db->commit_stmt);   db->commit_stmt = NULL; }
    if (db->rollback_stmt) { sqlite3_finalize(db->rollback_stmt); db->rollback_stmt = NULL; }

    sqlite3_close(db->handle);
    db->handle = NULL;
    db->is_open = 0;
  }

  if (db->backup_path)        free(db->backup_path);
  if (db->signed_url_endpoint) free(db->signed_url_endpoint);
  if (db->database_token)      free(db->database_token);

  wal_dbuf_destroy(&db->wal);

#ifndef _WIN32
  pthread_mutex_destroy(&db->write_mutex);
#else
  if (db->write_mutex) CloseHandle(db->write_mutex);
#endif

  free(db);
}

// ── Public API ──────────────────────────────────────────────────────

const char *db_errmsg(arkilian *db) {
  if (db->last_error_msg[0] != '\0') return db->last_error_msg;
  if (db->handle) return sqlite3_errmsg(db->handle);
  return "Unknown error";
}

sqlite3 *db_get_handle(arkilian *db) { return db->handle; }

// ── Backup ──────────────────────────────────────────────────────────

int backup_database(sqlite3 *pSource, const char *zFilename) {
  int rc;
  sqlite3 *pDest = NULL;
  sqlite3_backup *pBackup = NULL;

  const char *actualPath =
      (zFilename != NULL) ? zFilename : DEFAULT_BACKUP_PATH;
  rc = sqlite3_open_v2(actualPath, &pDest,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "Backup Error: Cannot open destination file %s: %s\n",
            zFilename, sqlite3_errmsg(pDest));
    sqlite3_close(pDest);
    return rc;
  }

  pBackup = sqlite3_backup_init(pDest, "main", pSource, "main");
  if (pBackup == NULL) {
    rc = sqlite3_errcode(pDest);
    fprintf(stderr, "Backup Error: Initialization failed: %s\n",
            sqlite3_errmsg(pDest));
    sqlite3_close(pDest);
    return rc;
  }

  do {
    rc = sqlite3_backup_step(pBackup, 5);
    if (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
      if (rc != SQLITE_OK) sqlite3_sleep(100);
    }
  } while (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED);

  (void)sqlite3_backup_finish(pBackup);

  if (rc == SQLITE_DONE) rc = SQLITE_OK;
  else fprintf(stderr, "Backup Error: Step failed with code %d: %s\n", rc,
               sqlite3_errmsg(pDest));

  sqlite3_close(pDest);
  return rc;
}

#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg) {
#else
void *run_hourly_backup(void *arg) {
#endif
  arkilian *db = (arkilian *)arg;
  while (1) {
#ifdef _WIN32
    Sleep(db->backup_interval * 1000);
#else
    sleep(db->backup_interval);
#endif
    if (db->shutdown_requested) {
#ifdef _WIN32
      return 0;
#else
      return NULL;
#endif
    }
    if (!db->is_open || db->handle == NULL) {
#ifdef _WIN32
      return 0;
#else
      return NULL;
#endif
    }
    int status = backup_database(db->handle, db->backup_path);
    if (status == SQLITE_OK) {
      char *signed_url = get_signed_url(
          db->signed_url_endpoint, db->database_token, &db->shutdown_requested);
      if (signed_url && strlen(signed_url) > 5) {
        int upload_status =
            upload_to_s3(signed_url, db->backup_path, db->database_token);
        if (upload_status == 0) ; // S3 upload ok
        else fprintf(stderr, "S3 Upload Failed with status: %d\n", upload_status);
      }
      free(signed_url);
    } else {
      fprintf(stderr, "Backup failed with error code: %d\n", status);
    }
  }
#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}

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

char *get_signed_url(const char *api_endpoint, const char *token,
                     int *shutdown_flag) {
  CURL *curl = curl_easy_init();
  struct Memory chunk;
  chunk.response = malloc(1);
  chunk.size = 0;
  chunk.shutdown_flag = shutdown_flag;

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
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (res == CURLE_OK) return chunk.response;
  }
  free(chunk.response);
  return NULL;
}

int upload_to_s3(const char *signed_url, const char *file_path,
                 const char *token) {
  CURL *curl = curl_easy_init();
  if (!curl) return 1;
  FILE *fd = fopen(file_path, "rb");
  if (!fd) return 1;

  fseek(fd, 0L, SEEK_END);
  long file_size = ftell(fd);
  rewind(fd);

  curl_easy_setopt(curl, CURLOPT_URL, signed_url);
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_READDATA, fd);
  curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_size);

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/x-sqlite3");
  if (token && strlen(token) > 0) {
    char auth_header[512];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
    headers = curl_slist_append(headers, auth_header);
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

  CURLcode res = curl_easy_perform(curl);
  fclose(fd);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return (res == CURLE_OK) ? 0 : 1;
}

// ── Core write execution ────────────────────────────────────────────
// db_exec: run the SQL against SQLite, then push metadata to the ring
// buffer for out-of-band streaming.  Read-only statements bypass
// the write mutex entirely.

int db_exec(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql)
    return SQLITE_ERROR;

  // Fast path for reads — skip prepare, mutex, ring buffer, everything.
  // Just let SQLite handle it directly.
  const char *p = sql;
  while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
  if ((p[0] == 'S' || p[0] == 's') &&
      (p[1] == 'E' || p[1] == 'e') &&
      (p[2] == 'L' || p[2] == 'l') &&
      (p[3] == 'E' || p[3] == 'e') &&
      (p[4] == 'C' || p[4] == 'c') &&
      (p[5] == 'T' || p[5] == 't') &&
      (p[6] == ' ' || p[6] == '\t' || p[6] == '\n' || p[6] == '\r' || p[6] == '\0')) {
    int rc = sqlite3_exec(db->handle, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK)
      snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
               sqlite3_errmsg(db->handle));
    return rc;
  }

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db->handle, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    return rc;
  }

  // Reads just execute — no mutex, no ring push
  if (sqlite3_stmt_readonly(stmt)) {
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc;
  }

  // Write path
  // Mutex may already be held by db_prepare (in_write_txn) or db_begin (in_batch_txn)
  int mutex_held = db->in_write_txn || db->in_batch_txn;

  if (!mutex_held) {
#ifndef _WIN32
    pthread_mutex_lock(&db->write_mutex);
#else
    WaitForSingleObject(db->write_mutex, INFINITE);
#endif
    // Autocommit: each write is its own implicit transaction via SQLite.
    // If the user wants batching they use db_begin / db_commit.
  }

  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  if (rc == SQLITE_DONE || rc == SQLITE_OK || rc == SQLITE_ROW) {
    db->has_new_writes = 1;

    // Push to ring buffer for async streaming
    struct wal_entry entry;
    build_wal_entry(&entry, sql);
    check_update_hook_before_push(db, &entry);
    entry.rk = (uint32_t)sqlite3_changes(db->handle);
    wal_dbuf_push(&db->wal, &entry);
  } else {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    if (db->in_batch_txn) {
      sqlite3_step(db->rollback_stmt);
      sqlite3_reset(db->rollback_stmt);
      db->in_batch_txn = 0;
#ifndef _WIN32
      pthread_mutex_unlock(&db->write_mutex);
#else
      ReleaseMutex(db->write_mutex);
#endif
      return rc;
    }
  }

  if (!mutex_held) {
#ifndef _WIN32
    pthread_mutex_unlock(&db->write_mutex);
#else
    ReleaseMutex(db->write_mutex);
#endif
  }

  return (rc == SQLITE_DONE || rc == SQLITE_OK || rc == SQLITE_ROW)
    ? SQLITE_DONE : rc;
}

// ── Prepared statement path ─────────────────────────────────────────

int db_prepare(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql)
    return SQLITE_ERROR;

  if (db->stmt_count >= db->stmt_capacity) {
    int new_cap = (db->stmt_capacity == 0) ? 4 : db->stmt_capacity * 2;
    sqlite3_stmt **new_arr =
        realloc(db->stmts, (size_t)new_cap * sizeof(sqlite3_stmt *));
    if (!new_arr) return SQLITE_NOMEM;
    db->stmts = new_arr;
    db->stmt_capacity = new_cap;
  }

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db->handle, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    return rc;
  }

  // Write statements: acquire mutex (autocommit — each write runs in its own txn)
  if (!sqlite3_stmt_readonly(stmt)) {
    int was_in_write = db->in_write_txn;
    if (db->in_write_txn || db->in_batch_txn) {
      // Already in a transaction from a previous write prepare or db_begin
    } else {
#ifndef _WIN32
      pthread_mutex_lock(&db->write_mutex);
#else
      WaitForSingleObject(db->write_mutex, INFINITE);
#endif
    }
    db->in_write_txn = 1;
    // Only record the first write in a transaction as the tracked statement
    if (!was_in_write) {
      db->write_stmt_index = db->stmt_count;
      strncpy(db->current_write_sql, sql, sizeof(db->current_write_sql) - 1);
      db->current_write_sql[sizeof(db->current_write_sql) - 1] = '\0';
    }
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
  if (!db) return 0;
  return db->stmt_count;
}

static sqlite3_stmt *get_current_stmt(arkilian *db) {
  if (!db || db->stmt_current < 0 || db->stmt_current >= db->stmt_count)
    return NULL;
  return db->stmts[db->stmt_current];
}

int db_step(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  int rc = sqlite3_step(stmt);
  db->last_step_rc = rc;
  return rc;
}

int db_finalize(arkilian *db) {
  if (!db) return SQLITE_ERROR;
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (stmt) {
    int is_this_write = (db->stmt_current == db->write_stmt_index);

    char *expanded = NULL;
    if (is_this_write) {
      expanded = sqlite3_expanded_sql(stmt);
    }

    sqlite3_finalize(stmt);
    db->stmts[db->stmt_current] = NULL;

    if (is_this_write) {
      int ok = (db->last_step_rc == SQLITE_DONE ||
                db->last_step_rc == SQLITE_ROW ||
                db->last_step_rc == SQLITE_OK);
      if (ok) {
        db->has_new_writes = 1;
        struct wal_entry entry;
        if (expanded) {
          build_wal_entry(&entry, expanded);
          sqlite3_free(expanded);
        } else {
          build_wal_entry(&entry, db->current_write_sql);
        }
        check_update_hook_before_push(db, &entry);
        entry.rk = (uint32_t)sqlite3_changes(db->handle);
        wal_dbuf_push(&db->wal, &entry);
      } else {
        if (expanded) sqlite3_free(expanded);
      }
      db->in_write_txn = 0;
      db->write_stmt_index = -1;
      db->current_write_sql[0] = '\0';
      if (!db->in_batch_txn) {
#ifndef _WIN32
        pthread_mutex_unlock(&db->write_mutex);
#else
        ReleaseMutex(db->write_mutex);
#endif
      }
    }
  }
  return SQLITE_OK;
}

int db_reset(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_reset(stmt);
}

// ── Column access ───────────────────────────────────────────────────

int db_column_count(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return 0;
  return sqlite3_column_count(stmt);
}

const char *db_column_name(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return NULL;
  return (const char *)sqlite3_column_name(stmt, col);
}

const char *db_column_text(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return NULL;
  return (const char *)sqlite3_column_text(stmt, col);
}

int db_column_int(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return 0;
  return sqlite3_column_int(stmt, col);
}

double db_column_double(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return 0.0;
  return sqlite3_column_double(stmt, col);
}

int db_bind_text(arkilian *db, int idx, const char *val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt || !val) return SQLITE_ERROR;
  return sqlite3_bind_text(stmt, idx, val, -1, SQLITE_TRANSIENT);
}

int db_bind_int(arkilian *db, int idx, int val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_bind_int(stmt, idx, val);
}

int db_bind_double(arkilian *db, int idx, double val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_bind_double(stmt, idx, val);
}

// ── Batch transaction API ───────────────────────────────────────────

int db_begin(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (db->in_batch_txn || db->in_write_txn) return SQLITE_BUSY;
#ifndef _WIN32
  pthread_mutex_lock(&db->write_mutex);
#else
  WaitForSingleObject(db->write_mutex, INFINITE);
#endif
  int rc = sqlite3_step(db->begin_stmt);
  sqlite3_reset(db->begin_stmt);
  if (rc != SQLITE_DONE) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
#ifndef _WIN32
    pthread_mutex_unlock(&db->write_mutex);
#else
    ReleaseMutex(db->write_mutex);
#endif
    return rc;
  }
  db->in_batch_txn = 1;
  return SQLITE_OK;
}

int db_commit(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (!db->in_batch_txn) return SQLITE_ERROR;
  int rc = sqlite3_step(db->commit_stmt);
  sqlite3_reset(db->commit_stmt);
  db->in_batch_txn = 0;
  db->has_new_writes = 1;
#ifndef _WIN32
  pthread_mutex_unlock(&db->write_mutex);
#else
  ReleaseMutex(db->write_mutex);
#endif
  return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

int db_rollback(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (!db->in_batch_txn) return SQLITE_ERROR;
  sqlite3_step(db->rollback_stmt);
  sqlite3_reset(db->rollback_stmt);
  db->in_batch_txn = 0;
#ifndef _WIN32
  pthread_mutex_unlock(&db->write_mutex);
#else
  ReleaseMutex(db->write_mutex);
#endif
  return SQLITE_OK;
}

int db_bind_null(arkilian *db, int idx) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_bind_null(stmt, idx);
}

int db_bind_int64(arkilian *db, int idx, sqlite3_int64 val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_bind_int64(stmt, idx, val);
}

sqlite3_int64 db_column_int64(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return 0;
  return sqlite3_column_int64(stmt, col);
}

int db_column_type(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_NULL;
  return sqlite3_column_type(stmt, col);
}

int db_changes(arkilian *db) {
  if (!db || !db->handle) return 0;
  return sqlite3_changes(db->handle);
}

sqlite3_int64 db_last_insert_rowid(arkilian *db) {
  if (!db || !db->handle) return 0;
  return sqlite3_last_insert_rowid(db->handle);
}

int db_set_token(arkilian *db, const char *token) {
  if (!db || !token) return 1;
  if (db->database_token) free(db->database_token);
  db->database_token = malloc(strlen(token) + 1);
  if (!db->database_token) return 1;
  strcpy(db->database_token, token);
  return 0;
}

// Returns the number of pending WAL entries in the ring buffer
int db_wal_pending(arkilian *db) {
  if (!db) return 0;
  return db->wal.count[0] + db->wal.count[1];
}

// Force a flush of the WAL double-buffer.  Must be called while the
// write_mutex is NOT held (the flush thread uses swap_mutex).
void db_wal_flush(arkilian *db) {
  if (!db || !db->handle) return;
  if (!db->wal.allocated) return;

  // Keep at it until we hand off the active buffer, or there's nothing
  // left to hand off.  A single retry isn't enough — the flush thread
  // could be mid-POST and take longer than our 200ms window.
  for (int retry = 0; retry < 50; retry++) {
    if (db->wal.count[db->wal.active] == 0) return;

#ifndef _WIN32
    pthread_mutex_lock(&db->wal.swap_mutex);
    if (db->wal.is_flushing) {
      pthread_mutex_unlock(&db->wal.swap_mutex);
      usleep(200000);
      continue;
    }
    if (db->wal.count[db->wal.active] > 0) {
      int a = db->wal.active;
      db->wal.active = 1 - a;
      db->wal.is_flushing = 1;
      pthread_cond_signal(&db->wal.flush_cond);
    }
    pthread_mutex_unlock(&db->wal.swap_mutex);
#else
    WaitForSingleObject(db->wal.swap_mutex, INFINITE);
    if (db->wal.is_flushing) {
      ReleaseMutex(db->wal.swap_mutex);
      Sleep(200);
      continue;
    }
    if (db->wal.count[db->wal.active] > 0) {
      int a = db->wal.active;
      db->wal.active = 1 - a;
      db->wal.is_flushing = 1;
      SetEvent(db->wal.flush_event);
    }
    ReleaseMutex(db->wal.swap_mutex);
#endif
  }
}
