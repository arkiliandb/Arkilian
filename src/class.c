// Arkilian SQLite Wrapper - C API

// Enable POSIX features for portable functions
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "class.h"
#include <curl/curl.h>
#ifdef _WIN32
#include <windows.h>
#define strncasecmp _strnicmp
#else
#include <pthread.h>
#include <strings.h>
#include <unistd.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
// deps
#include "deps/sqlite/sqlite3.h"

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
  char current_write_sql[1024];
  int last_step_rc;
  // Cached statements for write interception (avoid re-prepare overhead)
  sqlite3_stmt *log_insert_stmt;
  sqlite3_stmt *begin_stmt;
  sqlite3_stmt *commit_stmt;
  sqlite3_stmt *rollback_stmt;
  // Log buffer for batched writes (amortizes log INSERT overhead)
  #define LOG_BUFFER_CAPACITY 128
  struct log_entry {
    sqlite3_int64 ts;
    int op;        // 0=INSERT, 1=UPDATE, 2=DELETE, 3=DDL
    char tbl[128];
    char sql[1024];
  };
  struct log_entry *log_buffer;
  int log_buffer_count;
};

struct Memory {
  char *response;
  size_t size;
  int shutdown_flag; // Pointer to arkilian->shutdown_requested
};

// Config defaults
#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600
#define DEFAULT_SIGNED_URL_ENDPOINT "https://api.arkilian.com/get-signed-url"

// Helper to get env var with default
static const char *get_env_default(const char *env_var,
                                   const char *default_val) {
  const char *val = getenv(env_var);
  // printf("%s = %s;\n", env_var, val);
  return (val && strlen(val) > 0) ? val : default_val;
}

// Helper to get env var as int with default
static int get_env_int_default(const char *env_var, int default_val) {
  const char *val = getenv(env_var);
  if (val && strlen(val) > 0) {
    // printf("%s = %s;\n", env_var, val);
    return atoi(val);
  }
  return default_val;
}

void load_env(void) {
  const char *file = ".env";
  FILE *fp = fopen(file, "r");
  if (!fp)
    return;

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

// forward declarations
int backup_database(sqlite3 *pSource, const char *zFilename);
#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg);
#else
void *run_hourly_backup(void *arg);
#endif
char *get_signed_url(const char *api_endpoint, const char *token,
                     int *shutdown_flag);
int upload_to_s3(const char *signed_url, const char *file_path,
                 const char *token);

int db_init(arkilian **db_ptr, const char *filename) {
  if (!db_ptr)
    return 1;
  arkilian *db = malloc(sizeof(arkilian));
  if (!db)
    return 1;
  db->is_open = 0;
  db->has_new_writes = 0;
  db->last_error_msg[0] = '\0';
  db->stmts = NULL;
  db->stmt_count = 0;
  db->stmt_capacity = 0;
  db->stmt_current = -1;
  load_env();
  // Get configuration from environment
  const char *db_path =
      (filename != NULL) ? filename
                         : get_env_default("ARKILIAN_DB_PATH", DEFAULT_DB_PATH);

  // Backup configuration (using portable string copy instead of strdup)
  const char *backup_path_tmp =
      get_env_default("ARKILIAN_BACKUP_PATH", DEFAULT_BACKUP_PATH);
  db->backup_path = malloc(strlen(backup_path_tmp) + 1);
  if (db->backup_path)
    strcpy(db->backup_path, backup_path_tmp);

  const char *signed_url_tmp = get_env_default("ARKILIAN_SIGNED_URL_ENDPOINT",
                                               DEFAULT_SIGNED_URL_ENDPOINT);

  db->signed_url_endpoint = malloc(strlen(signed_url_tmp) + 1);
  if (db->signed_url_endpoint)
    strcpy(db->signed_url_endpoint, signed_url_tmp);

  const char *token_tmp = get_env_default("ARKILIAN_DATABASE_TOKEN", "");
  db->database_token = malloc(strlen(token_tmp) + 1);
  if (db->database_token)
    strcpy(db->database_token, token_tmp);

  db->backup_interval =
      get_env_int_default("ARKILIAN_BACKUP_INTERVAL", DEFAULT_BACKUP_INTERVAL);
  db->backup_enabled = get_env_int_default("ARKILIAN_ENABLE_BACKUP", 1);

  // Initialize write mutex and interception state
#ifndef _WIN32
  pthread_mutex_init(&db->write_mutex, NULL);
#else
  db->write_mutex = CreateMutex(NULL, FALSE, NULL);
#endif
  db->in_write_txn = 0;
  db->write_stmt_index = -1;
  db->current_write_sql[0] = '\0';
  db->last_step_rc = 0;

  int rc = sqlite3_open_v2(
      db_path, &db->handle,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);

  if (rc != SQLITE_OK) {
    db->handle = NULL;
    // Capture error using error code (no handle needed)
    const char *err = sqlite3_errstr(rc);
    strncpy(db->last_error_msg, err, sizeof(db->last_error_msg) - 1);
    db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
    *db_ptr = db;
    return 1;
  }

  // Apply performance and safety pragmas
  sqlite3_exec(db->handle, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA busy_timeout=5000;", NULL, NULL, NULL);
  sqlite3_exec(db->handle, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

  // Ensure internal tables exist
  sqlite3_exec(db->handle,
    "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
    NULL, NULL, NULL);
  sqlite3_exec(db->handle,
    "CREATE TABLE IF NOT EXISTS _arkilian_log (lsn INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, op INTEGER, tbl TEXT, sql TEXT);",
    NULL, NULL, NULL);

  // Prepare cached statements for write interception (avoid per-write compile)
  sqlite3_prepare_v2(db->handle,
    "INSERT INTO _arkilian_log (ts, op, tbl, sql) VALUES (?1, ?2, ?3, ?4);",
    -1, &db->log_insert_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "BEGIN;", -1, &db->begin_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "COMMIT;", -1, &db->commit_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "ROLLBACK;", -1, &db->rollback_stmt, NULL);

  // Allocate log buffer for batched writes
  db->log_buffer = malloc(sizeof(*db->log_buffer) * LOG_BUFFER_CAPACITY);
  db->log_buffer_count = 0;

  db->is_open = 1;
  db->shutdown_requested = 0;
  *db_ptr = db;

  // Start backup thread if enabled
  if (db->backup_enabled) {
#ifdef _WIN32
    db->backup_thread_handle =
        CreateThread(NULL, 0, run_hourly_backup, db, 0, NULL);
    if (db->backup_thread_handle == NULL) {
      fprintf(stderr, "Failed to create backup thread\n");
    }
#else
    db->backup_thread_running = 0;
    if (pthread_create(&db->backup_thread_id, NULL, run_hourly_backup, db) !=
        0) {
      fprintf(stderr, "Failed to create backup thread\n");
    } else {
      db->backup_thread_running = 1;
    }
#endif
  } else if (db->backup_enabled) {
    fprintf(stderr, "Backup disabled\n");
  }

  return 0;
}

void db_close(arkilian *db) {
  if (!db)
    return;

  // Signal backup thread to stop
  db->shutdown_requested = 1;

  // Wait for backup thread to finish if it's running
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

  for (int i = 0; i < db->stmt_count; i++) {
    if (db->stmts[i]) {
      sqlite3_finalize(db->stmts[i]);
    }
  }
  free(db->stmts);
  db->stmts = NULL;
  db->stmt_count = 0;
  db->stmt_capacity = 0;
  db->stmt_current = -1;

  if (db->is_open && db->handle) {
    // Rollback any open write transaction and release mutex
    if (db->in_write_txn) {
      sqlite3_step(db->rollback_stmt);
      sqlite3_reset(db->rollback_stmt);
      db->in_write_txn = 0;
      db->write_stmt_index = -1;
#ifndef _WIN32
      pthread_mutex_unlock(&db->write_mutex);
#else
      ReleaseMutex(db->write_mutex);
#endif
    }

    // Flush any remaining log entries
    if (db->log_buffer_count > 0) {
      sqlite3_step(db->begin_stmt);
      sqlite3_reset(db->begin_stmt);
      for (int i = 0; i < db->log_buffer_count; i++) {
        sqlite3_bind_int64(db->log_insert_stmt, 1, db->log_buffer[i].ts);
        sqlite3_bind_int(db->log_insert_stmt, 2, db->log_buffer[i].op);
        sqlite3_bind_text(db->log_insert_stmt, 3, db->log_buffer[i].tbl,
                          -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(db->log_insert_stmt, 4, db->log_buffer[i].sql,
                          -1, SQLITE_TRANSIENT);
        sqlite3_step(db->log_insert_stmt);
        sqlite3_reset(db->log_insert_stmt);
      }
      sqlite3_step(db->commit_stmt);
      sqlite3_reset(db->commit_stmt);
      db->log_buffer_count = 0;
    }

    // Finalize cached interception statements
    if (db->log_insert_stmt) { sqlite3_finalize(db->log_insert_stmt); db->log_insert_stmt = NULL; }
    if (db->begin_stmt)      { sqlite3_finalize(db->begin_stmt);      db->begin_stmt = NULL; }
    if (db->commit_stmt)     { sqlite3_finalize(db->commit_stmt);     db->commit_stmt = NULL; }
    if (db->rollback_stmt)   { sqlite3_finalize(db->rollback_stmt);   db->rollback_stmt = NULL; }

    sqlite3_close(db->handle);
    db->handle = NULL;
    db->is_open = 0;
  }
  if (db->log_buffer)
    free(db->log_buffer);
  if (db->backup_path)
    free(db->backup_path);
  if (db->signed_url_endpoint)
    free(db->signed_url_endpoint);
  if (db->database_token)
    free(db->database_token);

  // Destroy write mutex
#ifndef _WIN32
  pthread_mutex_destroy(&db->write_mutex);
#else
  if (db->write_mutex) CloseHandle(db->write_mutex);
#endif

  free(db);
}

const char *db_errmsg(arkilian *db) {
  if (db->last_error_msg[0] != '\0') {
    return db->last_error_msg;
  }
  if (db->handle) {
    return sqlite3_errmsg(db->handle);
  }
  return "Unknown error";
}

sqlite3 *db_get_handle(arkilian *db) { return db->handle; }

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
      if (rc != SQLITE_OK) {
        sqlite3_sleep(100);
      }
    }
  } while (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED);

  (void)sqlite3_backup_finish(pBackup);

  if (rc == SQLITE_DONE) {
    rc = SQLITE_OK;
  } else {
    fprintf(stderr, "Backup Error: Step failed with code %d: %s\n", rc,
            sqlite3_errmsg(pDest));
  }

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

    // Check for shutdown request
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
      printf("Backup file made\n");
      char *signed_url = get_signed_url(
          db->signed_url_endpoint, db->database_token, &db->shutdown_requested);
      printf("----> %s", signed_url);
      if (signed_url && signed_url != NULL && strlen(signed_url) > 5) {
        int upload_status =
            upload_to_s3(signed_url, db->backup_path, db->database_token);
        if (upload_status == 0) {
          printf("S3 Upload Successful!\n");
        } else {
          fprintf(stderr, "S3 Upload Failed with status: %d\n", upload_status);
        }
        free(signed_url);
      } else {
        fprintf(stderr, "Backup failed: Signed URL is null or empty\n");
      }
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

  // Check shutdown flag - return 0 to abort transfer
  if (mem->shutdown_flag) {
    return 0;
  }

  size_t realsize = size * nmemb;
  char *ptr = realloc(mem->response, mem->size + realsize + 1);
  if (!ptr)
    return 0;
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
  chunk.shutdown_flag = shutdown_flag ? *shutdown_flag : 0;

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, api_endpoint);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    // Timeout settings - fail fast if endpoint is unresponsive
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L); // 10 second total timeout
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                     5L); // 5 second connection timeout

    // Set Bearer token authorization header
    struct curl_slist *headers = NULL;
    if (token && strlen(token) > 0) {
      char auth_header[512];
      snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s",
               token);
      headers = curl_slist_append(headers, auth_header);
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }

    CURLcode res = curl_easy_perform(curl);
    if (headers)
      curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res == CURLE_OK)
      return chunk.response;
  }
  free(chunk.response);
  return NULL;
}

int upload_to_s3(const char *signed_url, const char *file_path,
                 const char *token) {
  CURL *curl = curl_easy_init();
  if (!curl)
    return 1;

  FILE *fd = fopen(file_path, "rb");
  if (!fd)
    return 1;

  // Get file size
  fseek(fd, 0L, SEEK_END);
  long file_size = ftell(fd);
  printf("Backup size: %fmb\n", ((float)file_size / 1024.0f) / 1024.0f);
  rewind(fd);

  curl_easy_setopt(curl, CURLOPT_URL, signed_url);
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_READDATA, fd);
  curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_size);

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/x-sqlite3");
  if (token && strlen(token) > 0) {
    char auth_header[512];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s",
             token);
    headers = curl_slist_append(headers, auth_header);
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  // Timeout settings
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

  CURLcode res = curl_easy_perform(curl);

  fclose(fd);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return (res == CURLE_OK) ? 0 : 1;
}

// Parse SQL to extract operation type and table name.
// op: 0=INSERT, 1=UPDATE, 2=DELETE, 3=DDL
static void parse_sql_op_tbl(const char *sql, int *op, char *tbl, size_t tblsz) {
  *op = 3; // default DDL
  tbl[0] = '\0';

  // Skip leading whitespace and comments
  while (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r') sql++;
  if (*sql == '-' && *(sql+1) == '-') return; // comment
  if (*sql == '/' && *(sql+1) == '*') return; // block comment

  // Match first keyword (case-insensitive prefix match)
  #define MATCH(s, literal) (strncasecmp(s, literal, strlen(literal)) == 0)

  if (MATCH(sql, "INSERT")) {
    *op = 0;
    sql += 6; while (*sql == ' ') sql++;
    if (MATCH(sql, "INTO")) { sql += 4; while (*sql == ' ') sql++; }
    if (MATCH(sql, "OR")) { sql += 2; while (*sql == ' ') sql++;
      if (MATCH(sql, "REPLACE") || MATCH(sql, "ROLLBACK") ||
          MATCH(sql, "ABORT") || MATCH(sql, "FAIL") || MATCH(sql, "IGNORE"))
        { while (*sql && *sql != ' ') sql++; while (*sql == ' ') sql++; }
    }
  } else if (MATCH(sql, "UPDATE")) {
    *op = 1;
    sql += 6; while (*sql == ' ') sql++;
    if (MATCH(sql, "OR")) { sql += 2; while (*sql == ' ') sql++;
      while (*sql && *sql != ' ') sql++; while (*sql == ' ') sql++; }
  } else if (MATCH(sql, "DELETE")) {
    *op = 2;
    sql += 6; while (*sql == ' ') sql++;
    if (MATCH(sql, "FROM")) { sql += 4; while (*sql == ' ') sql++; }
  } else if (MATCH(sql, "REPLACE")) {
    *op = 0; // treat REPLACE as INSERT
    sql += 7; while (*sql == ' ') sql++;
    if (MATCH(sql, "INTO")) { sql += 4; while (*sql == ' ') sql++; }
  } else {
    // DDL: CREATE, ALTER, DROP, etc.
    *op = 3;
    if (MATCH(sql, "CREATE")) { sql += 6; while (*sql == ' ') sql++;
      if (MATCH(sql, "TABLE") || MATCH(sql, "INDEX") ||
          MATCH(sql, "VIEW") || MATCH(sql, "TRIGGER")) {
        while (*sql && *sql != ' ') sql++; while (*sql == ' ') sql++;
      }
    } else if (MATCH(sql, "DROP")) { sql += 4; while (*sql == ' ') sql++;
      if (MATCH(sql, "TABLE") || MATCH(sql, "INDEX") ||
          MATCH(sql, "VIEW") || MATCH(sql, "TRIGGER")) {
        while (*sql && *sql != ' ') sql++; while (*sql == ' ') sql++;
      }
      if (MATCH(sql, "IF")) {
        while (*sql && *sql != ' ') sql++; while (*sql == ' ') sql++;
        while (*sql && *sql != ' ') sql++; while (*sql == ' ') sql++;
      }
    } else if (MATCH(sql, "ALTER")) { sql += 5; while (*sql == ' ') sql++;
      if (MATCH(sql, "TABLE")) { sql += 5; while (*sql == ' ') sql++; }
    }
  }
  #undef MATCH

  // Extract table name (stop at space, paren, semicolon, or end)
  if (*sql == '`' || *sql == '"' || *sql == '[') {
    char quote = (*sql == '[') ? ']' : *sql;
    sql++; // skip opening quote
    size_t i = 0;
    while (*sql && *sql != quote && i < tblsz - 1)
      tbl[i++] = *sql++;
    tbl[i] = '\0';
  } else {
    size_t i = 0;
    while (*sql && *sql != ' ' && *sql != '\t' && *sql != '\n' &&
           *sql != '(' && *sql != ';' && *sql != '\0' && i < tblsz - 1)
      tbl[i++] = *sql++;
    tbl[i] = '\0';
  }
}

// Flush buffered log entries to disk in a single transaction
static void log_buffer_flush(arkilian *db) {
  if (db->log_buffer_count == 0) return;

  sqlite3_step(db->begin_stmt);
  sqlite3_reset(db->begin_stmt);

  for (int i = 0; i < db->log_buffer_count; i++) {
    sqlite3_bind_int64(db->log_insert_stmt, 1, db->log_buffer[i].ts);
    sqlite3_bind_int(db->log_insert_stmt, 2, db->log_buffer[i].op);
    sqlite3_bind_text(db->log_insert_stmt, 3, db->log_buffer[i].tbl,
                      -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(db->log_insert_stmt, 4, db->log_buffer[i].sql,
                      -1, SQLITE_TRANSIENT);
    sqlite3_step(db->log_insert_stmt);
    sqlite3_reset(db->log_insert_stmt);
  }

  sqlite3_step(db->commit_stmt);
  sqlite3_reset(db->commit_stmt);

  db->log_buffer_count = 0;
}

// Append a log entry to the in-memory buffer. Flushes if buffer is full.
static void log_buffer_append(arkilian *db, const char *sql) {
  if (db->log_buffer_count >= LOG_BUFFER_CAPACITY) {
    log_buffer_flush(db);
  }

  struct log_entry *e = &db->log_buffer[db->log_buffer_count++];
  e->ts = (sqlite3_int64)time(NULL);
  parse_sql_op_tbl(sql, &e->op, e->tbl, sizeof(e->tbl));
  strncpy(e->sql, sql, sizeof(e->sql) - 1);
  e->sql[sizeof(e->sql) - 1] = '\0';
}

int db_exec(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql)
    return SQLITE_ERROR;

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db->handle, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    return rc;
  }

  // Read-only statements execute without the write mutex
  if (sqlite3_stmt_readonly(stmt)) {
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc;
  }

  // Write path: acquire mutex, BEGIN → execute → log → COMMIT
#ifndef _WIN32
  pthread_mutex_lock(&db->write_mutex);
#else
  WaitForSingleObject(db->write_mutex, INFINITE);
#endif

  rc = sqlite3_step(db->begin_stmt);
  sqlite3_reset(db->begin_stmt);
  if (rc != SQLITE_DONE) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    sqlite3_finalize(stmt);
#ifndef _WIN32
    pthread_mutex_unlock(&db->write_mutex);
#else
    ReleaseMutex(db->write_mutex);
#endif
    return rc;
  }

  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  if (rc != SQLITE_OK && rc != SQLITE_DONE) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    sqlite3_step(db->rollback_stmt);
    sqlite3_reset(db->rollback_stmt);
#ifndef _WIN32
    pthread_mutex_unlock(&db->write_mutex);
#else
    ReleaseMutex(db->write_mutex);
#endif
    return rc;
  }

  rc = sqlite3_step(db->commit_stmt);
  sqlite3_reset(db->commit_stmt);
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

  db->has_new_writes = 1;

  // Append oplog entry to in-memory buffer (flushed independently)
  log_buffer_append(db, sql);

#ifndef _WIN32
  pthread_mutex_unlock(&db->write_mutex);
#else
  ReleaseMutex(db->write_mutex);
#endif

  return SQLITE_DONE;
}

int db_prepare(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql)
    return SQLITE_ERROR;

  if (db->stmt_count >= db->stmt_capacity) {
    int new_cap = (db->stmt_capacity == 0) ? 4 : db->stmt_capacity * 2;
    sqlite3_stmt **new_arr =
        realloc(db->stmts, (size_t)new_cap * sizeof(sqlite3_stmt *));
    if (!new_arr)
      return SQLITE_NOMEM;
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

  // If this is a write statement, acquire mutex and begin transaction
  if (!sqlite3_stmt_readonly(stmt)) {
    // Only one write transaction at a time
    if (db->in_write_txn) {
      sqlite3_finalize(stmt);
      snprintf(db->last_error_msg, sizeof(db->last_error_msg),
               "Only one write transaction allowed at a time");
      return SQLITE_BUSY;
    }
#ifndef _WIN32
    pthread_mutex_lock(&db->write_mutex);
#else
    WaitForSingleObject(db->write_mutex, INFINITE);
#endif
    int begin_rc = sqlite3_step(db->begin_stmt);
    sqlite3_reset(db->begin_stmt);
    if (begin_rc != SQLITE_DONE) {
      snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
               sqlite3_errmsg(db->handle));
      sqlite3_finalize(stmt);
#ifndef _WIN32
      pthread_mutex_unlock(&db->write_mutex);
#else
      ReleaseMutex(db->write_mutex);
#endif
      return begin_rc;
    }
    db->in_write_txn = 1;
    db->write_stmt_index = db->stmt_count;
    strncpy(db->current_write_sql, sql, sizeof(db->current_write_sql) - 1);
    db->current_write_sql[sizeof(db->current_write_sql) - 1] = '\0';
  }

  db->stmts[db->stmt_count] = stmt;
  db->stmt_current = db->stmt_count;
  db->stmt_count++;
  return rc;
}

int db_use_stmt(arkilian *db, int index) {
  if (!db || index < 0 || index >= db->stmt_count)
    return SQLITE_ERROR;
  if (!db->stmts[index])
    return SQLITE_ERROR;
  db->stmt_current = index;
  return SQLITE_OK;
}

int db_stmt_count(arkilian *db) {
  if (!db)
    return 0;
  return db->stmt_count;
}

static sqlite3_stmt *get_current_stmt(arkilian *db) {
  if (!db || db->stmt_current < 0 || db->stmt_current >= db->stmt_count)
    return NULL;
  return db->stmts[db->stmt_current];
}

int db_step(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return SQLITE_ERROR;
  int rc = sqlite3_step(stmt);
  db->last_step_rc = rc;
  return rc;
}

int db_finalize(arkilian *db) {
  if (!db)
    return SQLITE_ERROR;
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (stmt) {
    int is_write = db->in_write_txn;
    sqlite3_finalize(stmt);
    db->stmts[db->stmt_current] = NULL;

    if (is_write && db->stmt_current == db->write_stmt_index) {
      int step_ok = (db->last_step_rc == SQLITE_DONE ||
                     db->last_step_rc == SQLITE_ROW ||
                     db->last_step_rc == SQLITE_OK);
      if (step_ok) {
        sqlite3_step(db->commit_stmt);
        sqlite3_reset(db->commit_stmt);
        db->has_new_writes = 1;

        // Append oplog entry to in-memory buffer (flushed independently)
        log_buffer_append(db, db->current_write_sql);
      } else {
        snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
                 sqlite3_errmsg(db->handle));
        sqlite3_step(db->rollback_stmt);
        sqlite3_reset(db->rollback_stmt);
      }

      db->in_write_txn = 0;
      db->write_stmt_index = -1;
      db->current_write_sql[0] = '\0';
#ifndef _WIN32
      pthread_mutex_unlock(&db->write_mutex);
#else
      ReleaseMutex(db->write_mutex);
#endif
    }
  }
  return SQLITE_OK;
}

int db_reset(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return SQLITE_ERROR;
  return sqlite3_reset(stmt);
}

int db_column_count(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return 0;
  return sqlite3_column_count(stmt);
}

const char *db_column_name(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return NULL;
  return (const char *)sqlite3_column_name(stmt, col);
}

const char *db_column_text(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return NULL;
  return (const char *)sqlite3_column_text(stmt, col);
}

int db_column_int(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return 0;
  return sqlite3_column_int(stmt, col);
}

double db_column_double(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return 0.0;
  return sqlite3_column_double(stmt, col);
}

int db_bind_text(arkilian *db, int idx, const char *val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt || !val)
    return SQLITE_ERROR;
  return sqlite3_bind_text(stmt, idx, val, -1, SQLITE_TRANSIENT);
}

int db_bind_int(arkilian *db, int idx, int val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return SQLITE_ERROR;
  return sqlite3_bind_int(stmt, idx, val);
}

int db_bind_double(arkilian *db, int idx, double val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt)
    return SQLITE_ERROR;
  return sqlite3_bind_double(stmt, idx, val);
}

int db_set_token(arkilian *db, const char *token) {
  if (!db || !token)
    return 1;
  if (db->database_token)
    free(db->database_token);
  db->database_token = malloc(strlen(token) + 1);
  if (!db->database_token)
    return 1;
  strcpy(db->database_token, token);
  return 0;
}
