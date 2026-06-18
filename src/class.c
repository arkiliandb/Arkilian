// Arkilian SQLite Wrapper - C API

// Enable POSIX features for portable functions
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
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
#endif

// ── Fixed-Size Bounded Queue for out-of-band WAL shipping ──────────

#define MAX_QUEUE_SIZE 2048

typedef struct {
  char *sql_queries[MAX_QUEUE_SIZE];
  int head;             // consumer read index
  int tail;             // producer insert index
  int count;            // number of live entries
  int shutdown;
  pthread_mutex_t lock;
  pthread_cond_t  not_full;
  pthread_cond_t  not_empty;
} BoundedLogQueue;

static void queue_init(BoundedLogQueue *q) {
  memset(q->sql_queries, 0, sizeof(q->sql_queries));
  q->head = 0;
  q->tail = 0;
  q->count = 0;
  q->shutdown = 0;
  pthread_mutex_init(&q->lock, NULL);
  pthread_cond_init(&q->not_full, NULL);
  pthread_cond_init(&q->not_empty, NULL);
}

static void queue_destroy(BoundedLogQueue *q) {
  // Free any remaining entries
  for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
    if (q->sql_queries[i]) { free(q->sql_queries[i]); q->sql_queries[i] = NULL; }
  }
  pthread_mutex_destroy(&q->lock);
  pthread_cond_destroy(&q->not_full);
  pthread_cond_destroy(&q->not_empty);
}

// Producer: push one SQL string. Blocks with backpressure if queue is full.
static void queue_push(BoundedLogQueue *q, const char *sql) {
  pthread_mutex_lock(&q->lock);
  while (q->count == MAX_QUEUE_SIZE && !q->shutdown)
    pthread_cond_wait(&q->not_full, &q->lock);
  if (q->shutdown) { pthread_mutex_unlock(&q->lock); return; }
  q->sql_queries[q->tail] = strdup(sql);
  q->tail = (q->tail + 1) % MAX_QUEUE_SIZE;
  q->count++;
  pthread_cond_signal(&q->not_empty);
  pthread_mutex_unlock(&q->lock);
}

// Consumer: drain up to max entries. Returns number drained.
// dst[] must have space for max char* pointers.
static int queue_drain(BoundedLogQueue *q, char **dst, int max, int block_ms) {
  int drained = 0;
  pthread_mutex_lock(&q->lock);

  if (q->count == 0 && !q->shutdown && block_ms > 0) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec  += block_ms / 1000;
    ts.tv_nsec += (block_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
    pthread_cond_timedwait(&q->not_empty, &q->lock, &ts);
  }

  while (q->count > 0 && drained < max) {
    dst[drained] = q->sql_queries[q->head];
    q->sql_queries[q->head] = NULL;
    q->head = (q->head + 1) % MAX_QUEUE_SIZE;
    q->count--;
    drained++;
  }

  if (drained > 0)
    pthread_cond_signal(&q->not_full);
  pthread_mutex_unlock(&q->lock);
  return drained;
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
  int last_step_rc;
  // Cached statements
  sqlite3_stmt *begin_stmt;
  sqlite3_stmt *commit_stmt;
  sqlite3_stmt *rollback_stmt;
  // Bounded queue for out-of-band WAL shipping
  BoundedLogQueue log_queue;
  // Flush thread
  int flush_interval_ms;
#ifndef _WIN32
  pthread_t flush_thread_id;
  int flush_thread_running;
#else
  HANDLE flush_thread_handle;
#endif
};

struct Memory {
  char *response;
  size_t size;
  int shutdown_flag;
};

// ── Config defaults ─────────────────────────────────────────────────

#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600
#define DEFAULT_SIGNED_URL_ENDPOINT "https://api.arkilian.com/get-signed-url"
#define DEFAULT_FLUSH_INTERVAL_MS 1000

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
  const char *token    = db->database_token;

  while (1) {
    // Sleep between drain cycles
    {
      int ms = db->flush_interval_ms;
      if (ms > 0) {
        struct timespec ts;
        ts.tv_sec  = ms / 1000;
        ts.tv_nsec = (ms % 1000) * 1000000L;
        nanosleep(&ts, NULL);
      }
    }
    if (db->log_queue.shutdown) break;

    // Drain everything currently in the queue (non-blocking)
    char *batch[256];
    int n;
    while ((n = queue_drain(&db->log_queue, batch, 256, 0)) > 0) {
      if (push_url && strlen(push_url) > 0) {
        // Build JSON payload and POST
        size_t json_cap = (size_t)n * 512 + 64;
        char *json = malloc(json_cap);
        if (json) {
          int off = snprintf(json, 64, "[");
          for (int i = 0; i < n; i++) {
            char *sql = batch[i];
            off += snprintf(json + off, json_cap - (size_t)off,
              "{\"sql\":\"");
            for (char *s = sql; *s && off < (int)json_cap - 32; s++) {
              if (*s == '"' || *s == '\\') json[off++] = '\\';
              json[off++] = *s;
            }
            off += snprintf(json + off, 16, "\"}%s", (i < n - 1) ? "," : "");
            free(sql);
          }
          off += snprintf(json + off, 8, "]");

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
            if (res != CURLE_OK)
              fprintf(stderr, "WAL push: %s\n", curl_easy_strerror(res));

            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
          }
          free(json);
        }
      } else {
        // No URL configured — drain to /dev/null (just free the entries)
        for (int i = 0; i < n; i++) free(batch[i]);
      }
    }

    if (db->log_queue.shutdown && db->log_queue.count == 0) break;
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

  // Internal tables
  sqlite3_exec(db->handle,
    "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
    NULL, NULL, NULL);

  // Cached transaction statements
  sqlite3_prepare_v2(db->handle, "BEGIN;", -1, &db->begin_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "COMMIT;", -1, &db->commit_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "ROLLBACK;", -1, &db->rollback_stmt, NULL);

  // Bounded queue for WAL shipping
  queue_init(&db->log_queue);

  // Flush interval
  db->flush_interval_ms =
    get_env_int_default("ARKILIAN_WAL_FLUSH_MS", DEFAULT_FLUSH_INTERVAL_MS);

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

  // Signal queue shutdown
  db->log_queue.shutdown = 1;
  pthread_cond_signal(&db->log_queue.not_empty);

  // Wait for flush thread (if running)
#ifndef _WIN32
  if (db->flush_thread_running) {
    pthread_cond_signal(&db->log_queue.not_empty);
    pthread_join(db->flush_thread_id, NULL);
    db->flush_thread_running = 0;
  }
#else
  // Windows: queue is pthread-based; flush thread not supported yet
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

  queue_destroy(&db->log_queue);

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
      printf("Backup file made\n");
      char *signed_url = get_signed_url(
          db->signed_url_endpoint, db->database_token, &db->shutdown_requested);
      if (signed_url && strlen(signed_url) > 5) {
        int upload_status =
            upload_to_s3(signed_url, db->backup_path, db->database_token);
        if (upload_status == 0) printf("S3 Upload Successful!\n");
        else fprintf(stderr, "S3 Upload Failed with status: %d\n", upload_status);
        free(signed_url);
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
  if (mem->shutdown_flag) return 0;
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
  chunk.shutdown_flag = shutdown_flag ? *shutdown_flag : 0;

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
  int in_batch = db->in_batch_txn;

  if (!in_batch) {
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
    queue_push(&db->log_queue, sql);
  } else {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s",
             sqlite3_errmsg(db->handle));
    if (in_batch) {
#ifndef _WIN32
      pthread_mutex_unlock(&db->write_mutex);
#else
      ReleaseMutex(db->write_mutex);
#endif
      return rc;
    }
  }

  if (!in_batch) {
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
    int is_write = db->in_write_txn;
    int is_this_write = (db->stmt_current == db->write_stmt_index);
    sqlite3_finalize(stmt);
    db->stmts[db->stmt_current] = NULL;

    if (is_write && is_this_write) {
      int ok = (db->last_step_rc == SQLITE_DONE ||
                db->last_step_rc == SQLITE_ROW ||
                db->last_step_rc == SQLITE_OK);
      if (ok) {
        db->has_new_writes = 1;
        queue_push(&db->log_queue, db->current_write_sql);
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

// ── Token management ────────────────────────────────────────────────

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
  return db->log_queue.count;
}
