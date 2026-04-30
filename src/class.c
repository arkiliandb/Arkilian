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
#include <unistd.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// deps
#include "deps/sqlite/sqlite3.h"

struct arkilian {
  sqlite3 *handle;
  int last_error_code;
  char last_error_msg[256];
  int is_open;
  int has_new_writes;
  // Backup config
  char *backup_path;
  char *signed_url_endpoint;
  int backup_interval;
  int backup_enabled;
};

struct Memory {
  char *response;
  size_t size;
};

// Config defaults
#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600
#define DEFAULT_SIGNED_URL_ENDPOINT ""

// Helper to get env var with default
static const char* get_env_default(const char *env_var, const char *default_val) {
  const char *val = getenv(env_var);
  printf("%s = %s;\n", env_var, val);
  return (val && strlen(val) > 0) ? val : default_val;
}

// Helper to get env var as int with default
static int get_env_int_default(const char *env_var, int default_val) {
  const char *val = getenv(env_var);
  if (val && strlen(val) > 0) {
    printf("%s = %s;\n", env_var, val);
    return atoi(val);
  }
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
            setenv(key, val, 1); // Adds it to the environment
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
char *get_signed_url(const char *api_endpoint);
int upload_to_s3(const char *signed_url, const char *file_path);

int db_init(arkilian **db_ptr, const char *filename) {
  if (!db_ptr)
    return 1;
  arkilian *db = malloc(sizeof(arkilian));
  if (!db)
    return 1;
  db->is_open = 0;
  db->has_new_writes = 0;
  db->last_error_msg[0] = '\0';
  load_env();
  // Get configuration from environment
  const char *db_path = (filename != NULL) ? filename :
                        get_env_default("ARKILIAN_DB_PATH", DEFAULT_DB_PATH);

  // Backup configuration (using portable string copy instead of strdup)
  const char *backup_path_tmp = get_env_default("ARKILIAN_BACKUP_PATH", DEFAULT_BACKUP_PATH);
  db->backup_path = malloc(strlen(backup_path_tmp) + 1);
  if (db->backup_path) strcpy(db->backup_path, backup_path_tmp);
  
  const char *signed_url_tmp = get_env_default("ARKILIAN_SIGNED_URL_ENDPOINT", DEFAULT_SIGNED_URL_ENDPOINT);
  
  db->signed_url_endpoint = malloc(strlen(signed_url_tmp) + 1);
  if (db->signed_url_endpoint) strcpy(db->signed_url_endpoint, signed_url_tmp);
  
  db->backup_interval = get_env_int_default("ARKILIAN_BACKUP_INTERVAL", DEFAULT_BACKUP_INTERVAL);
  db->backup_enabled = get_env_int_default("ARKILIAN_ENABLE_BACKUP", 1);

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

  db->is_open = 1;
  *db_ptr = db;
  // Backup system
  // NOTE TO ME: Use this for test
  // run_hourly_backup(db);
  // Start backup thread if enabled
  if (db->backup_enabled && db->signed_url_endpoint && strlen(db->signed_url_endpoint) > 0) {
#ifdef _WIN32
    HANDLE hThread = CreateThread(NULL, 0, run_hourly_backup, db, 0, NULL);
    if (hThread == NULL) {
      fprintf(stderr, "Failed to create backup thread\n");
    } else {
      CloseHandle(hThread);
    }
#else
    pthread_t backup_thread;
    if (pthread_create(&backup_thread, NULL, run_hourly_backup, db) != 0) {
      fprintf(stderr, "Failed to create backup thread\n");
    } else {
      // Detach the thread so it cleans up after itself and runs independently
      pthread_detach(backup_thread);
    }
#endif
  } else if (db->backup_enabled) {
    fprintf(stderr, "Backup disabled: ARKILIAN_SIGNED_URL_ENDPOINT not set\n");
  }

  return 0;
}

void db_close(arkilian *db) {
  if (!db)
    return;
  if (db->is_open && db->handle) {
    sqlite3_close(db->handle);
    db->handle = NULL;
    db->is_open = 0;
  }
  if (db->backup_path) free(db->backup_path);
  if (db->signed_url_endpoint) free(db->signed_url_endpoint);
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

  const char *actualPath = (zFilename != NULL) ? zFilename : DEFAULT_BACKUP_PATH;
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

    if (!db->is_open || db->handle == NULL) {
#ifdef _WIN32
      return 0;
#else
      pthread_exit(NULL);
#endif
    }

    int status = backup_database(db->handle, db->backup_path);
    if (status == SQLITE_OK) {
      printf("Backup file made\n");
      char *signed_url = get_signed_url(db->signed_url_endpoint);
      printf("----> %s", signed_url);
      if (signed_url && signed_url != NULL && strlen(signed_url) > 5) {
        int upload_status = upload_to_s3(signed_url, db->backup_path);
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
  size_t realsize = size * nmemb;
  struct Memory *mem = (struct Memory *)userp;
  char *ptr = realloc(mem->response, mem->size + realsize + 1);
  if (!ptr)
    return 0;
  mem->response = ptr;
  memcpy(&(mem->response[mem->size]), data, realsize);
  mem->size += realsize;
  mem->response[mem->size] = 0;
  return realsize;
}

char *get_signed_url(const char *api_endpoint) {
  CURL *curl = curl_easy_init();
  struct Memory chunk = {malloc(1), 0};
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, api_endpoint);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res == CURLE_OK)
      return chunk.response;
  }
  free(chunk.response);
  return NULL;
}

int upload_to_s3(const char *signed_url, const char *file_path) {
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
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);

  fclose(fd);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return (res == CURLE_OK) ? 0 : 1;
}


