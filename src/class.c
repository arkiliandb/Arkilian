// Arkilian SQLite Wrapper - C API

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
};

struct Memory {
  char *response;
  size_t size;
};

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

  const char *actualPath = (filename != NULL) ? filename : "app.sqlite";

  int rc = sqlite3_open_v2(
      actualPath, &db->handle,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);

  if (rc != SQLITE_OK) {
    // Capture the error
    const char *err = sqlite3_errmsg(db->handle);
    strncpy(db->last_error_msg, err, sizeof(db->last_error_msg) - 1);
    db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
    *db_ptr = db;
    return 1;
  }

  db->is_open = 1;
  db->has_new_writes = 0;
  db->last_error_msg[0] = '\0';
  *db_ptr = db;

  // Backup system
  // NOTE TO ME: Use this for test
  // run_hourly_backup(db);

  // NOTE TO ME: Use this for prod
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

  const char *actualPath = (zFilename != NULL) ? zFilename : "backup.sqlite";
  rc = sqlite3_open_v2(actualPath, &pDest, SQLITE_OPEN_READWRITE, NULL);

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
  const char *backup_path = "backup.sqlite";
  arkilian *db = (arkilian *)arg;
  while (1) {
#ifdef _WIN32
    Sleep(10 * 1000); // milliseconds
#else
    sleep(10); // seconds
#endif

    if (!db->is_open || db->handle == NULL) {
#ifdef _WIN32
      return 0;
#else
      pthread_exit(NULL);
#endif
    }
    int status = backup_database(db->handle, backup_path);
    if (status == SQLITE_OK) {
      printf("Backup file made\n");
      const char *api_endpoint = "http://localhost:3000/get-signed-url";
      char *signed_url = get_signed_url(api_endpoint);
      // printf("Signed URL: %s\n", signed_url);
      if (signed_url && signed_url != NULL && strlen(signed_url) > 5) {
        int upload_status = upload_to_s3(signed_url, backup_path);
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

// ============================================================================
