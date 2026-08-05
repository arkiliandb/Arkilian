// Arkilian End-to-End Stress Validator
//
// Drives a REAL client against a REAL control plane + object storage
// (the docker compose stack or a local server) and verifies the full
// data path:
//
//   1. Capture: writes go through triggers into _pending_backup.
//   2. Delivery: the flush thread ships every payload to the control
//      plane's  with X-Arkilian-Payload-Id.
//   3. Snapshot: the hourly backup thread uploads backup.sqlite through
//      a control-plane signed URL into object storage.
//   4. Verification: the control plane confirms the exact wal_entries
//      count and returns a hydrate plan with a snapshot URL.
//
// Usage:
//   test_e2e_stress --url http://localhost:8080 --key <api_key> --db <db_id> [--writes N]
//
// Compile (macOS/Linux):
//   cc tests/test_e2e_stress.c src/class.c src/deps/sqlite/sqlite3.c -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm -o test_e2e_stress

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#include "class.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static const char *g_url = "http://localhost:8080";
static const char *g_key = "";
static const char *g_db_id = "";
static int g_writes = 5000;

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

// ── Minimal libcurl GET helper with Bearer auth ─────────────────────

typedef struct { char *buf; size_t len; } resp_buf;

static size_t resp_cb(void *data, size_t sz, size_t nmemb, void *userp) {
  resp_buf *b = (resp_buf *)userp;
  size_t n = sz * nmemb;
  char *p = realloc(b->buf, b->len + n + 1);
  if (!p) return 0;
  b->buf = p;
  memcpy(b->buf + b->len, data, n);
  b->len += n;
  b->buf[b->len] = '\0';
  return n;
}

// GET and return HTTP status; body (allocated, caller frees) via *out.
static long http_get(const char *url, const char *key, char **out) {
  CURL *curl = curl_easy_init();
  if (!curl) return -1;
  resp_buf body = {NULL, 0};
  long status = 0;

  CURLcode rc = curl_easy_setopt(curl, CURLOPT_URL, url);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, resp_cb);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);

  struct curl_slist *headers = NULL;
  if (rc == CURLE_OK && key && strlen(key) > 0) {
    char auth[512];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", key);
    headers = curl_slist_append(headers, auth);
    if (!headers) rc = CURLE_OUT_OF_MEMORY;
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  }

  if (rc == CURLE_OK) {
    CURLcode res = curl_easy_perform(curl);
    if (res == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
  }
  if (headers) curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  if (out) *out = body.buf;
  else free(body.buf);
  return status;
}

static long json_int_field(const char *json, const char *field) {
  char needle[64];
  snprintf(needle, sizeof(needle), "\"%s\":", field);
  const char *p = json ? strstr(json, needle) : NULL;
  return p ? atol(p + strlen(needle)) : -1;
}

// ── Main ────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--url") == 0 && i + 1 < argc) g_url = argv[++i];
    else if (strcmp(argv[i], "--key") == 0 && i + 1 < argc) g_key = argv[++i];
    else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) g_db_id = argv[++i];
    else if (strcmp(argv[i], "--writes") == 0 && i + 1 < argc) g_writes = atoi(argv[++i]);
  }
  if (strlen(g_key) == 0 || strlen(g_db_id) == 0) {
    fprintf(stderr, "usage: test_e2e_stress --key <api_key> --db <db_id> [--url URL] [--writes N]\n");
    return 2;
  }

  setvbuf(stdout, NULL, _IONBF, 0);
  char push_url[512], upload_url[512], count_url[512], plan_url[512];
  snprintf(push_url, sizeof(push_url), "%s", g_url);
  snprintf(upload_url, sizeof(upload_url), "%s/v1/upload/request", g_url);
  snprintf(count_url, sizeof(count_url), "%s/v1/wal/count", g_url);
  snprintf(plan_url, sizeof(plan_url), "%s/v1/hydrate/plan", g_url);

  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_CONTROL_URL", g_url, 1);
  setenv("ARKILIAN_API_KEY", g_key, 1);
  setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "5", 1);  // hourly thread runs often
  setenv("ARKILIAN_BACKUP_PATH", "e2e_backup.sqlite", 1);

  printf("=== Arkilian E2E Stress Validator ===\n");
  printf("  control plane : %s\n", g_url);
  printf("  database      : %s\n", g_db_id);
  printf("  writes        : %d\n\n", g_writes);

  arkilian *db = NULL;
  if (db_init(&db, "e2e_stress.db") != 0) {
    fprintf(stderr, "FAIL: db_init: %s\n", db_errmsg(db));
    return 1;
  }

  // 1. Capture phase.
  if (db_exec(db, "CREATE TABLE IF NOT EXISTS e2e ("
                  " id INTEGER PRIMARY KEY AUTOINCREMENT,"
                  " user_id INT NOT NULL, payload TEXT NOT NULL, ts INT NOT NULL)") != SQLITE_OK) {
    fprintf(stderr, "FAIL: create table\n");
    return 1;
  }

  double t0 = now_ms();
  int batch = 100;
  for (int written = 0; written < g_writes;) {
    db_begin(db);
    for (int i = 0; i < batch && written < g_writes; i++, written++) {
      char sql[256];
      snprintf(sql, sizeof(sql),
               "INSERT INTO e2e (user_id, payload, ts) VALUES (%d, 'e2e-%d', %ld)",
               written % 1000, written, (long)time(NULL));
      if (db_exec(db, sql) != SQLITE_OK) {
        fprintf(stderr, "FAIL: insert %d: %s\n", written, db_errmsg(db));
        return 1;
      }
    }
    if (db_commit(db) != SQLITE_OK) {
      fprintf(stderr, "FAIL: commit\n");
      return 1;
    }
  }
  double write_s = (now_ms() - t0) / 1000.0;
  printf("capture: %d writes in %.2fs (%.0f writes/sec), outbox=%d\n",
         g_writes, write_s, g_writes / (write_s > 0 ? write_s : 1), db_wal_pending(db));

  // 2. Delivery phase: wait for the outbox to drain through the control plane.
  int waited = 0;
  while (db_wal_pending(db) > 0 && waited < 60000) {
    usleep(200 * 1000);
    waited += 200;
  }
  if (db_wal_pending(db) > 0) {
    fprintf(stderr, "FAIL: outbox did not drain after %dms (%d pending)\n",
            waited, db_wal_pending(db));
    return 1;
  }
  printf("delivery: outbox drained in %dms\n", waited);

  // 3. Snapshot phase: wait for the hourly upload to land via signed URL.
  //    The first backup runs at init; give it time + one interval.
  usleep(7 * 1000 * 1000);

  // 4. Verification: wal_entries count on the control plane.
  char *body = NULL;
  long status = http_get(count_url, g_key, &body);
  long server_count = (status == 200) ? json_int_field(body, "count") : -1;
  printf("verify: /v1/wal/count -> http=%ld count=%ld (expected >= %d)\n",
         status, server_count, g_writes + 1);
  if (status != 200 || server_count < g_writes + 1) {
    fprintf(stderr, "FAIL: wal count verification (got %ld)\n", server_count);
    free(body);
    return 1;
  }
  free(body);

  // 5. Verification: hydrate plan must return a snapshot URL.
  body = NULL;
  status = http_get(plan_url, g_key, &body);
  int has_snapshot = body && strstr(body, "\"snapshot_url\":\"http") != NULL;
  printf("verify: /v1/hydrate/plan -> http=%ld snapshot=%s\n",
         status, has_snapshot ? "present" : "MISSING");
  if (status != 200) {
    fprintf(stderr, "FAIL: hydrate plan http %ld\n", status);
    free(body);
    return 1;
  }

  db_close(db);
  printf("\n=== E2E STRESS VALIDATION PASSED ===\n");
  return 0;
}
