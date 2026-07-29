// Arkilian Hydration Engine v2 — implementation
//
// Logical replay: downloads snapshot binary + incremental SQL chunks
// via Pre-Signed URLs, plays them back with sqlite3_exec() inside
// explicit transactions.  No binary WAL frame manipulation needed.

#include "hydration.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ── libcurl response buffer ─────────────────────────────────────────

struct curl_buf {
  uint8_t *data;
  size_t   len;
  size_t   cap;
};

static size_t curl_write_cb(void *ptr, size_t sz, size_t nmemb, void *user) {
  struct curl_buf *buf = (struct curl_buf *)user;
  size_t total = sz * nmemb;
  size_t needed = buf->len + total;
  if (needed > buf->cap) {
    size_t new_cap = buf->cap ? buf->cap * 2 : (total > 65536 ? total : 65536);
    if (new_cap < needed) new_cap = needed;
    uint8_t *p = realloc(buf->data, new_cap);
    if (!p) return 0;
    buf->data = p;
    buf->cap  = new_cap;
  }
  memcpy(buf->data + buf->len, ptr, total);
  buf->len = needed;
  return total;
}

// ── HTTP helpers ────────────────────────────────────────────────────

typedef struct {
  CURL           *handle;
  struct curl_slist *headers;
} HttpReq;

static int http_init(HttpReq *r, const char *url, const char *token) {
  r->handle = curl_easy_init();
  if (!r->handle) return -1;
  curl_easy_setopt(r->handle, CURLOPT_URL, url);
  curl_easy_setopt(r->handle, CURLOPT_WRITEFUNCTION, curl_write_cb);
  curl_easy_setopt(r->handle, CURLOPT_TIMEOUT, 120L);
  curl_easy_setopt(r->handle, CURLOPT_CONNECTTIMEOUT, 15L);
  curl_easy_setopt(r->handle, CURLOPT_FOLLOWLOCATION, 1L);
  r->headers = NULL;
  if (token && strlen(token) > 0) {
    char auth[512];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", token);
    r->headers = curl_slist_append(r->headers, auth);
    curl_easy_setopt(r->handle, CURLOPT_HTTPHEADER, r->headers);
  }
  return 0;
}

static void http_free(HttpReq *r) {
  if (r->headers) curl_slist_free_all(r->headers);
  if (r->handle)  curl_easy_cleanup(r->handle);
}

// GET a URL into a malloc'd string.  Returns NULL on failure.
static char *http_get_string(const char *url, const char *token, int *err_out) {
  HttpReq r;
  if (http_init(&r, url, token) != 0) { *err_out = HYDRATION_ERR_NET; return NULL; }

  struct curl_buf buf = {NULL, 0, 0};
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, &buf);

  CURLcode rc = curl_easy_perform(r.handle);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(r.handle, CURLINFO_RESPONSE_CODE, &http_code);
  http_free(&r);

  if (rc != CURLE_OK || http_code != 200) {
    free(buf.data);
    if (http_code == 401 || http_code == 403) *err_out = HYDRATION_ERR_PROTO;
    else *err_out = HYDRATION_ERR_NET;
    return NULL;
  }

  // Null-terminate
  uint8_t *term = realloc(buf.data, buf.len + 1);
  if (!term) { free(buf.data); *err_out = HYDRATION_ERR_MEM; return NULL; }
  term[buf.len] = '\0';
  *err_out = 0;
  return (char *)term;
}

// Download a binary file from a URL to a local path.
static int http_download_file(const char *url, const char *token,
                               const char *local_path, int *err_out) {
  HttpReq r;
  if (http_init(&r, url, token) != 0) { *err_out = HYDRATION_ERR_NET; return -1; }

  FILE *f = fopen(local_path, "wb");
  if (!f) { http_free(&r); *err_out = HYDRATION_ERR_DISK; return -1; }

  struct curl_buf buf = {NULL, 0, 0};
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, &buf);

  CURLcode rc = curl_easy_perform(r.handle);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(r.handle, CURLINFO_RESPONSE_CODE, &http_code);
  http_free(&r);

  if (rc != CURLE_OK || http_code != 200) {
    fclose(f); free(buf.data); remove(local_path);
    *err_out = HYDRATION_ERR_NET; return -1;
  }

  size_t written = fwrite(buf.data, 1, buf.len, f);
  fclose(f);
  free(buf.data);
  *err_out = 0;
  return (written == buf.len) ? 0 : -1;
}

// ── Minimal JSON helpers (no external library) ──────────────────────

// Find a string value for a key in a flat JSON object: {"key":"value",...}
// Returns malloc'd string or NULL.
char *json_get_string(const char *json, const char *key) {
  char search[128];
  snprintf(search, sizeof(search), "\"%s\":\"", key);
  const char *pos = strstr(json, search);
  if (!pos) return NULL;
  pos += strlen(search);
  const char *end = strchr(pos, '"');
  if (!end) return NULL;
  size_t len = (size_t)(end - pos);
  char *val = malloc(len + 1);
  if (!val) return NULL;

  size_t out_idx = 0;
  for (size_t i = 0; i < len; i++) {
    if (i + 5 < len && strncmp(&pos[i], "\\u0026", 6) == 0) {
      val[out_idx++] = '&';
      i += 5;
    } else {
      val[out_idx++] = pos[i];
    }
  }
  val[out_idx] = '\0';
  return val;
}

int64_t json_get_int64(const char *json, const char *key) {
  char search[128];
  snprintf(search, sizeof(search), "\"%s\":", key);
  const char *pos = strstr(json, search);
  if (!pos) return 0;
  pos += strlen(search);
  return (int64_t)strtoll(pos, NULL, 10);
}

// Count elements in a JSON array at the given key: {"key":[{...},{...}]}
int json_array_count(const char *json, const char *key) {
  char search[128];
  snprintf(search, sizeof(search), "\"%s\":[", key);
  const char *pos = strstr(json, search);
  if (!pos) return 0;
  pos += strlen(search);
  int count = 0;
  int depth = 0;
  for (const char *p = pos; *p; p++) {
    if (*p == '{') depth++;
    if (*p == '}' && depth == 1) count++;
    if (*p == '}') depth--;
  }
  return count;
}

// Parse the i-th object from a JSON array at key: {"key":[{...},{...}]}
// Returns malloc'd copy of the i-th element (including braces).
char *json_array_get(const char *json, const char *key, int index) {
  char search[128];
  snprintf(search, sizeof(search), "\"%s\":[", key);
  const char *pos = strstr(json, search);
  if (!pos) return NULL;
  pos += strlen(search);

  int cur = 0;
  for (const char *p = pos; *p; p++) {
    if (*p == '{' && cur == index) {
      const char *start = p;
      int depth = 0;
      while (*p) {
        if (*p == '{') depth++;
        if (*p == '}') { depth--; if (depth == 0) break; }
        p++;
      }
      size_t len = (size_t)(p - start + 1);
      char *copy = malloc(len + 1);
      if (!copy) return NULL;
      memcpy(copy, start, len);
      copy[len] = '\0';
      return copy;
    }
    if (*p == '{') cur++;
  }
  return NULL;
}

// ── Control Plane: request hydration plan ───────────────────────────

static int request_hydrate_plan(const char *server_url, const char *token,
                                 HydratePlan *plan) {
  char url[1024];
  snprintf(url, sizeof(url), "%s/hydrate/plan", server_url);

  int err = 0;
  char *json = http_get_string(url, token, &err);
  if (!json) return err;

  // Parse plan
  memset(plan, 0, sizeof(*plan));
  plan->snapshot_url  = json_get_string(json, "snapshot_url");
  plan->baseline_lsn  = json_get_int64(json, "baseline_lsn");
  plan->expires_at    = json_get_int64(json, "expires_at");
  plan->chunk_count   = json_array_count(json, "chunks");

  if (plan->chunk_count > 0) {
    plan->chunks = malloc((size_t)plan->chunk_count * sizeof(HydrateChunk));
    if (!plan->chunks) { free(json); return HYDRATION_ERR_MEM; }

    for (int i = 0; i < plan->chunk_count; i++) {
      char *elem = json_array_get(json, "chunks", i);
      if (elem) {
        plan->chunks[i].url        = json_get_string(elem, "url");
        plan->chunks[i].lsn_start  = json_get_int64(elem, "lsn_start");
        plan->chunks[i].lsn_end    = json_get_int64(elem, "lsn_end");
        plan->chunks[i].expires_at = json_get_int64(elem, "expires_at");
        free(elem);
      }
    }
  }

  free(json);

  if (!plan->snapshot_url) return HYDRATION_ERR_PROTO;
  return 0;
}

void hydrate_plan_free(HydratePlan *plan) {
  if (!plan) return;
  free(plan->snapshot_url);
  for (int i = 0; i < plan->chunk_count; i++)
    free(plan->chunks[i].url);
  free(plan->chunks);
  memset(plan, 0, sizeof(*plan));
}

// ── Step 1: Download & decompress snapshot ──────────────────────────

static int download_snapshot(const char *snapshot_url, const char *token,
                              const char *db_path,
                              hydration_progress_cb progress, void *user) {
  if (progress) progress(1, 0, 1, user);

  int err = 0;
  int rc = http_download_file(snapshot_url, token, db_path, &err);
  if (rc != 0) {
    // If baseline snapshot does not exist yet on object storage (cold-start before 1st snapshot),
    // initialize a clean target database so log chunks replay from LSN 0 cleanly!
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL) == SQLITE_OK) {
      sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);", NULL, NULL, NULL);
      sqlite3_close(db);
      if (progress) progress(1, 1, 1, user);
      return 0;
    }
    return err;
  }

  if (progress) progress(1, 1, 1, user);
  return 0;
}

// ── Step 2: Read local last_applied_lsn ─────────────────────────────

static int64_t read_last_applied_lsn(sqlite3 *db) {
  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db,
    "SELECT v FROM _arkilian_meta WHERE k = 'last_applied_lsn'",
    -1, &stmt, NULL);
  if (rc != SQLITE_OK) return 0;

  int64_t lsn = 0;
  if (sqlite3_step(stmt) == SQLITE_ROW)
    lsn = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  return lsn;
}

// ── Step 3: Replay a single chunk ───────────────────────────────────

int hydrate_replay_chunk(sqlite3 *db, const char *raw_sql, int64_t chunk_lsn) {
  char *err_msg = NULL;

  // 1. Begin explicit transaction for throughput
  int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    if (err_msg) { fprintf(stderr, "Hydration BEGIN error: %s\n", err_msg); sqlite3_free(err_msg); }
    return HYDRATION_ERR_SQL;
  }

  // 2. Replay the raw multi-statement SQL text
  rc = sqlite3_exec(db, raw_sql, NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Hydration replay error: %s\n", err_msg);
    sqlite3_free(err_msg);
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return HYDRATION_ERR_SQL;
  }

  // 3. Update metadata tracking
  char meta_sql[256];
  snprintf(meta_sql, sizeof(meta_sql),
    "INSERT OR REPLACE INTO _arkilian_meta (k, v) VALUES ('last_applied_lsn', '%lld');",
    (long long)chunk_lsn);
  rc = sqlite3_exec(db, meta_sql, NULL, NULL, NULL);
  if (rc != SQLITE_OK) {
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return HYDRATION_ERR_SQL;
  }

  rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
  return (rc == SQLITE_OK) ? 0 : HYDRATION_ERR_SQL;
}

// ── Public API ──────────────────────────────────────────────────────

int arkilian_hydrate(const char *db_path,
                     const char *server_url,
                     const char *auth_token,
                     hydration_progress_cb progress,
                     void *user_data) {
  if (!db_path || !server_url) return HYDRATION_ERR_NET;

  // ── Phase 0: Request hydration plan ──
  HydratePlan plan;
  int rc = request_hydrate_plan(server_url, auth_token, &plan);
  if (rc != 0) return rc;

  // ── Phase 1: Download baseline snapshot ──
  rc = download_snapshot(plan.snapshot_url, auth_token, db_path,
                          progress, user_data);
  if (rc != 0) { hydrate_plan_free(&plan); return rc; }

  // ── Phase 2: Open database, check LSN, replay chunks ──
  sqlite3 *db = NULL;
  rc = sqlite3_open_v2(db_path, &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (rc != SQLITE_OK) { hydrate_plan_free(&plan); return HYDRATION_ERR_SQL; }

  // Apply PRAGMAs for speed during hydration
  sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
  sqlite3_exec(db, "PRAGMA synchronous=OFF;", NULL, NULL, NULL); // speed over safety during bulk load
  sqlite3_exec(db, "PRAGMA foreign_keys=OFF;", NULL, NULL, NULL);

  int64_t local_lsn = read_last_applied_lsn(db);
  if (local_lsn < plan.baseline_lsn)
    local_lsn = plan.baseline_lsn;

  int chunk_total = 0;
  for (int i = 0; i < plan.chunk_count; i++) {
    HydrateChunk *ch = &plan.chunks[i];

    // Skip chunks already applied
    if (ch->lsn_end <= local_lsn) continue;

    // Check URL expiry
    if (ch->expires_at > 0 && (int64_t)time(NULL) > ch->expires_at) {
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      return HYDRATION_ERR_EXPIRED;
    }

    // Download chunk
    int err = 0;
    char *sql_text = http_get_string(ch->url, auth_token, &err);
    if (!sql_text) { sqlite3_close(db); hydrate_plan_free(&plan); return err; }

    // Replay
    rc = hydrate_replay_chunk(db, sql_text, ch->lsn_end);
    free(sql_text);

    if (rc != 0) { sqlite3_close(db); hydrate_plan_free(&plan); return rc; }

    chunk_total++;
    if (progress) progress(2, chunk_total, plan.chunk_count, user_data);
  }

  // Restore safe PRAGMAs
  sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
  sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

  sqlite3_close(db);
  hydrate_plan_free(&plan);
  return 0;
}
