// Arkilian Hydration Engine v2 — implementation
//
// Logical replay: downloads snapshot binary + incremental SQL chunks
// via Pre-Signed URLs, plays them back with sqlite3_exec() inside
// explicit transactions.  No binary WAL frame manipulation needed.

#include "hydration.h"
#include <curl/curl.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

// Pre-signed object-storage URLs carry their own credentials in the
// query string — attaching our bearer token both leaks the credential
// to the storage host and can break signature validation.
static int url_is_presigned(const char *url) {
  if (!url) return 0;
  return strstr(url, "X-Amz-Signature=") != NULL ||
         strstr(url, "X-Amz-Credential=") != NULL ||
         strstr(url, "X-Goog-Signature=") != NULL ||
         strstr(url, "X-Goog-Credential=") != NULL ||
         strstr(url, "sig=") != NULL; /* Azure SAS */
}

void hydration_remove_db_files(const char *db_path) {
  if (!db_path) return;
  char side[4096];
  remove(db_path);
  static const char *const suffixes[] = {"-wal", "-shm", "-journal", NULL};
  for (int i = 0; suffixes[i]; i++) {
    int n = snprintf(side, sizeof(side), "%s%s", db_path, suffixes[i]);
    if (n > 0 && (size_t)n < sizeof(side)) remove(side);
  }
}

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
  if (token && strlen(token) > 0 && !url_is_presigned(url)) {
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

// libcurl write callback that streams straight to a FILE*
static size_t file_write_cb(void *ptr, size_t sz, size_t nmemb, void *user) {
  return fwrite(ptr, sz, nmemb, (FILE *)user);
}

// Download a binary file from a URL and atomically install it at
// local_path.  Streams to a temp file first; only on a complete 200
// response are the old database AND its stale -wal/-shm sidecars
// replaced.  A 404 maps to HYDRATION_ERR_NOTFOUND (cold start); any
// other failure leaves the existing local database untouched.
static int http_download_file(const char *url, const char *token,
                               const char *local_path, int *err_out) {
  HttpReq r;
  if (http_init(&r, url, token) != 0) { *err_out = HYDRATION_ERR_NET; return -1; }

  char tmp_path[4096];
  int n = snprintf(tmp_path, sizeof(tmp_path), "%s.arkdl", local_path);
  if (n <= 0 || (size_t)n >= sizeof(tmp_path)) {
    http_free(&r); *err_out = HYDRATION_ERR_DISK; return -1;
  }

  FILE *f = fopen(tmp_path, "wb");
  if (!f) { http_free(&r); *err_out = HYDRATION_ERR_DISK; return -1; }

  curl_easy_setopt(r.handle, CURLOPT_WRITEFUNCTION, file_write_cb);
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, f);

  CURLcode rc = curl_easy_perform(r.handle);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(r.handle, CURLINFO_RESPONSE_CODE, &http_code);
  http_free(&r);

  int io_failed = ferror(f);
  if (fclose(f) != 0) io_failed = 1;

  if (rc != CURLE_OK || http_code != 200 || io_failed) {
    remove(tmp_path);
    if (io_failed && rc == CURLE_OK && http_code == 200) *err_out = HYDRATION_ERR_DISK;
    else if (http_code == 404) *err_out = HYDRATION_ERR_NOTFOUND;
    else if (http_code == 401 || http_code == 403) *err_out = HYDRATION_ERR_PROTO;
    else *err_out = HYDRATION_ERR_NET;
    return -1;
  }

  // Success: drop the previous database and its stale WAL/SHM frames
  // (replaying them into the new snapshot would corrupt it), then
  // atomically move the snapshot into place.
  hydration_remove_db_files(local_path);
  if (rename(tmp_path, local_path) != 0) {
    remove(tmp_path);
    *err_out = HYDRATION_ERR_DISK;
    return -1;
  }

  *err_out = 0;
  return 0;
}

// ── Minimal JSON helpers (no external library) ──────────────────────
//
// These are deliberately small but CORRECT for the control-plane
// contract: string-aware scanning (braces/brackets inside string
// values never confuse structure), full escape handling, and exact
// key matching at the top level of the object.

// Advance past a JSON string starting at p (which points at the
// opening quote), honoring backslash escapes.
static const char *json_skip_string(const char *p) {
  p++; // opening quote
  while (*p) {
    if (*p == '\\') { p += 2; continue; }
    if (*p == '"') return p + 1;
    p++;
  }
  return p;
}

// Locate a top-level key in a JSON object; returns a pointer to the
// start of its value (whitespace already skipped), or NULL.
static const char *json_find_key(const char *json, const char *key) {
  const char *p = json;
  while (*p && *p != '{') p++;
  if (!*p) return NULL;
  p++;
  int depth = 1;
  size_t klen = strlen(key);
  while (*p && depth > 0) {
    if (*p == '"') {
      const char *end = json_skip_string(p);
      if (depth == 1 && (size_t)(end - p - 2) == klen &&
          strncmp(p + 1, key, klen) == 0) {
        const char *q = end;
        while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
        if (*q == ':') {
          q++;
          while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
          return q;
        }
      }
      p = end;
      continue;
    }
    if (*p == '{' || *p == '[') depth++;
    else if (*p == '}' || *p == ']') depth--;
    p++;
  }
  return NULL;
}

// Encode a Unicode code point as UTF-8 (BMP only; surrogate halves
// that arrive unpaired become U+FFFD).
static size_t utf8_encode(unsigned cp, char *out) {
  if (cp < 0x80) { out[0] = (char)cp; return 1; }
  if (cp < 0x800) {
    out[0] = (char)(0xC0 | (cp >> 6));
    out[1] = (char)(0x80 | (cp & 0x3F));
    return 2;
  }
  if (cp >= 0xD800 && cp <= 0xDFFF) cp = 0xFFFD;
  out[0] = (char)(0xE0 | (cp >> 12));
  out[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
  out[2] = (char)(0x80 | (cp & 0x3F));
  return 3;
}

static unsigned hex4(const char *p) {
  unsigned v = 0;
  for (int i = 0; i < 4; i++) {
    char c = p[i];
    v <<= 4;
    if (c >= '0' && c <= '9') v |= (unsigned)(c - '0');
    else if (c >= 'a' && c <= 'f') v |= (unsigned)(c - 'a' + 10);
    else if (c >= 'A' && c <= 'F') v |= (unsigned)(c - 'A' + 10);
    else return 0xFFFFFFFF; // invalid
  }
  return v;
}

// Find a string value for a key in a flat JSON object: {"key":"value",...}
// Handles all JSON string escapes.  Returns malloc'd string or NULL.
char *json_get_string(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '"') return NULL;
  p++;

  char *val = malloc(strlen(p) + 1); // output can only be shorter
  if (!val) return NULL;

  size_t out = 0;
  while (*p && *p != '"') {
    if (*p != '\\') { val[out++] = *p++; continue; }
    p++;
    switch (*p) {
      case 'n': val[out++] = '\n'; p++; break;
      case 't': val[out++] = '\t'; p++; break;
      case 'r': val[out++] = '\r'; p++; break;
      case 'b': val[out++] = '\b'; p++; break;
      case 'f': val[out++] = '\f'; p++; break;
      case '/': val[out++] = '/';  p++; break;
      case '\\': val[out++] = '\\'; p++; break;
      case '"': val[out++] = '"';  p++; break;
      case 'u': {
        unsigned cp = hex4(p + 1);
        if (cp == 0xFFFFFFFF) { val[out++] = '?'; p++; break; }
        p += 5; // past \uXXXX
        // Combine surrogate pairs when present
        if (cp >= 0xD800 && cp <= 0xDBFF && p[0] == '\\' && p[1] == 'u') {
          unsigned lo = hex4(p + 2);
          if (lo >= 0xDC00 && lo <= 0xDFFF) {
            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
            p += 6;
            // Encode 4-byte UTF-8
            val[out++] = (char)(0xF0 | (cp >> 18));
            val[out++] = (char)(0x80 | ((cp >> 12) & 0x3F));
            val[out++] = (char)(0x80 | ((cp >> 6) & 0x3F));
            val[out++] = (char)(0x80 | (cp & 0x3F));
            break;
          }
        }
        out += utf8_encode(cp, &val[out]);
        break;
      }
      default: if (*p) { val[out++] = *p++; } break; // unknown escape → literal
    }
  }
  val[out] = '\0';
  return val;
}

int64_t json_get_int64(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p) return 0;
  return (int64_t)strtoll(p, NULL, 10);
}

// Return the end of the array element starting at p: the first comma
// or closing bracket at the element's own nesting level, skipping
// string contents entirely.
static const char *json_array_elem_end(const char *p) {
  int depth = 0;
  while (*p) {
    if (*p == '"') { p = json_skip_string(p); continue; }
    if (*p == '{' || *p == '[') depth++;
    else if (*p == '}' || *p == ']') {
      if (depth == 0) return p;
      depth--;
    } else if (*p == ',' && depth == 0) {
      return p;
    }
    p++;
  }
  return p;
}

// Count elements in a JSON array at the given key: {"key":[{...},{...}]}
int json_array_count(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '[') return 0;
  p++;
  int count = 0;
  for (;;) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (!*p || *p == ']') break;
    count++;
    p = json_array_elem_end(p);
    if (*p == ',') p++;
    else break;
  }
  return count;
}

// Parse the i-th element from a JSON array at key: {"key":[{...},{...}]}
// Returns malloc'd copy of the i-th element, or NULL if out of range.
char *json_array_get(const char *json, const char *key, int index) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '[' || index < 0) return NULL;
  p++;
  for (int i = 0;; i++) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (!*p || *p == ']') return NULL;
    const char *end = json_array_elem_end(p);
    if (i == index) {
      while (end > p && isspace((unsigned char)end[-1])) end--;
      size_t len = (size_t)(end - p);
      char *copy = malloc(len + 1);
      if (!copy) return NULL;
      memcpy(copy, p, len);
      copy[len] = '\0';
      return copy;
    }
    if (*end == ',') { p = end + 1; continue; }
    return NULL;
  }
}

// ── Control Plane: request hydration plan ───────────────────────────

static int request_hydrate_plan(const char *server_url, const char *token,
                                 HydratePlan *plan) {
  size_t url_len = strlen(server_url) + strlen("/hydrate/plan") + 1;
  char *url = malloc(url_len);
  if (!url) return HYDRATION_ERR_MEM;
  snprintf(url, url_len, "%s/hydrate/plan", server_url);

  int err = 0;
  char *json = http_get_string(url, token, &err);
  free(url);
  if (!json) return err;

  // Parse plan
  memset(plan, 0, sizeof(*plan));
  plan->snapshot_url  = json_get_string(json, "snapshot_url");
  plan->baseline_lsn  = json_get_int64(json, "baseline_lsn");
  plan->expires_at    = json_get_int64(json, "expires_at");
  plan->chunk_count   = json_array_count(json, "chunks");

  if (plan->chunk_count > 0) {
    // calloc so a partial parse failure leaves no uninitialized
    // pointers for hydrate_plan_free() to choke on.
    plan->chunks = calloc((size_t)plan->chunk_count, sizeof(HydrateChunk));
    if (!plan->chunks) { free(json); hydrate_plan_free(plan); return HYDRATION_ERR_MEM; }

    for (int i = 0; i < plan->chunk_count; i++) {
      char *elem = json_array_get(json, "chunks", i);
      if (!elem) { free(json); hydrate_plan_free(plan); return HYDRATION_ERR_PROTO; }
      plan->chunks[i].url        = json_get_string(elem, "url");
      plan->chunks[i].lsn_start  = json_get_int64(elem, "lsn_start");
      plan->chunks[i].lsn_end    = json_get_int64(elem, "lsn_end");
      plan->chunks[i].expires_at = json_get_int64(elem, "expires_at");
      free(elem);
      if (!plan->chunks[i].url) { free(json); hydrate_plan_free(plan); return HYDRATION_ERR_PROTO; }
    }
  }

  free(json);

  if (!plan->snapshot_url) { hydrate_plan_free(plan); return HYDRATION_ERR_PROTO; }
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
    // Only a genuine 404 means "no baseline snapshot uploaded yet"
    // (cold start).  Every other failure — network blip, expired URL,
    // disk full — is a real error and MUST NOT be masked as success,
    // otherwise hydration would replay chunks onto an empty database
    // and report a silently incomplete restore as OK.
    if (err != HYDRATION_ERR_NOTFOUND) return err;

    // Cold start: initialize a clean target so chunks replay from
    // LSN 0.  Never destroy an existing local database in the process.
    struct stat st;
    if (stat(db_path, &st) != 0) {
      // No local DB: clear any orphaned sidecars from a previously
      // deleted database, then create a fresh one.
      hydration_remove_db_files(db_path);
    }
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL) == SQLITE_OK) {
      sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);", NULL, NULL, NULL);
      sqlite3_close(db);
      if (progress) progress(1, 1, 1, user);
      return 0;
    }
    return HYDRATION_ERR_DISK;
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
  if (rc != SQLITE_OK) {
    // Best effort: don't leave an open transaction behind.
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return HYDRATION_ERR_SQL;
  }
  return 0;
}

// ── Public API ──────────────────────────────────────────────────────

int arkilian_hydrate(const char *db_path,
                     const char *server_url,
                     const char *auth_token,
                     hydration_progress_cb progress,
                     void *user_data) {
  if (!db_path || !server_url) return HYDRATION_ERR_PROTO;

  // ── Phase 0: Request hydration plan ──
  HydratePlan plan;
  int rc = request_hydrate_plan(server_url, auth_token, &plan);
  if (rc != 0) return rc;

  // A snapshot URL that is already expired can only fail — say so.
  if (plan.expires_at > 0 && (int64_t)time(NULL) > plan.expires_at) {
    hydrate_plan_free(&plan);
    return HYDRATION_ERR_EXPIRED;
  }

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

    // A hole between what we have applied and what this chunk covers
    // means permanently missing data — fail loudly, never skip silently.
    if (ch->lsn_start > local_lsn + 1) {
      fprintf(stderr,
              "arkilian: hydration LSN gap — have up to %lld but chunk "
              "starts at %lld (missing %lld LSN(s))\n",
              (long long)local_lsn, (long long)ch->lsn_start,
              (long long)(ch->lsn_start - local_lsn - 1));
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      return HYDRATION_ERR_PROTO;
    }

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

    local_lsn = ch->lsn_end;
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
