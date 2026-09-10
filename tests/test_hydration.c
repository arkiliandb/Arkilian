// Arkilian S3-only hydration test suite.
//
// Spins up an in-process S3 stub server (raw POSIX sockets, ephemeral
// port) that stores objects in memory, drives a REAL client against it
// (db_init → capture → chunk/snapshot/manifest shipping), then restores
// with arkilian_hydrate_s3() and verifies the restored database.
// Also covers the refusal paths: SHA-256 mismatch, LSN gap, absent
// manifest. POSIX-only (raw sockets) — gated out of Windows CI builds.

#include "class.h"
#include "hydration.h"
#include "sha256.h"
#include "deps/sqlite/sqlite3.h"
#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

// ── S3 stub server ──────────────────────────────────────────────────

#define STUB_MAX_OBJECTS 512

typedef struct {
  char   key[512];
  char  *data;
  size_t len;
} stub_object;

static stub_object g_objects[STUB_MAX_OBJECTS];
static int         g_object_count = 0;
static pthread_mutex_t g_store_mutex = PTHREAD_MUTEX_INITIALIZER;
static atomic_int  g_port = 0;
static atomic_int  g_server_up = 0;

static void stub_put(const char *key, const char *data, size_t len) {
  pthread_mutex_lock(&g_store_mutex);
  for (int i = 0; i < g_object_count; i++) {
    if (strcmp(g_objects[i].key, key) == 0) {
      free(g_objects[i].data);
      g_objects[i].data = malloc(len ? len : 1);
      memcpy(g_objects[i].data, data, len);
      g_objects[i].len = len;
      pthread_mutex_unlock(&g_store_mutex);
      return;
    }
  }
  assert(g_object_count < STUB_MAX_OBJECTS);
  stub_object *o = &g_objects[g_object_count++];
  snprintf(o->key, sizeof(o->key), "%s", key);
  o->data = malloc(len ? len : 1);
  memcpy(o->data, data, len);
  o->len = len;
  pthread_mutex_unlock(&g_store_mutex);
}

static int stub_get(const char *key, char **out, size_t *out_len) {
  pthread_mutex_lock(&g_store_mutex);
  for (int i = 0; i < g_object_count; i++) {
    if (strcmp(g_objects[i].key, key) == 0) {
      *out = malloc(g_objects[i].len + 1);
      if (!*out) { pthread_mutex_unlock(&g_store_mutex); return 0; }
      memcpy(*out, g_objects[i].data, g_objects[i].len);
      (*out)[g_objects[i].len] = '\0';
      *out_len = g_objects[i].len;
      pthread_mutex_unlock(&g_store_mutex);
      return 1;
    }
  }
  pthread_mutex_unlock(&g_store_mutex);
  return 0;
}

static int stub_contains(const char *needle) {
  pthread_mutex_lock(&g_store_mutex);
  for (int i = 0; i < g_object_count; i++) {
    if (strstr(g_objects[i].key, needle)) {
      pthread_mutex_unlock(&g_store_mutex);
      return 1;
    }
  }
  pthread_mutex_unlock(&g_store_mutex);
  return 0;
}

// Handle one client connection. Request line: "PUT|GET /bucket/key HTTP/1.1".
static void stub_handle(int fd) {
  char header[8192];
  size_t got = 0;
  while (got < sizeof(header) - 1) {
    char c;
    if (recv(fd, &c, 1, 0) != 1) break;
    header[got++] = c;
    if (got >= 4 && memcmp(header + got - 4, "\r\n\r\n", 4) == 0) break;
  }
  header[got] = '\0';

  char method[8], path[1024];
  if (sscanf(header, "%7s %1023s", method, path) != 2) { close(fd); return; }
  // libcurl sends "Expect: 100-continue" for uploads > 1 KiB (snapshots);
  // answer it immediately or curl stalls for ~1s per upload.
  if (strcasestr(header, "expect: 100-continue")) {
    send(fd, "HTTP/1.1 100 Continue\r\n\r\n", 25, 0);
  }
  // Strip the presigned-URL query string, then "/{bucket}/" — the stub
  // keys objects by key only.
  char *q = strchr(path, '?');
  if (q) *q = '\0';
  const char *key = strchr(path + 1, '/');
  key = key ? key + 1 : path + 1;

  char *cl = strcasestr(header, "content-length:");
  size_t body_len = cl ? (size_t)atoi(cl + 15) : 0;
  char *body = malloc(body_len ? body_len : 1);
  size_t have = 0;
  while (have < body_len) {
    ssize_t n = recv(fd, body + have, body_len - have, 0);
    if (n <= 0) break;
    have += (size_t)n;
  }

  if (strcmp(method, "PUT") == 0) {
    stub_put(key, body, have);
    const char *resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    send(fd, resp, strlen(resp), 0);
  } else if (strcmp(method, "GET") == 0) {
    char *data = NULL;
    size_t len = 0;
    if (stub_get(key, &data, &len)) {
      char head[256];
      snprintf(head, sizeof(head),
               "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\n\r\n", len);
      send(fd, head, strlen(head), 0);
      if (len) send(fd, data, len, 0);
      free(data);
    } else {
      const char *resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
      send(fd, resp, strlen(resp), 0);
    }
  } else {
    const char *resp = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
    send(fd, resp, strlen(resp), 0);
  }
  free(body);
  close(fd);
}

static void *stub_server_thread(void *arg) {
  (void)arg;
  int srv = socket(AF_INET, SOCK_STREAM, 0);
  assert(srv >= 0);
  int one = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  assert(bind(srv, (struct sockaddr *)&addr, sizeof(addr)) == 0);
  socklen_t alen = sizeof(addr);
  assert(getsockname(srv, (struct sockaddr *)&addr, &alen) == 0);
  atomic_store(&g_port, ntohs(addr.sin_port));
  assert(listen(srv, 16) == 0);
  atomic_store(&g_server_up, 1);
  while (atomic_load(&g_server_up)) {
    int fd = accept(srv, NULL, NULL);
    if (fd < 0) break;
    stub_handle(fd);
  }
  close(srv);
  return NULL;
}

// ── Test scaffolding ────────────────────────────────────────────────

static char g_endpoint[64];
static const char *BUCKET = "test-bucket";
static const char *PREFIX = "user-42-appdb";

static void stub_start(void) {
  signal(SIGPIPE, SIG_IGN);
  pthread_t t;
  pthread_create(&t, NULL, stub_server_thread, NULL);
  while (!atomic_load(&g_server_up)) usleep(1000);
  snprintf(g_endpoint, sizeof(g_endpoint), "http://127.0.0.1:%d", atomic_load(&g_port));
}

static void set_s3_env(void) {
  setenv("ARKILIAN_S3_ENDPOINT", g_endpoint, 1);
  setenv("ARKILIAN_S3_BUCKET", "test-bucket", 1);
  setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  setenv("ARKILIAN_S3_PREFIX", "test-prefix", 1);
  setenv("ARKILIAN_S3_BUCKET", BUCKET, 1);
  setenv("ARKILIAN_S3_REGION", "us-east-1", 1);
  setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  setenv("ARKILIAN_S3_PREFIX", PREFIX, 1);
  setenv("ARKILIAN_MANIFEST_HMAC_KEY", "test-hmac-key-for-unit-tests-32b", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "1", 1);
  setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);
}

// Poll until `needle` appears in any stub object key (or timeout).
static int wait_for_key(const char *needle, int timeout_s) {
  for (int i = 0; i < timeout_s * 20; i++) {
    if (stub_contains(needle)) return 1;
    usleep(50000);
  }
  return 0;
}

// Poll until the published manifest's baseline LSN reaches `lsn` — i.e.
// the snapshot thread has re-baselined every row written so far (or a
// chunk covering it was flushed and the manifest refreshed), and the
// corresponding manifest.sig has been published and verifies against it.
static int wait_for_baseline(uint64_t lsn, int timeout_s) {
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  char skey[512];
  snprintf(skey, sizeof(skey), "%s/manifest.sig", PREFIX);
  char needle[64];
  snprintf(needle, sizeof(needle), "\"baseline_lsn\":%llu",
           (unsigned long long)lsn);
  const char *hmac_key = getenv("ARKILIAN_MANIFEST_HMAC_KEY");
  for (int i = 0; i < timeout_s * 20; i++) {
    char *body = NULL;
    size_t len = 0;
    if (stub_get(mkey, &body, &len)) {
      int ok = strstr(body, needle) != NULL;
      if (ok && hmac_key && *hmac_key) {
        char *sig = NULL;
        size_t slen = 0;
        if (stub_get(skey, &sig, &slen)) {
          char expect[65];
          ark_hmac_sha256_hex((const uint8_t *)hmac_key, strlen(hmac_key),
                              body, len, expect);
          ok = (slen >= 64 && strncasecmp(sig, expect, 64) == 0);
          free(sig);
        } else {
          ok = 0;
        }
      }
      free(body);
      if (ok) return 1;
    }
    usleep(50000);
  }
  {
    char *body = NULL;
    size_t len = 0;
    if (stub_get(mkey, &body, &len)) {
      fprintf(stderr, "DIAG manifest at timeout: %.*s\n", (int)len, body);
      free(body);
    } else {
      fprintf(stderr, "DIAG manifest at timeout: ABSENT\n");
    }
  }
  return 0;
}

static long long count_rows(const char *db_path, const char *table) {
  sqlite3 *db = NULL;
  if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK)
    return -1;
  sqlite3_stmt *stmt = NULL;
  char sql[256];
  snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM \"%s\"", table);
  long long n = -1;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK &&
      sqlite3_step(stmt) == SQLITE_ROW)
    n = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return n;
}

static void cleanup_db_path(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
  snprintf(side, sizeof(side), "%s.arklock", path); remove(side);
}

static void cleanup_files(void) {
  cleanup_db_path("hydrate_src.db");
  cleanup_db_path("hydrate_dst.db");
  pthread_mutex_lock(&g_store_mutex);
  for (int i = 0; i < g_object_count; i++) free(g_objects[i].data);
  g_object_count = 0;
  pthread_mutex_unlock(&g_store_mutex);
}

// Full loop: capture rows → chunks + snapshot + manifest land in the
// stub → hydrate into a fresh file → every row is present.
static void test_roundtrip(void) {
  cleanup_files();
  set_s3_env();
  arkilian *db = NULL;
  assert(db_init(&db, "hydrate_src.db") == 0);
  assert(db_exec(db,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)") == SQLITE_OK);
  char sql[256];
  for (int i = 1; i <= 50; i++) {
    snprintf(sql, sizeof(sql),
             "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // Wait for the manifest (published after the first chunk flush and the
  // first snapshot) and at least one chunk object, then for the snapshot
  // thread to re-baseline all 50 rows so the restore is deterministic.
  assert(wait_for_key("manifest.json", 15));
  assert(wait_for_key("/chunks/", 15));
  // All 50 rows covered. The baseline LSN is the highest flushed OUTBOX
  // id: id 1 is the CREATE TABLE DDL capture, so the 50th INSERT ends at
  // outbox id 51.
  assert(wait_for_baseline(51, 15));
  db_close(db);

  // Restore into a fresh file.
  cleanup_db_path("hydrate_dst.db");
  int rc = arkilian_hydrate_s3("hydrate_dst.db", g_endpoint, BUCKET,
                               "us-east-1", "test-access", "test-secret",
                               PREFIX, NULL, NULL);
  if (rc != HYDRATION_OK) {
    fprintf(stderr, "DIAG roundtrip rc=%d\n", rc);
    pthread_mutex_lock(&g_store_mutex);
    for (int i = 0; i < g_object_count; i++)
      fprintf(stderr, "DIAG store[%d]=%s (%zu bytes)\n", i, g_objects[i].key,
              g_objects[i].len);
    pthread_mutex_unlock(&g_store_mutex);
  }
  assert(rc == HYDRATION_OK);
  assert(count_rows("hydrate_dst.db", "users") == 50);
  // Restored database must have sanitized outbox state
  assert(count_rows("hydrate_dst.db", "_pending_backup") == 0);
  assert(count_rows("hydrate_dst.db", "_dead_backup") == 0);
  cleanup_files();
  printf("  roundtrip (capture → ship → restore): OK\n");
}

// A tampered snapshot object (body ≠ recorded digest) must be refused
// before it ever touches the local file.
static void test_sha_mismatch(void) {
  cleanup_files();
  const char *snap_body = "CREATE TABLE t (id INTEGER PRIMARY KEY);\n";
  char snap_key[512], manifest[2048];
  snprintf(snap_key, sizeof(snap_key), "%s/backup.sqlite", PREFIX);
  stub_put(snap_key, snap_body, strlen(snap_body));
  // Digest of some OTHER content — mismatch is guaranteed.
  snprintf(manifest, sizeof(manifest),
           "{\"version\":3,\"prefix\":\"%s\",\"snapshot\":{\"s3_key\":\"%s\","
           "\"sha256\":\"%064d\",\"baseline_lsn\":0},\"chunks\":[]}",
           PREFIX, snap_key, 0);
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  stub_put(mkey, manifest, strlen(manifest));

  cleanup_db_path("hydrate_dst.db");
  int rc = arkilian_hydrate_s3("hydrate_dst.db", g_endpoint, BUCKET,
                               "us-east-1", "test-access", "test-secret",
                               PREFIX, NULL, NULL);
  if (rc != HYDRATION_ERR_PROTO) fprintf(stderr, "DIAG rc=%d\n", rc);
  assert(rc == HYDRATION_ERR_PROTO);
  cleanup_files();
  printf("  sha256 mismatch refusal: OK\n");
}

// A manifest whose chunk starts beyond local LSN + 1 means permanently
// missing data — hydration must refuse loudly instead of skipping.
static void test_lsn_gap(void) {
  cleanup_files();
  // Snapshot must be a valid SQLite file (hydration does PRAGMA quick_check
  // before replay). Create a minimal DB file and use its bytes.
  const char *tmp_snap = "tmp_lsn_gap_snapshot.db";
  remove(tmp_snap);
  {
    sqlite3 *tmp = NULL;
    assert(sqlite3_open(tmp_snap, &tmp) == SQLITE_OK);
    assert(sqlite3_exec(tmp, "CREATE TABLE t (id INTEGER PRIMARY KEY);", NULL, NULL, NULL) == SQLITE_OK);
    sqlite3_close(tmp);
  }
  FILE *sf = fopen(tmp_snap, "rb");
  assert(sf);
  fseek(sf, 0, SEEK_END);
  long snap_len = ftell(sf);
  fseek(sf, 0, SEEK_SET);
  char *snap_body = malloc((size_t)snap_len);
  assert(snap_body && fread(snap_body, 1, (size_t)snap_len, sf) == (size_t)snap_len);
  fclose(sf);
  const char *chunk_body = "REPLACE INTO t (id) VALUES (6);\n";
  char snap_key[512], chunk_key[512], manifest[2048], chunk_sha[65];
  snprintf(snap_key, sizeof(snap_key), "%s/backup.sqlite", PREFIX);
  snprintf(chunk_key, sizeof(chunk_key),
           "%s/chunks/lsn_0000000005_0000000010.sql", PREFIX);
  stub_put(snap_key, snap_body, (size_t)snap_len);
  stub_put(chunk_key, chunk_body, strlen(chunk_body));
  // Real digests: manifest validation requires well-formed digests now,
  // so the refusal below must come from the LSN-gap check itself — not
  // from digest shape (which would mask the regression this test guards).
  ark_sha256_hex(chunk_body, strlen(chunk_body), chunk_sha);
  char snap_sha[65];
  ark_sha256_hex(snap_body, (size_t)snap_len, snap_sha);
  snprintf(manifest, sizeof(manifest),
           "{\"version\":3,\"prefix\":\"%s\",\"snapshot\":{\"s3_key\":\"%s\","
           "\"sha256\":\"%s\",\"baseline_lsn\":0},\"chunks\":[{"
           "\"s3_key\":\"%s\",\"sha256\":\"%s\",\"lsn_start\":5,\"lsn_end\":10}]}",
           PREFIX, snap_key, snap_sha, chunk_key, chunk_sha);
  free(snap_body);
  remove(tmp_snap);
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  stub_put(mkey, manifest, strlen(manifest));

  cleanup_db_path("hydrate_dst.db");
  int rc = arkilian_hydrate_s3("hydrate_dst.db", g_endpoint, BUCKET,
                               "us-east-1", "test-access", "test-secret",
                               PREFIX, NULL, NULL);
  if (rc != HYDRATION_ERR_PROTO) fprintf(stderr, "DIAG rc=%d\n", rc);
  assert(rc == HYDRATION_ERR_PROTO);
  cleanup_files();
  printf("  LSN gap refusal: OK\n");
}

// A cold start (no manifest in storage at all) is refused with
// HYDRATION_ERR_PROTO — "nothing to restore" is an operator-visible
// state, not a silently-created empty database.
static void test_no_manifest(void) {
  cleanup_files();
  cleanup_db_path("hydrate_dst.db");
  int rc = arkilian_hydrate_s3("hydrate_dst.db", g_endpoint, BUCKET,
                               "us-east-1", "test-access", "test-secret",
                               PREFIX, NULL, NULL);
  if (rc != HYDRATION_ERR_PROTO) fprintf(stderr, "DIAG rc=%d\n", rc);
  assert(rc == HYDRATION_ERR_PROTO);
  cleanup_files();
  printf("  absent-manifest refusal: OK\n");
}

int main(void) {
  printf("=== Arkilian S3-only hydration tests ===\n");
  stub_start();
  printf("  stub server on %s\n", g_endpoint);

  test_no_manifest();
  test_sha_mismatch();
  test_lsn_gap();
  test_roundtrip();

  printf("\nAll hydration tests passed!\n");
  return 0;
}
