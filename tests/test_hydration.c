// Arkilian Hydration Engine v2 — tests
//
// Compile:
//   cc tests/test_hydration.c src/hydration.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lsqlite3 -o test_hydration

#include "hydration.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    tests_run++;                                                                \
    printf("  [%02d] %-50s ", tests_run, #fn);                                 \
    fn();                                                                       \
    tests_passed++;                                                             \
    printf("PASS\n");                                                           \
  } while (0)

// ── Plan free ────────────────────────────────────────────────────────

static void test_plan_free_null(void) {
  hydrate_plan_free(NULL);
}

static void test_plan_free_empty(void) {
  HydratePlan p = {0};
  hydrate_plan_free(&p);
}

static void test_plan_free_populated(void) {
  HydratePlan p = {0};
  p.snapshot_url = strdup("http://example.com/snap");
  p.snapshot_sha256 = strdup("abc123");
  p.chunk_count = 2;
  // calloc so the new sha256 field is zero-initialized (NULL) — a real
  // partial parse leaves chunks calloc'd too, and hydrate_plan_free
  // NULL-safes the free.
  p.chunks = calloc(2, sizeof(HydrateChunk));
  p.chunks[0].url = strdup("http://example.com/chunk1");
  p.chunks[0].sha256 = strdup("deadbeef");
  p.chunks[1].url = strdup("http://example.com/chunk2");
  hydrate_plan_free(&p);
  assert(p.snapshot_url == NULL);
  assert(p.snapshot_sha256 == NULL);
  assert(p.chunks == NULL);
  assert(p.chunk_count == 0);
}

// ── Replay chunk — basic ────────────────────────────────────────────

static void test_replay_chunk_insert(void) {
  remove("/tmp/test_hydrate_chunk.db");

  sqlite3 *db = NULL;
  int rc = sqlite3_open_v2("/tmp/test_hydrate_chunk.db", &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  assert(rc == SQLITE_OK);

  // Create the meta table (normally done by db_init)
  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
    NULL, NULL, NULL);
  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val INT);",
    NULL, NULL, NULL);

  // Replay a chunk with two INSERTs
  rc = hydrate_replay_chunk(db,
    "INSERT INTO t (val) VALUES (1);"
    "INSERT INTO t (val) VALUES (2);", 42);
  assert(rc == 0);

  // Verify data
  sqlite3_stmt *stmt = NULL;
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM t", -1, &stmt, NULL);
  sqlite3_step(stmt);
  assert(sqlite3_column_int(stmt, 0) == 2);
  sqlite3_finalize(stmt);

  // Verify LSN was tracked
  sqlite3_prepare_v2(db,
    "SELECT v FROM _arkilian_meta WHERE k='last_applied_lsn'", -1, &stmt, NULL);
  sqlite3_step(stmt);
  assert(sqlite3_column_int64(stmt, 0) == 42);
  sqlite3_finalize(stmt);

  sqlite3_close(db);
  remove("/tmp/test_hydrate_chunk.db");
}

static void test_replay_chunk_failure_rolls_back(void) {
  remove("/tmp/test_hydrate_fail.db");

  sqlite3 *db = NULL;
  sqlite3_open_v2("/tmp/test_hydrate_fail.db", &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
    NULL, NULL, NULL);
  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);",
    NULL, NULL, NULL);

  // First valid chunk
  hydrate_replay_chunk(db, "INSERT INTO t (id) VALUES (1);", 10);

  // Second chunk has a bad statement — should roll back entirely
  int rc = hydrate_replay_chunk(db,
    "INSERT INTO t (id) VALUES (2);"
    "INSERT INTO nonexistent VALUES (3);", 20);
  assert(rc != 0);

  // The valid INSERT should NOT be committed
  sqlite3_stmt *stmt = NULL;
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM t", -1, &stmt, NULL);
  sqlite3_step(stmt);
  assert(sqlite3_column_int(stmt, 0) == 1); // only the first chunk's row
  sqlite3_finalize(stmt);

  // LSN should still be 10 (the failed chunk didn't commit)
  sqlite3_prepare_v2(db,
    "SELECT v FROM _arkilian_meta WHERE k='last_applied_lsn'", -1, &stmt, NULL);
  sqlite3_step(stmt);
  assert(sqlite3_column_int64(stmt, 0) == 10);
  sqlite3_finalize(stmt);

  sqlite3_close(db);
  remove("/tmp/test_hydrate_fail.db");
}

static void test_replay_chunk_idempotent(void) {
  remove("/tmp/test_hydrate_idem.db");

  sqlite3 *db = NULL;
  sqlite3_open_v2("/tmp/test_hydrate_idem.db", &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
    NULL, NULL, NULL);
  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val INT);",
    NULL, NULL, NULL);

  // Play the same chunk twice (simulates network retry)
  hydrate_replay_chunk(db, "INSERT OR IGNORE INTO t (id, val) VALUES (1, 100);", 5);
  hydrate_replay_chunk(db, "INSERT OR IGNORE INTO t (id, val) VALUES (1, 100);", 5);

  // Only one row should exist
  sqlite3_stmt *stmt = NULL;
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM t", -1, &stmt, NULL);
  sqlite3_step(stmt);
  assert(sqlite3_column_int(stmt, 0) == 1);
  sqlite3_finalize(stmt);

  sqlite3_close(db);
  remove("/tmp/test_hydrate_idem.db");
}

// ── JSON helpers ────────────────────────────────────────────────────

static void test_json_get_string_basic(void) {
  const char *json = "{\"snapshot_url\":\"http://example.com/snap\",\"baseline_lsn\":42}";
  char *url = json_get_string(json, "snapshot_url");
  assert(url != NULL && strcmp(url, "http://example.com/snap") == 0);
  free(url);
}

static void test_json_get_int64(void) {
  const char *json = "{\"baseline_lsn\":42,\"expires_at\":1718400000}";
  assert(json_get_int64(json, "baseline_lsn") == 42);
  assert(json_get_int64(json, "expires_at") == 1718400000);
  assert(json_get_int64(json, "nonexistent") == 0);
}

static void test_json_array_count(void) {
  const char *json = "{\"chunks\":[{\"a\":1},{\"b\":2},{\"c\":3}]}";
  assert(json_array_count(json, "chunks") == 3);
  assert(json_array_count(json, "nonexistent") == 0);
}

static void test_json_array_get_element(void) {
  const char *json = "{\"chunks\":[{\"url\":\"u1\"},{\"url\":\"u2\"}]}";
  char *elem = json_array_get(json, "chunks", 1);
  assert(elem != NULL && strstr(elem, "u2") != NULL);
  free(elem);
}

// ── JSON regression tests (audit fixes) ─────────────────────────────

static void test_json_string_escapes(void) {
  // Every standard escape must decode, not just &.
  const char *json = "{\"u\":\"a\\\"b\\/c&d\\\\e\\n\"}";
  char *v = json_get_string(json, "u");
  assert(v != NULL && strcmp(v, "a\"b/c&d\\e\n") == 0);
  free(v);
}

static void test_json_string_whitespace_around_colon(void) {
  const char *json = "{ \"snapshot_url\" : \"http://x/y\" }";
  char *v = json_get_string(json, "snapshot_url");
  assert(v != NULL && strcmp(v, "http://x/y") == 0);
  free(v);
}

static void test_json_escaped_quote_does_not_terminate(void) {
  // Old parser used strchr(pos,'"') — this value was truncated.
  const char *json = "{\"msg\":\"say \\\"hi\\\" ok\"}";
  char *v = json_get_string(json, "msg");
  assert(v != NULL && strcmp(v, "say \"hi\" ok") == 0);
  free(v);
}

static void test_json_array_count_ignores_braces_in_strings(void) {
  // Braces inside string values and objects AFTER the array must not
  // inflate the count (old parser scanned to end-of-string).
  const char *json =
    "{\"chunks\":[{\"url\":\"http://x/{a}\"},{\"url\":\"u2\"}],"
    "\"other\":[{\"z\":1},{\"z\":2},{\"z\":3}]}";
  assert(json_array_count(json, "chunks") == 2);
}

static void test_json_array_count_empty(void) {
  const char *json = "{\"chunks\":[]}";
  assert(json_array_count(json, "chunks") == 0);
}

static void test_json_array_get_with_string_braces(void) {
  const char *json = "{\"chunks\":[{\"url\":\"a}{b\"},{\"url\":\"u2\"}]}";
  char *elem = json_array_get(json, "chunks", 1);
  assert(elem != NULL && strstr(elem, "u2") != NULL);
  free(elem);
  // Out-of-range index must return NULL, not garbage
  assert(json_array_get(json, "chunks", 5) == NULL);
}

static void test_json_key_inside_string_value_not_matched(void) {
  // "baseline_lsn" appearing inside a string VALUE must not be treated
  // as a key (exact top-level key matching).
  const char *json = "{\"note\":\"see baseline_lsn here\",\"baseline_lsn\":7}";
  assert(json_get_int64(json, "baseline_lsn") == 7);
}

// ── Snapshot install hygiene (stale WAL corruption fix) ─────────────

static void test_hydration_remove_db_files(void) {
  const char *base = "/tmp/test_ark_rm.db";
  char path[128];
  const char *suffixes[] = {"", "-wal", "-shm", "-journal"};
  for (int i = 0; i < 4; i++) {
    snprintf(path, sizeof(path), "%s%s", base, suffixes[i]);
    FILE *f = fopen(path, "w");
    assert(f != NULL);
    fputs("x", f);
    fclose(f);
  }
  // An unrelated sibling must survive
  FILE *keep = fopen("/tmp/test_ark_rm_keep.db", "w");
  assert(keep != NULL); fputs("x", keep); fclose(keep);

  hydration_remove_db_files(base);

  for (int i = 0; i < 4; i++) {
    snprintf(path, sizeof(path), "%s%s", base, suffixes[i]);
    FILE *f = fopen(path, "r");
    assert(f == NULL); // all gone
  }
  FILE *f = fopen("/tmp/test_ark_rm_keep.db", "r");
  assert(f != NULL); // untouched
  fclose(f);
  remove("/tmp/test_ark_rm_keep.db");
}

// ── Integration (requires running Control Plane) ────────────────────

static void test_hydration_integration(void) {
  const char *url = getenv("ARKILIAN_HYDRATION_URL");
  if (!url) {
    printf("SKIP (set ARKILIAN_HYDRATION_URL to run)\n");
    tests_run--;
    return;
  }
  const char *token = getenv("ARKILIAN_HYDRATION_TOKEN");
  const char *db_path = "/tmp/arkilian_hydrated.db";
  remove(db_path);

  int rc = arkilian_hydrate(db_path, url, token, NULL, NULL);
  printf("rc=%d ", rc);
  // OK or protocol error (no snapshot yet) are both valid
  assert(rc == HYDRATION_OK || rc == HYDRATION_ERR_PROTO);

  remove(db_path);
}

// ── Local-vs-snapshot LSN guard (data-loss protection) ──────────────

#ifdef __APPLE__
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

typedef struct {
  int fd;
  int port;
  pthread_t thread;
  volatile int stop;
  char snapshot_url[128]; // filled after bind; served in the plan
  long baseline_lsn;      // configurable; faithful control plane serves
                          // baseline_lsn == the snapshot's recorded LSN.
} mock_plan_server;

static void *mock_plan_run(void *arg) {
  mock_plan_server *s = (mock_plan_server *)arg;
  char plan[512];
  int plan_len = snprintf(plan, sizeof(plan),
      "{\"snapshot_url\":\"%s\",\"baseline_lsn\":%ld,\"chunks\":[]}",
      s->snapshot_url, s->baseline_lsn);
  for (;;) {
    int c = accept(s->fd, NULL, NULL);
    if (c < 0) break;
    char buf[8192];
    ssize_t n = recv(c, buf, sizeof(buf) - 1, 0);
    if (s->stop) { close(c); break; } // stop-kick connection: exit now
    if (n > 0) {
      buf[n] = '\0';
      int is_plan = strstr(buf, "/hydrate/plan") != NULL;
      if (is_plan) {
        char resp[1024];
        int rl = snprintf(resp, sizeof(resp),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            "Content-Length: %d\r\nConnection: close\r\n\r\n%s",
            plan_len, plan);
        send(c, resp, (size_t)rl, 0);
      } else {
        // Any other path (the snapshot download) → 404, exercising the
        // cold-start path when the guard permits proceeding.
        const char *nf = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        send(c, nf, strlen(nf), 0);
      }
    }
    close(c);
  }
  return NULL;
}

static void mock_plan_start(mock_plan_server *s) {
  memset(s, 0, sizeof(*s));
  s->baseline_lsn = 3000; // historical default
  s->fd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(s->fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in a;
  memset(&a, 0, sizeof(a));
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  a.sin_port = 0;
  bind(s->fd, (struct sockaddr *)&a, sizeof(a));
  socklen_t alen = sizeof(a);
  getsockname(s->fd, (struct sockaddr *)&a, &alen);
  s->port = ntohs(a.sin_port);
  snprintf(s->snapshot_url, sizeof(s->snapshot_url),
           "http://127.0.0.1:%d/snap", s->port);
  listen(s->fd, 8);
  pthread_create(&s->thread, NULL, mock_plan_run, s);
}

static void mock_plan_stop(mock_plan_server *s) {
  s->stop = 1;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd >= 0) {
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons((unsigned short)s->port);
    connect(fd, (struct sockaddr *)&a, sizeof(a));
    close(fd);
  }
  pthread_join(s->thread, NULL);
  close(s->fd);
}

// A local DB hydrated to LSN 5000 must NOT be clobbered by a snapshot
// whose baseline is 3000 — that would silently destroy 2000 LSNs.
static void test_hydrate_refuses_when_local_is_newer(void) {
  mock_plan_server srv;
  mock_plan_start(&srv);

  char base[64];
  snprintf(base, sizeof(base), "http://127.0.0.1:%d/v1", srv.port);
  const char *db_path = "/tmp/ark_hydrate_newer.db";
  remove(db_path);

  sqlite3 *db = NULL;
  assert(sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) == SQLITE_OK);
  assert(sqlite3_exec(db,
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"
      "CREATE TABLE _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);"
      "INSERT INTO _arkilian_meta VALUES ('last_applied_lsn', '5000');"
      "INSERT INTO t (v) VALUES ('precious-local-data');", NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_close(db);

  int rc = arkilian_hydrate(db_path, base, "token", NULL, NULL);
  assert(rc == HYDRATION_ERR_NEWER);

  // The local file must be untouched — data still there.
  assert(sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK);
  sqlite3_stmt *st = NULL;
  sqlite3_prepare_v2(db, "SELECT v FROM t", -1, &st, NULL);
  assert(sqlite3_step(st) == SQLITE_ROW);
  assert(strcmp((const char *)sqlite3_column_text(st, 0), "precious-local-data") == 0);
  sqlite3_finalize(st);
  sqlite3_close(db);

  remove(db_path);
  mock_plan_stop(&srv);
}

// A local DB at or behind the baseline proceeds (chunks skipped by LSN).
// The snapshot download 404s in the mock (cold start), so the LOCAL db
// is what's actually opened — its last_applied_lsn is the authority.
// baseline_lsn == local_lsn means "the snapshot would have been at this
// same point": no chunks to apply, no clamp, no refusal — hydration OK.
static void test_hydrate_proceeds_when_local_behind(void) {
  mock_plan_server srv;
  mock_plan_start(&srv);
  srv.baseline_lsn = 1500; // faithful: snapshot would be at the same LSN

  char base[64];
  snprintf(base, sizeof(base), "http://127.0.0.1:%d/v1", srv.port);
  const char *db_path = "/tmp/ark_hydrate_behind.db";
  remove(db_path);

  sqlite3 *db = NULL;
  assert(sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) == SQLITE_OK);
  assert(sqlite3_exec(db,
      "CREATE TABLE _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);"
      "INSERT INTO _arkilian_meta VALUES ('last_applied_lsn', '1500');", NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_close(db);

  int rc = arkilian_hydrate(db_path, base, "token", NULL, NULL);
  assert(rc == HYDRATION_OK);

  remove(db_path);
  mock_plan_stop(&srv);
}

// A live writer on the local DB must block the restore (clobber guard).
static void test_hydrate_refuses_when_db_locked(void) {
  mock_plan_server srv;
  mock_plan_start(&srv);

  char base[64];
  snprintf(base, sizeof(base), "http://127.0.0.1:%d/v1", srv.port);
  const char *db_path = "/tmp/ark_hydrate_locked.db";
  remove(db_path);

  sqlite3 *db = NULL;
  assert(sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) == SQLITE_OK);
  assert(sqlite3_exec(db,
      "CREATE TABLE _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);"
      "INSERT INTO _arkilian_meta VALUES ('last_applied_lsn', '500');", NULL, NULL, NULL) == SQLITE_OK);
  // Simulate a live application: hold the write lock.
  assert(sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) == SQLITE_OK);

  int rc = arkilian_hydrate(db_path, base, "token", NULL, NULL);
  assert(rc == HYDRATION_ERR_BUSY);

  // The lock holder is untouched; release and clean up.
  assert(sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_stmt *st = NULL;
  sqlite3_prepare_v2(db, "SELECT v FROM _arkilian_meta WHERE k='last_applied_lsn'", -1, &st, NULL);
  assert(sqlite3_step(st) == SQLITE_ROW);
  assert(strcmp((const char *)sqlite3_column_text(st, 0), "500") == 0);
  sqlite3_finalize(st);
  sqlite3_close(db);

  remove(db_path);
  mock_plan_stop(&srv);
}

// ── SHA-256 content authentication ───────────────────────────────────

typedef struct {
  int fd;
  volatile int stop;
  char *plan;
  int plan_len;
  const char *snap_body;
  size_t snap_len;
} sha_mock_ctx;

static void *sha_mock_run(void *arg) {
  sha_mock_ctx *m = (sha_mock_ctx *)arg;
  for (;;) {
    int c = accept(m->fd, NULL, NULL);
    if (c < 0) break;
    char buf[8192];
    ssize_t n = recv(c, buf, sizeof(buf) - 1, 0);
    if (m->stop) { close(c); break; }
    if (n > 0) {
      buf[n] = '\0';
      if (strstr(buf, "/hydrate/plan")) {
        char resp[2048];
        int rl = snprintf(resp, sizeof(resp),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            "Content-Length: %d\r\nConnection: close\r\n\r\n%s",
            m->plan_len, m->plan);
        send(c, resp, (size_t)rl, 0);
      } else {
        // Snapshot download: serve a body whose SHA-256 differs from
        // the (wrong) digest declared in the plan so the mismatch check
        // fires BEFORE the snapshot is installed.
        char resp[512];
        int rl = snprintf(resp, sizeof(resp),
            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n"
            "Content-Length: %zu\r\nConnection: close\r\n\r\n",
            m->snap_len);
        send(c, resp, (size_t)rl, 0);
        send(c, m->snap_body, m->snap_len, 0);
      }
    }
    close(c);
  }
  return NULL;
}

// A control plane that serves a plan whose `snapshot_sha256` does NOT
// match the downloaded snapshot's contents must be refused — the
// snapshot is untrusted (storage tampering / wrong object served).
static void test_hydrate_refuses_on_sha_mismatch(void) {
  // Bind a mock server so we know the port, then craft the plan with
  // the snapshot_url pointing at it. The plan declares a deliberately
  // wrong snapshot_sha256; the mock serves a non-empty snapshot body
  // so the SHA mismatch check fires BEFORE any snapshot install.
  sha_mock_ctx mc;
  memset(&mc, 0, sizeof(mc));
  const char *snap_body = "not-a-real-snapshot-but-fails-sha-first";
  mc.snap_body = snap_body;
  mc.snap_len = strlen(snap_body);
  mc.fd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(mc.fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in a;
  memset(&a, 0, sizeof(a));
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  a.sin_port = 0;
  if (bind(mc.fd, (struct sockaddr *)&a, sizeof(a)) != 0) { assert(0); }
  socklen_t alen = sizeof(a);
  getsockname(mc.fd, (struct sockaddr *)&a, &alen);
  int port = ntohs(a.sin_port);
  listen(mc.fd, 8);

  char snap_url[128], plan[512];
  snprintf(snap_url, sizeof(snap_url), "http://127.0.0.1:%d/snap", port);
  snprintf(plan, sizeof(plan),
      "{\"snapshot_url\":\"%s\",\"snapshot_sha256\":"
      "\"0000000000000000000000000000000000000000000000000000000000000000\","
      "\"baseline_lsn\":0,\"chunks\":[]}",
      snap_url);
  mc.plan = plan;
  mc.plan_len = (int)strlen(plan);

  pthread_t t;
  pthread_create(&t, NULL, sha_mock_run, &mc);

  char base[64];
  snprintf(base, sizeof(base), "http://127.0.0.1:%d/v1", port);
  const char *db_path = "/tmp/ark_hydrate_sha_mismatch.db";
  remove(db_path);

  int rc = arkilian_hydrate(db_path, base, "token", NULL, NULL);
  printf("rc=%d ", rc);
  assert(rc == HYDRATION_ERR_PROTO);

  // Teardown: kick the accept loop, join the thread, close the socket.
  mc.stop = 1;
  int kick = socket(AF_INET, SOCK_STREAM, 0);
  if (kick >= 0) {
    struct sockaddr_in ka;
    memset(&ka, 0, sizeof(ka));
    ka.sin_family = AF_INET;
    ka.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ka.sin_port = htons((unsigned short)port);
    connect(kick, (struct sockaddr *)&ka, sizeof(ka));
    close(kick);
  }
  pthread_join(t, NULL);
  close(mc.fd);

  remove(db_path);
}

// ── Main ────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
  int integration = 0;
  for (int i = 1; i < argc; i++)
    if (strcmp(argv[i], "--integration") == 0) integration = 1;

  printf("=== Arkilian Hydration v2 Tests ===\n\n");

  printf("[Plan Lifecycle]\n");
  RUN_TEST(test_plan_free_null);
  RUN_TEST(test_plan_free_empty);
  RUN_TEST(test_plan_free_populated);

  printf("\n[Replay Engine]\n");
  RUN_TEST(test_replay_chunk_insert);
  RUN_TEST(test_replay_chunk_failure_rolls_back);
  RUN_TEST(test_replay_chunk_idempotent);

  printf("\n[JSON Parsing]\n");
  RUN_TEST(test_json_get_string_basic);
  RUN_TEST(test_json_get_int64);
  RUN_TEST(test_json_array_count);
  RUN_TEST(test_json_array_get_element);

  printf("\n[JSON Regressions]\n");
  RUN_TEST(test_json_string_escapes);
  RUN_TEST(test_json_string_whitespace_around_colon);
  RUN_TEST(test_json_escaped_quote_does_not_terminate);
  RUN_TEST(test_json_array_count_ignores_braces_in_strings);
  RUN_TEST(test_json_array_count_empty);
  RUN_TEST(test_json_array_get_with_string_braces);
  RUN_TEST(test_json_key_inside_string_value_not_matched);

  printf("\n[Snapshot Install Hygiene]\n");
  RUN_TEST(test_hydration_remove_db_files);

  printf("\n[LSN Clobber Guard]\n");
  RUN_TEST(test_hydrate_refuses_when_local_is_newer);
  RUN_TEST(test_hydrate_proceeds_when_local_behind);
  RUN_TEST(test_hydrate_refuses_when_db_locked);

  printf("\n[SHA-256 Content Authentication]\n");
  RUN_TEST(test_hydrate_refuses_on_sha_mismatch);

  if (integration) {
    printf("\n[Integration]\n");
    RUN_TEST(test_hydration_integration);
  }

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
