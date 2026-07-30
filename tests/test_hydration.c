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
  p.chunk_count = 2;
  p.chunks = malloc(2 * sizeof(HydrateChunk));
  p.chunks[0].url = strdup("http://example.com/chunk1");
  p.chunks[1].url = strdup("http://example.com/chunk2");
  hydrate_plan_free(&p);
  assert(p.snapshot_url == NULL);
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

  if (integration) {
    printf("\n[Integration]\n");
    RUN_TEST(test_hydration_integration);
  }

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
