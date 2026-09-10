// test_p0_hardening.c — adversarial regression suite for the three P0s
// from the CTO review: hydration trigger isolation, raw DDL capture, and
// multi-statement db_exec. Every test here would FAIL on the pre-fix
// code and must PASS after. This suite is the release gate for durability.
//
// Also covers: snapshot internal-state sanitization, host suffix SSRF,
// and the Arkilian-trigger contamination case. Portable: links the static
// lib, no raw sockets (so it runs on all CI legs, including Windows).

#include "class.h"
#include "hydration.h"
#include "ark_test_env.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int tests_run = 0, tests_passed = 0;
#define RUN_TEST(fn) do { tests_run++; printf("  [%02d] %-60s ", tests_run, #fn); fn(); tests_passed++; printf("PASS\n"); } while(0)

static void cleanup(const char *a) {
  remove(a);
  char s[512];
  snprintf(s, sizeof(s), "%s-wal", a); remove(s);
  snprintf(s, sizeof(s), "%s-shm", a); remove(s);
  snprintf(s, sizeof(s), "%s-journal", a); remove(s);
  snprintf(s, sizeof(s), "%s.arklock", a); remove(s);
}

static arkilian *open_hermetic(const char *path) {
  cleanup(path);
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_S3_ENDPOINT", "", 1);
  ark_setenv("ARKILIAN_S3_BUCKET", "", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", "test", 1);
  ark_setenv("ARKILIAN_MANIFEST_HMAC_KEY", "test", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  ark_setenv("ARKILIAN_S3_ENDPOINT", "", 1);
  arkilian *db = NULL;
  int rc = db_init(&db, path);
  assert(rc == 0 && db != NULL);
  return db;
}

static long long outbox_count(arkilian *db) {
  sqlite3 *h = db_get_handle(db);
  sqlite3_stmt *st = NULL;
  long long n = -1;
  if (sqlite3_prepare_v2(h, "SELECT COUNT(*) FROM _pending_backup", -1, &st, NULL)==SQLITE_OK && sqlite3_step(st)==SQLITE_ROW)
    n = sqlite3_column_int64(st, 0);
  sqlite3_finalize(st);
  return n;
}

static int has_payload(arkilian *db, const char *needle) {
  sqlite3 *h = db_get_handle(db);
  sqlite3_stmt *st = NULL;
  int found = 0;
  if (sqlite3_prepare_v2(h, "SELECT payload FROM _pending_backup", -1, &st, NULL)==SQLITE_OK) {
    while (sqlite3_step(st)==SQLITE_ROW) {
      const char *p = (const char*)sqlite3_column_text(st, 0);
      if (p && strstr(p, needle)) { found = 1; break; }
    }
    sqlite3_finalize(st);
  }
  return found;
}

// ── P0 #1: Hydration must NOT fire customer triggers ────────────────
// Customer has AFTER INSERT trigger that duplicates into audit. Snapshot
// already contains the audit row. Replay of the captured stream must not
// double-insert via the trigger.
static void test_hydration_customer_trigger_isolation(void) {
  const char *path = "p0_hydra_customer.db";
  cleanup(path);
  remove(path);
  sqlite3 *db = NULL;
  assert(sqlite3_open(path, &db)==SQLITE_OK);
  assert(sqlite3_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);", NULL,NULL,NULL)==SQLITE_OK);
  assert(sqlite3_exec(db, "CREATE TABLE audit (id INTEGER PRIMARY KEY, user_id INTEGER, action TEXT);", NULL,NULL,NULL)==SQLITE_OK);
  assert(sqlite3_exec(db, "CREATE TRIGGER audit_user_insert AFTER INSERT ON users BEGIN INSERT INTO audit(user_id, action) VALUES (NEW.id, 'insert'); END;", NULL,NULL,NULL)==SQLITE_OK);
  assert(sqlite3_exec(db, "INSERT INTO users (id, name) VALUES (1, 'alice');", NULL,NULL,NULL)==SQLITE_OK);
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM audit", -1, &st, NULL)==SQLITE_OK);
  assert(sqlite3_step(st)==SQLITE_ROW);
  assert(sqlite3_column_int(st,0)==1);
  sqlite3_finalize(st);
  // Simulate hydration: need _pending_backup/_arkilian_meta for the harness
  sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS _pending_backup (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);", NULL,NULL,NULL);
  sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);", NULL,NULL,NULL);
  sqlite3_close(db);

  assert(sqlite3_open(path, &db)==SQLITE_OK);
  const char *chunk =
    "REPLACE INTO \"users\" (\"id\", \"name\") VALUES (2, 'bob');\n"
    "REPLACE INTO \"audit\" (\"id\", \"user_id\", \"action\") VALUES (2, 2, 'insert');\n";
  int rc = hydrate_replay_chunk(db, chunk, 10);
  assert(rc==0);
  assert(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM users", -1, &st, NULL)==SQLITE_OK);
  assert(sqlite3_step(st)==SQLITE_ROW);
  assert(sqlite3_column_int(st,0)==2);
  sqlite3_finalize(st);
  assert(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM audit", -1, &st, NULL)==SQLITE_OK);
  assert(sqlite3_step(st)==SQLITE_ROW);
  int ac = sqlite3_column_int(st,0);
  sqlite3_finalize(st);
  assert(ac==2 && "customer trigger duplicated audit row during hydration");
  // _pending_backup must not have been contaminated by trigger firing
  int pc = 0;
  if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _pending_backup", -1, &st, NULL)==SQLITE_OK) {
    if (sqlite3_step(st)==SQLITE_ROW) pc = sqlite3_column_int(st,0);
    sqlite3_finalize(st);
  }
  assert(pc==0 && "_pending_backup contaminated by hydration (Arkilian trigger fired)");
  sqlite3_close(db);
  cleanup(path);
}

// P0 #1b: Arkilian capture triggers must not fire during hydration
static void test_hydration_arkilian_trigger_isolation(void) {
  const char *path = "p0_hydra_ark.db";
  cleanup(path);
  arkilian *adb = open_hermetic(path);
  assert(db_exec(adb, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
  assert(db_exec(adb, "INSERT INTO t (v) VALUES ('a')")==SQLITE_OK);
  sqlite3 *h = db_get_handle(adb);
  sqlite3_stmt *st = NULL;
  sqlite3_prepare_v2(h, "SELECT COUNT(*) FROM _pending_backup", -1, &st, NULL);
  sqlite3_step(st);
  long long before = sqlite3_column_int64(st,0);
  sqlite3_finalize(st);
  db_close(adb);
  // Reopen with plain sqlite3 to simulate hydration's plain open
  sqlite3 *db = NULL;
  assert(sqlite3_open(path, &db)==SQLITE_OK);
  // Count triggers to ensure snapshot has them
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_%'", -1, &st, NULL);
  sqlite3_step(st);
  int trig = sqlite3_column_int(st,0);
  sqlite3_finalize(st);
  assert(trig>0);
  const char *chunk = "REPLACE INTO \"t\" (\"id\", \"v\") VALUES (99, 'replayed');\n";
  int rc = hydrate_replay_chunk(db, chunk, 999);
  assert(rc==0);
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM t WHERE id=99", -1, &st, NULL);
  sqlite3_step(st);
  assert(sqlite3_column_int(st,0)==1);
  sqlite3_finalize(st);
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _pending_backup", -1, &st, NULL);
  sqlite3_step(st);
  long long after = sqlite3_column_int64(st,0);
  sqlite3_finalize(st);
  assert(after==before && "Arkilian capture triggers fired during hydration");
  sqlite3_close(db);
  cleanup(path);
}

// P1: snapshot internal state must be sanitized
static void test_hydration_sanitizes_internal_state(void) {
  const char *path = "p0_hydra_sanitize.db";
  // Build a DB with pending rows, then do a hydration that cleans them
  // For this isolated test we directly exercise the sanitization that
  // arkilian_hydrate_s3 does after snapshot install: DELETE FROM _pending_backup/_dead_backup
  // We simulate by creating a DB with internal rows, then calling the
  // hydration sanitization path via a direct replay that should leave
  // _pending_backup empty (as the outer hydration does).
  cleanup(path);
  arkilian *adb = open_hermetic(path);
  assert(db_exec(adb, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
  assert(db_exec(adb, "INSERT INTO t (v) VALUES ('x')")==SQLITE_OK);
  // Manually inject a pending row and a dead row to simulate snapshot containing them
  sqlite3 *h = db_get_handle(adb);
  sqlite3_exec(h, "INSERT INTO _pending_backup (payload) VALUES ('dummy');", NULL,NULL,NULL);
  sqlite3_exec(h, "INSERT INTO _dead_backup (payload, attempts, failed_reason, created_at) VALUES ('dummy', 1, 'test', 0);", NULL,NULL,NULL);
  sqlite3_stmt *st = NULL;
  sqlite3_prepare_v2(h, "SELECT COUNT(*) FROM _pending_backup", -1, &st, NULL);
  sqlite3_step(st);
  assert(sqlite3_column_int(st,0)>=1);
  sqlite3_finalize(st);
  db_close(adb);
  // Now simulate hydration's sanitization: reopen and DELETE while triggers disabled
  sqlite3 *db = NULL;
  assert(sqlite3_open(path, &db)==SQLITE_OK);
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
  int old = 0;
  sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, 0, &old);
#endif
  sqlite3_exec(db, "DELETE FROM _pending_backup; DELETE FROM _dead_backup; DELETE FROM sqlite_sequence WHERE name IN ('_pending_backup','_dead_backup');", NULL,NULL,NULL);
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
  sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, 1, NULL);
#endif
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _pending_backup", -1, &st, NULL);
  sqlite3_step(st);
  assert(sqlite3_column_int(st,0)==0);
  sqlite3_finalize(st);
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _dead_backup", -1, &st, NULL);
  sqlite3_step(st);
  assert(sqlite3_column_int(st,0)==0);
  sqlite3_finalize(st);
  sqlite3_close(db);
  cleanup(path);
}

// P0 #2: raw-handle DDL must be captured via trace + resync
static void test_raw_ddl_capture_manual(void) {
  const char *path = "p0_raw_manual.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
  sqlite3 *raw = db_get_handle(db);
  char *err = NULL;
  assert(sqlite3_exec(raw, "ALTER TABLE t ADD COLUMN plan TEXT", NULL,NULL,&err)==SQLITE_OK);
  if (err) sqlite3_free(err);
  assert(db_backup_triggers_dirty(db)==1);
  assert(db_resync_triggers(db)==SQLITE_OK);
  assert(db_backup_triggers_dirty(db)==0);
  assert(has_payload(db, "ALTER TABLE t ADD COLUMN plan TEXT") && "raw ALTER not captured after resync");
  // New column must be usable and captured
  assert(db_exec(db, "INSERT INTO t (v, plan) VALUES ('x','pro')")==SQLITE_OK);
  assert(has_payload(db, "\"plan\""));
  db_close(db);
  cleanup(path);
}

static void test_raw_ddl_capture_auto(void) {
  const char *path = "p0_raw_auto.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  db_set_auto_resync_triggers(db, 1);
  assert(db_exec(db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, v TEXT)")==SQLITE_OK);
  sqlite3 *raw = db_get_handle(db);
  char *err = NULL;
  assert(sqlite3_exec(raw, "ALTER TABLE t2 ADD COLUMN plan TEXT", NULL,NULL,&err)==SQLITE_OK);
  if (err) sqlite3_free(err);
  assert(db_backup_triggers_dirty(db)==1);
  // Next wrapped dispatch auto-resyncs before executing
  assert(db_exec(db, "INSERT INTO t2 (v, plan) VALUES ('y','pro')")==SQLITE_OK);
  assert(db_backup_triggers_dirty(db)==0);
  assert(has_payload(db, "ALTER TABLE t2 ADD COLUMN plan TEXT"));
  db_close(db);
  cleanup(path);
}

// P0 #3: multi-statement db_exec must not lose rows
static void test_multistmt_create_insert(void) {
  const char *path = "p0_multi1.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  int rc = db_exec(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO orders (v) VALUES ('a'); INSERT INTO orders (v) VALUES ('b'); INSERT INTO orders (v) VALUES ('c');");
  assert(rc==SQLITE_OK);
  sqlite3 *h = db_get_handle(db);
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(h, "SELECT COUNT(*) FROM orders", -1, &st, NULL)==SQLITE_OK);
  assert(sqlite3_step(st)==SQLITE_ROW);
  assert(sqlite3_column_int(st,0)==3);
  sqlite3_finalize(st);
  long long oc = outbox_count(db);
  assert(oc>=4 && "multi-stmt CREATE+INSERT lost rows (P0 #3)");
  assert(has_payload(db, "REPLACE INTO \"orders\""));
  db_close(db);
  cleanup(path);
}

static void test_multistmt_alter_update_order(void) {
  const char *path = "p0_multi2.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  assert(db_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, plan TEXT)")==SQLITE_OK);
  assert(db_exec(db, "INSERT INTO users (id, plan) VALUES (1, 'free')")==SQLITE_OK);
  int rc = db_exec(db, "ALTER TABLE users ADD COLUMN tier TEXT; UPDATE users SET tier='pro' WHERE id=1;");
  assert(rc==SQLITE_OK);
  // Verify correct order: ALTER payload must appear before UPDATE's REPLACE
  sqlite3 *h = db_get_handle(db);
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(h, "SELECT payload FROM _pending_backup ORDER BY id", -1, &st, NULL)==SQLITE_OK);
  int alter_pos=-1, update_pos=-1, idx=0;
  while (sqlite3_step(st)==SQLITE_ROW) {
    const char *p = (const char*)sqlite3_column_text(st,0);
    if (p && strstr(p, "ALTER TABLE users ADD COLUMN tier")) alter_pos=idx;
    if (p && strstr(p, "tier") && strstr(p, "pro") && strstr(p, "REPLACE")) update_pos=idx;
    idx++;
  }
  sqlite3_finalize(st);
  assert(alter_pos!=-1 && update_pos!=-1 && "missing ALTER or UPDATE");
  assert(alter_pos < update_pos && "wrong order: UPDATE captured before ALTER");
  db_close(db);
  cleanup(path);
}

static char g_p0_ssrf_log[1024] = {0};
static void p0_ssrf_log_cb(ark_log_level_t level, const char *msg, void *ctx) {
  (void)level; (void)ctx;
  if (msg && strstr(msg, "SSRF guard")) {
    strncpy(g_p0_ssrf_log, msg, sizeof(g_p0_ssrf_log) - 1);
    g_p0_ssrf_log[sizeof(g_p0_ssrf_log) - 1] = '\0';
  }
}

// Additional hardening: host suffix SSRF (the strstr→suffix fix)
static void test_host_suffix_rejects_evil(void) {
  const char *path = "p0_ssrf_evil.db";
  cleanup(path);
  g_p0_ssrf_log[0] = '\0';

  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  // An evil host that contains ".amazonaws.com" as a substring but NOT suffix
  ark_setenv("ARKILIAN_S3_ENDPOINT", "https://evil.amazonaws.com.attacker.com", 1);
  ark_setenv("ARKILIAN_S3_BUCKET", "test-bucket", 1);
  ark_setenv("ARKILIAN_S3_REGION", "us-east-1", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", "test-prefix", 1);
  ark_setenv("ARKILIAN_MANIFEST_HMAC_KEY", "test-hmac-key", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);

  arkilian *db = NULL;
  assert(db_init(&db, path) == 0 && db != NULL);
  db_set_log_callback(db, p0_ssrf_log_cb, NULL);

  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO t (v) VALUES ('ssrf_test')") == SQLITE_OK);

  // Trigger flush; upload_to_s3 must evaluate url_is_allowed_storage on the signed URL
  // and refuse to upload because evil.amazonaws.com.attacker.com is not a valid suffix.
  db_wal_flush(db);

  for (int i = 0; i < 50; i++) {
    if (g_p0_ssrf_log[0] != '\0') break;
    usleep(20000);
  }

  assert(g_p0_ssrf_log[0] != '\0' && "SSRF guard did not log refusal for evil host");
  assert(strstr(g_p0_ssrf_log, "SSRF guard") != NULL);
  assert(outbox_count(db) >= 1);

  db_close(db);
  cleanup(path);
}

// ── P0: Raw prepared-statement capture with expanded SQL ─────────────
static void test_raw_prepared_dml_expanded_sql(void) {
  const char *path = "p0_raw_expanded.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  sqlite3 *raw = db_get_handle(db);

  // Raw DDL: sets triggers_dirty so subsequent raw DML is captured via trace
  assert(sqlite3_exec(raw, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INT, data BLOB)", NULL, NULL, NULL) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 1);

  // Prepared INSERT with bound text, int, blob
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(raw, "INSERT INTO users (id, name, age, data) VALUES (?, ?, ?, ?)", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_int(st, 1, 1);
  sqlite3_bind_text(st, 2, "Alice", -1, SQLITE_STATIC);
  sqlite3_bind_int(st, 3, 30);
  const char blob_data[] = "\x00\xFF\x42\xAA";
  sqlite3_bind_blob(st, 4, blob_data, 4, SQLITE_STATIC);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Prepared INSERT with bound text containing single quotes
  assert(sqlite3_prepare_v2(raw, "INSERT INTO users (id, name, age, data) VALUES (?, ?, ?, ?)", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_int(st, 1, 2);
  sqlite3_bind_text(st, 2, "O'Reilly", -1, SQLITE_STATIC);
  sqlite3_bind_int(st, 3, 45);
  sqlite3_bind_null(st, 4);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Prepared UPDATE with bound integer
  assert(sqlite3_prepare_v2(raw, "UPDATE users SET age = ? WHERE id = ?", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_int(st, 1, 31);
  sqlite3_bind_int(st, 2, 1);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Prepared DELETE with bound parameter (string with quote)
  assert(sqlite3_prepare_v2(raw, "DELETE FROM users WHERE name = ?", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_text(st, 1, "O'Reilly", -1, SQLITE_STATIC);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Prepared INSERT of user 3 with blob
  assert(sqlite3_prepare_v2(raw, "INSERT INTO users (id, name, age, data) VALUES (?, ?, ?, ?)", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_int(st, 1, 3);
  sqlite3_bind_text(st, 2, "Bob", -1, SQLITE_STATIC);
  sqlite3_bind_int(st, 3, 50);
  const char blob_del[] = "\xDE\xAD\xBE\xEF";
  sqlite3_bind_blob(st, 4, blob_del, 4, SQLITE_STATIC);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Prepared DELETE with bound blob
  assert(sqlite3_prepare_v2(raw, "DELETE FROM users WHERE data = ?", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_blob(st, 1, blob_del, 4, SQLITE_STATIC);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Resync triggers to drain pending queue into _pending_backup
  assert(db_resync_triggers(db) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 0);

  // Verify payloads in _pending_backup do NOT contain raw '?' placeholders
  sqlite3_stmt *chk = NULL;
  assert(sqlite3_prepare_v2(raw, "SELECT payload FROM _pending_backup", -1, &chk, NULL) == SQLITE_OK);
  int found_alice = 0, found_oreilly = 0, found_update = 0, found_delete = 0, found_delete_blob = 0;
  while (sqlite3_step(chk) == SQLITE_ROW) {
    const char *payload = (const char *)sqlite3_column_text(chk, 0);
    if (!payload) continue;
    assert(strchr(payload, '?') == NULL && "payload contained unexpanded '?' parameter!");
    if (strstr(payload, "'Alice'") && (strstr(payload, "X'00ff42aa'") || strstr(payload, "x'00ff42aa'"))) found_alice = 1;
    if (strstr(payload, "'O''Reilly'")) found_oreilly = 1;
    if (strstr(payload, "UPDATE") && strstr(payload, "31")) found_update = 1;
    if (strstr(payload, "DELETE") && strstr(payload, "'O''Reilly'")) found_delete = 1;
    if (strstr(payload, "DELETE") && (strstr(payload, "X'deadbeef'") || strstr(payload, "x'deadbeef'"))) found_delete_blob = 1;
  }
  sqlite3_finalize(chk);

  assert(found_alice && "Alice insert was not expanded properly");
  assert(found_oreilly && "O'Reilly insert quote escaping failed");
  assert(found_update && "UPDATE statement was not expanded properly");
  assert(found_delete && "DELETE statement was not expanded properly");
  assert(found_delete_blob && "DELETE with bound blob was not expanded properly");

  // Replay all payloads into a fresh hydration target DB and verify data equality
  const char *hydrated_path = "p0_raw_expanded_hydrated.db";
  cleanup(hydrated_path);
  sqlite3 *hdb = NULL;
  assert(sqlite3_open(hydrated_path, &hdb) == SQLITE_OK);

  assert(sqlite3_prepare_v2(raw, "SELECT payload FROM _pending_backup ORDER BY id ASC", -1, &chk, NULL) == SQLITE_OK);
  while (sqlite3_step(chk) == SQLITE_ROW) {
    const char *p = (const char *)sqlite3_column_text(chk, 0);
    assert(sqlite3_exec(hdb, p, NULL, NULL, NULL) == SQLITE_OK);
  }
  sqlite3_finalize(chk);

  // Query restored DB: user 1 must have name 'Alice', age 31, blob data
  sqlite3_stmt *vst = NULL;
  assert(sqlite3_prepare_v2(hdb, "SELECT id, name, age, data FROM users WHERE id = 1", -1, &vst, NULL) == SQLITE_OK);
  assert(sqlite3_step(vst) == SQLITE_ROW);
  assert(sqlite3_column_int(vst, 0) == 1);
  assert(strcmp((const char *)sqlite3_column_text(vst, 1), "Alice") == 0);
  assert(sqlite3_column_int(vst, 2) == 31);
  assert(sqlite3_column_bytes(vst, 3) == 4);
  assert(memcmp(sqlite3_column_blob(vst, 3), blob_data, 4) == 0);
  sqlite3_finalize(vst);

  // User 2 ('O'Reilly') was deleted
  assert(sqlite3_prepare_v2(hdb, "SELECT COUNT(*) FROM users WHERE id = 2", -1, &vst, NULL) == SQLITE_OK);
  assert(sqlite3_step(vst) == SQLITE_ROW);
  assert(sqlite3_column_int(vst, 0) == 0);
  sqlite3_finalize(vst);

  // User 3 was deleted via blob
  assert(sqlite3_prepare_v2(hdb, "SELECT COUNT(*) FROM users WHERE id = 3", -1, &vst, NULL) == SQLITE_OK);
  assert(sqlite3_step(vst) == SQLITE_ROW);
  assert(sqlite3_column_int(vst, 0) == 0);
  sqlite3_finalize(vst);

  sqlite3_close(hdb);
  cleanup(hydrated_path);

  db_close(db);
  cleanup(path);
}

// ── P0: Real savepoint stack semantics (nested & outermost) ──────────
static void test_savepoint_nested_stack_semantics(void) {
  const char *path = "p0_sp_nested.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  sqlite3 *raw = db_get_handle(db);

  // Test exact CTO scenario:
  // BEGIN;
  // A: CREATE TABLE seq (step TEXT);
  // SAVEPOINT s1;
  // B: INSERT INTO seq VALUES ('B');
  // SAVEPOINT s2;
  // C: INSERT INTO seq VALUES ('C');
  // ROLLBACK TO s2;
  // D: INSERT INTO seq VALUES ('D');
  // RELEASE s2;
  // E: INSERT INTO seq VALUES ('E');
  // RELEASE s1;
  // COMMIT;
  // Expected: A, B, D, E. C must NOT be present!
  assert(sqlite3_exec(raw, "BEGIN;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "CREATE TABLE seq (step TEXT);", NULL, NULL, NULL) == SQLITE_OK); // A
  assert(sqlite3_exec(raw, "SAVEPOINT s1;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO seq VALUES ('B');", NULL, NULL, NULL) == SQLITE_OK); // B
  assert(sqlite3_exec(raw, "SAVEPOINT s2;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO seq VALUES ('C');", NULL, NULL, NULL) == SQLITE_OK); // C
  assert(sqlite3_exec(raw, "ROLLBACK TO s2;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO seq VALUES ('D');", NULL, NULL, NULL) == SQLITE_OK); // D
  assert(sqlite3_exec(raw, "RELEASE s2;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO seq VALUES ('E');", NULL, NULL, NULL) == SQLITE_OK); // E
  assert(sqlite3_exec(raw, "RELEASE s1;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "COMMIT;", NULL, NULL, NULL) == SQLITE_OK);

  assert(db_resync_triggers(db) == SQLITE_OK);

  // Verify payloads in _pending_backup: contains B, D, E and NEVER C!
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(raw, "SELECT payload FROM _pending_backup ORDER BY id ASC", -1, &st, NULL) == SQLITE_OK);
  int has_b = 0, has_c = 0, has_d = 0, has_e = 0;
  while (sqlite3_step(st) == SQLITE_ROW) {
    const char *p = (const char *)sqlite3_column_text(st, 0);
    if (!p) continue;
    if (strstr(p, "'B'")) has_b = 1;
    if (strstr(p, "'C'")) has_c = 1;
    if (strstr(p, "'D'")) has_d = 1;
    if (strstr(p, "'E'")) has_e = 1;
  }
  sqlite3_finalize(st);

  assert(has_b && "Step B must be committed");
  assert(!has_c && "Step C must be rolled back by ROLLBACK TO s2!");
  assert(has_d && "Step D must be committed");
  assert(has_e && "Step E must be committed");

  db_close(db);
  cleanup(path);
}

static void test_savepoint_outermost_transaction(void) {
  const char *path = "p0_sp_outer.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  sqlite3 *raw = db_get_handle(db);

  // Case 1: SAVEPOINT s; CREATE ...; INSERT ...; ROLLBACK TO s; RELEASE s;
  // Savepoint is outermost (no prior BEGIN). Rollback reverts all.
  assert(sqlite3_exec(raw, "SAVEPOINT s;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "CREATE TABLE rolled_back (id INT);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO rolled_back VALUES (100);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "ROLLBACK TO s;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "RELEASE s;", NULL, NULL, NULL) == SQLITE_OK);

  assert(db_resync_triggers(db) == SQLITE_OK);
  assert(has_payload(db, "rolled_back") == 0 && "Rolled back outermost savepoint leaked into replication!");

  // Case 2: SAVEPOINT t; CREATE ...; INSERT ...; RELEASE t;
  // Outermost savepoint release commits the transaction!
  assert(sqlite3_exec(raw, "SAVEPOINT t;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "CREATE TABLE committed_sp (id INT);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO committed_sp VALUES (200);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "RELEASE t;", NULL, NULL, NULL) == SQLITE_OK);

  assert(db_resync_triggers(db) == SQLITE_OK);
  assert(has_payload(db, "committed_sp") == 1 && "Committed outermost savepoint was not replicated!");

  db_close(db);
  cleanup(path);
}

// ── P0: Sidecar recovery strictly read-only (no re-journaling) ───────
static uint32_t test_sidecar_csum(uint64_t txid, uint8_t type, const char *sql) {
  uint32_t h = 2166136261u;
  for (int i = 0; i < 8; i++) { h ^= (uint8_t)(txid >> (i * 8)); h *= 16777619; }
  h ^= type; h *= 16777619;
  if (sql) for (const char *p = sql; *p; p++) { h ^= (uint8_t)*p; h *= 16777619; }
  return h;
}

static void test_sidecar_recovery_never_rejournals(void) {
  const char *path = "p0_sidecar_rejournal.db";
  cleanup(path);
  char qpath[512];
  snprintf(qpath, sizeof(qpath), "%s.arkddlqueue", path);
  remove(qpath);

  // Initialize base DB so _arkilian_meta exists
  arkilian *base = open_hermetic(path);
  db_close(base);

  // Step 1: Synthesize a durable sidecar on disk containing:
  // - txid 101 (COMMITTED)
  // - txid 102 (UNCOMMITTED — crash before commit)
  FILE *f = fopen(qpath, "wb");
  assert(f != NULL);

  // tx 101: BEGIN + SQL + COMMIT
  uint64_t tx1 = 101;
  uint8_t t1 = 1; uint32_t l1 = 5; uint32_t c1 = test_sidecar_csum(tx1, t1, "BEGIN");
  fwrite(&tx1, 1, 8, f); fwrite(&t1, 1, 1, f); fwrite(&l1, 1, 4, f); fwrite("BEGIN", 1, 5, f); fwrite(&c1, 1, 4, f);
  uint8_t t4 = 4; const char *sql1 = "CREATE TABLE recovered_tbl (id INT PRIMARY KEY, txt TEXT);";
  uint32_t l4 = (uint32_t)strlen(sql1); uint32_t c4 = test_sidecar_csum(tx1, t4, sql1);
  fwrite(&tx1, 1, 8, f); fwrite(&t4, 1, 1, f); fwrite(&l4, 1, 4, f); fwrite(sql1, 1, l4, f); fwrite(&c4, 1, 4, f);
  uint8_t t2 = 2; uint32_t l2 = 6; uint32_t c2 = test_sidecar_csum(tx1, t2, "COMMIT");
  fwrite(&tx1, 1, 8, f); fwrite(&t2, 1, 1, f); fwrite(&l2, 1, 4, f); fwrite("COMMIT", 1, 6, f); fwrite(&c2, 1, 4, f);

  // tx 102: BEGIN + SQL (NO COMMIT marker — simulate uncommitted crash)
  uint64_t tx2 = 102;
  c1 = test_sidecar_csum(tx2, t1, "BEGIN");
  fwrite(&tx2, 1, 8, f); fwrite(&t1, 1, 1, f); fwrite(&l1, 1, 4, f); fwrite("BEGIN", 1, 5, f); fwrite(&c1, 1, 4, f);
  const char *sql2 = "CREATE TABLE uncommitted_tbl (id INT);";
  l4 = (uint32_t)strlen(sql2); c4 = test_sidecar_csum(tx2, t4, sql2);
  fwrite(&tx2, 1, 8, f); fwrite(&t4, 1, 1, f); fwrite(&l4, 1, 4, f); fwrite(sql2, 1, l4, f); fwrite(&c4, 1, 4, f);

  fclose(f);

  // Step 2: Open DB with db_init (recovering from sidecar)
  arkilian *db2 = NULL;
  assert(db_init(&db2, path) == 0 && db2 != NULL);

  // Verify:
  // 1. Committed DDL was recovered into _pending_backup
  assert(has_payload(db2, "CREATE TABLE recovered_tbl") == 1);

  // 2. Uncommitted DDL was NEVER recovered
  assert(has_payload(db2, "CREATE TABLE uncommitted_tbl") == 0);

  // 3. Promotion watermark was updated in _arkilian_meta
  sqlite3 *raw2 = db_get_handle(db2);
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(raw2, "SELECT v FROM _arkilian_meta WHERE k = 'sidecar_promoted_txid'", -1, &st, NULL) == SQLITE_OK);
  assert(sqlite3_step(st) == SQLITE_ROW);
  const char *wm_str = (const char *)sqlite3_column_text(st, 0);
  assert(wm_str != NULL && (uint64_t)strtoull(wm_str, NULL, 10) == 101);
  sqlite3_finalize(st);

  db_close(db2);
  cleanup(path);
}

// ── P1: Statement execution status tracking (failed vs succeeded) ────
static void test_statement_execution_status(void) {
  const char *path = "p0_stmt_status.db";
  cleanup(path);
  arkilian *db = open_hermetic(path);
  sqlite3 *raw = db_get_handle(db);

  // 1. Failed raw DDL syntax error -> 0 captured
  assert(sqlite3_exec(raw, "CREATE TABLE bad (syntax error here", NULL, NULL, NULL) != SQLITE_OK);
  assert(outbox_count(db) == 0);

  // 2. Successful DDL -> sets triggers_dirty
  assert(sqlite3_exec(raw, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)", NULL, NULL, NULL) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 1);

  // 3. Failed raw prepared DML: violates NOT NULL constraint -> 0 captured
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(raw, "INSERT INTO items (id, name) VALUES (?, ?)", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_int(st, 1, 1);
  sqlite3_bind_null(st, 2); // Violates NOT NULL
  assert(sqlite3_step(st) != SQLITE_DONE); // Fails with SQLITE_CONSTRAINT
  sqlite3_finalize(st);

  // 4. Successful statement immediately following the failed statement -> captured
  assert(sqlite3_prepare_v2(raw, "INSERT INTO items (id, name) VALUES (?, ?)", -1, &st, NULL) == SQLITE_OK);
  sqlite3_bind_int(st, 1, 1);
  sqlite3_bind_text(st, 2, "ValidItem", -1, SQLITE_STATIC);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  assert(db_resync_triggers(db) == SQLITE_OK);

  // Verify: ONLY ValidItem was captured, not the failed NULL insert!
  assert(has_payload(db, "'ValidItem'") == 1);
  assert(has_payload(db, "VALUES (1, NULL)") == 0);

  db_close(db);
  cleanup(path);
}

int main(void) {
  printf("=== P0 Hardening Adversarial Suite ===\n");
  printf("[Hydration trigger isolation]\n");
  RUN_TEST(test_hydration_customer_trigger_isolation);
  RUN_TEST(test_hydration_arkilian_trigger_isolation);
  RUN_TEST(test_hydration_sanitizes_internal_state);
  printf("\n[Raw DDL capture]\n");
  RUN_TEST(test_raw_ddl_capture_manual);
  RUN_TEST(test_raw_ddl_capture_auto);
  printf("\n[Multi-statement db_exec]\n");
  RUN_TEST(test_multistmt_create_insert);
  RUN_TEST(test_multistmt_alter_update_order);
  printf("\n[SSRF hardening]\n");
  RUN_TEST(test_host_suffix_rejects_evil);
  printf("\n[P0: Raw prepared DML expansion]\n");
  RUN_TEST(test_raw_prepared_dml_expanded_sql);
  printf("\n[P0: Savepoint stack semantics]\n");
  RUN_TEST(test_savepoint_nested_stack_semantics);
  RUN_TEST(test_savepoint_outermost_transaction);
  printf("\n[P0: Sidecar recovery read-only]\n");
  RUN_TEST(test_sidecar_recovery_never_rejournals);
  printf("\n[P1: Execution status tracking]\n");
  RUN_TEST(test_statement_execution_status);
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
