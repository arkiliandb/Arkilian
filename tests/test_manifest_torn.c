// tests/test_manifest_torn.c — a publish torn between manifest and signature.
//
// Field report (1.6.1, Fly.io): after days of normal operation the machine
// restarted and every start since logged
//   manifest HMAC MISMATCH ... remote manifest for prefix ... is unreadable
//   — shipping and publishing frozen
// The manifest used to be published as two objects, manifest.json then
// manifest.sig. A process killed between the two PUTs (the platform's
// SIGKILL during the shutdown drain, an OOM kill) left the new manifest
// next to the previous signature. Nothing could repair it: the only
// process able to republish refused to start shipping.
//
// Invariants:
//   1. A manifest whose separate signature is stale still verifies (the
//      signature is embedded in manifest.json) — for restore and for the
//      writer's own restart, which resumes shipping.
//   2. manifest.sig keeps verifying the whole object for older readers.
//   3. A freeze on an unreadable (legacy) manifest is re-checked and lifts
//      once the prefix verifies, without a restart.
//   4. A tampered manifest is still refused.

#include "class.h"
#include "hydration.h"
#include "sha256.h"
#include "ark_stub_s3.h"
#include "ark_test_env.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KEY "operator-secret-key"
static const char *SRC = "mt_src.db";
static const char *DST = "mt_dst.db";

static void rm_db(const char *p) {
  char b[256];
  remove(p);
  snprintf(b, sizeof(b), "%s-wal", p); remove(b);
  snprintf(b, sizeof(b), "%s-shm", p); remove(b);
}

static char *get_obj(const char *name) {
  char k[512];
  snprintf(k, sizeof(k), "%s/%s", PREFIX, name);
  char *body = NULL;
  size_t len = 0;
  if (!stub_get(k, &body, &len)) return NULL;
  char *s = malloc(len + 1);
  memcpy(s, body, len);
  s[len] = '\0';
  free(body);
  return s;
}

static void put_obj(const char *name, const char *data) {
  char k[512];
  snprintf(k, sizeof(k), "%s/%s", PREFIX, name);
  stub_put(k, data, strlen(data));
}

// What a 1.6.0/1.6.1 reader checks: manifest.sig == HMAC(whole manifest).
static int legacy_sig_verifies(void) {
  char *m = get_obj("manifest.json"), *sig = get_obj("manifest.sig");
  int ok = 0;
  if (m && sig) {
    char e[65];
    ark_hmac_sha256_hex((const uint8_t *)KEY, strlen(KEY), m, strlen(m), e);
    ok = strlen(sig) >= 64 && strncasecmp(sig, e, 64) == 0;
  }
  free(m); free(sig);
  return ok;
}

static int hydrate(void) {
  rm_db(DST);
  return arkilian_hydrate_s3(DST, g_endpoint, BUCKET, "us-east-1",
                             "test-access", "test-secret", PREFIX, NULL, NULL);
}

static long long count_rows(const char *path) {
  sqlite3 *db = NULL;
  long long n = -1;
  if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW)
      n = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
  }
  if (db) sqlite3_close(db);
  return n;
}

static void insert_rows(arkilian *db, int from, int to) {
  for (int i = from; i <= to; i++) {
    char sql[96];
    snprintf(sql, sizeof(sql), "INSERT INTO t (id, v) VALUES (%d, 'v%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }
}

static int resolved(arkilian *db) {
  return (db_backup_health_flags(db) & ARK_HF_MANIFEST_RESOLVED) != 0;
}

// Wait (up to ~20 s) until the published manifest contains `needle`.
static int wait_manifest_contains(const char *needle) {
  for (int i = 0; i < 400; i++) {
    char *m = get_obj("manifest.json");
    int ok = m && strstr(m, needle);
    free(m);
    if (ok) return 1;
    usleep(50000);
  }
  return 0;
}

int main(void) {
  printf("=== manifest torn between manifest.json and manifest.sig ===\n");
  stub_start();
  set_s3_env();
  setenv("ARKILIAN_MANIFEST_HMAC_KEY", KEY, 1);
  setenv("ARKILIAN_SHUTDOWN_DRAIN_SEC", "1", 1);
  setenv("ARKILIAN_MANIFEST_RECHECK_SEC", "1", 1);
  setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1", 1);
  rm_db(SRC);

  // ── Writer A publishes normally ──────────────────────────────────────
  arkilian *db = NULL;
  assert(db_init(&db, SRC) == 0);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  insert_rows(db, 1, 5);
  assert(wait_manifest_contains("\"hmac\":\""));
  for (int i = 0; i < 400 && !(legacy_sig_verifies() && wait_manifest_contains("lsn_end")); i++)
    usleep(50000);
  assert(legacy_sig_verifies());
  printf("  manifest carries an embedded signature; manifest.sig verifies for old readers\n");

  // ── Torn publish: manifest.json lands, manifest.sig does not ─────────
  char *before = get_obj("manifest.json");
  stub_set_status_override_manifest_sig(500);
  insert_rows(db, 6, 10);
  int changed = 0;
  for (int i = 0; i < 400 && !changed; i++) {
    char *m = get_obj("manifest.json");
    changed = m && strcmp(m, before) != 0;
    free(m);
    if (!changed) usleep(50000);
  }
  assert(changed);
  free(before);
  // The process dies before it can retry the signature.
  stub_set_status_override_put(500);
  db_close(db);
  stub_set_status_override_put(0);
  stub_set_status_override_manifest_sig(0);
  assert(!legacy_sig_verifies());
  printf("  torn state reproduced: manifest.sig does not match manifest.json\n");

  // 1. Restore from the torn prefix.
  assert(hydrate() == HYDRATION_OK);
  long long restored = count_rows(DST);
  printf("  restore from the torn prefix: OK (%lld rows)\n", restored);
  assert(restored == 10);

  // 1. The writer restarts on the same database and resumes shipping.
  assert(db_init(&db, SRC) == 0);
  for (int i = 0; i < 200 && !resolved(db); i++) usleep(50000);
  assert(resolved(db));
  insert_rows(db, 11, 12);
  // 2. Its next publish repairs manifest.sig for old readers too.
  for (int i = 0; i < 400 && !(wait_manifest_contains("\"hmac\":\"") && legacy_sig_verifies()); i++)
    usleep(50000);
  assert(legacy_sig_verifies());
  printf("  writer restart resumes shipping and republishes a valid manifest.sig\n");
  db_close(db);

  // 3. A legacy (pre-1.6.2) torn prefix freezes the writer, and the freeze
  //    lifts by itself once the prefix verifies (no restart).
  {
    char *m = get_obj("manifest.json");
    assert(m);
    char *h = strstr(m, ",\"hmac\":\"");
    assert(h);
    strcpy(h, "}");  // legacy body: no embedded signature
    put_obj("manifest.json", m);
    put_obj("manifest.sig", "0000000000000000000000000000000000000000000000000000000000000000");
    assert(db_init(&db, SRC) == 0);
    sleep(3);
    assert(!resolved(db));
    char sig[65];
    ark_hmac_sha256_hex((const uint8_t *)KEY, strlen(KEY), m, strlen(m), sig);
    put_obj("manifest.sig", sig);
    for (int i = 0; i < 200 && !resolved(db); i++) usleep(50000);
    assert(resolved(db));
    free(m);
    db_close(db);
    printf("  frozen legacy prefix is re-checked and shipping resumes once it verifies\n");
  }

  // 4. Tampering is still refused: change the body under its embedded
  //    signature (and fix manifest.sig so only the embedded check catches it).
  {
    char *m = get_obj("manifest.json");
    assert(m);
    if (!strstr(m, "\"hmac\":\"")) {
      // Re-publish once from the writer so the current manifest is new-format.
      assert(db_init(&db, SRC) == 0);
      insert_rows(db, 13, 13);
      free(m);
      assert(wait_manifest_contains("\"hmac\":\""));
      db_close(db);
      m = get_obj("manifest.json");
    }
    char *p = strstr(m, "\"version\":3");
    assert(p);
    char *t = strstr(m, "\"prefix\":\"");
    assert(t);
    t[10] = (char)(t[10] == 'x' ? 'y' : 'x');  // alter the prefix value
    put_obj("manifest.json", m);
    char sig[65];
    ark_hmac_sha256_hex((const uint8_t *)KEY, strlen(KEY), m, strlen(m), sig);
    put_obj("manifest.sig", sig);  // a forged legacy signature cannot help
    assert(hydrate() == HYDRATION_ERR_PROTO);
    free(m);
    printf("  tampered manifest under an embedded signature: refused\n");
  }

  rm_db(SRC);
  rm_db(DST);
  stub_stop();
  printf("=== PASS ===\n");
  return 0;
}
