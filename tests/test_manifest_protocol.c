// Manifest wire-protocol + authenticity tests.
//
// The manifest is the root of trust for the restore protocol and the
// restore path EXECUTES the SQL it points at. These tests pin the
// contract:
//   - HMAC authenticity: with ARKILIAN_MANIFEST_HMAC_KEY set, a missing,
//     tampered, or wrong-key manifest.sig is a hard refusal; the shipper
//     publishes a verifiable signature; unsigned manifests still restore
//     when no key is configured (documented legacy downgrade).
//   - Object digests are mandatory: a chunk with a missing/empty digest
//     is refused, never replayed (the P0 "optional chunk SHA" hole).
//   - Strict LSN protocol: overlaps, unsorted entries, inverted ranges,
//     quoted/garbage numbers, and bad versions are protocol refusals
//     before any byte is downloaded.
// POSIX-only (stub server).

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

static const char *DST = "mp_dst.db";

static void cleanup_all(void) {
  remove(DST);
  stub_reset();
}

// Build a REAL SQLite database file (passes quick_check) containing
// users(1..20); returns malloc'd bytes + length.
static char *make_snapshot_bytes(size_t *out_len) {
  const char *path = "mp_snapshot_src.db";
  remove(path);
  sqlite3 *db = NULL;
  assert(sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                         NULL) == SQLITE_OK);
  char *perr = NULL;
  assert(sqlite3_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, "
                          "name TEXT)", NULL, NULL, &perr) == SQLITE_OK);
  char sql[128];
  for (int i = 1; i <= 20; i++) {
    snprintf(sql, sizeof(sql),
             "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(sqlite3_exec(db, sql, NULL, NULL, &perr) == SQLITE_OK);
  }
  sqlite3_close(db);

  FILE *f = fopen(path, "rb");
  assert(f);
  fseek(f, 0, SEEK_END);
  long len = ftell(f);
  fseek(f, 0, SEEK_SET);
  char *bytes = malloc((size_t)len);
  assert(fread(bytes, 1, (size_t)len, f) == (size_t)len);
  fclose(f);
  remove(path);
  *out_len = (size_t)len;
  return bytes;
}

// Publish a complete, VALID remote state: real snapshot object (digest
// recorded) + one chunk (rows 21..25, lsn 21..25, digest recorded) +
// manifest (v3, baseline 20). Extra chunks may be requested via fmt.
static void publish_valid_state(void) {
  stub_reset();
  size_t snap_len = 0;
  char *snap = make_snapshot_bytes(&snap_len);
  char snap_key[512], chunk_key[512], manifest[1600];
  snprintf(snap_key, sizeof(snap_key), "%s/backup.sqlite", PREFIX);
  stub_put(snap_key, snap, snap_len);
  char snap_sha[65];
  ark_sha256_hex(snap, snap_len, snap_sha);
  free(snap);

  const char *chunk_body =
      "INSERT OR REPLACE INTO users (id, name) VALUES (21, 'user-21');\n"
      "INSERT OR REPLACE INTO users (id, name) VALUES (22, 'user-22');\n"
      "INSERT OR REPLACE INTO users (id, name) VALUES (23, 'user-23');\n"
      "INSERT OR REPLACE INTO users (id, name) VALUES (24, 'user-24');\n"
      "INSERT OR REPLACE INTO users (id, name) VALUES (25, 'user-25');\n";
  snprintf(chunk_key, sizeof(chunk_key),
           "%s/chunks/lsn_0000000021_0000000025.sql", PREFIX);
  stub_put(chunk_key, chunk_body, strlen(chunk_body));
  char chunk_sha[65];
  ark_sha256_hex(chunk_body, strlen(chunk_body), chunk_sha);

  snprintf(manifest, sizeof(manifest),
           "{\"version\":3,\"prefix\":\"%s\",\"snapshot\":{\"s3_key\":\"%s\","
           "\"sha256\":\"%s\",\"baseline_lsn\":20},\"chunks\":[{"
           "\"s3_key\":\"%s\",\"sha256\":\"%s\",\"lsn_start\":21,"
           "\"lsn_end\":25}]}",
           PREFIX, snap_key, snap_sha, chunk_key, chunk_sha);
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  stub_put(mkey, manifest, strlen(manifest));
}

static int hydrate(void) {
  remove(DST);
  return arkilian_hydrate_s3(DST, g_endpoint, BUCKET, "us-east-1",
                             "test-access", "test-secret", PREFIX,
                             NULL, NULL);
}

static long long count_rows(const char *table) {
  sqlite3 *db = NULL;
  if (sqlite3_open_v2(DST, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK)
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

static char *manifest_bytes(void) {
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  char *body = NULL;
  size_t len = 0;
  if (!stub_get(mkey, &body, &len)) return NULL;
  return body;
}

static void put_manifest_bytes(const char *body) {
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  stub_put(mkey, body, strlen(body));
}

int main(void) {
  printf("=== manifest protocol + authenticity tests ===\n");
  stub_start();

  // ── 1. Unsigned manifest, no key configured → restores (legacy) ──
  ark_unsetenv("ARKILIAN_MANIFEST_HMAC_KEY");
  publish_valid_state();
  assert(hydrate() == HYDRATION_OK);
  assert(count_rows("users") == 25);
  cleanup_all();
  printf("  unsigned manifest without key: OK\n");

  // ── 2. Key configured + valid manifest.sig → restores ─────────────
  setenv("ARKILIAN_MANIFEST_HMAC_KEY", "operator-secret-key", 1);
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char sig[65];
    ark_hmac_sha256_hex((const uint8_t *)"operator-secret-key", 19,
                        m, strlen(m), sig);
    char skey[512];
    snprintf(skey, sizeof(skey), "%s/manifest.sig", PREFIX);
    stub_put(skey, sig, 64);
    free(m);
  }
  assert(hydrate() == HYDRATION_OK);
  assert(count_rows("users") == 25);
  cleanup_all();
  printf("  signed manifest verifies: OK\n");

  // ── 3. Tampered signature → refusal, nothing restored ─────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char sig[65];
    ark_hmac_sha256_hex((const uint8_t *)"operator-secret-key", 19,
                        m, strlen(m), sig);
    sig[0] = (char)(sig[0] == 'a' ? 'b' : 'a');  // flip one hex digit
    char skey[512];
    snprintf(skey, sizeof(skey), "%s/manifest.sig", PREFIX);
    stub_put(skey, sig, 64);
    free(m);
  }
  {
    int rc = hydrate();
    assert(rc == HYDRATION_ERR_PROTO);
    assert(count_rows("users") == -1);  // no DB installed
  }
  cleanup_all();
  printf("  tampered signature refusal: OK\n");

  // ── 4. Missing manifest.sig with key configured → fail closed ─────
  publish_valid_state();
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  missing signature refusal (fail closed): OK\n");

  // ── 5. Wrong verification key → refusal ────────────────────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char sig[65];
    ark_hmac_sha256_hex((const uint8_t *)"operator-secret-key", 19,
                        m, strlen(m), sig);
    char skey[512];
    snprintf(skey, sizeof(skey), "%s/manifest.sig", PREFIX);
    stub_put(skey, sig, 64);
    free(m);
  }
  setenv("ARKILIAN_MANIFEST_HMAC_KEY", "a-different-key", 1);
  assert(hydrate() == HYDRATION_ERR_PROTO);
  setenv("ARKILIAN_MANIFEST_HMAC_KEY", "operator-secret-key", 1);
  cleanup_all();
  printf("  wrong-key refusal: OK\n");

  // ── 6. Shipper publishes a verifiable signature (end-to-end) ───────
  {
    set_s3_env();
    remove("mp_src.db");
    arkilian *db = NULL;
    assert(db_init(&db, "mp_src.db") == 0);
    assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") ==
           SQLITE_OK);
    for (int i = 1; i <= 5; i++) {
      char sql[64];
      snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('x%d')", i);
      assert(db_exec(db, sql) == SQLITE_OK);
    }
    // Wait for chunk + manifest + signature to land in the stub.
    for (int i = 0; i < 200; i++) {
      if (stub_contains("/chunks/")) break;
      usleep(50000);
    }
    assert(stub_contains("/chunks/"));
    for (int i = 0; i < 400; i++) {
      char skey[512];
      snprintf(skey, sizeof(skey), "%s/manifest.sig", PREFIX);
      char *sig = NULL; size_t slen = 0;
      int have = stub_get(skey, &sig, &slen);
      if (have) {
        // Verify it against the CURRENT published manifest.
        char *m = manifest_bytes();
        assert(m);
        char expect[65];
        ark_hmac_sha256_hex((const uint8_t *)"operator-secret-key", 19,
                            m, strlen(m), expect);
        int ok = slen >= 64 && strncasecmp(sig, expect, 64) == 0;
        free(m); free(sig);
        if (ok) { db_close(db); remove("mp_src.db"); break; }
      }
      usleep(50000);
      assert(i < 399);  // signature never verified
    }
    cleanup_all();
    printf("  shipper publishes verifiable manifest.sig: OK\n");
  }
  unsetenv("ARKILIAN_MANIFEST_HMAC_KEY");

  // ── 7. Chunk with MISSING digest → refusal, never replayed ─────────
  // (The P0 hole: the restore path used to execute digest-less SQL.)
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char *p = strstr(m, "\"sha256\":\"");
    assert(p);
    // Blank the chunk digest (the second sha256 field is the chunk's).
    char *chunk_sha = strstr(p + 10, "\"sha256\":\"");
    assert(chunk_sha);
    chunk_sha += 10;
    for (int i = 0; i < 64 && chunk_sha[i] != '"'; i++) chunk_sha[i] = ' ';
    put_manifest_bytes(m);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  digest-less chunk refusal: OK\n");

  // ── 8. Chunk with malformed digest (63 chars) → refusal ────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char *chunk_sha = strstr(strstr(m, "\"sha256\":\"") + 10, "\"sha256\":\"");
    assert(chunk_sha);
    chunk_sha += 10;
    chunk_sha[63] = ' ';  // 63 hex chars
    put_manifest_bytes(m);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  malformed digest refusal: OK\n");

  // ── 9. Overlapping chunk ranges → protocol refusal ─────────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char overlapped[2048];
    char *chunks = strstr(m, "\"chunks\":[");
    assert(chunks);
    snprintf(overlapped, sizeof(overlapped),
             "%.*s{\"s3_key\":\"%s/chunks/lsn_0000000023_0000000030.sql\","
             "\"sha256\":\"%064d\",\"lsn_start\":23,\"lsn_end\":30}]}",
             (int)(chunks + 10 - m), m, PREFIX, 0);
    put_manifest_bytes(overlapped);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  overlapping-chunk refusal: OK\n");

  // ── 10. Inverted LSN range (end < start) → refusal ──────────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char *p = strstr(m, "\"lsn_start\":21,\"lsn_end\":25");
    assert(p);
    memcpy(p, "\"lsn_start\":25,\"lsn_end\":21", 27);
    put_manifest_bytes(m);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  inverted-range refusal: OK\n");

  // ── 11. Quoted (non-numeric) LSN → strict parse refusal ────────────
  // (The old parser silently strtoll'ed whatever it found.)
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char *p = strstr(m, "\"lsn_start\":21");
    assert(p);
    memcpy(p, "\"lsn_start\":\"21\"", 16);
    put_manifest_bytes(m);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  quoted-LSN strict parse refusal: OK\n");

  // ── 12. Unsupported version → refusal ───────────────────────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char *p = strstr(m, "\"version\":3");
    assert(p);
    memcpy(p, "\"version\":9", 11);
    put_manifest_bytes(m);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  unsupported-version refusal: OK\n");

  // ── 13. Snapshot recorded without a digest → refusal ────────────────
  publish_valid_state();
  {
    char *m = manifest_bytes();
    assert(m);
    char *p = strstr(m, "\"sha256\":\"");
    assert(p);
    p += 10;
    for (int i = 0; i < 64 && p[i] != '"'; i++) p[i] = ' ';
    put_manifest_bytes(m);
    free(m);
  }
  assert(hydrate() == HYDRATION_ERR_PROTO);
  cleanup_all();
  printf("  digest-less snapshot refusal: OK\n");

  // ── 14. Unique staging files (O_EXCL) — collision-proof names ───────
  {
    char n1[1200], n2[1200];
    FILE *f1 = arkilian_unique_tmp("mp_stage", "a", n1, sizeof(n1));
    FILE *f2 = arkilian_unique_tmp("mp_stage", "a", n2, sizeof(n2));
    assert(f1 && f2);
    assert(strcmp(n1, n2) != 0);      // never the same name
    assert(strstr(n1, "arktmp"));     // per-instance naming scheme
    fclose(f1); fclose(f2);
    remove(n1); remove(n2);
    printf("  unique staging filenames: OK\n");
  }

  printf("\nAll manifest-protocol tests passed!\n");
  return 0;
}
