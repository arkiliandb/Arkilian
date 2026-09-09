// Health state machine tests.
//
// db_backup_is_healthy() used to check five things (enabled, destination,
// two heartbeats, queue < cap) and read GREEN under states where the
// backup contract was silently broken: a raw-DDL trigger hole, rows
// dead-lettered, a frozen/unresolved manifest registry, and a cap-induced
// capture gap. The boolean is now "every core ARK_HF_* flag set" and each
// failure class has its own bit — these tests pin that semantics.
//
// Portable: links the static lib, no sockets.

#include "class.h"
#include "ark_test_env.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void hermetic_env(void) {
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  ark_unsetenv("ARKILIAN_MAX_QUEUE_DEPTH");
  ark_unsetenv("ARKILIAN_MANIFEST_HMAC_KEY");
  ark_unsetenv("ARKILIAN_OUTBOX_DURABLE");
  // Empty, not unset, so load_env(overwrite=0) cannot re-inject .env
  ark_setenv("ARKILIAN_S3_ENDPOINT", "", 1);
  ark_setenv("ARKILIAN_S3_BUCKET", "", 1);
  ark_setenv("ARKILIAN_S3_REGION", "", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", "", 1);
}

static void cleanup(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
  snprintf(side, sizeof(side), "%s.arklock", path); remove(side);
}

static void wait_flush_heartbeat(arkilian *db) {
  for (int i = 0; i < 30; i++) {
    if (db_backup_thread_heartbeat_age_ms(db) >= 0 &&
        db_backup_snapshot_heartbeat_age_ms(db) >= 0) return;
    usleep(100 * 1000);
  }
}

// A capture-only handle (no destination): threads alive, schema in sync,
// no DLQ, manifest resolved (nothing to adopt), durable capture — but no
// destination, so NOT healthy. Each live condition is visible as a flag.
static void test_capture_only_flags(void) {
  cleanup("hf_capture_only.db");
  hermetic_env();
  arkilian *db = NULL;
  assert(db_init(&db, "hf_capture_only.db") == 0);
  wait_flush_heartbeat(db);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)") == SQLITE_OK);

  unsigned f = db_backup_health_flags(db);
  assert(f & ARK_HF_BACKUP_ENABLED);
  assert(!(f & ARK_HF_DEST_CONFIGURED));
  assert(f & ARK_HF_FLUSH_ALIVE);
  assert(f & ARK_HF_SNAPSHOT_ALIVE);
  assert(f & ARK_HF_QUEUE_BELOW_CAP);
  assert(f & ARK_HF_SCHEMA_IN_SYNC);
  assert(f & ARK_HF_NO_DEAD_LETTER);
  assert(f & ARK_HF_MANIFEST_RESOLVED);
  assert(f & ARK_HF_NO_CAPTURE_GAP);
  assert(f & ARK_HF_DURABLE_CAPTURE);
  assert(db_backup_is_healthy(db) == 0);  // no destination → never green

  db_close(db);
  cleanup("hf_capture_only.db");
  printf("  capture-only flags: OK\n");
}

// Dead-lettered rows are CDC rows that never shipped → NOT healthy.
static void test_dead_letter_flag(void) {
  cleanup("hf_dlq.db");
  hermetic_env();
  arkilian *db = NULL;
  assert(db_init(&db, "hf_dlq.db") == 0);
  wait_flush_heartbeat(db);
  assert(db_exec(db, "INSERT INTO _dead_backup (payload, attempts, "
                     "failed_reason) VALUES ('x', 3, 'test')") == SQLITE_OK);
  assert(db_backup_dead_letter_count(db) == 1);
  unsigned f = db_backup_health_flags(db);
  assert(!(f & ARK_HF_NO_DEAD_LETTER));
  assert(db_backup_is_healthy(db) == 0);
  db_close(db);
  cleanup("hf_dlq.db");
  printf("  dead-letter flag: OK\n");
}

// A raw-handle DDL desyncs capture triggers → SCHEMA_IN_SYNC clears and
// health goes red until a resync (previously: green).
static void test_triggers_dirty_flag(void) {
  cleanup("hf_dirty.db");
  hermetic_env();
  arkilian *db = NULL;
  assert(db_init(&db, "hf_dirty.db") == 0);
  wait_flush_heartbeat(db);

  assert(db_backup_is_healthy(db) == 0);  // still no dest — baseline red
  unsigned before = db_backup_health_flags(db);
  assert(before & ARK_HF_SCHEMA_IN_SYNC);

  // Raw-handle DDL bypassing the wrapper → authorizer raises triggers_dirty.
  sqlite3 *raw = db_get_handle(db);
  char *perr = NULL;
  assert(sqlite3_exec(raw, "CREATE TABLE raw_ddl (id INTEGER PRIMARY KEY)",
                      NULL, NULL, &perr) == SQLITE_OK);
  sqlite3_free(perr);
  assert(db_backup_triggers_dirty(db) == 1);
  unsigned dirty = db_backup_health_flags(db);
  assert(!(dirty & ARK_HF_SCHEMA_IN_SYNC));
  assert(db_backup_is_healthy(db) == 0);

  // Repair restores the flag.
  assert(db_resync_triggers(db) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 0);
  unsigned repaired = db_backup_health_flags(db);
  assert(repaired & ARK_HF_SCHEMA_IN_SYNC);

  db_close(db);
  cleanup("hf_dirty.db");
  printf("  triggers-dirty flag: OK\n");
}

// DURABLE_CAPTURE reflects ARKILIAN_OUTBOX_DURABLE (informational only).
static void test_durability_mode_flag(void) {
  cleanup("hf_nodurable.db");
  hermetic_env();
  ark_setenv("ARKILIAN_OUTBOX_DURABLE", "0", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "hf_nodurable.db") == 0);
  unsigned f = db_backup_health_flags(db);
  assert(!(f & ARK_HF_DURABLE_CAPTURE));
  db_close(db);
  cleanup("hf_nodurable.db");
  hermetic_env();
  printf("  durability-mode flag: OK\n");
}

// Kill-switch: ENABLED clears and health goes red (unchanged semantics).
static void test_kill_switch_flag(void) {
  cleanup("hf_kill.db");
  hermetic_env();
  arkilian *db = NULL;
  assert(db_init(&db, "hf_kill.db") == 0);
  wait_flush_heartbeat(db);
  db_backup_set_enabled(db, 0);
  unsigned f = db_backup_health_flags(db);
  assert(!(f & ARK_HF_BACKUP_ENABLED));
  assert(db_backup_is_healthy(db) == 0);
  db_backup_set_enabled(db, 1);
  assert(db_backup_health_flags(db) & ARK_HF_BACKUP_ENABLED);
  db_close(db);
  cleanup("hf_kill.db");
  printf("  kill-switch flag: OK\n");
}

// A configured-but-unreachable destination: heartbeats beat and the queue
// is empty, but the startup manifest read never resolves and nothing can
// ever publish — health must be RED (this state previously read GREEN,
// on heartbeats alone).
static void test_dead_destination_not_healthy(void) {
  cleanup("hf_deaddest.db");
  hermetic_env();
  ark_setenv("ARKILIAN_S3_ENDPOINT", "http://127.0.0.1:1", 1);
  ark_setenv("ARKILIAN_S3_BUCKET", "test-bucket", 1);
  ark_setenv("ARKILIAN_S3_REGION", "us-east-1", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", "test-prefix", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "hf_deaddest.db") == 0);
  wait_flush_heartbeat(db);

  unsigned f = db_backup_health_flags(db);
  assert(f & ARK_HF_DEST_CONFIGURED);       // configured...
  assert(f & ARK_HF_FLUSH_ALIVE);           // ...and alive...
  assert(f & ARK_HF_QUEUE_BELOW_CAP);       // ...and idle...
  assert(!(f & ARK_HF_MANIFEST_RESOLVED));  // ...but publish-never: RED
  assert(db_backup_is_healthy(db) == 0);

  db_close(db);
  cleanup("hf_deaddest.db");
  printf("  dead-destination (unresolved manifest) red: OK\n");
}

int main(void) {
  printf("=== health state machine tests ===\n");
  test_capture_only_flags();
  test_dead_letter_flag();
  test_triggers_dirty_flag();
  test_durability_mode_flag();
  test_kill_switch_flag();
  test_dead_destination_not_healthy();
  printf("\nAll health-flag tests passed!\n");
  return 0;
}
