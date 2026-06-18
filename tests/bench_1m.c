// Arkilian vs Raw SQLite — 1M Write Benchmark
//
// Compile:
//   cc tests/bench_1m.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread -O2 -o bench_1m
//
// Run:
//   ./bench_1m

#include "class.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#include <unistd.h>
#endif

#define BENCH_DB       "bench_1m.db"
#define TOTAL_WRITES   1000000
#define WARMUP_WRITES  10000
#define BATCH_SIZE     1000
#define PROGRESS_EVERY (TOTAL_WRITES / 10)

// Operation weights for varied workload
#define INSERT_WEIGHT  60
#define UPDATE_WEIGHT  30
#define DELETE_WEIGHT  10

// Schema
#define CREATE_TABLE_SQL                                                      \
  "CREATE TABLE IF NOT EXISTS bench_data ("                                   \
  "  id       INTEGER PRIMARY KEY, "                                          \
  "  name     TEXT    NOT NULL, "                                             \
  "  value    REAL    NOT NULL, "                                             \
  "  created  INTEGER NOT NULL, "                                             \
  "  updated  INTEGER NOT NULL"                                               \
  ");"

// ---------------------------------------------------------------------------
// High-precision timer
// ---------------------------------------------------------------------------

static double now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static double ns_to_ms(double ns) { return ns / 1e6; }

// ---------------------------------------------------------------------------
// Progress bar
// ---------------------------------------------------------------------------

static void progress(const char *label, int done, int total) {
  int pct = (int)((double)done / (double)total * 100.0);
  fprintf(stderr, "\r  %-8s [", label);
  int bars = pct / 5;
  for (int i = 0; i < 20; i++) fputc(i < bars ? '=' : (i == bars ? '>' : ' '), stderr);
  fprintf(stderr, "] %3d%%  %d/%d", pct, done, total);
  if (done == total) fputc('\n', stderr);
}

// ---------------------------------------------------------------------------
// SQL generation (deterministic pseudo-random for reproducibility)
// ---------------------------------------------------------------------------

static unsigned int g_seed = 42;

static unsigned int xorshift32(void) {
  g_seed ^= g_seed << 13;
  g_seed ^= g_seed >> 17;
  g_seed ^= g_seed << 5;
  return g_seed;
}

static int rand_range(int lo, int hi) {
  return lo + (int)(xorshift32() % (unsigned int)(hi - lo + 1));
}

static int pick_op(void) {
  int r = rand_range(1, 100);
  if (r <= INSERT_WEIGHT)  return 0; // INSERT
  if (r <= INSERT_WEIGHT + UPDATE_WEIGHT) return 1; // UPDATE
  return 2; // DELETE
}

static int g_max_id = 0;

// Generate a varied INSERT/UPDATE/DELETE SQL into buf. Returns 0=INSERT, 1=UPDATE, 2=DELETE.
static int gen_write_sql(char *buf, size_t bufsz) {
  int op = pick_op();

  if (op == 0 || g_max_id == 0) {
    // INSERT
    g_max_id++;
    int  rname = rand_range(0, 999);
    double rval = (double)rand_range(0, 99999) / 100.0;
    long long now = (long long)time(NULL);
    snprintf(buf, bufsz,
      "INSERT INTO bench_data (id, name, value, created, updated) "
      "VALUES (%d, 'user_%04d', %.2f, %lld, %lld)",
      g_max_id, rname, rval, now, now);
    return 0;
  } else if (op == 1) {
    // UPDATE a random existing row
    int target_id = rand_range(1, g_max_id);
    double rval = (double)rand_range(0, 99999) / 100.0;
    long long now = (long long)time(NULL);
    snprintf(buf, bufsz,
      "UPDATE bench_data SET value = %.2f, updated = %lld WHERE id = %d",
      rval, now, target_id);
    return 1;
  } else {
    // DELETE a random existing row (keep at least 1000 rows)
    if (g_max_id > 1000) {
      int target_id = rand_range(1, g_max_id - 500);
      snprintf(buf, bufsz, "DELETE FROM bench_data WHERE id = %d", target_id);
      return 2;
    } else {
      // Fall back to INSERT if not enough rows to safely delete
      g_max_id++;
      int  rname = rand_range(0, 999);
      double rval = (double)rand_range(0, 99999) / 100.0;
      long long now = (long long)time(NULL);
      snprintf(buf, bufsz,
        "INSERT INTO bench_data (id, name, value, created, updated) "
        "VALUES (%d, 'user_%04d', %.2f, %lld, %lld)",
        g_max_id, rname, rval, now, now);
      return 0;
    }
  }
}

// ---------------------------------------------------------------------------
// Benchmark: Raw SQLite (explicit BEGIN/COMMIT per write)
// ---------------------------------------------------------------------------

static int bench_raw_txn_per_write(sqlite3 *handle, int total, double *out_ms) {
  char sql[512];
  int inserts = 0, updates = 0, deletes = 0;

  double t0 = now_ns();
  for (int i = 0; i < total; i++) {
    int op = gen_write_sql(sql, sizeof(sql));
    if (op == 0) inserts++;
    else if (op == 1) updates++;
    else deletes++;

    char *err = NULL;
    sqlite3_exec(handle, "BEGIN;", NULL, NULL, NULL);
    int rc = sqlite3_exec(handle, sql, NULL, NULL, &err);
    if (rc == SQLITE_OK) {
      sqlite3_exec(handle, "COMMIT;", NULL, NULL, NULL);
    } else {
      sqlite3_exec(handle, "ROLLBACK;", NULL, NULL, NULL);
      if (err) sqlite3_free(err);
    }

    if (i > 0 && i % PROGRESS_EVERY == 0) {
      progress("raw-txn", i, total);
    }
  }
  double elapsed = now_ns() - t0;
  *out_ms = ns_to_ms(elapsed);
  progress("raw-txn", total, total);

  return inserts + updates + deletes;
}

// ---------------------------------------------------------------------------
// Benchmark: Arkilian db_exec() (wrapper: mutex + BEGIN/COMMIT + log)
// ---------------------------------------------------------------------------

static int bench_arkilian_exec(arkilian *db, int total, double *out_ms) {
  char sql[512];
  int inserts = 0, updates = 0, deletes = 0;

  double t0 = now_ns();
  for (int i = 0; i < total; i++) {
    int op = gen_write_sql(sql, sizeof(sql));
    if (op == 0) inserts++;
    else if (op == 1) updates++;
    else deletes++;

    db_exec(db, sql);

    if (i > 0 && i % PROGRESS_EVERY == 0) {
      progress("arkilian", i, total);
    }
  }
  double elapsed = now_ns() - t0;
  *out_ms = ns_to_ms(elapsed);
  progress("arkilian", total, total);

  return inserts + updates + deletes;
}

// ---------------------------------------------------------------------------
// Benchmark: Arkilian prepare/bind/step/finalize (per write)
// ---------------------------------------------------------------------------

static int bench_arkilian_prepared(arkilian *db, int total, double *out_ms) {
  int inserts = 0, updates = 0, deletes = 0;

  double t0 = now_ns();
  for (int i = 0; i < total; i++) {
    char sql[512];
    int op = gen_write_sql(sql, sizeof(sql));
    if (op == 0) inserts++;
    else if (op == 1) updates++;
    else deletes++;

    db_prepare(db, sql);
    db_step(db);
    db_finalize(db);

    if (i > 0 && i % PROGRESS_EVERY == 0) {
      progress("ark-prep", i, total);
    }
  }
  double elapsed = now_ns() - t0;
  *out_ms = ns_to_ms(elapsed);
  progress("ark-prep", total, total);

  return inserts + updates + deletes;
}

// ---------------------------------------------------------------------------
// Benchmark: Raw SQLite batched (one transaction per BATCH_SIZE writes)
// ---------------------------------------------------------------------------

static int bench_raw_batched(sqlite3 *handle, int total, double *out_ms) {
  char sql[512];
  int inserts = 0, updates = 0, deletes = 0;

  double t0 = now_ns();
  int batch_count = 0;
  for (int i = 0; i < total; i++) {
    if (batch_count == 0) {
      sqlite3_exec(handle, "BEGIN;", NULL, NULL, NULL);
    }

    int op = gen_write_sql(sql, sizeof(sql));
    if (op == 0) inserts++;
    else if (op == 1) updates++;
    else deletes++;

    char *err = NULL;
    int rc = sqlite3_exec(handle, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK && err) sqlite3_free(err);

    batch_count++;
    if (batch_count >= BATCH_SIZE || i == total - 1) {
      sqlite3_exec(handle, "COMMIT;", NULL, NULL, NULL);
      batch_count = 0;
    }

    if (i > 0 && i % PROGRESS_EVERY == 0) {
      progress("raw-batch", i, total);
    }
  }
  double elapsed = now_ns() - t0;
  *out_ms = ns_to_ms(elapsed);
  progress("raw-batch", total, total);

  return inserts + updates + deletes;
}

// ---------------------------------------------------------------------------
// Row counting helper
// ---------------------------------------------------------------------------

static int count_rows(sqlite3 *handle, const char *table) {
  char sql[256];
  snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", table);
  sqlite3_stmt *stmt = NULL;
  sqlite3_prepare_v2(handle, sql, -1, &stmt, NULL);
  sqlite3_step(stmt);
  int c = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  return c;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(void) {
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  remove(BENCH_DB);

  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║   Arkilian vs Raw SQLite — 1M Write Benchmark               ║\n");
  printf("╚══════════════════════════════════════════════════════════════╝\n\n");

  printf("Configuration:\n");
  printf("  Total writes : %d\n", TOTAL_WRITES);
  printf("  Warmup       : %d writes\n", WARMUP_WRITES);
  printf("  Mix          : %d%% INSERT / %d%% UPDATE / %d%% DELETE\n",
         INSERT_WEIGHT, UPDATE_WEIGHT, DELETE_WEIGHT);
  printf("  Batch size   : %d (for batched bench)\n", BATCH_SIZE);
  printf("  PRAGMAs      : journal_mode=WAL, synchronous=NORMAL,\n");
  printf("                 busy_timeout=5000, foreign_keys=ON\n");
  printf("  Arkilian log : _arkilian_log (ts, sql, params)\n");
  printf("\n");

  // ── Phase 0: Setup ──────────────────────────────────────────────
  printf("── Phase 0: Setup ──────────────────────────────────────────\n");

  arkilian *db = NULL;
  int rc = db_init(&db, BENCH_DB);
  assert(rc == 0 && "db_init failed");
  sqlite3 *raw_handle = db_get_handle(db);

  sqlite3_exec(raw_handle, CREATE_TABLE_SQL, NULL, NULL, NULL);
  printf("  Table created.\n");

  // ── Phase 1: Warmup ─────────────────────────────────────────────
  printf("\n── Phase 1: Warmup (%d writes each runner) ────────────────\n",
         WARMUP_WRITES);

  // Reset seed for deterministic warmup
  g_seed = 42;
  g_max_id = 0;

  double warmup_ms = 0;
  sqlite3_exec(raw_handle, "DELETE FROM bench_data", NULL, NULL, NULL);
  bench_raw_txn_per_write(raw_handle, WARMUP_WRITES, &warmup_ms);
  printf("  Warmup complete: %.0f ms, %d rows in table\n",
         warmup_ms, count_rows(raw_handle, "bench_data"));

  // ── Phase 2: Benchmarks ─────────────────────────────────────────
  printf("\n── Phase 2: 1M Write Benchmarks ─────────────────────────\n\n");

  double t_raw_txn_ms      = 0;
  double t_ark_exec_ms     = 0;
  double t_ark_prep_ms     = 0;
  double t_raw_batch_ms    = 0;

  int rows_raw_txn   = 0;
  int rows_ark_exec  = 0;
  int rows_ark_prep  = 0;
  int rows_raw_batch = 0;

  // Run 1: Raw SQLite (txn per write)
  {
    printf("  [1/4] Raw SQLite  (explicit BEGIN/COMMIT per write)\n");
    sqlite3_exec(raw_handle, "DELETE FROM bench_data", NULL, NULL, NULL);
    g_seed = 42; g_max_id = 0;
    rows_raw_txn = bench_raw_txn_per_write(raw_handle, TOTAL_WRITES, &t_raw_txn_ms);
    int final_rows = count_rows(raw_handle, "bench_data");
    printf("  Result: %d rows, %.0f ms, %.0f writes/sec\n",
           final_rows, t_raw_txn_ms,
           (double)TOTAL_WRITES / (t_raw_txn_ms / 1000.0));
  }

  // Run 2: Arkilian db_exec()
  {
    printf("\n  [2/4] Arkilian     (db_exec: mutex + BEGIN/COMMIT + log)\n");
    sqlite3_exec(raw_handle, "DELETE FROM bench_data", NULL, NULL, NULL);

    // Count log rows before
    int log_before = count_rows(raw_handle, "_arkilian_log");

    g_seed = 42; g_max_id = 0;
    rows_ark_exec = bench_arkilian_exec(db, TOTAL_WRITES, &t_ark_exec_ms);

    int final_rows = count_rows(raw_handle, "bench_data");
    int log_after = count_rows(raw_handle, "_arkilian_log");
    int log_added = log_after - log_before;

    printf("  Result: %d rows, %d log entries, %.0f ms, %.0f writes/sec\n",
           final_rows, log_added, t_ark_exec_ms,
           (double)TOTAL_WRITES / (t_ark_exec_ms / 1000.0));
  }

  // Run 3: Arkilian prepare/bind/step/finalize
  {
    printf("\n  [3/4] Arkilian     (prepare/step/finalize per write)\n");
    sqlite3_exec(raw_handle, "DELETE FROM bench_data", NULL, NULL, NULL);

    int log_before = count_rows(raw_handle, "_arkilian_log");

    g_seed = 42; g_max_id = 0;
    rows_ark_prep = bench_arkilian_prepared(db, TOTAL_WRITES, &t_ark_prep_ms);

    int final_rows = count_rows(raw_handle, "bench_data");
    int log_after = count_rows(raw_handle, "_arkilian_log");
    int log_added = log_after - log_before;

    printf("  Result: %d rows, %d log entries, %.0f ms, %.0f writes/sec\n",
           final_rows, log_added, t_ark_prep_ms,
           (double)TOTAL_WRITES / (t_ark_prep_ms / 1000.0));
  }

  // Run 4: Raw SQLite batched
  {
    printf("\n  [4/4] Raw SQLite  (batched: 1 txn per %d writes)\n", BATCH_SIZE);
    sqlite3_exec(raw_handle, "DELETE FROM bench_data", NULL, NULL, NULL);

    g_seed = 42; g_max_id = 0;
    rows_raw_batch = bench_raw_batched(raw_handle, TOTAL_WRITES, &t_raw_batch_ms);

    int final_rows = count_rows(raw_handle, "bench_data");
    printf("  Result: %d rows, %.0f ms, %.0f writes/sec\n",
           final_rows, t_raw_batch_ms,
           (double)TOTAL_WRITES / (t_raw_batch_ms / 1000.0));
  }

  // ── Phase 3: Comparison ─────────────────────────────────────────
  printf("\n── Phase 3: Comparison ────────────────────────────────────\n\n");

  double rps_raw_txn   = (double)TOTAL_WRITES / (t_raw_txn_ms / 1000.0);
  double rps_ark_exec  = (double)TOTAL_WRITES / (t_ark_exec_ms / 1000.0);
  double rps_ark_prep  = (double)TOTAL_WRITES / (t_ark_prep_ms / 1000.0);
  double rps_raw_batch = (double)TOTAL_WRITES / (t_raw_batch_ms / 1000.0);

  double overhead_exec  = ((t_ark_exec_ms - t_raw_txn_ms) / t_raw_txn_ms) * 100.0;
  double overhead_prep  = ((t_ark_prep_ms - t_raw_txn_ms) / t_raw_txn_ms) * 100.0;

  printf("  ┌─────────────────────┬──────────┬─────────────┬──────────┐\n");
  printf("  │ Runner              │ Time (ms)│ Writes/sec  │ Overhead │\n");
  printf("  ├─────────────────────┼──────────┼─────────────┼──────────┤\n");
  printf("  │ Raw (txn/write)     │ %8.0f │ %10.0f │    —     │\n",
         t_raw_txn_ms, rps_raw_txn);
  printf("  │ Arkilian db_exec    │ %8.0f │ %10.0f │ %+7.1f%% │\n",
         t_ark_exec_ms, rps_ark_exec, overhead_exec);
  printf("  │ Arkilian prepare    │ %8.0f │ %10.0f │ %+7.1f%% │\n",
         t_ark_prep_ms, rps_ark_prep, overhead_prep);
  printf("  │ Raw (batched %4d)  │ %8.0f │ %10.0f │    —     │\n",
         BATCH_SIZE, t_raw_batch_ms, rps_raw_batch);
  printf("  └─────────────────────┴──────────┴─────────────┴──────────┘\n");

  // Log growth
  {
    int total_log = count_rows(raw_handle, "_arkilian_log");
    double log_mb = (double)total_log * 256.0 / (1024.0 * 1024.0); // rough estimate
    printf("\n  _arkilian_log rows: %d (~%.1f MB estimated)\n",
           total_log, log_mb);
  }

  printf("\n  NOTE: Overhead = (Arkilian_time - Raw_time) / Raw_time\n");
  printf("        This measures the cost of: mutex, BEGIN/COMMIT,\n");
  printf("        and INSERT into _arkilian_log per write.\n");
  printf("        The batched run shows the theoretical ceiling with\n");
  printf("        transaction batching (no log overhead).\n\n");

  // ── Cleanup ─────────────────────────────────────────────────────
  db_close(db);
  remove(BENCH_DB);

  return 0;
}
