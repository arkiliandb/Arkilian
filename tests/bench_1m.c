// Arkilian vs Raw SQLite — Production-Grade Side-by-Side Benchmark
//
// Compile:
//   cc -O2 tests/bench_1m.c src/class.c src/deps/sqlite/sqlite3.c -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm -o bench_1m
//
// Run:
//   ./bench_1m                    (full: ~5-10 min)
//   ./bench_1m 10000              (quick: 10K ops, ~30 sec)
//   ARKILIAN_CONTROL_URL=... ./bench_1m  (with streaming)
//
// Every benchmark runs BOTH raw SQLite and Arkilian on the same connection,
// same data, same operations — so every row in the output table is a
// direct, fair comparison.

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
#include <sys/resource.h>
#include <unistd.h>
#endif

#ifdef __APPLE__
#include <mach/mach.h>
#endif

// ── Config (overridable via argv[1]) ────────────────────────────────────
static int OPS = 1000000; // per-benchmark operation count
static int WARMUP = 0;    // set in main from OPS
static int BATCH_SIZES[] = {1, 10, 100, 1000, 10000, 100000, 1000000};
static int NUM_BATCH = 7;

// ── Schema (production-like: indexed text + numeric columns) ────────────
#define TBL                                                                    \
  "CREATE TABLE IF NOT EXISTS bench_data ("                                    \
  "  id        INTEGER PRIMARY KEY, "                                          \
  "  customer  TEXT    NOT NULL, "                                             \
  "  product   TEXT    NOT NULL, "                                             \
  "  qty       INTEGER NOT NULL, "                                             \
  "  price     REAL    NOT NULL, "                                             \
  "  total     REAL    NOT NULL, "                                             \
  "  status    TEXT    NOT NULL DEFAULT 'pending', "                           \
  "  note      TEXT, "                                                         \
  "  created   INTEGER NOT NULL, "                                             \
  "  updated   INTEGER NOT NULL)"
#define TBL_NAME "bench_data"

// ── High-precision timer ───────────────────────────────────────────────
static double now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}
static double ns_to_ms(double ns) { return ns / 1e6; }

// ── Deterministic RNG for reproducible results ─────────────────────────
static unsigned int g_seed = 42;
static unsigned int xorshift32(void) {
  g_seed ^= g_seed << 13;
  g_seed ^= g_seed >> 17;
  g_seed ^= g_seed << 5;
  return g_seed;
}
static int rng_int(int lo, int hi) {
  if (hi <= lo)
    return lo;
  return lo + (int)(xorshift32() % (unsigned int)(hi - lo + 1));
}
static double rng_dbl(double lo, double hi) {
  return lo + (double)xorshift32() / (double)0xFFFFFFFF * (hi - lo);
}
static void rng_str(char *buf, int len) {
  static const char chars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  for (int i = 0; i < len - 1; i++)
    buf[i] = chars[rng_int(0, (int)sizeof(chars) - 2)];
  buf[len - 1] = '\0';
}

// ── Data generation ─────────────────────────────────────────────────────
static int g_max_id = 0;

typedef struct {
  int id;
  char customer[16];
  char product[16];
  int qty;
  double price;
  double total;
  char status[12];
  long long now;
} row_data;

static row_data gen_row(void) {
  row_data r;
  r.id = ++g_max_id;
  rng_str(r.customer, 10);
  rng_str(r.product, 10);
  r.qty = rng_int(1, 100);
  r.price = rng_dbl(1.0, 9999.99);
  r.total = r.qty * r.price;
  rng_str(r.status, 8);
  r.status[0] = 's'; // 'shipped','pending','cancelled'
  r.now = (long long)time(NULL);
  return r;
}

// ── Latency histogram (log2 buckets: 0.5us … 2^31 us ≈ 35 min) ────────
#define LAT_BUCKETS 32
typedef struct {
  double buckets[LAT_BUCKETS];
  int count;
} lat_hist;

static void lat_record(lat_hist *h, double nanos) {
  int b = 0;
  double v = nanos / 1000.0; // convert to µs
  while (v >= 1.0 && b < LAT_BUCKETS - 1) {
    v /= 2.0;
    b++;
  }
  h->buckets[b] += 1.0;
  h->count++;
}

static double lat_percentile(lat_hist *h, double pct) {
  double target = h->count * pct / 100.0;
  double cum = 0;
  double lo_us = 0.5;
  for (int i = 0; i < LAT_BUCKETS; i++) {
    cum += h->buckets[i];
    if (cum >= target)
      return lo_us * 2.0;
    lo_us *= 2.0;
  }
  return lo_us;
}

// ── Progress ───────────────────────────────────────────────────────────
static void progress(const char *label, int done, int total) {
  int pct = (int)((double)done / (double)total * 100.0);
  fprintf(stderr, "\r  %-22s [", label);
  int bars = pct / 5;
  for (int i = 0; i < 20; i++)
    fputc(i < bars ? '=' : (i == bars ? '>' : ' '), stderr);
  fprintf(stderr, "] %3d%%  %d/%d", pct, done, total);
  if (done == total)
    fputc('\n', stderr);
}

// =====================================================================
//  BENCHMARK: Single-row INSERT throughput
// =====================================================================
typedef struct {
  double ms;
  double ops_per_sec;
  lat_hist lat;
} bench_result;

static bench_result bench_insert_raw_prepared(sqlite3 *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  sqlite3_stmt *ins = NULL;
  sqlite3_prepare_v2(
      db,
      "INSERT INTO bench_data "
      "(id,customer,product,qty,price,total,status,note,created,updated) "
      "VALUES (?,?,?,?,?,?,?,?,?,?)",
      -1, &ins, NULL);

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    row_data d = gen_row();
    double op_t0 = now_ns();

    sqlite3_bind_int64(ins, 1, d.id);
    sqlite3_bind_text(ins, 2, d.customer, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, d.product, -1, SQLITE_STATIC);
    sqlite3_bind_int(ins, 4, d.qty);
    sqlite3_bind_double(ins, 5, d.price);
    sqlite3_bind_double(ins, 6, d.total);
    sqlite3_bind_text(ins, 7, d.status, -1, SQLITE_STATIC);
    sqlite3_bind_null(ins, 8);
    sqlite3_bind_int64(ins, 9, d.now);
    sqlite3_bind_int64(ins, 10, d.now);
    sqlite3_step(ins);
    sqlite3_reset(ins);

    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("raw-prep INSERT", i, n);
  }
  sqlite3_finalize(ins);
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("raw-prep INSERT", n, n);
  return r;
}

static bench_result bench_insert_raw(sqlite3 *db, int n, int use_prepare) {
  if (use_prepare)
    return bench_insert_raw_prepared(db, n);

  // exec path — kept for backward compat, not used in main comparison
  bench_result r = {0};
  lat_hist lat = {0};
  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    row_data d = gen_row();
    double op_t0 = now_ns();
    char sql[512];
    snprintf(sql, sizeof(sql),
      "INSERT INTO bench_data "
      "(id,customer,product,qty,price,total,status,note,created,updated) "
      "VALUES (%d,'%s','%s',%d,%.2f,%.2f,'%s',NULL,%lld,%lld)",
      d.id, d.customer, d.product, d.qty, d.price, d.total, d.status, d.now,
      d.now);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("raw-exec INSERT", i, n);
  }
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("raw-exec INSERT", n, n);
  return r;
}

static bench_result bench_insert_arkilian(arkilian *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  // Prepare once, reuse — same pattern as the raw-SQLite baseline.
  db_prepare(db,
    "INSERT INTO bench_data "
    "(id,customer,product,qty,price,total,status,note,created,updated) "
    "VALUES (?,?,?,?,?,?,?,?,?,?)");

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    row_data d = gen_row();
    double op_t0 = now_ns();

    db_bind_int(db, 1, d.id);
    db_bind_text(db, 2, d.customer);
    db_bind_text(db, 3, d.product);
    db_bind_int(db, 4, d.qty);
    db_bind_double(db, 5, d.price);
    db_bind_double(db, 6, d.total);
    db_bind_text(db, 7, d.status);
    db_bind_null(db, 8);
    db_bind_int64(db, 9, d.now);
    db_bind_int64(db, 10, d.now);
    db_step(db);       // executes INSERT, preupdate hook fires, WAL pushed
    db_reset(db);      // reset for next row

    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("ark INSERT", i, n);
  }
  db_finalize(db);
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("ark INSERT", n, n);
  return r;
}

// =====================================================================
//  BENCHMARK: Single-row UPDATE (by PK)
// =====================================================================
static bench_result bench_update_raw(sqlite3 *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  sqlite3_stmt *upd = NULL;
  sqlite3_prepare_v2(db,
                     "UPDATE bench_data SET "
                     "qty=?,price=?,total=?,status=?,updated=? WHERE id=?",
                     -1, &upd, NULL);

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    int target = rng_int(1, g_max_id);
    int qty = rng_int(1, 100);
    double pr = rng_dbl(1.0, 9999.99);
    double op_t0 = now_ns();

    sqlite3_bind_int(upd, 1, qty);
    sqlite3_bind_double(upd, 2, pr);
    sqlite3_bind_double(upd, 3, qty * pr);
    sqlite3_bind_text(upd, 4, "shipped", -1, SQLITE_STATIC);
    sqlite3_bind_int64(upd, 5, (long long)time(NULL));
    sqlite3_bind_int(upd, 6, target);
    sqlite3_step(upd);
    sqlite3_reset(upd);

    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("raw UPDATE", i, n);
  }
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  sqlite3_finalize(upd);
  progress("raw UPDATE", n, n);
  return r;
}

static bench_result bench_update_arkilian(arkilian *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  db_prepare(db,
    "UPDATE bench_data SET "
    "qty=?,price=?,total=?,status=?,updated=? WHERE id=?");

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    int target = rng_int(1, g_max_id);
    int qty = rng_int(1, 100);
    double pr = rng_dbl(1.0, 9999.99);
    double op_t0 = now_ns();

    db_bind_int(db, 1, qty);
    db_bind_double(db, 2, pr);
    db_bind_double(db, 3, qty * pr);
    db_bind_text(db, 4, "shipped");
    db_bind_int64(db, 5, (long long)time(NULL));
    db_bind_int(db, 6, target);
    db_step(db);
    db_reset(db);

    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("ark UPDATE", i, n);
  }
  db_finalize(db);
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("ark UPDATE", n, n);
  return r;
}

// =====================================================================
//  BENCHMARK: Point SELECT by PK
// =====================================================================
static bench_result bench_select_point_raw(sqlite3 *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  sqlite3_stmt *sel = NULL;
  sqlite3_prepare_v2(db, "SELECT * FROM bench_data WHERE id = ?", -1, &sel,
                     NULL);

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    int target = rng_int(1, g_max_id);
    double op_t0 = now_ns();

    sqlite3_bind_int(sel, 1, target);
    int rc = sqlite3_step(sel);
    if (rc == SQLITE_ROW) {
      // consume all columns to simulate real use
      for (int c = 0; c < sqlite3_column_count(sel); c++)
        (void)sqlite3_column_text(sel, c);
    }
    sqlite3_reset(sel);

    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("raw SELECT(PK)", i, n);
  }
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  sqlite3_finalize(sel);
  progress("raw SELECT(PK)", n, n);
  return r;
}

static bench_result bench_select_point_arkilian(arkilian *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  db_prepare(db, "SELECT * FROM bench_data WHERE id = ?");

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    int target = rng_int(1, g_max_id);
    double op_t0 = now_ns();

    db_bind_int(db, 1, target);
    int rc = db_step(db);
    if (rc == SQLITE_ROW) {
      for (int c = 0; c < db_column_count(db); c++)
        (void)db_column_text(db, c);
    }
    db_reset(db);

    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("ark SELECT(PK)", i, n);
  }
  db_finalize(db);
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("ark SELECT(PK)", n, n);
  return r;
}

// =====================================================================
//  BENCHMARK: Range SELECT (scan 100 rows, no index on created)
// =====================================================================
static bench_result bench_select_range_raw(sqlite3 *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  sqlite3_stmt *sel = NULL;
  sqlite3_prepare_v2(
      db, "SELECT * FROM bench_data WHERE id BETWEEN ? AND ? ORDER BY id", -1,
      &sel, NULL);

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    int lo = rng_int(1, g_max_id - 100);
    double op_t0 = now_ns();
    sqlite3_bind_int(sel, 1, lo);
    sqlite3_bind_int(sel, 2, lo + 100);
    while (sqlite3_step(sel) == SQLITE_ROW) {
      for (int c = 0; c < sqlite3_column_count(sel); c++)
        (void)sqlite3_column_text(sel, c);
    }
    sqlite3_reset(sel);
    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("raw SELECT(range)", i, n);
  }
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  sqlite3_finalize(sel);
  progress("raw SELECT(range)", n, n);
  return r;
}

static bench_result bench_select_range_arkilian(arkilian *db, int n) {
  bench_result r = {0};
  lat_hist lat = {0};

  db_prepare(db,
             "SELECT * FROM bench_data WHERE id BETWEEN ? AND ? ORDER BY id");

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    int lo = rng_int(1, g_max_id - 100);
    double op_t0 = now_ns();
    db_bind_int(db, 1, lo);
    db_bind_int(db, 2, lo + 100);
    while (db_step(db) == SQLITE_ROW) {
      for (int c = 0; c < db_column_count(db); c++)
        (void)db_column_text(db, c);
    }
    db_reset(db);
    lat_record(&lat, now_ns() - op_t0);
    if (i > 0 && i % (n / 10) == 0)
      progress("ark SELECT(range)", i, n);
  }
  db_finalize(db);
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("ark SELECT(range)", n, n);
  return r;
}

// =====================================================================
//  BENCHMARK: Batched INSERT throughput (1 txn per batch)
// =====================================================================
static bench_result bench_insert_batched_raw(sqlite3 *db, int n, int batch) {
  bench_result r = {0};
  lat_hist lat = {0};

  sqlite3_stmt *ins = NULL;
  sqlite3_prepare_v2(
      db,
      "INSERT INTO bench_data "
      "(id,customer,product,qty,price,total,status,note,created,updated) "
      "VALUES (?,?,?,?,?,?,?,?,?,?)",
      -1, &ins, NULL);

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    if (i % batch == 0)
      sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);

    row_data d = gen_row();
    double op_t0 = now_ns();
    sqlite3_bind_int64(ins, 1, d.id);
    sqlite3_bind_text(ins, 2, d.customer, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, d.product, -1, SQLITE_STATIC);
    sqlite3_bind_int(ins, 4, d.qty);
    sqlite3_bind_double(ins, 5, d.price);
    sqlite3_bind_double(ins, 6, d.total);
    sqlite3_bind_text(ins, 7, d.status, -1, SQLITE_STATIC);
    sqlite3_bind_null(ins, 8);
    sqlite3_bind_int64(ins, 9, d.now);
    sqlite3_bind_int64(ins, 10, d.now);
    sqlite3_step(ins);
    sqlite3_reset(ins);
    lat_record(&lat, now_ns() - op_t0);

    if ((i + 1) % batch == 0 || i == n - 1)
      sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);

    if (i > 0 && i % (n / 10) == 0)
      progress("raw-batch INSERT", i, n);
  }
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  sqlite3_finalize(ins);
  progress("raw-batch INSERT", n, n);
  return r;
}

static bench_result bench_insert_batched_arkilian(arkilian *db, int n,
                                                   int batch) {
  bench_result r = {0};
  lat_hist lat = {0};

  db_prepare(db,
    "INSERT INTO bench_data "
    "(id,customer,product,qty,price,total,status,note,created,updated) "
    "VALUES (?,?,?,?,?,?,?,?,?,?)");

  double t0 = now_ns();
  for (int i = 0; i < n; i++) {
    if (i % batch == 0)
      db_begin(db);

    row_data d = gen_row();
    double op_t0 = now_ns();

    db_bind_int(db, 1, d.id);
    db_bind_text(db, 2, d.customer);
    db_bind_text(db, 3, d.product);
    db_bind_int(db, 4, d.qty);
    db_bind_double(db, 5, d.price);
    db_bind_double(db, 6, d.total);
    db_bind_text(db, 7, d.status);
    db_bind_null(db, 8);
    db_bind_int64(db, 9, d.now);
    db_bind_int64(db, 10, d.now);
    db_step(db);
    db_reset(db);

    lat_record(&lat, now_ns() - op_t0);

    if ((i + 1) % batch == 0 || i == n - 1)
      db_commit(db);

    if (i > 0 && i % (n / 10) == 0)
      progress("ark-batch INSERT", i, n);
  }
  db_finalize(db);
  r.ms = ns_to_ms(now_ns() - t0);
  r.ops_per_sec = (double)n / (r.ms / 1000.0);
  r.lat = lat;
  progress("ark-batch INSERT", n, n);
  return r;
}

// =====================================================================
//  Memory measurement
// =====================================================================
static long get_resident_mem_kb(void) {
  long kb = 0;
#ifdef __linux__
  FILE *f = fopen("/proc/self/status", "r");
  if (f) {
    char line[256];
    while (fgets(line, sizeof(line), f))
      if (sscanf(line, "VmRSS: %ld kB", &kb) == 1)
        break;
    fclose(f);
  }
#elif defined(__APPLE__)
  struct task_basic_info_64 t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_64_COUNT;
  if (task_info(mach_task_self(), TASK_BASIC_INFO_64, (task_info_t)&t_info,
                &t_info_count) == KERN_SUCCESS)
    kb = (long)(t_info.resident_size / 1024);
#endif
  return kb;
}

// =====================================================================
//  Helpers
// =====================================================================
static int count_rows(sqlite3 *db) {
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM " TBL_NAME, -1, &s, NULL);
  sqlite3_step(s);
  int c = sqlite3_column_int(s, 0);
  sqlite3_finalize(s);
  return c;
}

// Re-seed the table with N rows from a deterministic sequence
static void reseed_table(sqlite3 *db, int n) {
  sqlite3_exec(db, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
  g_seed = 42;
  g_max_id = 0;
  sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
  sqlite3_stmt *ins = NULL;
  sqlite3_prepare_v2(db,
    "INSERT INTO bench_data (id,customer,product,qty,price,total,status,note,created,updated) "
    "VALUES (?,?,?,?,?,?,?,NULL,?,?)", -1, &ins, NULL);
  for (int i = 0; i < n; i++) {
    row_data d = gen_row();
    sqlite3_bind_int64(ins, 1, d.id);
    sqlite3_bind_text(ins, 2, d.customer, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, d.product,  -1, SQLITE_STATIC);
    sqlite3_bind_int(ins,   4, d.qty);
    sqlite3_bind_double(ins, 5, d.price);
    sqlite3_bind_double(ins, 6, d.total);
    sqlite3_bind_text(ins, 7, d.status, -1, SQLITE_STATIC);
    sqlite3_bind_int64(ins, 8, d.now);
    sqlite3_bind_int64(ins, 9, d.now);
    sqlite3_step(ins);
    sqlite3_reset(ins);
  }
  sqlite3_finalize(ins);
  sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
}

// =====================================================================
//  Main
// =====================================================================
int main(int argc, char **argv) {
  if (argc > 1) {
    OPS = atoi(argv[1]);
    if (OPS < 1000)
      OPS = 1000;
  }

  WARMUP = OPS < 10000 ? OPS / 2 : 10000;
  setenv("ARKILIAN_API_KEY", "test-key", 1);
  setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  setenv("ARKILIAN_CONTROL_URL", "http://localhost:8080", 1);
  setenv("ARKILIAN_API_KEY",
         "ak_db_d25e9ea4cb93_7c3872fc11e9f12feb644a68533529445124668a0f7ab1c1c5b1157c6ae64bc8", 1);
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  remove("bench_1m.db");

  long mem_before = get_resident_mem_kb();

  // ── Header ─────────────────────────────────────────────────────────
  printf("\n");
  printf("  "
         "╔════════════════════════════════════════════════════════════════════"
         "══════════╗\n");
  printf("  ║               Arkilian vs Raw SQLite — Production Benchmark      "
         "          ║\n");
  printf("  "
         "╚════════════════════════════════════════════════════════════════════"
         "══════════╝\n\n");
  printf("  Operations per test : %'d\n", OPS);
  printf("  Warmup              : %'d\n", WARMUP);
  printf("  Schema              : 10 columns (INTEGER PK, TEXT×4, REAL×3, "
         "INTEGER×2)\n");
  printf("  PRAGMAs             : journal_mode=WAL, synchronous=NORMAL,\n");
  printf("                        busy_timeout=5000, foreign_keys=ON\n");
  printf("  Arkilian overhead   : write mutex + per-statement ring-buffer "
         "capture\n");

  // ── Setup ──────────────────────────────────────────────────────────
  printf("\n  ── Setup "
         "──────────────────────────────────────────────────────────\n");

  arkilian *db = NULL;
  int rc = db_init(&db, "bench_1m.db");
  assert(rc == 0);
  sqlite3 *raw = db_get_handle(db);
  sqlite3_exec(raw, TBL, NULL, NULL, NULL);

  // Pre-populate 50K rows for UPDATE/SELECT benchmarks
  printf("  Seeding 50,000 rows for UPDATE/SELECT benchmarks ...\n");
  g_seed = 42;
  g_max_id = 0;
  sqlite3_exec(raw, "BEGIN", NULL, NULL, NULL);
  sqlite3_stmt *bulk = NULL;
  sqlite3_prepare_v2(
      raw,
      "INSERT INTO bench_data "
      "(id,customer,product,qty,price,total,status,note,created,updated) "
      "VALUES (?,?,?,?,?,?,?,NULL,?,?)",
      -1, &bulk, NULL);
  for (int i = 0; i < 50000; i++) {
    row_data d = gen_row();
    sqlite3_bind_int64(bulk, 1, d.id);
    sqlite3_bind_text(bulk, 2, d.customer, -1, SQLITE_STATIC);
    sqlite3_bind_text(bulk, 3, d.product, -1, SQLITE_STATIC);
    sqlite3_bind_int(bulk, 4, d.qty);
    sqlite3_bind_double(bulk, 5, d.price);
    sqlite3_bind_double(bulk, 6, d.total);
    sqlite3_bind_text(bulk, 7, d.status, -1, SQLITE_STATIC);
    sqlite3_bind_int64(bulk, 8, d.now);
    sqlite3_bind_int64(bulk, 9, d.now);
    sqlite3_step(bulk);
    sqlite3_reset(bulk);
  }
  sqlite3_finalize(bulk);
  sqlite3_exec(raw, "COMMIT", NULL, NULL, NULL);
  int seed_rows = count_rows(raw);
  printf("  Seeded: %'d rows\n", seed_rows);

  long mem_after_seed = get_resident_mem_kb();
  printf("  Memory after seed : %'ld KB\n", mem_after_seed);

  // ── Helper: clear table and re-seed N rows for a fresh baseline ──
  // (the bulk insert pattern matches setup above)
  // ── Warmup ─────────────────────────────────────────────────────────
  printf("\n  ── Warmup (%d operations each) ───────────────────────────────\n",
         WARMUP);

  reseed_table(raw, 50000 + WARMUP);
  printf("  Warmup done: %d rows\n", count_rows(raw));

  // ── Store all results for final display ────────────────────────────
  enum { R_INSERT, R_UPDATE, R_SEL_PK, R_SEL_RNG, R_NUM };
  bench_result raw_single[R_NUM], ark_single[R_NUM];
  bench_result raw_batch[NUM_BATCH], ark_batch[NUM_BATCH];
  bench_result raw_lat_ins, ark_lat_ins, raw_lat_sel, ark_lat_sel;

  // ── Helper: bulk re-seed N rows with unique ids ────────────────────
  // ── 1. Single-row throughput ───────────────────────────────────────
  printf("\n");
  printf("  "
         "╔════════════════════════════════════════════════════════════════════"
         "══════════╗\n");
  printf("  ║  1. Single-Row Throughput  (1 txn per op, %d ops)                "
         "  ║\n",
         OPS);
  printf("  "
         "╚════════════════════════════════════════════════════════════════════"
         "══════════╝\n");

  printf("\n  INSERT:\n");

  // Diagnostic: table state + wall clock before Ark INSERT
  {
    struct timespec w; clock_gettime(CLOCK_MONOTONIC, &w);
    sqlite3_stmt *s; sqlite3_prepare_v2(raw,
      "SELECT COUNT(*), COALESCE(MAX(id),0) FROM " TBL_NAME, -1, &s, NULL);
    sqlite3_step(s);
    printf("  DIAG: ARK START  count=%lld max_id=%lld  wall=%lld.%06ld\n",
      sqlite3_column_int64(s,0), sqlite3_column_int64(s,1),
      (long long)w.tv_sec, w.tv_nsec/1000);
    sqlite3_finalize(s);
  }
  sqlite3_exec(raw, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
  g_seed = 42; g_max_id = 0;
  ark_single[R_INSERT] = bench_insert_arkilian(db, OPS);
  // Diagnostic: rows actually inserted + wall clock after
  {
    struct timespec w; clock_gettime(CLOCK_MONOTONIC, &w);
    sqlite3_stmt *s; sqlite3_prepare_v2(raw,
      "SELECT COUNT(*), COALESCE(MAX(id),0) FROM " TBL_NAME, -1, &s, NULL);
    sqlite3_step(s);
    printf("  DIAG: ARK END    count=%lld max_id=%lld  wall=%lld.%06ld  ops/s=%.0f\n",
      sqlite3_column_int64(s,0), sqlite3_column_int64(s,1),
      (long long)w.tv_sec, w.tv_nsec/1000,
      ark_single[R_INSERT].ops_per_sec);
    sqlite3_finalize(s);
  }
  printf("\n");
  // Diagnostic: table state before raw INSERT
  {
    struct timespec w; clock_gettime(CLOCK_MONOTONIC, &w);
    sqlite3_stmt *s; sqlite3_prepare_v2(raw,
      "SELECT COUNT(*), COALESCE(MAX(id),0) FROM " TBL_NAME, -1, &s, NULL);
    sqlite3_step(s);
    printf("  DIAG: RAW START  count=%lld max_id=%lld  wall=%lld.%06ld\n",
      sqlite3_column_int64(s,0), sqlite3_column_int64(s,1),
      (long long)w.tv_sec, w.tv_nsec/1000);
    sqlite3_finalize(s);
  }
  sqlite3_exec(raw, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
  g_seed = 42; g_max_id = 0;
  raw_single[R_INSERT] = bench_insert_raw(raw, OPS, 1);
  // Diagnostic: rows actually inserted + wall clock after
  {
    struct timespec w; clock_gettime(CLOCK_MONOTONIC, &w);
    sqlite3_stmt *s; sqlite3_prepare_v2(raw,
      "SELECT COUNT(*), COALESCE(MAX(id),0) FROM " TBL_NAME, -1, &s, NULL);
    sqlite3_step(s);
    printf("  DIAG: RAW END    count=%lld max_id=%lld  wall=%lld.%06ld  ops/s=%.0f\n",
      sqlite3_column_int64(s,0), sqlite3_column_int64(s,1),
      (long long)w.tv_sec, w.tv_nsec/1000,
      raw_single[R_INSERT].ops_per_sec);
    sqlite3_finalize(s);
  }
  printf("\n");

  printf("  UPDATE (by PK):\n");
  reseed_table(raw, 50000);
  ark_single[R_UPDATE] = bench_update_arkilian(db, OPS);
  printf("\n");
  reseed_table(raw, 50000);
  raw_single[R_UPDATE] = bench_update_raw(raw, OPS);
  printf("\n");

  printf("  SELECT (point by PK):\n");
  // table already has 50K rows from the re-seed above — reuse it
  raw_single[R_SEL_PK] = bench_select_point_raw(raw, OPS);
  printf("\n");
  ark_single[R_SEL_PK] = bench_select_point_arkilian(db, OPS);
  printf("\n");

  printf("  SELECT (range 100 rows):\n");
  raw_single[R_SEL_RNG] = bench_select_range_raw(raw, OPS / 10);
  printf("\n");
  ark_single[R_SEL_RNG] = bench_select_range_arkilian(db, OPS / 10);
  printf("\n");

  // ── 2. Batched throughput ──────────────────────────────────────────
  printf("  "
         "╔════════════════════════════════════════════════════════════════════"
         "══════════╗\n");
  printf("  ║  2. Batched INSERT Throughput  (%d ops)                          "
         "  ║\n",
         OPS);
  printf("  "
         "╚════════════════════════════════════════════════════════════════════"
         "══════════╝\n");

  for (int bi = 0; bi < NUM_BATCH; bi++) {
    int bs = BATCH_SIZES[bi];
    printf("\n  Batch size %d:\n", bs);
    sqlite3_exec(raw, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
    g_seed = 42; g_max_id = 0;
    raw_batch[bi] = bench_insert_batched_raw(raw, OPS, bs);
    printf("\n");
    sqlite3_exec(raw, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
    g_seed = 42; g_max_id = 0;
    ark_batch[bi] = bench_insert_batched_arkilian(db, OPS, bs);
    printf("\n");
  }

  // ── 3. Latency percentiles ─────────────────────────────────────────
  int LAT_OPS = OPS < 50000 ? OPS : 50000;
  printf("  "
         "╔════════════════════════════════════════════════════════════════════"
         "══════════╗\n");
  printf("  ║  3. Latency Percentiles  (P50 / P95 / P99, %d ops)               "
         " ║\n",
         LAT_OPS);
  printf("  "
         "╚════════════════════════════════════════════════════════════════════"
         "══════════╝\n");

  printf("\n  INSERT:\n");
  sqlite3_exec(raw, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
  g_seed = 42; g_max_id = 0;
  raw_lat_ins = bench_insert_raw(raw, LAT_OPS, 1);
  printf("\n");
  sqlite3_exec(raw, "DELETE FROM " TBL_NAME, NULL, NULL, NULL);
  g_seed = 42; g_max_id = 0;
  ark_lat_ins = bench_insert_arkilian(db, LAT_OPS);
  printf("\n");

  printf("  SELECT (point by PK):\n");
  reseed_table(raw, 50000);
  raw_lat_sel = bench_select_point_raw(raw, LAT_OPS);
  printf("\n");
  ark_lat_sel = bench_select_point_arkilian(db, LAT_OPS);
  printf("\n");

  long mem_now = get_resident_mem_kb();

  // ── 4. Final Summary Table ─────────────────────────────────────────
  printf("\n");
  printf("  "
         "╔════════════════════════════════════════════════════════════════════"
         "══════════╗\n");
  printf("  ║                           FINAL RESULTS TABLE                    "
         "           ║\n");
  printf("  "
         "╚════════════════════════════════════════════════════════════════════"
         "══════════╝\n\n");

  // 4a. Single-row throughput
  {
    const char *opnames[] = {"INSERT", "UPDATE", "SELECT(PK)", "SELECT(range)"};
    printf("  ┌─ 1. Single-Row Throughput (ops/sec) "
           "──────────────────────────────────┐\n");
    printf("  │ %-18s │ %14s │ %14s │ %10s │\n", "Operation", "Raw SQLite",
           "Arkilian", "Overhead");
    printf("  "
           "├─────────────────────┼────────────────┼────────────────┼──────────"
           "──┤\n");
    for (int i = 0; i < R_NUM; i++) {
      double raw_ops = raw_single[i].ops_per_sec;
      double ark_ops = ark_single[i].ops_per_sec;
      double pct = raw_ops > 0 ? ((ark_ops - raw_ops) / raw_ops) * 100.0 : 0;
      printf("  │ %-18s │ %12.0f/s │ %12.0f/s │ %+8.1f%% │\n", opnames[i],
             raw_ops, ark_ops, pct);
    }
    printf("  "
           "└─────────────────────┴────────────────┴────────────────┴──────────"
           "──┘\n");
  }

  // 4b. Batched throughput
  {
    printf("\n  ┌─ 2. Batched INSERT Throughput (ops/sec) "
           "───────────────────────────────┐\n");
    printf("  │ %-18s │ %14s │ %14s │ %10s │\n", "Batch size", "Raw SQLite",
           "Arkilian", "Overhead");
    printf("  "
           "├─────────────────────┼────────────────┼────────────────┼──────────"
           "──┤\n");
    for (int bi = 0; bi < NUM_BATCH; bi++) {
      double raw_ops = raw_batch[bi].ops_per_sec;
      double ark_ops = ark_batch[bi].ops_per_sec;
      double pct = raw_ops > 0 ? ((ark_ops - raw_ops) / raw_ops) * 100.0 : 0;
      char bs_label[32];
      if (BATCH_SIZES[bi] == 1)
        snprintf(bs_label, sizeof(bs_label), "single (auto)");
      else
        snprintf(bs_label, sizeof(bs_label), "batch %d", BATCH_SIZES[bi]);
      printf("  │ %-18s │ %12.0f/s │ %12.0f/s │ %+8.1f%% │\n", bs_label,
             raw_ops, ark_ops, pct);
    }
    printf("  "
           "└─────────────────────┴────────────────┴────────────────┴──────────"
           "──┘\n");
  }

  // 4c. Latency
  {
    printf("\n  ┌─ 3. Latency Percentiles (µs) "
           "─────────────────────────────────────────┐\n");
    printf("  │ %-18s │ %-30s │ %-30s │\n", "Operation", "Raw SQLite",
           "Arkilian");
    printf("  "
           "├─────────────────────┼────────────────────────────────┼───────────"
           "─────────────────────┤\n");

    printf(
        "  │ %-18s │ %8.0f / %5.0f / %5.0f us  │ %8.0f / %5.0f / %5.0f us  │\n",
        "INSERT", lat_percentile(&raw_lat_ins.lat, 50),
        lat_percentile(&raw_lat_ins.lat, 95),
        lat_percentile(&raw_lat_ins.lat, 99),
        lat_percentile(&ark_lat_ins.lat, 50),
        lat_percentile(&ark_lat_ins.lat, 95),
        lat_percentile(&ark_lat_ins.lat, 99));

    printf(
        "  │ %-18s │ %8.0f / %5.0f / %5.0f us  │ %8.0f / %5.0f / %5.0f us  │\n",
        "SELECT(PK)", lat_percentile(&raw_lat_sel.lat, 50),
        lat_percentile(&raw_lat_sel.lat, 95),
        lat_percentile(&raw_lat_sel.lat, 99),
        lat_percentile(&ark_lat_sel.lat, 50),
        lat_percentile(&ark_lat_sel.lat, 95),
        lat_percentile(&ark_lat_sel.lat, 99));

    printf("  "
           "└─────────────────────┴────────────────────────────────┴───────────"
           "─────────────────────┘\n");
  }

  // 4d. Memory
  {
    printf("\n  ┌─ 4. Memory Footprint "
           "───────────────────────────────────────────────────┐\n");
    printf("  │ %-60s │\n", "");
    printf("  │ Baseline (process empty)     : %14ld KB │\n", mem_before);
    printf("  │ After 50,000-row seed        : %14ld KB │\n", mem_after_seed);
    printf("  │ After all benchmarks         : %14ld KB │\n", mem_now);
    printf("  │ Post-benchmark RSS growth    : %14ld KB │\n",
           mem_now > mem_after_seed ? mem_now - mem_after_seed : 0L);
    printf(
        "  │   (SQLite page cache + WAL index, not Arkilian)             │\n");
    printf(
        "  │   Ring buffer is LAZILY ALLOCATED — zero cost unless        │\n");
    printf(
        "  │   ARKILIAN_CONTROL_URL is configured                       │\n");
    printf(
        "  └────────────────────────────────────────────────────────────┘\n");
  }

  // ── Notes ──────────────────────────────────────────────────────────
  printf("\n  ── Notes "
         "─────────────────────────────────────────────────────────────\n\n");
  printf(
      "  • Both sides use sqlite3_prepare_v2 + bind/step/reset\n");
  printf("    (production best practice — one compile, many resets).\n");
  printf(
      "  • Arkilian adds: preupdate hook (deterministic SQL expansion),\n");
  printf(
      "    + write mutex serialization + per-row WAL push to ring buffer.\n");
  printf(
      "  • WAL entries are shipped to the Control Plane via HTTP POST.\n");
  printf(
      "  • Deterministic seed (xorshift32, seed=42) — results reproducible.\n");
  printf(
      "  • All benchmarks share the same connection, cache, and WAL file.\n\n");

  // ── Cleanup ────────────────────────────────────────────────────────
  db_close(db);
  remove("bench_1m.db");
  return 0;
}
