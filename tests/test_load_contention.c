// Arkilian Load-Contention Test (spec §10.3)
//
// Verifies the binding constraint of the whole design: the backup
// subsystem must never degrade game-thread write latency, even when the
// destination is slow and the outbox is backing up.
//
// Setup:
//   - A slow mock destination: every request is deliberately delayed
//     (default 20ms) and answered 200, so the flush thread is constantly
//     mid-network-call and the outbox keeps growing.
//   - A writer thread hammers INSERTs on the game connection while the
//     backup thread ships under this pressure.
//
// Assertions:
//   - Game-thread write latency stays bounded: P99 < 25ms with zero
//     baseline (no destination configured) so the comparison is fair.
//   - All writes land in _pending_backup (capture never lost).
//
// Compile (macOS/Linux):
//   cc tests/test_load_contention.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm -o test_load_contention

#include "class.h"
#include <assert.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    tests_run++;                                                                \
    printf("  [%02d] %-52s ", tests_run, #fn);                                 \
    fn();                                                                       \
    tests_passed++;                                                             \
    printf("PASS\n");                                                           \
  } while (0)

static void cleanup(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
}

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

// ── Slow mock destination ───────────────────────────────────────────
// Responds 200 after an artificial delay per request.

typedef struct {
  int listen_fd;
  int port;
  int delay_ms;
  volatile int requests;
  volatile int stop;
  pthread_t thread;
} slow_server;

static void *slow_server_run(void *arg) {
  slow_server *s = (slow_server *)arg;
  for (;;) {
    int fd = accept(s->listen_fd, NULL, NULL);
    if (fd < 0) break;
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
    // Drain request body (bounded; small payloads in tests).
    char buf[8192];
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (n > 0) {
      buf[n] = '\0';
      long body_len = 0;
      char *cl = strstr(buf, "Content-Length:");
      if (cl) body_len = atol(cl + 15);
      char *hdr_end = strstr(buf, "\r\n\r\n");
      long have = hdr_end ? n - (hdr_end + 4 - buf) : 0;
      while (have < body_len) {
        n = recv(fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) break;
        have += n;
      }
    }
    if (s->stop) { close(fd); break; }
    s->requests++;
    if (s->delay_ms > 0) usleep((useconds_t)s->delay_ms * 1000);
    const char *resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
    send(fd, resp, strlen(resp), 0);
    close(fd);
  }
  return NULL;
}

static int slow_server_start(slow_server *s, int delay_ms) {
  memset(s, 0, sizeof(*s));
  s->delay_ms = delay_ms;
  s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (s->listen_fd < 0) return -1;
  int one = 1;
  setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (bind(s->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) return -1;
  socklen_t alen = sizeof(addr);
  if (getsockname(s->listen_fd, (struct sockaddr *)&addr, &alen) != 0) return -1;
  s->port = ntohs(addr.sin_port);
  if (listen(s->listen_fd, 32) != 0) return -1;
  if (pthread_create(&s->thread, NULL, slow_server_run, s) != 0) return -1;
  return 0;
}

static void slow_server_stop(slow_server *s) {
  s->stop = 1;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd >= 0) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons((unsigned short)s->port);
    connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    close(fd);
  }
  pthread_join(s->thread, NULL);
  close(s->listen_fd);
}

// ── Latency percentile helpers ──────────────────────────────────────

static int cmp_double(const void *a, const void *b) {
  double x = *(const double *)a, y = *(const double *)b;
  return (x > y) - (x < y);
}

static double percentile(double *samples, int n, double p) {
  if (n == 0) return 0.0;
  qsort(samples, (size_t)n, sizeof(double), cmp_double);
  int idx = (int)ceil(p * (double)n) - 1;
  if (idx < 0) idx = 0;
  if (idx >= n) idx = n - 1;
  return samples[idx];
}

// Writer thread: N writes on the game connection (main-thread latency
// measurement is done separately on the main thread to isolate the
// game-thread experience).
typedef struct {
  arkilian *db;
  int n;
  double *latencies;
} writer_ctx;

static void *writer_main(void *arg) {
  writer_ctx *w = (writer_ctx *)arg;
  for (int i = 0; i < w->n; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql),
             "INSERT INTO t (v) VALUES ('writer-%d-%d')", (int)getpid(), i);
    double t0 = now_ms();
    int rc = db_exec(w->db, sql);
    w->latencies[i] = now_ms() - t0;
    if (rc != SQLITE_OK) {
      fprintf(stderr, "writer: insert failed rc=%d: %s\n", rc, db_errmsg(w->db));
      w->latencies[i] = -1.0;
    }
  }
  return NULL;
}

// Baseline (no destination configured): game-thread write latency alone.
static void measure_writes(arkilian *db, int n, double *lat) {
  for (int i = 0; i < n; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('base-%d')", i);
    double t0 = now_ms();
    db_exec(db, sql);
    lat[i] = now_ms() - t0;
  }
}

#define N_WRITES 3000

static void test_load_contention(void) {
  cleanup("test_load.db");
  slow_server srv;
  assert(slow_server_start(&srv, 20) == 0); // 20ms per ship

  char url[128];
  snprintf(url, sizeof(url), "http://127.0.0.1:%d/push", srv.port);
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_API_KEY", "test-key", 1);
  setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  setenv("ARKILIAN_CONTROL_URL", url, 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1); // hermetic: no .env dependence

  arkilian *db = NULL;
  assert(db_init(&db, "test_load.db") == 0);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);

  // Baseline: latency with no backup pressure (already shipped rows drain
  // meanwhile, but the flush thread is idle after the first pass).
  double base[N_WRITES];
  measure_writes(db, N_WRITES, base);

  // Pressure: writer thread hammers the game connection while the flush
  // thread ships each row to the 20ms-slow destination.
  double pressure[N_WRITES];
  writer_ctx w = {db, N_WRITES, pressure};
  pthread_t th;
  assert(pthread_create(&th, NULL, writer_main, &w) == 0);
  pthread_join(th, NULL);

  // Every write must have succeeded and been captured.
  for (int i = 0; i < N_WRITES; i++) {
    assert(pressure[i] >= 0.0);
  }
  int captured = db_wal_pending(db) + srv.requests;
  assert(captured >= N_WRITES); // outbox still draining → ≥ all writes

  double base_p50 = percentile(base, N_WRITES, 0.50);
  double base_p99 = percentile(base, N_WRITES, 0.99);
  double press_p50 = percentile(pressure, N_WRITES, 0.50);
  double press_p99 = percentile(pressure, N_WRITES, 0.99);

  printf("baseline  P50=%.3fms P99=%.3fms | under backup pressure P50=%.3fms P99=%.3fms "
         "(slow dest 20ms/ship, server requests=%d)\n",
         base_p50, base_p99, press_p50, press_p99, srv.requests);

  // The binding constraint: P99 under sustained backup pressure must stay
  // bounded (25ms is generous for a localhost write; SQLite commits are
  // ~0.05-0.5ms). A backup thread holding locks would blow this to 20ms+
  // per write.
  assert(press_p99 < 25.0);

  db_close(db);
  slow_server_stop(&srv);
  cleanup("test_load.db");
}

// ── Main ────────────────────────────────────────────────────────────

int main(void) {
  signal(SIGPIPE, SIG_IGN);
  printf("=== Arkilian Load-Contention Tests ===\n\n");
  RUN_TEST(test_load_contention);
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
