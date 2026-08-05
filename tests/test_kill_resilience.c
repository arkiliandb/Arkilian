// Arkilian Kill / Crash-Resilience Tests (spec §8.2, §8.3, §10.4)
//
// Verifies the crash-safety guarantees by SIGKILL-ing a live process at
// three distinct points and checking the invariants from a fresh process:
//
//   1. Mid-transaction write  — an uncommitted transaction must vanish
//      (WAL rollback), every committed row must exist in the table AND
//      be captured in _pending_backup (atomicity of trigger capture),
//      and the database must pass PRAGMA integrity_check.
//
//   2. Mid-drain             — rows are only ever deleted from the
//      outbox AFTER the destination acked them; anything still pending
//      after the kill must be re-delivered. Zero rows may be lost.
//
//   3. Mid-ship              — same invariants while a network ship is
//      in flight to a slow destination (at-least-once redelivery).
//
// The destination is a mock HTTP server that records every
// X-Arkilian-Payload-Id it receives, so the parent can prove that the
// union of all deliveries equals the exact set of captured rows.
//
// Compile (macOS/Linux):
//   cc tests/test_kill_resilience.c src/class.c src/deps/sqlite/sqlite3.c -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_kill_resilience

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifdef __APPLE__
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#include "class.h"
#include <assert.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

static int tests_run = 0;
static int tests_passed = 0;
static const char *g_self = "./test_kill_resilience";

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

// ── Recording mock destination ──────────────────────────────────────
// Answers 200 (optionally after a delay) and records every payload id
// from the X-Arkilian-Payload-Id header.

typedef struct {
  int listen_fd;
  int port;
  int delay_ms;
  pthread_t thread;
  volatile int stop;
  volatile int requests;
  int count;
  sqlite3_int64 ids[200000];
} rec_server;

static void *rec_server_run(void *arg) {
  rec_server *s = (rec_server *)arg;
  for (;;) {
    int fd = accept(s->listen_fd, NULL, NULL);
    if (fd < 0) break;
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
    char buf[16384];
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (s->stop) { close(fd); break; } // stop-kick connection: exit now
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
      char *pid = strstr(buf, "X-Arkilian-Payload-Id:");
      if (pid) {
        long long v = atoll(pid + strlen("X-Arkilian-Payload-Id:"));
        if (s->count < (int)(sizeof(s->ids) / sizeof(s->ids[0]))) {
          s->ids[s->count++] = (sqlite3_int64)v;
        }
      }
      s->requests++;
      if (s->delay_ms > 0) usleep((useconds_t)s->delay_ms * 1000);
      const char *resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
      send(fd, resp, strlen(resp), 0);
    }
    close(fd);
  }
  return NULL;
}

static int rec_server_start(rec_server *s, int delay_ms) {
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
  if (pthread_create(&s->thread, NULL, rec_server_run, s) != 0) return -1;
  return 0;
}

static void rec_server_stop(rec_server *s) {
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

// Sorted unique set view of a server's received ids.
typedef struct {
  sqlite3_int64 *v;
  int n;
} id_set;

static int cmp_i64(const void *a, const void *b) {
  sqlite3_int64 x = *(const sqlite3_int64 *)a, y = *(const sqlite3_int64 *)b;
  return (x > y) - (x < y);
}

static id_set make_set(rec_server *s) {
  id_set set;
  set.n = s->count;
  set.v = malloc((size_t)(set.n > 0 ? set.n : 1) * sizeof(sqlite3_int64));
  if (set.n > 0) memcpy(set.v, s->ids, (size_t)set.n * sizeof(sqlite3_int64));
  qsort(set.v, (size_t)set.n, sizeof(sqlite3_int64), cmp_i64);
  int w = 0;
  for (int i = 0; i < set.n; i++) {
    if (w == 0 || set.v[w - 1] != set.v[i]) set.v[w++] = set.v[i];
  }
  set.n = w;
  return set;
}

static int set_has(id_set *set, sqlite3_int64 id) {
  for (int i = 0; i < set->n; i++)
    if (set->v[i] == id) return 1;
  return 0;
}

// ── Read-only DB helpers (fresh connections, no arkilian) ───────────

static sqlite3 *open_ro(const char *path) {
  sqlite3 *h = NULL;
  sqlite3_open_v2(path, &h, SQLITE_OPEN_READONLY, NULL);
  return h;
}

static int integrity_ok(const char *path) {
  sqlite3 *h = open_ro(path);
  if (!h) return 0;
  sqlite3_stmt *st = NULL;
  int ok = 0;
  if (sqlite3_prepare_v2(h, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK &&
      sqlite3_step(st) == SQLITE_ROW) {
    const char *r = (const char *)sqlite3_column_text(st, 0);
    ok = r && strcmp(r, "ok") == 0;
  }
  sqlite3_finalize(st);
  sqlite3_close(h);
  return ok;
}

static long long count_rows(const char *path, const char *table) {
  sqlite3 *h = open_ro(path);
  if (!h) return -1;
  char sql[256];
  snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", table);
  sqlite3_stmt *st = NULL;
  long long n = -1;
  if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
    n = sqlite3_column_int64(st, 0);
  }
  sqlite3_finalize(st);
  sqlite3_close(h);
  return n;
}

// Read pending outbox ids (unsorted) into out; returns count.
static int pending_ids(const char *path, sqlite3_int64 *out, int cap) {
  sqlite3 *h = open_ro(path);
  if (!h) return -1;
  sqlite3_stmt *st = NULL;
  int n = 0;
  if (sqlite3_prepare_v2(h, "SELECT id FROM _pending_backup ORDER BY id", -1, &st, NULL) == SQLITE_OK) {
    while (sqlite3_step(st) == SQLITE_ROW && n < cap) {
      out[n++] = sqlite3_column_int64(st, 0);
    }
  }
  sqlite3_finalize(st);
  sqlite3_close(h);
  return n;
}

static int wait_pending_zero(arkilian *db, int timeout_ms) {
  int waited = 0;
  while (db_wal_pending(db) > 0 && waited < timeout_ms) {
    usleep(100 * 1000);
    waited += 100;
  }
  return db_wal_pending(db) == 0 ? 0 : -1;
}

// ── Crash scenarios ─────────────────────────────────────────────────
//
// The child process (spawned via posix_spawn — fork() in a threaded
// process is unsafe on macOS with libcurl's ObjC runtime) writes
// committed rows, reaches a kill point, then blocks in pause(). The
// parent SIGKILLs it and verifies from a fresh process.
// kill_mode selects where the child stops:
//   0 = inside an open transaction (mid-write, uncommitted)
//   1 = mid-drain (destination has acked ≥3 rows)
//   2 = mid-ship (slow destination, ≥1 ack while more ships in flight)

#define CHILD_WRITES 200
#define CHILD_PIPE_FD 200

// Child entrypoint: --child <mode> <db_path> <server_port> <pipe_fd>.
// Ships to the destination server owned by the parent (so its delivery
// log survives the child's death).
static int child_entrypoint(int argc, char **argv) {
  if (argc < 6) return 3;
  int kill_mode = atoi(argv[2]);
  const char *db_path = argv[3];
  int port = atoi(argv[4]);
  int pipe_fd = atoi(argv[5]);

  char url[128];
  snprintf(url, sizeof(url), "http://127.0.0.1:%d/push", port);

  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_API_KEY", "test-key", 1);
  setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  setenv("ARKILIAN_CONTROL_URL", url, 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);

  arkilian *db = NULL;
  if (db_init(&db, db_path) != 0) return 2;
  if (db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") != SQLITE_OK) return 2;

  // Committed rows.
  for (int i = 0; i < CHILD_WRITES; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('committed-%d')", i);
    if (db_exec(db, sql) != SQLITE_OK) return 2;
  }

  if (kill_mode == 0) {
    // Open a transaction, write uncommitted rows, then stop.
    if (db_begin(db) != SQLITE_OK) return 2;
    for (int i = 0; i < 50; i++) {
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('uncommitted-%d')", i);
      if (db_exec(db, sql) != SQLITE_OK) return 2;
    }
    if (write(pipe_fd, "intxn\n", 6) != 6) return 2;
  } else {
    // Wait until the destination has acked enough rows, then stop.
    int waited = 0;
    while (waited < 15000) {
      usleep(100 * 1000);
      waited += 100;
      long long pending = count_rows(db_path, "_pending_backup");
      long long threshold = (kill_mode == 1) ? CHILD_WRITES - 3 : CHILD_WRITES;
      if (pending >= 0 && pending <= threshold) break;
    }
    if (write(pipe_fd, "shipping\n", 9) != 9) return 2;
  }

  fflush(NULL);
  pause();
  return 3; // must not get here
}

// Runs the child, SIGKILLs it at the kill point, then verifies from a
// fresh process. The recording destination lives in the PARENT so its
// delivery log spans the child's lifetime and death.
static void run_kill_scenario(int kill_mode) {
  const char *db_path = "test_kr.db";
  cleanup(db_path);

  rec_server srv;
  assert(rec_server_start(&srv, kill_mode == 2 ? 250 : 0) == 0); // slow for mid-ship

  int pipefd[2];
  assert(pipe(pipefd) == 0);

  // posix_spawn: the child is a clean exec — no fork-in-threaded-process
  // hazards (macOS ObjC runtime + libcurl).
  extern char **environ;
  char mode_s[8], port_s[16], fd_s[8];
  snprintf(mode_s, sizeof(mode_s), "%d", kill_mode);
  snprintf(port_s, sizeof(port_s), "%d", srv.port);
  snprintf(fd_s, sizeof(fd_s), "%d", CHILD_PIPE_FD);
  char *child_argv[] = {
      (char *)g_self,
      (char *)"--child",
      mode_s,
      (char *)db_path,
      port_s,
      fd_s,
      NULL,
  };

  posix_spawn_file_actions_t fa;
  posix_spawn_file_actions_init(&fa);
  posix_spawn_file_actions_adddup2(&fa, pipefd[1], CHILD_PIPE_FD);
  posix_spawn_file_actions_addclose(&fa, pipefd[0]);
  posix_spawn_file_actions_addclose(&fa, pipefd[1]);

  pid_t pid = -1;
  int sp = posix_spawn(&pid, child_argv[0], &fa, NULL, child_argv, environ);
  posix_spawn_file_actions_destroy(&fa);
  close(pipefd[1]);
  assert(sp == 0 && pid > 0);

  // Wait for the child to reach its kill point.
  char marker[16] = {0};
  ssize_t got = read(pipefd[0], marker, sizeof(marker) - 1);
  close(pipefd[0]);
  assert(got > 0);

  // Let a bit more in-flight work happen (mid-ship: the ship in flight
  // when the marker was written), then kill hard.
  if (kill_mode == 2) usleep(100 * 1000);
  kill(pid, SIGKILL);
  int status = 0;
  waitpid(pid, &status, 0);

  // ── Post-mortem verification from a fresh process ──

  assert(integrity_ok(db_path));                       // no corruption
  long long t_rows = count_rows(db_path, "t");
  assert(t_rows == CHILD_WRITES);                      // committed rows survived
  if (kill_mode == 0) {
    assert(t_rows == CHILD_WRITES);                    // txn rolled back
  }

  // Snapshot what's still pending before we drain.
  sqlite3_int64 pending_before[CHILD_WRITES + 4];
  int n_pending = pending_ids(db_path, pending_before, CHILD_WRITES + 4);
  assert(n_pending >= 0);

  // Drain the remaining outbox with a fresh instance to the same
  // recording destination (the parent's server).
  char purl[128];
  snprintf(purl, sizeof(purl), "http://127.0.0.1:%d/push", srv.port);
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_CONTROL_URL", purl, 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  arkilian *db = NULL;
  assert(db_init(&db, db_path) == 0);
  // Mid-ship drains through the 250ms-per-row destination: allow longer.
  assert(wait_pending_zero(db, kill_mode == 2 ? 90000 : 20000) == 0);
  db_close(db);

  // At-least-once zero-loss invariant: the single recording destination
  // (spanning child lifetime + parent drain) must have delivered every
  // captured row exactly ≥1 times. Id 1 is the CREATE TABLE DDL capture;
  // ids 2..N+1 are the rows.
  id_set dset = make_set(&srv);
  for (sqlite3_int64 id = 1; id <= CHILD_WRITES + 1; id++) {
    if (!set_has(&dset, id)) {
      fprintf(stderr, "FAIL: row id %lld never delivered after kill\n", (long long)id);
      assert(0);
    }
  }
  free(dset.v);

  rec_server_stop(&srv);
  cleanup(db_path);
}

static void test_kill_mid_write(void)    { run_kill_scenario(0); }
static void test_kill_mid_drain(void)    { run_kill_scenario(1); }
static void test_kill_mid_ship(void)     { run_kill_scenario(2); }

// ── Main ────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
  signal(SIGPIPE, SIG_IGN);
  setenv("ARKILIAN_API_KEY", "test-key", 1);
  setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  setenv("ARKILIAN_MAX_ATTEMPTS", "3", 1);

  if (argc >= 2 && strcmp(argv[1], "--child") == 0) {
    return child_entrypoint(argc, argv);
  }
  if (argc > 0 && argv[0] && argv[0][0] != '\0') {
    g_self = argv[0];
  }

  printf("=== Arkilian Kill-Resilience Tests ===\n\n");
  RUN_TEST(test_kill_mid_write);
  RUN_TEST(test_kill_mid_drain);
  RUN_TEST(test_kill_mid_ship);
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
