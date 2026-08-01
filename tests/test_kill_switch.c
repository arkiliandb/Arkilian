// Arkilian Kill-Switch Tests (spec §1 + §10.7)
//
// Covers:
//   - ARKILIAN_ENABLE_BACKUP=0 at startup: game CRUD works with backup
//     fully disabled; capture still queues rows but nothing is shipped
//     (attempts stay 0, rows are never deleted).
//   - Runtime kill-switch (db_backup_set_enabled): with a live mock
//     destination, rows ship and drain; disabling stops ALL shipping
//     (zero requests reach the destination, queue grows, attempts stay
//     0); re-enabling resumes exactly where the queue left off.
//
// Compile (macOS/Linux):
//   cc tests/test_kill_switch.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_kill_switch

#include "class.h"
#include <assert.h>
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

// ── Mock HTTP destination ───────────────────────────────────────────
// Tiny socket server that answers every request with 200 OK and counts
// completed requests, so the test can assert *exactly* how many payloads
// the client shipped.

typedef struct {
  int listen_fd;
  int port;
  volatile int requests;
  volatile int stop;
  pthread_t thread;
} mock_server;

// Consume the request (headers + Content-Length body) so curl sees a
// clean completed transfer before we answer and close.
static void drain_request(int fd) {
  char buf[16384];
  ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
  if (n <= 0) return;
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

static void *mock_server_run(void *arg) {
  mock_server *s = (mock_server *)arg;
  for (;;) {
    struct sockaddr_in cli;
    socklen_t clen = sizeof(cli);
    int fd = accept(s->listen_fd, (struct sockaddr *)&cli, &clen);
    if (fd < 0) break;
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
    drain_request(fd);
    if (s->stop) { close(fd); break; }
    s->requests++;
    const char *resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
    ssize_t sent = send(fd, resp, strlen(resp), 0);
    (void)sent;
    close(fd);
  }
  return NULL;
}

static int mock_server_start(mock_server *s) {
  memset(s, 0, sizeof(*s));
  s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (s->listen_fd < 0) return -1;
  int one = 1;
  setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0; // OS-assigned
  if (bind(s->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) return -1;

  socklen_t alen = sizeof(addr);
  if (getsockname(s->listen_fd, (struct sockaddr *)&addr, &alen) != 0) return -1;
  s->port = ntohs(addr.sin_port);

  if (listen(s->listen_fd, 16) != 0) return -1;
  if (pthread_create(&s->thread, NULL, mock_server_run, s) != 0) return -1;
  return 0;
}

static void mock_server_stop(mock_server *s) {
  s->stop = 1;
  // Kick the accept loop with a connect so it observes stop and exits.
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

// ── Helpers ─────────────────────────────────────────────────────────

// Poll _pending_backup until empty or timeout. 0 = drained.
static int wait_queue_empty(arkilian *db, int timeout_ms) {
  int waited = 0;
  while (db_wal_pending(db) > 0 && waited < timeout_ms) {
    usleep(100 * 1000);
    waited += 100;
  }
  return db_wal_pending(db) == 0 ? 0 : -1;
}

// Sum of `attempts` across _pending_backup — 0 proves no ship was ever
// attempted (attempts only increment after a failed ship_to_backup).
static int sum_attempts(arkilian *db) {
  db_prepare(db, "SELECT COALESCE(SUM(attempts), 0) FROM _pending_backup");
  int sum = -1;
  if (db_step(db) == SQLITE_ROW) sum = db_column_int(db, 0);
  db_finalize(db);
  return sum;
}

// ── Startup: backup disabled via env ────────────────────────────────

static void test_disabled_at_startup(void) {
  cleanup("test_ks_off.db");
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  setenv("ARKILIAN_WAL_PUSH_URL", "http://127.0.0.1:1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1); // hermetic: no .env dependence
  arkilian *db = NULL;
  assert(db_init(&db, "test_ks_off.db") == 0);
  assert(db_backup_is_enabled(db) == 0);

  // Game runs correctly with the backup subsystem disabled (§10.7).
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO t (v) VALUES ('x')") == SQLITE_OK);
  db_prepare(db, "SELECT COUNT(*) FROM t");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 1);
  db_finalize(db);

  // Capture still runs (rows queued), but nothing is shipped: rows stay
  // in _pending_backup with attempts == 0.
  assert(db_wal_pending(db) >= 1);
  sleep(3); // > POLL_INTERVAL_MS (2s) — give a bogus-enabled thread time to act
  assert(db_wal_pending(db) >= 1);   // never deleted
  assert(sum_attempts(db) == 0);     // never attempted

  db_close(db);
  cleanup("test_ks_off.db");
}

// ── Runtime kill-switch: enable → disable → re-enable ───────────────

static void test_runtime_kill_switch(void) {
  cleanup("test_ks_toggle.db");
  mock_server srv;
  assert(mock_server_start(&srv) == 0);

  char url[128];
  snprintf(url, sizeof(url), "http://127.0.0.1:%d/push", srv.port);
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_WAL_PUSH_URL", url, 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1); // hermetic: no .env dependence

  arkilian *db = NULL;
  assert(db_init(&db, "test_ks_toggle.db") == 0);
  assert(db_backup_is_enabled(db) == 1);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);

  // Phase 1 — enabled: rows ship, queue drains, destination receives them.
  for (int i = 0; i < 5; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('row%d')", i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }
  assert(wait_queue_empty(db, 10000) == 0);
  int shipped_enabled = srv.requests;
  assert(shipped_enabled >= 5);

  // Let the flush thread finish its pass and fall asleep in cond-wait
  // before flipping the switch, so no drain is mid-flight across the
  // toggle (an in-flight pass completes by design).
  sleep(2); // > POLL_INTERVAL_MS (2s)

  // Phase 2 — kill-switch off: nothing ships, nothing is deleted,
  // attempts stay 0 (queue just accumulates for later replay).
  // Disable BEFORE writing so no pass can start mid-stream.
  db_backup_set_enabled(db, 0);
  assert(db_backup_is_enabled(db) == 0);
  for (int i = 0; i < 5; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('off%d')", i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }
  int baseline = srv.requests; // thread confirmed asleep: nothing in flight
  sleep(3); // > poll interval
  assert(db_wal_pending(db) == 5);             // queued, not drained
  assert(srv.requests == baseline);            // zero new requests
  assert(sum_attempts(db) == 0);               // zero ship attempts

  // Phase 3 — re-enabled: shipping resumes from the queue, no data lost.
  db_backup_set_enabled(db, 1);
  assert(db_backup_is_enabled(db) == 1);
  assert(wait_queue_empty(db, 10000) == 0);
  assert(srv.requests >= shipped_enabled + 5);

  db_close(db);
  mock_server_stop(&srv);
  cleanup("test_ks_toggle.db");
}

// ── Main ────────────────────────────────────────────────────────────

int main(void) {
  // A closed mock connection must never kill the process via SIGPIPE.
  signal(SIGPIPE, SIG_IGN);

  printf("=== Arkilian Kill-Switch Tests ===\n\n");
  RUN_TEST(test_disabled_at_startup);
  RUN_TEST(test_runtime_kill_switch);
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
