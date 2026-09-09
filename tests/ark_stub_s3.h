/* tests/ark_stub_s3.h — in-process S3 stub server for the POSIX test legs.

   Raw-socket HTTP server on an ephemeral loopback port that stores objects
   in memory keyed by path (query string stripped). Understands PUT and GET,
   answers "Expect: 100-continue" (libcurl sends it for >1 KiB uploads —
   snapshots — and stalls ~1s per upload without the immediate 100).

   Modeled on the stub embedded in test_hydration.c; shared here so the
   snapshot-watermark and manifest-protocol suites exercise the REAL
   SigV4-presign/libcurl/upload paths against the same harness. POSIX-only
   (BSD sockets) — kept out of the Windows/MinGW test set, like
   test_hydration.c. */
#ifndef ARK_STUB_S3_H
#define ARK_STUB_S3_H

#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#define STUB_MAX_OBJECTS 512

typedef struct {
  char   key[512];
  char  *data;
  size_t len;
} stub_object;

static stub_object g_stub_objects[STUB_MAX_OBJECTS];
static int         g_stub_object_count = 0;
static pthread_mutex_t g_stub_store_mutex = PTHREAD_MUTEX_INITIALIZER;
static atomic_int  g_stub_port = 0;
static atomic_int  g_stub_server_up = 0;

static inline void stub_put(const char *key, const char *data, size_t len) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) {
    if (strcmp(g_stub_objects[i].key, key) == 0) {
      free(g_stub_objects[i].data);
      g_stub_objects[i].data = malloc(len ? len : 1);
      memcpy(g_stub_objects[i].data, data, len);
      g_stub_objects[i].len = len;
      pthread_mutex_unlock(&g_stub_store_mutex);
      return;
    }
  }
  assert(g_stub_object_count < STUB_MAX_OBJECTS);
  stub_object *o = &g_stub_objects[g_stub_object_count++];
  snprintf(o->key, sizeof(o->key), "%s", key);
  o->data = malloc(len ? len : 1);
  memcpy(o->data, data, len);
  o->len = len;
  pthread_mutex_unlock(&g_stub_store_mutex);
}

static inline int stub_get(const char *key, char **out, size_t *out_len) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) {
    if (strcmp(g_stub_objects[i].key, key) == 0) {
      *out = malloc(g_stub_objects[i].len + 1);
      if (!*out) { pthread_mutex_unlock(&g_stub_store_mutex); return 0; }
      memcpy(*out, g_stub_objects[i].data, g_stub_objects[i].len);
      (*out)[g_stub_objects[i].len] = '\0';
      *out_len = g_stub_objects[i].len;
      pthread_mutex_unlock(&g_stub_store_mutex);
      return 1;
    }
  }
  pthread_mutex_unlock(&g_stub_store_mutex);
  return 0;
}

static inline int stub_contains(const char *needle) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) {
    if (strstr(g_stub_objects[i].key, needle)) {
      pthread_mutex_unlock(&g_stub_store_mutex);
      return 1;
    }
  }
  pthread_mutex_unlock(&g_stub_store_mutex);
  return 0;
}

static inline void stub_reset(void) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) free(g_stub_objects[i].data);
  g_stub_object_count = 0;
  pthread_mutex_unlock(&g_stub_store_mutex);
}

static inline void stub_handle(int fd) {
  char header[8192];
  size_t got = 0;
  while (got < sizeof(header) - 1) {
    char c;
    if (recv(fd, &c, 1, 0) != 1) break;
    header[got++] = c;
    if (got >= 4 && memcmp(header + got - 4, "\r\n\r\n", 4) == 0) break;
  }
  header[got] = '\0';

  char method[8], path[1024];
  if (sscanf(header, "%7s %1023s", method, path) != 2) { close(fd); return; }
  if (strcasestr(header, "expect: 100-continue")) {
    send(fd, "HTTP/1.1 100 Continue\r\n\r\n", 25, 0);
  }
  char *q = strchr(path, '?');
  if (q) *q = '\0';
  const char *key = strchr(path + 1, '/');
  key = key ? key + 1 : path + 1;

  char *cl = strcasestr(header, "content-length:");
  size_t body_len = cl ? (size_t)atoi(cl + 15) : 0;
  char *body = malloc(body_len ? body_len : 1);
  size_t have = 0;
  while (have < body_len) {
    ssize_t n = recv(fd, body + have, body_len - have, 0);
    if (n <= 0) break;
    have += (size_t)n;
  }

  if (strcmp(method, "PUT") == 0) {
    stub_put(key, body, have);
    const char *resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    send(fd, resp, strlen(resp), 0);
  } else if (strcmp(method, "GET") == 0) {
    char *data = NULL;
    size_t len = 0;
    if (stub_get(key, &data, &len)) {
      char head[256];
      snprintf(head, sizeof(head),
               "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\n\r\n", len);
      send(fd, head, strlen(head), 0);
      if (len) send(fd, data, len, 0);
      free(data);
    } else {
      const char *resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
      send(fd, resp, strlen(resp), 0);
    }
  } else {
    const char *resp = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
    send(fd, resp, strlen(resp), 0);
  }
  free(body);
  close(fd);
}

static inline void *stub_server_thread(void *arg) {
  (void)arg;
  int srv = socket(AF_INET, SOCK_STREAM, 0);
  assert(srv >= 0);
  int one = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  assert(bind(srv, (struct sockaddr *)&addr, sizeof(addr)) == 0);
  socklen_t alen = sizeof(addr);
  assert(getsockname(srv, (struct sockaddr *)&addr, &alen) == 0);
  atomic_store(&g_stub_port, ntohs(addr.sin_port));
  assert(listen(srv, 16) == 0);
  atomic_store(&g_stub_server_up, 1);
  while (atomic_load(&g_stub_server_up)) {
    int fd = accept(srv, NULL, NULL);
    if (fd < 0) break;
    stub_handle(fd);
  }
  close(srv);
  return NULL;
}

static char g_endpoint[64];
static const char *BUCKET = "test-bucket";
static const char *PREFIX = "user-42-appdb";

static inline void stub_start(void) {
  signal(SIGPIPE, SIG_IGN);
  pthread_t t;
  pthread_create(&t, NULL, stub_server_thread, NULL);
  while (!atomic_load(&g_stub_server_up)) usleep(1000);
  snprintf(g_endpoint, sizeof(g_endpoint), "http://127.0.0.1:%d",
           atomic_load(&g_stub_port));
}

static inline void set_s3_env(void) {
  setenv("ARKILIAN_S3_ENDPOINT", g_endpoint, 1);
  setenv("ARKILIAN_S3_BUCKET", BUCKET, 1);
  setenv("ARKILIAN_S3_REGION", "us-east-1", 1);
  setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  setenv("ARKILIAN_S3_PREFIX", PREFIX, 1);
}

static inline int stub_manifest_contains(const char *needle, int timeout_s) {
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  for (int i = 0; i < timeout_s * 20; i++) {
    char *body = NULL;
    size_t len = 0;
    if (stub_get(mkey, &body, &len)) {
      int ok = strstr(body, needle) != NULL;
      free(body);
      if (ok) return 1;
    }
    usleep(50000);
  }
  return 0;
}

#endif /* ARK_STUB_S3_H */
