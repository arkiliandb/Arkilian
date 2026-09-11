/* tests/ark_stub_s3.h — in-process 1:1 S3 / MinIO mock server for tests.
 *
 * Implements the AWS S3 / MinIO REST API protocol:
 *   - SigV4 presigned PUT/GET/HEAD/DELETE handling
 *   - "Expect: 100-continue" protocol handling
 *   - Proper HTTP status codes (200 OK, 204 No Content, 404 NoSuchKey XML, 403, 503)
 *   - Zero stdout leakage (Content-Length: 0 on PUT/DELETE)
 *   - In-memory key/value object storage with thread-safe access
 *   - Fault injection: status override (500, 503, etc.), latency delay, connection drop
 *   - Request and method atomic counters (PUT, GET, HEAD, DELETE, TOTAL)
 *   - POSIX sockets on loopback with ephemeral OS-assigned port
 */
#ifndef ARK_STUB_S3_H
#define ARK_STUB_S3_H

#ifdef _WIN32
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ark_test_env.h"

// Raw BSD socket mock server is disabled on Windows (MinGW/MSVC).
// Provide portable stubs so translation units including ark_stub_s3.h compile cleanly.
static char g_endpoint[64] = "http://127.0.0.1:1";
static const char *BUCKET = "test-bucket";
static const char *PREFIX = "user-42-appdb";

static inline void stub_start(void) {}
static inline void stub_stop(void) {}
static inline void stub_reset(void) {}
static inline void stub_set_status_override(int sc) { (void)sc; }
static inline void stub_set_status_override_put(int sc) { (void)sc; }
static inline void stub_set_status_override_get(int sc) { (void)sc; }
static inline void stub_set_status_override_manifest(int sc) { (void)sc; }
static inline void stub_set_status_override_manifest_sig(int sc) { (void)sc; }
static inline void stub_set_delay_ms(int ms) { (void)ms; }
static inline void stub_set_drop_connection(int d) { (void)d; }
static inline void stub_set_require_presign(int r) { (void)r; }
static inline int stub_manifest_contains(const char *needle, int timeout_s) {
  (void)needle; (void)timeout_s; return 0;
}
static inline int stub_contains(const char *needle) { (void)needle; return 0; }
static inline int stub_object_count(void) { return 0; }
static inline void stub_put(const char *key, const char *data, size_t len) {
  (void)key; (void)data; (void)len;
}
static inline int stub_get(const char *key, char **out, size_t *out_len) {
  (void)key; (void)out; (void)out_len; return 0;
}
static inline int stub_delete(const char *key) { (void)key; return 0; }

static inline void set_s3_env(void) {
  ark_setenv("ARKILIAN_S3_ENDPOINT", g_endpoint, 1);
  ark_setenv("ARKILIAN_S3_BUCKET", BUCKET, 1);
  ark_setenv("ARKILIAN_S3_REGION", "us-east-1", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", PREFIX, 1);
  ark_setenv("ARKILIAN_MANIFEST_HMAC_KEY", "test-hmac-key-for-unit-tests-32b", 1);
}

static inline void clear_s3_env(void) {
  ark_unsetenv("ARKILIAN_S3_ENDPOINT");
  ark_unsetenv("ARKILIAN_S3_BUCKET");
  ark_unsetenv("ARKILIAN_S3_REGION");
  ark_unsetenv("ARKILIAN_S3_ACCESS_KEY");
  ark_unsetenv("ARKILIAN_S3_SECRET_KEY");
  ark_unsetenv("ARKILIAN_S3_PREFIX");
  ark_unsetenv("ARKILIAN_MANIFEST_HMAC_KEY");
}
#else

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

#include "ark_test_env.h"
#include "sha256.h"

#define STUB_MAX_OBJECTS 1024

typedef struct {
  char   key[1024];
  char  *data;
  size_t len;
} stub_object;

static stub_object g_stub_objects[STUB_MAX_OBJECTS];
static int         g_stub_object_count = 0;
static pthread_mutex_t g_stub_store_mutex = PTHREAD_MUTEX_INITIALIZER;
static atomic_int  g_stub_port = 0;
static atomic_int  g_stub_server_up = 0;
static pthread_t   g_stub_thread;

static atomic_int g_stub_put_count = 0;
static atomic_int g_stub_get_count = 0;
static atomic_int g_stub_head_count = 0;
static atomic_int g_stub_delete_count = 0;
static atomic_int g_stub_requests_total = 0;

static atomic_int g_stub_status_override = 0; // e.g. 503 SlowDown, 500 InternalError (all methods)
static atomic_int g_stub_status_override_put = 0; // overrides PUT only
static atomic_int g_stub_status_override_get = 0; // overrides GET only
static atomic_int g_stub_status_override_manifest = 0; // overrides PUT of manifest.json / manifest.sig
static atomic_int g_stub_status_override_manifest_sig = 0; // overrides PUT of manifest.sig ONLY
static atomic_int g_stub_delay_ms = 0;
static atomic_int g_stub_drop_connection = 0;
static atomic_int g_stub_require_presign = 0;

static inline void stub_put(const char *key, const char *data, size_t len) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) {
    if (strcmp(g_stub_objects[i].key, key) == 0) {
      free(g_stub_objects[i].data);
      g_stub_objects[i].data = malloc(len ? len : 1);
      if (len && data) memcpy(g_stub_objects[i].data, data, len);
      g_stub_objects[i].len = len;
      pthread_mutex_unlock(&g_stub_store_mutex);
      return;
    }
  }
  assert(g_stub_object_count < STUB_MAX_OBJECTS);
  stub_object *o = &g_stub_objects[g_stub_object_count++];
  snprintf(o->key, sizeof(o->key), "%s", key);
  o->data = malloc(len ? len : 1);
  if (len && data) memcpy(o->data, data, len);
  o->len = len;
  pthread_mutex_unlock(&g_stub_store_mutex);
}

static inline int stub_get(const char *key, char **out, size_t *out_len) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) {
    if (strcmp(g_stub_objects[i].key, key) == 0) {
      *out = malloc(g_stub_objects[i].len + 1);
      if (!*out) { pthread_mutex_unlock(&g_stub_store_mutex); return 0; }
      if (g_stub_objects[i].len) {
        memcpy(*out, g_stub_objects[i].data, g_stub_objects[i].len);
      }
      (*out)[g_stub_objects[i].len] = '\0';
      *out_len = g_stub_objects[i].len;
      pthread_mutex_unlock(&g_stub_store_mutex);
      return 1;
    }
  }
  pthread_mutex_unlock(&g_stub_store_mutex);
  return 0;
}

static inline int stub_delete(const char *key) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) {
    if (strcmp(g_stub_objects[i].key, key) == 0) {
      free(g_stub_objects[i].data);
      for (int j = i; j < g_stub_object_count - 1; j++) {
        g_stub_objects[j] = g_stub_objects[j + 1];
      }
      g_stub_object_count--;
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

static inline int stub_object_count(void) {
  pthread_mutex_lock(&g_stub_store_mutex);
  int count = g_stub_object_count;
  pthread_mutex_unlock(&g_stub_store_mutex);
  return count;
}

static inline void stub_reset(void) {
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++) free(g_stub_objects[i].data);
  g_stub_object_count = 0;
  pthread_mutex_unlock(&g_stub_store_mutex);
  atomic_store(&g_stub_put_count, 0);
  atomic_store(&g_stub_get_count, 0);
  atomic_store(&g_stub_head_count, 0);
  atomic_store(&g_stub_delete_count, 0);
  atomic_store(&g_stub_requests_total, 0);
  atomic_store(&g_stub_status_override, 0);
  atomic_store(&g_stub_status_override_put, 0);
  atomic_store(&g_stub_status_override_get, 0);
  atomic_store(&g_stub_status_override_manifest, 0);
  atomic_store(&g_stub_status_override_manifest_sig, 0);
  atomic_store(&g_stub_delay_ms, 0);
  atomic_store(&g_stub_drop_connection, 0);
  atomic_store(&g_stub_require_presign, 0);
}

static inline void stub_set_status_override(int status_code) {
  atomic_store(&g_stub_status_override, status_code);
}

static inline void stub_set_status_override_put(int status_code) {
  atomic_store(&g_stub_status_override_put, status_code);
}

static inline void stub_set_status_override_get(int status_code) {
  atomic_store(&g_stub_status_override_get, status_code);
}

static inline void stub_set_status_override_manifest(int status_code) {
  atomic_store(&g_stub_status_override_manifest, status_code);
}

static inline void stub_set_status_override_manifest_sig(int status_code) {
  atomic_store(&g_stub_status_override_manifest_sig, status_code);
}

static inline void stub_set_delay_ms(int ms) {
  atomic_store(&g_stub_delay_ms, ms);
}

static inline void stub_set_drop_connection(int drop) {
  atomic_store(&g_stub_drop_connection, drop);
}

static inline void stub_set_require_presign(int req) {
  atomic_store(&g_stub_require_presign, req);
}

static inline int stub_validate_presigned_url(const char *path_with_query) {
  if (!path_with_query) return 0;
  const char *q = strchr(path_with_query, '?');
  if (!q) return !atomic_load(&g_stub_require_presign);
  if (!strstr(q, "X-Amz-Algorithm=AWS4-HMAC-SHA256")) return 0;
  if (!strstr(q, "X-Amz-Credential=")) return 0;
  if (!strstr(q, "X-Amz-Date=")) return 0;
  if (!strstr(q, "X-Amz-Expires=")) return 0;
  if (!strstr(q, "X-Amz-SignedHeaders=host")) return 0;
  if (!strstr(q, "X-Amz-Signature=")) return 0;
  const char *sig = strstr(q, "X-Amz-Signature=");
  if (sig) {
    sig += strlen("X-Amz-Signature=");
    size_t hex = 0;
    while (hex < 64 && sig[hex] &&
           ((sig[hex] >= '0' && sig[hex] <= '9') ||
            (sig[hex] >= 'a' && sig[hex] <= 'f') ||
            (sig[hex] >= 'A' && sig[hex] <= 'F'))) hex++;
    if (hex != 64) return 0;
  }
  return 1;
}

static char g_endpoint[64];
static const char *BUCKET = "test-bucket";
static const char *PREFIX = "user-42-appdb";

static inline void stub_handle(int fd) {
  atomic_fetch_add(&g_stub_requests_total, 1);

  if (atomic_load(&g_stub_drop_connection)) {
    close(fd);
    return;
  }

  int delay = atomic_load(&g_stub_delay_ms);
  if (delay > 0) usleep(delay * 1000);

  char header[8192];
  size_t got = 0;
  while (got < sizeof(header) - 1) {
    char c;
    if (recv(fd, &c, 1, 0) != 1) break;
    header[got++] = c;
    if (got >= 4 && memcmp(header + got - 4, "\r\n\r\n", 4) == 0) break;
  }
  header[got] = '\0';

  char method[8], path_presigned[2048];
  if (sscanf(header, "%7s %2047s", method, path_presigned) != 2) { close(fd); return; }
  if (strcasestr(header, "expect: 100-continue")) {
    send(fd, "HTTP/1.1 100 Continue\r\n\r\n", 25, 0);
  }

  int presigned_ok = stub_validate_presigned_url(path_presigned);

  char path[1024];
  strncpy(path, path_presigned, sizeof(path) - 1);
  path[sizeof(path) - 1] = '\0';
  char *q = strchr(path, '?');
  if (q) *q = '\0';

  const char *key = path;
  while (*key == '/') key++;
  size_t blen = strlen(BUCKET);
  if (strncmp(key, BUCKET, blen) == 0 && key[blen] == '/') {
    key += blen + 1;
  } else if (strncmp(key, "push/", 5) == 0) {
    key += 5;
  }

  char *cl = strcasestr(header, "content-length:");
  size_t body_len = cl ? (size_t)atoi(cl + 15) : 0;
  char *body = malloc(body_len ? body_len : 1);
  size_t have = 0;
  while (have < body_len) {
    ssize_t n = recv(fd, body + have, body_len - have, 0);
    if (n <= 0) break;
    have += (size_t)n;
  }

  // Check fault injection / status override
  int override_status = atomic_load(&g_stub_status_override);
  if (override_status <= 0) {
    if (atomic_load(&g_stub_status_override_manifest_sig) > 0 &&
        strstr(key, "manifest.sig") != NULL) {
      override_status = atomic_load(&g_stub_status_override_manifest_sig);
    } else if (atomic_load(&g_stub_status_override_manifest) > 0 &&
        (strstr(key, "manifest.json") != NULL || strstr(key, "manifest.sig") != NULL)) {
      override_status = atomic_load(&g_stub_status_override_manifest);
    } else if (strcmp(method, "PUT") == 0) {
      override_status = atomic_load(&g_stub_status_override_put);
    } else if (strcmp(method, "GET") == 0) {
      override_status = atomic_load(&g_stub_status_override_get);
    }
  }
  if (override_status > 0) {
    const char *code_str = override_status == 503 ? "SlowDown" :
                           override_status == 500 ? "InternalError" :
                           override_status == 403 ? "AccessDenied" : "Error";
    const char *msg_str = override_status == 503 ? "Please reduce your request rate." :
                          override_status == 500 ? "We encountered an internal error." :
                          override_status == 403 ? "Access Denied." : "An error occurred.";
    char err_body[2048];
    snprintf(err_body, sizeof(err_body),
             "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
             "<Error><Code>%s</Code><Message>%s</Message><Key>%.512s</Key></Error>\n",
             code_str, msg_str, key);
    char resp[4096];
    snprintf(resp, sizeof(resp),
             "HTTP/1.1 %d %s\r\n"
             "Content-Type: application/xml\r\n"
             "Content-Length: %zu\r\n"
             "Server: AmazonS3\r\n"
             "Connection: close\r\n\r\n%s",
             override_status,
             override_status == 503 ? "Service Unavailable" :
             override_status == 500 ? "Internal Server Error" : "Error",
             strlen(err_body), err_body);
    send(fd, resp, strlen(resp), 0);
    free(body);
    close(fd);
    return;
  }

  if (strcmp(method, "PUT") == 0) {
    if (!presigned_ok) {
      const char *resp =
          "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\n"
          "Content-Length: 0\r\nServer: AmazonS3\r\nConnection: close\r\n\r\n";
      send(fd, resp, strlen(resp), 0);
    } else {
      atomic_fetch_add(&g_stub_put_count, 1);
      stub_put(key, body, have);
      char etag[65] = "00000000000000000000000000000000";
      ark_sha256_hex(body, have, etag);
      char resp[512];
      snprintf(resp, sizeof(resp),
               "HTTP/1.1 200 OK\r\n"
               "Content-Length: 0\r\n"
               "ETag: \"%s\"\r\n"
               "Server: AmazonS3\r\n"
               "x-amz-request-id: 1234567890ABCDEF\r\n"
               "Connection: close\r\n\r\n",
               etag);
      send(fd, resp, strlen(resp), 0);
    }
  } else if (strcmp(method, "GET") == 0) {
    atomic_fetch_add(&g_stub_get_count, 1);
    char *data = NULL;
    size_t len = 0;
    if (stub_get(key, &data, &len)) {
      char head[256];
      snprintf(head, sizeof(head),
               "HTTP/1.1 200 OK\r\n"
               "Content-Type: application/octet-stream\r\n"
               "Content-Length: %zu\r\n"
               "Server: AmazonS3\r\n"
               "Connection: close\r\n\r\n",
               len);
      send(fd, head, strlen(head), 0);
      if (len) send(fd, data, len, 0);
      free(data);
    } else {
      const char *err_xml =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>\n";
      char resp[512];
      snprintf(resp, sizeof(resp),
               "HTTP/1.1 404 Not Found\r\n"
               "Content-Type: application/xml\r\n"
               "Content-Length: %zu\r\n"
               "Server: AmazonS3\r\n"
               "Connection: close\r\n\r\n%s",
               strlen(err_xml), err_xml);
      send(fd, resp, strlen(resp), 0);
    }
  } else if (strcmp(method, "HEAD") == 0) {
    atomic_fetch_add(&g_stub_head_count, 1);
    char *data = NULL;
    size_t len = 0;
    if (stub_get(key, &data, &len)) {
      char head[256];
      snprintf(head, sizeof(head),
               "HTTP/1.1 200 OK\r\n"
               "Content-Length: %zu\r\n"
               "Server: AmazonS3\r\n"
               "Connection: close\r\n\r\n",
               len);
      send(fd, head, strlen(head), 0);
      free(data);
    } else {
      const char *resp =
          "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n"
          "Server: AmazonS3\r\nConnection: close\r\n\r\n";
      send(fd, resp, strlen(resp), 0);
    }
  } else if (strcmp(method, "DELETE") == 0) {
    atomic_fetch_add(&g_stub_delete_count, 1);
    stub_delete(key);
    const char *resp =
        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n"
        "Server: AmazonS3\r\nConnection: close\r\n\r\n";
    send(fd, resp, strlen(resp), 0);
  } else {
    const char *resp =
        "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n"
        "Server: AmazonS3\r\nConnection: close\r\n\r\n";
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
  assert(listen(srv, 64) == 0);
  atomic_store(&g_stub_server_up, 1);
  while (atomic_load(&g_stub_server_up)) {
    int fd = accept(srv, NULL, NULL);
    if (fd < 0) break;
    stub_handle(fd);
  }
  close(srv);
  return NULL;
}

static inline void stub_start(void) {
  if (atomic_load(&g_stub_server_up)) return;
  signal(SIGPIPE, SIG_IGN);
  pthread_create(&g_stub_thread, NULL, stub_server_thread, NULL);
  while (!atomic_load(&g_stub_server_up)) usleep(1000);
  snprintf(g_endpoint, sizeof(g_endpoint), "http://127.0.0.1:%d",
           atomic_load(&g_stub_port));
}

static inline void stub_stop(void) {
  if (!atomic_load(&g_stub_server_up)) return;
  atomic_store(&g_stub_server_up, 0);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd >= 0) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons((unsigned short)atomic_load(&g_stub_port));
    connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    close(fd);
  }
  pthread_join(g_stub_thread, NULL);
  stub_reset();
}

static inline void set_s3_env(void) {
  ark_setenv("ARKILIAN_S3_ENDPOINT", g_endpoint, 1);
  ark_setenv("ARKILIAN_S3_BUCKET", BUCKET, 1);
  ark_setenv("ARKILIAN_S3_REGION", "us-east-1", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "test-access", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "test-secret", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", PREFIX, 1);
  // HMAC is required; use a deterministic test key for the stub.
  ark_setenv("ARKILIAN_MANIFEST_HMAC_KEY", "test-hmac-key-for-unit-tests-32b", 1);
}

static inline void clear_s3_env(void) {
  ark_unsetenv("ARKILIAN_S3_ENDPOINT");
  ark_unsetenv("ARKILIAN_S3_BUCKET");
  ark_unsetenv("ARKILIAN_S3_REGION");
  ark_unsetenv("ARKILIAN_S3_ACCESS_KEY");
  ark_unsetenv("ARKILIAN_S3_SECRET_KEY");
  ark_unsetenv("ARKILIAN_S3_PREFIX");
  ark_unsetenv("ARKILIAN_MANIFEST_HMAC_KEY");
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

#endif /* !_WIN32 */

#endif /* ARK_STUB_S3_H */
