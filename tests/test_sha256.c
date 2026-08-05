// SHA-256 NIST FIPS 180-4 test vectors — pins the dependency-free
// implementation in src/sha256.c so a future change cannot silently
// break the content-authentication guarantee that hydration relies on.
//
// Vectors from NIST FIPS 180-4 / RFC 6234:
//   ""           -> e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
//   "abc"        -> ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
//   "abcdbc..."  -> 248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1
//   1M 'a'       -> cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0

#include "sha256.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0, tests_passed = 0;
#define RUN_TEST(fn) do { \
  tests_run++; \
  printf("  [%02d] %-50s ", tests_run, #fn); \
  fn(); \
  tests_passed++; \
  printf("PASS\n"); \
} while (0)

static void check(const char *input, const char *expected) {
  char out[65];
  ark_sha256_hex(input, strlen(input), out);
  if (strcmp(out, expected) != 0) {
    fprintf(stderr, "FAIL: input=\"%s\"\n  expected: %s\n  got:      %s\n",
            input, expected, out);
    abort();
  }
}

static void test_empty(void) {
  check("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

static void test_abc(void) {
  check("abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

static void test_abcdbcdecdef(void) {
  check("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

static void test_one_million_a(void) {
  char *mil = malloc(1000000);
  assert(mil);
  memset(mil, 'a', 1000000);
  char out[65];
  ark_sha256_hex(mil, 1000000, out);
  free(mil);
  if (strcmp(out, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0") != 0) {
    fprintf(stderr, "FAIL: 1M 'a'\n  expected: cdc76e5c...\n  got:      %s\n", out);
    abort();
  }
}

// File-based digest: the snapshot hydration path streams the downloaded
// file through ark_sha256_hex_file. Verify it matches the in-memory
// digest of the same bytes.
static void test_file_matches_memory(void) {
#ifdef _WIN32
  const char *path = "ark_sha256_test.bin";
#else
  const char *path = "/tmp/ark_sha256_test.bin";
#endif
  const char *body = "the quick brown fox jumps over the lazy dog";
  FILE *f = fopen(path, "wb");
  assert(f);
  assert(fwrite(body, 1, strlen(body), f) == strlen(body));
  fclose(f);

  char mem[65], file_d[65];
  ark_sha256_hex(body, strlen(body), mem);
  assert(ark_sha256_hex_file(path, file_d) == 0);
  if (strcmp(mem, file_d) != 0) {
    fprintf(stderr, "FAIL: file digest != memory digest\n  mem:  %s\n  file: %s\n",
            mem, file_d);
    remove(path);
    abort();
  }
  remove(path);
}

int main(void) {
  printf("=== SHA-256 NIST FIPS 180-4 Vectors ===\n\n");
  RUN_TEST(test_empty);
  RUN_TEST(test_abc);
  RUN_TEST(test_abcdbcdecdef);
  RUN_TEST(test_one_million_a);
  printf("\n[File Digest]\n");
  RUN_TEST(test_file_matches_memory);
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
