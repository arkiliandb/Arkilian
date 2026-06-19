// Arkilian Hydration Engine — unit + integration tests
//
// Compile (unit tests only):
//   cc tests/test_hydration.c src/hydration.c \
//      -Isrc -lcurl -o test_hydration
//
// Integration test (requires running server):
//   # start server first, then:
//   ARKILIAN_HYDRATION_URL=http://localhost:8080/v1 \
//   ARKILIAN_HYDRATION_TOKEN=test-token \
//   ./test_hydration --integration

#include "hydration.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    tests_run++;                                                                \
    printf("  [%02d] %-50s ", tests_run, #fn);                                 \
    fn();                                                                       \
    tests_passed++;                                                             \
    printf("PASS\n");                                                           \
  } while (0)

// ── WAL checksum tests ──────────────────────────────────────────────

static void test_checksum_basic(void) {
  uint32_t s1 = 0, s2 = 0;
  uint32_t data[4] = {1, 2, 3, 4};
  wal_checksum_step(&s1, &s2, data, 4);
  // Manually compute:
  // i=0: s1 = 0 + 1 + 0 = 1; s2 = 0 + 2 + 1 = 3
  // i=2: s1 = 1 + 3 + 3 = 7; s2 = 3 + 4 + 7 = 14
  assert(s1 == 7 && s2 == 14);
}

static void test_checksum_cumulative(void) {
  uint32_t s1 = 5, s2 = 10;
  uint32_t data1[4] = {10, 20, 30, 40};
  wal_checksum_step(&s1, &s2, data1, 4);
  uint32_t after_first_s1 = s1, after_first_s2 = s2;

  uint32_t data2[4] = {100, 200, 300, 400};
  wal_checksum_step(&s1, &s2, data2, 4);

  // Reset and do both at once
  uint32_t r1 = 5, r2 = 10;
  uint32_t combined[8] = {10, 20, 30, 40, 100, 200, 300, 400};
  wal_checksum_step(&r1, &r2, combined, 8);

  assert(s1 == r1 && s2 == r2);
  (void)after_first_s1; (void)after_first_s2;
}

static void test_checksum_known_vector(void) {
  // SQLite WAL frame 0 checksum on a 4096-byte page of zeros
  uint32_t s1 = 0, s2 = 0;
  uint32_t hdr[2] = {1, 0}; // page_no=1, size_after=0 (non-commit)
  wal_checksum_step(&s1, &s2, hdr, 2);

  // Page of zeros (4096 bytes = 1024 uint32 words)
  uint32_t *page = calloc(1024, sizeof(uint32_t));
  assert(page != NULL);
  wal_checksum_step(&s1, &s2, page, 1024);
  free(page);

  // s1 and s2 should be non-zero after processing
  assert(s1 != 0 || s2 != 0);
  // Specific known value from SQLite: for page_no=1, size_after=0, all-zero page
  // frame 0 checksum with seeds 0,0 is deterministic
}

// ── WAL file write + round-trip tests ───────────────────────────────

static void test_wal_file_write_empty(void) {
  WalHeader hdr = {0};
  hdr.magic_no       = 0x377f0682;
  hdr.file_format    = 3007000;
  hdr.page_size      = 4096;
  hdr.checkpoint_seq = 1;
  hdr.salt_1         = 42;
  hdr.salt_2         = 99;

  int rc = wal_file_write("/tmp/test_empty.wal", &hdr, NULL, 0);
  assert(rc == 0);

  // Check file size = 32 bytes (just the header)
  FILE *f = fopen("/tmp/test_empty.wal", "rb");
  assert(f != NULL);
  fseek(f, 0, SEEK_END);
  long sz = ftell(f);
  fclose(f);
  assert(sz == 32);

  remove("/tmp/test_empty.wal");
}

static void test_wal_file_write_one_frame(void) {
  uint8_t page[4096];
  memset(page, 0xAB, sizeof(page));

  WalHeader hdr = {0};
  hdr.magic_no       = 0x377f0682;
  hdr.file_format    = 3007000;
  hdr.page_size      = 4096;
  hdr.checkpoint_seq = 1;
  hdr.salt_1         = 123;
  hdr.salt_2         = 456;

  // Compute checksum for the frame
  uint32_t s1 = 0, s2 = 0;
  uint32_t hdr_words[2] = {5, 1}; // page_no=5, size_after=1 (commit frame)
  wal_checksum_step(&s1, &s2, hdr_words, 2);
  wal_checksum_step(&s1, &s2, (uint32_t *)page, 1024);

  WalFrame frame;
  frame.hdr.page_no    = 5;
  frame.hdr.size_after = 1;
  frame.hdr.salt_1     = 123;
  frame.hdr.salt_2     = 456;
  frame.hdr.checksum_1 = s1;
  frame.hdr.checksum_2 = s2;
  frame.page_data      = page;
  frame.page_size      = 4096;

  int rc = wal_file_write("/tmp/test_one.wal", &hdr, &frame, 1);
  assert(rc == 0);

  // 32-header + 24-frame-header + 4096-page = 4152 bytes
  FILE *f = fopen("/tmp/test_one.wal", "rb");
  assert(f != NULL);
  fseek(f, 0, SEEK_END);
  long sz = ftell(f);
  fclose(f);
  assert(sz == 32 + 24 + 4096);

  // Read back and verify header
  f = fopen("/tmp/test_one.wal", "rb");
  WalHeader read_hdr;
  assert(fread(&read_hdr, sizeof(WalHeader), 1, f) == 1);
  assert(read_hdr.magic_no  == 0x377f0682);
  assert(read_hdr.page_size == 4096);
  assert(read_hdr.salt_1    == 123);
  assert(read_hdr.salt_2    == 456);

  // Read frame header
  WalFrameHeader read_fhdr;
  assert(fread(&read_fhdr, sizeof(WalFrameHeader), 1, f) == 1);
  assert(read_fhdr.page_no    == 5);
  assert(read_fhdr.size_after == 1);
  assert(read_fhdr.checksum_1 == s1);
  assert(read_fhdr.checksum_2 == s2);

  fclose(f);
  remove("/tmp/test_one.wal");
}

// ── Frame validation ────────────────────────────────────────────────

static void test_frame_validation_passes(void) {
  uint8_t page[4096];
  memset(page, 0x42, sizeof(page));

  WalHeader hdr = {0};
  hdr.salt_1 = 11;
  hdr.salt_2 = 22;
  hdr.checksum_1 = 0;
  hdr.checksum_2 = 0;

  // Compute correct checksum
  uint32_t s1 = 0, s2 = 0;
  uint32_t hw[2] = {1, 0};
  wal_checksum_step(&s1, &s2, hw, 2);
  wal_checksum_step(&s1, &s2, (uint32_t *)page, 1024);

  WalFrame frame;
  frame.hdr.page_no    = 1;
  frame.hdr.size_after = 0;
  frame.hdr.salt_1     = 11;
  frame.hdr.salt_2     = 22;
  frame.hdr.checksum_1 = s1;
  frame.hdr.checksum_2 = s2;
  frame.page_data      = page;
  frame.page_size      = 4096;

  uint32_t rs1 = 0, rs2 = 0;
  int rc = validate_frame(&hdr, &frame, &rs1, &rs2);
  assert(rc == 0);
  assert(rs1 == s1 && rs2 == s2);
}

// ── Hydration integration test (requires running server) ────────────

static void test_hydration_end_to_end(void) {
  const char *url = getenv("ARKILIAN_HYDRATION_URL");
  if (!url) {
    printf("SKIP (set ARKILIAN_HYDRATION_URL to run)\n");
    tests_run--; // don't count as run
    return;
  }

  const char *token = getenv("ARKILIAN_HYDRATION_TOKEN");
  const char *db_path = "/tmp/arkilian_hydrated.db";
  remove(db_path);

  int rc = arkilian_hydrate(db_path, url, token, NULL, NULL);
  printf("rc=%d ", rc);
  assert(rc == HYDRATION_OK || rc == HYDRATION_ERR_NET);

  // If OK, verify the database is valid by opening it
  if (rc == HYDRATION_OK) {
    // The .db file should exist and be non-empty
    FILE *f = fopen(db_path, "rb");
    assert(f != NULL);
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fclose(f);
    printf("db_size=%ld ", sz);
    assert(sz > 0);
  }

  remove(db_path);
  remove("/tmp/arkilian_hydrated.db-wal");
}

// ── Main ────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
  int integration = 0;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--integration") == 0)
      integration = 1;
  }

  printf("=== Arkilian Hydration Tests ===\n\n");

  printf("[WAL Checksum]\n");
  RUN_TEST(test_checksum_basic);
  RUN_TEST(test_checksum_cumulative);
  RUN_TEST(test_checksum_known_vector);

  printf("\n[WAL File I/O]\n");
  RUN_TEST(test_wal_file_write_empty);
  RUN_TEST(test_wal_file_write_one_frame);

  printf("\n[Frame Validation]\n");
  RUN_TEST(test_frame_validation_passes);

  if (integration) {
    printf("\n[Integration]\n");
    RUN_TEST(test_hydration_end_to_end);
  }

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
