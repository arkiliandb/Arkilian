// Arkilian Hydration Engine — implementation

#include "hydration.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ── libcurl response buffer ─────────────────────────────────────────

struct curl_buf {
  uint8_t *data;
  size_t   len;
  size_t   cap;
};

static size_t curl_write_cb(void *ptr, size_t sz, size_t nmemb, void *user) {
  struct curl_buf *buf = (struct curl_buf *)user;
  size_t total = sz * nmemb;
  if (buf->len + total > buf->cap) {
    size_t new_cap = buf->cap ? buf->cap * 2 : (total > 65536 ? total : 65536);
    if (new_cap < buf->len + total) new_cap = buf->len + total;
    uint8_t *p = realloc(buf->data, new_cap);
    if (!p) return 0;
    buf->data = p;
    buf->cap  = new_cap;
  }
  memcpy(buf->data + buf->len, ptr, total);
  buf->len += total;
  return total;
}

// ── HTTP helpers ────────────────────────────────────────────────────

static CURL *make_curl(const char *url, const char *token,
                        struct curl_slist **headers_out) {
  CURL *c = curl_easy_init();
  if (!c) return NULL;
  curl_easy_setopt(c, CURLOPT_URL, url);
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, curl_write_cb);
  curl_easy_setopt(c, CURLOPT_TIMEOUT, 120L);
  curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 15L);
  curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);

  struct curl_slist *h = NULL;
  if (token && strlen(token) > 0) {
    char auth[512];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", token);
    h = curl_slist_append(h, auth);
  }
  curl_easy_setopt(c, CURLOPT_HTTPHEADER, h);
  *headers_out = h;
  return c;
}

// Download a file from URL into a malloc'd buffer.  Returns malloc'd
// data (caller frees), sets *out_len on success, or returns NULL.
static uint8_t *http_download(const char *url, const char *token,
                               size_t *out_len, int *err) {
  struct curl_buf buf = {NULL, 0, 0};
  struct curl_slist *headers = NULL;
  CURL *c = make_curl(url, token, &headers);
  if (!c) { *err = HYDRATION_ERR_NET; return NULL; }

  curl_easy_setopt(c, CURLOPT_WRITEDATA, &buf);
  CURLcode rc = curl_easy_perform(c);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);

  curl_slist_free_all(headers);
  curl_easy_cleanup(c);

  if (rc != CURLE_OK || http_code != 200) {
    free(buf.data);
    *err = HYDRATION_ERR_NET;
    return NULL;
  }
  *out_len = buf.len;
  return buf.data;
}

// ── WAL checksum (SQLite cumulative algorithm) ──────────────────────

void wal_checksum_step(uint32_t *s1, uint32_t *s2,
                       const uint32_t *data, size_t words) {
  for (size_t i = 0; i < words; i += 2) {
    *s1 += data[i] + *s2;
    *s2 += data[i + 1] + *s1;
  }
}

// ── WAL file writer ─────────────────────────────────────────────────

int wal_file_write(const char *wal_path, const WalHeader *hdr,
                   const WalFrame *frames, int frame_count) {
  FILE *f = fopen(wal_path, "wb");
  if (!f) return HYDRATION_ERR_DISK;

  // Write WAL header
  if (fwrite(hdr, sizeof(WalHeader), 1, f) != 1) {
    fclose(f); return HYDRATION_ERR_DISK;
  }

  // Write each frame: header + page data
  for (int i = 0; i < frame_count; i++) {
    if (fwrite(&frames[i].hdr, sizeof(WalFrameHeader), 1, f) != 1 ||
        fwrite(frames[i].page_data, frames[i].page_size, 1, f) != 1) {
      fclose(f); return HYDRATION_ERR_DISK;
    }
  }

  fclose(f);
  return 0;
}

// ── Frame validation ────────────────────────────────────────────────

// Validate a single WAL frame: salts must match the header, checksums
// must be correct for the given page data.  Returns 0 on success.
int validate_frame(const WalHeader *hdr, WalFrame *frame,
                          uint32_t *running_s1, uint32_t *running_s2) {
  if (frame->hdr.salt_1 != hdr->salt_1 ||
      frame->hdr.salt_2 != hdr->salt_2)
    return HYDRATION_ERR_CHECK;

  // Recompute checksum over frame header (first 8 bytes: page_no + size_after)
  uint32_t s1 = *running_s1, s2 = *running_s2;
  uint32_t hdr_words[2];
  hdr_words[0] = frame->hdr.page_no;
  hdr_words[1] = frame->hdr.size_after;
  wal_checksum_step(&s1, &s2, hdr_words, 2);

  // Continue over page data
  wal_checksum_step(&s1, &s2, (const uint32_t *)frame->page_data,
                    frame->page_size / 4);

  if (s1 != frame->hdr.checksum_1 || s2 != frame->hdr.checksum_2)
    return HYDRATION_ERR_CHECK;

  *running_s1 = s1;
  *running_s2 = s2;
  return 0;
}

// ── Phase 1: Download latest snapshot ───────────────────────────────

static int download_snapshot(const char *server_url, const char *token,
                              const char *db_path,
                              uint32_t *out_lsn,
                              hydration_progress_cb progress, void *user) {
  char url[1024];
  snprintf(url, sizeof(url), "%s/snapshot/latest", server_url);

  if (progress) progress(1, 0, user);

  size_t len = 0;
  int err = 0;
  uint8_t *data = http_download(url, token, &len, &err);
  if (!data) return err;

  if (progress) progress(1, 50, user);

  // The first 4 bytes of the response are the snapshot LSN (uint32 LE).
  // The rest is the raw SQLite database file.
  uint32_t snapshot_lsn = 0;
  if (len >= 4) {
    snapshot_lsn = (uint32_t)data[0] |
                   ((uint32_t)data[1] << 8) |
                   ((uint32_t)data[2] << 16) |
                   ((uint32_t)data[3] << 24);
  }

  // Write the .db file (skip the 4-byte LSN prefix)
  FILE *f = fopen(db_path, "wb");
  if (!f) { free(data); return HYDRATION_ERR_DISK; }

  size_t written = 0;
  if (len > 4) written = fwrite(data + 4, 1, len - 4, f);
  fclose(f);
  free(data);

  if (written != (len > 4 ? len - 4 : 0)) return HYDRATION_ERR_DISK;

  if (out_lsn) *out_lsn = snapshot_lsn;

  if (progress) progress(1, 100, user);
  return 0;
}

// ── Phase 2: Download and reconstruct WAL ───────────────────────────

static int download_wal_frames(const char *server_url, const char *token,
                                const char *db_path, uint32_t after_lsn,
                                hydration_progress_cb progress, void *user) {
  char url[1024];
  snprintf(url, sizeof(url), "%s/wal/frames?after=%u", server_url, after_lsn);

  if (progress) progress(2, 0, user);

  size_t len = 0;
  int err = 0;
  uint8_t *data = http_download(url, token, &len, &err);
  if (!data) return err;

  if (progress) progress(2, 30, user);

  // Response format: binary stream of frames.
  // Each frame: [4B page_no LE][4B size_after LE][4B salt_1 LE][4B salt_2 LE]
  //             [4B checksum_1 LE][4B checksum_2 LE][4B page_size LE]
  //             [page_size bytes of page data]
  //
  // First 32 bytes: WalHeader (magic, format, page_size, checkpoint_seq,
  //                             salt_1, salt_2, checksum_1, checksum_2)

  if (len < 32) { free(data); return HYDRATION_ERR_CHECK; }

  size_t off = 0;

  // Read WalHeader from the first 32 bytes
  WalHeader hdr;
  memcpy(&hdr, data + off, sizeof(WalHeader));
  off += sizeof(WalHeader);

  // Parse frames
  int frame_cap = 256;
  int frame_cnt = 0;
  WalFrame *frames = malloc((size_t)frame_cap * sizeof(WalFrame));
  if (!frames) { free(data); return HYDRATION_ERR_MEM; }

  uint32_t running_s1 = hdr.checksum_1;
  uint32_t running_s2 = hdr.checksum_2;
  int salt_mismatch = 0;

  while (off + 28 <= len) {
    // Read frame header fields (6 x uint32 LE)
    uint32_t page_no    = *(uint32_t *)(data + off);      off += 4;
    uint32_t size_after = *(uint32_t *)(data + off);      off += 4;
    uint32_t salt_1     = *(uint32_t *)(data + off);      off += 4;
    uint32_t salt_2     = *(uint32_t *)(data + off);      off += 4;
    uint32_t cksum1_in  = *(uint32_t *)(data + off);      off += 4;
    uint32_t cksum2_in  = *(uint32_t *)(data + off);      off += 4;
    uint32_t page_size  = *(uint32_t *)(data + off);      off += 4;

    if (page_size == 0 || off + page_size > len) break;

    // Salt mismatch check — discard this and all subsequent frames
    if (salt_1 != hdr.salt_1 || salt_2 != hdr.salt_2) {
      salt_mismatch = 1;
      break;
    }

    // Grow frame array if needed
    if (frame_cnt >= frame_cap) {
      frame_cap *= 2;
      WalFrame *p = realloc(frames, (size_t)frame_cap * sizeof(WalFrame));
      if (!p) { free(frames); free(data); return HYDRATION_ERR_MEM; }
      frames = p;
    }

    WalFrame *f = &frames[frame_cnt];
    f->hdr.page_no    = page_no;
    f->hdr.size_after = size_after;
    f->hdr.salt_1     = salt_1;
    f->hdr.salt_2     = salt_2;
    f->hdr.checksum_1 = cksum1_in;
    f->hdr.checksum_2 = cksum2_in;
    f->page_size      = page_size;
    f->page_data      = data + off;
    off += page_size;

    // Validate checksum
    if (validate_frame(&hdr, f, &running_s1, &running_s2) != 0) {
      // Checksum mismatch — discard this frame (keep previous valid ones)
      break;
    }

    frame_cnt++;
    if (progress && frame_cnt % 100 == 0)
      progress(2, 30 + (int)((float)off / (float)len * 60.0f), user);
  }

  if (progress) progress(2, 90, user);

  // Write the .db-wal file
  char wal_path[1024];
  snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
  int wrc = wal_file_write(wal_path, &hdr, frames, frame_cnt);
  free(frames);
  free(data);

  if (wrc != 0) return wrc;

  if (progress) progress(2, 100, user);
  return salt_mismatch ? HYDRATION_ERR_CHECK : 0;
}

// ── Public API ──────────────────────────────────────────────────────

int arkilian_hydrate(const char *db_path,
                     const char *server_url,
                     const char *auth_token,
                     hydration_progress_cb progress,
                     void *user_data) {
  if (!db_path || !server_url) return HYDRATION_ERR_NET;

  // Phase 1: download base snapshot
  uint32_t snapshot_lsn = 0;
  int rc = download_snapshot(server_url, auth_token, db_path,
                              &snapshot_lsn, progress, user_data);
  if (rc != 0) return rc;

  // Phase 2: download and reconstruct WAL frames
  rc = download_wal_frames(server_url, auth_token, db_path,
                            snapshot_lsn, progress, user_data);
  // Even if there are no WAL frames (HYDRATION_ERR_NET because 404),
  // the database from Phase 1 is still valid.
  if (rc == HYDRATION_ERR_NET) rc = 0;

  return rc;
}
