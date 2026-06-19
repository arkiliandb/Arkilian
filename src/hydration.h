// Arkilian Hydration Engine — Point-in-Time Recovery / Cold Start
//
// Phase 1: Download the latest base snapshot (.db) from remote storage.
// Phase 2: Download missing WAL frames, reconstruct local .db-wal file
//          with valid SQLite binary headers and checksums.
// Phase 3: Open with sqlite3_open_v2 — SQLite auto-recovers from .db-wal.
//
// Usage:
//   arkilian_hydrate("mydb.db", "https://server/v1", "token", NULL);

#ifndef ARKILIAN_HYDRATION_H
#define ARKILIAN_HYDRATION_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ── WAL binary structures (packed, matching SQLite on-disk layout) ──

#pragma pack(push, 1)

typedef struct {
  uint32_t magic_no;       // 0x377f0682 LE or 0x377f0683 BE
  uint32_t file_format;    // 3007000
  uint32_t page_size;      // e.g. 4096
  uint32_t checkpoint_seq; // incremental checkpoint counter
  uint32_t salt_1;         // random salt, must match in all frames
  uint32_t salt_2;         // random salt, must match in all frames
  uint32_t checksum_1;     // cumulative frame-0 checksum seed (normally 0)
  uint32_t checksum_2;     // cumulative frame-0 checksum seed (normally 0)
} WalHeader;

typedef struct {
  uint32_t page_no;        // target B-Tree page number
  uint32_t size_after;     // DB size in pages (0 = non-commit frame, >0 = commit)
  uint32_t salt_1;         // must match WalHeader.salt_1
  uint32_t salt_2;         // must match WalHeader.salt_2
  uint32_t checksum_1;     // cumulative frame checksum
  uint32_t checksum_2;     // cumulative frame checksum
} WalFrameHeader;

#pragma pack(pop)

// A single WAL frame ready to be written to disk.
typedef struct {
  WalFrameHeader hdr;
  uint8_t       *page_data;   // hdr.page_size bytes (caller owns)
  size_t         page_size;
} WalFrame;

// ── Progress callback ──────────────────────────────────────────────
// Called after each major phase completes.  phase: 1 = snapshot, 2 = WAL.
// percent: 0-100.  user_data: opaque pointer passed to arkilian_hydrate.
typedef void (*hydration_progress_cb)(int phase, int percent, void *user_data);

// ── Hydration result ───────────────────────────────────────────────

#define HYDRATION_OK          0
#define HYDRATION_ERR_NET    -1   // network / HTTP error
#define HYDRATION_ERR_DISK   -2   // local file I/O error
#define HYDRATION_ERR_CHECK  -3   // WAL frame checksum / salt mismatch
#define HYDRATION_ERR_MEM    -4   // out of memory

// ── Public API ─────────────────────────────────────────────────────

// Run the full two-phase hydration protocol.  Downloads the latest
// base snapshot, then downloads any WAL frames newer than the snapshot,
// reconstructs the local .db-wal file, and opens the database via
// SQLite's normal crash-recovery path.
//
//   db_path        Local path for the restored database (e.g. "mydb.db")
//   server_url     Base URL of the Arkilian server (e.g. "https://fly.io/v1")
//   auth_token     Bearer token for server authentication (may be NULL)
//   progress       Optional progress callback (may be NULL)
//
// Returns HYDRATION_OK on success, or a negative error code.
int arkilian_hydrate(const char *db_path,
                     const char *server_url,
                     const char *auth_token,
                     hydration_progress_cb progress,
                     void *user_data);

// ── WAL checksum helpers (exposed for testing) ─────────────────────

// Cumulative WAL checksum step.  Processes pairs of uint32 words.
// s1 and s2 are in/out running checksum seeds.
void wal_checksum_step(uint32_t *s1, uint32_t *s2,
                       const uint32_t *data, size_t words);

// Validate a single WAL frame: salts must match header, checksum must
// match the page data.  Updates running_s1/s2 on success.
// Returns 0 on success, HYDRATION_ERR_CHECK on mismatch.
int validate_frame(const WalHeader *hdr, WalFrame *frame,
                   uint32_t *running_s1, uint32_t *running_s2);

// Write a complete WAL file from scratch given an array of frames.
// Returns 0 on success, negative on error.
int wal_file_write(const char        *wal_path,
                   const WalHeader   *hdr,
                   const WalFrame    *frames,
                   int                frame_count);

#ifdef __cplusplus
}
#endif

#endif
