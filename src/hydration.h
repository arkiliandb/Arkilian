// Arkilian Hydration Engine v2 — Logical, Client-Driven Cloud Model
//
// Cold-start recovery via signed-URL snapshot + incremental log chunk replay.
// The client never touches the server's bandwidth — all heavy transfers go
// directly through Pre-Signed S3 URLs issued by the Control Plane API.
//
// Phases:
//   1. Request hydrate plan from Control Plane → signed URLs + baseline LSN
//   2. Download .snapshot via signed GET → decompress → save as local .db
//   3. Open DB, query _arkilian_meta for last_applied_lsn
//   4. Iterate incremental log chunks via signed GET → decompress → replay SQL
//
//   arkilian_hydrate("mydb.db", "https://control-plane/v1", "token");
//
#ifndef ARKILIAN_HYDRATION_H
#define ARKILIAN_HYDRATION_H

#include <stdint.h>
#include <stddef.h>
#include "deps/sqlite/sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

// ── Error codes ─────────────────────────────────────────────────────

#define HYDRATION_OK             0
#define HYDRATION_ERR_NET       -1   // HTTP / network failure
#define HYDRATION_ERR_DISK      -2   // local file I/O failure
#define HYDRATION_ERR_MEM       -3   // out of memory
#define HYDRATION_ERR_PROTO     -4   // control plane returned unexpected response
#define HYDRATION_ERR_SQL       -5   // SQL replay failed
#define HYDRATION_ERR_DECOMP    -6   // decompression failure
#define HYDRATION_ERR_EXPIRED   -7   // signed URL expired, caller should retry
#define HYDRATION_ERR_NOTFOUND  -8   // snapshot not yet uploaded (cold start)
#define HYDRATION_ERR_NEWER     -9   // local DB is AHEAD of the snapshot; refusing to clobber

// ── Types ───────────────────────────────────────────────────────────

// A single signed URL with its LSN range.
typedef struct {
  char   *url;          // Pre-Signed GET URL (caller frees)
  int64_t lsn_start;    // first LSN in this chunk (inclusive)
  int64_t lsn_end;      // last  LSN in this chunk (inclusive)
  int64_t expires_at;   // unix timestamp when URL expires (0 = no expiry)
} HydrateChunk;

// The complete hydration plan returned by the Control Plane.
typedef struct {
  char   *snapshot_url;   // Pre-Signed GET URL for the baseline .snapshot
  int64_t baseline_lsn;   // LSN embedded in the snapshot
  int64_t expires_at;     // when snapshot URL expires (0 = no expiry)

  HydrateChunk *chunks;   // ordered list of incremental chunks (caller frees)
  int           chunk_count;
} HydratePlan;

// Progress callback.
//   phase:   1 = downloading snapshot, 2 = replaying log chunks
//   current: number of chunks processed (phase 1) or SQL statements played (phase 2)
//   total:   total expected (0 if unknown)
typedef void (*hydration_progress_cb)(int phase, int current, int total,
                                       void *user_data);

// ── Minimal JSON helpers (exposed for testing) ──────────────────────

char   *json_get_string(const char *json, const char *key);
int64_t json_get_int64(const char *json, const char *key);
int     json_array_count(const char *json, const char *key);
char   *json_array_get(const char *json, const char *key, int index);

// Run the full two-phase hydration protocol.
//   db_path      Local target database path (e.g. "mydb.db")
//   server_url   Control Plane base URL (e.g. "https://api.arkilian.com/v1")
//   auth_token   Bearer token for authentication (may be NULL)
//   progress     Optional progress callback (may be NULL)
//
// Returns HYDRATION_OK on success, or a negative error code.
int arkilian_hydrate(const char *db_path,
                     const char *server_url,
                     const char *auth_token,
                     hydration_progress_cb progress,
                     void *user_data);

// Download a single plaintext SQL log chunk and replay it against an
// open database.  The chunk is wrapped in an explicit transaction.
// Updates _arkilian_meta.last_applied_lsn on success.
// Returns 0 on success, negative on error.
int hydrate_replay_chunk(sqlite3 *db, const char *raw_sql, int64_t chunk_lsn);

// Free all memory associated with a HydratePlan.
void hydrate_plan_free(HydratePlan *plan);

// Remove db_path along with its SQLite sidecar files (-wal, -shm,
// -journal).  Must be called before installing a downloaded snapshot:
// leftover WAL frames from a previous database file would otherwise be
// replayed into the new snapshot, silently corrupting it.
void hydration_remove_db_files(const char *db_path);

#ifdef __cplusplus
}
#endif

#endif
