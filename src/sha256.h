// Arkilian SHA-256 — minimal, dependency-free FIPS 180-4 implementation.
//
// Used to authenticate hydration payloads: the control plane records the
// SHA-256 of each snapshot/chunk it stored, returns it in the hydrate
// plan, and the client recomputes the digest of what it downloaded and
// refuses on mismatch. This closes the "pre-signed URL authorizes WHO can
// read but not WHAT was stored" gap: a tampered object body (e.g. from a
// leaked bucket-write credential) is detected before any SQL is replayed
// against the local database.
//
// The implementation is deliberately small and self-contained (no
// OpenSSL/libcrypto dependency) to preserve Arkilian's minimal-deps
// build. Correctness is pinned by NIST FIPS 180-4 test vectors in
// tests/test_sha256.c.

#ifndef ARKILIAN_SHA256_H
#define ARKILIAN_SHA256_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// SHA-256 digest of `data` (len bytes) rendered as a lowercase hex
// string in `out`. `out` must point to at least 65 bytes (32-byte digest
// → 64 hex chars + NUL).
void ark_sha256_hex(const void *data, size_t len, char out[65]);

// SHA-256 digest of the file at `path`, rendered as a lowercase hex
// string in `out`. Streams the file in 64 KiB chunks so multi-hundred-MB
// snapshots do not need to be slurped into memory. Returns 0 on success,
// non-zero on open/read/I/O failure (out is left in an indeterminate
// state on failure).
int ark_sha256_hex_file(const char *path, char out[65]);

#ifdef __cplusplus
}
#endif

#endif // ARKILIAN_SHA256_H
