// Shared SQLite schema for the Arkilian control plane. Used by initDB()
// (production) and setupTestDB() (tests) so the two never drift.
package main

import "time"

const schemaSQL = `
CREATE TABLE IF NOT EXISTS users (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	email         TEXT UNIQUE NOT NULL,
	password_hash TEXT NOT NULL,
	created_at    INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS databases (
	db_id     TEXT PRIMARY KEY,
	user_id   INTEGER NOT NULL REFERENCES users(id),
	name      TEXT NOT NULL,
	api_key   TEXT UNIQUE NOT NULL,
	created_at INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS wal_entries (
	lsn         INTEGER PRIMARY KEY AUTOINCREMENT,
	db_id       TEXT NOT NULL REFERENCES databases(db_id),
	ts          INTEGER NOT NULL,
	op          INTEGER NOT NULL,
	table_id    INTEGER NOT NULL,
	pk          INTEGER NOT NULL,
	sql         TEXT,
	payload_id  TEXT,
	received_at INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_wal_db ON wal_entries(db_id, lsn);
-- At-least-once redelivery (§8.2): the client sends X-Arkilian-Payload-Id
-- (its _pending_backup row id) and replays of the same payload must be
-- no-ops instead of duplicates. The key is scoped PER DATABASE: outbox
-- ids restart at 1 for every fresh client file, so a global unique
-- index would silently drop one tenant's rows when ids collide with
-- another tenant's. NULL payload_ids (bulk array pushes) are never
-- deduped — SQLite UNIQUE allows multiple NULLs.
CREATE UNIQUE INDEX IF NOT EXISTS idx_wal_payload_id ON wal_entries(db_id, payload_id);
CREATE TABLE IF NOT EXISTS snapshots (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	db_id        TEXT NOT NULL REFERENCES databases(db_id),
	baseline_lsn INTEGER NOT NULL,
	s3_key       TEXT NOT NULL,
	sha256       TEXT,
	created_at   INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS chunks (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	db_id      TEXT NOT NULL REFERENCES databases(db_id),
	lsn_start  INTEGER NOT NULL,
	lsn_end    INTEGER NOT NULL,
	s3_key     TEXT NOT NULL,
	sha256     TEXT,
	created_at  INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS db_stats (
	db_id         TEXT PRIMARY KEY REFERENCES databases(db_id),
	last_seen     INTEGER NOT NULL DEFAULT 0,
	total_entries INTEGER NOT NULL DEFAULT 0,
	today         TEXT NOT NULL DEFAULT '',
	entries_today INTEGER NOT NULL DEFAULT 0,
	bytes_today   INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER
);
CREATE TABLE IF NOT EXISTS db_daily_stats (
	db_id   TEXT NOT NULL REFERENCES databases(db_id),
	day     TEXT NOT NULL,
	entries INTEGER NOT NULL DEFAULT 0,
	bytes   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (db_id, day)
);
`

// dayBucket returns the UTC calendar day (YYYY-MM-DD) for a unix timestamp.
func dayBucket(unix int64) string {
	return time.Unix(unix, 0).UTC().Format("2006-01-02")
}
