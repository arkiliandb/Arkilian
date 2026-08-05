import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// Runtime resolution via node-gyp-build: picks the prebuilt .node for this
// platform/arch from the bundled prebuilds/ dir (offline, no compiler),
// falling back to the build/Release output of a source build. Lets
// `npm install arkilian` succeed on toolchain-less hosts (Alpine/Lambda/
// serverless) where the prebuildify-produced .node is bundled — launch
// Checklist #2.
const native = require("node-gyp-build")(__dirname);

const SQLITE_OK = 0;
const SQLITE_ROW = 100;
const SQLITE_DONE = 101;

class Arkilian {
  constructor(apiKey, dbPath = "app.sqlite") {
    if (!apiKey) throw new Error("Your API key is required");
    this.id = native.db_init(dbPath);
    if (!this.id) {
      throw new Error("Failed to initialize database");
    }
    this.setApiKey(apiKey);
  }

  static async open(apiKey, dbPath = "app.sqlite") {
    return new Arkilian(apiKey, dbPath);
  }

  // Cold-start restore from the control plane. MUST be called from a
  // cold process (before db_init opens the database). Downloads the
  // latest snapshot + replays incremental chunks via pre-signed URLs.
  //   dbPath     — local database file path
  //   controlUrl — control plane base URL (e.g. "https://api.arkilian.com")
  //   apiKey     — the tenant's API key
  static hydrate(dbPath, controlUrl, apiKey) {
    const rc = native.db_hydrate(dbPath, controlUrl, apiKey);
    if (rc !== true) throw new Error(`Hydration failed (see logs for error code)`);
    return true;
  }

  setApiKey(apiKey) {
    const result = native.db_set_api_key(this.id, apiKey);
    if (result !== SQLITE_OK) {
      throw new Error("Failed to set API key");
    }
    return this;
  }

  // Runtime kill-switch (spec §1): stops all outbound backup activity
  // (WAL shipping + snapshot uploads) without a restart. Capture keeps
  // running — rows queue up in _pending_backup and shipping resumes
  // where it left off when re-enabled. Intended for incident response.
  setBackupEnabled(enabled) {
    native.db_backup_set_enabled(this.id, enabled);
    return this;
  }

  get backupEnabled() {
    return native.db_backup_is_enabled(this.id);
  }

  // Route diagnostics through a JS callback (level, message). Logs can
  // fire from the backup threads — this is marshalled thread-safely.
  setLogCallback(fn) {
    native.db_set_log_callback(this.id, fn || null);
    return this;
  }

  get backupQueueDepth() {
    return native.db_backup_queue_depth(this.id);
  }

  get backupOldestPendingAgeSec() {
    return native.db_backup_oldest_pending_age_sec(this.id);
  }

  get backupDeadLetterCount() {
    return native.db_backup_dead_letter_count(this.id);
  }

  get backupThreadHeartbeatAgeMs() {
    return native.db_backup_thread_heartbeat_age_ms(this.id);
  }

  get backupSnapshotHeartbeatAgeMs() {
    return native.db_backup_snapshot_heartbeat_age_ms(this.id);
  }

  get backupTriggerCoverage() {
    return native.db_backup_trigger_coverage(this.id);
  }

  // Tables with no PRIMARY KEY are skipped by capture (unreplayable).
  // Must be 0 — every skipped table is data that never leaves the box.
  get backupSkippedTableCount() {
    return native.db_backup_skipped_table_count(this.id);
  }

  get backupHealthy() {
    return native.db_backup_is_healthy(this.id);
  }

  // (Risk #1) Returns true when a raw-handle schema change (DDL via
  // db_get_handle()) has desynchronized the capture triggers. Alert when
  // true — triggers are stale and a db_resyncTriggers() is needed.
  get triggersDirty() {
    return native.db_backup_triggers_dirty(this.id);
  }

  resyncTriggers() {
    const rc = native.db_resync_triggers(this.id);
    if (rc !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  close() {
    if (this.id) {
      native.db_close(this.id);
      this.id = null;
    }
    return this;
  }

  exec(sql) {
    const query = typeof sql === "object" && sql !== null ? sql.sql : sql;
    const result = native.db_exec(this.id, query);
    // db_exec's public contract is SQLITE_OK on success.
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return result;
  }

  prepare(sql = "") {
    const query = typeof sql === "object" && sql !== null ? sql.sql : sql;
    const result = native.db_prepare(this.id, query);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  useStmt(index) {
    const result = native.db_use_stmt(this.id, index);
    if (result !== SQLITE_OK) {
      throw new Error("Invalid statement index or statement already finalized");
    }
    return this;
  }

  stmtCount() {
    return native.db_stmt_count(this.id);
  }

  step() {
    return native.db_step(this.id);
  }

  finalize() {
    const result = native.db_finalize(this.id);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  reset() {
    const result = native.db_reset(this.id);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  begin() {
    const rc = native.db_begin(this.id);
    if (rc !== SQLITE_OK) throw new Error(native.db_errmsg(this.id));
    return this;
  }

  commit() {
    const rc = native.db_commit(this.id);
    if (rc !== SQLITE_OK) throw new Error(native.db_errmsg(this.id));
    return this;
  }

  rollback() {
    const rc = native.db_rollback(this.id);
    if (rc !== SQLITE_OK) throw new Error(native.db_errmsg(this.id));
    return this;
  }

  transaction(fn) {
    this.begin();
    try {
      const res = fn(this);
      this.commit();
      return res;
    } catch (err) {
      this.rollback();
      throw err;
    }
  }

  getColumns() {
    const count = native.db_column_count(this.id);
    const columns = [];
    for (let i = 0; i < count; i++) {
      columns.push(native.db_column_name(this.id, i));
    }
    return columns;
  }

  get(index) {
    return native.db_column_text(this.id, index);
  }

  getInt(index) {
    return native.db_column_int(this.id, index);
  }

  getDouble(index) {
    return native.db_column_double(this.id, index);
  }

  bindText(index, value) {
    const result = native.db_bind_text(this.id, index, value);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  bindInt(index, value) {
    const result = native.db_bind_int(this.id, index, value);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  bindInt64(index, value) {
    const result = native.db_bind_int64(this.id, index, value);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  // Route a JS number to the correct binder. Integers beyond 32 bits
  // MUST go through bind_int64 — Int32Value() would silently wrap them
  // (epoch-ms timestamps, large IDs, etc.).
  _bindParam(index, p) {
    if (typeof p === "string") {
      this.bindText(index, p);
    } else if (typeof p === "bigint") {
      this.bindInt64(index, p);
    } else if (Number.isInteger(p)) {
      if (!Number.isSafeInteger(p)) {
        throw new Error("Integer parameter out of safe range (|v| > 2^53-1)");
      }
      if (p >= -2147483648 && p <= 2147483647) {
        this.bindInt(index, p);
      } else {
        this.bindInt64(index, p);
      }
    } else if (p === null || p === undefined) {
      native.db_bind_null(this.id, index);
    } else {
      this.bindDouble(index, p);
    }
  }

  bindDouble(index, value) {
    const result = native.db_bind_double(this.id, index, value);
    if (result !== SQLITE_OK) {
      throw new Error(native.db_errmsg(this.id));
    }
    return this;
  }

  run(sql, params = []) {
    const query = typeof sql === "object" && sql !== null ? sql.sql : sql;
    const bindParams =
      typeof sql === "object" && sql !== null ? sql.params || params : params;
    this.prepare(query);
    for (let i = 0; i < bindParams.length; i++) {
      this._bindParam(i + 1, bindParams[i]);
    }
    // A failed step (UNIQUE/NOT NULL/FK constraint, I/O error) must
    // throw — silently swallowing it is silent data loss.
    const rc = this.step();
    if (rc !== SQLITE_DONE && rc !== SQLITE_ROW) {
      const msg = native.db_errmsg(this.id);
      this.finalize();
      throw new Error(msg);
    }
    this.finalize();
    return this;
  }

  // High performance C++ native single-turn fetch. Optional maxRows caps
  // the materialized result set (0/unset = unlimited).
  all(sql, params = [], maxRows = 0) {
    const query = typeof sql === "object" && sql !== null ? sql.sql : sql;
    const bindParams =
      typeof sql === "object" && sql !== null ? sql.params || params : params;
    this.prepare(query);
    for (let i = 0; i < bindParams.length; i++) {
      this._bindParam(i + 1, bindParams[i]);
    }
    return native.db_all_native(this.id, maxRows);
  }

  get lastError() {
    return native.db_errmsg(this.id);
  }

  get changes() {
    return native.db_changes(this.id);
  }

  get lastInsertRowid() {
    return native.db_last_insert_rowid(this.id);
  }

  static get SQLITE_OK() {
    return SQLITE_OK;
  }
  static get SQLITE_ROW() {
    return SQLITE_ROW;
  }
  static get SQLITE_DONE() {
    return SQLITE_DONE;
  }
}

export default Arkilian;
