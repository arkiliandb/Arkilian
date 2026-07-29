import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const native = require("./build/Release/arkilian");

const SQLITE_OK = 0;
const SQLITE_ROW = 100;
const SQLITE_DONE = 101;

class Arkilian {
  constructor(token, dbPath = "app.sqlite") {
    if (!token) throw new Error("Your database token is required");
    this.id = native.db_init(dbPath);
    if (!this.id) {
      throw new Error("Failed to initialize database");
    }
    this.setToken(token);
  }

  static async open(token, dbPath = "app.sqlite") {
    return new Arkilian(token, dbPath);
  }

  setToken(token) {
    const result = native.db_set_token(this.id, token);
    if (result !== SQLITE_OK) {
      throw new Error("Failed to set account token");
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
    if (result !== SQLITE_OK && result !== SQLITE_DONE) {
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
      const p = bindParams[i];
      if (typeof p === "string") {
        this.bindText(i + 1, p);
      } else if (Number.isInteger(p)) {
        this.bindInt(i + 1, p);
      } else if (p === null || p === undefined) {
        native.db_bind_null(this.id, i + 1);
      } else {
        this.bindDouble(i + 1, p);
      }
    }
    this.step();
    this.finalize();
    return this;
  }

  all(sql, params = []) {
    const query = typeof sql === "object" && sql !== null ? sql.sql : sql;
    const bindParams =
      typeof sql === "object" && sql !== null ? sql.params || params : params;
    this.prepare(query);
    for (let i = 0; i < bindParams.length; i++) {
      const p = bindParams[i];
      if (typeof p === "string") {
        this.bindText(i + 1, p);
      } else if (Number.isInteger(p)) {
        this.bindInt(i + 1, p);
      } else if (p === null || p === undefined) {
        native.db_bind_null(this.id, i + 1);
      } else {
        this.bindDouble(i + 1, p);
      }
    }
    // High performance C++ native single-turn fetch
    return native.db_all_native(this.id);
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
