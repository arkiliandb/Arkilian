import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const native = require("./build/Release/arkilian");

const SQLITE_OK = 0;
const SQLITE_ROW = 100;
const SQLITE_DONE = 101;

class Arkilian {
  constructor(token, dbPath = "app.sqlite") {
    if (!token) throw new Error("Your data token is required");
    this.id = native.db_init(dbPath);
    this.setToken(token);
    if (!this.id) {
      throw new Error("Failed to initialize database");
    }
  }

  static async open(dbPath = "app.sqlite") {
    return new Promise((resolve, reject) => {
      try {
        const db = new Arkilian(dbPath);
        resolve(db);
      } catch (err) {
        reject(err);
      }
    });
  }

  setToken(token) {
    const result = native.db_set_token(this.id, token);
    if (result !== SQLITE_OK) {
      throw new Error("Failed to set account token");
    }
    return this;
  }

  async close() {
    return new Promise((resolve, reject) => {
      try {
        if (this.id) {
          native.db_close(this.id);
          this.id = null;
        }
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }

  async exec(sql) {
    return new Promise((resolve, reject) => {
      try {
        const result = native.db_exec(this.id, sql);
        if (result !== SQLITE_OK && result !== SQLITE_DONE) {
          return reject(new Error(native.db_errmsg(this.id)));
        }
        resolve(result);
      } catch (err) {
        reject(err);
      }
    });
  }

  async prepare(sql) {
    return new Promise((resolve, reject) => {
      try {
        const result = native.db_prepare(this.id, sql);
        if (result !== SQLITE_OK) {
          return reject(new Error(native.db_errmsg(this.id)));
        }
        resolve(this);
      } catch (err) {
        reject(err);
      }
    });
  }

  step() {
    return native.db_step(this.id);
  }

  async finalize() {
    return new Promise((resolve, reject) => {
      try {
        const result = native.db_finalize(this.id);
        if (result !== SQLITE_OK) {
          return reject(new Error(native.db_errmsg(this.id)));
        }
        resolve(this);
      } catch (err) {
        reject(err);
      }
    });
  }

  async reset() {
    return new Promise((resolve, reject) => {
      try {
        const result = native.db_reset(this.id);
        if (result !== SQLITE_OK) {
          return reject(new Error(native.db_errmsg(this.id)));
        }
        resolve(this);
      } catch (err) {
        reject(err);
      }
    });
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

  async run(sql, params = []) {
    await this.prepare(sql);
    for (let i = 0; i < params.length; i++) {
      const p = params[i];
      if (typeof p === "string") {
        this.bindText(i + 1, p);
      } else if (Number.isInteger(p)) {
        this.bindInt(i + 1, p);
      } else {
        this.bindDouble(i + 1, p);
      }
    }
    this.step();
    await this.finalize();
    return this;
  }

  async all(sql, params = []) {
    const results = [];
    await this.prepare(sql);
    for (let i = 0; i < params.length; i++) {
      const p = params[i];
      if (typeof p === "string") {
        this.bindText(i + 1, p);
      } else if (Number.isInteger(p)) {
        this.bindInt(i + 1, p);
      } else {
        this.bindDouble(i + 1, p);
      }
    }
    const columns = this.getColumns();
    while (this.step() === SQLITE_ROW) {
      const row = {};
      for (let i = 0; i < columns.length; i++) {
        row[columns[i]] = this.get(i);
      }
      results.push(row);
    }
    await this.finalize();
    return results;
  }

  get lastError() {
    return native.db_errmsg(this.id);
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
