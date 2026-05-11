// Arkilian — Node.js & Bun.js JavaScript wrapper
// Provides a clean, idiomatic API on top of the N-API addon.

'use strict';

const path = require('path');
const os = require('os');

// ---------------------------------------------------------------------------
// Load the native addon
// ---------------------------------------------------------------------------
function loadAddon() {
  const moduleName = 'arkilian';

  // Try platform-specific prebuild first, then fall back to build directory
  const candidates = [
    // Prebuild: ./prebuilds/<platform>-<arch>/arkilian.node
    path.join(__dirname, '..', '..', 'prebuilds', `${os.platform()}-${os.arch()}`, `${moduleName}.node`),
    // cmake-js build output
    path.join(__dirname, '..', '..', 'build', 'Release', `${moduleName}.node`),
    path.join(__dirname, '..', '..', 'build', 'Debug', `${moduleName}.node`),
    path.join(__dirname, '..', '..', 'build', `${moduleName}.node`),
  ];

  for (const candidate of candidates) {
    try {
      return require(candidate);
    } catch {
      // try next
    }
  }

  throw new Error(
    `Failed to load arkilian native addon. Searched:\n${candidates.map(c => `  - ${c}`).join('\n')}\n\n` +
    'Make sure you have built the project: npm run build'
  );
}

const native = loadAddon();

// ---------------------------------------------------------------------------
// Arkilian class
// ---------------------------------------------------------------------------
class Arkilian {
  /** @type {any} */
  #handle = null;

  /**
   * Open an Arkilian database.
   * @param {string} [filename='app.sqlite'] - Path to the SQLite database file.
   */
  constructor(filename = 'app.sqlite') {
    this.#handle = native.init(filename);
  }

  /**
   * Execute one or more SQL statements (no parameters).
   * Good for DDL (CREATE TABLE, etc.) or multi-statement scripts.
   *
   * @param {string} sql - SQL string to execute.
   * @returns {{ changes: number, lastInsertRowid: number }}
   *
   * @example
   * db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
   * db.exec(`
   *   INSERT INTO users (name) VALUES ('Alice');
   *   INSERT INTO users (name) VALUES ('Bob');
   * `);
   */
  exec(sql) {
    this.#ensureOpen();
    return native.exec(this.#handle, sql);
  }

  /**
   * Execute a single parameterized statement (INSERT, UPDATE, DELETE).
   *
   * @param {string} sql - SQL with `?` placeholders.
   * @param {...*} params - Values to bind to placeholders.
   * @returns {{ changes: number, lastInsertRowid: number }}
   *
   * @example
   * db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30);
   */
  run(sql, ...params) {
    this.#ensureOpen();
    return native.run(this.#handle, sql, params);
  }

  /**
   * Execute a parameterized query and return all result rows.
   *
   * @param {string} sql - SQL SELECT with optional `?` placeholders.
   * @param {...*} params - Values to bind to placeholders.
   * @returns {Array<Object>} Array of row objects.
   *
   * @example
   * const users = db.all('SELECT * FROM users WHERE age > ?', 25);
   * // => [{ id: 1, name: 'Alice', age: 30 }]
   */
  all(sql, ...params) {
    this.#ensureOpen();
    return native.all(this.#handle, sql, params);
  }

  /**
   * Get the last error message from the database.
   * @returns {string}
   */
  get errorMessage() {
    this.#ensureOpen();
    return native.errmsg(this.#handle);
  }

  /**
   * Close the database connection and release resources.
   * Safe to call multiple times.
   */
  close() {
    if (this.#handle) {
      native.close(this.#handle);
      this.#handle = null;
    }
  }

  /**
   * Whether the database connection is open.
   * @returns {boolean}
   */
  get isOpen() {
    return this.#handle !== null;
  }

  /** @private */
  #ensureOpen() {
    if (!this.#handle) {
      throw new Error('Database is closed');
    }
  }
}

module.exports = { Arkilian };
