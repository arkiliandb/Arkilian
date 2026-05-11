/**
 * Arkilian — TypeScript type definitions
 */

export interface RunResult {
  /** Number of rows changed by the last statement. */
  changes: number;
  /** Row ID of the last inserted row. */
  lastInsertRowid: number;
}

export class Arkilian {
  /**
   * Open an Arkilian database.
   * @param filename Path to the SQLite database file. Defaults to `'app.sqlite'`.
   */
  constructor(filename?: string);

  /**
   * Execute one or more SQL statements without parameters.
   * Use for DDL or multi-statement scripts.
   */
  exec(sql: string): RunResult;

  /**
   * Execute a single parameterized statement (INSERT, UPDATE, DELETE).
   * @param sql SQL with `?` placeholders.
   * @param params Values to bind.
   */
  run(sql: string, ...params: unknown[]): RunResult;

  /**
   * Execute a parameterized query and return all rows.
   * @param sql SQL SELECT with optional `?` placeholders.
   * @param params Values to bind.
   * @returns Array of row objects keyed by column name.
   */
  all<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    ...params: unknown[]
  ): T[];

  /** Last error message from the database engine. */
  readonly errorMessage: string;

  /** Whether the database connection is currently open. */
  readonly isOpen: boolean;

  /** Close the database and release all resources. Safe to call multiple times. */
  close(): void;
}
