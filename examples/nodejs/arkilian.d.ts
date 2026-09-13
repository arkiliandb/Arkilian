declare module 'arkilian' {
  export interface S3Config {
    endpoint: string;
    bucket: string;
    region?: string;
    accessKey: string;
    secretKey: string;
    prefix: string;
  }

  export const SQLITE_OK: number;
  export const SQLITE_ROW: number;
  export const SQLITE_DONE: number;

  export const HealthFlags: {
    readonly BACKUP_ENABLED: number;
    readonly DEST_CONFIGURED: number;
    readonly FLUSH_ALIVE: number;
    readonly SNAPSHOT_ALIVE: number;
    readonly QUEUE_BELOW_CAP: number;
    readonly SCHEMA_IN_SYNC: number;
    readonly NO_DEAD_LETTER: number;
    readonly MANIFEST_RESOLVED: number;
    readonly NO_CAPTURE_GAP: number;
    readonly DURABLE_CAPTURE: number;
    readonly ALL_CORE: number;
  };

  export const HydrationErrors: {
    readonly OK: number;
    readonly NET: number;
    readonly DISK: number;
    readonly MEM: number;
    readonly PROTO: number;
    readonly SQL: number;
    readonly DECOMP: number;
    readonly EXPIRED: number;
    readonly NOTFOUND: number;
    readonly NEWER: number;
    readonly BUSY: number;
  };

  export type SQLParam = string | number | bigint | boolean | null | undefined;

  export default class Arkilian {
    constructor(dbPath?: string, maybePath?: string);

    static open(dbPath?: string, maybePath?: string): Promise<Arkilian>;
    static hydrateS3(dbPath: string, s3: S3Config): boolean;

    exec(sql: string): this;
    run(sql: string | { sql: string; params?: SQLParam[] }, params?: SQLParam[]): this;
    all<T = Record<string, any>>(
      sql: string | { sql: string; params?: SQLParam[] },
      params?: SQLParam[],
      maxRows?: number
    ): T[];

    transaction<T>(fn: () => T): T;

    walFlush(): this;
    get walLastSql(): string | null;

    get backupHealthy(): boolean;
    get backupHealthFlags(): number;
    get backupQueueDepth(): number;
    get backupDeadLetterCount(): number;
    get backupTriggerCoverage(): number;
    get backupSkippedTableCount(): number;
    get triggersDirty(): boolean;
    get capturePaused(): boolean;

    setAutoResyncTriggers(enabled: boolean): this;
    resyncTriggers(): this;
    setBackupEnabled(enabled: boolean): this;

    get lastError(): string;
    get changes(): number;
    get lastInsertRowid(): bigint | number;

    close(): void;
  }
}
