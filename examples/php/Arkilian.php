<?php
/**
 * Arkilian - PHP FFI bindings for Arkilian Managed SQLite Database Engine.
 * 1:1 Parity with core C engine (src/class.h, src/hydration.h).
 */

// SQLite Result Codes
if (!defined('SQLITE_OK'))    define('SQLITE_OK', 0);
if (!defined('SQLITE_ERROR')) define('SQLITE_ERROR', 1);
if (!defined('SQLITE_BUSY'))  define('SQLITE_BUSY', 5);
if (!defined('SQLITE_ROW'))   define('SQLITE_ROW', 100);
if (!defined('SQLITE_DONE'))  define('SQLITE_DONE', 101);

// SQLite Column Types
if (!defined('SQLITE_INTEGER')) define('SQLITE_INTEGER', 1);
if (!defined('SQLITE_FLOAT'))   define('SQLITE_FLOAT', 2);
if (!defined('SQLITE_TEXT'))    define('SQLITE_TEXT', 3);
if (!defined('SQLITE_BLOB'))    define('SQLITE_BLOB', 4);
if (!defined('SQLITE_NULL'))    define('SQLITE_NULL', 5);

// Health State Machine Flags (ARK_HF_*)
if (!defined('ARK_HF_BACKUP_ENABLED'))      define('ARK_HF_BACKUP_ENABLED', 1 << 0);
if (!defined('ARK_HF_DEST_CONFIGURED'))     define('ARK_HF_DEST_CONFIGURED', 1 << 1);
if (!defined('ARK_HF_FLUSH_ALIVE'))         define('ARK_HF_FLUSH_ALIVE', 1 << 2);
if (!defined('ARK_HF_SNAPSHOT_ALIVE'))      define('ARK_HF_SNAPSHOT_ALIVE', 1 << 3);
if (!defined('ARK_HF_QUEUE_BELOW_CAP'))     define('ARK_HF_QUEUE_BELOW_CAP', 1 << 4);
if (!defined('ARK_HF_SCHEMA_IN_SYNC'))      define('ARK_HF_SCHEMA_IN_SYNC', 1 << 5);
if (!defined('ARK_HF_NO_DEAD_LETTER'))      define('ARK_HF_NO_DEAD_LETTER', 1 << 6);
if (!defined('ARK_HF_MANIFEST_RESOLVED'))   define('ARK_HF_MANIFEST_RESOLVED', 1 << 7);
if (!defined('ARK_HF_NO_CAPTURE_GAP'))      define('ARK_HF_NO_CAPTURE_GAP', 1 << 8);
if (!defined('ARK_HF_DURABLE_CAPTURE'))     define('ARK_HF_DURABLE_CAPTURE', 1 << 9);
if (!defined('ARK_HF_ALL_CORE'))            define('ARK_HF_ALL_CORE', 0x3ff);

// Hydration Error Codes
if (!defined('HYDRATION_OK'))            define('HYDRATION_OK', 0);
if (!defined('HYDRATION_ERR_NET'))       define('HYDRATION_ERR_NET', -1);
if (!defined('HYDRATION_ERR_DISK'))      define('HYDRATION_ERR_DISK', -2);
if (!defined('HYDRATION_ERR_MEM'))       define('HYDRATION_ERR_MEM', -3);
if (!defined('HYDRATION_ERR_PROTO'))     define('HYDRATION_ERR_PROTO', -4);
if (!defined('HYDRATION_ERR_SQL'))       define('HYDRATION_ERR_SQL', -5);
if (!defined('HYDRATION_ERR_DECOMP'))    define('HYDRATION_ERR_DECOMP', -6);
if (!defined('HYDRATION_ERR_EXPIRED'))   define('HYDRATION_ERR_EXPIRED', -7);
if (!defined('HYDRATION_ERR_NOT_FOUND')) define('HYDRATION_ERR_NOT_FOUND', -8);
if (!defined('HYDRATION_ERR_NEWER'))     define('HYDRATION_ERR_NEWER', -9);
if (!defined('HYDRATION_ERR_BUSY'))      define('HYDRATION_ERR_BUSY', -10);

class Arkilian {
    private static ?FFI $globalFFI = null;
    private $db;
    private FFI $ffi;
    private bool $isOpen = false;

    public function __construct(string $dbPath = 'app.sqlite', ?string $token = null) {
        $this->ffi = self::getFFI();

        $dbPtr = $this->ffi->new('arkilian*');
        $result = $this->ffi->db_init(\FFI::addr($dbPtr), $dbPath);

        if ($result !== SQLITE_OK) {
            $error = $this->ffi->db_errmsg($dbPtr);
            $msg = ($error !== null) ? \FFI::string($error) : 'Unknown error';
            if ($dbPtr !== null) {
                $this->ffi->db_close($dbPtr);
            }
            throw new RuntimeException("Failed to initialize database: " . $msg);
        }

        $this->db = $dbPtr;
        $this->isOpen = true;

        if ($token !== null) {
            $this->setToken($token);
        }
    }

    public static function getFFI(): FFI {
        if (self::$globalFFI !== null) {
            return self::$globalFFI;
        }

        $libPath = self::findLibrary();
        if (!file_exists($libPath)) {
            throw new RuntimeException("Library not found at: " . $libPath);
        }

        $cdef = "
            typedef struct arkilian arkilian;
            typedef struct sqlite3 sqlite3;
            typedef long long int64_t;

            typedef struct {
                const char *endpoint;
                const char *region;
                const char *bucket;
                const char *access_key_id;
                const char *secret_access_key;
                const char *session_token;
                int use_ssl;
                int timeout_ms;
            } arkilian_s3_config;

            int db_init(arkilian **db, const char *connection_url);
            void db_close(arkilian *db);
            const char* db_errmsg(arkilian *db);
            sqlite3* db_get_handle(arkilian *db);

            int db_exec(arkilian *db, const char *sql);
            int db_begin(arkilian *db);
            int db_commit(arkilian *db);
            int db_rollback(arkilian *db);
            int db_changes(arkilian *db);
            int64_t db_last_insert_rowid(arkilian *db);

            int db_prepare(arkilian *db, const char *sql);
            int db_use_stmt(arkilian *db, int index);
            int db_stmt_count(arkilian *db);
            int db_step(arkilian *db);
            int db_finalize(arkilian *db);
            int db_reset(arkilian *db);

            int db_column_count(arkilian *db);
            const char* db_column_name(arkilian *db, int col);
            int db_column_type(arkilian *db, int col);
            const char* db_column_text(arkilian *db, int col);
            int db_column_int(arkilian *db, int col);
            int64_t db_column_int64(arkilian *db, int col);
            double db_column_double(arkilian *db, int col);
            const void* db_column_blob(arkilian *db, int col);
            int db_column_bytes(arkilian *db, int col);

            int db_bind_text(arkilian *db, int idx, const char *val);
            int db_bind_int(arkilian *db, int idx, int val);
            int db_bind_int64(arkilian *db, int idx, int64_t val);
            int db_bind_double(arkilian *db, int idx, double val);
            int db_bind_null(arkilian *db, int idx);
            int db_bind_blob(arkilian *db, int idx, const void *val, int n);

            int db_wal_pending(arkilian *db);
            void db_wal_flush(arkilian *db);
            const char* db_wal_last_sql(arkilian *db);

            void db_backup_set_enabled(arkilian *db, int enabled);
            int db_backup_is_enabled(arkilian *db);
            int db_resync_triggers(arkilian *db);
            void db_set_auto_resync_triggers(arkilian *db, int enabled);
            int db_get_auto_resync_triggers(arkilian *db);
            int db_backup_triggers_dirty(arkilian *db);
            int db_backup_capture_paused(arkilian *db);

            int db_backup_queue_depth(arkilian *db);
            int db_backup_oldest_pending_age_sec(arkilian *db);
            int db_backup_dead_letter_count(arkilian *db);
            int db_backup_thread_heartbeat_age_ms(arkilian *db);
            int db_backup_snapshot_heartbeat_age_ms(arkilian *db);
            double db_backup_trigger_coverage(arkilian *db);
            int db_backup_skipped_table_count(arkilian *db);
            int db_backup_chunk_count(arkilian *db);
            int db_backup_last_chunk_flush_age_ms(arkilian *db);
            unsigned int db_backup_health_flags(arkilian *db);
            int db_backup_is_healthy(arkilian *db);

            int arkilian_hydrate_s3(
                const char *db_path,
                const char *s3_endpoint,
                const char *s3_bucket,
                const char *s3_region,
                const char *s3_access_key,
                const char *s3_secret_key,
                const char *s3_prefix,
                void *progress,
                void *user_data
            );
        ";

        self::$globalFFI = FFI::cdef($cdef, $libPath);
        return self::$globalFFI;
    }

    private static function findLibrary(): string {
        $envPath = getenv('ARKILIAN_LIB_PATH');
        if ($envPath !== false && file_exists($envPath)) {
            return $envPath;
        }

        $candidates = [
            __DIR__ . '/libarkilian.dylib',
            __DIR__ . '/libarkilian.so',
            __DIR__ . '/../c/lib/libarkilian.dylib',
            __DIR__ . '/../c/lib/libarkilian.so',
            __DIR__ . '/../../build/libarkilian.dylib',
            __DIR__ . '/../../build/libarkilian.so',
            __DIR__ . '/../../build-c/libarkilian.dylib',
            __DIR__ . '/../../build-c/libarkilian.so',
            __DIR__ . '/../../build/Release/libarkilian.dylib',
            __DIR__ . '/../../build/Release/libarkilian.so',
            '/opt/homebrew/lib/libarkilian.dylib',
            '/usr/local/lib/libarkilian.dylib',
            '/usr/local/lib/libarkilian.so',
            '/usr/lib/libarkilian.so',
        ];

        foreach ($candidates as $path) {
            if (file_exists($path)) {
                return realpath($path) ?: $path;
            }
        }

        return $candidates[0];
    }

    public function close(): void {
        if ($this->isOpen && $this->db !== null) {
            $this->ffi->db_close($this->db);
            $this->db = null;
            $this->isOpen = false;
        }
    }

    public function getHandle() {
        return $this->ffi->db_get_handle($this->db);
    }

    public function setToken(string $token): self {
        // Preserved for backward compatibility (S3 creds configured via environment).
        return $this;
    }

    // ── Queries & DML ─────────────────────────────────────────────────────────

    public function exec(string $sql): int {
        $result = $this->ffi->db_exec($this->db, $sql);
        if ($result !== SQLITE_OK && $result !== SQLITE_DONE) {
            throw new RuntimeException($this->lastError());
        }
        return $result;
    }

    // ── Transactions ──────────────────────────────────────────────────────────

    public function begin(): self {
        $rc = $this->ffi->db_begin($this->db);
        if ($rc !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function commit(): self {
        $rc = $this->ffi->db_commit($this->db);
        if ($rc !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function rollback(): self {
        $rc = $this->ffi->db_rollback($this->db);
        if ($rc !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    // ── Changes & Row ID ──────────────────────────────────────────────────────

    public function changes(): int {
        return $this->ffi->db_changes($this->db);
    }

    public function lastInsertRowId(): int {
        return (int)$this->ffi->db_last_insert_rowid($this->db);
    }

    // ── Prepared Statements ───────────────────────────────────────────────────

    public function prepare(string $sql): self {
        $result = $this->ffi->db_prepare($this->db, $sql);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function useStmt(int $index): self {
        $result = $this->ffi->db_use_stmt($this->db, $index);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException("Invalid statement index or statement already finalized");
        }
        return $this;
    }

    public function stmtCount(): int {
        return $this->ffi->db_stmt_count($this->db);
    }

    public function step(): int {
        return $this->ffi->db_step($this->db);
    }

    public function finalize(): int {
        $result = $this->ffi->db_finalize($this->db);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $result;
    }

    public function reset(): int {
        $result = $this->ffi->db_reset($this->db);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $result;
    }

    // ── Column Introspection & Access ─────────────────────────────────────────

    public function columnCount(): int {
        return $this->ffi->db_column_count($this->db);
    }

    public function columnName(int $col): ?string {
        $result = $this->ffi->db_column_name($this->db, $col);
        if ($result === null) {
            return null;
        }
        return is_string($result) ? $result : \FFI::string($result);
    }

    public function columnType(int $col): int {
        return $this->ffi->db_column_type($this->db, $col);
    }

    public function columnText(int $col): ?string {
        $result = $this->ffi->db_column_text($this->db, $col);
        if ($result === null) {
            return null;
        }
        return is_string($result) ? $result : \FFI::string($result);
    }

    public function columnInt(int $col): int {
        return $this->ffi->db_column_int($this->db, $col);
    }

    public function columnInt64(int $col): int {
        return (int)$this->ffi->db_column_int64($this->db, $col);
    }

    public function columnDouble(int $col): float {
        return $this->ffi->db_column_double($this->db, $col);
    }

    public function columnBlob(int $col): string {
        $ptr = $this->ffi->db_column_blob($this->db, $col);
        $len = $this->ffi->db_column_bytes($this->db, $col);
        if ($ptr === null || $len <= 0) {
            return '';
        }
        return is_string($ptr) ? substr($ptr, 0, $len) : \FFI::string($ptr, $len);
    }

    public function columnBytes(int $col): int {
        return $this->ffi->db_column_bytes($this->db, $col);
    }

    public function columnValue(int $col): mixed {
        $type = $this->columnType($col);
        switch ($type) {
            case SQLITE_NULL:
                return null;
            case SQLITE_INTEGER:
                return $this->columnInt64($col);
            case SQLITE_FLOAT:
                return $this->columnDouble($col);
            case SQLITE_BLOB:
                return $this->columnBlob($col);
            default:
                return $this->columnText($col);
        }
    }

    // ── Parameter Binding ─────────────────────────────────────────────────────

    public function bindText(int $idx, string $value): self {
        $result = $this->ffi->db_bind_text($this->db, $idx, $value);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function bindInt(int $idx, int $value): self {
        $result = $this->ffi->db_bind_int($this->db, $idx, $value);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function bindInt64(int $idx, int $value): self {
        $result = $this->ffi->db_bind_int64($this->db, $idx, $value);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function bindDouble(int $idx, float $value): self {
        $result = $this->ffi->db_bind_double($this->db, $idx, $value);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function bindNull(int $idx): self {
        $result = $this->ffi->db_bind_null($this->db, $idx);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    public function bindBlob(int $idx, string $value): self {
        $n = strlen($value);
        $result = $this->ffi->db_bind_blob($this->db, $idx, $value, $n);
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $this;
    }

    // ── WAL & Shipping ────────────────────────────────────────────────────────

    public function walPending(): int {
        return $this->ffi->db_wal_pending($this->db);
    }

    public function walFlush(): void {
        $this->ffi->db_wal_flush($this->db);
    }

    public function walLastSql(): ?string {
        $ptr = $this->ffi->db_wal_last_sql($this->db);
        if ($ptr === null) {
            return null;
        }
        return is_string($ptr) ? $ptr : \FFI::string($ptr);
    }

    // ── Backup & Trigger Controls ─────────────────────────────────────────────

    public function setBackupEnabled(bool $enabled): void {
        $this->ffi->db_backup_set_enabled($this->db, $enabled ? 1 : 0);
    }

    public function isBackupEnabled(): bool {
        return (bool)$this->ffi->db_backup_is_enabled($this->db);
    }

    public function resyncTriggers(): int {
        $rc = $this->ffi->db_resync_triggers($this->db);
        if ($rc !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        return $rc;
    }

    public function setAutoResyncTriggers(bool $enabled): void {
        $this->ffi->db_set_auto_resync_triggers($this->db, $enabled ? 1 : 0);
    }

    public function getAutoResyncTriggers(): bool {
        return (bool)$this->ffi->db_get_auto_resync_triggers($this->db);
    }

    public function backupTriggersDirty(): bool {
        return (bool)$this->ffi->db_backup_triggers_dirty($this->db);
    }

    public function backupCapturePaused(): bool {
        return (bool)$this->ffi->db_backup_capture_paused($this->db);
    }

    // ── Monitoring & Health State Machine ─────────────────────────────────────

    public function backupQueueDepth(): int {
        return $this->ffi->db_backup_queue_depth($this->db);
    }

    public function backupOldestPendingAgeSec(): int {
        return $this->ffi->db_backup_oldest_pending_age_sec($this->db);
    }

    public function backupDeadLetterCount(): int {
        return $this->ffi->db_backup_dead_letter_count($this->db);
    }

    public function backupThreadHeartbeatAgeMs(): int {
        return $this->ffi->db_backup_thread_heartbeat_age_ms($this->db);
    }

    public function backupSnapshotHeartbeatAgeMs(): int {
        return $this->ffi->db_backup_snapshot_heartbeat_age_ms($this->db);
    }

    public function backupTriggerCoverage(): float {
        return $this->ffi->db_backup_trigger_coverage($this->db);
    }

    public function backupSkippedTableCount(): int {
        return $this->ffi->db_backup_skipped_table_count($this->db);
    }

    public function backupChunkCount(): int {
        return $this->ffi->db_backup_chunk_count($this->db);
    }

    public function backupLastChunkFlushAgeMs(): int {
        return $this->ffi->db_backup_last_chunk_flush_age_ms($this->db);
    }

    public function backupHealthFlags(): int {
        return (int)$this->ffi->db_backup_health_flags($this->db);
    }

    public function backupIsHealthy(): bool {
        return (bool)$this->ffi->db_backup_is_healthy($this->db);
    }

    // ── S3 Cold Hydration ─────────────────────────────────────────────────────

    public static function hydrateS3(string $localDbPath, string $prefix, array $s3Config): void {
        $ffi = self::getFFI();

        $endpoint = $s3Config['endpoint'] ?? '';
        $bucket = $s3Config['bucket'] ?? '';
        $region = $s3Config['region'] ?? 'us-east-1';
        $accessKey = $s3Config['access_key_id'] ?? ($s3Config['accessKey'] ?? '');
        $secretKey = $s3Config['secret_access_key'] ?? ($s3Config['secretKey'] ?? '');

        $rc = $ffi->arkilian_hydrate_s3(
            $localDbPath,
            $endpoint,
            $bucket,
            $region,
            $accessKey,
            $secretKey,
            $prefix,
            null,
            null
        );

        if ($rc !== HYDRATION_OK) {
            throw new RuntimeException("Cold hydration failed with error code {$rc}");
        }
    }

    // ── High-Level Helpers ────────────────────────────────────────────────────

    public function run(string $sql, array $params = []): self {
        $this->prepare($sql);

        foreach ($params as $i => $param) {
            $idx = $i + 1;
            if ($param === null) {
                $this->bindNull($idx);
            } elseif (is_int($param)) {
                $this->bindInt64($idx, $param);
            } elseif (is_float($param)) {
                $this->bindDouble($idx, $param);
            } elseif (is_bool($param)) {
                $this->bindInt($idx, $param ? 1 : 0);
            } else {
                $this->bindText($idx, (string)$param);
            }
        }

        $this->step();
        $this->finalize();

        return $this;
    }

    public function all(string $sql, array $params = []): array {
        $this->prepare($sql);

        foreach ($params as $i => $param) {
            $idx = $i + 1;
            if ($param === null) {
                $this->bindNull($idx);
            } elseif (is_int($param)) {
                $this->bindInt64($idx, $param);
            } elseif (is_float($param)) {
                $this->bindDouble($idx, $param);
            } elseif (is_bool($param)) {
                $this->bindInt($idx, $param ? 1 : 0);
            } else {
                $this->bindText($idx, (string)$param);
            }
        }

        $columns = [];
        for ($i = 0; $i < $this->columnCount(); $i++) {
            $columns[] = $this->columnName($i);
        }

        $results = [];
        while ($this->step() === SQLITE_ROW) {
            $row = [];
            for ($i = 0; $i < count($columns); $i++) {
                $row[$columns[$i]] = $this->columnValue($i);
            }
            $results[] = $row;
        }

        $this->finalize();

        return $results;
    }

    public function lastError(): string {
        if ($this->db === null) {
            return "";
        }
        $result = $this->ffi->db_errmsg($this->db);
        if ($result === null) {
            return "";
        }
        return is_string($result) ? $result : \FFI::string($result);
    }

    public function __destruct() {
        $this->close();
    }
}