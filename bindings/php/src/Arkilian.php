<?php
/**
 * Arkilian — PHP binding via FFI
 *
 * Requires PHP 7.4+ with ext-ffi enabled (ffi.enable=true in php.ini).
 *
 * Usage:
 *   $db = new Arkilian\Arkilian('myapp.sqlite');
 *   $db->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
 *   $db->run('INSERT INTO users (name) VALUES (?)', 'Alice');
 *   $rows = $db->all('SELECT * FROM users');
 *   $db->close();
 */

declare(strict_types=1);

namespace Arkilian;

use FFI;
use RuntimeException;

class Arkilian
{
    private static ?FFI $ffi = null;
    private ?FFI\CData $handle = null;
    private ?FFI\CData $rawSqlite = null;

    // SQLite constants
    private const SQLITE_OK      = 0;
    private const SQLITE_ROW     = 100;
    private const SQLITE_DONE    = 101;
    private const SQLITE_INTEGER = 1;
    private const SQLITE_FLOAT   = 2;
    private const SQLITE_TEXT    = 3;
    private const SQLITE_BLOB    = 4;
    private const SQLITE_NULL    = 5;

    private static function loadFFI(): FFI
    {
        if (self::$ffi !== null) {
            return self::$ffi;
        }

        $header = <<<CDEF
        // Arkilian API
        typedef struct arkilian arkilian;
        int db_init(arkilian **db, const char *filename);
        void db_close(arkilian *db);
        const char* db_errmsg(arkilian *db);
        void* db_get_handle(arkilian *db);

        // SQLite3 API (subset)
        int sqlite3_exec(void *db, const char *sql, void *cb, void *arg, char **errmsg);
        int sqlite3_prepare_v2(void *db, const char *sql, int nByte, void **stmt, void **tail);
        int sqlite3_step(void *stmt);
        int sqlite3_finalize(void *stmt);
        int sqlite3_column_count(void *stmt);
        const char* sqlite3_column_name(void *stmt, int col);
        int sqlite3_column_type(void *stmt, int col);
        long long sqlite3_column_int64(void *stmt, int col);
        double sqlite3_column_double(void *stmt, int col);
        const char* sqlite3_column_text(void *stmt, int col);
        const void* sqlite3_column_blob(void *stmt, int col);
        int sqlite3_column_bytes(void *stmt, int col);
        int sqlite3_bind_null(void *stmt, int idx);
        int sqlite3_bind_int64(void *stmt, int idx, long long val);
        int sqlite3_bind_double(void *stmt, int idx, double val);
        int sqlite3_bind_text(void *stmt, int idx, const char *val, int n, void *destructor);
        int sqlite3_bind_blob(void *stmt, int idx, const void *val, int n, void *destructor);
        int sqlite3_changes(void *db);
        long long sqlite3_last_insert_rowid(void *db);
        const char* sqlite3_errmsg(void *db);
        void sqlite3_free(void *ptr);
        CDEF;

        // Find the shared library
        $libName = match (PHP_OS_FAMILY) {
            'Darwin' => 'libarkilian.dylib',
            'Windows' => 'arkilian.dll',
            default => 'libarkilian.so',
        };

        // Search paths
        $searchDirs = [
            __DIR__ . '/../../build/Release',
            __DIR__ . '/../../build',
            __DIR__ . '/../lib',
        ];

        foreach ($searchDirs as $dir) {
            $path = "$dir/$libName";
            if (file_exists($path)) {
                self::$ffi = FFI::cdef($header, $path);
                return self::$ffi;
            }
        }

        // Try system library
        self::$ffi = FFI::cdef($header, $libName);
        return self::$ffi;
    }

    public function __construct(string $filename = 'app.sqlite')
    {
        $ffi = self::loadFFI();

        // Allocate pointer-to-pointer for db_init
        $this->handle = $ffi->new('arkilian*');
        $rc = $ffi->db_init(FFI::addr($this->handle), $filename);

        if ($rc !== 0) {
            $msg = FFI::string($ffi->db_errmsg($this->handle));
            $ffi->db_close($this->handle);
            $this->handle = null;
            throw new RuntimeException("Arkilian init failed: {$msg}");
        }

        $this->rawSqlite = $ffi->db_get_handle($this->handle);
    }

    private function ensureOpen(): void
    {
        if ($this->handle === null) {
            throw new RuntimeException('Database is closed');
        }
    }

    /**
     * Bind parameters to a prepared statement.
     */
    private function bindParams(FFI\CData $stmt, array $params): void
    {
        $ffi = self::$ffi;
        // SQLITE_TRANSIENT = (void*)-1
        $transient = FFI::cast('void*', FFI::new('long long'));

        foreach ($params as $i => $val) {
            $idx = $i + 1;
            if ($val === null) {
                $ffi->sqlite3_bind_null($stmt, $idx);
            } elseif (is_bool($val)) {
                $ffi->sqlite3_bind_int64($stmt, $idx, $val ? 1 : 0);
            } elseif (is_int($val)) {
                $ffi->sqlite3_bind_int64($stmt, $idx, $val);
            } elseif (is_float($val)) {
                $ffi->sqlite3_bind_double($stmt, $idx, $val);
            } elseif (is_string($val)) {
                // Use SQLITE_TRANSIENT (-1) so SQLite copies the string
                $ffi->sqlite3_bind_text($stmt, $idx, $val, strlen($val), FFI::cast('void*', -1));
            } else {
                throw new RuntimeException("Unsupported parameter type: " . gettype($val));
            }
        }
    }

    /**
     * Read a single column value from a stepped statement.
     */
    private function readColumn(FFI\CData $stmt, int $col): mixed
    {
        $ffi = self::$ffi;
        $type = $ffi->sqlite3_column_type($stmt, $col);

        return match ($type) {
            self::SQLITE_INTEGER => $ffi->sqlite3_column_int64($stmt, $col),
            self::SQLITE_FLOAT   => $ffi->sqlite3_column_double($stmt, $col),
            self::SQLITE_TEXT    => FFI::string($ffi->sqlite3_column_text($stmt, $col)),
            self::SQLITE_BLOB    => FFI::string(
                $ffi->sqlite3_column_blob($stmt, $col),
                $ffi->sqlite3_column_bytes($stmt, $col)
            ),
            default => null,
        };
    }

    /**
     * Execute one or more SQL statements (no parameters).
     *
     * @return array{changes: int, lastInsertRowid: int}
     */
    public function exec(string $sql): array
    {
        $this->ensureOpen();
        $ffi = self::$ffi;

        $errMsg = $ffi->new('char*');
        $rc = $ffi->sqlite3_exec($this->rawSqlite, $sql, null, null, FFI::addr($errMsg));

        if ($rc !== self::SQLITE_OK) {
            $msg = $errMsg !== null ? FFI::string($errMsg) : 'Unknown error';
            $ffi->sqlite3_free($errMsg);
            throw new RuntimeException("SQL error: {$msg}");
        }

        return [
            'changes' => $ffi->sqlite3_changes($this->rawSqlite),
            'lastInsertRowid' => $ffi->sqlite3_last_insert_rowid($this->rawSqlite),
        ];
    }

    /**
     * Execute a single parameterized statement (INSERT, UPDATE, DELETE).
     *
     * @return array{changes: int, lastInsertRowid: int}
     */
    public function run(string $sql, mixed ...$params): array
    {
        $this->ensureOpen();
        $ffi = self::$ffi;

        $stmt = $ffi->new('void*');
        $rc = $ffi->sqlite3_prepare_v2($this->rawSqlite, $sql, -1, FFI::addr($stmt), null);

        if ($rc !== self::SQLITE_OK) {
            $msg = FFI::string($ffi->sqlite3_errmsg($this->rawSqlite));
            throw new RuntimeException("Prepare failed: {$msg}");
        }

        try {
            $this->bindParams($stmt, $params);
            $rc = $ffi->sqlite3_step($stmt);

            if ($rc !== self::SQLITE_DONE && $rc !== self::SQLITE_ROW) {
                $msg = FFI::string($ffi->sqlite3_errmsg($this->rawSqlite));
                throw new RuntimeException("Step failed: {$msg}");
            }
        } finally {
            $ffi->sqlite3_finalize($stmt);
        }

        return [
            'changes' => $ffi->sqlite3_changes($this->rawSqlite),
            'lastInsertRowid' => $ffi->sqlite3_last_insert_rowid($this->rawSqlite),
        ];
    }

    /**
     * Execute a parameterized query and return all rows.
     *
     * @return array<int, array<string, mixed>>
     */
    public function all(string $sql, mixed ...$params): array
    {
        $this->ensureOpen();
        $ffi = self::$ffi;

        $stmt = $ffi->new('void*');
        $rc = $ffi->sqlite3_prepare_v2($this->rawSqlite, $sql, -1, FFI::addr($stmt), null);

        if ($rc !== self::SQLITE_OK) {
            $msg = FFI::string($ffi->sqlite3_errmsg($this->rawSqlite));
            throw new RuntimeException("Prepare failed: {$msg}");
        }

        try {
            $this->bindParams($stmt, $params);
            $colCount = $ffi->sqlite3_column_count($stmt);

            // Get column names
            $colNames = [];
            for ($c = 0; $c < $colCount; $c++) {
                $colNames[] = FFI::string($ffi->sqlite3_column_name($stmt, $c));
            }

            $rows = [];
            while (($rc = $ffi->sqlite3_step($stmt)) === self::SQLITE_ROW) {
                $row = [];
                for ($c = 0; $c < $colCount; $c++) {
                    $row[$colNames[$c]] = $this->readColumn($stmt, $c);
                }
                $rows[] = $row;
            }

            if ($rc !== self::SQLITE_DONE) {
                $msg = FFI::string($ffi->sqlite3_errmsg($this->rawSqlite));
                throw new RuntimeException("Query failed: {$msg}");
            }

            return $rows;
        } finally {
            $ffi->sqlite3_finalize($stmt);
        }
    }

    /**
     * Get the last error message.
     */
    public function errorMessage(): string
    {
        if ($this->handle !== null) {
            return FFI::string(self::$ffi->db_errmsg($this->handle));
        }
        return 'Database closed';
    }

    /**
     * Close the database. Safe to call multiple times.
     */
    public function close(): void
    {
        if ($this->handle !== null) {
            self::$ffi->db_close($this->handle);
            $this->handle = null;
            $this->rawSqlite = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}
