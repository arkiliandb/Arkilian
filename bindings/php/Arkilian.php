<?php
/**
 * Arkilian - PHP FFI bindings for SQLite with cloud backup
 */

define('SQLITE_OK', 0);
define('SQLITE_ROW', 100);
define('SQLITE_DONE', 101);
define('SQLITE_ERROR', 1);

class Arkilian {
    private $db;
    private $ffi;
    private $isOpen = false;

    public function __construct(string $dbPath = 'app.sqlite', ?string $token = null) {
        $this->ffi = $this->loadFFI();
        
        $dbPtr = $this->ffi->new('arkilian*');
        $result = $this->ffi->db_init(\FFI::addr($dbPtr), $dbPath);
        
        if ($result !== SQLITE_OK) {
            $error = $this->lastError();
            throw new RuntimeException("Failed to initialize database: " . $error);
        }
        
        $this->db = $dbPtr;
        $this->isOpen = true;

        if ($token !== null) {
            $this->setToken($token);
        }
    }

    private function loadFFI(): FFI {
        $libPath = $this->findLibrary();
        
        if (!file_exists($libPath)) {
            throw new RuntimeException("Library not found at: " . $libPath);
        }

        $cdef = "
            typedef struct arkilian arkilian;
            
            int db_init(arkilian **db, const char *connection_url);
            void db_close(arkilian *db);
            const char* db_errmsg(arkilian *db);
            int db_set_token(arkilian *db, const char *token);
            
            int db_exec(arkilian *db, const char *sql);
            int db_prepare(arkilian *db, const char *sql);
            int db_use_stmt(arkilian *db, int index);
            int db_stmt_count(arkilian *db);
            int db_step(arkilian *db);
            int db_finalize(arkilian *db);
            int db_reset(arkilian *db);
            int db_column_count(arkilian *db);
            const char* db_column_name(arkilian *db, int col);
            const char* db_column_text(arkilian *db, int col);
            int db_column_int(arkilian *db, int col);
            double db_column_double(arkilian *db, int col);
            int db_bind_text(arkilian *db, int idx, const char *val);
            int db_bind_int(arkilian *db, int idx, int val);
            int db_bind_double(arkilian *db, int idx, double val);
        ";

        $ffi = FFI::cdef($cdef, $libPath);
        
        return $ffi;
    }

    private function findLibrary(): string {
        $paths = [
            __DIR__ . '/../../build/Release/libarkilian.dylib',
            __DIR__ . '/../../build/Release/libarkilian.so',
            '/usr/local/lib/libarkilian.dylib',
            '/usr/lib/libarkilian.so',
        ];

        foreach ($paths as $path) {
            if (file_exists($path)) {
                return $path;
            }
        }

        return $paths[0];
    }

    public function close(): void {
        if ($this->isOpen && $this->db !== null) {
            $this->ffi->db_close($this->db);
            $this->db = null;
            $this->isOpen = false;
        }
    }

    public function setToken(string $token): self {
        $result = $this->ffi->db_set_token($this->db, $token);
        
        if ($result !== SQLITE_OK) {
            throw new RuntimeException("Failed to set account token");
        }
        
        return $this;
    }

    public function exec(string $sql): int {
        $result = $this->ffi->db_exec($this->db, $sql);
        
        if ($result !== SQLITE_OK && $result !== SQLITE_DONE) {
            throw new RuntimeException($this->lastError());
        }
        
        return $result;
    }

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

    public function columnCount(): int {
        return $this->ffi->db_column_count($this->db);
    }

    public function columnName(int $col): ?string {
        $result = $this->ffi->db_column_name($this->db, $col);
        return $result !== null && strlen($result) > 0 ? $result : null;
    }

    public function columnText(int $col): ?string {
        $result = $this->ffi->db_column_text($this->db, $col);
        return $result !== null && strlen($result) > 0 ? $result : null;
    }

    public function columnInt(int $col): int {
        return $this->ffi->db_column_int($this->db, $col);
    }

    public function columnDouble(int $col): float {
        return $this->ffi->db_column_double($this->db, $col);
    }

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

    public function bindDouble(int $idx, float $value): self {
        $result = $this->ffi->db_bind_double($this->db, $idx, $value);
        
        if ($result !== SQLITE_OK) {
            throw new RuntimeException($this->lastError());
        }
        
        return $this;
    }

    public function run(string $sql, array $params = []): self {
        $this->prepare($sql);
        
        foreach ($params as $i => $param) {
            $idx = $i + 1;
            if (is_string($param)) {
                $this->bindText($idx, $param);
            } elseif (is_int($param)) {
                $this->bindInt($idx, $param);
            } elseif (is_float($param)) {
                $this->bindDouble($idx, $param);
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
            if (is_string($param)) {
                $this->bindText($idx, $param);
            } elseif (is_int($param)) {
                $this->bindInt($idx, $param);
            } elseif (is_float($param)) {
                $this->bindDouble($idx, $param);
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
                $row[$columns[$i]] = $this->columnText($i);
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
        return $result !== null && strlen($result) > 0 ? $result : "";
    }

    public function __destruct() {
        $this->close();
    }
}