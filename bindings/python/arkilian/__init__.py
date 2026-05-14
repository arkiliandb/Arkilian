from .binding import lib, ffi

SQLITE_OK = 0
SQLITE_ROW = 100
SQLITE_DONE = 101
SQLITE_ERROR = 1


class Arkilian:
    def __init__(self, db_path="app.sqlite", token=None):
        self._db = ffi.new("arkilian**")
        result = lib.db_init(self._db, db_path.encode())
        if result != 0:
            raise RuntimeError("Failed to initialize database: " + self.last_error)
        if self._db[0] is None:
            raise RuntimeError("Database handle is null")
        if token is not None:
            self.set_token(token)

    def set_token(self, token):
        result = lib.db_set_token(self._db[0], token.encode())
        if result != 0:
            raise RuntimeError("Failed to set account token")
        return self

    def close(self):
        if self._db and self._db[0]:
            lib.db_close(self._db[0])
            self._db = None

    def exec(self, sql):
        result = lib.db_exec(self._db[0], sql.encode())
        if result != SQLITE_OK and result != SQLITE_DONE:
            raise RuntimeError(self.last_error)
        return result

    def prepare(self, sql):
        result = lib.db_prepare(self._db[0], sql.encode())
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def use_stmt(self, index):
        """Switch the active statement by index."""
        result = lib.db_use_stmt(self._db[0], index)
        if result != SQLITE_OK:
            raise RuntimeError("Invalid statement index or statement already finalized")
        return self

    def stmt_count(self):
        """Return the number of prepared statements in the pool."""
        return lib.db_stmt_count(self._db[0])

    def step(self):
        return lib.db_step(self._db[0])

    def finalize(self):
        result = lib.db_finalize(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def reset(self):
        result = lib.db_reset(self._db[0])
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def column_count(self):
        return lib.db_column_count(self._db[0])

    def column_name(self, col):
        return ffi.string(lib.db_column_name(self._db[0], col)).decode()

    def column_text(self, col):
        ptr = lib.db_column_text(self._db[0], col)
        return ffi.string(ptr).decode() if ptr else None

    def column_int(self, col):
        return lib.db_column_int(self._db[0], col)

    def column_double(self, col):
        return lib.db_column_double(self._db[0], col)

    def bind_text(self, idx, value):
        result = lib.db_bind_text(self._db[0], idx, value.encode())
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_int(self, idx, value):
        result = lib.db_bind_int(self._db[0], idx, value)
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def bind_double(self, idx, value):
        result = lib.db_bind_double(self._db[0], idx, value)
        if result != SQLITE_OK:
            raise RuntimeError(self.last_error)
        return self

    def run(self, sql, params=None):
        self.prepare(sql)
        if params:
            for i, p in enumerate(params):
                if isinstance(p, str):
                    self.bind_text(i + 1, p)
                elif isinstance(p, int):
                    self.bind_int(i + 1, p)
                else:
                    self.bind_double(i + 1, p)
        self.step()
        self.finalize()
        return self

    def all(self, sql, params=None):
        results = []
        self.prepare(sql)
        if params:
            for i, p in enumerate(params):
                if isinstance(p, str):
                    self.bind_text(i + 1, p)
                elif isinstance(p, int):
                    self.bind_int(i + 1, p)
                else:
                    self.bind_double(i + 1, p)
        columns = [self.column_name(i) for i in range(self.column_count())]
        while self.step() == SQLITE_ROW:
            row = {}
            for i, col in enumerate(columns):
                row[col] = self.column_text(i)
            results.append(row)
        self.finalize()
        return results

    @property
    def last_error(self):
        return ffi.string(lib.db_errmsg(self._db[0])).decode() if self._db and self._db[0] else ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False