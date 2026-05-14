import os
import sys
import cffi

ffi = cffi.FFI()

ffi.cdef("""
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
""")

this_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(this_dir, "..", "..", "..", "build")

lib_name = "libarkilian.dylib" if sys.platform == "darwin" else "libarkilian.so"
lib_path = os.path.join(src_dir, lib_name)

if not os.path.exists(lib_path):
    lib_path = os.path.join(src_dir, "Release", lib_name)

if not os.path.exists(lib_path):
    lib_path = os.path.join(src_dir, "Release", "libarkilian.1.0.0.dylib")

if not os.path.exists(lib_path):
    lib_path = os.path.join(src_dir, "Release", "libarkilian.1.dylib")

if not os.path.exists(lib_path):
    lib_path = os.path.join(src_dir, "Release", "libarkilian.1.0.0.so")

if not os.path.exists(lib_path):
    raise RuntimeError(f"Library not found at {lib_path}")

lib = ffi.dlopen(lib_path)

__all__ = ["ffi", "lib"]