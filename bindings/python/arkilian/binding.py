import os
import sys
import cffi

ffi = cffi.FFI()

ffi.cdef("""
typedef struct arkilian arkilian;

int db_init(arkilian **db, const char *connection_url);
void db_close(arkilian *db);
const char* db_errmsg(arkilian *db);

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

if sys.platform == "darwin":
    lib_names = ["libarkilian.dylib", "libarkilian.1.dylib", "libarkilian.1.0.0.dylib"]
elif sys.platform == "win32":
    lib_names = ["arkilian.dll", "libarkilian.dll"]
else:
    lib_names = ["libarkilian.so", "libarkilian.so.1", "libarkilian.1.0.0.so"]

candidate_paths = []

# 1. Explicit env var override
if "ARKILIAN_LIB_PATH" in os.environ and os.path.exists(os.environ["ARKILIAN_LIB_PATH"]):
    candidate_paths.append(os.environ["ARKILIAN_LIB_PATH"])

# 2. Bundled inside package directory (e.g. from binary wheel)
for name in lib_names:
    candidate_paths.append(os.path.join(this_dir, name))
    candidate_paths.append(os.path.join(this_dir, "lib", name))

# 3. Standard build output directories (dev/repo build)
repo_build_dirs = [
    os.path.join(this_dir, "..", "..", "..", "build"),
    os.path.join(this_dir, "..", "..", "..", "build", "Release"),
    os.path.join(this_dir, "..", "..", "..", "build-c"),
    os.path.join(this_dir, "..", "..", "..", "cmake-build-release"),
    os.path.join(this_dir, "..", "..", "build"),
    os.path.join(this_dir, "..", "build"),
]
for b_dir in repo_build_dirs:
    for name in lib_names:
        candidate_paths.append(os.path.join(b_dir, name))

# 4. Standard system search paths
system_dirs = ["/usr/local/lib", "/usr/lib", "/opt/homebrew/lib", "/lib"]
for s_dir in system_dirs:
    for name in lib_names:
        candidate_paths.append(os.path.join(s_dir, name))

resolved_path = None
for p in candidate_paths:
    if os.path.exists(p):
        resolved_path = p
        break

if not resolved_path:
    # Try system loader lookup as final fallback
    for name in lib_names:
        try:
            lib = ffi.dlopen(name)
            resolved_path = name
            break
        except Exception:
            pass

if not resolved_path:
    raise RuntimeError(
        f"Arkilian shared library not found ({', '.join(lib_names)}). "
        "Set ARKILIAN_LIB_PATH or compile via 'cmake --build build'."
    )

if "lib" not in locals():
    lib = ffi.dlopen(resolved_path)

__all__ = ["ffi", "lib"]