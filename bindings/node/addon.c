// Arkilian N-API Addon
// Exposes Arkilian's C API to Node.js and Bun via N-API (stable ABI)

#define NAPI_VERSION 8
#include <node_api.h>
#include "class.h"
#include "deps/sqlite/sqlite3.h"
#include <stdlib.h>
#include <string.h>

// ---------------------------------------------------------------------------
// Wrapper struct — prevents double-free when both close() and GC fire
// ---------------------------------------------------------------------------
typedef struct {
  arkilian *db;
  int closed;
} ak_wrap;

static void ak_destructor(napi_env env, void *data, void *hint) {
  (void)env;
  (void)hint;
  ak_wrap *w = (ak_wrap *)data;
  if (!w->closed && w->db) {
    db_close(w->db);
  }
  free(w);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
#define AK_THROW(env, msg)                    \
  do {                                        \
    napi_throw_error((env), NULL, (msg));     \
    return NULL;                              \
  } while (0)

#define AK_STATUS_CHECK(env, status)          \
  do {                                        \
    if ((status) != napi_ok) {                \
      AK_THROW((env), "N-API call failed");   \
    }                                         \
  } while (0)

// Extract the ak_wrap* from the first argument (the external handle)
static ak_wrap *ak_unwrap(napi_env env, napi_value external) {
  void *data;
  napi_status s = napi_get_value_external(env, external, &data);
  if (s != napi_ok || !data) {
    napi_throw_error(env, NULL, "Invalid database handle");
    return NULL;
  }
  ak_wrap *w = (ak_wrap *)data;
  if (w->closed) {
    napi_throw_error(env, NULL, "Database is already closed");
    return NULL;
  }
  return w;
}

// Bind a JS array of parameters to a sqlite3_stmt
static int ak_bind_params(napi_env env, sqlite3_stmt *stmt, napi_value params) {
  bool is_array;
  napi_is_array(env, params, &is_array);
  if (!is_array) return 0; // no params to bind

  uint32_t len;
  napi_get_array_length(env, params, &len);

  for (uint32_t i = 0; i < len; i++) {
    napi_value val;
    napi_get_element(env, params, i, &val);

    napi_valuetype vtype;
    napi_typeof(env, val, &vtype);

    int idx = (int)(i + 1); // sqlite params are 1-indexed

    switch (vtype) {
      case napi_null:
      case napi_undefined:
        sqlite3_bind_null(stmt, idx);
        break;

      case napi_boolean: {
        bool b;
        napi_get_value_bool(env, val, &b);
        sqlite3_bind_int(stmt, idx, b ? 1 : 0);
        break;
      }

      case napi_number: {
        // Check if integer or float
        int64_t i64;
        double d;
        napi_get_value_double(env, val, &d);
        napi_get_value_int64(env, val, &i64);
        if (d == (double)i64 && d >= -9007199254740991.0 && d <= 9007199254740991.0) {
          sqlite3_bind_int64(stmt, idx, i64);
        } else {
          sqlite3_bind_double(stmt, idx, d);
        }
        break;
      }

      case napi_string: {
        size_t len_str;
        napi_get_value_string_utf8(env, val, NULL, 0, &len_str);
        char *buf = malloc(len_str + 1);
        if (!buf) return -1;
        napi_get_value_string_utf8(env, val, buf, len_str + 1, &len_str);
        sqlite3_bind_text(stmt, idx, buf, (int)len_str, free);
        break;
      }

      case napi_bigint: {
        int64_t i64;
        bool lossless;
        napi_get_value_bigint_int64(env, val, &i64, &lossless);
        sqlite3_bind_int64(stmt, idx, i64);
        break;
      }

      default: {
        // Try buffer (blob)
        bool is_buffer;
        napi_is_buffer(env, val, &is_buffer);
        if (is_buffer) {
          void *buf_data;
          size_t buf_len;
          napi_get_buffer_info(env, val, &buf_data, &buf_len);
          sqlite3_bind_blob(stmt, idx, buf_data, (int)buf_len, SQLITE_TRANSIENT);
        } else {
          sqlite3_bind_null(stmt, idx);
        }
        break;
      }
    }
  }
  return 0;
}

// Read a single column value from a sqlite3_stmt into a napi_value
static napi_value ak_column_value(napi_env env, sqlite3_stmt *stmt, int col) {
  napi_value val;
  int col_type = sqlite3_column_type(stmt, col);

  switch (col_type) {
    case SQLITE_INTEGER: {
      int64_t i64 = sqlite3_column_int64(stmt, col);
      // Use number for safe-integer range, BigInt would lose interop
      napi_create_int64(env, i64, &val);
      break;
    }
    case SQLITE_FLOAT: {
      napi_create_double(env, sqlite3_column_double(stmt, col), &val);
      break;
    }
    case SQLITE_TEXT: {
      const char *text = (const char *)sqlite3_column_text(stmt, col);
      napi_create_string_utf8(env, text ? text : "", NAPI_AUTO_LENGTH, &val);
      break;
    }
    case SQLITE_BLOB: {
      const void *blob = sqlite3_column_blob(stmt, col);
      int blob_size = sqlite3_column_bytes(stmt, col);
      void *copy;
      napi_create_buffer_copy(env, (size_t)blob_size, blob, &copy, &val);
      break;
    }
    case SQLITE_NULL:
    default:
      napi_get_null(env, &val);
      break;
  }
  return val;
}

// ---------------------------------------------------------------------------
// arkilian_init(filename?: string) → External
// ---------------------------------------------------------------------------
static napi_value AkInit(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);

  char filename[2048];
  if (argc >= 1) {
    napi_valuetype t;
    napi_typeof(env, args[0], &t);
    if (t == napi_string) {
      size_t len;
      napi_get_value_string_utf8(env, args[0], filename, sizeof(filename), &len);
    } else {
      strcpy(filename, "app.sqlite");
    }
  } else {
    strcpy(filename, "app.sqlite");
  }

  ak_wrap *w = malloc(sizeof(ak_wrap));
  if (!w) AK_THROW(env, "Memory allocation failed");
  w->db = NULL;
  w->closed = 0;

  int rc = db_init(&w->db, filename);
  if (rc != 0) {
    const char *err = w->db ? db_errmsg(w->db) : "Memory allocation error";
    if (w->db) db_close(w->db);
    free(w);
    AK_THROW(env, err);
  }

  napi_value external;
  napi_status s = napi_create_external(env, w, ak_destructor, NULL, &external);
  if (s != napi_ok) {
    db_close(w->db);
    free(w);
    AK_THROW(env, "Failed to create external handle");
  }

  return external;
}

// ---------------------------------------------------------------------------
// arkilian_close(handle) → undefined
// ---------------------------------------------------------------------------
static napi_value AkClose(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  if (argc < 1) AK_THROW(env, "close() requires a database handle");

  void *data;
  napi_get_value_external(env, args[0], &data);
  if (!data) AK_THROW(env, "Invalid database handle");

  ak_wrap *w = (ak_wrap *)data;
  if (!w->closed && w->db) {
    db_close(w->db);
    w->db = NULL;
    w->closed = 1;
  }

  return NULL;
}

// ---------------------------------------------------------------------------
// arkilian_exec(handle, sql: string) → { changes, lastInsertRowid }
// Executes one or more SQL statements (no parameters).
// ---------------------------------------------------------------------------
static napi_value AkExec(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  if (argc < 2) AK_THROW(env, "exec() requires (handle, sql)");

  ak_wrap *w = ak_unwrap(env, args[0]);
  if (!w) return NULL;

  size_t sql_len;
  napi_get_value_string_utf8(env, args[1], NULL, 0, &sql_len);
  char *sql = malloc(sql_len + 1);
  if (!sql) AK_THROW(env, "Memory allocation failed");
  napi_get_value_string_utf8(env, args[1], sql, sql_len + 1, &sql_len);

  sqlite3 *raw = db_get_handle(w->db);
  char *err_msg = NULL;
  int rc = sqlite3_exec(raw, sql, NULL, NULL, &err_msg);

  if (rc != SQLITE_OK) {
    const char *msg = err_msg ? err_msg : sqlite3_errmsg(raw);
    // Copy message before freeing
    char errbuf[512];
    strncpy(errbuf, msg, sizeof(errbuf) - 1);
    errbuf[sizeof(errbuf) - 1] = '\0';
    if (err_msg) sqlite3_free(err_msg);
    free(sql);
    AK_THROW(env, errbuf);
  }

  free(sql);

  // Build result: { changes, lastInsertRowid }
  napi_value result, changes_val, rowid_val;
  napi_create_object(env, &result);
  napi_create_int32(env, sqlite3_changes(raw), &changes_val);
  napi_create_int64(env, sqlite3_last_insert_rowid(raw), &rowid_val);
  napi_set_named_property(env, result, "changes", changes_val);
  napi_set_named_property(env, result, "lastInsertRowid", rowid_val);

  return result;
}

// ---------------------------------------------------------------------------
// arkilian_run(handle, sql, params?: any[]) → { changes, lastInsertRowid }
// Executes a single parameterized statement (INSERT/UPDATE/DELETE).
// ---------------------------------------------------------------------------
static napi_value AkRun(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  if (argc < 2) AK_THROW(env, "run() requires (handle, sql [, params])");

  ak_wrap *w = ak_unwrap(env, args[0]);
  if (!w) return NULL;

  size_t sql_len;
  napi_get_value_string_utf8(env, args[1], NULL, 0, &sql_len);
  char *sql = malloc(sql_len + 1);
  if (!sql) AK_THROW(env, "Memory allocation failed");
  napi_get_value_string_utf8(env, args[1], sql, sql_len + 1, &sql_len);

  sqlite3 *raw = db_get_handle(w->db);
  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(raw, sql, (int)sql_len, &stmt, NULL);
  free(sql);

  if (rc != SQLITE_OK) {
    AK_THROW(env, sqlite3_errmsg(raw));
  }

  // Bind parameters if provided
  if (argc >= 3) {
    if (ak_bind_params(env, stmt, args[2]) != 0) {
      sqlite3_finalize(stmt);
      AK_THROW(env, "Failed to bind parameters");
    }
  }

  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
    AK_THROW(env, sqlite3_errmsg(raw));
  }

  napi_value result, changes_val, rowid_val;
  napi_create_object(env, &result);
  napi_create_int32(env, sqlite3_changes(raw), &changes_val);
  napi_create_int64(env, sqlite3_last_insert_rowid(raw), &rowid_val);
  napi_set_named_property(env, result, "changes", changes_val);
  napi_set_named_property(env, result, "lastInsertRowid", rowid_val);

  return result;
}

// ---------------------------------------------------------------------------
// arkilian_all(handle, sql, params?: any[]) → Array<Object>
// Executes a parameterized query and returns all rows.
// ---------------------------------------------------------------------------
static napi_value AkAll(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  if (argc < 2) AK_THROW(env, "all() requires (handle, sql [, params])");

  ak_wrap *w = ak_unwrap(env, args[0]);
  if (!w) return NULL;

  size_t sql_len;
  napi_get_value_string_utf8(env, args[1], NULL, 0, &sql_len);
  char *sql = malloc(sql_len + 1);
  if (!sql) AK_THROW(env, "Memory allocation failed");
  napi_get_value_string_utf8(env, args[1], sql, sql_len + 1, &sql_len);

  sqlite3 *raw = db_get_handle(w->db);
  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(raw, sql, (int)sql_len, &stmt, NULL);
  free(sql);

  if (rc != SQLITE_OK) {
    AK_THROW(env, sqlite3_errmsg(raw));
  }

  // Bind parameters
  if (argc >= 3) {
    if (ak_bind_params(env, stmt, args[2]) != 0) {
      sqlite3_finalize(stmt);
      AK_THROW(env, "Failed to bind parameters");
    }
  }

  // Build result array
  napi_value result_array;
  napi_create_array(env, &result_array);

  int col_count = sqlite3_column_count(stmt);
  uint32_t row_index = 0;

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    napi_value row;
    napi_create_object(env, &row);

    for (int c = 0; c < col_count; c++) {
      const char *col_name = sqlite3_column_name(stmt, c);
      napi_value key;
      napi_create_string_utf8(env, col_name, NAPI_AUTO_LENGTH, &key);

      napi_value val = ak_column_value(env, stmt, c);
      napi_set_property(env, row, key, val);
    }

    napi_set_element(env, result_array, row_index++, row);
  }

  sqlite3_finalize(stmt);

  if (rc != SQLITE_DONE) {
    AK_THROW(env, sqlite3_errmsg(raw));
  }

  return result_array;
}

// ---------------------------------------------------------------------------
// arkilian_errmsg(handle) → string
// ---------------------------------------------------------------------------
static napi_value AkErrmsg(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  if (argc < 1) AK_THROW(env, "errmsg() requires a database handle");

  ak_wrap *w = ak_unwrap(env, args[0]);
  if (!w) return NULL;

  const char *msg = db_errmsg(w->db);
  napi_value result;
  napi_create_string_utf8(env, msg ? msg : "Unknown error", NAPI_AUTO_LENGTH, &result);
  return result;
}

// ---------------------------------------------------------------------------
// Module initialization
// ---------------------------------------------------------------------------
static napi_value AkModuleInit(napi_env env, napi_value exports) {
  napi_property_descriptor props[] = {
    {"init",   NULL, AkInit,   NULL, NULL, NULL, napi_default, NULL},
    {"close",  NULL, AkClose,  NULL, NULL, NULL, napi_default, NULL},
    {"exec",   NULL, AkExec,   NULL, NULL, NULL, napi_default, NULL},
    {"run",    NULL, AkRun,    NULL, NULL, NULL, napi_default, NULL},
    {"all",    NULL, AkAll,    NULL, NULL, NULL, napi_default, NULL},
    {"errmsg", NULL, AkErrmsg, NULL, NULL, NULL, napi_default, NULL},
  };

  napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props);
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, AkModuleInit)
