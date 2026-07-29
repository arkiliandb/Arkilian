#include <napi.h>
#include <cstring>
#include <cstdio>
#include "class.h"

// Direct pointer conversion — zero-overhead handle lookup
static inline arkilian* getDbFromArg(const Napi::CallbackInfo& info) {
  if (info.Length() < 1 || !info[0].IsNumber()) return nullptr;
  int64_t id = info[0].As<Napi::Number>().Int64Value();
  return reinterpret_cast<arkilian*>(id);
}

Napi::Value db_init(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    Napi::TypeError::New(env, "String expected for database path").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  std::string path = info[0].As<Napi::String>().Utf8Value();
  arkilian* db = nullptr;
  
  int result = db_init(&db, path.c_str());
  if (result != 0 || db == nullptr) {
    Napi::Error::New(env, db ? db_errmsg(db) : "Failed to initialize database").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  return Napi::Number::New(env, reinterpret_cast<int64_t>(db));
}

Napi::Value db_close(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (db) {
    db_close(db);
  }
  return env.Null();
}

Napi::Value db_exec(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  if (info.Length() < 2 || !info[1].IsString()) {
    Napi::TypeError::New(env, "String expected for SQL").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  std::string sql = info[1].As<Napi::String>().Utf8Value();
  int result = db_exec(db, sql.c_str());
  return Napi::Number::New(env, result);
}

Napi::Value db_prepare(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  if (info.Length() < 2 || !info[1].IsString()) {
    Napi::TypeError::New(env, "String expected for SQL").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  std::string sql = info[1].As<Napi::String>().Utf8Value();
  int result = db_prepare(db, sql.c_str());
  return Napi::Number::New(env, result);
}

Napi::Value db_use_stmt(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) {
    Napi::Error::New(env, "Invalid database id or index").ThrowAsJavaScriptException();
    return env.Null();
  }
  int index = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_use_stmt(db, index));
}

Napi::Value db_stmt_count(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  return Napi::Number::New(env, db ? db_stmt_count(db) : 0);
}

Napi::Value db_step(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_step(db));
}

Napi::Value db_finalize(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_finalize(db));
}

Napi::Value db_reset(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_reset(db));
}

Napi::Value db_column_count(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) return Napi::Number::New(env, 0);
  return Napi::Number::New(env, db_column_count(db));
}

Napi::Value db_column_name(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) return env.Null();
  int col = info[1].As<Napi::Number>().Int32Value();
  const char* name = db_column_name(db, col);
  return name ? Napi::String::New(env, name) : env.Null();
}

Napi::Value db_column_text(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) return env.Null();
  int col = info[1].As<Napi::Number>().Int32Value();
  const char* text = db_column_text(db, col);
  return text ? Napi::String::New(env, text) : env.Null();
}

Napi::Value db_column_int(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 0);
  int col = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_column_int(db, col));
}

Napi::Value db_column_double(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 0.0);
  int col = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_column_double(db, col));
}

Napi::Value db_column_type(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 5); // SQLITE_NULL
  int col = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_column_type(db, col));
}

Napi::Value db_bind_text(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 3 || !info[1].IsNumber() || !info[2].IsString()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  std::string val = info[2].As<Napi::String>().Utf8Value();
  return Napi::Number::New(env, db_bind_text(db, idx, val.c_str()));
}

Napi::Value db_bind_int(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 3 || !info[1].IsNumber() || !info[2].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  int val = info[2].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_bind_int(db, idx, val));
}

Napi::Value db_bind_double(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 3 || !info[1].IsNumber() || !info[2].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  double val = info[2].As<Napi::Number>().DoubleValue();
  return Napi::Number::New(env, db_bind_double(db, idx, val));
}

Napi::Value db_bind_null(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_bind_null(db, idx));
}

Napi::Value db_begin(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  return Napi::Number::New(env, db ? db_begin(db) : SQLITE_ERROR);
}

Napi::Value db_commit(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  return Napi::Number::New(env, db ? db_commit(db) : SQLITE_ERROR);
}

Napi::Value db_rollback(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  return Napi::Number::New(env, db ? db_rollback(db) : SQLITE_ERROR);
}

Napi::Value db_errmsg(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) return env.Null();
  const char* msg = db_errmsg(db);
  return msg ? Napi::String::New(env, msg) : env.Null();
}

Napi::Value db_set_token(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db || info.Length() < 2 || !info[1].IsString()) return env.Null();
  std::string token = info[1].As<Napi::String>().Utf8Value();
  return Napi::Number::New(env, db_set_token(db, token.c_str()));
}

Napi::Value db_changes(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  return Napi::Number::New(env, db ? db_changes(db) : 0);
}

Napi::Value db_last_insert_rowid(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  return Napi::Number::New(env, db ? db_last_insert_rowid(db) : 0);
}

// ── Native Fast Path: Single N-API Turn Query Execution ─────────────

Napi::Value db_all_native(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }

  int col_count = db_column_count(db);
  Napi::Array results = Napi::Array::New(env);
  uint32_t row_idx = 0;

  std::vector<std::string> col_names(col_count);
  for (int i = 0; i < col_count; i++) {
    const char *name = db_column_name(db, i);
    col_names[i] = name ? name : "";
  }

  while (db_step(db) == SQLITE_ROW) {
    Napi::Object row = Napi::Object::New(env);
    for (int i = 0; i < col_count; i++) {
      int type = db_column_type(db, i);
      if (type == SQLITE_INTEGER) {
        row.Set(col_names[i], Napi::Number::New(env, db_column_int64(db, i)));
      } else if (type == SQLITE_FLOAT) {
        row.Set(col_names[i], Napi::Number::New(env, db_column_double(db, i)));
      } else if (type == SQLITE_TEXT) {
        const char *txt = db_column_text(db, i);
        row.Set(col_names[i], txt ? Napi::String::New(env, txt) : env.Null());
      } else if (type == SQLITE_NULL) {
        row.Set(col_names[i], env.Null());
      } else {
        const char *txt = db_column_text(db, i);
        row.Set(col_names[i], txt ? Napi::String::New(env, txt) : env.Null());
      }
    }
    results.Set(row_idx++, row);
  }

  db_finalize(db);
  return results;
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  exports.Set("db_init", Napi::Function::New<db_init>(env));
  exports.Set("db_close", Napi::Function::New<db_close>(env));
  exports.Set("db_exec", Napi::Function::New<db_exec>(env));
  exports.Set("db_prepare", Napi::Function::New<db_prepare>(env));
  exports.Set("db_use_stmt", Napi::Function::New<db_use_stmt>(env));
  exports.Set("db_stmt_count", Napi::Function::New<db_stmt_count>(env));
  exports.Set("db_step", Napi::Function::New<db_step>(env));
  exports.Set("db_finalize", Napi::Function::New<db_finalize>(env));
  exports.Set("db_reset", Napi::Function::New<db_reset>(env));
  exports.Set("db_column_count", Napi::Function::New<db_column_count>(env));
  exports.Set("db_column_name", Napi::Function::New<db_column_name>(env));
  exports.Set("db_column_text", Napi::Function::New<db_column_text>(env));
  exports.Set("db_column_int", Napi::Function::New<db_column_int>(env));
  exports.Set("db_column_double", Napi::Function::New<db_column_double>(env));
  exports.Set("db_column_type", Napi::Function::New<db_column_type>(env));
  exports.Set("db_bind_text", Napi::Function::New<db_bind_text>(env));
  exports.Set("db_bind_int", Napi::Function::New<db_bind_int>(env));
  exports.Set("db_bind_double", Napi::Function::New<db_bind_double>(env));
  exports.Set("db_bind_null", Napi::Function::New<db_bind_null>(env));
  exports.Set("db_begin", Napi::Function::New<db_begin>(env));
  exports.Set("db_commit", Napi::Function::New<db_commit>(env));
  exports.Set("db_rollback", Napi::Function::New<db_rollback>(env));
  exports.Set("db_errmsg", Napi::Function::New<db_errmsg>(env));
  exports.Set("db_set_token", Napi::Function::New<db_set_token>(env));
  exports.Set("db_changes", Napi::Function::New<db_changes>(env));
  exports.Set("db_last_insert_rowid", Napi::Function::New<db_last_insert_rowid>(env));
  exports.Set("db_all_native", Napi::Function::New<db_all_native>(env));
  
  exports.Set("SQLITE_OK", Napi::Number::New(env, 0));
  exports.Set("SQLITE_ROW", Napi::Number::New(env, 100));
  exports.Set("SQLITE_DONE", Napi::Number::New(env, 101));
  exports.Set("SQLITE_ERROR", Napi::Number::New(env, 1));
  
  return exports;
}

NODE_API_MODULE(arkilian, Init)