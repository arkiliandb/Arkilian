#include <napi.h>
#include <cstring>
#include <cstdio>
#include <map>
#include "class.h"

static std::map<void*, arkilian*> db_map;

static arkilian* getDbFromArg(const Napi::CallbackInfo& info) {
  if (info.Length() < 1 || !info[0].IsNumber()) {
    return nullptr;
  }
  int64_t id = info[0].As<Napi::Number>().Int64Value();
  void* ptr = reinterpret_cast<void*>(id);
  auto it = db_map.find(ptr);
  return (it != db_map.end()) ? it->second : nullptr;
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
  
  void* id = static_cast<void*>(db);
  db_map[id] = db;
  
  return Napi::Number::New(env, reinterpret_cast<int64_t>(id));
}

Napi::Value db_close(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "Number expected for database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int64_t id = info[0].As<Napi::Number>().Int64Value();
  void* ptr = reinterpret_cast<void*>(id);
  
  auto it = db_map.find(ptr);
  if (it != db_map.end()) {
    db_close(it->second);
    db_map.erase(it);
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
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  return Napi::Number::New(env, db_column_count(db));
}

Napi::Value db_column_name(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 2 || !info[1].IsNumber()) {
    Napi::TypeError::New(env, "Number expected for column index").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int col = info[1].As<Napi::Number>().Int32Value();
  const char* name = db_column_name(db, col);
  
  return name ? Napi::String::New(env, name) : env.Null();
}

Napi::Value db_column_text(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 2 || !info[1].IsNumber()) {
    Napi::TypeError::New(env, "Number expected for column index").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int col = info[1].As<Napi::Number>().Int32Value();
  const char* text = db_column_text(db, col);
  
  return text ? Napi::String::New(env, text) : env.Null();
}

Napi::Value db_column_int(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 2 || !info[1].IsNumber()) {
    Napi::TypeError::New(env, "Number expected for column index").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int col = info[1].As<Napi::Number>().Int32Value();
  int value = db_column_int(db, col);
  
  return Napi::Number::New(env, value);
}

Napi::Value db_column_double(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 2 || !info[1].IsNumber()) {
    Napi::TypeError::New(env, "Number expected for column index").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int col = info[1].As<Napi::Number>().Int32Value();
  double value = db_column_double(db, col);
  
  return Napi::Number::New(env, value);
}

Napi::Value db_bind_text(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 3 || !info[1].IsNumber() || !info[2].IsString()) {
    Napi::TypeError::New(env, "Number index and String value expected").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int idx = info[1].As<Napi::Number>().Int32Value();
  std::string val = info[2].As<Napi::String>().Utf8Value();
  int result = db_bind_text(db, idx, val.c_str());
  
  return Napi::Number::New(env, result);
}

Napi::Value db_bind_int(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 3 || !info[1].IsNumber() || !info[2].IsNumber()) {
    Napi::TypeError::New(env, "Number index and Number value expected").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int idx = info[1].As<Napi::Number>().Int32Value();
  int val = info[2].As<Napi::Number>().Int32Value();
  int result = db_bind_int(db, idx, val);
  
  return Napi::Number::New(env, result);
}

Napi::Value db_bind_double(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  if (info.Length() < 3 || !info[1].IsNumber() || !info[2].IsNumber()) {
    Napi::TypeError::New(env, "Number index and Number value expected").ThrowAsJavaScriptException();
    return env.Null();
  }
  
  int idx = info[1].As<Napi::Number>().Int32Value();
  double val = info[2].As<Napi::Number>().DoubleValue();
  int result = db_bind_double(db, idx, val);
  
  return Napi::Number::New(env, result);
}

Napi::Value db_errmsg(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  arkilian* db = getDbFromArg(info);
  
  if (!db) {
    return env.Null();
  }
  
  const char* msg = db_errmsg(db);
  
  return msg ? Napi::String::New(env, msg) : env.Null();
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  exports.Set("db_init", Napi::Function::New<db_init>(env));
  exports.Set("db_close", Napi::Function::New<db_close>(env));
  exports.Set("db_exec", Napi::Function::New<db_exec>(env));
  exports.Set("db_prepare", Napi::Function::New<db_prepare>(env));
  exports.Set("db_step", Napi::Function::New<db_step>(env));
  exports.Set("db_finalize", Napi::Function::New<db_finalize>(env));
  exports.Set("db_reset", Napi::Function::New<db_reset>(env));
  exports.Set("db_column_count", Napi::Function::New<db_column_count>(env));
  exports.Set("db_column_name", Napi::Function::New<db_column_name>(env));
  exports.Set("db_column_text", Napi::Function::New<db_column_text>(env));
  exports.Set("db_column_int", Napi::Function::New<db_column_int>(env));
  exports.Set("db_column_double", Napi::Function::New<db_column_double>(env));
  exports.Set("db_bind_text", Napi::Function::New<db_bind_text>(env));
  exports.Set("db_bind_int", Napi::Function::New<db_bind_int>(env));
  exports.Set("db_bind_double", Napi::Function::New<db_bind_double>(env));
  exports.Set("db_errmsg", Napi::Function::New<db_errmsg>(env));
  
  exports.Set("SQLITE_OK", Napi::Number::New(env, 0));
  exports.Set("SQLITE_ROW", Napi::Number::New(env, 100));
  exports.Set("SQLITE_DONE", Napi::Number::New(env, 101));
  exports.Set("SQLITE_ERROR", Napi::Number::New(env, 1));
  
  return exports;
}

NODE_API_MODULE(arkilian, Init)