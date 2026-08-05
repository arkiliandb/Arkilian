#include <napi.h>
#include <cstring>
#include <cstdio>
#include <cmath>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <string>
#include "class.h"
#include "hydration.h"

// ── Handle Registry ─────────────────────────────────────────────────
// Raw pointers are handed to JS as numbers.  Without a registry, any
// stale or crafted number is a use-after-free / wild-pointer deref.
// Every live handle is registered at init and removed at close; lookups
// validate membership AND acquire the per-handle mutex before the
// pointer is returned, so a concurrent db_close cannot free the struct
// between the lookup and the dereference (the UAF that the previous
// drop-the-lock-then-relock pattern had).

// g_registry_mutex protects BOTH g_registry AND g_stmt_mutexes (unified
// to a single mutex so there is no gap between registry lookup and
// per-handle mutex acquisition).
static std::mutex g_registry_mutex;
static std::unordered_set<arkilian*> g_registry;

// ── Per-handle statement cursor mutex ───────────────────────────────
// The C layer keeps ONE "current statement" cursor per handle
// (stmt_current). Node worker_threads / Bun workers can call db_step /
// db_bind_* / db_column_* on the SAME handle concurrently — without
// serialization they race stmt_current/stmts (memory corruption). Every
// cursor-touching binding locks the handle's mutex for the duration of
// the call. Entries are erased in db_close (after the C struct is freed
// and the per-handle mutex is unlocked) so the map does not grow
// unboundedly across handle churn.
static std::unordered_map<arkilian*, std::mutex> g_stmt_mutexes;

// RAII helper: acquires g_registry_mutex, validates the handle, acquires
// the per-handle mutex, releases g_registry_mutex, and returns both the
// pointer and the held lock. The lock prevents db_close from freeing the
// struct for the duration of the call — eliminating the UAF.
struct DbLock {
  arkilian* db;
  std::unique_lock<std::mutex> lock;
  explicit operator bool() const { return db != nullptr; }
};

static DbLock lockDb(const Napi::CallbackInfo& info) {
  if (info.Length() < 1 || !info[0].IsNumber())
    return {nullptr, std::unique_lock<std::mutex>()};
  int64_t id = info[0].As<Napi::Number>().Int64Value();
  arkilian* db = reinterpret_cast<arkilian*>(id);
  std::lock_guard<std::mutex> regLock(g_registry_mutex);
  if (!g_registry.count(db))
    return {nullptr, std::unique_lock<std::mutex>()};
  // Acquire the per-handle mutex while holding g_registry_mutex so
  // db_close (which also needs g_registry_mutex to unregister) cannot
  // free the struct between our lookup and our lock.
  std::unique_lock<std::mutex> lock(g_stmt_mutexes[db]);
  return {db, std::move(lock)};
}

// Overload for functions that receive the id as a raw int64_t (db_close).
static DbLock lockDbById(int64_t id) {
  arkilian* db = reinterpret_cast<arkilian*>(id);
  std::lock_guard<std::mutex> regLock(g_registry_mutex);
  if (!g_registry.count(db))
    return {nullptr, std::unique_lock<std::mutex>()};
  std::unique_lock<std::mutex> lock(g_stmt_mutexes[db]);
  return {db, std::move(lock)};
}

// ── Log callback bridge ─────────────────────────────────────────────
// ark_log can fire from the backup threads, so the JS callback is
// marshalled through a thread-safe function.
static std::mutex g_log_mutex;
static std::unordered_map<arkilian*, Napi::ThreadSafeFunction> g_log_tsfns;

static void log_bridge(ark_log_level_t level, const char* msg, void* ctx) {
  arkilian* db = (arkilian*)ctx;
  Napi::ThreadSafeFunction tsfn;
  {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    auto it = g_log_tsfns.find(db);
    if (it == g_log_tsfns.end()) return;
    tsfn = it->second;
  }
  std::string copy = msg ? msg : "";
  tsfn.BlockingCall([level, copy](Napi::Env env, Napi::Function jsFn) {
    jsFn.Call({Napi::Number::New(env, (double)level), Napi::String::New(env, copy)});
  });
}

static void releaseLogTsfn(arkilian* db) {
  std::lock_guard<std::mutex> lock(g_log_mutex);
  auto it = g_log_tsfns.find(db);
  if (it != g_log_tsfns.end()) {
    it->second.Release();
    g_log_tsfns.erase(it);
  }
}

static void registerDb(arkilian* db) {
  std::lock_guard<std::mutex> lock(g_registry_mutex);
  g_registry.insert(db);
}

// Returns true (and removes the handle) only if it was live.
static bool unregisterDb(arkilian* db) {
  std::lock_guard<std::mutex> lock(g_registry_mutex);
  return g_registry.erase(db) > 0;
}

// ── db_init / db_close ──────────────────────────────────────────────

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
    std::string msg = db ? db_errmsg(db) : "Failed to initialize database";
    if (db) db_close(db);
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    return env.Null();
  }

  registerDb(db);
  return Napi::Number::New(env, reinterpret_cast<int64_t>(db));
}

Napi::Value db_close(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsNumber()) return env.Null();
  int64_t id = info[0].As<Napi::Number>().Int64Value();
  arkilian* db = reinterpret_cast<arkilian*>(id);
  if (!unregisterDb(db)) return env.Null();

  // Acquire the per-handle mutex and wait for in-flight calls to finish.
  // Use a unique_lock so we can release it before erasing the mutex from
  // the map (erasing a locked mutex is UB).
  DbLock dl = lockDbById(id);
  // dl.db is null here because we already unregistered, but the mutex
  // entry still exists — lockDbById found it in g_stmt_mutexes. However,
  // lockDbById checks g_registry which we just removed from. So we need
  // to acquire the mutex directly.
  {
    std::lock_guard<std::mutex> regLock(g_registry_mutex);
    auto it = g_stmt_mutexes.find(db);
    if (it != g_stmt_mutexes.end()) {
      it->second.lock();
      db_close(db); // joins backup threads — no more logs can fire
      releaseLogTsfn(db);
      it->second.unlock();
      g_stmt_mutexes.erase(it);
    } else {
      // No mutex entry — no calls were ever made; just close.
      db_close(db);
      releaseLogTsfn(db);
    }
  }
  return env.Null();
}

// ── Cursor / exec bindings (all use lockDb for UAF-safe access) ─────

Napi::Value db_exec(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  if (info.Length() < 2 || !info[1].IsString()) {
    Napi::TypeError::New(env, "String expected for SQL").ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string sql = info[1].As<Napi::String>().Utf8Value();
  int result = db_exec(dl.db, sql.c_str());
  return Napi::Number::New(env, result);
}

Napi::Value db_prepare(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  if (info.Length() < 2 || !info[1].IsString()) {
    Napi::TypeError::New(env, "String expected for SQL").ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string sql = info[1].As<Napi::String>().Utf8Value();
  int result = db_prepare(dl.db, sql.c_str());
  return Napi::Number::New(env, result);
}

Napi::Value db_use_stmt(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) {
    Napi::Error::New(env, "Invalid database id or index").ThrowAsJavaScriptException();
    return env.Null();
  }
  int index = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_use_stmt(dl.db, index));
}

Napi::Value db_stmt_count(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_stmt_count(dl.db) : 0);
}

Napi::Value db_step(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_step(dl.db));
}

Napi::Value db_finalize(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_finalize(dl.db));
}

Napi::Value db_reset(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_reset(dl.db));
}

Napi::Value db_column_count(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) return Napi::Number::New(env, 0);
  return Napi::Number::New(env, db_column_count(dl.db));
}

Napi::Value db_column_name(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return env.Null();
  int col = info[1].As<Napi::Number>().Int32Value();
  const char* name = db_column_name(dl.db, col);
  return name ? Napi::String::New(env, name) : env.Null();
}

Napi::Value db_column_text(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return env.Null();
  int col = info[1].As<Napi::Number>().Int32Value();
  const char* text = db_column_text(dl.db, col);
  return text ? Napi::String::New(env, text) : env.Null();
}

Napi::Value db_column_int(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 0);
  int col = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_column_int(dl.db, col));
}

Napi::Value db_column_double(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 0.0);
  int col = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_column_double(dl.db, col));
}

Napi::Value db_column_type(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 5);
  int col = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_column_type(dl.db, col));
}

Napi::Value db_bind_text(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 3 || !info[1].IsNumber() || !info[2].IsString()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  std::string val = info[2].As<Napi::String>().Utf8Value();
  return Napi::Number::New(env, db_bind_text(dl.db, idx, val.c_str()));
}

Napi::Value db_bind_int(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 3 || !info[1].IsNumber() || !info[2].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  int val = info[2].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_bind_int(dl.db, idx, val));
}

Napi::Value db_bind_int64(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 3 || !info[1].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  sqlite3_int64 v;
  if (info[2].IsBigInt()) {
    bool lossless = false;
    v = (sqlite3_int64)info[2].As<Napi::BigInt>().Int64Value(&lossless);
    if (!lossless) {
      Napi::RangeError::New(env, "BigInt parameter out of int64 range").ThrowAsJavaScriptException();
      return env.Null();
    }
  } else if (info[2].IsNumber()) {
    double d = info[2].As<Napi::Number>().DoubleValue();
    if (!std::isfinite(d) || d > 9223372036854775807.0 || d < -9223372036854775808.0) {
      Napi::RangeError::New(env, "Integer parameter out of int64 range").ThrowAsJavaScriptException();
      return env.Null();
    }
    v = (sqlite3_int64)d;
  } else {
    return env.Null();
  }
  return Napi::Number::New(env, db_bind_int64(dl.db, idx, v));
}

Napi::Value db_column_int64(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return Napi::Number::New(env, 0);
  int col = info[1].As<Napi::Number>().Int32Value();
  sqlite3_int64 v = db_column_int64(dl.db, col);
  if (v > 9007199254740991LL || v < -9007199254740991LL)
    return Napi::BigInt::New(env, v);
  return Napi::Number::New(env, (double)v);
}

Napi::Value db_bind_double(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 3 || !info[1].IsNumber() || !info[2].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  double val = info[2].As<Napi::Number>().DoubleValue();
  return Napi::Number::New(env, db_bind_double(dl.db, idx, val));
}

Napi::Value db_bind_null(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsNumber()) return env.Null();
  int idx = info[1].As<Napi::Number>().Int32Value();
  return Napi::Number::New(env, db_bind_null(dl.db, idx));
}

Napi::Value db_begin(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_begin(dl.db) : SQLITE_ERROR);
}

Napi::Value db_commit(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_commit(dl.db) : SQLITE_ERROR);
}

Napi::Value db_rollback(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_rollback(dl.db) : SQLITE_ERROR);
}

Napi::Value db_errmsg(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) return env.Null();
  const char* msg = db_errmsg(dl.db);
  return msg ? Napi::String::New(env, msg) : env.Null();
}

// ── Kill-switch, API key, monitoring (all use lockDb — no UAF) ──────

Napi::Value db_backup_set_enabled(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::TypeError::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  bool enabled = info.Length() < 2 ? true : info[1].As<Napi::Boolean>().Value();
  db_backup_set_enabled(dl.db, enabled ? 1 : 0);
  return env.Null();
}

Napi::Value db_backup_is_enabled(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) return Napi::Boolean::New(env, false);
  return Napi::Boolean::New(env, db_backup_is_enabled(dl.db) != 0);
}

Napi::Value db_set_log_callback(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::TypeError::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  releaseLogTsfn(dl.db);
  bool hasFn = info.Length() >= 2 && info[1].IsFunction();
  if (hasFn) {
    auto fn = info[1].As<Napi::Function>();
    {
      std::lock_guard<std::mutex> lk(g_log_mutex);
      g_log_tsfns[dl.db] =
          Napi::ThreadSafeFunction::New(env, fn, "arkilianLog", 0, 1);
    }
    db_set_log_callback(dl.db, log_bridge, (void*)dl.db);
  } else {
    db_set_log_callback(dl.db, NULL, NULL);
  }
  return env.Null();
}

Napi::Value db_backup_queue_depth(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_backup_queue_depth(dl.db) : 0);
}

Napi::Value db_backup_oldest_pending_age_sec(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, (double)(dl ? db_backup_oldest_pending_age_sec(dl.db) : 0));
}

Napi::Value db_backup_dead_letter_count(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_backup_dead_letter_count(dl.db) : 0);
}

Napi::Value db_backup_thread_heartbeat_age_ms(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, (double)(dl ? db_backup_thread_heartbeat_age_ms(dl.db) : -1));
}

Napi::Value db_backup_snapshot_heartbeat_age_ms(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, (double)(dl ? db_backup_snapshot_heartbeat_age_ms(dl.db) : -1));
}

Napi::Value db_backup_trigger_coverage(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_backup_trigger_coverage(dl.db) : -1);
}

Napi::Value db_backup_skipped_table_count(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_backup_skipped_table_count(dl.db) : -1);
}

Napi::Value db_backup_is_healthy(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Boolean::New(env, dl ? db_backup_is_healthy(dl.db) != 0 : false);
}

Napi::Value db_backup_triggers_dirty(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Boolean::New(env, dl ? db_backup_triggers_dirty(dl.db) != 0 : false);
}

Napi::Value db_backup_capture_paused(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Boolean::New(env, dl ? db_backup_capture_paused(dl.db) != 0 : false);
}

Napi::Value db_set_auto_resync_triggers(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsBoolean()) return env.Null();
  db_set_auto_resync_triggers(dl.db, info[1].As<Napi::Boolean>().Value() ? 1 : 0);
  return env.Undefined();
}

Napi::Value db_get_auto_resync_triggers(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Boolean::New(env, dl ? db_get_auto_resync_triggers(dl.db) != 0 : false);
}

Napi::Value db_resync_triggers(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::TypeError::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Number::New(env, db_resync_triggers(dl.db));
}

Napi::Value db_set_api_key(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl || info.Length() < 2 || !info[1].IsString()) return env.Null();
  std::string key = info[1].As<Napi::String>().Utf8Value();
  return Napi::Number::New(env, db_set_api_key(dl.db, key.c_str()));
}

Napi::Value db_changes(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_changes(dl.db) : 0);
}

Napi::Value db_last_insert_rowid(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  return Napi::Number::New(env, dl ? db_last_insert_rowid(dl.db) : 0);
}

// ── Native Fast Path: Single N-API turn query execution ─────────────

Napi::Value db_all_native(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  DbLock dl = lockDb(info);
  if (!dl) {
    Napi::Error::New(env, "Invalid database id").ThrowAsJavaScriptException();
    return env.Null();
  }

  uint32_t max_rows = 0;
  if (info.Length() >= 2 && info[1].IsNumber()) {
    max_rows = info[1].As<Napi::Number>().Uint32Value();
  }

  int col_count = db_column_count(dl.db);
  Napi::Array results = Napi::Array::New(env);
  uint32_t row_idx = 0;

  std::vector<std::string> col_names(col_count);
  for (int i = 0; i < col_count; i++) {
    const char *name = db_column_name(dl.db, i);
    col_names[i] = name ? name : "";
  }

  int step_rc = SQLITE_OK;
  while ((step_rc = db_step(dl.db)) == SQLITE_ROW) {
    if (max_rows > 0 && row_idx >= max_rows) break;
    Napi::Object row = Napi::Object::New(env);
    for (int i = 0; i < col_count; i++) {
      int type = db_column_type(dl.db, i);
      if (type == SQLITE_INTEGER) {
        sqlite3_int64 v = db_column_int64(dl.db, i);
        if (v > 9007199254740991LL || v < -9007199254740991LL)
          row.Set(col_names[i], Napi::BigInt::New(env, v));
        else
          row.Set(col_names[i], Napi::Number::New(env, (double)v));
      } else if (type == SQLITE_FLOAT) {
        row.Set(col_names[i], Napi::Number::New(env, db_column_double(dl.db, i)));
      } else if (type == SQLITE_TEXT) {
        const char *txt = db_column_text(dl.db, i);
        row.Set(col_names[i], txt ? Napi::String::New(env, txt) : env.Null());
      } else if (type == SQLITE_NULL) {
        row.Set(col_names[i], env.Null());
      } else if (type == SQLITE_BLOB) {
        const void *blob = db_column_blob(dl.db, i);
        int nbytes = db_column_bytes(dl.db, i);
        if (blob && nbytes > 0)
          row.Set(col_names[i], Napi::Buffer<char>::Copy(env, (const char *)blob, nbytes));
        else
          row.Set(col_names[i], Napi::Buffer<char>::New(env, 0));
      } else {
        const char *txt = db_column_text(dl.db, i);
        row.Set(col_names[i], txt ? Napi::String::New(env, txt) : env.Null());
      }
    }
    results.Set(row_idx++, row);
  }

  if (step_rc != SQLITE_DONE) {
    std::string msg = db_errmsg(dl.db);
    db_finalize(dl.db);
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    return env.Null();
  }

  db_finalize(dl.db);
  return results;
}

// ── Hydration: cold-start restore from the control plane ────────────
// Exposed as a standalone function (not on a handle) because hydration
// must run from a cold process — before db_init() opens the database.
//   arg0: db_path (string)
//   arg1: control_url (string, e.g. "https://api.arkilian.com")
//   arg2: api_key (string)
// Returns: HYDRATION_OK (0) on success, negative error code on failure.

Napi::Value db_hydrate(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 3 || !info[0].IsString() || !info[1].IsString() || !info[2].IsString()) {
    Napi::TypeError::New(env, "Expected (dbPath, controlUrl, apiKey)").ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string dbPath = info[0].As<Napi::String>().Utf8Value();
  std::string controlUrl = info[1].As<Napi::String>().Utf8Value();
  std::string apiKey = info[2].As<Napi::String>().Utf8Value();

  int rc = arkilian_hydrate(dbPath.c_str(), controlUrl.c_str(), apiKey.c_str(),
                             NULL, NULL);
  if (rc != HYDRATION_OK) {
    std::string msg = "hydration failed (error code " + std::to_string(rc) + ")";
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    return env.Null();
  }
  return Napi::Boolean::New(env, true);
}

// ── Module init ─────────────────────────────────────────────────────

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
  exports.Set("db_column_int64", Napi::Function::New<db_column_int64>(env));
  exports.Set("db_column_double", Napi::Function::New<db_column_double>(env));
  exports.Set("db_column_type", Napi::Function::New<db_column_type>(env));
  exports.Set("db_bind_text", Napi::Function::New<db_bind_text>(env));
  exports.Set("db_bind_int", Napi::Function::New<db_bind_int>(env));
  exports.Set("db_bind_int64", Napi::Function::New<db_bind_int64>(env));
  exports.Set("db_bind_double", Napi::Function::New<db_bind_double>(env));
  exports.Set("db_bind_null", Napi::Function::New<db_bind_null>(env));
  exports.Set("db_begin", Napi::Function::New<db_begin>(env));
  exports.Set("db_commit", Napi::Function::New<db_commit>(env));
  exports.Set("db_rollback", Napi::Function::New<db_rollback>(env));
  exports.Set("db_errmsg", Napi::Function::New<db_errmsg>(env));
  exports.Set("db_backup_set_enabled", Napi::Function::New<db_backup_set_enabled>(env));
  exports.Set("db_backup_is_enabled", Napi::Function::New<db_backup_is_enabled>(env));
  exports.Set("db_set_log_callback", Napi::Function::New<db_set_log_callback>(env));
  exports.Set("db_backup_queue_depth", Napi::Function::New<db_backup_queue_depth>(env));
  exports.Set("db_backup_oldest_pending_age_sec", Napi::Function::New<db_backup_oldest_pending_age_sec>(env));
  exports.Set("db_backup_dead_letter_count", Napi::Function::New<db_backup_dead_letter_count>(env));
  exports.Set("db_backup_thread_heartbeat_age_ms", Napi::Function::New<db_backup_thread_heartbeat_age_ms>(env));
  exports.Set("db_backup_snapshot_heartbeat_age_ms", Napi::Function::New<db_backup_snapshot_heartbeat_age_ms>(env));
  exports.Set("db_backup_trigger_coverage", Napi::Function::New<db_backup_trigger_coverage>(env));
  exports.Set("db_backup_skipped_table_count", Napi::Function::New<db_backup_skipped_table_count>(env));
  exports.Set("db_backup_is_healthy", Napi::Function::New<db_backup_is_healthy>(env));
  exports.Set("db_backup_triggers_dirty", Napi::Function::New<db_backup_triggers_dirty>(env));
  exports.Set("db_backup_capture_paused", Napi::Function::New<db_backup_capture_paused>(env));
  exports.Set("db_set_auto_resync_triggers", Napi::Function::New<db_set_auto_resync_triggers>(env));
  exports.Set("db_get_auto_resync_triggers", Napi::Function::New<db_get_auto_resync_triggers>(env));
  exports.Set("db_resync_triggers", Napi::Function::New<db_resync_triggers>(env));
  exports.Set("db_set_api_key", Napi::Function::New<db_set_api_key>(env));
  exports.Set("db_changes", Napi::Function::New<db_changes>(env));
  exports.Set("db_last_insert_rowid", Napi::Function::New<db_last_insert_rowid>(env));
  exports.Set("db_all_native", Napi::Function::New<db_all_native>(env));
  exports.Set("db_hydrate", Napi::Function::New<db_hydrate>(env));

  exports.Set("SQLITE_OK", Napi::Number::New(env, 0));
  exports.Set("SQLITE_ROW", Napi::Number::New(env, 100));
  exports.Set("SQLITE_DONE", Napi::Number::New(env, 101));
  exports.Set("SQLITE_ERROR", Napi::Number::New(env, 1));

  // Hydration error codes
  exports.Set("HYDRATION_OK", Napi::Number::New(env, HYDRATION_OK));
  exports.Set("HYDRATION_ERR_NET", Napi::Number::New(env, HYDRATION_ERR_NET));
  exports.Set("HYDRATION_ERR_DISK", Napi::Number::New(env, HYDRATION_ERR_DISK));
  exports.Set("HYDRATION_ERR_MEM", Napi::Number::New(env, HYDRATION_ERR_MEM));
  exports.Set("HYDRATION_ERR_PROTO", Napi::Number::New(env, HYDRATION_ERR_PROTO));
  exports.Set("HYDRATION_ERR_SQL", Napi::Number::New(env, HYDRATION_ERR_SQL));
  exports.Set("HYDRATION_ERR_EXPIRED", Napi::Number::New(env, HYDRATION_ERR_EXPIRED));
  exports.Set("HYDRATION_ERR_NOTFOUND", Napi::Number::New(env, HYDRATION_ERR_NOTFOUND));
  exports.Set("HYDRATION_ERR_NEWER", Napi::Number::New(env, HYDRATION_ERR_NEWER));
  exports.Set("HYDRATION_ERR_BUSY", Napi::Number::New(env, HYDRATION_ERR_BUSY));

  return exports;
}

NODE_API_MODULE(arkilian, Init)
