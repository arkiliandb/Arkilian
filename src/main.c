// Arkilian SQLite Wrapper - C API

/*
TODOs
 
- [ ] Set up CMakeLists.txt for cross-platform builds (Windows MSVC, macOS
Clang, Linux GCC)
- [ ] Verify clean compilation on all three OS targets with strict warnings
enabled
- [ ] Set up CI (GitHub Actions) for Windows, macOS, Linux builds
*/


// main.c
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "class.h"

int main() {
    arkilian *db = NULL;
    
    if (db_init(&db, "app.sqlite") != 0) {
        printf("Failed to open: %s\n", db ? db_errmsg(db) : "Memory allocation failed");
        if (db) db_close(db);
        return 1;
    }

    sqlite3 *raw = db_get_handle(db);

    const char *create_sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);";
    char *err_msg = NULL;
    if (sqlite3_exec(raw, create_sql, 0, 0, &err_msg) != SQLITE_OK) {
        printf("Failed to create table: %s\n", err_msg);
        sqlite3_free(err_msg);
        db_close(db);
        return 1;
    }

    sqlite3_stmt *stmt;
    const char *sql = "INSERT INTO users (name) VALUES (?);";

    if (sqlite3_prepare_v2(raw, sql, -1, &stmt, 0) != SQLITE_OK) {
        printf("Prepare failed: %s\n", db_errmsg(db));
        db_close(db);
        return 1;
    }
 
    sqlite3_bind_text(stmt, 1, "Bob", -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        printf("Insert failed: %s\n", db_errmsg(db));
    } else {
        printf("Inserted Bob successfully!\n");
    }
 
    sqlite3_finalize(stmt);
    sleep(60);  
    db_close(db);

    return 0;
}