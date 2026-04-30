// Basic tests for Arkilian library
#include "class.h"
#include <stdio.h>
#include <sqlite3.h>
#include <assert.h>
#include <stdlib.h>

int main(void) {
    printf("Running Arkilian tests...\n");

    // Test 1: Initialize database
    arkilian *db = NULL;
    int rc = db_init(&db, "test.db");
    assert(rc == 0);
    assert(db != NULL);
    printf("Test 1 passed: db_init\n");

    // Test 2: Get database handle
    sqlite3 *handle = db_get_handle(db);
    assert(handle != NULL);
    printf("Test 2 passed: db_get_handle\n");

    // Test 3: Error message (no error should be present)
    const char *err = db_errmsg(db);
    assert(err != NULL);
    printf("Test 3 passed: db_errmsg\n");

    // Test 4: Close database
    db_close(db);
    printf("Test 4 passed: db_close\n");

    // Test 5: Test with NULL filename (should use default)
    rc = db_init(&db, NULL);
    assert(rc == 0);
    db_close(db);
    printf("Test 5 passed: db_init with NULL filename\n");

    // Cleanup
    remove("test.db");
    remove("backup.sqlite");

    printf("All tests passed!\n");
    return 0;
}
