 

#ifndef ARKILIAN_H
#define ARKILIAN_H
 
#include "deps/sqlite/sqlite3.h" 
#include <stddef.h>

// public header
typedef struct arkilian arkilian;

int db_init(arkilian **db, const char *filename);
void db_close(arkilian *db); 
const char* db_errmsg(arkilian *db);
sqlite3* db_get_handle(arkilian *db);

#endif
