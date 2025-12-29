#ifndef DB_H
#define DB_H

#include "btree.h"

Table *db_open(const char *filename);
void   db_close(Table *t);

#endif
