#ifndef BTREE_H
#define BTREE_H

#include <stdint.h>
#include <stdbool.h>
#include "pager.h"

#define TABLE_MAX_PAGES 256

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    int32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef enum {
    NODE_INTERNAL = 0,
    NODE_LEAF = 1
} NodeType;

typedef struct {
    uint32_t num_rows;       // informational
    uint32_t root_page_num;  // root page
    uint32_t next_free_page; // allocator cursor
} DBHeader;

typedef struct {
    Pager *pager;
    DBHeader header;
} Table;

/* Cursor points to a leaf cell */
typedef struct {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

/* Open helpers */
void btree_init_new_db(Table *t);

/* Cursor */
Cursor *btree_table_start(Table *t);
void    btree_cursor_advance(Cursor *c);
void   *btree_cursor_value(Cursor *c);
void    btree_cursor_free(Cursor *c);

/* Find/Insert */
Cursor *btree_table_find(Table *t, int32_t key);
bool    btree_insert(Table *t, const Row *row, char *errbuf, uint32_t errbuf_sz);

#endif
