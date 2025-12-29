// src/main.c
// Commit 07: Cursor + Sorted Insert (binary search) on top of paged storage

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <stdint.h>

#define INPUT_BUFFER_SIZE 1024

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100

/* ---------- Row ---------- */

typedef struct {
    int32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

/* ---------- DB Header (Page 0) ---------- */

typedef struct {
    uint32_t num_rows;
    uint32_t root_page_num;    // future B-tree root
    uint32_t next_free_page;   // page allocator cursor
} DBHeader;

/* ---------- Pager ---------- */

typedef struct {
    FILE *file;
    uint32_t file_length;
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

/* ---------- Table ---------- */

typedef struct {
    Pager *pager;
    DBHeader header;
} Table;

static Table *table;

/* ---------- Cursor ---------- */

typedef struct {
    Table *table;
    uint32_t row_num;       // logical row index (0..num_rows)
    bool end_of_table;      // true when row_num == num_rows
} Cursor;

/* ---------- Utilities ---------- */

static void fatal(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static bool starts_with_icase_n(const char *str, const char *prefix, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (prefix[i] == '\0') return true;
        if (str[i] == '\0') return false;
        if (tolower((unsigned char)str[i]) != tolower((unsigned char)prefix[i]))
            return false;
    }
    return true;
}

/* SAFE page offset calculation (prevents integer overflow before fseek) */
static long page_offset(uint32_t page_num) {
    return (long)page_num * (long)PAGE_SIZE;
}

/* Max rows capacity with current layout (page 0 header; data pages 1..) */
static uint32_t table_max_rows(void) {
    uint32_t data_pages = TABLE_MAX_PAGES - 1; // page 0 reserved
    return (data_pages * PAGE_SIZE) / (uint32_t)sizeof(Row);
}

/* ---------- Pager ---------- */

static Pager *pager_open(const char *filename) {
    FILE *file = fopen(filename, "r+b");
    if (!file) file = fopen(filename, "w+b");
    if (!file) fatal("fopen");

    if (fseek(file, 0, SEEK_END) != 0) fatal("fseek");
    long fl = ftell(file);
    if (fl < 0) fatal("ftell");

    Pager *pager = calloc(1, sizeof(Pager));
    if (!pager) fatal("calloc");

    pager->file = file;
    pager->file_length = (uint32_t)fl;
    pager->num_pages = pager->file_length / PAGE_SIZE;

    if (pager->file_length % PAGE_SIZE != 0) {
        fatal("Corrupt DB file (partial page)");
    }

    return pager;
}

static void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num >= TABLE_MAX_PAGES) fatal("Page out of bounds");

    if (pager->pages[page_num] == NULL) {
        void *page = calloc(1, PAGE_SIZE);
        if (!page) fatal("calloc");

        if (page_num < pager->num_pages) {
            if (fseek(pager->file, page_offset(page_num), SEEK_SET) != 0) fatal("fseek");
            if (fread(page, PAGE_SIZE, 1, pager->file) != 1) fatal("fread");
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages)
            pager->num_pages = page_num + 1;
    }

    return pager->pages[page_num];
}

static void pager_flush(Pager *pager, uint32_t page_num) {
    if (!pager->pages[page_num]) return;

    if (fseek(pager->file, page_offset(page_num), SEEK_SET) != 0) fatal("fseek");
    if (fwrite(pager->pages[page_num], PAGE_SIZE, 1, pager->file) != 1) fatal("fwrite");
}

/* ---------- Table open/close ---------- */

static Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);

    Table *t = calloc(1, sizeof(Table));
    if (!t) fatal("calloc");

    t->pager = pager;

    if (pager->num_pages > 0) {
        void *page0 = get_page(pager, 0);
        memcpy(&t->header, page0, sizeof(DBHeader));
    } else {
        // Fresh DB
        t->header.num_rows = 0;
        t->header.root_page_num = 0;
        t->header.next_free_page = 1;
    }

    // Defensive: if file exists but header is garbage, cap num_rows to max
    uint32_t max_rows = table_max_rows();
    if (t->header.num_rows > max_rows) {
        fatal("Header corruption: num_rows too large");
    }

    return t;
}

static void db_close(Table *t) {
    Pager *pager = t->pager;

    // Persist header to page 0
    void *page0 = get_page(pager, 0);
    memcpy(page0, &t->header, sizeof(DBHeader));

    // Flush & free only allocated pages
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        if (pager->pages[i]) {
            pager_flush(pager, i);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }

    fclose(pager->file);
    free(pager);
    free(t);
}

/* ---------- Row serialization ---------- */

static void serialize_row(const Row *src, void *dst) {
    memcpy(dst, src, sizeof(Row));
}

static void deserialize_row(const void *src, Row *dst) {
    memcpy(dst, src, sizeof(Row));
}

/* Map row_num -> (page, offset). Page 0 is header; data begins at page 1. */
static void *row_slot(Table *t, uint32_t row_num) {
    uint32_t row_offset = row_num * (uint32_t)sizeof(Row);
    uint32_t page_num = row_offset / PAGE_SIZE;       // 0-based data-page index
    uint32_t byte_offset = row_offset % PAGE_SIZE;

    uint32_t physical_page = page_num + 1;            // +1 to skip header page
    if (physical_page >= TABLE_MAX_PAGES) {
        fatal("Database full (out of pages)");
    }

    void *page = get_page(t->pager, physical_page);
    return (char *)page + byte_offset;
}

/* ---------- Cursor helpers ---------- */

static Cursor *cursor_create(Table *t, uint32_t row_num) {
    Cursor *c = calloc(1, sizeof(Cursor));
    if (!c) fatal("calloc");
    c->table = t;
    c->row_num = row_num;
    c->end_of_table = (row_num >= t->header.num_rows);
    return c;
}

static void cursor_destroy(Cursor *c) {
    free(c);
}

static void *cursor_value(Cursor *c) {
    return row_slot(c->table, c->row_num);
}

static void cursor_advance(Cursor *c) {
    c->row_num++;
    if (c->row_num >= c->table->header.num_rows) {
        c->end_of_table = true;
    }
}

/* ---------- Finding rows (binary search by id) ---------- */

/*
 * Returns a cursor positioned at the place where key should be.
 * If key exists, cursor->row_num points to the existing row.
 * If key doesn't exist, cursor->row_num is the insertion point.
 */
static Cursor *table_find(Table *t, int32_t key) {
    uint32_t left = 0;
    uint32_t right = t->header.num_rows; // one past last

    while (left < right) {
        uint32_t mid = left + (right - left) / 2;

        Row mid_row;
        deserialize_row(row_slot(t, mid), &mid_row);

        if (mid_row.id == key) {
            return cursor_create(t, mid);
        } else if (mid_row.id < key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return cursor_create(t, left); // insertion point
}

/* ---------- SQL ---------- */

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
    Row row;
} Statement;

static bool prepare_insert(const char *input, Statement *s) {
    s->type = STATEMENT_INSERT;
    return sscanf(input, "insert %d %32s %255s",
                  &s->row.id, s->row.username, s->row.email) == 3;
}

static bool prepare_statement(const char *input, Statement *s) {
    if (starts_with_icase_n(input, "insert", 6))
        return prepare_insert(input, s);

    if (starts_with_icase_n(input, "select", 6)) {
        s->type = STATEMENT_SELECT;
        return true;
    }

    puts("Unrecognized statement");
    return false;
}

/* ---------- Execution ---------- */

static void execute_insert(const Statement *s) {
    if (table->header.num_rows >= table_max_rows()) {
        puts("Error: database full.");
        return;
    }

    // Find insertion point (or existing key)
    Cursor *cursor = table_find(table, s->row.id);

    // Duplicate check
    if (!cursor->end_of_table) {
        Row existing;
        deserialize_row(cursor_value(cursor), &existing);
        if (existing.id == s->row.id) {
            puts("Error: duplicate key.");
            cursor_destroy(cursor);
            return;
        }
    }

    // If inserting not at end, shift rows right by 1 to make room
    uint32_t insert_at = cursor->row_num;
    uint32_t num_rows = table->header.num_rows;

    if (insert_at < num_rows) {
        // Shift from last row down to insert_at
        for (uint32_t i = num_rows; i > insert_at; i--) {
            Row tmp;
            deserialize_row(row_slot(table, i - 1), &tmp);
            serialize_row(&tmp, row_slot(table, i));
        }
    }

    // Insert new row
    serialize_row(&s->row, row_slot(table, insert_at));
    table->header.num_rows++;

    puts("Executed.");
    cursor_destroy(cursor);
}

static void execute_select(void) {
    Cursor *c = cursor_create(table, 0);

    while (!c->end_of_table) {
        Row row;
        deserialize_row(cursor_value(c), &row);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        cursor_advance(c);
    }

    cursor_destroy(c);
}

/* ---------- Main ---------- */

int main(void) {
    table = db_open("test.db");
    char input[INPUT_BUFFER_SIZE];

    while (true) {
        printf("minidb> ");
        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\n")] = 0;

        if (strcmp(input, ".exit") == 0) break;

        Statement s;
        if (!prepare_statement(input, &s)) continue;

        if (s.type == STATEMENT_INSERT) execute_insert(&s);
        else execute_select();
    }

    db_close(table);
    return 0;
}
