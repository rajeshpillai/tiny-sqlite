// src/main.c
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

/* SAFE page offset calculation (prevents integer overflow) */
static long page_offset(uint32_t page_num) {
    return (long)page_num * (long)PAGE_SIZE;
}

/* ---------- Pager ---------- */

static Pager *pager_open(const char *filename) {
    FILE *file = fopen(filename, "r+b");
    if (!file) file = fopen(filename, "w+b");
    if (!file) fatal("fopen");

    fseek(file, 0, SEEK_END);
    long fl = ftell(file);
    if (fl < 0) fatal("ftell");

    Pager *pager = calloc(1, sizeof(Pager));
    pager->file = file;
    pager->file_length = (uint32_t)fl;
    pager->num_pages = pager->file_length / PAGE_SIZE;

    if (pager->file_length % PAGE_SIZE != 0) {
        fatal("Corrupt DB file");
    }

    return pager;
}

static void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num >= TABLE_MAX_PAGES) fatal("Page out of bounds");

    if (pager->pages[page_num] == NULL) {
        void *page = calloc(1, PAGE_SIZE);
        if (!page) fatal("calloc");

        if (page_num < pager->num_pages) {
            fseek(pager->file, page_offset(page_num), SEEK_SET);
            if (fread(page, PAGE_SIZE, 1, pager->file) != 1) {
                fatal("fread");
            }
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages)
            pager->num_pages = page_num + 1;
    }

    return pager->pages[page_num];
}

static void pager_flush(Pager *pager, uint32_t page_num) {
    if (!pager->pages[page_num]) return;

    fseek(pager->file, page_offset(page_num), SEEK_SET);
    if (fwrite(pager->pages[page_num], PAGE_SIZE, 1, pager->file) != 1) {
        fatal("fwrite");
    }
}

/* ---------- Table ---------- */

static Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    Table *t = calloc(1, sizeof(Table));
    t->pager = pager;

    if (pager->num_pages > 0) {
        void *page0 = get_page(pager, 0);
        memcpy(&t->header, page0, sizeof(DBHeader));
    } else {
        t->header.num_rows = 0;
        t->header.root_page_num = 0;
        t->header.next_free_page = 1; // page 0 is header
    }

    return t;
}

static void db_close(Table *t) {
    Pager *pager = t->pager;

    void *page0 = get_page(pager, 0);
    memcpy(page0, &t->header, sizeof(DBHeader));

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        if (pager->pages[i]) {
            pager_flush(pager, i);
            free(pager->pages[i]);
        }
    }

    fclose(pager->file);
    free(pager);
    free(t);
}

/* ---------- Row Serialization ---------- */

static void serialize_row(const Row *src, void *dst) {
    memcpy(dst, src, sizeof(Row));
}

static void deserialize_row(const void *src, Row *dst) {
    memcpy(dst, src, sizeof(Row));
}

static void *row_slot(Table *t, uint32_t row_num) {
    uint32_t row_offset = row_num * sizeof(Row);
    uint32_t page_num = row_offset / PAGE_SIZE;
    uint32_t byte_offset = row_offset % PAGE_SIZE;

    void *page = get_page(t->pager, page_num + 1);
    return (char *)page + byte_offset;
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

static void execute_insert(const Statement *s) {
    serialize_row(&s->row, row_slot(table, table->header.num_rows));
    table->header.num_rows++;
    puts("Executed.");
}

static void execute_select(void) {
    Row row;
    for (uint32_t i = 0; i < table->header.num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
    }
}

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
