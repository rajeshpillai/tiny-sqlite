// src/main.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <stdint.h>
#include <errno.h>

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
    uint32_t num_rows;
} Table;

static Table *table;

/* ---------- Utilities ---------- */

static void fatal(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

/* ---------- Pager ---------- */

static Pager *pager_open(const char *filename) {
    FILE *file = fopen(filename, "r+b");
    if (!file) {
        file = fopen(filename, "w+b");
        if (!file) {
            fatal("fopen");
        }
    }

    fseek(file, 0L, SEEK_END);
    uint32_t file_length = ftell(file);

    Pager *pager = calloc(1, sizeof(Pager));
    pager->file = file;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;

    if (file_length % PAGE_SIZE != 0) {
        fatal("DB file is not a whole number of pages");
    }

    return pager;
}

static void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num >= TABLE_MAX_PAGES) {
        fatal("Page number out of bounds");
    }

    if (pager->pages[page_num] == NULL) {
        void *page = calloc(1, PAGE_SIZE);

        if (page_num < pager->num_pages) {
            fseek(pager->file, page_num * PAGE_SIZE, SEEK_SET);
            fread(page, PAGE_SIZE, 1, pager->file);
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

static void pager_flush(Pager *pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) return;

    fseek(pager->file, page_num * PAGE_SIZE, SEEK_SET);
    fwrite(pager->pages[page_num], PAGE_SIZE, 1, pager->file);
}

/* ---------- Table ---------- */

static Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);

    Table *table = calloc(1, sizeof(Table));
    table->pager = pager;
    
    // Read row count from metadata page (page 0)
    if (pager->num_pages > 0) {
        void *header_page = get_page(pager, 0);
        memcpy(&table->num_rows, header_page, sizeof(uint32_t));
    } else {
        table->num_rows = 0;
    }

    return table;
}

static void db_close(Table *table) {
    Pager *pager = table->pager;

    // Write row count to metadata page (page 0)
    void *header_page = get_page(pager, 0);
    memcpy(header_page, &table->num_rows, sizeof(uint32_t));

    // Flush and free only pages that were actually allocated
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        if (pager->pages[i] != NULL) {
            pager_flush(pager, i);
            free(pager->pages[i]);
        }
    }

    fclose(pager->file);
    free(pager);
    free(table);
}

/* ---------- Row Serialization ---------- */

static void serialize_row(const Row *source, void *destination) {
    memcpy(destination, source, sizeof(Row));
}

static void deserialize_row(const void *source, Row *destination) {
    memcpy(destination, source, sizeof(Row));
}

static void *row_slot(Table *table, uint32_t row_num) {
    uint32_t row_offset = row_num * sizeof(Row);
    uint32_t page_num = row_offset / PAGE_SIZE;
    uint32_t byte_offset = row_offset % PAGE_SIZE;

    // Page 0 is reserved for metadata, data starts at page 1
    void *page = get_page(table->pager, page_num + 1);
    return (char *)page + byte_offset;
}

/* ---------- REPL & SQL ---------- */

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

static void print_prompt(void) {
    printf("minidb> ");
}

static bool read_input(char *buffer, size_t size) {
    if (!fgets(buffer, size, stdin)) return false;
    buffer[strcspn(buffer, "\n")] = 0;
    return true;
}

static bool do_meta_command(const char *input) {
    if (strcmp(input, ".exit") == 0) return false;
    return true;
}

static bool prepare_insert(const char *input, Statement *statement) {
    statement->type = STATEMENT_INSERT;

    int args = sscanf(
        input,
        "insert %d %32s %255s",
        &statement->row_to_insert.id,
        statement->row_to_insert.username,
        statement->row_to_insert.email
    );

    if (args < 3) {
        puts("Syntax error");
        return false;
    }
    return true;
}

static bool prepare_statement(const char *input, Statement *statement) {
    if (strncasecmp(input, "insert", 6) == 0) {
        return prepare_insert(input, statement);
    }

    if (strncasecmp(input, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return true;
    }

    puts("Unrecognized statement");
    return false;
}

static void execute_insert(const Statement *statement) {
    serialize_row(
        &statement->row_to_insert,
        row_slot(table, table->num_rows)
    );
    table->num_rows++;
    puts("Executed.");
}

static void execute_select(void) {
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
    }
}

static void execute_statement(const Statement *statement) {
    if (statement->type == STATEMENT_INSERT) {
        execute_insert(statement);
    } else {
        execute_select();
    }
}

/* ---------- Main ---------- */

int main(void) {
    table = db_open("test.db");

    char input[INPUT_BUFFER_SIZE];

    while (true) {
        print_prompt();
        if (!read_input(input, sizeof(input))) break;

        if (input[0] == '.') {
            if (!do_meta_command(input)) break;
            continue;
        }

        Statement statement;
        if (!prepare_statement(input, &statement)) continue;
        execute_statement(&statement);
    }

    db_close(table);
    return 0;
}
