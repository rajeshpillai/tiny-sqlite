// Commit 10: Full recursive B-Tree (internal splits + root split)

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#include "db.h"
#include "btree.h"

#define INPUT_BUFFER_SIZE 1024

typedef enum {
    STMT_INSERT,
    STMT_SELECT
} StatementType;

typedef struct {
    StatementType type;
    Row row;
} Statement;

static bool starts_with_icase_n(const char *s, const char *p, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (!p[i]) return true;
        if (!s[i]) return false;
        unsigned char a = (unsigned char)s[i];
        unsigned char b = (unsigned char)p[i];
        if (a >= 'A' && a <= 'Z') a = (unsigned char)(a - 'A' + 'a');
        if (b >= 'A' && b <= 'Z') b = (unsigned char)(b - 'A' + 'a');
        if (a != b) return false;
    }
    return true;
}

static bool prepare_insert(const char *input, Statement *st) {
    st->type = STMT_INSERT;
    return sscanf(input, "insert %d %32s %255s",
                  &st->row.id, st->row.username, st->row.email) == 3;
}

static bool prepare_statement(const char *input, Statement *st) {
    if (starts_with_icase_n(input, "insert", 6)) return prepare_insert(input, st);
    if (starts_with_icase_n(input, "select", 6)) { st->type = STMT_SELECT; return true; }
    puts("Unrecognized statement");
    return false;
}

static void execute_select(Table *t) {
    Cursor *c = btree_table_start(t);
    while (!c->end_of_table) {
        Row row;
        memcpy(&row, btree_cursor_value(c), sizeof(Row));
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        btree_cursor_advance(c);
    }
    btree_cursor_free(c);
}

static void execute_insert(Table *t, const Row *row) {
    char err[128] = {0};
    if (!btree_insert(t, row, err, sizeof(err))) {
        printf("Error: %s\n", err[0] ? err : "insert failed");
        return;
    }
    puts("Executed.");
}

int main(void) {
    // Delete old test.db before running this commit.
    Table *t = db_open("test.db");

    char input[INPUT_BUFFER_SIZE];

    while (true) {
        printf("minidb> ");
        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\n")] = 0;

        if (input[0] == '.') {
            if (strcmp(input, ".exit") == 0) break;
            puts("Unrecognized meta command");
            continue;
        }

        Statement st;
        if (!prepare_statement(input, &st)) continue;

        if (st.type == STMT_INSERT) execute_insert(t, &st.row);
        else execute_select(t);
    }

    db_close(t);
    return 0;
}
