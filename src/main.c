// src/main.c
// Commit 08: B-Tree begins (single leaf root node)
// NOTE: On-disk format changed from prior commits. Delete old test.db.

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

/* =========================
 * Row
 * ========================= */

typedef struct {
    int32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

/* =========================
 * DB Header (Page 0)
 * ========================= */

typedef struct {
    uint32_t num_rows;        // optional/legacy metric; we can derive from leaf num_cells later
    uint32_t root_page_num;   // root of B-tree
    uint32_t next_free_page;  // next page id to allocate
} DBHeader;

/* =========================
 * Pager
 * ========================= */

typedef struct {
    FILE *file;
    uint32_t file_length;
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

/* =========================
 * Table
 * ========================= */

typedef struct {
    Pager *pager;
    DBHeader header;
} Table;

static Table *table;

/* =========================
 * Utilities
 * ========================= */

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

static long page_offset(uint32_t page_num) {
    return (long)page_num * (long)PAGE_SIZE;
}

/* =========================
 * Pager
 * ========================= */

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

/* =========================
 * B-Tree Node Layout (Leaf only for now)
 * ========================= */

/*
 * Node header common fields:
 * - node_type: 1 byte (0=internal, 1=leaf)
 * - is_root:  1 byte (0/1)
 * - parent:   4 bytes (page num)
 */
typedef enum {
    NODE_INTERNAL = 0,
    NODE_LEAF = 1
} NodeType;

#define NODE_TYPE_SIZE        1
#define NODE_TYPE_OFFSET      0
#define IS_ROOT_SIZE          1
#define IS_ROOT_OFFSET        (NODE_TYPE_OFFSET + NODE_TYPE_SIZE)
#define PARENT_POINTER_SIZE   4
#define PARENT_POINTER_OFFSET (IS_ROOT_OFFSET + IS_ROOT_SIZE)

#define COMMON_NODE_HEADER_SIZE (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

/*
 * Leaf node header:
 * - num_cells: 4 bytes
 */
#define LEAF_NODE_NUM_CELLS_SIZE   4
#define LEAF_NODE_NUM_CELLS_OFFSET (COMMON_NODE_HEADER_SIZE)
#define LEAF_NODE_HEADER_SIZE      (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE)

/*
 * Leaf cell:
 * - key: 4 bytes (int32/uint32)
 * - value: Row bytes
 */
#define LEAF_NODE_KEY_SIZE    4
#define LEAF_NODE_KEY_OFFSET  0
#define LEAF_NODE_VALUE_SIZE  ((uint32_t)sizeof(Row))
#define LEAF_NODE_VALUE_OFFSET (LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)
#define LEAF_NODE_CELL_SIZE   (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)

#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS       (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

/* ---- node access helpers ---- */

static NodeType get_node_type(void *node) {
    uint8_t v = *((uint8_t *)node + NODE_TYPE_OFFSET);
    return (NodeType)v;
}

static void set_node_type(void *node, NodeType type) {
    *((uint8_t *)node + NODE_TYPE_OFFSET) = (uint8_t)type;
}

static bool is_node_root(void *node) {
    return (*((uint8_t *)node + IS_ROOT_OFFSET)) != 0;
}

static void set_node_root(void *node, bool is_root) {
    *((uint8_t *)node + IS_ROOT_OFFSET) = is_root ? 1 : 0;
}

static uint32_t *node_parent(void *node) {
    return (uint32_t *)((uint8_t *)node + PARENT_POINTER_OFFSET);
}

static uint32_t *leaf_node_num_cells(void *node) {
    return (uint32_t *)((uint8_t *)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

static void *leaf_node_cell(void *node, uint32_t cell_num) {
    return (uint8_t *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

static uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
    return (uint32_t *)((uint8_t *)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_OFFSET);
}

static void *leaf_node_value(void *node, uint32_t cell_num) {
    return (uint8_t *)leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

static void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *node_parent(node) = 0;
    *leaf_node_num_cells(node) = 0;
}

/* =========================
 * DB open/close
 * ========================= */

static void db_init_new(Table *t) {
    // Page 0: header
    t->header.num_rows = 0;
    t->header.root_page_num = 1;   // root is page 1
    t->header.next_free_page = 2;  // next alloc after root

    // Page 1: root leaf node
    void *root = get_page(t->pager, t->header.root_page_num);
    initialize_leaf_node(root);
    set_node_root(root, true);
}

static Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);

    Table *t = calloc(1, sizeof(Table));
    if (!t) fatal("calloc");
    t->pager = pager;

    if (pager->num_pages == 0) {
        // Fresh DB
        db_init_new(t);
    } else {
        // Existing DB: read header from page 0
        void *page0 = get_page(pager, 0);
        memcpy(&t->header, page0, sizeof(DBHeader));

        // Very basic sanity: if root_page_num is 0, treat as invalid/new
        if (t->header.root_page_num == 0 || t->header.root_page_num >= TABLE_MAX_PAGES) {
            fatal("Invalid header/root page (delete old db; format changed in Commit 08)");
        }

        void *root = get_page(pager, t->header.root_page_num);
        if (get_node_type(root) != NODE_LEAF) {
            fatal("Unsupported node type (only leaf supported in Commit 08)");
        }
    }

    return t;
}

static void db_close(Table *t) {
    Pager *pager = t->pager;

    // Persist header to page 0
    void *page0 = get_page(pager, 0);
    memcpy(page0, &t->header, sizeof(DBHeader));

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

/* =========================
 * Row serialization
 * ========================= */

static void serialize_row(const Row *src, void *dst) {
    memcpy(dst, src, sizeof(Row));
}

static void deserialize_row(const void *src, Row *dst) {
    memcpy(dst, src, sizeof(Row));
}

/* =========================
 * Cursor (now points into a leaf node)
 * ========================= */

typedef struct {
    Table *table;
    uint32_t page_num;     // which leaf page (root only for now)
    uint32_t cell_num;     // cell index within leaf
    bool end_of_table;
} Cursor;

static Cursor *cursor_create(Table *t, uint32_t page_num, uint32_t cell_num) {
    Cursor *c = calloc(1, sizeof(Cursor));
    if (!c) fatal("calloc");
    c->table = t;
    c->page_num = page_num;
    c->cell_num = cell_num;

    void *node = get_page(t->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    c->end_of_table = (cell_num >= num_cells);
    return c;
}

static void cursor_destroy(Cursor *c) {
    free(c);
}

static void *cursor_value(Cursor *c) {
    void *node = get_page(c->table->pager, c->page_num);
    return leaf_node_value(node, c->cell_num);
}

static void cursor_advance(Cursor *c) {
    void *node = get_page(c->table->pager, c->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    c->cell_num++;
    if (c->cell_num >= num_cells) {
        c->end_of_table = true; // single leaf only (for now)
    }
}

static Cursor *table_start(Table *t) {
    return cursor_create(t, t->header.root_page_num, 0);
}

/* =========================
 * Find (binary search within leaf)
 * ========================= */

static Cursor *table_find(Table *t, int32_t key) {
    uint32_t root_page = t->header.root_page_num;
    void *node = get_page(t->pager, root_page);

    uint32_t num_cells = *leaf_node_num_cells(node);

    uint32_t left = 0;
    uint32_t right = num_cells; // one past last

    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        uint32_t mid_key = *leaf_node_key(node, mid);

        if ((int32_t)mid_key == key) {
            return cursor_create(t, root_page, mid);
        } else if ((int32_t)mid_key < key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    // insertion point
    return cursor_create(t, root_page, left);
}

/* =========================
 * SQL layer
 * ========================= */

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

/* =========================
 * Execution (leaf insert + scan)
 * ========================= */

static void execute_insert(const Statement *s) {
    Cursor *cursor = table_find(table, s->row.id);
    void *node = get_page(table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);

    // Duplicate key check
    if (!cursor->end_of_table && cursor->cell_num < num_cells) {
        int32_t existing_key = (int32_t)(*leaf_node_key(node, cursor->cell_num));
        if (existing_key == s->row.id) {
            puts("Error: duplicate key.");
            cursor_destroy(cursor);
            return;
        }
    }

    // No split yet in Commit 08
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        puts("Error: leaf node full. (Split not implemented yet — next commit)");
        cursor_destroy(cursor);
        return;
    }

    // Shift cells right to make room
    if (cursor->cell_num < num_cells) {
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            void *dest = leaf_node_cell(node, i);
            void *src  = leaf_node_cell(node, i - 1);
            memmove(dest, src, LEAF_NODE_CELL_SIZE);
        }
    }

    // Write cell
    *leaf_node_num_cells(node) = num_cells + 1;
    *leaf_node_key(node, cursor->cell_num) = (uint32_t)s->row.id;
    serialize_row(&s->row, leaf_node_value(node, cursor->cell_num));

    table->header.num_rows++; // optional; leaf num_cells is the real count for now
    puts("Executed.");
    cursor_destroy(cursor);
}

static void execute_select(void) {
    Cursor *c = table_start(table);

    while (!c->end_of_table) {
        Row row;
        deserialize_row(cursor_value(c), &row);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        cursor_advance(c);
    }

    cursor_destroy(c);
}

/* =========================
 * Main
 * ========================= */

int main(void) {
    // Tip: use a new db filename if you want to keep old files around
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
