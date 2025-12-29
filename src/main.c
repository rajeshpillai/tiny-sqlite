// src/main.c
// Commit 09: Leaf split + Internal root + Linked leaf scan (SELECT)
// NOTE: Delete old test.db before running (disk format changed).

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
    uint32_t num_rows;        // informational (leaf cells are truth); kept for now
    uint32_t root_page_num;   // root of B-tree
    uint32_t next_free_page;  // allocator cursor
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
 * B-Tree Node Layout
 * ========================= */

typedef enum {
    NODE_INTERNAL = 0,
    NODE_LEAF = 1
} NodeType;

/*
 * Common header:
 * - node_type: 1
 * - is_root: 1
 * - parent: 4
 */
#define NODE_TYPE_SIZE        1
#define NODE_TYPE_OFFSET      0
#define IS_ROOT_SIZE          1
#define IS_ROOT_OFFSET        (NODE_TYPE_OFFSET + NODE_TYPE_SIZE)
#define PARENT_POINTER_SIZE   4
#define PARENT_POINTER_OFFSET (IS_ROOT_OFFSET + IS_ROOT_SIZE)
#define COMMON_NODE_HEADER_SIZE (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

/* ----- Leaf header ----- */
#define LEAF_NODE_NUM_CELLS_SIZE    4
#define LEAF_NODE_NUM_CELLS_OFFSET  (COMMON_NODE_HEADER_SIZE)
#define LEAF_NODE_NEXT_LEAF_SIZE    4
#define LEAF_NODE_NEXT_LEAF_OFFSET  (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
#define LEAF_NODE_HEADER_SIZE       (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE)

/* Leaf cell: key + value(row) */
#define LEAF_NODE_KEY_SIZE          4
#define LEAF_NODE_VALUE_SIZE        ((uint32_t)sizeof(Row))
#define LEAF_NODE_CELL_SIZE         (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)
#define LEAF_NODE_SPACE_FOR_CELLS   (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS         (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

/* ----- Internal header ----- */
/*
 * internal:
 * - num_keys: 4
 * - right_child: 4
 */
#define INTERNAL_NODE_NUM_KEYS_SIZE     4
#define INTERNAL_NODE_NUM_KEYS_OFFSET   (COMMON_NODE_HEADER_SIZE)
#define INTERNAL_NODE_RIGHT_CHILD_SIZE  4
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
#define INTERNAL_NODE_HEADER_SIZE        (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)

/* Internal cell: child_page + key */
#define INTERNAL_NODE_CHILD_SIZE 4
#define INTERNAL_NODE_KEY_SIZE   4
#define INTERNAL_NODE_CELL_SIZE  (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)
#define INTERNAL_NODE_SPACE_FOR_CELLS (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)
#define INTERNAL_NODE_MAX_CELLS (INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE)
#define INTERNAL_NODE_MAX_KEYS  (INTERNAL_NODE_MAX_CELLS)

/* ----- Accessors ----- */

static NodeType get_node_type(void *node) {
    return (NodeType)(*((uint8_t *)node + NODE_TYPE_OFFSET));
}
static void set_node_type(void *node, NodeType t) {
    *((uint8_t *)node + NODE_TYPE_OFFSET) = (uint8_t)t;
}
static bool is_node_root(void *node) {
    return *((uint8_t *)node + IS_ROOT_OFFSET) != 0;
}
static void set_node_root(void *node, bool v) {
    *((uint8_t *)node + IS_ROOT_OFFSET) = v ? 1 : 0;
}
static uint32_t *node_parent(void *node) {
    return (uint32_t *)((uint8_t *)node + PARENT_POINTER_OFFSET);
}

/* Leaf */
static uint32_t *leaf_node_num_cells(void *node) {
    return (uint32_t *)((uint8_t *)node + LEAF_NODE_NUM_CELLS_OFFSET);
}
static uint32_t *leaf_node_next_leaf(void *node) {
    return (uint32_t *)((uint8_t *)node + LEAF_NODE_NEXT_LEAF_OFFSET);
}
static void *leaf_node_cell(void *node, uint32_t cell_num) {
    return (uint8_t *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
static uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
    return (uint32_t *)leaf_node_cell(node, cell_num);
}
static void *leaf_node_value(void *node, uint32_t cell_num) {
    return (uint8_t *)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

/* Internal */
static uint32_t *internal_node_num_keys(void *node) {
    return (uint32_t *)((uint8_t *)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}
static uint32_t *internal_node_right_child(void *node) {
    return (uint32_t *)((uint8_t *)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}
static void *internal_node_cell(void *node, uint32_t cell_num) {
    return (uint8_t *)node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}
static uint32_t *internal_node_child(void *node, uint32_t cell_num) {
    return (uint32_t *)internal_node_cell(node, cell_num);
}
static uint32_t *internal_node_key(void *node, uint32_t cell_num) {
    return (uint32_t *)((uint8_t *)internal_node_cell(node, cell_num) + INTERNAL_NODE_CHILD_SIZE);
}

/* Init */
static void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *node_parent(node) = 0;
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0; // 0 means "no next leaf"
}
static void initialize_internal_node(void *node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *node_parent(node) = 0;
    *internal_node_num_keys(node) = 0;
    *internal_node_right_child(node) = 0;
}

/* Helpers */
static uint32_t get_node_max_key(void *node) {
    if (get_node_type(node) == NODE_LEAF) {
        uint32_t n = *leaf_node_num_cells(node);
        if (n == 0) return 0;
        return *leaf_node_key(node, n - 1);
    } else {
        uint32_t n = *internal_node_num_keys(node);
        if (n == 0) return 0;
        return *internal_node_key(node, n - 1);
    }
}

/* =========================
 * Page allocation
 * ========================= */

static uint32_t allocate_page(Table *t) {
    if (t->header.next_free_page >= TABLE_MAX_PAGES) {
        fatal("Out of pages");
    }
    return t->header.next_free_page++;
}

/* =========================
 * DB open/close
 * ========================= */

static void db_init_new(Table *t) {
    t->header.num_rows = 0;
    t->header.root_page_num = 1;
    t->header.next_free_page = 2;

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
        db_init_new(t);
    } else {
        void *page0 = get_page(pager, 0);
        memcpy(&t->header, page0, sizeof(DBHeader));

        if (t->header.root_page_num == 0 || t->header.root_page_num >= TABLE_MAX_PAGES) {
            fatal("Invalid header/root page (delete old db; format changed)");
        }
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
 * Cursor
 * ========================= */

typedef struct {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

static Cursor *cursor_create(Table *t, uint32_t page_num, uint32_t cell_num) {
    Cursor *c = calloc(1, sizeof(Cursor));
    if (!c) fatal("calloc");
    c->table = t;
    c->page_num = page_num;
    c->cell_num = cell_num;

    void *node = get_page(t->pager, page_num);
    if (get_node_type(node) != NODE_LEAF) fatal("Cursor must point to leaf");

    uint32_t n = *leaf_node_num_cells(node);
    c->end_of_table = (cell_num >= n);
    return c;
}

static void cursor_destroy(Cursor *c) { free(c); }

static void *cursor_value(Cursor *c) {
    void *node = get_page(c->table->pager, c->page_num);
    return leaf_node_value(node, c->cell_num);
}

static void cursor_advance(Cursor *c) {
    void *node = get_page(c->table->pager, c->page_num);
    uint32_t n = *leaf_node_num_cells(node);

    c->cell_num++;
    if (c->cell_num < n) return;

    // move to next leaf if exists
    uint32_t next = *leaf_node_next_leaf(node);
    if (next == 0) {
        c->end_of_table = true;
        return;
    }
    c->page_num = next;
    c->cell_num = 0;

    void *next_node = get_page(c->table->pager, next);
    uint32_t nn = *leaf_node_num_cells(next_node);
    c->end_of_table = (nn == 0);
}

static Cursor *table_start(Table *t) {
    // go to leftmost leaf (root may be internal)
    uint32_t page = t->header.root_page_num;
    while (true) {
        void *node = get_page(t->pager, page);
        if (get_node_type(node) == NODE_LEAF) break;

        // internal: leftmost child is cell 0 child
        if (*internal_node_num_keys(node) == 0) fatal("Corrupt internal node");
        page = *internal_node_child(node, 0);
    }
    return cursor_create(t, page, 0);
}

/* =========================
 * Find (descend internal -> leaf)
 * ========================= */

static uint32_t internal_node_find_child(void *node, int32_t key) {
    // Find the first key >= key, return that index; else return num_keys (means right_child)
    uint32_t num_keys = *internal_node_num_keys(node);

    uint32_t left = 0;
    uint32_t right = num_keys;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        uint32_t mid_key = *internal_node_key(node, mid);
        if ((int32_t)mid_key >= key) right = mid;
        else left = mid + 1;
    }
    return left;
}

static Cursor *leaf_node_find(Table *t, uint32_t page_num, int32_t key) {
    void *node = get_page(t->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    uint32_t left = 0;
    uint32_t right = num_cells;

    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        int32_t mid_key = (int32_t)(*leaf_node_key(node, mid));
        if (mid_key == key) return cursor_create(t, page_num, mid);
        if (mid_key < key) left = mid + 1;
        else right = mid;
    }
    return cursor_create(t, page_num, left); // insertion point
}

static Cursor *table_find(Table *t, int32_t key) {
    uint32_t page = t->header.root_page_num;

    while (true) {
        void *node = get_page(t->pager, page);

        if (get_node_type(node) == NODE_LEAF) {
            return leaf_node_find(t, page, key);
        }

        // internal node: choose child
        uint32_t child_index = internal_node_find_child(node, key);
        uint32_t num_keys = *internal_node_num_keys(node);

        if (child_index == num_keys) {
            page = *internal_node_right_child(node);
        } else {
            page = *internal_node_child(node, child_index);
        }
    }
}

/* =========================
 * Internal root insertion helpers (2-level tree support)
 * ========================= */

static void internal_node_insert_child(void *parent, uint32_t child_page, uint32_t child_max_key) {
    uint32_t num_keys = *internal_node_num_keys(parent);
    if (num_keys >= INTERNAL_NODE_MAX_KEYS) fatal("Internal node full (not handled yet)");

    // If parent currently has 0 keys: set cell0 + right_child
    // In general: insert in sorted order of keys.
    uint32_t index = 0;
    while (index < num_keys && *internal_node_key(parent, index) < child_max_key) {
        index++;
    }

    // If inserting into middle, shift cells right
    if (index < num_keys) {
        for (uint32_t i = num_keys; i > index; i--) {
            memmove(internal_node_cell(parent, i),
                    internal_node_cell(parent, i - 1),
                    INTERNAL_NODE_CELL_SIZE);
        }
    }

    // Place new cell
    *internal_node_child(parent, index) = child_page;
    *internal_node_key(parent, index) = child_max_key;
    *internal_node_num_keys(parent) = num_keys + 1;
}

/*
 * After splitting a leaf that is one of root's children, we need to update
 * the key that represents the old child and add a new child for the new leaf.
 *
 * Root policy here:
 * - We keep a "right_child" pointer for the rightmost child.
 * - Each cell i holds (child, key=max_key_of_child).
 *
 * We'll do a simple rebuild for small root sizes.
 */
static void internal_root_rebuild_for_two_or_three_children(Table *t, uint32_t *children, uint32_t count) {
    // children[] are leaf page nums, must be sorted by max key
    void *root = get_page(t->pager, t->header.root_page_num);
    initialize_internal_node(root);
    set_node_root(root, true);

    for (uint32_t i = 0; i < count; i++) {
        void *child_node = get_page(t->pager, children[i]);
        *node_parent(child_node) = t->header.root_page_num;
    }

    if (count < 2) fatal("Need at least 2 children");
    if (count > 3) fatal("Commit 09 supports up to 3 leaf children under root");

    // For internal node representation:
    // - store first (count-1) children as cells with keys
    // - store last child as right_child
    for (uint32_t i = 0; i < count - 1; i++) {
        uint32_t child = children[i];
        void *child_node = get_page(t->pager, child);
        uint32_t k = get_node_max_key(child_node);
        *internal_node_child(root, i) = child;
        *internal_node_key(root, i) = k;
    }

    *internal_node_num_keys(root) = count - 1;
    *internal_node_right_child(root) = children[count - 1];
}

/* =========================
 * Leaf insert + split
 * ========================= */

static void leaf_node_insert_simple(void *leaf, uint32_t cell_num, int32_t key, const Row *value) {
    uint32_t num_cells = *leaf_node_num_cells(leaf);

    if (cell_num < num_cells) {
        for (uint32_t i = num_cells; i > cell_num; i--) {
            memmove(leaf_node_cell(leaf, i),
                    leaf_node_cell(leaf, i - 1),
                    LEAF_NODE_CELL_SIZE);
        }
    }

    *leaf_node_num_cells(leaf) = num_cells + 1;
    *leaf_node_key(leaf, cell_num) = (uint32_t)key;
    serialize_row(value, leaf_node_value(leaf, cell_num));
}

/*
 * Split a leaf node. Works for:
 * - splitting root leaf (creates internal root)
 * - splitting a leaf under an internal root (supports up to 3 children total)
 */
static void leaf_node_split_and_insert(Table *t, Cursor *cursor, int32_t key, const Row *value) {
    uint32_t old_page = cursor->page_num;
    void *old_leaf = get_page(t->pager, old_page);

    uint32_t new_page = allocate_page(t);
    void *new_leaf = get_page(t->pager, new_page);
    initialize_leaf_node(new_leaf);

    // Link leaf chain: new_leaf becomes old_leaf's next, old_leaf points to new_leaf
    *leaf_node_next_leaf(new_leaf) = *leaf_node_next_leaf(old_leaf);
    *leaf_node_next_leaf(old_leaf) = new_page;

    // Temporary: gather all cells + new one, then split
    uint32_t old_num = *leaf_node_num_cells(old_leaf);
    if (old_num != LEAF_NODE_MAX_CELLS) {
        // We only call split when full; keep strict.
        // (If you want, you can relax this later.)
    }

    uint32_t total = old_num + 1;
    // Split point: left gets ceil(total/2) in SQLite tutorial, but either is ok.
    uint32_t left_count = total / 2;
    uint32_t right_count = total - left_count;

    // We'll rebuild both leaves from scratch into temp arrays of keys+rows
    uint32_t keys[LEAF_NODE_MAX_CELLS + 1];
    Row rows[LEAF_NODE_MAX_CELLS + 1];

    // Read existing into temp
    for (uint32_t i = 0; i < old_num; i++) {
        keys[i] = *leaf_node_key(old_leaf, i);
        deserialize_row(leaf_node_value(old_leaf, i), &rows[i]);
    }

    // Insert new (key,row) into temp at cursor->cell_num
    uint32_t insert_at = cursor->cell_num;
    if (insert_at > old_num) insert_at = old_num;

    for (uint32_t i = total - 1; i > insert_at; i--) {
        keys[i] = keys[i - 1];
        rows[i] = rows[i - 1];
    }
    keys[insert_at] = (uint32_t)key;
    rows[insert_at] = *value;

    // Rebuild old_leaf as left
    *leaf_node_num_cells(old_leaf) = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        leaf_node_insert_simple(old_leaf, i, (int32_t)keys[i], &rows[i]);
    }

    // Rebuild new_leaf as right
    *leaf_node_num_cells(new_leaf) = 0;
    for (uint32_t i = 0; i < right_count; i++) {
        leaf_node_insert_simple(new_leaf, i, (int32_t)keys[left_count + i], &rows[left_count + i]);
    }

    // Parent handling
    if (is_node_root(old_leaf)) {
        // Root leaf split => create internal root in place of old root page.
        uint32_t root_page = t->header.root_page_num;

        // Create left leaf copy page
        uint32_t left_page = allocate_page(t);
        void *left_leaf = get_page(t->pager, left_page);
        initialize_leaf_node(left_leaf);

        // Copy old_leaf (which currently contains left half) into left_leaf
        memcpy(left_leaf, old_leaf, PAGE_SIZE);
        set_node_root(left_leaf, false);

        // old_leaf page becomes internal root
        initialize_internal_node(old_leaf);
        set_node_root(old_leaf, true);

        // Fix leaf links:
        // left_leaf -> new_leaf -> old_next (already in new_leaf)
        *leaf_node_next_leaf(left_leaf) = new_page;

        // Set parents of leaves
        *node_parent(left_leaf) = root_page;
        *node_parent(new_leaf) = root_page;

        // Internal root with 2 children:
        // cell0 = left_leaf with key=max(left_leaf)
        // right_child = new_leaf
        *internal_node_num_keys(old_leaf) = 1;
        *internal_node_child(old_leaf, 0) = left_page;
        *internal_node_key(old_leaf, 0) = get_node_max_key(left_leaf);
        *internal_node_right_child(old_leaf) = new_page;

        // Note: root_page_num remains same
        return;
    }

    // Non-root leaf split: assume parent is root internal (Commit 09 scope)
    uint32_t parent_page = *node_parent(old_leaf);
    void *parent = get_page(t->pager, parent_page);

    if (get_node_type(parent) != NODE_INTERNAL || !is_node_root(parent)) {
        fatal("Commit 09 supports splits under a root internal only");
    }

    // Collect existing children (2 or 3 total)
    // We will rebuild root internal with up to 3 children.
    uint32_t children[3] = {0,0,0};
    uint32_t count = 0;

    uint32_t num_keys = *internal_node_num_keys(parent);
    // cells children:
    for (uint32_t i = 0; i < num_keys; i++) {
        children[count++] = *internal_node_child(parent, i);
    }
    // rightmost:
    children[count++] = *internal_node_right_child(parent);

    // Replace old_page with "old_page" (left) and insert new_page (right)
    // First ensure ordering by max key
    // Compute max keys for each child, then sort small list.
    uint32_t max_keys[3];
    for (uint32_t i = 0; i < count; i++) {
        void *cn = get_page(t->pager, children[i]);
        max_keys[i] = get_node_max_key(cn);
    }

    // Insert new_page into children list
    children[count] = new_page;
    max_keys[count] = get_node_max_key(new_leaf);
    count++;

    if (count > 3) {
        fatal("Commit 09: root internal max 3 leaf children (next commit will generalize)");
    }

    // Simple bubble sort (count <= 3)
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t j = i + 1; j < count; j++) {
            if (max_keys[j] < max_keys[i]) {
                uint32_t tk = max_keys[i]; max_keys[i] = max_keys[j]; max_keys[j] = tk;
                uint32_t tp = children[i]; children[i] = children[j]; children[j] = tp;
            }
        }
    }

    internal_root_rebuild_for_two_or_three_children(t, children, count);
}

/* =========================
 * INSERT and SELECT
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

static void execute_insert(const Statement *s) {
    Cursor *cursor = table_find(table, s->row.id);
    void *leaf = get_page(table->pager, cursor->page_num);

    uint32_t n = *leaf_node_num_cells(leaf);

    // Duplicate check
    if (!cursor->end_of_table && cursor->cell_num < n) {
        int32_t existing = (int32_t)(*leaf_node_key(leaf, cursor->cell_num));
        if (existing == s->row.id) {
            puts("Error: duplicate key.");
            cursor_destroy(cursor);
            return;
        }
    }

    if (n < LEAF_NODE_MAX_CELLS) {
        leaf_node_insert_simple(leaf, cursor->cell_num, s->row.id, &s->row);
    } else {
        leaf_node_split_and_insert(table, cursor, s->row.id, &s->row);
    }

    table->header.num_rows++;
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
