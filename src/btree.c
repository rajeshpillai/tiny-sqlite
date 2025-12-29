/*
 * ============================================================================
 * B-TREE IMPLEMENTATION FOR TINYDB
 * ============================================================================
 * 
 * This file implements a B+tree data structure for storing rows on disk.
 * 
 * OVERVIEW:
 * ---------
 * - Internal nodes: Store keys and child page pointers (no actual data)
 * - Leaf nodes: Store keys and actual row data, linked together for scans
 * - All data resides in leaf nodes; internal nodes only guide searches
 * - Tree grows/shrinks automatically via splitting and rebalancing
 * 
 * B-TREE STRUCTURE EXAMPLE:
 * -------------------------
 * 
 *     [Internal: 50]              Root (page 1)
 *         /    \
 *        /      \
 *   [Leaf: 10,20,30]  →  [Leaf: 60,70,80]    (pages 2,3)
 * 
 * - Internal node contains key "50" separating two children
 * - Left child: all keys ≤ 50
 * - Right child: all keys > 50  
 * - Leaves are linked (→) for efficient range scans
 * 
 * NODE LAYOUT IN MEMORY (4KB pages):
 * -----------------------------------
 * 
 * LEAF NODE:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Common Header (6 bytes)                                     │
 * │  - node_type: 1 byte (0=internal, 1=leaf)                  │
 * │  - is_root: 1 byte (0=no, 1=yes)                           │
 * │  - parent: 4 bytes (page number of parent)                 │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Leaf Header (8 bytes)                                       │
 * │  - num_cells: 4 bytes (how many key-value pairs)           │
 * │  - next_leaf: 4 bytes (page number of next leaf, 0 if none)│
 * ├─────────────────────────────────────────────────────────────┤
 * │ Cell 0: [key: 4 bytes][row: 291 bytes]                     │
 * │ Cell 1: [key: 4 bytes][row: 291 bytes]                     │
 * │ ...                                                         │
 * │ Cell N: [key: 4 bytes][row: 291 bytes]                     │
 * └─────────────────────────────────────────────────────────────┘
 * 
 * INTERNAL NODE:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Common Header (6 bytes)                                     │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Internal Header (8 bytes)                                   │
 * │  - num_keys: 4 bytes (number of child pointers - 1)        │
 * │  - right_child: 4 bytes (page number of rightmost child)   │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Cell 0: [child_ptr: 4 bytes][max_key: 4 bytes]            │
 * │ Cell 1: [child_ptr: 4 bytes][max_key: 4 bytes]            │
 * │ ...                                                         │
 * │ Cell N: [child_ptr: 4 bytes][max_key: 4 bytes]            │
 * │ Right Child: stored in header, no key needed               │
 * └─────────────────────────────────────────────────────────────┘
 * 
 * KEY INVARIANTS:
 * ---------------
 * 1. All keys in a node are sorted in ascending order
 * 2. Leaf nodes: MIN_CELLS ≤ cells ≤ MAX_CELLS (except root)
 * 3. Internal nodes: MIN_KEYS ≤ num_keys ≤ MAX_KEYS (except root)
 * 4. Root can have as few as 1 cell (leaf) or 0 keys (internal with 1 child)
 * 5. All leaves are at the same depth (balanced tree)
 * 
 * OPERATIONS:
 * -----------
 * - INSERT: Find leaf, insert sorted, split if overflow, propagate up
 * - DELETE: Find leaf, remove key, rebalance if underflow (borrow/merge)
 * - SEARCH: Descend from root following key comparisons
 * - SCAN: Start at leftmost leaf, follow next_leaf pointers
 * 
 * REBALANCING STRATEGY:
 * ---------------------
 * When a node becomes underfull after deletion:
 * 1. Try to borrow from left sibling (if it has extra cells/keys)
 * 2. Try to borrow from right sibling (if it has extra cells/keys)
 * 3. Merge with a sibling (combine into one node)
 * 4. Recursively rebalance parent if it becomes underfull
 * 5. Shrink tree height if root becomes empty
 * 
 * ============================================================================
 */

#include "btree.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Minimum thresholds for rebalancing */
#define LEAF_NODE_MIN_CELLS (LEAF_NODE_MAX_CELLS / 2)
#define INTERNAL_NODE_MIN_KEYS (INTERNAL_NODE_MAX_KEYS / 2)


static void die(const char *msg) {
    perror(msg);
    exit(1);
}

/* ============================================================
 * Node layout (B+tree style)
 * - Internal node stores children and "max key of child" for all except rightmost.
 * - right_child is stored separately.
 * ============================================================ */

/* Common header */
#define NODE_TYPE_SIZE        1
#define IS_ROOT_SIZE          1
#define PARENT_POINTER_SIZE   4

#define NODE_TYPE_OFFSET      0
#define IS_ROOT_OFFSET        (NODE_TYPE_OFFSET + NODE_TYPE_SIZE)
#define PARENT_POINTER_OFFSET (IS_ROOT_OFFSET + IS_ROOT_SIZE)

#define COMMON_NODE_HEADER_SIZE (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

/* Leaf header: num_cells + next_leaf */
#define LEAF_NODE_NUM_CELLS_SIZE   4
#define LEAF_NODE_NEXT_LEAF_SIZE   4

#define LEAF_NODE_NUM_CELLS_OFFSET (COMMON_NODE_HEADER_SIZE)
#define LEAF_NODE_NEXT_LEAF_OFFSET (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
#define LEAF_NODE_HEADER_SIZE      (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE)

/* Leaf cell: key + value(row) */
#define LEAF_NODE_KEY_SIZE    4
#define LEAF_NODE_VALUE_SIZE  ((uint32_t)sizeof(Row))
#define LEAF_NODE_CELL_SIZE   (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)

#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS       (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

/* Internal header: num_keys + right_child */
#define INTERNAL_NODE_NUM_KEYS_SIZE    4
#define INTERNAL_NODE_RIGHT_CHILD_SIZE 4

#define INTERNAL_NODE_NUM_KEYS_OFFSET  (COMMON_NODE_HEADER_SIZE)
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
#define INTERNAL_NODE_HEADER_SIZE      (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)

/* Internal cell: child + key(max key of that child) */
#define INTERNAL_NODE_CHILD_SIZE 4
#define INTERNAL_NODE_KEY_SIZE   4
#define INTERNAL_NODE_CELL_SIZE  (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)

#define INTERNAL_NODE_SPACE_FOR_CELLS (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)
#define INTERNAL_NODE_MAX_KEYS        (INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE)
#define INTERNAL_NODE_MAX_CHILDREN    (INTERNAL_NODE_MAX_KEYS + 1)

/* ---------- Common accessors ---------- */

static NodeType get_node_type(void *node) {
    return (NodeType)(*((uint8_t *)node + NODE_TYPE_OFFSET));
}
static void set_node_type(void *node, NodeType type) {
    *((uint8_t *)node + NODE_TYPE_OFFSET) = (uint8_t)type;
}
static bool is_node_root(void *node) {
    return *((uint8_t *)node + IS_ROOT_OFFSET) != 0;
}
static void set_node_root(void *node, bool is_root) {
    *((uint8_t *)node + IS_ROOT_OFFSET) = is_root ? 1 : 0;
}
static uint32_t *node_parent(void *node) {
    return (uint32_t *)((uint8_t *)node + PARENT_POINTER_OFFSET);
}

/* ---------- Leaf accessors ---------- */

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
    return (uint32_t *)((uint8_t *)leaf_node_cell(node, cell_num));
}
static void *leaf_node_value(void *node, uint32_t cell_num) {
    return (uint8_t *)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

/* ---------- Internal accessors ---------- */

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

/* Forward declarations for rebalancing */
static void internal_node_update_key_for_child(Table *t, uint32_t parent_page, uint32_t child_page);
static void internal_node_remove_child(Table *t, uint32_t parent_page, uint32_t child_page);
static void internal_node_rebuild(Table *t, uint32_t internal_page, uint32_t *children, uint32_t count);


static void print_indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) printf("  ");
}

static void print_node(Table *t, uint32_t page, uint32_t level) {
    void *node = pager_get_page(t->pager, page);

    print_indent(level);

    if (get_node_type(node) == NODE_LEAF) {
        uint32_t n = *leaf_node_num_cells(node);
        printf("- leaf (page %u, cells %u): ", page, n);
        for (uint32_t i = 0; i < n; i++) {
            printf("%u ", *leaf_node_key(node, i));
        }
        printf("\n");
        return;
    }

    uint32_t keys = *internal_node_num_keys(node);
    printf("- internal (page %u, keys %u)\n", page, keys);

    for (uint32_t i = 0; i < keys; i++) {
        uint32_t child = *internal_node_child(node, i);
        print_node(t, child, level + 1);
        print_indent(level + 1);
        printf("key <= %u\n", *internal_node_key(node, i));
    }

    uint32_t right = *internal_node_right_child(node);
    print_node(t, right, level + 1);
}

void btree_print(Table *t) {
    printf("B-Tree structure:\n");
    print_node(t, t->header.root_page_num, 0);
}

/* ============================================================
 * LEAF NODE REBALANCING
 * ============================================================ */

/*
 * find_leaf_siblings - Locate left and right siblings of a leaf node
 * 
 * Given a leaf node, finds its siblings by looking at the parent's children.
 * 
 * EXAMPLE: Parent has 3 children stored as:
 * 
 *   Parent Internal Node (num_keys = 2):
 *   ┌────────────────────────────────────────────┐
 *   │ child[0]=pg2, key[0]=10                   │
 *   │ child[1]=pg3, key[1]=20                   │
 *   │ right_child=pg4                           │
 *   └────────────────────────────────────────────┘
 *        ↓             ↓            ↓
 *     [pg2]         [pg3]        [pg4]
 *   (keys≤10)    (10<keys≤20)  (keys>20)
 * 
 * If searching for siblings of pg3:
 *   - left_page = pg2  (child[i-1])
 *   - right_page = pg4 (right_child, since i+1 == num_keys)
 * 
 * CRITICAL: When i+1 == num_keys, we must use right_child,
 *           NOT child[i+1] which doesn't exist!
 * 
 * Returns: true if siblings found, false if node is root
 */
static bool find_leaf_siblings(
    Table *t,
    uint32_t leaf_page,
    uint32_t *left_page,
    uint32_t *right_page,
    uint32_t *parent_page
) {
    void *leaf = pager_get_page(t->pager, leaf_page);
    if (is_node_root(leaf)) return false;  // Root has no siblings

    *parent_page = *node_parent(leaf);
    void *parent = pager_get_page(t->pager, *parent_page);

    uint32_t num_keys = *internal_node_num_keys(parent);

    /* Search through parent's children to find our position */
    for (uint32_t i = 0; i <= num_keys; i++) {
        uint32_t child =
            (i == num_keys)
                ? *internal_node_right_child(parent)
                : *internal_node_child(parent, i);

        if (child == leaf_page) {
            /* Found ourselves! Now identify siblings */
            
            /* Left sibling: child[i-1] if i > 0, else none */
            *left_page  = (i > 0) ? *internal_node_child(parent, i - 1) : 0;
            
            /* Right sibling: more complex due to right_child storage */
            if (i == num_keys) {
                /* We're the rightmost child, no right sibling */
                *right_page = 0;
            } else {
                /* Right sibling is either child[i+1] or right_child */
                *right_page = (i + 1 < num_keys) 
                    ? *internal_node_child(parent, i + 1)
                    : *internal_node_right_child(parent);
            }
            return true;
        }
    }
    return false;
}

/*
 * try_borrow_from_left - Borrow a cell from left sibling if possible
 * 
 * When a leaf becomes underfull, try to borrow from its left sibling.
 * Only borrow if left sibling has more than the minimum cells.
 * 
 * BEFORE:
 *   ┌──────────────────┐     ┌───────────────┐
 *   │ Left: [1,2,3,4,5]│ →   │ Curr: [10,11] │  (underfull!)
 *   └──────────────────┘     └───────────────┘
 *        (7 cells)               (2 cells)
 * 
 * AFTER borrowing last cell from left:
 *   ┌──────────────────┐     ┌─────────────────┐
 *   │ Left: [1,2,3,4]  │ →   │ Curr: [5,10,11] │  (now OK!)
 *   └──────────────────┘     └─────────────────┘
 *        (6 cells)               (3 cells)
 * 
 * STEPS:
 * 1. Shift all cells in current node right to make space at index 0
 * 2. Copy last cell from left sibling to current[0]
 * 3. Update num_cells in both nodes
 * 4. Update parent key for left sibling (its max key changed)
 * 
 * Returns: true if borrowed, false if left has too few cells
 */
static bool try_borrow_from_left(
    Table *t,
    uint32_t leaf_page,
    uint32_t left_page,
    uint32_t parent_page
) {
    if (!left_page) return false;  // No left sibling exists

    void *leaf = pager_get_page(t->pager, leaf_page);
    void *left = pager_get_page(t->pager, left_page);

    /* Can only borrow if left has more than minimum */
    if (*leaf_node_num_cells(left) <= LEAF_NODE_MIN_CELLS)
        return false;

    /* Shift current node's cells right to make room at position 0 */
    memmove(
        leaf_node_cell(leaf, 1),
        leaf_node_cell(leaf, 0),
        (*leaf_node_num_cells(leaf)) * LEAF_NODE_CELL_SIZE
    );

    /* Copy last cell from left sibling to current node's first position */
    uint32_t borrow_idx = *leaf_node_num_cells(left) - 1;
    memcpy(
        leaf_node_cell(leaf, 0),
        leaf_node_cell(left, borrow_idx),
        LEAF_NODE_CELL_SIZE
    );

    /* Update cell counts */
    (*leaf_node_num_cells(left))--;
    (*leaf_node_num_cells(leaf))++;

    /* Update parent's key for left sibling (its max key changed!) */
    internal_node_update_key_for_child(t, parent_page, left_page);
    return true;
}

/*
 * try_borrow_from_right - Borrow a cell from right sibling if possible
 * 
 * Similar to borrowing from left, but takes first cell from right sibling
 * and appends it to the end of current node.
 * 
 * BEFORE:
 *   ┌───────────────┐     ┌────────────────────┐
 *   │ Curr: [1,2]   │ →   │ Right: [10,11,12,13]│
 *   └───────────────┘     └────────────────────┘
 *      (2 cells)              (7 cells)
 * 
 * AFTER borrowing first cell from right:
 *   ┌─────────────────┐     ┌────────────────┐
 *   │ Curr: [1,2,10]  │ →   │ Right: [11,12,13]│
 *   └─────────────────┘     └────────────────┘
 *      (3 cells)              (6 cells)
 * 
 * STEPS:
 * 1. Copy first cell from right sibling to end of current node
 * 2. Shift all cells in right sibling left (removing first cell)
 * 3. Update num_cells in both nodes
 * 4. Update parent key for current node (its max key changed)
 */
static bool try_borrow_from_right(
    Table *t,
    uint32_t leaf_page,
    uint32_t right_page,
    uint32_t parent_page
) {
    if (!right_page) return false;  // No right sibling exists

    void *leaf = pager_get_page(t->pager, leaf_page);
    void *right = pager_get_page(t->pager, right_page);

    /* Can only borrow if right has more than minimum */
    if (*leaf_node_num_cells(right) <= LEAF_NODE_MIN_CELLS)
        return false;

    /* Append first cell of right sibling to end of current node */
    memcpy(
        leaf_node_cell(leaf, *leaf_node_num_cells(leaf)),
        leaf_node_cell(right, 0),
        LEAF_NODE_CELL_SIZE
    );

    /* Shift right sibling's cells left to remove first cell */
    memmove(
        leaf_node_cell(right, 0),
        leaf_node_cell(right, 1),
        (*leaf_node_num_cells(right) - 1) * LEAF_NODE_CELL_SIZE
    );

    /* Update cell counts */
    (*leaf_node_num_cells(right))--;
    (*leaf_node_num_cells(leaf))++;

    /* Update parent's key for current node (its max key changed!) */
    internal_node_update_key_for_child(t, parent_page, leaf_page);
    return true;
}

/*
 * merge_leaf_nodes - Combine two underfull leaf nodes into one
 * 
 * When borrowing isn't possible, merge two siblings into one node.
 * All cells from right node are copied to left node, then right is removed.
 * 
 * BEFORE:
 *            Parent [key=5]
 *              /        \
 *   ┌──────────┐     ┌───────────┐
 *   │ Left:[1,2]│ →   │Right:[6,7]│ →  (next leaf)
 *   └──────────┘     └───────────┘
 *     (2 cells)        (2 cells)
 * 
 * AFTER merging:
 *          Parent (one child removed)
 *              |
 *   ┌────────────────┐
 *   │ Left:[1,2,6,7] │ →  (next leaf)
 *   └────────────────┘
 *       (4 cells)
 * 
 * STEPS:
 * 1. Copy all cells from right to end of left
 * 2. Update left's next_leaf pointer to skip over right
 * 3. Remove right node from parent (may trigger parent rebalancing)
 */
static void merge_leaf_nodes(
    Table *t,
    uint32_t left_page,
    uint32_t right_page,
    uint32_t parent_page
) {
    void *left = pager_get_page(t->pager, left_page);
    void *right = pager_get_page(t->pager, right_page);

    uint32_t left_n = *leaf_node_num_cells(left);
    uint32_t right_n = *leaf_node_num_cells(right);

    /* Copy all cells from right to end of left */
    memcpy(
        leaf_node_cell(left, left_n),
        leaf_node_cell(right, 0),
        right_n * LEAF_NODE_CELL_SIZE
    );

    /* Update left's metadata */
    *leaf_node_num_cells(left) = left_n + right_n;
    *leaf_node_next_leaf(left) = *leaf_node_next_leaf(right);  // Skip over right

    /* Remove right node from parent (this may trigger parent rebalancing!) */
    internal_node_remove_child(t, parent_page, right_page);
}

/*
 * maybe_shrink_root - Reduce tree height if root becomes empty
 * 
 * After deleting and merging nodes, the root internal node may end up with
 * only one child (num_keys = 0). In this case, we promote the child to
 * become the new root, reducing the tree height by 1.
 * 
 * BEFORE (3 levels):
 *         Root Internal [num_keys=0]
 *              |
 *          (only child)
 *              |
 *         Internal Node
 *           /    \
 *        Leaf  Leaf
 * 
 * AFTER shrinking (2 levels):
 *         Internal Node (new root!)
 *           /    \
 *        Leaf  Leaf
 * 
 * This is the opposite of split_root which increases height.
 * 
 * NOTE: This only applies to internal roots. A leaf root can have
 *       as few as 1 cell and doesn't shrink.
 */
static void maybe_shrink_root(Table *t) {
    void *root = pager_get_page(t->pager, t->header.root_page_num);

    /* Only shrink if root is internal and has no keys (only right_child) */
    if (get_node_type(root) == NODE_INTERNAL &&
        *internal_node_num_keys(root) == 0) {

        /* The only remaining child becomes the new root */
        uint32_t new_root = *internal_node_right_child(root);

        void *child = pager_get_page(t->pager, new_root);
        set_node_root(child, true);
        *node_parent(child) = 0;  // Root has no parent

        t->header.root_page_num = new_root;
    }
}

/*
 * rebalance_leaf - Orchestrate leaf rebalancing after deletion
 * 
 * This is the main entry point for leaf rebalancing. It tries multiple
 * strategies in order of preference:
 * 
 * 1. Borrow from left sibling (if possible)
 * 2. Borrow from right sibling (if possible)
 * 3. Merge with left sibling
 * 4. Merge with right sibling
 * 5. Shrink root if it became empty
 * 
 * STRATEGY SELECTION:
 * - Borrowing is preferred (keeps nodes more balanced)
 * - Merging is last resort (may trigger cascading rebalancing)
 * - Always try left before right (arbitrary choice)
 */
static void rebalance_leaf(Table *t, uint32_t leaf_page) {
    uint32_t left = 0, right = 0, parent = 0;

    /* Find siblings. If root, no rebalancing needed */
    if (!find_leaf_siblings(t, leaf_page, &left, &right, &parent))
        return;

    /* Try borrowing first (preferred) */
    if (try_borrow_from_left(t, leaf_page, left, parent)) return;
    if (try_borrow_from_right(t, leaf_page, right, parent)) return;

    /* Borrowing failed, must merge */
    if (left) merge_leaf_nodes(t, left, leaf_page, parent);
    else if (right) merge_leaf_nodes(t, leaf_page, right, parent);

    /* Check if tree height should be reduced */
    maybe_shrink_root(t);
}

/* ============================================================
 * Internal Node Rebalancing
 * ============================================================ */

static bool find_internal_siblings(
    Table *t,
    uint32_t internal_page,
    uint32_t *left_page,
    uint32_t *right_page,
    uint32_t *parent_page
) {
    void *internal = pager_get_page(t->pager, internal_page);
    if (is_node_root(internal)) return false;

    *parent_page = *node_parent(internal);
    void *parent = pager_get_page(t->pager, *parent_page);

    uint32_t num_keys = *internal_node_num_keys(parent);

    for (uint32_t i = 0; i <= num_keys; i++) {
        uint32_t child =
            (i == num_keys)
                ? *internal_node_right_child(parent)
                : *internal_node_child(parent, i);

        if (child == internal_page) {
            *left_page  = (i > 0) ? *internal_node_child(parent, i - 1) : 0;
            // Right sibling: if we're the rightmost child, there's no right sibling
            if (i == num_keys) {
                *right_page = 0;
            } else {
                *right_page = (i + 1 < num_keys) 
                    ? *internal_node_child(parent, i + 1)
                    : *internal_node_right_child(parent);
            }
            return true;
        }
    }
    return false;
}

static bool try_borrow_from_left_internal(
    Table *t,
    uint32_t internal_page,
    uint32_t left_page,
    uint32_t parent_page
) {
    if (!left_page) return false;

    void *internal = pager_get_page(t->pager, internal_page);
    void *left = pager_get_page(t->pager, left_page);
    
    // Ensure both nodes are internal nodes
    if (get_node_type(left) != NODE_INTERNAL) return false;
    if (get_node_type(internal) != NODE_INTERNAL) return false;

    if (*internal_node_num_keys(left) <= INTERNAL_NODE_MIN_KEYS)
        return false;

    // Collect all children from left sibling
    uint32_t left_num_keys = *internal_node_num_keys(left);
    uint32_t left_children[INTERNAL_NODE_MAX_CHILDREN];
    uint32_t left_count = 0;

    for (uint32_t i = 0; i < left_num_keys; i++) {
        left_children[left_count++] = *internal_node_child(left, i);
    }
    left_children[left_count++] = *internal_node_right_child(left);

    // Take the last child from left
    uint32_t borrowed_child = left_children[--left_count];

    // Rebuild left with remaining children
    internal_node_rebuild(t, left_page, left_children, left_count);

    // Collect all children from current node
    uint32_t curr_num_keys = *internal_node_num_keys(internal);
    uint32_t curr_children[INTERNAL_NODE_MAX_CHILDREN];
    uint32_t curr_count = 0;

    for (uint32_t i = 0; i < curr_num_keys; i++) {
        curr_children[curr_count++] = *internal_node_child(internal, i);
    }
    curr_children[curr_count++] = *internal_node_right_child(internal);

    // Insert borrowed child at the beginning
    memmove(&curr_children[1], &curr_children[0], curr_count * sizeof(uint32_t));
    curr_children[0] = borrowed_child;
    curr_count++;

    // Rebuild current node
    internal_node_rebuild(t, internal_page, curr_children, curr_count);

    // Update parent keys
    internal_node_update_key_for_child(t, parent_page, left_page);
    internal_node_update_key_for_child(t, parent_page, internal_page);

    return true;
}

static bool try_borrow_from_right_internal(
    Table *t,
    uint32_t internal_page,
    uint32_t right_page,
    uint32_t parent_page
) {
    if (!right_page) return false;

    void *internal = pager_get_page(t->pager, internal_page);
    void *right = pager_get_page(t->pager, right_page);
    
    // Ensure both nodes are internal nodes
    if (get_node_type(right) != NODE_INTERNAL) return false;
    if (get_node_type(internal) != NODE_INTERNAL) return false;

    if (*internal_node_num_keys(right) <= INTERNAL_NODE_MIN_KEYS)
        return false;

    // Collect all children from right sibling
    uint32_t right_num_keys = *internal_node_num_keys(right);
    uint32_t right_children[INTERNAL_NODE_MAX_CHILDREN];
    uint32_t right_count = 0;

    for (uint32_t i = 0; i < right_num_keys; i++) {
        right_children[right_count++] = *internal_node_child(right, i);
    }
    right_children[right_count++] = *internal_node_right_child(right);

    // Take the first child from right
    uint32_t borrowed_child = right_children[0];
    memmove(&right_children[0], &right_children[1], (right_count - 1) * sizeof(uint32_t));
    right_count--;

    // Rebuild right with remaining children
    internal_node_rebuild(t, right_page, right_children, right_count);

    // Collect all children from current node
    uint32_t curr_num_keys = *internal_node_num_keys(internal);
    uint32_t curr_children[INTERNAL_NODE_MAX_CHILDREN];
    uint32_t curr_count = 0;

    for (uint32_t i = 0; i < curr_num_keys; i++) {
        curr_children[curr_count++] = *internal_node_child(internal, i);
    }
    curr_children[curr_count++] = *internal_node_right_child(internal);

    // Append borrowed child
    curr_children[curr_count++] = borrowed_child;

    // Rebuild current node
    internal_node_rebuild(t, internal_page, curr_children, curr_count);

    // Update parent keys
    internal_node_update_key_for_child(t, parent_page, internal_page);
    internal_node_update_key_for_child(t, parent_page, right_page);

    return true;
}

static void merge_internal_nodes(
    Table *t,
    uint32_t left_page,
    uint32_t right_page,
    uint32_t parent_page
) {
    void *left = pager_get_page(t->pager, left_page);
    void *right = pager_get_page(t->pager, right_page);
    
    // Ensure both nodes are internal nodes
    if (get_node_type(left) != NODE_INTERNAL) return;
    if (get_node_type(right) != NODE_INTERNAL) return;

    // Collect all children from both nodes
    uint32_t all_children[INTERNAL_NODE_MAX_CHILDREN * 2];
    uint32_t count = 0;

    uint32_t left_num_keys = *internal_node_num_keys(left);
    for (uint32_t i = 0; i < left_num_keys; i++) {
        all_children[count++] = *internal_node_child(left, i);
    }
    all_children[count++] = *internal_node_right_child(left);

    uint32_t right_num_keys = *internal_node_num_keys(right);
    for (uint32_t i = 0; i < right_num_keys; i++) {
        all_children[count++] = *internal_node_child(right, i);
    }
    all_children[count++] = *internal_node_right_child(right);

    // Rebuild left with all children
    internal_node_rebuild(t, left_page, all_children, count);

    // Remove right from parent (this may trigger recursive rebalancing)
    internal_node_remove_child(t, parent_page, right_page);
}

static void rebalance_internal(Table *t, uint32_t internal_page) {
    uint32_t left = 0, right = 0, parent = 0;

    if (!find_internal_siblings(t, internal_page, &left, &right, &parent))
        return;

    if (try_borrow_from_left_internal(t, internal_page, left, parent)) return;
    if (try_borrow_from_right_internal(t, internal_page, right, parent)) return;

    // merge
    if (left) merge_internal_nodes(t, left, internal_page, parent);
    else if (right) merge_internal_nodes(t, internal_page, right, parent);

    maybe_shrink_root(t);
}


/* ============================================================
 * Init
 * ============================================================ */

static void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *node_parent(node) = 0;
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
}
static void initialize_internal_node(void *node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *node_parent(node) = 0;
    *internal_node_num_keys(node) = 0;
    *internal_node_right_child(node) = 0;
}

/* ============================================================
 * Serialization
 * ============================================================ */

static void serialize_row(const Row *src, void *dst) { memcpy(dst, src, sizeof(Row)); }
static void deserialize_row(const void *src, Row *dst) { memcpy(dst, src, sizeof(Row)); }

/* ============================================================
 * Page allocation
 * ============================================================ */

static uint32_t allocate_page(Table *t) {
    if (t->header.next_free_page >= TABLE_MAX_PAGES) die("out of pages");
    return t->header.next_free_page++;
}

/* ============================================================
 * Max key of a node
 * ============================================================ */

static uint32_t get_node_max_key(Table *t, uint32_t page_num) {
    void *node = pager_get_page(t->pager, page_num);

    if (get_node_type(node) == NODE_LEAF) {
        uint32_t n = *leaf_node_num_cells(node);
        if (n == 0) return 0;
        return *leaf_node_key(node, n - 1);
    }

    // internal: max key is max key of rightmost child
    uint32_t right = *internal_node_right_child(node);
    return get_node_max_key(t, right);
}

/* ============================================================
 * Internal node rebuild (simple + correct)
 * Build from children[] sorted by their max key.
 * ============================================================ */

static void internal_node_rebuild(Table *t, uint32_t internal_page, uint32_t *children, uint32_t count) {
    if (count < 2) die("internal rebuild needs >=2 children");
    if (count > INTERNAL_NODE_MAX_CHILDREN) die("internal rebuild too many children");

    void *node = pager_get_page(t->pager, internal_page);

    bool root_flag = is_node_root(node);
    uint32_t parent_page = *node_parent(node);

    initialize_internal_node(node);
    set_node_root(node, root_flag);
    *node_parent(node) = parent_page;

    // Set parent pointers on children
    for (uint32_t i = 0; i < count; i++) {
        void *child_node = pager_get_page(t->pager, children[i]);
        *node_parent(child_node) = internal_page;
        // keep child's is_root as-is (should be false unless it's the actual root page)
        if (is_node_root(child_node)) set_node_root(child_node, false);
    }

    // Fill cells for first count-1 children
    uint32_t num_keys = count - 1;
    *internal_node_num_keys(node) = num_keys;

    for (uint32_t i = 0; i < num_keys; i++) {
        *internal_node_child(node, i) = children[i];
        *internal_node_key(node, i) = get_node_max_key(t, children[i]);
    }

    // Last child is right_child
    *internal_node_right_child(node) = children[count - 1];
}

/* Small sort for children by their max key (count is small) */
static void sort_children_by_maxkey(Table *t, uint32_t *children, uint32_t count) {
    uint32_t keys[INTERNAL_NODE_MAX_CHILDREN];
    for (uint32_t i = 0; i < count; i++) keys[i] = get_node_max_key(t, children[i]);

    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t j = i + 1; j < count; j++) {
            if (keys[j] < keys[i]) {
                uint32_t tk = keys[i]; keys[i] = keys[j]; keys[j] = tk;
                uint32_t tp = children[i]; children[i] = children[j]; children[j] = tp;
            }
        }
    }
}

/* ============================================================
 * Find child in internal node by key
 * Keys stored are max keys of child cells (not right child).
 * Choose first key >= target else choose right_child.
 * ============================================================ */

static uint32_t internal_node_find_child(void *internal, int32_t key) {
    uint32_t num_keys = *internal_node_num_keys(internal);

    uint32_t left = 0;
    uint32_t right = num_keys;

    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        uint32_t mid_key = *internal_node_key(internal, mid);
        if ((int32_t)mid_key >= key) right = mid;
        else left = mid + 1;
    }

    // left == first index where key <= mid_key
    return left;
}

/* ============================================================
 * Find leaf position (binary search in leaf)
 * ============================================================ */

static Cursor *leaf_node_find(Table *t, uint32_t leaf_page, int32_t key) {
    void *leaf = pager_get_page(t->pager, leaf_page);
    uint32_t n = *leaf_node_num_cells(leaf);

    uint32_t left = 0;
    uint32_t right = n;

    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        int32_t mid_key = (int32_t)(*leaf_node_key(leaf, mid));

        if (mid_key == key) {
            Cursor *c = calloc(1, sizeof(Cursor));
            c->table = t; c->page_num = leaf_page; c->cell_num = mid;
            c->end_of_table = false;
            return c;
        } else if (mid_key < key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    Cursor *c = calloc(1, sizeof(Cursor));
    c->table = t; c->page_num = leaf_page; c->cell_num = left;
    c->end_of_table = (left >= n);
    return c;
}

/* ============================================================
 * Descend tree to find leaf for a key
 * ============================================================ */

Cursor *btree_table_find(Table *t, int32_t key) {
    uint32_t page = t->header.root_page_num;

    while (true) {
        void *node = pager_get_page(t->pager, page);
        if (get_node_type(node) == NODE_LEAF) {
            return leaf_node_find(t, page, key);
        }

        uint32_t child_index = internal_node_find_child(node, key);
        uint32_t num_keys = *internal_node_num_keys(node);

        if (child_index == num_keys) {
            page = *internal_node_right_child(node);
        } else {
            page = *internal_node_child(node, child_index);
        }
    }
}

/* ============================================================
 * Cursor API
 * ============================================================ */

Cursor *btree_table_start(Table *t) {
    uint32_t page = t->header.root_page_num;

    // go to leftmost leaf
    while (true) {
        void *node = pager_get_page(t->pager, page);
        if (get_node_type(node) == NODE_LEAF) break;

        // leftmost child is child(0)
        if (*internal_node_num_keys(node) == 0) die("corrupt internal");
        page = *internal_node_child(node, 0);
    }

    Cursor *c = calloc(1, sizeof(Cursor));
    c->table = t;
    c->page_num = page;
    c->cell_num = 0;

    void *leaf = pager_get_page(t->pager, page);
    c->end_of_table = (*leaf_node_num_cells(leaf) == 0);
    return c;
}

void *btree_cursor_value(Cursor *c) {
    void *leaf = pager_get_page(c->table->pager, c->page_num);
    return leaf_node_value(leaf, c->cell_num);
}

void btree_cursor_advance(Cursor *c) {
    void *leaf = pager_get_page(c->table->pager, c->page_num);
    uint32_t n = *leaf_node_num_cells(leaf);

    c->cell_num++;
    if (c->cell_num < n) return;

    uint32_t next = *leaf_node_next_leaf(leaf);
    if (next == 0) {
        c->end_of_table = true;
        return;
    }

    c->page_num = next;
    c->cell_num = 0;

    void *leaf2 = pager_get_page(c->table->pager, next);
    c->end_of_table = (*leaf_node_num_cells(leaf2) == 0);
}

void btree_cursor_free(Cursor *c) { free(c); }

/* ============================================================
 * Parent updates
 * - Update key for a given child entry if present in parent's cells
 * - If the child is the parent's right_child, no key stored => nothing to update.
 * ============================================================ */

static void internal_node_update_key_for_child(Table *t, uint32_t parent_page, uint32_t child_page) {
    void *parent = pager_get_page(t->pager, parent_page);
    uint32_t num_keys = *internal_node_num_keys(parent);

    for (uint32_t i = 0; i < num_keys; i++) {
        if (*internal_node_child(parent, i) == child_page) {
            *internal_node_key(parent, i) = get_node_max_key(t, child_page);
            return;
        }
    }
    // if it's right_child => no stored key
}

/* Remove a child from internal node */
static void internal_node_remove_child(Table *t, uint32_t parent_page, uint32_t child_page) {
    void *parent = pager_get_page(t->pager, parent_page);
    uint32_t num_keys = *internal_node_num_keys(parent);

    // Collect all children except the one to remove
    uint32_t children[INTERNAL_NODE_MAX_CHILDREN];
    uint32_t count = 0;

    for (uint32_t i = 0; i < num_keys; i++) {
        uint32_t child = *internal_node_child(parent, i);
        if (child != child_page) {
            children[count++] = child;
        }
    }

    uint32_t right = *internal_node_right_child(parent);
    if (right != child_page) {
        children[count++] = right;
    }

    // Rebuild with remaining children
    if (count >= 2) {
        internal_node_rebuild(t, parent_page, children, count);
    } else if (count == 1) {
        // Only one child left - this will be handled by maybe_shrink_root
        *internal_node_num_keys(parent) = 0;
        *internal_node_right_child(parent) = children[0];
    }

    // Check if parent became underfull and needs rebalancing
    uint32_t min_keys = is_node_root(parent) ? 0 : INTERNAL_NODE_MIN_KEYS;
    if (*internal_node_num_keys(parent) < min_keys) {
        rebalance_internal(t, parent_page);
    }
}

/* ============================================================
 * TREE GROWTH: Split Root and Create New Level
 * ============================================================ */

/*
 * create_new_root - Increase tree height when root splits
 * 
 * When the root node splits (either leaf or internal), we need to create
 * a new root internal node that points to both halves. This increases
 * the tree height by 1.
 * 
 * BEFORE (root is full and about to split):
 *         Root Leaf [1,2,3,4,5,6,7] (FULL - 7 cells)
 * 
 * AFTER split (tree height increased from 1 to 2):
 *         New Root Internal [key=3]
 *              /           \
 *     Left [1,2,3]     Right [4,5,6,7]
 *     (old root)        (new page)
 * 
 * STEPS:
 * 1. Allocate new page for left child
 * 2. Copy old root contents to left child
 * 3. Transform root page into new internal node
 * 4. Set parent pointers correctly
 * 5. Build new root with 2 children (left and right)
 * 
 * NOTE: We reuse the root page number to avoid updating the DBHeader.
 *       The old root content is moved to a new page (left child).
 * 
 * Parameters:
 *   right_child_page: The newly created right half from the split
 */
static void create_new_root(Table *t, uint32_t right_child_page) {
    uint32_t root_page = t->header.root_page_num;
    void *root = pager_get_page(t->pager, root_page);

    /* Allocate new page to hold old root's content (becomes left child) */
    uint32_t left_child_page = allocate_page(t);
    void *left_child = pager_get_page(t->pager, left_child_page);

    /* Move old root content to left_child */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);  // No longer root
    *node_parent(left_child) = root_page;

    /* Transform root into internal node */
    initialize_internal_node(root);
    set_node_root(root, true);
    *node_parent(root) = 0;  // Root has no parent

    /* Build new root with 2 children */
    uint32_t children[2] = { left_child_page, right_child_page };
    sort_children_by_maxkey(t, children, 2);
    internal_node_rebuild(t, root_page, children, 2);
}

/* ============================================================
 * Insert child into parent internal node (with split if needed)
 * - We treat internal nodes by collecting children list, inserting, sorting, rebuilding.
 * - If overflow, split internal node and propagate upward.
 * ============================================================ */

static void insert_into_parent(Table *t, uint32_t left_page, uint32_t right_page);

static void internal_node_insert_child(Table *t, uint32_t parent_page, uint32_t new_child_page) {
    void *parent = pager_get_page(t->pager, parent_page);
    if (get_node_type(parent) != NODE_INTERNAL) die("parent not internal");

    // collect existing children
    uint32_t num_keys = *internal_node_num_keys(parent);
    uint32_t count = num_keys + 1;

    if (count > INTERNAL_NODE_MAX_CHILDREN) die("corrupt internal children count");

    uint32_t children[INTERNAL_NODE_MAX_CHILDREN + 1];

    for (uint32_t i = 0; i < num_keys; i++) children[i] = *internal_node_child(parent, i);
    children[num_keys] = *internal_node_right_child(parent);

    // append new child
    children[count] = new_child_page;
    count++;

    // sort by max key
    sort_children_by_maxkey(t, children, count);

    if (count <= INTERNAL_NODE_MAX_CHILDREN) {
        internal_node_rebuild(t, parent_page, children, count);
        return;
    }

    // split internal
    uint32_t new_internal_page = allocate_page(t);
    void *new_internal = pager_get_page(t->pager, new_internal_page);
    initialize_internal_node(new_internal);

    // keep parent/root flags and parent pointer will be set via rebuild
    bool parent_is_root = is_node_root(parent);
    uint32_t parent_parent = *node_parent(parent);

    // split children into two halves
    uint32_t left_count = count / 2;
    uint32_t right_count = count - left_count;

    uint32_t left_children[INTERNAL_NODE_MAX_CHILDREN];
    uint32_t right_children[INTERNAL_NODE_MAX_CHILDREN];

    for (uint32_t i = 0; i < left_count; i++) left_children[i] = children[i];
    for (uint32_t i = 0; i < right_count; i++) right_children[i] = children[left_count + i];

    // rebuild old parent with left half
    internal_node_rebuild(t, parent_page, left_children, left_count);

    // rebuild new_internal with right half
    internal_node_rebuild(t, new_internal_page, right_children, right_count);

    // restore root flag if it was root (rebuild preserves is_root on that node)
    if (parent_is_root) {
        // If root internal split, create new root above them
        create_new_root(t, new_internal_page);
        return;
    }

    // otherwise, propagate into parent of this internal node
    // (update key for left child in parent-of-parent, then insert new_internal as new child)
    if (parent_parent == 0) die("non-root internal without parent?");
    internal_node_update_key_for_child(t, parent_parent, parent_page);
    internal_node_insert_child(t, parent_parent, new_internal_page);
}

/* Insert right page into parent, handling root case */
static void insert_into_parent(Table *t, uint32_t left_page, uint32_t right_page) {
    void *left = pager_get_page(t->pager, left_page);

    if (is_node_root(left)) {
        create_new_root(t, right_page);
        return;
    }

    uint32_t parent_page = *node_parent(left);
    internal_node_update_key_for_child(t, parent_page, left_page);
    internal_node_insert_child(t, parent_page, right_page);
}

/* ============================================================
 * Leaf insert (no split)
 * ============================================================ */

static bool leaf_insert_no_split(Table *t, Cursor *c, int32_t key, const Row *row) {
    void *leaf = pager_get_page(t->pager, c->page_num);
    uint32_t n = *leaf_node_num_cells(leaf);

    if (n >= LEAF_NODE_MAX_CELLS) return false;

    if (c->cell_num < n) {
        for (uint32_t i = n; i > c->cell_num; i--) {
            memmove(leaf_node_cell(leaf, i), leaf_node_cell(leaf, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *leaf_node_num_cells(leaf) = n + 1;
    *leaf_node_key(leaf, c->cell_num) = (uint32_t)key;
    serialize_row(row, leaf_node_value(leaf, c->cell_num));
    return true;
}

/* ============================================================
 * Leaf split + insert
 * - Build temp arrays of (key,row), insert new, split into old and new leaf.
 * - Link leaves and propagate to parent.
 * ============================================================ */

static void leaf_split_and_insert(Table *t, Cursor *c, int32_t key, const Row *row) {
    uint32_t old_page = c->page_num;
    void *old_leaf = pager_get_page(t->pager, old_page);
    uint32_t old_n = *leaf_node_num_cells(old_leaf);

    // new leaf
    uint32_t new_page = allocate_page(t);
    void *new_leaf = pager_get_page(t->pager, new_page);
    initialize_leaf_node(new_leaf);

    // link new leaf into list
    *leaf_node_next_leaf(new_leaf) = *leaf_node_next_leaf(old_leaf);
    *leaf_node_next_leaf(old_leaf) = new_page;

    // temp arrays
    uint32_t total = old_n + 1;
    uint32_t keys[LEAF_NODE_MAX_CELLS + 1];
    Row rows[LEAF_NODE_MAX_CELLS + 1];

    for (uint32_t i = 0; i < old_n; i++) {
        keys[i] = *leaf_node_key(old_leaf, i);
        deserialize_row(leaf_node_value(old_leaf, i), &rows[i]);
    }

    // insert into temp at insertion point
    uint32_t ins = c->cell_num;
    if (ins > old_n) ins = old_n;

    for (uint32_t i = total - 1; i > ins; i--) {
        keys[i] = keys[i - 1];
        rows[i] = rows[i - 1];
    }
    keys[ins] = (uint32_t)key;
    rows[ins] = *row;

    // split counts
    uint32_t left_count = total / 2;
    uint32_t right_count = total - left_count;

    // rebuild old leaf (left)
    *leaf_node_num_cells(old_leaf) = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        *leaf_node_num_cells(old_leaf) = i + 1;
        *leaf_node_key(old_leaf, i) = keys[i];
        serialize_row(&rows[i], leaf_node_value(old_leaf, i));
    }

    // rebuild new leaf (right)
    *leaf_node_num_cells(new_leaf) = 0;
    for (uint32_t i = 0; i < right_count; i++) {
        *leaf_node_num_cells(new_leaf) = i + 1;
        *leaf_node_key(new_leaf, i) = keys[left_count + i];
        serialize_row(&rows[left_count + i], leaf_node_value(new_leaf, i));
    }

    // parent pointers
    *node_parent(new_leaf) = *node_parent(old_leaf);

    // propagate to parent (or create new root)
    insert_into_parent(t, old_page, new_page);
}

/* ============================================================
 * Public insert API
 * ============================================================ */

bool btree_insert(Table *t, const Row *row, char *errbuf, uint32_t errbuf_sz) {
    // find insertion point
    Cursor *c = btree_table_find(t, row->id);
    void *leaf = pager_get_page(t->pager, c->page_num);
    uint32_t n = *leaf_node_num_cells(leaf);

    // duplicate check
    if (c->cell_num < n) {
        int32_t existing = (int32_t)(*leaf_node_key(leaf, c->cell_num));
        if (existing == row->id) {
            if (errbuf && errbuf_sz) snprintf(errbuf, errbuf_sz, "duplicate key");
            btree_cursor_free(c);
            return false;
        }
    }

    if (leaf_insert_no_split(t, c, row->id, row)) {
        t->header.num_rows++;
        btree_cursor_free(c);
        return true;
    }

    // split
    leaf_split_and_insert(t, c, row->id, row);
    t->header.num_rows++;
    btree_cursor_free(c);
    return true;
}

bool btree_delete(Table *t, int32_t key, char *errbuf, uint32_t errbuf_sz) {
    Cursor *c = btree_table_find(t, key);
    void *leaf = pager_get_page(t->pager, c->page_num);
    uint32_t n = *leaf_node_num_cells(leaf);

    if (c->cell_num >= n ||
        (int32_t)(*leaf_node_key(leaf, c->cell_num)) != key) {
        if (errbuf && errbuf_sz)
            snprintf(errbuf, errbuf_sz, "key not found");
        btree_cursor_free(c);
        return false;
    }

    /* Shift cells left */
    for (uint32_t i = c->cell_num + 1; i < n; i++) {
        memmove(
            leaf_node_cell(leaf, i - 1),
            leaf_node_cell(leaf, i),
            LEAF_NODE_CELL_SIZE
        );
    }

    *leaf_node_num_cells(leaf) = n - 1;
    t->header.num_rows--;

    /*
     * IMPORTANT:
     * If leaf becomes underfull, we DO NOTHING in Commit 11.
     * This is intentional groundwork for Commit 12 (merge/redistribute).
     */
    uint32_t min_cells = is_node_root(leaf) ? 1 : LEAF_NODE_MIN_CELLS;

    if (*leaf_node_num_cells(leaf) < min_cells) {
        rebalance_leaf(t, c->page_num);
    }

    btree_cursor_free(c);
    return true;
}


/* ============================================================
 * New DB init
 * ============================================================ */

void btree_init_new_db(Table *t) {
    t->header.num_rows = 0;
    t->header.root_page_num = 1;
    t->header.next_free_page = 2;

    void *root = pager_get_page(t->pager, t->header.root_page_num);
    initialize_leaf_node(root);
    set_node_root(root, true);
}
