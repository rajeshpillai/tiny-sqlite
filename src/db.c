#include "db.h"
#include <stdlib.h>
#include <string.h>

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

Table *db_open(const char *filename) {
    Pager *p = pager_open(filename);
    Table *t = calloc(1, sizeof(Table));
    if (!t) die("calloc");

    t->pager = p;

    if (p->num_pages == 0) {
        btree_init_new_db(t);
    } else {
        // read header from page 0
        void *page0 = pager_get_page(p, 0);
        memcpy(&t->header, page0, sizeof(DBHeader));

        // basic sanity
        if (t->header.root_page_num == 0 || t->header.root_page_num >= TABLE_MAX_PAGES) {
            die("invalid header/root; delete db");
        }
        if (t->header.next_free_page == 0 || t->header.next_free_page >= TABLE_MAX_PAGES) {
            die("invalid next_free_page; delete db");
        }
    }

    return t;
}

void db_close(Table *t) {
    // write header to page 0
    void *page0 = pager_get_page(t->pager, 0);
    memcpy(page0, &t->header, sizeof(DBHeader));

    pager_close(t->pager);
    free(t);
}
