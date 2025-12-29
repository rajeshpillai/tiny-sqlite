#include "pager.h"
#include <stdlib.h>
#include <string.h>

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static long page_offset(uint32_t page_num) {
    // safe promotion before multiply
    return (long)page_num * (long)PAGE_SIZE;
}

Pager *pager_open(const char *filename) {
    FILE *f = fopen(filename, "r+b");
    if (!f) f = fopen(filename, "w+b");
    if (!f) die("fopen");

    if (fseek(f, 0, SEEK_END) != 0) die("fseek");
    long size = ftell(f);
    if (size < 0) die("ftell");

    if (size % PAGE_SIZE != 0 && size != 0) {
        die("corrupt db (partial page)");
    }

    Pager *p = calloc(1, sizeof(Pager));
    if (!p) die("calloc");

    p->file = f;
    p->num_pages = (uint32_t)(size / PAGE_SIZE);

    return p;
}

void *pager_get_page(Pager *pager, uint32_t page_num) {
    if (page_num >= 256) die("page out of bounds");

    if (pager->pages[page_num] == NULL) {
        void *page = calloc(1, PAGE_SIZE);
        if (!page) die("calloc");

        if (page_num < pager->num_pages) {
            if (fseek(pager->file, page_offset(page_num), SEEK_SET) != 0) die("fseek");
            size_t nread = fread(page, PAGE_SIZE, 1, pager->file);
            if (nread != 1 && !feof(pager->file)) die("fread");
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

void pager_flush(Pager *pager, uint32_t page_num) {
    if (!pager->pages[page_num]) return;

    if (fseek(pager->file, page_offset(page_num), SEEK_SET) != 0) die("fseek");
    if (fwrite(pager->pages[page_num], PAGE_SIZE, 1, pager->file) != 1) die("fwrite");
}

void pager_close(Pager *pager) {
    if (!pager) return;
    for (uint32_t i = 0; i < 256; i++) {
        if (pager->pages[i]) {
            pager_flush(pager, i);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }
    fclose(pager->file);
    free(pager);
}
