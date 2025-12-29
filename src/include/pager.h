#ifndef PAGER_H
#define PAGER_H

#include <stdint.h>
#include <stdio.h>

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

typedef struct {
    FILE *file;
    uint32_t num_pages;
    void *pages[256];   // TABLE_MAX_PAGES fixed here for simplicity in commit-10
} Pager;

Pager *pager_open(const char *filename);
void  *pager_get_page(Pager *pager, uint32_t page_num);
void   pager_flush(Pager *pager, uint32_t page_num);
void   pager_close(Pager *pager);

#endif
