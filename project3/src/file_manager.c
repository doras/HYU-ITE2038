#include "file_manager.h"


// CONSTANTS.

/* Constant value
 * that represents size of on disk page.
 */
const uint64_t ON_DISK_PAGE_SIZE = 4096;

/* Real size of on-disk header page
 * except reserved area.
 */
const uint64_t HEADER_PAGE_SIZE = 24;

/* Real size of on-disk free page
 * except reserved area.
 */
const uint64_t FREE_PAGE_SIZE = 8;


// GLOBALS.

int fd = -1;


// FUNCTION DEFINITIONS.

/* Open or Create a file(table) corresponding to pathname.
 * Set global variable fd to file descriptor of opened file.
 * If the file does not exist, create the file and header page.
 */
void file_open_file(char *pathname) {
    page_t header;

    fd = open(pathname, O_RDWR);
    if (fd >= 0) return; // Just open file.

    // Create file.
    fd = open(pathname, O_RDWR | O_CREAT, S_IRWXG | S_IRWXU | S_IRWXO);
    header.header_page.free_pagenum = 0;
    header.header_page.root_pagenum = 0;
    header.header_page.num_of_pages = 1;
    pwrite(fd, &header, ON_DISK_PAGE_SIZE, 0);
    fsync(fd);
}

/* Extend currently opened file for one page.
 * Additional length is on-disk page size.
 * Additional space is not initiate. But, last byte is 0x00.
 * Increase number of pages in header page.
 * Returns last offset of the extended file if success.
 * If fail, return -1.
 */
off_t file_extend_file() {
    off_t result;
    pagenum_t num_of_pages;
    char buf = 0;

    result = lseek(fd, ON_DISK_PAGE_SIZE - 1, SEEK_END);

    if (write(fd, &buf, 1) < 1) {
        perror("Fail to extend file");
        return -1;
    }
    fsync(fd);

    num_of_pages = (result + 1) / ON_DISK_PAGE_SIZE;
    pwrite(fd, &num_of_pages, 8, 16);
    fsync(fd);

    return result;
}

/* Allocate an on-disk page from the free page list.
 * Allocated page is popped from the front of free page list.
 * If free page list is empty, increase file size and make new free page.
 * Return allocated page number. 
 * This function may return 0 when fail to allocate.
 */
pagenum_t file_alloc_page() {
    page_t header_page;
    page_t free_page;
    pagenum_t result;
    
    if (fd < 0) {
        return 0;
    }

    header_page.style = PAGE_HEADER;
    file_read_page(0, &header_page);

    result = header_page.header_page.free_pagenum;

    // Special case : There is no free page. So, extend file.
    if (result == 0) {
        return file_extend_file() / ON_DISK_PAGE_SIZE;
    }

    // Normal case : Pop free page from free page list.

    free_page.style = PAGE_FREE;
    file_read_page(result, &free_page);

    // renew header page information
    header_page.header_page.free_pagenum = free_page.free_page.next_free_pagenum;
    file_write_page(0, &header_page);

    return result;
}

/* Free an on-disk page to the free page list.
 * Freed page is inserted to front of free page list.
 */
void file_free_page(pagenum_t pagenum) {
    page_t tmp_page; // used for header page and freed page
    pagenum_t next_free_page;

    tmp_page.style = PAGE_HEADER;
    file_read_page(0, &tmp_page);

    // renew header page info
    next_free_page = tmp_page.header_page.free_pagenum;
    tmp_page.header_page.free_pagenum = pagenum;
    file_write_page(0, &tmp_page);

    // renew freed page info
    tmp_page.style = PAGE_FREE;
    tmp_page.free_page.next_free_pagenum = next_free_page;
    file_write_page(pagenum, &tmp_page);
}

/* Read an on-disk page into the in-memory page structure(dest)
 * In given dest, style member variable must be set to correct value.
 */
void file_read_page(pagenum_t pagenum, page_t* dest) {
    uint64_t read_size[5] = {HEADER_PAGE_SIZE,
                             FREE_PAGE_SIZE,
                             ON_DISK_PAGE_SIZE,
                             ON_DISK_PAGE_SIZE,
                             ON_DISK_PAGE_SIZE};

    if (pread(fd, dest, read_size[dest->style], pagenum * ON_DISK_PAGE_SIZE) < read_size[dest->style]) {
        perror("Fail to read page");
    }
}

/* Write an in-memory page(src) to the on-disk page
 * In given src, style member variable must be set to correct value.
 */
void file_write_page(pagenum_t pagenum, const page_t* src) {
    uint64_t write_size[5] = {HEADER_PAGE_SIZE,
                              FREE_PAGE_SIZE,
                              ON_DISK_PAGE_SIZE,
                              ON_DISK_PAGE_SIZE,
                              ON_DISK_PAGE_SIZE};

    pwrite(fd, src, write_size[src->style], pagenum * ON_DISK_PAGE_SIZE);
    fsync(fd);
}

/* Close a file(table) that be currently opened
 * Reset global variable fd to -1.
 * Return zero if success, otherwise -1.
 */
int file_close_file(void) {
    int result = close(fd);

    if(result == 0) {
        fd = -1;
    }
    return result;
}
