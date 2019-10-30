#ifndef __FILE_MANAGER_H__
#define __FILE_MANAGER_H__


#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>


#define MAX_TABLE_ID 10


// TYPES.

/* ID of pages.
 * Implemented as offset of that page from the beginning of file.
 */
typedef uint64_t pagenum_t;


/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */
typedef struct record {
    int64_t key;
    char value[120];
} record;

/* Structure representing record key - page number pair.
 * For internal page's body.
 */
typedef struct {
    int64_t key;
    pagenum_t pagenum;
} key_pagenum_pair;

/* In-memory page structure.
 * Generic structure that can represent all kinds of pages
 * such as Header, Free, Internal or Leaf page.
 * Because of generalness, this structure needs typecasting in many cases.
 */
typedef union {
    struct {
        pagenum_t free_pagenum;
        pagenum_t root_pagenum;
        pagenum_t num_of_pages;
    } header_page;

    struct {
        pagenum_t next_free_pagenum;
    } free_page;

    struct {
        pagenum_t parent_pagenum;
        int is_leaf;
        int num_of_keys;
        char _reserved[104];
        pagenum_t first_pagenum;
        key_pagenum_pair entries[248];
    } internal_page;

    struct {
        pagenum_t parent_pagenum;
        int is_leaf;
        int num_of_keys;
        char _reserved[104];
        pagenum_t right_sibling_pagenum;
        record records[31];
    } leaf_page;
} page_t;



// CONSTANTS.

/* Constant value
 * that represents size of on disk page.
 */
extern const uint64_t ON_DISK_PAGE_SIZE;


// GLOBALS.


/** 
 * File descriptors.
 * A valid file descriptor is non-negative integer value.
 * Negative value means invalid value.
 * Use table id (1 ~ MAX_TABLE_ID) for index.
 * First element of array is invalid negative valude.
 */
extern int fd[MAX_TABLE_ID + 1];

/** 
 * Stored path names.
 * Used for table id checking.
 * Use table id (1 ~ MAX_TABLE_ID) for index.
 * First element of array is invalid pathname, NULL.
 */
extern char *stored_pathname[MAX_TABLE_ID + 1];


// FUNCTIONS.


int file_open_file(char *pathname);
off_t file_extend_file(int table_id, page_t *header_page);
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);
int file_close_file(int table_id);

#endif // __FILE_MANAGER_H__