#ifndef __FILE_MANAGER_H__
#define __FILE_MANAGER_H__


#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>


// TYPES.

/* ID of pages.
 * Implemented as offset of that page from the beginning of file.
 */
typedef uint64_t pagenum_t;


/* Enum structure for in-memory page structure.
 * This enum value indicates what kind of pages that is.
 */
typedef enum {
    PAGE_HEADER,
    PAGE_FREE,
    PAGE_INTERNAL,
    PAGE_LEAF,
    PAGE_INVALID
} page_style_t;

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
typedef struct {

    // Equivalent area to on-disk page layout 
    
    union {
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
    };

    // Additional area only in in-memory structure

    page_style_t style;

} page_t;



// CONSTANTS.

/* Constant value
 * that represents size of on disk page.
 */
extern const uint64_t ON_DISK_PAGE_SIZE;

/* Real size of on-disk header page
 * except reserved area.
 */
extern const uint64_t HEADER_PAGE_SIZE;

/* Real size of on-disk free page
 * except reserved area.
 */
extern const uint64_t FREE_PAGE_SIZE;


// GLOBALS.


/* File descriptor.
 * File descriptor is non-negative integer value.
 * Negative value means invalid value.
 * When file opens, fd is set.
 * When file closes, fd is unset to negative value.
 */
extern int fd;


// FUNCTIONS.


void file_open_file(char *pathname);
off_t file_extend_file();
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);
int file_close_file(void);


#endif // __FILE_MANAGER_H__