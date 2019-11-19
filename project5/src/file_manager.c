#include "file_manager.h"

// CONSTANTS.

/**
 * Constant value
 * that represents size of on disk page.
 */
const uint64_t ON_DISK_PAGE_SIZE = 4096;


// GLOBALS.

/** 
 * File descriptors.
 * A valid file descriptor is non-negative integer value.
 * Use table id (1 ~ MAX_TABLE_ID) for index.
 * First element of array is invalid negative value.
 */
int fd[MAX_TABLE_ID + 1] = { -1 };

/** 
 * Stored path names.
 * Used for table id checking.
 * NULL means invalid table_id.
 * Use table id (1 ~ 10) for index.
 * First element of array is invalid pathname, NULL.
 */
char *stored_pathname[MAX_TABLE_ID + 1] = {};


// FUNCTION DEFINITIONS.

/**
 * Open or Create a file(table) corresponding to pathname.
 * If open this table for the first time,
 *   set empty space of global array fd to file descriptor of opened file.
 * Then store pathname in same index of global array stored_pathname.
 * The index of empty space is allocate to this table for table id.
 * Otherwise, just return unique table id.
 * \param pathname Path name for a file to be opened or created.
 * \return If success, return unique table id of corresponding file to \p pathname .
 *      Otherwise, return negative value.
 */
int file_open_file(char *pathname) {
    page_t header;
    char *new_pathname;
    int table_id, empty_id = 0;
    int new_fd;
    size_t len;

    for (table_id = MAX_TABLE_ID; table_id >= 1; --table_id) {
        if (stored_pathname[table_id]) {
            if (strcmp(stored_pathname[table_id], pathname) == 0) {
                return table_id;
            }
        } else {
            empty_id = table_id;
        }
    }

    if (empty_id == 0) {
        return -1;
    }


    new_fd = open(pathname, O_RDWR);
    // Create file.
    if (new_fd < 0) {
        new_fd = open(pathname, O_RDWR | O_CREAT, S_IRWXG | S_IRWXU | S_IRWXO);
        header.header_page.free_pagenum = 0;
        header.header_page.root_pagenum = 0;
        header.header_page.num_of_pages = 1;
        fd[empty_id] = new_fd;
        file_write_page(empty_id, 0, &header);
    }

    // Allocate and Copy pathname
    len = strlen(pathname);
    new_pathname = malloc(sizeof(char) * (len + 1));
    strncpy(new_pathname, pathname, len);
    new_pathname[len] = '\0';

    // Use new table_id, empty_id
    // Store fd and pathname in global arrays.
    fd[empty_id] = new_fd;
    stored_pathname[empty_id] = new_pathname;


    return empty_id;
}

/**
 * Extend given corresponding table(file) to \p table_id for one page.
 * Additional length is on-disk page size.
 * Additional space is not initialized.
 * Increase number of pages in header page.
 * \param table_id Table id of extension target table(file)
 * \param header_page If this argument is NULL, just directly write header page.
 *      Otherwise, the case that the buffer manager buffers header page.
 *      Not change the header page of disk, but change given in-memory header page.
 * \return Last offset of the extended file if success.
 *      If fail, return -1.
 */
off_t file_extend_file(int table_id, page_t *header_page) {
    off_t result;
    pagenum_t num_of_pages;
    char buf = 0;

    // Extend the file.

    result = lseek(fd[table_id], ON_DISK_PAGE_SIZE - 1, SEEK_END);

    if (write(fd[table_id], &buf, 1) < 1) {
        perror("Fail to extend file");
        return -1;
    }
    fsync(fd[table_id]);

    // Update header page.

    num_of_pages = (result + 1) / ON_DISK_PAGE_SIZE;

    if (header_page == NULL) {
        pwrite(fd[table_id], &num_of_pages, 8, 16);
        fsync(fd[table_id]);
    } else {
        header_page->header_page.num_of_pages = num_of_pages;
    }

    return result;
}

/**
 * Read an on-disk page into the in-memory page structure(dest)
 * \param table_id Indicating the table 
 *      where reading operation is performed.
 * \param pagenum Indicating the page which is target of reading operation.
 * \param dest Result of reading operation. The page is stored in here.
 */
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest) {
    pread(fd[table_id], dest, ON_DISK_PAGE_SIZE, pagenum * ON_DISK_PAGE_SIZE);
}

/**
 * Write an in-memory page(src) to the on-disk page
 * \param table_id Indicating the table 
 *      where writing operation is performed.
 * \param pagenum Indicating the page which is target of writing operation.
 * \param src Source structure of writing operation.
 */
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src) {
    pwrite(fd[table_id], src, ON_DISK_PAGE_SIZE, pagenum * ON_DISK_PAGE_SIZE);
    fsync(fd[table_id]);
}

/** 
 * Discard the table id.
 * \param table_id Indicating target table to be closed.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int file_close_file(int table_id) {
    if (close(fd[table_id]) != 0) {
        return -1;
    }
    fd[table_id] = -1;
    free(stored_pathname[table_id]);
    stored_pathname[table_id] = NULL;

    return 0;
}
