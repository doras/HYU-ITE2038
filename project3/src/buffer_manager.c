/*
 * buffer_manager.c
 */

#include "buffer_manager.h"


// GLOBALS.

/**
 * Initialized by init_db function.
 * A buffer array where all buffers are stored.
 */
buffer_t *g_buffer_pool = NULL;

/**
 * Store size of buffer pool.
 */
int g_buffer_size = 0;

/**
 * Replacement policy uses LRU clock.
 * And this global variable is clock hand.
 */
int g_lru_clock_hand = 0;


// FUNCTIONS.


/**
 * A buffer initializing function.
 * Allocate the buffer pool (array) 
 *   with the given number of entries by buf_num.
 * Initialize other fields such as state info, LRU info, etc.
 * And store size of buffer pool in global variable.
 * \param buf_num Number of entries in the buffer pool.
 *      Allocate with this number of buffers.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int buf_init_db(int buf_num) {
    int i;
    // invalid condition
    if (buf_num < 1 || g_buffer_pool != NULL) {
        return 1;
    }

    g_buffer_pool = malloc(sizeof(buffer_t) * buf_num);
    
    // Fail to allocate.
    if (g_buffer_pool == NULL) {
        return 1;
    }

    g_buffer_size = buf_num;

    for (i = 0; i < buf_num; ++i) {
        g_buffer_pool[i].table_id = -1; // means that object is invalid.
    }
    
    return 0;
}

/**
 * Open or Create a file(table) corresponding to pathname.
 * If open this table for the first time,
 *   set empty space of global array fd to file descriptor of opened file.
 * Then store pathname in same index of global array stored_pathname.
 * The index of empty space is allocate to this table for table id.
 * Otherwise, just return unique table id.
 * \param pathname path name for a file to be opened or created.
 * \return If success, return unique table id of corresponding file to \p pathname .
 *      Otherwise, return negative value.
 */
int buf_open_table(char *pathname) {
    return file_open_file(pathname);
}

/** 
 * Write all pages of this table from buffer to disk
 *      and discard the table id.
 * \param table_id Indicating target table to be closed.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int buf_close_table(int table_id) {
    int i;
    for (i = 0; i < g_buffer_size; ++i) {
        if (g_buffer_pool[i].table_id == table_id) {
            // Wait until the buffer is unpin
            while (g_buffer_pool[i].is_pinned) continue;
            if (g_buffer_pool[i].is_dirty) {
                file_write_page(table_id, g_buffer_pool[i].page_number, &g_buffer_pool[i].frame);
            }
            // Empty the buffer structure.
            g_buffer_pool[i].table_id = -1;
        }
    }
    return file_close_file(table_id);
}

/**
 * Get particular page from buffer pool.
 * If buffer miss, perform replacement 
 *      and read from disk to that page.
 * \param table_id Indicate the table where the page is.
 * \param page_num Page number of the page to be returned.
 * \return Returns a pointer to the buffer structure designated by arguments.
 */
buffer_t *buf_get_page(int table_id, pagenum_t page_num) {
    int i;
    char done = 0;
    buffer_t *curr_buf;
    // Find the page from buffer pool
    for (i = 0; i < g_buffer_size; ++i) {
        if (g_buffer_pool[i].table_id == table_id 
                && g_buffer_pool[i].page_number == page_num) {

            while (g_buffer_pool[i].is_pinned) continue; // If the page is pinned, wait for unpin

            ++g_buffer_pool[i].is_pinned;
            g_buffer_pool[i].ref_bit = 1;
            return g_buffer_pool + i;
        }
    }

    /* Case: Buffer caching miss.
     * There is no such page in pool.
     */

    // check buffer pool
    for (i = 0; i < g_buffer_size && g_buffer_pool[i].table_id > 0; ++i) {
        continue;
    }


    // Case: pool is not full.
    if (i < g_buffer_size) {
        // Read page into the empty space of buffer pool.
        // And return it.
        file_read_page(table_id, page_num, &g_buffer_pool[i].frame);

        g_buffer_pool[i].table_id = table_id;
        g_buffer_pool[i].page_number = page_num;
        g_buffer_pool[i].is_dirty = 0;
        g_buffer_pool[i].is_pinned = 1;
        g_buffer_pool[i].ref_bit = 1;

        return g_buffer_pool + i;
    }


    /* Perform replacement */

    while (!done) {
        curr_buf = &g_buffer_pool[g_lru_clock_hand];

        // Happy case : found victim. evict this page.
        if (!curr_buf->is_pinned && !curr_buf->ref_bit) {
            ++curr_buf->is_pinned;
            if (curr_buf->is_dirty) {
                file_write_page(curr_buf->table_id, curr_buf->page_number, &curr_buf->frame);
            }
            file_read_page(table_id, page_num, &curr_buf->frame);
            curr_buf->table_id = table_id;
            curr_buf->page_number = page_num;
            curr_buf->is_dirty = 0;
            curr_buf->ref_bit = 1;

            done = 1;
        }
        // Current buffer is referenced in this cycle.
        else if (!curr_buf->is_pinned && curr_buf->ref_bit) {
            curr_buf->ref_bit = 0;
        }
        // else : current buffer is in use. -> Do nothing.

        g_lru_clock_hand = (g_lru_clock_hand + 1) % g_buffer_size;
    }

    return curr_buf;
}

/**
 * Put the buffer page down to buffer pool.
 * If the frame has some changes, notify that to this function
 *      with argument \p dirty .
 * \param buf Pointer of the buffer structure to be put
 * \param dirty If the frame is NOT changed, will be 0.
 *      Otherwise, non-zero value.
 * \return Returns nothing.
 */
void buf_put_page(buffer_t *buf, char dirty) {
    
    // (buf->is_dirty | dirty) means this is clean
    // only when previous clean and clean in this turn too.
    buf->is_dirty |= dirty;
    --buf->is_pinned;
}

/**
 * Allocate a page from the free page list.
 * Allocated page is popped from the front of free page list.
 * If free page list is empty, increase file size and make new free page.
 * \param table_id Indicating the table from which allocate a page.
 * \return Return allocated page number,
 *      and may return 0 when fail to allocate.
 */
pagenum_t buf_alloc_page(int table_id) {
    buffer_t *header_page, *free_page;
    pagenum_t result;

    header_page = buf_get_page(table_id, 0);

    result = header_page->frame.header_page.free_pagenum;
    
    // Special case : There is no free page in file. So, extend file.
    if (result == 0) {
        result = file_extend_file(table_id, &header_page->frame) / ON_DISK_PAGE_SIZE;
    }
    // Normal case : allocate a page from free page list.
    else {
        free_page = buf_get_page(table_id, result);
        header_page->frame.header_page.free_pagenum = free_page->frame.free_page.next_free_pagenum;
        buf_put_page(free_page, 0);
    }

    buf_put_page(header_page, 1);

    return result;
}

/**
 * Free an on-disk page to the free page list.
 * Freed page is inserted to front of free page list.
 * \param table_id Indicate the table in which the page to be freed is.
 * \param pagenum Indicate the page to be freed.
 */
void buf_free_page(int table_id, pagenum_t pagenum) {
    buffer_t *header, *freeing_page;

    header = buf_get_page(table_id, 0);
    freeing_page = buf_get_page(table_id, pagenum);

    freeing_page->frame.free_page.next_free_pagenum = header->frame.header_page.free_pagenum;
    header->frame.header_page.free_pagenum = pagenum;

    buf_put_page(header, 1);
    buf_put_page(freeing_page, 1);
}

/**
 * Flush all data from buffer and destroy allocated buffer.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int buf_shutdown_db(void) {
    int i;
    
    for (i = 0; i < g_buffer_size; ++i) {
        if (g_buffer_pool[i].table_id > 0) {
            while (g_buffer_pool[i].is_pinned) continue;
            if (g_buffer_pool[i].is_dirty) {
                file_write_page(g_buffer_pool[i].table_id, g_buffer_pool[i].page_number, &g_buffer_pool[i].frame);
            }
        }
    }

    g_buffer_size = 0;
    free(g_buffer_pool);

    return 0;
}
