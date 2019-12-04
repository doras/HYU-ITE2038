#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include "file_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

// TYPES.

/** 
 * buffer_t structure
 * Represent buffer block structure
 *   which is compose buffer management layer.
 * Contains physical frame and more meta-data.
 * If table_id is negative value, the instance is invalid.
 * For replacement policy, this structure is managed by LRU clock.
 */
typedef struct _Buffer{
    page_t frame;
    int table_id;
    pagenum_t page_number;
    char is_dirty;
    char is_pinned;
    char ref_bit;
} buffer_t;


// GLOBALS.

/**
 * Initialized by init_db function.
 * A buffer array where all buffers are stored.
 */
extern buffer_t *g_buffer_pool;

/**
 * Store size of buffer pool.
 */
extern int g_buffer_size;

/**
 * Replacement policy uses LRU clock.
 * And this global variable is clock hand.
 */
extern int g_lru_clock_hand;


// FUNCTIONS.

int buf_init_db(int buf_num);
int buf_open_table(char *pathname);
int buf_close_table(int table_id);
buffer_t *buf_get_page(int table_id, pagenum_t page_num);
void buf_put_page(buffer_t *buf, char dirty);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
int buf_shutdown_db(void);

#ifdef __cplusplus
}
#endif

#endif
