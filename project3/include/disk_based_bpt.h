#ifndef __DISK_BASED_BPT_H__
#define __DISK_BASED_BPT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_manager.h"


// CONSTANTS.

/* Order of leaf page.
 */
extern const int ORDER_OF_LEAF;

/* Order of internal page.
 */
extern const int ORDER_OF_INTERNAL;


// FUNCTIONS.

int init_db(int num_buf);
int open_table(char *pathname);
int db_insert(int64_t key, char * value);
int db_find(int64_t key, char * ret_val);
int db_delete(int64_t key);
int close_table(int table_id);



#endif