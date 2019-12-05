#ifndef __DISK_BASED_BPT_H__
#define __DISK_BASED_BPT_H__

#include <algorithm>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_manager.hpp"
#include "lock_manager.hpp"

#define OPERATION_SUCCESS 0
#define OPERATION_ABORTED -1
#define OPERATION_NOTFOUND 1


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
int db_insert(int table_id, int64_t key, char *value);
int db_find(int table_id, int64_t key, char *ret_val, int trx_id);
int db_update(int table_id, int64_t key, char *values, int trx_id);
int db_delete(int table_id, int64_t key);
int close_table(int table_id);
int shutdown_db(void);
int join_table(int table_id_1, int table_id_2, char * pathname);

#endif
