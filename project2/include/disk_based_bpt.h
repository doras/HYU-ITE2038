#ifndef __DISK_BASED_BPT_H__
#define __DISK_BASED_BPT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "file_manager.h"

// CONSTANTS.

/* Order of leaf page.
 */
extern const int ORDER_OF_LEAF;

/* Order of internal page.
 */
extern const int ORDER_OF_INTERNAL;


// GLOBALS.

// For table id
extern char* g_path_names[5]; // TODO: implement detail of table id

// FUNCTIONS.

// Internal functions
// Reference to bpt.c

pagenum_t _find_leaf(pagenum_t root, int64_t key, int debug);
int _cut(int length);
int _get_left_index(pagenum_t parent, pagenum_t left);
pagenum_t _start_new_tree(int64_t key, char * value);
pagenum_t _insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right);
void _insert_into_node(pagenum_t node, int left_index, int64_t key, pagenum_t right);
pagenum_t _insert_into_node_after_split(pagenum_t root, pagenum_t old_node, int left_index, int64_t key, pagenum_t right);
pagenum_t _insert_into_parent(pagenum_t root, pagenum_t left, int64_t key, pagenum_t right);
void _insert_into_leaf(pagenum_t leaf, int64_t key, char *value);
pagenum_t _insert_into_leaf_after_split(pagenum_t root, pagenum_t leaf, int64_t key, char *value);


// Functions of own disk-based bpt

int open_table(char *pathname);
int db_insert(int64_t key, char * value);
int db_find(int64_t key, char * ret_val);
int db_delete(int64_t key);
int close_table(void);



#endif