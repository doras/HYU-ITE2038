#ifndef __DISK_BASED_BPT_H__
#define __DISK_BASED_BPT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "file_manager.h"

// TYPES.

typedef struct _queue_node {
    pagenum_t value;
    struct _queue_node *next;
} queue_node_t;

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

extern queue_node_t *g_queue;

// FUNCTIONS.

// Internal functions
// Reference to bpt.c

void _enqueue(pagenum_t val);
pagenum_t _dequeue();
void _print_tree();
void _print_keys();

pagenum_t _find_leaf(pagenum_t root, int64_t key, int debug);
int _cut(int length);
int _get_left_index(pagenum_t parent, pagenum_t left);
pagenum_t _start_new_tree(int64_t key, char * value);
pagenum_t _insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right);
void _insert_into_node(pagenum_t node, int left_index, int64_t key, pagenum_t right);
pagenum_t _insert_into_node_after_split(pagenum_t root, pagenum_t old_node, int left_index, 
        int64_t key, pagenum_t right);
pagenum_t _insert_into_parent(pagenum_t root, pagenum_t left, int64_t key, pagenum_t right);
void _insert_into_leaf(pagenum_t leaf, int64_t key, char *value);
pagenum_t _insert_into_leaf_after_split(pagenum_t root, pagenum_t leaf, int64_t key, char *value);

void _adjust_root(pagenum_t root);
int _get_neighbor_index(pagenum_t parent, pagenum_t node);
int _remove_entry_from_internal_node(pagenum_t node, int64_t key, pagenum_t pointer);
int _remove_record_from_leaf(pagenum_t leaf, int64_t key, char *value);
void _delayed_merge_nodes(pagenum_t root, pagenum_t node, pagenum_t parent
        , pagenum_t neighbor, int neighbor_index, int64_t k_prime);
void _delete_record(pagenum_t root, pagenum_t leaf, int64_t key, char *value);
void _redistribute_nodes(pagenum_t node, pagenum_t parent
        , pagenum_t neighbor, int neighbor_index, int64_t k_prime, int k_prime_index);
void _delete_internal_entry(pagenum_t root, pagenum_t node, int64_t key, pagenum_t pointer);

// Functions of own disk-based bpt

int open_table(char *pathname);
int db_insert(int64_t key, char * value);
int db_find(int64_t key, char * ret_val);
int db_delete(int64_t key);
int close_table(void);



#endif