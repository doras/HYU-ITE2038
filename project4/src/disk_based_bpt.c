/*
 * disk_based_bpt.c
 */

/*
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, 
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice, 
 *  this list of conditions and the following disclaimer in the documentation 
 *  and/or other materials provided with the distribution.
 
 *  3. Neither the name of the copyright holder nor the names of its 
 *  contributors may be used to endorse or promote products derived from this 
 *  software without specific prior written permission.
 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *  POSSIBILITY OF SUCH DAMAGE.
 */


#include "disk_based_bpt.h"


// CONSTANTS.

/**
 * Order of leaf page.
 */
const int ORDER_OF_LEAF = 32;

/**
 * Order of internal page.
 */
const int ORDER_OF_INTERNAL = 249;


// FUNCTIONS.

// Declaration.

static pagenum_t _find_leaf(int table_id, pagenum_t root, int64_t key);
static int _cut(int length);
static int _get_left_index(int table_id, pagenum_t parent, pagenum_t left);
static pagenum_t _start_new_tree(int table_id, int64_t key, char * value);
static pagenum_t _insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right);
static void _insert_into_node(int table_id, pagenum_t node, int left_index, int64_t key, pagenum_t right);
static pagenum_t _insert_into_node_after_split(int table_id, pagenum_t root, pagenum_t old_node, int left_index, 
        int64_t key, pagenum_t right);
static pagenum_t _insert_into_parent(int table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right);
static void _insert_into_leaf(int table_id, pagenum_t leaf, int64_t key, char *value);
static pagenum_t _insert_into_leaf_after_split(int table_id, pagenum_t root, pagenum_t leaf, int64_t key, char *value);

static void _adjust_root(int table_id, pagenum_t root);
static int _get_neighbor_index(int table_id, pagenum_t parent, pagenum_t node);
static int _remove_entry_from_internal_node(int table_id, pagenum_t node, int64_t key, pagenum_t pointer);
static int _remove_record_from_leaf(int table_id, pagenum_t leaf, int64_t key, char *value);
static void _delayed_merge_nodes(int table_id, pagenum_t root, pagenum_t node, pagenum_t parent
        , pagenum_t neighbor, int neighbor_index, int64_t k_prime);
static void _delete_record(int table_id, pagenum_t root, pagenum_t leaf, int64_t key, char *value);
static void _redistribute_nodes(int table_id, pagenum_t node, pagenum_t parent
        , pagenum_t neighbor, int neighbor_index, int64_t k_prime, int k_prime_index);
static void _delete_internal_entry(int table_id, pagenum_t root, pagenum_t node, int64_t key, pagenum_t pointer);


// Internal functions
// Reference to bpt.c

// find.

/**
 * Traces the path from the root to a leaf, searching
 * by key.
 * \return Returns the page number of leaf containing the given key.
 *      If tree is empty, returns 0.
 */
static pagenum_t _find_leaf(int table_id, pagenum_t root, int64_t key) {
    int i = 0;
    buffer_t *c;

    // if tree is empty.
    if (root == 0) {
        return root;
    }
    
    // Read root page to c

    c = buf_get_page(table_id, root);


    while (!c->frame.internal_page.is_leaf) {
        i = 0;
        while (i < c->frame.internal_page.num_of_keys) {
            if (key >= c->frame.internal_page.entries[i].key) i++;
            else break;
        }
        root = *(&c->frame.internal_page.first_pagenum + 2 * i);
        buf_put_page(c, 0);
        c = buf_get_page(table_id, root);
    }
    buf_put_page(c, 0);

    return root;
}

// insertion.

/**
 * Finds the appropriate place to
 * split a node that is too big into two.
 */
static int _cut(int length) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

/**
 * Helper function used in _insert_into_parent
int table_id,  * to find the index of the parent's child pagenum to 
 * the left that has the key to be inserted.
 */
static int _get_left_index(int table_id, pagenum_t parent, pagenum_t left) {

    int left_index = 0;
    buffer_t *parent_page;
    parent_page = buf_get_page(table_id, parent);
    while (left_index <= parent_page->frame.internal_page.num_of_keys && 
            *(&(parent_page->frame.internal_page.first_pagenum) + 2 * left_index) != left)
        left_index++;
    buf_put_page(parent_page, 0);
    return left_index;
}

/**
 * Start a new tree with given record.
 * This function will be called at first insertion.
 */
static pagenum_t _start_new_tree(int table_id, int64_t key, char * value) {
    buffer_t *root_page;
    pagenum_t root = buf_alloc_page(table_id);

    root_page = buf_get_page(table_id, root);

    root_page->frame.leaf_page.is_leaf = 1;
    root_page->frame.leaf_page.num_of_keys = 1;
    root_page->frame.leaf_page.parent_pagenum = 0;
    root_page->frame.leaf_page.records[0].key = key;
    strncpy(root_page->frame.leaf_page.records[0].value, value, 119);
    root_page->frame.leaf_page.records[0].value[119] = '\0';
    root_page->frame.leaf_page.right_sibling_pagenum = 0;
    
    buf_put_page(root_page, 1);

    return root;
}

/**
 * Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 * Returns new root page number.
 */
static pagenum_t _insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right) {

    pagenum_t root = buf_alloc_page(table_id);
    buffer_t *root_page = buf_get_page(table_id, root);

    root_page->frame.internal_page.entries[0].key = key;
    root_page->frame.internal_page.first_pagenum = left;
    root_page->frame.internal_page.entries[0].pagenum = right;
    root_page->frame.internal_page.num_of_keys = 1;
    root_page->frame.internal_page.parent_pagenum = 0;
    root_page->frame.internal_page.is_leaf = 0;
    
    buf_put_page(root_page, 1);

    root_page = buf_get_page(table_id, left);
    root_page->frame.internal_page.parent_pagenum = root;
    buf_put_page(root_page, 1);

    root_page = buf_get_page(table_id, right);
    root_page->frame.internal_page.parent_pagenum = root;
    buf_put_page(root_page, 1);

    return root;
}

/**
 * Inserts a new key and child node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
static void _insert_into_node(int table_id, pagenum_t node, int left_index, int64_t key, pagenum_t right) {

    int i;
    buffer_t *node_page;

    node_page = buf_get_page(table_id, node);

    for (i = node_page->frame.internal_page.num_of_keys; i > left_index; i--) {

        *(&node_page->frame.internal_page.first_pagenum + 2 * (i + 1)) = *(&node_page->frame.internal_page.first_pagenum + 2 * i);
        node_page->frame.internal_page.entries[i].key = node_page->frame.internal_page.entries[i - 1].key;
    }
    node_page->frame.internal_page.entries[left_index].pagenum = right;
    node_page->frame.internal_page.entries[left_index].key = key;
    ++node_page->frame.internal_page.num_of_keys;
    buf_put_page(node_page, 1);
}

/**
 * Inserts a new key and child node page number
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 * Returns the root page number of the tree after insertion.
 */
static pagenum_t _insert_into_node_after_split(int table_id, pagenum_t root, pagenum_t old_node, int left_index, 
        int64_t key, pagenum_t right) {

    buffer_t *old_node_page, *new_node_page, *child_page;
    pagenum_t child, new_node = buf_alloc_page(table_id);
    int64_t *temp_keys;
    int64_t k_prime;
    pagenum_t *temp_pagenums;
    int i, j, split;

    /* First create a temporary set of keys and pagenums
     * to hold everything in order, including
     * the new key and pagenum, inserted in their
     * correct places. 
     * Then create a new node and copy half of the 
     * keys and pagenums to the old node and
     * the other half to the new.
     */

    temp_pagenums = malloc((ORDER_OF_INTERNAL + 1) * sizeof(pagenum_t));
    if (temp_pagenums == NULL) {
        perror("Temporary pagenums array for splitting nodes.");
        exit(1);
    }
    temp_keys = malloc(ORDER_OF_INTERNAL * sizeof(int64_t));
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(1);
    }

    old_node_page = buf_get_page(table_id, old_node);

    for (i = 0, j = 0; i < old_node_page->frame.internal_page.num_of_keys + 1; ++i, ++j) {
        if (j == left_index + 1) ++j;
        temp_pagenums[j] = *(&old_node_page->frame.internal_page.first_pagenum + 2 * i);
    }
    
    for (i = 0, j = 0; i < old_node_page->frame.internal_page.num_of_keys; ++i, ++j) {
        if (j == left_index) ++j;
        temp_keys[j] = old_node_page->frame.internal_page.entries[i].key;
    }

    temp_pagenums[left_index + 1] = right;
    temp_keys[left_index] = key;

    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */  
    new_node_page = buf_get_page(table_id, new_node);

    split = _cut(ORDER_OF_INTERNAL);
    new_node_page->frame.internal_page.is_leaf = 0;
    for (i = 0; i < split - 1; ++i) {
        *(&old_node_page->frame.internal_page.first_pagenum + 2 * i) = temp_pagenums[i];
        old_node_page->frame.internal_page.entries[i].key = temp_keys[i];
    }
    old_node_page->frame.internal_page.num_of_keys = i;
    old_node_page->frame.internal_page.entries[i - 1].pagenum = temp_pagenums[i];
    k_prime = temp_keys[split - 1];
    for (++i, j = 0; i < ORDER_OF_INTERNAL; ++i, ++j) {
        *(&new_node_page->frame.internal_page.first_pagenum + 2 * j) = temp_pagenums[i];
        new_node_page->frame.internal_page.entries[j].key = temp_keys[i];
    }
    new_node_page->frame.internal_page.num_of_keys = j;
    new_node_page->frame.internal_page.entries[j - 1].pagenum = temp_pagenums[i];
    free(temp_pagenums);
    free(temp_keys);

    new_node_page->frame.internal_page.parent_pagenum = old_node_page->frame.internal_page.parent_pagenum;

    for (i = 0; i <= new_node_page->frame.internal_page.num_of_keys; ++i) {
        child = *(&new_node_page->frame.internal_page.first_pagenum + 2 * i);
        child_page = buf_get_page(table_id, child);
        child_page->frame.internal_page.parent_pagenum = new_node;
        buf_put_page(child_page, 1);
    }

    buf_put_page(old_node_page, 1);
    buf_put_page(new_node_page, 1);

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */

    return _insert_into_parent(table_id, root, old_node, k_prime, new_node);
}

/**
 * Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root page number of the tree after insertion.
 */
static pagenum_t _insert_into_parent(int table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {
    
    int left_index;
    pagenum_t parent;
    buffer_t *page;

    page = buf_get_page(table_id, left);

    parent = page->frame.internal_page.parent_pagenum;

    /* Case: parent is new root. */
    if (parent == 0) {
        buf_put_page(page, 0);
        return _insert_into_new_root(table_id, left, key, right);
    }
    /* Otherwise, parent is leaf or internal. */

    /* Find the parent's child pagenum to the left node */
    left_index = _get_left_index(table_id, parent, left);

    buf_put_page(page, 0);

    page = buf_get_page(table_id, parent);

    /* Simple case: the new key fits into the node. */
    if (page->frame.internal_page.num_of_keys < ORDER_OF_INTERNAL - 1) {
        buf_put_page(page, 0);
        _insert_into_node(table_id, parent, left_index, key, right);
        return root;
    }

    /* Harder case: split a node in order
     * to preserve the B+ tree properties.
     */
    buf_put_page(page, 0);
    return _insert_into_node_after_split(table_id, root, parent, left_index, key, right);
}

/**
 * Inserts a new record into a leaf.
 */
static void _insert_into_leaf(int table_id, pagenum_t leaf, int64_t key, char *value) {

    int i, insertion_point;
    buffer_t *leaf_page;

    leaf_page = buf_get_page(table_id, leaf);
    insertion_point = 0;
    while (insertion_point < leaf_page->frame.leaf_page.num_of_keys && leaf_page->frame.leaf_page.records[insertion_point].key < key) {
        ++insertion_point;
    }

    for(i = leaf_page->frame.leaf_page.num_of_keys; i > insertion_point; --i) {
        leaf_page->frame.leaf_page.records[i].key = leaf_page->frame.leaf_page.records[i - 1].key;
        strncpy(leaf_page->frame.leaf_page.records[i].value, leaf_page->frame.leaf_page.records[i - 1].value, 120);
    }
    leaf_page->frame.leaf_page.records[insertion_point].key = key;
    strncpy(leaf_page->frame.leaf_page.records[insertion_point].value, value, 119);
    leaf_page->frame.leaf_page.records[insertion_point].value[119] = '\0';
    ++leaf_page->frame.leaf_page.num_of_keys;
    buf_put_page(leaf_page, 1);
}

/**
 * Inserts a new record into a leaf
 * in case of full leaf. So perform split
 * the leaf page.
 * Returns the root page number of the tree after insertion.
 */
static pagenum_t _insert_into_leaf_after_split(int table_id, pagenum_t root, pagenum_t leaf, int64_t key, char *value) {
    pagenum_t new_leaf = buf_alloc_page(table_id);
    buffer_t *new_leaf_page, *leaf_page;
    int insertion_index, i, j, split;
    int64_t *temp_keys;
    int64_t new_key;
    char (*temp_values)[120];

    new_leaf_page = buf_get_page(table_id, new_leaf);
    new_leaf_page->frame.leaf_page.is_leaf = 1;

    temp_keys = malloc(ORDER_OF_LEAF * sizeof(int64_t));
    if (temp_keys == NULL) {
        perror("Temporary keys array.");
        exit(1);
    }

    temp_values = malloc(ORDER_OF_LEAF * 120 * sizeof(char));
    if (temp_values == NULL) {
        perror("Temporary keys array.");
        exit(1);
    }

    leaf_page = buf_get_page(table_id, leaf);

    insertion_index = 0;
    while (insertion_index < ORDER_OF_LEAF - 1 && leaf_page->frame.leaf_page.records[insertion_index].key < key) {
        ++insertion_index;
    }

    for (i = 0, j = 0; i < ORDER_OF_LEAF - 1; ++i, ++j) {
        if (j == insertion_index) ++j;
        temp_keys[j] = leaf_page->frame.leaf_page.records[i].key;
        strncpy(temp_values[j], leaf_page->frame.leaf_page.records[i].value, 120);
    }

    temp_keys[insertion_index] = key;
    strncpy(temp_values[insertion_index], value, 120);
    temp_values[insertion_index][119] = '\0';

    leaf_page->frame.leaf_page.num_of_keys = 0;

    split = _cut(ORDER_OF_LEAF - 1);

    for (i = 0; i < split; ++i) {
        strncpy(leaf_page->frame.leaf_page.records[i].value, temp_values[i], 120);
        leaf_page->frame.leaf_page.records[i].key = temp_keys[i];
    }
    leaf_page->frame.leaf_page.num_of_keys = i;

    for (i = split, j = 0; i < ORDER_OF_LEAF; ++i, ++j) {
        strncpy(new_leaf_page->frame.leaf_page.records[j].value, temp_values[i], 120);
        new_leaf_page->frame.leaf_page.records[j].key = temp_keys[i];
    }
    new_leaf_page->frame.leaf_page.num_of_keys = j;

    free(temp_keys);
    free(temp_values);

    new_leaf_page->frame.leaf_page.right_sibling_pagenum = leaf_page->frame.leaf_page.right_sibling_pagenum;
    leaf_page->frame.leaf_page.right_sibling_pagenum = new_leaf;

    new_leaf_page->frame.leaf_page.parent_pagenum = leaf_page->frame.leaf_page.parent_pagenum;
    new_key = new_leaf_page->frame.leaf_page.records[0].key;

    buf_put_page(leaf_page, 1);
    buf_put_page(new_leaf_page, 1);

    return _insert_into_parent(table_id, root, leaf, new_key, new_leaf);
}

// deletion.

/**
 * Check whether the root is empty or not.
 * And if it is empty, adjust root.
 * Then free old root page.
 */
static void _adjust_root(int table_id, pagenum_t root) {
    pagenum_t new_root;
    buffer_t *root_page;

    root_page = buf_get_page(table_id, root);

    /* Case: nonempty root. */
    if (root_page->frame.internal_page.num_of_keys > 0) {
        buf_put_page(root_page, 0);
        return;
    }
    
    /* Case: empty root. */

    // If it has a child, promote
    // the first (only) child as the new root.
    if (!root_page->frame.internal_page.is_leaf) {
        new_root = root_page->frame.internal_page.first_pagenum;

        buf_put_page(root_page, 0);
        root_page = buf_get_page(table_id, new_root);
        root_page->frame.internal_page.parent_pagenum = 0;
        buf_put_page(root_page, 1);
    }

    // If it is a leaf (has no child),
    // then the whole tree is empty.

    else {
        new_root = 0;
        buf_put_page(root_page, 0);
    }

    root_page = buf_get_page(table_id, 0);
    root_page->frame.header_page.root_pagenum = new_root;
    buf_put_page(root_page, 1);

    buf_free_page(table_id, root);
}

/**
 * Utility function for deletion. Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
static int _get_neighbor_index(int table_id, pagenum_t parent, pagenum_t node) {
    int i;
    buffer_t *temp_page;
        
    temp_page = buf_get_page(table_id, parent);
    /* Return the index of the neighbor node to the left
     * of the pagenum in the parent pointing to
     * given node.
     * If given node is the leftmost child,
     * this means return -1.
     */
    for (i = 0; i <= temp_page->frame.internal_page.num_of_keys; ++i) {
        if (*(&temp_page->frame.internal_page.first_pagenum + 2 * i) == node) {
            buf_put_page(temp_page, 0);
            return i - 1;
        }
    }

    perror("Search for nonexistent pointer to node in parent.");
    exit(EXIT_FAILURE);
}

/* Remove given pointer from the given node.
 * Interanl page version of remove_entry_from_node in bpt.c
 * Return number of keys of given leaf page.
 */
static int _remove_entry_from_internal_node(int table_id, pagenum_t node, int64_t key, pagenum_t pointer) {
    
    int i;
    buffer_t *internal_page;
    int result;

    internal_page = buf_get_page(table_id, node);

    // Remove the key and shift other keys accordingly.
    i = 0;
    while (internal_page->frame.internal_page.entries[i].key != key) {
        ++i;
    }
    for (++i; i < internal_page->frame.internal_page.num_of_keys; ++i) {
        internal_page->frame.internal_page.entries[i - 1].key = internal_page->frame.internal_page.entries[i].key;
    }

    // Remove the child pagenum and shift other values accordingly.
    i = 0;
    while (*(&internal_page->frame.internal_page.first_pagenum + 2 * i) != pointer) {
        ++i;
    }
    for (++i; i < internal_page->frame.internal_page.num_of_keys + 1; ++i) {
        *(&internal_page->frame.internal_page.first_pagenum + 2 * (i - 1)) = *(&internal_page->frame.internal_page.first_pagenum + 2 * i);
    }

    // Decrease number of keys
    --internal_page->frame.internal_page.num_of_keys;

    result = internal_page->frame.internal_page.num_of_keys;

    buf_put_page(internal_page, 1);

    return result;
}

/**
 * Remove given record from the given leaf.
 * Leaf page version of remove_entry_from_node in bpt.c
 * Return number of keys of given leaf page.
 */
static int _remove_record_from_leaf(int table_id, pagenum_t leaf, int64_t key, char *value) {
    
    int i;
    buffer_t *leaf_page;
    int result;

    leaf_page = buf_get_page(table_id, leaf);

    // Remove the key and shift other keys accordingly.
    i = 0;
    while (leaf_page->frame.leaf_page.records[i].key != key) {
        ++i;
    }
    for (++i; i < leaf_page->frame.leaf_page.num_of_keys; ++i) {
        leaf_page->frame.leaf_page.records[i - 1].key = leaf_page->frame.leaf_page.records[i].key;
    }

    // Remove the value and shift other values accordingly.
    i = 0;
    while (strcmp(leaf_page->frame.leaf_page.records[i].value, value) != 0) {
        ++i;
    }
    for (++i; i < leaf_page->frame.leaf_page.num_of_keys; ++i) {
        strncpy(leaf_page->frame.leaf_page.records[i - 1].value, leaf_page->frame.leaf_page.records[i].value, 120);
    }

    // Decrease number of keys
    --leaf_page->frame.leaf_page.num_of_keys;

    result = leaf_page->frame.leaf_page.num_of_keys;

    buf_put_page(leaf_page, 1);

    return result;
}

/**
 * Perform delayed merge with a node that has become
 * empty after deletion
 * and a neighboring node.
 */
static void _delayed_merge_nodes(int table_id, pagenum_t root, pagenum_t node, pagenum_t parent
        , pagenum_t neighbor, int neighbor_index, int64_t k_prime) {
    
    buffer_t *node_page, *neighbor_page, *temp_page;
    pagenum_t last_pagenum;
    int is_leaf, i;
    char dirty = 0;

    /* Read is_leaf and right_sibling_pagenum(in leaf)
     * or first_child_pagenum(in internal) from given node.
     */
    node_page = buf_get_page(table_id, node);
    is_leaf = node_page->frame.internal_page.is_leaf;
    last_pagenum = node_page->frame.leaf_page.right_sibling_pagenum;

    // Read neighbor page to temp_page    
    neighbor_page = buf_get_page(table_id, neighbor);

    /* Case: leaf node.
     * If given node is not leftmost child,
     * change neighbor's right sibling node to
     * given node's right sibling node.
     */
    if (is_leaf) {
        if (neighbor_index != -1) {
            neighbor_page->frame.leaf_page.right_sibling_pagenum = last_pagenum;
        } else {
            memcpy(&node_page->frame, &neighbor_page->frame, ON_DISK_PAGE_SIZE);
            node = neighbor;
            dirty = 1;
        }
    }

    /* Case: non-leaf node. */
    else {
        /* If given node has neighbor to left,
         * append k_prime and given node's only child to neighbor.
         */
        if (neighbor_index != -1) {
            neighbor_page->frame.internal_page.entries[neighbor_page->frame.internal_page.num_of_keys].key = k_prime;
            neighbor_page->frame.internal_page.entries[neighbor_page->frame.internal_page.num_of_keys].pagenum = last_pagenum;
            ++neighbor_page->frame.internal_page.num_of_keys;
        } 
        // If given node is leftmost child.
        else {
            /* Insert k_prime and given node's only child to
             * left of neighbor node.
             */
            for (i = neighbor_page->frame.internal_page.num_of_keys; i > 0; --i) {
                neighbor_page->frame.internal_page.entries[i].key = neighbor_page->frame.internal_page.entries[i - 1].key;
                neighbor_page->frame.internal_page.entries[i].pagenum = neighbor_page->frame.internal_page.entries[i - 1].pagenum;
            }
            neighbor_page->frame.internal_page.entries[0].pagenum = neighbor_page->frame.internal_page.first_pagenum;

            neighbor_page->frame.internal_page.entries[0].key = k_prime;
            neighbor_page->frame.internal_page.first_pagenum = last_pagenum;
            ++neighbor_page->frame.internal_page.num_of_keys;
        }

        temp_page = buf_get_page(table_id, last_pagenum);
        temp_page->frame.internal_page.parent_pagenum = neighbor;
        buf_put_page(temp_page, 1);
    }

    buf_put_page(node_page, dirty);
    buf_put_page(neighbor_page, !dirty);

    buf_free_page(table_id, node);
    _delete_internal_entry(table_id, root, parent, k_prime, node);
}

/**
 * Deletes an record from the B+ tree.
 * Removes the record from the leaf, and then
 * makes all appropriate changes to preserve
 * the B+ tree properties.
 */
static void _delete_record(int table_id, pagenum_t root, pagenum_t leaf, int64_t key, char *value) {
    
    int leaf_num_keys, neighbor_index, k_prime_index;
    pagenum_t neighbor, parent;
    buffer_t *temp_page;
    int64_t k_prime;

    // Remove record from leaf.
    leaf_num_keys = _remove_record_from_leaf(table_id, leaf, key, value);

    /* Case: deletion was performed in the root. */
    if (leaf == root) {
        _adjust_root(table_id, root);
        return;
    }

    /* Case: leaf page still has some records.
     * (The simple case.)
     */
    if (leaf_num_keys > 0) {
        return;
    }

    /* Case: leaf page has no record any more.
     * Merge is needed.
     */

    /* Find the appropriate neighbor node with which
     * to merge.
     * Also find the key (k_prime) in the parent
     * between the leaf and the neighbor leaf.
     */

    temp_page = buf_get_page(table_id, leaf);
    parent = temp_page->frame.leaf_page.parent_pagenum;
    buf_put_page(temp_page, 0);

    neighbor_index = _get_neighbor_index(table_id, parent, leaf);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

    temp_page = buf_get_page(table_id, parent);
    neighbor = *(&temp_page->frame.internal_page.first_pagenum + (neighbor_index == -1 ? 2 * 1 : 2 * neighbor_index));
    k_prime = temp_page->frame.internal_page.entries[k_prime_index].key;
    buf_put_page(temp_page, 0);


    _delayed_merge_nodes(table_id, root, leaf, parent, neighbor, neighbor_index, k_prime);
}

/**
 * Redistributes entries between two internal nodes
 * when one has become empty after deletion
 * but its neighbor is too big to append
 * one more entry without exceeding the
 * maximum
 */
static void _redistribute_nodes(int table_id, pagenum_t node, pagenum_t parent
        , pagenum_t neighbor, int neighbor_index, int64_t k_prime, int k_prime_index) {  
    
    buffer_t *neighbor_page, *parent_page, *node_page, *temp_page;
    pagenum_t temp_pagenum;
    int64_t temp_key;
    int i;

    /* Case: node has a neighbor to the left.
     * Pull the neighbor's last key-pagenum pair over
     * from the neighbor's right end to node's left end.
     */
    if (neighbor_index != -1) {
        neighbor_page = buf_get_page(table_id, neighbor);

        temp_key = neighbor_page->frame.internal_page.entries[ORDER_OF_INTERNAL - 2].key;
        temp_pagenum = neighbor_page->frame.internal_page.entries[ORDER_OF_INTERNAL - 2].pagenum;
        --neighbor_page->frame.internal_page.num_of_keys;


        parent_page = buf_get_page(table_id, parent);
        parent_page->frame.internal_page.entries[k_prime_index].key = temp_key;

        node_page = buf_get_page(table_id, node);
        node_page->frame.internal_page.entries[0].key = k_prime;
        node_page->frame.internal_page.entries[0].pagenum = node_page->frame.internal_page.first_pagenum;
        node_page->frame.internal_page.first_pagenum = temp_pagenum;
        ++node_page->frame.internal_page.num_of_keys;

        temp_page = buf_get_page(table_id, temp_pagenum);
        temp_page->frame.internal_page.parent_pagenum = node;
    }

    /* Case: node is the leftmost child.
     * Take a key-pagenum pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pagenum pair
     * to node's rightmost position.
     */
    else {
        neighbor_page = buf_get_page(table_id, neighbor);

        temp_key = neighbor_page->frame.internal_page.entries[0].key;
        temp_pagenum = neighbor_page->frame.internal_page.first_pagenum;
        neighbor_page->frame.internal_page.first_pagenum = neighbor_page->frame.internal_page.entries[0].pagenum;

        for (i = 0; i < neighbor_page->frame.internal_page.num_of_keys - 1; ++i) {
            neighbor_page->frame.internal_page.entries[i].key = neighbor_page->frame.internal_page.entries[i + 1].key;
            neighbor_page->frame.internal_page.entries[i].pagenum = neighbor_page->frame.internal_page.entries[i + 1].pagenum;
        }
        --neighbor_page->frame.internal_page.num_of_keys;

        parent_page = buf_get_page(table_id, parent);
        parent_page->frame.internal_page.entries[k_prime_index].key = temp_key;

        node_page = buf_get_page(table_id, node);
        node_page->frame.internal_page.entries[0].key = k_prime;
        node_page->frame.internal_page.entries[0].pagenum = temp_pagenum;
        ++node_page->frame.internal_page.num_of_keys;

        temp_page->frame.internal_page.parent_pagenum = node;
    }

    buf_put_page(neighbor_page, 1);
    buf_put_page(parent_page, 1);
    buf_put_page(node_page, 1);
    buf_put_page(temp_page, 1);
}

/**
 * Deletes an entry from the B+ tree.
 * Removes the entry from the internal node, and then
 * makes all appropriate changes to preserve
 * the B+ tree properties.
 */
static void _delete_internal_entry(int table_id, pagenum_t root, pagenum_t node, int64_t key, pagenum_t pointer) {
    
    int node_num_keys, neighbor_index, k_prime_index, neighbor_num_keys;
    pagenum_t neighbor, parent;
    buffer_t *temp_page;
    int64_t k_prime;

    // Remove key and pagenum from internal node.
    node_num_keys = _remove_entry_from_internal_node(table_id, node, key, pointer);

    /* Case: deletion was performed in the root. */
    if (node == root) {
        _adjust_root(table_id, root);
        return;
    }

    /* Case: internal page still has some keys.
     * (The simple case.)
     */
    if (node_num_keys > 0) {
        return;
    }

    /* Case: internal page has no key any more.
     * Either merge or redistribution is needed.
     */

    /* Find the appropriate neighbor node with which
     * to merge or redistribute.
     * Also find the key (k_prime) in the parent
     * between the pagenum to given node and 
     * the pagenum to the neighbor node.
     */

    temp_page = buf_get_page(table_id, node);
    parent = temp_page->frame.internal_page.parent_pagenum;
    buf_put_page(temp_page, 0);
    
    neighbor_index = _get_neighbor_index(table_id, parent, node);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

    temp_page = buf_get_page(table_id, parent);
    neighbor = *(&temp_page->frame.internal_page.first_pagenum + (neighbor_index == -1 ? 2 * 1 : 2 * neighbor_index));
    k_prime = temp_page->frame.internal_page.entries[k_prime_index].key;
    buf_put_page(temp_page, 0);


    temp_page = buf_get_page(table_id, neighbor);
    neighbor_num_keys = temp_page->frame.internal_page.num_of_keys;
    buf_put_page(temp_page, 0);

    /* Delayed merge. */
    if (neighbor_num_keys < ORDER_OF_INTERNAL - 1)
        _delayed_merge_nodes(table_id, root, node, parent, neighbor, neighbor_index, k_prime);
    /* Redistribution. */
    else
        _redistribute_nodes(table_id, node, parent, neighbor, neighbor_index, k_prime, k_prime_index);
}



// External functions.

/**
 * A initializing function.
 * Allocate the buffer pool (array) 
 *   with the given number of entries by buf_num
 *   by calling buffer initializing function in buffer management layer.
 * Initialize other fields such as state info, LRU info, etc.
 * \param buf_num Number of entries in the buffer pool.
 *      Allocate with this number of buffers.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int init_db(int num_buf) {
    return buf_init_db(num_buf);
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
int open_table(char *pathname) {
    return buf_open_table(pathname);
}


/**
 * Insert input ‘key/value’ (record) to data file at the right place.
 * If success, return 0. Otherwise, return non-zero value.
 */
int db_insert(int table_id, int64_t key, char * value) {
    buffer_t *tmp_page;
    pagenum_t root, leaf, new_root;
    int leaf_num_keys;

    tmp_page = buf_get_page(table_id, 0);
    root = tmp_page->frame.header_page.root_pagenum;
    buf_put_page(tmp_page, 0);

    // No duplicates.
    if (db_find(table_id, key, NULL) == 0)
        return 1;
    
    /* Case: the tree doesn't exist yet.
     * Start new tree with given record.
     */
    if (root == 0) {
        new_root = _start_new_tree(table_id, key, value);
        tmp_page = buf_get_page(table_id, 0);
        tmp_page->frame.header_page.root_pagenum = new_root;
        buf_put_page(tmp_page, 1);
        return 0;
    }
    // Tree already exists.

    leaf = _find_leaf(table_id, root, key);

    // Read leaf page to tmp_page
    tmp_page = buf_get_page(table_id, leaf);

    /* Case: leaf has some space to store a new record.
     */
    leaf_num_keys = tmp_page->frame.leaf_page.num_of_keys;
    buf_put_page(tmp_page, 0);
    if (leaf_num_keys < ORDER_OF_LEAF - 1) {
        _insert_into_leaf(table_id, leaf, key, value);
        return 0;
    }

    /* Case: leaf must be split.
     */

    new_root = _insert_into_leaf_after_split(table_id, root, leaf, key, value);
    if (root != new_root) {
        tmp_page = buf_get_page(table_id, 0);
        tmp_page->frame.header_page.root_pagenum = new_root;
        buf_put_page(tmp_page, 1);
    }
    return 0;
}


/**
 * Find the record containing input ‘key’.
 * If found matching ‘key’, store matched ‘value’ string in ret_val and return 0.
 * Otherwise, return non-zero value.
 * Memory allocation for record structure(ret_val) should occur in caller function.
 * If ret_val is NULL, just find whether there is the key or not wi
 */
int db_find(int table_id, int64_t key, char * ret_val) {
    int i;
    pagenum_t leaf, root;
    buffer_t *tmp_page;

    tmp_page = buf_get_page(table_id, 0);
    root = tmp_page->frame.header_page.root_pagenum;
    buf_put_page(tmp_page, 0);

    leaf = _find_leaf(table_id, root, key);
    if (leaf == 0) return 1;

    tmp_page = buf_get_page(table_id, leaf);
    for (i = 0; i < tmp_page->frame.leaf_page.num_of_keys; ++i) {
        if (tmp_page->frame.leaf_page.records[i].key == key) break;
    }
    if (i == tmp_page->frame.leaf_page.num_of_keys) {
        buf_put_page(tmp_page, 0);
        return 1;
    } else {
        if (ret_val != NULL)
            strncpy(ret_val, tmp_page->frame.leaf_page.records[i].value, 120);
        buf_put_page(tmp_page, 0);
        return 0;
    }
}


/**
 * Find the matching record and delete it if found.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int db_delete(int table_id, int64_t key) {
    buffer_t *temp_page;
    pagenum_t root, new_root;
    char value[120];

    temp_page = buf_get_page(table_id, 0);
    root = temp_page->frame.header_page.root_pagenum;
    buf_put_page(temp_page, 0);

    /* If there isn't given key in tree,
     * deletion fails and return non-zero value
     */
    if (db_find(table_id, key, value) != 0) {
        return 1;
    }

    _delete_record(table_id, root, _find_leaf(table_id, root, key), key, value);

    return 0;
}


/** 
 * Write all pages of this table from buffer to disk
 *      and discard the table id.
 * \param table_id Indicating target table to be closed.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int close_table(int table_id) {
    return buf_close_table(table_id);
}

/**
 * Flush all data from buffer and destroy allocated buffer.
 * \return If success, return 0. Otherwise, return non-zero value.
 */
int shutdown_db(void) {
    return buf_shutdown_db();
}

/**
 * Do natural join with given two tables
 * and write result table to the file using given pathname.
 * Two tables should have been opened earlier.
 * 
 * Result file format should contain a line of 
 *      “a.key,a.value,b.key,b.value”
 * where each items are separated by comma.
 * 
 * \param table_id_1 table id of the first join target table.
 * \param table_id_2 table id of the second join target table.
 * \param pathname path name for result file.
 * 
 * \return Return 0 if success, otherwise return non-zero value.
 */
int join_table(int table_id_1, int table_id_2, char * pathname) {
    buffer_t *curr_page_1, *curr_page_2;
    pagenum_t root_pagenum_1, root_pagenum_2, temp_pagenum;
    buffer_t *header_1, *header_2;
    int curr_rec_1 = 0, curr_rec_2 = 0;
    FILE *output;

    if (!pathname) {
        return -1;
    }

    output = fopen(pathname, "w");

    header_1 = buf_get_page(table_id_1, 0);
    root_pagenum_1 = header_1->frame.header_page.root_pagenum;
    buf_put_page(header_1, 0);

    header_2 = buf_get_page(table_id_2, 0);
    root_pagenum_2 = header_2->frame.header_page.root_pagenum;
    buf_put_page(header_2, 0);

    if (!root_pagenum_1 || !root_pagenum_2) {
        fclose(output);
        return 0;
    }

    curr_page_1 = buf_get_page(table_id_1, _find_leaf(table_id_1, root_pagenum_1, INT64_MIN));
    curr_page_2 = buf_get_page(table_id_2, _find_leaf(table_id_2, root_pagenum_2, INT64_MIN));

    while (1) {
        while (curr_page_1->frame.leaf_page.records[curr_rec_1].key
                < curr_page_2->frame.leaf_page.records[curr_rec_2].key) {
            
            ++curr_rec_1;
            if (curr_rec_1 >= ORDER_OF_LEAF - 1) {
                temp_pagenum = curr_page_1->frame.leaf_page.right_sibling_pagenum;
                if (temp_pagenum == 0) {
                    buf_put_page(curr_page_1, 0);
                    buf_put_page(curr_page_2, 0);
                    fclose(output);
                    return 0;
                }
                buf_put_page(curr_page_1, 0);
                curr_page_1 = buf_get_page(table_id_1, temp_pagenum);
                curr_rec_1 = 0;
            }
        }
        
        while (curr_page_2->frame.leaf_page.records[curr_rec_2].key
                < curr_page_1->frame.leaf_page.records[curr_rec_1].key) {
            
            ++curr_rec_2;
            if (curr_rec_2 >= ORDER_OF_LEAF - 1) {
                temp_pagenum = curr_page_2->frame.leaf_page.right_sibling_pagenum;
                if (temp_pagenum == 0) {
                    buf_put_page(curr_page_1, 0);
                    buf_put_page(curr_page_2, 0);
                    fclose(output);
                    return 0;
                }
                buf_put_page(curr_page_2, 0);
                curr_page_2 = buf_get_page(table_id_2, temp_pagenum);
                curr_rec_2 = 0;
            }
        }

        if (curr_page_1->frame.leaf_page.records[curr_rec_1].key
                == curr_page_2->frame.leaf_page.records[curr_rec_2].key) {
            fprintf(output, "%ld,%s,%ld,%s\n"
                , curr_page_1->frame.leaf_page.records[curr_rec_1].key
                , curr_page_1->frame.leaf_page.records[curr_rec_1].value
                , curr_page_2->frame.leaf_page.records[curr_rec_2].key
                , curr_page_2->frame.leaf_page.records[curr_rec_2].value);

            ++curr_rec_2;
            if (curr_rec_2 >= ORDER_OF_LEAF - 1) {
                temp_pagenum = curr_page_2->frame.leaf_page.right_sibling_pagenum;
                if (temp_pagenum == 0) {
                    buf_put_page(curr_page_1, 0);
                    buf_put_page(curr_page_2, 0);
                    fclose(output);
                    return 0;
                }
                buf_put_page(curr_page_2, 0);
                curr_page_2 = buf_get_page(table_id_2, temp_pagenum);
                curr_rec_2 = 0;
            }
        }
    }
}
