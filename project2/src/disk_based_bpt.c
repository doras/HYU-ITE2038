#include "disk_based_bpt.h"

// CONSTANTS.

/* Order of leaf page.
 */
const int ORDER_OF_LEAF = 32;

/* Order of internal page.
 */
const int ORDER_OF_INTERNAL = 249;


// GLOBALS.

char* g_path_names[5];


// FUNCTIONS.

// Internal functions
// Reference to bpt.c

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the debug flag is set.
 * Returns the page number of leaf containing the given key.
 */
pagenum_t _find_leaf(pagenum_t root, int64_t key, int debug) {
    int i = 0;
    page_t c;

    // if tree is empty.
    if (root == 0) {
        if (debug) 
            printf("Empty tree.\n");
        return root;
    }
    
    // Read root page to c
    c.style = PAGE_INTERNAL;
    file_read_page(root, &c);


    while (!c.internal_page.is_leaf) {
        if (debug) {
            printf("[");
            for (i = 0; i < c.internal_page.num_of_keys - 1; i++)
                printf("%lld ", c.internal_page.entries[i].key);
            printf("%lld] ", c.internal_page.entries[i].key);
        }
        i = 0;
        while (i < c.internal_page.num_of_keys) {
            if (key >= c.internal_page.entries[i].key) i++;
            else break;
        }
        if (debug)
            printf("%d ->\n", i);
        root = *(&c.internal_page.first_pagenum + 2 * i);
        file_read_page(root, &c);
    }
    if (debug) {
        printf("Leaf [");
        for (i = 0; i < c.leaf_page.num_of_keys - 1; i++)
            printf("%lld ", c.leaf_page.records[i].key);
        printf("%lld] ->\n", c.leaf_page.records[i].key);
    }
    return root;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int _cut(int length) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

/* Helper function used in _insert_into_parent
 * to find the index of the parent's child pagenum to 
 * the left that has the key to be inserted.
 */
int _get_left_index(pagenum_t parent, pagenum_t left) {

    int left_index = 0;
    page_t parent_page;
    parent_page.style = PAGE_INTERNAL;
    file_read_page(parent, &parent_page);
    while (left_index <= parent_page.internal_page.num_of_keys && 
            *(&(parent_page.internal_page.first_pagenum) + 2 * left_index) != left)
        left_index++;
    return left_index;
}

/* Start a new tree with given record.
 * This function will be called at first insertion.
 */
pagenum_t _start_new_tree(int64_t key, char * value) {
    page_t root;
    pagenum_t pagenum = file_alloc_page();

    root.leaf_page.is_leaf = 1;
    root.leaf_page.num_of_keys = 1;
    root.leaf_page.parent_pagenum = 0;
    root.leaf_page.records[0].key = key;
    strcpy(root.leaf_page.records[0].value, value);
    root.leaf_page.right_sibling_pagenum = 0;
    root.style = PAGE_LEAF;
    
    file_write_page(pagenum, &root);

    return pagenum;
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 * Returns new root page number.
 */
pagenum_t _insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right) {

    pagenum_t root = file_alloc_page();
    page_t root_page;
    root_page.internal_page.entries[0].key = key;
    root_page.internal_page.first_pagenum = left;
    root_page.internal_page.entries[0].pagenum = right;
    root_page.internal_page.num_of_keys = 1;
    root_page.internal_page.parent_pagenum = 0;
    root_page.internal_page.is_leaf = 0;
    root_page.style = PAGE_INTERNAL;
    file_write_page(root, &root_page);

    /* For minimize the FILE I/O,
     * change only first 8 byte that is parent page number.
     * For this, set the page style to PAGE_FREE,
     * which means that read and write will be performed in
     * only first 8 byte.
     */
    root_page.style = PAGE_FREE;
    root_page.internal_page.parent_pagenum = root;
    file_write_page(left, &root_page);
    file_write_page(right, &root_page);

    return root;
}

/* Inserts a new key and child node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
void _insert_into_node(pagenum_t node, 
        int left_index, int64_t key, pagenum_t right) {

    int i;
    page_t node_page;

    node_page.style = PAGE_INTERNAL;
    file_read_page(node, &node_page);

    for (i = node_page.internal_page.num_of_keys; i > left_index; i--) {
        *(&node_page.internal_page.first_pagenum + 2 * (i + 1)) = *(&node_page.internal_page.first_pagenum + 2 * i);
        node_page.internal_page.entries[i].key = node_page.internal_page.entries[i - 1].key;
    }
    node_page.internal_page.entries[left_index].pagenum = right;
    node_page.internal_page.entries[left_index].key = key;
    ++node_page.internal_page.num_of_keys;
    file_write_page(node, &node_page);
}

/* Inserts a new key and child node page number
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 * Returns the root page number of the tree after insertion.
 */
pagenum_t _insert_into_node_after_split(pagenum_t root, pagenum_t old_node, int left_index, 
        int64_t key, pagenum_t right) {

    page_t old_node_page, new_node_page, child_page;
    pagenum_t child, new_node = file_alloc_page();
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

    old_node_page.style = PAGE_INTERNAL;
    file_read_page(old_node, &old_node_page);

    for (i = 0, j = 0; i < old_node_page.internal_page.num_of_keys + 1; ++i, ++j) {
        if(j == left_index + 1) ++j;
        temp_pagenums[j] = *(&old_node_page.internal_page.first_pagenum + 2 * i);
    }
    
    for (i = 0, j = 0; i < old_node_page.internal_page.num_of_keys; ++i, ++j) {
        if (j == left_index) ++j;
        temp_keys[j] = old_node_page.internal_page.entries[i].key;
    }

    temp_pagenums[left_index + 1] = right;
    temp_keys[left_index] = key;

    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */  
    split = _cut(ORDER_OF_INTERNAL);
    new_node_page.internal_page.is_leaf = 0;
    new_node_page.style = PAGE_INTERNAL;
    for (i = 0; i < split - 1; ++i) {
        *(&old_node_page.internal_page.first_pagenum + 2 * i) = temp_pagenums[i];
        old_node_page.internal_page.entries[i].key = temp_keys[i];
    }
    old_node_page.internal_page.num_of_keys = i;
    old_node_page.internal_page.entries[i - 1].pagenum = temp_pagenums[i];
    k_prime = temp_keys[split - 1];
    for (++i, j = 0; i < ORDER_OF_INTERNAL; ++i, ++j) {
        *(&new_node_page.internal_page.first_pagenum + 2 * j) = temp_pagenums[i];
        new_node_page.internal_page.entries[j].key = temp_keys[i];
    }
    new_node_page.internal_page.num_of_keys = j;
    new_node_page.internal_page.entries[j - 1].pagenum = temp_pagenums[i];
    free(temp_pagenums);
    free(temp_keys);

    new_node_page.internal_page.parent_pagenum = old_node_page.internal_page.parent_pagenum;

    child_page.style = PAGE_FREE; // For writing only 8-byte.
    child_page.internal_page.parent_pagenum = new_node;
    for (i = 0; i <= new_node_page.internal_page.num_of_keys; ++i) {
        child = *(&new_node_page.internal_page.first_pagenum + 2 * i);
        file_write_page(child, &child_page);
    }

    file_write_page(old_node, &old_node_page);
    file_write_page(new_node, &new_node_page);

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */

    return _insert_into_parent(root, old_node, k_prime, new_node);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root page number of the tree after insertion.
 */
pagenum_t _insert_into_parent(pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {
    
    int left_index;
    pagenum_t parent;
    page_t page;

    /* For minimize FILE I/O, set style of left_page
     * to PAGE_FREE, which means that
     * read will be performed in first 8 byte
     * that is parent_pagenum.
     */
    page.style = PAGE_FREE;
    file_read_page(left, &page);

    parent = page.internal_page.parent_pagenum;

    /* Case: parent is new root. */
    if (parent == 0) {
        return _insert_into_new_root(left, key, right);
    }
    /* Otherwise, parent is leaf or internal. */

    /* Find the parent's child pagenum to the left node */
    left_index = _get_left_index(parent, left);


    // For reading only 24-byte, set style of left_page to PAGE_HEADER.
    page.style = PAGE_HEADER;
    file_read_page(parent, &page);

    /* Simple case: the new key fits into the node. */
    if (page.internal_page.num_of_keys < ORDER_OF_INTERNAL - 1) {
        _insert_into_node(parent, left_index, key, right);
        return root;
    }

    /* Harder case: split a node in order
     * to preserve the B+ tree properties.
     */

    return _insert_into_node_after_split(root, parent, left_index, key, right);
}

/* Inserts a new record into a leaf.
 */
void _insert_into_leaf(pagenum_t leaf, int64_t key, char *value) {

    int i, insertion_point;
    page_t leaf_page;

    leaf_page.style = PAGE_LEAF;
    file_read_page(leaf, &leaf_page);
    insertion_point = 0;
    while (insertion_point < leaf_page.leaf_page.num_of_keys && leaf_page.leaf_page.records[insertion_point].key < key) {
        ++insertion_point;
    }

    for(i = leaf_page.leaf_page.num_of_keys; i > insertion_point; --i) {
        leaf_page.leaf_page.records[i].key = leaf_page.leaf_page.records[i - 1].key;
        strcpy(leaf_page.leaf_page.records[i].value, leaf_page.leaf_page.records[i - 1].value);
    }
    leaf_page.leaf_page.records[insertion_point].key = key;
    strcpy(leaf_page.leaf_page.records[insertion_point].value, value);
    ++leaf_page.leaf_page.num_of_keys;
    file_write_page(leaf, &leaf_page);
}

/* Inserts a new record into a leaf
 * in case of full leaf. So perform split
 * the leaf page.
 * Returns the root page number of the tree after insertion.
 */
pagenum_t _insert_into_leaf_after_split(pagenum_t root, pagenum_t leaf, int64_t key, char *value) {
    
    pagenum_t new_leaf = file_alloc_page();
    page_t new_leaf_page, leaf_page;
    int insertion_index, i, j, split;
    int64_t *temp_keys;
    int64_t new_key;
    char (*temp_values)[120];

    new_leaf_page.leaf_page.is_leaf = 1;
    new_leaf_page.style = PAGE_LEAF;

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

    leaf_page.style = PAGE_LEAF;
    file_read_page(leaf, &leaf_page);

    insertion_index = 0;
    while (insertion_index < ORDER_OF_LEAF - 1 && leaf_page.leaf_page.records[insertion_index].key < key) {
        ++insertion_index;
    }

    for (i = 0, j = 0; i < ORDER_OF_LEAF - 1; ++i, ++j) {
        if (j == insertion_index) ++j;
        temp_keys[j] = leaf_page.leaf_page.records[i].key;
        strcpy(temp_values[j], leaf_page.leaf_page.records[i].value);
    }

    temp_keys[insertion_index] = key;
    strcpy(temp_values[insertion_index], value);

    leaf_page.leaf_page.num_of_keys = 0;

    split = _cut(ORDER_OF_LEAF - 1);

    for (i = 0; i < split; ++i) {
        strcpy(leaf_page.leaf_page.records[i].value, temp_values[i]);
        leaf_page.leaf_page.records[i].key = temp_keys[i];
    }
    leaf_page.leaf_page.num_of_keys = i;

    for (i = split, j = 0; i < ORDER_OF_LEAF; ++i, ++j) {
        strcpy(new_leaf_page.leaf_page.records[j].value, temp_values[i]);
        new_leaf_page.leaf_page.records[j].key = temp_keys[i];
    }
    new_leaf_page.leaf_page.num_of_keys = j;

    free(temp_keys);
    free(temp_values);

    new_leaf_page.leaf_page.right_sibling_pagenum = leaf_page.leaf_page.right_sibling_pagenum;
    leaf_page.leaf_page.right_sibling_pagenum = new_leaf;

    new_leaf_page.leaf_page.parent_pagenum = leaf_page.leaf_page.parent_pagenum;
    new_key = new_leaf_page.leaf_page.records[0].key;

    file_write_page(leaf, &leaf_page);
    file_write_page(new_leaf, &new_leaf_page);

    return _insert_into_parent(root, leaf, new_key, new_leaf);
}


// Functions of own disk-based bpt

/* Open data file using ‘pathname’ or create one if not existed.
 * If success, return the unique table id.
 * Otherwise, return negative value.
 */
int open_table(char *pathname) {
    int table_id;
    
    // Open file
    file_open_file(pathname);

    /* Search for table id
     * If the table id that has this pathnamealready exist,
     *     return that id.
     */
    for(table_id = 0; table_id < 5; ++table_id) {
        if (g_path_names[table_id] != NULL
            && strcmp(pathname, g_path_names[table_id]) == 0) {
            return table_id;
        }
    }
    // Otherwise, search free id.
    for(table_id = 0; g_path_names[table_id] != NULL; ++table_id) {
        continue;
    }

    // If there is no free id, close file and fail.
    if(table_id >= 5) {
        file_close_file();
        return -1;
    }
    // Otherwise, record path name to global array and return id.
    g_path_names[table_id] = pathname;
    return table_id;
}


/* Insert input ‘key/value’ (record) to data file at the right place.
 * If success, return 0. Otherwise, return non-zero value.
 */
int db_insert(int64_t key, char * value) {
    page_t tmp_page;
    pagenum_t root, leaf, new_root;

    tmp_page.style = PAGE_HEADER;
    file_read_page(0, &tmp_page);

    root = tmp_page.header_page.root_pagenum;

    // No duplicates.
    if (db_find(key, NULL) == 0)
        return 1;
    
    /* Case: the tree doesn't exist yet.
     * Start new tree with given record.
     */
    if (root == 0) {
        new_root = _start_new_tree(key, value);
        file_read_page(0, &tmp_page);
        tmp_page.header_page.root_pagenum = new_root;
        file_write_page(0, &tmp_page);
        return 0;
    }
    // Tree already exists.

    leaf = _find_leaf(root, key, 0);

    // Read leaf page to tmp_page
    tmp_page.style = PAGE_LEAF;
    file_read_page(leaf, &tmp_page);

    /* Case: leaf has some space to store a new record.
     */

    if (tmp_page.leaf_page.num_of_keys < ORDER_OF_LEAF - 1) {
        _insert_into_leaf(leaf, key, value);
        return 0;
    }

    /* Case: leaf must be split.
     */

    new_root = _insert_into_leaf_after_split(root, leaf, key, value);
    if (root != new_root) {
        tmp_page.style = PAGE_HEADER;
        file_read_page(0, &tmp_page);
        tmp_page.header_page.root_pagenum = new_root;
        file_write_page(0, &tmp_page);
    }
    return 0;
}


/* Find the record containing input ‘key’.
 * If found matching ‘key’, store matched ‘value’ string in ret_val and return 0.
 * Otherwise, return non-zero value.
 * Memory allocation for record structure(ret_val) should occur in caller function.
 * If ret_val is NULL, just find whether there is the key or not wi
 */
int db_find(int64_t key, char * ret_val) {
    int i;
    pagenum_t leaf;
    page_t tmp_page;
    tmp_page.style = PAGE_HEADER;
    file_read_page(0, &tmp_page);

    leaf = _find_leaf(tmp_page.header_page.root_pagenum, key, 0);
    if (leaf == 0) return 1;

    tmp_page.style = PAGE_LEAF;
    file_read_page(leaf, &tmp_page);
    for (i = 0; i < tmp_page.leaf_page.num_of_keys; ++i) {
        if (tmp_page.leaf_page.records[i].key == key) break;
    }
    if (i == tmp_page.leaf_page.num_of_keys) {
        return 1;
    } else {
        if(ret_val != NULL)
            strcpy(ret_val, tmp_page.leaf_page.records[i].value);
        return 0;
    }
}


/* Find the matching record and delete it if found.
 * If success, return 0. Otherwise, return non-zero value.
 */
int db_delete(int64_t key);


/* Close the opened data file.
 * If success, return 0. Otherwise, return -1.
 */
int close_table(void) {
    return file_close_file();
}
