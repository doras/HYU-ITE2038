#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include <list>
#include <new>
#include <pthread.h>
#include <stack>
#include <vector>

#include "disk_based_bpt.hpp"

#define LOCK_HASH_TABLE_SIZE 128

#define LOCK_SUCCESS 0
#define LOCK_CONFLICT 1
#define LOCK_DEADLOCK -1

// TYPES.

enum class trx_status_t {
    RUNNING,
    WAITING
};

enum class lock_mode_t {
    SHARED,
    EXCLUSIVE
};

class undo_log_t {
public:
    int table_id;
    pagenum_t page_number;
    int record_index;
    char old_record[120];
};

class trx_t;

class lock_t {
public:
    int table_id;
    pagenum_t page_number;
    int record_index;
    lock_mode_t mode;
    bool acquired;
    trx_t *trx;
    lock_t *hash_prev;
    lock_t *hash_next;
    lock_t *same_record_prev;
    lock_t *same_record_next;

    lock_t(int table_id, pagenum_t page_number
        , int record_index, lock_mode_t mode, trx_t *trx);
    ~lock_t() = default;
};

class trx_t {
public:
    int tid;
    trx_status_t status;
    std::list<lock_t*> trx_locks;
    lock_t *waiting_for;
    pthread_mutex_t trx_mutex;
    pthread_cond_t trx_cond;
    std::stack<undo_log_t> undo_logs;

    trx_t();
    ~trx_t();
};

class trx_system_t {
public:
    static pthread_mutex_t latch;
    static std::vector<trx_t*> table;
    static int next_tid;
};

struct lock_hash_table_element_t {
    lock_t *head;
    lock_t *tail;
};

class lock_hash_table_t {
public:
    static pthread_mutex_t table_latch;
    static lock_hash_table_element_t table[LOCK_HASH_TABLE_SIZE];
    static int hashing(pagenum_t page_number);
};


// FUNCTIONS.

int begin_trx();
int end_trx(int tid);
int deadlock_detection(trx_t *target_trx, trx_t *waited_trx);
int acquire_lock(int table_id, pagenum_t page_number, int record_index, lock_mode_t mode, trx_t *trx);
void undo_trx(trx_t *trx);
void release_locks(trx_t *trx);

#endif
