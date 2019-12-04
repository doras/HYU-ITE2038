#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include "disk_based_bpt.h"

#define TRX_TABLE_SIZE 10

// TYPES.

enum class trx_status_t{
    RUNNING,
    WAITING
};

class lock_t {
//TODO
};

class trx_t{
public:
    int tid;
    trx_status_t status;
    lock_t **locks;
    int num_of_locks;
    lock_t *waiting_for;
    trx_t *next_trx; // for trx table. (linked list)
};

// GLOBALS.

extern trx_t *trx_table[TRX_TABLE_SIZE];


// FUNCTIONS.

int begin_trx();
int end_trx(int tid);

#endif
