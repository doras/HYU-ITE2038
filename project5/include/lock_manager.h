#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include "disk_based_bpt.h"

#define TRX_TABLE_SIZE 10

// TYPES.

typedef enum {
    RUNNING,
    WAITING
} trx_status_t;

typedef struct _lock_t lock_t;

typedef struct _trx_t{
    int tid;
    trx_status_t status;
    lock_t **locks;
    int num_of_locks;
    lock_t *waiting_for;
    struct _trx_t *next_trx; // for trx table. (linked list)
} trx_t;

// GLOBALS.

extern trx_t *trx_table[TRX_TABLE_SIZE];


// FUNCTIONS.

int begin_trx();
int end_trx(int tid);

#endif
