#include "lock_manager.h"

// GLOBALS.

static int g_usable_tid = 1;

trx_t *trx_table[TRX_TABLE_SIZE];


// FUNCTIONS.

int begin_trx() {
    int tid = g_usable_tid;
    ++g_usable_tid;

    trx_t *new_trx = new trx_t;
    if (new_trx == NULL) {
        return 0;
    }
    new_trx->status = trx_status_t::RUNNING;
    new_trx->tid = tid;
    new_trx->locks = NULL;
    new_trx->num_of_locks = 0;
    new_trx->waiting_for = NULL;
    new_trx->next_trx = trx_table[tid % TRX_TABLE_SIZE];

    trx_table[tid % TRX_TABLE_SIZE] = new_trx;

    return tid;
}

int end_trx(int tid) {
    trx_t *curr_trx = trx_table[tid % TRX_TABLE_SIZE];
    lock_t *curr_lock = NULL;

    if (curr_trx == NULL) {
        return 0;
    }

    if (curr_trx->tid == tid) {
        trx_table[tid % TRX_TABLE_SIZE] = curr_trx->next_trx;
    } else {

        while (curr_trx->next_trx && curr_trx->next_trx->tid != tid) {
            curr_trx = curr_trx->next_trx;
        }
        
        if (curr_trx->next_trx == NULL) {
            return 0;
        }
        
        curr_trx->next_trx = curr_trx->next_trx->next_trx;

        curr_trx = curr_trx->next_trx;
    }

    int i;
    for (i = 0; i < curr_trx->num_of_locks; ++i) {
        curr_lock = curr_trx->locks[i];
        delete curr_lock; // Just deallocate yet, because lock_t and locking function are not implemented.
    }
    if (curr_trx->locks)
        delete curr_trx->locks;
    
    delete curr_trx;

    return tid;
}
