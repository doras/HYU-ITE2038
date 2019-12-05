#include "lock_manager.hpp"

// STATIC VARIABLES.

std::vector<trx_t*> trx_system_t::table = std::vector<trx_t*>();
pthread_mutex_t trx_system_t::latch = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock_hash_table_t::table_latch = PTHREAD_MUTEX_INITIALIZER;


// MEMBER FUNCTIONS.

lock_t::lock_t(int table_id, pagenum_t page_number
            , int record_index, lock_mode_t mode, trx_t *trx) 
        : table_id(table_id), page_number(page_number)
            , record_index(record_index), mode(mode), acquired(false)
            , trx(trx), hash_prev(nullptr), hash_next(nullptr)
            , same_record_prev(nullptr), same_record_next(nullptr) {
    // Do nothing.
}


trx_t::trx_t() : status(trx_status_t::RUNNING), trx_locks()
        , waiting_for(nullptr)
        , trx_mutex(PTHREAD_MUTEX_INITIALIZER)
        , trx_cond(PTHREAD_COND_INITIALIZER), undo_logs() {

    // Do nothing.
}


/**
 * Hash given page number.
 * \return hashed value about \p page_number .
 */
int lock_hash_table_t::hashing(pagenum_t page_number) {
    return page_number % LOCK_HASH_TABLE_SIZE;
}


trx_t::~trx_t() {
    // TODO
}


// FUNCTIONS.

/**
 * 
 */
int begin_trx() {

    trx_t *new_trx = new trx_t();

    pthread_mutex_lock(&trx_system_t::latch);

    new_trx->tid = trx_system_t::next_tid;
    ++trx_system_t::next_tid;

    trx_system_t::table.push_back(new_trx);

    pthread_mutex_unlock(&trx_system_t::latch);

    return new_trx->tid;
}

/**
 * 
 */
int deadlock_detection(trx_t *target_trx, trx_t *waited_trx) {

    trx_t *curr_trx = waited_trx;
    lock_t *curr_lock;
    while (curr_trx->status == trx_status_t::WAITING && curr_trx != target_trx) {
        curr_lock = curr_trx->waiting_for;
        curr_trx = curr_lock->trx;
    }

    if (curr_trx == target_trx) {
        return LOCK_DEADLOCK;
    } else {
        return LOCK_SUCCESS;
    }
}


/**
 * 
 */
int acquire_lock(int table_id, pagenum_t page_number
        , int record_index, lock_mode_t mode, trx_t *trx) {

    int hashed_idx = lock_hash_table_t::hashing(page_number);
    lock_t *new_lock = nullptr;
    
    pthread_mutex_lock(&lock_hash_table_t::table_latch);

    lock_t *curr_lock_node = lock_hash_table_t::table[hashed_idx].head;

    // Find first lock about same record.
    while (curr_lock_node && (record_index != curr_lock_node->record_index 
            || table_id != curr_lock_node->table_id 
            || page_number != curr_lock_node->page_number)) {
        curr_lock_node = curr_lock_node->hash_next;
    }

    // case : There is no lock about the record.
    // Just lock.
    if (curr_lock_node == nullptr) {
        new_lock = new lock_t(table_id, page_number, record_index, mode, trx);
        new_lock->acquired = true;
        new_lock->hash_prev = lock_hash_table_t::table[hashed_idx].tail;
        lock_hash_table_t::table[hashed_idx].tail->hash_next = new_lock;
        lock_hash_table_t::table[hashed_idx].tail = new_lock;

        trx->trx_locks.push_back(new_lock);

        pthread_mutex_unlock(&lock_hash_table_t::table_latch);

        return LOCK_SUCCESS;
    }

    lock_t *head_of_the_record = curr_lock_node;
    
    // traverse the list and check whether given trx already acquired lock for the record.
    bool lock_upgrade = false;
    lock_t *tail_of_the_record = nullptr;
    while (true) {
        if (curr_lock_node->trx == trx) {
            /* If lock target record is same and current lock is by given trx. 
               It means given trx already acquired lock for the record. */
            
            if (mode == lock_mode_t::SHARED 
                    || curr_lock_node->mode == lock_mode_t::EXCLUSIVE) {
                pthread_mutex_unlock(&lock_hash_table_t::table_latch);
                return LOCK_SUCCESS;
            }

            // case : need to lock mode upgrade
            lock_upgrade = true;
            break; // set flag and break the loop.
        }

        if (curr_lock_node->same_record_next) {
            curr_lock_node = curr_lock_node->same_record_next;
            continue;
        } else {
            tail_of_the_record = curr_lock_node; // Store the tail of same record list.
            break;
        }
    }

    if (lock_upgrade) {
        
        // Find the tail lock node of same record list.
        while (curr_lock_node->same_record_next) {
            curr_lock_node = curr_lock_node->same_record_next;
        }

        tail_of_the_record = curr_lock_node;

        // case : Lock waiting for given trx exists. So, deadlock.
        if (!curr_lock_node->acquired) {
            pthread_mutex_unlock(&lock_hash_table_t::table_latch);
            return LOCK_DEADLOCK;
        }

        while (curr_lock_node) {
            // case : There is another S mode lock. Need to wait for it.
            if (curr_lock_node->trx != trx) {
                
                new_lock = new lock_t(table_id, page_number, record_index, mode, trx);
                trx->waiting_for = curr_lock_node;
                new_lock->hash_prev = lock_hash_table_t::table[hashed_idx].tail;
                lock_hash_table_t::table[hashed_idx].tail->hash_next = new_lock;
                lock_hash_table_t::table[hashed_idx].tail = new_lock;
                new_lock->same_record_prev = tail_of_the_record;
                tail_of_the_record->same_record_next = new_lock;

                trx->trx_locks.push_back(new_lock);
                trx->status = trx_status_t::WAITING;

                pthread_mutex_unlock(&lock_hash_table_t::table_latch);

                return LOCK_CONFLICT;
            }
            curr_lock_node = curr_lock_node->same_record_prev;
        }

        // case : can upgrade lock immediately.
        tail_of_the_record->mode = mode;

        pthread_mutex_unlock(&lock_hash_table_t::table_latch);

        return LOCK_SUCCESS;
    }


    // case : there is only locks by other trx. Check last lock's modes.
    if (mode == lock_mode_t::SHARED && tail_of_the_record->mode == lock_mode_t::SHARED) {
        if (tail_of_the_record->acquired) {

            new_lock = new lock_t(table_id, page_number, record_index, mode, trx);
            new_lock->hash_prev = lock_hash_table_t::table[hashed_idx].tail;
            lock_hash_table_t::table[hashed_idx].tail->hash_next = new_lock;
            lock_hash_table_t::table[hashed_idx].tail = new_lock;
            new_lock->same_record_prev = tail_of_the_record;
            tail_of_the_record->same_record_next = new_lock;
            new_lock->acquired = true;

            trx->trx_locks.push_back(new_lock);

            pthread_mutex_unlock(&lock_hash_table_t::table_latch);
            return LOCK_SUCCESS;
        } else {
            if (deadlock_detection(trx, tail_of_the_record->trx->waiting_for->trx) == LOCK_DEADLOCK) {
                pthread_mutex_unlock(&lock_hash_table_t::table_latch);
                return LOCK_DEADLOCK;
            }
            trx->waiting_for = tail_of_the_record->trx->waiting_for;
        }
    } else {
        if (deadlock_detection(trx, tail_of_the_record->trx) == LOCK_DEADLOCK) {
            pthread_mutex_unlock(&lock_hash_table_t::table_latch);
            return LOCK_DEADLOCK;
        }
        trx->waiting_for = tail_of_the_record;
    }

    new_lock = new lock_t(table_id, page_number, record_index, mode, trx);
    new_lock->hash_prev = lock_hash_table_t::table[hashed_idx].tail;
    lock_hash_table_t::table[hashed_idx].tail->hash_next = new_lock;
    lock_hash_table_t::table[hashed_idx].tail = new_lock;
    new_lock->same_record_prev = tail_of_the_record;
    tail_of_the_record->same_record_next = new_lock;

    trx->trx_locks.push_back(new_lock);
    trx->status = trx_status_t::WAITING;

    pthread_mutex_unlock(&lock_hash_table_t::table_latch);

    return LOCK_CONFLICT;
}

int check_conflict(trx_t *trx) {

}

/**
 * 
 */
void undo_trx(trx_t *trx) {
    buffer_t *temp_page;
    undo_log_t log;
    while (!trx->undo_logs.empty()) {
        log = trx->undo_logs.top();
        trx->undo_logs.pop();

        temp_page = buf_get_page(log.table_id, log.page_number);
        strncpy(temp_page->frame.leaf_page.records[log.record_index].value, log.old_record, 120);
        buf_put_page(temp_page, 1);
    }
}


/**
 * 
 */
// void release_locks(trx_t *trx) {
//     lock_t *temp_lock;
//     while (!trx->trx_locks.empty()) {
//         temp_lock = trx->trx_locks.front();
//         trx->trx_locks.pop_front();

//         pthread_mutex_lock(&lock_hash_table_t::table_latch);
//         lok
//     }
// }



// int end_trx(int tid) {
//     //TODO
// }
