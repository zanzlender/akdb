/**
@file transaction.h Header file that defines includes and datastructures for the transaction execution
 */
/*
 *
 * transaction.h is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * transaction.h is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TRANSACTION_H_
#define TRANSACTION_H_
#include <pthread.h> //TRANSACTIONS
#include "configuration.h"

/**
 * @author Frane Jakelic
 * @struct transaction_locks_list_elem
 * @brief Structure that represents LockTable entry about transaction resource lock.
 */
struct transaction_locks_list_elem{
    int TransactionId;
    int lock_type;
    int isWaiting;
    struct transaction_locks_list_elem *nextLock;
    struct transaction_locks_list_elem *prevLock;
};

/**
 * @author Frane Jakelic
 * @struct transaction_list_elem
 * @brief Structure that represents LockTable entry about transaction lock holder.Element indexed by Hash table.
 */
struct transaction_list_elem{
    int id;
    int lock_type;
    int isWaiting;
    struct transaction_locks_list_elem *DLLLocksHead;
    struct transaction_list_elem *nextBucket;
    struct transaction_list_elem *prevBucket;
};

/**
 * @author Frane Jakelic
 * @struct transaction_list_head
 * @brief Structure that represents LockTable entry about doubly linked list of collision in Hash table.
 */
struct transaction_list_head{
    struct transaction_list_elem *DLLHead;
};



typedef struct transaction_list_head AK_transaction_list;
typedef struct transaction_list_elem *AK_transaction_elem;
typedef struct transaction_locks_list_elem *AK_transaction_lock_elem;

#endif /* TRANSACTION_H_ */


