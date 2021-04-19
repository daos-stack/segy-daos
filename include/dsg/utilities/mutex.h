/*
 * mutex.h
 *
 *  Created on: Jan 20, 2021
 *      Author: mirnamoawad
 */
#ifndef INCLUDE_DSG_UTILITIES_MUTEX_H_
#define INCLUDE_DSG_UTILITIES_MUTEX_H_

#include <mpi.h>
#include "error_handler.h"

#define MPI_MUTEX_MSG_TAG_BASE 1023

typedef struct MPI_Mutex {
	int numprocs;
	int ID;
	int home;
	int tag;
	MPI_Comm comm;
	MPI_Win win;
	unsigned char *waitlist;
}mpi_mutex_t;

/** Function responsible for initializing mutex lock through utilizing
 *  MPI window objects.
 *  All processes must call mutex_lock_init function to be able to use locks.
 *
 * \param[in]   mutex      address of mutex object to be allocated
 * 			   and initalized.
 * \param[in]	home	   id of process that will create the waitlist
 * 			   and the window object.
 *
 * \return      0 on success
 * 		error_code otherwise
 *
 */
int
mutex_lock_init(mpi_mutex_t **mutex, int home);

/** Function responsible for destroying mutex lock and the previously created
 *  MPI window objects.
 *  All processes must call mutex_lock_destroy function.
 *
 * \param[in]   mutex      pointer to mutex object to be destroyed.
 *
 * \return      0 on success
 * 		error_code otherwise
 *
 */
int
mutex_lock_destroy(mpi_mutex_t *mutex);

/** Function responsible for acquiring the mutex lock through setting
 *  process index in the waitlist to (1) and then wait if any of the
 *  processes has the lock.
 *
 * \param[in]   mutex      pointer to mutex object.
 *
 * \return      0 on success
 * 		error_code otherwise
 *
 */
int
mutex_lock_acquire(mpi_mutex_t *mutex);

/** Function responsible for releasing the mutex lock through setting
 *  process index in the waitlist to (0) and then send a dummy message
 *  to any process setting
 *  its index to (1) so that it can acquire the lock.
 *
 * \param[in]   mutex      pointer to mutex object.
 *
 * \return      0 on success
 * 		error_code otherwise
 *
 */
int
mutex_lock_release(mpi_mutex_t *mutex);

#endif /* INCLUDE_DSG_UTILITIES_MUTEX_H_ */
