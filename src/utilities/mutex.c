/*
 * mutex.c
 *
 *  Created on: Jan 20, 2021
 *      Author: mirnamoawad
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "utilities/mutex.h"

int
mutex_lock_init(mpi_mutex_t **mutex, int home)
{
	static int 	tag = MPI_MUTEX_MSG_TAG_BASE;
	int		numprocs;
	int		id;

	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &id);

	(*mutex) = (mpi_mutex_t *)malloc(sizeof(mpi_mutex_t));

	if ((*mutex) == NULL) {
		char message[50];
		sprintf(message,"Warning: malloc failed on worker %2d\n", id);
		DSG_ERROR(1,message);
		return 1;
	}

	(*mutex)->numprocs = numprocs;
	(*mutex)->ID = id;
	(*mutex)->home = home;
	(*mutex)->tag = tag++;
	(*mutex)->comm = MPI_COMM_WORLD;

	if (id == (*mutex)->home) {
		/** Allocate and expose waitlist */
		MPI_Alloc_mem((*mutex)->numprocs, MPI_INFO_NULL,
			      &(*mutex)->waitlist);
		if ((*mutex)->waitlist == NULL) {
			char message[50];
			sprintf(message,"Warning: MPI_Alloc_mem failed"
					" on worker %2d\n", id);
			DSG_ERROR(1,message);
			return 1;
		}
		memset((*mutex)->waitlist, 0, (*mutex)->numprocs);
		MPI_Win_create((*mutex)->waitlist, (*mutex)->numprocs, 1,
			       MPI_INFO_NULL, (*mutex)->comm, &(*mutex)->win);
	} else {
		/** Don't expose anything */
		(*mutex)->waitlist = NULL;
		MPI_Win_create((*mutex)->waitlist, 0, 1, MPI_INFO_NULL,
			       (*mutex)->comm, &(*mutex)->win);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	return 0;
}

int
mutex_lock_destroy(mpi_mutex_t *mutex)
{
	int 		id;

	if(mutex == NULL) {
		DSG_ERROR(1,"Mutex uninitialized");
		return 1;
	}

	MPI_Barrier(MPI_COMM_WORLD);

	MPI_Comm_rank(MPI_COMM_WORLD, &id);

	if (id == mutex->home) {
		/** Free window & waitlist allocated */
		MPI_Win_free(&mutex->win);
		MPI_Free_mem(mutex->waitlist);
	} else {
		/** Free only window allocated */
		MPI_Win_free(&mutex->win);
		mutex->waitlist = NULL;
	}

	return 0;
}

int
mutex_lock_acquire(mpi_mutex_t *mutex)
{
	assert(mutex != NULL);

	unsigned char 		waitlist[mutex->numprocs];
	unsigned char 		lock = 1;
	int 			i;

	/** set lock to one through updating the waitlist[index == current ID]
	 *  then read all the waitlist to wait on the lock.
	 */
	MPI_Win_lock(MPI_LOCK_EXCLUSIVE, mutex->home, 0, mutex->win);
	MPI_Put(&lock, 1, MPI_CHAR, mutex->home, mutex->ID/* &win[mutex->ID] */,
		1, MPI_CHAR, mutex->win);
	MPI_Get(waitlist, mutex->numprocs, MPI_CHAR, mutex->home, 0,
		mutex->numprocs, MPI_CHAR, mutex->win);
	MPI_Win_unlock(mutex->home, mutex->win);

	assert(waitlist[mutex->ID] == 1);

	for (i = 0; i < mutex->numprocs; i++) {
		/** if any process has the lock or want to acquire the lock
		 *  then we must wait until a dummy message is received,
		 *  which means that process acquired the lock now successfully.
		 *  otherwise pass the loop and congratulations
		 *  you have the lock now, nobody wants it...
		 */
		if (waitlist[i] == 1 && i != mutex->ID) {
			/** We have to wait for the lock */
			MPI_Recv(&lock, 0, MPI_CHAR, MPI_ANY_SOURCE,
				 mutex->tag, mutex->comm, MPI_STATUS_IGNORE);
			break;
		}
	}

	return 0;
}

int
mutex_lock_release(mpi_mutex_t *mutex)
{
	assert(mutex != NULL);

	unsigned char 		waitlist[mutex->numprocs];
	unsigned char 		lock = 0;
	int 			i;
	int			next;


	/** set your index in the waiting list to zero
	 *  then read the list to find which process wants the lock
	 *  to send it a dummy message.
	 */
	MPI_Win_lock(MPI_LOCK_EXCLUSIVE, mutex->home, 0, mutex->win);
	MPI_Put(&lock, 1, MPI_CHAR, mutex->home, mutex->ID/* &win[mutex->ID]*/,
		1, MPI_CHAR, mutex->win);
	MPI_Get(waitlist, mutex->numprocs, MPI_CHAR, mutex->home, 0,
		mutex->numprocs, MPI_CHAR, mutex->win);
	MPI_Win_unlock(mutex->home, mutex->win);

	/** Start from the next process id following yours
	 *  and if there are other processes waiting for the lock,
	 *  transfer ownership
	 */
        next = (mutex->ID + 1 + mutex->numprocs) % mutex->numprocs;
	for (i = 0; i < mutex->numprocs; i++, next = (next + 1) % mutex->numprocs){
		if (waitlist[next] == 1) {
			MPI_Send(&lock, 0, MPI_CHAR, next, mutex->tag,
				 mutex->comm);
			break;
		}
	}

	return 0;
}




