/*
 * mutex_test.c
 *
 *  Created on: Jan 20, 2021
 *      Author: mirnamoawad
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>
#include "utilities/mutex.h"
#define MASTER_ID 0

static void lock_function(int *buf, MPI_Win *win)
{

	MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER_ID, 0, *win);
	MPI_Get(buf, 1, MPI_INT, MASTER_ID, 0, 1, MPI_INT, *win);
	MPI_Win_unlock(MASTER_ID, *win);
	(*buf)++;
	MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER_ID, 0, *win);
	MPI_Put(buf, 1, MPI_INT, MASTER_ID, 0, 1, MPI_INT, *win);
	MPI_Win_unlock(MASTER_ID, *win);
}

void
test_mutex(void **state)
{
	mpi_mutex_t 	*mutex;
	int 		 rc;
	int		 *counter;
	int		 buf = 0;
	int		 process_rank;
	MPI_Win 	 win;

	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
	int	num_of_processes;
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);

	rc = mutex_lock_init(&mutex, 0);
	assert_int_equal(rc, 0);
	MPI_Barrier(MPI_COMM_WORLD);

	if(process_rank == 0) {
		MPI_Alloc_mem(sizeof(int), MPI_INFO_NULL, &counter);
		*counter = 0;
		MPI_Win_create(counter, sizeof(int), 1, MPI_INFO_NULL,
			       MPI_COMM_WORLD, &win);
	} else {
		MPI_Win_create(counter, 0, 1, MPI_INFO_NULL, MPI_COMM_WORLD,
			       &win);
	}

	rc = mutex_lock_acquire(mutex);
	assert_int_equal(rc, 0);

	lock_function(&buf, &win);

	rc = mutex_lock_release(mutex);
	assert_int_equal(rc, 0);

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Win_lock(MPI_LOCK_SHARED, MASTER_ID, 0, win);
	MPI_Get(&buf, 1, MPI_INT, MASTER_ID, 0, 1, MPI_INT, win);
	MPI_Win_unlock(MASTER_ID, win);

	MPI_Win_free(&win);
	if(process_rank == 0) {
		MPI_Free_mem(counter);
	}

	rc = mutex_lock_destroy(mutex);
	assert_int_equal(rc, 0);
	assert_int_equal(buf, num_of_processes);
}

void
test_mutex_init(void **state)
{
	mpi_mutex_t 	*mutex;
	int		rc;
	/** Make sure locks can be initialized and destroyed smoothly */
	rc = mutex_lock_init(&mutex, 0);
	assert_int_equal(rc, 0);
	MPI_Barrier(MPI_COMM_WORLD);
	rc = mutex_lock_destroy(mutex);
	assert_int_equal(rc, 0);
}

/** Make sure locks can be initialized, acquired, released
 *  and destroyed smoothly
 */
void
test_mutex_acquire_lock(void **state)
{
	mpi_mutex_t 	*mutex;
	int		rc;
	rc = mutex_lock_init(&mutex, 0);
	assert_int_equal(rc, 0);

	MPI_Barrier(MPI_COMM_WORLD);
	/** acquire lock */
	rc = mutex_lock_acquire(mutex);
	assert_int_equal(rc, 0);
	/** release lock */
	rc = mutex_lock_release(mutex);
	assert_int_equal(rc, 0);

	MPI_Barrier(MPI_COMM_WORLD);

	rc = mutex_lock_destroy(mutex);
	assert_int_equal(rc, 0);
}

int main(int argc, char* argv[])
{
	int	rc;
	rc = MPI_Init(&argc, &argv);
	int	num_of_processes;
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);

  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_mutex),
      cmocka_unit_test(test_mutex_init),
      cmocka_unit_test(test_mutex_acquire_lock),
  };
  rc = cmocka_run_group_tests(tests, NULL, NULL);
	MPI_Barrier(MPI_COMM_WORLD);

  MPI_Finalize();
  return rc;
}
