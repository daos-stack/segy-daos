/*
 * dsg_timer_test.c
 *
 *  Created on: Jan 18, 2021
 *      Author: mirnamoawad
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "utilities/timer.h"

void
test_timer_create(void **state)
{
	/** Create multiple valid timers. */
	int timer_instances = 0;
	dsg_timer_t *t1;
	dsg_timer_t *t2;
	int rc;
	timer_instances += create_timer(&t1);
	timer_instances += create_timer(&t2);
	/** make sure timer instances equal zero
	 * and that both timers created successfully */
	assert_int_equal(timer_instances, 0);
	assert_int_equal(t1->elapsed_time, 0);
	assert_int_equal(t2->elapsed_time, 0);
	rc = destroy_timer(t1);
	assert_int_equal(rc, 0);
	rc = destroy_timer(t2);
	assert_int_equal(rc, 0);

}

void
test_timer_destroy(void **state)
{
	/** Correctly destroy a timer. */
	dsg_timer_t *t;
	int rc = create_timer(&t);
	assert_int_equal(rc, 0);
	rc = destroy_timer(t);
	assert_int_equal(rc, 0);
}

void
test_timer_start(void **state)
{
	/** Correctly start a timer. */
	dsg_timer_t *t;
	int rc = create_timer(&t);
	assert_int_equal(rc, 0);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	rc = end_timer(t,1);
	assert_int_equal(rc, 0);
	rc = destroy_timer(t);
	assert_int_equal(rc, 0);
	/** Correctly start a timer after the timer has run a while. */
	rc = create_timer(&t);
	assert_int_equal(rc, 0);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	sleep(3);
	rc = end_timer(t, 0);
	assert_int_equal(rc, 0);
	/** check elapsed time is almost 3 msec*/
	assert_int_equal(t->elapsed_time,3);
	rc = reset_timer(t, NULL);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	assert_int_equal(t->elapsed_time,0);
	rc = destroy_timer(t);
	assert_int_equal(rc, 0);

}

void
test_timer_reset(void **state)
{
	dsg_timer_t *t;
	/** try to reset timer after starting it directly */
	int rc = create_timer(&t);
	rc = start_timer(t);
	rc = reset_timer(t, NULL);
	assert_int_equal(rc, -1);
	/** Verify that a timer can be reset even if it has never been started. */
	rc = create_timer(&t);
	assert_int_equal(rc, 0);
	rc = reset_timer(t, NULL);
	assert_int_equal(rc, 0);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	sleep(2);
	rc = end_timer(t, 0);
	assert_int_equal(rc, 0);
	/** check elapsed time is almost 2 msec*/
	assert_int_equal(t->elapsed_time,2);
	rc = destroy_timer(t);
	assert_int_equal(rc, 0);

}

void
test_timer_end(void **state)
{
	/** Verify that a timer can be stopped without having been started. */
	dsg_timer_t *t;
	int rc = create_timer(&t);
	assert_int_equal(rc, 0);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	rc = end_timer(t, 1);
	assert_int_equal(rc, 0);
	sleep(2);
	rc = end_timer(t, 0);
	assert_int_equal(rc, -1);
	assert_int_equal(t->elapsed_time,0);
	/** Verify that a timer can be successfully stopped. */
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	sleep(2);
	rc = end_timer(t, 0);
	assert_int_equal(rc, 0);
	assert_int_equal(t->elapsed_time,2);
	rc = reset_timer(t, NULL);
	assert_int_equal(rc, 0);
	/** Verify that a timer can be successfully stopped. */
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	sleep(2);
	rc = end_timer(t, 1);
	assert_int_equal(rc, 0);
	sleep(2);
	assert_int_equal(t->elapsed_time,0);

	rc = destroy_timer(t);
	assert_int_equal(rc, 0);
}

void
test_timer_restart(void **state)
{
	/** Verify that a timer can be successfully restarted */
	dsg_timer_t *t;
	int rc = create_timer(&t);
	assert_int_equal(rc, 0);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	sleep(3);
	rc = end_timer(t, 0);
	assert_int_equal(rc, 0);
	assert_int_equal(t->elapsed_time,3);
	sleep(3);
	rc = start_timer(t);
	assert_int_equal(t->running, 1);
	assert_int_equal(t->elapsed_time,3);
	sleep(3);
	rc = end_timer(t, 0);
	assert_int_equal(rc, 0);
	assert_int_equal(t->elapsed_time,6);
	rc = destroy_timer(t);
	assert_int_equal(rc, 0);

}

int main(int argc, char* argv[])
{
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_timer_create),
    cmocka_unit_test(test_timer_destroy),
    cmocka_unit_test(test_timer_start),
    cmocka_unit_test(test_timer_reset),
    cmocka_unit_test(test_timer_end),
    cmocka_unit_test(test_timer_restart),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
