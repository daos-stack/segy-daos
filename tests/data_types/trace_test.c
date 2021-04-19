/*
 * trace_test.c
 *
 *  Created on: Jan 24, 2021
 *      Author: omar
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include "data_types/trace.h"

static void
test_trace_get(void **state)
{
	trace_t tr;
	tr.fldr = 400;
	tr.d1 = 5.2;
	generic_value v;
	trace_get_header(tr, "fldr", &v);
	assert_int_equal(v.i, 400);
	trace_get_header(tr, "d1", &v);
	assert_true(abs(v.f - 5.2) <= 0.01);
	trace_get_header(tr, "cdp", &v);
	assert_int_equal(v.i, 0);
}

static void
test_trace_set(void **state)
{
	trace_t tr;
	generic_value v, vtest;
	v.i = 400;
	trace_set_header(&tr, "fldr", &v);
	trace_get_header(tr, "fldr", &vtest);
	assert_int_equal(vtest.i, 400);
	v.f = 5.2;
	trace_set_header(&tr, "d1", &v);
	trace_get_header(tr, "d1", &vtest);
	assert_true(abs(vtest.f - 5.2) <= 0.01);
}

static void
test_trace_linked_list(void **state)
{
	doubly_linked_list_t *ll = doubly_linked_list_init();
	trace_t *tr1 = malloc(sizeof(trace_t));
	trace_t *tr2 = malloc(sizeof(trace_t));
	trace_t *tr3 = malloc(sizeof(trace_t));
	tr1->fldr = 1;
	tr2->fldr = 2;
	tr3->fldr = 3;
	tr2->d2 = 4.9;
	tr1->data = NULL;
	tr2->data = NULL;
	tr3->data = NULL;

	doubly_linked_list_add_node(ll, &(tr1->n));
	doubly_linked_list_add_node(ll, &(tr2->n));
	doubly_linked_list_add_node(ll, &(tr3->n));
	generic_value v;
	v.i = 2;
	trace_t *test = trace_search(ll, "fldr", v);
	assert_int_equal(test->fldr, 2);
	trace_get_header(*test, "d2", &v);
	assert_true(abs(v.f - 4.9) <= 0.01);

	v.i = 5;
	test = trace_search(ll, "fldr", v);
	assert_null(test);

	int rc = trace_delete(ll, "fldr", v);
	assert_int_equal(rc, -1);

	v.i = 2;
	rc = trace_delete(ll, "fldr", v);
	assert_int_equal(rc, 0);
	assert_int_equal(ll->size, 2);

}

static void
test_trace_init_destroy(void **state)
{
	trace_t *tr;
	int rc = trace_init(&tr);
	assert_int_equal(rc, 0);
	rc = trace_destroy(tr);
	assert_int_equal(rc, 0);
}

static void test_header_calculate_set(void** state){
	trace_t* tr;
	trace_init(&tr);
	calculate_header_set(tr, "cdp", 0, 5, 5.5, 0, 10, 20);
	generic_value test;
	trace_get_header(*tr, "cdp", &test);
	assert_int_equal(test.i, 60);
	free(tr);

}

static void test_header_calculate_change(void** state){
	generic_value val1;
	val1.i = 50;
	generic_value val2;
	val2.f = 10.0;
	generic_value val3;
	val3.f = 25.0;
	trace_t* tr;
	trace_init(&tr);
	generic_value test;
	trace_set_header(tr, "cdp", &val1);
	trace_set_header(tr, "d1", &val2);
	trace_set_header(tr, "d2", &val3);
	calculate_header_change(tr,"cdp","d1","d2",5.5, 10.5, 6.5, 10.5, 2.5, 0);
	trace_get_header(*tr, "cdp", &test);
	assert_int_equal(test.i, 317);
	free(tr);

}

int
main(int argc, char *argv[])
{
	const struct CMUnitTest tests[] = {
			cmocka_unit_test(test_trace_get),
			cmocka_unit_test(test_trace_set),
			cmocka_unit_test(test_trace_linked_list),
			cmocka_unit_test(test_trace_init_destroy),
			cmocka_unit_test(test_header_calculate_set),
			cmocka_unit_test(test_header_calculate_change),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
