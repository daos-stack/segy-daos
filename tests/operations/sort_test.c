/*
 * sort_test.c
 *
 *  Created on: Jan 26, 2021
 *      Author: omar
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>

#include "operations/sort.h"
#include "data_types/trace.h"

void
trace_generator(trace_t *tr, int range)
{
	int size = 0, i = 1, j;
	while (size < range) {
		j = 0;
		while (j < i) {
			trace_t *temp;
			trace_init(&temp);
			temp->fldr = i;
			temp->cdp = size;
			tr[size++] = *temp;
			if (size >= range)
				break;
			j++;
		}
		i++;
	}
}

static void
test_sort(void **state)
{
	int arr[10] = { 1, 2, 3, 4, 5, 6, 6, 8, 9, 10 };
	int direction = -1;
	int rc = sort(arr, 10, sizeof(int), &direction, &int_compare);
	assert_int_equal(rc, 0);
	for (int i = 0; i < 9; i++) {
		assert_true(arr[i] >= arr[i + 1]);
	}
	direction = 1;
	rc = sort(arr, 10, sizeof(int), &direction, &int_compare);
	assert_int_equal(rc, 0);
	for (int i = 0; i < 9; i++) {
		assert_true(arr[i] <= arr[i + 1]);
	}

}

static void
test_sort_trace(void **state)
{
	trace_t *tr = malloc(10 * sizeof(trace_t));
	trace_generator(tr, 10);
	char key1[10], key2[10];
	strcpy(key1, "fldr");
	strcpy(key2, "cdp");
	char **keys = malloc(sizeof(char*) * 2);
	keys[0] = key1;
	keys[1] = key2;
	int directions[2] = { -1, -1 };
	sort_params_t *sp;
	init_sort_params(&sp, keys, 2, directions);
	int rc = sort(tr, 10, sizeof(trace_t), sp, &trace_comp);
	assert_int_equal(rc, 0);

	for (int i = 0; i < 9; i++) {
		assert_true(tr[i].fldr >= tr[i + 1].fldr);
		if (tr[i].fldr == tr[i + 1].fldr) {
			assert_true(tr[i].cdp >= tr[i + 1].cdp);
		}
	}

}

int
main(int argc, char *argv[])
{
	const struct CMUnitTest tests[] = { cmocka_unit_test(test_sort),
			cmocka_unit_test(test_sort_trace) };
	return cmocka_run_group_tests(tests, NULL, NULL);
}
