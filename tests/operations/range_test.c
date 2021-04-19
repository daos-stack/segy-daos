/*
 * range_test.c
 *
 *  Created on: Feb 2, 2021
 *      Author: omar
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include "operations/range.h"

trace_t**
trace_generator(int range)
{
	trace_t **tr = malloc(range * sizeof(trace_t*));
	int size = 0, i = 1, j;
	while (size < range) {
		j = 0;
		while (j < i) {
			trace_t *temp;
			trace_init(&temp);
			temp->fldr = i;
			temp->cdp = size;
			temp->d1 = 10 * size;
			temp->d2 = 25 * size;
			temp->cdpt = 50 + size;
			temp->f1 = 10.0 / (size + 1);
			temp->f2 = 25.0 / (size + 1);
			temp->ep = 50 - size;
			temp->data = NULL;
			tr[size++] = temp;
			if (size >= range)
				break;
			j++;
		}
		i++;
	}
	return tr;
}

int
ensemble_list_generator(ensemble_list **e, int range)
{
	trace_t **tr = trace_generator(range);
	*e = init_ensemble_list();
	int flag = 0, rc;
	for (int i = 0; i < 10; i++) {
		if (i != 0) {
			if (tr[i]->fldr != tr[i - 1]->fldr) {
				flag = 0;
			} else {
				flag = 1;
			}
		}
		rc = ensemble_list_add_trace(tr[i], *e, flag);

	}
	return rc;
}

static void
test_range(void **state)
{
	ensemble_list *en;
	int rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);
	headers_ranges_t *rng = malloc(sizeof(headers_ranges_t));
	char key1[10], key2[10], key3[10], key4[10];
	strcpy(key1, "fldr");
	strcpy(key2, "cdp");
	strcpy(key3, "cdpt");
	strcpy(key4, "f1");
	int num_of_keys = 4;
	char **keys = malloc(num_of_keys * sizeof(char*));
	keys[0] = key1;
	keys[1] = key2;
	keys[2] = key3;
	keys[3] = key4;
	rng->number_of_keys = num_of_keys;
	rng->keys = keys;
	ensemble_range(en, rng);
	assert_int_equal(rng->ntr, 10);
	assert_int_equal(rng->trmin->fldr, 1);
	assert_true(rng->trfirst->f1 == 10);

}

static void
test_range_all_keys(void **state)
{
	ensemble_list *en;
	int rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);
	char **range_keys = malloc(SEIS_NKEYS * sizeof(char*));
	for (int k = 0; k < SEIS_NKEYS; k++) {
		range_keys[k] = malloc((strlen(hdr[k].key) + 1) * sizeof(char));
		strcpy(range_keys[k], hdr[k].key);
	}
	headers_ranges_t *rng = malloc(sizeof(headers_ranges_t));
	rng->number_of_keys = 0;
	rng->keys = range_keys;
	ensemble_range(en, rng);
	assert_true(rng->trlast->d2 == 225);
	assert_true(rng->trmax->f2 == 25);
}

int
main(int argc, char *argv[])
{
	const struct CMUnitTest tests[] = { cmocka_unit_test(test_range),
			cmocka_unit_test(test_range),
			cmocka_unit_test(test_range_all_keys)};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
