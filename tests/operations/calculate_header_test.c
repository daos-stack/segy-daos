/*
 * header_calculate_test.c
 *
 *  Created on: Jan 27, 2021
 *      Author: omar
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>

#include "operations/calculate_header.h"

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
			temp->d1 = 10;
			temp->d2 = 25;
			temp->cdpt = 50;
			temp->f1 = 10;
			temp->f2 = 25;
			temp->ep = 50;
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
test_set_headers(void **state)
{
	ensemble_list *en;
	int rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);
	char key1[10], key2[10];
	strcpy(key1, "fldr");
	strcpy(key2, "cdp");
	char **keys = malloc(sizeof(char*) * 2);
	keys[0] = key1;
	keys[1] = key2;
	strcpy(keys[0], "fldr");
	strcpy(keys[1], "cdp");
	double a[2] = { 5, 5 };
	double b[2] = { 5.5, 5.5 };
	double c[2] = { 0, 1 };
	double d[2] = { 10, 20 };
	double e[2] = { 20, 20 };
	set_headers(en, 2, keys, a, b, c, d, e, 0);
	ensemble_t *temp_e = doubly_linked_list_get_object(en->ensembles->head,
			offsetof(ensemble_t, n));
	trace_t *temp_t = doubly_linked_list_get_object(temp_e->traces->head,
			offsetof(trace_t, n));
	generic_value fldr, cdp;
	trace_get_header(*temp_t, "fldr", &fldr);
	trace_get_header(*temp_t, "cdp", &cdp);
	assert_int_equal(fldr.i, 60);
	assert_int_equal(cdp.i, 6);
	destroy_ensemble_list(en);

}

static void
test_change_headers(void **state)
{
	ensemble_list *en;
	int rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);
	char key1[10], key2[10], key3[10];
	strcpy(key1, "cdpt");
	strcpy(key2, "d1");
	strcpy(key3, "d2");
	char key4[10], key5[10], key6[10];
	strcpy(key4, "ep");
	strcpy(key5, "f1");
	strcpy(key6, "f2");
	char **keys_1 = malloc(sizeof(char*) * 2);
	char **keys_2 = malloc(sizeof(char*) * 2);
	char **keys_3 = malloc(sizeof(char*) * 2);
	keys_1[0] = key1;
	keys_1[1] = key4;
	keys_2[0] = key2;
	keys_2[1] = key5;
	keys_3[0] = key3;
	keys_3[1] = key6;
	double a[2] = { 5.5, 5.5 };
	double b[2] = { 10.5, 10.5 };
	double c[2] = { 6.5, 6.5 };
	double d[2] = { 10.5, 10.5 };
	double e[2] = { 2.5, 2.5 };
	double f[2] = { 0, 1 };
	change_headers(en, 2, keys_1, keys_2, keys_3, a, b, c, d, e, f , 0);
	ensemble_t *temp_e = doubly_linked_list_get_object(en->ensembles->head,
			offsetof(ensemble_t, n));
	trace_t *temp_t = doubly_linked_list_get_object(temp_e->traces->head,
			offsetof(trace_t, n));
	generic_value cdpt, ep;
	trace_get_header(*temp_t, "cdpt", &cdpt);
	trace_get_header(*temp_t, "ep", &ep);
	assert_int_equal(cdpt.i, 317);
	assert_int_equal(ep.i, 332);
	destroy_ensemble_list(en);

}

int
main(int argc, char *argv[])
{
	const struct CMUnitTest tests[] = { cmocka_unit_test(test_set_headers),
			cmocka_unit_test(test_change_headers) };
	return cmocka_run_group_tests(tests, NULL, NULL);
}
