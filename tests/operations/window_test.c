/*
 * window_test.c
 *
 *  Created on: Jan 30, 2021
 *      Author: omar
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "operations/window.h"
#include "data_types/ensemble.h"

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
			temp->d1 = 10*size;
			temp->d2 = 25*size;
			temp->cdpt = 50+size;
			temp->f1 = size/10;
			temp->f2 = size/25;
			temp->ep = 50-size;
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


static void test_window_traces(void** state)
{
	ensemble_list *en;
	int rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);
	window_params_t* wp = malloc(sizeof(window_params_t));
	char key1[10], key2[10], key3[10];
	strcpy(key1,"fldr");
	strcpy(key2,"cdp");
	strcpy(key3,"cdpt");
	int num_of_keys = 3;
	char** keys = malloc(num_of_keys * sizeof(char*));
	keys[0] = key1;
	keys[1] = key2;
	keys[2] = key3;
	generic_value min[3];
	generic_value max[3];
	min[0].i = 2;
	max[0].i = 8;
	min[1].i = 3;
	max[1].i = 7;
	min[2].i = 53;
	max[2].i = 57;
	wp->keys = keys;
	wp->min_keys = min;
	wp->max_keys = max;
	wp->num_of_keys = num_of_keys;
	window(en->ensembles, wp, &trace_window, &destroy_ensemble,offsetof(ensemble_t,n),0);
	assert_int_equal(en->ensembles->size, 2);
	ensemble_t* test_ensemble = doubly_linked_list_get_object(en->ensembles->tail, offsetof(ensemble_t, n));
	assert_int_equal(test_ensemble->traces->size, 2);
	free(keys);
	free(wp);

}

static void test_window_traces_with_start_index(void ** state){
	ensemble_list *en;
	int rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);
	window_params_t* wp = malloc(sizeof(window_params_t));
	char key1[10], key2[10], key3[10];
	strcpy(key1,"fldr");
	strcpy(key2,"cdp");
	strcpy(key3,"cdpt");
	int num_of_keys = 3;
	char** keys = malloc(num_of_keys * sizeof(char*));
	keys[0] = key1;
	keys[1] = key2;
	keys[2] = key3;
	generic_value min[3];
	generic_value max[3];
	min[0].i = 2;
	max[0].i = 8;
	min[1].i = 3;
	max[1].i = 7;
	min[2].i = 53;
	max[2].i = 57;
	wp->keys = keys;
	wp->min_keys = min;
	wp->max_keys = max;
	wp->num_of_keys = num_of_keys;
	window(en->ensembles, wp, &trace_window, &destroy_ensemble,offsetof(ensemble_t,n),3);
	assert_int_equal(en->ensembles->size, 4);
	ensemble_t* test_ensemble = doubly_linked_list_get_object(en->ensembles->tail, offsetof(ensemble_t, n));
	assert_int_equal(test_ensemble->traces->size, 2);
	test_ensemble = doubly_linked_list_get_object(en->ensembles->head->next, offsetof(ensemble_t, n));
	assert_int_equal(test_ensemble->traces->size, 2);

}



int main(int argc, char* argv[])
{

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_window_traces),
    cmocka_unit_test(test_window_traces_with_start_index)
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
