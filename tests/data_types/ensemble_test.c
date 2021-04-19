/*
 * ensemble_test.c
 *
 *  Created on: Jan 25, 2021
 *      Author: omar
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "data_types/ensemble.h"

trace_t** trace_generator(int range){
	trace_t** tr = malloc(range * sizeof(trace_t*));
	int size = 0, i = 1, j;
	while(size < range){
		j = 0;
		while(j < i){
			trace_t* temp;
			trace_init(&temp);
			temp->fldr = i;
			temp->cdp = size;
			temp->data = NULL;
			tr[size++] = temp;
			if(size >= range) break;
			j++;
		}
		i++;
	}
	return tr;
}

static void test_add_traces(void ** state){
	trace_t** tr = trace_generator(10);
	ensemble_list* e = init_ensemble_list();
	int flag = 0, rc;
	for(int i = 0; i < 10; i++){
		if(i != 0){
			if(tr[i]->fldr != tr[i-1]->fldr){
				flag = 0;
			}
			else{
				flag = 1;
			}
		}
		rc = ensemble_list_add_trace(tr[i], e, flag);
		assert_int_equal(rc, 0);

	}
	assert_int_equal(e->ensembles->size, 4);
	ensemble_t* temp = doubly_linked_list_get_object(e->ensembles->tail, offsetof(ensemble_t, n));
	assert_int_equal(temp->traces->size, 4);
	trace_t* temp_trace = doubly_linked_list_get_object(temp->traces->tail, offsetof(trace_t, n));
	assert_int_equal(temp_trace->cdp, 9);
	temp = doubly_linked_list_get_object(e->ensembles->tail->prev, offsetof(ensemble_t, n));
	assert_int_equal(temp->traces->size, 3);
	rc = destroy_ensemble_list(e);
	assert_int_equal(rc,0);
}


int main(int argc, char* argv[])
{

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_add_traces)
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
