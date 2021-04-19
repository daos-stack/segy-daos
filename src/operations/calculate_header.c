/*
 * header_calculate.c
 *
 *  Created on: Jan 27, 2021
 *      Author: omar
 */

#include "operations/calculate_header.h"

int
set_headers(ensemble_list *ensembles, int num_keys, char **keys, double *a,
	    double *b, double *c, double *d, double *e, int start_index)
{
	if(start_index >= ensembles->ensembles->size){
		DSG_ERROR(-1,"Invalid Starting Index to start "
			  "from while setting headers");
		return -1;
	}
	node_t *temp_ensemble = ensembles->ensembles->head;
	int i = 0;
	while(i < start_index){
		i++;
		temp_ensemble = temp_ensemble->next;
	}
	node_t *temp_trace;
	int itr = 0;
	for (i = start_index; i < ensembles->ensembles->size; i++) {
		ensemble_t *en =(ensemble_t*)
				doubly_linked_list_get_object(
							     temp_ensemble,
							     offsetof(
								     ensemble_t,
								     n));
		temp_trace = en->traces->head;
		for (int j = 0; j < en->traces->size; j++) {
			trace_t *t = (trace_t*)
				     doubly_linked_list_get_object(
						     	     	  temp_trace,
								  offsetof(
									trace_t,
									n));
			for (int k = 0; k < num_keys; k++) {
				calculate_header_set(t, keys[k], itr, a[k],
						     b[k], c[k], d[k], e[k]);
			}
			itr++;
			temp_trace = temp_trace->next;

		}
		temp_ensemble = temp_ensemble->next;
	}
	return 0;

}

int
change_headers(ensemble_list *ensembles, int num_keys, char **keys1,
		char **keys2, char **keys3, double *a, double *b, double *c,
		double *d, double *e, double *f, int start_index)
{
	if(start_index >= ensembles->ensembles->size){
		DSG_ERROR(-1,"Invalid Starting Index to start "
			     "from while changing headers");
		return -1;
	}
	node_t *temp_ensemble = ensembles->ensembles->head;
	int i = 0;
	while(i < start_index){
		i++;
		temp_ensemble = temp_ensemble->next;
	}
	node_t *temp_trace;
	for (i = start_index; i < ensembles->ensembles->size; i++) {
		ensemble_t *en = (ensemble_t*)
				doubly_linked_list_get_object(
				temp_ensemble, offsetof(ensemble_t, n));
		temp_trace = en->traces->head;
		for (int j = 0; j < en->traces->size; j++) {
			trace_t *t = (trace_t*) doubly_linked_list_get_object(
					temp_trace, offsetof(trace_t, n));
			for (int k = 0; k < num_keys; k++) {
				calculate_header_change(t, keys1[k], keys2[k],
							keys3[k], a[k], b[k],
							c[k],d[k], e[k], f[k]);
			}
			temp_trace = temp_trace->next;

		}
		temp_ensemble = temp_ensemble->next;
	}
	return 0;
}
