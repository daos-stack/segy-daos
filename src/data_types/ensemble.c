/*
 * ensemble.c
 *
 *  Created on: Jan 25, 2021
 *      Author: omar
 */

#include "data_types/trace.h"
#include "data_types/ensemble.h"
#include "data_structures/doubly_linked_list.h"

ensemble_t*
init_ensemble()
{
	ensemble_t *e = malloc(sizeof(ensemble_t));
	e->traces = doubly_linked_list_init();
	return e;
}

ensemble_list*
init_ensemble_list()
{
	ensemble_list *e = malloc(sizeof(ensemble_list));
	e->ensembles = doubly_linked_list_init();
	return e;
}

int
destroy_ensemble(void *ensemble)
{

	ensemble_t* e = (ensemble_t*)ensemble;
	if (e->traces != NULL) {
		doubly_linked_list_destroy(e->traces, &trace_destroy,
					   offsetof(trace_t, n));
	}
	free(e);
	return 0;
}

int
destroy_ensemble_list(ensemble_list *e)
{
	if (e->ensembles != NULL) {
		doubly_linked_list_destroy(e->ensembles, &destroy_ensemble,
					   offsetof(ensemble_t, n));
	}
	free(e);
	return 0;
}

int
ensemble_list_add_trace(trace_t *tr, ensemble_list *e, int index)
{
	ensemble_t *temp;

	if (index == 0) {
		temp = init_ensemble();
		doubly_linked_list_add_node(e->ensembles, &(temp->n));
	} else {
		temp = (ensemble_t*)
		       doubly_linked_list_get_object(e->ensembles->tail,
				                     offsetof(ensemble_t, n));
	}

	doubly_linked_list_add_node(temp->traces, &(tr->n));

	return 0;

}

int
trace_window(node_t *ensemble_node, void *window_params)
{
	window_params_t*	wp;
	generic_value 		val;
	ensemble_t 		*e;
	trace_t 		*tr;
	node_t 			*h;
	node_t 			*temp;
	char 			*type;
	int			delete_flag;
	int 			i = 0;

	e = doubly_linked_list_get_object(ensemble_node,
					  offsetof(ensemble_t, n));

	h = e->traces->head;

	wp = (window_params_t*) window_params;

	while (h != NULL) {
		temp = h;

		delete_flag = 0;
		tr = doubly_linked_list_get_object(temp, offsetof(trace_t, n));

		for (int j = 0; j < wp->num_of_keys; j++) {
			trace_get_header(*tr, wp->keys[j], &val);
			if (trace_in_range(val, wp, j)) {
				continue;
			} else {
				delete_flag = 1;
				break;
			}
		}
		h = h->next;
		i++;
		if (delete_flag == 1) {
			doubly_linked_list_delete_node(e->traces, temp,
						       &trace_destroy,
						       offsetof(trace_t, n));

		}
	}

	if (e->traces->size == 0) {
		free(e->traces);
		e->traces = NULL;
		return 1;
	} else {
		return 0;
	}

}

int
trace_in_range(generic_value val, window_params_t *wp, int index)
{
	if (!(valcmp(hdtype(wp->keys[index]), val, wp->min_keys[index]) == 1||
	    valcmp(hdtype(wp->keys[index]), val,wp->min_keys[index]) == 0)||
	    !(valcmp(hdtype(wp->keys[index]), val,wp->max_keys[index]) == -1||
	    valcmp(hdtype(wp->keys[index]), val,wp->max_keys[index])== 0)) {
		return 0;
	} else {
		return 1;
	}
}

int
ensemble_to_array(ensemble_list *e, trace_t **traces, int index)
{
	ensemble_t* 	temp_ensemble;
	trace_t*    	temp_trace;
	trace_t 	*curr_trace;
	node_t* 	ensemble_node;
	node_t*		trace_node;
	int 		i = 0;
	int 		num_of_traces;

	ensemble_node = e->ensembles->head;

	if(index >= e->ensembles->size) {
		DSG_ERROR(-1, "Invalid Starting Index while "
			      "converting ensemble to array");
		return -1;
	}
	while (i < index) {
		i++;
		ensemble_node = ensemble_node->next;
	}

	temp_ensemble = doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
	num_of_traces = temp_ensemble->traces->size;
	(*traces) = malloc(num_of_traces * sizeof(trace_t));
	trace_node = temp_ensemble->traces->head;
	for (i = 0; i < num_of_traces; i++) {
		curr_trace =doubly_linked_list_get_object(trace_node,
						          offsetof(trace_t, n));
		(*traces)[i] = *curr_trace;

		if (curr_trace->data != NULL) {
			(*traces)[i].data = malloc(curr_trace->ns
							      * sizeof(float));
			memcpy((*traces)[i].data, curr_trace->data,
			       curr_trace->ns * sizeof(float));
		} else {
			(*traces)[i].data = NULL;
		}

		trace_node = trace_node->next;
	}
	return 0;
}

