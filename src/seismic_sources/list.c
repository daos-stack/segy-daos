/*
 * list.c
 *
 *  Created on: Jan 31, 2021
 *      Author: mirnamoawad
 */

#include "seismic_sources/list.h"

int
init_list_parameters(void **parameters, void *list_source)
{
	list_parameters_t **list_parameters = (list_parameters_t **)parameters;

	(*list_parameters) = malloc(sizeof(list_parameters_t));
	(*list_parameters)->list = (ensemble_list *)list_source;
	(*list_parameters)->current_ensemble =
			doubly_linked_list_get_object(
				(*list_parameters)->list->ensembles->head,
				offsetof(ensemble_t, n));
	(*list_parameters)->current_trace =
			doubly_linked_list_get_object(
			(*list_parameters)->current_ensemble->traces->head,
			offsetof(trace_t, n));
	return 0;
}

int
init_list_parse_functions(parse_functions_t **parsing_functions)
{
	(*parsing_functions) = malloc(sizeof(parse_functions_t));
	list_parameters_t *parameters = (list_parameters_t *)
			(*parsing_functions)->parse_parameters;
	int (*init_parameters)(void **, void *) = &init_list_parameters;
	(*parsing_functions)->parse_parameters_init = init_parameters;
	(*parsing_functions)->parse_file_headers = NULL;
	(*parsing_functions)->get_text_header = NULL;
	(*parsing_functions)->get_binary_header = NULL;
	(*parsing_functions)->get_num_of_exth = NULL;
	(*parsing_functions)->get_exth = NULL;
	trace_t * (*get_trace)(void *) = &get_list_trace;
	(*parsing_functions)->get_trace = get_trace;
	int (*release_parameters)(void *) = &release_list_parameters;
	(*parsing_functions)->release_parsing_parameters = release_parameters;
	(*parsing_functions)->read_junk = NULL;
	(*parsing_functions)->set_raw_ns = NULL;
	return 0;
}

int
get_next_trace(list_parameters_t *list_parameters)
{
	node_t *n = (node_t*)
		    doubly_linked_list_get_node(list_parameters->current_trace,
					        offsetof(trace_t, n));
	n = n->next;
	if(n != NULL) {
		list_parameters->current_trace = (trace_t *)
				doubly_linked_list_get_object(n,
						              offsetof(trace_t,
						        	       n));
	} else {
		n =(node_t*)
		    doubly_linked_list_get_node(list_parameters->current_ensemble,
				    	    	offsetof(ensemble_t, n));
		n = n->next;
		if(n != NULL) {
			list_parameters->current_ensemble = (ensemble_t *)
				doubly_linked_list_get_object(n,
						offsetof(ensemble_t, n));
			list_parameters->current_trace = (trace_t *)
				doubly_linked_list_get_object(
				list_parameters->current_ensemble->traces->head,
				offsetof(trace_t, n));
		} else {
			list_parameters->current_ensemble = NULL;
			list_parameters->current_trace = NULL;
		}
	}

	return 0;
}

trace_t*
get_list_trace(void *parameters)
{
	int		  rc;
	trace_t 	  *trace;
	list_parameters_t *list_parameters = (list_parameters_t *) parameters;

	if(list_parameters->current_trace == NULL)
	{
		return NULL;
	}
	rc = trace_init(&trace);
	if(rc != 0) {
		printf("Error initializing trace, error code %d \n", rc);
	}

	memcpy(trace, list_parameters->current_trace, TRACEHDR_BYTES);
	trace->trace_header_obj =
			list_parameters->current_trace->trace_header_obj;
	if(trace->ns > 0 && list_parameters->current_trace->data != NULL){
		trace->data = malloc(trace->ns * sizeof(float));
		memcpy(trace->data, list_parameters->current_trace->data,
		       trace->ns * sizeof(float));
	} else {
		trace->data = NULL;
	}

	get_next_trace(list_parameters);

	return trace;
}

int
release_list_parameters(void *parameters)
{
	list_parameters_t *list_parameters = (list_parameters_t *) parameters;
	free(list_parameters);
	return 0;
}
