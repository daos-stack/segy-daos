/*
 * serial_seismic_api_helpers.c
 *
 *  Created on: Feb 11, 2021
 *      Author: mirnamoawad
 */
#include "data_types/ensemble.h"

int
wind_headers(root_obj_t *root_obj, root_obj_t *new_root,
	     window_params_t window_params,
	     operations_controllers_t *op_controller, int parent_idx,
	     int *curr_idx, ensemble_list **ensembles_list,
	     duplicate_traces_t *duplicate_traces)
{
	seismic_object_oid_oh_t oid_oh;
	gather_obj_t 		*gather_obj;
	char 			**gather_obj_dkeys;
	char 			*obj_key_name;
	int 			old_num_of_ensembles;
	int 			root_num_of_keys;
	int 			num_of_gathers;
	int 			key_exist = 0;
	int 			rc = 0;
	int 			i;

	(*ensembles_list) = init_ensemble_list();

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);


	for (i = 0; i < root_num_of_keys; i++) {
		if (strcmp(root_obj->keys[i], window_params.keys[0]) == 0) {
			oid_oh = root_obj->gather_oids[i];
			strcpy(obj_key_name, root_obj->keys[i]);
			key_exist = 1;
			break;
		}
	}

	if (i >= root_num_of_keys){
		oid_oh = root_obj->gather_oids[0];
		strcpy(obj_key_name, root_obj->keys[0]);
	}

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Failed to open gather object ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 1);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
	gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1,
				 num_of_gathers);

	generic_value 	unique_value;
	for (i = 0; i < num_of_gathers; i++) {
		rc = gather_obj_fetch_gather_unique_value(gather_obj, NULL,
							  gather_obj_dkeys[i],
							  &unique_value, -1,
							  curr_idx);
		DSG_ERROR(rc, "Failed to fetch gather unique values ", end);

		if ((key_exist == 1) &&
		    trace_in_range(unique_value, &window_params,0) == 0) {
			continue;
		}

		old_num_of_ensembles = (*ensembles_list)->ensembles->size;

		rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
								 gather_obj_dkeys[i],
								 op_controller,
								 (*ensembles_list),
								 parent_idx,
								 curr_idx);
		DSG_ERROR(rc, "Failed to fetch gather metadata and traces ",
			  end);

		if (key_exist == 0 || window_params.num_of_keys > 1) {
			window((*ensembles_list)->ensembles,
			       (void*) &window_params, &trace_window,
				&destroy_ensemble, offsetof(ensemble_t, n),
				old_num_of_ensembles);
		}
		if (old_num_of_ensembles < (*ensembles_list)->ensembles->size) {
			if (duplicate_traces == NULL) {
				rc = traces_data_obj_fetch_data_in_list((*ensembles_list),
									old_num_of_ensembles,
									1, op_controller,
									parent_idx,
									curr_idx);
				DSG_ERROR(rc, "Failed to fetch trace data object in list ",
					  end);
				continue;
			}
			switch (*duplicate_traces) {
			case (DUPLICATE_TRACES):
				rc = traces_data_obj_fetch_data_in_list((*ensembles_list),
									old_num_of_ensembles,
									1, op_controller,
									parent_idx,
									curr_idx);
				DSG_ERROR(rc,"Failed to fetch trace data object in list ",
					  end);

				rc = daos_seis_parse_linked_list(new_root,
								 (*ensembles_list),
								 *duplicate_traces,
								 NULL);
				DSG_ERROR(rc, "Failed to parse linked list ",
					  end);
				rc = doubly_linked_list_delete_node((*ensembles_list)->ensembles,
								    (*ensembles_list)->ensembles->head,
								    &destroy_ensemble,
								    offsetof(ensemble_t,n));
				DSG_ERROR(rc, "Failed to delete ensemble ",end);

				break;
			case (NO_TRACES_DUPLICATION):
				rc = daos_seis_parse_linked_list(new_root,
								 (*ensembles_list),
								 *duplicate_traces,
								 NULL);
				DSG_ERROR(rc, "Failed to parse linked list ",
					  end);
				rc =doubly_linked_list_delete_node((*ensembles_list)->ensembles,
								   (*ensembles_list)->ensembles->head,
								   &destroy_ensemble,
								   offsetof(ensemble_t,n));
				DSG_ERROR(rc, "Failed to delete ensemble ",end);
				break;
			default:
				break;
			}
		}
	}

	rc = release_tokenized_array((void*) gather_obj_dkeys, STRING,
				     num_of_gathers);
	DSG_ERROR(rc, "Failed to release gather object dkeys ");

end: 	free(obj_key_name);

	return rc;
}

int
gather_obj_build_indexing(seismic_object_oid_oh_t oid_oh, char *gather_name,
			  ensemble_list *ensembles_list)
{
	parse_functions_t *parse_functions;
	gather_obj_t 	  *gather_obj;
	int 		  old_num_of_gathers;
	int		  num_of_gathers;
	int 		  curr_idx;
	int 		  rc;

	rc = gather_obj_open(&(oid_oh), O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open gather object ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, gather_name, 1);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

	old_num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);

	if(old_num_of_gathers > 0) {
		rc = gather_obj_add_gathers_metadata_to_list(gather_obj, NULL, -1,
							     &curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object metadata "
			  "and adding it to gathers list ");
	}


	rc = init_parsing_parameters(LL, &parse_functions,
				     (void*) ensembles_list);
	DSG_ERROR(rc, "Failed to initialize parsing parameters ", end);

	trace_t 	*trace;
	while (1) {
		trace = get_trace(parse_functions);
		if (trace == NULL) {
			break;
		}

		rc = trace_linking(trace, gather_obj,
				   gather_obj_get_name(gather_obj),
				   NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to link trace to gather object ", end2);

		trace_destroy(trace);
	}

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);

	rc = gather_obj_update_num_of_gathers(gather_obj, &num_of_gathers,
					      NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update gather object number of gathers ");

	rc = gather_obj_create_traces_array(gather_obj, NULL, -1, &curr_idx,
					    old_num_of_gathers);
	DSG_ERROR(rc, "Failed to create gather object traces array ");

	rc = gather_obj_dump_gathers_list_in_graph(gather_obj, NULL, -1,
						   &curr_idx,
						   old_num_of_gathers);
	DSG_ERROR(rc, "Failed to dump gathers list in graph ");

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
	DSG_ERROR(rc, "Failed to close gather object ");

end2: 	rc = release_parsing_parameters(parse_functions);

end:	return rc;
}

int
wind_and_sort_traces(ensemble_list *ensembles_list, int sort_flag,
		     int wind_flag, sort_params_t *sp, window_params_t *wp,
		     ensemble_list *final_list)
{
	read_traces_t *gather_traces;
	ensemble_t    *curr_ensemble;
	int 	      num_of_traces;
	int 	      rc;
	int 	      k;

	gather_traces = malloc(sizeof(read_traces_t));

	if (wind_flag == 0) {
		curr_ensemble =
			doubly_linked_list_get_object(ensembles_list->ensembles->head,
						      offsetof(ensemble_t, n));
		num_of_traces = curr_ensemble->traces->size;

		gather_traces->number_of_traces = num_of_traces;
		rc = ensemble_to_array(ensembles_list,
				       &(gather_traces->traces), 0);
		if (sort_flag == 1) {
			sort(gather_traces->traces,
			     gather_traces->number_of_traces, sizeof(trace_t),
			     sp, &trace_comp);
		}
	} else {
		window(ensembles_list->ensembles, wp, &trace_window,
		       &destroy_ensemble, offsetof(ensemble_t, n), 0);

		if (ensembles_list->ensembles->size > 0) {
			curr_ensemble =
				doubly_linked_list_get_object(ensembles_list->ensembles->head,
							      offsetof(ensemble_t, n));
			num_of_traces = curr_ensemble->traces->size;

			gather_traces->number_of_traces = num_of_traces;

			rc = ensemble_to_array(ensembles_list,
					       &(gather_traces->traces), 0);
			if (sort_flag == 1) {
				sort(gather_traces->traces,
				     gather_traces->number_of_traces,
				     sizeof(trace_t), sp, &trace_comp);
			}
		} else {
			gather_traces->number_of_traces = 0;
		}
	}

	if (ensembles_list->ensembles->head != NULL) {
		doubly_linked_list_delete_node(ensembles_list->ensembles,
					       ensembles_list->ensembles->head,
					       &destroy_ensemble,
					       offsetof(ensemble_t, n));
	}

	for (k = 0; k < gather_traces->number_of_traces; k++) {
		trace_t *tr;
		rc = trace_init(&tr);
		memcpy(tr, &(gather_traces->traces[k]), sizeof(trace_t));
		ensemble_list_add_trace(tr, final_list, k);
	}

	if (gather_traces->number_of_traces > 0) {
		free(gather_traces->traces);
		gather_traces->number_of_traces = 0;
	}

	return rc;
}

int
build_second_level_gather(gather_obj_t *gather_obj, char *key, char *dkey,
		operations_controllers_t *op_controller)
{
	seismic_object_oid_oh_t oid_oh;
	ensemble_list 		*ensembles_list;
	gather_obj_t 		*new_gather_obj;
	char 			*obj_key_name;
	int 			curr_idx;
	int 			rc;

	ensembles_list = init_ensemble_list();

	rc = gather_obj_create_init_new_obj(&new_gather_obj, key);

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));

	oid_oh = *gather_obj_get_oid_oh(new_gather_obj);
	strcpy(obj_key_name, gather_obj_get_name(new_gather_obj));

	rc = gather_obj_update_soids(gather_obj, &oid_oh, 1, &key, dkey, NULL,
				     -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update gather object soids ", end);

	rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj, dkey,
							 op_controller,
							 ensembles_list, -1,
							 &curr_idx);
	DSG_ERROR(rc, "Failed to fetch gather object metadata and traces ",
		  end);

	rc = gather_obj_build_indexing(oid_oh, obj_key_name, ensembles_list);
	DSG_ERROR(rc, "Failed to build indexing ");

	rc = doubly_linked_list_delete_node(ensembles_list->ensembles,
					    ensembles_list->ensembles->head,
					    &destroy_ensemble,
					    offsetof(ensemble_t, n));
	DSG_ERROR(rc, "Failed to delete node from ensemble list ");

end: 	rc = gather_obj_release(new_gather_obj);
	free(obj_key_name);

	return rc;
}

int
complex_gather_obj_add_complex_gather(seismic_object_oid_oh_t complex_oid,
			      ensemble_list *final_list, int index)
{
	traces_array_obj_t 			*traces_array_obj;
	gather_node_t 				*gather;
	complex_gather_obj_t 		*complex_gather_obj;
	ensemble_t					*curr_ensemble;
	trace_t						*curr_trace;
	node_t						*ensemble_node;
	node_t						*trace_node;
	int							curr_idx;
	int							size=0;
	int							rc=0;
	int							i=0;
	int							j=0;

	ensemble_node = final_list->ensembles->head;
	gather = malloc(sizeof(gather_node_t));
	gather->number_of_traces = 0;

	for(i=0; i < final_list->ensembles->size; i++) {
		curr_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = curr_ensemble->traces->head;
		size += curr_ensemble->traces->size;
		gather->oids =
			(daos_obj_id_t *)realloc(gather->oids,
						 size*sizeof(daos_obj_id_t));
		while(trace_node != NULL) {
			curr_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,n));
			gather->oids[j] = curr_trace->trace_header_obj;
			gather->number_of_traces++;
			j++;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}

	rc = traces_array_obj_create(&traces_array_obj, O_RDWR, gather->oids,
				     gather->number_of_traces);
	rc = traces_array_obj_open(&traces_array_obj->oid_oh, O_RDWR, NULL, -1,
				   &curr_idx, sizeof(daos_obj_id_t),
				   500 * sizeof(daos_obj_id_t));
	rc = traces_array_obj_update_oids(traces_array_obj,gather, -1,NULL, -1,
					  &curr_idx);
	rc = complex_gather_obj_open(&complex_oid, O_RDWR, NULL, -1, &curr_idx);
	rc = complex_gather_obj_fetch_entries(&complex_gather_obj, complex_oid);
	char temp[10] = "";
	sprintf(temp, "%d", index);
	rc = complex_gather_obj_update_gather_num_of_traces(complex_gather_obj, temp,
						    &gather->number_of_traces,
						    NULL, -1, &curr_idx);
	rc = complex_gather_obj_update_gather_traces_array_oid(complex_gather_obj, temp,
						       &traces_array_obj->oid_oh.oid,
						       NULL, -1, &curr_idx);
	rc = complex_gather_obj_close(complex_gather_obj, NULL, -1, &curr_idx, 1);
	rc = traces_array_obj_close(traces_array_obj, NULL, -1, &curr_idx, 1);

	free(gather->oids);
	free(gather);

	return rc;
}
