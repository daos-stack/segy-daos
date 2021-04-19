/*
 * serial_seismic_api.c
 *
 *  Created on: Feb 11, 2021
 *      Author: mirnamoawad
 */
#include "api/seismic_graph_api.h"
#include "serial_seismic_api_helpers.c"

ensemble_list*
daos_seis_wind_headers_return_traces(root_obj_t *root_obj, char **window_keys,
				     int num_of_keys, generic_value *min_keys,
				     generic_value *max_keys)
{
	operations_controllers_t *op_controller;
	window_params_t 	 *window_params;
	ensemble_list 		 *ensembles_list;
	int 			 curr_idx;
	int 			 rc;

	init_wind_params(&window_params, window_keys, num_of_keys,
			 min_keys, max_keys);

	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to init operations controller ");

	rc = wind_headers(root_obj, NULL, *window_params, op_controller, -1,
			  &curr_idx, &ensembles_list, NULL);
	DSG_ERROR(rc, "Window headers failed ");

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

	free(window_params);

	return ensembles_list;
}

int
daos_seis_wind_headers_then_dump_in_graph(root_obj_t *root_obj,
					  root_obj_t *new_root,
					  duplicate_traces_t duplicate_traces,
					  char **window_keys, int num_of_keys,
					  generic_value *min_keys,
					  generic_value *max_keys)
{
	operations_controllers_t *op_controller;
	window_params_t 	 *window_params;
	ensemble_list 		 *ensembles_list;
	int 			 curr_idx;
	int 			 rc;

	init_wind_params(&window_params, window_keys, num_of_keys,
			 min_keys, max_keys);

	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to init operations controller ");

	rc = wind_headers(root_obj, new_root, *window_params, op_controller, -1,
			  &curr_idx, &ensembles_list, &duplicate_traces);
	DSG_ERROR(rc, "Window headers failed ");

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

	free(window_params);

	return rc;
}

int
daos_seis_parse_linked_list(root_obj_t *root_obj, ensemble_list *e,
			    duplicate_traces_t duplicate_traces,
			    parsing_helpers_t *parsing_helpers)
{
	parse_functions_t *parse_functions;
	gather_obj_t 	  **seismic_obj;
	int		  root_num_of_traces;
	int		  root_num_of_keys;
	int 		  num_of_gathers;
	int 		  curr_idx;
	int 		  rc;
	int		  i;

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	int old_num_of_gathers[root_num_of_keys];

	seismic_obj = malloc(root_num_of_keys * sizeof(gather_obj_t*));

	for (i = 0; i < root_num_of_keys; i++) {
		old_num_of_gathers[i] = 0;

		rc = gather_obj_open(&(root_obj->gather_oids[i]), O_RDWR, NULL,
				     -1, &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object ", end);

		rc = gather_obj_fetch_entries(&(seismic_obj[i]),
					      root_obj->gather_oids[i],
					      root_obj->keys[i], 1);
		DSG_ERROR(rc, "Failed to fetch gather object entry ", end);

		old_num_of_gathers[i] =
				gather_obj_get_number_of_gathers(seismic_obj[i]);

		rc = gather_obj_add_gathers_metadata_to_list(seismic_obj[i],
							     NULL, -1,
							     &curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object metadata "
			  "and adding it to gathers list ");
	}

	rc = init_parsing_parameters(LL, &parse_functions, (void*) e);
	DSG_ERROR(rc, "Failed to initialize linked list parsing functions",
	  	  end);

	root_num_of_traces = root_obj_get_num_of_traces(root_obj);

	trace_t 	*trace;
	while (1) {
		trace = get_trace(parse_functions);
		if (trace == NULL) {
			break;
		}
		root_num_of_traces++;
		trace_hdr_obj_t 	*trace_hdr;
		trace_data_obj_t 	*trace_data;

		if (duplicate_traces == DUPLICATE_TRACES) {
			rc = trace_hdr_obj_create(&trace_hdr, O_RDWR, trace);
			DSG_ERROR(rc, "Failed to create trace header object ",
				  end2);
			rc = trace_hdr_update_headers(trace_hdr, trace, NULL,
						      -1, &curr_idx);
			DSG_ERROR(rc,"Failed to update trace header object headers ",
				  end2);
			rc = trace_data_obj_create(&trace_data, O_RDWR,
						   trace->data, trace->ns);
			DSG_ERROR(rc, "Failed to create trace data object ",
				  end2);
			rc = trace_data_update_data(trace_data, trace, NULL,
						    -1, &curr_idx);
		}
		for (i = 0; i < root_obj->num_of_keys; i++) {
			rc = trace_linking(trace, seismic_obj[i],
					   root_obj->keys[i], NULL, -1,
					   &curr_idx);
			DSG_ERROR(rc, "Linking trace to gather object failed ",
				  end2);
		}
		if (duplicate_traces == DUPLICATE_TRACES){
			trace_hdr_obj_release(trace_hdr);
			trace_data_obj_release(trace_data);
		} else {
			trace_destroy(trace);
		}
	}

	for (i = 0; i < root_obj->num_of_keys; i++) {
		num_of_gathers =
			gather_obj_get_number_of_gathers(seismic_obj[i]);

		rc = gather_obj_update_num_of_gathers(seismic_obj[i],
						      &num_of_gathers, NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object number of gathers ",
			  end3);

		rc = gather_obj_create_traces_array(seismic_obj[i], NULL, -1,
						    &curr_idx,
						    old_num_of_gathers[i]);
		DSG_ERROR(rc, "Failed to create gather object traces array ",
			 end3);

		rc = gather_obj_dump_gathers_list_in_graph(seismic_obj[i], NULL,
							   -1, &curr_idx,
							   old_num_of_gathers[i]);
		DSG_ERROR(rc, "Failed to dump gathers list in graph ", end3);

end3: 		rc = gather_obj_close(seismic_obj[i], NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close gather object ");
	}

	rc = root_obj_update_num_of_traces(root_obj, NULL, &(root_num_of_traces), -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update root object number of traces ");

end2: 	rc = release_parsing_parameters(parse_functions);

end: 	free(seismic_obj);

	return rc;
}

ensemble_list*
daos_seis_get_shot_traces(root_obj_t *root_obj, int shot_id)
{
	generic_value min;
	char 	      *key;

	min.i = shot_id;
	key = malloc((strlen("fldr") + 1) * sizeof(char));
	strcpy(key, "fldr");

	return daos_seis_wind_headers_return_traces(root_obj, &key, 1, &min,
						    &min);
}

ensemble_list*
daos_seis_sort_traces(root_obj_t *root_obj, char **sort_keys, int *directions,
		      int num_of_keys, char **window_keys, int num_of_window_keys,
		      generic_value *min_keys, generic_value *max_keys)
{
	operations_controllers_t *op_controller;
	seismic_object_oid_oh_t  oid_oh;
	window_params_t 	 *wp;
	sort_params_t 		 *sp;
	ensemble_list 		 *ensembles_list;
	ensemble_list 		 *final_list;
	gather_obj_t 		 *gather_obj;
	gather_obj_t 		 *new_gather_obj;
	char 			 **gather_obj_dkeys;
	char 			 *obj_key_name;
	char 			 complex_var[MAX_KEY_LENGTH] = "";
	int			 root_num_of_keys;
	int 			 complex_index = 0;
	int 			 complex_sort = 0;
	int 			 complex_key_exists = 0;
	int 			 dkey_direction = 1;
	int 			 num_of_gathers;
	int 			 key_exists = 0;
	int 			 curr_idx;
	int 			 sort_flag = 0;
	int			 wind_flag = 0;
	int 			 index;
	int 			 rc;
	int 			 i;

	ensembles_list = init_ensemble_list();
	final_list = init_ensemble_list();
	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	rc = init_operations_controller(&op_controller, 0, 1, 10000);

	if (window_keys != NULL) {
		init_wind_params(&wp, window_keys, num_of_window_keys,
				 min_keys, max_keys);
		wind_flag = 1;
	} else{
		wp = NULL;
	}

	if (num_of_keys > 2) {
		complex_sort = 1;
		rc = concatenate_complex_strings(sort_keys, directions, "_",
						 num_of_keys, complex_var);

		complex_key_exists = check_if_key_exists(root_obj->variations,
							 complex_var,
							 root_obj->nvariations,
							 &complex_index);
		if (complex_key_exists == 0) {
			rc = root_obj_add_new_variation(root_obj, complex_var,
							root_obj->nvariations + 1,
							NULL, -1, &curr_idx);
			DSG_ERROR(rc, "Failed to add root object variation ",
				  end);
			complex_index = root_obj->nvariations;
		} else {
			goto fetch_complex_gather;
		}
	}

	key_exists = check_if_key_exists(root_obj->keys, sort_keys[0],
					 root_num_of_keys, &index);

	if (key_exists == 1) {
		oid_oh = root_obj->gather_oids[index];
		strcpy(obj_key_name, sort_keys[0]);
		dkey_direction = directions[0];
	} else {
		int new_root_num_of_keys = root_num_of_keys + 1;
		rc = gather_obj_create_init_new_obj(&new_gather_obj,
						    sort_keys[0]);
		DSG_ERROR(rc, "Failed to create new gather object", end);

		oid_oh = *gather_obj_get_oid_oh(new_gather_obj);
		rc = root_obj_link_new_gather_obj(root_obj, oid_oh,
						  sort_keys[0],
						  new_root_num_of_keys,
						  NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to link gather object to root ", end);
		oid_oh = root_obj->gather_oids[0];
		strcpy(obj_key_name, root_obj->keys[0]);
	}

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open gather object ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 1);
	DSG_ERROR(rc, "Failed to fetch gather object entry ", end);

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
	gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));

	if (key_exists == 0) {
		gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys,
					 dkey_direction, num_of_gathers);

		/** If key doesn't exist then start building the indexing
		 *  for a new object with new key */
		for (i = 0; i < num_of_gathers; i++) {
			rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
									 gather_obj_dkeys[i],
									 op_controller,
									 ensembles_list,
									 -1, &curr_idx);
			DSG_ERROR(rc, "Failed to fetch gather object metadata "
				  "and traces ", end);

			rc = gather_obj_build_indexing(new_gather_obj->oid_oh,
						       new_gather_obj->name,
						       ensembles_list);
			DSG_ERROR(rc,"Failed to build indexing of gather object\n",
				  end);

			rc = doubly_linked_list_delete_node(ensembles_list->ensembles,
							    ensembles_list->ensembles->head,
							    &destroy_ensemble,
							    offsetof(ensemble_t, n));
			DSG_ERROR(rc,"Failed to destroy doubly linked list "
				  "node ", end);
		}
		rc = release_tokenized_array(gather_obj_dkeys, STRING,
					     num_of_gathers);

		rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);

		oid_oh = *gather_obj_get_oid_oh(new_gather_obj);
		strcpy(obj_key_name, gather_obj_get_name(new_gather_obj));

		rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object ", end);

		rc = gather_obj_fetch_entries(&gather_obj, oid_oh,
					      obj_key_name, 1);
		DSG_ERROR(rc, "Failed to fetch gather object entry ", end);

		num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
		gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));
		dkey_direction = directions[0];

		free(new_gather_obj);
	}

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, dkey_direction,
				 num_of_gathers);

	num_of_keys--;

	char 		**sorting_keys;
	int 		*sorting_directions;
	int 		second_key_exist = 0;
	int 		new_snum_of_keys;

	if (num_of_keys >= 1) {
		second_key_exist = check_if_key_exists(gather_obj->keys,
						       sort_keys[1],
						       gather_obj->num_of_keys,
						       &index);



		if (second_key_exist == 0) {
			new_snum_of_keys = gather_obj_get_num_of_skeys(gather_obj) + 1;
			rc = gather_obj_add_new_skey(gather_obj, sort_keys[1],
						     new_snum_of_keys, NULL,
						     -1, &curr_idx);
			DSG_ERROR(rc,"Failed to add new skey to gather "
				  "object ", end);

			for (i = 0; i < num_of_gathers; i++){
				rc = build_second_level_gather(gather_obj,
							       sort_keys[1],
							       gather_obj_dkeys[i],
							       op_controller);
			}
			second_key_exist = 1;
		}

		num_of_keys--;

		if (num_of_keys >= 1) {
			sorting_keys = malloc(num_of_keys * sizeof(char*));
			sorting_directions = malloc(num_of_keys * sizeof(int));

			for (i = 0; i < num_of_keys; i++) {
				sorting_keys[i] =
					malloc(MAX_KEY_LENGTH * sizeof(char));
				memcpy(sorting_keys[i], sort_keys[i + 2],
				       MAX_KEY_LENGTH);
				sorting_directions[i] = directions[i + 2];
			}
			init_sort_params(&sp, sorting_keys, num_of_keys,
					 sorting_directions);
			sort_flag = 1;
		} else {
			sp = NULL;
		}
	}

	seismic_object_oid_oh_t 	soid_oh;
	gather_obj_t 			*sgather_obj;
	char 				**sgather_obj_dkeys;
	int 				snum_of_gathers;
	int 				j;

	for (i = 0; i < num_of_gathers; i++) {
		if (second_key_exist == 1) {
			rc = gather_obj_fetch_soids(gather_obj,
						    gather_obj_dkeys[i],
						    &sort_keys[1], 1, &soid_oh,
						    NULL, -1, &curr_idx);
			DSG_ERROR(rc, "Failed to fetch gather object soids ",
				  end);

			gather_obj_open(&soid_oh, O_RDWR, NULL, -1, &curr_idx);
			DSG_ERROR(rc, "Failed to open gather object ", end);

			gather_obj_fetch_entries(&sgather_obj, soid_oh,
						 sort_keys[1], 1);
			DSG_ERROR(rc, "Failed to fetch gather object entry ",
				  end);

			snum_of_gathers =
				gather_obj_get_number_of_gathers(sgather_obj);

			sgather_obj_dkeys =
				malloc(snum_of_gathers * sizeof(char*));

			gather_obj_prepare_dkeys(sgather_obj, sgather_obj_dkeys,
						 directions[1], snum_of_gathers);

			for (j = 0; j < snum_of_gathers; j++) {
				rc =gather_obj_fetch_gather_metadata_and_traces(sgather_obj,
									        sgather_obj_dkeys[j],
										op_controller,
										ensembles_list,
										-1, &curr_idx);
				DSG_ERROR(rc,"Failed to fetch gather object "
					  "metadata and traces ", end);

				rc = wind_and_sort_traces(ensembles_list,
							  sort_flag, wind_flag,
							  sp, wp, final_list);
			}
			release_tokenized_array(sgather_obj_dkeys, STRING,
						snum_of_gathers);
			gather_obj_close(sgather_obj, NULL, -1, &curr_idx, 1);
		} else {
			rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
									 gather_obj_dkeys[i],
									 op_controller,
									 ensembles_list,
									 -1, &curr_idx);

			DSG_ERROR(rc, "Failed to fetch gather object metadata"
				  " and traces", end);

			rc = wind_and_sort_traces(ensembles_list,
						  sort_flag, wind_flag,
						  sp, wp, final_list);

		}
	}

	if (num_of_keys >= 1) {
		free(sp);
		release_tokenized_array(sorting_keys, STRING, num_of_keys);
		free(sorting_directions);
	}

	rc = release_tokenized_array(gather_obj_dkeys, STRING, num_of_gathers);
	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
	DSG_ERROR(rc, "Failed to close gather object", end);

	if(complex_sort == 1 && complex_key_exists == 0) {

		rc = complex_gather_obj_add_complex_gather(root_obj->complex_oid,
						   final_list, complex_index);
		DSG_ERROR(rc, "Failed to add gather object complex gather");
	} else if(complex_sort == 1 && complex_key_exists == 1) {
fetch_complex_gather:
		rc = complex_gather_obj_fetch_complex_gather(root_obj->complex_oid,
						     final_list,
						     complex_index);
		DSG_ERROR(rc, "Failed to fetch gather object complex gather");
		if (wind_flag == 1) {
			window(final_list->ensembles, wp, &trace_window,
			       &destroy_ensemble, offsetof(ensemble_t, n), 0);
		}
	}

	rc = traces_data_obj_fetch_data_in_list(final_list, 0,
						final_list->ensembles->size,
						op_controller, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to fetch trace data into ensembles list ");

end:	if (window_keys != NULL) {
		free(wp);
	}

	rc = destroy_ensemble_list(ensembles_list);
	free(obj_key_name);

	rc = release_operations_controller(op_controller);

	return final_list;
}
