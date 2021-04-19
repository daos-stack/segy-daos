/*
 * mpi_seismic_api.c
 *
 *  Created on: Feb 11, 2021
 *      Author: mirnamoawad
 */

#include "api/seismic_graph_api.h"
#include "mpi_seismic_api_helpers.c"

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
	DSG_ERROR(rc, "Failed to initialize operations controller ", end);

	rc = wind_headers(root_obj, NULL, *window_params, op_controller, -1,
			  &curr_idx, &ensembles_list, NULL);
	DSG_ERROR(rc, "Window Headers failed ");

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ", end);

	free(window_params);

	return ensembles_list;

end:	return NULL;
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
	DSG_ERROR(rc, "Failed to initialize operations controller ", end);

	rc = wind_headers(root_obj, new_root, *window_params, op_controller, -1,
			  &curr_idx, &ensembles_list, &duplicate_traces);
	DSG_ERROR(rc, "Window Headers failed ");

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

	free(window_params);

end:	return rc;
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
	int 		  process_rank;
	int 		  curr_idx;
	int 		  rc;
	int 		  i;

	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	seismic_obj = malloc(root_num_of_keys * sizeof(gather_obj_t*));

	for (i = 0; i < root_num_of_keys; i++) {
		rc = gather_obj_open(&(root_obj->gather_oids[i]), O_RDWR, NULL,
				     -1, &curr_idx);
		DSG_ERROR(rc, "Opening gather object failed ", end);

		rc = gather_obj_fetch_entries(&(seismic_obj[i]),
					      root_obj->gather_oids[i],
					      root_obj->keys[i], 0);
		DSG_ERROR(rc, "Failed to fetch gather object entries ", end);
	}

	rc = init_parsing_parameters(LL, &parse_functions, (void*) e);
	DSG_ERROR(rc, "Initializing parsing parameters failed ", end);

	rc = mutex_lock_acquire(parsing_helpers->traces_mutex);
	DSG_ERROR(rc, "Acquiring mutex lock failed ", end2);

	ensemble_t 	*curr_ensemble;
	node_t 		*ensemble_node;

	ensemble_node = e->ensembles->head;

	root_num_of_traces = root_obj_get_num_of_traces(root_obj);

	for (i = 0; i < e->ensembles->size; i++) {
		curr_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
		root_num_of_traces += curr_ensemble->traces->size;

		ensemble_node = ensemble_node->next;
	}

	rc = root_obj_update_num_of_traces(root_obj, NULL,
					   &(root_num_of_traces), -1,
					   &curr_idx);

	rc = mutex_lock_release(parsing_helpers->traces_mutex);
	DSG_ERROR(rc, "Releasing mutex lock failed\n", end2);

	trace_t 	*trace;
	while (1){
		trace = get_trace(parse_functions);
		if (trace == NULL) {
			break;
		}
		trace_hdr_obj_t 	*trace_hdr;
		trace_data_obj_t 	*trace_data;

		if (duplicate_traces == DUPLICATE_TRACES) {
			rc = trace_hdr_obj_create(&trace_hdr, O_RDWR, trace);
			DSG_ERROR(rc, "Creating trace header object failed ",
				  end3);

			rc = trace_hdr_update_headers(trace_hdr, trace, NULL,
						      -1, &curr_idx);
			DSG_ERROR(rc, "Failed to update trace header ", end3);

			rc = trace_data_obj_create(&trace_data, O_RDWR,
						   trace->data, trace->ns);
			DSG_ERROR(rc, "Creating trace data object failed ",
				  end3);

			rc = trace_data_update_data(trace_data, trace, NULL, -1,
						    &curr_idx);
			DSG_ERROR(rc, "Failed to update trace data object data ",
				  end3);
		}
		for (i = 0; i < root_obj->num_of_keys; i++) {
			rc = trace_linking(trace, seismic_obj[i],
					   root_obj->keys[i], NULL, -1,
					   &curr_idx);
			DSG_ERROR(rc, "Linking trace to gather object failed\n",
				  end3);

		}
		if (duplicate_traces == DUPLICATE_TRACES){
			trace_hdr_obj_release(trace_hdr);
			trace_data_obj_release(trace_data);
		} else {
			trace_destroy(trace);
		}
	}

	for (i = 0; i < root_num_of_keys; i++) {

		rc = dump_gathers_list_in_graph(seismic_obj[i], NULL, -1,
						&curr_idx, parsing_helpers);
		DSG_ERROR(rc, "Failed to dump gathers list in graph");

		rc = gather_obj_close(seismic_obj[i], NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close gather object");
	}

end3: 	release_parsing_parameters(parse_functions);
end: 	free(seismic_obj);

	return rc;

end2: 	release_parsing_parameters(parse_functions);
	free(seismic_obj);
	return rc;
}

ensemble_list*
daos_seis_get_shot_traces(root_obj_t *root_obj, int shot_id)
{
	generic_value 		min;
	char 			*key;

	min.i = shot_id;
	key = malloc((strlen("fldr") + 1) * sizeof(char));
	strcpy(key, "fldr");

	return daos_seis_wind_headers_return_traces(root_obj, &key, 1, &min,
						    &min);
}

ensemble_list*
daos_seis_sort_traces(root_obj_t *root_obj, char **sort_keys, int *directions,
		      int num_of_keys, char **window_keys,
		      int num_of_window_keys, generic_value *min_keys,
		      generic_value *max_keys)
{


	operations_controllers_t *op_controller;
	seismic_object_oid_oh_t  oid_oh;
	parsing_helpers_t 	 parsing_helpers;
	window_params_t 	 *wp;
	ensemble_list 		 *ensembles_list;
	ensemble_list 		 *final_list;
	sort_params_t 		 *sp;
	gather_obj_t 		 *gather_obj;
	gather_obj_t		 *new_gather_obj;
	char 			 complex_var[MAX_KEY_LENGTH]="";
	char 			 **gather_obj_dkeys;
	char 			 *obj_key_name;
	int  			 complex_key_exists = 0;
	int  			 second_key_exist = 0;
	int 			 root_num_of_keys;
	int 			 num_of_processes;
	int			 dkey_direction = 1;
	int			 num_of_gathers;
	int  			 complex_index;
	int			 num_of_skeys;
	int 			 process_rank;
	int  			 complex_sort = 0;
	int  			 wind_flag = 0;
	int 			 key_exists = 0;
	int 			 sort_flag = 0;
	int 			 chunksize;
	int 			 nrecords;
	int 			 curr_idx;
	int 			 offset;
	int 			 st_idx;
	int 			 rc;
	int			 i;

	if(window_keys != NULL){
		init_wind_params(&wp, window_keys, num_of_window_keys,
				 min_keys, max_keys);
		wind_flag = 1;
	}

	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to initialize operations controller ", end3);

	ensembles_list = init_ensemble_list();
	final_list = init_ensemble_list();
	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);

	rc = mutex_lock_init(&parsing_helpers.traces_mutex, 0);
	DSG_ERROR(rc, "Initializing mutex lock for traces failed", end);

	rc = mutex_lock_init(&parsing_helpers.gather_mutex, 0);
	DSG_ERROR(rc, "Initializing mutex lock for gather failed", end);

	MPI_Barrier(MPI_COMM_WORLD);

	if(num_of_keys > 2) {
		complex_sort = 1;
		rc = concatenate_complex_strings(sort_keys, directions, "_",
						 num_of_keys,complex_var);
		complex_key_exists = check_if_key_exists(root_obj->variations,
							 complex_var,
							 root_obj->nvariations,
							 &complex_index);
		if(complex_key_exists == 0) {
			if(process_rank == 0 && wind_flag == 0) {
				rc = root_obj_add_new_variation(root_obj,
								complex_var,
								root_obj->nvariations +1,
								NULL, -1, &curr_idx);
			}
			complex_index = root_obj->nvariations;
		} else {
			goto fetch_complex_gather;
		}
	}

	for (i = 0; i < root_num_of_keys; i++) {
		if (strcmp(root_obj->keys[i], sort_keys[0]) == 0) {
			oid_oh = root_obj->gather_oids[i];
			strcpy(obj_key_name, sort_keys[0]);
			key_exists = 1;
			dkey_direction = directions[0];
			break;
		}
	}

	if (key_exists == 0 && process_rank == 0) {
		int new_root_num_of_keys;
		new_root_num_of_keys = root_num_of_keys + 1;

		rc = gather_obj_create(&new_gather_obj, sort_keys[0], O_RDWR,
				       LINKED_LIST);
		DSG_ERROR(rc, "Creating gather object failed ", end2);

		rc = gather_obj_open(gather_obj_get_oid_oh(new_gather_obj),
				     O_RDWR, NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Opening gather object failed ", end2);

		num_of_gathers =
			gather_obj_get_number_of_gathers(new_gather_obj);

		rc = gather_obj_update_num_of_gathers(new_gather_obj,
						      &num_of_gathers, NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc, "Updating gather object number of gathers "
			  "failed ",end2);

		num_of_skeys = gather_obj_get_num_of_skeys(new_gather_obj);

		rc = gather_obj_update_skeys(new_gather_obj, NULL,
					     num_of_skeys, NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Updating gather object skeys failed ", end2);

		rc = gather_obj_close(new_gather_obj, NULL, -1, &curr_idx, 0);
		DSG_ERROR(rc, "Closing gather object failed ", end2);

		oid_oh = *gather_obj_get_oid_oh(new_gather_obj);

		rc = root_obj_link_new_gather_obj(root_obj, oid_oh,
						  sort_keys[0],
						  new_root_num_of_keys, NULL,
						  -1, &curr_idx);
		DSG_ERROR(rc, "Linking gather to root object failed ", end2);
		if (num_of_processes > 1) {
			mpi_broadcast_daos_oid(&(oid_oh.oid), 0);
		}

		oid_oh = root_obj->gather_oids[0];
		strcpy(obj_key_name, root_obj->keys[0]);
	}

	if (key_exists == 0 && process_rank > 0) {
		seismic_object_oid_oh_t 	new_oid_oh;

		mpi_broadcast_daos_oid(&new_oid_oh.oid, 0);
		rc = gather_obj_fetch_entries(&new_gather_obj, new_oid_oh,
					      sort_keys[0], 0);
		DSG_ERROR(rc, "Failed to fetch gather object entries ", end2);

		oid_oh = root_obj->gather_oids[0];
		strcpy(obj_key_name, root_obj->keys[0]);
	}

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Opening gather object failed ", end2);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 0);
	DSG_ERROR(rc, "Fetching gather object entry failed ", end2);

	if (process_rank == 0) {
		rc = gather_obj_fetch_num_of_gathers(gather_obj, NULL,
						     &(gather_obj->number_of_gathers),
						     -1, &curr_idx);
		DSG_ERROR(rc,"Fetching gather object number of gathers "
			  "failed ", end2);
		if (num_of_processes > 1) {
			mpi_broadcast_integer(&gather_obj->number_of_gathers,
					      0);
		}
	} else {
		mpi_broadcast_integer(&gather_obj->number_of_gathers, 0);
	}

	calculate_chunksize(num_of_processes,gather_obj->number_of_gathers,
			    &offset,&chunksize);
	if(process_rank == 0) {
		gather_obj->unique_values =
				malloc(offset * sizeof(generic_value));
		st_idx = 0;
		nrecords = offset;
	} else {
		gather_obj->unique_values =
				malloc(chunksize * sizeof(generic_value));
		st_idx = offset + ((process_rank - 1) * chunksize);
		nrecords = chunksize;
	}

	rc = gather_obj_fetch_dkeys_list(gather_obj, NULL, st_idx, nrecords,
					 gather_obj->unique_values, -1,
					 &curr_idx);
	DSG_ERROR(rc, "Fetching gather object dkeys failed ", end2);

	gather_obj_dkeys = malloc(nrecords * sizeof(char*));

	if (key_exists == 0) {
		gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys,
					 dkey_direction, nrecords);

		/** If key doesn't exist then start building the indexing
		 *  for a new object with new key */
		for (i = 0; i < nrecords; i++) {
			rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
									 gather_obj_dkeys[i],
									 op_controller,
									 ensembles_list,
									 -1,&curr_idx);
			DSG_ERROR(rc, "Fetching gather object metadata and "
				  "traces failed ", end2);

			rc = gather_obj_build_indexing(new_gather_obj->oid_oh,
						       new_gather_obj->name,
						       ensembles_list,
						       &parsing_helpers);
			DSG_ERROR(rc, "Building gather object indexing "
				  "failed ", end2);

			rc = doubly_linked_list_delete_node(ensembles_list->ensembles,
							    ensembles_list->ensembles->head,
							    &destroy_ensemble,
							    offsetof(ensemble_t, n));
			DSG_ERROR(rc,"Destroying ensemble list failed ", end2);
		}

		rc = release_tokenized_array(gather_obj_dkeys, STRING,
					     nrecords);

		rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);

		MPI_Barrier(MPI_COMM_WORLD);

		oid_oh = new_gather_obj->oid_oh;
		strcpy(obj_key_name, gather_obj_get_name(new_gather_obj));

		rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Opening gather object failed ", end2);

		rc = gather_obj_fetch_entries(&gather_obj, oid_oh,
					      obj_key_name, 0);
		DSG_ERROR(rc, "Fetching gather object entries failed ", end2);

		if (process_rank == 0) {
			rc = gather_obj_fetch_num_of_gathers(gather_obj, NULL,
							    &num_of_gathers,
							    -1, &curr_idx);
			DSG_ERROR(rc, "Fetching gather object number of gathers"
				  " failed ", end2);
			if (num_of_processes > 1) {
				mpi_broadcast_integer(&num_of_gathers, 0);
			}
		} else {
			mpi_broadcast_integer(&num_of_gathers, 0);
		}

		calculate_chunksize(num_of_processes,num_of_gathers,
				    &offset,&chunksize);
		if(process_rank == 0) {
			gather_obj->unique_values =
					malloc(offset * sizeof(generic_value));
			st_idx = 0;
			nrecords = offset;
		} else {
			gather_obj->unique_values =
					malloc(chunksize * sizeof(generic_value));
			st_idx = offset + ((process_rank - 1) * chunksize);
			nrecords = chunksize;
		}

		rc = gather_obj_fetch_dkeys_list(gather_obj, NULL, st_idx,
						 nrecords,
						 gather_obj->unique_values,
						 -1, &curr_idx);
		DSG_ERROR(rc, "Fetching gather object dkeys failed ", end2);

		gather_obj_dkeys = malloc(nrecords * sizeof(char*));

		dkey_direction = directions[0];
	}

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, dkey_direction,
				 nrecords);
	num_of_keys--;

	int new_snum_of_keys;
	int current_num_of_ensembles = 0;
	char **sorting_keys;
	int *sorting_directions;
	int index;



	if(num_of_keys >= 1) {
		second_key_exist = check_if_key_exists(gather_obj->keys,
						       sort_keys[1],
						       gather_obj->num_of_keys,
						       &index);
		if(second_key_exist == 0) {
			if(process_rank == 0){
				new_snum_of_keys = gather_obj_get_num_of_skeys(gather_obj) +1;
				rc = gather_obj_add_new_skey(gather_obj, sort_keys[1],
								 new_snum_of_keys,
								 NULL, -1, &curr_idx);
			}
			for(i=0 ; i < nrecords; i++) {
				rc = build_second_level_gather(gather_obj,
							       sort_keys[1],
							       gather_obj_dkeys[i],
							       op_controller,
							       &parsing_helpers);

				post_sort_processing(gather_obj, gather_obj_dkeys[i], sort_keys[1],1,
						op_controller,-1,&curr_idx);
			}
			second_key_exist = 1;
		}
		num_of_keys --;
		if(num_of_keys >= 1) {
			sorting_keys = malloc(num_of_keys * sizeof(char *));
			sorting_directions = malloc(num_of_keys * sizeof(int));
			for(i=0; i < num_of_keys; i++) {
				sorting_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
				memcpy(sorting_keys[i], sort_keys[i+2], MAX_KEY_LENGTH);
				sorting_directions[i] = directions[i+2];
			}
			init_sort_params(&sp, sorting_keys, num_of_keys, sorting_directions);
			sort_flag = 1;
		}
		else{
			sp = NULL;
		}
	}

	ensemble_t* 	curr_ensemble;
	seismic_object_oid_oh_t 	soid_oh;
	gather_obj_t			*sgather_obj;
	char				**sgather_obj_dkeys;
	int				snum_of_gathers;
	int 		k;
	int 		j;

	for (i = 0; i < nrecords; i++) {
		if(second_key_exist == 1)
		{
			gather_obj_fetch_soids(gather_obj, gather_obj_dkeys[i],
					       &sort_keys[1], 1, &soid_oh, NULL,
					       -1, &curr_idx);

			gather_obj_open(&soid_oh, O_RDWR, NULL, -1, &curr_idx);
			gather_obj_fetch_entries(&sgather_obj, soid_oh,
						 sort_keys[1], 1);

			snum_of_gathers =
				gather_obj_get_number_of_gathers(sgather_obj);
			sgather_obj_dkeys =
				malloc(snum_of_gathers * sizeof(char *));

			gather_obj_prepare_dkeys(sgather_obj, sgather_obj_dkeys,
						 directions[1], snum_of_gathers);

			for(j=0; j<snum_of_gathers; j++) {
				rc = gather_obj_fetch_gather_metadata_and_traces(sgather_obj,
										 sgather_obj_dkeys[j],
										 op_controller,
										 ensembles_list,
										 -1, &curr_idx);


				DSG_ERROR(rc, "Fetching gather object metadata and traces failed\n",end2);

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

			if (rc != 0) {
				printf("Fetching gather object metadata and traces "
						"failed, error code = %d \n", rc);
				return NULL;
			}

			rc = wind_and_sort_traces(ensembles_list,
						  sort_flag, wind_flag,
						  sp, wp, final_list);
		}
	}

	if (window_keys != NULL) {
		free(wp);
	}

	if (num_of_keys >= 1) {
		free(sp);
		release_tokenized_array(sorting_keys, STRING, num_of_keys);
		free(sorting_directions);
	}


	rc = release_tokenized_array(gather_obj_dkeys, STRING, nrecords);
	rc = destroy_ensemble_list(ensembles_list);
	free(obj_key_name);
	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);

	if(complex_sort == 1 && complex_key_exists == 0 && wind_flag == 0) {
		rc = complex_gather_obj_add_complex_gather(root_obj->complex_oid,
						   final_list, complex_index,&parsing_helpers);
	} else if(complex_sort == 1 && complex_key_exists == 1) {
fetch_complex_gather:
		rc = complex_gather_obj_fetch_complex_gather(root_obj->complex_oid,
							 final_list,
							 complex_index);
		if(wind_flag == 1) {
			window(final_list->ensembles, wp, &trace_window,
				   &destroy_ensemble, offsetof(ensemble_t, n), 0);
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	rc = traces_data_obj_fetch_data_in_list(final_list, 0,
						final_list->ensembles->size,
						op_controller, -1, &curr_idx);
	DSG_ERROR(rc, "Fetching data into list failed ", end2);

	send_receive_traces_arrays(final_list);

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

	return final_list;

end: 	release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

end3: 	destroy_ensemble_list(ensembles_list);
	return NULL;

end2: 	release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller");
	return NULL;

}
