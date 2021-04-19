/*
 * mpi_seismic_api_helpers.c
 *
 *  Created on: Feb 11, 2021
 *      Author: mirnamoawad
 */

#include "data_types/ensemble.h"
#include "utilities/mutex.h"
#include <mpi.h>

int
post_wind_processing(duplicate_traces_t *duplicate_traces,
		     ensemble_list *ensembles_list, root_obj_t *new_root);
int
dump_gathers_list_in_graph(gather_obj_t *seismic_obj,
			   operations_controllers_t *op_controller,
			   int parent_idx,int *curr_idx,
			   parsing_helpers_t *parsing_helpers);
int
gather_obj_build_indexing(seismic_object_oid_oh_t oid_oh, char *gather_name,
			  ensemble_list *ensembles_list,
			  parsing_helpers_t *parsing_helpers);
void
mpi_receive_integer(int *value, int sender, int message_tag, MPI_Status *status);
void
mpi_receive_traces_array(trace_t *traces, int num_of_traces, int sender,
			 int message_tag, MPI_Status *status);
void
mpi_receive_trace_data(float *trace_data, int num_of_samples, int sender,
		       int message_tag, MPI_Status *status);
void
mpi_send_integer(int *value, int receiver, int message_tag);
void
mpi_send_traces_array(trace_t *traces, int num_of_traces, int receiver,
		      int message_tag);
void
mpi_send_trace_data(float *trace_data, int num_of_samples, int receiver,
		    int message_tag);
void
send_receive_traces_arrays(ensemble_list *ensembles_list);
void
mpi_broadcast_integer(int *value, int sender);
void
mpi_broadcast_daos_oid(daos_obj_id_t *oid, int sender);

int
wind_headers(root_obj_t *root_obj, root_obj_t *new_root,
	     window_params_t window_params,
	     operations_controllers_t *op_controller, int parent_idx,
	     int *curr_idx, ensemble_list **ensembles_list,
	     duplicate_traces_t *duplicate_traces)
{
	seismic_object_oid_oh_t oid_oh;
	parsing_helpers_t 	parsing_helpers;
	gather_obj_t 		*gather_obj;
	char 			**gather_obj_dkeys;
	char 			*obj_key_name;
	int 			old_num_of_ensembles;
	int			num_of_processes;
	int 			root_num_of_keys;
	int 			num_of_gathers;
	int 			process_rank;
	int			chunksize;
	int 			key_exist = 0;
	int 			nrecords;
	int 			offset;
	int 			st_idx;
	int 			rc = 0;
	int 			i;

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	(*ensembles_list) = init_ensemble_list();

	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);

	rc = mutex_lock_init(&parsing_helpers.traces_mutex, 0);
	DSG_ERROR(rc, "Initializing traces mutex failed ", end);

	rc = mutex_lock_init(&parsing_helpers.gather_mutex, 0);
	DSG_ERROR(rc, "Initializing gather mutex failed ", end);

	MPI_Barrier(MPI_COMM_WORLD);

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));

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
	DSG_ERROR(rc, "Opening gather object failed ", end2);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 0);
	DSG_ERROR(rc, "Fetching gather object entries failed ", end2);

	if (process_rank == 0) {
		rc = gather_obj_fetch_num_of_gathers(gather_obj, NULL,
						     &(num_of_gathers), -1,
						     curr_idx);
		DSG_ERROR(rc, "Failed at fetching gather object number of "
			  "gathers ", end2);
		if (num_of_processes > 0) {
			mpi_broadcast_integer(&num_of_gathers, 0);
		}
	} else {
		mpi_broadcast_integer(&num_of_gathers, 0);
	}

	gather_obj->number_of_gathers = num_of_gathers;

	calculate_chunksize(num_of_processes,gather_obj->number_of_gathers,
			    &offset,&chunksize);

	if (process_rank == 0) {
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
					 curr_idx);
	DSG_ERROR(rc, "Fetching dkeys from gather object failed ", end2);

	gather_obj_dkeys = malloc(nrecords * sizeof(char*));

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1, nrecords);

	generic_value 	unique_value;
	for (i = 0; i < nrecords; i++) {
		rc = gather_obj_fetch_gather_unique_value(gather_obj, NULL,
							  gather_obj_dkeys[i],
							  &unique_value, -1,
							  curr_idx);
		DSG_ERROR(rc, "Fetching unique value from gather object "
			  "failed ", end2);

		if ((key_exist == 1) &&
		    trace_in_range(unique_value, &window_params, 0) == 0) {
			continue;
		}

		old_num_of_ensembles = (*ensembles_list)->ensembles->size;

		rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
								 gather_obj_dkeys[i],
								 op_controller,
								 (*ensembles_list),
								 -1, curr_idx);
		DSG_ERROR(rc, "Fetching gather object metadata and traces"
			  " failed ", end2);

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
									-1, curr_idx);
				DSG_ERROR(rc,"Fetching data into list failed ",
					  end2);
				continue;
			}
			switch (*duplicate_traces) {
			case (DUPLICATE_TRACES):
				rc = traces_data_obj_fetch_data_in_list((*ensembles_list),
									old_num_of_ensembles,
									1, op_controller,
									-1, curr_idx);
				DSG_ERROR(rc,"Fetching data into list failed ",
					  end2);
				rc = daos_seis_parse_linked_list(new_root,
								 (*ensembles_list),
								 *duplicate_traces,
								 &parsing_helpers);
				DSG_ERROR(rc, "Parsing linked list failed ",
					  end2);
				rc =doubly_linked_list_delete_node((*ensembles_list)->ensembles,
								   (*ensembles_list)->ensembles->head,
								   &destroy_ensemble,
								   offsetof(ensemble_t,n));
				DSG_ERROR(rc,"Deleting ensemble from linked list failed ", end2);
				break;
			case (NO_TRACES_DUPLICATION):
				rc = daos_seis_parse_linked_list(new_root,
								 (*ensembles_list),
								 *duplicate_traces,
								 &parsing_helpers);
				DSG_ERROR(rc, "Parsing linked list failed ",
					  end2);
				rc = doubly_linked_list_delete_node((*ensembles_list)->ensembles,
								    (*ensembles_list)->ensembles->head,
								    &destroy_ensemble,
								    offsetof(ensemble_t,n));
				DSG_ERROR(rc,"Deleting ensemble from linked"
					  " list failed ", end2);
				break;
			default:
				break;
			}
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	if(duplicate_traces == NULL) {
		send_receive_traces_arrays(*ensembles_list);
	}

	rc = release_tokenized_array((void*) gather_obj_dkeys, STRING,
				     nrecords);
	DSG_ERROR(rc, "Releasing tokenized array failed ");

end2: 	free(obj_key_name);
	return rc;

end: 	return rc;
}

int
post_wind_processing(duplicate_traces_t *duplicate_traces,
		     ensemble_list *ensembles_list, root_obj_t *new_root)
{
	int root_num_of_keys;
	int num_of_gathers;
	int curr_idx;
	int rc;
	int i;

	root_num_of_keys = root_obj_get_num_of_keys(new_root);

	for (i = 0; i < root_num_of_keys; i++) {
		gather_obj_t 		*seismic_obj;
		char 			**dkeys_list;

		rc = gather_obj_open(&new_root->gather_oids[i], O_RDWR, NULL,
				     -1, &curr_idx);
		DSG_ERROR(rc, "Opening gather object failed ", end2);

		rc = gather_obj_fetch_entries(&seismic_obj,
					      new_root->gather_oids[i],
					      new_root->keys[i], 0);
		DSG_ERROR(rc, "Fetching gather object entry failed ", end);

		rc = gather_obj_fetch_num_of_gathers(seismic_obj, NULL,
						     &num_of_gathers,
						     -1, &curr_idx);
		DSG_ERROR(rc,"Fetching gather object number of gathers "
			  "failed ", end);

		seismic_obj->number_of_gathers = num_of_gathers;

		dkeys_list = malloc(num_of_gathers * sizeof(char*));

		rc = gather_obj_get_dkeys_array(seismic_obj, num_of_gathers,
						dkeys_list);
		DSG_ERROR(rc, "Fetching gather object dkeys failed ", end3);

		int z;
		if (num_of_gathers > 0) {
			seismic_obj->unique_values =
					malloc(num_of_gathers *
					       sizeof(generic_value));

			for (z = 0; z < num_of_gathers; z++) {
				rc = gather_obj_fetch_gather_unique_value(seismic_obj,
									  NULL,
									  dkeys_list[z],
									  &seismic_obj->unique_values[z],
									  -1, &curr_idx);
				DSG_ERROR(rc,"Fetching gather object unique value failed\n",
					  end3);
			}
			generic_value_sort_params_t 	*sp;
			sp = init_generic_value_sort_params(hdtype(seismic_obj->name),
							    1);
			rc = sort(seismic_obj->unique_values, num_of_gathers,
				  sizeof(generic_value), sp,
				  &generic_value_compare);
			free(sp);

			rc = gather_obj_update_dkeys_list(seismic_obj, 0,
							  num_of_gathers,
							  seismic_obj->unique_values,
							  NULL, -1, &curr_idx);
			DSG_ERROR(rc, "Updating gather object dkeys "
				  "failed ", end3);
		}

end3: 		release_tokenized_array(dkeys_list, STRING, num_of_gathers);

end: 		rc = gather_obj_close(seismic_obj, NULL, -1, &curr_idx, 1);
	}


end2:
	return rc;
}


void post_sort_processing(gather_obj_t *gather_obj, char* dkey, char* key,int direction,
			operations_controllers_t *op_controller,
		    int parent_idx,int *curr_idx)
{


	seismic_object_oid_oh_t 	soid_oh;
	gather_obj_t*				sgather_obj;
	char 						**dkeys_list;
	int rc = 0;
	int i;
	gather_obj_fetch_soids(gather_obj, dkey,&key, 1, &soid_oh, NULL,-1,curr_idx);
	gather_obj_open(&soid_oh, O_RDWR, NULL, -1, curr_idx);
	gather_obj_fetch_entries(&sgather_obj, soid_oh,
				 key, 1);

	int num_of_gathers =
		gather_obj_get_number_of_gathers(sgather_obj);

	dkeys_list = malloc(num_of_gathers * sizeof(char*));

	rc = gather_obj_get_dkeys_array(sgather_obj, num_of_gathers,
					dkeys_list);


	if(num_of_gathers > 0) {
				sgather_obj->unique_values =
						malloc(sizeof(generic_value) *
						       num_of_gathers);
				for(i=0; i < num_of_gathers; i++) {
					rc = gather_obj_fetch_gather_unique_value(sgather_obj,
										  NULL,
										  dkeys_list[i],
										  &sgather_obj->unique_values[i],
										  -1, curr_idx);
				}
				generic_value_sort_params_t *sp;
				sp = init_generic_value_sort_params(hdtype(sgather_obj->name),
								    direction);
				rc = sort(sgather_obj->unique_values, num_of_gathers,
					  sizeof(generic_value), sp,
					  &generic_value_compare);
				free(sp);
				rc = gather_obj_update_dkeys_list(sgather_obj, 0,
								  num_of_gathers,
								  sgather_obj->unique_values,
								  NULL, -1, curr_idx);
			}
	release_tokenized_array(dkeys_list, STRING, num_of_gathers);
	rc = gather_obj_close(sgather_obj, NULL, -1, curr_idx, 1);
}

//int
//post_parse_processing(gather_obj_t *seismic_obj)
//{
//
//	char 			**dkeys_list;
//	int 			rc;
//	int 			num_of_gathers;
//	int			curr_idx;
//
//	rc = gather_obj_fetch_num_of_gathers(seismic_obj, NULL,
//					     &num_of_gathers,
//					     -1, &curr_idx);
//	DSG_ERROR(rc,"Fetching gather object number of gathers "
//		  "failed ", end);
//
//	seismic_obj->number_of_gathers = num_of_gathers;
//
//	dkeys_list = malloc(num_of_gathers * sizeof(char*));
//
//	rc = gather_obj_get_dkeys_array(seismic_obj, num_of_gathers,
//					dkeys_list);
//	DSG_ERROR(rc, "Fetching gather object dkeys failed ", end2);
//
//	int z;
//	if (num_of_gathers > 0) {
//		seismic_obj->unique_values =
//				malloc(num_of_gathers *
//				       sizeof(generic_value));
//
//		for (z = 0; z < num_of_gathers; z++) {
//			rc =
//			gather_obj_fetch_gather_unique_value(seismic_obj,
//							     NULL,
//							     dkeys_list[z],
//							     &seismic_obj->unique_values[z],
//							     -1, &curr_idx);
//			DSG_ERROR(rc,"Fetching gather object unique value failed\n",
//				  end2);
//		}
//		generic_value_sort_params_t 	*sp;
//		sp = init_generic_value_sort_params(hdtype(seismic_obj->name),
//						    1);
//		rc = sort(seismic_obj->unique_values, num_of_gathers,
//			  sizeof(generic_value), sp,
//			  &generic_value_compare);
//		free(sp);
//
//		rc = gather_obj_update_dkeys_list(seismic_obj, 0,
//						  num_of_gathers,
//						  seismic_obj->unique_values,
//						  NULL, -1, &curr_idx);
//		DSG_ERROR(rc, "Updating gather object dkeys "
//			  "failed ", end2);
//	}
//
//end2:
//	release_tokenized_array(dkeys_list, STRING, num_of_gathers);
//
//end:
//
//	return rc;
//
//}

int
dump_gathers_list_in_graph(gather_obj_t *seismic_obj,
			   operations_controllers_t *op_controller, int parent_idx,
			   int *curr_idx, parsing_helpers_t *parsing_helpers)
{
	traces_array_obj_t **traces_array_obj;
	gather_node_t 	   *curr_gather;
	uint64_t 	   iod_nr;
	uint64_t	   iod_size;
	node_t 		   *gather_node;
	char		   *dkey_name;
	int 		   old_num_of_traces;
	int		   num_of_gathers;
	int 		   rc;
	int 		   z = 0;
	int 		   i = 0;
	int		   size = seismic_obj->gathers_list->gathers->size;


	gather_node = seismic_obj->gathers_list->gathers->head;

	if (gather_node == NULL){
		return 0;
	}

	num_of_gathers = gather_obj_get_number_of_gathers(seismic_obj);

	seismic_obj->unique_values =
			malloc(num_of_gathers * sizeof(generic_value));

	seismic_obj->trace_oids_ohs =
			malloc(num_of_gathers *
			       sizeof(seismic_object_oid_oh_t));

	seismic_obj->keys = NULL;

	traces_array_obj = malloc(num_of_gathers * sizeof(traces_array_obj_t*));

	dkey_name = seismic_obj->name;

	int y =0;
	while (z < size) {
		curr_gather =
			doubly_linked_list_get_object(gather_node,
						      offsetof(gather_node_t, n));

		int ntraces = curr_gather->number_of_traces;
		char temp[200] = "";
		char gather_dkey_name[200] = "";
		strcat(gather_dkey_name, dkey_name);
		strcat(gather_dkey_name, KEY_SEPARATOR);
		val_sprintf(temp, curr_gather->unique_key, hdtype(dkey_name));
		strcat(gather_dkey_name, temp);

		rc = mutex_lock_acquire(parsing_helpers->gather_mutex);
		DSG_ERROR(rc, "Acquiring mutex lock failed ", end);

		int 	num_of_akeys;
		num_of_akeys =
			gather_obj_get_dkey_num_of_akeys(seismic_obj,
							 gather_dkey_name, 3);
		if (num_of_akeys == 0) {

			seismic_obj->unique_values[y] =
					curr_gather->unique_key;
			rc = gather_obj_update_gather_unique_value(seismic_obj,
								   gather_dkey_name,
								   &(curr_gather->unique_key),
								   NULL, -1,
								   curr_idx);
			DSG_ERROR(rc,"Updating gather object unique value"
				  " failed ", end2);

			rc = traces_array_obj_create(&traces_array_obj[z],
						     O_RDWR, curr_gather->oids,
						     curr_gather->number_of_traces);
			DSG_ERROR(rc, "Creating traces array object failed\n",
				  end2);

			seismic_obj->trace_oids_ohs[z] =
					*(traces_array_obj_get_id_oh(traces_array_obj[z]));

			rc = gather_obj_update_gather_traces_array_oid(seismic_obj,
								       gather_dkey_name,
								       &(seismic_obj->trace_oids_ohs[z].oid),
								       NULL, -1, curr_idx);
			DSG_ERROR(rc,"Updating gather object traces array"
				  " oids failed ", end2);
			y++;
		} else {
			seismic_obj->number_of_gathers--;

			traces_array_obj[z] = malloc(sizeof(traces_array_obj_t));
			traces_array_obj[z]->oids = NULL;
			traces_array_obj[z]->io_parameters = NULL;

			rc = gather_obj_fetch_gather_traces_array_oid(seismic_obj,
								      NULL,
								      gather_dkey_name,
								      &(traces_array_obj[z]->oid_oh.oid),
								      -1, curr_idx);

			DSG_ERROR(rc, "Fetching gather object traces array oids"
				  " failed ", end2);

			seismic_obj->trace_oids_ohs[z] =
					*(traces_array_obj_get_id_oh(traces_array_obj[z]));

			rc = gather_obj_fetch_gather_num_of_traces(seismic_obj,
								   NULL,
								   gather_dkey_name,
								   &old_num_of_traces,
								   -1, curr_idx);
			DSG_ERROR(rc, "Fetching gather object number of traces"
				  " failed ", end2);

			ntraces += old_num_of_traces;
			curr_gather->number_of_traces += old_num_of_traces;
		}

		rc = gather_obj_update_gather_num_of_traces(seismic_obj,
							    gather_dkey_name,
							    &ntraces, NULL,
							    -1, curr_idx);
		DSG_ERROR(rc, "Updating gather object number of traces "
			  "failed ", end2);

		rc = traces_array_obj_open(&traces_array_obj[z]->oid_oh, O_RDWR,
					   NULL, parent_idx, curr_idx,
					   sizeof(daos_obj_id_t),
					   500 * sizeof(daos_obj_id_t));
		DSG_ERROR(rc, "Opening traces array object failed ", end2);

		rc = traces_array_obj_update_oids(traces_array_obj[z],
						  curr_gather, -1,op_controller,
						  parent_idx, curr_idx);
		DSG_ERROR(rc, "Updating traces array oids failed ", end2);

		rc = traces_array_obj_close(traces_array_obj[z], NULL, -1,
					    curr_idx, 1);

		rc = mutex_lock_release(parsing_helpers->gather_mutex);
		DSG_ERROR(rc, "Releasing mutex lock failed ", end);

		z++;
		gather_node = gather_node->next;
	}

	int old_num_of_gathers;

	if (seismic_obj->number_of_gathers > 0) {
		old_num_of_gathers = seismic_obj->number_of_gathers;
		rc = mutex_lock_acquire(parsing_helpers->traces_mutex);

		char	**dkeys_list;
		int 	existing_num_of_gathers;
		rc = gather_obj_fetch_num_of_gathers(seismic_obj, NULL,
						     &existing_num_of_gathers,
						     -1, curr_idx);
		DSG_ERROR(rc,"Fetching gather object num of gathers failed ",
			  end2);

		seismic_obj->number_of_gathers += existing_num_of_gathers;
		num_of_gathers = seismic_obj->number_of_gathers;

		rc = gather_obj_update_num_of_gathers(seismic_obj,
						      &seismic_obj->number_of_gathers,
						      NULL, -1, curr_idx);

		DSG_ERROR(rc,"Updating gather object num of gathers failed ",
			  end2);

		int index;

		if(existing_num_of_gathers > 0) {
			seismic_obj->unique_values =
					(generic_value*)realloc(seismic_obj->unique_values,
								(y+existing_num_of_gathers) *
								sizeof(generic_value));

			rc = gather_obj_fetch_dkeys_list(seismic_obj, NULL, 0,
							 existing_num_of_gathers,
							 &seismic_obj->unique_values[y],
							 -1, curr_idx);
		}

		generic_value_sort_params_t 	*sp;
		sp = init_generic_value_sort_params(hdtype(seismic_obj->name), 1);

		rc = sort(seismic_obj->unique_values, num_of_gathers,
			  sizeof(generic_value), sp, &generic_value_compare);
		free(sp);

		rc = gather_obj_update_dkeys_list(seismic_obj, 0,
						  num_of_gathers,
						  seismic_obj->unique_values,
						  NULL, -1, curr_idx);
		DSG_ERROR(rc, "Updating gather object dkeys "
			  "failed ", end2);

		rc = mutex_lock_release(parsing_helpers->traces_mutex);
	}

end2: 	free(traces_array_obj);
	return rc;

end: 	free(traces_array_obj);
	return rc;
}

int
complex_gather_obj_add_complex_gather(seismic_object_oid_oh_t complex_oid,
			      ensemble_list *final_list, int index,parsing_helpers_t *parsing_helpers)
{
	traces_array_obj_t 			*traces_array_obj;
	gather_node_t 				*gather;
	complex_gather_obj_t 		*complex_gather_obj;
	ensemble_t					*curr_ensemble;
	trace_t						*curr_trace;
	node_t						*ensemble_node;
	node_t						*trace_node;
	MPI_Status 					status;
	int							curr_idx;
	int							size=0;
	int							rc=0;
	int							i=0;
	int							j=0;
	int							process_rank;
	int							num_of_processes;
	int							start_index;
	int							tag = 3;


	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);

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


	if(process_rank == 0){
		if(num_of_processes > 1){
			mpi_send_integer(&(gather->number_of_traces),
					 1, tag);
		}
		start_index = 0;
	}
	else if(process_rank == num_of_processes - 1){
		mpi_receive_integer((&start_index),
				    process_rank-1, tag,
				    &status);
	}
	else{
		mpi_receive_integer((&start_index),
				    process_rank-1, tag,
				    &status);
		int temp = gather->number_of_traces + start_index;
		mpi_send_integer(&(temp),
				 process_rank + 1, tag);

	}
	rc = complex_gather_obj_open(&complex_oid, O_RDWR, NULL, -1, &curr_idx);
	rc = complex_gather_obj_fetch_entries(&complex_gather_obj, complex_oid);
	char temp[10] = "";
	sprintf(temp, "%d", index);

	int	num_of_akeys;

	rc = mutex_lock_acquire(parsing_helpers->traces_mutex);
	num_of_akeys =
		complex_gather_obj_get_dkey_num_of_akeys(complex_gather_obj,
						 temp, 2);
	if(num_of_akeys == 0){
		rc = traces_array_obj_create(&traces_array_obj, O_RDWR, gather->oids,
				     	 gather->number_of_traces);
		rc = complex_gather_obj_update_gather_traces_array_oid(complex_gather_obj, temp,
							       &traces_array_obj->oid_oh.oid,
							       NULL, -1, &curr_idx);
		rc=complex_gather_obj_update_gather_num_of_traces(complex_gather_obj,
							       temp,
							       &gather->number_of_traces,
							       NULL, -1,
							       &curr_idx);
	}
	else{
		int num_of_traces;
		rc = traces_array_obj_init(&traces_array_obj);
		rc = complex_gather_obj_fetch_gather_traces_array_oid(complex_gather_obj, NULL, temp,
			       &traces_array_obj->oid_oh.oid,-1, &curr_idx);
		rc =
		complex_gather_obj_fetch_gather_num_of_traces(complex_gather_obj,
							      NULL, temp,&num_of_traces,
							      -1,&curr_idx);
		gather->number_of_traces += num_of_traces;
		rc=complex_gather_obj_update_gather_num_of_traces(complex_gather_obj,
							       temp,
							       &gather->number_of_traces,
							       NULL, -1,
							       &curr_idx);
	}

	rc = mutex_lock_release(parsing_helpers->traces_mutex);
	rc = traces_array_obj_open(&traces_array_obj->oid_oh, O_RDWR, NULL, -1,
			   &curr_idx, sizeof(daos_obj_id_t),
			   500 * sizeof(daos_obj_id_t));
	rc = traces_array_obj_update_oids(traces_array_obj,gather,start_index,NULL, -1,
				  &curr_idx);

	rc = complex_gather_obj_close(complex_gather_obj, NULL, -1, &curr_idx, 1);
	rc = traces_array_obj_close(traces_array_obj, NULL, -1, &curr_idx, 1);


	free(gather->oids);
	free(gather);

	return rc;
}

int
gather_obj_build_indexing(seismic_object_oid_oh_t oid_oh, char *gather_name,
			  ensemble_list *ensembles_list,
			  parsing_helpers_t *parsing_helpers)
{
	parse_functions_t *parse_functions;
	gather_obj_t 	  *gather_obj;
	char 		  **dkeys;
	int 		  old_num_of_gathers = 0;
	int 		  curr_idx;
	int 		  rc;

	rc = gather_obj_open(&(oid_oh), O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Opening gather object failed ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, gather_name, 0);

	rc = init_parsing_parameters(LL, &parse_functions,
				     (void*) ensembles_list);
	DSG_ERROR(rc, "Initializing parsing parameters failed ", end);

	trace_t 	*trace;
	while (1) {
		trace = get_trace(parse_functions);
		if (trace == NULL) {
			break;
		}
		rc = trace_linking(trace, gather_obj,
				   gather_obj_get_name(gather_obj),
				   NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Linking traces to gather object failed ", end2);

		trace_destroy(trace);
	}

	rc = dump_gathers_list_in_graph(gather_obj, NULL, -1, &curr_idx,
					parsing_helpers);
	DSG_ERROR(rc, "Failed at dumping gathers list in graph ");

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);



end2: 	rc = release_parsing_parameters(parse_functions);

end:	return rc;
}

int
build_second_level_gather(gather_obj_t *gather_obj,char *key, char *dkey,
			  operations_controllers_t *op_controller,
			  parsing_helpers_t *parsing_helpers)
{
	seismic_object_oid_oh_t		 oid_oh;
	ensemble_list 			 *ensembles_list;
	gather_obj_t			 *new_gather_obj;
	char 				 *obj_key_name;
	int				 curr_idx;
	int				 rc;

	ensembles_list = init_ensemble_list();

	rc = gather_obj_create_init_new_obj(&new_gather_obj,key);

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));
	oid_oh = *gather_obj_get_oid_oh(new_gather_obj);
	strcpy(obj_key_name, gather_obj_get_name(new_gather_obj));

	rc = gather_obj_update_soids(gather_obj, &oid_oh, 1, &key,
				     dkey, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update gather object soids \n", end);

	rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj, dkey,
							 op_controller,
							 ensembles_list,
							 -1, &curr_idx);
	DSG_ERROR(rc, "Failed to fetch gather object metadata and traces \n", end);
	rc = gather_obj_build_indexing(oid_oh, obj_key_name,
				       ensembles_list, parsing_helpers);
	DSG_ERROR(rc, "Failed to build indexing \n");
	rc = doubly_linked_list_delete_node(ensembles_list->ensembles,
					    ensembles_list->ensembles->head,
					    &destroy_ensemble,
					    offsetof(ensemble_t,n));
	DSG_ERROR(rc, "Failed to delete node from ensemble list \n");

end:
	rc = gather_obj_release(new_gather_obj);
	free(obj_key_name);

	return rc;
}


int
wind_and_sort_traces(ensemble_list *ensembles_list, int sort_flag,
		     int wind_flag, sort_params_t *sp, window_params_t *wp,
		     ensemble_list *final_list)
{
	ensemble_t* 	curr_ensemble;
	int		rc;
	int 		k;
	int		num_of_traces;
	read_traces_t *gather_traces;

	gather_traces = malloc(sizeof(read_traces_t));

	if (wind_flag == 0) {
		curr_ensemble =
			doubly_linked_list_get_object(ensembles_list->ensembles->head,
						      offsetof(ensemble_t, n));
		num_of_traces = curr_ensemble->traces->size;

		gather_traces->number_of_traces = num_of_traces;
		rc = ensemble_to_array(ensembles_list,
				       &(gather_traces->traces),
				       0);
		if(sort_flag == 1){
			sort(gather_traces->traces,
			     gather_traces->number_of_traces,
			     sizeof(trace_t), sp, &trace_comp);
		}
	} else {
		window(ensembles_list->ensembles, wp, &trace_window,
		       &destroy_ensemble, offsetof(ensemble_t, n), 0);

		if(ensembles_list->ensembles->size > 0){
			curr_ensemble =
				doubly_linked_list_get_object(ensembles_list->ensembles->head,
							      offsetof(ensemble_t, n));
			num_of_traces = curr_ensemble->traces->size;

			gather_traces->number_of_traces = num_of_traces;

			rc = ensemble_to_array(ensembles_list,
					       &(gather_traces->traces),
					       0);
			if(sort_flag == 1){
				sort(gather_traces->traces,
				     gather_traces->number_of_traces,
				     sizeof(trace_t), sp, &trace_comp);
			}
		} else {
			gather_traces->number_of_traces = 0;
		}
	}

	if(ensembles_list->ensembles->head != NULL) {
		doubly_linked_list_delete_node(ensembles_list->ensembles,
					       ensembles_list->ensembles->head,
					       &destroy_ensemble,
					       offsetof(ensemble_t, n));
	}

	for(k = 0; k < gather_traces->number_of_traces; k++){
		trace_t *tr = malloc(sizeof(trace_t));
		memcpy(tr, &(gather_traces->traces[k]), sizeof(trace_t));
		tr->data = NULL;
		ensemble_list_add_trace(tr, final_list, k);
	}
	if(gather_traces->number_of_traces > 0 ){
		free(gather_traces->traces);
		gather_traces->number_of_traces = 0;
	}


	return rc;
}

void
update_dkeys_list_array(seismic_object_oid_oh_t oid_oh, char *gather_name,
			operations_controllers_t *op_controller,
			int parent_idx, int *curr_idx)
{
	gather_obj_t *seismic_obj;
	char 	     **dkeys_list;
	int 	     num_of_gathers;
	int 	     rc;

	rc = gather_obj_open(&oid_oh, O_RDWR, op_controller, -1, curr_idx);
	DSG_ERROR(rc, "Opening gather object failed ", end);

	rc = gather_obj_fetch_entries(&seismic_obj, oid_oh, gather_name, 0);
	DSG_ERROR(rc, "Fetching gather object entry failed ", end2);

	rc = gather_obj_fetch_num_of_gathers(seismic_obj, NULL,
					     &(seismic_obj->number_of_gathers),
					     -1, curr_idx);
	DSG_ERROR(rc, "Fetching gather object number of gathers failed ",
		  end2);

	num_of_gathers = gather_obj_get_number_of_gathers(seismic_obj);

	dkeys_list = malloc(num_of_gathers * sizeof(char*));

	daos_key_desc_t 	*kds;
	daos_anchor_t 		anchor = { 0 };
	d_sg_list_t 		sglo;
	d_iov_t 		iov_temp;
	uint32_t 		nr = num_of_gathers + 2;
	char 			*temp_array;
	int 			temp_array_offset = 0;
	int 			keys_read = 0;
	int 			kds_i = 0;

	temp_array = malloc(nr * MAX_KEY_LENGTH * sizeof(char));
	kds = malloc((nr) * sizeof(daos_key_desc_t));
	sglo.sg_nr_out = sglo.sg_nr = 1;
	sglo.sg_iovs = &iov_temp;

	while (!daos_anchor_is_eof(&anchor)) {

		nr = (num_of_gathers + 2) - keys_read;
		d_iov_set(&iov_temp, temp_array + temp_array_offset,
			  nr * MAX_KEY_LENGTH);

		rc = daos_obj_list_dkey(seismic_obj->oid_oh.oh, DAOS_TX_NONE,
					&nr, &kds[keys_read], &sglo, &anchor,
					NULL);

		for (kds_i = 0; kds_i < nr; kds_i++) {
			temp_array_offset += kds[keys_read + kds_i].kd_key_len;
		}

		keys_read += nr;
		if (keys_read == (num_of_gathers + 2)) {
			break;
		}
	}
	DSG_ERROR(rc, "Listing seismic object akeys failed ");

	char 	temp[MAX_KEY_LENGTH] = "";
	int 	z;
	int 	off = 0;
	int 	k = 0;
	for (z = 0; z < keys_read; z++)	{
		strncpy(temp, &temp_array[off], kds[z].kd_key_len);
		if (strcmp(temp, DS_D_NGATHERS) == 0 ||
		    strcmp(temp, DS_D_SKEYS) == 0) {
			off += kds[z].kd_key_len;
			continue;
		}

		dkeys_list[k] = malloc((kds[z].kd_key_len + 1) * sizeof(char));
		strncpy(dkeys_list[k], &temp_array[off], kds[z].kd_key_len);
		dkeys_list[k][kds[z].kd_key_len] = '\0';
		off += kds[z].kd_key_len;
		k++;
	}

	if (num_of_gathers > 0) {
		seismic_obj->unique_values =
				malloc(num_of_gathers *	sizeof(generic_value));

		for (z = 0; z < num_of_gathers; z++){
			rc = gather_obj_fetch_gather_unique_value(seismic_obj,
								  NULL,
								  dkeys_list[z],
								  &seismic_obj->unique_values[z],
								  -1, curr_idx);
			DSG_ERROR(rc,"Fetching gather object unique value "
				  "failed ", end3);
		}

		generic_value_sort_params_t *sp =
				init_generic_value_sort_params(hdtype(seismic_obj->name),
							       1);

		rc = sort(seismic_obj->unique_values, num_of_gathers,
			  sizeof(generic_value), sp, &generic_value_compare);
		free(sp);

		unsigned int 	iod_nr;
		uint64_t 	iod_size;
		uint64_t 	rx_idx;
		uint64_t 	rx_nr;

		iod_size = sizeof(generic_value);
		rx_nr = num_of_gathers;
		rx_idx = 0;
		iod_nr = 1;

		gather_obj_init_io_parameters(seismic_obj, 0, DS_D_DKEYS_LIST,1);
		DSG_ERROR(rc, "Preparing gather object io parameters failed ",
			  end3);

		gather_obj_set_io_parameters(seismic_obj, DS_A_DKEYS_LIST,
					     DAOS_IOD_ARRAY, iod_size,
					     iod_nr, &rx_idx, &rx_nr,
					     (char*) seismic_obj->unique_values,
					     num_of_gathers * sizeof(generic_value));
		DSG_ERROR(rc, "Setting gather object io parameters failed ",
			  end3);

		gather_obj_update(seismic_obj, NULL, -1, curr_idx);
		DSG_ERROR(rc, "Updating gather object failed ");

end3: 		gather_obj_release_io_parameters(seismic_obj);
		DSG_ERROR(rc, "Releasing gather object io parameters failed ");
	}

	release_tokenized_array(dkeys_list, STRING, num_of_gathers);
	free(kds);
	free(temp_array);

end2: 	rc = gather_obj_close(seismic_obj, NULL, -1, curr_idx, 1);

end:	return;
}

void
send_receive_traces_arrays(ensemble_list *ensembles_list)
{
	MPI_Status status;
	int	   process_rank;
	int 	   num_of_ensembles = 0;
	int 	   num_of_processes;
	int 	   sender = 1;
	int 	   tag4 = 3;
	int 	   rc;
	int 	   j;
	int        i;

	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);

	if (process_rank == 0){
		for (j = 1; j <= num_of_processes - 1; j++) {
			read_traces_t 	*gather_traces;

			mpi_receive_integer(&num_of_ensembles, sender, tag4,
					    &status);
			if (num_of_ensembles > 0) {
				gather_traces =
					malloc(num_of_ensembles *
					       sizeof(read_traces_t));
			}
			for (i = 0; i < num_of_ensembles; i++) {
				mpi_receive_integer(&(gather_traces[i].number_of_traces),
						    sender, tag4, &status);

				gather_traces[i].traces =
						malloc(gather_traces[i].number_of_traces
							* sizeof(trace_t));
				mpi_receive_traces_array(gather_traces[i].traces,
							 gather_traces[i].number_of_traces,
							 sender, tag4, &status);
				trace_t 	*tr;
				int 		k;

				for (k = 0;k < gather_traces[i].number_of_traces; k++) {
					tr = malloc(sizeof(trace_t));

					memcpy(tr,&(gather_traces[i].traces[k]),
						sizeof(trace_t));

					if (gather_traces[i].traces[k].ns > 0) {
						tr->data =
							malloc(gather_traces[i].traces[k].ns
								* sizeof(float));
						mpi_receive_trace_data(tr->data,
								       tr->ns,
								       sender,
								       tag4,
								       &status);
					} else {
						tr->data = NULL;
					}
					rc = ensemble_list_add_trace(tr,
								     ensembles_list,
								     k);
				}
				free(gather_traces[i].traces);
			}
			if (num_of_ensembles > 0) {
				free(gather_traces);
			}
		}
		sender++;
	} else {
		mpi_send_integer(&(ensembles_list->ensembles->size), 0, tag4);

		ensemble_t 	*curr_ensemble;
		node_t 		*ensemble_node;

		if (ensembles_list->ensembles->size > 0){
			read_traces_t 	*gather_traces;
			gather_traces =
				malloc(ensembles_list->ensembles->size *
				       sizeof(read_traces_t));
			ensemble_node = ensembles_list->ensembles->head;

			for (i = 0; i < ensembles_list->ensembles->size; i++) {
				curr_ensemble = doubly_linked_list_get_object(ensemble_node,
									      offsetof(ensemble_t, n));

				gather_traces[i].number_of_traces = curr_ensemble->traces->size;
				ensemble_to_array(ensembles_list,&gather_traces[i].traces,i);
				mpi_send_integer(&(gather_traces[i].number_of_traces),
						 0, tag4);
				mpi_send_traces_array(gather_traces[i].traces,
						      gather_traces[i].number_of_traces,
						      0, tag4);
				for (j = 0; j < gather_traces[i].number_of_traces; j++)	{
					mpi_send_trace_data(gather_traces[i].traces[j].data,
							    gather_traces[i].traces[j].ns,
							    0, tag4);
					if (gather_traces[i].traces[j].data !=
					    NULL) {
						free(gather_traces[i].traces[j].data);
					}
				}
				free(gather_traces[i].traces);
				ensemble_node = ensemble_node->next;

			}
			free(gather_traces);
		}
	}
}

void
mpi_receive_integer(int *value, int sender, int message_tag,
		    MPI_Status *status)
{
	MPI_Recv(value, 1, MPI_INT, sender, message_tag, MPI_COMM_WORLD,
		 status);
}

void
mpi_receive_traces_array(trace_t *traces, int num_of_traces, int sender,
			 int message_tag, MPI_Status *status)
{
	MPI_Recv(traces, num_of_traces * sizeof(trace_t), MPI_BYTE, sender,
		 message_tag, MPI_COMM_WORLD, status);

}

void
mpi_receive_trace_data(float *trace_data, int num_of_samples, int sender,
		       int message_tag, MPI_Status *status)
{
	MPI_Recv(trace_data, num_of_samples, MPI_FLOAT, sender, message_tag,
		 MPI_COMM_WORLD, status);
}

void
mpi_send_integer(int *value, int receiver, int message_tag)
{
	MPI_Send(value, 1, MPI_INT, receiver, message_tag, MPI_COMM_WORLD);
}

void
mpi_send_traces_array(trace_t *traces, int num_of_traces, int receiver,
		      int message_tag)
{
	MPI_Send(traces, num_of_traces * sizeof(trace_t), MPI_BYTE, receiver,
		 message_tag, MPI_COMM_WORLD);

}

void
mpi_send_trace_data(float *trace_data, int num_of_samples, int receiver,
		    int message_tag)
{
	MPI_Send(trace_data, num_of_samples, MPI_FLOAT, receiver, message_tag,
		 MPI_COMM_WORLD);

}

void
mpi_broadcast_integer(int *value, int sender)
{
	MPI_Bcast(value, 1, MPI_INT, sender, MPI_COMM_WORLD);
}

void
mpi_broadcast_daos_oid(daos_obj_id_t *oid, int sender)
{
	MPI_Bcast(oid, sizeof(daos_obj_id_t), MPI_BYTE, sender,
		  MPI_COMM_WORLD);
}
