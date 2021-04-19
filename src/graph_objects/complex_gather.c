/*
 * complex_gather.c
 *
 *  Created on: Mar 30, 2021
 *      Author: omar
 */

#include "graph_objects/complex_gather.h"

int
complex_gather_obj_create(complex_gather_obj_t **complex_gather_obj, int flags)
{
	int rc = 0;
	(*complex_gather_obj) = malloc(sizeof(complex_gather_obj_t));
	if ((*complex_gather_obj) == NULL) {
		return ENOMEM;
	}
	rc = oid_gen(DAOS_OBJ_CLASS_ID, false, &((*complex_gather_obj)->oid_oh.oid),
			false);
	if (rc != 0) {
		printf("Generating Object id for complex gather object failed,"
				" error code = %d \n", rc);
		return rc;
	}
	(*complex_gather_obj)->daos_mode = get_daos_obj_mode(flags);
	(*complex_gather_obj)->io_parameters = NULL;

	return rc;
}

int
complex_gather_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		operations_controllers_t *op_controller, int parent_idx,
		int *curr_idx)
{
	int rc = 0;
	rc = open_object(oid_oh, op_controller, get_dfs()->coh, mode,
			parent_idx, curr_idx);
	if (rc != 0) {
		printf("Opening complex gather object failed,"
				" error code = %d \n", rc);
		return rc;
	}

	return rc;
}

int
complex_gather_obj_close(complex_gather_obj_t *complex_gather_obj,
		operations_controllers_t *op_controller, int parent_idx,
		int *curr_idx, int release_gather)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = complex_gather_obj_get_oid_oh(complex_gather_obj);

	rc = close_object(oid_oh, op_controller, parent_idx, curr_idx);
	if (rc != 0) {
		printf("Closing gather object failed,"
				" error code = %d \n", rc);
		return rc;
	}

	if (release_gather == 1) {
		rc = complex_gather_obj_release(complex_gather_obj);
		if (rc != 0) {
			printf("Releasing gather object memory failed,"
					" error code = %d \n", rc);
			return rc;
		}
	}
	return rc;
}

int
complex_gather_obj_release(complex_gather_obj_t *complex_gather_obj)
{
	int rc = 0;

	if (complex_gather_obj->io_parameters != NULL) {
		rc = complex_gather_obj_release_io_parameters(complex_gather_obj);
		if (rc != 0) {
			printf("Releasing complex gather object io parameters failed,"
					" error code = %d \n", rc);
			return rc;
		}
	}

	free(complex_gather_obj);

	return rc;
}

int
complex_gather_obj_punch(complex_gather_obj_t *complex_gather_obj,
		operations_controllers_t *op_controller, int punch_flags,
		int parent_idx, int *curr_idx)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = complex_gather_obj_get_oid_oh(complex_gather_obj);

	rc = destroy_object(oid_oh, punch_flags, op_controller, parent_idx,
			curr_idx);
	if (rc != 0) {
		printf("Punching complex gather object io parameters failed,"
				" error code = %d \n", rc);
		return rc;
	}

	rc = complex_gather_obj_release(complex_gather_obj);
	if (rc != 0) {
		printf("Releasing complex gather failed,"
				" error code = %d \n", rc);
		return rc;
	}

	return rc;
}

int
complex_gather_obj_fetch(complex_gather_obj_t *complex_gather_obj,
		operations_controllers_t *op_controller, int parent_idx,
		int *curr_idx)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = complex_gather_obj_get_oid_oh(complex_gather_obj);

	rc = fetch_object_entry(oid_oh, op_controller,
			complex_gather_obj->io_parameters, parent_idx, curr_idx);
	if (rc != 0) {
		printf("Fetching from complex gather object failed,"
				" error code = %d \n", rc);
		return rc;
	}
	return rc;
}

int
complex_gather_obj_update(complex_gather_obj_t *complex_gather_obj,
		operations_controllers_t *op_controller, int parent_idx,
		int *curr_idx)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = complex_gather_obj_get_oid_oh(complex_gather_obj);

	rc = update_object_entry(oid_oh, op_controller,
			complex_gather_obj->io_parameters, parent_idx, curr_idx);
	if (rc != 0) {
		printf("Updating complex gather object failed,"
				" error code = %d \n", rc);
		return rc;
	}
	return rc;
}

seismic_object_oid_oh_t*
complex_gather_obj_get_oid_oh(complex_gather_obj_t *complex_gather_obj)
{
	return &(complex_gather_obj->oid_oh);
}

int
complex_gather_obj_get_mode(complex_gather_obj_t *complex_gather_obj)
{
	return complex_gather_obj->daos_mode;
}

int
complex_gather_obj_init_io_parameters(complex_gather_obj_t *complex_gather_obj, uint64_t op_flags,
		char *dkey_name, unsigned int num_of_iods_sgls)
{
	int rc = 0;

	rc = init_object_io_parameters(&(complex_gather_obj->io_parameters), op_flags,
			dkey_name, num_of_iods_sgls);
	if (rc != 0) {
		printf("Initializing complex gather object io parameters failed,"
				" error code = %d \n", rc);
		return rc;
	}

	return rc;
}

int
complex_gather_obj_set_io_parameters(complex_gather_obj_t *complex_gather_obj, char *akey_name,
		daos_iod_type_t type, uint64_t iod_size, unsigned int iod_nr,
		uint64_t *rx_idx, uint64_t *rx_nr, char *data, size_t data_size)
{

	int rc = 0;

	rc = set_object_io_parameters(complex_gather_obj->io_parameters, akey_name,
			type, iod_size, iod_nr, rx_idx, rx_nr, data, data_size);
	if (rc != 0) {
		printf("Setting complex gather object io parameters failed,"
				" error code = %d \n", rc);
		return rc;
	}

	return rc;
}

int
complex_gather_obj_release_io_parameters(complex_gather_obj_t *complex_gather_obj)
{
	int rc = 0;

	rc = release_object_io_parameters(complex_gather_obj->io_parameters);
	if (rc != 0) {
		printf("Releasing gather object io parameters failed,"
				" error code = %d \n", rc);
		return rc;
	}
	complex_gather_obj->io_parameters = NULL;
	return rc;
}

int
complex_gather_obj_fetch_gather_num_of_traces(
		complex_gather_obj_t *complex_gather_obj,
		operations_controllers_t *op_controller, char *gather_dkey_name,
		int *num_of_traces, int parent_idx, int *curr_idx)
{
	int rc = 0;

	rc = complex_gather_obj_init_io_parameters(complex_gather_obj, 0, gather_dkey_name, 1);
	if (rc != 0) {
		err("Initializing complex gather object io parameters failed"
				" error code = %d\n", rc);
		return rc;
	}
	rc = complex_gather_obj_set_io_parameters(complex_gather_obj, DS_A_NTRACES,
			DAOS_IOD_SINGLE, sizeof(int), 1, NULL, NULL,
			(char*) num_of_traces, sizeof(int));
	if (rc != 0) {
		err("Setting complex gather object io parameters failed"
				" error code = %d\n", rc);
		return rc;
	}

	rc = complex_gather_obj_fetch(complex_gather_obj, NULL, -1, curr_idx);
	if (rc != 0) {
		err("Fetching complex gather object num of traces failed"
				" error code = %d\n", rc);
		return rc;
	}
	rc = complex_gather_obj_release_io_parameters(complex_gather_obj);
	if (rc != 0) {
		err("Releasing complex gather object io parameters failed"
				" error code = %d\n", rc);
		return rc;
	}
	return rc;
}

int
complex_gather_obj_fetch_gather_traces_array_oid(complex_gather_obj_t *complex_gather_obj,
				operations_controllers_t *op_controller,
				char* gather_dkey_name,
				daos_obj_id_t* oid,
				int parent_idx, int *curr_idx)
{
	int rc = 0;
	unsigned int iod_nr;
	uint64_t iod_size;
	iod_nr = 1;
	iod_size = sizeof(daos_obj_id_t);

	complex_gather_obj_init_io_parameters(complex_gather_obj, 0, gather_dkey_name, 1);
	complex_gather_obj_set_io_parameters(complex_gather_obj, DS_A_GATHER_TRACE_OIDS,
			DAOS_IOD_SINGLE, iod_size, iod_nr, NULL, NULL,
			(char*) oid, sizeof(daos_obj_id_t));

	rc = complex_gather_obj_fetch(complex_gather_obj, NULL, -1, curr_idx);
	if (rc != 0) {
		err("Fetching complex gather object num of gathers failed"
				" error code = %d\n", rc);
		return rc;
	}
	rc = complex_gather_obj_release_io_parameters(complex_gather_obj);
	if (rc != 0) {
		err("Releasing complex gather object io parameters failed"
				" error code = %d\n", rc);
		return rc;
	}
	return rc;
}

int
complex_gather_obj_update_gather_num_of_traces(complex_gather_obj_t *complex_gather_obj, char *dkey,
				       int *num_of_traces,
				       operations_controllers_t *op_controller,
				       int parent_idx, int *curr_idx)
{
	uint64_t iod_nr;
	uint64_t iod_size;
	int rc;

	iod_nr = 1;
	iod_size = sizeof(int);

	rc = complex_gather_obj_init_io_parameters(complex_gather_obj, 0, dkey, 1);

	rc = complex_gather_obj_set_io_parameters(complex_gather_obj, DS_A_NTRACES,
			DAOS_IOD_SINGLE, iod_size, iod_nr, NULL, NULL,
			(char*) num_of_traces, sizeof(int));

	rc = complex_gather_obj_update(complex_gather_obj, NULL, -1, curr_idx);

	if (rc != 0) {
		err("Writing Traces Number under %s failed"
				" error code = %d\n", dkey, rc);
		return rc;
	}
	rc = complex_gather_obj_release_io_parameters(complex_gather_obj);

	return rc;
}

int
complex_gather_obj_update_gather_traces_array_oid(complex_gather_obj_t *complex_gather_obj, char *dkey,
					  daos_obj_id_t *oid,
					  operations_controllers_t *op_controller,
					  int parent_idx, int *curr_idx)
{
	uint64_t iod_nr;
	uint64_t iod_size;
	int rc;
	iod_nr = 1;
	iod_size = sizeof(daos_obj_id_t);
	rc = complex_gather_obj_init_io_parameters(complex_gather_obj, 0, dkey, 1);
	rc = complex_gather_obj_set_io_parameters(complex_gather_obj, DS_A_GATHER_TRACE_OIDS,
			DAOS_IOD_SINGLE, iod_size, iod_nr, NULL, NULL,
			(char*) oid, sizeof(daos_obj_id_t));
	rc = complex_gather_obj_update(complex_gather_obj, op_controller, -1, curr_idx);
	if (rc != 0) {
		err("Writing Traces Number under root failed"
				" error code = %d\n", rc);
		return rc;
	}
	rc = complex_gather_obj_release_io_parameters(complex_gather_obj);
	return rc;
}



int
complex_gather_obj_fetch_entries(complex_gather_obj_t **complex_gather_obj,
				 seismic_object_oid_oh_t oid_oh)
{
	int curr_idx;
	int rc = 0;
	int i;
	(*complex_gather_obj) = malloc(sizeof(complex_gather_obj_t));
	(*complex_gather_obj)->oid_oh = oid_oh;
	(*complex_gather_obj)->daos_mode = O_RDWR;
	(*complex_gather_obj)->io_parameters = NULL;
	return rc;
}

int
complex_gather_obj_fetch_complex_gather(seismic_object_oid_oh_t complex_oid,
			        ensemble_list *final_list, int index)
{
	complex_gather_obj_t 		*complex_gather_obj;
	int							curr_idx;
	int							rc=0;

	char temp[10] = "";
	sprintf(temp, "%d", index);
	rc = complex_gather_obj_open(&complex_oid, O_RDWR, NULL, -1, &curr_idx);
	rc = complex_gather_obj_fetch_entries(&complex_gather_obj, complex_oid);
	rc = complex_gather_obj_fetch_gather_metadata_and_traces(complex_gather_obj, temp,
							 NULL, final_list,
							 -1, &curr_idx);
	rc = complex_gather_obj_close(complex_gather_obj, NULL, -1, &curr_idx, 1);

	return rc;
}

int
complex_gather_obj_fetch_gather_metadata_and_traces(complex_gather_obj_t *complex_gather_obj,
		char *dkey, operations_controllers_t *op_controller,
		ensemble_list *ensembles_list, int parent_idx, int *curr_idx)
{
	traces_array_obj_t 		*traces_array_obj;
	int				num_of_traces;
	int				rc = 0;
	int				i;
	int				num_of_processes;
	int				process_rank;
	int				chunksize;
	int				offset;
	int 				st_idx;
	int 				nrecords;
	int 				mpi_initialized = 0;


#ifdef MPI_BUILD
	mpi_initialized = 1;
	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);
#endif

	traces_array_obj = malloc(sizeof(traces_array_obj_t));
	rc = complex_gather_obj_fetch_gather_num_of_traces(complex_gather_obj, NULL, dkey,
			&num_of_traces, -1, curr_idx);
	if (rc != 0) {
		err("Fetching complex gather object num of traces failed"
				" error code = %d\n", rc);
		return rc;
	}
	rc = complex_gather_obj_fetch_gather_traces_array_oid(complex_gather_obj, NULL, dkey,
			&(traces_array_obj->oid_oh.oid), -1, curr_idx);
	if (rc != 0) {
		err("Fetching complex gather object traces array oid failed"
				" error code = %d\n", rc);
		return rc;
	}


	if (mpi_initialized)
	{
		calculate_chunksize(num_of_processes, num_of_traces,&offset,
				        &chunksize);
		if(process_rank == 0) {
			traces_array_obj->oids = malloc(offset * sizeof(daos_obj_id_t));
			st_idx = 0;
			nrecords = offset;
		} else {
			traces_array_obj->oids = malloc(chunksize * sizeof(daos_obj_id_t));
			st_idx = offset + ((process_rank - 1) * chunksize);
			nrecords = chunksize;
		}
	}
	else{
		nrecords = num_of_traces;
		st_idx = 0;
		traces_array_obj->oids = malloc(num_of_traces * sizeof(daos_obj_id_t));
	}
	rc = traces_array_obj_open(&traces_array_obj->oid_oh, O_RDWR, NULL, -1,
			curr_idx, sizeof(daos_obj_id_t),
			500 * sizeof(daos_obj_id_t));
	if (rc != 0) {
		err("Opening traces array object failed"
				" error code = %d\n", rc);
		return rc;
	}
	rc = traces_array_obj_fetch_oids(traces_array_obj, NULL, -1, curr_idx,
			traces_array_obj->oids, nrecords, st_idx);
	if (rc != 0) {
		err("Fetching array oids failed error code = %d\n", rc);
		return rc;
	}
	trace_hdr_obj_t **trace_hdr = malloc(nrecords *
					     sizeof(trace_hdr_obj_t *));
	trace_t	**traces = malloc(nrecords * sizeof(trace_t*));
	seismic_object_oid_oh_t oid_oh;
	for (i = 0; i < nrecords; i++) {
		trace_hdr[i] = malloc(sizeof(trace_hdr_obj_t));
		oid_oh.oid = traces_array_obj->oids[i];
		trace_hdr[i]->oid_oh.oid = traces_array_obj->oids[i];
		rc = trace_hdr_obj_open(&(trace_hdr[i]->oid_oh), O_RDWR, NULL,
				-1, curr_idx);
		if (rc != 0) {
			err("Opening trace header object failed,"
					" error code = %d \n", rc);
			return rc;
		}
		trace_hdr[i]->trace = NULL;
		traces[i] = malloc(sizeof(trace_t));
		traces[i]->data = NULL;
		traces[i]->trace_header_obj = oid_oh.oid;
		rc = trace_hdr_fetch_headers(trace_hdr[i], traces[i],
				op_controller, parent_idx, curr_idx);
		if (rc != 0) {
			err("Fetching trace headers object failed"
					" error code = %d\n", rc);
			return rc;
		}
	}
	if (op_controller != NULL) {
		rc = wait_all_events(op_controller);
	}
	for (i = 0; i < nrecords; i++) {
		rc = ensemble_list_add_trace(traces[i], ensembles_list, i);
		if (rc != 0) {
			err("Adding trace to ensemble list failed"
					" error code = %d\n", rc);
			return rc;
		}
		rc = trace_hdr_obj_release_io_parameters(trace_hdr[i]);

		rc = trace_hdr_obj_close(trace_hdr[i], NULL, -1, curr_idx, 1);
		if (rc != 0) {
			err("Closing traces hdr object failed"
					" error code = %d\n", rc);
			return rc;
		}
	}
	rc = traces_array_obj_close(traces_array_obj, NULL, -1,
				    curr_idx, 1);
	if (rc != 0) {
		err("Closing traces array object failed"
				" error code = %d\n", rc);
		return rc;
	}
	free(trace_hdr);
	free(traces);

	return rc;
}

int
complex_gather_obj_update_gather_metadata_and_traces(complex_gather_obj_t *complex_gather_obj,
		char *dkey, operations_controllers_t *op_controller,
		ensemble_list *ensembles, int ensemble_index, int parent_idx,
		int *curr_idx)
{
	int rc = 0;
	if (ensemble_index >= ensembles->ensembles->size) {
		return -1;
	}
	node_t *temp_ensemble = ensembles->ensembles->head;
	int i = 0;
	while (i < ensemble_index) {
		i++;
		temp_ensemble = temp_ensemble->next;
	}
	ensemble_t *en = (ensemble_t*) doubly_linked_list_get_object(
			temp_ensemble, offsetof(ensemble_t, n));
	node_t *temp_trace = en->traces->head;
	trace_t *tr;
	seismic_object_oid_oh_t oid_oh;
	trace_hdr_obj_t **trace_hdr = malloc(
			en->traces->size * sizeof(trace_hdr_obj_t*));
	for (i = 0; i < en->traces->size; i++) {
		tr = (trace_t*) doubly_linked_list_get_object(temp_trace,
				offsetof(trace_t, n));
		trace_hdr[i] = malloc(sizeof(trace_hdr_obj_t));
		trace_hdr[i]->oid_oh.oid = tr->trace_header_obj;
		trace_hdr[i]->trace = NULL;
		trace_hdr[i]->daos_mode = O_RDWR;
		rc = trace_hdr_update_headers(trace_hdr[i], tr, op_controller,
				-1, curr_idx);
		trace_hdr_obj_release(trace_hdr[i]);
		temp_trace = temp_trace->next;
	}

	free(trace_hdr);
	return rc;
}

int
complex_gather_obj_get_dkey_num_of_akeys(complex_gather_obj_t *complex_gather_obj, char *dkey_name,
		int expected_num_of_keys)
{
	daos_key_desc_t *kds;
	daos_key_t dkey;
	uint32_t nr = expected_num_of_keys;
	d_iov_t iov_temp;
	char *temp_array;
	int temp_array_offset = 0;
	int rc;

	d_iov_set(&dkey, (void*) dkey_name, strlen(dkey_name));

	int keys_read = 0;
	daos_anchor_t anchor = { 0 };
	d_sg_list_t sglo;
	int kds_i = 0;

	temp_array = malloc(nr * MAX_KEY_LENGTH * sizeof(char));
	kds = malloc((nr) * sizeof(daos_key_desc_t));
	sglo.sg_nr_out = sglo.sg_nr = 1;
	sglo.sg_iovs = &iov_temp;

	while (!daos_anchor_is_eof(&anchor)) {
		nr = nr - keys_read;
		d_iov_set(&iov_temp, temp_array + temp_array_offset,
				nr * MAX_KEY_LENGTH);
		rc = daos_obj_list_akey(complex_gather_obj_get_oid_oh(complex_gather_obj)->oh,
		DAOS_TX_NONE, &dkey, &nr, &kds[keys_read], &sglo, &anchor,
				NULL);

		for (kds_i = 0; kds_i < nr; kds_i++) {
			temp_array_offset += kds[keys_read + kds_i].kd_key_len;
		}
		if (nr == 0) {
			break;
		}
		keys_read += nr;
	}
	if (rc != 0) {
		err("Listing complex gather object akeys failed,"
				" error code = %d\n",rc);
	}
	free(kds);
	free(temp_array);

	return keys_read;
}

int
complex_gather_obj_init(complex_gather_obj_t **complex_obj)
{
	*complex_obj = (complex_gather_obj_t*) malloc(sizeof(complex_gather_obj_t));

	(*complex_obj)->io_parameters = NULL;

	return 0;
}

