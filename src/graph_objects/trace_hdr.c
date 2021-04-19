/*
 * trace_hdr.c
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#include "graph_objects/trace_hdr.h"
#include "seismic_sources/segy.h"

int
trace_hdr_obj_create(trace_hdr_obj_t **trace_hdr_obj, int flags, trace_t *trace)
{
	int rc = 0;

	(*trace_hdr_obj) = malloc(sizeof(trace_hdr_obj_t));
	if ((*trace_hdr_obj) == NULL) {
		return ENOMEM;
	}

	rc = oid_gen(DAOS_OBJ_CLASS_ID, false, &((*trace_hdr_obj)->oid_oh.oid),
			false);
	DSG_ERROR(rc,"Generating Object id for trace hdr object failed \n",
		  end);

	(*trace_hdr_obj)->trace = trace;

	(*trace_hdr_obj)->daos_mode = get_daos_obj_mode(flags);

end:
	return rc;
}

int
trace_hdr_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		   operations_controllers_t *op_controller,
		   int parent_idx,int *curr_idx)
{
	int rc = 0;
	rc = open_object(oid_oh, op_controller, get_dfs()->coh, mode,
			 parent_idx, curr_idx);
	DSG_ERROR(rc,"Opening trace header object failed \n");

	return rc;
}

int
trace_hdr_obj_close(trace_hdr_obj_t *trace_hdr_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx,int *curr_idx, int release_trace)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_hdr_obj_get_id_oh(trace_hdr_obj);
	rc = close_object(oid_oh, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Closing trace header object failed", end);

	if (release_trace == 1) {
		rc = trace_hdr_obj_release(trace_hdr_obj);
		DSG_ERROR(rc,"Releasing Trace header object failed");
	}

end:
	return rc;
}

int
trace_hdr_obj_release(trace_hdr_obj_t *trace_hdr_obj)
{
	int rc = 0;

	if (trace_hdr_obj->trace != NULL) {
		rc = trace_destroy(trace_hdr_obj->trace);
		DSG_ERROR(rc,"Releasing trace header trace object failed");
	}

	if (trace_hdr_obj->io_parameters != NULL) {
		rc = trace_hdr_obj_release_io_parameters(trace_hdr_obj);
		DSG_ERROR(rc,"Releasing trace header io parameters failed");
	}

	free(trace_hdr_obj);

	return rc;
}

int
trace_hdr_object_punch(trace_hdr_obj_t *trace_hdr_obj,
		       operations_controllers_t *op_controller,
		       int punch_flags,
		       int parent_idx, int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_hdr_obj_get_id_oh(trace_hdr_obj);

	rc = destroy_object(oid_oh, punch_flags, op_controller, parent_idx,
			    curr_idx);
	DSG_ERROR(rc, "Destroying trace header object failed");

	rc = trace_hdr_obj_release(trace_hdr_obj);
	DSG_ERROR(rc, "Releasing trace header object memory failed");

	return rc;
}

seismic_object_oid_oh_t*
trace_hdr_obj_get_id_oh(trace_hdr_obj_t *trace_hdr_obj)
{
	return &(trace_hdr_obj->oid_oh);
}

int
trace_hdr_obj_fetch(trace_hdr_obj_t *trace_hdr_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_hdr_obj_get_id_oh(trace_hdr_obj);
	rc = fetch_object_entry(oid_oh, op_controller,
				trace_hdr_obj->io_parameters,
				parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching trace data object entry failed");
	return rc;
}

int
trace_hdr_obj_update(trace_hdr_obj_t *trace_hdr_obj,
		     operations_controllers_t *op_controller,
		     int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_hdr_obj_get_id_oh(trace_hdr_obj);
	rc = update_object_entry(oid_oh, op_controller,
				 trace_hdr_obj->io_parameters,
				 parent_idx, curr_idx);
	DSG_ERROR(rc, "Updating trace header object entry failed");
	return rc;
}

int
trace_hdr_obj_init_io_parameters(trace_hdr_obj_t *trace_hdr_obj,
				 uint64_t op_flags, char *dkey_name,
				 unsigned int num_of_iods_sgls)
{
	int rc = 0;

	rc = init_object_io_parameters(&(trace_hdr_obj->io_parameters),
				       op_flags, dkey_name, num_of_iods_sgls);
	DSG_ERROR(rc,"Initializing trace header object io parameters failed \n");

	return rc;
}

int
trace_hdr_object_set_io_parameters(trace_hdr_obj_t *trace_hdr_obj,
		char *akey_name, daos_iod_type_t type, uint64_t iod_size,
		unsigned int iod_nr, uint64_t *rx_idx, uint64_t *rx_nr,
		char *data, size_t data_size)
{

	int rc = 0;

	rc = set_object_io_parameters(trace_hdr_obj->io_parameters, akey_name,
				      type, iod_size, iod_nr, rx_idx, rx_nr,
				      data, data_size);
	DSG_ERROR(rc,"Setting trace header object io parameters failed \n");

	return rc;
}

int
trace_hdr_obj_release_io_parameters(trace_hdr_obj_t *trace_hdr_obj)
{
	int rc = 0;

	rc = release_object_io_parameters(trace_hdr_obj->io_parameters);
	DSG_ERROR(rc,"Releasing trace header io parameters failed");

	trace_hdr_obj->io_parameters = NULL;
	return rc;
}

int
trace_hdr_obj_get_mode(trace_hdr_obj_t *trace_hdr_obj)
{
	return trace_hdr_obj->daos_mode;
}

float*
trace_hdr_obj_get_data_array(trace_hdr_obj_t *trace_hdr_obj)
{
	return trace_hdr_obj->trace->data;
}

int
trace_hdr_update_headers(trace_hdr_obj_t* trace_hdr,trace_t *trace,
			 operations_controllers_t *op,
			 int parent_idx, int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	unsigned int 		iod_nr;
	uint64_t 		*rx_idx;
	uint64_t		*rx_nr;
	uint64_t		iod_size;
	int 			rc;

	rc = 0;

	oid_oh = trace_hdr_obj_get_id_oh(trace_hdr);
	trace->trace_header_obj = oid_oh->oid;
	int mode = trace_hdr_obj_get_mode(trace_hdr);
	trace_hdr_obj_open(oid_oh, mode, NULL, -1, curr_idx);
	iod_nr = 1;
	iod_size = 1;
	rx_idx = malloc(iod_nr * sizeof(uint64_t));
	rx_nr = malloc(iod_nr * sizeof(uint64_t));
	rx_idx[0] = 0;
	rx_nr[0] = SEGY_HDRBYTES;
	trace_hdr_obj_init_io_parameters(trace_hdr, 0, DS_D_TRACE_HEADER, 1);
	trace_hdr_object_set_io_parameters(trace_hdr, DS_A_TRACE_HEADER,
					   DAOS_IOD_ARRAY, iod_size, iod_nr,
					   rx_idx, rx_nr,
					   (char*) trace, SEGY_HDRBYTES);
	rc = trace_hdr_obj_update(trace_hdr, NULL, -1, curr_idx);
	DSG_ERROR(rc,"Failed to update trace header object headers\n");
	trace_hdr_obj_release_io_parameters(trace_hdr);
	trace_hdr_obj_close(trace_hdr, NULL, -1, curr_idx, 0);

	return rc;
}

int
trace_hdr_fetch_headers(trace_hdr_obj_t *trace_hdr,trace_t* trace,
			operations_controllers_t *op,
			int parent_idx, int *curr_idx)
{
	unsigned int 		iod_nr;
	uint64_t 		*rx_idx;
	uint64_t		*rx_nr;
	uint64_t		iod_size;
	int 			rc;

	rc = 0;
	iod_nr = 1;
	iod_size = 1;
	rx_idx = malloc(iod_nr * sizeof(uint64_t));
	rx_nr = malloc(iod_nr * sizeof(uint64_t));
	rx_idx[0] = 0;
	rx_nr[0] = SEGY_HDRBYTES;

	trace_hdr_obj_init_io_parameters(trace_hdr, 0, DS_D_TRACE_HEADER, 1);
	trace_hdr_object_set_io_parameters(trace_hdr, DS_A_TRACE_HEADER,
					   DAOS_IOD_ARRAY, iod_size, iod_nr,
					   rx_idx, rx_nr,(char*) trace,
					   SEGY_HDRBYTES);
	rc = trace_hdr_obj_fetch(trace_hdr, op, parent_idx, curr_idx);
	DSG_ERROR(rc,"Failed to fetch trace header object headers\n");
//	trace_hdr_obj_release_io_parameters(trace_hdr);

	free(rx_idx);
	free(rx_nr);

	return rc;

}

int
trace_hdr_fetch_custom_headers(trace_hdr_obj_t *trace_hdr,key_value_pair_t* kv,
			       operations_controllers_t *op,
			       int parent_idx, int *curr_idx,
			       char** keys, int num_of_keys,int trace_idx)
{

	unsigned int 	iod_nr;
	uint64_t 	iod_size;
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	void** 		buffers;
	int 		rc;
	int 		i;
	int 		index;

	rc = trace_hdr_obj_init_io_parameters(trace_hdr,0,DS_D_TRACE_HEADER,
					      num_of_keys);
	iod_size = 1;
	iod_nr = 1;
	buffers = malloc(num_of_keys * sizeof(void*));
	for(i = 0; i <num_of_keys; i++){
		index = getindex(keys[i]);
		rx_idx = hdr[index].offs;
		rx_nr = key_get_size(hdr[index].type);
		buffers[i] = malloc(rx_nr);
		rc = trace_hdr_object_set_io_parameters(trace_hdr,
							DS_A_TRACE_HEADER,
							DAOS_IOD_ARRAY,
							iod_size,iod_nr,&rx_idx,
							&rx_nr,(char*)buffers[i],
							rx_nr);
	}
	rc = trace_hdr_obj_fetch(trace_hdr,op,parent_idx,curr_idx);
	for(i = 0; i < num_of_keys; i++){
		generic_value_init(hdtype(keys[i]),
				   buffers[i],&(kv[i].values[trace_idx]));
	}
	rc = trace_hdr_obj_release_io_parameters(trace_hdr);
	for(i=0; i < num_of_keys; i++) {
		free(buffers[i]);
	}
	free(buffers);
	return 0;
}

int
key_value_print(key_value_pair_t* kv, int num_of_keys)
{
	int i;
	int j;
	if(kv == NULL){
		return -1;
	}
	for(i = 0; i < kv[0].num_of_values ; i++) {
		for(j = 0; j < num_of_keys; j++) {
			printf("%s: ",kv[j].key);
			printfval(hdtype(kv[j].key),kv[j].values[i]);
			printf("\t\t");
		}
		printf("\n");
	}

	return 0;
}


int
trace_hdr_obj_init(trace_hdr_obj_t **trace_hdr_obj)
{
	*trace_hdr_obj = (trace_hdr_obj_t*) malloc(sizeof(trace_hdr_obj_t));

	(*trace_hdr_obj)->trace = NULL;
	(*trace_hdr_obj)->io_parameters = NULL;

	return 0;
}
