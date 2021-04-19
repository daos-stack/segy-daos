/*
 * traces_array.c
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#include "graph_objects/traces_array.h"

int
traces_array_obj_create(traces_array_obj_t **traces_array_obj, int flags,
			daos_obj_id_t *oids, int num_of_traces)
{
	int rc = 0;

	(*traces_array_obj) = malloc(sizeof(traces_array_obj_t));

	if ((*traces_array_obj) == NULL) {
		return ENOMEM;
	}

	rc = oid_gen(DAOS_OBJ_CLASS_ID, false,&((*traces_array_obj)->oid_oh.oid),
		     true);
	DSG_ERROR(rc,"Generating Object id for traces array object failed \n",
		  end);

	(*traces_array_obj)->oids = malloc(num_of_traces* sizeof(daos_obj_id_t));
	memcpy((*traces_array_obj)->oids, oids, num_of_traces*
	       sizeof(daos_obj_id_t));
	(*traces_array_obj)->num_of_traces = num_of_traces;
	(*traces_array_obj)->daos_mode = get_daos_obj_mode(flags);

end:
	return rc;
}

int
traces_array_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		      operations_controllers_t *op_controller,
		      int parent_idx,int *curr_idx,
		      daos_size_t cell_size, daos_size_t chunk_size)
{
	int rc = 0;
	rc = open_array_object(oid_oh, op_controller, get_dfs()->coh, mode,
			       cell_size, chunk_size, parent_idx, curr_idx);
	DSG_ERROR(rc,"Opening traces array object failed \n");

	return rc;
}

int
traces_array_obj_close(traces_array_obj_t *traces_array_obj,
		       operations_controllers_t *op_controller, int parent_idx,
		       int *curr_idx, int release_data)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	rc = 0;

	rc = close_array_object(oid_oh, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Closing traces array object failed", end);

	if (release_data == 1) {
		rc = traces_array_obj_release(traces_array_obj);
		DSG_ERROR(rc,"Releasing Traces Array object failed");
	}

end:
	return rc;
}

seismic_object_oid_oh_t*
traces_array_obj_get_id_oh(traces_array_obj_t *traces_array_obj)
{
	return &(traces_array_obj->oid_oh);
}

int
traces_array_obj_release(traces_array_obj_t *traces_array_obj)
{
	int rc = 0;

	if (traces_array_obj->oids != NULL) {
		free(traces_array_obj->oids);
	}

	if (traces_array_obj->io_parameters != NULL) {
		rc = traces_array_obj_release_io_parameters(traces_array_obj);
		DSG_ERROR(rc,"Releasing traces array io parameters failed");
	}

	free(traces_array_obj);

	return rc;
}

int
traces_array_obj_release_io_parameters(traces_array_obj_t *traces_array_obj)
{
	int rc = 0;

	rc = release_array_io_parameters(traces_array_obj->io_parameters);

	DSG_ERROR(rc,"Releasing traces array io parameters failed");
	traces_array_obj->io_parameters = NULL;
	return rc;
}

int
traces_array_object_punch(traces_array_obj_t *traces_array_obj,
			  operations_controllers_t *op_controller,
			  int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	rc = 0;

	rc = destroy_array_object(oid_oh, op_controller, parent_idx, curr_idx);

	DSG_ERROR(rc, "Destroying traces array object failed");

	rc = traces_array_obj_release(traces_array_obj);
	DSG_ERROR(rc, "Releasing traces array object memory failed");

	return rc;
}

int
traces_array_obj_fetch_oids(traces_array_obj_t *traces_array_obj,
			    operations_controllers_t *op_controller,
			    int parent_idx,int *curr_idx,
			    daos_obj_id_t *oids,
			    int number_of_traces,int start_index)
{
	uint64_t 	arr_nr;
	uint64_t 	*rg_idx;
	uint64_t 	*rg_len;
	int 	 	rc;

	rc = 0;
	arr_nr = 1;
	rg_idx = malloc(arr_nr * sizeof(uint64_t));
	rg_len = malloc(arr_nr * sizeof(uint64_t));
	rg_idx[0] = start_index;
	rg_len[0] = number_of_traces;
	rc = traces_array_obj_init_io_parameters(traces_array_obj);
	rc = traces_array_obj_set_io_parameters(traces_array_obj, arr_nr,
						rg_idx,rg_len, 0, (char*)oids,
						number_of_traces *
						sizeof(daos_obj_id_t));
	rc = traces_array_obj_fetch(traces_array_obj, op_controller, -1, curr_idx);
	rc = traces_array_obj_release_io_parameters(traces_array_obj);
	free(rg_idx);
	free(rg_len);
	return rc;
}

int
traces_array_obj_fetch(traces_array_obj_t *traces_array_obj,
		       operations_controllers_t *op_controller, int parent_idx,
		       int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	rc = 0;

	rc = fetch_array_object_entry(oid_oh, op_controller,
				      traces_array_obj->io_parameters,
				      parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching traces array object entry failed");
	return rc;
}

int
traces_array_obj_update(traces_array_obj_t *traces_array_obj,
			operations_controllers_t *op_controller, int parent_idx,
			int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	rc = 0;

	rc = update_array_object_entry(oid_oh, op_controller,
				       traces_array_obj->io_parameters,
				       parent_idx, curr_idx);
	DSG_ERROR(rc, "Updating traces array object entry failed");
	return rc;
}

int
traces_array_obj_init_io_parameters(traces_array_obj_t *traces_array_obj)
{
	int rc = 0;

	rc = init_array_io_parameters((&traces_array_obj->io_parameters));

	DSG_ERROR(rc,"Initializing traces array object io parameters failed \n");

	return rc;
}

int
traces_array_obj_set_io_parameters(traces_array_obj_t *traces_array_obj,
				   uint64_t arr_nr, uint64_t *rg_idx,
				   uint64_t *rg_len,uint64_t arr_nr_short_read,
				   char *data, size_t data_size)
{
	int rc = 0;

	rc = set_array_io_parameters(traces_array_obj->io_parameters, arr_nr,
				     rg_idx, rg_len, arr_nr_short_read,
				     data, data_size);

	DSG_ERROR(rc,"Setting traces array object io parameters failed \n");
	return rc;
}

int
get_traces_array_obj_mode(traces_array_obj_t *traces_array_obj)
{
	return traces_array_obj->daos_mode;
}

int
get_traces_array_obj_size(traces_array_obj_t *traces_array_obj,
			  operations_controllers_t *op_controller,
			  int parent_idx, int *curr_idx, uint64_t *size)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	rc = 0;

	rc = get_size_array_object(oid_oh, op_controller, size, parent_idx,
				   curr_idx);
	DSG_ERROR(rc, "Failed to get array size from traces array object \n");
	return rc;
}

int
traces_array_obj_update_oids(traces_array_obj_t *traces_array_obj,
			     gather_node_t *gather, int offset,
			     operations_controllers_t *op_controller,
			     int parent_idx, int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	daos_size_t 		old_size = 0;
	uint64_t 		arr_nr;
	uint64_t 		*rg_idx;
	uint64_t 		*rg_len;
	int 			rc;

	oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	rc = 0;
	old_size = 0;

	rc = daos_array_get_size(oid_oh->oh, DAOS_TX_NONE, &old_size, NULL);
	DSG_ERROR(rc,"Finding old size of OIDS array object failed,"
				"error code = %d \n",end);

	if (gather->number_of_traces - old_size == 0) {
		return 0;
	}

	arr_nr = 1;
	rg_idx = malloc(arr_nr * sizeof(uint64_t));
	rg_len = malloc(arr_nr * sizeof(uint64_t));
	if(offset != -1){
		rg_idx[0] = offset;
	}
	else{
		rg_idx[0] = old_size;
	}
	rg_len[0] = (gather->number_of_traces - old_size);
	rc = traces_array_obj_init_io_parameters(traces_array_obj);
	rc = traces_array_obj_set_io_parameters(traces_array_obj, 1, rg_idx,
						rg_len, 0, (char*) gather->oids,
						(gather->number_of_traces - old_size)
						* sizeof(daos_obj_id_t));
	rc = traces_array_obj_update(traces_array_obj, NULL, -1, curr_idx);
	rc = traces_array_obj_release_io_parameters(traces_array_obj);
	free(rg_idx);
	free(rg_len);

end:
	return rc;

}

int
traces_array_obj_init(traces_array_obj_t **traces_array_obj)
{
	*traces_array_obj = (traces_array_obj_t*) malloc(sizeof(traces_array_obj_t));

	(*traces_array_obj)->oids = NULL;
	(*traces_array_obj)->io_parameters = NULL;

	return 0;
}

