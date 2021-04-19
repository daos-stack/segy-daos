/*
 * trace_data.c
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#include "graph_objects/trace_data.h"

int
trace_data_obj_create(trace_data_obj_t **trace_data_obj, int flags,
		      float *trace_data, int ns)
{
	int rc = 0;

	(*trace_data_obj) = malloc(sizeof(trace_data_obj_t));

	if ((*trace_data_obj) == NULL) {
		return ENOMEM;
	}

	rc = oid_gen(DAOS_OBJ_CLASS_ID, false, &((*trace_data_obj)->oid_oh.oid),
			true);
	DSG_ERROR(rc, "Generating Object id for trace data object failed \n",
		  end);

	(*trace_data_obj)->trace_data = malloc(sizeof(float) * ns);
	memcpy((*trace_data_obj)->trace_data, trace_data, ns * sizeof(float));
	(*trace_data_obj)->ns = ns;
	(*trace_data_obj)->daos_mode = get_daos_obj_mode(flags);

end:	return rc;
}

int
trace_data_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		    operations_controllers_t *op_controller, int parent_idx,
		    int *curr_idx, daos_size_t cell_size,
		    daos_size_t chunk_size)
{
	int rc = 0;
	rc = open_array_object(oid_oh, op_controller, get_dfs()->coh, mode,
			       cell_size, chunk_size, parent_idx, curr_idx);
	DSG_ERROR(rc, "Opening trace data object failed \n");

	return rc;
}

int
trace_data_obj_close(trace_data_obj_t *trace_data_obj,
		     operations_controllers_t *op_controller,
		     int parent_idx,int *curr_idx, int release_data)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_data_obj_get_id_oh(trace_data_obj);

	rc = close_array_object(oid_oh, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Closing trace data object failed", end);

	if (release_data == 1) {
		rc = trace_data_obj_release(trace_data_obj);
		DSG_ERROR(rc, "Releasing Trace data object failed");
	}

end:	return rc;
}

seismic_object_oid_oh_t*
trace_data_obj_get_id_oh(trace_data_obj_t *trace_data_obj)
{
	return &(trace_data_obj->oid_oh);
}

int
trace_data_obj_release(trace_data_obj_t *trace_data_obj)
{
	int rc = 0;

	if (trace_data_obj->trace_data != NULL) {
		free(trace_data_obj->trace_data);
	}

	if (trace_data_obj->io_parameters != NULL) {
		rc = trace_data_obj_release_io_parameters(trace_data_obj);
		DSG_ERROR(rc, "Releasing trace data io parameters failed");
	}

	free(trace_data_obj);

	return rc;
}

int
trace_data_obj_release_io_parameters(trace_data_obj_t *trace_data_obj)
{
	int rc = 0;

	rc = release_array_io_parameters(trace_data_obj->io_parameters);

	DSG_ERROR(rc, "Releasing trace data io parameters failed");
	trace_data_obj->io_parameters = NULL;
	return rc;
}

int
trace_data_object_punch(trace_data_obj_t *trace_data_obj,
			operations_controllers_t *op_controller,
			int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_data_obj_get_id_oh(trace_data_obj);

	rc = destroy_array_object(oid_oh, op_controller, parent_idx, curr_idx);

	DSG_ERROR(rc, "Destroying trace data object failed");

	rc = trace_data_obj_release(trace_data_obj);
	DSG_ERROR(rc, "Releasing trace data object memory failed");

	return rc;
}

int
trace_data_obj_fetch(trace_data_obj_t *trace_data_obj,
		     operations_controllers_t *op_controller,
		     int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_data_obj_get_id_oh(trace_data_obj);

	rc = fetch_array_object_entry(oid_oh, op_controller,
				      trace_data_obj->io_parameters,
				      parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching trace data object entry failed");
	return rc;
}

int
trace_data_obj_update(trace_data_obj_t *trace_data_obj,
		      operations_controllers_t *op_controller,
		      int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_data_obj_get_id_oh(trace_data_obj);

	rc = update_array_object_entry(oid_oh, op_controller,
				       trace_data_obj->io_parameters,
				       parent_idx, curr_idx);
	DSG_ERROR(rc, "Updating trace data object entry failed");
	return rc;
}

int
trace_data_obj_init_io_parameters(trace_data_obj_t *trace_data_obj)
{
	int rc = 0;

	rc = init_array_io_parameters((&trace_data_obj->io_parameters));

	DSG_ERROR(rc, "Initializing trace data array io parameters failed \n");

	return rc;
}

int
trace_data_obj_set_io_parameters(trace_data_obj_t *trace_data_obj,
				 uint64_t arr_nr, uint64_t *rg_idx,
				 uint64_t *rg_len,uint64_t arr_nr_short_read,
				 char *data, size_t data_size)
{
	int rc = 0;

	rc = set_array_io_parameters(trace_data_obj->io_parameters, arr_nr,
				     rg_idx, rg_len, arr_nr_short_read,
				     data, data_size);

	DSG_ERROR(rc, "Setting trace data array io parameters failed \n");

	return rc;
}

int
trace_data_obj_get_mode(trace_data_obj_t *trace_data_obj)
{
	return trace_data_obj->daos_mode;
}

int
get_trace_data_obj_size(trace_data_obj_t *trace_data_obj,
			operations_controllers_t *op_controller,
			int parent_idx,int *curr_idx, uint64_t *size)
{
	seismic_object_oid_oh_t *oid_oh;
	int 			rc;

	rc = 0;

	oid_oh = trace_data_obj_get_id_oh(trace_data_obj);

	rc = get_size_array_object(oid_oh, op_controller, size, parent_idx,
				   curr_idx);
	DSG_ERROR(rc, "Failed getting trace data array size");
	return rc;
}

int
trace_data_update_data(trace_data_obj_t *trace_data, trace_t *trace,
		       operations_controllers_t *op_controller,
		       int parent_idx,int *curr_idx)
{
	seismic_object_oid_oh_t *oid_oh_trace_data;
	unsigned int 		arr_nr;
	uint64_t 		*rg_idx;
	uint64_t		*rg_len;
	int 			rc;


	if (trace->data == NULL) {
		trace_data->io_parameters = NULL;
		return 0;
	}
//	rc = trace_data_obj_create(&trace_data, O_RDWR, trace->data, trace->ns);
//	if (rc != 0) {
//		err("Creating Trace Data Object Failed"
//				" error code = %d \n", rc);
//		return rc;
//	}


	rc = 0;

	oid_oh_trace_data = trace_data_obj_get_id_oh(trace_data);
	int mode = trace_data_obj_get_mode(trace_data);
	trace_data_obj_open(oid_oh_trace_data, mode, NULL, -1, curr_idx,
			    sizeof(float), 200 * sizeof(float));
	arr_nr = 1;
	rg_idx = malloc(arr_nr * sizeof(uint64_t));
	rg_len = malloc(arr_nr * sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = trace->ns;

	trace_data_obj_init_io_parameters(trace_data);
	trace_data_obj_set_io_parameters(trace_data, arr_nr, rg_idx, rg_len, 0,
					 (char*) trace->data,
					 trace->ns * sizeof(float));

	rc = trace_data_obj_update(trace_data, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Failed to update trace data object");
	trace_data_obj_release_io_parameters(trace_data);
	trace_data_obj_close(trace_data, NULL, -1, curr_idx, 0);

	free(rg_idx);
	free(rg_len);
	return rc;

}

daos_obj_id_t
trace_data_oid_calculation(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid)
{

	daos_obj_id_t tr_data_oid;
	uint64_t ofeats;
	uint64_t hdr_val;

	tr_data_oid = *tr_hdr;
	tr_data_oid.hi++;

	ofeats = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_ARRAY_BYTE;

	/* TODO: add check at here, it should return error if user specified
	 * bits reserved by DAOS
	 */
	tr_data_oid.hi &= (1ULL << OID_FMT_INTR_BITS) - 1;

	/**
	 * | Upper bits contain
	 * | OID_FMT_VER_BITS (version)		 |
	 * | OID_FMT_FEAT_BITS (object features) |
	 * | OID_FMT_CLASS_BITS (object class)	 |
	 * | 96-bit for upper layer ...		 |
	 */
	hdr_val = ((uint64_t) OID_FMT_VER << OID_FMT_VER_SHIFT);
	hdr_val |= ((uint64_t) ofeats << OID_FMT_FEAT_SHIFT);
	hdr_val |= ((uint64_t) cid << OID_FMT_CLASS_SHIFT);
	tr_data_oid.hi |= hdr_val;

	return tr_data_oid;
}

int
traces_data_obj_fetch_data_in_list(ensemble_list *ensembles_list, int st_idx,
				   int num_of_ensembles,
				   operations_controllers_t *op_controller,
				   int parent_idx, int *curr_idx)
{

	trace_data_obj_t *trace_data_obj;
	ensemble_t 	 *curr_ensemble;
	uint64_t	 *rg_idx;
	uint64_t 	 *rg_len;
	node_t 		 *ensemble_node;
	trace_t 	 *curr_trace;
	node_t 		 *trace_node;
	int 		 rc;
	int 		 i;
	int 		 j;

	rc = 0;
	ensemble_node = ensembles_list->ensembles->head;

	curr_ensemble = doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));

	if (st_idx > 0) {
		for (i = 0; i < st_idx; i++) {
			ensemble_node = ensemble_node->next;
		}
		curr_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
	}

	i = 0;
	while (i < num_of_ensembles) {
		trace_node = curr_ensemble->traces->head;
		j = 0;

		trace_data_obj = malloc(curr_ensemble->traces->size *
					sizeof(trace_data_obj_t));
		rg_idx = malloc(curr_ensemble->traces->size * sizeof(uint64_t));
		rg_len = malloc(curr_ensemble->traces->size * sizeof(uint64_t));

		while (j < curr_ensemble->traces->size) {
			curr_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,
									      n));
			rg_idx[j] = 0;
			rg_len[j] = curr_trace->ns;

			curr_trace->data =
				malloc(curr_trace->ns * sizeof(float));

			trace_data_obj[j].oid_oh.oid =
					trace_data_oid_calculation(&(curr_trace->trace_header_obj),
								   DAOS_OBJ_CLASS_ID);
			trace_data_obj[j].trace_data = NULL;
//			trace_data_obj[j].io_parameters = NULL;
			rc = trace_data_obj_open(&(trace_data_obj[j].oid_oh),
						 O_RDWR, NULL, -1, curr_idx,
						 sizeof(float),
						 200 * sizeof(float));
			DSG_ERROR(rc, "Failed to open trace data object", end);

			rc = trace_data_obj_init_io_parameters(&trace_data_obj[j]);
			DSG_ERROR(rc,"Initializing trace data array io parameters failed \n",
				  end);
			rc = trace_data_obj_set_io_parameters(&trace_data_obj[j],
							      1, &rg_idx[j],
							      &rg_len[j], 0,
							      (char*)curr_trace->data,
							      curr_trace->ns *
							      sizeof(float));
			DSG_ERROR(rc,"Setting trace data array io parameters failed \n",
				  end);

			rc = trace_data_obj_fetch(&trace_data_obj[j],
						  op_controller, parent_idx,
						  curr_idx);
			DSG_ERROR(rc,"Failed to fetch from trace data object\n");
			trace_node = trace_node->next;

			j++;
		}
		if (op_controller != NULL) {
			rc = wait_all_events(op_controller);
		}
		j = 0;
		while (j < curr_ensemble->traces->size) {
			rc =
			trace_data_obj_release_io_parameters(&trace_data_obj[j]);

			DSG_ERROR(rc,"Failed to release trace data io parameters\n");

			rc = trace_data_obj_close(&trace_data_obj[j], NULL,
						  parent_idx, curr_idx, 0);

			DSG_ERROR(rc, "Failed to close trace data object");
			j++;
		}
		free(trace_data_obj);
		free(rg_idx);
		free(rg_len);

		ensemble_node = ensemble_node->next;
		curr_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
		i++;
	}

end:	return rc;
}

int
trace_data_obj_init(trace_data_obj_t **trace_data_obj)
{
	*trace_data_obj = (trace_data_obj_t*) malloc(sizeof(trace_data_obj_t));

	(*trace_data_obj)->trace_data = NULL;
	(*trace_data_obj)->io_parameters = NULL;

	return 0;
}

