/*
 * array.c
 *
 *  Created on: Jan 25, 2021
 *      Author: mirnamoawad
 */

#include "daos_primitives/array.h"

int
init_array_io_parameters(array_io_parameters_t **io_parameters)
{
	(*io_parameters) = malloc(sizeof(array_io_parameters_t));
	(*io_parameters)->iod = malloc(sizeof(daos_array_iod_t));
	(*io_parameters)->sgl = malloc(sizeof(d_sg_list_t));

	return 0;
}

int
set_array_io_parameters(array_io_parameters_t *io_parameters,
			uint64_t arr_nr, uint64_t *rg_idx,
			uint64_t *rg_len, uint64_t arr_nr_short_read,
			char *data, size_t data_size)
{
	int i;
	io_parameters->iod->arr_nr_short_read = arr_nr_short_read;
	/** size of array of ranges */
	io_parameters->iod->arr_nr = arr_nr;
	io_parameters->iod->arr_rgs = malloc(arr_nr * sizeof(daos_range_t));
	for(i=0; i < arr_nr; i++) {
		io_parameters->iod->arr_rgs[i].rg_idx = rg_idx[i];
		io_parameters->iod->arr_rgs[i].rg_len = rg_len[i];
	}

	io_parameters->sgl->sg_nr = 1;
	io_parameters->sgl->sg_nr_out = 1;
	io_parameters->sgl->sg_iovs = malloc(data_size * sizeof(char));
	d_iov_set(io_parameters->sgl->sg_iovs, (void*)data, data_size);

	return 0;
}

int
release_array_io_parameters(array_io_parameters_t *io_parameters)
{
	int	i;

	free(io_parameters->iod->arr_rgs);
	free(io_parameters->sgl->sg_iovs);
	free(io_parameters->iod);
	free(io_parameters->sgl);
	free(io_parameters);
	return 0;
}

int
fetch_array_object_entry(seismic_object_oid_oh_t *object_id_oh,
		   	 operations_controllers_t *op_controller,
			 array_io_parameters_t *io_parameters,
			 int parent_ev_idx, int *curr_idx)
{
	int	rc = 0;

	if(op_controller != NULL) {
		if(parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc,"Creating new event failed in fetch "
				"array object entry function",error1);
		*curr_idx = ((void*)op_controller->current_event -
			     (void*)op_controller->event_array)/
			     sizeof(daos_event_t);

		rc = daos_array_read(object_id_oh->oh,
				     *(op_controller->transaction_handle),
				     io_parameters->iod, io_parameters->sgl,
				     op_controller->current_event);
	} else {
		rc =daos_array_read(object_id_oh->oh, DAOS_TX_NONE,
				    io_parameters->iod, io_parameters->sgl,
				    NULL);
	}

	DSG_ERROR(rc,"Failed to fetch array data");

error1:
	return rc;

}

int
update_array_object_entry(seismic_object_oid_oh_t *object_id_oh,
		   	  operations_controllers_t *op_controller,
			  array_io_parameters_t *io_parameters,
			  int parent_ev_idx, int *curr_idx)
{
	int	rc = 0;

	if(op_controller != NULL) {
		if(parent_ev_idx >= 0) {
			op_controller->parent_event =
				&(op_controller->event_array[parent_ev_idx]);
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc,"Creating new event failed in update "
				"array object entry function",error1);
		*curr_idx = ((void*)op_controller->current_event -
			     (void*)op_controller->event_array)/
			     sizeof(daos_event_t);
		rc = daos_array_write(object_id_oh->oh,
				      *(op_controller->transaction_handle),
				      io_parameters->iod,
				      io_parameters->sgl,
				      op_controller->current_event);
	} else {
		rc = daos_array_write(object_id_oh->oh, DAOS_TX_NONE,
				      io_parameters->iod, io_parameters->sgl,
				      NULL);
	}

	DSG_ERROR(rc,"Failed to update array data");

error1:
	return rc;

}

int
open_array_object(seismic_object_oid_oh_t *object_id_oh,
		  operations_controllers_t *op_controller, daos_handle_t coh,
		  unsigned int mode, daos_size_t cell_size,
		  daos_size_t chunk_size,
		  int parent_ev_idx, int *curr_idx)
{
	int	rc = 0;

	if(op_controller != NULL) {
		if(parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc,"Creating new event failed in open "
				"array object function",error1);
		*curr_idx = ((void*)op_controller->current_event -
			     (void*)op_controller->event_array)/
			     sizeof(daos_event_t);
		rc =
		daos_array_open_with_attr(coh, object_id_oh->oid,
					  *(op_controller->transaction_handle),
					  mode, cell_size,chunk_size,
					  &(object_id_oh->oh),
					  op_controller->current_event);
	} else {
		rc = daos_array_open_with_attr(coh, object_id_oh->oid,
					       DAOS_TX_NONE,
					       mode, cell_size,chunk_size,
					       &(object_id_oh->oh),
					       NULL);
	}
	DSG_ERROR(rc,"Failed to open array object");
error1:
	return rc;
}

int
close_array_object(seismic_object_oid_oh_t *object_id_oh,
		   operations_controllers_t *op_controller,
		   int parent_ev_idx, int *curr_idx)

{
	int	rc = 0;
	if(op_controller != NULL) {
		if(parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc,"Creating new event failed in close "
				"array object function",error1);
		*curr_idx = ((void*)op_controller->current_event -
			     (void*)op_controller->event_array)/
			     sizeof(daos_event_t);
		rc = daos_array_close(object_id_oh->oh,
				      op_controller->current_event);

	} else {
		rc = daos_array_close(object_id_oh->oh, NULL);
	}

	DSG_ERROR(rc,"Failed to close array object");
error1:
	return rc;
}

int
destroy_array_object(seismic_object_oid_oh_t *object_id_oh,
		     operations_controllers_t *op_controller,
		     int parent_ev_idx, int *curr_idx)
{
	int	rc = 0;

	if(op_controller != NULL) {
		if(parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc,"Creating new event failed in destroy "
				"array object function",error1);
		*curr_idx = ((void*)op_controller->current_event -
			     (void*)op_controller->event_array)/
			      sizeof(daos_event_t);
		rc = daos_array_destroy(object_id_oh->oh,
					*(op_controller->transaction_handle),
					op_controller->current_event);
	} else {
		rc = daos_array_destroy(object_id_oh->oh, DAOS_TX_NONE, NULL);
	}

	DSG_ERROR(rc,"Failed to destroy array object");
error1:
	return rc;
}

int
get_size_array_object(seismic_object_oid_oh_t *object_id_oh,
		      operations_controllers_t *op_controller,
		      uint64_t *size,
		      int parent_ev_idx, int *curr_idx)
{
	int	rc = 0;
	if(op_controller != NULL) {
		if(parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc,"Creating new event failed in get "
				"array object size function",error1);
		*curr_idx = ((void*)op_controller->current_event -
			     (void*)op_controller->event_array)/
			     sizeof(daos_event_t);
		rc = daos_array_get_size(object_id_oh->oh,
					 *(op_controller->transaction_handle),
					 size, op_controller->current_event);
	} else {
		rc = daos_array_get_size(object_id_oh->oh, DAOS_TX_NONE, size,
				    	 NULL);
	}

	DSG_ERROR(rc,"Failed to get array object size");
error1:
	return rc;
}
