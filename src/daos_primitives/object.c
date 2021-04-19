/*
 * object.c
 *
 *  Created on: Jan 23, 2021
 *      Author: mirnamoawad
 */

#include "daos_primitives/object.h"

int
init_object_io_parameters(object_io_parameters_t **io_parameters,
			  uint64_t op_flags, char *dkey_name,
			  unsigned int num_of_iods_sgls)
{
	(*io_parameters) = malloc(sizeof(object_io_parameters_t));

	d_iov_set(&(*io_parameters)->dkey_name, (void*) dkey_name,
			strlen(dkey_name));
	(*io_parameters)->flags = op_flags;
	(*io_parameters)->nr = num_of_iods_sgls;
	(*io_parameters)->iods = malloc(num_of_iods_sgls * sizeof(daos_iod_t));
	(*io_parameters)->sgls = malloc(num_of_iods_sgls * sizeof(d_sg_list_t));
	(*io_parameters)->curr_idx = 0;

	return 0;

}
int
set_object_io_parameters(object_io_parameters_t *io_parameters,
			 char *akey_name, daos_iod_type_t type,
			 uint64_t iod_size, unsigned int iod_nr,
			 uint64_t *rx_idx, uint64_t *rx_nr,
			 char *data, size_t data_size)
{
	int 	j;
	int 	i = (io_parameters)->curr_idx;
	char 	*akey = malloc(MAX_KEY_LENGTH * sizeof(char));
	strcpy(akey, akey_name);

	d_iov_set(&((io_parameters)->iods[i].iod_name), (void*) akey,
		strlen(akey));

	(io_parameters)->iods[i].iod_type = type;
	(io_parameters)->iods[i].iod_nr = iod_nr;
	(io_parameters)->iods[i].iod_size = iod_size;

	(io_parameters)->iods[i].iod_recxs = malloc(iod_nr*sizeof(daos_recx_t));

	if (type == DAOS_IOD_ARRAY) {
		for (j = 0; j < iod_nr; j++) {
			(io_parameters)->iods[i].iod_recxs[j].rx_idx =
					rx_idx[j];
			(io_parameters)->iods[i].iod_recxs[j].rx_nr = rx_nr[j];
		}
	} else {
		(io_parameters)->iods[i].iod_recxs = NULL;
		(io_parameters)->iods[i].iod_recxs = NULL;
	}

	(io_parameters)->sgls[i].sg_nr = 1;
	(io_parameters)->sgls[i].sg_nr_out = 1;
	(io_parameters)->sgls[i].sg_iovs = malloc(data_size * sizeof(char));
	d_iov_set((io_parameters)->sgls[i].sg_iovs, (void*) data, data_size);

	(io_parameters)->curr_idx++;

	return 0;
}

int
release_object_io_parameters(object_io_parameters_t *io_parameters)
{
	int i;

	for (i = 0; i < io_parameters->nr; i++) {
		free(io_parameters->iods[i].iod_name.iov_buf);
		free(io_parameters->iods[i].iod_recxs);
		free(io_parameters->sgls[i].sg_iovs);
	}
	free(io_parameters->iods);
	free(io_parameters->sgls);
	free(io_parameters);
	return 0;
}

int
fetch_object_entry(seismic_object_oid_oh_t *object_id_oh,
		   operations_controllers_t *op_controller,
		   object_io_parameters_t *io_parameters, int parent_ev_idx,
		   int *curr_idx)
{
	int 		rc = 0;

	if (op_controller != NULL) {
		if (parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc, "Creating new event failed in fetch "
				"object entry function",error1);
		*curr_idx = ((void*) op_controller->current_event
			    - (void*) op_controller->event_array)
			    / sizeof(daos_event_t);
		rc = daos_obj_fetch(object_id_oh->oh,
				*(op_controller->transaction_handle),
				io_parameters->flags,
				&(io_parameters->dkey_name), io_parameters->nr,
				io_parameters->iods, io_parameters->sgls,
				op_controller->maps,
				op_controller->current_event);
	} else {
		rc = daos_obj_fetch(object_id_oh->oh, DAOS_TX_NONE,
				io_parameters->flags,
				&(io_parameters->dkey_name), io_parameters->nr,
				io_parameters->iods, io_parameters->sgls, NULL,
				NULL);
	}

	char message[100];
	sprintf(message,"Failed to fetch data from <%s>, error"
		" code = %d\n", (char*)io_parameters->dkey_name.iov_buf, rc);
	DSG_ERROR(rc,message);

error1:
	return rc;
}

int
update_object_entry(seismic_object_oid_oh_t *object_id_oh,
		    operations_controllers_t *op_controller,
		    object_io_parameters_t *io_parameters, int parent_ev_idx,
		    int *curr_idx)
{
	int		rc = 0;
	if (op_controller != NULL) {
		if (parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc, "Creating new event failed in update "
			  "object entry function",error1);

		*curr_idx = ((void*) op_controller->current_event
			    - (void*) op_controller->event_array)
			    / sizeof(daos_event_t);

		rc = daos_obj_update(object_id_oh->oh,
				     *(op_controller->transaction_handle),
				     io_parameters->flags,
				     &(io_parameters->dkey_name),
				     io_parameters->nr,
				     io_parameters->iods, io_parameters->sgls,
				     op_controller->current_event);
	} else {

		rc = daos_obj_update(object_id_oh->oh, DAOS_TX_NONE,
				     io_parameters->flags,
				     &(io_parameters->dkey_name),
				     io_parameters->nr,
				     io_parameters->iods, io_parameters->sgls,
				     NULL);
	}

	char message[100];
	sprintf(message,"Failed to update data in <%s>, error"
		" code = %d\n", (char*)io_parameters->dkey_name.iov_buf, rc);
	DSG_ERROR(rc,message);

error1:
	return rc;
}

int
open_object(seismic_object_oid_oh_t *object_id_oh,
	    operations_controllers_t *op_controller, daos_handle_t coh,
	    unsigned int mode, int parent_ev_idx, int *curr_idx)
{
	int		rc = 0;

	if (op_controller != NULL) {
		if (parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc, "Creating new event failed in open "
				"object function",error1);
		*curr_idx = ((void*) op_controller->current_event
			    - (void*) op_controller->event_array)
			    / sizeof(daos_event_t);
		rc = daos_obj_open(coh, object_id_oh->oid, mode,
				   &(object_id_oh->oh),
				   op_controller->current_event);

	} else {
		rc = daos_obj_open(coh, object_id_oh->oid, mode,
				   &(object_id_oh->oh), NULL);

	}


	DSG_ERROR(rc, "Opening object failed", error1);

error1:
	return rc;
}

int
close_object(seismic_object_oid_oh_t *object_id_oh,
	     operations_controllers_t *op_controller, int parent_ev_idx,
	     int *curr_idx)
{
	int 		rc = 0;

	if (op_controller != NULL) {
		if (parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc, "Creating new event failed in close "
			  "object function",error1);
		*curr_idx = ((void*) op_controller->current_event
			    - (void*) op_controller->event_array)
			    / sizeof(daos_event_t);
		rc = daos_obj_close(object_id_oh->oh,
				    op_controller->current_event);
	} else {
		rc = daos_obj_close(object_id_oh->oh, NULL);
	}

	DSG_ERROR(rc, "Closing object failed", error1);

error1:
	return rc;

}

int
destroy_object(seismic_object_oid_oh_t *object_id_oh, uint64_t flags,
	       operations_controllers_t *op_controller, int parent_ev_idx,
	       int *curr_idx)
{
	int 		rc = 0;
	if (op_controller != NULL) {
		if (parent_ev_idx >= 0) {
			op_controller->parent_event =
				&op_controller->event_array[parent_ev_idx];
		} else {
			op_controller->parent_event = NULL;
		}
		rc = create_new_event(op_controller);
		DSG_ERROR(rc, "Creating new event failed in destroy "
			  "object function",error1);
		*curr_idx = ((void*) op_controller->current_event
			    - (void*) op_controller->event_array)
			    / sizeof(daos_event_t);
		rc = daos_obj_punch(object_id_oh->oh,
				    *(op_controller->transaction_handle), 0,
				    op_controller->current_event);
	} else {
		rc = daos_obj_punch(object_id_oh->oh, DAOS_TX_NONE, 0,
		NULL);
	}

	DSG_ERROR(rc, "Destroying object failed", error1);

error1:
	return rc;

}

