/*
 * helpers.c
 *
 *  Created on: Jan 25, 2021
 *      Author: mirnamoawad
 */

#include "daos_primitives/operations_controller.h"

int
init_operations_controller(operations_controllers_t **op_controller,
			   int init_th, int init_eq, int max_ev)
{
	int	rc;

	(*op_controller) = malloc(sizeof(operations_controllers_t));
	(*op_controller)->event_array = malloc(max_ev * sizeof(daos_event_t));
	(*op_controller)->maps = NULL;
	(*op_controller)->curr_index = 0;
	(*op_controller)->transaction_handle = malloc(sizeof(daos_handle_t));
	*((*op_controller)->transaction_handle) = DAOS_TX_NONE;
	(*op_controller)->max_inflight_event = max_ev;
	rc = daos_eq_create(&(*op_controller)->event_queue);
//	if(rc != 0) {
//		err("Creating event queue failed, "
//		    "error code = %d \n", rc);
//		return rc;
//	}
	return 0;
}

int
release_operations_controller(operations_controllers_t *op_controller)
{
	int	rc;
	int	ret;
        do {
                ret = daos_eq_poll(op_controller->event_queue, 1,
                		   DAOS_EQ_WAIT, 1,
				   &(op_controller->current_event));
                if (rc == 0 && ret == 1)
                        rc = op_controller->current_event->ev_error;
        } while (ret == 1);

	if(rc == 0 && ret < 0) {
		rc = ret;
	}
        /** Destroy event queue */
        rc = daos_eq_destroy(op_controller->event_queue, 0);
//	if(rc != 0) {
//		err("Destroying event queue failed, "
//		    "error code = %d \n", rc);
//		return rc;
//	}
	free(op_controller->transaction_handle);
	free(op_controller->event_array);
	free(op_controller);
	return 0;
}

int
create_new_event(operations_controllers_t *op_controller)
{
	int	rc;

	if(op_controller->curr_index < op_controller->max_inflight_event) {
		op_controller->current_event =
		&(op_controller->event_array[op_controller->curr_index]);
		rc = daos_event_init(op_controller->current_event,
				     op_controller->event_queue,
				     op_controller->parent_event);
//		if(rc != 0) {
//			err("initializing daos event failed, "
//			    "error code = %d \n", rc);
//			return rc;
//		}
		(op_controller->curr_index)++;
	} else {
		rc = daos_eq_poll(op_controller->event_queue, 1,
				  DAOS_EQ_WAIT, 1,
				  &(op_controller->current_event));
                if (rc < 0)
                        return rc;
                if (rc == 0) {
                        rc = -DER_IO;
                        return rc;
                }
                /** Check if completed operation failed */
                if (op_controller->current_event->ev_error != DER_SUCCESS) {
                        rc = op_controller->current_event->ev_error;
                        return rc;
                }

                op_controller->current_event->ev_error = 0;
	}

	return 0;
}

int
wait_all_events(operations_controllers_t *op_controller)
{
	int	rc;
	int	ret;
        do {
                ret = daos_eq_poll(op_controller->event_queue, 1,
                		   DAOS_EQ_WAIT, 1,
				   &(op_controller->current_event));
                if (rc == 0 && ret == 1)
                        rc = op_controller->current_event->ev_error;
        } while (ret == 1);

	if(rc == 0 && ret < 0) {
		rc = ret;
	}
	op_controller->curr_index = 0;
	return 0;
}

