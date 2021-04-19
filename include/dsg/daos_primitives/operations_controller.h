/*
 * helpers.h
 *
 *  Created on: Jan 25, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_DAOS_PRIMITIVES_OPERATIONS_CONTROLLER_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_DAOS_PRIMITIVES_OPERATIONS_CONTROLLER_H_

#include <fcntl.h>
#include "daos.h"
#include "daos_fs.h"
#include "daos/pool.h"
#include "daos/placement.h"
#include "utilities/error_handler.h"

#define	DAOS_OBJ_CLASS_ID	OC_S1
#define MAX_KEY_LENGTH		2000

typedef enum {
	FETCH_OP,
	UPDATE_OP
}operation_type_t;

typedef struct seismic_object_oid_oh {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
}seismic_object_oid_oh_t;

/** Operations controller struct to be used to handle daos operations events
 *  & transactions.
 */
typedef struct operations_controllers {
	daos_handle_t 	*transaction_handle;
	daos_iom_t	*maps;
	daos_handle_t 	event_queue;
	daos_event_t 	*event_array;
	daos_event_t	*parent_event;
	daos_event_t	*current_event;
	int		max_inflight_event;
	int 		curr_index;
}operations_controllers_t;

/** initialize event queue and/or transaction
 *
 * /param[in]	op_controller		pointer to operations controllers
 * 					struct to be initialized.
 * /param[in]	init_th			flag indicating if a transaction handle
 * 					will be initialized or not.
 * /param[in]	init_eq			flag indicating if an event queue
 * 					will be initialized or not.
 * /param[in]	max_ev			maximum number of inflight events to be
 * 					initialized in one event queue.
 * /return	0 on success
 * 		error code otherwise.
 */
int
init_operations_controller(operations_controllers_t **op_controller,
			   int init_th, int init_eq, int max_ev);

/** Release event queue and any transaction allocated after committing any open
 *  transaction and waiting on any running events
 *
 * /param[in]	op_controller		pointer to operations controllers
 * 					struct to be released.
 *
 * /return	0 on success
 * 		error code otherwise.
 */
int
release_operations_controller(operations_controllers_t *op_controller);

/** Function responsible for creating new event in an event queue if maximum
 *  number of events is not reached or waiting on one event to complete
 *  to reuse it.
 *
 * /param[in]	op_controller		pointer to operations controller
 * 					struct having array of events.
 */
int
create_new_event(operations_controllers_t *op_controller);

/** Function responsible for waiting on all events in an event queue to complete
 *
 * /param[in]	op_controller		pointer to operations controller
 * 					struct having array of events.
 */
int
wait_all_events(operations_controllers_t *op_controller);

#endif
/*DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_DAOS_PRIMITIVES_OPERATIONS_CONTROLLER_H_*/
