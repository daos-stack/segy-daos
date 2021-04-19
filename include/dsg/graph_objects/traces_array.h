/*
 * traces_array.h
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACES_ARRAY_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACES_ARRAY_H_

#include "daos_primitives/operations_controller.h"
#include "daos_primitives/array.h"
#include "daos_primitives/dfs_helpers.h"
#include "graph_objects/gather.h"

typedef struct traces_array_obj {
	seismic_object_oid_oh_t 	oid_oh;
	array_io_parameters_t 		*io_parameters;
	daos_obj_id_t 			*oids;
	int				num_of_traces;
	int				daos_mode;
}traces_array_obj_t;

typedef struct gather_node gather_node_t;

/** Function responsible for creating and allocating a traces array
 *  object
 *
 * \param[in]	traces_array_obj	pointer to uninitialized traces_array object
 * \param[in]	flags			DAOS flags
 * \param[in]	oids			array of oids representing each trace
 * \param[in]   num_of_traces		number of traces
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_create(traces_array_obj_t **traces_array_obj, int flags,
		      	daos_obj_id_t *oids, int num_of_traces);

/** Function responsible for opening a seismic trace array object
 *
 * \param[in]	oid_oh		pointer to seismic object struct holding
 *  				daos array object id and open handle.
 * \param[in]   mode   		Open mode: DAOS_OO_RO/RW/EXCL/IO_RAND/IO_SEQ
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	cell_size	cell size required for opening an array object
 * \param[in]	chunk_size	chunkk size required for opening an array object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		      operations_controllers_t *op_controller,
		      int parent_idx, int *curr_idx, daos_size_t cell_size,
		      daos_size_t chunk_size);

/** Function responsible for closing a seismic trace array
 *  object
 *
 * \param[in]	trace_data_obj  pointer to trace array object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	release_trace 	flag to release trace array object once it's closed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_close(traces_array_obj_t *traces_array_obj,
		       operations_controllers_t *op_controller,
		       int parent_idx, int *curr_idx, int release_trace);

/* Function responsible for releasing an allocated seismic
 * trace array object
 *
 * \param[in]	trace_array_obj		seismic trace array object to be released
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_release(traces_array_obj_t *trace_array_obj);

/** Function responsible for destroying a seismic trace array
 *  object
 *
 * \param[in]	traces_array_obj	pointer to trace array object
 * \param[in]	op_controller		operation controller struct holding event
 *  					and transaction handles that will be used
 *  					during the daos array object update operation.
 * \param[in]	parent_idx		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx		current index of event initialized in case
 *  					daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_object_punch(traces_array_obj_t *traces_array_obj,
		          operations_controllers_t *op_controller,
		          int parent_idx, int *curr_idx);


/* Function responsible for getting an allocated seismic
 * trace array object's oid and open handle
 *
 * \param[in]	traces_array_obj	seismic trace array object
 *
 * \return	seismic object oid_oh struct pointer
 */
seismic_object_oid_oh_t*
traces_array_obj_get_id_oh(traces_array_obj_t *traces_array_obj);


/** Function responsible for fetching data from a seismic trace array
 *  object
 *
 * \param[in]	trace_array_obj	pointer to seismic trace array object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_fetch(traces_array_obj_t *traces_array_obj,
		       operations_controllers_t *op_controller,
		       int parent_idx, int *curr_idx);

/** Function responsible for updating data in a seismic trace array
 *  object
 *
 * \param[in]	trace_array_obj	pointer to seismic trace array object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_update(traces_array_obj_t *traces_array_obj,
			operations_controllers_t *op_controller,
			int parent_idx, int *curr_idx);

/** Function responsible for initializing trace array object io operation parameters.
 *
 * \param[in]	    trace_array_obj		Pointer to trace array object
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
traces_array_obj_init_io_parameters(traces_array_obj_t *traces_array_obj);


/** Function responsible for setting trace array object io operation parameters.
 *
 * \param[in]	   trace_array_obj	Pointer to trace array object
 *  					size of this array equals num_of_iods_sgls.
 * \param[in]	   arr_nr		number of elements to used in array of
 * 					ranges.
 * \param[in]	   rx_idx		array of records starting indexes.
 * \param[in]	   rg_len		array of range accessed during io.
 * \param[in]	   arr_nr_short_read	number of records that are short fetched
 * 					used during daos array read.
 * \param[in]	   data			array of data buffers to be used during io.
 * \param[in]	   data_size		array of size of each data buffer.
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
traces_array_obj_set_io_parameters(traces_array_obj_t *trace_array_obj,
			  	   uint64_t arr_nr, uint64_t *rg_idx,
				   uint64_t *rg_len, uint64_t arr_nr_short_read,
				   char *data, size_t data_size);

/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    trace_array_obj		pointer to trace_array_object
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
traces_array_obj_release_io_parameters(traces_array_obj_t *trace_array_obj);


/* Function responsible for getting an allocated seismic
 * trace array object's mode
 *
 * \param[in]	trace_array_obj	seismic trace array object
 *
 * \return	daos mode
 */
int
get_traces_array_obj_mode(traces_array_obj_t *traces_array_obj);


/* Function responsible for fetching the trace array object size
 *
 * \param[in]   trace_array_obj	pointer to trace_array object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[out]	size		size of trace array object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
get_traces_array_obj_size(traces_array_obj_t *traces_array_obj,
		          operations_controllers_t *op_controller,
			  int parent_idx, int *curr_idx, uint64_t *size);

/** Function responsible for updating trace oids stored in a seismic trace array
 *  object
 *
 * \param[in]	trace_array_obj	pointer to seismic trace array object
 * \param[in]	gather		gather node to be read from
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	offset		record index to write in (-1 if ignored)
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_update_oids(traces_array_obj_t *traces_array_obj,
			     gather_node_t *gather, int offset,
			     operations_controllers_t *op_controller,
			     int parent_idx, int *curr_idx);

/** Function responsible for fetching trace oids stored in a seismic trace array
 *  object
 *
 * \param[in]	trace_array_obj		pointer to seismic trace array object
 * \param[in]	op_controller		operation controller struct holding event
 *  					and transaction handles that will be used
 *  					during the daos array object update operation.
 * \param[in]	parent_idx		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx		current index of event initialized in case
 *  					daos events are used.
 * \param[out]	oids			array of oids to be written to
 * \param[in]	number_of_traces	number of traces oids to be fetched
 * \param[in]	start_index		starting index in traces array object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
traces_array_obj_fetch_oids(traces_array_obj_t *traces_array_obj,
			    operations_controllers_t *op_controller,
			    int parent_idx,int *curr_idx,
			    daos_obj_id_t *oids,
			    int number_of_traces,int start_index);

/** Function responsible for initializing empty traces array object
 *
 * \param[in]	traces_array_obj	double pointer to uninitialized trace_array_object
 *
 * \return      0 for successful allocation
 */
int
traces_array_obj_init(traces_array_obj_t **traces_array_obj);

#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACES_ARRAY_H_ */
