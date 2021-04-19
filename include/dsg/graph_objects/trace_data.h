/*
 * trace_data.h
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACE_DATA_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACE_DATA_H_

#include "daos_primitives/operations_controller.h"
#include "data_types/trace.h"
#include "daos_primitives/array.h"
#include "daos_primitives/dfs_helpers.h"
#include "data_types/ensemble.h"

typedef struct trace_data_obj {
	/** DAOS object ID & open handle*/
	seismic_object_oid_oh_t oid_oh;

	array_io_parameters_t *io_parameters;
	/** daos object mode */
	int	daos_mode;
	int	ns;

	float *trace_data;
}trace_data_obj_t;


/** Function responsible for creating and allocating a trace data
 *  object
 *
 * \param[in]	trace_data_obj		pointer to uninitialized trace_data object
 * \param[in]	flags			DAOS flags
 * \param[in]	trace_data		array of floats representing trace data
 * \param[in]   ns			size of data
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_data_obj_create(trace_data_obj_t **trace_data_obj, int flags,
		      float *trace_data, int ns);


/** Function responsible for opening a seismic trace data object
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
trace_data_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		    operations_controllers_t *op_controller,
		    int parent_idx, int *curr_idx, daos_size_t cell_size,
		    daos_size_t chunk_size);


/** Function responsible for closing a seismic trace data
 *  object
 *
 * \param[in]	trace_data_obj  pointer to trace data object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	release_data  	flag to release trace data object once it's closed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_data_obj_close(trace_data_obj_t *trace_data_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx, int *curr_idx, int release_data);


/* Function responsible for releasing an allocated seismic
 * trace data object
 *
 * \param[in]	trace_data	seismic trace data object to be released
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_data_obj_release(trace_data_obj_t *trace_data_obj);


/** Function responsible for destroying a seismic trace data
 *  object
 *
 * \param[in]	trace_data_obj	pointer to trace data object
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
trace_data_object_punch(trace_data_obj_t *trace_data_obj,
		        operations_controllers_t *op_controller,
		        int parent_idx, int *curr_idx);

/* Function responsible for getting an allocated seismic
 * trace data object's oid and open handle
 *
 * \param[in]	trace_data_obj	seismic trace data object
 *
 * \return	seismic object oid_oh struct pointer
 */
seismic_object_oid_oh_t*
trace_data_obj_get_id_oh(trace_data_obj_t *trace_data_obj);

/** Function responsible for fetching data from a seismic trace data
 *  object
 *
 * \param[in]	trace_data_obj	pointer to seismic trace data object
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
trace_data_obj_fetch(trace_data_obj_t *trace_data_obj,
		     operations_controllers_t *op_controller,
		     int parent_idx, int *curr_idx);


/** Function responsible for updating data in a seismic trace data
 *  object
 *
 * \param[in]	trace_data_obj	pointer to seismic trace data object
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
trace_data_obj_update(trace_data_obj_t *trace_data_obj,
		      operations_controllers_t *op_controller,
		      int parent_idx, int *curr_idx);


/** Function responsible for initializing trace data object io operation parameters.
 *
 * \param[in]	    trace_data_obj		Pointer to trace data object
 *
 *
 * \return          0 on success
 *		    error_code otherwise
 *
 */
int
trace_data_obj_init_io_parameters(trace_data_obj_t *trace_data_obj);


/** Function responsible for setting trace data object io operation parameters.
 *
 * \param[in]	   trace_data_obj	Pointer to trace data object
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
trace_data_obj_set_io_parameters(trace_data_obj_t *trace_data_obj,
			  	 uint64_t arr_nr, uint64_t *rg_idx,
				 uint64_t *rg_len, uint64_t arr_nr_short_read,
				 char *data, size_t data_size);


/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    trace_data_obj		pointer to trace_data_object
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
trace_data_obj_release_io_parameters(trace_data_obj_t *trace_data_obj);


/* Function responsible for getting an allocated seismic
 * trace data object's mode
 *
 * \param[in]	trace_data_obj	seismic trace data object
 *
 * \return	daos mode
 */
int
trace_data_obj_get_mode(trace_data_obj_t *trace_data_obj);


/* Function responsible for fetching the trace data object size
 *
 * \param[in]   trace_data_obj	pointer to trace_data object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[out]	size		size of trace data object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
get_trace_data_obj_size(trace_data_obj_t *trace_data_obj,
		        operations_controllers_t *op_controller,
			int parent_idx, int *curr_idx, uint64_t *size);

/** Function responsible for updating trace data stored in a seismic trace data
 *  object
 *
 * \param[in]	trace_data_obj	pointer to seismic trace data object
 * \param[in]	trace		trace struct holding data to be written
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
trace_data_update_data(trace_data_obj_t* trace_data,trace_t *trace,
			operations_controllers_t *op_controller, int parent_idx,
			int *curr_idx);




/** Function responsible for calculating trace data object id from
 *  its associated trace header object id
 *
 * \param[in]	tr_hdr		oid of trace header object
 * \param[in]	cid		class of DAOS object
 *
 *
 * \return	object id for the trace data object
 */
daos_obj_id_t
trace_data_oid_calculation(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid);




/** Function responsible for fetching trace data stored in a seismic trace data
 *  object into an ensemble list
 *
 * \param[in]	ensembles_list		ensemble list to be iterated on
 * \param[in]	st_index		starting index of ensemble
 * \param[in]	num_of_ensembles 	number of ensembles
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
traces_data_obj_fetch_data_in_list(ensemble_list *ensembles_list,
				   int st_idx, int num_of_ensembles,
				   operations_controllers_t *op_controller,
				   int parent_idx, int *curr_idx);

/** Function responsible for initializing empty traces data object
 *
 * \param[in]	traces_data_obj	double pointer to uninitialized trace_data_object
 *
 * \return      0 for successful allocation
 */
int
trace_data_obj_init(trace_data_obj_t **trace_data_obj);


#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACE_DATA_H_ */
