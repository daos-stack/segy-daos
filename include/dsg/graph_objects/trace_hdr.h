/*
 * trace_hdr.h
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACE_HDR_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACE_HDR_H_

#include "daos_primitives/operations_controller.h"
#include "data_types/trace.h"
#include "daos_primitives/object.h"
#include "daos_primitives/dfs_helpers.h"

#define DS_D_TRACE_HEADER "Trace_Header"
#define DS_A_TRACE_HEADER "File_Trace_Header"

typedef struct trace_hdr_obj{
	/** DAOS object ID & open handle*/
	seismic_object_oid_oh_t oid_oh;
	/** daos object mode */
	int	daos_mode;
	/**trace header */
	trace_t *trace;

	object_io_parameters_t *io_parameters;
}trace_hdr_obj_t;


/** Function responsible for creating and allocating a trace header
 *  object
 *
 * \param[in]	trace_hdr_obj		pointer to uninitialized trace_header object
 * \param[in]	flags			DAOS flags
 * \param[in]	trace			trace object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_hdr_obj_create(trace_hdr_obj_t **trace_hdr_obj, int flags,
		     trace_t *trace);


/** Function responsible for opening a seismic trace header object
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
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_hdr_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		   operations_controllers_t *op_controller,
		   int parent_idx, int *curr_idx);

/** Function responsible for closing a seismic trace header
 *  object
 *
 * \param[in]	trace_header_obj  	pointer to trace header object
 * \param[in]	op_controller		operation controller struct holding event
 *  					and transaction handles that will be used
 *  					during the daos array object update operation.
 * \param[in]	parent_idx		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx		current index of event initialized in case
 *  					daos events are used.
 * \param[in]	release_trace 		flag to release trace object once it's closed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_hdr_obj_close(trace_hdr_obj_t *trace_hdr_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx, int *curr_idx, int release_trace);

/* Function responsible for releasing an allocated seismic
 * trace header object
 *
 * \param[in]	trace_hdr_obj	seismic trace header object to be released
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_hdr_obj_release(trace_hdr_obj_t *trace_hdr_obj);



/** Function responsible for destroying a seismic trace header
 *  object
 *
 * \param[in]	trace_hdr_obj	pointer to trace header object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	punch_flags	punch flags
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_hdr_object_punch(trace_hdr_obj_t *trace_hdr_obj,
		       operations_controllers_t *op_controller,
		       int punch_flags, int parent_idx, int *curr_idx);


/* Function responsible for getting an allocated seismic
 * trace header object's oid and open handle
 *
 * \param[in]	trace_hdr_obj	seismic trace header object
 *
 * \return	seismic object oid_oh struct pointer
 */
seismic_object_oid_oh_t*
trace_hdr_obj_get_id_oh(trace_hdr_obj_t *trace_hdr_obj);


/** Function responsible for fetching data from a seismic trace header
 *  object
 *
 * \param[in]	trace_hdr_obj	pointer to seismic trace header object
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
trace_hdr_obj_fetch(trace_hdr_obj_t *trace_hdr_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx, int *curr_idx);

/** Function responsible for updating data in a seismic trace header
 *  object
 *
 * \param[in]	trace_hdr_obj	pointer to seismic trace header object
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
trace_hdr_obj_update(trace_hdr_obj_t *trace_hdr_obj,
		     operations_controllers_t *op_controller,
		     int parent_idx, int *curr_idx);


/** Function responsible for initializing trace header object io operation parameters.
 *
 * \param[in]	    trace_hdr_obj	Pointer to trace header object
 * \param[in]	    op_flags		fetch/update operation flags.
 * \param[in]	    dkey_name		string holding dkey name of object that
 * 					will be accessed.
 * \param[in]	    num_of_iods_sgls	number of io descriptors and scatter
 *  					gather lists that will be used.
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
trace_hdr_obj_init_io_parameters(trace_hdr_obj_t *trace_hdr_obj,
				 uint64_t op_flags, char *dkey_name,
				 unsigned int num_of_iods_sgls);

/** Function responsible for setting trace_header object io operation parameters.
 *
 * \param[in]	    trace_hdr_obj	Pointer to trace header object
 * \param[in]	    akey_name		array of strings holding akey names
 *  					that will be used to update their data.
 *  					size of this array equals num_of_iods_sgls.
 * \param[in]	    type		types of value in each io descriptor.
 *  					size of this array equals num_of_iods_sgls.
 * \param[in]	    iod_size		array of sizes, it holds size of the single
 *  					value or the record size of the array.
 *  					size of this array equals num_of_iods_sgls.
 * \param[in]	    iod_nr		array holding number of extents in each iod.
 *  					should be 1 if single value.
 *  					size of this array equals num_of_iods_sgls.
 * \param[in]	    rx_idx		array of records starting indexes.
 *  					size of each array is equal to its equivalent
 *  					one in iod_nr.
 * \param[in]	    rx_nr		array of records sizes accessed during io.
 *  					size of each array is equal to its equivalent
 *  					one in iod_nr.
 * \param[in]	   data			array of data buffers to be used during io.
 *  					size of this array equals num_of_iods_sgls.
 * \param[in]	   data_size		array of size of each data buffer.
 *  					size of this array equals num_of_iods_sgls.
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
trace_hdr_object_set_io_parameters(trace_hdr_obj_t *trace_hdr_obj,
				   char *akey_name, daos_iod_type_t type,
				   uint64_t iod_size, unsigned int iod_nr,
				   uint64_t *rx_idx, uint64_t *rx_nr,
				   char *data, size_t data_size);


/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    trace_hdr_obj		pointer to trace_header_object
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
trace_hdr_obj_release_io_parameters(trace_hdr_obj_t *trace_hdr_obj);

/* Function responsible for getting an allocated seismic
 * trace data object's mode
 *
 * \param[in]	trace_data_obj	seismic trace data object
 *
 * \return	daos mode
 */
int
trace_hdr_obj_get_mode(trace_hdr_obj_t *trace_hdr_obj);


/* Function responsible for fetching the trace data array from a trace header
 * object
 *
 * \param[in]   trace_hdr_obj	pointer to trace_header object
 *
 * \return	float pointer to trace data
 */
float*
trace_hdr_obj_get_data_array(trace_hdr_obj_t *trace_hdr_obj);

/* Function responsible for updating the trace headers under a
 * trace header object
 *
 * \param[in]   trace_hdr	pointer to trace_header object
 * \param[in]	trace		trace variable to be read from
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
trace_hdr_update_headers(trace_hdr_obj_t* trace_hdr,trace_t *trace,
			 operations_controllers_t *op,
			 int parent_idx, int *curr_idx);


/* Function responsible for fetching the trace headers under a
 * trace header object
 *
 * \param[in]   trace_hdr	pointer to trace_header object
 * \param[in]	trace		trace variable to be written to
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
trace_hdr_fetch_headers(trace_hdr_obj_t *trace_hdr,trace_t* trace,
			operations_controllers_t *op,
			int parent_idx, int *curr_idx);


/* Function responsible for fetching the customized selected trace headers under a
 * trace header object
 *
 * \param[in]   trace_hdr	pointer to trace_header object
 * \param[in]	kv		initialized key-value pair object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	keys		array of strings representing keys selected by user
 * \param[in]	num_of_keys	number of keys selected by user
 * \param[in]	trace_idx	position to write inside the key-value array of values
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
trace_hdr_fetch_custom_headers(trace_hdr_obj_t *trace_hdr,key_value_pair_t* kv,
			       operations_controllers_t *op,
			       int parent_idx, int *curr_idx,
			       char** keys, int num_of_keys,
			       int trace_idx);



/* Function responsible for printing values in a key value pair object
 *
 * \param[in]	kv		initialized key-value pair object
 * \param[in]	num_of_keys	number of keys to be printed
 *
 *\return	0 if successful,
 * 		error code otherwise
 */
int
key_value_print(key_value_pair_t* kv, int num_of_keys);

/** Function responsible for initializing empty trace hdr object
 *
 * \param[in]	trace_hdr_obj	double pointer to uninitialized trace_hdr_object
 *
 * \return      0 for successful allocation
 */
int
trace_hdr_obj_init(trace_hdr_obj_t **trace_hdr_obj);


#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_TRACE_HDR_H_ */
