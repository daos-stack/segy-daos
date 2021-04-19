/*
 * complex_gather.h
 *
 *  Created on: Mar 30, 2021
 *      Author: omar
 */

#include "graph_objects/gather.h"
#include "operations/general_operations.h"

#ifndef DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_COMPLEX_GATHER_H_
#define DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_COMPLEX_GATHER_H_

/* struct representing a seismic complex gather object */
typedef struct complex_gather_obj{
	/** DAOS object ID & open handle*/
	seismic_object_oid_oh_t oid_oh;
	/** daos object mode */
	int	daos_mode;

	/* io parameters to be used for fetch and updates */
	object_io_parameters_t *io_parameters;
}complex_gather_obj_t;


/** Function responsible for creating and allocating a seismic complex gather
 *  object
 *
 * \param[in]	complex_gather_obj	pointer to uninitialized complex gather object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_create(complex_gather_obj_t **complex_gather_obj, int flags);


/** Function responsible for opening a seismic complex gather
 *  object
 *
 * \param[in]	oid_oh			pointer to seismic object struct holding
 *  							daos array object id and open handle.
 * \param[in]   mode   			Open mode: DAOS_OO_RO/RW/EXCL/IO_RAND/IO_SEQ
 * \param[in]	op_controller	operation controller struct holding event
 *  							and transaction handles that will be used
 *  							during the daos array object update operation.
 * \param[in]	parent_idx		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx		current index of event initialized in case
 *  							daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		   operations_controllers_t *op_controller,
		   int parent_idx, int *curr_idx);

/** Function responsible for closing a seismic complex gather
 *  object
 *
 * \param[in]	oid_oh			pointer to seismic object struct holding
 *  							daos array object id and open handle.
 * \param[in]	op_controller	operation controller struct holding event
 *  							and transaction handles that will be used
 *  							during the daos array object update operation.
 * \param[in]	parent_idx		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx		current index of event initialized in case
 *  							daos events are used.
 * \param[in]	release_gather  flag to release complex gather object once it's closed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_close(complex_gather_obj_t *complex_gather_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx, int *curr_idx, int release_gather);

/* Function responsible for releasing an allocated seismic complex
 * gather object
 *
 * \param[in]	complex_gather_obj	seismic complexgather object to be released
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_release(complex_gather_obj_t *complex_gather_obj);


/** Function responsible for destroying a seismic complex gather
 *  object
 *
 * \param[in]	oid_oh			pointer to seismic object struct holding
 *  							daos array object id and open handle.
 * \param[in]	op_controller	operation controller struct holding event
 *  							and transaction handles that will be used
 *  							during the daos array object update operation.
 * \param[in]	punch_flags		Punch flags
 * \param[in]	parent_idx		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx		current index of event initialized in case
 *  							daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_punch(complex_gather_obj_t *complex_gather_obj,
		       operations_controllers_t *op_controller,
		       int punch_flags, int parent_idx, int *curr_idx);

/* Function responsible for getting an allocated seismic complex
 * gather object's oid and open handle
 *
 * \param[in]	complex_gather_obj	seismic complex gather object
 *
 * \return	seismic object oid_oh struct pointer
 */
seismic_object_oid_oh_t*
complex_gather_obj_get_oid_oh(complex_gather_obj_t *complex_gather_obj);

/* Function responsible for getting an allocated seismic complex
 * gather object's mode
 *
 * \param[in]	complex_gather_obj	seismic complex gather object
 *
 * \return	daos mode
 */
int
complex_gather_obj_get_mode(complex_gather_obj_t *complex_gather_obj);

/** Function responsible for fetching data from a seismic complex gather
 *  object
 *
 * \param[in]	complex_gather_obj	pointer to seismic complex gather object
 * \param[in]	op_controller		operation controller struct holding event
 *  								and transaction handles that will be used
 *  								during the daos array object update operation.
 * \param[in]	parent_idx			id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx			current index of event initialized in case
 *  								daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_fetch(complex_gather_obj_t *complex_gather_obj,
		    operations_controllers_t *op_controller,
		    int parent_idx, int *curr_idx);

/** Function responsible for updating data in a seismic complex gather
 *  object
 *
 * \param[in]	complex_gather_obj	pointer to seismic complex gather object
 * \param[in]	op_controller		operation controller struct holding event
 *  								and transaction handles that will be used
 *  								during the daos array object update operation.
 * \param[in]	parent_idx			id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx			current index of event initialized in case
 *  								daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_update(complex_gather_obj_t *complex_gather_obj,
		     operations_controllers_t *op_controller,
		     int parent_idx, int *curr_idx);



/** Function responsible for initializing complex gather object io operation parameters.
 *
 * \param[in]	    complex_gather_obj		Pointer to complex gather object
 * \param[in]	    op_flags				fetch/update operation flags.
 * \param[in]	    dkey_name				string holding dkey name of object that
 * 											will be accessed.
 * \param[in]	    num_of_iods_sgls		number of io descriptors and scatter
 *  										gather lists that will be used.
 * \return      0 on success, error_code otherwise
 *
 */
int
complex_gather_obj_init_io_parameters(complex_gather_obj_t *complex_gather_obj, uint64_t op_flags,
				  char *dkey_name, unsigned int num_of_iods_sgls);

/** Function responsible for setting complex gather object io operation parameters.
 *
 * \param[in]	    complex_gather_obj		Pointer to complex gather object
 * \param[in]	    akey_name				array of strings holding akey names
 *  										that will be used to update their data.
 *  										size of this array equals num_of_iods_sgls.
 * \param[in]	    type					types of value in each io descriptor.
 *  										size of this array equals num_of_iods_sgls.
 * \param[in]	    iod_size				array of sizes, it holds size of the single
 *  										value or the record size of the array.
 *  										size of this array equals num_of_iods_sgls.
 * \param[in]	    iod_nr					array holding number of extents in each iod.
 *  										should be 1 if single value.
 *  										size of this array equals num_of_iods_sgls.
 * \param[in]	    rx_idx					array of records starting indexes.
 *  										size of each array is equal to its equivalent
 *  										one in iod_nr.
 * \param[in]	    rx_nr					array of records sizes accessed during io.
 *  										size of each array is equal to its equivalent
 *  										one in iod_nr.
 * \param[in]	   data						array of data buffers to be used during io.
 *  										size of this array equals num_of_iods_sgls.
 * \param[in]	   data_size				array of size of each data buffer.
 *  										size of this array equals num_of_iods_sgls.
 * \return      0 on success error_code otherwise
 *
 */
int
complex_gather_obj_set_io_parameters(complex_gather_obj_t *complex_gather_obj, char *akey_name,
				daos_iod_type_t type, uint64_t iod_size,
				unsigned int iod_nr, uint64_t *rx_idx,
				uint64_t *rx_nr, char *data, size_t data_size);

/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    complex_gather_obj		pointer to complex gather_object
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
complex_gather_obj_release_io_parameters(complex_gather_obj_t *complex_gather_obj);

/* Function responsible for fetching number of traces under a specific gather
 * under the complex gather object in the seismic graph
 *
 * \param[in]   complex_gather_obj	 pointer to gather object
 * \param[in]	op_controller	 	operation controller struct holding event
 *  				 				and transaction handles that will be used
 *  				 				during the daos array object update operation.
 * \param[in]	gather_dkey_name 	name of gather whose number traces is fetched
 * \param[out]  num_of_traces    	address of an integer variable to hold the fetch result
 * \param[in]	parent_idx	 		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 		current index of event initialized in case
 *  				 				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_fetch_gather_num_of_traces(complex_gather_obj_t *complex_gather_obj,
				operations_controllers_t *op_controller,
				char* gather_dkey_name,
				int *num_of_traces,
				int parent_idx, int *curr_idx);

/* Function responsible for fetching array of traces array object
 * object ids under the complex gather object in the seismic graph
 *
 * \param[in]   complex gather_obj	pointer to gather object
 * \param[in]	op_controller		operation controller struct holding event
 *  								and transaction handles that will be used
 *  								during the daos array object update operation.
 * \param[in]	parent_idx			id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx			current index of event initialized in case
 *  								daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_fetch_gather_traces_array_oid(complex_gather_obj_t *complex_gather_obj,
				operations_controllers_t *op_controller,
				char* gather_dkey_name,
				daos_obj_id_t* oid,
				int parent_idx, int *curr_idx);

/* Function responsible for updating the number of traces of a specific
 * gather under a complex gather object
 *
 * \param[in]   complex_gather_obj	pointer to complex gather object
 * \param[in]	dkey		 		dkey for the required gather
 * \param[in]	num_of_traces	 	number of traces
 * \param[in]	op_controller	 	operation controller struct holding event
 *  				 				and transaction handles that will be used
 *  				 				during the daos array object update operation.
 * \param[in]	parent_idx	 		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 		current index of event initialized in case
 *  				 				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_update_gather_num_of_traces(complex_gather_obj_t *complex_gather_obj, char *dkey,
				       int *num_of_traces,
				       operations_controllers_t *op_controller,
				       int parent_idx, int *curr_idx);

/* Function responsible for updating the oid of a traces array object
 * under a complex gather object
 *
 * \param[in]   complex_gather_obj	pointer to complex gather object
 * \param[in]	dkey		 		dkey for the required gather
 * \param[in]	oid	 	 			oid of traces array object to be written
 * \param[in]	op_controller	 	operation controller struct holding event
 *  				 				and transaction handles that will be used
 *  				 				during the daos array object update operation.
 * \param[in]	parent_idx	 		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx			current index of event initialized in case
 *  				 				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_update_gather_traces_array_oid(complex_gather_obj_t *complex_gather_obj, char *dkey,
					  daos_obj_id_t *oid,
					  operations_controllers_t *op_controller,
					  int parent_idx, int *curr_idx);



/* Function responsible for initializing a complex gather object variable (or loading
 * it from graph if already initialized)
 *
 * \param[in]   gather_obj	double pointer to complex gather object
 * \param[in]   oid_oh		object id and open handle
 * \param[in]   initialized	initialized flag to load from graph if applicable
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_fetch_entries(complex_gather_obj_t** complex_gather_obj, seismic_object_oid_oh_t oid_oh);

/* Function responsible for updating the meta-data and traces
 *  under a complex gather object from an ensemble list
 *
 * \param[in]   complex_gather_obj	pointer to complex gather object
 * \param[in]	op_controller	 	operation controller struct holding event
 *  				 				and transaction handles that will be used
 *  				 				during the daos array object update operation.
 * \param[in]	ensembles_list	    ensembles list object to be read from
 * \param[in]	parent_idx	 		id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 		current index of event initialized in case
 *  				 				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
complex_gather_obj_update_gather_metadata_and_traces(complex_gather_obj_t *complex_gather_obj, char *dkey,
					    operations_controllers_t *op_controller,
					    ensemble_list *ensembles, int ensemble_index,
					    int parent_idx, int *curr_idx);


/* Function for getting the number of a-keys
 * under a specific d-key stored under a complex gather
 *
 * \param[in]	complex_gather_obj		pointer to complex gather object
 * \param[in]	dkey					dkey under the gather object
 * \param[in]	expected_ num_of_keys	expected number of records to be read
 *
 * return	number of a-keys found under the given d-key
 */
int
complex_gather_obj_get_dkey_num_of_akeys(complex_gather_obj_t *complex_gather_obj, char *dkey,
				 int expected_num_of_keys);

int
complex_gather_obj_fetch_gather_metadata_and_traces(complex_gather_obj_t *complex_gather_obj,
		char *dkey, operations_controllers_t *op_controller,
		ensemble_list *ensembles_list, int parent_idx, int *curr_idx);

int
complex_gather_obj_fetch_complex_gather(seismic_object_oid_oh_t complex_oid,
			        ensemble_list *final_list, int index);

/** Function responsible for initializing empty complex gather object
 *
 * \param[in]	complex_obj	double pointer to uninitialized complex_object
 *
 * \return      0 for successful allocation
 */
int
complex_gather_obj_init(complex_gather_obj_t **complex_obj);


#endif /* DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_COMPLEX_GATHER_H_ */
