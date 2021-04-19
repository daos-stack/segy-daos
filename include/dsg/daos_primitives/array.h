/*
 * array.h
 *
 *  Created on: Jan 25, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_DAOS_PRIMITIVES_ARRAY_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_DAOS_PRIMITIVES_ARRAY_H_


#include "operations_controller.h"
#include "utilities/error_handler.h"

typedef struct array_io_parameters {
	/** scatter gather list */
	d_sg_list_t 		*sgl;
	/** io descriptor*/
	daos_array_iod_t	*iod;
}array_io_parameters_t;


/** Function responsible for initializing daos array io operation parameters.
 *
 * \param[out]	io_parameters	Pointer to io parameters struct
 *
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
init_array_io_parameters(array_io_parameters_t **io_parameters);


/** Function responsible for setting daos array io operation parameters.
 *
 * \param[out]	    io_parameters	Pointer to io parameters struct
 *  					to be allocated and initialized.
 * \param[in]	    iod_size		array of sizes, it holds size of the
 * 					single value or the record size of the
 * 					array.
 *  					size of this array equals
 *  					num_of_iods_sgls.
 * \param[in]	    arr_nr		number of elements to used in array of
 * 					ranges.
 * \param[in]	    rx_idx		array of records starting indexes.
 * \param[in]	    rg_len		array of range accessed during io.
 * \param[in]	    arr_nr_short_read	number of records that are short fetched
 * 					used during daos array read.
 * \param[in]	    data		data buffer to be used during io.
 * \param[in]	    data_size		size of data buffer.
 *
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
set_array_io_parameters(array_io_parameters_t *io_parameters,
			uint64_t arr_nr, uint64_t *rg_idx,
			uint64_t *rg_len, uint64_t arr_nr_short_read,
			char *data, size_t data_size);

/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	io_parameters	Pointer to io parameters struct
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
release_array_io_parameters(array_io_parameters_t *io_parameters);

/** Function responsible for calling the DAOS object fetch api.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos object id and open handle.
 *  \param[in]	    op_controller	operation controller struct holding
 *  					eventand transaction handles that will
 *  					be used during the fetch operation.
 *  \param[in]	    io_parameters	object io parameters struct that will
 *  					hold all daos fetch operation parameters
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized in
 *  					case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
fetch_array_object_entry(seismic_object_oid_oh_t *object_id_oh,
		   	 operations_controllers_t *op_controller,
			 array_io_parameters_t *io_parameters,
			 int parent_ev_idx, int *curr_idx);

/** Function responsible for calling the DAOS array object update api.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos object id and open handle.
 *  \param[in]	    op_controller	operation controller struct holding
 *  					event and transaction handles that will
 *  					be used during the daos object update
 *  					operation.
 *  \param[in]	    io_parameters	object io parameters struct that will
 *  					hold all daos update operation
 *  					parameters.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case	daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
update_array_object_entry(seismic_object_oid_oh_t *object_id_oh,
		   	  operations_controllers_t *op_controller,
			  array_io_parameters_t *io_parameters,
			  int parent_ev_idx, int *curr_idx);

/** Function responsible for opening the DAOS array object.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos array object id and open handle.
 *  \param[in]	    op_controller	operation controller struct holding
 *  					event and transaction handles that will
 *  					be used during the daos array object
 *  					update operation.
 *  \param[in]	    coh			container open handle.
 *  \param[in]	    mode		open mode DAOS_OO_(RO/RW).
 *  \param[in]	    cell_size		Record size of the array.
 *  \param[in]	    chunk_size		Contiguous bytes to store per DKey
 *  					before moving to a different dkey.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
open_array_object(seismic_object_oid_oh_t *object_id_oh,
		  operations_controllers_t *op_controller,
		  daos_handle_t coh, unsigned int mode,
		  daos_size_t cell_size, daos_size_t chunk_size,
		  int parent_ev_idx, int *curr_idx);

/** Function responsible for closing the DAOS array object.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos array object id and open handle.
 *  \param[in]	    op_controller	operation controller struct
 *  					holding event
 *  					and transaction handles that will be
 *  					used during the daos array object close
 *  					operation.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  				        (currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
close_array_object(seismic_object_oid_oh_t *object_id_oh,
		   operations_controllers_t *op_controller,
		   int parent_ev_idx, int *curr_idx);

/** Function responsible for destroying the DAOS array object.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos array object id and open handle.
 *  \param[in]	    op_controller	operation controller struct
 *  					holding event and transaction handles
 *  					that will be used during the daos array
 *  					object destroy operation.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
destroy_array_object(seismic_object_oid_oh_t *object_id_oh,
		     operations_controllers_t *op_controller,
		     int parent_ev_idx, int *curr_idx);

/** Function responsible for getting the DAOS array object size.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos array object id and open handle.
 *  \param[in]	    op_controller	operation controller struct
 *  					holding event
 *  					and transaction handles that will be
 *  					used during the daos array object
 *  					destroy operation.
 *  \param[out]	    size		returned size array (number of records).
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
get_size_array_object(seismic_object_oid_oh_t *object_id_oh,
		      operations_controllers_t *op_controller, uint64_t *size,
		      int parent_ev_idx, int *curr_idx);

#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_DAOS_PRIMITIVES_ARRAY_H_ */
