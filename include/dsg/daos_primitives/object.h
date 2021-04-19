/*
 * object.h
 *
 *  Created on: Jan 23, 2021
 *      Author: mirnamoawad
 */

#ifndef INCLUDE_DSG_DAOS_PRIMITIVES_OBJECT_H_
#define INCLUDE_DSG_DAOS_PRIMITIVES_OBJECT_H_

#include "operations_controller.h"
#include "utilities/error_handler.h"

typedef struct object_io_parameters {
	/** Number of io descriptors and scatter gather lists */
	unsigned int 		nr;
	/** array of scatter gather lists */
	d_sg_list_t 		*sgls;
	/** array of io descriptors */
	daos_iod_t		*iods;
	/** IO operation flags */
	uint64_t 		flags;
	/** IO operation dkey name */
	daos_key_t		dkey_name;
	/** current index */
	int			curr_idx;
}object_io_parameters_t;


/** Function responsible for initializing daos object io operation parameters.
 *
 * \param[out]	    io_parameters	Pointer to io parameters struct
 *  					to be allocated and initialized.
 * \param[in]	    op_flags		fetch/update operation flags.
 * \param[in]	    dkey_name		string holding dkey name of object that
 * 					will be accessed.
 * \param[in]	    num_of_iods_sgls	number of io descriptors and scatter
 *  					gather lists that will be used.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
init_object_io_parameters(object_io_parameters_t **io_parameters,
			  uint64_t op_flags, char *dkey_name,
			  unsigned int num_of_iods_sgls);


/*Function to set variables in the object io parameters
 *
 *
 * \param[in]	    akey_name		string holding akey name
 *  					that will be used to update their data.
 * \param[in]	    types		types of value in each io descriptor.
 * \param[in]	    iod_size		it holds size of the single value or
 * 					the record size of the array.
 * \param[in]	    iod_nr		number of extents in each iod, should
 * 					be 1 if single value.
 * \param[in]	    rx_idx		records starting indexes.
 *  					size is equal to its equivalent index
 *  					in iod_nr.
 * \param[in]	    rx_nr		records sizes accessed during io.
 *  					size is equal to its equivalent one in
 *  					iod_nr.
 * \param[in]	    data		array of data buffers to be used during
 * 					io.
 *  					size of this array equals
 *  					num_of_iods_sgls.
 * \param[in]	   data_size		array of size of each data buffer.
 *  					size of this array equals
 *  					num_of_iods_sgls.
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
set_object_io_parameters(object_io_parameters_t *io_parameters,
			 char *akey_name, daos_iod_type_t type,
			 uint64_t iod_size, unsigned int iod_nr,
			 uint64_t *rx_idx, uint64_t *rx_nr, char *data,
			 size_t data_size);
/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    io_parameters	Pointer to io parameters struct
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
release_object_io_parameters(object_io_parameters_t *io_parameters);

/** Function responsible for calling the DAOS object fetch api.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos object id and open handle.
 *  \param[in]	    op_controller	operation controller struct
 *  					holding eventand transaction handles
 *  					that will be used during the
 *  					fetch operation.
 *  \param[in]	    io_parameters	object io parameters struct that will
 *  					hold all daos fetch operation parameters
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
fetch_object_entry(seismic_object_oid_oh_t *object_id_oh,
		   operations_controllers_t *op_controller,
		   object_io_parameters_t *io_parameters,
		   int parent_ev_idx, int *curr_idx);

/** Function responsible for calling the DAOS object update api.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos object id and open handle.
 *  \param[in]	    op_controller	operation controller struct
 *  					holding event and transaction handles
 *  					that will be used during the daos object
 *  					update operation.
 *  \param[in]	    io_parameters	object io parameters struct that will
 *  					hold all daos update operation
 *  					parameters.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
update_object_entry(seismic_object_oid_oh_t *object_id_oh,
		    operations_controllers_t *op_controller,
		    object_io_parameters_t *io_parameters,
		    int parent_ev_idx, int *curr_idx);

/** Function responsible for opening the DAOS object.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos array object id and open handle.
 *  \param[in]	    op_controller	operation controller struct
 *  					holding event and transaction handles
 *  					that will be used during the daos array
 *  					object update operation.
 *  \param[in]	    coh			container open handle.
 *  \param[in]	    mode		Open mode:
 *  					DAOS_OO_RO/RW/EXCL/IO_RAND/IO_SEQ
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized
 *  					in case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
open_object(seismic_object_oid_oh_t *object_id_oh,
	    operations_controllers_t *op_controller,
	    daos_handle_t coh, unsigned int mode,
	    int parent_ev_idx, int *curr_idx);

/** Function responsible for closing the DAOS object.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos object id and open handle.
 *  \param[in]	    op_controller	operation controller struct holding
 *  					event and transaction handles that will
 *  					be used during the daos object
 *  					close operation.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized in case
 *  					daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
close_object(seismic_object_oid_oh_t *object_id_oh,
	     operations_controllers_t *op_controller,
	     int parent_ev_idx, int *curr_idx);

/** Function responsible for destroying the DAOS object.
 *
 *  \param[in]	    object_id_oj	pointer to seismic object struct holding
 *  					daos object id and open handle.
 *  \param[in]	    flags		Punch flags.
 *  \param[in]	    op_controller	operation controller struct holding
 *  					event and transaction handles that will
 *  					be used during the daos object destroy
 *  					operation.
 *  \param[in]	    parent_ev_idx	id of parent event to wait on
 *  					(currently ignored)
 *  \param[out]	    curr_idx		current index of event initialized in
 *  					case daos events are used.
 *
 *  \return	    0 on success
 *  		    error_code otherwise
 */
int
destroy_object(seismic_object_oid_oh_t *object_id_oh, uint64_t flags,
	       operations_controllers_t *op_controller,
	       int parent_ev_idx, int *curr_idx);

#endif /* INCLUDE_DSG_DAOS_PRIMITIVES_OBJECT_H_ */
