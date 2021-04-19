/*
 * root.h
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_ROOT_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_ROOT_H_

#include "daos_primitives/operations_controller.h"
#include "daos_primitives/object.h"
#include "daos_primitives/dfs_helpers.h"
#include "seismic_sources/segy.h"

#define DS_D_GATHERS_OIDS "GATHERS_OIDS"
#define	DS_D_KEYS "KEYS"
#define	DS_A_KEYS "KEY_"
#define	DS_A_NUM_OF_KEYS "NUMBER_OF_KEYS"
#define DS_D_FILE_HEADER "File_Header"
#define DS_A_TEXT_HEADER "Text_Header"
#define DS_A_BINARY_HEADER "Binary_Header"
#define DS_A_EXTENDED_HEADER "Extended_Text_Header"
#define DS_A_NTRACES_HEADER "Number_of_Traces"
#define DS_D_SORT_VARIATIONS "Sorting_variations"
#define	DS_A_NUM_OF_VARS "NUMBER_OF_VARIATIONS"
#define DS_A_COMPLEX_GATHER "Complex_gather_oid"
#define	DS_A_VAR "VAR_"
#define COMPLEX_GATHER "COMPLEX_GATHER"

typedef struct root_obj{
	seismic_object_oid_oh_t 	oid_oh;
	seismic_object_oid_oh_t		parent_oid_oh;
	object_io_parameters_t 		*io_parameters;
	seismic_object_oid_oh_t		*gather_oids;
	char 				**keys;
	mode_t				permissions_and_type;
	int 				num_of_traces;
	int 				num_of_keys;
	int				daos_mode;
	int 				nexth;
	int				nvariations;
	char				**variations;
	seismic_object_oid_oh_t		complex_oid;
}root_obj_t;


/** Function responsible for creating and allocating a root
 *  object
 *
 * \param[in]	root_obj		pointer to uninitialized root object
 * \param[in]	flags			DAOS flags
 * \param[in]	keys			Initial keys for gather objects stored under the root
 * \param[in]   num_of_keys		number of keys(i.e number of gather objects)
 * \param[in]   permissions_and_type  	permissions for the object
 * \param[in]	parent			oid of parent object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_create(root_obj_t **root_obj, int flags, char **keys, int num_of_keys,
		mode_t permissions_and_type, seismic_object_oid_oh_t *parent);



/** Function responsible for opening a seismic root object
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
root_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
	      operations_controllers_t *op_controller,
	      int parent_idx, int *curr_idx);


/** Function responsible for closing a seismic gather
 *  object
 *
 * \param[in]	oid_oh		pointer to seismic object struct holding
 *  				daos array object id and open handle.
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	release_root  	flag to release root object once it's closed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_close(root_obj_t *root_obj,
	       operations_controllers_t *op_controller,
	       int parent_idx, int *curr_idx, int release_root);

/* Function responsible for releasing an allocated seismic
 * root object
 *
 * \param[in]	root_obj	seismic root object to be released
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_release(root_obj_t *root_obj);


/** Function responsible for destroying a seismic root
 *  object
 *
 * \param[in]	oid_oh		pointer to seismic object struct holding
 *  				daos array object id and open handle.
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	punch_flags	Punch flags
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_punch(root_obj_t *root_obj,
	       operations_controllers_t *op_controller,
	       int punch_flags, int parent_idx, int *curr_idx);


/* Function responsible for getting an allocated seismic
 * root object's oid and open handle
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	seismic object oid_oh struct pointer
 */
seismic_object_oid_oh_t*
root_obj_get_id_oh(root_obj_t *root_obj);


/** Function responsible for fetching data from a seismic root
 *  object
 *
 * \param[in]	root_obj	pointer to seismic root object
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
root_obj_fetch(root_obj_t *root_obj,
	       operations_controllers_t *op_controller,
	       int parent_idx, int *curr_idx);


/** Function responsible for updating data in a seismic root
 *  object
 *
 * \param[in]	root_obj	pointer to seismic root object
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
root_obj_update(root_obj_t *root_obj,
		operations_controllers_t *op_controller,
		int parent_idx, int *curr_idx);


/** Function responsible for initializing root object io operation parameters.
 *
 * \param[in]	    root_obj		Pointer to root object
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
root_obj_init_io_parameters(root_obj_t *root_obj,
			    uint64_t op_flags, char *dkey_name,
			    unsigned int num_of_iods_sgls);

/** Function responsible for setting root object io operation parameters.
 *
 * \param[in]	    root_obj		Pointer to root object
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
root_obj_set_io_parameters(root_obj_t *root_obj, char *akey_name,
		      	   daos_iod_type_t type, uint64_t iod_size,
			   unsigned int iod_nr, uint64_t *rx_idx,
			   uint64_t *rx_nr, char *data, size_t data_size);


/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    root_obj		pointer to root_object
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
root_obj_release_io_parameters(root_obj_t *root_obj);


/* Function responsible for getting an allocated seismic
 * root object's mode
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	daos mode
 */
int
root_obj_get_mode(root_obj_t *root_obj);


/* Function responsible for getting an allocated seismic
 * root object's number of keys
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	number of keys
 */
int
root_obj_get_num_of_keys(root_obj_t *root_obj);


/* Function responsible for getting an allocated seismic
 * root object's keys list
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	keys array
 */
char**
root_obj_get_array_of_keys(root_obj_t *root_obj);


/* Function responsible for getting an allocated seismic
 * root object's number of traces
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	number of traces
 */
int
root_obj_get_num_of_traces(root_obj_t *root_obj);

/* Function responsible for getting an allocated seismic
 * root object's daos object id
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	daos object id
 */
daos_obj_id_t
root_obj_get_oid(root_obj_t *root_obj);


/* Function responsible for getting an allocated seismic
 * root object's permissions and type
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	integer flag
 */
int
root_obj_get_permissions_and_type(root_obj_t *root_obj);


/* Function responsible for getting an allocated seismic
 * root object's parent open handle
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	daos handle
 */
daos_handle_t
root_obj_get_parent_oh(root_obj_t *root_obj);

/* Function responsible for getting an allocated seismic
 * root object's parent's open handle & id
 *
 * \param[in]	root_obj	seismic root object
 *
 * \return	object oid-oh struct
 */
seismic_object_oid_oh_t*
root_obj_get_parent_oid_oh(dfs_obj_t *object);



/* Function responsible for updating number of keys under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	num_of_keys	number of keys to be written
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_keys(root_obj_t *root_obj,
		     operations_controllers_t *op_controller, int num_of_keys,
		     char **keys, int parent_idx, int *curr_idx);


/* Function responsible for updating the binary and text headers header under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	text		string representing text header
 * \param[in]	bh		binary header struct
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
root_obj_update_headers(root_obj_t *root_obj, char* text, bhed* bh,
		        operations_controllers_t *op_controller, int parent_idx,
		        int *curr_idx);


/* Function responsible for updating the extended text headers header under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	parse_functions	parsing functions for parsing the required header
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	nextended	nextended flag to specify if extended text header exists
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_extheaders(root_obj_t* root_obj,
			   parse_functions_t *parse_functions,
			   operations_controllers_t* op_controller,
			   int nextended, int parent_idx, int* curr_idx);


/* Function responsible for updating number traces under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	num_of_traces	number of traces to be written
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_num_of_traces(root_obj_t* root_obj,
			      operations_controllers_t* op_controller,
			      int *num_of_traces,
			      int parent_idx, int *curr_idx);

/* Function responsible for updating the oids of gathers under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	keys		keys for each seismic gather object
 * \param[in]	num_of_gathers	number of gathers to be written under the root
 * \param[in]	gathers		array of oids and open handles for each gather
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_gather_oids(root_obj_t *root_obj,
			    operations_controllers_t *op_controller, char **keys,
			    int num_of_gathers, seismic_object_oid_oh_t *gathers,
			    int parent_idx, int *curr_idx);

/* Function responsible for fetching the oids of gathers under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	keys		keys for each seismic gather object
 * \param[in]	num_of_gathers	number of gathers to be written under the root
 * \param[in]	gathers		array of oids and open handles for each gather
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_gather_oids(root_obj_t *root_obj,
			   operations_controllers_t *op_controller, char **keys,
			   int num_of_gathers, seismic_object_oid_oh_t *gathers,
			   int parent_idx, int *curr_idx);


/* Function responsible for creating a dfs entry for the root object
 *
 * \param[in]	root_obj	seismic root object
 * \param[in]	name		entry name
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_insert_in_dfs(root_obj_t *root_obj, char *name);


/* Function responsible for updating number keys under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[out]	num_of_keys	number of keys to be fetched
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_num_of_keys(root_obj_t *root_obj, int parent_idx, int *curr_idx,
			   int *num_of_keys);


/* Function responsible for fetching keys under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	num_of_keys	number of keys to be fetched
 * \param[out]	keys		array of strings to hold the keys
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_keys(root_obj_t *root_obj, int parent_idx, int *curr_idx,
		    int num_of_keys, char **keys);


/* Function responsible for fetching number traces under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	num_of_traces	number of traces to be fetched
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_num_of_traces(root_obj_t* root_obj,
			     operations_controllers_t* op_controller,
			     int *num_of_traces,
			     int parent_idx, int *curr_idx);


/* Function responsible for initializing a root object variable (or loading
 * it from graph if already initialized). Initializes oid, mode, number of keys,
 * keys, gather oid and number of traces
 *
 * \param[in]   root_obj	double pointer to root object
 * \param[in]   oid_oh		object id and open handle
 * \param[in]   name		name of root object
 * \param[in]   initialized	initialized flag to load from graph if applicable
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_entries(root_obj_t **root_obj, dfs_obj_t *dfs_obj);


/* Function responsible for fetching the text headers header under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[out]	text		string representing text header
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_text_header(root_obj_t *root_obj, char *ebcbuf);


/* Function responsible for fetching the binary headers header under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[out]	binary_header	binary header struct object
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_binary_header(root_obj_t *root_obj, bhed *binary_header);


/* Function responsible for uadding a new gather under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	oid_oh		oid and open handle of new gather object
 * \param[in]	key		keys for the seismic gather object
 * \param[in]	num_of_keys	number of gathers to be written under the root
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
root_obj_link_new_gather_obj(root_obj_t *root_obj,
			     seismic_object_oid_oh_t oid_oh, char *key,
			     int num_of_keys,
			     operations_controllers_t *op_controller,
			     int parent_idx, int *curr_idx);


/* Function responsible for fetching number of variations under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[out]	num_of_var	number of variations to be fetched
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_num_of_variations(root_obj_t *root_obj,
				 operations_controllers_t *op,
				 int *num_of_variations,
				 int parent_idx, int *curr_idx);


/* Function responsible for updating number of variations under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	num_of_var	number of variations to be fetched
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_num_of_variations(root_obj_t *root_obj,
				  operations_controllers_t *op,
				  int *num_of_variations,
				  int parent_idx, int *curr_idx);



/* Function responsible for fetching variations under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	num_of_var	number of variations to be fetched
 * \param[out]	variations	array of strings to hold the variations
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_variations(root_obj_t *root_obj, int parent_idx, int *curr_idx,
		    	  int num_of_variations, char **variations);




/* Function responsible for updating variations under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	num_of_var	number of variations to be updated
 * \param[in]	variations	array of strings to hold the variations
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_variations(root_obj_t *root_obj, int parent_idx, int *curr_idx,
		    	   int num_of_variations, char **variations);




/* Function responsible for fetching the oid of complex gather under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	complex_gather	oid and open handle for complex gather
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_fetch_complex_oid(root_obj_t *root_obj,
			   operations_controllers_t *op_controller,
			   int parent_idx, int *curr_idx,
			   seismic_object_oid_oh_t *complex_gather);




/* Function responsible for updating the oid of complex gather under the
 * root object in the seismic graph
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 * \param[in]	complex_gather	oid and open handle for complex gather
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
root_obj_update_complex_oid(root_obj_t *root_obj,
			    operations_controllers_t *op_controller,
			    int parent_idx, int *curr_idx,
			    seismic_object_oid_oh_t *complex_gather);



/* Function responsible for adding a variation under the
 * root object
 *
 * \param[in]   root_obj	pointer to root object
 * \param[in]	key		the new variation to be added
 * \param[in]	num_of_keys	number of keys
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
root_obj_add_new_variation(root_obj_t *root_obj,
			   char *key,int num_of_keys,
			   operations_controllers_t *op_controller,
			   int parent_idx, int *curr_idx);

/** Function responsible for initializing empty root object
 *
 * \param[in]	root_obj	double pointer to uninitialized root_object
 *
 *
 * \return      0 for successful allocation
 */
int
root_obj_init(root_obj_t **root_obj);

#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_GRAPH_OBJECTS_ROOT_H_ */
