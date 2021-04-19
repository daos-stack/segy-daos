/*
 * gather.h
 *
 *  Created on: Feb 2, 2021
 *      Author: omar
 */

#ifndef INCLUDE_DSG_GRAPH_OBJECTS_GATHER_H_
#define INCLUDE_DSG_GRAPH_OBJECTS_GATHER_H_

#define SEIS_MAX_PATH NAME_MAX
#define KEY_SEPARATOR "_"
#define DS_A_UNIQUE_VAL "Unique_value"
#define DS_D_NGATHERS "Number_of_gathers"
#define DS_A_NGATHERS "Number_of_gathers"
#define DS_A_GATHER_TRACE_OIDS "TRACE_OIDS_OBJECT_ID"
#define DS_A_NTRACES "Number_of_traces"
#define DS_D_DKEYS_LIST "Dkeys_list"
#define DS_A_DKEYS_LIST "Dkeys_list"
#define	DS_D_SKEYS "SECOND_LEVEL_KEYS"
#define	DS_A_KEYS "KEY_"
#define	DS_A_NUM_OF_KEYS "NUMBER_OF_KEYS"
#include "data_types/generic_value.h"
#include "data_structures/doubly_linked_list.h"
#include "daos_primitives/object.h"
#include "daos_primitives/dfs_helpers.h"
#include "data_types/trace.h"
#include "operations/sort.h"
#include "graph_objects/traces_array.h"
#include "graph_objects/trace_hdr.h"
#include "data_types/ensemble.h"
#include "seismic_sources/source_interface.h"
#include "utilities/string_helpers.h"
#include "utilities/error_handler.h"

/* enum to specify which data structure to be used to hold gathers */
typedef enum gather_data_structure{
	LINKED_LIST,
	BPLUS_TREE
}gather_data_structure;

/* struct representing each individual gather with its own unique key */
typedef struct gather_node{
	/*number of traces under this gather*/
	int number_of_traces;
	/* array of trace oids */
	daos_obj_id_t* oids;
	int curr_oids_index;
	/* unique value shared by all traces under this gather */
	generic_value unique_key;
	/* node member for linked list construction */
	node_t n;
}gather_node_t;

/* struct representing the gathers list */
typedef struct gathers_list{
	/* linked list of gather nodes */
	doubly_linked_list_t* gathers;
}gathers_list_t;

/* struct representing a seismic gather object */
typedef struct gather_obj{
	/** DAOS object ID & open handle*/
	seismic_object_oid_oh_t oid_oh;
	/** daos object mode */
	int	daos_mode;
	/** entry name of the object */
	char*			name;
	/** number of gathers */
	int 			number_of_gathers;
	/**linked list of gathers */
	gathers_list_t 		*gathers_list;
	/** pointer to array of trace_oids objects*/
	seismic_object_oid_oh_t *trace_oids_ohs;

	generic_value		*unique_values;
	/* io parameters to be used for fetch and updates */
	object_io_parameters_t *io_parameters;
	int 				num_of_keys;
	char 				**keys;
}gather_obj_t;

/** Function responsible for creating and allocating a seismic gather
 *  object
 *
 * \param[in]	gather_obj	pointer to uninitialized gather object
 * \param[in]   gather_ds   	enum representing which data structure to be
 * 				initialized to hold the gathers
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_create(gather_obj_t **gather_obj, char* name, int flags,
		  gather_data_structure gather_ds);


/** Function responsible for opening a seismic gather
 *  object
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
gather_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
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
 * \param[in]	release_gather  flag to release gather object once it's closed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_close(gather_obj_t *gather_obj,
		 operations_controllers_t *op_controller,
		 int parent_idx, int *curr_idx, int release_gather);

/* Function responsible for releasing an allocated seismic
 * gather object
 *
 * \param[in]	gather_obj	seismic gather object to be released
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_release(gather_obj_t *gather_obj);


/** Function responsible for destroying a seismic gather
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
gather_obj_punch(gather_obj_t *gather_obj,
		 operations_controllers_t *op_controller,
		 int punch_flags, int parent_idx, int *curr_idx);

/* Function responsible for getting an allocated seismic
 * gather object's oid and open handle
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	seismic object oid_oh struct pointer
 */
seismic_object_oid_oh_t*
gather_obj_get_oid_oh(gather_obj_t *gather_obj);

/* Function responsible for getting an allocated seismic
 * gather object's number of gathers
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	number of gathers
 */
int
gather_obj_get_number_of_gathers(gather_obj_t *gather_obj);

/* Function responsible for getting an allocated seismic
 * gather object's mode
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	daos mode
 */
int
gather_obj_get_mode(gather_obj_t *gather_obj);

/* Function responsible for getting an allocated seismic
 * gather object's gathers_list
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	pointer to gathers list
 */
gathers_list_t**
gather_obj_get_list_of_gathers(gather_obj_t *gather_obj);


/* Function responsible for getting an allocated seismic
 * gather object's name
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	name of gather object
 */
char*
gather_obj_get_name(gather_obj_t *gather_obj);

/* Function responsible for getting an allocated seismic
 * gather object's dkeys_list
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	pointer to array of dkeys_list
 */
generic_value*
gather_obj_get_unique_values(gather_obj_t *gather_obj);

/* Function responsible for getting an allocated seismic
 * gather object's array of trace oids and open handles
 *
 * \param[in]	gather_obj	seismic gather object
 *
 * \return	array of trace oids and open handles
 */
seismic_object_oid_oh_t**
gather_obj_get_trace_oids(gather_obj_t *gather_obj);

/** Function responsible for fetching data from a seismic gather
 *  object
 *
 * \param[in]	gather_obj	pointer to seismic gather object
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
gather_obj_fetch(gather_obj_t *gather_obj,
		 operations_controllers_t *op_controller,
		 int parent_idx, int *curr_idx);

/** Function responsible for updating data in a seismic gather
 *  object
 *
 * \param[in]	gather_obj	pointer to seismic gather object
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
gather_obj_update(gather_obj_t *gather_obj,
		  operations_controllers_t *op_controller,
		  int parent_idx, int *curr_idx);



/** Function responsible for initializing gather object io operation parameters.
 *
 * \param[in]	    gather_obj		Pointer to gather object
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
gather_obj_init_io_parameters(gather_obj_t *gather_obj, uint64_t op_flags,
			      char *dkey_name, unsigned int num_of_iods_sgls);

/** Function responsible for setting gather object io operation parameters.
 *
 * \param[in]	    gather_obj		Pointer to gather object
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
gather_obj_set_io_parameters(gather_obj_t *gather_obj, char *akey_name,
			     daos_iod_type_t type, uint64_t iod_size,
			     unsigned int iod_nr, uint64_t *rx_idx,
			     uint64_t *rx_nr, char *data, size_t data_size);

/** Function responsible for releasing all the allocated memory in
 *  object_io parameters struct
 *
 * \param[in]	    gather_obj		pointer to gather_object
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
gather_obj_release_io_parameters(gather_obj_t *gather_obj);



/** Function responsible for adding one more gather to the linked list of
 *  existing gathers under seismic_objects.
 *  It is called if the unique header value of a trace doesn't exist to an
 *  existing gather node in the linked list of gathers.
 *
 * \param[in]	gather		pointer to the gather that will be added
 * 				to the linked list of gathers(gather_list).
 * \param[in]	list		pointer of the gathers list,
 * 				new node will be created and linked
 * 				to this list.
 *
 * \return	0 if successful,
 * 		error code otherwise
 *
 */
int
gather_obj_add_gather(gather_node_t* gather, gathers_list_t* list);

/** Function responsible for checking gather unique value, if it exists
 *  in any of the existing gathers under seismic_objects.
 *  It is called to check if the target value exists.
 *  If Yes--> number of traces belonging to this gather is incremented by 1
 *  and the trace header object id is also added.
 *  ELSE--> it returns with false
 *
 * \param[in]	target		target value to be checked if it exists or not.
 * \param[in]	key		string containing the header key of each
 * 				seismic object to check its value.
 * \param[in]	gathers_list	pointer to linked list of gathers holding
 * 				all gathers data.
 * \param[in]	trace_obj_id	oid of the trace object.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
gather_obj_check_gather(generic_value target, char *key,
			gathers_list_t *gathers_list,
			daos_obj_id_t trace_obj_id);


/* Function responsible for releasing an allocated gather node
 *
 * \param[in]	g	pointer to gather node
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_destroy_gather_node(void* g);

/* Function responsible for linking a trace to the gathers list
 * under a gather object
 *
 * \param[in]	trace		pointer to trace variable
 * \param[in]   gather_obj	pointer to gather object
 * \param[in]	key		key of gather lists
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
trace_linking(trace_t* trace,gather_obj_t* gather_obj, char* key,
	      operations_controllers_t *op_controller, int parent_idx,
	      int* curr_idx);


/* Function responsible for writing number of gathers under the
 * gather object in the seismic graph
 *
 * \param[in]   gather_obj	pointer to gather object
 * \param[in]	num_of_gathers	pointer to num of gathers to be updated
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
gather_obj_update_num_of_gathers(gather_obj_t *gather_obj, int *num_of_gathers,
				 operations_controllers_t *op_controller,
				 int parent_idx, int *curr_idx);


/* Function responsible for updating array of traces array object
 * object ids under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	pointer to gather object
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
gather_obj_dump_gathers_list_in_graph(gather_obj_t *gather_obj,
				      operations_controllers_t *op_controller,
				      int parent_idx,int *curr_idx,
				      int curr_num_gathers);


/* Function responsible for fetching number of gathers
 * under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	pointer to gather object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[out]  num_of_gathers  address of an integer variable to hold the fetch result
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_num_of_gathers(gather_obj_t *gather_obj,
				operations_controllers_t *op_controller,
				int *num_of_gathers,
				int parent_idx, int *curr_idx);


/* Function responsible for initializing a gather object variable (or loading
 * it from graph if already initialized)
 *
 * \param[in]   gather_obj	double pointer to gather object
 * \param[in]   oid_oh		object id and open handle
 * \param[in]   name		name of gather object
 * \param[in]   initialized	initialized flag to load from graph if applicable
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_entries(gather_obj_t** gather_obj,
			 seismic_object_oid_oh_t oid_oh,
			 char* name, int initialized);


/* Function responsible for fetching array of unique values
 * under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	pointer to gather object
 * \param[in]	op_controller	operation controller struct holding event
 *  				and transaction handles that will be used
 *  				during the daos array object update operation.
 * \param[out]  unique_values   array of generic values to hold the fetch result
 * \param[in]	parent_idx	id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	current index of event initialized in case
 *  				daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_dkeys_list(gather_obj_t *gather_obj,
			    operations_controllers_t *op_controller,
			    uint64_t st_idx,uint64_t nrecords,
			    generic_value *unique_values,
			    int parent_idx, int *curr_idx);


/* Function responsible for fetching number of traces under a specific gather
 * under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	gather_dkey_name name of gather whose number traces is fetched
 * \param[out]  num_of_traces    address of an integer variable to hold the fetch result
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_gather_num_of_traces(gather_obj_t *gather_obj,
				      operations_controllers_t *op_controller,
				      char* gather_dkey_name,
				      int *num_of_traces,
				      int parent_idx, int *curr_idx);

/* Function responsible for fetching array of traces array object
 * object ids under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	pointer to gather object
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
gather_obj_fetch_gather_traces_array_oid(gather_obj_t *gather_obj,
					 operations_controllers_t *op_controller,
					 char* gather_dkey_name,
					 daos_obj_id_t* oid,
					 int parent_idx, int *curr_idx);


/* Function responsible for fetching unique value represented by a specific gather
 * under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	gather_dkey_name name of gather whose number traces is fetched
 * \param[out]  val	    	 address of a generic value variable to hold the fetch result
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_gather_unique_value(gather_obj_t *gather_obj,
				     operations_controllers_t *op_controller,
				     char* gather_dkey_name,
				     generic_value* val,
				     int parent_idx, int *curr_idx);


/* Function responsible for creating/updating a traces array
 * object under a specific gather
 * under the gather object in the seismic graph
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 * \param[in]	curr_num_gathers current number of gathers to check if new traces array
 * 				 needs to be computed
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_create_traces_array(gather_obj_t *gather,
			       operations_controllers_t *op_controller,
			       int parent_idx,
			       int *curr_idx, int curr_num_gathers);


/* Function responsible for fetching the meta-data and traces
 *  under a gather object in an ensemble list
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[out]	ensembles_list	 ensembles list object to be fetched into
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_gather_metadata_and_traces(gather_obj_t *gather_obj, char *dkey,
					    operations_controllers_t *op_controller,
					    ensemble_list *ensembles_list,
					    int parent_idx, int *curr_idx);

/* Function responsible for updating the meta-data and traces
 *  under a gather object from an ensemble list
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	ensembles_list	 ensembles list object to be read from
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_gather_metadata_and_traces(gather_obj_t *gather_obj,
					     char *dkey,
					     operations_controllers_t *op_controller,
					     ensemble_list *ensembles,
					     int ensemble_index,
					     int parent_idx, int *curr_idx);


/* Function for initializing an array of strings representing the dkeys
 * under a gather gather sorted ascendingly or descendingly by value
 *
 * \param[in]	gather_obj	pointer to gather object
 * \param[out]	gather_dkeys	array of strings carrying dkeys
 * \param[in]	direction	direction flag to identify direction of sorting (1 for ascending,
 * 				-1 for descending)
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
gather_obj_prepare_dkeys(gather_obj_t *gather_obj, char **gather_dkeys,
			 int direction, int num_of_gathers);


/* Function for fetching an array of strings representing the dkeys
 * stored under a gather
 *
 * \param[in]	seismic_obj	pointer to gather object
 * \param[in]	num_of_gathers	number of gathers under the seismic object
 * \param[out]	dkeys_list	array of strings carrying dkeys
 */
int
gather_obj_get_dkeys_array(gather_obj_t *seismic_obj, int num_of_gathers,
			   char **dkeys_list);

/* Function responsible for updating the dkeys list
 *  under a gather object
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	st_idx		 record index for updating in graph
 * \param[in]	num_of_records	 number of records to be stored in graph
 * \param[in]	values		 values array to be written under the seimsic object
 * 				 in graph
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_dkeys_list(gather_obj_t *gather_obj, int st_idx,
			     int num_of_records, generic_value *values,
			     operations_controllers_t *op_controller,
			     int parent_idx, int *curr_idx);

/* Function for getting the number of a-keys
 * under a specific d-key stored under a gather
 *
 * \param[in]	gather_obj		pointer to gather object
 * \param[in]	dkey			dkey under the gather object
 * \param[in]	expected_ num_of_keys	expected number of records to be read
 *
 * return	number of a-keys found under the given d-key
 */
int
gather_obj_get_dkey_num_of_akeys(gather_obj_t *gather_obj, char *dkey,
				 int expected_num_of_keys);

/* Function responsible for updating the number of traces of a specific
 * gather under a gather object
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	dkey		 dkey for the required gather
 * \param[in]	num_of_traces	 number of traces
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_gather_num_of_traces(gather_obj_t *gather_obj, char *dkey,
				       int *num_of_traces,
				       operations_controllers_t *op_controller,
				       int parent_idx, int *curr_idx);

/* Function responsible for updating the unique value of a specific
 * gather under a gather object
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	dkey		 dkey for the required gather
 * \param[in]	value	 	 generic value to be written
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_gather_unique_value(gather_obj_t *gather_obj, char *dkey,
				      generic_value *value,
				      operations_controllers_t *op_controller,
				      int parent_idx, int *curr_idx);
/* Function responsible for updating the oid of a traces array object
 * under a gather object
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	dkey		 dkey for the required gather
 * \param[in]	oid	 	 oid of traces array object to be written
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_gather_traces_array_oid(gather_obj_t *gather_obj, char *dkey,
					  daos_obj_id_t *oid,
					  operations_controllers_t *op_controller,
					  int parent_idx, int *curr_idx);
/* Function responsible for updating secondary level keys of existing
 * gather object.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the object update operation.
 * \param[in]	num_of_keys	 integer holding number of secondary level
 * 				 keys to be updated.
 * \param[in]	keys		 array of strings holding the keys to update.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_skeys(gather_obj_t *gather_obj,
			operations_controllers_t *op_controller,
			int num_of_keys, char **keys, int parent_idx,
			int *curr_idx);

/* Function responsible for fetching number of existing secondary level keys
 * gather object.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 * \param[in]	num_of_keys	 integer to hold number of secondary level
 * 				 keys
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_num_of_skeys(gather_obj_t *gather_obj, int parent_idx,
			      int *curr_idx, int *num_of_keys);

/* Function responsible for fetching existing secondary level keys
 * gather object.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 * \param[in]	num_of_keys	 integer holding new number of secondary level
 * 				 keys
 * \param[in]	keys		 array of keys to fetch.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_skeys(gather_obj_t *gather_obj,
		       operations_controllers_t *op_controller,
		       int parent_idx, int *curr_idx,
		       int num_of_keys, char **keys);

/* Function responsible for adding new secondary level keys to existing
 * gather object.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	key		 secondary level key to be added to
 * 				 gather object
 * \param[in]	num_of_keys	 integer holding new number of secondary level
 * 				 keys
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_add_new_skey(gather_obj_t *gather_obj, char *key, int num_of_keys,
			operations_controllers_t *op_controller,
			int parent_idx, int *curr_idx);

/* Function responsible for updating gather object array of secondary
 * level object oids.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	dkey		 gather object dkey name
 * \param[in]	keys		 array of strings of second level keys
 * 				 to fetch their oids.
 * \param[in]	num_of_keys	 integer holding number of keys to fetch
 * 				 their oids.
 * \param[in]	oids		 array of oids to fetch in.
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_update_soids(gather_obj_t *gather_obj, seismic_object_oid_oh_t *oids,
		        int num_of_keys, char **keys, char *dkey,
		        operations_controllers_t *op_controller,
		        int parent_idx, int *curr_idx);

/* Function responsible for fetching gather object array of secondary
 * level object oids.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	dkey		 gather object dkey name
 * \param[in]	keys		 array of strings of second level keys
 * 				 to fetch their oids.
 * \param[in]	num_of_keys	 integer holding number of keys to fetch
 * 				 their oids.
 * \param[in]	oids		 array of oids to fetch in.
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the object fetch operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_soids(gather_obj_t *gather_obj,  char *dkey, char **keys,
		       int num_of_keys, seismic_object_oid_oh_t *oids,
		       operations_controllers_t *op_controller,
		       int parent_idx, int *curr_idx);

/* Function responsible for creating and initializing number of gathers and
 * number of secondary level keys in gather object
 *
 * \param[in]	gather_obj	 pointer to gather object
 * \param[in]	key		 name of gather object.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_create_init_new_obj(gather_obj_t **gather_obj, char *key);

/* Function responsible for getting number of  secondary level keys
 * from gather object
 *
 * \param[in]	gather_obj	 pointer to gather object
 *
 * \return	number of secondary level keys.
 */
int
gather_obj_get_num_of_skeys(gather_obj_t *gather_obj);

/* Function responsible for adding new complex gather
 *
 * \param[in]	complex_oid	 oid_oh of complex gather object
 * \param[in]	final_list	 ensembles list holding new gather oids.
 * \param[in]	index		 index of the new gather array,
 * 				 used in setting dkey name.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_add_complex_gather(seismic_object_oid_oh_t complex_oid,
			      ensemble_list *final_list, int index);

/* Function responsible for fetching existing complex gather
 *
 * \param[in]	complex_oid	 oid_oh of complex gather object
 * \param[in]	final_list	 ensembles list to hold gather oids.
 * \param[in]	index		 index of the gather array,
 * 				 used in setting dkey name.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_fetch_complex_gather(seismic_object_oid_oh_t complex_oid,
			        ensemble_list *final_list, int index);


/* Function responsible for fetching the meta-data of specific gather
 * and adding it to a gather list.
 *
 * \param[in]   gather_obj	 pointer to gather object
 * \param[in]	op_controller	 operation controller struct holding event
 *  				 and transaction handles that will be used
 *  				 during the daos array object update operation.
 * \param[in]	parent_idx	 id of parent event to wait on (currently ignored)
 * \param[in]	curr_idx	 current index of event initialized in case
 *  				 daos events are used.
 *
 * \return	rc error code, 0 if successful, failure otherwise
 */
int
gather_obj_add_gathers_metadata_to_list(gather_obj_t *gather_obj,
					operations_controllers_t *op_controller,
					int parent_idx, int *curr_idx);

/** Function responsible for initializing empty gather object
 *
 * \param[in]	gather_obj	double pointer to uninitialized gather_object
 *
 * \return      0 for successful allocation
 */
int
gather_obj_init(gather_obj_t **gather_obj);

#endif /* INCLUDE_DSG_GRAPH_OBJECTS_GATHER_H_ */
