/*
 * seismic_graph_api.h
 *
 *  Created on: Feb 3, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_API_SEISMIC_GRAPH_API_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_API_SEISMIC_GRAPH_API_H_

#include "graph_objects/root.h"
#include "graph_objects/gather.h"
#include "graph_objects/trace_hdr.h"
#include "graph_objects/trace_data.h"
#include "graph_objects/traces_array.h"
#include "graph_objects/complex_gather.h"
#include "seismic_sources/segy.h"
#include "data_types/ensemble.h"
#include "operations/window.h"
#include "operations/calculate_header.h"
#include "operations/range.h"
#include "utilities/string_helpers.h"
#include "utilities/timer.h"

typedef enum{
	DUPLICATE_TRACES,
	NO_TRACES_DUPLICATION,
}duplicate_traces_t;

/** Function responsible for creating an empty graph seismic root under
 *  specific parent daos file system object, also creates num_of_keys empty
 *  gather objects and links them to the root object created.
 *
 * \param[in]   parent         		Root dfs object which root seismic
 * 					object is inserted under.
 * \param[in]   name          		name of create root seismic object.
 * \param[in]	num_of_keys		Number of strings(keys) that will be
 * 					used to	create empty gather objects.
 * \param[in]	keys			array of strings containing header_keys
 * 					that is used to create the empty gather
 * 					objects.
 * \param[in]	root_obj		double pointer to the non allocated
 * 					seismic	root object that will be created.
 * \param[in]	flags			flags used to set the daos object mode.
 * \param[in]	seismic_obj		array of pointers to seismic objects
 * 					to be allocated, created and opened.
 *
 *  \return     0 on success
 *  		error_code otherwise
 */
int
daos_seis_create_graph(dfs_obj_t *parent, char *name, int num_of_keys,
		       char **keys, root_obj_t **root_obj, int flags,
		       int graph_permissions_and_type);

/** Open seismic graph
 *
 * \param[in]   path      		absolute path of root seismic object.
 * \param[in]	flags	 		flags used to lookup the dfs object.
 *
 * \return      pointer to opened seismic root object
 */
root_obj_t*
daos_seis_open_graph(const char *path, int flags);

/** Close opened seismic graph
 *
 * \param[in]   root_obj        	pointer to opened root seismic object.
 *
 * \return      0 on success
 *  		error_code otherwise
 */
int
daos_seis_close_graph(root_obj_t *root_obj);

/** Parse segy file and build the needed indexing for the previously created
 *  seismic objects.
 *
 * \param[in]   segy_root     	 	pointer to dfs object pointing to the
 * 				 	previously mounted segy file to
 * 				 	be parsed.
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_parse_segy(dfs_obj_t *segy_root, root_obj_t *root_obj);

/** Parse bare traces and add headers.
 *  Traces headers will be created or read if a header file is passed.
 *
 * \param[in]   raw_root     	 	pointer to dfs object pointing to the
 * 				 	previously mounted raw data file to
 * 				 	be parsed.
 * \param[in]   hdr_root     	 	pointer to dfs object pointing to the
 * 					previously mounted headers file to
 * 					be parsed.
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]	ns			number of samples that will be used while
 * 					parsing traces data.
 * \param[in]	ftn			fortran flag specifying whether data is
 *  					written from C or Fortran.
 *					0 = data written unformatted from C.
 *				 	1 = data written unformatted from
 *				 	    Fortran.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_parse_raw_data(dfs_obj_t *raw_root, dfs_obj_t *hdr_root,
			 root_obj_t *root_obj, int ns, int ftn);

/** Function responsible for parsing linked list of traces and build the needed
 *  indexing for the previously created seismic objects.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]	e		 	pointer to linked list of ensembles to
 * 					parse.
 * \param[in]	duplicate_traces 	enum specifying whether to create new
 * 					trace objects or not.
 * \param[in]	parsing_helpers		pointer to parsing helpers struct
 * 					ex: mpi mutex locks.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_parse_linked_list(root_obj_t *root_obj, ensemble_list *e,
			    duplicate_traces_t duplicate_traces,
			    parsing_helpers_t *parsing_helpers);

/**
 * Fetch total number of traces stored under seismic root object.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 *
 * \return      number of traces.
 */
int
daos_seis_get_num_of_traces(root_obj_t *root_obj);

/**
 * Fetch text header data stored under seismic root object.
 * User is responsible for freeing the allocated text header array.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 *
 * \return      character array holding text header data.
 */
char*
daos_seis_get_text_header(root_obj_t *root_obj);

/**
 * Fetch binary header data stored under seismic root object.
 * User is responsible for freeing the allocated binary header struct.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 *
 * \return      pointer to struct holding binary header data.
 */
bhed*
daos_seis_get_binary_header(root_obj_t *root_obj);

/**
 * Fetch number of gathers under seismic object.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   key            		string containing name of target
 * 					seismic object.
 *
 * \return      number of gathers.
 */
int
daos_seis_get_num_of_gathers(root_obj_t *root_obj, char *key);

/** Fetch traces headers customized key-values.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]	keys			array of strings holding keys to fetch
 * 					their header values.
 * \param[in]	num_of_keys		number of keys to fetch their values.
 *
 * \return      pointer to array of key value pairs holding all values for
 * 		each key requested.
 */
key_value_pair_t*
daos_seis_get_custom_headers(root_obj_t *root_obj, char **keys, int num_of_keys);

/** Window traces by keyword based on min and max values.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   window_keys   		array of strings containing window key
 * 					headers.
 * \param[in]	num_of_keys 		number of window keys.
 * \param[in]   min_keys       		array of generic_value containing
 * 					window header keys minimum values.
 * \param[in]   max_keys       		array of generic_value containing
 * 					window header keys maximum values.
 *
 * \return      pointer to linked list of ensembles holding all traces
 * 		falling in the requested range.
 */
ensemble_list*
daos_seis_wind_headers_return_traces(root_obj_t *root, char **window_keys,
				     int num_of_keys, generic_value *min_keys,
				     generic_value *max_keys);

/** Window traces by keyword based on min and max values.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]	new_root	 	pointer to new opened root seismic object.
 * \param[in]	duplicate_traces 	enum specifying whether to create new
 * 					trace objects or not.
 * \param[in]   window_keys   		array of strings containing window key
 * 					headers.
 * \param[in]	num_of_keys 		number of window keys.
 * \param[in]   min_keys       		array of generic_value containing
 * 					window header keys minimum values.
 * \param[in]   max_keys       		array of generic_value containing
 * 					window header keys maximum values.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_wind_headers_then_dump_in_graph(root_obj_t *root_obj,
					  root_obj_t *new_root,
					  duplicate_traces_t duplicate_traces,
					  char **window_keys, int num_of_keys,
					  generic_value *min_keys,
					  generic_value *max_keys);

/** Update traces data
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]	e		 	pointer to linked list of ensembles
 * 					holding the new traces data.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_set_traces_data(root_obj_t* root_obj, ensemble_list* e);

/** Set traces headers
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   num_of_keys    		number of header keys to set their value.
 * \param[in]	keys_1	       		array of strings containing header keys
 * 			       		to set their header value.
 * \param[in] 	a	       		array of doubles containing values on
 * 					first trace.
 * \param[in] 	b	      		array of doubles containing increment
 * 					values within group.
 * \param[in] 	c	       		array of doubles containing group
 * 					increments.
 * \param[in] 	d	      		array of doubles containing trace number
 * 					shifts.
 * \param[in]	e	       		array of doubles containing exponent on
 * 					first input key.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_set_headers(root_obj_t* root_obj, int num_of_keys,char** keys1,
		      double* a, double* b, double* c, double* d, double* e);

/** Change traces headers
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   num_of_keys    		number of header keys to set their value.
 * \param[in]	keys_1	       		array of strings containing header keys
 * 			       		to set their header value.
 * \param[in]	keys_1	       		array of strings containing header keys
 * 			       		to use their header value while setting
 * 			    		keys_1.
 * \param[in]	keys_1	       		array of strings containing header keys
 * 			       		to use their header value while setting
 * 			    		keys_1.
 * \param[in] 	a	       		array of doubles containing values on
 * 					first trace.
 * \param[in] 	b	      		array of doubles containing increment
 * 					values within group.
 * \param[in] 	c	       		array of doubles containing group
 * 					increments.
 * \param[in] 	d	      		array of doubles containing trace number
 * 					shifts.
 * \param[in]	e	       		array of doubles containing exponent on
 * 					first input key.
 * \param[in] 	f	      		array of doubles containing exponent on
 * 					second input keys.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_change_headers(root_obj_t* root_obj, int num_of_keys,char** keys1,
			 char** keys2,char** keys3, double* a, double* b,
			 double* c, double* d, double* e, double* f);

/**
 * Fetch specific shot traces
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   shot_id        		shot_id value to lookup and fetch.
 *
 * \return      pointer to linked list of ensembles holding all traces
 * 		of this shot otherwise NULL.
 */
ensemble_list*
daos_seis_get_shot_traces(root_obj_t *root_obj, int shot_id);

/** Sort traces headers based on any number of primary and optionally secondary
 *  keys with either, ascending (+) or descending (-) order.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   sort_keys      		array of strings containing sorting key
 * 					headers.
 * \param[in]	directions	   	array of sorting orders(descending)(-)
 * 					or ascending(+)) of each key header.
 * \param[in]	num_of_keys 		number of sorting keys.
 * \param[in] 	window_keys	   	array of strings containing window key
 * 					headers.
 * \param[in]	num_of_window _keys	number of window keys, in case of
 * 					windowing headers after sorting.
 * \param[in] 	min_keys	   	array of VALUE(su_helpers.h) containing
 * 					window header keys minimum values.
 * \param[in]	max_keys	   	array of VALUE(su_helpers.h) containing
 * 					window header keys maximum values.
 *
 * \return      pointer to linked list of ensembles holding sorted traces.
 */
ensemble_list*
daos_seis_sort_traces(root_obj_t *root_obj, char **sort_keys, int *directions,
		      int num_of_keys, char **window_keys,
		      int num_of_window_keys, generic_value *min_keys,
		      generic_value *max_keys);

/**
 * Get list of unique values of different gathers under root object.
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   gather_name        	string containing name of target
 * 					seismic object.
 *
 * \return      array of generic values holding all unique keys of specific
 * 		gather.
 */
generic_value*
daos_seis_get_gather_unique_values(root_obj_t *root_obj, char *gather_name);


/** returns the ranges for the specified headers
 *
 * \param[in]	root_obj	 	pointer to opened root seismic object.
 * \param[in]   num_of_keys    		number of header keys to set their value.
 * \param[in]	keys       		array of strings containing header keys
 * \param[in]	dim			dimensionality
 *
 * \return      header ranges object
 */
headers_ranges_t*
daos_seis_range(root_obj_t* root_obj, int number_of_keys, char** keys, int dim);

#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_API_SEISMIC_GRAPH_API_H_ */
