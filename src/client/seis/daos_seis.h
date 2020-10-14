/*
 * daos_seis.h
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

#ifndef DAOS_SEIS_DAOS_SEIS_H_
#define DAOS_SEIS_DAOS_SEIS_H_

#include <dirent.h>
#include <string.h>

#include "dfs_helper_api.h"
#include "daos.h"
#include "daos_fs.h"
#include "daos_seis_internal_functions.h"

/** closes opened root object
 *
 *  \param[in]   root            pointer to opened root seismic object.
 *
 *  \return     0 on success
 *  		error_code otherwise
 */
int
daos_seis_close_root(seis_root_obj_t *segy_root_object);

/** Open root seismic object
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root_path      absolute path of root seismic object.
 *
 * \return      pointer to segy root object
 */
seis_root_obj_t*
daos_seis_open_root_path(dfs_t *dfs, const char *root_path);

/**
 * Fetch total number of traces stored under seismic root object.
 *
 * \param[in]   root            pointer to opened root seismic object.
 *
 * \return      returns the number of traces.
 */
int
daos_seis_get_trace_count(seis_root_obj_t *root);

/**
 * Fetch number of gathers stored under any of the main seismic objects.
 *
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   key            string containing name of target seismic object.
 *
 * \return      returns number of gathers.
 */
int
daos_seis_get_number_of_gathers(seis_root_obj_t *root, char *key);

/**
 * Fetch binary header data stored under seismic root object.
 * User is responsible for freeing the allocated binary header struct.
 *
 * \param[in]   root            pointer to opened root seismic object.
 *
 * \return      returns pointer to struct holding binary header data.
 */
bhed*
daos_seis_get_binary_header(seis_root_obj_t *root);

/**
 * Fetch text header data stored under seismic root object.
 * User is responsible for freeing the allocated text header array.
 *
 * \param[in]   root            pointer to root seismic object.
 *
 * \return      returns pointer to character array holding text header data.
 */
char*
daos_seis_get_text_header(seis_root_obj_t *root);

/**
 * Fetch specific shot traces
 * User is responsible for freeing the allocated traces_list struct.
 *
 * \param[in]   shot_id        shot_id value to lookup and fetch.
 * \param[in]   root           pointer to opened root seismic object.
 *
 * \return      returns head, tail and size of a linked list of shot traces
 * 		each node will be holding a trace header and data.
 */
traces_metadata_t*
daos_seis_get_shot_traces(int shot_id, seis_root_obj_t *root);

/** Parse segy file and build equivalent daos-seismic graph
 *
 * \param[in]   dfs            	pointer to mounted DAOS file system.
 * \param[in]   segy_root     	pointer to segy file that will be parsed.
 * \param[in]	root_obj	pointer to opened root seismic object.
 * \param[in]	seismic_obj	array of allocated pointers to root seismic objects.
 * 				Objects will be opened in case of parsing
 * 				the first segy file, otherwise each object will
 * 				be allocated and opened in the parse function.
 * \param[in]	additional	boolean flag, used in case of parsing multiple
 * 				files to the same graph.
 * 				(0 -> First segy file)
 * 				(1 -> Additional segy file)
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *segy_root, seis_root_obj_t *root_obj,
		     seis_obj_t **seismic_obj, int additional);

/** Sort traces headers based on any number of primary and optionally secondary
 *  keys with either, ascending (+) or descending (-) order.
 *  It returns a list of sorted traces.
 *  User is responsible for freeing the allocated traces_list struct.
 *
 *
 * \param[in]   root           		pointer to opened root seismic object.
 * \param[in]	number_of_keys 		number of sorting keys.
 * \param[in]   sort_keys      		array of strings containing sorting key
 * 					headers.
 * \param[in]	directions	   	array of sorting orders(descending)(-)
 * 					or ascending(+)) of each key header.
 * \param[in]	number_of_window _keys	number of window keys, in case of
 * 					windowing headers after sorting.
 * \param[in] 	window_keys	   	array of strings containing window key
 * 					headers.
 * \param[in]	type		   	array of types of each window header key
 * 					as defined in su_helpers.h
 * \param[in] 	min_keys	   	array of VALUE(su_helpers.h) containing
 * 					window header keys minimum values.
 * \param[in]	max_keys	   	array of VALUE(su_helpers.h) containing
 * 					window header keys maximum values.
 *
 * \return      pointer to traces_list including pointers to head, tail
 * 		and size of linked list of traces headers
 * 		and data after sorting and optionally after windowing.
 */
traces_metadata_t*
daos_seis_sort_traces(seis_root_obj_t *root, int number_of_keys,
		      char **sort_keys, int *directions,
		      int number_of_window_keys, char **window_keys,
		      cwp_String *type, Value *min_keys, Value *max_keys);

/** Window traces by keyword based on min and max values.
 *  User is responsible for freeing the allocated traces_list struct.
 *
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   window_keys    array of strings containing window key headers.
 * \param[in]	number_of_keys number of window keys.
 * \param[in]   min_keys       array of VALUE(su_helpers.h) containing window
 * 			       header keys minimum values.
 * \param[in]   max_keys       array of VALUE(su_helpers.h) containing window
 * 			       header keys maximum values based on key type.
 * \param[in]	type	       array of types of each window header key as
 * 			       defined in su_helpers.h
 *
 * \return      pointer to traces_list including pointers to head, tail
 * 		and size of linked list of traces headers
 * 		and data after applying window.
 */
traces_metadata_t*
daos_seis_wind_traces(seis_root_obj_t *root, char **window_keys,
		      int number_of_keys, Value *min_keys,
		      Value *max_keys, cwp_String *type);

/** Set traces headers (used with Set_headers/ Change_headers)
 *
 * \param[in]   dfs            pointer to mounted DAOS file system.
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   num_of_keys    number of header keys to set their value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in]	keys_2	       array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case).
 * \param[in]	keys_3         array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case).
 * \param[in] 	a	       array of doubles containing values on first
 * 			       trace(set_headers case),
 * 			       or overall shift(change headers case).
 * \param[in] 	b	       array of doubles containing increment values
 * 			       within group(set_headers case).
 * 			       or scale on first input key(change headers case)
 * \param[in] 	c	       array of doubles containing group increments
 * 			       (set_headers case),
 * 			       or scale on second input key(change headers case).
 * \param[in] 	d	       array of doubles containing trace number shifts
 * 			       (set_headers case) or overall scale
 * 			       (change headers case).
 * \param[in] 	j	       array of doubles containing number of elements
 * 			       in group (set headers case only).
 * \param[in]	e	       array of doubles containing exponent on first
 * 			       input key(change headers case only).
 * \param[in] 	f	       array of doubles containing exponent on second
 * 			       input keys(change headers case only).
 * \param[in]   type           type of operation requested whether it is set
 * 			       headers or change headers defined in the enum
 * 			       header_operation_type_t
 * 			       (SET_HEADERS/ CHANGE_VALUES).
 *
 */
void
daos_seis_set_headers(dfs_t *dfs, seis_root_obj_t *root, int num_of_keys,
		      char **keys_1, char **keys_2, char **keys_3,
	 	      double *a, double *b, double *c,
		      double *d, double *j, double *e,
		      double *f, header_operation_type_t type);

/** Get max and min values for non-zero header entries.
 *
 * \param[in]   root           	pointer to opened root seismic object.
 * \param[in]   number_of_keys 	number of header keys to find their range of
 * 				values otherwise it finds all non-zero
 * 				header entries values.
 * \param[in]	keys		array of strings containing keys to get their
 * 				header keys min and max ranges.
 * \param[in]	dim		dim seismic flag (0 -> not dim)
 * 				(1 -> coord in ft) (2 -> coord in m)
 *
 * \return      headers ranges struct holding:
 * 			number of traces
 * 			key min max (first - last).
 * 			north-south-east-west limits of	shot/receiver/midpoint.
 * 			midpoint interval and
 * 			line length if dim.
 */
headers_ranges_t
daos_seis_range_headers(seis_root_obj_t *root, int number_of_keys,
			char **keys, int dim);

/** Get all traces headers key values.
 *
 * \param[in]   root           pointer to opened root seismic object.
 *
 * \return      pointer to traces_list including pointers to head, tail
 * 		and size of linked list of headers values.
 *
 */
traces_metadata_t*
daos_seis_get_headers(seis_root_obj_t *root, char *key);

/** Update traces data
 *
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   trace_list     pointer to list of updated traces data ready
 * 			       to be written in Traces data objects, oids of
 * 			       theses objects is defined in each trace header.
 *
 */
void
daos_seis_set_data(seis_root_obj_t *root, traces_list_t *trace_list);

/** Parse bare traces and add headers.
 *  Traces headers will be created or read if a header file is passed.
 *  Created traces will be written in empty seismic graph.
 *
 *  \param[in]	dfs		pointer to mounted daos file system.
 *  \param[in]	root		pointer to opened root seismic object.
 *  \param[in]	seismic_obj	array of pointers to opened seismic objects.
 *  \param[in]	input_file	pointer to file holding bare traces.
 *  \param[in]	header_file	pointer to file holding traces headers,
 *  				could be NULL.
 *  \param[in]	ns		number of samples that will be used while
 *  				parsing traces.
 *  \param[in]	ftn		fortran flag specifying whether data is
 *  				written from C or Fortran.
 *				0 = data written unformatted from C.
 *				1 = data written unformatted from Fortran.
 *
 */
void
daos_seis_parse_raw_data (dfs_t *dfs, seis_root_obj_t *root,
		       	  seis_obj_t **seismic_obj, dfs_obj_t *input_file,
			  dfs_obj_t *header_file, int ns, int ftn);
/** Release traces metadata struct
 *  It will release all traces metadata and data previously allocated.
 *
 *  \param[in]	traces_metadata		pointer to traces metadata struct.
 */
void
daos_seis_release_traces_metadata(traces_metadata_t *traces_metadata);

/** Function responsible for creating an empty graph
 *  seismic root and gather objects.
 *
 * \param[in]   dfs            	pointer to mounted DAOS file system.
 * \param[in]   parent         	pointer to root dfs object.
 * \param[in]   name          	name of root seismic object that will be created.
 * \param[in]	num_of_keys	Number of strings(keys) that will be used.
 * \param[in]	keys		array of strings containing header_keys that
 * 				will be used to create gather objects.
 * \param[in]	root_obj	double pointer to the seismic root object
 * 				that will be allocated, created and opened.
 * \param[in]	seismic_obj	array of pointers to seismic objects
 * 				to be allocated, created and opened.
 *
 */
void
daos_seis_create_graph(dfs_t *dfs, dfs_obj_t *parent, char *name,
		       int num_of_keys, char **keys,
		       seis_root_obj_t **root_obj, seis_obj_t **seismic_obj);

/** Function responsible for fetching traces data to traces list
 *  struct (linked list of traces)
 *  It is called at the end of sort and window functions.
 *
 *  \param[in]	coh		opened container handle.
 *  \param[in]	head_traces	pointer to the linked list of traces.
 *  \param[in]	daos_mode	array object mode(read only or read/write)
 *
 */
void
daos_seis_fetch_traces_data(daos_handle_t coh, traces_list_t **head_traces,
			    int daos_mode);

/** Function responsible for finding the parent dfs object of file
 *  given its absolute path.
 *  It is called once at the beginning of seismic_object_creation.
 *  Function.
 *
 * \param[in]   dfs             pointer to mounted DAOS file system.
 * \param[in	file_directory  absolute path of file to find its parent object.
 * \param[in]	allow_creation  boolean flag to allow creation of directories
 * 				in the path in case they doesn't exist.
 * \param[in]	file_name	array of characters containing name of the
 * 				file.
 * \param[in]	verbose_output	boolean flag to enable verbosity.
 *
 * \return	pointer to opened parent dfs object, seismic root object
 * 		will be created later in the parse_segy_file
 * 		under the opened parent
 */
dfs_obj_t*
dfs_get_parent_of_file(dfs_t *dfs, const char *file_directory,
		       int allow_creation, char *file_name,
		       int verbose_output);

/** Function responsible for converting the trace struct back
 *  to the original segy struct(defined in segy.h)
 *
 * \param[in]	trace	pointer to the trace struct that will be converted
 *
 * \return	segy struct.
 *
 */
segy*
daos_seis_trace_to_segy(trace_t *trace);

#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
