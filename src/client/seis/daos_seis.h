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
#include "su_helpers.h"
#include "dfs_helpers.h"
#include "daos_seis_internal_functions.h"
#include "daos.h"
#include "daos_fs.h"

/** closes opened root object
 *
 *  \param[in]   root            pointer to root seismic object.
 *
 *  \return     0 on success
 *  			error_code otherwise
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
 * \param[in]   root            pointer to root seismic object.
 *
 * \return      returns the number of traces.
 */
int
daos_seis_get_trace_count(seis_root_obj_t *root);

/**
 * Fetch number of gathers stored under any of the main seismic objects (CMP/ SHOT/ OFFSET/etc..).
 *
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   key            string containing the name of target seismic object.
 *
 * \return      returns number of gathers.
 */
int
daos_seis_get_number_of_gathers(seis_root_obj_t *root, char *key);

/**
 * Fetch binary header data stored under seismic root object.
 * User is responsible for freeing the allocated binary header struct.
 *
 * \param[in]   root            pointer to root seismic object.
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
 * \param[in]   root           pointer to root seismic object.
 *
 * \return      returns head, tail and size of a linked list of shot traces
 * 		each node will be holding a trace headers and data.
 */
traces_list_t*
daos_seis_get_shot_traces(int shot_id, seis_root_obj_t *root);

/** Parse segy file and build equivalent daos-seismic graph
 *
 * \param[in]   dfs            	pointer to DAOS file system.
 * \param[in]   parent         	pointer to parent DAOS file system object.
 * \param[in]   name          	name of root object that will be create.
 * \param[in]   segy_root     	pointer to file that will be parsed.
 * \param[in]	num_of_keys	Number of strings(keys) in the array of keys.
 * \param[in]	keys		array of strings containing header_keys that
 * 				will be used to create gather objects.
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name,
		     dfs_obj_t *segy_root, int num_of_keys, char **keys);

/** Sort traces headers based on any number of (secondary) keys with either,
 * ascending (+) or descending (-) directions for each.
 * It returns a list of sorted traces.
 *
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]	number_of_keys number of keys to sort on.
 * \param[in]   sort_keys      array of strings containing key headers to sort on.
 * \param[in]	directions	   array of sorting directions(descending)(-) or ascending(+)) of each key header.
 * \param[in]	number_of_window _keys	number of keys to window on, in case of windowing headers after sorting.
 * \param[in] 	window_keys	   array of strings containing key headers to window on.
 * \param[in]	type		   type of window header keys as defined in su_helpers.h
 * \param[in] 	min_keys	   array of VALUE(su_helpers.h) containing window header keys minimum values based on key type.
 * \param[in]	max_keys	   array of VALUE(su_helpers.h) containing window header keys maximum values based on key type.
 *
 * \return      pointer to traces_list including pointers to head, tail and size of linked list of traces headers and data after sorting.
 */
traces_list_t*
daos_seis_sort_traces(seis_root_obj_t *root, int number_of_keys,
		      char **sort_keys, int *directions,
		      int number_of_window_keys, char **window_keys,
		      cwp_String *type, Value *min_keys, Value *max_keys);

/** Window traces by keyword based on min and max values.
 *
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   window_keys    array of strings containing key headers to window on.
 * \param[in]	number_of_keys number of keys to window on.
 * \param[in]   min            array of VALUE(su_helpers.h) containing window header keys minimum values based on key type.
 * \param[in]   max            array of VALUE(su_helpers.h) containing window header keys maximum values based on key type.
 * \param[in]	type		   type of window header keys as defined in su_helpers.h
 *
 * \return      pointer to traces_list including pointers to head, tail and size of linked list of traces headers and data after applying window.
 */
traces_list_t*
daos_seis_wind_traces(seis_root_obj_t *root, char **window_keys,
		      int number_of_keys, Value *min_keys,
		      Value *max_keys, cwp_String *type);

/** Set traces headers (used with Add_headers/ Set_headers/ Change_headers)
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   num_of_keys    number of header keys to set their value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in]	keys_2	       array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in]	keys_3         array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in] 	a	       array of doubles containing values on first
 * 			       trace(set_headers case)
 * 			       or overall shift(change headers case)
 * \param[in] 	b	       array of doubles containing increments values
 * 			       within group(set_headers case)
 * 			       or scale on first input key(change headers case)
 * \param[in] 	c	       array of doubles containing group increments
 * 			       (set_headers case) or scale on second input key
 * 			       (change headers case)
 * \param[in] 	d	       array of doubles containing trace number shifts
 * 			       (set_headers case) or overall scale
 * 			       (change headers case)
 * \param[in] 	j	       array of doubles containing number of elements
 * 			       in group (set headers case only)
 * \param[in]	e	       array of doubles containing exponent on first
 * 			       input key(change headers case only)
 * \param[in] 	f	       array of doubles containing exponent on second
 * 			       input keys(change headers case only)
 * \param[in]   type           type of operation requested whether it is set
 * 			       headers or change headers defined in the enum
 * 			       header_operation_type_t
 * 			       (SET_HEADERS/ CHANGE_VALUES).
 *
 * \return      pointer to traces_list including pointers to head, tail
 * 		and size of linked list of traces headers and data
 * 		after setting header key values of each trace.
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
 * 					number of traces
 * 					key min max (first - last).
 * 					north-south-east-west limits of
 * 					 shot/receiver/midpoint.
 * 					midpoint interval and
 * 					 line length if dim.
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
 */
traces_list_t*
daos_seis_get_headers(seis_root_obj_t *root);

/** Update traces data
 *
 * \param[in]   root           pointer to opened root seismic object.
 * \param[in]   trace_list     pointer to list of updated traces data ready
 * 			       to be written in Traces data objects, oids of
 * 			       theses objects is defined in each trace header.
 */
void
daos_seis_set_data(seis_root_obj_t *root, traces_list_t *trace_list);

#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
