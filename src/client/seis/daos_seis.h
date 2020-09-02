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
 *  \return     0 on success
 *  			error_code otherwise
 */
int daos_seis_close_root(seis_root_obj_t *segy_root_object);

/** Open root seismic object
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root_path      path of root seismic object.
 * \return      pointer to segy root object
 */
seis_root_obj_t* daos_seis_open_root_path(dfs_t *dfs, const char *root_path);

/**
 * Fetch total number of traces stored under seismic root object.
 *
 * \param[in]   root            pointer to root seismic object.
 * \return      returns the number of traces.
 */
int daos_seis_get_trace_count(seis_root_obj_t *root);

/**
 * Fetch number of gathers stored under any of the main seismic objects (CDP/ FLDR/ OFFSET).
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   key            name of target seismic object.
 *
 * \return      returns number of gathers.
 */
int daos_seis_get_number_of_gathers(dfs_t *dfs, seis_root_obj_t *root, char *key);

/**
 * Fetch binary header data stored under seismic root object.
 *
 * \param[in]   root            pointer to root seismic object.
 * \return      returns pointer to struct holding binary header data.
 */
bhed* daos_seis_read_binary_header(seis_root_obj_t *root);

/**
 * Fetch text header data stored under seismic root object.
 *
 * \param[in]   root            pointer to root seismic object.
 * \return      returns pointer to character array holding text header data.
 */
char* daos_seis_read_text_header(seis_root_obj_t *root);

/**
 * Fetch shot traces
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   shot_id        shot_id value to lookup and fetch.
 * \param[in]   root           pointer to root seismic object.
 * \return      returns array of traces holding all shot gather traces headers and data.
 */
traces_list_t* daos_seis_read_shot_traces(dfs_t* dfs, int shot_id, seis_root_obj_t *root);


/** Parse segy file and build equivalent daos-seismic graph
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   parent         pointer to parent DAOS file system object.
 * \param[in]   name           name of root object that will be create.
 * \param[in]   segy_root      pointer to file that will be parsed.
 * \return      0 on success
 * 				error_code otherwise
 */
int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_root);

/** Sort traces headers
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   array_keys     array of key headers to sort on.
 * \return      pointer to traces_list including pointers to head, tail and size of linked list of headers after sorting.
 */
traces_list_t* daos_seis_sort_headers(dfs_t *dfs, seis_root_obj_t *root, int number_of_keys, char **sort_keys, int *directions,
							int number_of_window_keys,char **window_keys, cwp_String *type, Value *min_keys, Value *max_keys);

/** Window traces headers
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   array_keys     array of key headers to use in window
 * \param[in]   min            minimum values of key headers to accept.
 * \param[in]   max            maximum values of key headers to accept.
 * \return      pointer to traces_list including pointers to head, tail and size of linked list of headers after applying window.
 */
traces_list_t* daos_seis_wind_traces(dfs_t *dfs, seis_root_obj_t *root, char **window_keys, int number_of_keys,
							Value *min_keys, Value *max_keys, cwp_String *type);

/** Set traces headers (used with Add_headers/ Set_headers/ Change_headers)
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   num_of_keys    number of header keys to set their value.
 * \param[in]	keys_1		   array of keys to set the header value.
 * \param[in]	keys_2		   array of keys to use their header value while setting keys_1(change headers case)
 * \param[in]	keys_3		   array of keys to use their header value while setting keys_1(change headers case)
 * \param[in] 	a			   values on first trace(set_headers case) or overall shift(change headers case)
 * \param[in] 	b			   increments within group(set_headers case) or scale on first input key(change headers case)
 * \param[in] 	c			   group increments(set_headers case) or scale on second input key(change headers case)
 * \param[in] 	d			   trace number shifts(set_headers case) or overall scale (change headers case)
 * \param[in] 	j			   number of elements in group (set headers case only)
 * \param[in]	e			   exponent on first input key(change headers case only)
 * \param[in] 	f			   exponent on second input keys(change headers case only)
 * \param[in]   type           type of operation requested set headers or change headers.
 * \return      pointer to traces_list including pointers to head, tail and size of linked list of headers after applying window.
 */
traces_list_t* daos_seis_set_headers(dfs_t *dfs, seis_root_obj_t *root, int num_of_keys, char **keys_1, char **keys_2, char **keys_3,
								double *a, double *b, double *c, double *d, double *j, double *e, double *f, header_type_t type);

/** Fetch range of traces headers values.
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   num_of_keys    number of header keys to find their range of values.
 * \param[in]	keys		   array of keys to fetch their header min and max ranges.
 * \param[in]	dim			   dim seismic flag (0 -> not dim)(1 -> coord in ft) (2 -> coord in m)
 * \return      number of traces.
 * 				key min max (first - last).
 * 				north-south-east-west limits of shot/receiver/midpoint.
 * 				midpoint interval and line length if dim.
 */
void daos_seis_range_headers(dfs_t *dfs, seis_root_obj_t *root, int number_of_keys, char **keys, int dim);

/** Get all traces headers
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \return      pointer to traces_list including pointers to head, tail and size of linked list of headers after applying window.
 */
traces_list_t* daos_seis_get_headers(dfs_t *dfs, seis_root_obj_t *root);

/** Update traces data
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   trace_list	   pointer to list of updated traces
 */
void daos_seis_update_traces_data(dfs_t *dfs,seis_root_obj_t *root, traces_list_t *trace_list);

#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
