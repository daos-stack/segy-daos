/*
 * daos_seis.h
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

#ifndef DAOS_SEIS_DAOS_SEIS_H_
#define DAOS_SEIS_DAOS_SEIS_H_

#include <dirent.h>
#include<string.h>

#include "dfs_helper_api.h"
#include "su_helpers.h"
#include "dfs_helpers.h"
#include "daos_seis_internal_functions.h"
#include "daos.h"
#include "daos_fs.h"

/** Parsing segy file and building equivalent daos-seismic graph  */
int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_root);

/** returns pointer to segy root object */
seis_root_obj_t* daos_seis_open_root(dfs_t *dfs, dfs_obj_t *root);
/** returns pointer to segy root object */
seis_root_obj_t* daos_seis_open_root_path(dfs_t *dfs, dfs_obj_t *parent, const char *root_name);

int daos_seis_close_root(seis_root_obj_t *segy_root_object);


/**
 * Fetch total number of traces stored under seismic root object.
 *
 * \param[in]   root            pointer to root seismic object.
 * \return      returns the number of traces.
 */
int daos_seis_get_trace_count(seis_root_obj_t *root);

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
 * Fetch number of cmp gathers stored under CMP seismic object.
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \return      returns number of cmp gathers.
 */
int daos_seis_get_cmp_gathers(dfs_t *dfs, seis_root_obj_t *root);

/**
 * Fetch number of shot gathers stored under SHOT seismic object.
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \return      returns number of shot gathers.
 */
int daos_seis_get_shot_gathers(dfs_t *dfs, seis_root_obj_t *root);

/**
 * Fetch number of offset gathers stored under OFFSET seismic object.
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \return      returns number of offset gathers.
 */
int daos_seis_get_offset_gathers(dfs_t *dfs, seis_root_obj_t *root);

/**
 * Fetch shot traces
 *
 * \param[in]   dfs            pointer to DAOS file system.
 * \param[in]   root           pointer to root seismic object.
 * \param[in]   name           string containing name of file.
 * \return      returns number of offset gathers.
 */
int daos_seis_read_shot_traces(dfs_t* dfs, int shot_id, seis_root_obj_t *root, char *name);

#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
