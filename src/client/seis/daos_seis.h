/*
 * daos_seis.h
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

#ifndef DAOS_SEIS_DAOS_SEIS_H_
#define DAOS_SEIS_DAOS_SEIS_H_

#include <dirent.h>

#include "daos.h"
#include "daos_fs.h"

#define SEIS_MAX_PATH		NAME_MAX


typedef struct segy_root_obj segy_root_obj_t;


/** object struct that is instantiated for a DFS open object */
struct dfs_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** mode_t containing permissions & type */
	mode_t			mode;
	/** open access flags */
	int			flags;
	/** DAOS object ID of the parent of the object */
	daos_obj_id_t		parent_oid;
	/** entry name of the object in the parent */
	char			name[DFS_MAX_PATH + 1];
	/** Symlink value if object is a symbolic link */
	char			*value;
};

/** object struct that is instantiated for a SEGY ROOT open object */
struct segy_root_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** mode_t containing permissions & type */
	mode_t			mode;
	/** open access flags */
	int			flags;
	/** DAOS object ID of the CMP object */
	daos_obj_id_t		cmp_oid;
	/** DAOS object ID of the SHOT object */
	daos_obj_id_t		shot_oid;
	/** DAOS object ID of the GATHER object */
	daos_obj_id_t		cdp_oid;
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
};

/** object struct that is instantiated for a Seismic open object */
struct seis_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** mode_t containing permissions & type */
	mode_t			mode;
	/** open access flags */
	int			flags;
	/** DAOS object ID of the parent of the object */
	daos_obj_id_t		parent_oid;
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
};

/** Initialize the Graph
 * 1. create segyroot object
 * 2. create cmp/shot/cdp objects
 * 3. create trace object
 */
int daos_seis_init(dfs_t* dfs, dfs_obj_t *parent, char *name);

/** Read from SEGY file with offset */

/** Update object Akeys single values*/

/** Update object Akeys array values*/



#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
