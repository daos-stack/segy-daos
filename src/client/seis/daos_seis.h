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
//#include "dfs_helper_api.h"

#define SEIS_MAX_PATH NAME_MAX
#define DS_D_FILE_HEADER "File_Header"
#define DS_A_NEXTENDED_HEADER "Number_Extended_Headers"
#define DS_A_TEXT_HEADER "Text_Header"
#define DS_A_BINARY_HEADER "Binary_Header"
#define DS_A_EXTENDED_HEADER "Extended_Text_Header"
#define DS_D_TRACE_HEADER "Trace_Header"
#define DS_A_TRACE_HEADER "File_Trace_Header"
#define DS_D_TRACE_DATA "Trace_Data_"
#define DS_A_TRACE_DATA "File_Trace_Data"
#define DS_D_SORTING_TYPES "Sorting_Types"
#define DS_A_SHOT_GATHER "Shot_Gather"
#define DS_A_CMP_GATHER "Cmp_Gather"
#define DS_A_OFFSET_GATHER "Offset_Gather"


typedef struct segy_root_obj segy_root_obj_t;
typedef struct seis_obj seis_obj_t;
typedef struct trace_obj trace_obj_t;


/** object struct that is instantiated for SEGYROOT open object */
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
	daos_obj_id_t		offset_oid;
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
	/** current sequence number */
	int sequence_number;
	/** number of traces */
	int number_of_traces;
};

/** object struct that is instantiated for a Seismic open object */
struct trace_obj {
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
	/**trace header */

};



int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_root);

///** returns pointer to segy root object */
segy_root_obj_t* daos_seis_open_root(dfs_t *dfs, dfs_obj_t *root);
//
///** returns pointer to segy root object */
//segy_root_obj_t* daos_seis_open_root(dfs_t *dfs, char *directory_name);

/** Returns number of traces in segyroot file.
 * equivalent to sutrcount command.
 */
int daos_seis_get_trace_count(segy_root_obj_t *root);


/** Read from SEGY file with offset */

/** Update object Akeys single values*/

/** Update object Akeys array values*/



#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
