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
//#include "segy.h"

//#include "dfs_helper_api.h"

#define SEIS_MAX_PATH NAME_MAX
#define DS_D_FILE_HEADER "File_Header"
#define DS_A_NEXTENDED_HEADER "Number_Extended_Headers"
#define DS_A_TEXT_HEADER "Text_Header"
#define DS_A_BINARY_HEADER "Binary_Header"
#define DS_A_EXTENDED_HEADER "Extended_Text_Header"
#define DS_A_NTRACES_HEADER "Number_of_Traces"
#define DS_D_TRACE_HEADER "Trace_Header"
#define DS_A_TRACE_HEADER "File_Trace_Header"
#define DS_D_TRACE_DATA "Trace_Data_"
#define DS_A_TRACE_DATA "File_Trace_Data"
#define DS_D_SORTING_TYPES "Sorting_Types"
#define DS_A_SHOT_GATHER "Shot_Gather"
#define DS_A_CMP_GATHER "Cmp_Gather"
#define DS_A_OFFSET_GATHER "Offset_Gather"
#define DS_D_SHOT "Shot_"
#define DS_D_CMP "Cmp_"
#define DS_D_OFFSET "Off_"
#define DS_A_TRACE "TRACE_"
#define DS_D_NGATHERS "Number_of_gathers"
#define DS_A_NGATHERS "Number_of_gathers"
#define DS_A_TRACE_OIDS "Trace_oids"
#define DS_A_SHOT_ID "Shot_id"
#define DS_A_NTRACES "Number_of_traces"
#define DS_A_CMP_VAL "Cmp_value"
#define DS_A_OFF_VAL "Offset_value"

typedef struct segy_root_obj segy_root_obj_t;
typedef struct seis_obj seis_obj_t;
typedef struct trace_obj trace_obj_t;
typedef struct seis_gather seis_gather_t;
typedef struct seismic_entry seismic_entry_t;

//struct seis_gather{
//	/** number of traces under specific gather */
//	int number_of_traces;
//	/** array of object ids under specific gather*/
//	daos_obj_id_t *oids;
//
//	int nkeys;
//	float *keys;
//
//};
//
///** object struct that is instantiated for SEGYROOT open object */
//struct segy_root_obj {
//	/** DAOS object ID */
//	daos_obj_id_t		oid;
//	/** DAOS object open handle */
//	daos_handle_t		oh;
//	/** mode_t containing permissions & type */
//	mode_t			mode;
//	/** open access flags */
//	int			flags;
//	/** DAOS object ID of the CMP object */
//	daos_obj_id_t		cmp_oid;
//	/** DAOS object ID of the SHOT object */
//	daos_obj_id_t		shot_oid;
//	/** DAOS object ID of the GATHER object */
//	daos_obj_id_t		offset_oid;
//	/** entry name of the object */
//	char			name[SEIS_MAX_PATH + 1];
//	/** number of traces */
//	int 	number_of_traces;
//	/** number of extended text headers */
//	int 	nextended;
//};
//
///** object struct that is instantiated for a Seismic open object */
//struct seis_obj {
//	/** DAOS object ID */
//	daos_obj_id_t		oid;
//	/** DAOS object open handle */
//	daos_handle_t		oh;
//	/** mode_t containing permissions & type */
//	mode_t			mode;
//	/** open access flags */
//	int			flags;
//	/** DAOS object ID of the parent of the object */
//	daos_obj_id_t		parent_oid;
//	/** entry name of the object */
//	char			name[SEIS_MAX_PATH + 1];
//	/** current sequence number */
//	int sequence_number;
//	/** number of gathers */
//	int number_of_gathers;
//	/**array of gathers */
//	seis_gather_t *gathers;
//};


///** object struct that is instantiated for a Seismic trace object */
//struct trace_obj {
//	/** DAOS object ID */
//	daos_obj_id_t		oid;
//	/** DAOS object open handle */
//	daos_handle_t		oh;
//	/** mode_t containing permissions & type */
//	mode_t			mode;
//	/** open access flags */
//	int			flags;
//	/** DAOS object ID of the parent of the object */
//	daos_obj_id_t		parent_oid;
//	/** entry name of the object */
//	char			name[SEIS_MAX_PATH + 1];
//	/**trace header */
//	segy *trace;
//};



int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_root);

/** returns pointer to segy root object */
segy_root_obj_t* daos_seis_open_root(dfs_t *dfs, dfs_obj_t *root);
/** returns pointer to segy root object */
segy_root_obj_t* daos_seis_open_root_path(dfs_t *dfs, dfs_obj_t *parent, char *root_name);

int daos_seis_close_root(segy_root_obj_t *segy_root_object);

/** Returns number of traces in segyroot file.
 * equivalent to sutrcount command.
 */
int daos_seis_get_trace_count(segy_root_obj_t *root);

int daos_seis_read_binary_header(segy_root_obj_t *segy_root_object);

int daos_seis_read_text_header(segy_root_obj_t *segy_root_object);

int daos_seis_get_cmp_gathers(dfs_t *dfs, segy_root_obj_t *root);

int daos_seis_get_shot_gathers(dfs_t *dfs, segy_root_obj_t *root);

int daos_seis_get_offset_gathers(dfs_t *dfs, segy_root_obj_t *root);

int daos_seis_read_shot_traces(dfs_t* dfs, int shot_id, segy_root_obj_t *segy_root_object);

/** Read from SEGY file with offset */

/** Update object Akeys single values*/

/** Update object Akeys array values*/



#endif /* DAOS_SEIS_DAOS_SEIS_H_ */
