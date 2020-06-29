/*
 * daos_seis_internal_functions.h
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#ifndef LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_
#define LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_

#include "su_helpers.h"
#include "dfs_helpers.h"

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


//typedef struct seismic_entry seismic_entry_t;
//typedef struct seis_gather seis_gather_t;
struct stat *seismic_stat;

typedef struct segy_root_obj segy_root_obj_t;
typedef struct seis_obj seis_obj_t;
typedef struct trace_obj trace_obj_t;
typedef struct seis_gather seis_gather_t;
typedef struct seismic_entry seismic_entry_t;

struct seis_gather{
	/** number of traces under specific gather key */
	int number_of_traces;
	/** array of object ids under specific gather key*/
	daos_obj_id_t *oids;
	/** number of keys
	 * =1 if its shot gather
	 * =2 if its cmp or offset gather
	 */
	int nkeys;
	/** gather unique info */
	int keys[2];
};

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
	/** number of traces */
	int 	number_of_traces;
	/** number of extended text headers */
	int 	nextended;
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
	/** number of gathers */
	int number_of_gathers;
	/**array of gathers */
	seis_gather_t *gathers;
};

/** object struct that is instantiated for a Seismic trace object */
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
	segy *trace;
};


struct seismic_entry {
	char 		*dkey_name;

	char 		*akey_name;

	daos_obj_id_t	oid;

	char		*data;

	int		size;
};



int daos_seis_fetch_entry(daos_handle_t oh, daos_handle_t th, struct seismic_entry *entry);

int daos_seis_array_fetch_entry(daos_handle_t oh, daos_handle_t th,int nrecords, struct seismic_entry *entry);

int daos_seis_array_obj_update(daos_handle_t oh, daos_handle_t th, int nrecords, struct seismic_entry entry);

int daos_seis_th_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *data, int nbytes);

int daos_seis_root_obj_create(dfs_t *dfs, segy_root_obj_t **obj,daos_oclass_id_t cid,
			char *name, dfs_obj_t *parent);

int daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry);

int daos_seis_root_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char* databuf, int nbytes);

int daos_seis_bh_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , bhed *bhdr, int nbytes);

int daos_seis_exth_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *ebcbuf, int index, int nbytes);

int daos_seis_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid, segy_root_obj_t *parent,
			seis_obj_t **shot_obj, seis_obj_t **cmp_obj, seis_obj_t **offset_obj);

int daos_seis_trh_update(dfs_t* dfs, trace_obj_t* tr_obj, segy *tr, int hdrbytes);

int daos_seis_tr_data_update(dfs_t* dfs, trace_obj_t* trace_data_obj, segy *trace);

daos_obj_id_t get_tr_data_oid(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid);

int daos_seis_tr_obj_create(dfs_t* dfs, trace_obj_t **trace_hdr_obj, int index, segy *trace, int nbytes);

int prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid, char *dkey, char *akey,
			char *data,int size);

int daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, segy *trace,
			seis_obj_t *shot_obj, seis_obj_t *cmp_obj, seis_obj_t *off_obj);

int pcreate(int fds[2], const char *command, char *const argv[]);

int execute_command(char *const argv[], char *write_buffer,
    int write_bytes, char *read_buffer, int read_bytes);

#endif /* LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_ */
