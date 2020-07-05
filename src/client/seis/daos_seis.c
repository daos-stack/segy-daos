/*
 * daos_seis.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

//#include <unistd.h>
//#include "su.h"
//#include "segy.h"
//#include "tapesegy.h"
//#include "tapebhdr.h"
//	#include <stdio.h>
//	#include <sys/types.h>
//	#include <fcntl.h>
//	#include "bheader.h"
//	#include "header.h"
//	#include <sys/stat.h>
//	#include <sys/xattr.h>
//	#include <daos/checksum.h>
//	#include <daos/common.h>
//	#include <daos/event.h>
//	#include <daos/container.h>
//	#include <daos/array.h>
//	#include <string.h>
//#include "dfs_helper_api.h"
//#include "su_helpers.h"
//#include "dfs_helpers.h"
//#include "daos_seis_internal_functions.h"

#include "daos_seis.h"


#define TRACEHDR_BYTES 240

#define GET_MACRO(_1,_2,_3,_4,NAME,...) NAME
#define warn(...) GET_MACRO(__VA_ARGS__, warn4, warn3, warn2, warn1)(__VA_ARGS__)
#define warn1(x) fprintf(stderr,x)
#define warn2(x,y) fprintf(stderr,x,y)
#define warn3(x,y,z) fprintf(stderr,x,y,z)
#define warn4(x,y,z,u) fprintf(stderr,x,y,z,u)

#define err(...) GET_MACRO(__VA_ARGS__, err4, err3, err2, err1)(__VA_ARGS__)
#define err1(x) fprintf(stderr,x);exit(0)
#define err2(x,y) fprintf(stderr,x,y);exit(0)
#define err3(x,y,z) fprintf(stderr,x,y,z);exit(0)
#define err4(x,y,z,u) fprintf(stderr,x,y,z,u);exit(0)



int daos_seis_close_root(segy_root_obj_t *segy_root_object){
	int rc;
//	rc = daos_obj_close(segy_root_object->oh, NULL);
//	free(segy_root_object);
	dfs_obj_t *obj = malloc(sizeof(dfs_obj_t));

	obj->oh = segy_root_object->oh;
	obj->flags = segy_root_object->flags;
	obj->mode = segy_root_object->mode;
	strcpy(obj->name, segy_root_object->name);
	obj->oid = segy_root_object->oid;

	rc = dfs_release(obj);

	return rc;

}

segy_root_obj_t* daos_seis_open_root(dfs_t *dfs, dfs_obj_t *root){

	segy_root_obj_t* root_obj = malloc(sizeof(segy_root_obj_t));
	int rc;
	struct seismic_entry entry ={0};
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;

	root_obj->oid = root->oid;
	root_obj->oh = root->oh;
	root_obj->mode = root->mode;
	root_obj->flags = root->flags;
	strcpy(root_obj->name, root->name);
/** fetch shot oid value */

	rc = prepare_seismic_entry(&entry, root->oid, DS_D_SORTING_TYPES, DS_A_SHOT_GATHER,
			(char*)(&root_obj->shot_oid), sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	if(rc !=0){
		printf("ERROR IN PREPARING SEISMIC ENTRY IN DAOS_SEIS_OPEN_ROOT");
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if(rc !=0){
		return rc;
	}

/** fetch CMP oid value */
	rc = prepare_seismic_entry(&entry, root->oid, DS_D_SORTING_TYPES, DS_A_CMP_GATHER,
			(char*)(&root_obj->cmp_oid), sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	if(rc !=0){
		printf("ERROR IN PREPARING SEISMIC ENTRY IN DAOS_SEIS_OPEN_ROOT");
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if(rc != 0){
		return rc;
	}

/** fetch OFFSET oid value */
	rc = prepare_seismic_entry(&entry, root->oid, DS_D_SORTING_TYPES, DS_A_OFFSET_GATHER,
			(char*)(&root_obj->offset_oid), sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	if(rc !=0){
		printf("ERROR IN PREPARING SEISMIC ENTRY IN DAOS_SEIS_OPEN_ROOT");
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if( rc != 0 ){
		return rc;
	}

/** fetch number of traces */
	rc = prepare_seismic_entry(&entry, root->oid, DS_D_FILE_HEADER, DS_A_NTRACES_HEADER,
			(char*)(&root_obj->number_of_traces), sizeof(int), DAOS_IOD_SINGLE);
	if(rc !=0){
		printf("ERROR IN PREPARING SEISMIC ENTRY IN DAOS_SEIS_OPEN_ROOT");
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if(rc != 0){
		return rc;
	}

/** fetch number of extended text headers */
	rc = prepare_seismic_entry(&entry, root->oid, DS_D_FILE_HEADER, DS_A_NEXTENDED_HEADER,
			(char*)(&root_obj->nextended), sizeof(int), DAOS_IOD_SINGLE);
	if(rc !=0){
		printf("ERROR IN PREPARING SEISMIC ENTRY IN DAOS_SEIS_OPEN_ROOT");
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if(rc != 0){
		return rc;
	}
	return root_obj;
}

segy_root_obj_t* daos_seis_open_root_path(dfs_t *dfs, dfs_obj_t *parent, const char *root_name){
	segy_root_obj_t* root_obj;
	int rc;
	dfs_obj_t *root;
	mode_t *mode = malloc(sizeof(mode_t));
	struct stat *stbuf = malloc(sizeof(struct stat));

	rc = dfs_lookup(dfs, root_name, O_RDWR, &root, mode, stbuf);
	if(rc !=0){
		printf("ERROR USING DFS LOOKUP , errorno = %d \n", rc);
	}
	root_obj = daos_seis_open_root(dfs, root);
	return root_obj;
}

int daos_seis_get_trace_count(segy_root_obj_t *root){
	return root->number_of_traces;
}

int daos_seis_get_cmp_gathers(dfs_t *dfs, segy_root_obj_t *root){
	int rc;
	int daos_mode;
	int flags = O_RDWR;
	dfs_obj_t cmp_object;
	daos_handle_t	th = DAOS_TX_NONE;
	struct seismic_entry cmp_gather = {0};
	seis_obj_t *cmp_obj = 	malloc(sizeof(seis_obj_t));
	daos_mode = get_daos_obj_mode(flags);
	int number_of_gathers;

	rc = daos_obj_open(dfs->coh, root->cmp_oid, daos_mode, &cmp_object.oh, NULL);
	if (rc) {
			D_ERROR("daos_obj_open() Failed (%d)\n", rc);
			return daos_der2errno(rc);
	}

	prepare_seismic_entry(&cmp_gather, cmp_object.oid, DS_D_NGATHERS, DS_A_NGATHERS,
				(char*)&number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(cmp_object.oh, th, &cmp_gather);
	if(rc !=0){
		printf("Fetch number of gathers failed error = %d \n", rc);
	}
	cmp_obj->number_of_gathers = number_of_gathers;
	return cmp_obj->number_of_gathers;
}

int daos_seis_get_shot_gathers(dfs_t *dfs, segy_root_obj_t *root){
	int rc;
	int daos_mode;
	int flags = O_RDWR;
	dfs_obj_t shot_object;
	daos_handle_t	th = DAOS_TX_NONE;
	struct seismic_entry shot_gather = {0};
	seis_obj_t *shot_obj = 	malloc(sizeof(seis_obj_t));
	daos_mode = get_daos_obj_mode(flags);
	int number_of_gathers;

	rc = daos_obj_open(dfs->coh, root->shot_oid, daos_mode, &shot_object.oh, NULL);
	if (rc) {
			D_ERROR("daos_obj_open() Failed (%d)\n", rc);
			return daos_der2errno(rc);
	}
	prepare_seismic_entry(&shot_gather, shot_object.oid, DS_D_NGATHERS, DS_A_NGATHERS,
				(char*)&number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(shot_object.oh, th, &shot_gather);
	if(rc !=0){
			printf("Fetch number of gathers failed error = %d \n", rc);
		}
	shot_obj->number_of_gathers = number_of_gathers;
	return shot_obj->number_of_gathers;
}

int daos_seis_get_offset_gathers(dfs_t *dfs, segy_root_obj_t *root){
	int rc;
	int daos_mode;
	int flags = O_RDWR;
	dfs_obj_t offset_object;
	daos_handle_t	th = DAOS_TX_NONE;
	struct seismic_entry offset_gather = {0};
	seis_obj_t *off_obj = 	malloc(sizeof(seis_obj_t));
	daos_mode = get_daos_obj_mode(flags);
	int number_of_gathers;

	rc = daos_obj_open(dfs->coh, root->offset_oid, daos_mode, &offset_object.oh, NULL);
	if (rc) {
			D_ERROR("daos_obj_open() Failed (%d)\n", rc);
			return daos_der2errno(rc);
	}
	prepare_seismic_entry(&offset_gather, offset_object.oid, DS_D_NGATHERS, DS_A_NGATHERS,
				(char*)&number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(offset_object.oh, th, &offset_gather);
	if(rc !=0){
		printf("Fetch number of gathers failed error = %d \n", rc);
	}
	off_obj->number_of_gathers = number_of_gathers;
	return off_obj->number_of_gathers;
}

bhed* daos_seis_read_binary_header(segy_root_obj_t *root){
	struct seismic_entry entry ={0};
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    bhed *bhdr =malloc(sizeof(bhed));
	int rc;
	rc = prepare_seismic_entry(&entry, root->oid, DS_D_FILE_HEADER, DS_A_BINARY_HEADER
			,(char*)bhdr, BNYBYTES, DAOS_IOD_ARRAY);
	if(rc != 0){
		printf("ERROR PREPARING SEISMIC ENTRY IN READ TEXT HEADER error = %d \n", rc);
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if(rc!=0)
	{
		printf("ERROR IN FETCHING BINARY HEADER DATA %d \n" , rc);
	}

	return bhdr;
}

char* daos_seis_read_text_header(segy_root_obj_t *root){
	struct seismic_entry entry ={0};
	char *text_header = malloc(EBCBYTES*sizeof(char));
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    int rc;
	rc = prepare_seismic_entry(&entry, root->oid, DS_D_FILE_HEADER, DS_A_TEXT_HEADER
			, text_header, EBCBYTES, DAOS_IOD_ARRAY);
	if(rc != 0){
		printf("ERROR PREPARING SEISMIC ENTRY IN READ TEXT HEADER error = %d \n", rc);
	}
	rc = daos_seis_fetch_entry(root->oh, th, &entry);
	if(rc!=0)
	{
		printf("ERROR IN FETCHING TEXT HEADER DATA %d \n" , rc);
	}
	return text_header;
}

int daos_seis_read_shot_traces(dfs_t* dfs, int shot_id, segy_root_obj_t *root, char *name){
	int rc;
	int daos_mode;
	int flags = O_RDWR;
    char *tr_file = name;
    daos_oclass_id_t cid= OC_SX;
    FILE *fd = fopen(tr_file, "w");
	dfs_obj_t shot_object;
	daos_handle_t	th = DAOS_TX_NONE;
	struct seismic_entry shot_gather = {0};
	seis_obj_t *shot_obj = 	malloc(sizeof(seis_obj_t));
	int shot_exists = 0;
	daos_mode = get_daos_obj_mode(flags);
		//Open Shot Gather Object
	rc = daos_obj_open(dfs->coh, root->shot_oid, daos_mode, &shot_object.oh, NULL);
	if (rc) {
			printf("daos_obj_open()__shot_object Failed (%d)\n", rc);
			return daos_der2errno(rc);
	}
	//Fetch Number of Gathers Under Shot Gather object
	prepare_seismic_entry(&shot_gather, shot_object.oid, DS_D_NGATHERS, DS_A_NGATHERS,
				(char*)&shot_obj->number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(shot_object.oh, th, &shot_gather);
	shot_obj->oid = shot_object.oid;
	shot_obj->oh = shot_object.oh;
	int i;

	shot_obj->gathers=malloc(sizeof(seis_gather_t));

		char temp[200]="";
		char shot_dkey_name[200] = "";
		strcat(shot_dkey_name,DS_D_SHOT);
		sprintf(temp, "%d", shot_id);
		strcat(shot_dkey_name,temp);
		int temp_shot_id;

		//Fetch shot id
		prepare_seismic_entry(&shot_gather, shot_obj->oid, shot_dkey_name, DS_A_SHOT_ID,
					(char*)&temp_shot_id, sizeof(int), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(shot_obj->oh, th, &shot_gather);
		if(rc!=0){
			printf("FETCH using this dkey (%s) failed \n", shot_dkey_name);
			return rc;
		}

		//Check if it equals requested shot id
		//If yes read number of traces and the array of their object ids
		//else skip for the next shot gather


		//can insert this if condition in previous else and remove the if condition !!!
		if(temp_shot_id == shot_id){
			//Fetch number of traces
			prepare_seismic_entry(&shot_gather, shot_obj->oid, shot_dkey_name, DS_A_NTRACES,
								(char*)&(shot_obj->gathers->number_of_traces), sizeof(int), DAOS_IOD_SINGLE);
			rc = daos_seis_fetch_entry(shot_obj->oh, th, &shot_gather);

			shot_obj->gathers->oids = malloc(sizeof(daos_obj_id_t) * shot_obj->gathers->number_of_traces);
			//Fetch object id of traces object
			trace_obj_t *trace_oids_obj = malloc(sizeof(trace_obj_t));

			prepare_seismic_entry(&shot_gather, shot_obj->oid, shot_dkey_name, "TRACE_OIDS_OBJECT_ID",
								(char*)&(trace_oids_obj->oid), sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
			rc = daos_seis_fetch_entry(shot_obj->oh, th, &shot_gather);
			if(rc!=0){
				printf("Fetch TRACE OIDS object id failed (%d) \n", rc);
				return rc;
			}

			rc = daos_array_open_with_attr(dfs->coh, (trace_oids_obj)->oid, th,
				DAOS_OO_RW, 1,200*sizeof(daos_obj_id_t), &(trace_oids_obj->oh), NULL);
			if (rc != 0) {
				printf("daos_array_open_with_attr()-->>GATHER OIDS object<<-- failed (%d)\n", rc);
				return daos_der2errno(rc);
			}

			daos_array_iod_t iod;
			daos_range_t		rg;
			d_sg_list_t sgl;
			int offset = 0;

			sgl.sg_nr = shot_obj->gathers->number_of_traces;
			sgl.sg_nr_out = 0;
			d_iov_t iov[sgl.sg_nr];
			struct seismic_entry temp = {0};

			temp.data = shot_obj->gathers->oids;
			int j=0;
			for(int i=0; i < sgl.sg_nr; i++){
				d_iov_set(&iov[i], (void*)&(temp.data[j]), sizeof(daos_obj_id_t));
				j+=sizeof(daos_obj_id_t);
			}

			sgl.sg_iovs = &iov;
			iod.arr_nr = 1;
			rg.rg_len = shot_obj->gathers->number_of_traces * sizeof(daos_obj_id_t);
			rg.rg_idx = offset;
			iod.arr_rgs = &rg;

			printf("HELLOOOOOOO2 \n");

			rc = daos_array_read(trace_oids_obj->oh, th, &iod, &sgl, NULL);
			if(rc!=0){
				printf("ERROR READING GATHER OIDS----------------- error = %d  \n", rc);
				return rc;
			}
			printf("HELLOOOOOOO 3 \n");

			daos_array_close(trace_oids_obj->oh,NULL);

			//Fetch array of object ids
//			for(int m = 1; m <= shot_obj->gathers->number_of_traces; m++){
//				char trace_akey_name[200] = "";
//				char tr_temp[200] = "";
//				strcat(trace_akey_name, DS_A_TRACE);
//				sprintf(tr_temp, "%d", m);
//				strcat(trace_akey_name, tr_temp);
//
//			prepare_seismic_entry(&shot_gather, shot_obj->oid, shot_dkey_name, trace_akey_name,
//								(char*)&shot_obj->gathers->oids[m-1], sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
//			rc = daos_seis_fetch_entry(shot_obj->oh, th, &shot_gather);
//			if (rc) {
//							printf("fetch trace header object id Failed (%d)\n", rc);
//							return daos_der2errno(rc);
//					}
//			}
			shot_exists =1;
		} else {
			printf("SHOT ID %d doesn't exist \n",shot_id);
			return 0;
		}

	if(shot_exists==1){
		for(int j=0; j < shot_obj->gathers->number_of_traces; j++){
			struct seismic_entry trace_gather = {0};
			//Open trace object
			trace_obj_t *trace_obj= malloc(sizeof(trace_obj_t));
			trace_obj_t *trace_data_obj = malloc(sizeof(trace_obj_t));

			trace_obj->oid = shot_obj->gathers->oids[j];
//			printf("TRACE HEADER OID === %llu%llu \n", trace_obj->oid.lo, trace_obj->oid.hi);
			rc = daos_obj_open(dfs->coh, trace_obj->oid, daos_mode, &trace_obj->oh, NULL);
			if (rc) {
					printf("daos_obj_open()__ trace_header_obj Failed (%d)\n", rc);
					return daos_der2errno(rc);
			}

			trace_data_obj->oid = get_tr_data_oid(&(trace_obj->oid),cid);

//			printf("TRACE DATA OID === %llu%llu \n", trace_data_obj->oid.lo, trace_data_obj->oid.hi);
			/** Open the array object for the file */
			rc = daos_array_open_with_attr(dfs->coh, (trace_data_obj)->oid, th,
				DAOS_OO_RW, 1,200*sizeof(float), &(trace_data_obj->oh), NULL);
			if (rc != 0) {
				printf("daos_array_open_with_attr()-->>Trace data object<<-- failed (%d)\n", rc);
				return daos_der2errno(rc);
			}

			char tr_index[50];
			char trace_name[200] = "Trace_hdr_obj_";

			sprintf(tr_index, "%d",j);
			strcat(trace_name, tr_index);

			trace_obj->mode = daos_mode;
			trace_obj->flags = flags;
//			trace_obj->oid = shot_obj->gathers->oids[j];
			//why parent oid here ?!
			trace_obj->parent_oid = shot_obj->oid;
			strncpy(trace_obj->name, trace_name, SEIS_MAX_PATH);
			trace_obj->name[SEIS_MAX_PATH] = '\0';
			trace_obj->trace = malloc(sizeof(segy));

			//Read Trace header
			prepare_seismic_entry(&trace_gather, trace_obj->oid, DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
								(char*)trace_obj->trace, 240, DAOS_IOD_ARRAY);
			rc = daos_seis_fetch_entry(trace_obj->oh, th, &trace_gather);
			if (rc != 0) {
				printf("Error reading trace  %d header error = %d \n", j, rc);
				exit(0);
			}

			char trace_data_name[200] = "Trace_data_obj_";
			strcat(trace_data_name, tr_index);
			trace_data_obj->mode = S_IFREG | S_IWUSR | S_IRUSR;
			trace_data_obj->flags = flags;
//			trace_obj->oid = shot_obj->gathers->oids[j];
			//why parent oid here ?!
			trace_data_obj->parent_oid = shot_obj->oid;
			strncpy(trace_data_obj->name, trace_data_name, SEIS_MAX_PATH);
			trace_data_obj->name[SEIS_MAX_PATH] = '\0';
			// no need for this!!
			trace_data_obj->trace = malloc(sizeof(segy));

			//Read Trace data
			int data_length = (trace_obj->trace)->ns;
			int offset = 0;
			struct seismic_entry trace_data = {0};
			trace_data.data = (char*)(trace_obj->trace->data);

			daos_array_iod_t iod;
			daos_range_t		rg;
			d_sg_list_t sgl;

			sgl.sg_nr = data_length;
			sgl.sg_nr_out = 0;
			d_iov_t iov[sgl.sg_nr];

			int j=0;
			for(int i=0; i < sgl.sg_nr; i++){
				d_iov_set(&iov[i], (void*)&(trace_data.data[j]), sizeof(float));
				j+=4;
			}

			sgl.sg_iovs = &iov;
			iod.arr_nr = 1;
			rg.rg_len = data_length * sizeof(float);
			rg.rg_idx = offset;
			iod.arr_rgs = &rg;

			rc = daos_array_read(trace_data_obj->oh, th, &iod, &sgl, NULL);
			if(rc!=0){
				printf("ERROR READING TRACE DATA KEY----------------- error = %d  \n", rc);
			}

			/*
//			trace_data.data = malloc(sizeof(trace_obj->trace->data));
			while(data_length > 0){
				daos_array_iod_t iod;
				daos_range_t		rg;
				d_sg_list_t sgl;

				sgl.sg_nr = min(200,data_length);
				sgl.sg_nr_out = 0;
			    d_iov_t iov[sgl.sg_nr];

			    int j=0;
			    for(int i=0; i < sgl.sg_nr; i++){
					d_iov_set(&iov[i], (void*)&(trace_data.data[j+offset]), sizeof(float));
					j+=4;
				}

			    sgl.sg_iovs = &iov;
			    iod.arr_nr = 1;
				rg.rg_len = min(200,data_length) * sizeof(float);
				rg.rg_idx = offset;
				iod.arr_rgs = &rg;

				rc = daos_array_read(trace_data_obj->oh, th, &iod, &sgl, NULL);

//				rc = dfs_read(dfs, trace_data_obj, &sgl, offset, &read_size, NULL);
				if(rc!=0){
					printf("ERROR READING TRACE DATA KEY----------------- error = %d  \n", rc);
				}

//				prepare_seismic_entry(&trace_gather, trace_data_obj->oid, trace_dkey, DS_A_TRACE_DATA,
//									(char*)((trace_obj->trace->data)+offset), min(200,data_length)*sizeof(float));
//				rc = daos_seis_fetch_entry(trace_data_obj->oh, th, &trace_gather);
//				if(rc!=0){
//					printf("ERROR READING TRACE DATA KEY----------------- error = %d \n", rc);
//				}
				data_length = data_length - 200;
				offset = offset + 200;
			}
			*/
			daos_obj_close(trace_obj->oh,NULL);
			daos_array_close(trace_data_obj->oh,NULL);
			fputtr(fd, trace_obj->trace);
		}
	}
	return rc;
}

int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_root){

	/* create pointer to segyroot object */
	segy_root_obj_t *root_obj;
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    char *temp_name;
    int		rc;
	seis_obj_t *shot_obj;
	seis_obj_t *offset_obj;
	seis_obj_t *cmp_obj;
	DAOS_FILE *daos_tape = malloc(sizeof(DAOS_FILE));
	daos_tape->file = segy_root;
	daos_tape->offset = 0;

	rc = daos_seis_root_obj_create(dfs, &root_obj, cid, name, parent);
	if (rc != 0)
	{
		warn("FAILED TO create ROOT OBJECT");
		return rc;
	}

	rc = daos_seis_gather_obj_create(dfs,cid, root_obj, &shot_obj, &cmp_obj, &offset_obj);
	if (rc != 0)
	{
		warn("FAILED TO create gather OBJECT");
		return rc;
	}
	/* Globals */
	tapesegy tapetr;
	tapebhed tapebh;
	segy tr;
	bhed bh;


	  /* Declare variables that will be parsed from commandline */
	    /**********************************************************/
	    /* Required */
	    char *tape;		/* name of raw tape device	*/

	    int tapefd = 0;		/* file descriptor for tape	*/

	    FILE *tapefp = NULL;	/* file pointer for tape	*/
	    FILE *binaryfp;		/* file pointer for bfile	*/
	    FILE *headerfp;		/* file pointer for hfile	*/
	    FILE *xheaderfp;	/* file pointer for xfile	*/
	    FILE *pipefp;		/* file pointer for popen write */

	    size_t nsegy;		/* size of whole trace in bytes		*/
	    int i;			/* counter				*/
	    int itr;		/* current trace number			*/
	    int trmin;		/* first trace to read			*/
	    int trmax;		/* last trace to read			*/
	    int ns;			/* number of data samples		*/

	    int over;		/* flag for bhed.float override		*/
	    int format;		/* flag for to specify override format	*/
	    int result;
	    cwp_Bool format_set = cwp_false;
	    /* flag to see if new format is set	*/
	    int conv;		/* flag for data conversion		*/
	    int trcwt;		/* flag for trace weighting		*/
	    int verbose = 0;		/* echo every ...			*/
	    int vblock =50;		/* ... vblock traces with verbose=1	*/
	    int buff;		/* flag for buffered/unbuffered device	*/
	    int endian;		/* flag for big=1 or little=0 endian	*/
	    int swapdata;		/* flag for big=1 or little=0 endian	*/
	    int swapbhed;		/* flag for big=1 or little=0 endian	*/
	    int swaphdrs;		/* flag for big=1 or little=0 endian	*/
	    int errmax;		/* max consecutive tape io errors	*/
	    int errcount = 0;	/* counter for tape io errors		*/
	    cwp_Bool nsflag;	/* flag for error in tr.ns		*/

	    char *cmdbuf;	/* dd command buffer			*/
	    cmdbuf = (char*) malloc(BUFSIZ * sizeof(char));

	    char *ebcbuf;	/* ebcdic data buffer			*/
	    ebcbuf = (char*) malloc(EBCBYTES * sizeof(char));
	    int ebcdic=1;		/* ebcdic to ascii conversion flag	*/


	    cwp_String *key1;	/* output key(s)		*/
	    key1 = (cwp_String*) malloc(SU_NKEYS * sizeof(cwp_String));
	    cwp_String key2[SU_NKEYS];	/* first input key(s)		*/
	    cwp_String type1[SU_NKEYS];	/* array of types for key1	*/
	    char type2[SU_NKEYS];		/* array of types for key2	*/
	    int ubyte[SU_NKEYS];
	    int nkeys;			/* number of keys to be computed*/
	    int n;				/* counter of keys getparred	*/
	    int ikey;			/* loop counter of keys 	*/
	    int index1[SU_NKEYS];		/* array of indexes for key1 	*/
	    Value val1;			/* value of key1		*/

	    /* deal with number of extended text headers */
	    short nextended;


	    /* Set parameters */
	    trmin = 1;
	    trmax = INT_MAX;
		 union { short s; char c[2]; } testend;
		 testend.s = 1;
		 endian = (testend.c[0] == '\0') ? 1 : 0;

		swapdata=endian;
	    swapbhed=endian;
	    swaphdrs=endian;

		ebcdic=1;

	   	errmax = 0;
	   	buff = 1;

	     /* Override binary format value */
	   over = 0;
	//     if (getparint("format", &format))	format_set = cwp_true;
	     if (((over!=0) && (format_set))) {
	         bh.format = format;
	         if ( !((format == 1)
	                 || (format == 2)
	                 || (format == 3)
	                 || (format == 5)
	                 || (format == 8)) ) {
	             warn("Specified format=%d not supported", format);
	             warn("Assuming IBM floating point format, instead");
	         }
	     }

	     /* Override conversion of IBM floating point data? */
	 	conv = 1;

	     /* Get parameters */
	     /* get key1's */
	//     if ((n = countparval("remap")) != 0){
	//         nkeys = n;
	//         getparstringarray("remap", key1);
	//     } else { /* set default */
	//         nkeys = 0;
	//     }
	    nkeys = 0;

	//     /* get key2's */
	//     if ((n = countparval("byte")) != 0){
	//         if (n != nkeys)
	//             err("number of byte's and remap's must be equal!");
	//
	//         getparstringarray("byte", key2);
	//     }

	     for (ikey = 0; ikey < nkeys; ++ikey) {
	         /* get types and index values */
	         type1[ikey]  = hdtype(key1[ikey]);
	         index1[ikey] = getindex(key1[ikey]);
	     }

	     for (ikey = 0; ikey < nkeys; ++ikey) {
	    	 if (sscanf(key2[ikey],"%d%c", &ubyte[ikey], &type2[ikey]) != 2)
	    	 {
	             err("user format XXXt");
	    	 }
	     }

	     daos_size_t size;

	     int error;
/** NO need for this */
	     //	     /* Open files - first the tape */
//	         if ( tape[0] == '-' && tape[1] == '\0' ) daos_tape->file = NULL;
//	         else{
//	             daos_tape = open_dfs_file(tape, S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
//	         }

		 size = read_dfs_file(daos_tape, ebcbuf, EBCBYTES);


		    /* Open pipe to use dd to convert  ebcdic to ascii */
		    /* this command gives a file containing 3240 bytes on sun */
		    /* see top of Makefile.config for versions */
		    /* not sure why this breaks now; works in version 37 */
		    char *tbuf = malloc(EBCBYTES * sizeof(char));

		    if (ebcdic==1){
		        char *arr[] = {"dd", "ibs=1", "conv=ascii", "count=3200", NULL};
			    int read_bytes_from_command = execute_command(arr, ebcbuf, EBCBYTES, tbuf, EBCBYTES);
//			    printf("READ BYTES === %d \n", read_bytes_from_command);
		    } else {
		        char *arr[] = {"dd", "ibs=1", "count=3200", NULL};
			    int read_bytes_from_command = execute_command(arr, ebcbuf, EBCBYTES, tbuf, EBCBYTES);
//			    printf("READ BYTES === %d \n", read_bytes_from_command);
		    }

//		    pipefp = epopen(cmdbuf, "w");
//		    /* Write ebcdic stream from buffer into pipe */
//		    efwrite(ebcbuf, EBCBYTES, 1, pipefp);





		    rc =daos_seis_th_update(dfs, root_obj, DS_D_FILE_HEADER,
					 	 DS_A_TEXT_HEADER, tbuf, EBCBYTES);
			if (rc != 0)
			{
				warn("FAILED TO update text header in  ROOT OBJECT");
				return rc;
			}




	// save cmdbuf data under Text header key.
//		 check_error_code(daos_seis_root_update(get_dfs(), get_root(), DS_D_FILE_HEADER, DS_A_TEXT_HEADER, cmdbuf, EBCBYTES), "Updating Text File Header Key");

		size = read_dfs_file(daos_tape, (char *) &tapebh, BNYBYTES);


	    /* Convert from bytes to ints/shorts */
	    tapebhed_to_bhed(&tapebh, &bh);


	    /* if little endian machine, swap bytes in binary header */
	    if (swapbhed == 0) {
	    	for (i = 0; i < BHED_NKEYS; ++i) {
	    		swapbhval(&bh, i);
	    	}
	    }

	    /* Override binary format value */
	   	over = 0;
//	    if (getparint("format", &format))	format_set = cwp_true;
	    if (((over!=0) && (format_set)))
	    	{
	    	bh.format = format;
	    	}

	    /* Override application of trace weighting factor? */
	    /* Default no for floating point formats, yes for integer formats. */
	    trcwt = (bh.format == 1 || bh.format == 5) ? 0 : 1;


	    switch (bh.format) {
	        case 1:
	            if (verbose) {
	            	warn("assuming IBM floating point input");
	            }
	            break;
	        case 2:
	            if (verbose)
	            	{
	            	warn("assuming 4 byte integer input");
	            	}
	            break;
	        case 3:
	            if (verbose) {
	            	warn("assuming 2 byte integer input");
	            }
	            break;
	        case 5:
	            if (verbose)
	            	{
	            	warn("assuming IEEE floating point input");
	            	}
	            break;
	        case 8:
	            if (verbose)
	            	{
	            	warn("assuming 1 byte integer input");
	            	}
	            break;
	        default:
	        	if(over){
	        		warn("ignoring bh.format ... continue");
	        	}else {
	                err("format not SEGY standard (1, 2, 3, 5, or 8)");
	        	}

	    }

	    /* Compute length of trace (can't use sizeof here!) */
	    ns = bh.hns; /* let user override */
	    if (!ns)
	    	{
	    	err("samples/trace not set in binary header");
	    	}
	    bh.hns = ns;
//	    printf("ns = %d", ns);

	    switch (bh.format) {
	        case 8:
	            nsegy = ns + SEGY_HDRBYTES;
	            break;
	        case 3:
	            nsegy = ns * 2 + SEGY_HDRBYTES;
	            break;
	        case 1:
	        case 2:
	        case 5:
	        default:
	            nsegy = ns * 4 + SEGY_HDRBYTES;
	    }

	    //write data of binary header under binary header akey
	    rc = daos_seis_bh_update(dfs, root_obj, DS_D_FILE_HEADER,
	    			DS_A_BINARY_HEADER, &bh, BNYBYTES);
		if (rc != 0)
		{
			warn("FAILED TO update binary header in  ROOT OBJECT");
			return rc;
		}


	        nextended = *((short *) (((unsigned char *)&tapebh) + 304));

	    if (endian == 0)
	    	{
	    	swap_short_2((short *) &nextended);
	    	}
	    if (verbose)
	    	{
	    	warn("Number of extended text headers: %d", nextended);
	    	}

	    rc = daos_seis_root_update(dfs, root_obj,  DS_D_FILE_HEADER,	DS_A_NEXTENDED_HEADER,
	    			(char*)&nextended, sizeof(int), DAOS_IOD_SINGLE);
		if (rc != 0)
		{
			warn("FAILED TO update ROOT OBJECT");
			return rc;
		}

	    if (nextended > 0) /* number of extended text headers > 0 */
	    {
	        /* need to deal with -1 nextended headers */
	        /* so test should actually be !=0, but ... */
	        for (i = 0; i < nextended; i++) {
	            /* cheat -- an extended text header is same size as
	             * EBCDIC header */
	            /* Read the bytes from the tape for one xhdr into the
	             * buffer */
				size = read_dfs_file(daos_tape, ebcbuf, EBCBYTES);
	// write the data in ebcbuf under extended text header key.
				rc = daos_seis_exth_update(dfs, root_obj, DS_D_FILE_HEADER,
					 DS_A_EXTENDED_HEADER, ebcbuf, i, EBCBYTES);
				if (rc != 0)
				{
					warn("FAILED TO update extended header in  ROOT OBJECT");
					return rc;
				}

	        }
	    }


	    /* Read the traces */
	    nsflag = cwp_false;
	    itr = 0;

	    while (itr < trmax) {
	        int nread;

	        size = read_dfs_file(daos_tape, (char *) &tapetr, nsegy);
	        nread = size;

	        if (!nread)   /* middle exit loop instead of mile-long while */
	            break;

	        /* Convert from bytes to ints/shorts */
	        tapesegy_to_segy(&tapetr, &tr);

	        /* If little endian machine, then swap bytes in trace header */
	        if (swaphdrs == 0)
	            for (i = 0; i < SEGY_NKEYS; ++i) swaphval(&tr, i);

	        /* Check tr.ns field */
	        if (!nsflag && ns != tr.ns) {
	        	int temp_itr = itr +1;
	            warn("discrepant tr.ns = %d with tape/user ns = %d\n\t... first noted on trace %d",
	                 tr.ns, ns, temp_itr);
	            nsflag = cwp_true;
	        }

	        /* loop over key fields and remap */
	        for (ikey = 0; ikey < nkeys; ++ikey) {

	            /* get header values */

	            ugethval(type1[ikey], &val1,
	                     type2[ikey], ubyte[ikey] - 1,
	                     (char*) &tapetr, endian, conv, verbose);
	            puthval(&tr, index1[ikey], &val1);
	        }
	        /* Are there different swapping instructions for the data */
	        /* Convert and write desired traces */
	        if (++itr >= trmin) {
	            /* Convert IBM floats to native floats */
	            if (conv) {
	                switch (bh.format) {
	                    case 1:
	                        /* Convert IBM floats to native floats */
	                        ibm_to_float((int *) tr.data,
	                                     (int *) tr.data, ns,
	                                     swapdata,verbose);
	                        break;
	                    case 2:
	                        /* Convert 4 byte integers to native floats */
	                        int_to_float((int *) tr.data,
	                                     (float *) tr.data, ns,
	                                     swapdata);
	                        break;
	                    case 3:
	                        /* Convert 2 byte integers to native floats */
	                        short_to_float((short *) tr.data,
	                                       (float *) tr.data, ns,
	                                       swapdata);
	                        break;
	                    case 5:
	                        /* IEEE floats.  Byte swap if necessary. */
	                        if (swapdata == 0)
	                            for (i = 0; i < ns ; ++i)
	                                swap_float_4(&tr.data[i]);
	                        break;
	                    case 8:
	                        /* Convert 1 byte integers to native floats */
	                        integer1_to_float((signed char *)tr.data,
	                                          (float *) tr.data, ns);
	                        break;
	                }

	                /* Apply trace weighting. */
	                if (trcwt && tr.trwf != 0) {
	                    float scale = pow(2.0, -tr.trwf);
	                    //int i;
	                    for (i = 0; i < ns; ++i) {
	                        tr.data[i] *= scale;
	                    }
	                }
	            } else if (conv == 0) {
	                /* don't convert, if not appropriate */

	                switch (bh.format) {
	                    case 1: /* swapdata=0 byte swapping */
	                    case 5:
	                        if (swapdata == 0)
	                            for (i = 0; i < ns ; ++i)
	                                swap_float_4(&tr.data[i]);
	                        break;
	                    case 2: /* convert longs to floats */
	                        /* SU has no provision for reading */
	                        /* data as longs */
	                        int_to_float((int *) tr.data,
	                                     (float *) tr.data, ns, endian);
	                        break;
	                    case 3: /* shorts are the SHORTPAC format */
	                        /* used by supack2 and suunpack2 */
	                        if (swapdata == 0)/* swapdata=0 byte swap */
	                            for (i = 0; i < ns ; ++i)
	                                swap_short_2((short *) &tr.data[i]);
	                        /* Set trace ID to SHORTPACK format */
	                        tr.trid = SHORTPACK;
	                        break;
	                    case 8: /* convert bytes to floats */
	                        /* SU has no provision for reading */
	                        /* data as bytes */
	                        integer1_to_float((signed char *)tr.data,
	                                          (float *) tr.data, ns);
	                        break;
	                }
	            }

	            /* Write the trace to disk */
	            tr.ns = ns;
//	            puttr(&tr);
	            trace_obj_t *trace_obj;
	            root_obj->number_of_traces++;

	            rc = daos_seis_tr_obj_create(dfs, &trace_obj, itr, &tr, 240);
	            if(rc !=0)
	            	{
						printf("ERROR creating and updating trace object, error number = %d  \n", rc);
						return rc;
	            	}
	        	rc = daos_seis_tr_linking(dfs, trace_obj, &tr, shot_obj, cmp_obj, offset_obj);
	            if(rc!=0)
	            	{
	            		printf("ERROR LINKING TRACE TO MULTIPLE GATHER OBJECTS, err number= %d \n",rc);
	            		return rc;
	            	}
				/* Echo under verbose option */
			 if (verbose && (itr % vblock) == 0)
	            {
				 warn(" %d traces from tape", itr);
	            }

				daos_obj_close(trace_obj->oh, NULL);
				free(trace_obj);
	        }
	    }
	    rc = daos_seis_root_update(dfs, root_obj,  DS_D_FILE_HEADER, DS_A_NTRACES_HEADER,
	    			(char*)&(root_obj->number_of_traces), sizeof(int), DAOS_IOD_SINGLE);
		if (rc != 0)
		{
			warn("FAILED TO update ROOT OBJECT");
			return rc;
		}


		rc = update_gather_object(shot_obj, DS_D_NGATHERS, DS_A_NGATHERS,
				(char*)&shot_obj->number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
		if(rc !=0){
			printf("ERROR adding shot Number of gathers key, error: %d", rc);
			return rc;
		}

		rc = update_gather_object(cmp_obj, DS_D_NGATHERS, DS_A_NGATHERS,
						(char*)&cmp_obj->number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
		if(rc !=0){
			printf("ERROR adding CMP Number of gathers key, error: %d", rc);
			return rc;
		}

		rc = update_gather_object(offset_obj, DS_D_NGATHERS, DS_A_NGATHERS,
								(char*)&offset_obj->number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
		if(rc !=0){
			printf("ERROR adding OFFSET Number of gathers key, error: %d", rc);
			return rc;
		}

		trace_obj_t *shot_trace_oids_obj;
		trace_obj_t *cmp_trace_oids_obj;
		trace_obj_t *offset_trace_oids_obj;

		rc = daos_seis_trace_oids_obj_create(dfs,cid, &shot_trace_oids_obj, &cmp_trace_oids_obj, &offset_trace_oids_obj);
		if (rc != 0)
		{
			warn("FAILED TO create gather OBJECT");
			return rc;
		}


		rc = update_gather_traces(dfs, shot_obj->gathers, shot_obj, shot_trace_oids_obj, DS_D_SHOT, DS_A_NTRACES);
		if(rc!=0){
			printf("ERROR UPDATING shot number_of_traces key, error: %d \n", rc);
		}

		rc = update_gather_traces(dfs, cmp_obj->gathers, cmp_obj, cmp_trace_oids_obj, DS_D_CMP, DS_A_NTRACES);
		if(rc!=0){
			printf("ERROR UPDATING CMP number_of_traces key, error: %d \n", rc);
		}

		rc = update_gather_traces(dfs, offset_obj->gathers, offset_obj, offset_trace_oids_obj, DS_D_OFFSET, DS_A_NTRACES);
		if(rc!=0){
			printf("ERROR UPDATING OFFSET number_of_traces key, error: %d \n", rc);
		}


		daos_array_close(shot_trace_oids_obj->oh, NULL);
		daos_array_close(cmp_trace_oids_obj->oh, NULL);
		daos_array_close(offset_trace_oids_obj->oh, NULL);

		daos_obj_close(root_obj->oh, NULL);
		daos_obj_close(shot_obj->oh, NULL);
		daos_obj_close(cmp_obj->oh, NULL);
		daos_obj_close(offset_obj->oh, NULL);

	    free(daos_tape);
	return rc;
}

