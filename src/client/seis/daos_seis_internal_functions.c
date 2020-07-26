/*
 * daos_seis_internal_functions.c
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#include "daos_seis_internal_functions.h"


int daos_seis_fetch_entry(daos_handle_t oh, daos_handle_t th, struct seismic_entry *entry, daos_event_t *ev){

	d_sg_list_t	sgl;
	d_iov_t		sg_iovs;
	daos_iod_t	iod;
	daos_recx_t	recx;
	daos_key_t	dkey;
	int		rc;

	d_iov_set(&dkey, (void *)entry->dkey_name, strlen(entry->dkey_name));
	d_iov_set(&iod.iod_name, (void *)entry->akey_name, strlen(entry->akey_name));

	if(entry->iod_type == DAOS_IOD_SINGLE) {
		recx.rx_nr	= 1;
		iod.iod_size	= entry->size;
	} else if(entry->iod_type == DAOS_IOD_ARRAY) {
		recx.rx_nr	= entry->size;
		iod.iod_size	= 1;
	}

	iod.iod_nr	= 1;
	recx.rx_idx	= 0;
	iod.iod_recxs	= &recx;
	iod.iod_type	= entry->iod_type;

	d_iov_set(&sg_iovs, entry->data, entry->size);

	sgl.sg_nr	= 1;
	sgl.sg_nr_out	= 0;
	sgl.sg_iovs	= &sg_iovs;

	if(ev){
		rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL, ev);
		if (ev->ev_error) {
			printf("Failed to fetch entry with event %s (%d)\n", entry->dkey_name, ev->ev_error);
			return ev->ev_error;
		}
	} else {
		rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL, NULL);
	}
	if (rc) {
		printf("Failed to fetch entry %s (%d)\n", entry->dkey_name, rc);
		return rc;
	}
	return rc;
}
//
//int daos_seis_array_fetch_entry(daos_handle_t oh, daos_handle_t th,int nrecords, struct seismic_entry *entry){
//
//	d_sg_list_t	sgl;
//	d_iov_t		sg_iovs;
//	daos_iod_t	iod;
//	daos_recx_t	recx;
//	daos_key_t	dkey;
//	int		rc;
//
//	d_iov_set(&dkey, (void *)entry->dkey_name, strlen(entry->dkey_name));
//	d_iov_set(&iod.iod_name, (void *)entry->akey_name, strlen(entry->akey_name));
//
//	iod.iod_nr	= nrecords;
//	recx.rx_idx	= 0;
//	recx.rx_nr	= nrecords;
//	iod.iod_recxs	= &recx;
//	iod.iod_type	= entry->iod_type;
//	iod.iod_size	= sizeof(daos_obj_id_t);
//
//	d_iov_set(&sg_iovs, entry->data, entry->size);
//
//	sgl.sg_nr	= nrecords;
//	sgl.sg_nr_out	= 0;
//	sgl.sg_iovs	= &sg_iovs;
//
//	rc = daos_obj_fetch(oh, th, 0, &dkey, nrecords, &iod, &sgl, NULL, NULL);
//	if (rc)	{
//		printf("Failed to fetch arrayy entry %s (%d)\n", entry->dkey_name, rc);
//		return rc;
//	}
//		return rc;
//}
//
//int daos_seis_array_obj_update(daos_handle_t oh, daos_handle_t th, int nrecords, struct seismic_entry entry){
//
//	d_sg_list_t	sgl;
//	d_iov_t		sg_iovs;
//	daos_iod_t	iod;
//	daos_recx_t	recx;
//	daos_key_t	dkey;
//	int		rc;
//
//	d_iov_set(&dkey, (void *)entry.dkey_name, strlen(entry.dkey_name));
//	d_iov_set(&iod.iod_name, (void *)entry.akey_name, strlen(entry.akey_name));
//
//	iod.iod_type	= entry.iod_type;
//	iod.iod_size	= sizeof(daos_obj_id_t);
//
//	d_iov_set(&sg_iovs, (void*)entry.data, nrecords*sizeof(daos_obj_id_t));
//	recx.rx_idx = 0;
//	recx.rx_nr = entry.size;
//
//	iod.iod_nr	= nrecords;
//	iod.iod_recxs	= &recx;
//
//	sgl.sg_nr	= nrecords;
//	sgl.sg_nr_out	= 0;
//	sgl.sg_iovs	= &sg_iovs;
//
//	rc = daos_obj_update(oh, th, 0, &dkey, nrecords, &iod, &sgl, NULL);
//
//	if (rc) {
//		printf("Failed to insert in array dkey: %s and akey: %s (%d)\n", entry.dkey_name, entry.akey_name, rc);
//		return rc;
//	}
//	return rc;
//}

int daos_seis_th_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,char* akey_name ,
																				char *data, int nbytes){

	int		rc;
	struct seismic_entry	th_seismic_entry = {0};

	prepare_seismic_entry(& th_seismic_entry, root_obj->root_obj->oid, dkey_name, akey_name,
							data, nbytes, DAOS_IOD_ARRAY);

	rc = daos_seis_obj_update(root_obj->root_obj->oh, DAOS_TX_NONE, th_seismic_entry);
	if (rc) {
		return rc;
	}

	return rc;
}

int daos_seis_root_obj_create(dfs_t *dfs, seis_root_obj_t **obj,daos_oclass_id_t cid, char *name, dfs_obj_t *parent){

	int		rc;

	/*Allocate object pointer */
	D_ALLOC_PTR(*obj);
	if (*obj == NULL)
		return ENOMEM;

	D_ALLOC_PTR((*obj)->root_obj);
	if ((*obj)->root_obj == NULL)
		return ENOMEM;

	strncpy((*obj)->root_obj->name, name, DFS_MAX_PATH);
	(*obj)->root_obj->name[DFS_MAX_PATH] = '\0';
	(*obj)->root_obj->mode = S_IFDIR | S_IWUSR | S_IRUSR;
	(*obj)->root_obj->flags = O_RDWR;
	(*obj)->number_of_traces = 0;
	if(parent==NULL)
		parent = &dfs->root;

	/** Get new OID for root object */
	rc = oid_gen(dfs, cid,false, &((*obj)->root_obj->oid));
	if (rc) {
		printf("ERROR GENERATING OBJECT ID FOR SEISMIC ROOT OBJECT ERR= %d \n", rc);
		return rc;
	}

	int daos_mode = get_daos_obj_mode((*obj)->root_obj->flags);

	rc = daos_obj_open(dfs->coh, (*obj)->root_obj->oid, daos_mode, &((*obj)->root_obj->oh), NULL);
	if (rc) {
		printf("daos_obj_open() Failed (%d)\n", rc);
		return rc;
	}

	struct dfs_entry	dfs_entry = {0};
	dfs_entry.oid = (*obj)->root_obj->oid;
	dfs_entry.mode = (*obj)->root_obj->mode;
	dfs_entry.chunk_size = 0;
	dfs_entry.atime = dfs_entry.mtime = dfs_entry.ctime = time(NULL);

	/** insert Seismic root object created under parent */
	rc = insert_entry(parent->oh, DAOS_TX_NONE, (*obj)->root_obj->name, dfs_entry);
	if(rc) {
		printf("SEISMIC ROOT INSERTION FAILED err = %d \n", rc);
		return rc;
	}

	return rc;
}

int daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry){

	d_sg_list_t	sgl;
	d_iov_t		sg_iovs;
	daos_iod_t	iod;
	daos_recx_t	recx;
	daos_key_t	dkey;
	int		rc;

	d_iov_set(&dkey, (void *)entry.dkey_name, strlen(entry.dkey_name));
	d_iov_set(&iod.iod_name, (void *)entry.akey_name, strlen(entry.akey_name));

	if(entry.iod_type == DAOS_IOD_SINGLE) {
		recx.rx_nr	= 1;
		iod.iod_size	= entry.size;
	} else if(entry.iod_type == DAOS_IOD_ARRAY) {
		recx.rx_nr	= entry.size;
		iod.iod_size	= 1;
	}

	iod.iod_nr	= 1;
	recx.rx_idx	= 0;
	iod.iod_recxs	= &recx;
	iod.iod_type	= entry.iod_type;

	d_iov_set(&sg_iovs, entry.data, entry.size);

	sgl.sg_nr	= 1;
	sgl.sg_nr_out	= 0;
	sgl.sg_iovs	= &sg_iovs;

	rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
	if (rc) {
		printf("Failed to insert dkey: %s and akey: %s (%d)\n", entry.dkey_name, entry.akey_name, rc);
		return rc;
	}

	return rc;
}

int daos_seis_root_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
									char* akey_name , char* databuf, int nbytes, daos_iod_type_t iod_type){

	int		rc;
	struct seismic_entry	seismic_entry = {0};

	prepare_seismic_entry(& seismic_entry, root_obj->root_obj->oid, dkey_name, akey_name,
							databuf, nbytes, iod_type);

	rc = daos_seis_obj_update(root_obj->root_obj->oh, DAOS_TX_NONE, seismic_entry);
	if (rc)	{
		printf("ERROR UPDATING SEISMIC OBJECT  error = %d \n", rc);
		return rc;
	}

	return rc;
}

int daos_seis_bh_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
											char* akey_name , bhed *bhdr, int nbytes){

	int		rc;
	struct seismic_entry	bh_seismic_entry = {0};

	prepare_seismic_entry(&bh_seismic_entry, root_obj->root_obj->oid, dkey_name, akey_name,
							(char *)bhdr, nbytes, DAOS_IOD_ARRAY);

	rc = daos_seis_obj_update(root_obj->root_obj->oh, DAOS_TX_NONE, bh_seismic_entry);
	if (rc){
		printf("ERROR UPDATING SEISMIC OBJECT  error = %d \n", rc);
		return rc;
	}

	return rc;
}

int daos_seis_exth_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
										char* akey_name , char *ebcbuf, int index, int nbytes){
	int		rc;
	struct seismic_entry	exth_seismic_entry = {0};

	char akey_index[100];
	sprintf(akey_index, "%d",index);
	char akey_extended[200] = "";
	strcat(akey_extended, akey_name);
	strcat(akey_extended, akey_index);

	prepare_seismic_entry(& exth_seismic_entry, root_obj->root_obj->oid, dkey_name, akey_extended,
						ebcbuf, nbytes, DAOS_IOD_ARRAY);

	rc = daos_seis_obj_update(root_obj->root_obj->oh, DAOS_TX_NONE, exth_seismic_entry);
	if(rc != 0) {
		printf("ERROR UPDATING SEISMIC OBJECT error = %d \n", rc);
		return rc;
	}

	return rc;
}

void add_gather(seis_gather_t **head, seis_gather_t *new_gather) {

//	printf("ADDING NEW GATHER............................................. \n");
	if((*head) == NULL){
		(*head) = (seis_gather_t *) malloc(sizeof(seis_gather_t));
		(*head)->oids = (daos_obj_id_t *) malloc(50 * sizeof(daos_obj_id_t));
		(*head)->nkeys = new_gather->nkeys;
		(*head)->keys[0] = new_gather->keys[0];
		if((*head)->nkeys == 2 ) {
			(*head)->keys[1] = new_gather->keys[1];
		}
		(*head)->number_of_traces = new_gather->number_of_traces;
//		(*head)->oids = new_gather->oids;
		memcpy((*head)->oids, new_gather->oids, sizeof(daos_obj_id_t) * 50);
		(*head)->next_gather = NULL;
	} else {
		seis_gather_t * current = (*head);
		while (current->next_gather != NULL) {
			current = current->next_gather;
		}
		current->next_gather = (seis_gather_t *) malloc(sizeof(seis_gather_t));
		current->next_gather->oids = (daos_obj_id_t *) malloc(50 * sizeof(daos_obj_id_t));
		current->next_gather->nkeys = new_gather->nkeys;
		current->next_gather->keys[0] = new_gather->keys[0];
		if(current->next_gather->nkeys == 2 ) {
			current->next_gather->keys[1] = new_gather->keys[1];
		}
		current->next_gather->number_of_traces = new_gather->number_of_traces;
//		current->next_gather->oids = new_gather->oids;
		memcpy(current->next_gather->oids, new_gather->oids, sizeof(daos_obj_id_t) * 50);
		current->next_gather->next_gather = NULL;
	}
//	printf("FINISHED ADDING NEW GATHER \n");
}

int update_gather_traces(dfs_t *dfs, seis_gather_t *head, seis_obj_t *object, char *dkey_name, char *akey_name){

	if(head == NULL){
		printf("NO GATHERS EXIST \n");
		return 0;
	} else {
		int z = 0;
		while(head != NULL){
			int ntraces = head->number_of_traces;
			int rc;
			int nkeys = head->nkeys;
			char temp[200]="";
			char gather_dkey_name[200] = "";
			if(nkeys==1){
				strcat(gather_dkey_name,dkey_name);
				sprintf(temp, "%d", head->keys[0]);
				strcat(gather_dkey_name,temp);
			} else if(nkeys ==2){
				strcat(gather_dkey_name,dkey_name);
				sprintf(temp, "%d", head->keys[0]);
				strcat(gather_dkey_name,temp);
				strcat(gather_dkey_name,"_");
				sprintf(temp, "%d", head->keys[1]);
				strcat(gather_dkey_name,temp);
			}
			//insert array object_id in gather object...
			rc = daos_seis_gather_oids_array_update(dfs, &(object->seis_gather_trace_oids_obj[z]), head);
			if(rc != 0) {
				printf("ERROR UPDATING %s object TRACE OBJECT IDS ARRAY, error: %d \n",object->name, rc);
				return rc;
			}
			rc = update_gather_object(object, gather_dkey_name, DS_A_GATHER_TRACE_OIDS, (char*)&((object->seis_gather_trace_oids_obj[z]).oid),
								sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
			if(rc != 0) {
				printf("ERROR UPDATING %s object TRACE OBJECT IDS key, error: %d \n",object->name, rc);
				return rc;
			}
			rc = update_gather_object(object, gather_dkey_name, akey_name, (char*)&ntraces,
								sizeof(int), DAOS_IOD_SINGLE);
			if(rc != 0) {
				printf("ERROR UPDATING %s object number_of_traces key, error: %d \n",object->name, rc);
				return rc;
			}

			head = head->next_gather;
			z++;
		}
	}
	return 0;
}

int check_key_value(int *targets,seis_gather_t *head, daos_obj_id_t trace_obj_id, int *ntraces){

	int exists = 0;
	if(head == NULL) {
		printf("NO GATHERS EXIST \n");
		exists =0;
		return exists;
	} else {
		if(head->nkeys == 1) {
			while(head != NULL) {
				if(head->keys[0] == targets[0]) {
					head->oids[head->number_of_traces] = trace_obj_id;
					head->number_of_traces++;
					*ntraces = head->number_of_traces;
					exists = 1;
					if(head->number_of_traces % 50 == 0) {
						head->oids = (daos_obj_id_t *)realloc(head->oids, (head->number_of_traces + 50) * sizeof(daos_obj_id_t));
					}
					return exists;
				} else {
					head = head->next_gather;
				}
			}
		} else {
			while(head != NULL){
					if(head->keys[0] == targets[0] && head->keys[1] == targets[1]){
						head->oids[head->number_of_traces] = trace_obj_id;
						head->number_of_traces++;
						*ntraces = head->number_of_traces;
						exists = 1;
						if(head->number_of_traces % 50 == 0) {
							   head->oids = (daos_obj_id_t *)realloc(head->oids, (head->number_of_traces + 50) * sizeof(daos_obj_id_t));
						}
						return exists;
				} else {
					head = head->next_gather;
				}
			}
		}
	}
	return exists;
}

int daos_seis_trace_oids_obj_create(dfs_t* dfs,daos_oclass_id_t cid,seis_obj_t *seis_obj){

	int rc;

	seis_obj->seis_gather_trace_oids_obj = malloc(seis_obj->number_of_gathers * sizeof(trace_oid_oh_t));

	int i;
	for(i=0;i< seis_obj->number_of_gathers;i++){
		if ( &seis_obj->seis_gather_trace_oids_obj[i] == NULL) {
			return ENOMEM;
		}
		/** Get new OID for shot object */
		rc = oid_gen(dfs, cid,true, &(seis_obj->seis_gather_trace_oids_obj[i]).oid);
		if (rc) {
			printf("ERROR GENERATING OBJECT ID for gather trace OIDS %d \n", rc);
			return rc;
		}

		/** Open the array object for the gather oids */
		rc = daos_array_open_with_attr(dfs->coh, (seis_obj->seis_gather_trace_oids_obj[i]).oid, DAOS_TX_NONE,
											DAOS_OO_RW, 1,500*sizeof(daos_obj_id_t), &(seis_obj->seis_gather_trace_oids_obj[i]).oh, NULL);
		if (rc) {
			printf("daos_array_open_with_attr()-->>Trace data object<<-- failed (%d)\n", rc);
			return rc;
		}

	}
	return rc;
}

int daos_seis_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid, seis_root_obj_t *parent,
							seis_obj_t **shot_obj, seis_obj_t **cmp_obj, seis_obj_t **offset_obj){
	int		rc;
	int daos_mode;

	/*Allocate shot object pointer */
	D_ALLOC_PTR(*shot_obj);
	if (*shot_obj == NULL)
		return ENOMEM;
	strncpy((*shot_obj)->name, "shot_gather", SEIS_MAX_PATH);
	(*shot_obj)->name[SEIS_MAX_PATH] = '\0';
	(*shot_obj)->sequence_number = 0;
	(*shot_obj)->gathers = NULL;
	(*shot_obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*shot_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID err= %d \n",rc);
		return rc;
	}

	oid_cp(&parent->shot_oid, (*shot_obj)->oid);

	daos_mode = get_daos_obj_mode(O_RDWR);

	rc = daos_obj_open(dfs->coh, (*shot_obj)->oid, daos_mode, &(*shot_obj)->oh, NULL);
	if (rc) {
		printf("daos_obj_open() Failed (%d)\n", rc);
		return rc;
	}

	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_SHOT_GATHER,
							(char*)&(*shot_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	if(rc){
		printf("ERROR UPDATING SEISMIC ROOT ERR = %d", rc);
		return rc;
	}

	/*Allocate object pointer */
	D_ALLOC_PTR(*cmp_obj);
	if (*cmp_obj == NULL)
		return ENOMEM;
	strncpy((*cmp_obj)->name, "cmp_gather", SEIS_MAX_PATH);
	(*cmp_obj)->name[SEIS_MAX_PATH] = '\0';
	(*cmp_obj)->sequence_number = 0;
	(*cmp_obj)->gathers = NULL;
	(*cmp_obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*cmp_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID err= %d \n",rc);
		return rc;
	}
	oid_cp(&parent->cmp_oid, (*cmp_obj)->oid);

	daos_mode = get_daos_obj_mode(O_RDWR);

	rc = daos_obj_open(dfs->coh, (*cmp_obj)->oid, daos_mode, &(*cmp_obj)->oh, NULL);
	if (rc) {
		printf("daos_obj_open() Failed (%d)\n", rc);
		return rc;
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_CMP_GATHER ,
			(char*)&(*cmp_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	if(rc){
		printf("ERROR UPDATING SEISMIC ROOT ERR = %d", rc);
		return rc;
	}

	/*Allocate object pointer */
	D_ALLOC_PTR(*offset_obj);
	if (*offset_obj == NULL)
		return ENOMEM;
	strncpy((*offset_obj)->name, "offset_gather", SEIS_MAX_PATH);
	(*offset_obj)->name[SEIS_MAX_PATH] = '\0';
	(*offset_obj)->sequence_number = 0;
	(*offset_obj)->gathers = NULL;
	(*offset_obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*offset_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID err= %d \n",rc);
		return rc;
	}
	oid_cp(&parent->offset_oid, (*offset_obj)->oid);

	daos_mode = get_daos_obj_mode(O_RDWR);

	rc = daos_obj_open(dfs->coh, (*offset_obj)->oid, daos_mode, &(*offset_obj)->oh, NULL);
	if (rc) {
		printf("daos_obj_open() Failed (%d)\n", rc);
		return rc;
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_OFFSET_GATHER ,
									(char*)&(*offset_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	if(rc) {
		printf("ERROR UPDATING SEISMIC ROOT ERR = %d", rc);
		return rc;
	}

	return rc;
}

int daos_seis_trh_update(dfs_t* dfs, trace_obj_t* tr_obj, segy *tr, int hdrbytes){

	int		rc;
	struct seismic_entry	tr_entry = {0};

	prepare_seismic_entry(&tr_entry, tr_obj->oid, DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
				(char*)tr, hdrbytes, DAOS_IOD_ARRAY);

	rc = daos_seis_obj_update(tr_obj->oh, DAOS_TX_NONE, tr_entry);
	if(rc) {
		printf("ERROR UPDATING TRACE header KEY err = %d ----------------- \n", rc);
		return rc;
	}
	return rc;
}

int daos_seis_tr_data_update(dfs_t* dfs, trace_obj_t* trace_data_obj, segy *trace){

	int		rc;
	int offset = 0;
	struct seismic_entry	tr_entry = {0};
	daos_array_iod_t iod;
	daos_range_t		rg;
	d_sg_list_t sgl;

	tr_entry.data = (char*)(trace->data);

	sgl.sg_nr = 1; //trace->ns;
	sgl.sg_nr_out = 0;
	d_iov_t iov; //[sgl.sg_nr];
//	int j=0;
//	int i;
//	for(i=0; i < sgl.sg_nr; i++){
		d_iov_set(&iov, (void*)(tr_entry.data), trace->ns * sizeof(float));
//		j+=4;
//	}

	sgl.sg_iovs = &iov;
	iod.arr_nr = 1;
	rg.rg_len = trace->ns * sizeof(float);
	rg.rg_idx = offset;
	iod.arr_rgs = &rg;

	rc = daos_array_write(trace_data_obj->oh, DAOS_TX_NONE, &iod, &sgl, NULL);
	if (rc) {
		printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
		return rc;
	}


	/*
//	printf("Data length %d \n", data_length);
	while(data_length > 0){
//		char trace_dkey[200] = "";
//		sprintf(st_index, "%d",start);
//		sprintf(end_index, "%d",end);
//		strcat(trace_dkey, DS_D_TRACE_DATA);
//		strcat(trace_dkey, st_index);
//		strcat(trace_dkey, "_");
//		strcat(trace_dkey, end_index);
//		printf("DKEY === %s \n", trace_dkey);


		daos_array_iod_t iod;
		daos_range_t		rg;
		d_sg_list_t sgl;

//		sgl.sg_nr = 1;
//		sgl.sg_nr_out = 0;
//	    d_iov_t iov;
//		    d_iov_set(&iov, (void*)tr_entry.data, sizeof(float) * min(200,data_length));
//	    sgl.sg_iovs = &iov;


		sgl.sg_nr = min(200,data_length);
		sgl.sg_nr_out = 0;
		d_iov_t iov[sgl.sg_nr];
		int j=0;
		for(int i=0; i < sgl.sg_nr; i++){
			d_iov_set(&iov[i], (void*)(&tr_entry.data[j+offset]), sizeof(float));
			j+=4;
		}

		sgl.sg_iovs = &iov;
		iod.arr_nr = 1;
		rg.rg_len = min(200,data_length) * sizeof(float);
		rg.rg_idx = offset;
		iod.arr_rgs = &rg;

		rc = daos_array_write(trace_data_obj->oh, DAOS_TX_NONE, &iod, &sgl, NULL);
		if(rc!=0){
			printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
		}
//	    rc = dfs_write(dfs, trace_data_obj, &sgl, offset, NULL);
//	    if(rc!=0){
//			printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
//		}

//		tr_entry.oid = trace_data_obj->oid;
//		tr_entry.dkey_name = trace_dkey;
//		tr_entry.akey_name = DS_A_TRACE_DATA;
//		tr_entry.data = (char*)((trace->data)+offset);
//		tr_entry.size = min(200,data_length)*sizeof(float);
//
//		rc = daos_seis_obj_update(trace_data_obj->oh, th, tr_entry);
//		if(rc!=0){
//			printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
//		}

		data_length = data_length - 200;
		offset = offset + 200;
//		start = end+1;
//		end = start +199;
//		if(end > data_length && data_length <= 200)
//		{
//			end = start + data_length;
//		}

	}
*/

	return rc;

}

int daos_seis_gather_oids_array_update(dfs_t* dfs, trace_oid_oh_t* object, seis_gather_t *gather){

	int		rc;
	struct seismic_entry	tr_entry = {0};
	tr_entry.data = (char*)(gather->oids);
	daos_array_iod_t iod;
	daos_range_t		rg;
	d_sg_list_t sgl;

	sgl.sg_nr = 1; //gather->number_of_traces;
	sgl.sg_nr_out = 0;
	d_iov_t iov; //[sgl.sg_nr];
//	int j=0;
//	int i;
//	for(i=0; i < sgl.sg_nr; i++){
		d_iov_set(&iov, (void*)(tr_entry.data), gather->number_of_traces * sizeof(daos_obj_id_t));
//		j+=sizeof(daos_obj_id_t);
//	}

	sgl.sg_iovs = &iov;
	iod.arr_nr = 1;
	rg.rg_len = gather->number_of_traces * sizeof(daos_obj_id_t);
	rg.rg_idx = 0;
	iod.arr_rgs = &rg;

	rc = daos_array_write(object->oh, DAOS_TX_NONE, &iod, &sgl, NULL);
	if(rc){
		printf("ERROR UPDATING TRACE OBJECT IDS ARRAY----------------- error = %d \n", rc);
		return rc;
	}
	daos_array_close(object->oh, NULL);

	return rc;
}

daos_obj_id_t get_tr_data_oid(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid){

	daos_obj_id_t tr_data_oid = *tr_hdr;
	tr_data_oid.hi++;

	uint64_t ofeats;

	ofeats = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT |
		DAOS_OF_ARRAY_BYTE;

	uint64_t hdr;

	/* TODO: add check at here, it should return error if user specified
	 * bits reserved by DAOS
	 */
	tr_data_oid.hi &= (1ULL << OID_FMT_INTR_BITS) - 1;

	/**
	 * | Upper bits contain
	 * | OID_FMT_VER_BITS (version)		 |
	 * | OID_FMT_FEAT_BITS (object features) |
	 * | OID_FMT_CLASS_BITS (object class)	 |
	 * | 96-bit for upper layer ...		 |
	 */
	hdr  = ((uint64_t)OID_FMT_VER << OID_FMT_VER_SHIFT);
	hdr |= ((uint64_t)ofeats << OID_FMT_FEAT_SHIFT);
	hdr |= ((uint64_t)cid << OID_FMT_CLASS_SHIFT);
	tr_data_oid.hi |= hdr;

	return tr_data_oid;
}

int daos_seis_tr_obj_create(dfs_t* dfs, trace_obj_t **trace_hdr_obj, int index, segy *trace, int nbytes){
	/** Create Trace Object*/
	int		rc;
	int daos_mode;
	daos_oclass_id_t cid= OC_SX;

	char tr_index[50];
	char trace_hdr_name[200] = "Trace_hdr_obj_";
	char trace_data_name[200] = "Trace_data_obj_";
	trace_obj_t *trace_data_obj;

	/*Allocate object pointer */
	D_ALLOC_PTR(*trace_hdr_obj);
	if ((*trace_hdr_obj) == NULL)
		return ENOMEM;

	sprintf(tr_index, "%d",index);
	strcat(trace_hdr_name, tr_index);

	strncpy((*trace_hdr_obj)->name, trace_hdr_name, SEIS_MAX_PATH);
	(*trace_hdr_obj)->name[SEIS_MAX_PATH] = '\0';
	(*trace_hdr_obj)->trace = malloc(sizeof(segy));

	/** Get new OID for trace header object */
	rc = oid_gen(dfs, cid,false, &(*trace_hdr_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID for trace header %d \n", rc);
		return rc;
	}

	daos_mode = get_daos_obj_mode(O_RDWR);
	rc = daos_obj_open(dfs->coh, (*trace_hdr_obj)->oid, daos_mode, &(*trace_hdr_obj)->oh, NULL);
	if (rc) {
		printf("daos_obj_open() Failed (%d)\n", rc);
		return rc;
	}

	rc = daos_seis_trh_update(dfs, *trace_hdr_obj,trace, 240);
	if(rc) {
		printf("ERROR updating trace header object error number = %d  \n", rc);
		return rc;
	}


	D_ALLOC_PTR(trace_data_obj);
	if ((trace_data_obj) == NULL)
		return ENOMEM;

	strcat(trace_data_name, tr_index);

	strncpy((trace_data_obj)->name, trace_data_name, SEIS_MAX_PATH);
	(trace_data_obj)->name[SEIS_MAX_PATH] = '\0';
	(trace_data_obj)->trace = malloc(sizeof(segy));

	/** Get new OID for trace data object */
	rc = oid_gen(dfs, cid,true, &(trace_data_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID for trace data %d \n", rc);
		return rc;
	}

	daos_obj_id_t temp_oid;
	temp_oid = get_tr_data_oid(&(*trace_hdr_obj)->oid,cid);

	/** Open the array object for the file */
	rc = daos_array_open_with_attr(dfs->coh, (trace_data_obj)->oid, DAOS_TX_NONE,
							DAOS_OO_RW, 1,200*sizeof(float), &trace_data_obj->oh, NULL);
	if (rc) {
		printf("daos_array_open_with_attr()-->>Trace data object<<-- failed (%d)\n", rc);
		return rc;
	}

	rc = daos_seis_tr_data_update(dfs, trace_data_obj,trace);
	if(rc){
		printf("ERROR updating trace data object error number = %d  \n", rc);
		return rc;
	}

	rc = daos_array_close(trace_data_obj->oh, NULL);
	if(rc) {
		printf("ERROR Closing trace data object \n");
		return rc;
	}
	free((trace_data_obj)->trace);
	D_FREE_PTR(trace_data_obj);
	return rc;
}

void prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid, char *dkey, char *akey,
				char *data,int size, daos_iod_type_t iod_type){
	entry->oid = oid;
	entry->dkey_name = dkey;
	entry->akey_name = akey;
	entry->data = data;
	entry->size = size;
	entry->iod_type = iod_type;
}

int update_gather_object(seis_obj_t *gather_obj, char *dkey_name, char *akey_name,
											char *data, int nbytes, daos_iod_type_t type){
	struct seismic_entry gather_entry = {0};
	int rc;

	prepare_seismic_entry(&gather_entry, gather_obj->oid, dkey_name, akey_name,
						data, nbytes, type);
	rc = daos_seis_obj_update(gather_obj->oh, DAOS_TX_NONE, gather_entry);
	if(rc){
		printf("ERROR UPDATING (%s) gather object (%s) key, error: %d \n", gather_obj->name, dkey_name, rc);
		return rc;
	}
	return rc;
}

void prepare_keys(char *dkey_name, char *akey_name, char *dkey_prefix,
								char *akey_prefix, int nkeys, int *dkey_suffix, int *akey_suffix){

	char temp[200]="";
	strcat(dkey_name,dkey_prefix);
	if(dkey_suffix != NULL) {
		sprintf(temp, "%d", dkey_suffix[0]);
		strcat(dkey_name,temp);
		if(nkeys==2) {
			strcat(dkey_name,"_");
			sprintf(temp, "%d", dkey_suffix[1]);
			strcat(dkey_name,temp);
		}
	}
	strcat(akey_name, akey_prefix);
	if(akey_suffix != NULL) {
		sprintf(temp, "%d", *akey_suffix);
		strcat(akey_name,temp);
	}
}

int daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, segy *trace,
									seis_obj_t *shot_obj, seis_obj_t *cmp_obj, seis_obj_t *off_obj){

	int rc = 0;
	int shot_id = trace->fldr;
	int s_x = trace->sx;
	int s_y = trace->sy;
	int r_x = trace->gx;
	int r_y = trace->gy;
	int cmp_x = (s_x + r_x)/2;
	int cmp_y = (s_y + r_y)/2;
	int off_x = (r_x - s_x)/2;
	int off_y = (r_y - s_y)/2;
	int shot_exists=0;
	int cmp_exists=0;
	int offset_exists=0;
	int ntraces;
	int keys[2];
	struct seismic_entry gather_entry = {0};
	int no_of_traces;

	keys[0]=shot_id;
	if(check_key_value(keys,shot_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
		shot_exists=1;
	}

	keys[0]=cmp_x;
	keys[1] = cmp_y;
	if(check_key_value(keys,cmp_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
			cmp_exists=1;
	}

	keys[0]=off_x;
	keys[1] =off_y;
	if(check_key_value(keys,off_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
		offset_exists=1;
	}

	/** if shot id, cmp, and offset doesn't already exist */
	if(!shot_exists){
		seis_gather_t shot_gather_data = {0};
		shot_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
		char temp[200]="";
		shot_gather_data.oids[0] = trace_obj->oid;
		shot_gather_data.number_of_traces = 1;
		shot_gather_data.nkeys = 1;
		shot_gather_data.keys[0] = shot_id;
		char shot_dkey_name[200] = "";
		char trace_akey_name[200] = "";

		prepare_keys(shot_dkey_name, trace_akey_name, DS_D_SHOT, DS_A_TRACE, 1,
								shot_gather_data.keys, &shot_gather_data.number_of_traces);

		rc = update_gather_object(shot_obj, shot_dkey_name, DS_A_SHOT_ID, (char*)&shot_id,
							sizeof(int), DAOS_IOD_SINGLE);
		if(rc) {
			printf("ERROR adding shot shot_id key, error: %d", rc);
			return rc;
		}
		add_gather(&(shot_obj->gathers), &shot_gather_data);
		shot_obj->sequence_number++;
		shot_obj->number_of_gathers++;
		free(shot_gather_data.oids);
	}

	if(!cmp_exists){
		char cmp_dkey_name[200] = "";
		char temp[200]="";
		seis_gather_t cmp_gather_data= {0};
		cmp_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
		cmp_gather_data.oids[0] = trace_obj->oid;
		cmp_gather_data.number_of_traces=1;
		cmp_gather_data.nkeys=2;
		cmp_gather_data.keys[0] = cmp_x;
		cmp_gather_data.keys[1] = cmp_y;
		char trace_akey_name[200] = "";

		prepare_keys(cmp_dkey_name, trace_akey_name, DS_D_CMP, DS_A_TRACE, 2,
								cmp_gather_data.keys, &cmp_gather_data.number_of_traces);

		rc = update_gather_object(cmp_obj, cmp_dkey_name, DS_A_CMP_VAL, (char*)cmp_gather_data.keys,
							sizeof(int)*2, DAOS_IOD_ARRAY);
		if(rc){
			printf("ERROR adding cmp value key, error: %d", rc);
			return rc;
		}
		add_gather(&(cmp_obj->gathers), &cmp_gather_data);
		cmp_obj->sequence_number++;
		cmp_obj->number_of_gathers++;
		free(cmp_gather_data.oids);
	}

	if(!offset_exists){
		char off_dkey_name[200] = "";
		char temp[200]="";
		seis_gather_t off_gather_data= {0};
		off_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
		off_gather_data.oids[0] = trace_obj->oid;
		off_gather_data.number_of_traces = 1;
		off_gather_data.nkeys = 2;
		off_gather_data.keys[0] = off_x;
		off_gather_data.keys[1] = off_y;

		char trace_akey_name[200] = "";

		prepare_keys(off_dkey_name, trace_akey_name, DS_D_OFFSET, DS_A_TRACE, 2,
								off_gather_data.keys, &off_gather_data.number_of_traces);
		rc = update_gather_object(off_obj, off_dkey_name, DS_A_OFF_VAL, (char*)off_gather_data.keys,
							sizeof(int)*2, DAOS_IOD_ARRAY);
		if(rc){
			printf("ERROR adding gather value key, error: %d", rc);
			return rc;
		}
		add_gather(&(off_obj->gathers), &off_gather_data);
		off_obj->sequence_number++;
		off_obj->number_of_gathers++;
		free(off_gather_data.oids);
	}
	return rc;
}

int pcreate(int fds[2], const char *command, char *const argv[]) {
	/* Spawn a process running the command, returning it's pid. The fds array passed will
	 * be filled with two descriptors: fds[0] will read from the child process,
	 * and fds[1] will write to it.
	 * Similarly, the child process will receive a reading/writing fd set (in
	 * that same order) as arguments.
	*/
	int pid;
	int pipes[4];

	/* Warning: I'm not handling possible errors in pipe/fork */

	pipe(&pipes[0]); /* Parent read/child write pipe */
	pipe(&pipes[2]); /* Child read/parent write pipe */

	if ((pid = fork()) > 0) {
		/* Parent process */
		fds[0] = pipes[0];
		fds[1] = pipes[3];

		close(pipes[1]);
		close(pipes[2]);

		return pid;

	} else {
		close(pipes[0]);
		close(pipes[3]);
		dup2(pipes[2], STDIN_FILENO);
		dup2(pipes[1], STDOUT_FILENO);
		execvp(command, argv);
		exit(-1);
	}

	return -1; /* ? */
}

int execute_command(char *const argv[], char *write_buffer,
									int write_bytes, char *read_buffer, int read_bytes) {

	// Executes the command given in argv, which is a string array
	// For example if command is ls -l -a
	// argv should be {"ls, "-l", "-a", NULL}
	// descriptors to use for read/write.
	int fd[2];
	// Setup pipe, redirections and execute command.
	int pid = pcreate(fd, argv[0], argv);
	// Check for error.
	if (pid == -1) {
		return -1;
	}
	// If user wants to write to subprocess STDIN, we write here.
	if (write_bytes > 0) {
		write(fd[1], write_buffer, write_bytes);
	}
	// Read cycle : read as many bytes as possible or until we reach the maximum requested by user.
	char *buffer = read_buffer;
	ssize_t bytesread = 1;

	int total_bytes = 0;
//    total_bytes = read(fd[0], buffer, read_bytes);
	while ((bytesread = read(fd[0], buffer, read_bytes)) > 0) {
		buffer += bytesread;
		total_bytes += bytesread;
		if (bytesread >= read_bytes) {
			break;
		}
	}
	// Return number of bytes actually read from the STDOUT of the subprocess.
	return total_bytes;
}

segy* trace_to_segy(trace_t *trace){

	segy *tp = malloc(sizeof(segy));
	memcpy(tp, trace, HDRBYTES);
	memcpy(tp->data, trace->data, tp->ns*sizeof(float));
	return tp;
}

void fetch_traces_header(dfs_t *dfs, daos_obj_id_t *oids, read_traces *traces, int daos_mode){

	trace_oid_oh_t *trace_hdr_obj = malloc( traces->number_of_traces * sizeof(trace_oid_oh_t));

	int i;
	int rc;
	struct seismic_entry seismic_entry = {0};
	for (i=0; i < traces->number_of_traces; i++){
		trace_hdr_obj[i].oid = oids[i];
		rc = daos_obj_open(dfs->coh, trace_hdr_obj[i].oid, daos_mode, &trace_hdr_obj[i].oh, NULL);
		if(rc) {
			printf("daos_obj_open()__ trace_header_obj Failed (%d)\n", rc);
			exit(rc);
		}
		//Read Trace header
		prepare_seismic_entry(&seismic_entry, trace_hdr_obj[i].oid, DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
							(char*)&(traces->traces[i]), HDRBYTES, DAOS_IOD_ARRAY);

		rc = daos_seis_fetch_entry(trace_hdr_obj[i].oh, DAOS_TX_NONE, &seismic_entry, NULL);
		if (rc) {
			printf("Error reading trace  %d header error = %d \n", i, rc);
			exit(rc);
		}
		daos_obj_close(trace_hdr_obj[i].oh,NULL);

		traces->traces[i].trace_header_obj = oids[i];
	}
	free(trace_hdr_obj);
}

void fetch_traces_data(dfs_t *dfs, daos_obj_id_t *oids, read_traces *traces, int daos_mode){

	trace_oid_oh_t *trace_data_obj = malloc( traces->number_of_traces * sizeof(trace_oid_oh_t));
	int i;
	int rc;
	daos_array_iod_t iod;
	daos_range_t		rg;
	d_sg_list_t sgl;
	struct seismic_entry seismic_entry = {0};
	for(i=0 ; i< traces->number_of_traces; i++){
		trace_data_obj[i].oid = get_tr_data_oid(&(traces->traces[i].trace_header_obj),OC_SX);

		rc = daos_array_open_with_attr(dfs->coh, (trace_data_obj[i]).oid, DAOS_TX_NONE, DAOS_OO_RW,
								1,200*sizeof(float), &(trace_data_obj[i].oh), NULL);
		if (rc) {
			printf("daos_array_open_with_attr()-->>Trace data object<<-- failed (%d)\n", rc);
			exit(rc);
		}
		traces->traces[i].data = malloc(traces->traces[i].ns * sizeof(float));
		sgl.sg_nr = 1; // traces->traces[j].ns;
		sgl.sg_nr_out = 0;
		d_iov_t iov;

		seismic_entry.data = (char*)traces->traces[i].data;

		d_iov_set(&iov, (void*)(seismic_entry.data), traces->traces[i].ns * sizeof(float));

		sgl.sg_iovs = &iov;
		iod.arr_nr = 1;
		rg.rg_len = traces->traces[i].ns * sizeof(float);
		rg.rg_idx = 0;
		iod.arr_rgs = &rg;

		rc = daos_array_read(trace_data_obj[i].oh, DAOS_TX_NONE, &iod, &sgl, NULL);
		if(rc) {
			printf("ERROR READING TRACE DATA KEY----------------- error = %d  \n", rc);
			exit(rc);
		}
		daos_array_close(trace_data_obj[i].oh,NULL);
	}
	free(trace_data_obj);

}

void sort_dkeys_list(int *first_array, int number_of_gathers, char** unique_keys, int direction){

    const char *sep = "_";
    char *token;
    int i;



    char new_temp[4096];
    int *positive = malloc(number_of_gathers * sizeof(int));
    char **sorted_keys = malloc(number_of_gathers * sizeof(char *));
    int j=0;
    for(i=0;i< number_of_gathers;i++){
        strcpy(new_temp, unique_keys[j]);
        token = strtok(new_temp, sep);
      	sorted_keys[j] = malloc((strlen(token) + 1) * sizeof(char));
        while( token != NULL ) {
        	token = strtok(NULL, sep);
        	if(token == NULL)
        		break;
        	if(token[0]== '-'){
				positive[j]= 0;
	            strcpy(sorted_keys[j], &token[1]);
        	}else{
				positive[j]=1;
	            strcpy(sorted_keys[j], token);
			}
        	first_array[j] = atoi(sorted_keys[j]);
        }
        j++;
    }

    int temp2;
    if(direction==1){
        for (i = 0; i < number_of_gathers; i++)
            {
                for (j = 0; j < number_of_gathers - i - 1; j++)
                {
                    if (first_array[j] > first_array[j + 1])
                    {
                        temp2 = first_array[j];
                        first_array[j] = first_array[j + 1];
                        first_array[j + 1] = temp2;
                    }
                }
            }
    } else {
        for (i = 0; i < number_of_gathers ; ++i)
        {
            for (j = i + 1; j < number_of_gathers; ++j)
            {
                if (first_array[i] < first_array[j])
                {
                	temp2 = first_array[i];
                    first_array[i] = first_array[j];
                    first_array[j] = temp2;
                }
            }
        }

    }


 }

void sort_headers(read_traces *gather_traces, char *sort_key, int direction){
	int i;
	int j;
	trace_t temp;
	if(strcmp(sort_key,"cdp")==0){
		for(i=0; i< gather_traces->number_of_traces;i++){
			gather_traces->traces[i].cdp =(gather_traces->traces[i].sx + gather_traces->traces[i].gx)/2;
		}
	} else if(strcmp(sort_key,"offset")==0){
		for(i=0; i< gather_traces->number_of_traces;i++){
			gather_traces->traces[i].offset =(gather_traces->traces[i].sx - gather_traces->traces[i].gx)/2;
		}
	}

	MergeSort(gather_traces->traces, 0, gather_traces->number_of_traces-1, sort_key, direction);

//	for (i = 1; i < gather_traces->number_of_traces; i++){
//		for (j = 0; j < gather_traces->number_of_traces - i; j++) {
//			if (gather_traces->traces[j].cdp > gather_traces->traces[j+1]	.cdp) {
//				temp = gather_traces->traces[j];
//				gather_traces->traces[j] = gather_traces->traces[j+1];
//				gather_traces->traces[j+1] = temp;
//			 }
//		}
//	}
//	printf("AfTER SORTING BASED ON CDP ============================================= \n");
//	for(int i=0; i<gather_traces->number_of_traces; i++){
//		printf("CDP OF TRACE %d issssss %d \n", i, gather_traces->traces[i].cdp);
//	}
}

void Merge(trace_t *arr, int low, int mid, int high, char *sort_key, int direction)
{
    int mergedSize = high - low + 1;
    trace_t *temp = (trace_t *)malloc(mergedSize * sizeof(trace_t));
    int mergePos = 0;
    int leftPos = low;
    int rightPos = mid + 1;

    while (leftPos <= mid && rightPos <= high)
    {
        if (check_sorting_key(arr[leftPos], arr[rightPos], sort_key) && direction==1)
        {
            temp[mergePos++] = arr[leftPos++];
        }
        else if (check_sorting_key(arr[leftPos], arr[rightPos], sort_key) && direction==0)
        {
            temp[mergePos++] = arr[rightPos++];
        }
    }

    while (leftPos <= mid)
    {
        temp[mergePos++] = arr[leftPos++];
    }

    while (rightPos <= high)
    {
        temp[mergePos++] = arr[rightPos++];
    }

    assert(mergePos == mergedSize);

    for (mergePos = 0; mergePos < mergedSize; ++mergePos)
        arr[low + mergePos] = temp[mergePos];

    free(temp);
}

void MergeSort(trace_t *arr, int low, int high, char *sort_key, int direction)
{
    if (low < high)
    {
        int mid = (low + high) / 2;
        //printf("-->> %s: lo = %d, md = %d, hi = %d\n", __func__, low, mid, high);

        MergeSort(arr, low, mid, sort_key, direction);
        MergeSort(arr, mid + 1, high, sort_key, direction);

        Merge(arr, low, mid, high, sort_key, direction);
        //printf("<<-- %s: lo = %d, md = %d, hi = %d\n", __func__, low, mid, high);
    }
}

int check_sorting_key(trace_t leftPos, trace_t rightPos, char *sort_key){

	if(strcmp(sort_key, "tracl") == 0){
		if(leftPos.tracl < rightPos.tracl){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"tracr")==0){
		if(leftPos.tracr < rightPos.tracr){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"fldr")==0){
		if(leftPos.fldr < rightPos.fldr){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"tracf")==0){
		if(leftPos.tracf < rightPos.tracf){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"ep")==0){
		if(leftPos.ep < rightPos.ep){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"cdp")==0){
		if(leftPos.cdp < rightPos.cdp){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"cdpt")==0){
		if(leftPos.cdpt < rightPos.cdpt){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"nvs")==0){
		if(leftPos.nvs < rightPos.nvs){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"nhs")==0){
		if(leftPos.nhs < rightPos.nhs){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"offset")==0){
		if(leftPos.offset < rightPos.offset){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"gelev")==0){
		if(leftPos.gelev < rightPos.gelev){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"selev")==0){
		if(leftPos.selev < rightPos.selev){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"sdepth")==0){
		if(leftPos.sdepth < rightPos.sdepth){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"gdel")==0){
		if(leftPos.gdel < rightPos.gdel){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"sdel")==0){
		if(leftPos.sdel < rightPos.sdel){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"swdep")==0){
		if(leftPos.swdep < rightPos.swdep){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(sort_key,"gwdep")==0){
		if(leftPos.gwdep < rightPos.gwdep){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"scalel")==0){
		if(leftPos.scalel < rightPos.scalel){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"scalco")==0){
		if(leftPos.scalco < rightPos.scalco){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sx")==0){
		if(leftPos.sx < rightPos.sx){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sy")==0){
		if(leftPos.sy < rightPos.sy){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"gx")==0){
		if(leftPos.gx < rightPos.gx){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"gy")==0){
		if(leftPos.gy < rightPos.gy){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"wevel")==0){
		if(leftPos.wevel < rightPos.wevel){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"swevel")==0){
		if(leftPos.swevel < rightPos.swevel){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sut")==0){
		if(leftPos.sut < rightPos.sut){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"gut")==0){
		if(leftPos.gut < rightPos.gut){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sstat")==0){
		if(leftPos.sstat < rightPos.sstat){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"gstat")==0){
		if(leftPos.gstat < rightPos.gstat){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"tstat")==0){
		if(leftPos.tstat < rightPos.tstat){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"laga")==0){
		if(leftPos.laga < rightPos.laga){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"lagb")==0){
		if(leftPos.lagb < rightPos.lagb){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"delrt")==0){
		if(leftPos.delrt < rightPos.delrt){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"muts")==0){
		if(leftPos.muts < rightPos.muts){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"mute")==0){
		if(leftPos.mute < rightPos.mute){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"ns")==0){
		if(leftPos.ns < rightPos.ns){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"dt")==0){
		if(leftPos.dt < rightPos.dt){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"gain")==0){
		if(leftPos.gain < rightPos.gain){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"igc")==0){
		if(leftPos.igc < rightPos.igc){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"igi")==0){
		if(leftPos.igi < rightPos.igi){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"corr")==0){
		if(leftPos.corr < rightPos.corr){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sfs")==0){
		if(leftPos.sfs < rightPos.sfs){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sfe")==0){
		if(leftPos.sfe < rightPos.sfe){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"slen")==0){
		if(leftPos.slen < rightPos.slen){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"styp")==0){
		if(leftPos.styp < rightPos.styp){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"stas")==0){
		if(leftPos.stas < rightPos.stas){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"stae")==0){
		if(leftPos.stae < rightPos.stae){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"tatyp")==0){
		if(leftPos.tatyp < rightPos.tatyp){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"afilf")==0){
		if(leftPos.afilf < rightPos.afilf){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"afils")==0){
		if(leftPos.afils < rightPos.afils){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"nofilf")==0){
		if(leftPos.nofilf < rightPos.nofilf){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"nofils")==0){
		if(leftPos.nofils < rightPos.nofils){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"lcf")==0){
		if(leftPos.lcf < rightPos.lcf){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"hcf")==0){
		if(leftPos.hcf < rightPos.hcf){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"lcs")==0){
		if(leftPos.lcs < rightPos.lcs){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"hcs")==0){
		if(leftPos.hcs < rightPos.hcs){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"grnors")==0){
		if(leftPos.grnors < rightPos.grnors){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"grnofr")==0){
		if(leftPos.grnofr < rightPos.grnofr){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"grnlof")==0){
		if(leftPos.grnlof < rightPos.grnlof){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"gaps")==0){
		if(leftPos.gaps < rightPos.gaps){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"d1")==0){
		if(leftPos.d1 < rightPos.d1){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"f1")==0){
		if(leftPos.f1 < rightPos.f1){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"d2")==0){
		if(leftPos.d2 < rightPos.d2){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"f2")==0){
		if(leftPos.f2 < rightPos.f2){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"sfs")==0){
		if(leftPos.sfs < rightPos.sfs){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(sort_key,"ntr")==0){
		if(leftPos.ntr < rightPos.ntr){
			return 1;
		} else {
			return 0;
		}
	} else {
		return -1;
	}
}

void window_headers(read_traces *window_traces, read_traces *gather_traces, daos_obj_id_t *oids, char *key, long min, long max){

	int i;
	int k;
	int temp = gather_traces->number_of_traces;
	window_traces->number_of_traces = 0;
	daos_obj_id_t *temp_oids = malloc(gather_traces->number_of_traces * sizeof(daos_obj_id_t));
	memcpy(temp_oids, oids,gather_traces->number_of_traces * sizeof(daos_obj_id_t));

	memset(oids,0,gather_traces->number_of_traces * sizeof(daos_obj_id_t));
	printf("number of traces before = %d \n", window_traces->number_of_traces);

	for(i=0; i < temp ;i++){
//		printf("i = %d \n", i);
		if(check_windowing_key(gather_traces->traces[i], key, min, max) == 0){
//			for(k=i; k < gather_traces->number_of_traces ; k++){
//				memcpy(&oids[k],&oids[k+1], sizeof(daos_obj_id_t));
//				memcpy(&gather_traces->traces[k], &gather_traces->traces[k+1], sizeof(trace_t));
//			}
//			gather_traces->number_of_traces--;
		} else {
			memcpy(&window_traces->traces[i], &gather_traces->traces[i], sizeof(trace_t));
			memcpy(&oids[i], &temp_oids[i],sizeof(daos_obj_id_t));
			window_traces->number_of_traces ++;
		}
//		printf("number of traces in loop = %d \n", gather_traces->number_of_traces);
	}

	printf("number of traces after = %d \n", window_traces->number_of_traces);

}

int check_windowing_key(trace_t trace, char *wind_key, long min, long max){
	if(strcmp(wind_key, "tracl") == 0){
		if((trace.tracl >= min) && (trace.tracl <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"tracr")==0){
		if((trace.tracr >= min) && (trace.tracr <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"fldr")==0){
		if((trace.fldr >= min) && (trace.fldr <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"tracf")==0){
		if((trace.tracf >= min) && (trace.tracf <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"ep")==0){
		if((trace.ep >= min) && (trace.ep <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"cdp")==0){
		if((trace.cdp >= min) && (trace.cdp <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"cdpt")==0){
		if((trace.cdpt >= min) && (trace.cdpt <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"nvs")==0){
		if((trace.nvs >= min) && (trace.nvs <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"nhs")==0){
		if((trace.nhs >= min) && (trace.nhs <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"offset")==0){
		if((trace.offset >= min) && (trace.offset <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"gelev")==0){
		if((trace.gelev >= min) && (trace.gelev <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"selev")==0){
		if((trace.selev >= min) && (trace.selev <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"sdepth")==0){
		if((trace.sdepth >= min) && (trace.sdepth <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"gdel")==0){
		if((trace.gdel >= min) && (trace.gdel <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"sdel")==0){
		if((trace.sdel >= min) && (trace.sdel <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"swdep")==0){
		if((trace.swdep >= min) && (trace.swdep <= max)){
			return 1;
		} else {
			return 0;
		}
	}  else if(strcmp(wind_key,"gwdep")==0){
		if((trace.gwdep >= min) && (trace.gwdep <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"scalel")==0){
		if((trace.scalel >= min) && (trace.scalel <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"scalco")==0){
		if((trace.scalco >= min) && (trace.scalco <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sx")==0){
		if((trace.sx >= min) && (trace.sx <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sy")==0){
		if((trace.sy >= min) && (trace.sy <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"gx")==0){
		if((trace.gx >= min) && (trace.gx <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"gy")==0){
		if((trace.gy >= min) && (trace.gy <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"wevel")==0){
		if((trace.wevel >= min) && (trace.wevel <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"swevel")==0){
		if((trace.swevel >= min) && (trace.swevel <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sut")==0){
		if((trace.sut >= min) && (trace.sut <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"gut")==0){
		if((trace.gut >= min) && (trace.gut <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sstat")==0){
		if((trace.sstat >= min) && (trace.sstat <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"gstat")==0){
		if((trace.gstat >= min) && (trace.gstat <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"tstat")==0){
		if((trace.tstat >= min) && (trace.tstat <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"laga")==0){
		if((trace.laga >= min) && (trace.laga <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"lagb")==0){
		if((trace.lagb >= min) && (trace.lagb <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"delrt")==0){
		if((trace.delrt >= min) && (trace.delrt <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"muts")==0){
		if((trace.muts >= min) && (trace.muts <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"mute")==0){
		if((trace.mute >= min) && (trace.mute <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"ns")==0){
		if((trace.ns >= min) && (trace.ns <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"dt")==0){
		if((trace.dt >= min) && (trace.dt <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"gain")==0){
		if((trace.gain >= min) && (trace.gain <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"igc")==0){
		if((trace.igc >= min) && (trace.igc <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"igi")==0){
		if((trace.igi >= min) && (trace.igi <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"corr")==0){
		if((trace.corr >= min) && (trace.corr <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sfs")==0){
		if((trace.sfs >= min) && (trace.sfs <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sfe")==0){
		if((trace.sfe >= min) && (trace.sfe <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"slen")==0){
		if((trace.slen >= min) && (trace.slen <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"styp")==0){
		if((trace.styp >= min) && (trace.styp <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"stas")==0){
		if((trace.stas >= min) && (trace.stas <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"stae")==0){
		if((trace.stae >= min) && (trace.stae <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"tatyp")==0){
		if((trace.tatyp >= min) && (trace.tatyp <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"afilf")==0){
		if((trace.afilf >= min) && (trace.afilf <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"afils")==0){
		if((trace.afils >= min) && (trace.afils <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"nofilf")==0){
		if((trace.nofilf >= min) && (trace.nofilf <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"nofils")==0){
		if((trace.nofils >= min) && (trace.nofils <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"lcf")==0){
		if((trace.lcf >= min) && (trace.lcf <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"hcf")==0){
		if((trace.hcf >= min) && (trace.hcf <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"lcs")==0){
		if((trace.lcs >= min) && (trace.lcs <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"hcs")==0){
		if((trace.hcs >= min) && (trace.hcs <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"grnors")==0){
		if((trace.grnors >= min) && (trace.grnors <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"grnofr")==0){
		if((trace.grnofr >= min) && (trace.grnofr <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"grnlof")==0){
		if((trace.grnlof >= min) && (trace.grnlof <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"gaps")==0){
		if((trace.gaps >= min) && (trace.gaps <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"d1")==0){
		if((trace.d1 >= min) && (trace.d1 <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"f1")==0){
		if((trace.f1 >= min) && (trace.f1 <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"d2")==0){
		if((trace.d2 >= min) && (trace.d2 <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"f2")==0){
		if((trace.f2 >= min) && (trace.f2 <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"sfs")==0){
		if((trace.sfs >= min) && (trace.sfs <= max)){
			return 1;
		} else {
			return 0;
		}
	} else if(strcmp(wind_key,"ntr")==0){
		if((trace.ntr >= min) && (trace.ntr <= max)){
			return 1;
		} else {
			return 0;
		}
	} else {
		return -1;
	}

}
