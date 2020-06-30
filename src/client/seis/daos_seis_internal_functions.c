/*
 * daos_seis_internal_functions.c
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#include "daos_seis_internal_functions.h"


int daos_seis_fetch_entry(daos_handle_t oh, daos_handle_t th, struct seismic_entry *entry)
{
	d_sg_list_t	sgl;
	d_iov_t		sg_iovs;
	daos_iod_t	iod;
	daos_recx_t	recx;
	daos_key_t	dkey;
	int		rc;

	d_iov_set(&dkey, (void *)entry->dkey_name, strlen(entry->dkey_name));
	d_iov_set(&iod.iod_name, (void *)entry->akey_name, strlen(entry->akey_name));

	if(entry->iod_type == DAOS_IOD_SINGLE){
		recx.rx_nr	= 1;
		iod.iod_size	= entry->size;
	} else if(entry->iod_type == DAOS_IOD_ARRAY){
		recx.rx_nr	= entry->size;
		iod.iod_size	= 1;
	}

	iod.iod_nr	= 1;
	recx.rx_idx	= 0;
//	recx.rx_nr	= 1;
	iod.iod_recxs	= &recx;
	iod.iod_type	= entry->iod_type;
//	iod.iod_size	= entry->size;

	d_iov_set(&sg_iovs, entry->data, entry->size);

	sgl.sg_nr	= 1;
	sgl.sg_nr_out	= 0;
	sgl.sg_iovs	= &sg_iovs;

	rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL, NULL);

	if (rc) {
		printf("Failed to fetch entry %s (%d)\n", entry->dkey_name, rc);
	}
		return rc;
}

int daos_seis_array_fetch_entry(daos_handle_t oh, daos_handle_t th,int nrecords, struct seismic_entry *entry)
{
	d_sg_list_t	sgl;
	d_iov_t		sg_iovs;
	daos_iod_t	iod;
	daos_recx_t	recx;
	daos_key_t	dkey;
	int		rc;

	d_iov_set(&dkey, (void *)entry->dkey_name, strlen(entry->dkey_name));
	d_iov_set(&iod.iod_name, (void *)entry->akey_name, strlen(entry->akey_name));


	iod.iod_nr	= nrecords;
	recx.rx_idx	= 0;
	recx.rx_nr	= nrecords;
	iod.iod_recxs	= &recx;
	iod.iod_type	= entry->iod_type;
	iod.iod_size	= sizeof(daos_obj_id_t);

	d_iov_set(&sg_iovs, entry->data, entry->size);

	sgl.sg_nr	= nrecords;
	sgl.sg_nr_out	= 0;
	sgl.sg_iovs	= &sg_iovs;

	rc = daos_obj_fetch(oh, th, 0, &dkey, nrecords, &iod, &sgl, NULL, NULL);
	if (rc) {
		printf("Failed to fetch arrayy entry %s (%d)\n", entry->dkey_name, rc);
	}
//	else{
//		printf("SUCCESSFULLY FETCHED FROM ARRAYY \n");
//	}
		return rc;
}

int daos_seis_array_obj_update(daos_handle_t oh, daos_handle_t th, int nrecords, struct seismic_entry entry){
		d_sg_list_t	sgl;
		d_iov_t		sg_iovs;
		daos_iod_t	iod;
		daos_recx_t	recx;
		daos_key_t	dkey;
		unsigned int	i=0;
		int		rc;

		d_iov_set(&dkey, (void *)entry.dkey_name, strlen(entry.dkey_name));
		d_iov_set(&iod.iod_name, (void *)entry.akey_name, strlen(entry.akey_name));


		iod.iod_type	= entry.iod_type;
		iod.iod_size	= sizeof(daos_obj_id_t);

//		printf("ENTRY SIZE = %d \n", entry.size);
		d_iov_set(&sg_iovs, (void*)entry.data, nrecords*sizeof(daos_obj_id_t));
		recx.rx_idx = 0;
		recx.rx_nr = entry.size;

		iod.iod_nr	= nrecords;
		iod.iod_recxs	= &recx;

		sgl.sg_nr	= nrecords;
		sgl.sg_nr_out	= 0;
		sgl.sg_iovs	= &sg_iovs;

		rc = daos_obj_update(oh, th, 0, &dkey, nrecords, &iod, &sgl, NULL);

		if (rc) {
				printf("Failed to insert in array dkey: %s and akey: %s (%d)\n", entry.dkey_name, entry.akey_name, rc);
				return daos_der2errno(rc);
			}
//		else{
//			printf("SUCCESSFULLY inserted in array \n");
//		}

		return rc;
}

int daos_seis_th_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *data, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	th_seismic_entry = {0};

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

//	th_seismic_entry.oid = root_obj->oid;
//	th_seismic_entry.dkey_name = dkey_name;
//	th_seismic_entry.akey_name = akey_name;
//	th_seismic_entry.data = data;
//	th_seismic_entry.size = nbytes;

	rc = prepare_seismic_entry(& th_seismic_entry, root_obj->oid, dkey_name, akey_name,
				data, nbytes, DAOS_IOD_ARRAY);
	if(rc !=0){
		printf("ERROR PREPARING SEISMIC ENTRY IN daos_seis_th_update error = %d \n", rc);
	}
	rc = daos_seis_obj_update(root_obj->oh, th, th_seismic_entry);
	if (rc != 0)
		{
			return rc;
		}
//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_root_obj_create(dfs_t *dfs, segy_root_obj_t **obj,daos_oclass_id_t cid, char *name, dfs_obj_t *parent){

	int		rc;
	int daos_mode;
	daos_handle_t		th = DAOS_TX_NONE;
    daos_oclass_id_t ocid= cid;
	struct dfs_entry	dfs_entry = {0};
	struct seismic_entry	seismic_entry = {0};


	/*Allocate object pointer */
	D_ALLOC_PTR(*obj);
	if (*obj == NULL)
		return ENOMEM;

	strncpy((*obj)->name, name, SEIS_MAX_PATH);
	(*obj)->name[SEIS_MAX_PATH] = '\0';
	(*obj)->mode = S_IFDIR | S_IWUSR | S_IRUSR;
	(*obj)->flags = O_RDWR;
	(*obj)->number_of_traces = 0;
	if(parent==NULL)
		parent = &dfs->root;

//	oid_cp(&obj->cmp_oid, NULL);
//	oid_cp(&obj->shot_oid, NULL);
//	oid_cp(&obj->cdp_oid, NULL);

	/** Get new OID for root object */
	rc = oid_gen(dfs, cid,false, &(*obj)->oid);
	if (rc != 0)
		return rc;

	daos_mode = get_daos_obj_mode((*obj)->flags);

	rc = daos_obj_open(dfs->coh, (*obj)->oid, daos_mode, &(*obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}

	dfs_entry.oid = (*obj)->oid;
	dfs_entry.mode = (*obj)->mode;
	dfs_entry.chunk_size = 0;
	dfs_entry.atime = dfs_entry.mtime = dfs_entry.ctime = time(NULL);

/** insert segyRoot object created under dfs root */
	rc = insert_entry(parent->oh, th, (*obj)->name, dfs_entry);

	return rc;
}

int daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry){
		d_sg_list_t	sgl;
		d_iov_t		sg_iovs;
		daos_iod_t	iod;
		daos_recx_t	recx;
		daos_key_t	dkey;
		unsigned int	i;
		int		rc;

		d_iov_set(&dkey, (void *)entry.dkey_name, strlen(entry.dkey_name));
		d_iov_set(&iod.iod_name, (void *)entry.akey_name, strlen(entry.akey_name));

		if(entry.iod_type == DAOS_IOD_SINGLE){
			recx.rx_nr	= 1;
			iod.iod_size	= entry.size;
		} else if(entry.iod_type == DAOS_IOD_ARRAY){
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
				return daos_der2errno(rc);
			}

//		i = 0;
//		d_iov_set(&sg_iovs[i], &entry.sort_key, strlen(entry.sort_key));
//		d_iov_set(&iods[i].iod_name, &entry.akey_name[0], strlen(entry.akey_name[0]));
//		i++;
//		d_iov_set(&sg_iovs[i], &entry.bfh, strlen(entry.bfh));
//		d_iov_set(&iods[i].iod_name, &entry.akey_name[1], strlen(entry.akey_name[1]));
//		i++;
//		d_iov_set(&sg_iovs[i], &entry.etfh, strlen(entry.etfh));
//		d_iov_set(&iods[i].iod_name, &entry.akey_name[2], strlen(entry.akey_name[2]));
//		i++;
//
//
//		for (i = 0; i < 3; i++) {
//			sgls[i].sg_nr		= 1;
//			sgls[i].sg_nr_out	= 0;
//			sgls[i].sg_iovs		= &sg_iovs[i];
//
//			iods[i].iod_nr		= 1;
//			iods[i].iod_size	= DAOS_REC_ANY;
//			iods[i].iod_recxs	= NULL;
//			iods[i].iod_type	= DAOS_IOD_SINGLE;
//		}


	return rc;
}

int daos_seis_root_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char* databuf, int nbytes, daos_iod_type_t iod_type){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	seismic_entry = {0};

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

//	seismic_entry.oid = root_obj->oid;
//	seismic_entry.dkey_name = dkey_name;
//	seismic_entry.akey_name = akey_name;
//	seismic_entry.data = databuf;
//	seismic_entry.size = nbytes;

	rc = prepare_seismic_entry(& seismic_entry, root_obj->oid, dkey_name, akey_name,
			databuf, nbytes, iod_type);
	if(rc !=0){
		printf("ERROR PREPARING SEISMIC ENTRY IN daos_seis_root_update error = %d \n", rc);
	}

	rc = daos_seis_obj_update(root_obj->oh, th, seismic_entry);
	if (rc != 0)
	{
		return rc;
	}

//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_bh_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , bhed *bhdr, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	bh_seismic_entry = {0};

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

//	bh_seismic_entry.oid = root_obj->oid;
//	bh_seismic_entry.dkey_name = dkey_name;
//	bh_seismic_entry.akey_name = akey_name;
//	bh_seismic_entry.data = (char *)bhdr;
//	bh_seismic_entry.size = nbytes;

	rc = prepare_seismic_entry(& bh_seismic_entry, root_obj->oid, dkey_name, akey_name,
			(char *)bhdr, nbytes, DAOS_IOD_ARRAY);
	if(rc !=0){
		printf("ERROR PREPARING SEISMIC ENTRY IN daos_seis_root_update error = %d \n", rc);
	}

	rc = daos_seis_obj_update(root_obj->oh, th, bh_seismic_entry);
	if (rc != 0)
	{
		return rc;
	}
//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_exth_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *ebcbuf, int index, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	exth_seismic_entry = {0};
	char akey_index[100];
	sprintf(akey_index, "%d",index);
	char akey_extended[200] = "";
	strcat(akey_extended, akey_name);
	strcat(akey_extended, akey_index);

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

//	exth_seismic_entry.oid = root_obj->oid;
//	exth_seismic_entry.dkey_name = dkey_name;
//	exth_seismic_entry.akey_name = akey_extended;
//	exth_seismic_entry.data = ebcbuf;
//	exth_seismic_entry.size = nbytes;

	rc = prepare_seismic_entry(& exth_seismic_entry, root_obj->oid, dkey_name, akey_extended,
			ebcbuf, nbytes, DAOS_IOD_ARRAY);
	if(rc !=0){
		printf("ERROR PREPARING SEISMIC ENTRY IN daos_seis_exth_update error = %d \n", rc);
	}

	rc = daos_seis_obj_update(root_obj->oh, th, exth_seismic_entry);
	if(rc != 0){
		return rc;
	}

//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

void add_gather(seis_gather_t **head, seis_gather_t *new_gather) {
	if((*head) == NULL){
		(*head) = (seis_gather_t *) malloc(sizeof(seis_gather_t));
		(*head)->oids = (daos_obj_id_t *) malloc(5000 * sizeof(daos_obj_id_t));
		(*head)->nkeys = new_gather->nkeys;
		if((*head)->nkeys == 1 ){
			(*head)->keys[0] = new_gather->keys[0];
		} else {
			(*head)->keys[0] = new_gather->keys[0];
			(*head)->keys[1] = new_gather->keys[1];
		}
		(*head)->number_of_traces = new_gather->number_of_traces;
		(*head)->oids = new_gather->oids;
		(*head)->next_gather = NULL;
	} else{
		seis_gather_t * current = (*head);
		while (current->next_gather != NULL) {
			current = current->next_gather;
		}
	    current->next_gather = (seis_gather_t *) malloc(sizeof(seis_gather_t));
	    current->next_gather->oids = (daos_obj_id_t *) malloc(5000 * sizeof(daos_obj_id_t));
	    current->next_gather->nkeys = new_gather->nkeys;
		if(current->next_gather->nkeys == 1 ){
			current->next_gather->keys[0] = new_gather->keys[0];
		} else {
			current->next_gather->keys[0] = new_gather->keys[0];
			current->next_gather->keys[1] = new_gather->keys[1];
		}
		current->next_gather->number_of_traces = new_gather->number_of_traces;
		current->next_gather->oids = new_gather->oids;
		current->next_gather->next_gather = NULL;
	}
}

int update_gather_traces(seis_gather_t *head, seis_obj_t *object, char *dkey_name, char *akey_name){
	if(head == NULL){
		printf("NO GATHERS EXIST \n");
		return 0;
	}else {
		while(head != NULL){
			int ntraces = head->number_of_traces;
			struct seismic_entry entry;
			int rc;
			int nkeys = head->nkeys;
			char temp[200]="";
			char gather_dkey_name[200] = "";
			if(nkeys == 1){
				strcat(gather_dkey_name,dkey_name);
				sprintf(temp, "%d", head->keys[0]);
				strcat(gather_dkey_name,temp);

				prepare_seismic_entry(&entry, object->oid, gather_dkey_name, akey_name,
												(char*)&ntraces, sizeof(int), DAOS_IOD_SINGLE);
				rc = daos_seis_obj_update(object->oh, DAOS_TX_NONE, entry);
				if(rc !=0){
					printf("ERROR UPDATING %s object number_of_traces key, error: %d \n",object->name, rc);
					return rc;
				}
			} else {
				strcat(gather_dkey_name,DS_D_CMP);
				sprintf(temp, "%d", head->keys[0]);
				strcat(gather_dkey_name,temp);
				strcat(gather_dkey_name,"_");
				sprintf(temp, "%d", head->keys[1]);
				strcat(gather_dkey_name,temp);
				prepare_seismic_entry(&entry, object->oid, gather_dkey_name, akey_name,
									(char*)&ntraces, sizeof(int), DAOS_IOD_SINGLE);
				rc = daos_seis_obj_update(object->oh, DAOS_TX_NONE, entry);
				if(rc !=0){
					printf("ERROR UPDATING %s _ object number_of_traces key, error: %d", object->name, rc);
					return rc;
				}
			}
			head = head->next_gather;
		}
	}
return 0;
}

int check_key_value(int *targets,seis_gather_t *head, daos_obj_id_t trace_obj, int *ntraces){
	int exists = 0;
	if(head == NULL){
		printf("NO GATHERS EXIST \n");
		exists =0;
		return exists;
	} else {
		if(head->nkeys == 1){
			while(head != NULL){
				if(head->keys[0] == targets[0]){
					head->number_of_traces++;
					*ntraces = head->number_of_traces;
				exists =1;
				return exists;
			} else {
				head = head->next_gather;
			}
		}
	} else{
		while(head != NULL){
				if(head->keys[0] == targets[0] && head->keys[1] == targets[1]){
				head->number_of_traces++;
				*ntraces = head->number_of_traces;
				exists =1;
				return exists;
			} else {
				head = head->next_gather;
			}
		}
	}
	}
	return exists;
}

int daos_seis_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid, segy_root_obj_t *parent,
			seis_obj_t **shot_obj, seis_obj_t **cmp_obj, seis_obj_t **offset_obj){
	int		rc;
	int daos_mode;
	daos_handle_t		th = DAOS_TX_NONE;
    daos_oclass_id_t ocid= OC_SX;

	/*Allocate shot object pointer */
	D_ALLOC_PTR(*shot_obj);
	if (*shot_obj == NULL)
		return ENOMEM;
	strncpy((*shot_obj)->name, "shot_gather", SEIS_MAX_PATH);
	(*shot_obj)->name[SEIS_MAX_PATH] = '\0';
	(*shot_obj)->mode = S_IWUSR | S_IRUSR;
	(*shot_obj)->flags = O_RDWR;
	(*shot_obj)->sequence_number = 0;
	(*shot_obj)->gathers = NULL;
	(*shot_obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*shot_obj)->oid);
	if (rc != 0)
		return rc;
	oid_cp(&parent->shot_oid, (*shot_obj)->oid);

	daos_mode = get_daos_obj_mode((*shot_obj)->flags);

	rc = daos_obj_open(dfs->coh, (*shot_obj)->oid, daos_mode, &(*shot_obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_SHOT_GATHER,
			(char*)&(*shot_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);

	/*Allocate object pointer */
	D_ALLOC_PTR(*cmp_obj);
	if (*cmp_obj == NULL)
		return ENOMEM;
	strncpy((*cmp_obj)->name, "cmp_gather", SEIS_MAX_PATH);
	(*cmp_obj)->name[SEIS_MAX_PATH] = '\0';
	(*cmp_obj)->mode = S_IWUSR | S_IRUSR;
	(*cmp_obj)->flags = O_RDWR;
	(*cmp_obj)->sequence_number = 0;
	(*cmp_obj)->gathers = NULL;
	(*cmp_obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*cmp_obj)->oid);
	if (rc != 0)
		return rc;
	oid_cp(&parent->cmp_oid, (*cmp_obj)->oid);

	daos_mode = get_daos_obj_mode((*cmp_obj)->flags);

	rc = daos_obj_open(dfs->coh, (*cmp_obj)->oid, daos_mode, &(*cmp_obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_CMP_GATHER ,
			(char*)&(*cmp_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);


	/*Allocate object pointer */
	D_ALLOC_PTR(*offset_obj);
	if (*offset_obj == NULL)
		return ENOMEM;
	strncpy((*offset_obj)->name, "offset_gather", SEIS_MAX_PATH);
	(*offset_obj)->name[SEIS_MAX_PATH] = '\0';
	(*offset_obj)->mode = S_IFREG | S_IWUSR | S_IRUSR;
	(*offset_obj)->flags = O_RDWR;
	(*offset_obj)->sequence_number = 0;
	(*offset_obj)->gathers = NULL;
	(*offset_obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*offset_obj)->oid);
	if (rc != 0)
		return rc;
	oid_cp(&parent->offset_oid, (*offset_obj)->oid);

	daos_mode = get_daos_obj_mode((*offset_obj)->flags);

	rc = daos_obj_open(dfs->coh, (*offset_obj)->oid, daos_mode, &(*offset_obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_OFFSET_GATHER ,
				(char*)&(*offset_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);

//	daos_obj_close(shot_obj->oh, NULL);
//	daos_obj_close(cmp_obj->oh, NULL);
//	daos_obj_close(offset_obj->oh, NULL);

	return rc;
}

int daos_seis_trh_update(dfs_t* dfs, trace_obj_t* tr_obj, segy *tr, int hdrbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	tr_entry = {0};

//	tr_entry.oid = tr_obj->oid;
//	tr_entry.dkey_name = DS_D_TRACE_HEADER;
//	tr_entry.akey_name = DS_A_TRACE_HEADER;
//	tr_entry.data = (char*)tr;
//	tr_entry.size = hdrbytes;
	rc = prepare_seismic_entry(&tr_entry, tr_obj->oid, DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
				(char*)tr, hdrbytes, DAOS_IOD_ARRAY);
	if(rc!=0){
		printf("FAILED TO PREPARE SEISMIC ENTRY IN TRH UPDATE rc= %d \n", rc);
	}
	rc = daos_seis_obj_update(tr_obj->oh, th, tr_entry);
	if(rc!=0){
		printf("ERROR UPDATING TRACE header KEY----------------- \n");
	}

//
//	int data_length = tr->ns;
//	int offset = 0;
//	int start=0;
//	int end=199;
//	char st_index[100];
//	char end_index[100];
//	char trace_data_dkey[200]= DS_D_TRACE_DATA;
////	printf("Data length %d \n", data_length);
//	while(data_length > 0){
//		char trace_dkey[200] = "";
//		sprintf(st_index, "%d",start);
//		sprintf(end_index, "%d",end);
//		strcat(trace_dkey, DS_D_TRACE_DATA);
//		strcat(trace_dkey, st_index);
//		strcat(trace_dkey, "_");
//		strcat(trace_dkey, end_index);
////		printf("DKEY === %s \n", trace_dkey);
//
//		tr_entry.oid = tr_obj->oid;
//		tr_entry.dkey_name = trace_dkey;
//		tr_entry.akey_name = DS_A_TRACE_DATA;
//		tr_entry.data = (char*)((tr->data)+offset);
//		tr_entry.size = min(200,data_length)*sizeof(float);
//
//		rc = daos_seis_obj_update(tr_obj->oh, th, tr_entry);
//		if(rc!=0){
//			printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
//		}
//
//		data_length = data_length - 200;
//		offset = offset + 200;
//		start = end+1;
//		end = start +199;
//		if(end > data_length && data_length <= 200)
//		{
//			end = start + data_length;
//		}
//
//	}

	return rc;
}

int daos_seis_tr_data_update(dfs_t* dfs, trace_obj_t* trace_data_obj, segy *trace){
    int		rc;
    int daos_mode;


	int data_length = trace->ns;
	int offset = 0;
//	int start=0;
//	int end=199;
//	char st_index[100];
//	char end_index[100];
//	char trace_data_dkey[200]= DS_D_TRACE_DATA;
	struct seismic_entry	tr_entry = {0};
	tr_entry.data = (char*)(trace->data);


	daos_array_iod_t iod;
	daos_range_t		rg;
	d_sg_list_t sgl;

	sgl.sg_nr = data_length;
	sgl.sg_nr_out = 0;
	d_iov_t iov[sgl.sg_nr];
	int j=0;
	for(int i=0; i < sgl.sg_nr; i++){
		d_iov_set(&iov[i], (void*)&(tr_entry.data[j]), sizeof(float));
		j+=4;
	}

	sgl.sg_iovs = &iov;
	iod.arr_nr = 1;
	rg.rg_len = data_length * sizeof(float);
	rg.rg_idx = offset;
	iod.arr_rgs = &rg;

	rc = daos_array_write(trace_data_obj->oh, DAOS_TX_NONE, &iod, &sgl, NULL);
	if(rc!=0){
		printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
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
		daos_handle_t		th = DAOS_TX_NONE;
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
		(*trace_hdr_obj)->mode = S_IFREG | S_IWUSR | S_IRUSR;
		(*trace_hdr_obj)->flags = O_RDWR;
		(*trace_hdr_obj)->trace = malloc(sizeof(segy));

		/** Get new OID for trace header object */
		rc = oid_gen(dfs, cid,false, &(*trace_hdr_obj)->oid);
		if (rc != 0){
			printf("ERROR GENERATING OBJECT ID for trace header %d \n", rc);
			return rc;
		}

		printf("Created TRACE HEADER OID %llu%llu\n", (*trace_hdr_obj)->oid.lo, (*trace_hdr_obj)->oid.hi);

		daos_mode = get_daos_obj_mode((*trace_hdr_obj)->flags);
		rc = daos_obj_open(dfs->coh, (*trace_hdr_obj)->oid, daos_mode, &(*trace_hdr_obj)->oh, NULL);
		if (rc) {
			printf("daos_obj_open() Failed (%d)\n", rc);
			return daos_der2errno(rc);
		}
		struct seismic_entry	tr_entry = {0};
		rc = daos_seis_trh_update(dfs, *trace_hdr_obj,trace, 240);
		if(rc !=0){
			printf("ERROR updating trace header object error number = %d  \n", rc);
			return rc;
		}


		D_ALLOC_PTR(trace_data_obj);
		if ((trace_data_obj) == NULL)
			return ENOMEM;

		strcat(trace_data_name, tr_index);

		strncpy((trace_data_obj)->name, trace_data_name, SEIS_MAX_PATH);
		(trace_data_obj)->name[SEIS_MAX_PATH] = '\0';
		(trace_data_obj)->mode = S_IFREG | S_IWUSR | S_IRUSR;
		(trace_data_obj)->flags = O_RDWR;
		// trace struct here is useless!!
		(trace_data_obj)->trace = malloc(sizeof(segy));

		/** Get new OID for trace data object */
		rc = oid_gen(dfs, cid,true, &(trace_data_obj)->oid);

		daos_obj_id_t temp_oid;
		temp_oid = get_tr_data_oid(&(*trace_hdr_obj)->oid,cid);

		printf("Calculated TEMP TRACE DATA OID %llu  %llu\n", temp_oid.lo, temp_oid.hi);


		if (rc != 0){
			printf("ERROR GENERATING OBJECT ID for trace data %d \n", rc);
			return rc;
		}
		printf("Created TRACE DATA OID %llu  %llu\n", (trace_data_obj)->oid.lo, (trace_data_obj)->oid.hi);

		/** Open the array object for the file */
		rc = daos_array_open_with_attr(dfs->coh, (trace_data_obj)->oid, th,
			DAOS_OO_RW, 1,200*sizeof(float), &trace_data_obj->oh, NULL);
		if (rc != 0) {
			printf("daos_array_open_with_attr()-->>Trace data object<<-- failed (%d)\n", rc);
			return daos_der2errno(rc);
		}


//		daos_mode = get_daos_obj_mode((trace_data_obj)->flags);
//		rc = daos_obj_open(dfs->coh, (trace_data_obj)->oid, daos_mode, &(trace_data_obj)->oh, NULL);
//		if (rc) {
//			D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//			return daos_der2errno(rc);
//		}

		rc = daos_seis_tr_data_update(dfs, trace_data_obj,trace);
		if(rc !=0){
			printf("ERROR updating trace data object error number = %d  \n", rc);
			return rc;
		}
		rc = daos_array_close(trace_data_obj->oh, NULL);

//		rc = daos_obj_close(trace_data_obj->oh, NULL);
		if(rc != 0){
			printf("ERROR Closing trace data object \n");
			return rc;
		}

	return rc;
}

int prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid, char *dkey, char *akey,
			char *data,int size, daos_iod_type_t iod_type){
	entry->oid = oid;
	entry->dkey_name = dkey;
	entry->akey_name = akey;
	entry->data = data;
	entry->size = size;
	entry->iod_type = iod_type;
	return 0;
}

int daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, segy *trace,
			seis_obj_t *shot_obj, seis_obj_t *cmp_obj, seis_obj_t *off_obj){
	int rc;
	daos_handle_t	th = DAOS_TX_NONE;
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

//	for(int i=0; i< (shot_obj->sequence_number); i++){
		keys[0]=shot_id;
		int no_of_traces;
		if(check_key_value(keys,shot_obj->gathers, trace_obj->oid, & no_of_traces) == 1){
			char temp[200]="";
			char shot_dkey_name[200] = "";
			strcat(shot_dkey_name,DS_D_SHOT);
			sprintf(temp, "%d", shot_id);
			strcat(shot_dkey_name,temp);
			shot_exists=1;
//			seis_gather_t shot_gather_data = {0};
//			update_gather(shot_obj->gathers, trace_obj->oid);


//			(shot_obj->gathers[i].number_of_traces)++;

			char trace_akey_name[200] = "";
			char tr_temp[200] = "";
			strcat(trace_akey_name, DS_A_TRACE);
			sprintf(tr_temp, "%d", no_of_traces);
			strcat(trace_akey_name, tr_temp);

//			printf("number of traces already exist ===== %d \n", shot_obj->gathers[i].number_of_traces);
			prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, trace_akey_name,
					(char*)&trace_obj->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
			rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR UPDATING shot trace_OIDS array, error: %d \n", rc);
				return rc;
			}

//			(shot_obj->gathers[i].number_of_traces)++;
//			prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_NTRACES,
//											(char*)&(shot_obj->gathers[i].number_of_traces), sizeof(int));
//			rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
//			if(rc !=0){
//				printf("ERROR UPDATING shot number_of_traces key, error: %d \n", rc);
//				return rc;
//			}
		}
//			break;
//		} else {
//			continue;
//		}
//	}
//	for(int i=0; i<(cmp_obj->sequence_number);i++){
		keys[0]=cmp_x;
		keys[1] = cmp_y;
//		int no_of_traces;
		if(check_key_value(keys,cmp_obj->gathers, trace_obj->oid, & no_of_traces) == 1){

//		if(((cmp_obj->gathers[i].keys[0]) == cmp_x) && ((cmp_obj->gathers[i].keys[1]) == cmp_y)){
				char cmp_dkey_name[200] = "";
				char temp[200]="";
				strcat(cmp_dkey_name,DS_D_CMP);
				sprintf(temp, "%d", cmp_x);
				strcat(cmp_dkey_name,temp);
				strcat(cmp_dkey_name,"_");
				sprintf(temp, "%d", cmp_y);
				strcat(cmp_dkey_name,temp);
				cmp_exists=1;

//				(cmp_obj->gathers[i].number_of_traces)++;

				char trace_akey_name[200] = "";
				char tr_temp[200] = "";
				strcat(trace_akey_name, DS_A_TRACE);
				sprintf(tr_temp, "%d", no_of_traces);
				strcat(trace_akey_name, tr_temp);

				prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, trace_akey_name,
						(char*)&trace_obj->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
				rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
				if(rc !=0){
					printf("ERROR UPDATING Cmp trace_OIDS array, error: %d", rc);
					return rc;
				}

//				cmp_obj->gathers[i].number_of_traces++;
//				prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_NTRACES,
//									(char*)&(cmp_obj->gathers[i].number_of_traces), sizeof(int));
//				rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
//				if(rc !=0){
//					printf("ERROR UPDATING CMP number_of_traces key, error: %d", rc);
//					return rc;
//				}
//				break;

		}
//			} else {
//				cmp_exists=0;
//				continue;
//			}
//	}
//
//	for(int i=0; i<(off_obj->sequence_number);i++){
			keys[0]=off_x;
			keys[1] =off_y;
	//		int no_of_traces;
		if(check_key_value(keys,off_obj->gathers, trace_obj->oid, & no_of_traces) == 1){
//		if(((off_obj->gathers[i]).keys[0]) == off_x && ((off_obj->gathers[i]).keys[1]) == off_y){
				offset_exists=1;
				char temp[200]="";
				char off_dkey_name[200] = "";
				strcat(off_dkey_name,DS_D_OFFSET);
				sprintf(temp, "%d", off_x);
				strcat(off_dkey_name,temp);
				strcat(off_dkey_name,"_");
				sprintf(temp, "%d", off_y);
				strcat(off_dkey_name,temp);
//
//				sprintf(temp, "%d", i);
//				strcat(off_dkey_name,temp);

//				(off_obj->gathers[i].number_of_traces)++;

				char trace_akey_name[200] = "";
				char tr_temp[200] = "";
				strcat(trace_akey_name, DS_A_TRACE);
				sprintf(tr_temp, "%d", no_of_traces);
				strcat(trace_akey_name, tr_temp);

				prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, trace_akey_name,
						(char*)&trace_obj->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
				rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
				if(rc !=0){
					printf("ERROR UPDATING OFFSET trace_OIDS array, error: %d", rc);
					return rc;
				}

//				prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_NTRACES,
//												(char*)&(off_obj->gathers[i].number_of_traces), sizeof(int));
//				rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
//				if(rc !=0){
//					printf("ERROR UPDATING number of traces key, error: %d", rc);
//					return rc;
//				}
		}
//				break;
//			} else {
//				offset_exists=0;
//				continue;
//			}
//	}

	/** if shot id, cmp, and offset doesn't already exist */
	if(!shot_exists){
		printf("HELLLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO SHOT \n");
		seis_gather_t shot_gather_data = {0};
		shot_gather_data.oids = malloc(5000 *sizeof(daos_obj_id_t));
		char temp[200]="";
		shot_gather_data.oids[0] = trace_obj->oid;
		shot_gather_data.number_of_traces = 1;
		shot_gather_data.nkeys = 1;
		shot_gather_data.keys[0] = shot_id;
		char shot_dkey_name[200] = "";
		char trace_akey_name[200] = "";
		char tr_temp[200] = "";
		strcat(trace_akey_name, DS_A_TRACE);
		sprintf(tr_temp, "%d", 1);
		strcat(trace_akey_name, tr_temp);

		strcat(shot_dkey_name,DS_D_SHOT);
		sprintf(temp, "%d", shot_id);
		strcat(shot_dkey_name,temp);

		prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, trace_akey_name,
										(char*)&(shot_gather_data.oids[0]), sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
		if(rc !=0){
			printf("ERROR adding shot array_of_traces key, error: %d", rc);
			return rc;
		}
		prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_SHOT_ID,
							(char*)&shot_id, sizeof(int), DAOS_IOD_SINGLE);
		rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
		if(rc !=0){
			printf("ERROR adding shot shot_id key, error: %d", rc);
			return rc;
		}
		ntraces=1;

//		prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_NTRACES,
//										(char*)&ntraces, sizeof(int));
//		rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
//		if(rc !=0){
//			printf("ERROR Adding shot number of traces key, error: %d", rc);
//			return rc;
//		}

		add_gather(&(shot_obj->gathers), &shot_gather_data);

//		shot_obj->gathers = shot_gather_data;
//		shot_obj->gathers->number_of_traces = 1;
		shot_obj->sequence_number++;
		shot_obj->number_of_gathers++;
	}

		if(!cmp_exists){
			printf("HELLLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO CMP \n");
			char cmp_dkey_name[200] = "";
			char temp[200]="";
			seis_gather_t cmp_gather_data= {0};
			cmp_gather_data.oids = malloc(5000 *sizeof(daos_obj_id_t));
			cmp_gather_data.oids[0] = trace_obj->oid;
			cmp_gather_data.number_of_traces=1;
			cmp_gather_data.nkeys=2;
			cmp_gather_data.keys[0] = cmp_x;
			cmp_gather_data.keys[1] = cmp_y;
			char trace_akey_name[200] = "";
			char tr_temp[200] = "";
			strcat(trace_akey_name, DS_A_TRACE);
			sprintf(tr_temp, "%d", 1);
			strcat(trace_akey_name, tr_temp);

			strcat(cmp_dkey_name,DS_D_CMP);
			sprintf(temp, "%d", cmp_x);
			strcat(cmp_dkey_name,temp);
			strcat(cmp_dkey_name,"_");
			sprintf(temp, "%d", cmp_y);
			strcat(cmp_dkey_name,temp);

//			sprintf(temp, "%d", cmp_obj->sequence_number);
//			strcat(cmp_dkey_name,temp);
//			printf("CMPX>>>> %d \n",cmp_x);
//			printf("CMPY>>>> %d \n",cmp_y);
//			printf("CMPDKEYNAME >>>>>>>>(%s) \n", cmp_dkey_name);

			prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, trace_akey_name,
					(char*)&trace_obj->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);

			rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING Cmp trace_OIDS array, error: %d", rc);
				return rc;
			}

			prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_CMP_VAL,
								(char*)cmp_gather_data.keys, sizeof(int)*2, DAOS_IOD_ARRAY);

			rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING CMP value key, error: %d", rc);
				return rc;
			}
			ntraces=1;
//			prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_NTRACES,
//											(char*)&ntraces, sizeof(int));
//			rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
//			if(rc !=0){
//				printf("ERROR ADDING CMP number_of_traces key, error: %d", rc);
//				return rc;
//			}

			add_gather(&(cmp_obj->gathers), &cmp_gather_data);

//			cmp_obj->gathers[cmp_obj->sequence_number]=cmp_gather_data;
//			cmp_obj->gathers[cmp_obj->sequence_number].number_of_traces = 1;
			cmp_obj->sequence_number++;
			cmp_obj->number_of_gathers++;

		}

		if(!offset_exists){
			printf("HELLLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO OFFSET \n");
			char off_dkey_name[200] = "";
			char temp[200]="";
			seis_gather_t off_gather_data= {0};
			off_gather_data.oids = malloc(5000 *sizeof(daos_obj_id_t));
			off_gather_data.oids[0] = trace_obj->oid;
			off_gather_data.number_of_traces=1;
			off_gather_data.nkeys=2;
			off_gather_data.keys[0] = off_x;
			off_gather_data.keys[1] = off_y;

			char trace_akey_name[200] = "";
			char tr_temp[200] = "";
			strcat(trace_akey_name, DS_A_TRACE);
			sprintf(tr_temp, "%d", 1);
			strcat(trace_akey_name, tr_temp);

			strcat(off_dkey_name,DS_D_OFFSET);
			sprintf(temp, "%d", off_x);
			strcat(off_dkey_name,temp);
			strcat(off_dkey_name,"_");
			sprintf(temp, "%d", off_y);
			strcat(off_dkey_name,temp);

//			sprintf(temp, "%d", off_obj->sequence_number);
//			strcat(off_dkey_name,temp);
//			printf("OFFX>>>> %d \n",off_x);
//			printf("OFFY>>>> %d \n",off_y);
//			printf("OFFDKEYNAME >>>>>>>>(%s) \n", off_dkey_name);

			prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, trace_akey_name,
					(char*)&trace_obj->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
			rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING OFFSET trace_OIDS array, error: %d", rc);
				return rc;
			}

			prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_OFF_VAL,
						(char*)off_gather_data.keys, sizeof(int)*2, DAOS_IOD_ARRAY);
			rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING GATHER offset value key, error: %d", rc);
				return rc;
			}
			ntraces = 1;
//			prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_NTRACES,
//											(char*)&ntraces, sizeof(int));
//			rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
//			if(rc !=0){
//				printf("ERROR ADDING number of traces key, error: %d", rc);
//				return rc;
//			}
			add_gather(&(off_obj->gathers), &off_gather_data);

//			off_obj->gathers[off_obj->sequence_number]=off_gather_data;
//			off_obj->gathers[off_obj->sequence_number].number_of_traces = 1;
			off_obj->sequence_number++;
			off_obj->number_of_gathers++;
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


