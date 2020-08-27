/*
 * daos_seis_internal_functions.c
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#include "daos_seis_internal_functions.h"


dfs_obj_t * get_parent_of_file_new(dfs_t *dfs, const char *file_directory, int allow_creation,
                               char *file_name, int verbose_output) {
	dfs_obj_t *parent = NULL;
    daos_oclass_id_t cid= OC_SX;
    char temp[2048];
    int array_len = 0;
    strcpy(temp, file_directory);
    const char *sep = "/";
    char *token = strtok(temp, sep);
    while( token != NULL ) {
        array_len++;
        token = strtok(NULL, sep);
    }
    char **array = malloc(sizeof(char *) * array_len);
    strcpy(temp, file_directory);
    token = strtok(temp, sep);
    int i = 0;
    while( token != NULL ) {
        array[i] = malloc(sizeof(char) * (strlen(token) + 1));
        strcpy(array[i], token);
        token = strtok(NULL, sep);
        i++;
    }
    for (i = 0; i < array_len - 1; i++) {
        dfs_obj_t *temp_obj;
        int err = dfs_lookup_rel(dfs, parent, array[i], O_RDWR, &temp_obj, NULL, NULL);
        if (err == 0) {
            if(verbose_output){
                warn("Subdirectory '%s' already exist \n", array[i]);
            }
        } else if (allow_creation) {
            mode_t mode = 0666;
            err = dfs_mkdir(dfs, parent, array[i], mode,cid);
            if (err == 0) {
                if(verbose_output) {
                    warn("Created directory '%s'\n", array[i]);
                }
                check_error_code(dfs_lookup_rel(dfs, parent, array[i], O_RDWR, &temp_obj, NULL, NULL)
                        , "Lookup after mkdir");
            } else {
                warn("Mkdir on %s failed with error code : %d \n", array[i], err);
            }
        } else {
            warn("Relative lookup on %s failed with error code : %d \n", array[i], err);
        }
        parent = temp_obj;
    }
    strcpy(file_name, array[array_len - 1]);
//    for (i = 0; i < array_len; i++) {
//        free(array[i]);
//    }
//    free(array);

    return parent;
}


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
		(*head)->unique_key = new_gather->unique_key;
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
		current->next_gather->unique_key = new_gather->unique_key;
		current->next_gather->number_of_traces = new_gather->number_of_traces;
//		current->next_gather->oids = new_gather->oids;
		memcpy(current->next_gather->oids, new_gather->oids, sizeof(daos_obj_id_t) * 50);
		current->next_gather->next_gather = NULL;
	}
//	printf("FINISHED ADDING NEW GATHER \n");
}

void merge_trace_lists(traces_list_t **headers,traces_list_t **gather_headers){

	traces_headers_t *temp = (*headers)->head;
	if((*headers)->head == NULL){
		(*headers)->head = (*gather_headers)->head;
		(*headers)->tail = (*gather_headers)->tail;
		(*headers)->size = (*gather_headers)->size;
	} else {
		(*headers)->tail->next_trace = (*gather_headers)->head;
		(*headers)->tail = (*gather_headers)->tail;
		(*headers)->size = (*headers)->size + (*gather_headers)->size;
	}
}

void add_trace_header(trace_t *trace, traces_list_t **head){
//	printf("ADDING NEW TRACE HEADER \n");
	traces_headers_t *new_node = (traces_headers_t *) malloc(sizeof(traces_headers_t));
	new_node->trace = *trace;
	new_node->next_trace = NULL;
//	traces_headers_t *last = *head;

	if((*head)->head == NULL){
		(*head)->head = new_node;
		(*head)->tail = new_node;
		(*head)->size ++;
		return;
//		printf("AFTER ADDING NEW TRACE HEADER L.L was EMPTY\n");
	} else {
		(*head)->tail->next_trace = new_node;
		(*head)->tail = new_node;
		(*head)->size ++;
	}
//	while (last->next_trace != NULL) {
//		last = last->next_trace;
//	}
//	last->next_trace = new_node;
//	printf("AFTER ADDING NEW TRACE HEADER L.L wasNOT EMPTY\n");

}

add_trace_data(trace_t trace, traces_headers_t *head){
	if((head) == NULL){
		(head) = (traces_headers_t *) malloc(sizeof(traces_headers_t));
		(head)->trace.data = trace.data;
		(head)->next_trace = NULL;
	} else {
		traces_headers_t *current = (head);
		while (current->next_trace != NULL) {
			current = current->next_trace;
		}
		current->next_trace = (traces_headers_t *) malloc(sizeof(traces_headers_t));
		current->next_trace->trace.data = trace.data;
		current->next_trace->next_trace = NULL;
	}
}

//int update_gather_traces(dfs_t *dfs, seis_gather_t *head, seis_obj_t *object, char *dkey_name, char *akey_name){
//
//	if(head == NULL){
//		printf("NO GATHERS EXIST \n");
//		return 0;
//	} else {
//		int z = 0;
//		while(head != NULL){
//			int ntraces = head->number_of_traces;
//			int rc;
//			int nkeys = head->nkeys;
//			char temp[200]="";
//			char gather_dkey_name[200] = "";
//			if(nkeys==1){
//				strcat(gather_dkey_name,dkey_name);
//				sprintf(temp, "%d", head->keys[0]);
//				strcat(gather_dkey_name,temp);
//			} else if(nkeys ==2){
//				strcat(gather_dkey_name,dkey_name);
//				sprintf(temp, "%d", head->keys[0]);
//				strcat(gather_dkey_name,temp);
//				strcat(gather_dkey_name,"_");
//				sprintf(temp, "%d", head->keys[1]);
//				strcat(gather_dkey_name,temp);
//			}
//			//insert array object_id in gather object...
//			rc = daos_seis_gather_oids_array_update(dfs, &(object->seis_gather_trace_oids_obj[z]), head);
//			if(rc != 0) {
//				printf("ERROR UPDATING %s object TRACE OBJECT IDS ARRAY, error: %d \n",object->name, rc);
//				return rc;
//			}
//			rc = update_gather_object(object, gather_dkey_name, DS_A_GATHER_TRACE_OIDS, (char*)&((object->seis_gather_trace_oids_obj[z]).oid),
//								sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
//			if(rc != 0) {
//				printf("ERROR UPDATING %s object TRACE OBJECT IDS key, error: %d \n",object->name, rc);
//				return rc;
//			}
//			rc = update_gather_object(object, gather_dkey_name, akey_name, (char*)&ntraces,
//								sizeof(int), DAOS_IOD_SINGLE);
//			if(rc != 0) {
//				printf("ERROR UPDATING %s object number_of_traces key, error: %d \n",object->name, rc);
//				return rc;
//			}
//
//			head = head->next_gather;
//			z++;
//		}
//	}
//	return 0;
//}

int new_update_gather_traces(dfs_t *dfs, seis_gather_t *head, seis_obj_t *object, char *dkey_name, char *akey_name){

	if(head == NULL){
		printf("NO GATHERS EXIST \n");
		return 0;
	} else {
		int z = 0;
		while(head != NULL){
			int ntraces = head->number_of_traces;
			int rc;
			char temp[200]="";
			char gather_dkey_name[200] = "";
			strcat(gather_dkey_name,dkey_name);
//			sprintf(temp, "%ld", head->unique_key);
			val_sprintf(temp, head->unique_key, object->name);

			strcat(gather_dkey_name,temp);
//			printf("GATHER_DKEY %s z>>>> %d\n", gather_dkey_name, z);

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

//int check_key_value(int *targets,seis_gather_t *head, daos_obj_id_t trace_obj_id, int *ntraces){
//
//	int exists = 0;
//	if(head == NULL) {
//		printf("NO GATHERS EXIST \n");
//		exists =0;
//		return exists;
//	} else {
//		if(head->nkeys == 1) {
//			while(head != NULL) {
//				if(head->keys[0] == targets[0]) {
//					head->oids[head->number_of_traces] = trace_obj_id;
//					head->number_of_traces++;
//					*ntraces = head->number_of_traces;
//					exists = 1;
//					if(head->number_of_traces % 50 == 0) {
//						head->oids = (daos_obj_id_t *)realloc(head->oids, (head->number_of_traces + 50) * sizeof(daos_obj_id_t));
//					}
//					return exists;
//				} else {
//					head = head->next_gather;
//				}
//			}
//		} else {
//			while(head != NULL){
//					if(head->keys[0] == targets[0] && head->keys[1] == targets[1]){
//						head->oids[head->number_of_traces] = trace_obj_id;
//						head->number_of_traces++;
//						*ntraces = head->number_of_traces;
//						exists = 1;
//						if(head->number_of_traces % 50 == 0) {
//							   head->oids = (daos_obj_id_t *)realloc(head->oids, (head->number_of_traces + 50) * sizeof(daos_obj_id_t));
//						}
//						return exists;
//				} else {
//					head = head->next_gather;
//				}
//			}
//		}
//	}
//	return exists;
//}

int new_check_key_value(Value target, char *key, seis_gather_t *head, daos_obj_id_t trace_obj_id, int *ntraces){

	int exists = 0;
	if(head == NULL) {
		printf("NO GATHERS EXIST \n");
		exists =0;
		return exists;
	} else {
		while(head != NULL) {
			if(!valcmp(hdtype(key), head->unique_key, target)) {
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
							seis_obj_t **obj, char* key){
	int		rc;
	int daos_mode;

	/*Allocate shot object pointer */
	D_ALLOC_PTR(*obj);
	if (*obj == NULL)
		return ENOMEM;
//	strncpy((*obj)->name, "shot_gather", SEIS_MAX_PATH);
	strcpy((*obj)->name, key);
//	(*obj)->name[SEIS_MAX_PATH] = '\0';
	(*obj)->sequence_number = 0;
	(*obj)->gathers = NULL;
	(*obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid,false, &(*obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID err= %d \n",rc);
		return rc;
	}


	daos_mode = get_daos_obj_mode(O_RDWR);

	rc = daos_obj_open(dfs->coh, (*obj)->oid, daos_mode, &(*obj)->oh, NULL);
	if (rc) {
		printf("daos_obj_open() Failed (%d)\n", rc);
		return rc;
	}
	if(!strcmp(key, "fldr")){
		oid_cp(&parent->shot_oid, (*obj)->oid);
		rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_SHOT_GATHER,
								(char*)&(*obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	} else if (!strcmp(key, "cdp")){
		oid_cp(&parent->cmp_oid, (*obj)->oid);
		rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_CMP_GATHER ,
							(char*)&(*obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);

	} else if (!strcmp(key, "offset")){
		oid_cp(&parent->offset_oid, (*obj)->oid);
		rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_OFFSET_GATHER ,
							(char*)&(*obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
	}
	if(rc){
		printf("ERROR UPDATING SEISMIC ROOT ERR = %d", rc);
		return rc;
	}

//	/*Allocate object pointer */
//	D_ALLOC_PTR(*cmp_obj);
//	if (*cmp_obj == NULL)
//		return ENOMEM;
//	strncpy((*cmp_obj)->name, "cmp_gather", SEIS_MAX_PATH);
//	(*cmp_obj)->name[SEIS_MAX_PATH] = '\0';
//	(*cmp_obj)->sequence_number = 0;
//	(*cmp_obj)->gathers = NULL;
//	(*cmp_obj)->number_of_gathers = 0;
//
//	/** Get new OID for shot object */
//	rc = oid_gen(dfs, cid,false, &(*cmp_obj)->oid);
//	if (rc) {
//		printf("ERROR GENERATING OBJECT ID err= %d \n",rc);
//		return rc;
//	}
//	oid_cp(&parent->cmp_oid, (*cmp_obj)->oid);
//
//	daos_mode = get_daos_obj_mode(O_RDWR);
//
//	rc = daos_obj_open(dfs->coh, (*cmp_obj)->oid, daos_mode, &(*cmp_obj)->oh, NULL);
//	if (rc) {
//		printf("daos_obj_open() Failed (%d)\n", rc);
//		return rc;
//	}
//	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_CMP_GATHER ,
//			(char*)&(*cmp_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
//	if(rc){
//		printf("ERROR UPDATING SEISMIC ROOT ERR = %d", rc);
//		return rc;
//	}
//
//	/*Allocate object pointer */
//	D_ALLOC_PTR(*offset_obj);
//	if (*offset_obj == NULL)
//		return ENOMEM;
//	strncpy((*offset_obj)->name, "offset_gather", SEIS_MAX_PATH);
//	(*offset_obj)->name[SEIS_MAX_PATH] = '\0';
//	(*offset_obj)->sequence_number = 0;
//	(*offset_obj)->gathers = NULL;
//	(*offset_obj)->number_of_gathers = 0;
//
//	/** Get new OID for shot object */
//	rc = oid_gen(dfs, cid,false, &(*offset_obj)->oid);
//	if (rc) {
//		printf("ERROR GENERATING OBJECT ID err= %d \n",rc);
//		return rc;
//	}
//	oid_cp(&parent->offset_oid, (*offset_obj)->oid);
//
//	daos_mode = get_daos_obj_mode(O_RDWR);
//
//	rc = daos_obj_open(dfs->coh, (*offset_obj)->oid, daos_mode, &(*offset_obj)->oh, NULL);
//	if (rc) {
//		printf("daos_obj_open() Failed (%d)\n", rc);
//		return rc;
//	}
//	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_OFFSET_GATHER ,
//									(char*)&(*offset_obj)->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
//	if(rc) {
//		printf("ERROR UPDATING SEISMIC ROOT ERR = %d", rc);
//		return rc;
//	}

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

int new_daos_seis_trh_update(dfs_t* dfs, trace_oid_oh_t* tr_obj, trace_t *tr, int hdrbytes){

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

int daos_seis_tr_data_update(dfs_t* dfs, trace_oid_oh_t* trace_data_obj, segy *trace){

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
//	trace_obj_t *trace_data_obj;
	trace_oid_oh_t *trace_data_obj;

	/*Allocate object pointer */
	D_ALLOC_PTR(*trace_hdr_obj);
	if ((*trace_hdr_obj) == NULL)
		return ENOMEM;

	sprintf(tr_index, "%d",index);
	strcat(trace_hdr_name, tr_index);

	strncpy((*trace_hdr_obj)->name, trace_hdr_name, SEIS_MAX_PATH);
	(*trace_hdr_obj)->name[SEIS_MAX_PATH] = '\0';
	(*trace_hdr_obj)->trace = malloc(sizeof(trace_t));
	memcpy((*trace_hdr_obj)->trace,trace, 240);

	/** Get new OID for trace header object */
	rc = oid_gen(dfs, cid,false, &(*trace_hdr_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID for trace header %d \n", rc);
		return rc;
	}
	(*trace_hdr_obj)->trace->trace_header_obj = (*trace_hdr_obj)->oid;
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

//	strcat(trace_data_name, tr_index);

//	strncpy((trace_data_obj)->name, trace_data_name, SEIS_MAX_PATH);
//	(trace_data_obj)->name[SEIS_MAX_PATH] = '\0';
//	(trace_data_obj)->trace = malloc(sizeof(trace_t));

	/** Get new OID for trace data object */
	rc = oid_gen(dfs, cid,true, &(trace_data_obj)->oid);
	if (rc) {
		printf("ERROR GENERATING OBJECT ID for trace data %d \n", rc);
		return rc;
	}

//	daos_obj_id_t temp_oid;
//	temp_oid = get_tr_data_oid(&(*trace_hdr_obj)->oid,cid);

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
//	free((trace_data_obj)->trace);
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

//void new_prepare_keys(char *dkey_name, char *akey_name, char *key, long unique_value,
//								char *akey_prefix, int nkeys, int *dkey_suffix, int *akey_suffix){
//
//	new_prepare_keys(dkey_name, akey_name,key, unique_value, &new_gather_data.number_of_traces);
//
//	char temp[200]="";
//	strcat(dkey_name,dkey_prefix);
//	if(dkey_suffix != NULL) {
//		sprintf(temp, "%d", dkey_suffix[0]);
//		strcat(dkey_name,temp);
//		if(nkeys==2) {
//			strcat(dkey_name,"_");
//			sprintf(temp, "%d", dkey_suffix[1]);
//			strcat(dkey_name,temp);
//		}
//	}
//	strcat(akey_name, akey_prefix);
//	if(akey_suffix != NULL) {
//		sprintf(temp, "%d", *akey_suffix);
//		strcat(akey_name,temp);
//	}
//}

//int daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, segy *trace,
//									seis_obj_t *shot_obj, seis_obj_t *cmp_obj, seis_obj_t *off_obj){
//
//	int rc = 0;
//	int shot_id = trace->fldr;
//	int s_x = trace->sx;
//	int s_y = trace->sy;
//	int r_x = trace->gx;
//	int r_y = trace->gy;
//	int cmp_x = (s_x + r_x)/2;
//	int cmp_y = (s_y + r_y)/2;
//	int off_x = (r_x - s_x)/2;
//	int off_y = (r_y - s_y)/2;
//	int shot_exists=0;
//	int cmp_exists=0;
//	int offset_exists=0;
//	int ntraces;
//	int keys[2];
//	struct seismic_entry gather_entry = {0};
//	int no_of_traces;
//
//	keys[0]=shot_id;
//	if(check_key_value(keys,shot_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
//		shot_exists=1;
//	}
//
//	keys[0]=cmp_x;
//	keys[1] = cmp_y;
//	if(check_key_value(keys,cmp_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
//			cmp_exists=1;
//	}
//
//	keys[0]=off_x;
//	keys[1] =off_y;
//	if(check_key_value(keys,off_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
//		offset_exists=1;
//	}
//
//	/** if shot id, cmp, and offset doesn't already exist */
//	if(!shot_exists){
//		seis_gather_t shot_gather_data = {0};
//		shot_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
//		char temp[200]="";
//		shot_gather_data.oids[0] = trace_obj->oid;
//		shot_gather_data.number_of_traces = 1;
//		shot_gather_data.nkeys = 1;
//		shot_gather_data.keys[0] = shot_id;
//		char shot_dkey_name[200] = "";
//		char trace_akey_name[200] = "";
//
//		prepare_keys(shot_dkey_name, trace_akey_name, DS_D_SHOT, DS_A_TRACE, 1,
//								shot_gather_data.keys, &shot_gather_data.number_of_traces);
//
//		rc = update_gather_object(shot_obj, shot_dkey_name, DS_A_SHOT_ID, (char*)&shot_id,
//							sizeof(int), DAOS_IOD_SINGLE);
//		if(rc) {
//			printf("ERROR adding shot shot_id key, error: %d", rc);
//			return rc;
//		}
//		add_gather(&(shot_obj->gathers), &shot_gather_data);
//		shot_obj->sequence_number++;
//		shot_obj->number_of_gathers++;
//		free(shot_gather_data.oids);
//	}
//
//	if(!cmp_exists){
//		char cmp_dkey_name[200] = "";
//		char temp[200]="";
//		seis_gather_t cmp_gather_data= {0};
//		cmp_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
//		cmp_gather_data.oids[0] = trace_obj->oid;
//		cmp_gather_data.number_of_traces=1;
//		cmp_gather_data.nkeys=2;
//		cmp_gather_data.keys[0] = cmp_x;
//		cmp_gather_data.keys[1] = cmp_y;
//		char trace_akey_name[200] = "";
//
//		prepare_keys(cmp_dkey_name, trace_akey_name, DS_D_CMP, DS_A_TRACE, 2,
//								cmp_gather_data.keys, &cmp_gather_data.number_of_traces);
//
//		rc = update_gather_object(cmp_obj, cmp_dkey_name, DS_A_CMP_VAL, (char*)cmp_gather_data.keys,
//							sizeof(int)*2, DAOS_IOD_ARRAY);
//		if(rc){
//			printf("ERROR adding cmp value key, error: %d", rc);
//			return rc;
//		}
//		add_gather(&(cmp_obj->gathers), &cmp_gather_data);
//		cmp_obj->sequence_number++;
//		cmp_obj->number_of_gathers++;
//		free(cmp_gather_data.oids);
//	}
//
//	if(!offset_exists){
//		char off_dkey_name[200] = "";
//		char temp[200]="";
//		seis_gather_t off_gather_data= {0};
//		off_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
//		off_gather_data.oids[0] = trace_obj->oid;
//		off_gather_data.number_of_traces = 1;
//		off_gather_data.nkeys = 2;
//		off_gather_data.keys[0] = off_x;
//		off_gather_data.keys[1] = off_y;
//
//		char trace_akey_name[200] = "";
//
//		prepare_keys(off_dkey_name, trace_akey_name, DS_D_OFFSET, DS_A_TRACE, 2,
//								off_gather_data.keys, &off_gather_data.number_of_traces);
//		rc = update_gather_object(off_obj, off_dkey_name, DS_A_OFF_VAL, (char*)off_gather_data.keys,
//							sizeof(int)*2, DAOS_IOD_ARRAY);
//		if(rc){
//			printf("ERROR adding gather value key, error: %d", rc);
//			return rc;
//		}
//		add_gather(&(off_obj->gathers), &off_gather_data);
//		off_obj->sequence_number++;
//		off_obj->number_of_gathers++;
//		free(off_gather_data.oids);
//	}
//	return rc;
//}

int new_daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, seis_obj_t *seis_obj, char *key){

	int rc = 0;
//	long unique_value = get_header_value(*(trace_obj->trace),key);
	Value unique_value;
	get_header_value_new(*(trace_obj->trace),key,&unique_value);
	int key_exists=0;
	int ntraces;
	struct seismic_entry gather_entry = {0};
	int no_of_traces;

	if(new_check_key_value(unique_value, key, seis_obj->gathers, trace_obj->oid, &no_of_traces) == 1) {
		key_exists=1;
	}

/** if shot id, cmp, and offset doesn't already exist */
	if(!key_exists){
		seis_gather_t new_gather_data = {0};
		new_gather_data.oids = malloc(50*sizeof(daos_obj_id_t));
		char temp[200]="";
		new_gather_data.oids[0] = trace_obj->oid;
		new_gather_data.number_of_traces = 1;
//		new_gather_data.nkeys = 1;
		new_gather_data.unique_key = unique_value;
		char dkey_name[200] = "";
		strcat(dkey_name, key);
		strcat(dkey_name,"_");
//		sprintf(temp, "%ld", unique_value);
		val_sprintf(temp, unique_value, key);
		strcat(dkey_name,temp);
		rc = update_gather_object(seis_obj, dkey_name, DS_A_UNIQUE_VAL, (char*)&new_gather_data.unique_key,
							sizeof(long), DAOS_IOD_SINGLE);
		if(rc) {
			printf("ERROR adding Seismic object unique value key , error: %d", rc);
			return rc;
		}

		add_gather(&(seis_obj->gathers), &new_gather_data);
		seis_obj->sequence_number++;
		seis_obj->number_of_gathers++;
		free(new_gather_data.oids);
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

void new_fetch_traces_header(dfs_t *dfs, daos_obj_id_t *oids, traces_list_t **head_traces, int daos_mode, int number_of_traces){

	trace_oid_oh_t *trace_hdr_obj = malloc( number_of_traces * sizeof(trace_oid_oh_t));
	int i;
	int rc;
	struct seismic_entry seismic_entry = {0};
	trace_t *temp = malloc(sizeof(trace_t));
	for (i=0; i < number_of_traces; i++){
		trace_hdr_obj[i].oid = oids[i];
		rc = daos_obj_open(dfs->coh, trace_hdr_obj[i].oid, daos_mode, &trace_hdr_obj[i].oh, NULL);
		if(rc) {
			printf("daos_obj_open()__ trace_header_obj Failed (%d)\n", rc);
			exit(rc);
		}
		//Read Trace header
		prepare_seismic_entry(&seismic_entry, trace_hdr_obj[i].oid, DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
							(char*)(temp), HDRBYTES, DAOS_IOD_ARRAY);

		rc = daos_seis_fetch_entry(trace_hdr_obj[i].oh, DAOS_TX_NONE, &seismic_entry, NULL);
		if (rc) {
			printf("Error reading trace  %d header error = %d \n", i, rc);
			exit(rc);
		}
		daos_obj_close(trace_hdr_obj[i].oh,NULL);
		temp->trace_header_obj = oids[i];
		add_trace_header(temp, head_traces);
	}

	free(temp);
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

void new_fetch_traces_data(dfs_t *dfs, traces_list_t **head_traces, int daos_mode){

//	trace_oid_oh_t *trace_data_obj = malloc( number_of_traces * sizeof(trace_oid_oh_t));

	traces_headers_t *current = (*head_traces)->head;
	if(current == NULL){
		printf("LINKED LIST EMPTY \n");
		return;
	}

	int i;
	int rc;
	daos_array_iod_t iod;
	daos_range_t		rg;
	d_sg_list_t sgl;
//	trace_t temp;
	struct seismic_entry seismic_entry = {0};
	trace_oid_oh_t trace_data_obj;
	while(current!=NULL){
		trace_data_obj.oid = get_tr_data_oid(&(current->trace.trace_header_obj),OC_SX);

		rc = daos_array_open_with_attr(dfs->coh, (trace_data_obj).oid, DAOS_TX_NONE, DAOS_OO_RW,
								1,200*sizeof(float), &(trace_data_obj.oh), NULL);
		if (rc) {
			printf("daos_array_open_with_attr()-->>Trace data object<<-- failed (%d)\n", rc);
			exit(rc);
		}
//		temp.data = malloc((*head_traces)->trace.ns * sizeof(float));
		sgl.sg_nr = 1; // traces->traces[j].ns;
		sgl.sg_nr_out = 0;
		d_iov_t iov;
		current->trace.data = malloc(current->trace.ns * sizeof(float));
		seismic_entry.data = (char*)current->trace.data;
//printf("HELLO HELLO HELLLOOOOOOO \n");
		d_iov_set(&iov, (void*)(seismic_entry.data), current->trace.ns * sizeof(float));

		sgl.sg_iovs = &iov;
		iod.arr_nr = 1;
		rg.rg_len = current->trace.ns * sizeof(float);
		rg.rg_idx = 0;
		iod.arr_rgs = &rg;

		rc = daos_array_read(trace_data_obj.oh, DAOS_TX_NONE, &iod, &sgl, NULL);
		if(rc) {
			printf("ERROR READING TRACE DATA KEY----------------- error = %d  \n", rc);
			exit(rc);
		}
		daos_array_close(trace_data_obj.oh,NULL);
//		add_trace_data(temp,head_traces);
		current = current->next_trace;
	}
	free(current);
//	free(trace_data_obj);
}

void sort_dkeys_list(long *first_array, int number_of_gathers, char** unique_keys, int direction){

    const char *sep = "_";
    char *token;
    int i;
    char new_temp[4096];
    int *positive = malloc(number_of_gathers * sizeof(int));
    char **sorted_keys = malloc(number_of_gathers * sizeof(char *));
    int j=0;
//    for(int k=0 ; k<number_of_gathers; k++){
//    	printf("KEY KEY KEY %s \n", unique_keys[k]);
//    }
    for(i=0;i< number_of_gathers;i++){
        strcpy(new_temp, unique_keys[j]);
        token = strtok(new_temp, sep);
//        printf("TOKEN IS %s \n", token);
//      	sorted_keys[j] = malloc((strlen(token) + 1) * sizeof(char));
        while( token != NULL ) {
        	token = strtok(NULL, sep);
        	if(token == NULL){
        		break;
        	}
          	sorted_keys[j] = malloc((strlen(token) + 1) * sizeof(char));
        	if(token[0]== '-'){
        		positive[j]= 0;
	            strcpy(sorted_keys[j], &token[1]);
        	}else{
				positive[j]=1;
	            strcpy(sorted_keys[j], token);
			}
        	first_array[j] = atol(sorted_keys[j]);
        }
        j++;
    }

    long temp2;
    if(direction==1){
        for (i = 0; i < number_of_gathers; i++) {
                for (j = 0; j < number_of_gathers - i - 1; j++) {
                    if (first_array[j] > first_array[j + 1]) {
                        temp2 = first_array[j];
                        first_array[j] = first_array[j + 1];
                        first_array[j + 1] = temp2;
                    }
                }
            }
    } else {
        for (i = 0; i < number_of_gathers ; ++i) {
            for (j = i + 1; j < number_of_gathers; ++j) {
                if (first_array[i] < first_array[j]) {
                	temp2 = first_array[i];
                    first_array[i] = first_array[j];
                    first_array[j] = temp2;
                }
            }
        }

    }
 }

void sort_headers(read_traces *gather_traces, char **sort_key, int *direction, int number_of_keys){
	int i;
	int j;
	trace_t temp;
//	if(strcmp(sort_key,"cdp")==0){
//		for(i=0; i< gather_traces->number_of_traces;i++){
//			gather_traces->traces[i].cdp =(gather_traces->traces[i].sx + gather_traces->traces[i].gx)/2;
//		}
//	} else if(strcmp(sort_key,"offset")==0){
//		for(i=0; i< gather_traces->number_of_traces;i++){
//			gather_traces->traces[i].offset =(gather_traces->traces[i].sx - gather_traces->traces[i].gx)/2;
//		}
//	}
//
//	printf("BEFORE SORTING BASED ON SX ============================================= \n");
//	for(i=0; i<gather_traces->number_of_traces; i++){
//		printf("SX OF TRACE %d issssss %d \n", i, gather_traces->traces[i].gx);
//	}


//	printf("NUMBER OF KEYS ==== %d \n", number_of_keys);
//	for(int i =1; i <= number_of_keys; i++){
//		printf("KEY IS ============== %s \n", sort_key[i]);
//	}
	MergeSort(gather_traces->traces, 0, gather_traces->number_of_traces-1, sort_key, direction, number_of_keys);

//	for (i = 1; i < gather_traces->number_of_traces; i++){
//		for (j = 0; j < gather_traces->number_of_traces - i; j++) {
//			if (gather_traces->traces[j].cdp > gather_traces->traces[j+1]	.cdp) {
//				temp = gather_traces->traces[j];
//				gather_traces->traces[j] = gather_traces->traces[j+1];
//				gather_traces->traces[j+1] = temp;
//			 }
//		}
//	}
//	printf("AfTER SORTING BASED ON SX ============================================= \n");
//	for(i=0; i<gather_traces->number_of_traces; i++){
//		printf("SX OF TRACE %d issssss %d \n", i, gather_traces->traces[i].gx);
//	}
}

void Merge(trace_t *arr, int low, int mid, int high, char **sort_key, int *direction, int number_of_keys)
{
    int mergedSize = high - low + 1;
    trace_t *temp = (trace_t *)malloc(mergedSize * sizeof(trace_t));
    int mergePos = 0;
    int leftPos = low;
    int rightPos = mid + 1;
    int z=1;
    Value val1;
    Value val2;
//    printf("SORTKEY IS %s direction is %d \n", sort_key[z], direction[z]);
    while (leftPos <= mid && rightPos <= high)
    {
    	while(z <= number_of_keys){
    		get_header_value_new(arr[leftPos],sort_key[z],&val1);
    		get_header_value_new(arr[rightPos],sort_key[z],&val2);

    		if(valcmp(hdtype(sort_key[z]),val1,val2) == -1) {
				if(direction[z] == 1){
//					printf("ONE \n");
					temp[mergePos++] = arr[leftPos++];
				} else {
//					printf("TWO \n");
					temp[mergePos++] = arr[rightPos++];
				}
				break;
			} else if(valcmp(hdtype(sort_key[z]),val1,val2) == 1) {
				if(direction[z] == 1){
//					printf("THREE \n");
					temp[mergePos++] = arr[rightPos++];
				} else {
//					printf("FOUR \n");
					temp[mergePos++] = arr[leftPos++];
				}
				break;
			} else {
				z++;
			}
    	}

//        if (check_sorting_key(arr[leftPos], arr[rightPos], sort_key)){
//        	if(direction == 1){
////            	printf("ONE \n");
//                temp[mergePos++] = arr[leftPos++];
//        	} else{
////            	printf("THREE \n");
//                temp[mergePos++] = arr[rightPos++];
//        	}
//        } else {
//        	if(direction == 1) {
////            	printf("two \n");
//                temp[mergePos++] = arr[rightPos++];
//        	} else {
////            	printf("FOUR \n");
//                temp[mergePos++] = arr[leftPos++];
//        	}
//        }
    }

    while (leftPos <= mid)
    {
        temp[mergePos++] = arr[leftPos++];
    }

    while (rightPos <= high)
    {
        temp[mergePos++] = arr[rightPos++];
    }

//    assert(mergePos == mergedSize);

    for (mergePos = 0; mergePos < mergedSize; ++mergePos){
        arr[low + mergePos] = temp[mergePos];
    }

    free(temp);
}

void MergeSort(trace_t *arr, int low, int high, char **sort_key, int *direction, int number_of_keys)
{
    if (low < high)
    {
        int mid = (low + high) / 2;

        MergeSort(arr, low, mid, sort_key, direction, number_of_keys);
        MergeSort(arr, mid + 1, high, sort_key, direction, number_of_keys);

        Merge(arr, low, mid, high, sort_key, direction, number_of_keys);
    }
}

long get_header_value(trace_t trace, char *sort_key){

	if(strcmp(sort_key, "tracl") == 0){
		return trace.tracl;
	} else if(strcmp(sort_key,"tracr")==0){
		return trace.tracr;
	} else if(strcmp(sort_key,"fldr")==0){
		return trace.fldr;
	} else if(strcmp(sort_key,"tracf")==0){
		return trace.tracf;
	} else if(strcmp(sort_key,"ep")==0){
		return trace.ep;
	} else if(strcmp(sort_key,"cdp")==0){
		return trace.cdp;
	} else if(strcmp(sort_key,"cdpt")==0){
		return trace.cdpt;
	} else if(strcmp(sort_key,"nvs")==0){
		return trace.nvs;
	} else if(strcmp(sort_key,"nhs")==0){
		return trace.nhs;
	} else if(strcmp(sort_key,"offset")==0){
		return trace.offset;
	} else if(strcmp(sort_key,"gelev")==0){
		return trace.gelev;
	} else if(strcmp(sort_key,"selev")==0){
		return trace.selev;
	} else if(strcmp(sort_key,"sdepth")==0){
		return trace.sdepth;
	} else if(strcmp(sort_key,"gdel")==0){
		return trace.gdel;
	} else if(strcmp(sort_key,"sdel")==0){
		return trace.sdel;
	} else if(strcmp(sort_key,"swdep")==0){
		return trace.swdep;
	} else if(strcmp(sort_key,"gwdep")==0){
		return trace.gwdep;
	} else if(strcmp(sort_key,"scalel")==0){
		return trace.scalel;
	} else if(strcmp(sort_key,"scalco")==0){
		return trace.scalco;
	} else if(strcmp(sort_key,"sx")==0){
		return trace.sx;
	} else if(strcmp(sort_key,"sy")==0){
		return trace.sy;
	} else if(strcmp(sort_key,"gx")==0){
		return trace.gx;
	} else if(strcmp(sort_key,"gy")==0){
		return trace.gy;
	} else if(strcmp(sort_key,"wevel")==0){
		return trace.wevel;
	} else if(strcmp(sort_key,"swevel")==0){
		return trace.swevel;
	} else if(strcmp(sort_key,"sut")==0){
		return trace.sut;
	} else if(strcmp(sort_key,"gut")==0){
		return trace.gut;
	} else if(strcmp(sort_key,"sstat")==0){
		return trace.sstat;
	} else if(strcmp(sort_key,"gstat")==0){
		return trace.gstat;
	} else if(strcmp(sort_key,"tstat")==0){
		return trace.tstat;
	} else if(strcmp(sort_key,"laga")==0){
		return trace.laga;
	} else if(strcmp(sort_key,"lagb")==0){
		return trace.lagb;
	} else if(strcmp(sort_key,"delrt")==0){
		return trace.delrt;
	} else if(strcmp(sort_key,"muts")==0){
		return trace.muts;
	} else if(strcmp(sort_key,"mute")==0){
		return trace.mute;
	} else if(strcmp(sort_key,"ns")==0){
		return trace.ns;
	} else if(strcmp(sort_key,"dt")==0){
		return trace.dt;
	} else if(strcmp(sort_key,"gain")==0){
		return trace.gain;
	} else if(strcmp(sort_key,"igc")==0){
		return trace.igc;
	} else if(strcmp(sort_key,"igi")==0){
		return trace.igi;
	} else if(strcmp(sort_key,"corr")==0){
		return trace.corr;
	} else if(strcmp(sort_key,"sfs")==0){
		return trace.sfs;
	} else if(strcmp(sort_key,"sfe")==0){
		return trace.sfe;
	} else if(strcmp(sort_key,"slen")==0){
		return trace.slen;
	} else if(strcmp(sort_key,"styp")==0){
		return trace.styp;
	} else if(strcmp(sort_key,"stas")==0){
		return trace.stas;
	} else if(strcmp(sort_key,"stae")==0){
		return trace.stae;
	} else if(strcmp(sort_key,"tatyp")==0){
		return trace.tatyp;
	} else if(strcmp(sort_key,"afilf")==0){
		return trace.afilf;
	} else if(strcmp(sort_key,"afils")==0){
		return trace.afils;
	} else if(strcmp(sort_key,"nofilf")==0){
		return trace.nofilf;
	} else if(strcmp(sort_key,"nofils")==0){
		return trace.nofils;
	} else if(strcmp(sort_key,"lcf")==0){
		return trace.lcf;
	} else if(strcmp(sort_key,"hcf")==0){
		return trace.hcf;
	} else if(strcmp(sort_key,"lcs")==0){
		return trace.lcs;
	} else if(strcmp(sort_key,"hcs")==0){
		return trace.hcs;
	} else if(strcmp(sort_key,"grnors")==0){
		return trace.grnors;
	} else if(strcmp(sort_key,"grnofr")==0){
		return trace.grnofr;
	} else if(strcmp(sort_key,"grnlof")==0){
		return trace.grnlof;
	} else if(strcmp(sort_key,"gaps")==0){
		return trace.gaps;
	} else if(strcmp(sort_key,"d1")==0){
		return trace.d1;
	} else if(strcmp(sort_key,"f1")==0){
		return trace.f1;
	} else if(strcmp(sort_key,"d2")==0){
		return trace.d2;
	} else if(strcmp(sort_key,"f2")==0){
		return trace.f2;
	} else if(strcmp(sort_key,"sfs")==0){
		return trace.sfs;
	} else if(strcmp(sort_key,"ntr")==0){
		return trace.ntr;
	} else {
		return -1;
	}
}

void get_header_value_new(trace_t trace, char *sort_key, Value *value){

	if(!strcmp(sort_key, "tracl")){
		value->i = trace.tracl;
	} else if(!strcmp(sort_key,"tracr")){
		value->i = trace.tracr;
	} else if(!strcmp(sort_key,"fldr")){
		value->i = trace.fldr;
	} else if(!strcmp(sort_key,"tracf")){
		value->i = trace.tracf;
	} else if(!strcmp(sort_key,"ep")){
		value->i = trace.ep;
	} else if(!strcmp(sort_key,"cdp")){
		value->i = trace.cdp;
	}else if(!strcmp(sort_key,"ns")){
		value->u = trace.ns;
	} else if(!strcmp(sort_key,"gx")){
		value->i = trace.gx;
	} else if(!strcmp(sort_key,"sx")){
		value->i = trace.sx;
	} else if(!strcmp(sort_key,"offset")){
		value->i = trace.offset;
	} else if(!strcmp(sort_key,"dt")){
		value->u = trace.dt;
	} else{
		return;
	}

}

void set_header_value(trace_t *trace, char *sort_key, Value *value){

	if(!strcmp(sort_key, "tracl")){
		trace->tracl = value->i;
//		printf("TRACL    %d \n ", value.i);
	} else if(!strcmp(sort_key,"tracr")){
		trace->tracr = value->i;
	} else if(!strcmp(sort_key,"fldr")){
		trace->fldr = value->i;
	} else if(!strcmp(sort_key,"tracf")){
		trace->tracf = value->i;
	} else if(!strcmp(sort_key,"ep")){
		trace->ep = value->i;
	} else if(!strcmp(sort_key,"cdp")){
//		printf("CDP VALUES IS %d \n", value->i);
		trace->cdp = value->i;
	}else if(!strcmp(sort_key,"ns")){
		trace->ns = value->u;
	} else if(!strcmp(sort_key,"gx")){
//		printf("GX VALUES IS %d \n", value->i);
		trace->gx = value->i;
	} else if(!strcmp(sort_key,"dt")){
		trace->dt = value->u;
	}else{
		return;
	}
//	} else if(strcmp(sort_key,"cdpt")==0){
//		trace.cdpt = value;
//	} else if(strcmp(sort_key,"nvs")==0){
//		trace.nvs = value;
//	} else if(strcmp(sort_key,"nhs")==0){
//		trace.nhs = value;
//	} else if(strcmp(sort_key,"offset")==0){
//		trace.offset = value;
//	} else if(strcmp(sort_key,"gelev")==0){
//		trace.gelev = value;
//	} else if(strcmp(sort_key,"selev")==0){
//		trace.selev = value;
//	} else if(strcmp(sort_key,"sdepth")==0){
//		trace.sdepth = value;
//	} else if(strcmp(sort_key,"gdel")==0){
//		trace.gdel = value;
//	} else if(strcmp(sort_key,"sdel")==0){
//		trace.sdel = value;
//	} else if(strcmp(sort_key,"swdep")==0){
//		trace.swdep = value;
//	} else if(strcmp(sort_key,"gwdep")==0){
//		trace.gwdep = value;
//	} else if(strcmp(sort_key,"scalel")==0){
//		trace.scalel = value;
//	} else if(strcmp(sort_key,"scalco")==0){
//		trace.scalco = value;
//	} else if(strcmp(sort_key,"sx")==0){
//		trace.sx = value;
//	} else if(strcmp(sort_key,"sy")==0){
//		trace.sy = value;
//	} else if(strcmp(sort_key,"gx")==0){
//		trace.gx = value;
//	} else if(strcmp(sort_key,"gy")==0){
//		trace.gy = value;
//	} else if(strcmp(sort_key,"wevel")==0){
//		trace.wevel = value;
//	} else if(strcmp(sort_key,"swevel")==0){
//		trace.swevel = value;
//	} else if(strcmp(sort_key,"sut")==0){
//		trace.sut = value;
//	} else if(strcmp(sort_key,"gut")==0){
//		trace.gut = value;
//	} else if(strcmp(sort_key,"sstat")==0){
//		trace.sstat = value;
//	} else if(strcmp(sort_key,"gstat")==0){
//		trace.gstat = value;
//	} else if(strcmp(sort_key,"tstat")==0){
//		trace.tstat = value;
//	} else if(strcmp(sort_key,"laga")==0){
//		trace.laga = value;
//	} else if(strcmp(sort_key,"lagb")==0){
//		trace.lagb = value;
//	} else if(strcmp(sort_key,"delrt")==0){
//		trace.delrt = value;
//	} else if(strcmp(sort_key,"muts")==0){
//		trace.muts = value;
//	} else if(strcmp(sort_key,"mute")==0){
//		trace.mute = value;
//	} else if(strcmp(sort_key,"ns")==0){
//		trace.ns = value;
//	} else if(strcmp(sort_key,"dt")==0){
//		trace.dt = value;
//	} else if(strcmp(sort_key,"gain")==0){
//		trace.gain = value;
//	} else if(strcmp(sort_key,"igc")==0){
//		trace.igc = value;
//	} else if(strcmp(sort_key,"igi")==0){
//		trace.igi = value;
//	} else if(strcmp(sort_key,"corr")==0){
//		trace.corr = value;
//	} else if(strcmp(sort_key,"sfs")==0){
//		trace.sfs = value;
//	} else if(strcmp(sort_key,"sfe")==0){
//		trace.sfe = value;
//	} else if(strcmp(sort_key,"slen")==0){
//		trace.slen = value;
//	} else if(strcmp(sort_key,"styp")==0){
//		trace.styp = value;
//	} else if(strcmp(sort_key,"stas")==0){
//		trace.stas = value;
//	} else if(strcmp(sort_key,"stae")==0){
//		trace.stae = value;
//	} else if(strcmp(sort_key,"tatyp")==0){
//		trace.tatyp = value;
//	} else if(strcmp(sort_key,"afilf")==0){
//		trace.afilf = value;
//	} else if(strcmp(sort_key,"afils")==0){
//		trace.afils = value;
//	} else if(strcmp(sort_key,"nofilf")==0){
//		trace.nofilf = value;
//	} else if(strcmp(sort_key,"nofils")==0){
//		trace.nofils = value;
//	} else if(strcmp(sort_key,"lcf")==0){
//		trace.lcf = value;
//	} else if(strcmp(sort_key,"hcf")==0){
//		trace.hcf = value;
//	} else if(strcmp(sort_key,"lcs")==0){
//		trace.lcs = value;
//	} else if(strcmp(sort_key,"hcs")==0){
//		trace.hcs = value;
//	} else if(strcmp(sort_key,"grnors")==0){
//		trace.grnors = value;
//	} else if(strcmp(sort_key,"grnofr")==0){
//		trace.grnofr = value;
//	} else if(strcmp(sort_key,"grnlof")==0){
//		trace.grnlof = value;
//	} else if(strcmp(sort_key,"gaps")==0){
//		trace.gaps = value;
//	} else if(strcmp(sort_key,"d1")==0){
//		trace.d1 = value;
//	} else if(strcmp(sort_key,"f1")==0){
//		trace.f1 = value;
//	} else if(strcmp(sort_key,"d2")==0){
//		trace.d2 = value;
//	} else if(strcmp(sort_key,"f2")==0){
//		trace.f2 = value;
//	} else if(strcmp(sort_key,"sfs")==0){
//		trace.sfs = value;
//	} else if(strcmp(sort_key,"ntr")==0){
//		trace.ntr = value;
//	} else {
//		return;
//	}
}

char* get_dkey(char *key){

	if(strcmp(key, "tracl") == 0){
		return "Tracl_";
	} else if(strcmp(key,"tracr")==0){
		return "Tracr_";
	} else if(strcmp(key,"fldr")==0){
		return "Shot_";
	} else if(strcmp(key,"tracf")==0){
		return "Tracf_";
	} else if(strcmp(key,"ep")==0){
		return "Ep_";
	} else if(strcmp(key,"cdp")==0){
		return "Cdp_";
	} else if(strcmp(key,"cdpt")==0){
		return "Cdpt_";
	} else if(strcmp(key,"nvs")==0){
		return "Nvs_";
	} else if(strcmp(key,"nhs")==0){
		return "Nhs_";
	} else if(strcmp(key,"offset")==0){
		return "Offset_";
	} else if(strcmp(key,"gelev")==0){
		return "Gelev_";
	} else if(strcmp(key,"selev")==0){
		return "Selev_";
	} else if(strcmp(key,"sdepth")==0){
		return "Sdepth_";
	} else if(strcmp(key,"gdel")==0){
		return "Gdel_";
	} else if(strcmp(key,"sdel")==0){
		return "Sdel_";
	} else if(strcmp(key,"swdep")==0){
		return "Swdep_";
	} else if(strcmp(key,"gwdep")==0){
		return "Gwdep_";
	} else if(strcmp(key,"scalel")==0){
		return "Scalel_";
	} else if(strcmp(key,"scalco")==0){
		return "Scalco_";
	} else if(strcmp(key,"sx")==0){
		return "Sx_";
	} else if(strcmp(key,"sy")==0){
		return "Sy_";
	} else if(strcmp(key,"gx")==0){
		return "Gx_";
	} else if(strcmp(key,"gy")==0){
		return "Gy_";
	} else if(strcmp(key,"wevel")==0){
		return "Wevel_";
	} else if(strcmp(key,"swevel")==0){
		return "Swevel_";
	} else if(strcmp(key,"sut")==0){
		return "Sut_";
	} else if(strcmp(key,"gut")==0){
		return "Gut_";
	} else if(strcmp(key,"sstat")==0){
		return "Sstat_";
	} else if(strcmp(key,"gstat")==0){
		return "Gstat_";
	} else if(strcmp(key,"tstat")==0){
		return "Tstat_";
	} else if(strcmp(key,"laga")==0){
		return "Laga_";
	} else if(strcmp(key,"lagb")==0){
		return "Lagb_";
	} else if(strcmp(key,"delrt")==0){
		return "Delrt_";
	} else if(strcmp(key,"muts")==0){
		return "Muts_";
	} else if(strcmp(key,"mute")==0){
		return "Mute_";
	} else if(strcmp(key,"ns")==0){
		return "Ns_";
	} else if(strcmp(key,"dt")==0){
		return "Dt_";
	} else if(strcmp(key,"gain")==0){
		return "Gain_";
	} else if(strcmp(key,"igc")==0){
		return "Igc_";
	} else if(strcmp(key,"igi")==0){
		return "Igi_";
	} else if(strcmp(key,"corr")==0){
		return "Corr_";
	} else if(strcmp(key,"sfs")==0){
		return "Sfs_";
	} else if(strcmp(key,"sfe")==0){
		return "Sfe_";
	} else if(strcmp(key,"slen")==0){
		return "Slen_";
	} else if(strcmp(key,"styp")==0){
		return "Styp_";
	} else if(strcmp(key,"stas")==0){
		return "Stas_";
	} else if(strcmp(key,"stae")==0){
		return "Stae_";
	} else if(strcmp(key,"tatyp")==0){
		return "Tatyp_";
	} else if(strcmp(key,"afilf")==0){
		return "Afilf_";
	} else if(strcmp(key,"afils")==0){
		return "Afils_";
	} else if(strcmp(key,"nofilf")==0){
		return "Nofilf_";
	} else if(strcmp(key,"nofils")==0){
		return "Nofils_";
	} else if(strcmp(key,"lcf")==0){
		return "Lcf_";
	} else if(strcmp(key,"hcf")==0){
		return "Hcf_";
	} else if(strcmp(key,"lcs")==0){
		return "Lcs_";
	} else if(strcmp(key,"hcs")==0){
		return "Hcs_";
	} else if(strcmp(key,"grnors")==0){
		return "Grnors_";
	} else if(strcmp(key,"grnofr")==0){
		return "Grnofr_";
	} else if(strcmp(key,"grnlof")==0){
		return "Grnlof_";
	} else if(strcmp(key,"gaps")==0){
		return "Gaps_";
	} else if(strcmp(key,"d1")==0){
		return "D1_";
	} else if(strcmp(key,"f1")==0){
		return "F1_";
	} else if(strcmp(key,"d2")==0){
		return "D2_";
	} else if(strcmp(key,"f2")==0){
		return "F2_";
	} else if(strcmp(key,"sfs")==0){
		return "Sfs_";
	} else if(strcmp(key,"ntr")==0){
		return "Ntr_";
	} else {
		return -1;
	}
}

void window_headers(read_traces *window_traces, read_traces *gather_traces, daos_obj_id_t *oids, char *key, long min, long max){

	int i;
	int k =0;
	int temp = gather_traces->number_of_traces;
	window_traces->number_of_traces = 0;
	daos_obj_id_t *temp_oids = malloc(gather_traces->number_of_traces * sizeof(daos_obj_id_t));
	memcpy(temp_oids, oids,gather_traces->number_of_traces * sizeof(daos_obj_id_t));

	memset(oids,0,gather_traces->number_of_traces * sizeof(daos_obj_id_t));
	printf("number of traces before = %d \n", window_traces->number_of_traces);

	long value;
	for(i=0; i < temp ;i++){
//		printf("i = %d \n", i);
		value = get_header_value(gather_traces->traces[i], key);
		if(value >= min && value <= max) {
			memcpy(&window_traces->traces[k], &gather_traces->traces[i], sizeof(trace_t));
			memcpy(&oids[k], &temp_oids[i],sizeof(daos_obj_id_t));
			window_traces->number_of_traces ++;
			k++;
		}
//		if(check_windowing_key(gather_traces->traces[i], key, min, max) == 0){
////			for(k=i; k < gather_traces->number_of_traces ; k++){
////				memcpy(&oids[k],&oids[k+1], sizeof(daos_obj_id_t));
////				memcpy(&gather_traces->traces[k], &gather_traces->traces[k+1], sizeof(trace_t));
////			}
////			gather_traces->number_of_traces--;
//		} else {
//			memcpy(&window_traces->traces[k], &gather_traces->traces[i], sizeof(trace_t));
//			memcpy(&oids[k], &temp_oids[i],sizeof(daos_obj_id_t));
//			window_traces->number_of_traces ++;
//			k++;
//		}
//		printf("number of traces in loop = %d \n", gather_traces->number_of_traces);
	}

	printf("number of traces after = %d \n", window_traces->number_of_traces);

}

void set_traces_header(dfs_t *dfs, int daos_mode, traces_list_t **head, int num_of_keys, char **keys_1, char **keys_2, char **keys_3, double *a, double *b, double *c,
					double *d, double *e, double *f, double *j, header_type_t type){
	int rc;
	printf("===================KEY VALUES IN SET TRACES HEADER FUNCTION================ \n");
	for(int k=0; k< num_of_keys; k++){
		printf("===================(%d) KEY VALUES================ \n", k);
		printf("KEY 1 IS %s \n", keys_1[k]);
		if(keys_2 != NULL && keys_3 != NULL){
			printf("KEY 2 IS %s \n", keys_2[k]);
			printf("KEY 3 IS %s \n", keys_3[k]);
		}
		printf("A IS %lf \n", a[k]);
		printf("B IS %lf \n", b[k]);
		printf("C IS %lf \n", c[k]);
		printf("D IS %lf \n", d[k]);
		if(j != NULL){
			printf("J IS %lf \n", j[k]);
		}
		if(e != NULL && f != NULL){
			printf("E IS %lf \n", e[k]);
			printf("F IS %lf \n", f[k]);
		}
	}

	traces_headers_t *current = (*head)->head;
	int i;
	int itr;
	trace_oid_oh_t *trace_hdr_obj = malloc( (*head)->size * sizeof(trace_oid_oh_t));
	cwp_String header_data_type_key_1[num_of_keys]; /* array of keywords			*/
	cwp_String header_data_type_key_2[num_of_keys]; /* array of keywords			*/
	cwp_String header_data_type_key_3[num_of_keys]; /* array of keywords			*/
	if(current == NULL) {
			printf("NO traces exist in linked list \n");
			return;
	} else {
		itr=0;
	while(current != NULL){
		trace_hdr_obj[itr].oid = current->trace.trace_header_obj;
		rc = daos_obj_open(dfs->coh, trace_hdr_obj[itr].oid, daos_mode, &(trace_hdr_obj[itr].oh), NULL);
		if(rc) {
			printf("daos_obj_open()__ trace_header_obj Failed (%d)\n", rc);
			exit(rc);
		}

		for(i=0; i<num_of_keys; i++){
			header_data_type_key_1[i]=hdtype(keys_1[i]);
			if(type==0){
				calculate_new_header_value(current, keys_1[i], NULL, NULL, a[i], b[i], c[i], d[i],0, 0, j[i], itr,
										type, header_data_type_key_1[i], NULL, NULL);
			} else {
				header_data_type_key_2[i]=hdtype(keys_2[i]);
				header_data_type_key_3[i]=hdtype(keys_3[i]);
				calculate_new_header_value(current, keys_1[i], keys_2[i], keys_3[i], a[i], b[i], c[i], d[i], e[i], f[i], 0, 0,
										type, header_data_type_key_1[i], header_data_type_key_2[i], header_data_type_key_3[i]);
			}
		}
		rc = new_daos_seis_trh_update(dfs, &trace_hdr_obj[itr], &(current->trace), HDRBYTES);
		if(rc){
			printf("FAILED TO UPDATE TrACE %d \n" ,rc);
		}
		rc = daos_obj_close(trace_hdr_obj[itr].oh, NULL);
		itr++;
		current = current->next_trace;
	}
		}
}

void calculate_new_header_value(traces_headers_t *current, char *key1, char *key2, char *key3, double a, double b,
							double c, double d,double e, double f, double j, int itr, header_type_t type,
							cwp_String header_data_type_key1, cwp_String header_data_type_key2, cwp_String header_data_type_key3){
	double i;
	long temp;
	Value val1;		/* value of key field 			*/
	Value val2;		/* value of key field 			*/
	Value val3;		/* value of key field 			*/
	switch(type){
		case set_header:
			i = (double) itr + d;
			setval(header_data_type_key1, &val1, a, b, c, i, j);
			set_header_value(&(current->trace), key1, &val1);
			break;
		case change_header:
			get_header_value_new(current->trace, key2, &val2);
			get_header_value_new(current->trace, key3, &val3);
			changeval(header_data_type_key1, &val1, header_data_type_key2, &val2,
					header_data_type_key3, &val3, a, b, c, d, e, f);
			set_header_value(&(current->trace), key1, &val1);
			break;
		default:
			break;
	}

}

void new_window_headers(traces_list_t **head, char *keys, char *min, char *max){

	char temp[4096];
	char min_temp[4096];
	char max_temp[4096];
	int number_of_keys = 0;
	strcpy(temp, keys);
	const char *sep = ",";
	char *token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}
	printf("NUMBER OF KEYS === %d \n",number_of_keys);
	char **window_keys = malloc(number_of_keys * sizeof(char*));
//	long min_keys[number_of_keys];
	Value min_keys[number_of_keys];
	Value max_keys[number_of_keys];
	cwp_String type[number_of_keys];

//	long max_keys[number_of_keys];

	int i=0;
	strcpy(temp,keys);
	strcpy(min_temp,min);
	strcpy(max_temp,max);
	token =strtok(temp,sep);
	while(token != NULL){
		window_keys[i]= malloc((strlen(token) + 1)*sizeof(char));
		strcpy(window_keys[i], token);
		type[i] = hdtype(window_keys[i]);
		token = strtok(NULL,sep);
		i++;
	}
	char *min_token =strtok(min_temp, sep);
	i=0;
	while(min_token != NULL){
		atoval(type[i], min_token, &min_keys[i]);
//		min_keys[i]= atol(min_token);
		min_token = strtok(NULL,sep);
		i++;
	}
	char *max_token = strtok(max_temp, sep);
	i=0;
	while(max_token != NULL){
		atoval(type[i], max_token, &max_keys[i]);
//		max_keys[i]= atol(max_token);
		max_token = strtok(NULL,sep);
		i++;
	}


//	for(int k=0; k<number_of_keys; k++) {
//		printf("KEY is = %s \n", window_keys[k]);
//		printf("MIN is = %ld \n", min_keys[k]);
//		printf("MAX is = %ld \n", max_keys[k]);
//	}

//	long values[number_of_keys];
	traces_headers_t *current = (*head)->head;
	traces_headers_t *previous = NULL;
	if(current == NULL) {
		printf("NO traces exist in linked list \n");
		return;
	}

	int l;
	int break_loop;
	Value val;
	while(current != NULL) {
		break_loop = 0;
		for(l = 0; l<number_of_keys && !break_loop; l++){
			get_header_value_new(current->trace, window_keys[l], &val);
			if(!(valcmp(type[l], val, min_keys[l]) == 1 || valcmp(type[l], val, min_keys[l]) == 0) ||
					!(valcmp(type[l], val, max_keys[l]) == -1 || valcmp(type[l], val, max_keys[l]) == 0)) {
					if(current == (*head)->head){
						(*head)->head= (*head)->head->next_trace;
						free(current);
						current = (*head)->head;
					} else{
						previous->next_trace = current->next_trace;
						free(current);
						current = previous->next_trace;
					}
					break_loop=1;
				}
		}

		if(break_loop){
			continue;
		} else {
			previous = current;
			current = current->next_trace;
		}
	}

//	for(l=0; l<number_of_keys; l++){
//		free(window_keys[l]);
//	}
//	free(window_keys);
}

char ** daos_seis_fetch_dkeys(seis_obj_t *seismic_object, int sort, int shot_obj,
																int cmp_obj, int off_obj, int direction){
	uint32_t nr = seismic_object->number_of_gathers +1;
	d_sg_list_t sglo;
	sglo.sg_nr_out = sglo.sg_nr = 1;
	d_iov_t iov_temp;
	char *temp_array = malloc(seismic_object->number_of_gathers * 11 * sizeof(char));
	d_iov_set(&iov_temp, temp_array, seismic_object->number_of_gathers * 11);
	sglo.sg_iovs = &iov_temp;
	daos_anchor_t	anchor = { 0 };
	int rc;
//printf("BEFORE FETCH %d \n", nr);
	daos_key_desc_t  *kds= malloc((seismic_object->number_of_gathers + 1) * sizeof(daos_key_desc_t));
	rc = daos_obj_list_dkey(seismic_object->oh, DAOS_TX_NONE, &nr, kds, &sglo, &anchor, NULL);
	if(rc){
		printf(" LIST DKEY FAILED \n");
	}
	char **dkeys_list = malloc((seismic_object->number_of_gathers +1) * sizeof(char*));
	int off=0;
	int out;
	char **unique_keys = malloc(seismic_object->number_of_gathers * sizeof(char *));
	int u=0;
	int z;
	for( z=0; z< seismic_object->number_of_gathers +1; z++ ){
    	dkeys_list[z] = malloc(kds[z].kd_key_len +1 * sizeof(char));
		strncpy(dkeys_list[z],&temp_array[off], kds[z].kd_key_len);
		dkeys_list[z][kds[z].kd_key_len] = '\0';

//		printf("TMP KEY >> %s\n",dkeys_list[z]);
		off += kds[z].kd_key_len;
		for (int k=0; k< strlen(dkeys_list[z])+1;k++){
			if(isdigit(dkeys_list[z][k])){
				unique_keys[u]=malloc(kds[z].kd_key_len * sizeof(char));
				unique_keys[u]= dkeys_list[z];
				u++;
				break;
			} else {
				out = z;
				continue;
			}
		}
	}

//	printf("AFTER FETCH %d \n", nr);

//	for(z=0; z< seismic_object->number_of_gathers +1; z++){
//		free(dkeys_list[z]);
//	}
//	free(dkeys_list);

	if(sort && (shot_obj || cmp_obj || off_obj)){
		   long *first_array = malloc(seismic_object->number_of_gathers * sizeof(long));
		   sort_dkeys_list(first_array, seismic_object->number_of_gathers, unique_keys,direction);

		   char **dkeys_sorted_list = malloc((seismic_object->number_of_gathers) * sizeof(char*));
		   int m;
		   z=0;

			for(m=0; m< seismic_object->number_of_gathers; m++){
				if(z == out){
					z++;
					continue;
				}
				dkeys_sorted_list[m]=malloc(kds[z].kd_key_len *sizeof(char));
				char dkey_name[200] = "";
				char temp_st[200]="";
				if(shot_obj){
					strcat(dkey_name,"fldr_");
				} else if (cmp_obj){
					strcat(dkey_name,"cdp_");
				} else if(off_obj){
					strcat(dkey_name,"offset_");
				} else{
					strcat(dkey_name,"fldr_");
				}
	//				seismic_object->gathers[m].unique_key = first_array[m];
				sprintf(temp_st, "%ld", first_array[m]);
				strcat(dkey_name,temp_st);
				strcpy(dkeys_sorted_list[m], dkey_name);
				z++;
			}
			free(first_array);

		return dkeys_sorted_list;
	}
	return unique_keys;
}

void daos_seis_replace_objects(dfs_t *dfs, int daos_mode, char **keys_1, int shot_header_key, int cmp_header_key,
					int offset_header_key, traces_list_t *trace_list ,seis_root_obj_t *root){
	int rc;
	int i;
	seismic_entry_t seismic_entry={0};

	seis_obj_t *new_seis_obj;
	seis_obj_t *existing_obj = malloc(sizeof(seis_obj_t));
	seismic_entry_t new_entry={0};
//	printf("SHOT_OID %llu %llu \n", root->shot_oid.lo, root->shot_oid.hi);
//	printf("CMP_OID %llu %llu \n", root->cmp_oid.lo, root->cmp_oid.hi);
//	printf("OFFSET_OID %llu %llu \n", root->offset_oid.lo, root->offset_oid.hi);
		if(shot_header_key){
			printf("KEY MATCHES FLDR \n");
			existing_obj->oid = root->shot_oid;
//			rc = daos_seis_gather_obj_create(dfs,OC_SX, root, &new_seis_obj, "fldr");
		} else if(cmp_header_key){
			printf("KEY MATCHES CDP \n");
			existing_obj->oid = root->cmp_oid;
//			rc = daos_seis_gather_obj_create(dfs,OC_SX, root, &new_seis_obj, "cdp");
		} else if (offset_header_key){
			printf("KEY MATCHES OFFSET \n");
			existing_obj->oid = root->offset_oid;
//			rc = daos_seis_gather_obj_create(dfs,OC_SX, root, &new_seis_obj, "offset");
		} else {
//			continue;
		}
//		if (rc) {
//			warn("FAILED TO create gather OBJECT");
////			return rc;
//		}

		rc = daos_obj_open(dfs->coh, existing_obj->oid, daos_mode, &(existing_obj->oh), NULL);
		printf("OPENED SEISMIC OBJECT \n");
		if (rc) {
			printf("daos_obj_open()__ seismic _object Failed (%d)\n", rc);
//			return rc;
		}
//		int temp;
		//Fetch Number of Gathers Under opened Gather object
		prepare_seismic_entry(&seismic_entry, existing_obj->oid, DS_D_NGATHERS, DS_A_NGATHERS,
					(char*)&(existing_obj->number_of_gathers), sizeof(int), DAOS_IOD_SINGLE);

		rc = daos_seis_fetch_entry(existing_obj->oh, DAOS_TX_NONE, &seismic_entry, NULL);
		if(rc){
			printf("FAILED TO FETCH NUMBER OF GATHERS IN EXISTING OBJECT %d \n", rc);
		}
//		existing_obj->number_of_gathers = temp;
		printf("NUMBER OF GATHERS IN EXISTING OBJECT IS %d \n", existing_obj->number_of_gathers);


		char **temp_keys;
		temp_keys = daos_seis_fetch_dkeys(existing_obj, 0,shot_header_key,cmp_header_key, offset_header_key, 1);
//		printf("SEG FAULT HERE %d %d %d \n", shot_header_key, cmp_header_key, offset_header_key);

//		for(i=0; i<existing_obj->number_of_gathers; i++){
//			printf("KEY number %d is %s \n", i, temp_keys[i]);
//		}

		for(i=0; i < existing_obj->number_of_gathers; i++){
			printf("KEY number %d is %s \n", i, temp_keys[i]);
			trace_oid_oh_t *temp = malloc(sizeof(trace_oid_oh_t));
			prepare_seismic_entry(&seismic_entry, existing_obj->oid, temp_keys[i], DS_A_GATHER_TRACE_OIDS,
									(char*)&temp->oid, sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
			rc = daos_seis_fetch_entry(existing_obj->oh, DAOS_TX_NONE, &seismic_entry, NULL);
			rc = daos_array_open_with_attr(dfs->coh, temp->oid, DAOS_TX_NONE,DAOS_OO_RW, 1,
									500*sizeof(daos_obj_id_t), &(temp->oh), NULL);
			if (rc) {
				printf("daos_array_open_with_attr()-->>GATHER OIDS object<<-- failed (%d)\n", rc);
				exit(0);
			}
			rc = daos_array_destroy(temp->oh, DAOS_TX_NONE, NULL);
			if(rc){
				printf("failed to punch array object \n");
			}
			rc = daos_array_close(temp->oh, NULL);
			if(rc){
				printf("FAILED TO CLOSE ARRAY OBJECT \n");
			}
			free(temp);

		}
		for(i=0; i < existing_obj->number_of_gathers; i++){
			free(temp_keys[i]);
		}
		free(temp_keys);

		rc = daos_obj_punch(existing_obj->oh, DAOS_TX_NONE, 0, NULL);
		if(rc){
			printf("Failed to punch existing object \n");
		}
//		daos_obj_close(existing_obj->oh,NULL);

		if(shot_header_key){
			printf("KEY MATCHES FLDR \n");
//			existing_obj->oid = root->shot_oid;
			rc = daos_seis_gather_obj_create(dfs,OC_SX, root, &new_seis_obj, "fldr");
		} else if(cmp_header_key){
			printf("KEY MATCHES CDP \n");
//			existing_obj->oid = root->cmp_oid;
			rc = daos_seis_gather_obj_create(dfs,OC_SX, root, &new_seis_obj, "cdp");
		} else if (offset_header_key){
			printf("KEY MATCHES OFFSET \n");
//			existing_obj->oid = root->offset_oid;
			rc = daos_seis_gather_obj_create(dfs,OC_SX, root, &new_seis_obj, "offset");
		} else {
			return;
		}
		if (rc) {
			warn("FAILED TO create gather OBJECT");
//			return rc;
		}
		printf("LINKING TRACES ========== \n");
		traces_headers_t *current= trace_list->head;
		while(current != NULL){
//		printf("HELLO \n");
		trace_obj_t *trace_obj = malloc(sizeof(trace_obj_t));
		trace_obj->trace = malloc(sizeof(trace_t));
//		printf("HELLO \n");
//		*(trace_obj->trace)= current->trace;
		memcpy(trace_obj->trace, &(current->trace), sizeof(trace_t));
//		printf("HELLO \n");

//		*(trace_obj->trace) = current->trace;
		trace_obj->oid = current->trace.trace_header_obj;
		if(shot_header_key) {
			rc = new_daos_seis_tr_linking(dfs,trace_obj,new_seis_obj,"fldr");
		} else if(cmp_header_key) {
			rc = new_daos_seis_tr_linking(dfs,trace_obj,new_seis_obj,"cdp");
		} else {
			rc = new_daos_seis_tr_linking(dfs,trace_obj,new_seis_obj,"offset");
		}
		if(rc) {
			printf("failed in linking trace to new seismic object %d \n", rc);
		}
		current = current->next_trace;
		free(trace_obj->trace);
		free(trace_obj);
		}
		rc = update_gather_object(new_seis_obj, DS_D_NGATHERS, DS_A_NGATHERS,
						(char*)&new_seis_obj->number_of_gathers, sizeof(int), DAOS_IOD_SINGLE);
		if(rc){
			printf("ERROR adding Number of gathers key, error: %d", rc);
//			return rc;
		}
		printf("NEW NUMBER OF GATHERS IS %d \n", new_seis_obj->number_of_gathers);
		rc = daos_seis_trace_oids_obj_create(dfs,OC_SX, new_seis_obj);
		if (rc)
		{
			warn("FAILED TO CREATE ARRAY OIDs OBJECT");
//			return rc;
		}
		if(shot_header_key){
			rc = new_update_gather_traces(dfs, new_seis_obj->gathers, new_seis_obj, "fldr_", DS_A_NTRACES);
		} else if (cmp_header_key){
			rc = new_update_gather_traces(dfs, new_seis_obj->gathers, new_seis_obj, "cdp_", DS_A_NTRACES);
		} else {
			rc = new_update_gather_traces(dfs, new_seis_obj->gathers, new_seis_obj, "offset_", DS_A_NTRACES);
		}
		if(rc){
			printf("ERROR UPDATING shot number_of_traces key, error: %d \n", rc);
		}
		daos_obj_close(new_seis_obj->oh, NULL);
		free(existing_obj);
}

void tokenize_str(void **str, char *sep, char *string, int type){
	char temp[4096];
	strcpy(temp, string);
	char *token = strtok(temp, sep);
	int i=0;
	char **temp_c;
	long *temp_l;
	double *temp_d;
	char *ptr;

	while( token != NULL ) {
		switch(type){
			case 0:
				temp_c = (char **) str;
				temp_c[i]= malloc((strlen(token)+1)*sizeof(char));
				strcpy(temp_c[i],token);
				break;
			case 1:
				temp_l = *((long **) str);
				temp_l[i]= atol(token);
				break;
			case 2:
				temp_d = *((double **) str);
				temp_d[i] = strtod(token, &ptr);
				break;
			default:
				printf("ERROR\n");
				exit(0);
		}
		i++;
		token = strtok(NULL, sep);
	}
}

void range_traces_headers(traces_list_t *trace_list, int number_of_keys, char **keys, int dim){
	int i;
	traces_headers_t *current = trace_list->head;
	Value val;
	Value valmin;
	Value valmax;
	trace_t *trmin = malloc(sizeof(trace_t));
	trace_t *trmax = malloc(sizeof(trace_t));
	trace_t *trfirst = malloc(sizeof(trace_t));
	trace_t *trlast = malloc(sizeof(trace_t));
    double eastShot[2], westShot[2], northShot[2], southShot[2];
    double eastRec[2], westRec[2], northRec[2], southRec[2];
    double eastCmp[2], westCmp[2], northCmp[2], southCmp[2];
    double dcoscal = 1.0;
    double sx, sy, gx, gy, mx, my;
    double mx1=0.0, my1=0.0;
    double mx2=0.0, my2=0.0, dm=0.0, dmin=0.0, dmax=0.0, davg=0.0;
    int coscal = 1;

    northShot[0] = southShot[0] = eastShot[0] = westShot[0] = 0.0;
    northShot[1] = southShot[1] = eastShot[1] = westShot[1] = 0.0;
    northRec[0] = southRec[0] = eastRec[0] = westRec[0] = 0.0;
    northRec[1] = southRec[1] = eastRec[1] = westRec[1] = 0.0;
    northCmp[0] = southCmp[0] = eastCmp[0] = westCmp[0] = 0.0;
    northCmp[1] = southCmp[1] = eastCmp[1] = westCmp[1] = 0.0;
    sx = sy = gx = gy = mx = my = 0.0;

	if (number_of_keys==0) {
		for (i = 0; i < SU_NKEYS; ++i) {
			get_header_value_new(current->trace, keys[i], &val);
			set_header_value(trmin, keys[i], &val);
			set_header_value(trmax, keys[i], &val);
			set_header_value(trfirst, keys[i], &val);

			if(i == 20) {
				coscal = val.h;
				if(coscal == 0) {
					coscal = 1;
				} else if(coscal > 0) {
					dcoscal = 1.0*coscal;
				} else {
					dcoscal = 1.0/coscal;
				}
			} else if(i == 21) {
				sx = eastShot[0] = westShot[0] = northShot[0] = southShot[0] = val.i*dcoscal;
			} else if(i == 22) {
				sy = eastShot[1] = westShot[1] = northShot[1] = southShot[1] = val.i*dcoscal;
			} else if(i == 23) {
				gx = eastRec[0] = westRec[0] = northRec[0] = southRec[0] = val.i*dcoscal;
			} else if(i == 24) {
				gy = eastRec[1] = westRec[1] = northRec[1] = southRec[1] = val.i*dcoscal;
			} else {
				continue;
			}
		}
	} else	{
		for (i=0; i<number_of_keys; i++) {
			get_header_value_new(current->trace, keys[i], &val);
			set_header_value(trmin,keys[i],&val);
			set_header_value(trmax,keys[i],&val);
			set_header_value(trfirst,keys[i],&val);
		}
	}
    if(number_of_keys == 0) {
        mx = eastCmp[0] = westCmp[0] = northCmp[0] = southCmp[0] = 0.5*(eastShot[0]+eastRec[0]);
        my = eastCmp[1] = westCmp[1] = northCmp[1] = southCmp[1] = 0.5*(eastShot[1]+eastRec[1]);
    }

	int ntr = 1;
	current = current->next_trace;

	while(current != NULL){
        sx = sy = gx = gy = mx = my = 0.0;
		if (number_of_keys == 0) {
			for (i = 0; i < SU_NKEYS; i++) {
				get_header_value_new(current->trace, keys[i], &val);
				get_header_value_new(*trmin, keys[i], &valmin);
				get_header_value_new(*trmax, keys[i], &valmax);

				if (valcmp(hdr[i].type, val, valmin) < 0) {
					set_header_value(trmin,keys[i],&val);
				} else if (valcmp(hdr[i].type, val, valmax) > 0) {
					set_header_value(trmax,keys[i],&val);
				}

				set_header_value(trlast,keys[i],&val);

				if(i == 20) {
					coscal = val.h;
					if(coscal == 0) {
						coscal = 1;
					} else if (coscal > 0 ){
						dcoscal = 1.0*coscal;
					} else {
						dcoscal = 1.0/coscal;
					}
				} else if(i == 21) {
					sx = val.i*dcoscal;
				} else if(i == 22) {
					sy = val.i*dcoscal;
				} else if(i == 23) {
					gx = val.i*dcoscal;
				} else if(i == 24) {
					gy = val.i*dcoscal;
				} else {
					continue;
				}
			}
		} else {
			for (i=0; i < number_of_keys; i++) {
				get_header_value_new(current->trace, keys[i], &val);
				get_header_value_new(*trmin, keys[i], &valmin);
				get_header_value_new(*trmax, keys[i], &valmax);
				if (valcmp(hdtype(keys[i]), val, valmin) < 0) {
					set_header_value(trmin,keys[i],&val);
				} else if (valcmp(hdtype(keys[i]), val, valmax) > 0) {
					set_header_value(trmax,keys[i],&val);
				}
				set_header_value(trlast,keys[i],&val);
			}
		}

        if(number_of_keys == 0) {
            mx = 0.5*(sx+gx);
            my = 0.5*(sy+gy);
            if(eastShot[0] < sx) {
            	eastShot[0] = sx;
            	eastShot[1] = sy;
            }
            if(westShot[0] > sx) {
            	westShot[0] = sx;
            	westShot[1] = sy;
            }
            if(northShot[1] < sy) {
            	northShot[0] = sx;
            	northShot[1] = sy;
            }
            if(southShot[1] > sy) {
            	southShot[0] = sx;
            	southShot[1] = sy;
            }
            if(eastRec[0] < gx) {
            	eastRec[0] = gx;
            	eastRec[1] = gy;
            }
            if(westRec[0] > gx) {
            	westRec[0] = gx;
            	westRec[1] = gy;
            }
            if(northRec[1] < gy) {
            	northRec[0] = gx;
            	northRec[1] = gy;
            }
            if(southRec[1] > gy) {
            	southRec[0] = gx;
            	southRec[1] = gy;
            }
            if(eastCmp[0] < mx) {
            	eastCmp[0] = mx;
            	eastCmp[1] = my;
            }
            if(westCmp[0] > mx) {
            	westCmp[0] = mx;
            	westCmp[1] = my;
            }
            if(northCmp[1] < my){
            	northCmp[0] = mx;
            	northCmp[1] = my;
            }
            if(southCmp[1] > my){
            	southCmp[0] = mx;
            	southCmp[1] = my;
            }
        }

        if (ntr == 1) {
            /* get midpoint (mx1,my1) on trace 1 */
            mx1 = 0.5*(current->trace.sx+current->trace.gx);
            my1 = 0.5*(current->trace.sy+current->trace.gy);
        } else if (ntr == 2) {
            /* get midpoint (mx2,my2) on trace 2 */
            mx2 = 0.5*(current->trace.sx + current->trace.gx);
            my2 = 0.5*(current->trace.sy + current->trace.gy);
            /* midpoint interval between traces 1 and 2 */
            dm = sqrt( (mx1 - mx2)*(mx1 - mx2) + (my1 - my2)*(my1 - my2) );
            /* set min, max and avg midpoint interval holders */
            dmin = dm;
            dmax = dm;
            davg = (dmin+dmax)/2.0;
            /* hold this midpoint */
            mx1 = mx2;
            my1 = my2;
        } else if (ntr > 2) {
            /* get midpoint (mx,my) on this trace */
            mx2 = 0.5*(current->trace.sx + current->trace.gx);
            my2 = 0.5*(current->trace.sy + current->trace.gy);
            /* get midpoint (mx,my) between this and previous trace */
            dm = sqrt( (mx1 - mx2)*(mx1 - mx2) + (my1 - my2)*(my1 - my2) );
            /* reset min, max and avg midpoint interval holders, if needed */
            if (dm < dmin) dmin = dm;
            if (dm > dmax) dmax = dm;
            davg = (davg + (dmin+dmax)/2.0) / 2.0;
            /* hold this midpoint */
            mx1 = mx2;
            my1 = my2;
        }
        ntr++;
		current = current->next_trace;
	}

	printf("%ld traces: \n",trace_list->size);

	print_headers_ranges(number_of_keys, keys, trmin, trmax, trfirst, trlast);

    if(number_of_keys == 0) {
        if(northShot[1] != 0.0 || southShot[1] != 0.0 || eastShot[0] != 0.0 || westShot[0] != 0.0) {
        	printf("\nShot coordinate limits:\n" "\tNorth(%g,%g) South(%g,%g) East(%g,%g) West(%g,%g)\n",
               northShot[0],northShot[1],southShot[0],southShot[1], eastShot[0],eastShot[1],westShot[0],westShot[1]);
        }
        if(northRec[1] != 0.0 || southRec[1] != 0.0 || eastRec[0] != 0.0 || westRec[0] != 0.0) {
        	printf("\nReceiver coordinate limits:\n" "\tNorth(%g,%g) South(%g,%g) East(%g,%g) West(%g,%g)\n",
               northRec[0],northRec[1],southRec[0],southRec[1], eastRec[0],eastRec[1],westRec[0],westRec[1]);
        }
        if(northCmp[1] != 0.0 || southCmp[1] != 0.0 || eastCmp[0] != 0.0 || westCmp[0] != 0.0) {
        	printf("\nMidpoint coordinate limits:\n" "\tNorth(%g,%g) South(%g,%g) East(%g,%g) West(%g,%g)\n",
               northCmp[0],northCmp[1],southCmp[0],southCmp[1], eastCmp[0],eastCmp[1],westCmp[0],westCmp[1]);
        }
    }

	if (dim != 0){
		if (dim == 1) {
			printf("\n2D line: \n");
			printf("Min CMP interval = %g ft\n",dmin);
			printf("Max CMP interval = %g ft\n",dmax);
			printf("Line length = %g miles (using avg CMP interval of %g ft)\n",davg*ntr/5280,davg);
		} else if (dim == 2) {
			printf("ddim line: \n");
			printf("Min CMP interval = %g m\n",dmin);
			printf("Max CMP interval = %g m\n",dmax);
			printf("Line length = %g km (using avg CMP interval of %g m)\n",davg*ntr/1000,davg);
		}
	}

	return;
}

void print_headers_ranges(int number_of_keys, char **keys, trace_t *trmin, trace_t *trmax, trace_t *trfirst, trace_t *trlast){
	int i;
	Value valmin, valmax, valfirst, vallast;
	double dvalmin, dvalmax;
	cwp_String key;
	cwp_String type;
	int kmax;
	if(number_of_keys==0){
		kmax = SU_NKEYS;
	} else {
		kmax = number_of_keys;
	}
	for(i = 0; i < kmax; i++) {
//		key = getkey(i);
//		type = hdtype(key);
		get_header_value_new(*trmin, keys[i], &valmin);
		get_header_value_new(*trmax, keys[i], &valmax);
		get_header_value_new(*trfirst, keys[i], &valfirst);
		get_header_value_new(*trlast, keys[i], &vallast);
//		type = hdtype(keys[i]);
		dvalmin = vtod(hdtype(keys[i]), valmin);
		dvalmax = vtod(hdtype(keys[i]), valmax);
		if (dvalmin || dvalmax) {
			if (dvalmin < dvalmax) {
				printf("%s ", keys[i]);
				printfval(hdtype(keys[i]), valmin);
				printf(" ");
				printfval(hdtype(keys[i]), valmax);
				printf(" (");
				printfval(hdtype(keys[i]), valfirst);
				printf(" - ");
				printfval(hdtype(keys[i]), vallast);
				printf(")");
			} else {
				printf("%s ", keys[i]);
				printfval(hdtype(keys[i]), valmin);
			}
			printf("\n");
		}
	}
	return;
}

void val_sprintf(char *temp, Value unique_value, char *key){

    switch(*hdtype(key)) {
	case 's':
            (void) sprintf(temp, "%s", unique_value.s);
	break;
	case 'h':
            (void) sprintf(temp, "%d", unique_value.h);
	break;
	case 'u':
            (void) sprintf(temp, "%d", unique_value.u);
	break;
	case 'i':
            (void) sprintf(temp, "%d", unique_value.i);
	break;
	case 'p':
            (void) sprintf(temp, "%d", unique_value.p);
	break;
	case 'l':
            (void) sprintf(temp, "%ld", unique_value.l);
	break;
	case 'v':
            (void) sprintf(temp, "%ld", unique_value.v);
	break;
	case 'f':
            (void) sprintf(temp, "%f", unique_value.f);
	break;
	case 'd':
            (void) sprintf(temp, "%f", unique_value.d);
	break;
	case 'U':
            (void) sprintf(temp, "%d", unique_value.U);
	break;
	case 'P':
            (void) sprintf(temp, "%d", unique_value.P);
	break;
	default:
		err("fprintfval: unknown type %s", *hdtype(key));
	}

	return;

}

