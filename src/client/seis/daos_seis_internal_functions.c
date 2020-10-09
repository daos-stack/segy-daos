/*
 * daos_seis_internal_functions.c
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#include "daos_seis_internal_functions.h"


seis_root_obj_t*
fetch_seismic_root_entries(dfs_t *dfs, dfs_obj_t *root)
{
	seis_root_obj_t 	*root_obj;
	seismic_entry_t 	 entry = {0};
	daos_handle_t 		 th = DAOS_TX_NONE;
	int 			 rc;
	int			 i;

	root_obj = malloc(sizeof(seis_root_obj_t));
	root_obj->coh = dfs->coh;
	root_obj->root_obj = root;

	/** Fetch number of keys */
	prepare_seismic_entry(&entry, root->oid, DS_D_KEYS,
			      DS_A_NUM_OF_KEYS,
			      (char*)&(root_obj->num_of_keys),
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = fetch_seismic_entry(root->oh, th, &entry, NULL);
	if (rc != 0) {
		err("Fetching number of keys failed, "
		    "error code = %d \n", rc);
		exit(rc);
	}
	root_obj->keys = malloc(root_obj->num_of_keys * sizeof(char*));

	for(i = 0 ;i < root_obj->num_of_keys; i++) {
		root_obj->keys[i] = malloc(10 * sizeof(char));
		char temp[10]="";
		char akey[100]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);
		prepare_seismic_entry(&entry, root->oid, DS_D_KEYS, akey,
				      root_obj->keys[i], 10 * sizeof(char),
				      DAOS_IOD_ARRAY);
		rc = fetch_seismic_entry(root->oh, th, &entry, NULL);
		if (rc != 0) {
			err("Fetching array of keys failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}
	}
	root_obj->gather_oids = malloc(root_obj->num_of_keys * sizeof(daos_obj_id_t));
	/** Fetch gather object ids */
	for(i = 0; i < root_obj->num_of_keys; i++){
		prepare_seismic_entry(&entry, root->oid, DS_D_SORTING_TYPES,
				      get_dkey(root_obj->keys[i]),
				      (char*)(&root_obj->gather_oids[i]),
				      sizeof(daos_obj_id_t),
				      DAOS_IOD_SINGLE);
		rc = fetch_seismic_entry(root->oh, th, &entry, NULL);
		if (rc != 0) {
			err("Fetching <%s> gather oid failed, "
			    "error code = %d \n", root_obj->keys[i], rc);
			exit(rc);
		}
	}

	/** fetch number of traces */
	prepare_seismic_entry(&entry, root->oid, DS_D_FILE_HEADER,
			      DS_A_NTRACES_HEADER,
			      (char*) (&root_obj->number_of_traces),
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = fetch_seismic_entry(root->oh, th, &entry, NULL);
	if (rc != 0) {
		err("Fetching number of traces failed, error code = %d \n", rc);
		exit(rc);
	}

	/** fetch number of extended text headers */
	prepare_seismic_entry(&entry, root->oid, DS_D_FILE_HEADER,
			      DS_A_NEXTENDED_HEADER,
			      (char*) (&root_obj->nextended),
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = fetch_seismic_entry(root->oh, th, &entry, NULL);
	if (rc != 0) {
		err("Fetching number of extended headers oid failed,"
				" error code = %d \n", rc);
		exit(rc);
	}


	return root_obj;
}

int
fetch_seismic_entry(daos_handle_t oh, daos_handle_t th,
	    seismic_entry_t *entry, daos_event_t *ev)
{
	daos_recx_t 	recx;
	d_sg_list_t 	sgl;
	daos_iod_t 	iod;
	daos_key_t 	dkey;
	d_iov_t 	sg_iovs;
	int 		rc;

	d_iov_set(&dkey, (void*)entry->dkey_name, strlen(entry->dkey_name));
	d_iov_set(&iod.iod_name, (void*) entry->akey_name,
		  strlen(entry->akey_name));
	d_iov_set(&sg_iovs, entry->data, entry->size);

	if (entry->iod_type == DAOS_IOD_SINGLE) {
		recx.rx_nr = 1;
		iod.iod_size = entry->size;
	} else if (entry->iod_type == DAOS_IOD_ARRAY) {
		recx.rx_nr = entry->size;
		iod.iod_size = 1;
	}

	iod.iod_nr = 1;
	recx.rx_idx = 0;
	iod.iod_recxs = &recx;
	iod.iod_type = entry->iod_type;
	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	sgl.sg_iovs = &sg_iovs;
	/** insert task in event queue if event is passed
	 * otherwise function will run in blocking mode */
	if (ev != NULL) {
		rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL, ev);
		if (ev->ev_error) {
			err("Failed to fetch <%s> and <%s> entry, error"
			    " code = %d\n", entry->dkey_name,
			    entry->akey_name, ev->ev_error);
			return ev->ev_error;
		}
	} else {
		rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL,
				    NULL);
		if (rc != 0) {
			err("Failed to fetch <%s> and <%s> entry, error"
			    " code = %d\n", entry->dkey_name, entry->akey_name, rc);
			return rc;
		}
	}

	return rc;
}

int
seismic_root_obj_create(dfs_t *dfs, seis_root_obj_t **obj,
			  daos_oclass_id_t cid, char *name,
			  dfs_obj_t *parent, int num_of_keys, char **keys)
{
	struct dfs_entry 	dfs_entry = {0};
	int 			daos_mode;
	int 			rc;
	int 			i;

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
	(*obj)->num_of_keys = num_of_keys;
	(*obj)->gather_oids = malloc(num_of_keys * sizeof(daos_obj_id_t));
	(*obj)->keys = malloc(num_of_keys * sizeof(char*));

	for(i = 0; i < num_of_keys; i++){
		(*obj)->keys[i] = malloc((strlen(keys[i]) + 1) * sizeof(char));
		strcpy((*obj)->keys[i], keys[i]);
	}

	if (parent == NULL)
		parent = &dfs->root;

	/** Get new OID for root object */
	rc = oid_gen(dfs, cid, false, &((*obj)->root_obj->oid));
	if (rc != 0) {
		err("Generating OID for seismic root object failed,"
		    " error code = %d \n",rc);
		return rc;
	}

	daos_mode = get_daos_obj_mode((*obj)->root_obj->flags);

	rc = daos_obj_open(dfs->coh, (*obj)->root_obj->oid, daos_mode,
			   &((*obj)->root_obj->oh), NULL);
	if (rc != 0) {
		err("Opening seismic root object failed,"
		    " error code = %d \n",rc);
		return rc;
	}

	dfs_entry.oid = (*obj)->root_obj->oid;
	dfs_entry.mode = (*obj)->root_obj->mode;
	dfs_entry.chunk_size = 0;
	dfs_entry.atime = dfs_entry.mtime = dfs_entry.ctime = time(NULL);

	/** insert Seismic root object created under parent */
	rc = insert_entry(parent->oh, DAOS_TX_NONE, (*obj)->root_obj->name,
			  dfs_entry);
	if (rc != 0) {
		err("Inserting seismic root object under parent failed,"
		    " error code = %d \n",rc);
		return rc;
	}

	return rc;
}

int
seismic_root_obj_update(seis_root_obj_t *root_obj, char *dkey_name,
		      char *akey_name, char *databuf, int nbytes,
		      daos_iod_type_t iod_type)
{
	seismic_entry_t 	seismic_entry = {0};
	int 			rc;

	prepare_seismic_entry(&seismic_entry, root_obj->root_obj->oid,
			      dkey_name, akey_name, databuf, nbytes, iod_type);

	rc = seismic_obj_update(root_obj->root_obj->oh, DAOS_TX_NONE,
				  seismic_entry);
	if (rc != 0) {
		err("Updating root seismic object failed, "
		    "error code = %d \n", rc);
		return rc;
	}

	return rc;
}

void
merge_trace_lists(traces_list_t **headers, traces_list_t **temp_list)
{
	trace_node_t	 	*temp = (*headers)->head;

	if ((*temp_list)->head == NULL) {
//		warn("Temp linked list of traces is empty.\n");
		return;
	}
	/** merge two linked lists in one */
	if ((*headers)->head == NULL) {
		(*headers)->head = (*temp_list)->head;
		(*headers)->tail = (*temp_list)->tail;
		(*headers)->size = (*temp_list)->size;
	} else {
		(*headers)->tail->next_trace = (*temp_list)->head;
		(*headers)->tail = (*temp_list)->tail;
		(*headers)->size = (*headers)->size + (*temp_list)->size;
	}
}

void
add_trace_header(trace_t *trace, traces_list_t **traces,
		 ensembles_list_t **ensembles, int index)
{
	trace_node_t 	*new_node;
	ensemble_node_t		*new_ensemble;
	new_node = (trace_node_t*) malloc(sizeof(trace_node_t));
	new_node->trace = *trace;
	new_node->trace.data = NULL;
	new_node->next_trace = NULL;

	if ((*traces)->head == NULL) {
		(*traces)->head = new_node;
		(*traces)->tail = new_node;
		(*traces)->size++;
		new_ensemble = (ensemble_node_t*) malloc(sizeof(ensemble_node_t));
		new_ensemble->ensemble = new_node;
		new_ensemble->next_ensemble = NULL;
		(*ensembles)->first_ensemble = new_ensemble;
		(*ensembles)->last_ensemble = new_ensemble;
		(*ensembles)->last_ensemble->next_ensemble = NULL;
		(*ensembles)->num_of_ensembles++;
	} else {
		(*traces)->tail->next_trace = new_node;
		(*traces)->tail = new_node;
		(*traces)->size++;
		if(index == 0) {
			new_ensemble = (ensemble_node_t*) malloc(sizeof(ensemble_node_t));
			new_ensemble->ensemble = new_node;
			new_ensemble->next_ensemble = NULL;
			(*ensembles)->last_ensemble->next_ensemble = new_ensemble;
			(*ensembles)->last_ensemble = new_ensemble;
			(*ensembles)->last_ensemble->next_ensemble = NULL;
			(*ensembles)->num_of_ensembles++;
		}
	}
}

int
update_gather_data(dfs_t *dfs, gathers_list_t *head, seis_obj_t *object,
		     char *dkey_name)
{
	trace_oid_oh_t		gather_trace;

	seis_gather_t *curr_gather = head->head;

	if (curr_gather == NULL) {
//		warn("No gathers exist in linked list \n");
		return 0;
	} else {
		int z = 0;
		while (curr_gather != NULL) {
			int ntraces = curr_gather->number_of_traces;
			int rc;
			char temp[200] = "";
			char gather_dkey_name[200] = "";
			strcat(gather_dkey_name, dkey_name);
			strcat(gather_dkey_name, KEY_SEPARATOR);
			val_sprintf(temp, curr_gather->unique_key, object->name);
			strcat(gather_dkey_name, temp);

			gather_trace = object->seis_gather_trace_oids_obj[z];
			/** insert array object_id in gather object... */
			rc = gather_oids_array_update(&gather_trace, curr_gather);
			if (rc != 0) {
				err("Updating <%s> object trace object "
				    "array failed, error code = %d \n",
				    object->name, rc);
				return rc;
			}
			rc = update_seismic_gather_object(object,
							  gather_dkey_name,
							  DS_A_GATHER_TRACE_OIDS,
							  (char*)&(gather_trace.oid),
							  sizeof(daos_obj_id_t),
							  DAOS_IOD_SINGLE);
			if (rc != 0) {
				err("Updating <%s> object trace object ids key"
				    "failed, error code = %d \n",
				    object->name, rc);
				return rc;
			}
			rc = update_seismic_gather_object(object,
							  gather_dkey_name,
							  DS_A_NTRACES,
							  (char*) &ntraces,
							  sizeof(int),
							  DAOS_IOD_SINGLE);
			if (rc != 0) {
				err("Updating <%s> object number of traces key"
				    "failed, error code = %d \n",
				    object->name, rc);
				return rc;
			}
			curr_gather = curr_gather->next_gather;
			z++;
		}
	}
	return 0;
}

int
trace_oids_obj_create(dfs_t *dfs, daos_oclass_id_t cid,
		      seis_obj_t *seis_obj, int num_of_gathers)
{
	int 		rc;
	int 		i;

	if(seis_obj->seis_gather_trace_oids_obj == NULL) {
		seis_obj->seis_gather_trace_oids_obj =
				malloc(seis_obj->number_of_gathers *
				       sizeof(trace_oid_oh_t));
	} else {
		seis_obj->seis_gather_trace_oids_obj = (trace_oid_oh_t*)
				realloc(seis_obj->seis_gather_trace_oids_obj,
					seis_obj->number_of_gathers *
				        sizeof(trace_oid_oh_t));
	}
	for (i = 0; i < seis_obj->number_of_gathers; i++) {
		if (&(seis_obj->seis_gather_trace_oids_obj[i]) == NULL) {
			return ENOMEM;
		}

		if(i >= num_of_gathers) {
			/** Get new OID for shot object */
			rc = oid_gen(dfs, cid, true,
				     &(seis_obj->seis_gather_trace_oids_obj[i]).oid);
			if (rc != 0) {
				err("Generating object id for gather trace oids failed,"
				    " error code = %d \n", rc);
				return rc;
			}
		}
		/** Open the array object for the gather oids */
		rc = daos_array_open_with_attr(dfs->coh, seis_obj->seis_gather_trace_oids_obj[i].oid,
					       DAOS_TX_NONE, DAOS_OO_RW, 1,
					       500 * sizeof(daos_obj_id_t),
					       &(seis_obj->seis_gather_trace_oids_obj[i]).oh, NULL);
		if (rc != 0) {
			err("Opening gather oids array object failed,"
			    " error code = %d \n", rc);
			return rc;
		}

	}
	return rc;
}

int
seismic_gather_obj_create(dfs_t *dfs, daos_oclass_id_t cid,
			  seis_root_obj_t *parent, seis_obj_t **obj,
			  char *key, int index)
{
	int 		daos_mode;
	int 		rc;

	/*Allocate shot object pointer */
	D_ALLOC_PTR(*obj);

	if (*obj == NULL){
		return ENOMEM;
	}
	strcpy((*obj)->name, key);
	(*obj)->seis_gather_trace_oids_obj = NULL;
	(*obj)->gathers = malloc(sizeof(gathers_list_t));
	(*obj)->gathers->head = NULL;
	(*obj)->gathers->tail = NULL;
	(*obj)->gathers->size = 0;
	(*obj)->number_of_gathers = 0;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid, false, &(*obj)->oid);
	if (rc != 0) {
		err("GENERATING OBJECT ID for seismic object failed,"
		    " error code = %d \n", rc);
		return rc;
	}
	daos_mode = get_daos_obj_mode(O_RDWR);

	rc = daos_obj_open(dfs->coh, (*obj)->oid, daos_mode, &(*obj)->oh, NULL);
	if (rc != 0) {
		err("Opening seismic object failed,"
		    " error code = %d \n", rc);
		return rc;
	}

	oid_cp(&parent->gather_oids[index], (*obj)->oid);
	rc = seismic_root_obj_update(parent, DS_D_SORTING_TYPES, get_dkey(key),
				    (char*) &(*obj)->oid, sizeof(daos_obj_id_t),
				    DAOS_IOD_SINGLE);
	if (rc != 0) {
		err("Updating seismic root object failed,"
		    " error code = %d \n", rc);
		return rc;
	}

	return rc;
}

int
trace_data_update(trace_oid_oh_t *trace_data_obj, segy *trace)
{
	daos_array_iod_t 	iod;
	daos_range_t 		rg;
	d_sg_list_t 		sgl;
	d_iov_t 		iov;
	int 			offset = 0;
	int 			rc;

	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	d_iov_set(&iov, (void*)(char*)(trace->data),
		  trace->ns * sizeof(float));

	sgl.sg_iovs = &iov;
	iod.arr_nr = 1;
	rg.rg_len = trace->ns * sizeof(float);
	rg.rg_idx = offset;
	iod.arr_rgs = &rg;

	rc = daos_array_write(trace_data_obj->oh, DAOS_TX_NONE, &iod,
			      &sgl, NULL);
	if (rc != 0) {
		err("Updating trace data failed, error code = %d \n", rc);
		return rc;
	}

	return rc;
}

daos_obj_id_t
get_trace_data_oid(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid)
{

	daos_obj_id_t 		tr_data_oid;
	uint64_t 		ofeats;
	uint64_t 		hdr_val;

	tr_data_oid= *tr_hdr;
	tr_data_oid.hi++;

	ofeats = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_ARRAY_BYTE;


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
	hdr_val = ((uint64_t) OID_FMT_VER << OID_FMT_VER_SHIFT);
	hdr_val |= ((uint64_t) ofeats << OID_FMT_FEAT_SHIFT);
	hdr_val |= ((uint64_t) cid << OID_FMT_CLASS_SHIFT);
	tr_data_oid.hi |= hdr_val;

	return tr_data_oid;
}

int
trace_obj_create(dfs_t *dfs, trace_obj_t **trace_hdr_obj, int index,
		 segy *trace)
{
	trace_oid_oh_t 		*trace_data_obj;
	char 			 trace_hdr_name[200];
	char 			 trace_data_name[200];
	char 			 tr_index[50];
	int 			 daos_mode;
	int 			 rc;

	strcpy(trace_hdr_name, "Trace_hdr_obj_");
	strcpy(trace_data_name, "Trace_data_obj_");

	/** Allocate object pointer */
	D_ALLOC_PTR(*trace_hdr_obj);
	if ((*trace_hdr_obj) == NULL) {
		return ENOMEM;
	}

	sprintf(tr_index, "%d", index);
	strcat(trace_hdr_name, tr_index);

	strncpy((*trace_hdr_obj)->name, trace_hdr_name, SEIS_MAX_PATH);
	(*trace_hdr_obj)->name[SEIS_MAX_PATH] = '\0';
	(*trace_hdr_obj)->trace = malloc(sizeof(trace_t));
	memcpy((*trace_hdr_obj)->trace, trace, TRACEHDR_BYTES);

	/** Get new OID for trace header object */
	rc = oid_gen(dfs, OC_SX, false, &(*trace_hdr_obj)->oid);
	if (rc != 0) {
		err("Generating Object id for trace header failed,"
		    " error code = %d \n",rc);
		return rc;
	}

	(*trace_hdr_obj)->trace->trace_header_obj = (*trace_hdr_obj)->oid;
	daos_mode = get_daos_obj_mode(O_RDWR);

	rc = daos_obj_open(dfs->coh, (*trace_hdr_obj)->oid, daos_mode,
			   &(*trace_hdr_obj)->oh, NULL);
	if (rc != 0) {
		err("Opening trace header object failed,"
		    " error code = %d \n",rc);
		return rc;
	}
	trace_oid_oh_t 		trace_oid_oh;

	trace_oid_oh.oid = (*trace_hdr_obj)->oid;
	trace_oid_oh.oh = (*trace_hdr_obj)->oh;

	rc = trace_header_update(&trace_oid_oh, (*trace_hdr_obj)->trace,
				 TRACEHDR_BYTES);
	if (rc != 0) {
		err("Updating trace header object failed, "
		    "error code = %d\n", rc);
		return rc;
	}

	D_ALLOC_PTR(trace_data_obj);

	if ((trace_data_obj) == NULL) {
		return ENOMEM;
	}

	/** Get new OID for trace data object */
	rc = oid_gen(dfs, OC_SX, true, &(trace_data_obj)->oid);
	if (rc != 0) {
		err("Generating Object id for trace data failed,"
		    " error code = %d \n",rc);
		return rc;
	}

	/** Open the array object for the file */
	rc = daos_array_open_with_attr(dfs->coh, (trace_data_obj)->oid,
				       DAOS_TX_NONE, DAOS_OO_RW, 1,
				       200 * sizeof(float),
				       &trace_data_obj->oh, NULL);
	if (rc != 0) {
		err("Opening trace data object failed,"
		    " error code = %d \n",rc);
		return rc;
	}
	/** Update trace data object */
	rc = trace_data_update(trace_data_obj, trace);
	if (rc != 0) {
		err("Updating trace data object failed, "
		    "error code = %d\n", rc);
		return rc;
	}
	/** Close trace data object */
	rc = daos_array_close(trace_data_obj->oh, NULL);
	if (rc != 0) {
		err("Closing trace data object failed,"
		    " error code = %d \n",rc);
		return rc;
	}

	D_FREE_PTR(trace_data_obj);
	return rc;
}

void
prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid,
		      char *dkey, char *akey, char *data, int size,
		      daos_iod_type_t iod_type)
{
	entry->oid = oid;
	entry->dkey_name = dkey;
	entry->akey_name = akey;
	entry->data = data;
	entry->size = size;
	entry->iod_type = iod_type;
}

int
update_seismic_gather_object(seis_obj_t *gather_obj, char *dkey_name,
			     char *akey_name, char *data, int nbytes,
			     daos_iod_type_t type)
{
	seismic_entry_t 	gather_entry = {0};
	int 			rc;
	prepare_seismic_entry(&gather_entry, gather_obj->oid, dkey_name,
			      akey_name, data, nbytes, type);
	rc = seismic_obj_update(gather_obj->oh, DAOS_TX_NONE, gather_entry);
	if (rc != 0) {
		err("Updating gather object failed, error code = %d\n", rc);
		return rc;
	}

	return rc;
}

int
trace_linking(trace_obj_t *trace_obj, seis_obj_t *seis_obj, char *key)
{
	seis_gather_t 	new_gather_data = {0};
	Value 		unique_value;
	int 		key_exists = 0;
	int 		ntraces;
	int 		rc = 0;

	get_header_value(*(trace_obj->trace), key, &unique_value);
	if (check_key_value(unique_value, key, seis_obj->gathers,
			    trace_obj->oid) == 1) {
		key_exists = 1;
	}

	/** if key value doesn't exist in the object gathers */
	if (key_exists == 0) {
		new_gather_data.oids = malloc(50 * sizeof(daos_obj_id_t));
		char temp[200] = "";
		new_gather_data.oids[0] = trace_obj->oid;
		new_gather_data.number_of_traces = 1;
		new_gather_data.unique_key = unique_value;
		char dkey_name[200] = "";
		strcat(dkey_name, get_dkey(key));
		strcat(dkey_name, KEY_SEPARATOR);
		val_sprintf(temp, unique_value, key);
		strcat(dkey_name, temp);
		rc = update_seismic_gather_object(seis_obj, dkey_name,
						  DS_A_UNIQUE_VAL,
						  (char*)&new_gather_data.unique_key,
						  sizeof(long), DAOS_IOD_SINGLE);
		if (rc != 0) {
			err("Adding unique value key to seismic object failed,"
			    " error code = %d\n", rc);
			return rc;
		}
		add_gather(&new_gather_data,&(seis_obj->gathers),0);
		seis_obj->number_of_gathers++;
		free(new_gather_data.oids);
	}

	return rc;
}

void
fetch_traces_header_read_traces(daos_handle_t coh, daos_obj_id_t *oids,
				read_traces *traces, int daos_mode)
{
	seismic_entry_t 	seismic_entry = {0};
	trace_oid_oh_t		trace_hdr_obj;
	int 			rc;
	int 			i;

	for (i = 0; i < traces->number_of_traces; i++) {
		trace_hdr_obj.oid = oids[i];
		/** Open trace header object */
		rc = daos_obj_open(coh, trace_hdr_obj.oid, daos_mode,
				   &trace_hdr_obj.oh, NULL);
		if (rc != 0) {
			err("Opening trace header object failed, error"
			    " code = %d \n", rc);
			exit(rc);
		}
		/** Fetch Trace headers */
		prepare_seismic_entry(&seismic_entry, trace_hdr_obj.oid,
				      DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
				      (char*)&(traces->traces[i]), TRACEHDR_BYTES,
				      DAOS_IOD_ARRAY);
		rc = fetch_seismic_entry(trace_hdr_obj.oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching trace headers failed, error"
			    " code = %d \n", rc);
			exit(rc);
		}
		/** Close trace header object */
		daos_obj_close(trace_hdr_obj.oh, NULL);
		/** Write trace header object id */
		traces->traces[i].trace_header_obj = oids[i];
	}
}

void
fetch_traces_header_traces_list(daos_handle_t coh, daos_obj_id_t *oids,
				traces_metadata_t *traces_metadata,
				int daos_mode, int num_of_traces)
{
	seismic_entry_t 	seismic_entry = {0};
	trace_oid_oh_t		trace_hdr_obj;
	trace_t 	        temp_trace;
	int 			rc;
	int 			i;

	for (i = 0; i < num_of_traces; i++) {
		trace_hdr_obj.oid = oids[i];

		/** open trace header object */
		rc = daos_obj_open(coh, trace_hdr_obj.oid, daos_mode,
				   &trace_hdr_obj.oh, NULL);
		if (rc != 0) {
			err("Opening trace header object failed, error"
			    " code = %d \n", rc);
			return;
		}
		/** Fetch Trace header */
		prepare_seismic_entry(&seismic_entry, trace_hdr_obj.oid,
				      DS_D_TRACE_HEADER, DS_A_TRACE_HEADER,
				      (char*)&temp_trace, TRACEHDR_BYTES, DAOS_IOD_ARRAY);
		rc = fetch_seismic_entry(trace_hdr_obj.oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching trace headers failed, error"
			    " code = %d \n", rc);
			return;
		}
		/** close header object */
		daos_obj_close(trace_hdr_obj.oh, NULL);
		temp_trace.trace_header_obj = oids[i];
		add_trace_header(&temp_trace, &(traces_metadata->traces_list),
				 &(traces_metadata->ensembles_list), i);
	}
}

void
sort_headers(read_traces *gather_traces, char **sort_key, int *direction,
	     int number_of_keys)
{

	MergeSort(gather_traces->traces, 0,
		  gather_traces->number_of_traces - 1,
		  sort_key, direction, number_of_keys);
}

char*
get_dkey(char *key)
{
	int		i;
	for(i=0; i<SEIS_NKEYS; i++) {
		if(strcmp(key, hdr[i].key) == 0) {
			return hdr[i].key;
		}
	}
}

void
set_traces_header(daos_handle_t coh, int daos_mode, traces_list_t **head,
		  int num_of_keys, char **keys_1, char **keys_2, char **keys_3,
		  double *a, double *b, double *c, double *d, double *e,
		  double *f, double *j, header_operation_type_t type)
{
	trace_node_t 	*current;
	trace_oid_oh_t 		 trace_hdr_obj;
	cwp_String 		 type_key1[num_of_keys];
	cwp_String 		 type_key2[num_of_keys];
	cwp_String 		 type_key3[num_of_keys];
	int 			 itr;
	int 			 rc;
	int 			 i;

	current = (*head)->head;

	if (current == NULL) {
		warn("linked list of traces is empty\n");
		return;
	} else {
		itr = 0;
		while (current != NULL) {
			trace_hdr_obj.oid =
					current->trace.trace_header_obj;
			rc = daos_obj_open(coh, trace_hdr_obj.oid,
					   daos_mode, &(trace_hdr_obj.oh),
					   NULL);
			if (rc != 0) {
				err("Opening trace header object failed, error"
				    " code = %d \n", rc);
				return;
			}
			for (i = 0; i < num_of_keys; i++) {
				type_key1[i] = hdtype(keys_1[i]);
				if (type == 0) {
					calculate_new_header_value(current,
								   keys_1[i],
								   NULL, NULL,
								   a[i], b[i],
								   c[i], d[i],
								   0, 0, j[i],
								   itr, type,
								   type_key1[i]
								   , NULL, NULL
								   );
				} else {
					type_key2[i] = hdtype(keys_2[i]);
					type_key3[i] = hdtype(keys_3[i]);
					calculate_new_header_value(current,
								   keys_1[i],
								   keys_2[i],
								   keys_3[i],
								   a[i], b[i],
								   c[i], d[i],
								   e[i], f[i],
								   0, 0, type,
								   type_key1[i]
								   ,type_key2[i],
								   type_key3[i]
								   );
				}
			}
			rc = trace_header_update(&trace_hdr_obj,
						 &(current->trace),
						 TRACEHDR_BYTES);
			if (rc != 0) {
				err("Updating trace header failed error"
				    " code = %d \n", rc);
				return;
			}
			rc = daos_obj_close(trace_hdr_obj.oh, NULL);
			if(rc !=0) {
				err("Closing trace header object failed"
				    " error code = %d \n", rc);
				return;
			}
			itr++;
			current = current->next_trace;
		}
	}
}

void
window_headers(traces_list_t **head, char **window_keys,
	       int number_of_keys, cwp_String *type,
	       Value *min_keys, Value *max_keys)
{
	trace_node_t 		*current;
	trace_node_t 		*previous;
	Value 			 val;
	int 			 i;
	int 			 break_loop;

	current = (*head)->head;
	previous = NULL;

	if (current == NULL) {
		warn("Linked list of traces headers is empty.\n");
		return;
	}
	while (current != NULL) {
		break_loop = 0;
		for (i = 0; i < number_of_keys && break_loop == 0; i++) {
			/** get the trace header value */
			get_header_value(current->trace, window_keys[i], &val);
			/** check the value of trace header if it falls
			 *  within the min and max values or not
			 *  if yes the continue to check the value
			 *  of trace header for the next key
			 *  else, delete the trace header from
			 *  the linked list of headers.			 *
			 */
			if (!(valcmp(type[i], val, min_keys[i]) == 1 ||
			      valcmp(type[i], val, min_keys[i]) == 0) ||
			    !(valcmp(type[i], val, max_keys[i]) == -1 ||
			      valcmp(type[i], val, max_keys[i]) == 0)) {
				if (current == (*head)->head) {
					(*head)->head =
						(*head)->head->next_trace;
					free(current);
					current = (*head)->head;
				} else {
					previous->next_trace =
							current->next_trace;
					free(current);
					current = previous->next_trace;
				}
				/** if the trace header value doesn't
				 *  fall in the range of min and max values
				 *  then break the loop.
				 *  otherwise, move to the next node.
				 */
				break_loop = 1;
			}
		}
		if (break_loop == 1) {
			continue;
		} else {
			previous = current;
			current = current->next_trace;
		}
	}
}

char**
fetch_seismic_obj_dkeys(seis_obj_t *seismic_object, int sort, char *key,
		        int direction)
{
	daos_key_desc_t 	*kds;
	daos_anchor_t 		 anchor = {0};
	d_sg_list_t 		 sglo;
	d_iov_t 		 iov_temp;
	uint32_t 		 nr;
	char 			*temp_array;
	char 		       **dkeys_list;
	char 		       **unique_keys;
	int 			 temp_array_offset = 0;
	int 			 keys_read = 0;
	int 			 kds_i = 0;
	int 			 out;
	int 			 rc;
	int 			 z;
	/** temp arrays allocations */
	nr = seismic_object->number_of_gathers + 1;

	temp_array = malloc(nr * SEIS_MAX_KEY_LENGTH *
			    sizeof(char));
	kds = malloc((nr) * sizeof(daos_key_desc_t));
	dkeys_list = malloc((nr) * sizeof(char*));
	unique_keys = malloc(seismic_object->number_of_gathers * sizeof(char*));
	sglo.sg_nr_out = sglo.sg_nr = 1;	
	sglo.sg_iovs = &iov_temp;
	/** fetch list of dkeys */
	while (!daos_anchor_is_eof(&anchor)) {
		nr = seismic_object->number_of_gathers + 1 - keys_read;
		d_iov_set(&iov_temp, temp_array + temp_array_offset,
			  nr * SEIS_MAX_KEY_LENGTH);
		rc = daos_obj_list_dkey(seismic_object->oh, DAOS_TX_NONE, &nr,
					&kds[keys_read], &sglo, &anchor, NULL);
		for (kds_i = 0; kds_i < nr; kds_i++) {
			temp_array_offset += kds[keys_read + kds_i].kd_key_len;
		}
		keys_read += nr;
		
	}
	if (rc != 0) {
		err("Listing <%s> seismic object dkeys failed,"
		    " error code = %d\n", seismic_object->name, rc);
	}
	/** Copy dkeys from temp array to dkeys list
	 * then check if the key contain digits or not
	 * if yes, key is copied to array of unique keys(gather keys)
	 * otherwise it is ignored.
	 */
	int 			 digit;
	int			 off = 0;
	int 			 u = 0;
	int			 k;

	for (z = 0; z < keys_read; z++) {
		digit = 0;
		dkeys_list[z] = malloc((kds[z].kd_key_len + 1) * sizeof(char));
		strncpy(dkeys_list[z], &temp_array[off], kds[z].kd_key_len);
		dkeys_list[z][kds[z].kd_key_len] = '\0';
		off += kds[z].kd_key_len;
		for (k = 0; k < strlen(dkeys_list[z]) + 1; k++) {
			if (isdigit(dkeys_list[z][k])) {
				digit = 1;
				unique_keys[u] = malloc(kds[z].kd_key_len *
							sizeof(char));
				strcpy(unique_keys[u], dkeys_list[z]);
				u++;
				break;
			}
		}
		if(digit == 0) {
			out = z;
		}
	}
	/** check sorting flag, if yes then sort dkeys fetched
	 *  based on direction(ascending or descending
	 *  and return the sorted list
	 *  otherwise return the array of unique keys as it is.
	 */
	if (sort == 1) {
		char 	**dkeys_sorted_list;
		long	 *first_array;

		first_array = malloc(seismic_object->number_of_gathers
				     * sizeof(long));
		dkeys_sorted_list = malloc(seismic_object->number_of_gathers
					   * sizeof(char*));

		sort_dkeys_list(first_array, seismic_object->number_of_gathers,
				unique_keys, direction);

		k = 0;
		for (z = 0; z < seismic_object->number_of_gathers; z++) {
			if (k == out) {
				k++;
			}
			dkeys_sorted_list[z] = malloc(kds[k].kd_key_len *
						      sizeof(char));
			char dkey_name[200] = "";
			char temp_st[200] = "";
			strcat(dkey_name, get_dkey(key));
			strcat(dkey_name, KEY_SEPARATOR);
			sprintf(temp_st, "%ld", first_array[z]);
			strcat(dkey_name, temp_st);
			strcpy(dkeys_sorted_list[z], dkey_name);
			k++;
		}	
		free(first_array);
		return dkeys_sorted_list;
	}
	/** free allocated memory */
	for(z=0; z<seismic_object->number_of_gathers +1; z++) {
		free(dkeys_list[z]);
	}
	free(temp_array);
	free(kds);
	free(dkeys_list);

	return unique_keys;
}

void
replace_seismic_objects(dfs_t *dfs, int daos_mode, char *key,
			traces_list_t *trace_list, seis_root_obj_t *root)
{
	seismic_entry_t 	seismic_entry = {0};
	seis_obj_t 	       *new_seis_obj;
	seis_obj_t 	       *existing_obj;
	int 			index;
	int 			rc;
	int 			i;

	existing_obj = malloc(sizeof(seis_obj_t));

	for(i = 0; i < root->num_of_keys; i++) {
		if(strcmp(root->keys[i],key) == 0) {
			existing_obj->oid = root->gather_oids[i];
			index = i;
			break;
		}
	}

	rc = daos_obj_open(dfs->coh, existing_obj->oid, daos_mode,
			&(existing_obj->oh), NULL);
	if (rc != 0) {
		err("Opening seismic object failed error code = %d \n", rc);
		return;
	}
	/** Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, existing_obj->oid, DS_D_NGATHERS,
			      DS_A_NGATHERS,
			      (char*) &(existing_obj->number_of_gathers),
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = fetch_seismic_entry(existing_obj->oh, DAOS_TX_NONE,
			 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		return;
	}
	char **temp_keys;
	temp_keys = fetch_seismic_obj_dkeys(existing_obj, 0, key, 1);

	/** Destroy all trace headers oids objects in existing object */
	for (i = 0; i < existing_obj->number_of_gathers; i++) {
		trace_oid_oh_t 		temp;

		prepare_seismic_entry(&seismic_entry, existing_obj->oid,
				      temp_keys[i], DS_A_GATHER_TRACE_OIDS,
				      (char*) &temp.oid,
				      sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = fetch_seismic_entry(existing_obj->oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		rc = daos_array_open_with_attr(dfs->coh, temp.oid,
					       DAOS_TX_NONE, DAOS_OO_RW, 1,
					       500 * sizeof(daos_obj_id_t),
					       &(temp.oh), NULL);
		if (rc != 0) {
			err("Opening array object with attr() failed, error"
			    " code = %d \n", rc);
			return;
		}
		rc = daos_array_destroy(temp.oh, DAOS_TX_NONE, NULL);
		if (rc != 0) {
			err("Destroying array object failed, "
			    "error code = %d \n", rc);
			return;
		}
		rc = daos_array_close(temp.oh, NULL);
		if (rc != 0) {
			err("Closing array object failed, "
			    "error code = %d \n", rc);
			return;
		}
		free(temp_keys[i]);
	}
	free(temp_keys);
	/** Punch exisiting object */
	rc = daos_obj_punch(existing_obj->oh, DAOS_TX_NONE, 0, NULL);
	if (rc != 0) {
		err("Punching existing seismic object failed, "
		    "error code = %d \n", rc);
		return;
	}
	/** Create new seismic object based on key type*/
	rc = seismic_gather_obj_create(dfs, OC_SX, root,
				       &new_seis_obj, key, index);

	if (rc != 0) {
		err("Creating new seismic object failed, "
		    "error code = %d \n", rc);
		return;
	}
	/** Start linking trace list to the created gather object */
	trace_node_t 		*current = trace_list->head;
	while (current != NULL) {
		trace_obj_t *trace_obj = malloc(sizeof(trace_obj_t));
		trace_obj->trace = malloc(sizeof(trace_t));

		memcpy(trace_obj->trace, &(current->trace), sizeof(trace_t));

		trace_obj->oid = current->trace.trace_header_obj;

		rc = trace_linking(trace_obj, new_seis_obj,
				   root->keys[index]);
		if (rc != 0) {
			err("Linking trace to <%s> gather object failed,"
			    " error code = %d \n",root->keys[index], rc);
			return;
		}

		current = current->next_trace;
		free(trace_obj->trace);
		free(trace_obj);
	}
	/** Update new object number of gathers key */
	rc = update_seismic_gather_object(new_seis_obj, DS_D_NGATHERS,
					  DS_A_NGATHERS,
					  (char*) &new_seis_obj->number_of_gathers,
					  sizeof(int), DAOS_IOD_SINGLE);
	if (rc != 0) {
		err("Adding number of gathers failed, "
		    "error code = %d \n", rc);
		return;
	}
	/** Create new trace oids object */
	rc = trace_oids_obj_create(dfs, OC_SX, new_seis_obj,0);
	if (rc != 0) {
		err("Creating new trace oids object failed, "
		    "error code = %d \n", rc);
		return;
	}
	/** Update gather keys */
	rc = update_gather_data(dfs, new_seis_obj->gathers,
				new_seis_obj, get_dkey(root->keys[index]));
	if (rc != 0) {
		err("Updating gather keys failed, "
		    "error code = %d \n", rc);
		return;
	}
	/** close new object */
	daos_obj_close(new_seis_obj->oh, NULL);
	free(existing_obj);
}

headers_ranges_t
range_traces_headers(traces_list_t *trace_list, int number_of_keys,
		     char **keys, int dim)
{
	trace_node_t	 	*current;
	headers_ranges_t	headers_ranges;
	trace_t 		*trmin;
	trace_t 		*trmax;
	trace_t 		*trfirst;
	trace_t 		*trlast;
	double 			east_shot[2];
	double 			west_shot[2];
	double			north_shot[2];
	double			south_shot[2];
	double 			east_rec[2];
	double			west_rec[2];
	double			north_rec[2];
	double			south_rec[2];
	double 			east_cmp[2];
	double			west_cmp[2];
	double			north_cmp[2];
	double			south_cmp[2];
	double 			dcoscal = 1.0;
	double 			sx = 0.0;
	double			sy = 0.0;
	double			gx = 0.0;
	double			gy = 0.0;
	double			mx = 0.0;
	double			my = 0.0;
	double 			mx1 = 0.0;
	double			my1 = 0.0;
	double 			mx2 = 0.0;
	double			my2 = 0.0;
	double			dm = 0.0;
	double			dmin = 0.0;
	double			dmax = 0.0;
	double			davg = 0.0;
	int 			coscal = 1;
	Value 			 val;
	Value 			 valmin;
	Value 			 valmax;
	int 			 i;

	current = trace_list->head;
	trmin = malloc(sizeof(trace_t));
	trmax = malloc(sizeof(trace_t));
	trfirst = malloc(sizeof(trace_t));
	trlast = malloc(sizeof(trace_t));

	north_shot[0] = south_shot[0] = east_shot[0] = west_shot[0] = 0.0;
	north_shot[1] = south_shot[1] = east_shot[1] = west_shot[1] = 0.0;
	north_rec[0] = south_rec[0] = east_rec[0] = west_rec[0] = 0.0;
	north_rec[1] = south_rec[1] = east_rec[1] = west_rec[1] = 0.0;
	north_cmp[0] = south_cmp[0] = east_cmp[0] = west_cmp[0] = 0.0;
	north_cmp[1] = south_cmp[1] = east_cmp[1] = west_cmp[1] = 0.0;

	if (number_of_keys == 0) {
		for (i = 0; i < SEIS_NKEYS; i++) {
			get_header_value(current->trace, keys[i], &val);
			set_header_value(trmin, keys[i], &val);
			set_header_value(trmax, keys[i], &val);
			set_header_value(trfirst, keys[i], &val);

			if (i == 20) {
				coscal = val.h;
				if (coscal == 0) {
					coscal = 1;
				} else if (coscal > 0) {
					dcoscal = 1.0 * coscal;
				} else {
					dcoscal = 1.0 / coscal;
				}
			} else if (i == 21) {
				sx = east_shot[0] = west_shot[0] = north_shot[0] =
						south_shot[0] = val.i * dcoscal;
			} else if (i == 22) {
				sy = east_shot[1] = west_shot[1] = north_shot[1] =
						south_shot[1] = val.i * dcoscal;
			} else if (i == 23) {
				gx = east_rec[0] = west_rec[0] = north_rec[0] =
						south_rec[0] = val.i * dcoscal;
			} else if (i == 24) {
				gy = east_rec[1] = west_rec[1] = north_rec[1] =
						south_rec[1] = val.i * dcoscal;
			} else {
				continue;
			}
		}
	} else {
		for (i = 0; i < number_of_keys; i++) {
			get_header_value(current->trace, keys[i], &val);
			set_header_value(trmin, keys[i], &val);
			set_header_value(trmax, keys[i], &val);
			set_header_value(trfirst, keys[i], &val);
		}
	}
	if (number_of_keys == 0) {
		mx = east_cmp[0] = west_cmp[0] = north_cmp[0] = south_cmp[0] = 0.5 *
				  (east_shot[0] + east_rec[0]);
		my = east_cmp[1] = west_cmp[1] = north_cmp[1] = south_cmp[1] = 0.5 *
				  (east_shot[1] + east_rec[1]);
	}

	int 		ntr = 1;
	current = current->next_trace;

	while (current != NULL) {
		sx = sy = gx = gy = mx = my = 0.0;
		if (number_of_keys == 0) {
			for (i = 0; i < SEIS_NKEYS; i++) {
				get_header_value(current->trace, keys[i],
						 &val);
				get_header_value(*trmin, keys[i], &valmin);
				get_header_value(*trmax, keys[i], &valmax);

				if (valcmp(hdr[i].type, val, valmin) < 0) {
					set_header_value(trmin, keys[i], &val);
				} else if (valcmp(hdr[i].type, val, valmax) >
					   0) {
					set_header_value(trmax, keys[i], &val);
				}

				set_header_value(trlast, keys[i], &val);

				if (i == 20) {
					coscal = val.h;
					if (coscal == 0) {
						coscal = 1;
					} else if (coscal > 0) {
						dcoscal = 1.0 * coscal;
					} else {
						dcoscal = 1.0 / coscal;
					}
				} else if (i == 21) {
					sx = val.i * dcoscal;
				} else if (i == 22) {
					sy = val.i * dcoscal;
				} else if (i == 23) {
					gx = val.i * dcoscal;
				} else if (i == 24) {
					gy = val.i * dcoscal;
				} else {
					continue;
				}
			}
		} else {
			for (i = 0; i < number_of_keys; i++) {
				get_header_value(current->trace, keys[i],
						 &val);
				get_header_value(*trmin, keys[i], &valmin);
				get_header_value(*trmax, keys[i], &valmax);
				if (valcmp(hdtype(keys[i]), val, valmin) < 0) {
					set_header_value(trmin, keys[i], &val);
				} else if (valcmp(hdtype(keys[i]), val, valmax)
						> 0) {
					set_header_value(trmax, keys[i], &val);
				}
				set_header_value(trlast, keys[i], &val);
			}
		}

		if (number_of_keys == 0) {
			mx = 0.5 * (sx + gx);
			my = 0.5 * (sy + gy);
			if (east_shot[0] < sx) {
				east_shot[0] = sx;
				east_shot[1] = sy;
			}
			if (west_shot[0] > sx) {
				west_shot[0] = sx;
				west_shot[1] = sy;
			}
			if (north_shot[1] < sy) {
				north_shot[0] = sx;
				north_shot[1] = sy;
			}
			if (south_shot[1] > sy) {
				south_shot[0] = sx;
				south_shot[1] = sy;
			}
			if (east_rec[0] < gx) {
				east_rec[0] = gx;
				east_rec[1] = gy;
			}
			if (west_rec[0] > gx) {
				west_rec[0] = gx;
				west_rec[1] = gy;
			}
			if (north_rec[1] < gy) {
				north_rec[0] = gx;
				north_rec[1] = gy;
			}
			if (south_rec[1] > gy) {
				south_rec[0] = gx;
				south_rec[1] = gy;
			}
			if (east_cmp[0] < mx) {
				east_cmp[0] = mx;
				east_cmp[1] = my;
			}
			if (west_cmp[0] > mx) {
				west_cmp[0] = mx;
				west_cmp[1] = my;
			}
			if (north_cmp[1] < my) {
				north_cmp[0] = mx;
				north_cmp[1] = my;
			}
			if (south_cmp[1] > my) {
				south_cmp[0] = mx;
				south_cmp[1] = my;
			}
		}

		if (ntr == 1) {
			/** get midpoint (mx1,my1) on trace 1 */
			mx1 = 0.5 * (current->trace.sx + current->trace.gx);
			my1 = 0.5 * (current->trace.sy + current->trace.gy);
		} else if (ntr == 2) {
			/** get midpoint (mx2,my2) on trace 2 */
			mx2 = 0.5 * (current->trace.sx + current->trace.gx);
			my2 = 0.5 * (current->trace.sy + current->trace.gy);
			/** midpoint interval between traces 1 and 2 */
			dm = sqrt((mx1 - mx2) * (mx1 - mx2) +
				  (my1 - my2) * (my1 - my2));
			/** set min, max and avg midpoint interval holders */
			dmin = dm;
			dmax = dm;
			davg = (dmin + dmax) / 2.0;
			/* hold this midpoint */
			mx1 = mx2;
			my1 = my2;
		} else if (ntr > 2) {
			/** get midpoint (mx,my) on this trace */
			mx2 = 0.5 * (current->trace.sx + current->trace.gx);
			my2 = 0.5 * (current->trace.sy + current->trace.gy);
			/** get midpoint (mx,my) between this
			 * and previous trace
			 */
			dm = sqrt((mx1 - mx2) * (mx1 - mx2) +
				  (my1 - my2) * (my1 - my2));
			/** reset min, max and avg midpoint interval holders,
			 *  if needed
			 */
			if (dm < dmin)
				dmin = dm;
			if (dm > dmax)
				dmax = dm;
			davg = (davg + (dmin + dmax) / 2.0) / 2.0;
			/* hold this midpoint */
			mx1 = mx2;
			my1 = my2;
		}
		ntr++;
		current = current->next_trace;
	}


	headers_ranges.east_cmp[0] = east_cmp[0];
	headers_ranges.east_cmp[1] = east_cmp[1];
	headers_ranges.east_rec[0] = east_rec[0];
	headers_ranges.east_rec[1] = east_rec[1];
	headers_ranges.east_shot[0] = east_shot[0];
	headers_ranges.east_shot[1] = east_shot[1];
	headers_ranges.north_cmp[0] = north_cmp[0];
	headers_ranges.north_cmp[1] = north_cmp[1];
	headers_ranges.north_rec[0] = north_rec[0];
	headers_ranges.north_rec[1] = north_rec[1];
	headers_ranges.north_shot[0] = north_shot[0];
	headers_ranges.north_shot[1] = north_shot[1];
	headers_ranges.south_cmp[0] = south_cmp[0];
	headers_ranges.south_cmp[1] = south_cmp[1];
	headers_ranges.south_rec[0] = south_rec[0];
	headers_ranges.south_rec[1] = south_rec[1];
	headers_ranges.south_shot[0] = south_shot[0];
	headers_ranges.south_shot[1] = south_shot[1];
	headers_ranges.west_cmp[0] = west_cmp[0];
	headers_ranges.west_cmp[1] = west_cmp[1];
	headers_ranges.west_rec[0] = west_rec[0];
	headers_ranges.west_rec[1] = west_rec[1];
	headers_ranges.west_shot[0] = west_shot[0];
	headers_ranges.west_rec[1] = west_rec[1];
	headers_ranges.number_of_keys = number_of_keys;
	headers_ranges.trfirst = trfirst;
	headers_ranges.trlast = trlast;
	headers_ranges.trmax = trmax;
	headers_ranges.trmin = trmin;
	headers_ranges.keys = keys;
	headers_ranges.davg = davg;
	headers_ranges.dmax = dmax;
	headers_ranges.dmin = dmin;
	headers_ranges.ntr = ntr;
	headers_ranges.dim = dim;

	print_headers_ranges(headers_ranges);

	return headers_ranges;
}

int
fetch_array_of_trace_headers_oids(seis_root_obj_t *root, daos_obj_id_t *oids,
				  trace_oid_oh_t *gather_oid_oh,
				  int number_of_traces)
{
	seismic_entry_t 	seismic_entry = {0};
	daos_array_iod_t 	iod;
	daos_range_t 		rg;
	d_sg_list_t 		sgl;
	d_iov_t 		iov;
	int 			offset;
	int 			rc;

	/** open array object */
	rc = daos_array_open_with_attr(root->coh, gather_oid_oh->oid,
				       DAOS_TX_NONE, DAOS_OO_RW, 1,
				       500 * sizeof(daos_obj_id_t),
				       &gather_oid_oh->oh, NULL);
	if (rc != 0) {
		err("Opening array object with attr() failed, error"
		    " code = %d \n", rc);
		return rc;
	}
	/** set scatter gather list and IO descriptor */
	d_iov_set(&iov, (void*)(char*)oids,number_of_traces * sizeof(daos_obj_id_t));
	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	offset = 0;
	sgl.sg_iovs = &iov;
	iod.arr_nr = 1;
	rg.rg_len = number_of_traces * sizeof(daos_obj_id_t);
	rg.rg_idx = offset;
	iod.arr_rgs = &rg;

	rc = daos_array_read(gather_oid_oh->oh,
			     DAOS_TX_NONE, &iod, &sgl, NULL);
	if (rc != 0) {
		err("Reading gather oids array failed, error"
		    " code = %d \n", rc);
		return rc;
	}

	rc = daos_array_close(gather_oid_oh->oh,
			      NULL);
	if (rc != 0) {
		err("Closing array object failed, error"
		    " code = %d \n", rc);
		return rc;
	}

	return rc;
}

void
release_gathers_list(gathers_list_t *gather_list){
	seis_gather_t	 	*temp;
	seis_gather_t	 	*next;

	temp = gather_list->head;

	while(temp != NULL ){
		next = temp->next_gather;
		free(temp);
		temp = next;
	}
	free(gather_list);
}

void
read_headers(bhed *bh, char *ebcbuf, short *nextended,
	     seis_root_obj_t *root_obj, DAOS_FILE *daos_tape,
	     int swapbhed, int endian)
{
	daos_size_t 		size;
	tapebhed 		tapebh;
	char		       *temp_name;
	int 			over;
	int 			rc;
	int 			i;

	/* flag for bhed.float override*/
	/* Override binary format value */
	over = 0;
	/** Read_text_header */
	size = read_dfs_file(daos_tape, ebcbuf, SEIS_EBCBYTES);
	/** Read_binary_header */
	size = read_dfs_file(daos_tape, (char*) &tapebh, SEIS_BNYBYTES);
	/* Convert from bytes to ints/shorts */
	tapebhed_to_bhed(&tapebh, bh);
	/* if little endian machine, swap bytes in binary header */
	if (swapbhed == 0) {
		for (i = 0; i < BHED_NKEYS; ++i) {
			swapbhval(bh, i);
		}
	}
	*nextended = *((short*) (((unsigned char*) &tapebh) + 304));
	if (endian == 0) {
		swap_short_2(nextended);
	}
	warn("Number of extended text headers: %d", *nextended);
}

void
write_headers(bhed bh, char *ebcbuf, seis_root_obj_t *root_obj)
{
	char 		*tbuf;
	int 		 read_bytes_from_command;
	int 		 ebcdic;
	int 		 rc;

	/* ebcdic to ascii conversion flag	*/
	ebcdic = 1;

	/* Open pipe to use dd to convert  ebcdic to ascii */
	/* this command gives a file containing 3240 bytes on sun */
	/* see top of Makefile.config for versions */
	/* not sure why this breaks now; works in version 37 */
	tbuf = malloc(SEIS_EBCBYTES * sizeof(char));

	if (ebcdic == 1) {
		char *arr[] ={"dd", "ibs=1", "conv=ascii", "count=3200", NULL};
		read_bytes_from_command = execute_command(arr, ebcbuf,
							  SEIS_EBCBYTES, tbuf,
							  SEIS_EBCBYTES);
	} else {
		char *arr[] = { "dd", "ibs=1", "count=3200", NULL };
		read_bytes_from_command = execute_command(arr, ebcbuf,
							  SEIS_EBCBYTES, tbuf,
							  SEIS_EBCBYTES);
	}
	/** Update text header under root seismic object */
	rc = seismic_root_obj_update(root_obj, DS_D_FILE_HEADER,
				     DS_A_TEXT_HEADER, tbuf, SEIS_EBCBYTES,
				     DAOS_IOD_ARRAY);
	if (rc != 0) {
		err("Updating text header of root seismic object failed, "
		    "error code = %d \n",rc);
		return;
	}
	/** Update binary header under root seismic object */
	rc = seismic_root_obj_update(root_obj, DS_D_FILE_HEADER,
				     DS_A_BINARY_HEADER, (char*)&bh,
				     SEIS_BNYBYTES, DAOS_IOD_ARRAY);
	if (rc != 0) {
		err("Updating binary header of root seismic object failed, "
		    "error code = %d \n",rc);
		return;
	}
}

void
parse_exth(short nextended, DAOS_FILE *daos_tape, char *ebcbuf,
	   seis_root_obj_t *root_obj)
{
	daos_size_t		size;
	int 			rc;
	int 			i;

	rc = seismic_root_obj_update(root_obj, DS_D_FILE_HEADER,
				     DS_A_NEXTENDED_HEADER, (char*)&nextended,
				     sizeof(short), DAOS_IOD_SINGLE);
	if (rc != 0) {
		err("Updating number of EXTH of root seismic object failed, "
		    "error code = %d \n",rc);
		return;
	}
	if (nextended > 0)
	{
		/* need to deal with -1 nextended headers
		 * so test should actually be !=0, but ...
		 */
		for (i = 0; i < nextended; i++) {
			/* cheat -- an extended text header is same size as
			 * EBCDIC header.
			 * Read the bytes from the tape for one xhdr into the
			 * buffer.
			 */
			size = read_dfs_file(daos_tape, ebcbuf, SEIS_EBCBYTES);
			/** write the data in ebcbuf under extended text
			 *  header key.
			 */
			char 		akey_extended[200] = "";
			char 		akey_index[100];
			sprintf(akey_index, "%d", i);
			strcat(akey_extended, DS_A_EXTENDED_HEADER);
			strcat(akey_extended, akey_index);

			rc = seismic_root_obj_update(root_obj, DS_D_FILE_HEADER,
						     akey_extended, ebcbuf,
						     SEIS_EBCBYTES,DAOS_IOD_ARRAY);
			if (rc != 0) {
				err("Updating extended header of root seismic"
				    " object failed, error code = %d \n",rc);
				return;
			}
		}
	}
}

void
process_headers(bhed *bh, int format, int over, cwp_Bool format_set, int *trcwt,
		int verbose, int *ns, size_t *nsegy)
{
	/* Override binary format value */
	over = 0;
	if (((over != 0) && (format_set))) {
		bh->format = format;
	}
	/* Override application of trace weighting factor?
	 *
	 * Default no for floating point formats, yes for integer formats.
	 */
	*trcwt = (bh->format == 1 || bh->format == 5) ? 0 : 1;

	switch (bh->format) {
	case 1:
		if (verbose) {
			warn("assuming IBM floating point input");
		}
		break;
	case 2:
		if (verbose) {
			warn("assuming 4 byte integer input");
		}
		break;
	case 3:
		if (verbose) {
			warn("assuming 2 byte integer input");
		}
		break;
	case 5:
		if (verbose) {
			warn("assuming IEEE floating point input");
		}
		break;
	case 8:
		if (verbose) {
			warn("assuming 1 byte integer input");
		}
		break;
	default:
		if (over) {
			warn("ignoring bh.format ... continue");
		} else {
			err("format not SEGY standard (1, 2, 3, 5, or 8)");
		}

	}

	/* Compute length of trace (can't use sizeof here!) */
	*ns = bh->hns; /* let user override */
	if (!(*ns)) {
		err("samples/trace not set in binary header");
	}
	bh->hns = *ns;

	switch (bh->format) {
	case 8:
		*nsegy = *ns + SEGY_HDRBYTES;
		break;
	case 3:
		*nsegy = *ns * 2 + SEGY_HDRBYTES;
		break;
	case 1:
	case 2:
	case 5:
	default:
		*nsegy = *ns * 4 + SEGY_HDRBYTES;
	}
}

void
process_trace(tapesegy tapetr, segy *tr, bhed bh, int ns, int swaphdrs,
	      int nsflag, int *itr, int nkeys, cwp_String *type1,
	      cwp_String *type2, int *ubyte, int endian, int conv,
	      int swapdata, int *index1, int trmin, int trcwt,
	      int verbose)
{
	Value 		val1;
	int 		ikey;
	int 		i;

	/* Convert from bytes to ints/shorts */
	tapesegy_to_segy(&tapetr, tr);
	/* If little endian machine, then swap bytes in trace header */
	if (swaphdrs == 0) {
		for (i = 0; i < SEGY_NKEYS; ++i) {
			swaphval(tr, i);
		}
	}
	/* Check tr.ns field */
	if (!nsflag && ns != tr->ns) {
		int temp_itr = *itr + 1;
		warn("discrepant tr.ns = %d with tape/user ns = %d\n\t"
		     "... first noted on trace %d",
		     tr->ns, ns, temp_itr);
		nsflag = cwp_true;
	}
	/* loop over key fields and remap */
	for (ikey = 0; ikey < nkeys; ++ikey) {
		/* get header values */
		ugethval(type1[ikey], &val1, type2[ikey],
			 ubyte[ikey] - 1, (char*) &tapetr,
			 endian, conv, verbose);
		puthval(tr, index1[ikey], &val1);
	}
	/* Are there different swapping instructions for the data
	 *
	 * Convert and write desired traces
	 */
	if (++(*itr) >= trmin) {
		/* Convert IBM floats to native floats */
		if (conv) {
			switch (bh.format) {
			case 1:
				/* Convert IBM float to native float*/
				ibm_to_float((int*) tr->data,
					     (int*) tr->data, ns,
					     swapdata, verbose);
				break;
			case 2:
				/* Convert 4 byte integer to native float*/
				int_to_float((int*) tr->data,
					     (float*) tr->data, ns,
					     swapdata);
				break;
			case 3:
				/* Convert 2 byte integer to native float*/
				short_to_float((short*) tr->data,
						(float*) tr->data, ns,
						swapdata);
				break;
			case 5:
				/* IEEE floats.
				 * Byte swap if necessary.
				 */
				if (swapdata == 0)
					for (i = 0; i < ns; ++i) {
						swap_float_4(&tr->
							     data[i]);
					}
				break;
			case 8:
				/*Convert 1 byte integer to native float*/
				integer1_to_float((signed char*)tr->data,
						  (float*) tr->data,
						  ns);
				break;
			}
			/* Apply trace weighting. */
			if (trcwt && tr->trwf != 0) {
				float scale = pow(2.0, -tr->trwf);
				//int i;
				for (i = 0; i < ns; ++i) {
					tr->data[i] *= scale;
				}
			}
		} else if (conv == 0) {
			/* don't convert, if not appropriate */

			switch (bh.format) {
			case 1: /* swapdata=0 byte swapping */
			case 5:
				if (swapdata == 0) {
					for (i = 0; i < ns; ++i) {
						swap_float_4(&tr->data[i]);
					}
				}
				break;
			case 2: /* convert longs to floats */
				/* SU has no provision for reading */
				/* data as longs */
				int_to_float((int*) tr->data,
					     (float*)tr->data,
					     ns, endian);
				break;
			case 3: /* shorts are the SHORTPAC format */
				/* used by supack2 and suunpack2 */
				if (swapdata == 0)/* swapdata=0 byte swap */
					for (i = 0; i < ns; ++i) {
						swap_short_2((short*)
							     &tr->
							     data[i]);
					}
				/* Set trace ID to SHORTPACK format */
				tr->trid = SHORTPACK;
				break;
			case 8: /* convert bytes to floats */
				/* SU has no provision for reading */
				/* data as bytes */
				integer1_to_float((signed char*)tr->data
						  ,(float*)tr->data,
						  ns);
				break;
			}
		}
		/* Write the trace to disk */
		tr->ns = ns;
	}
}

void
read_object_gathers(seis_root_obj_t *root, seis_obj_t *seis_obj){
	seismic_entry_t 	seismic_entry = {0};
	seis_gather_t 		temp_gather;
	int 			rc;
	int 			i;

	/**Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seis_obj->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*)&seis_obj->number_of_gathers,
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = fetch_seismic_entry(seis_obj->oh, DAOS_TX_NONE,
			 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		return;
	}
	/** Fetch list of dkeys stored under gather object */
	char **unique_keys = fetch_seismic_obj_dkeys(seis_obj, 1,
						     seis_obj->name, 1);
	seis_obj->seis_gather_trace_oids_obj =
				malloc(seis_obj->number_of_gathers *
				       sizeof(trace_oid_oh_t));
	for (i = 0; i < seis_obj->number_of_gathers; i++) {
 		/** Fetch number of traces */
		prepare_seismic_entry(&seismic_entry, seis_obj->oid,
				      unique_keys[i], DS_A_NTRACES,
				      (char*)&(temp_gather.number_of_traces),
				      sizeof(int), DAOS_IOD_SINGLE);
		rc = fetch_seismic_entry(seis_obj->oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			return;
		}
		/** Fetch unique value */
		prepare_seismic_entry(&seismic_entry, seis_obj->oid,
				      unique_keys[i], DS_A_UNIQUE_VAL,
				      (char*)&(temp_gather.unique_key),
				      sizeof(long), DAOS_IOD_SINGLE);
		rc = fetch_seismic_entry(seis_obj->oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching unique key value failed, error "
			    "code = %d \n", rc);
			return;
		}
		/** Fetch oid of traces headers array object */
		prepare_seismic_entry(&seismic_entry, seis_obj->oid,
				      unique_keys[i], DS_A_GATHER_TRACE_OIDS,
				      (char*)
				      &((seis_obj->seis_gather_trace_oids_obj[i]).oid),
				      sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = fetch_seismic_entry(seis_obj->oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching traces headers oid failed, error "
			    "code = %d \n", rc);
			return;
		}
		/** Allocate oids array , size = number of traces */
		temp_gather.oids = malloc(temp_gather.number_of_traces *
					   sizeof(daos_obj_id_t));
		/** Fetch array of trace headers oids*/
		rc = fetch_array_of_trace_headers_oids(root, temp_gather.oids,
						       &(seis_obj->seis_gather_trace_oids_obj[i]),
						       temp_gather.number_of_traces);
		if(rc != 0) {
			err("Fetching array of traces headers oids failed, error"
			    " code = %d \n", rc);
			return;
		}
		add_gather(&temp_gather, &(seis_obj->gathers),1);
		free(temp_gather.oids);
	}
}

void
release_traces_list(traces_list_t *trace_list)
{
	trace_node_t	 	*temp;
	trace_node_t	 	*next;

	temp = trace_list->head;

	while(temp != NULL ){
		next = temp->next_trace;
		if (temp->trace.data != NULL) {
			free(temp->trace.data);
		}
		free(temp);
		temp = next;
	}
	free(trace_list);
}

void
release_ensembles_list(ensembles_list_t *ensembles_list)
{
	ensemble_node_t 	*temp;
	ensemble_node_t 	*next;

	temp = ensembles_list->first_ensemble;

	while(temp != NULL) {
		next = temp->next_ensemble;
		free(temp);
		temp = next;
	}

	free(ensembles_list);
}
