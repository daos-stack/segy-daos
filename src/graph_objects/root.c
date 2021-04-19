/*
 * root.c
 *
 *  Created on: Feb 2, 2021
 *      Author: mirnamoawad
 */
#include "graph_objects/root.h"

int
root_obj_create(root_obj_t **root_obj, int flags, char **keys, int num_of_keys,
		mode_t permissions_and_type, seismic_object_oid_oh_t *parent)
{
	int rc;
	int i;

	rc = 0;

	(*root_obj) = malloc(sizeof(root_obj_t));
	if ((*root_obj) == NULL) {
		return ENOMEM;
	}

	rc = oid_gen(DAOS_OBJ_CLASS_ID, false, &((*root_obj)->oid_oh.oid),
			false);
	DSG_ERROR(rc,"Generating Object id for root object failed \n", end);

	if (num_of_keys > 0) {
		(*root_obj)->keys = malloc(num_of_keys * sizeof(char*));

		for (i = 0; i < num_of_keys; i++) {
			(*root_obj)->keys[i] = malloc(MAX_KEY_LENGTH *
						      sizeof(char));
			strcpy((*root_obj)->keys[i], keys[i]);
		}
	} else {
		(*root_obj)->keys = NULL;
	}

	(*root_obj)->num_of_keys = num_of_keys;
	(*root_obj)->num_of_traces = 0;
	(*root_obj)->nexth = 0;
	(*root_obj)->daos_mode = get_daos_obj_mode(flags);
	(*root_obj)->permissions_and_type = permissions_and_type;
	(*root_obj)->gather_oids = NULL;
	(*root_obj)->io_parameters = NULL;
	(*root_obj)->nvariations = 0;
	(*root_obj)->variations = NULL;
	if (parent == NULL) {
		(*root_obj)->parent_oid_oh.oid = get_dfs()->root.oid;
		(*root_obj)->parent_oid_oh.oh = get_dfs()->root.oh;
	} else {
		(*root_obj)->parent_oid_oh = *parent;
	}

	return rc;

end:
	free((*root_obj));
	return rc;
}

int
root_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
	      operations_controllers_t *op_controller, int parent_idx,
	      int *curr_idx)
{
	int rc = 0;
	rc = open_object(oid_oh, op_controller, get_dfs()->coh, mode,
			 parent_idx, curr_idx);
	DSG_ERROR(rc,"Opening root object failed \n");

	return rc;
}

int
root_obj_close(root_obj_t *root_obj, operations_controllers_t *op_controller,
	       int parent_idx, int *curr_idx, int release_data)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = root_obj_get_id_oh(root_obj);

	rc = close_object(oid_oh, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Closing root object failed", end);
	if (release_data == 1) {
		rc = root_obj_release(root_obj);
		DSG_ERROR(rc,"Releasing Root object failed");
	}

end:
	return rc;
}

int
root_obj_release(root_obj_t *root_obj)
{
	int	 	rc = 0;
	int 		i;

	if (root_obj->gather_oids != NULL) {
		free(root_obj->gather_oids);
	}

	if (root_obj->io_parameters != NULL) {
		rc = root_obj_release_io_parameters(root_obj);
		DSG_ERROR(rc,"Releasing root io parameters failed");
	}

	if (root_obj->keys != NULL) {
		for (i = 0; i < root_obj->num_of_keys; i++) {
			free(root_obj->keys[i]);
		}
		free(root_obj->keys);
	}

	if(root_obj->variations != NULL) {
		for(i = 0; i < root_obj->nvariations; i++) {
			free(root_obj->variations[i]);
		}
		free(root_obj->variations);
	}

	free(root_obj);

	return rc;
}

int
root_obj_punch(root_obj_t *root_obj, operations_controllers_t *op_controller,
	       int punch_flags, int parent_idx, int *curr_idx)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = root_obj_get_id_oh(root_obj);

	rc = destroy_object(oid_oh, punch_flags, op_controller, parent_idx,
			    curr_idx);
	DSG_ERROR(rc, "Destroying root object failed");

	rc = root_obj_release(root_obj);
	DSG_ERROR(rc, "Releasing root object memory failed");

	return rc;

}

seismic_object_oid_oh_t*
root_obj_get_id_oh(root_obj_t *root_obj)
{
	return &(root_obj->oid_oh);

}

int
root_obj_fetch(root_obj_t *root_obj, operations_controllers_t *op_controller,
	       int parent_idx, int *curr_idx)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = root_obj_get_id_oh(root_obj);

	rc = fetch_object_entry(oid_oh, op_controller, root_obj->io_parameters,
				parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching root object entry failed");
	return rc;

}

int
root_obj_update(root_obj_t *root_obj, operations_controllers_t *op_controller,
		int parent_idx, int *curr_idx)
{
	int rc = 0;

	seismic_object_oid_oh_t *oid_oh = root_obj_get_id_oh(root_obj);

	rc = update_object_entry(oid_oh, op_controller, root_obj->io_parameters,
				 parent_idx, curr_idx);
	DSG_ERROR(rc, "Updating root object entry failed");
	return rc;
}

int
root_obj_init_io_parameters(root_obj_t *root_obj, uint64_t op_flags,
			    char *dkey_name, unsigned int num_of_iods_sgls)
{
	int rc = 0;

	rc = init_object_io_parameters(&(root_obj->io_parameters), op_flags,
				      dkey_name, num_of_iods_sgls);
	DSG_ERROR(rc,"Initializing root object io parameters failed \n");

	return rc;

}

int
root_obj_set_io_parameters(root_obj_t *root_obj, char *akey_name,
			   daos_iod_type_t type, uint64_t iod_size,
			   unsigned int iod_nr,
			   uint64_t *rx_idx, uint64_t *rx_nr, char *data,
			   size_t data_size)
{

	int rc = 0;

	rc = set_object_io_parameters(root_obj->io_parameters, akey_name, type,
				      iod_size, iod_nr, rx_idx,
				      rx_nr, data, data_size);
	DSG_ERROR(rc,"Setting root object io parameters failed \n");

	return rc;
}

int
root_obj_release_io_parameters(root_obj_t *root_obj)
{
	int rc = 0;

	rc = release_object_io_parameters(root_obj->io_parameters);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");
	root_obj->io_parameters = NULL;
	return rc;
}

int
root_obj_get_mode(root_obj_t *root_obj)
{
	return root_obj->daos_mode;
}

int
root_obj_get_num_of_keys(root_obj_t *root_obj)
{
	return root_obj->num_of_keys;
}

char**
root_obj_get_array_of_keys(root_obj_t *root_obj)
{
	return root_obj->keys;
}

int
root_obj_get_num_of_traces(root_obj_t *root_obj)
{
	return root_obj->num_of_traces;
}

daos_obj_id_t
root_obj_get_oid(root_obj_t *root_obj)
{
	return root_obj->oid_oh.oid;
}

int
root_obj_get_permissions_and_type(root_obj_t *root_obj)
{
	return root_obj->permissions_and_type;
}

daos_handle_t
root_obj_get_parent_oh(root_obj_t *root_obj)
{
	return root_obj->parent_oid_oh.oh;
}

seismic_object_oid_oh_t*
root_obj_get_parent_oid_oh(dfs_obj_t *object)
{
	seismic_object_oid_oh_t *seis_object =
			malloc(sizeof(seismic_object_oid_oh_t));
	seis_object->oid = object->oid;
	seis_object->oh = object->oh;

	return seis_object;
}

int
root_obj_update_keys(root_obj_t *root_obj,
		     operations_controllers_t *op_controller,
		     int num_of_keys,char **keys,
		     int parent_idx, int *curr_idx)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int		rc;
	int		i;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS,
					 (num_of_keys + 1));
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_KEYS,
					DAOS_IOD_SINGLE, sizeof(int), 1,
					NULL, NULL,(char*) &num_of_keys,
					sizeof(int));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	for (i = 0; i < num_of_keys; i++) {
		char temp[10] = "";
		char akey[MAX_KEY_LENGTH] = "";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey, temp);

		rc = root_obj_set_io_parameters(root_obj, akey,
						DAOS_IOD_ARRAY, 1,
						1, &rx_idx, &rx_nr,
						(char *)keys[i],
						MAX_KEY_LENGTH * sizeof(char));
		DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	}
	rc = root_obj_update(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Updating root object keys failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_update_gather_oids(root_obj_t *root_obj,
			    operations_controllers_t *op_controller,
			    char **keys,int num_of_gathers,
			    seismic_object_oid_oh_t *gathers,
			    int parent_idx, int *curr_idx)
{
	int		rc;
	int		i;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_GATHERS_OIDS,
					 num_of_gathers);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	for(i=0 ; i < num_of_gathers; i++) {
		rc = root_obj_set_io_parameters(root_obj, keys[i],
						DAOS_IOD_SINGLE,
						sizeof(daos_obj_id_t),
						1, NULL, NULL,
						(char *)&(gathers[i].oid),
						sizeof(daos_obj_id_t));
		DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	}

	rc = root_obj_update(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Updating root object gather oids failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_fetch_gather_oids(root_obj_t *root_obj,
			   operations_controllers_t *op_controller,
			   char **keys,int num_of_gathers,
			   seismic_object_oid_oh_t *gathers,
			   int parent_idx, int *curr_idx)
{
	int		rc;
	int		i;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_GATHERS_OIDS,
					 num_of_gathers);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	for(i=0 ; i < num_of_gathers; i++) {
		rc = root_obj_set_io_parameters(root_obj, keys[i],
						DAOS_IOD_SINGLE,
						sizeof(daos_obj_id_t),
						1, NULL, NULL,
						(char *)&(gathers[i].oid),
						sizeof(daos_obj_id_t));
		DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	}

	rc = root_obj_fetch(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Fetching root object gather oids failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_insert_in_dfs(root_obj_t *root_obj, char *name)
{
	int			rc;
	struct dfs_entry 	entry;

	entry.oid = root_obj_get_oid(root_obj);

	entry.mode = root_obj_get_permissions_and_type(root_obj);
	entry.chunk_size = 0;
	entry.atime = entry.mtime = entry.ctime = time(NULL);

	rc = insert_dfs_entry(root_obj_get_parent_oh(root_obj), DAOS_TX_NONE,
			      name, entry);
	DSG_ERROR(rc, "Inserting root object in dfs failed \n");
	return rc;
}

int
root_obj_fetch_num_of_keys(root_obj_t *root_obj, int parent_idx, int *curr_idx,
			   int *num_of_keys)
{
	int		rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_KEYS,
					DAOS_IOD_SINGLE, sizeof(int),
					1, NULL, NULL,
					(char *)num_of_keys,
					sizeof(int));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	rc = root_obj_fetch(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Fetching root object number of keys failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_fetch_keys(root_obj_t *root_obj, int parent_idx, int *curr_idx,
		    int num_of_keys, char **keys)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int		rc;
	int		i;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS,
					 num_of_keys);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	for(i=0 ; i < num_of_keys; i++) {
		char temp[10]="";
		char akey[MAX_KEY_LENGTH]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);

		rc = root_obj_set_io_parameters(root_obj, akey,
						DAOS_IOD_ARRAY, 1,
						1, &rx_idx, &rx_nr,
						(char *)keys[i],
						MAX_KEY_LENGTH * sizeof(char));
		DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	}
	rc = root_obj_fetch(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Fetching root object keys failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_update_headers(root_obj_t *root_obj, char *ebcbuf, bhed *bh,
			operations_controllers_t *op_controller,
			int parent_idx,int* curr_idx)
{
	int 		rc;
	unsigned int 	iod_nr;
	uint64_t	*rx_idx;
	uint64_t	*rx_nr;
	uint64_t	iod_size;

	iod_nr = 1;
	iod_size = 1;
	rx_idx = malloc(iod_nr * sizeof(uint64_t));
	rx_nr = malloc(iod_nr * sizeof(uint64_t));
	rx_idx[0] = 0;
	rx_nr[0] = EBCBYTES;
	rc = 0;

	root_obj_init_io_parameters(root_obj, 0, DS_D_FILE_HEADER, 1);
	root_obj_set_io_parameters(root_obj, DS_A_TEXT_HEADER,
				   DAOS_IOD_ARRAY,
				   iod_size,iod_nr, rx_idx, rx_nr,(char*)ebcbuf,
				   EBCBYTES);

	rc = root_obj_update(root_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc,"Updating root object text header failed \n");
	root_obj_release_io_parameters(root_obj);

	rx_idx[0] = 0;
	rx_nr[0] = BNYBYTES;
	root_obj_init_io_parameters(root_obj, 0, DS_D_FILE_HEADER, 1);
	root_obj_set_io_parameters(root_obj, DS_A_BINARY_HEADER,
				   DAOS_IOD_ARRAY,
				   iod_size,iod_nr, rx_idx, rx_nr, (char*) bh,
				   BNYBYTES);
	rc = root_obj_update(root_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Updating root object binary header failed");
	root_obj_release_io_parameters(root_obj);

	free(rx_idx);
	free(rx_nr);
	return rc;
}

int
root_obj_update_extheaders(root_obj_t* root_obj,
			   parse_functions_t *parse_functions,
			   operations_controllers_t* op_controller,
			   int nextended,int parent_idx, int* curr_idx)
{
	int 		rc;
	unsigned int 	iod_nr;
	uint64_t	*rx_idx;
	uint64_t	*rx_nr;
	uint64_t	iod_size;

	iod_nr = 1;
	iod_size = 1;
	rx_idx= malloc(iod_nr *sizeof(uint64_t));
	rx_nr = malloc(iod_nr *sizeof(uint64_t));
	rx_idx[0] = 0;
	rx_nr[0] = EBCBYTES;
	rc = 0;

	for(int i = 0; i < nextended; i++) {
		char* exthdr = get_extended_header(parse_functions);
		char akey_extended[200] = "";
		char akey_index[100];
		sprintf(akey_index, "%d", i);
		strcat(akey_extended, DS_A_EXTENDED_HEADER);
		strcat(akey_extended, akey_index);
		root_obj_init_io_parameters(root_obj, 0,DS_D_FILE_HEADER,1);
		root_obj_set_io_parameters(root_obj,
					   akey_extended,DAOS_IOD_ARRAY,
					   iod_size,iod_nr,
					   rx_idx,rx_nr,(char*)exthdr,EBCBYTES);
		rc = root_obj_update(root_obj, NULL, -1 , curr_idx);
		DSG_ERROR(rc,"Updating root object extended text header failed \n");
		root_obj_release_io_parameters(root_obj);
	}
	free(rx_idx);
	free(rx_nr);
	return rc;
}

int
root_obj_update_num_of_traces(root_obj_t* root_obj,
			      operations_controllers_t* op_controller,
			      int *num_of_traces,
			      int parent_idx, int *curr_idx)
{
	int 			rc;

	seismic_object_oid_oh_t *oid_oh_root = root_obj_get_id_oh(root_obj);
	int objmode_root = root_obj_get_mode(root_obj);

	root_obj_init_io_parameters(root_obj, 0,DS_D_FILE_HEADER,1);
	root_obj_set_io_parameters(root_obj,DS_A_NTRACES_HEADER,
				   DAOS_IOD_SINGLE,sizeof(int),1, NULL,
				   NULL,(char*)num_of_traces,
				   sizeof(int));

	rc = root_obj_update(root_obj, NULL, -1 , curr_idx);
	DSG_ERROR(rc,"Updating root object number of traces failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

	return rc;
}

int
root_obj_fetch_num_of_traces(root_obj_t* root_obj,
			     operations_controllers_t* op_controller,
			     int *num_of_traces,
			     int parent_idx, int *curr_idx)
{
	int 			rc;

	seismic_object_oid_oh_t *oid_oh_root = root_obj_get_id_oh(root_obj);
	int objmode_root = root_obj_get_mode(root_obj);

	root_obj_open(oid_oh_root, objmode_root, NULL, -1, curr_idx);
	root_obj_init_io_parameters(root_obj, 0,DS_D_FILE_HEADER,1);
	root_obj_set_io_parameters(root_obj,DS_A_NTRACES_HEADER,
				   DAOS_IOD_SINGLE,sizeof(int),1, NULL,
				   NULL,(char*)num_of_traces,
				   sizeof(int));

	rc = root_obj_fetch(root_obj, NULL, -1 , curr_idx);
	DSG_ERROR(rc,"Fetching root object number of traces failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

	return rc;
}

int
root_obj_fetch_entries(root_obj_t **root_obj, dfs_obj_t *dfs_obj)
{
	int		curr_idx;
	int		rc;
	int		i;
	(*root_obj) = malloc(sizeof(root_obj_t));
	(*root_obj)->oid_oh = *(root_obj_get_parent_oid_oh(dfs_obj));
	(*root_obj)->daos_mode = get_daos_obj_mode(dfs_obj->flags);
	rc = root_obj_fetch_num_of_keys((*root_obj), -1, &curr_idx,
					&((*root_obj)->num_of_keys));
	DSG_ERROR(rc,"Fetching root object number of keys failed \n");

	(*root_obj)->keys = malloc((*root_obj)->num_of_keys * sizeof(char*));
	for(i=0; i < (*root_obj)->num_of_keys; i++) {
		(*root_obj)->keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	rc = root_obj_fetch_keys((*root_obj), -1, &curr_idx,
				 (*root_obj)->num_of_keys, (*root_obj)->keys);
	DSG_ERROR(rc,"Fetching root object keys failed \n");

	(*root_obj)->gather_oids = malloc((*root_obj)->num_of_keys *
					  sizeof(seismic_object_oid_oh_t));

	rc = root_obj_fetch_gather_oids((*root_obj), NULL, (*root_obj)->keys,
					(*root_obj)->num_of_keys,
					(*root_obj)->gather_oids, -1, &curr_idx);
	DSG_ERROR(rc,"Fetching root object gather oids failed \n");

	rc = root_obj_fetch_num_of_traces((*root_obj), NULL,
					  &(*root_obj)->num_of_traces,
					  -1, &curr_idx);
	DSG_ERROR(rc,"Fetching root object number of traces failed \n");
	rc = root_obj_fetch_num_of_variations((*root_obj), NULL,
					      &(*root_obj)->nvariations,
					      -1, &curr_idx);
	DSG_ERROR(rc,"Fetching root object number of variations failed \n");

	if((*root_obj)->nvariations > 0) {
		(*root_obj)->variations =
				malloc((*root_obj)->nvariations * sizeof(char*));
		for(i=0; i < (*root_obj)->nvariations; i++) {
			(*root_obj)->variations[i] =
					malloc(MAX_KEY_LENGTH * sizeof(char));
		}
		rc = root_obj_fetch_variations((*root_obj), -1, &curr_idx,
					       (*root_obj)->nvariations,
					       (*root_obj)->variations);
		DSG_ERROR(rc,"Fetching root object variations failed \n");
	} else {
		(*root_obj)->variations = NULL;
	}
	rc = root_obj_fetch_complex_oid((*root_obj), NULL, -1, &curr_idx,
					&(*root_obj)->complex_oid);
	(*root_obj)->io_parameters = NULL;
	return rc;
}

int
root_obj_fetch_text_header(root_obj_t *root_obj, char *ebcbuf)
{
	seismic_object_oid_oh_t 	*oid_oh_root;
	int				curr_idx;
	uint64_t 			rx_idx;
	uint64_t 			rx_nr;
	int 				objmode_root;
	int 				rc;

	rx_idx = 0;
	rx_nr = EBCBYTES;
	rc = 0;

	oid_oh_root = root_obj_get_id_oh(root_obj);
	objmode_root = root_obj_get_mode(root_obj);

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_FILE_HEADER, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_TEXT_HEADER,
					DAOS_IOD_ARRAY, 1, 1, &rx_idx,
					&rx_nr, ebcbuf, EBCBYTES);
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rc = root_obj_fetch(root_obj, NULL, -1, &curr_idx);
	DSG_ERROR(rc,"Fetching root object text header failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_fetch_binary_header(root_obj_t *root_obj, bhed *binary_header)
{
	seismic_object_oid_oh_t 	*oid_oh_root;
	int				curr_idx;
	uint64_t 			rx_idx;
	uint64_t 			rx_nr;
	int 				objmode_root;
	int 				rc;

	rx_idx = 0;
	rx_nr = BNYBYTES;
	rc = 0;

	oid_oh_root = root_obj_get_id_oh(root_obj);
	objmode_root = root_obj_get_mode(root_obj);

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_FILE_HEADER, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	root_obj_set_io_parameters(root_obj, DS_A_BINARY_HEADER, DAOS_IOD_ARRAY,
				   1, 1, &rx_idx, &rx_nr, (char*) binary_header,
				   BNYBYTES);
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rc = root_obj_fetch(root_obj, NULL, -1, &curr_idx);
	DSG_ERROR(rc,"Fetching root object binary header failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_link_new_gather_obj(root_obj_t *root_obj,
			     seismic_object_oid_oh_t oid_oh, char *key,
			     int num_of_keys,
			     operations_controllers_t *op_controller,
			     int parent_idx, int *curr_idx)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int		rc;

	rc = 0;

	rc = root_obj_update_gather_oids(root_obj, op_controller, &key,
					 1, &oid_oh,
					 -1, curr_idx);
	DSG_ERROR(rc,"Updating root object gather oids failed \n", end);

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS, 2);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_KEYS,
					DAOS_IOD_SINGLE, sizeof(int),
					1, NULL, NULL,
					(char*) &num_of_keys,
					sizeof(int));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	char temp[10] = "";
	char akey[MAX_KEY_LENGTH] = "";
	sprintf(temp, "%d", (num_of_keys-1));
	strcpy(akey, DS_A_KEYS);
	strcat(akey, temp);

	rc = root_obj_set_io_parameters(root_obj, akey,
					DAOS_IOD_ARRAY, 1,
					1, &rx_idx, &rx_nr,
					key,
					MAX_KEY_LENGTH * sizeof(char));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	rc = root_obj_update(root_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc,"Updating root object gather failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");


end:
	return rc;
}

int
root_obj_fetch_num_of_variations(root_obj_t *root_obj,
				 operations_controllers_t *op,
				 int *num_of_variations,
				 int parent_idx, int *curr_idx)
{
	int		rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_VARS,
					DAOS_IOD_SINGLE, sizeof(int),
					1, NULL, NULL,
					(char *)num_of_variations,
					sizeof(int));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	rc = root_obj_fetch(root_obj, op, parent_idx, curr_idx);
	DSG_ERROR(rc,"Fetching root object number of variations failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_update_num_of_variations(root_obj_t *root_obj,
				  operations_controllers_t *op,
				  int *num_of_variations,
				  int parent_idx, int *curr_idx)
{
	int		rc;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_VARS,
					DAOS_IOD_SINGLE, sizeof(int),
					1, NULL, NULL,
					(char *)num_of_variations,
					sizeof(int));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	rc = root_obj_update(root_obj, op, parent_idx, curr_idx);
	DSG_ERROR(rc,"Updating root object number of variation failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return 0;
}

int
root_obj_fetch_variations(root_obj_t *root_obj, int parent_idx, int *curr_idx,
		    	  int num_of_variations, char **variations)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int		rc;
	int		i;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS,
					 num_of_variations);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	for(i=0 ; i < num_of_variations; i++) {
		char temp[10]="";
		char akey[MAX_KEY_LENGTH]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_VAR);
		strcat(akey,temp);

		rc = root_obj_set_io_parameters(root_obj, akey,
						DAOS_IOD_ARRAY, 1,
						1, &rx_idx, &rx_nr,
						(char *)variations[i],
						MAX_KEY_LENGTH * sizeof(char));
		DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	}
	rc = root_obj_fetch(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Fetching root object variations failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return 0;
}

int
root_obj_update_variations(root_obj_t *root_obj, int parent_idx, int *curr_idx,
		    	   int num_of_variations, char **variations)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int		rc;
	int		i;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS,
					 num_of_variations);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	for(i=0 ; i < num_of_variations; i++) {
		char temp[10]="";
		char akey[MAX_KEY_LENGTH]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_VAR);
		strcat(akey,temp);

		rc = root_obj_set_io_parameters(root_obj, akey,
						DAOS_IOD_ARRAY, 1,
						1, &rx_idx, &rx_nr,
						(char *)variations[i],
						MAX_KEY_LENGTH * sizeof(char));
		DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	}
	rc = root_obj_update(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Updating root object variations failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_fetch_complex_oid(root_obj_t *root_obj,
			   operations_controllers_t *op_controller,
			   int parent_idx, int *curr_idx,
			   seismic_object_oid_oh_t *complex_gather)
{
	int		rc;
	int		i;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_COMPLEX_GATHER,
					DAOS_IOD_SINGLE,
					sizeof(daos_obj_id_t),
					1, NULL, NULL,
					(char *)&(complex_gather->oid),
					sizeof(daos_obj_id_t));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rc = root_obj_fetch(root_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc,"Fetching root object complex oids failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_update_complex_oid(root_obj_t *root_obj,
			    operations_controllers_t *op_controller,
			    int parent_idx, int *curr_idx,
			    seismic_object_oid_oh_t *complex_gather)
{
	int		rc;
	int		i;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS, 1);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_COMPLEX_GATHER,
					DAOS_IOD_SINGLE,
					sizeof(daos_obj_id_t),
					1, NULL, NULL,
					(char *)&(complex_gather->oid),
					sizeof(daos_obj_id_t));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rc = root_obj_update(root_obj, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc,"Updating root object complex oids failed \n");

	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_add_new_variation(root_obj_t *root_obj,
			   char *key,int num_of_keys,
			   operations_controllers_t *op_controller,
			   int parent_idx, int *curr_idx)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int		rc;

	rc = 0;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_SORT_VARIATIONS, 2);
	DSG_ERROR(rc,"Preparing root object io parameters failed \n", end);

	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_VARS,
					DAOS_IOD_SINGLE, sizeof(int),
					1, NULL, NULL,
					(char*) &num_of_keys,
					sizeof(int));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	char temp[10] = "";
	char akey[MAX_KEY_LENGTH] = "";
	sprintf(temp, "%d", (num_of_keys-1));
	strcpy(akey, DS_A_VAR);
	strcat(akey, temp);

	rc = root_obj_set_io_parameters(root_obj, akey,
					DAOS_IOD_ARRAY, 1,
					1, &rx_idx, &rx_nr,
					key,
					MAX_KEY_LENGTH * sizeof(char));
	DSG_ERROR(rc,"Setting root object io parameters failed \n", end);
	rc = root_obj_update(root_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc,"Updating root object variations failed \n");
	rc = root_obj_release_io_parameters(root_obj);
	DSG_ERROR(rc,"Releasing root object io parameters failed \n");

end:
	return rc;
}

int
root_obj_init(root_obj_t **root_obj)
{
	*root_obj = (root_obj_t*) malloc(sizeof(root_obj_t));

	(*root_obj)->variations = NULL;
	(*root_obj)->keys = NULL;
	(*root_obj)->io_parameters = NULL;
	(*root_obj)->gather_oids = NULL;

	return 0;
}
