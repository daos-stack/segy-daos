/*
 * gather.c
 *
 *  Created on: Feb 2, 2021
 *      Author: omar
 */
#include "graph_objects/gather.h"

int
gather_obj_create(gather_obj_t **gather_obj, char *name, int flags,
		  gather_data_structure gather_ds)
{
	int	 rc = 0;

	(*gather_obj) = malloc(sizeof(gather_obj_t));

	if ((*gather_obj) == NULL) {
		return ENOMEM;
	}

	rc = oid_gen(DAOS_OBJ_CLASS_ID, false, &((*gather_obj)->oid_oh.oid),
		     false);
	DSG_ERROR(rc, "Generating Object id for gather object failed ", end);

	((*gather_obj))->name = malloc(sizeof(char) * 50);
	strcpy((*gather_obj)->name, name);
	(*gather_obj)->daos_mode = get_daos_obj_mode(flags);
	(*gather_obj)->number_of_gathers = 0;
	(*gather_obj)->trace_oids_ohs = NULL;
	(*gather_obj)->unique_values = NULL;
	(*gather_obj)->io_parameters = NULL;
	(*gather_obj)->num_of_keys = 0;
	(*gather_obj)->keys = NULL;
	if (gather_ds == LINKED_LIST) {
		(*gather_obj)->gathers_list =
				malloc(sizeof(gathers_list_t));
		(*gather_obj)->gathers_list->gathers =
				doubly_linked_list_init();
	} else {
		//BLUS TREE goes here
		(*gather_obj)->gathers_list = NULL;
	}
	return rc;

end: 	free((*gather_obj));
	return rc;
}

int
gather_obj_open(seismic_object_oid_oh_t *oid_oh, int mode,
		operations_controllers_t *op_controller, int parent_idx,
		int *curr_idx)
{
	int 	rc = 0;
	rc = open_object(oid_oh, op_controller, get_dfs()->coh, mode,
			 parent_idx, curr_idx);
	DSG_ERROR(rc, "Opening Gather Object failed\n");

	return rc;
}

seismic_object_oid_oh_t*
gather_obj_get_oid_oh(gather_obj_t *gather_obj)
{
	return &(gather_obj->oid_oh);
}

int
gather_obj_get_number_of_gathers(gather_obj_t *gather_obj)
{
	return gather_obj->number_of_gathers;
}

int
gather_obj_get_mode(gather_obj_t *gather_obj)
{
	return gather_obj->daos_mode;
}

gathers_list_t**
get_gather_list_of_gathers(gather_obj_t *gather_obj)
{
	return &(gather_obj->gathers_list);
}

char*
gather_obj_get_name(gather_obj_t *gather_obj)
{
	return gather_obj->name;
}

generic_value*
gather_obj_get_gather_unique_values(gather_obj_t *gather_obj)
{
	return gather_obj->unique_values;
}

seismic_object_oid_oh_t**
gather_obj_get_trace_oids(gather_obj_t *gather_obj)
{
	return &(gather_obj->trace_oids_ohs);
}

int
gather_obj_destroy_gather_node(void *gather_node)
{

	gather_node_t* g = (gather_node_t*)gather_node;
	if (g->oids != NULL) {
		free(g->oids);
	}
	free(g);
	return 0;
}

int
gather_obj_release_io_parameters(gather_obj_t *gather_obj)
{
	int 	rc = 0;

	rc = release_object_io_parameters(gather_obj->io_parameters);
	DSG_ERROR(rc, "Releasing gather object io parameters failed ", end);

	gather_obj->io_parameters = NULL;
end:	return rc;
}

int
gather_obj_release(gather_obj_t *gather_obj)
{
	int 	rc = 0;

	if (gather_obj->gathers_list != NULL) {
		rc = doubly_linked_list_destroy(gather_obj->gathers_list->gathers,
						&gather_obj_destroy_gather_node,
						offsetof(gather_node_t, n));
		DSG_ERROR(rc, "Destroying gather object gathers list failed ",
			  end);
	}

	if (gather_obj->io_parameters != NULL) {
		rc = gather_obj_release_io_parameters(gather_obj);
		DSG_ERROR(rc, "Releasing gather object io parameters failed ",
			  end);
	}

	if (gather_obj->unique_values != NULL) {
		free(gather_obj->unique_values);
	}

	if (gather_obj->trace_oids_ohs != NULL) {
		free(gather_obj->trace_oids_ohs);
	}

	if (gather_obj->name != NULL){
		free(gather_obj->name);
	}

	if (gather_obj->keys != NULL) {
		rc = release_tokenized_array(gather_obj->keys, STRING,
					     gather_obj->num_of_keys);
	}

	free(gather_obj);

end:	return rc;
}

int
gather_obj_close(gather_obj_t *gather_obj,
		 operations_controllers_t *op_controller, int parent_idx,
		 int *curr_idx, int release_gather)
{
	int 	rc = 0;

	seismic_object_oid_oh_t *oid_oh = gather_obj_get_oid_oh(gather_obj);

	rc = close_object(oid_oh, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Closing Gather object failed ", end);

	if (release_gather == 1) {
		rc = gather_obj_release(gather_obj);
		DSG_ERROR(rc, "Releasing Gather object failed ", end);
	}

end:	return rc;
}

int
gather_obj_punch(gather_obj_t *gather_obj,
		 operations_controllers_t *op_controller, int punch_flags,
		 int parent_idx, int *curr_idx)
{
	int 	rc = 0;

	seismic_object_oid_oh_t *oid_oh = gather_obj_get_oid_oh(gather_obj);

	rc = destroy_object(oid_oh, punch_flags, op_controller, parent_idx,
			    curr_idx);
	DSG_ERROR(rc, "Punching Gather object failed ", end);

	rc = gather_obj_release(gather_obj);
	DSG_ERROR(rc, "Releasing Gather object failed ", end);

end:	return rc;
}

int
gather_obj_fetch(gather_obj_t *gather_obj,
		 operations_controllers_t *op_controller, int parent_idx,
		 int *curr_idx)
{
	int 	rc = 0;

	seismic_object_oid_oh_t *oid_oh = gather_obj_get_oid_oh(gather_obj);

	rc = fetch_object_entry(oid_oh, op_controller,
				gather_obj->io_parameters, parent_idx,
				curr_idx);
	DSG_ERROR(rc, "Fetching from Gather object failed ");

	return rc;
}

int
gather_obj_update(gather_obj_t *gather_obj,
		  operations_controllers_t *op_controller, int parent_idx,
		  int *curr_idx)
{
	int 	rc = 0;

	seismic_object_oid_oh_t *oid_oh = gather_obj_get_oid_oh(gather_obj);

	rc = update_object_entry(oid_oh, op_controller,
				 gather_obj->io_parameters, parent_idx,
				 curr_idx);
	DSG_ERROR(rc, "Updating Gather Object entry failed ");
	return rc;
}

int
gather_obj_init_io_parameters(gather_obj_t *gather_obj, uint64_t op_flags,
			      char *dkey_name, unsigned int num_of_iods_sgls)
{
	int 	rc = 0;

	rc = init_object_io_parameters(&(gather_obj->io_parameters), op_flags,
				       dkey_name, num_of_iods_sgls);
	DSG_ERROR(rc, "Initializing Gather object IO parameters failed ");

	return rc;
}

int
gather_obj_set_io_parameters(gather_obj_t *gather_obj, char *akey_name,
			     daos_iod_type_t type, uint64_t iod_size,
			     unsigned int iod_nr, uint64_t *rx_idx,
			     uint64_t *rx_nr, char *data, size_t data_size)
{
	int 	rc = 0;

	rc = set_object_io_parameters(gather_obj->io_parameters, akey_name,
				      type, iod_size, iod_nr, rx_idx, rx_nr,
				      data, data_size);
	DSG_ERROR(rc, "Setting Gather object IO parameters failed ");

	return rc;
}

int
gather_obj_add_gather(gather_node_t *gather, gathers_list_t *list)
{
	gather_node_t 		*new_gather;
	int 			num_traces;

	new_gather = (gather_node_t*) malloc(sizeof(gather_node_t));
	new_gather->unique_key = gather->unique_key;
	new_gather->number_of_traces = gather->number_of_traces;
	num_traces = 50;
	new_gather->oids = malloc(num_traces * sizeof(daos_obj_id_t));
	new_gather->curr_oids_index = gather->curr_oids_index;
	if (gather->oids != NULL) {
		if (new_gather->curr_oids_index < num_traces) {
			memcpy(new_gather->oids, gather->oids,
			       new_gather->curr_oids_index *
			       sizeof(daos_obj_id_t));
		} else {
			printf("number of gathers > 50 \n");
			return -1;
		}
	}
	doubly_linked_list_add_node(list->gathers, &(new_gather->n));

	return 0;
}

int
gather_obj_check_gather(generic_value target, char *key,
			gathers_list_t *gathers_list,
			daos_obj_id_t trace_obj_id)
{
	gather_node_t 		*temp_gather;
	node_t 			*temp_node;
	char 			*type;
	int 			ntraces;
	int 			exists = 0;
	int 			i;

	if (gathers_list == NULL || gathers_list->gathers->head == NULL) {
		exists = 0;
		return exists;
	} else {
		type = hdtype(key);
		temp_node = gathers_list->gathers->head;
		//printf("Gathers Size in Check Gather: %d\n", gathers_list->gathers->size);
		for (i = 0; i < gathers_list->gathers->size; i++) {
			temp_gather =
				doubly_linked_list_get_object(temp_node,
							      offsetof(gather_node_t, n));
			if (valcmp(type, temp_gather->unique_key, target) == 0) {
				temp_gather->oids[temp_gather->curr_oids_index] =
									trace_obj_id;
				temp_gather->curr_oids_index++;
				temp_gather->number_of_traces++;
				ntraces = temp_gather->curr_oids_index;
				exists = 1;
				if (temp_gather->curr_oids_index % 50 == 0) {
					temp_gather->oids =
						(daos_obj_id_t*) realloc(temp_gather->oids,
									 (ntraces + 50)
									 * sizeof(daos_obj_id_t));
				}
				return exists;
			} else {
				temp_node = temp_node->next;
			}
		}
		exists = 0;
		return exists;
	}
}

int
trace_linking(trace_t *trace, gather_obj_t *gather_obj, char *key,
	      operations_controllers_t *op_controller, int parent_idx,
	      int *curr_idx)
{
	generic_value 		val;
	int 			key_exists;
	int 			rc = 0;

	trace_get_header(*trace, key, &val);
	key_exists = gather_obj_check_gather(val, key,
					     gather_obj->gathers_list,
					     trace->trace_header_obj);
	if (key_exists == 0) {
		unsigned int 	iod_nr;
		uint64_t 	iod_size;

		iod_nr = 1;
		iod_size = sizeof(generic_value);
		gather_node_t 	g;
		g.oids = malloc(50 * sizeof(daos_obj_id_t));
		char temp[200] = "";
		g.oids[0] = trace->trace_header_obj;
		g.number_of_traces = 1;
		g.unique_key = val;
		g.curr_oids_index = 1;
		gather_obj_add_gather(&g, gather_obj->gathers_list);
		gather_obj->number_of_gathers++;
		free(g.oids);
	}
	return rc;
}

int
gather_obj_update_num_of_gathers(gather_obj_t *gather_obj, int *num_of_gathers,
				 operations_controllers_t *op_controller,
				 int parent_idx,int *curr_idx)
{
	unsigned int 	iod_nr;
	uint64_t 	iod_size;
	int 		rc = 0;

	iod_nr = 1;
	iod_size = sizeof(int);

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_NGATHERS, 1);
	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NGATHERS,
					  DAOS_IOD_SINGLE, iod_size, iod_nr,
					  NULL, NULL, (char*)num_of_gathers,
					  sizeof(int));
	rc = gather_obj_update(gather_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Writing number of gathers under gather object failed ");

	rc = gather_obj_release_io_parameters(gather_obj);

	return rc;
}

size_t
dkey_get_size(char *type)
{
	switch (*type) {
	case 's':
		return sizeof(char*);
	case 'h':
		return sizeof(short);
	case 'u':
		return sizeof(unsigned short);
	case 'l':
		return sizeof(long);
	case 'v':
		return sizeof(unsigned long);
	case 'i':
		return sizeof(int);
	case 'p':
		return sizeof(unsigned int);
	case 'f':
		return sizeof(float);
	case 'd':
		return sizeof(double);
	case 'U':
		return sizeof(unsigned short int);
	case 'P':
		return sizeof(unsigned long int);
	default:
		printf("get_size: unknown type %s", type);
		return 0; /* for lint */
	}
}

int
gather_obj_dump_gathers_list_in_graph(gather_obj_t *gather_obj,
				      operations_controllers_t *op_controller,
				      int parent_idx, int *curr_idx,
				      int curr_num_gathers)
{
	seismic_object_oid_oh_t 	traces_array_oid;
	gather_node_t 			*curr_gather;
	generic_value 			*gather_keys;
	node_t 				*temp_node;
	unsigned int 			iod_nr;
	uint64_t 			iod_size;
	char 				*dkey_name;
	int 				size;
	int 				rc = 0;

	gather_keys = malloc(size * sizeof(generic_value));
	size = gather_obj->gathers_list->gathers->size;
	temp_node = gather_obj->gathers_list->gathers->head;
	dkey_name = gather_obj->name;

	for (int i = 0; i < size; i++) {
		if (temp_node == NULL) {
			break;
		}
		curr_gather =
			doubly_linked_list_get_object(temp_node,
						      offsetof(gather_node_t, n));
		gather_keys[i] = curr_gather->unique_key;

		if (curr_gather->curr_oids_index == 0) {
			temp_node = temp_node->next;
			continue;
		}

		int ntraces = curr_gather->number_of_traces;
		char temp[200] = "";
		char gather_dkey_name[200] = "";
		strcat(gather_dkey_name, dkey_name);
		strcat(gather_dkey_name, KEY_SEPARATOR);
		val_sprintf(temp, curr_gather->unique_key, hdtype(dkey_name));
		strcat(gather_dkey_name, temp);

		gather_keys[i] = curr_gather->unique_key;

		rc = gather_obj_update_gather_unique_value(gather_obj,
							   gather_dkey_name,
							   &gather_keys[i],
							   NULL, -1,
							   curr_idx);

		traces_array_oid = gather_obj->trace_oids_ohs[i];

		rc = gather_obj_update_gather_traces_array_oid(gather_obj,
							       gather_dkey_name,
							       &(traces_array_oid.oid),
							       NULL, -1,
							       curr_idx);

		rc = gather_obj_update_gather_num_of_traces(gather_obj,
							    gather_dkey_name,
							    &ntraces, NULL,
							    -1, curr_idx);

		temp_node = temp_node->next;

	}
	if (size > curr_num_gathers) {
		generic_value_sort_params_t *sp =
				init_generic_value_sort_params(hdtype(dkey_name),
							       1);

		sort(gather_keys, size, sizeof(generic_value), sp,
		     &generic_value_compare);

		gather_obj->unique_values =
				(generic_value*) realloc(gather_obj->unique_values,
							 size *
							 sizeof(generic_value));
		for (int i = 0; i < size; i++){
			gather_obj->unique_values[i] = gather_keys[i];
		}

		rc = gather_obj_update_dkeys_list(gather_obj, 0,
						  gather_obj->number_of_gathers,
						  gather_obj->unique_values,
						  NULL, -1, curr_idx);
	}
	return rc;

}

int
gather_obj_fetch_gather_num_of_traces(gather_obj_t *gather_obj,
				      operations_controllers_t *op_controller,
				      char *gather_dkey_name,
				      int *num_of_traces, int parent_idx,
				      int *curr_idx)
{
	int 	rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, gather_dkey_name, 1);
	DSG_ERROR(rc, "Initializing Gather Object IO Parameters failed ", end);

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NTRACES,
					  DAOS_IOD_SINGLE, sizeof(int), 1,
					  NULL, NULL, (char*) num_of_traces,
					  sizeof(int));
	DSG_ERROR(rc, "Setting Gather Object IO Parameters failed ", end);

	rc = gather_obj_fetch(gather_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Fetching Number of traces from Gather Object failed ");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc, "Releasing Gather Object IO Parameters failed ");

end:	return rc;
}

int
gather_obj_fetch_num_of_gathers(gather_obj_t *gather_obj,
				operations_controllers_t *op_controller,
				int *num_of_gathers,
				int parent_idx, int *curr_idx)
{
	int	 rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_NGATHERS, 1);
	DSG_ERROR(rc, "Initializing Gather Object IO Parameters failed ", end);

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NGATHERS,
					  DAOS_IOD_SINGLE, sizeof(int), 1,
					  NULL, NULL, (char*) num_of_gathers,
					  sizeof(int));
	DSG_ERROR(rc, "Setting Gather Object IO Parameters failed ", end);

	rc = gather_obj_fetch(gather_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Fetching Number of Gathers from Gather Object failed ");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc, "Releasing Gather Object IO Parameters failed");

end:	return rc;
}

int
gather_obj_fetch_dkeys_list(gather_obj_t *gather_obj,
			    operations_controllers_t *op_controller,
			    uint64_t st_idx, uint64_t nrecords,
			    generic_value *unique_values, int parent_idx,
			    int *curr_idx)
{
	int 	rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_DKEYS_LIST, 1);
	DSG_ERROR(rc, "Initializing Gather Object IO Parameters failed ", end);

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_DKEYS_LIST,
					  DAOS_IOD_ARRAY, sizeof(generic_value),
					  1, &st_idx, &nrecords,
					  (char*) unique_values,
					  nrecords * sizeof(generic_value));
	DSG_ERROR(rc, "Setting Gather Object IO Parameters failed ", end);

	rc = gather_obj_fetch(gather_obj, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching Dkeys from Gather Object failed ");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc, "Releasing Gather Object IO Parameters failed ");

end:	return rc;
}

int
gather_obj_fetch_gather_traces_array_oid(gather_obj_t *gather_obj,
					 operations_controllers_t *op_controller,
					 char *gather_dkey_name,
					 daos_obj_id_t *oid, int parent_idx,
					 int *curr_idx)
{
	unsigned int 	iod_nr;
	uint64_t 	iod_size;
	int 		rc = 0;

	iod_nr = 1;
	iod_size = sizeof(daos_obj_id_t);

	rc = gather_obj_init_io_parameters(gather_obj, 0, gather_dkey_name, 1);
	rc = gather_obj_set_io_parameters(gather_obj, DS_A_GATHER_TRACE_OIDS,
					  DAOS_IOD_SINGLE, iod_size, iod_nr,
					  NULL, NULL, (char*) oid,
					  sizeof(daos_obj_id_t));

	rc = gather_obj_fetch(gather_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Fetching Traces array oid from Gather Object failed ");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc, "Releasing Gather Object IO Parameters failed ");

	return rc;
}

int
gather_obj_fetch_gather_unique_value(gather_obj_t *gather_obj,
				     operations_controllers_t *op_controller,
				     char *gather_dkey_name,
				     generic_value *val, int parent_idx,
				     int *curr_idx)
{
	unsigned int 	iod_nr;
	uint64_t 	iod_size;
	int 		rc = 0;

	iod_nr = 1;
	iod_size = sizeof(generic_value);

	rc = gather_obj_init_io_parameters(gather_obj, 0, gather_dkey_name, 1);
	rc = gather_obj_set_io_parameters(gather_obj, DS_A_UNIQUE_VAL,
					  DAOS_IOD_SINGLE, iod_size, iod_nr,
					  NULL, NULL, (char*) val,
					  sizeof(generic_value));

	rc = gather_obj_fetch(gather_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Fetching Gather unique value from Gather Object failed ");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc, "Releasing Gather Object IO Parameters failed ");

	return rc;
}

int
gather_obj_fetch_entries(gather_obj_t **gather_obj,
			 seismic_object_oid_oh_t oid_oh, char *name,
			 int initialized)
{
	int 	curr_idx;
	int 	rc = 0;
	int 	i;

	(*gather_obj) = malloc(sizeof(gather_obj_t));
	(*gather_obj)->oid_oh = oid_oh;
	(*gather_obj)->daos_mode = O_RDWR;
	(*gather_obj)->name = malloc(sizeof(char) * strlen(name));
	strcpy((*gather_obj)->name, name);

	if (initialized == 1) {
		rc = gather_obj_fetch_num_of_gathers(*gather_obj, NULL,
						     &((*gather_obj)->number_of_gathers),
						     -1, &curr_idx);
		DSG_ERROR(rc, "Fetching Number of Gathers from Gather "
			  " Object failed ", end1);

		if((*gather_obj)->number_of_gathers == 0) {
			goto init_NULL;
		}

		(*gather_obj)->unique_values =
				malloc((*gather_obj)->number_of_gathers *
				       sizeof(generic_value));

		rc = gather_obj_fetch_dkeys_list(*gather_obj, NULL, 0,
						 (*gather_obj)->number_of_gathers,
						 (*gather_obj)->unique_values,
						 -1, &curr_idx);
		DSG_ERROR(rc, "Fetching Dkeys from Gather Object failed ", end2);

		rc = gather_obj_fetch_num_of_skeys((*gather_obj), -1, &curr_idx,
						    &(*gather_obj)->num_of_keys);
		DSG_ERROR(rc, "Fetching Number of Secondary keys from Gather"
			  " Object failed ", end2);

		if ((*gather_obj)->num_of_keys > 0){
			(*gather_obj)->keys =
					malloc((*gather_obj)->num_of_keys
						* sizeof(char*));
			for (i = 0; i < (*gather_obj)->num_of_keys; i++) {
				(*gather_obj)->keys[i] =
						malloc(MAX_KEY_LENGTH *
						       sizeof(char));
			}
			rc = gather_obj_fetch_skeys((*gather_obj), NULL, -1,
						    &curr_idx,
						    (*gather_obj)->num_of_keys,
						    (*gather_obj)->keys);
			DSG_ERROR(rc,"Fetching Skeys from Gather Object failed ",
				  end3);
		} else {
			(*gather_obj)->keys = NULL;

		}
	} else {
init_NULL:	(*gather_obj)->number_of_gathers = 0;
		(*gather_obj)->unique_values = NULL;
		(*gather_obj)->num_of_keys = 0;
		(*gather_obj)->keys = NULL;
	}
	(*gather_obj)->gathers_list = malloc(sizeof(gathers_list_t));
	(*gather_obj)->gathers_list->gathers = doubly_linked_list_init();
	(*gather_obj)->trace_oids_ohs = NULL;
	(*gather_obj)->io_parameters = NULL;
	return rc;

end3:	for (i = 0; i < (*gather_obj)->num_of_keys; i++) {
		free((*gather_obj)->keys[i]);
	}
	free((*gather_obj)->keys);
end2: 	free((*gather_obj)->unique_values);

end1: 	free((*gather_obj)->name);
	free((*gather_obj));
	return rc;
}

int
gather_obj_prepare_dkeys(gather_obj_t *gather_obj, char **gather_dkeys,
			 int direction, int num_of_gathers)
{
	char 	*dkey;
	int 	i;
	int 	z;

	dkey = malloc(MAX_KEY_LENGTH * sizeof(char));

	strcpy(dkey, gather_obj_get_name(gather_obj));

	if (direction == -1) {
		i = 0;
		for (z = num_of_gathers - 1; z >= 0; z--, i++){
			char 	temp[200] = "";
			val_sprintf(temp, gather_obj->unique_values[z],
				    hdtype(dkey));

			gather_dkeys[i] =
					malloc(MAX_KEY_LENGTH * sizeof(char));
			strcpy(gather_dkeys[i], dkey);
			strcat(gather_dkeys[i], KEY_SEPARATOR);
			strcat(gather_dkeys[i], temp);
		}
	} else {
		for (z = 0; z < num_of_gathers; z++) {
			char 	temp[200] = "";
			val_sprintf(temp, gather_obj->unique_values[z],
				    hdtype(dkey));
			gather_dkeys[z] =
					malloc(MAX_KEY_LENGTH * sizeof(char));
			strcpy(gather_dkeys[z], dkey);
			strcat(gather_dkeys[z], KEY_SEPARATOR);
			strcat(gather_dkeys[z], temp);
		}
	}
	free(dkey);

	return 0;
}

int
gather_obj_create_traces_array(gather_obj_t *gather,
			       operations_controllers_t *op_controller,
			       int parent_idx,int *curr_idx,
			       int curr_num_gathers)
{
	traces_array_obj_t 	*traces_array_obj;
	gather_node_t 		*temp_gather;
	node_t 			*temp;
	int			num_of_gathers;
	int 			rc;

	temp = gather->gathers_list->gathers->head;
	num_of_gathers = gather_obj_get_number_of_gathers(gather);

	if (gather->trace_oids_ohs == NULL) {
		gather->trace_oids_ohs =
				malloc(num_of_gathers *
				       sizeof(seismic_object_oid_oh_t));
	} else {
		gather->trace_oids_ohs =
				(seismic_object_oid_oh_t*)
					realloc(gather->trace_oids_ohs,
						num_of_gathers *
						sizeof(seismic_object_oid_oh_t));
	}
	for (int i = 0; i < num_of_gathers; i++) {
		temp_gather =
			doubly_linked_list_get_object(temp,
						      offsetof(gather_node_t, n));
		if (temp_gather == NULL) {
			return ENOMEM;
		}
		if (i >= curr_num_gathers) {
			rc = traces_array_obj_create(&traces_array_obj, O_RDWR,
						     temp_gather->oids,
						     temp_gather->number_of_traces);
			DSG_ERROR(rc, "Error Creating Traces Array Object",
					end);
			gather->trace_oids_ohs[i] =
					*(traces_array_obj_get_id_oh(
							traces_array_obj));
		} else {
			traces_array_obj = malloc(sizeof(traces_array_obj_t));
			traces_array_obj->oid_oh = gather->trace_oids_ohs[i];
			traces_array_obj->oids = NULL;
			traces_array_obj->io_parameters = NULL;
		}
		rc = traces_array_obj_open(&traces_array_obj->oid_oh, O_RDWR,
					   NULL, -1, curr_idx,
					   sizeof(daos_obj_id_t),500 *
					   sizeof(daos_obj_id_t));
		DSG_ERROR(rc, "Error Opening Traces Array Object", end);
		rc = traces_array_obj_update_oids(traces_array_obj, temp_gather,
						  -1, op_controller, parent_idx,
						  curr_idx);
		DSG_ERROR(rc, "Error Updating Traces Array Object oids", end);
		rc = traces_array_obj_close(traces_array_obj, NULL, -1,
					    curr_idx, 1);
		DSG_ERROR(rc, "Error Closing Traces Array Object", end);

		temp = temp->next;
	}

end:	return rc;
}

int
gather_obj_fetch_gather_metadata_and_traces(gather_obj_t *gather_obj,
					    char *dkey,
					    operations_controllers_t *op_controller,
					    ensemble_list *ensembles_list,
					    int parent_idx, int *curr_idx)
{
	seismic_object_oid_oh_t 	oid_oh;
	traces_array_obj_t 		*traces_array_obj;
	trace_hdr_obj_t 		**trace_hdr;
	trace_t 			**traces;
	int 				num_of_traces;
	int 				rc;
	int 				i;

	rc = 0;

	traces_array_obj = malloc(sizeof(traces_array_obj_t));

	rc = gather_obj_fetch_gather_num_of_traces(gather_obj, NULL, dkey,
						   &num_of_traces, -1, curr_idx);
	DSG_ERROR(rc, "Failed to fetch gather object number of traces\n", end);

	rc = gather_obj_fetch_gather_traces_array_oid(gather_obj, NULL, dkey,
						      &(traces_array_obj->oid_oh.oid),
						      -1, curr_idx);
	DSG_ERROR(rc, "Failed to fetch gather object traces array oids\n", end);

	traces_array_obj->oids = malloc(num_of_traces * sizeof(daos_obj_id_t));

	rc = traces_array_obj_open(&traces_array_obj->oid_oh, O_RDWR, NULL, -1,
				   curr_idx, sizeof(daos_obj_id_t),
				   500 * sizeof(daos_obj_id_t));
	DSG_ERROR(rc, "Error Opening Traces Array Object", end2);

	rc = traces_array_obj_fetch_oids(traces_array_obj, NULL, -1, curr_idx,
					 traces_array_obj->oids, num_of_traces,
					 0);
	DSG_ERROR(rc, "Error Fetching Traces Array Object oids", end2);

	trace_hdr = malloc(num_of_traces *
					     sizeof(trace_hdr_obj_t*));
	traces = malloc(num_of_traces * sizeof(trace_t*));
	for (i = 0; i < num_of_traces; i++) {
		trace_hdr[i] = malloc(sizeof(trace_hdr_obj_t));
		oid_oh.oid = traces_array_obj->oids[i];
		trace_hdr[i]->oid_oh.oid = traces_array_obj->oids[i];
		rc = trace_hdr_obj_open(&(trace_hdr[i]->oid_oh), O_RDWR, NULL,
					-1, curr_idx);
		DSG_ERROR(rc, "Error Opening Traces Header Object", end2);

		trace_hdr[i]->trace = NULL;
		traces[i] = malloc(sizeof(trace_t));
		traces[i]->data = NULL;
		traces[i]->trace_header_obj = oid_oh.oid;
		rc = trace_hdr_fetch_headers(trace_hdr[i], traces[i],
					     op_controller, parent_idx,
					     curr_idx);
		DSG_ERROR(rc, "Fetching trace headers failed", end2);
	}

	if (op_controller != NULL) {
		rc = wait_all_events(op_controller);
	}

	for (i = 0; i < num_of_traces; i++) {
		rc = ensemble_list_add_trace(traces[i], ensembles_list, i);
		DSG_ERROR(rc, "Adding Trace to ensemble failed");
		rc = trace_hdr_obj_release_io_parameters(trace_hdr[i]);
		DSG_ERROR(rc, "Error Releasing io parameters", end3);
		rc = trace_hdr_obj_close(trace_hdr[i], NULL, -1, curr_idx, 1);
		DSG_ERROR(rc, "Closing Trace Header Object failed", end3);
	}

	rc = traces_array_obj_close(traces_array_obj, NULL, -1, curr_idx, 1);
	DSG_ERROR(rc, "Closing Traces array object failed", end3);
	free(trace_hdr);
	free(traces);
	return rc;

end3: 	free(trace_hdr);
	free(traces);
end2: 	free(traces_array_obj->oids);
end: 	free(traces_array_obj);
	return rc;
}

int
gather_obj_update_gather_metadata_and_traces(gather_obj_t *gather_obj,
					     char *dkey,
					     operations_controllers_t *op_controller,
					     ensemble_list *ensembles,
					     int ensemble_index, int parent_idx,
					     int *curr_idx)
{
	seismic_object_oid_oh_t oid_oh;
	trace_hdr_obj_t 	**trace_hdr;
	ensemble_t 		*en;
	trace_t 		*tr;
	node_t 			*temp_ensemble;
	node_t 			*temp_trace;
	int 			rc;
	int 			i;

	rc = 0;
	i = 0;


	if (ensemble_index >= ensembles->ensembles->size) {
		return -1;
	}
	temp_ensemble = ensembles->ensembles->head;
	while (i < ensemble_index) {
		i++;
		temp_ensemble = temp_ensemble->next;
	}
	en = (ensemble_t*)
	     doubly_linked_list_get_object(temp_ensemble,
			     	           offsetof(ensemble_t, n));
	temp_trace = en->traces->head;
	trace_hdr = malloc(en->traces->size * sizeof(trace_hdr_obj_t*));
	for (i = 0; i < en->traces->size; i++) {
		tr = (trace_t*)
		     doubly_linked_list_get_object(temp_trace,
				     	     	   offsetof(trace_t, n));
		trace_hdr[i] = malloc(sizeof(trace_hdr_obj_t));
		trace_hdr[i]->oid_oh.oid = tr->trace_header_obj;
		trace_hdr[i]->trace = NULL;
		trace_hdr[i]->daos_mode = O_RDWR;
		rc = trace_hdr_update_headers(trace_hdr[i], tr, op_controller,
					      -1, curr_idx);
		trace_hdr_obj_release(trace_hdr[i]);
		temp_trace = temp_trace->next;
	}

	free(trace_hdr);
	return rc;
}

int
gather_obj_get_dkeys_array(gather_obj_t *seismic_obj, int num_of_gathers,
			   char **dkeys_list)
{

	daos_key_desc_t 	*kds;
	daos_anchor_t 		anchor = { 0 };
	d_sg_list_t 		sglo;
	d_iov_t 		iov_temp;
	uint32_t 		nr;
	char 			*temp_array;
	int 			temp_array_offset;
	int 			keys_read;
	int 			kds_i;
	int 			rc;
	int 			z;
	int 			off;
	int 			k;


	nr = num_of_gathers + 3;
	keys_read = 0;
	kds_i = 0;
	rc = 0;
	temp_array_offset = 0;
	off = 0;
	k = 0;
	temp_array = malloc(nr * MAX_KEY_LENGTH * sizeof(char));
	kds = malloc((nr) * sizeof(daos_key_desc_t));
	sglo.sg_nr_out = sglo.sg_nr = 1;
	sglo.sg_iovs = &iov_temp;

	while (!daos_anchor_is_eof(&anchor)) {
		nr = (num_of_gathers + 3) - keys_read;
		d_iov_set(&iov_temp, temp_array + temp_array_offset,
			  nr * MAX_KEY_LENGTH);

		rc = daos_obj_list_dkey(seismic_obj->oid_oh.oh, DAOS_TX_NONE,
					&nr, &kds[keys_read], &sglo, &anchor,
					NULL);

		for (kds_i = 0; kds_i < nr; kds_i++) {
			temp_array_offset += kds[keys_read + kds_i].kd_key_len;
		}
		keys_read += nr;
		if (keys_read == (num_of_gathers + 3)) {
			break;
		}
	}
	char message[100];
	sprintf(message, "Listing <%s> seismic object akeys failed,"
		" error code = %d\n", seismic_obj->name, rc);
	DSG_ERROR(rc, message);


	for (z = 0; z < keys_read; z++) {
		char temp[MAX_KEY_LENGTH] = "";
		strncpy(temp, &temp_array[off], kds[z].kd_key_len);
		if (strcmp(temp, DS_D_NGATHERS) == 0 ||
		    strcmp(temp, DS_D_SKEYS) == 0 ||
		    strcmp(temp, DS_D_DKEYS_LIST) == 0) {
			off += kds[z].kd_key_len;
			continue;
		}
		dkeys_list[k] = malloc((kds[z].kd_key_len + 1) * sizeof(char));
		strncpy(dkeys_list[k], &temp_array[off], kds[z].kd_key_len);
		dkeys_list[k][kds[z].kd_key_len] = '\0';
		off += kds[z].kd_key_len;
		k++;
	}
	free(kds);
	free(temp_array);

	return rc;
}

int
gather_obj_update_dkeys_list(gather_obj_t *gather_obj, int st_idx,
			     int num_of_records, generic_value *values,
			     operations_controllers_t *op_controller,
			     int parent_idx,int *curr_idx)
{

	unsigned int 		iod_nr;
	uint64_t 		iod_size;
	uint64_t 		rx_idx;
	uint64_t 		rx_nr;
	int 			num_of_gathers;
	int 			rc;

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
	iod_size = sizeof(generic_value);
	rx_idx = st_idx;
	rx_nr = num_of_records;
	iod_nr = 1;
	rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_DKEYS_LIST, 1);
	rc = gather_obj_set_io_parameters(gather_obj, DS_A_DKEYS_LIST,
					  DAOS_IOD_ARRAY, iod_size, iod_nr,
					  &rx_idx, &rx_nr,(char*) values,
					  num_of_gathers * sizeof(generic_value));

	rc = gather_obj_update(gather_obj, op_controller, parent_idx, curr_idx);
	rc = gather_obj_release_io_parameters(gather_obj);
	return rc;
}

int
gather_obj_get_dkey_num_of_akeys(gather_obj_t *gather_obj, char *dkey_name,
				 int expected_num_of_keys)
{
	daos_key_desc_t 	*kds;
	daos_anchor_t 		anchor = { 0 };
	d_sg_list_t 		sglo;
	daos_key_t 		dkey;
	uint32_t 		nr;
	d_iov_t 		iov_temp;
	char 			*temp_array;
	int 			temp_array_offset;
	int 			rc;
	int 			keys_read;
	int 			kds_i;


	rc = 0;
	nr = expected_num_of_keys;
	temp_array_offset = 0;
	keys_read = 0;
	kds_i = 0;

	d_iov_set(&dkey, (void*) dkey_name, strlen(dkey_name));


	temp_array = malloc(nr * MAX_KEY_LENGTH * sizeof(char));
	kds = malloc((nr) * sizeof(daos_key_desc_t));
	sglo.sg_nr_out = sglo.sg_nr = 1;
	sglo.sg_iovs = &iov_temp;

	while (!daos_anchor_is_eof(&anchor)) {
		nr = nr - keys_read;
		d_iov_set(&iov_temp, temp_array + temp_array_offset,
			  nr * MAX_KEY_LENGTH);
		rc =
		daos_obj_list_akey(gather_obj_get_oid_oh(gather_obj)->oh,
				   DAOS_TX_NONE, &dkey, &nr, &kds[keys_read],
				   &sglo, &anchor,NULL);

		for (kds_i = 0; kds_i < nr; kds_i++) {
			temp_array_offset += kds[keys_read + kds_i].kd_key_len;
		}
		if (nr == 0) {
			break;
		}
		keys_read += nr;
	}
	char message[100];
	sprintf(message, "Listing <%s> seismic object akeys failed,"
			" error code = %d\n", gather_obj_get_name(gather_obj),
			rc);
	DSG_ERROR(rc, message);
	free(kds);
	free(temp_array);

	return keys_read;
}

int
gather_obj_update_gather_num_of_traces(gather_obj_t *gather_obj, char *dkey,
				       int *num_of_traces,
				       operations_controllers_t *op_controller,
				       int parent_idx, int *curr_idx)
{
	uint64_t 	iod_nr;
	uint64_t 	iod_size;
	int 		rc;

	iod_nr = 1;
	iod_size = sizeof(int);

	rc = gather_obj_init_io_parameters(gather_obj, 0, dkey, 1);

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NTRACES,
					  DAOS_IOD_SINGLE, iod_size, iod_nr,
					  NULL, NULL,(char*) num_of_traces,
					  sizeof(int));

	rc = gather_obj_update(gather_obj, NULL, -1, curr_idx);

	char message[100];
	sprintf(message, "Writing Traces Number under %s failed"
			" error code = %d\n", dkey, rc);
	DSG_ERROR(rc, message);
	rc = gather_obj_release_io_parameters(gather_obj);

	return rc;
}

int
gather_obj_update_gather_unique_value(gather_obj_t *gather_obj, char *dkey,
				      generic_value *value,
				      operations_controllers_t *op_controller,
				      int parent_idx, int *curr_idx)
{
	uint64_t 	iod_nr;
	uint64_t 	iod_size;
	int 		rc = 0;

	iod_nr = 1;
	iod_size = sizeof(generic_value);

	rc = gather_obj_init_io_parameters(gather_obj, 0, dkey, 1);
	rc = gather_obj_set_io_parameters(gather_obj, DS_A_UNIQUE_VAL,
					  DAOS_IOD_SINGLE, iod_size, iod_nr,
					  NULL, NULL,(char*) value,
					  sizeof(generic_value));
	rc = gather_obj_update(gather_obj, NULL, -1, curr_idx);
	DSG_ERROR(rc, "Adding unique value key to seismic object failed\n");
	rc = gather_obj_release_io_parameters(gather_obj);
	return rc;

}

int
gather_obj_update_gather_traces_array_oid(gather_obj_t *gather_obj, char *dkey,
					  daos_obj_id_t *oid,
					  operations_controllers_t *op_controller,
					  int parent_idx, int *curr_idx)
{
	uint64_t 	iod_nr;
	uint64_t 	iod_size;
	int 		rc;
	iod_nr = 1;
	iod_size = sizeof(daos_obj_id_t);
	rc = gather_obj_init_io_parameters(gather_obj, 0, dkey, 1);
	rc = gather_obj_set_io_parameters(gather_obj, DS_A_GATHER_TRACE_OIDS,
					  DAOS_IOD_SINGLE, iod_size, iod_nr,
					  NULL, NULL,(char*) oid,
					  sizeof(daos_obj_id_t));
	rc = gather_obj_update(gather_obj, op_controller, -1, curr_idx);
	DSG_ERROR(rc, "Writing Traces array oid under gather failed\n");
	rc = gather_obj_release_io_parameters(gather_obj);
	return rc;
}

int
gather_obj_update_skeys(gather_obj_t *gather_obj,
			operations_controllers_t *op_controller,
			int num_of_keys,
			char **keys, int parent_idx, int *curr_idx)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int 		rc;
	int 		i;

	if (num_of_keys > 1) {
		rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_SKEYS,
						   (num_of_keys + 1));
	}else {
		rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_SKEYS,
						   1);
	}

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NUM_OF_KEYS,
					  DAOS_IOD_SINGLE, sizeof(int), 1,
					  NULL, NULL,(char*) &num_of_keys,
					  sizeof(int));

	if (num_of_keys > 1) {
		rx_idx = 0;
		rx_nr = MAX_KEY_LENGTH;

		for (i = 0; i < num_of_keys; i++) {
			char temp[10] = "";
			char akey[MAX_KEY_LENGTH] = "";
			sprintf(temp, "%d", i);
			strcpy(akey, DS_A_KEYS);
			strcat(akey, temp);

			rc = gather_obj_set_io_parameters(gather_obj, akey,
							  DAOS_IOD_ARRAY, 1, 1,
							  &rx_idx, &rx_nr,
							  (char*) keys[i],
							  MAX_KEY_LENGTH *
							  sizeof(char));
		}
	}
	rc = gather_obj_update(gather_obj, NULL, parent_idx, curr_idx);

	rc = gather_obj_release_io_parameters(gather_obj);

	return rc;
}

int
gather_obj_fetch_num_of_skeys(gather_obj_t *gather_obj, int parent_idx,
			      int *curr_idx, int *num_of_keys)
{
	int rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_SKEYS, 1);
	DSG_ERROR(rc, "Preparing seismic gather object io parameters failed\n",
		  end);

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NUM_OF_KEYS,
					  DAOS_IOD_SINGLE, sizeof(int), 1,
					  NULL, NULL,(char*) num_of_keys,
					  sizeof(int));
	DSG_ERROR(rc, "Setting seismic gather object io parameters failed \n",
		  end);

	rc = gather_obj_fetch(gather_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching number of skeys from gather object failed \n");
	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc,"Releasing seismic gather object io parameters failed \n");

end:	return rc;
}

int
gather_obj_fetch_skeys(gather_obj_t *gather_obj,
		       operations_controllers_t *op_controller,
		       int parent_idx,int *curr_idx,
		       int num_of_keys, char **keys)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int 		rc;
	int 		i;


	rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_SKEYS,
			num_of_keys);
	DSG_ERROR(rc, "Preparing seismic gather object io parameters failed\n",
		  end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	for (i = 0; i < num_of_keys; i++) {
		char temp[10] = "";
		char akey[MAX_KEY_LENGTH] = "";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey, temp);

		rc = gather_obj_set_io_parameters(gather_obj, akey,
						  DAOS_IOD_ARRAY, 1, 1, &rx_idx,
						  &rx_nr,
						  (char*) keys[i],
						  MAX_KEY_LENGTH * sizeof(char));
		DSG_ERROR(rc,"Setting seismic gather object "
			  "io parameters failed \n",end);
	}
	rc = gather_obj_fetch(gather_obj, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching skeys from gather object failed \n");
	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc,"Releasing seismic gather object io parameters failed \n");

end:	return rc;
}

int
gather_obj_add_new_skey(gather_obj_t *gather_obj, char *key, int num_of_keys,
			operations_controllers_t *op_controller, int parent_idx,
			int *curr_idx)
{
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	int 		rc;

	rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, DS_D_SKEYS, 2);
	DSG_ERROR(rc, "Preparing seismic gather object io parameters failed\n",
		  end);

	rc = gather_obj_set_io_parameters(gather_obj, DS_A_NUM_OF_KEYS,
					  DAOS_IOD_SINGLE, sizeof(int), 1,
					  NULL, NULL,(char*) &num_of_keys,
					  sizeof(int));
	DSG_ERROR(rc, "Setting seismic gather object io parameters failed \n",
		  end);

	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH;

	char temp[10] = "";
	char akey[MAX_KEY_LENGTH] = "";
	sprintf(temp, "%d", (num_of_keys - 1));
	strcpy(akey, DS_A_KEYS);
	strcat(akey, temp);

	rc = gather_obj_set_io_parameters(gather_obj, akey, DAOS_IOD_ARRAY, 1,
					  1, &rx_idx, &rx_nr, key,
					  MAX_KEY_LENGTH * sizeof(char));
	DSG_ERROR(rc, "Setting seismic gather object io parameters failed \n",
		  end);
	rc = gather_obj_update(gather_obj, op_controller, -1, curr_idx);
	DSG_ERROR(rc, "Updating soids under gather object failed \n");
	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc,"Releasing seismic gather object io parameters failed \n");

end:	return rc;
}

int
gather_obj_update_soids(gather_obj_t *gather_obj, seismic_object_oid_oh_t *oids,
			int num_of_keys, char **keys, char *dkey,
			operations_controllers_t *op_controller, int parent_idx,
			int *curr_idx)
{
	int 	rc;
	int 	i;

	rc = 0;

	rc = gather_obj_init_io_parameters(gather_obj, 0, dkey, num_of_keys);
	DSG_ERROR(rc, "Preparing seismic gather object io parameters failed\n",
		  end);

	for (i = 0; i < num_of_keys; i++) {
		rc = gather_obj_set_io_parameters(gather_obj, keys[i],
						  DAOS_IOD_SINGLE,
						  sizeof(daos_obj_id_t), 1,
						  NULL,NULL,(char*) &oids[i].oid,
						  sizeof(daos_obj_id_t));
		DSG_ERROR(rc,"Setting seismic gather object "
			  "io parameters failed \n",end);
	}

	rc = gather_obj_update(gather_obj, NULL, parent_idx, curr_idx);
	DSG_ERROR(rc, "Updating soids under gather object failed \n");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc,"Releasing seismic gather object io parameters failed \n");

end:	return rc;
}

int
gather_obj_fetch_soids(gather_obj_t *gather_obj, char *dkey, char **keys,
		       int num_of_keys, seismic_object_oid_oh_t *oids,
		       operations_controllers_t *op_controller, int parent_idx,
		       int *curr_idx)
{
	int rc;
	int i;

	rc = gather_obj_init_io_parameters(gather_obj, 0, dkey, num_of_keys);
	DSG_ERROR(rc, "Preparing seismic gather object io parameters failed\n",
		  end);

	for (i = 0; i < num_of_keys; i++) {
		rc = gather_obj_set_io_parameters(gather_obj, keys[i],
						  DAOS_IOD_SINGLE,
						  sizeof(daos_obj_id_t), 1, NULL,
						  NULL, (char*) &oids[i].oid,
						  sizeof(daos_obj_id_t));
		DSG_ERROR(rc,"Setting seismic gather object "
			  "io parameters failed \n",
			  end);
	}

	rc = gather_obj_fetch(gather_obj, op_controller, parent_idx, curr_idx);
	DSG_ERROR(rc, "Fetching soids from gather object failed \n");

	rc = gather_obj_release_io_parameters(gather_obj);
	DSG_ERROR(rc,"Releasing seismic gather object io parameters failed \n");

end:	return rc;
}

int
gather_obj_create_init_new_obj(gather_obj_t **gather_obj, char *key)
{
	int curr_idx;
	int rc;

	rc = 0;

	rc = gather_obj_create(gather_obj, key, O_RDWR, LINKED_LIST);
	DSG_ERROR(rc, "Failed creating gather object", end);
	rc = gather_obj_open(gather_obj_get_oid_oh(*gather_obj), O_RDWR, NULL,
			     -1, &curr_idx);
	rc = gather_obj_update_num_of_gathers(*gather_obj,
					      &(*gather_obj)->number_of_gathers,
					      NULL, -1, &curr_idx);
	rc = gather_obj_update_skeys(*gather_obj, NULL,
				    (*gather_obj)->num_of_keys, NULL, -1,
				    &curr_idx);
	rc = gather_obj_close(*gather_obj, NULL, -1, &curr_idx, 0);

end:	return rc;
}

int
gather_obj_get_num_of_skeys(gather_obj_t *gather_obj)
{
	return gather_obj->num_of_keys;
}

int
gather_obj_add_complex_gather(seismic_object_oid_oh_t complex_oid,
			      ensemble_list *final_list, int index)
{
	traces_array_obj_t 	*traces_array_obj;
	gather_node_t 		*gather;
	gather_obj_t 		*gather_obj;
	ensemble_t 		*curr_ensemble;
	trace_t 		*curr_trace;
	node_t 			*ensemble_node;
	node_t 			*trace_node;
	int 			curr_idx;
	int 			size;
	int 			rc;
	int 			i;
	int 			j;

	size= 0;
	rc = 0;
	i = 0;
	j = 0;

	ensemble_node = final_list->ensembles->head;
	gather = malloc(sizeof(gather_node_t));
	gather->number_of_traces = 0;

	for (i = 0; i < final_list->ensembles->size; i++) {
		curr_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
		trace_node = curr_ensemble->traces->head;
		size += curr_ensemble->traces->size;
		gather->oids = (daos_obj_id_t*)
				realloc(gather->oids,
					size * sizeof(daos_obj_id_t));
		while (trace_node != NULL) {
			curr_trace =
				doubly_linked_list_get_object(trace_node,
						              offsetof(trace_t, n));
			gather->oids[j] = curr_trace->trace_header_obj;
			gather->number_of_traces++;
			j++;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}

	rc = traces_array_obj_create(&traces_array_obj, O_RDWR, gather->oids,
				     gather->number_of_traces);
	rc = traces_array_obj_open(&traces_array_obj->oid_oh, O_RDWR, NULL, -1,
				   &curr_idx, sizeof(daos_obj_id_t),
				   500 * sizeof(daos_obj_id_t));
	rc = traces_array_obj_update_oids(traces_array_obj, gather, -1, NULL,
					  -1, &curr_idx);
	rc = gather_obj_open(&complex_oid, O_RDWR, NULL, -1, &curr_idx);
	rc = gather_obj_fetch_entries(&gather_obj, complex_oid,
			"	      COMPLEX_GATHER", 0);
	char temp[10] = "";
	sprintf(temp, "%d", index);
	rc = gather_obj_update_gather_num_of_traces(gather_obj, temp,
						    &gather->number_of_traces,
						    NULL, -1, &curr_idx);
	rc = gather_obj_update_gather_traces_array_oid(gather_obj, temp,
						       &traces_array_obj->oid_oh.oid,
						       NULL, -1, &curr_idx);
	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
	rc = traces_array_obj_close(traces_array_obj, NULL, -1, &curr_idx, 1);

	free(gather->oids);
	free(gather);

	return rc;
}

int
gather_obj_fetch_complex_gather(seismic_object_oid_oh_t complex_oid,
				ensemble_list *final_list, int index)
{
	traces_array_obj_t 	*traces_array_obj;
	gather_obj_t 		*gather_obj;
	ensemble_t 		*curr_ensemble;
	trace_t 		*curr_trace;
	node_t 			*ensemble_node;
	node_t 			*trace_node;
	int 			num_of_traces;
	int 			curr_idx;
	int 			size;
	int 			rc;
	int 			j;
	int 			i;


	num_of_traces = 0;
	size = 0;
	rc = 0;
	j = 0;
	i = 0;

	ensemble_node = final_list->ensembles->head;

	char temp[10] = "";
	sprintf(temp, "%d", index);

	rc = gather_obj_open(&complex_oid, O_RDWR, NULL, -1, &curr_idx);

	rc = gather_obj_fetch_entries(&gather_obj, complex_oid,
				      "COMPLEX_GATHER", 0);

	rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj, temp, NULL,
							 final_list, -1,
							 &curr_idx);

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);

	return rc;
}

int
gather_obj_add_gathers_metadata_to_list(gather_obj_t *gather_obj,
					operations_controllers_t *op_controller,
					int parent_idx, int *curr_idx)
{
	char		**dkeys;
	int		num_of_gathers;
	int		rc;
	int 		j;

	num_of_gathers =
		gather_obj_get_number_of_gathers(gather_obj);

	gather_obj->trace_oids_ohs =
			malloc(num_of_gathers
			       * sizeof(seismic_object_oid_oh_t));
	dkeys = malloc(num_of_gathers * sizeof(char*));

	gather_obj_prepare_dkeys(gather_obj, dkeys, 1, num_of_gathers);

	for (j = 0; j < num_of_gathers; j++)
	{

		rc =
		gather_obj_fetch_gather_traces_array_oid(gather_obj,
							 NULL, dkeys[j],
							 &(gather_obj->trace_oids_ohs[j].oid),
							 -1, curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object traces array oids",
			  end);

		gather_node_t 		gather;

		rc = gather_obj_fetch_gather_unique_value(gather_obj, NULL,
							  dkeys[j],
							  &gather.unique_key,
							  -1, curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object unique value",
			  end);

		rc = gather_obj_fetch_gather_num_of_traces(gather_obj, NULL,
							   dkeys[j],
							   &gather.number_of_traces,
							   -1, curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object number of traces",
		          end);
		gather.oids = NULL;
		gather.curr_oids_index = 0;
		gather_obj_add_gather(&gather, gather_obj->gathers_list);
	}

end:	rc = release_tokenized_array(dkeys, STRING, num_of_gathers);

	return rc;
}

int
gather_obj_init(gather_obj_t **gather_obj)
{
	*gather_obj = (gather_obj_t*) malloc(sizeof(gather_obj_t));

	(*gather_obj)->trace_oids_ohs = NULL;
	(*gather_obj)->unique_values = NULL;
	(*gather_obj)->name = NULL;
	(*gather_obj)->keys = NULL;
	(*gather_obj)->io_parameters = NULL;
	(*gather_obj)->gathers_list = NULL;

	return 0;
}

