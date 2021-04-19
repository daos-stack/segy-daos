/*
 * seismic_graph_api.c
 *
 *  Created on: Feb 3, 2021
 *      Author: mirnamoawad
 */

#include "api/seismic_graph_api.h"

int
gather_obj_build_indexing(seismic_object_oid_oh_t oid_oh, char *gather_name,
			  ensemble_list *ensembles_list);

int
daos_seis_create_graph(dfs_obj_t *parent, char *name, int num_of_keys,
		       char **keys, root_obj_t **root_obj, int flags,
		       int graph_permissions_and_type)
{
	seismic_object_oid_oh_t	*parent_oid_oh;
	seismic_object_oid_oh_t *gather_oid_oh;
	seismic_object_oid_oh_t *root_oid_oh;
	gather_obj_t 		**gather_obj;
	int 			curr_idx;
	int 			objmode;
	int 			rc;
	int 			i;

	if (parent != NULL) {
		parent_oid_oh = root_obj_get_parent_oid_oh(parent);
	}else {
		parent_oid_oh = NULL;
	}

	rc = root_obj_create(root_obj, flags, keys, num_of_keys,
			     graph_permissions_and_type, parent_oid_oh);
	DSG_ERROR(rc, "Failed to create root object while creating graph",
		  end);

	rc = root_obj_insert_in_dfs(*root_obj, name);
	DSG_ERROR(rc, "Failed to insert root object in dfs", end);

	root_oid_oh = root_obj_get_id_oh(*root_obj);
	objmode = root_obj_get_mode(*root_obj);

	rc = root_obj_open(root_oid_oh, objmode, NULL, -1, &curr_idx);

	DSG_ERROR(rc, "Failed to open root object", end);

	rc = root_obj_update_keys(*root_obj, NULL, num_of_keys, keys, -1,
				  &curr_idx);
	DSG_ERROR(rc, "Failed to update root object keys");

	gather_oid_oh = malloc(num_of_keys * sizeof(seismic_object_oid_oh_t));
	gather_obj = malloc(num_of_keys * sizeof(gather_obj_t*));

	for (i = 0; i < num_of_keys; i++) {
		rc = gather_obj_create(&gather_obj[i], keys[i], flags,
				       LINKED_LIST);
		DSG_ERROR(rc, "Failed to create gather object", end);

		gather_oid_oh[i] = *(gather_obj_get_oid_oh(gather_obj[i]));

		rc = gather_obj_open(&gather_obj[i]->oid_oh, O_RDWR, NULL, -1,
				     &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object", end);

		int 		num_of_gathers;

		num_of_gathers = gather_obj[i]->number_of_gathers;

		rc = gather_obj_update_num_of_gathers(gather_obj[i],
						      &(num_of_gathers), NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object num of gathers",
			  end);

		rc = gather_obj_update_skeys(gather_obj[i], NULL,
					     gather_obj[i]->num_of_keys, NULL,
					     -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object num of skeys",
			  end);

		rc = gather_obj_close(gather_obj[i], NULL, -1, &curr_idx, 0);
		DSG_ERROR(rc, "Failed to close gather object", end);
	}
	rc = root_obj_update_gather_oids(*root_obj, NULL, keys, num_of_keys,
					 gather_oid_oh, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update root object gather oids");

	for (i = 0; i < num_of_keys; i++) {
		rc = gather_obj_release(gather_obj[i]);
		DSG_ERROR(rc, "Failed to release Gather object");
	}

	int 	num_of_traces = 0;

	rc = root_obj_update_num_of_traces(*root_obj, NULL, &num_of_traces, -1,
					   &curr_idx);
	DSG_ERROR(rc, "Failed to update root object number of traces");

	int	num_of_variations = 0;

	rc = root_obj_update_num_of_variations(*root_obj, NULL,
					       &num_of_variations,
					       -1, &curr_idx);

	DSG_ERROR(rc,"Failed to update root object number of variations\n");

	complex_gather_obj_t *complex_gather;
	rc = complex_gather_obj_create(&complex_gather, O_RDWR);
	rc = root_obj_update_complex_oid(*root_obj, NULL, -1, &curr_idx,
					 &(complex_gather->oid_oh));
	rc = complex_gather_obj_release(complex_gather);

	free(gather_obj);
	free(parent_oid_oh);
	free(gather_oid_oh);

	rc = root_obj_close(*root_obj, NULL, -1, &curr_idx, 0);
	DSG_ERROR(rc, "Failed to close root object");

end:	return rc;
}

root_obj_t*
daos_seis_open_graph(const char *path, int flags)
{
	root_obj_t *root_obj;
	dfs_obj_t  *dfs_obj;
	int 	   rc;

	rc = lookup_dfs_obj(path, flags, &dfs_obj);
	if (rc != 0) {
		char message[100];
		sprintf(message,"Looking up path <%s> in dfs failed,"
			" error code = %d", path, rc);
		DSG_ERROR(rc, message, end);
	}
	rc = root_obj_fetch_entries(&root_obj, dfs_obj);
	DSG_ERROR(rc, "Failed to fetch root object entries", end);

	return root_obj;

end: 	return NULL;
}

int
daos_seis_close_graph(root_obj_t *root_obj)
{
	int curr_idx;
	int rc = 0;

	rc = root_obj_close(root_obj, NULL, -1, &curr_idx, 1);
	DSG_ERROR(rc, "Closing Graph failed");

	return rc;
}

int
daos_seis_parse_segy(dfs_obj_t *segy_root, root_obj_t *root_obj)
{
	parse_functions_t *parse_functions;
	gather_obj_t 	  **seismic_obj;
	int 		  root_num_of_keys;
	int		  num_of_gathers;
	int 		  additional = 0;
	int 		  curr_idx;
	int 		  num_of_traces;
	int 		  rc;
	int 		  i;

	num_of_traces = root_obj_get_num_of_traces(root_obj);

	if (num_of_traces > 0) {
		additional = 1;
	}

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	/** used in case of additional file only */
	int		old_num_of_gathers[root_num_of_keys];

	seismic_obj = malloc(root_obj_get_num_of_keys(root_obj) *
			     sizeof(gather_obj_t*));

	for (i = 0; i < root_num_of_keys; i++) {
		rc = gather_obj_open(&(root_obj->gather_oids[i]), O_RDWR, NULL,
				     -1, &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object", end);

		rc = gather_obj_fetch_entries(&(seismic_obj[i]),
					      root_obj->gather_oids[i],
					      root_obj->keys[i], additional);
		DSG_ERROR(rc, "Failed to fetch gather object entries", end);
		old_num_of_gathers[i] =
				gather_obj_get_number_of_gathers(seismic_obj[i]);
		if (additional == 1) {
			rc = gather_obj_add_gathers_metadata_to_list(seismic_obj[i],
								     NULL, -1,
								     &curr_idx);
			DSG_ERROR(rc,"Failed to fetch gather object metadata "
				  "and adding it to gathers list ");
		}
	}

	rc = init_parsing_parameters(SEGY, &parse_functions, (void*)segy_root);
	DSG_ERROR(rc, "Failed to initialize segy parsing parameters", end);

	rc = parse_text_and_binary_headers(parse_functions);
	DSG_ERROR(rc, "Failed to parse text and binary headers");

	if (additional == 0) {
		char *ebcbuf = get_text_header(parse_functions);
		bhed *bh = (bhed*) get_binary_header(parse_functions);
		/** Write binary and text headers to root object */
		rc = root_obj_update_headers(root_obj, ebcbuf, bh, NULL, -1,
					     &curr_idx);
		DSG_ERROR(rc, "Failed to update root object headers ");
	}

	int nextended = get_number_of_extended_headers(parse_functions);

	if (nextended > 0) {
		rc = root_obj_update_extheaders(root_obj, parse_functions, NULL,
				nextended, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update root object extended headers \n");
	}

	int 		gather_num;
	trace_t 	*trace;
	while (1) {
		trace = get_trace(parse_functions);
		if (trace == NULL) {
			break;
		}
		root_obj->num_of_traces++;

		trace_hdr_obj_t 	*trace_hdr;

		rc = trace_hdr_obj_create(&trace_hdr, O_RDWR, trace);
		DSG_ERROR(rc, "Failed to create trace header object", end2);

		rc = trace_hdr_update_headers(trace_hdr, trace, NULL, -1,
					      &curr_idx);
		DSG_ERROR(rc, "Failed to update trace_hdr obj headers", end2);

		trace_data_obj_t 	*trace_data;

		rc = trace_data_obj_create(&trace_data, O_RDWR, trace->data,
					   trace->ns);
		DSG_ERROR(rc, "Failed to create trace data object", end2);

		rc = trace_data_update_data(trace_data, trace, NULL, -1,
					    &curr_idx);
		DSG_ERROR(rc, "Failed to update trace data object data", end2);

		for (i = 0; i < root_num_of_keys; i++) {
			rc = trace_linking(trace, seismic_obj[i],
					   root_obj->keys[i], NULL, -1,
					   &curr_idx);
			DSG_ERROR(rc,"Linking trace to gather object failed",
				  end2);
		}
		trace_hdr_obj_release(trace_hdr);
		trace_data_obj_release(trace_data);
	}

	for (i = 0; i < root_num_of_keys; i++) {
		num_of_gathers =
			gather_obj_get_number_of_gathers(seismic_obj[i]);

		rc = gather_obj_update_num_of_gathers(seismic_obj[i],
						      &(num_of_gathers), NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc,"Failed to update gather object number of gathers ",
			  end3);
		rc = gather_obj_create_traces_array(seismic_obj[i], NULL, -1,
						    &curr_idx,
						    old_num_of_gathers[i]);
		DSG_ERROR(rc, "Failed to create gather object traces array",
			  end3);
		rc = gather_obj_dump_gathers_list_in_graph(seismic_obj[i], NULL,
							   -1, &curr_idx,
							   old_num_of_gathers[i]);
		DSG_ERROR(rc, "Failed to dump gathers list in graph ");
end3: 		rc = gather_obj_close(seismic_obj[i], NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close gather object");
	}

	num_of_traces = root_obj_get_num_of_traces(root_obj);

	rc = root_obj_update_num_of_traces(root_obj, NULL, &num_of_traces,
					   -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update root object number of traces\n");

end2: 	rc = release_parsing_parameters(parse_functions);
	DSG_ERROR(rc, "Failed to release parsing parameters");

end: 	free(seismic_obj);

	return rc;

}

int
daos_seis_parse_raw_data(dfs_obj_t *raw_root, dfs_obj_t *hdr_root,
			 root_obj_t *root_obj, int ns, int ftn)
{
	parse_functions_t *raw_parse_functions;
	parse_functions_t *hdr_parse_functions;
	gather_obj_t 	  **seismic_obj;
	int 		  root_num_of_keys;
	int 		  additional = 0;
	int		  num_of_gathers;
	int 		  num_of_traces;
	int 		  curr_idx;
	int 		  ihead = 0;
	int 		  rc;
	int 		  i;

	num_of_traces = root_obj_get_num_of_traces(root_obj);
	if (num_of_traces > 0) {
		additional = 1;
	}
	root_num_of_keys = root_obj_get_num_of_keys(root_obj);
	/** used in case of additional file only */
	int 		old_num_of_gathers[root_num_of_keys];

	seismic_obj = malloc(root_num_of_keys * sizeof(gather_obj_t*));

	for (i = 0; i < root_num_of_keys; i++) {
		rc = gather_obj_open(&(root_obj->gather_oids[i]), O_RDWR, NULL,
				     -1, &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object ", end);

		rc = gather_obj_fetch_entries(&(seismic_obj[i]),
					      root_obj->gather_oids[i],
					      root_obj->keys[i], additional);
		DSG_ERROR(rc, "Failed to fetch gather object entries");

		old_num_of_gathers[i] =
				gather_obj_get_number_of_gathers(seismic_obj[i]);
		if (additional == 1) {
			rc = gather_obj_add_gathers_metadata_to_list(seismic_obj[i],
								     NULL, -1,
								     &curr_idx);
			DSG_ERROR(rc,"Failed to fetch gather object metadata "
				  "and adding it to gathers list ");
		}
	}

	rc = init_parsing_parameters(RAW, &raw_parse_functions,(void*)raw_root);
	DSG_ERROR(rc, "Failed to initialize raw parsing parameters ", end);

	set_raw_ns(&raw_parse_functions, ns);

	if (hdr_root != NULL) {
		rc = init_parsing_parameters(HEADER, &hdr_parse_functions,
					     (void*) hdr_root);
		DSG_ERROR(rc,"Failed to initialize header file parsing "
			  "parameters ", end);
		ihead = 1;
	}

	trace_t 	*hdr_trace;
	trace_t 	*trace;
	int 		gather_num;
	int 		itr = 0;
	while (1) {
		static int tracl = 0;
		/** If Fortran data, read past the record size bytes */
		if (ftn) {
			read_junk(raw_parse_functions);
		}
		trace = get_trace(raw_parse_functions);
		if (trace == NULL) {
			break;
		}
		if (ihead == 0) {
			trace->tracl = ++tracl;
		} else {
			hdr_trace = get_trace(hdr_parse_functions);
			memcpy(trace, hdr_trace, TRACEHDR_BYTES);
			trace_destroy(hdr_trace);
		}
		trace->trid = TREAL;
		root_obj->num_of_traces++;

		trace_hdr_obj_t 	*trace_hdr;

		rc = trace_hdr_obj_create(&trace_hdr, O_RDWR, trace);
		DSG_ERROR(rc, "Failed to create trace header object", end2);

		rc = trace_hdr_update_headers(trace_hdr, trace, NULL, -1,
					      &curr_idx);
		DSG_ERROR(rc, "Failed to update trace hdr object headers",
			  end2);

		trace_data_obj_t 	*trace_data;
		rc = trace_data_obj_create(&trace_data, O_RDWR, trace->data,
					   trace->ns);
		DSG_ERROR(rc, "Failed to create trace data object", end2);

		rc = trace_data_update_data(trace_data, trace, NULL, -1,
					    &curr_idx);
		DSG_ERROR(rc, "Failed to update trace data object data", end2);

		for (i = 0; i < root_num_of_keys; i++) {
			rc = trace_linking(trace, seismic_obj[i],
					   root_obj->keys[i], NULL, -1,
					   &curr_idx);
			DSG_ERROR(rc, "Linking trace to gather object failed ",
				  end2);
		}
		if (ftn) {
			read_junk(raw_parse_functions);
		}

		trace_hdr_obj_release(trace_hdr);
		trace_data_obj_release(trace_data);
		itr++;
	}

	for (i = 0; i < root_num_of_keys; i++) {
		num_of_gathers =
			gather_obj_get_number_of_gathers(seismic_obj[i]);

		rc = gather_obj_update_num_of_gathers(seismic_obj[i],
						      &num_of_gathers, NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc,"Failed to update gather object num of gathers ",
			  end3);

		rc = gather_obj_create_traces_array(seismic_obj[i], NULL, -1,
						    &curr_idx,
						    old_num_of_gathers[i]);
		DSG_ERROR(rc, "Failed to create gather object traces array",
			  end3);

		rc = gather_obj_dump_gathers_list_in_graph(seismic_obj[i],
							   NULL, -1, &curr_idx,
							   old_num_of_gathers[i]);
		DSG_ERROR(rc, "Failed to dump gathers list in graph ");

end3: 		gather_obj_close(seismic_obj[i], NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close gather object");
	}

	num_of_traces = root_obj_get_num_of_traces(root_obj);

	rc = root_obj_update_num_of_traces(root_obj, NULL, &num_of_traces,
					   -1, &curr_idx);
	DSG_ERROR(rc, "Failed to update root object number of traces");

end2: 	rc = release_parsing_parameters(raw_parse_functions);
	DSG_ERROR(rc, "Failed to release raw parsing parameters");

	if (hdr_root != NULL) {
		rc = release_parsing_parameters(hdr_parse_functions);
		DSG_ERROR(rc, "Failed to release header parsing parameters");
	}

end: 	free(seismic_obj);

	return rc;
}

int
daos_seis_get_num_of_traces(root_obj_t *root_obj)
{
	return root_obj_get_num_of_traces(root_obj);
}

char*
daos_seis_get_text_header(root_obj_t *root_obj)
{
	char *text_header;
	int  rc;

	text_header = malloc(EBCBYTES * sizeof(char));

	rc = root_obj_fetch_text_header(root_obj, text_header);
	DSG_ERROR(rc, "Failed to fetch text header ", end);

	return text_header;

end: 	free(text_header);

	return NULL;
}

bhed*
daos_seis_get_binary_header(root_obj_t *root_obj)
{
	bhed *binary_header;
	int  rc;

	binary_header = malloc(sizeof(bhed));

	rc = root_obj_fetch_binary_header(root_obj, binary_header);
	DSG_ERROR(rc, "Failed to fetch binary header ", end);

	return binary_header;

end: 	free(binary_header);
	return NULL;
}

int
daos_seis_get_num_of_gathers(root_obj_t *root_obj, char *key)
{
	operations_controllers_t *op_controller;
	seismic_object_oid_oh_t  oid_oh;
	ensemble_list 		 *ensembles_list;
	gather_obj_t 		 *new_gather_obj;
	gather_obj_t 		 *gather_obj;
	char			 **gather_obj_dkeys;
	char 			 *obj_key_name;
	int 			 root_num_of_keys;
	int 			 num_of_gathers;
	int 			 curr_idx;
	int 			 rc = 0;
	int 			 i;

	ensembles_list = init_ensemble_list();
	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));
	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to initialize operations controller ", end);

	for (i = 0; i < root_num_of_keys; i++) {
		if (strcmp(root_obj->keys[i], key) == 0) {
			oid_oh = root_obj->gather_oids[i];
			strcpy(obj_key_name, root_obj->keys[i]);
			break;
		}
	}

	if (i == root_num_of_keys) {

		int 	new_root_num_of_keys;

		new_root_num_of_keys = root_num_of_keys + 1;

		rc = gather_obj_create(&new_gather_obj, key, O_RDWR,
				       LINKED_LIST);
		DSG_ERROR(rc, "Failed to create gather object ", end);

		rc = gather_obj_open(&new_gather_obj->oid_oh, O_RDWR, NULL, -1,
				     &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object", end);

		num_of_gathers = new_gather_obj->number_of_gathers;

		rc = gather_obj_update_num_of_gathers(new_gather_obj,
						      &(num_of_gathers), NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object num of gathers",
			  end);

		rc = gather_obj_update_skeys(new_gather_obj, NULL,
					     new_gather_obj->num_of_keys, NULL,
					     -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object num of skeys",
			  end);

		rc = root_obj_link_new_gather_obj(root_obj,
						  new_gather_obj->oid_oh,
						  key, new_root_num_of_keys,
						  NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to link gather object to root", end);

		rc = gather_obj_close(new_gather_obj, NULL, -1, &curr_idx, 0);
		DSG_ERROR(rc, "Failed to close gather object", end);

		oid_oh = root_obj->gather_oids[0];
		strcpy(obj_key_name, root_obj->keys[0]);

		rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to open Gather object ", end);

		rc = gather_obj_fetch_entries(&gather_obj, oid_oh,
					      obj_key_name, 1);
		DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

		num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
		gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));

		gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1,
					 num_of_gathers);

		/** If key doesn't exist then start building the indexing
		 *  for a new object with new key */
		for (i = 0; i < num_of_gathers; i++) {
			rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
									 gather_obj_dkeys[i],
									 op_controller,
									 ensembles_list,
									 -1, &curr_idx);
			DSG_ERROR(rc,"Fetching gather object metadata and traces failed ",
				  end2);
			rc = gather_obj_build_indexing(new_gather_obj->oid_oh,
						       new_gather_obj->name,
						       ensembles_list);
			DSG_ERROR(rc, "Building indexing failed ", end2);

			rc = doubly_linked_list_delete_node(ensembles_list->ensembles,
							    ensembles_list->ensembles->head,
							    &destroy_ensemble,
							    offsetof(ensemble_t, n));
			DSG_ERROR(rc, "Deleting ensemble failed ", end2);
		}

		release_tokenized_array(gather_obj_dkeys, STRING, num_of_gathers);

		rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close gather object\n", end2);

		oid_oh = new_gather_obj->oid_oh;
		strcpy(obj_key_name, gather_obj_get_name(new_gather_obj));
		free(new_gather_obj);
	}

	rc = destroy_ensemble_list(ensembles_list);

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ", end3);

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open Gather object ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 0);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end3);

	rc = gather_obj_fetch_num_of_gathers(gather_obj, NULL,
					     &(num_of_gathers),
					     -1, &curr_idx);
	DSG_ERROR(rc, "Fetching gather object number of gathers failed ");

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
	DSG_ERROR(rc, "Closing gather object failed ");

end: 	free(obj_key_name);

	return num_of_gathers;

end2: 	release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

end3: 	free(obj_key_name);
	free(new_gather_obj);
	destroy_ensemble_list(ensembles_list);
	return rc;
}

key_value_pair_t*
daos_seis_get_custom_headers(root_obj_t *root_obj, char **keys, int num_of_keys)
{
	key_value_pair_t *kv;
	daos_obj_id_t 	 *trace_array_oids;
	gather_obj_t 	 *seismic_obj;
	char 		 **dkeys;
	int 		 num_of_gathers;
	int 		 gather_traces;
	int 		 total_traces;
	int 		 trace_idx = 0;
	int 		 curr_idx;
	int 		 rc;
	int 		 i;
	int		 j;

	total_traces = root_obj_get_num_of_traces(root_obj);

	rc = key_value_pair_init(&kv, keys, total_traces, num_of_keys);

	rc = gather_obj_open(&(root_obj->gather_oids[0]), O_RDWR, NULL, -1,
			     &curr_idx);
	DSG_ERROR(rc, "Failed to open gather object ", end);

	rc = gather_obj_fetch_entries(&seismic_obj, root_obj->gather_oids[0],
				      root_obj->keys[0], 1);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

	num_of_gathers = gather_obj_get_number_of_gathers(seismic_obj);

	trace_array_oids = malloc(num_of_gathers * sizeof(daos_obj_id_t));

	dkeys = malloc(num_of_gathers * sizeof(char*));

	gather_obj_prepare_dkeys(seismic_obj, dkeys, 1, num_of_gathers);

	for (i = 0; i < num_of_gathers; i++) {
		traces_array_obj_t 	*tr_array;

		rc = traces_array_obj_init(&tr_array);

		rc = gather_obj_fetch_gather_traces_array_oid(seismic_obj, NULL,
							      dkeys[i],
							      &(trace_array_oids[i]),
							      -1, &curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object traces array oids ",
			  end2);

		tr_array->oid_oh.oid = trace_array_oids[i];

		rc = traces_array_obj_open(&(tr_array->oid_oh), O_RDWR, NULL,
					   -1, &curr_idx, sizeof(daos_obj_id_t),
					   500 * sizeof(daos_obj_id_t));
		tr_array->daos_mode = O_RDWR;

		rc = gather_obj_fetch_gather_num_of_traces(seismic_obj, NULL,
							   dkeys[i],
							   &(tr_array->num_of_traces),
							   -1, &curr_idx);
		DSG_ERROR(rc,"Failed to fetch gather object number of traces ",
			  end2);

		gather_traces = tr_array->num_of_traces;
		tr_array->oids = malloc(gather_traces * sizeof(daos_obj_id_t));

		rc = traces_array_obj_fetch_oids(tr_array, NULL, -1, &curr_idx,
						 tr_array->oids, gather_traces, 0);
		DSG_ERROR(rc, "Failed to fetch traces array object oids ",
			  end2);

		for (j = 0; j < gather_traces; j++) {
			trace_hdr_obj_t *tr_hdr;

			rc = trace_hdr_obj_init(&tr_hdr);

			tr_hdr->oid_oh.oid = tr_array->oids[j];

			rc = trace_hdr_obj_open(&tr_hdr->oid_oh, O_RDWR, NULL,
						-1, &curr_idx);
			DSG_ERROR(rc, "Failed to open trace header object ",
				  end3);

			tr_hdr->daos_mode = O_RDWR;

			rc = trace_hdr_fetch_custom_headers(tr_hdr, kv, NULL,
						            -1, &curr_idx, keys,
							    num_of_keys,
							    trace_idx);
			DSG_ERROR(rc,"Failed to fetch trace header object "
				  "custom headers ", end3);

			trace_idx++;

end3: 			rc = trace_hdr_obj_close(tr_hdr, NULL, -1, &curr_idx,
						 1);
			DSG_ERROR(rc, "Failed to close trace header object ");
		}

end2: 		traces_array_obj_close(tr_array, NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close traces array object ");
	}

	release_tokenized_array(dkeys, STRING, num_of_gathers);

	free(trace_array_oids);

end: 	gather_obj_close(seismic_obj, NULL, -1, &curr_idx, 1);
	DSG_ERROR(rc, "Failed closing gather object ");

	return kv;
}

int
daos_seis_set_traces_data(root_obj_t *root_obj, ensemble_list *e)
{
	trace_data_obj_t *tr_data;
	ensemble_t 	 *temp_ensemble;
	trace_t 	 *temp_trace;
	node_t 		 *ensemble_node;
	node_t 	 	 *trace_node;
	int 	 	 parent_idx;
	int 		 curr_idx;
	int 		 i;
	int 		 j;
	int 		 rc = 0;

	ensemble_node = e->ensembles->head;

	for (i = 0; i < e->ensembles->size; i++) {
		temp_ensemble =
			(ensemble_t*)doubly_linked_list_get_object(ensemble_node,
								   offsetof(ensemble_t, n));
		trace_node = temp_ensemble->traces->head;

		for (j = 0; j < temp_ensemble->traces->size; j++) {
			temp_trace =
				(trace_t*)doubly_linked_list_get_object(trace_node,
									offsetof(trace_t, n));
			rc = trace_data_obj_init(&tr_data);
			tr_data->oid_oh.oid =
					trace_data_oid_calculation(&(temp_trace->trace_header_obj),
								   DAOS_OBJ_CLASS_ID);
			tr_data->daos_mode = O_RDWR;

			rc = trace_data_update_data(tr_data, temp_trace, NULL,
						    parent_idx, &curr_idx);
			DSG_ERROR(rc, "Failed to update trace data object data ");

			rc = trace_data_obj_release(tr_data);

			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	return rc;
}

int
daos_seis_set_headers(root_obj_t *root_obj, int num_of_keys, char **keys1,
		      double *a, double *b, double *c, double *d, double *e)
{
	operations_controllers_t *op_controller;
	seismic_object_oid_oh_t  oid_oh;
	ensemble_list 		 *ensembles_list;
	gather_obj_t 		 *gather_obj;
	char 			 **gather_obj_dkeys;
	char 			 *obj_key_name;
	int 			 root_num_of_keys;
	int 			 num_of_gathers;
	int 			 curr_idx;
	int 			 rc;
	int 			 i;

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));
	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to initialize operations controller ", end);

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);
	ensembles_list = init_ensemble_list();
	oid_oh = root_obj->gather_oids[0];
	strcpy(obj_key_name, root_obj->keys[0]);

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open Gather object ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 1);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);

	gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1,
				 num_of_gathers);

	for (i = 0; i < num_of_gathers; i++) {
		rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
								 gather_obj_dkeys[i],
								 op_controller,
								 ensembles_list,
								 -1, &curr_idx);
		DSG_ERROR(rc, "Failed to fetch gather metadata and traces ",
			  end2);

		rc = set_headers(ensembles_list, num_of_keys, keys1, a, b, c, d,
				 e, 0);

		rc = gather_obj_update_gather_metadata_and_traces(gather_obj,
								  gather_obj_dkeys[i],
								  NULL,
								  ensembles_list,
								  0, -1,
								  &curr_idx);
		DSG_ERROR(rc, "Failed to update gather metadata and traces ",
			  end2);

		doubly_linked_list_delete_node(ensembles_list->ensembles,
					       ensembles_list->ensembles->head,
					       &destroy_ensemble,
					       offsetof(ensemble_t, n));
	}

end2:	release_tokenized_array(gather_obj_dkeys, STRING, num_of_gathers);

end: 	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

	free(obj_key_name);

	return rc;
}

int
daos_seis_change_headers(root_obj_t *root_obj, int num_of_keys, char **keys1,
			 char **keys2, char **keys3, double *a, double *b,
			 double *c, double *d, double *e, double *f)
{
	operations_controllers_t *op_controller;
	seismic_object_oid_oh_t  oid_oh;
	ensemble_list 		 *ensembles_list;
	gather_obj_t 		 *gather_obj;
	char 			 **gather_obj_dkeys;
	char 			 *obj_key_name;
	int 			 root_num_of_keys;
	int 			 num_of_gathers;
	int 			 curr_idx;
	int 			 rc;
	int 			 i;

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));

	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to initialize operations controller ", end);

	root_num_of_keys = root_obj_get_num_of_keys(root_obj);
	ensembles_list = init_ensemble_list();

	oid_oh = root_obj->gather_oids[0];
	strcpy(obj_key_name, root_obj->keys[0]);

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open Gather object ", end2);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 1);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end2);

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
	gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1,
				 num_of_gathers);

	for (i = 0; i < num_of_gathers; i++) {
		rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
								 gather_obj_dkeys[i],
								 op_controller,
								 ensembles_list,
								 -1, &curr_idx);
		DSG_ERROR(rc, "Failed to fetch gather metadata and traces ",
			  end3);

		rc = change_headers(ensembles_list, num_of_keys, keys1, keys2,
				    keys3, a, b, c, d, e, f, 0);

		rc = gather_obj_update_gather_metadata_and_traces(gather_obj,
								  gather_obj_dkeys[i],
								  NULL,
								  ensembles_list,
								  0, -1,
								  &curr_idx);

		doubly_linked_list_delete_node(ensembles_list->ensembles,
					       ensembles_list->ensembles->head,
					       &destroy_ensemble,
					       offsetof(ensemble_t, n));
	}

end3:	release_tokenized_array(gather_obj_dkeys, STRING, num_of_gathers);

end2: 	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller ");

end: 	free(obj_key_name);

	return rc;
}

generic_value*
daos_seis_get_gather_unique_values(root_obj_t *root_obj, char *gather_name)
{
	operations_controllers_t *op_controller;
	seismic_object_oid_oh_t  oid_oh;
	generic_value 		 *values;
	ensemble_list 		 *ensembles_list;
	gather_obj_t 		 *new_gather_obj;
	gather_obj_t 		 *gather_obj;
	char 			 **gather_obj_dkeys;
	char 			 *obj_key_name;
	int 			 root_num_of_keys;
	int 			 num_of_gathers;
	int 			 curr_idx;
	int 			 rc;
	int 			 i;

	ensembles_list = init_ensemble_list();
	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));
	root_num_of_keys = root_obj_get_num_of_keys(root_obj);

	rc = init_operations_controller(&op_controller, 0, 1, 10000);
	DSG_ERROR(rc, "Failed to initialize operations controller ", end);

	for (i = 0; i < root_num_of_keys; i++) {
		if (strcmp(root_obj->keys[i], gather_name) == 0) {
			oid_oh = root_obj->gather_oids[i];
			strcpy(obj_key_name, root_obj->keys[i]);
			break;
		}
	}

	if (i == root_num_of_keys) {
		int 	new_root_num_of_keys;
		new_root_num_of_keys = root_num_of_keys + 1;

		rc = gather_obj_create(&new_gather_obj, gather_name, O_RDWR,
				       LINKED_LIST);
		DSG_ERROR(rc, "Failed to create gather object ", end);

		rc = gather_obj_open(&new_gather_obj->oid_oh, O_RDWR, NULL, -1,
				     &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object", end);


		num_of_gathers = new_gather_obj->number_of_gathers;

		rc = gather_obj_update_num_of_gathers(new_gather_obj,
						      &(num_of_gathers), NULL,
						      -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object num of gathers",
			  end);

		rc = gather_obj_update_skeys(new_gather_obj, NULL,
					     new_gather_obj->num_of_keys, NULL,
					     -1, &curr_idx);
		DSG_ERROR(rc, "Failed to update gather object num of skeys",
			  end);

		oid_oh = *gather_obj_get_oid_oh(new_gather_obj);

		rc = root_obj_link_new_gather_obj(root_obj,
						  oid_oh,
						  gather_name,
						  new_root_num_of_keys,
						  NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to link gather object to root", end);

		rc = gather_obj_close(new_gather_obj, NULL, -1, &curr_idx, 0);
		DSG_ERROR(rc, "Failed to close gather object", end);


		oid_oh = root_obj->gather_oids[0];
		strcpy(obj_key_name, root_obj->keys[0]);

		rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
		DSG_ERROR(rc, "Failed to open gather object ", end);

		rc = gather_obj_fetch_entries(&gather_obj, oid_oh,
					      obj_key_name, 1);
		DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

		num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
		gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));
		gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1,
					 num_of_gathers);

		/** If key doesn't exist then start building the indexing
		 *  for a new object with new key
		 */
		for (i = 0; i < num_of_gathers; i++) {

			rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
									 gather_obj_dkeys[i],
									 op_controller,
									 ensembles_list,
									 -1, &curr_idx);
			DSG_ERROR(rc,"Failed to fetch gather metadata & traces ",
				  end);
			oid_oh = *gather_obj_get_oid_oh(new_gather_obj);
			strcpy(obj_key_name,
			       gather_obj_get_name(new_gather_obj));

			rc = gather_obj_build_indexing(oid_oh, obj_key_name,
						       ensembles_list);
			DSG_ERROR(rc, "Failed to build indexing ", end);

			rc = doubly_linked_list_delete_node(ensembles_list->ensembles,
							    ensembles_list->ensembles->head,
							    &destroy_ensemble,
							    offsetof(ensemble_t, n));
			DSG_ERROR(rc, "Failed to delete ensemble ", end);
		}

		rc = release_tokenized_array(gather_obj_dkeys, STRING,
					     num_of_gathers);

		rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
		DSG_ERROR(rc, "Failed to close gather object ");

		oid_oh = new_gather_obj->oid_oh;
		strcpy(obj_key_name, gather_obj_get_name(new_gather_obj));
		free(new_gather_obj);
	}

	rc = destroy_ensemble_list(ensembles_list);

	rc = release_operations_controller(op_controller);
	DSG_ERROR(rc, "Failed to release operations controller\n", end);

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open gather object", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 0);
	DSG_ERROR(rc, "Failed to fetch gather object entries", end);

	rc = gather_obj_fetch_num_of_gathers(gather_obj, NULL, &num_of_gathers,
					     -1, &curr_idx);
	DSG_ERROR(rc, "Failed to fetch gather object number of gathers ");

	values = malloc(num_of_gathers * sizeof(generic_value));
	rc = gather_obj_fetch_dkeys_list(gather_obj, NULL, 0, num_of_gathers,
					 values, -1, &curr_idx);

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
	DSG_ERROR(rc, "Failed to close gather object\n");

end: 	free(obj_key_name);

	return values;
}

headers_ranges_t*
daos_seis_range(root_obj_t *root_obj, int number_of_keys, char **keys, int dim)
{
	seismic_object_oid_oh_t oid_oh;
	ensemble_list 		*ensembles_list;
	gather_obj_t 		*gather_obj;
	char 			**gather_obj_dkeys;
	char 			*obj_key_name;
	int 			num_of_gathers;
	int 			curr_idx;
	int 			rc;
	int 			i;

	obj_key_name = malloc(MAX_KEY_LENGTH * sizeof(char));
	ensembles_list = init_ensemble_list();
	oid_oh = root_obj->gather_oids[0];
	strcpy(obj_key_name, root_obj->keys[0]);

	rc = gather_obj_open(&oid_oh, O_RDWR, NULL, -1, &curr_idx);
	DSG_ERROR(rc, "Failed to open gather object ", end);

	rc = gather_obj_fetch_entries(&gather_obj, oid_oh, obj_key_name, 1);
	DSG_ERROR(rc, "Failed to fetch gather object entries ", end);

	num_of_gathers = gather_obj_get_number_of_gathers(gather_obj);
	gather_obj_dkeys = malloc(num_of_gathers * sizeof(char*));

	gather_obj_prepare_dkeys(gather_obj, gather_obj_dkeys, 1,
				 num_of_gathers);

	for (i = 0; i < num_of_gathers; i++) {
		rc = gather_obj_fetch_gather_metadata_and_traces(gather_obj,
								 gather_obj_dkeys[i],
								 NULL,
								 ensembles_list,
								 -1, &curr_idx);
		DSG_ERROR(rc, "Failed to fetch gather metadata and traces ",
			  end);
	}

	headers_ranges_t 	*rng;

	rc = headers_ranges_init(&rng);

	rng->keys = keys;
	rng->number_of_keys = number_of_keys;
	rng->dim = dim;

	ensemble_range(ensembles_list, rng);
	return rng;

end:	return NULL;
}
