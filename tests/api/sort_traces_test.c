/*
 * sort_traces_test.c
 *
 *  Created on: Feb 14, 2021
 *      Author: omar
 */


#include <stdarg.h>
#include <stddef.h>

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include "daos_primitives/dfs_helpers.h"
#include "daos.h"
#include "daos_fs.h"
#include "graph_objects/root.h"
#include "api/seismic_graph_api.h"

static char pool_string[37];
static char container_string[37];
static char inputfile[4096];
static char out_file[4096];
//static char out_path[37];

DAOS_FILE * daos_file;
uint64_t	oid_hi = 1;
daos_obj_id_t	oid = { .hi = 1, .lo = 1 }; /** object ID */
daos_size_t dfs_file_size;
size_t posix_file_size;
char *ret;

static int setup(void **state)
{
	d_rank_list_t *svc = NULL;
	uuid_t pool_uuid;
	uuid_t co_uuid;
	init_dfs_api(pool_string,container_string, 1,0);
	return 0;
}

static int teardown(void **state)
{
	fini_dfs_api();

	return 0;
}

void
dfs_file_mount_test(void **state)
{
	int rc;
	rc = dfs_file_exists(out_file);
	assert_int_equal(rc, 0);
	posix_file_size = read_posix(inputfile, &ret);
	assert_int_equal(99014040, posix_file_size);
	daos_file = open_dfs_file(out_file, S_IFREG | S_IWUSR | S_IRUSR, 'w',
			1);
	write_dfs_file(daos_file, ret, posix_file_size);
	dfs_file_size = get_dfs_file_size(daos_file);
	assert_int_equal(dfs_file_size, posix_file_size);
}

static void parse_segy_test(void **state)
{
	int	rc;
	int i;
	root_obj_t	*root_obj;
	int		curr_idx;
	char **keys = malloc(1 * sizeof(char*));
	for(i=0; i < 1; i++) {
		keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	strcpy(keys[0], "fldr");
	rc = daos_seis_create_graph(NULL, "test",
				    1, keys, &root_obj, O_RDWR,
				    S_IFDIR | S_IWUSR | S_IRUSR);

	assert_int_equal(rc,0);
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	rc = daos_seis_parse_segy(daos_file->file, root_obj);
	assert_int_equal(rc,0);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc,0);
	root_obj = daos_seis_open_graph("/test", O_RDWR);
	int number;
	root_obj_fetch_num_of_keys(root_obj,-1,&curr_idx,&number);
	assert_int_equal(number,1);
	gather_obj_open(&(root_obj->gather_oids[0]),O_RDWR,NULL,-1,&curr_idx);
	gather_obj_t *seismic_obj = malloc(sizeof(gather_obj_t));
	gather_obj_fetch_entries(&seismic_obj,root_obj->gather_oids[0],root_obj->keys[0],1);
	assert_int_equal(seismic_obj->number_of_gathers,10);
	assert_int_equal(seismic_obj->unique_values[0].i,601);
	assert_int_equal(seismic_obj->unique_values[9].i,610);
	char temp[200] = "";
	char gather_dkey_name[200] = "";
	strcat(gather_dkey_name, seismic_obj->name);
	strcat(gather_dkey_name, KEY_SEPARATOR);
	val_sprintf(temp, seismic_obj->unique_values[4], hdtype(seismic_obj->name));
	strcat(gather_dkey_name, temp);
	gather_obj_fetch_gather_num_of_traces(seismic_obj,NULL,gather_dkey_name,&number,-1,&curr_idx);
	assert_int_equal(number,1201);
	daos_obj_id_t daos_oid;
	gather_obj_fetch_gather_traces_array_oid(seismic_obj,NULL,gather_dkey_name,&daos_oid,-1,&curr_idx);
	generic_value val;
	gather_obj_fetch_gather_unique_value(seismic_obj,NULL,gather_dkey_name,&val,-1,&curr_idx);
	assert_int_equal(val.i,605);
	seismic_object_oid_oh_t oid_oh;
	oid_oh.oid = daos_oid;
	traces_array_obj_open(&oid_oh,O_RDWR,NULL,-1,&curr_idx,sizeof(daos_obj_id_t),
			   500 * sizeof(daos_obj_id_t));
	traces_array_obj_t* tr_array;
	tr_array = malloc(sizeof(traces_array_obj_t));
	tr_array->io_parameters = NULL;
	tr_array->daos_mode = O_RDWR;
	tr_array->num_of_traces = number;
	tr_array->oid_oh = oid_oh;
	tr_array->oids = malloc(number * sizeof(daos_obj_id_t));
	traces_array_obj_fetch_oids(tr_array,NULL,-1,&curr_idx,tr_array->oids,tr_array->num_of_traces,0);

	seismic_object_oid_oh_t hdr_oid_oh;
	hdr_oid_oh.oid = tr_array->oids[0];
	rc = traces_array_obj_close(tr_array, NULL, -1, &curr_idx, 1);
	trace_hdr_obj_open(&hdr_oid_oh,O_RDWR,NULL,-1,&curr_idx);
	trace_hdr_obj_t* tr_hdr = malloc(sizeof(trace_hdr_obj_t));
	tr_hdr->daos_mode = O_RDWR;
	tr_hdr->io_parameters = NULL;
	tr_hdr->oid_oh = hdr_oid_oh;
	tr_hdr->trace = NULL;
	trace_t* tr = malloc(sizeof(trace_t));
	trace_hdr_fetch_headers(tr_hdr,tr,NULL,-1,&curr_idx);
	assert_int_equal(tr->fldr, 605);
	rc = gather_obj_close(seismic_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(rc, 0);
	rc = trace_hdr_obj_close(tr_hdr, NULL, -1, &curr_idx, 1);
	assert_int_equal(rc, 0);

	free(tr);
	free(keys[0]);
	free(keys);


}

static void
sort_headers_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 2;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "fldr");
	strcpy(sort_keys[1], "cdp");
	int directions[2] = {1, -1};
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys, NULL,0,NULL, NULL);
	assert_non_null(e);
	assert_int_equal(e->ensembles->size, 12010);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	while(ensemble_node != NULL){
		int prev_cdp;
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = temp_ensemble->traces->head;

		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_cdp = temp_trace->cdp;
		trace_node = trace_node->next;

		while(trace_node != NULL){
			temp_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,n));
//			temp_trace->data = NULL;

			assert_true(temp_trace->cdp <= prev_cdp);
			prev_cdp = temp_trace->cdp;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	destroy_ensemble_list(e);

	free(sort_keys[0]);
	free(sort_keys[1]);

	free(sort_keys);
}

static void
sort_headers_with_window_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 2;
	int 		num_of_window_keys = 2;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	char		**window_keys = malloc(num_of_window_keys * sizeof(char *));

	for(i = 0; i < num_of_window_keys; i++){
		window_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "fldr");
	strcpy(sort_keys[1], "cdp");
	strcpy(window_keys[0], "fldr");
	strcpy(window_keys[1], "tracl");
	int directions[2] = {1, -1};
	generic_value min[2];
	generic_value max[2];
	min[0].i = 603;
	max[0].i = 610;
	min[1].i = 1;
	max[1].i = 12000;
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys,window_keys,num_of_window_keys,min,max);
	assert_non_null(e);
	assert_int_equal(e->ensembles->size, 9598);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	while(ensemble_node != NULL){
		int prev_cdp;
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = temp_ensemble->traces->head;

		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_cdp = temp_trace->cdp;
		trace_node = trace_node->next;
		while(trace_node != NULL){
			temp_trace = doubly_linked_list_get_object(trace_node, offsetof(trace_t,n));
			assert_true(temp_trace->cdp <= prev_cdp);
			assert_true(temp_trace->fldr >= 603 || temp_trace->fldr <= 610);
			assert_true(temp_trace->tracl >= 1 || temp_trace->tracl <= 12000);

			prev_cdp = temp_trace->cdp;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	destroy_ensemble_list(e);
	free(window_keys[0]);
	free(window_keys[1]);
	free(sort_keys[0]);
	free(sort_keys[1]);
	free(window_keys);
	free(sort_keys);

}

static void
sort_headers_non_existing_key_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 2;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "cdp");
	strcpy(sort_keys[1], "fldr");
	int directions[2] = {-1, -1};
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys, NULL,0,NULL, NULL);
	assert_non_null(e);
	assert_int_equal(e->ensembles->size, 12010);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	int prev_fldr;
	int prev_cdp;
	while(ensemble_node != NULL){
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = temp_ensemble->traces->head;
		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_fldr = temp_trace->fldr;
		prev_cdp = temp_trace->cdp;
		trace_node = trace_node->next;

		while(trace_node != NULL){
			temp_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,n));
//			temp_trace->data = NULL;

			assert_true(temp_trace->cdp <= prev_cdp);
			assert_true(temp_trace->fldr <= prev_fldr);
			prev_cdp = temp_trace->cdp;
			prev_fldr = temp_trace->fldr;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	destroy_ensemble_list(e);

	free(sort_keys[0]);
	free(sort_keys[1]);

	free(sort_keys);
}

static void
sort_headers_non_existing_key_with_window_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 2;
	int 		num_of_window_keys = 2;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	char		**window_keys = malloc(num_of_window_keys * sizeof(char *));

	for(i = 0; i < num_of_window_keys; i++){
		window_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "cdp");
	strcpy(sort_keys[1], "fldr");
	strcpy(window_keys[0], "fldr");
	strcpy(window_keys[1], "tracl");
	int directions[2] = {-1, -1};
	generic_value min[2];
	generic_value max[2];
	min[0].i = 603;
	max[0].i = 610;
	min[1].i = 1;
	max[1].i = 12000;
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys,
				  window_keys,num_of_window_keys,min,max);
	assert_non_null(e);

	assert_int_equal(e->ensembles->size, 9598);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	int prev_fldr;
	int prev_cdp;
	i=0;

	while(ensemble_node != NULL){
		i++;
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = temp_ensemble->traces->head;
		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_fldr = temp_trace->fldr;
		prev_cdp = temp_trace->cdp;
		trace_node = trace_node->next;

		while(trace_node != NULL){
			i++;

			temp_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,n));
			assert_true(temp_trace->cdp <= prev_cdp);
			assert_true(temp_trace->fldr <= prev_fldr);
			assert_true(temp_trace->fldr >= 603 || temp_trace->fldr <= 610);
			assert_true(temp_trace->tracl >= 1 || temp_trace->tracl <= 12000);
			prev_fldr = temp_trace->fldr;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	assert_int_equal(i, 9598);
	destroy_ensemble_list(e);
	free(window_keys[0]);
	free(window_keys[1]);
	free(sort_keys[0]);
	free(sort_keys[1]);
	free(window_keys);
	free(sort_keys);

}

static void
sort_headers_non_existing_key_with_window_fldr_gx_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 2;
	int 		num_of_window_keys = 2;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	char		**window_keys = malloc(num_of_window_keys * sizeof(char *));

	for(i = 0; i < num_of_window_keys; i++){
		window_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "cdp");
	strcpy(sort_keys[1], "fldr");
	strcpy(window_keys[0], "fldr");
	strcpy(window_keys[1], "gx");
	int directions[2] = {-1, -1};
	generic_value min[2];
	generic_value max[2];
	min[0].i = 603;
	max[0].i = 610;
	min[1].i = 200500;
	max[1].i = 300000;
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys,
				  window_keys,num_of_window_keys,min,max);
	assert_non_null(e);
	assert_int_equal(e->ensembles->size, 6376);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	int prev_fldr;
	int prev_cdp;
	i=0;

	while(ensemble_node != NULL){
		i++;
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = temp_ensemble->traces->head;
		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_fldr = temp_trace->fldr;
		prev_cdp = temp_trace->cdp;
		trace_node = trace_node->next;

		while(trace_node != NULL){
			i++;

			temp_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,n));
			assert_true(temp_trace->cdp <= prev_cdp);
			assert_true(temp_trace->fldr <= prev_fldr);
			assert_true(temp_trace->fldr >= 603 || temp_trace->fldr <= 610);
			assert_true(temp_trace->gx >= 200500 || temp_trace->gx <= 300000);
			prev_fldr = temp_trace->fldr;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	assert_int_equal(i, 6376);
	destroy_ensemble_list(e);
	free(window_keys[0]);
	free(window_keys[1]);
	free(sort_keys[0]);
	free(sort_keys[1]);
	free(window_keys);
	free(sort_keys);

}

static void
sort_complex_headers_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 3;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "fldr");
	strcpy(sort_keys[1], "cdp");
	strcpy(sort_keys[2], "sx");
	int directions[3] = {1, -1, -1};
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys, NULL,0,NULL, NULL);
	assert_non_null(e);
	assert_int_equal(e->ensembles->size, 12010);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	while(ensemble_node != NULL){
		int prev_cdp;
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		trace_node = temp_ensemble->traces->head;

		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_cdp = temp_trace->cdp;
		trace_node = trace_node->next;

		while(trace_node != NULL){
			temp_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t,n));
//			temp_trace->data = NULL;

			assert_true(temp_trace->cdp <= prev_cdp);
			prev_cdp = temp_trace->cdp;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	destroy_ensemble_list(e);

	free(sort_keys[0]);
	free(sort_keys[1]);
	free(sort_keys[2]);

	free(sort_keys);
}

static void
sort_complex_headers_with_window_test(void** state){
	root_obj_t	*root_obj;
	int		num_of_keys = 3;
	int 		num_of_window_keys = 2;
	int		rc;
	int		i;
	int		curr_idx;
	char		**sort_keys = malloc(num_of_keys * sizeof(char *));

	for(i = 0; i < num_of_keys; i++){
		sort_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	char		**window_keys = malloc(num_of_window_keys * sizeof(char *));

	for(i = 0; i < num_of_window_keys; i++){
		window_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}

	strcpy(sort_keys[0], "fldr");
	strcpy(sort_keys[1], "cdp");
	strcpy(sort_keys[2], "sx");
	strcpy(window_keys[0], "fldr");
	strcpy(window_keys[1], "tracl");
	int directions[3] = {1, -1, -1};
	generic_value min[2];
	generic_value max[2];
	min[0].i = 603;
	max[0].i = 610;
	min[1].i = 1;
	max[1].i = 12000;
	ensemble_list* e;
	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	e = daos_seis_sort_traces(root_obj, sort_keys,directions,num_of_keys,window_keys,num_of_window_keys,min,max);
	assert_non_null(e);
	assert_int_equal(e->ensembles->size, 1);
	node_t* ensemble_node = e->ensembles->head;
	node_t* trace_node;
	ensemble_t* temp_ensemble;
	trace_t* temp_trace;
	while(ensemble_node != NULL){
		int prev_sx;
		int prev_cdp;
		int prev_fldr;
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t,n));
		assert_int_equal(temp_ensemble->traces->size,9598);
		trace_node = temp_ensemble->traces->head;

		temp_trace =
			doubly_linked_list_get_object(trace_node,
						      offsetof(trace_t,n));

		prev_sx = temp_trace->sx;
		prev_cdp = temp_trace->cdp;
		prev_fldr = temp_trace->fldr;
		trace_node = trace_node->next;
		while(trace_node != NULL){
			temp_trace = doubly_linked_list_get_object(trace_node, offsetof(trace_t,n));
			if(temp_trace->fldr > prev_fldr) {
				prev_sx = temp_trace->sx;
				prev_cdp = temp_trace->cdp;
				assert_true(temp_trace->sx <= prev_sx);
				assert_true(temp_trace->cdp <= prev_cdp);
			} else {
				assert_true(temp_trace->sx <= prev_sx);
				assert_true(temp_trace->cdp <= prev_cdp);
			}

			assert_true(temp_trace->fldr >= 603 || temp_trace->fldr <= 610);
			assert_true(temp_trace->tracl >= 1 || temp_trace->tracl <= 12000);
			prev_cdp = temp_trace->cdp;
			prev_sx = temp_trace->sx;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;
	}
	destroy_ensemble_list(e);
	free(window_keys[0]);
	free(window_keys[1]);
	free(sort_keys[0]);
	free(sort_keys[1]);
	free(sort_keys[2]);
	free(window_keys);
	free(sort_keys);

}

int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[24] == 'f') {
		container_string[24] = 'a';
	} else if(container_string[24] == '9') {
		container_string[24] = '1';
	} else {
		container_string[24] = container_string[24] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(parse_segy_test),
    cmocka_unit_test(sort_headers_test),
    cmocka_unit_test(sort_headers_with_window_test),
    cmocka_unit_test(sort_headers_non_existing_key_test),
    cmocka_unit_test(sort_headers_non_existing_key_with_window_test),
    cmocka_unit_test(sort_headers_non_existing_key_with_window_fldr_gx_test),
    cmocka_unit_test(sort_complex_headers_test),
    cmocka_unit_test(sort_complex_headers_with_window_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
