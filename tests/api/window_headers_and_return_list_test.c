/*
 * window_headers_and_return_list_test.c
 *
 *  Created on: Feb 8, 2021
 *      Author: mirnamoawad
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
	int rc;
	d_rank_list_t *svc = NULL;
	uuid_t pool_uuid;
	uuid_t co_uuid;
	init_dfs_api(pool_string,container_string, 1,0);
	return 0;
}

static int teardown(void **state)
{
	int rc;
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
	gather_obj_t *seismic_obj;// = malloc(sizeof(gather_obj_t));
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
	rc = trace_hdr_obj_close(tr_hdr, NULL, -1, &curr_idx, 1);
	assert_int_equal(rc, 0);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc, 0);
	rc = gather_obj_close(seismic_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(rc, 0);

	free(tr);
	free(keys[0]);
	free(keys);


}

static void window_headers_return_list_test(void **state)
{
	root_obj_t	*root_obj;
	int		num_of_keys = 2;
	int		rc;
	int		i;
	char		**window_keys = malloc(num_of_keys * sizeof(char *));
	for(i=0; i < num_of_keys; i++) {
		window_keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	strcpy(window_keys[0], "fldr");
	strcpy(window_keys[1], "tracl");

	generic_value 	*min_keys = malloc(num_of_keys * sizeof(generic_value));
	generic_value 	*max_keys = malloc(num_of_keys * sizeof(generic_value));

	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	min_keys[0].i = 602;
	max_keys[0].i = 607;
	min_keys[1].i = 1201;
	max_keys[1].i = 3603;
	ensemble_list	*ensembles_list;
	ensembles_list = daos_seis_wind_headers_return_traces(root_obj, window_keys,
							      num_of_keys, min_keys,
							      max_keys);

	ensemble_t* curr_ensemble =
			doubly_linked_list_get_object(ensembles_list->ensembles->head,
						      offsetof(ensemble_t, n));
	trace_t* 	curr_trace ;
	node_t*		trace_node;
	node_t*		ensemble_node;
	int		j;
	assert_int_equal(ensembles_list->ensembles->size, 2);
	ensemble_node = ensembles_list->ensembles->head;
	int tracl = 1202;
	for( i=0; i < ensembles_list->ensembles->size; i++) {
		curr_trace = doubly_linked_list_get_object(curr_ensemble->traces->head,
							   offsetof(trace_t, n));
		j = 0;
		trace_node = curr_ensemble->traces->head;

		while(j < curr_ensemble->traces->size){
			assert_int_equal(curr_trace->tracl, tracl++);
			trace_node = trace_node->next;
			curr_trace = doubly_linked_list_get_object(trace_node,
								   offsetof(trace_t, n));
			j++;
		}
		ensemble_node = ensemble_node->next;
		curr_ensemble = doubly_linked_list_get_object(ensemble_node,
							      offsetof(ensemble_t, n));
	}

	rc = destroy_ensemble_list(ensembles_list);
	assert_int_equal(rc, 0);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc,0);

	free(min_keys);
	free(max_keys);

}

static void window_headers_return_list_one_key_test(void **state)
{
	root_obj_t	*root_obj;
	int		num_of_keys = 1;
	int		rc;
	int		i;
	char		**window_keys = malloc(num_of_keys * sizeof(char *));

	window_keys[0] = malloc(MAX_KEY_LENGTH * sizeof(char));

	strcpy(window_keys[0], "tracl");

	generic_value 	*min_keys = malloc(num_of_keys * sizeof(generic_value));
	generic_value 	*max_keys = malloc(num_of_keys * sizeof(generic_value));

	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	min_keys[0].i = 1201;
	max_keys[0].i = 3603;
	ensemble_list	*ensembles_list;
	ensembles_list = daos_seis_wind_headers_return_traces(root_obj, window_keys,
							      num_of_keys, min_keys,
							      max_keys);
	ensemble_t* curr_ensemble =
			doubly_linked_list_get_object(ensembles_list->ensembles->head,
						      offsetof(ensemble_t, n));
	trace_t* 	curr_trace ;
	node_t*		trace_node;
	node_t*		ensemble_node;
	int		j;
	assert_int_equal(ensembles_list->ensembles->size, 3);
	ensemble_node = ensembles_list->ensembles->head;
	int tracl = 1201;
	for( i=0; i < ensembles_list->ensembles->size; i++) {
		curr_trace = doubly_linked_list_get_object(curr_ensemble->traces->head,
							   offsetof(trace_t, n));
		j = 0;
		trace_node = curr_ensemble->traces->head;

		while(j < curr_ensemble->traces->size){
			assert_int_equal(curr_trace->tracl, tracl++);
			trace_node = trace_node->next;
//			curr_trace->data = NULL;
			curr_trace = doubly_linked_list_get_object(trace_node,
								   offsetof(trace_t, n));
			j++;
		}
		ensemble_node = ensemble_node->next;
		curr_ensemble = doubly_linked_list_get_object(ensemble_node,
							      offsetof(ensemble_t, n));
	}

	rc = destroy_ensemble_list(ensembles_list);
	assert_int_equal(rc, 0);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc,0);

	free(min_keys);
	free(max_keys);
}

static void get_shot_traces_test(void **state)
{
	root_obj_t	*root_obj;
	int		rc;
	int		i;

	root_obj  = daos_seis_open_graph("/test", O_RDWR);

	ensemble_list	*ensembles_list;
	ensembles_list = daos_seis_get_shot_traces(root_obj, 605);
	ensemble_t* curr_ensemble =
			doubly_linked_list_get_object(ensembles_list->ensembles->head,
						      offsetof(ensemble_t, n));
	trace_t* 	curr_trace ;
	node_t*		trace_node;
	node_t*		ensemble_node;
	int		j;
	assert_int_equal(ensembles_list->ensembles->size, 1);
	ensemble_node = ensembles_list->ensembles->head;
	int tracl = 4805;
	for( i=0; i < ensembles_list->ensembles->size; i++) {
		curr_trace = doubly_linked_list_get_object(curr_ensemble->traces->head,
							   offsetof(trace_t, n));
		j = 0;
		trace_node = curr_ensemble->traces->head;

		assert_int_equal(curr_ensemble->traces->size, 1201);
		while(j < curr_ensemble->traces->size){
			assert_int_equal(curr_trace->tracl, tracl++);
			trace_node = trace_node->next;
			curr_trace = doubly_linked_list_get_object(trace_node,
								   offsetof(trace_t, n));
			j++;
		}
		ensemble_node = ensemble_node->next;
		curr_ensemble = doubly_linked_list_get_object(ensemble_node,
							      offsetof(ensemble_t, n));
	}

	rc = destroy_ensemble_list(ensembles_list);
	assert_int_equal(rc, 0);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc,0);
}

int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[15] == 'f') {
		container_string[15] = 'a';
	} else if(container_string[15] == '9') {
		container_string[15] = '1';
	} else {
		container_string[15] = container_string[15] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(parse_segy_test),
    cmocka_unit_test(window_headers_return_list_test),
    cmocka_unit_test(window_headers_return_list_one_key_test),
    cmocka_unit_test(get_shot_traces_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
