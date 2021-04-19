/*
 * set_traces_data_test.c
 *
 *  Created on: Feb 11, 2021
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

DAOS_FILE *daos_file;
uint64_t oid_hi = 1;
daos_obj_id_t oid = { .hi = 1, .lo = 1 }; /** object ID */
daos_size_t dfs_file_size;
size_t posix_file_size;
char *ret;

static int
setup(void **state)
{
	int rc;
	d_rank_list_t *svc = NULL;
	uuid_t pool_uuid;
	uuid_t co_uuid;
	init_dfs_api(pool_string,container_string, 1, 0);
	return 0;
}

static int
teardown(void **state)
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

static void
set_traces_data_test(void **state)
{
	int rc;
	int i;
	int j;
	ensemble_list 			*ensembles_list1, *ensembles_list2, *ensembles_list3;
	seismic_object_oid_oh_t 	oid_oh;
	gather_obj_t			*gather_obj;
	int 				num_of_gathers;
	char 				**gather_obj_dkeys;
	root_obj_t *root_obj;
	int curr_idx;
	char **keys = malloc(1 * sizeof(char*));
	for (i = 0; i < 1; i++) {
		keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	strcpy(keys[0], "fldr");
	rc = daos_seis_create_graph(NULL, "test", 1, keys, &root_obj, O_RDWR,
			S_IFDIR | S_IWUSR | S_IRUSR);

	assert_int_equal(rc, 0);
	root_obj = daos_seis_open_graph("/test", O_RDWR);
	rc = daos_seis_parse_segy(daos_file->file, root_obj);
	assert_int_equal(rc, 0);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc, 0);
	root_obj = daos_seis_open_graph("/test", O_RDWR);

	generic_value min;
	min.i = 601;

	ensembles_list1 = daos_seis_wind_headers_return_traces(root_obj, keys, 1, &min, &min);
	assert_int_equal(ensembles_list1->ensembles->size,1);

	min.i = 602;
	ensembles_list2 = daos_seis_wind_headers_return_traces(root_obj, keys, 1, &min, &min);
	assert_int_equal(ensembles_list2->ensembles->size,1);

	node_t* ensemble_node1 = ensembles_list1->ensembles->head;
	node_t* ensemble_node2 = ensembles_list2->ensembles->head;
	node_t* trace_node1;
	node_t* trace_node2;
	ensemble_t* temp_ensemble1;
	ensemble_t* temp_ensemble2;
	trace_t* temp_trace1;
	trace_t* temp_trace2;
	for(i = 0; i < ensembles_list1->ensembles->size; i++)
	{
		temp_ensemble1 = doubly_linked_list_get_object(ensemble_node1,offsetof(ensemble_t,n));
		temp_ensemble2 = doubly_linked_list_get_object(ensemble_node2,offsetof(ensemble_t,n));
		ensemble_node1 = ensemble_node1->next;
		ensemble_node2 = ensemble_node2->next;
		trace_node1 = temp_ensemble1->traces->head;
		trace_node2 = temp_ensemble2->traces->head;
		for(j = 0; j < temp_ensemble1->traces->size; j++)
		{
			temp_trace1 = doubly_linked_list_get_object(trace_node1,offsetof(trace_t,n));
			temp_trace2 = doubly_linked_list_get_object(trace_node2,offsetof(trace_t,n));

			temp_trace1->data = malloc(sizeof(float) * temp_trace2->ns);
			memcpy(temp_trace1->data, temp_trace2->data, sizeof(float) * temp_trace2->ns);

			trace_node1 = trace_node1->next;
			trace_node2 = trace_node2->next;

		}

	}

	rc = daos_seis_set_traces_data(root_obj,ensembles_list1);
	assert_int_equal(rc,0);

	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc, 0);
	root_obj = daos_seis_open_graph("/test", O_RDWR);
	min.i = 601;
	destroy_ensemble_list(ensembles_list1);

	ensembles_list1 = daos_seis_wind_headers_return_traces(root_obj, keys, 1, &min, &min);
	assert_int_equal(ensembles_list1->ensembles->size,1);

	ensemble_node1 = ensembles_list1->ensembles->head;
	ensemble_node2 = ensembles_list2->ensembles->head;
	for(i = 0; i < ensembles_list1->ensembles->size; i++)
	{
		temp_ensemble1 = doubly_linked_list_get_object(ensemble_node1,offsetof(ensemble_t,n));
		temp_ensemble2 = doubly_linked_list_get_object(ensemble_node2,offsetof(ensemble_t,n));
		ensemble_node1 = ensemble_node1->next;
		ensemble_node2 = ensemble_node2->next;
		trace_node1 = temp_ensemble1->traces->head;
		trace_node2 = temp_ensemble2->traces->head;
		for(j = 0; j < temp_ensemble1->traces->size; j++)
		{
			temp_trace1 = doubly_linked_list_get_object(trace_node1,offsetof(trace_t,n));
			temp_trace2 = doubly_linked_list_get_object(trace_node2,offsetof(trace_t,n));

			assert_memory_equal(temp_trace1->data, temp_trace2->data, temp_trace2->ns * sizeof(float));

			trace_node1 = trace_node1->next;
			trace_node2 = trace_node2->next;

		}

	}


}

int
main(int argc, char *argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if (container_string[21] == 'f') {
		container_string[21] = 'a';
	} else if (container_string[21] == '9') {
		container_string[21] = '1';
	} else {
		container_string[21]= container_string[21] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);

	const struct CMUnitTest tests[] =
			{ cmocka_unit_test(dfs_file_mount_test),
			  cmocka_unit_test(set_traces_data_test)};
	return cmocka_run_group_tests(tests, setup, teardown);
}
