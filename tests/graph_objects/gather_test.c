/*
 * gather_test.c
 *
 *  Created on: Feb 3, 2021
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

#include "seismic_sources/source_interface.h"
#include "seismic_sources/segy.h"
#include "graph_objects/gather.h"

static char pool_string[37];
static char container_string[37];
static char inputfile[4096];
static char out_file[4096];

DAOS_FILE *daos_file;
uint64_t oid_hi = 1;
daos_obj_id_t oid = { .hi = 1, .lo = 1 }; /** object ID */
daos_size_t dfs_file_size;
size_t posix_file_size;
char *ret;
char *read_array;

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

void
gather_update_fetch_test(void **state)
{
	int rc;
	int j = 1;
	int max = 0;
	int start = 0;
	int fldr_counter = 0;
	int current_fldr = -1;
	int curr_idx = 0;

	parse_functions_t *parse_functions;
	trace_t **traces = malloc(12010 * sizeof(trace_t*));
	int i = 0;

	rc = init_parsing_parameters(SEGY, &parse_functions,
			(void*) daos_file->file);
	assert_int_equal(0, rc);
	rc = parse_text_and_binary_headers(parse_functions);
	assert_int_equal(0, rc);
	while (1) {
		traces[i] = get_trace(parse_functions);
		if (traces[i] == NULL) {
			break;
		}
		if (current_fldr != traces[i]->fldr) {
			current_fldr = traces[i]->fldr;
			fldr_counter++;
		}
		i++;
	}

	gather_obj_t *gather_obj;
	char* name = malloc(sizeof(char)* 20);
	strcpy(name, "Test_gather");
	rc = gather_obj_create(&gather_obj, name, O_RDWR, LINKED_LIST);
	assert_int_equal(0, rc);
	seismic_object_oid_oh_t *oid_oh = gather_obj_get_oid_oh(gather_obj);
	int mode = gather_obj_get_mode(gather_obj);
	rc = gather_obj_open(oid_oh, mode, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = gather_obj_init_io_parameters(gather_obj, 0, "GATHER_DKEY",1);
	assert_int_equal(0, rc);
	rc = gather_obj_set_io_parameters(gather_obj, "GATHER_AKEY", DAOS_IOD_SINGLE,
					     sizeof(int), 1, NULL, NULL,
					     (char*)&fldr_counter, sizeof(int));
	assert_int_equal(rc, 0);

	rc = gather_obj_update(gather_obj, NULL, -1, &curr_idx);
	assert_int_equal(0, rc);

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 0);
	assert_int_equal(0, rc);

	rc = gather_obj_release_io_parameters(gather_obj);
	assert_int_equal(0, rc);

	rc = gather_obj_open(oid_oh, mode, NULL, -1, &curr_idx);
	assert_int_equal(0, rc);
	int *counter_fetched;
	counter_fetched = malloc(sizeof(int));
	rc = gather_obj_init_io_parameters(gather_obj, 0, "GATHER_DKEY",1);
	assert_int_equal(0, rc);
	rc = gather_obj_set_io_parameters(gather_obj, "GATHER_AKEY", DAOS_IOD_SINGLE,
					     sizeof(int), 1, NULL, NULL,
					     (char*)counter_fetched, sizeof(int));
	assert_int_equal(rc, 0);


	rc = gather_obj_fetch(gather_obj, NULL, -1, &curr_idx);
	assert_int_equal(0, rc);

	rc = gather_obj_close(gather_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(0, rc);

	assert_int_equal(*counter_fetched, fldr_counter);

	for (i = 0; i < 12010; i++) {
		free(traces[i]);
	}
	free(traces);
	rc = release_parsing_parameters(parse_functions);
	assert_int_equal(0, rc);

}

int
main(int argc, char *argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if (container_string[2] == 'f') {
		container_string[2] = '6';
	} else if (container_string[2] == '9') {
		container_string[2] = '0';
	} else {
		container_string[2] = container_string[2] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(dfs_file_mount_test),
		cmocka_unit_test(gather_update_fetch_test) };
	return cmocka_run_group_tests(tests, setup, teardown);
}
