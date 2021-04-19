/*
 * traces_array_test.c
 *
 *  Created on: Feb 2, 2021
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
#include "seismic_sources/source_interface.h"
#include "seismic_sources/segy.h"
#include "graph_objects/traces_array.h"

static char pool_string[37];
static char container_string[37];
static char inputfile[4096];
static char out_file[4096];

DAOS_FILE * daos_file;
uint64_t	oid_hi = 1;
daos_obj_id_t	oid = { .hi = 1, .lo = 1 }; /** object ID */
daos_size_t dfs_file_size;
size_t	posix_file_size;
char *ret;
char *read_array;


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
	int	rc;
        rc = dfs_file_exists(out_file);
        assert_int_equal(rc, 0);
        posix_file_size = read_posix(inputfile, &ret);
        assert_int_equal(99014040, posix_file_size);
        daos_file = open_dfs_file(out_file, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
        write_dfs_file(daos_file, ret, posix_file_size);
        dfs_file_size = get_dfs_file_size(daos_file);
        assert_int_equal(dfs_file_size, posix_file_size);
}

void
traces_array_update_fetch_test(void **state)
{
	int	rc;
	int 	j =1;
	int 	max =0;
	int 	start = 0;
	int 	curr_idx =0;

	parse_functions_t *parse_functions;
	trace_t ** traces = malloc(12010 * sizeof(trace_t*));
	int i=0;

	rc = init_parsing_parameters(SEGY, &parse_functions,(void *)daos_file->file);
	assert_int_equal(0, rc);
	rc = parse_text_and_binary_headers(parse_functions);
	assert_int_equal(0, rc);
	daos_obj_id_t *oids = malloc(12010 * sizeof(daos_obj_id_t));

	while(1) {
		traces[i] = get_trace(parse_functions);
		if(traces[i] == NULL){
			break;
		}
		oid_gen(DAOS_OBJ_CLASS_ID, false,
			&oids[i], false);
		assert_int_equal(0, rc);
		i++;
	}

	traces_array_obj_t *traces_array_obj;
	rc = traces_array_obj_create(&traces_array_obj, O_RDWR, oids, 12010);
	assert_int_equal(0, rc);

	seismic_object_oid_oh_t *oid_oh = traces_array_obj_get_id_oh(traces_array_obj);
	int mode = get_traces_array_obj_mode(traces_array_obj);

	rc = traces_array_obj_open(oid_oh, mode, NULL, -1, &curr_idx,
				   sizeof(daos_obj_id_t),
				   500 * sizeof(daos_obj_id_t));
	assert_int_equal(rc, 0);

	uint64_t *rg_idx = malloc(sizeof(uint64_t));
	uint64_t *rg_len = malloc(sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = traces_array_obj->num_of_traces;

	rc = traces_array_obj_init_io_parameters(traces_array_obj);
	assert_int_equal(rc, 0);
	rc =traces_array_obj_set_io_parameters(traces_array_obj,1, rg_idx,
						   rg_len, 0, (char*)traces_array_obj->oids,
						   traces_array_obj->num_of_traces *sizeof(daos_obj_id_t));
	assert_int_equal(0, rc);

	rc = traces_array_obj_update(traces_array_obj, NULL, -1 , &curr_idx);
	assert_int_equal(0, rc);

	rc = traces_array_obj_close(traces_array_obj, NULL, -1, &curr_idx, 0);
	assert_int_equal(0, rc);

	rc = traces_array_obj_release_io_parameters(traces_array_obj);
	assert_int_equal(0, rc);

	rc = traces_array_obj_open(oid_oh, mode, NULL, -1, &curr_idx,
				   sizeof(daos_obj_id_t), 500 * sizeof(daos_obj_id_t));
	assert_int_equal(0, rc);

	uint64_t	size;
	rc = get_traces_array_obj_size(traces_array_obj, NULL, -1, &curr_idx, &size);
	assert_int_equal(0, rc);
	assert_int_equal(size, traces_array_obj->num_of_traces);

	daos_obj_id_t *oids_fetched;
	oids_fetched = malloc(12010 *sizeof(daos_obj_id_t));

	rc = traces_array_obj_init_io_parameters(traces_array_obj);
	assert_int_equal(rc, 0);
	rc =traces_array_obj_set_io_parameters(traces_array_obj,1, rg_idx,
						 rg_len, 0, (char*)oids_fetched,
						 traces_array_obj->num_of_traces *sizeof(daos_obj_id_t));
	assert_int_equal(0, rc);

	rc = traces_array_obj_fetch(traces_array_obj, NULL, -1 , &curr_idx);
	assert_int_equal(0, rc);

	assert_true((memcmp((void*)&oids_fetched[1784],
		     (void*) &traces_array_obj->oids[1784],
		     sizeof(daos_obj_id_t)) == 0));

	rc = traces_array_obj_close(traces_array_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(0, rc);

	for(i=0; i < 12010; i++) {
		free(traces[i]);
	}
	free(traces);
	free(rg_idx);
	free(rg_len);
	rc = release_parsing_parameters(parse_functions);
	assert_int_equal(0, rc);
}


int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[6] == 'f') {
		container_string[6] = '2';
	} else if(container_string[6] == '9') {
		container_string[6] = 'c';
	} else {
		container_string[6] = container_string[6] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(traces_array_update_fetch_test),
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
