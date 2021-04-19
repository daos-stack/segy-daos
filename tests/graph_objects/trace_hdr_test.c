/*
 * trace_hdr_test.c
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
#include "graph_objects/trace_hdr.h"

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
	init_dfs_api(pool_string, container_string, 1,0);
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
trace_hdr_update_fetch(void **state)
{
	int	rc;
	int 	j =1;
	int 	max =0;
	int 	start = 0;
	int	fldr_written = 0;
	int	fldr_fetched = 0;
	int 	curr_idx =0;

	parse_functions_t *parse_functions;
	trace_t ** traces = malloc(12010 * sizeof(trace_t*));
	int i=0;

	rc = init_parsing_parameters(SEGY, &parse_functions,(void *)daos_file->file);
	assert_int_equal(0, rc);
	rc = parse_text_and_binary_headers(parse_functions);
	assert_int_equal(0, rc);
	while(1) {
		traces[i] = get_trace(parse_functions);
		if(traces[i] == NULL){
			break;
		}
		i++;
	}

	trace_hdr_obj_t *trace_hdr_obj;
	rc = trace_hdr_obj_create(&trace_hdr_obj, O_RDWR, NULL);
	assert_int_equal(0, rc);
	seismic_object_oid_oh_t *oid_oh = trace_hdr_obj_get_id_oh(trace_hdr_obj);
	int mode = trace_hdr_obj_get_mode(trace_hdr_obj);
	rc = trace_hdr_obj_open(oid_oh, mode, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	unsigned int iod_nr = 1;
	fldr_written = traces[1]->fldr;
	uint64_t *rx_idx = malloc(iod_nr *sizeof(uint64_t));
	uint64_t *rx_nr = malloc(iod_nr *sizeof(uint64_t));
	for(j=0; j < iod_nr; j++) {
		rx_idx[j] = 0;
		rx_nr[j] = TRACEHDR_BYTES;
	}

	rc = trace_hdr_obj_init_io_parameters(trace_hdr_obj, 0, "TRACE_DKEY",1);
	assert_int_equal(0, rc);
	rc = trace_hdr_object_set_io_parameters(trace_hdr_obj, "TRACE_AKEY", DAOS_IOD_ARRAY,
					     	 1, 1, rx_idx, rx_nr,
						 (char*)traces[1], sizeof(trace_t));
	assert_int_equal(0, rc);

	rc = trace_hdr_obj_update(trace_hdr_obj, NULL, -1 , &curr_idx);
	assert_int_equal(0, rc);

	rc = trace_hdr_obj_close(trace_hdr_obj, NULL, -1, &curr_idx, 0);
	assert_int_equal(0, rc);

	rc = trace_hdr_obj_release_io_parameters(trace_hdr_obj);
	assert_int_equal(0, rc);

	rc = trace_hdr_obj_open(oid_oh, mode, NULL, -1, &curr_idx);
	assert_int_equal(0, rc);
	trace_t *trace_fetched;
	trace_fetched = malloc(sizeof(trace_t));
	rc = trace_hdr_obj_init_io_parameters(trace_hdr_obj, 0, "TRACE_DKEY",1);
	assert_int_equal(0, rc);
	rc = trace_hdr_object_set_io_parameters(trace_hdr_obj, "TRACE_AKEY",
						DAOS_IOD_ARRAY,  1, 1, rx_idx, rx_nr,
						 (char*)trace_fetched, sizeof(trace_t));
	assert_int_equal(0, rc);

	rc = trace_hdr_obj_fetch(trace_hdr_obj, NULL, -1 , &curr_idx);
	assert_int_equal(0, rc);

	rc = trace_hdr_obj_close(trace_hdr_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(0, rc);

	fldr_fetched = trace_fetched->fldr;
	assert_int_equal(fldr_fetched, fldr_written);

	for(i=0; i < 12010; i++) {
		free(traces[i]);
	}
	free(traces);
	rc = release_parsing_parameters(parse_functions);
	assert_int_equal(0, rc);
}


int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[5] == 'f') {
		container_string[5] = 'b';
	} else if(container_string[5] == '9') {
		container_string[5] = 'e';
	} else {
		container_string[5] = container_string[5] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(trace_hdr_update_fetch),
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
