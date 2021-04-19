/*
 * segy_test.c
 *
 *  Created on: Jan 31, 2021
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
static daos_handle_t poh;
static daos_handle_t coh;
static daos_handle_t oh;
static dfs_t	*dfs;

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
segy_parsing_test(void **state)
{
	int	rc;
	parse_functions_t *parse_functions;
	rc = init_parsing_parameters(SEGY, &parse_functions,(void *)daos_file->file);
	assert_int_equal(0, rc);
	rc = parse_text_and_binary_headers(parse_functions);
	assert_int_equal(0, rc);
	char *th;
	th =  get_text_header(parse_functions);
	int size = strlen(th);
	assert_int_equal(size, 3200);

	trace_t ** traces = malloc(12010 * sizeof(trace_t*));
	int i=0;
	while(1) {
		traces[i] = get_trace(parse_functions);
		if(traces[i] == NULL){
			break;
		}
		i++;
	}

	int j =1;
	int max =0;
	int start = 0;
	while(j <= 10) {
		start = max;
		max = (j) * 1201;

		for(i=start; i < max; i++)
		{
			assert_int_equal(traces[i]->fldr, (600 +j));
		}
		j++;
	}

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
	if(container_string[7] == 'f') {
		container_string[7] = '1';
	} else if(container_string[7] == '9') {
		container_string[7] = 'b';
	} else {
		container_string[7] = container_string[7] + 2;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(segy_parsing_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
