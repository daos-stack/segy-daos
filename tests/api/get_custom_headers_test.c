/*
 * get_custom_headers_test.c
 *
 *  Created on: Feb 8, 2021
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

static void get_custom_headers_test(void **state)
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
	printf("Parsing completed\n");
	int num_of_keys = 2;
	char** header_keys = malloc(num_of_keys * sizeof(char*));
	header_keys[0] = malloc(sizeof(char) * 10);
	header_keys[1] = malloc(sizeof(char) * 10);
	strcpy(header_keys[0],"fldr");
	strcpy(header_keys[1],"cdp");
	key_value_pair_t* kv;
	kv = daos_seis_get_custom_headers(root_obj,header_keys,num_of_keys);
	assert_non_null(kv);
	//key_value_print(kv, num_of_keys);
	free(header_keys[0]);
	free(header_keys[1]);
	free(header_keys);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc,0);
	free(keys[0]);
	free(keys);
	for(i = 0; i < num_of_keys; i++){
		free(kv[i].key);
		free(kv[i].values);
	}
	free(kv);





}

int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[20] == 'f') {
		container_string[20] = 'a';
	} else if(container_string[20] == '9') {
		container_string[20] = '1';
	} else {
		container_string[20] = container_string[20] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(get_custom_headers_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}

