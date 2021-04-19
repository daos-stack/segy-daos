/*
 * fetch_binary_and_text_headers_test.c
 *
 *  Created on: Feb 6, 2021
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
bhed* binary_header;
char* text_header;
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
create_graph_test(void **state)
{
	int	rc;
	int	i;
	root_obj_t	*root_obj;
	int		curr_idx;
	struct dfs_entry 	dfs_entry = {0};

	char **keys = malloc(2 * sizeof(char*));
	for(i=0; i < 2; i++) {
		keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	strcpy(keys[0], "HELLO_1");
	strcpy(keys[1], "HELLO_2");

	rc = daos_seis_create_graph(NULL, "test",
				    2, keys, &root_obj, O_RDWR,
				    S_IFDIR | S_IWUSR | S_IRUSR);

	parse_functions_t *parse_functions;

	rc = init_parsing_parameters(SEGY, &parse_functions,(void *)daos_file->file);
	assert_int_equal(0, rc);
	rc = parse_text_and_binary_headers(parse_functions);
	assert_int_equal(0, rc);
	text_header = get_text_header(parse_functions);
	binary_header = (bhed*) get_binary_header(parse_functions);
	int objmode = root_obj_get_mode(root_obj);
	seismic_object_oid_oh_t *oid_oh = root_obj_get_id_oh(root_obj);
	rc = root_obj_open(oid_oh, objmode, NULL, -1, &curr_idx);
	rc = root_obj_update_headers(root_obj, text_header, binary_header,
				    NULL, -1, &curr_idx);
	assert_int_equal(0, rc);
	int	num_of_keys = 0;
	uint64_t 	rx_idx;
	uint64_t 	rx_nr;
	rx_idx = 0;
	rx_nr = MAX_KEY_LENGTH * sizeof(char);
	assert_int_equal(rc, 0);
	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS, 1);
	assert_int_equal(0,rc);
	rc = root_obj_set_io_parameters(root_obj, DS_A_NUM_OF_KEYS,
					   DAOS_IOD_SINGLE, sizeof(int),
					   1, NULL, NULL, (char *)&num_of_keys,
					   sizeof(int));
	rc = root_obj_fetch(root_obj, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = root_obj_release_io_parameters(root_obj);
	assert_int_equal(rc, 0);
	char **keys_fetched = malloc(num_of_keys * sizeof(char*));

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS, num_of_keys);
	assert_int_equal(0,rc);

	for(i=0 ; i < num_of_keys; i++) {
		char temp[10]="";
		char akey[MAX_KEY_LENGTH]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);
		keys_fetched[i] = malloc(MAX_KEY_LENGTH * sizeof(char));

		rc = root_obj_set_io_parameters(root_obj, akey,
						   DAOS_IOD_ARRAY, 1,
						   1, &rx_idx, &rx_nr,
						   (char *)keys_fetched[i],
						   MAX_KEY_LENGTH * sizeof(char));
		assert_int_equal(0,rc);
	}
	rc = root_obj_fetch(root_obj, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	for(i=0; i < num_of_keys; i++) {
		assert_true(strcmp(keys_fetched[i],keys[i]) == 0);
	}
	rc = root_obj_close(root_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(rc, 0);

	for(i=0; i<num_of_keys; i++) {
		free(keys_fetched[i]);
	}
	free(keys_fetched);

}

void
fetching_graph_binary_and_text_hdrs_test(void **state)
{
	root_obj_t *root_obj;
	int	curr_idx;
	int		rc;
	int		i;

	root_obj  = daos_seis_open_graph("/test", O_RDWR);
	root_obj->num_of_traces = 1000;

	rc = root_obj_update_num_of_traces(root_obj, NULL, &(root_obj->num_of_traces),
					   -1, &curr_idx);

	int fetched_num_of_traces = root_obj_get_num_of_traces(root_obj);
	assert_int_equal(fetched_num_of_traces, root_obj->num_of_traces);

	char *text_header_fetched = daos_seis_get_text_header(root_obj);
	bhed *binary_header_fetched = daos_seis_get_binary_header(root_obj);

	for(i=0; i < EBCBYTES; i++) {
		assert_true(text_header[i] == text_header_fetched[i]);
	}

	assert_memory_equal((void *)binary_header_fetched, (void *)binary_header, sizeof(bhed));

	free(text_header_fetched);
	free(binary_header_fetched);
	free(text_header);
	free(binary_header);
	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc, 0);

}

int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[12] == 'f') {
		container_string[12] = 'a';
	} else if(container_string[12] == '9') {
		container_string[12] = '1';
	} else {
		container_string[12] = container_string[12] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(dfs_file_mount_test),
    cmocka_unit_test(create_graph_test),
    cmocka_unit_test(fetching_graph_binary_and_text_hdrs_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
