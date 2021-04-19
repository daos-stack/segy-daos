/*
 * dfs_helpers_test.c
 *
 *  Created on: Jan 27, 2021
 *      Author: mirnamoawad
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "daos_primitives/dfs_helpers.h"
#include "daos.h"
#include "daos_fs.h"
#include "string.h"

static daos_handle_t poh;
static daos_handle_t coh;
static daos_handle_t oh;
static dfs_t	*dfs;
//static uuid_t pool_uuid;
//static uuid_t container_uuid;
static char pool_string[37];
static char container_string[37];
static char inputfile[4096];
static char out_file[4096];
static char posix_file[4096];

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
validate_dfs_file_mount_test(void **state)
{

        daos_size_t read_size;
        read_array = malloc(posix_file_size);
        read_size = read_dfs_file_with_offset(daos_file, read_array, posix_file_size, 0);
        // Compare byte arrays.
        int i;
        for(i=0 ; i<posix_file_size; i++){
        	assert_true(ret[i] == read_array[i]);
        }
        free(read_array);
        free(ret);
}

void
dfs_file_unmount_test(void **state)
{
	int	rc;
        rc = dfs_file_exists(out_file);
        assert_int_equal(rc, 1);
        daos_file = open_dfs_file(out_file, S_IFREG | S_IWUSR | S_IRUSR,'r', 0);
        dfs_file_size= get_dfs_file_size(daos_file);
        assert_int_equal(dfs_file_size, 99014040);
        read_array = malloc(dfs_file_size);
        dfs_file_size = read_dfs_file(daos_file, read_array, dfs_file_size);
        dfs_file_size = write_posix(posix_file, read_array, dfs_file_size);
        assert_int_equal(dfs_file_size, 99014040);
}

void
validate_dfs_file_unmount_test(void **state)
{

        posix_file_size = read_posix(posix_file, &ret);
        int i;
        for(i=0 ; i<posix_file_size; i++){
        	assert_true(ret[i] == read_array[i]);
        }
        free(ret);
        free(read_array);
}

int
main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[1] == 'f') {
		container_string[1] = '0';
	} else if(container_string[1] == '9') {
		container_string[1] = 'a';

	} else {
		container_string[1] = container_string[1] + 1;
	}

	strcpy(inputfile, argv[3]);
	strcpy(out_file, argv[4]);
	strcpy(posix_file, argv[5]);
  const struct CMUnitTest tests[] = {
		    cmocka_unit_test(dfs_file_mount_test),
		    cmocka_unit_test(validate_dfs_file_mount_test),
		    cmocka_unit_test(dfs_file_unmount_test),
		    cmocka_unit_test(validate_dfs_file_unmount_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
