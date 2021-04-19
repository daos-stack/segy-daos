/*
 * root_test.c
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
#include "graph_objects/root.h"

static char pool_string[37];
static char container_string[37];

DAOS_FILE * daos_file;
uint64_t	oid_hi = 1;
daos_obj_id_t	oid = { .hi = 1, .lo = 1 }; /** object ID */

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

//void print_io_parameters(object_io_parameters_t* io_parameters){
//	printf("nr: %d\n", io_parameters->nr);
//	for(int i = 0; i < io_parameters->nr; i++){
//		printf("iod type[%d]: %d\n", i,(io_parameters)->iods[i].iod_type);
//		printf("iod nr[%d]: %d\n", i,(io_parameters)->iods[i].iod_nr);
//		printf("iod size[%d]: %d\n", i,(io_parameters)->iods[i].iod_size);
//}
void
root_object_test(void **state)
{
	int	rc;
	int	i;
	root_obj_t	*root_obj;
	int		curr_idx;
	struct dfs_entry 	dfs_entry = {0};

	char **keys = malloc(2 * sizeof(char*));
	for(i=0; i<2; i++) {
		keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	strcpy(keys[0],"KEY1");
	strcpy(keys[1], "KEY2");

	seismic_object_oid_oh_t *parent = NULL;
	mode_t mode = S_IFDIR | S_IWUSR | S_IRUSR;
	rc = root_obj_create(&root_obj, O_RDWR, keys, 2,
			     mode, parent);
	assert_int_equal(rc, 0);

	seismic_object_oid_oh_t *oid_oh = root_obj_get_id_oh(root_obj);
	int objmode = root_obj_get_mode(root_obj);

	rc = root_obj_open(oid_oh, objmode, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	dfs_entry.oid = root_obj_get_oid(root_obj);
	dfs_entry.mode = root_obj_get_permissions_and_type(root_obj);
	dfs_entry.chunk_size = 0;
	dfs_entry.atime = dfs_entry.mtime = dfs_entry.ctime = time(NULL);

	rc = insert_dfs_entry(root_obj_get_parent_oh(root_obj), DAOS_TX_NONE,
			"TEST_ROOT_OBJECT", dfs_entry);
	if (rc != 0) {
		printf("Inserting  root object under parent failed,"
		    " error code = %d \n",rc);
	}
	assert_int_equal(rc, 0);

	unsigned int iod_nr = 1;
	uint64_t *rx_idx= malloc(iod_nr *sizeof(uint64_t));
	uint64_t *rx_nr = malloc(iod_nr *sizeof(uint64_t));
	int	j;
	for(j=0; j < iod_nr; j++) {
		rx_idx[j] = 0;
		rx_nr[j] = MAX_KEY_LENGTH * sizeof(char);
	}
	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS,2);
	assert_int_equal(0, rc);

	for(i=0; i < 2; i++) {

		char temp[10]= "";
		char akey[MAX_KEY_LENGTH]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);
		rc = root_obj_set_io_parameters(root_obj, akey, DAOS_IOD_ARRAY,
						1, 1, rx_idx, rx_nr,
					     (char*)keys[i], MAX_KEY_LENGTH * sizeof(char));
		assert_int_equal(rc, 0);

	}

	rc = root_obj_update(root_obj, NULL, -1 , &curr_idx);
	assert_int_equal(0, rc);

	rc = root_obj_close(root_obj, NULL, -1, &curr_idx, 0);
	assert_int_equal(0, rc);

	rc = root_obj_release_io_parameters(root_obj);
	assert_int_equal(0, rc);

	rc = root_obj_open(oid_oh, objmode, NULL, -1, &curr_idx);
	assert_int_equal(0, rc);

	char **key_fetched = malloc(2 * sizeof(char*));
	for(i=0 ; i <2; i++) {
		key_fetched[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
	}
	root_obj->oid_oh.oh = oid_oh->oh;
	root_obj->oid_oh.oid = oid_oh->oid;

	rc = root_obj_init_io_parameters(root_obj, 0, DS_D_KEYS,2);
	assert_int_equal(0, rc);
	for(i=0; i <2; i++) {
		char temp[10]= "";
		char akey[MAX_KEY_LENGTH]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);
		rc = root_obj_set_io_parameters(root_obj, akey, DAOS_IOD_ARRAY,
						     1, 1, rx_idx, rx_nr,
						     (char*)key_fetched[i],
						     MAX_KEY_LENGTH * sizeof(char));
		assert_int_equal(rc, 0);
	}

	rc = root_obj_fetch(root_obj, NULL, -1 , &curr_idx);
	assert_int_equal(0, rc);

	rc = root_obj_close(root_obj, NULL, -1, &curr_idx, 1);
	assert_int_equal(0, rc);

	for(i=0; i < 2; i++) {
		assert_true(strcmp(key_fetched[i],keys[i]) == 0);
	}
	free(rx_nr);
	free(rx_idx);
	for(i=0; i<2; i++) {
		free(key_fetched[i]);
		free(keys[i]);
	}
	free(key_fetched);
	free(keys);

}

int main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[3] == 'f') {
		container_string[3] = '5';
	} else if(container_string[3] == '9') {
		container_string[3] = 'f';
	} else {
		container_string[3] = container_string[3] + 1;
	}

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(root_object_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
