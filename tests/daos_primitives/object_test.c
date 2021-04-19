/*
 * object_test.c
 *
 *  Created on: Jan 23, 2021
 *      Author: mirnamoawad
 */


#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "daos_primitives/object.h"
#include "daos.h"
#include "daos_fs.h"
#include "string.h"

static daos_handle_t poh;
static daos_handle_t coh;
static daos_handle_t oh;
static char pool_string[37];
static char container_string[37];

uint64_t	oid_hi = 1;
daos_obj_id_t	oid = { .hi = 1, .lo = 1 }; /** object ID */


static inline void
new_oid(void)
{
	oid.hi = ++oid_hi;
	oid.lo = 1;
}
static int setup(void **state)
{
	int rc;
	d_rank_list_t *svc = NULL;
	uuid_t pool_uuid;
	uuid_t co_uuid;

	rc = uuid_parse(pool_string, pool_uuid);
	assert_int_equal(rc, 0);
	rc = daos_init();
	assert_int_equal(rc, 0);
	rc = daos_pool_connect(pool_uuid, 0, DAOS_PC_RW, &poh, NULL, NULL);
	assert_int_equal(rc, 0);
	rc = uuid_parse(container_string, co_uuid);
	assert_int_equal(rc, 0);
	rc = daos_cont_create(poh, co_uuid, NULL, NULL);
	assert_int_equal(rc, 0);
	rc = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
	assert_int_equal(rc, 0);
	return 0;
}

static int teardown(void **state)
{
	int rc;
	rc = daos_cont_close(coh, 0);
	assert_int_equal(rc, 0);
	rc = daos_pool_disconnect(poh, NULL);
	assert_int_equal(rc, 0);
	rc = daos_fini();
	assert_int_equal(rc, 0);
	return 0;
}

void
object_write_test(void **state)
{
	object_io_parameters_t	*io_parameters;
	seismic_object_oid_oh_t *oid_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int 	rc;
	int	i;
	int	*x;
	int	curr_idx;
	new_oid();
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	x = malloc(sizeof(int));
	*x = 10;
	oid_oh->oid = oid;
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x,
				      sizeof(int));
	assert_int_equal(rc, 0);

	rc = update_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	/**REopen daos object to read written data */
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	int	*z = malloc(sizeof(int));
	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)z,
				      sizeof(int));

	rc =
	fetch_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	assert_int_equal(*z, *x);
	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	free(oid_oh);
}

void
object_array_test(void **state)
{
	object_io_parameters_t	*io_parameters;
	seismic_object_oid_oh_t *oid_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int 	rc;
	int	i;
	new_oid();
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	int *x = malloc(5 * sizeof(int));
	x[0] = 1;
	x[1] = 5;
	x[2] = 6;
	x[3] = 20;
	x[4] = 15;
	int	curr_idx;
	oid_oh->oid = oid;
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);
	int	j;
	uint64_t *rx_idx = malloc(1 * sizeof(uint64_t));
	uint64_t *rx_nr = malloc(1 * sizeof(uint64_t));

	for(j=0; j < 1; j++) {
		rx_idx[j] = 0;
		rx_nr[j] = 5;
	}

	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY_ARRAY", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST", DAOS_IOD_ARRAY,
				      sizeof(int), 1, rx_idx, rx_nr,(char*)x,
				      5*sizeof(int));
	assert_int_equal(rc, 0);
	rc = update_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);


	/**REopen daos object to read written data */
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	int	*z = malloc(5 * sizeof(int));

	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY_ARRAY", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST", DAOS_IOD_ARRAY,
				      sizeof(int), 1, rx_idx, rx_nr,(char*)z,
				      5 *sizeof(int));
	assert_int_equal(rc, 0);

	rc = fetch_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);
	assert_int_equal(z[0],1);
	assert_int_equal(z[1],5);
	assert_int_equal(z[2],6);
	assert_int_equal(z[3],20);
	assert_int_equal(z[4],15);
	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	free(rx_idx);
	free(rx_nr);
	free(oid_oh);
}

void
object_two_akeystest(void **state)
{
	object_io_parameters_t	*io_parameters;
	seismic_object_oid_oh_t *oid_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int 	rc;
	int	i;
	int	j;

	new_oid();
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	int *single_value = malloc(sizeof(int));
	*single_value = 100;
	int *array = malloc(5 * sizeof(int));
	array[0] = 1;
	array[1] = 5;
	array[2] = 6;
	array[3] = 10;
	array[4] = 20;
	int *array2 = malloc(5 * sizeof(int));
	array2[0] = 200;
	array2[1] = 100;
	array2[2] = 60;
	array2[3] = 50;
	array2[4] = 1000;

	int	curr_idx;
	unsigned int num_oids_sgls = 2;

	uint64_t *rx_idx = malloc(sizeof(uint64_t));
	uint64_t *rx_nr = malloc(sizeof(uint64_t));

	for(j=0; j < 1; j++) {
		rx_idx[j] = 0;
		rx_nr[j] = 5;
	}
	oid_oh->oid = oid;
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);


	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY_TWO_AKEYS", 3);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST_SINGLE_VALUE", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)single_value,
				      sizeof(int));
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST_ARRAY", DAOS_IOD_ARRAY,
				      sizeof(int), 1, rx_idx, rx_nr,(char*)array,
				      5 *sizeof(int));
	assert_int_equal(rc, 0);

	rc = set_object_io_parameters(io_parameters, "AKEY_TEST_ARRAY2", DAOS_IOD_ARRAY,
				      sizeof(int), 1, rx_idx, rx_nr,(char*)array2,
				      5 *sizeof(int));
	assert_int_equal(rc, 0);

	rc = update_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	int	*array_read = malloc(5 * sizeof(int));
	int	*array2_read = malloc(5 * sizeof(int));
	int	*single_value_read = malloc(sizeof(int));
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY_TWO_AKEYS", 3);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST_SINGLE_VALUE", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)single_value_read,
				      sizeof(int));
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST_ARRAY", DAOS_IOD_ARRAY,
				      sizeof(int), 1, rx_idx, rx_nr,(char*)array_read,
				      5 *sizeof(int));
	assert_int_equal(rc, 0);

	rc = set_object_io_parameters(io_parameters, "AKEY_TEST_ARRAY2", DAOS_IOD_ARRAY,
				      sizeof(int), 1, rx_idx, rx_nr,(char*)array2_read,
				      5 *sizeof(int));
	assert_int_equal(rc, 0);

	rc = fetch_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	assert_int_equal(*single_value_read,100);
	assert_int_equal(array_read[0],1);
	assert_int_equal(array_read[1],5);
	assert_int_equal(array_read[2],6);
	assert_int_equal(array_read[3],10);
	assert_int_equal(array_read[4],20);
	assert_int_equal(array2_read[0],200);
	assert_int_equal(array2_read[1],100);
	assert_int_equal(array2_read[2],60);
	assert_int_equal(array2_read[3],50);
	assert_int_equal(array2_read[4],1000);
	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	free(rx_idx);
	free(rx_nr);
	free(oid_oh);
}

void
object_destroy_test(void **state)
{
	object_io_parameters_t	*io_parameters;
	seismic_object_oid_oh_t *oid_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int 	rc;
	int	i;
	int	*x;
	int	curr_idx;
	new_oid();
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	x = malloc(sizeof(int));
	*x = 10;
	oid_oh->oid = oid;
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = init_object_io_parameters(&io_parameters, 0, "NEW_DKEY", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters, "AKEY_TEST", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x,
				      sizeof(int));
	assert_int_equal(rc, 0);

	rc = update_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_object(oid_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	rc = release_object_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	/**REopen daos object to read written data */
	rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = destroy_object(oid_oh, 0, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	free(oid_oh);
}

void
object_read_write_with_events_test(void **state)
{
	object_io_parameters_t **io_parameters = malloc(5 * sizeof(object_io_parameters_t*));
	seismic_object_oid_oh_t *object_id_oh = malloc(5 * sizeof(seismic_object_oid_oh_t));
	operations_controllers_t *op_controller;

	int 	rc;
	int	i;
	int	curr_idx;

	rc = init_operations_controller(&op_controller, 0, 1, 3);
	assert_int_equal(rc, 0);
	int *x = malloc(sizeof(int));
	*x = 10;

	new_oid();
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);


	rc = init_object_io_parameters(&io_parameters[0], 0, "DKEY", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters[0], "AKEY", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x,
				      sizeof(int));

	object_id_oh[0].oid = oid;
	rc = open_object(&(object_id_oh[0]), NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_object_entry(&(object_id_oh[0]), op_controller, io_parameters[0],
				 -1, &curr_idx);

	assert_int_equal(rc, 0);
	int *x1 = malloc(sizeof(int));
	*x1 = 10;

	new_oid();
//	daos_obj_generate_id(&oid, 0, DAOS_OBJ_CLASS_ID, 0);
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	rc = init_object_io_parameters(&io_parameters[1], 0, "DKEY1", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters[1], "AKEY1", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x1,
				      sizeof(int));

	object_id_oh[1].oid = oid;
	rc = open_object(&(object_id_oh[1]), NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_object_entry(&(object_id_oh[1]), op_controller, io_parameters[1],
				 -1, &curr_idx);
	assert_int_equal(rc, 0);
	int *x2 = malloc(sizeof(int));
	*x2 = 10;


	new_oid();
//	daos_obj_generate_id(&oid, 0, DAOS_OBJ_CLASS_ID, 0);
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	rc = init_object_io_parameters(&io_parameters[2], 0, "DKEY2", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters[2], "AKEY2", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x2,
				      sizeof(int));

	object_id_oh[2].oid = oid;
	rc = open_object(&(object_id_oh[2]), NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_object_entry(&(object_id_oh[2]), op_controller, io_parameters[2],
				 -1, &curr_idx);
	assert_int_equal(rc, 0);
	int *x3 = malloc(sizeof(int));
	*x3 = 10;

	new_oid();
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	rc = init_object_io_parameters(&io_parameters[3], 0, "DKEY3", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters[3], "AKEY3", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x3,
				      sizeof(int));

	object_id_oh[3].oid = oid;
	rc = open_object(&(object_id_oh[3]), NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_object_entry(&(object_id_oh[3]), op_controller, io_parameters[3],
				 -1, &curr_idx);
	assert_int_equal(rc, 0);
	int *x4 = malloc(sizeof(int));
	*x4 = 10;

	new_oid();
//	daos_obj_generate_id(&oid, 0, DAOS_OBJ_CLASS_ID, 0);
	daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

	rc = init_object_io_parameters(&io_parameters[4], 0, "DKEY4", 1);
	assert_int_equal(rc, 0);
	rc = set_object_io_parameters(io_parameters[4], "AKEY4", DAOS_IOD_SINGLE,
				      sizeof(int), 1, NULL, NULL,(char*)x4,
				      sizeof(int));

	object_id_oh[4].oid = oid;
	rc = open_object(&(object_id_oh[4]), NULL, coh, DAOS_OO_RW, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_object_entry(&(object_id_oh[4]), op_controller, io_parameters[4],
				 -1, &curr_idx);
	assert_int_equal(rc, 0);


	rc = release_operations_controller(op_controller);
	assert_int_equal(rc, 0);

	for(i=0; i < 5; i++) {
		rc = close_object(&(object_id_oh[i]), NULL, -1, &curr_idx);
		assert_int_equal(rc, 0);
		rc = release_object_io_parameters(io_parameters[i]);
		assert_int_equal(rc, 0);
	}
	free(object_id_oh);
}

void
object_read_write_multiple_strings(void **state)
{
		object_io_parameters_t	*io_parameters;
		seismic_object_oid_oh_t *oid_oh = malloc(sizeof(seismic_object_oid_oh_t));
		int 	rc;
		int	i;
		int	j;

		new_oid();
		daos_obj_generate_oid(coh, &oid, 0, DAOS_OBJ_CLASS_ID, 0, 0);

		char **keys_written = malloc(2 * sizeof(char *));
		char **keys_fetched = malloc(2 * sizeof(char*));
		for(i=0; i <2; i++) {
			keys_written[i] = malloc(2000 * sizeof(char));
			keys_fetched[i] = malloc(2000 * sizeof(char));
		}
		strcpy(keys_written[0], "KEY_1");
		strcpy(keys_written[1], "KEY_2");


		int	curr_idx;
		unsigned int num_oids_sgls = 2;

		uint64_t *rx_idx = malloc(sizeof(uint64_t));
		uint64_t *rx_nr = malloc(sizeof(uint64_t));

		for(j=0; j < 1; j++) {
			rx_idx[j] = 0;
			rx_nr[j] = 2000 * sizeof(char);
		}
		oid_oh->oid = oid;
		rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
		assert_int_equal(rc, 0);


		rc = init_object_io_parameters(&io_parameters, 0, "DKEY_ARRAY", 2);
		assert_int_equal(rc, 0);
		rc = set_object_io_parameters(io_parameters, "AKEY1", DAOS_IOD_ARRAY,
					      1, 1, rx_idx, rx_nr,(char*)keys_written[0],
					      2000 *sizeof(char));
		assert_int_equal(rc, 0);

		rc = set_object_io_parameters(io_parameters, "AKEY2", DAOS_IOD_ARRAY,
					      1, 1, rx_idx, rx_nr,(char*)keys_written[1],
					      2000 *sizeof(char));
		assert_int_equal(rc, 0);

		rc = update_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
		assert_int_equal(rc, 0);

		rc = close_object(oid_oh, NULL, -1, &curr_idx);
		assert_int_equal(rc, 0);

		rc = release_object_io_parameters(io_parameters);
		assert_int_equal(rc, 0);

		rc = open_object(oid_oh, NULL, coh, DAOS_OO_RW, -1, &curr_idx);
		assert_int_equal(rc, 0);

		rc = init_object_io_parameters(&io_parameters, 0, "DKEY_ARRAY", 2);
		assert_int_equal(rc, 0);
		rc = set_object_io_parameters(io_parameters, "AKEY1", DAOS_IOD_ARRAY,
					      1, 1, rx_idx, rx_nr,(char*)keys_fetched[0],
					      2000 *sizeof(char));
		assert_int_equal(rc, 0);

		rc = set_object_io_parameters(io_parameters, "AKEY2", DAOS_IOD_ARRAY,
					      1, 1, rx_idx, rx_nr,(char*)keys_fetched[1],
					      2000 *sizeof(char));
		assert_int_equal(rc, 0);

		rc = fetch_object_entry(oid_oh, NULL, io_parameters, -1, &curr_idx);
		assert_int_equal(rc, 0);

		rc = release_object_io_parameters(io_parameters);
		assert_int_equal(rc, 0);

		rc = close_object(oid_oh, NULL, -1, &curr_idx);
		assert_int_equal(rc, 0);

		free(rx_idx);
		free(rx_nr);
		free(oid_oh);
}

int
main(int argc, char* argv[])
{
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);

  const struct CMUnitTest tests[] = {
		    cmocka_unit_test(object_write_test),
		    cmocka_unit_test(object_array_test),
		    cmocka_unit_test(object_two_akeystest),
		    cmocka_unit_test(object_destroy_test),
		    cmocka_unit_test(object_read_write_with_events_test),
		    cmocka_unit_test(object_read_write_multiple_strings)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
