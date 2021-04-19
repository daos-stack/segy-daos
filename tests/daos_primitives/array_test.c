/*
 * array_test.c
 *
 *  Created on: Jan 25, 2021
 *      Author: mirnamoawad
 */


#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "daos.h"
#include "daos_fs.h"
#include "string.h"
#include "daos_primitives/array.h"

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
array_read_write_one_iod_test(void **state)
{
	int	i;
	int	rc;
	array_io_parameters_t *io_parameters;
	seismic_object_oid_oh_t *array_id_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int	*array = malloc(5 * sizeof(int));
	for(i=0; i < 5; i++) {
		array[i] = i*2;
	}
	int	curr_idx;

	uint64_t *rg_idx = malloc(sizeof(uint64_t));
	uint64_t *rg_len = malloc(sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = 5;

	new_oid();

	rc = daos_array_generate_id(&oid, DAOS_OBJ_CLASS_ID, false, 0);

	assert_int_equal(rc, 0);

	rc = init_array_io_parameters(&io_parameters);
	assert_int_equal(rc, 0);
	rc = set_array_io_parameters(io_parameters,1, rg_idx, rg_len, 0,
				      (char*)array, 5*sizeof(int));
	assert_int_equal(rc, 0);

	daos_size_t cell_size = sizeof(int);
	daos_size_t chunk_size = 2 * sizeof(int);

	array_id_oh->oid = oid;

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_array_object_entry(array_id_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_array_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	int	*array_read = malloc(5 *sizeof(int));
	rc = init_array_io_parameters(&io_parameters);
	assert_int_equal(rc, 0);
	rc = set_array_io_parameters(io_parameters, 1, rg_idx, rg_len,0,(char*)array_read, 5*sizeof(int));
	assert_int_equal(rc, 0);

	array_id_oh->oid = oid;
	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);

	rc = fetch_array_object_entry(array_id_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);
	for(i=0; i <5; i++) {
		assert_int_equal(array_read[i], i*2);
	}
	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_array_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	free(array_read);
	free(array);
	free(rg_idx);
	free(rg_len);
	free(array_id_oh);
}

void
array_read_write_multiple_iods_test(void **state)
{
	int	i;
	int	rc;
	array_io_parameters_t *io_parameters;
	seismic_object_oid_oh_t *array_id_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int	*array = malloc(5 * sizeof(int));
	for(i=0; i < 5; i++) {
		array[i] = i*2;
	}

	uint64_t *rg_idx = malloc(sizeof(uint64_t));
	uint64_t *rg_len = malloc(sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = 5;
	int 	curr_idx;
	new_oid();

	rc = daos_array_generate_id(&oid, DAOS_OBJ_CLASS_ID, false, 0);

	assert_int_equal(rc, 0);

	rc = init_array_io_parameters(&io_parameters);
	assert_int_equal(rc, 0);
	rc = set_array_io_parameters(io_parameters,1, rg_idx, rg_len,0,(char*)array, 5*sizeof(int));
	assert_int_equal(rc, 0);

	daos_size_t cell_size = sizeof(int);
	daos_size_t chunk_size = 2 * sizeof(int);

	array_id_oh->oid = oid;

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_array_object_entry(array_id_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_array_io_parameters(io_parameters);
	assert_int_equal(rc, 0);




	int	*array_read = malloc(3 *sizeof(int));
	uint64_t *rg_idx_read = malloc(2 * sizeof(uint64_t));
	uint64_t *rg_len_read = malloc(2 *sizeof(uint64_t));
	rg_idx_read[0] = 0;
	rg_len_read[0] = 1;
	rg_idx_read[1] = 3;
	rg_len_read[1] = 2;

	rc = init_array_io_parameters(&io_parameters);
	assert_int_equal(rc, 0);
	rc = set_array_io_parameters(io_parameters, 2, rg_idx_read, rg_len_read,0,(char*)array_read, 3*sizeof(int));
	assert_int_equal(rc, 0);
	array_id_oh->oid = oid;

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	rc = fetch_array_object_entry(array_id_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	assert_int_equal(array_read[0], 0);
	assert_int_equal(array_read[1], 3*2);
	assert_int_equal(array_read[2], 4*2);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_array_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	free(array_read);
	free(rg_idx_read);
	free(rg_len_read);
	free(array);
	free(rg_idx);
	free(rg_len);
	free(array_id_oh);
}

void
array_get_size_test(void **state)
{
	int	i;
	int	rc;
	array_io_parameters_t *io_parameters;
	seismic_object_oid_oh_t *array_id_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int	*array = malloc(5 * sizeof(int));
	for(i=0; i < 5; i++) {
		array[i] = i*2;
	}
	int	curr_idx;
	uint64_t *rg_idx = malloc(sizeof(uint64_t));
	uint64_t *rg_len = malloc(sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = 5;

	new_oid();

	rc = daos_array_generate_id(&oid, DAOS_OBJ_CLASS_ID, false, 0);

	assert_int_equal(rc, 0);

	rc = init_array_io_parameters(&io_parameters);
	assert_int_equal(rc, 0);
	rc = set_array_io_parameters(io_parameters, 1, rg_idx, rg_len,0,(char*)array, 5*sizeof(int));
	assert_int_equal(rc, 0);
	daos_size_t cell_size = sizeof(int);
	daos_size_t chunk_size = 2 * sizeof(int);

	array_id_oh->oid = oid;

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_array_object_entry(array_id_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_array_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	assert_int_equal(rc, 0);

	uint64_t array_size;
	rc = get_size_array_object(array_id_oh, NULL, &array_size, -1, &curr_idx);
	assert_int_equal(rc, 0);
	assert_int_equal(array_size, 5);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	free(array);
	free(rg_idx);
	free(rg_len);
	free(array_id_oh);
}

void
array_destroy_test(void **state)
{
	int	i;
	int	rc;
	array_io_parameters_t *io_parameters;
	seismic_object_oid_oh_t *array_id_oh = malloc(sizeof(seismic_object_oid_oh_t));
	int	*array = malloc(5 * sizeof(int));
	for(i=0; i < 5; i++) {
		array[i] = i*2;
	}
	int	curr_idx;
	uint64_t *rg_idx = malloc(sizeof(uint64_t));
	uint64_t *rg_len = malloc(sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = 5;

	new_oid();

	rc = daos_array_generate_id(&oid, DAOS_OBJ_CLASS_ID, false, 0);

	assert_int_equal(rc, 0);

	rc = init_array_io_parameters(&io_parameters);
	assert_int_equal(rc, 0);
	rc = set_array_io_parameters(io_parameters, 1, rg_idx, rg_len,0,(char*)array, 5*sizeof(int));
	assert_int_equal(rc, 0);

	daos_size_t cell_size = sizeof(int);
	daos_size_t chunk_size = 2 * sizeof(int);

	array_id_oh->oid = oid;

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = update_array_object_entry(array_id_oh, NULL, io_parameters, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = release_array_io_parameters(io_parameters);
	assert_int_equal(rc, 0);

	rc = open_array_object(array_id_oh, NULL, coh,
				DAOS_OO_RW, cell_size,
				chunk_size, -1, &curr_idx);
	assert_int_equal(rc, 0);

	uint64_t array_size;
	rc = destroy_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);

	rc = close_array_object(array_id_oh, NULL, -1, &curr_idx);
	assert_int_equal(rc, 0);
	free(array);
	free(rg_idx);
	free(rg_len);
	free(array_id_oh);

}

void
array_read_write_one_iod_events_test(void **state)
{
	int	i;
	int	rc;
	int 	curr_idx ;
	int	parent;
	array_io_parameters_t **io_parameters = malloc(5 * sizeof(array_io_parameters_t*));
	seismic_object_oid_oh_t *array_id_oh = malloc(5 * sizeof(seismic_object_oid_oh_t));
	operations_controllers_t *op_controller;

	rc = init_operations_controller(&op_controller, 0, 1, 2);
	assert_int_equal(rc, 0);
	int	**array = malloc(5 * sizeof(int*));
	uint64_t *rg_idx = malloc(sizeof(uint64_t));
	uint64_t *rg_len = malloc(sizeof(uint64_t));
	rg_idx[0] = 0;
	rg_len[0] = 5;
	d_sg_list_t 		sgl;
	d_iov_t 		iov;
	daos_array_iod_t 	iod;
	daos_range_t	rg;
	int	j;
	for(j=0; j < 5; j++) {
		new_oid();

		rc = daos_array_generate_id(&oid, DAOS_OBJ_CLASS_ID, false, 0);
		assert_int_equal(rc, 0);

		array[j] = malloc(5 * sizeof(int));

		for(i=0; i < 5; i++) {
			array[j][i] = (i*2+j);
		}

		rc = init_array_io_parameters(&io_parameters[j]);
		assert_int_equal(rc, 0);
		rc = set_array_io_parameters(io_parameters[j], 1, rg_idx, rg_len, 0,
					      (char*)array[j], 5*sizeof(int));
		assert_int_equal(rc, 0);

		daos_size_t cell_size = sizeof(int);
		daos_size_t chunk_size = 2 * sizeof(int);

		array_id_oh[j].oid = oid;


		rc = open_array_object(&array_id_oh[j], NULL,
				       coh, DAOS_OO_RW, cell_size,
				       chunk_size, -1, &curr_idx);
		assert_int_equal(rc, 0);

		rc = update_array_object_entry(&array_id_oh[j], op_controller,
						io_parameters[j], -1, &curr_idx);
		assert_int_equal(rc, 0);
	}

	rc = release_operations_controller(op_controller);
	assert_int_equal(rc, 0);

	for(i=0; i < 5; i++) {
		rc = close_array_object(&array_id_oh[i], NULL, -1, &curr_idx);
		assert_int_equal(rc, 0);

		rc = release_array_io_parameters(io_parameters[i]);
		assert_int_equal(rc, 0);

		free(array[i]);
	}
	free(array);
	free(rg_idx);
	free(rg_len);
	free(array_id_oh);
	free(io_parameters);
}

int
main(int argc, char* argv[])
{
	int length;
	char str[2];
	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);

  const struct CMUnitTest tests[] = {
		    cmocka_unit_test(array_read_write_one_iod_test),
		    cmocka_unit_test(array_read_write_multiple_iods_test),
		    cmocka_unit_test(array_get_size_test),
		    cmocka_unit_test(array_destroy_test),
		    cmocka_unit_test(array_read_write_one_iod_events_test)
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
