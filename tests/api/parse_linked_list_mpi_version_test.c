/*
 * parse_linked_list_mpi_version.c
 *
 *  Created on: Apr 8, 2021
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
#include <mpi.h>

static char pool_string[37];
static char container_string[37];
//static char out_path[37];
static int	num_of_processes;
static int	process_rank;

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
	rc = init_dfs_api(pool_string, container_string, 1,0);
	return rc;
}

static int teardown(void **state)
{
	int rc;
	rc = fini_dfs_api();

	return rc;
}

trace_t**
trace_generator(int range)
{
	trace_t **tr = malloc(range * sizeof(trace_t*));
	int size = 0, i = 1, j;
	while (size < range) {
		j = 0;
		while (j < i) {
			trace_t *temp;
			trace_init(&temp);
			temp->fldr = i;
			temp->cdp = size;
			temp->d1 = 10*size;
			temp->d2 = 25*size;
			temp->cdpt = 50+size;
			temp->f1 = size/10;
			temp->f2 = size/25;
			temp->ep = 50-size;
			temp->ns = 0;
			temp->data = NULL;
			tr[size++] = temp;
			if (size >= range)
				break;
			j++;
		}
		i++;
	}
	return tr;
}


int
ensemble_list_generator(ensemble_list **e, int range)
{
	trace_t **tr = trace_generator(range);
	*e = init_ensemble_list();
	int flag = 0, rc;
	for (int i = 0; i < 10; i++) {
		if (i != 0) {
			if (tr[i]->fldr != tr[i - 1]->fldr) {
				flag = 0;
			} else {
				flag = 1;
			}
		}
		rc = ensemble_list_add_trace(tr[i], *e, flag);

	}
	return rc;
}

static void parse_linked_list_test(void **state)
{
	int 			rc;
	int 			i;
	root_obj_t		*root_obj;
	parsing_helpers_t 	 parsing_helpers;
	gather_obj_t 		*seismic_obj;
	int			curr_idx;
	char**			keys;

	if(process_rank == 0){
		keys = malloc(2 * sizeof(char*));
		for(i=0; i < 2; i++) {
			keys[i] = malloc(MAX_KEY_LENGTH * sizeof(char));
		}
		strcpy(keys[0], "fldr");
		strcpy(keys[1], "cdp");
		rc = daos_seis_create_graph(NULL, "test",
					    2, keys, &root_obj, O_RDWR,
					    S_IFDIR | S_IWUSR | S_IRUSR);

		assert_int_equal(rc,0);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	rc = mutex_lock_init(&parsing_helpers.traces_mutex, 0);

	rc = mutex_lock_init(&parsing_helpers.gather_mutex, 0);

	MPI_Barrier(MPI_COMM_WORLD);

	root_obj = daos_seis_open_graph("/test", O_RDWR);
	ensemble_list *en;
	rc = ensemble_list_generator(&en, 10);
	assert_int_equal(rc, 0);

	rc = daos_seis_parse_linked_list(root_obj,en,DUPLICATE_TRACES,&parsing_helpers);
	assert_int_equal(rc,0);

	rc = daos_seis_close_graph(root_obj);
	assert_int_equal(rc,0);

	MPI_Barrier(MPI_COMM_WORLD);
	if(process_rank == 0) {
		root_obj = daos_seis_open_graph("/test", O_RDWR);
		int number;
		root_obj_fetch_num_of_keys(root_obj,-1,&curr_idx,&number);
		assert_int_equal(number,2);

		gather_obj_open(&(root_obj->gather_oids[1]),O_RDWR,NULL,-1,&curr_idx);
		gather_obj_fetch_entries(&seismic_obj,root_obj->gather_oids[1],root_obj->keys[1],1);
		assert_int_equal(seismic_obj->number_of_gathers,10);
		assert_int_equal(seismic_obj->unique_values[0].i,0);
		assert_int_equal(seismic_obj->unique_values[9].i,9);

		char temp[200] = "";
		char gather_dkey_name[200] = "";
		strcat(gather_dkey_name, seismic_obj->name);
		strcat(gather_dkey_name, KEY_SEPARATOR);
		val_sprintf(temp, seismic_obj->unique_values[3], hdtype(seismic_obj->name));
		strcat(gather_dkey_name, temp);
		gather_obj_fetch_gather_num_of_traces(seismic_obj,NULL,gather_dkey_name,&number,-1,&curr_idx);
		assert_int_equal(number,2);

		daos_obj_id_t daos_oid;
		gather_obj_fetch_gather_traces_array_oid(seismic_obj,NULL,gather_dkey_name,&daos_oid,-1,&curr_idx);
		generic_value val;
		gather_obj_fetch_gather_unique_value(seismic_obj,NULL,gather_dkey_name,&val,-1,&curr_idx);
		assert_int_equal(val.i,3);

		seismic_object_oid_oh_t oid_oh;
		oid_oh.oid = daos_oid;
		traces_array_obj_open(&oid_oh,O_RDWR,NULL,-1,&curr_idx,sizeof(daos_obj_id_t),
				   500 * sizeof(daos_obj_id_t));
		traces_array_obj_t* tr_array;
		tr_array = malloc(sizeof(traces_array_obj_t));
		tr_array->io_parameters = NULL;
		tr_array->daos_mode = O_RDWR;
		tr_array->num_of_traces = number;
		tr_array->oid_oh = oid_oh;
		tr_array->oids = malloc(number * sizeof(daos_obj_id_t));
		traces_array_obj_fetch_oids(tr_array,NULL,-1,&curr_idx,tr_array->oids,tr_array->num_of_traces,0);

		seismic_object_oid_oh_t hdr_oid_oh;
		hdr_oid_oh.oid = tr_array->oids[0];
		rc = traces_array_obj_close(tr_array, NULL, -1, &curr_idx, 1);
		trace_hdr_obj_open(&hdr_oid_oh,O_RDWR,NULL,-1,&curr_idx);
		trace_hdr_obj_t* tr_hdr = malloc(sizeof(trace_hdr_obj_t));
		tr_hdr->daos_mode = O_RDWR;
		tr_hdr->io_parameters = NULL;
		tr_hdr->oid_oh = hdr_oid_oh;
		tr_hdr->trace = NULL;
		trace_t* tr = malloc(sizeof(trace_t));
		trace_hdr_fetch_headers(tr_hdr,tr,NULL,-1,&curr_idx);

		assert_int_equal(tr->fldr,3);

		free(tr);
		free(keys[0]);
		free(keys[1]);
		free(keys);
		rc = daos_seis_close_graph(root_obj);
		assert_int_equal(rc,0);

	}
	MPI_Barrier(MPI_COMM_WORLD);

}

int main(int argc, char* argv[])
{

	int rc;
	rc = MPI_Init(&argc, &argv);

	MPI_Comm_size(MPI_COMM_WORLD, &num_of_processes);
//	printf("number of processes  = %d \n", num_of_processes);
	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
//	printf("process id = %d \n", process_rank);

	strcpy(pool_string, argv[1]);
	strcpy(container_string, argv[2]);
	if(container_string[24] == 'f') {
		container_string[24] = 'b';
	} else if(container_string[24]== '9') {
		container_string[24] = '2';
	} else {
		container_string[24] = container_string[24] + 2;
	}

	MPI_Barrier(MPI_COMM_WORLD);

  const struct CMUnitTest tests[] = {
    cmocka_unit_test(parse_linked_list_test)
  };
  rc =  cmocka_run_group_tests(tests, setup, teardown);

  MPI_Finalize();

  return rc;
}
