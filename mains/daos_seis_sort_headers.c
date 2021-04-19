/*
 * daos_seis_sort_headers.c
 *
 *  Created on: Feb 28, 2021
 *      Author: mirnamoawad
 */


#include "api/seismic_graph_api.h"
#ifdef MPI_BUILD
#include <mpi.h>
#endif

int
main(int argc, char *argv[])
{
	/** string of the pool uuid to connect to */
	char		*pool_id;
	/** string of the container uuid to connect to */
	char 		*container_id;
	/** string of the path of the file that will be written */
	char 		*seis_root_path;
	/** string holding keys that will be during sorting */
	char		*sort_keys;
	/** string holding keys that will be used to window their header values */
	char 		*keys;
	/** string holding min values that will be used in windowing */
	char 		*min;
	/** string holding max values that will be used in windowing */
	char 		*max;
	/** Flag to allow container creation if not found */
	int		allow_container_creation = 1;

	int		rc;

	root_obj_t	*root_obj;
	int 		i;
	int 		num_of_wind_keys =0;
	char 		**window_keys;
	char 		**min_values;
	char 		**max_values;
	char 		**sorting_keys;
	int 		*sorting_directions;

	if(argc <= 4) {
		warn("you are passing only %d arguments. "
		      "Minimum 4 arguments are required: pool_id, container_id,"
		      "seismic graph path, array of keys to sort on "
		      " \n", argc-1);
		exit(0);
	}

#ifdef MPI_BUILD
	rc = MPI_Init(&argc, &argv);
#endif
	pool_id = malloc(37 *sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 *sizeof(char));
	strcpy(container_id, argv[2]);

	size_t seis_root_path_length = strlen(argv[3]);
	seis_root_path = malloc(seis_root_path_length +1 *sizeof(char));
	strcpy(seis_root_path, argv[3]);

	size_t sort_keys_length = strlen(argv[4]);
	sort_keys = malloc(sort_keys_length +1 *sizeof(char));
	strcpy(sort_keys, argv[4]);

	size_t keys_length;
	size_t min_length;
	size_t max_length;
	if(argc >= 6) {
		keys_length = strlen(argv[5]);
		keys = malloc(keys_length +1 *sizeof(char));
		strcpy(keys, argv[5]);
		min_length = strlen(argv[6]);
		min = malloc(min_length +1 *sizeof(char));
		strcpy(min, argv[6]);
		max_length = strlen(argv[7]);
		max = malloc(max_length +1 *sizeof(char));
		strcpy(max, argv[7]);
	}

	int	num_of_sorting_keys = 0;
	rc = tokenize_str((void**)&window_keys,",", keys, STRING,
			  &num_of_wind_keys);
	rc = tokenize_str((void**)&min_values,",", min, STRING,
			  &num_of_wind_keys);
	rc = tokenize_str((void**)&max_values,",", max, STRING,
			  &num_of_wind_keys);
	rc = tokenize_str((void**)&sorting_keys,",", sort_keys, STRING,
			  &num_of_sorting_keys);

	sorting_directions = malloc(num_of_sorting_keys * sizeof(int));
	char ** sorting_keys_updated =
			malloc(num_of_sorting_keys * sizeof(char*));
	for(i=0; i < num_of_sorting_keys; i++) {
		sorting_keys_updated[i] =
				malloc(strlen(sorting_keys[i]) * sizeof(char));
		if(sorting_keys[i][0] == '+') {
			sorting_directions[i] = 1;
			strcpy(sorting_keys_updated[i],&sorting_keys[i][1]);
		} else if(sorting_keys[i][0] == '-') {
			sorting_directions[i] = -1;
			strcpy(sorting_keys_updated[i],&sorting_keys[i][1]);
		} else {
			sorting_directions[i] = 1;
			strcpy(sorting_keys_updated[i],sorting_keys[i]);
		}
	}

	generic_value *min_vals =
			malloc(num_of_wind_keys * sizeof(generic_value));
	generic_value *max_vals =
			malloc(num_of_wind_keys * sizeof(generic_value));

	for(i=0; i < num_of_wind_keys; i++) {
		atoval(hdtype(window_keys[i]), min_values[i], &min_vals[i]);
		atoval(hdtype(window_keys[i]), max_values[i], &max_vals[i]);
	}

	init_dfs_api(pool_id,container_id,
		     allow_container_creation, 0);

	root_obj  = daos_seis_open_graph(seis_root_path, O_RDWR);

	ensemble_list	*ensembles_list;
	dsg_timer_t *t;
	rc = create_timer(&t);

	rc = start_timer(t);
	ensembles_list =
		daos_seis_sort_traces(root_obj, sorting_keys_updated,
				      sorting_directions, num_of_sorting_keys,
				      window_keys, num_of_wind_keys,
				      min_vals, max_vals);
	rc = end_timer(t, 0);
	printf("elapsed time during sorting > %f \n", t->elapsed_time);

	rc = destroy_timer(t);


	ensemble_t 	*curr_ensemble;
	curr_ensemble =
		doubly_linked_list_get_object(ensembles_list->ensembles->head,
					      offsetof(ensemble_t, n));
	trace_t* 	curr_trace;
	node_t*		trace_node;
	node_t*		ensemble_node;
	int		j;

	ensemble_node = ensembles_list->ensembles->head;
	for( i=0; i < ensembles_list->ensembles->size; i++) {
		curr_trace =
			doubly_linked_list_get_object(curr_ensemble->traces->head,
						      offsetof(trace_t, n));
		j = 0;
		trace_node = curr_ensemble->traces->head;

		while(j < curr_ensemble->traces->size){
			trace_node = trace_node->next;
			curr_trace =
				doubly_linked_list_get_object(trace_node,
							      offsetof(trace_t, n));
			j++;
		}
		ensemble_node = ensemble_node->next;
		curr_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
	}

	rc = daos_seis_close_graph(root_obj);

	/** no need to close dfs file it's already done while
	 *  releasing segy parameters
	 */
	destroy_ensemble_list(ensembles_list);

	fini_dfs_api();

	release_tokenized_array(window_keys, STRING, num_of_wind_keys);
	release_tokenized_array(min_values, STRING, num_of_wind_keys);
	release_tokenized_array(max_values, STRING, num_of_wind_keys);
	release_tokenized_array(sorting_keys, STRING, num_of_sorting_keys);
	release_tokenized_array(sorting_keys_updated, STRING, num_of_sorting_keys);

	free(sorting_directions);
	free(seis_root_path);
	free(container_id);
	free(sort_keys);
	free(pool_id);
	free(keys);
	free(min);
	free(max);
#ifdef MPI_BUILD
	  MPI_Finalize();
#endif

	return 0;
}

