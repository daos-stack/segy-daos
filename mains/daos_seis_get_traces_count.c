/*
 * daos_seis_get_traces_count.c
 *
 *  Created on: Mar 7, 2021
 *      Author: omar
 */

#include "api/seismic_graph_api.h"

int
main(int argc, char *argv[])
{
	root_obj_t *root_obj;
	/** string of the pool uuid to connect to */
	char 	   *pool_id;
	/** string of the container uuid to connect to */
	char 	   *container_id;
	/** string of the path of the file that will be read */
	char	   *seis_root_path;
	/** Flag to allow container creation if not found */
	int 	   allow_container_creation = 0;
	int 	   num_of_gathers;
	int 	   num_of_traces;
	int 	   rc;
	int	   i;

	if(argc != 4) {
		warn("you are passing only %d arguments. "
		      "3 arguments are required: pool_id, container_id,"
		      "seismic graph path \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 * sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 * sizeof(char));
	strcpy(container_id, argv[2]);

	size_t seis_root_path_length = strlen(argv[3]);
	seis_root_path = malloc(seis_root_path_length + 1 * sizeof(char));
	strcpy(seis_root_path, argv[3]);

	init_dfs_api(pool_id, container_id, allow_container_creation, 0);

	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);

	num_of_traces = daos_seis_get_num_of_traces(root_obj);
	printf("NUMBER OF TRACES: %d\n", num_of_traces);

	for (i = 0; i < root_obj->num_of_keys; i++) {
		num_of_gathers = daos_seis_get_num_of_gathers(root_obj,
				root_obj->keys[i]);
		printf("Number of gathers under %s object = %d \n",
				root_obj->keys[i], num_of_gathers);
	}
	rc = daos_seis_close_graph(root_obj);

	fini_dfs_api();
	free(pool_id);
	free(container_id);
	free(seis_root_path);

	return 0;
}
