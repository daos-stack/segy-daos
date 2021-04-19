/*
 * daos_seis_parse_additional_file.c
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
	char 	   *segy_file;
	/** string of the path of the file that will be written to */
	char 	   *seis_root_path;
	/** Flag to allow container creation if not found */
	int 	   allow_container_creation = 0;
	int 	   rc;


	if(argc != 5) {
		warn("you are passing only %d arguments. "
		      "5 arguments are required: pool_id, container_id,"
		      "segy file path, seismic graph path \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 * sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 * sizeof(char));
	strcpy(container_id, argv[2]);

	size_t segy_file_path_length = strlen(argv[3]);
	segy_file = malloc(segy_file_path_length + 1 * sizeof(char));
	strcpy(segy_file, argv[3]);

	size_t seis_root_path_length = strlen(argv[4]);
	seis_root_path = malloc(seis_root_path_length + 1 * sizeof(char));
	strcpy(seis_root_path, argv[4]);

	init_dfs_api(pool_id, container_id, allow_container_creation, 0);

	DAOS_FILE *dfs_segy_object = open_dfs_file(segy_file,
						   S_IFREG | S_IWUSR | S_IRUSR,
						   'r', 0);

	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);

	dsg_timer_t *t;
	rc = create_timer(&t);

	rc = start_timer(t);
	rc = daos_seis_parse_segy(dfs_segy_object->file, root_obj);
	rc = end_timer(t, 0);
	printf("elapsed time during parsing extra file > %f \n",
			t->elapsed_time);
	rc = daos_seis_close_graph(root_obj);

	rc = destroy_timer(t);

	fini_dfs_api();

	free(pool_id);
	free(container_id);
	free(segy_file);
	free(seis_root_path);

	return 0;
}

