/*
 * daos_seis_range_headers.c
 *
 *  Created on: Mar 7, 2021
 *      Author: omar
 */

#include "api/seismic_graph_api.h"

int
main(int argc, char *argv[])
{
	headers_ranges_t *rng;
	root_obj_t 	 *root_obj;
	char 		 **seismic_objects_keys;
	/** string of the pool uuid to connect to */
	char 		 *pool_id;
	/** string of the container uuid to connect to */
	char 		 *container_id;
	/** string of the path of the file that will be read */
	char 		 *seis_root_path;
	/** string holding keys that will be used in parsing
	 * the file and creating the graph.
	 */
	char 		 *keys;
	/** Flag to allow container creation if not found */
	int 		 allow_container_creation = 0;
	int 		 number_of_keys = 0;
	int 		 rc;
	int 		 k;

	if(argc <= 4 ) {
		warn("you are passing only %d arguments. "
		      " Minimum 3 arguments are required: pool_id, container_id, "
		      "seismic graph path  \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 * sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 * sizeof(char));
	strcpy(container_id, argv[2]);

	size_t seis_root_path_length = strlen(argv[3]);
	seis_root_path = malloc(seis_root_path_length + 1 * sizeof(char));
	strcpy(seis_root_path, argv[3]);

	if (argc >= 5) {
		size_t keys_length = strlen(argv[4]);
		keys = malloc(keys_length + 1 * sizeof(char));
		strcpy(keys, argv[4]);
		rc = tokenize_str((void**) &seismic_objects_keys, ",", keys,
				STRING, &number_of_keys);
	} else {
		seismic_objects_keys = malloc(SEIS_NKEYS * sizeof(char*));
		for (k = 0; k < SEIS_NKEYS; k++) {
			seismic_objects_keys[k] = malloc(
					(strlen(hdr[k].key) + 1)
							* sizeof(char));
			strcpy(seismic_objects_keys[k], hdr[k].key);
		}
	}

	init_dfs_api(pool_id, container_id, allow_container_creation, 0);

	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);

	rng = daos_seis_range(root_obj, number_of_keys, seismic_objects_keys, 1);
	print_headers_ranges(rng);

	rc = daos_seis_close_graph(root_obj);

	fini_dfs_api();
	release_tokenized_array(seismic_objects_keys, STRING, number_of_keys);
	destroy_header_ranges(rng);
	free(pool_id);
	free(container_id);
	free(seis_root_path);

	return 0;
}
