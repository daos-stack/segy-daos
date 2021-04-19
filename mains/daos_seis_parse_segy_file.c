/*
 * daos_seis_parse_segy_file.c
 *
 *  Created on: Feb 28, 2021
 *      Author: mirnamoawad
 */

#include "api/seismic_graph_api.h"


int
main(int argc, char *argv[])
{
	dfs_obj_t 	*parent = NULL;
	root_obj_t	*root_obj;
	char 		**seismic_objects_keys;
	char 		*seis_root_path;
	/** string of the container uuid to connect to */
	char 		*container_id;
	/** string of the path of the file that will be read */
	char 		*segy_file;
	/** string of the path of the file that will be written */
	char 		*root_name;
	/** string of the pool uuid to connect to */
	char		*pool_id;
	/** string holding keys that will be used in parsing
	 * the file and creating the graph.
	 */
	char 		*keys;
	/** Flag to allow container creation if not found */
	int		allow_container_creation = 1;
	int 		number_of_keys =0;
	int		rc;
	int 		i;

	if(argc != 6) {
		warn("you are passing only %d arguments. "
		      "5 arguments are required: pool_id, container_id,"
		      "keys, segy_file_path, seismic_graph_path \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 *sizeof(char));
	container_id = malloc(37 *sizeof(char));

	strcpy(pool_id, argv[1]);

	strcpy(container_id, argv[2]);

	size_t keys_length = strlen(argv[3]);

	keys = malloc(keys_length +1 *sizeof(char));
	strcpy(keys, argv[3]);

	size_t segy_file_path_length = strlen(argv[4]);

	segy_file = malloc(segy_file_path_length +1 *sizeof(char));
	strcpy(segy_file, argv[4]);

	size_t seis_root_path_length = strlen(argv[5]);

	seis_root_path = malloc(seis_root_path_length +1 *sizeof(char));
	strcpy(seis_root_path, argv[5]);

	rc = tokenize_str((void**)&seismic_objects_keys,",", keys, STRING,
			  &number_of_keys);


	for(i = 0; i < number_of_keys; i++) {
		if(strcmp(seismic_objects_keys[i], "fldr") == 0){
			if(i != 0){
				char 	key_temp[200] = "";
				strcpy(key_temp,seismic_objects_keys[i]);
				strcpy(seismic_objects_keys[i],
				       seismic_objects_keys[0]);
				strcpy(seismic_objects_keys[0],key_temp);
				break;
			} else {
				break;
			}
		}
	}

	root_name = malloc(1024 * sizeof(char));

	parent = get_parent_of_file(seis_root_path, 1, root_name);

	init_dfs_api(pool_id,container_id, allow_container_creation, 0);

	DAOS_FILE *dfs_segy_object = open_dfs_file(segy_file,
					    	   S_IFREG | S_IWUSR | S_IRUSR,
						   'r', 0);

	rc = daos_seis_create_graph(parent, root_name,
				    number_of_keys, seismic_objects_keys,
				    &root_obj, O_RDWR,
				    S_IFDIR | S_IWUSR | S_IRUSR);

	root_obj  = daos_seis_open_graph(seis_root_path, O_RDWR);
	dsg_timer_t *t;
	rc = create_timer(&t);

	rc = start_timer(t);
	rc = daos_seis_parse_segy(dfs_segy_object->file, root_obj);
	rc = end_timer(t, 0);
	printf("elapsed time during parsing > %f \n", t->elapsed_time);
	rc = daos_seis_close_graph(root_obj);

	rc = destroy_timer(t);

	/** no need to close dfs file it's already done while releasing
	 * segy parameters
	 */

	fini_dfs_api();

	release_tokenized_array(seismic_objects_keys, STRING, number_of_keys);
	free(root_name);
	free(segy_file);
	free(seis_root_path);
	free(keys);
	free(pool_id);
	free(container_id);
	return 0;
}
