/*
 * daos_seis_add_headers.c
 *
 *  Created on: Mar 8, 2021
 *      Author: omar
 */

#include "api/seismic_graph_api.h"

int
main(int argc, char *argv[])
{
	root_obj_t *root_obj;
	dfs_obj_t  *parent = NULL;
	char 	   **seismic_objects_keys;
	char	   *seis_root_path;
	char 	   *container_id;
	char 	   *header_file;
	char 	   *root_name;
	char 	   *raw_file;
	char 	   *pool_id;
	char 	   *keys;
	int 	   allow_container_creation = 1;
	int 	   number_of_keys = 0;
	int 	   fortran;
	int 	   rc;
	int 	   ns;
	int 	   i;

	if(argc <= 7) {
		warn("you are passing only %d arguments. "
		      "Minimum 6 arguments are required: pool_id, container_id,"
		      " array_of_keys, raw file path, seismic root path,"
		      " number of samples \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 * sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 * sizeof(char));
	strcpy(container_id, argv[2]);

	size_t keys_length = strlen(argv[3]);
	keys = malloc(keys_length + 1 * sizeof(char));
	strcpy(keys, argv[3]);

	size_t raw_file_path_length = strlen(argv[4]);
	raw_file = malloc(raw_file_path_length + 1 * sizeof(char));
	strcpy(raw_file, argv[4]);

	size_t seis_root_path_length = strlen(argv[5]);
	seis_root_path = malloc(seis_root_path_length + 1 * sizeof(char));
	strcpy(seis_root_path, argv[5]);

	rc = tokenize_str((void**) &seismic_objects_keys, ",", keys, STRING,
			  &number_of_keys);

	ns = atoi(argv[6]);

	if (argc >= 8) {
		fortran = atoi(argv[7]);
	} else {
		fortran = 0;
	}

	if (argc >= 9) {
		size_t hdr_file_path_length = strlen(argv[8]);
		header_file = malloc(hdr_file_path_length + 1 * sizeof(char));
		strcpy(header_file, argv[8]);
	} else {
		header_file = NULL;
	}

	for (i = 0; i < number_of_keys; i++) {
		if (strcmp(seismic_objects_keys[i], "fldr") == 0) {
			if (i != 0) {
				char key_temp[200] = "";
				strcpy(key_temp, seismic_objects_keys[i]);
				strcpy(seismic_objects_keys[i],
						seismic_objects_keys[0]);
				strcpy(seismic_objects_keys[0], key_temp);
				break;
			} else {
				break;
			}
		}
	}

	root_name = malloc(1024 * sizeof(char));

	parent = get_parent_of_file(seis_root_path, 1, root_name);

	init_dfs_api(pool_id, container_id, allow_container_creation,
			0);

	DAOS_FILE *dfs_raw_object = open_dfs_file(raw_file,
			S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);

	DAOS_FILE *dfs_hdr_object;
	if (header_file != NULL) {
		dfs_hdr_object = open_dfs_file(header_file,
				S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
	}

	rc = daos_seis_create_graph(parent, root_name, number_of_keys,
			seismic_objects_keys, &root_obj, O_RDWR,
			S_IFDIR | S_IWUSR | S_IRUSR);

	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);
	dsg_timer_t *t;
	rc = create_timer(&t);

	rc = start_timer(t);
	if (header_file != NULL) {
		rc = daos_seis_parse_raw_data(dfs_raw_object->file,
				dfs_hdr_object->file, root_obj, ns, fortran);
	} else {
		rc = daos_seis_parse_raw_data(dfs_raw_object->file, NULL,
				root_obj, ns, fortran);
	}
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
	free(raw_file);
	free(header_file);
	free(seis_root_path);
	free(keys);
	free(pool_id);
	free(container_id);

	return 0;
}
