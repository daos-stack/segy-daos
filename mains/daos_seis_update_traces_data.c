/*
 * daos_seis_update_traces_data.c
 *
 *  Created on: Mar 8, 2021
 *      Author: omar
 */

#include "api/seismic_graph_api.h"

int
main(int argc, char *argv[])
{
	ensemble_list *src_shot_traces;
	ensemble_list *dest_shot_traces;
	root_obj_t    *root_obj;
	/** string of the container uuid to connect to */
	char 	      *container_id;
	/** string of the path of the file that will be read */
	char 	      *seis_root_path;
	/** string of the pool uuid to connect to */
	char 	      *pool_id;
	int 	      allow_container_creation = 0;
	int 	      dest_shotid;
	int	      src_shotid;
	int 	      rc;

	if(argc != 6) {
		warn("you are passing only %d arguments. "
		      "5 arguments are required: pool_id, container_id,"
		      "seismic graph path, source shot id, "
		      "destination shot id"
		      " \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 * sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 * sizeof(char));
	strcpy(container_id, argv[2]);

	size_t seis_root_path_length = strlen(argv[3]);
	seis_root_path = malloc(seis_root_path_length + 1 * sizeof(char));
	strcpy(seis_root_path, argv[3]);

	src_shotid = atoi(argv[4]);

	dest_shotid = atoi(argv[5]);

	init_dfs_api(pool_id, container_id, allow_container_creation,0);
	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);

	src_shot_traces = daos_seis_get_shot_traces(root_obj, src_shotid);
	dest_shot_traces = daos_seis_get_shot_traces(root_obj, dest_shotid);

	ensemble_t *e_src = doubly_linked_list_get_object(
			src_shot_traces->ensembles->head,
			offsetof(ensemble_t, n));
	ensemble_t *e_dest = doubly_linked_list_get_object(
			dest_shot_traces->ensembles->head,
			offsetof(ensemble_t, n));

	trace_t *t_src, *t_dest;
	node_t *src_temp = e_src->traces->head;
	node_t *dest_temp = e_dest->traces->head;

	while (src_temp != NULL) {
		t_src = doubly_linked_list_get_object(src_temp,
				offsetof(trace_t, n));
		t_dest = doubly_linked_list_get_object(dest_temp,
				offsetof(trace_t, n));
		memcpy(t_dest->data, t_src->data, t_src->ns * sizeof(float));
		src_temp = src_temp->next;
		dest_temp = dest_temp->next;
	}

	daos_seis_set_traces_data(root_obj, dest_shot_traces);

	dest_shot_traces = daos_seis_get_shot_traces(root_obj, dest_shotid);

	rc = daos_seis_close_graph(root_obj);

	fini_dfs_api();
	destroy_ensemble_list(src_shot_traces);
	destroy_ensemble_list(dest_shot_traces);
	free(pool_id);
	free(container_id);
	free(seis_root_path);

	return 0;
}
