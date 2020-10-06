/*
 * get_traces_count.c
 *
 *  Created on: Jul 5, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>

char *sdoc[] = {
	        "									",
		" Get traces count functionality equivalent to Seismic unix"
		" sutrcount functionality.						",
		" It prints the total number of traces stored in the seismic root object.",
		"									",
		" get_traces_count pool=uuid container=uuid svc=r0:r1:r2 in=root_obj_path ",
		"									",
		"  Required parameters:							",
		"  pool_id 			the pool uuid to connect to.			",
		"  container_id			the container uuid to connect to.		",
		"  svc_list 			service rank list to connect to.		",
		"  in_file 			path of the seismic root object.		",
		"									",
		"  Optional parameters:							",
		"  verbose 			=1 to allow verbose output.		",
		NULL};

int
main(int argc, char *argv[])
{
	/** string of the pool uuid to connect to */
	char		*pool_id;
	/** string of the container uuid to connect to */
	char 		*container_id;
	/** string of the service rank list to connect to */
	char 		*svc_list;
	/** string of the path of the seismic root object */
	char 		*in_file;
	/** Flag to allow container creation if not found */
	int		allow_container_creation = 0;
	/** Flag to allow verbose output */
	int 		verbose;

	initargs(argc, argv);
   	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}


//	warn("\n Finding traces and gathers count \n"
//	     "========================================== \n");

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	int number_of_traces;
	number_of_traces = daos_seis_get_trace_count(seis_root_object);
	warn("NUMBER OF TRACES = %d \n", number_of_traces);
	int 		no_of_gathers;
	int 		i;
	for(i = 0; i < seis_root_object->num_of_keys; i++) {
		no_of_gathers = daos_seis_get_number_of_gathers(seis_root_object,
								seis_root_object->keys[i]);
		warn("Number of gathers under %s object = %d \n",
		     seis_root_object->keys[i], no_of_gathers);
	}

	daos_seis_close_root(seis_root_object);

	fini_dfs_api();

	return 0;
}
