/*
 * read_traces.c
 *
 *  Created on: Jul 5, 2020
 *      Author: mirnamoawad
 */

#include <daos_seis.h>
char *sdoc[] = {
	        "									",
		" Read shot traces functionality					",
		" It reads one shot traces 						",
		"									",
		" read_traces pool=uuid container=uuid svc=r0:r1:r2 in=input_file_path out=output_file_path shot_id= ",
		"									",
		"  Required parameters:							",
		"  pool_id 		the pool uuid to connect to.			",
		"  container_id 	the container uuid to connect to.		",
		"  svc_list 		service rank list to connect to.		",
		"  in_file 		path of the seismic root object.		",
		"  out_file 		path of the file to which traces will be written",
		"  shot_id		id of the shot to fetch its traces		",
		"									",
		"  Optional parameters:							",
		"  verbose 			=1 to allow verbose output.		",
		"  allow_container_creation 	flag to allow creation of container if	",
		"				its not found.				",
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
	/** string of the path of the file that will be read */
	char 		*in_file;
	/** string of the path of the file that will be written */
	char 		*out_file;
	/** Flag to allow container creation if not found */
	int		allow_container_creation;
	/** Flag to allow verbose output */
	int 		verbose;
	/** Shot id to read its traces */
	int		shot_id;

	/** Parse input parameters */
	initargs(argc, argv);
   	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("out",  &out_file);
	MUSTGETPARINT("shot_id",  &shot_id);

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;

//	warn("\n Read shot (%d) traces \n"
//	     "=======================\n", shot_id);

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	/** Open seis root object */
	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),
								     in_file);

	gettimeofday(&tv1, NULL);
	/** Read shot traces */
	traces_list_t *src_trace_list = daos_seis_get_shot_traces(shot_id, seis_root_object);
	/** Open output file to write traces to */
	FILE *fd = fopen(out_file, "w");

	traces_headers_t *temp_src = src_trace_list->head;

	/** Fetch traces from linked list and write them to out_file */
	if (temp_src == NULL) {
		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp_src != NULL){
			/** convert trace struct back to original segy struct */
			segy* tp = trace_to_segy(&(temp_src->trace));
			/** Write segy struct to file */
			fputtr(fd, tp);
			temp_src = temp_src->next_trace;
		}
	}
	gettimeofday(&tv2, NULL);

	/** Release allocated linked list */
	daos_seis_release_traces_list(src_trace_list);
	/** Close opened root seismic object */
	daos_seis_close_root(seis_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);

	warn("Time taken to read one shot %f \n", time_taken);

	fini_dfs_api();

	return 0;
}
