/*
 * update_traces_data.c
 *
 *  Created on: Sep 11, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>

int
main(int argc, char *argv[])
{
	/** string of the pool uuid to connect to */
	char			       *pool_id;
	/** string of the container uuid to connect to */
	char 			       *container_id;
	/** string of the service rank list to connect to */
	char 			       *svc_list;
	/** string of the path of the file that will be read */
	char 			       *in_file;
	/** string of the path of the file that will be written */
	char 			       *out_file;
	/** integer holding shot_id value to read */
	int 				shot_id;
	/** Flag to allow container creation if not found */
	int		 		allow_container_creation;
	/** Flag to allow verbose output */
	int 				verbose;

	/** Parse input parameters */
	initargs(argc, argv);
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

	warn("\n Update traces data \n"
	     "=======================\n");

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	/** Open seis root object */
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	/** Read 2 seperate shots traces in 2 linked lists */
	traces_list_t *src_trace_list = daos_seis_get_shot_traces(shot_id, segy_root_object);
	traces_list_t *dst_trace_list = daos_seis_get_shot_traces( 601, segy_root_object);
	/** Open output file to write original traces to */
	FILE *fd = fopen(out_file, "w");

	traces_headers_t *temp_src = src_trace_list->head;
	traces_headers_t *temp_dst = dst_trace_list->head;

	/** Fetch traces from linked list and write them to out_file */
	if (temp_src == NULL) {
		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp_src != NULL){
			memcpy(temp_dst->trace.data, temp_src->trace.data,
			       temp_src->trace.ns * sizeof(float));
			/** convert trace struct back to original segy struct */
			segy *tp = trace_to_segy(&(temp_src->trace));
			/** Write segy struct to file */
			fputtr(fd, tp);
			temp_src = temp_src->next_trace;
			temp_dst = temp_dst->next_trace;
		}
	}
	gettimeofday(&tv1, NULL);
	/** update traces data */
	daos_seis_set_data(segy_root_object, dst_trace_list);
	gettimeofday(&tv2, NULL);
	/** read shot 601 after writing shot_id traces in it */
	traces_list_t *trace_list = daos_seis_get_shot_traces(601, segy_root_object);
	/** Open a new output file to write updated shot_601 traces to */
	fd = fopen("shot_601.su", "w");

	traces_headers_t *temp = trace_list->head;

	if (temp == NULL) {
		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp != NULL) {
			/** convert trace struct back to original segy struct */
			segy* tp = trace_to_segy(&(temp->trace));
			/** Write segy struct to file */
			fputtr(fd, tp);
			temp = temp->next_trace;
		}
	}
	/** Release allocated linked list */
	release_traces_list(src_trace_list);
	release_traces_list(dst_trace_list);
	release_traces_list(trace_list);
	/** Close opened root seismic object */
	daos_seis_close_root(segy_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);

	warn("Time taken to update traces data %f \n", time_taken);

	fini_dfs_api();

	return 0;
}


