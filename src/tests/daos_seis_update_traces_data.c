/*
 * update_traces_data.c
 *
 *  Created on: Sep 11, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>
char *sdoc[] = {
		"									",
		" Update traces data functionality					",
		" It reads src_shot_id traces data, then copies the data read to 	 ",
		" dest_shot_id traces data.",
		"									",
		" update_traces_data pool=uuid container=uuid svc=r0:r1:r2 in=root_obj_path out=output_file_path shot_id=..					",
		"									",
		" Required parameters:							",
		" pool=				pool uuid to connect		                ",
		" container=			container uuid to connect		        ",
		" svc=				service ranklist of pool seperated by: 		",
		" in_file 			path of the seismic root object.		",
		" src_out_file 			path of the file to which src_shot		",
		"				traces will be written.				",
		" dest_out_file 		path of the file to which dest_shot		",
		"				traces will be written after update.				",
		" src_shot_id			id of the shot to read its traces		",
		" dest_shot_id			id of the shot to update its traces		",
		"									",
		" Optional Parameters:							",
		" verbose=0			=1 for verbose				",
		"									",
		NULL};

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
	/** string of the path of the file src trace will be written to */
	char 			       *src_out_file;
	/** string of the path of the file trace after update will be written to */
	char 			       *dest_out_file;
	/** integer holding shot_id value to read */
	int 				src_shot_id;
	/** integer holding shot_id value to read */
	int 				dest_shot_id;
	/** Flag to allow container creation if not found */
	int		 		allow_container_creation = 0;
	/** Flag to allow verbose output */
	int 				verbose;

	/** Parse input parameters */
	initargs(argc, argv);
	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("src_out",  &src_out_file);
	MUSTGETPARSTRING("dest_out",  &dest_out_file);
	MUSTGETPARINT("src_shot_id",  &src_shot_id);
	MUSTGETPARINT("dest_shot_id",  &dest_shot_id);

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;

//	warn("\n Update traces data \n"
//	     "=======================\n");

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	/** Open seis root object */
	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	/** Read 2 seperate shots traces in 2 linked lists */
	traces_metadata_t *src_traces_metadata = daos_seis_get_shot_traces(src_shot_id, seis_root_object);
	traces_metadata_t *dest_traces_metadata = daos_seis_get_shot_traces(dest_shot_id, seis_root_object);
	/** Open output file to write original traces to */
	FILE *fd = fopen(src_out_file, "w");

	trace_node_t *temp_src = src_traces_metadata->traces_list->head;
	trace_node_t *temp_dst = dest_traces_metadata->traces_list->head;

	/** Fetch traces from linked list and write them to out_file */
	if (temp_src == NULL) {
//		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp_src != NULL){
			memcpy(temp_dst->trace.data, temp_src->trace.data,
			       temp_src->trace.ns * sizeof(float));
			/** convert trace struct back to original segy struct */
			segy *tp = daos_seis_trace_to_segy(&(temp_src->trace));
			/** Write segy struct to file */
			fputtr(fd, tp);
			temp_src = temp_src->next_trace;
			temp_dst = temp_dst->next_trace;
			free(tp);
		}
	}
	gettimeofday(&tv1, NULL);
	/** update traces data */
	daos_seis_set_data(seis_root_object, dest_traces_metadata->traces_list);
	gettimeofday(&tv2, NULL);
	/** read shot 601 after writing shot_id traces in it */
	traces_metadata_t *traces_metadata = daos_seis_get_shot_traces(dest_shot_id, seis_root_object);
	/** Open a new output file to write updated shot_601 traces to */
	fd = fopen(dest_out_file, "w");

	trace_node_t *temp = traces_metadata->traces_list->head;

	if (temp == NULL) {
//		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp != NULL) {
			/** convert trace struct back to original segy struct */
			segy* tp = daos_seis_trace_to_segy(&(temp->trace));
			/** Write segy struct to file */
			fputtr(fd, tp);
			temp = temp->next_trace;
			free(tp);
		}
	}
	/** Release allocated linked list */
	daos_seis_release_traces_metadata(src_traces_metadata);
	daos_seis_release_traces_metadata(dest_traces_metadata);
	daos_seis_release_traces_metadata(traces_metadata);
	/** Close opened root seismic object */
	daos_seis_close_root(seis_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);

	warn("Time taken to update traces data %f \n", time_taken);

	fini_dfs_api();

	return 0;
}


