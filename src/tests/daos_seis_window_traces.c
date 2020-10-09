/*
 * window_traces.c
 *
 *  Created on: Jul 26, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>

char *sdoc[] = {
		"									",
		" Window traces functionality equivalent to seismic unix		",
		" suwind functionality. 						",
		"									",
		" window_traces pool=uuid container=uuid svc=r0:r1:r2 in=root_obj_path out=output_file_path keys=.. min=.. max=..					",
		"									",
		" Required parameters:							",
		" pool=				pool uuid to connect		                ",
		" container=			container uuid to connect		        ",
		" svc=				service ranklist of pool seperated by: 		",
		" key=				Key header word to window on 			",
		" in_file 			path of the seismic root object.		",
		" out_file 			path of the file to which			",
		"				headers will be written 			",
		" min=				min value of key header word to pass		",
		" max=				max value of key header word to pass		",
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
	/** string of the path of the file that will be written */
	char 			       *out_file;
	/** string holding keys that will be used to window their header values */
	char 			       *keys;
	/** string holding min values that will be used in windowing */
	char 			       *min;
	/** string holding max values that will be used in windowing */
	char 			       *max;
	/** Flag to allow container creation if not found */
	int		 		allow_container_creation =0;
	/** Flag to allow verbose output */
	int 				verbose;

	/** Parse input parameters */
	initargs(argc, argv);
	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("out",  &out_file);
	MUSTGETPARSTRING("keys",  &keys);
	MUSTGETPARSTRING("min",  &min);
	MUSTGETPARSTRING("max",  &max);

	/** optional parameters*/
	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

//	warn("\n Window headers \n"
//	     "==================== \n");
	/** Window keys tokenization */
	char 			temp[4096];
	char 			min_temp[4096];
	char 			max_temp[4096];
	int 			number_of_keys = 0;
	const char 		*sep = ",";
	char 			*token;
	char 		       **window_keys;
	int 			 i;

	strcpy(min_temp,min);
	strcpy(max_temp,max);
	tokenize_str((void***)&window_keys,",", keys, 0, &number_of_keys);

	Value 			min_keys[number_of_keys];
	Value 			max_keys[number_of_keys];
	cwp_String		type[number_of_keys];

	for(i=0; i<number_of_keys; i++) {
		type[i] = hdtype(window_keys[i]);
	}
	char *min_token = strtok(min_temp, sep);
	i = 0;
	while(min_token != NULL){
		atoval(type[i], min_token, &min_keys[i]);
		min_token = strtok(NULL,sep);
		i++;
	}
	char *max_token = strtok(max_temp, sep);
	i = 0;
	while(max_token != NULL){
		atoval(type[i], max_token, &max_keys[i]);
		max_token = strtok(NULL,sep);
		i++;
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation,
		     verbose);
	/** Open seis root object */
	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),
								     in_file);
	gettimeofday(&tv1, NULL);
	/** Window traces headers */
	traces_metadata_t *traces_metadata = daos_seis_wind_traces(seis_root_object,
							  	   window_keys,
								   number_of_keys,
								   min_keys,
								   max_keys,
								   type);
	gettimeofday(&tv2, NULL);
	/** Open output file to write traces to */
	FILE *fd = fopen(out_file, "w");

	trace_node_t *temp_trace = traces_metadata->traces_list->head;
	/** Fetch traces from linked list and write them to out_file */
	if(temp_trace == NULL) {
		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp_trace != NULL){
		/** convert trace struct back to original segy struct */
	    	segy* tp = daos_seis_trace_to_segy(&(temp_trace->trace));
		/** Write segy struct to file */
	    	fputtr(fd, tp);
	    	temp_trace = temp_trace->next_trace;
		}
	}
	/** Free allocated memory */
	for (i = 0; i < number_of_keys; i++) {
		free(window_keys[i]);
	}
	free(window_keys);
	/** Release allocated linked list */
	daos_seis_release_traces_metadata(traces_metadata);
	/** Close opened root seismic object */
	daos_seis_close_root(seis_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);
	warn("Time taken to window traces headers %f \n", time_taken);

	fini_dfs_api();

	return 0;
}
