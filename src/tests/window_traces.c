/*
 * window_traces.c
 *
 *  Created on: Jul 26, 2020
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
	/** string holding keys that will be used to window their header values */
	char 			       *keys;
	/** string holding min values that will be used in windowing */
	char 			       *min;
	/** string holding max values that will be used in windowing */
	char 			       *max;
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
	MUSTGETPARSTRING("keys",  &keys);
	MUSTGETPARSTRING("min",  &min);
	MUSTGETPARSTRING("max",  &max);

	/** optional parameters*/
	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}
	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}

	warn("\n Window headers \n"
	     "==================== \n");
	/** Window keys tokenization */
	char 			temp[4096];
	char 			min_temp[4096];
	char 			max_temp[4096];
	int 			number_of_keys =0;
	const char 		*sep = ",";
	char 			*token;
	char 		       **window_keys;
	int 			 i;

	strcpy(temp, keys);
	token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}
	window_keys = malloc(number_of_keys * sizeof(char*));

	Value 			min_keys[number_of_keys];
	Value 			max_keys[number_of_keys];
	cwp_String		type[number_of_keys];


	i = 0;

	strcpy(temp,keys);
	strcpy(min_temp,min);
	strcpy(max_temp,max);
	token =strtok(temp,sep);
	while(token != NULL){
		window_keys[i] = malloc((strlen(token) + 1)*sizeof(char));
		strcpy(window_keys[i], token);
		type[i] = hdtype(window_keys[i]);
		token = strtok(NULL,sep);
		i++;
	}
	char *min_token =strtok(min_temp, sep);
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
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),
								     in_file);
	gettimeofday(&tv1, NULL);
	/** Window traces headers */
	traces_list_t *trace_list = daos_seis_wind_traces(segy_root_object,
							  window_keys,
							  number_of_keys,
							  min_keys, max_keys,
							  type);
	gettimeofday(&tv2, NULL);
	/** Open output file to write traces to */
	FILE *fd = fopen(out_file, "w");

	traces_headers_t *temp_trace = trace_list->head;
	/** Fetch traces from linked list and write them to out_file */
	if(temp_trace == NULL) {
		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp_trace != NULL){
		/** convert trace struct back to original segy struct */
	    	segy* tp = trace_to_segy(&(temp_trace->trace));
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
	release_traces_list(trace_list);
	/** Close opened root seismic object */
	daos_seis_close_root(segy_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);
	warn("Time taken to window traces headers %f \n", time_taken);

	fini_dfs_api();

	return 0;
}
