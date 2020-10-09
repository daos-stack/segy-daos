/*
 * sort_traces.c
 *
 *  Created on: Jul 22, 2020
 *      Author: mirnamoawad
 */

#include <daos_seis.h>

char *sdoc[] = {
		" 								",
		" Sort headers functionality equivalent to Seismic unix	"
		" susort functionality.						",
		" 								",
		" sort_traces pool=uuid container=uuid svc=r0:r1:r2 [[+-]key1 [+-]key2 ...] in=root_obj_path out=output_file_path		",
		" 								",
		" Required parameters:						",
		" pool=			pool uuid to connect		        ",
		" container=		container uuid to connect		",
		" svc=			service ranklist of pool seperated by :	",
		" in	 		path of root seismic object.		",
		" out	 		path of the file that will hold headers ",
		"			and data after sorting.			",
		" 								",
		" Susort supports any number of (secondary) keys with either	",
		" ascending (+, the default) or descending (-) directions for 	",
		" each.  The default sort key is cdp.				",
		" 								",
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
	/** string holding keys that will be used to sort their header values */
	char 			       *keys;
	/** string holding keys that will be used in case of windowing */
	char 			       *w_keys;
	/** string holding min values that will be used in case of windowing */
	char 			       *min;
	/** string holding max values that will be used in case of windowing */
	char 			       *max;
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
	MUSTGETPARSTRING("out",  &out_file);
	MUSTGETPARSTRING("keys",  &keys);
	/** optional parameters*/
	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

	if (!getparstring("window_keys", &w_keys)) {
		w_keys = NULL;
	}
	if (!getparstring("min", &min)) {
		min = NULL;
	}
	if (!getparstring("max", &max)) {
		max = NULL;
	}

//	warn("\n Sort header values \n"
//	     "==================== \n");
	/** Sort keys tokenization */
	char 			temp[4096];
	int 			number_of_keys =0;
	const char 		*sep = ",";
	char 			*token;
	int 			*directions;
	char 		       **sort_keys;
	int 			 i;

	strcpy(temp, keys);
	token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}

	if(number_of_keys == 0){
		number_of_keys = 1;
		directions = malloc(number_of_keys * sizeof(int));
		sort_keys = malloc(number_of_keys * sizeof(char *));
		sort_keys[0] = malloc((strlen("CDP") + 1) * sizeof(char));
		directions[0] = 1;
		strcpy(sort_keys[0], "CDP");
	} else {
		directions = malloc(number_of_keys * sizeof(int));
		sort_keys = malloc(number_of_keys * sizeof(char *));
		strcpy(temp, keys);
		token = strtok(temp, sep);
		i = 0;
		while( token != NULL ) {
			sort_keys[i] = malloc((strlen(token) + 1) *
					      sizeof(char));
			if(token[0]== '-'){
				directions[i]= 0;
				strcpy(sort_keys[i], &token[1]);
			} else if (token[0]== '+'){
				directions[i]=1;
				strcpy(sort_keys[i], &token[1]);
			} else {
				directions[i]=1;
				strcpy(sort_keys[i], token);
			}
			token = strtok(NULL, sep);
			i++;
		}
	}

	/** window keys tokenization in case window keys != NULL */
	char 			min_temp[4096];
	char 			max_temp[4096];
	int 			number_of_window_keys = 0;
	char 		      **window_keys;
	Value 		       *min_keys;
	Value 		       *max_keys;
	cwp_String 	       *type;

	if(w_keys != NULL) {
		tokenize_str((void***)&window_keys,",", w_keys, 0, &number_of_window_keys);
		min_keys = malloc(number_of_window_keys * sizeof(Value));
		max_keys = malloc(number_of_window_keys * sizeof(Value));
		type = malloc(number_of_window_keys * sizeof(cwp_String));
		for(i=0; i<number_of_window_keys; i++) {
			type[i] = hdtype(window_keys[i]);
		}
		strcpy(min_temp,min);
		strcpy(max_temp,max);

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
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	/** Open seis root object */
	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	/** Sort traces headers */
	traces_list_t *trace_list = daos_seis_sort_traces(seis_root_object,
							  number_of_keys,
							  sort_keys, directions,
							  number_of_window_keys,
							  window_keys, type,
							  min_keys, max_keys);
	gettimeofday(&tv2, NULL);
	/** Open output file to write traces to */
	FILE *fd = fopen(out_file, "w");
	int tracl_mod = 1;
	traces_headers_t *temp_trace = trace_list->head;

	/** Fetch traces from linked list and write them to out_file */
	if(temp_trace == NULL) {
		warn("Linked list of traces is empty \n");
		return 0;
	} else {
		while(temp_trace != NULL) {
			/** convert trace struct back to original segy struct */
			segy* tp = daos_seis_trace_to_segy(&(temp_trace->trace));
			tp->tracl = tp->tracr = tracl_mod;
			tracl_mod++;
			/** Write segy struct to file */
			fputtr(fd, tp);
			temp_trace = temp_trace->next_trace;
		}
	}
	/** Free allocated memory */
	for (i = 0; i < number_of_keys; i++) {
		free(sort_keys[i]);
	}
	free(sort_keys);
	free(directions);
	if(number_of_window_keys > 0) {
		for (i = 0; i < number_of_window_keys; i++) {
			free(window_keys[i]);
		}
		free(window_keys);
		free(min_keys);
		free(max_keys);
		free(type);
	}
	/** Release allocated linked list */
	daos_seis_release_traces_list(trace_list);
	/** Close opened root seismic object */
	daos_seis_close_root(seis_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);
	warn("Time taken to sort traces headers %f \n", time_taken);

	fini_dfs_api();

	return 0;
}
