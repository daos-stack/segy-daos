/*
 * get_headers.c
 *
 *  Created on: Aug 30, 2020
 *      Author: mirnamoawad
 */

#include <daos_seis.h>
char *sdoc[] = {
	        "									",
		" Get headers functionality equivalent to seismic unix"
		" sugethw functionality.							",
		" It returns traces headers values and writes them to a file"
		" or prints them to the terminal.  					",
		"									",
		" get_headers pool=uuid container=uuid svc=r0:r1:r2 in=input_file_path keys=...",
		"									",
		"  Required parameters:							",
		"  pool_id 		the pool uuid to connect to.			",
		"  container_id 	the container uuid to connect to.		",
		"  svc_list 		service rank list to connect to.		",
		"  in_file 		path of the seismic root object.		",
		"  keys			array of keys to fetch their header values.	",
		"									",
		"  Optional parameters:							",
		"  verbose 			=1 to allow verbose output.		",
		"  allow_container_creation 	flag to allow creation of container if",
		"				its not found.				",
		"  out_file 			path of the file to which		"
		"				headers will be written 		",
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
	/** string holding keys that will be used to return their header values */
	char 			       *keys;
	/** Flag to allow container creation if not found */
	int		 		allow_container_creation;
	/** Flag to allow verbose output */
	int 				verbose;
	int 				ascii = 1;

	/** Parse input parameters */
	initargs(argc, argv);
	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("keys",  &keys);

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}
	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}
	if(!getparstring("out", &out_file)) {
		out_file = NULL;
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;

//	warn("\n Get header values \n"
//	     "==================== \n");
	/** keys tokenization */
	char 			temp[4096];
	int 			number_of_keys =0;
	const char 		*sep = ",";
	char 			*token;
	char 		       **header_keys;

	strcpy(temp, keys);
	token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}

	header_keys = malloc(number_of_keys * sizeof(char*));
	tokenize_str((void**)header_keys,",", keys, 0);

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation,
		     verbose);
	/** Open seis root object */
	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	/** Get header values */
	traces_list_t *trace_list = daos_seis_get_headers( seis_root_object);
	gettimeofday(&tv2, NULL);

	traces_headers_t *temp_trace = trace_list->head;

	Value 			val;
	int 			i;

	if((ascii == 1) && (out_file == NULL)) {
		/** Fetch traces from linked list and write them to out_file */
		if(temp_trace == NULL) {
			("Linked list of traces is empty \n");
			return 0;
		} else {
			while(temp_trace != NULL){
				for(i = 0; i < number_of_keys; i++) {
					get_header_value(temp_trace->trace,
							 header_keys[i], &val);
					printf("%6s=", header_keys[i]);
					printfval(hdtype(header_keys[i]), val);
					putchar('\t');
				}
				printf("\n\n");
				temp_trace = temp_trace->next_trace;
			}
		}
	} else {
		/** Open output file to write traces to */
		FILE *fd = fopen(out_file, "w");
//		printf("Write headers in out file \n");
		if(temp_trace == NULL) {
			warn("Linked list of traces is empty \n");
			return 0;
		} else {
			while(temp_trace != NULL){
				for(i = 0; i < number_of_keys; i++) {
					/** Get specific key header value */
					get_header_value(temp_trace->trace,
							 header_keys[i], &val);
					char *buffer = malloc(512 * sizeof(char));
					sprintf(buffer, "%6s=", header_keys[i]);
					efwrite(buffer, strlen(buffer), 1, fd);
					val_sprintf(buffer, val, header_keys[i]);
					efwrite(buffer, strlen(buffer), 1, fd);
					putc('\t',fd);
					free(buffer);
				}
				putc('\n',fd);
				putc('\n',fd);
				temp_trace = temp_trace->next_trace;
			}
		}
	}
	/** Free allocated memory */
	for(i = 0; i < number_of_keys; i++) {
		free(header_keys[i]);
	}
	free(header_keys);
	/** Release allocated linked list */
	daos_seis_release_traces_list(trace_list);
	/** Close opened root seismic object */
	daos_seis_close_root(seis_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);
	warn("Time taken to get traces headers %f \n", time_taken);

	fini_dfs_api();

	return 0;
}
