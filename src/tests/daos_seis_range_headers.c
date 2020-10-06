/*
 * range_headers.c
 *
 *  Created on: Aug 27, 2020
 *      Author: mirnamoawad
 */

#include <daos_seis.h>

char *sdoc[] = {
		" 								",
		" Range headers functionality equivalent to Seismic unix"
		" surange functionality.					",
		"								",
		" range_headers pool=uuid container=uuid svc=r0:r1:r2 in=input_file_path ",
		"								",
		" Required parameters:						",
		" pool=				pool uuid to connect		        ",
		" container=			container uuid to connect		",
		" svc=				service ranklist of pool seperated by :	",
		" in_file 			path of the seismic root object.	",
		"								",
		" Optional parameters:						",
		" key=				Header key(s) to range (default=all)",
		" dim=0				dim seismic flag		",
		"	    			0 = not dim, 1 = coord in ft, 2 = coord in m",
		" verbose 			=1 to allow verbose output.	",
		" 								",
		" 								",
		" Output is: 							",
		" number of traces 						",
		" keyword min max (first - last) 				",
		" north-south-east-west limits of shot, receiver and midpoint   ",
		" if dim then also midpoint interval and line length   		",
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
	/** string holding keys that will be used to range headers */
	char 			       *keys;
	/** Flag to allow container creation if not found */
	int		 		allow_container_creation = 0;
	/** Flag to allow verbose output */
	int 				verbose;
	/** dim line with coords in ft (1) or m (2) */
	int 				dim;

	/** Parse input parameters */
	initargs(argc, argv);
   	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	/** optional parameters*/
	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}
	if(!getparstring("keys",  &keys)) {
		keys = NULL;
	}
	if (!getparint("dim", &dim)) {
		dim = 0;
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;

//	warn("\n Range header values \n"
//	     "==================== \n");
	/** keys tokenization */
	char 			temp[4096];
	int 			number_of_keys =0;
	const char 		*sep = ",";
	char 			*token;
	char 		       **range_keys;

	if(keys != NULL) {
		strcpy(temp, keys);
		token = strtok(temp, sep);
		while( token != NULL ) {
			number_of_keys++;
			token = strtok(NULL, sep);
		}
	} else {
		number_of_keys = SU_NKEYS;
	}

	int 			k;
	if(keys != NULL) {
		tokenize_str((void***)&range_keys,",", keys, 0, &number_of_keys);
	} else {
		range_keys = malloc(SU_NKEYS * sizeof(char*));
		for(k = 0; k < SU_NKEYS; k++) {
			range_keys[k]= malloc((strlen(hdr[k].key)+1) *
					      sizeof(char));
			strcpy(range_keys[k], hdr[k].key);
		}
	}
	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	/** Open seis root object */
	seis_root_obj_t *seis_root_object = daos_seis_open_root_path(get_dfs(),in_file);
	gettimeofday(&tv1, NULL);
	/** Range headers */
	headers_ranges_t ranges = daos_seis_range_headers(seis_root_object, number_of_keys, range_keys, dim);
	gettimeofday(&tv2, NULL);
	/** Print headers ranges */
	print_headers_ranges(ranges);
	/** Free allocated memory */
	for(k = 0; k < number_of_keys; k++) {
		free(range_keys[k]);
	}
	free(range_keys);
	/** Close opened root seismic object */
	daos_seis_close_root(seis_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);
	warn("Time taken to range traces headers %f \n", time_taken);

	fini_dfs_api();

	return 0;
}

