/*
 * add_headers.c
 *
 *  Created on: Sep 23, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>

char *sdoc[] = {
	        "									",
		" Add headers functionality equivalent to Seismic unix			",
		" addhead functionality.						",
		" It builds the seismic graph by creating root object and		",
		" seismic gather objects then parses bare traces data file, creates 	",
		" trace header(if no header file is passed) for each trace and dumps the",
		" traces headers and data to the created graph.				",
		"									",
		" add_headers pool=uuid container=uuid svc=r0:r1:r2 in=input_file_path out=output_file_path keys=key1,key2,.. 			",
		"									",
		"  Required parameters:							",
		"  pool_id 			the pool uuid to connect to.			",
		"  container_id			the container uuid to connect to.		",
		"  svc_list 			service rank list to connect to.		",
		"  in	 			path of the bare trace file that will be parsed.",
		"  out	 			path of the seismic root object that		",
		"  				will be created.				",
		"  keys				array of keys that will be used in linking	",
		"				traces to seismic gather objects.		",
		"  ns				number of samples per trace to be read.		",
		"  Optional parameters:							",
		"  verbose 			=1 to allow verbose output.		",
		"  header_file			path of file holding traces headers	",
		"  ftn=0				Fortran flag				",
		" 				0 = data written unformatted from C	",
		" 				1 = data written unformatted from Fortran ",
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
	/** string of the path of the file that will be read */
	char 		*out_file;
	/** string of the path of the file that will be written */
	char 		*header_file;
	/** Flag to allow container creation if not found */
	int		allow_container_creation;
	/** Flag to allow verbose output */
	int 		verbose;
	/** string holding keys that will be used in parsing
	 * the file and creating the graph.
	 */
	char 		*keys;

	int		ns;
	int 		ftn;

	/** Parse input parameters */
	initargs(argc, argv);
	requestdoc(1);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("out",  &out_file);
	MUSTGETPARINT("ns", &ns);
	MUSTGETPARSTRING("keys",  &keys);

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}

	if (!getparint("Fortran", &ftn)) {
		ftn = 0;
	}

	if (!getparstring("header_file", &header_file)) {
		header_file = NULL;
	}
	/** keys tokenization */
	char 			temp[4096];
	char			*temp_keys = malloc((strlen(keys) +1) * sizeof(char));
	int 			number_of_keys =0;
	const char 		*sep = ",";
	char 			*token;
	char 		        **header_keys;

	strcpy(temp_keys, keys);
	strcpy(temp, keys);
	token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}
	header_keys = malloc(number_of_keys * sizeof(char*));
	tokenize_str((void**) header_keys,",", keys, 0);

	int 			i;
	/** integer flag, it is set to check if fldr key exists or not */
	int 			fldr_exist = 0;

	for(i = 0; i < number_of_keys; i++) {
		/** Check if fldr exists
		 * if yes, it should be the first key in the array of keys.
		 */
		if(strcmp(header_keys[i], "fldr") == 0){
			fldr_exist = 1;
			if(i != 0){
				char 		key_temp[200] = "";
				strcpy(key_temp,header_keys[i]);
				strcpy(header_keys[i],header_keys[0]);
				strcpy(header_keys[0],key_temp);
				break;
			} else {
				break;
			}
		}
	}

	char 		**updated_keys;
	if(fldr_exist == 0) {
		char *old_keys = malloc((strlen(temp_keys) + 1) * sizeof(char));
		char *key_buffer = malloc((strlen(temp_keys) + 6) * sizeof(char));
		number_of_keys ++;
		strcpy(old_keys, temp_keys);
		strcpy(key_buffer, "fldr,");
		strcat(key_buffer,temp_keys);
		updated_keys = malloc(number_of_keys * sizeof(char*));
		tokenize_str((void**)updated_keys,",", key_buffer, 0);
		free(old_keys);
		free(key_buffer);
		for(i = 0; i<  number_of_keys - 1; i++){
			free(header_keys[i]);
		}
		free(header_keys);
	}

//	warn("\n PARSING RAW DATA \n"
//	     "================= \n");

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation,
		     verbose);
	DAOS_FILE *segyfile = open_dfs_file(in_file,
					    S_IFREG | S_IWUSR | S_IRUSR,
					    'r', 0);
	DAOS_FILE *head_file;
	if(header_file != NULL) {
		head_file = open_dfs_file(header_file,
					  S_IFREG | S_IWUSR | S_IRUSR,
					  'r', 0);
	}
	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;
	char 			*file_name;
	dfs_obj_t 		*parent = NULL;
	file_name = malloc(1024 * sizeof(char));

	parent = dfs_get_parent_of_file(get_dfs(), out_file, 1, file_name, 1);

	seis_root_obj_t 	*root_obj;
	seis_obj_t 		**seismic_obj;

	seismic_obj = malloc(number_of_keys * sizeof(seis_obj_t*));

	if(fldr_exist == 1){
		daos_seis_create_graph(get_dfs(), parent, file_name, number_of_keys,
				       header_keys, &root_obj, seismic_obj);
		gettimeofday(&tv1, NULL);
		if(header_file == NULL) {
			daos_seis_parse_raw_data (get_dfs(), root_obj,
					       	  seismic_obj, segyfile->file,
						  NULL, ns, ftn);
		} else {
			daos_seis_parse_raw_data (get_dfs(), root_obj,
					       seismic_obj, segyfile->file,
					       head_file->file, ns, ftn);
		}
		gettimeofday(&tv2, NULL);
		/** Free the allocated array of keys */
		for(i = 0; i<  number_of_keys; i++){
			free(header_keys[i]);
		}
		free(header_keys);
	} else {
		daos_seis_create_graph(get_dfs(), parent, file_name, number_of_keys,
				       updated_keys, &root_obj, seismic_obj);
		gettimeofday(&tv1, NULL);
		if(header_file == NULL) {
			daos_seis_parse_raw_data (get_dfs(), root_obj,
						  seismic_obj, segyfile->file,
						  NULL, ns, ftn);
		} else {
			daos_seis_parse_raw_data (get_dfs(), root_obj,
					       seismic_obj, segyfile->file,
					       head_file->file, ns, ftn);
		}
		gettimeofday(&tv2, NULL);
		/** Free the allocated array of keys */
		for(i = 0; i<  number_of_keys; i++){
			free(updated_keys[i]);
		}
		free(updated_keys);
	}

	/** Close opened segy file */
	close_dfs_file(segyfile);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);

	warn("Time taken to parse one file %f \n", time_taken);

	fini_dfs_api();

	return 0;
}


