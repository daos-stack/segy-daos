/*
 * seismic_obj_creation.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>

char *sdoc[] = {
	        "									",
		" Seismic object creation functionality equivalent to Seismic unix"
		" segyread functionality.						",
		" It builds the seismic graph by creating root object and		",
		" seismic gather objects then parses the segy file and dumps the	",
		" file headers, traces headers and data to the created graph.		",
		"									",
		"									",
		" seismic_obj_creation pool=uuid container=uuid svc=r0:r1:r2 in=input_file_path out=output_file_path keys=key1,key2,.. ",
		"									",
		"  Required parameters:							",
		"  pool_id			the pool uuid to connect to.			",
		"  container_id			the container uuid to connect to.		",
		"  svc_list			service rank list to connect to.		",
		"  in_file			path of the segy file that will be parsed.	",
		"  out_file			path of the seismic root object that		",
		"  				will be created.				",
		"  keys				array of keys that will be used in linking	",
		"				traces to seismic gather objects.		",
		"  Optional parameters:							",
		"  verbose			=1 to allow verbose output.			",
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
	/** string holding keys that will be used in parsing
	 * the file and creating the graph.
	 */
	char 		*keys;
	/** Flag to allow container creation if not found */
	int		allow_container_creation;
	/** Flag to allow verbose output */
	int 		verbose;

	/** Parse input parameters */
	initargs(argc, argv);
	requestdoc(0);

	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("out",  &out_file);
	MUSTGETPARSTRING("keys",  &keys);

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}

	/** keys tokenization */
	char			*temp_keys = malloc((strlen(keys) +1) * sizeof(char));
	int 			number_of_keys =0;
	char 		        **header_keys;

	strcpy(temp_keys, keys);
	tokenize_str((void***)&header_keys,",", keys, 0, &number_of_keys);

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
		tokenize_str((void***)&updated_keys,",", key_buffer, 0, &number_of_keys);
		free(old_keys);
		free(key_buffer);
		for(i = 0; i<  number_of_keys - 1; i++){
			free(header_keys[i]);
		}
		free(header_keys);
	}
//	warn("\n PARSING SEGY FILE \n"
//	     "================= \n");

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation,
		     verbose);
	DAOS_FILE *segyfile = open_dfs_file(in_file,
					    S_IFREG | S_IWUSR | S_IRUSR,
					    'r', 0);

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;
	char 			*file_name;
	dfs_obj_t 		*parent = NULL;

	file_name = malloc(1024 * sizeof(char));

	parent = dfs_get_parent_of_file(get_dfs(), out_file, 1,
					file_name, 1);

	seis_root_obj_t 	*root_obj;
	seis_obj_t 		**seismic_obj;

	seismic_obj = malloc(number_of_keys * sizeof(seis_obj_t*));

	if(fldr_exist == 1){
		daos_seis_create_graph(get_dfs(), parent, file_name,
				       number_of_keys, header_keys,
				       &root_obj, seismic_obj);
		gettimeofday(&tv1, NULL);
		daos_seis_parse_segy(get_dfs(), segyfile->file,
				     root_obj, seismic_obj, 0);
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
		daos_seis_parse_segy(get_dfs(), segyfile->file,
				     root_obj, seismic_obj, 0);
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

	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), out_file);
	DAOS_FILE *daos_binary;
	char *bfile;		/* name of binary header file	*/
	bfile = "daos_seis_binary";
	daos_binary = open_dfs_file(bfile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
	bhed *binary_header = daos_seis_get_binary_header(segy_root_object);
	write_dfs_file(daos_binary, (char *) binary_header, BNYBYTES);
	close_dfs_file(daos_binary);
	free(binary_header);

	DAOS_FILE *daos_text_header;
	char *tfile;		/* name of text header file	*/
	int rc;
	tfile = "daos_seis_text_header";
	daos_text_header = open_dfs_file(tfile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
	char *text_header = daos_seis_get_text_header(segy_root_object);
	write_dfs_file(daos_text_header, text_header, EBCBYTES);
	close_dfs_file(daos_text_header);
	free(text_header);

	daos_seis_close_root(segy_root_object);

	fini_dfs_api();

	return 0;
}