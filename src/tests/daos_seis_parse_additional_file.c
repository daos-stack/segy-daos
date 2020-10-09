/*
 * parse_additional_file.c
 *
 *  Created on: Sep 17, 2020
 *      Author: mirnamoawad
 */

#include <dfs_helper_api.h>
//#include <dfs_helpers.h>
#include <su_helpers.h>
#include <daos_seis.h>
#include <sys/stat.h>

char *sdoc[] = {
	        "									",
		" Parse additional file functionality.					",
		" It dumps segy file traces headers and data to opened			",
		" root seismic object and seismic gather objects. 			",
		"									",
		"  parse_additional_file pool=uuid container=uuid svc=r0:r1:r2 in=input_file_path out=output_file_path ",
		"									",
		"  Required parameters:							",
		"  pool_id 			the pool uuid to connect to.			",
		"  container_id			the container uuid to connect to.		",
		"  svc_list 			service rank list to connect to.		",
		"  in_file 			path of the segy file that will be parsed.	",
		"  out_file 			path of the seismic root object that		",
		"  				will be opened.					",
		"  Optional parameters:							",
		"  verbose 			=1 to allow verbose output.		",
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

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}


//	warn("\n PARSING Additional SEGY FILE \n"
//	     "================================ \n");

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation,
		     verbose);
	DAOS_FILE *segyfile = open_dfs_file(in_file,
					    S_IFREG | S_IWUSR | S_IRUSR,
					    'r', 0);

	char 			*file_name;
	dfs_obj_t 		*parent = NULL;

	file_name = malloc(1024 * sizeof(char));

	seis_root_obj_t 	*seis_root_object;
	seis_obj_t 		**seismic_obj;

	seis_root_object = daos_seis_open_root_path(get_dfs(),out_file);
	seismic_obj = malloc(seis_root_object->num_of_keys * sizeof(seis_obj_t*));

	daos_seis_parse_segy(get_dfs(), segyfile->file, seis_root_object,
			     seismic_obj, 1);

	/** Close opened segy file */
	close_dfs_file(segyfile);

	/** Close opened root seis object */
	daos_seis_close_root(seis_root_object);

	fini_dfs_api();

	return 0;
}
