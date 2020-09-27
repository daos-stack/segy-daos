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

	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}

	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}


	warn("\n PARSING Additional SEGY FILE \n"
	     "================================ \n");

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

	daos_seis_parse_segy(get_dfs(), segyfile->file, seis_root_object->num_of_keys,
			     seis_root_object->keys, seis_root_object, seismic_obj, 1);

	/** Close opened segy file */
	close_dfs_file(segyfile);
//
//	printf(" OPEN SEIS ROOT OBJECT== \n");
////	seis_root_obj_t *root_object = daos_seis_open_root_path(get_dfs(), out_file);
//	printf("FINDING NUMBER OF GATHERS \n");
//	int cmp_gathers;
//	cmp_gathers = daos_seis_get_number_of_gathers(segy_root_object,"cdp");
//	printf("NUMBER OF CMP GATHERSS== %d \n", cmp_gathers);
//
//	int shot_gathers;
//	shot_gathers = daos_seis_get_number_of_gathers(segy_root_object,"fldr");
//	printf("NUMBER OF SHOT GATHERS== %d \n", shot_gathers);
//
//	int offset_gathers;
//	offset_gathers = daos_seis_get_number_of_gathers(segy_root_object,"offset");
//	printf("NUMBER OF OFFSET GATHERSS == %d \n\n", offset_gathers);


	/** Close opened root seis object */
	daos_seis_close_root(seis_root_object);

	fini_dfs_api();

	return 0;
}
