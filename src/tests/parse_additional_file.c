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

int main(int argc, char *argv[]){

    char *pool_id;      /* string of the pool uuid to connect to */
    char *container_id; /* string of the container uuid to connect to */
    char *svc_list;     /* string of the service rank list to connect to */
    char *in_file;      /* string of the path of the file that will be read */
    char *out_file;     /* string of the path of the file that will be written */
    char *keys;
    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("out",  &out_file);
    MUSTGETPARSTRING("keys",  &keys);

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;


	char temp[4096];
	int number_of_keys =0;
	strcpy(temp, keys);
	const char *sep = ",";
	char *token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}
	char **header_keys = malloc(number_of_keys * sizeof(char*));
	tokenize_str(header_keys,",", keys, 0);
	int i;
	int fldr_exist = 0;
	for(i=0; i<number_of_keys; i++) {
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

	char **updated_keys;
	if(fldr_exist == 0) {
		char *old_keys = malloc((strlen(keys) + 1) * sizeof(char));

		number_of_keys ++;
		strcpy(old_keys, keys);
		strcpy(keys, "fldr,");
		strcat(keys,old_keys);
		updated_keys = malloc(number_of_keys * sizeof(char*));
		tokenize_str(updated_keys,",", keys, 0);
		free(old_keys);
	}

	printf(" PARSING SEGY FILE == \n");
	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	DAOS_FILE *segyfile = open_dfs_file(in_file, S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);

	char * file_name = malloc(1024 * sizeof(char));
	dfs_obj_t *parent = NULL;

	parent = get_parent_of_file_new(get_dfs(), in_file, 1, file_name, 1);
	seis_root_obj_t 	*root_obj;
	seis_obj_t 		**seismic_obj;
	seismic_obj = malloc(number_of_keys * sizeof(seis_obj_t*));
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),out_file);

//	daos_seis_create_graph(get_dfs(), parent, file_name, number_of_keys, header_keys,
//				&root_obj, seismic_obj);
	printf("CALLING PARSE SEGY FUNCTION \n");
	if(fldr_exist == 1){
		daos_seis_parse_segy(get_dfs(), segyfile->file,
				    number_of_keys, header_keys, segy_root_object, seismic_obj, 1);
		for(i = 0; i<  number_of_keys; i++){
			free(header_keys[i]);
		}
		free(header_keys);
	} else {
		daos_seis_parse_segy(get_dfs(), segyfile->file,
				number_of_keys, updated_keys, segy_root_object, seismic_obj, 1);
		for(i = 0; i<  number_of_keys; i++){
			free(updated_keys[i]);
		}
		free(updated_keys);
	}

	close_dfs_file(segyfile);
//	daos_seis_close_root(segy_root_object);

	printf(" OPEN SEIS ROOT OBJECT== \n");
//	seis_root_obj_t *root_object = daos_seis_open_root_path(get_dfs(), out_file);
	printf("FINDING NUMBER OF GATHERS \n");
	int cmp_gathers;
	cmp_gathers = daos_seis_get_number_of_gathers(segy_root_object,"cdp");
	printf("NUMBER OF CMP GATHERSS== %d \n", cmp_gathers);

	int shot_gathers;
	shot_gathers = daos_seis_get_number_of_gathers(segy_root_object,"fldr");
	printf("NUMBER OF SHOT GATHERS== %d \n", shot_gathers);

	int offset_gathers;
	offset_gathers = daos_seis_get_number_of_gathers(segy_root_object,"offset");
	printf("NUMBER OF OFFSET GATHERSS == %d \n\n", offset_gathers);


	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);

	printf("FINI DFS API=== \n");

    fini_dfs_api();


	return 0;
}
