/*
 * seismic_obj_creation.c
 *
 *  Created on: May 19, 2020
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

//	char *temp = malloc(strlen(out_file) * sizeof(char));
//	strcpy(temp, &out_file[1]);

	char * file_name = malloc(1024 * sizeof(char));
	dfs_obj_t *parent = NULL;

	parent = get_parent_of_file_new(get_dfs(), out_file, 1, file_name, 1);
	printf("CALLING PARSE SEGY FUNCTION \n");
	if(fldr_exist == 1){
		daos_seis_parse_segy(get_dfs(), parent, file_name, segyfile->file, number_of_keys, header_keys);
		for(i = 0; i<  number_of_keys; i++){
			free(header_keys[i]);
		}
		free(header_keys);
	} else {
		daos_seis_parse_segy(get_dfs(), parent, file_name, segyfile->file, number_of_keys, updated_keys);
		for(i = 0; i<  number_of_keys; i++){
			free(updated_keys[i]);
		}
		free(updated_keys);
	}

	close_dfs_file(segyfile);
	printf(" OPEN SEIS ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), out_file);
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

	printf("READING SEGY ROOT BINARY HEADER KEY == \n");
//	bhed *binary_header = malloc(sizeof(bhed));
	DAOS_FILE *daos_binary;
	char *bfile;		/* name of binary header file	*/
	bfile = "daos_seis_binary";
	daos_binary = open_dfs_file(bfile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
	bhed *binary_header = daos_seis_get_binary_header(segy_root_object);
	write_dfs_file(daos_binary, (char *) binary_header, BNYBYTES);
	close_dfs_file(daos_binary);
	free(binary_header);

	printf("READING SEGY ROOT TEXT HEADER KEY == \n");
//	char *text_header = malloc(EBCBYTES*sizeof(char));

    DAOS_FILE *daos_text_header;
    char *tfile;		/* name of text header file	*/
    int rc;
    tfile = "daos_seis_text_header";
    daos_text_header = open_dfs_file(tfile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
    char *text_header = daos_seis_get_text_header(segy_root_object);
    write_dfs_file(daos_text_header, text_header, EBCBYTES);
    close_dfs_file(daos_text_header);
    free(text_header);


	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);

	printf("FINI DFS API=== \n");

    fini_dfs_api();


	return 0;
}
