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

    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("out",  &out_file);

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

//    /* Optional */
//    int verbose = 0;                    /* Flag to allow verbose output */
//    int allow_container_creation =1;   /* Flag to allow container creation if not found */
//
//
//
//	char pool_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3561";
//	char container_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3560";
//
//	char svc_list[100]="0";

	printf(" PARSING SEGY FILE == \n");
	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	DAOS_FILE *segyfile = open_dfs_file(in_file, S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);

//	char *temp = malloc(strlen(out_file) * sizeof(char));
//	strcpy(temp, &out_file[1]);

	char * file_name = malloc(1024 * sizeof(char));
	dfs_obj_t *parent = NULL;

	parent = get_parent_of_file_new(get_dfs(), out_file, 1, file_name, 1);

	daos_seis_parse_segy(get_dfs(), parent, file_name, segyfile->file);
	close_dfs_file(segyfile);
	printf(" OPEN SEIS ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), out_file);

	int cmp_gathers;
	cmp_gathers = daos_seis_get_cmp_gathers(get_dfs(),segy_root_object);
	printf("NUMBER OF CMP GATHERSS== %d \n", cmp_gathers);

	int shot_gathers;
	shot_gathers = daos_seis_get_shot_gathers(get_dfs(),segy_root_object);
	printf("NUMBER OF SHOT GATHERS== %d \n", shot_gathers);

	int offset_gathers;
	offset_gathers = daos_seis_get_offset_gathers(get_dfs(),segy_root_object);
	printf("NUMBER OF OFFSET GATHERSS == %d \n\n", offset_gathers);

	printf("READING SEGY ROOT BINARY HEADER KEY == \n");
	bhed *binary_header = malloc(sizeof(bhed));
	DAOS_FILE *daos_binary;
	char *bfile;		/* name of binary header file	*/
	bfile = "daos_seis_binary";
	daos_binary = open_dfs_file(bfile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
	binary_header = daos_seis_read_binary_header(segy_root_object);
	write_dfs_file(daos_binary, (char *) binary_header, BNYBYTES);
	close_dfs_file(daos_binary);
	free(binary_header);

	printf("READING SEGY ROOT TEXT HEADER KEY == \n");
	char *text_header = malloc(EBCBYTES*sizeof(char));

    DAOS_FILE *daos_text_header;
    char *tfile;		/* name of text header file	*/
    int rc;
    tfile = "daos_seis_text_header";
    daos_text_header = open_dfs_file(tfile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
    text_header = daos_seis_read_text_header(segy_root_object);
    write_dfs_file(daos_text_header, text_header, EBCBYTES);
    close_dfs_file(daos_text_header);
    free(text_header);


	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);

	printf("FINI DFS API=== \n");

    fini_dfs_api();


	return 0;
}
