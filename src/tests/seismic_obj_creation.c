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

    /* Optional */
    int verbose = 0;                    /* Flag to allow verbose output */
    int allow_container_creation =1;   /* Flag to allow container creation if not found */



	char pool_id[100]="f3580d48-c945-4094-826d-2ef436fc1443";
	char container_id[100]="f3580d48-c945-4094-826d-2ef436fc1441";

	char svc_list[100]="0";

	printf(" PARSING SEGY FILE == \n");
	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	DAOS_FILE *segyfile = open_dfs_file("/Test/shot_601_610", S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);

	daos_seis_parse_segy(get_dfs(), NULL, "SHOTS_601_610_SEIS_ROOT_OBJECT", segyfile->file);
	close_dfs_file(segyfile);
	printf(" OPEN SEIS ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), NULL,"/SHOTS_601_610_SEIS_ROOT_OBJECT");

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
