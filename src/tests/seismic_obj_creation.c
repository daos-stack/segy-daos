/*
 * seismic_obj_creation.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */
#include <dfs_helper_api.h>
#include <daos_seis.h>
#include <sys/stat.h>


int main(int argc, char *argv[]){

	// Initialize DAOS


    /* Optional */
    int verbose =1;                    /* Flag to allow verbose output */
    int allow_container_creation =1;   /* Flag to allow container creation if not found */


    char pool_id[100]="a18bdf9d-c189-444b-95eb-2e7755919565";
	char container_id[100]="a18bdf9d-c189-444b-95eb-2e7755919561";
	char svc_list[100]="0";



	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
	DAOS_FILE *segyfile = open_dfs_file("/Test/segyobj", S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
	daos_seis_parse_segy(get_dfs(), NULL, "SEGY_ROOT_OBJECT", segyfile->file);
    close_dfs_file(segyfile);
    fini_dfs_api();




	return 0;
}
