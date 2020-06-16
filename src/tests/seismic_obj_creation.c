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

    /* Optional */
    int verbose =1;                    /* Flag to allow verbose output */
    int allow_container_creation =1;   /* Flag to allow container creation if not found */


    char pool_id[100]="6ed0aa46-0729-4cd8-b33b-defdcbe8ef23";
	char container_id[100]="6ed0aa46-0729-4cd8-b33b-defdcbe8ef21";
	char svc_list[100]="0";


	printf(" PARSING SEGY FILE ============================== \n");
	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
//	DAOS_FILE *segyfile = open_dfs_file("/Test/segyobj", S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
//	daos_seis_parse_segy(get_dfs(), NULL, "SEGY_ROOT_OBJECT", segyfile->file);
//    close_dfs_file(segyfile);
	printf(" BEFOREEEEEEEEEEEE OPEN SEGY ROOT OBJECT==================================== \n");


	printf(" OPEN SEGY ROOT OBJECT==================================== \n");
	segy_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), NULL,"SEGY_ROOT_OBJECT");
	int number_of_traces;
	printf(" PRINTING NUMBER OF TRACES UNDER SEGY ROOT OBJECT==================================== \n");
	number_of_traces = daos_seis_get_trace_count(segy_root_object);
	printf("NUMBER OF TRACES ===== %d", number_of_traces);

	printf("CLOSE SEGY ROOT OBJECT==================================== \n");
	daos_seis_close_root(segy_root_object);

    fini_dfs_api();




	return 0;
}
