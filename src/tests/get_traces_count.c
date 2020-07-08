/*
 * get_traces_count.c
 *
 *  Created on: Jul 5, 2020
 *      Author: mirnamoawad
 */


#include <daos_seis.h>

int main(int argc, char *argv[]){

    /* Optional */
    int verbose =1;                    /* Flag to allow verbose output */
    int allow_container_creation =1;   /* Flag to allow container creation if not found */


    char pool_id[100]="7d500429-c703-4759-ad63-ce332519c101";
	char container_id[100]="7d500429-c703-4759-ad63-ce332519c100";
	char svc_list[100]="0";

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), NULL,"/SEGY_ROOT_OBJECT");

	int number_of_traces;
	number_of_traces = daos_seis_get_trace_count(segy_root_object);
	printf("NUMBER OF TRACES == %d \n", number_of_traces);


	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);

	printf("FINI DFS API=== \n");

    fini_dfs_api();


	return 0;
}
