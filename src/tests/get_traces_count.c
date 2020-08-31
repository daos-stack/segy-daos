/*
 * get_traces_count.c
 *
 *  Created on: Jul 5, 2020
 *      Author: mirnamoawad
 */


#include <daos_seis.h>

int main(int argc, char *argv[]){

    char *pool_id;      /* string of the pool uuid to connect to */
    char *container_id; /* string of the container uuid to connect to */
    char *svc_list;     /* string of the service rank list to connect to */
    char *in_file;      /* string of the path of the file that will be read */

    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

//    /* Optional */
//    int verbose =1;                    /* Flag to allow verbose output */
//    int allow_container_creation =1;   /* Flag to allow container creation if not found */
//
//
//	char pool_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3561";
//	char container_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3560";
//
//	char svc_list[100]="0";

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	int number_of_traces;
	number_of_traces = daos_seis_get_trace_count(segy_root_object);
	printf("NUMBER OF TRACES == %d \n", number_of_traces);

	int cmp_gathers;
	cmp_gathers = daos_seis_get_number_of_gathers(get_dfs(),segy_root_object, "cdp");
	printf("NUMBER OF CMP GATHERSS== %d \n", cmp_gathers);

	int shot_gathers;
	shot_gathers = daos_seis_get_number_of_gathers(get_dfs(),segy_root_object, "fldr");
	printf("NUMBER OF SHOT GATHERS== %d \n", shot_gathers);

	int offset_gathers;
	offset_gathers = daos_seis_get_number_of_gathers(get_dfs(),segy_root_object, "offset");
	printf("NUMBER OF OFFSET GATHERSS == %d \n\n", offset_gathers);

	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);

	printf("FINI DFS API=== \n");

    fini_dfs_api();


	return 0;
}
