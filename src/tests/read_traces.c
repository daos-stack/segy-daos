/*
 * read_traces.c
 *
 *  Created on: Jul 5, 2020
 *      Author: mirnamoawad
 */


#include <daos_seis.h>
#include <daos_seis_internal_functions.h>

#include <sys/time.h>

int main(int argc, char *argv[]){

    /* Optional */
    int verbose =0;                    /* Flag to allow verbose output */
    int allow_container_creation =1;   /* Flag to allow container creation if not found */



        char pool_id[100]="1ee7a898-9f87-4ff6-92fc-8ef43955cdea";
	char container_id[100]="1ee7a898-9f87-4ff6-92fc-8ef43955cde0";
	char svc_list[100]="0";

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), NULL,"/SHOT_601_700_SEIS_ROOT_OBJECT");

//	time(&start);
//	daos_seis_read_shot_traces(get_dfs(), 610, segy_root_object, "old_daos_seis_SHOT_610_.su");
//    time(&end);
//
//    time_taken = (double)(end - start);
//    printf("TIME TAKEN IN ORIGINAL READ FUNCCTION ISSS %f \n", time_taken);

	read_traces *traces = new_daos_seis_read_shot_traces(get_dfs(), 600, segy_root_object);
	printf("READING SHOT 610 TRACES==\n");
	int shot_id = 610;

//	daos_seis_read_shot_traces(get_dfs(), shot_, segy_root_object, "daos_seis_SHOT_610_.su");

	gettimeofday(&tv1, NULL);
	read_traces *all_traces = new_daos_seis_read_shot_traces(get_dfs(), shot_id, segy_root_object);
	if (all_traces == 0) {
		printf("Couldn't open shot 610, test FAILED\n");
		return 0;
	}
    FILE *fd = fopen("daos_seis_SHOT_610_100_SHOT.su", "w");

	int i;
    for(i=0; i < all_traces->number_of_traces; i++){
    	segy* tp = trace_to_segy(&(all_traces->traces[i]));
    	fputtr(fd, tp);
    }

	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);
	gettimeofday(&tv2, NULL);
    time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
    printf("TIME TAKEN IN MODIFIED READ FUNCCTION ISSS %f \n", time_taken);

	printf("FINI DFS API=== \n");

    fini_dfs_api();

	return 0;
}
