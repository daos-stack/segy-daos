/*
 * window_traces.c
 *
 *  Created on: Jul 26, 2020
 *      Author: mirnamoawad
 */
#include <daos_seis.h>
#include <daos_seis_internal_functions.h>

#include <sys/time.h>

int main(int argc, char *argv[]){

    /* Optional */
    int verbose =0;                    /* Flag to allow verbose output */
    int allow_container_creation =1;   /* Flag to allow container creation if not found */

	char pool_id[100]="f3580d48-c945-4094-826d-2ef436fc1443";
	char container_id[100]="f3580d48-c945-4094-826d-2ef436fc1441";
	char svc_list[100]="0";

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), NULL,"/SHOTS_601_610_SEIS_ROOT_OBJECT");

	gettimeofday(&tv1, NULL);
	int ngathers;
	traces_headers_t *head = new_daos_seis_wind_traces(get_dfs(), segy_root_object, "fldr,tracl", "601,3100", "609,3500");

	if (head == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	}
    FILE *fd = fopen("daos_seis_SUWIND_fldr_tracl_3100_3500_10_SHOT.su", "w");
	int i;
	while(head!=NULL){
    	segy* tp = trace_to_segy(&(head->trace));
    	fputtr(fd, tp);
    	head=head->next_trace;
	}
    printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);
	gettimeofday(&tv2, NULL);
    time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
    printf("TIME TAKEN IN WINDOW FUNCCTION ISSS %f \n", time_taken);

	printf("FINI DFS API=== \n");

    fini_dfs_api();

	return 0;
}
