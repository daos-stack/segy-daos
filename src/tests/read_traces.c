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



	char pool_id[100]="9d9129b8-9e57-45e8-84fc-1a00310fcc62";
	char container_id[100]="9d9129b8-9e57-45e8-84fc-1a00310fcc61";
	char svc_list[100]="0";

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(), NULL,"/SHOTS_601_610_SEIS_ROOT_OBJECT");

	printf("READING SHOT 609 TRACES==\n");
	int shot_id = 609;

	gettimeofday(&tv1, NULL);
	traces_list_t *trace_list = new_daos_seis_read_shot_traces(get_dfs(), shot_id, segy_root_object);
    FILE *fd = fopen("daos_seis_SHOT_609_10_SHOT.su", "w");

	traces_headers_t *temp = trace_list->head;
	if (temp == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(temp != NULL){
	    	segy* tp = trace_to_segy(&(temp->trace));
	    	fputtr(fd, tp);
	    	temp = temp->next_trace;
		}
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
