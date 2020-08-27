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

    char *pool_id;      /* string of the pool uuid to connect to */
    char *container_id; /* string of the container uuid to connect to */
    char *svc_list;     /* string of the service rank list to connect to */
    char *in_file;      /* string of the path of the file that will be read */
    char *out_file;     /* string of the path of the file that will be written */
    char *keys;
    char *min;
    char *max;

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
    MUSTGETPARSTRING("min",  &min);
    MUSTGETPARSTRING("max",  &max);

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

//    /* Optional */
//    int verbose =0;                    /* Flag to allow verbose output */
//    int allow_container_creation =1;   /* Flag to allow container creation if not found */
//
//	char pool_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3561";
//	char container_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3560";
//	char svc_list[100]="0";

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	int ngathers;
	traces_list_t *trace_list = new_daos_seis_wind_traces(get_dfs(), segy_root_object, keys, min, max);
    FILE *fd = fopen(out_file, "w");
	traces_headers_t *temp = trace_list->head;
	if (temp == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(temp != NULL){
			printf("tracl  === %d , fldr === %d \n", temp->trace.tracl, temp->trace.fldr);
	    	segy* tp = trace_to_segy(&(temp->trace));
	    	fputtr(fd, tp);
	    	temp = temp->next_trace;
		}
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
