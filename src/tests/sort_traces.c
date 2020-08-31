/*
 * sort_traces.c
 *
 *  Created on: Jul 22, 2020
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
    char *sort_keys;
    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */
    char *window_keys;
    char *min;
    char *max;

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("out",  &out_file);
    MUSTGETPARSTRING("keys",  &sort_keys);

    if (!getparstring("window_keys", &window_keys))		window_keys = NULL;
    if (!getparstring("min", &min))		min = NULL;
    if (!getparstring("max", &max))		max = NULL;

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	int ngathers;
	traces_list_t *trace_list = daos_seis_sort_headers(get_dfs(), segy_root_object, sort_keys, window_keys, min, max);
//	if(window_keys != NULL){
////		printf("HELLO Hello  \n");
//		new_window_headers(&trace_list, window_keys, min, max);
//	}

	FILE *fd = fopen(out_file, "w");

	int tracl_mod = 1;
	traces_headers_t *temp = trace_list->head;
	if (temp == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(temp != NULL){
			printf("tracl  === %d , fldr === %d \n", temp->trace.tracl, temp->trace.fldr);
	    	segy* tp = trace_to_segy(&(temp->trace));
	    	tp->tracl = tp->tracr = tracl_mod;
	    	tracl_mod++;
	    	fputtr(fd, tp);
	    	temp = temp->next_trace;
		}
	}

    printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);
	gettimeofday(&tv2, NULL);
    time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
    printf("TIME TAKEN IN SORT FUNCCTION ISSS %f \n", time_taken);

	printf("FINI DFS API=== \n");

    fini_dfs_api();

	return 0;
}
