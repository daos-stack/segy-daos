/*
 * update_traces_data.c
 *
 *  Created on: Sep 11, 2020
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
    int shot_id;

    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("out",  &out_file);
    MUSTGETPARINT("shot_id",  &shot_id);

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	printf("READING SHOT (%d) TRACES==\n",shot_id);

	gettimeofday(&tv1, NULL);
	traces_list_t *src_trace_list = daos_seis_get_shot_traces(shot_id, segy_root_object);
	traces_list_t *dst_trace_list = daos_seis_get_shot_traces( 601, segy_root_object);

	FILE *fd = fopen(out_file, "w");

	traces_headers_t *temp_src = src_trace_list->head;
	traces_headers_t *temp_dst = dst_trace_list->head;

	if (temp_src == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(temp_src != NULL){
			memcpy(temp_dst->trace.data, temp_src->trace.data, temp_src->trace.ns * sizeof(float));
			segy* tp = trace_to_segy(&(temp_src->trace));
	    	fputtr(fd, tp);
	    	temp_src = temp_src->next_trace;
	    	temp_dst = temp_dst->next_trace;
		}
	}

	printf("NUMBER OF TRACES in linked list == %d \n", src_trace_list->size);
	int number_of_traces;
	number_of_traces = daos_seis_get_trace_count(segy_root_object);
	printf("NUMBER OF TRACES == %d \n", number_of_traces);


	daos_seis_set_data(segy_root_object, dst_trace_list);

	traces_list_t *trace_list = daos_seis_get_shot_traces(601, segy_root_object);

	FILE *fdd = fopen("shot_601.su", "w");

	traces_headers_t *temp = trace_list->head;

	if (temp == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(temp != NULL){
			segy* tp = trace_to_segy(&(temp->trace));
	    	fputtr(fdd, tp);
	    	temp = temp->next_trace;
		}
	}

	temp = src_trace_list->head;
	while(temp != NULL){
		free(temp);
		temp = temp->next_trace;
	}
	free(src_trace_list);

	printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);
	gettimeofday(&tv2, NULL);
	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
	printf("TIME TAKEN IN MODIFIED READ FUNCCTION ISSS %f \n", time_taken);

	printf("FINI DFS API=== \n");

    fini_dfs_api();

	return 0;
}


