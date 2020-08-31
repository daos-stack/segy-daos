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
    char *keys;
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

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

//    /* Optional */
//    int verbose =0;                    /* Flag to allow verbose output */
//    int allow_container_creation =1;   /* Flag to allow container creation if not found */
//
//	char pool_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3561";
//	char container_id[100]="08b9a6dc-aa4d-42e2-87bd-1d8dc86b3560";
//	char svc_list[100]="0";

//    int nkey = argc - 4;
//    char **keys = malloc(nkey * sizeof(char*));
//    int *directions = malloc(nkey * sizeof(int));
//    int i;
//    int k =0;
//
//	/* Initialize index, type and up */
//	if (argc == 4) {
//		keys[0] = malloc(strlen("cdp") * sizeof(char));
//		strcpy(keys[0], "cdp");
//		directions[0] = 1;
//	} else {
//		for(i=3; i <nkey-1; i++){
//			switch (**argv) { /* sign char of next arg */
//			case '+':
//				directions[k] = 1;
//				++*argv;   /* discard sign char in arg */
//			break;
//			case '-':
//				directions[k] = 0;
//				++*argv;   /* discard sign char in arg */
//			break;
//			default:
//				directions[k] = 1;
//			break;
//			}
//			keys[k] = malloc(strlen(*argv) * sizeof(char));
//			strcpy(keys[k], *argv[0]);
//			k++;
//		}
//	}

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

//	time(&start);
//	daos_seis_read_shot_traces(get_dfs(), 610, segy_root_object, "old_daos_seis_SHOT_610_.su");
//    time(&end);
//
//    time_taken = (double)(end - start);
//    printf("TIME TAKEN IN ORIGINAL READ FUNCCTION ISSS %f \n", time_taken);


//	daos_seis_read_shot_traces(get_dfs(), shot_, segy_root_object, "daos_seis_SHOT_610_.su");

	gettimeofday(&tv1, NULL);
	int ngathers;
//	read_traces *all_traces = daos_seis_sort_headers(get_dfs(), segy_root_object, "+fldr,-gx", &ngathers);
	traces_list_t *trace_list = daos_seis_sort_headers(get_dfs(), segy_root_object, keys);

	FILE *fd = fopen(out_file, "w");

	int tracl_mod = 1;
	traces_headers_t *temp = trace_list->head;
	if (temp == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(temp != NULL){
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
