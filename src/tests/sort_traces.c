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
    char *w_keys;
    char *min;
    char *max;

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("out",  &out_file);
    MUSTGETPARSTRING("keys",  &keys);

    if (!getparstring("window_keys", &w_keys))		w_keys = NULL;
    if (!getparstring("min", &min))		min = NULL;
    if (!getparstring("max", &max))		max = NULL;

    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;


	char temp[4096];
    int number_of_keys = 0;
    strcpy(temp, keys);
    const char *sep = ",";
    char *token = strtok(temp, sep);
    while( token != NULL ) {
    	number_of_keys++;
        token = strtok(NULL, sep);
    }
    int *directions;
    char **sort_keys;
    int i;
    if(number_of_keys == 0){
    	number_of_keys = 1;
        directions = malloc(number_of_keys * sizeof(int));
        sort_keys = malloc(number_of_keys * sizeof(char *));
    	sort_keys[0] = malloc((strlen("CDP") + 1) * sizeof(char));
		directions[0]=1;
		strcpy(sort_keys[0], "CDP");
		printf("DEFAULT SORTING KEY IS CDP \n");
    } else {
        //array of keys
        directions = malloc(number_of_keys * sizeof(int));
        sort_keys = malloc(number_of_keys * sizeof(char *));
        strcpy(temp, keys);
        token = strtok(temp, sep);

        i = 0;
        while( token != NULL ) {
        	sort_keys[i] = malloc((strlen(token) + 1) * sizeof(char));
        	if(token[0]== '-'){
        		directions[i]= 0;
        		strcpy(sort_keys[i], &token[1]);
        	} else if (token[0]== '+'){
        		directions[i]=1;
        		strcpy(sort_keys[i], &token[1]);
        	} else {
        		directions[i]=1;
    			strcpy(sort_keys[i], token);
        	}
            token = strtok(NULL, sep);
            i++;
        }
    }

	char temp2[4096];
	char min_temp[4096];
	char max_temp[4096];
	int number_of_window_keys = 0;
	char **window_keys;
	Value *min_keys;
	Value *max_keys;
	cwp_String *type;

	if(w_keys != NULL) {
		strcpy(temp2, w_keys);
	//	const char *sep = ",";
		token = strtok(temp2, sep);
		while( token != NULL ) {
			number_of_window_keys++;
			token = strtok(NULL, sep);
		}
		printf("NUMBER OF KEYS === %d \n",number_of_window_keys);

		window_keys = malloc(number_of_window_keys * sizeof(char*));
		min_keys = malloc(number_of_window_keys * sizeof(Value));
		max_keys = malloc(number_of_window_keys * sizeof(Value));
		type = malloc(number_of_window_keys * sizeof(cwp_String));
		i=0;
		strcpy(temp2,w_keys);
		strcpy(min_temp,min);
		strcpy(max_temp,max);
		token =strtok(temp2,sep);
		while(token != NULL){
			window_keys[i]= malloc((strlen(token) + 1)*sizeof(char));
			strcpy(window_keys[i], token);
			type[i] = hdtype(window_keys[i]);
			token = strtok(NULL,sep);
			i++;
		}
		char *min_token =strtok(min_temp, sep);
		i=0;
		while(min_token != NULL){
			atoval(type[i], min_token, &min_keys[i]);
			min_token = strtok(NULL,sep);
			i++;
		}
		char *max_token = strtok(max_temp, sep);
		i=0;
		while(max_token != NULL){
			atoval(type[i], max_token, &max_keys[i]);
			max_token = strtok(NULL,sep);
			i++;
		}
	}

    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	int ngathers;
	traces_list_t *trace_list = daos_seis_sort_headers(get_dfs(), segy_root_object, number_of_keys, sort_keys, directions, number_of_window_keys,
									window_keys, type, min_keys, max_keys);
//	if(window_keys != NULL){
////		printf("HELLO Hello  \n");
//		new_window_headers(&trace_list, window_keys, min, max);
//	}

	FILE *fd = fopen(out_file, "w");

	int tracl_mod = 1;
	traces_headers_t *tempo = trace_list->head;
	if (tempo == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else {
		while(tempo != NULL){
			printf("tracl  === %d , fldr === %d \n", tempo->trace.tracl, tempo->trace.fldr);
	    	segy* tp = trace_to_segy(&(tempo->trace));
	    	tp->tracl = tp->tracr = tracl_mod;
	    	tracl_mod++;
	    	fputtr(fd, tp);
	    	tempo = tempo->next_trace;
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
