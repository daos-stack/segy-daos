/*
 * range_headers.c
 *
 *  Created on: Aug 27, 2020
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
    char *keys;
	int dim;			/* dim line with coords in ft (1) or m (2) */

    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);

    if(!getparstring("keys",  &keys))keys = NULL;
    if (!getparint("dim", &dim)) dim = 0;

    /**optional */
    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

    struct timeval tv1, tv2;
    double time_taken;

	char temp[4096];
	int rc;
	int number_of_keys =0;
	if(keys != NULL) {
		strcpy(temp, keys);
		const char *sep = ",";
		char *token = strtok(temp, sep);
		while( token != NULL ) {
			number_of_keys++;
			token = strtok(NULL, sep);
		}
	} else {
		number_of_keys = 0;
	}

	printf("NUMBER OF KEYS === %d \n",number_of_keys);
	char **range_keys;
	if(number_of_keys == 0){
		range_keys = malloc(SU_NKEYS * sizeof(char*));
	} else{
		range_keys = malloc(number_of_keys * sizeof(char*));
	}

	int k;

	if(keys != NULL) {
		tokenize_str(range_keys,",", keys, 0);
	} else {
		for(k=0; k < SU_NKEYS; k++){
			range_keys[k]= malloc((strlen(hdr[k].key)+1)*sizeof(char));
			strcpy(range_keys[k], hdr[k].key);
		}
	}
	printf("===================KEY VALUES IN RANGE HEADER MAIN================ \n");
	if(number_of_keys == 0){
		for( k=0; k< SU_NKEYS; k++){
			printf("Range KEY IS %s \n", range_keys[k]);
		}
	} else {
		for( k=0; k< number_of_keys; k++){
			printf("Range KEY IS %s \n", range_keys[k]);
		}
	}

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

//	int cmp_gathers;
//	cmp_gathers = daos_seis_get_cmp_gathers(get_dfs(),segy_root_object);
//	printf("NUMBER OF CMP GATHERSS== %d \n", cmp_gathers);
//
//	int shot_gathers;
//	shot_gathers = daos_seis_get_shot_gathers(get_dfs(),segy_root_object);
//	printf("NUMBER OF SHOT GATHERS== %d \n", shot_gathers);
//
//	int offset_gathers;
//	offset_gathers = daos_seis_get_offset_gathers(get_dfs(),segy_root_object);
//	printf("NUMBER OF OFFSET GATHERSS == %d \n\n", offset_gathers);
//	printf("CMP_OID %llu %llu \n", segy_root_object->cmp_oid.lo, segy_root_object->cmp_oid.hi);


//	traces_list_t *trace_list = daos_seis_set_headers(get_dfs(), segy_root_object, number_of_keys, keys_1, keys_2, keys_3, a_values, b_values, c_values,
//													d_values, NULL, e_values, f_values, type);
	gettimeofday(&tv1, NULL);

	daos_seis_range_headers(get_dfs(), segy_root_object, number_of_keys, range_keys, dim);

	printf("AFTER RANGE HEADERS \n");
//	int tracl_mod = 1;
//	traces_headers_t *tempo = trace_list->head;
//	if (tempo == NULL) {
//		printf("LINKED LIST EMPTY>>FAILURE\n");
//		return 0;
//	} else{
//		while(tempo != NULL){
//			printf("TRACE GX ==== %d \n", tempo->trace.gx);
//			printf("TRACE CDP ==== %d \n", tempo->trace.cdp);
//	    	tempo = tempo->next_trace;
//		}
//	}

//	cmp_gathers = daos_seis_get_cmp_gathers(get_dfs(),segy_root_object);
//	printf("NUMBER OF CMP GATHERSS== %d \n", cmp_gathers);

    printf("CLOSE SEGY ROOT OBJECT== \n");
	daos_seis_close_root(segy_root_object);
	gettimeofday(&tv2, NULL);
    time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
    printf("TIME TAKEN IN RANGE HEADERS FUNCCTION ISSS %f \n", time_taken);

	printf("FINI DFS API=== \n");

    fini_dfs_api();

	return 0;
}

