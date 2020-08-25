/*
 * change_headers.c
 *
 *  Created on: Aug 24, 2020
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
    char *key1;
    char *key2;
    char *key3;
    char *a;
    char *b;
    char *c;
    char *d;
    char *e;
    char *f;
    header_type_t type = 1;

    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */

    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);

    if(!getparstring("key1",  &key1))key1 = NULL;
    if(!getparstring("key2",  &key2))key2 = NULL;
    if(!getparstring("key3",  &key3)) key3 = NULL;
    if(!getparstring("a",  &a)) a = NULL;
    if(!getparstring("b",  &b)) b = NULL;
    if(!getparstring("c",  &c)) c = NULL;
    if(!getparstring("d",  &d)) d = NULL;
    if(!getparstring("e",  &e)) e = NULL;
    if(!getparstring("f",  &f)) f = NULL;

    /**optional */
    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

    struct timeval tv1, tv2;
    double time_taken;

	char temp[4096];
	int rc;
	int number_of_keys =0;
	if(key1 != NULL) {
		strcpy(temp, key1);
		const char *sep = ",";
		char *token = strtok(temp, sep);
		while( token != NULL ) {
			number_of_keys++;
			token = strtok(NULL, sep);
		}
	} else {
		number_of_keys = 1;
	}
	printf("NUMBER OF KEYS === %d \n",number_of_keys);

	char **keys_1 = malloc(number_of_keys * sizeof(char*));
	char **keys_2 = malloc(number_of_keys * sizeof(char*));
	char **keys_3 = malloc(number_of_keys * sizeof(char*));
	double *a_values = malloc(number_of_keys * sizeof(double));
	double *b_values = malloc(number_of_keys * sizeof(double));
	double *c_values = malloc(number_of_keys * sizeof(double));
	double *d_values = malloc(number_of_keys * sizeof(double));
	double *e_values = malloc(number_of_keys * sizeof(double));
	double *f_values = malloc(number_of_keys * sizeof(double));
	int k;

	if(key1 != NULL) {
		tokenize_str(keys_1,",", key1, 0);
	} else {
		keys_1[0] = malloc((strlen("cdp")+1) * sizeof(char));
		keys_1[0] = "cdp";
	}
	if(key2 != NULL) {
		tokenize_str(keys_2,",", key2, 0);
	} else {
		keys_2[0] = malloc((strlen("cdp")+1) * sizeof(char));
		keys_2[0] = "cdp";
	}
	if(key3 != NULL) {
		tokenize_str(keys_3,",", key3, 0);
	} else {
		keys_3[0] = malloc((strlen("cdp")+1) * sizeof(char));
		keys_3[0] = "cdp";
	}
	if(a != NULL) {
		tokenize_str(&a_values,",", a, 2);
	} else {
		for(k=0; k< number_of_keys; k++){
			a_values[k] = 0;
		}
	}
	if(b != NULL) {
		tokenize_str(&b_values,",", b, 2);
	} else {
		for(k=0; k< number_of_keys; k++){
			b_values[k] = 1;
		}
	}
	if(c != NULL) {
		tokenize_str(&c_values,",", c, 2);
	} else {
		for(k=0; k< number_of_keys; k++){
			c_values[k] = 0;
		}
	}
	if(d != NULL) {
		tokenize_str(&d_values,",", d, 2);
	} else {
		for(k=0; k< number_of_keys; k++){
			d_values[k] = 1;
		}
	}
	if(e != NULL) {
		tokenize_str(&e_values,",", e, 2);
	} else {
		for(k=0; k< number_of_keys; k++){
			e_values[k] = 1;
		}
	}
	if(f != NULL) {
		tokenize_str(&f_values,",", f, 2);
	} else {
		for(k=0; k< number_of_keys; k++){
			f_values[k] = 1;
		}
	}
	printf("===================KEY VALUES IN CHANGE HEADER MAIN================ \n");
	for( k=0; k< number_of_keys; k++){
		printf("KEY 1 IS %s \n", keys_1[k]);
		printf("KEY 2 IS %s \n", keys_2[k]);
		printf("KEY 3 IS %s \n", keys_3[k]);
		printf("A IS %lf \n", a_values[k]);
		printf("B IS %lf \n", b_values[k]);
		printf("C IS %lf \n", c_values[k]);
		printf("D IS %lf \n", d_values[k]);
		printf("E IS %lf \n", e_values[k]);
		printf("F IS %lf \n", f_values[k]);
	}

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	int ngathers;

	traces_list_t *trace_list = daos_seis_set_headers(get_dfs(), segy_root_object, number_of_keys, keys_1, keys_2, keys_3, a_values, b_values, c_values,
													d_values, NULL, e_values, f_values, type);

	FILE *fd = fopen(out_file, "w");

	int tracl_mod = 1;
	traces_headers_t *tempo = trace_list->head;
	if (tempo == NULL) {
		printf("LINKED LIST EMPTY>>FAILURE\n");
		return 0;
	} else{
		while(tempo != NULL){
			printf("TRACE GX ==== %d \n", tempo->trace.gx);
			printf("TRACE CDP ==== %d \n", tempo->trace.cdp);
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

