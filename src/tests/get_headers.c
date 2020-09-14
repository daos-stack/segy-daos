/*
 * get_headers.c
 *
 *  Created on: Aug 30, 2020
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
    int ascii = 1;
    initargs(argc, argv);
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("keys",  &keys);

    if(!getparstring("out",&out_file)) out_file = NULL;
    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;
	char temp[4096];
	int number_of_keys =0;
	strcpy(temp, keys);
	const char *sep = ",";
	char *token = strtok(temp, sep);
	while( token != NULL ) {
		number_of_keys++;
		token = strtok(NULL, sep);
	}
	char **header_keys = malloc(number_of_keys * sizeof(char*));
	tokenize_str(header_keys,",", keys, 0);


    struct timeval tv1, tv2;
    double time_taken;

	init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);

	printf(" OPEN SEGY ROOT OBJECT== \n");
	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	int ngathers;

	traces_list_t *trace_list = daos_seis_get_headers( segy_root_object);
	traces_headers_t *trace_header = trace_list->head;
	int i;
	Value val;

	if(ascii && out_file == NULL){
		if (trace_header == NULL) {
			printf("LINKED LIST EMPTY>>FAILURE\n");
			return 0;
		} else {
			while(trace_header != NULL){
				for(i=0; i<number_of_keys; i++){
					get_header_value(trace_header->trace, header_keys[i], &val);
                    printf("%6s=", header_keys[i]);
                    printfval(hdtype(header_keys[i]), val);
                    putchar('\t');
				}
                printf("\n\n");
		    	trace_header = trace_header->next_trace;
			}
		}
	} else {
		FILE *fd = fopen(out_file, "w");

//	    DAOS_FILE *daos_out_file;
//        daos_out_file = open_dfs_file(out_file, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
		if (trace_header == NULL) {
			printf("LINKED LIST EMPTY>>FAILURE\n");
			return 0;
		} else {
			while(trace_header != NULL){
				for(i=0; i<number_of_keys; i++) {
					get_header_value(trace_header->trace, header_keys[i], &val);
			        char *buffer = malloc(512 * sizeof(char));
	                sprintf(buffer, "%6s=", header_keys[i]);
	                efwrite(buffer, strlen(buffer), 1, fd);
//	                write_dfs_file(daos_out_file, buffer, strlen(buffer));
	                val_sprintf(buffer, val, header_keys[i]);
	                efwrite(buffer, strlen(buffer), 1, fd);
	                putc('\t',fd);
//	                printdfsval(hdtype(header_keys[i]), val, daos_out_file);
//	                putdfschar(daos_out_file, '\t');
	                free(buffer);
				}
				putc('\n',fd);
				putc('\n',fd);
				trace_header = trace_header->next_trace;
			}
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
