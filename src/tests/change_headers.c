/*
 * change_headers.c
 *
 *  Created on: Aug 24, 2020
 *      Author: mirnamoawad
 */

#include <daos_seis.h>

int
main(int argc, char *argv[])
{
	/** string of the pool uuid to connect to */
	char			       *pool_id;
	/** string of the container uuid to connect to */
	char 			       *container_id;
	/** string of the service rank list to connect to */
	char 			       *svc_list;
	/** string of the path of the file that will be read */
	char 			       *in_file;
	/** string of the path of the file that will be written */
	char 			       *out_file;
	/** string holding keys to change their header values */
	char 			       *key1;
	/** string holding keys that will be used to change values of key1 */
	char 			       *key2;
	/** string holding keys that will be used to change values of key1*/
	char 			       *key3;
	/** Flag to allow container creation if not found */
	int		 		allow_container_creation;
	/** Flag to allow verbose output */
	int 				verbose;
	char			       *a;
	char 			       *b;
	char 			       *c;
	char 			       *d;
	char 			       *e;
	char 			       *f;
	header_operation_type_t 	type = 1;

	/** Parse input parameters */
	initargs(argc, argv);
	MUSTGETPARSTRING("pool",  &pool_id);
	MUSTGETPARSTRING("container",  &container_id);
	MUSTGETPARSTRING("svc",  &svc_list);
	MUSTGETPARSTRING("in",  &in_file);
	MUSTGETPARSTRING("out",  &out_file);

	/** optional parameters*/
	if (!getparint("verbose", &verbose)) {
		verbose = 0;
	}
	if (!getparint("contcreation", &allow_container_creation)) {
		allow_container_creation = 1;
	}
	if(!getparstring("key1",  &key1)) {
		key1 = NULL;
	}
	if(!getparstring("key2",  &key2)) {
		key2 = NULL;
	}
	if(!getparstring("key3",  &key3)) {
		key3 = NULL;
	}
	if(!getparstring("a",  &a)) {
		a = NULL;
	}
	if(!getparstring("b",  &b)) {
		b = NULL;
	}
	if(!getparstring("c",  &c)) {
		c = NULL;
	}
	if(!getparstring("d",  &d)) {
		d = NULL;
	}
	if(!getparstring("e",  &e)) {
		e = NULL;
	}
	if(!getparstring("f",  &f)) {
		f = NULL;
	}

	struct timeval 		tv1;
	struct timeval		tv2;
	double 			time_taken;


	warn("\n Change header values \n"
	     "==================== \n");
	/** keys tokenization */
	char 			temp[4096];
	int 			number_of_keys = 0;
	const char 		*sep = ",";
	char 			*token;

	if(key1 != NULL) {
		strcpy(temp, key1);
		token = strtok(temp, sep);
		while( token != NULL ) {
			number_of_keys++;
			token = strtok(NULL, sep);
		}
	} else {
		number_of_keys = 1;
	}

	char 		       **keys_1;
	char 		       **keys_2;
	char 		       **keys_3;
	double 			*a_values;
	double 			*b_values;
	double 			*c_values;
	double 			*d_values;
	double 			*e_values;
	double 			*f_values;

	keys_1 = malloc(number_of_keys * sizeof(char*));
	keys_2 = malloc(number_of_keys * sizeof(char*));
	keys_3 = malloc(number_of_keys * sizeof(char*));
	a_values = malloc(number_of_keys * sizeof(double));
	b_values = malloc(number_of_keys * sizeof(double));
	c_values = malloc(number_of_keys * sizeof(double));
	d_values = malloc(number_of_keys * sizeof(double));
	e_values = malloc(number_of_keys * sizeof(double));
	f_values = malloc(number_of_keys * sizeof(double));
	int 			k;

	if(key1 != NULL) {
		tokenize_str(keys_1,",", key1, 0);
	} else {
		for(k = 0; k < number_of_keys; k++) {
			keys_1[k] = malloc((strlen("cdp")+1) * sizeof(char));
			strcpy(keys_1[k], "cdp");		}
	}
	if(key2 != NULL) {
		tokenize_str(keys_2,",", key2, 0);
	} else {
		for(k = 0; k < number_of_keys; k++) {
			keys_2[k] = malloc((strlen("cdp")+1) * sizeof(char));
			strcpy(keys_2[k], "cdp");		}
	}
	if(key3 != NULL) {
		tokenize_str(keys_3,",", key3, 0);
	} else {
		for(k = 0; k < number_of_keys; k++) {
			keys_3[k] = malloc((strlen("cdp")+1) * sizeof(char));
			strcpy(keys_3[k], "cdp");
		}
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

	init_dfs_api(pool_id, svc_list, container_id,
		     allow_container_creation, verbose);

	seis_root_obj_t *segy_root_object = daos_seis_open_root_path(get_dfs(),in_file);

	gettimeofday(&tv1, NULL);
	/** Change header values */
	daos_seis_set_headers(get_dfs(), segy_root_object, number_of_keys,
			      keys_1, keys_2, keys_3, a_values, b_values,
			      c_values, d_values, NULL, e_values, f_values,
			      type);
	gettimeofday(&tv2, NULL);
	/** Get traces headers */
	traces_list_t *traces = daos_seis_get_headers(segy_root_object);
	/** Get traces data */
	fetch_traces_data((get_dfs())->coh, &traces,get_daos_obj_mode(O_RDWR));
	/** Open output file to write traces to */
	FILE *fd = fopen(out_file, "w");

	traces_headers_t *temp_trace = traces->head;
	/** Fetch traces from linked list and write them to out_file */
	if(temp_trace == NULL) {
		("Linked list of traces is empty \n");
		return 0;
	}
	while(temp_trace != NULL) {
		/** convert trace struct back to original segy struct */
		segy* tp = trace_to_segy(&(temp_trace->trace));
		/** Write segy struct to file */
		fputtr(fd, tp);
		temp_trace = temp_trace->next_trace;
	}
	/** Free allocated memory */
	for(k = 0; k < number_of_keys; k++) {
		free(keys_1[k]);
		free(keys_2[k]);
		free(keys_3[k]);
	}
	free(keys_1);
	free(keys_2);
	free(keys_3);
	free(a_values);
	free(b_values);
	free(c_values);
	free(d_values);
	free(e_values);
	free(f_values);
	/** Release allocated linked list */
	release_traces_list(traces);
	/** Close opened root seismic object */
	daos_seis_close_root(segy_root_object);

	time_taken = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
		     (double) (tv2.tv_sec - tv1.tv_sec);
	warn("Time taken to change traces headers %f \n", time_taken);

	fini_dfs_api();

	return 0;
}

