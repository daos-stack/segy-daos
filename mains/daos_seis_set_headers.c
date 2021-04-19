/*
 * daos_seis_set_headers.c
 *
 *  Created on: Mar 7, 2021
 *      Author: omar
 */

#include "api/seismic_graph_api.h"

int
main(int argc, char *argv[])
{
	root_obj_t *root_obj;
	size_t 	   value_length;
	double 	   *a_values;
	double     *b_values;
	double 	   *c_values;
	double 	   *d_values;
	double 	   *e_values;
	/** string of the pool uuid to connect to */
	char 	   *pool_id;
	/** string of the container uuid to connect to */
	char 	   *container_id;
	/** string of the path of the file that will be read */
	char 	   *seis_root_path;
	/** string holding keys that will be used in setting the headers.
	 */
	char 	   *keys;
	char 	   **header_keys;
	char 	   *a;
	char 	   *b;
	char 	   *c;
	char 	   *d;
	char 	   *e;
	/** Flag to allow container creation if not found */
	int 	   allow_container_creation = 0;
	int 	   number_of_keys = 0;
	int 	   rc;
	int 	   k;


	if(argc <= 4) {
		warn("you are passing only %d arguments. "
		      "Minimum 4 arguments are required: pool_id, container_id,"
		      "seismic graph path \n", argc-1);
		exit(0);
	}


	pool_id = malloc(37 * sizeof(char));
	strcpy(pool_id, argv[1]);

	container_id = malloc(37 * sizeof(char));
	strcpy(container_id, argv[2]);

	size_t seis_root_path_length = strlen(argv[3]);
	seis_root_path = malloc(seis_root_path_length + 1 * sizeof(char));
	strcpy(seis_root_path, argv[3]);

	if (argc >= 5) {  //keys
		size_t keys_length = strlen(argv[4]);
		keys = malloc(keys_length + 1 * sizeof(char));
		strcpy(keys, argv[4]);
		rc = tokenize_str((void**) &header_keys, ",", keys, STRING,
				  &number_of_keys);
	} else {
		number_of_keys = 1;
		header_keys = malloc(number_of_keys * sizeof(char*));
		for (k = 0; k < number_of_keys; k++) {
			header_keys[k] = malloc(
					(strlen("cdp") + 1) * sizeof(char));
			strcpy(header_keys[k], "cdp");
		}
	}

	a_values = malloc(number_of_keys * sizeof(double));
	b_values = malloc(number_of_keys * sizeof(double));
	c_values = malloc(number_of_keys * sizeof(double));
	d_values = malloc(number_of_keys * sizeof(double));
	e_values = malloc(number_of_keys * sizeof(double));

	if (argc >= 6) { //a values
		value_length = strlen(argv[5]);
		a = malloc(value_length + 1 * sizeof(char));
		strcpy(a, argv[5]);
		rc = tokenize_str((void**) &a_values, ",", a, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			a_values[k] = 0;
		}
	}

	if (argc >= 7) { //b values
		value_length = strlen(argv[6]);
		b = malloc(value_length + 1 * sizeof(char));
		strcpy(b, argv[6]);
		rc = tokenize_str((void**) &b_values, ",", b, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			b_values[k] = 0;
		}
	}

	if (argc >= 8) { //c values
		value_length = strlen(argv[7]);
		c = malloc(value_length + 1 * sizeof(char));
		strcpy(c, argv[7]);
		rc = tokenize_str((void**) &c_values, ",", c, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			c_values[k] = 0;
		}
	}

	if (argc >= 9) { //d values
		value_length = strlen(argv[8]);
		d = malloc(value_length + 1 * sizeof(char));
		strcpy(d, argv[8]);
		rc = tokenize_str((void**) &d_values, ",", d, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			d_values[k] = 0;
		}
	}

	if (argc >= 10) { //e values
		value_length = strlen(argv[9]);
		e = malloc(value_length + 1 * sizeof(char));
		strcpy(e, argv[9]);
		rc = tokenize_str((void**) &e_values, ",", e, DOUBLE,
				&number_of_keys);
		for (k = 0; k < number_of_keys; k++) {
			if (e_values[k] == 0) {
				e_values[k] = ULONG_MAX;
			}
		}
	} else {
		for (k = 0; k < number_of_keys; k++) {
			e_values[k] = ULONG_MAX;
		}
	}
	init_dfs_api(pool_id, container_id, allow_container_creation,
			0);
	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);
	daos_seis_set_headers(root_obj, number_of_keys, header_keys, a_values,
			b_values, c_values, d_values, e_values);

	rc = daos_seis_close_graph(root_obj);

	fini_dfs_api();
	release_tokenized_array(header_keys, STRING, number_of_keys);
	free(a);
	free(b);
	free(c);
	free(d);
	free(e);
	free(a_values);
	free(b_values);
	free(c_values);
	free(d_values);
	free(e_values);
	free(pool_id);
	free(container_id);
	free(seis_root_path);

	return 0;
}
