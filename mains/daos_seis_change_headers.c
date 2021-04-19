/*
 * daos_seis_change_headers.c
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
	double	   *b_values;
	double	   *c_values;
	double	   *d_values;
	double	   *e_values;
	double	   *f_values;
	/** string of the pool uuid to connect to */
	char 	   *pool_id;
	/** string of the container uuid to connect to */
	char 	   *container_id;
	/** string of the path of the file that will be read */
	char 	   *seis_root_path;
	/** string holding keys that will be used in setting the headers.
	 */
	char 	   *keys1;
	char 	   *keys2;
	char 	   *keys3;
	char 	   *a;
	char 	   *b;
	char 	   *c;
	char 	   *d;
	char 	   *e;
	char 	   *f;
	char 	   **header_keys1;
	char 	   **header_keys2;
	char 	   **header_keys3;
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
		keys1 = malloc(keys_length + 1 * sizeof(char));
		strcpy(keys1, argv[4]);
		rc = tokenize_str((void**) &header_keys1, ",", keys1, STRING,
				&number_of_keys);
	} else {
		number_of_keys = 1;
		header_keys1 = malloc(number_of_keys * sizeof(char*));
		for (k = 0; k < number_of_keys; k++) {
			header_keys1[k] = malloc(
					(strlen("cdp") + 1) * sizeof(char));
			strcpy(header_keys1[k], "cdp");
		}
	}

	if (argc >= 6) {  //keys
		size_t keys_length = strlen(argv[5]);
		keys2 = malloc(keys_length + 1 * sizeof(char));
		strcpy(keys2, argv[5]);
		rc = tokenize_str((void**) &header_keys2, ",", keys2, STRING,
				&number_of_keys);
	} else {
		number_of_keys = 1;
		header_keys2 = malloc(number_of_keys * sizeof(char*));
		for (k = 0; k < number_of_keys; k++) {
			header_keys2[k] = malloc(
					(strlen("cdp") + 1) * sizeof(char));
			strcpy(header_keys2[k], "cdp");
		}
	}

	if (argc >= 7) {  //keys
		size_t keys_length = strlen(argv[6]);
		keys3 = malloc(keys_length + 1 * sizeof(char));
		strcpy(keys3, argv[6]);
		rc = tokenize_str((void**) &header_keys3, ",", keys3, STRING,
				&number_of_keys);
	} else {
		number_of_keys = 1;
		header_keys3 = malloc(number_of_keys * sizeof(char*));
		for (k = 0; k < number_of_keys; k++) {
			header_keys3[k] = malloc(
					(strlen("cdp") + 1) * sizeof(char));
			strcpy(header_keys3[k], "cdp");
		}
	}

	a_values = malloc(number_of_keys * sizeof(double));
	b_values = malloc(number_of_keys * sizeof(double));
	c_values = malloc(number_of_keys * sizeof(double));
	d_values = malloc(number_of_keys * sizeof(double));
	e_values = malloc(number_of_keys * sizeof(double));
	f_values = malloc(number_of_keys * sizeof(double));

	if (argc >= 8) { //a values
		value_length = strlen(argv[7]);
		a = malloc(value_length + 1 * sizeof(char));
		strcpy(a, argv[7]);
		rc = tokenize_str((void**) &a_values, ",", a, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			a_values[k] = 0;
		}
	}

	if (argc >= 9) { //b values
		value_length = strlen(argv[8]);
		b = malloc(value_length + 1 * sizeof(char));
		strcpy(b, argv[8]);
		rc = tokenize_str((void**) &b_values, ",", b, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			b_values[k] = 1;
		}
	}

	if (argc >= 10) { //c values
		value_length = strlen(argv[9]);
		c = malloc(value_length + 1 * sizeof(char));
		strcpy(c, argv[9]);
		rc = tokenize_str((void**) &c_values, ",", c, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			c_values[k] = 0;
		}
	}

	if (argc >= 11) { //d values
		value_length = strlen(argv[10]);
		d = malloc(value_length + 1 * sizeof(char));
		strcpy(d, argv[10]);
		rc = tokenize_str((void**) &d_values, ",", d, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			d_values[k] = 1;
		}
	}

	if (argc >= 12) { //e values
		value_length = strlen(argv[11]);
		e = malloc(value_length + 1 * sizeof(char));
		strcpy(e, argv[11]);
		rc = tokenize_str((void**) &e_values, ",", e, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			e_values[k] = 1;
		}
	}

	if (argc >= 13) { //f values
		value_length = strlen(argv[12]);
		f = malloc(value_length + 1 * sizeof(char));
		strcpy(f, argv[12]);
		rc = tokenize_str((void**) &f_values, ",", f, DOUBLE,
				&number_of_keys);
	} else {
		for (k = 0; k < number_of_keys; k++) {
			f_values[k] = 1;
		}
	}

	init_dfs_api(pool_id, container_id, allow_container_creation,
			0);
	root_obj = daos_seis_open_graph(seis_root_path, O_RDWR);
	daos_seis_change_headers(root_obj, number_of_keys, header_keys1,
			header_keys2, header_keys3, a_values, b_values,
			c_values, d_values, e_values, f_values);
	rc = daos_seis_close_graph(root_obj);
	/** no need to close dfs file it's already done while releasing
	 * segy parameters
	 */

	fini_dfs_api();
	release_tokenized_array(header_keys1, STRING, number_of_keys);
	release_tokenized_array(header_keys2, STRING, number_of_keys);
	release_tokenized_array(header_keys3, STRING, number_of_keys);
	free(a);
	free(b);
	free(c);
	free(d);
	free(e);
	free(f);
	free(a_values);
	free(b_values);
	free(c_values);
	free(d_values);
	free(e_values);
	free(f_values);
	free(pool_id);
	free(container_id);
	free(seis_root_path);

	return 0;
}
