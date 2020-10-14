/*
 * daos_seis_helpers.c
 *
 *  Created on: Oct 6, 2020
 *      Author: mirnamoawad
 */
#include "daos_seis_helpers.h"

int
seismic_obj_update(daos_handle_t oh, daos_handle_t th, seismic_entry_t entry)
{

	d_sg_list_t 		sgl;
	daos_recx_t 		recx;
	daos_iod_t 		iod;
	daos_key_t 		dkey;
	d_iov_t 		sg_iovs;
	int 			rc;

	d_iov_set(&dkey, (void*) entry.dkey_name, strlen(entry.dkey_name));
	d_iov_set(&iod.iod_name, (void*) entry.akey_name,
		  strlen(entry.akey_name));

	if (entry.iod_type == DAOS_IOD_SINGLE) {
		recx.rx_nr = 1;
		iod.iod_size = entry.size;
	} else if (entry.iod_type == DAOS_IOD_ARRAY) {
		recx.rx_nr = entry.size;
		iod.iod_size = 1;
	}

	iod.iod_nr = 1;
	recx.rx_idx = 0;
	iod.iod_recxs = &recx;
	iod.iod_type = entry.iod_type;

	d_iov_set(&sg_iovs, entry.data, entry.size);

	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	sgl.sg_iovs = &sg_iovs;

	rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
	if (rc != 0) {
		err("Updating daos object failed, error code = %d\n", rc);
		return rc;
	}

	return rc;
}

void
add_gather (seis_gather_t *gather, gathers_list_t **head, int fetch)
{
	seis_gather_t		*new_gather;
	int			num_traces;

	new_gather = (seis_gather_t*) malloc(sizeof(seis_gather_t));
	new_gather->unique_key = gather->unique_key;
	new_gather->number_of_traces = gather->number_of_traces;

	if(fetch == 1) {
		num_traces = new_gather->number_of_traces
			     + 50;
	} else {
		num_traces = 50;
	}

	new_gather->oids = malloc(num_traces *
				  sizeof(daos_obj_id_t));

	memcpy(new_gather->oids, gather->oids,
	       gather->number_of_traces * sizeof(daos_obj_id_t));
	new_gather->next_gather = NULL;
	if ((*head)->head == NULL) {
		(*head)->head = new_gather;
		(*head)->tail = new_gather;
		(*head)->size++;
	} else {
		(*head)->tail->next_gather = new_gather;
		(*head)->tail = new_gather;
		(*head)->size++;
	}
}

int
check_key_value(Value target, char *key, gathers_list_t *gathers_list,
		daos_obj_id_t trace_obj_id)
{
	seis_gather_t 		*curr_gather;
	cwp_String 		 type;
	int			 ntraces;
	int 			 exists = 0;

	curr_gather = gathers_list->head;
	type = hdtype(key);
	if (curr_gather == NULL) {
		exists = 0;
		return exists;
	} else {
		while (curr_gather != NULL) {
			if (valcmp(type, curr_gather->unique_key,
				   target) == 0) {
				curr_gather->oids[curr_gather->number_of_traces]
						  	  	 = trace_obj_id;
				curr_gather->number_of_traces++;
				ntraces = curr_gather->number_of_traces;
				exists = 1;
				if (curr_gather->number_of_traces % 50 == 0) {
					curr_gather->oids = (daos_obj_id_t*)
							realloc(curr_gather->oids,
								(ntraces + 50) *
								sizeof(daos_obj_id_t));
				}
				return exists;
			} else {
				curr_gather = curr_gather->next_gather;
			}
		}
	}
	return exists;
}

int
gather_oids_array_update(trace_oid_oh_t *object,
			 seis_gather_t *gather)
{
	daos_array_iod_t 	iod;
	seismic_entry_t 	tr_entry = {0};
	daos_range_t 		rg;
	d_sg_list_t 		sgl;
	d_iov_t 		iov;
	int 			rc;

	tr_entry.data = (char*)(gather->oids);

	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	d_iov_set(&iov, (void*) (tr_entry.data), gather->number_of_traces *
		  sizeof(daos_obj_id_t));

	sgl.sg_iovs = &iov;
	iod.arr_nr = 1;
	rg.rg_len = gather->number_of_traces * sizeof(daos_obj_id_t);
	rg.rg_idx = 0;
	iod.arr_rgs = &rg;
	rc = daos_array_write(object->oh, DAOS_TX_NONE, &iod, &sgl, NULL);
	if (rc != 0) {
		err("Updating trace object ids array failed,"
		    "error code = %d \n", rc);
		return rc;
	}
	daos_array_close(object->oh, NULL);

	return rc;
}

int
pcreate(int fds[2], const char *command, char *const argv[])
{
	/** Spawn a process running the command, returning it's pid.
	 *  The fds array passed will be filled with two descriptors:
	 *  fds[0] will read from the child process, and fds[1] will
	 *  write to it. Similarly, the child process will receive a
	 *  reading/writing fd set (in that same order) as arguments.
	 */
	int	 pid;
	int 	 pipes[4];

	/** Warning: I'm not handling possible errors in pipe/fork */

	pipe(&pipes[0]); /** Parent read/child write pipe */
	pipe(&pipes[2]); /** Child read/parent write pipe */

	if ((pid = fork()) > 0) {
		/** Parent process */
		fds[0] = pipes[0];
		fds[1] = pipes[3];

		close(pipes[1]);
		close(pipes[2]);

		return pid;

	} else {
		close(pipes[0]);
		close(pipes[3]);
		dup2(pipes[2], STDIN_FILENO);
		dup2(pipes[1], STDOUT_FILENO);
		execvp(command, argv);
		exit(-1);
	}

	return -1;
}

int
execute_command(char *const argv[], char *write_buffer, int write_bytes,
		char *read_buffer, int read_bytes)
{

	/** Executes the command given in argv, which is a string array
	 * For example if command is ls -l -a
	 * argv should be {"ls, "-l", "-a", NULL}
	 * descriptors to use for read/write.
	 */
	int 		fd[2];
	/** Setup pipe, redirections and execute command. */
	int pid = pcreate(fd, argv[0], argv);
	/** Check for error. */
	if (pid == -1) {
		return -1;
	}
	/** If user wants to write to subprocess STDIN, we write here. */
	if (write_bytes > 0) {
		write(fd[1], write_buffer, write_bytes);
	}
	/** Read cycle : read as many bytes as possible or until
	 *  we reach the maximum requested by user.
	 */
	char *buffer = read_buffer;
	ssize_t bytesread = 1;

	int total_bytes = 0;
	while ((bytesread = read(fd[0], buffer, read_bytes)) > 0) {
		buffer += bytesread;
		total_bytes += bytesread;
		if (bytesread >= read_bytes) {
			break;
		}
	}
	/** Return number of bytes actually read from the STDOUT
	 *  of the subprocess.
	 */
	return total_bytes;
}

trace_t*
segy_to_trace(segy *segy_struct, daos_obj_id_t hdr_oid)
{
	trace_t		*trace;
	trace = malloc(sizeof(trace_t));
	memcpy(trace, segy_struct, TRACEHDR_BYTES);
	trace->trace_header_obj = hdr_oid;
	memcpy(trace->data, segy_struct->data, segy_struct->ns * sizeof(float));
	return trace;
}

void
sort_dkeys_list(long *values, int number_of_gathers, char **unique_keys,
		int direction)
{

	const char	*sep = KEY_SEPARATOR;
	char 		*token;
	char	       **sorted_keys;
	int 		 i;
	int 		 j = 0;
	char 		 temp_key[4096];
	long		 temp;

	sorted_keys = malloc(number_of_gathers * sizeof(char*));
	/** loop on array of keys
	 *  and set each key type (+) or (-)
	 *  and insert unique value in array of values to be sorted
	 */
	for (i = 0; i < number_of_gathers; i++) {
		strcpy(temp_key, unique_keys[j]);
		token = strtok(temp_key, sep);
		while (token != NULL) {
			token = strtok(NULL, sep);
			if (token == NULL) {
				break;
			}
			sorted_keys[j] = malloc((strlen(token) + 1) *
						sizeof(char));
			strcpy(sorted_keys[j], token);

			values[j] = atol(sorted_keys[j]);
		}
		j++;
	}
	/** Sort array of values based on direction of sorting
	 *  (ascending or descending)
	 */
	if (direction == 1) {
		for (i = 0; i < number_of_gathers; i++) {
			for (j = 0; j < number_of_gathers - i - 1; j++) {
				if (values[j] > values[j + 1]) {
					temp = values[j];
					values[j] = values[j + 1];
					values[j + 1] = temp;
				}
			}
		}
	} else {
		for (i = 0; i < number_of_gathers; ++i) {
			for (j = i + 1; j < number_of_gathers; ++j) {
				if (values[i] < values[j]) {
					temp = values[i];
					values[i] = values[j];
					values[j] = temp;
				}
			}
		}
	}

	for(i = 0; i < number_of_gathers; i++){
		free(sorted_keys[i]);
	}
	free(sorted_keys);
}

void
merge_traces(trace_t *arr, int low, int mid, int high, char **sort_key,
	     int *direction, int num_of_keys)
{
	trace_t 	*temp;
	Value 		 val1;
	Value 		 val2;
	int 		 mergePos;
	int 		 leftPos;
	int 		 rightPos;
	int 		 z;

	temp = (trace_t*) malloc((high - low + 1) * sizeof(trace_t));
	mergePos = 0;
	leftPos = low;
	rightPos = mid + 1;
	z = 1;

	while (leftPos <= mid && rightPos <= high) {
		while (z <= num_of_keys) {
			/** Get header values of key of each traces*/
			get_header_value(arr[leftPos], sort_key[z], &val1);
			get_header_value(arr[rightPos], sort_key[z], &val2);
			/** Compare values of trace headers*/
			if (valcmp(hdtype(sort_key[z]), val1, val2) == -1) {
				if (direction[z] == 1) {
					temp[mergePos++] = arr[leftPos++];
				} else {
					temp[mergePos++] = arr[rightPos++];
				}
				break;
			} else if (valcmp(hdtype(sort_key[z]), val1, val2)
				    == 1) {
				if (direction[z] == 1) {
					temp[mergePos++] = arr[rightPos++];
				} else {
					temp[mergePos++] = arr[leftPos++];
				}
				break;
			} else {
				z++;
			}
		}
	}

	while (leftPos <= mid) {
		temp[mergePos++] = arr[leftPos++];
	}

	while (rightPos <= high) {
		temp[mergePos++] = arr[rightPos++];
	}

	for (mergePos = 0; mergePos < (high - low + 1); ++mergePos) {
		arr[low + mergePos] = temp[mergePos];
	}

	free(temp);
}

void
merge_sort_traces(trace_t *arr, int low, int high, char **sort_key,
		  int *direction, int num_of_keys)
{
	int		mid;
	if (low < high) {
		mid = (low + high) / 2;

		merge_sort_traces(arr, low, mid, sort_key, direction,
				  num_of_keys);
		merge_sort_traces(arr, mid + 1, high, sort_key, direction,
				  num_of_keys);

		merge_traces(arr, low, mid, high, sort_key, direction,
			     num_of_keys);
	}
}

void
get_header_value(trace_t trace, char *sort_key, Value *value)
{
	int		i;
	for(i=0; i<SEIS_NKEYS; i++) {
		if(strcmp(sort_key, hdr[i].key) == 0) {
			seis_gethval(&trace, i, value);
			break;
		}
	}
}

void
set_header_value(trace_t *trace, char *sort_key, Value *value)
{
	int		i;
	for(i=0; i<SEIS_NKEYS; i++) {
		if(strcmp(sort_key, hdr[i].key) == 0) {
			seis_puthval(trace,i,value);
			break;
		}
	}
}

void
calculate_new_header_value(trace_node_t *current, char *key1, char *key2,
			   char *key3, double a, double b, double c, double d,
			   double e, double f, double j, int itr,
			   header_operation_type_t type,
			   cwp_String type_key1, cwp_String type_key2,
			   cwp_String type_key3)
{
	Value 		val1;
	Value 		val2;
	Value 		val3;
	double 		i;
	long 		temp;

	switch (type) {
	case SET_HEADERS:
		i = (double) itr + d;
		setval(type_key1, &val1, a, b, c, i, j);
		set_header_value(&(current->trace), key1, &val1);
		break;
	case CHANGE_HEADERS:
		get_header_value(current->trace, key2, &val2);
		get_header_value(current->trace, key3, &val3);
		changeval(type_key1, &val1, type_key2,
			  &val2, type_key3, &val3, a, b, c, d,
			  e, f);
		set_header_value(&(current->trace), key1, &val1);
		break;
	default:
		break;
	}
}

void
tokenize_str(void ***str, char *sep, char *string, int type,
	     int *number_of_keys)
{
	double 	       *temp_d;
	char 		temp[4096];
	char	      **temp_c;
	char 	       *ptr;
	long 	       *temp_l;
	int 		i = 0;
	char 		temp_keys[4096];
	char 		*token;
	strcpy(temp_keys, string);

	if (*number_of_keys == 0) {
		strcpy(temp, string);

		token = strtok(temp, sep);
		while( token != NULL ) {
			(*number_of_keys)++;
			token = strtok(NULL, sep);
		}
	}
	token = strtok(temp_keys, sep);


	while (token != NULL) {
		switch (type) {
		case 0:
			if(i==0) {
				(*str) = (char**) malloc((*number_of_keys) * sizeof(char*));
			}
			temp_c = ((char**)(*str));
			temp_c[i] = malloc((strlen(token) + 1) * sizeof(char));
			strcpy(temp_c[i], token);
			break;
		case 1:
			temp_l = *((long**)(*str));
			temp_l[i] = atol(token);
			break;
		case 2:
			temp_d = *((double**)(*str));
			temp_d[i] = strtod(token, &ptr);
			break;
		default:
			exit(0);
		}
		i++;
		token = strtok(NULL, sep);
	}
}

void
print_headers_ranges(headers_ranges_t headers_ranges)
{
	cwp_String 	key;
	cwp_String 	type;
	Value 		valmin;
	Value		valmax;
	Value		valfirst;
	Value		vallast;
	double 		dvalmin;
	double		dvalmax;
	int 		kmax;
	int 		i;

	if (headers_ranges.number_of_keys == 0) {
		kmax = SEIS_NKEYS;
	} else {
		kmax = headers_ranges.number_of_keys;
	}

	printf("%d traces: \n", headers_ranges.ntr);

	for (i = 0; i < kmax; i++) {
		get_header_value(*headers_ranges.trmin,
				 headers_ranges.keys[i], &valmin);
		get_header_value(*headers_ranges.trmax,
				 headers_ranges.keys[i], &valmax);
		get_header_value(*headers_ranges.trfirst,
				 headers_ranges.keys[i], &valfirst);
		get_header_value(*headers_ranges.trlast,
				 headers_ranges.keys[i], &vallast);
		dvalmin = vtod(hdtype(headers_ranges.keys[i]), valmin);
		dvalmax = vtod(hdtype(headers_ranges.keys[i]), valmax);
		if (dvalmin || dvalmax) {
			if (dvalmin < dvalmax) {
				printf("%s ", headers_ranges.keys[i]);
				printfval(hdtype(headers_ranges.keys[i]),
					  valmin);
				printf(" ");
				printfval(hdtype(headers_ranges.keys[i]),
					  valmax);
				printf(" (");
				printfval(hdtype(headers_ranges.keys[i]),
					  valfirst);
				printf(" - ");
				printfval(hdtype(headers_ranges.keys[i]),
					  vallast);
				printf(")");
			} else {
				printf("%s ", headers_ranges.keys[i]);
				printfval(hdtype(headers_ranges.keys[i]),
					  valmin);
			}
			printf("\n");
		}
	}

	if (headers_ranges.number_of_keys == 0) {
		if ((headers_ranges.north_shot[1] != 0.0) ||
		    (headers_ranges.south_shot[1] != 0.0) ||
		    (headers_ranges.east_shot[0] != 0.0) ||
		    (headers_ranges.west_shot[0] != 0.0)) {
			printf("\nShot coordinate limits:\n" "\tNorth(%g,%g)"
			       " South(%g,%g) East(%g,%g) West(%g,%g)\n",
			       headers_ranges.north_shot[0],
			       headers_ranges.north_shot[1],
			       headers_ranges.south_shot[0],
			       headers_ranges.south_shot[1],
			       headers_ranges.east_shot[0],
			       headers_ranges.east_shot[1],
			       headers_ranges.west_shot[0],
			       headers_ranges.west_shot[1]);
		}
		if ((headers_ranges.north_rec[1] != 0.0) ||
		    (headers_ranges.south_rec[1] != 0.0) ||
		    (headers_ranges.east_rec[0] != 0.0) ||
		    (headers_ranges.west_rec[0] != 0.0)) {
			printf("\nReceiver coordinate limits:\n"
			       "\tNorth(%g,%g) South(%g,%g) East(%g,%g)"
			       " West(%g,%g)\n", headers_ranges.north_rec[0],
			       headers_ranges.north_rec[1],
			       headers_ranges.south_rec[0],
			       headers_ranges.south_rec[1],
			       headers_ranges.east_rec[0],
			       headers_ranges.east_rec[1],
			       headers_ranges.west_rec[0],
			       headers_ranges.west_rec[1]);
		}
		if ((headers_ranges.north_cmp[1] != 0.0) ||
		    (headers_ranges.south_cmp[1] != 0.0) ||
		    (headers_ranges.east_cmp[0] != 0.0) ||
		    (headers_ranges.west_cmp[0] != 0.0)) {
			printf("\nMidpoint coordinate limits:\n"
			       "\tNorth(%g,%g) South(%g,%g) East(%g,%g)"
			       " West(%g,%g)\n", headers_ranges.north_cmp[0],
			       headers_ranges.north_cmp[1],
			       headers_ranges.south_cmp[0],
			       headers_ranges.south_cmp[1],
			       headers_ranges.east_cmp[0],
			       headers_ranges.east_cmp[1],
			       headers_ranges.west_cmp[0],
			       headers_ranges.west_cmp[1]);
		}
	}

	if (headers_ranges.dim != 0) {
		if (headers_ranges.dim == 1) {
			printf("\n2D line: \n");
			printf("Min CMP interval = %g ft\n",
			       headers_ranges.dmin);
			printf("Max CMP interval = %g ft\n",
			       headers_ranges.dmax);
			printf("Line length = %g miles (using avg CMP"
			       " interval of %g ft)\n",	headers_ranges.davg *
			       headers_ranges.ntr / 5280, headers_ranges.davg);
		} else if (headers_ranges.dim == 2) {
			printf("ddim line: \n");
			printf("Min CMP interval = %g m\n",
			       headers_ranges.dmin);
			printf("Max CMP interval = %g m\n",
			       headers_ranges.dmax);
			printf("Line length = %g km (using avg CMP interval"
			       " of %g m)\n", headers_ranges.davg *
			       headers_ranges.ntr / 1000, headers_ranges.davg);
		}
	}

	return;
}

void
val_sprintf(char *temp, Value unique_value, char *key)
{

	switch (*hdtype(key)) {
	case 's':
		(void) sprintf(temp, "%s", unique_value.s);
		break;
	case 'h':
		(void) sprintf(temp, "%d", unique_value.h);
		break;
	case 'u':
		(void) sprintf(temp, "%d", unique_value.u);
		break;
	case 'i':
		(void) sprintf(temp, "%d", unique_value.i);
		break;
	case 'p':
		(void) sprintf(temp, "%d", unique_value.p);
		break;
	case 'l':
		(void) sprintf(temp, "%ld", unique_value.l);
		break;
	case 'v':
		(void) sprintf(temp, "%ld", unique_value.v);
		break;
	case 'f':
		(void) sprintf(temp, "%f", unique_value.f);
		break;
	case 'd':
		(void) sprintf(temp, "%f", unique_value.d);
		break;
	case 'U':
		(void) sprintf(temp, "%d", unique_value.U);
		break;
	case 'P':
		(void) sprintf(temp, "%d", unique_value.P);
		break;
	default:
		err("fprintfval: unknown type %s", *hdtype(key));
	}

	return;

}

void
create_dkeys_list(seis_obj_t *object, long *gather_keys) {
	int		  z;

	merge_sort(gather_keys, 0, object->number_of_gathers - 1, 1);

	object->dkeys_list = malloc((object->number_of_gathers * sizeof(long)) +
				    (object->number_of_gathers - 1));

	char temp_key[200] = "";
	sprintf(temp_key, "%ld", gather_keys[0]);
	strcpy(object->dkeys_list, temp_key);
	strcat(object->dkeys_list, ",");
	for(z = 1; z < object->number_of_gathers; z++) {
		char temp_key[200] = "";
		sprintf(temp_key, "%ld", gather_keys[z]);
		strcat(object->dkeys_list, temp_key);
		if(z == (object->number_of_gathers - 1)) {
			break;
		}
		strcat(object->dkeys_list, ",");
	}
}

void
merge(long *gather_keys, int low, int mid, int high, int direction)
{

	int 		 mergePos;
	int 		 leftPos;
	int 		 rightPos;
	long		 *temp;

	temp = malloc((high - low + 1) * sizeof(long));

	mergePos = 0;
	leftPos = low;
	rightPos = mid + 1;
	while (leftPos <= mid && rightPos <= high) {
			/** Compare values */
			if (gather_keys[leftPos] <= gather_keys[rightPos]) {
				if (direction == 1) {
					temp[mergePos++] = gather_keys[leftPos++];
				} else {
					temp[mergePos++] = gather_keys[rightPos++];
				}
			} else if (gather_keys[leftPos] > gather_keys[rightPos]) {
				if (direction == 1) {
					temp[mergePos++] = gather_keys[rightPos++];
				} else {
					temp[mergePos++] = gather_keys[leftPos++];
				}
			}
	}

	while (leftPos <= mid) {
		temp[mergePos++] = gather_keys[leftPos++];
	}

	while (rightPos <= high) {
		temp[mergePos++] = gather_keys[rightPos++];
	}

	for (mergePos = 0; mergePos < (high - low + 1); ++mergePos) {
		gather_keys[low + mergePos] = temp[mergePos];
	}

	free(temp);
}

void
merge_sort(long *gather_keys, int low, int high, int direction)
{
	int		mid;
	if (low < high) {
		mid = (low + high) / 2;

		merge_sort(gather_keys, low, mid, direction);
		merge_sort(gather_keys, mid + 1, high, direction);

		merge(gather_keys, low, mid, high, direction);
	}

}
