/*
 * daos_seis_helpers.h
 *
 *  Created on: Oct 6, 2020
 *      Author: mirnamoawad
 */
#ifndef LSU_DAOS_SEGY_SRC_CLIENT_SEIS_DAOS_SEIS_HELPERS_H_
#define LSU_DAOS_SEGY_SRC_CLIENT_SEIS_DAOS_SEIS_HELPERS_H_

#include "su_helpers.h"
#include "dfs_helpers.h"

/** Function responsible for updating seismic objects.
 *  It is called after preparing the seismic entry struct.
 *
 * \param[in]	oh	opened seismic object connection handle
 * \param[in]	th	daos transaction handle,
 * 			if zero then it's independent transaction.
 * \param[in]	entry	pointer to seimsmic_entry struct holding
 * 			data/akey/dkey/oid/size/iod_type that will be used to update seismic object.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
seismic_obj_update(daos_handle_t oh, daos_handle_t th, seismic_entry_t entry);

/** Function responsible for adding one more gather to the linked list of
 *  existing gathers under seismic_objects.
 *  It is called if the unique header value of a trace doesn't exist to an
 *  existing gather node in the linked list of gathers.
 *
 * \param[in]	gather		pointer to the gather that will be added
 * 				to the linked list of gathers(gather_list).
 * \param[in]	head		pointer to the pointer of the gathers list,
 * 				new node will be created and linked
 * 				to this list.
 * \param[in]	fetch		boolean flag
 * 				(=0 > in case of parsing a file and creating
 * 				the linked list of gathers)
 * 				(=1 > in case of fetching gathers list under
 * 				root seismic object)
 */
void
add_gather (seis_gather_t *gather, gathers_list_t **head, int fetch);

/** Function responsible for checking gather unique value, if it exists
 *  in any of the existing gathers under seismic_objects.
 *  It is called to check if the target value exists.
 *  If Yes--> number of traces belonging to this gather is incremented by 1
 *  and the trace header object id is also added.
 *  ELSE--> it returns with false
 *
 * \param[in]	target		target value to be checked if it exists or not.
 * \param[in]	key		string containing the header key of each
 * 				seismic object to check its value.
 * \param[in]	head		pointer to linked list of gathers holding
 * 				all gathers data.
 * \param[in]	trace_obj_id	oid of the trace object.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
check_key_value(Value target, char *key, gathers_list_t *head,
		daos_obj_id_t trace_obj_id);

/** Function responsible for preparing seismic entry with trace header data
 *  and calling object update functionality.
 *  It is called to update/insert trace header data under
 *  specific trace_header_object.
 *
 * \param[in]	tr_obj		pointer to opened trace header object
 *  				to update its header.
 * \param[in]	tr		trace struct holding all trace headers and
 * 				data array, only the trace headers is written
 * 				to the trace header object.
 * \param[in]	hdrbytes	number of bytes to be updated in trace header
 *  				object(240 bytes as defined in segy.h).
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
trace_header_update(trace_oid_oh_t* tr_obj, trace_t *tr, int hdrbytes);

/** Function responsible for updating gather_TRACE_OIDS array object.
 *  It is called mainly at the end of the parsing function and
 *  only stores the traces_hdr_oids
 *
 * \param[in]	trace_data_obj	pointer to trace_oid_oh_t
 * 				struct that holds object id and open
 * 				handle of the trace TRACE_OIDS object.
 * \param[in]	gather		pointer to linked list of gathers holding
 * 				all gathers data and the array of traces oids.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
gather_oids_array_update(trace_oid_oh_t* object,
			 seis_gather_t *gather);

/** Function responsible for creating two pipes.
 *  It is called to enable reading and writing directly through the pipe
 *  No need to go for Posix system.
 *
 * \param[in]	fds		array of 2 file descriptors. fd[0] will read
 * 				from child process and the other
 * 				will write to it.
 * \param[in]	command		string containing the command to be executed.
 * \param[in]	argv		array of arguments to be passed
 * 				while executing the command.
 *
 * \return	id of the process running the command.
 *
 */
int
pcreate(int fds[2], const char *command, char *const argv[]);

/** Function responsible for executing command passed in argv
 *
 * \param[in]	argv		array of strings containing command
 * 				and the arguments.
 * \param[in]	write_buffer	byte array to be written.
 * \param[in]	write_bytes	number of bytes to be written.
 * \param[in]	read_buffer	byte array to be read into.
 * \param[in]	read_bytes	number of bytes to be read
 *
 * \return 	number of bytes actually read from STDOUT of the subprocess.
 *
 */
int
execute_command(char *const argv[], char *write_buffer, int write_bytes,
		char *read_buffer, int read_bytes);

/** Function responsible for converting the original segy struct
 *  (defined in segy.h) to the trace_t struct(modified segy struct)
 *  (defined in daos_seis_datatypes.h)
 *
 * \param[in]	segy		pointer to the segy struct.
 * \param[in]	hdr_oid	object 	id of the header object.
 *
 * \return	trace struct.
 *
 */
trace_t*
segy_to_trace(segy *segy, daos_obj_id_t hdr_oid);

/** Function responsible for sorting dkeys in ascending order.
 *  dkeys will be sorted in the array of values.
 *
 *  \param[in]	values		 	array of long values that will be set
 *  				 	and used to sort dkeys unique values.
 *  \param[in]	number_of_gathers 	seismic_object number of gathers,
 *  				  	(size of the array of values).
 *  \param[in]	unique_keys	 	array of strings holding the dkeys fetched.
 *  \param[in]	direction	 	direction of sorting
 *  				 	(1 = ascending
 *  				 	 0 = descending)
 */
void
sort_dkeys_list(long *values, int number_of_gathers, char** unique_keys,
		int direction);

/** Function responsible for merging two halves of traces headers lists
 *  based on the sorting direction.
 *  It is called while sorting headers.
 *
 *  \param[in]	arr		pointer to array of traces to be sorted.
 *  \param[in]	low		index of the first element of the array(0),
 *  				will be used to set the left position
 *  				to start from.
 *  \param[in]	mid		index of the middle element, will be used
 *  				to set the right position of the array.
 *  \param[in]	high		index of the last element of the array
 *  				(number of traces) that will be merged.
 *  \param[in]	sort_key 	array of string holding the keys which traces
 *  			 	are sorted on its value.
 *  \param[in]	direction 	direction of sorting, ascending(1) or
 *  				descending(0), will be used while
 *  				merging the two arrays.
 *  \param[in]	num_of_keys	number of sorting keys.
 */

void
Merge(trace_t *arr, int low, int mid, int high, char **sort_key,
      int *direction, int num_of_keys);

/** Function recursively called to split array of traces headers.
 *  It is called only while sorting headers.
 *
 *  \param[in]	arr		pointer to array of traces to be sorted.
 *  \param[in]	low		index of the first element of the array,
 *  				will be used to calculate the midpoint
 *  				and split the array.
 *  \param[in]	high		index of the last element of the array,
 *  				will be used to calculate the midpoint
 *  				and split the array of traces.
 *  \param[in]	sort_key 	array of strings holding the sorting keys.
 *  \param[in]	direction 	direction of sorting,
 *  				ascending(1) or descending(0).
 *  \param[in]	num_of_keys	number of sorting keys.
 *
 */
void
MergeSort(trace_t *arr, int low, int high, char **sort_key,
	  int *direction, int numof_keys);

/** Function responsible for getting trace header value
 *
 * \param[in]	trace		trace struct to get a specific header value.
 * \param[in]	sort_key	Key header to get its value.
 * \param[in]	value		value of the header to be returned.
 *
 */
void
get_header_value(trace_t trace, char *sort_key, Value *value);

/** Function responsible for setting trace header value
 *
 * \param[in]	trace		pointer to trace struct to set
 * 				a specific header value.
 * \param[in]	sort_key	Key header to set its value.
 * \param[in]	value		value of the header to be written to the
 * 				trace.
 *
 */
void
set_header_value(trace_t *trace, char *sort_key, Value *value);

/** Function responsible for calculating new trace header value
 *
 * \param[in]	current	       pointer to trace struct to set its header value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in]	keys_2	       array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in]	keys_3         array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in] 	a	       array of doubles containing values on first
 * 			       trace(set_headers case)
 * 			       or overall shift(change headers case)
 * \param[in] 	b	       array of doubles containing increments values
 * 			       within group(set_headers case)
 * 			       or scale on first input key(change headers case)
 * \param[in] 	c	       array of doubles containing group increments
 * 			       (set_headers case) or scale on second input key
 * 			       (change headers case)
 * \param[in] 	d	       array of doubles containing trace number shifts
 * 			       (set_headers case) or overall scale
 * 			       (change headers case)
 * \param[in] 	j	       array of doubles containing number of elements
 * 			       in group (set headers case only)
 * \param[in]	e	       array of doubles containing exponent on first
 * 			       input key(change headers case only)
 * \param[in] 	f	       array of doubles containing exponent on second
 * 			       input keys(change headers case only)
 * \param[in]   type           type of operation requested whether it is set
 * 			       headers or change headers defined in the enum
 * 			       header_operation_type_t
 * 			       (SET_HEADERS/ CHANGE_VALUES).
 * \param[in]	type_key1      header type of first key.
 * \param[in]	type_key2      header type of second key.
 * \param[in]	type_key3      header type of third key.
 *
 */
void
calculate_new_header_value(traces_headers_t *current, char *key1, char *key2,
			   char *key3, double a, double b, double c, double d,
			   double e, double f, double j, int itr,
			   header_operation_type_t type, cwp_String type_key1,
			   cwp_String type_key2, cwp_String type_key3);

/** Function responsible for tokenizing a string given a separator
 *
 * \param[in]	str			void double pointer which will be
 * 					casted to one of the types requested.
 * \param[in]	sep			character separator that will be used
 * 					in tokenizing the string.
 * \param[in]	string			string that will be tokenized.
 * \param[in]	type			integer specifying the type the string
 * 					will be casted to (char/ double/ long)
 * \param[in]	number_of_keys		integer pointer to the number of keys
 * 					to be set.
 *
 */
void
tokenize_str(void ***str, char *sep, char *string, int type, int *number_of_keys);

/** Function responsible for printing the ranges of the traces headers.
 *
 *  \param[in]	headers_ranges	struct holding min/ max/ first/ last
 *  				traces ranges, array of keys,
 *  				number of keys, shot/rec/cmp coordinates,...
 *
 */
void
print_headers_ranges(headers_ranges_t headers_ranges);

/** Fucntion responsible for storing the unique value in character array
 *  based on its type (char/long/double/int/...)
 *
 * \param[in]	temp		character array to store the unique value in.
 * \param[in]	unique_value	value that will be written in a character array.
 * \param[in]	key		string containing the key used,
 * 				will be used to find the data type.
 *
 *  \return	the character array after writing the unique value in it.
 *
 */
void
val_sprintf(char *temp, Value unique_value, char *key);

#endif /* LSU_DAOS_SEGY_SRC_CLIENT_SEIS_DAOS_SEIS_HELPERS_H_ */
