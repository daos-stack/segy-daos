/*
 * daos_seis_internal_functions.h
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#ifndef LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_
#define LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_

#include "su_helpers.h"
#include "dfs_helpers.h"
#include "daos_seis_datatypes.h"

/** Function responsible for fetching all seismic_root object entries
 *  (seismic_gather object ids/number_of_traces/...)
 *  It is called once at the beginning of the program.
 *
 * \param[in]   dfs             pointer to mounted DAOS file system.
 * \param[in]   root            pointer to opened root dfs object.
 *
 * \return      pointer to seismic root object
 */
seis_root_obj_t*
fetch_seismic_root_entries(dfs_t *dfs, dfs_obj_t *root);

/** Function responsible for finding the parent dfs object of file
 *  given its absolute path.
 *  It is called once at the beginning of seismic_object_creation.
 *  Function.
 *
 * \param[in]   dfs             pointer to mounted DAOS file system.
 * \param[in	file_directory  absolute path of file to find its parent object.
 * \param[in]	allow_creation  boolean flag to allow creation of directories
 * 				in the path in case they doesn't exist.
 * \param[in]	file_name	array of characters containing name of the
 * 				file.
 * \param[in]	verbose_output	boolean flag to enable verbosity.
 *
 * \return	pointer to opened parent dfs object, seismic root object
 * 		will be created later in the parse_segy_file
 * 		under the opened parent
 */
dfs_obj_t*
dfs_get_parent_of_file(dfs_t *dfs, const char *file_directory,
		       int allow_creation, char *file_name,
		       int verbose_output);

/** Function responsible for fetching seismic entry(data stored under
 *  specific seismic object)
 *  -Set dkey buf→  (name) and buffer length → (size of name)
 *  -Set IOdescriptor name (AKEY) and its length
 *  -Set IOdescriptor number of entries =1 (How many elements will exist
 *  in the array of extents,(1 if single value)).
 *  -Set index of the first record in the extent.
 *  -Set number of contiguous records in the extent starting from the
 *  index set in previous step (size of mode/ctime/atime/mtime/chunck size).
 *  -Make IOdescriptor array of extents point to the allocated array of extents.
 *  -Set IOdescriptor type value to DAOS_IOD_array/ DAOS_IOD_SINGLE.
 *  -Set IOdescriptor size of each record in the array of extents.
 *  -Set sg_iovs(scatter gather iovector)
 *
 * \param[in]	oh		opened seismic object connection handle
 * \param[in]	th		daos transaction handle, if zero then
 * 				it's independent transaction.
 * \param[in]	entry		pointer to seismic_entry struct holding
 * 				data/akey/dkey/oid/size/iod_type that will be used
 * 				to fetch the seismic entry.
 * \param[in]	ev		Completion event, it's optional & can be NULL.
 *				The function will run in blocking
 *				mode if \a ev is NULL.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
fetch_seismic_entry(daos_handle_t oh, daos_handle_t th,
		      seismic_entry_t *entry, daos_event_t *ev);

/** Function responsible for creating seismic root object under root dfs
 *  (if parent is passed NULL) or specific dfs object(directory)
 *  It is called before parsing segy file in seismic_obj_creation or
 *  before parsing bare traces file in add_headers.
 *
 * \param[in]	dfs		pointer to mounted daos file system.
 * \param[in]	obj		pointer to pointer to the seimsic root object to be created.
 * \param[in]	cid		DAOS object class id (0 for default MAX_RW).
 * \param[in]	name		string containing the name of the root
 * 				object to be created.
 * \param[in]	parent		pointer to the opened parent(dfs object)
 * 				of the new seismic root object.
 * \param[in]	num_of_keys	Number of strings(keys) in the array of keys.
 * \param[in]	keys		array of strings containing header_keys that
 * 				will be used to create gather objects.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
seismic_root_obj_create(dfs_t *dfs, seis_root_obj_t **obj,
			  daos_oclass_id_t cid,	char *name, dfs_obj_t *parent,
			  int num_of_keys, char **keys);

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

/** Function responsible for preparing seismic entry to update keys stored
 *  under root seismic object.
 *
 * \param[in]	root_obj	pointer to opened root seismic_object.
 * \param[in]	dkey_name	string containing name of the dkey that will be
 * 				used to update the value of specific akey under
 * 				the root seismic object. dkey may already exist,
 * 				otherwise it will be created.
 * \param[in]	akey_name	string containing name of the akey that will be
 * 				used to update its value. akey may already exist,
 * 				otherwise will be created.
 * \param[in]	databuf		buffer holding the data that will be written
 * 				under specific entry in root seismic object.
 * \param[in]	nbytes		number of bytes that will be written from the
 * 				the data buffer under the dkey and akey in the
 * 				seismic root object.
 * \param[in]	iod_type	type of the value accessed in the IO descriptor
 * 				(dkey/ one value to be updated atomically/
 * 				or array of records).
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
seismic_root_obj_update(seis_root_obj_t* root_obj, char* dkey_name,
		      char* akey_name , char* databuf, int nbytes,
		      daos_iod_type_t iod_type);

/** Function responsible for merging two traces lists by making tail
 *  of one list points to the head of the other.
 *  It is called only in sorting function after sorting a
 *  subgroup of headers.
 *
 *  \param[in]	headers		pointer to pointer to the traces list
 *  				(first part of the linked list)
 *  \param[in]	temp_list	pointer to pointer to the other part
 *  				of the traces list that will be linked.
 *
 */
void
merge_trace_lists(traces_list_t **headers, traces_list_t **temp_list);

/** Function responsible for adding a new trace header to
 *  existing linked list of headers. It is called after sorting traces headers.
 *  Mainly used to copy traces headers from array of read_traces
 *  (that was used in sorting) to linked list of traces.
 *
 * \param[in]	trace		pointer to the trace header struct that will be
 * 				added to the linked list of traces(trace_list).
 * \param[in]	head		pointer to pointer of the traces linked list,
 * 				new node will be created and linked to the list.
 */
void
add_trace_header(trace_t *trace, traces_list_t **head);

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

/** Function responsible for updating gather keys at the end of parsing function.
 *  It writes the number_of_traces key(akey) under each gather(dkey).
 *  It writes the oid of the DAOS_ARRAY object holding the header_traces oids.
 *  It writes the array of OIDS_HDR_traces to the DAOS_ARRAY OBJECT.
 *
 * \param[in]	dfs		pointer to the mounted daos file system.
 * \param[in]	head		pointer to linked list of gathers holding
 * 				all gathers data.
 * \param[in]	object		pointer to opened seismic object to be updated.
 * \param[in]	dkey_name	string containing the prefix of the gather dkey.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
update_gather_data(dfs_t *dfs, gathers_list_t *head, seis_obj_t *object,
		   char *dkey_name);

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

/** Function responsible for creating trace_OIDS array object.
 *  It is called once after creating/ updating linked list of object gathers.
 *  It creates number of array objects equal to the number of gathers
 *  that will be stored under seismic object.
 *
 * \param[in]	dfs	 	 pointer to mounted daos file system.
 * \param[in]	cid	  	 DAOS object class id
 * 				 (pass 0 for default MAX_RW).
 * \param[in]	seis_obj 	 pointer to seismic object, array of trace oids
 * 				 will be created for each gather.
 * \param[in]	num_of_gathers	 integer holding the number of gathers that
 * 				 previously exist in the linked list of gathers.
 * 				 (passed 0 in case of parsing first segy file).
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
trace_oids_obj_create(dfs_t* dfs, daos_oclass_id_t cid,
		      seis_obj_t *seis_obj, int num_of_gathers);

/** Function responsible for creating seismic gather objects.
 *
 * \param[in]	dfs	 pointer to mounted daos file system.
 * \param[in]	cid	 DAOS object class id (pass 0 for default MAX_RW).
 * \param[in]	parent	 pointer to opened root seismic_object.
 * \param[in]	obj	 double pointer to the seismic object to be allocated.
 * \param[in]	key	 string containing the name of the seismic object to be
 * 			 created and linked to the root seismic object.
 * \param[in]	index	 index of the seismic object header key in the
 * 			 list of keys stored.
 *
 * \return      0 on success
 * 		error_code otherwise
 *
 */
int
seismic_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid,
			  seis_root_obj_t *parent, seis_obj_t **obj,
			  char* key, int index);

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

/** Function responsible for writing trace data as DAOS_ARRAY under
 *  specific trace data object. It is called to update/insert trace
 *  data under specific trace_data_object.
 *
 *  \param[in]	trace_data_obj	pointer to trace_oid_oh_t struct that holds
 *  				object id and open handle of trace data object.
 *  \param[in]	trace		trace struct holding all trace headers and data
 *  				array, only the data array is written to the
 *  				trace data object.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
trace_data_update(trace_oid_oh_t* trace_data_obj, segy *trace);

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

/** Function responsible for calculating the object id of trace_data_object
 *  from object_id of trace_header_object.
 *  It is called before reading from or writing to the trace_data_object.
 *
 * \param[in]	tr_hdr	oid of the trace header object.
 * \param[in]	cid	DAOS object class id (pass 0 for default MAX_RW).
 *
 * \return		object id of the trace data object.
 *
 */
daos_obj_id_t
get_trace_data_oid(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid);

/** Function responsible for creating trace_header_object & trace_data_object
 *  It is called once for each trace while parsing the segy_file.
 *  It also writes the trace header(240 bytes) to trace header object
 *  and data array to the trace data object.
 *
 * \param[in]	dfs		pointer to mounted daos file system.
 * \param[in]	trace_hdr_obj	double pointer to the trace header object to be
 * 				allocated.
 * \param[in]	index		Integer holding the index of the trace object
 * 				to be created.
 * \param[in]	trace		segy struct holding the trace headers and data.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
trace_obj_create(dfs_t* dfs, trace_obj_t **trace_hdr_obj, int index,
			segy *trace);

/** Function responsible for preparing the seismic entry.
 * It is called before reading from or writing to any seismic object.
 *
 * \param[in]	entry		pointer to the seismic entry to be prepared.
 * \param[in]	oid		object id of the seismic object that will be
 * 				accessed.
 * \param[in]	dkey		string containing dkey that will be used in
 * 				fetching/ updating the seismic object.
 * \param[in]	akey		string containing akey that will be used in
 * 				fetching/ updating its value.
 * \param[in]	data		buffer holding the data to be updated or
 * 				holds the data fetched from the seismic object.
 * \param[in]	size		Integer containing size(number of bytes) to be
 * 				fetched/updated in the seismic object.
 * \param[in]	iod_type	type of the value accessed in the IO descriptor
 * 				(dkey/one value to be updated atomically/
 * 				or array of records).
 *
 */
void
prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid,
		      char *dkey, char *akey, char *data,int size,
		      daos_iod_type_t iod_type);

/** Function responsible for updating any gather object.
 *
 * \param[in]	seis_obj 	pointer to opened seismic object to be updated.
 * \param[in]	dkey_name	string containing name of the dkey to be used
 * 				while updating the seismic gather object.
 * \param[in]	akey_name	string containing name of the akey to be used
 * 				to update its value.
 * \param[in]	data		data buffer that will be written.
 * \param[in]	nbytes		number of bytes that will be written to the
 * 				seismic gather object.
 * \param[in]	iod_type	type of the value accessed in the IO descriptor
 * 				(dkey/one value to be updated atomically/
 * 				or array of records).
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
update_seismic_gather_object(seis_obj_t *gather_obj, char *dkey_name,
			     char *akey_name, char *data, int nbytes,
			     daos_iod_type_t type);

/** Function responsible for linking each trace to the seismic object gathers.
 *  It is called once while creating the trace header & data objects.
 *  Also called while replacing objects if needed.
 *
 *  \param[in]	trace_obj	pointer to the trace object that will be linked
 *  				to a specific object gather.
 *  \param[in]	seis_obj	pointer to the seismic object to which the
 *  				trace will be linked based on the unique
 *  				value of the key.
 *  \param[in]	key		string containing key to get its unique value.
 *
 *  \return     0 on success
 * 		error_code otherwise
 */
int
trace_linking(trace_obj_t* trace_obj, seis_obj_t *seis_obj, char *key);

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

/** Function responsible for converting the trace struct back
 *  to the original segy struct(defined in segy.h)
 *
 * \param[in]	trace	pointer to the trace struct that will be converted
 *
 * \return	segy struct.
 *
 */
segy*
trace_to_segy(trace_t *trace);

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

/** Function responsible for fetching traces headers in
 *  read_traces struct(array of traces).
 *  It is only used in sorting headers function.
 *
 * \param[in]	coh		opened container handle
 * \param[in]	oids		array holding trace_headers oids to be fetched.
 * \param[in]	traces 		pointer to allocated array of traces.
 * \param[in]	daos_mode 	integer specifying daos object mode
 * 				(read only or read/write).
 *
 */
void
fetch_traces_header_read_traces(daos_handle_t coh, daos_obj_id_t *oids,
				read_traces *traces, int daos_mode);

/** Function responsible for fetching traces headers to traces list
 *  struct (linked list of traces)
 *  It is called in get headers and window functions.
 *
 * \param[in]	coh		opened container handle
 * \param[in]	oids		array holding trace_headers oids to be fetched.
 * \param[in]	head_traces 	pointer to the linked list of traces.
 * \param[in]	daos_mode 	daos object mode(read only or read/write).
 * \param[in]	num_of_traces	number of traces to fetch their headers.
 *
 */
void
fetch_traces_header_traces_list(daos_handle_t coh, daos_obj_id_t *oids,
				traces_list_t **head_traces, int daos_mode,
				int num_of_traces);

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

/** Function responsible for sorting traces headers.
 *  It is called while sorting headers and internally merge sort the headers
 *  based on their key value.
 *
 * \param[in]	gather_traces	pointer to gather array of traces to be sorted.
 * \param[in]	sort_key	array of strings holding primary and secondary
 * 				sorting keys.
 * \param[in]	direction	array of sorting directions
 * 				(1 = ascending
 * 				 0 = descending).
 * \param[in]	number_of_keys	number of sorting keys.
 *
 */
void
sort_headers(read_traces *gather_traces, char **sort_key, int *direction,
	     int number_of_keys);

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

/** Function responsible for getting dkey name given specific key
 *
 * \param[in]	key	string containing header name.
 *
 * \return	string containing the dkey of the header passed.
 */
char*
get_dkey(char *key);

/** Function responsible for calculating and setting trace header value
 *  It is called while setting/ changing traces headers values
 *
 * \param[in]	coh	       container open handle
 * \param[in]	daos_mode      daos object mode(read only or read/write).
 * \param[in[	head	       pointer to linked list of traces.
 * \param[in]   num_of_keys    number of header keys to set their value.
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
 */
void
set_traces_header(daos_handle_t coh, int daos_mode, traces_list_t **head,
		  int num_of_keys, char **keys_1, char **keys_2, char **keys_3,
		  double *a, double *b, double *c, double *d, double *e,
		  double *f, double *j, header_operation_type_t type);

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

/** Function responsible for windowing traces based on min and max of some keys
 *  It is called while executing daos_seis_window function.
 *
 *  \param[in]	head		pointer to linked list of traces headers.
 *  \param[in]	window_keys	array of strings containing window header keys.
 *  \param[in]	num_of_keys	number of keys in the array of window keys.
 *  \param[in]	type		type of window header keys
 *  				as defined in su_helpers.h
 *  \param[in]  min_keys        array of VALUE(su_helpers.h) containing window
 *  				header keys minimum values based on key type.
 *  \param[in]  max_keys        array of VALUE(su_helpers.h) containing window
 *  				header keys maximum values based on key type.
 *
 */
void
window_headers(traces_list_t **head, char **window_keys, int number_of_keys,
	       cwp_String *type, Value *min_keys, Value *max_keys);

/** Function responsible for fetching dkeys stored under seismic object
 *  and optionally sort dkeys in ascending or descending order.
 *
 *  \param[in]	seismic_object	pointer to opened seismic object.
 *  \param[in]	sort		sorting flag, if set then dkeys will be sorted.
 *  \param[in]	key		string containing seismic object unique key.
 *  \param[in]	direction	only used in case of sorting to check the direction
 *  				of sorting (ascending or descending)
 *
 *  \return	array of strings containing seismic object dkeys(only gather dkeys).
 *
 */
char **
fetch_seismic_obj_dkeys(seis_obj_t *seismic_object, int sort, char *key,
		      	int direction);

/** Function responsible for destroying existing seismic object
 *  and creating new one.
 *  It creates new object after destroying all array objects holding
 *  traces headers oids.
 *  It links all traces again to the newly created object.
 *  It is only called if the unique value of existing seismic object is changed.
 *
 *  \param[in]	dfs			pointer to mounted DAOS file system
 *  \param[in]	daos_mode		daos object mode
 *  					(read only or read/write).
 *  \param[in]	key			string containing the name of the key
 *  					gather that will be replaced.
 *  \param[in]	trace_list		pointer to a linked list of traces with
 *  					updated header values. Each trace will
 *  					be linked to a gather in the newly
 *  					created seismic object.
 *  \param[in]	root			pointer to opened root seismic object.
 */
void
replace_seismic_objects(dfs_t *dfs, int daos_mode, char *key,
			traces_list_t *trace_list, seis_root_obj_t *root);

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

/** Function responsible for finding ranges of traces headers.
 *  It is called while executing range headers programs.
 *
 *  \param[in]	trace_list	pointer to linked list of traces headers.
 *  \param[in]	number_of_keys	number of range keys.
 *  \param[in]	keys		array of strings containing range keys.
 *  \param[in]	dim		dim seismic flag.
 *
 *  \return	after finding headers ranges,
 *  		return a struct of all key headers ranges'.
 *
 */
headers_ranges_t
range_traces_headers(traces_list_t *trace_list, int number_of_keys,
		     char **keys, int dim);

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

/** Function responsible for fetching array of traces headers object ids
 *
 * \param[in]	root			pointer to opened seismic root object.
 * \param[in]	oids			pointer to allocated array of
 * 					traces headers oids.
 * \param[in] 	gather_oid_oh		pointer to struct holding object id
 * 					and open handle of the array object.
 * \param[in]	number_of_traces	number of header object ids
 * 					that will be fetched.
 *
 *  \return      0 on success
 *  		 error_code otherwise
 */
int
fetch_array_of_trace_headers_oids(seis_root_obj_t *root, daos_obj_id_t *oids,
				  trace_oid_oh_t *gather_oid_oh,
				  int number_of_traces);

/** Function responsible for releasing allocated list of gathers
 *
 * \param[in]	gather_list	pointer to linked list of gathers.
 *
 */
void
release_gathers_list(gathers_list_t *gather_list);

/** Function responsible for reading binary and text headers from segy file.
 *
 *  \param[in]	bh		pointer to binary header struct to hold
 *  				binary header data read
 *  \param[in]	ebcbuf		pointer to character array to be filled with
 *  				text header data read.
 *  \param[in]	nextended	pointer to variable holding number of extended
 *  				text headers in segy file.
 *  \param[in]	root_obj	pointer to opened root seismic object.
 *  \param[in]	daos_tape	pointer to daos file struct holding the segy
 *  				file handle after mounting to dfs and offset
 *  				of the last accessed byte defined in
 *  				(dfs_helper_api.h).
 *  \param[in]	swapbhed	integer indicating whether to swap binary
 *  				header bytes or not.
 *  \param[in]	endian		integer indicating little or big endian flag.
 *
 */
void
read_headers(bhed *bh, char *ebcbuf, short *nextended,
	     seis_root_obj_t *root_obj,	DAOS_FILE *daos_tape,
	     int swapbhed, int endian);
/** Function responsible for writing binary and text headers under
 *  root seismic object.
 *
 *  \param[in]	bh		binary header struct to be written
 *  				under root seismic object.
 *  \param[in]	ebcbuf		pointer to character array to be written
 *  				under root seismic object.
 *  \param[in]	root_obj	pointer to opened root seismic object.
 *
 */
void
write_headers(bhed bh, char *ebcbuf, seis_root_obj_t *root_obj);

/** Function responsible for parsing extended text headers from segy file.
 *
 *  \param[in]	nextended	number of extended text headers to be parsed.
 *  \param[in]	daos_tape	pointer to daos file struct holding the segy
 *  				file handle after mounting to dfs and offset
 *  				of the last accessed byte defined in
 *  				(dfs_helper_api.h).
 *  \param[in]	ebcbuf		pointer to character array to hold extended
 *  				text header data read.
 *  \param[in]	root_obj	pointer to opened root seismic object.
 *
 */
void
parse_exth(short nextended, DAOS_FILE *daos_tape, char *ebcbuf,
	   seis_root_obj_t *root_obj);

/** Function responsible for processing headers and calculating length of trace
 *
 * \param[in]	bh		pointer to binary header struct.
 * \param[in]	format		integer flag to specify override format.
 * \param[in]	over		integer flag for binary header float override.
 * \param[in]	format_set	boolean flag.
 * \param[in]	trcwt		integer flag for trace weighting.
 * \param[in]	verbose		verbosity flag
 * \param[in]	ns		integer to trace number of samples to be set.
 * \param[in]	nsegy		integer to trace size in bytes to be set.
 *
 */
void
process_headers(bhed *bh, int format, int over,cwp_Bool format_set, int *trcwt,
		int verbose, int *ns, size_t *nsegy);

/** Function responsible for processing trace.
 *
 * \param[in]	tapetr		tape segy struct holding trace
 * 				identification header.
 * \param[in]	tr		pointer to segy struct, tapetr will be
 * 				converted to trace struct.
 * \param[in]	bh		binary header struct fetched before.
 * \param[in]	ns		trace number of samples.
 * \param[in]	swaphdrs	integer flag for big(1) and little(0) endian to
 * 				indicate whether to swap trace headers or not.
 * \param[in]	nsflag		integer flag for error in tr.ns
 * \param[in]	itr		current trace number.
 * \param[in]	nkeys		number of keys to be computed.
 * \param[in]	type1		array of types for key1.
 * \param[in]	type2		array of types for key2.
 * \param[in]	ubyte		array of starting bytes of type2 array.
 * \param[in]	endian		integer flag specifying little or big endian.
 * \param[in]	conv		integer flag for data conversion.
 * \param[in]	swapdata	integer flag for big(1) and little(0) endian to
 * 				indicate whether to swap trace data or not.
 * \param[in]	index1		array of indexes for key1.
 * \param[in]	trmin		first trace to read.
 * \param[in]	trcwt		flag for trace weighting.
 * \param[in]	verbose		verbosity flag.
 *
 */
void
process_trace(tapesegy tapetr, segy *tr, bhed bh, int ns, int swaphdrs,
	      int nsflag, int *itr, int nkeys, cwp_String *type1,
	      cwp_String *type2, int *ubyte, int endian, int conv,
	      int swapdata, int *index1, int trmin, int trcwt,
	      int verbose);

/** Function responsible for reading all gathers stored under seismic
 *  object in a linked list of gathers.
 *
 *  \param[in]	root		pointer to opened root seismic object.
 *  \param[in]	seis_obj	pointer to opened seismic object.
 */
void
read_object_gathers(seis_root_obj_t *root, seis_obj_t *seis_obj);

#endif /* LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_ */
