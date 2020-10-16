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
#include "daos_seis_helpers.h"

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
seismic_root_obj_update(seis_root_obj_t *root_obj, char *dkey_name,
		        char *databuf, int size, daos_iod_t *iod);

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
 * \param[in]	traces_list	pointer the traces linked list,
 * 				new node will be created and linked to the list.
 * \param[in]	ensembles_list	pointer to ensembles linked list. New ensemble
 * 				node is created and linked to the list.
 * \param[in]	index		index of the trace, if index is zero then a new
 * 				node will be created and linked to the ensemble
 * 				list.
 * \param[in]	num_of_traces	total number of traces of each ensemble, will be
 * 				used only if the index is zero to set the number
 * 				of traces of this ensemble.
 */
void
add_trace_header(trace_t *trace, traces_list_t **traces_list,
		 ensembles_list_t **ensembles_list, int index,
		 int num_of_traces);

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
prepare_seismic_entry(seismic_entry_t *entry, daos_obj_id_t oid,
		      char *dkey, char *data, int size,
		      daos_iod_t *iod);
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
			     char *data, int size, daos_iod_t *iod);

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
				traces_metadata_t *traces_metadata,
				int daos_mode, int num_of_traces);

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
 *  \param[in]	key		string containing seismic object unique key.
 *  \param[in]	direction	only used in case of sorting to check the direction
 *  				of sorting (ascending or descending)
 *
 *  \return	array of strings containing seismic object dkeys(only gather dkeys).
 *
 */
char **
fetch_seismic_obj_dkeys(seis_obj_t *seismic_object, char *key,
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

/** Function responsible for finding ranges of traces headers.
 *  It is called while executing range headers programs.
 *
 *  \param[in]	trace_list	pointer to linked list of traces headers.
 *  \param[in]	number_of_keys	number of range keys.
 *  \param[in]	keys		array of strings containing range keys.
 *  \param[in]	dim		dim seismic flag.
 *  \param[in]	headers_ranges	pointer to previously allocated header
 *  				ranges struct.
 *
 */
void
range_traces_headers(traces_list_t *trace_list, int number_of_keys,
		     char **keys, int dim, headers_ranges_t *headers_ranges);

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

/** Function responsible for releasing allocated linked list of traces.
 *
 * \param[in]	trace_list	pointer to linked list of traces.
 *
 */
void
release_traces_list(traces_list_t *trace_list);

/** Function responsible for releasing allocated linked list of ensembles.
 *
 * \param[in]	trace_list	pointer to linked list of ensembles.
 *
 */
void
release_ensembles_list(ensembles_list_t *ensembles_list);

/** Function responsible for tokenizing seismic object dkeys list and
 *  creates an array of strings holding sorted dkeys.
 *
 *  \param[in]	object		pointer to opened seismic object.
 */
char**
tokenize_dkeys_list(seis_obj_t *object);

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

/** Function responsible for fetching array of traces headers oids
 *  of a unique gather of a specific seismic object.
 *
 *  \param[in]	root			pointer to opened root seismic object.
 *  \param[in]	seismic_object		pointer to opened seismic object.
 *  \param[in]	dkey_name		string holding the dkey of specific
 *  					gather under the seismic object.
 *  \param[in]	number_of_traces	pointer to number of traces stored
 *  					under this gather.
 *
 *  \return	array of traces headers oids of specific gather.
 *
 */
daos_obj_id_t *
get_gather_oids(seis_root_obj_t *root, seis_obj_t *seismic_object,
		char *dkey_name, int *number_of_traces);

/** Function responsible for preparing iod descriptor before updating
 *  or fetching seismic entry.
 *
 *  \param[in]	iod			Pointer to io descriptor.
 *  \param[in]	recx			Array of record extents in case of accessing
 *  					DAOS_IOD_ARRAY. otherwise set to NULL
 *  \param[in]	akey			string holding the akey of entry accessed
 *  \param[in]	type			Type of value accessed.
 *  					DAOS_IOD_SINGLE/ DAOS_IOD_ARRAY/
 *  					DAOS_IOD_NONE.
 *  \param[in]	record_size		size of single value or record size
 *  					in case of DAOS_IOD_ARRAY.
 *  \param[in]	num_of_recx_entries	number of record extents.
 *  					set to 1 in case of DAOS_IOD_SINGLE.
 */
void
prepare_iod(daos_iod_t *iod, daos_recx_t *recx, char *akey,
	    daos_iod_type_t type, daos_size_t record_size,
	    int num_of_recx_entries);
#endif /* LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_ */
