/*
 * daos_seis.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

#include "daos_seis.h"

#include <daos/event.h>

#define GET_MACRO(_1,_2,_3,_4,NAME,...) NAME
#define warn(...) GET_MACRO(__VA_ARGS__, warn4, warn3, warn2, warn1)(__VA_ARGS__)
#define warn1(x) fprintf(stderr,x)
#define warn2(x,y) fprintf(stderr,x,y)
#define warn3(x,y,z) fprintf(stderr,x,y,z)
#define warn4(x,y,z,u) fprintf(stderr,x,y,z,u)

#define err(...) GET_MACRO(__VA_ARGS__, err4, err3, err2, err1)(__VA_ARGS__)
#define err1(x) fprintf(stderr,x);exit(0)
#define err2(x,y) fprintf(stderr,x,y);exit(0)
#define err3(x,y,z) fprintf(stderr,x,y,z);exit(0)
#define err4(x,y,z,u) fprintf(stderr,x,y,z,u);exit(0)

int
daos_seis_close_root(seis_root_obj_t *segy_root_object)
{
	dfs_obj_t	*obj;
	int 		 rc;
	int 		 i;

	obj = segy_root_object->root_obj;

	for(i = 0; i < segy_root_object->num_of_keys; i++) {
		free(segy_root_object->keys[i]);
	}
	free(segy_root_object->keys);
	free(segy_root_object->gather_oids);
	/** Release daos file system object of the seismic root object. */
	rc = dfs_release(obj);
	if (rc != 0) {
		err("Releasing seismic root dfs object failed, error code"
			" = %d\n", rc);
	}

	/** Free the allocated seismic root object itself. */
	free(segy_root_object);

	return rc;
}

seis_root_obj_t*
daos_seis_open_root_path(dfs_t *dfs, const char *root_path)
{
	seis_root_obj_t 	*root_obj;
	dfs_obj_t 		*root;
	struct stat 		 stbuf;
	mode_t 			 mode;
	int 		 	 rc;

	/** Lookup and open the dfs object containing the root seismic object **/
	rc = dfs_lookup(dfs, root_path, O_RDWR, &root, &mode, &stbuf);
	if (rc != 0) {
		err("Looking up path<%s> in dfs failed , error code = %d \n",
		    root_path, rc);
		exit(rc);
	}

	/** Fetch parameters of the seismic root object **/
	root_obj = fetch_seismic_root_entries(dfs, root);

	return root_obj;
}

int
daos_seis_get_trace_count(seis_root_obj_t *root)
{
	return root->number_of_traces;
}

int
daos_seis_get_number_of_gathers(seis_root_obj_t *root, char *key)
{
	seismic_entry_t		seismic_entry = { 0 };
	seis_obj_t 	       *seismic_object;
	daos_iod_t		iod;
	int			number_of_gathers;
	int 			daos_mode;
	int 			rc;
	int 			i;

	daos_mode = get_daos_obj_mode(O_RDWR);
	seismic_object = malloc(sizeof(seis_obj_t));

	for(i = 0; i< root->num_of_keys; i++){
		if(strcmp(root->keys[i], key) == 0) {
			seismic_object->oid = root->gather_oids[i];
			break;
		}
	}

	rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
			   &seismic_object->oh, NULL);
	if (rc != 0) {
		err("Opening daos seismic obj <%s> failed, error"
		    " code = %d \n", key, rc);
		return rc;
	}

	prepare_iod(&iod, NULL, DS_A_NGATHERS, DAOS_IOD_SINGLE,
		    sizeof(int), 1);

	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS,
			      (char*)&seismic_object->number_of_gathers,
			      sizeof(int), &iod);
	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
			 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers under <%s> object failed, "
		    "error code = %d \n", key, rc);
		return rc;
	}
	number_of_gathers = seismic_object->number_of_gathers;

	rc = daos_obj_close(seismic_object->oh, NULL);
	if (rc != 0) {
		err("Closing <%s> object failed, error code = %d \n", key, rc);
		return rc;
	}
	free(seismic_object);

	return number_of_gathers;
}

bhed*
daos_seis_get_binary_header(seis_root_obj_t *root)
{
	seismic_entry_t 	entry = { 0 };
	daos_recx_t		*recx;
	daos_iod_t		iod;
	bhed 		       *bhdr;
	int 			rc;
	/** allocate binary header struct */
	bhdr = malloc(sizeof(bhed));

	recx = malloc(sizeof(daos_recx_t));
	recx->rx_idx = 0;
	recx->rx_nr = SEIS_BNYBYTES;

	prepare_iod(&iod, recx, DS_A_BINARY_HEADER, DAOS_IOD_ARRAY,
		    1, 1);

	/** prepare seismic entry & fetch binary header from root object */
	prepare_seismic_entry(&entry, root->root_obj->oid, DS_D_FILE_HEADER,
			      (char*) bhdr, SEIS_BNYBYTES, &iod);

	rc = fetch_seismic_entry(root->root_obj->oh, DAOS_TX_NONE, &entry,
			 NULL);
	if (rc != 0) {
		err("Fetching binary header data failed, error"
		    " code = %d \n", rc);
		exit(rc);
	}
	free(recx);
	return bhdr;
}

char*
daos_seis_get_text_header(seis_root_obj_t *root)
{
	seismic_entry_t 	entry = {0};
	daos_recx_t		*recx;
	daos_iod_t		iod;
	char 		       *text_header;
	int	 		rc;

	text_header = malloc(SEIS_EBCBYTES * sizeof(char));
	recx = malloc(sizeof(daos_recx_t));
	recx->rx_idx = 0;
	recx->rx_nr = SEIS_EBCBYTES;

	prepare_iod(&iod, recx, DS_A_TEXT_HEADER, DAOS_IOD_ARRAY,
		    1, 1);

	/** prepare seismic entry & fetch text header from root object */
	prepare_seismic_entry(&entry, root->root_obj->oid, DS_D_FILE_HEADER,
			      text_header, SEIS_EBCBYTES, &iod);
	rc = fetch_seismic_entry(root->root_obj->oh, DAOS_TX_NONE, &entry,
			 NULL);
	if (rc != 0) {
		err("Fetching textual header data failed, error"
		    " code = %d \n", rc);
		exit(rc);
	}
	free(recx);
	return text_header;
}

traces_metadata_t*
daos_seis_get_shot_traces(int shot_id, seis_root_obj_t *root)
{
	traces_metadata_t	*shot_metadata;
	Value 			 min;
	char 			*key;
	char 			*type;

	min.i = shot_id;
	key = malloc((strlen("fldr") + 1) * sizeof(char));
	strcpy(key, "fldr");
	type = hdtype(key);

	shot_metadata = daos_seis_wind_traces(root, &key, 1, &min, &min, &type);

	free(key);
	return shot_metadata;
}

int
daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *segy_root, seis_root_obj_t *root_obj,
		     seis_obj_t **seismic_obj, int additional)
{
	DAOS_FILE 		*daos_tape;
	char 			*temp_name;
	int 			rc;
	int 			i;

	daos_tape = malloc(sizeof(DAOS_FILE));

	daos_tape->file = segy_root;
	daos_tape->offset = 0;

	/* Globals */
	tapesegy tapetr;
	tapebhed tapebh;
	segy tr;
	bhed bh;

	/* Declare variables that will be parsed from commandline */
	/**********************************************************/
	/* Required */
	char *tape; /* name of raw tape device	*/

	int tapefd = 0; /* file descriptor for tape	*/

	FILE *tapefp = NULL; /* file pointer for tape	*/
	FILE *binaryfp; /* file pointer for bfile	*/
	FILE *headerfp; /* file pointer for hfile	*/
	FILE *xheaderfp; /* file pointer for xfile	*/
	FILE *pipefp; /* file pointer for popen write */

	size_t nsegy; /* size of whole trace in bytes		*/
//	int i; /* counter				*/
	int itr; /* current trace number			*/
	int trmin; /* first trace to read			*/
	int trmax; /* last trace to read			*/
	int ns; /* number of data samples		*/

	int over; /* flag for bhed.float override		*/
	int format; /* flag for to specify override format	*/
	int result;
	cwp_Bool format_set = cwp_false;
	/* flag to see if new format is set	*/
	int conv; /* flag for data conversion		*/
	int trcwt; /* flag for trace weighting		*/
	int verbose = 0; /* echo every ...			*/
	int vblock = 50; /* ... vblock traces with verbose=1	*/
	int buff; /* flag for buffered/unbuffered device	*/
	int endian; /* flag for big=1 or little=0 endian	*/
	int swapdata; /* flag for big=1 or little=0 endian	*/
	int swapbhed; /* flag for big=1 or little=0 endian	*/
	int swaphdrs; /* flag for big=1 or little=0 endian	*/
	int errmax; /* max consecutive tape io errors	*/
	int errcount = 0; /* counter for tape io errors		*/
	cwp_Bool nsflag; /* flag for error in tr.ns		*/

	char *cmdbuf; /* dd command buffer			*/
	cmdbuf = (char*) malloc(BUFSIZ * sizeof(char));

	char *ebcbuf; /* ebcdic data buffer			*/
	ebcbuf = (char*) malloc(SEIS_EBCBYTES * sizeof(char));
	int ebcdic = 1; /* ebcdic to ascii conversion flag	*/

	cwp_String *key1; /* output key(s)		*/
	key1 = (cwp_String*) malloc(SEIS_NKEYS * sizeof(cwp_String));
	cwp_String key2[SEIS_NKEYS]; /* first input key(s)		*/
	cwp_String type1[SEIS_NKEYS]; /* array of types for key1	*/
	char type2[SEIS_NKEYS]; /* array of types for key2	*/
	int ubyte[SEIS_NKEYS];
	int nkeys; /* number of keys to be computed*/
	int n; /* counter of keys getparred	*/
	int ikey; /* loop counter of keys 	*/
	int index1[SEIS_NKEYS]; /* array of indexes for key1 	*/

	/* deal with number of extended text headers */
	short nextended;

	/* Set parameters */
	trmin = 1;
	trmax = INT_MAX;
	union {
		short s;
		char c[2];
	} testend;
	testend.s = 1;
	endian = (testend.c[0] == '\0') ? 1 : 0;

	swapdata = endian;
	swapbhed = endian;
	swaphdrs = endian;

	ebcdic = 1;

	errmax = 0;
	buff = 1;
	int num_of_gathers[root_obj->num_of_keys];

	/* Override binary format value */
	over = 0;
	//     if (getparint("format", &format))	format_set = cwp_true;
	if (((over != 0) && (format_set))) {
		bh.format = format;
		if (!((format == 1) || (format == 2) || (format == 3)
				|| (format == 5) || (format == 8))) {
			warn("Specified format=%d not supported", format);
			warn("Assuming IBM floating point format, instead");
		}
	}

	conv = 1;

	nkeys = 0;

	for (ikey = 0; ikey < nkeys; ++ikey) {
		/* get types and index values */
		type1[ikey] = hdtype(key1[ikey]);
		index1[ikey] = getindex(key1[ikey]);
	}

	for (ikey = 0; ikey < nkeys; ++ikey) {
		if (sscanf(key2[ikey], "%d%c", &ubyte[ikey], &type2[ikey])
				!= 2) {
			err("user format XXXt");
		}
	}

	daos_size_t size;

	int error;

	/** Read Binary and text headers */
	read_headers(&bh, ebcbuf, &nextended, root_obj, daos_tape, swapbhed, endian);
	/** Process headers read */
	process_headers(&bh, format, over, format_set, &trcwt, verbose, &ns, &nsegy);
	if(additional == 0){
		/** Write binary and text headers to root object */
		write_headers(bh, ebcbuf, root_obj);
	}

	/** Parse Extended text headers */
	parse_exth(nextended, daos_tape,ebcbuf, root_obj);
	free(ebcbuf);

	/* Read the traces */
	nsflag = cwp_false;
	seismic_entry_t seismic_entry= {0};

	if(additional == 1) {
		daos_iod_t		iod;

		prepare_iod(&iod, NULL, DS_A_NTRACES_HEADER, DAOS_IOD_SINGLE,
			    sizeof(int), 1);

		/**Fetch Number of traces Under opened Root object */
		prepare_seismic_entry(&seismic_entry, root_obj->root_obj->oid,
				      DS_D_FILE_HEADER,
				      (char*)&root_obj->number_of_traces,
				      sizeof(int), &iod);
		rc = fetch_seismic_entry(root_obj->root_obj->oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			return rc;
		}

		for(i = 0; i < root_obj->num_of_keys; i++) {
			seismic_obj[i] = (seis_obj_t*)malloc(sizeof(seis_obj_t));
			seismic_obj[i]->oid = root_obj->gather_oids[i];
			strcpy(seismic_obj[i]->name, root_obj->keys[i]);
			rc = daos_obj_open(root_obj->coh,
					   seismic_obj[i]->oid,
					   get_daos_obj_mode(O_RDWR),
					   &(seismic_obj[i]->oh),
					   NULL);
			if (rc != 0) {
				err("Opening <%s> seismic object failed, error"
				    " code = %d \n",seismic_obj[i]->name, rc);
				return rc;
			}
			seismic_obj[i]->dkeys_list = NULL;
			seismic_obj[i]->gathers = malloc(sizeof(gathers_list_t));
			seismic_obj[i]->gathers->head = NULL;
			seismic_obj[i]->gathers->tail = NULL;
			seismic_obj[i]->gathers->size = 0;
			seismic_obj[i]->seis_gather_trace_oids_obj = NULL;
			read_object_gathers(root_obj, seismic_obj[i]);
			num_of_gathers[i]= seismic_obj[i]->number_of_gathers;
			printf("number of gathers in object %s is %ld %d \n",
			       seismic_obj[i]->name, seismic_obj[i]->gathers->size,
			       seismic_obj[i]->number_of_gathers);
		}
//		printf("FINISHED FETCHING LINKED LIST OF GATHERS \n");
	}
	int index;
	if(additional == 1) {
		index = root_obj->number_of_traces;
	} else {
		index = 0;
	}

	itr = 0;


	while (itr < trmax) {
		int nread;

		size = read_dfs_file(daos_tape, (char*) &tapetr, nsegy);
		nread = size;

		/* middle exit loop instead of mile-long while */
		if (!nread){
			break;
		}
		process_trace(tapetr, &tr, bh, ns, swaphdrs,
			      nsflag, &itr, nkeys, type1,
			      type2, ubyte, endian, conv, swapdata,
			      index1, trmin, trcwt, verbose);

		trace_obj_t *trace_obj;
		root_obj->number_of_traces++;
		rc = trace_obj_create(dfs, &trace_obj, index, &tr);
		if (rc != 0) {
			err("Creating and writing trace data failed,"
			    " error code = %d \n", rc);
			return rc;
		}
		for(i = 0; i < root_obj->num_of_keys; i++) {
			rc = trace_linking(trace_obj,
					   seismic_obj[i],
					   root_obj->keys[i]);
			if (rc != 0) {
				err("Linking trace to <%s> gather object failed,"
				    " error code = %d \n",root_obj->keys[i], rc);
				return rc;
			}
		}
		free(trace_obj->trace);

		/* Echo under verbose option */
		if (verbose && (itr % vblock) == 0) {
			warn(" %d traces from tape", itr);
		}
		rc = daos_obj_close(trace_obj->oh, NULL);
		if(rc != 0) {
			err("closing trace header object failed,"
			    " error code = %d \n", rc);
			return rc;
		}

		D_FREE_PTR(trace_obj);

		index++;
	}
	daos_iod_t	iod;

//	printf("All trace data written...\n");
	d_iov_set(&iod.iod_name, (void*) DS_A_NTRACES_HEADER,
		  strlen(DS_A_NTRACES_HEADER));
	iod.iod_type = DAOS_IOD_SINGLE;
	iod.iod_size = sizeof(int);
	iod.iod_recxs = NULL;
	iod.iod_nr = 1;

	rc = seismic_root_obj_update(root_obj, DS_D_FILE_HEADER,
				     (char*) &(root_obj->number_of_traces),
				     sizeof(int), &iod);
	if (rc != 0) {
		err("Updating number of traces of root seismic object failed, "
		    "error code = %d \n",rc);
		return rc;
	}
	for(i=0; i< root_obj->num_of_keys; i++) {
		d_iov_set(&iod.iod_name, (void*) DS_A_NGATHERS,
			  strlen(DS_A_NGATHERS));
		iod.iod_type = DAOS_IOD_SINGLE;
		iod.iod_size = sizeof(int);
		iod.iod_recxs = NULL;
		iod.iod_nr = 1;

		rc = update_seismic_gather_object(seismic_obj[i],
						  DS_D_NGATHERS,
						  (char*)&(seismic_obj[i]->number_of_gathers),
						  sizeof(int), &iod);
		if (rc != 0) {
			err("Adding number of gathers to <%s> gather object"
			    " failed, error code = %d \n",root_obj->keys[i], rc);
			return rc;
		}
	}

//	printf("Updated all gathers numbers...\n");

	for(i=0; i< root_obj->num_of_keys; i++) {
		if(additional == 1) {
			rc = trace_oids_obj_create(dfs, OC_SX,
						   seismic_obj[i],
						   num_of_gathers[i]);
		} else {
			rc = trace_oids_obj_create(dfs, OC_SX,
						   seismic_obj[i],
						   0);
		}
		if (rc != 0) {
			err("Creating array of object ids"
			    " failed, error code = %d \n", rc);
			return rc;
		}
	}
//	printf("Created trace oids objects \n");

	for(i=0; i< root_obj->num_of_keys; i++) {
		rc = update_gather_data(dfs, seismic_obj[i]->gathers, seismic_obj[i],
					get_dkey(seismic_obj[i]->name));
		if (rc != 0) {
			err("Updating number of traces key"
			    " failed, error code = %d \n", rc);
			return rc;
		}
	}
//	printf("Updated all gathers traces...\n");

	for(i=0; i< root_obj->num_of_keys; i++) {
		release_gathers_list(seismic_obj[i]->gathers);
		free(seismic_obj[i]->seis_gather_trace_oids_obj);
		free(seismic_obj[i]->dkeys_list);
		rc = daos_obj_close(seismic_obj[i]->oh, NULL);
		if (rc != 0) {
			err("Closing seismic object %s failed, "
			    "error code = %d \n",root_obj->keys[i], rc);
			return rc;
		}
	}
	if(additional == 0) {
		rc = daos_obj_close(root_obj->root_obj->oh, NULL);
		if (rc != 0) {
			err("Closing root seismic object %s failed, "
			    "error code = %d \n",root_obj->keys[i], rc);
			return rc;
		}
	}

	free(daos_tape);
	return rc;

}

traces_metadata_t*
daos_seis_sort_traces(seis_root_obj_t *root, int number_of_keys,
		      char **sort_keys, int *directions,
		      int number_of_window_keys, char **window_keys,
		      cwp_String *type, Value *min_keys, Value *max_keys)
{
	seismic_entry_t 	seismic_entry = {0};
	daos_iod_t		iod;
	daos_recx_t		*recx;
	traces_metadata_t      *traces_metadata;
	traces_list_t	       *trace_list;
	ensembles_list_t       *ensemble_list;
	seis_obj_t 	       *seismic_object;
	read_traces	       *gather_traces;
	int 		 	temp_number_of_keys;
	int 			offset_obj = 0;
	int 			shot_obj = 0;
	int 			cmp_obj = 0;
	int 			key_exist = 0;
	int 			daos_mode;
	int 			rc;
	int 			i;

	seismic_object = malloc(sizeof(seis_obj_t));
	daos_mode = get_daos_obj_mode(O_RDWR);
	traces_metadata = malloc(sizeof(traces_metadata_t));
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;
	ensemble_list = malloc(sizeof(ensembles_list_t));
	ensemble_list->first_ensemble = NULL;
	ensemble_list->last_ensemble = NULL;
	ensemble_list->num_of_ensembles = 0;
	traces_metadata->traces_list = trace_list;
	traces_metadata->ensembles_list = ensemble_list;

	for(i = 0; i< root->num_of_keys; i++){
		if(strcmp(root->keys[i], sort_keys[0]) == 0) {
			seismic_object->oid = root->gather_oids[i];
			strcpy(seismic_object->name, sort_keys[0]);
			rc = daos_obj_open(root->coh, seismic_object->oid,
					   daos_mode, &(seismic_object->oh),
					   NULL);
			key_exist = 1;
			break;
		}
	}

	if(key_exist == 0) {
		seismic_object->oid = root->gather_oids[0];
		strcpy(seismic_object->name, root->keys[0]);
		shot_obj = 1;
		rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
				   &(seismic_object->oh), NULL);
	}
	if (rc != 0) {
		err("Opening seismic object <%s> failed, error"
		    " code = %d \n", seismic_object->name, rc);
		exit(rc);
	}

	prepare_iod(&iod, NULL, DS_A_NGATHERS, DAOS_IOD_SINGLE,
		    sizeof(int), 1);

	/**Fetch Number of Gathers Under opened seismic Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS,
			      (char*) &seismic_object->number_of_gathers,
			      sizeof(int), &iod);
	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
			 	 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers in <%s> seismic object failed,"
		    " error code = %d \n", seismic_object->name, rc);
		exit(rc);
	}

	seismic_object->dkeys_list = malloc(seismic_object->number_of_gathers * sizeof(long));

	recx = malloc(sizeof(daos_recx_t));
	recx->rx_idx = 0;
	recx->rx_nr = seismic_object->number_of_gathers;

	prepare_iod(&iod, recx, DS_A_DKEYS_LIST, DAOS_IOD_ARRAY,
		    sizeof(long), 1);

	/** Fetch dkeys character array */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_DKEYS_LIST,
			      (char*)seismic_object->dkeys_list,
			      (sizeof(long)*seismic_object->number_of_gathers), &iod);
	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
			 	 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching dkeys list failed, error "
		    "code = %d \n", rc);
		exit(rc);
	}

	/**Fetch list of dkeys under seismic_object */
	char **unique_keys;
	unique_keys = tokenize_dkeys_list(seismic_object);
	if(directions[0] == 0) {
		int high = seismic_object->number_of_gathers;
		for(i=0 ; i< high; i++, high--) {
			char temp[200]= "";
			strcpy(temp, unique_keys[high-1]);
			strcpy(unique_keys[high-1], unique_keys[i]);
			strcpy(unique_keys[i], temp);
		}
	}

	gather_traces = malloc(seismic_object->number_of_gathers *
			       sizeof(read_traces));
	for (i = 0; i < seismic_object->number_of_gathers; i++) {
		temp_number_of_keys = number_of_keys;

		daos_obj_id_t *oids =
				get_gather_oids(root, seismic_object,
						unique_keys[i],
						&(gather_traces[i].number_of_traces));

		gather_traces[i].traces = malloc(gather_traces[i].number_of_traces *
						 sizeof(trace_t));

		fetch_traces_header_read_traces(root->coh, oids,
						&gather_traces[i], daos_mode);

		if (key_exist == 1) {
			temp_number_of_keys--;
		}
		/** if secondary keys exist (number of keys is greater than 1*/
		if (temp_number_of_keys > 0) {
			sort_headers(&gather_traces[i], sort_keys, directions,
				     temp_number_of_keys);
		}

		int 	k;
		/** Create new temp list */
		traces_list_t 	*gather_trace_list =
					malloc(sizeof(traces_list_t));
		gather_trace_list->head = NULL;
		gather_trace_list->tail = NULL;
		gather_trace_list->size = 0;
		/** copy traces data from the array of read_traces
		 *  to linked list of traces
		 */
		for (k = 0; k < gather_traces[i].number_of_traces; k++) {
			add_trace_header(&(gather_traces[i].traces[k]),
					 &gather_trace_list, &ensemble_list, k,
					 gather_traces[i].number_of_traces);
		}
		/** check array of window keys if not null
		 *  then call window headers functionality
		 */
		if (window_keys != NULL) {
			window_headers(&gather_trace_list, window_keys,
				       number_of_window_keys, type, min_keys,
				       max_keys);
		}
		/** fetch traces data and merge the two linked lists */
		if (gather_trace_list->head != NULL) {
			daos_seis_fetch_traces_data(root->coh, &gather_trace_list,
					  daos_mode);
			merge_trace_lists(&trace_list, &gather_trace_list);
		}

		free(gather_trace_list);
		free(oids);
	}
	/** Close seismic object */
	rc = daos_obj_close(seismic_object->oh, NULL);
	if(rc != 0) {
		err("Closing seismic object failed, error code = %d \n", rc);
		exit(rc);
	}
	/** free allocated memory */
	for(i=0; i<seismic_object->number_of_gathers; i++){
		free(unique_keys[i]);
		free(gather_traces[i].traces);
	}
	free(unique_keys);
	free(gather_traces);
	free(seismic_object->dkeys_list);
	free(seismic_object);
	free(recx);

	return traces_metadata;
}

traces_metadata_t*
daos_seis_wind_traces(seis_root_obj_t *root, char **window_keys, 
		      int number_of_keys, Value *min_keys,
		      Value *max_keys, cwp_String *type)
{
	seismic_entry_t 	seismic_entry = {0};
	traces_list_t 	       *trace_list;
	daos_iod_t		iod;
	daos_recx_t		*recx;
	ensembles_list_t       *ensemble_list;
	traces_metadata_t      *traces_metadata;
	seis_obj_t 	       *seismic_object;
	char 			temp[4096];
	int 			daos_mode;
	int 			key_exist = 0;
	int	 		rc;
	int 			i = 0;
	int 			j;
	daos_mode = get_daos_obj_mode(O_RDWR);
	seismic_object = malloc(sizeof(seis_obj_t));
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;
	ensemble_list = malloc(sizeof(ensembles_list_t));
	ensemble_list->first_ensemble = NULL;
	ensemble_list->last_ensemble = NULL;
	ensemble_list->num_of_ensembles = 0;
	traces_metadata = malloc(sizeof(traces_metadata_t));
	traces_metadata->traces_list = trace_list;
	traces_metadata->ensembles_list = ensemble_list;

	while(i < number_of_keys){
		for(j = 0; j < root->num_of_keys; j++){
			if(strcmp(window_keys[i],root->keys[j]) == 0) {
				seismic_object->oid = root->gather_oids[j];
				strcpy(seismic_object->name, root->keys[j]);
				key_exist = 1;
				if (i > 0) {
					cwp_String 	type_temp;
					Value 		min_temp;
					Value 		max_temp;
					char 		key_temp[200] = "";

					type_temp = type[i];
					type[i] = type[0];
					type[0] = type_temp;
					min_temp = min_keys[i];
					min_keys[i] = min_keys[0];
					min_keys[0] = min_temp;
					max_temp = max_keys[i];
					max_keys[i] = max_keys[0];
					max_keys[0] = max_temp;
					strcpy(key_temp,window_keys[i]);
					strcpy(window_keys[i],window_keys[0]);
					strcpy(window_keys[0],key_temp);
					break;
				}
			}
		}
		if (key_exist == 1) {
			break;
		}
		i++;
	}
	/** if first key is not one of the gather objects
	 *  then start with the shot object
	 */
	if (key_exist == 0) {
		seismic_object->oid = root->gather_oids[0];
		strcpy(seismic_object->name, root->keys[0]);
	}
	rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
			   &(seismic_object->oh), NULL);
	if (rc != 0) {
		err("Opening <%s> seismic object failed, error"
		    " code = %d \n",seismic_object->name, rc);
		exit(rc);
	}

	prepare_iod(&iod, NULL, DS_A_NGATHERS, DAOS_IOD_SINGLE,
		    sizeof(int), 1);

	/**Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS,
			      (char*)&seismic_object->number_of_gathers,
			      sizeof(int), &iod);
	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
				 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		exit(rc);
	}

	seismic_object->dkeys_list = malloc(seismic_object->number_of_gathers * sizeof(long));

	recx = malloc(sizeof(daos_recx_t));
	recx->rx_idx = 0;
	recx->rx_nr = seismic_object->number_of_gathers;

	prepare_iod(&iod, recx, DS_A_DKEYS_LIST, DAOS_IOD_ARRAY,
		    sizeof(long), 1);

	/** Fetch dkeys character array */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_DKEYS_LIST,
			      (char*)seismic_object->dkeys_list,
			      (sizeof(long)*seismic_object->number_of_gathers),
			      &iod);
	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
			 	 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching dkeys list failed, error "
		    "code = %d \n", rc);
		exit(rc);
	}



	/**Fetch list of dkeys under seismic_object */
	char 		**unique_keys;

	unique_keys = tokenize_dkeys_list(seismic_object);
//	for(i=0; i<seismic_object->number_of_gathers; i++) {
//		printf("KEY NUMBER %d IS %s \n", i, unique_keys[i]);
//	}
	int 		number_of_traces;
	Value		unique_value;
 	for(i=0; i< seismic_object->number_of_gathers; i++){

 		prepare_iod(&iod, NULL, DS_A_UNIQUE_VAL, DAOS_IOD_SINGLE,
 			    sizeof(long), 1);

 		/** Fetch gather unique val */
 		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i],
				      (char*)&unique_value, sizeof(long),
				      &iod);
		rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
					 &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			exit(rc);
		}
		if((key_exist ==1) &&
		   ((valcmp(type[0],unique_value, min_keys[0]) == -1) ||
		    (valcmp(type[0], unique_value, max_keys[0]) == 1)) ) {
			continue;
		}

		daos_obj_id_t *oids = get_gather_oids(root, seismic_object,
						      unique_keys[i],
						      &number_of_traces);

		fetch_traces_header_traces_list(root->coh, oids, traces_metadata,
						daos_mode, number_of_traces);
		free(oids);
 	}
 	/** apply window on headers fetched */
	window_headers(&trace_list, window_keys, number_of_keys,
		       type, min_keys, max_keys);
	/** Fetch traces data */
	daos_seis_fetch_traces_data(root->coh, &trace_list, daos_mode);
	/** Close seismic object */
	rc = daos_obj_close(seismic_object->oh, NULL);
	if(rc != 0) {
		err("Closing seismic object failed, error code = %d \n", rc);
		exit(rc);
	}
	/** free allocated memory */
	for(i=0; i<seismic_object->number_of_gathers; i++){
		free(unique_keys[i]);
	}
	free(unique_keys);
	free(seismic_object->dkeys_list);
	free(seismic_object);
	free(recx);
	return traces_metadata;
}

void
daos_seis_set_headers(dfs_t *dfs, seis_root_obj_t *root,
		      int num_of_keys, char **keys_1, char **keys_2,
		      char **keys_3, double *a, double *b, double *c,
		      double *d, double *j, double *e, double *f,
		      header_operation_type_t type)
{
	traces_metadata_t      *traces_metadata;
	int			existing_keys[num_of_keys];
	int 			daos_mode;
	int 			i;
	int			k;

	daos_mode = get_daos_obj_mode(O_RDWR);

	for (i = 0; i < num_of_keys; i++) {
		existing_keys[i] = 0;
		for(k = 0; k < num_of_keys; k++) {
			if(strcmp(root->keys[k], keys_1[i]) == 0) {
				existing_keys[i] = 1;
				break;
			}
		}
	}
	traces_metadata = daos_seis_get_headers(root, NULL);
	/** Set new traces headers */
	set_traces_header(root->coh, daos_mode, &(traces_metadata->traces_list),
			  num_of_keys, keys_1, keys_2, keys_3, a, b, c, d, e,
			  f, j, type);
	/** check if the header keys was one of the already created gathers.
	 *  if yes then the gather object should be replaced by a new object
	 *  with the new key values.
	 */
	for(i = 0; i < num_of_keys; i++) {
		if(existing_keys[i] == 1) {
			replace_seismic_objects(dfs, daos_mode, keys_1[i],
						traces_metadata->traces_list,
						root);
		}
	}
	/** Release traces metadata */
	daos_seis_release_traces_metadata(traces_metadata);
}

headers_ranges_t*
daos_seis_range_headers(seis_root_obj_t *root, int number_of_keys,
			char **keys, int dim)
{
	traces_metadata_t      *traces_metadata;

	traces_metadata = daos_seis_get_headers(root, NULL);

	/** Get ranges of fetched headers list */
	headers_ranges_t *ranges = malloc(sizeof(headers_ranges_t));
	range_traces_headers(traces_metadata->traces_list,
			     number_of_keys, keys, dim, ranges);

	/** Release traces metadata */
	daos_seis_release_traces_metadata(traces_metadata);
	return ranges;
}

traces_metadata_t*
daos_seis_get_headers(seis_root_obj_t *root, char *key)
{
	seismic_entry_t 	seismic_entry = {0};
	traces_metadata_t      *traces_metadata;
	seis_obj_t 	       *seismic_object;
	daos_recx_t  	       *recx;
	daos_iod_t 		iod;
	int 			daos_mode;
	int 			key_exists=0;
	int			rc;
	int 			i;

	daos_mode = get_daos_obj_mode(O_RDWR);
	seismic_object = malloc(sizeof(seis_obj_t));
	traces_metadata = malloc(sizeof(traces_metadata_t));
	traces_metadata->traces_list = malloc(sizeof(traces_list_t));
	traces_metadata->ensembles_list = malloc(sizeof(ensembles_list_t));
	traces_metadata->traces_list->head = NULL;
	traces_metadata->traces_list->tail = NULL;
	traces_metadata->traces_list->size = 0;
	traces_metadata->ensembles_list->first_ensemble = NULL;
	traces_metadata->ensembles_list->last_ensemble = NULL;
	traces_metadata->ensembles_list->num_of_ensembles = 0;

	recx = malloc(sizeof(daos_recx_t));

	if(key != NULL) {
		for(i=0 ; i < root->num_of_keys; i++) {
			if(strcmp(key, root->keys[i]) == 0) {
				seismic_object->oid = root->gather_oids[i];
				strcpy(seismic_object->name, root->keys[i]);
				key_exists = 1;
				break;
			}
		}
	}

	if(key_exists == 0) {
		seismic_object->oid = root->gather_oids[0];
		strcpy(seismic_object->name, root->keys[0]);
	}

	rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
			   &(seismic_object->oh), NULL);
	if (rc != 0) {
		err("Opening seismic object failed, error"
		    " code = %d \n", rc);
		exit(rc);
	}

	prepare_iod(&iod, NULL, DS_A_NGATHERS, DAOS_IOD_SINGLE,
		    sizeof(int), 1);

	/** Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS,
			      (char*)&seismic_object->number_of_gathers,
			      sizeof(int), &iod);

	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
			 	 &seismic_entry, NULL);
	if(rc != 0){
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		exit(rc);
	}

	seismic_object->dkeys_list = malloc(seismic_object->number_of_gathers * sizeof(long));

	recx->rx_idx = 0;
	recx->rx_nr = seismic_object->number_of_gathers;

	prepare_iod(&iod, recx, DS_A_DKEYS_LIST, DAOS_IOD_ARRAY,
		    sizeof(long), 1);

	/** Fetch dkeys character array */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_DKEYS_LIST,
			      (char*)seismic_object->dkeys_list,
			      (sizeof(long)*seismic_object->number_of_gathers),
			      &iod);
	rc = fetch_seismic_entry(seismic_object->oh, DAOS_TX_NONE,
			 	 &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching dkeys list failed, error "
		    "code = %d \n", rc);
		exit(rc);
	}

	/** Fetch list of dkeys under seismic_object */
	char **unique_keys;
	unique_keys = tokenize_dkeys_list(seismic_object);

	int number_of_traces;
	for (i = 0; i < seismic_object->number_of_gathers; i++) {

		daos_obj_id_t *oids = get_gather_oids(root, seismic_object,
						      unique_keys[i],
						      &number_of_traces);

		fetch_traces_header_traces_list(root->coh, oids,
						traces_metadata, daos_mode,
						number_of_traces);
		free(oids);
	}
	/** Free allocated memory */
	for(i=0; i<seismic_object->number_of_gathers; i++){
		free(unique_keys[i]);
	}
	free(unique_keys);
	free(seismic_object->dkeys_list);
	free(seismic_object);

	return traces_metadata;
}

void
daos_seis_set_data(seis_root_obj_t *root, traces_list_t *trace_list)
{
	trace_node_t 	*current;
	trace_oid_oh_t 		data_oids;
	int 			 rc;
	int 			 i;

	current = trace_list->head;

	for (i = 0; i < trace_list->size; i++) {
		data_oids.oid =	get_trace_data_oid(&(current->trace.trace_header_obj),
						   OC_SX);
		segy *trace = daos_seis_trace_to_segy(&current->trace);

		rc = daos_array_open_with_attr(root->coh, data_oids.oid,
					       DAOS_TX_NONE, DAOS_OO_RW, 1,
					       200 * sizeof(float),
					       &data_oids.oh, NULL);
		if (rc != 0) {
			err("Opening trace data object failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}
		rc = trace_data_update(&data_oids, trace);
		if (rc != 0) {
			err("Updating trace data object failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}
		rc = daos_array_close(data_oids.oh, NULL);
		if (rc != 0) {
			err("Closing trace data object failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}
		free(trace);
		current = current->next_trace;
	}
}

void
daos_seis_parse_raw_data (dfs_t *dfs, seis_root_obj_t *root,
		       seis_obj_t **seismic_obj, dfs_obj_t *input_file,
		       dfs_obj_t *header_file, int ns, int ftn)
{

	DAOS_FILE 		*daos_in_file;
	DAOS_FILE 		*daos_headfp = NULL;
	cwp_Bool 		 isreading = cwp_true;
	segy 			 tr;
	/** to discard ftn junk  */
	char 			 junk[ISIZE];
	int 			 ihead ;
	int 			 iread=0;
	int 			 i;
	int 			 itr = 0;
	int 			 rc;

	daos_in_file = malloc(sizeof(DAOS_FILE));
	daos_in_file->file = input_file;
	daos_in_file->offset = 0;

	if(header_file != NULL) {
		daos_headfp = malloc(sizeof(DAOS_FILE));
		daos_headfp->file = header_file;
		daos_headfp->offset = 0;
		ihead = 1;
	} else {
		ihead = 0;
	}

	while (isreading==cwp_true) {
		static int tracl = 0;
		/** If Fortran data, read past the record size bytes */
		if (ftn) {
			read_dfs_file(daos_in_file, junk, ISIZE);
		}

		iread = read_dfs_file(daos_in_file, (char *) tr.data, FSIZE * ns);
		iread /= FSIZE;

		if(iread!=ns) {
			break;
		} else {
			trace_obj_t 	*trace_obj;

			if(ihead==0) {
				tr.tracl = ++tracl;
			} else {
				read_dfs_file(daos_headfp, (char *)&tr, TRACEHDR_BYTES);
			}
			tr.ns = ns;
			tr.trid = TREAL;

			rc = trace_obj_create(dfs, &trace_obj, itr, &tr);
			if (rc != 0) {
				err("Creating and writing trace data failed,"
				    " error code = %d \n", rc);
				exit(rc);
			}

			for(i = 0; i < root->num_of_keys; i++) {
				rc = trace_linking(trace_obj,
						   seismic_obj[i],
						   root->keys[i]);
				if (rc != 0) {
					err("Linking trace to <%s> gather "
					    "object failed, error"
					    " code = %d \n",root->keys[i], rc);
					exit(rc);
				}
			}
			daos_obj_close(trace_obj->oh, NULL);
			free(trace_obj->trace);
			free(trace_obj);
		}
		if (ftn) {
			read_dfs_file(daos_in_file, junk, ISIZE);
		}
		root->number_of_traces++;
		itr++;
	}
	daos_iod_t	iod;
//	printf("All trace data written...\n");

	prepare_iod(&iod, NULL, DS_A_NTRACES_HEADER, DAOS_IOD_SINGLE,
		    sizeof(int), 1);
	rc = seismic_root_obj_update(root, DS_D_FILE_HEADER,
				     (char*) &(root->number_of_traces),
				     sizeof(int), &iod);
	if (rc != 0) {
		err("Updating number of traces of root seismic object failed, "
		    "error code = %d \n",rc);
		exit(rc);
	}


	for(i=0; i< root->num_of_keys; i++) {

		prepare_iod(&iod, NULL, DS_A_NGATHERS, DAOS_IOD_SINGLE,
			    sizeof(int), 1);

		rc = update_seismic_gather_object(seismic_obj[i],
						  DS_D_NGATHERS,
					  	  (char*)&(seismic_obj[i]->number_of_gathers),
						  sizeof(int), &iod);
		if (rc != 0) {
			err("Adding number of gathers to <%s> gather object"
			    " failed, error code = %d \n",root->keys[i], rc);
			exit(rc);
		}
	}

	for(i=0; i< root->num_of_keys; i++) {
			rc = trace_oids_obj_create(dfs, OC_SX,
						   seismic_obj[i],
						   0);
		if (rc != 0) {
			err("Creating array of object ids"
			    " failed, error code = %d \n", rc);
			exit(rc);
		}
	}
//	printf("Created trace oids objects \n");

	for(i=0; i< root->num_of_keys; i++) {
		rc = update_gather_data(dfs, seismic_obj[i]->gathers,
					  seismic_obj[i],
					  get_dkey(seismic_obj[i]->name));
		if (rc != 0) {
			err("Updating number of traces key"
			    " failed, error code = %d \n", rc);
			exit(rc);
		}
	}
//	printf("Updated all gathers traces...\n");

	for(i=0; i< root->num_of_keys; i++) {
		release_gathers_list(seismic_obj[i]->gathers);
		free(seismic_obj[i]->seis_gather_trace_oids_obj);
		free(seismic_obj[i]->dkeys_list);
		rc = daos_obj_close(seismic_obj[i]->oh, NULL);
		if (rc != 0) {
			err("Closing seismic object %s failed, "
			    "error code = %d \n",root->keys[i], rc);
			exit(rc);
		}
	}


	free(daos_in_file);
	if(header_file != NULL) {
		free(daos_headfp);
	}
}

void
daos_seis_release_traces_metadata(traces_metadata_t *traces_metadata)
{
	release_traces_list(traces_metadata->traces_list);
	release_ensembles_list(traces_metadata->ensembles_list);
	free(traces_metadata);
}

void
daos_seis_create_graph(dfs_t *dfs, dfs_obj_t *parent, char *name,
		       int num_of_keys, char **keys,
		       seis_root_obj_t **root_obj, seis_obj_t **seismic_obj)
{
	int 			rc;
	int 			i;

	rc = seismic_root_obj_create(dfs, root_obj, OC_SX, name, parent,
				       num_of_keys, keys);
	if(rc != 0) {
		err("Creating seismic root object failed, "
		    "error code = %d \n", rc);
		return;
	}

	for(i = 0; i < num_of_keys; i++) {
		rc = seismic_gather_obj_create(dfs, OC_SX, *root_obj,
					       &(seismic_obj[i]),
					       keys[i], i);
		if (rc != 0) {
			err("Creating seismic <%s> object failed, "
			    "error code = %d \n",keys[i], rc);
			return;
		}
	}
	/** store number of keys and array of keys under
	 *  seperate dkeys and akeys
	 */
	daos_iod_t	iod;

	d_iov_set(&iod.iod_name, (void*) DS_A_NUM_OF_KEYS,
		  strlen(DS_A_NUM_OF_KEYS));
	iod.iod_type = DAOS_IOD_SINGLE;
	iod.iod_size = sizeof(int);
	iod.iod_recxs = NULL;
	iod.iod_nr = 1;

	rc = seismic_root_obj_update(*root_obj, DS_D_KEYS,
				     (char*)&num_of_keys, sizeof(int),
				     &iod);
	if (rc != 0) {
		err("Updating root object num_of_keys failed, "
		    "error code = %d \n", rc);
		return;
	}

	daos_recx_t	*recx;
	recx = malloc(sizeof(daos_recx_t));

	for(i = 0 ;i < num_of_keys; i++) {
		char temp[10]="";
		char akey[100]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);

		d_iov_set(&iod.iod_name, (void*) akey,
			  strlen(akey));
		iod.iod_type = DAOS_IOD_ARRAY;
		iod.iod_size = 1;
		iod.iod_nr = 1;
		recx->rx_idx = 0;
		recx->rx_nr = KEY_LENGTH * sizeof(char);
		iod.iod_recxs = recx;

		rc = seismic_root_obj_update(*root_obj, DS_D_KEYS,
					     keys[i], KEY_LENGTH * sizeof(char),
					     &iod);
		if (rc != 0) {
			err("Updating root object <%s> key failed, "
			    "error code = %d \n",keys[i], rc);
			return;
		}
	}
	free(recx);
}

void
daos_seis_fetch_traces_data(daos_handle_t coh, traces_list_t **head_traces,
		  	    int daos_mode)
{
	daos_array_iod_t 	iod;
	seismic_entry_t 	seismic_entry = {0};
	trace_node_t	       *current;
	trace_oid_oh_t 		trace_data_obj;
	daos_obj_id_t		hdr_obj_id;
	daos_range_t 		rg;
	d_sg_list_t 		sgl;
	d_iov_t 		iov;
	int 			rc;
	int 			i;

	current = (*head_traces)->head;
	if (current == NULL) {
		warn("Linked list of traces headers is empty.\n");
		return;
	}

	while (current != NULL) {
		/** calculate trace data oid from header oid*/
		hdr_obj_id = current->trace.trace_header_obj;
		trace_data_obj.oid = get_trace_data_oid(&hdr_obj_id, OC_SX);
		/** open trace data object */
		rc = daos_array_open_with_attr(coh, (trace_data_obj).oid,
					       DAOS_TX_NONE, DAOS_OO_RW, 1,
					       200 * sizeof(float),
					       &(trace_data_obj.oh), NULL);
		if (rc != 0) {
			err("Opening trace data object with attr() failed, "
			    "error code = %d \n", rc);
			return;
		}
		/** set scatter gather list and io descriptor */
		sgl.sg_nr = 1;
		sgl.sg_nr_out = 0;
		current->trace.data = malloc(current->trace.ns *
					     sizeof(float));
		seismic_entry.data = (char*) current->trace.data;
		d_iov_set(&iov, (void*) (seismic_entry.data),
			  current->trace.ns * sizeof(float));
		sgl.sg_iovs = &iov;
		iod.arr_nr = 1;
		rg.rg_len = current->trace.ns * sizeof(float);
		rg.rg_idx = 0;
		iod.arr_rgs = &rg;
		/** Read trace data */
		rc = daos_array_read(trace_data_obj.oh, DAOS_TX_NONE,
				     &iod, &sgl, NULL);
		if (rc != 0) {
			err("Reading trace data failed, error"
			    " code = %d \n", rc);
			return;
		}
		/** close trace data object */
		rc = daos_array_close(trace_data_obj.oh, NULL);
		if(rc != 0){
			err("Closing trace data object failed, error code "
					"= %d \n", rc);
			return;
		}
		current = current->next_trace;
	}

}

dfs_obj_t*
dfs_get_parent_of_file(dfs_t *dfs, const char *file_directory,
		       int allow_creation, char *file_name,
		       int verbose_output)
{
	daos_oclass_id_t 	cid = OC_SX;
	dfs_obj_t 	       *parent = NULL;
	char 			temp[2048];
	int 			array_len = 0;
	int 			err;
	strcpy(temp, file_directory);
	const char 		*sep = "/";
	char *token = strtok(temp, sep);
	while (token != NULL) {
		array_len++;
		token = strtok(NULL, sep);
	}
	char **array = malloc(sizeof(char*) * array_len);
	strcpy(temp, file_directory);
	token = strtok(temp, sep);
	int 			i = 0;
	while (token != NULL) {
		array[i] = malloc(sizeof(char) * (strlen(token) + 1));
		strcpy(array[i], token);
		token = strtok(NULL, sep);
		i++;
	}

	for (i = 0; i < array_len - 1; i++) {
		dfs_obj_t 	*temp_obj;
		err = dfs_lookup_rel(dfs, parent, array[i], O_RDWR,
				     &temp_obj, NULL, NULL);
		if (err == 0) {
			if (verbose_output) {
				warn("Subdirectory '%s' already exist \n",
				     array[i]);
			}
		} else if (allow_creation) {
			mode_t mode = 0666;
			err = dfs_mkdir(dfs, parent, array[i], mode, cid);
			if (err == 0) {
				if (verbose_output) {
					warn("Created directory '%s'\n",
					     array[i]);
				}
				dfs_lookup_rel(dfs, parent, array[i], O_RDWR,
					       &temp_obj, NULL, NULL);
			} else {
				warn("Mkdir on %s failed with error code :"
				     " %d \n", array[i], err);
			}
		} else {
			warn("Relative lookup on %s failed with error code :"
			     " %d \n", array[i], err);
		}
		parent = temp_obj;
	}
	strcpy(file_name, array[array_len - 1]);
	for (i = 0; i < array_len; i++) {
		free(array[i]);
	}
	free(array);

	return parent;
}

segy*
daos_seis_trace_to_segy(trace_t *trace)
{
	segy 		*tp;

	tp = malloc(sizeof(segy));
	memcpy(tp, trace, TRACEHDR_BYTES);
	memcpy(tp->data, trace->data, tp->ns * sizeof(float));
	return tp;
}

