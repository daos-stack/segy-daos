/*
 * daos_seis.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

#include "daos_seis.h"

#include <daos/event.h>

#define TRACEHDR_BYTES 240

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

	obj = segy_root_object->root_obj;

	/** Release daos file system object of the seismic root object. */
	rc = dfs_release(obj);
	if (rc != 0) {
		err("Releasing seismic root dfs object failed, error code"
			" = %d\n",
		    rc);
	}

	/** Free the allocated seismic root object itself. */
	free(segy_root_object);

	return rc;
}

seis_root_obj_t*
daos_seis_open_root_path(dfs_t *dfs, const char *root_path)
{
	seis_root_obj_t 	*root_obj;
	int 		 	 rc;
	dfs_obj_t 		*root;
	mode_t 			 mode;
	struct stat 		 stbuf;

	/** Open the dfs object containing the root seismic object **/
	rc = dfs_lookup(dfs, root_path, O_RDWR, &root, &mode, &stbuf);
	if (rc != 0) {
		err("Looking up path<%s> in dfs failed , error code = %d \n",
		    root_path, rc);
		return rc;
	}

	/** Fetch parameters of the seismic root object **/
	root_obj = daos_seis_open_root(dfs, root);

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

	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*)&seismic_object->number_of_gathers,
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
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
	bhed 		       *bhdr;
	int 			rc;
	/** allocate binary header struct */
	bhdr = malloc(sizeof(bhed));
	/** prepare seismic entry & fetch binary header from root object */
	prepare_seismic_entry(&entry, root->root_obj->oid, DS_D_FILE_HEADER,
			      DS_A_BINARY_HEADER, (char*) bhdr, BNYBYTES,
			      DAOS_IOD_ARRAY);
	rc = daos_seis_fetch_entry(root->root_obj->oh, DAOS_TX_NONE, &entry,
				   NULL);
	if (rc != 0) {
		err("Fetching binary header data failed, error"
		    " code = %d \n", rc);
		return rc;
	}

	return bhdr;
}

char*
daos_seis_get_text_header(seis_root_obj_t *root)
{
	seismic_entry_t 	entry = { 0 };
	char 		       *text_header;
	int	 		rc;

	text_header = malloc(EBCBYTES * sizeof(char));
	/** prepare seismic entry & fetch text header from root object */
	prepare_seismic_entry(&entry, root->root_obj->oid, DS_D_FILE_HEADER,
			      DS_A_TEXT_HEADER, text_header, EBCBYTES,
		              DAOS_IOD_ARRAY);
	rc = daos_seis_fetch_entry(root->root_obj->oh, DAOS_TX_NONE, &entry,
				   NULL);
	if (rc != 0) {
		err("Fetching textual header data failed, error"
		    " code = %d \n", rc);
		return rc;
	}

	return text_header;
}

traces_list_t*
daos_seis_get_shot_traces(int shot_id, seis_root_obj_t *root)
{
	traces_list_t 		*trace_list;
	Value 			 min;
	char 			*key;
	char 			*type;

	min.i = shot_id;
	key = "fldr";
	type = hdtype(key);

	trace_list = daos_seis_wind_traces(root, &key, 1, &min, &min, &type);
	printf("BESOOOOOOOOOOOOOOOO \n");
	return trace_list;
}

int
daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name,
		     dfs_obj_t *segy_root, int num_of_keys, char **keys)
{
	seis_root_obj_t 	*root_obj;
	seis_obj_t 		**seismic_obj;
	DAOS_FILE 		*daos_tape;
	char 			*temp_name;
	int 			rc;
	int 			i;

	seismic_obj = malloc(num_of_keys * sizeof(seis_obj_t*));
	daos_tape = malloc(sizeof(DAOS_FILE));

	daos_tape->file = segy_root;
	daos_tape->offset = 0;

	rc = daos_seis_root_obj_create(dfs, &root_obj, OC_SX, name, parent,
				       num_of_keys, keys);
	if(rc != 0) {
		err("Creating seismic root object failed, "
		    "error code = %d \n", rc);
		return rc;
	}
	printf("CREATED ROOT OBJECT \n");
	for(i = 0; i < num_of_keys; i++) {
		rc = daos_seis_gather_obj_create(dfs, OC_SX, root_obj,
						 &(seismic_obj[i]),
						 keys[i], i);
		if (rc != 0) {
			err("Creating seismic <%s> object failed, "
			    "error code = %d \n",keys[i], rc);
			return rc;
		}
		printf("seismic object name >> %s \n",seismic_obj[i]->name);
	}
	printf("CREATED GATHER OBJECTS \n");

	/** store number of keys and array of keys under
	 *  seperate dkeys and akeys
	 */
	rc = daos_seis_root_update(root_obj, DS_D_NUM_OF_KEYS, DS_A_NUM_OF_KEYS,
				   (char*)&num_of_keys, sizeof(int),
				   DAOS_IOD_SINGLE);
	if (rc != 0) {
		err("Updating root object num_of_keys failed, "
		    "error code = %d \n",keys[i], rc);
		return rc;
	}
	printf("Updated number of keys under root \n");
	for(i = 0 ;i < num_of_keys; i++) {
		char temp[10]="";
		char akey[100]="";
		sprintf(temp, "%d", i);
		strcpy(akey, DS_A_KEYS);
		strcat(akey,temp);
		rc = daos_seis_root_update(root_obj, DS_D_KEYS, akey,
					   keys[i], 10 * sizeof(char),
					   DAOS_IOD_ARRAY);
		if (rc != 0) {
			err("Updating root object <%s> key failed, "
			    "error code = %d \n",keys[i], rc);
			return rc;
		}
	}

	printf("Updated array of keys under root \n");

	printf("AFTER CREATING ROOT AND GATHER OBJECTS \n");
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
	ebcbuf = (char*) malloc(EBCBYTES * sizeof(char));
	int ebcdic = 1; /* ebcdic to ascii conversion flag	*/

	cwp_String *key1; /* output key(s)		*/
	key1 = (cwp_String*) malloc(SU_NKEYS * sizeof(cwp_String));
	cwp_String key2[SU_NKEYS]; /* first input key(s)		*/
	cwp_String type1[SU_NKEYS]; /* array of types for key1	*/
	char type2[SU_NKEYS]; /* array of types for key2	*/
	int ubyte[SU_NKEYS];
	int nkeys; /* number of keys to be computed*/
	int n; /* counter of keys getparred	*/
	int ikey; /* loop counter of keys 	*/
	int index1[SU_NKEYS]; /* array of indexes for key1 	*/
	Value val1; /* value of key1		*/

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
	size = read_dfs_file(daos_tape, ebcbuf, EBCBYTES);

	/* Open pipe to use dd to convert  ebcdic to ascii */
	/* this command gives a file containing 3240 bytes on sun */
	/* see top of Makefile.config for versions */
	/* not sure why this breaks now; works in version 37 */
	char *tbuf = malloc(EBCBYTES * sizeof(char));
	int read_bytes_from_command;

	if (ebcdic == 1) {
		char *arr[] ={"dd", "ibs=1", "conv=ascii", "count=3200", NULL};
		read_bytes_from_command = execute_command(arr, ebcbuf,
							  EBCBYTES, tbuf,
							  EBCBYTES);
	} else {
		char *arr[] = { "dd", "ibs=1", "count=3200", NULL };
		read_bytes_from_command = execute_command(arr, ebcbuf,
							  EBCBYTES, tbuf,
							  EBCBYTES);
	}
	/** Update text header under root seismic object */
	rc = daos_seis_root_update(root_obj, DS_D_FILE_HEADER,
				   DS_A_TEXT_HEADER, tbuf, EBCBYTES,
				   DAOS_IOD_ARRAY);
	if (rc != 0) {
		err("Updating text header of root seismic object failed, "
		    "error code = %d \n",rc);
		return rc;
	}
	printf("UPDATED TEXT HEADER \n");
	size = read_dfs_file(daos_tape, (char*) &tapebh, BNYBYTES);

	/* Convert from bytes to ints/shorts */
	tapebhed_to_bhed(&tapebh, &bh);

	/* if little endian machine, swap bytes in binary header */
	if (swapbhed == 0) {
		for (i = 0; i < BHED_NKEYS; ++i) {
			swapbhval(&bh, i);
		}
	}

	/* Override binary format value */
	over = 0;
//	    if (getparint("format", &format))	format_set = cwp_true;
	if (((over != 0) && (format_set))) {
		bh.format = format;
	}

	/* Override application of trace weighting factor? */
	/* Default no for floating point formats, yes for integer formats. */
	trcwt = (bh.format == 1 || bh.format == 5) ? 0 : 1;

	switch (bh.format) {
	case 1:
		if (verbose) {
			warn("assuming IBM floating point input");
		}
		break;
	case 2:
		if (verbose) {
			warn("assuming 4 byte integer input");
		}
		break;
	case 3:
		if (verbose) {
			warn("assuming 2 byte integer input");
		}
		break;
	case 5:
		if (verbose) {
			warn("assuming IEEE floating point input");
		}
		break;
	case 8:
		if (verbose) {
			warn("assuming 1 byte integer input");
		}
		break;
	default:
		if (over) {
			warn("ignoring bh.format ... continue");
		} else {
			err("format not SEGY standard (1, 2, 3, 5, or 8)");
		}

	}

	/* Compute length of trace (can't use sizeof here!) */
	ns = bh.hns; /* let user override */
	if (!ns) {
		err("samples/trace not set in binary header");
	}
	bh.hns = ns;
//	    printf("ns = %d", ns);

	switch (bh.format) {
	case 8:
		nsegy = ns + SEGY_HDRBYTES;
		break;
	case 3:
		nsegy = ns * 2 + SEGY_HDRBYTES;
		break;
	case 1:
	case 2:
	case 5:
	default:
		nsegy = ns * 4 + SEGY_HDRBYTES;
	}

	//write data of binary header under binary header akey
	rc = daos_seis_root_update(root_obj, DS_D_FILE_HEADER,
				   DS_A_BINARY_HEADER, (char*)&bh,
				   BNYBYTES, DAOS_IOD_ARRAY);
	if (rc != 0) {
		err("Updating binary header of root seismic object failed, "
		    "error code = %d \n",rc);
		return rc;
	}
	printf("UPDATED BINARY HEADER \n");


	nextended = *((short*) (((unsigned char*) &tapebh) + 304));

	if (endian == 0) {
		swap_short_2((short*) &nextended);
	}
	if (verbose) {
		warn("Number of extended text headers: %d", nextended);
	}

	rc = daos_seis_root_update(root_obj, DS_D_FILE_HEADER,
				   DS_A_NEXTENDED_HEADER, (char*) &nextended,
				   sizeof(int),	DAOS_IOD_SINGLE);
	if (rc != 0) {
		err("Updating number of EXTH of root seismic object failed, "
		    "error code = %d \n",rc);
		return rc;
	}

	if (nextended > 0) /* number of extended text headers > 0 */
	{
		/* need to deal with -1 nextended headers */
		/* so test should actually be !=0, but ... */
		for (i = 0; i < nextended; i++) {
			/* cheat -- an extended text header is same size as
			 * EBCDIC header */
			/* Read the bytes from the tape for one xhdr into the
			 * buffer */
			size = read_dfs_file(daos_tape, ebcbuf, EBCBYTES);
			// write the data in ebcbuf under extended text header key.
			char akey_index[100];
			sprintf(akey_index, "%d", i);
			char akey_extended[200] = "";
			strcat(akey_extended, DS_A_EXTENDED_HEADER);
			strcat(akey_extended, akey_index);

			rc = daos_seis_root_update(root_obj, DS_D_FILE_HEADER,
						   akey_extended, ebcbuf,
						   EBCBYTES,DAOS_IOD_ARRAY);
			if (rc != 0) {
				err("Updating extended header of root seismic"
				    " object failed, error code = %d \n",rc);
				return rc;
			}
		}
	}
	printf("UPDATED EXTENDED HEADERS \n");

	/* Read the traces */
	nsflag = cwp_false;
	itr = 0;

	while (itr < trmax) {
		int nread;

		size = read_dfs_file(daos_tape, (char*) &tapetr, nsegy);
		nread = size;

		if (!nread) /* middle exit loop instead of mile-long while */
			break;

		/* Convert from bytes to ints/shorts */
		tapesegy_to_segy(&tapetr, &tr);

		/* If little endian machine, then swap bytes in trace header */
		if (swaphdrs == 0)
			for (i = 0; i < SEGY_NKEYS; ++i)
				swaphval(&tr, i);

		/* Check tr.ns field */
		if (!nsflag && ns != tr.ns) {
			int temp_itr = itr + 1;
			warn("discrepant tr.ns = %d with tape/user ns = %d\n\t"
			     "... first noted on trace %d",
			     tr.ns, ns, temp_itr);
			nsflag = cwp_true;
		}

		/* loop over key fields and remap */
		for (ikey = 0; ikey < nkeys; ++ikey) {
			/* get header values */
			ugethval(type1[ikey], &val1, type2[ikey],
				 ubyte[ikey] - 1, (char*) &tapetr,
				 endian, conv, verbose);
			puthval(&tr, index1[ikey], &val1);
		}
		/* Are there different swapping instructions for the data */
		/* Convert and write desired traces */
		if (++itr >= trmin) {
			/* Convert IBM floats to native floats */
			if (conv) {
				switch (bh.format) {
				case 1:
					/* Convert IBM float to native float*/
					ibm_to_float((int*) tr.data,
						     (int*) tr.data, ns,
						     swapdata, verbose);
					break;
				case 2:
					/* Convert 4 byte integer to native float*/
					int_to_float((int*) tr.data,
						     (float*) tr.data, ns,
						     swapdata);
					break;
				case 3:
					/* Convert 2 byte integer to native float*/
					short_to_float((short*) tr.data,
							(float*) tr.data, ns,
							swapdata);
					break;
				case 5:
					/* IEEE floats.
					 * Byte swap if necessary.
					 */
					if (swapdata == 0)
						for (i = 0; i < ns; ++i) {
							swap_float_4(&tr.
								     data[i]);
						}
					break;
				case 8:
					/*Convert 1 byte integer to native float*/
					integer1_to_float((signed char*)tr.data
							  ,(float*) tr.data,
							  ns);
					break;
				}

				/* Apply trace weighting. */
				if (trcwt && tr.trwf != 0) {
					float scale = pow(2.0, -tr.trwf);
					//int i;
					for (i = 0; i < ns; ++i) {
						tr.data[i] *= scale;
					}
				}
			} else if (conv == 0) {
				/* don't convert, if not appropriate */

				switch (bh.format) {
				case 1: /* swapdata=0 byte swapping */
				case 5:
					if (swapdata == 0)
						for (i = 0; i < ns; ++i) {
							swap_float_4(&tr.
								     data[i]);
						}
					break;
				case 2: /* convert longs to floats */
					/* SU has no provision for reading */
					/* data as longs */
					int_to_float((int*) tr.data,
						     (float*)tr.data,
						     ns, endian);
					break;
				case 3: /* shorts are the SHORTPAC format */
					/* used by supack2 and suunpack2 */
					if (swapdata == 0)/* swapdata=0 byte swap */
						for (i = 0; i < ns; ++i) {
							swap_short_2((short*)
								     &tr.
								     data[i]);
						}
					/* Set trace ID to SHORTPACK format */
					tr.trid = SHORTPACK;
					break;
				case 8: /* convert bytes to floats */
					/* SU has no provision for reading */
					/* data as bytes */
					integer1_to_float((signed char*)tr.data
							  ,(float*)tr.data,
							  ns);
					break;
				}
			}
			/* Write the trace to disk */
			tr.ns = ns;
//	            puttr(&tr);
			trace_obj_t *trace_obj;
			root_obj->number_of_traces++;

			rc = daos_seis_tr_obj_create(dfs, &trace_obj, itr, &tr);
			if (rc != 0) {
				err("Creating and writing trace data failed,"
				    " error code = %d \n", rc);
				return rc;
			}
			for(i=0; i< num_of_keys; i++) {
				rc = daos_seis_tr_linking(trace_obj,
							  seismic_obj[i],
							  keys[i]);
				if (rc != 0) {
					err("Linking trace to <%s> gather object failed,"
					    " error code = %d \n",keys[i], rc);
					return rc;
				}
			}
			/* Echo under verbose option */
			if (verbose && (itr % vblock) == 0) {
				warn(" %d traces from tape", itr);
			}

			daos_obj_close(trace_obj->oh, NULL);
			free(trace_obj);
		}
	}
	printf("All trace data written...\n");
	rc = daos_seis_root_update(root_obj, DS_D_FILE_HEADER,
				  DS_A_NTRACES_HEADER,
				  (char*) &(root_obj->number_of_traces),
				  sizeof(int),DAOS_IOD_SINGLE);
	if (rc != 0) {
		err("Updating number of traces of root seismic object failed, "
		    "error code = %d \n",rc);
		return rc;
	}

	for(i=0; i< num_of_keys; i++) {
		rc = update_gather_object(seismic_obj[i], DS_D_NGATHERS,
					  DS_A_NGATHERS,
					  (char*)&(seismic_obj[i]->number_of_gathers),
					  sizeof(int), DAOS_IOD_SINGLE);
		if (rc != 0) {
			err("Adding number of gathers to <%s> gather object"
			    " failed, error code = %d \n",keys[i], rc);
			return rc;
		}
	}

	printf("Updated all gathers numbers...\n");

	for(i=0; i< num_of_keys; i++) {
		rc = daos_seis_trace_oids_obj_create(dfs, OC_SX, seismic_obj[i]);
		if (rc != 0) {
			err("Creating array of object ids"
			    " failed, error code = %d \n",keys[i], rc);
			return rc;
		}
	}
	printf("Created trace oids obejcts \n");

	for(i=0; i< num_of_keys; i++) {
		rc = update_gather_traces(dfs, seismic_obj[i]->gathers, seismic_obj[i],
					  get_dkey(seismic_obj[i]->name), DS_A_NTRACES);
		if (rc != 0) {
			err("Updating number of traces key"
			    " failed, error code = %d \n",keys[i], rc);
			return rc;
		}
	}
	printf("Updated all gathers traces...\n");

	for(i=0; i< num_of_keys; i++) {
		free(seismic_obj[i]->seis_gather_trace_oids_obj);
		rc = daos_obj_close(seismic_obj[i]->oh, NULL);
		if (rc != 0) {
			err("Closing seismic object failed, "
			    "error code = %d \n",keys[i], rc);
			return rc;
		}
	}

	rc = daos_obj_close(root_obj->root_obj->oh, NULL);
	if (rc != 0) {
		err("Closing root seismic object failed, "
		    "error code = %d \n",keys[i], rc);
		return rc;
	}

	free(daos_tape);
	return rc;
}

traces_list_t*
daos_seis_sort_traces(seis_root_obj_t *root, int number_of_keys,
		      char **sort_keys, int *directions,
		      int number_of_window_keys, char **window_keys,
		      cwp_String *type, Value *min_keys, Value *max_keys)
{
	seismic_entry_t 	seismic_entry = {0};
	traces_list_t	       *trace_list;
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
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;


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
		return rc;
	}

	/**Fetch Number of Gathers Under opened seismic Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*) &seismic_object->number_of_gathers,
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
				   &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers in <%s> seismic object failed,"
		    " error code = %d \n", seismic_object->name, rc);
		return rc;
	}

//	seismic_object->gathers = malloc(seismic_object->number_of_gathers *
//					 sizeof(seis_gather_t));
	seismic_object->seis_gather_trace_oids_obj =
			malloc(seismic_object->number_of_gathers *
			       sizeof(trace_oid_oh_t));

	/**Fetch list of dkeys under seimsic_object */
	char **unique_keys;
	if ((seismic_object->number_of_gathers > 2)) {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 1,
						    seismic_object->name,
						    directions[0]);
	} else {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 0,
						    seismic_object->name,
						    directions[0]);
	}

	gather_traces = malloc(seismic_object->number_of_gathers *
			       sizeof(read_traces));

	for (i = 0; i < seismic_object->number_of_gathers; i++) {
		temp_number_of_keys = number_of_keys;
		/**Fetch number of traces */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_NTRACES,
				      (char*)&gather_traces[i].number_of_traces
				      , sizeof(int), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error"
			    " code = %d \n", rc);
			return rc;
		}
		trace_oid_oh_t gather_traces_oids =
				seismic_object->seis_gather_trace_oids_obj[i];

		/**Fetch trace headers object id */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_GATHER_TRACE_OIDS,
				      (char*)&gather_traces_oids.oid,
				      sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching traces headers object id failed, error"
			    " code = %d \n", rc);
			return rc;
		}
		int num_of_traces = gather_traces[i].number_of_traces;
		/**Allocate oids array , size = number of traces */
		daos_obj_id_t *oids = malloc(num_of_traces * sizeof(daos_obj_id_t));
		/** Fetch array of trace headers oids */
		rc = fetch_array_of_trace_headers(root, oids,
						  &gather_traces_oids,
						  num_of_traces);
		if(rc != 0) {
			err("Fetching array of traces headers failed, error"
			    " code = %d \n", rc);
			return rc;
		}
		gather_traces[i].traces = malloc(num_of_traces *
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
					 &gather_trace_list);
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
			fetch_traces_data(root->coh, &gather_trace_list,
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
		return rc;
	}
	/** free allocated memory */
	for(i=0; i<seismic_object->number_of_gathers; i++){
		free(unique_keys[i]);
		free(gather_traces[i].traces);
	}
	free(unique_keys);
	free(gather_traces);
//	free(seismic_object->gathers);
	free(seismic_object->seis_gather_trace_oids_obj);
	free(seismic_object);

	return trace_list;
}

traces_list_t* 
daos_seis_wind_traces(seis_root_obj_t *root, char **window_keys, 
		      int number_of_keys, Value *min_keys,
		      Value *max_keys, cwp_String *type)
{
	seismic_entry_t 	seismic_entry = {0};
	traces_list_t 	       *trace_list;
	seis_obj_t 	       *seismic_object;
	char 			temp[4096];
	int 			daos_mode;
	int 			key_exist = 0;
	int	 		rc;
	int 			i = 0;

	daos_mode = get_daos_obj_mode(O_RDWR);
	seismic_object = malloc(sizeof(seis_obj_t));
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;

	while(i < number_of_keys){
		if(strcmp(window_keys[i],root->keys[i]) == 0) {
			seismic_object->oid = root->gather_oids[i];
			strcpy(seismic_object->name, root->keys[i]);
			key_exist = 1;
		} else {
			i++;
			continue;
		}
		if(i >0){
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
		} else {
			break;
		}
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
		return rc;
	}

	/**Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*)&seismic_object->number_of_gathers,
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
				   &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		return rc;
	}

	seismic_object->seis_gather_trace_oids_obj =
				malloc(seismic_object->number_of_gathers *
				       sizeof(trace_oid_oh_t));

	char 		**unique_keys;
	/** Fetch list of dkeys under seimsic_object */
	if(seismic_object->number_of_gathers > 2) {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 1,
						    seismic_object->name, 1);
	} else {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 0,
						    seismic_object->name, 1);
	}
	/** Sort list of dkeys */
	long 		*first_array;
	first_array = malloc(seismic_object->number_of_gathers * sizeof(long));
	sort_dkeys_list(first_array, seismic_object->number_of_gathers, unique_keys, 1);

	int 		number_of_traces = 0;
 	for(i=0; i< seismic_object->number_of_gathers; i++){
		/** Check bool and shot id number,
		 *  if out of range --> continue.
		 */
 		if((key_exist == 1) &&
 		   (first_array[i]< vtol(type[0], min_keys[0]) ||
		    first_array[i] > vtol(type[0], max_keys[0]))) {
 			warn("key is out of range \n");
 			continue;
 		}

 		/** Fetch number of traces */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_NTRACES,
				      (char*)&number_of_traces, sizeof(int),
				      DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			return rc;
		}
		trace_oid_oh_t gather_traces_oids =
				seismic_object->seis_gather_trace_oids_obj[i];
		/** Fetch trace headers object id */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_GATHER_TRACE_OIDS,
				      (char*)&gather_traces_oids.oid,
				      sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching traces headers oid failed, error "
			    "code = %d \n", rc);
			return rc;
		}
		/** Allocate oids array , size = number of traces */
		daos_obj_id_t *oids = malloc(number_of_traces * sizeof(daos_obj_id_t));
		/** Fetch array of trace headers oids*/
		rc = fetch_array_of_trace_headers(root, oids,
						  &gather_traces_oids,
						  number_of_traces);
		if(rc != 0) {
			err("Fetching array of traces headers oids failed, error"
			    " code = %d \n", rc);
			return rc;
		}

		fetch_traces_header_traces_list(root->coh, oids, &trace_list,
						daos_mode,number_of_traces);
		free(oids);
 	}
 	/** apply window on headers fetched */
	window_headers(&trace_list, window_keys, number_of_keys,
		       type, min_keys, max_keys);
	/** Fetch traces data */
	fetch_traces_data(root->coh, &trace_list, daos_mode);
	/** Close seismic object */
	rc = daos_obj_close(seismic_object->oh, NULL);
	if(rc != 0) {
		err("Closing seismic object failed, error code = %d \n", rc);
		return rc;
	}
	/** free allocated memory */
	free(first_array);
	free(seismic_object->seis_gather_trace_oids_obj);
	free(seismic_object);

	return trace_list;
}

void
daos_seis_set_headers(dfs_t *dfs, seis_root_obj_t *root,
		      int num_of_keys, char **keys_1, char **keys_2,
		      char **keys_3, double *a, double *b, double *c,
		      double *d, double *j, double *e, double *f,
		      header_operation_type_t type)
{
	seismic_entry_t 	seismic_entry = {0};
	trace_oid_oh_t 		gather_traces_oids;
	daos_obj_id_t 	       *oids;
	traces_list_t	       *trace_list;
	seis_obj_t 	       *seismic_object;
	int 			number_of_traces = 0;
	int			existing_keys[num_of_keys];
	int 			daos_mode;
	int 			rc;
	int 			i;
	int			k;

	seismic_object = malloc(sizeof(seis_obj_t));
	daos_mode = get_daos_obj_mode(O_RDWR);
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;

	for (i = 0; i < num_of_keys; i++) {
		existing_keys[i] = 0;
		for(k = 0; k < num_of_keys; k++) {
			if(strcmp(root->keys[k], keys_1[i]) == 0) {
				existing_keys[i] = 1;
				break;
			}
		}
	}

	seismic_object->oid = root->gather_oids[0];
	strcpy(seismic_object->name, root->keys[0]);

	rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
			   &(seismic_object->oh), NULL);
	if (rc != 0) {
		err("Opening seismic object failed, error"
		    " code = %d \n", rc);
		return;
	}

	/** Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*) &seismic_object->number_of_gathers,
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
				   &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		return;
	}

	char 		**unique_keys;
	/** Fetch list of dkeys under seimsic_object */
	if (seismic_object->number_of_gathers > 2) {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 1,
						    seismic_object->name, 1);
	} else {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 0,
						    seismic_object->name, 1);
	}

	seismic_object->seis_gather_trace_oids_obj =
			malloc(seismic_object->number_of_gathers *
			       sizeof(trace_oid_oh_t));

	for (i = 0; i < seismic_object->number_of_gathers; i++) {
		/**Fetch number of traces */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_NTRACES,
				      (char*) &number_of_traces, sizeof(int),
				      DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
				&seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			return;
		}
		gather_traces_oids =
				seismic_object->seis_gather_trace_oids_obj[i];

		/** Fetch trace headers object id. */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				unique_keys[i], DS_A_GATHER_TRACE_OIDS,
				(char*) &gather_traces_oids.oid,
				sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching traces headers oid failed, error "
			    "code = %d \n", rc);
			return;
		}

		/** Allocate oids array , size = number of traces */
		oids = malloc(number_of_traces * sizeof(daos_obj_id_t));
		/** Fetch array of trace headers oids */
		rc = fetch_array_of_trace_headers(root, oids,
						  &gather_traces_oids,
						  number_of_traces);
		if(rc != 0) {
			err("Fetching array of traces headers oids failed,"
			    " error code = %d \n", rc);
			return;
		}

		fetch_traces_header_traces_list(root->coh, oids, &trace_list,
						daos_mode, number_of_traces);
		free(oids);
	}
	/** Set new traces headers */
	set_traces_header(root->coh, daos_mode, &trace_list, num_of_keys,
			  keys_1, keys_2, keys_3, a, b, c, d, e, f, j, type);
	/** check if the header keys was one of the already created gathers.
	 *  if yes then the gather object should be replaced by a new object
	 *  with the new key values.
	 */
	for(i = 0; i < num_of_keys; i++) {
		if(existing_keys[i] == 1) {
		daos_seis_replace_objects(dfs, daos_mode, keys_1[i],
					  trace_list, root);
		}
	}

//	if (shot_header_key == 1 || cmp_header_key == 1 ||
//	    offset_header_key == 1) {
//		for (i = 0; i < num_of_keys; i++) {
//			/** if key matches with fldr object*/
//			if (strcmp("fldr", keys_1[i]) == 0) {
//				shot_header_key = 1;
//				cmp_header_key = 0;
//				offset_header_key = 0;
//				/** Replace shot object with a new one */
//				daos_seis_replace_objects(dfs, daos_mode,
//							  shot_header_key,
//							  cmp_header_key,
//							  offset_header_key,
//							  trace_list, root);
//			} else if (strcmp("cdp", keys_1[i]) == 0) {
//				shot_header_key = 0;
//				cmp_header_key = 1;
//				offset_header_key = 0;
//				daos_seis_replace_objects(dfs, daos_mode,
//							  shot_header_key,
//							  cmp_header_key,
//							  offset_header_key,
//							  trace_list, root);
//			} else if (!strcmp("offset", keys_1[i])) {
//				shot_header_key = 0;
//				cmp_header_key = 0;
//				offset_header_key = 1;
//				daos_seis_replace_objects(dfs, daos_mode,
//							  shot_header_key,
//							  cmp_header_key,
//							  offset_header_key,
//							  trace_list, root);
//			} else {
//				continue;
//			}
//		}
//	}
	/** close opened seismic object */
	rc = daos_obj_close(seismic_object->oh, NULL);
	if(rc != 0) {
		err("Closing seismic object failed, error code = %d\n", rc);
		return;
	}
	/** Release traces list */
	release_traces_list(trace_list);
	/** Free allocated memory */
	free(seismic_object->seis_gather_trace_oids_obj);
	free(seismic_object);
}

headers_ranges_t
daos_seis_range_headers(seis_root_obj_t *root, int number_of_keys,
			char **keys, int dim)
{
	seismic_entry_t 	seismic_entry = {0};
	trace_oid_oh_t		gather_traces_oids;
	traces_list_t 	       *trace_list;
	daos_obj_id_t 	       *oids;
	seis_obj_t 	       *seismic_object;
	int 			daos_mode;
	int 			rc;
	int 			i;

	seismic_object = malloc(sizeof(seis_obj_t));
	daos_mode = get_daos_obj_mode(O_RDWR);
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;

	seismic_object->oid = root->gather_oids[0];
	strcpy(seismic_object->name, root->keys[0]);

	rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
			   &(seismic_object->oh), NULL);
	if (rc != 0) {
		err("Opening seismic object failed, error"
		    " code = %d \n", rc);
		exit(rc);
	}

	/** Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*) &seismic_object->number_of_gathers,
			      sizeof(int), DAOS_IOD_SINGLE);
	rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
				   &seismic_entry, NULL);
	if (rc != 0) {
		err("Fetching number of gathers failed, error "
		    "code = %d \n", rc);
		exit(rc);
	}

	/** Fetch list of dkeys under seimsic_object */
	char **unique_keys;
	if (seismic_object->number_of_gathers > 2) {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 1,
						    seismic_object->name, 1);
	} else {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 0,
						    seismic_object->name, 1);
	}

	seismic_object->seis_gather_trace_oids_obj =
			malloc(seismic_object->number_of_gathers *
			       sizeof(trace_oid_oh_t));

	int 		num_of_traces = 0;
	for (i = 0; i < seismic_object->number_of_gathers; i++) {
		/** Fetch number of traces */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_NTRACES,
				      (char*) &num_of_traces, sizeof(int),
				      DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			exit(rc);
		}

		gather_traces_oids =
				seismic_object->seis_gather_trace_oids_obj[i];

		/** Fetch trace headers object id */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_GATHER_TRACE_OIDS,
				      (char*) &gather_traces_oids.oid,
				      sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching traces headers oid failed, error "
			    "code = %d \n", rc);
			exit(rc);
		}

		/** Allocate oids array , size = number of traces */
		oids = malloc(num_of_traces * sizeof(daos_obj_id_t));
		/** Fetch array of trace headers oids */
		rc = fetch_array_of_trace_headers(root, oids,
						  &gather_traces_oids,
						  num_of_traces);
		if(rc != 0) {
			err("Fetching array of traces headers oids failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}

		fetch_traces_header_traces_list(root->coh, oids, &trace_list,
						daos_mode, num_of_traces);
		free(oids);
	}
	/** Get ranges of fetched headers list */
	headers_ranges_t ranges = range_traces_headers(trace_list,
						       number_of_keys, keys,
						       dim);

	/** Release traces list */
	release_traces_list(trace_list);
	/** Free allocated memory */
	free(seismic_object->seis_gather_trace_oids_obj);
	free(seismic_object);

	return ranges;
}

traces_list_t*
daos_seis_get_headers(seis_root_obj_t *root)
{
	seismic_entry_t 	seismic_entry = {0};
	trace_oid_oh_t		gather_traces_oids;
	traces_list_t 	       *trace_list;
	daos_obj_id_t 	       *oids;
	seis_obj_t 	       *seismic_object;
	int 			daos_mode;
	int			rc;
	int 			i;

	daos_mode = get_daos_obj_mode(O_RDWR);
	seismic_object = malloc(sizeof(seis_obj_t));
	trace_list = malloc(sizeof(traces_list_t));
	trace_list->head = NULL;
	trace_list->tail = NULL;
	trace_list->size = 0;

	seismic_object->oid = root->gather_oids[0];
	strcpy(seismic_object->name, root->keys[0]);

	rc = daos_obj_open(root->coh, seismic_object->oid, daos_mode,
			   &(seismic_object->oh), NULL);
	if (rc != 0) {
		err("Opening seismic object failed, error"
		    " code = %d \n", rc);
		exit(rc);
	}

	/** Fetch Number of Gathers Under opened Gather object */
	prepare_seismic_entry(&seismic_entry, seismic_object->oid,
			      DS_D_NGATHERS, DS_A_NGATHERS,
			      (char*) &seismic_object->number_of_gathers,
			      sizeof(int),DAOS_IOD_SINGLE);

	rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
			&seismic_entry, NULL);

	char **unique_keys;

	if (seismic_object->number_of_gathers > 2) {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 1,
						    seismic_object->name, 1);
	} else {
		unique_keys = daos_seis_fetch_dkeys(seismic_object, 0,
						    seismic_object->name, 1);
	}

	seismic_object->seis_gather_trace_oids_obj =
			malloc(seismic_object->number_of_gathers *
			       sizeof(trace_oid_oh_t));

	int number_of_traces = 0;
	for (i = 0; i < seismic_object->number_of_gathers; i++) {
		/** Fetch number of traces */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				      unique_keys[i], DS_A_NTRACES,
				      (char*) &number_of_traces, sizeof(int),
				      DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
					   &seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching number of traces failed, error "
			    "code = %d \n", rc);
			exit(rc);
		}
		gather_traces_oids =
				seismic_object->seis_gather_trace_oids_obj[i];

		/** Fetch trace headers object id */
		prepare_seismic_entry(&seismic_entry, seismic_object->oid,
				unique_keys[i], DS_A_GATHER_TRACE_OIDS,
				(char*) &gather_traces_oids.oid,
				sizeof(daos_obj_id_t), DAOS_IOD_SINGLE);
		rc = daos_seis_fetch_entry(seismic_object->oh, DAOS_TX_NONE,
				&seismic_entry, NULL);
		if (rc != 0) {
			err("Fetching traces headers oid failed, error "
			    "code = %d \n", rc);
			exit(rc);
		}

		/** Allocate oids array , size = number of traces */
		oids = malloc(number_of_traces * sizeof(daos_obj_id_t));
		/** Fetch array of trace headers oids */
		rc = fetch_array_of_trace_headers(root, oids,
						  &gather_traces_oids,
						  number_of_traces);
		if(rc != 0) {
			err("Fetching array of traces headers oids failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}

		fetch_traces_header_traces_list(root->coh, oids, &trace_list,
				daos_mode, number_of_traces);
		free(oids);
	}
	/** Free allocated memory */
	free(seismic_object->seis_gather_trace_oids_obj);
	free(seismic_object);

	return trace_list;
}

void
daos_seis_set_data(seis_root_obj_t *root, traces_list_t *trace_list)
{
	traces_headers_t 	*current;
	trace_oid_oh_t 		data_oids;
	int 			 rc;
	int 			 i;

	current = trace_list->head;

	for (i = 0; i < trace_list->size; i++) {
		data_oids.oid =
			get_tr_data_oid(&(current->trace.trace_header_obj),
				        OC_SX);
		segy *trace = trace_to_segy(&current->trace);

		rc = daos_array_open_with_attr(root->coh, data_oids.oid,
					       DAOS_TX_NONE, DAOS_OO_RW, 1,
					       200 * sizeof(float),
					       &data_oids.oh, NULL);
		if (rc != 0) {
			err("Opening trace data object failed, "
			    "error code = %d \n", rc);
			exit(rc);
		}
		rc = daos_seis_tr_data_update(&data_oids, trace);
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

