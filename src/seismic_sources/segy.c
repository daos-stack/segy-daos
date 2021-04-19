/*
 * segy.c
 *
 *  Created on: Jan 30, 2021
 *      Author: mirnamoawad
 */

#include "seismic_sources/source_interface.h"
#include "seismic_sources/segy.h"
#include "segy_helpers.c"
//#include <err.h>

int
init_segy_parameters(void **parameters, void *segy_source)
{
	segy_parameters_t **segy_parameters = (segy_parameters_t **)parameters;

	int	i;
	union {
		short s;
		char c[2];
	} testend;
	testend.s = 1;

	(*segy_parameters) = malloc(sizeof(segy_parameters_t));
	(*segy_parameters)->bh = malloc(sizeof(bhed));
	(*segy_parameters)->conv = 1;
	(*segy_parameters)->daos_tape = malloc(sizeof(DAOS_FILE));
	(*segy_parameters)->daos_tape->file = malloc(sizeof(dfs_obj_t));
	(*segy_parameters)->daos_tape->file = (dfs_obj_t *)segy_source;
	(*segy_parameters)->daos_tape->offset = 0;
	(*segy_parameters)->ebcbuf = malloc((EBCBYTES+1) * sizeof(char));
	(*segy_parameters)->ebcbuf[EBCBYTES] = '\0';
	(*segy_parameters)->exthbuf = malloc(EBCBYTES * sizeof(char));
	(*segy_parameters)->endian = (testend.c[0] == '\0') ? 1 : 0;

	(*segy_parameters)->format_set = 1;
	(*segy_parameters)->over = 0;
	/** what is format default value ?? */
	(*segy_parameters)->format = 0;
	(*segy_parameters)->itr = 0;
	(*segy_parameters)->nextended = 0;
	(*segy_parameters)->ns = 0;
	(*segy_parameters)->nsegy = 0;
	(*segy_parameters)->nsflag = 0;
	(*segy_parameters)->swapbhed = (*segy_parameters)->endian;
	(*segy_parameters)->swapdata = (*segy_parameters)->endian;
	(*segy_parameters)->swaphdrs = (*segy_parameters)->endian;
	(*segy_parameters)->tapetr = malloc(sizeof(tapesegy));
	(*segy_parameters)->tr = malloc(sizeof(segy));
	(*segy_parameters)->trcwt = 0;
	(*segy_parameters)->trmin = 1;
	(*segy_parameters)->verbose = 1;
	return 0;
}

static int
process_headers(bhed *bh, int format, int over, int format_set, int *trcwt,
		int verbose, int *ns, size_t *nsegy)
{
	/* Override binary format value */
	over = 0;
	if (((over != 0) && (format_set))) {
		bh->format = format;
	}
	/* Override application of trace weighting factor?
	 *
	 * Default no for floating point formats, yes for integer formats.
	 */

	*trcwt = (bh->format == 1 || bh->format == 5) ? 0 : 1;

	switch (bh->format) {
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
//			err("format not SEGY standard (1, 2, 3, 5, or 8)");
		}

	}

	/* Compute length of trace (can't use sizeof here!) */
	*ns = bh->hns; /* let user override */
	if (!(*ns)) {
//		err("samples/trace not set in binary header");
	}
	bh->hns = *ns;

	switch (bh->format) {
	case 8:
		*nsegy = *ns + SEGY_HDRBYTES;
		break;
	case 3:
		*nsegy = *ns * 2 + SEGY_HDRBYTES;
		break;
	case 1:
	case 2:
	case 5:
	default:
		*nsegy = *ns * 4 + SEGY_HDRBYTES;
	}

	return 0;
}

static int
process_trace(tapesegy tapetr, segy *tr, bhed bh, int ns, int swaphdrs,
	      int nsflag, int *itr,
	      int endian, int conv,
	      int swapdata, int trmin, int trcwt,
	      int verbose)
{
	generic_value	val1;
	int 		ikey;
	int 		i;

	/* Convert from bytes to ints/shorts */
	tapesegy_to_segy(&tapetr, tr);

	/* If little endian machine, then swap bytes in trace header */
	if (swaphdrs == 0) {
		for (i = 0; i < SEGY_NKEYS; ++i) {
			swaphval(tr, i);
		}
	}

	/* Check tr.ns field */
	if (!nsflag && ns != tr->ns) {
		int temp_itr = *itr + 1;
		warn("discrepant tr.ns = %d with tape/user ns = %d\n\t"
		     "... first noted on trace %d",
		     tr->ns, ns, temp_itr);
		nsflag = 1;
	}
	/* Are there different swapping instructions for the data
	 *
	 * Convert and write desired traces
	 */
	if (++(*itr) >= trmin) {
		/* Convert IBM floats to native floats */
		if (conv) {
			switch (bh.format) {
			case 1:
				/* Convert IBM float to native float*/
				ibm_to_float((int*) tr->data,
					     (int*) tr->data, ns,
					     swapdata, verbose);
				break;
			case 2:
				/* Convert 4 byte integer to native float*/
				int_to_float((int*) tr->data,
					     (float*) tr->data, ns,
					     swapdata);
				break;
			case 3:
				/* Convert 2 byte integer to native float*/
				short_to_float((short*) tr->data,
						(float*) tr->data, ns,
						swapdata);
				break;
			case 5:
				/* IEEE floats.
				 * Byte swap if necessary.
				 */
				if (swapdata == 0)
					for (i = 0; i < ns; ++i) {
						swap_float_4(&tr->
							     data[i]);
					}
				break;
			case 8:
				/*Convert 1 byte integer to native float*/
				integer1_to_float((signed char*)tr->data,
						  (float*) tr->data,
						  ns);
				break;
			}
			/* Apply trace weighting. */
			if (trcwt && tr->trwf != 0) {
				float scale = pow(2.0, -tr->trwf);
				//int i;
				for (i = 0; i < ns; ++i) {
					tr->data[i] *= scale;
				}
			}
		} else if (conv == 0) {
			/* don't convert, if not appropriate */

			switch (bh.format) {
			case 1: /* swapdata=0 byte swapping */
			case 5:
				if (swapdata == 0) {
					for (i = 0; i < ns; ++i) {
						swap_float_4(&tr->data[i]);
					}
				}
				break;
			case 2: /* convert longs to floats */
				/* SU has no provision for reading */
				/* data as longs */
				int_to_float((int*) tr->data,
					     (float*)tr->data,
					     ns, endian);
				break;
			case 3: /* shorts are the SHORTPAC format */
				/* used by supack2 and suunpack2 */
				if (swapdata == 0)/* swapdata=0 byte swap */
					for (i = 0; i < ns; ++i) {
						swap_short_2((short*)
							     &tr->
							     data[i]);
					}
				/* Set trace ID to SHORTPACK format */
				tr->trid = SHORTPACK;
				break;
			case 8: /* convert bytes to floats */
				/* SU has no provision for reading */
				/* data as bytes */
				integer1_to_float((signed char*)tr->data
						  ,(float*)tr->data,
						  ns);
				break;
			}
		}
		/* Write the trace to disk */
		tr->ns = ns;
	}

	return 0;
}

int
parse_segy_bh_th(void *parameters)
{
	uint64_t		size = 0;
	tapebhed 		tapebh;
	int 			i;
	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;
	/* flag for bhed.float override*/
	/* Override binary format value */
	segy_parameters->over = 0;
	/** Read_text_header */
	size = read_dfs_file(segy_parameters->daos_tape,
			     segy_parameters->ebcbuf, EBCBYTES);

	/** Read_binary_header */
	size = read_dfs_file(segy_parameters->daos_tape,
			     (char*) &tapebh, BNYBYTES);
	/* Convert from bytes to ints/shorts */
	tapebhed_to_bhed(&tapebh, segy_parameters->bh);
	/* if little endian machine, swap bytes in binary header */
	if (segy_parameters->swapbhed == 0) {
		for (i = 0; i < BHED_NKEYS; ++i) {
			swapbhval(segy_parameters->bh, i);
		}
	}

	segy_parameters->nextended = *((short*)
			              (((unsigned char*) &tapebh) + 304));
	if (segy_parameters->endian == 0) {
		swap_short_2(&segy_parameters->nextended);
	}

	process_headers(segy_parameters->bh, segy_parameters->format,
			segy_parameters->over, segy_parameters->format_set,
			&segy_parameters->trcwt, segy_parameters->verbose,
			&segy_parameters->ns, &segy_parameters->nsegy);

	return 0;
}

int
init_segy_parse_functions(parse_functions_t **parsing_functions)
{
	(*parsing_functions) = malloc(sizeof(parse_functions_t));
	segy_parameters_t *parameters = (segy_parameters_t *)
					(*parsing_functions)->parse_parameters;
	int (*init_parameters)(void **, void *) = &init_segy_parameters;
	(*parsing_functions)->parse_parameters_init = init_parameters;
	int (*parse_headers)(void *) = &parse_segy_bh_th;
	(*parsing_functions)->parse_file_headers = parse_headers;
	char * (*get_text_hdr)(void *) = &get_segy_text_header;
	(*parsing_functions)->get_text_header = get_text_hdr;
	char * (*get_binary_hdr)(void *) = &get_segy_binary_header;
	(*parsing_functions)->get_binary_header = get_binary_hdr;
	int (*get_num_exth)(void*) = &get_segy_num_of_extended_headers;
	(*parsing_functions)->get_num_of_exth = get_num_exth;
	char * (*get_extended_hdr)(void *) = &get_segy_extended_file_header;
	(*parsing_functions)->get_exth = get_extended_hdr;
	trace_t * (*get_trace)(void *) = &get_segy_trace;
	(*parsing_functions)->get_trace = get_trace;
	int (*release_parameters)(void *) = &release_segy_parameters;
	(*parsing_functions)->release_parsing_parameters = release_parameters;
	(*parsing_functions)->read_junk = NULL;
	(*parsing_functions)->set_raw_ns = NULL;
	return 0;
}

char *
get_segy_text_header(void *parameters)
{
	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;

	return segy_parameters->ebcbuf;
}

char *
get_segy_binary_header(void *parameters)
{
	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;

	return (char*)segy_parameters->bh;
}

int
get_segy_num_of_extended_headers(void *parameters)
{
	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;
	return segy_parameters->nextended;
}

char *
get_segy_extended_file_header(void *parameters)
{
	uint64_t	size;

	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;
	size = read_dfs_file(segy_parameters->daos_tape,
			     segy_parameters->exthbuf, EBCBYTES);
	return segy_parameters->exthbuf;
}

int
segy_to_trace(trace_t *trace, segy *segy_trace)
{
	memcpy(trace, segy_trace, TRACEHDR_BYTES);
	if(trace->ns > 0){
		trace->data = malloc(trace->ns * sizeof(float));
		memcpy(trace->data, segy_trace->data,trace->ns * sizeof(float));
	}
	return 0;
}

trace_t*
get_segy_trace(void *parameters)
{
	uint64_t	size;
	int		nread;
	int		rc;
	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;

	size = read_dfs_file(segy_parameters->daos_tape,
			     (char*)segy_parameters->tapetr,
			     segy_parameters->nsegy);
	nread = size;
	if(nread == 0) {
		return NULL;
	}

	process_trace(*(segy_parameters->tapetr), segy_parameters->tr,
		      *(segy_parameters->bh), segy_parameters->ns,
		      segy_parameters->swaphdrs, segy_parameters->nsflag,
		      &(segy_parameters->itr),
		      segy_parameters->endian, segy_parameters->conv,
		      segy_parameters->swapdata, segy_parameters->trmin,
		      segy_parameters->trcwt, segy_parameters->verbose);

	trace_t *trace;

	rc = trace_init(&trace);
	DSG_ERROR(rc, "Error initializing trace in get_segy_trace function",
		  error);
	trace->data = NULL;
	rc = segy_to_trace(trace, segy_parameters->tr);
	DSG_ERROR(rc, "Error initializing trace in get_hdr_trace function",
		  error2);
	return trace;

error2:
	trace_destroy(trace);
error:
	return NULL;
}

int
release_segy_parameters(void *parameters)
{
	int i;
	segy_parameters_t *segy_parameters = (segy_parameters_t *)parameters;
	free(segy_parameters->bh);
	free(segy_parameters->daos_tape->file);
	free(segy_parameters->daos_tape);
	free(segy_parameters->ebcbuf);
	free(segy_parameters->exthbuf);
	free(segy_parameters->tapetr);
	free(segy_parameters->tr);
	free(segy_parameters);

	return 0;
}

