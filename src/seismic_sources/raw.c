/*
 * raw.c
 *
 *  Created on: Feb 23, 2021
 *      Author: omar
 */
#include "seismic_sources/raw.h"

int
init_raw_parameters(void **parameters, void *raw_source)
{
	raw_parameters_t **raw_parameters = (raw_parameters_t**) parameters;

	(*raw_parameters) = malloc(sizeof(raw_parameters_t));
	(*raw_parameters)->daos_tape = malloc(sizeof(DAOS_FILE));
	(*raw_parameters)->daos_tape->file = malloc(sizeof(dfs_obj_t));
	(*raw_parameters)->daos_tape->file = (dfs_obj_t*) raw_source;
	(*raw_parameters)->daos_tape->offset = 0;
	(*raw_parameters)->ns = 0;
	return 0;
}

int
release_raw_parameters(void *parameters)
{
	raw_parameters_t *raw_parameters = (raw_parameters_t*) parameters;
	free(raw_parameters->daos_tape->file);
	free(raw_parameters->daos_tape);
	free(raw_parameters);
	return 0;
}

int
init_hdr_parameters(void **parameters, void *hdr_source)
{
	hdr_parameters_t **hdr_parameters = (hdr_parameters_t**) parameters;

	(*hdr_parameters) = malloc(sizeof(hdr_parameters_t));
	(*hdr_parameters)->daos_tape = malloc(sizeof(DAOS_FILE));
	(*hdr_parameters)->daos_tape->file = malloc(sizeof(dfs_obj_t));
	(*hdr_parameters)->daos_tape->file = (dfs_obj_t*) hdr_source;
	(*hdr_parameters)->daos_tape->offset = 0;
	return 0;
}

int
release_hdr_parameters(void *parameters)
{
	hdr_parameters_t *hdr_parameters = (hdr_parameters_t*) parameters;
	free(hdr_parameters->daos_tape->file);
	free(hdr_parameters->daos_tape);
	free(hdr_parameters);
	return 0;
}

int
init_raw_parse_functions(parse_functions_t **parsing_functions)
{
	(*parsing_functions) = malloc(sizeof(parse_functions_t));
	raw_parameters_t *parameters =(raw_parameters_t*)
				      (*parsing_functions)->parse_parameters;
	int (*init_parameters)(void**, void*) = &init_raw_parameters;
	(*parsing_functions)->parse_parameters_init = init_parameters;
	trace_t* (*get_trace)(void*) = &get_raw_trace;
	(*parsing_functions)->get_trace = get_trace;
	int (*release_parameters)(void*) = &release_raw_parameters;
	(*parsing_functions)->release_parsing_parameters = release_parameters;
	(*parsing_functions)->parse_file_headers = NULL;
	(*parsing_functions)->get_text_header = NULL;
	(*parsing_functions)->get_binary_header = NULL;
	(*parsing_functions)->get_num_of_exth = NULL;
	(*parsing_functions)->get_exth = NULL;
	int (*read_junk)(void*) = &read_fortran_record;
	(*parsing_functions)->read_junk = read_junk;
	int (*set_raw_ns)(void**, int) = &set_ns;
	(*parsing_functions)->set_raw_ns = set_raw_ns;
	return 0;
}

int
init_hdr_parse_functions(parse_functions_t **parsing_functions)
{
	(*parsing_functions) = malloc(sizeof(parse_functions_t));
	hdr_parameters_t *parameters =(hdr_parameters_t*)
				      (*parsing_functions)->parse_parameters;
	int (*init_parameters)(void**, void*) = &init_hdr_parameters;
	(*parsing_functions)->parse_parameters_init = init_parameters;
	trace_t* (*get_trace)(void*) = &get_hdr_trace;
	(*parsing_functions)->get_trace = get_trace;
	int (*release_parameters)(void*) = &release_hdr_parameters;
	(*parsing_functions)->release_parsing_parameters = release_parameters;
	(*parsing_functions)->parse_file_headers = NULL;
	(*parsing_functions)->get_text_header = NULL;
	(*parsing_functions)->get_binary_header = NULL;
	(*parsing_functions)->get_num_of_exth = NULL;
	(*parsing_functions)->get_exth = NULL;
	(*parsing_functions)->read_junk = NULL;
	return 0;
}

trace_t*
get_raw_trace(void *parameters)
{
	int size;
	int nread;
	int rc;
	int ns;
	trace_t *tr;
	raw_parameters_t *raw_parameters = (raw_parameters_t*) parameters;
	rc = trace_init(&tr);
	DSG_ERROR(rc, "Error initializng trace in get_raw_trace function",
		  error1);

	ns = raw_parameters->ns;
	tr->data = malloc(ns * sizeof(float));
	size = read_dfs_file(raw_parameters->daos_tape, (char*) tr->data,
			     ns * sizeof(float));
	nread = size / sizeof(float);
	if (nread != ns) {
		return NULL;
	}
	//memcpy(tr->data, segy_tr.data, raw_parameters->ns * sizeof(float));
	return tr;

error1:
	return NULL;
}

trace_t*
get_hdr_trace(void *parameters)
{
	uint64_t size;
	int nread;
	int rc;
	trace_t *tr;
	hdr_parameters_t *hdr_parameters = (hdr_parameters_t*) parameters;

	rc = trace_init(&tr);
	DSG_ERROR(rc, "Error initializing trace in get_hdr_trace function",
		  error1);


	tr->data = NULL;
	size = read_dfs_file(hdr_parameters->daos_tape, (char*) tr,
			     TRACEHDR_BYTES);
	nread = size;
	if (nread == 0) {
		return NULL;
	}
	return tr;

error1:
	return NULL;
}

int
read_fortran_record(void *parameters)
{
	char junk[sizeof(int)];
	raw_parameters_t *raw_parameters = (raw_parameters_t*) parameters;

	read_dfs_file(raw_parameters->daos_tape, junk, sizeof(int));

	return 0;
}

int
set_ns(void **parameters, int ns)
{
	raw_parameters_t **raw_parameters = (raw_parameters_t**) parameters;
	(*raw_parameters)->ns = ns;
	return 0;

}
