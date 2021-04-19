/*
 * source_interface.h
 *
 *  Created on: Jan 28, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_SOURCE_INTERFACE_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_SOURCE_INTERFACE_H_

#include "data_types/trace.h"
#ifdef MPI_BUILD
#include "utilities/mutex.h"
#endif
typedef enum{
	SEGY,
	LL,
	BINARY,
	RAW,
	HEADER
}source_type_t;

typedef struct parse_functions {
	void *parse_parameters;
	int (*parse_parameters_init) (void **, void *);
	int (*parse_file_headers) (void *);
	char* (*get_text_header) (void *);
	char* (*get_binary_header) (void *);
	int (*get_num_of_exth) (void *);
	char* (*get_exth) (void *);
	trace_t* (*get_trace) (void *);
	int (*release_parsing_parameters)(void *);
	int (*read_junk)(void *);
	int (*set_raw_ns)(void**, int);
}parse_functions_t;

typedef struct parsing_helpers {
#ifdef MPI_BUILD
	mpi_mutex_t *traces_mutex;
	mpi_mutex_t *gather_mutex;
#endif
}parsing_helpers_t;

/** Function responsible for initializing parsing parameters based on
 *  the seismic source type.
 *
 *  /param[in]		seismic_source_type	seismic source type
 *  						(SEGY, LL, BINARY)
 *  /param[in]		parse_functions		double pointer to parsing
 *  						functions struct to be allocated
 *  						and initialized with the proper
 *  						parsing functions.
 * /param[in] 		source			void pointer to the seismic
 * 						source to be used during parsing
 *
 * /return	0 on success
 * 		error code otherwise.
 */
int
init_parsing_parameters(source_type_t seismic_source_type,
			parse_functions_t **parse_functions,
			void *source);
/** Function responsible for calling proper function to
 *  parse only binary and text headers from a seismic source.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
parse_text_and_binary_headers(parse_functions_t *parse_functions);

/** Function responsible for calling proper function to
 *  return text header.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
char*
get_text_header(parse_functions_t *parse_functions);

/** Function responsible for calling proper function to
 *  return binary header.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
char*
get_binary_header(parse_functions_t *parse_functions);

/** Function responsible for calling proper function to
 *  get the number of extended headers in a seismic source.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
get_number_of_extended_headers(parse_functions_t *parse_functions);

/** Function responsible for calling proper function to
 *  return an extended text header from a seismic source.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
char*
get_extended_header(parse_functions_t *parse_functions);

/** Function responsible for calling proper function to
 *  parse, process if needed and return a trace.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
trace_t*
get_trace(parse_functions_t *parse_functions);

/** Function responsible for calling proper function to
 *  release all previously allocated parsing parameters.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
release_parsing_parameters(parse_functions_t *parse_functions);


/** Function responsible for calling proper function to
 *  read redundant bytes of fortran records.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
read_junk(parse_functions_t *parse_functions);


/** Function responsible for calling proper function to
 *  set ns value for parsing raw files.
 *
 * /param[in]		parse_functions		pointer to parsing functions
 * 						struct holding previously
 * 						initialized function pointers.
 * /param[in]		ns			ns value to be set
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
set_raw_ns(parse_functions_t **parse_functions, int ns);


#endif
/* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_SOURCE_INTERFACE_H_ */
