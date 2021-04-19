/*
 * raw.h
 *
 *  Created on: Feb 23, 2021
 *      Author: omar
 */

#ifndef DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_RAW_H_
#define DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_RAW_H_

#include "daos_primitives/dfs_helpers.h"
#include "seismic_sources/segy.h"
#include "data_types/trace.h"
#include "utilities/error_handler.h"

typedef struct raw_parameters {
	DAOS_FILE 	*daos_tape;
	int		ns;
}raw_parameters_t;

typedef struct hdr_parameters{
	DAOS_FILE 	*daos_tape;
}hdr_parameters_t;

/** Function responsible for initializing raw parameters.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						raw_parameters struct and
 * 						initialized to point to the
 * 						ensemble list to be parsed.
 * /param[in]		source			void pointer to the mounted
 * 						raw file.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
init_raw_parameters(void **parameters, void *raw_source);
/** Function responsible for releasing raw previously
 *  allocated parameters.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						raw_parameters struct and
 * 						used to release any allocated
 * 						member.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */

int
release_raw_parameters(void *parameters);

/** Function responsible for initializing header file parameters.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						hdr_parameters struct and
 * 						initialized to point to the
 * 						ensemble list to be parsed.
 * /param[in]		source			void pointer to the mounted
 * 						header file.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
init_hdr_parameters(void **parameters, void *hdr_source);

/** Function responsible for releasing header file previously
 *  allocated parameters.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						hdr_parameters struct and
 * 						used to release any allocated
 * 						member.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
release_hdr_parameters(void *parameters);

/** Function responsible for initializing raw parsing function pointers.
 *
 * /param[in]		parsing_fucntions	void pointer to be allocated
 * 						and initialized with all
 * 						raw parsing function
 * 						pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
init_raw_parse_functions(parse_functions_t **parsing_functions);

/** Function responsible for initializing header file parsing function pointers.
 *
 * /param[in]		parsing_fucntions	void pointer to be allocated
 * 						and initialized with all
 * 						hdr parsing function
 * 						pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
init_hdr_parse_functions(parse_functions_t **parsing_functions);

/** Function responsible for parsing a trace
 *  from a mounted raw file.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						raw_parameters struct.
 *
 * /return	trace_t struct.
 *
 */
trace_t*
get_raw_trace(void *parameters);

/** Function responsible for parsing a trace
 *  from a mounted header file.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						hdr_parameters struct.
 *
 * /return	trace_t struct.
 *
 */
trace_t*
get_hdr_trace(void *parameters);


/** Function responsible for
 *  skipping redundant bytes of fortran records.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						raw_parameters struct.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
read_fortran_record(void *parameters);

/** Function responsible for
 *  setting ns value for parsing raw files.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						raw_parameters struct.
 * /param[in]		ns			ns value to be set
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
set_ns(void **parameters, int ns);


#endif /* DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_RAW_H_ */
