/*
 * list.h
 *
 *  Created on: Jan 31, 2021
 *      Author: mirnamoawad
 */

#ifndef DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_LIST_H_
#define DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_LIST_H_

#include "data_types/ensemble.h"
#include "data_types/trace.h"
#include "seismic_sources/source_interface.h"

typedef struct list_parameters {
	ensemble_list	*list;
	ensemble_t	*current_ensemble;
	trace_t		*current_trace;
}list_parameters_t;

/** Function responsible for initializing ensemble list parameters.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						list_parameters struct and
 * 						initialized to point to the
 * 						ensemble list to be parsed.
 * /param[in]		source			void pointer to the ensemble
 * 						list.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
init_list_parameters(void **parameters, void *source);

/** Function responsible for releasing ensemble list previously
 *  allocated parameters.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						list_parameters struct and
 * 						used to release any allocated
 * 						member.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
release_list_parameters(void *parameters);

/** Function responsible for initializing ensemble list parsing
 *  function pointers.
 *
 * /param[in]		parsing_fucntions	void pointer to be allocated
 * 						and initialized with all
 * 						ensemble list parsing function
 * 						pointers.
 *
 * /return	0 on success
 * 		error code otherwise.
 *
 */
int
init_list_parse_functions(parse_functions_t **parsing_functions);

/** Function responsible for parsing, processing if need a trace
 *  from ensemble list.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						list_parameters struct.
 *
 * /return	trace_t struct.
 *
 */
trace_t*
get_list_trace(void *parameters);

/** Function responsible for getting the next trace from current traces list or
 *  moving to the next ensemble and point to the head of its traces list.
 *
 * /param[in]		parameters		void pointer to be casted to
 * 						list_parameters struct and
 * 						initialized to point to the
 * 						ensemble list to be parsed.
 *
 * /return	0 if successful.
 */

int
get_next_trace(list_parameters_t *list_parameters);

#endif /* DAOS_DAOS_SEISMIC_GRAPH_INCLUDE_DSG_SEISMIC_SOURCES_LIST_H_ */
