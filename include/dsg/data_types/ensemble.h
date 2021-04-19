/*
 * ensemble.h
 *
 *  Created on: Jan 25, 2021
 *      Author: omar
 */

#ifndef INCLUDE_DSG_DATA_STRUCTURES_ENSEMBLE_H_
#define INCLUDE_DSG_DATA_STRUCTURES_ENSEMBLE_H_

#include "data_structures/doubly_linked_list.h"
#include "trace.h"
#include "utilities/error_handler.h"

/* struct representing an ensemble (linked list of traces) */
typedef struct ensemble {
	/* linked list of traces sharing a common property*/
	doubly_linked_list_t *traces;
	/* node member to be used in linked list of ensembles*/
	node_t n;
} ensemble_t;

/* struct representing an ensemble_list (linked list of ensembles) */
typedef struct ensemble_list {
	/* linked list of ensembles, each representing a unique value*/
	doubly_linked_list_t *ensembles;
} ensemble_list;

/** Function responsible for initializing ensemble variable
 *
 * \return      pointer to allocated ensemble variable
 */
ensemble_t*
init_ensemble();

/** Function responsible for initializing ensemble_list variable
 *
 *
 * \return      pointer to allocated ensemble_list variable
 */
ensemble_list*
init_ensemble_list();

/** Function responsible for adding trace to an ensemble list
 *
 * \param[in]	tr			initialized trace pointer
 * \param[in]   e			initialized ensemble_list
 * \param[in]   add_ensemble_flag	flag to determine whether a new
 * 					ensemble needs to be added
 *
 * \return      0 for successful addition
 */
int
ensemble_list_add_trace(trace_t *tr, ensemble_list *e, int add_ensemble_flag);

/** Function responsible for destroying ensemble variable
 *
 * \param[in]	e			pointer to ensemble variable
 *
 * \return      0 for successful destruction
 */
int
destroy_ensemble(void *e);

/** Function responsible for destroying ensemble_list variable
 *
 * \param[in]	e			pointer to ensemble_list variable
 *
 * \return      0 for successful destruction
 */
int
destroy_ensemble_list(ensemble_list *e);

/** Function responsible for filtering the linked list of traces under an
 *  ensemble, based on a specific set of window parameters. For every trace
 *  that doesn't satisfy all filters, the trace is removed from the linked list
 *
 *\param[in]	n			pointer to node signifying the current
 *					ensemble in the ensemble list
 *\param[in]	wp			pointer to window parameters
 *
 * \return      1 if traces list is completely deleted, 0 otherwise
 */
int
trace_window(node_t *n, void *wp);


/** Function responsible for deciding if a specific header in a trace
 *  object lies in the appropriate range specified in the user's windowing
 *  parameters
 *
 *\param[in]	val	generic value to be tested
 *\param[in]	wp	pointer to window parameters
 *\param[in]	index	index of key in window parameters
 *
 * \return      1 if trace is in range, 0 otherwise
 */
int
trace_in_range(generic_value val, window_params_t* wp,int index);

int
ensemble_to_array(ensemble_list* e,trace_t** traces, int index);

#endif /* INCLUDE_DSG_DATA_STRUCTURES_ENSEMBLE_H_ */
