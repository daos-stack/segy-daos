/*
 * calculate_header.h
 *
 *  Created on: Jan 27, 2021
 *      Author: omar
 */

#ifndef INCLUDE_DSG_OPERATIONS_CALCULATE_HEADER_H_
#define INCLUDE_DSG_OPERATIONS_CALCULATE_HEADER_H_

#include "data_types/ensemble.h"
#include "utilities/error_handler.h"



 /** Function responsible for calculating and setting trace header value
 *  It is called while setting traces headers values
 *
 * \param[in[	ensembles      pointer to ensemble_list.
 * \param[in]   num_of_keys    number of header keys to set their value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in] 	a	       array of doubles containing values on first
 * 			       trace
 * \param[in] 	b	       array of doubles containing increments values
 * \param[in] 	c	       array of doubles containing group increments
 * \param[in] 	d	       array of doubles containing trace number shifts
 * \param[in] 	e	       array of doubles containing number of elements
 * 			       in group
 */
int
set_headers(ensemble_list *ensembles, int num_keys, char **keys, double *a,
	    double *b, double *c, double *d, double *e, int start_index);

/** Function responsible for calculating and setting trace header value
 *  It is called while changing traces headers values
 *
 * \param[in[	ensembles      pointer to ensemble_list.
 * \param[in]   num_of_keys    number of header keys to set their value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in]	keys_2	       array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1
 * \param[in]	keys_3         array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1
 * \param[in] 	a	       array of doubles containing overall shift
 * \param[in] 	b	       array of doubles containing scale on first
 * 			       input key
 * \param[in] 	c	       array of doubles containing scale on second input
 * 			       key
 * \param[in] 	d	       array of doubles containing overall scale
 * \param[in]	e	       array of doubles containing exponent on first
 * 			       input key
 * \param[in] 	f	       array of doubles containing exponent on second
 * 			       input keys
 */
int
change_headers(ensemble_list *ensembles, int num_keys, char **keys1,
	       char **keys2, char **keys3, double *a, double *b, double *c,
	       double *d, double *e, double *f, int start_index);
#endif /* INCLUDE_DSG_OPERATIONS_CALCULATE_HEADER_H_ */
