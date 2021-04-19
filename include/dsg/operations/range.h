/*
 * range.h
 *
 *  Created on: Jan 30, 2021
 *      Author: omar
 */

#ifndef INCLUDE_DSG_OPERATIONS_RANGE_H_
#define INCLUDE_DSG_OPERATIONS_RANGE_H_

#include "data_types/ensemble.h"

typedef struct headers_ranges {
	trace_t *trmin;
	trace_t *trmax;
	trace_t *trfirst;
	trace_t *trlast;
	double north_shot[2];
	double south_shot[2];
	double east_shot[2];
	double west_shot[2];
	double north_rec[2];
	double south_rec[2];
	double east_rec[2];
	double west_rec[2];
	double north_cmp[2];
	double south_cmp[2];
	double east_cmp[2];
	double west_cmp[2];
	double dmin;
	double dmax;
	double davg;
	char **keys;
	int ntr; //trace index.
	int dim;
	int number_of_keys;
} headers_ranges_t;


/** Function responsible for initializing empty headers_ranges struct
 *
 * \param[in]	headers_ranges	double pointer to uninitialized hdrs ranges struct
 *
 * \return      0 for successful allocation
 */
int
headers_ranges_init(headers_ranges_t **headers_ranges);

/** Function to compute ranges of header values in an ensemble list
 *  based on the keys passed in the header_ranges object.
 *
 * \param[in]	e		pointer to ensemble_list
 * \param[in]	rng		header ranges structure containing all
 * 				required header range information
 * \return	0 if successful,
 * 		error code otherwise
 */
int
ensemble_range(ensemble_list *e, headers_ranges_t *rng);

/** Function to print the ranges of headers saved in a headers_ranges
 *  object
 *
 * \param[in]	rng		header ranges structure containing all
 * 				required header range information
 *
 * \return	0 if successful,
 * 		error code otherwise
 *
 */
int
print_headers_ranges(headers_ranges_t *rng);

/** Function to delete/free a headers_ranges
 *  object
 *
 * \param[in]	rng		header ranges structure to be
 * 				deleted
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
destroy_header_ranges(headers_ranges_t *rng);

#endif /* INCLUDE_DSG_OPERATIONS_RANGE_H_ */
