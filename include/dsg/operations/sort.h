/*
 * sort.h
 *
 *  Created on: Jan 26, 2021
 *      Author: omar
 */

#ifndef INCLUDE_DSG_OPERATIONS_SORT_H_
#define INCLUDE_DSG_OPERATIONS_SORT_H_

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* Function passed to sort function to compare two integers
 *
 * \param[in]	a		pointer to first integer
 * \param[in]	b		pointer to second integer
 * \param[in]	direction	pointer to direction variable
 * 				(1 for ascending, -1 for descending)
 *
 * \return	0 if equal, -1 if a is smaller, 1 if b is smaller
 *
 */
int
int_compare(void *a, void *b, void *direction);

/* Function passed to sort function to compare two longs
 *
 * \param[in]	a		pointer to first long
 * \param[in]	b		pointer to second long
 * \param[in]	direction	pointer to direction variable
 * 				(1 for ascending, -1 for descending)
 *
 * \return	0 if equal, -1 if a is smaller, 1 if b is smaller
 *
 */
int
long_compare(void *a, void *b, void *direction);

/* Function passed to sort a generic array according to specified parameters
 * using the merge sort algorithm
 *
 * \param[in]	array		array of elements to be sorted
 * \param[in]	num_of_elements	number of elements in array
 * \param[in]	element_size	size of element
 * \param[in]   sort_props	pointer to sort properties
 * \param[in]   compare		pointer to comparison function for a
 * 				specific datatype
 *
 * \return	0 if equal, -1 if a is smaller, 1 if b is smaller
 *
 */
int
sort(void *array, int num_of_elements, size_t element_size, void *sort_props,
     int (*compare)(void*, void*, void*));

#endif /* INCLUDE_DSG_OPERATIONS_SORT_H_ */
