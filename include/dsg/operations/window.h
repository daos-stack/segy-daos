/*
 * window.h
 *
 *  Created on: Jan 30, 2021
 *      Author: omar
 */

#ifndef INCLUDE_DSG_OPERATIONS_WINDOW_H_
#define INCLUDE_DSG_OPERATIONS_WINDOW_H_

#include "data_structures/doubly_linked_list.h"
#include "utilities/error_handler.h"

/** Function responsible for filtering a linked list based on a specific
 *  set of window parameters.
 *  For every element that doesn't satisfy all filters,
 *  the element is removed from the linked list
 *
 *\param[in]	ll		pointer to linked list
 *\param[in]	wp		pointer to window parameters
 *\param[in]	func		pointer to filtering function to be used to
 *\				detect if each element should be deleted or not
 *\				(return 1, or 0 respectively)
 *\param[in]	destructor	pointer to destructor function to be used for
 *\				each deleted element
 *\param[in]	offset		offset of each object's address from its node
 *\				member
 *\param[in]	start_index	index from which to start windowing/filtering
 *\				the linked list
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
window(doubly_linked_list_t *ll, void *window_params,
       int(*func)(node_t*, void*), int(*destructor)(void*),
       size_t offset,int start_index);

#endif /* INCLUDE_DSG_OPERATIONS_WINDOW_H_ */
