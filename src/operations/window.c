/*
 * window.c
 *
 *  Created on: Jan 30, 2021
 *      Author: omar
 */

#include "operations/window.h"
#include <stdio.h>

int
window(doubly_linked_list_t *ll, void *window_params,
       int(*func)(node_t*, void*),
       int(*destructor)(void*), size_t offset, int start_index)
{
	node_t 		*h;
	node_t 		*temp;
	int 		res;
	int 		i = 0;

	h = ll->head;

	if(start_index > ll->size){
		DSG_ERROR(-1,"Invalid starting index while windowing");
		return -1;
	}

	while (i < start_index) {
		h = h->next;
		i++;
	}

	while (h != NULL) {
		temp = h;
		res = (*func)(temp, window_params);
		h = h->next;
		i++;
		if (res == 1) {
			doubly_linked_list_delete_node(ll, temp, destructor,
						       offset);
		}
	}

	return 0;
}
