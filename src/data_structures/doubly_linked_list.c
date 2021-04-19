/*
 * linked_list_test.c
 *
 *  Created on: Jan 24, 2021
 *      Author: omar
 */

#include "data_structures/doubly_linked_list.h"
#include "data_types/trace.h"
#include <stdio.h>
#include <stdint.h>

int
doubly_linked_node_init(node_t *n)
{
	n->next = NULL;
	n->prev = NULL;

	return 0;
}

doubly_linked_list_t*
doubly_linked_list_init()
{
	doubly_linked_list_t *ll = malloc(sizeof(doubly_linked_list_t));
	ll->head = NULL;
	ll->tail = NULL;
	ll->size = 0;
	return ll;
}

int
doubly_linked_list_add_node(doubly_linked_list_t *ll, node_t *n)
{
	if (n == NULL) {
		doubly_linked_node_init(n);
	}
	if (ll->head == NULL) {
		ll->head = n;
		ll->tail = n;
		n->next = NULL;
		n->prev = NULL;
		ll->size++;
		return 0;
	}

	ll->tail->next = n;
	n->prev = ll->tail;
	n->next = NULL;
	ll->tail = n;
	ll->size++;

	return 0;

}

void*
doubly_linked_list_get_object(node_t *n, size_t offset)
{
	return (void*) ((char*) n - (char*) offset);
}

void*
doubly_linked_list_get_node(void *obj, size_t offset)
{
	return (void*) ((char*) obj + offset);
}

int
doubly_linked_list_destroy(doubly_linked_list_t *ll,
			   int (*destructor)(void*), size_t offset)
{
	node_t 		*h = ll->head;
	node_t 		*temp;
	int 		size = ll->size;
	if (ll->head == NULL) {
		return 0;
	}
	for (int i = 0; i < size; i++) {
		temp = h;
		h = h->next;
		doubly_linked_list_delete_node(ll, temp, destructor, offset);

	}
	free(ll);
	return 0;
}

void*
doubly_linked_list_search(doubly_linked_list_t *ll, void *key,
			  int (*func)(void*, void*), size_t offset)
{
	node_t *h = ll->head;
	if (h == NULL) {
		DSG_ERROR(-1,"Empty Linked List",empty_list);
	}
	for (int i = 0; i < ll->size; i++) {
		void *obj = doubly_linked_list_get_object(h, offset);
		if ((*func)(key, obj) == 0) {
			return obj;
		}
		h = h->next;
	}

empty_list:
	return NULL;

}

int
doubly_linked_list_delete(doubly_linked_list_t *ll, void *key,
			  int (*func)(void*, void*),
			  int (*destructor)(void*), size_t offset)
{
	void *obj = doubly_linked_list_search(ll, key, func, offset);
	if (obj == NULL) {
		DSG_ERROR(-1, "No such object in Linked List");
		return -1;
	}
	node_t *n = (node_t*) doubly_linked_list_get_node(obj, offset);

	return doubly_linked_list_delete_node(ll, n, destructor, offset);

}

int
doubly_linked_list_delete_node(doubly_linked_list_t *ll, node_t *n,
			       int (*destructor)(void*), size_t offset)
{

	void *obj = doubly_linked_list_get_object(n, offset);

	if (n == ll->head) {
		ll->head = n->next;
	}

	if (n == ll->tail) {
		ll->tail = n->prev;
	}

	if (n->next != NULL) {
		n->next->prev = n->prev;
	}
	if (n->prev != NULL) {
		n->prev->next = n->next;
	}

	int rc;
	if (destructor == NULL) {
		free(obj);
		rc = 0;
	} else {
		rc = (*destructor)(obj);
	}
	DSG_ERROR(rc, "Error in object destruction", destroy_error);
	ll->size--;
	if (ll->size == 0) {
		ll->head = NULL;
		ll->tail = NULL;
	}
destroy_error:
	return rc;
}

int
doubly_linked_list_merge_lists(doubly_linked_list_t *first,
			       doubly_linked_list_t *second)
{
	if (first->head == NULL || second->head == NULL) {
		DSG_ERROR(-1, "One of the lists is empty");
		return -1;
	}

	first->tail->next = second->head;
	second->head->prev = first->tail;
	first->tail = second->tail;
	first->size += second->size;

	return 0;
}
