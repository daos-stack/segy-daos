//
// Created by Omar Ahmed Hesham Aziz on 13/01/2021.
//

#include <stdlib.h>
#include "utilities/error_handler.h"

#ifndef LINKEDLIST_LINKED_LIST_H
#define LINKEDLIST_LINKED_LIST_H

/** Macro to compute offset in bytes of a member from its enclosing struct.
 *
 * \param[in]	st		type of structure
 * \param[in]	m		name of member variable
 *
 * \return	number of bytes (offset)
 */
#define offsetof(st, m) \
    ((size_t)((char *)&((st *)0)->m - (char *)0))

/* Structure defining a linked list node */
typedef struct node_t {
	/* Node pointer to next node */
	struct node_t *next;
	/* Node pointer to previous node */
	struct node_t *prev;
} node_t;

/* Structure defining a doubly circular linked list */
typedef struct doubly_linked_list_t {
	/* Node pointer to head of linked list */
	node_t *head;
	/* Node pointer to tail of linked list node_t*/
	node_t *tail;
	/* size of linked list */
	int size;
} doubly_linked_list_t;

/** Function responsible for initializing linked list nodes.
 *
 * \param[in]	n		pointer to uninitialized node
 *
 * \return	0 if successful
 */
int
doubly_linked_node_init(node_t *n);

/** Function responsible for initializing linked list.
 *
 * \return pointer to initialized linked list
 */
doubly_linked_list_t*
doubly_linked_list_init();

/** Function responsible for adding node to the end of a linked list and
 *  resolving appropriate linkages.
 *
 * \param[in]	ll		pointer to initialized linked list
 * \param[in]	n		pointer to initialized node
 *
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
doubly_linked_list_add_node(doubly_linked_list_t *ll, node_t *n);

/** Function responsible for getting the address of an object given the address
 *  of its node member and its offset
 *
 * \param[in]	n		pointer to initialized node
 * \param[in]	offset		number of bytes between the address of an
 * 				object and the address of its node member
 *
 * \return	pointer to generic object
 */
void*
doubly_linked_list_get_object(node_t *n, size_t offset);

/** Function responsible for getting the address of a node given the address
 *  of its enclosing object and its offset
 *
 * \param[in]	obj		pointer to initialized object
 * \param[in]	offset		number of bytes between the address of an
 * 				object and the address of its node member
 *
 * \return	pointer to node member
 */
void*
doubly_linked_list_get_node(void *obj, size_t offset);

/** Function responsible for freeing memory allocated to the linked list and
 *  its nodes
 *
 * \param[in]	ll		pointer to initialized linked list
 * \param[in]	offset		number of bytes between the address of an
 * 				object and the address of its node member
 *
 * \return	error code, success if 0, failure otherwise
 */
int
doubly_linked_list_destroy(doubly_linked_list_t *ll, int (*destructor)(void*),
			   size_t offset);

/** Function responsible for searching a linked list for a specific key.
 *  Returns the found object, and NULL if not found
 *
 * \param[in]	ll		pointer to initialized linked list
 * \param[in]   key		pointer to search key
 * \param[in]   func		pointer to comparison function used to search
 * 				the linked list
 * \param[in]	offset		number of bytes between the address of an
 * 				object and the address of its node member
 *
 *
 * \return	pointer to found object, NULL otherwise
 */
void*
doubly_linked_list_search(doubly_linked_list_t *ll, void *key,
			  int(*func)(void*, void*), size_t offset);

/** Function responsible for deleting a specific object in the linked list
 *  according to a search key, and resolving appropriate linked list linkages
 *
 * \param[in]	ll		pointer to initialized linked list
 * \param[in]   key		pointer to search key
 * \param[in]   func		pointer to comparison function used to search
 * 				the linked list
 * \param[in]	offset		number of bytes between the address of an object
 * 				and the address of its node member
 *
 * \return	error code, success if 0, failure otherwise
 */

int
doubly_linked_list_delete(doubly_linked_list_t *ll, void *key,
			  int(*func)(void*, void*), int(*destructor)(void*),
			  size_t offset);

/** Function responsible for deleting a specific node in the linked list,
 *  and resolving appropriate linked list linkages
 *
 * \param[in]	ll		pointer to initialized linked list
 * \param[in]   n		pointer to node to be deleted
 * \param[in]   destructor      pointer to destructor function;
 * \param[in]	offset		number of bytes between the address of an object
 * 				and the address of its node member
 *
 *
 * \return	error code, success ifdoubly_linked_list otherwise
 */
int
doubly_linked_list_delete_node(doubly_linked_list_t *ll, node_t *n,
			       int(*destructor)(void*), size_t offset);

/** Function responsible for linking two linked lists together. The
 *  first linked list pointer points to the full merged list
 *
 * \param[in]	first		pointer to first linked list, contains merged
 * 			        list at the end of the function
 * \param[in]	second		pointer to second linked list
 *
 * \return	error code, success if 0, failure otherwise
 */
int
doubly_linked_list_merge_lists(doubly_linked_list_t *first,
			       doubly_linked_list_t *second);

#endif //LINKEDLIST_LINKED_LIST_H

