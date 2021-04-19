/*
 * linked_list_test.c
 *
 *  Created on: Jan 24, 2021
 *      Author: omar
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>

#include "data_structures/doubly_linked_list.h"

typedef struct big {
	char z;
	int x[10];
	node_t n;
	size_t offset;
} big;

int
comp(void *a, void *b)
{
	char* x = (char*)a;
	big*  y = (big*)b;

	return *x - y->z;
}

big*
init_big(char z)
{
	big *b = malloc(sizeof(big));
	b->z = z;
	doubly_linked_node_init(&(b->n));
	return b;
}

int
destroy(void *x)
{
	big* b = (big*)x;
	free(b);
	return 0;
}

static void
test_createList(void **state)
{
	doubly_linked_list_t *list = doubly_linked_list_init();
	assert_non_null(list);
	assert_null(list->head);
	assert_null(list->tail);
	assert_int_equal(list->size, 0);
	free(list);
}

static void
test_createNode(void **state)
{
	node_t n;
	doubly_linked_node_init(&n);
	assert_null(n.next);
	assert_null(n.prev);
}

static void
test_offset(void **state)
{
	big *b1 = init_big('a');
	size_t offset = offsetof(big, n);
	node_t *n = (node_t*) doubly_linked_list_get_node(b1, offset);
	big *test = (big*) doubly_linked_list_get_object(n, offset);
	assert_ptr_equal(b1, test);
	free(b1);
}

static void
test_add_node(void **state)
{
	doubly_linked_list_t *ll = doubly_linked_list_init();
	big *b1 = init_big('a');
	doubly_linked_list_add_node(ll, &(b1->n));
	assert_int_equal(ll->size, 1);
	big *b2 = init_big('b');
	doubly_linked_list_add_node(ll, &(b2->n));
	assert_int_equal(ll->size, 2);
	assert_ptr_equal(ll->head->next, ll->tail);
	assert_ptr_equal(ll->tail->prev, ll->head);

	free(ll);
	free(b1);
	free(b2);

}

static void
test_destroy_linked_list(void **state)
{
	doubly_linked_list_t *ll = doubly_linked_list_init();
	big *b1 = init_big('a');
	doubly_linked_list_add_node(ll, &(b1->n));
	size_t offset = offsetof(big, n);
	big *b2 = init_big('b');
	doubly_linked_list_add_node(ll, &(b2->n));
	int rc = doubly_linked_list_destroy(ll, NULL, offset);
	assert_int_equal(rc, 0);
}

static void
test_search(void **state)
{
	doubly_linked_list_t *ll = doubly_linked_list_init();
	big *b1 = init_big('a');
	doubly_linked_list_add_node(ll, &(b1->n));
	size_t offset = offsetof(big, n);
	big *b2 = init_big('b');
	doubly_linked_list_add_node(ll, &(b2->n));
	char key = 'a';
	big *test1 = (big*) doubly_linked_list_search(ll, &key, &comp, offset);
	assert_ptr_equal(b1, test1);
	key = 'b';
	big *test2 = (big*) doubly_linked_list_search(ll, &key, &comp, offset);
	assert_ptr_equal(b2, test2);
	key = 'c';
	big *test3 = (big*) doubly_linked_list_search(ll, &key, &comp, offset);
	assert_null(test3);
}

static void
test_delete(void **state)
{
	doubly_linked_list_t *ll = doubly_linked_list_init();
	big *b1 = init_big('a');
	doubly_linked_list_add_node(ll, &(b1->n));
	size_t offset = offsetof(big, n);
	big *b2 = init_big('b');
	doubly_linked_list_add_node(ll, &(b2->n));
	assert_int_equal(ll->size, 2);
	char key = 'a';
	int rc = doubly_linked_list_delete(ll, &key, &comp, NULL, offset);
	assert_int_equal(rc, 0);
	assert_int_equal(ll->size, 1);

	key = 'c';
	rc = doubly_linked_list_delete(ll, &key, &comp, NULL, offset);
	assert_int_equal(rc, -1);
	assert_int_equal(ll->size, 1);

	key = 'b';
	rc = doubly_linked_list_delete(ll, &key, &comp, &destroy, offset);
	assert_int_equal(rc, 0);
	assert_int_equal(ll->size, 0);

	key = 'a';
	rc = doubly_linked_list_delete(ll, &key, &comp, &destroy, offset);
	assert_int_equal(rc, -1);
	assert_int_equal(ll->size, 0);

}

static void
test_merge_list(void **state)
{
	doubly_linked_list_t *first = doubly_linked_list_init();
	big *b1 = init_big('a');
	doubly_linked_list_add_node(first, &(b1->n));
	size_t offset = offsetof(big, n);
	big *b2 = init_big('b');
	doubly_linked_list_add_node(first, &(b2->n));

	doubly_linked_list_t *second = doubly_linked_list_init();
	int rc = doubly_linked_list_merge_lists(first, second);
	assert_int_equal(rc, -1);
	big *b3 = init_big('c');
	doubly_linked_list_add_node(second, &(b3->n));
	big *b4 = init_big('d');
	doubly_linked_list_add_node(second, &(b4->n));

	rc = doubly_linked_list_merge_lists(first, second);
	assert_int_equal(rc, 0);
	assert_int_equal(first->size, 4);
	assert_ptr_equal(first->tail, second->tail);
	big *temp = doubly_linked_list_get_object(first->tail, offset);
	assert_int_equal(temp->z, 'd');

}

int
main(int argc, char *argv[])
{
	const struct CMUnitTest tests[] = { cmocka_unit_test(test_createList),
			cmocka_unit_test(test_createNode), cmocka_unit_test(
					test_offset), cmocka_unit_test(
					test_add_node), cmocka_unit_test(
					test_destroy_linked_list),
			cmocka_unit_test(test_search), cmocka_unit_test(
					test_delete), cmocka_unit_test(
					test_merge_list) };
	return cmocka_run_group_tests(tests, NULL, NULL);
}
