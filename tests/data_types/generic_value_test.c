/*
 * generic_value_test.c
 *
 *  Created on: Jan 22, 2021
 *      Author: mirnamoawad
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include "data_types/generic_value.h"

void
test_generic_value_vtol(void **state)
{
	generic_value val;
	long num;
	val.l = 10000;
	num = vtol("l", val);
	assert_true(num == val.l);
}

void
test_generic_value_vtod(void **state)
{
	generic_value val;
	double num;
	val.d = 100.50;
	num = vtod("d", val);
	assert_true(num == val.d);
}

void
test_generic_value_valcmp(void **state)
{
	generic_value val1;
	generic_value val2;
	val1.i = 10;
	val2.i = 15;
	int cmp;
	cmp = valcmp("i", val1, val2);
	assert_int_equal(cmp,-1);
}

void
test_generic_value_val_sprintf(void **state)
{
	generic_value val1;
	val1.i = 10;
	char *temp = malloc(10 * sizeof(char));
	val_sprintf(temp, val1, "i");
	assert_string_equal(temp,"10");
}

void
test_generic_value_setval(void **state)
{
	generic_value val1;

	setval("i", &val1, 5, 5.5, 0, 10, 20);
	assert_int_equal(val1.i, 60);

}

void
test_generic_value_changeval(void **state)
{
	generic_value val1;
	val1.i = 50;
	generic_value val2;
	val2.i = 10;
	generic_value val3;
	val3.i = 25;
	changeval("i", &val1, "i", &val2, "i", &val3,
		  5.5, 10.5, 6.5, 10.5, 2.5, 0);
	assert_int_equal(val1.i, 317);

}

void
test_generic_value_atoval(void **state)
{
	generic_value val;
	char *s = malloc(100 * sizeof(char));
	strcpy(s, "1000");
	atoval("i", s, &val);
	assert_int_equal(val.i, 1000);
	free(s);
	strcpy(s, "55011.06");
	atoval("d", s, &val);
	assert_true(55011.06 == val.d);
}

int main(int argc, char* argv[])
{
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_generic_value_vtol),
    cmocka_unit_test(test_generic_value_vtod),
    cmocka_unit_test(test_generic_value_valcmp),
    cmocka_unit_test(test_generic_value_val_sprintf),
    cmocka_unit_test(test_generic_value_setval),
    cmocka_unit_test(test_generic_value_changeval),
    cmocka_unit_test(test_generic_value_atoval)
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

