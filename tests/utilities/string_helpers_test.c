/*
 * string_helpers_test.c
 *
 *  Created on: Jan 21, 2021
 *      Author: mirnamoawad
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <math.h>
#include "utilities/string_helpers.h"

#define TOLERANCE		1e-7

void
test_tokenize_string(void **state)
{
	char	*string = malloc(MAX_STR_LEN * sizeof(char));
	char	**tokens;
	int	num_of_tokens = 0;
	int	rc;
	int	i;
	strcpy(string, "STRING,TOKENIZE,TEST");
	rc = tokenize_str((void**)&tokens,",", string, STRING, &num_of_tokens);
	assert_int_equal(rc, 0);
	assert_int_equal(num_of_tokens, 3);
	assert_string_equal(tokens[0],"STRING");
	assert_string_equal(tokens[1],"TOKENIZE");
	assert_string_equal(tokens[2],"TEST");
	rc = release_tokenized_array((void*)tokens, STRING, num_of_tokens);
	assert_int_equal(rc, 0);

	free(string);
}

void
test_tokenize_double(void **state)
{
	char	*string = malloc(MAX_STR_LEN * sizeof(char));
	double	*tokens;
	int	num_of_tokens = 0;
	int	rc;
	int	i;
	strcpy(string, "1.005,500.5,120");
	rc = tokenize_str((void**)&tokens,",", string, DOUBLE, &num_of_tokens);
	assert_int_equal(rc, 0);
	assert_int_equal(num_of_tokens, 3);
	assert_true(fabs(tokens[0] - 1.005) < TOLERANCE);
	assert_true(fabs(tokens[1] - 500.5) < TOLERANCE);
	assert_true(fabs(tokens[2] - 120) < TOLERANCE);
	release_tokenized_array((void*)tokens, DOUBLE, num_of_tokens);
	free(string);
}

void
test_tokenize_long(void **state)
{
	char	*string = malloc(MAX_STR_LEN * sizeof(char));
	long	*tokens;
	int	num_of_tokens = 0;
	int	rc;
	int	i;
	strcpy(string, "1000,55000,121110");
	rc = tokenize_str((void**)&tokens,",", string, LONG, &num_of_tokens);
	assert_int_equal(rc, 0);
	assert_int_equal(num_of_tokens, 3);
	assert_true(tokens[0] == 1000L);
	assert_true(tokens[1] == 55000L);
	assert_true(tokens[2] == 121110L);
	release_tokenized_array((void*)tokens, LONG, num_of_tokens);
	free(string);
}

int main(int argc, char* argv[])
{
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_tokenize_string),
    cmocka_unit_test(test_tokenize_double),
    cmocka_unit_test(test_tokenize_long)
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
