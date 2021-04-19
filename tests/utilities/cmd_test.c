/*
 * cmd_test.c
 *
 *  Created on: Jan 21, 2021
 *      Author: mirnamoawad
 */


#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include "utilities/cmd.h"

void
test_cmd_preate_and_execute_command(void **state)
{
	/** The command and arguments.
	 *  Replace all occurrences of a by x.
	 */
	char *arr[] = {"sed", "-u", "s/a/x/g", NULL};
	/** The stdin given to the command */
	char write_buffer[5] = "abc\n";
	/** number of bytes to write from write buffer */
	int write_bytes = 4;
	/** Buffer to read data from command output to */
	char read_buffer[5];
	memset(read_buffer, 0, 5);
	/** Number of maximum bytes to read from the command,
	 *  command might return a smaller number which is the
	 *  the exact number written.
	 */
	int read_bytes = 4;
	int read_bytes_from_command;
	int rc;

	rc = execute_command(arr, write_buffer,
			     write_bytes, read_buffer,
			     read_bytes, &read_bytes_from_command);
	assert_int_equal(rc, 0);

	assert_int_not_equal(read_bytes_from_command, -1);
	assert_int_equal(read_bytes_from_command, 4);
	assert_true(strcmp(read_buffer, "xbc\n") == 0);
//	assert_string_equal(read_buffer,"xbc\n");

}
int main(int argc, char* argv[])
{
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_cmd_preate_and_execute_command)
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
