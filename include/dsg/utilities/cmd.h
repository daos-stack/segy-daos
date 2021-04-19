/*
 * cmd.h
 *
 *  Created on: Jan 21, 2021
 *      Author: mirnamoawad
 */

#ifndef INCLUDE_DSG_UTILITIES_CMD_H_
#define INCLUDE_DSG_UTILITIES_CMD_H_

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "error_handler.h"


/** Function responsible for creating two pipes.
 *  It is called to enable reading and writing directly through the pipe
 *  No need to go for Posix system.
 *
 * \param[in]	fds		array of 2 file descriptors. fd[0] will read
 * 				from child process and the other
 * 				will write to it.
 * \param[in]	command		string containing the command to be executed.
 * \param[in]	argv		array of arguments to be passed
 * 				while executing the command.
 *
 * \return	id of the process running the command.
 *
 */
int
pcreate(int fds[2], const char *command, char *const argv[]);

/** Function responsible for executing command passed in argv
 *
 * \param[in]	argv			array of strings containing command
 * 					and the arguments.
 * \param[in]	write_buffer		byte array to be written.
 * \param[in]	write_bytes		number of bytes to be written.
 * \param[in]	read_buffer		byte array to be read into.
 * \param[in]	read_bytes		number of bytes to be read
 * \param[in]	total_bytes_read	actual number of bytes read.
 *
 * \return 	An error flag in case of error, 0 if successful.
 *
 */
int
execute_command(char *const argv[], char *write_buffer, int write_bytes,
		char *read_buffer, int read_bytes, int *total_bytes_read);

#endif /* INCLUDE_DSG_UTILITIES_CMD_H_ */
