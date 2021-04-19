/*
 * cmd.c
 *
 *  Created on: Jan 21, 2021
 *      Author: mirnamoawad
 */

#include "utilities/cmd.h"

int
pcreate(int fds[2], const char *command, char *const argv[])
{

	/** Spawn a process running the command, returning it's pid.
	 *  The fds array passed will be filled with two descriptors:
	 *  fds[0] will read from the child process, and fds[1] will
	 *  write to it. Similarly, the child process will receive a
	 *  reading/writing fd set (in that same order) as arguments.
	 */
	int	 	pid;
	int 		pipes[4];

	/** Warning: I'm not handling possible errors in pipe/fork */

	pipe(&pipes[0]); /** Parent read/child write pipe */
	pipe(&pipes[2]); /** Child read/parent write pipe */

	if ((pid = fork()) > 0) {
		/** Parent process */
		fds[0] = pipes[0];
		fds[1] = pipes[3];

		close(pipes[1]);
		close(pipes[2]);

		return pid;

	} else {
		close(pipes[0]);
		close(pipes[3]);
		dup2(pipes[2], STDIN_FILENO);
		dup2(pipes[1], STDOUT_FILENO);
		execvp(command, argv);
		return -1;
	}

	return -1;
}

int
execute_command(char *const argv[], char *write_buffer, int write_bytes,
		char *read_buffer, int read_bytes, int *total_bytes_read)
{


	/** Executes the command given in argv, which is a string array
	 * For example if command is ls -l -a
	 * argv should be {"ls, "-l", "-a", NULL}
	 * descriptors to use for read/write.
	 */
	int 		fd[2];
	/** Setup pipe, redirections and execute command. */
	int pid = pcreate(fd, argv[0], argv);
	/** Check for error. */
	if (pid == -1) {
		DSG_ERROR(-1,"Error in creating process");
		return -1;
	}
	/** If user wants to write to subprocess STDIN, we write here. */
	if (write_bytes > 0) {
		write(fd[1], write_buffer, write_bytes);
	}
	/** Read cycle : read as many bytes as possible or until
	 *  we reach the maximum requested by user.
	 */
	char 		*buffer;
	ssize_t 	bytesread;

	buffer = read_buffer;
	bytesread = 1;

	*total_bytes_read = 0;
	while ((bytesread = read(fd[0], buffer, read_bytes)) > 0) {
		buffer += bytesread;
		*total_bytes_read += bytesread;
		if (bytesread >= read_bytes) {
			break;
		}
	}
	/** Return number of bytes actually read from the STDOUT
	 *  of the subprocess.
	 */
	return 0;

}
