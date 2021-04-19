/*
 * general_operations.c
 *
 *  Created on: Apr 7, 2021
 *      Author: omar
 */

#include "operations/general_operations.h"

void
calculate_chunksize(int num_of_processes, int total, int* offset, int* chunksize)
{
	*(chunksize) = total / num_of_processes;
	int leftover = total % num_of_processes;
	*(offset) = leftover + *(chunksize);
}
