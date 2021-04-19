/*
 * general_operations.h
 *
 *  Created on: Apr 7, 2021
 *      Author: omar
 */

#ifndef DAOS_SEISMIC_GRAPH_INCLUDE_DSG_OPERATIONS_GENERAL_OPERATIONS_H_
#define DAOS_SEISMIC_GRAPH_INCLUDE_DSG_OPERATIONS_GENERAL_OPERATIONS_H_



void
calculate_chunksize(int num_of_processes, int total, int* offset, int* chunksize);

#endif /* DAOS_SEISMIC_GRAPH_INCLUDE_DSG_OPERATIONS_GENERAL_OPERATIONS_H_ */
