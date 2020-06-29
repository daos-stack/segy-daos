/*
 * su_helpers.h
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#ifndef LSU_SRC_CLIENT_SEIS_SU_HELPERS_H_
#define LSU_SRC_CLIENT_SEIS_SU_HELPERS_H_


#include <stdio.h>
#include <sys/types.h>
#include "su.h"
#include "segy.h"
#include "tapesegy.h"
#include "tapebhdr.h"
#include "bheader.h"
#include "header.h"


void ibm_to_float(int from[], int to[], int n, int endian, int verbose);

void tapebhed_to_bhed(const tapebhed *tapebhptr, bhed *bhptr);

void tapesegy_to_segy(const tapesegy *tapetrptr, segy *trptr);

void int_to_float(int from[], float to[], int n, int endian);

void short_to_float(short from[], float to[], int n, int endian);

void integer1_to_float(signed char from[], float to[], int n);

void ugethval(cwp_String type1, Value *valp1, char type2, int ubyte,
              char *ptr2, int endian, int conv, int verbose);

#endif /* LSU_SRC_CLIENT_SEIS_SU_HELPERS_H_ */
