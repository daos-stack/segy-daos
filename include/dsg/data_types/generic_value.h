/*
 * generic_value.h
 *
 *  Created on: Jan 22, 2021
 *      Author: mirnamoawad
 */

#ifndef INCLUDE_DSG_DATA_TYPES_GENERIC_VALUE_H_
#define INCLUDE_DSG_DATA_TYPES_GENERIC_VALUE_H_

#include <string.h>
#include <stdio.h>
#include <math.h>
#include <errno.h>
#include <limits.h>
#include <float.h>
#include <stdlib.h>
#include "utilities/error_handler.h"

/* storage for arbitrary type */
typedef union {
	char s[8];
	short h;
	unsigned short u;
	long l;
	unsigned long v;
	int i;
	unsigned int p;
	float f;
	double d;
	unsigned int U:16;
	unsigned int P:32;
} generic_value;

typedef struct {
	char* type;
	int direction;
}generic_value_sort_params_t;

typedef struct {
	char* key;
	generic_value* values;
	int num_of_values;
}key_value_pair_t;

generic_value_sort_params_t*
init_generic_value_sort_params(char* type, int direction);

int
generic_value_compare(void *val1, void *val2,
		      void* sp);

/* Function responsible for converting val to long.
 *
 * \param[in]	type		datatyoe of val.
 * \param[in]	val		Val that will be casted to long.
 *
 * \return	long integer.
 *
 */
long
vtol(char *type, generic_value val);

/* Function responsible for converting val to double.
 *
 * \param[in]	type		datatyoe of val.
 * \param[in]	val		Val that will be casted to double.
 *
 * \return	double.
 *
 */
double
vtod(char *type, generic_value val);

/* Function responsible for comparing two Vals given their datatype.
 *
 * \param[in]	type		datatyoe of val.
 * \param[in]	val1		first val.
 * \param[in]	val2		second val.
 *
 * \return	-1 if val1.(type) < val2.(type)
 *		1  if val1.(type) > val2.(type)
 * 		0  otherwise.
 *
 */
int
valcmp(char *type, generic_value val1, generic_value val2);

/* Function responsible for printing val to stdout given its datatype.
 *
 * \param[in]	type		datatyoe of val.
 * \param[in]	val		Val to be printed.
 *
 * \return
 *
 */
int
printfval(char *type, generic_value val);

/* Function responsible for printing val to char array given its datatype.
 *
 * \param[in]	temp		char array to which val will be stored.
 * \param[in]	val		Val to be printed..
 * \param[in]	type		datatyoe of val.
 *
 * \return
 *
 */
int
val_sprintf(char *temp, generic_value unique_value, char *type);

/* Function responsible for setting val given its data type
 * and some other parameters to calculate the new value using
 * the following formula:
 * a + b * mod(i, j) + c * ((int) (i/j))
 *
 *
 * \param[in]	type	       datatyoe of val.
 * \param[in]	val	       Val to be set.
 * \param[in] 	a	       double value
 * \param[in] 	b	       double value.
 * \param[in] 	c	       double value.
 * \param[in]	i	       double value
 * \param[in] 	j	       double value.
 *
 * \return	0 if successfu,
 * 		error code otherwise
 *
 */
int
setval(char *type, generic_value *valp, double a, double b,
       double c, double i, double j);

/* Function responsible for changing specific val given its data type,
 * some parameters, & two other values to change the val1 using
 * the following formula:
 * (a+b*pow(dval2,e)+c*pow(dval3,f))/d
 *
 * \param[in]	type1	       datatyoe of first val.
 * \param[in]	val1	       Val to be set.
 * \param[in]	type2	       datatyoe of second val.
 * \param[in]	val2	       Val that will be used to set first val.
 * \param[in]	type3	       datatyoe of third val.
 * \param[in]	val3	       Val that will be used to set first val.
 * \param[in] 	a	       double value
 * \param[in] 	b	       double value.
 * \param[in] 	c	       double value.
 * \param[in] 	d	       double value.
 * \param[in]	e	       double value
 * \param[in] 	f	       double value.
 *
 * \return	0 if successful,
 * 		error code otherwise
 *
 */
int
changeval(char *type1, generic_value *valp1, char *type2,
	  generic_value *valp2, char *type3, generic_value *valp3,
	  double a, double b, double c, double d, double e, double f);

/* Function responsible for converting keyval to Val given its datatype.
 *
 * \param[in]	type		datatype of val to be set.
 * \param[in]	keyval		value in ascii to be set in val.
 * \param[in]	valp		val to be converted.
 *
 * \return	0 if successful,
 * 		error code otherwise
 *
 */
int
atoval(char *type, char *keyval, generic_value *valp);

/******************************************/
/** Generic value module helper functions */
/******************************************/

/* Function responsible for calculating mod/remainder of two double numbers.
 *
 * \param[in]	x		first double value.
 * \param[in]	y		second double value.
 *
 * \return	result of x mod y.
 *
 */
double
mod(double x, double y);

/* Function responsible for converting string s to short integer.
 *
 * \param[in]	s		input string to be converted to short integer.
 *
 * \return	short integer.
 *
 */
short
eatoh(char *s);

/* Function responsible for converting string s to unsigned short integer.
 *
 * \param[in]	s		input string to be converted to
 * 				unsigned short integer.
 *
 * \return	unsigned short integer.
 *
 */
unsigned short
eatou(char *s);

/* Function responsible for converting string s to integer.
 *
 * \param[in]	s		input string to be converted to integer.
 *
 * \return	integer.
 *
 */
int
eatoi(char *s);

/* Function responsible for converting string s to unsigned integer.
 *
 * \param[in]	s		input string to be converted to unsigned integer
 *
 * \return	unsigned integer.
 *
 */
unsigned int
eatop(char *s);

/* Function responsible for converting string s to long integer.
 *
 * \param[in]	s		input string to be converted to long integer.
 *
 * \return	long integer.
 *
 */
long
eatol(char *s);

/* Function responsible for converting string s to unsigned long integer.
 *
 * \param[in]	s		input string to be converted to unsigned
 * 				long integer.
 *
 * \return	unsigned long integer.
 *
 */
unsigned long
eatov(char *s);

/* Function responsible for converting string s to float
 *
 * \param[in]	s		input string to be converted to float.
 *
 * \return	float.
 *
 */
float
eatof(char *s);

/* Function responsible for converting string s to double.
 *
 * \param[in]	s		input string to be converted to double.
 *
 * \return	double.
 *
 */
double
eatod(char *s);


/* Function responsible * \return	0 if successful,
 * 		error code otherwise for computing size of each type.
 *
 * \param[in]	type		input type.
 *
 * \return	size of the associated type.
 *
 */
size_t
key_get_size(char *type);


/* Function responsible for initializing a generic value with a
 * value of a specified type.
 *
 * \param[in]	type		input type.
 * \param[in]	buff		void pointer carrying value to be set
 * \param[out]	val		generic value to be initialized
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
generic_value_init(char* type, void* buff, generic_value* val);


/* Function responsible for initializing key value pair object.
 *
 * \param[out]	kv		pointer to key value pair object
 * \param[in]	keys		array of strings to be set in the key value
 * 				pair object
 * \param[in]	num_of_traces	size of values array
 * \param[in]	num_of_keys	size of keys array
 *
 * \return	0 if successfull,
 * 		error code otherwise
 *
 */
int
key_value_pair_init(key_value_pair_t** kv, char** keys,
		    int num_of_traces, int num_of_keys);

#endif /* INCLUDE_DSG_DATA_TYPES_GENERIC_VALUE_H_ */
