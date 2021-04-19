/*
 * string_helpers.h
 *
 *  Created on: Jan 21, 2021
 *      Author: mirnamoawad
 */

#ifndef INCLUDE_DSG_UTILITIES_STRING_HELPERS_H_
#define INCLUDE_DSG_UTILITIES_STRING_HELPERS_H_

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

typedef enum{
	STRING,
	LONG,
	DOUBLE
}data_type_t;

#define MAX_STR_LEN 4096

/** Function responsible for tokenizing string using a given separator.
 *  It currently supports tokenizing  string, long, double,...
 *
 *  \param[out] array           Array to be allocated to contain output.
 *  \param[in]	sep		string separator.
 *  \param[in]	string		input string to be tokenized.
 *  \param[in]	type 		enum holding datatype of data in string.
 *  \param[in]	num_of_tokens	integer to hold resulting number of tokens.
 *
 *  \return     0 on success
 *		error_code otherwise
 *
 */
int
tokenize_str(void **array, char *sep, char *string, data_type_t type,
	     int *num_of_tokens);

/** Function responsible for releasing an array of strings.
 *
 *  \param[in]	array		array of strings to be released.
 *  \param[in]	type 		enum holding datatype of data in string.
 *  \param[in]	size		size of array.
 *
 *  \return     0 on success
 *		error_code otherwise
 *
 */
int
release_tokenized_array(void *array, data_type_t type, int size);

/** Function responsible for checking if a string exists in array of strings.
 *
 *  \param[in]	array		array of strings to be checked.
 *  \param[in]	key		string to lookup.
 *  \param[in]	size		size of array.
 *  \param[in]	index		pointer to integer variable to be updated by
 *  				index of string if found.
 *
 *  \return     1 if string exists.
 *		0 otherwise.
 *
 */
int
check_if_key_exists(char **array, char *key, int num_of_elements, int *index);



/** Function responsible for forming a complex key string
 *  formed of a combination of the provided keys
 *
 *  \param[in]	array		array of keys to be concatenated.
 *  \param[in]	directions	direction of sorting for each key.
 *  \param[in]	sep		string separator.
 *  				(+ for ascending, - for descending)
 *  \param[in]	size		size of array and directions.
 *  \param[out]	str		resultant string
 *
 *  \return     0 if successful.
 *		error code otherwise.
 *
 */
int
concatenate_complex_strings(char **array, int *directions,
			    char *sep, int size, char *str);
#endif /* INCLUDE_DSG_UTILITIES_STRING_HELPERS_H_ */
