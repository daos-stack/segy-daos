/*
 * string_helpers.c
 *
 *  Created on: Jan 21, 2021
 *      Author: mirnamoawad
 */

#include "utilities/string_helpers.h"

int
tokenize_str(void **array, char *sep, char *string, data_type_t type,
	     int *num_of_tokens)
{
	double 	        *temp_d;
	char 		temp_str[MAX_STR_LEN];
	char 		temp[MAX_STR_LEN];
	char	      	**temp_c;
	char 		*token;
	char 	      	*ptr;
	long 	       	*temp_l;
	int 		i;

	strcpy(temp_str, string);

	/** sometimes number of tokens is already calculated. */
	if (*num_of_tokens == 0) {
		strcpy(temp, string);
		token = strtok(temp, sep);
		while( token != NULL ) {
			(*num_of_tokens)++;
			token = strtok(NULL, sep);
		}
	}

	token = strtok(temp_str, sep);
	i=0;

	while (token != NULL) {
		switch (type) {
		case STRING:
			if(i == 0) {
				*array = malloc((*num_of_tokens) *
					        sizeof(char*));
			}
			temp_c = ((char**)(*array));
			temp_c[i] = malloc((strlen(token) + 1) * sizeof(char));
			strcpy(temp_c[i], token);
			break;
		case LONG:
			if(i == 0) {
				*array = malloc((*num_of_tokens) *
						sizeof(long));
			}
			temp_l = ((long*)(*array));
			temp_l[i] = atol(token);
			break;
		case DOUBLE:
			if(i == 0) {
				*array = malloc((*num_of_tokens) *
					        sizeof(double));
			}
			temp_d = ((double*)(*array));
			temp_d[i] = strtod(token, &ptr);
			break;
		default:
			return 0;
		}
		i++;
		token = strtok(NULL, sep);
	}
	return 0;
}

int
release_tokenized_array(void *array, data_type_t type, int size)
{
	int		i;
	if (type == STRING) {
		for(i=0; i<size; i++) {
			free(((char **)array)[i]);
		}
	}
	free(array);
	return 0;
}

int
check_if_key_exists(char **array, char *key, int size, int *index)
{
	int		i;
	int		exists = 0;
	if(array == NULL) {
		return exists;
	}
	for(i = 0; i < size; i++) {
		if(strcmp(array[i], key) == 0) {
			exists = 1;
			*index = i;
			break;
		}
	}
	return exists;
}

int
concatenate_complex_strings(char **array, int *directions,
			    char *sep, int size, char *str)
{
	int		i;

	for(i =0  ; i < size; i++) {
		if(directions[i] == 1) {
			strcat(str, "+");
		} else {
			strcat(str,"-");
		}
		strcat(str,array[i]);
		strcat(str, sep);
	}
	return 0;
}


