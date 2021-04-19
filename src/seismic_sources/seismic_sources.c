/*
 * seismic_sources.c
 *
 *  Created on: Jan 30, 2021
 *      Author: mirnamoawad
 */

#include "seismic_sources/source_interface.h"
#include "seismic_sources/segy.h"
#include "seismic_sources/list.h"
#include "seismic_sources/raw.h"

int
init_parsing_parameters(source_type_t seismic_source_type,
			parse_functions_t **parse_functions,
			void *source)
{
	int	rc  = 0;
	switch(seismic_source_type){
	case(SEGY):
			rc = init_segy_parse_functions(parse_functions);
			break;
	case(LL):
			rc = init_list_parse_functions(parse_functions);
			break;
	case(BINARY):
			break;
	case(RAW):
			rc = init_raw_parse_functions(parse_functions);
			break;
	case(HEADER):
			rc = init_hdr_parse_functions(parse_functions);
			break;
	default:
			break;
	}
	DSG_ERROR(rc, "Initializing parsing functions failed");

	rc = (*parse_functions)->parse_parameters_init(
			(void **)&(*parse_functions)->parse_parameters,
			source);
	DSG_ERROR(rc, "Initializing parameters failed");

	return rc;
}

int
parse_text_and_binary_headers(parse_functions_t *parse_functions)
{
	int	rc = 0;

	rc = parse_functions->parse_file_headers(
					parse_functions->parse_parameters);
	DSG_ERROR(rc,"Parsing Headers failed");

	return rc;
}

char *
get_text_header(parse_functions_t *parse_functions)
{
	return parse_functions->get_text_header(
					parse_functions->parse_parameters);
}

char *
get_binary_header(parse_functions_t *parse_functions)
{
	return parse_functions->get_binary_header(
					parse_functions->parse_parameters);
}

int
get_number_of_extended_headers(parse_functions_t *parse_functions)
{
	return parse_functions->get_num_of_exth(
					parse_functions->parse_parameters);
}

char *
get_extended_header(parse_functions_t *parse_functions)
{
	return parse_functions->get_exth(
					parse_functions->parse_parameters);
}

trace_t*
get_trace(parse_functions_t *parse_functions)
{
	return parse_functions->get_trace(
					parse_functions->parse_parameters);
}

int
release_parsing_parameters(parse_functions_t *parse_functions)
{
	return parse_functions->release_parsing_parameters(
					parse_functions->parse_parameters);
}

int
read_junk(parse_functions_t *parse_functions)
{
	return parse_functions->read_junk(
					parse_functions->parse_parameters);
}

int
set_raw_ns(parse_functions_t **parse_functions, int ns)
{
	return (*parse_functions)->set_raw_ns(
				&((*parse_functions)->parse_parameters), ns);
}




