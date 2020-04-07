//
// Created by mirnamoawad on 3/24/20.
// Updated by amrnasr on 3/28/20.
//
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include "dfs_helper_api.h"

/**
 * Reads a byte array from a specified file in the posix system.
 *
 * \param[in]	file			String containing the path of the file we want to read from.
 * \param[in]	byte_array		Pointer to the byte array to be allocated and filled with the bytes read.
 * \param[in]	verbose_output	Integer to enable verbosity to print messages in case of sucess if given non-zero values.
 * \return		Will return the number of bytes read from the file.
 */
size_t read_posix(const char *file, char **byte_array, int verbose_output);


int main(int argc, char *argv[]) {
    daos_size_t size, read_size;
    int verbose = 1;
    int allow_container_creation = 1;
    int allow_directory_creation = 1;
    int validate = 1;
    char filename[2048];

    char str[38];
    char * str2;
    str2 = fgets(str, 38, stdin);
    for(int i=0;i<38;i++)
    {
        if( str[i] == '\n')
        {
            str[i] = '\0';
            break;
        }
    }
    if(str2 == str){
        printf("No error \n");
    }
    else{
        printf("ERROR \n");
    }
    printf(" UUID IS: %s \n", str);

    char ranklist[200];
    char *ranklist2;
    ranklist2 = fgets(ranklist, 200, stdin);
    for(int i=0;i<200;i++)
    {
        if( ranklist[i] == '\n')
        {
            ranklist[i] = '\0';
            break;
        }
    }
    if(ranklist2 == ranklist){
        printf("No error \n");
    }
    else{
        printf("ERROR \n");
    }
    printf("RANK LIST IS: %s \n", ranklist);

	char co_id[38];
	char * co_id2;
	printf("Please enter container id: \n");
	co_id2 = fgets(co_id, 38, stdin);
	for(int i=0;i<38;i++)
	{
		if( co_id[i] == '\n') {
		    co_id[i] = '\0';
		    break;
		}
	}
	if(co_id2 == co_id){
		printf("No error \n");
	}
	else{
		printf("ERROR \n");
	}
	printf(" UUID IS: %s \n", co_id);

	char dir_name[200];
    char *dir_name2;
    printf("Please enter directory name to check if it already exist: \n");
    dir_name2 = fgets(dir_name, 200, stdin);
    for(int i=0;i<200;i++)
    {
        if( dir_name[i] == '\n')
        {
            dir_name[i] = '\0';
            break;
        }
    }
    if(dir_name2 == dir_name){
        printf("No error \n");
    }else{
        printf("error \n");
    }

    
	const char *file_directory = (const char *)dir_name;
	// Initialize DAOS
    init_dfs_api(str, ranklist, co_id, allow_container_creation, verbose);
	// Check 
	if(dfs_file_exists(file_directory)) {
		printf("File '%s' already exists...\n", file_directory);
		exit(0);
	}
	long len;
    char *ret;    
    len = read_posix("vel_bp.segy", &ret, verbose);
	DAOS_FILE * daos_file = open_dfs_file(file_directory, S_IFREG | S_IWUSR | S_IRUSR,
     'w', allow_directory_creation);
    write_dfs_file(daos_file, ret, len);
	size = get_dfs_file_size(daos_file);
	if (verbose) {
		printf("File size returned from DAOS API : %ld \n", size);
	}
	if (validate) {
		char *read = malloc(len);
        read_size = read_dfs_file_with_offset(daos_file, read, len, 0);
		if (verbose) {
			printf("File size read back from DAOS is %ld \n", read_size);
		}
		for(int i=0 ; i<len; i++){
    		if(ret[i]!=read[i]){
		        printf("Validation fails : bytes mismatch \n");
		        free(read);
		        return 0;
	     	}
	 	}
	 	free(read);
	 	if (verbose) {
	 		printf("Valdiation on bytes arrays : passes...\n");
	 	}
 	}
    close_dfs_file(daos_file);
    fini_dfs_api();
    return 0;
}

size_t read_posix(const char *file, char **byte_array, int verbose_output) {
	FILE *fl = fopen(file, "r");
	if (fl == 0) {
		printf("Posix read not successfull for file '%s'...", file);
		exit(0);
	}
    fseek(fl, 0, SEEK_END);
    long len = ftell(fl);
    *byte_array = malloc(len);
    fseek(fl, 0, SEEK_SET);
    size_t file_size = fread(*byte_array, 1, len, fl);
    fclose(fl);
    if (verbose_output) {
    	printf("Read %zu bytes from posix file %s\n", file_size*sizeof(**byte_array), file);
    }
    return len;
}
