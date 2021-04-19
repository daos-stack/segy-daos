/*
 * daos_seis_file_mount.c
 *
 *  Created on: Feb 28, 2021
 *      Author: mirnamoawad
 */

#include "api/seismic_graph_api.h"

int main(int argc, char *argv[]) {
	/* Declare variables that will be parsed from commandline */
	/**********************************************************/
	/* Required */
	char	*pool_id;
	/** string of the container uuid to connect to */
	char 	*container_id;
	/* string of the path of the file that will be read */
	char 	*in_file;
	/* string of the path of the file that will be written */
	char 	*out_file;
	/* Optional */
	/* Flag to allow verbose output */
	int 	verbose = 1 ;
	/* Flag to allow container creation if not found */
	int 	allow_container_creation = 1;
	/* Flag for validation after write */
	int	validate = 0;
	/* Flag to indicate a transfer from DAOS DFS to Posix, otherwise opposite */
	int 	daostoposix = 0;


    	/* Variables used in program */
    	daos_size_t size;

	if(argc != 6) {
		warn("you are passing only %d arguments. "
		      "5 arguments are required: pool_id, container_id,"
		      "segy_file_path, output_file_path, direction of transfer"
		      " \n", argc-1);
		exit(0);
	}

	pool_id = malloc(37 *sizeof(char));

	container_id = malloc(37 *sizeof(char));

    	strcpy(pool_id, argv[1]);

	strcpy(container_id, argv[2]);

	size_t segy_file_path_length = strlen(argv[3]);
	in_file = malloc(segy_file_path_length +1 *sizeof(char));
	strcpy(in_file, argv[3]);

	size_t seis_root_path_length = strlen(argv[4]);
	out_file = malloc(seis_root_path_length +1 *sizeof(char));
	strcpy(out_file, argv[4]);

	daostoposix = atoi(argv[5]);
	// Initialize DAOS
	init_dfs_api(pool_id,container_id, allow_container_creation, verbose);

	// Check DAOS TO POSIX transfer or posix to daos
	if (daostoposix) {
	// Check if the DFS file exists to be read.
	if(!dfs_file_exists(in_file)) {
	    printf("File '%s' doesn't exist...\n", in_file);
	    exit(0);
	}
	// Open, get file size and read as a byte array.
	DAOS_FILE * daos_file =
		open_dfs_file(in_file, S_IFREG | S_IWUSR | S_IRUSR,'r', 0);
	size = get_dfs_file_size(daos_file);
	char *read = malloc(size);
	size = read_dfs_file(daos_file, read, size);
	// Write the byte array read to the posix file.
	size = write_posix(out_file, read, size);
	if (verbose) {
	    printf("File size returned from Posix write : %ld \n", size);
	}
	// Validation of transfer.
	if (validate) {
	    // Read written posix file back.
	    char *ret;
	    long len = read_posix(out_file, &ret);
	    if (verbose) {
		printf("File size read back from Posix is %ld \n", len);
	    }
	    // Compare byte arrays.
	    long i;
	    for(i=0 ; i<len; i++){
		if(ret[i]!=read[i]){
		    printf("Validation fails : bytes mismatch \n");
                    free(read);
                    return -1;
                }
            }
            free(ret);
            if (verbose) {
                printf("Valdiation on bytes arrays : passes...\n");
            }
        }
        // Free resources.
        free(read);
        close_dfs_file(daos_file);
    } else {
        // Check if the DFS file exists to be written, if it exists cancel run.
        if(dfs_file_exists(out_file)) {
            printf("File '%s' already exists...\n", out_file);
            exit(-1);
        }
        // Read posix file.
        long len;
        char *ret;
        len = read_posix(in_file, &ret);
        /** Open DFS file to be written, this will create it.
         * And then write the byte array to it.
         */
        DAOS_FILE * daos_file =
        	open_dfs_file(out_file, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
        write_dfs_file(daos_file, ret, len);
        // Check written size.
        size = get_dfs_file_size(daos_file);
        if (verbose) {
            printf("File size returned from DAOS API : %ld \n", size);
        }
        // Validation of transfer.
        if (validate) {
            // Read written DFS file back.
            daos_size_t read_size;
            char *read = malloc(len);
            read_size = read_dfs_file_with_offset(daos_file, read, len, 0);
            if (verbose) {
                printf("File size read back from DAOS is %ld \n", read_size);
            }
            // Compare byte arrays.
            long i;
            for(i=0 ; i<len; i++){
                if(ret[i]!=read[i]){
                    printf("Validation fails : bytes mismatch \n");
                    free(read);
                    return -1;
                }
            }
            free(read);
            if (verbose) {
                printf("Validation on bytes arrays : passes...\n");
            }
        }
        free(ret);
        close_dfs_file(daos_file);
    }
    fini_dfs_api();
    return 0;
}
