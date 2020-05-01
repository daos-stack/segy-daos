//
// Created by mirnamoawad on 3/24/20.
// Updated by amrnasr on 3/28/20.
//
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include "par.h"
#include "dfs_helper_api.h"
/*********************** self documentation *****************************/
char *sdoc[] = {
" DFS_FILE_MOUNT - program to transfer files from posix to DAOS DFS or vice versa      ",
"                                   ",
"   dfs_file_mount pool=uuid container=uuid svc=r0:r1:r2   in=file_path out=file_path       ",
" Required parameters:                      ",
" pool=         pool uuid to connect                ",
" container=        container uuid to connect       ",
" svc=          service ranklist of pool seperated by : ",
" in=           the path to the file to be read for the transfer(If DFS file path must be absolute) ",
" out=          the path to the file to be written by the transfer(If DFS file path must be absolute) ",
"                               ",
" Optional parameter:                       ",
" verbose=0 silent operation(default)                    ",
"        =1 print messages to indicate progress          ",
" validate=0 doesn't validate the write operation (default) ",
"         =1 validates the write operation          ",
" daostoposix=0 transfers a posix file <in> to a dfs file <out> (default)",
"            =1 transfers a dfs file <in> to a posix file <out> ",
" contcreation=0 disables container creation if a container with the given uuid is non-existent ",
"             =1 enables container creation if a container with the given uuid is non-existent(default) ",
NULL};



int main(int argc, char *argv[]) {
    /* Declare variables that will be parsed from commandline */
    /**********************************************************/
    /* Required */
    char *pool_id;      /* string of the pool uuid to connect to */
    char *container_id; /* string of the container uuid to connect to */
    char *svc_list;     /* string of the service rank list to connect to */
    char *in_file;      /* string of the path of the file that will be read */
    char *out_file;     /* string of the path of the file that will be written */
    /* Optional */
    int verbose;                    /* Flag to allow verbose output */
    int allow_container_creation;   /* Flag to allow container creation if not found */
    int validate;                   /* Flag for validation after write */
    int daostoposix;                /* Flag to indicate a transfer from DAOS DFS to Posix, otherwise opposite */
    int i;

    /* Variables used in program */
    daos_size_t size;

    initargs(argc, argv);
    requestdoc(1);
    
    /* Set Required Parameters */
    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    MUSTGETPARSTRING("in",  &in_file);
    MUSTGETPARSTRING("out",  &out_file);
    /* Set Optional Parameters */
    if (!getparint("verbose", &verbose))    verbose = 0;
    if (!getparint("validate", &validate))    validate = 0;
    if (!getparint("daostoposix", &daostoposix))    daostoposix = 0;
    if (!getparint("contcreation", &allow_container_creation))    allow_container_creation = 1;

    
	// Initialize DAOS
    init_dfs_api(pool_id, svc_list, container_id, allow_container_creation, verbose);
    // Check DAOS TO POSIX transfer or posix to daos
    if (daostoposix) {
        // Check if the DFS file exists to be read.
        if(!dfs_file_exists(in_file)) {
            printf("File '%s' doesn't exist...\n", in_file);
            exit(0);
        }
        // Open, get file size and read as a byte array.
        DAOS_FILE * daos_file = open_dfs_file(in_file, S_IFREG | S_IWUSR | S_IRUSR,'r', 0);
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
        // Open DFS file to be written, this will create it. And then write the byte array to it.
        DAOS_FILE * daos_file = open_dfs_file(out_file, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
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
            for(i=0 ; i<len; i++){
                if(ret[i]!=read[i]){
                    printf("Validation fails : bytes mismatch \n");
                    free(read);
                    return -1;
                }
            }
            free(read);
            if (verbose) {
                printf("Valdiation on bytes arrays : passes...\n");
            }
        }
        free(ret);
        close_dfs_file(daos_file);
    }
    fini_dfs_api();
    return 0;
}


