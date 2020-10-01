//
// Created by mirnamoawad on 4/5/20.
//

#include "dfs_helper_api.h"
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "daos.h"
#include "daos_fs.h"

#define GET_MACRO(_1,_2,_3,NAME,...) NAME
#define warn(...) GET_MACRO(__VA_ARGS__, warn3, warn2, warn1)(__VA_ARGS__)
#define warn1(x) fprintf(stderr,x)
#define warn2(x,y) fprintf(stderr,x,y)
#define warn3(x,y,z) fprintf(stderr,x,y,z)
static daos_handle_t poh;
static daos_handle_t coh;
static dfs_t *dfs;
static int verbose_output;

/**
 * Check the error code, it it is non-zero print error and terminate.
 * Otherwise, only print success message if verbosity enabled.
 *
 * \param[in]	err		Integer containing the error code.
 * \param[in]	verbose	Enable verbosity if given any non-zero integer.
 * \param[in]	message	String containing the operation that this error check belongs to.
 *
 */
void check_error_code(int err, const char *message) {
    if (err == 0) {
        if(verbose_output){
            warn("%s : Finished Successfully...\n", message);
        }
    } else {
        warn("%s : Error in call with value %d...\n", message, err);
        exit(0);
    }
}

void init_dfs_api(const char *pool_uid, const char *pool_svc_list, const char *container_uuid,
                         int allow_creation, int verbose) {
    // Pool UUID
    uuid_t po_uuid;
    // Container UUID
    uuid_t co_uuid;
    // Pool service replica ranks
    d_rank_list_t *svc = NULL;
    verbose_output = verbose;
    // Initialize DAOS API.
    check_error_code(daos_init(), "Initializing DAOS API Library");
    // Parse Pool UUID.
    int err = uuid_parse(pool_uid, po_uuid);
    if(po_uuid == NULL){
        warn("Error parsing pool uuid : %d\n", err);
        exit(0);
    }
    if (verbose_output) {
        warn("Pool UUID : %s \n", pool_uid);
    }
    // Parse Pool service replica ranks.
    svc = daos_rank_list_parse(pool_svc_list, ":");
    if(svc == NULL){
        warn("Error parsing rank list : %d\n ", err);
        exit(0);
    }
    check_error_code(daos_pool_connect(po_uuid, 0, svc, DAOS_PC_RW, &poh, NULL, NULL), "Connecting To Pool");
    // Parse Container UUID.
    err = uuid_parse(container_uuid, co_uuid);
    if(co_uuid == NULL){
        warn("Error parsing container uuid : %d \n", err);
        exit(0);
    }
    if (verbose_output) {
        warn("Container UUID : %s \n", container_uuid);
    }
    // Open Container
    err = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
    if (err == 0 && verbose_output) {
        warn("Container opened successfully...\n");
    } else if (err == -DER_NONEXIST){
        if (allow_creation) {
            // Create container if it doesn't exist and you are allowed to do that.
            check_error_code(dfs_cont_create(poh, co_uuid, NULL, &coh, NULL), "Creating Container");
        } else {
            warn("Container doesn't exist...\n");
            exit(0);
        }
    } else {
        check_error_code(err, "Opening Container");
    }
    // Mounting DFS system.
    check_error_code(dfs_mount(poh, coh, O_RDWR, &dfs), "Mounting DFS to DAOS");
}

int dfs_file_exists(const char *file_directory){
    if (file_directory[0] == '/') {
        dfs_obj_t *parent = NULL;
        int err = dfs_lookup(dfs, file_directory, O_RDWR, &parent, NULL, NULL);
        if (err == 0) {
            if (verbose_output) {
                warn("Directory '%s'already exist \n", file_directory);
            }
            dfs_release(parent);
            return 1;
        } else {
            if (verbose_output) {
                warn("Directory '%s' doesn't exist \n", file_directory);
            }
            return 0;
        }
    } else {
        dfs_obj_t *parent = NULL;
        int err = dfs_lookup_rel(dfs, NULL, file_directory, O_RDWR, &parent, NULL, NULL);
        if (err == 0) {
            if (verbose_output) {
                warn("File '%s'already exist \n", file_directory);
            }
            dfs_release(parent);
            return 1;
        } else {
            if (verbose_output) {
                warn("File '%s' doesn't exist \n", file_directory);
            }
            return 0;
        }
    }
}

dfs_obj_t * get_parent_of_file(const char *file_directory, int allow_creation,
                               char *file_name) {
    dfs_obj_t *parent = NULL;
    daos_oclass_id_t cid= OC_SX;
    char temp[2048];
    int array_len = 0;
    strcpy(temp, file_directory);
    const char *sep = "/";
    char *token = strtok(temp, sep);
    while( token != NULL ) {
        array_len++;
        token = strtok(NULL, sep);
    }
    char **array = malloc(sizeof(char *) * array_len);
    strcpy(temp, file_directory);
    token = strtok(temp, sep);
    int i = 0;
    while( token != NULL ) {
        array[i] = malloc(sizeof(char) * (strlen(token) + 1));
        strcpy(array[i], token);
        token = strtok(NULL, sep);
        i++;
    }
    for (i = 0; i < array_len - 1; i++) {
        dfs_obj_t *temp_obj;
        int err = dfs_lookup_rel(dfs, parent, array[i], O_RDWR, &temp_obj, NULL, NULL);
        if (err == 0) {
            if(verbose_output){
                warn("Subdirectory '%s' already exist \n", array[i]);
            }
        } else if (allow_creation) {
            mode_t mode = 0666;
            err = dfs_mkdir(dfs, parent, array[i], mode,cid);
            if (err == 0) {
                if(verbose_output) {
                    warn("Created directory '%s'\n", array[i]);
                }
                check_error_code(dfs_lookup_rel(dfs, parent, array[i], O_RDWR, &temp_obj, NULL, NULL)
                        , "Lookup after mkdir");
            } else {
                warn("Mkdir on %s failed with error code : %d \n", array[i], err);
            }
        } else {
            warn("Relative lookup on %s failed with error code : %d \n", array[i], err);
        }
        parent = temp_obj;
    }
    strcpy(file_name, array[array_len - 1]);
    for (i = 0; i < array_len; i++) {
        free(array[i]);
    }
    free(array);
    return parent;
}

DAOS_FILE * open_dfs_file(const char *path, mode_t mode, char operation, int allow_directory_creation){
    dfs_obj_t *parent;
    daos_oclass_id_t cid= OC_SX;
    DAOS_FILE *daos_file = malloc(sizeof(DAOS_FILE));
    daos_file->offset = 0 ;
    char filename[2048];
    if (operation == 'r') {
        if (dfs_file_exists(path)) {
            parent = get_parent_of_file(path, allow_directory_creation, filename);
            check_error_code(dfs_open(dfs, parent, filename, mode, O_RDWR, cid, 0, NULL, &(daos_file->file))
                    ,"Opening DAOS Object To Read From");
        } else {
            return  NULL;
        }
    } else {
        parent = get_parent_of_file(path, allow_directory_creation, filename);
        check_error_code(dfs_open(dfs, parent, filename, mode, O_RDWR | O_CREAT, cid, 0, NULL, &(daos_file->file))
                ,"Opening DAOS Object To Write To");
    }
    return daos_file;
}

daos_size_t read_dfs_file(DAOS_FILE* file, char *byte_array, long len){
    daos_size_t size;
    size = read_dfs_file_with_offset(file, byte_array, len, file->offset);
    file->offset += size;
    return size;
}

daos_size_t read_dfs_file_with_offset(DAOS_FILE* file, char *byte_array, long len, long offset){
    d_iov_t iov;
    daos_size_t size;
    d_sg_list_t sgl;
    /** set memory location */
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    d_iov_set(&iov, (void *)byte_array, len);
    sgl.sg_iovs = &iov;
    check_error_code(dfs_read(dfs, file->file, &sgl, offset, &size, 0), "Reading To File");
    if (verbose_output) {
        warn("File size read is %ld \n", size);
    }

    return size;
}

daos_size_t write_dfs_file(DAOS_FILE* file, char *byte_array, long len){
    write_dfs_file_with_offset(file, byte_array, len, file->offset);
    file->offset += len;
    return len;
}

daos_size_t write_dfs_file_with_offset(DAOS_FILE* file, char *byte_array, long len, long offset){
    d_iov_t iov;
    d_sg_list_t sgl;
    /** set memory location */
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    d_iov_set(&iov, (void *)byte_array, len);
    sgl.sg_iovs = &iov;
    check_error_code(dfs_write(dfs, file->file, &sgl, offset, NULL), "Writing To File");
    return len;
}

daos_size_t get_dfs_file_size(DAOS_FILE* file){
    daos_size_t size;
    check_error_code(dfs_get_size(dfs, file->file, &size), "Get DAOS Object Size");
    return  size;
}

void seek_daos_file(DAOS_FILE* file, long offset){
    file->offset = offset;
}

long get_daos_file_offset(DAOS_FILE* file){
    return file->offset;
}

void close_dfs_file(DAOS_FILE *file){
    check_error_code(dfs_release(file->file),"Closing DAOS Object");
    free(file);
}


void fini_dfs_api(){
    check_error_code(dfs_umount(dfs), "Unmounting DFS");
    check_error_code(daos_cont_close(coh, 0), "Closing Container");
    check_error_code(daos_pool_disconnect( poh, NULL), "Closing Pool Connection");
    check_error_code(daos_fini(), "Finalizing DAOS API Library");
}

void get_file_name(FILE* fp, char *file_name){
    char path[1024];
    char result[1024];
    int fd = fileno(fp);
    sprintf(path, "/proc/self/fd/%d", fd);
    memset(result, 0, sizeof(result));
    int error= readlink(path, result, sizeof(result)-1);
    char* token = strtok(result, "/");
    char* temp = token;
    while (token != NULL) {
        temp = token;
        token = strtok(NULL, "/");
    }
    if(error!=0)
        strcpy(file_name, temp);
    return;
}


size_t read_posix(const char *file, char **byte_array) {
	FILE *fl = fopen(file, "r");
	if (fl == 0) {
		warn("Posix read not successfull for file '%s'...", file);
		exit(0);
	}
    fseek(fl, 0, SEEK_END);
    long len = ftell(fl);
    *byte_array = malloc(len);
    fseek(fl, 0, SEEK_SET);
    size_t file_size = fread(*byte_array, 1, len, fl);
    fclose(fl);
    if (verbose_output) {
    	warn("Read %zu bytes from posix file %s\n", file_size*sizeof(**byte_array), file);
    }
    return len;
}

size_t write_posix(const char *file, char *byte_array, int len) {
    FILE *fl = fopen(file, "w");
    if (fl == 0) {
        warn("Posix write not successfull for file '%s'...", file);
        exit(0);
    }
    size_t file_size = fwrite(byte_array, 1, len, fl);
    fclose(fl);
    if (verbose_output) {
        warn("Writed %zu bytes to posix file %s\n", file_size*sizeof(*byte_array), file);
    }
    return file_size;
}


void remove_dfs_file(const char *file){
    if(dfs_file_exists(file)){
        char *file_name = malloc(1024 * sizeof(char));
        dfs_obj_t * parent = get_parent_of_file(file, 0, file_name);
        check_error_code(dfs_remove(dfs, parent, file_name, 0, NULL), "Removing file");
        dfs_release(parent);
        free(file_name);
    } else {
        warn("File %s doesn't exist to be removed \n", file);
    }
}


dfs_t* get_dfs(){
	return dfs;
}

