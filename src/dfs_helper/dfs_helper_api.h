/**
 * \file
 *
 * DAOS File System Helper API
 *
 * The DFS Helper API provides a POSIX like API directly
 * on top of the DAOS FS API.
 */

#ifndef __DAOS_HELPER_FS_H__
#define __DAOS_HELPER_FS_H__

#if defined(__cplusplus)
extern "C" {
#endif

#include "daos.h"
#include "daos_fs.h"

typedef struct DAOS_FILE {
	/** A daos object handle that will contain the file handle of the file to write and read */
	dfs_obj_t *file;
	/** 
	 * The offset which represents the number of bytes already written/read using default functions,
	 * Modified by seek directly. Incremented naturally by read_dfs_file, write_dfs_file functions.
	 * Won't be modified by read_dfs_file_with_offset, write_dfs_file_with_offset.
	 * A getter with get_daos_file_offset to obtain the offset.
	 */
	long offset;
} DAOS_FILE;

/**
 * Initialization for the DFS HELPER API, must be called once to use any of the functions, and would be valid until finalized.
 *
 * Initializes DAOS API library, connects to a pool, connects to a container and mounts a file system over DAOS.
 *
 * This will set the internal state of the helper API, it won't return anything.
 *
 * If any command fails, this will print a message with the error return code and exit the program.
 *
 * You can enable verbose output to enable printing in case of success.
 *
 * You can enable container creation when the container uuid is not existent, if disabled and no container with the given uuid exists
 * the program will terminate.
 *
 * The pool uuid and service replica ranks can be obtained by cmd command `dmg system list-pools`.
 * The container uuid can be obtained by cmd command `dmg pool list-containers`.
 *
 * \param[in]	pool_uid		String containing the uuid of the pool to connect to.
 * \param[in]	pool_svc_list	String containing the pool service replica ranks seperated by : eg: "0:1:2".
 * \param[in]	container_uuid	String containing the uuid of the container to connect to(or create).
 * \param[in]	allow_creation	Integer to enable container creation if not existent already if given non-zero values.
 * \param[in]	verbose_output 	Integer to enable verbosity to print messages in case of sucess if given non-zero values.
 *
 */
void init_dfs_api(const char *pool_uid, const char *pool_svc_list, const char *container_uuid,
		int allow_creation, int verbose_output);

/**
 * Set the current offset in bytes for an opened DAOS file.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \param[in] 	offset			The absolute value of the offset to set the DAOS file to.
 */
void seek_daos_file(DAOS_FILE* file, long offset);

/**
 * Get the current offset in bytes for an opened DAOS file.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \return 		Will return the current offset in bytes in the given file.
 */
long get_daos_file_offset(DAOS_FILE* file);

/**
 * Reads a byte array from a specified file.
 * Internally will update the offset of the file struct.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \param[in]	byte_array		Pointer to the byte array we want to read to.
 * \param[in]	len				Long that containes the length of the byte array to be read from the file.
 * \return		Will return the number of bytes read from the file.
 */
daos_size_t read_dfs_file(DAOS_FILE* file, char *byte_array, long len);

/**
 * Reads a byte array from a specified file.
 * Will not update the offset of the file.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \param[in]	byte_array		Pointer to the byte array we want to read to.
 * \param[in]	len				Long that containes the length of the byte array to be read from the file.
 * \param[in]	offset			Long that contains the offset to bypass in the file before doing the operation.
 * \return		Will return the number of bytes read from the file.
 */
daos_size_t read_dfs_file_with_offset(DAOS_FILE* file, char *byte_array, long len, long offset);

/**
 * Writes a byte array to a specified file.
 * Internally will update the offset of the file struct.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \param[in]	byte_array		Pointer to the byte array we want to write.
 * \param[in]	len				Long that containes the length of the byte array to be written to the file.
 * \param[in]	verbose_output	Integer to enable verbosity to print messages in case of sucess if given non-zero values.
 * \return		Will return the number of bytes written to the file.
 */
daos_size_t write_dfs_file(DAOS_FILE* file, char *byte_array, long len);

/**
 * Writes a byte array to a specified file.
 * Will not update the offset of the file.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \param[in]	byte_array		Pointer to the byte array we want to write.
 * \param[in]	len				Long that containes the length of the byte array to be written to the file.
 * \param[in]	offset			Long that contains the offset to bypass in the file before doing the operation.
 * \param[in]	verbose_output	Integer to enable verbosity to print messages in case of sucess if given non-zero values.
 * \return		Will return the number of bytes written to the file.
 */
daos_size_t write_dfs_file_with_offset(DAOS_FILE* file, char *byte_array, long len, long offset);

/**
 * Query for the size of an opened file in DAOS.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 * \return		Will return the size of the file given to the function.
 */
daos_size_t get_dfs_file_size(DAOS_FILE* file);

/**
 * Tries to open a file.
 * In case of write, it will try to create or open the file if it is existent already.
 * If not allowed to create the directory and the directory is non-existent,
 * it will throw an error and terminate program. Otherwise, will create directory and file.
 * In case of read, it will check for file existance and either return NULL if non-existent or returns
 * an opened DAOS FILE.
 *
 * \param[in]	path			String containing the absolute path of the file we want to open.
 * \param[in]	mode			The mode to open the file with.
 * \param[in]	operation		A character that indicates read 'r' or write 'w' operation.
 * \param[in]	allow_creation	Allow creation of the file directory of the file(Only relevant in case of write).
 * \return 		A pointer to the opened DAOS file if it was opened successfully, NULL otherwise.
 */
DAOS_FILE * open_dfs_file(const char *path, mode_t mode, char operation, int allow_directory_creation);

/**
 * Closes an opened DAOS file.
 *
 * \param[in]	file			Pointer to the opened DAOS file.
 */
void close_dfs_file(DAOS_FILE *file);

/**
 * Check if a file exists in DAOS.
 *
 * \param[in]	file_directory	String containing the absolute path of the file we want to check for existence.
 * \return		Will return 0 if non-existent, 1 if it exists.
 */
int dfs_file_exists(const char *file_directory);

/**
 * Finalizes the DFS helper API, cleans reused resources to get ready for another initialization or program ending.
 *
 * Unmounts a file system over DAOS, closes container, closes pool connection and finalizes DAOS API library.
 *
 * This will render the pool connection handle, the container open handle, and the pointer to the dfs system object created
 * useless and they shouldn't be used after a call to this function.
 * Users should not close any of them by any other than the use of the finalize_daos_dfs function.
 *
 * If any command fails, this will print a message with the error return code and exit the program.
 *
 *
 */
void fini_dfs_api();

void get_file_name(FILE* fp, char *file_name);


/**
 * Reads a byte array from a specified file in the posix system.
 *
 * \param[in]	file			String containing the path of the file we want to read from.
 * \param[in]	byte_array		Pointer to the byte array to be allocated and filled with the bytes read.
 * \param[in]	verbose_output	Integer to enable verbosity to print messages in case of sucess if given non-zero values.
 * \return		Will return the number of bytes read from the file.
 */
size_t read_posix(const char *file, char **byte_array);

/**
 * Writes a byte array to a specified file in the posix system.
 *
 * \param[in]   file            String containing the path of the file we want to write to.
 * \param[in]   byte_array      Pointer to the byte array to be written to the file.
 * \param[in]   len             Integer containing the number of bytes to be written from the array to the file.
 * \param[in]   verbose_output  Integer to enable verbosity to print messages in case of sucess if given non-zero values.
 * \return      Will return the number of bytes written to the file.
 */
size_t write_posix(const char *file, char *byte_array, int len);


/**
 * Remove file from dfs container.
 *
 * \param[in]   file            String containing the path of the file we want to remove.
 */
void remove_dfs_file(const char *file);


dfs_t* get_dfs();

#if defined(__cplusplus)
}
#endif
#endif /* __DAOS_HELPER_FS_H__ */
