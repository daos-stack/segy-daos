/*
 * dfs.c
 *
 *  Created on: Jan 27, 2021
 *      Author: mirnamoawad
 */

#include "daos_primitives/dfs_helpers.h"
static daos_handle_t poh;
static daos_handle_t coh;
static dfs_t *dfs;
int verbose_output = 1;

enum handleType {
	HANDLE_POOL,
	HANDLE_CO,
	HANDLE_DFS
};

int
init_dfs_api(const char *pool_uid,const char *container_uuid,
	     int allow_creation, int verbose)
{
	/** Pool UUID */
	uuid_t po_uuid;
	/** Container UUID */
	uuid_t co_uuid;
	verbose_output = verbose;
	/** Initialize DAOS API. */
	DSG_ERROR(daos_init(), "Initializing DAOS API Library", error1);
	/** Parse Pool UUID. */
	int err = uuid_parse(pool_uid, po_uuid);
	if (po_uuid == NULL) {
		DSG_ERROR(err, "Error parsing pool uuid\n", error1);
	}
	if (verbose_output) {
		warn("Pool UUID : %s \n", pool_uid);
	}
	/** Parse Container UUID . */
	err = uuid_parse(container_uuid, co_uuid);
	if (co_uuid == NULL) {
		DSG_ERROR(err,"Error parsing container uuid\n", error1);
	}
	if (verbose_output) {
		warn("Container UUID : %s \n", container_uuid);
	}
	// Open Container
#ifndef MPI_BUILD
	DSG_ERROR(daos_pool_connect(po_uuid, 0, DAOS_PC_RW, &poh, NULL, NULL),
			  "Connecting To Pool", error1);
	err = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
	if (err == 0 && verbose_output) {
		warn("Container opened successfully...\n");
	} else if (err == -DER_NONEXIST) {
		if (allow_creation) {
			DSG_ERROR(dfs_cont_create(poh, co_uuid, NULL,&coh,NULL),
				  "Creating Container", error1);
		} else {
			warn("Container doesn't exist...\n");
			return err;
		}
	} else {
		DSG_ERROR(err, "Opening Container", error1);
	}
	DSG_ERROR(dfs_mount(poh, coh, O_RDWR, &dfs),"Mounting DFS to DAOS",
			  error1);
#else
	int mpi_flag;
	MPI_Initialized(&mpi_flag);

	if(mpi_flag == 0) {
		DSG_ERROR(daos_pool_connect(po_uuid, 0, DAOS_PC_RW, &poh, NULL, NULL),
				  "Connecting To Pool", error1);
		err = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
	} else {
		int	process_rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
		if (process_rank == 0) {
			DSG_ERROR(daos_pool_connect(po_uuid, 0, DAOS_PC_RW, &poh, NULL, NULL),
				  "Connecting To Pool", error1);
		}
		handle_share(&poh, HANDLE_POOL);
		if (process_rank == 0) {
			err = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
		}
		MPI_Bcast(&err, 1, MPI_INT, 0, MPI_COMM_WORLD);
	}
	if (err == 0 && verbose_output) {
		if(mpi_flag == 1) {
			handle_share(&coh, HANDLE_CO);
		}
		warn("Container opened successfully...\n");
	} else if (err == -DER_NONEXIST) {
		if (allow_creation) {
			if(mpi_flag == 1) {
				int	process_rank;
				MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

				if(process_rank == 0 ){
					DSG_ERROR(dfs_cont_create(poh, co_uuid, NULL,&coh,NULL),
					  "Creating Container", error1);
				}
				handle_share(&coh, HANDLE_CO);
				MPI_Barrier(MPI_COMM_WORLD);
			} else {
				DSG_ERROR(dfs_cont_create(poh, co_uuid, NULL,&coh,NULL),
					  "Creating Container", error1);
			}
		} else {
			warn("Container doesn't exist...\n");
			return err;
		}
	} else {
		DSG_ERROR(err, "Opening Container", error1);
	}

	if(mpi_flag == 1) {
		int	process_rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

		if(process_rank == 0){
			DSG_ERROR(dfs_mount(poh, coh, O_RDWR, &dfs),"Mounting DFS to DAOS",
							  error1);
		}
		handle_share(&coh, HANDLE_DFS);
	} else {
		DSG_ERROR(dfs_mount(poh, coh, O_RDWR, &dfs),"Mounting DFS to DAOS",
						  error1);
	}
#endif
	return 0;

error1:
	return err;
}

int
dfs_file_exists(const char *file_directory)
{
	if (file_directory[0] == '/') {
		dfs_obj_t *parent = NULL;
		int err = dfs_lookup(dfs, file_directory, O_RDWR, &parent, NULL,
				     NULL);
		if (err == 0) {
			if (verbose_output) {
				warn("Directory '%s'already exist \n",
				      file_directory);
			}
			dfs_release(parent);
			return 1;
		} else {
			if (verbose_output) {
				warn("Directory '%s' doesn't exist \n",
				      file_directory);
			}
			return 0;
		}
	} else {
		dfs_obj_t *parent = NULL;
		int err = dfs_lookup_rel(dfs, NULL, file_directory, O_RDWR,
				&parent, NULL, NULL);
		if (err == 0) {
			if (verbose_output) {
				warn("File '%s'already exist \n",
				      file_directory);
			}
			dfs_release(parent);
			return 1;
		} else {
			if (verbose_output) {
				warn("File '%s' doesn't exist \n",
				      file_directory);
			}
			return 0;
		}
	}
}

dfs_obj_t*
get_parent_of_file(const char *file_directory, int allow_creation,
		   char *file_name)
{
	dfs_obj_t 		*parent = NULL;
	daos_oclass_id_t 	cid = DAOS_OBJ_CLASS_ID;
	char 			temp[2048];
	int 			array_len = 0;
	strcpy(temp, file_directory);
	const char *sep = "/";
	char *token = strtok(temp, sep);
	while (token != NULL) {
		array_len++;
		token = strtok(NULL, sep);
	}
	char **array = malloc(sizeof(char*) * array_len);
	strcpy(temp, file_directory);
	token = strtok(temp, sep);
	int i = 0;
	while (token != NULL) {
		array[i] = malloc(sizeof(char) * (strlen(token) + 1));
		strcpy(array[i], token);
		token = strtok(NULL, sep);
		i++;
	}
	for (i = 0; i < array_len - 1; i++) {
		dfs_obj_t *temp_obj;
		int err = dfs_lookup_rel(get_dfs(), parent, array[i], O_RDWR,
				&temp_obj, NULL, NULL);
		if (err == 0) {
			if (verbose_output) {
				warn("Subdirectory '%s' already exist \n",
				      array[i]);
			}
		} else if (allow_creation) {
			mode_t mode = 0666;
			err = dfs_mkdir(dfs, parent, array[i], mode, cid);
			if (err == 0) {
				if (verbose_output) {
					warn("Created directory '%s'\n",
					      array[i]);
				}
				DSG_ERROR(dfs_lookup_rel(get_dfs(),parent,
								array[i],
								O_RDWR,
								&temp_obj, NULL,
								NULL),
					   "Lookup after mkdir",error1);
			} else {
				warn("Mkdir on %s failed with error code: %d\n",
				      array[i], err);
			}
		} else {
			warn("Relative lookup on %s failed "
			     "with error code: %d\n",
			     array[i], err);
		}
		parent = temp_obj;
	}
	strcpy(file_name, array[array_len - 1]);
	for (i = 0; i < array_len; i++) {
		free(array[i]);
	}
	free(array);
	return parent;

error1:
	for (i = 0; i < array_len; i++) {
		free(array[i]);
	}
	free(array);
	return NULL;
}

DAOS_FILE*
open_dfs_file(const char *path, mode_t mode, char operation,
	      int allow_directory_creation)
{
	daos_oclass_id_t 	cid = DAOS_OBJ_CLASS_ID;
	dfs_obj_t 		*parent;
	DAOS_FILE 		*daos_file;

	daos_file = malloc(sizeof(DAOS_FILE));
	daos_file->offset = 0;
	char filename[2048];
	if (operation == 'r') {
		if (dfs_file_exists(path)) {
			parent = get_parent_of_file(path,
					allow_directory_creation, filename);
			DSG_ERROR(dfs_open(dfs, parent, filename, mode,
							O_RDWR, cid, 0, NULL,
							&(daos_file->file)),
				  "Opening DAOS Object To Read From",error1);
		} else {
			return NULL;
		}
	} else {
		parent = get_parent_of_file(path, allow_directory_creation,
				filename);
		DSG_ERROR(dfs_open(dfs, parent, filename, mode,
				   O_RDWR | O_CREAT, cid, 0, NULL,
				   &(daos_file->file)),
			  "Opening DAOS Object To Write To",error1);
	}
	return daos_file;

error1:
	free(daos_file);
	return NULL;
}

daos_size_t
read_dfs_file(DAOS_FILE *file, char *byte_array, long len)
{
	daos_size_t size;

	size = read_dfs_file_with_offset(file, byte_array, len, file->offset);
	file->offset += size;

	return size;
}

daos_size_t
read_dfs_file_with_offset(DAOS_FILE *file, char *byte_array, long len,
			  long offset)
{
	daos_size_t	size;
	d_sg_list_t 	sgl;
	d_iov_t 	iov;

	/** set memory location */
	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	d_iov_set(&iov, (void*) byte_array, len);
	sgl.sg_iovs = &iov;

	DSG_ERROR(dfs_read(dfs, file->file, &sgl, offset, &size, 0),
		 "Reading To File", error1);

	if (verbose_output) {
		warn("File size read is %ld \n", size);
	}

	return size;

error1:
	return -1;
}

daos_size_t
write_dfs_file(DAOS_FILE *file, char *byte_array, long len)
{
	write_dfs_file_with_offset(file, byte_array, len, file->offset);
	file->offset += len;
	return len;
}

daos_size_t
write_dfs_file_with_offset(DAOS_FILE *file, char *byte_array, long len,
			   long offset)
{
	d_sg_list_t 	sgl;
	d_iov_t 	iov;
	/** set memory location */
	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	d_iov_set(&iov, (void*) byte_array, len);
	sgl.sg_iovs = &iov;
	DSG_ERROR(dfs_write(dfs, file->file, &sgl, offset, NULL),
		  "Writing To File", error1);
	return len;

error1:
	return -1;
}

daos_size_t
get_dfs_file_size(DAOS_FILE *file)
{
	daos_size_t size;
	DSG_ERROR(dfs_get_size(dfs, file->file, &size),
		  "Get DAOS Object Size",error1);
	return size;

error1:
	return -1;
}

void
seek_daos_file(DAOS_FILE *file, long offset)
{
	file->offset = offset;
}

long
get_daos_file_offset(DAOS_FILE *file)
{
	return file->offset;
}

int
close_dfs_file(DAOS_FILE *file)
{
	DSG_ERROR(dfs_release(file->file), "Closing DAOS Object", error1);
	free(file);
	return 0;

error1:
	free(file);
	return -1;
}

int
fini_dfs_api()
{

	int rc = 0;
#ifdef MPI_BUILD
	int mpi_flag;
	MPI_Initialized(&mpi_flag);
	if(mpi_flag == 1) {
		MPI_Barrier(MPI_COMM_WORLD);
	}
#endif
	rc = dfs_umount(dfs);
	DSG_ERROR(rc, "Unmounting DFS",error1);
	rc = daos_cont_close(coh, 0);
	DSG_ERROR(rc , "Closing Container",error1);
	rc = daos_pool_disconnect(poh, NULL);
	DSG_ERROR(rc , "Closing Pool Connection",error1);
	rc = daos_fini();
	DSG_ERROR(rc , "Finalizing DAOS API Library",error1);

	return rc;
error1:
	return rc;
}

int
get_file_name(FILE *fp, char *file_name)
{
	char 		path[1024];
	char 		result[1024];
	int 		fd;

	fd = fileno(fp);
	sprintf(path, "/proc/self/fd/%d", fd);
	memset(result, 0, sizeof(result));

	char	*token;
	char 	*temp;
	int 	error;

	error= readlink(path, result, sizeof(result) - 1);
	token = strtok(result, "/");
	temp = token;

	while (token != NULL) {
		temp = token;
		token = strtok(NULL, "/");
	}
	if (error != 0)
		strcpy(file_name, temp);
	return 0;
}

size_t
read_posix(const char *file, char **byte_array)
{
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
		warn("Read %zu bytes from posix file %s\n",
		     file_size * sizeof(**byte_array), file);
	}
	return len;
}

size_t
write_posix(const char *file, char *byte_array, int len)
{
	FILE *fl = fopen(file, "w");
	if (fl == 0) {
		warn("Posix write not successfull for file '%s'...", file);
		exit(0);
	}
	size_t file_size = fwrite(byte_array, 1, len, fl);
	fclose(fl);
	if (verbose_output) {
		warn("Writed %zu bytes to posix file %s\n",
		     file_size * sizeof(*byte_array), file);
	}
	return file_size;
}

int
remove_dfs_file(const char *file)
{
	char* file_name;
	if (dfs_file_exists(file)) {
		file_name = malloc(1024 * sizeof(char));
		dfs_obj_t *parent = get_parent_of_file(file, 0, file_name);
		DSG_ERROR(dfs_remove(dfs, parent, file_name, 0, NULL),
				"Removing file", error1);
		dfs_release(parent);
		free(file_name);
	} else {
		warn("File %s doesn't exist to be removed \n", file);
	}

	return 0;

error1:
	free(file_name);
	return -1;
}

dfs_t*
get_dfs()
{
	return dfs;
}

int
insert_dfs_entry(daos_handle_t oh, daos_handle_t th, const char *name,
		 struct dfs_entry entry)
{
	unsigned int 		i;
	d_sg_list_t 		sgl;
	daos_recx_t 		recx;
	daos_key_t 		dkey;
	daos_iod_t 		iod;
	d_iov_t 		sg_iovs[INODE_AKEYS];
	int 			rc;

	d_iov_set(&dkey, (void*) name, strlen(name));
	d_iov_set(&iod.iod_name, INODE_AKEY_NAME, strlen(INODE_AKEY_NAME));

	iod.iod_nr = 1;
	recx.rx_idx = 0;
	recx.rx_nr = sizeof(mode_t) + sizeof(time_t) * 3 + sizeof(daos_obj_id_t)
		     + sizeof(daos_size_t);
	iod.iod_recxs = &recx;
	iod.iod_type = DAOS_IOD_ARRAY;
	iod.iod_size = 1;
	i = 0;

	d_iov_set(&sg_iovs[i++], &entry.mode, sizeof(mode_t));
	d_iov_set(&sg_iovs[i++], &entry.oid, sizeof(daos_obj_id_t));
	d_iov_set(&sg_iovs[i++], &entry.atime, sizeof(time_t));
	d_iov_set(&sg_iovs[i++], &entry.mtime, sizeof(time_t));
	d_iov_set(&sg_iovs[i++], &entry.ctime, sizeof(time_t));
	d_iov_set(&sg_iovs[i++], &entry.chunk_size, sizeof(daos_size_t));

	sgl.sg_nr = i;
	sgl.sg_nr_out = 0;
	sgl.sg_iovs = sg_iovs;

	rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
	if (rc) {
		D_ERROR("Failed to insert entry %s (%d)\n", name, rc);
		return daos_der2errno(rc);
	}

	return rc;
}

int
dfs_helpers_obj_set_oid(daos_obj_id_t *oid, daos_ofeat_t ofeats,
			daos_oclass_id_t cid, uint32_t args)
{
	uint64_t hdr;

	/* TODO: add check at here, it should return error if user specified
	 * bits reserved by DAOS
	 */
	oid->hi &= (1ULL << OID_FMT_INTR_BITS) - 1;
	/**
	 * | Upper bits contain
	 * | OID_FMT_VER_BITS (version) |
	 * | OID_FMT_FEAT_BITS (object features) |
	 * | OID_FMT_CLASS_BITS (object class) |
	 * | 96-bit for upper layer ... |
	 */
	hdr = ((uint64_t) OID_FMT_VER << OID_FMT_VER_SHIFT);
	hdr |= ((uint64_t) ofeats << OID_FMT_FEAT_SHIFT);
	hdr |= ((uint64_t) cid << OID_FMT_CLASS_SHIFT);
	oid->hi |= hdr;

	return 0;

}

int
dsg_helpers_oclass_select(daos_oclass_id_t oc_id, daos_oclass_id_t *oc_id_p)
{
	struct dc_pool 		*pool;
	struct pl_map_attr 	attr;
	int 			rc;

	pool = dc_hdl2pool(poh);
	D_ASSERT(pool);

	rc = pl_map_query(pool->dp_pool, &attr);
	D_ASSERT(rc == 0);
	dc_pool_put(pool);

	D_DEBUG(DB_TRACE, "available domain=%d, targets=%d\n",
			attr.pa_domain_nr, attr.pa_target_nr);

	return daos_oclass_fit_max(oc_id, attr.pa_domain_nr, attr.pa_target_nr,
			oc_id_p);
}

int
oid_gen(daos_oclass_id_t oclass, bool file, daos_obj_id_t *oid, bool array)
{

	daos_ofeat_t 	feat = 0;
	int 		rc;

	if (oclass == 0)
		oclass = get_dfs()->attr.da_oclass_id;

	rc = dsg_helpers_oclass_select(oclass, &oclass);
	if (rc)
		return rc;

	D_MUTEX_LOCK(&get_dfs()->lock);
	/** If we ran out of local OIDs, alloc one from the container */
	if (get_dfs()->oid.hi >= MAX_OID_HI) {
		/** Allocate an OID for the namespace */
		rc = daos_cont_alloc_oids(get_dfs()->coh, 1, &get_dfs()->oid.lo,
					  NULL);
		if (rc) {
			D_ERROR("daos_cont_alloc_oids() Failed (%d)\n", rc);
			D_MUTEX_UNLOCK(&get_dfs()->lock);
			return daos_der2errno(rc);
		}
		get_dfs()->oid.hi = 0;
	}

	/** set oid and lo, bump the current hi value */
	oid->lo = get_dfs()->oid.lo;
	oid->hi = get_dfs()->oid.hi++;
	D_MUTEX_UNLOCK(&get_dfs()->lock);

	/** if a regular file, use UINT64 typed dkeys for the array object */
	if (file)
		feat = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT
				| DAOS_OF_ARRAY_BYTE;

	/** generate the daos object ID (set the DAOS owned bits) */
	if (array) {
		daos_array_generate_id(oid, oclass, false, 0);
	} else {
		/** generate the daos object ID (set the DAOS owned bits) */
		dfs_helpers_obj_set_oid(oid, feat, oclass, 0);
	}

	return 0;
}

int
oid_cp(daos_obj_id_t *dst, daos_obj_id_t src)
{
	dst->hi = src.hi;
	dst->lo = src.lo;
	return 0;
}

int
lookup_dfs_obj(const char *path, int flags, dfs_obj_t **dfs_obj)
{
	struct stat 	stbuf;
	mode_t 		mode;
	int 		rc;

	/** Lookup and open the dfs object given its path **/
	rc = dfs_lookup(get_dfs(), path, flags, dfs_obj, &mode, &stbuf);
	if (rc != 0) {
		err("Looking up path<%s> in dfs failed , error code = %d \n",
		     path, rc);
		return rc;
	}
	return rc;
}

int
get_daos_obj_mode(int flags)
{
	if ((flags & O_ACCMODE) == O_RDONLY)
		return DAOS_OO_RO;
	else if ((flags & O_ACCMODE) == O_RDWR
			|| (flags & O_ACCMODE) == O_WRONLY)
		return DAOS_OO_RW;
	else
		return -1;
}

void
handle_share(daos_handle_t *hdl, int type)
{
#ifdef MPI_BUILD
	d_iov_t	ghdl = { NULL, 0, 0 };
	int	rc;
	int	process_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

	if (process_rank == 0) {
		/** fetch size of global handle */
		if (type == HANDLE_POOL) {
			rc = daos_pool_local2global(*hdl, &ghdl);
		} else if (type == HANDLE_CO) {
			rc = daos_cont_local2global(*hdl, &ghdl);
		} else {
			rc = dfs_local2global(dfs, &ghdl);
		}
//		ASSERT(rc == 0, "local2global failed with %d", rc);
		DSG_ERROR(rc, "local2global failed", error);

	}

	/** broadcast size of global handle to all peers */
	MPI_Bcast(&ghdl.iov_buf_len, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

	/** allocate buffer for global pool handle */
	ghdl.iov_buf = malloc(ghdl.iov_buf_len);
	ghdl.iov_len = ghdl.iov_buf_len;

	if (process_rank == 0) {
		/** generate actual global handle to share with peer tasks */
		if (type == HANDLE_POOL) {
			rc = daos_pool_local2global(*hdl, &ghdl);
		} else if (type == HANDLE_CO) {
			rc = daos_cont_local2global(*hdl, &ghdl);
		} else {
			rc = dfs_local2global(dfs, &ghdl);
		}
		DSG_ERROR(rc, "local2global failed", error);
	}

	/** broadcast global handle to all peers */
	MPI_Bcast(ghdl.iov_buf, ghdl.iov_len, MPI_BYTE, 0, MPI_COMM_WORLD);

	if (process_rank != 0) {
		/** unpack global handle */
		if (type == HANDLE_POOL) {
			/* NB: Only pool_global2local are different */
			rc = daos_pool_global2local(ghdl, hdl);
		} else if (type == HANDLE_CO) {
			rc = daos_cont_global2local(poh, ghdl, hdl);
		} else {
			rc = dfs_global2local(poh, coh, 0, ghdl, &dfs);
		}
		DSG_ERROR(rc, "global2local failed", error);
	}

	free(ghdl.iov_buf);

	MPI_Barrier(MPI_COMM_WORLD);
error:	return;
#endif
}


