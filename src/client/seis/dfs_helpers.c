/*
 * dfs_helpers.c
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#include "dfs_helpers.h"

int oid_gen(dfs_t *dfs, uint16_t oclass, bool file, daos_obj_id_t *oid)
{
	daos_ofeat_t	feat = 0;
	int		rc;

	if (oclass == 0)
		oclass = dfs->attr.da_oclass_id;

	D_MUTEX_LOCK(&dfs->lock);
	/** If we ran out of local OIDs, alloc one from the container */
	if (dfs->oid.hi >= MAX_OID_HI) {
		/** Allocate an OID for the namespace */
		rc = daos_cont_alloc_oids(dfs->coh, 1, &dfs->oid.lo, NULL);
		if (rc) {
			D_ERROR("daos_cont_alloc_oids() Failed (%d)\n", rc);
			D_MUTEX_UNLOCK(&dfs->lock);
			return daos_der2errno(rc);
		}
		dfs->oid.hi = 0;
	}

	/** set oid and lo, bump the current hi value */
	oid->lo = dfs->oid.lo;
	oid->hi = dfs->oid.hi++;
	D_MUTEX_UNLOCK(&dfs->lock);

	/** if a regular file, use UINT64 typed dkeys for the array object */
	if (file)
		feat = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT |
			DAOS_OF_ARRAY_BYTE;

	/** generate the daos object ID (set the DAOS owned bits) */
	daos_obj_generate_id(oid, feat, oclass, 0);

	return 0;
}

void oid_cp(daos_obj_id_t *dst, daos_obj_id_t src)
{
	dst->hi = src.hi;
	dst->lo = src.lo;
}

int get_daos_obj_mode(int flags)
{
	if ((flags & O_ACCMODE) == O_RDONLY)
		return DAOS_OO_RO;
	else if ((flags & O_ACCMODE) == O_RDWR ||
		 (flags & O_ACCMODE) == O_WRONLY)
		return DAOS_OO_RW;
	else
		return -1;
}

int check_name(const char *name)
{
	if (name == NULL || strchr(name, '/'))
		return EINVAL;
	if (strnlen(name, DFS_MAX_PATH) > DFS_MAX_PATH)
		return EINVAL;
	return 0;
}

int fetch_entry(daos_handle_t oh, daos_handle_t th, const char *name,
					bool *exists, struct dfs_entry *entry)
{
	d_sg_list_t	sgl;
	d_iov_t		sg_iovs[INODE_AKEYS];
	daos_iod_t	iod;
	daos_recx_t	recx;
	char		*value = NULL;
	daos_key_t	dkey;
	unsigned int	i;
	int		rc;

	D_ASSERT(name);

	/** TODO - not supported yet */
	if (strcmp(name, ".") == 0)
		D_ASSERT(0);

	d_iov_set(&dkey, (void *)name, strlen(name));
	d_iov_set(&iod.iod_name, INODE_AKEY_NAME, strlen(INODE_AKEY_NAME));
	iod.iod_nr	= 1;
	recx.rx_idx	= 0;
	recx.rx_nr	= sizeof(mode_t) + sizeof(time_t) * 3 +
			    sizeof(daos_obj_id_t) + sizeof(daos_size_t);
	iod.iod_recxs	= &recx;
	iod.iod_type	= DAOS_IOD_ARRAY;
	iod.iod_size	= 1;
	i = 0;

	d_iov_set(&sg_iovs[i++], &entry->mode, sizeof(mode_t));
	d_iov_set(&sg_iovs[i++], &entry->oid, sizeof(daos_obj_id_t));
	d_iov_set(&sg_iovs[i++], &entry->atime, sizeof(time_t));
	d_iov_set(&sg_iovs[i++], &entry->mtime, sizeof(time_t));
	d_iov_set(&sg_iovs[i++], &entry->ctime, sizeof(time_t));
	d_iov_set(&sg_iovs[i++], &entry->chunk_size, sizeof(daos_size_t));


	sgl.sg_nr	= i;
	sgl.sg_nr_out	= 0;
	sgl.sg_iovs	= sg_iovs;

	rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL, NULL);
	if (rc) {
		D_ERROR("Failed to fetch entry %s (%d)\n", name, rc);
	}

	if (sgl.sg_nr_out == 0)
		*exists = false;
	else
		*exists = true;

	return rc;
}

int insert_entry(daos_handle_t oh, daos_handle_t th, const char *name,
		struct dfs_entry entry){
		d_sg_list_t	sgl;
		d_iov_t		sg_iovs[INODE_AKEYS];
		daos_iod_t	iod;
		daos_recx_t	recx;
		daos_key_t	dkey;
		unsigned int	i;
		int		rc;


		d_iov_set(&dkey, (void *)name, strlen(name));
		d_iov_set(&iod.iod_name, INODE_AKEY_NAME, strlen(INODE_AKEY_NAME));

		iod.iod_nr	= 1;
		recx.rx_idx	= 0;
		recx.rx_nr	= sizeof(mode_t) + sizeof(time_t) * 3 +
					sizeof(daos_obj_id_t) + sizeof(daos_size_t);
		iod.iod_recxs	= &recx;
		iod.iod_type	= DAOS_IOD_ARRAY;
		iod.iod_size	= 1;
		i = 0;

		d_iov_set(&sg_iovs[i++], &entry.mode, sizeof(mode_t));
		d_iov_set(&sg_iovs[i++], &entry.oid, sizeof(daos_obj_id_t));
		d_iov_set(&sg_iovs[i++], &entry.atime, sizeof(time_t));
		d_iov_set(&sg_iovs[i++], &entry.mtime, sizeof(time_t));
		d_iov_set(&sg_iovs[i++], &entry.ctime, sizeof(time_t));
		d_iov_set(&sg_iovs[i++], &entry.chunk_size, sizeof(daos_size_t));

		sgl.sg_nr	= i;
		sgl.sg_nr_out	= 0;
		sgl.sg_iovs	= sg_iovs;

		rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
		if (rc) {
				D_ERROR("Failed to insert entry %s (%d)\n", name, rc);
				return daos_der2errno(rc);
			}

	return rc;
}

