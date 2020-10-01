/*
 * dfs_helpers.h
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#ifndef LSU_SRC_CLIENT_SEIS_DFS_HELPERS_H_
#define LSU_SRC_CLIENT_SEIS_DFS_HELPERS_H_

#define INODE_AKEYS	7
#define INODE_AKEY_NAME	"DFS_INODE"
#define MAX_OID_HI ((1UL << 32) - 1)


#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <daos/checksum.h>
#include <daos/common.h>
#include <daos/event.h>
#include <daos/container.h>
#include <daos/array.h>
#include <string.h>
#include "daos.h"
#include "daos_fs.h"


/** object struct that is instantiated for a DFS open object */
struct dfs_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** mode_t containing permissions & type */
	mode_t			mode;
	/** open access flags */
	int			flags;
	/** DAOS object ID of the parent of the object */
	daos_obj_id_t		parent_oid;
	/** entry name of the object in the parent */
	char			name[DFS_MAX_PATH + 1];
	/** Symlink value if object is a symbolic link */
	char			*value;
};

/** dfs struct that is instantiated for a mounted DFS namespace */
struct dfs {
	/** flag to indicate whether the dfs is mounted */
	bool			mounted;
	/** lock for threadsafety */
	pthread_mutex_t		lock;
	/** uid - inherited from pool. TODO - make this from container. */
	uid_t			uid;
	/** gid - inherited from pool. TODO - make this from container. */
	gid_t			gid;
	/** Access mode (RDONLY, RDWR) */
	int			amode;
	/** Open pool handle of the DFS */
	daos_handle_t		poh;
	/** Open container handle of the DFS */
	daos_handle_t		coh;
	/** Object ID reserved for this DFS (see oid_gen below) */
	daos_obj_id_t		oid;
	/** Open object handle of SB */
	daos_handle_t		super_oh;
	/** Root object info */
	dfs_obj_t		root;
	/** DFS container attributes (Default chunk size, oclass, etc.) */
	dfs_attr_t		attr;
	/** Optional Prefix to account for when resolving an absolute path */
	char			*prefix;
	daos_size_t		prefix_len;
};

struct dfs_entry {
	/** mode (permissions + entry type) */
	mode_t		mode;
	/** Object ID if not a symbolic link */
	daos_obj_id_t	oid;
	/* Time of last access */
	time_t		atime;
	/* Time of last modification */
	time_t		mtime;
	/* Time of last status change */
	time_t		ctime;
	/** chunk size of file */
	daos_size_t	chunk_size;
	/** Sym Link value */
	char		*value;
};



/*
 * OID generation for the dfs objects.
 *
 * The oid.lo uint64_t value will be allocated from the DAOS container using the
 * unique oid allocator. 1 oid at a time will be allocated for the dfs mount.
 * The oid.hi value has the high 32 bits reserved for DAOS (obj class, type,
 * etc.). The lower 32 bits will be used locally by the dfs mount point, and
 * hence discarded when the dfs is unmounted.
 */
int oid_gen(dfs_t *dfs, uint16_t oclass, bool file, daos_obj_id_t *oid);

void oid_cp(daos_obj_id_t *dst, daos_obj_id_t src);

int get_daos_obj_mode(int flags);

int check_name(const char *name);

int fetch_entry(daos_handle_t oh, daos_handle_t th, const char *name,
					bool *exists, struct dfs_entry *entry);

int insert_entry(daos_handle_t oh, daos_handle_t th, const char *name,
		struct dfs_entry entry);

#endif /* LSU_SRC_CLIENT_SEIS_DFS_HELPERS_H_ */
