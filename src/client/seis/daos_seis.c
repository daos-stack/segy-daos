/*
 * daos_seis.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */

#include <unistd.h>
#include "su.h"
#include "segy.h"
#include "tapesegy.h"
#include "tapebhdr.h"
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include "bheader.h"
#include "header.h"
#include <sys/stat.h>
#include <sys/xattr.h>
#include <daos/checksum.h>
#include <daos/common.h>
#include <daos/event.h>
#include <daos/container.h>
#include <daos/array.h>
#include <string.h>
#include "dfs_helper_api.h"
#include "daos_seis.h"


#define INODE_AKEYS	7
#define INODE_AKEY_NAME	"DFS_INODE"

#define GET_MACRO(_1,_2,_3,_4,NAME,...) NAME
#define warn(...) GET_MACRO(__VA_ARGS__, warn4, warn3, warn2, warn1)(__VA_ARGS__)
#define warn1(x) fprintf(stderr,x)
#define warn2(x,y) fprintf(stderr,x,y)
#define warn3(x,y,z) fprintf(stderr,x,y,z)
#define warn4(x,y,z,u) fprintf(stderr,x,y,z,u)

#define err(...) GET_MACRO(__VA_ARGS__, err4, err3, err2, err1)(__VA_ARGS__)
#define err1(x) fprintf(stderr,x);exit(0)
#define err2(x,y) fprintf(stderr,x,y);exit(0)
#define err3(x,y,z) fprintf(stderr,x,y,z);exit(0)
#define err4(x,y,z,u) fprintf(stderr,x,y,z,u);exit(0)


/* Subroutine prototypes */
static void ibm_to_float(int from[], int to[], int n, int endian, int verbose);
static void int_to_float(int from[], float to[], int n, int endian);
static void short_to_float(short from[], float to[], int n, int endian);
static void integer1_to_float(signed char from[], float to[], int n);
static void tapebhed_to_bhed(const tapebhed *tapebhptr, bhed *bhptr);
static void tapesegy_to_segy(const tapesegy *tapetrptr, segy *trptr);

static void ugethval(cwp_String type1, Value *valp1,
                     char type2, int ubyte,
                     char *tr, int endian, int conv, int verbose);


typedef struct seismic_entry seismic_entry_t;
typedef struct seis_gather seis_gather_t;
struct stat *seismic_stat;


struct seis_gather{
	/** number of traces under specific gather key */
	int number_of_traces;
	/** array of object ids under specific gather key*/
	daos_obj_id_t *oids;
	/** number of keys
	 * =1 if its shot gather
	 * =2 if its cmp or offset gather
	 */
	int nkeys;
	/** gather unique info */
	int keys[2];
};

/** object struct that is instantiated for SEGYROOT open object */
struct segy_root_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** mode_t containing permissions & type */
	mode_t			mode;
	/** open access flags */
	int			flags;
	/** DAOS object ID of the CMP object */
	daos_obj_id_t		cmp_oid;
	/** DAOS object ID of the SHOT object */
	daos_obj_id_t		shot_oid;
	/** DAOS object ID of the GATHER object */
	daos_obj_id_t		offset_oid;
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
	/** number of traces */
	int 	number_of_traces;
	/** number of extended text headers */
	int 	nextended;
};

/** object struct that is instantiated for a Seismic open object */
struct seis_obj {
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
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
	/** current sequence number */
	int sequence_number;
	/** number of gathers */
	int number_of_gathers;
	/**array of gathers */
	seis_gather_t *gathers;
};

/** object struct that is instantiated for a Seismic trace object */
struct trace_obj {
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
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
	/**trace header */
	segy *trace;
};

/*********************** self documentation **********************/
char *sdoc[] = {
        "									",
        " SEGYREAD - read an SEG-Y tape						",
            "									",
        NULL};


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

struct seismic_entry {
	char 		*dkey_name;

	char 		*akey_name;

	daos_obj_id_t	oid;

	char		*data;

	int		size;
};
#define MAX_OID_HI ((1UL << 32) - 1)

int daos_seis_root_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char* databuf, int nbytes);
int daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry);
/*
 * OID generation for the dfs objects.
 *
 * The oid.lo uint64_t value will be allocated from the DAOS container using the
 * unique oid allocator. 1 oid at a time will be allocated for the dfs mount.
 * The oid.hi value has the high 32 bits reserved for DAOS (obj class, type,
 * etc.). The lower 32 bits will be used locally by the dfs mount point, and
 * hence discarded when the dfs is unmounted.
 */
static int
oid_gen(dfs_t *dfs, daos_oclass_id_t oclass, daos_obj_id_t *oid)
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


	/** generate the daos object ID (set the DAOS owned bits) */
	daos_obj_generate_id(oid, feat, oclass, 0);

	return 0;
}

static inline void
oid_cp(daos_obj_id_t *dst, daos_obj_id_t src)
{
	dst->hi = src.hi;
	dst->lo = src.lo;
}

static inline int
get_daos_obj_mode(int flags)
{
	if ((flags & O_ACCMODE) == O_RDONLY)
		return DAOS_OO_RO;
	else if ((flags & O_ACCMODE) == O_RDWR ||
		 (flags & O_ACCMODE) == O_WRONLY)
		return DAOS_OO_RW;
	else
		return -1;
}

segy_root_obj_t* daos_seis_open_root(dfs_t *dfs, dfs_obj_t *root){
	segy_root_obj_t *root_obj;





	return root_obj;
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

//static int
//fetch_entry(daos_handle_t oh, daos_handle_t th, const char *dkey_name, const char *akey_name, bool *exists, struct seismic_entry *entry, int target)
//{
//	d_sg_list_t	sgl;
//	d_iov_t		sg_iovs[INODE_AKEYS];
//	daos_iod_t	iod;
//	daos_recx_t	recx;
//	char		*value = NULL;
//	daos_key_t	dkey;
//	unsigned int	i;
//	int		rc;
//
//	D_ASSERT(dkey_name);
//
//	/** TODO - not supported yet */
//	if (strcmp(dkey_name, ".") == 0)
//		D_ASSERT(0);
//
//	d_iov_set(&dkey, (void *)dkey_name, strlen(dkey_name));
//	d_iov_set(&iod.iod_name, akey_name, strlen(akey_name));
//	iod.iod_nr	= 1;
//	recx.rx_idx	= 0;
//	recx.rx_nr	= 3 * PATH_MAX + sizeof(daos_obj_id_t) + sizeof(int);
//	iod.iod_recxs	= &recx;
//	iod.iod_type	= DAOS_IOD_ARRAY;
//	iod.iod_size	= 1;
//	i = 0;
//
//	d_iov_set(&sg_iovs[i++], &entry->akey_name, sizeof(PATH_MAX));
//	d_iov_set(&sg_iovs[i++], &entry->oid, sizeof(daos_obj_id_t));
//	d_iov_set(&sg_iovs[i++], &entry->dkey_name, sizeof(PATH_MAX));
//	d_iov_set(&sg_iovs[i++], &entry->data, sizeof(PATH_MAX));
//	d_iov_set(&sg_iovs[i++], &entry->size, sizeof(int));
//
//	if (fetch_sym) {
//		D_ALLOC(value, PATH_MAX);
//		if (value == NULL)
//			return ENOMEM;
//
//		recx.rx_nr += PATH_MAX;
//		/** Set Akey for Symlink Value, will be empty if no symlink */
//		d_iov_set(&sg_iovs[i++], value, PATH_MAX);
//	}
//
//	sgl.sg_nr	= i;
//	sgl.sg_nr_out	= 0;
//	sgl.sg_iovs	= sg_iovs;
//
//	rc = daos_obj_fetch(oh, th, 0, &dkey, 1, &iod, &sgl, NULL, NULL);
//	if (rc) {
//		D_ERROR("Failed to fetch entry %s (%d)\n", name, rc);
//		D_GOTO(out, rc = daos_der2errno(rc));
//	}
//
//	if (fetch_sym && S_ISLNK(entry->mode)) {
//		if (sgl.sg_nr_out == i) {
//			size_t sym_len = sg_iovs[i-1].iov_len;
//
//			if (sym_len != 0) {
//				D_ASSERT(value);
//				D_STRNDUP(entry->value, value, PATH_MAX - 1);
//				if (entry->value == NULL)
//					D_GOTO(out, rc = ENOMEM);
//			}
//		}
//	}
//
//	if (sgl.sg_nr_out == 0)
//		*exists = false;
//	else
//		*exists = true;
//
//out:
//	if (fetch_sym)
//		D_FREE(value);
//	return rc;
//}

int daos_seis_th_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *data, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	th_seismic_entry = {0};

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

	th_seismic_entry.oid = root_obj->oid;
	th_seismic_entry.dkey_name = dkey_name;
	th_seismic_entry.akey_name = akey_name;
	th_seismic_entry.data = data;
	th_seismic_entry.size = nbytes;

	rc = daos_seis_obj_update(root_obj->oh, th, th_seismic_entry);
	if (rc != 0)
		{
			return rc;
		}
//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_root_obj_create(dfs_t *dfs, segy_root_obj_t **obj,daos_oclass_id_t cid, char *name, dfs_obj_t *parent){

	int		rc;
	int daos_mode;
	daos_handle_t		th = DAOS_TX_NONE;
    daos_oclass_id_t ocid= cid;
	struct dfs_entry	dfs_entry = {0};
	struct seismic_entry	seismic_entry = {0};


	/*Allocate object pointer */
	D_ALLOC_PTR(*obj);
	if (*obj == NULL)
		return ENOMEM;

	strncpy((*obj)->name, name, SEIS_MAX_PATH);
	(*obj)->name[SEIS_MAX_PATH] = '\0';
	(*obj)->mode = S_IFDIR | S_IWUSR | S_IRUSR;
	(*obj)->flags = O_RDWR;
	(*obj)->number_of_traces = 0;
	if(parent==NULL)
		parent = &dfs->root;

//	oid_cp(&obj->cmp_oid, NULL);
//	oid_cp(&obj->shot_oid, NULL);
//	oid_cp(&obj->cdp_oid, NULL);

	/** Get new OID for root object */
	rc = oid_gen(dfs, cid, &(*obj)->oid);
	if (rc != 0)
		return rc;

	daos_mode = get_daos_obj_mode((*obj)->flags);

	rc = daos_obj_open(dfs->coh, (*obj)->oid, daos_mode, &(*obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}

	dfs_entry.oid = (*obj)->oid;
	dfs_entry.mode = (*obj)->mode;
	dfs_entry.chunk_size = 0;
	dfs_entry.atime = dfs_entry.mtime = dfs_entry.ctime = time(NULL);

/** insert segyRoot object created under dfs root */
	rc = insert_entry(parent->oh, th, (*obj)->name, dfs_entry);

	return rc;
}

int daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry){
		d_sg_list_t	sgl;
		d_iov_t		sg_iovs;
		daos_iod_t	iod;
		daos_recx_t	recx;
		daos_key_t	dkey;
		unsigned int	i;
		int		rc;

		d_iov_set(&dkey, (void *)entry.dkey_name, strlen(entry.dkey_name));
		d_iov_set(&iod.iod_name, (void *)entry.akey_name, strlen(entry.akey_name));

		iod.iod_nr	= 1;
		recx.rx_idx	= 0;
		recx.rx_nr	= entry.size;
		iod.iod_recxs	= &recx;
		iod.iod_type	= DAOS_IOD_ARRAY;
		iod.iod_size	= 1;

		d_iov_set(&sg_iovs, entry.data, entry.size);

		sgl.sg_nr	= 1;
		sgl.sg_nr_out	= 0;
		sgl.sg_iovs	= &sg_iovs;

		rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
		if (rc) {
				D_ERROR("Failed to insert dkey: %s and akey: %s (%d)\n", entry.dkey_name, entry.akey_name, rc);
				return daos_der2errno(rc);
			}

//		i = 0;
//		d_iov_set(&sg_iovs[i], &entry.sort_key, strlen(entry.sort_key));
//		d_iov_set(&iods[i].iod_name, &entry.akey_name[0], strlen(entry.akey_name[0]));
//		i++;
//		d_iov_set(&sg_iovs[i], &entry.bfh, strlen(entry.bfh));
//		d_iov_set(&iods[i].iod_name, &entry.akey_name[1], strlen(entry.akey_name[1]));
//		i++;
//		d_iov_set(&sg_iovs[i], &entry.etfh, strlen(entry.etfh));
//		d_iov_set(&iods[i].iod_name, &entry.akey_name[2], strlen(entry.akey_name[2]));
//		i++;
//
//
//		for (i = 0; i < 3; i++) {
//			sgls[i].sg_nr		= 1;
//			sgls[i].sg_nr_out	= 0;
//			sgls[i].sg_iovs		= &sg_iovs[i];
//
//			iods[i].iod_nr		= 1;
//			iods[i].iod_size	= DAOS_REC_ANY;
//			iods[i].iod_recxs	= NULL;
//			iods[i].iod_type	= DAOS_IOD_SINGLE;
//		}


	return rc;
}

int daos_seis_root_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char* databuf, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	seismic_entry = {0};

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

	seismic_entry.oid = root_obj->oid;
	seismic_entry.dkey_name = dkey_name;
	seismic_entry.akey_name = akey_name;
	seismic_entry.data = databuf;
	seismic_entry.size = nbytes;

	rc = daos_seis_obj_update(root_obj->oh, th, seismic_entry);
	if (rc != 0)
	{
		return rc;
	}

//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_bh_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , bhed *bhdr, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	bh_seismic_entry = {0};

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

	bh_seismic_entry.oid = root_obj->oid;
	bh_seismic_entry.dkey_name = dkey_name;
	bh_seismic_entry.akey_name = akey_name;
	bh_seismic_entry.data = (char *)bhdr;
	bh_seismic_entry.size = nbytes;

	rc = daos_seis_obj_update(root_obj->oh, th, bh_seismic_entry);
	if (rc != 0)
	{
		return rc;
	}
//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_exth_update(dfs_t* dfs, segy_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *ebcbuf, int index, int nbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	exth_seismic_entry = {0};
	char akey_index[100];
	sprintf(akey_index, "%d",index);
	char akey_extended[200] = "";
	strcat(akey_extended, akey_name);
	strcat(akey_extended, akey_index);

//	daos_mode = get_daos_obj_mode(root_obj->flags);

//	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

	exth_seismic_entry.oid = root_obj->oid;
	exth_seismic_entry.dkey_name = dkey_name;
	exth_seismic_entry.akey_name = akey_extended;
	exth_seismic_entry.data = ebcbuf;
	exth_seismic_entry.size = nbytes;

	rc = daos_seis_obj_update(root_obj->oh, th, exth_seismic_entry);

//	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid, segy_root_obj_t *parent,
			seis_obj_t **shot_obj, seis_obj_t **cmp_obj, seis_obj_t **offset_obj){
	int		rc;
	int daos_mode;
	daos_handle_t		th = DAOS_TX_NONE;
    daos_oclass_id_t ocid= OC_SX;

	/*Allocate shot object pointer */
	D_ALLOC_PTR(*shot_obj);
	if (*shot_obj == NULL)
		return ENOMEM;
	strncpy((*shot_obj)->name, "shot_gather", SEIS_MAX_PATH);
	(*shot_obj)->name[SEIS_MAX_PATH] = '\0';
	(*shot_obj)->mode = S_IWUSR | S_IRUSR;
	(*shot_obj)->flags = O_RDWR;
	(*shot_obj)->sequence_number = 0;
	seis_gather_t *seis_gather= malloc(5000*sizeof(seis_gather_t));
	(*shot_obj)->gathers = seis_gather;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid, &(*shot_obj)->oid);
	if (rc != 0)
		return rc;
	oid_cp(&parent->shot_oid, (*shot_obj)->oid);

	daos_mode = get_daos_obj_mode((*shot_obj)->flags);

	rc = daos_obj_open(dfs->coh, (*shot_obj)->oid, daos_mode, &(*shot_obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_SHOT_GATHER,
			(char*)&(*shot_obj)->oid, sizeof((*shot_obj)->oid));

	/*Allocate object pointer */
	D_ALLOC_PTR(*cmp_obj);
	if (*cmp_obj == NULL)
		return ENOMEM;
	strncpy((*cmp_obj)->name, "cmp_gather", SEIS_MAX_PATH);
	(*cmp_obj)->name[SEIS_MAX_PATH] = '\0';
	(*cmp_obj)->mode = S_IWUSR | S_IRUSR;
	(*cmp_obj)->flags = O_RDWR;
	(*cmp_obj)->sequence_number = 0;
	(*cmp_obj)->gathers = seis_gather;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid, &(*cmp_obj)->oid);
	if (rc != 0)
		return rc;
	oid_cp(&parent->cmp_oid, (*cmp_obj)->oid);

	daos_mode = get_daos_obj_mode((*cmp_obj)->flags);

	rc = daos_obj_open(dfs->coh, (*cmp_obj)->oid, daos_mode, &(*cmp_obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_CMP_GATHER , (char*)&(*cmp_obj)->oid, sizeof((*cmp_obj)->oid));


	/*Allocate object pointer */
	D_ALLOC_PTR(*offset_obj);
	if (*offset_obj == NULL)
		return ENOMEM;
	strncpy((*offset_obj)->name, "offset_gather", SEIS_MAX_PATH);
	(*offset_obj)->name[SEIS_MAX_PATH] = '\0';
	(*offset_obj)->mode = S_IWUSR | S_IRUSR;
	(*offset_obj)->flags = O_RDWR;
	(*offset_obj)->sequence_number = 0;
	(*offset_obj)->gathers = seis_gather;

	/** Get new OID for shot object */
	rc = oid_gen(dfs, cid, &(*offset_obj)->oid);
	if (rc != 0)
		return rc;
	oid_cp(&parent->offset_oid, (*offset_obj)->oid);

	daos_mode = get_daos_obj_mode((*offset_obj)->flags);

	rc = daos_obj_open(dfs->coh, (*offset_obj)->oid, daos_mode, &(*offset_obj)->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}
	rc = daos_seis_root_update(dfs, parent, DS_D_SORTING_TYPES, DS_A_OFFSET_GATHER , (char*)&(*offset_obj)->oid, sizeof((*offset_obj)->oid));

//	daos_obj_close(shot_obj->oh, NULL);
//	daos_obj_close(cmp_obj->oh, NULL);
//	daos_obj_close(offset_obj->oh, NULL);

	return rc;
}

int daos_seis_trace_ids_array_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry){

}

//int daos_seis_shot_obj_update(dfs_t* dfs, seis_obj_t *shot_obj, seis_obj_t* tr_obj, int shot_id){
//
//	/** check first if akey (shot id) exists.
//	 * if yes then add the trace object id to array of ids.
//	 * if no then create a new dkey (shot n) and add the shot id and trace object id as previously mentioned.
//	 */
//
//		daos_handle_t	th = DAOS_TX_NONE;
//	    daos_oclass_id_t cid = OC_SX;
//	    d_sg_list_t *sgl;
//	    int		rc;
//	    int daos_mode;
//		struct seismic_entry	shot_entry = {0};
//		bool			exists;
//
//		char *dkey_name = "shot_";
//		char *dkey_index;
//		int i;
//
//		for(i=0; i < (shot_obj->sequence_number); i++){
//			sprintf(dkey_index, "%d",i);
//			strcat(dkey_name, dkey_index);
//			rc = fetch_entry(shot_obj->oh, th, dkey_name, "shot_id", &exists, shot_entry, shot_id);
//			/**shot_id already exists then just add the trace object_id to the array of ids & increment number of traces*/
//			if(exists){
//				shot_entry.oid = shot_obj->oid;
//				shot_entry.dkey_name = dkey_name;
//				shot_entry.akey_name = "Traces_ids";
//				sprintf(shot_entry.data, "%d",tr_obj->oid);
//				shot_entry.size = sizeof(tr_obj->oid);
//				rc = daos_seis_obj_update(shot_obj->oh, th, shot_entry);
//				/** fetch trace number value and increment it */
//				int number_of_traces;
//				rc = fetch_entry(shot_obj->oh, th, dkey_name, "number_of_traces", &exists, shot_entry);
//				number_of_traces = (int)shot_entry->data;
//				number_of_traces++;
//				shot_entry.oid = shot_obj->oid;
//				shot_entry.dkey_name = dkey_name;
//				shot_entry.akey_name = "number_of_traces";
//				sprintf(shot_entry.data, "%d",number_of_traces);
//				shot_entry.size = sizeof(number_of_traces);
//				rc = daos_seis_obj_update(shot_obj->oh, th, shot_entry);
//			}
//		}
//		/**shot_id doesn't exists then just add the trace object_id to the array of ids & increment number of traces*/
//		if(i==shot_obj->sequence_number && exists==false){
//			sprintf(dkey_index, "%d",shot_obj->sequence_number);
//			strcat(dkey_name, dkey_index);
//			shot_entry.oid = shot_obj->oid;
//			shot_entry.dkey_name = dkey_name;
//			shot_entry.akey_name = "shot_id";
//			sprintf(shot_entry.data, "%d",shot_id);
//			shot_entry.size = sizeof(shot_id);
//
//			rc = daos_seis_obj_update(shot_obj->oh, th, shot_entry);
//
//			shot_entry.oid = shot_obj->oid;
//			shot_entry.dkey_name = dkey_name;
//			shot_entry.akey_name = "number_of_traces";
//			sprintf(shot_entry.data, "%d",shot_id);
//			shot_entry.size = sizeof(shot_id);
//
//			rc = daos_seis_obj_update(shot_obj->oh, th, shot_entry);
//
//
//			shot_entry.oid = shot_obj->oid;
//			shot_entry.dkey_name = dkey_name;
//			shot_entry.akey_name = "Traces_ids";
//			sprintf(shot_entry.data, "%d",tr_obj->oid);
//			shot_entry.size = sizeof(tr_obj->oid);
//
//			rc = daos_seis_obj_update(shot_obj->oh, th, shot_entry);
//			shot_obj->sequence_number++;
//		}
//
//		return rc;
//}

int daos_seis_trh_update(dfs_t* dfs, trace_obj_t* tr_obj, segy *tr, int hdrbytes){
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    int		rc;
    int daos_mode;
	struct seismic_entry	tr_entry = {0};

//	daos_mode = get_daos_obj_mode(tr_obj->flags);
//
//	rc = daos_obj_open(dfs->coh, tr_obj->oid, daos_mode, &tr_obj->oh, NULL);
//	if (rc) {
//		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
//		return daos_der2errno(rc);
//	}

	tr_entry.oid = tr_obj->oid;
	tr_entry.dkey_name = DS_D_TRACE_HEADER;
	tr_entry.akey_name = DS_A_TRACE_HEADER;
	tr_entry.data = (char*)tr;
	tr_entry.size = hdrbytes;

	rc = daos_seis_obj_update(tr_obj->oh, th, tr_entry);
	if(rc!=0){
		printf("ERROR UPDATING TRACE header KEY----------------- \n");
	}

	int data_length = tr->ns;
	int offset = 0;
	int start=0;
	int end=199;
	char st_index[100];
	char end_index[100];
	char trace_data_dkey[200]= DS_D_TRACE_DATA;
	char trace_dkey[200] = "";

	while(data_length > 0){
		sprintf(st_index, "%d",start);
		sprintf(end_index, "%d",end);
		strcat(trace_dkey, DS_D_TRACE_DATA);
		strcat(trace_dkey, st_index);
		strcat(trace_dkey, "_");
		strcat(trace_dkey, end_index);

		tr_entry.oid = tr_obj->oid;
		tr_entry.dkey_name = trace_dkey;
		tr_entry.akey_name = DS_A_TRACE_DATA;
		tr_entry.data = (char*)((tr->data)+offset);
		tr_entry.size = min(200,data_length)*sizeof(float);

		rc = daos_seis_obj_update(tr_obj->oh, th, tr_entry);
		if(rc!=0){
			printf("ERROR UPDATING TRACE DATA KEY----------------- error = %d \n", rc);
		}

		data_length = data_length - 200;
		offset = offset + 200;
		start = end+1;
		end = start +199;
		if(end > data_length){
			end = data_length;
		}

	}
//	printf("TRACE UPDATE FUNCTION----------------- \n");


	/** update shot object by shot id */
//	rc = daos_seis_shot_obj_update(dfs, shot_obj, tr_obj, shot_id);

	/** update cmp object by cmp value */

	/** update offset object by offset value */

//	daos_obj_close(tr_obj->oh, NULL);

	return rc;
}

int daos_seis_tr_obj_create(dfs_t* dfs, trace_obj_t **trace_obj, int index, segy *trace, int nbytes){

	/** Create Trace Object*/
		int		rc;
		int daos_mode;
		daos_handle_t		th = DAOS_TX_NONE;
	    daos_oclass_id_t cid= OC_SX;
		char tr_index[50];
		char trace_name[200] = "Trace_obj_";

		/*Allocate object pointer */
		D_ALLOC_PTR(*trace_obj);
		if ((*trace_obj) == NULL)
			return ENOMEM;

		sprintf(tr_index, "%d",index);
		strcat(trace_name, tr_index);

		strncpy((*trace_obj)->name, trace_name, SEIS_MAX_PATH);
		(*trace_obj)->name[SEIS_MAX_PATH] = '\0';
		(*trace_obj)->mode = S_IFDIR | S_IWUSR | S_IRUSR;
		(*trace_obj)->flags = O_RDWR;
		(*trace_obj)->trace = trace;

		/** Get new OID for trace object */
		rc = oid_gen(dfs, cid, &(*trace_obj)->oid);
		if (rc != 0)
			return rc;

		daos_mode = get_daos_obj_mode((*trace_obj)->flags);

		rc = daos_obj_open(dfs->coh, (*trace_obj)->oid, daos_mode, &(*trace_obj)->oh, NULL);
		if (rc) {
			D_ERROR("daos_obj_open() Failed (%d)\n", rc);
			return daos_der2errno(rc);
		}

		struct seismic_entry	tr_entry = {0};
		rc = daos_seis_trh_update(dfs, *trace_obj,trace, 240);
		if(rc !=0){
			printf("ERROR updating trace object error number = %d  \n", rc);
			return rc;
		}
	return 0;
}

int prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid, char *dkey, char *akey,
			char *data,int size){
	entry->oid = oid;
	entry->dkey_name = dkey;
	entry->akey_name = akey;
	entry->data = data;
	entry->size = size;
	return 0;
}

int daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, segy *trace,
			seis_obj_t *shot_obj, seis_obj_t *cmp_obj, seis_obj_t *off_obj){
	int rc;
	daos_handle_t	th = DAOS_TX_NONE;
	int shot_id = trace->fldr;
	int s_x = trace->sx;
	int s_y = trace->sy;
	int r_x = trace->gx;
	int r_y = trace->gy;
	int cmp_x = (s_x + r_x)/2;
	int cmp_y = (s_y + r_y)/2;
	int off_x = (r_x - s_x)/2;
	int off_y = (r_y - s_y)/2;
	int shot_exists=0;
	int cmp_exists=0;
	int offset_exists=0;

	struct seismic_entry gather_entry = {0};

	for(int i=0; i< (shot_obj->sequence_number); i++){
		if(!shot_exists && ((shot_obj->gathers[i]).keys[0]) == shot_id){
			char temp[200]="";
			char temp_data[400]="";
			char shot_dkey_name[200] = "";

			strcat(shot_dkey_name,DS_D_SHOT);
			sprintf(temp, "%d", i);
			strcat(shot_dkey_name,temp);
			sprintf(temp_data, "%d", trace_obj->oid);
			shot_exists=1;

			prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_TRACE_OIDS,
					temp_data, sizeof(daos_obj_id_t));
			rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR UPDATING shot trace_OIDS array, error: %d", rc);
				return rc;
			}

			sprintf(temp_data, "%d", ((shot_obj->gathers[i]).number_of_traces)++);
			prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_NTRACES,
											temp_data, sizeof(int));
			rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR UPDATING shot number_of_traces key, error: %d", rc);
				return rc;
			}
			break;
		} else {
			shot_exists=0;
			continue;
		}
	}
	for(int i=0; i<(cmp_obj->sequence_number);i++){
		if(!cmp_exists && ((cmp_obj->gathers[i].keys[0]) == cmp_x) && ((cmp_obj->gathers[i].keys[1]) == cmp_y)){
				printf("HELLOOOO SECOND IF \n");
				char cmp_dkey_name[200] = "";
				char temp[200]="";
				char temp_data[400]="";
				strcat(cmp_dkey_name,DS_D_CMP);
				sprintf(temp, "%d", i);
				strcat(cmp_dkey_name,temp);
				sprintf(temp_data, "%d", trace_obj->oid);
				cmp_exists=1;

				prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_TRACE_OIDS,
						temp_data, sizeof(daos_obj_id_t));
				rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
				if(rc !=0){
					printf("ERROR UPDATING Cmp trace_OIDS array, error: %d", rc);
					return rc;
				}
				sprintf(temp_data, "%d", (cmp_obj->gathers[i].number_of_traces)++);

				prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_NTRACES,
												temp_data, sizeof(int));
				rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
				if(rc !=0){
					printf("ERROR UPDATING CMP number_of_traces key, error: %d", rc);
					return rc;
				}
				break;


			} else {
				cmp_exists=0;
				continue;
			}
	}
	for(int i=0; i<(off_obj->sequence_number);i++){

		if(!offset_exists && ((off_obj->gathers[i]).keys[0]) == off_x && ((off_obj->gathers[i]).keys[1]) == off_y){
				offset_exists=1;
				char temp[200]="";
				char temp_data[400]="";
				char off_dkey_name[200] = "";
				strcat(off_dkey_name,DS_D_OFFSET);
				sprintf(temp, "%d", i);
				strcat(off_dkey_name,temp);
				sprintf(temp_data, "%d", trace_obj->oid);

				prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_TRACE_OIDS,
						temp_data, sizeof(daos_obj_id_t));
				rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
				if(rc !=0){
					printf("ERROR UPDATING OFFSET trace_OIDS array, error: %d", rc);
					return rc;
				}

				sprintf(temp_data, "%d", ((off_obj->gathers[i]).number_of_traces)++);
				prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_NTRACES,
												temp_data, sizeof(int));
				rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
				if(rc !=0){
					printf("ERROR UPDATING number of traces key, error: %d", rc);
					return rc;
				}
				break;
			} else {
				offset_exists=0;
				continue;
			}
	}

	/** if shot id, cmp, and offset doesn't already exist */
	if(!shot_exists){
		seis_gather_t shot_gather_data = {0};
		char temp[200]="";
		char temp_data[400]="";
		shot_gather_data.oids = &(trace_obj->oid);
		shot_gather_data.number_of_traces = 1;
		shot_gather_data.nkeys = 1;
		shot_gather_data.keys[0] = shot_id;
		char shot_dkey_name[200] = "";

		strcat(shot_dkey_name,DS_D_SHOT);
		sprintf(temp, "%d", shot_obj->sequence_number);
		strcat(shot_dkey_name,temp);
		sprintf(temp_data, "%d", trace_obj->oid);

		prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_TRACE_OIDS,
										temp_data, sizeof(daos_obj_id_t));
		rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
		if(rc !=0){
			printf("ERROR adding shot array_of_traces key, error: %d", rc);
			return rc;
		}
		sprintf(temp_data, "%d", shot_id);

		prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_SHOT_ID,
							temp_data, sizeof(int));
		rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
		if(rc !=0){
			printf("ERROR adding shot shot_id key, error: %d", rc);
			return rc;
		}
		sprintf(temp_data, "%d", 1);

		prepare_seismic_entry(&gather_entry, shot_obj->oid, shot_dkey_name, DS_A_NTRACES,
										temp_data, sizeof(int));
		rc = daos_seis_obj_update(shot_obj->oh, th, gather_entry);
		if(rc !=0){
			printf("ERROR Adding shot number of traces key, error: %d", rc);
			return rc;
		}
		shot_obj->gathers[shot_obj->sequence_number]=shot_gather_data;
		shot_obj->sequence_number++;
		shot_obj->number_of_gathers++;
	}

		if(!cmp_exists){
			char cmp_dkey_name[200] = "";
			char temp[200]="";
			char temp_data[400]="";
			seis_gather_t cmp_gather_data= {0};
			cmp_gather_data.oids = &(trace_obj->oid);
			cmp_gather_data.number_of_traces=1;
			cmp_gather_data.nkeys=2;
			cmp_gather_data.keys[0] = cmp_x;
			cmp_gather_data.keys[1] = cmp_y;
			strcat(cmp_dkey_name,DS_D_CMP);
			sprintf(temp, "%d", cmp_obj->sequence_number);
			strcat(cmp_dkey_name,temp);

			sprintf(temp_data, "%d", trace_obj->oid);
			prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_TRACE_OIDS,
					temp_data, sizeof(daos_obj_id_t));

			rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING Cmp trace_OIDS array, error: %d", rc);
				return rc;
			}

			sprintf(temp_data, "%d", cmp_gather_data.keys[0]);
			char arr[200]="";
			sprintf(arr, "%d", cmp_gather_data.keys[1]);
			strcat(temp_data,arr);

			prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_CMP_VAL,
								temp_data, sizeof(int)*2);

			rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING CMP value key, error: %d", rc);
				return rc;
			}
			sprintf(temp_data, "%d", 1);
			prepare_seismic_entry(&gather_entry, cmp_obj->oid, cmp_dkey_name, DS_A_NTRACES,
											temp_data, sizeof(int));
			rc = daos_seis_obj_update(cmp_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING CMP number_of_traces key, error: %d", rc);
				return rc;
			}
			cmp_obj->gathers[cmp_obj->sequence_number]=cmp_gather_data;
			cmp_obj->sequence_number++;
			cmp_obj->number_of_gathers++;

		}

		if(!offset_exists){
			char off_dkey_name[200] = "";
			char temp[200]="";
			char temp_data[400]="";
			seis_gather_t off_gather_data= {0};
			off_gather_data.oids = &(trace_obj->oid);
			off_gather_data.number_of_traces=1;
			off_gather_data.nkeys=2;
			off_gather_data.keys[0] = off_x;
			off_gather_data.keys[1] = off_y;

			strcat(off_dkey_name,DS_D_OFFSET);
			sprintf(temp, "%d", off_obj->sequence_number);
			strcat(off_dkey_name,temp);
			sprintf(temp_data, "%d", trace_obj->oid);
			prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_TRACE_OIDS,
					temp_data, sizeof(daos_obj_id_t));
			rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING OFFSET trace_OIDS array, error: %d", rc);
				return rc;
			}
			sprintf(temp_data, "%d", off_gather_data.keys[0]);
			char arr[200]="";
			sprintf(arr, "%d", off_gather_data.keys[1]);
			strcat(temp_data,arr);

			prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_OFF_VAL,
								temp_data, sizeof(int)*2);
			rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING GATHER offset value key, error: %d", rc);
				return rc;
			}
			sprintf(temp_data, "%d", 1);
			prepare_seismic_entry(&gather_entry, off_obj->oid, off_dkey_name, DS_A_NTRACES,
											temp_data, sizeof(int));
			rc = daos_seis_obj_update(off_obj->oh, th, gather_entry);
			if(rc !=0){
				printf("ERROR ADDING number of traces key, error: %d", rc);
				return rc;
			}
			off_obj->gathers[off_obj->sequence_number]=off_gather_data;
			off_obj->sequence_number++;
			off_obj->number_of_gathers++;
		}
	return rc;
}

int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_root){

	/* create pointer to segyroot object */
	segy_root_obj_t *root_obj;
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    char *temp_name;
    int		rc;
	seis_obj_t *shot_obj;
	seis_obj_t *offset_obj;
	seis_obj_t *cmp_obj;
	DAOS_FILE *daos_tape = malloc(sizeof(DAOS_FILE));
	daos_tape->file = segy_root;
	daos_tape->offset = 0;


	rc = daos_seis_root_obj_create(dfs, &root_obj, cid, name, parent);
	if (rc != 0)
	{
		warn("FAILED TO create ROOT OBJECT");
		return rc;
	}



	rc = daos_seis_gather_obj_create(dfs,cid, root_obj, &shot_obj, &offset_obj, &cmp_obj);
	if (rc != 0)
	{
		warn("FAILED TO create gather OBJECT");
		return rc;
	}
	/* Globals */
	tapesegy tapetr;
	tapebhed tapebh;
	segy tr;
	bhed bh;


	  /* Declare variables that will be parsed from commandline */
	    /**********************************************************/
	    /* Required */
	    char *tape;		/* name of raw tape device	*/

	    int tapefd = 0;		/* file descriptor for tape	*/

	    FILE *tapefp = NULL;	/* file pointer for tape	*/
	    FILE *binaryfp;		/* file pointer for bfile	*/
	    FILE *headerfp;		/* file pointer for hfile	*/
	    FILE *xheaderfp;	/* file pointer for xfile	*/
	    FILE *pipefp;		/* file pointer for popen write */

	    size_t nsegy;		/* size of whole trace in bytes		*/
	    int i;			/* counter				*/
	    int itr;		/* current trace number			*/
	    int trmin;		/* first trace to read			*/
	    int trmax;		/* last trace to read			*/
	    int ns;			/* number of data samples		*/

	    int over;		/* flag for bhed.float override		*/
	    int format;		/* flag for to specify override format	*/
	    int result;
	    cwp_Bool format_set = cwp_false;
	    /* flag to see if new format is set	*/
	    int conv;		/* flag for data conversion		*/
	    int trcwt;		/* flag for trace weighting		*/
	    int verbose = 0;		/* echo every ...			*/
	    int vblock =50;		/* ... vblock traces with verbose=1	*/
	    int buff;		/* flag for buffered/unbuffered device	*/
	    int endian;		/* flag for big=1 or little=0 endian	*/
	    int swapdata;		/* flag for big=1 or little=0 endian	*/
	    int swapbhed;		/* flag for big=1 or little=0 endian	*/
	    int swaphdrs;		/* flag for big=1 or little=0 endian	*/
	    int errmax;		/* max consecutive tape io errors	*/
	    int errcount = 0;	/* counter for tape io errors		*/
	    cwp_Bool nsflag;	/* flag for error in tr.ns		*/

	    char *cmdbuf;	/* dd command buffer			*/
	    cmdbuf = (char*) malloc(BUFSIZ * sizeof(char));

	    char *ebcbuf;	/* ebcdic data buffer			*/
	    ebcbuf = (char*) malloc(EBCBYTES * sizeof(char));
	    int ebcdic=1;		/* ebcdic to ascii conversion flag	*/


	    cwp_String *key1;	/* output key(s)		*/
	    key1 = (cwp_String*) malloc(SU_NKEYS * sizeof(cwp_String));
	    cwp_String key2[SU_NKEYS];	/* first input key(s)		*/
	    cwp_String type1[SU_NKEYS];	/* array of types for key1	*/
	    char type2[SU_NKEYS];		/* array of types for key2	*/
	    int ubyte[SU_NKEYS];
	    int nkeys;			/* number of keys to be computed*/
	    int n;				/* counter of keys getparred	*/
	    int ikey;			/* loop counter of keys 	*/
	    int index1[SU_NKEYS];		/* array of indexes for key1 	*/
	    Value val1;			/* value of key1		*/

	    /* deal with number of extended text headers */
	    short nextended;


	    /* Set parameters */
	    trmin = 1;
	    trmax = INT_MAX;
		 union { short s; char c[2]; } testend;
		 testend.s = 1;
		 endian = (testend.c[0] == '\0') ? 1 : 0;

		swapdata=endian;
	    swapbhed=endian;
	    swaphdrs=endian;

		ebcdic=1;

	   	errmax = 0;
	   	buff = 1;

	     /* Override binary format value */
	   over = 0;
	//     if (getparint("format", &format))	format_set = cwp_true;
	     if (((over!=0) && (format_set))) {
	         bh.format = format;
	         if ( !((format == 1)
	                 || (format == 2)
	                 || (format == 3)
	                 || (format == 5)
	                 || (format == 8)) ) {
	             warn("Specified format=%d not supported", format);
	             warn("Assuming IBM floating point format, instead");
	         }
	     }

	     /* Override conversion of IBM floating point data? */
	 	conv = 1;

	     /* Get parameters */
	     /* get key1's */
	//     if ((n = countparval("remap")) != 0){
	//         nkeys = n;
	//         getparstringarray("remap", key1);
	//     } else { /* set default */
	//         nkeys = 0;
	//     }
	    nkeys = 0;

	//     /* get key2's */
	//     if ((n = countparval("byte")) != 0){
	//         if (n != nkeys)
	//             err("number of byte's and remap's must be equal!");
	//
	//         getparstringarray("byte", key2);
	//     }

	     for (ikey = 0; ikey < nkeys; ++ikey) {
	         /* get types and index values */
	         type1[ikey]  = hdtype(key1[ikey]);
	         index1[ikey] = getindex(key1[ikey]);
	     }

	     for (ikey = 0; ikey < nkeys; ++ikey) {
	    	 if (sscanf(key2[ikey],"%d%c", &ubyte[ikey], &type2[ikey]) != 2)
	    	 {
	             err("user format XXXt");
	    	 }
	     }

	     daos_size_t size;

	     int error;
/** NO need for this */
	     //	     /* Open files - first the tape */
//	         if ( tape[0] == '-' && tape[1] == '\0' ) daos_tape->file = NULL;
//	         else{
//	             daos_tape = open_dfs_file(tape, S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
//	         }

		 size = read_dfs_file(daos_tape, ebcbuf, EBCBYTES);


		    /* Open pipe to use dd to convert  ebcdic to ascii */
		    /* this command gives a file containing 3240 bytes on sun */
		    /* see top of Makefile.config for versions */
		    /* not sure why this breaks now; works in version 37 */
		    if (ebcdic==1){
		        sprintf(cmdbuf, "dd ibs=1 of=%s conv=ascii count=3200", "header");
		    } else {

		        sprintf(cmdbuf, "dd ibs=1 of=%s count=3200", "header");
		    }

		    pipefp = epopen(cmdbuf, "w");
		    /* Write ebcdic stream from buffer into pipe */
		    efwrite(ebcbuf, EBCBYTES, 1, pipefp);


		    rc =daos_seis_th_update(dfs, root_obj, DS_D_FILE_HEADER,
					 	 DS_A_TEXT_HEADER, ebcbuf, EBCBYTES);
			if (rc != 0)
			{
				warn("FAILED TO update text header in  ROOT OBJECT");
				return rc;
			}




	// save cmdbuf data under Text header key.
//		 check_error_code(daos_seis_root_update(get_dfs(), get_root(), DS_D_FILE_HEADER, DS_A_TEXT_HEADER, cmdbuf, EBCBYTES), "Updating Text File Header Key");

		size = read_dfs_file(daos_tape, (char *) &tapebh, BNYBYTES);


	    /* Convert from bytes to ints/shorts */
	    tapebhed_to_bhed(&tapebh, &bh);


	    /* if little endian machine, swap bytes in binary header */
	    if (swapbhed == 0) {
	    	for (i = 0; i < BHED_NKEYS; ++i) {
	    		swapbhval(&bh, i);
	    	}
	    }

	    /* Override binary format value */
	   	over = 0;
//	    if (getparint("format", &format))	format_set = cwp_true;
	    if (((over!=0) && (format_set)))
	    	{
	    	bh.format = format;
	    	}

	    /* Override application of trace weighting factor? */
	    /* Default no for floating point formats, yes for integer formats. */
	    trcwt = (bh.format == 1 || bh.format == 5) ? 0 : 1;


	    switch (bh.format) {
	        case 1:
	            if (verbose) {
	            	warn("assuming IBM floating point input");
	            }
	            break;
	        case 2:
	            if (verbose)
	            	{
	            	warn("assuming 4 byte integer input");
	            	}
	            break;
	        case 3:
	            if (verbose) {
	            	warn("assuming 2 byte integer input");
	            }
	            break;
	        case 5:
	            if (verbose)
	            	{
	            	warn("assuming IEEE floating point input");
	            	}
	            break;
	        case 8:
	            if (verbose)
	            	{
	            	warn("assuming 1 byte integer input");
	            	}
	            break;
	        default:
	        	if(over){
	        		warn("ignoring bh.format ... continue");
	        	}else {
	                err("format not SEGY standard (1, 2, 3, 5, or 8)");
	        	}

	    }

	    /* Compute length of trace (can't use sizeof here!) */
	    ns = bh.hns; /* let user override */
	    if (!ns)
	    	{
	    	err("samples/trace not set in binary header");
	    	}
	    bh.hns = ns;
//	    printf("ns = %d", ns);

	    switch (bh.format) {
	        case 8:
	            nsegy = ns + SEGY_HDRBYTES;
	            break;
	        case 3:
	            nsegy = ns * 2 + SEGY_HDRBYTES;
	            break;
	        case 1:
	        case 2:
	        case 5:
	        default:
	            nsegy = ns * 4 + SEGY_HDRBYTES;
	    }

	    //write data of binary header under binary header akey
	    rc = daos_seis_bh_update(dfs, root_obj, DS_D_FILE_HEADER,
	    			DS_A_BINARY_HEADER, &bh, BNYBYTES);
		if (rc != 0)
		{
			warn("FAILED TO update binary header in  ROOT OBJECT");
			return rc;
		}


	        nextended = *((short *) (((unsigned char *)&tapebh) + 304));

	    if (endian == 0)
	    	{
	    	swap_short_2((short *) &nextended);
	    	}
	    if (verbose)
	    	{
	    	warn("Number of extended text headers: %d", nextended);
	    	}

	    rc = daos_seis_root_update(dfs, root_obj,  DS_D_FILE_HEADER,	DS_A_NEXTENDED_HEADER,
	    			(char*)&nextended, sizeof(nextended));
		if (rc != 0)
		{
			warn("FAILED TO update ROOT OBJECT");
			return rc;
		}

	    if (nextended > 0) /* number of extended text headers > 0 */
	    {
	        /* need to deal with -1 nextended headers */
	        /* so test should actually be !=0, but ... */
	        for (i = 0; i < nextended; i++) {
	            /* cheat -- an extended text header is same size as
	             * EBCDIC header */
	            /* Read the bytes from the tape for one xhdr into the
	             * buffer */
				size = read_dfs_file(daos_tape, ebcbuf, EBCBYTES);
	// write the data in ebcbuf under extended text header key.
				rc = daos_seis_exth_update(dfs, root_obj, DS_D_FILE_HEADER,
					 DS_A_EXTENDED_HEADER, ebcbuf, i, EBCBYTES);
				if (rc != 0)
				{
					warn("FAILED TO update extended header in  ROOT OBJECT");
					return rc;
				}

	        }
	    }


	    /* Read the traces */
	    nsflag = cwp_false;
	    itr = 0;

	    while (itr < trmax) {
	        int nread;

	        size = read_dfs_file(daos_tape, (char *) &tapetr, nsegy);
	        nread = size;

	        if (!nread)   /* middle exit loop instead of mile-long while */
	            break;

	        /* Convert from bytes to ints/shorts */
	        tapesegy_to_segy(&tapetr, &tr);

	        /* If little endian machine, then swap bytes in trace header */
	        if (swaphdrs == 0)
	            for (i = 0; i < SEGY_NKEYS; ++i) swaphval(&tr, i);

	        /* Check tr.ns field */
	        if (!nsflag && ns != tr.ns) {
	        	int temp_itr = itr +1;
	            warn("discrepant tr.ns = %d with tape/user ns = %d\n\t... first noted on trace %d",
	                 tr.ns, ns, temp_itr);
	            nsflag = cwp_true;
	        }

	        /* loop over key fields and remap */
	        for (ikey = 0; ikey < nkeys; ++ikey) {

	            /* get header values */

	            ugethval(type1[ikey], &val1,
	                     type2[ikey], ubyte[ikey] - 1,
	                     (char*) &tapetr, endian, conv, verbose);
	            puthval(&tr, index1[ikey], &val1);
	        }
	        /* Are there different swapping instructions for the data */
	        /* Convert and write desired traces */
	        if (++itr >= trmin) {
	            /* Convert IBM floats to native floats */
	            if (conv) {
	                switch (bh.format) {
	                    case 1:
	                        /* Convert IBM floats to native floats */
	                        ibm_to_float((int *) tr.data,
	                                     (int *) tr.data, ns,
	                                     swapdata,verbose);
	                        break;
	                    case 2:
	                        /* Convert 4 byte integers to native floats */
	                        int_to_float((int *) tr.data,
	                                     (float *) tr.data, ns,
	                                     swapdata);
	                        break;
	                    case 3:
	                        /* Convert 2 byte integers to native floats */
	                        short_to_float((short *) tr.data,
	                                       (float *) tr.data, ns,
	                                       swapdata);
	                        break;
	                    case 5:
	                        /* IEEE floats.  Byte swap if necessary. */
	                        if (swapdata == 0)
	                            for (i = 0; i < ns ; ++i)
	                                swap_float_4(&tr.data[i]);
	                        break;
	                    case 8:
	                        /* Convert 1 byte integers to native floats */
	                        integer1_to_float((signed char *)tr.data,
	                                          (float *) tr.data, ns);
	                        break;
	                }

	                /* Apply trace weighting. */
	                if (trcwt && tr.trwf != 0) {
	                    float scale = pow(2.0, -tr.trwf);
	                    //int i;
	                    for (i = 0; i < ns; ++i) {
	                        tr.data[i] *= scale;
	                    }
	                }
	            } else if (conv == 0) {
	                /* don't convert, if not appropriate */

	                switch (bh.format) {
	                    case 1: /* swapdata=0 byte swapping */
	                    case 5:
	                        if (swapdata == 0)
	                            for (i = 0; i < ns ; ++i)
	                                swap_float_4(&tr.data[i]);
	                        break;
	                    case 2: /* convert longs to floats */
	                        /* SU has no provision for reading */
	                        /* data as longs */
	                        int_to_float((int *) tr.data,
	                                     (float *) tr.data, ns, endian);
	                        break;
	                    case 3: /* shorts are the SHORTPAC format */
	                        /* used by supack2 and suunpack2 */
	                        if (swapdata == 0)/* swapdata=0 byte swap */
	                            for (i = 0; i < ns ; ++i)
	                                swap_short_2((short *) &tr.data[i]);
	                        /* Set trace ID to SHORTPACK format */
	                        tr.trid = SHORTPACK;
	                        break;
	                    case 8: /* convert bytes to floats */
	                        /* SU has no provision for reading */
	                        /* data as bytes */
	                        integer1_to_float((signed char *)tr.data,
	                                          (float *) tr.data, ns);
	                        break;
	                }
	            }

	            /* Write the trace to disk */
	            tr.ns = ns;
//	            puttr(&tr);
	            trace_obj_t *trace_obj;
	            root_obj->number_of_traces++;

	            rc = daos_seis_tr_obj_create(dfs, &trace_obj, itr, &tr, 240);
	            if(rc !=0)
	            	{
						printf("ERROR creating and updating trace object, error number = %d  \n", rc);
						return rc;
	            	}
	            rc = daos_seis_tr_linking(dfs, &trace_obj, &tr, shot_obj, cmp_obj, offset_obj);
	            if(rc!=0)
	            	{
	            		printf("ERROR LINKING TRACE TO MULTIPLE GATHER OBJECTS, err number= %d \n",rc);
	            		return rc;
	            	}

	            /* Echo under verbose option */
			 if (verbose && (itr % vblock) == 0)
	            {
				 warn(" %d traces from tape", itr);
	            }

				daos_obj_close(trace_obj->oh, NULL);
	        }
	    }
	    rc = daos_seis_root_update(dfs, root_obj,  DS_D_FILE_HEADER, DS_A_NTRACES_HEADER,
	    			(char*)&(root_obj->number_of_traces), sizeof(root_obj->number_of_traces));
		if (rc != 0)
		{
			warn("FAILED TO update ROOT OBJECT");
			return rc;
		}


		daos_obj_close(root_obj->oh, NULL);
		daos_obj_close(shot_obj->oh, NULL);
		daos_obj_close(cmp_obj->oh, NULL);
		daos_obj_close(offset_obj->oh, NULL);

	    free(daos_tape);
	return rc;
}

static void ibm_to_float(int from[], int to[], int n, int endian, int verbose)
/***********************************************************************
ibm_to_float - convert between 32 bit IBM and IEEE floating numbers
************************************************************************
Input::
from		input vector
to		output vector, can be same as input vector
endian		byte order =0 little endian (DEC, PC's)
			    =1 other systems
*************************************************************************
Notes:
Up to 3 bits lost on IEEE -> IBM

Assumes sizeof(int) == sizeof(float) == 4

IBM -> IEEE may overflow or underflow, taken care of by
substituting large number or zero

*************************************************************************
Credits: SEP: Stewart A. Levin,  c.1995
*************************************************************************/

/* See if this fits the bill for your needs - Stew */
/* ibmflt.f -- translated by f2c (version 1995/10/25).
*/
/* Subroutine */
{
    /* Initialized data */

    static int first = 1;

    /* System generated locals */
    int i__1;
    int j,k;

    /* Local variables */
    int   *in;
    float *out;
    int eibm, i__, mhibm;
    static int m1[512];
    static float r1[512];

    unsigned int jj;
    union {
        float rrf;
        int iif;
        unsigned int uuf;
    } cvtmp;

    float r_infinity__;
    int et3e;

    if(endian == 0) {
        swab(from,to,n*sizeof(int));
        for(i__ = 0; i__<n; ++i__) {
            j = to[i__];
            k = j<<16;
            to[i__] = k+((j>>16)&65535);
        }
        in = to;
    } else {
        in = from;
    }
    /* Parameter adjustments */
    out = (float *) to;
    --out;
    --in;
    /* Function Body */

    if (first) {
        first = ! first;
        cvtmp.iif = 2139095039;
        r_infinity__ = cvtmp.rrf;
        for (i__ = 0; i__ <= 511; ++i__) {
            i__1 = i__ & 255;
            eibm = i__1 >> 1;
            mhibm = i__ & 1;
            et3e = (eibm << 2) - 130;
            if (et3e > 0 && et3e <= 255) {
                i__1 = et3e ^ (i__ & 255);
                m1[i__] = i__1 << 23;
                if (mhibm == 1) {
                    r1[i__] = 0.f;
                } else {
                    i__1 = et3e | (i__ & 256);
                    cvtmp.iif = i__1 << 23;
                    r1[i__] = -(cvtmp.rrf);
                }
            } else if (et3e <= 0) {
                m1[i__] = i__ << 23;
                r1[i__] = 0.f;
            } else {
                m1[i__] = i__ << 23;
                if (i__ < 256) {
                    r1[i__] = r_infinity__;
                } else {
                    r1[i__] = -r_infinity__;
                }
            }
/* L10: */
        }
    }

    for (i__ = 1; i__ <= n; ++i__) {
        cvtmp.iif = in[i__];
/* use 9 high bits for table lookup */
        jj = cvtmp.uuf>>23;
/* fix up exponent */
        cvtmp.iif = m1[jj] ^ cvtmp.iif;
/* fix up mantissa */
        out[i__] = cvtmp.rrf + r1[jj];
/* L20: */
    }
}

static void tapebhed_to_bhed(const tapebhed *tapebhptr, bhed *bhptr)
/****************************************************************************
tapebhed_to_bhed -- converts the seg-y standard 2 byte and 4 byte
	integer header fields to, respectively, the
	machine's short and int types.
*****************************************************************************
Input:
tapbhed		pointer to array of
*****************************************************************************
Notes:
The present implementation assumes that these types are actually the "right"
size (respectively 2 and 4 bytes), so this routine is only a placeholder for
the conversions that would be needed on a machine not using this convention.
*****************************************************************************
Author: CWP: Jack  K. Cohen, August 1994
****************************************************************************/

{
    register int i;
    Value val;

    /* convert binary header, field by field */
    for (i = 0; i < BHED_NKEYS; ++i) {
        gettapebhval(tapebhptr, i, &val);
        putbhval(bhptr, i, &val);
    }
}

static void tapesegy_to_segy(const tapesegy *tapetrptr, segy *trptr)
/****************************************************************************
tapesegy_to_segy -- converts the seg-y standard 2 byte and 4 byte
		    integer header fields to, respectively, the machine's
		    short and int types.
*****************************************************************************
Input:
tapetrptr	pointer to trace in "tapesegy" (SEG-Y on tape) format

Output:
trptr		pointer to trace in "segy" (SEG-Y as in	 SU) format
*****************************************************************************
Notes:
Also copies float data byte by byte.  The present implementation assumes that
the integer types are actually the "right" size (respectively 2 and 4 bytes),
so this routine is only a placeholder for the conversions that would be needed
on a machine not using this convention.	 The float data is preserved as
four byte fields and is later converted to internal floats by ibm_to_float
(which, in turn, makes additonal assumptions).
*****************************************************************************
Author: CWP:Jack K. Cohen,  August 1994
****************************************************************************/
{
    register int i;
    Value val;

    /* convert header trace header fields */
    for (i = 0; i < SEGY_NKEYS; ++i) {
        gettapehval(tapetrptr, i, &val);
        puthval(trptr, i, &val);
    }

    /* copy the optional portion */
    memcpy((char *)&(trptr->otrav) + 2, tapetrptr->unass, 60);

    /* copy data portion */
    memcpy(trptr->data, tapetrptr->data, 4 * SU_NFLTS);
}

static void int_to_float(int from[], float to[], int n, int endian)
/****************************************************************************
Author:	J.W. de Bruijn, May 1995
****************************************************************************/
{
    register int i;

    if (endian == 0) {
        for (i = 0; i < n; ++i) {
            swap_int_4(&from[i]);
            to[i] = (float) from[i];
        }
    } else {
        for (i = 0; i < n; ++i) {
            to[i] = (float) from[i];
        }
    }
}

static void short_to_float(short from[], float to[], int n, int endian)
/****************************************************************************
short_to_float - type conversion for additional SEG-Y formats
*****************************************************************************
Author: Delft: J.W. de Bruijn, May 1995
Modified by: Baltic Sea Reasearch Institute: Toralf Foerster, March 1997
****************************************************************************/
{
    register int i;

    if (endian == 0) {
        for (i = n - 1; i >= 0 ; --i) {
            swap_short_2(&from[i]);
            to[i] = (float) from[i];
        }
    } else {
        for (i = n - 1; i >= 0 ; --i)
            to[i] = (float) from[i];
    }
}

static void integer1_to_float(signed char from[], float to[], int n)
/****************************************************************************
integer1_to_float - type conversion for additional SEG-Y formats
*****************************************************************************
Author: John Stockwell,  2005
****************************************************************************/
{
    while (n--) {
        to[n] = from[n];
    }
}

void ugethval(cwp_String type1, Value *valp1,
              char type2, int ubyte,
              char *ptr2, int endian, int conv, int verbose)
{	double dval1 = 0;
    char   c = 0;
    short  s = 0;
    int    l = 0;
    float  f = 0.0;
    char	*ptr1;

#if 0
    fprintf(stderr, "start ugethval %d %c\n", ubyte, type2);
#endif

    switch (type2) {
        case 'b':
            ptr1 = (char*) &c;
            ptr1[0] = ptr2[ubyte];
            dval1 = (double) c;
            break;
        case 's':
            ptr1 = (char*) &s;
            ptr1[0] = ptr2[ubyte];
            ptr1[1] = ptr2[ubyte+1];
            if (endian == 0)
                swap_short_2(&s);
            dval1 = (double) s;
            break;
        case 'l':
            ptr1 = (char*) &l;
            ptr1[0] = ptr2[ubyte];
            ptr1[1] = ptr2[ubyte+1];
            ptr1[2] = ptr2[ubyte+2];
            ptr1[3] = ptr2[ubyte+3];
            if (endian == 0)
/* segyread.c:903: warning: dereferencing type-punned pointer will break strict-aliasing rules */
/* 		   swap_long_4((long *)&l); */
/* note: long is 64-bits on 64-bit machine! */
                swap_int_4((int *)&l);
            dval1 = (double) l;
            break;
        case 'f':
            ptr1 = (char*) &f;
            ptr1[0] = ptr2[ubyte];
            ptr1[1] = ptr2[ubyte+1];
            ptr1[2] = ptr2[ubyte+2];
            ptr1[3] = ptr2[ubyte+3];
            if (conv)
/* get this message twice */
/* segyread.c:913: warning: dereferencing type-punned pointer will break strict-aliasing rules */
/* 		   ibm_to_float((int*) &f, (int*) &f, 1, endian, verbose); */
            {
                memcpy (&l, &f, 4);
                ibm_to_float(&l, &l, 1, endian, verbose);
            }
            else if (conv == 0 && endian == 0)
                swap_float_4(&f);
            dval1 = (double) f;
            break;
        default:
            err("unknown type %s", type2);
            break;
    }

#if 0
    fprintf(stderr, "value %lf\n", dval1);
#endif

    switch (*type1) {
        case 's':
            err("can't change char header word");
            break;
        case 'h':
            valp1->h = (short) dval1;
            break;
        case 'u':
            valp1->u = (unsigned short) dval1;
            break;
        case 'l':
            valp1->l = (long) dval1;
            break;
        case 'v':
            valp1->v = (unsigned long) dval1;
            break;
        case 'i':
            valp1->i = (int) dval1;
            break;
        case 'p':
            valp1->p = (unsigned int) dval1;
            break;
        case 'f':
            valp1->f = (float) dval1;
            break;
        case 'd':
            valp1->d = (double) dval1;
            break;
        default:
            err("unknown type %s", type1);
            break;
    }
}




