/*
 * daos_seis.c
 *
 *  Created on: May 19, 2020
 *      Author: mirnamoawad
 */


#include "daos_seis.h"
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

#define INODE_AKEYS	7
#define INODE_AKEY_NAME	"DFS_INODE"

segy_root_obj_t *root_obj;

typedef struct seismic_entry seismic_entry_t;
struct stat *seismic_stat;

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
//static daos_handle_t poh;
//static daos_handle_t coh;

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

int insert_entry(daos_handle_t oh, daos_handle_t th, const char *name,
		struct dfs_entry entry){
		d_sg_list_t	sgl;
		d_iov_t		sg_iovs[INODE_AKEYS];
		daos_iod_t	iod;
		daos_recx_t	recx;
		daos_key_t	dkey;
		unsigned int	i;
		int		rc;
		float *temp_array = malloc(1000000*sizeof(float));


		d_iov_set(&dkey, (void *)name, strlen(name));
		d_iov_set(&iod.iod_name, INODE_AKEY_NAME, strlen(INODE_AKEY_NAME));

		iod.iod_nr	= 1;
		recx.rx_idx	= 0;
		recx.rx_nr	= sizeof(mode_t) + sizeof(time_t) * 3 +
					sizeof(daos_obj_id_t) + sizeof(daos_size_t) + (sizeof(float)*1000000);
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
		d_iov_set(&sg_iovs[i++], temp_array, sizeof(float)*1000000);

		sgl.sg_nr	= i;
		sgl.sg_nr_out	= 0;
		sgl.sg_iovs	= sg_iovs;

		rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
		if (rc) {
				D_ERROR("Failed to insert entry %s (%d)\n", name, rc);
				return daos_der2errno(rc);
			}



	return 0;
}

int root_obj_create(dfs_t *dfs, segy_root_obj_t *obj,daos_oclass_id_t cid, char *name, dfs_obj_t *parent){

	int		rc;
	int daos_mode;
	daos_handle_t		th = DAOS_TX_NONE;
    daos_oclass_id_t ocid= cid;
	struct dfs_entry	dfs_entry = {0};
	struct seismic_entry	seismic_entry = {0};


	/*Allocate object pointer */
	D_ALLOC_PTR(obj);
	if (obj == NULL)
		return ENOMEM;

	strncpy(obj->name, name, SEIS_MAX_PATH);
	obj->name[SEIS_MAX_PATH] = '\0';
	obj->mode = S_IWUSR | S_IRUSR;
	obj->flags = O_RDWR;
	if(parent==NULL)
		parent = &dfs->root;

//	oid_cp(&obj->cmp_oid, NULL);
//	oid_cp(&obj->shot_oid, NULL);
//	oid_cp(&obj->cdp_oid, NULL);

	/** Get new OID for root object */
	rc = oid_gen(dfs, cid, &obj->oid);
	if (rc != 0)
		return rc;

	daos_mode = get_daos_obj_mode(obj->flags);

	rc = daos_obj_open(dfs->coh, obj->oid, daos_mode, &obj->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}

/** insert segyRoot object created under dfs root */
	rc = insert_entry(parent->oh, th, obj->name, dfs_entry);

//	seismic_entry.oid = obj->oid;
//	seismic_entry.akey_name[0] = "sort_key";
//	seismic_entry.akey_name[1] = "Binary_File_header";
//	seismic_entry.akey_name[2] = "Ext_text_header";
//	seismic_entry.dkey_name = "File Header";
//	seismic_entry.sort_key = "CMP";
//	seismic_entry.bfh = "binary file header";
//	seismic_entry.etfh = "Extended Text file header";
//
//	rc = root_obj_update(obj->oh, th, seismic_entry);

	daos_obj_close(obj->oh, NULL);

	return rc;
}

int root_obj_update(daos_handle_t oh, daos_handle_t th,	struct seismic_entry entry){
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
		recx.rx_nr	= sizeof(entry.data);
		iod.iod_recxs	= &recx;
		iod.iod_type	= DAOS_IOD_ARRAY;
		iod.iod_size	= 1;
		i = 0;

		d_iov_set(&sg_iovs[i++], &entry.data, sizeof(entry.data));

		sgl.sg_nr	= i;
		sgl.sg_nr_out	= 0;
		sgl.sg_iovs	= sg_iovs;

		rc = daos_obj_update(oh, th, 0, &dkey, 1, &iod, &sgl, NULL);
		if (rc) {
				D_ERROR("Failed to insert entry %s (%d)\n", name, rc);
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

	daos_mode = get_daos_obj_mode(root_obj->flags);

	rc = daos_obj_open(dfs->coh, root_obj->oid, daos_mode, &root_obj->oh, NULL);
	if (rc) {
		D_ERROR("daos_obj_open() Failed (%d)\n", rc);
		return daos_der2errno(rc);
	}

	seismic_entry.oid = root_obj->oid;
	seismic_entry.dkey_name = dkey_name;
	seismic_entry.akey_name = akey_name;
	seismic_entry.data = databuf;
	seismic_entry.size = nbytes;

	rc = root_obj_update(root_obj->oh, th, seismic_entry);

	daos_obj_close(root_obj->oh, NULL);

	return rc;
}

int daos_seis_parse_segy(dfs_t *dfs, dfs_obj_t *parent, char *name, dfs_obj_t *segy_file){

	/* create pointer to segyroot object */
	daos_handle_t	th = DAOS_TX_NONE;
    daos_oclass_id_t cid = OC_SX;
    d_sg_list_t *sgl;
    char *temp_name;
    int		rc;

	rc = root_obj_create(dfs, root_obj, cid, name, parent);
	if (rc != 0)
		return rc;

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
	    int verbose;		/* echo every ...			*/
	    int vblock;		/* ... vblock traces with verbose=1	*/
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
	         if ( !(
	                 (format == 1)
	                 || (format == 2)
	                 || (format == 3)
	                 || (format == 5)
	                 || (format == 8)
	         ) ) {
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
	             err("user format XXXt");
	     }

	     daos_size_t size;
	     DAOS_FILE *daos_tape;

	     int error;
	     /* Open files - first the tape */
	         if ( tape[0] == '-' && tape[1] == '\0' ) daos_tape->file = NULL;
	         else{
	             daos_tape = open_dfs_file(tape, S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
	         }

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



		// save cmdbuf data under Text header key.
//		 check_error_code(daos_seis_root_update(get_dfs(), get_root(), DS_D_FILE_HEADER, DS_A_TEXT_HEADER, cmdbuf, EBCBYTES), "Updating Text File Header Key");

		size = read_dfs_file(daos_tape, (char *) &tapebh, BNYBYTES);


	    /* Convert from bytes to ints/shorts */
	    tapebhed_to_bhed(&tapebh, &bh);

	    /* if little endian machine, swap bytes in binary header */
	    if (swapbhed == 0) for (i = 0; i < BHED_NKEYS; ++i) swapbhval(&bh, i);

	    /* Override binary format value */
	   	over = 0;
//	    if (getparint("format", &format))	format_set = cwp_true;
	    if (((over!=0) && (format_set)))	bh.format = format;

	    /* Override application of trace weighting factor? */
	    /* Default no for floating point formats, yes for integer formats. */
	    trcwt = (bh.format == 1 || bh.format == 5) ? 0 : 1;

	    switch (bh.format) {
	        case 1:
	            if (verbose) warn("assuming IBM floating point input");
	            break;
	        case 2:
	            if (verbose) warn("assuming 4 byte integer input");
	            break;
	        case 3:
	            if (verbose) warn("assuming 2 byte integer input");
	            break;
	        case 5:
	            if (verbose) warn("assuming IEEE floating point input");
	            break;
	        case 8:
	            if (verbose) warn("assuming 1 byte integer input");
	            break;
	        default:
	            (over) ? warn("ignoring bh.format ... continue") :
	            err("format not SEGY standard (1, 2, 3, 5, or 8)");
	    }

	    /* Compute length of trace (can't use sizeof here!) */
	    ns = bh.hns; /* let user override */
	    if (!ns) err("samples/trace not set in binary header");
	    bh.hns = ns;
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
	//	 check_error_code(daos_seis_root_update(get_dfs(), get_root(), "Binary Header",
	//			 "binaryfileheader", cmdbuf, EBCBYTES), "Updating Text File Header Key");


	        nextended = *((short *) (((unsigned char *)&tapebh) + 304));

	    if (endian == 0) swap_short_2((short *) &nextended);
	    if (verbose) warn("Number of extended text headers: %d", nextended);
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
	        }

	    }


	return rc;
}
