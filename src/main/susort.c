/* Copyright (c) Colorado School of Mines, 2011.*/
/* All rights reserved.                       */

/* SUSORT: $Revision: 1.28 $ ; $Date: 2011/11/12 00:44:21 $	*/

#include "su.h"
#include "segy.h"
#include <signal.h>
#include "dfs_helper_api.h"
/*********************** self documentation **********************/
char *sdoc[] = {
" 								",
" SUSORT - sort on any segy header keywords			",
" 								",
" susort <stdin pool=uuid container=uuid svc=r0:r1:r2 >stdout [[+-]key1 [+-]key2 ...]			",
" 								",
" Required parameters:							",
" pool=			pool uuid to connect		                ",
" container=		container uuid to connect		        ",
" svc=			service ranklist of pool seperated by :		",
" 								",
" Susort supports any number of (secondary) keys with either	",
" ascending (+, the default) or descending (-) directions for 	",
" each.  The default sort key is cdp.				",
" 								",
" Note:	Only the following types of input/output are supported	",
"	Disk input --> any output				",
"	Pipe input --> Disk output				",
" 								",
" Caveat:  On some Linux systems Pipe input and or output often ",
"		fails						",
"	Disk input ---> Disk output is recommended		",
" 								",
" Note: If the the CWP_TMPDIR environment variable is set use	",
"	its value for the path; else use tmpfile()		",
" 								",
" Example:							",
" To sort traces by cdp gather and within each gather		",
" by offset with both sorts in ascending order:			",
" 								",
" 	susort <INDATA >OUTDATA cdp offset			",
" 								",
" Caveat: In the case of Pipe input a temporary file is made	",
"	to hold the ENTIRE data set.  This temporary is		",
"	either an actual disk file (usually in /tmp) or in some	",
"	implementations, a memory buffer.  It is left to the	",
"	user to be SENSIBLE about how big a file to pipe into	",
"	susort relative to the user's computer.			",
" 								",
NULL};

/* Credits:
 *	SEP: Einar Kjartansson , Stew Levin
 *	CWP: Shuki Ronen,  Jack K. Cohen
 *
 * Caveats:
 *	Since the algorithm depends on sign reversal of the key value
 *	to obtain a descending sort, the most significant figure may
 *	be lost for unsigned data types.  The old SEP support for tape
 *	input was removed in version 1.16---version 1.15 is in the
 *	Portability directory for those who may want to input SU data
 *	stored on tape.
 *
 * Trace header fields modified: tracl, tracr
 */
/**************** end self doc ***********************************/


#define NTRSTEP	1024	/* realloc() icrement measured in traces */
#define ENABLE_DFS 1

segy tr;
static int nkey;	/* number of keys to sort on	*/
static cwp_String type;	/* header key types		*/

/* Prototypes */
Value negval(cwp_String typee, Value val);   /* reverse sign of value	*/
int cmp_list(const void *a, const void *b);
               				/* qsort comparison function	*/
static void closefiles(void);		/* signal handler		*/

/* Globals (so can trap signal) defining temporary disk files */
char tracefile[BUFSIZ];	/* filename for trace storage file	*/
FILE *tracefp;		/* fp for trace storage file		*/
DAOS_FILE *daos_tmp;


int startsWith(const char *pre, const char *str) {
    size_t lenpre = strlen(pre), lenstr = strlen(str);
    return lenstr < lenpre ? 0 : memcmp(pre, str, lenpre) == 0;
}


int
main(int argc, char **argv)
{
	static Value *val_list;	/* a list of the key values for each    */
				/* trace with each group headed by the	*/
				/* trace number of that trace		*/
	static int *index;	/* header key indices			*/
  	static cwp_Bool *up;	/* sort direction (+ = up = ascending)	*/
	register Value *vptr;	/* location pointer for val_list	*/
	int ngroup;		/* size of unit in val_list (nkey + 1)	*/
	int nv;			/* number of groups in val_list		*/
	size_t nvsize;		/* size of group in val_list		*/
  	Value val;		/* value of a keyword			*/
	long ntr;		/* number of traces from gettr		*/
	long itr;		/* index of trace (0,1,2,...)		*/
	FileType ftypein;	/* filetype of stdin			*/
	FileType ftypeout;	/* filetype of stdout			*/
	int nsegy;		/* number of bytes on the trace		*/
	cwp_Bool ispipe;	/* ftypein == PIPE ?			*/
	cwp_Bool isdisk;	/* ftypein == DISK ?			*/
	char *tmpdir;		/* directory path for tmp files		*/
	cwp_Bool istmpdir=cwp_false;/* true for user given path		*/

    char *pool_id;  /* string of the pool uuid to connect to */
    char *container_id; /*string of the container uuid to connect to */
    char *svc_list;		/*string of the service rank list to connect to */

    daos_size_t size;
    DAOS_FILE *daos_out_file;
    int is_out_file;
    char file_name[2048];

    /* Initialize */
	initargs(argc, argv);
	requestdoc(1);

    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    init_dfs_api(pool_id, svc_list, container_id, 0, 0);

    /* Look for user-supplied tmpdir form environment */
	if (!(tmpdir = getenv("CWP_TMPDIR"))) tmpdir="";
	if (!STREQ(tmpdir, "") && access(tmpdir, WRITE_OK))
		err("you can't write in %s (or it doesn't exist)", tmpdir);


	/* The algorithm requires stdin to be rewound and */
	/* random access to either stdin or stdout.    */
	ftypein = filestat(STDIN);
	ftypeout = filestat(STDOUT);
	isdisk = (ftypein == DISK) ? cwp_true : cwp_false;
	ispipe = (ftypein == PIPE || ftypein == FIFO) ? cwp_true : cwp_false;
	if (ispipe && ftypeout != DISK)
		err("OK IO types are: DISK->ANYTHING, PIPE->DISK");

    if (DISK == filestat(fileno(stdout))) {
        get_file_name(stdout, file_name);
        is_out_file = 1;
        daos_out_file = open_dfs_file(file_name, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
    } else {
        is_out_file = 0;
    }

        /* If pipe, prepare temporary file to hold data */
	if (ispipe) {
		if (STREQ(tmpdir,"")) {
		    if (ENABLE_DFS) {
		        char buffer[512]="";
                strcpy(tracefile, temporary_filename(buffer));
                daos_tmp = open_dfs_file(tracefile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
                istmpdir=cwp_true;
            } else {
                tracefp = etmpfile();
		    }
		} else {	/* user-supplied tmpdir from environment */
			char *directory;
            directory = (char*) malloc(BUFSIZ * sizeof(char));
            strcpy(directory, tmpdir);
			strcpy(tracefile, temporary_filename(directory));
			/* Handle user interrupts */
			signal(SIGINT, (void (*) (int)) closefiles);
			signal(SIGQUIT, (void (*) (int)) closefiles);
			signal(SIGHUP,  (void (*) (int)) closefiles);
			signal(SIGTERM, (void (*) (int)) closefiles);
			if(ENABLE_DFS){
                daos_tmp = open_dfs_file(tracefile, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
            } else{
                tracefp = efopen(tracefile, "w+");
			}
			istmpdir=cwp_true;
		}
	}

	/* Set number of sort keys */
	if (argc == 4) nkey = 1; /* no explicit keys: default cdp key */
	else  nkey = argc - 4;   /* one or more explicit keys */


	/* Allocate lists for key indices, sort directions and key types */
	index = ealloc1int(nkey);
  	up = (cwp_Bool *) ealloc1(nkey, sizeof(cwp_Bool));
	type = (char *) ealloc1(nkey, sizeof(char));


	/* Initialize index, type and up */
	if (argc == 4) {
		index[0] = getindex("cdp");
		up[0] = cwp_true;
		type[0] = 'l';
	} else {
		register int i;
		for (i = 0; i < nkey; ++i) {
		    ++argv;
		    while(startsWith("pool=", *argv) || startsWith("container=", *argv) || startsWith("svc=", *argv)){
		        ++argv;
		    }
            switch (**argv) { /* sign char of next arg */
			case '+':
				up[i] = cwp_true;
				++*argv;   /* discard sign char in arg */
			break;
			case '-':
				up[i] = cwp_false;
				++*argv;   /* discard sign char in arg */
			break;
			default:
				up[i] = cwp_true;
			break;
			}
			index[i] = getindex(*argv);
			type[i] = hdtype(*argv)[0]; /* need single char */
		}
	}


	/* Allocate list of trace numbers + key values */
	ngroup = nkey + 1;
	nvsize = ngroup*sizeof(Value);
	nv = NTRSTEP;  /* guess at required number */
	val_list = (Value *) ealloc1(nv, nvsize);

	vptr = val_list;


	/* Run through traces once to collect header values */
	ntr = 0;
	if (!(nsegy = gettr(&tr)))  err("can't get first trace");

	do {
		itr = ntr++;
		/* realloc if out of room */
		if (0 == (ntr % NTRSTEP)) {
			nv += NTRSTEP;
			val_list = (Value *) erealloc(val_list, nv*nvsize);
			vptr = val_list + itr * ngroup;
		}

		/* enter trace index in list and then key values */
		vptr++->l = itr;	/* insert number and advance */
		{ register int i;
		  for (i = 0; i < nkey; ++i) {
			gethval(&tr, index[i], &val);
			*vptr++ = up[i] ? val : negval(type + i, val);
				/* negative values give reverse sort */
		  }
		}
		if (ispipe){
		    if(ENABLE_DFS){
                size = write_dfs_file(daos_tmp, (char *)&tr, nsegy);
		    } else {
		        efwrite((char *)&tr, 1, nsegy, tracefp);
		    }
		}
	} while (gettr(&tr));

	if(ispipe){
	    if(ENABLE_DFS){
            seek_daos_file(daos_tmp,0);
        } else {
            rewind(tracefp);
        }
	} else {
	    if(ENABLE_DFS){
            seek_daos_file(gettr_daos_file(),0);
        } else {
            /* disk */	  rewind(stdin);
        }
	}


	/* Sort the headers values */
	qsort(val_list, ntr, nvsize, cmp_list);

	if (isdisk) {
		/* run through sorted list and write output sequentially */
		register int i;
		for (i = 0; i < ntr; ++i) {
			itr = val_list[i*ngroup].l;
			gettra(&tr, ((int) itr));
			tr.tracl = tr.tracr = i + 1;
			puttr(&tr);
		}
	} else /* pipe */ {
		/* invert permutation and read input sequentially */
		register int i;
		for (i = 0; i < ntr; ++i) {
			itr = val_list[i*ngroup].l;
			val_list[itr*ngroup + 1].l = i;
	        }
		for (i = 0; i < ntr; ++i) {
			itr = val_list[i*ngroup + 1].l;
            if(ENABLE_DFS){
                read_dfs_file(daos_tmp, (char *) &tr, nsegy);
            } else {
                efread(&tr, 1, nsegy, tracefp);
            }
			tr.tracl = tr.tracr = (int) (itr + 1);
			if(ENABLE_DFS && is_out_file){
                seek_daos_file(daos_out_file,(((off_t) itr) * ((off_t) nsegy)));
                write_dfs_file(daos_out_file,(char *) &tr,nsegy);
			} else {
                efseeko(stdout, ((off_t) itr)*((off_t) nsegy), SEEK_SET);
                efwrite(&tr, 1, nsegy, stdout);
			}
		}
	}

	/* Clean up */
	if (ispipe) {
        if(ENABLE_DFS){
            close_dfs_file(daos_tmp);
            if(istmpdir) remove_dfs_file(tracefile);
        } else {
            efclose(tracefp);
            if (istmpdir) eremove(tracefile);
        }
	}

	fini_dfs_api();
	return(CWP_Exit());
}


/* Comparison routine for qsort */
int cmp_list(const void *a, const void *b)
{
	register int i;
	Value va, vb;
	register const Value *pa, *pb;
	int compare;

	pa = (Value *) a;
	pb = (Value *) b;

	/* Can order as soon as any components are unequal */
	for (i = 0; i < nkey; ++i) {
		va = *++pa; vb = *++pb; /* advance and dereference */
		if ((compare = valcmp(type + i, va, vb)))
			return compare;
        }
        return 0;
}


/* Reverse sign of value */
Value negval(cwp_String typee, Value val)
{
	switch (*typee) {
	case 'h':
		val.h = -val.h;
	break;
	case 'u': 
		val.u = USHRT_MAX -val.u;
	break;
	case 'l':
		val.l = -val.l;
	break;
	case 'v':
		val.v = ULONG_MAX -val.v;
	break;
	case 'i':
		val.i = -val.i;
	break;
	case 'p':
		val.p = (int) -val.p;
	break;
	case 'f':
		val.f = -val.f;
	break;
	case 'd':
		val.d = -val.d;
	break;
	default: err("%d: mysterious type %s", __LINE__, typee);
	}

	return val;
}


/* for graceful interrupt termination */
static void closefiles(void)
{
    if(ENABLE_DFS){
       close_dfs_file(daos_tmp);
       remove_dfs_file(tracefile);
    } else {
        efclose(tracefp);
        eremove(tracefile);
    }
    exit(EXIT_FAILURE);
}
