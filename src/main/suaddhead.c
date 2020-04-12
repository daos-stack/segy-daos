/* Copyright (c) Colorado School of Mines, 2011.*/
/* All rights reserved.                       */

/* SUADDHEAD: $Revision: 1.21 $ ; $Date: 2011/11/16 22:10:29 $		*/

#include "su.h"
#include "segy.h"
#include "header.h"
#include "dfs_helper_api.h"

/*********************** self documentation ******************************/
char *sdoc[] = {
" 									",
" SUADDHEAD - put headers on bare traces and set the tracl and ns fields",
" 									",
" suaddhead <stdin >stdout ns= ftn=0					",
" 									",
" Required parameter:							",
" 	ns=the number of samples per trace				",
"   pool=			pool uuid to connect		                ",
"   container=		container uuid to connect		        ",
"   svc=			service ranklist of pool seperated by :		",
" 									",
" Optional parameter:							",
#ifdef SU_LINE_HEADER
"	head=           file to read headers in				",
"                       not supplied --  will generate headers 		",
"                       given        --  will read in headers and attach",
"                                        floating point arrays to form 	",
"                                        traces 			", 
"                       (head can be created via sustrip program)	",
#endif
" 	ftn=0		Fortran flag					",
" 			0 = data written unformatted from C		",
" 			1 = data written unformatted from Fortran	",
"       tsort=3         trace sorting code:				",
"                                1 = as recorded (no sorting)		",
"                                2 = CDP ensemble			",
"                                3 = single fold continuous profile	",
"                                4 = horizontally stacked		", 
"       ntrpr=1         number of data traces per record		",
"                       if tsort=2, this is the number of traces per cdp", 
" 									",
" Trace header fields set: ns, tracl					",
" Use sushw/suchw to set other needed fields.				",
" 									",
" Caution: An incorrect ns field will munge subsequent processing.	",
" Note:    n1 and nt are acceptable aliases for ns.			",
" 									",
" Example:								",
" suaddhead ns=1024 <bare_traces | sushw key=dt a=4000 >segy_traces	",
" 									",
" This command line adds headers with ns=1024 samples.  The second part	",
" of the pipe sets the trace header field dt to 4 ms.	See also the	",
" selfdocs of related programs  sustrip and supaste.			",
" See:   sudoc supaste							",
" Related Programs:  supaste, sustrip 					",
NULL};
/**************** end self doc *******************************************/

/* Credits:
 *	SEP: Einar Kjartansson   c. 1985
 *	CWP: Jack K. Cohen      April 1990
 *      UNOCAL: Zhiming Li	add ascii and binary headers
 */

#define ENABLE_DFS 1

extern unsigned char su_text_hdr[3200];
extern bhed su_binary_hdr;

segy tr;

int
main(int argc, char **argv)
{
	int ns;			/* number of samples			*/
	int ftn;		/* ftn=1 for Fortran			*/
	char junk[ISIZE];	/* to discard ftn junk  		*/
	cwp_Bool isreading=cwp_true;    /* true/false flag for while    */

	int ihead=0;		/* counter */
	int iread=0;		/* counter */
	int tsort, ntrpr;	/* Unocal header fields */
#ifdef SU_LINE_HEADER
	cwp_String head;	/* name of file holding headers         */
	FILE *infp=stdin, *outfp=stdout; /* input and output files 	*/
#endif
	FILE *headfp=NULL;	/* . file pointer for pointers		*/
	DAOS_FILE *daos_headfp = NULL; /*  daos file pointer for pointers     */ 
	char *pool_id;  /* string of the pool uuid to connect to */
    char *container_id; /*string of the container uuid to connect to */
    char *svc_list;		/*string of the service rank list to connect to */
    char file_name[2048];
    DAOS_FILE *daos_in_file;
    int is_file;

	/* Initialize */
	initargs(argc, argv);
	requestdoc(1);

    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    init_dfs_api(pool_id, svc_list, container_id, 0, 1);
	/* Get parameters */
	if (!getparint("n1", &ns)
	 && !getparint("nt", &ns)
	 && !getparint("ns", &ns))  err("must specify ns=");
	if (!getparint("ftn", &ftn))	ftn = 0;
	if (ftn != 0 && ftn != 1)	err("ftn=%d must be 0 or 1", ftn);
	if (!getparint("ntrpr", &ntrpr)) ntrpr = 1;
	if (!getparint("tsort", &tsort)) tsort = 3;

#ifdef SU_LINE_HEADER

	if (!getparstring("head"  , &head)) {
		ihead = 0;

	} else {
		ihead = 1;
		if (ENABLE_DFS) {
			daos_headfp = open_dfs_file(head, S_IFREG | S_IWUSR | S_IRUSR, 'r', 0);
		} else {
			if( !(headfp=efopen(head, "r")) ){
                err( "unable to open header file " );
            }	
		}     
	}
        checkpars();

	/* create id headers */
	if(ihead==0) {
		su_binary_hdr.format = 1;
		su_binary_hdr.ntrpr = ntrpr;
		su_binary_hdr.tsort = tsort;
		su_binary_hdr.fold = ntrpr;
	} else {
		if (ENABLE_DFS) {
			dfsfgethdr(daos_headfp, &su_text_hdr, &su_binary_hdr);
		} else {
			fgethdr(headfp,&su_text_hdr,&su_binary_hdr);
		}
	}

	su_binary_hdr.hns = ns;

#endif
	
	if (DISK == filestat(fileno(stdin))) {
        get_file_name(stdin, file_name);
        is_file = 1;
        daos_in_file = open_dfs_file(file_name, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
    } else {
        is_file = 0;
    }

	while (isreading==cwp_true) {
		static int tracl = 0;	/* one-based trace number */

		/* If Fortran data, read past the record size bytes */
		if (ENABLE_DFS && is_file) {
			if (ftn) read_dfs_file(daos_in_file, junk, ISIZE);
		} else {
			if (ftn) efread(junk, ISIZE, 1, stdin);
		}
		/* Do read of data for the segy */
		if (ENABLE_DFS && is_file) {
			iread = read_dfs_file(daos_in_file, (char *) tr.data, FSIZE * ns);
		} else {
			iread = fread((char *) tr.data, FSIZE, ns, stdin);
		}
		if(iread!=ns) {
			return(CWP_Exit());

		} else {
			if(ihead==0) {
				tr.tracl = ++tracl;
			} else {
				if (ENABLE_DFS && is_file) {
					read_dfs_file(daos_headfp, &tr, HDRBYTES);
				} else {
					efread(&tr, 1, HDRBYTES, headfp);
				}
			}
			tr.ns = ns;
			tr.trid = TREAL;
			puttr(&tr);
		}

		/* If Fortran data, read past the record size bytes */
		if (ENABLE_DFS && is_file) {
			if (ftn) read_dfs_file(daos_in_file, junk, ISIZE);
		} else {
			if (ftn) efread(junk, ISIZE, 1, stdin);
		}
	}
	fini_dfs_api();
	return(CWP_Exit());
}
