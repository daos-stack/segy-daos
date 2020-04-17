/* Copyright (c) Colorado School of Mines, 2011.*/
/* All rights reserved.                       */

/* SUGETHW: $Revision: 1.18 $ ; $Date: 2011/11/16 22:10:29 $	*/

#include "su.h"
#include "segy.h"
#include "header.h"
#include "dfs_helper_api.h"

/*********************** self documentation **********************/
char *sdoc[] = {
" 									",
" SUGETHW - sugethw writes the values of the selected key words		",
" 									",
"   sugethw key=key1,... pool=pool_uuid container=container_uuid svc=svc_ranklist [output=] <infile [>outfile]			",
" 									",
" Required parameters:							",
" key=key1,...		At least one key word.				",
" pool=			pool uuid to connect		        ",
" container=		container uuid to connect		",
" svc=			service ranklist of pool seperated by : ",
" 									",
" Optional parameters:							",
" output=ascii		output written as ascii for display		",
" 			=binary for output as binary floats		",
" 			=geom   ascii output for geometry setting	",
" verbose=0 		quiet						",
" 			=1 chatty					",
" 									",
" Output is written in the order of the keys on the command		",
" line for each trace in the data set.					",
" 									",
" Example:								",
" 	sugethw <stdin key=sx,gx					",
" writes sx, gx values as ascii trace by trace to the terminal.		",
" 									",
" Comment: 								",
" Users wishing to edit one or more header field (as in geometry setting)",
" may do this via the following sequence:				",
"     sugethw < sudata output=geom key=key1,key2,... > hdrfile 		",
" Now edit the ASCII file hdrfile with any editor, setting the fields	",
" appropriately. Convert hdrfile to a binary format via:		",
"     a2b < hdrfile n1=nfields > binary_file				",
" Then set the header fields via:					",
"     sushw < sudata infile=binary_file key=key1,key2,... > sudata.edited",
" 									",
NULL};

/* Credits:
 *
 *	SEP: Shuki Ronen
 *	CWP: Jack K. Cohen
 *      CWP: John Stockwell, added geom stuff, and getparstringarray
 */
/**************** end self doc ***********************************/

#define ASCII 0
#define BINARY 1
#define GEOM 2
#define ENABLE_DFS 1


segy tr;

int
main(int argc, char **argv)
{
	cwp_String key[SU_NKEYS];	/* array of keywords		*/
	int nkeys;		/* number of keywords to be gotten 	*/
	int iarg;		/* arguments in argv loop		*/
	int countkey=0;		/* counter of keywords in argc loop	*/
	cwp_String output;	/* string representing output format	*/
	int ioutput=ASCII;	/* integer representing output format	*/
	int verbose;		/* verbose?				*/
    char *pool_id;  /* string of the pool uuid to connect to */
    char *container_id; /*string of the container uuid to connect to */
    char *svc_list;		/*string of the service rank list to connect to */

    char file_name[1024];
    int is_file=0;

    /* Initialize */
	initargs(argc, argv);
	requestdoc(1);

    MUSTGETPARSTRING("pool",  &pool_id);
    MUSTGETPARSTRING("container",  &container_id);
    MUSTGETPARSTRING("svc",  &svc_list);
    init_dfs_api(pool_id, svc_list, container_id, 0, 0);
    daos_size_t size;
    DAOS_FILE *daos_out_file;


    /* Get key values */
	if (!getparint("verbose",&verbose))	verbose=0;
	if ((nkeys=countparval("key"))!=0) {
		getparstringarray("key",key);
	} else {
		/* support old fashioned method for inputting key fields */
		/* as single arguments:  sugethw key1 key2 ... 		*/
		if (argc==1) err("must set at least one key value!");

		for (iarg = 1; iarg < argc; ++iarg) {
			cwp_String keyword;	     /* keyword */

			keyword = argv[iarg];

			if (verbose) warn("argv=%s",argv[iarg]);
			/* get array of types and indexes to be set */
			if ((strncmp(keyword,"output=",7)!=0)) {
				key[countkey] = keyword;
				++countkey;
			}

			if (countkey==0)
				err("must set at least one key value!");
		}
		nkeys=countkey;
	}

	/* Set and check output format; recall ioutput initialized to ASCII */
	if (!getparstring("output", &output))   output = "ascii";

        checkpars();

	if      (STREQ(output, "binary"))	ioutput = BINARY;
	else if (STREQ(output, "geom"))		ioutput = GEOM;
	else if (!STREQ(output, "ascii"))
		err("unknown format output=\"%s\", see self-doc", output);


    if (DISK == filestat(fileno(stdout))) {
        get_file_name(stdout, file_name);
        is_file = 1;
        daos_out_file = open_dfs_file(file_name, S_IFREG | S_IWUSR | S_IRUSR, 'w', 1);
    } else {
        is_file = 0;
    }




    /* Loop over traces writing selected header field values */
	while (gettr(&tr)) {
		register int ikey;

		for (ikey = 0; ikey < nkeys; ++ikey) {
			Value val;
			float fval;

			gethdval(&tr, key[ikey], &val);

			switch(ioutput) {
			case ASCII:
			    if(ENABLE_DFS && is_file){
			        char *buffer = malloc(512 * sizeof(char));
                    sprintf(buffer, "%6s=", key[ikey]);
                    write_dfs_file(daos_out_file, buffer, strlen(buffer));
                    printdfsval(hdtype(key[ikey]), val, daos_out_file);
                    putdfschar(daos_out_file, '\t');
                    free(buffer);
                } else {
                    printf("%6s=", key[ikey]);
                    printfval(hdtype(key[ikey]), val);
                    putchar('\t');
			    }
			break;
			case BINARY:
			    if(ENABLE_DFS && is_file){
                    fval = vtof(hdtype(key[ikey]), val);
                    write_dfs_file(daos_out_file, (char *) &fval, FSIZE);
                } else {
                    fval = vtof(hdtype(key[ikey]), val);
                    efwrite((char *) &fval, FSIZE, 1, stdout);
			    }
			break;
			case GEOM:
			    if(ENABLE_DFS && is_file){
                    printdfsval(hdtype(key[ikey]), val, daos_out_file);
                    putdfschar(daos_out_file, ' ');
                }else{
                    printfval(hdtype(key[ikey]), val);
                    putchar(' ');
			    }
			break;
			}
		}

		switch(ioutput) {
		case GEOM:
		    if(ENABLE_DFS && is_file){
		        putdfschar(daos_out_file, '\n');
		    } else {
                printf("\n");
		    }
		break;
		case ASCII:
		    if(ENABLE_DFS && is_file){
                putdfschar(daos_out_file, '\n');
                putdfschar(daos_out_file, '\n');
		    } else {
                printf("\n\n");
		    }
		break;
		}
	}
//    if(ENABLE_DFS){
//        close_dfs_file(daos_out_file);
//    }

    fini_dfs_api();
	return(CWP_Exit());
}
