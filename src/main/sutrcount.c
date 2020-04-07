/* Copyright (c) Colorado School of Mines, 2011.*/
/* All rights reserved.                       */

/* SUTRCOUNT: $Revision: 1.5 $ ; $Date: 2015/08/11 21:31:50 $	*/


#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "su.h"
#include "segy.h"
#include "tapesegy.h"
#include "tapebhdr.h"
#include "bheader.h"
#include "header.h"
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include "dfs_helper_api.h"
#include "daos.h"
#include "daos_fs.h"
/*********************** self documentation *****************************/
char *sdoc[] = {
" SUTRCOUNT - SU program to count the TRaces in infile		",
"       							",
"   sutrcount < infile					     	",
" Required parameters:						",
"       none							",
" Optional parameter:						",
"    outpar=stdout						",
" Notes:       							",
" Once you have the value of ntr, you may set the ntr header field",
" via:      							",
"       sushw key=ntr a=NTR < datain.su  > dataout.su 		",
" Where NTR is the value of the count obtained with sutrcount 	",
NULL};

/*
 * Credits:  B.Nemeth, Potash Corporation, Saskatchewan 
 * 		given to CWP in 2008 with permission of Potash Corporation
 */

/**************** end self doc ********************************/
#define ENABLE_DFS 1

/* Segy data constants */
segy tr;				/* SEGY trace */

int 
main(int argc, char **argv)
{
	/* Segy data constans */
	int ntr=0;		/* number of traces			*/
	char *outpar=NULL;	/* name of file holding output		*/
	FILE *outparfp=stdout;	/* ... its file pointer			*/

	initargs(argc, argv);
   	requestdoc(1);
    init_dfs_api("8c83156e-598b-4546-b6b8-f342df715e02","0", "8c83156e-598b-4546-b6b8-f342df715e00",0,1);
    daos_size_t size;
    DAOS_FILE *daos_outpar;

	/* Get information from the first header */
	if (!gettr(&tr)) err("can't get first trace");
	if (!getparstring("outpar", &outpar))	outpar = "/dev/stdout" ;

    if(ENABLE_DFS && strcmp(outpar, "/dev/stdout") != 0) {
        daos_outpar = open_dfs_file(outpar, S_IFREG | S_IWUSR | S_IRUSR, 'w', 0);
    } else {
        outparfp = efopen(outpar, "w");
    }

        checkpars();
	/* Loop over traces getting a count */
	do {
		++ntr;
	} while(gettr(&tr));

	fprintf(outparfp, "%d", ntr);
    fini_dfs_api();

	return(CWP_Exit());

}
