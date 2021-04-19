/*
 * segy_helpers.c
 *
 *  Created on: Jan 31, 2021
 *      Author: mirnamoawad
 */

#include "seismic_sources/segy.h"
#include "data_types/generic_value.h"
#include <unistd.h>
//#include <err.h>
void
swab(const void *restrict from, void *restrict to, ssize_t n);

static struct {
        char *key;      char *type;     int offs;
} tapehdr[] = {
           {"tracl",             "P",            0},
           {"tracr",             "P",            4},
            {"fldr",             "P",            8},
           {"tracf",             "P",            12},
              {"ep",             "P",            16},
             {"cdp",             "P",            20},
            {"cdpt",             "P",            24},
            {"trid",             "U",            28},
             {"nvs",             "U",            30},
             {"nhs",             "U",            32},
            {"duse",             "U",            34},
          {"offset",             "P",            36},
           {"gelev",             "P",            40},
           {"selev",             "P",            44},
          {"sdepth",             "P",            48},
            {"gdel",             "P",            52},
            {"sdel",             "P",            56},
           {"swdep",             "P",            60},
           {"gwdep",             "P",            64},
          {"scalel",             "U",            68},
          {"scalco",             "U",            70},
              {"sx",             "P",            72},
              {"sy",             "P",            76},
              {"gx",             "P",            80},
              {"gy",             "P",            84},
          {"counit",             "U",            88},
           {"wevel",             "U",            90},
          {"swevel",             "U",            92},
             {"sut",             "U",            94},
             {"gut",             "U",            96},
           {"sstat",             "U",            98},
           {"gstat",             "U",            100},
           {"tstat",             "U",            102},
            {"laga",             "U",            104},
            {"lagb",             "U",            106},
           {"delrt",             "U",            108},
            {"muts",             "U",            110},
            {"mute",             "U",            112},
              {"ns",             "U",            114},
              {"dt",             "U",            116},
            {"gain",             "U",            118},
             {"igc",             "U",            120},
             {"igi",             "U",            122},
            {"corr",             "U",            124},
             {"sfs",             "U",            126},
             {"sfe",             "U",            128},
            {"slen",             "U",            130},
            {"styp",             "U",            132},
            {"stas",             "U",            134},
            {"stae",             "U",            136},
           {"tatyp",             "U",            138},
           {"afilf",             "U",            140},
           {"afils",             "U",            142},
          {"nofilf",             "U",            144},
          {"nofils",             "U",            146},
             {"lcf",             "U",            148},
             {"hcf",             "U",            150},
             {"lcs",             "U",            152},
             {"hcs",             "U",            154},
            {"year",             "U",            156},
             {"day",             "U",            158},
            {"hour",             "U",            160},
          {"minute",             "U",            162},
             {"sec",             "U",            164},
          {"timbas",             "U",            166},
            {"trwf",             "U",            168},
          {"grnors",             "U",            170},
          {"grnofr",             "U",            172},
          {"grnlof",             "U",            174},
            {"gaps",             "U",            176},
           {"otrav",             "U",            178},
};

static struct {
        char *key;      char *type;     int offs;
} bhdr[] = {
            {"jobid",             "i",            0},
            {"lino",              "i",            4},
            {"reno",              "i",            8},
            {"ntrpr",             "h",            12},
            {"nart",              "h",            14},
            {"hdt",               "h",            16},
            {"dto",               "h",            18},
            {"hns",               "h",            20},
            {"nso",               "h",            22},
            {"format",            "h",            24},
            {"fold",              "h",            26},
            {"tsort",             "h",            28},
            {"vscode",            "h",            30},
            {"hsfs",              "h",            32},
            {"hsfe",              "h",            34},
            {"hslen",             "h",            36},
            {"hstyp",             "h",            38},
            {"schn",              "h",            40},
            {"hstas",             "h",            42},
            {"hstae",             "h",            44},
            {"htatyp",            "h",            46},
            {"hcorr",             "h",            48},
            {"bgrcv",             "h",            50},
            {"rcvm",              "h",            52},
            {"mfeet",             "h",            54},
            {"polyt",             "h",            56},
            {"vpol",              "h",            58}
};

static struct {
        char *key;      char *type;     int offs;
} tapebhdr[] = {
           {"jobid",             "P",            0},
           {"lino",              "P",            4},
           {"reno",              "P",            8},
           {"ntrpr",             "U",            12},
           {"nart",              "U",            14},
           {"hdt",               "U",            16},
           {"dto",               "U",            18},
           {"hns",               "U",            20},
           {"nso",               "U",            22},
           {"format",            "U",            24},
           {"fold",              "U",            26},
           {"tsort",             "U",            28},
           {"vscode",            "U",            30},
           {"hsfs",              "U",            32},
           {"hsfe",              "U",            34},
           {"hslen",             "U",            36},
           {"hstyp",             "U",            38},
           {"schn",              "U",            40},
           {"hstas",             "U",            42},
           {"hstae",             "U",            44},
           {"htatyp",            "U",            46},
           {"hcorr",             "U",            48},
           {"bgrcv",             "U",            50},
           {"rcvm",              "U",            52},
           {"mfeet",             "U",            54},
           {"polyt",             "U",            56},
           {"vpol",              "U",            58},
};

typedef struct {        /* bhedtape - binary header */

        unsigned int jobid:32;  /* job identification number */

        unsigned int lino:32;   /* line number (only one line per reel) */

        unsigned int reno:32;   /* reel number */

        unsigned int ntrpr:16;  /* number of data traces per record */

        unsigned int nart:16;   /* number of auxiliary traces per record */

        unsigned int hdt:16;    /* sample interval (microsecs) for this reel */

        unsigned int dto:16;    /* same for original field recording */

        unsigned int hns:16;    /* number of samples per trace for this reel */

        unsigned int nso:16;    /* same for original field recording */

        unsigned int format:16; /* data sample format code:
                                1 = floating point (4 bytes)
                                2 = fixed point (4 bytes)
                                3 = fixed point (2 bytes)
                                4 = fixed point w/gain code (4 bytes) */

        unsigned int fold:16;   /* CDP fold expected per CDP ensemble */

        unsigned int tsort:16;  /* trace sorting code:
                                1 = as recorded (no sorting)
                                2 = CDP ensemble
                                3 = single fold continuous profile
                                4 = horizontally stacked */

        unsigned int vscode:16; /* vertical sum code:
                                1 = no sum
                                2 = two sum ...
                                N = N sum (N = 32,767) */

        unsigned int hsfs:16;   /* sweep frequency at start */

        unsigned int hsfe:16;   /* sweep frequency at end */

        unsigned int hslen:16;  /* sweep length (ms) */

        unsigned int hstyp:16;  /* sweep type code:
                                1 = linear
                                2 = parabolic
                                3 = exponential
                                4 = other */

        unsigned int schn:16;   /* trace number of sweep channel */

        unsigned int hstas:16;  /* sweep trace taper length at start if
                           tapered (the taper starts at zero time
                           and is effective for this length) */

        unsigned int hstae:16;  /* sweep trace taper length at end (the ending
                           taper starts at sweep length minus the taper
                           length at end) */

        unsigned int htatyp:16; /* sweep trace taper type code:
                                1 = linear
                                2 = cos-squared
                                3 = other */

        unsigned int hcorr:16;  /* correlated data traces code:
                                1 = no
                                2 = yes */

        unsigned int bgrcv:16;  /* binary gain recovered code:
                                1 = yes
                                2 = no */

        unsigned int rcvm:16;   /* amplitude recovery method code:
                                1 = none
                                2 = spherical divergence
                                3 = AGC
                                4 = other */

        unsigned int mfeet:16;  /* measurement system code:
                                1 = meters
                                2 = feet */

        unsigned int polyt:16;  /* impulse signal polarity code:
                                1 = increase in pressure or upward
                                    geophone case movement gives
                                    negative number on tape
                                2 = increase in pressure or upward
                                    geophone case movement gives
                                    positive number on tape */

        unsigned int vpol:16;   /* vibratory polarity code:
                                code    seismic signal lags pilot by
                                1       337.5 to  22.5 degrees
                                2        22.5 to  67.5 degrees
                                3        67.5 to 112.5 degrees
                                4       112.5 to 157.5 degrees
                                5       157.5 to 202.5 degrees
                                6       202.5 to 247.5 degrees
                                7       247.5 to 292.5 degrees
                                8       293.5 to 337.5 degrees */

        unsigned char hunass[340];      /* unassigned */

} tapebhed;

void
gettapehval(const tapesegy *tr, int index, generic_value *valp);
void
swapbhval(bhed *bh, int index);
void
tapesegy_to_segy(const tapesegy *tapetrptr, segy *trptr);
void
puthval(segy *tr, int index, generic_value *valp);
void
swap_short_2(short *tni2);
void
swap_int_4(int *tni4);
void
swaphval(segy *tr, int index);
void
swap_u_short_2(unsigned short *tni2);
void
swap_u_int_4(unsigned int *tni4);
void
swap_long_4(long *tni4);
void
swap_u_long_4(unsigned long *tni4);
void
swap_float_4(float *tnf4);
void
swap_double_8(double *tndd8);
void
ibm_to_float(int from[], int to[], int n, int endian, int verbose);
void
short_to_float(short from[], float to[], int n, int endian);
void
integer1_to_float(signed char from[], float to[], int n);
void
tapebhed_to_bhed(const tapebhed *tapebhptr, bhed *bhptr);
void
gettapebhval(const tapebhed *tr, int index, generic_value *valp);
void
putbhval(bhed *bh, int index, generic_value *valp);

void
tapesegy_to_segy(const tapesegy *tapetrptr, segy *trptr)
{
    register int i;
    generic_value val;

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

void
gettapehval(const tapesegy *tr, int index, generic_value *valp)
{
	char *tp = (char*) tr;
	switch(*(tapehdr[index].type)) {
	case 'U':
		valp->h = (short) *((short*) (tp + tapehdr[index].offs));
	break;
	case 'P':
		valp->i = (int) *((int*) (tp + tapehdr[index].offs));

	break;
	default:
		//err("%s: %s: mysterious data type", __FILE__, __LINE__);
	break;
	}

	return;
}

void
swapbhval(bhed *bh, int index)
{
	register char *bhp= (char *) bh;

        switch(*(bhdr[index].type)) {
        case 'h': swap_short_2((short*)(bhp + bhdr[index].offs)); break;
        case 'i': swap_int_4((int*)(bhp + bhdr[index].offs)); break;
        default: //err("%s: %s: unsupported data type", __FILE__, __LINE__);
	break;
        }

        return;
}

void
puthval(segy *tr, int index, generic_value *valp)
{
	char *tp = (char*) tr;

	switch(*(hdr[index].type)) {
	case 's': (void) strcpy(tp + hdr[index].offs, valp->s);  break;
	case 'h': *((short*)  (tp + hdr[index].offs)) = valp->h; break;
	case 'u': *((unsigned short*) (tp + hdr[index].offs)) = valp->u; break;
	case 'i': *((int*)   (tp + hdr[index].offs)) = valp->i; break;
	case 'p': *((unsigned int*)   (tp + hdr[index].offs)) = valp->p; break;
	case 'l': *((long*)   (tp + hdr[index].offs)) = valp->l; break;
	case 'v': *((unsigned long*)  (tp + hdr[index].offs)) = valp->v; break;
	case 'f': *((float*)  (tp + hdr[index].offs)) = valp->f; break;
	case 'd': *((double*) (tp + hdr[index].offs)) = valp->d; break;
	default: //err("%s: %s: mysterious data type", __FILE__,__LINE__);
		break;
	}

	return;
}

void
swap_short_2(short *tni2)
{
 *tni2=(((*tni2>>8)&0xff) | ((*tni2&0xff)<<8));
}

void
swap_int_4(int *tni4)
{
 *tni4=(((*tni4>>24)&0xff) | ((*tni4&0xff)<<24) |
	    ((*tni4>>8)&0xff00) | ((*tni4&0xff00)<<8));
}

void
swaphval(segy *tr, int index)
{
	register char *tp= (char *) tr;

        switch(*(hdr[index].type)) {
        case 'h': swap_short_2((short*)(tp + hdr[index].offs));
	break;
        case 'u': swap_u_short_2((unsigned short*)(tp + hdr[index].offs));
	break;
        case 'i': swap_int_4((int*)(tp + hdr[index].offs));
	break;
        case 'p': swap_u_int_4((unsigned int*)(tp + hdr[index].offs));
	break;
        case 'l': swap_long_4((long*)(tp + hdr[index].offs));
	break;
        case 'v': swap_u_long_4((unsigned long*)(tp + hdr[index].offs));
	break;
        case 'f': swap_float_4((float*)(tp + hdr[index].offs));
	break;
        case 'd': swap_double_8((double*)(tp + hdr[index].offs));
	break;
        default: //err("%s: %s: unsupported data type", __FILE__, __LINE__);
	break;
        }

        return;
}

void
swap_u_short_2(unsigned short *tni2)
{
 *tni2=(((*tni2>>8)&0xff) | ((*tni2&0xff)<<8));
}

void
swap_u_int_4(unsigned int *tni4)
{
 *tni4=(((*tni4>>24)&0xff) | ((*tni4&0xff)<<24) |
	    ((*tni4>>8)&0xff00) | ((*tni4&0xff00)<<8));
}

void
swap_long_4(long *tni4)
/**************************************************************************
swap_long_4		swap a long integer
***************************************************************************/
{
 *tni4=(((*tni4>>24)&0xff) | ((*tni4&0xff)<<24) |
	    ((*tni4>>8)&0xff00) | ((*tni4&0xff00)<<8));
}

void
swap_u_long_4(unsigned long *tni4)
/**************************************************************************
swap_u_long_4		swap an unsigned long integer
***************************************************************************/
{
 *tni4=(((*tni4>>24)&0xff) | ((*tni4&0xff)<<24) |
	    ((*tni4>>8)&0xff00) | ((*tni4&0xff00)<<8));
}

void
swap_float_4(float *tnf4)
/**************************************************************************
swap_float_4		swap a float
***************************************************************************/
{
 int *tni4=(int *)tnf4;
 *tni4=(((*tni4>>24)&0xff) | ((*tni4&0xff)<<24) |
	    ((*tni4>>8)&0xff00) | ((*tni4&0xff00)<<8));
}

void
swap_double_8(double *tndd8)
/**************************************************************************
swap_double_8		swap a double
***************************************************************************/
{
  char *tnd8=(char *)tndd8;
  char tnc;

  tnc= *tnd8;
  *tnd8= *(tnd8+7);
  *(tnd8+7)=tnc;

  tnc= *(tnd8+1);
  *(tnd8+1)= *(tnd8+6);
  *(tnd8+6)=tnc;

  tnc= *(tnd8+2);
  *(tnd8+2)= *(tnd8+5);
  *(tnd8+5)=tnc;

  tnc= *(tnd8+3);
  *(tnd8+3)= *(tnd8+4);
  *(tnd8+4)=tnc;
}

void
ibm_to_float(int from[], int to[], int n, int endian, int verbose)
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

void
int_to_float(int from[], float to[], int n, int endian)
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

void
short_to_float(short from[], float to[], int n, int endian)
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

void
integer1_to_float(signed char from[], float to[], int n)
{
    while (n--) {
        to[n] = from[n];
    }
}

void
tapebhed_to_bhed(const tapebhed *tapebhptr, bhed *bhptr)
{
    register int i;
    generic_value val;

    /* convert binary header, field by field */
    for (i = 0; i < BHED_NKEYS; ++i) {
        gettapebhval(tapebhptr, i, &val);
        putbhval(bhptr, i, &val);
    }
}

void
gettapebhval(const tapebhed *tr, int index, generic_value *valp)
{
	char *tp = (char*) tr;

	switch(*(tapebhdr[index].type)) {
	case 'U': valp->h = (short) *((short*) (tp + tapebhdr[index].offs));
	break;
	case 'P': valp->i = (int) *((int*) (tp + tapebhdr[index].offs));
	break;
	default: //err("%s: %s: mysterious data type", __FILE__, __LINE__);
	break;
	}

	return;
}

void
putbhval(bhed *bh, int index, generic_value *valp)
{
	char *bhp = (char*) bh;

	switch(*(bhdr[index].type)) {
	case 'h': *((short*) (bhp + bhdr[index].offs)) = valp->h; break;
	case 'i': *((int*)   (bhp + bhdr[index].offs)) = valp->i; break;
	default: //err("%s: %s: mysterious data type", __FILE__, __LINE__);
	break;
	}

	return;
}
