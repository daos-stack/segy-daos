/*
 * trace.h
 *
 *  Created on: Jan 24, 2021
 *      Author: omar
 */

#ifndef TRACE_H
#define TRACE_H

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

#include "data_structures/doubly_linked_list.h"
#include "daos.h"
#include "daos_fs.h"
#include "data_types/generic_value.h"
#include "utilities/error_handler.h"

#define SEIS_NKEYS 80
#define TRACEHDR_BYTES 240

/* struct to hold properties by which to sort an array of traces */
typedef struct sort_params {
	/* array of key strings */
	char **keys;
	/* number of keys in the array of keys */
	int num_of_keys;
	/* array of directions corresponding to array of keys */
	int *directions;
} sort_params_t;

/* struct to hold properties by which to search an array of traces */
typedef struct comp_params {
	/* key being searched for */
	char *key;
	/* value of key being searched for */
	generic_value val;
} comp_params_t;

/* struct to hold properties by which to filter an array of traces */
typedef struct window_params {
	/* Array of key strings */
	char **keys;
	/* Minimum values for each key */
	generic_value *min_keys;
	/* Maximum values for each key */
	generic_value *max_keys;
	/* Number of keys being filtered */
	int num_of_keys;
} window_params_t;

/** struct holding trace header fields , id of the array object holding trace
 *  data, and array of trace data.
 *  equivalent to segy struct defined in segy.h.
 */
typedef struct trace {

	int tracl; /* Trace sequence number within line
	 --numbers continue to increase if the
	 same line continues across multiple
	 SEG Y files.
	 byte# 1-4
	 */

	int tracr; /* Trace sequence number within SEG Y file
	 ---each file starts with trace sequence
	 one
	 byte# 5-8
	 */

	int fldr; /* Original field record number
	 byte# 9-12
	 */

	int tracf; /* Trace number within original field record
	 byte# 13-16
	 */

	int ep; /* energy source point number
	 ---Used when more than one record occurs
	 at the same effective surface location.
	 byte# 17-20
	 */

	int cdp; /* Ensemble number (i.e. CDP, CMP, CRP,...)
	 byte# 21-24
	 */

	int cdpt; /* trace number within the ensemble
	 ---each ensemble starts with trace number one.
	 byte# 25-28
	 */

	short trid; /* trace identification code:
	 -1 = Other
	 0 = Unknown
	 1 = Seismic data
	 2 = Dead
	 3 = Dummy
	 4 = Time break
	 5 = Uphole
	 6 = Sweep
	 7 = Timing
	 8 = Water break
	 9 = Near-field gun signature
	 10 = Far-field gun signature
	 11 = Seismic pressure sensor
	 12 = Multicomponent seismic sensor
	 - Vertical component
	 13 = Multicomponent seismic sensor
	 - Cross-line component
	 14 = Multicomponent seismic sensor
	 - in-line component
	 15 = Rotated multicomponent seismic sensor
	 - Vertical component
	 16 = Rotated multicomponent seismic sensor
	 - Transverse component
	 17 = Rotated multicomponent seismic sensor
	 - Radial component
	 18 = Vibrator reaction mass
	 19 = Vibrator baseplate
	 20 = Vibrator estimated ground force
	 21 = Vibrator reference
	 22 = Time-velocity pairs
	 23 ... N = optional use
	 (maximum N = 32,767)

	 Following are CWP id flags:

	 109 = autocorrelation
	 110 = Fourier transformed - no packing
	 xr[0],xi[0], ..., xr[N-1],xi[N-1]
	 111 = Fourier transformed - unpacked Nyquist
	 xr[0],xi[0],...,xr[N/2],xi[N/2]
	 112 = Fourier transformed - packed Nyquist
	 even N:
	 xr[0],xr[N/2],xr[1],xi[1], ...,
	 xr[N/2 -1],xi[N/2 -1]
	 (note the exceptional second entry)
	 odd N:
	 xr[0],xr[(N-1)/2],xr[1],xi[1], ...,
	 xr[(N-1)/2 -1],xi[(N-1)/2 -1],xi[(N-1)/2]
	 (note the exceptional second & last entries)
	 113 = Complex signal in the time domain
	 xr[0],xi[0], ..., xr[N-1],xi[N-1]
	 114 = Fourier transformed - amplitude/phase
	 a[0],p[0], ..., a[N-1],p[N-1]
	 115 = Complex time signal - amplitude/phase
	 a[0],p[0], ..., a[N-1],p[N-1]
	 116 = Real part of complex trace from 0 to Nyquist
	 117 = Imag part of complex trace from 0 to Nyquist
	 118 = Amplitude of complex trace from 0 to Nyquist
	 119 = Phase of complex trace from 0 to Nyquist
	 121 = Wavenumber time domain (k-t)
	 122 = Wavenumber frequency (k-omega)
	 123 = Envelope of the complex time trace
	 124 = Phase of the complex time trace
	 125 = Frequency of the complex time trace
	 126 = log amplitude
	 127 = real cepstral domain F(t_c)= invfft[log[fft(F(t)]]
	 130 = Depth-Range (z-x) traces
	 201 = Seismic data packed to bytes (by supack1)
	 202 = Seismic data packed to 2 bytes (by supack2)
	 byte# 29-30
	 */

	short nvs; /* Number of vertically summed traces yielding
	 this trace. (1 is one trace,
	 2 is two summed traces, etc.)
	 byte# 31-32
	 */

	short nhs; /* Number of horizontally summed traces yielding
	 this trace. (1 is one trace
	 2 is two summed traces, etc.)
	 byte# 33-34
	 */

	short duse; /* Data use:
	 1 = Production
	 2 = Test
	 byte# 35-36
	 */

	int offset; /* Distance from the center of the source point
	 to the center of the receiver group
	 (negative if opposite to direction in which
	 the line was shot).
	 byte# 37-40
	 */

	int gelev; /* Receiver group elevation from sea level
	 (all elevations above the Vertical datum are
	 positive and below are negative).
	 byte# 41-44
	 */

	int selev; /* Surface elevation at source.
	 byte# 45-48
	 */

	int sdepth; /* Source depth below surface (a positive number).
	 byte# 49-52
	 */

	int gdel; /* Datum elevation at receiver group.
	 byte# 53-56
	 */

	int sdel; /* Datum elevation at source.
	 byte# 57-60
	 */

	int swdep; /* Water depth at source.
	 byte# 61-64
	 */

	int gwdep; /* Water depth at receiver group.
	 byte# 65-68
	 */

	short scalel; /* Scalar to be applied to the previous 7 entries
	 to give the real value.
	 Scalar = 1, +10, +100, +1000, +10000.
	 If positive, scalar is used as a multiplier,
	 if negative, scalar is used as a divisor.
	 byte# 69-70
	 */

	short scalco; /* Scalar to be applied to the next 4 entries
	 to give the real value.
	 Scalar = 1, +10, +100, +1000, +10000.
	 If positive, scalar is used as a multiplier,
	 if negative, scalar is used as a divisor.
	 byte# 71-72
	 */

	int sx; /* Source coordinate - X
	 byte# 73-76
	 */

	int sy; /* Source coordinate - Y
	 byte# 77-80
	 */

	int gx; /* Group coordinate - X
	 byte# 81-84
	 */

	int gy; /* Group coordinate - Y
	 byte# 85-88
	 */

	short counit; /* Coordinate units: (for previous 4 entries and
	 for the 7 entries before scalel)
	 1 = Length (meters or feet)
	 2 = Seconds of arc
	 3 = Decimal degrees
	 4 = Degrees, minutes, seconds (DMS)

	 In case 2, the X values are longitude and
	 the Y values are latitude, a positive value designates
	 the number of seconds east of Greenwich
	 or north of the equator

	 In case 4, to encode +-DDDMMSS
	 counit = +-DDD*10^4 + MM*10^2 + SS,
	 with scalco = 1. To encode +-DDDMMSS.ss
	 counit = +-DDD*10^6 + MM*10^4 + SS*10^2
	 with scalco = -100.
	 byte# 89-90
	 */

	short wevel; /* Weathering velocity.
	 byte# 91-92
	 */

	short swevel; /* Subweathering velocity.
	 byte# 93-94
	 */

	short sut; /* Uphole time at source in milliseconds.
	 byte# 95-96
	 */

	short gut; /* Uphole time at receiver group in milliseconds.
	 byte# 97-98
	 */

	short sstat; /* Source static correction in milliseconds.
	 byte# 99-100
	 */

	short gstat; /* Group static correction  in milliseconds.
	 byte# 101-102
	 */

	short tstat; /* Total static applied  in milliseconds.
	 (Zero if no static has been applied.)
	 byte# 103-104
	 */

	short laga; /* Lag time A, time in ms between end of 240-
	 byte trace identification header and time
	 break, positive if time break occurs after
	 end of header, time break is defined as
	 the initiation pulse which maybe recorded
	 on an auxiliary trace or as otherwise
	 specified by the recording system
	 byte# 105-106
	 */

	short lagb; /* lag time B, time in ms between the time break
	 and the initiation time of the energy source,
	 may be positive or negative
	 byte# 107-108
	 */

	short delrt; /* delay recording time, time in ms between
	 initiation time of energy source and time
	 when recording of data samples begins
	 (for deep water work if recording does not
	 start at zero time)
	 byte# 109-110
	 */

	short muts; /* mute time--start
	 byte# 111-112
	 */

	short mute; /* mute time--end
	 byte# 113-114
	 */

	unsigned short ns; /* number of samples in this trace
	 byte# 115-116
	 */

	unsigned short dt; /* sample interval; in micro-seconds
	 byte# 117-118
	 */

	short gain; /* gain type of field instruments code:
	 1 = fixed
	 2 = binary
	 3 = floating point
	 4 ---- N = optional use
	 byte# 119-120
	 */

	short igc; /* instrument gain constant
	 byte# 121-122
	 */

	short igi; /* instrument early or initial gain
	 byte# 123-124
	 */

	short corr; /* correlated:
	 1 = no
	 2 = yes
	 byte# 125-126
	 */

	short sfs; /* sweep frequency at start
	 byte# 127-128
	 */

	short sfe; /* sweep frequency at end
	 byte# 129-130
	 */

	short slen; /* sweep length in ms
	 byte# 131-132
	 */

	short styp; /* sweep type code:
	 1 = linear
	 2 = cos-squared
	 3 = other
	 byte# 133-134
	 */

	short stas; /* sweep trace length at start in ms
	 byte# 135-136
	 */

	short stae; /* sweep trace length at end in ms
	 byte# 137-138
	 */

	short tatyp; /* taper type: 1=linear, 2=cos^2, 3=other
	 byte# 139-140
	 */

	short afilf; /* alias filter frequency if used
	 byte# 141-142
	 */

	short afils; /* alias filter slope
	 byte# 143-144
	 */

	short nofilf; /* notch filter frequency if used
	 byte# 145-146
	 */

	short nofils; /* notch filter slope
	 byte# 147-148
	 */

	short lcf; /* low cut frequency if used
	 byte# 149-150
	 */

	short hcf; /* high cut frequncy if used
	 byte# 151-152
	 */

	short lcs; /* low cut slope
	 byte# 153-154
	 */

	short hcs; /* high cut slope
	 byte# 155-156
	 */

	short year; /* year data recorded
	 byte# 157-158
	 */

	short day; /* day of year
	 byte# 159-160
	 */

	short hour; /* hour of day (24 hour clock)
	 byte# 161-162
	 */

	short minute; /* minute of hour
	 byte# 163-164
	 */

	short sec; /* second of minute
	 byte# 165-166
	 */

	short timbas; /* time basis code:
	 1 = local
	 2 = GMT
	 3 = other
	 byte# 167-168
	 */

	short trwf; /* trace weighting factor, defined as 1/2^N
	 volts for the least sigificant bit
	 byte# 169-170
	 */

	short grnors; /* geophone group number of roll switch
	 position one
	 byte# 171-172
	 */

	short grnofr; /* geophone group number of trace one within
	 original field record
	 byte# 173-174
	 */

	short grnlof; /* geophone group number of last trace within
	 original field record
	 byte# 175-176
	 */

	short gaps; /* gap size (total number of groups dropped)
	 byte# 177-178
	 */

	short otrav; /* overtravel taper code:
	 1 = down (or behind)
	 2 = up (or ahead)
	 byte# 179-180
	 */

	/* cwp local assignments */
	float d1; /* sample spacing for non-seismic data
	 byte# 181-184
	 */

	float f1; /* first sample location for non-seismic data
	 byte# 185-188
	 */

	float d2; /* sample spacing between traces
	 byte# 189-192
	 */

	float f2; /* first trace location
	 byte# 193-196
	 */

	float ungpow; /* negative of power used for dynamic
	 range compression
	 byte# 197-200
	 */

	float unscale; /* reciprocal of scaling factor to normalize
	 range
	 byte# 201-204
	 */

	int ntr; /* number of traces
	 byte# 205-208
	 */

	short mark; /* mark selected traces
	 byte# 209-210
	 */

	short shortpad; /* alignment padding
	 byte# 211-212
	 */

	short unass[14]; /* unassigned--NOTE: last entry causes
	 a break in the word alignment, if we REALLY
	 want to maintain 240 bytes, the following
	 entry should be an odd number of short/UINT2
	 OR do the insertion above the "mark" keyword
	 entry
	 byte# 213-240
	 */

	daos_obj_id_t trace_header_obj;
	float *data;

	node_t n;
} trace_t;

/** struct holding array of traces struct previously defined and number of
 *  traces in this array
 *  mainly used while sorting traces headers.
 */
typedef struct read_traces{
	int number_of_traces;
	trace_t *traces ;
}read_traces_t;

/** Function responsible for initializing trace variable
 *
 * \param[in]	tr	double pointer to uninitialized trace variable
 *
 *
 * \return      0 for successful allocation
 */
int
trace_init(trace_t **tr);

/** Function responsible for destroying trace variable
 *
 * \param[in]	tr	pointer to initialized trace variable
 *
 *
 * \return      0 for successful allocation
 */
int
trace_destroy(void *tr);

/** Function responsible for getting a specific trace header variable
 *  and assigning it to a generic_value variable
 *
 * \param[in]	tr	pointer to initialized trace variable
 * \param[in]   index   index of required header
 * \param[in]   valp    pointer to valp variable to be assigned the header value
 *
 *\return	0 if successful,
 * 		error code otherwise
 */
int
trace_gethval(trace_t *tr, int index, generic_value *valp);

/** Function responsible for setting a specific trace header variable
 *  from value stored in a generic_value variable
 *
 * \param[in]	tr	pointer to initialized trace variable
 * \param[in]   index   index of required header
 * \param[in]   valp    pointer to valp variable carrying required value
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
trace_puthval(trace_t *tr, int index, generic_value *valp);

/** Function responsible for getting trace header value
 *
 * \param[in]	trace		trace struct to get a specific header value.
 * \param[in]	sort_key	Key header to get its value.
 * \param[in]	value		value of the header to be returned.
 *
 */
int
trace_get_header(trace_t trace, char *sort_key, generic_value *value);

/** Function responsible for setting trace header value
 *
 * \param[in]	trace		pointer to trace struct to set
 * 				a specific header value.
 * \param[in]	sort_key	Key header to set its value.
 * \param[in]	value		value of the header to be written to the
 * 				trace.
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
trace_set_header(trace_t *trace, char *sort_key, generic_value *value);

/** Function responsible for setting new trace header value
 *
 * \param[in]	tr	       pointer to trace struct to set its header value.
 * \param[in]	itr	       integer to be added to trace shift
 * \param[in] 	a	       double containing value on first
 * 			       trace
 * \param[in] 	b	       double containing increments value
 * 			       within group
 * \param[in] 	c	       double containing group increments
 * \param[in] 	d	       double containing trace number shifts
 * \param[in] 	e	       double containing number of elements
 * 			       in group
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
calculate_header_set(trace_t *tr, char *key, int itr, double a, double b,
		     double c, double d, double e);

/** Function responsible for setting new trace header value
 *
 * \param[in]	tr	       pointer to trace struct to set its header value.
 * \param[in]	keys_1	       header key to set their header value.
 * \param[in]	keys_2	       header key to use their header value while
 * 			       setting keys_1
 * \param[in]	keys_3         header key to use their header value while
 * 			       setting keys_1
 * \param[in] 	a	       double containing overall shift
 * \param[in] 	b	       double containing scale on first input key
 * \param[in] 	c	       double containing scale on second input key
 * \param[in] 	d	       double containing overall scale
 * \param[in]	e	       double containing exponent on first
 * 			       input key
 * \param[in] 	f	       double containing exponent on second
 * 			       input keys
 *
 * \return	0 if successful,
 * 		error code otherwise
 */
int
calculate_header_change(trace_t *tr, char *key1, char *key2, char *key3,
			double a, double b, double c, double d, double e,
			double f);
int
init_comp_params(comp_params_t **cp, char *key, generic_value v);

/** Function responsible for initializing sort parameters
 *
 * \param[in]	sp		double pointer to sort parameters variable
 * \param[in]	keys		Keys to be sorted on
 * \param[in]	num_of_keys	number of keys in keys array
 * \param[in]	directions	array of directions corresponding to each key
 *
 *
 * \return      0 for successful allocation
 */
int
init_sort_params(sort_params_t **sp, char **keys, int num_of_keys,
		 int *directions);

/** Function responsible for searching for a trace by key value in a linked list
 *  of traces
 *
 * \param[in]	ll		initialized linked list of traces
 * \param[in]	key		string of key to be searched for
 * \param[in]	val		value of key to be searched for
 *
 * \return      0 for successful allocation
 */
trace_t*
trace_search(doubly_linked_list_t *ll, char *key, generic_value val);

/** Function responsible for deleting a trace  in a linked list
 *  of traces based on key value
 *
 * \param[in]	ll		initialized linked list of traces
 * \param[in]	key		string of key to be searched for
 * \param[in]	val		value of key to be searched for
 *
 * \return      pointer to found trace, NULL if not found
 */
int
trace_delete(doubly_linked_list_t *ll, char *key, generic_value val);

/** Function passed to the linked list search function to compare each trace
 *  to the required key
 *
 * \param[in]	cp		comparison parameters
 * \param[in]	t		pointer of trace to be checked
 *
 * \return      0 for successful deletion, error code otherwise
 */
int
trace_check_header(void *cp, void *t);

/** Function passed to the sort function to compare two traces
 *  based on sort parameters
 *
 * \param[in]	first		pointer to first trace
 * \param[in]	second		pointer of second trace
 *
 * \return      0 if equal, -1 if first is smaller, 1 if second is smaller
 */
int
trace_comp(void *first, void *second, void *sort_props);


/** Function responsible for getting the index of a header in
 *  the array of trace headers
 *
 *\param[in]	key	pointer to the key
 *
 * \return      index of found key
 */
int
getindex(const char *key);

/** Function responsible for getting the type of a header in
 *  the array of trace headers
 *
 *\param[in]	key	pointer to the key
 *
 * \return      type of found key
 */
char*
hdtype(const char *key);

/** Function responsible for initializing wind parameters
 *
 * \param[in]	wp		double pointer to wind parameters variable
 * \param[in]	keys		Keys to be used while windowing on
 * \param[in]	num_of_keys	number of keys in keys array
 * \param[in]	min		array of minimum generic values.
 * \param[in]	max		array of maximum generic values.
 *
 * \return      0 for successful allocation
 */
int
init_wind_params(window_params_t **wp, char **keys, int num_of_keys,
		 generic_value *min, generic_value *max);

static struct {
	char *key;
	char *type;
	int offs;
} hdr[] = { { "tracl", "i", 0 },
	    { "tracr", "i", 4 },
	    { "fldr", "i", 8 },
	    {"tracf", "i", 12 },
	    { "ep", "i", 16 },
	    { "cdp", "i", 20 },
	    {"cdpt", "i", 24 },
	    { "trid", "h", 28 },
	    { "nvs", "h", 30 },
	    {"nhs", "h", 32 },
	    { "duse", "h", 34 },
	    { "offset", "i", 36 },
	    {"gelev", "i", 40 },
	    { "selev", "i", 44 },
	    { "sdepth", "i", 48 },
	    { "gdel", "i", 52 },
	    { "sdel", "i", 56 },
	    { "swdep", "i", 60 },
	    { "gwdep", "i", 64 },
	    { "scalel", "h", 68 },
	    { "scalco", "h", 70 },
	    { "sx", "i", 72 },
	    { "sy", "i", 76 },
	    {"gx", "i", 80 },
	    { "gy", "i", 84 },
	    { "counit","h", 88 },
	    { "wevel", "h", 90 },
	    { "swevel","h", 92 },
	    { "sut", "h", 94 },
	    { "gut", "h", 96 },
	    { "sstat", "h", 98 },
	    { "gstat", "h", 100 },
	    { "tstat", "h", 102 },
	    { "laga", "h", 104 },
	    { "lagb", "h", 106 },
	    { "delrt", "h", 108 },
	    { "muts", "h", 110 },
	    { "mute", "h", 112 },
	    { "ns", "u", 114 },
	    { "dt", "u", 116 },
	    { "gain", "h", 118 },
	    { "igc", "h", 120 },
	    {"igi", "h", 122 },
	    { "corr", "h", 124 },
	    {"sfs", "h", 126 },
	    { "sfe", "h", 128 },
	    {"slen", "h", 130 },
	    { "styp", "h", 132 },
	    {"stas", "h", 134 },
	    { "stae", "h", 136 },
	    {"tatyp", "h", 138 },
	    { "afilf", "h", 140 },
	    {"afils", "h", 142 },
	    { "nofilf", "h", 144 },
	    {"nofils", "h", 146 },
	    { "lcf", "h", 148 },
	    {"hcf", "h", 150 },
	    { "lcs", "h", 152 },
	    { "hcs","h", 154 },
	    { "year", "h", 156 },
	    { "day", "h",158 },
	    { "hour", "h", 160 },
	    { "minute", "h",162 },
	    { "sec", "h", 164 },
	    { "timbas", "h", 166 },
	    { "trwf", "h", 168 },
	    { "grnors", "h",170 },
	    { "grnofr", "h", 172 },
	    { "grnlof", "h",174 },
	    { "gaps", "h", 176 },
	    { "otrav", "h", 178 },
	    { "d1", "f", 180 },
	    { "f1", "f", 184 },
	    {"d2", "f", 188 },
	    { "f2", "f", 192 },
	    {"ungpow", "f", 196 },
	    { "unscale", "f", 200 },
	    {"ntr", "i", 204 },
	    { "mark", "h", 208 },
	    {"shortpad", "h", 210 },
};


#endif //TRACE_H
