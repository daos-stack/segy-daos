/*
 * daos_seis_internal_functions.h
 *
 *  Created on: Jun 29, 2020
 *      Author: mirnamoawad
 */

#ifndef LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_
#define LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_

#include "su_helpers.h"
#include "dfs_helpers.h"

#define SEIS_MAX_PATH NAME_MAX
#define DS_D_FILE_HEADER "File_Header"
#define DS_A_NEXTENDED_HEADER "Number_Extended_Headers"
#define DS_A_TEXT_HEADER "Text_Header"
#define DS_A_BINARY_HEADER "Binary_Header"
#define DS_A_EXTENDED_HEADER "Extended_Text_Header"
#define DS_A_NTRACES_HEADER "Number_of_Traces"
#define DS_D_TRACE_HEADER "Trace_Header"
#define DS_A_TRACE_HEADER "File_Trace_Header"
#define DS_D_TRACE_DATA "Trace_Data_"
#define DS_A_TRACE_DATA "File_Trace_Data"
#define DS_D_SORTING_TYPES "Sorting_Types"
#define DS_A_SHOT_GATHER "Shot_Gather"
#define DS_A_CMP_GATHER "Cmp_Gather"
#define DS_A_OFFSET_GATHER "Offset_Gather"
#define DS_D_SHOT "Shot_"
#define DS_D_CMP "Cmp_"
#define DS_D_OFFSET "Off_"
#define DS_A_TRACE "TRACE_"
#define DS_D_NGATHERS "Number_of_gathers"
#define DS_A_NGATHERS "Number_of_gathers"
#define DS_A_TRACE_OIDS "Trace_oids"
#define DS_A_SHOT_ID "Shot_id"
#define DS_A_NTRACES "Number_of_traces"
#define DS_A_CMP_VAL "Cmp_value"
#define DS_A_OFF_VAL "Offset_value"
#define DS_A_GATHER_TRACE_OIDS "TRACE_OIDS_OBJECT_ID"

struct stat *seismic_stat;

typedef struct seis_gather{
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
	/** pointer to the next gather */
	struct seis_gather *next_gather;
}seis_gather_t;

/** object struct that is instantiated for SEGYROOT open object */
typedef struct seis_root_obj {
	/** root dfs object */
	dfs_obj_t *root_obj;
	/** DAOS object ID of the CMP object */
	daos_obj_id_t		cmp_oid;
	/** DAOS object ID of the SHOT object */
	daos_obj_id_t		shot_oid;
	/** DAOS object ID of the GATHER object */
	daos_obj_id_t		offset_oid;
	/** number of traces */
	int 	number_of_traces;
	/** number of extended text headers */
	int 	nextended;
}seis_root_obj_t;

/** object struct that is instantiated for a Seismic open object */
typedef struct seis_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
	/** current sequence number */
	int sequence_number;
	/** number of gathers */
	int number_of_gathers;
	/**array of gathers */
	seis_gather_t *gathers;
}seis_obj_t;

/** object struct that is instantiated for a Seismic trace object */
typedef struct trace_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
	/**trace header */
	segy *trace;
}trace_obj_t;

typedef struct seismic_entry {
	char 		*dkey_name;

	char 		*akey_name;

	daos_obj_id_t	oid;

	char		*data;

	int		size;

	daos_iod_type_t		iod_type;
}seismic_entry_t;

/** object struct that is instantiated for a Seismic trace object */
typedef struct trace_array_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
}trace_array_obj_t;

typedef struct trace {

	daos_obj_id_t trace_header_obj;


	int tracl;	/* Trace sequence number within line
			   --numbers continue to increase if the
			   same line continues across multiple
			   SEG Y files.
			   byte# 1-4
			 */

	int tracr;	/* Trace sequence number within SEG Y file
			   ---each file starts with trace sequence
			   one
			   byte# 5-8
			 */

	int fldr;	/* Original field record number
			   byte# 9-12
			*/

	int tracf;	/* Trace number within original field record
			   byte# 13-16
			*/

	int ep;		/* energy source point number
			   ---Used when more than one record occurs
			   at the same effective surface location.
			   byte# 17-20
			 */

	int cdp;	/* Ensemble number (i.e. CDP, CMP, CRP,...)
			   byte# 21-24
			*/

	int cdpt;	/* trace number within the ensemble
			   ---each ensemble starts with trace number one.
			   byte# 25-28
			 */

	short trid;	/* trace identification code:
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

	short nvs;	/* Number of vertically summed traces yielding
			   this trace. (1 is one trace,
			   2 is two summed traces, etc.)
			   byte# 31-32
			 */

	short nhs;	/* Number of horizontally summed traces yielding
			   this trace. (1 is one trace
			   2 is two summed traces, etc.)
			   byte# 33-34
			 */

	short duse;	/* Data use:
				1 = Production
				2 = Test
			   byte# 35-36
			 */

	int offset;	/* Distance from the center of the source point
			   to the center of the receiver group
			   (negative if opposite to direction in which
			   the line was shot).
			   byte# 37-40
			 */

	int gelev;	/* Receiver group elevation from sea level
			   (all elevations above the Vertical datum are
			   positive and below are negative).
			   byte# 41-44
			 */

	int selev;	/* Surface elevation at source.
			   byte# 45-48
			 */

	int sdepth;	/* Source depth below surface (a positive number).
			   byte# 49-52
			 */

	int gdel;	/* Datum elevation at receiver group.
			   byte# 53-56
			*/

	int sdel;	/* Datum elevation at source.
			   byte# 57-60
			*/

	int swdep;	/* Water depth at source.
			   byte# 61-64
			*/

	int gwdep;	/* Water depth at receiver group.
			   byte# 65-68
			*/

	short scalel;	/* Scalar to be applied to the previous 7 entries
			   to give the real value.
			   Scalar = 1, +10, +100, +1000, +10000.
			   If positive, scalar is used as a multiplier,
			   if negative, scalar is used as a divisor.
			   byte# 69-70
			 */

	short scalco;	/* Scalar to be applied to the next 4 entries
			   to give the real value.
			   Scalar = 1, +10, +100, +1000, +10000.
			   If positive, scalar is used as a multiplier,
			   if negative, scalar is used as a divisor.
			   byte# 71-72
			 */

	int  sx;	/* Source coordinate - X
			   byte# 73-76
			*/

	int  sy;	/* Source coordinate - Y
			   byte# 77-80
			*/

	int  gx;	/* Group coordinate - X
			   byte# 81-84
			*/

	int  gy;	/* Group coordinate - Y
			   byte# 85-88
			*/

	short counit;	/* Coordinate units: (for previous 4 entries and
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

	short wevel;	/* Weathering velocity.
			   byte# 91-92
			*/

	short swevel;	/* Subweathering velocity.
			   byte# 93-94
			*/

	short sut;	/* Uphole time at source in milliseconds.
			   byte# 95-96
			*/

	short gut;	/* Uphole time at receiver group in milliseconds.
			   byte# 97-98
			*/

	short sstat;	/* Source static correction in milliseconds.
			   byte# 99-100
			*/

	short gstat;	/* Group static correction  in milliseconds.
			   byte# 101-102
			*/

	short tstat;	/* Total static applied  in milliseconds.
			   (Zero if no static has been applied.)
			   byte# 103-104
			*/

	short laga;	/* Lag time A, time in ms between end of 240-
			   byte trace identification header and time
			   break, positive if time break occurs after
			   end of header, time break is defined as
			   the initiation pulse which maybe recorded
			   on an auxiliary trace or as otherwise
			   specified by the recording system
			   byte# 105-106
			*/

	short lagb;	/* lag time B, time in ms between the time break
			   and the initiation time of the energy source,
			   may be positive or negative
			   byte# 107-108
			*/

	short delrt;	/* delay recording time, time in ms between
			   initiation time of energy source and time
			   when recording of data samples begins
			   (for deep water work if recording does not
			   start at zero time)
			   byte# 109-110
			*/

	short muts;	/* mute time--start
			   byte# 111-112
			*/

	short mute;	/* mute time--end
			   byte# 113-114
			*/

	unsigned short ns;	/* number of samples in this trace
			   byte# 115-116
			*/

	unsigned short dt;	/* sample interval; in micro-seconds
			   byte# 117-118
			*/

	short gain;	/* gain type of field instruments code:
				1 = fixed
				2 = binary
				3 = floating point
				4 ---- N = optional use
			   byte# 119-120
			*/

	short igc;	/* instrument gain constant
			   byte# 121-122
			*/

	short igi;	/* instrument early or initial gain
			   byte# 123-124
			*/

	short corr;	/* correlated:
				1 = no
				2 = yes
			   byte# 125-126
			*/

	short sfs;	/* sweep frequency at start
			   byte# 127-128
			*/

	short sfe;	/* sweep frequency at end
			   byte# 129-130
			*/

	short slen;	/* sweep length in ms
			   byte# 131-132
			*/

	short styp;	/* sweep type code:
				1 = linear
				2 = cos-squared
				3 = other
			   byte# 133-134
			*/

	short stas;	/* sweep trace length at start in ms
			   byte# 135-136
			*/

	short stae;	/* sweep trace length at end in ms
			   byte# 137-138
			*/

	short tatyp;	/* taper type: 1=linear, 2=cos^2, 3=other
			   byte# 139-140
			*/

	short afilf;	/* alias filter frequency if used
			   byte# 141-142
			*/

	short afils;	/* alias filter slope
			   byte# 143-144
			*/

	short nofilf;	/* notch filter frequency if used
			   byte# 145-146
			*/

	short nofils;	/* notch filter slope
			   byte# 147-148
			*/

	short lcf;	/* low cut frequency if used
			   byte# 149-150
			*/

	short hcf;	/* high cut frequncy if used
			   byte# 151-152
			*/

	short lcs;	/* low cut slope
			   byte# 153-154
			*/

	short hcs;	/* high cut slope
			   byte# 155-156
			*/

	short year;	/* year data recorded
			   byte# 157-158
			*/

	short day;	/* day of year
			   byte# 159-160
			*/

	short hour;	/* hour of day (24 hour clock)
			   byte# 161-162
			*/

	short minute;	/* minute of hour
			   byte# 163-164
			*/

	short sec;	/* second of minute
			   byte# 165-166
			*/

	short timbas;	/* time basis code:
				1 = local
				2 = GMT
				3 = other
			   byte# 167-168
			*/

	short trwf;	/* trace weighting factor, defined as 1/2^N
			   volts for the least sigificant bit
			   byte# 169-170
			*/

	short grnors;	/* geophone group number of roll switch
			   position one
			   byte# 171-172
			*/

	short grnofr;	/* geophone group number of trace one within
			   original field record
			   byte# 173-174
			*/

	short grnlof;	/* geophone group number of last trace within
			   original field record
			   byte# 175-176
			*/

	short gaps;	/* gap size (total number of groups dropped)
			   byte# 177-178
			*/

	short otrav;	/* overtravel taper code:
				1 = down (or behind)
				2 = up (or ahead)
			   byte# 179-180
			*/

	/* cwp local assignments */
	float d1;	/* sample spacing for non-seismic data
			   byte# 181-184
			*/

	float f1;	/* first sample location for non-seismic data
			   byte# 185-188
			*/

	float d2;	/* sample spacing between traces
			   byte# 189-192
			*/

	float f2;	/* first trace location
			   byte# 193-196
			*/

	float ungpow;	/* negative of power used for dynamic
			   range compression
			   byte# 197-200
			*/

	float unscale;	/* reciprocal of scaling factor to normalize
			   range
			   byte# 201-204
			*/

	int ntr; 	/* number of traces
			   byte# 205-208
			*/

	short mark;	/* mark selected traces
			   byte# 209-210
			*/

	short shortpad; /* alignment padding
		   byte# 211-212
		*/


	short unass[14];	/* unassigned--NOTE: last entry causes
			   a break in the word alignment, if we REALLY
			   want to maintain 240 bytes, the following
			   entry should be an odd number of short/UINT2
			   OR do the insertion above the "mark" keyword
			   entry
			   byte# 213-240
			*/

	float  *data;

}trace_t;

typedef struct read_traces{
	int number_of_traces;
	trace_t *traces ;
}read_traces;

int daos_seis_fetch_entry(daos_handle_t oh, daos_handle_t th, struct seismic_entry *entry);

int daos_seis_array_fetch_entry(daos_handle_t oh, daos_handle_t th,int nrecords, struct seismic_entry *entry);

int daos_seis_array_obj_update(daos_handle_t oh, daos_handle_t th, int nrecords, struct seismic_entry entry);

int daos_seis_th_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *data, int nbytes);

int daos_seis_root_obj_create(dfs_t *dfs, seis_root_obj_t **obj,daos_oclass_id_t cid,
			char *name, dfs_obj_t *parent);

int daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, struct seismic_entry entry);

int daos_seis_root_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char* databuf, int nbytes, daos_iod_type_t iod_type);

int daos_seis_bh_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , bhed *bhdr, int nbytes);

int daos_seis_exth_update(dfs_t* dfs, seis_root_obj_t* root_obj, char* dkey_name,
			char* akey_name , char *ebcbuf, int index, int nbytes);

int daos_seis_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid, seis_root_obj_t *parent,
			seis_obj_t **shot_obj, seis_obj_t **cmp_obj, seis_obj_t **offset_obj);

int daos_seis_trh_update(dfs_t* dfs, trace_obj_t* tr_obj, segy *tr, int hdrbytes);

int daos_seis_tr_data_update(dfs_t* dfs, trace_obj_t* trace_data_obj, segy *trace);

daos_obj_id_t get_tr_data_oid(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid);

int daos_seis_tr_obj_create(dfs_t* dfs, trace_obj_t **trace_hdr_obj, int index, segy *trace, int nbytes);

void prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid, char *dkey, char *akey,
			char *data,int size, daos_iod_type_t iod_type);

int daos_seis_tr_linking(dfs_t* dfs, trace_obj_t* trace_obj, segy *trace,
			seis_obj_t *shot_obj, seis_obj_t *cmp_obj, seis_obj_t *off_obj);

int pcreate(int fds[2], const char *command, char *const argv[]);

int execute_command(char *const argv[], char *write_buffer,
						int write_bytes, char *read_buffer, int read_bytes);

void add_gather(seis_gather_t **head, seis_gather_t *new_gather);

int check_key_value(int *targets,seis_gather_t *head, daos_obj_id_t trace_obj, int *ntraces);

int update_gather_traces(dfs_t *dfs, seis_gather_t *head, seis_obj_t *object, trace_array_obj_t *trace_oids_obj, char *dkey_name, char *akey_name);

int update_gather_object(seis_obj_t *shot_obj, char *dkey_name, char *akey_name,
								char *data, int nbytes, daos_iod_type_t type);

int daos_seis_gather_oids_array_update(dfs_t* dfs, trace_array_obj_t* object, seis_gather_t *gather);

void prepare_keys(char *dkey_name, char *akey_name, char *dkey_prefix,
						char *akey_prefix, int nkeys, int *dkey_suffix, int *akey_suffix);

#endif /* LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_ */
