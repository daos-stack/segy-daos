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
#define DS_A_UNIQUE_VAL "Unique_value"
#define DS_A_GATHER_TRACE_OIDS "TRACE_OIDS_OBJECT_ID"
#define	DS_D_NUM_OF_KEYS "NUMBER_OF_KEYS"
#define	DS_A_NUM_OF_KEYS "NUMBER_OF_KEYS"
#define	DS_D_KEYS "KEYS"
#define	DS_A_KEYS "KEY_"

struct stat *seismic_stat;

/** object struct that is instantiated for a Seismic trace object */
typedef struct trace_oid_oh_t {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
}trace_oid_oh_t;

/** seismic gather struct that is instantiated for each new gather while creating */
typedef struct seis_gather{
	/** number of traces under specific gather key */
	int number_of_traces;
	/** array of object ids under specific gather key*/
	daos_obj_id_t *oids;
	/** gather unique info */
	Value unique_key;
	/** pointer to the next gather */
	struct seis_gather *next_gather;
}seis_gather_t;

/** struct of gathers list wrapping the linked list of gathers defined by defining
 *  pointer to head (first gather), pointer to tail (last gather),
 *  and size of linked list(number of gathers).
 */
typedef struct gathers_list{
	seis_gather_t *head;
	seis_gather_t *tail;
	long size;
}gathers_list_t;

/** root object struct that is instantiated for SEGYROOT open object */
typedef struct seis_root_obj {
	/** root dfs object */
	dfs_obj_t *root_obj;
	/** dfs container open handle */
	daos_handle_t coh;
	/** DAOS object ID of the CMP object */
	daos_obj_id_t		*gather_oids;
	/** array of keys */
	char 			**keys;
	/** number of keys */
	int 			num_of_keys;
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
	/** number of gathers */
	int number_of_gathers;
	/**linked list of gathers */
//	seis_gather_t *gathers;
	gathers_list_t *gathers;
	/** pointer to array of trace_oids objects*/
	trace_oid_oh_t *seis_gather_trace_oids_obj;
}seis_obj_t;

/** struct that is instantiated to fetch or write data under seismic object */
typedef struct seismic_entry {
	/** dkey name */
	char 		*dkey_name;
	/** akey name */
	char 		*akey_name;
	/** daos object id of the seimsic object holding the data to be written or fetched */
	daos_obj_id_t	oid;
	/** character array which holds address of data that will be fetched or to be written */
	char		*data;
	/** size of data to be written or fetched */
	int		size;
	/** Type of the value accessed by an io descriptor*/
	daos_iod_type_t		iod_type;
}seismic_entry_t;

/** struct holding trace header fields , id of the array object holding trace data, and array of trace data.
 * equivalent to segy struct defined in segy.h.
 */
typedef struct trace {

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

	daos_obj_id_t trace_header_obj;

//	trace_t *next_trace;
	float  *data;

}trace_t;

/** object struct that is instantiated for a Seismic trace object */
typedef struct trace_obj {
	/** DAOS object ID */
	daos_obj_id_t		oid;
	/** DAOS object open handle */
	daos_handle_t		oh;
	/** entry name of the object */
	char			name[SEIS_MAX_PATH + 1];
	/**trace header */
	trace_t *trace;
}trace_obj_t;

/** struct holding array of traces struct previously defined and number of traces in this array
 *	mainly used while sorting traces headers.
 */
typedef struct read_traces{
	int number_of_traces;
	trace_t *traces ;
}read_traces;

/** struct that is used as a linked list holding trace struct holding all trace data
 *  also holds pointer to next trace.
 */
typedef struct traces_headers{
	trace_t trace;
	struct traces_headers *next_trace;
}traces_headers_t;

/** struct of traces list wrapping the linked list of headers defined by defining
 *  pointer to head (first trace returned to user), pointer to tail (last trace),
 *  and size of linked list(number of traces).
 *  This is the main struct returned after sorting/ windowing/ read headers/...
 */
typedef struct traces_list{
	traces_headers_t *head;
	traces_headers_t *tail;
	long size;
}traces_list_t;

/** enum used in set header value.
 *  0 for SET_HEADERS.
 *  1 for CHANGE_HEADERS.
 */
typedef enum{
	SET_HEADERS,
	CHANGE_HEADERS
}header_operation_type_t;

/** struct used in case of range headers, it holds all header ranges. */
typedef struct headers_ranges{
	trace_t 	*trmin;
	trace_t 	*trmax;
	trace_t 	*trfirst;
	trace_t 	*trlast;
	double		north_shot[2];
	double		south_shot[2];
	double		east_shot[2];
	double		west_shot[2];
	double		north_rec[2];
	double		south_rec[2];
	double		east_rec[2];
	double		west_rec[2];
	double		north_cmp[2];
	double		south_cmp[2];
	double		east_cmp[2];
	double		west_cmp[2];
	double		dmin;
	double		dmax;
	double 		davg;
	char	      **keys;
	int 		ntr; //trace index.
	int 		dim;
	int		number_of_keys;
}headers_ranges_t;

/** Function responsible for fetching all seismic_root object metadata
 * (seismic_gather object ids/number_of_traces/...)
 *  It is called once at the beginning of the program.
 *
 * \param[in]   dfs             pointer to DAOS file system.
 * \param[in]   root            pointer to DAOS file system.
 *
 * \return      pointer to seismic root object
 */
seis_root_obj_t*
daos_seis_open_root(dfs_t *dfs, dfs_obj_t *root);

/** Function responsible for finding the parent of file
 *  given the path to the directory.
 *  It is called once at the beginning of seismic_object_creation.
 *  Function is taken from dfs helper api so it can be called directly
 *  but after adding dfs and verbose output to the parameters.!!!!!
 *
 * \param[in]   dfs             pointer to DAOS file system.
 * \param[in	file_directory  absolute path of file to be created.
 * \param[in]	allow_creation  flag to allow creation of directories
 * 				in case they doesn't exist.
 * \param[in]	file_name	array of characters containing name of the
 * 				file to be created as seismic root object.
 * \param[in]	verbose_output	Integer to enable verbosity
 *
 * \return	pointer to opened parent dfs object, seismic root object
 * 		will be created later in the parse_segy_file
 * 		under the opened parent
 */
dfs_obj_t*
get_parent_of_file_new(dfs_t *dfs, const char *file_directory,
		       int allow_creation, char *file_name,
		       int verbose_output);

/** Function responsible for fetching seismic entry(data stored under specific seismic object)
 *  Set dkey buf→  (name) and buffer length → (size of name)
 *  Set IOdescriptor name (AKEY) and its length
 *  Set IOdescriptor number of entries =1 (How many elements will exist in the array of extents,
 *   (1 if single value))
 *  Set index of the first record in the extent.
 *  Set number of contiguous records in the extent
 *  starting from the index set in previous step(size of mode/ctime/atime/mtime/chunck size).
 *  Make IOdescriptor array of extents point to the allocated array of extents.
 *  Set IOdescriptor type value to DAOS_IOD_array.
 *  Set IOdescriptor size of each record in the array of extents.
 *  Set sg_iovs(scatter gather iovector)
 *
 * \param[in]	oh		opened seismic object connection handle
 * \param[in]	th		daos transaction handle, if zero then
 * 				it's independent transaction.
 * \param[in]	entry		pointer to seimsmic_entry struct holding
 * 				data/akey/dkey/oid/.. that will be used to fetch.!!!!!!!!!!!!!!!!!!!!!!!
 * \param[in]	ev		Completion event, it's optional & can be NULL.
 *				The function will run in blocking
 *				mode if \a ev is NULL.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
daos_seis_fetch_entry(daos_handle_t oh, daos_handle_t th, struct seismic_entry *entry, daos_event_t *ev);

/** FUnction responsible for creating seismic root object under root dfs
 *  or specific dfs object(directory)
 *  It is called once at the beginning of the parsing function.
 *
 * \param[in]	dfs		pointer to daos file system.
 * \param[in]	obj		pointer holding the address of the pointer
 * 				to the seimsic root object to be allocated.
 * \param[in]	cid		DAOS object class id (0 for default MAX_RW).
 * \param[in]	name		string containing the name of the root
 * 				object to be created.
 * \param[in]	parent		pointer to the opened parent(dfs object)
 * 				of the new seismic root object.
 * \param[in]	num_of_keys	Number of strings(keys) in the array of keys.
 * \param[in]	keys		array of strings containing header_keys that
 * 				will be used to create gather objects.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_root_obj_create(dfs_t *dfs, seis_root_obj_t **obj,
			  daos_oclass_id_t cid,	char *name, dfs_obj_t *parent,
			  int num_of_keys, char **keys);

/** Function responsible for updating seismic objects.
 *  It is called after preparing the seismic entry struct.
 *
 * \param[in]	oh	opened seismic object connection handle
 * \param[in]	th	daos transaction handle,
 * 			if zero then it's independent transaction.
 * \param[in]	entry	pointer to seimsmic_entry struct holding
 * 			data/akey/dkey/oid/......
 * 			that will be used to update seismic object.!!!!!!!!!!!!!!!!!!!!!!!
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
daos_seis_obj_update(daos_handle_t oh, daos_handle_t th, seismic_entry_t entry);

/** Function responsible for preparing seismic entry to update keys stored under root seismic object.
 *
 * \param[in]	root_obj	pointer to opened root seismic_object.
 * \param[in]	dkey_name	string containing name of the dkey that will be used to update specific entry under root seismic object.
 * \param[in]	akey_name	string containing name of the akey that exists or will be created.
 * \param[in]	databuf		char array containing the address of the data that will be written/ updated.
 * \param[in]	nbytes		number of bytes that will be written starting from the address of the data buffer under the dkey and akey of the seismic root object.
 * \param[in]	iod_type	type of the value accessed in the IO descriptor(dkey/one value to be updated atomically/ or array of records).
 *
 * \return      0 on success
 * 				error_code otherwise
 */
int
daos_seis_root_update(seis_root_obj_t* root_obj, char* dkey_name,
		      char* akey_name , char* databuf, int nbytes,
		      daos_iod_type_t iod_type);

/** Function responsible for adding one more gather to the existing gathers
 *  under seismic_objects.
 *  Array of gathers is implemented as a linked list.
 *  It is called if the unique value of any trace doesn't belong to an existing
 *  gather in the linked list of gathers.
 *
 * \param[in]	head		pointer to pointer to the head of
 * 				seismic gathers linked list.
 * \param[in]	new_gather	pointer to a temp seismic gather, a new node
 * 				will be created and linked to the linked list
 * 				and this one will be destroyed.
 */
void
add_gather(seis_gather_t **head, seis_gather_t *new_gather);

/** Function responsible for merging two traces lists by making tail of one list points to the head of the other.
 *  It is called only in sorting function after sorting a subgroup of headers.
 *
 *  \param[in]	headers		pointer to pointer to the traces list
 *  				(first part of the linked list)
 *  \param[in]	temp_list	pointer to pointer to the other part
 *  				of the traces list that will be linked.
 *
 */
void
merge_trace_lists(traces_list_t **headers, traces_list_t **temp_list);

/** Function responsible for adding a new trace header to
 *  existing list of headers. It is called after sorting traces headers.
 *  Mainly used to copy traces headers from array of read_traces(that was used
 *  in sorting) to linked list of traces.
 *
 * \param[in]	trace		pointer to the trace header that will be added
 * 				to the linked list of traces(trace_list)
 * \param[in]	head		pointer to the pointer of the traces list,
 * 				new node will be created and linked
 * 				to this traces list
 */
void
add_trace_header(trace_t *trace, traces_list_t **head);

void
add_gather_to_list (seis_gather_t *gather, gathers_list_t **head);

/** Function responsible for updating gather keys at the end of parsing segy file.
 *  It writes the number_of_traces key(akey) under each gather(dkey).
 *  It writes the object id of the DAOS_ARRAY object holding the traces oids.
 *  Writes the array of OIDS_HDR_traces to the DAOS_ARRAY OBJECT.
 *
 * \param[in]	dfs		pointer to the daos file system.
 * \param[in]	head		pointer to linked list of gathers holding
 * 				all gathers data.
 * \param[in]	object		pointer to seismic object to be updated.
 * \param[in]	dkey_name	string containing the prefix of the gather dkey.
 * \param[in]	akey_name	string containing the number of traces akey.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
update_gather_traces(dfs_t *dfs, gathers_list_t *head, seis_obj_t *object,
		     char *dkey_name, char *akey_name);

/** Function responsible for checking specific shot_id/Cmp_value/Offset_value
 *  of a trace exist in any of the existing gathers under seismic_objects.
 *  It is called to check if the target value exists.
 *  If Yes--> number of traces belonging to this gather is incremented by 1
 *  and the trace header object id is also added.
 *  ELSE--> it returns with false
 *
 * \param[in]	target		target value to be checked if it exists or not.
 * \param[in]	key		string containing the header key of each
 * 				seismic object to check its value.
 * \param[in]	head		pointer to linked list of gathers holding
 * 				all gathers data.
 * \param[in]	trace_obj_id	id of the trace object that will
 * 				check its header value.
 * \param[in]	ntraces		new number of traces to set its value if the
 * 				key value is found in one of the gathers.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
check_key_value(Value target, char *key, gathers_list_t *head,
		daos_obj_id_t trace_obj_id, int *ntraces);

/** Function responsible for creating trace_OIDS array object.
 *  It is called once after preparing linked list of object gathers.
 *  It creates number of array objects equal to the number of gathers
 *  under seismic object.
 *
 * \param[in]	dfs	 	 pointer to daos file system.
 * \param[in]	cid	  	 DAOS object class id
 * 				(pass 0 for default MAX_RW).
 * \param[in]	seis_obj 	 pointer to seismic object, array objects will
 * 				 be created for each gather under this object.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_trace_oids_obj_create(dfs_t* dfs, daos_oclass_id_t cid,
				seis_obj_t *seis_obj, int num_of_gathers);

/** Function responsible for creating seismic gather objects.
 *
 * \param[in]	dfs	 pointer to daos file system.
 * \param[in]	cid	 DAOS object class id (pass 0 for default MAX_RW).
 * \param[in]	parent	 pointer to opened root seismic_object.
 * \param[in]	obj	 pointer holding the address of the pointer
 * 			 to the seimsic object to be allocated.
 * \param[in]	key	 string containing the name of the seismic object,
 * 			 and will be used to find dkey to link this seismic
 * 			 object to the root seismic object.
 * \param[in]	index	 index of the seismic object in the
 * 			 list of keys stored under root.
 *
 * \return      0 on success
 * 		error_code otherwise
 *
 */
int
daos_seis_gather_obj_create(dfs_t* dfs,daos_oclass_id_t cid,
			    seis_root_obj_t *parent, seis_obj_t **obj,
			    char* key, int index);

/** Function responsible for preparing seismic entry with trace header data.
 *  It is called to update/insert trace header data under
 *  specific trace_header_object.
 *
 * \param[in]	tr_obj		pointer to opened trace header object
 *  				to update its header.
 * \param[in]	tr		segy struct holding all headers and data
 *  				values to be written to the trace header.
 *  				(will only extract trace headers
 *  				from this struct)
 * \param[in]	hdrbytes	number of bytes to be updated in trace header
 *  				object(240 bytes as defined in segy.h).
 *
 * \return      0 on success
 *		error_code otherwise
 *
 */
int
daos_seis_trh_update(trace_oid_oh_t* tr_obj, trace_t *tr, int hdrbytes);

/** Function responsible for writing trace data as DAOS_ARRAY under
 *  specific trace data object. It is called to update/insert trace
 *  data under specific trace_data_object.
 *
 *  \param[in]	trace_data_obj	pointer to trace_oid_oh_t struct that holds
 *  				object id and open handle of trace data object.
 *  \param[in]	trace		segy struct holding all headers and data values
 *  				to be written to the trace header object.
 *  				(will only extract traces data from this struct).
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
daos_seis_tr_data_update(trace_oid_oh_t* trace_data_obj, segy *trace);

/** Function responsible for updating gather_TRACE_OIDS object.
 *  It is called mainly at the end of the parsing function and
 *  only stores the traces_hdr_oids
 *
 * \param[in]	trace_data_obj	pointer to trace_oid_oh_t
 * 				struct that holds object id and open
 * 				handle of the trace data object.
 * \param[in]	gather		pointer to linked list of gathers holding
 * 				all gathers data.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_gather_oids_array_update(trace_oid_oh_t* object,
				   seis_gather_t *gather);

/** Function responsible for calculating the object id of trace_data_object
 *  from object_id of trace_header_object. by incrementing the hi bits
 *  of the header object by 1.
 *  It is called before reading from or writing to trace_data_object.
 *
 * \param[in]	tr_hdr	id of the trace header object.
 * \param[in]	cid	DAOS object class id (pass 0 for default MAX_RW).
 *
 * \return		object id of the trace data object.
 *
 */
daos_obj_id_t
get_tr_data_oid(daos_obj_id_t *tr_hdr, daos_oclass_id_t cid);

/** Function responsible for creating trace_header_object & trace_data_object
 * It is called once for each trace while parsing the segy_file.
 *
 * \param[in]	dfs		pointer to daos file system.
 * \param[in]	trace_hdr_obj	pointer holding the address of the pointer
 * 				to the trace header object to be allocated.
 * \param[in]	index		Integer holding the index of the trace object
 * 				to be created.
 * \param[in]	trace		segy struct holding the trace heades and data.
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
daos_seis_tr_obj_create(dfs_t* dfs, trace_obj_t **trace_hdr_obj, int index,
			segy *trace);

/** Function responsible for preparing the seismic entry.
 * It is called before reading from or writing to any seismic object.
 *
 * \param[in]	entry		pointer to the seismic entry to be prepared.
 * \param[in]	oid			object id of the seismic object that will be used.
 * \param[in]	dkey		string containing dkey that will be used in fetching/ updating the seismic object.
 * \param[in]	akey		string containing akey that will be used in fetching/ updating the seismic object.
 * \param[in]	data		byte array to be fetched/updated.
 * \param[in]	size		Integer containing size(number of bytes) to be fetched/updated in the seismic object.
 * \param[in]	iod_type	type of the value accessed in the IO descriptor(dkey/one value to be updated atomically/ or array of records).
 *
 */
void
prepare_seismic_entry(struct seismic_entry *entry, daos_obj_id_t oid,
		      char *dkey, char *akey, char *data,int size,
		      daos_iod_type_t iod_type);

/** Function responsible for updating any gather object.
 *  It is called mainly at the end of the parsing function
 *  while pushing all gather data under specific keys.
 *
 * \param[in]	seis_obj 	pointer to seismic object to be updated.
 * \param[in]	dkey_name	string containing name of the dkey to be used while updating the seismic gather object.
 * \param[in]	akey_name	string containing name of the akey to be used while updating the seismic gather object.
 * \param[in]	data		byte array of data to be written under the akey and dkey of seismic gather object.
 * \param[in]	nbytes		number of bytes to be written to the seimsic gather object.
 * \param[in]	iod_type	type of the value accessed in the IO descriptor(dkey/one value to be updated atomically/ or array of records).
 *
 * \return      0 on success
 * 		error_code otherwise
 */
int
update_gather_object(seis_obj_t *gather_obj, char *dkey_name, char *akey_name,
       		     char *data, int nbytes, daos_iod_type_t type);

/** Function responsible for linking each trace to the seismic object gathers.
 *  It is called once while creating the trace header & data objects.
 *  Also called while replacing objects.
 *  \param[in]	trace_obj	pointer to trace object to be linked to a
 *  				specific object gather.
 *  \param[in]	seis_obj	pointer to the seismic object to which the
 *  				trace will be linked based on the unique
 *  				value of the key.
 *  \param[in]	key		string containing key which will be used to
 *  				get the unique value of the trace.
 *
 *  \return     0 on success
 * 		error_code otherwise
 */
int
daos_seis_tr_linking(trace_obj_t* trace_obj, seis_obj_t *seis_obj, char *key);

/** Function responsible for creating two pipes.
 *  It is called to enable reading and writing directly through the pipe
 *  No need to go for Posix system.
 *
 * \param[in]	fds		array of 2 file descriptors. fd[0] will read
 * 				from child process and the other
 * 				will write to it.
 * \param[in]	command		string containing the command to be executed.
 * \param[in]	argv		array of arguments to be passed
 * 				while executing the command.
 *
 * \return	id of the process running the command.
 *
 */
int
pcreate(int fds[2], const char *command, char *const argv[]);

/** Function responsible for executing command passed in argv
 *
 * \param[in]	argv		array of strings containing command
 * 				and the arguments.
 * \param[in]	write_buffer	byte array to be written.
 * \param[in]	write_bytes	number of bytes to be written.
 * \param[in]	read_buffer	byte array to be read into.
 * \param[in]	read_bytes	number of bytes to be read
 *
 * \return 		number of bytes actually read from the STDOUT of the subprocess.
 *
 */
int
execute_command(char *const argv[], char *write_buffer, int write_bytes,
		char *read_buffer, int read_bytes);

/** Function responsible for converting the trace struct back
 *  to the original segy struct(defined in segy.h)
 *
 * \param[in]	trace	pointer to the trace struct that will be converted
 *
 * \return	segy struct.
 *
 */
segy*
trace_to_segy(trace_t *trace);

/** Function responsible for converting the original segy struct
 * (defined in segy.h) to the trace_t struct(modified segy struct)
 * (defined in daos_seis_internal_functions.h)
 *
 * \param[in]	segy		pointer to the segy struct that will be converted.
 * \param[in]	hdr_oid	object 	id of the header object.
 *
 * \return	trace struct.
 *
 */
trace_t*
segy_to_trace(segy *segy, daos_obj_id_t hdr_oid);

/** Function responsible for fetching traces headers in read_traces struct
 *  It is only used in sorting headers function.
 *
 * \param[in]	coh		opened container handle
 * \param[in]	oids		array holding trace_headers oids to be fetched.
 * \param[in]	traces 		pointer to allocated array traces.
 * \param[in]	daos_mode 	daos object mode(read only or read/write).
 *
 */
void
fetch_traces_header_read_traces(daos_handle_t coh, daos_obj_id_t *oids,
				read_traces *traces, int daos_mode);

/** Function responsible for fetching traces headers to traces list.
 * It is called in get headers and window functions.
 *
 * \param[in]	coh		opened container handle
 * \param[in]	oids		array holding trace_headers oids to be fetched.
 * \param[in]	head_traces 	pointer to linked list of traces.
 * \param[in]	daos_mode 	daos object mode(read only or read/write).
 * \param[in]	num_of_traces	number of traces to fetch headers.
 *
 */
void
fetch_traces_header_traces_list(daos_handle_t coh, daos_obj_id_t *oids,
				traces_list_t **head_traces, int daos_mode,
				int num_of_traces);

/** Function responsible for fetching traces data to traces list
 *  It is called at the end of sorting/ windowing functions.
 *
 *  \param[in]	coh		daos container open handle.
 *  \param[in]	head_traces	pointer to linked list of traces.
 *  \param[in]	daos_mode	array object mode(read only or read/write)
 *
 */
void
fetch_traces_data(daos_handle_t coh, traces_list_t **head_traces,
		  int daos_mode);

/** Function responsible for sorting dkeys in ascending order.
 *  dkeys will be sorted in the array of values.
 *
 *  \param[in]	values		 array of long values that will be set and used
 *  				 to sort dkeys unique values.
 *  \param[in]	number_of_gathers seismic_object number of gathers,
 *  				  size of the array of values.
 *  \param[in]	unique_keys	 array of strings having fetched dkeys.
 *  \param[in]	direction	 direction of sorting
 *  				 (1 >> ascending 0 >> descending)
 */
void
sort_dkeys_list(long *values, int number_of_gathers, char** unique_keys,
		int direction);

/** Function responsible for sorting traces headers
 *  It is called while sorting headers and internally merge sort the headers
 *  based on their key value.
 *
 * \param[in]	gather_traces	pointer to gather array of traces to be sorted.
 * \param[in]	sort_key	array of strings holding primary and secondary
 * 				keys of sorting.
 * \param[in]	direction	direction of sorting(ascending or descending).
 * \param[in]	number_of_keys	number of keys to sort headers on.
 *
 */
void
sort_headers(read_traces *gather_traces, char **sort_key, int *direction, int number_of_keys);

/** Function responsible for sorting two halves of traces headers
 *  based on the direction. It is called while sorting headers.
 *
 *  \param[in]	arr		pointer to array of traces to be sorted.
 *  \param[in]	low		index of the first element of the array(0),
 *  				will be used to set the left position
 *  				to start from.
 *  \param[in]	mid		index of the middle element, will be used
 *  				to set the right position of the array.
 *  \param[in]	high		index of the last element of the array
 *  				(number of traces) that will be merged.
 *  \param[in]	sort_key 	array of string holding the keys which traces
 *  			 	are sorted on.
 *  \param[in]	direction 	direction of sorting, ascending(1) or
 *  				descending(0), will be used while
 *  				merging the two arrays.
 *  \param[in]	num_of_keys	number of keys to sort traces on.
 */
void
Merge(trace_t *arr, int low, int mid, int high, char **sort_key,
      int *direction, int num_of_keys);

/** Function responsible recursively called to split array of traces headers.
 *  It is called only while sorting headers.
 *
 *  \param[in]	arr		pointer to array of traces to be sorted.
 *  \param[in]	low		index of the first element of the array(0),
 *  				will be used to calculate the midpoint
 *  				and split the array.
 *  \param[in]	high		index of the last element of the array
 *  				(number of traces), will be used to calculate
 *  				the midpoint and split the array of traces.
 *  \param[in]	sort_key 	array of string holding the keys which traces
 *  			 	will be sorted on.
 *  \param[in]	direction 	direction of sorting,
 *  				ascending(1) or descending(0).
 *  \param[in]	num_of_keys	number of keys to sort traces on.
 *
 */
void
MergeSort(trace_t *arr, int low, int high, char **sort_key,
	  int *direction, int numof_keys);

/** Function responsible for getting trace header value
 *
 * \param[in]	trace		trace struct.
 * \param[in]	sort_key	Key header to get its value.
 * \param[in]	value		value of the header to be returned.
 *
 */
void
get_header_value(trace_t trace, char *sort_key, Value *value);

/** Function responsible for setting trace header value
 *
 * \param[in]	trace		pointer to trace header struct.
 * \param[in]	sort_key	Key header to set its value.
 * \param[in]	value		value of the header to be set.
 *
 */
void set_header_value(trace_t *trace, char *sort_key, Value *value);

/** Function responsible for getting dkey name given specific key
 *
 * \param[in]	key	string containing header name.
 *
 * \return	string containing the dkey of the header passed.
 */
char*
get_dkey(char *key);

/** Function responsible for calculating and setting trace header value
 *  It is called while setting/ changing traces headers values
 *
 * \param[in]	coh	       container open handle
 * \param[in]	daos_mode      daos object mode(read only or read/write).
 * \param[in[	head	       pointer to linked list of traces.
 * \param[in]   num_of_keys    number of header keys to set their value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in]	keys_2	       array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in]	keys_3         array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in] 	a	       array of doubles containing values on first
 * 			       trace(set_headers case)
 * 			       or overall shift(change headers case)
 * \param[in] 	b	       array of doubles containing increments values
 * 			       within group(set_headers case)
 * 			       or scale on first input key(change headers case)
 * \param[in] 	c	       array of doubles containing group increments
 * 			       (set_headers case) or scale on second input key
 * 			       (change headers case)
 * \param[in] 	d	       array of doubles containing trace number shifts
 * 			       (set_headers case) or overall scale
 * 			       (change headers case)
 * \param[in] 	j	       array of doubles containing number of elements
 * 			       in group (set headers case only)
 * \param[in]	e	       array of doubles containing exponent on first
 * 			       input key(change headers case only)
 * \param[in] 	f	       array of doubles containing exponent on second
 * 			       input keys(change headers case only)
 * \param[in]   type           type of operation requested whether it is set
 * 			       headers or change headers defined in the enum
 * 			       header_operation_type_t
 * 			       (SET_HEADERS/ CHANGE_VALUES).
 */
void
set_traces_header(daos_handle_t coh, int daos_mode, traces_list_t **head,
		  int num_of_keys, char **keys_1, char **keys_2, char **keys_3,
		  double *a, double *b, double *c, double *d, double *e,
		  double *f, double *j, header_operation_type_t type);

/** Function responsible for calculating new trace header value
 *
 * \param[in]	current	       pointer to trace struct to set its header value.
 * \param[in]	keys_1	       array of strings containing header keys
 * 			       to set their header value.
 * \param[in]	keys_2	       array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in]	keys_3         array of strings containing header keys
 * 			       to use their header value while setting
 * 			       keys_1(change headers case)
 * \param[in] 	a	       array of doubles containing values on first
 * 			       trace(set_headers case)
 * 			       or overall shift(change headers case)
 * \param[in] 	b	       array of doubles containing increments values
 * 			       within group(set_headers case)
 * 			       or scale on first input key(change headers case)
 * \param[in] 	c	       array of doubles containing group increments
 * 			       (set_headers case) or scale on second input key
 * 			       (change headers case)
 * \param[in] 	d	       array of doubles containing trace number shifts
 * 			       (set_headers case) or overall scale
 * 			       (change headers case)
 * \param[in] 	j	       array of doubles containing number of elements
 * 			       in group (set headers case only)
 * \param[in]	e	       array of doubles containing exponent on first
 * 			       input key(change headers case only)
 * \param[in] 	f	       array of doubles containing exponent on second
 * 			       input keys(change headers case only)
 * \param[in]   type           type of operation requested whether it is set
 * 			       headers or change headers defined in the enum
 * 			       header_operation_type_t
 * 			       (SET_HEADERS/ CHANGE_VALUES).
 * \param[in]	type_key1	header type of first key.
 * \param[in]	type_key2	header type of second key.
 * \param[in]	type_key3	header type of third key.
 *
 */
void
calculate_new_header_value(traces_headers_t *current, char *key1, char *key2,
			   char *key3, double a, double b, double c, double d,
			   double e, double f, double j, int itr,
			   header_operation_type_t type, cwp_String type_key1,
			   cwp_String type_key2, cwp_String type_key3);

/** Function responsible for windowing traces based on min and max of some keys
 *  It is called while executing daos_seis_window function.
 *
 *  \param[in]	head		pointer to linked list of traces headers.
 *  \param[in]	window_keys	array of strings containing the header keys
 *  				to window on.
 *  \param[in]	num_of_keys	number of keys in the array of window keys.
 *  \param[in]	type		type of window header keys
 *  				as defined in su_helpers.h
 *  \param[in]  min_keys        array of VALUE(su_helpers.h) containing window
 *  				header keys minimum values based on key type.
 *  \param[in]  max_keys        array of VALUE(su_helpers.h) containing window
 *  				header keys maximum values based on key type.
 *
 */
void
window_headers(traces_list_t **head, char **window_keys, int number_of_keys,
	       cwp_String *type, Value *min_keys, Value *max_keys);

/** Function responsible for fetching dkeys under seismic object
 *  and optionally sort dkeys in ascending or descending order.
 *
 *  \param[in]	seismic_object	pointer to opened seismic object,
 *  				will be used to list dkeys.
 *  \param[in]	sort		sorting flag, if set then dkeys will be sorted.
 *  \param[in]	shot_obj	shot_obj flag, it is only used in case of sorting..
 *  \param[in]	cmp_obj		cmp_obj flag, it is only used in case of sorting..
 *  \param[in]	off_obj		off_obj flag, it is only used in case of sorting..
 *  \param[in]	direction	only used in case of sorting to check the direction
 *  				of sorting (ascending or descending)
 *
 *  \return	array of strings containing seismic object dkeys(only gather dkeys).
 *  */
char **
daos_seis_fetch_dkeys(seis_obj_t *seismic_object, int sort, char *key,
		      int direction);

/** Function responsible for destroying existing seismic object and creating new one.
 *  It creates new object after destroying all array objects holding traces headers oids.
 *  It links all traces again to the newly created object.
 *  It is called if the unique value of existing seismic object is changed.
 *
 *  \param[in]	dfs			pointer to DAOS file system
 *  \param[in]	daos_mode		daos object mode
 *  					(read only or read/write).
 *  \param[in]	key			string containing the name of the key
 *  					gather that will be replaced.
 *  \param[in]	trace_list		pointer to linked list of traces with
 *  					new header values. Each trace will be
 *  					linked to gather in the newly
 *  					created seismic object.
 *  \param[in]	root			pointer to opened root seismic object.
 */
void
daos_seis_replace_objects(dfs_t *dfs, int daos_mode, char *key,
			  traces_list_t *trace_list, seis_root_obj_t *root);

/** Function responsible for tokenzing a string given a separator
 *
 * \param[in]	str			void double pointer which will be
 * 					casted to one of the types requested.
 * \param[in]	sep			character separator that will be used
 * 					in tokenizing the string
 * \param[in]	type			integer specifying type the string
 * 					will be casted to (char/ double/ long)
 *
 * \return	array of strings after tokenization
 */
void
tokenize_str(void **str, char *sep, char *string, int type);

/** Function responsible for finding ranges of traces headers.
 *  It is called while executing range headers programs.
 *
 *  \param[in]	trace_list	pointer to linked of traces headers.
 *  \param[in]	number_of_keys	number of keys to find their headers.
 *  \param[in]	keys		array of strings containing keys
 *  				to find their ranges
 *  \param[in]	dim		dim seismic flag.
 *
 *  \return	after finding headers ranges,
 *  		return a struct of all key headers ranges'.
 *
 */
headers_ranges_t
range_traces_headers(traces_list_t *trace_list, int number_of_keys,
		     char **keys, int dim);

/** Function responsible for printing the ranges of the traces headers.
 *  It is called at the end of range traces headers function.
 *
 *  \param[in]	headers_ranges	struct holding min/ max/ first/ last
 *  				traces ranges, array of keys,
 *  				number of keys, shot/rec/cmp coordinates,...
 *
 */
void
print_headers_ranges(headers_ranges_t headers_ranges);

/** Fucntion responsible for storing the unique value in character array
 *  based on its type (char/long/double/int/...)
 *
 * \param[in]	temp		character array to store the unique value in.
 * \param[in]	unique_value	value that will be written in character array.
 * \param[in]	key		string containig the key used,
 * 				will be used to find the data type.
 *
 *  \return	the character array after writing the unique value in it.
 *
 */
void
val_sprintf(char *temp, Value unique_value, char *key);

/** Function responsible for fetching array of traces headers object ids
 *
 * \param[in]	root			pointer to opened seismic root object.
 * \param[in]	oids			pointer to allocated array of
 * 					traces headers oids.
 * \param[in] 	gather_oid_oh		pointer to struct holding object id
 * 					and open handle of the array object.
 * \param[in]	number_of_traces	number of header object ids
 * 					that will be fetched.
 *
 *  \return      0 on success
 *  		 error_code otherwise
 */
int
fetch_array_of_trace_headers(seis_root_obj_t *root, daos_obj_id_t *oids,
			     trace_oid_oh_t *gather_oid_oh,
			     int number_of_traces);

/** Function responsible for releasing allocating list of traces
 *
 * \param[in]	trace_list	pointer to linked list of traces.
 *
 */
void
release_traces_list(traces_list_t *trace_list);


/** Function responsible for creating an empty graph
 *  seismic root object and gather objects.
 *
 * \param[in]   dfs            	pointer to DAOS file system.
 * \param[in]   parent         	pointer to parent DAOS file system object.
 * \param[in]   name          	name of root object that will be create.
 * \param[in]	num_of_keys	Number of strings(keys) in the array of keys.
 * \param[in]	keys		array of strings containing header_keys that
 * 				will be used to create gather objects.
 * \param[in]	root_obj	double pointer to the seismic root object
 * 				that will be allocated.
 * \param[in]	seismic_obj	array of pointers to seismic objects to be allocated.
 *
 */
void
daos_seis_create_graph(dfs_t *dfs, dfs_obj_t *parent, char *name,
		       int num_of_keys, char **keys,
		       seis_root_obj_t **root_obj, seis_obj_t **seismic_obj);

/** Function responsible for reading binary and text headers
 *  \param[in]	bh		pointer to binary header struct to hold
 *  				binary header data read
 *  \param[in]	ebcbuf		pointer to character array to be filled with
 *  				text header data read.
 *  \param[in]	nextended	pointer to variable to hold number of extended
 *  				text headers in segy file.
 *  \param[in]	root_obj	pointer to opened root seismic object.
 *  \param[in]	daos_tape	pointer to daos file struct holding the segy
 *  				file handle after mounting to dfs and offset
 *  				to the last accessed byte defined in
 *  				(dfs_helper_api.h).
 *  \param[in]	swapbhed	integer indicating whether to swap binary
 *  				header bytes or not.
 *  \param[in]	endian		integer indication little or big endian flag.
 *
 */
void
read_headers(bhed *bh, char *ebcbuf, short *nextended,
	     seis_root_obj_t *root_obj,	DAOS_FILE *daos_tape,
	     int swapbhed, int endian);
/** Function responsible for writing binary and text headers under
 *  root seismic object.
 *
 *  \param[in]	bh		pointer to binary header struct to be written
 *  				under root seismic object.
 *  \param[in]	ebcbuf		pointer to character array to be written
 *  				under root seismic object.
 *  \param[in]	root_obj	pointer to opened root seismic object.
 *
 */
void
write_headers(bhed bh, char *ebcbuf, seis_root_obj_t *root_obj);

/** Function responsible for parsing extended text headers from segy file.
 *
 *  \param[in]	nextended	number of extended text headers to be parsed.
 *  \param[in]	daos_tape	pointer to daos file struct holding the segy
 *  				file handle after mounting to dfs and offset
 *  				to the last accessed byte defined in
 *  				(dfs_helper_api.h).
 *  \param[in]	ebcbuf		pointer to character array to hold extended
 *  				text header data read.
 *  \param[in]	root_obj	pointer to opened root seismic object.
 *
 */
void
parse_exth(short nextended, DAOS_FILE *daos_tape, char *ebcbuf,
	   seis_root_obj_t *root_obj);

/** Function responsible for processing headers and calculating length of trace
 *
 * \param[in]	bh		pointer to binary header struct.
 * \param[in]	format		integer flag to specify override format.
 * \param[in]	over		integer flag for binary header float override.
 * \param[in]	format_set	!!
 * \param[in]	trcwt		integer flag for trace weighting.
 * \param[in]	verbose		verbosity flag
 * \param[in]	ns		integer to trace number of samples to be set.
 * \param[in]	nsegy		integer to trace size in bytes to be set.
 *
 */
void
process_headers(bhed *bh, int format, int over,cwp_Bool format_set, int *trcwt,
		int verbose, int *ns, int *nsegy);

/** Function responsible for processing trace.
 *
 * \param[in]	tapetr		tape segy struct holding trace
 * 				identification header.
 * \param[in]	tr		pointer to segy struct, tapetr will be
 * 				converted to trace struct.
 * \param[in]	bh		binary header struct fetched before.
 * \param[in]	ns		trace number of samples
 * \param[in]	swaphdrs	flag for big(1) and little(0) endian to
 * 				indicate whether to swap trace headers or not.
 * \param[in]	nsflag		flag for error in tr.ns
 * \param[in]	itr		current trace number.
 * \param[in]	nkeys		number of keys to be computed.
 * \param[in]	type1		array of types for key1.
 * \param[in]	type2		array of types for key2.
 * \param[in]	ubyte		!!
 * \param[in]	endian		little or big endian.
 * \param[in]	conv		flag for data conversion.
 * \param[in]	swapdata	flag for big(1) and little(0) endian to
 * 				indicate whether to swap trace data or not.
 * \param[in]	index1		array of indexes for key1.
 * \param[in]	trmin		first trace to read.
 * \param[in]	trcwt		flag for trace weighting.
 * \param[in]	verbose		verbosity flag.
 *
 */
void
process_trace(tapesegy tapetr, segy *tr, bhed bh, int ns, int swaphdrs,
	      int nsflag, int *itr, int nkeys, cwp_String *type1,
	      cwp_String *type2, int *ubyte, int endian, int conv,
	      int swapdata, int *index1, int trmin, int trcwt,
	      int verbose);

/** Function responsible for reading all gathers stored under seismic
 *  object in a linked list of gathers.
 *
 *  \param[in]	root		pointer to opened root seismic object.
 *  \param[in]	seis_obj	pointer to opened seismic object.
 */
void
read_object_gathers(seis_root_obj_t *root, seis_obj_t *seis_obj);

#endif /* LSU_SRC_CLIENT_SEIS_DAOS_SEIS_INTERNAL_FUNCTIONS_H_ */
