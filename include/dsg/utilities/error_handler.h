/*
 * error_handler.h
 *
 *  Created on: Mar 8, 2021
 *      Author: omar
 */

#ifndef DAOS_SEISMIC_GRAPH_INCLUDE_DSG_UTILITIES_ERROR_HANDLER_H_
#define DAOS_SEISMIC_GRAPH_INCLUDE_DSG_UTILITIES_ERROR_HANDLER_H_

#define GET_MACRO(_1,_2,_3,_4,NAME,...) NAME
#define warn(...) GET_MACRO(__VA_ARGS__, warn4,warn3, warn2, warn1)(__VA_ARGS__)
#define warn1(x) fprintf(stderr,x)
#define warn2(x,y) fprintf(stderr,x,y)
#define warn3(x,y,z) fprintf(stderr,x,y,z)
#define warn4(x,y,z,u) fprintf(stderr,x,y,z,u)

#define err(...) GET_MACRO(__VA_ARGS__, err4, err3, err2, err1)(__VA_ARGS__)
#define err1(x) fprintf(stderr,x);exit(0)
#define err2(x,y) fprintf(stderr,x,y);exit(0)
#define err3(x,y,z) fprintf(stderr,x,y,z);exit(0)
#define err4(x,y,z,u) fprintf(stderr,x,y,z,u);exit(0)
static int verbosity = 0;

#define	GET_ERROR(_1,_2,_3,NAME,...) NAME
#define DSG_ERROR(...) GET_ERROR(__VA_ARGS__,DSG_ERROR2,DSG_ERROR1)(__VA_ARGS__)
#define DSG_ERROR2(rc, message, label)									\
	do{																	\
		if(rc == 0){													\
			if(verbosity){ 												\
			      warn("%s : Finished Successfully...\n", message); 	\
			        }													\
		} else {														\
			warn("%s : Error in call with value %d on line %d ", message, rc,__LINE__);\
			warn("in function %s in file %s \n", __func__, __FILE__);    \
			goto label;													\
	 }}while(0)

#define DSG_ERROR1(rc, message) 										\
	do{																	\
		if(rc == 0){													\
			if(verbosity){ 												\
			      warn("%s : Finished Successfully...\n", message); 	\
			        }													\
		} else {														\
			warn("%s : Error in call with value %d on line %d ", message, rc,__LINE__);	\
			warn("in function %s in file %s \n", __func__, __FILE__);    \
	 }}while(0)

#endif /* DAOS_SEISMIC_GRAPH_INCLUDE_DSG_UTILITIES_ERROR_HANDLER_H_ */
