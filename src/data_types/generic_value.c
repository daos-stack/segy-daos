/*
 * generic_value.c
 *
 *  Created on: Jan 22, 2021
 *      Author: mirnamoawad
 */

#include "data_types/generic_value.h"

int
key_value_pair_init(key_value_pair_t** kv, char** keys, int num_of_traces,
		    int num_of_keys)
{
	int i;
	*kv = malloc(sizeof(key_value_pair_t) * num_of_keys);
	for(i = 0; i < num_of_keys; i++){
		(*kv)[i].key = malloc(strlen(keys[i]) * sizeof(char));
		strcpy((*kv)[i].key, keys[i]);
		(*kv)[i].values = malloc(sizeof(generic_value) * num_of_traces);
		(*kv)[i].num_of_values = num_of_traces;
	}
	return 0;
}

long
vtol(char *type, generic_value val)
{
	switch(*type) {
		case 's': return (long) val.s[0];
		case 'h': return (long) val.h;
		case 'u': return (long) val.u;
		case 'l': return        val.l;
		case 'v': return (long) val.v;
		case 'i': return (long) val.i;
		case 'p': return (long) val.p;
		case 'f': return (long) val.f;
		case 'd': return (long) val.d;
		case 'U': return (long) val.U;
		case 'P': return (long) val.P;
		default: printf("vtol: unknown type %s", type);
			 return 0L;	/* for lint */
	}
}

double
vtod(char *type, generic_value val)
{
	switch(*type) {
		case 's': return (double) val.s[0];
		case 'h': return (double) val.h;
		case 'u': return (double) val.u;
		case 'l': return (double) val.l;
		case 'v': return (double) val.v;
		case 'i': return (double) val.i;
		case 'p': return (double) val.p;
		case 'f': return (double) val.f;
		case 'd': return          val.d;
		case 'U': return (double) val.U;
		case 'P': return (double) val.P;
		default: printf("vtod: unknown type %s", type);
			 return 0.0;	/* for lint */
	}
}

int
valcmp(char *type, generic_value val1, generic_value val2)
{
	switch(*type) {
		case 's': return strcmp(val1.s, val2.s);
		case 'h':
			if      ( val1.h < val2.h ) return -1;
			else if ( val1.h > val2.h ) return  1;
			else                        return  0;
		case 'u':
			if      ( val1.u < val2.u ) return -1;
			else if ( val1.u > val2.u ) return  1;
			else                        return  0;
		case 'l':
			if      ( val1.l < val2.l ) return -1;
			else if ( val1.l > val2.l ) return  1;
			else                        return  0;
		case 'v':
			if      ( val1.v < val2.v ) return -1;
			else if ( val1.v > val2.v ) return  1;
			else                        return  0;
		case 'i':
			if      ( val1.i < val2.i ) return -1;
			else if ( val1.i > val2.i ) return  1;
			else                        return  0;
		case 'p':
			if      ( val1.p < val2.p ) return -1;
			else if ( val1.p > val2.p ) return  1;
			else                        return  0;
		case 'f':
			if      ( val1.f < val2.f ) return -1;
			else if ( val1.f > val2.f ) return  1;
			else                        return  0;
		case 'd':
			if      ( val1.d < val2.d ) return -1;
			else if ( val1.d > val2.d ) return  1;
			else                        return  0;
		case 'U':
			if      ( val1.U < val2.U ) return -1;
			else if ( val1.U > val2.U ) return  1;
			else                        return  0;
		case 'P':
			if      ( val1.P < val2.P ) return -1;
			else if ( val1.P > val2.P ) return  1;
			else                        return  0;
		default: printf("valcmp: unknown type %s", type);
			 return 0;	/* for lint */
	}
}

int
printfval(char *type, generic_value val)
{
	switch(*type) {
	case 's':
		(void) printf("%s", val.s);
	break;
	case 'h':
		(void) printf("%d", val.h);
	break;
	case 'u':
		(void) printf("%d", val.u);
	break;
	case 'i':
		(void) printf("%d", val.i);
	break;
	case 'p':
		(void) printf("%d", val.p);
	break;
	case 'l':
		(void) printf("%ld", val.l);
	break;
	case 'v':
		(void) printf("%ld", val.v);
	break;
	case 'f':
		(void) printf("%f", val.f);
	break;
	case 'd':
		(void) printf("%f", val.d);
	break;
	case 'U':
		(void) printf("%d", val.U);
	break;
	case 'P':
		(void) printf("%d", val.P);
	break;
	default:
		printf("printfval: unknown type %s", type);
	}

	return 0;
}

int
val_sprintf(char *temp, generic_value unique_value, char *type)
{

	switch (*type) {
	case 's':
		(void) sprintf(temp, "%s", unique_value.s);
		break;
	case 'h':
		(void) sprintf(temp, "%d", unique_value.h);
		break;
	case 'u':
		(void) sprintf(temp, "%d", unique_value.u);
		break;
	case 'i':
		(void) sprintf(temp, "%d", unique_value.i);
		break;
	case 'p':
		(void) sprintf(temp, "%d", unique_value.p);
		break;
	case 'l':
		(void) sprintf(temp, "%ld", unique_value.l);
		break;
	case 'v':
		(void) sprintf(temp, "%ld", unique_value.v);
		break;
	case 'f':
		(void) sprintf(temp, "%f", unique_value.f);
		break;
	case 'd':
		(void) sprintf(temp, "%f", unique_value.d);
		break;
	case 'U':
		(void) sprintf(temp, "%d", unique_value.U);
		break;
	case 'P':
		(void) sprintf(temp, "%d", unique_value.P);
		break;
	default:
		printf("fprintfval: unknown type %s", type);
	}

	return 0;

}

int
setval(char *type, generic_value *valp, double a, double b,
       double c, double i, double j)
{
	switch (*type) {
	case 's':
		printf("can't set char header word");
	break;
	case 'h':
		valp->h = a + b * mod(i, j) + c * ((int) (i/j));
	break;
	case 'u':
		valp->u = a + b * mod(i, j) + c * ((int) (i/j));
	break;
	case 'l':
		valp->l = (long) (a + b * mod(i, j) + c * ((int) (i/j)));
	break;
	case 'v':
		valp->v = (unsigned long)(a + b * mod(i, j) + c *((int) (i/j)));
	break;
	case 'i':
		valp->i = a + b * mod(i, j) + c * ((int) (i/j));
	break;
	case 'p':
		valp->p = a + b * mod(i, j) + c * ((int) (i/j));
	break;
	case 'f':
		valp->f = a + b * mod(i, j) + c * ((int) (i/j));
	break;
	case 'd':
		valp->d = a + b * mod(i, j) + c * ((int) (i/j));
	break;
	default:
		printf("unknown type %s", type);
	break;
	}
	return 0;
}

/** Can be moved later to operations module... */
double
mod(double x, double y)	/* As defined in Knuth, vol. 1	*/
{
	return y == 0.0 ? x : x - y * floor(x/y);
}

int
changeval(char *type1, generic_value *valp1, char *type2,
	  generic_value *valp2, char *type3, generic_value *valp3,
	  double a, double b, double c, double d, double e, double f)
{
	double dval2=vtod( type2, *valp2);
	double dval3=vtod( type3, *valp3);
	double dval1=(a+b*pow(dval2,e)+c*pow(dval3,f))/d;

	switch (*type1) {
	case 's':
		printf("can't change char header word");
	break;
	case 'h':
		valp1->h = (short) dval1;
	break;
	case 'u':
		valp1->u = (unsigned short) dval1;
	break;
	case 'l':
		valp1->l = (long) dval1;
	break;
	case 'v':
		valp1->v = (unsigned long) dval1;
	break;
	case 'i':
		valp1->i = (int) dval1;
	break;
	case 'p':
		valp1->p = (unsigned int) dval1;
	break;
	case 'f':
		valp1->f = (float) dval1;
	break;
	case 'd':
		valp1->d = (double) dval1;
	break;
	default:
		printf("unknown type %s", type1);
	break;
	}

	return 0;
}

int
atoval(char *type, char *keyval, generic_value *valp)
{
	switch(*type) {
	case 's':
		(void) strcpy(valp->s, keyval);
	break;
	case 'h':
		valp->h = eatoh(keyval);
	break;
	case 'u':
		valp->u = eatou(keyval);
	break;
	case 'i':
		valp->i = eatoi(keyval);
	break;
	case 'p':
		valp->p = eatop(keyval);
	break;
	case 'l':
		valp->l = eatol(keyval);
	break;
	case 'v':
		valp->v = eatov(keyval);
	break;
	case 'f':
		valp->f = eatof(keyval);
	break;
	case 'd':
		valp->d = eatod(keyval);
	break;
	default:
		printf("%s: %d: mysterious data type: %s",
					__FILE__, __LINE__, keyval);
	break;
	}

	return 0;
}

short
eatoh(char *s)
{
	long n = strtol(s, NULL, 10);

	if ( (n > SHRT_MAX) || (n < SHRT_MIN) || (errno == ERANGE) ){
		char message[50];
		sprintf(message,"%s: eatoh: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return (short) n;
}

unsigned short
eatou(char *s)
{
	unsigned long n = strtoul(s, NULL, 10);

	if ( (n > USHRT_MAX) || (errno == ERANGE) ){
		char message[50];
		sprintf(message,"%s: eatou: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return (unsigned short) n;
}

int
eatoi(char *s)
{
	long n = strtol(s, NULL, 10);

	if ( (n > INT_MAX) || (n < INT_MIN) || (errno == ERANGE) ){
		char message[50];
		sprintf(message,"%s: eatoi: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return (int) n;
}

unsigned int
eatop(char *s)
{
	unsigned long n = strtoul(s, NULL, 10);

	if ( (n > UINT_MAX) || (errno == ERANGE) ){
		char message[50];
		sprintf(message,"%s: eatop: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return (unsigned int) n;
}


long
eatol(char *s)
{
	long n = strtol(s, NULL, 10);

	if (errno == ERANGE){
		char message[50];
		sprintf(message,"%s: eatol: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return n;
}


unsigned long
eatov(char *s)
{
	unsigned long n = strtoul(s, NULL, 10);

	if (errno == ERANGE){
		char message[50];
		sprintf(message,"%s: eatov: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return n;
}


float
eatof(char *s)
{
	float x = strtod(s, NULL);

	if ( (x > FLT_MAX) || (x < -FLT_MAX) || (errno == ERANGE) ){
		char message[50];
		sprintf(message,"%s: eatof: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return (float) x;
}

double
eatod(char *s)
{
	double x = strtod(s, NULL);

	if ( (errno == ERANGE) || (x > DBL_MAX) || (x < -DBL_MAX) ){
		char message[50];
		sprintf(message,"%s: eatod: overflow", __FILE__);
		DSG_ERROR(-1,message);
		return -1;
	}

	return x;
}

size_t
key_get_size(char *type)
{
	switch (*type) {
	case 's':
		return sizeof(char*);
	case 'h':
		return sizeof(short);
	case 'u':
		return sizeof(unsigned short);
	case 'l':
		return sizeof(long);
	case 'v':
		return sizeof(unsigned long);
	case 'i':
		return sizeof(int);
	case 'p':
		return sizeof(unsigned int);
	case 'f':
		return sizeof(float);
	case 'd':
		return sizeof(double);
	case 'U':
		return sizeof(unsigned short int);
	case 'P':
		return sizeof(unsigned long int);
	default:
		printf("get_size: unknown type %s", type);
		return 0; /* for lint */
	}
}

int
generic_value_compare(void *v1, void *v2,
		      void* sort_params)
{

	generic_value* val1 = (generic_value*)v1;
	generic_value* val2 = (generic_value*)v2;
	generic_value_sort_params_t* sp = (generic_value_sort_params_t*)sort_params;
	return valcmp(sp->type, *val1, *val2) * sp->direction;
}

generic_value_sort_params_t*
init_generic_value_sort_params(char* type, int direction)
{
	generic_value_sort_params_t* sp = malloc(sizeof(generic_value_sort_params_t));
	sp->type = type;
	sp->direction = direction;
	return sp;
}

int
generic_value_init(char* type, void* buff, generic_value* val)
{
	switch (*type) {
	case 's':
		strcpy(val->s,(char*)buff);
		break;
	case 'h':
		val->h = *((short int*)buff);
		break;
	case 'u':
		val->u = *((unsigned short*)buff);
		break;
	case 'l':
		val->l = *((long*)buff);
		break;
	case 'v':
		val->v = *((unsigned long*)buff);
		break;
	case 'i':
		val->i = *((int*)buff);
		break;
	case 'p':
		val->p = *((unsigned int*)buff);
		break;
	case 'f':
		val->f = *((float*)buff);
		break;
	case 'd':
		val->d = *((double*)buff);
		break;
	case 'U':
		val->U = *((unsigned short int*)buff);
		break;
	case 'P':
		val->P = *((unsigned long int*)buff);
		break;
	default:
		printf("init: unknown type %s", type); /* for lint */
	}

	return 0;
}

