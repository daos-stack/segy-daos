#include "data_types/trace.h"

int
trace_init(trace_t **tr)
{
	*tr = (trace_t*) malloc(sizeof(trace_t));
	(*tr)->data = NULL;
	return 0;
}

int
init_comp_params(comp_params_t **cp, char *key, generic_value v)
{
	*cp = (comp_params_t*) malloc(sizeof(comp_params_t));
	strcpy((*cp)->key, key);
	(*cp)->val = v;
	return 0;
}

int
init_sort_params(sort_params_t **sp, char **keys, int num_of_keys,
		 int *directions)
{
	*sp = (sort_params_t*) malloc(sizeof(sort_params_t));
	(*sp)->keys = keys;
	(*sp)->num_of_keys = num_of_keys;
	(*sp)->directions = directions;
	return 0;
}

int
trace_destroy(void *trace)
{

	trace_t* tr = (trace_t*) trace;
	if (tr->data != NULL) {
		free(tr->data);
	}

	free(tr);

	return 0;
}

int
getindex(const char *key) /* get index for this key */
{
	register int 	i;
	for (i = 0; i < SEIS_NKEYS; i++)
		if (strcmp(hdr[i].key, key) == 0)
			return i; /* key found */

	/* not found */
	return -1;
}

char*
hdtype(const char *key)
{
	int index = getindex(key);
	if (-1 == (index)){
		char message[50];
		sprintf(message,"%s: key word not in segy.h: %s",
		__FILE__, key);
		DSG_ERROR(-1,message);
		return NULL;
	}

	return hdr[index].type;
}

int
trace_gethval(trace_t *tr, int index, generic_value *valp)
{
	char 	*tp;
	char 	message[50];

	tp = (char*) tr;

	switch (*(hdr[index].type)) {
	case 's':
		(void) strcpy(valp->s, tp + hdr[index].offs);
		break;
	case 'h':
		valp->h = *((short*) (tp + hdr[index].offs));
		break;
	case 'u':
		valp->u = *((unsigned short*) (tp + hdr[index].offs));
		break;
	case 'i':
		valp->i = *((int*) (tp + hdr[index].offs));
		break;
	case 'p':
		valp->p = *((unsigned int*) (tp + hdr[index].offs));
		break;
	case 'l':
		valp->l = *((long*) (tp + hdr[index].offs));
		break;
	case 'v':
		valp->v = *((unsigned long*) (tp + hdr[index].offs));
		break;
	case 'f':
		valp->f = *((float*) (tp + hdr[index].offs));
		break;
	case 'd':
		valp->d = *((double*) (tp + hdr[index].offs));
		break;
	default:
		sprintf(message,"%s: %d: mysterious data type",
		__FILE__, __LINE__);
		DSG_ERROR(-1,message);
		return -1;
		break;
	}

	return 0;
}

int
trace_puthval(trace_t *tr, int index, generic_value *valp)
{
	char 	*tp;
	char 	message[50];

	tp = (char*) tr;

	switch (*(hdr[index].type)) {
	case 's':
		(void) strcpy(tp + hdr[index].offs, valp->s);
		break;
	case 'h':
		*((short*) (tp + hdr[index].offs)) = valp->h;
		break;
	case 'u':
		*((unsigned short*) (tp + hdr[index].offs)) = valp->u;
		break;
	case 'i':
		*((int*) (tp + hdr[index].offs)) = valp->i;
		break;
	case 'p':
		*((unsigned int*) (tp + hdr[index].offs)) = valp->p;
		break;
	case 'l':
		*((long*) (tp + hdr[index].offs)) = valp->l;
		break;
	case 'v':
		*((unsigned long*) (tp + hdr[index].offs)) = valp->v;
		break;
	case 'f':
		*((float*) (tp + hdr[index].offs)) = valp->f;
		break;
	case 'd':
		*((double*) (tp + hdr[index].offs)) = valp->d;
		break;
	default:
		sprintf(message,"%s: %d: mysterious data type",
		__FILE__, __LINE__);
		DSG_ERROR(-1,message);
		return -1;
		break;
	}

	return 0;
}

int
trace_get_header(trace_t trace, char *sort_key, generic_value *value)
{
	int i;
	for (i = 0; i < SEIS_NKEYS; i++) {
		if (strcmp(sort_key, hdr[i].key) == 0) {
			trace_gethval(&trace, i, value);
			break;
		}
	}

	return 0;
}

int
trace_set_header(trace_t *trace, char *sort_key, generic_value *value)
{
	int i;

	for (i = 0; i < SEIS_NKEYS; i++) {
		if (strcmp(sort_key, hdr[i].key) == 0) {
			trace_puthval(trace, i, value);
			break;
		}
	}

	return 0;
}

int
calculate_header_set(trace_t *tr, char *key, int itr, double a, double b,
		     double c, double d, double e)
{
	generic_value 	v;
	double 		f;

	f = (double) itr + d;
	setval(hdtype(key), &v, a, b, c, f, e);
	trace_set_header(tr, key, &v);
	return 0;
}

int
calculate_header_change(trace_t *tr, char *key1, char *key2, char *key3,
			double a, double b, double c, double d, double e,
			double f)
{
	generic_value 	v1;
	generic_value	v2;
	generic_value	v3;

	trace_get_header(*tr, key2, &v2);
	trace_get_header(*tr, key3, &v3);
	changeval(hdtype(key1), &v1, hdtype(key2), &v2, hdtype(key3), &v3, a, b,
			c, d, e, f);
	trace_set_header(tr, key1, &v1);
	return 0;
}

int
trace_check_header(void*comparison, void *trace)
{
	generic_value val2;

	comp_params_t* cp = (comp_params_t*)comparison;

	trace_t* t = (trace_t*)trace;

	trace_get_header(*t, cp->key, &val2);
	return valcmp(hdtype(cp->key), cp->val, val2);
}

trace_t*
trace_search(doubly_linked_list_t *ll, char *key, generic_value val)
{
	comp_params_t cp;
	cp.key = key;
	cp.val = val;
	return doubly_linked_list_search(ll, &cp, &trace_check_header,
			offsetof(trace_t, n));
}

int
trace_delete(doubly_linked_list_t *ll, char *key, generic_value val)
{
	comp_params_t cp;
	cp.key = key;
	cp.val = val;
	return doubly_linked_list_delete(ll, &cp, &trace_check_header,
			&trace_destroy, offsetof(trace_t, n));
}

int
trace_comp(void *first_trace, void *second_trace, void *sp)
{
	generic_value 	v1;
	generic_value	v2;
	int 		comp;
	int 		i = 0;

	trace_t* first = (trace_t*)first_trace;
	trace_t* second = (trace_t*)second_trace;
	sort_params_t* sort_props = (sort_params_t*)sp;

	while (i < sort_props->num_of_keys) {
		trace_get_header(*first, sort_props->keys[i], &v1);
		trace_get_header(*second, sort_props->keys[i], &v2);
		comp = valcmp(hdtype(sort_props->keys[i]), v1, v2)
				* sort_props->directions[i];
		if (comp == 0) {
			i++;
		} else {
			return comp;
		}
	}
	return comp;
}

int
init_wind_params(window_params_t **wp, char **keys, int num_of_keys,
		 generic_value *min, generic_value *max)
{
	*wp = malloc(sizeof(window_params_t));
	(*wp)->keys = keys;
	(*wp)->max_keys = max;
	(*wp)->min_keys = min;
	(*wp)->num_of_keys = num_of_keys;

	return 0;
}
