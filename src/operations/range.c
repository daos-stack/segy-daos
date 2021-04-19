/*
 * range.c
 *
 *  Created on: Jan 30, 2021
 *      Author: omar
 */

#include "operations/range.h"

int
headers_ranges_init(headers_ranges_t **headers_ranges)
{

	*headers_ranges = (headers_ranges_t*) malloc(sizeof(headers_ranges_t));

	(*headers_ranges)->trmin = NULL;
	(*headers_ranges)->trmax = NULL;
	(*headers_ranges)->trlast = NULL;
	(*headers_ranges)->trfirst = NULL;
	(*headers_ranges)->keys = NULL;

	return 0;
}

int
ensemble_range(ensemble_list *e, headers_ranges_t *rng)
{
	generic_value 	val;
	generic_value 	valmin;
	generic_value 	valmax;
	trace_t 	*trmin;
	trace_t 	*trmax;
	trace_t 	*trfirst;
	trace_t 	*trlast;
	double 		east_shot[2];
	double 		west_shot[2];
	double 		north_shot[2];
	double 		south_shot[2];
	double 		east_rec[2];
	double 		west_rec[2];
	double 		north_rec[2];
	double 		south_rec[2];
	double 		east_cmp[2];
	double 		west_cmp[2];
	double 		north_cmp[2];
	double 		south_cmp[2];
	double 		dcoscal = 1.0;
	double 		sx = 0.0;
	double 		sy = 0.0;
	double 		gx = 0.0;
	double 		gy = 0.0;
	double 		mx = 0.0;
	double 		my = 0.0;
	double 		mx1 = 0.0;
	double 		my1 = 0.0;
	double 		mx2 = 0.0;
	double 		my2 = 0.0;
	double 		dm = 0.0;
	double 		dmin = 0.0;
	double 		dmax = 0.0;
	double 		davg = 0.0;
	int 		coscal = 1;
	int 		i;

	north_shot[0] = south_shot[0] = east_shot[0] = west_shot[0] = 0.0;
	north_shot[1] = south_shot[1] = east_shot[1] = west_shot[1] = 0.0;
	north_rec[0] = south_rec[0] = east_rec[0] = west_rec[0] = 0.0;
	north_rec[1] = south_rec[1] = east_rec[1] = west_rec[1] = 0.0;
	north_cmp[0] = south_cmp[0] = east_cmp[0] = west_cmp[0] = 0.0;
	north_cmp[1] = south_cmp[1] = east_cmp[1] = west_cmp[1] = 0.0;

	trmin = malloc(sizeof(trace_t));
	trmin->data = NULL;
	trmax = malloc(sizeof(trace_t));
	trmax->data = NULL;
	trfirst = malloc(sizeof(trace_t));
	trfirst->data = NULL;
	trlast = malloc(sizeof(trace_t));
	trlast->data = NULL;

	ensemble_t *first_ensemble, *last_ensemble;
	trace_t *first_trace, *last_trace;
	first_ensemble = doubly_linked_list_get_object(e->ensembles->head,
						       offsetof(ensemble_t, n));
	first_trace = doubly_linked_list_get_object(first_ensemble->traces->head,
			                            offsetof(trace_t, n));

	if (rng->number_of_keys == 0) {
		for (i = 0; i < SEIS_NKEYS; i++){
			trace_get_header(*first_trace, rng->keys[i], &val);
			trace_set_header(trmin, rng->keys[i], &val);
			trace_set_header(trmax, rng->keys[i], &val);
			trace_set_header(trfirst, rng->keys[i], &val);

			if (i == 20) {
				coscal = val.h;
				if (coscal == 0) {
					coscal = 1;
				} else if (coscal > 0) {
					dcoscal = 1.0 * coscal;
				} else {
					dcoscal = 1.0 / coscal;
				}
			} else if (i == 21) {
				sx = east_shot[0] = west_shot[0] =
				north_shot[0] = south_shot[0] =
				val.i * dcoscal;
			} else if (i == 22) {
				sy = east_shot[1] = west_shot[1] =
				north_shot[1] = south_shot[1] =
				val.i * dcoscal;
			} else if (i == 23) {
				gx = east_rec[0] = west_rec[0] = north_rec[0] =
				south_rec[0] = val.i * dcoscal;
			} else if (i == 24) {
				gy = east_rec[1] = west_rec[1] = north_rec[1] =
				south_rec[1] = val.i * dcoscal;
			} else{
				continue;
			}
		}
	} else {
		for (i = 0; i < rng->number_of_keys; i++) {
			trace_get_header(*first_trace, rng->keys[i], &val);
			trace_set_header(trmin, rng->keys[i], &val);
			trace_set_header(trmax, rng->keys[i], &val);
			trace_set_header(trfirst, rng->keys[i], &val);
		}
	}
	if (rng->number_of_keys == 0) {
		mx = east_cmp[0] = west_cmp[0] = north_cmp[0] = south_cmp[0] =
		0.5 * (east_shot[0] + east_rec[0]);
		my = east_cmp[1] = west_cmp[1] = north_cmp[1] = south_cmp[1] =
		0.5 * (east_shot[1] + east_rec[1]);
	}

	ensemble_t 	*temp_ensemble;
	trace_t 	*temp_trace;
	node_t 		*ensemble_node;
	node_t		*trace_node;
	int 		ensembles_num;
	int 		traces_num;
	int 		ntr = 0;

	ensembles_num = e->ensembles->size;
	ensemble_node = e->ensembles->head;

	for (int en = 0; en < ensembles_num; en++) {
		temp_ensemble =
			doubly_linked_list_get_object(ensemble_node,
						      offsetof(ensemble_t, n));
		traces_num = temp_ensemble->traces->size;
		trace_node = temp_ensemble->traces->head;
		for (int tr = 0; tr < traces_num; tr++) {
			temp_trace =
				doubly_linked_list_get_object(trace_node,
						      	      offsetof(trace_t, n));
			sx = sy = gx = gy = mx = my = 0.0;
			if (rng->number_of_keys == 0) {
				for (i = 0; i < SEIS_NKEYS; i++) {
					trace_get_header(*temp_trace,
							 rng->keys[i], &val);
					trace_get_header(*trmin, rng->keys[i],
							 &valmin);
					trace_get_header(*trmax, rng->keys[i],
							 &valmax);

					if (valcmp(hdr[i].type, val, valmin)< 0){
						trace_set_header(trmin,
								rng->keys[i],
								&val);
					} else if (valcmp(hdr[i].type, val, valmax) > 0){
						trace_set_header(trmax,
								rng->keys[i],
								&val);
					}

					trace_set_header(trlast, rng->keys[i],
							&val);

					if (i == 20) {
						coscal = val.h;
						if (coscal == 0){
							coscal = 1;
						} else if (coscal > 0){
							dcoscal = 1.0 * coscal;
						} else{
							dcoscal = 1.0 / coscal;
						}
					} else if (i == 21) {
						sx = val.i * dcoscal;
					} else if (i == 22) {
						sy = val.i * dcoscal;
					} else if (i == 23) {
						gx = val.i * dcoscal;
					} else if (i == 24) {
						gy = val.i * dcoscal;
					} else{
						continue;
					}
				}
			} else {
				for (i = 0; i < rng->number_of_keys; i++) {
					trace_get_header(*temp_trace,
							 rng->keys[i], &val);
					trace_get_header(*trmin, rng->keys[i],
							 &valmin);
					trace_get_header(*trmax, rng->keys[i],
							 &valmax);
					if (valcmp(hdtype(rng->keys[i]), val,
					    valmin) < 0) {
						trace_set_header(trmin,
								 rng->keys[i],
								 &val);
					} else if (valcmp(hdtype(rng->keys[i]),
						   val, valmax) > 0) {
						trace_set_header(trmax,
								 rng->keys[i],
								 &val);
					}
					trace_set_header(trlast, rng->keys[i],
							 &val);
				}
			}

			if (rng->number_of_keys == 0) {
				mx = 0.5 * (sx + gx);
				my = 0.5 * (sy + gy);
				if (east_shot[0] < sx) {
					east_shot[0] = sx;
					east_shot[1] = sy;
				}
				if (west_shot[0] > sx) {
					west_shot[0] = sx;
					west_shot[1] = sy;
				}
				if (north_shot[1] < sy) {
					north_shot[0] = sx;
					north_shot[1] = sy;
				}
				if (south_shot[1] > sy) {
					south_shot[0] = sx;
					south_shot[1] = sy;
				}
				if (east_rec[0] < gx) {
					east_rec[0] = gx;
					east_rec[1] = gy;
				}
				if (west_rec[0] > gx) {
					west_rec[0] = gx;
					west_rec[1] = gy;
				}
				if (north_rec[1] < gy) {
					north_rec[0] = gx;
					north_rec[1] = gy;
				}
				if (south_rec[1] > gy) {
					south_rec[0] = gx;
					south_rec[1] = gy;
				}
				if (east_cmp[0] < mx) {
					east_cmp[0] = mx;
					east_cmp[1] = my;
				}
				if (west_cmp[0] > mx) {
					west_cmp[0] = mx;
					west_cmp[1] = my;
				}
				if (north_cmp[1] < my) {
					north_cmp[0] = mx;
					north_cmp[1] = my;
				}
				if (south_cmp[1] > my) {
					south_cmp[0] = mx;
					south_cmp[1] = my;
				}
			}

			if (ntr == 1) {
				/** get midpoint (mx1,my1) on trace 1 */
				mx1 = 0.5 * (temp_trace->sx + temp_trace->gx);
				my1 = 0.5 * (temp_trace->sy + temp_trace->gy);
			} else if (ntr == 2) {
				/** get midpoint (mx2,my2) on trace 2 */
				mx2 = 0.5 * (temp_trace->sx + temp_trace->gx);
				my2 = 0.5 * (temp_trace->sy + temp_trace->gy);
				/** midpoint interval between traces 1 and 2 */
				dm =sqrt((mx1 - mx2)* (mx1- mx2)+
					 (my1- my2)* (my1- my2));
				/** set min, max and avg midpoint
				 * interval holders */
				dmin = dm;
				dmax = dm;
				davg = (dmin + dmax) / 2.0;
				/* hold this midpoint */
				mx1 = mx2;
				my1 = my2;
			} else if (ntr > 2) {
				/** get midpoint (mx,my) on this trace */
				mx2 = 0.5 * (temp_trace->sx + temp_trace->gx);
				my2 = 0.5 * (temp_trace->sy + temp_trace->gy);
				/** get midpoint (mx,my) between this
				 * and previous trace
				 */
				dm =sqrt((mx1 - mx2)* (mx1- mx2)+
					 (my- my2)* (my1- my2));
				/** reset min, max and avg midpoint
				 * interval holders,
				 *  if needed
				 */
				if (dm < dmin)
					dmin = dm;
				if (dm > dmax)
					dmax = dm;
				davg = (davg + (dmin + dmax) / 2.0) / 2.0;
				/* hold this midpoint */
				mx1 = mx2;
				my1 = my2;
			}
			ntr++;
			trace_node = trace_node->next;
		}
		ensemble_node = ensemble_node->next;

	}

	rng->east_cmp[0] = east_cmp[0];
	rng->east_cmp[1] = east_cmp[1];
	rng->east_rec[0] = east_rec[0];
	rng->east_rec[1] = east_rec[1];
	rng->east_shot[0] = east_shot[0];
	rng->east_shot[1] = east_shot[1];
	rng->north_cmp[0] = north_cmp[0];
	rng->north_cmp[1] = north_cmp[1];
	rng->north_rec[0] = north_rec[0];
	rng->north_rec[1] = north_rec[1];
	rng->north_shot[0] = north_shot[0];
	rng->north_shot[1] = north_shot[1];
	rng->south_cmp[0] = south_cmp[0];
	rng->south_cmp[1] = south_cmp[1];
	rng->south_rec[0] = south_rec[0];
	rng->south_rec[1] = south_rec[1];
	rng->south_shot[0] = south_shot[0];
	rng->south_shot[1] = south_shot[1];
	rng->west_cmp[0] = west_cmp[0];
	rng->west_cmp[1] = west_cmp[1];
	rng->west_rec[0] = west_rec[0];
	rng->west_rec[1] = west_rec[1];
	rng->west_shot[0] = west_shot[0];
	rng->west_rec[1] = west_rec[1];
	rng->trfirst = trfirst;
	rng->trlast = trlast;
	rng->trmax = trmax;
	rng->trmin = trmin;
	rng->davg = davg;
	rng->dmax = dmax;
	rng->dmin = dmin;
	rng->ntr = ntr;

	return 0;
}

int
print_headers_ranges(headers_ranges_t *headers_ranges)
{
	generic_value 	valmin;
	generic_value 	valmax;
	generic_value 	valfirst;
	generic_value 	vallast;
	double 		dvalmin;
	double 		dvalmax;
	char 		*key;
	char 		*type;
	int 		kmax;
	int 		i;

	if (headers_ranges->number_of_keys == 0){
		kmax = SEIS_NKEYS;
	} else {
		kmax = headers_ranges->number_of_keys;
	}

	printf("%d traces: \n", headers_ranges->ntr);

	for (i = 0; i < kmax; i++) {
		type = hdtype(headers_ranges->keys[i]);
		trace_get_header(*headers_ranges->trmin,
				headers_ranges->keys[i], &valmin);
		trace_get_header(*headers_ranges->trmax,
				headers_ranges->keys[i], &valmax);
		trace_get_header(*headers_ranges->trfirst,
				headers_ranges->keys[i], &valfirst);
		trace_get_header(*headers_ranges->trlast,
				headers_ranges->keys[i], &vallast);

		dvalmin = vtod(hdtype(headers_ranges->keys[i]), valmin);
		dvalmax = vtod(hdtype(headers_ranges->keys[i]), valmax);
		if (dvalmin || dvalmax) {
			if (dvalmin < dvalmax) {
				printf("%s ", headers_ranges->keys[i]);
				printfval(hdtype(headers_ranges->keys[i]),
						valmin);
				printf(" ");
				printfval(hdtype(headers_ranges->keys[i]),
						valmax);
				printf(" (");
				printfval(hdtype(headers_ranges->keys[i]),
						valfirst);
				printf(" - ");
				printfval(hdtype(headers_ranges->keys[i]),
						vallast);
				printf(")");
			} else {
				printf("%s ", headers_ranges->keys[i]);
				printfval(hdtype(headers_ranges->keys[i]),
						valmin);
			}
			printf("\n");
		}
	}

	if (headers_ranges->number_of_keys == 0) {
		if ((headers_ranges->north_shot[1] != 0.0) ||
		    (headers_ranges->south_shot[1] != 0.0) ||
		    (headers_ranges->east_shot[0] != 0.0) ||
		    (headers_ranges->west_shot[0] != 0.0)) {
			printf("\nShot coordinate limits:\n" "\tNorth(%g,%g)"
				" South(%g,%g) East(%g,%g) West(%g,%g)\n",
				headers_ranges->north_shot[0],
				headers_ranges->north_shot[1],
				headers_ranges->south_shot[0],
				headers_ranges->south_shot[1],
				headers_ranges->east_shot[0],
				headers_ranges->east_shot[1],
				headers_ranges->west_shot[0],
				headers_ranges->west_shot[1]);
		}
		if ((headers_ranges->north_rec[1] != 0.0) ||
		    (headers_ranges->south_rec[1] != 0.0) ||
		    (headers_ranges->east_rec[0] != 0.0)  ||
		    (headers_ranges->west_rec[0] != 0.0)) {
			printf("\nReceiver coordinate limits:\n"
				"\tNorth(%g,%g) South(%g,%g) East(%g,%g)"
				" West(%g,%g)\n",
				headers_ranges->north_rec[0],
				headers_ranges->north_rec[1],
				headers_ranges->south_rec[0],
				headers_ranges->south_rec[1],
				headers_ranges->east_rec[0],
				headers_ranges->east_rec[1],
				headers_ranges->west_rec[0],
				headers_ranges->west_rec[1]);
		}
		if ((headers_ranges->north_cmp[1] != 0.0) ||
		    (headers_ranges->south_cmp[1] != 0.0) ||
		    (headers_ranges->east_cmp[0] != 0.0)  ||
		    (headers_ranges->west_cmp[0] != 0.0)) {
			printf("\nMidpoint coordinate limits:\n"
			       "\tNorth(%g,%g) South(%g,%g) East(%g,%g)"
				" West(%g,%g)\n",
				headers_ranges->north_cmp[0],
				headers_ranges->north_cmp[1],
				headers_ranges->south_cmp[0],
				headers_ranges->south_cmp[1],
				headers_ranges->east_cmp[0],
				headers_ranges->east_cmp[1],
				headers_ranges->west_cmp[0],
				headers_ranges->west_cmp[1]);
		}
	}

	if (headers_ranges->dim != 0) {
		if (headers_ranges->dim == 1) {
			printf("\n2D line: \n");
			printf("Min CMP interval = %g ft\n",
				headers_ranges->dmin);
			printf("Max CMP interval = %g ft\n",
				headers_ranges->dmax);
			printf("Line length = %g miles (using avg CMP"
				" interval of %g ft)\n",
				headers_ranges->davg* headers_ranges->ntr/ 5280,
				headers_ranges->davg);
		} else if (headers_ranges->dim == 2) {
			printf("ddim line: \n");
			printf("Min CMP interval = %g m\n",
				headers_ranges->dmin);
			printf("Max CMP interval = %g m\n",
				headers_ranges->dmax);
			printf("Line length = %g km (using avg CMP interval"
				" of %g m)\n",
				headers_ranges->davg* headers_ranges->ntr/ 1000,
				headers_ranges->davg);
		}
	}
	return 0;
}

int
destroy_header_ranges(headers_ranges_t *rng)
{
	int i;
	if (rng->trfirst != NULL) {
		trace_destroy(rng->trfirst);
	}
	if (rng->trlast != NULL) {
		trace_destroy(rng->trlast);
	}
	if (rng->trmin != NULL) {
		trace_destroy(rng->trmin);
	}
	if (rng->trmax != NULL) {
		trace_destroy(rng->trmax);
	}
	free(rng);

	return 0;
}
