/*
 * sort.c
 *
 *  Created on: Jan 26, 2021
 *      Author: omar
 */

#include "operations/sort.h"

int
int_compare(void *x, void *y, void *d)
{

	int* a = (int*)x;
	int* b = (int*)y;
	int* direction = (int*)d;
	if (*a == *b) {
		return 0;
	} else if (*a < *b) {
		return -1 * *direction;
	} else {
		return 1 * *direction;
	}
}

int
long_compare(void *x, void *y, void *d)
{

	long* a = (long*)x;
	long* b = (long*)y;
	int* direction = (int*)d;
	if (*a == *b) {
		return 0;
	} else if (*a < *b) {
		return -1 * *direction;
	} else {
		return 1 * *direction;
	}
}

void
merge(void *array, int low, int mid, int high, size_t element_size,
      void *sort_props, int(*compare)(void*, void*, void*))
{

	void *temp = malloc((high - low + 1) * element_size);
	int merge_pos = 0, left_pos = low, right_pos = mid + 1;
	int comparison;

	while (left_pos <= mid && right_pos <= high) {
		void *left = array + (left_pos * element_size);
		void *right = array + (right_pos * element_size);
		comparison = (*compare)(left, right, sort_props);
		if (comparison == -1 || comparison == 0) {
			memcpy(temp + element_size * merge_pos++, left,
			       element_size);
			left_pos++;
		} else if (comparison == 1) {
			memcpy(temp + element_size * merge_pos++, right,
			       element_size);
			right_pos++;
		}

	}

	while (left_pos <= mid) {
		memcpy(temp + element_size * merge_pos++,
		       array + element_size * left_pos++,
		       element_size);
	}

	while (right_pos <= high) {
		memcpy(temp + element_size * merge_pos++,
		       array + element_size * right_pos++,
		       element_size);
	}

	for (merge_pos = 0; merge_pos < (high - low + 1); ++merge_pos) {
		memcpy(array + element_size * (low + merge_pos),
		       temp + element_size * merge_pos, element_size);
	}

	free(temp);

}

void
merge_sort(void *array, int low, int high, size_t element_size,
	   void *sort_props, int(*compare)(void*, void*, void*))
{
	int mid;
	if (low < high) {
		mid = (low + high) / 2;
		merge_sort(array, low, mid, element_size, sort_props, compare);
		merge_sort(array, mid + 1, high, element_size, sort_props,
			   compare);
		merge(array, low, mid, high, element_size, sort_props, compare);
	}
}

int
sort(void *array, int num_of_elements, size_t element_size, void *sort_props,
     int(*compare)(void*, void*, void*))
{
	merge_sort(array, 0, num_of_elements - 1, element_size, sort_props,
		   compare);
	return 0;
}
