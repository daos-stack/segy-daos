/*
 * timer.c
 *
 *  Created on: Nov 15, 2020
 *      Author: omar
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "utilities/timer.h"

int
create_timer(dsg_timer_t **t)
{
	if(timer_log == NULL) {
		fclose(fopen("timer_file.txt", "w"));
		timer_log = fopen("timer_file.txt", "a");
		if(timer_log == NULL) {
			DSG_ERROR(-1,"Error in opening timer log file");
			return -1;
		}
	}
	(*t) = (dsg_timer_t*) malloc(sizeof(dsg_timer_t));
	(*t)->running = FALSE;
	(*t)->elapsed_time = 0.0;
	(*t)->func_name = NULL;
	return 0;
}

int
start_timer(dsg_timer_t *t)
{
	if (isrunning(t) == 1) {
		DSG_ERROR(-1,"Can't start already running timer");
		return -1;
	} else {
		gettimeofday(&t->start_time, NULL);
		t->running = TRUE;
	}
	return 0;
}

int
end_timer(dsg_timer_t *t, int reset)
{
	if (isrunning(t) == 0) {
		DSG_ERROR(-1,"Can't stop already stopped timer");
		return -1;
	} else {
		gettimeofday(&(t->end_time), NULL);
		t->elapsed_time += (t->end_time.tv_usec / MICRO
				+ t->end_time.tv_sec)
				- (t->start_time.tv_usec / MICRO
				+ t->start_time.tv_sec);
		t->running = FALSE;
		if(reset == 1) {
			reset_timer(t, NULL);
		}
	}
	return 0;
}

int
set_timer_function(dsg_timer_t *t, char *func_name)
{
	if(func_name != NULL) {
		int length = strlen(func_name);
		t->func_name = malloc(length * sizeof(char));
		strcpy(t->func_name, func_name);
		return 0;
	} else {
		DSG_ERROR(-1,"Function name not initialized");
		return -1;
	}
}

int
isrunning(dsg_timer_t *t)
{
	return t->running;
}

int
print_report(dsg_timer_t *t)
{
	if(t->func_name == NULL){
		fprintf(timer_log,"Timer for unspecified function");
	} else {
		fprintf(timer_log,"Timer for function %s \n", t->func_name);
	}
	fprintf(timer_log,"Elapsed time: %lf seconds\n", t->elapsed_time);
	fprintf(timer_log,"Timer will now reset!\n");
	fprintf(timer_log,"**********************************************\n");
	return 0;
}

int
reset_timer(dsg_timer_t *t, char *func_name)
{
	if (isrunning(t) == 1) {
		DSG_ERROR(-1,"Can't reset running timer");
		return -1;
	} else {
		print_report(t);
		t->running = FALSE;
		set_timer_function(t, func_name);
		t->elapsed_time = 0.0;
		return 0;
	}
}

int
destroy_timer(dsg_timer_t *t)
{
	if(t != NULL) {
		if(t->func_name != NULL) {
			free(t->func_name);
		}
		free(t);
	}
	return 0;
}

//void foo(){
//	for(int i = 0; i < 999999; i++);
//}
//
//int main(){
//	timer* t = create_timer();
//	set_timer_function(t, "foo");
//	for(int i = 0; i < 500; i++){
//		start_timer(t);
//		foo();
//		end_timer(t, FALSE);
//	}
//	reset_timer(t,NULL);
//	printf("%lf",t->elapsed_time);
//	return 0;
//}
