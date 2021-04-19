/*
 * timerlib.h
 *
 *  Created on: Nov 15, 2020
 *      Author: omar
 */
#ifndef TIMER_LIBRARY_H
#define TIMER_LIBRARY_H
#include <sys/time.h>
#include <stdio.h>
#include "error_handler.h"
#define TRUE 1
#define FALSE 0
#define MICRO 1000000.0
static FILE* timer_log;

typedef struct timer{
    struct timeval start_time;
    struct timeval end_time;
    char* func_name;
    int running;
    double elapsed_time;
}dsg_timer_t;

/** Function responsible for initializing timer.
 *  If the timer isn't already running, the timer's elapsed time is set to 0.
 *
 * \param[in]	timer		uninitialized timer or timer to be reset
 * \param[in]	func_name	name of function to be timed. This is ignored
 *                              if reset flag is not set
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
reset_timer(dsg_timer_t *t, char* func_name);

/** Function responsible for setting function name of timer to be displayed
 *  later in the report.
 *
 * \param[in]	timer		initialized timer object.
 * \param[in]	func_name	name of function to be timed.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
set_timer_function(dsg_timer_t *t, char* func_name);

/** Function responsible for starting timer. Responsible for recording
 *  the starting time.
 *
 * \param[in]	timer		initialized timer object.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
start_timer(dsg_timer_t *t);

/** Function responsible for ending timer. Responsible for recording
 *  the ending time and adding it to the timer's elapsed time.
 *
 * \param[in]	timer		initialized timer object.
 * \param[in]	reset flag	flag to indicate if timer should be reset.
 *
 * \return      0 on success
 *		error_code otherwise
 */
int
end_timer(dsg_timer_t *t, int reset);

/** Function responsible for printing timing report to an external file.
 *
 * \param[in]	timer		initialized timer object.
 *
 * \return
 */
int
print_report(dsg_timer_t *t);

/** Function indicating if timer is running or not.
 *
 * \param[in]	timer		initialized timer object
 *
 * \return      1 if running, 0 otherwise
 */
int
isrunning(dsg_timer_t *t);

/** Function responsible for allocating a new timer object,
 *  and opening timer file to be dumped in.
 *
 * \return     pointer to allocated timer object.
 */
int
create_timer(dsg_timer_t **t);


/** Function responsible for destroying a timer object
 *
 * \return     pointer to allocated timer object.
 */
int
destroy_timer(dsg_timer_t *t);

#endif
