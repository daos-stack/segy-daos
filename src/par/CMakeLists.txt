cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
project(seismic-unix LANGUAGES C)

include_directories(./include)

add_library(
	par
	STATIC
	atopkge.c
	docpkge.c
	ealloc.c
	errpkge.c
	filestat.c
	fractal.c
	getpars.c
	include
	lincoeff.c
	minfunc.c
	modeling.c
	refaniso.c
	rke.c
	smooth.c
	subcalls.c
	syscalls.c
	taup.c
	upweik.c
	VND.c
	wtlib.c
)
target_link_libraries(par cwp -lm)
