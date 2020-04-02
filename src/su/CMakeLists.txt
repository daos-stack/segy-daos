cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
project(seismic-unix LANGUAGES C)

include_directories(./include)

add_library(
	su
	STATIC
	elco_scalar.c
	fgetgthr.c
	fgethdr.c
	fgettr.c
	fputgthr.c
	fputtr.c
	getSPSfield.c
	hdrpkge.c
	tabplot.c
	valpkge.c
	xdrbhdrsub.c
	xdrhdrsub.c
)
target_link_libraries(su par cwp -lm)
