#!/bin/bash

if [ "$#" -ne 5 ]; then
    echo "Illegal number of parameters"
    echo "Requires 5 parameters : pool uuid, container uuid, svc ranklist, first segyfile, second segyfile"
    exit 1
fi

cd data

tests_program_path=./../build/tests_build
main_program_path=./../build/main_build
first_file=$4
second_file=$5

## Funtion to compare files.
function compare_files {
	if cmp -s "$1" "$2" ; then
	   echo $3 " : test passed"
	else
	   echo $3 " : test failed"
	fi
}

function daos_seis_tests {
	echo "Parse original segy file"
	time $tests_program_path/seismic_obj_creation pool=$1 container=$2 svc=$3 in=/first_file out=/SEIS_ROOT_OBJECT keys=offset
	echo "Find number of traces and gathers in original segy file"
	time $tests_program_path/get_traces_count pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT
	echo "Read shot traces" 
	time $tests_program_path/read_traces pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_segyread.su shot_id=610
	echo "Sort traces headers"
	time $tests_program_path/sort_traces pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_sort.su keys=+fldr,+gx
	echo "Window traces headers"
	time $tests_program_path/window_traces pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_wind.su keys=tracl,fldr min=10666,609 max=12010,800 
	echo "Change traces headers"
	time $tests_program_path/change_headers pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_chw.su key1=tracr key2=tracr a=1000  
	echo "Set traces headers"
	time $tests_program_path/set_headers pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_shw.su keys=dt a=4000
	echo "Parse additional segy file"
	time $tests_program_path/parse_additional_file pool=$1 container=$2 svc=$3 in=/second_file out=/SEIS_ROOT_OBJECT
	echo "Read shot traces after parsing additional file"
	time $tests_program_path/read_traces pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_segyread_shot_855.su shot_id=855
	echo "Read shot traces after parsing additional file"
	time $tests_program_path/read_traces pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT out=daos_seis_segyread_shot_609.su shot_id=609
	echo "Find number of traces and gathers after parsing additional segy file"
	time $tests_program_path/get_traces_count pool=$1 container=$2 svc=$3 in=/SEIS_ROOT_OBJECT

}

function su_tests {
	echo "Parse first segy file"  
	time $main_program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=/first_file >daos_segyread_temp.su
	echo "Find number of traces in segy file"
	time $main_program_path/daos_sutrcount pool=$1 container=$2 svc=$3 <daos_segyread_temp.su 
	echo "Window traces headers"
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=610 max=610  >daos_segyread.su
	echo "Sort traces headers"
	time $main_program_path/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread_temp.su +fldr +gx >daos_sort.su
	echo "Window traces headers"
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=tracl min=10666 max=12010 >daos_window.su
	
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_window.su key=fldr min=609 max=800 >daos_wind.su
	echo "Change traces headers"
	time $main_program_path/daos_suchw pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key1=tracr key2=tracr a=1000 >daos_chw.su 
	echo "Set traces headers"
	time $main_program_path/daos_sushw pool=$1 container=$2 svc=$3 <daos_chw.su key=dt a=4000 >daos_shw.su
	echo "Parse second segy file"
	time $main_program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=/second_file >daos_segyread_second_file_shots.su
	echo "Window traces headers in second file"
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_second_file_shots.su key=fldr min=855 max=855  >daos_segyread_shot_855.su

	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_shw.su key=fldr min=609 max=609  >daos_segyread_shot_609.su
	echo "Find number of traces in segy file"
	time $main_program_path/daos_sutrcount pool=$1 container=$2 svc=$3 <daos_segyread_second_file_shots.su 
}

function run_tests {
	echo "Run tests using daos seismic mapping"
	daos_seis_tests $1 $2 $3
	echo "Run tests using Light seismic unix"
	su_tests $1 $2 $3
}

echo 'Copying segy to DFS container...'
## Copy velocity segy file to daos.
$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in=$first_file out=/first_file
$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in=$second_file out=/second_file

echo 'Running commands...'
## Run seismic unix commands.
run_tests $1 $2 $3

echo 'Copy commands output...'
file_list=(segyread sort wind chw shw segyread_shot_855 segyread_shot_609)
## Copy from daos to posix.
for i in ${file_list[@]};
do
	$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_$i.su" out="daos_seis_$i.su" daostoposix=1
	$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_$i.su" out="daos_$i.su" daostoposix=1
	
done

$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="binary" out="daos_binary" daostoposix=1
$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="header" out="daos_header" daostoposix=1
$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_binary" out="daos_seis_binary" daostoposix=1
$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_text_header" out="daos_seis_text_header" daostoposix=1

echo 'Compare commands...'
file_list=(segyread sort wind chw shw segyread_shot_855 segyread_shot_609)
## Compare outputs.
for i in ${file_list[@]};
do
	compare_files "daos_seis_$i.su" "daos_$i.su" "$i" 
done

compare_files "daos_seis_binary" "daos_binary" "binary"
compare_files "daos_seis_text_header" "daos_header" "text_header"


