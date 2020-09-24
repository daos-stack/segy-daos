#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Illegal number of parameters"
    echo "Requires 3 parameters : pool uuid, container uuid, svc ranklist"
    exit 1
fi

tests_program_path=./build/tests_build
main_program_path=./build/main_build
first_file=shots_601_610_cdp_offset_calculated.segy
second_file=shots_611_620_cdp_offset_calculated.segy

## Funtion to compare files.
function compare_files {
	if cmp -s "$1" "$2" ; then
	   echo $3 " : test passed"
	else
	   echo $3 " : test failed"
	fi
}

function run_tests {

	time $tests_program_path/seismic_obj_creation pool=$1 container=$2 svc=$3 in=/shot_601_610 out=/SHOTS_601_610_SEIS_ROOT_OBJECT keys=offset

	time $tests_program_path/get_traces_count pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT
	
	time $tests_program_path/read_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_segyread.su shot_id=610
	
	time $tests_program_path/sort_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_sort.su keys=+fldr,+gx
	
	time $tests_program_path/window_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_wind.su keys=tracl,fldr min=10666,609 max=12010,800 

	time $tests_program_path/change_headers pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_chw.su key1=tracr key2=tracr a=1000  

	time $tests_program_path/set_headers pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_shw.su keys=dt a=4000
					
	time $tests_program_path/parse_additional_file pool=$1 container=$2 svc=$3 in=/shot_611_620 out=/SHOTS_601_610_SEIS_ROOT_OBJECT

	time $tests_program_path/read_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_segyread_shot_615.su shot_id=608
				
	time $tests_program_path/get_traces_count pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT
					
	time $main_program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=/shot_601_610 >daos_segyread_temp.su
	
	time $main_program_path/daos_sutrcount pool=$1 container=$2 svc=$3 <daos_segyread_temp.su 
	
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=610 max=610  >daos_segyread.su
	
	time $main_program_path/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread_temp.su +fldr +gx >daos_sort.su
	
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=tracl min=10666 max=12010 >daos_window.su

	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_window.su key=fldr min=609 max=800 >daos_wind.su

	time $main_program_path/daos_suchw pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key1=tracr key2=tracr a=1000 >daos_chw.su 

	time $main_program_path/daos_sushw pool=$1 container=$2 svc=$3 <daos_chw.su key=dt a=4000 >daos_shw.su

	time $main_program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=/shot_611_620 >daos_segyread_shots_611_620.su

	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_shots_611_620.su key=fldr min=615 max=615  >daos_segyread_shot_615.su

	time $main_program_path/daos_sutrcount pool=$1 container=$2 svc=$3 <daos_segyread_shots_611_620.su 

}

echo 'Copying segy to DFS container...'
## Copy velocity segy file to daos.
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in=$first_file out=/shot_601_610
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in=$second_file out=/shot_611_620

echo 'Running commands...'
## Run seismic unix commands.
run_tests $1 $2 $3

echo 'Copy commands output...'
file_list=(segyread sort wind chw shw segyread_shot_615)
## Copy from daos to posix.
for i in ${file_list[@]};
do
	./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_$i.su" out="daos_seis_$i.su" daostoposix=1
	./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_$i.su" out="daos_$i.su" daostoposix=1
	
done

./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="binary" out="daos_binary" daostoposix=1
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="header" out="daos_header" daostoposix=1
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_binary" out="daos_seis_binary" daostoposix=1
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_text_header" out="daos_seis_text_header" daostoposix=1

echo 'Compare commands...'
file_list=(segyread sort wind chw shw segyread_shot_615)
## Compare outputs.
for i in ${file_list[@]};
do
	compare_files "daos_seis_$i.su" "daos_$i.su" "$i" 
done

compare_files "daos_seis_binary" "daos_binary" "binary"
compare_files "daos_seis_text_header" "daos_header" "text_header"


