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
	#original su segyread
	time segyread tape=$first_file >original_segyread_temp.su
	#original su wind one shot	
	time suwind <original_segyread_temp.su key=fldr min=610 max=610 >original_segyread.su
	#original su wind ten shots
	time suwind <original_segyread_temp.su key=fldr min=640 max=650 >original_wind.su
	#original su sort cdp gx	
	time susort <original_segyread_temp.su +cdp +gx >original_sort.su
	#original su segyread in dfs
	time segyread tape=/shot_601_610 >segyread_temp.su
	#original su wind one shot in dfs
	time suwind <segyread_temp.su key=fldr min=610 max=610 >segyread.su
	#original su wind ten shots in dfs
	time suwind <segyread_temp.su key=fldr min=640 max=650 >wind.su
	#original su sort cdp gx in dfs
	time susort <segyread_temp.su +cdp +gx >sort.su
	#lsu segyread					
	time $main_program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=/shot_601_610 >daos_segyread_temp.su
	#lsu wind, one shot
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=610 max=610  >daos_segyread.su
	#lsu wind, ten shots
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=640 max=650 >daos_wind.su
	#lsu sort cdp gx	
	time $main_program_path/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread_temp.su +cdp +gx >daos_sort.su
	#daos_seis mapping read
	time $tests_program_path/seismic_obj_creation pool=$1 container=$2 svc=$3 in=/shot_601_610 out=/SHOTS_601_610_SEIS_ROOT_OBJECT keys=fldr,cdp,offset
	#daos_seis mapping wind one shot
	time $tests_program_path/read_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_segyread.su shot_id=610
	#daos_seis mapping wind ten shots
	time $tests_program_path/window_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_wind.su keys=fldr min=640 max=650 
	#daos_seis mapping sort cdp gx
	time $tests_program_path/sort_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_sort.su keys=+cdp,+gx
	#lsu segyread					
	time $main_program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=/shot_601_610 >daos_segyread_temp.su
	#lsu wind, one shot
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=610 max=610  >daos_segyread.su
	#lsu wind, ten shots
	time $main_program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=640 max=650 >daos_wind.su
	#lsu sort cdp gx	
	time $main_program_path/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread_temp.su +cdp +gx >daos_sort.su
	
}

echo 'Copying segy to DFS container...'
## Copy velocity segy file to daos.
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in=$first_file out=/shot_601_610
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in=$second_file out=/shot_611_620

echo 'Running commands...'
## Run seismic unix commands.
run_tests $1 $2 $3

echo 'Copy commands output...'
file_list=(segyread wind sort)
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
file_list=(segyread wind sort)
## Compare outputs.
for i in ${file_list[@]};
do
	compare_files "daos_seis_$i.su" "daos_$i.su" "$i" 
done

compare_files "daos_seis_binary" "daos_binary" "binary"
compare_files "daos_seis_text_header" "daos_header" "text_header"


