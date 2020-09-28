#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "Requires 3 parameters : pool uuid, container uuid, svc ranklist, output file"
    exit 1
fi

tests_program_path=./build/tests_build
main_program_path=./build/main_build
first_file=shots_601_610_cdp_offset_calculated.segy
second_file=shots_611_620_cdp_offset_calculated.segy
mount_path=/tmp/dfs_test

function original_su_time_tests {
	#original su segyread
	echo "Read shot traces " >>$2	
	{ time segyread tape=$1 >original_segyread_temp.su; } 2>>$2
	#original su wind one shot	
	echo "Read shot traces " >>$2	
	{ time suwind <original_segyread_temp.su key=fldr min=610 max=610 >original_segyread.su; } 2>>$2
	#original su wind ten shots
	echo "Window traces headers " >>$2	
	{ time suwind <original_segyread_temp.su key=fldr min=640 max=650 >original_wind.su; } 2>>$2
	#original su sort cdp gx	
	echo "Sort traces headers " >>$2	
	{ time susort <original_segyread_temp.su +cdp +gx >original_sort.su; } 2>>$2 
}

function original_su_dfs_time_tests {
	#original su segyread in dfs
	echo "Parse segy file " >>$2  							
	{ time segyread tape=$1/shot_601_610 >$1/segyread_temp.su; } 2>>$2
	#original su wind one shot in dfs
	echo "Read shot traces " >>$2	
	{ time suwind <$1/segyread_temp.su key=fldr min=610 max=610 >$1/segyread.su; } 2>>$2
	#original su wind ten shots in dfs
	echo "Window traces headers " >>$2	
	{ time suwind <$1/segyread_temp.su key=fldr min=640 max=650 >$1/wind.su; } 2>>$2
	#original su sort cdp gx in dfs
	echo "Sort traces headers " >>$2	
	{ time susort <$1/segyread_temp.su +cdp +gx >sort.su; } 2>>$2
}

function light_su_time_tests {
	#lsu segyread
	echo "Parse segy file " >>$5  						
	{ time $4/daos_segyread pool=$1 container=$2 svc=$3 tape=/shot_601_610 >daos_segyread_temp.su; } 2>>$5  
	#lsu wind, one shot
	echo "Read shot traces " >>$5
	{ time $4/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=610 max=610  >daos_segyread.su; } 2>>$5
	#lsu wind, ten shots
	echo "Window traces headers " >>$5
	{ time $4/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread_temp.su key=fldr min=640 max=650 >daos_wind.su; } 2>>$5
	#lsu sort cdp gx	
	echo "Sort traces headers " >>$5	
	{ time $4/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread_temp.su +cdp +gx >daos_sort.su; } 2>>$5
}

function daos_seis_mapping_time_tests {
	#daos_seis mapping read
	echo "Parse segy file " >>$5
	{ time $4/seismic_obj_creation pool=$1 container=$2 svc=$3 in=/shot_601_610 out=/SHOTS_601_610_SEIS_ROOT_OBJECT keys=fldr,cdp,offset; } 2>> $5
	#daos_seis mapping wind one shot
	echo "Read shot traces " >>$5
	{ time $4/read_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_segyread.su shot_id=610; } 2>> $5
	#daos_seis mapping wind ten shots
	echo "Window traces headers " >>$5
	{ time $4/window_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_wind.su keys=fldr min=605 max=608; } 2>> $5 
	#daos_seis mapping sort cdp gx
	echo "Sort traces headers " >>$5	
	{ time $4/sort_traces pool=$1 container=$2 svc=$3 in=/SHOTS_601_610_SEIS_ROOT_OBJECT out=daos_seis_sort.su keys=+cdp,+gx; } 2>> $5
}


function run_tests {
	echo "Running Original SU commands outside dfs container." >>$4
	original_su_time_tests $first_file $4
	#echo "Running Original SU commands in dfs container." >>$4
	#original_su_dfs_time_tests $mount_path	$4
	echo "Running light SU commands in dfs container." >>$4
	light_su_time_tests $1 $2 $3 $main_program_path $4
	echo "Running daos seis mapping commands in dfs container." >>$4
	daos_seis_mapping_time_tests $1 $2 $3 $tests_program_path $4
}

echo 'Copying segy to DFS container...'
## Copy velocity segy file to daos.
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in=$first_file out=/shot_601_610
./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in=$second_file out=/shot_611_620

echo 'Running commands...'
## Run seismic unix commands.
run_tests $1 $2 $3 $4

echo 'Copy commands output...'
file_list=(segyread wind sort)
## Copy from daos to posix.
for i in ${file_list[@]};
do
	./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_seis_$i.su" out="daos_seis_$i.su" daostoposix=1
	./build/main_build/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_$i.su" out="daos_$i.su" daostoposix=1
	
done

