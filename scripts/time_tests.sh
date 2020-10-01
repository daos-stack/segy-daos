#!/bin/bash

if [ "$#" -ne 6 ]; then
    echo "Illegal number of parameters"
    echo "Requires 6 parameters : pool uuid, container uuid, svc ranklist, output file, segyfile_path, dfs_mount_path"
    exit 1
fi

cd data 

tests_program_path=./../build/tests_build
main_program_path=./../build/main_build
first_file=$5
mount_path=$6

function original_su_time_tests {
	#original su segyread
	echo "Parse segy file " >>$2  							
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
	{ time segyread tape=$1/shots_file >$1/segyread_temp.su; } 2>>$2
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
	{ time $4/daos_segyread pool=$1 container=$2 svc=$3 tape=/shots_file >daos_segyread_temp.su; } 2>>$5  
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
	{ time $4/seismic_obj_creation pool=$1 container=$2 svc=$3 in=/shots_file out=/SHOTS_601_610_SEIS_ROOT_OBJECT keys=fldr,cdp,offset; } 2>> $5
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
	echo "Running Original SU commands in dfs container." >>$4
	original_su_dfs_time_tests $mount_path	$4
	echo "Running light SU commands in dfs container." >>$4
	light_su_time_tests $1 $2 $3 $main_program_path $4
	echo "Running daos seis mapping commands in dfs container." >>$4
	daos_seis_mapping_time_tests $1 $2 $3 $tests_program_path $4
}

echo 'Copying segy to DFS container...'
## Copy velocity segy file to daos.
$main_program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in=$first_file out=/shots_file

echo 'Running commands...'
## Run seismic unix commands.
run_tests $1 $2 $3 $4

