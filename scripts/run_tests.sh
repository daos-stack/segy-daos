#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Illegal number of parameters"
    echo "Requires 3 parameters : pool uuid, container uuid, svc ranklist"
    exit 1
fi

program_path=./../build/main_build

cd data

## Funtion to compare files.
function compare_files {
	if cmp -s "$1" "$2" ; then
	   echo $3 " : test passed"
	else
	   echo $3 " : test failed"
	fi
}

function run_tests {
	## Segyread 
	$program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=vel_z6.25m_x12.5m_exact.segy >daos_segyread.su
	## Sutrcount
	$program_path/daos_sutrcount pool=$1 container=$2 svc=$3 <daos_segyread.su >daos_sutrcount.su
	## Sugethw
	$program_path/daos_sugethw pool=$1 container=$2 svc=$3 <daos_segyread.su key=tracl,offset,dt output=ascii  >daos_sugethw_ascii.su
	$program_path/daos_sugethw pool=$1 container=$2 svc=$3 <daos_segyread.su key=tracl,offset,dt output=geom   >daos_sugethw_geom.su 
	$program_path/daos_sugethw pool=$1 container=$2 svc=$3 <daos_segyread.su key=tracl,offset,dt output=binary >daos_sugethw_binary.su
	## Suaddhead
	$program_path/daos_suaddhead pool=$1 container=$2 svc=$3 <daos_segyread.su ns=1024 >daos_suaddhead.su
	## Sushw
	$program_path/daos_sushw pool=$1 container=$2 svc=$3 <daos_segyread.su key=tracl,offset,dt infile=daos_sugethw_binary.su >daos_sushw_infile.su  	
	$program_path/daos_sushw pool=$1 container=$2 svc=$3 <daos_segyread.su key=dt    a=4000 		>daos_sushw_a.su
	## Susort
	$program_path/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread.su >daos_sort_filetofile.su cdp
	$program_path/daos_susort pool=$1 container=$2 svc=$3 <daos_segyread.su | $program_path/daos_sutrcount pool=$1 container=$2 svc=$3 >daos_sutrcount_pipefrom_susort.su
	$program_path/daos_segyread pool=$1 container=$2 svc=$3 tape=vel_z6.25m_x12.5m_exact.segy | $program_path/daos_susort pool=$1 container=$2 svc=$3 >daos_sort_pipetofile.su cdp
	## Surange
	$program_path/daos_surange pool=$1 container=$2 svc=$3 <daos_segyread.su key=tracl >daos_surange.su
	## Suchw
	$program_path/daos_suchw pool=$1 container=$2 svc=$3 <daos_segyread.su key1=tracr key2=tracr a=1000  >daos_suchw.su 
	## Suwind
	$program_path/daos_suwind pool=$1 container=$2 svc=$3 <daos_segyread.su key=sx min=669000 max=670000  >daos_suwind.su
}
echo 'Copying segy to DFS container...'
## Copy velocity segy file to daos.
$program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in=vel_z6.25m_x12.5m_exact.segy out=vel_z6.25m_x12.5m_exact.segy

echo 'Running commands...'
## Run seismic unix commands.
run_tests $1 $2 $3

echo 'Copy commands output...'
file_list=(segyread sugethw_ascii sugethw_geom sugethw_binary suaddhead sushw_infile sushw_a sort_filetofile sort_pipetofile suchw suwind)
## Copy from daos to posix.
for i in ${file_list[@]};
do
	$program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="daos_$i.su" out="daos_$i.su" daostoposix=1
done
$program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="binary" out="daos_binary" daostoposix=1
$program_path/dfs_file_mount pool=$1 container=$2 svc=$3 in="header" out="daos_header" daostoposix=1

echo 'Compare commands...'
file_list=(segyread sutrcount sugethw_ascii sugethw_geom sugethw_binary suaddhead sushw_infile sushw_a sort_filetofile sutrcount_pipefrom_susort sort_pipetofile surange suchw suwind)
## Compare outputs.
for i in ${file_list[@]};
do
	compare_files "original_$i.su" "daos_$i.su" "$i" 
done
compare_files "original_binary" "daos_binary" "binary"
compare_files "original_header" "daos_header" "header" 