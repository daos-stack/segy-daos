#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    echo "Requires 1 parameter : mpi_flag"
    exit 1
fi

# Running DAOS Seismic graph Unit Tests
build_path=./build/tests_build
Segy_file=/home/daos/daos/shots_601_610_cdp_offset_calculated.segy
Raw_file=/home/daos/daos/raw.su
Header_file=/home/daos/daos/header.su

failed=0
passed=0
failures=()

run_test()
{
	echo $2
	echo "***************"
    if ! $1; then
	        ((failed = failed + 1))
            failures+=("$2")
    else
	        ((passed = passed + 1))
    fi
        
}

run_mpi_tests()
{
	run_test "mpirun --cpu-set 2-23 --bind-to core --allow-run-as-root -np 2 --mca btl tcp,self \
	$build_path/utilities/mutex_test" "MUTEX lock module test"
	echo "******************************************************************************"
	run_test "mpirun --cpu-set 2-23 --bind-to core --allow-run-as-root -np 2 --mca btl tcp,self \
	$build_path/api/window_headers_mpi_version_and_dump_in_graph_test \
	"$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest"\
	 " " MPI window headers and dump traces in graph"
	echo "******************************************************************************"
	run_test "mpirun --cpu-set 2-23 --bind-to core --allow-run-as-root -np 2 --mca btl tcp,self \
	$build_path/api/window_headers_mpi_version_and_return_list_test \
	"$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " \
	" MPI window headers and return list test"
	echo "******************************************************************************"
	run_test "mpirun --cpu-set 2-23 --bind-to core --allow-run-as-root -np 2 --mca btl tcp,self \
	$build_path/api/sort_traces_mpi_version_test \
	"$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " \
	" MPI Sort headers test"
	echo "******************************************************************************"
	run_test "mpirun --cpu-set 2-23 --bind-to core --allow-run-as-root -np 2 --mca btl tcp,self \
	$build_path/api/parse_linked_list_mpi_version_test \
	"$POOL_ID" "$CONT_ID" "filemounttest" " \
	" MPI Parse Linked List test"
	echo "******************************************************************************"

}
run_serial_tests()
{
	run_test "$build_path/api/create_graph "$POOL_ID" "$CONT_ID" " "Create Graph test"
	echo "******************************************************************************"
	run_test "$build_path/api/open_graph "$POOL_ID" "$CONT_ID" " "Open Graph test"
	echo "******************************************************************************"
	run_test "$build_path/api/update_num_of_traces "$POOL_ID" "$CONT_ID" " "Update graph num of traces test"
	echo "******************************************************************************"
	run_test "$build_path/api/fetch_binary_text_headers "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Fetch binary and text headers test"
	echo "******************************************************************************"
	run_test "$build_path/api/get_num_of_gathers_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "get/set gather object num of gathers test"
	echo "******************************************************************************"
	run_test "$build_path/api/parse_segy_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Parse segy file test"
	echo "******************************************************************************"
	run_test "$build_path/api/window_headers_and_return_list_test "$POOL_ID" "$CONT_ID"  "$Segy_file" "filemounttest" " "window headers and return list test"
	echo "******************************************************************************"
	run_test "$build_path/api/parse_linked_list_test "$POOL_ID" "$CONT_ID" "filemounttest" " "Parse From Linked List"
	echo "******************************************************************************"
	run_test "$build_path/api/window_headers_and_dump_in_graph_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "window headers and dump traces in graph"
	echo "******************************************************************************"
	run_test "$build_path/api/set_headers_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Set/Change headers test"
	echo "******************************************************************************"
	run_test "$build_path/api/get_custom_headers_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "get custom headers test"
	echo "******************************************************************************"
	run_test "$build_path/api/set_traces_data_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "set traces data test"
	echo "******************************************************************************"
	run_test "$build_path/api/sort_traces_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "sort traces data test"
        echo "******************************************************************************"
	run_test "$build_path/api/range_headers_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Range Headers test"
	echo "******************************************************************************"
	run_test "$build_path/api/get_unique_values_list_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Get gather object unique values"
	echo "******************************************************************************"
	run_test "$build_path/api/parse_raw_test "$POOL_ID" "$CONT_ID" "$Raw_file" "$Header_file" "filemounttest" "filemounttest2" " "parse raw data test"
	echo "******************************************************************************"
}


PARAMS=$(dmg -i pool create -s 8G -n 8G)
echo "$PARAMS"
POOL_ID=`echo -e $PARAMS | cut -d':' -f 3 | cut -d ' ' -f 2 | xargs`
echo "$POOL_ID"
CONT_ID=$(echo "$POOL_ID")
echo "$CONT_ID"

echo "***************"
echo "Utilities Tests"
echo "***************"


run_test "$build_path/utilities/timer_test" "Timer module test"
echo "******************************************************************************"
run_test "$build_path/utilities/string_helpers_test" "String helpers module test"
echo "******************************************************************************"
run_test "$build_path/utilities/cmd_test" "CMD module test"
echo "******************************************************************************"

echo "***************"
echo "DAOS Primitives Tests"
echo "***************"

run_test "$build_path/daos_primitives/daos_object_test "$POOL_ID" "$CONT_ID" "  "Daos object module test"
echo "******************************************************************************"
run_test "$build_path/daos_primitives/daos_array_object_test "$POOL_ID" "$CONT_ID" "  "Daos array object module test"
echo "******************************************************************************"
run_test "$build_path/daos_primitives/dfs_helpers_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" "posix_file_returned" "  "DFS helpers module test"
echo "******************************************************************************"

echo "***************"
echo "Data Types Tests"
echo "***************"

run_test "$build_path/data_types/generic_value_test" "Generic value module test"
echo "******************************************************************************"
run_test "$build_path/data_types/trace_test" "Trace module test"
echo "******************************************************************************"
run_test "$build_path/data_types/ensemble_test" "Ensemble module test"
echo "******************************************************************************"

echo "***************"
echo "Data Structure Tests"
echo "***************"

run_test "$build_path/data_structures/linked_list_test" "Linked List module test"
echo "******************************************************************************"

echo "***************"
echo "Operations Tests"
echo "***************"

run_test "$build_path/operations/sort_test" "Sort module test"
echo "******************************************************************************"
run_test "$build_path/operations/calculate_header_test" "Header Calculation test"
echo "******************************************************************************"
run_test "$build_path/operations/window_test" "Window module test"
echo "******************************************************************************"
run_test "$build_path/operations/range_test" "Range module test"
echo "******************************************************************************"

echo "***************"
echo "Seismic Sources Tests"
echo "***************"
run_test "$build_path/seismic_sources/segy_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "SEGY source test"
echo "******************************************************************************"
run_test "$build_path/seismic_sources/list_test " "Ensemble list source test"
echo "******************************************************************************"


echo "***************"
echo "Graph Objects Tests"
echo "***************"
run_test "$build_path/graph_objects/trace_hdr_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Trace header object test"
echo "******************************************************************************"
run_test "$build_path/graph_objects/trace_data_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Trace data object test"
echo "******************************************************************************"
run_test "$build_path/graph_objects/traces_array_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Traces array object test"
echo "******************************************************************************"
run_test "$build_path/graph_objects/root_test "$POOL_ID" "$CONT_ID" " "Root object test"
echo "******************************************************************************"
run_test "$build_path/graph_objects/gather_test "$POOL_ID" "$CONT_ID" "$Segy_file" "filemounttest" " "Gather object test"
echo "******************************************************************************"

if [ $1 -eq 0 ]; then
	echo "running DSG serial tests"
	run_serial_tests
else
	echo "running DSG mpi tests"
	run_mpi_tests
fi

echo "*****************"
echo "End of unit Tests"
echo "*****************"

dmg -i pool destroy -f --pool $POOL_ID

# Check if any tests failed!

if [ $failed -eq 0 ]; then
    echo "SUCCESS! NO TEST FAILURES, $passed tests passed"
    exit 0
else
    echo "FAILURE: $failed tests failed (listed below)"
    for ((i = 0; i < ${#failures[@]}; i++)); do
        echo "${failures[$i]}"
    done
    exit 1
fi
