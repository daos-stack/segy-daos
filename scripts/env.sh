#!/bin/bash

dsg_path=$(pwd)
#daos_path=/home/daos/daos
#export C_INCLUDE_PATH=${dsg_path}/include/dsg:${daos_path}/src/include:$C_INCLUDE_PATH
export C_INCLUDE_PATH=${dsg_path}/include/dsg:$C_INCLUDE_PATH
export LD_LIBRARY_PATH=${dsg_path}/build/src_build/utilities:${dsg_path}/build/src_build/data_types:\
${dsg_path}/build/src_build/daos_primitives:/usr/lib64:${dsg_path}/build/src_build/seismic_sources:\
${dsg_path}/build/src_build/data_structures:${dsg_path}/build/src_build/operations:\
${dsg_path}/build/src_build/graph_objects:${dsg_path}/build/src_build/api:$LD_LIBRARY_PATH:
 