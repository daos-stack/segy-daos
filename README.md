# DAOS Mapping

* Make sure you export these shared libraries
``` 
export LD_LIBRARY_PATH=/usr/lib:/home/daos/daos/build/src/common:/home/daos/daos/build/src/client/api:/home/daos/daos/build/src/client/dfs:/home/daos/daos/lsu/build/dfs_helper_build:/home/daos/daos/_build.external/pmdk/src/nondebug:/home/daos/daos/lsu/build/client/seis_build:
```


# DAOS Compatible Seismic Unix
* This works on modifying the original seismic unix and achieving an application level integration for the DFS API for the basic 9 commands supported in it.

## Run Test
* Make sure you have daos_server and daos_agent processes running.
* Make sure you have the original seismic unix running.
* Build using scons.
* Make sure to export needed shared libraries
 eg: export LD_LIBRARY_PATH=/usr/lib:/home/daos/daos/build/src/common:/home/daos/daos/build/src/client/api:/home/daos/daos/build/src/client/dfs:/home/daos/daos/lsu/build/dfs_helper_build:/home/daos/daos/_build.external/pmdk/src/nondebug:
* run ./scripts/prepare_tests.sh to download the velocity model of the BP and generate the actual output from the original seismic unix to work as comparision metrics for the DFS counterparts.
* Create a pool using dmg pool create command.
* run ./scripts/run_tests.sh <pool_uuid> <container_uuid> <ranklist> that will mount the segy to dfs, use the counterpart seismic unix to generate the output, copy it back to the posix system and compare it with the original output.

## Known issues
* Currently due to the <stdin >stdout way of handling io in seismic unix, even the daos compatible commands would create ghost files(empty files with just the name provided due to the way bash works, however all data will be stored in the equivalent dfs files. We propose moving away from this way to providing commandline arguments for input/output of commands with tty being an option if supported.
* Currently we only support operations being given absolute path in tape in segyread and as for stdin/stdout redirection we only support files being read/written to the root directory.
* The header part in segyread due to its dependencies on an external command line program 'dd', is being written to the posix system then copied unlike the others where they written directly to DFS.

##DAOS-Segy mapping
* DAOS_Segy mapping target is to convert seismic data stored in segy files back to its native format through utilizing the DAOS API and object storage. 
* This will help get rid of segy files(widely used in seismic processing) constraints by directly accessing the required data(traces) saving the time wasted in passing through all data stored.

##Prepare tests
* Make sure you have the original seismic unix running.
* Run ./scripts/daos_seis_prepare_tests.sh to download two shot files of the BP model, and calculate the cdp and offset header values.

##DAOS-SEIS mapping tests.
* Make sure you have daos_server and daos_agent processes running.
* Make sure to export needed shared libraries
   eg: export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/daosteam/mirna/daos-segy/build/dfs_helper_build:/home/daosteam/mirna/daos-segy/build/client/seis_build:
* Create a pool using dmg pool create command.
* Run ./scripts/daos_seis_tests.sh <pool_uuid> <container_uuid> <ranklist>, will mount the downloaded segy files, and run a series of seismic processing commands:
 	* Parse the segy file and build the DAOS-SEIS mapping graph.
	* Find traces count
	* Read shot traces and save output to file.
	* Sort traces data and save sorted data to file.
	* Window traces and save the windowed data to file.
	* Change headers values and save the changed headers to file.
	* Set headers values and save the new header values to file.
	* Parse the second segy file and dump data to the same graph.
	* Read shot traces and save output to file.
	* Find the traces count and updated number of gathers after parsing the second segy file. 

Then the same flow of the processing commands will be executed using the previously modified SU commands(light seismic unix).
Then all created files through the processing flows will be copied from the daos file system to the posix directory and compared.
NOTE: daos_seis_tests.sh script can be updated to change the first and second file paths.

##time tests.
This test simply compares the run time of the main processing commands in the original seismic unix on posix system vs original seismic unix in dfs container vs modified(light) seismic unix in dfs container vs daos seis mapping in dfs container.
 
* Make sure you have daos_server and daos_agent processes running.
* Make sure to export needed shared libraries
   eg: export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/daosteam/mirna/daos-segy/build/dfs_helper_build:/home/daosteam/mirna/daos-segy/build/client/seis_build:
* Make sure first to download one segy file.
   eg: BP model segy files through the following link: https://wiki.seg.org/wiki/2004_BP_velocity_estimation_benchmark_model
* Create a pool using dmg pool create command.
* Run ./scripts/time_tests.sh <pool_uuid> <container_uuid> <ranklist> <output_file>, will mount the downloaded segy file, and call four functions executing a series of seismic processing commands:
 	* Parse the segy file and build the DAOS-SEIS mapping graph.
	* Read shot traces and save output to file.
	* Window traces and save the windowed data to file.
	* Sort traces data and save sorted data to file.
The time of each command will be written to output file.

The same flow of the processing commands will be executed four time using:
	* original seismic unix on posix system
	* original seismic unix in dfs container
	* modified(light) seismic unix in dfs container
	* daos seismic mapping in dfs container.
NOTE: time_tests.sh script can be updated to change the first,second file paths and the path of the mounted dfs container.

