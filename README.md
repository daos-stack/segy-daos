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
