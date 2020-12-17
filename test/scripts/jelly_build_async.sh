#!/bin/bash -l

echo "Running build script from repository"
echo "(current dir is: $PWD)"

# Spack
export SPACK_ROOT=/mnt/wrk/jsoumagne/spack
source $SPACK_ROOT/share/spack/setup-env.sh
spack load -r daos@master
spack load -r hdf5@1.12-async
spack load -r cmake
spack load -r gcc

# store the current directory in a local variable to get back to it later
export HDF5_VOL_DAOS_ROOT=/scr/jsoumagne/daos

# set up testing configuration
export HDF5_VOL_DAOS_DASHBOARD_MODEL="Nightly"

# modifying these variables may prevent compile flags to be set correctly
export CC=gcc
export GCOV=gcov

# get back to the testing script location
pushd $HDF5_VOL_DAOS_ROOT

export HDF5_VOL_DAOS_BUILD_CONFIGURATION="Debug"
export HDF5_VOL_DAOS_DO_COVERAGE="true"
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script_async.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_async_debug_coverage.log
unset HDF5_VOL_DAOS_DO_COVERAGE

export HDF5_VOL_DAOS_BUILD_CONFIGURATION="Asan"
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script_async.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_async_asan.log

export HDF5_VOL_DAOS_BUILD_CONFIGURATION="Ubsan"
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script_async.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_async_ubsan.log

export HDF5_VOL_DAOS_BUILD_CONFIGURATION="RelWithDebInfo"
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script_async.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_async_release.log

# clean up
rm -rf /mnt/daos/*

popd

