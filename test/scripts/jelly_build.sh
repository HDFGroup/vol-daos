#!/bin/bash -l

echo "Running build script from repository"
echo "(current dir is: $PWD)"

# Spack
export SPACK_ROOT=/mnt/wrk/jsoumagne/spack
source $SPACK_ROOT/share/spack/setup-env.sh
spack load -r daos@0.6
spack load -r hdf5
spack load -r cmake
spack load -r gcc@9.2.0

# store the current directory in a local variable to get back to it later
export HDF5_VOL_DAOS_ROOT=/scr/jsoumagne/daos

# set up testing configuration
export HDF5_VOL_DAOS_BUILD_CONFIGURATION="Debug"
export HDF5_VOL_DAOS_DASHBOARD_MODEL="Nightly"

export CC=`which gcc`
export GCOV=`which gcov`

# get back to the testing script location
pushd $HDF5_VOL_DAOS_ROOT

export HDF5_VOL_DAOS_DO_COVERAGE="true"
export HDF5_VOL_DAOS_DO_MEMCHECK="false"
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_coverage.log

export HDF5_VOL_DAOS_DO_COVERAGE="false"
export HDF5_VOL_DAOS_DO_MEMCHECK="true"
export HDF5_VOL_DAOS_MEMORYCHECK_TYPE="AddressSanitizer"
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_memcheck.log

export HDF5_VOL_DAOS_BUILD_CONFIGURATION="RelWithDebInfo"
export HDF5_VOL_DAOS_DO_COVERAGE="false"
export HDF5_VOL_DAOS_DO_MEMCHECK="false"
unset  HDF5_VOL_DAOS_MEMORYCHECK_TYPE
ctest -S $HDF5_VOL_DAOS_ROOT/source/test/scripts/jelly_script.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_DAOS_ROOT/last_build_release.log

# clean up
rm -rf /mnt/daos/*

popd

