#!/bin/bash -l

echo "Running build script from repository"
echo "(current dir is: $PWD)"

# Spack
export SPACK_ROOT=/mnt/wrk/jsoumagne/spack
source $SPACK_ROOT/share/spack/setup-env.sh
source <(spack module tcl loads --dependencies daos)
source <(spack module tcl loads --dependencies hdf5@daos-develop)
source <(spack module tcl loads --dependencies gcc@8.3.0)

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

popd

