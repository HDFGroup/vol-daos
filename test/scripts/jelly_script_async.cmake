# This script takes in optional environment variables.
#   HDF5_VOL_DAOS_BUILD_CONFIGURATION=Debug | RelWithDebInfo | Release | Asan | Ubsan
#   HDF5_VOL_DAOS_DASHBOARD_MODEL=Experimental | Nightly | Continuous
#   HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES
#   HDF5_VOL_DAOS_DO_COVERAGE
#   HDF5_VOL_DAOS_DO_MEMCHECK

set(CTEST_PROJECT_NAME "HDF5_VOL_DAOS")

if(NOT dashboard_git_url)
  set(dashboard_git_url "https://github.com/HDFGroup/vol-daos.git")
endif()

# Update source
set(dashboard_do_checkout 0)
set(dashboard_do_update 1)

if(NOT DEFINED CTEST_TEST_TIMEOUT)
  set(CTEST_TEST_TIMEOUT 180)
endif()

if(NOT DEFINED CTEST_SUBMIT_NOTES)
  set(CTEST_SUBMIT_NOTES TRUE)
endif()

# Give a site name
set(CTEST_SITE "jelly.ad.hdfgroup.org")

# Must specify existing source directory
set(CTEST_DASHBOARD_ROOT "$ENV{HDF5_VOL_DAOS_ROOT}")
set(dashboard_source_name "source")

# HDF5_VOL_DAOS_BUILD_CONFIGURATION
set(HDF5_VOL_DAOS_BUILD_CONFIGURATION "$ENV{HDF5_VOL_DAOS_BUILD_CONFIGURATION}")
if(NOT HDF5_VOL_DAOS_BUILD_CONFIGURATION)
  set(HDF5_VOL_DAOS_BUILD_CONFIGURATION "Debug")
endif()
string(TOLOWER ${HDF5_VOL_DAOS_BUILD_CONFIGURATION} lower_hdf5_vol_daos_build_configuration)
set(CTEST_BUILD_CONFIGURATION ${HDF5_VOL_DAOS_BUILD_CONFIGURATION})

if(HDF5_VOL_DAOS_BUILD_CONFIGURATION MATCHES "Asan")
  set(HDF5_VOL_DAOS_MEMORYCHECK_TYPE "AddressSanitizer")
endif()
if(HDF5_VOL_DAOS_BUILD_CONFIGURATION MATCHES "Ubsan")
  set(HDF5_VOL_DAOS_MEMORYCHECK_TYPE "UndefinedBehaviorSanitizer")
endif()

# HDF5_VOL_DAOS_DASHBOARD_MODEL=Experimental | Nightly | Continuous
set(HDF5_VOL_DAOS_DASHBOARD_MODEL "$ENV{HDF5_VOL_DAOS_DASHBOARD_MODEL}")
if(NOT HDF5_VOL_DAOS_DASHBOARD_MODEL)
  set(HDF5_VOL_DAOS_DASHBOARD_MODEL "Experimental")
endif()
set(dashboard_model ${HDF5_VOL_DAOS_DASHBOARD_MODEL})

# Add current script to notes files
list(APPEND CTEST_UPDATE_NOTES_FILES "${CMAKE_CURRENT_LIST_FILE}")

# Number of jobs to build and keep going if some targets can't be made
set(CTEST_BUILD_FLAGS "-k -j4")

# Default num proc
set(MAX_NUMPROCS "4")

# Build shared libraries
set(hdf5_vol_daos_build_shared ON)
set(HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES $ENV{HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES})
if(HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES)
  message("Building static libraries")
  set(hdf5_vol_daos_build_shared OFF)
endif()

set(CTEST_CMAKE_GENERATOR "Unix Makefiles")

# Optional coverage options
set(HDF5_VOL_DAOS_DO_COVERAGE $ENV{HDF5_VOL_DAOS_DO_COVERAGE})
if(HDF5_VOL_DAOS_DO_COVERAGE)
  message("Enabling Coverage")
  set(CTEST_COVERAGE_COMMAND "$ENV{GCOV}")

  # don't run parallel coverage tests, no matter what.
  set(CTEST_TEST_ARGS PARALLEL_LEVEL 1)

  # needed by hdf5_vol_daos_common.cmake
  set(dashboard_do_coverage TRUE)

  # build suffix
  set(coverage_suffix "-coverage")
endif()

# Optional memcheck options
set(HDF5_VOL_DAOS_DO_MEMCHECK $ENV{HDF5_VOL_DAOS_DO_MEMCHECK})
if(HDF5_VOL_DAOS_DO_MEMCHECK OR HDF5_VOL_DAOS_MEMORYCHECK_TYPE)
  message("Enabling Memcheck")

  if(NOT HDF5_VOL_DAOS_MEMORYCHECK_TYPE)
    set(HDF5_VOL_DAOS_MEMORYCHECK_TYPE "Valgrind")
  endif()
  set(CTEST_MEMORYCHECK_TYPE ${HDF5_VOL_DAOS_MEMORYCHECK_TYPE})

  # Valgrind
  if(${HDF5_VOL_DAOS_MEMORYCHECK_TYPE} MATCHES "Valgrind")
    set(CTEST_MEMORYCHECK_COMMAND "/usr/bin/valgrind")
    set(CTEST_MEMORYCHECK_COMMAND_OPTIONS "--gen-suppressions=all --trace-children=yes --fair-sched=yes -q --leak-check=yes --show-reachable=yes --num-callers=50 -v")
    #set(CTEST_MEMORYCHECK_SUPPRESSIONS_FILE ${CTEST_SCRIPT_DIRECTORY}/ValgrindSuppressions.supp)
  endif()

  # Asan
  if(${HDF5_VOL_DAOS_MEMORYCHECK_TYPE} MATCHES "AddressSanitizer")
    # Must add verbosity / Error in build if no memory output file is produced
    set(CTEST_MEMORYCHECK_SANITIZER_OPTIONS "verbosity=1")
  endif()

  # Ubsan
  if(${HDF5_VOL_DAOS_MEMORYCHECK_TYPE} MATCHES "UndefinedBehaviorSanitizer")
    # Must add verbosity / Error in build if no memory output file is produced
    set(CTEST_MEMORYCHECK_SANITIZER_OPTIONS "verbosity=1")
  endif()

  # needed by hdf5_vol_daos_common.cmake
  set(dashboard_do_memcheck TRUE)
endif()

# Build name referenced in cdash
set(CTEST_BUILD_NAME "daos-master-async-gcc-${lower_hdf5_vol_daos_build_configuration}${coverage_suffix}")

set(dashboard_binary_name hdf5_vol_daos-${lower_hdf5_vol_daos_build_configuration})
if(NOT hdf5_vol_daos_build_shared)
  set(dashboard_binary_name ${dashboard_binary_name}-static)
endif()

# OS specific options
# set(CMAKE_FIND_ROOT_PATH $ENV{HOME}/install ${CMAKE_FIND_ROOT_PATH})

if($ENV{CC} MATCHES "^gcc.*")
  set(HDF5_VOL_DAOS_C_FLAGS "-Wall -Wextra -Wshadow -Winline -Wundef -Wcast-qual -Wconversion -Wmissing-prototypes -pedantic -Wpointer-arith -Wformat=2 -std=gnu99")
endif()
set(HDF5_VOL_DAOS_C_FLAGS ${HDF5_VOL_DAOS_C_FLAGS})
set(HDF5_VOL_DAOS_CXX_FLAGS ${HDF5_VOL_DAOS_CXX_FLAGS})

# Initial cache used to build hdf5_vol_daos, options can be modified here
set(dashboard_cache "
CMAKE_C_FLAGS:STRING=${HDF5_VOL_DAOS_C_FLAGS}
CMAKE_CXX_FLAGS:STRING=${HDF5_VOL_DAOS_CXX_FLAGS}

BUILD_SHARED_LIBS:BOOL=${hdf5_vol_daos_build_shared}
BUILD_TESTING:BOOL=ON

MEMORYCHECK_COMMAND:FILEPATH=${CTEST_MEMORYCHECK_COMMAND}
MEMORYCHECK_SUPPRESSIONS_FILE:FILEPATH=${CTEST_MEMORYCHECK_SUPPRESSIONS_FILE}
COVERAGE_COMMAND:FILEPATH=${CTEST_COVERAGE_COMMAND}

DAOS_POOL_SIZE:STRING=8
DAOS_SERVER_IFACE:STRING=em1
DAOS_SERVER_SCM_SIZE:STRING=16
DAOS_SERVER_TRANSPORT:STRING=ofi+sockets
HDF5_VOL_DAOS_ENABLE_COVERAGE:BOOL=${dashboard_do_coverage}
HDF5_VOL_TEST_ENABLE_PARALLEL:BOOL=ON
HDF5_VOL_TEST_ENABLE_PART:BOOL=ON
HDF5_VOL_TEST_ENABLE_ASYNC:BOOL=ON

MPIEXEC_MAX_NUMPROCS:STRING=${MAX_NUMPROCS}
")

include(${CTEST_DASHBOARD_ROOT}/${dashboard_source_name}/test/scripts/hdf5_vol_daos_common.cmake)
