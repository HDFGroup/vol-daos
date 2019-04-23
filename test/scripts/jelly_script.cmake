# This script takes in optional environment variables.
#   HDF5_VOL_DAOS_BUILD_CONFIGURATION=Debug | Release
#   HDF5_VOL_DAOS_DASHBOARD_MODEL=Experimental | Nightly | Continuous
#   HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES
#   HDF5_VOL_DAOS_DO_COVERAGE
#   HDF5_VOL_DAOS_DO_MEMCHECK

# HDF5_VOL_DAOS_BUILD_CONFIGURATION = Debug | Release
set(HDF5_VOL_DAOS_BUILD_CONFIGURATION "$ENV{HDF5_VOL_DAOS_BUILD_CONFIGURATION}")
if(NOT HDF5_VOL_DAOS_BUILD_CONFIGURATION)
  set(HDF5_VOL_DAOS_BUILD_CONFIGURATION "Debug")
endif()
string(TOLOWER ${HDF5_VOL_DAOS_BUILD_CONFIGURATION} lower_hdf5_vol_daos_build_configuration)
set(CTEST_BUILD_CONFIGURATION ${HDF5_VOL_DAOS_BUILD_CONFIGURATION})

# HDF5_VOL_DAOS_DASHBOARD_MODEL=Experimental | Nightly | Continuous
set(HDF5_VOL_DAOS_DASHBOARD_MODEL "$ENV{HDF5_VOL_DAOS_DASHBOARD_MODEL}")
if(NOT HDF5_VOL_DAOS_DASHBOARD_MODEL)
  set(HDF5_VOL_DAOS_DASHBOARD_MODEL "Experimental")
endif()
set(dashboard_model ${HDF5_VOL_DAOS_DASHBOARD_MODEL})

# Disable loop when HDF5_VOL_DAOS_DASHBOARD_MODEL=Continuous
set(HDF5_VOL_DAOS_NO_LOOP $ENV{HDF5_VOL_DAOS_NO_LOOP})
if(HDF5_VOL_DAOS_NO_LOOP)
  message("Disabling looping (if applicable)")
  set(dashboard_disable_loop TRUE)
endif()

# Disable source tree update and use current version
# set(CTEST_UPDATE_VERSION_ONLY TRUE)

# Number of jobs to build and verbose mode
set(CTEST_BUILD_FLAGS "-j4")

# Build shared libraries
set(hdf5_vol_daos_build_shared ON)
set(HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES $ENV{HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES})
if(HDF5_VOL_DAOS_BUILD_STATIC_LIBRARIES)
  message("Building static libraries")
  set(hdf5_vol_daos_build_shared OFF)
endif()

set(CTEST_CMAKE_GENERATOR "Unix Makefiles")
# Must point to the root where we can checkout/build/run the tests
set(CTEST_DASHBOARD_ROOT "$ENV{HDF5_VOL_DAOS_ROOT}")
# Must specify existing source directory
set(CTEST_SOURCE_DIRECTORY "$ENV{HDF5_VOL_DAOS_ROOT}/source")
# Give a site name
set(CTEST_SITE "jelly.ad.hdfgroup.org")
set(CTEST_TEST_TIMEOUT 180) # 180s timeout

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

  # add Coverage dir to the root so that we don't mess the non-coverage
  # dashboard.
  set(CTEST_DASHBOARD_ROOT "${CTEST_DASHBOARD_ROOT}/Coverage")
endif()

# Optional memcheck options
set(HDF5_VOL_DAOS_DO_MEMCHECK $ENV{HDF5_VOL_DAOS_DO_MEMCHECK})
set(HDF5_VOL_DAOS_MEMORYCHECK_TYPE "$ENV{HDF5_VOL_DAOS_MEMORYCHECK_TYPE}")
if(HDF5_VOL_DAOS_DO_MEMCHECK OR HDF5_VOL_DAOS_MEMORYCHECK_TYPE)
  message("Enabling Memcheck")

  if(NOT HDF5_VOL_DAOS_MEMORYCHECK_TYPE)
    set(HDF5_VOL_DAOS_MEMORYCHECK_TYPE "Valgrind")
  endif()
  string(TOLOWER "-${HDF5_VOL_DAOS_MEMORYCHECK_TYPE}" lower_hdf5_vol_daos_memorycheck_type)
  set(CTEST_MEMORYCHECK_TYPE ${HDF5_VOL_DAOS_MEMORYCHECK_TYPE})

  # Valgrind
  if(${HDF5_VOL_DAOS_MEMORYCHECK_TYPE} MATCHES "Valgrind")
    set(CTEST_MEMORYCHECK_COMMAND "/usr/bin/valgrind")
    set(CTEST_MEMORYCHECK_COMMAND_OPTIONS "--gen-suppressions=all --trace-children=yes --fair-sched=yes -q --leak-check=yes --show-reachable=yes --num-callers=50 -v")
    #set(CTEST_MEMORYCHECK_SUPPRESSIONS_FILE ${CTEST_SCRIPT_DIRECTORY}/ValgrindSuppressions.supp)
  endif()
  # Tsan
  if(${HDF5_VOL_DAOS_MEMORYCHECK_TYPE} MATCHES "ThreadSanitizer")
    set(HDF5_VOL_DAOS_MEMCHECK_FLAGS "-O1 -fsanitize=thread -fno-omit-frame-pointer -fPIC -fuse-ld=gold -pthread")
    # Must add verbosity / Error in build if no memory output file is produced
    set(CTEST_MEMORYCHECK_SANITIZER_OPTIONS "verbosity=1")
  endif()
  # Asan
  if(${HDF5_VOL_DAOS_MEMORYCHECK_TYPE} MATCHES "AddressSanitizer")
    set(HDF5_VOL_DAOS_MEMCHECK_FLAGS "-O1 -fsanitize=address -fno-omit-frame-pointer -fPIC -fuse-ld=gold -pthread")
    # Must add verbosity / Error in build if no memory output file is produced
    set(CTEST_MEMORYCHECK_SANITIZER_OPTIONS "verbosity=1")
  endif()

  # needed by hdf5_vol_daos_common.cmake
  set(dashboard_do_memcheck TRUE)
endif()

# Build name referenced in cdash
set(CTEST_BUILD_NAME "develop-x64-gcc-${lower_hdf5_vol_daos_build_configuration}${lower_hdf5_vol_daos_memorycheck_type}${coverage_suffix}")

set(dashboard_binary_name hdf5_vol_daos-${lower_hdf5_vol_daos_build_configuration})
if(NOT hdf5_vol_daos_build_shared)
  set(dashboard_binary_name ${dashboard_binary_name}-static)
endif()

# OS specific options
# set(CMAKE_FIND_ROOT_PATH $ENV{HOME}/install ${CMAKE_FIND_ROOT_PATH})

if($ENV{CC} MATCHES "^gcc.*")
  set(HDF5_VOL_DAOS_C_FLAGS "-Wall -Wextra -Wshadow -Winline -Wundef -Wcast-qual -Wconversion -Wmissing-prototypes -pedantic -Wpointer-arith -Wformat=2 -std=gnu99 ${HDF5_VOL_DAOS_MEMCHECK_FLAGS}")
endif()

# Initial cache used to build hdf5_vol_daos, options can be modified here
set(dashboard_cache "
CMAKE_C_FLAGS:STRING=${HDF5_VOL_DAOS_C_FLAGS}
CMAKE_CXX_FLAGS:STRING=${HDF5_VOL_DAOS_MEMCHECK_FLAGS}

BUILD_SHARED_LIBS:BOOL=${hdf5_vol_daos_build_shared}
BUILD_TESTING:BOOL=ON

MEMORYCHECK_COMMAND:FILEPATH=${CTEST_MEMORYCHECK_COMMAND}
MEMORYCHECK_SUPPRESSIONS_FILE:FILEPATH=${CTEST_MEMORYCHECK_SUPPRESSIONS_FILE}
COVERAGE_COMMAND:FILEPATH=${CTEST_COVERAGE_COMMAND}

DAOS_SERVER_IFACE:STRING=em1
DAOS_SERVER_TRANSPORT:STRING=na+sm
HDF5_VOL_DAOS_ENABLE_COVERAGE:BOOL=${dashboard_do_coverage}
HDF5_VOL_DAOS_USE_SYSTEM_HDF5:BOOL=ON
HDF5_VOL_TEST_ENABLE_PARALLEL:BOOL=ON
")

#set(ENV{CC}  /usr/bin/gcc)
#set(ENV{CXX} /usr/bin/g++)

include(${CTEST_SOURCE_DIRECTORY}/test/scripts/hdf5_vol_daos_common.cmake)

#######################################################################
