#!/bin/sh     
#
# HDF5 DAOS VOL connector -- CMake build script.
# This script is provided for convenience and does not replace standard ways
# of building that package with cmake or ccmake. For instructions, please refer
# to the README.md file that is included in the DAOS VOL source directory.

# Get the directory of the script itself
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set the default install directory
INSTALL_DIR="${SCRIPT_DIR}/daos_vol_install"

# Set the default build directory
BUILD_DIR="${SCRIPT_DIR}/daos_vol_build"

# By default, tell CMake to generate Unix Makefiles
CMAKE_GENERATOR="Unix Makefiles"

# Determine the number of processors to use when
# building in parallel with Autotools make
NPROCS=0

# Extra compiler options passed to the various steps, such as -Wall
COMP_OPTS=

# Extra options passed to the DAOS VOL's CMake script
PLUGIN_DEBUG_OPT=
MEM_TRACK_OPT=

HDF5_DIR_OPTS=
DAOS_DIR_OPTS=
CART_DIR_OPTS=
UUID_DIR_OPTS=


echo
echo "*************************"
echo "* DAOS VOL build script *"
echo "*************************"
echo

usage()
{
    echo "usage: $0 [OPTIONS]"
    echo
    echo "      -h      Print this help message."
    echo
    echo "      -d      Enable debugging output in the DAOS VOL."
    echo
    echo "      -m      Enable memory tracking in the DAOS VOL."
    echo
    echo "      -G      Specify the CMake Generator to use for the build"
    echo "              files created. Default is 'Unix Makefiles'."
    echo
    echo "      -B DIR  Specifies the directory that CMake should use as"
    echo "              the build tree location. Default is"
    echo "              'source directory/daos_vol_build'."
    echo "              Note that the DAOS VOL does not support in-source"
    echo "              CMake builds."
    echo
    echo "      -P DIR  Similar to '-DCMAKE_INSTALL_PREFIX=DIR', specifies"
    echo "              where the DAOS VOL should be installed to. Default"
    echo "              is 'source directory/daos_vol_install'."
    echo
    echo "      -H DIR  To specify a directory where HDF5 has already"
    echo "              been built."
    echo
    echo "      -D DIR  To specify the top-level directory where the DAOS"
    echo "              library is installed, if it was not installed to a"
    echo "              system directory."
    echo
    echo "      -C DIR  To specify the top-level directory where the CART"
    echo "              library is installed, if it was not installed to a"
    echo "              system directory."
    echo
    echo "      -U DIR  To specify the top-level directory where the UUID"
    echo "              library is installed, if it was not installed to a"
    echo "              system directory."
    echo
}

optspec=":hdmG:B:P:H:D:C:U:-"
while getopts "$optspec" optchar; do
    case "${optchar}" in
    h)
        usage
        exit 0
        ;;
    d)
        PLUGIN_DEBUG_OPT="-DHDF5_VOL_DAOS_ENABLE_DEBUG:BOOL=ON"
        echo "Enabled DAOS VOL connector debugging"
        echo
        ;;
    m)
        MEM_TRACK_OPT="-DHDF5_VOL_DAOS_ENABLE_MEM_TRACKING:BOOL=ON"
        echo "Enabled DAOS VOL connector memory tracking"
        echo
        ;;
    G)
        CMAKE_GENERATOR="$OPTARG"
        echo "CMake Generator set to: ${CMAKE_GENERATOR}"
        echo
        ;;
    B)
        BUILD_DIR="$OPTARG"
        echo "Build directory set to: ${BUILD_DIR}"
        echo
        ;;
    P)
        INSTALL_DIR="$OPTARG"
        echo "Prefix set to: ${INSTALL_DIR}"
        echo
        ;;
    H)
        HDF5_DIR="$OPTARG"
        HDF5_DIR_OPTS="-DHDF5_DIR:PATH=${HDF5_DIR}/share/cmake/hdf5"
        echo "Set HDF5 install directory to: ${HDF5_DIR}"
        echo
        ;;
    D)
        DAOS_DIR="$OPTARG"
        DAOS_INCLUDE_DIR="${DAOS_DIR}/include"
        DAOS_LIBRARY="${DAOS_DIR}/lib/libdaos.so"
        DAOS_DIR_OPTS="-DDAOS_INCLUDE_DIR:PATH=${DAOS_INCLUDE_DIR} -DDAOS_LIBRARY:FILEPATH=${DAOS_LIBRARY}"
        echo "DAOS include directory set to: ${DAOS_INCLUDE_DIR}"
        echo "DAOS library set to: ${DAOS_LIBRARY}"
        echo
        ;;
    C)
        CART_DIR="$OPTARG"
        CART_INCLUDE_DIR="${CART_DIR}/include"
        CART_LIBRARY="${CART_DIR}/lib/libcart.so"
        CART_DIR_OPTS="-DCART_INCLUDE_DIR:PATH=${CART_INCLUDE_DIR} -DCART_LIBRARY:FILEPATH=${CART_LIBRARY}"
        echo "CART include directory set to: ${CART_INCLUDE_DIR}"
        echo "CART library set to: ${CART_LIBRARY}"
        echo
        ;;
    U)
        UUID_DIR="$OPTARG"
        UUID_INCLUDE_DIR="${UUID_DIR}/include"
        UUID_LIBRARY="${UUID_DIR}/lib/libuuid.so"
        UUID_DIR_OPTS="-DUUID_INCLUDE_DIR:PATH=${UUID_INCLUDE_DIR} -DUUID_LIBRARY:FILEPATH=${UUID_LIBRARY}"
        echo "UUID include directory set to: ${UUID_INCLUDE_DIR}"
        echo "UUID library set to: ${UUID_LIBRARY}"
        echo
        ;;
   *)
        if [ "$OPTERR" != 1 ] || case $optspec in :*) ;; *) false; esac; then
            echo "ERROR: non-option argument: '-${OPTARG}'" >&2
            echo
            usage
            echo
            exit 1
        fi
        ;;
    esac
done


# Try to determine a good number of cores to use for parallelizing both builds
if [ "$NPROCS" -eq "0" ]; then
    NPROCS=`getconf _NPROCESSORS_ONLN 2> /dev/null`

    # Handle FreeBSD
    if [ -z "$NPROCS" ]; then
        NPROCS=`getconf NPROCESSORS_ONLN 2> /dev/null`
    fi
fi

# Once HDF5 has been built, build the DAOS VOL connector against HDF5.
echo "**********************************************"
echo "* Building DAOS VOL connector and test suite *"
echo "**********************************************"
echo

# Clean out the old CMake cache
if [ -f "${BUILD_DIR}/CMakeCache.txt" ]; then
  rm -f "${BUILD_DIR}/CMakeCache.txt"
fi

cmake -G "${CMAKE_GENERATOR}" -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_DIR} \
  ${HDF5_DIR_OPTS} ${DAOS_DIR_OPTS} ${CART_DIR_OPTS} ${UUID_DIR_OPTS}    \
  "${PLUGIN_DEBUG_OPT}" "${MEM_TRACK_OPT}" \
  -S "${SCRIPT_DIR}" -B "${BUILD_DIR}"

echo "***********************************"
echo "* Build files have been generated *"
echo "***********************************"

cmake --build "${BUILD_DIR}" -j ${NPROCS} --target install

exit 0
