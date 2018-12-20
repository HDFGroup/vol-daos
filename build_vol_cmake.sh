#!/bin/sh
#
# Copyright by The HDF Group.                                              
# All rights reserved.                                                     
#
# This file is part of HDF5. The full HDF5 copyright notice, including     
# terms governing use, modification, and redistribution, is contained in   
# the files COPYING and Copyright.html.  COPYING can be found at the root  
# of the source code distribution tree; Copyright.html can be found at the 
# root level of an installed copy of the electronic document set and is    
# linked from the top-level documents page.  It can also be found at       
# http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have access  
# to either file, you may request a copy from help@hdfgroup.org.           
#
# A script used to first configure and build the HDF5 source distribution
# included with the DAOS VOL plugin source code, and then use that built
# HDF5 to build the DAOS VOL plugin itself.

# Get the directory of the script itself
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set the default install directory
INSTALL_DIR="${SCRIPT_DIR}/daos_vol_build"

# Set the default build directory
BUILD_DIR="${SCRIPT_DIR}/daos_vol_cmake_build_files"

# By default, tell CMake to generate Unix Makefiles
CMAKE_GENERATOR="Unix Makefiles"

# Determine the number of processors to use when
# building in parallel with Autotools make
NPROCS=0

# Default is to not build tools due to circular dependency on VOL being
# already built
build_tools=false

# Compiler flags for linking with UUID
UUID_DIR=""
UUID_LINK="-luuid"

# Compiler flag for linking with the built DAOS VOL
DAOS_VOL_LINK="-ldaosvol"

# Extra compiler options passed to the various steps, such as -Wall
COMP_OPTS="-Wall -pedantic -Wunused-macros"

# Extra options passed to the DAOS VOL's CMake script
PLUGIN_DEBUG_OPT=
MEM_TRACK_OPT=
PREBUILT_HDF5_OPT=


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
    echo "      -t      Build the tools with DAOS VOL support. Note"
    echo "              that due to a circular build dependency, this"
    echo "              option should not be chosen until after the"
    echo "              included HDF5 source distribution and the"
    echo "              DAOS VOL plugin have been built once."
    echo
    echo "      -G      Specify the CMake Generator to use for the build"
    echo "              files created. Default is 'Unix Makefiles'."
    echo
    echo "      -P DIR  Similar to '-DCMAKE_INSTALL_PREFIX=DIR', specifies"
    echo "              where the DAOS VOL should be installed to. Default"
    echo "              is 'source directory/daos_vol_build'."
    echo
    echo "      -H DIR  To specify a directory where HDF5 has already"
    echo "              been built."
    echo
    echo "      -B DIR  Specifies the directory that CMake should use as"
    echo "              the build tree location. Default is"
    echo "              'source directory/daos_vol_cmake_build_files'."
    echo "              Note that the DAOS VOL does not support in-source"
    echo "              CMake builds."
    echo
    echo "      -U DIR  To specify the top-level directory where the UUID"
    echo "              library is installed, if it was not installed to a"
    echo "              system directory."
    echo
}

optspec=":htdmG:H:C:Y:U:P:-"
while getopts "$optspec" optchar; do
    case "${optchar}" in
    h)
        usage
        exit 0
        ;;
    d)
        PLUGIN_DEBUG_OPT="-DDAOS_VOL_ENABLE_DEBUG=ON"
        echo "Enabled DAOS VOL plugin debugging"
        echo
        ;;
    m)
        MEM_TRACK_OPT="-DDAOS_VOL_ENABLE_MEM_TRACKING=ON"
        echo "Enabled DAOS VOL plugin memory tracking"
        echo
        ;;
    t)
        build_tools=true
        echo "Building tools with DAOS VOL support"
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
        PREBUILT_HDF5_OPT="-DPREBUILT_HDF5_DIR=$OPTARG"
        echo "Set HDF5 install directory to: $OPTARG"
        echo
        ;;
    U)
        UUID_DIR="$OPTARG"
        UUID_LINK="-L${UUID_DIR}/lib ${UUID_LINK}"
        CMAKE_OPTS="--with-uuid=${UUID_DIR} ${CMAKE_OPTS}"
        echo "UUID lib directory set to: ${UUID_DIR}"
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

# Once HDF5 has been built, build the DAOS VOL plugin against HDF5.
echo "*******************************************"
echo "* Building DAOS VOL plugin and test suite *"
echo "*******************************************"
echo

# Make sure to checkout HDF5 submodule
if [ -n "${PREBUILT_HDF5_OPT}" ] && [ -z "$(ls -A ${SCRIPT_DIR}/${HDF5_DIR})" ]; then
    git submodule init
    git submodule update
fi

mkdir -p "${BUILD_DIR}"
mkdir -p "${INSTALL_DIR}"

# Clean out the old CMake cache
rm -f "${BUILD_DIR}/CMakeCache.txt"

cd "${BUILD_DIR}"

cmake -G "${CMAKE_GENERATOR}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" "${PREBUILT_HDF5_OPT}" "${PLUGIN_DEBUG_OPT}" "${MEM_TRACK_OPT}" "${SCRIPT_DIR}"

echo "Build files have been generated for CMake generator '${CMAKE_GENERATOR}'"

# Build with autotools make by default
if [ "${CMAKE_GENERATOR}" = "Unix Makefiles" ]; then
  make -j${NPROCS} && make install || exit 1
fi

exit 0
