#-------------------------------------------------#
# CMake Build Script for the HDF5 DAOS VOL connector #
#-------------------------------------------------#

cmake_minimum_required (VERSION 3.10)
PROJECT (DAOS_VOL C)

#-----------------------------------------------------------------------------
# Instructions for use : Normal Build
#
# For standard build of the HDF5 DAOS VOL connector.
# Run cmake using the DAOS VOL source tree to generate a build tree.
# Enable/Disable options according to requirements and
# set CMAKE_INSTALL_PREFIX to the required install path.
# Make install can be used to install all components for system-wide use.
#
if ("${CMAKE_CURRENT_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_BINARY_DIR}")
    MESSAGE(FATAL_ERROR "\nERROR! ${PROJECT_NAME} DOES NOT SUPPORT IN SOURCE BUILDS!\n"
      "CMAKE_CURRENT_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
      " == CMAKE_CURRENT_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}\n"
      "NEXT STEPS:\n"
      "(1) Delete the CMakeCache.txt file and the CMakeFiles/ directory\n"
      "     under the source directory for ${PROJECT_NAME}, otherwise you\n"
      "     will not be able to configure ${PROJECT_NAME} correctly!\n"
      "      * For example, on linux machines do:\n"
      "        $ rm -r CMakeCache.txt CMakeFiles/\n"
      "(2) Create a different directory and configure ${PROJECT_NAME} in that directory.\n"
      "      * For example, on linux machines do:\n"
      "        $ mkdir MY_BUILD\n"
      "        $ cd MY_BUILD\n"
      "        $ cmake [OPTIONS] ..\n"
      )
endif ()

string(TIMESTAMP CONFIG_DATE "%Y-%m-%d")


#-----------------------------------------------------------------------------
# Set the core names of all the libraries
#-----------------------------------------------------------------------------
set (DAOS_VOL_LIB_CORENAME        "daosvol")
set (DAOS_VOL_TEST_LIB_CORENAME   "daosvol_test")

#-----------------------------------------------------------------------------
# Set the target names of all the libraries
#-----------------------------------------------------------------------------
set (DAOS_VOL_LIB_TARGET          "${DAOS_VOL_LIB_CORENAME}-static")
set (DAOS_VOL_LIBSH_TARGET        "${DAOS_VOL_LIB_CORENAME}-shared")
set (DAOS_VOL_TEST_LIB_TARGET     "${DAOS_VOL_TEST_LIB_CORENAME}-static")
set (DAOS_VOL_TEST_LIBSH_TARGET   "${DAOS_VOL_TEST_LIB_CORENAME}-shared")

#-----------------------------------------------------------------------------
# Define some CMake variables for use later in the project
#-----------------------------------------------------------------------------
set (DAOS_VOL_RESOURCES_DIR       ${DAOS_VOL_SOURCE_DIR}/config/cmake)
set (DAOS_VOL_RESOURCES_EXT_DIR   ${DAOS_VOL_SOURCE_DIR}/config/cmake_ext_mod)
set (DAOS_VOL_RESOURCES_MOD_DIR   ${DAOS_VOL_SOURCE_DIR}/config/cmake/modules)
set (DAOS_VOL_SRC_DIR             ${DAOS_VOL_SOURCE_DIR}/src)
set (DAOS_VOL_TEST_SRC_DIR        ${DAOS_VOL_SOURCE_DIR}/test)
set (DAOS_VOL_EXAMPLES_DIR        ${DAOS_VOL_SOURCE_DIR}/examples)
set (HDF5_DIR_NAME                "hdf5")
set (HDF5_DIR                     ${DAOS_VOL_SOURCE_DIR}/${HDF5_DIR_NAME})
set (HDF5_BINARY_DIR              ${DAOS_VOL_BINARY_DIR}/${HDF5_DIR_NAME})

#-----------------------------------------------------------------------------
# Define a CMake variable which the user can override to use
# a pre-built HDF5 distribution for building the DAOS VOL connector
#-----------------------------------------------------------------------------
set (PREBUILT_HDF5_DIR CACHE STRING "Directory of pre-built HDF5 distribution")


#-----------------------------------------------------------------------------
# Find UUID before building
#-----------------------------------------------------------------------------
find_package (uuid REQUIRED)
if (UUID_FOUND)
  include_directories(${UUID_INCLUDE_DIRS})
  set (LINK_LIBS ${LINK_LIBS} ${UUID_LIBRARIES})
endif ()


#-----------------------------------------------------------------------------
# Setup the RPATH for the installed executables
#-----------------------------------------------------------------------------
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE) 
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
   SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
ENDIF("${isSystemDir}" STREQUAL "-1")


#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects using FindH4H5.
#-----------------------------------------------------------------------------
if (NOT DAOS_VOL_EXPORTED_TARGETS)
  set (DAOS_VOL_EXPORTED_TARGETS "daosvol-targets")
endif ()


if(DEFINED ADDITIONAL_CMAKE_PREFIX_PATH AND EXISTS "${ADDITIONAL_CMAKE_PREFIX_PATH}")
  set (CMAKE_PREFIX_PATH ${ADDITIONAL_CMAKE_PREFIX_PATH} ${CMAKE_PREFIX_PATH})
endif()


#-----------------------------------------------------------------------------
# Setup the install directories
#-----------------------------------------------------------------------------
if (NOT DAOS_VOL_INSTALL_BIN_DIR)
  set (DAOS_VOL_INSTALL_BIN_DIR bin)
endif ()
if (NOT DAOS_VOL_INSTALL_LIB_DIR)
  if (APPLE)
    set (DAOS_VOL_INSTALL_FMWK_DIR ${CMAKE_INSTALL_FRAMEWORK_PREFIX})
  else (APPLE)
    set (DAOS_VOL_INSTALL_JAR_DIR lib)
  endif ()
  set (DAOS_VOL_INSTALL_LIB_DIR lib)
endif ()
if (NOT DAOS_VOL_INSTALL_INCLUDE_DIR)
  set (DAOS_VOL_INSTALL_INCLUDE_DIR include)
endif ()
if (NOT DAOS_VOL_INSTALL_DATA_DIR)
  if (NOT WIN32)
    if (APPLE)
      if (DAOS_VOL_BUILD_FRAMEWORKS)
        set (DAOS_VOL_INSTALL_EXTRA_DIR ../SharedSupport)
      else (DAOS_VOL_BUILD_FRAMEWORKS)
        set (DAOS_VOL_INSTALL_EXTRA_DIR share)
      endif ()
      set (DAOS_VOL_INSTALL_FWRK_DIR ${CMAKE_INSTALL_FRAMEWORK_PREFIX})
    endif ()
    set (DAOS_VOL_INSTALL_DATA_DIR share)
    set (DAOS_VOL_INSTALL_CMAKE_DIR share/cmake)
  else (NOT WIN32)
    set (DAOS_VOL_INSTALL_DATA_DIR ".")
    set (DAOS_VOL_INSTALL_CMAKE_DIR cmake)
  endif ()
endif ()

#-------------------------------------------------------------------------------
# Parse the full version number from README.md and include in DAOS_VOL_VERS_INFO
#-------------------------------------------------------------------------------
file (READ ${DAOS_VOL_SOURCE_DIR}/README.md _readme_contents)
string (REGEX REPLACE "^.*DAOS.?VOL.?connector.?version.([0-9][0-9]*).[0-9][0-9]*.[0-9][0-9]*.*$"
    "\\1" DAOS_VOL_VERS_MAJOR ${_readme_contents})
string (REGEX REPLACE "^.*DAOS.?VOL.?connector.?version.[0-9][0-9]*.([0-9][0-9]*).[0-9][0-9]*.*$"
    "\\1" DAOS_VOL_VERS_MINOR ${_readme_contents})
string (REGEX REPLACE "^.*DAOS.?VOL.?connector.?version.[0-9][0-9]*.[0-9][0-9]*.([0-9][0-9]*).*$"
    "\\1" DAOS_VOL_VERS_RELEASE ${_readme_contents})
message (STATUS "VERSION: ${DAOS_VOL_VERS_MAJOR}.${DAOS_VOL_VERS_MINOR}.${DAOS_VOL_VERS_RELEASE}")


#-------------------------------------------------------------------------------
# Basic DAOS VOL stuff here
#-------------------------------------------------------------------------------
set (DAOS_VOL_PACKAGE "daos-vol")
set (DAOS_VOL_PACKAGE_NAME "DAOS_VOL")
set (DAOS_VOL_PACKAGE_VERSION "${DAOS_VOL_VERS_MAJOR}.${DAOS_VOL_VERS_MINOR}.${DAOS_VOL_VERS_RELEASE}")
set (DAOS_VOL_PACKAGE_VERSION_MAJOR "${DAOS_VOL_VERS_MAJOR}.${DAOS_VOL_VERS_MINOR}")
set (DAOS_VOL_PACKAGE_VERSION_MINOR "${DAOS_VOL_VERS_RELEASE}")
if (NOT "${DAOS_VOL_VERS_SUBRELEASE}" STREQUAL "")
  set (DAOS_VOL_PACKAGE_VERSION_STRING "${DAOS_VOL_PACKAGE_VERSION}-${DAOS_VOL_VERS_SUBRELEASE}")
else (NOT "${DAOS_VOL_VERS_SUBRELEASE}" STREQUAL "")
  set (DAOS_VOL_PACKAGE_VERSION_STRING "${DAOS_VOL_PACKAGE_VERSION}")
endif ()

set (DAOS_VOL_PACKAGE_STRING "${DAOS_VOL_PACKAGE_NAME} ${DAOS_VOL_PACKAGE_VERSION_STRING}")
set (DAOS_VOL_PACKAGE_TARNAME "${DAOS_VOL_PACKAGE}${DAOS_VOL_PACKAGE_EXT}")
set (DAOS_VOL_PACKAGE_URL "http://www.hdfgroup.org")
set (DAOS_VOL_PACKAGE_BUGREPORT "help@hdfgroup.org")

#-----------------------------------------------------------------------------
# Include some macros for reusable code
#-----------------------------------------------------------------------------

#-------------------------------------------------------------------------------
macro (DAOS_VOL_SET_LIB_OPTIONS libtarget libname libtype)
  set (LIB_OUT_NAME "${libname}")
  # SOVERSION passed in ARGN when shared
  if (${libtype} MATCHES "SHARED")
    if (ARGN)
      set (PACKAGE_SOVERSION ${ARGN})
    else (ARGN)
      set (PACKAGE_SOVERSION ${DAOS_VOL_PACKAGE_SOVERSION})
    endif (ARGN)
    if (WIN32)
      set (LIBDAOS_VOL_VERSION ${DAOS_VOL_PACKAGE_VERSION_MAJOR})
    else (WIN32)
      set (LIBDAOS_VOL_VERSION ${DAOS_VOL_PACKAGE_VERSION})
    endif (WIN32)
    set_target_properties (${libtarget} PROPERTIES VERSION ${LIBDAOS_VOL_VERSION})
    if (WIN32)
        set (${LIB_OUT_NAME} "${LIB_OUT_NAME}-${PACKAGE_SOVERSION}")
    else (WIN32)
        set_target_properties (${libtarget} PROPERTIES SOVERSION ${PACKAGE_SOVERSION})
    endif (WIN32)
  endif (${libtype} MATCHES "SHARED")
  DAOS_VOL_SET_LIB_OPTIONS (${libtarget} ${LIB_OUT_NAME} ${libtype})

  #-- Apple Specific install_name for libraries
  if (APPLE)
    option (DAOS_VOL_BUILD_WITH_INSTALL_NAME "Build with library install_name set to the installation path" OFF)
    if (DAOS_VOL_BUILD_WITH_INSTALL_NAME)
      set_target_properties (${libtarget} PROPERTIES
          LINK_FLAGS "-current_version ${DAOS_VOL_PACKAGE_VERSION} -compatibility_version ${DAOS_VOL_PACKAGE_VERSION}"
          INSTALL_NAME_DIR "${CMAKE_INSTALL_PREFIX}/lib"
          BUILD_WITH_INSTALL_RPATH ${DAOS_VOL_BUILD_WITH_INSTALL_NAME}
      )
    endif (DAOS_VOL_BUILD_WITH_INSTALL_NAME)
    if (DAOS_VOL_BUILD_FRAMEWORKS)
      if (${libtype} MATCHES "SHARED")
        # adapt target to build frameworks instead of dylibs
        set_target_properties(${libtarget} PROPERTIES
            XCODE_ATTRIBUTE_INSTALL_PATH "@rpath"
            FRAMEWORK TRUE
            FRAMEWORK_VERSION ${DAOS_VOL_PACKAGE_VERSION_MAJOR}
            MACOSX_FRAMEWORK_IDENTIFIER org.hdfgroup.${libtarget}
            MACOSX_FRAMEWORK_SHORT_VERSION_STRING ${DAOS_VOL_PACKAGE_VERSION_MAJOR}
            MACOSX_FRAMEWORK_BUNDLE_VERSION ${DAOS_VOL_PACKAGE_VERSION_MAJOR})
      endif (${libtype} MATCHES "SHARED")
    endif (DAOS_VOL_BUILD_FRAMEWORKS)
  endif (APPLE)
endmacro (DAOS_VOL_SET_LIB_OPTIONS)

#-------------------------------------------------------------------------------
macro (TARGET_C_PROPERTIES wintarget libtype addcompileflags addlinkflags)
  if (MSVC)
    TARGET_MSVC_PROPERTIES (${wintarget} ${libtype} "${addcompileflags} ${WIN_COMPILE_FLAGS}" "${addlinkflags} ${WIN_LINK_FLAGS}")
  else ()
    set_target_properties (${wintarget} PROPERTIES COMPILE_FLAGS "${addcompileflags}" LINK_FLAGS "${addlinkflags}")
  endif ()
endmacro ()

#-------------------------------------------------------------------------------
macro (INSTALL_TARGET_PDB libtarget targetdestination targetcomponent)
  if (WIN32 AND MSVC)
    get_target_property (target_type ${libtarget} TYPE)
    if (${libtype} MATCHES "SHARED")
      set (targetfilename $<TARGET_PDB_FILE:${libtarget}>)
    else ()
      get_property (target_name TARGET ${libtarget} PROPERTY OUTPUT_NAME_RELWITHDEBINFO)
      set (targetfilename $<TARGET_FILE_DIR:${libtarget}>/${target_name}.pdb)
    endif ()
    install (
      FILES
          ${targetfilename}
      DESTINATION
          ${targetdestination}
      CONFIGURATIONS RelWithDebInfo
      COMPONENT ${targetcomponent}
  )
  endif ()
endmacro ()


#-----------------------------------------------------------------------------
# Setup output Directories
#-----------------------------------------------------------------------------
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Exectuables."
)
set (CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Libraries"
)
set (CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all static libraries."
)

if (WIN32)
  set (CMAKE_TEST_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${CMAKE_BUILD_TYPE})
else (WIN32)
  set (CMAKE_TEST_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
endif ()

#-----------------------------------------------------------------------------
# To include a library in the list exported by the project AT BUILD TIME,
# add it to this variable. This is NOT used by Make Install, but for projects
# which include the DAOS VOL as a sub-project within their build tree
#-----------------------------------------------------------------------------
set (DAOS_VOL_LIBRARIES_TO_EXPORT ${DAOS_VOL_LIB_TARGET} CACHE INTERNAL "Used to pass variables between directories" FORCE)
set (EXTERNAL_HEADER_LIST "")
set (EXTERNAL_LIBRARY_LIST "")
set (EXTERNAL_LIBRARYDLL_LIST "")

#-----------------------------------------------------------------------------
# Run all the CMake configuration tests for our build environment
#-----------------------------------------------------------------------------
#include (${DAOS_VOL_RESOURCES_DIR}/ConfigureChecks.cmake)

set (CMAKE_INCLUDE_CURRENT_DIR_IN_INTERFACE ON)

#-----------------------------------------------------------------------------
# Mac OS X Options
#-----------------------------------------------------------------------------
if (DAOS_VOL_BUILD_FRAMEWORKS AND NOT BUILD_SHARED_LIBS)
  set (BUILD_SHARED_LIBS ON CACHE BOOL "Build Shared Libraries")
endif ()

#-----------------------------------------------------------------------------
# Option to Build Shared and Static libs, default is static
#-----------------------------------------------------------------------------
option (BUILD_SHARED_LIBS "Build Shared Libraries" ON)
set (DAOS_VOL_ENABLE_SHARED_LIB NO)
if (BUILD_SHARED_LIBS)
  set (DAOS_VOL_ENABLE_SHARED_LIB YES)
  set (LINK_SHARED_LIBS ${LINK_LIBS})
endif ()
set (DAOS_VOL_ENABLE_STATIC_LIB YES)
set (CMAKE_POSITION_INDEPENDENT_CODE ON)

#-----------------------------------------------------------------------------
# Option to Build Static executables
#-----------------------------------------------------------------------------
option (BUILD_STATIC_EXECS "Build Static Executabless" OFF)
if (BUILD_STATIC_EXECS)
  if (NOT WIN32)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -static")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static")
  endif ()
endif ()

#-----------------------------------------------------------------------------
# Option to use code coverage
#-----------------------------------------------------------------------------
option (DAOS_VOL_ENABLE_COVERAGE "Enable code coverage for Libraries and Programs" OFF)
if (DAOS_VOL_ENABLE_COVERAGE)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O0 -fprofile-arcs -ftest-coverage")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -fprofile-arcs -ftest-coverage")
  set (LDFLAGS "${LDFLAGS} -fprofile-arcs -ftest-coverage")
endif ()

#-----------------------------------------------------------------------------
# Option to build the examples
#-----------------------------------------------------------------------------
option (DAOS_VOL_ENABLE_EXAMPLES "Indicate that building examples should be enabled" ON)

#-----------------------------------------------------------------------------
# Option to indicate enabling connector debugging output
#-----------------------------------------------------------------------------
option (DAOS_VOL_ENABLE_DEBUG "Indicate that connector debug output is used" OFF)
if (DAOS_VOL_ENABLE_DEBUG)
  set (DV_PLUGIN_DEBUG 1)
endif ()

#-----------------------------------------------------------------------------
# Option to indicate using a memory checker
#-----------------------------------------------------------------------------
option (DAOS_VOL_ENABLE_MEM_TRACKING "Indicate that a memory checker is used" OFF)
if (DAOS_VOL_ENABLE_MEM_TRACKING)
  set (DV_TRACK_MEM_USAGE 1)
endif ()

#-----------------------------------------------------------------------------
# Allow Visual Studio solution directories
#-----------------------------------------------------------------------------
# Provide a way for Visual Studio Express users to turn OFF the new FOLDER
# organization feature. Default to ON for non-Express users. Express users must
# explicitly turn off this option to build HDF5 in the Express IDE...
#
option (DAOS_VOL_USE_FOLDERS "Enable folder grouping of projects in IDEs." ON)
mark_as_advanced (DAOS_VOL_USE_FOLDERS)
if (DAOS_VOL_USE_FOLDERS)
  set_property (GLOBAL PROPERTY USE_FOLDERS ON)
endif ()
option (DAOS_VOL_NO_PACKAGES "CPACK - Disable packaging" OFF)
mark_as_advanced (DAOS_VOL_NO_PACKAGES)


#-----------------------------------------------------------------------------
# Generate the daos_vol_config.h file containing user settings needed by
# compilation
#-----------------------------------------------------------------------------
configure_file (${DAOS_VOL_RESOURCES_DIR}/dv_cmake_config.h.in ${DAOS_VOL_BINARY_DIR}/daos_vol_config.h @ONLY)

#-----------------------------------------------------------------------------
# Include the main src directory
#-----------------------------------------------------------------------------
set (DAOS_VOL_INCLUDE_DIRECTORIES
    ${DAOS_VOL_SRC_DIR}
    ${DAOS_VOL_BINARY_DIR}
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
)

#-----------------------------------------------------------------------------
# Include the HDF5 distribution directory
#-----------------------------------------------------------------------------
if (PREBUILT_HDF5_DIR)
  set (DAOS_VOL_INCLUDE_DIRECTORIES
      ${DAOS_VOL_INCLUDE_DIRECTORIES}
      ${PREBUILT_HDF5_DIR}/include
  )
else ()
  set (DAOS_VOL_INCLUDE_DIRECTORIES
      ${DAOS_VOL_INCLUDE_DIRECTORIES}    
      ${HDF5_DIR}
      ${HDF5_BINARY_DIR}
  )
endif ()

INCLUDE_DIRECTORIES (${DAOS_VOL_INCLUDE_DIRECTORIES})


#-----------------------------------------------------------------------------
# When building utility executables that generate other (source) files :
# we make use of the following variables defined in the root CMakeLists.
# Certain systems may add /Debug or /Release to output paths
# and we need to call the executable from inside the CMake configuration
#-----------------------------------------------------------------------------
set (EXE_EXT "")
if (WIN32)
  set (EXE_EXT ".exe")
  if (NOT CYGWIN)
    add_definitions (-D_BIND_TO_CURRENT_VCLIBS_VERSION=1)
    add_definitions (-D_CRT_SECURE_NO_WARNINGS)
    add_definitions (-D_CONSOLE)
  endif (NOT CYGWIN)
endif ()

if (MSVC)
  set (CMAKE_MFC_FLAG 0)
  set (WIN_COMPILE_FLAGS "")
  set (WIN_LINK_FLAGS "")
endif ()

set (MAKE_SYSTEM)
if (CMAKE_BUILD_TOOL MATCHES "make")
  set (MAKE_SYSTEM 1)
endif ()

set (CFG_INIT "/${CMAKE_CFG_INTDIR}")
if (MAKE_SYSTEM)
  set (CFG_INIT "")
endif ()

#-----------------------------------------------------------------------------
# Add some definitions for Debug Builds
#-----------------------------------------------------------------------------
if (CMAKE_BUILD_TYPE MATCHES Debug)
  add_definitions (-DDEBUG)
else (CMAKE_BUILD_TYPE MATCHES Debug)
  add_definitions (-DNDEBUG)
endif ()

include (${DAOS_VOL_RESOURCES_DIR}/HDFCompilerFlags.cmake)

#-----------------------------------------------------------------------------
# Build HDF5, unless the user has specified to use
# a pre-built HDF5 distribution
#-----------------------------------------------------------------------------
if (NOT PREBUILT_HDF5_DIR)
  add_subdirectory(${HDF5_DIR} ${PROJECT_BINARY_DIR}/${HDF5_DIR_NAME})
else ()
  link_directories(${PREBUILT_HDF5_DIR}/lib)
  set (HDF5_LIBRARIES_TO_EXPORT
       libhdf5.so
       libhdf5_hl.so
       libhdf5.a
       libhdf5_hl.a
  )
endif ()

#-----------------------------------------------------------------------------
# Build the DAOS VOL
#-----------------------------------------------------------------------------
add_subdirectory(${DAOS_VOL_SRC_DIR} ${PROJECT_BINARY_DIR}/src)

#-----------------------------------------------------------------------------
# Add the DAOS VOL test Target to the build if testing is enabled
#-----------------------------------------------------------------------------
option (BUILD_TESTING "Build DAOS VOL Unit Testing" ON)
if (BUILD_TESTING)
  set (DART_TESTING_TIMEOUT 1200
      CACHE INTEGER
      "Timeout in seconds for each test (default 1200=20minutes)"
  )

  enable_testing ()
  include (CTest)

  add_subdirectory(${DAOS_VOL_TEST_SRC_DIR} ${PROJECT_BINARY_DIR}/test)

  include (${DAOS_VOL_SOURCE_DIR}/CTestConfig.cmake)
endif ()

#-----------------------------------------------------------------------------
# Add the DAOS VOL examples Target to the build
#-----------------------------------------------------------------------------
if (DAOS_VOL_ENABLE_EXAMPLES)
  add_subdirectory(${DAOS_VOL_EXAMPLES_DIR} ${PROJECT_BINARY_DIR}/examples)
endif ()

include (CMakeInstallation.cmake)