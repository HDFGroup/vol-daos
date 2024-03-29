cmake_minimum_required(VERSION 2.8.12.2 FATAL_ERROR)

# Setup cmake policies.
foreach(policy
    CMP0012
    CMP0013
    CMP0014
    CMP0022 # CMake 2.8.12
    CMP0025 # CMake 3.0
    CMP0053 # CMake 3.1
    CMP0054 # CMake 3.1
    CMP0074 # CMake 3.12
    CMP0075 # CMake 3.12
    CMP0083 # CMake 3.14
    CMP0093 # CMake 3.15
  )
  if(POLICY ${policy})
    cmake_policy(SET ${policy} NEW)
  endif()
endforeach()

# Set a consistent MACOSX_RPATH default across all CMake versions.
# When CMake 2.8.12 is required, change this default to 1.
# When CMake 3.0.0 is required, remove this block (see CMP0042).
if(NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_MACOSX_RPATH 0)
endif()

project(HDF5_VOL_DAOS C)

#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if(NOT HDF5_VOL_DAOS_INSTALL_BIN_DIR)
  set(HDF5_VOL_DAOS_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT HDF5_VOL_DAOS_INSTALL_LIB_DIR)
  set(HDF5_VOL_DAOS_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT HDF5_VOL_DAOS_INSTALL_INCLUDE_DIR)
  # Interface include will default to prefix/include
  set(HDF5_VOL_DAOS_INSTALL_INTERFACE include)
  set(HDF5_VOL_DAOS_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
else()
  set(HDF5_VOL_DAOS_INSTALL_INTERFACE ${HDF5_VOL_DAOS_INSTALL_INCLUDE_DIR})
endif()
if(NOT HDF5_VOL_DAOS_INSTALL_DATA_DIR)
  set(HDF5_VOL_DAOS_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()

# Setting this ensures that "make install" will leave rpaths to external
# libraries intact on "make install". This ensures that one can install a
# version of HDF5 DAOS on the build machine without any issues. If this not
# desired, simply specify CMAKE_INSTALL_RPATH_USE_LINK_PATH when configuring
# HDF5 VOL DAOS and "make install" will strip all rpaths, which is default
# behavior.
if(NOT CMAKE_INSTALL_RPATH_USE_LINK_PATH)
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

#------------------------------------------------------------------------------
# Set module path
#------------------------------------------------------------------------------
set(HDF5_VOL_DAOS_CMAKE_MODULE_PATH "${HDF5_VOL_DAOS_SOURCE_DIR}/CMake")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${HDF5_VOL_DAOS_CMAKE_MODULE_PATH})

#------------------------------------------------------------------------------
# Version information
#------------------------------------------------------------------------------
include(${HDF5_VOL_DAOS_CMAKE_MODULE_PATH}/Git/Git.cmake)
include(Hdf5VolDaosDetermineVersion)
# Hardcoded version variables are read-in from a separate file. This makes it
# easier to have a script to update version numbers automatically.
file(STRINGS version.txt version_txt)
extract_version_components("${version_txt}" "${PROJECT_NAME}")
determine_version(${HDF5_VOL_DAOS_SOURCE_DIR} ${GIT_EXECUTABLE} "${PROJECT_NAME}")
set(HDF5_VOL_DAOS_PACKAGE "hdf5_vol_daos")
set(HDF5_VOL_DAOS_PACKAGE_NAME "HDF5_VOL_DAOS")
set(HDF5_VOL_DAOS_PACKAGE_DESCRIPTION "HDF5 VOL connector for DAOS")
set(HDF5_VOL_DAOS_PACKAGE_URL "https://git.hdfgroup.org/projects/HDF5VOL/repos/daos-vol/")
set(HDF5_VOL_DAOS_PACKAGE_VENDOR "The HDF Group")
message(STATUS "Configuring ${HDF5_VOL_DAOS_PACKAGE} v${HDF5_VOL_DAOS_VERSION_FULL}")

#------------------------------------------------------------------------------
# Setup CMake Environment
#------------------------------------------------------------------------------
if(APPLE AND NOT HDF5_VOL_DAOS_EXTERNALLY_CONFIGURED)
  # We are doing a unix-style install i.e. everything will be installed in
  # CMAKE_INSTALL_PREFIX/bin and CMAKE_INSTALL_PREFIX/lib etc. as on other unix
  # platforms. We still need to setup CMAKE_INSTALL_NAME_DIR correctly so that
  # the binaries point to appropriate location for the libraries.

  # 1. Make CMAKE_INSTALL_PREFIX publicly accessible, if it was hidden in
  #    previous pass
  get_property(is_internal CACHE CMAKE_INSTALL_PREFIX PROPERTY TYPE)
  if(is_internal STREQUAL "INTERNAL")
    set(CMAKE_INSTALL_PREFIX ${CACHED_CMAKE_INSTALL_PREFIX} CACHE PATH "Install prefix" FORCE)
  else()
    set(CMAKE_INSTALL_PREFIX ${CACHED_CMAKE_INSTALL_PREFIX} CACHE PATH "Install prefix")
  endif()
  unset(MACOSX_APP_INSTALL_PREFIX CACHE)

  set(CMAKE_INSTALL_NAME_DIR "@rpath")
  mark_as_advanced(
    CMAKE_OSX_ARCHITECTURES
    CMAKE_OSX_DEPLOYMENT_TARGET
    CMAKE_OSX_SYSROOT
    )
else()
  if(WIN32)
    message("The HDF5 DAOS VOL connector is not supported on this platform." FATAL_ERROR)
  endif()
endif()

#------------------------------------------------------------------------------
if(NOT HDF5_VOL_DAOS_EXTERNALLY_CONFIGURED)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
  )
  set(EXECUTABLE_OUTPUT_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Libraries"
  )
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all static libraries."
  )
endif()

#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if("${HDF5_VOL_DAOS_SOURCE_DIR}" STREQUAL "${HDF5_VOL_DAOS_BINARY_DIR}")
  message(FATAL_ERROR
    "HDF5 DAOS VOL requires an out of source Build. "
    "Please create a separate binary directory and run CMake there.")
endif()

#------------------------------------------------------------------------------
# Set a default build type if none was specified
#------------------------------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo" "Asan" "Ubsan")
endif()

if(NOT CMAKE_C_FLAGS AND CMAKE_COMPILER_IS_GNUCC)
  message(STATUS "GCC detected, setting additional flags")
  set(CMAKE_C_FLAGS "-Wall -Wextra -Winline -Wcast-qual -std=gnu99 -Wshadow" CACHE STRING "Flags used by the compiler during all build types." FORCE)
endif()

# Detect Asan/Ubsan compiler flags
include(CheckAsan)
include(CheckUbsan)

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT HDF5_VOL_DAOS_EXPORTED_TARGETS)
  set(HDF5_VOL_DAOS_EXPORTED_TARGETS "${HDF5_VOL_DAOS_PACKAGE}-targets")
endif()

#------------------------------------------------------------------------------
# Choose static or shared libraries.
#------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build with shared libraries." ON)
if(BUILD_SHARED_LIBS)
  set(H5VL_DAOS_BUILD_SHARED_LIBS 1)
  set(HDF5_VOL_DAOS_LIBTYPE SHARED)
else()
  message(FATAL_ERROR
    "HDF5 DAOS VOL requires shared libraries.")
  set(H5VL_DAOS_BUILD_SHARED_LIBS 0)
  set(HDF5_VOL_DAOS_LIBTYPE STATIC)
endif()
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

#------------------------------------------------------------------------------
# Enable debug output.
#------------------------------------------------------------------------------
option(HDF5_VOL_DAOS_ENABLE_DEBUG "Enable debug statements." OFF)
if(HDF5_VOL_DAOS_ENABLE_DEBUG)
  set(H5VL_DAOS_HAS_DEBUG 1)
  set(DV_PLUGIN_DEBUG 1)
else()
  set(H5VL_DAOS_HAS_DEBUG 0)
  set(DV_PLUGIN_DEBUG 0)
  endif()

#-------------------------------------------------------------------------------
function(hdf5_vol_daos_set_lib_options libtarget libname libtype)
  if(${libtype} MATCHES "SHARED")
    set(LIB_RELEASE_NAME "${libname}")
    set(LIB_DEBUG_NAME "${libname}_debug")
    set(LIB_VERSION ${HDF5_VOL_DAOS_VERSION}.${HDF5_VOL_DAOS_VERSION_PATCH})
    set(API_VERSION ${HDF5_VOL_DAOS_VERSION_MAJOR})
  else()
    # if the generator supports configuration types or if the CMAKE_BUILD_TYPE has a value
    if(CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE)
      set(LIB_RELEASE_NAME "${libname}")
      set(LIB_DEBUG_NAME "${libname}_debug")
    else()
      set(LIB_RELEASE_NAME "lib${libname}")
      set(LIB_DEBUG_NAME "lib${libname}_debug")
    endif()
  endif()

  set_target_properties(${libtarget}
      PROPERTIES
      OUTPUT_NAME_DEBUG          ${LIB_DEBUG_NAME}
      OUTPUT_NAME_RELEASE        ${LIB_RELEASE_NAME}
      OUTPUT_NAME_MINSIZEREL     ${LIB_RELEASE_NAME}
      OUTPUT_NAME_RELWITHDEBINFO ${LIB_RELEASE_NAME}
      OUTPUT_NAME_ASAN           ${LIB_DEBUG_NAME}
      OUTPUT_NAME_UBSAN          ${LIB_DEBUG_NAME}
      VERSION                    ${LIB_VERSION}
      SOVERSION                  ${API_VERSION}
  )
endfunction()

# Avoid explicitly including system include paths
set(HDF5_VOL_DAOS_SYSTEM_INCLUDE_PATH ${CMAKE_SYSTEM_INCLUDE_PATH}
  ${CMAKE_C_IMPLICIT_INCLUDE_DIRECTORIES})
set(HDF5_VOL_DAOS_SYSTEM_INCLUDE_PATH /usr/include ${HDF5_VOL_DAOS_SYSTEM_INCLUDE_PATH})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(NOT HDF5_VOL_DAOS_EXTERNALLY_CONFIGURED)
  option(HDF5_VOL_DAOS_ENABLE_COVERAGE "Enable coverage." OFF)
  if(HDF5_VOL_DAOS_ENABLE_COVERAGE)
    set(COVERAGE_FLAGS "-fprofile-arcs -ftest-coverage" CACHE STRING
      "Flags to the coverage program that CTest uses to perform coverage inspection"
    )
    mark_as_advanced(COVERAGE_FLAGS)
  endif()

  macro(set_coverage_flags target)
    set_target_properties(${target}
      PROPERTIES
        COMPILE_FLAGS ${COVERAGE_FLAGS}
        LINK_FLAGS ${COVERAGE_FLAGS}
    )
  endmacro()
endif()

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------
if (NOT HDF5_VOL_DAOS_PACKAGE_SOURCE) # skip when packaging source
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
endif()

#-----------------------------------------------------------------------------
# Build doxygen documentation.
#-----------------------------------------------------------------------------
option(BUILD_DOCUMENTATION "Build documentation." OFF)
#if(BUILD_DOCUMENTATION)
#  add_subdirectory(Documentation/Doxygen)
#endif()

#-----------------------------------------------------------------------------
# Examples
#-----------------------------------------------------------------------------
option(BUILD_EXAMPLES "Build examples." OFF)
if(BUILD_EXAMPLES)
  # BuildExamples.cmake builds the examples as a separate project. This ensures
  # that examples can be built by themselves as well as avoiding pollution of
  # the target space with targets (and other things) from examples.
  include(${CMAKE_CURRENT_SOURCE_DIR}/examples/BuildExamples.cmake)
endif()

#-----------------------------------------------------------------------------
# Testing
#-----------------------------------------------------------------------------
option(BUILD_TESTING "Build testing." ON)
if(NOT HDF5_VOL_DAOS_EXTERNALLY_CONFIGURED AND BUILD_TESTING)
  enable_testing()
  include(CTest)
  add_subdirectory(test)
endif()

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the build directory
#-----------------------------------------------------------------------------
set(HDF5_VOL_DAOS_CONFIG_INSTALLED FALSE)
configure_file(
  ${HDF5_VOL_DAOS_SOURCE_DIR}/CMake/${HDF5_VOL_DAOS_PACKAGE}-config.cmake.in
  ${HDF5_VOL_DAOS_BINARY_DIR}/${HDF5_VOL_DAOS_PACKAGE}-config.cmake @ONLY
)

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the install directory
#-----------------------------------------------------------------------------
set(HDF5_VOL_DAOS_CONFIG_INSTALLED TRUE)
configure_file(
  ${HDF5_VOL_DAOS_SOURCE_DIR}/CMake/${HDF5_VOL_DAOS_PACKAGE}-config.cmake.in
  ${HDF5_VOL_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_VOL_DAOS_PACKAGE}-config.cmake @ONLY
)

install(
  FILES
    ${HDF5_VOL_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_VOL_DAOS_PACKAGE}-config.cmake
  DESTINATION
    ${HDF5_VOL_DAOS_INSTALL_DATA_DIR}/cmake/${HDF5_VOL_DAOS_PACKAGE}
)

#-----------------------------------------------------------------------------
# Configure the config-version.cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
  ${HDF5_VOL_DAOS_SOURCE_DIR}/CMake/${HDF5_VOL_DAOS_PACKAGE}-config-version.cmake.in
  ${HDF5_VOL_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_VOL_DAOS_PACKAGE}-config-version.cmake @ONLY
)

install(
  FILES
    ${HDF5_VOL_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_VOL_DAOS_PACKAGE}-config-version.cmake
  DESTINATION
    ${HDF5_VOL_DAOS_INSTALL_DATA_DIR}/cmake/${HDF5_VOL_DAOS_PACKAGE}
)

#-----------------------------------------------------------------------------
# For automake compatibility, also provide a pkgconfig file
#-----------------------------------------------------------------------------
configure_file(
  ${HDF5_VOL_DAOS_SOURCE_DIR}/CMake/${HDF5_VOL_DAOS_PACKAGE}.pc.in
  ${HDF5_VOL_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_VOL_DAOS_PACKAGE}.pc @ONLY
)

install(
  FILES
    ${HDF5_VOL_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_VOL_DAOS_PACKAGE}.pc
  DESTINATION
    ${HDF5_VOL_DAOS_INSTALL_LIB_DIR}/pkgconfig
)

#-----------------------------------------------------------------------------
# CPack
#-----------------------------------------------------------------------------
if(NOT HDF5_VOL_DAOS_EXTERNALLY_CONFIGURED)
  set(CPACK_GENERATOR "TBZ2")
  set(CPACK_PACKAGE_DESCRIPTION_FILE ${HDF5_VOL_DAOS_SOURCE_DIR}/README.md)
  set(CPACK_RESOURCE_FILE_LICENSE ${HDF5_VOL_DAOS_SOURCE_DIR}/COPYING)
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY ${HDF5_VOL_DAOS_PACKAGE_DESCRIPTION})
  set(CPACK_PACKAGE_NAME ${HDF5_VOL_DAOS_PACKAGE_NAME})
  set(CPACK_PACKAGE_VENDOR ${HDF5_VOL_DAOS_PACKAGE_VENDOR})
  set(CPACK_PACKAGE_VERSION_MAJOR ${HDF5_VOL_DAOS_VERSION_MAJOR})
  set(CPACK_PACKAGE_VERSION_MINOR ${HDF5_VOL_DAOS_VERSION_MINOR})
  set(CPACK_PACKAGE_VERSION_PATCH ${HDF5_VOL_DAOS_VERSION_PATCH})
  set(CPACK_SOURCE_GENERATOR "TBZ2")
  set(CPACK_SOURCE_PACKAGE_FILE_NAME ${HDF5_VOL_DAOS_PACKAGE}-${HDF5_VOL_DAOS_VERSION_FULL})
  set(CPACK_SOURCE_IGNORE_FILES
    # Files specific to version control
    "/\\\\.git/"
    "/\\\\.git$"
    "/\\\\.gitattributes$"
    "/\\\\.github/"
    "/\\\\.gitignore$"
    "/\\\\.gitmodules$"

    # IDE files
    "/\\\\.vscode/"

    # Build
    "/build/"

    # Temporary files
    "\\\\.swp$"
    "\\\\.#"
    "/#"
    "~$"
  )
  set(CPACK_SOURCE_STRIP_FILES "")
  include(CPack)
endif()
