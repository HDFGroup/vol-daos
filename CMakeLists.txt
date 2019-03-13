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
  )
  if(POLICY ${policy})
    cmake_policy(SET ${policy} NEW)
  endif()
endforeach()

project(HDF5_DAOS C)

#------------------------------------------------------------------------------
# Version information 
#------------------------------------------------------------------------------
set(HDF5_DAOS_VERSION_MAJOR "0")
set(HDF5_DAOS_VERSION_MINOR "1")
set(HDF5_DAOS_VERSION_PATCH "0")
set(HDF5_DAOS_PACKAGE "hdf5_daos")
set(HDF5_DAOS_PACKAGE_NAME "HDF5_DAOS")
set(HDF5_DAOS_PACKAGE_VERSION "${HDF5_DAOS_VERSION_MAJOR}.${HDF5_DAOS_VERSION_MINOR}.${HDF5_DAOS_VERSION_PATCH}")
set(HDF5_DAOS_PACKAGE_VERSION_MAJOR "${HDF5_DAOS_VERSION_MAJOR}.${HDF5_DAOS_VERSION_MINOR}")
set(HDF5_DAOS_PACKAGE_VERSION_MINOR "${HDF5_DAOS_VERSION_PATCH}")
set(HDF5_DAOS_PACKAGE_STRING "${HDF5_DAOS_PACKAGE_NAME} ${HDF5_DAOS_PACKAGE_VERSION}")
set(HDF5_DAOS_PACKAGE_TARNAME "${HDF5_DAOS_PACKAGE}")

#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if(NOT HDF5_DAOS_INSTALL_BIN_DIR)
  set(HDF5_DAOS_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT HDF5_DAOS_INSTALL_LIB_DIR)
  set(HDF5_DAOS_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT HDF5_DAOS_INSTALL_INCLUDE_DIR)
  # Interface include will default to prefix/include
  set(HDF5_DAOS_INSTALL_INTERFACE include)
  set(HDF5_DAOS_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
else()
  set(HDF5_DAOS_INSTALL_INTERFACE ${HDF5_DAOS_INSTALL_INCLUDE_DIR})
endif()
if(NOT HDF5_DAOS_INSTALL_DATA_DIR)
  set(HDF5_DAOS_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()

# Setting this ensures that "make install" will leave rpaths to external
# libraries intact on "make install". This ensures that one can install a
# version of HDF5 DAOS on the build machine without any issues. If this not
# desired, simply specify CMAKE_INSTALL_RPATH_USE_LINK_PATH when configuring
# HDF5 DAOS and "make install" will strip all rpaths, which is default behavior.
if(NOT CMAKE_INSTALL_RPATH_USE_LINK_PATH)
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

#------------------------------------------------------------------------------
# Setup CMake Environment
#------------------------------------------------------------------------------
if(APPLE OR WIN32)
  message("The HDF5 DAOS VOL connector is not supported on this platform." FATAL_ERROR)
endif()

#------------------------------------------------------------------------------
if(NOT HDF5_DAOS_EXTERNALLY_CONFIGURED)
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
set(HDF5_DAOS_CMAKE_DIR "${HDF5_DAOS_SOURCE_DIR}/CMake")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${HDF5_DAOS_CMAKE_DIR})

#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if("${HDF5_DAOS_SOURCE_DIR}" STREQUAL "${HDF5_DAOS_BINARY_DIR}")
  message(FATAL_ERROR
    "HDF5 DAOS requires an out of source Build. "
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
    "MinSizeRel" "RelWithDebInfo")
endif()

if(NOT CMAKE_C_FLAGS AND CMAKE_COMPILER_IS_GNUCC)
  message(STATUS "GCC detected, setting additional flags")
  set(CMAKE_C_FLAGS "-Wall -Wextra -Winline -Wcast-qual -std=gnu99 -Wshadow" CACHE STRING "Flags used by the compiler during all build types." FORCE)
endif()

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT HDF5_DAOS_EXPORTED_TARGETS)
  set(HDF5_DAOS_EXPORTED_TARGETS "${HDF5_DAOS_PACKAGE}-targets")
endif()

#------------------------------------------------------------------------------
# Choose static or shared libraries.
#------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build with shared libraries." OFF)
if(BUILD_SHARED_LIBS)
  set(HG_BUILD_SHARED_LIBS 1)
  set(HDF5_DAOS_LIBTYPE SHARED)
  set (DAOS_VOL_ENABLE_SHARED_LIB NO)
else()
  set(HG_BUILD_SHARED_LIBS 0)
  set(HDF5_DAOS_LIBTYPE STATIC)
    set (DAOS_VOL_ENABLE_SHARED_LIB YES)
endif()
set (CMAKE_POSITION_INDEPENDENT_CODE ON)

#-------------------------------------------------------------------------------
function(hdf5_daos_set_lib_options libtarget libname libtype)
  if(${libtype} MATCHES "SHARED")
    set(LIB_RELEASE_NAME "${libname}")
    set(LIB_DEBUG_NAME "${libname}_debug")
    set(LIB_VERSION ${HDF5_DAOS_PACKAGE_VERSION})
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
      DEBUG_OUTPUT_NAME          ${LIB_DEBUG_NAME}
      RELEASE_OUTPUT_NAME        ${LIB_RELEASE_NAME}
      MINSIZEREL_OUTPUT_NAME     ${LIB_RELEASE_NAME}
      RELWITHDEBINFO_OUTPUT_NAME ${LIB_RELEASE_NAME}
      VERSION                    ${LIB_VERSION}
      SOVERSION                  ${LIB_VERSION}
  )
endfunction()

# Avoid explicitly including system include paths
set(HDF5_DAOS_SYSTEM_INCLUDE_PATH ${CMAKE_SYSTEM_INCLUDE_PATH}
  ${CMAKE_C_IMPLICIT_INCLUDE_DIRECTORIES})
if(NOT WIN32)
  set(HDF5_DAOS_SYSTEM_INCLUDE_PATH /usr/include ${HDF5_DAOS_SYSTEM_INCLUDE_PATH})
endif()

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(NOT HDF5_DAOS_EXTERNALLY_CONFIGURED)
  option(HDF5_DAOS_ENABLE_COVERAGE "Enable coverage." OFF)
  if(HDF5_DAOS_ENABLE_COVERAGE)
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
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)

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
#option(BUILD_TESTING "Build testing." OFF)
#if(NOT HDF5_DAOS_EXTERNALLY_CONFIGURED AND BUILD_TESTING)
#  enable_testing()
#  include(CTest)
#  add_subdirectory(Testing)
#endif()

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the build directory
#-----------------------------------------------------------------------------
configure_file(
  ${HDF5_DAOS_SOURCE_DIR}/CMake/${HDF5_DAOS_PACKAGE}-config.cmake.build.in
  ${HDF5_DAOS_BINARY_DIR}/${HDF5_DAOS_PACKAGE}-config.cmake @ONLY
)

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
  ${HDF5_DAOS_SOURCE_DIR}/CMake/${HDF5_DAOS_PACKAGE}-config.cmake.install.in
  ${HDF5_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_DAOS_PACKAGE}-config.cmake @ONLY
)

install(
  FILES
    ${HDF5_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_DAOS_PACKAGE}-config.cmake
  DESTINATION
    ${HDF5_DAOS_INSTALL_DATA_DIR}/cmake/${HDF5_DAOS_PACKAGE}
)

#-----------------------------------------------------------------------------
# Configure the config-version.cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
  ${HDF5_DAOS_SOURCE_DIR}/CMake/${HDF5_DAOS_PACKAGE}-config-version.cmake.in
  ${HDF5_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_DAOS_PACKAGE}-config-version.cmake @ONLY
)

install(
  FILES
    ${HDF5_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_DAOS_PACKAGE}-config-version.cmake
  DESTINATION
    ${HDF5_DAOS_INSTALL_DATA_DIR}/cmake/${HDF5_DAOS_PACKAGE}
)

#-----------------------------------------------------------------------------
# For automake compatibility, also provide a pkgconfig file
#-----------------------------------------------------------------------------
if(NOT WIN32)
  configure_file(
    ${HDF5_DAOS_SOURCE_DIR}/CMake/${HDF5_DAOS_PACKAGE}.pc.in
    ${HDF5_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_DAOS_PACKAGE}.pc @ONLY
  )

  install(
    FILES
      ${HDF5_DAOS_BINARY_DIR}/CMakeFiles/${HDF5_DAOS_PACKAGE}.pc
    DESTINATION
      ${HDF5_DAOS_INSTALL_LIB_DIR}/pkgconfig
  )
endif()

#-----------------------------------------------------------------------------
# CPack
#-----------------------------------------------------------------------------
if(NOT HDF5_DAOS_EXTERNALLY_CONFIGURED)
  set(CPACK_PACKAGE_DESCRIPTION_FILE ${HDF5_DAOS_SOURCE_DIR}/README.md)
  set(CPACK_RESOURCE_FILE_LICENSE ${HDF5_DAOS_SOURCE_DIR}/COPYING)
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "HDF5 DAOS VOL connector")
  set(CPACK_PACKAGE_NAME "HDF5 DAOS")
  set(CPACK_PACKAGE_VENDOR "The HDF Group")
  set(CPACK_PACKAGE_VERSION_MAJOR ${HDF5_DAOS_VERSION_MAJOR})
  set(CPACK_PACKAGE_VERSION_MINOR ${HDF5_DAOS_VERSION_MINOR})
  set(CPACK_PACKAGE_VERSION_PATCH ${HDF5_DAOS_VERSION_PATCH})
  set(CPACK_GENERATOR "TBZ2")
  set(CPACK_SOURCE_PACKAGE_FILE_NAME ${HDF5_DAOS_PACKAGE_TARNAME}-${HDF5_DAOS_PACKAGE_VERSION})
  set(CPACK_SOURCE_IGNORE_FILES ".git*;/GitSetup/;/.git/;.swp$;.#;/#;.*~")
  set(CPACK_SOURCE_STRIP_FILES "")
  include(CPack)
endif()