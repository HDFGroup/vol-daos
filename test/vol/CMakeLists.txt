cmake_minimum_required(VERSION 2.8.12.2 FATAL_ERROR)
project(HDF5_VOL_TEST C)

#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# HDF5
find_package(HDF5 REQUIRED)
if(HDF5_FOUND)
  if(NOT TARGET hdf5::hdf5-shared)
      message(FATAL_ERROR "Could not find hdf5 shared target, please make "
      "sure that HDF5 has ben compiled with shared libraries enabled.")
  endif()
  set(HDF5_VOL_TEST_EXT_PKG_DEPENDENCIES
    ${HDF5_VOL_TEST_EXT_PKG_DEPENDENCIES}
    hdf5::hdf5-shared
  )
else()
  message(FATAL_ERROR "Could not find HDF5, please check HDF5_DIR.")
endif()

# MPI
# Temporary workaround because HDF5 does not pull MPI dependency
find_package(MPI REQUIRED)
if(MPI_FOUND)
  set(HDF5_VOL_TEST_EXT_INCLUDE_DEPENDENCIES
    ${HDF5_VOL_TEST_EXT_INCLUDE_DEPENDENCIES}
    ${MPI_INCLUDE_PATH}
  )
  set(HDF5_VOL_TEST_EXT_LIB_DEPENDENCIES
    ${HDF5_VOL_TEST_EXT_LIB_DEPENDENCIES}
    ${MPI_LIBRARIES}
  )
else()
  message(FATAL_ERROR "Could not find MPI.")
endif()

#-----------------------------------------------------------------------------
# Define Sources
#-----------------------------------------------------------------------------
set(vol_tests
#  attribute
#  dataset
#  datatype
#  file
  group
#  link
#  misc
#  object
)

foreach(vol_test ${vol_tests})
  add_executable (h5vl_test_${vol_test}
    ${CMAKE_CURRENT_SOURCE_DIR}/vol_${vol_test}_test.c vol_test.c
  )
#  target_include_directories(h5vl_test_${vol_test}
#    PUBLIC  "$<BUILD_INTERFACE:${HDF5_VOL_TEST_BUILD_INCLUDE_DEPENDENCIES}>"
#  )
  target_include_directories(h5vl_test_${vol_test}
    SYSTEM PUBLIC ${HDF5_VOL_TEST_EXT_INCLUDE_DEPENDENCIES}
  )
  target_link_libraries(h5vl_test_${vol_test}
    ${HDF5_VOL_TEST_EXPORTED_LIBS}
    ${HDF5_VOL_TEST_EXT_LIB_DEPENDENCIES}
    ${HDF5_VOL_TEST_EXT_PKG_DEPENDENCIES}
  )
endforeach()
