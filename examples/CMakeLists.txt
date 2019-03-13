cmake_minimum_required(VERSION 2.8.12.2 FATAL_ERROR)
project(HDF5_DAOS_EXAMPLES C)

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
# HDF5 DAOS
find_package(HDF5_DAOS REQUIRED)

#-----------------------------------------------------------------------------
# Define Sources
#-----------------------------------------------------------------------------
set(examples
  attr_create
  attr_iter
  attr_open
  attr_read
  attr_write
  dset_create
  dset_open
  dset_r1m
  dset_read
  dset_rpartial
  dset_w1m
  dset_wpartial
  dset_write
  file_create
  file_open
  group_create
  group_open
  link_exists
  link_iter
#  map
  obj_info
  obj_open_addr
  obj_open
  slink_create
  tcommit
  ttconv
  tvlen
)

foreach (example ${examples})
  add_executable (h5daos_${example}
    ${CMAKE_CURRENT_SOURCE_DIR}/h5dsm_${example}.c
  )
  target_link_libraries(h5daos_${example} hdf5_daos)
endforeach()
