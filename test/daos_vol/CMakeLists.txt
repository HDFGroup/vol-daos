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

#-----------------------------------------------------------------------------
# Define Sources and tests
#-----------------------------------------------------------------------------
set(daos_vol_tests
  map
  example
)
# will expect h5daos_test_map.c etc

foreach(vol_test ${daos_vol_tests})
  add_executable(h5daos_test_${vol_test}
    ${CMAKE_CURRENT_SOURCE_DIR}/h5daos_test_${vol_test}.c
  )
  target_link_libraries(h5daos_test_${vol_test}
    hdf5_vol_daos
  )
endforeach()

