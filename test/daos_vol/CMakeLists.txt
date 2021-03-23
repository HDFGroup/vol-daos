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
  oclass
  recovery
#  example
)
if(HDF5_VOL_TEST_ENABLE_PARALLEL)
  set(daos_vol_parallel_tests
    map
    metadata
  )
endif()
# will expect h5daos_test_map.c etc

foreach(vol_test ${daos_vol_tests})
  add_executable(h5daos_test_${vol_test}
    ${CMAKE_CURRENT_SOURCE_DIR}/h5daos_test_${vol_test}.c
  )
  target_link_libraries(h5daos_test_${vol_test}
    hdf5_vol_daos
  )
  if(${vol_test} STREQUAL "recovery")
    target_link_libraries(h5daos_test_${vol_test}
      daos_common
    )
  endif()
  set(HDF5_VOL_EXT_SERIAL_TESTS_EXE
    ${HDF5_VOL_EXT_SERIAL_TESTS_EXE}
    h5daos_test_${vol_test}
  )
endforeach()
if(HDF5_VOL_TEST_ENABLE_PARALLEL)
  foreach(vol_parallel_test ${daos_vol_parallel_tests})
    add_executable(h5daos_test_${vol_parallel_test}_parallel
      ${CMAKE_CURRENT_SOURCE_DIR}/h5daos_test_${vol_parallel_test}_parallel.c
    )
    target_link_libraries(h5daos_test_${vol_parallel_test}_parallel
      hdf5_vol_daos
    )
    if(h5daos_test_${vol_parallel_test}_parallel STREQUAL "h5daos_test_metadata_parallel")
      target_link_libraries(h5daos_test_${vol_parallel_test}_parallel
        m
      )
    endif()
    set(HDF5_VOL_EXT_PARALLEL_TESTS_EXE
      ${HDF5_VOL_EXT_PARALLEL_TESTS_EXE}
      h5daos_test_${vol_parallel_test}_parallel
    )
  endforeach()
endif()

# Set list of executables to run with external VOL test suite
set(HDF5_VOL_EXT_SERIAL_TESTS ${HDF5_VOL_EXT_SERIAL_TESTS_EXE} PARENT_SCOPE)
set(HDF5_VOL_EXT_PARALLEL_TESTS ${HDF5_VOL_EXT_PARALLEL_TESTS_EXE} PARENT_SCOPE)
