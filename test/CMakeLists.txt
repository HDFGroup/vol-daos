cmake_minimum_required (VERSION 3.1.0)
PROJECT (DAOS_VOL_TEST C)

#-----------------------------------------------------------------------------
# Apply definitions to compiler in this directory
#-----------------------------------------------------------------------------
add_definitions(${DAOS_VOL_EXTRA_C_FLAGS})

set (DAOS_VOL_TEST_TARGET "test_daos_vol")

#-----------------------------------------------------------------------------
# Define test sources
#-----------------------------------------------------------------------------
set (TEST_LIB_SRCS
    ${DAOS_VOL_TEST_SOURCE_DIR}/test_daos_vol.c
)

MACRO (ADD_DAOS_VOL_EXE file)
  add_executable (${file} ${DAOS_VOL_TEST_SOURCE_DIR}/${file}.c)
  TARGET_C_PROPERTIES (${file} STATIC " " " ")
  target_link_libraries (${file} ${DAOS_VOL_LIB_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
  set_target_properties (${file} PROPERTIES FOLDER test)
  if (NOT PREBUILT_HDF5_DIR)
    add_dependencies(${file} ${DAOS_VOL_LIB_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
  endif ()
  if (BUILD_SHARED_LIBS)
    add_executable (${file}-shared ${DAOS_VOL_TEST_SOURCE_DIR}/${file}.c)
    TARGET_C_PROPERTIES (${file}-shared SHARED " " " ")
    target_link_libraries (${file}-shared ${DAOS_VOL_LIBSH_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
    set_target_properties (${file}-shared PROPERTIES FOLDER test)
    if (NOT PREBUILT_HDF5_DIR)
      add_dependencies(${file}-shared ${DAOS_VOL_LIBSH_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
    endif ()
  endif (BUILD_SHARED_LIBS)
ENDMACRO (ADD_DAOS_VOL_EXE file)

set (DAOS_VOL_TESTS
    test_daos_vol
)

foreach (test ${DAOS_VOL_TESTS})
    ADD_DAOS_VOL_EXE(${test})
endforeach (test ${DAOS_VOL_TESTS})

foreach (test ${DAOS_VOL_TESTS})
  if (${test} STREQUAL "big" AND CYGWIN)
    add_test (
        NAME DVTEST-${test}
        COMMAND ${CMAKE_COMMAND} -E echo "SKIP ${test}"
    )
  else (${test} STREQUAL "big" AND CYGWIN)
    add_test (NAME DVTEST-${test} COMMAND $<TARGET_FILE:${test}>)
  endif (${test} STREQUAL "big" AND CYGWIN)
  set_tests_properties (DVTEST-${test} PROPERTIES
      ENVIRONMENT "srcdir=${DAOS_VOL_TEST_BINARY_DIR}"
      WORKING_DIRECTORY ${DAOS_VOL_TEST_BINARY_DIR}
  )
endforeach (test ${DAOS_VOL_TESTS})

if (BUILD_SHARED_LIBS)
  foreach (test ${DAOS_VOL_TESTS})
    if (${test} STREQUAL "big" AND CYGWIN)
      add_test (
          NAME DVTEST-shared-${test}
          COMMAND ${CMAKE_COMMAND} -E echo "SKIP ${test}-shared"
      )
    else (${test} STREQUAL "big" AND CYGWIN)
      add_test (NAME DVTEST-shared-${test} COMMAND $<TARGET_FILE:${test}-shared>)
    endif (${test} STREQUAL "big" AND CYGWIN)
    set_tests_properties (DVTEST-shared-${test} PROPERTIES
        ENVIRONMENT "srcdir=${DAOS_VOL_TEST_BINARY_DIR}"
        WORKING_DIRECTORY ${DAOS_VOL_TEST_BINARY_DIR}
    )
  endforeach (test ${DAOS_VOL_TESTS})
endif (BUILD_SHARED_LIBS)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
if (DAOS_VOL_EXPORTED_TARGETS)
  install (
      TARGETS
          ${DAOS_VOL_TEST_TARGET}
      EXPORT
          ${DAOS_VOL_EXPORTED_TARGETS}
      DESTINATION
          ${DAOS_VOL_INSTALL_BIN_DIR} COMPONENT testing
  )
endif ()