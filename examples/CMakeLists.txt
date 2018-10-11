cmake_minimum_required (VERSION 3.1.0)
PROJECT (DAOS_VOL_EXAMPLES)

#-----------------------------------------------------------------------------
# Apply Definitions to compiler in this directory and below
#-----------------------------------------------------------------------------
add_definitions (${DAOS_VOL_EXTRA_C_FLAGS})

#-----------------------------------------------------------------------------
# Define Sources
#-----------------------------------------------------------------------------
set (examples
    h5dsm_dset_rpartial
    h5dsm_obj_info
    h5dsm_dset_w1m
    h5dsm_obj_open
    h5dsm_dset_wpartial
    h5dsm_obj_open_addr
    h5dsm_attr_create
    h5dsm_dset_write
    h5dsm_slink_create
    h5dsm_attr_iter
    h5dsm_tcommit
    h5dsm_attr_open
    h5dsm_file_create
    h5dsm_attr_read
    h5dsm_file_open
    h5dsm_attr_write
    h5dsm_group_create
    h5dsm_ttconv
    h5dsm_dset_create
    h5dsm_group_open
    h5dsm_tvlen
    h5dsm_dset_open
    h5dsm_link_exists
    h5dsm_dset_r1m
    h5dsm_link_iter
    h5dsm_dset_read
    h5dsm_map
)

foreach (example ${examples})
  add_executable (${example} ${DAOS_VOL_EXAMPLES_SOURCE_DIR}/${example}.c)
  TARGET_C_PROPERTIES (${example} STATIC " " " ")
  target_link_libraries (${example} ${DAOS_VOL_LIB_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
  set_target_properties (${example} PROPERTIES FOLDER examples)
  if (NOT PREBUILT_HDF5_DIR)
    add_dependencies(${example} ${DAOS_VOL_LIB_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
  endif ()
  if (BUILD_SHARED_LIBS)
    add_executable (${example}-shared ${DAOS_VOL_EXAMPLES_SOURCE_DIR}/${example}.c)
    TARGET_C_PROPERTIES (${example}-shared SHARED " " " ")
    target_link_libraries (${example}-shared ${DAOS_VOL_LIBSH_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
    set_target_properties (${example}-shared PROPERTIES FOLDER examples)
    if (NOT PREBUILT_HDF5_DIR)
      add_dependencies(${example}-shared ${DAOS_VOL_LIBSH_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})
    endif ()
  endif (BUILD_SHARED_LIBS)

  #-----------------------------------------------------------------------------
  # Add Target(s) to CMake Install for import into other projects
  #-----------------------------------------------------------------------------
  if (DAOS_VOL_EXPORTED_TARGETS)
    install (
        TARGETS
            ${example}
        EXPORT
            ${DAOS_VOL_EXPORTED_TARGETS}
        DESTINATION
            ${DAOS_VOL_INSTALL_DATA_DIR}/daos_vol_examples COMPONENT examples
    )
  endif ()
endforeach (example ${examples})

add_subdirectory(hl ${PROJECT_BINARY_DIR}/examples)

if (BUILD_TESTING)
  include (${DAOS_VOL_EXAMPLES_SOURCE_DIR}/CMakeTests.cmake)
endif (BUILD_TESTING)
