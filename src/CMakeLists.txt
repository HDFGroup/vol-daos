cmake_minimum_required (VERSION 3.1.0)
PROJECT (DAOS_VOL_SRC C)

macro (IDE_GENERATED_PROPERTIES SOURCE_PATH HEADERS SOURCES)
  #set(source_group_path "Source/AIM/${NAME}")
  string (REPLACE "/" "\\\\" source_group_path ${SOURCE_PATH})
  source_group (${source_group_path} FILES ${HEADERS} ${SOURCES})

  #-- The following is needed if we ever start to use OS X Frameworks but only
  #--  works on CMake 2.6 and greater
  #set_property (SOURCE ${HEADERS}
  #       PROPERTY MACOSX_PACKAGE_LOCATION Headers/${NAME}
  #)
endmacro (IDE_GENERATED_PROPERTIES)

#-----------------------------------------------------------------------------
# Apply definitions to compiler in this directory
#-----------------------------------------------------------------------------
add_definitions(${DAOS_VOL_EXTRA_C_FLAGS})

#-----------------------------------------------------------------------------
# List source files
#-----------------------------------------------------------------------------
set (DAOS_VOL_SRCS
    ${DAOS_VOL_SRC_DIR}/daos_vol.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_attr.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_dset.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_file.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_group.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_link.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_obj.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_req.c
    ${DAOS_VOL_SRC_DIR}/daos_vol_type.c
)

set (DAOS_VOL_HDRS
    ${DAOS_VOL_SRC_DIR}/daos_vol.h
    ${DAOS_VOL_SRC_DIR}/daos_vol_public.h
)

#-----------------------------------------------------------------------------
# Add in source files from the "util" diectory
#-----------------------------------------------------------------------------
add_subdirectory(util ${PROJECT_BINARY_DIR}/src/util)

IDE_GENERATED_PROPERTIES("DAOS_VOL" "${DAOS_VOL_HDRS}" "${DAOS_VOL_SRCS}")

set (DAOS_VOL_PUBLIC_HEADERS
    ${DAOS_VOL_SRC_DIR}/daos_vol_public.h
)

add_library (${DAOS_VOL_LIB_TARGET} STATIC ${DAOS_VOL_SRCS} ${DAOS_VOL_HDRS})
TARGET_C_PROPERTIES (${DAOS_VOL_LIB_TARGET} STATIC " " " ")
target_link_libraries (${DAOS_VOL_LIB_TARGET} ${HDF5_LIBRARIES_TO_EXPORT} ${LINK_LIBS})
if (NOT WIN32)
  target_link_libraries (${DAOS_VOL_LIB_TARGET} dl)
endif ()
set (DAOS_VOL_LIBRARIES_TO_EXPORT ${DAOS_VOL_LIB_TARGET} CACHE INTERNAL "Used to pass variables between directories" FORCE)
set_target_properties (${DAOS_VOL_LIB_TARGET} PROPERTIES
    FOLDER libraries
    INTERFACE_INCLUDE_DIRECTORIES "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>"
)
if (WIN32)
  set_target_properties (${DAOS_VOL_LIB_TARGET} PROPERTIES
    OUTPUT_NAME lib${DAOS_VOL_LIB_CORENAME}
  )
else ()
  set_target_properties (${DAOS_VOL_LIB_TARGET} PROPERTIES
    OUTPUT_NAME ${DAOS_VOL_LIB_CORENAME}
  )
endif()

#add_dependencies(${DAOS_VOL_LIB_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})

set (install_targets ${DAOS_VOL_LIB_TARGET})

if (BUILD_SHARED_LIBS)
  file (MAKE_DIRECTORY "${DAOS_VOL_BINARY_DIR}/shared")
  add_library (${DAOS_VOL_LIBSH_TARGET} SHARED ${DAOS_VOL_SRCS} ${DAOS_VOL_HDRS})
  TARGET_C_PROPERTIES (${DAOS_VOL_LIBSH_TARGET} SHARED " " " ")
  target_link_libraries (${DAOS_VOL_LIBSH_TARGET} ${HDF5_LIBRARIES_TO_EXPORT} ${LINK_SHARED_LIBS})
  if (NOT WIN32)
    target_link_libraries (${DAOS_VOL_LIBSH_TARGET} dl)
  endif ()
  set (DAOS_VOL_LIBRARIES_TO_EXPORT "${DAOS_VOL_LIBRARIES_TO_EXPORT};${DAOS_VOL_LIBSH_TARGET}" CACHE INTERNAL "Used to pass variables between directories" FORCE)
  set_target_properties (${DAOS_VOL_LIBSH_TARGET} PROPERTIES
      OUTPUT_NAME ${DAOS_VOL_LIB_CORENAME}
      FOLDER libraries
      COMPILE_DEFINITIONS "DAOS_VOL_BUILT_AS_DYNAMIC_LIB"
      INTERFACE_INCLUDE_DIRECTORIES "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>"
      INTERFACE_COMPILE_DEFINITIONS DAOS_VOL_BUILT_AS_DYNAMIC_LIB=1
  )
  
  #add_dependencies(${DAOS_VOL_LIBSH_TARGET} ${HDF5_LIBRARIES_TO_EXPORT})

  set (install_targets ${install_targets} ${DAOS_VOL_LIBSH_TARGET})
endif ()

#-----------------------------------------------------------------------------
# Make the DAOS VOL depend on H5pubconf.h existing
#-----------------------------------------------------------------------------
if (PREBUILT_HDF5_DIR)
  add_custom_target(
    daos_vol_pubconf_depend
    DEPENDS ${PREBUILT_HDF5_DIR}/include/H5pubconf.h
  )
else ()
  add_custom_target(
    daos_vol_pubconf_depend
    DEPENDS ${CMAKE_BINARY_DIR}/${HDF5_DIR_NAME}/H5pubconf.h
  )
endif ()

add_dependencies(${DAOS_VOL_LIB_TARGET} daos_vol_pubconf_depend)

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
if (NOT DAOS_VOL_INSTALL_NO_DEVELOPMENT)
  install (
      FILES
          ${DAOS_VOL_PUBLIC_HEADERS}
      DESTINATION
          ${DAOS_VOL_INSTALL_INCLUDE_DIR}
      COMPONENT
          headers
  )
endif ()

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
if (DAOS_VOL_EXPORTED_TARGETS)
  if (BUILD_SHARED_LIBS)
    INSTALL_TARGET_PDB (${DAOS_VOL_LIBSH_TARGET} ${DAOS_VOL_INSTALL_BIN_DIR} libraries)
  endif ()

  install (
      TARGETS
          ${install_targets}
      EXPORT
          ${DAOS_VOL_EXPORTED_TARGETS}
      LIBRARY DESTINATION ${DAOS_VOL_INSTALL_LIB_DIR} COMPONENT libraries
      ARCHIVE DESTINATION ${DAOS_VOL_INSTALL_LIB_DIR} COMPONENT libraries
      RUNTIME DESTINATION ${DAOS_VOL_INSTALL_BIN_DIR} COMPONENT libraries
      FRAMEWORK DESTINATION ${DAOS_VOL_INSTALL_FWRK_DIR} COMPONENT libraries
  )
endif ()

#-----------------------------------------------------------------------------
# Create pkgconfig files
#-----------------------------------------------------------------------------
set (_PKG_CONFIG_PREFIX ${CMAKE_INSTALL_PREFIX})
set (_PKG_CONFIG_EXEC_PREFIX \${prefix})
set (_PKG_CONFIG_LIBDIR \${exec_prefix}/lib)
set (_PKG_CONFIG_INCLUDEDIR \${prefix}/include)
set (_PKG_CONFIG_LIBNAME "${DAOS_VOL_LIB_CORENAME}")
set (_PKG_CONFIG_VERSION "${DAOS_VOL_PACKAGE_VERSION}")

foreach (libs ${LINK_LIBS} ${LINK_COMP_LIBS})
  set (_PKG_CONFIG_LIBS_PRIVATE "${_PKG_CONFIG_LIBS_PRIVATE} -l${libs}")
endforeach ()

set (_PKG_CONFIG_LIBS "${_PKG_CONFIG_LIBS} -l${DAOS_VOL_LIB_CORENAME}")
if (BUILD_SHARED_LIBS)
  set (_PKG_CONFIG_SH_LIBS "${_PKG_CONFIG_SH_LIBS} -l${DAOS_VOL_LIB_CORENAME}")
endif ()

set (_PKG_CONFIG_REQUIRES "hdf5")
set (_PKG_CONFIG_REQUIRES_PRIVATE "hdf5")

configure_file (
    ${DAOS_VOL_RESOURCES_DIR}/libdaosvol.pc.in
    ${DAOS_VOL_BINARY_DIR}/CMakeFiles/${DAOS_VOL_LIB_CORENAME}-${DAOS_VOL_PACKAGE_VERSION}.pc
    @ONLY
)
install (
    FILES ${DAOS_VOL_BINARY_DIR}/CMakeFiles/${DAOS_VOL_LIB_CORENAME}-${DAOS_VOL_PACKAGE_VERSION}.pc
    DESTINATION ${DAOS_VOL_INSTALL_LIB_DIR}/pkgconfig
    COMPONENT libraries
)

if (NOT WIN32)
  set (_PKG_CONFIG_COMPILER ${CMAKE_C_COMPILER})
  configure_file (
      ${DAOS_VOL_RESOURCES_DIR}/dvcc.in
      ${DAOS_VOL_BINARY_DIR}/CMakeFiles/dvcc
      @ONLY
  )
  install (
      FILES ${DAOS_VOL_BINARY_DIR}/CMakeFiles/dvcc
      DESTINATION ${DAOS_VOL_INSTALL_BIN_DIR}
      PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
      COMPONENT libraries
  )
endif ()
