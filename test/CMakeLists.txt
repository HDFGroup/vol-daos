#------------------------------------------------------------------------------
# Orterun config
#------------------------------------------------------------------------------
find_program(ORTERUN_EXECUTABLE
  NAMES orterun
  PATH_SUFFIXES bin sbin
  HINTS ${MPI_HINT_DIRS}
  DOC "Executable for running MPI programs."
)
mark_as_advanced(ORTERUN_EXECUTABLE)

#------------------------------------------------------------------------------
# DAOS server config
#------------------------------------------------------------------------------
# Exec
find_program(DAOS_SERVER_EXECUTABLE
  NAMES daos_server
  PATH_SUFFIXES bin sbin
  HINTS ${DAOS_HINT_DIRS}
  DOC "DAOS server executable."
)
mark_as_advanced(DAOS_SERVER_EXECUTABLE)

# Preflags
set(DAOS_SERVER_URI_FILEPATH ${CMAKE_CURRENT_BINARY_DIR}/daos_uri.txt)
set(DAOS_SERVER_PREFLAGS
  "--enable-recovery --report-uri ${DAOS_SERVER_URI_FILEPATH}"
)

# Postflags
set(DAOS_SERVER_NUMSTREAMS "1" CACHE STRING
  "Number of ABT streams used by DAOS server for testing."
)
mark_as_advanced(DAOS_SERVER_NUMSTREAMS)

set(DAOS_SERVER_CONFIG_FILEPATH
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_server.yml
)
set(DAOS_SERVER_POSTFLAGS
  "-c ${DAOS_SERVER_NUMSTREAMS} -d ${CMAKE_CURRENT_BINARY_DIR} -o ${DAOS_SERVER_CONFIG_FILEPATH}"
)

# Transport
set(DAOS_SERVER_TRANSPORT "ofi+tcp" CACHE STRING
  "Transport used by DAOS server for testing."
)
mark_as_advanced(DAOS_SERVER_TRANSPORT)

# Interface
set(DAOS_SERVER_IFACE "lo" CACHE STRING
  "Interface used by DAOS server for testing."
)
mark_as_advanced(DAOS_SERVER_IFACE)

# Init config
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daos_server.yml.in
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_server.yml
)

#------------------------------------------------------------------------------
# DAOS client config
#------------------------------------------------------------------------------

set(HDF5_VOL_TEST_PLUGIN_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(HDF5_VOL_TEST_PLUGIN_NAME "daos")

set(DAOS_CLIENT_ENV_VARS
  "D_LOG_FILE=${CMAKE_CURRENT_BINARY_DIR}/daos_client.log"
  "CRT_PHY_ADDR_STR=${DAOS_SERVER_TRANSPORT}"
  "OFI_INTERFACE=${DAOS_SERVER_IFACE}"
  "HDF5_PLUGIN_PATH=${HDF5_VOL_TEST_PLUGIN_PATH}"
  "HDF5_VOL_CONNECTOR=${HDF5_VOL_TEST_PLUGIN_NAME}"
)

# Preflags
set(DAOS_CLIENT_PREFLAGS
  "--ompi-server file:${DAOS_SERVER_URI_FILEPATH}"
)

#------------------------------------------------------------------------------
# Test config
#------------------------------------------------------------------------------

# For compatibility reasons, we also define MPIEXEC
set(MPIEXEC_EXECUTABLE "${ORTERUN_EXECUTABLE}" CACHE FILEPATH "Executable for running MPI programs." FORCE)
set(MPIEXEC_SERVER_PREFLAGS "${DAOS_SERVER_PREFLAGS}")
set(MPIEXEC_SERVER_POSTFLAGS "${DAOS_SERVER_POSTFLAGS}")
set(MPIEXEC_PREFLAGS "${DAOS_CLIENT_PREFLAGS}")
set(MPIEXEC_MAX_NUMPROCS "1" CACHE STRING
  "Maximum number of processors available to run MPI applications."
  FORCE
)

set(HDF5_VOL_TEST_SERVER ${DAOS_SERVER_EXECUTABLE})
# Do not forget to include env command
set(H5VL_TEST_ENV_VARS "env ${DAOS_CLIENT_ENV_VARS}")

# Test will start running after following line is printed
set(H5VL_TEST_SERVER_START_MSG "DAOS I/O server")
set(H5VL_TEST_INIT_COMMAND "killall -9 daos_io_server")

#------------------------------------------------------------------------------
# DAOS VOL specific tests
#------------------------------------------------------------------------------
add_subdirectory(daos_vol)

#------------------------------------------------------------------------------
# VOL tests
#------------------------------------------------------------------------------
set(HDF5_VOL_TEST_EXTERNALLY_CONFIGURED TRUE)
set(HDF5_VOL_TEST_ENABLE_PARALLEL "ON" CACHE BOOL "Enable testing in parallel (requires MPI).")
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/vol/CMakeLists.txt)
  message(STATUS "VOL test submodule not found, checking it out")
  # get newest submodule info
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" submodule update --init --recursive
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_SUBMODULE_UPDATE_OUTPUT
  )
  message(STATUS ${GIT_SUBMODULE_UPDATE_OUTPUT})
endif()
add_subdirectory(vol)
