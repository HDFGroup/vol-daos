#------------------------------------------------------------------------------
# Options
#------------------------------------------------------------------------------
option(HDF5_VOL_DAOS_TESTING_USE_SYSTEM_SERVER "Use system running server for testing." OFF)
mark_as_advanced(HDF5_VOL_DAOS_TESTING_USE_SYSTEM_SERVER)

set(HDF5_VOL_TEST_ENABLE_PARALLEL "ON" CACHE BOOL "Enable testing in parallel (requires MPI).")

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

# Default paths
set(DAOS_LOG_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(DAOS_SERVER_SOCKET_DIR ${CMAKE_CURRENT_BINARY_DIR})

# Preflags
set(DAOS_SERVER_PREFLAGS
  "-genv DAOS_DISABLE_REQ_FWD 1"
)

set(DAOS_SERVER_CONFIG_FILEPATH
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_server.yml
)

# Postflags
# Storage is formatted explicitly via "dmg storage format" (see daos_pool.sh.in).
set(DAOS_SERVER_POSTFLAGS
  "start -o ${DAOS_SERVER_CONFIG_FILEPATH}"
)

# Transport
set(DAOS_SERVER_TRANSPORT "ofi+tcp" CACHE STRING
  "Transport used by DAOS server for testing (sm, ofi+tcp)."
)
mark_as_advanced(DAOS_SERVER_TRANSPORT)

# Interface
set(DAOS_SERVER_IFACE "lo" CACHE STRING
  "Interface used by DAOS server for testing."
)
mark_as_advanced(DAOS_SERVER_IFACE)

# SCM size
set(DAOS_SERVER_SCM_SIZE "8" CACHE STRING
  "Size of RAM available for storage in GB."
)
mark_as_advanced(DAOS_SERVER_SCM_SIZE)

# Engine targets
set(DAOS_SERVER_TARGETS "4" CACHE STRING
  "Number of storage targets for the DAOS server engine to use. Must not exceed the number of cores available on the host."
)
mark_as_advanced(DAOS_SERVER_TARGETS)

# SCM mnt
set(DAOS_SERVER_SCM_MNT "/mnt/daos" CACHE STRING
  "Path to SCM mount point."
)
mark_as_advanced(DAOS_SERVER_SCM_MNT)

# Init config
configure_file(
   ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daos_server.yml.in
   ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_server.yml
)

#------------------------------------------------------------------------------
# DAOS agent config
#------------------------------------------------------------------------------
# Exec
find_program(DAOS_AGENT_EXECUTABLE
  NAMES daos_agent
  PATH_SUFFIXES bin sbin
  HINTS ${DAOS_HINT_DIRS}
  DOC "DAOS agent executable."
)
mark_as_advanced(DAOS_AGENT_EXECUTABLE)

# Default paths
set(DAOS_AGENT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(DAOS_AGENT_SOCKET_DIR ${CMAKE_CURRENT_BINARY_DIR})

# Agent config
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daos_agent.yml.in
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_agent.yml
)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daos_agent.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_agent.sh
)
execute_process(COMMAND chmod u+x ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_agent.sh)

#------------------------------------------------------------------------------
# DAOS dmg config
#------------------------------------------------------------------------------
# Exec
find_program(DAOS_DMG_EXECUTABLE
  NAMES dmg
  PATH_SUFFIXES bin sbin
  HINTS ${DAOS_HINT_DIRS}
  DOC "DAOS dmg executable."
)
mark_as_advanced(DAOS_DMG_EXECUTABLE)

# SCM size
set(DAOS_POOL_SIZE "4" CACHE STRING
  "Size of pool created for testing in GB."
)
mark_as_advanced(DAOS_POOL_SIZE)

# Dmg config
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daos_pool.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_pool.sh
)
execute_process(COMMAND chmod u+x ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_pool.sh)

#------------------------------------------------------------------------------
# DAOS client config
#
# Consumed both by test/daos_vol's own native tests and, when this project is
# built inline as an HDF5_VOL_ALLOW_EXTERNAL subproject of HDF5's own build,
# by the environment HDF5's test/API suite sets for this connector's ctest
# entries (see HDF5_VOL_<name>_NAME / HDF5_EXTERNAL_VOL_TARGETS in HDF5's
# CMakeVOL.cmake).
#------------------------------------------------------------------------------

set(HDF5_VOL_TEST_PLUGIN_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(HDF5_VOL_TEST_PLUGIN_NAME "daos")

set(DAOS_CLIENT_ENV_VARS
  "D_LOG_FILE=${CMAKE_CURRENT_BINARY_DIR}/daos_client.log"
  "D_LOG_MASK=ERR"
  "CRT_PHY_ADDR_STR=${DAOS_SERVER_TRANSPORT}"
  "OFI_INTERFACE=${DAOS_SERVER_IFACE}"
  "DAOS_AGENT_DRPC_DIR=${DAOS_AGENT_DIR}"
  "HDF5_PLUGIN_PATH=${HDF5_VOL_TEST_PLUGIN_PATH}"
  "HDF5_VOL_CONNECTOR=${HDF5_VOL_TEST_PLUGIN_NAME}"
  # Disable automatic chunk sizing for the API tests
  "HDF5_DAOS_CHUNK_TARGET_SIZE=1073741824"
)

#------------------------------------------------------------------------------
# DAOS VOL specific tests
#------------------------------------------------------------------------------
add_subdirectory(daos_vol)

# Relay HDF5_API_EXT_SERIAL_TESTS / HDF5_API_EXT_PARALLEL_TESTS (set by
# test/daos_vol/CMakeLists.txt) further up so they reach HDF5's own
# top-level scope when this connector is built inline as an
# HDF5_VOL_ALLOW_EXTERNAL subproject.
set(HDF5_API_EXT_SERIAL_TESTS ${HDF5_API_EXT_SERIAL_TESTS} PARENT_SCOPE)
set(HDF5_API_EXT_PARALLEL_TESTS ${HDF5_API_EXT_PARALLEL_TESTS} PARENT_SCOPE)
