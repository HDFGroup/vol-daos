#------------------------------------------------------------------------------
# Options
#------------------------------------------------------------------------------
option(HDF5_VOL_DAOS_TESTING_USE_SYSTEM_SERVER "Use system running server for testing." OFF)
mark_as_advanced(HDF5_VOL_DAOS_TESTING_USE_SYSTEM_SERVER)

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

# Postflags
set(DAOS_SERVER_CONFIG_FILEPATH
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_server.yml
)

set(DAOS_SERVER_POSTFLAGS
  "start"
  "--recreate-superblocks"
  "-o ${DAOS_SERVER_CONFIG_FILEPATH}"
)

# Transport
set(DAOS_SERVER_TRANSPORT "ofi+sockets" CACHE STRING
  "Transport used by DAOS server for testing (sm, ofi+sockets)."
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

# Dmg config
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daos_pool.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_pool.sh
)
execute_process(COMMAND chmod u+x ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_pool.sh)

#------------------------------------------------------------------------------
# DAOS client config
#------------------------------------------------------------------------------

# SCM size
set(DAOS_POOL_SIZE "4" CACHE STRING
  "Size of pool created for testing in GB."
)
mark_as_advanced(DAOS_POOL_SIZE)

set(HDF5_VOL_TEST_PLUGIN_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(HDF5_VOL_TEST_PLUGIN_NAME "daos")

# Preflags
set(DAOS_CLIENT_ENV_VARS
  "D_LOG_FILE=${CMAKE_CURRENT_BINARY_DIR}/daos_client.log"
  "D_LOG_MASK=ERR"
  "CRT_PHY_ADDR_STR=${DAOS_SERVER_TRANSPORT}"
  "OFI_INTERFACE=${DAOS_SERVER_IFACE}"
  "DAOS_AGENT_DRPC_DIR=${DAOS_AGENT_DIR}"
  "HDF5_PLUGIN_PATH=${HDF5_VOL_TEST_PLUGIN_PATH}"
  "HDF5_VOL_CONNECTOR=${HDF5_VOL_TEST_PLUGIN_NAME}"
)

#------------------------------------------------------------------------------
# Test config
#------------------------------------------------------------------------------

# Define MPIEXEC flags
set(MPIEXEC_SERVER_PREFLAGS "${DAOS_SERVER_PREFLAGS}")
set(MPIEXEC_SERVER_POSTFLAGS "${DAOS_SERVER_POSTFLAGS}")

if(NOT HDF5_VOL_DAOS_TESTING_USE_SYSTEM_SERVER)
  # Clear pool and files
  set(H5VL_TEST_INIT_COMMAND "rm -rf ${DAOS_SERVER_SCM_MNT}/* ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/*.h5")

  # Client helper
  set(HDF5_VOL_TEST_CLIENT_HELPER ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_agent.sh)
  set(H5VL_TEST_CLIENT_HELPER_EXIT_COMMAND "killall daos_agent")

  # Client setup
  set(HDF5_VOL_TEST_CLIENT_INIT ${CMAKE_CURRENT_BINARY_DIR}/scripts/daos_pool.sh)

  # Server
  set(HDF5_VOL_TEST_SERVER ${DAOS_SERVER_EXECUTABLE})
  set(H5VL_TEST_SERVER_EXIT_COMMAND "killall -9 daos_server")

  # Test will start running after following line is printed
  set(H5VL_TEST_SERVER_START_MSG "DAOS I/O Engine")

  # Test will start running after following line is printed
  set(H5VL_TEST_CLIENT_HELPER_START_MSG "listening on")

  # Look for and match DAOS POOL UUID using regex
  set(H5VL_TEST_CLIENT_INIT_TOKEN_REGEX "([a-f0-9]+-[a-f0-9]+-[a-f0-9]+-[a-f0-9]+-[a-f0-9]+)")
  set(H5VL_TEST_CLIENT_INIT_TOKEN_VAR "DAOS_POOL")

  # Allow server errors
  set(HDF5_VOL_TEST_SERVER_ALLOW_ERRORS TRUE)
endif()

# Do not forget to include env command
set(H5VL_TEST_ENV_VARS "env ${DAOS_CLIENT_ENV_VARS}")

#------------------------------------------------------------------------------
# DAOS VOL specific tests
#------------------------------------------------------------------------------
add_subdirectory(daos_vol)

#------------------------------------------------------------------------------
# VOL tests
#------------------------------------------------------------------------------

# We don't support filters
set(H5VL_TEST_NO_FILTERS TRUE)

# Disable automatic chunk sizing for VOL tests
set(DAOS_CLIENT_ENV_VARS
  ${DAOS_CLIENT_ENV_VARS}
  "HDF5_DAOS_CHUNK_TARGET_SIZE=1073741824"
)
set(H5VL_TEST_ENV_VARS "env ${DAOS_CLIENT_ENV_VARS}")

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
