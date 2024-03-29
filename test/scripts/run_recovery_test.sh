#!/bin/bash
##
## This is an example of how to run the recovery test (h5daos_test_recovery.c) under test/daos_vol.  The binary is located at build/bin/h5daos_test_recovery. 
## Please update the HDF5_DAOS_VOL_BUILD_PATH variable for your installation of DAOS-VOL.  You may need to update other variables.
## To run the script, please pass in the DAOS Pool UUID and the DAOS server system name list as the command-line input.
##
TMP_DIR=/tmp/$USER
AGENT_DIR=$TMP_DIR
URI_PATH=$HOME
HDF5_DAOS_VOL_BUILD_PATH=$HOME/daos-vol_new/build

mpirun -np 4 \
--map-by node \
--hostfile $HOME/scripts/clients3.txt \
-x D_LOG_FILE=$TMP_DIR/daos_client.log             \
-x D_LOG_MASK=DEBUG                                \
-x CRT_PHY_ADDR_STR=ofi+sockets                    \
-x OFI_INTERFACE=ib0                               \
-x DAOS_SINGLETON_CLI=1                            \
-x CRT_TIMEOUT=30			           \
-x DD_STDERR=ERR                                   \
-x CRT_ATTACH_INFO_PATH=$URI_PATH                  \
-x DAOS_AGENT_DRPC_DIR=$AGENT_DIR                  \
-x HDF5_PLUGIN_PATH=$HDF5_DAOS_VOL_BUILD_PATH/bin  \
-x HDF5_VOL_CONNECTOR=daos                         \
-x DAOS_POOL=$1                                    \
-x DAOS_SYS=$2                                     \
$HDF5_DAOS_VOL_BUILD_PATH/bin/h5daos_test_recovery \
--daosObjClass=RP_4G1                           \
--nGroups=100                   \
--dimsDset=80x80                         \
--mapEntries=80                             \
--dimOfAttr=10                          \
--daosServerRanks=3,4                           \
--faultGroups=10,13                   \
--faultOps=write,open                    \
--objects=dset,attr				\
--nFaultInjects=2
