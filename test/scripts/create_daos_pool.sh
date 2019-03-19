#!/bin/bash
if [ $# -lt 1 ]
then
    pool_size=2G
else
    # 2GB pool by default
    pool_size=$1
fi

D_LOG_FILE=$PWD/daos_log/dmg.log \
OFI_INTERFACE=ofi+sockets \
OFI_INTERFACE=em1 \
orterun -np 1 --ompi-server file:$PWD/daos.uri dmg create --size="${pool_size}"
