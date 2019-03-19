#!/bin/bash

if [ $# -ne 1 ]; then
  echo "usage: $0 pool_uuid"
  exit
fi

D_LOG_FILE=$PWD/daos_log/dmg.log \
OFI_INTERFACE=ofi+sockets \
OFI_INTERFACE=em1 \
orterun -np 1 --ompi-server file:$PWD/daos.uri dmg destroy --pool=$1
