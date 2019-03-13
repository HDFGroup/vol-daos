#!/bin/bash

if [ $# -lt 2 ]; then
  echo "usage: $0 num_procs application"
  exit
fi

D_LOG_FILE=$PWD/$0.log \
OFI_INTERFACE=ofi+sockets \
OFI_INTERFACE=em1 \
orterun -np $1 --ompi-server file:$PWD/daos.uri "$2" "$3"
