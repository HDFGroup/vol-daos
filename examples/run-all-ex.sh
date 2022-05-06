#! /bin/sh
#

#
# Copyright (c) 2018-2022 The HDF Group.
#
# SPDX-License-Identifier: BSD-3-Clause
#

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                                               #
# This script will run the scripts to compile and run the installed hdf5        #
# examples.                                                                     #
#                                                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

echo "Run c examples"
if ((cd c; sh ./run-c-ex.sh) && \
   (if test -d fortran; then   
       echo "Run fortran examples" 
       cd fortran; sh ./run-fortran-ex.sh 
    fi) 
   (if test -d c++; then
       echo "Run c++ examples" 
       cd c++; sh ./run-c++-ex.sh
    fi)
   (if test -d hl; then
       echo "Run hl examples." 
       cd hl; sh ./run-hl-ex.sh
    fi)); then
   echo "Done"
   exit 0
else
   exit 1
fi

