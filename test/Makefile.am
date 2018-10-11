#
# Copyright by The HDF Group.
# Copyright by the Board of Trustees of the University of Illinois.
# All rights reserved.
#
# This file is part of HDF5.  The full HDF5 copyright notice, including
# terms governing use, modification, and redistribution, is contained in
# the files COPYING and Copyright.html.  COPYING can be found at the root
# of the source code distribution tree; Copyright.html can be found at the
# root level of an installed copy of the electronic HDF5 document set and
# is linked from the top-level documents page.  It can also be found at
# http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have
# access to either file, you may request a copy from help@hdfgroup.org.
##
## Makefile.am
## Run automake to generate a Makefile.in from this file.
#
# DAOS VOL test suite Makefile(.in)
#

HDF5_INSTALL_DIR=@DV_HDF5_INSTALL_DIR@

AM_CFLAGS=@AM_CFLAGS@ @DV_CFLAGS@
AM_CPPFLAGS=@AM_CPPFLAGS@ @DV_CPPFLAGS@
AM_LDFLAGS=@AM_LDFLAGS@ @DV_LDFLAGS@

ACLOCAL_AMFLAGS=-I m4

LIBDAOSVOL = $(top_builddir)/src/libdaosvol.la

# Defines for building the test suite
test_PROGRAMS = test_daos_vol
test_daos_vol_SOURCES = test_daos_vol.c
test_daos_vol_LDADD = $(LIBDAOSVOL) $(HDF5_INSTALL_DIR)/lib/libhdf5.la
test_daos_vol_CFLAGS = -I$(top_srcdir)/src -I$(HDF5_INSTALL_DIR)/include

testdir=$(bindir)

TESTS = $(test_PROGRAMS)

build-tests: $(TESTS)

# Tell Automake to build tests when the user types `make all' (this is
# not its default behavior).
all-local: $(TESTS)