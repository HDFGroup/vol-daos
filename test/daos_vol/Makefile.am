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