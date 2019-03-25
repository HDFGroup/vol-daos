##
## Makefile.am
## Run automake to generate a Makefile.in from this file.
#
# DAOS VOL Makefile(.in)
#

HDF5_INSTALL_DIR=@DV_HDF5_INSTALL_DIR@

AM_CFLAGS=@AM_CFLAGS@ @DV_CFLAGS@ -I$(HDF5_INSTALL_DIR)/include
AM_CPPFLAGS=@AM_CPPFLAGS@ @DV_CPPFLAGS@
AM_LDFLAGS=@AM_LDFLAGS@ @DV_LDFLAGS@ -L$(HDF5_INSTALL_DIR)/lib -lhdf5

ACLOCAL_AMFLAGS=-I m4

# Add in the linker flags for UUID
if !UUID_DEFAULT_DIR
    AM_CFLAGS += -I$(UUID_INSTALL_DIR)/include -L$(UUID_INSTALL_DIR)/lib$(LIB_SUFFIX)
endif

AM_CFLAGS += -luuid

# Our main target, the DAOS VOL connector library
lib_LTLIBRARIES=libdaosvol.la

# DAOS VOL's source file and private headers
libdaosvol_la_SOURCES=daos_vol.c daos_vol_attr.c daos_vol_dset.c \
	daos_vol_file.c daos_vol_group.c daos_vol_link.c daos_vol_map.c \
	daos_vol_obj.c daos_vol_req.c daos_vol_type.c daos_vol.h \
	util/daos_vol_err.h util/daos_vol_err.c util/daos_vol_mem.h \
	util/daos_vol_mem.c

# Public headers
include_HEADERS=daos_vol_public.h daos_vol_config.h

# install libdaosvol.settings in lib directory
settingsdir=$(libdir)
settings_DATA=libdaosvol.settings

# Automake needs to be taught how to build lib, progs, and tests targets.
# These will be filled in automatically for the most part (e.g.,
# lib_LIBRARIES are built for lib target), but EXTRA_LIB, EXTRA_PROG, and
# EXTRA_TEST variables are supplied to allow the user to force targets to
# be built at certain times.
LIB = $(lib_LTLIBRARIES)
PROGS = 

# lib/progs/tests targets recurse into subdirectories. build-* targets
# build files in this directory.
build-lib: $(LIB)
build-progs: $(LIB) $(PROGS)
