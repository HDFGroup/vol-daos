## Makefile.am
## Run automake to generate a Makefile.in from this file.
##
#
# DAOS VOL Library Examples Makefile(.in)
#

# Libraries to link to while building
LIBHDF5=$(top_builddir)/src/libhdf5.la

# Scripts used to build examples
# If only shared libraries have been installed, have h5cc build examples with
# shared libraries instead of static libraries
H5CC=${DESTDIR}$(bindir)/h5pcc


# H5_CFLAGS holds flags that should be used when building hdf5,
# but which should not be exported to h5cc for building other programs.
# AM_CFLAGS is an automake construct which should be used by Makefiles 
# instead of CFLAGS, as CFLAGS is reserved solely for the user to define.
# This applies to FCFLAGS, CXXFLAGS, CPPFLAGS, and LDFLAGS as well.

AM_CFLAGS=@AM_CFLAGS@ @DV_CFLAGS@
AM_FCFLAGS=@AM_FCFLAGS@ @DV_FCFLAGS@
AM_CXXFLAGS=@AM_CXXFLAGS@ @DV_CXXFLAGS@
AM_CPPFLAGS=@AM_CPPFLAGS@ @DV_CPPFLAGS@
AM_LDFLAGS=@AM_LDFLAGS@ @DV_LDFLAGS@

ACLOCAL_AMFLAGS=-I m4

# .chkexe files are used to mark tests that have run successfully.
# .chklog files are output from those tests.
CHECK_CLEANFILES=*.chkexe *.chklog

INSTALL_SCRIPT_FILES = run-c-ex.sh
INSTALL_TOP_SCRIPT_FILES = run-all-ex.sh
INSTALL_TOP_FILES = README

# Example programs.
# Don't tell automake about them, because if it knew they were programs,
# it would try to compile them instead of using the h5cc script.
# Use the boilerplate in config/examples.am instead.
EXAMPLE_PROG = h5dsm_dset_rpartial h5dsm_obj_info h5dsm_dset_w1m \
               h5dsm_obj_open h5dsm_dset_wpartial h5dsm_obj_open_addr \
               h5dsm_attr_create h5dsm_dset_write h5dsm_slink_create \
               h5dsm_attr_iter h5dsm_tcommit h5dsm_attr_open \
               h5dsm_file_create h5dsm_attr_read h5dsm_file_open \
               h5dsm_attr_write h5dsm_group_create h5dsm_ttconv \
               h5dsm_dset_create h5dsm_group_open h5dsm_tvlen \
               h5dsm_dset_open h5dsm_link_exists h5dsm_dset_r1m \
               h5dsm_link_iter h5dsm_dset_read h5dsm_map
TEST_SCRIPT=testh5cc.sh h5dsm_test.sh
TEST_EXAMPLES_SCRIPT=$(INSTALL_SCRIPT_FILES)

# Install files
# List all file that should be installed in examples directory
INSTALL_FILES = h5dsm_dset_rpartial.c h5dsm_obj_info.c h5dsm_dset_w1m.c \
                h5dsm_obj_open.c h5dsm_dset_wpartial.c h5dsm_obj_open_addr.c \
                h5dsm_attr_create.c h5dsm_dset_write.c h5dsm_slink_create.c \
                h5dsm_attr_iter.c h5dsm_example.h h5dsm_tcommit.c \
                h5dsm_attr_open.c h5dsm_file_create.c h5dsm_attr_read.c \
                h5dsm_file_open.c h5dsm_attr_write.c h5dsm_group_create.c \
                h5dsm_ttconv.c h5dsm_dset_create.c h5dsm_group_open.c \
                h5dsm_tvlen.c h5dsm_dset_open.c h5dsm_link_exists.c \
                h5dsm_dset_r1m.c h5dsm_link_iter.c h5dsm_dset_read.c h5dsm_map.c


# How to build examples, using installed version of h5cc
$(EXTRA_PROG): $(H5CC)
	$(H5CC) $(H5CCFLAGS) $(CFLAGS) -o $@ $(srcdir)/$@.c;

# Some examples depend on files created by other examples.
#dv_read.chkexe_: dv_write.chkexe_
#dv_chunk_read.chkexe_: dv_extend_write.chkexe_
#dv_crtgrpd.chkexe_: dv_crtgrpar.chkexe_
# dv_rdwt and dv_crtatt both modify the same file created by
# dv_crtdat. Serialize them.
#dv_rdwt.chkexe_: dv_crtdat.chkexe_
#dv_crtatt.chkexe_: dv_rdwt.chkexe_

# The external link examples demonstrate how to use paths; they need
# directories to be created to do this.
EXTLINK_DIRS=red blue u2w

$(EXTLINK_DIRS):
	echo $(mkdir_p) $@
	$(mkdir_p) $@

CHECK_CLEANFILES+=$(EXTLINK_DIRS)

# Example directory
# Note: no '/' after DESTDIR.  Explanation in commence.am
EXAMPLEHLDIR=${DESTDIR}$(exec_prefix)/share/daos_vol_examples/c/hl
EXAMPLEDIR=${DESTDIR}$(exec_prefix)/share/daos_vol_examples/c
EXAMPLETOPDIR=${DESTDIR}$(exec_prefix)/share/daos_vol_examples
EXAMPLEBASEDIR=${DESTDIR}$(exec_prefix)/share

# List dependencies for each program.  Normally, automake would take
# care of this for us, but if we tell automake about the programs it
# will try to build them with the normal C compiler, not h5cc.  This is
# an inelegant way of solving the problem.
# All programs share the same build rule and a dependency on the main hdf5
# library above.
h5dsm_file_create: $(srcdir)/h5dsm_file_create.c
h5dsm_file_open: $(srcdir)/h5dsm_file_open.c
h5dsm_group_create: $(srcdir)/h5dsm_group_create.c
h5dsm_group_open: $(srcdir)/h5dsm_group_open.c
h5dsm_dset_create: $(srcdir)/h5dsm_dset_create.c
h5dsm_dset_open: $(srcdir)/h5dsm_dset_open.c
h5dsm_dset_write: $(srcdir)/h5dsm_dset_write.c
h5dsm_dset_read: $(srcdir)/h5dsm_dset_read.c
h5dsm_dset_wpartial: $(srcdir)/h5dsm_dset_wpartial.c
h5dsm_dset_rpartial: $(srcdir)/h5dsm_dset_rpartial.c
h5dsm_dset_w1m: $(srcdir)/h5dsm_dset_w1m.c
h5dsm_dset_r1m: $(srcdir)/h5dsm_dset_r1m.c
h5dsm_link_exists: $(srcdir)/h5dsm_link_exists.c
h5dsm_link_iter: $(srcdir)/h5dsm_link_iter.c
h5dsm_attr_create: $(srcdir)/h5dsm_attr_create.c
h5dsm_attr_open: $(srcdir)/h5dsm_attr_open.c
h5dsm_attr_write: $(srcdir)/h5dsm_attr_write.c
h5dsm_attr_read: $(srcdir)/h5dsm_attr_read.c
h5dsm_attr_iter: $(srcdir)/h5dsm_attr_iter.c
h5dsm_slink_create: $(srcdir)/h5dsm_slink_create.c
h5dsm_obj_open: $(srcdir)/h5dsm_obj_open.c
h5dsm_obj_open_addr: $(srcdir)/h5dsm_obj_open_addr.c
h5dsm_obj_info: $(srcdir)/h5dsm_obj_info.c
h5dsm_map: $(srcdir)/h5dsm_map.c
h5dsm_ttconv: $(srcdir)/h5dsm_ttconv.c
h5dsm_tvlen: $(srcdir)/h5dsm_tvlen.c

# Assume that all tests in this directory are examples, and tell
# conclude.am when to build them.
EXTRA_PROG = $(EXAMPLE_PROG) $(EXAMPLE_PROG_PARA)

# We need to tell automake what to clean
MOSTLYCLEANFILES=*.raw *.meta *.o
CHECK_CLEANFILES+=*.h5
CLEANFILES=$(EXAMPLE_PROG) $(EXAMPLE_PROG_PARA)

# How to create EXAMPLEDIR if it doesn't already exist
$(EXAMPLEBASEDIR):
	mkdir -p $(EXAMPLEBASEDIR)
$(EXAMPLETOPDIR):
	mkdir -p $(EXAMPLEBASEDIR)
	mkdir -p $(EXAMPLETOPDIR)
$(EXAMPLEDIR):
	mkdir -p $(EXAMPLEBASEDIR)
	mkdir -p $(EXAMPLETOPDIR)
	mkdir -p $(EXAMPLEDIR)
	mkdir -p $(EXAMPLEHLDIR)

# Install and uninstall rules.  We install the source files, not the
# example programs themselves.
install-data-local:
	@$(MAKE) $(AM_MAKEFLAGS) install-examples
uninstall-local:
	@$(MAKE) $(AM_MAKEFLAGS) uninstall-examples

install-examples: $(EXAMPLEDIR) $(INSTALL_FILES) 
	@for f in X $(INSTALL_FILES); do                                     \
	  if test $$f != X; then                                             \
	    (set -x; $(INSTALL) $(srcdir)/$$f $(EXAMPLEDIR)/$$f || exit 1);    \
            chmod a-x $(EXAMPLEDIR)/$$f;                                     \
	  fi;                                                                \
	done
	@for f in X $(INSTALL_SCRIPT_FILES); do                                   \
	  if test $$f != X; then                                             \
	    (set -x; $(INSTALL) $$f $(EXAMPLEDIR)/. || exit 1);\
	  fi;                                                                \
	done
	@for f in X $(INSTALL_TOP_FILES); do                       \
	  if test $$f != X; then                                             \
	    (set -x; $(INSTALL) $(srcdir)/$$f $(EXAMPLETOPDIR)/. || exit 1); \
            chmod a-x $(EXAMPLETOPDIR)/$$f;\
	  fi;                                                                \
	done
	@for f in X $(INSTALL_TOP_SCRIPT_FILES); do                       \
	  if test $$f != X; then                                             \
	    (set -x; $(INSTALL) $(srcdir)/$$f $(EXAMPLETOPDIR)/. || exit 1); \
	  fi;                                                                \
	done

uninstall-examples:
	@if test -n "$(INSTALL_FILES)" -a -d $(EXAMPLEDIR); then             \
	  set -x; cd $(EXAMPLEDIR) && $(RM) $(INSTALL_FILES);                \
	fi
	@if test -n "$(INSTALL_SCRIPT_FILES)" -a -d $(EXAMPLEDIR); then           \
	  set -x; cd $(EXAMPLEDIR) && $(RM) $(INSTALL_SCRIPT_FILES);              \
	fi
	@if test -n "$(INSTALL_TOP_FILES)" -a -d $(EXAMPLETOPDIR); then    \
	  set -x; cd $(EXAMPLETOPDIR) && $(RM) $(INSTALL_TOP_FILES);       \
	fi
	@if test -n "$(INSTALL_TOP_SCRIPT_FILES)" -a -d $(EXAMPLETOPDIR); then    \
	  set -x; cd $(EXAMPLETOPDIR) && $(RM) $(INSTALL_TOP_SCRIPT_FILES);       \
	fi

installcheck-local:
	@if test "$(STATIC_SHARED)" = "static, shared"; then               \
	  H5CCFLAGS="-shlib" $(MAKE) $(AM_MAKEFLAGS) check;                \
	  $(MAKE) $(AM_MAKEFLAGS) clean;                                   \
	  H5CCFLAGS="" $(MAKE) $(AM_MAKEFLAGS) check;                      \
	elif test "$(STATIC_SHARED)" = "shared"; then                     \
	  H5CCFLAGS="-shlib" $(MAKE) $(AM_MAKEFLAGS) check;                \
	else                                                              \
	  $(MAKE) $(AM_MAKEFLAGS) check;                                   \
	fi 
	@if test "$(INSTALL_FILES)" -a $(TEST_EXAMPLES_SCRIPT) -a -d $(EXAMPLEDIR); then \
	    echo "============================";                                         \
	    echo "Testing $(TEST_EXAMPLES_SCRIPT)";                                      \
	    echo "============================";                                         \
	    (cd $(EXAMPLEDIR);                                                           \
	     /bin/sh ./$(TEST_EXAMPLES_SCRIPT);)                                                   \
	fi
