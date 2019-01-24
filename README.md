# HDF5 DAOS VOL connector

HDF5 DAOS VOL connector version 1.0.0 - currently under development


### Table of Contents:

    I. Introduction
    II. Installation
        A. Prerequisites
            i. External Libraries
        B. Building the DAOS VOL connector
            i. Obtaining the Source
            ii. One-Step Build
                a. Build Script Options
            iii. Manual Build
                a. Options for `configure`
                b. Options for CMake
            iv. Build Results
        C. Testing the DAOS VOL connector installation
    III. Using the DAOS VOL connector
        A. Writing HDF5 DAOS VOL connector applications
            i. Skeleton Example
        B. Building HDF5 DAOS VOL connector applications
        C. Running HDF5 DAOS VOL connector applications
            i. Runtime Environment
            ii. Example applications
    IV. Feature Support
        A. Unsupported HDF5 API calls
        B. Unsupported HDF5 Features
        C. Problematic HDF5 Features
    V. More Information



# I. Introduction

The DAOS VOL connector is a connector for HDF5 designed with the goal of allowing
HDF5 applications, both existing and future, to utilize the DAOS object storage
systems by translating HDF5 API calls into DAOS calls, as defined by the DAOS
API (See section V. for more information on DAOS).

Using a VOL connector allows an existing HDF5 application to interface with
different storage systems with minimal changes necessary. The connector accomplishes
this by utilizing the HDF5 Virtual Object Layer in order to re-route HDF5's
public API calls to specific callbacks in the connector which handle all of the
usual HDF5 operations. The HDF5 Virtual Object Layer is an abstraction layer
that sits directly between HDF5's public API and the underlying storage system.
In this manner of operation, the mental data model of an HDF5 application can
be preserved and transparently mapped onto storage systems that differ from a
native filesystem, such as Amazon's S3.

The DAOS VOL connector is under active development, and details given here may
change.

--------------------------------------------------------------------------------

# II. Installation

Notes and instructions related to obtaining, building and installing the DAOS VOL
connector and accompanying HDF5 library.



## II.A. Prerequisites

Before building and using the HDF5 DAOS VOL connector, a few requirements must be met.

### II.A.i. External Libraries

To build the DAOS VOL connector, the following libraries are required:

+ libuuid - UUID (Universally unique identifier) support

Compiled libraries must either exist in the system's library paths or must be
supplied to the DAOS VOL connector's build scripts. Refer to section II.B.ii. below
for more information.


## II.B. Building the DAOS VOL connector

### II.B.i. Obtaining the Source

The latest and most up-to-date DAOS VOL connector code can be viewed at:

https://bitbucket.hdfgroup.org/users/jhenderson/repos/daos-vol/browse

and can directly be obtained from:

`https://jhenderson@bitbucket.hdfgroup.org/scm/~jhenderson/daos-vol.git`

A source distribution of HDF5 has been included in the DAOS VOL connector source
in the `/hdf5` directory. This version of HDF5 has been modified to support
the DAOS VOL connector.

### II.B.ii. One-step Build

Use one of the supplied Autotools or CMake build scripts, depending on preference or
system support.

+ Autotools

    Run `build_vol_autotools.sh`. See section II.B.ii.a for configuration options.


+ CMake

    Run `build_vol_cmake.sh` (Linux or OS X) or `build_vol_cmake.bat` (Windows).
    See section II.B.ii.a for configuration options.


By default, all of these build scripts will compile and link with the provided
HDF5 source distribution. However, if you wish to use a manually built version of
the HDF5 library, include the flag `-H <dir>` where `dir` is the path to the HDF5
install prefix. Refer to the documentation in `hdf5/release_docs` (where `hdf5` is
the HDF5 distribution root directory) for more information on building HDF5 manually.
Note that if you wish to use a manually built version of the HDF5 library, it must be
a version which contains the VOL abstraction layer; otherwise, the DAOS VOL connector will
not function correctly.

NOTE: For those who are capable of using both build systems, the autotools build currently
does not support out-of-tree builds. If the DAOS VOL source directory is used for an autotools
build, it is important not to re-use the source directory for a later build using CMake.
This will causes build conflicts and result in strange and unexpected behavior.


### II.B.ii.a. Build Script Options

The following configuration options are available to all of the build scripts:

    -h      Prints out a help message indicating script usage and available options.

    -d      Enables debugging information printouts within the DAOS VOL connector.

    -m      Enables memory usage tracking within the DAOS VOL connector. This option is
            mostly useful in helping to diagnose any possible memory leaks or other
            memory errors within the connector.

    -t      Build the HDF5 tools with DAOS VOL connector support.
            WARNING: This option is experimental and should not currently be used.

    -P DIR  Specifies where the DAOS VOL connector should be installed. The default
            installation prefix is `daos_vol_build` inside the DAOS VOL connector source
            root directory.

    -H DIR  Prevents building of the provided HDF5 source. Instead, uses the compiled
            library found at directory `DIR`, where `DIR` is the path used as the
            installation prefix when building HDF5 manually.

    -U DIR  Specifies the top-level directory where cURL is installed. Used if cURL is
            not installed to a system path or used to override 

Additionally, the CMake build scripts have the following configuration option:

    -B DIR  Specifies the directory that CMake should use as the build tree location.
            The default build tree location is `daos_vol_cmake_build_files` inside the
            DAOS VOL connector source root directory. Note that the DAOS VOL does not
            support in-source CMake builds.

    -G DIR  Specifies the CMake Generator to use when generating the build files
            for the project. On Unix systems, the default is "Unix Makefiles" and if
            this is not changed, the build script will automatically attempt to build
            the project after generating the Makefiles. If the generator is changed, the
            build script will only generate the build files and the build command to
            build the project will have to be run manually.

### II.B.iii. Manual Build

In general, the process for building the DAOS VOL connector involves either obtaining a VOL-enabled
HDF5 distribution or building one from source. Then, the DAOS VOL connector is built using that
HDF5 distribution by including the appropriate header files and linking against the HDF5 library.

Once you have a VOL-enabled HDF5 distribution available, follow the instructions below for your
respective build system in order to build the DAOS VOL connector against the HDF5 distribution.


Autotools
---------

    $ autogen.sh
    $ configure --with-hdf5[=DIR] [options]
    $ make
    $ make check
    $ make install

CMake
-----

    $ mkdir builddir
    $ cd builddir
    $ cmake -G "CMake Generator (Unix Makefiles, etc.)" -DPREBUILT_HDF5_DIR=built_hdf5_dir [options] daos_vol_src_dir
    $ build command (e.g. `make && make install` for CMake Generator "Unix Makefiles")

and optionally:

    $ cpack

### II.B.iii.a. Options for `configure`

When building the DAOS VOL connector manually using Autotools, the following options are
available to `configure`.

The options in the supplied Autotools build script are mapped to the corresponding options here:

    -h, --help      Prints out a help message indicating script usage and available
                    options.

    --prefix=DIR    Specifies the location for the resulting files. The default location
                    is `daos_vol_build` in the same directory as configure.

    --enable-build-mode=(production|debug)
                    Sets the build mode to be used.
                    Debug - enable debugging printouts within the DAOS VOL connector.
                    Production - Focus more on optimization.

    --enable-mem-tracking
                    Enables memory tracking within the DAOS VOL connector. This option is
                    mostly useful in helping to diagnose any possible memory leaks or
                    other memory errors within the connector.

    --enable-tests
                    Enables/Disables building of the DAOS VOL connector tests.

    --enable-examples
                    Enables/Disables building of the DAOS VOL HDF5 examples.

    --enable-tools
                    Enables/Disables building of the HDF5 tools with DAOS VOL connector
                    support. (Currently experimental and should not be used)

    --with-hdf5=DIR Used to specify the directory where an HDF5 distribution that uses
                    the VOL layer has already been built. This is to help the DAOS VOL
                    connector locate the HDF5 header files that it needs to include.

    --with-uuid=DIR Used to specify the top-level directory where the UUID library is
                    installed, if it is not installed to a system path.


### II.B.iii.b. Options for CMake

When building the DAOS VOL connector manually using CMake, the following options are available.

Some of the options in the supplied CMake build script are mapped to the corresponding options here:

    CMAKE_INSTALL_PREFIX (Default: `daos_vol_build` in DAOS VOL connector source root
                          directory)
                    Specifies the directory where CMake will install the resulting
                    files to.

    PREBUILT_HDF5_DIR (Default: empty)
                    Specifies a directory which contains a pre-built HDF5
                    distribution which uses the VOL abstraction layer. By default,
                    the DAOS VOL connector's CMake build will attempt to build the
                    included HDF5 source distribution, then use that to build the
                    connector itself. However, if a VOL-enabled HDF5 distribution
                    is already available, this option can be set to point to the
                    directory of the HDF5 distribution. In this case, CMake will
                    use that HDF5 distribution to build the DAOS VOL connector and
                    will not attempt to build HDF5 again.

    BUILD_SHARED_LIBS (Default: ON)
                    Enables/Disables building of the DAOS VOL as a shared library.

    BUILD_STATIC_EXECS (Default: OFF)
                    Enables/Disables building of DAOS VOL executables as static
                    executables.

    DAOS_VOL_ENABLE_DEBUG (Default: OFF)
                    Enables/Disables debugging printouts within the DAOS VOL connector.

    DAOS_VOL_ENABLE_MEM_TRACKING (Default: OFF)
                    Enables/Disables memory tracking withing the DAOS VOL connector. This
                    options is mostly useful in helping to diagnose any possible memory
                    leaks or other memory errors within the connector.

    DAOS_VOL_ENABLE_EXAMPLES (Default: ON)
                    Enables/Disables building of the DAOS VOL HDF5 examples.

    BUILD_TESTING (Default: ON)
                    Enables/Disables building of the DAOS VOL tests.

### II.B.iv. Build Results

If the build is successful, files are written into an installation directory. By default,
these files are placed in `daos_vol_build` in the DAOS VOL connector source root directory.
For Autotools, this default can be overridden with `build_vol_autotools.sh -P DIR`
(when using the build script) or `configure --prefix=<DIR>` (when building manually).
For CMake, the equivalent for overriding this default is `build_vol_cmake.sh/.bat -P DIR`
(when using the build script) or `-DCMAKE_INSTALL_PREFIX=DIR` (when building manually).

If the DAOS VOL connector was built using one of the included build scripts, all of the usual files
from an HDF5 source build should appear in the respective `bin`, `include`, `lib` and `share`
directories in the install directory. Notable among these is `bin/h5pcc` (when built with
Autotools), a special-purpose compiler wrapper script that streamlines the process of building
HDF5 applications.



## II.C. Testing the DAOS VOL connector installation

The DAOS VOL connector tests require access to an HDF5 DAOS API-aware server -- see section
II.A.ii and III.C.

After building the DAOS VOL connector and obtaining access to a server which implements
the DAOS API according to the above reference, it is highly advised that you run
`make check` (for Autotools builds) or `ctest .` (for CMake builds) to verify that
the HDF5 library and DAOS VOL connector are functioning correctly.

Each of these commands will run the `test_daos_vol` executable, which is built by
each of the DAOS VOL connector's build systems and contains a set of tests to cover a
moderate amount of the HDF5 public API. Alternatively, this executable can simply
be run directly.

--------------------------------------------------------------------------------

# III. Using the DAOS VOL connector

This section outlines the unique aspects of writing, building and running HDF5
applications with the DAOS VOL connector.



## III.A. Writing HDF5 DAOS VOL connector applications

Any HDF5 application using the DAOS VOL connector must:

+ Include `daos_vol_public.h`, found in the `include` directory of the DAOS VOL
  connector installation directory.

+ Link against `libdaosvol.a` (or similar), found in the `lib` directory of the
  DAOS VOL connector installation directory.

An HDF5 DAOS VOL connector application requires three new function calls in addition
to those for an equivalent HDF5 application:

+ H5daos_init() - Initializes the DAOS VOL connector

    Called upon application startup, before any file is accessed.


+ H5Pset_fapl_daos() - Set DAOS VOL connector access on File Access Property List.
  
    Called to prepare a FAPL to open a file through the DAOS VOL connector. See
    `https://support.hdfgroup.org/HDF5/Tutor/property.html#fa` for more information
    about File Access Property Lists.


+ H5daos_term() - Cleanly shutdown the DAOS VOL connector

    Called on application shutdown, after all files have been closed.


### III.A.i. Skeleton Example

Below is a no-op application that opens and closes a file using the DAOS VOL connector.
For clarity, no error-checking is performed.

    #include "hdf5.h"
    #include "daos_vol_public.h"

    int main(void)
    {
        hid_t fapl_id;
        hid_t file_id;

        H5daos_init(MPI_COMM_WORLD, pool_uuid, NULL);

        fapl_id = H5Pcreate(H5P_FILE_ACCESS);
        H5Pset_fapl_daos(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL);
        file_id = H5Fopen("my/file.h5");

        /* operate on file */

        H5Pclose(fapl_id);
        H5Fclose(file_id);

        H5daos_term();

        return 0;
    }



## III.B. Building HDF5 DAOS VOL connector applications

Assuming an application has been written following the above instructions, the application
must be built prior to running. In general, the application should be built as normal for
any other HDF5 application.

To link in the required libraries, the compiler will likely require the additional linker
flags:

`-ldaosvol -luuid`

However, these may vary depending on platform, compiler and installation location of the
DAOS VOL connector.

If the DAOS VOL connector was built using Autotools, it is highly recommended that compilation
of HDF5 DAOS VOL connector applications be done using the supplied `h5pcc` script, as it will
manage linking with the HDF5 library. `h5pcc` may be found in the `/bin` directory of the
installation. The above notice about additional libraries applies to usage of `h5pcc`.
For example:

`h5pcc -ldaosvol -luuid my_daosvol_application.c -o my_executable`



## III.C. Running HDF5 DAOS VOL connector applications

Running applications that use the HDF5 DAOS VOL connector requires access to a server which implements
the DAOS API (); see section II.A.ii.

### III.C.i. Runtime Environment

For the DAOS VOL connector to correctly interact with an DAOS API-aware server instance, there are two
environment variables which should first be set. These are:

    + DAOS_POOL - The UUID of the DAOS pool to use

    + DAOS_SVCL - A comma-separated list of ...

### III.C.ii. Example applications

The file `test/test_daos_vol.c`, in addition to being the source for the DAOS VOL connector
test suite, serves double purpose with each test function being an example application
in miniature, focused on a particular behavior. This application tests a moderate amount
of HDF5's public API functionality with the DAOS VOL connector and should be a good indicator
of whether the DAOS VOL connector is working correctly in conjunction with a running HDF5 DAOS
API-aware instance.

In addition to this file, some of the example C applications included with HDF5
distributions have been adapted to work with the DAOS VOL connector and are included
under the top-level `examples` directory in the DAOS VOL connector source root directory.

--------------------------------------------------------------------------------

# IV. Feature Support

Not all aspects of HDF5 are implemented by or are applicable to the DAOS VOL connector.



## IV.A. Unsupported HDF5 API calls

Due to a combination of lack of server support and the complexity in implementing them,
or due to a particular call not making sense from the server's perspective, the following
HDF5 API calls are currently unsupported:

+ H5A interface

    + 

+ H5D interface

    + 

+ H5F interface

    + 

+ H5G interface

    + 

+ H5L interface

    + 

+ H5O interface

    + 

+ H5R interface

    + 



## IV.B. Unsupported HDF5 Features

The following other features are currently unsupported:

+ 



## IV.C. Problematic HDF5 Features

Due to underlying implementation details, the following circumstances are
known to be problematic for the DAOS VOL connector and will likely cause issues
for the application if not avoided or taken into account:

+ 

--------------------------------------------------------------------------------

# V. More Information

