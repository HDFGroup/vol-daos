# HDF5 DAOS VOL connector

HDF5 DAOS VOL connector - currently under development


### Table of Contents:

    I. Introduction
    II. Installation
        A. Prerequisites
            i. External Libraries
        B. Building the DAOS VOL connector
            i. Obtaining the Source
            ii. Quick one-Step Build
                a. Build Script Options
            iii. Manual Build
                a. Options for `configure`
                b. Options for CMake
            iv. Build Results
    III. Using/testing the DAOS VOL connector
    IV. More Information



# I. Introduction

The HDF5 DAOS VOL connector is a connector for HDF5 designed with the goal of allowing
HDF5 applications to utilize the DAOS object storage system by translating HDF5 API calls
into DAOS calls, as defined by the [DAOS API](https://github.com/daos-stack/daos/blob/master/src/include/daos_api.h) (See section IV. for more information on DAOS).
The DAOS VOL connector is currently built as a dynamically-loaded library that is external
to HDF5 and is treated similar to dynamically-loaded HDF5 filter plugins.

Using a VOL connector allows an existing HDF5 application to interface with
different storage systems with minimal changes necessary. The connector accomplishes
this by utilizing the HDF5 Virtual Object Layer in order to re-route HDF5's
public API calls to specific callbacks in the connector which handle all of the
usual HDF5 operations. The HDF5 Virtual Object Layer is an abstraction layer
that sits directly between HDF5's public API and the underlying storage system.
In this manner of operation, the mental data model of an HDF5 application can
be preserved and transparently mapped onto storage systems that differ from a
native filesystem.

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

+ libdaos - [DAOS](https://github.com/daos-stack/daos) (Distributed Asynchronous Object Storage)

+ libcart - DAOS CART dependency

+ libuuid - UUID (Universally unique identifier) support

Compiled libraries must either exist in the system's library paths or must be
supplied to the DAOS VOL connector's build scripts. Refer to section II.B.ii. below
for more information.


## II.B. Building the DAOS VOL connector

### II.B.i. Obtaining the Source

The latest and most up-to-date DAOS VOL connector code can be viewed at:

https://bitbucket.hdfgroup.org/projects/HDF5VOL/repos/daos-vol/browse

and can directly be obtained from:

`https://bitbucket.hdfgroup.org/scm/hdf5vol/daos-vol.git`

A source distribution of HDF5 has been included in the DAOS VOL connector source
in the `src/hdf5` directory. This version of HDF5 has been modified to support
the DAOS VOL connector.

### II.B.ii. Quick one-step Build

Use one of the supplied Autotools or CMake build scripts, depending on preference or
system support:

+ Autotools

    Run `build_vol_autotools.sh`. See section II.B.ii.a for configuration options.


+ CMake

    Run `build_vol_cmake.sh`. See section II.B.ii.a for configuration options.


These scripts are simply convenient wrappers around the necessary build commands
and they use default options. For example, the default installation directory is
`daos_vol_install` under the root of the source code tree.

By default, these build scripts will compile and link with the provided HDF5 source
distribution. However, if you wish to use a manually built version of the HDF5 library,
include the flag `-H <dir>` where `dir` is the path to the HDF5 install prefix. Refer
to the documentation in `src/hdf5/release_docs` (where `src/hdf5` is the included HDF5
distribution root directory) for more information on building HDF5 manually.
Note that if you wish to use a manually built version of the HDF5 library, it must be
a version which contains the VOL abstraction layer; otherwise, the DAOS VOL connector will
not function correctly. It is also important to note that the current version of the
DAOS VOL connector requires a specialized version of [HDF5](https://bitbucket.hdfgroup.org/projects/HDFFV/repos/hdf5/browse?at=refs%2Fheads%2Ffeature%2Fdaos_vol_support) which was modified to
support the DAOS VOL connector. This will not be required in the future.

NOTE: For those who are capable of using both build systems, the autotools build currently
does not support out-of-tree builds. If the DAOS VOL source directory is used for an autotools
build, it is important not to re-use the source directory for a later build using CMake.
This will cause build conflicts and result in strange and unexpected behavior.


### II.B.ii.a. Build Script Options

The following configuration options are available to the build scripts:

    -h      Prints out a help message indicating script usage and available options.

    -d      Enables debugging information printouts within the DAOS VOL connector.

    -m      Enables memory usage tracking within the DAOS VOL connector. This option is
            mostly useful in helping to diagnose any possible memory leaks or other
            memory errors within the connector.

    -P DIR  Specifies where the DAOS VOL connector should be installed. The default
            installation prefix is `daos_vol_install` inside the DAOS VOL connector source
            root directory.

    -H DIR  Prevents building of the provided HDF5 source. Instead, uses the compiled
            library found at directory `DIR`, where `DIR` is the path used as the
            installation prefix when building HDF5 manually.

    -D DIR  Specifies the top-level directory where DAOS is installed. Used if DAOS is
            not installed to a system path or used to override.

    -C DIR  Specifies the top-level directory where CART is installed. Used if CART is
            not installed to a system path or used to override.

    -U DIR  Specifies the top-level directory where UUID is installed. Used if UUID is
            not installed to a system path or used to override.

Additionally, the CMake build script has the following configuration option:

    -B DIR  Specifies the directory that CMake should use as the build tree location.
            The default build tree location is `daos_vol_build` inside the
            DAOS VOL connector source root directory. Note that the DAOS VOL does not
            support in-source CMake builds.

    -G DIR  Specifies the CMake Generator to use when generating the build files
            for the project. On Unix systems, the default is "Unix Makefiles".

### II.B.iii. Manual Build

First, make sure that the necessary git submodules are available:

    $ cd daos-vol
    $ git submodule init
    $ git submodule update

Following this, the general process for building the DAOS VOL connector involves first building the
included HDF5 source distribution (`src/hdf5`) or building one from source by obtaining a specialized
version of [HDF5](https://bitbucket.hdfgroup.org/projects/HDFFV/repos/hdf5/browse?at=refs%2Fheads%2Ffeature%2Fdaos_vol_support) which was modified to support the DAOS VOL connector. Then, the DAOS VOL connector is built using that HDF5 distribution by including the appropriate header files and linking against the HDF5 library.

The following should be noted when building HDF5 for use with the DAOS VOL connector:

+ The HDF5 parallel option must be enabled for the DAOS VOL connector to be built properly.

+ If the DAOS VOL connector is to be built using CMake, HDF5 must be built with CMake as well.

Once a suitable HDF5 distribution has been built and is available, follow the instructions below
for your respective build system in order to build the DAOS VOL connector against the HDF5 distribution.

Autotools
---------

    $ autogen.sh
    $ configure --with-hdf5[=DIR] --prefix=DIR [options]
    $ make
    $ make install

Optionally, `make check` can be run before `make install` in order to run the DAOS VOL connector tests.

CMake
-----

    $ mkdir build
    $ cd build
    $ cmake -DHDF5_VOL_DAOS_USE_SYSTEM_HDF5=ON -DCMAKE_INSTALL_PREFIX=install_dir [options] daos_vol_src_dir
    $ build command (e.g. `make && make install` for CMake Generator "Unix Makefiles")

### II.B.iii.a. Options for `configure`

When building the DAOS VOL connector manually using Autotools, the following options are
available to `configure`.

The options in the supplied Autotools build script are mapped to the corresponding options here:

    -h, --help      Prints out a help message indicating script usage and available
                    options.

    --prefix=DIR    Specifies the location for the resulting files. The default location
                    is `daos_vol_build` in the same directory as configure.

    --with-hdf5=DIR Used to specify the directory where an HDF5 distribution that uses
                    the VOL layer has already been built. This is to help the DAOS VOL
                    connector locate the HDF5 header files that it needs to include.

    --with-uuid=DIR Used to specify the top-level directory where the UUID library is
                    installed, if it is not installed to a system path.

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


### II.B.iii.b. Options for CMake

When building the DAOS VOL connector manually using CMake, the following options are available.

Some of the options in the supplied CMake build script are mapped to the corresponding options here:

    CMAKE_INSTALL_PREFIX (Default: `daos_vol_build` in DAOS VOL connector source root
                          directory)
                    Specifies the directory where CMake will install the resulting
                    files to.

    HDF5_VOL_DAOS_USE_SYSTEM_HDF5 (Default: ON)
                    Specifies whether to use the system-installed HDF5 when building
                    the DAOS VOL connector. If this option is turned off, the included
                    HDF5 source distribution will be built and used.

    HDF5_VOL_DAOS_ENABLE_DEBUG (Default: OFF)
                    Enables/Disables debugging printouts within the DAOS VOL connector.

    HDF5_VOL_DAOS_ENABLE_MEM_TRACKING (Default: OFF)
                    Enables/Disables memory tracking withing the DAOS VOL connector. This
                    options is mostly useful in helping to diagnose any possible memory
                    leaks or other memory errors within the connector.

    BUILD_SHARED_LIBS (Default: ON)
                    Enables/Disables building of the DAOS VOL as a shared library.

    BUILD_EXAMPLES (Default: ON)
                    Enables/Disables building of the DAOS VOL connector HDF5 examples.

    BUILD_TESTING (Default: ON)
                    Enables/Disables building of the DAOS VOL connector tests.

### II.B.iv. Build Results

If the build is successful, the following files will be written into the installation directory:

```
bin/

include/
     daos_vol_config.h - The header file containing the configuration options for the built DAOS VOL connector
     daos_vol_public.h - The DAOS VOL connector's public header file to include in HDF5 applications

lib/
    pkgconfig/
        hdf5_vol_daos.pc - The DAOS VOL connector pkgconfig file

    libhdf5_vol_daos.so - The DAOS VOL connector library

If built with CMake:
share/
    cmake/
        hdf5_vol_daos/
            hdf5_vol_daos-config.cmake
            hdf5_vol_daos-config-version.cmake
            hdf5_vol_daos-targets.cmake
            hdf5_vol_daos-targets-relwithdebinfo.cmake
```

If the DAOS VOL connector was built using one of the included build scripts, all of the usual files
from an HDF5 source build should also appear in the respective `bin`, `include`, `lib` and `share`
directories in the install directory. Notable among these is `bin/h5pcc` (when built with
Autotools) or `bin/h5cc` (when built with CMake), special-purpose compiler wrapper scripts that streamline the process of building HDF5 applications.


--------------------------------------------------------------------------------

# III. Using/testing the DAOS VOL connector

For information on how to use the DAOS VOL connector with an HDF5 application, as well as
how to test that the VOL connector is functioning properly, please refer to the DAOS VOL
User's Guide under `docs/HDF5 DAOS VOL User's Guide.pdf`.


--------------------------------------------------------------------------------

# IV. More Information

