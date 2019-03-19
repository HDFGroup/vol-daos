/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef VOL_TEST_H
#define VOL_TEST_H

#include <hdf5.h>
#include <H5private.h>

/* Moved from h5test */

/*
 * Predefined test verbosity levels.
 *
 * Convention:
 *
 * The higher the verbosity value, the more information printed.
 * So, output for higher verbosity also include output of all lower
 * verbosity.
 *
 *  Value     Description
 *  0         None:   No informational message.
 *  1                 "All tests passed"
 *  2                 Header of overall test
 *  3         Default: header and results of individual test
 *  4
 *  5         Low:    Major category of tests.
 *  6
 *  7         Medium: Minor category of tests such as functions called.
 *  8
 *  9         High:   Highest level.  All information.
 */
#define VERBO_NONE 0     /* None    */
#define VERBO_DEF  3     /* Default */
#define VERBO_LO   5     /* Low     */
#define VERBO_MED  7     /* Medium  */
#define VERBO_HI   9     /* High    */

/*
 * Verbose queries
 * Only None needs an exact match.  The rest are at least as much.
 */

/* A macro version of HDGetTestVerbosity(). */
/* Should be used internally by the libtest.a only. */
#define HDGetTestVerbosity() (TestVerbosity)

#define VERBOSE_NONE  (HDGetTestVerbosity()==VERBO_NONE)
#define VERBOSE_DEF  (HDGetTestVerbosity()>=VERBO_DEF)
#define VERBOSE_LO  (HDGetTestVerbosity()>=VERBO_LO)
#define VERBOSE_MED  (HDGetTestVerbosity()>=VERBO_MED)
#define VERBOSE_HI  (HDGetTestVerbosity()>=VERBO_HI)

/*
 * Test controls definitions.
 */
#define SKIPTEST  1  /* Skip this test */
#define ONLYTEST  2  /* Do only this test */
#define BEGINTEST  3  /* Skip all tests before this test */

/*
 * This contains the filename prefix specificied as command line option for
 * the parallel test files.
 */
H5TEST_DLLVAR char *paraprefix;
#ifdef H5_HAVE_PARALLEL
H5TEST_DLLVAR MPI_Info h5_io_info_g;         /* MPI INFO object for IO */
#endif

/*
 * Print the current location on the standard output stream.
 */
#define AT()     printf ("   at %s:%d in %s()...\n",        \
        __FILE__, __LINE__, FUNC);

/*
 * The name of the test is printed by saying TESTING("something") which will
 * result in the string `Testing something' being flushed to standard output.
 * If a test passes, fails, or is skipped then the PASSED(), H5_FAILED(), or
 * SKIPPED() macro should be called.  After H5_FAILED() or SKIPPED() the caller
 * should print additional information to stdout indented by at least four
 * spaces.  If the h5_errors() is used for automatic error handling then
 * the H5_FAILED() macro is invoked automatically when an API function fails.
 */
#define TESTING(WHAT)  {printf("Testing %-62s",WHAT); fflush(stdout);}
#define TESTING_2(WHAT)  {printf("  Testing %-60s",WHAT); fflush(stdout);}
#define PASSED()  {puts(" PASSED");fflush(stdout);}
#define H5_FAILED()  {puts("*FAILED*");fflush(stdout);}
#define H5_WARNING()  {puts("*WARNING*");fflush(stdout);}
#define SKIPPED()  {puts(" -SKIP-");fflush(stdout);}
#define PUTS_ERROR(s)   {puts(s); AT(); goto error;}
#define TEST_ERROR      {H5_FAILED(); AT(); goto error;}
#define STACK_ERROR     {H5Eprint2(H5E_DEFAULT, stdout); goto error;}
#define FAIL_STACK_ERROR {H5_FAILED(); AT(); H5Eprint2(H5E_DEFAULT, stdout); \
    goto error;}
#define FAIL_PUTS_ERROR(s) {H5_FAILED(); AT(); puts(s); goto error;}

/*
 * Alarm definitions to wait up (terminate) a test that runs too long.
 */
#define H5_ALARM_SEC  1200  /* default is 20 minutes */
#define ALARM_ON  TestAlarmOn()
#define ALARM_OFF  HDalarm(0)

/* Flags for h5_fileaccess_flags() */
#define H5_FILEACCESS_VFD       0x01
#define H5_FILEACCESS_VOL       0x02
#define H5_FILEACCESS_LIBVER    0x04

H5TEST_DLL hid_t h5_fileaccess(void);

/******************************************************************************/

/* The name of the file that all of the tests will operate on */
#define TEST_FILE_NAME "vol_test.h5"
extern char vol_test_filename[];

/* The names of a set of container groups which hold objects
 * created by each of the different types of tests.
 */
#define GROUP_TEST_GROUP_NAME         "group_tests"
#define ATTRIBUTE_TEST_GROUP_NAME     "attribute_tests"
#define DATASET_TEST_GROUP_NAME       "dataset_tests"
#define DATATYPE_TEST_GROUP_NAME      "datatype_tests"
#define LINK_TEST_GROUP_NAME          "link_tests"
#define OBJECT_TEST_GROUP_NAME        "object_tests"
#define MISCELLANEOUS_TEST_GROUP_NAME "miscellaneous_tests"

#define ARRAY_LENGTH(array) sizeof(array) / sizeof(array[0])

#define UNUSED(o) (void) (o);

#define VOL_TEST_FILENAME_MAX_LENGTH 1024

/* The maximum size of a dimension in an HDF5 dataspace as allowed
 * for this testing suite so as not to try to create too large
 * of a dataspace/datatype. */
#define MAX_DIM_SIZE 16

#define NO_LARGE_TESTS

/*
 * XXX: Set of compatibility macros that should be replaced once the
 * VOL connector feature support situation is resolved.
 */
#define GROUP_CREATION_IS_SUPPORTED

hid_t generate_random_datatype(H5T_class_t parent_class);
hid_t generate_random_dataspace(int rank, const hsize_t *max_dims);

#endif
