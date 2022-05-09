/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Contains macros to facilitate testing the DAOS VOL plugin.
 */

#ifndef H5DAOS_TEST_H
#define H5DAOS_TEST_H

/* Public headers needed by this file */
#include <assert.h>
#include <getopt.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <uuid/uuid.h>

#define MAINPROCESS (!mpi_rank)

/*
 * Print the current location on the standard output stream.
 */
#define AT()                                                                                                 \
    do {                                                                                                     \
        if (MAINPROCESS)                                                                                     \
            printf("   at %s:%d in %s()...\n", __FILE__, __LINE__, __func__);                                \
    } while (0)

/*
 * The name of the test is printed by saying TESTING("something") which will
 * result in the string `Testing something' being flushed to standard output.
 * If a test passes, fails, or is skipped then the PASSED(), H5_FAILED(), or
 * SKIPPED() macro should be called.  After H5_FAILED() or SKIPPED() the caller
 * should print additional information to stdout indented by at least four
 * spaces.
 */
#define TESTING(S)                                                                                           \
    do {                                                                                                     \
        if (MAINPROCESS)                                                                                     \
            printf("Testing %-66s", S);                                                                      \
        fflush(stdout);                                                                                      \
    } while (0)
#define TESTING_2(S)                                                                                         \
    do {                                                                                                     \
        if (MAINPROCESS)                                                                                     \
            printf("    Testing %-62s", S);                                                                  \
        fflush(stdout);                                                                                      \
    } while (0)
#define PASSED()                                                                                             \
    do {                                                                                                     \
        if (MAINPROCESS)                                                                                     \
            puts("PASSED");                                                                                  \
        fflush(stdout);                                                                                      \
    } while (0)
#define H5_FAILED()                                                                                          \
    do {                                                                                                     \
        if (MAINPROCESS)                                                                                     \
            puts("*FAILED*");                                                                                \
        fflush(stdout);                                                                                      \
    } while (0)
#define SKIPPED()                                                                                            \
    do {                                                                                                     \
        if (MAINPROCESS)                                                                                     \
            puts("- SKIPPED -");                                                                             \
        fflush(stdout);                                                                                      \
    } while (0)
#define HDputs(S) puts(S)
#define TEST_ERROR                                                                                           \
    do {                                                                                                     \
        H5_FAILED();                                                                                         \
        AT();                                                                                                \
        goto error;                                                                                          \
    } while (0)

/*
 * Global variables
 */

extern uuid_t pool_uuid;
extern int    mpi_rank;

#endif /* H5DAOS_TEST_H */
