/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include <daos.h>
#include <daos_vol.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Macros for printing standard messages and issuing errors */
#define AT() printf("        at %s:%d in %s()...\n", __FILE__, __LINE__, __FUNCTION__)
#define FAILED()                                                                                             \
    do {                                                                                                     \
        puts("*FAILED*");                                                                                    \
        fflush(stdout);                                                                                      \
    } while (0)
#define ERROR                                                                                                \
    do {                                                                                                     \
        FAILED();                                                                                            \
        AT();                                                                                                \
        goto error;                                                                                          \
    } while (0)
#define PRINTF_ERROR(...)                                                                                    \
    do {                                                                                                     \
        FAILED();                                                                                            \
        AT();                                                                                                \
        printf("        " __VA_ARGS__);                                                                      \
        printf("\n");                                                                                        \
        goto error;                                                                                          \
    } while (0)
