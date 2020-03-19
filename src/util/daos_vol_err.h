/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 DAOS VOL connector. The full copyright      *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * daos_vol_err.h
 *
 */

#ifndef daos_vol_err_H
#define daos_vol_err_H

#ifdef __cplusplus
extern "C" {
#endif

#include "H5Epublic.h"

const char *H5_daos_err_to_string(int ret);

extern hid_t dv_err_stack_g;
extern hid_t dv_err_class_g;

#define DAOS_VOL_ERR_CLS_NAME "DAOS VOL"
#define DAOS_VOL_ERR_LIB_NAME "DAOS VOL"
#define DAOS_VOL_ERR_VER      "1.0.0"

#define SUCCEED 0
#define FAIL    (-1)

#ifndef FALSE
  #define FALSE false
#endif
#ifndef TRUE
  #define TRUE true
#endif

/* Private error codes for asynchronous operations */
#define H5_DAOS_INCOMPLETE -1   /* Operation has not yet completed (should only be in the item struct) */
#define H5_DAOS_PRE_ERROR -2    /* A precursor to this task failed (should only be used as the task return value) */
#define H5_DAOS_H5_CLOSE_ERROR -3 /* Failed to close HDF5 object */
#define H5_DAOS_H5_DECODE_ERROR -4 /* Failed to decode HDF5 object */
#define H5_DAOS_H5_TCONV_ERROR -5 /* HDF5 type conversion failed */
#define H5_DAOS_H5_COPY_ERROR -6 /* HDF5 copy operation failed */
#define H5_DAOS_H5PSET_ERROR -7 /* Failed to set info on HDF5 property list */
#define H5_DAOS_H5PGET_ERROR -8 /* Failed to get info from HDF5 property list */
#define H5_DAOS_REMOTE_ERROR -9 /* An operation failed on another process */
#define H5_DAOS_MPI_ERROR -10    /* MPI operation failed */
#define H5_DAOS_DAOS_GET_ERROR -11 /* Can't get data from DAOS */
#define H5_DAOS_ALLOC_ERROR -12  /* Memory allocation failed */
#define H5_DAOS_CPL_CACHE_ERROR -13 /* Failed to fill creation property list cache */
#define H5_DAOS_BAD_VALUE -14    /* Invalid value received */
#define H5_DAOS_PROGRESS_ERROR -15 /* Failed to progress scheduler */

/* Use FUNC to safely handle variations of C99 __func__ keyword handling */
#ifdef H5_HAVE_C99_FUNC
#define FUNC __func__
#elif defined(H5_HAVE_FUNCTION)
#define FUNC __FUNCTION__
#else
#error "We need __func__ or __FUNCTION__ to test function names!"
#endif

/* Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside
 * the function
 */
#define D_GOTO_ERROR(err_major, err_minor, ret_val, ...)                                                           \
{                                                                                                                  \
    H5E_auto2_t func;                                                                                              \
                                                                                                                   \
    /* Check whether automatic error reporting has been disabled */                                                \
    H5Eget_auto2(H5E_DEFAULT, &func, NULL);                                                                        \
    if (func) {                                                                                                    \
        if (dv_err_stack_g >= 0 && dv_err_class_g >= 0)                                                            \
            H5Epush2(dv_err_stack_g, __FILE__, FUNC, __LINE__, dv_err_class_g, err_major, err_minor, __VA_ARGS__); \
        else {                                                                                                     \
            fprintf(stderr, __VA_ARGS__);                                                                          \
            fprintf(stderr, "\n");                                                                                 \
        }                                                                                                          \
    }                                                                                                              \
                                                                                                                   \
    ret_value = ret_val;                                                                                           \
    goto done;                                                                                                     \
}

/* Macro to push the current function to the current error stack
 * without calling goto. This is used for handling the case where
 * an error occurs during cleanup past the "done" label inside a
 * function so that an infinite loop does not occur where goto
 * continually branches back to the label.
 */
#define D_DONE_ERROR(err_major, err_minor, ret_val, ...)                                                           \
{                                                                                                                  \
    H5E_auto2_t func;                                                                                              \
                                                                                                                   \
    /* Check whether automatic error reporting has been disabled */                                                \
    H5Eget_auto2(H5E_DEFAULT, &func, NULL);                                                                        \
    if (func) {                                                                                                    \
        if (dv_err_stack_g >= 0 && dv_err_class_g >= 0)                                                            \
            H5Epush2(dv_err_stack_g, __FILE__, FUNC, __LINE__, dv_err_class_g, err_major, err_minor, __VA_ARGS__); \
        else {                                                                                                     \
            fprintf(stderr, __VA_ARGS__);                                                                          \
            fprintf(stderr, "\n");                                                                                 \
        }                                                                                                          \
    }                                                                                                              \
                                                                                                                   \
    ret_value = ret_val;                                                                                           \
}

/* Macro to simply jump to the "done" label inside the function,
 * setting ret_value to the given value. This is often used for
 * short circuiting in functions when certain conditions arise.
 */
#define D_GOTO_DONE(ret_val)                                                                                       \
{                                                                                                                  \
    ret_value = ret_val;                                                                                           \
    goto done;                                                                                                     \
}

/* Macro to print out the VOL connector's current error stack
 * and then clear it for future use
 */
#define PRINT_ERROR_STACK                                                                                          \
{                                                                                                                  \
    H5E_auto2_t func;                                                                                              \
                                                                                                                   \
    /* Check whether automatic error reporting has been disabled */                                                \
    H5Eget_auto2(H5E_DEFAULT, &func, NULL);                                                                        \
    if (func) {                                                                                                    \
        if ((dv_err_stack_g >= 0) && (H5Eget_num(dv_err_stack_g) > 0)) {                                           \
            H5Eprint2(dv_err_stack_g, NULL);                                                                       \
            H5Eclear2(dv_err_stack_g);                                                                             \
        }                                                                                                          \
    }                                                                                                              \
}

#define D_FUNC_LEAVE_API                                                                                           \
{                                                                                                                  \
    PRINT_ERROR_STACK                                                                                              \
    return ret_value;                                                                                              \
}

#define D_FUNC_LEAVE                                                                                               \
{                                                                                                                  \
    return ret_value;                                                                                              \
}

/* Error handling macros for the VOL test suite */

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
 * spaces.
 */
#ifdef DV_PLUGIN_DEBUG
#define TESTING(S)  {printf("Testing %-66s\n\n", S); fflush(stdout);}
#define PASSED()    {puts("PASSED\n"); fflush(stdout);}
#define H5_FAILED() {puts("*FAILED*\n"); fflush(stdout);}
#define SKIPPED()   {puts("- SKIPPED -\n"); fflush(stdout);}
#else
#define TESTING(S)  {printf("Testing %-66s", S); fflush(stdout);}
#define PASSED()    {puts("PASSED"); fflush(stdout);}
#define H5_FAILED() {puts("*FAILED*"); fflush(stdout);}
#define SKIPPED()   {puts("- SKIPPED -"); fflush(stdout);}
#endif

#define TEST_ERROR  {H5_FAILED(); AT(); goto error;}

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_err_H */
