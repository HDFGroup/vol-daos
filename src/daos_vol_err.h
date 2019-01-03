/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
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

/* Macro to print out the VOL plugin's current error stack
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
