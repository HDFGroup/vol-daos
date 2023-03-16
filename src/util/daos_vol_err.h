/**
 * Copyright (c) 2018-2023 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
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
extern hid_t dv_obj_err_maj_g;
extern hid_t dv_async_err_g;

#define DAOS_VOL_ERR_CLS_NAME "DAOS VOL"
#define DAOS_VOL_ERR_LIB_NAME "DAOS VOL"
#define DAOS_VOL_ERR_VER      "1.2.0"

#define H5E_OBJECT     dv_obj_err_maj_g
#define H5E_DAOS_ASYNC (dv_async_err_g)

#define SUCCEED 0
#define FAIL    (-1)

#ifndef FALSE
#define FALSE false
#endif
#ifndef TRUE
#define TRUE true
#endif

/* Private error codes for asynchronous operations */
typedef enum {
    H5_DAOS_INCOMPLETE =
        1, /* Operation has not yet completed (should only be in the item struct) (must be first) */
    H5_DAOS_SHORT_CIRCUIT, /* Operation completed successfully earlier than expected (but has not finished yet
                              internally) (must be second) */
    H5_DAOS_CANCELED,      /* Operation canceled by application (must be third) */
    H5_DAOS_PRE_ERROR, /* A precursor to this task failed (should only be used as the task return value) (must
                          be fourth) */
    H5_DAOS_PREREQ_ERROR,         /* A prerequisite operation failed */
    H5_DAOS_H5_OPEN_ERROR,        /* Failed to open HDF5 object */
    H5_DAOS_H5_CLOSE_ERROR,       /* Failed to close HDF5 object */
    H5_DAOS_H5_GET_ERROR,         /* Failed to get value */
    H5_DAOS_H5_ENCODE_ERROR,      /* Failed to encode HDF5 object */
    H5_DAOS_H5_DECODE_ERROR,      /* Failed to decode HDF5 object */
    H5_DAOS_H5_CREATE_ERROR,      /* Failed to create HDF5 object */
    H5_DAOS_H5_DESTROY_ERROR,     /* Failed to destroy HDF5 object */
    H5_DAOS_H5_TCONV_ERROR,       /* HDF5 type conversion failed */
    H5_DAOS_H5_COPY_ERROR,        /* HDF5 copy operation failed */
    H5_DAOS_H5_UNSUPPORTED_ERROR, /* Unsupported HDF5 operation */
    H5_DAOS_H5_ITER_ERROR,        /* Error occurred during iteration */
    H5_DAOS_H5_SCATGATH_ERROR,    /* Error occurred during HDF5 scatter/gather operation */
    H5_DAOS_H5PSET_ERROR,         /* Failed to set info on HDF5 property list */
    H5_DAOS_H5PGET_ERROR,         /* Failed to get info from HDF5 property list */
    H5_DAOS_REMOTE_ERROR,         /* An operation failed on another process */
    H5_DAOS_MPI_ERROR,            /* MPI operation failed */
    H5_DAOS_DAOS_GET_ERROR,       /* Can't get data from DAOS */
    H5_DAOS_TASK_LIST_ERROR,      /* Task list operation failed */
    H5_DAOS_ALLOC_ERROR,          /* Memory allocation failed */
    H5_DAOS_FREE_ERROR,           /* Failed to free memory */
    H5_DAOS_CPL_CACHE_ERROR,      /* Failed to fill creation property list cache */
    H5_DAOS_BAD_VALUE,            /* Invalid value received */
    H5_DAOS_NONEXIST_LINK,        /* Link does not exist */
    H5_DAOS_TRAVERSE_ERROR,       /* Failed to traverse path */
    H5_DAOS_FOLLOW_ERROR,         /* Failed to follow link */
    H5_DAOS_CALLBACK_ERROR,       /* Callback function returned failure */
    H5_DAOS_PROGRESS_ERROR,       /* Failed to progress scheduler */
    H5_DAOS_SETUP_ERROR,          /* Error during operation setup */
    H5_DAOS_FILE_EXISTS,          /* File already exists */
    H5_DAOS_LINK_EXISTS,          /* Link already exists */
} H5_daos_error_code_t;

/* Error macros */

#ifdef H5_NO_DEPRECATED_SYMBOLS

/*
 * Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside the
 * function. (v2 errors only)
 */
#define D_GOTO_ERROR(err_major, err_minor, ret_val, ...)                                                     \
    do {                                                                                                     \
        H5E_auto2_t err_func;                                                                                \
                                                                                                             \
        /* Check whether automatic error reporting has been disabled */                                      \
        (void)H5Eget_auto2(H5E_DEFAULT, &err_func, NULL);                                                    \
        if (err_func) {                                                                                      \
            if (dv_err_stack_g >= 0 && dv_err_class_g >= 0) {                                                \
                H5Epush2(dv_err_stack_g, __FILE__, __func__, __LINE__, dv_err_class_g, err_major, err_minor, \
                         __VA_ARGS__);                                                                       \
            }                                                                                                \
            else {                                                                                           \
                fprintf(stderr, __VA_ARGS__);                                                                \
                fprintf(stderr, "\n");                                                                       \
            }                                                                                                \
        }                                                                                                    \
                                                                                                             \
        ret_value = ret_val;                                                                                 \
        goto done;                                                                                           \
    } while (0)

/*
 * Macro to push the current function to the current error stack
 * without calling goto. This is used for handling the case where
 * an error occurs during cleanup past the "done" label inside a
 * function so that an infinite loop does not occur where goto
 * continually branches back to the label. (v2 errors only)
 */
#define D_DONE_ERROR(err_major, err_minor, ret_val, ...)                                                     \
    do {                                                                                                     \
        H5E_auto2_t err_func;                                                                                \
                                                                                                             \
        /* Check whether automatic error reporting has been disabled */                                      \
        (void)H5Eget_auto2(H5E_DEFAULT, &err_func, NULL);                                                    \
        if (err_func) {                                                                                      \
            if (dv_err_stack_g >= 0 && dv_err_class_g >= 0)                                                  \
                H5Epush2(dv_err_stack_g, __FILE__, __func__, __LINE__, dv_err_class_g, err_major, err_minor, \
                         __VA_ARGS__);                                                                       \
            else {                                                                                           \
                fprintf(stderr, __VA_ARGS__);                                                                \
                fprintf(stderr, "\n");                                                                       \
            }                                                                                                \
        }                                                                                                    \
                                                                                                             \
        ret_value = ret_val;                                                                                 \
    } while (0)

/*
 * Macro to print out the VOL connector's current error stack
 * and then clear it for future use. (v2 errors only)
 */
#define PRINT_ERROR_STACK                                                                                    \
    do {                                                                                                     \
        H5E_auto2_t err_func;                                                                                \
                                                                                                             \
        /* Check whether automatic error reporting has been disabled */                                      \
        (void)H5Eget_auto2(H5E_DEFAULT, &err_func, NULL);                                                    \
        if (err_func) {                                                                                      \
            if ((dv_err_stack_g >= 0) && (H5Eget_num(dv_err_stack_g) > 0)) {                                 \
                H5Eprint2(dv_err_stack_g, NULL);                                                             \
                H5Eclear2(dv_err_stack_g);                                                                   \
            }                                                                                                \
        }                                                                                                    \
    } while (0)

#else

/*
 * Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside the
 * function. (compatible with v1 and v2 errors)
 */
#define D_GOTO_ERROR(err_major, err_minor, ret_val, ...)                                                     \
    do {                                                                                                     \
        unsigned is_v2_err;                                                                                  \
        union {                                                                                              \
            H5E_auto1_t err_func_v1;                                                                         \
            H5E_auto2_t err_func_v2;                                                                         \
        } err_func;                                                                                          \
                                                                                                             \
        /* Determine version of error */                                                                     \
        (void)H5Eauto_is_v2(H5E_DEFAULT, &is_v2_err);                                                        \
                                                                                                             \
        if (is_v2_err)                                                                                       \
            (void)H5Eget_auto2(H5E_DEFAULT, &err_func.err_func_v2, NULL);                                    \
        else                                                                                                 \
            (void)H5Eget_auto1(&err_func.err_func_v1, NULL);                                                 \
                                                                                                             \
        /* Check whether automatic error reporting has been disabled */                                      \
        if ((is_v2_err && err_func.err_func_v2) || (!is_v2_err && err_func.err_func_v1)) {                   \
            if (dv_err_stack_g >= 0 && dv_err_class_g >= 0) {                                                \
                H5Epush2(dv_err_stack_g, __FILE__, __func__, __LINE__, dv_err_class_g, err_major, err_minor, \
                         __VA_ARGS__);                                                                       \
            }                                                                                                \
            else {                                                                                           \
                fprintf(stderr, __VA_ARGS__);                                                                \
                fprintf(stderr, "\n");                                                                       \
            }                                                                                                \
        }                                                                                                    \
                                                                                                             \
        ret_value = ret_val;                                                                                 \
        goto done;                                                                                           \
    } while (0)

/*
 * Macro to push the current function to the current error stack
 * without calling goto. This is used for handling the case where
 * an error occurs during cleanup past the "done" label inside a
 * function so that an infinite loop does not occur where goto
 * continually branches back to the label. (compatible with v1
 * and v2 errors)
 */
#define D_DONE_ERROR(err_major, err_minor, ret_val, ...)                                                     \
    do {                                                                                                     \
        unsigned is_v2_err;                                                                                  \
        union {                                                                                              \
            H5E_auto1_t err_func_v1;                                                                         \
            H5E_auto2_t err_func_v2;                                                                         \
        } err_func;                                                                                          \
                                                                                                             \
        /* Determine version of error */                                                                     \
        (void)H5Eauto_is_v2(H5E_DEFAULT, &is_v2_err);                                                        \
                                                                                                             \
        if (is_v2_err)                                                                                       \
            (void)H5Eget_auto2(H5E_DEFAULT, &err_func.err_func_v2, NULL);                                    \
        else                                                                                                 \
            (void)H5Eget_auto1(&err_func.err_func_v1, NULL);                                                 \
                                                                                                             \
        /* Check whether automatic error reporting has been disabled */                                      \
        if ((is_v2_err && err_func.err_func_v2) || (!is_v2_err && err_func.err_func_v1)) {                   \
            if (dv_err_stack_g >= 0 && dv_err_class_g >= 0) {                                                \
                H5Epush2(dv_err_stack_g, __FILE__, __func__, __LINE__, dv_err_class_g, err_major, err_minor, \
                         __VA_ARGS__);                                                                       \
            }                                                                                                \
            else {                                                                                           \
                fprintf(stderr, __VA_ARGS__);                                                                \
                fprintf(stderr, "\n");                                                                       \
            }                                                                                                \
        }                                                                                                    \
                                                                                                             \
        ret_value = ret_val;                                                                                 \
    } while (0)

/*
 * Macro to print out the VOL connector's current error stack
 * and then clear it for future use. (compatible with v1 and v2 errors)
 */
#define PRINT_ERROR_STACK                                                                                    \
    do {                                                                                                     \
        unsigned is_v2_err;                                                                                  \
        union {                                                                                              \
            H5E_auto1_t err_func_v1;                                                                         \
            H5E_auto2_t err_func_v2;                                                                         \
        } err_func;                                                                                          \
                                                                                                             \
        /* Determine version of error */                                                                     \
        (void)H5Eauto_is_v2(H5E_DEFAULT, &is_v2_err);                                                        \
                                                                                                             \
        if (is_v2_err)                                                                                       \
            (void)H5Eget_auto2(H5E_DEFAULT, &err_func.err_func_v2, NULL);                                    \
        else                                                                                                 \
            (void)H5Eget_auto1(&err_func.err_func_v1, NULL);                                                 \
                                                                                                             \
        /* Check whether automatic error reporting has been disabled */                                      \
        if ((is_v2_err && err_func.err_func_v2) || (!is_v2_err && err_func.err_func_v1)) {                   \
            if ((dv_err_stack_g >= 0) && (H5Eget_num(dv_err_stack_g) > 0)) {                                 \
                H5Eprint2(dv_err_stack_g, NULL);                                                             \
                H5Eclear2(dv_err_stack_g);                                                                   \
            }                                                                                                \
        }                                                                                                    \
    } while (0)

#endif

/*
 * Macro to simply jump to the "done" label inside the function,
 * setting ret_value to the given value. This is often used for
 * short circuiting in functions when certain conditions arise.
 */
#define D_GOTO_DONE(ret_val)                                                                                 \
    do {                                                                                                     \
        ret_value = ret_val;                                                                                 \
        goto done;                                                                                           \
    } while (0)

/*
 * Macro to return from a top-level API function, printing
 * out the VOL connector's error stack on the way out.
 * It should be ensured that this macro is only called once
 * per HDF5 operation. If it is called multiple times per
 * operation (e.g. due to calling top-level API functions
 * internally), the VOL connector's error stack will be
 * inconsistent/incoherent.
 */
#define D_FUNC_LEAVE_API                                                                                     \
    do {                                                                                                     \
        /* If at the top level of connector callbacks, make                                                  \
         * all "unsafe" task list tasks available.                                                           \
         */                                                                                                  \
        H5_daos_dec_api_cnt();                                                                               \
        if ((H5_daos_api_count == 0) && H5_daos_task_list_g)                                                 \
            H5_daos_task_list_safe(H5_daos_task_list_g);                                                     \
        PRINT_ERROR_STACK;                                                                                   \
        return ret_value;                                                                                    \
    } while (0)

/*
 * Macro to return from internal functions.
 */
#define D_FUNC_LEAVE                                                                                         \
    do {                                                                                                     \
        return ret_value;                                                                                    \
    } while (0)

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_err_H */
