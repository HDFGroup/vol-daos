/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose:	The private header file for the DAOS VOL connector.
 */

#ifndef daos_vol_private_H
#define daos_vol_private_H

/* Public headers needed by this file */
#include "daos_vol.h"

/* GURT headers */
#include <gurt/common.h>
#include <gurt/types.h>

/* DAOS headers */
#include <daos.h>
#include <daos/tse.h>
#include <daos_task.h>
#include <daos_uns.h>

/* System headers */
#include <assert.h>
#include <errno.h>

/* Hash table */
#include "util/daos_vol_hash_table.h"

/* Task list */
#include "util/daos_vol_task_list.h"

/* For DAOS compatibility */
typedef d_iov_t     daos_iov_t;
typedef d_sg_list_t daos_sg_list_t;
#define daos_rank_list_free d_rank_list_free
#define daos_iov_set        d_iov_set
#define DAOS_OF_AKEY_HASHED 0
#define DAOS_OF_DKEY_HASHED 0

/* For HDF5 compatibility */
#ifndef H5E_ID
#define H5E_ID H5E_ATOM
#endif

/******************/
/* Private Macros */
/******************/

/* Define H5VL_VERSION if not already defined */
#ifndef H5VL_VERSION
#define H5VL_VERSION 0
#endif

/* Check for unknown H5VL_VERSION */
#if H5VL_VERSION > 3
#error Unknown H5VL_VERSION - HDF5 library is probably too new or connector is too old
#endif

/* Versioning for request status */
#if H5VL_VERSION >= 2
#define H5_DAOS_REQ_STATUS_OUT_TYPE        H5VL_request_status_t
#define H5_DAOS_REQ_STATUS_OUT_IN_PROGRESS H5VL_REQUEST_STATUS_IN_PROGRESS
#define H5_DAOS_REQ_STATUS_OUT_SUCCEED     H5VL_REQUEST_STATUS_SUCCEED
#define H5_DAOS_REQ_STATUS_OUT_FAIL        H5VL_REQUEST_STATUS_FAIL
#define H5_DAOS_REQ_STATUS_OUT_CANT_CANCEL H5VL_REQUEST_STATUS_CANT_CANCEL
#define H5_DAOS_REQ_STATUS_OUT_CANCELED    H5VL_REQUEST_STATUS_CANCELED
#else
#define H5_DAOS_REQ_STATUS_OUT_TYPE        H5ES_status_t
#define H5_DAOS_REQ_STATUS_OUT_IN_PROGRESS H5ES_STATUS_IN_PROGRESS
#define H5_DAOS_REQ_STATUS_OUT_SUCCEED     H5ES_STATUS_SUCCEED
#define H5_DAOS_REQ_STATUS_OUT_FAIL        H5ES_STATUS_FAIL
#define H5_DAOS_REQ_STATUS_OUT_CANT_CANCEL H5ES_STATUS_IN_PROGRESS
#define H5_DAOS_REQ_STATUS_OUT_CANCELED    H5ES_STATUS_CANCELED
#endif

/* Versioning for "opt_query" callback */
#if H5VL_VERSION >= 1
#define H5_DAOS_OPT_QUERY_OUT_TYPE        uint64_t
#define H5_DAOS_OPT_QUERY_SUPPORTED       H5VL_OPT_QUERY_SUPPORTED
#define H5_DAOS_OPT_QUERY_READ_DATA       H5VL_OPT_QUERY_READ_DATA
#define H5_DAOS_OPT_QUERY_WRITE_DATA      H5VL_OPT_QUERY_WRITE_DATA
#define H5_DAOS_OPT_QUERY_QUERY_METADATA  H5VL_OPT_QUERY_QUERY_METADATA
#define H5_DAOS_OPT_QUERY_MODIFY_METADATA H5VL_OPT_QUERY_MODIFY_METADATA
#define H5_DAOS_OPT_QUERY_COLLECTIVE      H5VL_OPT_QUERY_COLLECTIVE
#define H5_DAOS_OPT_QUERY_NO_ASYNC        H5VL_OPT_QUERY_NO_ASYNC
#define H5_DAOS_OPT_QUERY_MULTI_OBJ       H5VL_OPT_QUERY_MULTI_OBJ
#else
#define H5_DAOS_OPT_QUERY_OUT_TYPE        hbool_t
#define H5_DAOS_OPT_QUERY_SUPPORTED       TRUE
#define H5_DAOS_OPT_QUERY_READ_DATA       FALSE
#define H5_DAOS_OPT_QUERY_WRITE_DATA      FALSE
#define H5_DAOS_OPT_QUERY_QUERY_METADATA  FALSE
#define H5_DAOS_OPT_QUERY_MODIFY_METADATA FALSE
#define H5_DAOS_OPT_QUERY_COLLECTIVE      FALSE
#define H5_DAOS_OPT_QUERY_NO_ASYNC        FALSE
#define H5_DAOS_OPT_QUERY_MULTI_OBJ       FALSE
#endif

/* Versioning for H5Aexists */
#if H5VL_VERSION >= 2
#define H5_DAOS_ATTR_EXISTS_OUT_TYPE hbool_t
#else
#define H5_DAOS_ATTR_EXISTS_OUT_TYPE htri_t
#endif

/* Versioning for H5Lexists */
#if H5VL_VERSION >= 2
#define H5_DAOS_LINK_EXISTS_OUT_TYPE hbool_t
#else
#define H5_DAOS_LINK_EXISTS_OUT_TYPE htri_t
#endif

#define HDF5_VOL_DAOS_VERSION_1 (1) /* Version number of DAOS VOL connector */

/* Macro to ensure H5_DAOS_g is initialized. H5_DAOS_g is only set if
 * the connector is manually initialized; if the connector has been
 * dynamically loaded, there are various places that this macro should
 * be used to check and set H5_DAOS_g if necessary.
 */
#define H5_DAOS_G_INIT(ERR)                                                                                  \
    do {                                                                                                     \
        if (H5_DAOS_g < 0)                                                                                   \
            if ((H5_DAOS_g = H5VLpeek_connector_id_by_value(H5_DAOS_CONNECTOR_VALUE)) < 0)                   \
                D_GOTO_ERROR(H5E_ID, H5E_CANTGET, ERR,                                                       \
                             "unable to get registered ID for DAOS VOL connector");                          \
    } while (0)

/* Constant keys */
#define H5_DAOS_CHUNK_KEY 0u

/* Default target chunk size for automatic chunking */
#define H5_DAOS_CHUNK_TARGET_SIZE_DEF ((uint64_t)(1024 * 1024))

/* Initial allocation sizes */
#define H5_DAOS_GH_BUF_SIZE        1024
#define H5_DAOS_LINK_NAME_BUF_SIZE 2048
#define H5_DAOS_ATTR_NAME_BUF_SIZE 2048
#define H5_DAOS_GINFO_BUF_SIZE     1024
#define H5_DAOS_TYPE_BUF_SIZE      1024
#define H5_DAOS_SPACE_BUF_SIZE     512
#define H5_DAOS_ACPL_BUF_SIZE      1024
#define H5_DAOS_DCPL_BUF_SIZE      1024
#define H5_DAOS_TCPL_BUF_SIZE      1024
#define H5_DAOS_MCPL_BUF_SIZE      1024
#define H5_DAOS_FILL_VAL_BUF_SIZE  1024
#define H5_DAOS_SEQ_LIST_LEN       128
#define H5_DAOS_ITER_LEN           128
#define H5_DAOS_ITER_SIZE_INIT     (4 * 1024)
#define H5_DAOS_ATTR_NUM_AKEYS     5
#define H5_DAOS_ATTR_NAME_BUF_SIZE 2048
#define H5_DAOS_POINT_BUF_LEN      128

/* Size of blob IDs */
#define H5_DAOS_BLOB_ID_SIZE sizeof(uuid_t)

/* Sizes of objects on storage */
#define H5_DAOS_ENCODED_OID_SIZE       16
#define H5_DAOS_ENCODED_CRT_ORDER_SIZE 8
#define H5_DAOS_ENCODED_NUM_ATTRS_SIZE 8
#define H5_DAOS_ENCODED_NUM_LINKS_SIZE 8
#define H5_DAOS_ENCODED_RC_SIZE        8

/* Size of encoded OID */
#define H5_DAOS_ENCODED_OID_SIZE 16

/* Generic encoded uint64 size */
#define H5_DAOS_ENCODED_UINT64_T_SIZE 8

/* Size of buffer for writing link creation order info */
#define H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1)

/* Definitions for building oids */
#define H5_DAOS_TYPE_MASK  0x00000000c0000000ull
#define H5_DAOS_TYPE_GRP   0x0000000000000000ull
#define H5_DAOS_TYPE_DSET  0x0000000040000000ull
#define H5_DAOS_TYPE_DTYPE 0x0000000080000000ull
#define H5_DAOS_TYPE_MAP   0x00000000c0000000ull

/* Predefined object indices */
#define H5_DAOS_OIDX_GMD        0ull
#define H5_DAOS_OIDX_ROOT       1ull
#define H5_DAOS_OIDX_FIRST_USER 2ull

/* Bits of oid.lo and oid.hi that are added to compacted addresses */
#define H5_DAOS_ADDR_OIDLO_MASK 0x000000003fffffffll
#define H5_DAOS_ADDR_OIDHI_MASK 0xffffffffc0000000ll

/* Number of object indices to allocate at a time */
#define H5_DAOS_OIDX_NALLOC 1024

/* Polling interval (in milliseconds) when waiting for asynchronous tasks to
 * finish */
#define H5_DAOS_ASYNC_POLL_INTERVAL 1

/* Predefined timeouts for different modes in which to make progress using
 * H5_daos_progress */
#define H5_DAOS_PROGRESS_KICK (uint64_t)0
#define H5_DAOS_PROGRESS_WAIT UINT64_MAX

/* Defines for connecting to pools */
#ifndef DAOS_DEFAULT_GROUP_ID
#define DAOS_DEFAULT_GROUP_ID "daos_server"
#endif
#define H5_DAOS_MAX_SVC_REPLICAS 13

/* Remove warnings when connector does not use callback arguments */
#if defined(__cplusplus)
#define H5VL_DAOS_UNUSED
#elif defined(__GNUC__) && (__GNUC__ >= 4)
#define H5VL_DAOS_UNUSED __attribute__((unused))
#else
#define H5VL_DAOS_UNUSED
#endif

/* Remove warnings when arguments passed to a callback by way of va_arg are not used. */
#define H5_DAOS_UNUSED_VAR(arg) (void)arg;

/* Min/max macros */
#ifndef MAX
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
#endif
#ifndef MIN
#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif

/* Macros borrowed from H5Fprivate.h */
#define UINT64ENCODE(p, n)                                                                                   \
    {                                                                                                        \
        uint64_t _n = (n);                                                                                   \
        size_t   _i;                                                                                         \
        uint8_t *_p = (uint8_t *)(p);                                                                        \
                                                                                                             \
        for (_i = 0; _i < sizeof(uint64_t); _i++, _n >>= 8)                                                  \
            *_p++ = (uint8_t)(_n & 0xff);                                                                    \
        for (/*void*/; _i < 8; _i++)                                                                         \
            *_p++ = 0;                                                                                       \
        (p) = (uint8_t *)(p) + 8;                                                                            \
    }

#define UINT64DECODE(p, n)                                                                                   \
    {                                                                                                        \
        /* WE DON'T CHECK FOR OVERFLOW! */                                                                   \
        size_t _i;                                                                                           \
                                                                                                             \
        n = 0;                                                                                               \
        (p) += 8;                                                                                            \
        for (_i = 0; _i < sizeof(uint64_t); _i++)                                                            \
            n = (n << 8) | *(--p);                                                                           \
        (p) += 8;                                                                                            \
    }

/* Decode a variable-sized buffer */
/* (Assumes that the high bits of the integer will be zero) */
#define DECODE_VAR(p, n, l)                                                                                  \
    {                                                                                                        \
        size_t _i;                                                                                           \
                                                                                                             \
        n = 0;                                                                                               \
        (p) += l;                                                                                            \
        for (_i = 0; _i < l; _i++)                                                                           \
            n = (n << 8) | *(--p);                                                                           \
        (p) += l;                                                                                            \
    }

/* Decode a variable-sized buffer into a 64-bit unsigned integer */
/* (Assumes that the high bits of the integer will be zero) */
#define UINT64DECODE_VAR(p, n, l) DECODE_VAR(p, n, l)

/* Compile-time "assert" macro (borrowed from H5private.h) */
#define H5daos_compile_assert(e) ((void)sizeof(char[!!(e) ? 1 : -1]))

/* FAPL property to tell the VOL connector to open a saved snapshot when opening a
 * file */
#ifdef DV_HAVE_SNAP_OPEN_ID
#define H5_DAOS_SNAP_OPEN_ID "daos_snap_open"
#endif

/* Property to specify DAOS property */
#define H5_DAOS_FILE_PROP_NAME "daos_prop_name"

/* Property to specify DAOS object class */
#define H5_DAOS_OBJ_CLASS_NAME "daos_object_class"

/* Property to specify DAOS object class of the root group when opening a file
 */
#define H5_DAOS_ROOT_OPEN_OCLASS_NAME "root_open_daos_oclass"

/* Property to specify independent metadata I/O */
#define H5_DAOS_IND_MD_IO_PROP_NAME "h5daos_independent_md_writes"

/* DSINC - There are serious problems in HDF5 when trying to call
 * H5Pregister2/H5Punregister on the H5P_FILE_ACCESS class.
 */
#undef DV_HAVE_SNAP_OPEN_ID

/* Macro to initialize all non-specific fields of an H5_daos_iter_data_t struct */
#define H5_DAOS_ITER_DATA_INIT(_iter_data, _iter_type, _idx_type, _iter_order, _is_recursive, _idx_p,        \
                               _iter_root_obj, _op_data, _op_ret_p, _req)                                    \
    do {                                                                                                     \
        memset(&_iter_data, 0, sizeof(H5_daos_iter_data_t));                                                 \
        _iter_data.iter_type     = _iter_type;                                                               \
        _iter_data.index_type    = _idx_type;                                                                \
        _iter_data.iter_order    = _iter_order;                                                              \
        _iter_data.is_recursive  = _is_recursive;                                                            \
        _iter_data.idx_p         = _idx_p;                                                                   \
        _iter_data.iter_root_obj = _iter_root_obj;                                                           \
        _iter_data.op_data       = _op_data;                                                                 \
        _iter_data.op_ret        = H5_ITER_CONT;                                                             \
        _iter_data.op_ret_p      = _op_ret_p;                                                                \
        _iter_data.req           = _req;                                                                     \
    } while (0)

/* Temporary macro to wait until an async chain is complete when async
 * code exists inside of synchronous code.
 */
#define H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, first_task, dep_task, err_maj, err_min, ret_value)                  \
    do {                                                                                                     \
        if (H5_daos_task_wait(&(first_task), &(dep_task)) < 0)                                               \
            D_GOTO_ERROR(err_maj, err_min, ret_value, "can't progress scheduler");                           \
        if (req->status < -H5_DAOS_CANCELED)                                                                 \
            D_GOTO_ERROR(err_maj, err_min, ret_value, "asynchronous task failed: %s",                        \
                         H5_daos_err_to_string(req->status));                                                \
    } while (0)

/* Macro to make progress in task engine whenever the
 * connector is entered from a public API call.
 */
#define H5_DAOS_MAKE_ASYNC_PROGRESS(ret_value)                                                               \
    do {                                                                                                     \
        /* Make progress on scheduler */                                                                     \
        if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)                                               \
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, ret_value, "can't progress scheduler");               \
    } while (0)

/* Macro to retrieve the metadata I/O mode setting (collective vs. independent)
 * for metadata reads and metadata writes.
 */
#define H5_DAOS_GET_METADATA_IO_MODES(file, apl_id, default_apl_id, collective_md_read, collective_md_write, \
                                      err_maj, ret_value)                                                    \
    do {                                                                                                     \
        /* Set initial collective metadata read/write status from file FAPL cache */                         \
        collective_md_read  = file->fapl_cache.is_collective_md_read;                                        \
        collective_md_write = file->fapl_cache.is_collective_md_write;                                       \
        if (apl_id != default_apl_id) {                                                                      \
            hbool_t _all_independent_md_io = FALSE;                                                          \
                                                                                                             \
            /* Determine if independent metadata I/O was requested for the operation */                      \
            if (H5daos_get_all_ind_metadata_ops(apl_id, &_all_independent_md_io) < 0)                        \
                D_GOTO_ERROR(err_maj, H5E_CANTGET, ret_value,                                                \
                             "can't get independent metadata I/O property");                                 \
                                                                                                             \
            if (_all_independent_md_io) {                                                                    \
                /* Override all metadata I/O to be independent */                                            \
                collective_md_read  = FALSE;                                                                 \
                collective_md_write = FALSE;                                                                 \
            }                                                                                                \
            else {                                                                                           \
                hbool_t _all_collective_md_io = FALSE;                                                       \
                                                                                                             \
                /* If all collective metadata I/O has not already been set by the file,                      \
                 * determine if collective metadata I/O was requested for the operation */                   \
                if ((!collective_md_read || !collective_md_write) &&                                         \
                    H5Pget_all_coll_metadata_ops(apl_id, &_all_collective_md_io) < 0)                        \
                    D_GOTO_ERROR(err_maj, H5E_CANTGET, ret_value,                                            \
                                 "can't get collective metadata I/O property");                              \
                                                                                                             \
                if (_all_collective_md_io) {                                                                 \
                    /* Override all metadata I/O to be collective */                                         \
                    collective_md_read  = TRUE;                                                              \
                    collective_md_write = TRUE;                                                              \
                }                                                                                            \
            }                                                                                                \
        }                                                                                                    \
    } while (0)

/* Macro to retrieve the metadata I/O mode setting just for metadata reads. */
#define H5_DAOS_GET_METADATA_READ_MODE(file, apl_id, default_apl_id, collective, err_maj, ret_value)         \
    do {                                                                                                     \
        hbool_t _is_collective_md_write = TRUE;                                                              \
        H5_DAOS_GET_METADATA_IO_MODES(file, apl_id, default_apl_id, collective, _is_collective_md_write,     \
                                      err_maj, ret_value);                                                   \
    } while (0)

/* Macro to retrieve the metadata I/O mode setting just for metadata writes. */
#define H5_DAOS_GET_METADATA_WRITE_MODE(file, apl_id, default_apl_id, collective, err_maj, ret_value)        \
    do {                                                                                                     \
        hbool_t _is_collective_md_read = FALSE;                                                              \
        H5_DAOS_GET_METADATA_IO_MODES(file, apl_id, default_apl_id, _is_collective_md_read, collective,      \
                                      err_maj, ret_value);                                                   \
    } while (0)

/* Macro to use at the start of a prep callback or a task function with no prep
 * callback to check for error/cancel/short-circuit in the request and prereq
 * request and to mark the request as in-progress */
#define H5_DAOS_PREP_REQ(req, err_maj)                                                                       \
    do {                                                                                                     \
        if ((req)->status < -H5_DAOS_CANCELED) {                                                             \
            assert((req)->status != -H5_DAOS_PRE_ERROR);                                                     \
            D_GOTO_DONE(-H5_DAOS_PRE_ERROR);                                                                 \
        } /* end if */                                                                                       \
        else if ((req)->status < -H5_DAOS_INCOMPLETE)                                                        \
            D_GOTO_DONE((req)->status);                                                                      \
        else if (!(req)->in_progress) {                                                                      \
            (req)->in_progress = TRUE;                                                                       \
            if ((req)->prereq_req1) {                                                                        \
                if ((req)->prereq_req1->status == -H5_DAOS_CANCELED) {                                       \
                    (req)->status = -H5_DAOS_CANCELED;                                                       \
                    D_GOTO_DONE(-H5_DAOS_CANCELED);                                                          \
                } /* end if */                                                                               \
                else if ((req)->prereq_req1->status != 0) {                                                  \
                    assert((req)->prereq_req1->status < -H5_DAOS_PRE_ERROR);                                 \
                    D_GOTO_ERROR(err_maj, H5E_BADVALUE, -H5_DAOS_PREREQ_ERROR,                               \
                                 "prerequisite operation failed");                                           \
                } /* end if */                                                                               \
                if ((req)->prereq_req2) {                                                                    \
                    if ((req)->prereq_req2->status == -H5_DAOS_CANCELED) {                                   \
                        (req)->status = -H5_DAOS_CANCELED;                                                   \
                        D_GOTO_DONE(-H5_DAOS_CANCELED);                                                      \
                    } /* end if */                                                                           \
                    else if ((req)->prereq_req2->status != 0) {                                              \
                        assert((req)->prereq_req2->status < -H5_DAOS_PRE_ERROR);                             \
                        D_GOTO_ERROR(err_maj, H5E_BADVALUE, -H5_DAOS_PREREQ_ERROR,                           \
                                     "second prerequisite operation failed");                                \
                    } /* end if */                                                                           \
                }     /* end if */                                                                           \
            }         /* end if */                                                                           \
            else                                                                                             \
                assert(!(req)->prereq_req2);                                                                 \
        } /* end if */                                                                                       \
    } while (0)

/* Like H5_DAOS_PREP_REQ but asserts req is in progress (for when you know this
 * is not the first task in the request) */
#define H5_DAOS_PREP_REQ_PROG(req)                                                                           \
    do {                                                                                                     \
        if ((req)->status < -H5_DAOS_CANCELED) {                                                             \
            assert((req)->status != -H5_DAOS_PRE_ERROR);                                                     \
            D_GOTO_DONE(-H5_DAOS_PRE_ERROR);                                                                 \
        } /* end if */                                                                                       \
        else if ((req)->status < -H5_DAOS_INCOMPLETE)                                                        \
            D_GOTO_DONE((req)->status);                                                                      \
        assert((req)->in_progress);                                                                          \
    } while (0)

/* Like H5_DAOS_PREP_REQ_PROG but does not go to done */
#define H5_DAOS_PREP_REQ_DONE(req)                                                                           \
    do {                                                                                                     \
        if ((req)->status < -H5_DAOS_CANCELED) {                                                             \
            assert((req)->status != -H5_DAOS_PRE_ERROR);                                                     \
            ret_value = -H5_DAOS_PRE_ERROR;                                                                  \
        } /* end if */                                                                                       \
        else if ((req)->status < -H5_DAOS_INCOMPLETE)                                                        \
            ret_value = (req)->status;                                                                       \
        assert((req)->in_progress);                                                                          \
    } while (0)

#if defined(DAOS_API_VERSION_MAJOR) && defined(DAOS_API_VERSION_MINOR)
#define CHECK_DAOS_API_VERSION(major, minor)                                                                 \
    ((DAOS_API_VERSION_MAJOR > (major)) ||                                                                   \
     (DAOS_API_VERSION_MAJOR == (major) && DAOS_API_VERSION_MINOR >= (minor)))
#else
#define CHECK_DAOS_API_VERSION(major, minor) 0
#endif

/********************/
/* Private Typedefs */
/********************/

/* DAOS-specific file/pool access parameters */
typedef struct H5_daos_acc_params_t {
    char pool[DAOS_PROP_LABEL_MAX_LEN + 1];
    char sys[DAOS_SYS_NAME_MAX + 1];
} H5_daos_acc_params_t;

/* Forward declaration of operation pool struct */
typedef struct H5_daos_op_pool_t H5_daos_op_pool_t;

/* Common object and attribute information */
typedef struct H5_daos_item_t {
    H5I_type_t             type;
    hbool_t                created;
    struct H5_daos_req_t  *open_req;
    H5_daos_op_pool_t     *cur_op_pool;
    struct H5_daos_file_t *file;
    hbool_t                nonblocking_close;
    int                    rc;
} H5_daos_item_t;

/* The OCPL cache struct */
typedef struct H5_daos_ocpl_cache_t {
    hbool_t track_acorder;
} H5_daos_ocpl_cache_t;

/* Common object information */
typedef struct H5_daos_obj_t {
    H5_daos_item_t       item; /* Must be first */
    daos_obj_id_t        oid;
    daos_handle_t        obj_oh;
    H5_daos_ocpl_cache_t ocpl_cache;
} H5_daos_obj_t;

/* The FAPL cache struct */
typedef struct H5_daos_fapl_cache_t {
    daos_oclass_id_t default_object_class;
    hbool_t          is_collective_md_read;
    hbool_t          is_collective_md_write;
} H5_daos_fapl_cache_t;

/* Structure for caching the default values
 * for various properties in HDF5's default
 * property lists */
typedef struct H5_daos_plist_cache_t {
    struct {
        unsigned link_corder_flags;
        unsigned acorder_flags;
    } gcpl_cache;

    struct {
        H5D_layout_t     layout;
        H5D_fill_value_t fill_status;
        H5D_fill_time_t  fill_time;
        unsigned         acorder_flags;
    } dcpl_cache;

    struct {
        unsigned acorder_flags;
    } tcpl_cache;

    struct {
        unsigned acorder_flags;
    } mcpl_cache;

    struct {
        size_t dkey_prefetch_size;
        size_t dkey_alloc_size;
    } mapl_cache;

    struct {
        unsigned crt_intermed_grp;
    } lcpl_cache;

    struct {
        unsigned obj_copy_options;
    } ocpypl_cache;
} H5_daos_plist_cache_t;

/* Encoded default property list buffer cache */
typedef struct H5_daos_enc_plist_cache_t {
    size_t buffer_size;
    void  *plist_buffer;
    size_t fcpl_size;
    void  *fcpl_buf;
    size_t dcpl_size;
    void  *dcpl_buf;
    size_t gcpl_size;
    void  *gcpl_buf;
    size_t tcpl_size;
    void  *tcpl_buf;
    size_t mcpl_size;
    void  *mcpl_buf;
    size_t acpl_size;
    void  *acpl_buf;
} H5_daos_enc_plist_cache_t;

/* The file struct */
typedef struct H5_daos_file_t {
    H5_daos_item_t            item; /* Must be first */
    daos_handle_t             coh;
    daos_handle_t             container_poh;
    daos_prop_t              *create_prop;
    daos_prop_t              *cont_prop;
    char                     *file_name;
    char                      cont[DAOS_PROP_LABEL_MAX_LEN + 1];
    H5_daos_acc_params_t      facc_params;
    unsigned                  flags;
    daos_handle_t             glob_md_oh;
    daos_obj_id_t             glob_md_oid;
    struct H5_daos_group_t   *root_grp;
    hid_t                     fapl_id;
    hid_t                     fcpl_id;
    H5_daos_fapl_cache_t      fapl_cache;
    H5_daos_enc_plist_cache_t def_plist_cache;
    MPI_Comm                  comm;
    MPI_Info                  info;
    int                       my_rank;
    int                       num_procs;
    uint64_t                  next_oidx;
    uint64_t                  max_oidx;
    uint64_t                  next_oidx_collective;
    uint64_t                  max_oidx_collective;
} H5_daos_file_t;

/* The GCPL cache struct */
typedef struct H5_daos_gcpl_cache_t {
    hbool_t track_corder;
} H5_daos_gcpl_cache_t;

/* The group struct */
typedef struct H5_daos_group_t {
    H5_daos_obj_t        obj; /* Must be first */
    hid_t                gcpl_id;
    hid_t                gapl_id;
    H5_daos_gcpl_cache_t gcpl_cache;
} H5_daos_group_t;

/* Different algorithms for handling fill values on dataset reads */
typedef enum {
    /* Do not touch the user's buffer for unwritten elements */
    H5_DAOS_NO_FILL,
    /* Fill the conversion/read buffer with zeros prior to read */
    H5_DAOS_ZERO_FILL,
    /* Copy the fill value to each element in the conversion/read buffer prior
     * to read */
    H5_DAOS_COPY_FILL
} H5_daos_fill_method_t;

/* The DCPL cache struct */
typedef struct H5_daos_dcpl_cache_t {
    H5D_layout_t          layout;
    hsize_t               chunk_dims[H5S_MAX_RANK];
    H5D_fill_value_t      fill_status;
    H5_daos_fill_method_t fill_method;
} H5_daos_dcpl_cache_t;

/* Information about a singular selected chunk during a dataset read/write */
typedef struct H5_daos_select_chunk_info_t {
    uint64_t chunk_coords[H5S_MAX_RANK]; /* The starting coordinates ("upper left corner") of the chunk */
    hssize_t num_elem_sel_file;          /* Number of elements selected in chunk's file dataspace */
    hid_t    mspace_id;                  /* The memory space corresponding to the
                                            selection in the chunk in memory */
    hid_t fspace_id;                     /* The file space corresponding to the
                                            selection in the chunk in the file */
} H5_daos_select_chunk_info_t;

/* The dataset struct */
typedef struct H5_daos_dset_t {
    H5_daos_obj_t        obj; /* Must be first */
    size_t               file_type_size;
    hid_t                type_id;
    hid_t                file_type_id;
    hid_t                space_id;
    hid_t                cur_set_extent_space_id;
    hid_t                dcpl_id;
    hid_t                dapl_id;
    H5_daos_dcpl_cache_t dcpl_cache;
    void                *fill_val;
    struct {
        hbool_t                      filled;
        H5_daos_select_chunk_info_t  single_chunk_info;
        H5_daos_select_chunk_info_t *chunk_info;
        size_t                       chunk_info_nalloc;
        hid_t                        mem_sel_iter_id;
        hid_t                        file_sel_iter_id;
    } io_cache;
} H5_daos_dset_t;

/* The datatype struct */
/* Note we could speed things up a bit by caching the serialized datatype.  We
 * may also not need to keep the type_id around.  -NAF */
typedef struct H5_daos_dtype_t {
    H5_daos_obj_t obj; /* Must be first */
    hid_t         type_id;
    hid_t         tcpl_id;
    hid_t         tapl_id;
} H5_daos_dtype_t;

/* The map struct */
typedef struct H5_daos_map_t {
    H5_daos_obj_t obj; /* Must be first */
    size_t        key_file_type_size;
    size_t        val_file_type_size;
    hid_t         key_type_id;
    hid_t         key_file_type_id;
    hid_t         val_type_id;
    hid_t         val_file_type_id;
    hid_t         mcpl_id;
    hid_t         mapl_id;
} H5_daos_map_t;

/* The attribute struct */
typedef struct H5_daos_attr_t {
    H5_daos_item_t item; /* Must be first */
    H5_daos_obj_t *parent;
    size_t         file_type_size;
    char          *name;
    hid_t          type_id;
    hid_t          file_type_id;
    hid_t          space_id;
    hid_t          acpl_id;
} H5_daos_attr_t;

/* The link value struct */
typedef struct H5_daos_link_val_t {
    H5L_type_t     type;
    daos_obj_id_t *target_oid_async;
    union {
        daos_obj_id_t hard;
        char         *soft;
    } target;
} H5_daos_link_val_t;

/* Enum to indicate if the supplied read buffer can be used as a type conversion
 * or background buffer */
typedef enum {
    H5_DAOS_TCONV_REUSE_NONE,  /* Cannot reuse buffer */
    H5_DAOS_TCONV_REUSE_TCONV, /* Use buffer as type conversion buffer */
    H5_DAOS_TCONV_REUSE_BKG    /* Use buffer as background buffer */
} H5_daos_tconv_reuse_t;

/* Enum type for distinguishing between I/O reads and writes. */
typedef enum H5_daos_io_type_t { IO_READ, IO_WRITE } H5_daos_io_type_t;

/* Forward declaration for generic request struct */
typedef struct H5_daos_req_t H5_daos_req_t;

/* Task user data for asynchronous MPI broadcast */
typedef struct H5_daos_mpi_ibcast_ud_t {
    H5_daos_req_t *req;
    H5_daos_obj_t *obj;
    tse_task_t    *bcast_metatask;
    void          *buffer;
    int            buffer_len;
    int            count;
    MPI_Comm       comm;
} H5_daos_mpi_ibcast_ud_t;

/* Task user data for asynchronous MPI broadcast (with flexible array member) */
typedef struct H5_daos_mpi_ibcast_ud_flex_t {
    H5_daos_mpi_ibcast_ud_t bcast_udata; /* Must be first */
    uint8_t                 flex_buf[];
} H5_daos_mpi_ibcast_ud_flex_t;

/* Generic request struct */
struct H5_daos_req_t {
    daos_handle_t         th;
    hbool_t               th_open;
    H5_daos_file_t       *file;
    hid_t                 dxpl_id;
    tse_task_t           *finalize_task;
    tse_task_t           *dep_task;
    H5_daos_req_t        *prereq_req1;
    H5_daos_req_t        *prereq_req2;
    H5_daos_req_t        *parent_req;
    H5VL_request_notify_t notify_cb;
    void                 *notify_ctx;
    int                   rc;
    int                   status;
    const char           *failed_task;
    const char           *op_name;
    hbool_t               in_progress;
    struct {
        H5_daos_mpi_ibcast_ud_t err_check_ud;
        int                     coll_status;
    } collective;
};

/* Different types of operation pools - can be either for read ops, for write
 * ops, order-enforced versions of these, close ops, or empty.  Order is
 * important, when combining ops the pool is upgraded to the highest value
 * (closest to EMPTY) */
typedef enum H5_daos_op_pool_type_t {
    H5_DAOS_OP_TYPE_READ,
    H5_DAOS_OP_TYPE_WRITE,
    H5_DAOS_OP_TYPE_READ_ORDERED,
    H5_DAOS_OP_TYPE_WRITE_ORDERED,
    H5_DAOS_OP_TYPE_CLOSE,
    H5_DAOS_OP_TYPE_EMPTY,
    H5_DAOS_OP_TYPE_NOPOOL,
} H5_daos_op_pool_type_t;

/* Different scopes for operation pools - can be either specific to an
 * attribute, an object, a file, or global */
typedef enum H5_daos_op_pool_scope_t {
    H5_DAOS_OP_SCOPE_ATTR,
    H5_DAOS_OP_SCOPE_OBJ,
    H5_DAOS_OP_SCOPE_FILE,
    H5_DAOS_OP_SCOPE_GLOB,
} H5_daos_op_pool_scope_t;

/* Struct for an operation pool */
struct H5_daos_op_pool_t {
    H5_daos_op_pool_type_t type;
    tse_task_t            *start_task;
    tse_task_t            *end_task;
    tse_task_t            *dep_task;
    uint64_t               op_gens[4];
    int                    rc;
};

/* Task user data for generic operations that need no special handling (only for
 * error tracking) */
typedef struct H5_daos_generic_cb_ud_t {
    H5_daos_req_t *req;
    const char    *task_name;
} H5_daos_generic_cb_ud_t;

/* Task user data object close operations */
typedef struct H5_daos_obj_close_task_ud_t {
    H5_daos_req_t  *req;
    H5_daos_item_t *item;
} H5_daos_obj_close_task_ud_t;

/* Task user data for generic metadata I/O */
typedef struct H5_daos_md_rw_cb_ud_t {
    H5_daos_req_t *req;
    H5_daos_obj_t *obj;
    daos_key_t     dkey;
    unsigned       nr;
    uint64_t       flags;
    daos_iod_t     iod[7];
    daos_sg_list_t sgl[7];
    daos_iov_t     sg_iov[7];
    hbool_t        free_dkey;
    hbool_t        free_akeys;
    hbool_t        free_sg_iov[7];
    const char    *task_name;
} H5_daos_md_rw_cb_ud_t;

/* Task user data for generic metadata I/O (with flexible array member) */
typedef struct H5_daos_md_rw_cb_ud_flex_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud; /* Must be first */
    uint8_t               flex_buf[];
} H5_daos_md_rw_cb_ud_flex_t;

/* Task user data for object metadata fetch */
typedef struct H5_daos_omd_fetch_ud_t {
    H5_daos_md_rw_cb_ud_t         md_rw_cb_ud; /* Must be first */
    H5_daos_mpi_ibcast_ud_flex_t *bcast_udata;
    tse_task_t                   *fetch_metatask;
    uint8_t                       flex_buf[];
} H5_daos_omd_fetch_ud_t;

/* Task user data for OIDX generation */
typedef struct H5_daos_oidx_generate_ud_t {
    H5_daos_generic_cb_ud_t generic_ud; /* Must be first */
    H5_daos_file_t         *file;
    hbool_t                 collective;
    uint64_t               *oidx_out;
    uint64_t               *next_oidx;
    uint64_t               *max_oidx;
} H5_daos_oidx_generate_ud_t;

/* Task user data for broadcasting the next OIDX
 * after allocating more from DAOS.
 */
typedef struct H5_daos_oidx_bcast_ud_t {
    H5_daos_mpi_ibcast_ud_t bcast_udata; /* Must be first */
    H5_daos_file_t         *file;
    uint8_t                 next_oidx_buf[H5_DAOS_ENCODED_UINT64_T_SIZE];
    uint64_t               *oidx_out;
    uint64_t               *next_oidx;
    uint64_t               *max_oidx;
} H5_daos_oidx_bcast_ud_t;

/* Task user data for OID encoding */
typedef struct H5_daos_oid_encode_ud_t {
    H5_daos_req_t  *req;
    H5_daos_file_t *file;
    daos_obj_id_t  *oid_out;
    uint64_t        oidx;
    H5I_type_t      obj_type;
    hid_t           crt_plist_id;
    const char     *oclass_prop_name;
} H5_daos_oid_encode_ud_t;

/*
 * Enum values for determining the type of iteration
 * being done with a given H5_daos_iter_data_t.
 */
typedef enum {
    H5_DAOS_ITER_TYPE_ATTR,
    H5_DAOS_ITER_TYPE_LINK,
    H5_DAOS_ITER_TYPE_MAP,
    H5_DAOS_ITER_TYPE_OBJ,
} H5_daos_iter_data_type_t;

/* Function type for asynchronous attribute iterate callbacks */
typedef herr_t (*H5_daos_attribute_iterate_async_t)(hid_t obj, const char *name, const H5A_info_t *info,
                                                    void *op_data, herr_t *op_ret, tse_task_t **first_task,
                                                    tse_task_t **dep_task);

/* Function type for asynchronous link iterate callbacks */
/* Could have an option to disable name and/or linfo here for performance -NAF */
typedef herr_t (*H5_daos_link_iterate_async_t)(hid_t group, const char *name, const H5L_info2_t *info,
                                               void *op_data, herr_t *op_ret, tse_task_t **first_task,
                                               tse_task_t **dep_task);

/* Function type for asynchronous map iterate callbacks */
typedef herr_t (*H5_daos_map_iterate_async_t)(hid_t map, const void *key, void *op_data, herr_t *op_ret,
                                              tse_task_t **first_task, tse_task_t **dep_task);

/* Function type for asynchronous object visit callbacks */
typedef herr_t (*H5_daos_object_visit_async_t)(hid_t obj, const char *name, const H5O_info2_t *info,
                                               void *op_data, herr_t *op_ret, tse_task_t **first_task,
                                               tse_task_t **dep_task);

/*
 * A struct which is filled out and used when performing
 * link, attribute and object iteration/visiting.
 */
typedef struct H5_daos_iter_data_t {
    H5_iter_order_t iter_order;
    H5_index_t      index_type;
    hbool_t         is_recursive;
    hbool_t         async_op;
    hsize_t        *idx_p;
    hid_t           iter_root_obj;
    void           *op_data;
    herr_t          op_ret;
    herr_t         *op_ret_p;

    hbool_t        short_circuit_init;
    H5_daos_req_t *req;

    H5_daos_iter_data_type_t iter_type;
    union {
        struct {
            union {
                H5A_operator2_t                   attr_iter_op;
                H5_daos_attribute_iterate_async_t attr_iter_op_async;
            } u;
        } attr_iter_data;

        struct {
            union {
                H5L_iterate2_t               link_iter_op;
                H5_daos_link_iterate_async_t link_iter_op_async;
            } u;
            dv_hash_table_t *visited_link_table;
            char            *recursive_link_path;
            size_t           recursive_link_path_nalloc;
            unsigned         recurse_depth; /* TODO: remove this from this struct */
        } link_iter_data;

        struct {
            union {
                H5M_iterate_t               map_iter_op;
                H5_daos_map_iterate_async_t map_iter_op_async;
            } u;
            hid_t key_mem_type_id;
        } map_iter_data;

        struct {
            union {
                H5O_iterate2_t               obj_iter_op;
                H5_daos_object_visit_async_t obj_iter_op_async;
            } u;
            unsigned    fields;
            const char *obj_name;
        } obj_iter_data;
    } u;
} H5_daos_iter_data_t;

/* A struct to track async iteration at a single level of recursion */
typedef struct H5_daos_iter_ud_t {
    H5_daos_iter_data_t *iter_data;
    H5_daos_obj_t       *target_obj;
    uint32_t             nr;
    daos_key_t           dkey;
    daos_key_desc_t     *kds;
    daos_key_desc_t      kds_static[H5_DAOS_ITER_LEN];
    daos_key_desc_t     *kds_dyn;
    size_t               kds_len;
    daos_sg_list_t       sgl;
    daos_iov_t           sg_iov;
    daos_anchor_t        anchor;
    hbool_t              base_iter;
    tse_task_t          *iter_metatask;
} H5_daos_iter_ud_t;

/* A union to contain either an hvl_t or a char *, for vlen conversions that
 * need to be handled explicitly (like for map keys) */
typedef union {
    hvl_t vl;
    char *vls;
} H5_daos_vl_union_t;

/** iovec for const memory buffer */
typedef struct {
    /** buffer address */
    const void *iov_buf;
    /** buffer length */
    size_t iov_buf_len;
    /** data length */
    size_t iov_len;
} d_const_iov_t;

/*********************/
/* Private Variables */
/*********************/

extern H5VL_DAOS_PRIVATE hid_t H5_DAOS_g;

/* Free list definitions */
/* DSINC - currently no external access to free lists
H5FL_DEFINE_EXTERN(H5_daos_file_t);
H5FL_DEFINE_EXTERN(H5_daos_group_t);
H5FL_DEFINE_EXTERN(H5_daos_dset_t);
H5FL_DEFINE_EXTERN(H5_daos_dtype_t);
H5FL_DEFINE_EXTERN(H5_daos_map_t);
H5FL_DEFINE_EXTERN(H5_daos_attr_t);*/

/* DSINC - Until we determine what to do with free lists,
 * these macros should at least keep the allocations working
 * correctly.
 */
#define H5FL_CALLOC(t)  DV_calloc(sizeof(t))
#define H5FL_FREE(t, o) DV_free(o)

#ifdef DV_TRACK_MEM_USAGE
/*
 * Counter to keep track of the currently allocated amount of bytes
 */
extern size_t daos_vol_curr_alloc_bytes;
#endif

/* Global variable used for bypassing the DUNS when requested. */
extern H5VL_DAOS_PRIVATE hbool_t H5_daos_bypass_duns_g;

/* Target chunk size for automatic chunking */
extern H5VL_DAOS_PRIVATE uint64_t H5_daos_chunk_target_size_g;

/* Global scheduler - used for tasks that are not tied to any open file */
extern tse_sched_t H5_daos_glob_sched_g;

/* Global ooperation pool - used for operations that are not tied to a single
 * file */
extern H5_daos_op_pool_t *H5_daos_glob_cur_op_pool_g;

/* Global variable for HDF5 property list cache */
extern H5VL_DAOS_PRIVATE H5_daos_plist_cache_t *H5_daos_plist_cache_g;

/* Global variable for DAOS task list */
extern H5VL_DAOS_PRIVATE H5_daos_task_list_t *H5_daos_task_list_g;

/* DAOS task and MPI request for current in-flight MPI operation.  Only allow
 * one at a time for now since:
 * - All MPI operations must be in the same order across all ranks, therefore
 *   we cannot start MPI operations in an HDF5 operation until all MPI
 *   operations in previous HDF5 operations are complete
 * - All individual HDF5 operations can only process MPI operations one at a
 *   time */
extern tse_task_t *H5_daos_mpi_task_g;
extern MPI_Request H5_daos_mpi_req_g;

/* Last collective request scheduled.  As described above, only one collective
 * operation can be in flight at any one time. */
extern struct H5_daos_req_t *H5_daos_collective_req_tail;

/* Counter to keep track of the level of recursion with
 * regards to top-level connector callback routines.
 * It should be incremented upon entering any top-level
 * connector callback routine (marked by the presence
 * of D_FUNC_LEAVE_API) and decremented upon leaving
 * that routine. */
extern int H5_daos_api_count;

/* Constant Keys */
extern H5VL_DAOS_PRIVATE const char H5_daos_int_md_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_root_grp_oid_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_rc_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_cpl_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_link_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_link_corder_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_nlinks_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_max_link_corder_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_type_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_space_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_attr_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_nattr_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_max_attr_corder_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_ktype_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_vtype_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_map_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_blob_key_g[];
extern H5VL_DAOS_PRIVATE const char H5_daos_fillval_key_g[];

extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_int_md_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_root_grp_oid_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_cpl_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_rc_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_link_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_link_corder_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_nlinks_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_max_link_corder_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_type_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_space_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_attr_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_nattr_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_max_attr_corder_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_ktype_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_vtype_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_map_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_blob_key_size_g;
extern H5VL_DAOS_PRIVATE const daos_size_t H5_daos_fillval_key_size_g;

/**********************/
/* Private Prototypes */
/**********************/

#ifdef __cplusplus
extern "C" {
#endif

/* General routines */
H5VL_DAOS_PRIVATE herr_t     H5_daos_pool_connect(H5_daos_acc_params_t *pool_acc_params, unsigned int flags,
                                                  daos_handle_t *poh_out, daos_pool_info_t *pool_info_out,
                                                  H5_daos_req_t *req, tse_task_t **first_task,
                                                  tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t     H5_daos_pool_disconnect(daos_handle_t *poh, H5_daos_req_t *req,
                                                     tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t     H5_daos_set_oclass_from_oid(hid_t plist_id, daos_obj_id_t oid);
H5VL_DAOS_PRIVATE herr_t     H5_daos_oidx_generate(uint64_t *oidx, H5_daos_file_t *file, hbool_t collective,
                                                   H5_daos_req_t *req, tse_task_t **first_task,
                                                   tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t     H5_daos_oid_encode(daos_obj_id_t *oid, uint64_t oidx, H5I_type_t obj_type,
                                                hid_t crt_plist_id, const char *oclass_prop_name,
                                                H5_daos_file_t *file);
H5VL_DAOS_PRIVATE herr_t     H5_daos_oid_generate(daos_obj_id_t *oid, hbool_t oidx_set, uint64_t oidx,
                                                  H5I_type_t obj_type, hid_t crt_plist_id,
                                                  const char *oclass_prop_name, H5_daos_file_t *file,
                                                  hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
                                                  tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t     H5_daos_oid_to_token(daos_obj_id_t oid, H5O_token_t *obj_token);
H5VL_DAOS_PRIVATE herr_t     H5_daos_token_to_oid(const H5O_token_t *obj_token, daos_obj_id_t *oid);
H5VL_DAOS_PRIVATE H5I_type_t H5_daos_oid_to_type(daos_obj_id_t oid);
H5VL_DAOS_PRIVATE void       H5_daos_hash128(const char *name, void *hash);
H5VL_DAOS_PRIVATE herr_t     H5_daos_obj_open(H5_daos_file_t *file, H5_daos_req_t *req, daos_obj_id_t *oid,
                                              unsigned mode, daos_handle_t *oh, const char *task_name,
                                              tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t     H5_daos_free_async(void *buf, tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_get_mpi_info(hid_t fapl_id, MPI_Comm *comm, MPI_Info *info, int *mpi_rank,
                                              int *mpi_size);
H5VL_DAOS_PRIVATE herr_t H5_daos_comm_info_get(hid_t fapl_id, MPI_Comm *comm, MPI_Info *info);
H5VL_DAOS_PRIVATE herr_t H5_daos_comm_info_free(MPI_Comm *comm, MPI_Info *info);

/* File callbacks */
H5VL_DAOS_PRIVATE void  *H5_daos_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                                             hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE void  *H5_daos_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id,
                                           void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_file_get(void *_item, H5VL_file_get_args_t *get_args, hid_t dxpl_id,
                                          void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_file_specific(void *item, H5VL_file_specific_args_t *specific_args,
                                               hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_file_close(void *_file, hid_t dxpl_id, void **req);

/* Other file routines */
H5VL_DAOS_PRIVATE herr_t H5_daos_file_flush(H5_daos_file_t *file, H5_daos_req_t *req, tse_task_t **first_task,
                                            tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_file_close_helper(H5_daos_file_t *file);

/* Link callbacks */
H5VL_DAOS_PRIVATE herr_t H5_daos_link_create(H5VL_link_create_args_t *create_args, void *_item,
                                             const H5VL_loc_params_t *loc_params, hid_t lcpl_id,
                                             hid_t lapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                           const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                           hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                           const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                           hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_get(void *_item, const H5VL_loc_params_t *loc_params,
                                          H5VL_link_get_args_t *get_args, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_specific(void *_item, const H5VL_loc_params_t *loc_params,
                                               H5VL_link_specific_args_t *specific_args, hid_t dxpl_id,
                                               void **req);

/* Other link routines */
H5VL_DAOS_PRIVATE int    H5_daos_link_write(H5_daos_group_t *grp, const char *name, size_t name_len,
                                            H5_daos_link_val_t *val, H5_daos_req_t *req, tse_task_t **first_task,
                                            tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_copy_move_int(H5_daos_item_t          *src_item,
                                                    const H5VL_loc_params_t *loc_params1,
                                                    H5_daos_item_t          *dst_item,
                                                    const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
                                                    hbool_t move, hbool_t collective, H5_daos_req_t *req,
                                                    tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_exists(H5_daos_item_t *item, const char *link_path,
#if H5VL_VERSION >= 2
                                             hbool_t ***exists_p, hbool_t *exists,
#else
                                             htri_t ***exists_p, htri_t *exists,
#endif
                                             H5_daos_req_t *req, tse_task_t **first_task,
                                             tse_task_t **dep_task);
H5VL_DAOS_PRIVATE htri_t H5_daos_link_follow(H5_daos_group_t *grp, const char *name, size_t name_len,
                                             hbool_t crt_missing_grp, H5_daos_req_t *req,
                                             daos_obj_id_t ***oid_ptr, hbool_t *link_exists,
                                             tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_iterate(H5_daos_group_t     *target_grp,
                                              H5_daos_iter_data_t *link_iter_data, tse_task_t **first_task,
                                              tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_get_name_by_idx(H5_daos_group_t *target_grp, H5_index_t index_type,
                                                      H5_iter_order_t iter_order, uint64_t idx,
                                                      size_t *link_name_size, char *link_name_out,
                                                      size_t link_name_out_size, H5_daos_req_t *req,
                                                      tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_get_name_by_idx_alloc(H5_daos_group_t *target_grp,
                                                            H5_index_t index_type, H5_iter_order_t iter_order,
                                                            uint64_t idx, const char **link_name,
                                                            size_t *link_name_size, char **link_name_buf,
                                                            size_t *link_name_buf_size, H5_daos_req_t *req,
                                                            tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_link_get_crt_order_by_name(H5_daos_group_t *target_grp,
                                                            const char *link_name, uint64_t *crt_order,
                                                            H5_daos_req_t *req, tse_task_t **first_task,
                                                            tse_task_t **dep_task);

/* Link iterate callbacks */
H5VL_DAOS_PRIVATE herr_t H5_daos_link_iterate_count_links_callback(hid_t group, const char *name,
                                                                   const H5L_info2_t *info, void *op_data);

/* Group callbacks */
H5VL_DAOS_PRIVATE void *H5_daos_group_create(void *_item, const H5VL_loc_params_t *loc_params,
                                             const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
                                             hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE void *H5_daos_group_open(void *_item, const H5VL_loc_params_t *loc_params, const char *name,
                                           hid_t gapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_get(void *_item, H5VL_group_get_args_t *get_args, hid_t dxpl_id,
                                           void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_specific(void *_item, H5VL_group_specific_args_t *specific_args,
                                                hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_close(void *_grp, hid_t dxpl_id, void **req);

/* Other group routines */
H5VL_DAOS_PRIVATE H5_daos_obj_t *H5_daos_group_traverse(H5_daos_item_t *item, const char *path, hid_t lcpl_id,
                                                        H5_daos_req_t *req, hbool_t collective,
                                                        char **path_buf, const char **obj_name,
                                                        size_t *obj_name_len, tse_task_t **first_task,
                                                        tse_task_t **dep_task);
H5VL_DAOS_PRIVATE void *H5_daos_group_create_helper(H5_daos_file_t *file, hbool_t is_root, hid_t gcpl_id,
                                                    hid_t gapl_id, H5_daos_group_t *parent_grp,
                                                    const char *name, size_t name_len, hbool_t collective,
                                                    H5_daos_req_t *req, tse_task_t **first_task,
                                                    tse_task_t **dep_task);
H5VL_DAOS_PRIVATE int   H5_daos_group_open_helper(H5_daos_file_t *file, H5_daos_group_t *grp, hid_t gapl_id,
                                                  hbool_t collective, H5_daos_req_t *req,
                                                  tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE H5_daos_group_t *H5_daos_group_open_int(H5_daos_item_t          *item,
                                                          const H5VL_loc_params_t *loc_params,
                                                          const char *name, hid_t gapl_id, H5_daos_req_t *req,
                                                          hbool_t collective, tse_task_t **first_task,
                                                          tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t           H5_daos_group_get_num_links(H5_daos_group_t *target_grp, hsize_t *nlinks,
                                                               H5_daos_req_t *req, tse_task_t **first_task,
                                                               tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_get_max_crt_order(H5_daos_group_t *target_grp, uint64_t *max_corder,
                                                         H5_daos_req_t *req, tse_task_t **first_task,
                                                         tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_refresh(H5_daos_group_t *grp, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_flush(H5_daos_group_t *grp, H5_daos_req_t *req,
                                             tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_group_close_real(H5_daos_group_t *grp);

/* Dataset callbacks */
H5VL_DAOS_PRIVATE void *H5_daos_dataset_create(void *_item, const H5VL_loc_params_t *loc_params,
                                               const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
                                               hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE void *H5_daos_dataset_open(void *_item, const H5VL_loc_params_t *loc_params,
                                             const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);

#if H5VL_VERSION >= 3
herr_t H5_daos_dataset_read(size_t count, void *_dset[], hid_t mem_type_id[], hid_t mem_space_id[],
                            hid_t file_space_id[], hid_t dxpl_id, void *buf[], void **req);
herr_t H5_daos_dataset_write(size_t count, void *_dset[], hid_t mem_type_id[], hid_t mem_space_id[],
                             hid_t file_space_id[], hid_t dxpl_id, const void *buf[], void **req);
#else
herr_t H5_daos_dataset_read(void *_dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                            hid_t dxpl_id, void *buf, void **req);
herr_t H5_daos_dataset_write(void *_dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                             hid_t dxpl_id, const void *buf, void **req);
#endif

H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_get(void *_dset, H5VL_dataset_get_args_t *get_args, hid_t dxpl_id,
                                             void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_specific(void *_item, H5VL_dataset_specific_args_t *specific_args,
                                                  hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_close(void *_dset, hid_t dxpl_id, void **req);

/* Other dataset routines */
H5VL_DAOS_PRIVATE void *H5_daos_dataset_create_helper(H5_daos_file_t *file, hid_t type_id, hid_t space_id,
                                                      hid_t dcpl_id, hid_t dapl_id,
                                                      H5_daos_group_t *parent_grp, const char *name,
                                                      size_t name_len, hbool_t collective, H5_daos_req_t *req,
                                                      tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE H5_daos_dset_t *H5_daos_dataset_open_helper(H5_daos_file_t *file, hid_t dapl_id,
                                                              hbool_t collective, H5_daos_req_t *req,
                                                              tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_refresh(H5_daos_dset_t *dset, hid_t dxpl_id, H5_daos_req_t *req,
                                                 tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_read_int(H5_daos_dset_t *dset, hid_t mem_type_id, hid_t mem_space_id,
                                                  hid_t file_space_id, htri_t need_tconv, void *buf,
                                                  tse_task_t *_end_task, H5_daos_req_t *req,
                                                  tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_write_int(H5_daos_dset_t *dset, hid_t mem_type_id,
                                                   hid_t mem_space_id, hid_t file_space_id, htri_t need_tconv,
                                                   const void *buf, tse_task_t *_end_task, H5_daos_req_t *req,
                                                   tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_flush(H5_daos_dset_t *dset, H5_daos_req_t *req,
                                               tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_dataset_close_real(H5_daos_dset_t *dset);

/* Datatype callbacks */
H5VL_DAOS_PRIVATE void  *H5_daos_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
                                                 const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                                                 hid_t tapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE void  *H5_daos_datatype_open(void *_item, const H5VL_loc_params_t *loc_params,
                                               const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_datatype_get(void *_dtype, H5VL_datatype_get_args_t *get_args, hid_t dxpl_id,
                                              void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_datatype_specific(void *_item, H5VL_datatype_specific_args_t *specific_args,
                                                   hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_datatype_close(void *_dtype, hid_t dxpl_id, void **req);

/* Other datatype routines */
H5VL_DAOS_PRIVATE void *H5_daos_datatype_commit_helper(H5_daos_file_t *file, hid_t type_id, hid_t tcpl_id,
                                                       hid_t tapl_id, H5_daos_group_t *parent_grp,
                                                       const char *name, size_t name_len, hbool_t collective,
                                                       H5_daos_req_t *req, tse_task_t **first_task,
                                                       tse_task_t **dep_task);
H5VL_DAOS_PRIVATE H5_daos_dtype_t *H5_daos_datatype_open_helper(H5_daos_file_t *file, hid_t tapl_id,
                                                                hbool_t collective, H5_daos_req_t *req,
                                                                tse_task_t **first_task,
                                                                tse_task_t **dep_task);
H5VL_DAOS_PRIVATE htri_t           H5_daos_detect_vl_vlstr_ref(hid_t type_id);
H5VL_DAOS_PRIVATE htri_t           H5_daos_need_tconv(hid_t src_type_id, hid_t dst_type_id);
H5VL_DAOS_PRIVATE herr_t H5_daos_tconv_init(hid_t src_type_id, size_t *src_type_size, hid_t dst_type_id,
                                            size_t *dst_type_size, size_t num_elem, hbool_t clear_tconv_buf,
                                            hbool_t dst_file, void **tconv_buf, void **bkg_buf,
                                            H5_daos_tconv_reuse_t *reuse, hbool_t *fill_bkg);
H5VL_DAOS_PRIVATE herr_t H5_daos_datatype_refresh(H5_daos_dtype_t *dtype, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_datatype_flush(H5_daos_dtype_t *dtype, H5_daos_req_t *req,
                                                tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_datatype_close_real(H5_daos_dtype_t *dtype);

/* Object callbacks */
H5VL_DAOS_PRIVATE void  *H5_daos_object_open(void *_item, const H5VL_loc_params_t *loc_params,
                                             H5I_type_t *opened_type, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_copy(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params,
                                             const char *src_name, void *dst_loc_obj,
                                             const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                                             hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_get(void *_item, const H5VL_loc_params_t *loc_params,
                                            H5VL_object_get_args_t *get_args, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_specific(void *_item, const H5VL_loc_params_t *loc_params,
                                                 H5VL_object_specific_args_t *specific_args, hid_t dxpl_id,
                                                 void **req);

/* Other object routines */
H5VL_DAOS_PRIVATE herr_t H5_daos_object_open_helper(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
                                                    H5I_type_t *opened_type, hbool_t collective,
                                                    H5_daos_obj_t ****ret_obj_p, H5_daos_obj_t **ret_obj,
                                                    H5_daos_req_t *req, tse_task_t **first_task,
                                                    tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_visit(H5_daos_obj_t ***target_obj_prev_out, H5_daos_obj_t *target_obj,
                                              H5_daos_iter_data_t *iter_data, H5_daos_req_t *req,
                                              tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE int    H5_daos_object_close_task(tse_task_t *task);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_close(H5_daos_item_t *item);
H5VL_DAOS_PRIVATE herr_t H5_daos_fill_ocpl_cache(H5_daos_obj_t *obj, hid_t ocpl_id);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj, hsize_t *num_attrs,
                                                      hbool_t post_decrement, H5_daos_req_t *req,
                                                      tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_object_update_num_attrs_key(H5_daos_obj_t *target_obj, hsize_t *new_nattrs,
                                                             tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                                                             H5_daos_req_t *req, tse_task_t **first_task,
                                                             tse_task_t **dep_task);
H5VL_DAOS_PRIVATE int    H5_daos_obj_read_rc(H5_daos_obj_t **obj_p, H5_daos_obj_t *obj, uint64_t *rc,
                                             unsigned *rc_uint, H5_daos_req_t *req, tse_task_t **first_task,
                                             tse_task_t **dep_task);
H5VL_DAOS_PRIVATE int    H5_daos_obj_write_rc(H5_daos_obj_t **obj_p, H5_daos_obj_t *obj, uint64_t *rc,
                                              int64_t adjust, H5_daos_req_t *req, tse_task_t **first_task,
                                              tse_task_t **dep_task);

/* Attribute callbacks */
H5VL_DAOS_PRIVATE void  *H5_daos_attribute_create(void *_obj, const H5VL_loc_params_t *loc_params,
                                                  const char *name, hid_t type_id, hid_t space_id,
                                                  hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE void  *H5_daos_attribute_open(void *_obj, const H5VL_loc_params_t *loc_params,
                                                const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_read(void *_attr, hid_t mem_type_id, void *buf, hid_t dxpl_id,
                                                void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_write(void *_attr, hid_t mem_type_id, const void *buf,
                                                 hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_get(void *_item, H5VL_attr_get_args_t *get_args, hid_t dxpl_id,
                                               void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_specific(void *_item, const H5VL_loc_params_t *loc_params,
                                                    H5VL_attr_specific_args_t *specific_args, hid_t dxpl_id,
                                                    void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_close(void *_attr, hid_t dxpl_id, void **req);

/* Other attribute routines */
H5VL_DAOS_PRIVATE void *H5_daos_attribute_create_helper(H5_daos_item_t          *item,
                                                        const H5VL_loc_params_t *loc_params, hid_t type_id,
                                                        hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                                                        const char *attr_name, hbool_t collective,
                                                        H5_daos_req_t *req, tse_task_t **first_task,
                                                        tse_task_t **dep_task);
H5VL_DAOS_PRIVATE H5_daos_attr_t *
H5_daos_attribute_open_helper(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
                              const char *attr_name, hid_t aapl_id, hbool_t collective, H5_daos_req_t *req,
                              tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_iterate(H5_daos_obj_t       *attr_container_obj,
                                                   H5_daos_iter_data_t *attr_iter_data, H5_daos_req_t *req,
                                                   tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_close_real(H5_daos_attr_t *attr);

/* Attribute iteration callbacks */
H5VL_DAOS_PRIVATE herr_t H5_daos_attribute_iterate_count_attrs_cb(hid_t loc_id, const char *attr_name,
                                                                  const H5A_info_t *attr_info, void *op_data);

/* Map callbacks */
H5VL_DAOS_PRIVATE void *H5_daos_map_create(void *_item, const H5VL_loc_params_t *loc_params, const char *name,
                                           hid_t lcpl_id, hid_t ktype_id, hid_t vtype_id, hid_t mcpl_id,
                                           hid_t mapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE void *H5_daos_map_open(void *_item, const H5VL_loc_params_t *loc_params, const char *name,
                                         hid_t mapl_id, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_map_get_val(void *_map, hid_t key_mem_type_id, const void *key,
                                             hid_t val_mem_type_id, void *value, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_map_exists(void *_map, hid_t key_mem_type_id, const void *key,
                                            hbool_t *exists, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_map_put(void *_map, hid_t key_mem_type_id, const void *key,
                                         hid_t val_mem_type_id, const void *value, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_map_get(void *_map, H5VL_map_args_t *map_args, hid_t dxpl_id, void **req);
H5VL_DAOS_PRIVATE herr_t H5_daos_map_specific(void *_item, H5VL_map_args_t *map_args, hid_t dxpl_id,
                                              void **req);
#ifdef DV_HAVE_MAP
H5PLUGIN_DLL herr_t H5_daos_map_get_types(void *_map, hid_t *key_type_id, hid_t *val_type_id, void **req);
H5PLUGIN_DLL herr_t H5_daos_map_get_count(void *_map, hsize_t *count, void **req);
#endif /* DV_HAVE_MAP */
H5VL_DAOS_PRIVATE herr_t H5_daos_map_close(void *_map, hid_t dxpl_id, void **req);

/* Other map routines */
H5VL_DAOS_PRIVATE H5_daos_map_t *H5_daos_map_open_helper(H5_daos_file_t *file, hid_t mapl_id,
                                                         hbool_t collective, H5_daos_req_t *req,
                                                         tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE H5_daos_map_t *H5_daos_map_open_int(H5_daos_item_t          *item,
                                                      const H5VL_loc_params_t *loc_params, const char *name,
                                                      hid_t mapl_id, H5_daos_req_t *req, hbool_t collective,
                                                      tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t         H5_daos_map_close_real(H5_daos_map_t *map);
H5VL_DAOS_PRIVATE herr_t H5_daos_map_flush(H5_daos_map_t *map, H5_daos_req_t *req, tse_task_t **first_task,
                                           tse_task_t **dep_task);

/* Blob callbacks */
H5VL_DAOS_PRIVATE herr_t H5_daos_blob_put(void *_file, const void *buf, size_t size, void *blob_id,
                                          void *_ctx);
H5VL_DAOS_PRIVATE herr_t H5_daos_blob_get(void *_file, const void *blob_id, void *buf, size_t size,
                                          void *_ctx);
H5VL_DAOS_PRIVATE herr_t H5_daos_blob_specific(void *_file, void *blob_id,
                                               H5VL_blob_specific_args_t *specific_args);

/* Request callbacks */
H5VL_DAOS_PRIVATE herr_t H5_daos_req_wait(void *req, uint64_t timeout, H5_DAOS_REQ_STATUS_OUT_TYPE *status);
H5VL_DAOS_PRIVATE herr_t H5_daos_req_notify(void *req, H5VL_request_notify_t cb, void *ctx);
H5VL_DAOS_PRIVATE herr_t H5_daos_req_cancel(void *_req
#if H5VL_VERSION >= 2
                                            ,
                                            H5_DAOS_REQ_STATUS_OUT_TYPE *status
#endif
);
H5VL_DAOS_PRIVATE herr_t H5_daos_req_specific(void *_req, H5VL_request_specific_args_t *specific_args);
H5VL_DAOS_PRIVATE herr_t H5_daos_req_free(void *req);

/* Other request routines */
H5VL_DAOS_PRIVATE H5_daos_req_t *H5_daos_req_create(H5_daos_file_t *file, const char *op_name,
                                                    H5_daos_req_t *prereq_req1, H5_daos_req_t *prereq_req2,
                                                    H5_daos_req_t *parent_req, hid_t dxpl_id);
H5VL_DAOS_PRIVATE herr_t         H5_daos_req_free_int(H5_daos_req_t *req);
H5VL_DAOS_PRIVATE herr_t H5_daos_req_enqueue(H5_daos_req_t *req, tse_task_t *first_task, H5_daos_item_t *item,
                                             H5_daos_op_pool_type_t op_type, H5_daos_op_pool_scope_t scope,
                                             hbool_t collective, hbool_t sync);
H5VL_DAOS_PRIVATE void   H5_daos_op_pool_free(H5_daos_op_pool_t *op_pool);

/* Generic asynchronous routines */
H5VL_DAOS_PRIVATE herr_t H5_daos_progress(H5_daos_req_t *req, uint64_t timeout);
H5VL_DAOS_PRIVATE herr_t H5_daos_create_task(tse_task_func_t task_func, unsigned num_deps,
                                             tse_task_t *dep_tasks[], tse_task_cb_t task_prep_cb,
                                             tse_task_cb_t task_comp_cb, void *task_priv, tse_task_t **taskp);
H5VL_DAOS_PRIVATE herr_t H5_daos_create_daos_task(daos_opc_t daos_opc, unsigned num_deps,
                                                  tse_task_t *dep_tasks[], tse_task_cb_t task_prep_cb,
                                                  tse_task_cb_t task_comp_cb, void *task_priv,
                                                  tse_task_t **taskp);
H5VL_DAOS_PRIVATE herr_t H5_daos_task_wait(tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE int    H5_daos_list_key_start(H5_daos_iter_ud_t *iter_udata, daos_opc_t opc,
                                                tse_task_cb_t comp_cb, tse_task_t **first_task,
                                                tse_task_t **dep_task);
H5VL_DAOS_PRIVATE int    H5_daos_list_key_init(H5_daos_iter_data_t *iter_data, H5_daos_obj_t *target_obj,
                                               daos_key_t *dkey, daos_opc_t opc, tse_task_cb_t comp_cb,
                                               hbool_t base_iter, size_t key_prefetch_size,
                                               size_t key_buf_size_init, tse_task_t **first_task,
                                               tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_mpi_ibcast(H5_daos_mpi_ibcast_ud_t *_bcast_udata, H5_daos_obj_t *obj,
                                            size_t buffer_size, hbool_t empty, tse_task_cb_t bcast_prep_cb,
                                            tse_task_cb_t bcast_comp_cb, H5_daos_req_t *req,
                                            tse_task_t **first_task, tse_task_t **dep_task);
H5VL_DAOS_PRIVATE herr_t H5_daos_collective_error_check(H5_daos_obj_t *obj, H5_daos_req_t *req,
                                                        tse_task_t **first_task, tse_task_t **dep_task);

/* Asynchronous task routines */
H5VL_DAOS_PRIVATE int H5_daos_h5op_finalize(tse_task_t *task);
H5VL_DAOS_PRIVATE int H5_daos_metatask_autocomplete(tse_task_t *task);
H5VL_DAOS_PRIVATE int H5_daos_metatask_autocomp_other(tse_task_t *task);
H5VL_DAOS_PRIVATE int H5_daos_mpi_ibcast_task(tse_task_t *task);
H5VL_DAOS_PRIVATE int H5_daos_mpi_ibarrier_task(tse_task_t *task);

/* Asynchronous prep/complete callbacks */
H5VL_DAOS_PRIVATE int H5_daos_generic_prep_cb(tse_task_t *task, void *args);
H5VL_DAOS_PRIVATE int H5_daos_generic_comp_cb(tse_task_t *task, void *args);
H5VL_DAOS_PRIVATE int H5_daos_obj_open_prep_cb(tse_task_t *task, void *args);
H5VL_DAOS_PRIVATE int H5_daos_md_rw_prep_cb(tse_task_t *task, void *args);
H5VL_DAOS_PRIVATE int H5_daos_md_update_comp_cb(tse_task_t *task, void *args);

/* Debugging routines */
#ifdef DV_PLUGIN_DEBUG
herr_t H5_daos_dump_obj_keys(daos_handle_t obj);
#endif

/* Routines to increment and decrement the counter keeping
 * track of the level of recursion with regards to top-level
 * connector callback routines. The counter should be
 * incremented at the very beginning of every top-level
 * connector callback, before anything else occurs. It
 * should be decremented before leaving that callback,
 * after everything else has occurred.
 */
static inline void
H5_daos_inc_api_cnt()
{
    H5_daos_api_count++;
}
static inline void
H5_daos_dec_api_cnt()
{
    H5_daos_api_count--;
}

/* Routine for setting const IOVEC */
static inline void
daos_const_iov_set(d_const_iov_t *iov, const void *buf, size_t size)
{
    iov->iov_buf = buf;
    iov->iov_len = iov->iov_buf_len = size;
}

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_private_H */
