/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 *          library.  General connector routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

#include <daos_mgmt.h> /* For pool creation */

/* HDF5 header for dynamic plugin loading */
#include <H5PLextern.h>

/****************/
/* Local Macros */
/****************/

/* Macro to "allocate" the next OIDX value from the local allocation of OIDXs */
#define H5_DAOS_ALLOCATE_NEXT_OIDX(oidx_out_ptr, next_oidx_ptr, max_oidx_ptr)                                \
    do {                                                                                                     \
        assert((*next_oidx_ptr) <= (*max_oidx_ptr));                                                         \
        (*oidx_out_ptr) = (*next_oidx_ptr);                                                                  \
        (*next_oidx_ptr)++;                                                                                  \
    } while (0)

/* Macro to adjust the next OIDX and max. OIDX pointers after
 * allocating more OIDXs from DAOS.
 */
#define H5_DAOS_ADJUST_MAX_AND_NEXT_OIDX(next_oidx_ptr, max_oidx_ptr)                                        \
    do {                                                                                                     \
        /* Set max oidx */                                                                                   \
        (*max_oidx_ptr) = (*next_oidx_ptr) + H5_DAOS_OIDX_NALLOC - 1;                                        \
                                                                                                             \
        /* Skip over reserved indices for the next oidx */                                                   \
        assert(H5_DAOS_OIDX_NALLOC > H5_DAOS_OIDX_FIRST_USER);                                               \
        if ((*next_oidx_ptr) < H5_DAOS_OIDX_FIRST_USER)                                                      \
            (*next_oidx_ptr) = H5_DAOS_OIDX_FIRST_USER;                                                      \
    } while (0)

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Task user data for pool connect */
typedef struct H5_daos_pool_connect_ud_t {
    H5_daos_req_t    *req;
    const char       *pool;
    daos_handle_t    *poh;
    daos_pool_info_t *info;
    const char       *sys;
    unsigned int      flags;
} H5_daos_pool_connect_ud_t;

/* Task user data for pool disconnect */
typedef struct H5_daos_pool_disconnect_ud_t {
    H5_daos_req_t *req;
    daos_handle_t *poh;
} H5_daos_pool_disconnect_ud_t;

/* Task user data for DAOS object open */
typedef struct H5_daos_obj_open_ud_t {
    H5_daos_generic_cb_ud_t generic_ud; /* Must be first */
    H5_daos_file_t         *file;
    daos_obj_id_t          *oid;
} H5_daos_obj_open_ud_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_set_prop(hid_t fcpl_id, const char *prop_str);
static herr_t H5_daos_set_object_class(hid_t plist_id, char *object_class);
static herr_t H5_daos_str_prop_delete(hid_t prop_id, const char *name, size_t size, void *_value);
static herr_t H5_daos_str_prop_copy(const char *name, size_t size, void *_value);
static int    H5_daos_str_prop_compare(const void *_value1, const void *_value2, size_t size);
static herr_t H5_daos_str_prop_close(const char *name, size_t size, void *_value);
static int    H5_daos_bool_prop_compare(const void *_value1, const void *_value2, size_t size);
static herr_t H5_daos_init(hid_t vipl_id);
static herr_t H5_daos_term(void);
static herr_t H5_daos_fill_def_plist_cache(void);
static void  *H5_daos_faccess_info_copy(const void *_old_fa);
static herr_t H5_daos_faccess_info_free(void *_fa);
static herr_t H5_daos_get_conn_cls(void *item, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);

#if H5VL_VERSION >= 3
static herr_t H5_daos_get_cap_flags(const void *info, uint64_t *cap_flags);
#else
static herr_t H5_daos_get_cap_flags(const void *info, unsigned *cap_flags);
#endif

static herr_t H5_daos_opt_query(void *item, H5VL_subclass_t cls, int opt_type,
                                H5_DAOS_OPT_QUERY_OUT_TYPE *supported);
static herr_t H5_daos_optional(void *item, H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req);

static herr_t H5_daos_oidx_bcast(H5_daos_file_t *file, uint64_t *oidx_out, H5_daos_req_t *req,
                                 tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_oidx_bcast_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_oidx_bcast_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_oidx_generate_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_oidx_generate_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_oid_encode_task(tse_task_t *task);
static int    H5_daos_list_key_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_list_key_finish(tse_task_t *task);
static int    H5_daos_free_async_task(tse_task_t *task);
static int    H5_daos_pool_connect_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_pool_connect_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_pool_disconnect_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_pool_disconnect_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_task_wait_task(tse_task_t *task);

static int H5_daos_collective_error_check_prep_cb(tse_task_t *task, void *args);
static int H5_daos_collective_error_check_comp_cb(tse_task_t *task, void *args);

#if H5VL_VERSION >= 3

#define H5VL_DAOS_CAP_FLAGS                                                                                  \
    (H5VL_CAP_FLAG_ASYNC | H5VL_CAP_FLAG_ATTR_BASIC | H5VL_CAP_FLAG_ATTR_MORE |                              \
     H5VL_CAP_FLAG_DATASET_BASIC | H5VL_CAP_FLAG_DATASET_MORE | H5VL_CAP_FLAG_FILE_BASIC |                   \
     H5VL_CAP_FLAG_FILE_MORE | H5VL_CAP_FLAG_GROUP_BASIC | H5VL_CAP_FLAG_GROUP_MORE |                        \
     H5VL_CAP_FLAG_LINK_BASIC | H5VL_CAP_FLAG_LINK_MORE | H5VL_CAP_FLAG_MAP_BASIC | H5VL_CAP_FLAG_MAP_MORE | \
     H5VL_CAP_FLAG_OBJECT_BASIC | H5VL_CAP_FLAG_OBJECT_MORE | H5VL_CAP_FLAG_REF_BASIC |                      \
     H5VL_CAP_FLAG_REF_MORE | H5VL_CAP_FLAG_OBJ_REF | H5VL_CAP_FLAG_REG_REF | H5VL_CAP_FLAG_ATTR_REF |       \
     H5VL_CAP_FLAG_STORED_DATATYPES | H5VL_CAP_FLAG_CREATION_ORDER | H5VL_CAP_FLAG_ITERATE |                 \
     H5VL_CAP_FLAG_STORAGE_SIZE | H5VL_CAP_FLAG_BY_IDX | H5VL_CAP_FLAG_GET_PLIST |                           \
     H5VL_CAP_FLAG_FLUSH_REFRESH | H5VL_CAP_FLAG_EXTERNAL_LINKS | H5VL_CAP_FLAG_HARD_LINKS |                 \
     H5VL_CAP_FLAG_SOFT_LINKS | H5VL_CAP_FLAG_UD_LINKS | H5VL_CAP_FLAG_TRACK_TIMES |                         \
     H5VL_CAP_FLAG_FILL_VALUES)

#else

#define H5VL_DAOS_CAP_FLAGS H5VL_CAP_FLAG_NONE

#endif

/*******************/
/* Local Variables */
/*******************/

/* The DAOS VOL connector struct */
static const H5VL_class_t H5_daos_g = {
    H5VL_VERSION,            /* VOL interface version number */
    H5_DAOS_CONNECTOR_VALUE, /* Connector Value */
    H5_DAOS_CONNECTOR_NAME,  /* Connector Name */
#if H5VL_VERSION >= 1
    HDF5_VOL_DAOS_VERSION_1, /* Connector Version number */
#endif
    H5VL_DAOS_CAP_FLAGS, /* Connector capability flags */
    H5_daos_init,        /* Connector initialize */
    H5_daos_term,        /* Connector terminate */
    {
        sizeof(H5_daos_acc_params_t), /* Connector Info size */
        H5_daos_faccess_info_copy,    /* Connector Info copy */
        NULL,                         /* Connector Info compare */
        H5_daos_faccess_info_free,    /* Connector Info free */
        NULL,                         /* Connector Info To String */
        NULL,                         /* Connector String To Info */
    },
    {
        NULL, /* Connector Get Object */
        NULL, /* Connector Get Wrap Ctx */
        NULL, /* Connector Wrap Object */
        NULL, /* Connector Unwrap Object */
        NULL, /* Connector Free Wrap Ctx */
    },
    {
        /* Connector Attribute cls */
        H5_daos_attribute_create,   /* Connector Attribute create */
        H5_daos_attribute_open,     /* Connector Attribute open */
        H5_daos_attribute_read,     /* Connector Attribute read */
        H5_daos_attribute_write,    /* Connector Attribute write */
        H5_daos_attribute_get,      /* Connector Attribute get */
        H5_daos_attribute_specific, /* Connector Attribute specific */
        NULL,                       /* Connector Attribute optional */
        H5_daos_attribute_close     /* Connector Attribute close */
    },
    {
        /* Connector Dataset cls */
        H5_daos_dataset_create,   /* Connector Dataset create */
        H5_daos_dataset_open,     /* Connector Dataset open */
        H5_daos_dataset_read,     /* Connector Dataset read */
        H5_daos_dataset_write,    /* Connector Dataset write */
        H5_daos_dataset_get,      /* Connector Dataset get */
        H5_daos_dataset_specific, /* Connector Dataset specific */
        NULL,                     /* Connector Dataset optional */
        H5_daos_dataset_close     /* Connector Dataset close */
    },
    {
        /* Connector Datatype cls */
        H5_daos_datatype_commit,   /* Connector Datatype commit */
        H5_daos_datatype_open,     /* Connector Datatype open */
        H5_daos_datatype_get,      /* Connector Datatype get */
        H5_daos_datatype_specific, /* Connector Datatype specific */
        NULL,                      /* Connector Datatype optional */
        H5_daos_datatype_close     /* Connector Datatype close */
    },
    {
        /* Connector File cls */
        H5_daos_file_create,   /* Connector File create */
        H5_daos_file_open,     /* Connector File open */
        H5_daos_file_get,      /* Connector File get */
        H5_daos_file_specific, /* Connector File specific */
        NULL,                  /* Connector File optional */
        H5_daos_file_close     /* Connector File close */
    },
    {
        /* Connector Group cls */
        H5_daos_group_create,   /* Connector Group create */
        H5_daos_group_open,     /* Connector Group open */
        H5_daos_group_get,      /* Connector Group get */
        H5_daos_group_specific, /* Connector Group specific */
        NULL,                   /* Connector Group optional */
        H5_daos_group_close     /* Connector Group close */
    },
    {
        /* Connector Link cls */
        H5_daos_link_create,   /* Connector Link create */
        H5_daos_link_copy,     /* Connector Link copy */
        H5_daos_link_move,     /* Connector Link move */
        H5_daos_link_get,      /* Connector Link get */
        H5_daos_link_specific, /* Connector Link specific */
        NULL                   /* Connector Link optional */
    },
    {
        /* Connector Object cls */
        H5_daos_object_open,     /* Connector Object open */
        H5_daos_object_copy,     /* Connector Object copy */
        H5_daos_object_get,      /* Connector Object get */
        H5_daos_object_specific, /* Connector Object specific */
        NULL                     /* Connector Object optional */
    },
    {
        H5_daos_get_conn_cls,  /* Connector get connector class */
        H5_daos_get_cap_flags, /* Connector get cap flags */
        H5_daos_opt_query      /* Connector optional callback query */
    },
    {
        H5_daos_req_wait,     /* Connector Request wait */
        H5_daos_req_notify,   /* Connector Request notify */
        H5_daos_req_cancel,   /* Connector Request cancel */
        H5_daos_req_specific, /* Connector Request specific */
        NULL,                 /* Connector Request optional */
        H5_daos_req_free      /* Connector Request free */
    },
    {
        H5_daos_blob_put,      /* Connector 'blob' put */
        H5_daos_blob_get,      /* Connector 'blob' get */
        H5_daos_blob_specific, /* Connector 'blob' specific */
        NULL                   /* Connector 'blob' optional */
    },
    {
        NULL, /* Connector Token compare */
        NULL, /* Connector Token to string */
        NULL  /* Connector Token from string */
    },
    H5_daos_optional /* Connector optional */
};

/* Free list definitions */
/* DSINC - currently no external access to free lists
H5FL_DEFINE(H5_daos_file_t);
H5FL_DEFINE(H5_daos_group_t);
H5FL_DEFINE(H5_daos_dset_t);
H5FL_DEFINE(H5_daos_dtype_t);
H5FL_DEFINE(H5_daos_map_t);
H5FL_DEFINE(H5_daos_attr_t);*/

hid_t          H5_DAOS_g             = H5I_INVALID_HID;
static hbool_t H5_daos_initialized_g = FALSE;

/* Identifiers for HDF5's error API */
hid_t dv_err_stack_g   = H5I_INVALID_HID;
hid_t dv_err_class_g   = H5I_INVALID_HID;
hid_t dv_obj_err_maj_g = H5I_INVALID_HID;
hid_t dv_async_err_g   = H5I_INVALID_HID;

#ifdef DV_TRACK_MEM_USAGE
/*
 * Counter to keep track of the currently allocated amount of bytes
 */
size_t daos_vol_curr_alloc_bytes;
#endif

/* Global variable used to bypass the DUNS in favor of standard DAOS
 * container operations if requested.
 */
hbool_t H5_daos_bypass_duns_g = FALSE;

/* Target chunk size for automatic chunking */
uint64_t H5_daos_chunk_target_size_g = H5_DAOS_CHUNK_TARGET_SIZE_DEF;

/* Global scheduler - used for tasks that are not tied to any open file */
tse_sched_t H5_daos_glob_sched_g;

/* Global ooperation pool - used for operations that are not tied to a single
 * file */
H5_daos_op_pool_t *H5_daos_glob_cur_op_pool_g = NULL;

/* Global variable for HDF5 property list cache */
H5_daos_plist_cache_t *H5_daos_plist_cache_g;

/* Global DAOS task list */
H5_daos_task_list_t *H5_daos_task_list_g = NULL;

/* DAOS task and MPI request for current in-flight MPI operation */
tse_task_t *H5_daos_mpi_task_g = NULL;
MPI_Request H5_daos_mpi_req_g;

/* Last collective request scheduled.  Only one collective operation can be in
 * flight at any one time. */
struct H5_daos_req_t *H5_daos_collective_req_tail = NULL;

/* Counter to keep track of the level of recursion with
 * regards to top-level connector callback routines. */
int H5_daos_api_count = 0;

/* Constant Keys */
const char H5_daos_int_md_key_g[]          = "/Internal Metadata";
const char H5_daos_root_grp_oid_key_g[]    = "Root Group OID";
const char H5_daos_rc_key_g[]              = "Ref Count";
const char H5_daos_cpl_key_g[]             = "Creation Property List";
const char H5_daos_link_key_g[]            = "Link";
const char H5_daos_link_corder_key_g[]     = "/Link Creation Order";
const char H5_daos_nlinks_key_g[]          = "Num Links";
const char H5_daos_max_link_corder_key_g[] = "Max Link Creation Order";
const char H5_daos_type_key_g[]            = "Datatype";
const char H5_daos_space_key_g[]           = "Dataspace";
const char H5_daos_attr_key_g[]            = "/Attribute";
const char H5_daos_nattr_key_g[]           = "Num Attributes";
const char H5_daos_max_attr_corder_key_g[] = "Max Attribute Creation Order";
const char H5_daos_ktype_g[]               = "Key Datatype";
const char H5_daos_vtype_g[]               = "Value Datatype";
const char H5_daos_map_key_g[]             = "Map Record";
const char H5_daos_blob_key_g[]            = "Blob";
const char H5_daos_fillval_key_g[]         = "Fill Value";

const daos_size_t H5_daos_int_md_key_size_g       = (daos_size_t)(sizeof(H5_daos_int_md_key_g) - 1);
const daos_size_t H5_daos_root_grp_oid_key_size_g = (daos_size_t)(sizeof(H5_daos_root_grp_oid_key_g) - 1);
const daos_size_t H5_daos_rc_key_size_g           = (daos_size_t)(sizeof(H5_daos_rc_key_g) - 1);
const daos_size_t H5_daos_cpl_key_size_g          = (daos_size_t)(sizeof(H5_daos_cpl_key_g) - 1);
const daos_size_t H5_daos_link_key_size_g         = (daos_size_t)(sizeof(H5_daos_link_key_g) - 1);
const daos_size_t H5_daos_link_corder_key_size_g  = (daos_size_t)(sizeof(H5_daos_link_corder_key_g) - 1);
const daos_size_t H5_daos_nlinks_key_size_g       = (daos_size_t)(sizeof(H5_daos_nlinks_key_g) - 1);
const daos_size_t H5_daos_max_link_corder_key_size_g =
    (daos_size_t)(sizeof(H5_daos_max_link_corder_key_g) - 1);
const daos_size_t H5_daos_type_key_size_g  = (daos_size_t)(sizeof(H5_daos_type_key_g) - 1);
const daos_size_t H5_daos_space_key_size_g = (daos_size_t)(sizeof(H5_daos_space_key_g) - 1);
const daos_size_t H5_daos_attr_key_size_g  = (daos_size_t)(sizeof(H5_daos_attr_key_g) - 1);
const daos_size_t H5_daos_nattr_key_size_g = (daos_size_t)(sizeof(H5_daos_nattr_key_g) - 1);
const daos_size_t H5_daos_max_attr_corder_key_size_g =
    (daos_size_t)(sizeof(H5_daos_max_attr_corder_key_g) - 1);
const daos_size_t H5_daos_ktype_size_g       = (daos_size_t)(sizeof(H5_daos_ktype_g) - 1);
const daos_size_t H5_daos_vtype_size_g       = (daos_size_t)(sizeof(H5_daos_vtype_g) - 1);
const daos_size_t H5_daos_map_key_size_g     = (daos_size_t)(sizeof(H5_daos_map_key_g) - 1);
const daos_size_t H5_daos_blob_key_size_g    = (daos_size_t)(sizeof(H5_daos_blob_key_g) - 1);
const daos_size_t H5_daos_fillval_key_size_g = (daos_size_t)(sizeof(H5_daos_fillval_key_g) - 1);

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_daos
 *
 * Purpose:     Modify the file access property list to use the DAOS VOL
 *              connector defined in this source file.
 *
 *              pool identifies the UUID or label of the DAOS pool to connect to. sys_name
 *              identifies the DAOS server system to use when connecting to the DAOS pool. This may
 *              be NULL, in which case a default system is used.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_daos(hid_t fapl_id, const char *pool, const char *sys_name)
{
    H5_daos_acc_params_t fa;
    H5I_type_t           idType = H5I_UNINIT;
    htri_t               is_fapl;
    herr_t               ret_value = FAIL;

    H5_daos_inc_api_cnt();

    /* Check arguments */
    if (fapl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if ((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (pool == NULL)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid POOL");

    /* Ensure HDF5 is initialized */
    if (H5open() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "HDF5 failed to initialize");

    /* Register the DAOS VOL connector if it isn't already registered */
    if (H5_DAOS_g >= 0 && (idType = H5Iget_type(H5_DAOS_g)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "failed to retrieve DAOS VOL connector's ID type");

    if (H5I_VOL != idType) {
        htri_t is_registered;

        if ((is_registered = H5VLis_connector_registered_by_value(H5_daos_g.value)) < 0)
            D_GOTO_ERROR(H5E_ID, H5E_CANTINIT, FAIL, "can't determine if DAOS VOL connector is registered");

        if (!is_registered) {
            /* Register connector */
            if ((H5_DAOS_g = H5VLregister_connector((const H5VL_class_t *)&H5_daos_g, H5P_DEFAULT)) < 0)
                D_GOTO_ERROR(H5E_ID, H5E_CANTINSERT, FAIL, "can't create ID for DAOS VOL connector");
        } /* end if */
        else {
            if ((H5_DAOS_g = H5VLget_connector_id_by_name(H5_daos_g.name)) < 0)
                D_GOTO_ERROR(H5E_ID, H5E_CANTGET, FAIL, "unable to get registered ID for DAOS VOL connector");
        } /* end else */
    }     /* end if */

    /* Initialize driver specific properties */
    strncpy(fa.pool, pool, DAOS_PROP_LABEL_MAX_LEN);
    fa.pool[DAOS_PROP_LABEL_MAX_LEN] = 0;

    memset(fa.sys, '\0', sizeof(fa.sys));
    if (sys_name) {
        strncpy(fa.sys, sys_name, DAOS_SYS_NAME_MAX);
        fa.sys[DAOS_SYS_NAME_MAX] = 0;
    }

    ret_value = H5Pset_vol(fapl_id, H5_DAOS_g, &fa);

done:
    D_FUNC_LEAVE_API;
} /* end H5Pset_fapl_daos() */

herr_t
H5daos_set_prop(hid_t fcpl_id, const char *prop_str)
{
    htri_t is_fcpl;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    /* Check arguments */
    if (fcpl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if ((is_fcpl = H5Pisa_class(fcpl_id, H5P_FILE_CREATE)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fcpl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file create property list");
    if (prop_str == NULL)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid DAOS property string");

    /* Call internal routine */
    if (H5_daos_set_prop(fcpl_id, prop_str) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't set DAOS properties");

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_set_prop() */

static herr_t
H5_daos_set_prop(hid_t fcpl_id, const char *prop_str)
{
    char  *copied_prop;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    /* Check if the property already exists on the property list */
    if ((prop_exists = H5Pexist(fcpl_id, H5_DAOS_FILE_PROP_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");

    /* Copy object class */
    if (prop_str)
        if (NULL == (copied_prop = strdup(prop_str)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy prop string");

    /* Set the property, or insert it if it does not exist */
    if (prop_exists) {
        if (H5Pset(fcpl_id, H5_DAOS_FILE_PROP_NAME, &copied_prop) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property");
    } /* end if */
    else if (H5Pinsert2(fcpl_id, H5_DAOS_FILE_PROP_NAME, sizeof(char *), &copied_prop, NULL, NULL,
                        H5_daos_str_prop_delete, H5_daos_str_prop_copy, H5_daos_str_prop_compare,
                        H5_daos_str_prop_close) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into list");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_set_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_set_object_class
 *
 * Purpose:     Sets the provided DAOS object class on the property list.
 *              See DAOS documentation for a list of object classes and
 *              descriptions of them.
 *
 *              If called on a FCPL, GCPL, TCPL, DCPL, or MCPL, it affects
 *              objects created using that creation property list (FCPL
 *              affects only the file root group and global metadata
 *              object).
 *
 *              If called on a FAPL it affects all objects created during
 *              this file open, except those with their object class
 *              specified via the creation property list, as above.
 *
 *              The default value is "", which allows the connector to set
 *              the object class according to its default for the object
 *              type.
 *
 *              If the root group is created with a non-default object
 *              class, then if the file is opened at a later time, the
 *              root group's object class must the be set on the FAPL
 *              using H5daos_set_root_open_object_class().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_set_object_class(hid_t plist_id, char *object_class)
{
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (plist_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");

    /* Call internal routine */
    if (H5_daos_set_object_class(plist_id, object_class) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't set object class");

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_set_object_class() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_set_object_class
 *
 * Purpose:     Internal version of H5daos_set_object_class().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_set_object_class(hid_t plist_id, char *object_class)
{
    char  *copied_object_class = NULL;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    /* Check if the property already exists on the property list */
    if ((prop_exists = H5Pexist(plist_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");

    /* Copy object class */
    if (object_class)
        if (NULL == (copied_object_class = strdup(object_class)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy object class string");

    /* Set the property, or insert it if it does not exist */
    if (prop_exists) {
        if (H5Pset(plist_id, H5_DAOS_OBJ_CLASS_NAME, &copied_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property");
    } /* end if */
    else if (H5Pinsert2(plist_id, H5_DAOS_OBJ_CLASS_NAME, sizeof(char *), &copied_object_class, NULL, NULL,
                        H5_daos_str_prop_delete, H5_daos_str_prop_copy, H5_daos_str_prop_compare,
                        H5_daos_str_prop_close) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into list");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_set_object_class() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_set_oclass_from_oid
 *
 * Purpose:     Decodes the object class embedded in the provided DAOS OID
 *              and adds it to the provided property list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_set_oclass_from_oid(hid_t plist_id, daos_obj_id_t oid)
{
    daos_oclass_id_t oc_id;
#if CHECK_DAOS_API_VERSION(2, 3)
    char oclass_str[MAX_OBJ_CLASS_NAME_LEN];
#else
    char oclass_str[24];
#endif
    herr_t ret_value = SUCCEED;

    /* Get object class id from oid */
#if CHECK_DAOS_API_VERSION(1, 6)
    oc_id = daos_obj_id2class(oid);
#else
    oc_id = ((oid.hi & OID_FMT_CLASS_MASK) >> OID_FMT_CLASS_SHIFT) & 0xffff;
#endif

    /* Get object class string */
    if (daos_oclass_id2name(oc_id, oclass_str) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class string");

    /* Set object class string on plist */
    if (H5_daos_set_object_class(plist_id, oclass_str) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't set object class");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_set_oclass_from_oid() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_get_object_class
 *
 * Purpose:     Retrieves the object class from the provided property
 *              list.  If plist_id was retrieved via a call to
 *              H5*get_create_plist(), the returned object class will be
 *              the actual DAOS object class of the object (it will not be
 *              the property list default value of "").
 *
 *              If not NULL, object_class points to a user-allocated
 *              output buffer, whose size is size.
 *
 * Return:      Success:        length of object class string (excluding
 *                              null terminator)
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5daos_get_object_class(hid_t plist_id, char *object_class, size_t size)
{
    char   *tmp_object_class = NULL;
    htri_t  prop_exists;
    size_t  len;
    ssize_t ret_value;

    H5_daos_inc_api_cnt();

    /* Check if the property already exists on the property list */
    if ((prop_exists = H5Pexist(plist_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");

    if (prop_exists) {
        /* Get the property */
        if (H5Pget(plist_id, H5_DAOS_OBJ_CLASS_NAME, &tmp_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get object class");

        /* Set output values */
        if (tmp_object_class) {
            len = strlen(tmp_object_class);
            if (object_class && (size > 0)) {
                strncpy(object_class, tmp_object_class, size);
                if (len >= size)
                    object_class[size - 1] = '\0';
            } /* end if */
        }     /* end if */
        else {
            /* Simply return an empty string */
            len = 0;
            if (object_class && (size > 0))
                object_class[0] = '\0';
        } /* end else */
    }     /* end if */
    else {
        /* Simply return an empty string */
        len = 0;
        if (object_class && (size > 0))
            object_class[0] = '\0';
    } /* end else */

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_object_class() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_set_root_open_object_class
 *
 * Purpose:     Sets the object class to use for opening the root group on
 *              the provided file access property list.  This should match
 *              the object class used to create the root group via
 *              H5daos_set_object_class().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_set_root_open_object_class(hid_t fapl_id, char *object_class)
{
    htri_t is_fapl;
    char  *copied_object_class = NULL;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (fapl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");

    if ((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Check if the property already exists on the property list */
    if ((prop_exists = H5Pexist(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");

    /* Copy object class */
    if (object_class)
        if (NULL == (copied_object_class = strdup(object_class)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy object class string");

    /* Set the property, or insert it if it does not exist */
    if (prop_exists) {
        if (H5Pset(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME, &copied_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property");
    } /* end if */
    else if (H5Pinsert2(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME, sizeof(char *), &copied_object_class, NULL,
                        NULL, H5_daos_str_prop_delete, H5_daos_str_prop_copy, H5_daos_str_prop_compare,
                        H5_daos_str_prop_close) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into list");

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_set_root_open_object_class() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_get_root_open_object_class
 *
 * Purpose:     Retrieves the object class for opening the root group from
 *              the provided file access property list, as set by
 *              H5daos_set_root_open_object_class().
 *
 *              If not NULL, object_class points to a user-allocated
 *              output buffer, whose size is size.
 *
 * Return:      Success:        length of object class string (excluding
 *                              null terminator)
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5daos_get_root_open_object_class(hid_t fapl_id, char *object_class, size_t size)
{
    htri_t  is_fapl;
    char   *tmp_object_class = NULL;
    htri_t  prop_exists;
    size_t  len;
    ssize_t ret_value;

    H5_daos_inc_api_cnt();

    if ((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Check if the property already exists on the property list */
    if ((prop_exists = H5Pexist(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");

    if (prop_exists) {
        /* Get the property */
        if (H5Pget(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME, &tmp_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get object class");

        /* Set output values */
        if (tmp_object_class) {
            len = strlen(tmp_object_class);
            if (object_class && (size > 0)) {
                strncpy(object_class, tmp_object_class, size);
                if (len >= size)
                    object_class[size - 1] = '\0';
            } /* end if */
        }     /* end if */
        else {
            /* Simply return an empty string */
            len = 0;
            if (object_class && (size > 0))
                object_class[0] = '\0';
        } /* end else */
    }     /* end if */
    else {
        /* Simply return an empty string */
        len = 0;
        if (object_class && (size > 0))
            object_class[0] = '\0';
    } /* end else */

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_root_open_object_class() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_set_all_ind_metadata_ops
 *
 * Purpose:     Modifies the access property list to indicate that all
 *              metadata I/O operations should be performed independently.
 *              By default, metadata reads are independent and metadata
 *              writes are collective.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_set_all_ind_metadata_ops(hid_t accpl_id, hbool_t is_independent)
{
    htri_t is_fapl;
    htri_t is_lapl;
    htri_t is_rapl;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (accpl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");

    if ((is_fapl = H5Pisa_class(accpl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if ((is_lapl = H5Pisa_class(accpl_id, H5P_LINK_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if ((is_rapl = H5Pisa_class(accpl_id, H5P_REFERENCE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fapl && !is_lapl && !is_rapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an access property list");

    /* Check if the independent metadata writes property already exists on the property list */
    if ((prop_exists = H5Pexist(accpl_id, H5_DAOS_IND_MD_IO_PROP_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for independent metadata I/O property");

    /* Set the property, or insert it if it does not exist */
    if (prop_exists) {
        if (H5Pset(accpl_id, H5_DAOS_IND_MD_IO_PROP_NAME, &is_independent) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set independent metadata I/O property");
    } /* end if */
    else if (H5Pinsert2(accpl_id, H5_DAOS_IND_MD_IO_PROP_NAME, sizeof(hbool_t), &is_independent, NULL, NULL,
                        NULL, NULL, H5_daos_bool_prop_compare, NULL) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into list");

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_set_all_ind_metadata_ops() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_get_all_ind_metadata_ops
 *
 * Purpose:     Retrieves the independent metadata I/O setting from the
 *              access property list accpl_id.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_get_all_ind_metadata_ops(hid_t accpl_id, hbool_t *is_independent)
{
    htri_t is_fapl;
    htri_t is_lapl;
    htri_t is_rapl;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if ((is_fapl = H5Pisa_class(accpl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if ((is_lapl = H5Pisa_class(accpl_id, H5P_LINK_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if ((is_rapl = H5Pisa_class(accpl_id, H5P_REFERENCE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fapl && !is_lapl && !is_rapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an access property list");

    /* Check if the independent metadata writes property exists on the property list */
    if ((prop_exists = H5Pexist(accpl_id, H5_DAOS_IND_MD_IO_PROP_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for independent metadata I/O property");

    if (prop_exists) {
        /* Get the property */
        if (H5Pget(accpl_id, H5_DAOS_IND_MD_IO_PROP_NAME, is_independent) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get independent metadata I/O property");
    } /* end if */
    else {
        /* Simply return FALSE as not all metadata I/O
         * operations are independent by default. */
        *is_independent = FALSE;
    } /* end else */

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_all_ind_metadata_ops() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_str_prop_delete
 *
 * Purpose:     Property list callback for deleting a string property.
 *              Frees the string.
 *
 * Return:      SUCCEED (never fails)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_str_prop_delete(hid_t H5VL_DAOS_UNUSED prop_id, const char H5VL_DAOS_UNUSED *name,
                        size_t H5VL_DAOS_UNUSED size, void *_value)
{
    char **value = (char **)_value;

    if (*value)
        free(*value);

    return SUCCEED;
} /* end H5_daos_str_prop_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_str_prop_copy
 *
 * Purpose:     Property list callback for copying a string property.
 *              Duplicates the string.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_str_prop_copy(const char H5VL_DAOS_UNUSED *name, size_t H5VL_DAOS_UNUSED size, void *_value)
{
    char **value     = (char **)_value;
    herr_t ret_value = SUCCEED;

    if (*value)
        if (NULL == (*value = strdup(*value)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy string property");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_str_prop_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_str_prop_compare
 *
 * Purpose:     Property list callback for comparing string properties.
 *              Compares the strings using strcmp().
 *
 * Return:      SUCCEED (never fails)
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_str_prop_compare(const void *_value1, const void *_value2, size_t H5VL_DAOS_UNUSED size)
{
    char *const *value1 = (char *const *)_value1;
    char *const *value2 = (char *const *)_value2;
    int          ret_value;

    if (*value1) {
        if (*value2)
            ret_value = strcmp(*value1, *value2);
        else
            ret_value = 1;
    } /* end if */
    else {
        if (*value2)
            ret_value = -1;
        else
            ret_value = 0;
    } /* end else */

    return ret_value;
} /* end H5_daos_str_prop_compare() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_str_prop_close
 *
 * Purpose:     Property list callback for deleting a string property.
 *              Frees the string.
 *
 * Return:      SUCCEED (never fails)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_str_prop_close(const char H5VL_DAOS_UNUSED *name, size_t H5VL_DAOS_UNUSED size, void *_value)
{
    char **value = (char **)_value;

    if (*value)
        free(*value);

    return SUCCEED;
} /* end H5_daos_str_prop_close() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_bool_prop_compare
 *
 * Purpose:     Property list callback for comparing boolean properties.
 *              Compares the boolean values directly.
 *
 * Return:      SUCCEED (never fails)
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_bool_prop_compare(const void *_value1, const void *_value2, size_t H5VL_DAOS_UNUSED size)
{
    const hbool_t *bool1 = (const hbool_t *)_value1;
    const hbool_t *bool2 = (const hbool_t *)_value2;

    return *bool1 == *bool2;
} /* end H5_daos_bool_prop_compare() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_snap_create
 *
 * Purpose:     Creates a snapshot and returns the snapshot ID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
#ifdef DSINC
herr_t
H5daos_snap_create(hid_t loc_id, H5_daos_snap_id_t *snap_id)
{
    H5_daos_item_t *item;
    H5_daos_file_t *file;
    H5VL_object_t  *obj       = NULL; /* object token of loc_id */
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!snap_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "snapshot ID pointer is NULL");

    /* get the location object */
    if (NULL == (obj = (H5VL_object_t *)H5I_object(loc_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Make sure object's VOL is this one */
    if (obj->driver->id != H5_DAOS_g)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location does not use DAOS VOL connector");

    /* Get file object */
    if (NULL == (item = H5VLobject(loc_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a VOL object");

    file = item->file;

    /* Check for write access */
    if (!(file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file");

    /* Tell the file to save a snapshot next time it is flushed (committed) */
    file->snap_epoch = (int)TRUE;

    /* Return epoch in snap_id */
    *snap_id = (uint64_t)file->epoch;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_snap_create() */
#endif

/*-------------------------------------------------------------------------
 * Function:    H5Pset_daos_snap_open
 *
 * XXX: text to be changed
 * Purpose:     Modify the file access property list to use the DAOS VOL
 *              connector defined in this source file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
#ifdef DV_HAVE_SNAP_OPEN_ID
herr_t
H5Pset_daos_snap_open(hid_t fapl_id, H5_daos_snap_id_t snap_id)
{
    htri_t is_fapl;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (fapl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");

    if ((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class");
    if (!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Set the property */
    if (H5Pset(fapl_id, H5_DAOS_SNAP_OPEN_ID, &snap_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property value for snap id");

done:
    D_FUNC_LEAVE_API;
} /* end H5Pset_daos_snap_open() */
#endif

/*-------------------------------------------------------------------------
 * Function:    H5_daos_init
 *
 * Purpose:     Initialize this VOL connector by registering the connector
 *              with the library.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_init(hid_t H5VL_DAOS_UNUSED vipl_id)
{
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id_default;
#endif
    char  *auto_chunk_str = NULL;
    int    ret;
    herr_t ret_value = SUCCEED; /* Return value */

    if (H5_daos_initialized_g)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "attempting to initialize connector twice");

    if ((dv_err_stack_g = H5Ecreate_stack()) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create HDF5 error stack");

    /* Register the connector with HDF5's error reporting API */
    if ((dv_err_class_g = H5Eregister_class(DAOS_VOL_ERR_CLS_NAME, DAOS_VOL_ERR_LIB_NAME, DAOS_VOL_ERR_VER)) <
        0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register error class with HDF5 error API");

    /* Register major error code for failures in object interface */
    if ((dv_obj_err_maj_g = H5Ecreate_msg(dv_err_class_g, H5E_MAJOR, "Object interface")) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register error message for object interface");

    /* Register major error code for failures in asynchronous interface */
    if ((dv_async_err_g = H5Ecreate_msg(dv_err_class_g, H5E_MAJOR, "Asynchronous interface")) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register error message for asynchronous interface");

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Register the DAOS SNAP_OPEN_ID property with HDF5 */
    snap_id_default = H5_DAOS_SNAP_ID_INVAL;
    if (H5Pregister2(H5P_FILE_ACCESS, H5_DAOS_SNAP_OPEN_ID, sizeof(H5_daos_snap_id_t),
                     (H5_daos_snap_id_t *)&snap_id_default, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "unable to register DAOS SNAP_OPEN_ID property");
#endif

    /* Initialize daos */
    if ((0 != (ret = daos_init())) && (ret != -DER_ALREADY))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "DAOS failed to initialize: %s",
                     H5_daos_err_to_string(ret));

#ifdef DV_TRACK_MEM_USAGE
    /* Initialize allocated memory counter */
    daos_vol_curr_alloc_bytes = 0;
#endif

    /* Determine if bypassing of the DUNS has been requested */
    if (NULL != getenv("HDF5_DAOS_BYPASS_DUNS"))
        H5_daos_bypass_duns_g = TRUE;

    /* Determine automatic chunking target size */
    if (NULL != (auto_chunk_str = getenv("HDF5_DAOS_CHUNK_TARGET_SIZE"))) {
        long long chunk_target_size_ll;

        errno = 0;
        if ((chunk_target_size_ll = strtoll(auto_chunk_str, NULL, 10)) < 0 || errno)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL,
                         "failed to parse automatic chunking target size from environment or invalid value "
                         "(H5_DAOS_CHUNK_TARGET_SIZE)");
        H5_daos_chunk_target_size_g = (uint64_t)chunk_target_size_ll;
    } /* end if */

    /* Initialize global scheduler */
    if (0 != (ret = tse_sched_init(&H5_daos_glob_sched_g, NULL, NULL)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create global task scheduler: %s",
                     H5_daos_err_to_string(ret));

    /* Create global DAOS task list */
    if (H5_daos_task_list_create(&H5_daos_task_list_g) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create DAOS task list");

    /* Setup HDF5 default property list cache */
    if (H5_daos_fill_def_plist_cache() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't populate HDF5 default property list cache");

    /* Initialized */
    H5_daos_initialized_g = TRUE;

done:
    if (ret_value < 0) {
        H5_daos_term();
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_init() */

/*---------------------------------------------------------------------------
 * Function:    H5_daos_term
 *
 * Purpose:     Shut down the DAOS VOL
 *
 * Returns:     Non-negative on success/Negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5_daos_term(void)
{
    herr_t ret_value = SUCCEED;

    /**
     * H5_DAOS_g is only set if the connector is manually initialized,
     * therefore we must check for proper DAOS initialization.
     */
    if (!H5_daos_initialized_g)
        D_GOTO_DONE(ret_value);

    /* Release global op pool */
    if (H5_daos_glob_cur_op_pool_g)
        H5_daos_op_pool_free(H5_daos_glob_cur_op_pool_g);

    /* Free global DAOS task list */
    if (H5_daos_task_list_g) {
        H5_daos_task_list_free(H5_daos_task_list_g);
        H5_daos_task_list_g = NULL;
    }

    /* Close global scheduler */
    if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_WAIT) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler");
    tse_sched_fini(&H5_daos_glob_sched_g);

    /* Terminate DAOS */
    if (daos_fini() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "DAOS failed to terminate");

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Unregister the DAOS SNAP_OPEN_ID property from HDF5 */
    if (H5Punregister(H5P_FILE_ACCESS, H5_DAOS_SNAP_OPEN_ID) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't unregister DAOS SNAP_OPEN_ID property");
#endif

    /* Free default property list cache */
    DV_free((void *)H5_daos_plist_cache_g);

    /* "Forget" connector id.  This should normally be called by the library
     * when it is closing the id, so no need to close it here. */
    H5_DAOS_g = H5I_INVALID_HID;

    /* No longer initialized */
    H5_daos_initialized_g = FALSE;

done:
#ifdef DV_TRACK_MEM_USAGE
    /* Check for allocated memory */
    if (0 != daos_vol_curr_alloc_bytes)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "%zu bytes were still left allocated",
                     daos_vol_curr_alloc_bytes)

    daos_vol_curr_alloc_bytes = 0;
#endif

    /* Unregister from the HDF5 error API */
    if (dv_err_class_g >= 0) {
        if (dv_obj_err_maj_g >= 0 && H5Eclose_msg(dv_obj_err_maj_g) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL,
                         "can't unregister error message for object interface");
        if (dv_async_err_g >= 0 && H5Eclose_msg(dv_async_err_g) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL,
                         "can't unregister error message for asynchronous interface");

        if (H5Eunregister_class(dv_err_class_g) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't unregister error class from HDF5 error API");

        /* Print the current error stack before destroying it */
        PRINT_ERROR_STACK;

        /* Destroy the error stack */
        if (H5Eclose_stack(dv_err_stack_g) < 0) {
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't close HDF5 error stack");
            PRINT_ERROR_STACK;
        } /* end if */

        dv_err_stack_g   = H5I_INVALID_HID;
        dv_err_class_g   = H5I_INVALID_HID;
        dv_obj_err_maj_g = H5I_INVALID_HID;
        dv_async_err_g   = H5I_INVALID_HID;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_term() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_fill_def_plist_cache
 *
 * Purpose:     Sets up a global cache of the default values for several
 *              properties of HDF5's default property lists. This can avoid
 *              some overhead from H5P calls when default property lists
 *              are used.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_fill_def_plist_cache(void)
{
    H5_daos_plist_cache_t *cache_ptr = NULL;
    herr_t                 ret_value = SUCCEED;

    if (NULL == (cache_ptr = DV_malloc(sizeof(H5_daos_plist_cache_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate property list cache");

    /* GCPL cache */
    if (H5Pget_link_creation_order(H5P_GROUP_CREATE_DEFAULT, &cache_ptr->gcpl_cache.link_corder_flags) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default link creation order flags");
    if (H5Pget_attr_creation_order(H5P_GROUP_CREATE_DEFAULT, &cache_ptr->gcpl_cache.acorder_flags) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL,
                     "can't get default attribute creation order flags for groups");

    /* DCPL cache */
    if ((cache_ptr->dcpl_cache.layout = H5Pget_layout(H5P_DATASET_CREATE_DEFAULT)) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default dataset layout property");
    if (H5Pfill_value_defined(H5P_DATASET_CREATE_DEFAULT, &cache_ptr->dcpl_cache.fill_status) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default dataset fill value status");
    if (H5Pget_fill_time(H5P_DATASET_CREATE_DEFAULT, &cache_ptr->dcpl_cache.fill_time) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default dataset fill time");
    if (H5Pget_attr_creation_order(H5P_DATASET_CREATE_DEFAULT, &cache_ptr->dcpl_cache.acorder_flags) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL,
                     "can't get default attribute creation order flags for datasets");

    /* TCPL cache */
    if (H5Pget_attr_creation_order(H5P_DATATYPE_CREATE_DEFAULT, &cache_ptr->tcpl_cache.acorder_flags) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL,
                     "can't get default attribute creation order flags for committed datatypes");

    /* MCPL cache */
    if (H5Pget_attr_creation_order(H5P_MAP_CREATE_DEFAULT, &cache_ptr->mcpl_cache.acorder_flags) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL,
                     "can't get default attribute creation order flags for maps");

    /* MAPL cache */
    if (H5Pget_map_iterate_hints(H5P_MAP_ACCESS_DEFAULT, &cache_ptr->mapl_cache.dkey_prefetch_size,
                                 &cache_ptr->mapl_cache.dkey_alloc_size) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get map iterate hints");

    /* LCPL cache */
    if (H5Pget_create_intermediate_group(H5P_LINK_CREATE_DEFAULT, &cache_ptr->lcpl_cache.crt_intermed_grp) <
        0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL,
                     "can't get default intermediate group creation property value");

    /* OcopyPL cache */
    if (H5Pget_copy_object(H5P_OBJECT_COPY_DEFAULT, &cache_ptr->ocpypl_cache.obj_copy_options) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "failed to retrieve default object copy options");

    H5_daos_plist_cache_g = cache_ptr;

done:
    if (ret_value < 0 && cache_ptr)
        DV_free(cache_ptr);

    D_FUNC_LEAVE;
} /* end H5_daos_fill_def_plist_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_connect
 *
 * Purpose:     Creates an asynchronous task for connecting to the
 *              specified pool.
 *
 *              DSINC - This routine should eventually be modified to serve
 *                      pool handles from a cache of open handles so that
 *                      we don't re-connect to pools which are already
 *                      connected to when doing multiple file creates/opens
 *                      within the same pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_pool_connect(H5_daos_acc_params_t *pool_acc_params, unsigned int flags, daos_handle_t *poh_out,
                     daos_pool_info_t *pool_info_out, H5_daos_req_t *req, tse_task_t **first_task,
                     tse_task_t **dep_task)
{
    H5_daos_pool_connect_ud_t *connect_udata = NULL;
    tse_task_t                *connect_task  = NULL;
    int                        ret;
    herr_t                     ret_value = SUCCEED;

    assert(pool_acc_params);
    assert(pool_acc_params->pool);
    assert(poh_out);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (connect_udata = (H5_daos_pool_connect_ud_t *)DV_malloc(sizeof(H5_daos_pool_connect_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for pool connect task");
    connect_udata->req   = req;
    connect_udata->pool  = pool_acc_params->pool;
    connect_udata->poh   = poh_out;
    connect_udata->sys   = pool_acc_params->sys;
    connect_udata->flags = flags;
    connect_udata->info  = pool_info_out;

    /* Create task for pool connect */
    if (H5_daos_create_daos_task(DAOS_OPC_POOL_CONNECT, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_pool_connect_prep_cb, H5_daos_pool_connect_comp_cb, connect_udata,
                                 &connect_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to connect to DAOS pool");

    /* Schedule DAOS pool connect task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(connect_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to connect to DAOS pool: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = connect_task;
    req->rc++;

    /* Relinquish control of the pool connect udata to the
     * task's function body */
    connect_udata = NULL;

    *dep_task = connect_task;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        connect_udata = DV_free(connect_udata);
    } /* end if */

    assert(!connect_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_pool_connect() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_connect_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_pool_connect.
 *              Currently checks for errors from previous tasks then sets
 *              arguments for daos_pool_connect.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_pool_connect_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_pool_connect_ud_t *udata;
    daos_pool_connect_t       *connect_args;
    int                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for pool connect task");

    assert(udata->req);
    assert(udata->pool);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_VOL);

    /* Ensure that DAOS pool is set */
    if (!udata->pool || (0 == strlen(udata->pool)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_BAD_VALUE, "DAOS pool is not set");

    /* Set daos_pool_connect task args */
    if (NULL == (connect_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for pool connect task");
    memset(connect_args, 0, sizeof(*connect_args));
    connect_args->poh   = udata->poh;
    connect_args->grp   = udata->sys;
    connect_args->flags = udata->flags;
    connect_args->info  = udata->info;
#if CHECK_DAOS_API_VERSION(1, 4)
    connect_args->pool = udata->pool;
#else
    if (uuid_parse(udata->pool, connect_args->uuid) < 0)
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "invalid pool uuid");
#endif

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_pool_connect_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_connect_comp_cb
 *
 * Purpose:     Completion callback for asynchronous daos_pool_connect.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_pool_connect_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_pool_connect_ud_t *udata;
    int                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DAOS pool connect task");

    assert(udata->req);

    /* Handle errors in daos_pool_connect task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "DAOS pool connect";
    } /* end if */
    else if (task->dt_result == 0) {
        /* After connecting to a pool, check if the file object's container_poh
         * field has been set yet. If not, make sure it gets updated with the
         * handle of the pool that we just connected to. This will most often
         * happen during file opens, where the file object's container_poh
         * field is initially invalid.
         */
        if (udata->req->file && daos_handle_is_inval(udata->req->file->container_poh))
            udata->req->file->container_poh = *udata->poh;
    } /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DAOS pool connect completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

#if !defined(DAOS_API_VERSION_MAJOR) || DAOS_API_VERSION_MAJOR < 1
        if (udata->svc)
            d_rank_list_free(udata->svc);
#endif

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_pool_connect_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_disconnect
 *
 * Purpose:     Creates an asynchronous task for disconnecting from the
 *              specified pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_pool_disconnect(daos_handle_t *poh, H5_daos_req_t *req, tse_task_t **first_task,
                        tse_task_t **dep_task)
{
    H5_daos_pool_disconnect_ud_t *disconnect_udata = NULL;
    tse_task_t                   *disconnect_task  = NULL;
    int                           ret;
    herr_t                        ret_value = SUCCEED;

    assert(poh);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL ==
        (disconnect_udata = (H5_daos_pool_disconnect_ud_t *)DV_malloc(sizeof(H5_daos_pool_disconnect_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for pool disconnect task");
    disconnect_udata->req = req;
    disconnect_udata->poh = poh;

    /* Create task for pool disconnect */
    if (H5_daos_create_daos_task(DAOS_OPC_POOL_DISCONNECT, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_pool_disconnect_prep_cb, H5_daos_pool_disconnect_comp_cb,
                                 disconnect_udata, &disconnect_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to disconnect from DAOS pool");

    /* Schedule DAOS pool disconnect task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(disconnect_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to disconnect from DAOS pool: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = disconnect_task;
    req->rc++;

    /* Relinquish control of the pool disconnect udata to the
     * task's function body */
    disconnect_udata = NULL;

    *dep_task = disconnect_task;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        disconnect_udata = DV_free(disconnect_udata);
    } /* end if */

    assert(!disconnect_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_pool_disconnect() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_disconnect_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_pool_disconnect.
 *              Currently checks for errors from previous tasks then sets
 *              arguments for daos_pool_disconnect.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_pool_disconnect_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_pool_disconnect_ud_t *udata;
    daos_pool_disconnect_t       *disconnect_args;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for pool disconnect task");

    assert(udata->req);
    assert(udata->poh);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_VOL);

    if (daos_handle_is_inval(*udata->poh))
        D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, -H5_DAOS_BAD_VALUE, "pool handle is invalid");

    /* Set daos_pool_disconnect task args */
    if (NULL == (disconnect_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for pool disconnect task");
    disconnect_args->poh = *udata->poh;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_pool_disconnect_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_disconnect_comp_cb
 *
 * Purpose:     Completion callback for asynchronous daos_pool_disconnect.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_pool_disconnect_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_pool_disconnect_ud_t *udata;
    int                           ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DAOS pool disconnect task");

    assert(udata->req);

    /* Handle errors in daos_pool_disconnect task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "DAOS pool disconnect";
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DAOS pool disconnect completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_pool_disconnect_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_faccess_info_copy
 *
 * Purpose:     Copies the DAOS-specific file access properties.
 *
 * Return:      Success:        Ptr to a new property list
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
static void *
H5_daos_faccess_info_copy(const void *_old_fa)
{
    const H5_daos_acc_params_t *old_fa    = (const H5_daos_acc_params_t *)_old_fa;
    H5_daos_acc_params_t       *new_fa    = NULL;
    void                       *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_old_fa)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid fapl");

    if (NULL == (new_fa = (H5_daos_acc_params_t *)DV_malloc(sizeof(H5_daos_acc_params_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the general information */
    memcpy(new_fa, old_fa, sizeof(H5_daos_acc_params_t));

    ret_value = new_fa;

done:
    if (NULL == ret_value) {
        /* cleanup */
        if (new_fa && H5_daos_faccess_info_free(new_fa) < 0)
            D_DONE_ERROR(H5E_PLIST, H5E_CANTFREE, NULL, "can't free fapl");
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_faccess_info_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_faccess_info_free
 *
 * Purpose:     Frees the DAOS-specific file access properties.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_faccess_info_free(void *_fa)
{
    H5_daos_acc_params_t *fa        = (H5_daos_acc_params_t *)_fa;
    herr_t                ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_fa)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fapl");

done:
    /* free the struct */
    DV_free(fa);

    D_FUNC_LEAVE_API;
} /* end H5_daos_faccess_info_free() */

/*---------------------------------------------------------------------------
 * Function:    H5_daos_get_conn_cls
 *
 * Purpose:     Query the connector class.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_conn_cls(void *item, H5VL_get_conn_lvl_t H5VL_DAOS_UNUSED lvl, const H5VL_class_t **conn_cls)
{
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "item parameter not supplied");
    if (!conn_cls)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "conn_cls parameter not supplied");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Retrieve the DAOS VOL connector class */
    *conn_cls = &H5_daos_g;

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_get_conn_cls() */

/*---------------------------------------------------------------------------
 * Function:    H5_daos_get_cap_flags
 *
 * Purpose:     Retrieves the capability flags for this VOL connector.
 *
 * Return:      Non-negative on Success/Negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_cap_flags(const void H5VL_DAOS_UNUSED *info,
#if H5VL_VERSION >= 3
                      uint64_t *cap_flags
#else
                      unsigned *cap_flags
#endif
)
{
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!cap_flags)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid cap_flags parameter");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Set the flags from the connector's capability flags field */
    *cap_flags = H5_daos_g.cap_flags;

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_get_cap_flags() */

/*---------------------------------------------------------------------------
 * Function:    H5_daos_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5_daos_opt_query(void *item, H5VL_subclass_t H5VL_DAOS_UNUSED cls, int opt_type,
                  H5_DAOS_OPT_QUERY_OUT_TYPE *supported)
{
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "\"item\" parameter not supplied");
    if (!supported)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output parameter not supplied");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check operation type */
    switch (opt_type) {
        /* H5Mcreate/create_anon */
        case H5VL_MAP_CREATE: {
            *supported = H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_MODIFY_METADATA;
            break;
        } /* end block */

        /* H5Mopen */
        case H5VL_MAP_OPEN: {
            *supported = H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_QUERY_METADATA;
            break;
        } /* end block */

        /* H5Mget */
        case H5VL_MAP_GET_VAL: {
            *supported =
                H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_READ_DATA | H5_DAOS_OPT_QUERY_NO_ASYNC;
            break;
        } /* end block */

        /* H5Mexists */
        case H5VL_MAP_EXISTS: {
            *supported =
                H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_READ_DATA | H5_DAOS_OPT_QUERY_NO_ASYNC;
            break;
        } /* end block */

        /* H5Mput */
        case H5VL_MAP_PUT: {
            *supported =
                H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_READ_DATA | H5_DAOS_OPT_QUERY_NO_ASYNC;
            break;
        } /* end block */

        /* Operations that get misc info from the map */
        case H5VL_MAP_GET: {
            *supported =
                H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_QUERY_METADATA | H5_DAOS_OPT_QUERY_NO_ASYNC;
            break;
        } /* end block */

        /* Specific operations (H5Miterate and H5Mdelete) */
        case H5VL_MAP_SPECIFIC: {
            *supported = H5_DAOS_OPT_QUERY_SUPPORTED | H5_DAOS_OPT_QUERY_QUERY_METADATA |
                         H5_DAOS_OPT_QUERY_MODIFY_METADATA | H5_DAOS_OPT_QUERY_NO_ASYNC;
            break;
        } /* end block */

        /* H5Mclose */
        case H5VL_MAP_CLOSE: {
            *supported = H5_DAOS_OPT_QUERY_SUPPORTED;
            break;
        } /* end block */

        default: {
            /* Not supported */
            *supported = 0;
            break;
        } /* end block */
    }     /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_opt_query() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_optional
 *
 * Purpose:     Optional VOL callbacks.  Thin switchboard to translate map
 *              object calls to a format analogous to other VOL object
 *              callbacks.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_optional(void *item, H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req)
{
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    /* Check operation type */
    switch (opt_args->op_type) {
        /* H5Mcreate/create_anon */
        case H5VL_MAP_CREATE: {
            H5VL_map_args_t *map_args = (H5VL_map_args_t *)opt_args->args;
            hid_t            lcpl_id  = H5I_INVALID_HID;
            hid_t            ktype_id = H5I_INVALID_HID;
            hid_t            vtype_id = H5I_INVALID_HID;
            hid_t            mcpl_id  = H5I_INVALID_HID;
            hid_t            mapl_id  = H5I_INVALID_HID;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_create. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            lcpl_id  = map_args->create.lcpl_id;
            ktype_id = map_args->create.key_type_id;
            vtype_id = map_args->create.val_type_id;
            mcpl_id  = map_args->create.mcpl_id;
            mapl_id  = map_args->create.mapl_id;

            /* Pass the call */
            if (NULL == (map_args->create.map =
                             H5_daos_map_create(item, &map_args->create.loc_params, map_args->create.name,
                                                lcpl_id, ktype_id, vtype_id, mcpl_id, mapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create map object");

            break;
        } /* end block */

        /* H5Mopen */
        case H5VL_MAP_OPEN: {
            H5VL_map_args_t *map_args = (H5VL_map_args_t *)opt_args->args;
            hid_t            mapl_id  = H5I_INVALID_HID;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_open. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            mapl_id = map_args->open.mapl_id;

            /* Pass the call */
            if (NULL == (map_args->open.map = H5_daos_map_open(item, &map_args->open.loc_params,
                                                               map_args->open.name, mapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "can't open map object");

            break;
        } /* end block */

        /* H5Mget */
        case H5VL_MAP_GET_VAL: {
            H5VL_map_args_t *map_args        = (H5VL_map_args_t *)opt_args->args;
            hid_t            key_mem_type_id = H5I_INVALID_HID;
            hid_t            val_mem_type_id = H5I_INVALID_HID;
            void            *value           = NULL;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_get_val. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            key_mem_type_id = map_args->get_val.key_mem_type_id;
            val_mem_type_id = map_args->get_val.value_mem_type_id;
            value           = map_args->get_val.value;

            /* Pass the call */
            if ((ret_value = H5_daos_map_get_val(item, key_mem_type_id, map_args->get_val.key,
                                                 val_mem_type_id, value, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_READERROR, ret_value, "can't get value");

            break;
        } /* end block */

        /* H5Mexists */
        case H5VL_MAP_EXISTS: {
            H5VL_map_args_t *map_args        = (H5VL_map_args_t *)opt_args->args;
            hid_t            key_mem_type_id = H5I_INVALID_HID;
            hbool_t         *exists          = NULL;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_exists. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            key_mem_type_id = map_args->exists.key_mem_type_id;
            exists          = &map_args->exists.exists;

            /* Pass the call */
            if ((ret_value = H5_daos_map_exists(item, key_mem_type_id, map_args->exists.key, exists, dxpl_id,
                                                req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_READERROR, ret_value, "can't check if value exists");

            break;
        } /* end block */

        /* H5Mput */
        case H5VL_MAP_PUT: {
            H5VL_map_args_t *map_args        = (H5VL_map_args_t *)opt_args->args;
            hid_t            key_mem_type_id = H5I_INVALID_HID;
            hid_t            val_mem_type_id = H5I_INVALID_HID;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_put. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            key_mem_type_id = map_args->put.key_mem_type_id;
            val_mem_type_id = map_args->put.value_mem_type_id;

            /* Pass the call */
            if ((ret_value = H5_daos_map_put(item, key_mem_type_id, map_args->put.key, val_mem_type_id,
                                             map_args->put.value, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_WRITEERROR, ret_value, "can't put value");

            break;
        } /* end block */

        /* Operations that get misc info from the map */
        case H5VL_MAP_GET: {
            H5VL_map_args_t *map_args = (H5VL_map_args_t *)opt_args->args;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_get. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            /* Pass the call */
            if ((ret_value = H5_daos_map_get(item, map_args, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, ret_value, "can't perform map get operation");

            break;
        } /* end block */

        /* Specific operations (H5Miterate and H5Mdelete) */
        case H5VL_MAP_SPECIFIC: {
            H5VL_map_args_t *map_args = (H5VL_map_args_t *)opt_args->args;

            /* Check operation arguments pointer.  All other arguments will
             * be checked by H5_daos_map_specific. */
            if (!map_args)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

            /* Pass the call */
            if ((ret_value = H5_daos_map_specific(item, map_args, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret_value, "can't perform specific map operation");

            break;
        } /* end block */

        /* H5Mclose */
        case H5VL_MAP_CLOSE: {
            /* Pass the call */
            if ((ret_value = H5_daos_map_close(item, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, ret_value, "can't close map object");

            break;
        } /* end block */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported optional operation");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_generate
 *
 * Purpose:     Generates a unique 64 bit object index.  This index will be
 *              used as the lower 64 bits of a DAOS object ID. If
 *              necessary, this routine creates a task to allocate
 *              additional object indices for the given container before
 *              generating the object index that is returned.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_oidx_generate(uint64_t *oidx, H5_daos_file_t *file, hbool_t collective, H5_daos_req_t *req,
                      tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_oidx_generate_ud_t *generate_udata = NULL;
    tse_task_t                 *generate_task  = NULL;
    uint64_t                   *next_oidx      = collective ? &file->next_oidx_collective : &file->next_oidx;
    uint64_t                   *max_oidx       = collective ? &file->max_oidx_collective : &file->max_oidx;
    int                         ret;
    herr_t                      ret_value = SUCCEED;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate more object indices for this process if necessary */
    if ((*max_oidx == 0) || (*next_oidx > *max_oidx)) {
        /* Check if this process should allocate object IDs or just wait for the
         * result from the leader process */
        if (!collective || (file->my_rank == 0)) {
            /* Set private data for OIDX generation task */
            if (NULL == (generate_udata =
                             (H5_daos_oidx_generate_ud_t *)DV_malloc(sizeof(H5_daos_oidx_generate_ud_t))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                             "can't allocate user data struct for OIDX generation task");
            generate_udata->generic_ud.req       = req;
            generate_udata->generic_ud.task_name = "OIDX generation";
            generate_udata->file                 = file;
            generate_udata->collective           = collective;
            generate_udata->oidx_out             = oidx;
            generate_udata->next_oidx            = next_oidx;
            generate_udata->max_oidx             = max_oidx;

            /* Create task to allocate oidxs */
            if (H5_daos_create_daos_task(DAOS_OPC_CONT_ALLOC_OIDS, *dep_task ? 1 : 0,
                                         *dep_task ? dep_task : NULL, H5_daos_oidx_generate_prep_cb,
                                         H5_daos_oidx_generate_comp_cb, generate_udata, &generate_task) < 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to generate OIDXs");

            /* Schedule OIDX generation task (or save it to be scheduled later) and give it
             * a reference to req */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(generate_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to generate OIDXs: %s",
                                 H5_daos_err_to_string(ret));
            }
            else
                *first_task = generate_task;
            req->rc++;
            file->item.rc++;

            /* Relinquish control of the OIDX generation udata to the
             * task's completion callback */
            generate_udata = NULL;

            *dep_task = generate_task;
        } /* end if */

        /* Broadcast next_oidx if there are other processes that need it */
        if (collective && (file->num_procs > 1) &&
            H5_daos_oidx_bcast(file, oidx, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't broadcast next object index");
    } /* end if */
    else {
        /* Allocate oidx from local allocation */
        H5_DAOS_ALLOCATE_NEXT_OIDX(oidx, next_oidx, max_oidx);
    }

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        generate_udata = DV_free(generate_udata);
    }

    /* Make sure we cleaned up */
    assert(!generate_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_oidx_generate() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_generate_prep_cb
 *
 * Purpose:     Prepare callback for DAOS OIDX generation task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_oidx_generate_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_oidx_generate_ud_t *udata;
    daos_cont_alloc_oids_t     *alloc_args;
    int                         ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for generic task");

    assert(udata->generic_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->generic_ud.req, H5E_VOL);

    assert(udata->generic_ud.req->file);
    assert(udata->generic_ud.req->file->item.open_req);

    /* Verify file was successfully opened */
    if (udata->generic_ud.req->file->item.open_req->status != 0)
        D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, -H5_DAOS_PREREQ_ERROR, "file open is incomplete");

    /* Set arguments for OIDX generation */
    if (NULL == (alloc_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for OIDX generation task");
    alloc_args->coh      = udata->generic_ud.req->file->coh;
    alloc_args->num_oids = H5_DAOS_OIDX_NALLOC;
    alloc_args->oid      = udata->next_oidx;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_oidx_generate_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_generate_comp_cb
 *
 * Purpose:     Complete callback for the DAOS OIDX generation task. When
 *              H5_daos_oidx_generate is called independently, this
 *              callback is responsible for updating the current process'
 *              file's max_oidx and next_oidx fields and "allocating" the
 *              actually returned next oidx. When H5_daos_oidx_generate is
 *              called collectively, the completion callback for the
 *              ensuing oidx broadcast task will be responsible for these
 *              tasks instead.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_oidx_generate_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_oidx_generate_ud_t *udata;
    uint64_t                   *next_oidx;
    uint64_t                   *max_oidx;
    int                         ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for OIDX generation task");

    assert(udata->file);

    /* Handle errors in OIDX generation task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->generic_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->generic_ud.req->status      = task->dt_result;
        udata->generic_ud.req->failed_task = udata->generic_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        next_oidx = udata->next_oidx;
        max_oidx  = udata->max_oidx;

        /* If H5_daos_oidx_generate was called independently, it is
         * safe to update the file's max and next OIDX fields and
         * allocate the next OIDX. Otherwise, this must be delayed
         * until after the next OIDX value has been broadcasted to
         * the other ranks.
         */
        if (!udata->collective || (udata->generic_ud.req->file->num_procs == 1)) {
            /* Adjust the max and next OIDX values for the file on this process */
            H5_DAOS_ADJUST_MAX_AND_NEXT_OIDX(next_oidx, max_oidx);

            /* Allocate oidx from local allocation */
            H5_DAOS_ALLOCATE_NEXT_OIDX(udata->oidx_out, next_oidx, max_oidx);
        }
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        /* Release our reference on the file */
        if (H5_daos_file_close_helper(udata->file) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close file");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->generic_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->generic_ud.req->status      = ret_value;
            udata->generic_ud.req->failed_task = udata->generic_ud.task_name;
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->generic_ud.req) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_oidx_generate_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_bcast
 *
 * Purpose:     Creates an asynchronous task for broadcasting the next OIDX
 *              value after rank 0 has allocated more from DAOS.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_oidx_bcast(H5_daos_file_t *file, uint64_t *oidx_out, H5_daos_req_t *req, tse_task_t **first_task,
                   tse_task_t **dep_task)
{
    H5_daos_oidx_bcast_ud_t *oidx_bcast_udata = NULL;
    tse_task_t              *bcast_task       = NULL;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    assert(file);
    assert(oidx_out);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if (NULL == (oidx_bcast_udata = (H5_daos_oidx_bcast_ud_t *)DV_malloc(sizeof(H5_daos_oidx_bcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "failed to allocate buffer for MPI broadcast user data");
    oidx_bcast_udata->bcast_udata.req            = req;
    oidx_bcast_udata->bcast_udata.obj            = NULL;
    oidx_bcast_udata->bcast_udata.bcast_metatask = NULL;
    oidx_bcast_udata->bcast_udata.buffer         = oidx_bcast_udata->next_oidx_buf;
    oidx_bcast_udata->bcast_udata.buffer_len     = H5_DAOS_ENCODED_UINT64_T_SIZE;
    oidx_bcast_udata->bcast_udata.count          = H5_DAOS_ENCODED_UINT64_T_SIZE;
    oidx_bcast_udata->bcast_udata.comm           = req->file->comm;
    oidx_bcast_udata->file                       = file;
    oidx_bcast_udata->oidx_out                   = oidx_out;
    oidx_bcast_udata->next_oidx                  = &file->next_oidx_collective;
    oidx_bcast_udata->max_oidx                   = &file->max_oidx_collective;

    /* Create task for broadcast */
    if (H5_daos_create_task(H5_daos_mpi_ibcast_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            (file->my_rank == 0) ? H5_daos_oidx_bcast_prep_cb : NULL,
                            H5_daos_oidx_bcast_comp_cb, oidx_bcast_udata, &bcast_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to broadcast next object index");

    /* Schedule OIDX broadcast task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(bcast_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL,
                         "can't schedule task to broadcast next object index: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = bcast_task;
    req->rc++;
    file->item.rc++;

    /* Relinquish control of the OIDX broadcast udata to the
     * task's completion callback */
    oidx_bcast_udata = NULL;

    *dep_task = bcast_task;

done:
    /* Cleanup on failure */
    if (oidx_bcast_udata) {
        assert(ret_value < 0);
        oidx_bcast_udata = DV_free(oidx_bcast_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_oidx_bcast() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_bcast_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous OIDX broadcasts.
 *              Currently checks for errors from previous tasks and then
 *              encodes the OIDX value into the broadcast buffer before
 *              sending. Meant only to be called by rank 0.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_oidx_bcast_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_oidx_bcast_ud_t *udata;
    uint8_t                 *p;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object index broadcast task");

    assert(udata->bcast_udata.req);
    assert(udata->bcast_udata.buffer);
    assert(udata->next_oidx);
    assert(H5_DAOS_ENCODED_UINT64_T_SIZE == udata->bcast_udata.buffer_len);
    assert(H5_DAOS_ENCODED_UINT64_T_SIZE == udata->bcast_udata.count);

    /* Note that we do not handle errors from a previous task here.
     * The broadcast must still proceed on all ranks even if a
     * previous task has failed.
     */

    p = udata->bcast_udata.buffer;
    UINT64ENCODE(p, *udata->next_oidx);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_oidx_bcast_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous OIDX broadcasts.
 *              Currently checks for a failed task, then performs the
 *              following in order:
 *
 *              - decodes the sent OIDX buffer on the ranks that are
 *                receiving it
 *              - adjusts the max OIDX and next OIDX fields in the file on
 *                all ranks
 *              - allocates the next OIDX value on all ranks
 *              - frees private data
 *
 *              Meant to be called by all ranks.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_oidx_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_oidx_bcast_ud_t *udata;
    uint64_t                *next_oidx;
    uint64_t                *max_oidx;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object index broadcast task");

    assert(udata->bcast_udata.req);
    assert(udata->bcast_udata.buffer);
    assert(udata->file);
    assert(udata->oidx_out);
    assert(udata->next_oidx);
    assert(udata->max_oidx);
    assert(H5_DAOS_ENCODED_UINT64_T_SIZE == udata->bcast_udata.buffer_len);
    assert(H5_DAOS_ENCODED_UINT64_T_SIZE == udata->bcast_udata.count);

    /* Handle errors in OIDX broadcast task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast next object index";
    } /* end if */
    else if (task->dt_result == 0) {
        next_oidx = udata->next_oidx;
        max_oidx  = udata->max_oidx;

        /* Decode sent OIDX on receiving ranks */
        if (udata->bcast_udata.req->file->my_rank != 0) {
            uint8_t *p = udata->bcast_udata.buffer;
            UINT64DECODE(p, *next_oidx);
        }

        /* Adjust the max and next OIDX values for the file on this process */
        H5_DAOS_ADJUST_MAX_AND_NEXT_OIDX(next_oidx, max_oidx);

        /* Allocate oidx from local allocation */
        H5_DAOS_ALLOCATE_NEXT_OIDX(udata->oidx_out, next_oidx, max_oidx);
    } /* end else */

done:
    if (udata) {
        /* Release our reference on the file */
        if (H5_daos_file_close_helper(udata->file) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close file");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast next object index completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_oidx_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oid_encode
 *
 * Purpose:     Creates a DAOS OID given the object type and a 64 bit
 *              object index.  Note that `file` must have at least the
 *              default_object_class field set, but may be otherwise
 *              uninitialized.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_oid_encode(daos_obj_id_t *oid, uint64_t oidx, H5I_type_t obj_type, hid_t crt_plist_id,
                   const char *oclass_prop_name, H5_daos_file_t *file)
{
    daos_oclass_id_t object_class = OC_UNKNOWN;
#if CHECK_DAOS_API_VERSION(2, 0)
    enum daos_otype_t object_type;
#else
    daos_ofeat_t object_feats;
#endif
    herr_t ret_value = SUCCEED;

    /* Initialize oid.lo to oidx */
    oid->lo = oidx;

    /* Set type bits in the upper 2 bits of the lower 32 of oid.hi (for
     * simplicity so they're in the same location as in the compacted haddr_t
     * form) */
    if (obj_type == H5I_GROUP)
        oid->hi = H5_DAOS_TYPE_GRP;
    else if (obj_type == H5I_DATASET)
        oid->hi = H5_DAOS_TYPE_DSET;
    else if (obj_type == H5I_DATATYPE)
        oid->hi = H5_DAOS_TYPE_DTYPE;
    else {
        assert(obj_type == H5I_MAP);
        oid->hi = H5_DAOS_TYPE_MAP;
    } /* end else */

    /* Set the object feature flags */
    if (H5I_GROUP == obj_type)
#if CHECK_DAOS_API_VERSION(2, 0)
        object_type = DAOS_OT_MULTI_LEXICAL;
    else
        object_type = DAOS_OT_AKEY_LEXICAL;
#else
        object_feats = DAOS_OF_DKEY_LEXICAL | DAOS_OF_AKEY_LEXICAL;
    else
        object_feats = DAOS_OF_DKEY_HASHED | DAOS_OF_AKEY_LEXICAL;
#endif

    /* Check for object class set on crt_plist_id */
    /* Note we do not copy the oclass_str in the property callbacks (there is no
     * "get" callback, so this is more like an H5P_peek, and we do not need to
     * free oclass_str as it points directly into the plist value */
    if (crt_plist_id != H5P_DEFAULT) {
        htri_t prop_exists;

        if ((prop_exists = H5Pexist(crt_plist_id, oclass_prop_name)) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");
        if (prop_exists) {
            char *oclass_str = NULL;

            if (H5Pget(crt_plist_id, oclass_prop_name, &oclass_str) < 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class");
            if (oclass_str && (oclass_str[0] != '\0'))
                if (OC_UNKNOWN == (object_class = (daos_oclass_id_t)daos_oclass_name2id(oclass_str)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unknown object class");
        } /* end if */
    }     /* end if */

    /* Check for object class set on file if not set from plist */
    if (object_class == OC_UNKNOWN)
        object_class = file->fapl_cache.default_object_class;

    /* Generate oid */
    daos_oclass_hints_t hints = 0;

    /** if user does not set default object class, use DAOS default, but set a large oclass for
     * datasets, and small for other object types */
    if (object_class == OC_UNKNOWN) {
        if (obj_type == H5I_DATASET)
            hints = DAOS_OCH_SHD_MAX;
        else
            hints = DAOS_OCH_SHD_DEF;
    }
#if CHECK_DAOS_API_VERSION(2, 0)
    if (daos_obj_generate_oid(file->coh, oid, object_type, object_class, hints, 0) != 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "Can't set object class");
#else
    if (daos_obj_generate_oid(file->coh, oid, object_feats, object_class, hints, 0) != 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "Can't set object class");
#endif

done:
    D_FUNC_LEAVE;
} /* end H5_daos_oid_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oid_encode_task
 *
 * Purpose:     Asynchronous task for calling H5_daos_oid_encode.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_oid_encode_task(tse_task_t *task)
{
    H5_daos_oid_encode_ud_t *udata     = NULL;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for OID encoding task");

    assert(udata->req);
    assert(udata->oid_out);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_VOL);

    if (H5_daos_oid_encode(udata->oid_out, udata->oidx, udata->obj_type, udata->crt_plist_id,
                           udata->oclass_prop_name, udata->req->file) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTENCODE, -H5_DAOS_H5_ENCODE_ERROR, "can't encode object ID");

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        if (H5P_DEFAULT != udata->crt_plist_id)
            if (H5Idec_ref(udata->crt_plist_id) < 0)
                D_DONE_ERROR(H5E_PLIST, H5E_CANTDEC, -H5_DAOS_H5_CLOSE_ERROR,
                             "can't decrement ref. count on creation plist");

        /* Release our reference on the file */
        if (H5_daos_file_close_helper(udata->file) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close file");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "OID encoding task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_oid_encode_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oid_generate
 *
 * Purpose:     Generate a DAOS OID given the object type and file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_oid_generate(daos_obj_id_t *oid, hbool_t oidx_set, uint64_t oidx, H5I_type_t obj_type,
                     hid_t crt_plist_id, const char *oclass_prop_name, H5_daos_file_t *file,
                     hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_oid_encode_ud_t *encode_udata = NULL;
    tse_task_t              *encode_task  = NULL;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up user data for OID encoding */
    if (NULL == (encode_udata = (H5_daos_oid_encode_ud_t *)DV_malloc(sizeof(H5_daos_oid_encode_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "failed to allocate buffer for OID encoding user data");
    encode_udata->req     = req;
    encode_udata->oid_out = oid;

    if (oidx_set) {
        encode_udata->oidx = oidx;
    }
    else {
        /* Generate oidx */
        if (H5_daos_oidx_generate(&encode_udata->oidx, file, collective, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't generate object index");
    }

    /* Create asynchronous task for OID encoding */
    encode_udata->file             = file;
    encode_udata->obj_type         = obj_type;
    encode_udata->crt_plist_id     = crt_plist_id;
    encode_udata->oclass_prop_name = oclass_prop_name;

    /* Create task to encode OID */
    if (H5_daos_create_task(H5_daos_oid_encode_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, encode_udata, &encode_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to encode OID");

    /* Schedule OID encoding task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(encode_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to encode OID: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = encode_task;
    req->rc++;
    file->item.rc++;

    /* Relinquish control of the OID encoding udata to the task's completion callback */
    encode_udata = NULL;

    if (H5P_DEFAULT != crt_plist_id)
        if (H5Iinc_ref(crt_plist_id) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTINC, FAIL, "can't increment ref. count on creation plist");

    *dep_task = encode_task;

done:
    encode_udata = DV_free(encode_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_oid_generate() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oid_to_token
 *
 * Purpose:     Converts an OID to an object "token".
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_oid_to_token(daos_obj_id_t oid, H5O_token_t *obj_token)
{
    uint8_t *p;
    herr_t   ret_value = SUCCEED;

    assert(obj_token);
    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE <= H5O_MAX_TOKEN_SIZE);

    p = (uint8_t *)obj_token;

    UINT64ENCODE(p, oid.lo);
    UINT64ENCODE(p, oid.hi);

    D_FUNC_LEAVE;
} /* end H5_daos_oid_to_token() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_token_to_oid
 *
 * Purpose:     Converts an object "token" to an OID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_token_to_oid(const H5O_token_t *obj_token, daos_obj_id_t *oid)
{
    const uint8_t *p;
    herr_t         ret_value = SUCCEED;

    assert(obj_token);
    assert(oid);
    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE <= H5O_MAX_TOKEN_SIZE);

    p = (const uint8_t *)obj_token;

    UINT64DECODE(p, oid->lo);
    UINT64DECODE(p, oid->hi);

    D_FUNC_LEAVE;
} /* end H5_daos_token_to_oid() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_oid_to_type
 *
 * Purpose:     Retrieve the HDF5 object type from an OID
 *
 * Return:      Success:    Object type
 *              Failure:    H5I_BADID
 *
 *-------------------------------------------------------------------------
 */
H5I_type_t
H5_daos_oid_to_type(daos_obj_id_t oid)
{
    uint64_t type_bits;

    /* Retrieve type */
    type_bits = oid.hi & H5_DAOS_TYPE_MASK;
    if (type_bits == H5_DAOS_TYPE_GRP)
        return (H5I_GROUP);
    else if (type_bits == H5_DAOS_TYPE_DSET)
        return (H5I_DATASET);
    else if (type_bits == H5_DAOS_TYPE_DTYPE)
        return (H5I_DATATYPE);
    else if (type_bits == H5_DAOS_TYPE_MAP)
        return (H5I_MAP);
    else
        return (H5I_BADID);
} /* end H5_daos_oid_to_type() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_mult128
 *
 * Purpose:     Multiply two 128 bit unsigned integers to yield a 128 bit
 *              unsigned integer
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static void
H5_daos_mult128(uint64_t x_lo, uint64_t x_hi, uint64_t y_lo, uint64_t y_hi, uint64_t *ans_lo,
                uint64_t *ans_hi)
{
    uint64_t xlyl;
    uint64_t xlyh;
    uint64_t xhyl;
    uint64_t xhyh;
    uint64_t temp;

    assert(ans_lo);
    assert(ans_hi);

    /*
     * First calculate x_lo * y_lo
     */
    /* Compute 64 bit results of multiplication of each combination of high and
     * low 32 bit sections of x_lo and y_lo */
    xlyl = (x_lo & 0xffffffff) * (y_lo & 0xffffffff);
    xlyh = (x_lo & 0xffffffff) * (y_lo >> 32);
    xhyl = (x_lo >> 32) * (y_lo & 0xffffffff);
    xhyh = (x_lo >> 32) * (y_lo >> 32);

    /* Calculate lower 32 bits of the answer */
    *ans_lo = xlyl & 0xffffffff;

    /* Calculate second 32 bits of the answer. Use temp to keep a 64 bit result
     * of the calculation for these 32 bits, to keep track of overflow past
     * these 32 bits. */
    temp = (xlyl >> 32) + (xlyh & 0xffffffff) + (xhyl & 0xffffffff);
    *ans_lo += temp << 32;

    /* Calculate third 32 bits of the answer, including overflowed result from
     * the previous operation */
    temp >>= 32;
    temp += (xlyh >> 32) + (xhyl >> 32) + (xhyh & 0xffffffff);
    *ans_hi = temp & 0xffffffff;

    /* Calculate highest 32 bits of the answer. No need to keep track of
     * overflow because it has overflowed past the end of the 128 bit answer */
    temp >>= 32;
    temp += (xhyh >> 32);
    *ans_hi += temp << 32;

    /*
     * Now add the results from multiplying x_lo * y_hi and x_hi * y_lo. No need
     * to consider overflow here, and no need to consider x_hi * y_hi because
     * those results would overflow past the end of the 128 bit answer.
     */
    *ans_hi += (x_lo * y_hi) + (x_hi * y_lo);

    return;
} /* end H5_daos_mult128() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_hash128
 *
 * Purpose:     Hashes the string name to a 128 bit buffer (hash).
 *              Implementation of the FNV hash algorithm.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_hash128(const char *name, void *hash)
{
    const uint8_t *name_p = (const uint8_t *)name;
    uint8_t       *hash_p = (uint8_t *)hash;
    uint64_t       name_lo;
    uint64_t       name_hi;
    /* Initialize hash value in accordance with the FNV algorithm */
    uint64_t hash_lo = 0x62b821756295c58d;
    uint64_t hash_hi = 0x6c62272e07bb0142;
    /* Initialize FNV prime number in accordance with the FNV algorithm */
    const uint64_t fnv_prime_lo = 0x13b;
    const uint64_t fnv_prime_hi = 0x1000000;
    size_t         name_len_rem;

    assert(name);
    assert(hash);

    name_len_rem = strlen(name);

    while (name_len_rem > 0) {
        /* "Decode" lower 64 bits of this 128 bit section of the name, so the
         * numberical value of the integer is the same on both little endian and
         * big endian systems */
        if (name_len_rem >= 8) {
            UINT64DECODE(name_p, name_lo)
            name_len_rem -= 8;
        } /* end if */
        else {
            name_lo = 0;
            UINT64DECODE_VAR(name_p, name_lo, name_len_rem)
            name_len_rem = 0;
        } /* end else */

        /* "Decode" second 64 bits */
        if (name_len_rem > 0) {
            if (name_len_rem >= 8) {
                UINT64DECODE(name_p, name_hi)
                name_len_rem -= 8;
            } /* end if */
            else {
                name_hi = 0;
                UINT64DECODE_VAR(name_p, name_hi, name_len_rem)
                name_len_rem = 0;
            } /* end else */
        }     /* end if */
        else
            name_hi = 0;

        /* FNV algorithm - XOR hash with name then multiply by fnv_prime */
        hash_lo ^= name_lo;
        hash_hi ^= name_hi;
        H5_daos_mult128(hash_lo, hash_hi, fnv_prime_lo, fnv_prime_hi, &hash_lo, &hash_hi);
    } /* end while */

    /* "Encode" hash integers to char buffer, so the buffer is the same on both
     * little endian and big endian systems */
    UINT64ENCODE(hash_p, hash_lo)
    UINT64ENCODE(hash_p, hash_hi)

    return;
} /* end H5_daos_hash128() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_tx_comp_cb
 *
 * Purpose:     Callback for daos_tx_commit()/abort() which closes the
 *              transaction.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_tx_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_req_t *req;
    int            ret;
    int            ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (req = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for transaction commit/abort task");

    /* Handle errors in commit/abort task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = task->dt_result;
        req->failed_task = "transaction commit/abort";
    } /* end if */

    /* Close transaction */
    if (0 != (ret = daos_tx_close(req->th, NULL /*event*/)))
        D_GOTO_ERROR(H5E_IO, H5E_CLOSEERROR, ret, "can't close transaction: %s", H5_daos_err_to_string(ret));
    req->th_open = FALSE;

done:
    /* Make notify callback */
    if (req->notify_cb) {
        H5_DAOS_REQ_STATUS_OUT_TYPE req_status;

        /* Determine request status */
        if (ret_value >= 0 && (req->status == -H5_DAOS_INCOMPLETE || req->status == -H5_DAOS_SHORT_CIRCUIT))
            req_status = H5_DAOS_REQ_STATUS_OUT_SUCCEED;
        else if (req->status == -H5_DAOS_CANCELED)
            req_status = H5_DAOS_REQ_STATUS_OUT_CANCELED;
        else
            req_status = H5_DAOS_REQ_STATUS_OUT_FAIL;

        /* Make callback */
        if (req->notify_cb(req->notify_ctx, req_status) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CANTOPERATE, -H5_DAOS_CALLBACK_ERROR,
                         "notify callback returned failure");
    } /* end if */

    /* Clear H5_daos_collective_req_tail if it refers to this request */
    if (H5_daos_collective_req_tail == req)
        H5_daos_collective_req_tail = NULL;

    /* Mark request as completed */
    if (ret_value >= 0 && (req->status == -H5_DAOS_INCOMPLETE || req->status == -H5_DAOS_SHORT_CIRCUIT))
        req->status = 0;

    /* Complete finalize task in engine */
    tse_task_complete(req->finalize_task, req->status);
    req->finalize_task = NULL;

    /* Complete dep_task.  Always succeeds since the purpose of dep_task is to
     * prevent errors from propagating. */
    if (req->dep_task) {
        tse_task_complete(req->dep_task, 0);
        req->dep_task = NULL;
    } /* end if */

    /* Propagate errors to parent request */
    if (req->parent_req && req->status != 0) {
        req->parent_req->status      = req->status;
        req->parent_req->failed_task = req->failed_task;
    } /* end else */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = ret_value;
        req->failed_task = "transaction commit/abort completion callback";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_tx_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_h5op_finalize
 *
 * Purpose:     Task function which is called when an HDF5 operation is
 *              complete.  Commits the transaction if one was opened for
 *              the operation, then releases its reference to req.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_h5op_finalize(tse_task_t *task)
{
    H5_daos_req_t *req;
    hbool_t        close_tx = FALSE;
    int            ret;
    int            ret_value = 0;

    /* Get private data */
    if (NULL == (req = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for operation finalize task");
    assert(task == req->finalize_task);

    /* Check for error */
    if (req->status < -H5_DAOS_SHORT_CIRCUIT) {
        /* Print error message (unless the operation was canceled) */
        if (req->status < -H5_DAOS_CANCELED)
            D_DONE_ERROR(H5E_IO, H5E_CANTINIT, req->status, "operation \"%s\" failed in task \"%s\": %s",
                         req->op_name, req->failed_task, H5_daos_err_to_string(req->status));

        /* Abort transaction if opened */
        if (req->th_open) {
            tse_task_t      *abort_task;
            daos_tx_abort_t *abort_args;

            /* Create task */
            if (H5_daos_create_daos_task(DAOS_OPC_TX_ABORT, 0, NULL, NULL, H5_daos_tx_comp_cb, req,
                                         &abort_task) < 0) {
                close_tx     = TRUE;
                req->th_open = FALSE;
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task to abort transaction");
            } /* end if */

            /* Set arguments */
            if (NULL == (abort_args = daos_task_get_args(abort_task))) {
                close_tx     = TRUE;
                req->th_open = FALSE;
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                             "can't get arguments for transaction abort task");
            } /* end if */
            abort_args->th = req->th;

            /* Schedule abort task */
            if (0 != (ret = tse_task_schedule(abort_task, false))) {
                close_tx     = TRUE;
                req->th_open = FALSE;
                tse_task_complete(abort_task, ret_value);
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't schedule task to abort transaction: %s",
                             H5_daos_err_to_string(ret));
            } /* end if */
            req->rc++;
        } /* end if */
    }     /* end if */
    else {
        /* Commit transaction if opened */
        if (req->th_open) {
            tse_task_t       *commit_task;
            daos_tx_commit_t *commit_args;

            /* Create task */
            if (H5_daos_create_daos_task(DAOS_OPC_TX_COMMIT, 0, NULL, NULL, H5_daos_tx_comp_cb, req,
                                         &commit_task) < 0) {
                close_tx     = TRUE;
                req->th_open = FALSE;
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task to commit transaction");
            } /* end if */

            /* Set arguments */
            if (NULL == (commit_args = daos_task_get_args(commit_task))) {
                close_tx     = TRUE;
                req->th_open = FALSE;
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                             "can't get arguments for transaction commit task");
            } /* end if */
            commit_args->th    = req->th;
            commit_args->flags = 0;

            /* Schedule commit task */
            if (0 != (ret = tse_task_schedule(commit_task, false))) {
                close_tx     = TRUE;
                req->th_open = FALSE;
                tse_task_complete(commit_task, ret_value);
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't schedule task to commit transaction: %s",
                             H5_daos_err_to_string(ret));
            } /* end if */
            req->rc++;
        } /* end if */
    }     /* end else */

done:
    if (req) {
        /* Check if we failed to start tx commit/abort task */
        if (close_tx) {
            /* Close transaction */
            if (0 != (ret = daos_tx_close(req->th, NULL /*event*/)))
                D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, ret, "can't close transaction: %s",
                             H5_daos_err_to_string(ret));
            req->th_open = FALSE;
        } /* end if */

        /* Check if we're done */
        if (!req->th_open) {
            /* Make notify callback */
            if (req->notify_cb)
                if (req->notify_cb(req->notify_ctx, ret_value >= 0 && (req->status == -H5_DAOS_INCOMPLETE ||
                                                                       req->status == -H5_DAOS_SHORT_CIRCUIT)
                                                        ? H5_DAOS_REQ_STATUS_OUT_SUCCEED
                                                    : req->status == -H5_DAOS_CANCELED
                                                        ? H5_DAOS_REQ_STATUS_OUT_CANCELED
                                                        : H5_DAOS_REQ_STATUS_OUT_FAIL) < 0)
                    D_DONE_ERROR(H5E_VOL, H5E_CANTOPERATE, -H5_DAOS_CALLBACK_ERROR,
                                 "notify callback returned failure");

            /* Clear H5_daos_collective_req_tail if it refers to this request */
            if (H5_daos_collective_req_tail == req)
                H5_daos_collective_req_tail = NULL;

            /* Mark request as completed if there were no errors */
            if (ret_value >= 0 &&
                (req->status == -H5_DAOS_INCOMPLETE || req->status == -H5_DAOS_SHORT_CIRCUIT))
                req->status = 0;

            /* Return task to task list */
            if (H5_daos_task_list_put(H5_daos_task_list_g, req->finalize_task) < 0)
                D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                             "can't return task to task list");

            /* Complete task in engine */
            tse_task_complete(req->finalize_task, req->status);
            req->finalize_task = NULL;

            /* Complete dep_task.  Always succeeds since the purpose of dep_task
             * is to prevent errors from propagating. */
            if (req->dep_task) {
                tse_task_complete(req->dep_task, 0);
                req->dep_task = NULL;
            } /* end if */
        }     /* end if */

        /* Report failures in this routine */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = "h5 op finalize";
        } /* end if */

        /* Propagate errors to parent request */
        if (req->parent_req && req->status != 0 && !req->th_open) {
            req->parent_req->status      = req->status;
            req->parent_req->failed_task = req->failed_task;
        } /* end else */

        /* Release our reference to req */
        if (H5_daos_req_free_int(req) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_h5op_finalize() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_generic_prep_cb
 *
 * Purpose:     Prepare callback for generic DAOS operations.  Currently
 *              only checks for errors from previous tasks.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              February, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_generic_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_generic_cb_ud_t *udata;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for generic task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_VOL);

    assert(udata->req->file);

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_generic_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_generic_comp_cb
 *
 * Purpose:     Complete callback for generic DAOS operations.  Currently
 *              checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              February, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_generic_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_generic_cb_ud_t *udata;
    int                      ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for generic task");

    assert(udata->req);
    assert(udata->req->file || task->dt_result != 0);

    /* Handle errors in task.  Only record error in udata->req_status if it does
     * not already contain an error (it could contain an error if another task
     * this task is not dependent on also failed). */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_generic_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_md_rw_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for metadata I/O.  Currently checks for
 *              errors from previous tasks then sets arguments for the
 *              DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_md_rw_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_md_rw_cb_ud_t *udata;
    daos_obj_rw_t         *update_args;
    int                    ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->req);
    assert(udata->obj);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_VOL);

    assert(udata->obj->item.file);

    /* Set update task arguments */
    if (NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for metadata I/O task");
    memset(update_args, 0, sizeof(*update_args));
    update_args->oh    = udata->obj->obj_oh;
    update_args->th    = udata->req->th;
    update_args->flags = udata->flags;
    update_args->dkey  = &udata->dkey;
    update_args->nr    = udata->nr;
    update_args->iods  = udata->iod;
    update_args->sgls  = udata->sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_md_rw_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_md_update_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update for
 *              metadata writes.  Currently checks for a failed task then
 *              frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_md_update_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_md_rw_cb_ud_t *udata;
    unsigned               i;
    int                    ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */

    /* Close object */
    if (udata->obj && H5_daos_object_close(&udata->obj->item) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = udata->task_name;
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    if (udata->free_dkey)
        DV_free(udata->dkey.iov_buf);
    if (udata->free_akeys)
        for (i = 0; i < udata->nr; i++)
            DV_free(udata->iod[i].iod_name.iov_buf);
    for (i = 0; i < udata->nr; i++)
        if (udata->free_sg_iov[i])
            DV_free(udata->sg_iov[i].iov_buf);
    DV_free(udata);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_md_update_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_mpi_ibcast_task
 *
 * Purpose:     Wraps a call to MPI_Ibcast in a DAOS/TSE task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_mpi_ibcast_task(tse_task_t *task)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int                      ret_value = 0;

    assert(!H5_daos_mpi_task_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for MPI broadcast task");

    assert(udata->req);
    assert(udata->buffer);

    /* Make call to MPI_Ibcast */
    if (MPI_SUCCESS != MPI_Ibcast(udata->buffer, udata->count, MPI_BYTE, 0, udata->comm, &H5_daos_mpi_req_g))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, -H5_DAOS_MPI_ERROR, "MPI_Ibcast failed");

    /* Register this task as the current in-flight MPI task */
    H5_daos_mpi_task_g = task;

    /* This task will be completed by the progress function once that function
     * detects that the MPI request is finished */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_mpi_ibcast_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_mpi_ibarrier_task
 *
 * Purpose:     Wraps a call to MPI_Ibarrier in a DAOS/TSE task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_mpi_ibarrier_task(tse_task_t *task)
{
    H5_daos_req_t *req;
    int            ret_value = 0;

    assert(!H5_daos_mpi_task_g);

    /* Get private data */
    if (NULL == (req = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for MPI barrier task");

    assert(req);
    assert(req->file);

    /* Make call to MPI_Ibarrier */
    if (MPI_SUCCESS != MPI_Ibarrier(req->file->comm, &H5_daos_mpi_req_g))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, -H5_DAOS_MPI_ERROR, "MPI_Ibarrier failed");

    /* Register this task as the current in-flight MPI task */
    H5_daos_mpi_task_g = task;

    /* This task will be completed by the progress function once that function
     * detects that the MPI request is finished */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_mpi_ibarrier_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_metatask_autocomp_other
 *
 * Purpose:     Body function for a metatask that needs to complete
 *              itself and another task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              March, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_metatask_autocomp_other(tse_task_t *task)
{
    tse_task_t *other_task = NULL;
    int         ret_value  = 0;

    /* Get other task */
    if (NULL == (other_task = (tse_task_t *)tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for autocomplete other metatask");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, other_task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete other task */
    tse_task_complete(other_task, ret_value);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_metatask_autocomp_other() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_metatask_autocomplete
 *
 * Purpose:     Body function for a metatask that needs to complete
 *              itself.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              March, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_metatask_autocomplete(tse_task_t *task)
{
    tse_task_complete(task, 0);

    return 0;
} /* end H5_daos_metatask_autocomplete() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_list_key_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos key list
 *              operations.  Currently checks for errors from previous
 *              tasks then sets arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_list_key_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_iter_ud_t *udata;
    daos_obj_list_t   *list_args;
    int                ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for key list task");

    assert(udata->iter_data->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->iter_data->req, H5E_VOL);

    assert(udata->target_obj);
    assert(udata->iter_data->req->file);

    /* Set oh argument */
    if (NULL == (list_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for key list task");
    list_args->oh = udata->target_obj->obj_oh;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_list_key_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_list_key_finish
 *
 * Purpose:     Frees key list udata and, if this is the base level of
 *              iteration, iter data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_list_key_finish(tse_task_t *task)
{
    H5_daos_iter_ud_t *udata;
    H5_daos_req_t     *req = NULL;
    int                ret;
    int                ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for iteration task");

    assert(task == udata->iter_metatask);

    /* Assign req convenience pointer.  We do this so we can still handle errors
     * after freeing.  This should be safe since we don't decrease the ref count
     * on req until we're done with it. */
    req = udata->iter_data->req;

    assert(req);
    assert(req->file);

    /* Finalize iter_data if this is the base of iteration */
    if (udata->base_iter) {
        /* Iteration is complete, we are no longer short-circuiting (if this
         * iteration caused the short circuit) */
        if (udata->iter_data->short_circuit_init) {
            if (udata->iter_data->req->status == -H5_DAOS_SHORT_CIRCUIT)
                udata->iter_data->req->status = -H5_DAOS_INCOMPLETE;
            udata->iter_data->short_circuit_init = FALSE;
        } /* end if */

        /* Decrement reference count on root obj id.  Use nonblocking close so
         * it doesn't deadlock */
        udata->target_obj->item.nonblocking_close = TRUE;
        if ((ret = H5Idec_ref(udata->iter_data->iter_root_obj)) < 0)
            D_DONE_ERROR(H5E_LINK, H5E_CANTDEC, -H5_DAOS_H5_CLOSE_ERROR,
                         "can't decrement reference count on iteration base object");
        if (ret)
            udata->target_obj->item.nonblocking_close = FALSE;
        udata->iter_data->iter_root_obj = H5I_INVALID_HID;

        /* Set *op_ret_p if present */
        if (udata->iter_data->op_ret_p)
            *udata->iter_data->op_ret_p = udata->iter_data->op_ret;

        /* Free hash table */
        if (udata->iter_data->iter_type == H5_DAOS_ITER_TYPE_LINK) {
            udata->iter_data->u.link_iter_data.recursive_link_path =
                DV_free(udata->iter_data->u.link_iter_data.recursive_link_path);

            if (udata->iter_data->u.link_iter_data.visited_link_table) {
                dv_hash_table_free(udata->iter_data->u.link_iter_data.visited_link_table);
                udata->iter_data->u.link_iter_data.visited_link_table = NULL;
            } /* end if */
        }     /* end if */

        /* Free iter data */
        udata->iter_data = DV_free(udata->iter_data);
    } /* end if */
    else
        assert(udata->iter_data->is_recursive);

    /* Close target_obj */
    if (H5_daos_object_close(&udata->target_obj->item) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Free buffer */
    if (udata->sg_iov.iov_buf)
        DV_free(udata->sg_iov.iov_buf);

    /* Free kds buffer if one was allocated */
    if (udata->kds_dyn)
        DV_free(udata->kds_dyn);

    /* Free udata */
    udata = DV_free(udata);

    /* Handle errors */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = ret_value;
        req->failed_task = "key list finish";
    } /* end if */

    /* Release req */
    if (H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Mark task as complete */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_list_key_finish() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_list_key_start
 *
 * Purpose:     Begins listing keys (akeys or dkeys depending on opc)
 *              asynchronously, calling comp_cb when finished.  iter_udata
 *              must already be exist and be filled in with valid info.
 *              Can be used to continue iteration if the first call did
 *              not return all the keys.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_list_key_start(H5_daos_iter_ud_t *iter_udata, daos_opc_t opc, tse_task_cb_t comp_cb,
                       tse_task_t **first_task, tse_task_t **dep_task)
{
    daos_obj_list_t *list_args;
    tse_task_t      *list_task = NULL;
    int              ret;
    int              ret_value = 0;

    assert(iter_udata);
    assert(iter_udata->iter_metatask);
    assert(first_task);
    assert(dep_task);

    /* Create task for key list */
    if (H5_daos_create_daos_task(opc, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_list_key_prep_cb, comp_cb, iter_udata, &list_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't create task to list keys");

    /* Get arguments for list operation */
    if (NULL == (list_args = daos_task_get_args(list_task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for key list task");
    memset(list_args, 0, sizeof(daos_obj_list_t));

    /* Set arguments */
    list_args->th  = iter_udata->iter_data->req->th;
    iter_udata->nr = (uint32_t)iter_udata->kds_len;
    list_args->nr  = &iter_udata->nr;
    list_args->kds = iter_udata->kds;
    list_args->sgl = &iter_udata->sgl;
    if (opc == DAOS_OPC_OBJ_LIST_DKEY)
        list_args->dkey_anchor = &iter_udata->anchor;
    else {
        assert(opc == DAOS_OPC_OBJ_LIST_AKEY);
        list_args->dkey        = &iter_udata->dkey;
        list_args->akey_anchor = &iter_udata->anchor;
    } /* end if */

    /* Schedule list task (or save it to be scheduled later) and give it a
     * reference to req and target_obj */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(list_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule task to list keys: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = list_task;
    *dep_task  = iter_udata->iter_metatask;
    iter_udata = NULL;

done:
    /* Cleanup */
    if (iter_udata) {
        assert(ret_value < 0);
        assert(iter_udata->iter_metatask);
        assert(iter_udata->sg_iov.iov_buf);

        if (*dep_task && 0 != (ret = tse_task_register_deps(iter_udata->iter_metatask, 1, dep_task)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't create dependencies for iteration metatask: %s",
                         H5_daos_err_to_string(ret));

        if (*first_task) {
            if (0 != (ret = tse_task_schedule(iter_udata->iter_metatask, false)))
                D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule iteration metatask: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = iter_udata->iter_metatask;
        *dep_task = iter_udata->iter_metatask;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_list_key_start() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_list_key_init
 *
 * Purpose:     Begins listing keys (akeys or dkeys depending on opc)
 *              asynchronously, calling comp_cb when finished.  Creates a
 *              metatask in the udata struct's "iter_metatask" field but
 *              does not schedule it.  It is the responsibility of comp_cb
 *              to make sure iter_metatask is scheduled such that it
 *              executes when everything is complete a this level of
 *              iteration.
 *
 *              key_prefetch_size specifies the number of keys to fetch at
 *              a time while prefetching keys during the listing operation.
 *              key_buf_size_init specifies the initial size in bytes of
 *              the buffer allocated to hold these keys. This buffer will
 *              be re-allocated as necessary if it is too small to hold the
 *              keys, but this may incur additional I/O overhead.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_list_key_init(H5_daos_iter_data_t *iter_data, H5_daos_obj_t *target_obj, daos_key_t *dkey,
                      daos_opc_t opc, tse_task_cb_t comp_cb, hbool_t base_iter, size_t key_prefetch_size,
                      size_t key_buf_size_init, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_iter_ud_t *iter_udata = NULL;
    char              *tmp_alloc  = NULL;
    int                ret;
    int                ret_value = 0;

    assert(iter_data);
    assert(target_obj);
    assert(comp_cb);
    assert(key_prefetch_size > 0);
    assert(key_buf_size_init > 0);
    assert(first_task);
    assert(dep_task);

    /* Allocate iter udata */
    if (NULL == (iter_udata = (H5_daos_iter_ud_t *)DV_calloc(sizeof(H5_daos_iter_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't allocate iteration user data");

    /* Fill in user data fields */
    iter_udata->target_obj = target_obj;
    if (dkey)
        iter_udata->dkey = *dkey;
    else
        assert(opc == DAOS_OPC_OBJ_LIST_DKEY);
    iter_udata->base_iter = base_iter;
    memset(&iter_udata->anchor, 0, sizeof(iter_udata->anchor));

    /* Copy iter_data if this is the base of iteration, otherwise point to
     * existing iter_data */
    if (base_iter) {
        if (NULL == (iter_udata->iter_data = (H5_daos_iter_data_t *)DV_malloc(sizeof(H5_daos_iter_data_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't allocate iteration data");
        memcpy(iter_udata->iter_data, iter_data, sizeof(*iter_data));
    } /* end if */
    else
        iter_udata->iter_data = iter_data;

    /* Allocate kds buffer if necessary */
    iter_udata->kds     = iter_udata->kds_static;
    iter_udata->kds_len = key_prefetch_size;
    if (key_prefetch_size * sizeof(daos_key_desc_t) > sizeof(iter_udata->kds_static)) {
        if (NULL ==
            (iter_udata->kds_dyn = (daos_key_desc_t *)DV_malloc(key_prefetch_size * sizeof(daos_key_desc_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                         "can't allocate key descriptor buffer");
        iter_udata->kds = iter_udata->kds_dyn;
    } /* end if */

    /* Allocate key_buf */
    if (NULL == (tmp_alloc = (char *)DV_malloc(key_buf_size_init)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't allocate buffer for keys");

    /* Set up sg_iov.  Report size as 1 less than buffer size so we always have
     * room for a null terminator. */
    daos_iov_set(&iter_udata->sg_iov, tmp_alloc, (daos_size_t)(key_buf_size_init - 1));

    /* Set up sgl */
    iter_udata->sgl.sg_nr     = 1;
    iter_udata->sgl.sg_nr_out = 0;
    iter_udata->sgl.sg_iovs   = &iter_udata->sg_iov;

    /* Create meta task for iteration.  This empty task will be completed when
     * the iteration is finished by comp_cb.  We can't use list_task since it
     * may not be completed by the first list.  Only free iter_data at the end
     * if this is the base of iteration. */
    if (H5_daos_create_task(H5_daos_list_key_finish, 0, NULL, NULL, NULL, iter_udata,
                            &iter_udata->iter_metatask) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't create meta task for iteration");

    /* Start list (create tasks) give it a reference to req and target obj, and
     * transfer ownership of iter_udata */
    if (0 != (ret = H5_daos_list_key_start(iter_udata, opc, comp_cb, first_task, dep_task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't start iteration");
    iter_udata->iter_data->req->rc++;
    iter_udata->target_obj->item.rc++;
    iter_udata = NULL;

done:
    /* Cleanup */
    if (iter_udata) {
        assert(ret_value < 0);

        if (iter_udata->iter_metatask) {
            /* The metatask should clean everything up */
            if (iter_udata->iter_metatask != *dep_task) {
                /* Queue up the metatask */
                if (*dep_task && 0 != (ret = tse_task_register_deps(iter_udata->iter_metatask, 1, dep_task)))
                    D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, ret,
                                 "can't create dependencies for iteration metatask: %s",
                                 H5_daos_err_to_string(ret));

                if (*first_task) {
                    if (0 != (ret = tse_task_schedule(iter_udata->iter_metatask, false)))
                        D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule iteration metatask: %s",
                                     H5_daos_err_to_string(ret));
                } /* end if */
                else
                    *first_task = iter_udata->iter_metatask;
                *dep_task = iter_udata->iter_metatask;
            } /* end if */
        }     /* end if */
        else {
            /* No metatask, clean up directly here */
            /* Free iter_data if this is the base of iteration */
            if (iter_data->is_recursive && iter_udata->base_iter) {
                /* Free hash table */
                if (iter_data->iter_type == H5_DAOS_ITER_TYPE_LINK) {
                    iter_data->u.link_iter_data.recursive_link_path =
                        DV_free(iter_data->u.link_iter_data.recursive_link_path);

                    if (iter_data->u.link_iter_data.visited_link_table) {
                        dv_hash_table_free(iter_data->u.link_iter_data.visited_link_table);
                        iter_data->u.link_iter_data.visited_link_table = NULL;
                    } /* end if */
                }     /* end if */

                /* Free iter data */
                iter_udata->iter_data = DV_free(iter_udata->iter_data);
            } /* end if */

            /* Decrement reference count on root obj id.  Use nonblocking close
             * so it doesn't deadlock */
            if (iter_udata->base_iter) {
                iter_udata->target_obj->item.nonblocking_close = TRUE;
                if ((ret = H5Idec_ref(iter_data->iter_root_obj)) < 0)
                    D_DONE_ERROR(H5E_VOL, H5E_CANTDEC, -H5_DAOS_H5_CLOSE_ERROR,
                                 "can't decrement reference count on iteration base object");
                if (ret)
                    iter_udata->target_obj->item.nonblocking_close = FALSE;
            } /* end if */

            /* Free key buffer */
            if (iter_udata->sg_iov.iov_buf)
                DV_free(iter_udata->sg_iov.iov_buf);

            /* Free kds buffer if one was allocated */
            if (iter_udata->kds_dyn)
                DV_free(iter_udata->kds_dyn);

            /* Free udata */
            iter_udata = DV_free(iter_udata);
        } /* end else */
    }     /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_list_key_init() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_open_prep_cb
 *
 * Purpose:     Prepare callback for daos_obj_open.  Currently only sets
 *              the coh and checks for errors from previous tasks.  This
 *              is only necessary for operations that might otherwise be
 *              run before file->coh is set up, since daos_obj_open is a
 *              non-blocking operation.  The other fields in the argument
 *              struct must have already been filled in.  Since this does
 *              not hold the object open it must only be used when there
 *              is a task that depends on it that does so.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              February, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_obj_open_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_obj_open_ud_t *udata;
    daos_obj_open_t       *open_args;
    int                    ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object open task");

    assert(udata->generic_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->generic_ud.req, H5E_VOL);

    assert(udata->file);

    /* Set container open handle and oid in args */
    if (NULL == (open_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for object open task");
    open_args->coh = udata->file->coh;
    open_args->oid = *udata->oid;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_obj_open_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_open
 *
 * Purpose:     Open a DAOS object object asynchronously.  daos_obj_open
 *              is a non-blocking call but it might be necessary to insert
 *              it into the scheduler so it doesn't run until certain
 *              conditions are met (such as the file's container handle
 *              being open).  oid must not point to memory that might be
 *              freed or go out of scope before the open task executes.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_obj_open(H5_daos_file_t *file, H5_daos_req_t *req, daos_obj_id_t *oid, unsigned mode,
                 daos_handle_t *oh, const char *task_name, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t            *open_task;
    H5_daos_obj_open_ud_t *open_udata = NULL;
    daos_obj_open_t       *open_args;
    int                    ret;
    herr_t                 ret_value = SUCCEED; /* Return value */

    assert(file);
    assert(req);
    assert(oid);
    assert(first_task);
    assert(dep_task);

    /* Set private data for object open */
    if (NULL == (open_udata = (H5_daos_obj_open_ud_t *)DV_malloc(sizeof(H5_daos_obj_open_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for object open task");
    open_udata->generic_ud.req       = req;
    open_udata->generic_ud.task_name = task_name;
    open_udata->file                 = file;
    open_udata->oid                  = oid;

    /* Create task for object open */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_OPEN, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_obj_open_prep_cb, H5_daos_generic_comp_cb, open_udata,
                                 &open_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to open object");

    /* Set arguments for object open (oid will be set later by the prep
     * callback) */
    if (NULL == (open_args = daos_task_get_args(open_task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get arguments for object open task");
    open_args->mode = mode;
    open_args->oh   = oh;

    /* Schedule object open task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(open_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to open object: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = open_task;
    req->rc++;
    open_udata = NULL;
    *dep_task  = open_task;

done:
    /* Cleanup */
    if (open_udata) {
        assert(ret_value < 0);
        open_udata = DV_free(open_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_obj_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_mpi_ibcast
 *
 * Purpose:     Creates an asynchronous task for broadcasting a buffer.
 *              `_bcast_udata` may be NULL, in which case this routine will
 *              allocate a broadcast udata struct and assume an empty
 *              buffer is to be sent to trigger a failure on other
 *              processes. If `empty` is TRUE, the buffer will be memset
 *              with 0.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_mpi_ibcast(H5_daos_mpi_ibcast_ud_t *_bcast_udata, H5_daos_obj_t *obj, size_t buffer_size,
                   hbool_t empty, tse_task_cb_t bcast_prep_cb, tse_task_cb_t bcast_comp_cb,
                   H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_t *bcast_udata = _bcast_udata;
    H5_daos_item_t          *item        = (H5_daos_item_t *)obj;
    tse_task_t              *bcast_task;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate bcast_udata if necessary */
    if (!bcast_udata) {
        if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_calloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->req  = req;
        bcast_udata->obj  = obj;
        bcast_udata->comm = req->file->comm;
    } /* end if */

    /* Allocate bcast_udata's buffer if necessary */
    if (!bcast_udata->buffer) {
        if (NULL == (bcast_udata->buffer = DV_calloc(buffer_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate MPI broadcast buffer");
        bcast_udata->buffer_len = (int)buffer_size;
        bcast_udata->count      = (int)buffer_size;
    } /* end if */
    else {
        assert(bcast_udata->buffer_len == (int)buffer_size);
        assert(bcast_udata->count == (int)buffer_size);
        if (empty)
            memset(bcast_udata->buffer, 0, buffer_size);
    } /* end else */

    /* Create meta task for bcast.  This empty task will be completed when
     * the bcast is finished by the completion callback. We can't use
     * bcast_task since it may not be completed after the first bcast. */
    if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &bcast_udata->bcast_metatask) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create meta task for empty buffer broadcast");

    /* Create task for bcast */
    if (H5_daos_create_task(H5_daos_mpi_ibcast_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            bcast_prep_cb, bcast_comp_cb, bcast_udata, &bcast_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to broadcast empty buffer");

    /* Schedule meta task */
    if (0 != (ret = tse_task_schedule(bcast_udata->bcast_metatask, false)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule meta task for empty buffer broadcast: %s",
                     H5_daos_err_to_string(ret));

    /* Schedule bcast task and transfer ownership of bcast_udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(bcast_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task for empty buffer broadcast: %s",
                         H5_daos_err_to_string(ret));
        else {
            req->rc++;
            if (item)
                item->rc++;
            *dep_task   = bcast_udata->bcast_metatask;
            bcast_udata = NULL;
        } /* end else */
    }     /* end if */
    else {
        *first_task = bcast_task;
        req->rc++;
        if (item)
            item->rc++;
        *dep_task   = bcast_udata->bcast_metatask;
        bcast_udata = NULL;
    } /* end else */

done:
    /* Cleanup on failure */
    if (bcast_udata) {
        DV_free(bcast_udata->buffer);
        DV_free(bcast_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_mpi_ibcast() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_collective_error_check
 *
 * Purpose:     Creates an asynchronous task for broadcasting the status of
 *              a collective asynchronous operation. `_bcast_udata` may be
 *              NULL, in which case this routine will allocate a broadcast
 *              udata struct and assume an empty buffer is to be sent to
 *              trigger a failure on other processes.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_collective_error_check(H5_daos_obj_t *obj, H5_daos_req_t *req, tse_task_t **first_task,
                               tse_task_t **dep_task)
{
    herr_t ret_value = SUCCEED;

    assert(req);
    assert(req->file->num_procs > 1);
    assert(first_task);
    assert(dep_task);

    /* Setup the request's bcast udata structure for broadcasting the operation status */
    req->collective.coll_status                 = 0;
    req->collective.err_check_ud.req            = req;
    req->collective.err_check_ud.obj            = obj;
    req->collective.err_check_ud.buffer         = &req->collective.coll_status;
    req->collective.err_check_ud.buffer_len     = sizeof(req->collective.coll_status);
    req->collective.err_check_ud.count          = req->collective.err_check_ud.buffer_len;
    req->collective.err_check_ud.bcast_metatask = NULL;
    req->collective.err_check_ud.comm           = req->file->comm;

    if (H5_daos_mpi_ibcast(&req->collective.err_check_ud, obj, sizeof(req->collective.coll_status), FALSE,
                           (req->file->my_rank == 0) ? H5_daos_collective_error_check_prep_cb : NULL,
                           H5_daos_collective_error_check_comp_cb, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't broadcast collective operation status");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_collective_error_check() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_collective_error_check_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous MPI_Ibcast to broadcast
 *              the result status of a collective operation. Currently just
 *              sets the value for the status buffer on rank 0. Only meant
 *              to be called by the rank that is the root of broadcasting
 *              (usually rank 0).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_collective_error_check_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for MPI broadcast task");

    assert(udata->req);
    assert(udata->req->file->my_rank == 0);
    assert(udata->buffer);
    assert(udata->buffer_len == sizeof(udata->req->status));

    *((int *)udata->buffer) = udata->req->status;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_collective_error_check_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_collective_error_check_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_Ibcast to broadcast
 *              the result status of a collective operation. Currently
 *              checks for a failed task, checks the status buffer to
 *              determine whether an error occurred on the broadcasting
 *              root rank, and then frees private data. Meant to be called
 *              by all ranks.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_collective_error_check_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for MPI broadcast task");

    assert(udata->req);
    assert(udata->buffer);
    assert(udata->buffer_len == sizeof(udata->req->status));

    /* Handle errors in broadcast task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast of collective operation status";
    } /* end if */
    else if ((task->dt_result == 0) && (udata->req->file->my_rank != 0)) {
        int *status_buf = (int *)udata->buffer;

        assert(*status_buf != -H5_DAOS_PRE_ERROR);
        if ((*status_buf) <= -H5_DAOS_H5_OPEN_ERROR) {
            udata->req->status      = -H5_DAOS_REMOTE_ERROR;
            udata->req->failed_task = "remote task";
        } /* end if */
    }     /* end else */

done:
    if (udata) {
        /* Close object */
        if (udata->obj && H5_daos_object_close(&udata->obj->item) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_metatask) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_metatask, ret_value);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_collective_error_check_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_free_async_task
 *
 * Purpose:     Frees a buffer (the private data).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_free_async_task(tse_task_t *task)
{
    void *buf       = NULL;
    int   ret_value = 0;

    /* Get private data */
    if (NULL == (buf = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for free task");

    /* Free buffer */
    DV_free(buf);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_free_async_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_free_async
 *
 * Purpose:     Schedules a task to free a buffer.  Executes even if a
 *              previous task failed, does not issue new failures.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_free_async(void *buf, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *free_task;
    int         ret;
    herr_t      ret_value = SUCCEED; /* Return value */

    assert(buf);
    assert(first_task);
    assert(dep_task);

    /* Create task for free */
    if (H5_daos_create_task(H5_daos_free_async_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, buf, &free_task) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to free buffer");

    /* Schedule free task (or save it to be scheduled later) */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(free_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to free buffer: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = free_task;

    /* Do not update *dep_task since nothing depends on this buffer being freed
     */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_free_async() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_create_task
 *
 * Purpose:     Creates a TSE task. May re-use a task from a task list if a
 *              task is available.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_create_task(tse_task_func_t task_func, unsigned num_deps, tse_task_t *dep_tasks[],
                    tse_task_cb_t task_prep_cb, tse_task_cb_t task_comp_cb, void *task_priv,
                    tse_task_t **taskp)
{
    int    ret;
    herr_t ret_value = SUCCEED;

    assert(taskp);
    assert(H5_daos_task_list_g);

    if (H5_daos_task_list_avail(H5_daos_task_list_g)) {
        if (H5_daos_task_list_get(H5_daos_task_list_g, taskp) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get task from task list");

        if (0 != (ret = tse_task_reset(*taskp, task_func, task_priv)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't reset task: %s", H5_daos_err_to_string(ret));
    }
    else {
        if (0 != (ret = tse_task_create(task_func, &H5_daos_glob_sched_g, task_priv, taskp)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task: %s", H5_daos_err_to_string(ret));
    }

    assert(*taskp);

    /* Register task dependency */
    if (num_deps && 0 != (ret = tse_task_register_deps(*taskp, (int)num_deps, dep_tasks)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register task dependencies: %s",
                     H5_daos_err_to_string(ret));

    if (task_prep_cb || task_comp_cb)
        if (0 != (ret = tse_task_register_cbs(*taskp, task_prep_cb, NULL, 0, task_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register callbacks for task: %s",
                         H5_daos_err_to_string(ret));

done:
    if (ret_value < 0 && *taskp)
        tse_task_complete(*taskp, -H5_DAOS_SETUP_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_create_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_create_daos_task
 *
 * Purpose:     Creates a DAOS task. May re-use a DAOS task from a task
 *              list if a task is available.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_create_daos_task(daos_opc_t daos_opc, unsigned num_deps, tse_task_t *dep_tasks[],
                         tse_task_cb_t task_prep_cb, tse_task_cb_t task_comp_cb, void *task_priv,
                         tse_task_t **taskp)
{
    int    ret;
    herr_t ret_value = SUCCEED;

    assert(taskp);
    assert(H5_daos_task_list_g);

    if (H5_daos_task_list_avail(H5_daos_task_list_g)) {
        if (H5_daos_task_list_get(H5_daos_task_list_g, taskp) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get DAOS task from task list");

        if (0 != (ret = daos_task_reset(*taskp, daos_opc)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't reset DAOS task: %s",
                         H5_daos_err_to_string(ret));

        if (num_deps && 0 != (ret = tse_task_register_deps(*taskp, (int)num_deps, dep_tasks)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register task dependencies: %s",
                         H5_daos_err_to_string(ret));
    }
    else {
        if (0 != (ret = daos_task_create(daos_opc, &H5_daos_glob_sched_g, num_deps, dep_tasks, taskp)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create DAOS task: %s",
                         H5_daos_err_to_string(ret));
    }

    assert(*taskp);

    if (task_prep_cb || task_comp_cb)
        if (0 != (ret = tse_task_register_cbs(*taskp, task_prep_cb, NULL, 0, task_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register callbacks for task: %s",
                         H5_daos_err_to_string(ret));

    /* Set private data for task */
    tse_task_set_priv(*taskp, task_priv);

done:
    if (ret_value < 0 && *taskp)
        tse_task_complete(*taskp, -H5_DAOS_SETUP_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_create_daos_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_progress
 *
 * Purpose:     Make progress on asynchronous tasks.  Can be run with a
 *              request (in which case it waits until the the request
 *              finishes) or without one (in which case it waits until all
 *              tasks in the file are complete.  Can be run with timeout
 *              set to H5_DAOS_PROGRESS_KICK in which case it makes
 *              non-blocking progress then exits immediately, with timeout
 *              set to H5_DAOS_PROGRESS_WAIT in which case it waits as
 *              long as it takes, or with timeout set to a value in
 *              nanoseconds in which case it waits up to that amount of
 *              time then exits as soon as the exit condition or the
 *              timeout is met.
 *
 * Return:      Success:    Non-negative.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_progress(H5_daos_req_t *req, uint64_t timeout)
{
    int64_t     timeout_rem;
    int         completed;
    bool        is_empty = FALSE;
    tse_task_t *tmp_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    /* Set timeout_rem, being careful to avoid overflow */
    timeout_rem = timeout > INT64_MAX ? INT64_MAX : (int64_t)timeout;

    /* Loop until the scheduler is empty, the timeout is met, the scheduler is
     * empty, or the provided request is complete */
    do {
        /* Progress MPI if there is a task in flight */
        if (H5_daos_mpi_task_g) {
            /* Check if task is complete */
            if (MPI_SUCCESS != (ret = MPI_Test(&H5_daos_mpi_req_g, &completed, MPI_STATUS_IGNORE)))
                D_DONE_ERROR(H5E_VOL, H5E_MPI, FAIL, "MPI_Test failed: %d", ret);

            /* Complete matching DAOS task if so */
            if (ret_value < 0) {
                tmp_task           = H5_daos_mpi_task_g;
                H5_daos_mpi_task_g = NULL;
                /* Return task to task list */
                if (H5_daos_task_list_put(H5_daos_task_list_g, tmp_task) < 0)
                    D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                 "can't return task to task list");
                tse_task_complete(tmp_task, -H5_DAOS_MPI_ERROR);
            } /* end if */
            else if (completed) {
                tmp_task           = H5_daos_mpi_task_g;
                H5_daos_mpi_task_g = NULL;
                /* Return task to task list */
                if (H5_daos_task_list_put(H5_daos_task_list_g, tmp_task) < 0)
                    D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                 "can't return task to task list");
                tse_task_complete(tmp_task, 0);
            } /* end if */
        }     /* end if */

        /* Progress DAOS */
        if ((0 != (ret = daos_progress(&H5_daos_glob_sched_g,
                                       timeout_rem > (1000000 * H5_DAOS_ASYNC_POLL_INTERVAL)
                                           ? H5_DAOS_ASYNC_POLL_INTERVAL
                                           : (timeout_rem / 1000000),
                                       &is_empty))) &&
            (ret != -DER_TIMEDOUT))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler: %s",
                         H5_daos_err_to_string(ret));

        /* Advance time (H5_DAOS_ASYNC_POLL_INTERVAL is in milliseconds) */
        /* Actually check clock here? */
        timeout_rem -= (1000000 * H5_DAOS_ASYNC_POLL_INTERVAL);
    } while ((req ? req->finalize_task != NULL : !is_empty) && timeout_rem > 0);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_progress() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_wait_task
 *
 * Purpose:     Sets the provided hbool_t to TRUE.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_task_wait_task(tse_task_t *task)
{
    hbool_t *task_complete = NULL;
    int      ret_value     = 0;

    /* Get private data */
    if (NULL == (task_complete = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for task wait task");

    /* Mark task as complete */
    *task_complete = TRUE;

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_task_wait_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_wait
 *
 * Purpose:     Schedules *first_task and blocks until *dep_task
 *              completes.  On successful exit, *first_task and *dep_task
 *              will be set to NULL.
 *
 * Return:      Success:    Non-negative.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_task_wait(tse_task_t **first_task, tse_task_t **dep_task)
{
    int         completed;
    bool        is_empty = FALSE;
    tse_task_t *tmp_task;
    tse_task_t *end_task;
    hbool_t     task_complete = FALSE;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(first_task);
    assert(dep_task);

    /* If no *dep_task, nothing to do */
    if (*dep_task) {
        assert(*first_task);

        /* Create end task which will execute after *dep_task, and mark
         * task_complete as TRUE */
        if (H5_daos_create_task(H5_daos_task_wait_task, 1, dep_task, NULL, NULL, &task_complete, &end_task) <
            0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create end task for task wait");

        /* Schedule end task */
        if (0 != (ret = tse_task_schedule(end_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule end task for task wait: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule first task */
        if (0 != (ret = tse_task_schedule(*first_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule first task: %s",
                         H5_daos_err_to_string(ret));
        *first_task = NULL;
        *dep_task   = NULL;

        /* Loop until the task is complete */
        while (!task_complete) {
            /* Progress MPI if there is a task in flight */
            if (H5_daos_mpi_task_g) {
                /* Check if task is complete */
                if (MPI_SUCCESS != (ret = MPI_Test(&H5_daos_mpi_req_g, &completed, MPI_STATUS_IGNORE)))
                    D_DONE_ERROR(H5E_VOL, H5E_MPI, FAIL, "MPI_Test failed: %d", ret);

                /* Complete matching DAOS task if so */
                if (ret_value < 0) {
                    tmp_task           = H5_daos_mpi_task_g;
                    H5_daos_mpi_task_g = NULL;
                    /* Return task to task list */
                    if (H5_daos_task_list_put(H5_daos_task_list_g, tmp_task) < 0)
                        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                     "can't return task to task list");
                    tse_task_complete(tmp_task, -H5_DAOS_MPI_ERROR);
                } /* end if */
                else if (completed) {
                    tmp_task           = H5_daos_mpi_task_g;
                    H5_daos_mpi_task_g = NULL;
                    /* Return task to task list */
                    if (H5_daos_task_list_put(H5_daos_task_list_g, tmp_task) < 0)
                        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                     "can't return task to task list");
                    tse_task_complete(tmp_task, 0);
                } /* end if */
            }     /* end if */

            /* Progress DAOS */
            if ((0 != (ret = daos_progress(&H5_daos_glob_sched_g, H5_DAOS_ASYNC_POLL_INTERVAL, &is_empty))) &&
                (ret != -DER_TIMEDOUT))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler: %s",
                             H5_daos_err_to_string(ret));
        } /* end while */
    }     /* end if */
    else
        assert(!*first_task);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_task_wait() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_mpi_info
 *
 * Purpose:     Retrieves any MPI parameters set on the given FAPL via
 *              H5Pset_fapl_mpio()/H5Pset_mpi_params(). Produces duplicates
 *              of the communicator/info objects that must be freed with
 *              H5_daos_comm_info_free().
 *
 * Return:      Success:    Non-negative. The new communicator and info
 *                          object handles are returned via the comm_out
 *                          and info_out pointers.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_get_mpi_info(hid_t fapl_id, MPI_Comm *comm_out, MPI_Info *info_out, int *mpi_rank, int *mpi_size)
{
    MPI_Comm comm = MPI_COMM_NULL;
    MPI_Info info = MPI_INFO_NULL;
    int      mpi_initialized;
    herr_t   ret_value = SUCCEED;

    if (H5Pget_mpi_params(fapl_id, &comm, &info) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get HDF5 MPI information");

    /*
     * If no MPI info was set (as in the case of passing a default FAPL),
     * simply use MPI_COMM_SELF as the communicator.
     */
    if (comm == MPI_COMM_NULL)
        comm = MPI_COMM_SELF;

    if (MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized");
    if (mpi_initialized) {
        if (mpi_rank)
            MPI_Comm_rank(comm, mpi_rank);
        if (mpi_size)
            MPI_Comm_size(comm, mpi_size);
    }
    else {
        if (mpi_rank)
            *mpi_rank = 0;
        if (mpi_size)
            *mpi_size = 1;
    }

    if (comm_out)
        *comm_out = comm;
    if (info_out)
        *info_out = info;

done:
    if (ret_value < 0) {
        if (H5_daos_comm_info_free(&comm, &info) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CANTFREE, FAIL, "can't free MPI comm and info objects");
    }
    else if (!comm_out || !info_out) {
        /* If comm or info are being returned to caller,
         * mark them as NULL to prevent freeing below.
         * Otherwise, free them since the caller will not.
         */
        if (comm_out)
            comm = MPI_COMM_NULL;
        if (info_out)
            info = MPI_INFO_NULL;

        if (H5_daos_comm_info_free(&comm, &info) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CANTFREE, FAIL, "can't free MPI comm and info objects");
    }

    D_FUNC_LEAVE;
} /* end H5_daos_get_mpi_info() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_comm_info_free
 *
 * Purpose:     Free the MPI communicator and info objects.
 *              If comm or info is in fact MPI_COMM_NULL or MPI_INFO_NULL,
 *              respectively, no action occurs to it.
 *
 * Return:      Success:    Non-negative.  The values the pointers refer
 *                          to will be set to the corresponding NULL
 *                          handles.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_comm_info_free(MPI_Comm *comm, MPI_Info *info)
{
    herr_t ret_value = SUCCEED;

    /* Check arguments. */
    if (!comm)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "comm pointer is NULL");
    if (!info)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "info pointer is NULL");

    if (MPI_COMM_NULL != *comm && MPI_COMM_WORLD != *comm && MPI_COMM_SELF != *comm)
        MPI_Comm_free(comm);
    if (MPI_INFO_NULL != *info)
        MPI_Info_free(info);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_comm_info_free() */

H5PL_type_t
H5PLget_plugin_type(void)
{
    return H5PL_TYPE_VOL;
}

const void *
H5PLget_plugin_info(void)
{
    return &H5_daos_g;
}
