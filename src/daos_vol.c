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
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 *          library.  General connector routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

#include <daos_mgmt.h>          /* For pool creation */

/****************/
/* Local Macros */
/****************/

/* Default DAOS group ID used for creating pools */
#ifndef DAOS_DEFAULT_GROUP_ID
# define DAOS_DEFAULT_GROUP_ID "daos_server"
#endif
#define H5_DAOS_MAX_GRP_NAME     64
#define H5_DAOS_MAX_SVC_REPLICAS 13

#define H5_DAOS_PRINT_UUID(uuid) do {       \
    char uuid_buf[37];                      \
    uuid_unparse(uuid, uuid_buf);           \
    printf("POOL UUID = %s\n", uuid_buf);   \
} while (0)

/************************************/
/* Local Type and Struct Definition */
/************************************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_set_object_class(hid_t plist_id, char *object_class);
static herr_t H5_daos_str_prop_delete(hid_t prop_id, const char *name,
    size_t size, void *_value);
static herr_t H5_daos_str_prop_copy(const char *name, size_t size,
    void *_value);
static int H5_daos_str_prop_compare(const void *_value1, const void *_value2,
    size_t size);
static herr_t H5_daos_str_prop_close(const char *name, size_t size,
    void *_value);
static herr_t H5_daos_init(hid_t vipl_id);
static herr_t H5_daos_term(void);
static herr_t H5_daos_pool_create(uuid_t uuid, const char **pool_grp, d_rank_list_t **svcl);
static herr_t H5_daos_pool_destroy(uuid_t uuid);
static herr_t H5_daos_pool_connect(void);
static herr_t H5_daos_pool_disconnect(void);
static herr_t H5_daos_pool_handle_bcast(int rank);
static void *H5_daos_fapl_copy(const void *_old_fa);
static herr_t H5_daos_fapl_free(void *_fa);
static herr_t H5_daos_get_conn_cls(void *item, H5VL_get_conn_lvl_t lvl,
    const H5VL_class_t **conn_cls);
static herr_t H5_daos_opt_query(void *item, H5VL_subclass_t cls, int opt_type,
    hbool_t *supported);
static herr_t H5_daos_optional(void *item, int op_type, hid_t dxpl_id,
    void **req, va_list arguments);

/*******************/
/* Local Variables */
/*******************/

/* The DAOS VOL connector struct */
static const H5VL_class_t H5_daos_g = {
    HDF5_VOL_DAOS_VERSION_1,                 /* Plugin Version number */
    H5_VOL_DAOS_CLS_VAL,                     /* Plugin Value */
    H5_DAOS_VOL_NAME,                        /* Plugin Name */
    H5VL_CAP_FLAG_NONE,                      /* Plugin capability flags */
    H5_daos_init,                            /* Plugin initialize */
    H5_daos_term,                            /* Plugin terminate */
    {
        sizeof(H5_daos_fapl_t),              /* Plugin Info size */
        H5_daos_fapl_copy,                   /* Plugin Info copy */
        NULL,                                /* Plugin Info compare */
        H5_daos_fapl_free,                   /* Plugin Info free */
        NULL,                                /* Plugin Info To String */
        NULL,                                /* Plugin String To Info */
    },
    {
        NULL,                                /* Plugin Get Object */
        NULL,                                /* Plugin Get Wrap Ctx */
        NULL,                                /* Plugin Wrap Object */
        NULL,                                /* Plugin Unwrap Object */
        NULL,                                /* Plugin Free Wrap Ctx */
    },
    {                                        /* Plugin Attribute cls */
        H5_daos_attribute_create,            /* Plugin Attribute create */
        H5_daos_attribute_open,              /* Plugin Attribute open */
        H5_daos_attribute_read,              /* Plugin Attribute read */
        H5_daos_attribute_write,             /* Plugin Attribute write */
        H5_daos_attribute_get,               /* Plugin Attribute get */
        H5_daos_attribute_specific,          /* Plugin Attribute specific */
        NULL,                                /* Plugin Attribute optional */
        H5_daos_attribute_close              /* Plugin Attribute close */
    },
    {                                        /* Plugin Dataset cls */
        H5_daos_dataset_create,              /* Plugin Dataset create */
        H5_daos_dataset_open,                /* Plugin Dataset open */
        H5_daos_dataset_read,                /* Plugin Dataset read */
        H5_daos_dataset_write,               /* Plugin Dataset write */
        H5_daos_dataset_get,                 /* Plugin Dataset get */
        H5_daos_dataset_specific,            /* Plugin Dataset specific */
        NULL,                                /* Plugin Dataset optional */
        H5_daos_dataset_close                /* Plugin Dataset close */
    },
    {                                        /* Plugin Datatype cls */
        H5_daos_datatype_commit,             /* Plugin Datatype commit */
        H5_daos_datatype_open,               /* Plugin Datatype open */
        H5_daos_datatype_get,                /* Plugin Datatype get */
        H5_daos_datatype_specific,           /* Plugin Datatype specific */
        NULL,                                /* Plugin Datatype optional */
        H5_daos_datatype_close               /* Plugin Datatype close */
    },
    {                                        /* Plugin File cls */
        H5_daos_file_create,                 /* Plugin File create */
        H5_daos_file_open,                   /* Plugin File open */
        H5_daos_file_get,                    /* Plugin File get */
        H5_daos_file_specific,               /* Plugin File specific */
        NULL,                                /* Plugin File optional */
        H5_daos_file_close                   /* Plugin File close */
    },
    {                                        /* Plugin Group cls */
        H5_daos_group_create,                /* Plugin Group create */
        H5_daos_group_open,                  /* Plugin Group open */
        H5_daos_group_get,                   /* Plugin Group get */
        H5_daos_group_specific,              /* Plugin Group specific */
        NULL,                                /* Plugin Group optional */
        H5_daos_group_close                  /* Plugin Group close */
    },
    {                                        /* Plugin Link cls */
        H5_daos_link_create,                 /* Plugin Link create */
        H5_daos_link_copy,                   /* Plugin Link copy */
        H5_daos_link_move,                   /* Plugin Link move */
        H5_daos_link_get,                    /* Plugin Link get */
        H5_daos_link_specific,               /* Plugin Link specific */
        NULL                                 /* Plugin Link optional */
    },
    {                                        /* Plugin Object cls */
        H5_daos_object_open,                 /* Plugin Object open */
        H5_daos_object_copy,                 /* Plugin Object copy */
        H5_daos_object_get,                  /* Plugin Object get */
        H5_daos_object_specific,             /* Plugin Object specific */
        NULL                                 /* Plugin Object optional */
    },
    {
        H5_daos_get_conn_cls,                /* Plugin get connector class */
        H5_daos_opt_query                    /* Plugin optional callback query */
    },
    {
        NULL,                                /* Plugin Request wait */
        NULL,                                /* Plugin Request notify */
        NULL,                                /* Plugin Request cancel */
        NULL,                                /* Plugin Request specific */
        NULL,                                /* Plugin Request optional */
        H5_daos_req_free                     /* Plugin Request free */
    },
    {
        H5_daos_blob_put,                    /* Plugin 'blob' put */
        H5_daos_blob_get,                    /* Plugin 'blob' get */
        H5_daos_blob_specific,               /* Plugin 'blob' specific */
        NULL                                 /* Plugin 'blob' optional */
    },
    {
        NULL,                                /* Plugin Token compare */
        NULL,                                /* Plugin Token to string */
        NULL                                 /* Plugin Token from string */
    },
    H5_daos_optional                         /* Plugin optional */
};

/* Free list definitions */
/* DSINC - currently no external access to free lists
H5FL_DEFINE(H5_daos_file_t);
H5FL_DEFINE(H5_daos_group_t);
H5FL_DEFINE(H5_daos_dset_t);
H5FL_DEFINE(H5_daos_dtype_t);
H5FL_DEFINE(H5_daos_map_t);
H5FL_DEFINE(H5_daos_attr_t);*/

hid_t H5_DAOS_g = -1;
static hbool_t H5_daos_initialized_g = FALSE;

/* Identifiers for HDF5's error API */
hid_t dv_err_stack_g = -1;
hid_t dv_err_class_g = -1;

#ifdef DV_TRACK_MEM_USAGE
/*
 * Counter to keep track of the currently allocated amount of bytes
 */
size_t daos_vol_curr_alloc_bytes;
#endif

/* Pool handle for use with all files */
daos_handle_t H5_daos_poh_g = DAOS_HDL_INVAL;

/* Global variables used to open the pool */
MPI_Comm H5_daos_pool_comm_g = MPI_COMM_NULL;       /* Pool communicator */
static hbool_t H5_daos_pool_globals_set_g = FALSE;  /* Pool config set */
static hbool_t H5_daos_pool_is_mine_g = FALSE;      /* Pool created internally */
static uuid_t  H5_daos_pool_uuid_g;                 /* Pool UUID */
static char H5_daos_pool_grp_g[H5_DAOS_MAX_GRP_NAME + 1] = {'\0'}; /* Pool Group */
static d_rank_t H5_daos_pool_ranks_g[H5_DAOS_MAX_SVC_REPLICAS]; /* Pool ranks */
static d_rank_list_t H5_daos_pool_svcl_g = {0};                  /* Pool svc list */
static const unsigned int   H5_daos_pool_default_mode_g          = 0731;         /* Default Mode */
static const daos_size_t    H5_daos_pool_default_scm_size_g      = (1ULL << 31); /*   2GB */
static const daos_size_t    H5_daos_pool_default_nvme_size_g     = (1ULL << 33); /*   8GB */
static const unsigned int   H5_daos_pool_default_svc_nreplicas_g = 1;            /* Number of replicas */

/* DAOS task and MPI request for current in-flight MPI operation */
tse_task_t *H5_daos_mpi_task = NULL;
MPI_Request H5_daos_mpi_req;

/* Constant Keys */
const char H5_daos_int_md_key_g[]          = "/Internal Metadata";
const char H5_daos_root_grp_oid_key_g[]    = "Root Group OID";
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

const daos_size_t H5_daos_int_md_key_size_g          = (daos_size_t)(sizeof(H5_daos_int_md_key_g) - 1);
const daos_size_t H5_daos_root_grp_oid_key_size_g    = (daos_size_t)(sizeof(H5_daos_root_grp_oid_key_g) - 1);
const daos_size_t H5_daos_cpl_key_size_g             = (daos_size_t)(sizeof(H5_daos_cpl_key_g) - 1);
const daos_size_t H5_daos_link_key_size_g            = (daos_size_t)(sizeof(H5_daos_link_key_g) - 1);
const daos_size_t H5_daos_link_corder_key_size_g     = (daos_size_t)(sizeof(H5_daos_link_corder_key_g) - 1);
const daos_size_t H5_daos_nlinks_key_size_g          = (daos_size_t)(sizeof(H5_daos_nlinks_key_g) - 1);
const daos_size_t H5_daos_max_link_corder_key_size_g = (daos_size_t)(sizeof(H5_daos_max_link_corder_key_g) - 1);
const daos_size_t H5_daos_type_key_size_g            = (daos_size_t)(sizeof(H5_daos_type_key_g) - 1);
const daos_size_t H5_daos_space_key_size_g           = (daos_size_t)(sizeof(H5_daos_space_key_g) - 1);
const daos_size_t H5_daos_attr_key_size_g            = (daos_size_t)(sizeof(H5_daos_attr_key_g) - 1);
const daos_size_t H5_daos_nattr_key_size_g           = (daos_size_t)(sizeof(H5_daos_nattr_key_g) - 1);
const daos_size_t H5_daos_max_attr_corder_key_size_g = (daos_size_t)(sizeof(H5_daos_max_attr_corder_key_g) - 1);
const daos_size_t H5_daos_ktype_size_g               = (daos_size_t)(sizeof(H5_daos_ktype_g) - 1);
const daos_size_t H5_daos_vtype_size_g               = (daos_size_t)(sizeof(H5_daos_vtype_g) - 1);
const daos_size_t H5_daos_map_key_size_g             = (daos_size_t)(sizeof(H5_daos_map_key_g) - 1);
const daos_size_t H5_daos_blob_key_size_g            = (daos_size_t)(sizeof(H5_daos_blob_key_g) - 1);
const daos_size_t H5_daos_fillval_key_size_g            = (daos_size_t)(sizeof(H5_daos_fillval_key_g) - 1);


/*-------------------------------------------------------------------------
 * Function:    H5daos_init
 *
 * Purpose:     Initialize this VOL connector by connecting to the pool and
 *              registering the connector with the library.  pool_comm
 *              identifies the communicator used to connect to the DAOS
 *              pool.  This should include all processes that will
 *              participate in I/O.  This call is collective across
 *              pool_comm.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              March, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_init(MPI_Comm pool_comm, uuid_t pool_uuid, const char *pool_grp, const char *pool_svcl)
{
    H5I_type_t idType = H5I_UNINIT;
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(MPI_COMM_NULL == pool_comm)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid MPI communicator")
    if(uuid_is_null(pool_uuid))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid UUID")
    if(NULL == pool_grp)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid service group")
    if(strlen(pool_grp) > H5_DAOS_MAX_GRP_NAME)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid service group")
    if(NULL == pool_svcl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid service list")

    /* Initialize HDF5 */
    if(H5open() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "HDF5 failed to initialize")

    if(H5_DAOS_g >= 0 && (idType = H5Iget_type(H5_DAOS_g)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "failed to retrieve DAOS VOL connector's ID type")

    /* Register the DAOS VOL, if it isn't already */
    if(H5I_VOL != idType) {
        htri_t is_registered;

        if((is_registered = H5VLis_connector_registered_by_value(H5_daos_g.value)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTINIT, FAIL, "can't determine if DAOS VOL connector is registered")

        if(!is_registered) {
            d_rank_list_t *svcl;
            uint32_t i;

            /* Save arguments to globals */
            H5_daos_pool_comm_g = pool_comm;
            memcpy(H5_daos_pool_uuid_g, pool_uuid, sizeof(uuid_t));
            strcpy(H5_daos_pool_grp_g, pool_grp);

            /* Parse rank list */
            if(NULL == (svcl = daos_rank_list_parse(pool_svcl, ":")))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "failed to parse service rank list")
            if(svcl->rl_nr == 0 || svcl->rl_nr > H5_DAOS_MAX_SVC_REPLICAS) {
                daos_rank_list_free(svcl);
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid service list")
            }
            H5_daos_pool_svcl_g.rl_nr = svcl->rl_nr;
            memset(H5_daos_pool_ranks_g, 0, sizeof(H5_daos_pool_ranks_g));
            H5_daos_pool_svcl_g.rl_ranks = H5_daos_pool_ranks_g;
            for(i = 0; i < svcl->rl_nr; i++)
                H5_daos_pool_ranks_g[i] = svcl->rl_ranks[i];
            daos_rank_list_free(svcl);
            H5_daos_pool_globals_set_g = TRUE;

            /* Register connector */
            if((H5_DAOS_g = H5VLregister_connector((const H5VL_class_t *)&H5_daos_g, H5P_DEFAULT)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTINSERT, FAIL, "can't create ID for DAOS VOL connector")
        } /* end if */
        else {
            if((H5_DAOS_g = H5VLget_connector_id_by_name(H5_daos_g.name)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTGET, FAIL, "unable to get registered ID for DAOS VOL connector")
        } /* end else */
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5daos_init() */


/*-------------------------------------------------------------------------
 * Function:    H5daos_term
 *
 * Purpose:     Shut down the DAOS VOL
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              March, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_term(void)
{
    herr_t ret_value = SUCCEED;            /* Return value */

    /* H5TRACE0("e",""); DSINC */

    /* Terminate the connector */
    if(H5_daos_term() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't terminate DAOS VOL connector")

done:
#ifdef DV_TRACK_MEM_USAGE
    /* Check for allocated memory */
    if(0 != daos_vol_curr_alloc_bytes)
        FUNC_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "%zu bytes were still left allocated", daos_vol_curr_alloc_bytes)

    daos_vol_curr_alloc_bytes = 0;
#endif

    /* Unregister from the HDF5 error API */
    if(dv_err_class_g >= 0) {
        if(H5Eunregister_class(dv_err_class_g) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't unregister error class from HDF5 error API")

        /* Print the current error stack before destroying it */
        PRINT_ERROR_STACK

        /* Destroy the error stack */
        if(H5Eclose_stack(dv_err_stack_g) < 0) {
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't close HDF5 error stack")
            PRINT_ERROR_STACK
        } /* end if */

        dv_err_stack_g = -1;
        dv_err_class_g = -1;
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5daos_term() */


/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_daos
 *
 * Purpose:     Modify the file access property list to use the DAOS VOL
 *              connector defined in this source file.  file_comm and
 *              file_info identify the communicator and info object used
 *              to coordinate actions on file create, open, flush, and
 *              close.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_daos(hid_t fapl_id, MPI_Comm file_comm, MPI_Info file_info)
{
    H5_daos_fapl_t fa;
    htri_t         is_fapl;
    herr_t         ret_value = FAIL;

    /* H5TRACE3("e", "iMcMi", fapl_id, file_comm, file_info); DSINC */

    if(H5_DAOS_g < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_UNINITIALIZED, FAIL, "DAOS VOL connector not initialized")

    if(fapl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list")

    if((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class")
    if(!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    if(MPI_COMM_NULL == file_comm)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a valid MPI communicator")

    /* Initialize driver specific properties */
    fa.comm = file_comm;
    fa.info = file_info;

    ret_value = H5Pset_vol(fapl_id, H5_DAOS_g, &fa);

done:
    D_FUNC_LEAVE_API
} /* end H5Pset_fapl_daos() */


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
    herr_t      ret_value = SUCCEED;

    if(plist_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list")

    /* Call internal routine */
    if(H5_daos_set_object_class(plist_id, object_class) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't set object class")

done:
    D_FUNC_LEAVE_API
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
    char        *copied_object_class = NULL;
    htri_t      prop_exists;
    herr_t      ret_value = SUCCEED;

    /* Check if the property already exists on the property list */
    if((prop_exists = H5Pexist(plist_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")

    /* Copy object class */
    if(object_class)
        if(NULL == (copied_object_class = strdup(object_class)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy object class string")

    /* Set the property, or insert it if it does not exist */
    if(prop_exists) {
        if(H5Pset(plist_id, H5_DAOS_OBJ_CLASS_NAME, &copied_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property")
    } /* end if */
    else
        if(H5Pinsert2(plist_id, H5_DAOS_OBJ_CLASS_NAME, sizeof(char *),
                &copied_object_class, NULL, NULL,
                H5_daos_str_prop_delete, H5_daos_str_prop_copy,
                H5_daos_str_prop_compare, H5_daos_str_prop_close) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into list")

done:
    D_FUNC_LEAVE
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
    char oclass_str[10]; /* DAOS uses a size of 10 internally for these calls */
    herr_t ret_value = SUCCEED;

    /* Get object class id from oid */
    /* Replace with DAOS function once public! DSINC */
    oc_id = (oid.hi & OID_FMT_CLASS_MASK) >> OID_FMT_CLASS_SHIFT;

    /* Get object class string */
    if(daos_oclass_id2name(oc_id, oclass_str) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class string")

    /* Set object class string on plist */
    if(H5_daos_set_object_class(plist_id, oclass_str) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't set object class")

done:
    D_FUNC_LEAVE
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
    char        *tmp_object_class = NULL;
    htri_t      prop_exists;
    size_t      len;
    ssize_t     ret_value;

    /* Check if the property already exists on the property list */
    if((prop_exists = H5Pexist(plist_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")

    if(prop_exists) {
        /* Get the property */
        if(H5Pget(plist_id, H5_DAOS_OBJ_CLASS_NAME, &tmp_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get object class")

        /* Set output values */
        if(tmp_object_class) {
            len = strlen(tmp_object_class);
            if(object_class && (size > 0)) {
                strncpy(object_class, tmp_object_class, size);
                if(len >= size)
                    object_class[size - 1] = '\0';
            } /* end if */
        } /* end if */
        else {
            /* Simply return an empty string */
            len = 0;
            if(object_class && (size > 0))
                object_class[0] = '\0';
        } /* end else */
    } /* end if */
    else {
        /* Simply return an empty string */
        len = 0;
        if(object_class && (size > 0))
            object_class[0] = '\0';
    } /* end else */

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    D_FUNC_LEAVE_API
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
    htri_t      is_fapl;
    char        *copied_object_class = NULL;
    htri_t      prop_exists;
    herr_t      ret_value = SUCCEED;

    if(fapl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list")

    if((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class")
    if(!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    /* Check if the property already exists on the property list */
    if((prop_exists = H5Pexist(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")

    /* Copy object class */
    if(object_class)
        if(NULL == (copied_object_class = strdup(object_class)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy object class string")

    /* Set the property, or insert it if it does not exist */
    if(prop_exists) {
        if(H5Pset(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME, &copied_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property")
    } /* end if */
    else
        if(H5Pinsert2(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME, sizeof(char *),
                &copied_object_class, NULL, NULL,
                H5_daos_str_prop_delete, H5_daos_str_prop_copy,
                H5_daos_str_prop_compare, H5_daos_str_prop_close) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into list")

done:
    D_FUNC_LEAVE_API
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
    htri_t      is_fapl;
    char        *tmp_object_class = NULL;
    htri_t      prop_exists;
    size_t      len;
    ssize_t     ret_value;

    if((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class")
    if(!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    /* Check if the property already exists on the property list */
    if((prop_exists = H5Pexist(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")

    if(prop_exists) {
        /* Get the property */
        if(H5Pget(fapl_id, H5_DAOS_ROOT_OPEN_OCLASS_NAME, &tmp_object_class) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get object class")

        /* Set output values */
        if(tmp_object_class) {
            len = strlen(tmp_object_class);
            if(object_class && (size > 0)) {
                strncpy(object_class, tmp_object_class, size);
                if(len >= size)
                    object_class[size - 1] = '\0';
            } /* end if */
        } /* end if */
        else {
            /* Simply return an empty string */
            len = 0;
            if(object_class && (size > 0))
                object_class[0] = '\0';
        } /* end else */
    } /* end if */
    else {
        /* Simply return an empty string */
        len = 0;
        if(object_class && (size > 0))
            object_class[0] = '\0';
    } /* end else */

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    D_FUNC_LEAVE_API
} /* end H5daos_get_root_open_object_class() */


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
H5_daos_str_prop_delete(hid_t H5VL_DAOS_UNUSED prop_id,
    const char H5VL_DAOS_UNUSED *name, size_t H5VL_DAOS_UNUSED size,
    void *_value)
{
    char **value = (char **)_value;

    if(*value)
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
H5_daos_str_prop_copy(const char H5VL_DAOS_UNUSED *name,
    size_t H5VL_DAOS_UNUSED size, void *_value)
{
    char **value = (char **)_value;
    herr_t ret_value = SUCCEED;

    if(*value)
        if(NULL == (*value = strdup(*value)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy string property")

done:
    D_FUNC_LEAVE
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
H5_daos_str_prop_compare(const void *_value1, const void *_value2,
    size_t H5VL_DAOS_UNUSED size)
{
    char * const *value1 = (char * const *)_value1;
    char * const *value2 = (char * const *)_value2;
    int ret_value;

    if(*value1) {
        if(*value2)
            ret_value = strcmp(*value1, *value2);
        else
            ret_value = 1;
    } /* end if */
    else {
        if(*value2)
            ret_value = -1;
        else
            ret_value = 0;
    } /* end else */

    return ret_value;
} /* end H5_daos_str_prop_compare() */


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
H5_daos_str_prop_close(const char H5VL_DAOS_UNUSED *name,
    size_t H5VL_DAOS_UNUSED size, void *_value)
{
    char **value = (char **)_value;

    if(*value)
        free(*value);

    return SUCCEED;
} /* end H5_daos_str_prop_close() */


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
    H5VL_object_t     *obj = NULL;    /* object token of loc_id */
    herr_t          ret_value = SUCCEED;

    if(!snap_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "snapshot ID pointer is NULL")

    /* get the location object */
    if(NULL == (obj = (H5VL_object_t *)H5I_object(loc_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier")

    /* Make sure object's VOL is this one */
    if(obj->driver->id != H5_DAOS_g)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location does not use DAOS VOL connector")

    /* Get file object */
    if(NULL == (item = H5VLobject(loc_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a VOL object")

    file = item->file;

    /* Check for write access */
    if(!(file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Tell the file to save a snapshot next time it is flushed (committed) */
    file->snap_epoch = (int)TRUE;

    /* Return epoch in snap_id */
    *snap_id = (uint64_t)file->epoch;

done:
    D_FUNC_LEAVE_API
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

    if(fapl_id == H5P_DEFAULT)
        D_GOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list")

    if((is_fapl = H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "couldn't determine property list class")
    if(!is_fapl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    /* Set the property */
    if(H5Pset(fapl_id, H5_DAOS_SNAP_OPEN_ID, &snap_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set property value for snap id")

done:
    D_FUNC_LEAVE_API
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
    int pool_rank, pool_num_procs;
    int mpi_initialized;
    int ret;
    herr_t ret_value = SUCCEED;            /* Return value */

    if(H5_daos_initialized_g)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "attempting to initialize connector twice")

    /* Register interfaces that might not be initialized in time (for example if
     * we open an object without knowing its type first, H5Oopen will not
     * initialize that type) */
    /* if(H5G_init() < 0)
        D_GOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to initialize group interface")
    if(H5M_init() < 0)
        D_GOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to initialize map interface")
    if(H5D_init() < 0)
        D_GOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to initialize dataset interface")
    if(H5T_init() < 0)
        D_GOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to initialize datatype interface") */

    if((dv_err_stack_g = H5Ecreate_stack()) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create HDF5 error stack")

    /* Register the connector with HDF5's error reporting API */
    if((dv_err_class_g = H5Eregister_class(DAOS_VOL_ERR_CLS_NAME, DAOS_VOL_ERR_LIB_NAME, DAOS_VOL_ERR_VER)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register error class with HDF5 error API")

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Register the DAOS SNAP_OPEN_ID property with HDF5 */
    snap_id_default = H5_DAOS_SNAP_ID_INVAL;
    if(H5Pregister2(H5P_FILE_ACCESS, H5_DAOS_SNAP_OPEN_ID, sizeof(H5_daos_snap_id_t), (H5_daos_snap_id_t *) &snap_id_default,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "unable to register DAOS SNAP_OPEN_ID property")
#endif

    /* Initialize daos */
    if((0 != (ret = daos_init())) && (ret != -DER_ALREADY))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "DAOS failed to initialize: %s", H5_daos_err_to_string(ret))

#ifdef DV_TRACK_MEM_USAGE
    /* Initialize allocated memory counter */
    daos_vol_curr_alloc_bytes = 0;
#endif

    /* Set pool globals to default values if they were not already set */
    if(!H5_daos_pool_globals_set_g) {
        H5_daos_pool_comm_g = MPI_COMM_WORLD;
        memset(H5_daos_pool_uuid_g, 0, sizeof(H5_daos_pool_uuid_g));
        memset(H5_daos_pool_grp_g, '\0', sizeof(H5_daos_pool_grp_g));
    } /* end if */

    if (MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized")
    if (mpi_initialized) {
        /* Obtain the process rank and size from the communicator attached to the
         * fapl ID */
        MPI_Comm_rank(H5_daos_pool_comm_g, &pool_rank);
        MPI_Comm_size(H5_daos_pool_comm_g, &pool_num_procs);
    }
    else {
        /* Execute in serial mode */
        pool_rank = 0;
        pool_num_procs = 1;
    }

    /* First connect to the pool */
    if((pool_rank == 0) && H5_daos_pool_connect() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't connect to DAOS pool")

    /* Broadcast pool handle to other procs if any */
    if((pool_num_procs > 1) && (H5_daos_pool_handle_bcast(pool_rank) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't broadcast DAOS pool handle")

    /* Initialized */
    H5_daos_initialized_g = TRUE;

done:
    if(ret_value < 0) {
        H5daos_term();
    } /* end if */

    D_FUNC_LEAVE
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
    if(!H5_daos_initialized_g)
        D_GOTO_DONE(ret_value);

    /* Disconnect from pool */
    if(H5_daos_pool_disconnect() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't disconnect from DAOS pool")

    /* Terminate DAOS */
    if(daos_fini() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "DAOS failed to terminate")

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Unregister the DAOS SNAP_OPEN_ID property from HDF5 */
    if(H5Punregister(H5P_FILE_ACCESS, H5_DAOS_SNAP_OPEN_ID) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't unregister DAOS SNAP_OPEN_ID property")
#endif

    /* "Forget" connector id.  This should normally be called by the library
     * when it is closing the id, so no need to close it here. */
    H5_DAOS_g = -1;

    /* No longer initialized */
    H5_daos_initialized_g = FALSE;

done:
    D_FUNC_LEAVE
} /* end H5_daos_term() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_create
 *
 * Purpose:     Create a pool using default values.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_pool_create(uuid_t uuid, const char **pool_grp, d_rank_list_t **svcl)
{
    unsigned int mode = H5_daos_pool_default_mode_g;
    unsigned int uid = geteuid();
    unsigned int gid = getegid();
    const char *group = DAOS_DEFAULT_GROUP_ID;
    d_rank_list_t *targets = NULL;
    const char *dev= "pmem";
    daos_size_t  scm_size = H5_daos_pool_default_scm_size_g;
    daos_size_t  nvme_size = H5_daos_pool_default_nvme_size_g;
    int ret;
    herr_t ret_value = SUCCEED; /* Return value */

    memset(H5_daos_pool_ranks_g, 0, sizeof(H5_daos_pool_ranks_g));
    H5_daos_pool_svcl_g.rl_ranks = H5_daos_pool_ranks_g;
    H5_daos_pool_svcl_g.rl_nr = H5_daos_pool_default_svc_nreplicas_g;
    strcpy(H5_daos_pool_grp_g, group);

    /* Create a pool using default values */
    if(0 != (ret = daos_pool_create(mode, uid, gid, group, targets, dev, scm_size, nvme_size, NULL, &H5_daos_pool_svcl_g, H5_daos_pool_uuid_g, NULL /* event */)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTCREATE, FAIL, "can't create pool: %s", H5_daos_err_to_string(ret))

    memcpy(uuid, H5_daos_pool_uuid_g, sizeof(uuid_t));
    *pool_grp = H5_daos_pool_grp_g;
    *svcl = &H5_daos_pool_svcl_g;

done:
    D_FUNC_LEAVE_API
}


/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_destroy
 *
 * Purpose:     Destroy the pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_pool_destroy(uuid_t uuid)
{
    const char *group = DAOS_DEFAULT_GROUP_ID;
    int ret;
    herr_t ret_value = SUCCEED; /* Return value */

    /* Destroy the pool using default values */
    if(0 != (ret = daos_pool_destroy(uuid, group, 0 /* force */, NULL /* event */)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTDELETE, FAIL, "can't destroy pool: %s", H5_daos_err_to_string(ret))

    memset(H5_daos_pool_uuid_g, 0, sizeof(H5_daos_pool_uuid_g));
    memset(H5_daos_pool_ranks_g, 0, sizeof(H5_daos_pool_ranks_g));
    H5_daos_pool_svcl_g.rl_nr = 0;

done:
    D_FUNC_LEAVE_API
}


/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_connect
 *
 * Purpose:     Connect to the pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_pool_connect(void)
{
    char *uuid_str = NULL;
    uuid_t pool_uuid;
    const char *pool_grp = NULL;
    char *svcl_str = NULL;
    d_rank_list_t *svcl = NULL;
    daos_pool_info_t pool_info;
    int ret;
    herr_t ret_value = SUCCEED;            /* Return value */

    /* Retrieve pool UUID */
    if(NULL != (uuid_str = getenv("DAOS_POOL"))) {
        if(uuid_parse(uuid_str, pool_uuid) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "failed to parse pool UUID from environment")
#ifdef DV_PLUGIN_DEBUG
        printf("POOL UUID = %s\n", uuid_str);
#endif
        /* Must also retrieve pool service replica ranks */
        if(NULL == (svcl_str = getenv("DAOS_SVCL")))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "DAOS_SVCL must be set to pool service replica rank list")

        /* Parse rank list */
        if(NULL == (svcl = daos_rank_list_parse(svcl_str, ":")))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "failed to parse SVC list from environment")
    #ifdef DV_PLUGIN_DEBUG
        printf("SVC LIST = %s\n", svcl_str);
    #endif
    } else if (H5_daos_pool_globals_set_g) {
        memcpy(pool_uuid, H5_daos_pool_uuid_g, sizeof(uuid_t));
        pool_grp = H5_daos_pool_grp_g;
        svcl = &H5_daos_pool_svcl_g;
#ifdef DV_PLUGIN_DEBUG
        H5_DAOS_PRINT_UUID(pool_uuid);
#endif
    } else {
        /* If neither the pool environment variable nor the pool UUID have been
         * explicitly set, attempt to create a default pool.
         */
        if(H5_daos_pool_create(pool_uuid, &pool_grp, &svcl) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTCREATE, FAIL, "failed to create pool")
        H5_daos_pool_is_mine_g = TRUE;
        H5_daos_pool_globals_set_g = TRUE;
#ifdef DV_PLUGIN_DEBUG
        H5_DAOS_PRINT_UUID(pool_uuid);
#endif
    }
    if(!pool_grp)
        pool_grp = DAOS_DEFAULT_GROUP_ID; /* Attempt to use default group */
    if(svcl->rl_nr == 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "service rank number cannot be null")

    /* Connect to the pool */
    if(0 != (ret = daos_pool_connect(pool_uuid, pool_grp, svcl, DAOS_PC_RW, &H5_daos_poh_g, &pool_info, NULL /*event*/)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't connect to pool: %s", H5_daos_err_to_string(ret))

done:
    if(svcl_str && svcl)
        daos_rank_list_free(svcl);
    D_FUNC_LEAVE_API
} /* end H5_daos_pool_connect() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_disconnect
 *
 * Purpose:     Disconnect from the pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_pool_disconnect(void)
{
    hbool_t destroy_pool = FALSE; /* DSINC Do not attempt to destroy pool for now */
    int ret;
    herr_t ret_value = SUCCEED;            /* Return value */

    if(daos_handle_is_inval(H5_daos_poh_g))
        D_GOTO_DONE(ret_value);

    if(0 != (ret = daos_pool_disconnect(H5_daos_poh_g, NULL /*event*/)))
        D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't disconnect from pool: %s", H5_daos_err_to_string(ret))
    H5_daos_poh_g = DAOS_HDL_INVAL;

    if (destroy_pool && H5_daos_pool_is_mine_g) {
#ifdef DV_PLUGIN_DEBUG
        {
            char uuid_buf[37];
            uuid_unparse(H5_daos_pool_uuid_g, uuid_buf);
            printf("\n **** Destroy POOL UUID = %s\n", uuid_buf);
        }
#endif

        /* DSINC destroy pool ? */
        if(H5_daos_pool_destroy(H5_daos_pool_uuid_g) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTCREATE, FAIL, "failed to create pool")
        H5_daos_pool_is_mine_g = FALSE;
    }

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_pool_disconnect() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_pool_handle_bcast
 *
 * Purpose:     Broadcast the pool handle.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_pool_handle_bcast(int rank)
{
    daos_iov_t glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    herr_t ret_value = SUCCEED; /* Return value */
    hbool_t err_occurred = FALSE;
    int ret;

    /* Calculate size of global pool handle */
    if((rank == 0) && (0 != (ret = daos_pool_local2global(H5_daos_poh_g, &glob))))
        err_occurred = TRUE; /* Defer goto error to make sure we enter bcast */

    /* Bcast size */
    if(MPI_SUCCESS != MPI_Bcast(&glob.iov_buf_len, 1, MPI_UINT64_T, 0, H5_daos_pool_comm_g))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast global pool handle size")

    /* Error checking */
    if(err_occurred) {
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get global pool handle size: %s", H5_daos_err_to_string(ret))
    } else if(0 == glob.iov_buf_len) {
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "invalid global handle size after bcast")
    }

    /* Allocate buffer */
    if(NULL == (glob.iov_buf = (char *)DV_malloc(glob.iov_buf_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for global pool handle")
    memset(glob.iov_buf, 0, glob.iov_buf_len);
    glob.iov_len = glob.iov_buf_len;

    /* Get global pool handle */
    if((rank == 0) && (0 != (ret = daos_pool_local2global(H5_daos_poh_g, &glob))))
        err_occurred = TRUE;

    /* Bcast handle */
    if(MPI_SUCCESS != MPI_Bcast(glob.iov_buf, (int)glob.iov_buf_len, MPI_BYTE, 0, H5_daos_pool_comm_g))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast global pool handle")

    /* Error checking */
    if(err_occurred) {
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get global pool handle: %s", H5_daos_err_to_string(ret))
    } else {
        size_t i;
        hbool_t non_zeros = FALSE;
        for(i = 0; i < glob.iov_buf_len; i++)
            if(0 != ((char *)(glob.iov_buf))[i]) {
                non_zeros = TRUE;
                break; /* Break if not 0 */
            }
        if(!non_zeros)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "invalid global handle size after bcast")
    }

    /* Get pool handle */
    if((rank != 0) && (0 != (ret = daos_pool_global2local(glob, &H5_daos_poh_g))))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get global pool handle: %s", H5_daos_err_to_string(ret))

done:
    DV_free(glob.iov_buf);

    D_FUNC_LEAVE_API
} /* end H5_daos_pool_handle_bcast() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_fapl_copy
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
H5_daos_fapl_copy(const void *_old_fa)
{
    const H5_daos_fapl_t *old_fa = (const H5_daos_fapl_t*)_old_fa;
    H5_daos_fapl_t       *new_fa = NULL;
    void                 *ret_value = NULL;

    if(!_old_fa)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid fapl")

    if(NULL == (new_fa = (H5_daos_fapl_t *)DV_malloc(sizeof(H5_daos_fapl_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed")

    /* Copy the general information */
    memcpy(new_fa, old_fa, sizeof(H5_daos_fapl_t));

    /* Clear allocated fields, so they aren't freed if something goes wrong.  No
     * need to clear info since it is only freed if comm is not null. */
    new_fa->comm = MPI_COMM_NULL;

    /* Duplicate communicator and Info object. */
    if(FAIL == H5_daos_comm_info_dup(old_fa->comm, old_fa->info, &new_fa->comm, &new_fa->info))
        D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "failed to duplicate MPI communicator and info")

    ret_value = new_fa;

done:
    if(NULL == ret_value) {
        /* cleanup */
        if(new_fa && H5_daos_fapl_free(new_fa) < 0)
            D_DONE_ERROR(H5E_PLIST, H5E_CANTFREE, NULL, "can't free fapl")
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5_daos_fapl_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_fapl_free
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
H5_daos_fapl_free(void *_fa)
{
    H5_daos_fapl_t *fa = (H5_daos_fapl_t*) _fa;
    herr_t          ret_value = SUCCEED;

    if(!_fa)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fapl")

    /* Free the internal communicator and INFO object */
    if(fa->comm != MPI_COMM_NULL)
        if(H5_daos_comm_info_free(&fa->comm, &fa->info) < 0)
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to free copy of MPI communicator and info")

    /* free the struct */
    DV_free(fa);

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_fapl_free() */


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
H5_daos_get_conn_cls(void H5VL_DAOS_UNUSED *item,
    H5VL_get_conn_lvl_t H5VL_DAOS_UNUSED lvl, const H5VL_class_t **conn_cls)
{
    herr_t          ret_value = SUCCEED;

    if(!conn_cls)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "conn_cls parameter not supplied")

    /* Retrieve the DAOS VOL connector class */
    *conn_cls = &H5_daos_g;

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_get_conn_cls() */


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
H5_daos_opt_query(void H5VL_DAOS_UNUSED *item,
    H5VL_subclass_t H5VL_DAOS_UNUSED cls, int H5VL_DAOS_UNUSED opt_type,
    hbool_t *supported)
{
    herr_t          ret_value = SUCCEED;

    if(!supported)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "\"supported\" parameter not supplied")

    /* This VOL connector currently supports no optional operations queried by
     * this function */
    *supported = FALSE;

done:
    D_FUNC_LEAVE_API
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
static herr_t H5_daos_optional(void *item, int op_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    herr_t          ret_value = SUCCEED;

    /* Check operation type */
    switch(op_type) {
        /* H5Mcreate/create_anon */
        case H5VL_MAP_CREATE:
        {
            const H5VL_loc_params_t *loc_params = va_arg(arguments, const H5VL_loc_params_t *);
            const char *name = va_arg(arguments, const char *);
            hid_t lcpl_id = va_arg(arguments, hid_t);
            hid_t ktype_id = va_arg(arguments, hid_t);
            hid_t vtype_id = va_arg(arguments, hid_t);
            hid_t mcpl_id = va_arg(arguments, hid_t);
            hid_t mapl_id = va_arg(arguments, hid_t);
            void **map = va_arg(arguments, void **);

            /* Check map argument.  All other arguments will be checked by
             * H5_daos_map_create. */
            if(!map)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object output parameter is NULL")

            /* Pass the call */
            if(NULL == (*map = H5_daos_map_create(item, loc_params, name, lcpl_id, ktype_id, vtype_id,
                    mcpl_id, mapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create map object")

            break;
        } /* end block */

        /* H5Mopen */
        case H5VL_MAP_OPEN:
        {
            const H5VL_loc_params_t *loc_params = va_arg(arguments, const H5VL_loc_params_t *);
            const char *name = va_arg(arguments, const char *);
            hid_t mapl_id = va_arg(arguments, hid_t);
            void **map = va_arg(arguments, void **);

            /* Check map argument.  All other arguments will be checked by
             * H5_daos_map_open. */
            if(!map)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object output parameter is NULL")

            /* Pass the call */
            if(NULL == (*map = H5_daos_map_open(item, loc_params, name, mapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "can't open map object")

            break;
        } /* end block */

        /* H5Mget */
        case H5VL_MAP_GET_VAL:
        {
            hid_t key_mem_type_id = va_arg(arguments, hid_t);
            const void *key = va_arg(arguments, const void *);
            hid_t val_mem_type_id = va_arg(arguments, hid_t);
            void *value = va_arg(arguments, void *);

            /* All arguments will be checked by H5_daos_map_get_val. */

            /* Pass the call */
            if((ret_value = H5_daos_map_get_val(item, key_mem_type_id, key, val_mem_type_id, value, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_READERROR, ret_value, "can't get value")

            break;
        } /* end block */

        /* H5Mexists */
        case H5VL_MAP_EXISTS:
        {
            hid_t key_mem_type_id = va_arg(arguments, hid_t);
            const void *key = va_arg(arguments, const void *);
            hbool_t *exists = va_arg(arguments, hbool_t *);

            /* All arguments will be checked by H5_daos_map_exists. */

            /* Pass the call */
            if((ret_value = H5_daos_map_exists(item, key_mem_type_id, key, exists, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_READERROR, ret_value, "can't check if value exists")

            break;
        } /* end block */

        /* H5Mput */
        case H5VL_MAP_PUT:
        {
            hid_t key_mem_type_id = va_arg(arguments, hid_t);
            const void *key = va_arg(arguments, const void *);
            hid_t val_mem_type_id = va_arg(arguments, hid_t);
            const void *value = va_arg(arguments, const void *);

            /* All arguments will be checked by H5_daos_map_put. */

            /* Pass the call */
            if((ret_value = H5_daos_map_put(item, key_mem_type_id, key, val_mem_type_id, value, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_WRITEERROR, ret_value, "can't put value")

            break;
        } /* end block */

        /* Operations that get misc info from the map */
        case H5VL_MAP_GET:
        {
            H5VL_map_get_t get_type = va_arg(arguments, H5VL_map_get_t);

            /* All arguments will be checked by H5_daos_map_get. */

            /* Pass the call */
            if((ret_value = H5_daos_map_get(item, get_type, dxpl_id, req, arguments)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, ret_value, "can't perform map get operation")

            break;
        } /* end block */

        /* Specific operations (H5Miterate and H5Mdelete) */
        case H5VL_MAP_SPECIFIC:
        {
            const H5VL_loc_params_t *loc_params = va_arg(arguments, const H5VL_loc_params_t *);
            H5VL_map_specific_t specific_type = va_arg(arguments, H5VL_map_specific_t);

            /* All arguments will be checked by H5_daos_map_specific. */

            /* Pass the call */
            if((ret_value = H5_daos_map_specific(item, loc_params, specific_type, dxpl_id, req, arguments)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret_value, "can't perform specific map operation")

            break;
        } /* end block */

        /* H5Mclose */
        case H5VL_MAP_CLOSE:
        {
            /* Pass the call */
            if((ret_value = H5_daos_map_close(item, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, ret_value, "can't close map object")

            break;
        } /* end block */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported optional operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_oidx_generate
 *
 * Purpose:     Generate a unique 64 bit object index.  This index will be
 *              used as the lower 64 bits of the DAOS object ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_oidx_generate(uint64_t *oidx, H5_daos_file_t *file, hbool_t collective)
{
    uint64_t *next_oidx = collective ? &file->next_oidx_collective : &file->next_oidx;
    uint64_t *max_oidx = collective ? &file->max_oidx_collective : &file->max_oidx;
    int ret;
    int ret_value = SUCCEED;

    /* Allocate more object indices for this process if necessary */
    if((*max_oidx == 0) || (*next_oidx > *max_oidx)) {
        uint8_t next_oidx_buf[H5_DAOS_ENCODED_UINT64_T_SIZE];
        uint8_t *p;

        /* Check if this process should allocate object IDs or just wait for the
         * result from the leader process */
        if(!collective || (file->my_rank == 0)) {
            /* Allocate oidxs */
            if((ret = daos_cont_alloc_oids(file->coh, H5_DAOS_OIDX_NALLOC, next_oidx, NULL /*event*/)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't allocate object indices: %s", H5_daos_err_to_string(ret))

            /* Broadcast next_oidx if there are other processes that need it */
            if(collective && (file->num_procs > 1)) {
                /* Encode next_oidx */
                p = next_oidx_buf;
                UINT64ENCODE(p, *next_oidx)

                /* MPI_Bcast next_oidx_buf */
                if(MPI_SUCCESS != MPI_Bcast((char *)next_oidx_buf, sizeof(next_oidx_buf), MPI_BYTE, 0, file->comm))
                    D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast next object index")
            } /* end if */
        } /* end if */
        else {
            /* Receive next_oidx_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)next_oidx_buf, sizeof(next_oidx_buf), MPI_BYTE, 0, file->comm))
                D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't receive broadcasted next object index")

            /* Decode next_oidx */
            p = next_oidx_buf;
            UINT64DECODE(p, *next_oidx)
        } /* end if */

        /* Set max oidx */
        *max_oidx = *next_oidx + H5_DAOS_OIDX_NALLOC - 1;

        /* Skip over reserved indices */
        assert(H5_DAOS_OIDX_NALLOC > H5_DAOS_OIDX_FIRST_USER);
        if(*next_oidx < H5_DAOS_OIDX_FIRST_USER)
            *next_oidx = H5_DAOS_OIDX_FIRST_USER;
    } /* end if */

    /* Allocate oidx from local allocation */
    assert(*next_oidx <= *max_oidx);
    *oidx = *next_oidx;
    (*next_oidx)++;

done:
    D_FUNC_LEAVE
} /* end H5_daos_oidx_generate() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_oid_encode
 *
 * Purpose:     Create a DAOS OID given the object type and a 64 bit
 *              index.  file must have at least the default_object_class
 *              field set, but may be otherwise uninitialized.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_oid_encode(daos_obj_id_t *oid, uint64_t oidx, H5I_type_t obj_type,
    hid_t crt_plist_id, const char *oclass_prop_name, H5_daos_file_t *file)
{
    daos_oclass_id_t object_class = OC_UNKNOWN;
    daos_ofeat_t object_feats;
    htri_t prop_exists;
    char *oclass_str = NULL;
    int ret_value = SUCCEED;

    /* Initialize oid.lo to oidx */
    oid->lo = oidx;

    /* Set type bits in the upper 2 bits of of the lower 32 of oid.hi (for
     * simplicity so they're in the same location as in the compacted haddr_t
     * form) */
    if(obj_type == H5I_GROUP)
        oid->hi = H5_DAOS_TYPE_GRP;
    else if(obj_type == H5I_DATASET)
        oid->hi = H5_DAOS_TYPE_DSET;
    else if(obj_type == H5I_DATATYPE)
        oid->hi = H5_DAOS_TYPE_DTYPE;
    else {
        assert(obj_type == H5I_MAP);
        oid->hi = H5_DAOS_TYPE_MAP;
    } /* end else */

    /* Set the object feature flags */
    if(H5I_GROUP == obj_type)
        object_feats = DAOS_OF_DKEY_LEXICAL | DAOS_OF_AKEY_LEXICAL;
    else
        object_feats = DAOS_OF_DKEY_HASHED | DAOS_OF_AKEY_LEXICAL;

    /* Check for object class set on crt_plist_id */
    /* Note we do not copy the oclass_str in the property callbacks (there is no
     * "get" callback, so this is more like an H5P_peek, and we do not need to
     * free oclass_str as it points directly into the plist value */
    if(crt_plist_id != H5P_DEFAULT) {
        if((prop_exists = H5Pexist(crt_plist_id, oclass_prop_name)) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")
        if(prop_exists) {
            if(H5Pget(crt_plist_id, oclass_prop_name, &oclass_str) < 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class")
            if(oclass_str && (oclass_str[0] != '\0'))
                if(OC_UNKNOWN == (object_class = daos_oclass_name2id(oclass_str)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unknown object class")
        } /* end if */
    } /* end if */

    /* Check for object class set on file if not set from plist */
    if(object_class == OC_UNKNOWN)
        object_class = file->fapl_cache.default_object_class;

    /* Set the object class by default according to object type if not set from
     * above */
    if(object_class == OC_UNKNOWN)
        object_class = (obj_type == H5I_DATASET) ? OC_SX : OC_S1;

    /* Generate oid */
    H5_daos_obj_generate_id(oid, object_feats, object_class);

done:
    D_FUNC_LEAVE
} /* end H5_daos_oid_encode() */


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
H5_daos_oid_generate(daos_obj_id_t *oid, H5I_type_t obj_type,
    hid_t crt_plist_id, H5_daos_file_t *file, hbool_t collective)
{
    uint64_t oidx;
    int ret_value = SUCCEED;

    /* Generate oidx */
    if(H5_daos_oidx_generate(&oidx, file, collective) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't generate object index")

    /* Encode oid */
    if(H5_daos_oid_encode(oid, oidx, obj_type, crt_plist_id, H5_DAOS_OBJ_CLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTENCODE, FAIL, "can't encode object ID")

done:
    D_FUNC_LEAVE
} /* end H5_daos_oid_generate() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_addr_to_oid
 *
 * Purpose:     Convert a compacted address to an OID
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_addr_to_oid(daos_obj_id_t *oid, haddr_t addr)
{
    int ret_value = SUCCEED;

    /* Check for HADDR_UNDEF */
    if(addr == HADDR_UNDEF)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "address is undefined")

    /* Build OID */
    oid->lo = addr & H5_DAOS_ADDR_OIDLO_MASK;
    oid->hi = addr & H5_DAOS_ADDR_OIDHI_MASK;

done:
    D_FUNC_LEAVE
} /* end H5_daos_addr_to_oid() */


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
    herr_t ret_value = SUCCEED;

    assert(obj_token);
    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE <= H5O_MAX_TOKEN_SIZE);

    p = (uint8_t *) obj_token;

    UINT64ENCODE(p, oid.lo);
    UINT64ENCODE(p, oid.hi);

    D_FUNC_LEAVE
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
H5_daos_token_to_oid(H5O_token_t *obj_token, daos_obj_id_t *oid)
{
    uint8_t *p;
    herr_t ret_value = SUCCEED;

    assert(obj_token);
    assert(oid);
    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE <= H5O_MAX_TOKEN_SIZE);

    p = (uint8_t *) obj_token;

    UINT64DECODE(p, oid->lo);
    UINT64DECODE(p, oid->hi);

    D_FUNC_LEAVE
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
    if(type_bits == H5_DAOS_TYPE_GRP)
        return(H5I_GROUP);
    else if(type_bits == H5_DAOS_TYPE_DSET)
        return(H5I_DATASET);
    else if(type_bits == H5_DAOS_TYPE_DTYPE)
        return(H5I_DATATYPE);
    else if(type_bits == H5_DAOS_TYPE_MAP)
        return(H5I_MAP);
    else
        return(H5I_BADID);
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
H5_daos_mult128(uint64_t x_lo, uint64_t x_hi, uint64_t y_lo, uint64_t y_hi,
    uint64_t *ans_lo, uint64_t *ans_hi)
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
    uint8_t *hash_p = (uint8_t *)hash;
    uint64_t name_lo;
    uint64_t name_hi;
    /* Initialize hash value in accordance with the FNV algorithm */
    uint64_t hash_lo = 0x62b821756295c58d;
    uint64_t hash_hi = 0x6c62272e07bb0142;
    /* Initialize FNV prime number in accordance with the FNV algorithm */
    const uint64_t fnv_prime_lo = 0x13b;
    const uint64_t fnv_prime_hi = 0x1000000;
    size_t name_len_rem;

    assert(name);
    assert(hash);

    name_len_rem = strlen(name);

    while(name_len_rem > 0) {
        /* "Decode" lower 64 bits of this 128 bit section of the name, so the
         * numberical value of the integer is the same on both little endian and
         * big endian systems */
        if(name_len_rem >= 8) {
            UINT64DECODE(name_p, name_lo)
            name_len_rem -= 8;
        } /* end if */
        else {
            name_lo = 0;
            UINT64DECODE_VAR(name_p, name_lo, name_len_rem)
            name_len_rem = 0;
        } /* end else */

        /* "Decode" second 64 bits */
        if(name_len_rem > 0) {
            if(name_len_rem >= 8) {
                UINT64DECODE(name_p, name_hi)
                name_len_rem -= 8;
            } /* end if */
            else {
                name_hi = 0;
                UINT64DECODE_VAR(name_p, name_hi, name_len_rem)
                name_len_rem = 0;
            } /* end else */
        } /* end if */
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
    int ret;
    int ret_value = 0;

    /* Get private data */
    req = tse_task_get_priv(task);

    /* Close transaction */
    if(0 != (ret = daos_tx_close(req->th, NULL /*event*/)))
        D_GOTO_ERROR(H5E_IO, H5E_CLOSEERROR, ret, "can't close transaction: %s", H5_daos_err_to_string(ret))
    req->th_open = FALSE;

    /* Mark request as completed */
    if(req->status == H5_DAOS_INCOMPLETE)
        req->status = 0;

    /* Release our reference to req */
    H5_daos_req_free_int(req);

done:
    D_FUNC_LEAVE
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
    int ret;
    int ret_value = 0;

    /* Get private data */
    req = tse_task_get_priv(task);

    /* Perform operation */
    if((ret = H5_daos_h5op_finalize_helper(req)) < 0 && req->status >= H5_DAOS_INCOMPLETE)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, ret, "failed to finalize H5 operation")

    /* Report failures in this routine */
    if(ret_value < 0 && req->status == H5_DAOS_INCOMPLETE) {
        req->status = ret_value;
        req->failed_task = "h5 op finalize";
    } /* end if */

    /* Complete task in engine */
    tse_task_complete(task, ret_value);

    /* Release our reference to req */
    H5_daos_req_free_int(req);

    D_FUNC_LEAVE
} /* end H5_daos_h5op_finalize() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_h5op_finalize_helper
 *
 * Purpose:     Like H5_daos_h5op_finalize but operates directly on a
 *              request, and can be called directly instead of through the
 *              task engine.  Does not release req.
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
H5_daos_h5op_finalize_helper(H5_daos_req_t *req)
{
    hbool_t close_tx = FALSE;
    int ret;
    int ret_value = 0;

    assert(!req->file->closed);

    /* Check for error */
    if(req->status < H5_DAOS_INCOMPLETE) {
        /* Print error message */
        D_DONE_ERROR(H5E_IO, H5E_CANTINIT, req->status, "operation failed in task \"%s\": %s", req->failed_task, H5_daos_err_to_string(req->status))

        /* Abort transaction if opened */
        if(req->th_open) {
            tse_task_t *abort_task;
            daos_tx_abort_t *abort_args;

            /* Create task */
            if(0 != (ret = daos_task_create(DAOS_OPC_TX_ABORT, &req->file->sched, 0, NULL, &abort_task))) {
                close_tx = TRUE;
                req->th_open = FALSE;
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't create task to abort transaction: %s", H5_daos_err_to_string(ret))
            } /* end if */

            /* Set arguments */
            abort_args = daos_task_get_args(abort_task);
            abort_args->th = req->th;

            /* Register callback to close transaction */
            if(0 != (ret = tse_task_register_comp_cb(abort_task, H5_daos_tx_comp_cb, NULL, 0))) {
                close_tx = TRUE;
                req->th_open = FALSE;
                tse_task_complete(abort_task, ret_value);
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't register callback to close transaction: %s", H5_daos_err_to_string(ret))
            } /* end if */

            /* Set private data for abort */
            (void)tse_task_set_priv(abort_task, req);

            /* Schedule abort task */
            if(0 != (ret = tse_task_schedule(abort_task, false))) {
                close_tx = TRUE;
                req->th_open = FALSE;
                tse_task_complete(abort_task, ret_value);
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't schedule task to abort transaction: %s", H5_daos_err_to_string(ret))
            } /* end if */
            req->rc++;
        } /* end if */
    } /* end if */
    else {
        /* Commit transaction if opened */
        if(req->th_open) {
            tse_task_t *commit_task;
            daos_tx_commit_t *commit_args;

            /* Create task */
            if(0 != (ret = daos_task_create(DAOS_OPC_TX_COMMIT, &req->file->sched, 0, NULL, &commit_task))) {
                close_tx = TRUE;
                req->th_open = FALSE;
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't create task to commit transaction: %s", H5_daos_err_to_string(ret))
            } /* end if */

            /* Set arguments */
            commit_args = daos_task_get_args(commit_task);
            commit_args->th = req->th;

            /* Register callback to close transaction */
            if(0 != (ret = tse_task_register_comp_cb(commit_task, H5_daos_tx_comp_cb, NULL, 0))) {
                close_tx = TRUE;
                req->th_open = FALSE;
                tse_task_complete(commit_task, ret_value);
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't register callback to close transaction: %s", H5_daos_err_to_string(ret))
            } /* end if */

            /* Set private data for commit */
            (void)tse_task_set_priv(commit_task, req);

            /* Schedule commit task */
            if(0 != (ret = tse_task_schedule(commit_task, false))) {
                close_tx = TRUE;
                req->th_open = FALSE;
                tse_task_complete(commit_task, ret_value);
                D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, ret, "can't schedule task to commit transaction: %s", H5_daos_err_to_string(ret))
            } /* end if */
            req->rc++;
        } /* end if */
    } /* end else */

done:
    if(close_tx) {
        if(0 != (ret = daos_tx_close(req->th, NULL /*event*/)))
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, ret, "can't close transaction: %s", H5_daos_err_to_string(ret))
        req->th_open = FALSE;
    } /* end if */

    if(req->th_open)
        /* Progress schedule */
        tse_sched_progress(&req->file->sched);
    else {
        /* Mark request as completed */
        if(ret_value >= 0 && req->status == H5_DAOS_INCOMPLETE)
            req->status = 0;
    } /* end else */

    D_FUNC_LEAVE
} /* end H5_daos_h5op_finalize_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_md_update_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_update for
 *              metadata writes.  Currently checks for errors from
 *              previous tasks then sets arguments for daos_obj_update.
 *
 * Return:      0 (Never fails)
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_md_update_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_md_update_cb_ud_t *udata;
    daos_obj_rw_t *update_args;

    /* Get private data */
    udata = tse_task_get_priv(task);

    assert(udata);
    assert(udata->obj);
    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);

    /* Handle errors */
    if(udata->req->status < H5_DAOS_INCOMPLETE)
        tse_task_complete(task, H5_DAOS_PRE_ERROR);

    /* Set update task arguments */
    update_args = daos_task_get_args(task);
    update_args->oh = udata->obj->obj_oh;
    update_args->th = DAOS_TX_NONE;
    update_args->flags = 0;
    update_args->dkey = &udata->dkey;
    update_args->nr = udata->nr;
    update_args->iods = udata->iod;
    update_args->sgls = udata->sgl;

    return 0;
} /* end H5_daos_md_update_prep_cb() */


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
    H5_daos_md_update_cb_ud_t *udata;
    unsigned i;
    int ret_value = 0;

    /* Get private data */
    udata = tse_task_get_priv(task);

    assert(!udata->req->file->closed);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */

    /* Close object */
    if(H5_daos_object_close(udata->obj, H5I_INVALID_HID, NULL) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close object")

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block */
    if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = ret_value;
        udata->req->failed_task = udata->task_name;
    } /* end if */

    /* Free private data */
    H5_daos_req_free_int(udata->req);
    if(udata->free_dkey)
        DV_free(udata->dkey.iov_buf);
    if(udata->free_akeys)
        for(i = 0; i < udata->nr; i++)
            DV_free(udata->iod[i].iod_name.iov_buf);
    for(i = 0; i < udata->nr; i++)
        DV_free(udata->sg_iov[i].iov_buf);
    DV_free(udata);

    return ret_value;
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
    int ret_value = 0;

    assert(!H5_daos_mpi_task);

    /* Get private data */
    udata = tse_task_get_priv(task);

    assert(!udata->req->file->closed);

    /* Make call to MPI_Ibcast */
    if(MPI_SUCCESS != MPI_Ibcast(udata->buffer, udata->count, MPI_BYTE, 0, udata->obj->item.file->comm, &H5_daos_mpi_req))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, H5_DAOS_MPI_ERROR, "MPI_Ibcast failed")

    /* Register this task as the current in-flight MPI task */
    H5_daos_mpi_task = task;

    /* This task will be completed by the progress function once that function
     * detects that the MPI request is finished */

done:
    D_FUNC_LEAVE
} /* end H5_daos_mpi_ibcast_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_progress
 *
 * Purpose:     Make progress on asynchronous tasks.  Can be run
 *
 * Return:      Success:    Non-negative.  The new communicator and info
 *                          object handles are returned via the comm_new
 *                          and info_new pointers.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_progress(H5_daos_file_t *file, H5_daos_progress_mode_t mode)
{
    int      completed;
    bool     is_empty = FALSE;
    int      ret;
    herr_t   ret_value = SUCCEED;

    assert(file);

    /* Different loops for kick and wait - do it this way to minimize the amount
     * of cycles in the wait loop, so we can set the polling interval as tight
     * as possible without using too much CPU time */
    if(mode == H5_DAOS_PROGRESS_KICK) {
        /* Progress MPI if there is a task in flight */
        if(H5_daos_mpi_task) {
            /* Check if task is complete */
            if(MPI_SUCCESS != (ret = MPI_Test(&H5_daos_mpi_req, &completed, MPI_STATUS_IGNORE)))
                D_DONE_ERROR(H5E_VOL, H5E_MPI, FAIL, "MPI_Test failed: %d", ret)

            /* Complete matching DAOS task if so */
            if(ret_value < 0) {
                tse_task_complete(H5_daos_mpi_task, H5_DAOS_MPI_ERROR);
                H5_daos_mpi_task = NULL;
            } /* end if */
            else if(completed) {
                tse_task_complete(H5_daos_mpi_task, 0);
                H5_daos_mpi_task = NULL;
            } /* end if */
        } /* end if */

        /* Progress DAOS */
        if(0 != (ret = daos_progress(&file->sched, DAOS_EQ_NOWAIT, &is_empty)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else {
        assert(mode == H5_DAOS_PROGRESS_WAIT);

        /* Loop until the scheduler is finished */
        do {
            /* Progress MPI if there is a task in flight */
            if(H5_daos_mpi_task) {
                /* Check if task is complete */
                if(MPI_SUCCESS != (ret = MPI_Test(&H5_daos_mpi_req, &completed, MPI_STATUS_IGNORE)))
                    D_DONE_ERROR(H5E_VOL, H5E_MPI, FAIL, "MPI_Test failed: %d", ret)

                /* Complete matching DAOS task if so */
                if(ret_value < 0) {
                    tse_task_complete(H5_daos_mpi_task, H5_DAOS_MPI_ERROR);
                    H5_daos_mpi_task = NULL;
                } /* end if */
                else if(completed) {
                    tse_task_complete(H5_daos_mpi_task, 0);
                    H5_daos_mpi_task = NULL;
                } /* end if */
            } /* end if */

            /* Progress DAOS */
            if(0 != (ret = daos_progress(&file->sched, H5_DAOS_ASYNC_POLL_INTERVAL, &is_empty)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler: %s", H5_daos_err_to_string(ret))
        } while(!is_empty);
    } /* end else */

done:
    D_FUNC_LEAVE
} /* end H5_daos_progress() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_comm_info_dup
 *
 * Purpose:     Make duplicates of MPI communicator and info objects.
 *              If the info object is in fact MPI_INFO_NULL, no duplicate
 *              is made but the same value is assigned to the 'info_new'
 *              object handle.
 *
 * Return:      Success:    Non-negative.  The new communicator and info
 *                          object handles are returned via the comm_new
 *                          and info_new pointers.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_comm_info_dup(MPI_Comm comm, MPI_Info info,
    MPI_Comm *comm_new, MPI_Info *info_new)
{
    MPI_Comm comm_dup = MPI_COMM_NULL;
    MPI_Info info_dup = MPI_INFO_NULL;
    int      mpi_code;
    herr_t   ret_value = SUCCEED;

    /* Check arguments */
    if(MPI_COMM_NULL == comm)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid MPI communicator -- MPI_COMM_NULL")
    if(!comm_new)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "comm_new pointer is NULL")
    if(!info_new)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "info_new pointer is NULL")

    /* Duplicate the MPI objects. Temporary variables are used for error recovery cleanup. */
    if(MPI_SUCCESS != (mpi_code = MPI_Comm_dup(comm, &comm_dup)))
        D_GOTO_ERROR(H5E_INTERNAL, H5E_MPI, FAIL, "MPI_Comm_dup failed: %d", mpi_code)
    if(MPI_INFO_NULL != info) {
        if(MPI_SUCCESS != (mpi_code = MPI_Info_dup(info, &info_dup)))
            D_GOTO_ERROR(H5E_INTERNAL, H5E_MPI, FAIL, "MPI_Info_dup failed: %d", mpi_code)
    }
    else {
        info_dup = info;
    }

    /* Set MPI_ERRORS_RETURN on comm_dup so that MPI failures are not fatal,
       and return codes can be checked and handled. May 23, 2017 FTW */
    if(MPI_SUCCESS != (mpi_code = MPI_Comm_set_errhandler(comm_dup, MPI_ERRORS_RETURN)))
        D_GOTO_ERROR(H5E_INTERNAL, H5E_MPI, FAIL, "MPI_Comm_set_errhandler failed: %d", mpi_code)

    /* Copy the duplicated MPI objects to the return arguments. */
    *comm_new = comm_dup;
    *info_new = info_dup;

done:
    if(FAIL == ret_value) {
        /* Need to free anything created */
        if(MPI_COMM_NULL != comm_dup)
            MPI_Comm_free(&comm_dup);
        if(MPI_INFO_NULL != info_dup)
            MPI_Info_free(&info_dup);
    }

    D_FUNC_LEAVE
} /* end H5_daos_comm_info_dup() */


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
    if(!comm)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "comm pointer is NULL")
    if(!info)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "info pointer is NULL")

    if(MPI_COMM_NULL != *comm)
        MPI_Comm_free(comm);
    if(MPI_INFO_NULL != *info)
        MPI_Info_free(info);

done:
    D_FUNC_LEAVE
} /* end H5_daos_comm_info_free() */


H5PL_type_t
H5PLget_plugin_type(void) {
    return H5PL_TYPE_VOL;
}


const void*
H5PLget_plugin_info(void) {
    return &H5_daos_g;
}

