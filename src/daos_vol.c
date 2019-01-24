/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
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
 * Programmer:  Neil Fortner <nfortne2@hdfgroup.org>
 *              September, 2016
 *
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 *          library.  General connector routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */
#include "daos_vol_config.h"    /* DAOS connector configuration header     */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Prototypes */
static void *H5_daos_fapl_copy(const void *_old_fa);
static herr_t H5_daos_fapl_free(void *_fa);
static herr_t H5_daos_term(void);

/* Declarations */
/* The DAOS VOL connector struct */
static H5VL_class_t H5_daos_g = {
    HDF5_VOL_DAOS_VERSION_1,                 /* Plugin Version number */
    H5_VOL_DAOS_CLS_VAL,                     /* Plugin Value */
    H5_DAOS_VOL_NAME,                        /* Plugin Name */
    0,                                       /* Plugin capability flags */
    H5_daos_init,                            /* Plugin initialize */
    H5_daos_term,                            /* Plugin terminate */
    sizeof(H5_daos_fapl_t),                  /* Plugin Info size */
    H5_daos_fapl_copy,                       /* Plugin Info copy */
    NULL,                                    /* Plugin Info compare */
    H5_daos_fapl_free,                       /* Plugin Info free */
    NULL,                                    /* Plugin Info To String */
    NULL,                                    /* Plugin String To Info */
    NULL,                                    /* Plugin Get Object */
    NULL,                                    /* Plugin Get Wrap Ctx */
    NULL,                                    /* Plugin Wrap Object */
    NULL,                                    /* Plugin Free Wrap Ctx */
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
        NULL,/*H5_daos_dataset_specific,*/   /* Plugin Dataset specific */
        NULL,                                /* Plugin Dataset optional */
        H5_daos_dataset_close                /* Plugin Dataset close */
    },
    {                                        /* Plugin Datatype cls */
        H5_daos_datatype_commit,             /* Plugin Datatype commit */
        H5_daos_datatype_open,               /* Plugin Datatype open */
        H5_daos_datatype_get,                /* Plugin Datatype get */
        NULL,                                /* Plugin Datatype specific */
        NULL,                                /* Plugin Datatype optional */
        H5_daos_datatype_close               /* Plugin Datatype close */
    },
    {                                        /* Plugin File cls */
        H5_daos_file_create,                 /* Plugin File create */
        H5_daos_file_open,                   /* Plugin File open */
        NULL,/*H5_daos_file_get,*/           /* Plugin File get */
        H5_daos_file_specific,               /* Plugin File specific */
        NULL,                                /* Plugin File optional */
        H5_daos_file_close                   /* Plugin File close */
    },
    {                                        /* Plugin Group cls */
        H5_daos_group_create,                /* Plugin Group create */
        H5_daos_group_open,                  /* Plugin Group open */
        NULL,/*H5_daos_group_get,*/          /* Plugin Group get */
        NULL,                                /* Plugin Group specific */
        NULL,                                /* Plugin Group optional */
        H5_daos_group_close                  /* Plugin Group close */
    },
    {                                        /* Plugin Link cls */
        H5_daos_link_create,                 /* Plugin Link create */
        NULL,/*H5_daos_link_copy,*/          /* Plugin Link copy */
        NULL,/*H5_daos_link_move,*/          /* Plugin Link move */
        NULL,/*H5_daos_link_get,*/           /* Plugin Link get */
        H5_daos_link_specific,               /* Plugin Link specific */
        NULL                                 /* Plugin Link optional */
    },
    {                                        /* Plugin Object cls */
        H5_daos_object_open,                 /* Plugin Object open */
        NULL,                                /* Plugin Object copy */
        NULL,                                /* Plugin Object get */
        NULL,/*H5_daos_object_specific,*/    /* Plugin Object specific */
        H5_daos_object_optional              /* Plugin Object optional */
    },
    {
        NULL,                                /* Plugin Request wait */
        NULL,                                /* Plugin Request notify */
        NULL,                                /* Plugin Request cancel */
        NULL,                                /* Plugin Request specific */
        NULL,                                /* Plugin Request optional */
        NULL,                                /* Plugin Request free */
    },
    NULL                                     /* Plugin optional */
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
daos_handle_t H5_daos_poh_g = {0}; /* Hack! use a DAOS macro if a usable one is created DSINC */

/* Global variables used to open the pool */
hbool_t pool_globals_set_g = FALSE;
MPI_Comm pool_comm_g;
uuid_t pool_uuid_g;
char *pool_grp_g = NULL;


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
H5daos_init(MPI_Comm pool_comm, uuid_t pool_uuid, char *pool_grp)
{
    H5I_type_t idType = H5I_UNINIT;
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(MPI_COMM_NULL == pool_comm)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid MPI communicator")

    /* Initialize HDF5 */
    if(H5open() < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "HDF5 failed to initialize")

    if(H5_DAOS_g >= 0 && (idType = H5Iget_type(H5_DAOS_g)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "failed to retrieve DAOS VOL connector's ID type")

    /* Register the DAOS VOL, if it isn't already */
    if(H5I_VOL != idType) {
        htri_t is_registered;

        if((is_registered = H5VLis_connector_registered(H5_daos_g.name)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTINIT, FAIL, "can't determine if DAOS VOL connector is registered")

        if(!is_registered) {
            /* Save arguments to globals */
            pool_comm_g = pool_comm;
            memcpy(pool_uuid_g, pool_uuid, sizeof(uuid_t));
            pool_grp_g = pool_grp;
            pool_globals_set_g = TRUE;

            /* Register connector */
            if((H5_DAOS_g = H5VLregister_connector((const H5VL_class_t *)&H5_daos_g, H5P_DEFAULT)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTINSERT, FAIL, "can't create ID for DAOS VOL connector")
        } /* end if */
        else {
            if((H5_DAOS_g = H5VLget_connector_id(H5_daos_g.name)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTGET, FAIL, "unable to get registered ID for DAOS VOL connector")
        } /* end else */
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5daos_init() */


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
H5_daos_init(hid_t vipl_id)
{
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id_default;
#endif
    int pool_rank;
    int pool_num_procs;
    daos_iov_t glob;
    uint64_t gh_buf_size;
    char gh_buf_static[H5_DAOS_GH_BUF_SIZE];
    char *gh_buf_dyn = NULL;
    char *gh_buf = gh_buf_static;
    uint8_t *p;
    hbool_t must_bcast = FALSE;
    int ret;
    herr_t ret_value = SUCCEED;            /* Return value */

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
    if(!pool_globals_set_g) {
        pool_comm_g = MPI_COMM_WORLD;
        memset(pool_uuid_g, 0, sizeof(pool_uuid_g));
        assert(!pool_grp_g);
    } /* end if */

    /* Obtain the process rank and size from the communicator attached to the
     * fapl ID */
    MPI_Comm_rank(pool_comm_g, &pool_rank);
    MPI_Comm_size(pool_comm_g, &pool_num_procs);

    if(pool_rank == 0) {
        char *svcl_str = NULL;
        daos_pool_info_t pool_info;
        d_rank_list_t *svcl = NULL;
        char *uuid_str = NULL;
        uuid_t pool_uuid;

        /* If there are other processes and we fail we must bcast anyways so they
         * don't hang */
        if(pool_num_procs > 1)
            must_bcast = TRUE;

        if(NULL != (uuid_str = getenv("DAOS_POOL"))) {
            if(uuid_parse(uuid_str, pool_uuid) < 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "failed to parse pool UUID from environment")
            printf("POOL UUID = %s\n", uuid_str);
        }
        else {
            char uuid_buf[37];

            memcpy(pool_uuid, pool_uuid_g, sizeof(uuid_t));
            uuid_unparse(pool_uuid, uuid_buf);
            printf("POOL UUID = %s\n", uuid_buf);
        }

        if(NULL != (svcl_str = getenv("DAOS_SVCL"))) {
            /* DSINC - this function creates an unavoidable memory leak, as the function
             * to free the memory it allocates is not currently exposed for use.
             */
            if(NULL == (svcl = daos_rank_list_parse(svcl_str, ":")))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "failed to parse SVC list from environment")
        }
        printf("SVC LIST = %s\n", svcl_str);

        /* Connect to the pool */
        if(0 != (ret = daos_pool_connect(pool_uuid, pool_grp_g, svcl, DAOS_PC_RW, &H5_daos_poh_g, &pool_info, NULL /*event*/)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't connect to pool: %s", H5_daos_err_to_string(ret))

        /* Bcast pool handle if there are other processes */
        if(pool_num_procs > 1) {
            /* Calculate size of global pool handle */
            glob.iov_buf = NULL;
            glob.iov_buf_len = 0;
            glob.iov_len = 0;
            if(0 != (ret = daos_pool_local2global(H5_daos_poh_g, &glob)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get global pool handle size: %s", H5_daos_err_to_string(ret))
            gh_buf_size = (uint64_t)glob.iov_buf_len;

            /* Check if the global handle won't fit into the static buffer */
            assert(sizeof(gh_buf_static) >= sizeof(uint64_t));
            if(gh_buf_size + sizeof(uint64_t) > sizeof(gh_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (gh_buf_dyn = (char *)DV_malloc(gh_buf_size + sizeof(uint64_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for global pool handle")

                /* Use dynamic buffer */
                gh_buf = gh_buf_dyn;
            } /* end if */

            /* Encode handle length */
            p = (uint8_t *)gh_buf;
            UINT64ENCODE(p, gh_buf_size)

            /* Get global pool handle */
            glob.iov_buf = (char *)p;
            glob.iov_buf_len = gh_buf_size;
            glob.iov_len = 0;
            if(0 != (ret = daos_pool_local2global(H5_daos_poh_g, &glob)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get global pool handle: %s", H5_daos_err_to_string(ret))
            assert(glob.iov_len == glob.iov_buf_len);

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast gh_buf */
            if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)sizeof(gh_buf_static), MPI_BYTE, 0, pool_comm_g))
                D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast global pool handle")

            /* Need a second bcast if we had to allocate a dynamic buffer */
            if(gh_buf == gh_buf_dyn)
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)gh_buf_size, MPI_BYTE, 0, pool_comm_g))
                    D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast global pool handle (second broadcast)")
        } /* end if */
    } /* end if */
    else {
        /* Receive global handle */
        if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)sizeof(gh_buf_static), MPI_BYTE, 0, pool_comm_g))
            D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't receive broadcasted global pool handle")

        /* Decode handle length */
        p = (uint8_t *)gh_buf;
        UINT64DECODE(p, gh_buf_size)

        /* Check for gh_buf_size set to 0 - indicates failure */
        if(gh_buf_size == 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "lead process failed to initialize")

        /* Check if we need to perform another bcast */
        if(gh_buf_size + sizeof(uint64_t) > sizeof(gh_buf_static)) {
            /* Check if we need to allocate a dynamic buffer */
            if(gh_buf_size > sizeof(gh_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (gh_buf_dyn = (char *)DV_malloc(gh_buf_size)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for global pool handle")
                gh_buf = gh_buf_dyn;
            } /* end if */

            /* Receive global handle */
            if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)gh_buf_size, MPI_BYTE, 0, pool_comm_g))
                D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't receive broadcasted global pool handle (second broadcast)")

            p = (uint8_t *)gh_buf;
        } /* end if */

        /* Create local pool handle */
        glob.iov_buf = (char *)p;
        glob.iov_buf_len = gh_buf_size;
        glob.iov_len = gh_buf_size;
        if(0 != (ret = daos_pool_global2local(glob, &H5_daos_poh_g)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTOPENOBJ, FAIL, "can't get local pool handle: %s", H5_daos_err_to_string(ret))
    } /* end else */

done:
    if(ret_value < 0) {
        /* Bcast gh_buf as '0' if necessary - this will trigger failures in the
         * other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(gh_buf_static, 0, sizeof(gh_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(gh_buf_static, sizeof(gh_buf_static), MPI_BYTE, 0, pool_comm_g))
                D_DONE_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast empty global handle")
        } /* end if */

        H5daos_term();
    } /* end if */

    DV_free(gh_buf_dyn);

    D_FUNC_LEAVE
} /* end H5_daos_init() */


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
    int ret;
    herr_t ret_value = SUCCEED;

    if(H5_DAOS_g >= 0) {
        /* Disconnect from pool */
        if(!daos_handle_is_inval(H5_daos_poh_g)) {
            if(0 != (ret = daos_pool_disconnect(H5_daos_poh_g, NULL /*event*/)))
                D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't disconnect from pool: %s", H5_daos_err_to_string(ret))
            H5_daos_poh_g = DAOS_HDL_INVAL;
        } /* end if */

        /* Terminate DAOS */
        if(daos_fini() < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "DAOS failed to terminate")

#ifdef DV_HAVE_SNAP_OPEN_ID
        /* Unregister the DAOS SNAP_OPEN_ID property from HDF5 */
        if(H5Punregister(H5P_FILE_ACCESS, H5_DAOS_SNAP_OPEN_ID) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't unregister DAOS SNAP_OPEN_ID property")
#endif
    } /* end if */

done:
    /* "Forget" connector id.  This should normally be called by the library
     * when it is closing the id, so no need to close it here. */
    H5_DAOS_g = -1;

    D_FUNC_LEAVE
} /* end H5_daos_term() */


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
    if(FAIL == H5FDmpi_comm_info_dup(old_fa->comm, old_fa->info, &new_fa->comm, &new_fa->info))
        D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "failed to duplicate MPI communicator and info")

    ret_value = new_fa;

done:
    if(NULL == ret_value) {
        /* cleanup */
        if(new_fa && H5_daos_fapl_free(new_fa) < 0)
            D_DONE_ERROR(H5E_PLIST, H5E_CANTFREE, NULL, "can't free fapl")
    } /* end if */

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
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
        if(H5FDmpi_comm_info_free(&fa->comm, &fa->info) < 0)
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to free copy of MPI communicator and info")

    /* free the struct */
    DV_free(fa);

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_fapl_free() */


/* Create a DAOS OID given the object type and a 64 bit address (with the object
 * type already encoded) */
void
H5_daos_oid_generate(daos_obj_id_t *oid, uint64_t addr, H5I_type_t obj_type)
{
    assert(oid);

    /* Encode type and address */
    oid->lo = addr;

    /* Generate oid */
    daos_obj_generate_id(oid, DAOS_OF_DKEY_HASHED | DAOS_OF_AKEY_HASHED,
            obj_type == H5I_DATASET ? DAOS_OC_LARGE_RW : DAOS_OC_TINY_RW);

    return;
} /* end H5_daos_oid_generate() */


/* Create a DAOS OID given the object type and a 64 bit index (top 2 bits are
 * ignored) */
void
H5_daos_oid_encode(daos_obj_id_t *oid, uint64_t idx, H5I_type_t obj_type)
{
    uint64_t type_bits;

    /* Set type_bits */
    if(obj_type == H5I_GROUP)
        type_bits = H5_DAOS_TYPE_GRP;
    else if(obj_type == H5I_DATASET)
        type_bits = H5_DAOS_TYPE_DSET;
    else if(obj_type == H5I_DATATYPE)
        type_bits = H5_DAOS_TYPE_DTYPE;
    else {
#ifdef DV_HAVE_MAP
        assert(obj_type == H5I_MAP);
#endif
        type_bits = H5_DAOS_TYPE_MAP;
    } /* end else */

    /* Encode type and address and generate oid */
    H5_daos_oid_generate(oid, type_bits | (idx & H5_DAOS_IDX_MASK), obj_type);

    return;
} /* end H5_daos_oid_encode() */


/* Retrieve the 64 bit address from a DAOS OID */
H5I_type_t
H5_daos_addr_to_type(uint64_t addr)
{
    uint64_t type_bits;

    /* Retrieve type */
    type_bits = addr & H5_DAOS_TYPE_MASK;
    if(type_bits == H5_DAOS_TYPE_GRP)
        return(H5I_GROUP);
    else if(type_bits == H5_DAOS_TYPE_DSET)
        return(H5I_DATASET);
    else if(type_bits == H5_DAOS_TYPE_DTYPE)
        return(H5I_DATATYPE);
#ifdef DV_HAVE_MAP
    else if(type_bits == H5_DAOS_TYPE_MAP)
        return(H5I_MAP);
#endif
    else
        return(H5I_BADID);
} /* end H5_daos_addr_to_type() */


/* Retrieve the 64 bit address from a DAOS OID */
H5I_type_t
H5_daos_oid_to_type(daos_obj_id_t oid)
{
    /* Retrieve type */
    return H5_daos_addr_to_type(oid.lo);
} /* end H5_daos_oid_to_type() */


/* Retrieve the 64 bit object index from a DAOS OID */
uint64_t
H5_daos_oid_to_idx(daos_obj_id_t oid)
{
    return oid.lo & H5_DAOS_IDX_MASK;
} /* end H5_daos_oid_to_idx() */


/* Multiply two 128 bit unsigned integers to yield a 128 bit unsigned integer */
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


/* Implementation of the FNV hash algorithm */
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
 * Function:    H5_daos_write_max_oid
 *
 * Purpose:     Writes the max OID (object index) to the global metadata
 *              object
 *
 * Return:      Success:        0
 *              Failure:        1
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_write_max_oid(H5_daos_file_t *file)
{
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    char int_md_key[] = H5_DAOS_INT_MD_KEY;
    char max_oid_key[] = H5_DAOS_MAX_OID_KEY;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Set up dkey */
    daos_iov_set(&dkey, int_md_key, (daos_size_t)(sizeof(int_md_key) - 1));

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)max_oid_key, (daos_size_t)(sizeof(max_oid_key) - 1));
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = (uint64_t)8;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, &file->max_oid, (daos_size_t)8);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Write max OID to gmd obj */
    if(0 != (ret = daos_obj_update(file->glob_md_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, FAIL, "can't write max OID to global metadata object: %s", H5_daos_err_to_string(ret))
done:
    D_FUNC_LEAVE
} /* end H5_daos_write_max_oid() */


H5PL_type_t
H5PLget_plugin_type(void) {
    return H5PL_TYPE_VOL;
}


const void*
H5PLget_plugin_info(void) {
    return &H5_daos_g;
}

