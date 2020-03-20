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
 * library.  File routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/************************************/
/* Local Type and Struct Definition */
/************************************/

typedef struct get_obj_count_udata_t {
    uuid_t file_id;
    ssize_t obj_count;
} get_obj_count_udata_t;

typedef struct get_obj_ids_udata_t {
    uuid_t file_id;
    size_t max_objs;
    hid_t *oid_list;
    size_t obj_count;
} get_obj_ids_udata_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_cont_get_fapl_info(hid_t fapl_id, H5_daos_fapl_t *fa_out);
static herr_t H5_daos_cont_set_mpi_info(H5_daos_file_t *file, H5_daos_fapl_t *fa);
static int H5_daos_tx_open_comp_cb(tse_task_t *task, void *args);
static int H5_daos_tx_open_prep_cb(tse_task_t *task, void *args);
static herr_t H5_daos_cont_open(H5_daos_file_t *file, unsigned flags,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_cont_destroy_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_cont_create(H5_daos_file_t *file, unsigned flags,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_gh_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_cont_handle_bcast(H5_daos_file_t *file,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_get_gch_task(tse_task_t *task);

static herr_t H5_daos_fill_fapl_cache(H5_daos_file_t *file, hid_t fapl_id);
static herr_t H5_daos_file_close_helper(H5_daos_file_t *file,
    hid_t dxpl_id, void **req);
static herr_t H5_daos_get_obj_count_callback(hid_t id, void *udata);
static herr_t H5_daos_get_obj_ids_callback(hid_t id, void *udata);


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_get_fapl_info
 *
 * Purpose:     Retrieve needed information from the given FAPL ID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_get_fapl_info(hid_t fapl_id, H5_daos_fapl_t *fa_out)
{
    H5_daos_fapl_t *local_fapl_info = NULL;
    herr_t ret_value = SUCCEED;

    /*
     * First, check to see if any MPI info was set through the use of
     * a H5Pset_fapl_daos() call.
     */
    if(H5Pget_vol_info(fapl_id, (void **) &local_fapl_info) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL info struct")
    if(local_fapl_info) {
        fa_out->comm = local_fapl_info->comm;
        fa_out->info = local_fapl_info->info;
    }
    else {
        hid_t driver_id;

        /*
         * If no info was set using H5Pset_fapl_daos(), see if the application
         * set any MPI info by using HDF5's H5Pset_fapl_mpio().
         */
        if((driver_id = H5Pget_driver(fapl_id)) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't determine if a MPI-based HDF5 VFD was requested for file access")
        if(H5FD_MPIO == driver_id) {
            if(H5Pget_fapl_mpio(fapl_id, &fa_out->comm, &fa_out->info) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get HDF5 MPI information")
        }
        else {
            /*
             * If no MPI info was set (as in the case of passing a default FAPL),
             * simply use MPI_COMM_SELF as the communicator.
             */
            fa_out->comm = MPI_COMM_SELF;
            fa_out->info = MPI_INFO_NULL;
        }
    }

done:
    if(local_fapl_info)
        H5VLfree_connector_info(H5_DAOS_g, local_fapl_info);

    D_FUNC_LEAVE
} /* end H5_daos_cont_get_fapl_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_set_mpi_info
 *
 * Purpose:     Set MPI info for file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_set_mpi_info(H5_daos_file_t *file, H5_daos_fapl_t *fa)
{
    int mpi_initialized;
    herr_t ret_value = SUCCEED;

    assert(file);
    assert(fa);

    if(MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized")
    if(mpi_initialized) {
        /* Duplicate communicator and Info object. */
        /*
         * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
         */
        if(FAIL == H5_daos_comm_info_dup(fa->comm, fa->info, &file->comm, &file->info))
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "failed to duplicate MPI communicator and info")

        /* Obtain the process rank and size from the communicator attached to the
         * fapl ID */
        MPI_Comm_rank(file->comm, &file->my_rank);
        MPI_Comm_size(file->comm, &file->num_procs);
    } else {
        file->my_rank = 0;
        file->num_procs = 1;
    }

done:
    D_FUNC_LEAVE
} /* end H5_daos_cont_set_mpi_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_tx_open_prep_cb
 *
 * Purpose:     Prepare callback for transaction opens.  Currently only
 *              checks for errors from previous tasks and sets the
 *              arguments.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              March, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_tx_open_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_generic_cb_ud_t *udata = (H5_daos_generic_cb_ud_t *)args;
    daos_tx_open_t *tx_open_args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for transaction open task")

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors */
    if(udata->req->status < H5_DAOS_INCOMPLETE)
        tse_task_complete(task, H5_DAOS_PRE_ERROR);

    /* Set arguments for transaction open */
    if(NULL == (tx_open_args = daos_task_get_args(task))) {
        tse_task_complete(task, H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get arguments for transaction open task")
    } /* end if */
    tx_open_args->coh = udata->req->file->coh;
    tx_open_args->th = &udata->req->th;

done:
    D_FUNC_LEAVE
} /* end H5_daos_tx_open_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_tx_open_comp_cb
 *
 * Purpose:     Complete callback for transaction opens.  Currently checks
 *              for a failed task, marks the transaction as open, then
 *              frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              February, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_tx_open_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_generic_cb_ud_t *udata = (H5_daos_generic_cb_ud_t *)args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for generic task")

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in transaction open task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */

    /* Transaction is open */
    udata->req->th_open = TRUE;

    /* Free private data */
    H5_daos_req_free_int(udata->req);
    DV_free(udata);

done:
    D_FUNC_LEAVE
} /* end H5_daos_tx_open_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_open
 *
 * Purpose:     Open a DAOS container and starts a transaction.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_open(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *open_task;
    tse_task_t *tx_open_task;
#ifdef H5_DAOS_CONT_OPEN_PRIV_DATA_WORKS
    H5_daos_generic_cb_ud_t *open_udata = NULL;
#endif
    H5_daos_generic_cb_ud_t *tx_open_udata = NULL;
    daos_cont_open_t *open_args;
    herr_t ret_value = SUCCEED;
    int ret;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container open */
    if(0 != (ret = daos_task_create(DAOS_OPC_CONT_OPEN, &file->sched, 0, NULL, &open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open container: %s", H5_daos_err_to_string(ret))

    /* Register dependency for task */
    if(*dep_task && 0 != (ret = tse_task_register_deps(open_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create dependencies for task to open container: %s", H5_daos_err_to_string(ret))

#ifdef H5_DAOS_CONT_OPEN_PRIV_DATA_WORKS
    /* Set callback functions for container open */
    if(0 != (ret = tse_task_register_cbs(open_task, H5_daos_generic_prep_cb, NULL, 0, H5_daos_generic_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to open container: %s", H5_daos_err_to_string(ret))

    /* Set private data for container open */
    if(NULL == (open_udata = (H5_daos_generic_cb_ud_t *)DV_malloc(sizeof(H5_daos_generic_cb_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for container open task")
    open_udata->req = req;
    open_udata->task_name = "container open";
    (void)tse_task_set_priv(open_task, open_udata);
#endif

    /* Set arguments for container open */
    if(NULL == (open_args = daos_task_get_args(open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't get arguments for container open task")
    open_args->poh = H5_daos_poh_g;
    uuid_copy(open_args->uuid, file->uuid);
    open_args->flags = flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO;
    open_args->coh = &file->coh;
    open_args->info = NULL;

    /* Schedule container open task (or save it to be scheduled later) and give
     * it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(open_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to open container: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else
        *first_task = open_task;
#ifdef H5_DAOS_CONT_OPEN_PRIV_DATA_WORKS
    req->rc++;
    open_udata = NULL;
#endif
    *dep_task = open_task;

    /* Create task for transaction open */
    if(0 != (ret = daos_task_create(DAOS_OPC_TX_OPEN, &file->sched, 0, NULL, &tx_open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open transaction: %s", H5_daos_err_to_string(ret))

    assert(*dep_task);
    /* Register dependency for task */
    if(0 != (ret = tse_task_register_deps(tx_open_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create dependencies for task to open transaction: %s", H5_daos_err_to_string(ret))

    /* Set private data for transaction open */
    if(NULL == (tx_open_udata = (H5_daos_generic_cb_ud_t *)DV_malloc(sizeof(H5_daos_generic_cb_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for transaction open task")
    tx_open_udata->req = req;
    tx_open_udata->task_name = "transaction open";
    (void)tse_task_set_priv(tx_open_task, tx_open_udata);

    /* Set callback functions for transaction open */
    if(0 != (ret = tse_task_register_cbs(tx_open_task, H5_daos_tx_open_prep_cb, NULL, 0, H5_daos_tx_open_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to open container: %s", H5_daos_err_to_string(ret))

    /* Schedule transaction open task and give it a reference to req */
    assert(*first_task);
    if(0 != (ret = tse_task_schedule(tx_open_task, false)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to open transaction: %s", H5_daos_err_to_string(ret))
    req->rc++;
    tx_open_udata = NULL;
    *dep_task = tx_open_task;

    /* If a snapshot was requested, use it as the epoch, otherwise query it
     */
#ifdef DV_HAVE_SNAP_OPEN_ID
    if(snap_id != H5_DAOS_SNAP_ID_INVAL) {
        epoch = (daos_epoch_t)snap_id;

        assert(!(flags & H5F_ACC_RDWR));
    } /* end if */
    else {
#endif
#ifdef DV_HAVE_SNAP_OPEN_ID
    } /* end else */
#endif

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
#ifdef H5_DAOS_CONT_OPEN_PRIV_DATA_WORKS
        open_udata = DV_free(open_udata);
#endif
        tx_open_udata = DV_free(tx_open_udata);
    } /* end if */
tx_open_udata = DV_free(tx_open_udata);
    /* Make sure we cleaned up */
#ifdef H5_DAOS_CONT_OPEN_PRIV_DATA_WORKS
    assert(!open_udata);
#endif
    assert(!tx_open_udata);

    D_FUNC_LEAVE
} /* end H5_daos_cont_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_destroy_comp_cb
 *
 * Purpose:     Complete callback for container destroy.  Identical to
 *              H5_daos_generic_comp_cb except allows -DER_NONEXIST.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              March, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_destroy_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_generic_cb_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for generic task")

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in task.  Only record error in udata->req_status if it does
     * not already contain an error (it could contain an error if another task
     * this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && task->dt_result != -DER_NONEXIST
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */

    /* Free private data */
    H5_daos_req_free_int(udata->req);
    DV_free(udata);

done:
    D_FUNC_LEAVE
} /* end H5_daos_cont_destroy_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_create
 *
 * Purpose:     Create a DAOS container.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_create(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *create_task;
    H5_daos_generic_cb_ud_t *destroy_udata = NULL;
    H5_daos_generic_cb_ud_t *create_udata = NULL;
    daos_cont_create_t *create_args;
    herr_t ret_value = SUCCEED;
    int ret;

    assert(file);
    assert(req);
    assert(first_task);
    assert(!*first_task);
    assert(dep_task);

    /* If the H5F_ACC_EXCL flag was specified, ensure that the container does
     * not exist. */
    /* DSINC we cannot make this asynchronous until we can set private data on
     * container open, since we need to be able to react to an error code. */
    if(flags & H5F_ACC_EXCL)
        if(0 == daos_cont_open(H5_daos_poh_g, file->uuid, DAOS_COO_RW, &file->coh, NULL /*&file->co_info*/, NULL /*event*/))
            D_GOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, FAIL, "container already existed and H5F_ACC_EXCL flag was used!")

    /* Delete the container if H5F_ACC_TRUNC is set.  This shouldn't cause a
     * problem even if the container doesn't exist. */
    if(flags & H5F_ACC_TRUNC) {
        tse_task_t *destroy_task;
        daos_cont_destroy_t *destroy_args;

        /* Create task for container destroy */
        if(0 != (ret = daos_task_create(DAOS_OPC_CONT_DESTROY, &file->sched, 0, NULL, &destroy_task)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy container: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        if(*dep_task && 0 != (ret = tse_task_register_deps(destroy_task, 1, dep_task)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create dependencies for task to destroy container: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for container destroy */
        if(0 != (ret = tse_task_register_cbs(destroy_task, H5_daos_generic_prep_cb, NULL, 0, H5_daos_cont_destroy_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to destroy container: %s", H5_daos_err_to_string(ret))

        /* Set private data for container destroy */
        if(NULL == (destroy_udata = (H5_daos_generic_cb_ud_t *)DV_malloc(sizeof(H5_daos_generic_cb_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for container destroy task")
        destroy_udata->req = req;
        destroy_udata->task_name = "container destroy within H5Fcreate";
        (void)tse_task_set_priv(destroy_task, destroy_udata);

        /* Set arguments for container destroy */
        if(NULL == (destroy_args = daos_task_get_args(destroy_task)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't get arguments for container destroy task")
        destroy_args->poh = H5_daos_poh_g;
        uuid_copy(destroy_args->uuid, file->uuid);
        destroy_args->force = 1;

        /* Schedule container destroy task (or save it to be scheduled later)
         * and give it a reference to req */
        if(*first_task) {
            if(0 != (ret = tse_task_schedule(destroy_task, false)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to destroy container: %s", H5_daos_err_to_string(ret))
        } /* end if */
        else
            *first_task = destroy_task;
        req->rc++;
        destroy_udata = NULL;
        *dep_task = destroy_task;
    } /* end if */

    /* Create task for container create */
    if(0 != (ret = daos_task_create(DAOS_OPC_CONT_CREATE, &file->sched, 0, NULL, &create_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create container: %s", H5_daos_err_to_string(ret))

    /* Register dependency for task */
    if(*dep_task && 0 != (ret = tse_task_register_deps(create_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create dependencies for task to create container: %s", H5_daos_err_to_string(ret))

    /* Set callback functions for container create */
    if(0 != (ret = tse_task_register_cbs(create_task, H5_daos_generic_prep_cb, NULL, 0, H5_daos_generic_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to create container: %s", H5_daos_err_to_string(ret))

    /* Set private data for container create */
    if(NULL == (create_udata = (H5_daos_generic_cb_ud_t *)DV_malloc(sizeof(H5_daos_generic_cb_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for container create task")
    create_udata->req = req;
    create_udata->task_name = "container create within H5Fcreate";
    (void)tse_task_set_priv(create_task, create_udata);

    /* Set arguments for container create */
    if(NULL == (create_args = daos_task_get_args(create_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't get arguments for container create task")
    create_args->poh = H5_daos_poh_g;
    uuid_copy(create_args->uuid, file->uuid);
    create_args->prop = NULL;

    /* Schedule container create task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(create_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to create container: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else
        *first_task = create_task;
    req->rc++;
    create_udata = NULL;
    *dep_task = create_task;

    /* Open the container and start the transaction */
    if(H5_daos_cont_open(file, flags, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't open DAOS container")

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        destroy_udata = DV_free(destroy_udata);
        create_udata = DV_free(create_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!destroy_udata);
    assert(!create_udata);

    D_FUNC_LEAVE
} /* end H5_daos_cont_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_gh_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for global
 *              container handles
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_gh_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for global handle broadcast task")

    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast group info";
    } /* end if */
    else {
        if(udata->req->file->my_rank == 0) {
            /* Reissue bcast if necesary */
            if(udata->buffer_len != udata->count) {
                tse_task_t *bcast_task;

                assert(udata->count == H5_DAOS_GH_BUF_SIZE);
                assert(udata->buffer_len > H5_DAOS_GH_BUF_SIZE);

                /* Use full buffer this time */
                udata->count = udata->buffer_len;

                /* Create task for second bcast */
                if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, &udata->req->file->sched, udata, &bcast_task)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't create task for second global handle broadcast")

                /* Set callback functions for second bcast */
                if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_gh_bcast_comp_cb, NULL, 0)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't register callbacks for second global handle broadcast: %s", H5_daos_err_to_string(ret))

                /* Schedule second bcast and transfer ownership of udata */
                if(0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule task for second global handle broadcast: %s", H5_daos_err_to_string(ret))
                udata = NULL;
            } /* end if */
        } /* end if */
        else {
            uint64_t gh_len;
            uint8_t *p;

            /* Decode global handle length */
            p = udata->buffer;
            UINT64DECODE(p, gh_len)

            /* Check for iov_buf_len set to 0 - indicates failure */
            if(gh_len == 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, H5_DAOS_REMOTE_ERROR, "lead process failed to obtain global handle")

            /* Check if we need another bcast */
            if(gh_len + H5_DAOS_ENCODED_UINT64_T_SIZE > (size_t)udata->count) {
                tse_task_t *bcast_task;

                assert(udata->buffer_len == H5_DAOS_GH_BUF_SIZE);
                assert(udata->count == H5_DAOS_GH_BUF_SIZE);

                /* Realloc buffer */
                DV_free(udata->buffer);
                udata->buffer_len = (int)gh_len + H5_DAOS_ENCODED_UINT64_T_SIZE;

                if(NULL == (udata->buffer = DV_malloc(udata->buffer_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "failed to allocate memory for global handle buffer")
                udata->count = udata->buffer_len;

                /* Create task for second bcast */
                if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, &udata->obj->item.file->sched, udata, &bcast_task)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't create task for second global handle broadcast")

                /* Set callback functions for second bcast */
                if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_gh_bcast_comp_cb, NULL, 0)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't register callbacks for second global handle broadcast: %s", H5_daos_err_to_string(ret))

                /* Schedule second bcast and transfer ownership of udata */
                if(0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule task for second global handle broadcast: %s", H5_daos_err_to_string(ret))
                udata = NULL;
            } /* end if */
            else {
                daos_iov_t glob;

                /* Set up glob */
                glob.iov_buf = p;
                glob.iov_len = (size_t)gh_len;
                glob.iov_buf_len = (size_t)gh_len;

                /* Get container handle */
                if(0 != (ret = daos_cont_global2local(H5_daos_poh_g, glob, &udata->req->file->coh)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get global container handle: %s", H5_daos_err_to_string(ret))
            } /* end else */
        } /* end else */
    } /* end else */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block */
        if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast global container handle completion callback";
        } /* end if */

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_metatask, ret_value);

        /* Release our reference to req */
        H5_daos_req_free_int(udata->req);

        /* Free buffer */
        DV_free(udata->buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == H5_DAOS_DAOS_GET_ERROR);

    return ret_value;
} /* end H5_daos_gh_bcast_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_gch_task
 *
 * Purpose:     Asynchronous task for creating a global container handle
 *              to use for broadcast.  Note this does not hold a reference
 *              to udata so it must be held by a task that depends on this
 *              task.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_get_gch_task(tse_task_t *task)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    daos_iov_t glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    uint8_t *p;
    int ret;
    herr_t ret_value = 0; /* Return value */

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data forget global container handle task")

    assert(udata->req);
    assert(udata->req->file);

    /* Calculate size of global cont handle */
    if(udata->req->status >= H5_DAOS_INCOMPLETE)
        if(0 != (ret = daos_cont_local2global(udata->req->file->coh, &glob)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't calculate size of global container handle: %s", H5_daos_err_to_string(ret))

    /* Allocate buffer */
    udata->buffer_len = MAX(glob.iov_buf_len + H5_DAOS_ENCODED_UINT64_T_SIZE, H5_DAOS_GH_BUF_SIZE);
    if(NULL == (udata->buffer = (char *)DV_calloc(udata->buffer_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "can't allocate space for global container handle")
    udata->count = H5_DAOS_GH_BUF_SIZE;
    glob.iov_len = glob.iov_buf_len;

    /* Check for previous errors (wait until after allocation because that must
     * always be done) */
    if(udata->req->status < H5_DAOS_INCOMPLETE)
        D_GOTO_DONE(H5_DAOS_PRE_ERROR)

    /* Encode global handle length */
    p = udata->buffer;
    UINT64ENCODE(p, (uint64_t)glob.iov_buf_len)

    /* Get global container handle */
    glob.iov_buf = p;
    if(0 != (ret = daos_cont_local2global(udata->req->file->coh, &glob)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get global container handle: %s", H5_daos_err_to_string(ret))

done:
    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block */
    if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = ret_value;
        udata->req->failed_task = "get global container handle";
    } /* end if */

    /* Release our reference to req */
    H5_daos_req_free_int(udata->req);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE
} /* end H5_daos_get_gch_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_handle_bcast
 *
 * Purpose:     Broadcast the container handle.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_handle_bcast(H5_daos_file_t *file, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    int ret;
    herr_t ret_value = SUCCEED; /* Return value */

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if(NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for MPI broadcast user data")
    bcast_udata->req = req;
    bcast_udata->obj = NULL;
    bcast_udata->bcast_metatask = NULL;
    bcast_udata->buffer = NULL;
    bcast_udata->buffer_len = 0;
    bcast_udata->count = 0;

    /* check if this is the lead rank */
    if(file->my_rank == 0) {
        tse_task_t *get_gch_task;

        /* Create task to get global container handle */
        if(0 != (ret = tse_task_create(H5_daos_get_gch_task, &file->sched, bcast_udata, &get_gch_task)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to get global container handle: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        if(*dep_task && 0 != (ret = tse_task_register_deps(get_gch_task, 1, dep_task)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create dependencies for task to get global container handle: %s", H5_daos_err_to_string(ret))

        /* Set private data for task to get global container handle */
        (void)tse_task_set_priv(get_gch_task, bcast_udata);

        /* Schedule task to get global container handle (or save it to be
         * scheduled later) and give it a reference to req.  Do not transfer
         * ownership of bcast_udata, as that will be owned by bcast_task (which
         * will depend on get_gch_task, so it will not free it before
         * get_gch_task runs). */
        if(*first_task) {
            if(0 != (ret = tse_task_schedule(get_gch_task, false)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to get global container handle: %s", H5_daos_err_to_string(ret))
        } /* end if */
        else
            *first_task = get_gch_task;
        req->rc++;
        *dep_task = get_gch_task;
    } /* end if */
    else {
        /* Allocate global handle buffer with default initial size */
        if(NULL == (bcast_udata->buffer = DV_malloc(H5_DAOS_GH_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for global container handle")
        bcast_udata->buffer_len = H5_DAOS_GH_BUF_SIZE;
        bcast_udata->count = H5_DAOS_GH_BUF_SIZE;
    } /* end else */

done:
    /* Do broadcast */
    if(bcast_udata) {
        tse_task_t *bcast_task;

        /* Create meta task for global handle bcast.  This empty task will be
         * completed when the bcast is finished by H5_daos_gh_bcast_comp_cb.  We
         * can't use bcast_task since it may not be completed after the first
         * bcast. */
        if(0 != (ret = tse_task_create(NULL, &file->sched, NULL, &bcast_udata->bcast_metatask)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create meta task for global handle broadcast: %s", H5_daos_err_to_string(ret))
        /* Create task for global handle bcast */
        else if(0 != (ret = tse_task_create(H5_daos_mpi_ibcast_task, &file->sched, bcast_udata, &bcast_task)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to broadcast global handle: %s", H5_daos_err_to_string(ret))
        /* Register dependency on dep_task if present */
        else if(*dep_task && 0 != (ret = tse_task_register_deps(bcast_task, 1, dep_task)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create dependencies for global handle broadcast task: %s", H5_daos_err_to_string(ret))
        /* Set callback functions for group info bcast */
        else if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_gh_bcast_comp_cb, NULL, 0)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't register callbacks for global handle broadcast: %s", H5_daos_err_to_string(ret))
        /* Schedule meta task */
        else if(0 != (ret = tse_task_schedule(bcast_udata->bcast_metatask, false)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule meta task for global handle broadcast: %s", H5_daos_err_to_string(ret))
        else {
            /* In case of failure, clear buffer */
            if(ret_value < 0)
                memset(bcast_udata->buffer, 0, bcast_udata->buffer_len);

            /* Schedule second bcast and transfer ownership of bcast_udata */
            if(*first_task) {
                if(0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task for global handle broadcast: %s", H5_daos_err_to_string(ret))
                else {
                    req->rc++;
                    *dep_task = bcast_udata->bcast_metatask;
                    bcast_udata = NULL;
                } /* end else */
            } /* end if */
            else {
                *first_task = bcast_task;
                req->rc++;
                *dep_task = bcast_udata->bcast_metatask;
                bcast_udata = NULL;
            } /* end else */
        } /* end else */
    } /* end if */

    /* Cleanup */
    if(bcast_udata) {
        assert(ret_value < 0);
        DV_free(bcast_udata->buffer);
        bcast_udata = DV_free(bcast_udata);
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_cont_handle_bcast() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_create
 *
 * Purpose:     Creates a file as a daos HDF5 file.
 *
 * Return:      Success:        the file ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              September, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_file_create(const char *name, unsigned flags, hid_t fcpl_id,
    hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_file_t *file = NULL;
    H5_daos_fapl_t fapl_info;
    daos_obj_id_t gmd_oid = {0, 0};
    hbool_t sched_init = FALSE;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
#if 0 /* Needed for storing the root group OID in the global metadata object -
       * see note below */
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    uint8_t root_group_oid_buf[H5_DAOS_ENCODED_OID_SIZE];
    uint8_t *p;
#endif
    int ret;
    void *ret_value = NULL;

    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE
            == H5_DAOS_ENCODED_UINT64_T_SIZE + H5_DAOS_ENCODED_UINT64_T_SIZE);

    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL")

    /*
     * Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if(0==(flags & (H5F_ACC_EXCL|H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL;      /*default*/
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* Get information from the FAPL */
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct")

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct")
    file->item.open_req = NULL;
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;
    file->item.rc = 1;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->flags = flags;
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Create CART context */
    if(0 != (ret = crt_context_create(&file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create CART context: %s", H5_daos_err_to_string(ret))

    /* Create DAOS task scheduler */
    if(0 != (ret = tse_sched_init(&file->sched, NULL, file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task scheduler: %s", H5_daos_err_to_string(ret))
    sched_init = TRUE;

    /* Set MPI container info */
    if(H5_daos_cont_set_mpi_info(file, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info")

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Fill FAPL cache */
    if(H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache")

    /* Generate oid for global metadata object */
    if(H5_daos_oid_encode(&gmd_oid, H5_DAOS_OIDX_GMD, H5I_GROUP,
            fcpl_id == H5P_FILE_CREATE_DEFAULT ? H5P_DEFAULT : fcpl_id,
            H5_DAOS_OBJ_CLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID")

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(file)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Create container on rank 0 */
    if((file->my_rank == 0) && H5_daos_cont_create(file, flags, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create DAOS container")

    /* Broadcast container handle to other procs if any */
    if((file->num_procs > 1) && (H5_daos_cont_handle_bcast(file, int_req, &first_task, &dep_task) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, NULL, "can't broadcast DAOS container handle")

    /* Open global metadata object */
    if(H5_daos_obj_open(file, int_req, &gmd_oid, DAOS_OO_RW, &file->glob_md_oh, "global metadata object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open global metadata object")

    /* Create root group */
    if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_create_helper(file, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, int_req, NULL, NULL, 0, H5_DAOS_OIDX_ROOT, TRUE, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group")

    /* Write root group OID to global metadata object */
    /* Only do this if a non-default object class is used DSINC */
    /* Disabled for now since we don't use it.  Right now, the user must specify
     * the root group's object class through
     * H5daos_set_root_open_object_class(), but eventually we will allow the
     * user to skip this by storing the root group OID in the global metadata
     * object (as below), and, if the intial root group open fails, reading the
     * stored root group OID and using that to open the root group.  DSINC */
#if 0
    /* Encode root group OID */
    p = root_group_oid_buf;
    UINT64ENCODE(p, file->root_grp->obj.oid.lo)
    UINT64ENCODE(p, file->root_grp->obj.oid.hi)

    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_root_grp_oid_key_g, H5_daos_root_grp_oid_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (uint64_t)H5_DAOS_ENCODED_OID_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, root_group_oid_buf, (daos_size_t)H5_DAOS_ENCODED_OID_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Write root group OID */
    if(0 != (ret = daos_obj_update(file->glob_md_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, NULL, "can't write root group OID to global metadata object: %s", H5_daos_err_to_string(ret))
#endif

    ret_value = (void *)file;

done:
    if(int_req) {
        tse_task_t *finalize_task;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(sched_init && H5_daos_progress(file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, NULL, "file creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))
    
        /* Close internal request */
        H5_daos_req_free_int(int_req);
    } /* end if */

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5_daos_file_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_open
 *
 * Purpose:     Opens a file as a daos HDF5 file.
 *
 * Return:      Success:        the file ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_file_open(const char *name, unsigned flags, hid_t fapl_id,
    hid_t dxpl_id, void **req)
{
    H5_daos_file_t *file = NULL;
    H5_daos_fapl_t fapl_info;
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    daos_obj_id_t gmd_oid = {0, 0};
    daos_obj_id_t root_grp_oid = {0, 0};
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    void *ret_value = NULL;

    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL")

    /* Get information from the FAPL */
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct")

#ifdef DV_HAVE_SNAP_OPEN_ID
    if(H5Pget(fapl_id, H5_DAOS_SNAP_OPEN_ID, &snap_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for snap ID")

    /* Check for opening a snapshot with write access (disallowed) */
    if((snap_id != H5_DAOS_SNAP_ID_INVAL) && (flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "write access requested to snapshot - disallowed")
#endif

    /* Allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct")
    file->item.open_req = NULL;
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;
    file->item.rc = 1;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->flags = flags;
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Create CART context */
    if(0 != (ret = crt_context_create(&file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create CART context: %s", H5_daos_err_to_string(ret))

    /* Create DAOS task scheduler */
    if(0 != (ret = tse_sched_init(&file->sched, NULL, file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task scheduler: %s", H5_daos_err_to_string(ret))

    /* Set MPI container info */
    if(H5_daos_cont_set_mpi_info(file, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info")

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Fill FAPL cache */
    if(H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache")

    /* Generate oid for global metadata object */
    if(H5_daos_oid_encode(&gmd_oid, H5_DAOS_OIDX_GMD, H5I_GROUP,
            fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
            H5_DAOS_ROOT_OPEN_OCLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID")

    /* Generate root group oid */
    if(H5_daos_oid_encode(&root_grp_oid, H5_DAOS_OIDX_ROOT, H5I_GROUP,
            fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
            H5_DAOS_ROOT_OPEN_OCLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode root group object ID")

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(file)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Open container on rank 0 */
    if((file->my_rank == 0) && H5_daos_cont_open(file, flags, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't open DAOS container")

    /* Broadcast container handle to other procs if any */
    if((file->num_procs > 1) && (H5_daos_cont_handle_bcast(file, int_req, &first_task, &dep_task) < 0))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't broadcast DAOS container handle")

    /* Open global metadata object */
    if(H5_daos_obj_open(file, int_req, &gmd_oid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->glob_md_oh, "global metadata object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open global metadata object")

    /* Open root group */
    if(NULL == (file->root_grp = H5_daos_group_open_helper_async(file, root_grp_oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, int_req, TRUE, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open root group")

    ret_value = (void *)file;

done:
    if(int_req) {
        tse_task_t *finalize_task;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, NULL, "file open failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        H5_daos_req_free_int(int_req);
    } /* end if */

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5_daos_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_get
 *
 * Purpose:     Performs a file "get" operation
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_file_get(void *_item, H5VL_file_get_t get_type, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req, va_list H5VL_DAOS_UNUSED arguments)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(get_type == H5VL_FILE_GET_NAME) {
        if(H5I_FILE != file->item.type && H5I_GROUP != file->item.type &&
           H5I_DATATYPE != file->item.type && H5I_DATASET != file->item.type &&
           H5I_ATTR != file->item.type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file, group, datatype, dataset or attribute")
    }
    else
        if(H5I_FILE != file->item.type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file")

    switch (get_type) {
        /* "get container info" */
        case H5VL_FILE_GET_CONT_INFO:
        {
            H5VL_file_cont_info_t *info = va_arg(arguments, H5VL_file_cont_info_t *);

            /* Verify structure version */
            if(info->version != H5VL_CONTAINER_INFO_VERSION)
                D_GOTO_ERROR(H5E_FILE, H5E_VERSION, FAIL, "wrong container info version number")

            /* Set the container info fields */
            info->feature_flags = 0;            /* None currently defined */
            info->token_size = H5_DAOS_ENCODED_OID_SIZE;
            info->blob_id_size = H5_DAOS_BLOB_ID_SIZE;

            break;
        } /* H5VL_FILE_GET_CONT_INFO */

        /* H5Fget_access_plist */
        case H5VL_FILE_GET_FAPL:
        {
            hid_t *ret_id = va_arg(arguments, hid_t *);

            if((*ret_id = H5Pcopy(file->fapl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FAPL")

            break;
        } /* H5VL_FILE_GET_FAPL */

        /* H5Fget_create_plist */
        case H5VL_FILE_GET_FCPL:
        {
            hid_t *ret_id = va_arg(arguments, hid_t *);

            /* The file's FCPL is stored as the group's GCPL */
            if((*ret_id = H5Pcopy(file->root_grp->gcpl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FCPL")

            /* Set root group's object class on fcpl */
            if(H5_daos_set_oclass_from_oid(*ret_id, file->root_grp->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property")

            break;
        } /* H5VL_FILE_GET_FCPL */

        /* H5Fget_intent */
        case H5VL_FILE_GET_INTENT:
        {
            unsigned *ret_intent = va_arg(arguments, unsigned *);

            if((file->flags & H5F_ACC_RDWR) == H5F_ACC_RDWR)
                *ret_intent = H5F_ACC_RDWR;
            else
                *ret_intent = H5F_ACC_RDONLY;

            break;
        } /* H5VL_FILE_GET_INTENT */

        /* H5Fget_name */
        case H5VL_FILE_GET_NAME:
        {
            H5I_type_t  obj_type = va_arg(arguments, H5I_type_t);
            size_t      name_buf_size = va_arg(arguments, size_t);
            char       *name_buf = va_arg(arguments, char *);
            ssize_t    *ret_size = va_arg(arguments, ssize_t *);

            if(H5I_FILE != obj_type)
                file = file->item.file;

            *ret_size = (ssize_t) strlen(file->file_name);

            if(name_buf) {
                strncpy(name_buf, file->file_name, name_buf_size - 1);
                name_buf[name_buf_size - 1] = '\0';
            } /* end if */

            break;
        } /* H5VL_FILE_GET_NAME */

        /* H5Fget_obj_count */
        case H5VL_FILE_GET_OBJ_COUNT:
        {
            unsigned obj_types = va_arg(arguments, unsigned);
            ssize_t *ret_val = va_arg(arguments, ssize_t *);
            get_obj_count_udata_t udata;

            udata.obj_count = 0;

            uuid_copy(udata.file_id, file->uuid);

            if(obj_types & H5F_OBJ_FILE)
                if(H5Iiterate(H5I_FILE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs")
            if(obj_types & H5F_OBJ_DATASET)
                if(H5Iiterate(H5I_DATASET, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open dataset IDs")
            if(obj_types & H5F_OBJ_GROUP)
                if(H5Iiterate(H5I_GROUP, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs")
            if(obj_types & H5F_OBJ_DATATYPE)
                if(H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open datatype IDs")
            if(obj_types & H5F_OBJ_ATTR)
                if(H5Iiterate(H5I_ATTR, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open attribute IDs")

            *ret_val = udata.obj_count;

            break;
        } /* H5VL_FILE_GET_OBJ_COUNT */

        /* H5Fget_obj_ids */
        case H5VL_FILE_GET_OBJ_IDS:
        {
            unsigned obj_types = va_arg(arguments, unsigned);
            size_t max_ids = va_arg(arguments, size_t);
            hid_t *oid_list = va_arg(arguments, hid_t *);
            ssize_t *ret_val = va_arg(arguments, ssize_t *);
            get_obj_ids_udata_t udata;

            udata.max_objs = max_ids;
            udata.obj_count = 0;
            udata.oid_list = oid_list;

            if(max_ids > 0) {
                uuid_copy(udata.file_id, file->uuid);

                if(obj_types & H5F_OBJ_FILE)
                    if(H5Iiterate(H5I_FILE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs")
                if(obj_types & H5F_OBJ_DATASET)
                    if(H5Iiterate(H5I_DATASET, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open dataset IDs")
                if(obj_types & H5F_OBJ_GROUP)
                    if(H5Iiterate(H5I_GROUP, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs")
                if(obj_types & H5F_OBJ_DATATYPE)
                    if(H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open datatype IDs")
                if(obj_types & H5F_OBJ_ATTR)
                    if(H5Iiterate(H5I_ATTR, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open attribute IDs")
            }

            *ret_val = udata.obj_count;

            break;
        } /* H5VL_FILE_GET_OBJ_IDS */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file get operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_file_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_specific
 *
 * Purpose:     Perform an operation
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_file_specific(void *item, H5VL_file_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5_daos_file_t *file = NULL;
    herr_t          ret_value = SUCCEED;    /* Return value */

    if(item)
        file = ((H5_daos_item_t *)item)->file;

    switch (specific_type) {
        /* H5Fflush */
        case H5VL_FILE_FLUSH:
        {
            if(H5_daos_file_flush(file) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

            break;
        } /* H5VL_FILE_FLUSH */

        /* H5Freopen */
        case H5VL_FILE_REOPEN:
        {
            void **ret_file = va_arg(arguments, void **);

            if(NULL == (*ret_file = H5_daos_file_open(file->file_name, file->flags, file->fapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "can't reopen file")

            break;
        } /* H5VL_FILE_REOPEN */

        /* H5Fmount */
        case H5VL_FILE_MOUNT:
        /* H5Fmount */
        case H5VL_FILE_UNMOUNT:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation")

        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE:
        {
            hid_t file_fapl = va_arg(arguments, hid_t);
            const char *filename = va_arg(arguments, const char *);
            htri_t *ret_is_accessible = va_arg(arguments, htri_t *);
            void *opened_file = NULL;

            if(NULL == filename)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "filename is NULL")

            H5E_BEGIN_TRY {
                opened_file = H5_daos_file_open(filename, H5F_ACC_RDONLY, file_fapl, dxpl_id, req);
            } H5E_END_TRY;

            *ret_is_accessible = opened_file ? TRUE : FALSE;

            if(opened_file)
                if(H5_daos_file_close(opened_file, dxpl_id, req) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "error occurred while closing file")

            break;
        } /* H5VL_FILE_IS_ACCESSIBLE */

        /* H5Fdelete */
        case H5VL_FILE_DELETE:
        {
            H5_daos_fapl_t fapl_info;
            hid_t fapl_id = va_arg(arguments, hid_t);
            const char *filename = va_arg(arguments, const char *);
            herr_t *delete_ret = va_arg(arguments, herr_t *);
            uuid_t cont_uuid;
            int mpi_rank, mpi_initialized;
            int ret;

            /* Initialize returned value in case we fail */
            *delete_ret = FAIL;

            /* Get information from the FAPL */
            if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get DAOS info struct")

            if(MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized")
            if(mpi_initialized) {
                MPI_Comm_rank(fapl_info.comm, &mpi_rank);
            } else {
                mpi_rank = 0;
            }

            if(mpi_rank == 0) {
                /* Hash file name to create uuid */
                H5_daos_hash128(filename, &cont_uuid);

                if(0 != (ret = daos_cont_destroy(H5_daos_poh_g, cont_uuid, 1, NULL /*event*/)))
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "can't destroy container: %s", H5_daos_err_to_string(ret))
            } /* end if */

            if(mpi_initialized)
                if(MPI_SUCCESS != MPI_Barrier(fapl_info.comm))
                    D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed during file deletion")

            *delete_ret = SUCCEED;

            break;
        } /* H5VL_FILE_DELETE */

        /* Check if two files are the same */
        case H5VL_FILE_IS_EQUAL:
        {
            H5_daos_file_t *file2 = (H5_daos_file_t *)va_arg(arguments, void *);
            hbool_t *is_equal = va_arg(arguments, hbool_t *);

            if(!file || !file2)
                *is_equal = FALSE;
            else
                *is_equal = (memcmp(&file->uuid, &file2->uuid, sizeof(file->uuid)) == 0);
            break;
        } /* H5VL_FILE_IS_EQUAL */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_decref
 *
 * Purpose:     Decrements the reference count on an HDF5/DAOS file,
 *              freeing it if the ref count reaches 0.
 *
 * Return:      Success:        the file id. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_file_decref(H5_daos_file_t *file)
{
    assert(file);

    if(--file->item.rc == 0) {
        /* Free file data structure */
        assert(file->closed);
        H5FL_FREE(H5_daos_file_t, file);
    } /* end if */

    return;
} /* end H5_daos_file_decref() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_close_helper
 *
 * Purpose:     Closes a daos HDF5 file.
 *
 * Return:      Success:        the file id. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_close_helper(H5_daos_file_t *file, hid_t dxpl_id, void **req)
{
    int ret;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Free file data structures */
    if(file->item.open_req)
        H5_daos_req_free_int(file->item.open_req);
    file->item.open_req = NULL;
    if(file->file_name)
        file->file_name = DV_free(file->file_name);
    if(file->comm || file->info)
        if(H5_daos_comm_info_free(&file->comm, &file->info) < 0)
            D_DONE_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to free copy of MPI communicator and info")
    if(file->fapl_id != FAIL && H5Idec_ref(file->fapl_id) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close fapl")
    if(!daos_handle_is_inval(file->glob_md_oh))
        if(0 != (ret = daos_obj_close(file->glob_md_oh, NULL /*event*/)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close global metadata object: %s", H5_daos_err_to_string(ret))
    if(file->root_grp)
        if(H5_daos_group_close(file->root_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close root group")
    if(!daos_handle_is_inval(file->coh))
        if(0 != (ret = daos_cont_close(file->coh, NULL /*event*/)))
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't close container: %s", H5_daos_err_to_string(ret))
    if(file->vol_id >= 0) {
        if(H5VLfree_connector_info(file->vol_id, file->vol_info) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't free VOL connector info")
        if(H5Idec_ref(file->vol_id) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "can't decrement VOL connector ID")
    } /* end if */

    /* Finish the scheduler *//* Make this cancel tasks?  Only if flush progresses until empty.  Otherwise change to custom progress function DSINC */
    tse_sched_complete(&file->sched, 0, FALSE);
    tse_sched_fini(&file->sched);

    /* Destroy CART context */
    if(0 != (ret = crt_context_destroy(file->crt_ctx, 1)))
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't destroy CART context: %s", H5_daos_err_to_string(ret))

    /* File is closed */
    file->closed = TRUE;

    /* Decrement ref count on file struct */
    H5_daos_file_decref(file);

    D_FUNC_LEAVE
} /* end H5_daos_file_close_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_close
 *
 * Purpose:     Closes a daos HDF5 file, committing the epoch if
 *              appropriate.
 *
 * Return:      Success:        the file ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_file_close(void *_file, hid_t dxpl_id, void **req)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
#if 0 /* DSINC */
    int ret;
#endif
    herr_t ret_value = SUCCEED;

    if(!_file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL")

    /* Flush the file (barrier, commit epoch, slip epoch) *Update comment DSINC */
    if(H5_daos_file_flush(file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

#if 0 /* DSINC */
    /* Flush the epoch */
    if(0 != (ret = daos_epoch_flush(file->coh, epoch, NULL /*state*/, NULL /*event*/)))
        D_DONE_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "can't flush epoch: %s", H5_daos_err_to_string(ret))
#endif

    /*
     * Ensure that all other processes are done with the file before
     * closing the container handle. This is to prevent invalid handle
     * issues due to rank 0 closing the container handle before other
     * ranks are done using it.
     */
    if(file->num_procs > 1 && (MPI_SUCCESS != MPI_Barrier(file->comm)))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed during file close")

    /* Close the file */
    if(H5_daos_file_close_helper(file, dxpl_id, req) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close file")

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_file_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_flush
 *
 * Purpose:     Flushes a DAOS file.  Currently a no-op, may create a
 *              snapshot in the future.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_file_flush(H5_daos_file_t *file)
{
#if 0
    int ret;
#endif
    herr_t ret_value = SUCCEED;    /* Return value */

    assert(file);

    /* Nothing to do if no write intent */
    if(!(file->flags & H5F_ACC_RDWR))
        D_GOTO_DONE(SUCCEED)

    /* Progress scheduler until empty? DSINC */

#if 0
    /* Collectively determine if anyone requested a snapshot of the epoch */
    if(MPI_SUCCESS != MPI_Reduce(file->my_rank == 0 ? MPI_IN_PLACE : &file->snap_epoch, &file->snap_epoch, 1, MPI_INT, MPI_LOR, 0, file->comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "failed to determine whether to take snapshot (MPI_Reduce)")

    /* Barrier on all ranks so we don't commit before all ranks are
     * finished writing. H5Fflush must be called collectively. */
    if(MPI_SUCCESS != MPI_Barrier(file->comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed")

    /* Commit the epoch */
    if(file->my_rank == 0) {
        /* Save a snapshot of this epoch if requested */
        /* Disabled until snapshots are supported in DAOS DSINC */

        if(file->snap_epoch)
            if(0 != (ret = daos_snap_create(file->coh, file->epoch, NULL /*event*/)))
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't create snapshot: %s", H5_daos_err_to_string(ret))

        /* Commit the epoch.  This should slip previous epochs automatically. */
        if(0 != (ret = daos_epoch_commit(file->coh, file->epoch, NULL /*state*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "failed to commit epoch: %s", H5_daos_err_to_string(ret))
    } /* end if */
#endif

done:
    D_FUNC_LEAVE
} /* end H5_daos_file_flush() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_fill_fapl_cache
 *
 * Purpose:     Fills the "fapl_cache" field of the file struct, using the
 *              file's FAPL.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_fill_fapl_cache(H5_daos_file_t *file, hid_t fapl_id)
{
    char *oclass_str = NULL;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Determine if we requested collective metadata reads for the file */
    file->fapl_cache.is_collective_md_read = FALSE;
    if(H5P_FILE_ACCESS_DEFAULT != fapl_id)
        if(H5Pget_all_coll_metadata_ops(fapl_id, &file->fapl_cache.is_collective_md_read) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get collective metadata reads property")

    /* Check for file default object class set on fapl_id */
    /* Note we do not copy the oclass_str in the property callbacks (there is no
     * "get" callback, so this is more like an H5P_peek, and we do not need to
     * free oclass_str as it points directly into the plist value */
    file->fapl_cache.default_object_class = OC_UNKNOWN;
    if((prop_exists = H5Pexist(fapl_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")
    if(prop_exists) {
        if(H5Pget(fapl_id, H5_DAOS_OBJ_CLASS_NAME, &oclass_str) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class")
        if(oclass_str && (oclass_str[0] != '\0'))
            if(OC_UNKNOWN == (file->fapl_cache.default_object_class = daos_oclass_name2id(oclass_str)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unknown object class")
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_fill_fapl_cache() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_obj_count_callback
 *
 * Purpose:     A callback for H5Iiterate which increments the passed in
 *              object count only if the current object's file ID matches
 *              the file ID passed in.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_obj_count_callback(hid_t id, void *udata)
{
    get_obj_count_udata_t *count_udata = (get_obj_count_udata_t *)udata;
    H5_daos_obj_t *cur_obj = NULL;
    ssize_t connector_name_len;
    char connector_name[H5_DAOS_VOL_NAME_LEN + 1];
    herr_t ret_value = H5_ITER_CONT;

    /* Ensure that the ID represents a DAOS VOL object */
    H5E_BEGIN_TRY {
        connector_name_len = H5VLget_connector_name(id, connector_name, H5_DAOS_VOL_NAME_LEN + 1);
    } H5E_END_TRY;

    /* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
    if(connector_name_len < 0)
        D_GOTO_DONE(H5_ITER_CONT);

    if(!strncmp(H5_DAOS_VOL_NAME, connector_name, H5_DAOS_VOL_NAME_LEN)) {
        if(NULL == (cur_obj = (H5_daos_obj_t *) H5VLobject(id)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID")

        if(!uuid_compare(cur_obj->item.file->uuid, count_udata->file_id))
            count_udata->obj_count++;
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_get_obj_count_callback() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_obj_ids_callback
 *
 * Purpose:     A callback for H5Iiterate which retrieves all of the open
 *              object IDs of the specified types for the given file ID.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_obj_ids_callback(hid_t id, void *udata)
{
    get_obj_ids_udata_t *id_udata = (get_obj_ids_udata_t *)udata;
    H5_daos_obj_t *cur_obj = NULL;
    ssize_t connector_name_len;
    char connector_name[H5_DAOS_VOL_NAME_LEN + 1];
    herr_t ret_value = H5_ITER_CONT;

    /* Ensure that the ID represents a DAOS VOL object */
    H5E_BEGIN_TRY {
        connector_name_len = H5VLget_connector_name(id, connector_name, H5_DAOS_VOL_NAME_LEN + 1);
    } H5E_END_TRY;

    /* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
    if(connector_name_len < 0)
        D_GOTO_DONE(H5_ITER_CONT);

    if(!strncmp(H5_DAOS_VOL_NAME, connector_name, H5_DAOS_VOL_NAME_LEN)) {
        if(NULL == (cur_obj = (H5_daos_obj_t *) H5VLobject(id)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID")

        if(id_udata->obj_count < id_udata->max_objs) {
            if(!uuid_compare(cur_obj->item.file->uuid, id_udata->file_id))
                id_udata->oid_list[id_udata->obj_count++] = id;
        }
        else
            ret_value = H5_ITER_STOP;
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_get_obj_ids_callback() */

