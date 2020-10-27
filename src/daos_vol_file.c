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

/* Common info used for DAOS Unified Namespace routines
 * and DAOS container create/open/destroy routines */
typedef struct H5_daos_cont_op_info_t {
    H5_daos_req_t *req;
    tse_task_t *cont_op_metatask;
    daos_handle_t *poh;
    daos_handle_t tmp_poh;
    daos_pool_info_t pool_info;
    struct duns_attr_t duns_attr;
    const char *path;
    unsigned flags;
    hbool_t ignore_missing_path;
} H5_daos_cont_op_info_t;

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
#ifdef H5_DAOS_USE_TRANSACTIONS
static int H5_daos_tx_open_prep_cb(tse_task_t *task, void *args);
static int H5_daos_tx_open_comp_cb(tse_task_t *task, void *args);
#endif /* H5_DAOS_USE_TRANSACTIONS */
static herr_t H5_daos_cont_create(H5_daos_file_t *file, unsigned flags,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_get_cont_create_task(H5_daos_cont_op_info_t *cont_op_info,
    tse_sched_t *sched, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_duns_create_path(H5_daos_cont_op_info_t *create_udata, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_duns_create_path_task(tse_task_t *task);
static int H5_daos_duns_create_path_prep_cb(tse_task_t *task, void *args);
static int H5_daos_duns_create_path_comp_cb(tse_task_t *task, void *args);
static int H5_daos_cont_create_prep_cb(tse_task_t *task, void *args);
static int H5_daos_cont_create_comp_cb(tse_task_t *task, void *args);
static int H5_daos_excl_open_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_cont_open(H5_daos_file_t *file, unsigned flags,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_get_cont_open_task(H5_daos_cont_op_info_t *cont_op_info,
    tse_sched_t *sched, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_duns_resolve_path(H5_daos_cont_op_info_t *resolve_udata,
    tse_sched_t *sched, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_duns_resolve_path_task(tse_task_t *task);
static int H5_daos_duns_resolve_path_comp_cb(tse_task_t *task, void *args);
static int H5_daos_cont_open_prep_cb(tse_task_t *task, void *args);
static int H5_daos_cont_open_comp_cb(tse_task_t *task, void *args);
static int H5_daos_handles_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_file_handles_bcast(H5_daos_file_t *file,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_get_container_handles_task(tse_task_t *task);
static herr_t H5_daos_file_delete(uuid_t *puuid, const char *file_path, hbool_t ignore_missing,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_get_cont_destroy_task(H5_daos_cont_op_info_t *cont_op_info, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_duns_destroy_path(H5_daos_cont_op_info_t *destroy_udata, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_duns_destroy_path_task(tse_task_t *task);
static int H5_daos_duns_destroy_path_comp_cb(tse_task_t *task, void *args);
static int H5_daos_cont_destroy_prep_cb(tse_task_t *task, void *args);
static int H5_daos_cont_destroy_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_file_delete_sync(const char *filename, hid_t fapl_id);

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

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(FAIL);

    /*
     * First, check to see if any MPI info was set through the use of
     * a H5Pset_fapl_daos() call.
     */
    if(H5Pget_vol_info(fapl_id, (void **) &local_fapl_info) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL info struct");
    if(local_fapl_info) {
        if(H5_daos_comm_info_dup(local_fapl_info->comm, local_fapl_info->info,
                &fa_out->comm, &fa_out->info) < 0)
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "failed to duplicate MPI communicator and info");
        fa_out->free_comm_info = TRUE;
    }
    else {
        hid_t driver_id;

        /*
         * If no info was set using H5Pset_fapl_daos(), see if the application
         * set any MPI info by using HDF5's H5Pset_fapl_mpio().
         */
        if((driver_id = H5Pget_driver(fapl_id)) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't determine if a MPI-based HDF5 VFD was requested for file access");
        if(H5FD_MPIO == driver_id) {
            if(H5Pget_fapl_mpio(fapl_id, &fa_out->comm, &fa_out->info) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get HDF5 MPI information");
            fa_out->free_comm_info = TRUE;
        }
        else {
            /*
             * If no MPI info was set (as in the case of passing a default FAPL),
             * simply use MPI_COMM_SELF as the communicator.
             */
            fa_out->comm = MPI_COMM_SELF;
            fa_out->info = MPI_INFO_NULL;
            fa_out->free_comm_info = FALSE;
        }
    }

done:
    if(local_fapl_info)
        H5VLfree_connector_info(H5_DAOS_g, local_fapl_info);

    D_FUNC_LEAVE;
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
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized");
    if(mpi_initialized) {
        /* Duplicate communicator and Info object. */
        /*
         * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
         */
        if(FAIL == H5_daos_comm_info_dup(fa->comm, fa->info, &file->comm, &file->info))
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "failed to duplicate MPI communicator and info");

        /* Obtain the process rank and size from the communicator attached to the
         * fapl ID */
        MPI_Comm_rank(file->comm, &file->my_rank);
        MPI_Comm_size(file->comm, &file->num_procs);
    } else {
        file->my_rank = 0;
        file->num_procs = 1;
    }

done:
    D_FUNC_LEAVE;
} /* end H5_daos_cont_set_mpi_info() */

#ifdef H5_DAOS_USE_TRANSACTIONS

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
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for transaction open task");

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_PRE_ERROR);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_SHORT_CIRCUIT);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Set arguments for transaction open */
    if(NULL == (tx_open_args = daos_task_get_args(task))) {
        tse_task_complete(task, -H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for transaction open task");
    } /* end if */
    tx_open_args->coh = udata->req->file->coh;
    tx_open_args->th = &udata->req->th;

done:
    D_FUNC_LEAVE;
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
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for generic task");

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in transaction open task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */
    else if(task->dt_result == 0)
        /* Transaction is open */
        udata->req->th_open = TRUE;

done:
    /* Release our reference to req */
    if(H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    DV_free(udata);

    D_FUNC_LEAVE;
} /* end H5_daos_tx_open_comp_cb() */
#endif /* H5_DAOS_USE_TRANSACTIONS */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_handles_bcast_comp_cb
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
H5_daos_handles_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for global handles broadcast task");

    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);
    assert(udata->sched);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast global container handles";
    } /* end if */
    else if(task->dt_result == 0) {
        if(udata->req->file->my_rank == 0) {
            /* Reissue bcast if necesary */
            if(udata->buffer_len != udata->count) {
                tse_task_t *bcast_task;

                assert(udata->count == (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));
                assert(udata->buffer_len > (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));

                /* Use full buffer this time */
                udata->count = udata->buffer_len;

                /* Create task for second bcast */
                if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, udata->sched, udata, &bcast_task)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't create task for second global handles broadcast: %s", H5_daos_err_to_string(ret));

                /* Set callback functions for second bcast */
                if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_handles_bcast_comp_cb, NULL, 0)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't register callbacks for second global handles broadcast: %s", H5_daos_err_to_string(ret));

                /* Schedule second bcast and transfer ownership of udata */
                if(0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule task for second global handles broadcast: %s", H5_daos_err_to_string(ret));
                udata = NULL;
            } /* end if */
        } /* end if */
        else {
            uint64_t gch_len, gph_len;
            uint8_t *p;

            /* Decode container's global pool handle length */
            p = udata->buffer;
            UINT64DECODE(p, gph_len)

            /* Check for gph_len set to 0 - indicates failure */
            if(gph_len == 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR, "lead process failed to obtain container's global pool handle");

            /* Decode global container handle length */
            UINT64DECODE(p, gch_len)

            /* Check for gch_len set to 0 - indicates failure */
            if(gch_len == 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR, "lead process failed to obtain global container handle");

            /* Check if we need another bcast */
            if(gch_len + gph_len + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE) > (size_t)udata->count) {
                tse_task_t *bcast_task;

                assert(udata->buffer_len == (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));
                assert(udata->count == (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));

                /* Realloc buffer */
                DV_free(udata->buffer);
                udata->buffer_len = (int)gch_len + (int)gph_len + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE);

                if(NULL == (udata->buffer = DV_malloc((size_t)udata->buffer_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "failed to allocate memory for global handles buffer");
                udata->count = udata->buffer_len;

                /* Create task for second bcast */
                if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, udata->sched, udata, &bcast_task)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't create task for second global handles broadcast: %s", H5_daos_err_to_string(ret));

                /* Set callback functions for second bcast */
                if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_handles_bcast_comp_cb, NULL, 0)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't register callbacks for second global handles broadcast: %s", H5_daos_err_to_string(ret));

                /* Schedule second bcast and transfer ownership of udata */
                if(0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule task for second global handles broadcast: %s", H5_daos_err_to_string(ret));
                udata = NULL;
            } /* end if */
            else {
                daos_iov_t gch_glob, gph_glob;

                /* Set up gph_glob */
                gph_glob.iov_buf = p;
                gph_glob.iov_len = (size_t)gph_len;
                gph_glob.iov_buf_len = (size_t)gph_len;

                /* Get pool handle */
                if(0 != (ret = daos_pool_global2local(gph_glob, &udata->req->file->container_poh)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get container's global pool handle: %s", H5_daos_err_to_string(ret));

                p += gph_len;

                /* Set up gch_glob */
                gch_glob.iov_buf = p;
                gch_glob.iov_len = (size_t)gch_len;
                gch_glob.iov_buf_len = (size_t)gch_len;

                /* Get container handle */
                if(0 != (ret = daos_cont_global2local(udata->req->file->container_poh, gch_glob, &udata->req->file->coh)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get global container handle: %s", H5_daos_err_to_string(ret));
            } /* end else */
        } /* end else */
    } /* end else */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast global container handles completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_metatask, ret_value);

        /* Free buffer */
        DV_free(udata->buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_handles_bcast_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_container_handles_task
 *
 * Purpose:     Asynchronous task for creating a global container handle
 *              and container pool handle to use for broadcast.  Note this
 *              does not hold a reference to udata so it must be held by a
 *              task that depends on this task.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_get_container_handles_task(tse_task_t *task)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    daos_iov_t gch_glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    daos_iov_t gph_glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    uint8_t *p;
    size_t req_buf_len;
    int ret;
    int ret_value = 0; /* Return value */

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for global container handles retrieval task");

    assert(udata->req);
    assert(udata->req->file);

    /* Calculate size of global cont handles */
    if(udata->req->status >= -H5_DAOS_INCOMPLETE) {
        if(0 != (ret = daos_cont_local2global(udata->req->file->coh, &gch_glob)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't calculate size of global container handle: %s", H5_daos_err_to_string(ret));
        if(0 != (ret = daos_pool_local2global(udata->req->file->container_poh, &gph_glob)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't calculate size of container's global pool handle: %s", H5_daos_err_to_string(ret));
    } /* end if */

    req_buf_len = (2 * H5_DAOS_ENCODED_UINT64_T_SIZE) +
            MAX(gch_glob.iov_buf_len + gph_glob.iov_buf_len, (2 * H5_DAOS_GH_BUF_SIZE));

    if(!udata->buffer || (udata->buffer_len < (int)req_buf_len)) {
        void *tmp;

        /* Allocate buffer */
        if(NULL == (tmp = DV_realloc(udata->buffer, req_buf_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't allocate space for global container handles");
        udata->buffer = tmp;
        udata->buffer_len = (int)req_buf_len;
        udata->count = (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE);
        gch_glob.iov_len = gch_glob.iov_buf_len;
        gph_glob.iov_len = gph_glob.iov_buf_len;
    } /* end if */

    /* Check for previous errors (wait until after allocation because that must
     * always be done) */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Encode container's global pool handle length */
    p = udata->buffer;
    UINT64ENCODE(p, (uint64_t)gph_glob.iov_buf_len)

    /* Encode global container handle length */
    UINT64ENCODE(p, (uint64_t)gch_glob.iov_buf_len)

    /* Get container's global pool handle */
    gph_glob.iov_buf = p;
    if(0 != (ret = daos_pool_local2global(udata->req->file->container_poh, &gph_glob)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get container's global pool handle: %s", H5_daos_err_to_string(ret));

    p += gph_glob.iov_buf_len;

    /* Get global container handle */
    gch_glob.iov_buf = p;
    if(0 != (ret = daos_cont_local2global(udata->req->file->coh, &gch_glob)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get global container handle: %s", H5_daos_err_to_string(ret));

done:
    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = ret_value;
        udata->req->failed_task = "get global container handles";
    } /* end if */

    /* Release our reference to req */
    if(H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_get_container_handles_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_handles_bcast
 *
 * Purpose:     Broadcast a file's pool handle and container handle to
 *              other processes.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_handles_bcast(H5_daos_file_t *file, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    size_t buf_size = 0;
    int ret;
    herr_t ret_value = SUCCEED; /* Return value */

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if(NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for MPI broadcast user data");
    bcast_udata->req = req;
    bcast_udata->obj = NULL;
    bcast_udata->sched = &file->sched;
    bcast_udata->bcast_metatask = NULL;
    bcast_udata->buffer = NULL;
    bcast_udata->buffer_len = 0;
    bcast_udata->count = 0;

    buf_size = (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE);

    /* check if this is the lead rank */
    if(file->my_rank == 0) {
        tse_task_t *get_handles_task;

        /* Create task to get global container handle and global container pool handle */
        if(0 != (ret = tse_task_create(H5_daos_get_container_handles_task, &file->sched, bcast_udata, &get_handles_task)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to get global container handles: %s", H5_daos_err_to_string(ret));

        /* Register dependency for task */
        if(*dep_task && 0 != (ret = tse_task_register_deps(get_handles_task, 1, dep_task)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create dependencies for task to get global container handles: %s", H5_daos_err_to_string(ret));

        /* Set private data for task to get global container handle */
        (void)tse_task_set_priv(get_handles_task, bcast_udata);

        /* Schedule task to get global handles (or save it to be
         * scheduled later) and give it a reference to req.  Do not transfer
         * ownership of bcast_udata, as that will be owned by bcast_task (which
         * will depend on get_handles_task, so it will not free it before
         * get_handles_task runs). */
        if(*first_task) {
            if(0 != (ret = tse_task_schedule(get_handles_task, false)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to get global container handles: %s", H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = get_handles_task;
        req->rc++;
        *dep_task = get_handles_task;
    } /* end if */
    else {
        /* Allocate global handle buffer with default initial size */
        if(NULL == (bcast_udata->buffer = DV_malloc(buf_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for global container handles");
        bcast_udata->buffer_len = (int)buf_size;
        bcast_udata->count = (int)buf_size;
    } /* end else */

done:
    /* Do broadcast */
    if(bcast_udata) {
        if(H5_daos_mpi_ibcast(bcast_udata, &file->sched, NULL, buf_size,
                (ret_value < 0 ? TRUE : FALSE), NULL, H5_daos_handles_bcast_comp_cb,
                req, first_task, dep_task) < 0) {
            DV_free(bcast_udata->buffer);
            DV_free(bcast_udata);
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "failed to broadcast global container handles");
        } /* end if */

        bcast_udata = NULL;
    } /* end if */

    /* Make sure we cleaned up */
    assert(!bcast_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_file_handles_bcast() */


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
    hid_t fapl_id, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_file_t *file = NULL;
    H5_daos_fapl_t fapl_info = {0};
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
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL");

    /*
     * Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if(0==(flags & (H5F_ACC_EXCL|H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL;      /*default*/
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct");
    file->item.open_req = NULL;
    file->container_poh = DAOS_HDL_INVAL;
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;
    file->item.rc = 1;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name");
    file->flags = flags;
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl");

    /* Create DAOS task scheduler */
    if(0 != (ret = tse_sched_init(&file->sched, NULL, NULL)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task scheduler: %s", H5_daos_err_to_string(ret));
    sched_init = TRUE;

    /* Get information from the FAPL */
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct");

    /* Set MPI container info */
    if(H5_daos_cont_set_mpi_info(file, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info");

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Fill FAPL cache */
    if(H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache");

    /* Generate oid for global metadata object */
    if(H5_daos_oid_encode(&file->glob_md_oid, H5_DAOS_OIDX_GMD, H5I_GROUP,
            fcpl_id == H5P_FILE_CREATE_DEFAULT ? H5P_DEFAULT : fcpl_id,
            H5_DAOS_OBJ_CLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID");

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request");

    /* If the pool UUID hasn't been explicitly set, attempt to create a default pool. */
    if(uuid_is_null(H5_daos_pool_uuid_g) &&
            H5_daos_pool_create(H5_daos_pool_uuid_g, NULL, NULL, file->comm) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTCREATE, NULL, "failed to create pool");

    if(file->my_rank == 0) {
        /* Connect to container's pool */
        if(H5_daos_pool_connect(&H5_daos_pool_uuid_g, H5_daos_pool_grp_g,
                &H5_daos_pool_svcl_g, DAOS_PC_RW, &file->container_poh, NULL, &file->sched,
                int_req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't connect to pool");

        /* Create container on rank 0 */
        if(H5_daos_cont_create(file, flags, int_req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create DAOS container");
    } /* end if */

    /* Broadcast handles (container handle and, optionally, pool handle)
     * to other procs if any.
     */
    if((file->num_procs > 1) && (H5_daos_file_handles_bcast(file, int_req, &first_task, &dep_task) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, NULL, "can't broadcast DAOS container/pool handles");

    /* Open global metadata object */
    if(H5_daos_obj_open(file, int_req, &file->glob_md_oid, DAOS_OO_RW, &file->glob_md_oh, "global metadata object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open global metadata object");

    /* Create root group */
    if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_create_helper(
            file, TRUE, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, NULL, NULL,
            0, TRUE, int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group");

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
        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, NULL, "can't write root group OID to global metadata object: %s", H5_daos_err_to_string(ret));
#endif

    ret_value = (void *)file;

done:
    if(fapl_info.free_comm_info)
        if(H5_daos_comm_info_free(&fapl_info.comm, &fapl_info.info) < 0)
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, NULL, "failed to free copy of MPI communicator and info");

    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &file->sched, int_req, &int_req->finalize_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(sched_init && H5_daos_progress(&file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, NULL, "file creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));
    
        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't free request");
    } /* end if */

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file");
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_file_create() */


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
    H5_daos_cont_op_info_t *create_udata = NULL;
    herr_t ret_value = SUCCEED;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (create_udata = (H5_daos_cont_op_info_t *)DV_malloc(sizeof(H5_daos_cont_op_info_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for container create task");
    create_udata->req = req;
    create_udata->cont_op_metatask = NULL;
    create_udata->poh = &file->container_poh;
    create_udata->path = file->file_name;
    create_udata->flags = flags;
    create_udata->ignore_missing_path = TRUE;
    create_udata->duns_attr.da_type = DAOS_PROP_CO_LAYOUT_HDF5;
    create_udata->duns_attr.da_props = NULL;
    create_udata->duns_attr.da_oclass_id = file->fapl_cache.default_object_class;
    memset(&create_udata->pool_info, 0, sizeof(daos_pool_info_t));

    if(!H5_daos_bypass_duns_g) {
        uuid_copy(create_udata->duns_attr.da_cuuid, file->uuid);

        /* Create task to attempt to resolve DUNS path. This task will handle
         * the H5F_ACC_EXCL and H5F_ACC_TRUNC flags. */
        if(H5_daos_duns_resolve_path(create_udata, &file->sched, NULL, H5_daos_duns_resolve_path_comp_cb,
                req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to attempt to resolve DUNS path");

        /* Create task for filling in pool UUID for container create */
        if(H5_daos_pool_query(create_udata->poh, &create_udata->pool_info, NULL, NULL,
                &file->sched, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to query pool UUID");

        /* Create task for DUNS path create and container create */
        if(H5_daos_duns_create_path(create_udata, &file->sched,
                H5_daos_duns_create_path_prep_cb, H5_daos_duns_create_path_comp_cb,
                req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create DUNS path");
    } /* end if */
    else {
        /* If the H5F_ACC_EXCL flag was specified, ensure that the container does
         * not exist. */
        if(flags & H5F_ACC_EXCL)
            if(H5_daos_get_cont_open_task(create_udata, &file->sched, H5_daos_cont_open_prep_cb,
                    H5_daos_excl_open_comp_cb, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to attempt to open container");

        /* Delete the container if H5F_ACC_TRUNC is set.  This shouldn't cause a
         * problem even if the container doesn't exist. */
        if(flags & H5F_ACC_TRUNC)
            if(H5_daos_get_cont_destroy_task(create_udata, &file->sched, H5_daos_cont_destroy_prep_cb,
                    H5_daos_cont_destroy_comp_cb, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy container");

        /* Create task for container create */
        if(H5_daos_get_cont_create_task(create_udata, &file->sched, H5_daos_cont_create_prep_cb,
                H5_daos_cont_create_comp_cb, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create container");
    } /* end else */

    /* Relinquish control of the container creation udata to the
     * task's completion callback */
    create_udata = NULL;

    /* Strip H5F_ACC_TRUNC and H5F_ACC_EXCL flags from following container open operation */
    flags = flags & ~H5F_ACC_TRUNC;
    flags = flags & ~H5F_ACC_EXCL;

    /* Open the container and start the transaction */
    if(H5_daos_cont_open(file, flags, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't open DAOS container");

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        create_udata = DV_free(create_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!create_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_cont_create_task
 *
 * Purpose:     Creates an asynchronous DAOS task that simply calls
 *              daos_cont_create.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_cont_create_task(H5_daos_cont_op_info_t *cont_op_info, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *create_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(cont_op_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container create */
    if(0 != (ret = daos_task_create(DAOS_OPC_CONT_CREATE, sched,
            *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, &create_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create container: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for container create */
    if(prep_cb || comp_cb)
        if(0 != (ret = tse_task_register_cbs(create_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to create container: %s", H5_daos_err_to_string(ret));

    /* Set private data for container create */
    (void)tse_task_set_priv(create_task, cont_op_info);

    /* Schedule container create task (or save it to be scheduled later) and give
     * it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(create_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to create container: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = create_task;
    req->rc++;
    *dep_task = create_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_cont_create_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_create_path
 *
 * Purpose:     Creates an asynchronous task that simply calls
 *              duns_create_path.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_duns_create_path(H5_daos_cont_op_info_t *create_udata, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *create_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(create_udata);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for DUNS path create */
    if(0 != (ret = tse_task_create(H5_daos_duns_create_path_task, sched, create_udata, &create_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create DUNS path: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(create_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register dependencies for DUNS path creation task: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for DUNS path create task */
    if(prep_cb || comp_cb)
        if(0 != (ret = tse_task_register_cbs(create_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for DUNS path creation task: %s", H5_daos_err_to_string(ret));

    /* Schedule DUNS path create task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(create_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to create DUNS path: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = create_task;
    req->rc++;
    *dep_task = create_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_duns_create_path() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_create_path_task
 *
 * Purpose:     Asynchronous task for container creation that simply calls
 *              duns_create_path.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_create_path_task(tse_task_t *task)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path creation task");

    assert(udata->req);
    assert(udata->path);
    assert(udata->poh);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Create the DUNS path and the DAOS container - allow for failure */
    ret_value = duns_create_path(*udata->poh, udata->path, &udata->duns_attr);

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_duns_create_path_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_create_path_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous duns_create_path.
 *              Currently checks for errors from previous tasks then copies
 *              the DAOS pool UUID from the daos_pool_info_t struct into
 *              the duns_attr_t struct for duns_create_path.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_create_path_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path creation task");

    assert(udata->req);

    /* Handle errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_PRE_ERROR);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_SHORT_CIRCUIT);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Set the pool UUID so that duns_create_path will store
     * this info with the resulting file that is created.
     */
    uuid_copy(udata->duns_attr.da_puuid, udata->pool_info.pi_uuid);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_duns_create_path_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_create_path_comp_cb
 *
 * Purpose:     Completion callback for asynchronous duns_create_path.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_create_path_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path creation task");

    assert(udata->req);

    /* Handle errors in duns_create_path task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "DUNS path creation";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "DUNS path creation completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_duns_create_path_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_create_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_cont_create.
 *              Currently checks for errors from previous tasks then sets
 *              arguments for daos_cont_create.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_create_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    daos_cont_create_t *create_args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for container create task");

    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);
    assert(udata->poh);

    /* Handle errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_PRE_ERROR);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_SHORT_CIRCUIT);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Set daos_cont_create task args */
    if(NULL == (create_args = daos_task_get_args(task))) {
        tse_task_complete(task, -H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for container create task");
    } /* end if */
    create_args->poh = *udata->poh;
    create_args->prop = NULL;
    uuid_copy(create_args->uuid, udata->req->file->uuid);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_cont_create_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_create_comp_cb
 *
 * Purpose:     Completion callback for asynchronous daos_cont_create.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_create_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DAOS container create task");

    assert(udata->req);

    /* Handle errors in daos_cont_create task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "DAOS container create";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "DAOS container create completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_create_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_excl_open_comp_cb
 *
 * Purpose:     Complete callback for container open attempt for
 *              H5F_ACC_EXCL.  Identical to H5_daos_generic_comp_cb except
 *              expects -DER_NONEXIST.
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
H5_daos_excl_open_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for generic task");

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in task.  Only record error in udata->req_status if it does
     * not already contain an error (it could contain an error if another task
     * this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && task->dt_result != -DER_NONEXIST
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "excl. container open";
    } /* end if */

    /* If the open succeeded, return failure (we're verifying that the file
     * doesn't exist) */
    if(task->dt_result == 0)
        D_DONE_ERROR(H5E_FILE, H5E_FILEEXISTS, -H5_DAOS_FILE_EXISTS, "exclusive open failed: file already exists");

done:
    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = ret_value;
        udata->req->failed_task = "excl. container open completion callback";
    } /* end if */

    /* Release our reference to req */
    if(H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_excl_open_comp_cb() */


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
    hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_file_t *file = NULL;
    H5_daos_fapl_t fapl_info = {0};
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    daos_obj_id_t root_grp_oid = {0, 0};
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    void *ret_value = NULL;

    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL");
    /* If bypassing DUNS, either DAOS_POOL must have been set, or H5daos_init
     * must have been called with a valid pool UUID.
     */
    if(H5_daos_bypass_duns_g && uuid_is_null(H5_daos_pool_uuid_g))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, NULL, "DAOS pool UUID is invalid - pool UUID must be valid when bypassing DUNS");

    /* Allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct");
    file->item.open_req = NULL;
    file->container_poh = DAOS_HDL_INVAL;
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;
    file->item.rc = 1;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name");
    file->flags = flags;
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl");

    /* Create DAOS task scheduler */
    if(0 != (ret = tse_sched_init(&file->sched, NULL, NULL)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task scheduler: %s", H5_daos_err_to_string(ret));

    /* Get information from the FAPL */
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct");

    /* Set MPI container info */
    if(H5_daos_cont_set_mpi_info(file, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info");

    /* Hash file name to create uuid if bypassing DUNS */
    if(H5_daos_bypass_duns_g)
        H5_daos_hash128(name, &file->uuid);

#ifdef DV_HAVE_SNAP_OPEN_ID
    if(H5Pget(fapl_id, H5_DAOS_SNAP_OPEN_ID, &snap_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for snap ID");

    /* Check for opening a snapshot with write access (disallowed) */
    if((snap_id != H5_DAOS_SNAP_ID_INVAL) && (flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "write access requested to snapshot - disallowed");
#endif

    /* Fill FAPL cache */
    if(H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache");

    /* Generate oid for global metadata object */
    if(H5_daos_oid_encode(&file->glob_md_oid, H5_DAOS_OIDX_GMD, H5I_GROUP,
            fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
            H5_DAOS_ROOT_OPEN_OCLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID");

    /* Generate root group oid */
    if(H5_daos_oid_encode(&root_grp_oid, H5_DAOS_OIDX_ROOT, H5I_GROUP,
            fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
            H5_DAOS_ROOT_OPEN_OCLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode root group object ID");

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request");

    /* Open container on rank 0 */
    if((file->my_rank == 0) && H5_daos_cont_open(file, flags, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't open DAOS container");

    /* Broadcast handles (container handle and, optionally, pool handle)
     * to other procs if any.
     */
    if((file->num_procs > 1) && (H5_daos_file_handles_bcast(file, int_req, &first_task, &dep_task) < 0))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't broadcast DAOS container/pool handles");

    /* Open global metadata object */
    if(H5_daos_obj_open(file, int_req, &file->glob_md_oid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->glob_md_oh, "global metadata object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open global metadata object");

    /* Open root group and fill in root group's oid */
    if(NULL == (file->root_grp = H5_daos_group_open_helper(file,
            H5P_GROUP_ACCESS_DEFAULT, TRUE, int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open root group");
    file->root_grp->obj.oid = root_grp_oid;

    ret_value = (void *)file;

done:
    if(fapl_info.free_comm_info)
        if(H5_daos_comm_info_free(&fapl_info.comm, &fapl_info.info) < 0)
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, NULL, "failed to free copy of MPI communicator and info");

    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &file->sched, int_req, &int_req->finalize_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(H5_daos_progress(&file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, NULL, "file open failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't free request");
    } /* end if */

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file");
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_file_open() */


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
H5_daos_cont_open(H5_daos_file_t *file, unsigned flags,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_cont_op_info_t *open_udata = NULL;
    H5_daos_generic_cb_ud_t *tx_open_udata = NULL;
#ifdef H5_DAOS_USE_TRANSACTIONS
    tse_task_t *tx_open_task;
#endif
    herr_t ret_value = SUCCEED;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (open_udata = (H5_daos_cont_op_info_t *)DV_malloc(sizeof(H5_daos_cont_op_info_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for container open task");
    open_udata->req = req;
    open_udata->poh = &file->container_poh;
    open_udata->cont_op_metatask = NULL;
    open_udata->path = file->file_name;
    open_udata->flags = flags;
    open_udata->ignore_missing_path = FALSE;
    memset(&open_udata->duns_attr, 0, sizeof(struct duns_attr_t));

    /* Create task for resolving DUNS path if not bypassing DUNS */
    if(!H5_daos_bypass_duns_g && H5_daos_duns_resolve_path(open_udata, &file->sched,
            NULL, H5_daos_duns_resolve_path_comp_cb, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to resolve DUNS path");

    /* Only connect to pool for file opens. Avoid pool connection for file creations */
    if(0 == (flags & H5F_ACC_CREAT)) {
        uuid_t *puuid = H5_daos_bypass_duns_g ? &H5_daos_pool_uuid_g : &open_udata->duns_attr.da_puuid;

        /* If the file doesn't already have a pool associated with it, connect to its pool */
        if(daos_handle_is_inval(file->container_poh) && H5_daos_pool_connect(puuid, H5_daos_pool_grp_g,
                &H5_daos_pool_svcl_g, flags & H5F_ACC_RDWR ? DAOS_PC_RW : DAOS_PC_RO,
                        open_udata->poh, NULL, &file->sched, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to connect to container's pool");
    }

    if(H5_daos_get_cont_open_task(open_udata, &file->sched, H5_daos_cont_open_prep_cb,
            H5_daos_cont_open_comp_cb, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open container");

    open_udata = NULL;

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Create task for transaction open */
    if(0 != (ret = daos_task_create(DAOS_OPC_TX_OPEN, &file->sched, 0, NULL, &tx_open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open transaction: %s", H5_daos_err_to_string(ret));

    /* Register dependency for task */
    assert(*dep_task);
    if(0 != (ret = tse_task_register_deps(tx_open_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create dependencies for task to open transaction: %s", H5_daos_err_to_string(ret));

    /* Set private data for transaction open */
    if(NULL == (tx_open_udata = (H5_daos_generic_cb_ud_t *)DV_malloc(sizeof(H5_daos_generic_cb_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for transaction open task");
    tx_open_udata->req = req;
    tx_open_udata->task_name = "transaction open";
    (void)tse_task_set_priv(tx_open_task, tx_open_udata);

    /* Set callback functions for transaction open */
    if(0 != (ret = tse_task_register_cbs(tx_open_task, H5_daos_tx_open_prep_cb, NULL, 0, H5_daos_tx_open_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to open container: %s", H5_daos_err_to_string(ret));

    /* Schedule transaction open task and give it a reference to req */
    assert(*first_task);
    if(0 != (ret = tse_task_schedule(tx_open_task, false)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to open transaction: %s", H5_daos_err_to_string(ret));
    req->rc++;
    tx_open_udata = NULL;
    *dep_task = tx_open_task;
#endif /* H5_DAOS_USE_TRANSACTIONS */

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
        open_udata = DV_free(open_udata);
        tx_open_udata = DV_free(tx_open_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!open_udata);
    assert(!tx_open_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_cont_open_task
 *
 * Purpose:     Creates an asynchronous DAOS task that simply calls
 *              daos_cont_open.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_cont_open_task(H5_daos_cont_op_info_t *cont_op_info, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *open_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(cont_op_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container open */
    if(0 != (ret = daos_task_create(DAOS_OPC_CONT_OPEN, sched,
            *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, &open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open container: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for container open */
    if(prep_cb || comp_cb)
        if(0 != (ret = tse_task_register_cbs(open_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to open container: %s", H5_daos_err_to_string(ret));

    /* Set private data for container open */
    (void)tse_task_set_priv(open_task, cont_op_info);

    /* Schedule container open task (or save it to be scheduled later) and give
     * it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(open_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to open container: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = open_task;
    req->rc++;
    *dep_task = open_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_cont_open_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_resolve_path
 *
 * Purpose:     Creates an asynchronous task that simply calls
 *              duns_resolve_path.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_duns_resolve_path(H5_daos_cont_op_info_t *resolve_udata, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *resolve_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(resolve_udata);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for resolving DUNS path */
    if(0 != (ret = tse_task_create(H5_daos_duns_resolve_path_task, sched, resolve_udata, &resolve_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to resolve DUNS path: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(resolve_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register dependencies for DUNS path resolve task: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for DUNS path resolve task */
    if(prep_cb || comp_cb)
        if(0 != (ret = tse_task_register_cbs(resolve_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for DUNS path resolve task: %s", H5_daos_err_to_string(ret));

    /* Schedule DUNS path resolve task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(resolve_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to resolve DUNS path: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = resolve_task;
    req->rc++;
    *dep_task = resolve_task;

    /* If resolving a DUNS path with H5F_ACC_TRUNC access, the DUNS path resolve
     * task may need to create more tasks to destroy an existing DUNS path.
     * Therefore, we create a metatask to ensure that those new tasks will
     * complete before any tasks depending on the initial DUNS path resolve
     * task can be scheduled.
     */
    if(resolve_udata->flags & H5F_ACC_TRUNC) {
        /* Create metatask for DUNS path resolve task in case more tasks
         * need to be created to destroy an existing DUNS path during
         * file creates with H5F_ACC_TRUNC access.
         */
        if(0 != (ret = tse_task_create(H5_daos_metatask_autocomplete, sched, NULL, &resolve_udata->cont_op_metatask)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create meta task for DUNS path resolve: %s", H5_daos_err_to_string(ret));

        /* Register dependency on DUNS path resolve task for metatask */
        if(0 != (ret = tse_task_register_deps(resolve_udata->cont_op_metatask, 1, &resolve_task)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, ret, "can't create dependencies for DUNS path resolve metatask: %s", H5_daos_err_to_string(ret));

        /* Schedule meta task */
        assert(*first_task);
        if(0 != (ret = tse_task_schedule(resolve_udata->cont_op_metatask, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule meta task for DUNS path resolve: %s", H5_daos_err_to_string(ret));

        *dep_task = resolve_udata->cont_op_metatask;
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_duns_resolve_path() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_resolve_path_task
 *
 * Purpose:     Asynchronous task that simply calls duns_resolve_path.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_resolve_path_task(tse_task_t *task)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path resolve task");

    assert(udata->req);
    assert(udata->path);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Resolve the DUNS path - allow for failure */
    ret_value = duns_resolve_path(udata->path, &udata->duns_attr);

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_duns_resolve_path_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_resolve_path_comp_cb
 *
 * Purpose:     Completion callback for asynchronous duns_resolve_path.
 *              Currently checks for a failed task, then determines if
 *              special action should be taken based on flags passed in
 *              via the user data structure.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_resolve_path_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path resolve task");

    assert(udata->req);
    assert(udata->path);

    if(task->dt_result != 0) {
        /* DSINC - DER_INVAL and DER_NONEXIST do not need to be checked against with the latest DAOS master. */
        if(udata->ignore_missing_path &&
                ((-DER_NONEXIST == task->dt_result) || (-DER_INVAL == task->dt_result) || (ENOENT == task->dt_result))) {
            D_GOTO_DONE(0); /* Short-circuit success when file is expected to potentially be missing */
        } /* end if */
        else if(task->dt_result != -H5_DAOS_PRE_ERROR
                && task->dt_result != -H5_DAOS_SHORT_CIRCUIT
                && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            /* Set result to -H5_DAOS_BAD_VALUE instead of task->dt_result
             * because DUNS functions return positive error codes and that would
             * trip up the error handling in this connector. */
            udata->req->status = -H5_DAOS_BAD_VALUE;
            udata->req->failed_task = "DUNS path resolve";
            if(ENOENT == task->dt_result) {
                udata->req->status = -H5_DAOS_H5_OPEN_ERROR;
                D_DONE_ERROR(H5E_FILE, H5E_CANTOPENFILE, -H5_DAOS_H5_OPEN_ERROR, "file '%s' does not exist", udata->path);
            }
            if(EOPNOTSUPP == task->dt_result) {
                udata->req->status = -H5_DAOS_H5_UNSUPPORTED_ERROR;
                D_DONE_ERROR(H5E_FILE, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR, "can't create/resolve DUNS path - DUNS not supported on file system");
            }
            else if(ENODATA == task->dt_result)
                D_DONE_ERROR(H5E_FILE, H5E_NOTHDF5, -H5_DAOS_BAD_VALUE, "file '%s' is not a valid HDF5 DAOS file", udata->path);
        } /* end if */
    } /* end if */
    else {
        /* Verify that this is actually an HDF5 DAOS file */
        if(udata->duns_attr.da_type != DAOS_PROP_CO_LAYOUT_HDF5)
            D_GOTO_ERROR(H5E_FILE, H5E_NOTHDF5, -H5_DAOS_BAD_VALUE, "file '%s' is not a valid HDF5 DAOS file", udata->path);

        /* Determine if special action is required according to flags specified */
        if(udata->flags & H5F_ACC_EXCL) {
            /* File already exists during exclusive open */
            D_GOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, -H5_DAOS_FILE_EXISTS, "exclusive open failed: file already exists");
        } /* end if */
        else if(udata->flags & H5F_ACC_TRUNC) {
            assert(udata->cont_op_metatask);

            if(H5_daos_file_delete(&udata->duns_attr.da_puuid, udata->path,
                    TRUE, &udata->req->file->sched, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_H5_DESTROY_ERROR, "can't create task to destroy container");

            /* Register dependency on dep_task for file deletion metatask */
            if(dep_task && 0 != (ret = tse_task_register_deps(udata->cont_op_metatask, 1, &dep_task)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, ret, "can't create dependencies for file deletion metatask: %s", H5_daos_err_to_string(ret));
        } /* end else */
    } /* end else */

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, ret, "can't schedule task to destroy DUNS path: %s", H5_daos_err_to_string(ret));

    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "DUNS path resolve completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_duns_resolve_path_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_open_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_cont_open.
 *              Currently checks for errors from previous tasks then sets
 *              arguments for daos_cont_open.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_open_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    daos_cont_open_t *open_args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for container open task");

    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);
    assert(udata->poh);

    /* Handle errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_PRE_ERROR);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_SHORT_CIRCUIT);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Set file's UUID if necessary */
    if(!H5_daos_bypass_duns_g)
        uuid_copy(udata->req->file->uuid, udata->duns_attr.da_cuuid);

    /* Set daos_cont_open task args */
    if(NULL == (open_args = daos_task_get_args(task))) {
        tse_task_complete(task, -H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for container open task");
    } /* end if */
    open_args->poh = *udata->poh;
    uuid_copy(open_args->uuid, udata->req->file->uuid);
    open_args->flags = udata->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO;
    open_args->coh = &udata->req->file->coh;
    open_args->info = NULL;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_cont_open_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_open_comp_cb
 *
 * Purpose:     Completion callback for asynchronous daos_cont_open.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_open_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DAOS container open task");

    assert(udata->req);

    /* Handle errors in daos_cont_open task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "DAOS container open";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "DAOS container open completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_open_comp_cb() */


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
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_file_t *file = NULL;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if(get_type == H5VL_FILE_GET_NAME) {
        file = item->file;

        if(H5I_FILE != item->type && H5I_GROUP != item->type &&
           H5I_DATATYPE != item->type && H5I_DATASET != item->type &&
           H5I_ATTR != item->type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file, group, datatype, dataset or attribute");
    }
    else {
        file = (H5_daos_file_t *)item;
        if(H5I_FILE != file->item.type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");
    }

    H5_DAOS_MAKE_ASYNC_PROGRESS(file->sched, FAIL);

    switch (get_type) {
        /* "get container info" */
        case H5VL_FILE_GET_CONT_INFO:
        {
            H5VL_file_cont_info_t *info = va_arg(arguments, H5VL_file_cont_info_t *);

            /* Verify structure version */
            if(info->version != H5VL_CONTAINER_INFO_VERSION)
                D_GOTO_ERROR(H5E_FILE, H5E_VERSION, FAIL, "wrong container info version number");

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
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FAPL");

            break;
        } /* H5VL_FILE_GET_FAPL */

        /* H5Fget_create_plist */
        case H5VL_FILE_GET_FCPL:
        {
            hid_t *ret_id = va_arg(arguments, hid_t *);

            /* The file's FCPL is stored as the group's GCPL */
            if((*ret_id = H5Pcopy(file->root_grp->gcpl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FCPL");

            /* Set root group's object class on fcpl */
            if(H5_daos_set_oclass_from_oid(*ret_id, file->root_grp->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");

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
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs");
            if(obj_types & H5F_OBJ_DATASET)
                if(H5Iiterate(H5I_DATASET, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open dataset IDs");
            if(obj_types & H5F_OBJ_GROUP)
                if(H5Iiterate(H5I_GROUP, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs");
            if(obj_types & H5F_OBJ_DATATYPE)
                if(H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open datatype IDs");
            if(obj_types & H5F_OBJ_ATTR)
                if(H5Iiterate(H5I_ATTR, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open attribute IDs");

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
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs");
                if(obj_types & H5F_OBJ_DATASET)
                    if(H5Iiterate(H5I_DATASET, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open dataset IDs");
                if(obj_types & H5F_OBJ_GROUP)
                    if(H5Iiterate(H5I_GROUP, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs");
                if(obj_types & H5F_OBJ_DATATYPE)
                    if(H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open datatype IDs");
                if(obj_types & H5F_OBJ_ATTR)
                    if(H5Iiterate(H5I_ATTR, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open attribute IDs");
            }

            *ret_val = (ssize_t)udata.obj_count;

            break;
        } /* H5VL_FILE_GET_OBJ_IDS */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file get operation");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
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
    herr_t ret_value = SUCCEED;    /* Return value */

    if(item) {
        file = ((H5_daos_item_t *)item)->file;
        H5_DAOS_MAKE_ASYNC_PROGRESS(file->sched, FAIL);
    }

    switch (specific_type) {
        /* H5Fflush */
        case H5VL_FILE_FLUSH:
        {
            if(H5_daos_file_flush(file) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file");

            break;
        } /* H5VL_FILE_FLUSH */

        /* H5Freopen */
        case H5VL_FILE_REOPEN:
        {
            unsigned reopen_flags = file->flags;
            void **ret_file = va_arg(arguments, void **);

            /* Strip any file creation-related flags */
            reopen_flags &= ~(H5F_ACC_TRUNC | H5F_ACC_EXCL | H5F_ACC_CREAT);
            if(NULL == (*ret_file = H5_daos_file_open(file->file_name, reopen_flags, file->fapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "can't reopen file");

            break;
        } /* H5VL_FILE_REOPEN */

        /* H5Fmount */
        case H5VL_FILE_MOUNT:
        /* H5Fmount */
        case H5VL_FILE_UNMOUNT:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation");
            break;

        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE:
        {
            hid_t file_fapl = va_arg(arguments, hid_t);
            const char *filename = va_arg(arguments, const char *);
            htri_t *ret_is_accessible = va_arg(arguments, htri_t *);
            struct duns_attr_t duns_attr;
            int ret;

            /* Initialize returned value in case we fail */
            *ret_is_accessible = FAIL;

            if(NULL == filename)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "filename is NULL");

            if(!H5_daos_bypass_duns_g) {
                /* Attempt to resolve the given filename to a valid HDF5 DAOS file */
                /* DSINC - DER_INVAL and DER_NONEXIST do not need to be checked against with the latest DAOS master. */
                if(0 != (ret = duns_resolve_path(filename, &duns_attr))) {
                    if((-DER_NONEXIST == ret) || (-DER_INVAL == ret) || (ENOENT == ret) || (ENODATA == ret)) {
                        /* File is missing, is not a HDF5 DAOS-based file, or does not contain the necessary DUNS attributes */
                        *ret_is_accessible = FALSE;
                    } /* end if */
                    else
                        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, FAIL, "duns_resolve_path failed: %s", H5_daos_err_to_string(ret));
                } /* end if */
                else if(duns_attr.da_type != DAOS_PROP_CO_LAYOUT_HDF5) {
                    /* File path resolved to a DAOS-based file, but was not an HDF5 DAOS file */
                    *ret_is_accessible = FALSE;
                } /* end else */
                else
                    *ret_is_accessible = TRUE;
            } /* end if */
            else {
                void *opened_file = NULL;

                H5E_BEGIN_TRY {
                    opened_file = H5_daos_file_open(filename, H5F_ACC_RDONLY, file_fapl, dxpl_id, NULL);
                } H5E_END_TRY;

                *ret_is_accessible = opened_file ? TRUE : FALSE;

                if(opened_file)
                    if(H5_daos_file_close(opened_file, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "error occurred while closing file");
            } /* end else */

            break;
        } /* H5VL_FILE_IS_ACCESSIBLE */

        /* H5Fdelete */
        case H5VL_FILE_DELETE:
        {
            hid_t fapl_id = va_arg(arguments, hid_t);
            const char *filename = va_arg(arguments, const char *);
            herr_t *delete_ret = va_arg(arguments, herr_t *);

            /* Initialize returned value in case we fail */
            *delete_ret = FAIL;

            if(H5_daos_file_delete_sync(filename, fapl_id) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETE, FAIL, "can't delete file");

            *delete_ret = SUCCEED;

            break;
        } /* H5VL_FILE_DELETE */

        /* Check if two files are the same */
        case H5VL_FILE_IS_EQUAL:
        {
            H5_daos_file_t *file2 = (H5_daos_file_t *)va_arg(arguments, void *);
            hbool_t *is_equal = va_arg(arguments, hbool_t *);

            if(file2->item.type != H5I_FILE)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "object is not a file!");

            if(!file || !file2)
                *is_equal = FALSE;
            else
                *is_equal = (memcmp(&file->uuid, &file2->uuid, sizeof(file->uuid)) == 0);
            break;
        } /* H5VL_FILE_IS_EQUAL */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_delete
 *
 * Purpose:     Creates an asynchronous task to delete a DUNS path/DAOS
 *              container.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_delete(uuid_t *puuid, const char *file_path, hbool_t ignore_missing,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_cont_op_info_t *destroy_udata = NULL;
    herr_t ret_value = SUCCEED;

    assert(puuid);
    assert(file_path);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (destroy_udata = (H5_daos_cont_op_info_t *)DV_malloc(sizeof(H5_daos_cont_op_info_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for file deletion task");
    destroy_udata->req = req;
    destroy_udata->poh = &destroy_udata->tmp_poh;
    destroy_udata->cont_op_metatask = NULL;
    destroy_udata->path = file_path;
    destroy_udata->flags = 0;
    destroy_udata->ignore_missing_path = ignore_missing;
    destroy_udata->duns_attr.da_type = DAOS_PROP_CO_LAYOUT_HDF5;

    /* Create tasks to connect to container's pool and destroy the DUNS path. */
    if(H5_daos_pool_connect(puuid, H5_daos_pool_grp_g, &H5_daos_pool_svcl_g, DAOS_PC_RW,
            &destroy_udata->tmp_poh, NULL, sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to connect to container's pool");

    if(H5_daos_duns_destroy_path(destroy_udata, sched, NULL,
            H5_daos_duns_destroy_path_comp_cb, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy DUNS path");

    if(H5_daos_pool_disconnect(&destroy_udata->tmp_poh, sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to disconnect from container's pool");

    /* Free private data after pool disconnect succeeds */
    if(H5_daos_free_async(destroy_udata->req->file, destroy_udata, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to free file deletion task data");

    /* Relinquish control of DUNS path destroy udata */
    destroy_udata = NULL;

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        destroy_udata = DV_free(destroy_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_file_delete() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_cont_destroy_task
 *
 * Purpose:     Creates an asynchronous DAOS task that simply calls
 *              daos_cont_destroy.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_cont_destroy_task(H5_daos_cont_op_info_t *cont_op_info, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *destroy_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(cont_op_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container destroy */
    if(0 != (ret = daos_task_create(DAOS_OPC_CONT_DESTROY, sched,
            *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, &destroy_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy container: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for container destroy */
    if(prep_cb || comp_cb)
        if(0 != (ret = tse_task_register_cbs(destroy_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to destroy container: %s", H5_daos_err_to_string(ret));

    /* Set private data for container destroy */
    (void)tse_task_set_priv(destroy_task, cont_op_info);

    /* Schedule container destroy task (or save it to be scheduled later) and give
     * it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(destroy_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to destroy container: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = destroy_task;
    req->rc++;
    *dep_task = destroy_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_cont_destroy_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_destroy_path
 *
 * Purpose:     Creates an asynchronous task that simply calls
 *              duns_destroy_path.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_duns_destroy_path(H5_daos_cont_op_info_t *destroy_udata, tse_sched_t *sched,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *destroy_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(destroy_udata);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for DUNS path/container destroy */
    if(0 != (ret = tse_task_create(H5_daos_duns_destroy_path_task, sched, destroy_udata, &destroy_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy DUNS path: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(destroy_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register dependencies for DUNS path destroy task: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for DUNS path destroy task */
    if(prep_cb || comp_cb)
        if(0 != (ret = tse_task_register_cbs(destroy_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for DUNS path destroy task: %s", H5_daos_err_to_string(ret));

    /* Schedule DUNS path/container destroy task (or save it to be scheduled later)
     * and give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(destroy_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to destroy DUNS path: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = destroy_task;
    req->rc++;
    *dep_task = destroy_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_duns_destroy_path() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_destroy_path_task
 *
 * Purpose:     Asynchronous task that simply calls duns_destroy_path.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_destroy_path_task(tse_task_t *task)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path destroy task");

    assert(udata->req);
    assert(udata->path);
    assert(udata->poh);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Destroy the DUNS path - allow for failure */
    ret_value = duns_destroy_path(*udata->poh, udata->path);

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_duns_destroy_path_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_duns_destroy_path_comp_cb
 *
 * Purpose:     Completion callback for asynchronous duns_destroy_path.
 *              Currently just checks for a failed task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_duns_destroy_path_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DUNS path destroy task");

    assert(udata->req);

    if(task->dt_result != 0 && task->dt_result != -H5_DAOS_SHORT_CIRCUIT) {
        /* DSINC - DER_INVAL and DER_NONEXIST do not need to be checked against with the latest DAOS master. */
        if(udata->ignore_missing_path &&
                ((-DER_NONEXIST == task->dt_result) || (-DER_INVAL == task->dt_result) || (ENOENT == task->dt_result)))
            D_GOTO_DONE(0); /* Short-circuit success for H5F_ACC_TRUNC access when file is missing */
        else if(ENODATA == task->dt_result)
            D_GOTO_ERROR(H5E_FILE, H5E_NOTHDF5, -H5_DAOS_BAD_VALUE, "file '%s' is not a valid HDF5 DAOS file", udata->path);
        else if(task->dt_result < -H5_DAOS_PRE_ERROR
                && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = task->dt_result;
            udata->req->failed_task = "DUNS path destroy";
        } /* end else */
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "DUNS path destroy completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_duns_destroy_path_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_destroy_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_cont_destroy.
 *              Currently checks for errors from previous tasks then sets
 *              arguments for daos_cont_destroy.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_destroy_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    daos_cont_destroy_t *destroy_args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for container destroy task");

    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);
    assert(udata->poh);

    /* Handle errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_PRE_ERROR);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT) {
        tse_task_complete(task, -H5_DAOS_SHORT_CIRCUIT);
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Set daos_cont_destroy task args */
    if(NULL == (destroy_args = daos_task_get_args(task))) {
        tse_task_complete(task, -H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for container destroy task");
    } /* end if */
    destroy_args->poh = *udata->poh;
    destroy_args->force = 1;
    uuid_copy(destroy_args->uuid, udata->req->file->uuid);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_cont_destroy_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_destroy_comp_cb
 *
 * Purpose:     Completion callback for asynchronous daos_cont_destroy.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_destroy_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_cont_op_info_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for DAOS container destroy task");

    assert(udata->req);

    /* Handle errors in daos_cont_destroy task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT
            && !(udata->ignore_missing_path && task->dt_result == -DER_NONEXIST)) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "DAOS container destroy";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "DAOS container destroy completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_destroy_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_delete_sync
 *
 * Purpose:     Synchronous routine for deleting a DAOS container/DAOS
 *              unified namespace path by using duns_destroy_path.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_delete_sync(const char *filename, hid_t fapl_id)
{
    H5_daos_fapl_t fapl_info = {0};
    daos_handle_t poh;
    hbool_t connected = FALSE;
    int mpi_rank, mpi_initialized;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(filename);

    /* Get information from the FAPL */
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get DAOS info struct");

    if(MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized");
    if(mpi_initialized) {
        MPI_Comm_rank(fapl_info.comm, &mpi_rank);
    } else {
        mpi_rank = 0;
    }

    if(mpi_rank == 0) {
        if(!H5_daos_bypass_duns_g) {
            struct duns_attr_t duns_attr;

            /* Attempt to resolve the given filename to a valid HDF5 DAOS file */
            if(0 != (ret = duns_resolve_path(filename, &duns_attr))) {
                if(ENODATA == ret)
                    D_GOTO_ERROR(H5E_FILE, H5E_NOTHDF5, FAIL, "file '%s' is not a valid HDF5 DAOS file", filename);
                else
                    D_GOTO_ERROR(H5E_FILE, H5E_PATH, FAIL, "duns_resolve_path failed: %s", H5_daos_err_to_string(ret));
            } /* end if */
            else {
                /* Verify that this is actually an HDF5 DAOS file */
                if(duns_attr.da_type != DAOS_PROP_CO_LAYOUT_HDF5)
                    D_GOTO_ERROR(H5E_FILE, H5E_NOTHDF5, FAIL, "file '%s' is not a valid HDF5 DAOS file", filename);

                /* Connect to container's pool */
                if(0 != (ret = daos_pool_connect(duns_attr.da_puuid, H5_daos_pool_grp_g,
                        &H5_daos_pool_svcl_g, DAOS_PC_RW, &poh, NULL, NULL)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't connect to pool: %s", H5_daos_err_to_string(ret));
                connected = TRUE;
            } /* end else */

            /* Destroy the DUNS path */
            if(0 != (ret = duns_destroy_path(poh, filename)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "duns_destroy_path failed: %s", H5_daos_err_to_string(ret));
        } /* end if */
        else {
            uuid_t cont_uuid;

            /* Connect to container's pool */
            if(0 != (ret = daos_pool_connect(H5_daos_pool_uuid_g, H5_daos_pool_grp_g,
                    &H5_daos_pool_svcl_g, DAOS_PC_RW, &poh, NULL, NULL)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't connect to pool: %s", H5_daos_err_to_string(ret));
            connected = TRUE;

            /* Hash file name to create uuid */
            H5_daos_hash128(filename, &cont_uuid);

            if(0 != (ret = daos_cont_destroy(poh, cont_uuid, 1, NULL /*event*/)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "can't destroy container: %s", H5_daos_err_to_string(ret));
        } /* end else */
    } /* end if */

    if(mpi_initialized)
        if(MPI_SUCCESS != MPI_Barrier(fapl_info.comm))
            D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed during file deletion");

done:
    if(fapl_info.free_comm_info)
        if(H5_daos_comm_info_free(&fapl_info.comm, &fapl_info.info) < 0)
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to free copy of MPI communicator and info");

    if(connected && (0 != (ret = daos_pool_disconnect(poh, NULL))))
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't disconnect from container's pool: %s", H5_daos_err_to_string(ret));

    D_FUNC_LEAVE;
} /* end H5_daos_file_delete_sync() */


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
H5_daos_file_close_helper(H5_daos_file_t *file, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    int ret;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Free file data structures */
    if(file->item.cur_op_pool) {
        assert(file->item.cur_op_pool->type == H5_DAOS_OP_TYPE_EMPTY);
        file->item.cur_op_pool = DV_free(file->item.cur_op_pool);
    } /* end if */
    if(file->item.open_req)
        if(H5_daos_req_free_int(file->item.open_req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't free request");
    file->item.open_req = NULL;
    if(file->file_name)
        file->file_name = DV_free(file->file_name);
    if(file->comm || file->info)
        if(H5_daos_comm_info_free(&file->comm, &file->info) < 0)
            D_DONE_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to free copy of MPI communicator and info");
    if(file->fapl_id != FAIL && H5Idec_ref(file->fapl_id) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close fapl");
    if(!daos_handle_is_inval(file->glob_md_oh))
        if(0 != (ret = daos_obj_close(file->glob_md_oh, NULL /*event*/)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close global metadata object: %s", H5_daos_err_to_string(ret));
    if(file->root_grp)
        if(H5_daos_group_close_real(file->root_grp) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close root group");
    if(!daos_handle_is_inval(file->coh))
        if(0 != (ret = daos_cont_close(file->coh, NULL /*event*/)))
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't close container: %s", H5_daos_err_to_string(ret));
    if(!daos_handle_is_inval(file->container_poh)) {
        if(0 != (ret = daos_pool_disconnect(file->container_poh, NULL)))
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't disconnect from container's pool: %s", H5_daos_err_to_string(ret));
        file->container_poh = DAOS_HDL_INVAL;
    }
    if(file->vol_id >= 0) {
        if(H5VLfree_connector_info(file->vol_id, file->vol_info) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't free VOL connector info");
        if(H5Idec_ref(file->vol_id) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "can't decrement VOL connector ID");
    } /* end if */

    /* Finish the scheduler *//* Make this cancel tasks?  Only if flush progresses until empty.  Otherwise change to custom progress function DSINC */
    if(H5_daos_progress(&file->sched, NULL, H5_DAOS_PROGRESS_WAIT) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't progress scheduler");
    tse_sched_fini(&file->sched);

    /* File is closed */
    file->closed = TRUE;

    /* Decrement ref count on file struct */
    H5_daos_file_decref(file);

    D_FUNC_LEAVE;
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
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    /* Flush the file (barrier, commit epoch, slip epoch) *Update comment DSINC */
    if(H5_daos_file_flush(file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file");

#if 0 /* DSINC */
    /* Flush the epoch */
    if(0 != (ret = daos_epoch_flush(file->coh, epoch, NULL /*state*/, NULL /*event*/)))
        D_DONE_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "can't flush epoch: %s", H5_daos_err_to_string(ret));
#endif

    /*
     * Ensure that all other processes are done with the file before
     * closing the container handle. This is to prevent invalid handle
     * issues due to rank 0 closing the container handle before other
     * ranks are done using it.
     */
    if(file->num_procs > 1 && (MPI_SUCCESS != MPI_Barrier(file->comm)))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed during file close");

    /* Close the file */
    if(H5_daos_file_close_helper(file, dxpl_id, req) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close file");

done:
    D_FUNC_LEAVE_API;
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
        D_GOTO_DONE(SUCCEED);

    /* Progress scheduler until empty? DSINC */

#if 0
    /* Collectively determine if anyone requested a snapshot of the epoch */
    if(MPI_SUCCESS != MPI_Reduce(file->my_rank == 0 ? MPI_IN_PLACE : &file->snap_epoch, &file->snap_epoch, 1, MPI_INT, MPI_LOR, 0, file->comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "failed to determine whether to take snapshot (MPI_Reduce)");

    /* Barrier on all ranks so we don't commit before all ranks are
     * finished writing. H5Fflush must be called collectively. */
    if(MPI_SUCCESS != MPI_Barrier(file->comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed");

    /* Commit the epoch */
    if(file->my_rank == 0) {
        /* Save a snapshot of this epoch if requested */
        /* Disabled until snapshots are supported in DAOS DSINC */

        if(file->snap_epoch)
            if(0 != (ret = daos_snap_create(file->coh, file->epoch, NULL /*event*/)))
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't create snapshot: %s", H5_daos_err_to_string(ret));

        /* Commit the epoch.  This should slip previous epochs automatically. */
        if(0 != (ret = daos_epoch_commit(file->coh, file->epoch, NULL /*state*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "failed to commit epoch: %s", H5_daos_err_to_string(ret));
    } /* end if */
#endif

done:
    D_FUNC_LEAVE;
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
    hbool_t collective_md_read;
    hbool_t collective_md_write;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Set initial collective metadata I/O settings for the file, then
     * determine if they are to be overridden from a specific setting
     * on the FAPL.
     */
    file->fapl_cache.is_collective_md_read = collective_md_read = FALSE;
    file->fapl_cache.is_collective_md_write = collective_md_write = TRUE;
    H5_DAOS_GET_METADATA_IO_MODES(file, fapl_id, H5P_FILE_ACCESS_DEFAULT,
            collective_md_read, collective_md_write, H5E_FILE, FAIL);
    file->fapl_cache.is_collective_md_read = collective_md_read;
    file->fapl_cache.is_collective_md_write = collective_md_write;

    /* Check for file default object class set on fapl_id */
    /* Note we do not copy the oclass_str in the property callbacks (there is no
     * "get" callback, so this is more like an H5P_peek, and we do not need to
     * free oclass_str as it points directly into the plist value */
    file->fapl_cache.default_object_class = OC_UNKNOWN;
    if((prop_exists = H5Pexist(fapl_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");
    if(prop_exists) {
        if(H5Pget(fapl_id, H5_DAOS_OBJ_CLASS_NAME, &oclass_str) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class");
        if(oclass_str && (oclass_str[0] != '\0'))
            if(OC_UNKNOWN == (file->fapl_cache.default_object_class = (daos_oclass_id_t)daos_oclass_name2id(oclass_str)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unknown object class");
    } /* end if */

done:
    D_FUNC_LEAVE;
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
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID");

        if(!uuid_compare(cur_obj->item.file->uuid, count_udata->file_id))
            count_udata->obj_count++;
    } /* end if */

done:
    D_FUNC_LEAVE;
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
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID");

        if(id_udata->obj_count < id_udata->max_objs) {
            if(!uuid_compare(cur_obj->item.file->uuid, id_udata->file_id))
                id_udata->oid_list[id_udata->obj_count++] = id;
        }
        else
            ret_value = H5_ITER_STOP;
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_obj_ids_callback() */

