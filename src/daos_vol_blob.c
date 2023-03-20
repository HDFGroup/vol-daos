/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library. Blob routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

static int H5_daos_blob_io_comp_cb(tse_task_t *task, void *args);

/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_io_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch or
 *              daos_obj_update for blob read/write operations. Just passes
 *              back the task's result status since the blob callbacks do
 *              not currently use a request object.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_blob_io_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    int *udata     = NULL;
    int  ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for blob I/O task");

    /* Simply set task result in udata */
    *udata = task->dt_result;

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    D_FUNC_LEAVE;
}

/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_blob_put(void *_file, const void *buf, size_t size, void *blob_id, void H5VL_DAOS_UNUSED *_ctx)
{
    uuid_t          blob_uuid; /* Blob ID */
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
    daos_obj_rw_t  *update_args;
    tse_task_t     *update_task = NULL;
    tse_task_t     *first_task  = NULL;
    tse_task_t     *dep_task    = NULL;
    daos_key_t      dkey;
    daos_iod_t      iod;
    daos_sg_list_t  sgl;
    daos_iov_t      sg_iov;
    int             task_result = 0;
    herr_t          ret_value   = SUCCEED; /* Return value */

    assert(H5_DAOS_BLOB_ID_SIZE == sizeof(blob_uuid));

    H5_daos_inc_api_cnt();

    /* Check parameters */
    if (!buf && size > 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer is NULL but size > 0");
    if (!blob_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "blob id is NULL");
    if (!file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Generate blob ID as a UUID */
    uuid_generate(blob_uuid);

    /* Copy uuid to blob_id output buffer */
    (void)memcpy(blob_id, &blob_uuid, H5_DAOS_BLOB_ID_SIZE);

    /* Only write if size > 0 */
    if (size > 0) {
        /* Set up dkey */
        daos_iov_set(&dkey, blob_id, H5_DAOS_BLOB_ID_SIZE);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_const_iov_set((d_const_iov_t *)&iod.iod_name, H5_daos_blob_key_g, H5_daos_blob_key_size_g);
        iod.iod_nr   = 1u;
        iod.iod_size = (uint64_t)size;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_const_iov_set((d_const_iov_t *)&sg_iov, buf, (daos_size_t)size);
        sgl.sg_nr     = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs   = &sg_iov;

        /* Create task for blob write */
        assert(!dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, 0, NULL, NULL, H5_daos_blob_io_comp_cb,
                                     &task_result, &update_task) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to write blob");

        /* Set update task arguments */
        if (NULL == (update_args = daos_task_get_args(update_task)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get arguments for blob write task");
        memset(update_args, 0, sizeof(*update_args));
        update_args->oh   = file->glob_md_oh;
        update_args->th   = DAOS_TX_NONE;
        update_args->dkey = &dkey;
        update_args->nr   = 1u;
        update_args->iods = &iod;
        update_args->sgls = &sgl;

        assert(!first_task);
        first_task = update_task;
        dep_task   = update_task;
    } /* end if */

done:
    if (first_task) {
        assert(dep_task);
        if (H5_daos_task_wait(&(first_task), &(dep_task)) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler");
        if (0 != task_result)
            D_DONE_ERROR(H5E_VOL, H5E_WRITEERROR, FAIL, "blob write failed: %s",
                         H5_daos_err_to_string(task_result));
    }
    else if (update_task) {
        assert(ret_value < 0);
        tse_task_complete(update_task, -H5_DAOS_SETUP_ERROR);
    }

    D_FUNC_LEAVE_API;
} /* end H5_daos_blob_put() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_blob_get(void *_file, const void *blob_id, void *buf, size_t size, void H5VL_DAOS_UNUSED *_ctx)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
    daos_obj_rw_t  *fetch_args;
    tse_task_t     *fetch_task = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    daos_key_t      dkey;
    daos_iod_t      iod;
    daos_sg_list_t  sgl;
    daos_iov_t      sg_iov;
    int             task_result = 0;
    herr_t          ret_value   = SUCCEED; /* Return value */

    H5_daos_inc_api_cnt();

    /* Check parameters */
    if (!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer is NULL");
    if (!blob_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "blob id is NULL");
    if (!file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Only read if size > 0 */
    if (size > 0) {
        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&dkey, blob_id, H5_DAOS_BLOB_ID_SIZE);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_const_iov_set((d_const_iov_t *)&iod.iod_name, H5_daos_blob_key_g, H5_daos_blob_key_size_g);
        iod.iod_nr   = 1u;
        iod.iod_size = (uint64_t)size;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, (void *)buf, (daos_size_t)size);
        sgl.sg_nr     = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs   = &sg_iov;

        /* Create task for blob read */
        assert(!dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, NULL, H5_daos_blob_io_comp_cb, &task_result,
                                     &fetch_task) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to read blob");

        /* Set fetch task arguments */
        if (NULL == (fetch_args = daos_task_get_args(fetch_task)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't get arguments for blob read task");
        memset(fetch_args, 0, sizeof(*fetch_args));
        fetch_args->oh   = file->glob_md_oh;
        fetch_args->th   = DAOS_TX_NONE;
        fetch_args->dkey = &dkey;
        fetch_args->nr   = 1u;
        fetch_args->iods = &iod;
        fetch_args->sgls = &sgl;

        assert(!first_task);
        first_task = fetch_task;
        dep_task   = fetch_task;
    } /* end if */

done:
    if (first_task) {
        assert(dep_task);
        if (H5_daos_task_wait(&(first_task), &(dep_task)) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't progress scheduler");
        if (0 != task_result)
            D_DONE_ERROR(H5E_VOL, H5E_READERROR, FAIL, "blob read failed: %s",
                         H5_daos_err_to_string(task_result));
    }
    else if (fetch_task) {
        assert(ret_value < 0);
        tse_task_complete(fetch_task, -H5_DAOS_SETUP_ERROR);
    }

    D_FUNC_LEAVE_API;
} /* end H5_daos_blob_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_blob_specific(void *_file, void *blob_id, H5VL_blob_specific_args_t *specific_args)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
    int             ret;
    herr_t          ret_value = SUCCEED; /* Return value */

    H5_daos_inc_api_cnt();

    /* Check parameters */
    if (!blob_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "blob id is NULL");
    if (!specific_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");
    if (!file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (specific_args->op_type) {
        case H5VL_BLOB_ISNULL: {
            hbool_t *isnull = specific_args->args.is_null.isnull;
            uint8_t  nul_buf[H5_DAOS_BLOB_ID_SIZE];

            /* Check parameters */
            if (!isnull)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "isnull output buffer is NULL");

            /* Initialize comparison buffer */
            (void)memset(nul_buf, 0, sizeof(nul_buf));

            /* Check if blob ID matches NULL ID */
            *isnull = (memcmp(blob_id, nul_buf, H5_DAOS_BLOB_ID_SIZE) == 0);

            break;
        }

        case H5VL_BLOB_SETNULL: {
            /* Encode NULL blob ID */
            (void)memset((void *)blob_id, 0, H5_DAOS_BLOB_ID_SIZE);

            break;
        }

        case H5VL_BLOB_DELETE: {
            daos_key_t dkey;

            /* Set up dkey */
            daos_iov_set(&dkey, blob_id, H5_DAOS_BLOB_ID_SIZE);

            /* Punch the blob's dkey, along with all of its akeys.  Due to
             * the (practical) guarantee of uniqueness of UUIDs, we can
             * assume we won't delete any unintended akeys. */
            if (0 != (ret = daos_obj_punch_dkeys(file->glob_md_oh, DAOS_TX_NONE, 0 /*flags*/, 1, &dkey,
                                                 NULL /*event*/)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTREMOVE, FAIL, "failed to punch blob dkey: %s",
                             H5_daos_err_to_string(ret));

            break;
        }

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid unsupported blob specific operation");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_blob_specific() */
