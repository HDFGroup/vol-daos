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
 *          library.  Asynchronous request routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */


static int H5_daos_op_pool_start_task(tse_task_t *task);
static int H5_daos_op_pool_close_task(tse_task_t *task);
static int H5_daos_op_pool_end_task(tse_task_t *task);



/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_wait
 *
 * Purpose:     Waits until the provided request is complete or the wait
 *              times out.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              April, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_wait(void *_req, uint64_t timeout, H5ES_status_t *status)
{
    H5_daos_req_t *req = (H5_daos_req_t *)_req;
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(!req)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "request object is NULL");

    /* Wait until request finished */
    if(H5_daos_progress(req, timeout) < 0)
        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't progress scheduler");

    /* Set status if requested */
    if(status) {
        if(req->status > -H5_DAOS_INCOMPLETE)
            *status = H5ES_STATUS_SUCCEED;
        else if(req->status >= -H5_DAOS_SHORT_CIRCUIT)
            *status = H5ES_STATUS_IN_PROGRESS;
        else if(req->status == -H5_DAOS_CANCELED)
            *status = H5ES_STATUS_CANCELED;
        else
            *status = H5ES_STATUS_FAIL;
    } /* end if */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_req_wait() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_notify
 *
 * Purpose:     Registers a notify callback for the provided request.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              May, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_notify(void *_req, H5VL_request_notify_t cb, void *ctx)
{
    H5_daos_req_t *req = (H5_daos_req_t *)_req;
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(!req)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "request object is NULL");

    /* Register callback */
    req->notify_cb = cb;
    req->notify_ctx = ctx;

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_req_notify() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_cancel
 *
 * Purpose:     Cancels the provided request.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              May, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_cancel(void *_req)
{
    H5_daos_req_t *req = (H5_daos_req_t *)_req;
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(!req)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "request object is NULL");

    /* Cancel operation */
    req->status = H5_DAOS_CANCELED;

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_req_cancel() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_free
 *
 * Purpose:     Decrement the reference count on the request and free it
 *              if the ref count drops to 0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_free(void *req)
{
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(!req)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "request object is NULL");

    if(H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_DAOS_ASYNC, H5E_CLOSEERROR, FAIL, "can't free request");

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_req_free() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_create
 *
 * Purpose:     Create a request.  If the operation will never need to use
 *              the dxpl_id it is OK to pass H5I_INVALID_HID to avoid
 *              H5Pcopy(), even if a DXPL is available.
 *
 * Return:      Valid pointer on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5_daos_req_t *
H5_daos_req_create(H5_daos_file_t *file, hid_t dxpl_id)
{
    H5_daos_req_t *ret_value = NULL;

    assert(file);

    if(NULL == (ret_value = (H5_daos_req_t *)DV_malloc(sizeof(H5_daos_req_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for request");
    ret_value->th = DAOS_TX_NONE;
    ret_value->th_open = FALSE;
    ret_value->file = file;
    if(dxpl_id == H5I_INVALID_HID || dxpl_id == H5P_DATASET_XFER_DEFAULT)
        ret_value->dxpl_id = dxpl_id;
    else
        if((ret_value->dxpl_id = H5Pcopy(dxpl_id)) < 0) {
            DV_free(ret_value);
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTCOPY, NULL, "can't copy data transfer property list");
        } /* end if */
    ret_value->finalize_task = NULL;
    ret_value->dep_task = NULL;
    ret_value->notify_cb = NULL;
    ret_value->file->item.rc++;
    ret_value->rc = 1;
    ret_value->status = -H5_DAOS_INCOMPLETE;
    ret_value->failed_task = "default (probably operation setup)";

done:
    D_FUNC_LEAVE;
} /* end H5_daos_req_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_free_int
 *
 * Purpose:     Internal version of H5_daos_req_free().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_free_int(H5_daos_req_t *req)
{
    herr_t ret_value = SUCCEED;

    assert(req);

    if(--req->rc == 0) {
        if(req->dxpl_id >= 0 && req->dxpl_id != H5P_DATASET_XFER_DEFAULT)
            if(H5Pclose(req->dxpl_id) < 0) {
                /* If H5Pclose failed we must update the request status, since
                 * the calling function can't access the request after calling
                 * this function.  Note this task name isn't very specific.
                 * This should be ok here since this plist isn't visible to the
                 * user and this failure shouldn't be caused by user errors,
                 * only errors in HDF5 and this connector. */
                if(req->status >= -H5_DAOS_INCOMPLETE) {
                    req->status = -H5_DAOS_H5_CLOSE_ERROR;
                    req->failed_task = "request free";
                } /* end if */
                D_DONE_ERROR(H5E_DAOS_ASYNC, H5E_CLOSEERROR, FAIL, "can't close data transfer property list");
            } /* end if */
        H5_daos_file_decref(req->file);
        DV_free(req);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_req_free_int() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_op_pool_finish
 *
 * Purpose:     Causes an operation pool to close itself, releasing all
 *              references it has to itself and completing all tasks (once
 *              operations in the pool are complete).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_op_pool_finish(H5_daos_op_pool_t *op_pool)
{
    int ret;
    int ret_value = 0;

    assert(op_pool);

    /* If the pool is empty we must create a dependency for the end task on the
     * start task so the end task does nor execute too soon, then schedule the
     * start task */
    if(op_pool->type == H5_DAOS_OP_TYPE_EMPTY) {
        assert(op_pool->end_task);
        assert(op_pool->start_task);
        assert(!op_pool->closed);

        /* Create dependency for pool end task on pool start task */
        if((ret = tse_task_register_deps(op_pool->end_task, 1, &op_pool->start_task)) < 0)
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dependencies for end task for operation pool: %s", H5_daos_err_to_string(ret)); 

        /* Schedule pool start task */
        if( 0 != (ret = tse_task_schedule(op_pool->start_task, false)))
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));
    } /* end if */

    /* Check if the pool is already closed */
    if(!op_pool->closed) {
        /* Mark pool as closed */
        op_pool->closed = TRUE;

        /* Schedule end task */
        if(0 != (ret = tse_task_schedule(op_pool->end_task, false)))
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, ret, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_op_pool_finish() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_op_pool_free
 *
 * Purpose:     Decrement ref count on op_pool, freeing it if it drops to
 *              0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              Novemnber, 2020
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_op_pool_free(H5_daos_op_pool_t *op_pool)
{
    assert(op_pool);

    if(--op_pool->rc == 0) {
        assert(op_pool->closed);
        DV_free(op_pool);
    } /* end if */

    return;
} /* end H5_daos_op_pool_free() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_op_pool_start_task
 *
 * Purpose:     Task to begin an operation pool.  Only clears the start
 *              task from the pool struct and completes itself.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_op_pool_start_task(tse_task_t *task)
{
    H5_daos_op_pool_t *op_pool = NULL;
    int ret_value = 0;

    /* Get op pool */
    if(NULL == (op_pool = (H5_daos_op_pool_t *)tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for op pool start task");

    assert(task == op_pool->start_task);
//printf("%s: %p\n", __func__, task); fflush(stdout);
    /* Clear start task so later tasks in this pool don't depend on this
     * (completed) task */
    op_pool->start_task = NULL;

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_op_pool_start_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_op_pool_close_task
 *
 * Purpose:     Task to close an operation pool to new operations.
 *              Schedules end_task, marks the pool closed, and releases
 *              this task's reference to the pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_op_pool_close_task(tse_task_t *task)
{
    H5_daos_op_pool_t *op_pool = NULL;
    int ret;
    int ret_value = 0;

    /* Get op pool */
    if(NULL == (op_pool = (H5_daos_op_pool_t *)tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for op pool close task");

    /* Check if the pool is already closed */
    if(!op_pool->closed) {
        /* Mark pool as closed */
        op_pool->closed = TRUE;

        /* Schedule end task */
        if(0 != (ret = tse_task_schedule(op_pool->end_task, false)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));
    } /* end if */

    /* Release our reference to op_pool */
    H5_daos_op_pool_free(op_pool);
    op_pool = NULL;

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_op_pool_close_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_op_pool_end_task
 *
 * Purpose:     Task to finalize an operation pool.  Simply completes
 *              dep_task if present then releases its reference to the
 *              pool.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_op_pool_end_task(tse_task_t *task)
{
    H5_daos_op_pool_t *op_pool = NULL;
    int ret_value = 0;

    /* Get op pool */
    if(NULL == (op_pool = (H5_daos_op_pool_t *)tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for op pool end task");
//printf("%s: %p\n", __func__, task); fflush(stdout);
    assert(task == op_pool->end_task);
    assert(!op_pool->start_task);

    /* Complete dep task if present */
    if(op_pool->dep_task) {
        tse_task_complete(op_pool->dep_task, 0);
        op_pool->dep_task = NULL;
    } /* end if */

    /* Set end_task to NULL to mark this task complete */
    op_pool->end_task = NULL;

    /* Release our reference to op_pool */
    H5_daos_op_pool_free(op_pool);
    op_pool = NULL;

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_op_pool_end_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_enqueue
 *
 * Purpose:     Adds a request to an object, file, or global operation
 *              pool.  If collective is true it is also added to the
 *              collective operation queue.  If dep_req is provided that
 *              is added as a dependency.  Also schedules first_task.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_enqueue(H5_daos_req_t *req, tse_task_t *first_task,
    H5_daos_item_t *item, H5_daos_op_pool_type_t op_type,
    H5_daos_op_pool_scope_t scope, hbool_t collective, H5_daos_req_t *dep_req)
{
    H5_daos_op_pool_t **parent_cur_op_pool[4];
    H5_daos_op_pool_t *tmp_pool;
    H5_daos_op_pool_t *tmp_new_pool_alloc = NULL;
    H5_daos_op_pool_t *tmp_new_pool_alloc_2 = NULL;
    hbool_t create_new_pool;
    hbool_t init_pool;
    int nlevels;
    int i;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(req);
    assert(op_type >= H5_DAOS_OP_TYPE_READ && op_type <= H5_DAOS_OP_TYPE_NOPOOL);
    assert(scope >= H5_DAOS_OP_SCOPE_ATTR &&  scope <= H5_DAOS_OP_SCOPE_GLOB);

    /* If there's no first task there's nothing to do */
    if(!first_task)
        D_GOTO_DONE(SUCCEED);

    /* Check if we don't need to add to a pool */
    if(op_type != H5_DAOS_OP_TYPE_NOPOOL
            && (item || scope == H5_DAOS_OP_SCOPE_GLOB)) {

        /* Assign parent_cur_op_pool and parent_static_op_pool */
        switch(scope) {
            case H5_DAOS_OP_SCOPE_ATTR:
                assert(item);
                assert(item->file == req->file);
                assert(item->type == H5I_ATTR);
                parent_cur_op_pool[0] = &item->cur_op_pool;
                parent_cur_op_pool[1] = &((H5_daos_attr_t *)item)->parent->item.cur_op_pool;
                parent_cur_op_pool[2] = &item->file->item.cur_op_pool;
                parent_cur_op_pool[3] = &H5_daos_glob_cur_op_pool_g;
                nlevels = 4;
                break;

            case H5_DAOS_OP_SCOPE_OBJ:
                assert(item);
                assert(item->file == req->file);
                parent_cur_op_pool[0] = item->type == H5I_FILE
                        ? &((H5_daos_file_t *)item)->root_grp->obj.item.cur_op_pool
                        : &item->cur_op_pool;
                parent_cur_op_pool[1] = &item->file->item.cur_op_pool;
                parent_cur_op_pool[2] = &H5_daos_glob_cur_op_pool_g;
                nlevels = 3;
                break;

            case H5_DAOS_OP_SCOPE_FILE:
                assert(item);
                assert(item->file == req->file);
                parent_cur_op_pool[0] = &item->file->item.cur_op_pool;
                parent_cur_op_pool[1] = &H5_daos_glob_cur_op_pool_g;
                nlevels = 2;
                break;

            case H5_DAOS_OP_SCOPE_GLOB:
                parent_cur_op_pool[0] = &H5_daos_glob_cur_op_pool_g;
                nlevels = 1;
                break;

            default:
                assert(0 && "Unknown scope");
        } /* end switch */

        assert(!*parent_cur_op_pool[0]
                || (*parent_cur_op_pool[0])->type != H5_DAOS_OP_TYPE_CLOSE);

        /* Determine if we need to allocate and/or initialize a new pool */
        if(!*parent_cur_op_pool[0]) {
            /* No pool present at this level, must create a new one */
            create_new_pool = TRUE;
            init_pool = TRUE;
        } /* end if */
        else if((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_EMPTY) {
            /* Empty pool present, must initialize */
            assert((*parent_cur_op_pool[0])->init_task);
            assert((*parent_cur_op_pool[0])->start_task);
            assert((*parent_cur_op_pool[0])->end_task);

            /* Take over empty pool */
            create_new_pool = FALSE;
            init_pool = TRUE;

            /* Assign tmp_pool pointer */
            tmp_pool = *parent_cur_op_pool[0];
        } /* end if */
        else if(!(*parent_cur_op_pool[0])->closed
                && ((op_type == H5_DAOS_OP_TYPE_READ
                    && ((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_WRITE
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ_ORDERED))
                || (op_type == H5_DAOS_OP_TYPE_WRITE
                    && ((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_WRITE))
                || (op_type == H5_DAOS_OP_TYPE_READ_ORDERED
                    && ((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ_ORDERED)))) {
            /* Op type is compatible with current pool type and current pool is
             * not closd.  Can add to current pool. */
            assert(!(*parent_cur_op_pool[0])->init_task);
            assert((*parent_cur_op_pool[0])->end_task);

            /* Start off using existing pool */
            create_new_pool = FALSE;
            init_pool = FALSE;
            tmp_pool = *parent_cur_op_pool[0];

            /* Upgrade pool type if appropriate */
            if(!init_pool && op_type > (*parent_cur_op_pool[0])->type)
                (*parent_cur_op_pool[0])->type = op_type;
        } /* end if */
        else {
            /* Cannot combine with existing pool, create new one */
            create_new_pool = TRUE;
            init_pool = TRUE;
        } /* end else */

        /* Create new pool if appropriate */
        if(create_new_pool) {
            /* Allocate pool struct */
            if(NULL == (tmp_new_pool_alloc = (H5_daos_op_pool_t *)DV_calloc(sizeof(H5_daos_op_pool_t))))
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTALLOC, FAIL, "can't allocate operation pool struct");
            tmp_pool = tmp_new_pool_alloc;

            /* Create start task */
            if(0 != (ret = tse_task_create(H5_daos_op_pool_start_task, &H5_daos_glob_sched_g, tmp_pool, &tmp_pool->start_task)))
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create start task for operation pool: %s", H5_daos_err_to_string(ret));

            /* Create end task and give it a reference to the pool */
            if(0 != (ret = tse_task_create(H5_daos_op_pool_end_task, &H5_daos_glob_sched_g, tmp_pool, &tmp_pool->end_task)))
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create end task for operation pool: %s", H5_daos_err_to_string(ret));
            tmp_pool->rc++;

            /* Handle previous pool */
            if(*parent_cur_op_pool[0]) {
                assert((*parent_cur_op_pool[0])->type != H5_DAOS_OP_TYPE_EMPTY);

                /* Only need to create dependencies if the previous pool hasn't
                 * already completed */
                if((*parent_cur_op_pool[0])->end_task) {
                    /* Create dep task for previous pool if necessary.  This will be
                     * completed by the end task.  We do this to prevent tse from
                     * propagating errors between pools. */
                    if(!(*parent_cur_op_pool[0])->dep_task) {
                        if(0 != (ret = tse_task_create(NULL, &H5_daos_glob_sched_g, NULL, &(*parent_cur_op_pool[0])->dep_task)))
                            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dep task for operation pool: %s", H5_daos_err_to_string(ret));

                        if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[0])->dep_task, false)))
                            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule final dependency task for operation pool: %s", H5_daos_err_to_string(ret));
                    } /* end if */

                    /* Create dependency on previous pool dep task */
                    if((ret = tse_task_register_deps(tmp_pool->start_task, 1, &(*parent_cur_op_pool[0])->dep_task)) < 0)
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dependencies for start task for operation pool: %s", H5_daos_err_to_string(ret));

                    /* Close previous pool to new operations if it isn't already */
                    if(!(*parent_cur_op_pool[0])->closed) {
                        (*parent_cur_op_pool[0])->closed = TRUE;

                        /* Schedule previous pool end task */
                        if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[0])->end_task, false)))
                            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));
                    } /* end if */
                } /* end if */
            } /* end if */
        } /* end if */

        /* Initialize pool if appropriate */
        if(init_pool) {
            assert(tmp_pool->start_task);
            assert(tmp_pool->end_task);

            /* Assign pool type */
            tmp_pool->type = op_type;

            /* If any higher level pools are non-empty, close them and create a new
             * empty pool */
            /* Adjust higher level pools if necessary */
            for(i = 1; i < nlevels; i++) {
                tse_task_t *close_task;

                /* Check if we must create a new higher level pool */
                if(!*parent_cur_op_pool[i] || (*parent_cur_op_pool[i])->type != H5_DAOS_OP_TYPE_EMPTY) {
                    /* Allocate pool struct */
                    if(NULL == (tmp_new_pool_alloc_2 = (H5_daos_op_pool_t *)DV_calloc(sizeof(H5_daos_op_pool_t))))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTALLOC, FAIL, "can't allocate operation pool struct");

                    /* Initialize ref count */
                    tmp_new_pool_alloc_2->rc = 1;

                    /* Set op_type */
                    tmp_new_pool_alloc_2->type = H5_DAOS_OP_TYPE_EMPTY;

                    /* Create start task */
                    if(0 != (ret = tse_task_create(H5_daos_op_pool_start_task, &H5_daos_glob_sched_g, tmp_new_pool_alloc_2, &tmp_new_pool_alloc_2->start_task)))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create start task for operation pool: %s", H5_daos_err_to_string(ret));

                    /* Create end task and give it a reference to the pool */
                    if(0 != (ret = tse_task_create(H5_daos_op_pool_end_task, &H5_daos_glob_sched_g, tmp_new_pool_alloc_2, &tmp_new_pool_alloc_2->end_task)))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create end task for operation pool: %s", H5_daos_err_to_string(ret));
                    tmp_new_pool_alloc_2->rc++;

                    /* If a previous higher level pool exists (and is
                     * non-empty), create a dependency on it for tmp_pool */
                    if(*parent_cur_op_pool[i]) {
                        /* Create the dependency on the higher level pool's dep
                         * task.  If the higher level pool is empty, the previous
                         * dependency will have been handled at a lower level than
                         * the current pool (by this line of code). */

                        /* Only need to create dependencies if the higher level
                         * pool hasn't already completed */
                        if((*parent_cur_op_pool[i])->end_task) {
                            /* Create dep task for higher level pool if necessary.  This will be
                             * completed by the end task.  We do this to prevent tse from
                             * propagating errors between pools. */
                            if(!(*parent_cur_op_pool[i])->dep_task) {
                                if(0 != (ret = tse_task_create(NULL, &H5_daos_glob_sched_g, NULL, &(*parent_cur_op_pool[i])->dep_task)))
                                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dep task for operation pool: %s", H5_daos_err_to_string(ret));

                                if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[i])->dep_task, false)))
                                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule final dependency task for operation pool: %s", H5_daos_err_to_string(ret));
                            } /* end if */

                            /* Create dependency */
                            if((ret = tse_task_register_deps(tmp_pool->start_task, 1, &(*parent_cur_op_pool[i])->dep_task)) < 0)
                                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));

                            /* Close higher level pool to new operations if it isn't
                             * already */
                            if(!(*parent_cur_op_pool[i])->closed) {
                                (*parent_cur_op_pool[i])->closed = TRUE;

                                /* Schedule previous higher level pool end task */
                                if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[i])->end_task, false)))
                                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));
                            } /* end if */
                        } /* end if */
                    } /* end if */

                    /* Set new pool in parent object, and transfer parent
                     * object's reference to the new pool */
                    if(*parent_cur_op_pool[i])
                        H5_daos_op_pool_free(*parent_cur_op_pool[i]);
                    *parent_cur_op_pool[i] = tmp_new_pool_alloc_2;
                    tmp_new_pool_alloc_2->rc++;
                    tmp_new_pool_alloc_2 = NULL;
                } /* end if */

                /* The higher level pool is now empty, register this pool's dep
                 * task as a dependency for the higher level pools' start tasks
                 */
                /* Create dep task for this pool if necessary.  This will be
                 * completed by the end task.  We do this to prevent tse from
                 * propagating errors between pools. */
                if(!tmp_pool->dep_task) {
                    if(0 != (ret = tse_task_create(NULL, &H5_daos_glob_sched_g, NULL, &tmp_pool->dep_task)))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dep task for operation pool: %s", H5_daos_err_to_string(ret));

                    if(0 != (ret = tse_task_schedule(tmp_pool->dep_task, false)))
                        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule final dependency task for operation pool: %s", H5_daos_err_to_string(ret));
                } /* end if */

                /* Create dependency */
                if((ret = tse_task_register_deps((*parent_cur_op_pool[i])->start_task, 1, &tmp_pool->dep_task)) < 0)
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));
//printf("hlp dependency on new pool %p -> %p\n", tmp_task, (*parent_cur_op_pool[i])->start_task); fflush(stdout);
                /* Create init task for higher level pool if necessary.  This
                 * will be completed when the (currently empty) higher level
                 * pool is initialized. */
                if(!(*parent_cur_op_pool[i])->init_task) {
                    if(0 != (ret = tse_task_create(NULL, &H5_daos_glob_sched_g, NULL, &(*parent_cur_op_pool[i])->init_task)))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create init task for operation pool: %s", H5_daos_err_to_string(ret));

                    if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[i])->init_task, false)))
                        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule initialization notification task for operation pool: %s", H5_daos_err_to_string(ret));
                } /* end if */

                /* Create close task for tmp_pool.  When the empty higher level
                 * pool is initialized, we must close tmp_pool. */
                if(0 != (ret = tse_task_create(H5_daos_op_pool_close_task, &H5_daos_glob_sched_g, tmp_pool, &close_task)))
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create close task for operation pool: %s", H5_daos_err_to_string(ret));

                /* Create dependency on higher level pool init task */
                if((ret = tse_task_register_deps(close_task, 1, &(*parent_cur_op_pool[i])->init_task)) < 0)
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));

                /* Schedule close task and give it a reference to tmp_pool */
                if(0 != (ret = tse_task_schedule(close_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule close task for operation pool: %s", H5_daos_err_to_string(ret));
                tmp_pool->rc++;
            } /* end for */

            /* Initialization is complete, complete init task */
            if(tmp_pool->init_task) {
                tse_task_complete(tmp_pool->init_task, 0);
                tmp_pool->init_task = NULL;
            } /* end if */
        } /* end if */

        /* Add request to the pool */
        assert(tmp_pool);
        if(tmp_pool->start_task) {
            /* Register dependency for first task */
            if((ret = tse_task_register_deps(first_task, 1, &tmp_pool->start_task)) < 0)
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));

            /* Schedule pool start task if appropriate.  If we initialized the pool
             * then this is the first task and we must schedule the start task,
             * otherwise this is not the first task. */
            if(init_pool)
                if(0 != (ret = tse_task_schedule(tmp_pool->start_task, false)))
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't schedule task to start operation pool: %s", H5_daos_err_to_string(ret));
        } /* end if */

        /* Register dependency for pool end task on this request */
        assert(tmp_pool->end_task);
        if((ret = tse_task_register_deps(tmp_pool->end_task, 1, &req->finalize_task)) < 0)
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));

        /* If this is a close operation, close the pool and schedule the end
         * task */
        if(op_type == H5_DAOS_OP_TYPE_CLOSE) {
            assert(!tmp_pool->closed);

            tmp_pool->closed = TRUE;

            if(0 != (ret = tse_task_schedule(tmp_pool->end_task, false)))
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));
        } /* end if */

        /* Set new pool as current pool if appropriate, and transfer item's
         * reference to the new pool */
        if(create_new_pool) {
            if(*parent_cur_op_pool[0])
                H5_daos_op_pool_free(*parent_cur_op_pool[0]);
            *parent_cur_op_pool[0] = tmp_pool;
            tmp_pool->rc++;
            tmp_pool = NULL;
            tmp_new_pool_alloc = NULL;
        } /* end if */
    } /* end if */

    /* Add dependency on H5_daos_collective_req_tail and update it if this is a
     * collective operation.  This cannot cause a deadlock since this schedules
     * requests in order, and requests can never be scheduled out of order by
     * the main pool scheme above. */
    if(collective) {
        if(H5_daos_collective_req_tail) {
            /* Create dep task for previous collective request if necessary.
             * This will be completed by the request finalize task.  We do this
             * to prevent tse from propagating errors between requests. */
            if(!H5_daos_collective_req_tail->dep_task) {
                if(0 != (ret = tse_task_create(NULL, &H5_daos_glob_sched_g, NULL, &H5_daos_collective_req_tail->dep_task)))
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dep task for request: %s", H5_daos_err_to_string(ret));

                if(0 != (ret = tse_task_schedule(H5_daos_collective_req_tail->dep_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule final dependency task for request: %s", H5_daos_err_to_string(ret));
            } /* end if */

            /* Create dependency */
           if((ret = tse_task_register_deps(first_task, 1, &H5_daos_collective_req_tail->dep_task)) < 0)
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));
        } /* end if */

        H5_daos_collective_req_tail = req;
    } /* end if */

    /* Add dependency on dep_req if necessary */
    if(dep_req && (dep_req->status == H5_DAOS_INCOMPLETE
            || dep_req->status == H5_DAOS_SHORT_CIRCUIT)) {
        /* Create dep_task for request if necessary.  This will be completed by
         * the request finalize task.  We do this to prevent tse from
         * propagating errors between collective requests. */
        if(!dep_req->dep_task) {
            if(0 != (ret = tse_task_create(NULL, &H5_daos_glob_sched_g, NULL, &dep_req->dep_task)))
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dep task for operation pool: %s", H5_daos_err_to_string(ret));

            if(0 != (ret = tse_task_schedule(dep_req->dep_task, false)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule final dependency task for operation pool: %s", H5_daos_err_to_string(ret));
        } /* end if */

        /* Register dependency */
        if((ret = tse_task_register_deps(first_task, 1, &dep_req->dep_task)) < 0)
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));
    } /* end if */

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule first task for operation: %s", H5_daos_err_to_string(ret));

    /* Cleanup on failure */
    if(ret_value < 0) {
        tmp_new_pool_alloc = DV_free(tmp_new_pool_alloc);
        tmp_new_pool_alloc_2 = DV_free(tmp_new_pool_alloc_2);
    } /* end if */

    /* Make sure we cleaned up */
    assert(tmp_new_pool_alloc == NULL);
    assert(tmp_new_pool_alloc_2 == NULL);

    D_FUNC_LEAVE;
} /* end H5_daos_req_enqueue() */

