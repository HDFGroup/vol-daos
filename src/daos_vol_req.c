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
 * Function:    H5_daos_op_pool_end_task
 *
 * Purpose:     Task to finalize an operation pool.  Either frees the pool
 *              or empties it, then releases any reference counts.
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
    H5_daos_item_t *item = NULL;
    int ret_value = 0;

    /* Get op pool */
    if(NULL == (op_pool = (H5_daos_op_pool_t *)tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for op pool start task");
//printf("%s: %p\n", __func__, task); fflush(stdout);
    assert(task == op_pool->end_task);
    assert(!op_pool->start_task);
    assert(op_pool->parent_cur_op_pool);
    assert(*op_pool->parent_cur_op_pool);

    /* Save item (to release later) */
    item = op_pool->item;

    /* If this is a close task, free the op pool and clear the item's
     * parent_cur_op_pool pointer */
    if(op_pool->type == H5_DAOS_OP_TYPE_CLOSE) {
        assert(op_pool == *op_pool->parent_cur_op_pool);
        *op_pool->parent_cur_op_pool = NULL;
        DV_free(op_pool);
    } /* end if */
    /* Check if this is the current op pool, if not, free it */
    else if(op_pool != *op_pool->parent_cur_op_pool)
        DV_free(op_pool);

done:
    /* Complete this task */
    tse_task_complete(task, ret_value);

    /* Release our reference to the object/file */
    if(item) {
        if(item->type == H5I_FILE)
            H5_daos_file_decref((H5_daos_file_t *)item);
        else
            if(H5_daos_object_close(item) < 0)
                D_DONE_ERROR(H5E_DAOS_ASYNC, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close object");
    } /* end if */

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
    hbool_t is_file_close = FALSE;
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
        /* Check for file close op - in this case a few things need to happen
         * differently since file close finalizes the file's scheduler.  Namely,
         * the pool needs to be created in the global scheduler. */
        /*if(op_type == H5_DAOS_OP_TYPE_CLOSE && scope == H5_DAOS_OP_SCOPE_FILE)
            is_file_close = TRUE;*/

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
                assert(item == &item->file->item);
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
        else if((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_EMPTY
                && !is_file_close) {
            /* Empty pool present, must initialize */
            assert((*parent_cur_op_pool[0])->start_task);

            /* Take over empty pool */
            create_new_pool = FALSE;
            init_pool = TRUE;

            /* Assign tmp_pool pointer */
            tmp_pool = *parent_cur_op_pool[0];
        } /* end if */
        else if((op_type == H5_DAOS_OP_TYPE_READ
                    && ((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_WRITE
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ_ORDERED))
                || (op_type == H5_DAOS_OP_TYPE_WRITE
                    && ((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_WRITE))
                || (op_type == H5_DAOS_OP_TYPE_READ_ORDERED
                    && ((*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ
                    || (*parent_cur_op_pool[0])->type == H5_DAOS_OP_TYPE_READ_ORDERED))) {
            /* Can potentially add to existing pool, check if the higher level pools
             * have the same operation generation */

            /* Start off using existing pool */
            create_new_pool = FALSE;
            init_pool = FALSE;
            tmp_pool = *parent_cur_op_pool[0];

            /* Check if we must create a new pool */
            for(i = 1; i < nlevels; i++)
                if(*parent_cur_op_pool[i] && (*parent_cur_op_pool[0])->op_gens[i]
                        != (*parent_cur_op_pool[i])->op_gens[0]) {
                    create_new_pool = TRUE;
                    init_pool = TRUE;
                    tmp_pool = NULL;
                    break;
                } /* end if */

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

            /* Create end task */
            if(0 != (ret = tse_task_create(H5_daos_op_pool_end_task, &H5_daos_glob_sched_g, tmp_pool, &tmp_pool->end_task)))
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create end task for operation pool: %s", H5_daos_err_to_string(ret));

            /* Handle previous pool */
            if(*parent_cur_op_pool[0]) {
                assert((*parent_cur_op_pool[0])->type != H5_DAOS_OP_TYPE_EMPTY
                        || is_file_close);
                assert((*parent_cur_op_pool[0])->end_task);

                /* Create dependency on current pool end task */
                if((ret = tse_task_register_deps(tmp_pool->start_task, 1, &(*parent_cur_op_pool[0])->end_task)) < 0)
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create dependencies for start task for operation pool: %s", H5_daos_err_to_string(ret));
//printf("new pool horiz dependency %p -> %p\n", tmp_task, tmp_pool->start_task); fflush(stdout);
                /* Schedule previous pool end task */
                if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[0])->end_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));

                /* Initialize op_gen.  Note that if there was no previous pool
                 * op_gens will be set to 0 since tmp_pool was calloc'ed. */
                tmp_pool->op_gens[0] = (*parent_cur_op_pool[0])->op_gens[0];
            } /* end if */

            /* Assign parent_cur_op_pool pointer */
            tmp_pool->parent_cur_op_pool = parent_cur_op_pool[0];
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
                /* Check if we must create a new higher level pool */
                if(!*parent_cur_op_pool[i] || (*parent_cur_op_pool[i])->type != H5_DAOS_OP_TYPE_EMPTY) {
                    /* Allocate pool struct */
                    if(NULL == (tmp_new_pool_alloc_2 = (H5_daos_op_pool_t *)DV_calloc(sizeof(H5_daos_op_pool_t))))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTALLOC, FAIL, "can't allocate operation pool struct");

                    /* Set op_type */
                    tmp_new_pool_alloc_2->type = H5_DAOS_OP_TYPE_EMPTY;

                    /* Create start task */
                    if(0 != (ret = tse_task_create(H5_daos_op_pool_start_task, &H5_daos_glob_sched_g, tmp_new_pool_alloc_2, &tmp_new_pool_alloc_2->start_task)))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create start task for operation pool: %s", H5_daos_err_to_string(ret));

                    /* Create end task */
                    if(0 != (ret = tse_task_create(H5_daos_op_pool_end_task, &H5_daos_glob_sched_g, tmp_new_pool_alloc_2, &tmp_new_pool_alloc_2->end_task)))
                        D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't create end task for operation pool: %s", H5_daos_err_to_string(ret));

                    /* If a previous higher level pool exists (and is
                     * non-empty), create a dependency on it for tmp_pool */
                    if(*parent_cur_op_pool[i]) {
                        /* Create the dependency on the higher level pool's end
                         * task.  If the higher level pool is empty, the previous
                         * dependency will have been handled at a lower level than
                         * the current pool (by this line of code). */
                        assert((*parent_cur_op_pool[i])->end_task);

                        /* Create dependency */
                        if((ret = tse_task_register_deps(tmp_pool->start_task, 1, &(*parent_cur_op_pool[i])->end_task)) < 0)
                            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));
//printf("new pool dependency on old hlp %p -> %p\n", tmp_task, tmp_pool->start_task); fflush(stdout);
                        /* Schedule previous higher level pool end task */
                        if(0 != (ret = tse_task_schedule((*parent_cur_op_pool[i])->end_task, false)))
                            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));

                        /* Initialize op_gen - it will retain the same op_gen as
                         * previous pool at this level until it is non-empty.
                         * Note that since tmp_new_pool_alloc_2 is calloc'ed, if
                         * there was no previous higher level pool op_gens will
                         * be initialized to 0. */
                        tmp_new_pool_alloc_2->op_gens[0] = (*parent_cur_op_pool[i])->op_gens[0];
                    } /* end if */

                    /* Assign parent_cur_op_pool pointer */
                    tmp_new_pool_alloc_2->parent_cur_op_pool = parent_cur_op_pool[i];

                    /* Set new pool in parent object */
                    *parent_cur_op_pool[i] = tmp_new_pool_alloc_2;
                    tmp_new_pool_alloc_2 = NULL;
                } /* end if */

                /* The higher level pool is now empty, register this pool's end
                 * task as a dependency for the higher level pools' start tasks
                 */
                /* Create dependency */
                if((ret = tse_task_register_deps((*parent_cur_op_pool[i])->start_task, 1, &tmp_pool->end_task)) < 0)
                    D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));
//printf("hlp dependency on new pool %p -> %p\n", tmp_task, (*parent_cur_op_pool[i])->start_task); fflush(stdout);
                /* Assign higher level op_gen */
                tmp_pool->op_gens[i] = (*parent_cur_op_pool[i])->op_gens[0];
            } /* end for */

            /* Adjust base level op_gen */
            tmp_pool->op_gens[0]++;

            /* Take a reference to the object/file */
            if(item)
                item->rc++;
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

        /* If this is a close operation, schedule the end task.  The end task
         * will free this operation pool. */
        if(op_type == H5_DAOS_OP_TYPE_CLOSE
                && 0 != (ret = tse_task_schedule(tmp_pool->end_task, false)))
            D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));

        /* Set new pool as current pool if appropriate */
        if(create_new_pool) {
            *parent_cur_op_pool[0] = tmp_pool;
            tmp_pool = NULL;
            tmp_new_pool_alloc = NULL;
        } /* end if */
    } /* end if */

    /* Add dependency on H5_daos_collective_req_tail and update it if this is a
     * collective operation.  This cannot cause a deadlock since this schedules
     * requests in order, and requests can never be scheduled out of order by
     * the main pool scheme above. */
    if(collective) {
        if(H5_daos_collective_req_tail)
            /* Create dependency */
           if((ret = tse_task_register_deps(first_task, 1, &H5_daos_collective_req_tail->finalize_task)) < 0)
                D_GOTO_ERROR(H5E_DAOS_ASYNC, H5E_CANTINIT, FAIL, "can't register task dependency: %s", H5_daos_err_to_string(ret));

        H5_daos_collective_req_tail = req;
    } /* end if */

    /* Add dependency on dep_req if necessary */
    if(dep_req && (dep_req->status == H5_DAOS_INCOMPLETE
            || dep_req->status == H5_DAOS_SHORT_CIRCUIT)) {
        /* Register dependency */
        if((ret = tse_task_register_deps(first_task, 1, &dep_req->finalize_task)) < 0)
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


/*-------------------------------------------------------------------------
 * Function:    H5_daos_op_pool_free
 *
 * Purpose:     Releases an operation pool.  Removes references to the
 *              parent object and schedules the end task.  The end task
 *              will perform the actual free.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_op_pool_free(H5_daos_op_pool_t *op_pool)
{
    int ret;
    herr_t ret_value = SUCCEED;

    assert(op_pool);

    /* Clear references to parent object */
    op_pool->parent_cur_op_pool = NULL;
    op_pool->item = NULL;

    /* Schedule end task */
    if(0 != (ret = tse_task_schedule(op_pool->end_task, false)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't schedule task to end operation pool: %s", H5_daos_err_to_string(ret));

done:
    D_FUNC_LEAVE;
} /* end H5_daos_op_pool_free() */

