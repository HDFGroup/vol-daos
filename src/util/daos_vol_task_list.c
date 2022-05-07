/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Implements a task list that keeps track of created DAOS/TSE tasks
 *          which can be reused with the tse_task_reset/daos_task_reset API,
 *          avoiding the need to create new tasks. Typical usage would be as
 *          follows:
 *
 *          1. Create a TSE/DAOS task and schedule/run it as normal
 *          2. Just before completion of the task (either in a completion
 *             callback for DAOS tasks or by calling tse_task_complete for
 *             TSE tasks), call H5_daos_task_list_put to place the task
 *             into a task list
 *          3. Once it is guaranteed that no more task scheduler progress
 *             can be made, H5_daos_task_list_safe should be called to
 *             make all "unsafe" tasks available for use. Tasks put into
 *             a task list are initially considered "unsafe" and
 *             unavailable for use since task scheduler progress may cause
 *             a task just placed on a task list to be grabbed while it is
 *             still completing.
 *          3. When a new task is required, a task previously placed
 *             on the task list may be retrieved and reset for re-use
 *             via tse_task_reset/daos_task_reset rather than creating
 *             a new task
 *
 *          Task lists are currently implemented as two stacks of tse_task_t
 *          pointers in a single array. One stack keeps track of tasks that
 *          are available and safe to use, while the other keeps track of
 *          the "unsafe" tasks that must be made available with
 *          H5_daos_task_list_safe.
 */

#include "daos_vol_task_list.h"

#include "daos_vol_private.h"

#include "daos_vol_err.h"
#include "daos_vol_mem.h"

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_list_create
 *
 * Purpose:     Creates a list of tasks that can be re-used by calling
 *              tse_task_reset/daos_task_reset on a list entry. This avoids
 *              overhead from creating tasks repeatedly.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_task_list_create(H5_daos_task_list_t **task_list)
{
    H5_daos_task_list_t *list      = NULL;
    herr_t               ret_value = SUCCEED;

    assert(task_list);

    if (NULL == (list = DV_malloc(sizeof(H5_daos_task_list_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't create task list");
    list->tasks            = NULL;
    list->num_tasks        = 0;
    list->num_unsafe_tasks = 0;
    list->max_tasks        = H5_DAOS_TASK_LIST_DEFAULT_NUM_TASKS;

    if (NULL == (list->tasks = DV_malloc(H5_DAOS_TASK_LIST_DEFAULT_NUM_TASKS * sizeof(tse_task_t *))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate task list slots");

    *task_list = list;

done:
    if (ret_value < 0 && list) {
        H5_daos_task_list_free(list);
        list = NULL;
    }

    D_FUNC_LEAVE;
} /* end H5_daos_task_list_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_list_free
 *
 * Purpose:     Frees a task list.
 *
 * Return:      Nothing
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_task_list_free(H5_daos_task_list_t *task_list)
{
    size_t i;

    assert(task_list);

    if (task_list->tasks) {
        /* Clear out all available tasks */
        for (i = 0; i < task_list->num_tasks; i++)
            if (task_list->tasks[i])
                tse_task_decref(task_list->tasks[i]);

        /* Clear out all "unsafe" tasks */
        if (task_list->num_unsafe_tasks) {
            assert(i == task_list->num_tasks);
            for (; i < task_list->num_tasks + task_list->num_unsafe_tasks; i++)
                if (task_list->tasks[i])
                    tse_task_decref(task_list->tasks[i]);
        }

        DV_free(task_list->tasks);
        task_list->tasks = NULL;
    }

    DV_free(task_list);
} /* end H5_daos_task_list_free() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_list_put
 *
 * Purpose:     Adds a task to a task list and marks it as "unsafe".
 *              H5_daos_task_list_safe must be called to make the task
 *              available for use.
 *
 * Return:      Nothing
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_task_list_put(H5_daos_task_list_t *task_list, tse_task_t *task)
{
    size_t total_tasks;
    herr_t ret_value = SUCCEED;

    assert(task_list);
    assert(task);

    total_tasks = task_list->num_tasks + task_list->num_unsafe_tasks;

    if (total_tasks == task_list->max_tasks) {
        void *tmp_realloc;

        /* Resize task list */
        if (NULL == (tmp_realloc = DV_realloc((void *)task_list->tasks,
                                              2 * task_list->max_tasks * sizeof(tse_task_t *))))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTRESIZE, FAIL, "can't resize task list");
        task_list->tasks = tmp_realloc;
        task_list->max_tasks *= 2;
    }

    /* Take an extra ref. to task so that task list owns
     * task and it will not be freed on completion */
    tse_task_addref(task);

    /* Push task to head of "unsafe" task list */
    task_list->tasks[total_tasks] = task;
    task_list->num_unsafe_tasks++;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_task_list_put() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_list_get
 *
 * Purpose:     Retrieves the next currently-available task from the given
 *              task list. H5_daos_task_list_avail should be used to check
 *              if a task is available before retrieving one.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_task_list_get(H5_daos_task_list_t *task_list, tse_task_t **task)
{
    herr_t ret_value = SUCCEED;

    assert(task_list);
    assert(task);

    if (task_list->num_tasks == 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "task list has no available tasks");

    /* Grab task from head of safe task list */
    *task = task_list->tasks[--task_list->num_tasks];

    /* If there are any "unsafe" tasks, grab the head task
     * to fill the gap.
     */
    if (task_list->num_unsafe_tasks)
        task_list->tasks[task_list->num_tasks] =
            task_list->tasks[task_list->num_tasks + task_list->num_unsafe_tasks];

done:
    D_FUNC_LEAVE;
} /* end H5_daos_task_list_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_list_avail
 *
 * Purpose:     Determines whether the given task list has an available
 *              task that can be retrieved with H5_daos_task_list_get.
 *
 * Return:      TRUE if a task is available/FALSE if no task is available
 *
 *-------------------------------------------------------------------------
 */
hbool_t
H5_daos_task_list_avail(H5_daos_task_list_t *task_list)
{
    hbool_t ret_value = FALSE;

    assert(task_list);

    ret_value = (task_list->num_tasks > 0);

    D_FUNC_LEAVE;
} /* end H5_daos_task_list_avail() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_task_list_safe
 *
 * Purpose:     Makes all "unsafe" tasks in task list available for use.
 *              Tasks added to a task list are initially "unsafe" for use,
 *              as progressing a TSE task scheduler could cause a task on
 *              the task list to be grabbed for re-use while it is still
 *              completing. Therefore, H5_daos_task_list_safe must be
 *              called periodically to make unsafe tasks available.
 *
 * Return:      Nothing
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_task_list_safe(H5_daos_task_list_t *task_list)
{
    assert(task_list);

    if (task_list->num_unsafe_tasks) {
        task_list->num_tasks += task_list->num_unsafe_tasks;
        task_list->num_unsafe_tasks = 0;
    }
}
