/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef DAOS_VOL_TASK_LIST_H_
#define DAOS_VOL_TASK_LIST_H_

#include "daos_vol.h"

#include <daos/tse.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Default number of task slots allocated in task list */
#define H5_DAOS_TASK_LIST_DEFAULT_NUM_TASKS 1024

/* Task list structure */
typedef struct H5_daos_task_list_t {
    tse_task_t **tasks;
    size_t       max_tasks;
    size_t       num_tasks;
    size_t       num_unsafe_tasks;
} H5_daos_task_list_t;

/* Creates a task list */
herr_t H5_daos_task_list_create(H5_daos_task_list_t **task_list);

/* Frees a task list */
void H5_daos_task_list_free(H5_daos_task_list_t *task_list);

/* Adds a task to the given task list */
herr_t H5_daos_task_list_put(H5_daos_task_list_t *task_list, tse_task_t *task);

/* Retrieves a task from the given task list */
herr_t H5_daos_task_list_get(H5_daos_task_list_t *task_list, tse_task_t **task);

/* Determine if task list has an available task to use */
hbool_t H5_daos_task_list_avail(H5_daos_task_list_t *task_list);

/* Make all "unsafe" tasks in task list available for use */
void H5_daos_task_list_safe(H5_daos_task_list_t *task_list);

#ifdef __cplusplus
}
#endif

#endif /* DAOS_VOL_TASK_LIST_H_ */
