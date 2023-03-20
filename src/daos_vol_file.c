/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library.  File routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

#include <libgen.h>

/************************************/
/* Local Type and Struct Definition */
/************************************/

typedef enum { H5_DAOS_CONT_CREATE, H5_DAOS_CONT_OPEN, H5_DAOS_CONT_DESTROY } H5_daos_cont_op_type_t;

/* Common info used for DAOS Unified Namespace routines
 * and DAOS container create/open/destroy routines */
typedef struct H5_daos_cont_op_info_t {
    H5_daos_req_t         *req;
    tse_task_t            *cont_op_metatask;
    daos_handle_t         *poh;
    struct duns_attr_t     duns_attr;
    const char            *path;
    unsigned               flags;
    hbool_t                ignore_missing_path;
    H5_daos_cont_op_type_t op_type;
    union {
        struct {
            H5_daos_acc_params_t facc_params;
            daos_handle_t        cont_poh;
            char                 cont[DAOS_PROP_LABEL_MAX_LEN + 1];
            herr_t              *delete_status;
        } cont_delete_info;
    } u;
} H5_daos_cont_op_info_t;

typedef struct get_obj_count_udata_t {
    char   file_id[DAOS_PROP_LABEL_MAX_LEN + 1];
    size_t obj_count;
} get_obj_count_udata_t;

typedef struct get_obj_ids_udata_t {
    char   file_id[DAOS_PROP_LABEL_MAX_LEN + 1];
    size_t max_objs;
    hid_t *oid_list;
    size_t obj_count;
} get_obj_ids_udata_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_get_cont_props(hid_t fcpl_id, daos_prop_t **props);
static herr_t H5_daos_get_file_access_info(hid_t fapl_id, H5_daos_acc_params_t *fa_out);
#ifdef H5_DAOS_USE_TRANSACTIONS
static int H5_daos_tx_open_prep_cb(tse_task_t *task, void *args);
static int H5_daos_tx_open_comp_cb(tse_task_t *task, void *args);
#endif /* H5_DAOS_USE_TRANSACTIONS */
static herr_t H5_daos_cont_create(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *req,
                                  tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_get_cont_create_task(H5_daos_cont_op_info_t *cont_op_info, tse_task_cb_t prep_cb,
                                           tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                           tse_task_t **dep_task);
static herr_t H5_daos_duns_create_path(H5_daos_cont_op_info_t *create_udata, tse_task_cb_t prep_cb,
                                       tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                       tse_task_t **dep_task);
static int    H5_daos_duns_create_path_task(tse_task_t *task);
static int    H5_daos_duns_create_path_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_duns_create_path_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_create_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_create_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_excl_open_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_cont_open(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *req,
                                tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_get_cont_open_task(H5_daos_cont_op_info_t *cont_op_info, tse_task_cb_t prep_cb,
                                         tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                         tse_task_t **dep_task);
static herr_t H5_daos_get_cont_query_task(tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
                                          tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_duns_resolve_path(H5_daos_cont_op_info_t *resolve_udata, tse_task_cb_t prep_cb,
                                        tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                        tse_task_t **dep_task);
static int    H5_daos_duns_resolve_path_task(tse_task_t *task);
static int    H5_daos_duns_resolve_path_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_open_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_open_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_query_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_query_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_file_get_pool(H5_daos_file_t *file, const char *filepath);
static int    H5_daos_handles_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_file_handles_bcast(H5_daos_file_t *file, H5_daos_req_t *req, tse_task_t **first_task,
                                         tse_task_t **dep_task);
static int    H5_daos_get_container_handles_task(tse_task_t *task);
static herr_t H5_daos_file_delete(const char *file_path, H5_daos_acc_params_t *file_acc_params,
                                  hbool_t ignore_missing, herr_t *delete_status, H5_daos_req_t *req,
                                  tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_file_delete_status_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_get_cont_destroy_task(H5_daos_cont_op_info_t *cont_op_info, tse_task_cb_t prep_cb,
                                            tse_task_cb_t comp_cb, H5_daos_req_t *req,
                                            tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_duns_destroy_path(H5_daos_cont_op_info_t *destroy_udata, tse_task_cb_t prep_cb,
                                        tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                        tse_task_t **dep_task);
static int    H5_daos_duns_destroy_path_task(tse_task_t *task);
static int    H5_daos_duns_destroy_path_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_destroy_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_cont_destroy_comp_cb(tse_task_t *task, void *args);

static herr_t H5_daos_fill_fapl_cache(H5_daos_file_t *file, hid_t fapl_id);
static herr_t H5_daos_fill_enc_plist_cache(H5_daos_file_t *file, hid_t fapl_id);
static herr_t H5_daos_get_obj_count_callback(hid_t id, void *udata);
static herr_t H5_daos_get_obj_ids_callback(hid_t id, void *udata);

static herr_t
H5_daos_get_cont_props(hid_t fcpl_id, daos_prop_t **prop)
{
    char  *prop_str = NULL;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    assert(prop);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(FAIL);

    if ((prop_exists = H5Pexist(fcpl_id, H5_DAOS_FILE_PROP_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for daos container property");
    if (prop_exists) {
        if (H5Pget(fcpl_id, H5_DAOS_FILE_PROP_NAME, &prop_str) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get container property");
    }
    else {
        prop_str = getenv("HDF5_DAOS_FILE_PROP");
        if (prop_str == NULL)
            D_GOTO_DONE(0);
    }

#if CHECK_DAOS_API_VERSION(1, 4)
    if (prop_str) {
        if (daos_prop_from_str(prop_str, strlen(prop_str), prop) != 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't serialize container property");
    }
#endif
done:
    D_FUNC_LEAVE;
}

/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_file_access_info
 *
 * Purpose:     Retrieves information needed for accessing DAOS files from
 *              the given FAPL ID. In case this information is not set on
 *              the given FAPL, or this information needs to be overridden,
 *              this info may also be parsed from the following environment
 *              variables:
 *
 *              DAOS_POOL - DAOS pool to use
 *              DAOS_SYS  - Process set name of the servers managing
 *                          the DAOS pool
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_file_access_info(hid_t fapl_id, H5_daos_acc_params_t *fa_out)
{
    H5_daos_acc_params_t *local_fapl_info = NULL;
    char                 *pool_env        = getenv("DAOS_POOL");
    char                 *sys_env         = getenv("DAOS_SYS");
    herr_t                ret_value       = SUCCEED;

    assert(fa_out);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(FAIL);

    memset(fa_out, 0, sizeof(*fa_out));

    /*
     * First, check to see if any info was set through the use of
     * a H5Pset_fapl_daos() call.
     */
    if (H5Pget_vol_info(fapl_id, (void **)&local_fapl_info) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL info struct");
    if (local_fapl_info) {
        if (!pool_env) {
            strcpy(fa_out->pool, local_fapl_info->pool);
        }

        if (!sys_env && (strlen(local_fapl_info->sys) > 0)) {
            strcpy(fa_out->pool, local_fapl_info->sys);
        }
    }

    /*
     * Check for any information set via environment variables
     */

    if (pool_env) {
        /* Parse pool UUID/Label from env. variable */
        strncpy(fa_out->pool, pool_env, DAOS_PROP_LABEL_MAX_LEN);
        fa_out->pool[DAOS_PROP_LABEL_MAX_LEN] = 0;
    }

    if (sys_env) {
        strncpy(fa_out->sys, sys_env, DAOS_SYS_NAME_MAX);
        fa_out->sys[DAOS_SYS_NAME_MAX] = 0;
    }

    /*
     * Finally, check for any information that wasn't provided at all.
     *
     * An unset pool UUID is an error if bypassing the DUNS. Otherwise,
     * the DUNS may be able to retrieve the pool UUID from the file's
     * parent directory, so a NULL UUID is allowed.
     *
     * A default will be provided for the pool group field if unset.
     */

    if (H5_daos_bypass_duns_g && (0 == strlen(fa_out->pool)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "DAOS pool is not set");

    if ((!local_fapl_info && !sys_env) || (0 == strlen(fa_out->sys))) {
        strncpy(fa_out->sys, DAOS_DEFAULT_GROUP_ID, DAOS_SYS_NAME_MAX);
        fa_out->sys[DAOS_SYS_NAME_MAX] = '\0';
    }

done:
    if (local_fapl_info)
        H5VLfree_connector_info(H5_DAOS_g, local_fapl_info);

    D_FUNC_LEAVE;
} /* end H5_daos_get_file_access_info() */

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
    daos_tx_open_t          *tx_open_args;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for transaction open task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

    assert(udata->req->file);

    /* Set arguments for transaction open */
    if (NULL == (tx_open_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for transaction open task");
    memset(tx_open_args, 0, sizeof(*tx_open_args));
    tx_open_args->coh = udata->req->file->coh;
    tx_open_args->th  = &udata->req->th;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

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
    H5_daos_generic_cb_ud_t *udata     = (H5_daos_generic_cb_ud_t *)args;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for generic task");

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in transaction open task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */
    else if (task->dt_result == 0)
        /* Transaction is open */
        udata->req->th_open = TRUE;

done:
    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    DV_free(udata);

    D_FUNC_LEAVE;
} /* end H5_daos_tx_open_comp_cb() */
#endif /* H5_DAOS_USE_TRANSACTIONS */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_get_pool
 *
 * Purpose:     Attempts to retrieve the DAOS pool UUID/label for a file by
 *              calling duns_resolve_path on the parent directory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_get_pool(H5_daos_file_t *file, const char *filepath)
{
    struct duns_attr_t duns_attr;
    char              *fpath_copy = NULL;
    char              *dir_name;
    char               cwd[PATH_MAX];
    int                ret;
    herr_t             ret_value = SUCCEED;

    assert(file);
    assert(filepath);
    assert(!H5_daos_bypass_duns_g);

    memset(&duns_attr, 0, sizeof(struct duns_attr_t));

    if (NULL == (fpath_copy = strdup(filepath)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy filepath");

    dir_name = dirname(fpath_copy);

    if (!strncmp(dir_name, ".", 2)) {
        if (NULL == getcwd(cwd, PATH_MAX))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get current working directory: %s",
                         strerror(errno));
        dir_name = cwd;
    }

    /* Try to resolve the DUNS directory path. ENODATA signifies that
     * the directory path was probably not created with the DUNS, so
     * we will try other methods for retrieving the file's pool UUID.
     */
    if (0 == (ret = duns_resolve_path(dir_name, &duns_attr)))
#if CHECK_DAOS_API_VERSION(1, 4)
        strcpy(file->facc_params.pool, duns_attr.da_pool);
#else
        uuid_unparse(duns_attr.da_puuid, file->facc_params.pool);
#endif
    else if (ENODATA != ret) {
        if (EOPNOTSUPP == ret)
            D_GOTO_ERROR(H5E_FILE, H5E_PATH, FAIL,
                         "duns_resolve_path failed - DUNS not supported on file system");
        else
            D_GOTO_ERROR(H5E_FILE, H5E_PATH, FAIL, "duns_resolve_path failed: %s",
                         H5_daos_err_to_string(ret));
    } /* end else */

done:
    if (fpath_copy)
        free(fpath_copy);

    D_FUNC_LEAVE;
} /* end H5_daos_file_get_pool() */

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
    int                      ret;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for global handles broadcast task");

    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast global container handles";
    } /* end if */
    else if (task->dt_result == 0) {
        if (udata->req->file->my_rank == 0) {
            /* Reissue bcast if necessary */
            if (udata->buffer_len != udata->count) {
                tse_task_t *bcast_task;

                assert(udata->count == (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));
                assert(udata->buffer_len > (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));

                /* Use full buffer this time */
                udata->count = udata->buffer_len;

                /* Create task for second bcast */
                if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_handles_bcast_comp_cb,
                                        udata, &bcast_task) < 0)
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                 "can't create task for second global handles broadcast");

                /* Schedule second bcast and transfer ownership of udata */
                if (0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret,
                                 "can't schedule task for second global handles broadcast: %s",
                                 H5_daos_err_to_string(ret));
                udata = NULL;
            } /* end if */
        }     /* end if */
        else {
            uint64_t gch_len, gph_len;
            uint8_t *p;

            /* Decode container's global pool handle length */
            p = udata->buffer;
            UINT64DECODE(p, gph_len)

            /* Check for gph_len set to 0 - indicates failure */
            if (gph_len == 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR,
                             "lead process failed to obtain container's global pool handle");

            /* Decode global container handle length */
            UINT64DECODE(p, gch_len)

            /* Check for gch_len set to 0 - indicates failure */
            if (gch_len == 0)
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR,
                             "lead process failed to obtain global container handle");

            /* Check if we need another bcast */
            if (gch_len + gph_len + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE) > (size_t)udata->count) {
                tse_task_t *bcast_task;

                assert(udata->buffer_len == (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));
                assert(udata->count == (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE));

                /* Realloc buffer */
                DV_free(udata->buffer);
                udata->buffer_len = (int)gch_len + (int)gph_len + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE);

                if (NULL == (udata->buffer = DV_malloc((size_t)udata->buffer_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "failed to allocate memory for global handles buffer");
                udata->count = udata->buffer_len;

                /* Create task for second bcast */
                if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_handles_bcast_comp_cb,
                                        udata, &bcast_task) < 0)
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                 "can't create task for second global handles broadcast");

                /* Schedule second bcast and transfer ownership of udata */
                if (0 != (ret = tse_task_schedule(bcast_task, false)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, ret,
                                 "can't schedule task for second global handles broadcast: %s",
                                 H5_daos_err_to_string(ret));
                udata = NULL;
            } /* end if */
            else {
                daos_iov_t gch_glob, gph_glob;

                /* Set up gph_glob */
                gph_glob.iov_buf     = p;
                gph_glob.iov_len     = (size_t)gph_len;
                gph_glob.iov_buf_len = (size_t)gph_len;

                /* Get pool handle */
                if (0 != (ret = daos_pool_global2local(gph_glob, &udata->req->file->container_poh)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get container's global pool handle: %s",
                                 H5_daos_err_to_string(ret));

                p += gph_len;

                /* Set up gch_glob */
                gch_glob.iov_buf     = p;
                gch_glob.iov_len     = (size_t)gch_len;
                gch_glob.iov_buf_len = (size_t)gch_len;

                /* Get container handle */
                if (0 != (ret = daos_cont_global2local(udata->req->file->container_poh, gch_glob,
                                                       &udata->req->file->coh)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get global container handle: %s",
                                 H5_daos_err_to_string(ret));
            } /* end else */
        }     /* end else */
    }         /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "MPI_Ibcast global container handles completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_metatask) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

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
    daos_iov_t               gch_glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    daos_iov_t               gph_glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    uint8_t                 *p;
    size_t                   req_buf_len;
    int                      ret;
    int                      ret_value = 0; /* Return value */

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for global container handles retrieval task");

    assert(udata->req);
    assert(udata->req->file);

    /* Calculate size of global cont handles */
    if (udata->req->status >= -H5_DAOS_INCOMPLETE) {
        if (0 != (ret = daos_cont_local2global(udata->req->file->coh, &gch_glob)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't calculate size of global container handle: %s",
                         H5_daos_err_to_string(ret));
        if (0 != (ret = daos_pool_local2global(udata->req->file->container_poh, &gph_glob)))
            D_DONE_ERROR(H5E_VOL, H5E_CANTGET, ret,
                         "can't calculate size of container's global pool handle: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */

    req_buf_len = (2 * H5_DAOS_ENCODED_UINT64_T_SIZE) +
                  MAX(gch_glob.iov_buf_len + gph_glob.iov_buf_len, (2 * H5_DAOS_GH_BUF_SIZE));

    if (!udata->buffer || (udata->buffer_len < (int)req_buf_len)) {
        void *tmp;

        /* Allocate buffer */
        if (NULL == (tmp = DV_realloc(udata->buffer, req_buf_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                         "can't allocate space for global container handles");
        udata->buffer     = tmp;
        udata->buffer_len = (int)req_buf_len;
        udata->count      = (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE);
        gch_glob.iov_len  = gch_glob.iov_buf_len;
        gph_glob.iov_len  = gph_glob.iov_buf_len;
    } /* end if */

    /* Check for previous errors (wait until after allocation because that must
     * always be done) */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

    /* Encode container's global pool handle length */
    p = udata->buffer;
    UINT64ENCODE(p, (uint64_t)gph_glob.iov_buf_len)

    /* Encode global container handle length */
    UINT64ENCODE(p, (uint64_t)gch_glob.iov_buf_len)

    /* Get container's global pool handle */
    gph_glob.iov_buf = p;
    if (0 != (ret = daos_pool_local2global(udata->req->file->container_poh, &gph_glob)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get container's global pool handle: %s",
                     H5_daos_err_to_string(ret));

    p += gph_glob.iov_buf_len;

    /* Get global container handle */
    gch_glob.iov_buf = p;
    if (0 != (ret = daos_cont_local2global(udata->req->file->coh, &gch_glob)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, ret, "can't get global container handle: %s",
                     H5_daos_err_to_string(ret));

done:
    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "get global container handles";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
H5_daos_file_handles_bcast(H5_daos_file_t *file, H5_daos_req_t *req, tse_task_t **first_task,
                           tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    size_t                   buf_size    = 0;
    int                      ret;
    herr_t                   ret_value = SUCCEED; /* Return value */

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "failed to allocate buffer for MPI broadcast user data");
    bcast_udata->req            = req;
    bcast_udata->obj            = NULL;
    bcast_udata->bcast_metatask = NULL;
    bcast_udata->buffer         = NULL;
    bcast_udata->buffer_len     = 0;
    bcast_udata->count          = 0;
    bcast_udata->comm           = req->file->comm;

    buf_size = (2 * H5_DAOS_GH_BUF_SIZE) + (2 * H5_DAOS_ENCODED_UINT64_T_SIZE);

    /* check if this is the lead rank */
    if (file->my_rank == 0) {
        tse_task_t *get_handles_task;

        /* Create task to get global container handle and global container pool handle */
        if (H5_daos_create_task(H5_daos_get_container_handles_task, *dep_task ? 1 : 0,
                                *dep_task ? dep_task : NULL, NULL, NULL, bcast_udata, &get_handles_task) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create task to get global container handles");

        /* Schedule task to get global handles (or save it to be
         * scheduled later) and give it a reference to req.  Do not transfer
         * ownership of bcast_udata, as that will be owned by bcast_task (which
         * will depend on get_handles_task, so it will not free it before
         * get_handles_task runs). */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(get_handles_task, false)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL,
                             "can't schedule task to get global container handles: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = get_handles_task;
        req->rc++;
        *dep_task = get_handles_task;
    } /* end if */
    else {
        /* Allocate global handle buffer with default initial size */
        if (NULL == (bcast_udata->buffer = DV_malloc(buf_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for global container handles");
        bcast_udata->buffer_len = (int)buf_size;
        bcast_udata->count      = (int)buf_size;
    } /* end else */

done:
    /* Do broadcast */
    if (bcast_udata) {
        if (H5_daos_mpi_ibcast(bcast_udata, NULL, buf_size, (ret_value < 0 ? TRUE : FALSE), NULL,
                               H5_daos_handles_bcast_comp_cb, req, first_task, dep_task) < 0) {
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
H5_daos_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                    hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_file_t *file       = NULL;
    H5_daos_req_t  *int_req    = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
#if 0 /* Needed for storing the root group OID in the global metadata object -                               \
       * see note below */
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    uint8_t root_group_oid_buf[H5_DAOS_ENCODED_OID_SIZE];
    uint8_t *p;
#endif
    int   ret;
    void *ret_value = NULL;

    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE ==
                          H5_DAOS_ENCODED_UINT64_T_SIZE + H5_DAOS_ENCODED_UINT64_T_SIZE);

    H5_daos_inc_api_cnt();

    if (!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL");

    /*
     * Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if (0 == (flags & (H5F_ACC_EXCL | H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL; /*default*/
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* allocate the file object that is returned to the user */
    if (NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct");
    file->item.open_req = NULL;
    file->container_poh = DAOS_HDL_INVAL;
    file->glob_md_oh    = DAOS_HDL_INVAL;
    file->root_grp      = NULL;
    file->create_prop   = NULL;
    file->fcpl_id       = H5P_FILE_CREATE_DEFAULT;
    file->fapl_id       = H5P_FILE_ACCESS_DEFAULT;
    file->item.rc       = 1;
    file->comm          = MPI_COMM_NULL;
    file->info          = MPI_INFO_NULL;

    /* Fill in fields of file we know */
    file->item.type    = H5I_FILE;
    file->item.created = TRUE;
    file->item.file    = file;
    if (NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name");
    file->flags = flags;
    if ((fapl_id != H5P_FILE_ACCESS_DEFAULT) && (file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl");

    /* Get needed cont create properties */
    if (H5_daos_get_cont_props(fcpl_id, &file->create_prop) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS properties");
    /* Get needed file/pool access information */
    if (H5_daos_get_file_access_info(fapl_id, &file->facc_params) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct");

    /* Try to retrieve file's pool UUID using DUNS */
    if (!H5_daos_bypass_duns_g && H5_daos_file_get_pool(file, file->file_name) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't retrieve file's DAOS pool UUID");

    /* Set MPI info on file object */
    if (H5_daos_get_mpi_info(fapl_id, &file->comm, &file->info, &file->my_rank, &file->num_procs) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info");

    /* Hash file name to create uuid */
    uuid_t uuid;
    H5_daos_hash128(name, &uuid);
    uuid_unparse(uuid, file->cont);

    /* Fill FAPL cache */
    if (H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache");

    /* Fill encoded default property list buffer cache */
    if (H5_daos_fill_enc_plist_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill encoded property list buffer cache");

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(file, "file create", NULL, NULL, NULL, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request");
    file->item.open_req = int_req;
    int_req->rc++;

    if (file->my_rank == 0) {
        /* Connect to container's pool */
        if (H5_daos_pool_connect(&file->facc_params, DAOS_PC_RW, &file->container_poh, NULL, int_req,
                                 &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't connect to pool");

        /* Create container on rank 0 */
        if (H5_daos_cont_create(file, flags, int_req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create DAOS container");
    } /* end if */

    /* Broadcast handles (container handle and, optionally, pool handle)
     * to other procs if any.
     */
    if ((file->num_procs > 1) && (H5_daos_file_handles_bcast(file, int_req, &first_task, &dep_task) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, NULL, "can't broadcast DAOS container/pool handles");

    /* Generate oid for global metadata object */
    if (H5_daos_oid_generate(&file->glob_md_oid, TRUE, H5_DAOS_OIDX_GMD, H5I_GROUP,
                             fcpl_id == H5P_FILE_CREATE_DEFAULT ? H5P_DEFAULT : fcpl_id,
                             H5_DAOS_OBJ_CLASS_NAME, file, TRUE, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID");

    /* Open global metadata object */
    if (H5_daos_obj_open(file, int_req, &file->glob_md_oid, DAOS_OO_RW, &file->glob_md_oh,
                         "global metadata object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open global metadata object");

    /* Create root group */
    if (NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_create_helper(
                     file, TRUE, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, NULL, NULL, 0, TRUE, int_req, &first_task,
                     &dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group");

        /* Write root group OID to global metadata object */
        /* Only do this if a non-default object class is used DSINC */
        /* Disabled for now since we don't use it.  Right now, the user must specify
         * the root group's object class through
         * H5daos_set_root_open_object_class(), but eventually we will allow the
         * user to skip this by storing the root group OID in the global metadata
         * object (as below), and, if the initial root group open fails, reading the
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
    if (int_req) {
        assert(file);

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Enqueue the request and add to the global operation pool */
        if (H5_daos_req_enqueue(int_req, first_task, &file->item, H5_DAOS_OP_TYPE_WRITE,
                                H5_DAOS_OP_SCOPE_GLOB, TRUE, !req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, NULL, "file creation failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close file */
        if (file && H5_daos_file_close_helper(file) < 0)
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
H5_daos_cont_create(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *req, tse_task_t **first_task,
                    tse_task_t **dep_task)
{
    H5_daos_cont_op_info_t *create_udata = NULL;
    herr_t                  ret_value    = SUCCEED;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (create_udata = (H5_daos_cont_op_info_t *)DV_malloc(sizeof(H5_daos_cont_op_info_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for container create task");
    create_udata->op_type             = H5_DAOS_CONT_CREATE;
    create_udata->req                 = req;
    create_udata->cont_op_metatask    = NULL;
    create_udata->poh                 = &file->container_poh;
    create_udata->path                = file->file_name;
    create_udata->flags               = flags;
    create_udata->ignore_missing_path = TRUE;
    memset(&create_udata->duns_attr, 0, sizeof(struct duns_attr_t));
    create_udata->duns_attr.da_type      = DAOS_PROP_CO_LAYOUT_HDF5;
    create_udata->duns_attr.da_oclass_id = file->fapl_cache.default_object_class;
    if (file->create_prop)
        create_udata->duns_attr.da_props = file->create_prop;
#if CHECK_DAOS_API_VERSION(1, 4)
    strcpy(create_udata->duns_attr.da_cont, file->cont);
#else
    uuid_parse(file->cont, create_udata->duns_attr.da_cuuid);
#endif

    if (!H5_daos_bypass_duns_g) {
        /* Create task to attempt to resolve DUNS path. This task will handle
         * the H5F_ACC_EXCL and H5F_ACC_TRUNC flags. */
        if (H5_daos_duns_resolve_path(create_udata, NULL, H5_daos_duns_resolve_path_comp_cb, req, first_task,
                                      dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to attempt to resolve DUNS path");

        /* Create task for DUNS path create and container create */
        if (H5_daos_duns_create_path(create_udata, H5_daos_duns_create_path_prep_cb,
                                     H5_daos_duns_create_path_comp_cb, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create DUNS path");
    } /* end if */
    else {
        /* If the H5F_ACC_EXCL flag was specified, ensure that the container does
         * not exist. */
        if (flags & H5F_ACC_EXCL)
            if (H5_daos_get_cont_open_task(create_udata, H5_daos_cont_open_prep_cb, H5_daos_excl_open_comp_cb,
                                           req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to attempt to open container");

        /* Delete the container if H5F_ACC_TRUNC is set.  This shouldn't cause a
         * problem even if the container doesn't exist. */
        if (flags & H5F_ACC_TRUNC)
            if (H5_daos_get_cont_destroy_task(create_udata, H5_daos_cont_destroy_prep_cb,
                                              H5_daos_cont_destroy_comp_cb, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy container");

        /* Create task for container create */
        if (H5_daos_get_cont_create_task(create_udata, H5_daos_cont_create_prep_cb,
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
    if (H5_daos_cont_open(file, flags, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't open DAOS container");

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
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
H5_daos_get_cont_create_task(H5_daos_cont_op_info_t *cont_op_info, tse_task_cb_t prep_cb,
                             tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                             tse_task_t **dep_task)
{
    tse_task_t *create_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(cont_op_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container create */
    if (0 != (ret = daos_task_create(DAOS_OPC_CONT_CREATE, &H5_daos_glob_sched_g, *dep_task ? 1 : 0,
                                     *dep_task ? dep_task : NULL, &create_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create container: %s",
                     H5_daos_err_to_string(ret));

    /* Set callback functions for container create */
    if (prep_cb || comp_cb)
        if (0 != (ret = tse_task_register_cbs(create_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL,
                         "can't register callbacks for task to create container: %s",
                         H5_daos_err_to_string(ret));

    /* Set private data for container create */
    (void)tse_task_set_priv(create_task, cont_op_info);

    /* Schedule container create task (or save it to be scheduled later) and give
     * it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(create_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to create container: %s",
                         H5_daos_err_to_string(ret));
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
H5_daos_duns_create_path(H5_daos_cont_op_info_t *create_udata, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                         H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *create_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(create_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for DUNS path create */
    if (H5_daos_create_task(H5_daos_duns_create_path_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            prep_cb, comp_cb, create_udata, &create_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to create DUNS path");

    /* Schedule DUNS path create task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(create_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to create DUNS path: %s",
                         H5_daos_err_to_string(ret));
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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path creation task");

    assert(udata->req);
    assert(udata->path);
    assert(udata->poh);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

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
 *              the DAOS pool from the daos_pool_info_t struct into
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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path creation task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

    /* Set the pool UUID for duns_create_path */
#if CHECK_DAOS_API_VERSION(1, 4)
    strcpy(udata->duns_attr.da_pool, udata->req->file->facc_params.pool);
#else
    uuid_parse(udata->req->file->facc_params.pool, udata->duns_attr.da_puuid);
#endif

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path creation task");

    assert(udata->req);

    /* Handle errors in duns_create_path task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "DUNS path creation";
    } /* end if */

#if CHECK_DAOS_API_VERSION(1, 4)
    strcpy(udata->req->file->cont, udata->duns_attr.da_cont);
#else
    uuid_unparse(udata->duns_attr.da_cuuid, udata->req->file->cont);
#endif

    if (udata->req->file->create_prop)
        daos_prop_free(udata->req->file->create_prop);
done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DUNS path creation completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
    daos_cont_create_t     *create_args;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for container create task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

    assert(udata->req->file);
    assert(udata->poh);

    /* Set daos_cont_create task args */
    if (NULL == (create_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for container create task");
    memset(create_args, 0, sizeof(*create_args));
    create_args->prop = udata->duns_attr.da_props;
    create_args->poh  = *udata->poh;
    uuid_parse(udata->req->file->cont, create_args->uuid);

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DAOS container create task");

    assert(udata->req);

    /* Handle errors in daos_cont_create task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "DAOS container create";
    } /* end if */

    if (udata->req->file->create_prop)
        daos_prop_free(udata->req->file->create_prop);

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DAOS container create completion callback";
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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for generic task");

    assert(udata->req);
    assert(udata->req->file || task->dt_result != 0);

    /* Handle errors in task.  Only record error in udata->req_status if it does
     * not already contain an error (it could contain an error if another task
     * this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && task->dt_result != -DER_NONEXIST &&
        udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "excl. container open";
    } /* end if */

    /* If the open succeeded, return failure (we're verifying that the file
     * doesn't exist) */
    if (task->dt_result == 0)
        D_DONE_ERROR(H5E_FILE, H5E_FILEEXISTS, -H5_DAOS_FILE_EXISTS,
                     "exclusive open failed: file already exists");

done:
    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "excl. container open completion callback";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
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
H5_daos_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t H5VL_DAOS_UNUSED dxpl_id,
                  void H5VL_DAOS_UNUSED **req)
{
    H5_daos_file_t *file = NULL;
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    H5_daos_req_t *int_req    = NULL;
    tse_task_t    *first_task = NULL;
    tse_task_t    *dep_task   = NULL;
    int            ret;
    void          *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL");

    /* Allocate the file object that is returned to the user */
    if (NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct");
    file->item.open_req = NULL;
    file->container_poh = DAOS_HDL_INVAL;
    file->glob_md_oh    = DAOS_HDL_INVAL;
    file->root_grp      = NULL;
    file->fapl_id       = H5P_FILE_ACCESS_DEFAULT;
    file->item.rc       = 1;
    file->comm          = MPI_COMM_NULL;
    file->info          = MPI_INFO_NULL;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if (NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name");
    file->flags = flags;
    if ((fapl_id != H5P_FILE_ACCESS_DEFAULT) && (file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl");

    /* Get needed file/pool access information */
    if (H5_daos_get_file_access_info(fapl_id, &file->facc_params) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct");

    /* Try to retrieve file's pool UUID using DUNS */
    if (!H5_daos_bypass_duns_g && H5_daos_file_get_pool(file, file->file_name) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't retrieve file's DAOS pool");

    /* Set MPI info on file object */
    if (H5_daos_get_mpi_info(fapl_id, &file->comm, &file->info, &file->my_rank, &file->num_procs) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info");

    /* Hash file name to create uuid if bypassing DUNS */
    if (H5_daos_bypass_duns_g) {
        uuid_t uuid;

        H5_daos_hash128(name, &uuid);
        uuid_unparse(uuid, file->cont);
    }

#ifdef DV_HAVE_SNAP_OPEN_ID
    if (H5Pget(fapl_id, H5_DAOS_SNAP_OPEN_ID, &snap_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for snap ID");

    /* Check for opening a snapshot with write access (disallowed) */
    if ((snap_id != H5_DAOS_SNAP_ID_INVAL) && (flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "write access requested to snapshot - disallowed");
#endif

    /* Fill FAPL cache */
    if (H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache");

    /* Fill encoded default property list buffer cache */
    if (H5_daos_fill_enc_plist_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill encoded property list buffer cache");

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(file, "file open", NULL, NULL, NULL, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request");
    file->item.open_req = int_req;
    int_req->rc++;

    /* Open container on rank 0 */
    if ((file->my_rank == 0) && H5_daos_cont_open(file, flags, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't open DAOS container");

    /* Broadcast handles (container handle and, optionally, pool handle)
     * to other procs if any.
     */
    if ((file->num_procs > 1) && (H5_daos_file_handles_bcast(file, int_req, &first_task, &dep_task) < 0))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't broadcast DAOS container/pool handles");

    /* Generate oid for global metadata object */
    if (H5_daos_oid_generate(&file->glob_md_oid, TRUE, H5_DAOS_OIDX_GMD, H5I_GROUP,
                             fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
                             H5_DAOS_ROOT_OPEN_OCLASS_NAME, file, TRUE, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID");

    /* Allocate the root group object */
    if (NULL == (file->root_grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct");

    /* Generate root group oid */
    if (H5_daos_oid_generate(&file->root_grp->obj.oid, TRUE, H5_DAOS_OIDX_ROOT, H5I_GROUP,
                             fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
                             H5_DAOS_ROOT_OPEN_OCLASS_NAME, file, TRUE, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID");

    /* Open global metadata object */
    if (H5_daos_obj_open(file, int_req, &file->glob_md_oid, flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO,
                         &file->glob_md_oh, "global metadata object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open global metadata object");

    /* Open root group and fill in root group's oid */
    if (H5_daos_group_open_helper(file, file->root_grp, H5P_GROUP_ACCESS_DEFAULT, TRUE, int_req, &first_task,
                                  &dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open root group");

    ret_value = (void *)file;

done:
    if (int_req) {
        assert(file);

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the global request queue */
        if (H5_daos_req_enqueue(int_req, first_task, &file->item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_GLOB,
                                TRUE, !req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, NULL, "file open failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close file */
        if (file && H5_daos_file_close_helper(file) < 0)
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
H5_daos_cont_open(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *req, tse_task_t **first_task,
                  tse_task_t **dep_task)
{
    H5_daos_cont_op_info_t  *open_udata    = NULL;
    H5_daos_generic_cb_ud_t *tx_open_udata = NULL;
#ifdef H5_DAOS_USE_TRANSACTIONS
    tse_task_t *tx_open_task;
    int         ret;
#endif
    herr_t ret_value = SUCCEED;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (open_udata = (H5_daos_cont_op_info_t *)DV_malloc(sizeof(H5_daos_cont_op_info_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for container open task");
    open_udata->op_type             = H5_DAOS_CONT_OPEN;
    open_udata->req                 = req;
    open_udata->poh                 = &file->container_poh;
    open_udata->cont_op_metatask    = NULL;
    open_udata->path                = file->file_name;
    open_udata->flags               = flags;
    open_udata->ignore_missing_path = FALSE;
    memset(&open_udata->duns_attr, 0, sizeof(struct duns_attr_t));

    /* For file opens only, create task for resolving DUNS path if not bypassing
     * DUNS and create task to connect to container's pool.
     */
    if (0 == (flags & H5F_ACC_CREAT)) {
        if (!H5_daos_bypass_duns_g &&
            H5_daos_duns_resolve_path(open_udata, NULL, H5_daos_duns_resolve_path_comp_cb, req, first_task,
                                      dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to resolve DUNS path");

        /* Connect to container's pool */
        if (H5_daos_pool_connect(&file->facc_params, flags & H5F_ACC_RDWR ? DAOS_PC_RW : DAOS_PC_RO,
                                 &file->container_poh, NULL, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't connect to pool");
    }

    if (H5_daos_get_cont_open_task(open_udata, H5_daos_cont_open_prep_cb, H5_daos_cont_open_comp_cb, req,
                                   first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open container");

    if (!H5_daos_bypass_duns_g) {
        if (H5_daos_get_cont_query_task(H5_daos_cont_query_prep_cb, H5_daos_cont_query_comp_cb, req,
                                        first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to query container");
    }

    open_udata = NULL;

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Create task for transaction open */
    if (0 != (ret = daos_task_create(DAOS_OPC_TX_OPEN, &H5_daos_glob_sched_g, 0, NULL, &tx_open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open transaction: %s",
                     H5_daos_err_to_string(ret));

    /* Register dependency for task */
    assert(*dep_task);
    if (0 != (ret = tse_task_register_deps(tx_open_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL,
                     "can't create dependencies for task to open transaction: %s",
                     H5_daos_err_to_string(ret));

    /* Set private data for transaction open */
    if (NULL == (tx_open_udata = (H5_daos_generic_cb_ud_t *)DV_malloc(sizeof(H5_daos_generic_cb_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for transaction open task");
    tx_open_udata->req       = req;
    tx_open_udata->task_name = "transaction open";
    (void)tse_task_set_priv(tx_open_task, tx_open_udata);

    /* Set callback functions for transaction open */
    if (0 != (ret = tse_task_register_cbs(tx_open_task, H5_daos_tx_open_prep_cb, NULL, 0,
                                          H5_daos_tx_open_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't register callbacks for task to open container: %s",
                     H5_daos_err_to_string(ret));

    /* Schedule transaction open task and give it a reference to req */
    assert(*first_task);
    if (0 != (ret = tse_task_schedule(tx_open_task, false)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to open transaction: %s",
                     H5_daos_err_to_string(ret));
    req->rc++;
    tx_open_udata = NULL;
    *dep_task     = tx_open_task;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* If a snapshot was requested, use it as the epoch, otherwise query it
     */
#ifdef DV_HAVE_SNAP_OPEN_ID
    if (snap_id != H5_DAOS_SNAP_ID_INVAL) {
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
    if (ret_value < 0) {
        open_udata    = DV_free(open_udata);
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
H5_daos_get_cont_open_task(H5_daos_cont_op_info_t *cont_op_info, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                           H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *open_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(cont_op_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container open */
    if (0 != (ret = daos_task_create(DAOS_OPC_CONT_OPEN, &H5_daos_glob_sched_g, *dep_task ? 1 : 0,
                                     *dep_task ? dep_task : NULL, &open_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to open container: %s",
                     H5_daos_err_to_string(ret));

    /* Set callback functions for container open */
    if (prep_cb || comp_cb)
        if (0 != (ret = tse_task_register_cbs(open_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL,
                         "can't register callbacks for task to open container: %s",
                         H5_daos_err_to_string(ret));

    /* Set private data for container open */
    (void)tse_task_set_priv(open_task, cont_op_info);

    /* Schedule container open task (or save it to be scheduled later) and give
     * it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(open_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to open container: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = open_task;
    req->rc++;
    *dep_task = open_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_cont_open_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_cont_query_task
 *
 * Purpose:     Creates an asynchronous DAOS task that simply calls
 *              daos_cont_query.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_cont_query_task(tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, H5_daos_req_t *req,
                            tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *query_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container query */
    if (H5_daos_create_daos_task(DAOS_OPC_CONT_QUERY, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, prep_cb,
                                 comp_cb, req, &query_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to query cont prop");

    /* Schedule container query task (or save it to be scheduled later) and give
     * it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(query_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to query container: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = query_task;
    req->rc++;
    *dep_task = query_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_cont_query_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_query_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_cont_query.
 *              Currently checks for errors from previous tasks then sets
 *              arguments for daos_cont_query.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_query_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_req_t     *req;
    daos_cont_query_t *query_args;
    int                ret_value = 0;

    /* Get private data */
    if (NULL == (req = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for container query task");

    assert(req);
    assert(req->file);

    /* Handle errors */
    H5_DAOS_PREP_REQ(req, H5E_FILE);

    /* Set daos_cont_query task args */
    if (NULL == (query_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for container query task");
    memset(query_args, 0, sizeof(*query_args));

    req->file->cont_prop = daos_prop_alloc(1);
    if (req->file->cont_prop == NULL)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, -H5_DAOS_DAOS_GET_ERROR, "can't allocate prop");

    req->file->cont_prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;
    query_args->coh                               = req->file->coh;
    query_args->info                              = NULL;
    query_args->prop                              = req->file->cont_prop;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_query_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_query_comp_cb
 *
 * Purpose:     Completion callback for asynchronous daos_cont_query.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cont_query_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_req_t *req;
    int            ret_value = 0;

    /* Get private data */
    if (NULL == (req = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DAOS container query task");

    assert(req);

    /* Handle errors in daos_cont_query task.  Only record error in req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = task->dt_result;
        req->failed_task = "DAOS container query";
    }

    if (task->dt_result == 0) {
        /** verify Layout property is HDF5 */
        if (req->file->cont_prop->dpp_entries[0].dpe_val != DAOS_PROP_CO_LAYOUT_HDF5)
            D_GOTO_ERROR(H5E_FILE, H5E_NOTHDF5, -H5_DAOS_BAD_VALUE, "Not a valid HDF5 DAOS file");
    }

done:
    /* Free private data if we haven't released ownership */
    if (req->file->cont_prop)
        daos_prop_free(req->file->cont_prop);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = ret_value;
        req->failed_task = "DAOS container query completion callback";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_cont_query_comp_cb() */

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
H5_daos_duns_resolve_path(H5_daos_cont_op_info_t *resolve_udata, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                          H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *resolve_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(resolve_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for resolving DUNS path */
    if (H5_daos_create_task(H5_daos_duns_resolve_path_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            prep_cb, comp_cb, resolve_udata, &resolve_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to resolve DUNS path");

    /* Schedule DUNS path resolve task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(resolve_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to resolve DUNS path: %s",
                         H5_daos_err_to_string(ret));
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
    if (resolve_udata->flags & H5F_ACC_TRUNC) {
        /* Create metatask for DUNS path resolve task in case more tasks
         * need to be created to destroy an existing DUNS path during
         * file creates with H5F_ACC_TRUNC access.
         */
        if (H5_daos_create_task(H5_daos_metatask_autocomplete, 1, &resolve_task, NULL, NULL, NULL,
                                &resolve_udata->cont_op_metatask) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create meta task for DUNS path resolve");

        /* Schedule meta task */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(resolve_udata->cont_op_metatask, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule meta task for DUNS path resolve: %s",
                         H5_daos_err_to_string(ret));

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path resolve task");

    assert(udata->req);
    assert(udata->path);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

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
    tse_task_t             *first_task = NULL;
    tse_task_t             *dep_task   = NULL;
    int                     ret;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path resolve task");

    assert(udata->req);
    assert(udata->path);

    if (task->dt_result != 0) {
        /* DSINC - DER_INVAL and DER_NONEXIST do not need to be checked against with the latest DAOS master.
         */
        if (udata->ignore_missing_path && ((-DER_NONEXIST == task->dt_result) ||
                                           (-DER_INVAL == task->dt_result) || (ENOENT == task->dt_result))) {
            D_GOTO_DONE(0); /* Short-circuit success when file is expected to potentially be missing */
        }                   /* end if */
        else if (task->dt_result != -H5_DAOS_PRE_ERROR && task->dt_result != -H5_DAOS_SHORT_CIRCUIT &&
                 udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            /* Set result to -H5_DAOS_BAD_VALUE instead of task->dt_result
             * because DUNS functions return positive error codes and that would
             * trip up the error handling in this connector. */
            udata->req->status      = -H5_DAOS_BAD_VALUE;
            udata->req->failed_task = "DUNS path resolve";
            if (ENOENT == task->dt_result) {
                udata->req->status = -H5_DAOS_H5_OPEN_ERROR;
                D_DONE_ERROR(H5E_FILE, H5E_CANTOPENFILE, -H5_DAOS_H5_OPEN_ERROR, "file '%s' does not exist",
                             udata->path);
            }
            if (EOPNOTSUPP == task->dt_result) {
                udata->req->status = -H5_DAOS_H5_UNSUPPORTED_ERROR;
                D_DONE_ERROR(H5E_FILE, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR,
                             "can't create/resolve DUNS path - DUNS not supported on file system");
            }
            else if (ENODATA == task->dt_result)
                D_DONE_ERROR(H5E_FILE, H5E_NOTHDF5, -H5_DAOS_BAD_VALUE,
                             "file '%s' is not a valid HDF5 DAOS file", udata->path);
        } /* end if */
    }     /* end if */
    else {
        /* Determine if special action is required according to flags specified */
        if (udata->flags & H5F_ACC_EXCL) {
            /* File already exists during exclusive open */
            D_GOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, -H5_DAOS_FILE_EXISTS,
                         "exclusive open failed: file already exists");
        } /* end if */
        else if (udata->flags & H5F_ACC_TRUNC) {
            assert(udata->cont_op_metatask);
            assert(udata->req->file);

            /* Set parameters for file delete operation */
#if CHECK_DAOS_API_VERSION(1, 4)
            strcpy(udata->u.cont_delete_info.facc_params.pool, udata->duns_attr.da_pool);
#else
            uuid_unparse(udata->duns_attr.da_puuid, udata->u.cont_delete_info.facc_params.pool);
#endif
            strcpy(udata->u.cont_delete_info.facc_params.sys, udata->req->file->facc_params.sys);

            if (H5_daos_file_delete(udata->path, &udata->u.cont_delete_info.facc_params, TRUE, NULL,
                                    udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_H5_DESTROY_ERROR,
                             "can't create task to destroy container");

            /* Register dependency on dep_task for file deletion metatask */
            if (dep_task && 0 != (ret = tse_task_register_deps(udata->cont_op_metatask, 1, &dep_task)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, ret,
                             "can't create dependencies for file deletion metatask: %s",
                             H5_daos_err_to_string(ret));
        } /* end else */

        /* If opening a DAOS container after resolving a DUNS path,
         * copy the resolved pool UUID into the file object.
         */
        if (udata->op_type == H5_DAOS_CONT_OPEN)
#if CHECK_DAOS_API_VERSION(1, 4)
            strcpy(udata->req->file->facc_params.pool, udata->duns_attr.da_pool);
#else
            uuid_unparse(udata->duns_attr.da_puuid, udata->req->file->facc_params.pool);
#endif
        /* If deleting a DUNS path/DAOS container, copy the resolved pool UUID for the delete operation */
        else if (udata->op_type == H5_DAOS_CONT_DESTROY)
#if CHECK_DAOS_API_VERSION(1, 4)
            strcpy(udata->u.cont_delete_info.facc_params.pool, udata->duns_attr.da_pool);
#else
            uuid_unparse(udata->duns_attr.da_puuid, udata->u.cont_delete_info.facc_params.pool);
#endif
    } /* end else */

done:
    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, ret, "can't schedule task to destroy DUNS path: %s",
                     H5_daos_err_to_string(ret));

    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DUNS path resolve completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
    daos_cont_open_t       *open_args;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for container open task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

    assert(udata->req->file);
    assert(udata->poh);

    /* Set file's UUID if necessary */
    if (!H5_daos_bypass_duns_g && 0 == (udata->flags & H5F_ACC_CREAT))
#if CHECK_DAOS_API_VERSION(1, 4)
        strcpy(udata->req->file->cont, udata->duns_attr.da_cont);
#else
        uuid_unparse(udata->duns_attr.da_cuuid, udata->req->file->cont);
#endif

    /* Set daos_cont_open task args */
    if (NULL == (open_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for container open task");
    memset(open_args, 0, sizeof(*open_args));
    open_args->poh = *udata->poh;
#if CHECK_DAOS_API_VERSION(1, 4)
    open_args->cont = udata->req->file->cont;
#else
    uuid_unparse(udata->req->file->cont, open_args->uuid);
#endif
    open_args->flags = udata->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO;
    open_args->coh   = &udata->req->file->coh;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DAOS container open task");

    assert(udata->req);

    /* Handle errors in daos_cont_open task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "DAOS container open";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DAOS container open completion callback";
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
H5_daos_file_get(void *_item, H5VL_file_get_args_t *get_args, hid_t H5VL_DAOS_UNUSED dxpl_id,
                 void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item      = (H5_daos_item_t *)_item;
    H5_daos_file_t *file      = NULL;
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!get_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    if (get_args->op_type == H5VL_FILE_GET_NAME) {
        file = item->file;

        if (H5I_FILE != item->type && H5I_GROUP != item->type && H5I_DATATYPE != item->type &&
            H5I_DATASET != item->type && H5I_ATTR != item->type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                         "object is not a file, group, datatype, dataset or attribute");
    }
    else {
        file = (H5_daos_file_t *)item;
        if (H5I_FILE != file->item.type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");
    }

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (get_args->op_type) {
        /* "get container info" */
        case H5VL_FILE_GET_CONT_INFO: {
            H5VL_file_cont_info_t *info = get_args->args.get_cont_info.info;

            /* Verify structure version */
            if (info->version != H5VL_CONTAINER_INFO_VERSION)
                D_GOTO_ERROR(H5E_FILE, H5E_VERSION, FAIL, "wrong container info version number");

            /* Set the container info fields */
            info->feature_flags = 0; /* None currently defined */
            info->token_size    = H5_DAOS_ENCODED_OID_SIZE;
            info->blob_id_size  = H5_DAOS_BLOB_ID_SIZE;

            break;
        } /* H5VL_FILE_GET_CONT_INFO */

        /* H5Fget_access_plist */
        case H5VL_FILE_GET_FAPL: {
            hid_t *ret_id = &get_args->args.get_fapl.fapl_id;

            if ((*ret_id = H5Pcopy(file->fapl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FAPL");

            break;
        } /* H5VL_FILE_GET_FAPL */

        /* H5Fget_create_plist */
        case H5VL_FILE_GET_FCPL: {
            hid_t *ret_id = &get_args->args.get_fcpl.fcpl_id;

            /* Wait for the file to open if necessary */
            if (!file->item.created && file->item.open_req->status != 0) {
                if (H5_daos_progress(file->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (file->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "file open failed");
            } /* end if */

            /* The file's FCPL is stored as the group's GCPL */
            if ((*ret_id = H5Pcopy(file->root_grp->gcpl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FCPL");

            /* Set root group's object class on fcpl */
            if (H5_daos_set_oclass_from_oid(*ret_id, file->root_grp->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");

            break;
        } /* H5VL_FILE_GET_FCPL */

        /* H5Fget_intent */
        case H5VL_FILE_GET_INTENT: {
            unsigned *ret_intent = get_args->args.get_intent.flags;

            if ((file->flags & H5F_ACC_RDWR) == H5F_ACC_RDWR)
                *ret_intent = H5F_ACC_RDWR;
            else
                *ret_intent = H5F_ACC_RDONLY;

            break;
        } /* H5VL_FILE_GET_INTENT */

        /* H5Fget_name */
        case H5VL_FILE_GET_NAME: {
            H5I_type_t obj_type      = get_args->args.get_name.type;
            size_t     name_buf_size = get_args->args.get_name.buf_size;
            char      *name_buf      = get_args->args.get_name.buf;
            size_t    *ret_size      = get_args->args.get_name.file_name_len;

            if (H5I_FILE != obj_type)
                file = file->item.file;

            *ret_size = strlen(file->file_name);

            if (name_buf) {
                strncpy(name_buf, file->file_name, name_buf_size - 1);
                name_buf[name_buf_size - 1] = '\0';
            } /* end if */

            break;
        } /* H5VL_FILE_GET_NAME */

        /* H5Fget_obj_count */
        case H5VL_FILE_GET_OBJ_COUNT: {
            unsigned              obj_types = get_args->args.get_obj_count.types;
            size_t               *ret_val   = get_args->args.get_obj_count.count;
            get_obj_count_udata_t udata;

            udata.obj_count = 0;

            strcpy(udata.file_id, file->cont);

            if (obj_types & H5F_OBJ_FILE)
                if (H5Iiterate(H5I_FILE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs");
            if (obj_types & H5F_OBJ_DATASET)
                if (H5Iiterate(H5I_DATASET, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                 "failed to iterate over file's open dataset IDs");
            if (obj_types & H5F_OBJ_GROUP)
                if (H5Iiterate(H5I_GROUP, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs");
            if (obj_types & H5F_OBJ_DATATYPE)
                if (H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                 "failed to iterate over file's open datatype IDs");
            if (obj_types & H5F_OBJ_ATTR)
                if (H5Iiterate(H5I_ATTR, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                 "failed to iterate over file's open attribute IDs");

            *ret_val = udata.obj_count;

            break;
        } /* H5VL_FILE_GET_OBJ_COUNT */

        /* H5Fget_obj_ids */
        case H5VL_FILE_GET_OBJ_IDS: {
            unsigned            obj_types = get_args->args.get_obj_ids.types;
            size_t              max_ids   = get_args->args.get_obj_ids.max_objs;
            hid_t              *oid_list  = get_args->args.get_obj_ids.oid_list;
            size_t             *ret_val   = get_args->args.get_obj_ids.count;
            get_obj_ids_udata_t udata;

            udata.max_objs  = max_ids;
            udata.obj_count = 0;
            udata.oid_list  = oid_list;

            if (max_ids > 0) {
                strcpy(udata.file_id, file->cont);

                if (obj_types & H5F_OBJ_FILE)
                    if (H5Iiterate(H5I_FILE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                     "failed to iterate over file's open file IDs");
                if (obj_types & H5F_OBJ_DATASET)
                    if (H5Iiterate(H5I_DATASET, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                     "failed to iterate over file's open dataset IDs");
                if (obj_types & H5F_OBJ_GROUP)
                    if (H5Iiterate(H5I_GROUP, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                     "failed to iterate over file's open group IDs");
                if (obj_types & H5F_OBJ_DATATYPE)
                    if (H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                     "failed to iterate over file's open datatype IDs");
                if (obj_types & H5F_OBJ_ATTR)
                    if (H5Iiterate(H5I_ATTR, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL,
                                     "failed to iterate over file's open attribute IDs");
            }

            *ret_val = udata.obj_count;

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
H5_daos_file_specific(void *item, H5VL_file_specific_args_t *specific_args, hid_t dxpl_id, void **req)
{
    H5_daos_file_t          *file               = NULL;
    H5_daos_acc_params_t     faccess_info       = {{0}, {0}};
    H5_daos_mpi_ibcast_ud_t *bcast_info         = NULL;
    H5_daos_req_t           *int_req            = NULL;
    tse_task_t              *first_task         = NULL;
    tse_task_t              *dep_task           = NULL;
    MPI_Comm                 file_delete_comm   = MPI_COMM_NULL;
    MPI_Info                 file_delete_info   = MPI_INFO_NULL;
    herr_t                   file_delete_status = SUCCEED;
    int                      ret;
    herr_t                   ret_value = SUCCEED; /* Return value */

    H5_daos_inc_api_cnt();

    if (!specific_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    if (item) {
        file = ((H5_daos_item_t *)item)->file;
        H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);
    }

    switch (specific_args->op_type) {
        /* H5Fflush */
        case H5VL_FILE_FLUSH: {
            if (!file)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "file not provided to file flush operation");

            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(file, "file flush", file->item.open_req, NULL, NULL,
                                                      H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            if (H5_daos_file_flush(file, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file");

            break;
        } /* H5VL_FILE_FLUSH */

        /* H5Freopen */
        case H5VL_FILE_REOPEN: {
            unsigned reopen_flags = file->flags;
            void   **ret_file     = specific_args->args.reopen.file;

            /* Strip any file creation-related flags */
            reopen_flags &= ~(H5F_ACC_TRUNC | H5F_ACC_EXCL | H5F_ACC_CREAT);
            if (NULL ==
                (*ret_file = H5_daos_file_open(file->file_name, reopen_flags, file->fapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "can't reopen file");

            break;
        } /* H5VL_FILE_REOPEN */

        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE: {
            hid_t              file_fapl         = specific_args->args.is_accessible.fapl_id;
            const char        *filename          = specific_args->args.is_accessible.filename;
            hbool_t           *ret_is_accessible = specific_args->args.is_accessible.accessible;
            struct duns_attr_t duns_attr;

            memset(&duns_attr, 0, sizeof(struct duns_attr_t));

            /* Initialize returned value in case we fail */
            *ret_is_accessible = FALSE;

            if (NULL == filename)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "filename is NULL");

            if (!H5_daos_bypass_duns_g) {
                /* Attempt to resolve the given filename to a valid HDF5 DAOS file */
                /* DSINC - DER_INVAL and DER_NONEXIST do not need to be checked against with the latest DAOS
                 * master. */
                if (0 != (ret = duns_resolve_path(filename, &duns_attr))) {
                    if ((-DER_NONEXIST == ret) || (-DER_INVAL == ret) || (ENOENT == ret) ||
                        (ENODATA == ret)) {
                        /* File is missing, is not a HDF5 DAOS-based file, or does not contain the necessary
                         * DUNS attributes */
                        *ret_is_accessible = FALSE;
                    } /* end if */
                    else
                        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, FAIL, "duns_resolve_path failed: %s",
                                     H5_daos_err_to_string(ret));
                } /* end if */
                else if (duns_attr.da_type != DAOS_PROP_CO_LAYOUT_HDF5) {
                    /* File path resolved to a DAOS-based file, but was not an HDF5 DAOS file */
                    *ret_is_accessible = FALSE;
                } /* end else */
                else
                    *ret_is_accessible = TRUE;
            } /* end if */
            else {
                void *opened_file = NULL;

                H5E_BEGIN_TRY
                {
                    opened_file = H5_daos_file_open(filename, H5F_ACC_RDONLY, file_fapl, dxpl_id, NULL);
                }
                H5E_END_TRY;

                *ret_is_accessible = opened_file ? TRUE : FALSE;

                if (opened_file)
                    if (H5_daos_file_close(opened_file, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "error occurred while closing file");
            } /* end else */

            break;
        } /* H5VL_FILE_IS_ACCESSIBLE */

        /* H5Fdelete */
        case H5VL_FILE_DELETE: {
            hid_t       fapl_id  = specific_args->args.del.fapl_id;
            const char *filename = specific_args->args.del.filename;
            int         mpi_rank, mpi_size;

            /* Start H5 operation */
            if (NULL ==
                (int_req = H5_daos_req_create(NULL, "file delete", NULL, NULL, NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Get needed file/pool access information */
            if (H5_daos_get_file_access_info(fapl_id, &faccess_info) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get DAOS info struct");

            /* Get any MPI info available */
            if (H5_daos_get_mpi_info(fapl_id, &file_delete_comm, &file_delete_info, &mpi_rank, &mpi_size) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get HDF5 MPI information");

            if ((mpi_rank == 0) && H5_daos_file_delete(filename, &faccess_info, FALSE, &file_delete_status,
                                                       int_req, &first_task, &dep_task) < 0) {
                /* Make sure to participate in following broadcast if needed */
                if (mpi_size > 1)
                    D_DONE_ERROR(H5E_FILE, H5E_CANTDELETE, FAIL, "can't delete file");
                else
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETE, FAIL, "can't delete file");
            }

            if (mpi_size > 1) {
                /* Setup broadcast of file deletion status to other ranks */
                if (NULL ==
                    (bcast_info = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                 "failed to allocate buffer for MPI broadcast user data");
                bcast_info->req        = int_req;
                bcast_info->obj        = NULL;
                bcast_info->buffer     = &file_delete_status;
                bcast_info->buffer_len = bcast_info->count = (int)sizeof(file_delete_status);
                bcast_info->comm                           = file_delete_comm;

                if (H5_daos_mpi_ibcast(bcast_info, NULL, sizeof(file_delete_status), FALSE, NULL,
                                       H5_daos_file_delete_status_bcast_comp_cb, int_req, &first_task,
                                       &dep_task) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETE, FAIL, "can't broadcast file deletion status");
            }

            break;
        } /* H5VL_FILE_DELETE */

        /* Check if two files are the same */
        case H5VL_FILE_IS_EQUAL: {
            H5_daos_file_t *file2    = (H5_daos_file_t *)specific_args->args.is_equal.obj2;
            hbool_t        *is_equal = specific_args->args.is_equal.same_file;

            if (file2->item.type != H5I_FILE)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "object is not a file!");

            if (!file || !file2)
                *is_equal = FALSE;
            else
                *is_equal = (strcmp(file->cont, file2->cont) == 0);
            break;
        } /* H5VL_FILE_IS_EQUAL */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation");
    } /* end switch */

done:
    if (int_req) {
        H5_daos_op_pool_type_t  op_type;
        H5_daos_op_pool_scope_t op_scope;

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary.  For flush operations
         * use WRITE_ORDERED so all previous operations complete before the
         * flush and all subsequent operations start after the flush (this is
         * where we implement the barrier semantics for flush). */
        if (specific_args->op_type == H5VL_FILE_FLUSH) {
            op_type = H5_DAOS_OP_TYPE_WRITE_ORDERED;
        }
        else {
            assert(specific_args->op_type == H5VL_FILE_DELETE);
            op_type = H5_DAOS_OP_TYPE_WRITE;
        }

        op_scope = item ? H5_DAOS_OP_SCOPE_FILE : H5_DAOS_OP_SCOPE_GLOB;

        if (H5_daos_req_enqueue(int_req, first_task, file ? &file->item : NULL, op_type, op_scope, FALSE,
                                !req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, FAIL,
                             "file specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* else */
    }     /* end if */

    if (H5_daos_comm_info_free(&file_delete_comm, &file_delete_info) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't free MPI communicator and info objects");

    D_FUNC_LEAVE_API;
} /* end H5_daos_file_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_delete
 *
 * Purpose:     Creates asynchronous tasks to delete a DUNS path/DAOS
 *              container.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_delete(const char *file_path, H5_daos_acc_params_t *file_acc_params, hbool_t ignore_missing,
                    herr_t *delete_status, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_cont_op_info_t *destroy_udata = NULL;
    herr_t                  ret_value     = SUCCEED;

    assert(file_path);
    assert(file_acc_params);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (destroy_udata = (H5_daos_cont_op_info_t *)DV_malloc(sizeof(H5_daos_cont_op_info_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for file deletion task");
    destroy_udata->op_type             = H5_DAOS_CONT_DESTROY;
    destroy_udata->req                 = req;
    destroy_udata->poh                 = &destroy_udata->u.cont_delete_info.cont_poh;
    destroy_udata->cont_op_metatask    = NULL;
    destroy_udata->path                = file_path;
    destroy_udata->flags               = 0;
    destroy_udata->ignore_missing_path = ignore_missing;
    memset(&destroy_udata->duns_attr, 0, sizeof(struct duns_attr_t));
    destroy_udata->duns_attr.da_type                = DAOS_PROP_CO_LAYOUT_HDF5;
    destroy_udata->u.cont_delete_info.delete_status = delete_status;
    destroy_udata->u.cont_delete_info.facc_params   = *file_acc_params;

    if (strlen(file_acc_params->pool) == 0) {
        if (!H5_daos_bypass_duns_g) {
            /* Create task to resolve given pathname */
            if (H5_daos_duns_resolve_path(destroy_udata, NULL, H5_daos_duns_resolve_path_comp_cb, req,
                                          first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't resolve DUNS path");
        }
        else
            D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "pool is NULL");
    }

    /* Hash file name to create container uuid if bypassing DUNS */
    if (H5_daos_bypass_duns_g) {
        uuid_t uuid;

        H5_daos_hash128(file_path, &uuid);
        uuid_unparse(uuid, destroy_udata->u.cont_delete_info.cont);
    }

    /* Create tasks to connect to container's pool and destroy the DUNS path/DAOS container. */
    if (H5_daos_pool_connect(&destroy_udata->u.cont_delete_info.facc_params, DAOS_PC_RW, destroy_udata->poh,
                             NULL, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to connect to container's pool");

    if (!H5_daos_bypass_duns_g) {
        if (H5_daos_duns_destroy_path(destroy_udata, NULL, H5_daos_duns_destroy_path_comp_cb, req, first_task,
                                      dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy DUNS path");
    }
    else {
        if (H5_daos_get_cont_destroy_task(destroy_udata, H5_daos_cont_destroy_prep_cb,
                                          H5_daos_cont_destroy_comp_cb, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy container");
    }

    if (H5_daos_pool_disconnect(destroy_udata->poh, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to disconnect from container's pool");

    /* Free private data after pool disconnect succeeds */
    if (H5_daos_free_async(destroy_udata, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to free file deletion task data");

    /* Relinquish control of DUNS path destroy udata */
    destroy_udata = NULL;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        destroy_udata = DV_free(destroy_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_file_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_delete_status_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast to broadcast
 *              the status of a file deletion operation to other ranks from
 *              rank 0.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_file_delete_status_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for file deletion status broadcast task");

    assert(udata->req);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast file deletion status";
    } /* end if */
    else if (*((herr_t *)udata->buffer) < 0) {
        /* Handle lead rank failing during file deletion */
        if (udata->req->file->my_rank != 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, -H5_DAOS_REMOTE_ERROR,
                         "lead process failed to delete file");
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "MPI_Ibcast file deletion status completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_metatask) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_metatask, ret_value);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_file_delete_bcast_comp_cb() */

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
H5_daos_get_cont_destroy_task(H5_daos_cont_op_info_t *cont_op_info, tse_task_cb_t prep_cb,
                              tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                              tse_task_t **dep_task)
{
    tse_task_t *destroy_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(cont_op_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for container destroy */
    if (0 != (ret = daos_task_create(DAOS_OPC_CONT_DESTROY, &H5_daos_glob_sched_g, *dep_task ? 1 : 0,
                                     *dep_task ? dep_task : NULL, &destroy_task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy container: %s",
                     H5_daos_err_to_string(ret));

    /* Set callback functions for container destroy */
    if (prep_cb || comp_cb)
        if (0 != (ret = tse_task_register_cbs(destroy_task, prep_cb, NULL, 0, comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL,
                         "can't register callbacks for task to destroy container: %s",
                         H5_daos_err_to_string(ret));

    /* Set private data for container destroy */
    (void)tse_task_set_priv(destroy_task, cont_op_info);

    /* Schedule container destroy task (or save it to be scheduled later) and give
     * it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(destroy_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to destroy container: %s",
                         H5_daos_err_to_string(ret));
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
H5_daos_duns_destroy_path(H5_daos_cont_op_info_t *destroy_udata, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                          H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *destroy_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(destroy_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for DUNS path/container destroy */
    if (H5_daos_create_task(H5_daos_duns_destroy_path_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            prep_cb, comp_cb, destroy_udata, &destroy_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to destroy DUNS path");

    /* Schedule DUNS path/container destroy task (or save it to be scheduled later)
     * and give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(destroy_task, false)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to destroy DUNS path: %s",
                         H5_daos_err_to_string(ret));
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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path destroy task");

    assert(udata->req);
    assert(udata->path);
    assert(udata->poh);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DUNS path destroy task");

    assert(udata->req);

    if (task->dt_result != 0 && task->dt_result != -H5_DAOS_SHORT_CIRCUIT) {
        /* DSINC - DER_INVAL and DER_NONEXIST do not need to be checked against with the latest DAOS master.
         */
        if (udata->ignore_missing_path && ((-DER_NONEXIST == task->dt_result) ||
                                           (-DER_INVAL == task->dt_result) || (ENOENT == task->dt_result)))
            D_GOTO_DONE(0); /* Short-circuit success for H5F_ACC_TRUNC access when file is missing */
        else if (ENODATA == task->dt_result)
            D_GOTO_ERROR(H5E_FILE, H5E_NOTHDF5, -H5_DAOS_BAD_VALUE, "file '%s' is not a valid HDF5 DAOS file",
                         udata->path);
        else if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = task->dt_result;
            udata->req->failed_task = "DUNS path destroy";
        } /* end else */
    }     /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DUNS path destroy completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Set status of container deletion */
        if ((udata->op_type == H5_DAOS_CONT_DESTROY) && udata->u.cont_delete_info.delete_status)
            *udata->u.cont_delete_info.delete_status = (ret_value == 0) ? SUCCEED : FAIL;
    } /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
    daos_cont_destroy_t    *destroy_args;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for container destroy task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_FILE);

    assert(udata->poh);

    /* Set daos_cont_destroy task args */
    if (NULL == (destroy_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for container destroy task");
    memset(destroy_args, 0, sizeof(*destroy_args));
    destroy_args->poh   = *udata->poh;
    destroy_args->force = 1;
    if (udata->op_type == H5_DAOS_CONT_DESTROY)
#if CHECK_DAOS_API_VERSION(1, 4)
        destroy_args->cont = udata->u.cont_delete_info.cont;
#else
        uuid_unparse(udata->u.cont_delete_info.cont, destroy_args->uuid);
#endif
    else
#if CHECK_DAOS_API_VERSION(1, 4)
        destroy_args->cont = udata->req->file->cont;
#else
        uuid_unparse(udata->req->file->cont, destroy_args->uuid);
#endif

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for DAOS container destroy task");

    assert(udata->req);

    /* Handle errors in daos_cont_destroy task.  Only record error in udata->req_status
     * if it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT &&
        !(udata->ignore_missing_path && task->dt_result == -DER_NONEXIST)) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "DAOS container destroy";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "DAOS container destroy completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Set status of container deletion */
        if ((udata->op_type == H5_DAOS_CONT_DESTROY) && udata->u.cont_delete_info.delete_status)
            *udata->u.cont_delete_info.delete_status = (ret_value == 0) ? SUCCEED : FAIL;
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_cont_destroy_comp_cb() */

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
herr_t
H5_daos_file_close_helper(H5_daos_file_t *file)
{
    int    ret;
    herr_t ret_value = SUCCEED;

    assert(file && (H5I_FILE == file->item.type));

    /* Decrement rc */
    if (--file->item.rc == 0) {
        /* Free file data structures */
        if (file->item.cur_op_pool)
            H5_daos_op_pool_free(file->item.cur_op_pool);
        assert(file->item.open_req == NULL);
        if (file->file_name)
            file->file_name = DV_free(file->file_name);
        if (file->def_plist_cache.plist_buffer)
            file->def_plist_cache.plist_buffer = DV_free(file->def_plist_cache.plist_buffer);
        if (H5_daos_comm_info_free(&file->comm, &file->info) < 0)
            D_DONE_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL,
                         "failed to free copy of MPI communicator and info");
        if (file->fapl_id != H5I_INVALID_HID && file->fapl_id != H5P_FILE_ACCESS_DEFAULT)
            if (H5Idec_ref(file->fapl_id) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close fapl");
        if (!daos_handle_is_inval(file->glob_md_oh))
            if (0 != (ret = daos_obj_close(file->glob_md_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close global metadata object: %s",
                             H5_daos_err_to_string(ret));
        assert(file->root_grp == NULL);
        if (!daos_handle_is_inval(file->coh))
            if (0 != (ret = daos_cont_close(file->coh, NULL /*event*/)))
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't close container: %s",
                             H5_daos_err_to_string(ret));
        if (!daos_handle_is_inval(file->container_poh)) {
            if (0 != (ret = daos_pool_disconnect(file->container_poh, NULL)))
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't disconnect from container's pool: %s",
                             H5_daos_err_to_string(ret));
            file->container_poh = DAOS_HDL_INVAL;
        }
        H5FL_FREE(H5_daos_file_t, file);
    } /* end if */
    else if (file->item.rc == 1) {
        /* Only the open request holds a reference, free the file's reference to
         * it.  Clear file's req first so if this function is reentered through
         * H5_daos_req_free_int it doesn't trigger an assertion or try to free
         * the req again. */
        if (file->item.open_req) {
            H5_daos_req_t *tmp_req = file->item.open_req;

            file->item.open_req = NULL;
            if (H5_daos_req_free_int(tmp_req) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end if */

        /* Close the root group so the root group releases its reference to the
         * file's open request.  Clear file's root_grp pointer first so if this
         * function is reentered through H5_daos_group_close_real it doesn't
         * trigger an assertion or try to close the root group again. */
        if (file->root_grp) {
            H5_daos_group_t *tmp_root_grp = file->root_grp;

            file->root_grp = NULL;
            if (H5_daos_group_close_real(tmp_root_grp) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close root group");
        } /* end if */
    }     /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_file_close_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_close_barrier_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibarrier for file
 *              closes.
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
H5_daos_file_close_barrier_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_req_t  *req;
    H5_daos_file_t *file;
    int             ret_value = 0;

    /* Get private data */
    if (NULL == (req = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for file close barrier task");

    assert(req);
    assert(req->file);
    file = req->file;

    /* Handle errors in barrier task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = task->dt_result;
        req->failed_task = "MPI_Ibarrier";
    } /* end if */

    /* Remove req's reference to file, so it can be closed before the file close
     * request finishes.  This prevents the file from being held open if, for
     * example, the application calls H5Fclose_async() but doesn't call
     * H5ESwait() for a while. */
    req->file = NULL;
    if (H5_daos_file_close_helper(file) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close file");

    /* Release the API/ID's reference to file.  Always close file even if
     * something failed */
    if (H5_daos_file_close_helper(file) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close file");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except
     * for H5_daos_req_free_int, which updates req->status if it sees an
     * error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        req->status      = ret_value;
        req->failed_task = "MPI_Ibarrier completion callback";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

done:
    return ret_value;
} /* end H5_daos_file_close_barrier_comp_cb() */

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
H5_daos_file_close(void *_file, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_file_t *file         = (H5_daos_file_t *)_file;
    tse_task_t     *barrier_task = NULL;
    tse_task_t     *first_task   = NULL;
    tse_task_t     *dep_task     = NULL;
    H5_daos_req_t  *int_req      = NULL;
    int             ret;
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");
    if (H5I_FILE != file->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");

    /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
    if (NULL == (int_req = H5_daos_req_create(file, "file close", file->item.open_req, NULL, NULL,
                                              H5P_DATASET_XFER_DEFAULT)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Create task for barrier (or just close if there is only one process) */
    if (H5_daos_create_task(file->num_procs > 1 ? H5_daos_mpi_ibarrier_task : H5_daos_metatask_autocomplete,
                            0, NULL, NULL, H5_daos_file_close_barrier_comp_cb, int_req, &barrier_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create MPI barrier task");

    /* Save task to be scheduled later and give it a reference to req */
    assert(!first_task);
    first_task = barrier_task;
    dep_task   = barrier_task;
    /* No need to take a reference to file here since the purpose is to release
     * the API's reference */
    int_req->rc++;

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the file's request queue.  This will add the
         * dependency on the file open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &file->item, H5_DAOS_OP_TYPE_CLOSE,
                                H5_DAOS_OP_SCOPE_FILE, TRUE, !req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't add request to request queue");
        file = NULL;

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CANTOPERATE, FAIL, "file close failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

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
H5_daos_file_flush(H5_daos_file_t H5VL_DAOS_UNUSED *file, H5_daos_req_t H5VL_DAOS_UNUSED *req,
                   tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *barrier_task = NULL;
    herr_t      ret_value    = SUCCEED; /* Return value */

    assert(file);

    /* Create task that does nothing but complete itself.  Only necessary
     * because we can't enqueue a request that has no tasks */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &barrier_task) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't create barrier task for file flush");

    /* Schedule barrier task (or save it to be scheduled later)  */
    assert(!*first_task);
    *first_task = barrier_task;
    *dep_task   = barrier_task;

#if 0
    /* Collectively determine if anyone requested a snapshot of the epoch */
    if(MPI_SUCCESS != MPI_Reduce(file->my_rank == 0 ? MPI_IN_PLACE : &file->snap_epoch, &file->snap_epoch, 1, MPI_INT, MPI_LOR, 0, file->facc_params.comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "failed to determine whether to take snapshot (MPI_Reduce)");

    /* Barrier on all ranks so we don't commit before all ranks are
     * finished writing. H5Fflush must be called collectively. */
    if(MPI_SUCCESS != MPI_Barrier(file->facc_params.comm))
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
    char   *oclass_str = NULL;
    hbool_t collective_md_read;
    hbool_t collective_md_write;
    htri_t  prop_exists;
    herr_t  ret_value = SUCCEED;

    assert(file);

    /* Set initial collective metadata I/O settings for the file, then
     * determine if they are to be overridden from a specific setting
     * on the FAPL.
     */
    file->fapl_cache.is_collective_md_read = collective_md_read = FALSE;
    file->fapl_cache.is_collective_md_write = collective_md_write = TRUE;
    H5_DAOS_GET_METADATA_IO_MODES(file, fapl_id, H5P_FILE_ACCESS_DEFAULT, collective_md_read,
                                  collective_md_write, H5E_FILE, FAIL);
    file->fapl_cache.is_collective_md_read  = collective_md_read;
    file->fapl_cache.is_collective_md_write = collective_md_write;

    /* set default object class from env variable first */
    oclass_str = getenv("HDF5_DAOS_OBJ_CLASS");
    if (oclass_str) {
        file->fapl_cache.default_object_class = (daos_oclass_id_t)daos_oclass_name2id(oclass_str);
    }
    else {
        file->fapl_cache.default_object_class = OC_UNKNOWN;
    }

    /* Check for file default object class set on fapl_id */
    /* Note we do not copy the oclass_str in the property callbacks (there is no
     * "get" callback, so this is more like an H5P_peek, and we do not need to
     * free oclass_str as it points directly into the plist value */
    if ((prop_exists = H5Pexist(fapl_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property");
    if (prop_exists) {
        if (H5Pget(fapl_id, H5_DAOS_OBJ_CLASS_NAME, &oclass_str) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class");
        if (oclass_str && (oclass_str[0] != '\0'))
            if (OC_UNKNOWN ==
                (file->fapl_cache.default_object_class = (daos_oclass_id_t)daos_oclass_name2id(oclass_str)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unknown object class");
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_fill_fapl_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_fill_enc_plist_cache
 *
 * Purpose:     Fills the "def_plist_cache" field of the file struct, using
 *              the file's FAPL. This cache holds encoded buffers for
 *              several default property lists to avoid overhead from
 *              H5Pencode.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_fill_enc_plist_cache(H5_daos_file_t *file, hid_t fapl_id)
{
    size_t cur_buf_idx;
    char  *plist_buffer = NULL;
    herr_t ret_value    = SUCCEED;

    assert(file);

    /* Determine sizes of various property list buffers */
    if (H5Pencode2(H5P_FILE_CREATE_DEFAULT, NULL, &file->def_plist_cache.fcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of FCPL");
    if (H5Pencode2(H5P_DATASET_CREATE_DEFAULT, NULL, &file->def_plist_cache.dcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of DCPL");
    if (H5Pencode2(H5P_GROUP_CREATE_DEFAULT, NULL, &file->def_plist_cache.gcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of GCPL");
    if (H5Pencode2(H5P_DATATYPE_CREATE_DEFAULT, NULL, &file->def_plist_cache.tcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of TCPL");
    if (H5Pencode2(H5P_MAP_CREATE_DEFAULT, NULL, &file->def_plist_cache.mcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of MCPL");
    if (H5Pencode2(H5P_ATTRIBUTE_CREATE_DEFAULT, NULL, &file->def_plist_cache.acpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of ACPL");

    /* Allocate single buffer to hold all encoded plists */
    file->def_plist_cache.buffer_size = file->def_plist_cache.fcpl_size + file->def_plist_cache.dcpl_size +
                                        file->def_plist_cache.gcpl_size + file->def_plist_cache.tcpl_size +
                                        file->def_plist_cache.mcpl_size + file->def_plist_cache.acpl_size;
    if (NULL == (file->def_plist_cache.plist_buffer = DV_calloc(file->def_plist_cache.buffer_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate encoded property list cache buffer");
    plist_buffer = (char *)file->def_plist_cache.plist_buffer;

    /* Encode FCPL */
    cur_buf_idx                    = 0;
    file->def_plist_cache.fcpl_buf = (void *)plist_buffer;
    if (H5Pencode2(H5P_FILE_CREATE_DEFAULT, file->def_plist_cache.fcpl_buf, &file->def_plist_cache.fcpl_size,
                   fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't serialize FCPL");

    /* Encode DCPL */
    cur_buf_idx += file->def_plist_cache.fcpl_size;
    file->def_plist_cache.dcpl_buf = (void *)&(plist_buffer[cur_buf_idx]);
    if (H5Pencode2(H5P_DATASET_CREATE_DEFAULT, file->def_plist_cache.dcpl_buf,
                   &file->def_plist_cache.dcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't serialize DCPL");

    /* Encode GCPL */
    cur_buf_idx += file->def_plist_cache.dcpl_size;
    file->def_plist_cache.gcpl_buf = (void *)&(plist_buffer[cur_buf_idx]);
    if (H5Pencode2(H5P_GROUP_CREATE_DEFAULT, file->def_plist_cache.gcpl_buf, &file->def_plist_cache.gcpl_size,
                   fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't serialize GCPL");

    /* Encode TCPL */
    cur_buf_idx += file->def_plist_cache.gcpl_size;
    file->def_plist_cache.tcpl_buf = (void *)&(plist_buffer[cur_buf_idx]);
    if (H5Pencode2(H5P_DATATYPE_CREATE_DEFAULT, file->def_plist_cache.tcpl_buf,
                   &file->def_plist_cache.tcpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't serialize TCPL");

    /* Encode MCPL */
    cur_buf_idx += file->def_plist_cache.tcpl_size;
    file->def_plist_cache.mcpl_buf = (void *)&(plist_buffer[cur_buf_idx]);
    if (H5Pencode2(H5P_MAP_CREATE_DEFAULT, file->def_plist_cache.mcpl_buf, &file->def_plist_cache.mcpl_size,
                   fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't serialize MCPL");

    /* Encode ACPL */
    cur_buf_idx += file->def_plist_cache.mcpl_size;
    file->def_plist_cache.acpl_buf = (void *)&(plist_buffer[cur_buf_idx]);
    if (H5Pencode2(H5P_ATTRIBUTE_CREATE_DEFAULT, file->def_plist_cache.acpl_buf,
                   &file->def_plist_cache.acpl_size, fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't serialize ACPL");

done:
    if (ret_value < 0) {
        if (file->def_plist_cache.plist_buffer)
            file->def_plist_cache.plist_buffer = DV_free(file->def_plist_cache.plist_buffer);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_fill_enc_plist_cache() */

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
    H5_daos_obj_t         *cur_obj     = NULL;
    ssize_t                connector_name_len;
    char                   connector_name[H5_DAOS_CONNECTOR_NAME_LEN + 1];
    herr_t                 ret_value = H5_ITER_CONT;

    /* Ensure that the ID represents a DAOS VOL object */
    H5E_BEGIN_TRY
    {
        connector_name_len = H5VLget_connector_name(id, connector_name, H5_DAOS_CONNECTOR_NAME_LEN + 1);
    }
    H5E_END_TRY;

    /* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
    if (connector_name_len < 0)
        D_GOTO_DONE(H5_ITER_CONT);

    if (!strncmp(H5_DAOS_CONNECTOR_NAME, connector_name, H5_DAOS_CONNECTOR_NAME_LEN)) {
        if (NULL == (cur_obj = (H5_daos_obj_t *)H5VLobject(id)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID");

        if (!strcmp(cur_obj->item.file->cont, count_udata->file_id))
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
    H5_daos_obj_t       *cur_obj  = NULL;
    ssize_t              connector_name_len;
    char                 connector_name[H5_DAOS_CONNECTOR_NAME_LEN + 1];
    herr_t               ret_value = H5_ITER_CONT;

    /* Ensure that the ID represents a DAOS VOL object */
    H5E_BEGIN_TRY
    {
        connector_name_len = H5VLget_connector_name(id, connector_name, H5_DAOS_CONNECTOR_NAME_LEN + 1);
    }
    H5E_END_TRY;

    /* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
    if (connector_name_len < 0)
        D_GOTO_DONE(H5_ITER_CONT);

    if (!strncmp(H5_DAOS_CONNECTOR_NAME, connector_name, H5_DAOS_CONNECTOR_NAME_LEN)) {
        if (NULL == (cur_obj = (H5_daos_obj_t *)H5VLobject(id)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID");

        if (id_udata->obj_count < id_udata->max_objs) {
            if (!strcmp(cur_obj->item.file->cont, id_udata->file_id))
                id_udata->oid_list[id_udata->obj_count++] = id;
        }
        else
            ret_value = H5_ITER_STOP;
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_get_obj_ids_callback() */
