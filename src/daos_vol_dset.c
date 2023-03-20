/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library. Dataset routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

/****************/
/* Local Macros */
/****************/

#define H5_DAOS_DINFO_BCAST_BUF_SIZE                                                                         \
    (H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE +    \
     6 * H5_DAOS_ENCODED_UINT64_T_SIZE)

/* Definitions for chunking code */
#define H5_DAOS_DEFAULT_NUM_SEL_CHUNKS 64
#define H5O_LAYOUT_NDIMS               (H5S_MAX_RANK + 1)
#define CHUNK_DKEY_BUF_SIZE            (1 + (sizeof(uint64_t) * H5S_MAX_RANK))

/* Definitions for automatic chunking */
/* Maximum size for contiguous datasets (target size * sqrt(2)) */
#define H5_DAOS_MAX_CONTIG_SIZE ((uint64_t)((double)H5_daos_chunk_target_size_g * 1.41421356237))
/* Minimum chunk size (target size * sqrt(2)/2) */
#define H5_DAOS_MIN_CHUNK_SIZE ((uint64_t)((double)H5_daos_chunk_target_size_g * 1.41421356237 / 2.))

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Udata type for H5Dscatter callback */
typedef struct H5_daos_scatter_cb_ud_t {
    void  *buf;
    size_t len;
} H5_daos_scatter_cb_ud_t;

/* Udata type for memory space H5Diterate callback */
typedef struct {
    daos_iod_t     *iods;
    daos_sg_list_t *sgls;
    daos_iov_t     *sg_iovs;
    hbool_t         is_vl_str;
    size_t          base_type_size;
    uint64_t        offset;
    uint64_t        idx;
} H5_daos_vl_mem_ud_t;

/* Udata type for file space H5Diterate callback */
typedef struct {
    uint8_t   **akeys;
    daos_iod_t *iods;
    uint64_t    idx;
} H5_daos_vl_file_ud_t;

/* Typedef for function to perform I/O on a single chunk */
typedef herr_t (*H5_daos_chunk_io_func)(H5_daos_select_chunk_info_t *chunk_info, H5_daos_dset_t *dset,
                                        uint64_t dset_ndims, hid_t mem_type_id, H5_daos_io_type_t io_type,
                                        void *buf, H5_daos_req_t *req, tse_task_t **first_task,
                                        tse_task_t **dep_task);

/* Task user data for raw data I/O on a single chunk */
typedef struct H5_daos_chunk_io_ud_t {
    H5_daos_req_t  *req;
    H5_daos_dset_t *dset;
    daos_key_t      dkey;
    uint8_t         dkey_buf[CHUNK_DKEY_BUF_SIZE];
    uint8_t         akey_buf;
    daos_iod_t      iod;
    daos_sg_list_t  sgl;
    daos_recx_t     recx;
    daos_recx_t    *recxs;
    daos_iov_t      sg_iov;
    daos_iov_t     *sg_iovs;

    /* Fields used for datatype conversion */
    struct {
        hssize_t              num_elem;
        hid_t                 mem_type_id;
        hid_t                 mem_space_id;
        void                 *buf;
        H5_daos_io_type_t     io_type;
        H5_daos_tconv_reuse_t reuse;
        hbool_t               fill_bkg;
        size_t                mem_type_size;
        size_t                file_type_size;
        void                 *tconv_buf;
        void                 *bkg_buf;
    } tconv;
} H5_daos_chunk_io_ud_t;

/* Task user data struct for I/O operations (API level) */
typedef struct H5_daos_io_task_ud_t {
    H5_daos_req_t    *req;
    H5_daos_io_type_t io_type;
    H5_daos_dset_t   *dset;
    hid_t             mem_type_id;
    hid_t             mem_space_id;
    hid_t             file_space_id;
    union {
        void       *rbuf;
        const void *wbuf;
    } buf;
    tse_task_t *end_task;
} H5_daos_io_task_ud_t;

/* Task user data struct for get operations */
typedef struct H5_daos_dset_get_ud_t {
    H5_daos_req_t     *req;
    H5_daos_dset_t    *dset;
    H5VL_dataset_get_t get_type;
    hsize_t           *hsize_out;
} H5_daos_dset_get_ud_t;

/* Task user data struct for set extent operations */
typedef struct H5_daos_dset_set_extent_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud;
    hid_t                 new_space_id;
} H5_daos_dset_set_extent_ud_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_dset_fill_dcpl_cache(H5_daos_dset_t *dset);
static int    H5_daos_fill_val_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_bcast_fill_val(H5_daos_dset_t *dset, H5_daos_req_t *req, size_t fill_val_size,
                                     tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_dset_open_end(H5_daos_dset_t *dset, uint8_t *p, uint64_t type_buf_len,
                                    uint64_t space_buf_len, uint64_t dcpl_buf_len, uint64_t fill_val_len,
                                    hid_t dxpl_id);
static int    H5_daos_dset_open_bcast_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_dset_open_recv_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_dset_fill_io_cache(H5_daos_dset_t *dset, hid_t file_space_id, hid_t mem_space_id);
static int    H5_daos_dinfo_read_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_sel_to_recx_iov(hid_t sel_iter_id, size_t type_size, void *buf, daos_recx_t **recxs,
                                      daos_iov_t **sg_iovs, size_t *list_nused);
static herr_t H5_daos_scatter_cb(const void **src_buf, size_t *src_buf_bytes_used, void *_udata);
static int    H5_daos_chunk_io_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_chunk_io_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_dataset_io_types_equal(H5_daos_select_chunk_info_t *chunk_info, H5_daos_dset_t *dset,
                                             uint64_t dset_ndims, hid_t mem_type_id,
                                             H5_daos_io_type_t io_type, void *buf, H5_daos_req_t *req,
                                             tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_chunk_io_tconv_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_chunk_io_tconv_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_chunk_fill_bkg_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_chunk_fill_bkg_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_dataset_io_types_unequal(H5_daos_select_chunk_info_t *chunk_info, H5_daos_dset_t *dset,
                                               uint64_t dset_ndims, hid_t mem_type_id,
                                               H5_daos_io_type_t io_type, void *buf, H5_daos_req_t *req,
                                               tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_dset_io_int_task(tse_task_t *task);
static int    H5_daos_dset_io_int_end_task(tse_task_t *task);
#if H5VL_VERSION >= 2
static herr_t H5_daos_dataset_get_realize(void *future_object, hid_t *actual_object_id);
static herr_t H5_daos_dataset_get_discard(void *future_object);
#endif
static int    H5_daos_dataset_get_task(tse_task_t *task);
static int    H5_daos_dataset_refresh_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_dset_set_extent_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_dataset_set_extent(H5_daos_dset_t *dset, const hsize_t *size, hbool_t collective,
                                         H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static hid_t  H5_daos_point_and_block(hid_t point_space, hsize_t rank, hsize_t *dims, hsize_t *start,
                                      hsize_t *block);
static herr_t H5_daos_get_selected_chunk_info(H5_daos_dcpl_cache_t *dcpl_cache, hid_t file_space_id,
                                              hid_t mem_space_id, H5_daos_select_chunk_info_t **chunk_info,
                                              size_t *chunk_info_len, size_t *nchunks_selected);

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_fill_dcpl_cache
 *
 * Purpose:     Fills the "dcpl_cache" field of the dataset struct, using
 *              the dataset's DCPL.  Assumes dset->dcpl_cache has been
 *              initialized to all zeros.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dset_fill_dcpl_cache(H5_daos_dset_t *dset)
{
    H5D_fill_time_t fill_time;
    htri_t          is_vl_ref;
    herr_t          ret_value = SUCCEED;

    assert(dset);

    /* Retrieve layout */
    if (dset->dcpl_id == H5P_DATASET_CREATE_DEFAULT)
        dset->dcpl_cache.layout = H5_daos_plist_cache_g->dcpl_cache.layout;
    else if ((dset->dcpl_cache.layout = H5Pget_layout(dset->dcpl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get layout property");

    /* Retrieve chunk dimensions */
    if (dset->dcpl_cache.layout == H5D_CHUNKED)
        if (H5Pget_chunk(dset->dcpl_id, H5S_MAX_RANK, dset->dcpl_cache.chunk_dims) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk dimensions");

    /* Retrieve fill status */
    if (dset->dcpl_id == H5P_DATASET_CREATE_DEFAULT)
        dset->dcpl_cache.fill_status = H5_daos_plist_cache_g->dcpl_cache.fill_status;
    else if (H5Pfill_value_defined(dset->dcpl_id, &dset->dcpl_cache.fill_status) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get fill value status");

    /* Check for vlen or reference */
    if ((is_vl_ref = H5_daos_detect_vl_vlstr_ref(dset->type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check for vl or reference type");

    /* Retrieve fill time */
    if (dset->dcpl_id == H5P_DATASET_CREATE_DEFAULT)
        fill_time = H5_daos_plist_cache_g->dcpl_cache.fill_time;
    else if (H5Pget_fill_time(dset->dcpl_id, &fill_time) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get fill time");

    /* Determine fill method */
    if (fill_time == H5D_FILL_TIME_NEVER) {
        /* Check for fill time never with vl/ref (illegal) */
        if (is_vl_ref)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                         "can't use fill time of NEVER with vlen or reference type");

        /* Never write fill values even if defined */
        dset->dcpl_cache.fill_method = H5_DAOS_NO_FILL;
    } /* end if */
    else if (dset->dcpl_cache.fill_status == H5D_FILL_VALUE_UNDEFINED) {
        /* If the fill value is undefined, must still write zeros for vl/ref,
         * otherwise write nothing */
        if (is_vl_ref)
            dset->dcpl_cache.fill_method = H5_DAOS_ZERO_FILL;
        else
            dset->dcpl_cache.fill_method = H5_DAOS_NO_FILL;
    } /* end if */
    else if (dset->dcpl_cache.fill_status == H5D_FILL_VALUE_DEFAULT)
        /* Always fill with zeros */
        dset->dcpl_cache.fill_method = H5_DAOS_ZERO_FILL;
    else {
        /* Always copy the fill value */
        assert(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED);
        dset->dcpl_cache.fill_method = H5_DAOS_COPY_FILL;
    } /* end else */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_dset_fill_dcpl_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_fill_val_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for dataset
 *              fill value.
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
H5_daos_fill_val_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset fill value broadcast task");

    assert(udata->req);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast fill value";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->obj);
        assert(udata->obj->item.file);
        assert(udata->obj->item.type == H5I_DATASET);
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close dataset */
        if (udata->obj && H5_daos_dataset_close_real((H5_daos_dset_t *)udata->obj) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "MPI_Ibcast fill value completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Do not free the buffer since it's owned by the dataset struct */

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    return ret_value;
} /* end H5_daos_fill_val_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_bcast_fill_val
 *
 * Purpose:     Broadcasts the dataset fill value
 *
 * Return:      Success:        dataset object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              March, 2020
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_bcast_fill_val(H5_daos_dset_t *dset, H5_daos_req_t *req, size_t fill_val_size,
                       tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t              *bcast_task;
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    assert(dset);
    assert(dset->fill_val);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "failed to allocate buffer for MPI broadcast user data");
    bcast_udata->req            = req;
    bcast_udata->obj            = &dset->obj;
    bcast_udata->bcast_metatask = NULL;
    bcast_udata->buffer         = dset->fill_val;
    bcast_udata->buffer_len     = (int)fill_val_size;
    bcast_udata->count          = (int)fill_val_size;
    bcast_udata->comm           = req->file->comm;

    /* Create task for fill value bcast */
    if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 1, dep_task, NULL, H5_daos_fill_val_bcast_comp_cb,
                            bcast_udata, &bcast_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to broadcast fill value");

    /* Schedule bcast task (or save it to be scheduled later) and give it a
     * reference to req and dset */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(bcast_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task for fill value broadcast: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = bcast_task;
    *dep_task = bcast_task;
    req->rc++;
    dset->obj.item.rc++;
    bcast_udata = NULL;

done:
    /* Cleanup on failure */
    if (bcast_udata) {
        assert(ret_value < 0);
        bcast_udata = DV_free(bcast_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_bcast_fill_val() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_create
 *
 * Purpose:     Sends a request to DAOS to create a dataset
 *
 * Return:      Success:        dataset object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_dataset_create(void *_item, const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
                       hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id,
                       hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_item_t *item            = (H5_daos_item_t *)_item;
    H5_daos_dset_t *dset            = NULL;
    H5_daos_obj_t  *target_obj      = NULL;
    H5_daos_req_t  *int_req         = NULL;
    tse_task_t     *first_task      = NULL;
    tse_task_t     *dep_task        = NULL;
    const char     *target_name     = NULL;
    size_t          target_name_len = 0;
    hbool_t         collective      = FALSE;
    char           *path_buf        = NULL;
    int             ret;
    void           *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /* Check for write access */
    if (!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file");

    /*
     * Determine if independent metadata writes have been requested. Otherwise,
     * like HDF5, metadata writes are collective by default.
     */
    H5_DAOS_GET_METADATA_WRITE_MODE(item->file, dapl_id, H5P_DATASET_ACCESS_DEFAULT, collective, H5E_DATASET,
                                    NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "dataset create", item->open_req, NULL, NULL,
                                              H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, 0, NULL /*event*/)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Traverse the path */
    /* Call this on every rank for now so errors are handled correctly.  If/when
     * we add a bcast to check for failure we could only call this on the lead
     * rank. */
    if (name) {
        if (NULL ==
            (target_obj = H5_daos_group_traverse(item, name, lcpl_id, int_req, collective, &path_buf,
                                                 &target_name, &target_name_len, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path");

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        /* Reject invalid object names during object creation - if a name is
         * given it must parse to a link name that can be created */
        if (target_name_len == 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, NULL, "path given does not resolve to a final link name");
    } /* end if */

    /* Create dataset and link to dataset */
    if (NULL == (dset = (H5_daos_dset_t *)H5_daos_dataset_create_helper(
                     item->file, type_id, space_id, dcpl_id, dapl_id, (H5_daos_group_t *)target_obj,
                     target_name, target_name_len, collective, int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dataset");

    /* Set return value */
    ret_value = (void *)dset;

done:
    if (int_req) {
        H5_daos_op_pool_type_t op_type;

        /* Free path_buf if necessary */
        if (path_buf && H5_daos_free_async(path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTFREE, NULL, "can't free path buffer");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Determine operation type - we will add the operation to item's
         * op pool.  If the target_obj might have link creation order tracked
         * and target_obj is not different from item use
         * H5_DAOS_OP_TYPE_WRITE_ORDERED, otherwise use H5_DAOS_OP_TYPE_WRITE.
         * Add to item's pool because that's where we're creating the link.  No
         * need to add to dataset's pool since it's the open request. */
        if (!target_obj || &target_obj->item != item || target_obj->item.type != H5I_GROUP ||
            ((target_obj->item.open_req->status == 0 || target_obj->item.created) &&
             !((H5_daos_group_t *)target_obj)->gcpl_cache.track_corder))
            op_type = H5_DAOS_OP_TYPE_WRITE;
        else
            op_type = H5_DAOS_OP_TYPE_WRITE_ORDERED;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the group open if necessary.  If this is an anonymous
         * create add to the file pool. */
        if (H5_daos_req_enqueue(int_req, first_task, item, op_type,
                                target_obj ? H5_DAOS_OP_SCOPE_OBJ : H5_DAOS_OP_SCOPE_FILE, collective,
                                !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, NULL, "dataset create failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close object");

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value)
        /* Close dataset */
        if (dset && H5_daos_dataset_close_real(dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset");

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_create_helper
 *
 * Purpose:     Performs the actual dataset creation.
 *
 * Return:      Success:        dataset object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_dataset_create_helper(H5_daos_file_t *file, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                              hid_t dapl_id, H5_daos_group_t *parent_grp, const char *name, size_t name_len,
                              hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
                              tse_task_t **dep_task)
{
    H5_daos_md_rw_cb_ud_flex_t *update_cb_ud = NULL;
    H5_daos_dset_t             *dset         = NULL;
    tse_task_t                 *dataset_metatask;
    tse_task_t                 *finalize_deps[3];
    hbool_t                     default_dcpl   = (dcpl_id == H5P_DATASET_CREATE_DEFAULT);
    htri_t                      is_vl_ref      = FALSE;
    hid_t                       tmp_dcpl_id    = H5I_INVALID_HID;
    size_t                      fill_val_size  = 0;
    int                         finalize_ndeps = 0;
    int                         ret;
    void                       *ret_value = NULL;

    assert(file);
    assert(file->flags & H5F_ACC_RDWR);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    /* Allocate the dataset object that is returned to the user */
    if (NULL == (dset = H5FL_CALLOC(H5_daos_dset_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct");
    dset->obj.item.type     = H5I_DATASET;
    dset->obj.item.created  = TRUE;
    dset->obj.item.open_req = req;
    req->rc++;
    dset->obj.item.file             = file;
    dset->obj.item.rc               = 1;
    dset->obj.obj_oh                = DAOS_HDL_INVAL;
    dset->type_id                   = H5I_INVALID_HID;
    dset->file_type_id              = H5I_INVALID_HID;
    dset->space_id                  = H5I_INVALID_HID;
    dset->cur_set_extent_space_id   = H5I_INVALID_HID;
    dset->dcpl_id                   = H5P_DATASET_CREATE_DEFAULT;
    dset->dapl_id                   = H5P_DATASET_ACCESS_DEFAULT;
    dset->io_cache.file_sel_iter_id = H5I_INVALID_HID;
    dset->io_cache.mem_sel_iter_id  = H5I_INVALID_HID;

    /* Set up datatypes, dataspace, property list fields.  Do this earlier
     * because we need some of these things */
    if ((dset->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy datatype");
    if ((dset->file_type_id = H5VLget_file_type(file, H5_DAOS_g, type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to get file datatype");
    if (0 == (dset->file_type_size = H5Tget_size(dset->file_type_id)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get file datatype size");
    if ((dset->space_id = H5Scopy(space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dataspace");
    if (H5Sselect_all(dset->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection");
    if (!default_dcpl && (dset->dcpl_id = H5Pcopy(dcpl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dcpl");
    if ((dapl_id != H5P_DATASET_ACCESS_DEFAULT) && (dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dapl");

    /* Fill DCPL cache */
    if (H5_daos_dset_fill_dcpl_cache(dset) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to fill DCPL cache");

    /* If the layout is contiguous try to automatically change to chunked */
    if (dset->dcpl_cache.layout == H5D_CONTIGUOUS && H5_daos_chunk_target_size_g > 0) {
        int      ndims;
        size_t   type_size = dset->file_type_size;
        uint64_t extent_size;
        hsize_t  extent_dims[H5S_MAX_RANK];
        int      i;

        /* Get dataspace ranks */
        if ((ndims = H5Sget_simple_extent_ndims(space_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get dataspace dimensionality");

        /* Get dataspace extent */
        if (H5Sget_simple_extent_dims(space_id, extent_dims, NULL) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get dataspace dimensions");

        /* Calculate dataset size */
        extent_size = (uint64_t)type_size;
        for (i = 0; i < ndims; i++)
            extent_size *= (uint64_t)extent_dims[i];

        /* If the dataset is larger than the max contig size and there are at
         * least two elements calculate auto chunk size */
        if (extent_size > H5_DAOS_MAX_CONTIG_SIZE && extent_size > type_size) {
            extent_size = (uint64_t)type_size;

            /* Scalar dataspaces have only one element and so (total)
             * extent_size == type size, so they should not get this far */
            assert(ndims > 0);

            /* Loop over dimensions */
            i = ndims;
            do {
                i--;
                extent_size *= (uint64_t)extent_dims[i];

                /* Check if a chunk spanning the full extent in this dimension
                 * is still too small, in this case we need to move up a
                 * dimension */
                if (extent_size < H5_DAOS_MIN_CHUNK_SIZE) {
                    dset->dcpl_cache.chunk_dims[i] = extent_dims[i];
                    assert(i > 0);
                } /* end if */
                else {
                    /* Calculate number of chunks using approximately rounded
                     * division */
                    uint64_t nchunks =
                        extent_size / H5_daos_chunk_target_size_g +
                        (extent_size % H5_daos_chunk_target_size_g > H5_daos_chunk_target_size_g / 3 ? 1 : 0);

                    /* nchunks should be greater than 0 and no greater than the
                     * extent size.  It should not be possible for nchunks to be
                     * 0 at this point since otherwise it would have went into
                     * the other branch above */
                    assert(nchunks > 0);
                    if (nchunks > extent_dims[i])
                        nchunks = extent_dims[i];

                    /* Calculate chunk dimension (rounded up) */
                    dset->dcpl_cache.chunk_dims[i] =
                        ((uint64_t)extent_dims[i] + nchunks - (uint64_t)1) / nchunks;

                    /* Set remaining chunk dims */
                    while (i > 0) {
                        i--;
                        dset->dcpl_cache.chunk_dims[i] = 1;
                    } /* end while */
                }     /* end else */
            } while (i > 0);

            /* Make sure we aren't trying to set chunking on a default DCPL */
            if (default_dcpl) {
                if ((dset->dcpl_id = H5Pcopy(dcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dcpl");
                default_dcpl = FALSE;
            } /* end if */

            /* Set chunk info in DCPL cache and DCPL */
            dset->dcpl_cache.layout = H5D_CHUNKED;
            if (H5Pset_chunk(dset->dcpl_id, ndims, dset->dcpl_cache.chunk_dims) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't set chunk dimensions");
        } /* end if */
    }     /* end if */

    /* Generate dataset oid */
    if (H5_daos_oid_generate(&dset->obj.oid, FALSE, 0, H5I_DATASET,
                             (default_dcpl ? H5P_DEFAULT : dset->dcpl_id), H5_DAOS_OBJ_CLASS_NAME, file,
                             collective, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't generate object id");

    /* Open dataset object */
    if (H5_daos_obj_open(file, req, &dset->obj.oid, DAOS_OO_RW, &dset->obj.obj_oh, "dataset object open",
                         first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset object");

    /* Create dataset and write metadata if this process should */
    if (!collective || (file->my_rank == 0)) {
        size_t      type_size    = 0;
        size_t      space_size   = 0;
        size_t      dcpl_size    = 0;
        void       *type_buf     = NULL;
        void       *space_buf    = NULL;
        void       *dcpl_buf     = NULL;
        void       *fill_val_buf = NULL;
        tse_task_t *update_task;

        /* Determine serialized datatype size */
        if (H5Tencode(type_id, NULL, &type_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype");

        /* Determine serialized dataspace size */
        if (H5Sencode2(space_id, NULL, &space_size, file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataspace");

        /* Actions to take if the DCPL is not the default */
        if (!default_dcpl) {
            /* If there's a vl or reference type fill value set we must copy the
             * DCPL and unset the fill value.  This is a workaround for the bug that
             * prevents deep copying/flattening of encoded fill values.  Even though
             * the encoded value is never used by the connector, when it is replaced
             * on file open with one converted from the explicitly stored fill,
             * the library tries to free the fill value stored in the property list,
             * causing memory errors. */
            if (dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED) {
                fill_val_size = dset->file_type_size;

                if ((is_vl_ref = H5_daos_detect_vl_vlstr_ref(type_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't check for vl or reference type");
                if (is_vl_ref) {
                    if ((tmp_dcpl_id = H5Pcopy(dset->dcpl_id)) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dcpl");
                    if (H5Pset_fill_value(tmp_dcpl_id, dset->type_id, NULL) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't unset fill value");
                } /* end if */
            }     /* end if */

            /* Determine serialized DCPL size */
            if (H5Pencode2(tmp_dcpl_id >= 0 ? tmp_dcpl_id : dset->dcpl_id, NULL, &dcpl_size, file->fapl_id) <
                0)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dcpl");
        } /* end if */
        else
            /* Default DCPL should not have a user defined fill value */
            assert(dset->dcpl_cache.fill_status != H5D_FILL_VALUE_USER_DEFINED);

        /* Create dataset */
        /* Allocate argument struct */
        if (NULL ==
            (update_cb_ud = (H5_daos_md_rw_cb_ud_flex_t *)DV_calloc(
                 sizeof(H5_daos_md_rw_cb_ud_flex_t) + type_size + space_size + dcpl_size + fill_val_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for update callback arguments");

        /* Encode datatype */
        type_buf = update_cb_ud->flex_buf;
        if (H5Tencode(type_id, type_buf, &type_size) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize datatype");

        /* Encode dataspace */
        space_buf = update_cb_ud->flex_buf + type_size;
        if (H5Sencode2(space_id, space_buf, &space_size, file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dataspace");

        /* Encode DCPL if not the default */
        if (!default_dcpl) {
            dcpl_buf = update_cb_ud->flex_buf + type_size + space_size;
            if (H5Pencode2(tmp_dcpl_id >= 0 ? tmp_dcpl_id : dset->dcpl_id, dcpl_buf, &dcpl_size,
                           file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dcpl");
        } /* end if */
        else {
            dcpl_buf  = file->def_plist_cache.dcpl_buf;
            dcpl_size = file->def_plist_cache.dcpl_size;
        } /* end else */

        /* Set up operation to write datatype, dataspace, and DCPL to dataset */
        /* Point to dset */
        update_cb_ud->md_rw_cb_ud.obj = &dset->obj;

        /* Point to req */
        update_cb_ud->md_rw_cb_ud.req = req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        update_cb_ud->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[0].iod_name, H5_daos_type_key_g,
                           H5_daos_type_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_size = (uint64_t)type_size;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[1].iod_name, H5_daos_space_key_g,
                           H5_daos_space_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[1].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[1].iod_size = (uint64_t)space_size;
        update_cb_ud->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[2].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[2].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[2].iod_size = (uint64_t)dcpl_size;
        update_cb_ud->md_rw_cb_ud.iod[2].iod_type = DAOS_IOD_SINGLE;

        update_cb_ud->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[0], type_buf, (daos_size_t)type_size);
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[0];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[1], space_buf, (daos_size_t)space_size);
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[1];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[1]   = FALSE;
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[2], dcpl_buf, (daos_size_t)dcpl_size);
        update_cb_ud->md_rw_cb_ud.sgl[2].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[2].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[2];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[2]   = FALSE;

        /* Set nr */
        update_cb_ud->md_rw_cb_ud.nr = 3u;

        /* Encode fill value if necessary.  Note that H5Pget_fill_value()
         * triggers type conversion and therefore writing any VL blobs to the
         * file. */
        /* Could potentially skip this and use value encoded in dcpl for non-vl/
         * ref types, or for all types once H5Pencode/decode works properly with
         * vl/ref fill values.  Latter would require a different code path for
         * filling in read values after conversion instead of before.  -NAF */
        if (dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED) {
            fill_val_size = dset->file_type_size;

            if (NULL == (dset->fill_val = DV_calloc(fill_val_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fill value");
            if (H5Pget_fill_value(dset->dcpl_id, dset->file_type_id, dset->fill_val) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get fill value");

            /* Copy fill value buffer - only needed because the generic
             * daos_obj_update frees the buffers.  We could avoid this with
             * extra work but this location isn't critical for performance. */
            fill_val_buf = update_cb_ud->flex_buf + type_size + space_size + (default_dcpl ? 0 : dcpl_size);
            (void)memcpy(fill_val_buf, dset->fill_val, fill_val_size);

            /* Set up iod */
            daos_const_iov_set(
                (d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[update_cb_ud->md_rw_cb_ud.nr].iod_name,
                H5_daos_fillval_key_g, H5_daos_fillval_key_size_g);
            update_cb_ud->md_rw_cb_ud.iod[update_cb_ud->md_rw_cb_ud.nr].iod_nr   = 1u;
            update_cb_ud->md_rw_cb_ud.iod[update_cb_ud->md_rw_cb_ud.nr].iod_size = (uint64_t)fill_val_size;
            update_cb_ud->md_rw_cb_ud.iod[update_cb_ud->md_rw_cb_ud.nr].iod_type = DAOS_IOD_SINGLE;

            /* Set up sgl */
            daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[update_cb_ud->md_rw_cb_ud.nr], fill_val_buf,
                         (daos_size_t)fill_val_size);
            update_cb_ud->md_rw_cb_ud.sgl[update_cb_ud->md_rw_cb_ud.nr].sg_nr     = 1;
            update_cb_ud->md_rw_cb_ud.sgl[update_cb_ud->md_rw_cb_ud.nr].sg_nr_out = 0;
            update_cb_ud->md_rw_cb_ud.sgl[update_cb_ud->md_rw_cb_ud.nr].sg_iovs =
                &update_cb_ud->md_rw_cb_ud.sg_iov[update_cb_ud->md_rw_cb_ud.nr];
            update_cb_ud->md_rw_cb_ud.free_sg_iov[update_cb_ud->md_rw_cb_ud.nr] = FALSE;

            /* Adjust nr */
            update_cb_ud->md_rw_cb_ud.nr++;

            /* Broadcast fill value if it contains any vl or reference types and
             * there are other processes that need it.  Needed for vl and
             * reference types because calling H5Pget_fill_value on each process
             * would write a separate vl sequence on each process. */
            if (is_vl_ref && collective && (file->num_procs > 1)) {
                finalize_deps[finalize_ndeps] = *dep_task;
                if (H5_daos_bcast_fill_val(dset, req, fill_val_size, first_task,
                                           &finalize_deps[finalize_ndeps]) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't broadcast fill value");
                finalize_ndeps++;
            } /* end if */
        }     /* end if */

        /* Set task name */
        update_cb_ud->md_rw_cb_ud.task_name = "dataset metadata write";

        /* Create task for dataset metadata write */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_md_update_comp_cb, update_cb_ud, &update_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to write dataset metadata");

        /* Schedule dataset metadata write task and give it a reference to req
         * and the dataset */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to write dataset metadata: %s",
                         H5_daos_err_to_string(ret));
        req->rc++;
        dset->obj.item.rc++;
        update_cb_ud = NULL;

        /* Add dependency for finalize task */
        finalize_deps[finalize_ndeps] = update_task;
        finalize_ndeps++;

        /* Create link to dataset */
        if (parent_grp) {
            H5_daos_link_val_t link_val;

            link_val.type                 = H5L_TYPE_HARD;
            link_val.target.hard          = dset->obj.oid;
            link_val.target_oid_async     = &dset->obj.oid;
            finalize_deps[finalize_ndeps] = *dep_task;
            if (0 != (ret = H5_daos_link_write(parent_grp, name, name_len, &link_val, req, first_task,
                                               &finalize_deps[finalize_ndeps])))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create link to dataset: %s",
                             H5_daos_err_to_string(ret));
            finalize_ndeps++;
        } /* end if */
        else {
            /* No link to dataset, write a ref count of 0 to dset */
            finalize_deps[finalize_ndeps] = *dep_task;
            if (0 != (ret = H5_daos_obj_write_rc(NULL, &dset->obj, NULL, 0, req, first_task,
                                                 &finalize_deps[finalize_ndeps])))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't write object ref count: %s",
                             H5_daos_err_to_string(ret));
            finalize_ndeps++;
        } /* end if */
    }     /* end if */
    else {
        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast though it
         * could only be an issue with dataset reopen so we'll skip it for now.
         * There is probably never an issue with file reopen since all commits
         * are from process 0, same as the dataset create above. */

        /* Handle fill value */
        if (dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED) {
            fill_val_size = dset->file_type_size;

            if (NULL == (dset->fill_val = DV_malloc(fill_val_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fill value");

            /* If there's a vl or reference type, receive fill value from lead
             * process above (see note above), otherwise just retrieve from DCPL
             */
            if ((is_vl_ref = H5_daos_detect_vl_vlstr_ref(type_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't check for vl or reference type");
            if (is_vl_ref) {
                finalize_deps[finalize_ndeps] = *dep_task;
                if (H5_daos_bcast_fill_val(dset, req, fill_val_size, first_task,
                                           &finalize_deps[finalize_ndeps]) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTRECV, NULL, "can't broadcast fill value");
                finalize_ndeps++;
            } /* end if */
            else if (H5Pget_fill_value(dset->dcpl_id, dset->file_type_id, dset->fill_val) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get fill value");
        } /* end if */

        /* Check for only dep_task created, register it as the finalize
         * dependency if so */
        if (*dep_task && finalize_ndeps == 0) {
            finalize_deps[0] = *dep_task;
            finalize_ndeps   = 1;
        } /* end if */
    }     /* end else */

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&dset->obj, dset->dcpl_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to fill OCPL cache");

    ret_value = (void *)dset;

done:
    /* Create metatask to use for dependencies on this dataset create */
    if (H5_daos_create_task(
            H5_daos_metatask_autocomplete, (finalize_ndeps > 0) ? (unsigned)finalize_ndeps : 0,
            (finalize_ndeps > 0) ? finalize_deps : NULL, NULL, NULL, NULL, &dataset_metatask) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create meta task for dataset create");
    /* Schedule dataset metatask (or save it to be scheduled later) */
    else {
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(dataset_metatask, false)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule dataset meta task: %s",
                             H5_daos_err_to_string(ret));
            else
                *dep_task = dataset_metatask;
        } /* end if */
        else {
            *first_task = dataset_metatask;
            *dep_task   = dataset_metatask;
        } /* end else */

        if (collective && (file->num_procs > 1))
            if (H5_daos_collective_error_check(&dset->obj, req, first_task, dep_task) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't perform collective error check");
    } /* end else */

    /* Close temporary DCPL */
    if (tmp_dcpl_id >= 0 && H5Pclose(tmp_dcpl_id) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close temporary DCPL");

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value) {
        /* Close dataset */
        if (dset && H5_daos_dataset_close_real(dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset");

        /* Free memory */
        if (update_cb_ud && update_cb_ud->md_rw_cb_ud.obj &&
            H5_daos_object_close(&update_cb_ud->md_rw_cb_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close object");
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    assert(!update_cb_ud);

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_create_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_open_end
 *
 * Purpose:     Decode serialized dataset info from a buffer and fill
 *              caches.
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
H5_daos_dset_open_end(H5_daos_dset_t *dset, uint8_t *p, uint64_t type_buf_len, uint64_t space_buf_len,
                      uint64_t dcpl_buf_len, uint64_t fill_val_len, hid_t dxpl_id)
{
    void *tconv_buf = NULL;
    void *bkg_buf   = NULL;
    int   ret_value = 0;

    assert(dset);
    assert(p);
    assert(type_buf_len > 0);

    /* Decode datatype */
    if ((dset->type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize datatype");
    p += type_buf_len;

    /* Decode dataspace and select all */
    if ((dset->space_id = H5Sdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize dataspace");
    if (H5Sselect_all(dset->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, -H5_DAOS_H5_DECODE_ERROR, "can't change selection");
    p += space_buf_len;

    /* Check if the dataset's DCPL is the default DCPL.
     * Otherwise, decode the dataset's DCPL.
     */
    if ((dcpl_buf_len == dset->obj.item.file->def_plist_cache.dcpl_size) &&
        !memcmp(p, dset->obj.item.file->def_plist_cache.dcpl_buf,
                dset->obj.item.file->def_plist_cache.dcpl_size))
        dset->dcpl_id = H5P_DATASET_CREATE_DEFAULT;
    else if ((dset->dcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize DCPL");

    /* Finish setting up dataset struct */
    if ((dset->file_type_id = H5VLget_file_type(dset->obj.item.file, H5_DAOS_g, dset->type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_H5_TCONV_ERROR, "failed to get file datatype");
    if (0 == (dset->file_type_size = H5Tget_size(dset->file_type_id)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get file datatype size");

    /* Fill DCPL cache */
    if (H5_daos_dset_fill_dcpl_cache(dset) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_CPL_CACHE_ERROR, "failed to fill DCPL cache");

    /* Check for fill value */
    if (fill_val_len > 0) {
        htri_t is_vl_ref;

        /* Copy fill value to dataset struct */
        p += dcpl_buf_len;
        if (NULL == (dset->fill_val = DV_malloc(fill_val_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                         "can't allocate buffer for fill value");
        (void)memcpy(dset->fill_val, p, fill_val_len);

        /* Set fill value in DCPL if it contains a VL or reference.  This is
         * necessary because the code in H5Pencode/decode for fill values does
         * not deep copy or flatten VL sequeneces, so the pointers stored in the
         * property list are invalid once decoded in a different context.  Note
         * this will cause every process to read the same VL sequence(s).  We
         * could remove this code once this feature is properly supported,
         * though once the library supports flattening VL we should consider
         * fundamentally changing how VL types work in this connector.  -NAF */
        if ((is_vl_ref = H5_daos_detect_vl_vlstr_ref(dset->type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, -H5_DAOS_H5_TCONV_ERROR,
                         "can't check for vl or reference type");
        if (is_vl_ref) {
            size_t  fill_val_size;
            size_t  fill_val_mem_size;
            hbool_t fill_bkg = FALSE;

            /* Initialize type conversion */
            if (H5_daos_tconv_init(dset->file_type_id, &fill_val_size, dset->type_id, &fill_val_mem_size, 1,
                                   FALSE, FALSE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_H5_TCONV_ERROR,
                             "can't initialize type conversion");

            /* Sanity check */
            if (fill_val_size != fill_val_len)
                D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                             "size of stored fill value does not match size of datatype");

            /* Copy file type fill value to tconv_buf */
            (void)memcpy(tconv_buf, dset->fill_val, fill_val_size);

            /* Perform type conversion */
            if (H5Tconvert(dset->file_type_id, dset->type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR,
                             "can't perform type conversion");

            /* Set fill value on DCPL */
            if (H5Pset_fill_value(dset->dcpl_id, dset->type_id, tconv_buf) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, -H5_DAOS_H5PSET_ERROR, "can't set fill value");

            /* Patch dcpl_cache because the fill value was cleared in dataset
             * create due to a workaround for a different problem caused by the
             * same bug */
            dset->dcpl_cache.fill_status = H5D_FILL_VALUE_USER_DEFINED;
            dset->dcpl_cache.fill_method = H5_DAOS_COPY_FILL;
        } /* end if */
    }     /* end if */
    else
        /* Check for missing fill value */
        if (dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "fill value defined on property list but not found in metadata");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&dset->obj, dset->dcpl_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache");

done:
    /* Free tconv_buf */
    if (tconv_buf) {
        hid_t scalar_space_id;

        if ((scalar_space_id = H5Screate(H5S_SCALAR)) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_H5_CREATE_ERROR,
                         "can't create scalar dataspace");
        else {
            if (H5Treclaim(dset->type_id, scalar_space_id, dxpl_id, tconv_buf) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTGC, -H5_DAOS_FREE_ERROR,
                             "can't reclaim memory from fill value conversion buffer");
            if (H5Sclose(scalar_space_id) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR,
                             "can't close scalar dataspace");
        } /* end else */
        tconv_buf = DV_free(tconv_buf);
    } /* end if */

    /* Free bkg_buf */
    bkg_buf = DV_free(bkg_buf);

    return ret_value;
} /* end H5_daos_dset_open_end() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_open_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for dataset
 *              opens (rank 0).
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
H5_daos_dset_open_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset info broadcast task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast dataset info";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank == 0);
        assert(udata->bcast_udata.obj->item.type == H5I_DATASET);

        /* Reissue bcast if necessary */
        if (udata->bcast_udata.buffer_len != udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.count == H5_DAOS_DINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.buffer_len > H5_DAOS_DINFO_BCAST_BUF_SIZE);

            /* Use full buffer this time */
            udata->bcast_udata.count = udata->bcast_udata.buffer_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_dset_open_bcast_comp_cb,
                                    udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second dataset info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret,
                             "can't schedule task for second dataset info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
    }     /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close dataset */
        if (udata->bcast_udata.obj &&
            H5_daos_dataset_close_real((H5_daos_dset_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast dataset info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_udata.bcast_metatask, ret_value);

        /* Free buffer */
        if (udata->bcast_udata.buffer != udata->flex_buf)
            DV_free(udata->bcast_udata.buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    return ret_value;
} /* end H5_daos_dset_open_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_open_recv_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for dataset
 *              opens (rank 1+).
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
H5_daos_dset_open_recv_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset info receive task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast dataset info";
    } /* end if */
    else if (task->dt_result == 0) {
        uint64_t type_buf_len  = 0;
        uint64_t space_buf_len = 0;
        uint64_t dcpl_buf_len  = 0;
        uint64_t fill_val_len  = 0;
        size_t   dinfo_len;
        uint8_t *p = udata->bcast_udata.buffer;

        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank > 0);
        assert(udata->bcast_udata.obj->item.type == H5I_DATASET);

        /* Decode oid */
        UINT64DECODE(p, udata->bcast_udata.obj->oid.lo)
        UINT64DECODE(p, udata->bcast_udata.obj->oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_buf_len)
        UINT64DECODE(p, space_buf_len)
        UINT64DECODE(p, dcpl_buf_len)
        UINT64DECODE(p, fill_val_len)

        /* Check for type_buf_len set to 0 - indicates failure */
        if (type_buf_len == 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR,
                         "lead process failed to open dataset");

        /* Calculate data length */
        dinfo_len = (size_t)type_buf_len + (size_t)space_buf_len + (size_t)dcpl_buf_len +
                    (size_t)fill_val_len + 6 * sizeof(uint64_t);

        /* Reissue bcast if necessary */
        if (dinfo_len > (size_t)udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.buffer_len == H5_DAOS_DINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.count == H5_DAOS_DINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.buffer == udata->flex_buf);

            /* Realloc buffer */
            if (NULL == (udata->bcast_udata.buffer = DV_malloc(dinfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "failed to allocate memory for dataset info buffer");
            udata->bcast_udata.buffer_len = (int)dinfo_len;
            udata->bcast_udata.count      = (int)dinfo_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_dset_open_recv_comp_cb,
                                    udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second dataset info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret,
                             "can't schedule task for second dataset info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
        else {
            /* Open dataset */
            if (0 != (ret = daos_obj_open(
                          udata->bcast_udata.obj->item.file->coh, udata->bcast_udata.obj->oid,
                          udata->bcast_udata.obj->item.file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO,
                          &udata->bcast_udata.obj->obj_oh, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, ret, "can't open dataset: %s",
                             H5_daos_err_to_string(ret));

            /* Finish building dataset object */
            if (0 != (ret = H5_daos_dset_open_end((H5_daos_dset_t *)udata->bcast_udata.obj, p, type_buf_len,
                                                  space_buf_len, dcpl_buf_len, fill_val_len,
                                                  udata->bcast_udata.req->dxpl_id)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't finish opening dataset");
        } /* end else */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close dataset */
        if (udata->bcast_udata.obj &&
            H5_daos_dataset_close_real((H5_daos_dset_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast dataset info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_udata.bcast_metatask, ret_value);

        /* Free buffer */
        if (udata->bcast_udata.buffer != udata->flex_buf)
            DV_free(udata->bcast_udata.buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    return ret_value;
} /* end H5_daos_dset_open_recv_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dinfo_read_comp_cb
 *
 * Purpose:     Complete callback for asynchronous metadata fetch for
 *              dataset opens.
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
H5_daos_dinfo_read_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_omd_fetch_ud_t *udata;
    uint8_t                *p;
    int                     ret;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset info read task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->fetch_metatask);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;
        size_t      daos_info_len = udata->md_rw_cb_ud.iod[0].iod_size + udata->md_rw_cb_ud.iod[1].iod_size +
                               udata->md_rw_cb_ud.iod[2].iod_size + udata->md_rw_cb_ud.iod[3].iod_size;

        assert(udata->md_rw_cb_ud.req->file);
        assert(udata->md_rw_cb_ud.obj);
        assert(udata->md_rw_cb_ud.obj->item.type == H5I_DATASET);

        /* Verify iod size makes sense */
        if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_TYPE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[1].iov_buf_len != H5_DAOS_SPACE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[2].iov_buf_len != H5_DAOS_DCPL_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[3].iov_buf_len != H5_DAOS_FILL_VAL_BUF_SIZE)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "buffer length does not match expected value");

        if (udata->bcast_udata) {
            assert(udata->bcast_udata->bcast_udata.buffer == udata->bcast_udata->flex_buf);

            /* Reallocate dataset info buffer if necessary */
            if (daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_DCPL_BUF_SIZE +
                                    H5_DAOS_FILL_VAL_BUF_SIZE) {
                if (NULL == (udata->bcast_udata->bcast_udata.buffer =
                                 DV_malloc(daos_info_len + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized dataset info");
                udata->bcast_udata->bcast_udata.buffer_len =
                    (int)(daos_info_len + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE);
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->bcast_udata->bcast_udata.buffer + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE;
        } /* end if */
        else {
            assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf == udata->flex_buf);

            /* Reallocate dataset info buffer if necessary */
            if (daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_DCPL_BUF_SIZE +
                                    H5_DAOS_FILL_VAL_BUF_SIZE) {
                if (NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(daos_info_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized dataset info");
                udata->md_rw_cb_ud.free_sg_iov[0] = TRUE;
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->md_rw_cb_ud.sg_iov[0].iov_buf;
        } /* end else */

        /* Set up sgl */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], p, udata->md_rw_cb_ud.iod[0].iod_size);
        udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        p += udata->md_rw_cb_ud.iod[0].iod_size;
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[1], p, udata->md_rw_cb_ud.iod[1].iod_size);
        udata->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        p += udata->md_rw_cb_ud.iod[1].iod_size;
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[2], p, udata->md_rw_cb_ud.iod[2].iod_size);
        udata->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        if (udata->md_rw_cb_ud.iod[3].iod_size > 0) {
            p += udata->md_rw_cb_ud.iod[2].iod_size;
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[3], p, udata->md_rw_cb_ud.iod[3].iod_size);
            udata->md_rw_cb_ud.sgl[3].sg_nr_out = 0;
        } /* end if */
        else
            udata->md_rw_cb_ud.nr--;

        /* Create task for reissued dataset metadata read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                     H5_daos_dinfo_read_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to read dataset metadata");

        /* Schedule dataset metadata read task and give it a reference to req
         * and the dataset */
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule task to read dataset metadata: %s",
                         H5_daos_err_to_string(ret));
        udata = NULL;
    } /* end if */
    else {
        /* Handle errors in fetch task.  Only record error in udata->req_status
         * if it does not already contain an error (it could contain an error if
         * another task this task is not dependent on also failed). */
        if (task->dt_result < -H5_DAOS_PRE_ERROR &&
            udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = task->dt_result;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        } /* end if */
        else if (task->dt_result == 0) {
            uint64_t type_buf_len  = (uint64_t)((char *)udata->md_rw_cb_ud.sg_iov[1].iov_buf -
                                               (char *)udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            uint64_t space_buf_len = (uint64_t)((char *)udata->md_rw_cb_ud.sg_iov[2].iov_buf -
                                                (char *)udata->md_rw_cb_ud.sg_iov[1].iov_buf);
            uint64_t dcpl_buf_len  = udata->md_rw_cb_ud.nr >= 4
                                         ? (uint64_t)((char *)udata->md_rw_cb_ud.sg_iov[3].iov_buf -
                                                     (char *)udata->md_rw_cb_ud.sg_iov[2].iov_buf)
                                         : udata->md_rw_cb_ud.iod[2].iod_size;

            assert(udata->md_rw_cb_ud.req->file);
            assert(udata->md_rw_cb_ud.obj);
            assert(udata->md_rw_cb_ud.obj->item.type == H5I_DATASET);

            /* Check for missing metadata */
            if (udata->md_rw_cb_ud.iod[0].iod_size == 0 || udata->md_rw_cb_ud.iod[1].iod_size == 0 ||
                udata->md_rw_cb_ud.iod[2].iod_size == 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, -H5_DAOS_DAOS_GET_ERROR,
                             "internal metadata not found");

            if (udata->bcast_udata) {
                /* Encode oid */
                p = udata->bcast_udata->bcast_udata.buffer;
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.lo)
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.hi)

                /* Encode serialized info lengths */
                UINT64ENCODE(p, type_buf_len)
                UINT64ENCODE(p, space_buf_len)
                UINT64ENCODE(p, dcpl_buf_len)
                UINT64ENCODE(p, udata->md_rw_cb_ud.iod[3].iod_size)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Finish building dataset object */
            if (0 != (ret = H5_daos_dset_open_end(
                          (H5_daos_dset_t *)udata->md_rw_cb_ud.obj, udata->md_rw_cb_ud.sg_iov[0].iov_buf,
                          type_buf_len, space_buf_len, dcpl_buf_len,
                          (uint64_t)udata->md_rw_cb_ud.iod[3].iod_size, udata->md_rw_cb_ud.req->dxpl_id)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't finish opening dataset");
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up if this is the last fetch task */
    if (udata) {
        /* Close dataset */
        if (udata->md_rw_cb_ud.obj &&
            H5_daos_dataset_close_real((H5_daos_dset_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        if (udata->bcast_udata) {
            /* Clear broadcast buffer if there was an error */
            if (udata->md_rw_cb_ud.req->status < -H5_DAOS_INCOMPLETE)
                (void)memset(udata->bcast_udata->bcast_udata.buffer, 0,
                             (size_t)udata->bcast_udata->bcast_udata.count);
        } /* end if */
        else if (udata->md_rw_cb_ud.free_sg_iov[0])
            /* No broadcast, free buffer */
            DV_free(udata->md_rw_cb_ud.sg_iov[0].iov_buf);

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->fetch_metatask) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete fetch metatask */
        tse_task_complete(udata->fetch_metatask, ret_value);

        assert(!udata->md_rw_cb_ud.free_dkey);
        assert(!udata->md_rw_cb_ud.free_akeys);

        /* Free udata */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    return ret_value;
} /* end H5_daos_dinfo_read_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_open
 *
 * Purpose:     Sends a request to DAOS to open a dataset
 *
 * Return:      Success:        dataset object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_dataset_open(void *_item, const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
                     hid_t dapl_id, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item       = (H5_daos_item_t *)_item;
    H5_daos_dset_t *dset       = NULL;
    H5_daos_obj_t  *target_obj = NULL;
    daos_obj_id_t   oid        = {0, 0};
    daos_obj_id_t **oid_ptr    = NULL;
    H5_daos_req_t  *int_req    = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    hbool_t         collective = FALSE;
    hbool_t         must_bcast = FALSE;
    char           *path_buf   = NULL;
    int             ret;
    void           *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    H5_DAOS_GET_METADATA_READ_MODE(item->file, dapl_id, H5P_DATASET_ACCESS_DEFAULT, collective, H5E_DATASET,
                                   NULL);

    /* Start H5 operation */
    if (NULL ==
        (int_req = H5_daos_req_create(item->file, "dataset open", item->open_req, NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Check for open by object token */
    if (H5VL_OBJECT_BY_TOKEN == loc_params->type) {
        /* Generate oid from token */
        if (H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't convert object token to OID");
    } /* end if */
    else {
        const char *target_name = NULL;
        size_t      target_name_len;

        /* Open using name parameter */
        if (H5VL_OBJECT_BY_SELF != loc_params->type)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL,
                         "unsupported dataset open location parameters type");
        if (!name)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset name is NULL");

        /* At this point we must broadcast on failure */
        if (collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Traverse the path */
        if (NULL == (target_obj = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, int_req,
                                                         collective, &path_buf, &target_name,
                                                         &target_name_len, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path");

        /* Check for no target_name, in this case just return target_obj */
        if (target_name_len == 0) {
            /* Check type of target_obj */
            if (target_obj->item.type != H5I_DATASET)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a dataset");

            /* Take ownership of target_obj */
            dset       = (H5_daos_dset_t *)target_obj;
            target_obj = NULL;

            /* No need to bcast since everyone just opened the already open
             * dataset */
            must_bcast = FALSE;

            D_GOTO_DONE(dset);
        } /* end if */

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        if (!collective || (item->file->my_rank == 0))
            /* Follow link to dataset */
            if (H5_daos_link_follow((H5_daos_group_t *)target_obj, target_name, target_name_len, FALSE,
                                    int_req, &oid_ptr, NULL, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_TRAVERSE, NULL, "can't follow link to dataset");
    } /* end else */

    must_bcast = FALSE;
    if (NULL == (dset = H5_daos_dataset_open_helper(item->file, dapl_id, collective, int_req, &first_task,
                                                    &dep_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset");

    /* Set dataset oid */
    if (oid_ptr)
        /* Retarget *oid_ptr to dset->obj.oid so H5_daos_link_follow fills in
         * the dataset's oid */
        *oid_ptr = &dset->obj.oid;
    else if (H5VL_OBJECT_BY_TOKEN == loc_params->type)
        /* Just set the static oid from the token */
        dset->obj.oid = oid;
    else
        /* We will receive oid from lead process */
        assert(collective && item->file->my_rank > 0);

    /* Set return value */
    ret_value = (void *)dset;

done:
    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Broadcast dataset info if needed */
        if (must_bcast && H5_daos_mpi_ibcast(NULL, &dset->obj, H5_DAOS_DINFO_BCAST_BUF_SIZE, TRUE, NULL,
                                             item->file->my_rank == 0 ? H5_daos_dset_open_bcast_comp_cb
                                                                      : H5_daos_dset_open_recv_comp_cb,
                                             int_req, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL,
                         "failed to broadcast empty dataset info buffer to signal failure");

        /* Close dataset */
        if (dset && H5_daos_dataset_close_real(dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset");
    } /* end if */
    else
        assert(!must_bcast);

    if (int_req) {
        /* Free path_buf if necessary */
        if (path_buf && H5_daos_free_async(path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTFREE, NULL, "can't free path buffer");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the group open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_OBJ,
                                collective, !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, NULL, "dataset open failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close object");

    /* If we are not returning a dataset we must close it */
    if (ret_value == NULL && dset && H5_daos_dataset_close_real(dset) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset");

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_open_helper
 *
 * Purpose:     Performs the actual dataset open. It is the responsibility
 *              of the calling function to make sure that the dataset's oid
 *              field is filled in before scheduled tasks are allowed to
 *              run.
 *
 * Return:      Success:        dataset object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5_daos_dset_t *
H5_daos_dataset_open_helper(H5_daos_file_t *file, hid_t dapl_id, hbool_t collective, H5_daos_req_t *req,
                            tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_flex_t *bcast_udata    = NULL;
    H5_daos_omd_fetch_ud_t       *fetch_udata    = NULL;
    H5_daos_dset_t               *dset           = NULL;
    size_t                        dinfo_buf_size = 0;
    int                           ret;
    H5_daos_dset_t               *ret_value = NULL;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    /* Allocate the dataset object that is returned to the user */
    if (NULL == (dset = H5FL_CALLOC(H5_daos_dset_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct");
    dset->obj.item.type     = H5I_DATASET;
    dset->obj.item.open_req = req;
    req->rc++;
    dset->obj.item.file             = file;
    dset->obj.item.rc               = 1;
    dset->obj.obj_oh                = DAOS_HDL_INVAL;
    dset->type_id                   = H5I_INVALID_HID;
    dset->file_type_id              = H5I_INVALID_HID;
    dset->space_id                  = H5I_INVALID_HID;
    dset->cur_set_extent_space_id   = H5I_INVALID_HID;
    dset->dcpl_id                   = H5P_DATASET_CREATE_DEFAULT;
    dset->dapl_id                   = H5P_DATASET_ACCESS_DEFAULT;
    dset->io_cache.file_sel_iter_id = H5I_INVALID_HID;
    dset->io_cache.mem_sel_iter_id  = H5I_INVALID_HID;
    if ((dapl_id != H5P_DATASET_ACCESS_DEFAULT) && (dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dapl");

    /* Set up broadcast user data (if appropriate) and calculate initial dataset
     * info buffer size */
    if (collective && (file->num_procs > 1)) {
        if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_flex_t *)DV_malloc(
                         sizeof(H5_daos_mpi_ibcast_ud_flex_t) + H5_DAOS_DINFO_BCAST_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->bcast_udata.req        = req;
        bcast_udata->bcast_udata.obj        = &dset->obj;
        bcast_udata->bcast_udata.buffer     = bcast_udata->flex_buf;
        bcast_udata->bcast_udata.buffer_len = H5_DAOS_DINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_udata.count      = H5_DAOS_DINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_udata.comm       = req->file->comm;

        dinfo_buf_size = H5_DAOS_DINFO_BCAST_BUF_SIZE;
    } /* end if */
    else
        dinfo_buf_size = H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_DCPL_BUF_SIZE +
                         H5_DAOS_FILL_VAL_BUF_SIZE;

    /* Check if we're actually opening the dataset or just receiving the dataset
     * info from the leader */
    if (!collective || (file->my_rank == 0)) {
        tse_task_t *fetch_task = NULL;
        uint8_t    *p;

        /* Open dataset object */
        if (H5_daos_obj_open(file, req, &dset->obj.oid,
                             (file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO), &dset->obj.obj_oh,
                             "dataset object open", first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset object");

        /* Allocate argument struct for fetch task */
        if (NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(sizeof(H5_daos_omd_fetch_ud_t) +
                                                                       (bcast_udata ? 0 : dinfo_buf_size))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for fetch callback arguments");

        /* Set up operation to read datatype, dataspace, and DCPL sizes from
         * dataset */
        /* Set up ud struct */
        fetch_udata->md_rw_cb_ud.req = req;
        fetch_udata->md_rw_cb_ud.obj = &dset->obj;
        fetch_udata->bcast_udata     = bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_type_key_g,
                           H5_daos_type_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[0].iod_nr    = 1u;
        fetch_udata->md_rw_cb_ud.iod[0].iod_size  = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[0].iod_type  = DAOS_IOD_SINGLE;
        fetch_udata->md_rw_cb_ud.iod[0].iod_flags = DAOS_COND_AKEY_FETCH;

        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[1].iod_name, H5_daos_space_key_g,
                           H5_daos_space_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[1].iod_nr    = 1u;
        fetch_udata->md_rw_cb_ud.iod[1].iod_size  = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[1].iod_type  = DAOS_IOD_SINGLE;
        fetch_udata->md_rw_cb_ud.iod[1].iod_flags = DAOS_COND_AKEY_FETCH;

        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[2].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[2].iod_nr    = 1u;
        fetch_udata->md_rw_cb_ud.iod[2].iod_size  = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[2].iod_type  = DAOS_IOD_SINGLE;
        fetch_udata->md_rw_cb_ud.iod[2].iod_flags = DAOS_COND_AKEY_FETCH;

        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[3].iod_name, H5_daos_fillval_key_g,
                           H5_daos_fillval_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[3].iod_nr    = 1u;
        fetch_udata->md_rw_cb_ud.iod[3].iod_size  = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[3].iod_type  = DAOS_IOD_SINGLE;
        fetch_udata->md_rw_cb_ud.iod[3].iod_flags = 0;

        fetch_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up buffer */
        if (bcast_udata)
            p = bcast_udata->flex_buf + (6 * H5_DAOS_ENCODED_UINT64_T_SIZE);
        else
            p = fetch_udata->flex_buf;

        /* Set up sgl */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], p, (daos_size_t)H5_DAOS_TYPE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[0];
        fetch_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        p += H5_DAOS_TYPE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[1], p, (daos_size_t)H5_DAOS_SPACE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[1];
        fetch_udata->md_rw_cb_ud.free_sg_iov[1]   = FALSE;
        p += H5_DAOS_SPACE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[2], p, (daos_size_t)H5_DAOS_DCPL_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[2].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[2].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[2];
        fetch_udata->md_rw_cb_ud.free_sg_iov[2]   = FALSE;
        p += H5_DAOS_DCPL_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[3], p, (daos_size_t)H5_DAOS_FILL_VAL_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[3].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[3].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[3].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[3];
        fetch_udata->md_rw_cb_ud.free_sg_iov[3]   = FALSE;

        /* Set conditional per-akey fetch for dataset metadata read operation */
        fetch_udata->md_rw_cb_ud.flags = DAOS_COND_PER_AKEY;

        /* Set nr */
        fetch_udata->md_rw_cb_ud.nr = 4u;

        /* Set task name */
        fetch_udata->md_rw_cb_ud.task_name = "dataset metadata read";

        /* Create meta task for dataset metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_dinfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &fetch_udata->fetch_metatask) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create meta task for dataset metadata read");

        /* Create task for dataset metadata read */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_dinfo_read_comp_cb, fetch_udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to read dataset metadata");

        /* Schedule meta task */
        if (0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL,
                         "can't schedule meta task for dataset metadata read: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule dataset metadata read task (or save it to be scheduled
         * later) and give it a reference to req and the dataset */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to read dataset metadata: %s",
                         H5_daos_err_to_string(ret));
        *dep_task = fetch_udata->fetch_metatask;
        req->rc++;
        dset->obj.item.rc++;
        fetch_udata = NULL;
    } /* end if */
    else
        assert(bcast_udata);

    ret_value = dset;

done:
    /* Broadcast dataset info */
    if (bcast_udata) {
        if (H5_daos_mpi_ibcast(
                &bcast_udata->bcast_udata, &dset->obj, dinfo_buf_size, NULL == ret_value ? TRUE : FALSE, NULL,
                file->my_rank == 0 ? H5_daos_dset_open_bcast_comp_cb : H5_daos_dset_open_recv_comp_cb, req,
                first_task, dep_task) < 0) {
            DV_free(bcast_udata);
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to broadcast dataset info buffer");
        } /* end if */

        bcast_udata = NULL;
    } /* end if */

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close dataset */
        if (dset && H5_daos_dataset_close_real(dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset");

        /* Free memory */
        fetch_udata = DV_free(fetch_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!fetch_udata);
    assert(!bcast_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_open_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_sel_to_recx_iov
 *
 * Purpose:     Given a dataspace with a selection and the datatype
 *              (element) size, build a list of DAOS records (recxs)
 *              and/or scatter/gather list I/O vectors (sg_iovs). *recxs
 *              and *sg_iovs should, if requested, point to a (probably
 *              statically allocated) single element.  Does not release
 *              buffers on error.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_sel_to_recx_iov(hid_t sel_iter_id, size_t type_size, void *buf, daos_recx_t **recxs,
                        daos_iov_t **sg_iovs, size_t *list_nused)
{
    size_t  nseq;
    size_t  nelem;
    hsize_t off[H5_DAOS_SEQ_LIST_LEN];
    size_t  len[H5_DAOS_SEQ_LIST_LEN];
    size_t  buf_len = 1;
    void   *vp_ret;
    size_t  szi;
    herr_t  ret_value = SUCCEED;

    assert(recxs || sg_iovs);
    assert(!recxs || *recxs);
    assert(!sg_iovs || *sg_iovs);
    assert(list_nused);

    /* Initialize list_nused */
    *list_nused = 0;

    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes */
        if (H5Ssel_iter_get_seq_list(sel_iter_id, (size_t)H5_DAOS_SEQ_LIST_LEN, (size_t)-1, &nseq, &nelem,
                                     off, len) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed");

        /* Make room for sequences in recxs */
        if ((buf_len == 1) && (nseq > 1)) {
            if (recxs)
                if (NULL == (*recxs = (daos_recx_t *)DV_malloc(H5_DAOS_SEQ_LIST_LEN * sizeof(daos_recx_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate memory for records");
            if (sg_iovs)
                if (NULL == (*sg_iovs = (daos_iov_t *)DV_malloc(H5_DAOS_SEQ_LIST_LEN * sizeof(daos_iov_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate memory for sgl iovs");
            buf_len = H5_DAOS_SEQ_LIST_LEN;
        } /* end if */
        else if (*list_nused + nseq > buf_len) {
            if (recxs) {
                if (NULL == (vp_ret = DV_realloc(*recxs, 2 * buf_len * sizeof(daos_recx_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate memory for records");
                *recxs = (daos_recx_t *)vp_ret;
            } /* end if */
            if (sg_iovs) {
                if (NULL == (vp_ret = DV_realloc(*sg_iovs, 2 * buf_len * sizeof(daos_iov_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate memory for sgls");
                *sg_iovs = (daos_iov_t *)vp_ret;
            } /* end if */
            buf_len *= 2;
        } /* end if */
        assert(*list_nused + nseq <= buf_len);

        /* Copy offsets/lengths to recxs and sg_iovs */
        for (szi = 0; szi < nseq; szi++) {
            if (recxs) {
                (*recxs)[szi + *list_nused].rx_idx = (uint64_t)off[szi];
                (*recxs)[szi + *list_nused].rx_nr  = (uint64_t)len[szi];
            } /* end if */
            if (sg_iovs)
                daos_iov_set(&(*sg_iovs)[szi + *list_nused], (uint8_t *)buf + (off[szi] * type_size),
                             (daos_size_t)len[szi] * (daos_size_t)type_size);
        } /* end for */
        *list_nused += nseq;
    } while (nseq == H5_DAOS_SEQ_LIST_LEN);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_sel_to_recx_iov() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_scatter_cb
 *
 * Purpose:     Callback function for H5Dscatter.  Simply passes the
 *              entire buffer described by udata to H5Dscatter.
 *
 * Return:      SUCCEED (never fails)
 *
 * Programmer:  Neil Fortner
 *              March, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_scatter_cb(const void **src_buf, size_t *src_buf_bytes_used, void *_udata)
{
    H5_daos_scatter_cb_ud_t *udata     = (H5_daos_scatter_cb_ud_t *)_udata;
    herr_t                   ret_value = SUCCEED;

    /* Set src_buf and src_buf_bytes_used to use the entire buffer */
    *src_buf            = udata->buf;
    *src_buf_bytes_used = udata->len;

    /* DSINC - This function used to always return SUCCEED without needing an
     * herr_t. Might need an additional FUNC_LEAVE macro to do this, or modify
     * the current one to take in the ret_value.
     */
    D_FUNC_LEAVE;
} /* end H5_daos_scatter_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_chunk_io_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for raw data I/O.  Currently checks for
 *              errors from previous tasks then sets arguments for daos
 *              task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_chunk_io_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_chunk_io_ud_t *udata;
    daos_obj_rw_t         *update_args;
    int                    ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for chunk I/O task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_IO);

    assert(udata->dset);
    assert(udata->req->file);

    /* Set I/O task arguments */
    if (NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for chunk I/O task");
    memset(update_args, 0, sizeof(*update_args));
    update_args->oh   = udata->dset->obj.obj_oh;
    update_args->th   = udata->req->th;
    update_args->dkey = &udata->dkey;
    update_args->nr   = 1;
    update_args->iods = &udata->iod;
    update_args->sgls = &udata->sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_chunk_io_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_chunk_io_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for raw data I/O.  Currently checks for a
 *              failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_chunk_io_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_chunk_io_ud_t *udata;
    int                    ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for chunk I/O task");

    assert(udata->req);
    assert(udata->dset);
    assert(udata->req->file);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "raw data I/O";
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        /* Close dataset */
        if (H5_daos_dataset_close_real(udata->dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "raw data I/O completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        if (udata->recxs != &udata->recx)
            DV_free(udata->recxs);
        if (udata->sg_iovs != &udata->sg_iov)
            DV_free(udata->sg_iovs);
        DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_chunk_io_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_io_types_equal
 *
 * Purpose:     Internal helper routine to perform I/O on a dataset
 *              composed of a non-variable-length datatype where the
 *              datatype specified for the memory buffer matches the
 *              dataset's datatype. In this case, datatype conversion is
 *              not necessary.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset I/O not performed.
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_io_types_equal(H5_daos_select_chunk_info_t *chunk_info, H5_daos_dset_t *dset,
                               uint64_t dset_ndims, hid_t H5VL_DAOS_UNUSED mem_type_id,
                               H5_daos_io_type_t io_type, void *buf, H5_daos_req_t *req,
                               tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_chunk_io_ud_t *chunk_io_ud = NULL;
    daos_opc_t             daos_op;
    size_t                 tot_nseq;
    size_t                 file_type_size;
    tse_task_t            *io_task;
    uint64_t               i;
    uint8_t               *p;
    int                    ret;
    herr_t                 ret_value = SUCCEED;

    assert(chunk_info);
    assert(dset);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate argument struct */
    if (NULL == (chunk_io_ud = (H5_daos_chunk_io_ud_t *)DV_calloc(sizeof(H5_daos_chunk_io_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O callback arguments");
    chunk_io_ud->recxs   = &chunk_io_ud->recx;
    chunk_io_ud->sg_iovs = &chunk_io_ud->sg_iov;

    /* Point to dset */
    chunk_io_ud->dset = dset;

    /* Point to req */
    chunk_io_ud->req = req;

    /* Encode dkey (chunk coordinates).  Prefix with '\0' to avoid accidental
     * collisions with other d-keys in this object.
     */
    p    = chunk_io_ud->dkey_buf;
    *p++ = (uint8_t)'\0';
    for (i = 0; i < dset_ndims; i++)
        UINT64ENCODE(p, chunk_info->chunk_coords[i]);

    /* Set up dkey */
    daos_iov_set(&chunk_io_ud->dkey, chunk_io_ud->dkey_buf,
                 (daos_size_t)(1 + ((size_t)dset_ndims * sizeof(chunk_info->chunk_coords[0]))));

    file_type_size = dset->file_type_size;

    /* Set up iod */
    memset(&chunk_io_ud->iod, 0, sizeof(chunk_io_ud->iod));
    chunk_io_ud->akey_buf = H5_DAOS_CHUNK_KEY;
    daos_iov_set(&chunk_io_ud->iod.iod_name, (void *)&chunk_io_ud->akey_buf,
                 (daos_size_t)(sizeof(chunk_io_ud->akey_buf)));
    chunk_io_ud->iod.iod_size = (daos_size_t)file_type_size;
    chunk_io_ud->iod.iod_type = DAOS_IOD_ARRAY;

    /* Check if the memory space and file space IDs are the same; use file space in this case */
    if (chunk_info->mspace_id == chunk_info->fspace_id) {
        /* Reset file selection iterator for current file dataspace */
        if (H5Ssel_iter_reset(dset->io_cache.file_sel_iter_id, chunk_info->fspace_id) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTRESET, FAIL, "can't reset file dataspace selection iterator");

        /* Calculate both recxs and sg_iovs at the same time from file space */
        if (H5_daos_sel_to_recx_iov(dset->io_cache.file_sel_iter_id, file_type_size, buf, &chunk_io_ud->recxs,
                                    &chunk_io_ud->sg_iovs, &tot_nseq) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O");
        chunk_io_ud->iod.iod_nr    = (unsigned)tot_nseq;
        chunk_io_ud->sgl.sg_nr     = (uint32_t)tot_nseq;
        chunk_io_ud->sgl.sg_nr_out = 0;
    } /* end if */
    else {
        /* Reset file selection iterator for current file dataspace */
        if (H5Ssel_iter_reset(dset->io_cache.file_sel_iter_id, chunk_info->fspace_id) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTRESET, FAIL, "can't reset file dataspace selection iterator");
        /* Reset memory selection iterator for current memory dataspace */
        if (H5Ssel_iter_reset(dset->io_cache.mem_sel_iter_id, chunk_info->mspace_id) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTRESET, FAIL,
                         "can't reset memory dataspace selection iterator");

        /* Calculate recxs from file space */
        if (H5_daos_sel_to_recx_iov(dset->io_cache.file_sel_iter_id, file_type_size, buf, &chunk_io_ud->recxs,
                                    NULL, &tot_nseq) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O");
        chunk_io_ud->iod.iod_nr = (unsigned)tot_nseq;

        /* Calculate sg_iovs from mem space */
        if (H5_daos_sel_to_recx_iov(dset->io_cache.mem_sel_iter_id, file_type_size, buf, NULL,
                                    &chunk_io_ud->sg_iovs, &tot_nseq) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O");
        chunk_io_ud->sgl.sg_nr     = (uint32_t)tot_nseq;
        chunk_io_ud->sgl.sg_nr_out = 0;
    } /* end else */

    /* Point iod and sgl to lists generated above */
    chunk_io_ud->iod.iod_recxs = chunk_io_ud->recxs;
    chunk_io_ud->sgl.sg_iovs   = chunk_io_ud->sg_iovs;

    /* No selection in the file */
    if (chunk_io_ud->iod.iod_nr == 0) {
        *dep_task = NULL;
        D_GOTO_DONE(SUCCEED);
    } /* end if */

    if (io_type == IO_READ) {
        /* Handle fill values */
        size_t j;

        if (dset->dcpl_cache.fill_method == H5_DAOS_ZERO_FILL) {
            /* Just set all locations pointed to by sg_iovs to zero */
            for (j = 0; j < tot_nseq; j++)
                (void)memset(chunk_io_ud->sg_iovs[j].iov_buf, 0, chunk_io_ud->sg_iovs[j].iov_len);
        } /* end if */
        else if (dset->dcpl_cache.fill_method == H5_DAOS_COPY_FILL) {
            /* Copy fill value to all locations pointed to by sg_iovs */
            size_t iov_buf_written;

            assert(dset->fill_val);

            for (j = 0; j < tot_nseq; j++) {
                for (iov_buf_written = 0; iov_buf_written < chunk_io_ud->sg_iovs[j].iov_len;
                     iov_buf_written += file_type_size)
                    (void)memcpy((uint8_t *)chunk_io_ud->sg_iovs[j].iov_buf + iov_buf_written, dset->fill_val,
                                 file_type_size);
                assert(iov_buf_written == chunk_io_ud->sg_iovs[j].iov_len);
            } /* end for */
        }     /* end if */

        /* Create task to read data from dataset */
        daos_op = DAOS_OPC_OBJ_FETCH;
    }    /* end (io_type == IO_READ) */
    else /* (io_type == IO_WRITE) */
        /* Create task to write data to dataset */
        daos_op = DAOS_OPC_OBJ_UPDATE;

    if (H5_daos_create_daos_task(daos_op, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_chunk_io_prep_cb, H5_daos_chunk_io_comp_cb, chunk_io_ud,
                                 &io_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to %s data",
                     (daos_op == DAOS_OPC_OBJ_FETCH) ? "read" : "write");

    /* Schedule IO task (or save it to be scheduled later) */
    if (*first_task) {
        assert(*dep_task);
        if (0 != (ret = tse_task_schedule(io_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule dataset I/O task");
    } /* end if */
    else
        *first_task = io_task;
    *dep_task = io_task;

    /* Task will be scheduled, give it a reference to req */
    chunk_io_ud->req->rc++;
    chunk_io_ud->dset->obj.item.rc++;

done:
    /* Cleanup on failure */
    if (ret_value < 0 && chunk_io_ud) {
        if (chunk_io_ud->recxs != &chunk_io_ud->recx)
            DV_free(chunk_io_ud->recxs);
        if (chunk_io_ud->sg_iovs != &chunk_io_ud->sg_iov)
            DV_free(chunk_io_ud->sg_iovs);
        chunk_io_ud = DV_free(chunk_io_ud);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_io_types_equal() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_chunk_io_tconv_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for raw data I/O with type conversion.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_chunk_io_tconv_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_chunk_io_ud_t *udata;
    daos_obj_rw_t         *update_args;
    int                    ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for chunk I/O task");

    assert(udata->req);
    assert(udata->dset);
    assert(udata->req->file);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_IO);

    /* If writing, gather the write buffer data to the type conversion buffer */
    if (udata->tconv.io_type == IO_WRITE) {
        /* Gather data to conversion buffer */
        if (H5Dgather(udata->tconv.mem_space_id, udata->tconv.buf, udata->tconv.mem_type_id,
                      (size_t)udata->tconv.num_elem * udata->tconv.mem_type_size, udata->tconv.tconv_buf,
                      NULL, NULL) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_H5_SCATGATH_ERROR,
                         "can't gather data to conversion buffer");

        /* Perform type conversion */
        if (H5Tconvert(udata->tconv.mem_type_id, udata->dset->file_type_id, (size_t)udata->tconv.num_elem,
                       udata->tconv.tconv_buf, udata->tconv.bkg_buf, udata->req->dxpl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR,
                         "can't perform type conversion");
    } /* end if */

    /* Set sg_iov to point to tconv_buf */
    daos_iov_set(&udata->sg_iov, udata->tconv.tconv_buf,
                 (daos_size_t)udata->tconv.num_elem * (daos_size_t)udata->tconv.file_type_size);

    /* Set I/O task arguments */
    if (NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for chunk I/O task");
    memset(update_args, 0, sizeof(*update_args));
    update_args->oh   = udata->dset->obj.obj_oh;
    update_args->th   = udata->req->th;
    update_args->dkey = &udata->dkey;
    update_args->nr   = 1;
    update_args->iods = &udata->iod;
    update_args->sgls = &udata->sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_chunk_io_tconv_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_chunk_io_tconv_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for raw data I/O with type conversion.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_chunk_io_tconv_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_chunk_io_ud_t *udata;
    int                    ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for chunk I/O task");

    assert(udata->req);
    assert(udata->req->file);
    assert(udata->dset);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "raw data I/O";
    } /* end if */

    /* If reading we must perform type conversion on the read data */
    if (udata->tconv.io_type == IO_READ) {
        /* Perform type conversion */
        if (H5Tconvert(udata->dset->file_type_id, udata->tconv.mem_type_id, (size_t)udata->tconv.num_elem,
                       udata->tconv.tconv_buf, udata->tconv.bkg_buf, udata->req->dxpl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR,
                         "can't perform type conversion");

        /* Scatter data to memory buffer if necessary */
        if (udata->tconv.reuse != H5_DAOS_TCONV_REUSE_TCONV) {
            H5_daos_scatter_cb_ud_t scatter_cb_ud;

            scatter_cb_ud.buf = udata->tconv.tconv_buf;
            scatter_cb_ud.len = (size_t)udata->tconv.num_elem * udata->tconv.mem_type_size;
            if (H5Dscatter(H5_daos_scatter_cb, &scatter_cb_ud, udata->tconv.mem_type_id,
                           udata->tconv.mem_space_id, udata->tconv.buf) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_H5_SCATGATH_ERROR,
                             "can't scatter data to read buffer");
        } /* end if */
    }     /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        /* Close dataset */
        if (H5_daos_dataset_close_real(udata->dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Close space and type IDs */
        if (H5Sclose(udata->tconv.mem_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR,
                         "can't close memory dataspace");
        if (H5Tclose(udata->tconv.mem_type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close memory datatype");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "raw data I/O completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        if (udata->recxs != &udata->recx)
            DV_free(udata->recxs);
        if (udata->tconv.reuse != H5_DAOS_TCONV_REUSE_TCONV)
            DV_free(udata->tconv.tconv_buf);
        if (udata->tconv.reuse != H5_DAOS_TCONV_REUSE_BKG)
            DV_free(udata->tconv.bkg_buf);
        DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_chunk_io_tconv_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_chunk_fill_bkg_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for filling background buffer for type
 *              conversion.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_chunk_fill_bkg_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_chunk_io_ud_t *udata;
    daos_obj_rw_t         *update_args;
    int                    ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for chunk I/O task");

    assert(udata->req);
    assert(udata->dset);
    assert(udata->req->file);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_IO);

    /* Set sg_iov to point to background buffer */
    daos_iov_set(&udata->sg_iov, udata->tconv.bkg_buf,
                 (daos_size_t)udata->tconv.num_elem * (daos_size_t)udata->tconv.file_type_size);

    /* Set I/O task arguments */
    if (NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for chunk I/O task");
    memset(update_args, 0, sizeof(*update_args));
    update_args->oh   = udata->dset->obj.obj_oh;
    update_args->th   = udata->req->th;
    update_args->dkey = &udata->dkey;
    update_args->nr   = 1;
    update_args->iods = &udata->iod;
    update_args->sgls = &udata->sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_chunk_fill_bkg_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_chunk_fill_bkg_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for filling background buffer for type
 *              conversion.  Does not free data, will be freed by
 *              H5_daos_chunk_io_tconv_comp_cb().
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_chunk_fill_bkg_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_chunk_io_ud_t *udata;
    int                    ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for chunk I/O task");

    assert(udata);
    assert(udata->req);
    assert(udata->req->file);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "background buffer fill (daos_obj_fetch) for raw data write";
    } /* end if */

    /* Reset iod_size, if the dataset was not allocated then it could
     * have been overwritten by daos_obj_fetch */
    udata->iod.iod_size = udata->tconv.file_type_size;

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    D_FUNC_LEAVE;
} /* end H5_daos_chunk_fill_bkg_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_io_types_unequal
 *
 * Purpose:     Internal helper routine to perform I/O on a dataset
 *              composed of a non-variable-length datatype where the
 *              datatype specified for the memory buffer doesn't match the
 *              dataset's datatype. In this case, datatype conversion must
 *              be performed.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset I/O not performed.
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_io_types_unequal(H5_daos_select_chunk_info_t *chunk_info, H5_daos_dset_t *dset,
                                 uint64_t dset_ndims, hid_t mem_type_id, H5_daos_io_type_t io_type, void *buf,
                                 H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_chunk_io_ud_t *chunk_io_ud = NULL;
    daos_opc_t             daos_op;
    hbool_t                contig = FALSE;
    size_t                 tot_nseq;
    tse_task_t            *io_task       = NULL;
    tse_task_t            *fill_bkg_task = NULL;
    uint64_t               i;
    uint8_t               *p;
    int                    ret;
    herr_t                 ret_value = SUCCEED;

    assert(chunk_info);
    assert(dset);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate argument struct */
    if (NULL == (chunk_io_ud = (H5_daos_chunk_io_ud_t *)DV_calloc(sizeof(H5_daos_chunk_io_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O callback arguments");

    /* Setup type conversion-related fields */
    chunk_io_ud->tconv.num_elem     = chunk_info->num_elem_sel_file;
    chunk_io_ud->tconv.mem_space_id = H5I_INVALID_HID;
    if ((chunk_io_ud->tconv.mem_type_id = H5Tcopy(mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy memory datatype");
    if ((chunk_io_ud->tconv.mem_space_id = H5Scopy(chunk_info->mspace_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy memory dataspace");
    chunk_io_ud->tconv.buf     = buf;
    chunk_io_ud->tconv.io_type = io_type;
    chunk_io_ud->recxs         = &chunk_io_ud->recx;
    assert(chunk_io_ud->tconv.reuse == H5_DAOS_TCONV_REUSE_NONE);

    /* Point to dset */
    chunk_io_ud->dset = dset;

    /* Point to req */
    chunk_io_ud->req = req;

    /* Encode dkey (chunk coordinates).  Prefix with '\0' to avoid accidental
     * collisions with other d-keys in this object.
     */
    p    = chunk_io_ud->dkey_buf;
    *p++ = (uint8_t)'\0';
    for (i = 0; i < dset_ndims; i++)
        UINT64ENCODE(p, chunk_info->chunk_coords[i]);

    /* Set up dkey */
    daos_iov_set(&chunk_io_ud->dkey, chunk_io_ud->dkey_buf,
                 (daos_size_t)(1 + ((size_t)dset_ndims * sizeof(chunk_info->chunk_coords[0]))));

    if (io_type == IO_READ) {
        size_t  nseq_tmp;
        size_t  nelem_tmp;
        hsize_t sel_off;
        size_t  sel_len;

        /* Reset memory selection iterator for current memory dataspace */
        if (H5Ssel_iter_reset(dset->io_cache.mem_sel_iter_id, chunk_info->mspace_id) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTRESET, FAIL, "can't reset file dataspace selection iterator");

        /* Get the sequence list - only check the first sequence because we only
         * care if it is contiguous and if so where the contiguous selection
         * begins */
        if (H5Ssel_iter_get_seq_list(dset->io_cache.mem_sel_iter_id, (size_t)1, (size_t)-1, &nseq_tmp,
                                     &nelem_tmp, &sel_off, &sel_len) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed");
        contig = (sel_len == (size_t)chunk_info->num_elem_sel_file);

        /* Initialize type conversion */
        if (H5_daos_tconv_init(dset->file_type_id, &chunk_io_ud->tconv.file_type_size, mem_type_id,
                               &chunk_io_ud->tconv.mem_type_size, (size_t)chunk_info->num_elem_sel_file,
                               dset->dcpl_cache.fill_method == H5_DAOS_ZERO_FILL, FALSE,
                               &chunk_io_ud->tconv.tconv_buf, &chunk_io_ud->tconv.bkg_buf,
                               contig ? &chunk_io_ud->tconv.reuse : NULL, &chunk_io_ud->tconv.fill_bkg) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion");

        /* Reuse buffer as appropriate */
        if (contig) {
            sel_off *= (hsize_t)chunk_io_ud->tconv.mem_type_size;
            if (chunk_io_ud->tconv.reuse == H5_DAOS_TCONV_REUSE_TCONV)
                chunk_io_ud->tconv.tconv_buf = (char *)buf + (size_t)sel_off;
            else if (chunk_io_ud->tconv.reuse == H5_DAOS_TCONV_REUSE_BKG)
                chunk_io_ud->tconv.bkg_buf = (char *)buf + (size_t)sel_off;
        } /* end if */
    }     /* end (io_type == IO_READ) */
    else
        /* Initialize type conversion */
        if (H5_daos_tconv_init(mem_type_id, &chunk_io_ud->tconv.mem_type_size, dset->file_type_id,
                               &chunk_io_ud->tconv.file_type_size, (size_t)chunk_info->num_elem_sel_file,
                               FALSE, TRUE, &chunk_io_ud->tconv.tconv_buf, &chunk_io_ud->tconv.bkg_buf, NULL,
                               &chunk_io_ud->tconv.fill_bkg) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion");

    /* Set up iod */
    memset(&chunk_io_ud->iod, 0, sizeof(chunk_io_ud->iod));
    chunk_io_ud->akey_buf = H5_DAOS_CHUNK_KEY;
    daos_iov_set(&chunk_io_ud->iod.iod_name, (void *)&chunk_io_ud->akey_buf,
                 (daos_size_t)(sizeof(chunk_io_ud->akey_buf)));
    chunk_io_ud->iod.iod_size = (daos_size_t)chunk_io_ud->tconv.file_type_size;
    chunk_io_ud->iod.iod_type = DAOS_IOD_ARRAY;

    /* Build recxs and sg_iovs */

    /* Reset file selection iterator for current file dataspace */
    if (H5Ssel_iter_reset(dset->io_cache.file_sel_iter_id, chunk_info->fspace_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTRESET, FAIL, "can't reset file dataspace selection iterator");

    /* Calculate recxs from file space */
    if (H5_daos_sel_to_recx_iov(dset->io_cache.file_sel_iter_id, chunk_io_ud->tconv.file_type_size, buf,
                                &chunk_io_ud->recxs, NULL, &tot_nseq) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O");
    chunk_io_ud->iod.iod_nr    = (unsigned)tot_nseq;
    chunk_io_ud->iod.iod_recxs = chunk_io_ud->recxs;

    /* No selection in the file */
    if (chunk_io_ud->iod.iod_nr == 0)
        D_GOTO_DONE(SUCCEED);

    /* Set up constant sgl info */
    chunk_io_ud->sgl.sg_nr     = 1;
    chunk_io_ud->sgl.sg_nr_out = 0;
    chunk_io_ud->sgl.sg_iovs   = &chunk_io_ud->sg_iov;

    if (io_type == IO_READ) {
        /* Gather data to background buffer if necessary */
        if (chunk_io_ud->tconv.fill_bkg && (chunk_io_ud->tconv.reuse != H5_DAOS_TCONV_REUSE_BKG))
            if (H5Dgather(chunk_info->mspace_id, buf, mem_type_id,
                          (size_t)chunk_info->num_elem_sel_file * chunk_io_ud->tconv.mem_type_size,
                          chunk_io_ud->tconv.bkg_buf, NULL, NULL) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to background buffer");

        /* Handle fill values */
        if (dset->dcpl_cache.fill_method == H5_DAOS_ZERO_FILL) {
            /* H5_daos_tconv_init() will have cleared the tconv buf, but not if
             * we're reusing buf as tconv_buf */
            if (chunk_io_ud->tconv.reuse == H5_DAOS_TCONV_REUSE_TCONV)
                (void)memset(chunk_io_ud->tconv.tconv_buf, 0,
                             (daos_size_t)chunk_info->num_elem_sel_file *
                                 (daos_size_t)chunk_io_ud->tconv.file_type_size);
        } /* end if */
        else if (dset->dcpl_cache.fill_method == H5_DAOS_COPY_FILL) {
            hssize_t j;

            assert(dset->fill_val);

            /* Copy the fill value to every element in tconv_buf */
            for (j = 0; j < chunk_info->num_elem_sel_file; j++)
                (void)memcpy((uint8_t *)chunk_io_ud->tconv.tconv_buf +
                                 ((size_t)j * chunk_io_ud->tconv.file_type_size),
                             dset->fill_val, chunk_io_ud->tconv.file_type_size);
        } /* end if */

        /* Create task to read data from dataset */
        daos_op = DAOS_OPC_OBJ_FETCH;
    } /* end (io_type == IO_READ) */
    else {
        /* Check if we need to fill background buffer */
        if (chunk_io_ud->tconv.fill_bkg) {
            assert(chunk_io_ud->tconv.bkg_buf);

            /* Create task to read data from dataset */
            if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                         H5_daos_chunk_fill_bkg_prep_cb, H5_daos_chunk_fill_bkg_comp_cb,
                                         chunk_io_ud, &fill_bkg_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                             "can't create task to read data to background buffer");

            /* Save bkg fill task to be scheduled later */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(fill_bkg_task, false)))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                 "can't schedule background buffer fill task");
            } /* end if */
            else
                *first_task = fill_bkg_task;
            *dep_task = fill_bkg_task;
        } /* end if */

        /* Create task to write data to dataset */
        daos_op = DAOS_OPC_OBJ_UPDATE;
    } /* end (io_type == IO_WRITE) */

    if (H5_daos_create_daos_task(daos_op, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_chunk_io_tconv_prep_cb, H5_daos_chunk_io_tconv_comp_cb, chunk_io_ud,
                                 &io_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to %s data",
                     (daos_op == DAOS_OPC_OBJ_FETCH) ? "read" : "write");

    /* Schedule IO task (or save it to be scheduled later) */
    if (*first_task) {
        assert(*dep_task);
        if (0 != (ret = tse_task_schedule(io_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule dataset I/O task");
    } /* end if */
    else
        *first_task = io_task;
    *dep_task = io_task;

    /* Task will be scheduled, give it a reference to req and the dataset */
    chunk_io_ud->req->rc++;
    chunk_io_ud->dset->obj.item.rc++;

done:
    /* Cleanup on failure */
    if (ret_value < 0 && chunk_io_ud && !fill_bkg_task) {
        if (chunk_io_ud->tconv.mem_type_id >= 0 && H5Tclose(chunk_io_ud->tconv.mem_type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close memory datatype");
        if (chunk_io_ud->tconv.mem_space_id >= 0 && H5Sclose(chunk_io_ud->tconv.mem_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close memory dataspace");
        if (chunk_io_ud->recxs != &chunk_io_ud->recx)
            DV_free(chunk_io_ud->recxs);
        if (chunk_io_ud->tconv.reuse != H5_DAOS_TCONV_REUSE_TCONV)
            chunk_io_ud->tconv.tconv_buf = DV_free(chunk_io_ud->tconv.tconv_buf);
        if (chunk_io_ud->tconv.reuse != H5_DAOS_TCONV_REUSE_BKG)
            chunk_io_ud->tconv.bkg_buf = DV_free(chunk_io_ud->tconv.bkg_buf);
        chunk_io_ud = DV_free(chunk_io_ud);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_io_types_unequal() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_fill_io_cache
 *
 * Purpose:     Fills the "io_cache" field of the dataset struct. This
 *              field is used to cache various things for dataset I/O
 *              including dataspace selection iterators and selected chunk
 *              info buffers.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dset_fill_io_cache(H5_daos_dset_t *dset, hid_t file_space_id, hid_t mem_space_id)
{
    herr_t ret_value = SUCCEED;

    assert(dset);
    assert(!dset->io_cache.filled);
    assert(dset->io_cache.file_sel_iter_id <= 0);
    assert(dset->io_cache.mem_sel_iter_id <= 0);
    assert((dset->dcpl_cache.layout != H5D_LAYOUT_ERROR) && (dset->dcpl_cache.layout != H5D_NLAYOUTS));

    /* Setup and cache selection iterators for dataset. We use 1 for the element
     * size here so that the sequence list offsets and lengths are returned in
     * terms of numbers of elements, not bytes. This way the returned values
     * better match the values DAOS expects to receive, which are also in terms
     * of numbers of elements. */
    if ((dset->io_cache.file_sel_iter_id =
             H5Ssel_iter_create(file_space_id, 1, H5S_SEL_ITER_SHARE_WITH_DATASPACE)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to create file dataspace selection iterator");
    if ((dset->io_cache.mem_sel_iter_id =
             H5Ssel_iter_create(mem_space_id, 1, H5S_SEL_ITER_SHARE_WITH_DATASPACE)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL,
                     "unable to create memory dataspace selection iterator");

    /* Setup selected chunk info buffer */
    switch (dset->dcpl_cache.layout) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            dset->io_cache.chunk_info        = &dset->io_cache.single_chunk_info;
            dset->io_cache.chunk_info_nalloc = 1;
            break;

        case H5D_CHUNKED:
            dset->io_cache.chunk_info        = NULL;
            dset->io_cache.chunk_info_nalloc = 0;
            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "dataset has invalid storage layout type");
    } /* end switch */

    dset->io_cache.filled = TRUE;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_dset_fill_io_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_io_int_task
 *
 * Purpose:     Asynchronous version of H5Dread()/H5Dwrite().
 *`
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dset_io_int_task(tse_task_t *task)
{
    H5_daos_io_task_ud_t *udata      = NULL;
    tse_task_t           *first_task = NULL;
    tse_task_t           *dep_task   = NULL;
    htri_t                need_tconv;
    int                   ret;
    int                   ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset I/O task");

    assert(udata->end_task);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_DATASET);

    /* Check if datatype conversion is needed */
    if ((need_tconv = H5_daos_need_tconv(udata->dset->file_type_id, udata->mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOMPARE, -H5_DAOS_H5_GET_ERROR,
                     "can't check if type conversion is needed");

    /* Call actual I/O routine */
    switch (udata->io_type) {
        case IO_READ:
            if (H5_daos_dataset_read_int(udata->dset, udata->mem_type_id, udata->mem_space_id,
                                         udata->file_space_id, need_tconv, udata->buf.rbuf, udata->end_task,
                                         udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, -H5_DAOS_H5_GET_ERROR,
                             "failed to read data from dataset");
            break;

        case IO_WRITE:
            if (H5_daos_dataset_write_int(udata->dset, udata->mem_type_id, udata->mem_space_id,
                                          udata->file_space_id, need_tconv, udata->buf.wbuf, udata->end_task,
                                          udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, -H5_DAOS_H5_COPY_ERROR,
                             "failed to write data to dataset");
            break;
    } /* end switch */

done:
    if (udata) {
        /* Schedule first task */
        if (first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule final task for dataset I/O: %s",
                         H5_daos_err_to_string(ret));

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "dataset I/O task";
        } /* end if */
    }     /* end if */
    else {
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);
        assert(!first_task);
    } /* end else */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_dset_io_int_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_io_int_end_task
 *
 * Purpose:     Finalizes an asynchronous I/O task.
 *`
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dset_io_int_end_task(tse_task_t *task)
{
    H5_daos_io_task_ud_t *udata     = NULL;
    int                   ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset I/O task");

    assert(task == udata->end_task);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ_DONE(udata->req);

    /* Free IDs */
    if (H5Tclose(udata->mem_type_id) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close memory datatype");
    if (udata->mem_space_id != H5S_ALL && H5Sclose(udata->mem_space_id) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close memory dataspace");
    if (udata->file_space_id != H5S_ALL && H5Sclose(udata->file_space_id) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close file dataspace");

    /* Close dataset */
    if (H5_daos_dataset_close_real(udata->dset) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR,
                     "can't close dataset used for I/O");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "dataset I/O end task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free udata */
    udata = DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_dset_io_int_end_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_read_int
 *
 * Purpose:     Internal version of H5_daos_dataset_read().
 *`
 * Return:      Success:        0
 *              Failure:        -1, dataset not read.
 *
 * Programmer:  Neil Fortner
 *              July. 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_read_int(H5_daos_dset_t *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                         htri_t need_tconv, void *buf, tse_task_t *_end_task, H5_daos_req_t *req,
                         tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_select_chunk_info_t *chunk_info = NULL; /* Array of info for each chunk selected in the file */
    H5_daos_chunk_io_func        single_chunk_read_func;
    uint64_t                     i;
    size_t                       nchunks_sel;
    hid_t                        real_file_space_id;
    hid_t                        real_mem_space_id;
    int                          ndims;
    hssize_t                     num_elem_file = -1, num_elem_mem;
    tse_task_t                  *io_task       = NULL;
    tse_task_t                  *end_task      = _end_task;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    assert(dset);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Get dataspace extent */
    if ((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions");

    /* Get "real" space ids */
    if (file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;
    if (mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else
        real_mem_space_id = mem_space_id;

    /* Get number of elements in selections */
    if ((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in file selection");
    if ((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in memory selection");

    /* Various sanity and special case checks */
    if (num_elem_file != num_elem_mem)
        D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL,
                     "number of elements selected in file and memory dataspaces is different");
    if (num_elem_file && !buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "read buffer is NULL but selection has >0 elements");
    if (num_elem_file == 0)
        D_GOTO_DONE(SUCCEED);

    /* Fill dataset I/O cache if it hasn't already been filled */
    if (!dset->io_cache.filled && H5_daos_dset_fill_io_cache(dset, real_file_space_id, real_mem_space_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize dataset I/O cache");

    /* Check for the dataset having a chunked storage layout. If it does not,
     * simply set up the dataset as a single "chunk".
     */
    switch (dset->dcpl_cache.layout) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            chunk_info  = dset->io_cache.chunk_info;
            nchunks_sel = 1;

            /* Set up "single-chunk dataset", with the "chunk" starting at coordinate 0 */
            chunk_info->fspace_id         = real_file_space_id;
            chunk_info->mspace_id         = real_mem_space_id;
            chunk_info->num_elem_sel_file = num_elem_file;
            memset(chunk_info->chunk_coords, 0, sizeof(chunk_info->chunk_coords));

            break;

        case H5D_CHUNKED:
            /* Get the coordinates of the currently selected chunks in the file, setting up memory and file
             * dataspaces for them */
            if (H5_daos_get_selected_chunk_info(&dset->dcpl_cache, real_file_space_id, real_mem_space_id,
                                                &dset->io_cache.chunk_info, &dset->io_cache.chunk_info_nalloc,
                                                &nchunks_sel) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selected chunk info");
            chunk_info = dset->io_cache.chunk_info;

            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                         "invalid, unknown or unsupported dataset storage layout type");
    } /* end switch */
    assert(nchunks_sel > 0);

    /* Setup the appropriate function for reading the selected chunks */
    if (need_tconv)
        /* Type conversion necessary */
        single_chunk_read_func = H5_daos_dataset_io_types_unequal;
    else
        /* No type conversion necessary */
        single_chunk_read_func = H5_daos_dataset_io_types_equal;

    /* Set up coordination metatasks if there is more than one chunk selected */
    if (nchunks_sel > 1) {
        /* Set up empty first task for coordination if there isn't one already */
        if (!*first_task) {
            if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, first_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create first metatask for dataset read");
            *dep_task = *first_task;
        } /* end if */

        /* Set up empty end task for coordination if not already provided */
        if (!end_task) {
            if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &end_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create last metatask for dataset read");
        }
    } /* end if */

    /* Perform I/O on each chunk selected */
    for (i = 0; i < nchunks_sel; i++) {
        io_task = *dep_task;
        if (single_chunk_read_func(&chunk_info[i], dset, (uint64_t)ndims, mem_type_id, IO_READ, buf, req,
                                   first_task, &io_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "dataset read failed");

        /* Set up dependency on io_task for end task */
        assert(io_task);
        if (end_task && 0 != (ret = tse_task_register_deps(end_task, 1, &io_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependency on chunk I/O task: %s",
                         H5_daos_err_to_string(ret));
    } /* end for */

done:
    /* Schedule end_task if appropriate and update *dep_task */
    if (end_task) {
        if (0 != (ret = tse_task_schedule(end_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule end task for IO operation: %s",
                         H5_daos_err_to_string(ret));
        *dep_task = end_task;
    } /* end if */
    else
        *dep_task = io_task;

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_read_int() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_read
 *
 * Purpose:     Reads raw data from a dataset into a buffer.
 *`
 * Return:      Success:        0
 *              Failure:        -1, dataset not read.
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
#if H5VL_VERSION >= 3
herr_t
H5_daos_dataset_read(size_t count, void *_dset[], hid_t mem_type_id[], hid_t mem_space_id[],
                     hid_t file_space_id[], hid_t dxpl_id, void *buf[], void **req)
#else
herr_t
H5_daos_dataset_read(void *_dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id,
                     void *buf, void **req)
#endif
{
    H5_daos_dset_t       *dset       = NULL;
    H5_daos_io_task_ud_t *task_ud    = NULL;
    tse_task_t           *io_task    = NULL;
    tse_task_t           *first_task = NULL;
    tse_task_t           *dep_task   = NULL;
    H5_daos_req_t        *int_req    = NULL;
    htri_t                need_tconv = FALSE;
    hid_t                 req_dxpl_id;
    hid_t                 local_mem_type_id   = H5I_INVALID_HID;
    hid_t                 local_mem_space_id  = H5I_INVALID_HID;
    hid_t                 local_file_space_id = H5I_INVALID_HID;
    void                 *local_buf           = NULL;
    int                   ret;
    herr_t                ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    /* Set convenience variables to handle VOL structure versioning */
#if H5VL_VERSION >= 3
    dset                = (H5_daos_dset_t *)_dset[0];
    local_mem_type_id   = mem_type_id[0];
    local_mem_space_id  = mem_space_id[0];
    local_file_space_id = file_space_id[0];
    local_buf           = buf[0];
#else
    dset                = (H5_daos_dset_t *)_dset;
    local_mem_type_id   = mem_type_id;
    local_mem_space_id  = mem_space_id;
    local_file_space_id = file_space_id;
    local_buf           = buf;
#endif

    if (!dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL");
    if (H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset");

#if H5VL_VERSION >= 3
    if (count != 1)
        D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "multi-dataset I/O is currently unsupported");
#endif

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* If the dataset's datatype is complete, check if type conversion is needed
     */
    if (dset->obj.item.open_req->status == 0 || dset->obj.item.created) {
        /* Check if datatype conversion is needed */
        if ((need_tconv = H5_daos_need_tconv(dset->file_type_id, local_mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
        req_dxpl_id = need_tconv ? dxpl_id : H5P_DATASET_XFER_DEFAULT;
    } /* end if */
    else
        req_dxpl_id = dxpl_id;

    /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
    if (NULL == (int_req = H5_daos_req_create(dset->obj.item.file, "dataset read", dset->obj.item.open_req,
                                              NULL, NULL, req_dxpl_id)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Check if we can call the internal routine directly -  the dataset open
     * must be complete and there must not be an in-flight set_extent. */
    if ((dset->obj.item.open_req->status == 0) && (dset->cur_set_extent_space_id == H5I_INVALID_HID)) {
        /* Call internal routine */
        if (H5_daos_dataset_read_int(dset, local_mem_type_id, local_mem_space_id, local_file_space_id,
                                     need_tconv, local_buf, NULL, int_req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "failed to read data from dataset");
    } /* end if */
    else {
        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_io_task_ud_t *)DV_calloc(sizeof(H5_daos_io_task_ud_t))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate space for I/O task udata struct");
        task_ud->req           = int_req;
        task_ud->io_type       = IO_READ;
        task_ud->dset          = dset;
        task_ud->mem_type_id   = H5I_INVALID_HID;
        task_ud->mem_space_id  = H5I_INVALID_HID;
        task_ud->file_space_id = H5I_INVALID_HID;
        task_ud->buf.rbuf      = local_buf;

        /* Copy dataspaces and datatype */
        if ((task_ud->mem_type_id = H5Tcopy(local_mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy memory type ID");
        if (local_mem_space_id == H5S_ALL)
            task_ud->mem_space_id = H5S_ALL;
        else if ((task_ud->mem_space_id = H5Scopy(local_mem_space_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy memory space ID");
        if (local_file_space_id == H5S_ALL)
            task_ud->file_space_id = H5S_ALL;
        else if ((task_ud->file_space_id = H5Scopy(local_file_space_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy file space ID");

        /* Create end task for reading data */
        if (H5_daos_create_task(H5_daos_dset_io_int_end_task, 0, NULL, NULL, NULL, task_ud,
                                &task_ud->end_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                         "can't create task to finish performing I/O operation");

        /* Create task to read data */
        if (H5_daos_create_task(H5_daos_dset_io_int_task, 0, NULL, NULL, NULL, task_ud, &io_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to perform I/O operation");

        /* Save task to be scheduled later and give it a reference to req and
         * dset */
        assert(!first_task);
        first_task = io_task;
        dep_task   = task_ud->end_task;
        dset->obj.item.rc++;
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &dset->obj.item, H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "dataset read failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        if (task_ud->mem_type_id >= 0 && H5Tclose(task_ud->mem_type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close memory datatype");
        if (task_ud->mem_space_id >= 0 && task_ud->mem_space_id != H5S_ALL &&
            H5Sclose(task_ud->mem_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close memory dataspace");
        if (task_ud->file_space_id >= 0 && task_ud->file_space_id != H5S_ALL &&
            H5Sclose(task_ud->file_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close file dataspace");
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_read() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_write_int
 *
 * Purpose:     Internal version of H5_daos_dataset_write().
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              July, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_write_int(H5_daos_dset_t *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                          htri_t need_tconv, const void *buf, tse_task_t *_end_task, H5_daos_req_t *req,
                          tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_select_chunk_info_t *chunk_info = NULL; /* Array of info for each chunk selected in the file */
    H5_daos_chunk_io_func        single_chunk_write_func;
    uint64_t                     i;
    size_t                       nchunks_sel;
    hid_t                        real_file_space_id;
    hid_t                        real_mem_space_id;
    int                          ndims;
    hssize_t                     num_elem_file = -1, num_elem_mem;
    tse_task_t                  *io_task       = NULL;
    tse_task_t                  *end_task      = _end_task;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    assert(dset);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(dset->obj.item.file->flags & H5F_ACC_RDWR);

    /* Get dataspace extent */
    if ((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions");

    /* Get "real" space ids */
    if (file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;
    if (mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else
        real_mem_space_id = mem_space_id;

    /* Get number of elements in selections */
    if ((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in file selection");
    if ((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in memory selection");

    /* Various sanity and special case checks */
    if (num_elem_file != num_elem_mem)
        D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL,
                     "number of elements selected in file and memory dataspaces is different");
    if (num_elem_file && !buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "write buffer is NULL but selection has >0 elements");
    if (num_elem_file == 0)
        D_GOTO_DONE(SUCCEED);

    /* Fill dataset I/O cache if it hasn't already been filled */
    if (!dset->io_cache.filled && H5_daos_dset_fill_io_cache(dset, real_file_space_id, real_mem_space_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize dataset I/O cache");

    /* Check for the dataset having a chunked storage layout. If it does not,
     * simply set up the dataset as a single "chunk".
     */
    switch (dset->dcpl_cache.layout) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            chunk_info  = dset->io_cache.chunk_info;
            nchunks_sel = 1;

            /* Set up "single-chunk dataset", with the "chunk" starting at coordinate 0 */
            chunk_info->fspace_id         = real_file_space_id;
            chunk_info->mspace_id         = real_mem_space_id;
            chunk_info->num_elem_sel_file = num_elem_file;
            memset(chunk_info->chunk_coords, 0, sizeof(chunk_info->chunk_coords));

            break;

        case H5D_CHUNKED:
            /* Get the coordinates of the currently selected chunks in the file, setting up memory and file
             * dataspaces for them */
            if (H5_daos_get_selected_chunk_info(&dset->dcpl_cache, real_file_space_id, real_mem_space_id,
                                                &dset->io_cache.chunk_info, &dset->io_cache.chunk_info_nalloc,
                                                &nchunks_sel) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selected chunk info");
            chunk_info = dset->io_cache.chunk_info;

            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                         "invalid, unknown or unsupported dataset storage layout type");
    } /* end switch */
    assert(nchunks_sel > 0);

    /* Setup the appropriate function for writing the selected chunks */
    if (need_tconv)
        /* Type conversion necessary */
        single_chunk_write_func = H5_daos_dataset_io_types_unequal;
    else
        /* No type conversion necessary */
        single_chunk_write_func = H5_daos_dataset_io_types_equal;

    /* Set up coordination metatasks if there is more than one chunk selected */
    if (nchunks_sel > 1) {
        /* Set up empty first task for coordination if there isn't one already */
        if (!*first_task) {
            if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, first_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                             "can't create first metatask for dataset write");
            *dep_task = *first_task;
        } /* end if */

        /* Set up empty end task for coordination if not already provided */
        if (!end_task) {
            if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &end_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create last metatask for dataset write");
        }
    } /* end if */

    /* Perform I/O on each chunk selected */
    for (i = 0; i < nchunks_sel; i++) {
        union {
            const void *const_buf;
            void       *buf;
        } safe_buf = {.const_buf = buf};

        io_task = *dep_task;
        if (single_chunk_write_func(&chunk_info[i], dset, (uint64_t)ndims, mem_type_id, IO_WRITE,
                                    safe_buf.buf, req, first_task, &io_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "dataset write failed");

        /* Set up dependency on io_task for end task */
        assert(io_task);
        if (end_task && 0 != (ret = tse_task_register_deps(end_task, 1, &io_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependency on chunk I/O task: %s",
                         H5_daos_err_to_string(ret));
    } /* end for */

done:
    /* Schedule end_task if appropriate and update *dep_task */
    if (end_task) {
        if (0 != (ret = tse_task_schedule(end_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule end task for IO operation: %s",
                         H5_daos_err_to_string(ret));
        *dep_task = end_task;
    } /* end if */
    else
        *dep_task = io_task;

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_write_int() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_write
 *
 * Purpose:     Writes raw data from a buffer into a dataset.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
#if H5VL_VERSION >= 3
herr_t
H5_daos_dataset_write(size_t count, void *_dset[], hid_t mem_type_id[], hid_t mem_space_id[],
                      hid_t file_space_id[], hid_t dxpl_id, const void *buf[], void **req)
#else
herr_t
H5_daos_dataset_write(void *_dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id,
                      const void *buf, void **req)
#endif
{
    H5_daos_dset_t       *dset       = NULL;
    H5_daos_io_task_ud_t *task_ud    = NULL;
    tse_task_t           *io_task    = NULL;
    tse_task_t           *first_task = NULL;
    tse_task_t           *dep_task   = NULL;
    H5_daos_req_t        *int_req    = NULL;
    htri_t                need_tconv = FALSE;
    hid_t                 req_dxpl_id;
    hid_t                 local_mem_type_id   = H5I_INVALID_HID;
    hid_t                 local_mem_space_id  = H5I_INVALID_HID;
    hid_t                 local_file_space_id = H5I_INVALID_HID;
    const void           *local_buf           = NULL;
    int                   ret;
    herr_t                ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    /* Set convenience variables to handle VOL structure versioning */
#if H5VL_VERSION >= 3
    dset                = (H5_daos_dset_t *)_dset[0];
    local_mem_type_id   = mem_type_id[0];
    local_mem_space_id  = mem_space_id[0];
    local_file_space_id = file_space_id[0];
    local_buf           = buf[0];
#else
    dset                = (H5_daos_dset_t *)_dset;
    local_mem_type_id   = mem_type_id;
    local_mem_space_id  = mem_space_id;
    local_file_space_id = file_space_id;
    local_buf           = buf;
#endif

    if (!dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL");
    if (H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset");

#if H5VL_VERSION >= 3
    if (count != 1)
        D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "multi-dataset I/O is currently unsupported");
#endif

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check for write access */
    if (!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file");

    /* If the dataset's datatype is complete, check if type conversion is needed
     */
    if (dset->obj.item.open_req->status == 0 || dset->obj.item.created) {
        /* Check if datatype conversion is needed */
        if ((need_tconv = H5_daos_need_tconv(dset->file_type_id, local_mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
        req_dxpl_id = need_tconv ? dxpl_id : H5P_DATASET_XFER_DEFAULT;
    } /* end if */
    else
        req_dxpl_id = dxpl_id;

    /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
    if (NULL == (int_req = H5_daos_req_create(dset->obj.item.file, "dataset write", dset->obj.item.open_req,
                                              NULL, NULL, req_dxpl_id)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Check if we can call the internal routine directly - the dataset open
     * must be complete and there must not be an in-flight set_extent. */
    if ((dset->obj.item.open_req->status == 0) && (dset->cur_set_extent_space_id == H5I_INVALID_HID)) {
        /* Call internal routine */
        if (H5_daos_dataset_write_int(dset, local_mem_type_id, local_mem_space_id, local_file_space_id,
                                      need_tconv, local_buf, NULL, int_req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "failed to write data to dataset");
    } /* end if */
    else {
        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_io_task_ud_t *)DV_calloc(sizeof(H5_daos_io_task_ud_t))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate space for I/O task udata struct");
        task_ud->req           = int_req;
        task_ud->io_type       = IO_WRITE;
        task_ud->dset          = dset;
        task_ud->mem_type_id   = H5I_INVALID_HID;
        task_ud->mem_space_id  = H5I_INVALID_HID;
        task_ud->file_space_id = H5I_INVALID_HID;
        task_ud->buf.wbuf      = local_buf;

        /* Copy dataspaces and datatype */
        if ((task_ud->mem_type_id = H5Tcopy(local_mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy memory type ID");
        if (local_mem_space_id == H5S_ALL)
            task_ud->mem_space_id = H5S_ALL;
        else if ((task_ud->mem_space_id = H5Scopy(local_mem_space_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy memory space ID");
        if (local_file_space_id == H5S_ALL)
            task_ud->file_space_id = H5S_ALL;
        else if ((task_ud->file_space_id = H5Scopy(local_file_space_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy file space ID");

        /* Create end task for writing data */
        if (H5_daos_create_task(H5_daos_dset_io_int_end_task, 0, NULL, NULL, NULL, task_ud,
                                &task_ud->end_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                         "can't create task to finish performing I/O operation");

        /* Create task to write data */
        if (H5_daos_create_task(H5_daos_dset_io_int_task, 0, NULL, NULL, NULL, task_ud, &io_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to perform I/O operation");

        /* Save task to be scheduled later and give it a reference to req and
         * dset */
        assert(!first_task);
        first_task = io_task;
        dep_task   = task_ud->end_task;
        dset->obj.item.rc++;
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &dset->obj.item, H5_DAOS_OP_TYPE_WRITE,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "dataset write failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        if (task_ud->mem_type_id >= 0 && H5Tclose(task_ud->mem_type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close memory datatype");
        if (task_ud->mem_space_id >= 0 && task_ud->mem_space_id != H5S_ALL &&
            H5Sclose(task_ud->mem_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close memory dataspace");
        if (task_ud->file_space_id >= 0 && task_ud->file_space_id != H5S_ALL &&
            H5Sclose(task_ud->file_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close file dataspace");
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_write() */

#if H5VL_VERSION >= 2

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_get_realize
 *
 * Purpose:     Future ID "realize" callback for H5_daos_dataset_get
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              December, 2020
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_get_realize(void *future_object, hid_t *actual_object_id)
{
    H5_daos_dset_get_ud_t *udata     = (H5_daos_dset_get_ud_t *)future_object;
    int                    ret_value = 0;

    H5_daos_inc_api_cnt();

    /* Handle errors in previous tasks.  Short circuit is still a failure here.
     */
    if (udata->req->status <= -H5_DAOS_SHORT_CIRCUIT) {
        D_GOTO_DONE(FAIL);
    } /* end if */

    /* Wait for the dataset to open if necessary */
    if (udata->dset->obj.item.open_req->status != 0) {
        if (H5_daos_progress(udata->dset->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
        if (udata->dset->obj.item.open_req->status != 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "dataset open failed");
    } /* end if */

    switch (udata->get_type) {
        case H5VL_DATASET_GET_DCPL: {
            /* Retrieve the dataset's creation property list */
            if ((*actual_object_id = H5Pcopy(udata->dset->dcpl_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset creation property list");

            /* Set dataset's object class on dcpl */
            if (H5_daos_set_oclass_from_oid(*actual_object_id, udata->dset->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");

            break;
        } /* end block */
        case H5VL_DATASET_GET_SPACE: {
            /* Retrieve the dataset's dataspace */
            if ((*actual_object_id = H5Scopy(udata->dset->space_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace ID of dataset");
            break;
        } /* end block */
        case H5VL_DATASET_GET_TYPE: {
            /* Retrieve the dataset's datatype */
            if ((*actual_object_id = H5Tcopy(udata->dset->type_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype ID of dataset");
            break;
        } /* end block */
        case H5VL_DATASET_GET_STORAGE_SIZE:
        case H5VL_DATASET_GET_DAPL:
        case H5VL_DATASET_GET_SPACE_STATUS:
            /* Should have been handled in top level function or get task */
        default:
            assert(0 && "can't realize this type of information from dataset");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_get_realize() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_get_discard
 *
 * Purpose:     Future ID "discard" callback for H5_daos_dataset_get
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              December, 2020
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_get_discard(void *future_object)
{
    H5_daos_dset_get_ud_t *udata     = (H5_daos_dset_get_ud_t *)future_object;
    int                    ret_value = 0;

    H5_daos_inc_api_cnt();

    if (udata) {
        /* Close dataset */
        if (H5_daos_dataset_close_real(udata->dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset used for I/O");

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_get_discard() */
#endif /* H5VL_VERSION >= 2 */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_get_task
 *
 * Purpose:     Asynchronous task for H5_daos_dataset_get
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dataset_get_task(tse_task_t *task)
{
    H5_daos_dset_get_ud_t *udata      = NULL;
    hssize_t               nelements  = 0;
    size_t                 dtype_size = 0;
    int                    ret_value  = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset I/O task");

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_DATASET);

    /* Verify dataset was successfully opened */
    if (udata->dset->obj.item.open_req->status != 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, -H5_DAOS_PREREQ_ERROR, "dataset open is incomplete");

    assert(udata->get_type == H5VL_DATASET_GET_STORAGE_SIZE);
    assert(udata->hsize_out);

    *udata->hsize_out = 0;

    if (H5I_INVALID_HID == udata->dset->space_id || H5I_INVALID_HID == udata->dset->type_id)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, -H5_DAOS_BAD_VALUE,
                     "can't get dataset's dataspace or datatype");

    /* Return the in-memory size of the data */
    if ((nelements = H5Sget_simple_extent_npoints(udata->dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR,
                     "can't get number of elements in dataset's dataspace");
    if (0 == (dtype_size = H5Tget_size(udata->dset->type_id)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get dataset's type size");
    *udata->hsize_out = (hsize_t)nelements * dtype_size;

done:
    if (udata) {
        /* Close dataset */
        if (H5_daos_dataset_close_real(udata->dset) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR,
                         "can't close dataset used for I/O");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "dataset get task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_get_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_get
 *
 * Purpose:     Gets certain information about a dataset
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_get(void *_dset, H5VL_dataset_get_args_t *get_args, hid_t H5VL_DAOS_UNUSED dxpl_id,
                    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_dset_t        *dset       = (H5_daos_dset_t *)_dset;
    H5_daos_dset_get_ud_t *get_udata  = NULL;
    tse_task_t            *first_task = NULL;
    tse_task_t            *dep_task   = NULL;
    H5_daos_req_t         *int_req    = NULL;
    int                    ret;
    herr_t                 ret_value = SUCCEED; /* Return value */

    H5_daos_inc_api_cnt();

    if (!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!get_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (get_args->op_type) {
        case H5VL_DATASET_GET_DCPL: {
            hid_t *plist_id = &get_args->args.get_dcpl.dcpl_id;

            if (!plist_id)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output argument not supplied");

            /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
            if (NULL ==
                (int_req = H5_daos_req_create(dset->obj.item.file, "get dataset create property list",
                                              dset->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Allocate future ID and set up async task if necessary */
            if (!dset->obj.item.created && dset->obj.item.open_req->status != 0) {
#if H5VL_VERSION >= 2
                /* Allocate udata struct */
                if (NULL == (get_udata = (H5_daos_dset_get_ud_t *)DV_calloc(sizeof(H5_daos_dset_get_ud_t))))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                 "can't allocate space for dataset get udata struct");

                /* Register future ID for dcpl */
                if ((*plist_id = H5Iregister_future(H5I_GENPROP_LST, get_udata, H5_daos_dataset_get_realize,
                                                    H5_daos_dataset_get_discard)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "can't register future ID");
            } /* end if */
            else
#else
                /* No future ID support, wait for the dataset to open */
                if (H5_daos_progress(dset->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dset->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "attribute open failed");
            } /* end if */
#endif
            {
                /* Retrieve the dataset's creation property list */
                if ((*plist_id = H5Pcopy(dset->dcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset creation property list");

                /* Set dataset's object class on dcpl */
                if (H5_daos_set_oclass_from_oid(*plist_id, dset->obj.oid) < 0)
                    D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");
            } /* end else/block */

            break;
        } /* end block */
        case H5VL_DATASET_GET_DAPL: {
            hid_t *plist_id = &get_args->args.get_dapl.dapl_id;

            if (!plist_id)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output argument not supplied");

            /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
            if (NULL ==
                (int_req = H5_daos_req_create(dset->obj.item.file, "get dataset access property list",
                                              dset->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Retrieve the dataset's access property list */
            if ((*plist_id = H5Pcopy(dset->dapl_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset access property list");

            break;
        } /* end block */
        case H5VL_DATASET_GET_SPACE: {
            hid_t *ret_id = &get_args->args.get_space.space_id;

            if (!ret_id)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output argument not supplied");

            /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
            if (NULL ==
                (int_req = H5_daos_req_create(dset->obj.item.file, "get dataset dataspace",
                                              dset->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Allocate future ID and set up async task if necessary */
            if (!dset->obj.item.created && dset->obj.item.open_req->status != 0) {
#if H5VL_VERSION >= 2
                /* Allocate udata struct */
                if (NULL == (get_udata = (H5_daos_dset_get_ud_t *)DV_calloc(sizeof(H5_daos_dset_get_ud_t))))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                 "can't allocate space for dataset get udata struct");

                /* Register future ID for dataspace */
                if ((*ret_id = H5Iregister_future(H5I_DATASPACE, get_udata, H5_daos_dataset_get_realize,
                                                  H5_daos_dataset_get_discard)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "can't register future ID");
            } /* end if */
            else
#else
                /* No future ID support, wait for the dataset to open */
                if (H5_daos_progress(dset->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dset->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "attribute open failed");
            } /* end if */
#endif
            {
                /* Retrieve the dataset's dataspace.  Use
                 * cur_set_extent_space_id if present, otherwise just use
                 * space_id. */
                if (dset->cur_set_extent_space_id >= 0) {
                    if ((*ret_id = H5Scopy(dset->cur_set_extent_space_id)) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace ID of dataset");
                } /* end if */
                else if ((*ret_id = H5Scopy(dset->space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace ID of dataset");
            } /* end else/block */

            break;
        } /* end block */
        case H5VL_DATASET_GET_SPACE_STATUS: {
            H5D_space_status_t *allocation = get_args->args.get_space_status.status;

            if (!allocation)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output argument not supplied");

            /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
            if (NULL ==
                (int_req = H5_daos_req_create(dset->obj.item.file, "get dataset dataspace status",
                                              dset->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Retrieve the dataset's space status */
            *allocation = H5D_SPACE_STATUS_ALLOCATED;
            break;
        } /* end block */
        case H5VL_DATASET_GET_TYPE: {
            hid_t *ret_id = &get_args->args.get_type.type_id;

            if (!ret_id)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output argument not supplied");

            /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
            if (NULL ==
                (int_req = H5_daos_req_create(dset->obj.item.file, "get dataset datatype",
                                              dset->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Allocate future ID and set up async task if necessary */
            if (!dset->obj.item.created && dset->obj.item.open_req->status != 0) {
#if H5VL_VERSION >= 2
                /* Allocate udata struct */
                if (NULL == (get_udata = (H5_daos_dset_get_ud_t *)DV_calloc(sizeof(H5_daos_dset_get_ud_t))))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                 "can't allocate space for dataset get udata struct");

                /* Register future ID for datatype */
                if ((*ret_id = H5Iregister_future(H5I_DATATYPE, get_udata, H5_daos_dataset_get_realize,
                                                  H5_daos_dataset_get_discard)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "can't register future ID");
            } /* end if */
            else
#else
                /* No future ID support, wait for the dataset to open */
                if (H5_daos_progress(dset->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dset->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "attribute open failed");
            } /* end if */
#endif
            {
                /* Retrieve the dataset's datatype */
                if ((*ret_id = H5Tcopy(dset->type_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype ID of dataset");
            } /* end else/block */

            break;
        } /* end block */
        case H5VL_DATASET_GET_STORAGE_SIZE: {
            hsize_t *storage_size = get_args->args.get_storage_size.storage_size;
            hssize_t nelements    = 0;
            size_t   dtype_size   = 0;

            if (!storage_size)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "output argument not supplied");

            /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
            if (NULL ==
                (int_req = H5_daos_req_create(dset->obj.item.file, "get dataset storage size",
                                              dset->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Set up async task if necessary */
            if (!dset->obj.item.created && dset->obj.item.open_req->status != 0) {
                /* Allocate udata struct */
                if (NULL == (get_udata = (H5_daos_dset_get_ud_t *)DV_calloc(sizeof(H5_daos_dset_get_ud_t))))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                 "can't allocate space for dataset get udata struct");

                /* Set output value pointer in udata */
                get_udata->hsize_out = storage_size;
            } /* end if */
            else {
                *storage_size = 0;

                if (H5I_INVALID_HID == dset->space_id || H5I_INVALID_HID == dset->type_id)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset's dataspace or datatype");

                /* Return the in-memory size of the data */
                if ((nelements = H5Sget_simple_extent_npoints(dset->space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                                 "can't get number of elements in dataset's dataspace");
                if (0 == (dtype_size = H5Tget_size(dset->type_id)))
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get dataset's type size");
                *storage_size = (hsize_t)nelements * dtype_size;
            } /* end else */

            break;
        }
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from dataset");
    } /* end switch */

    /* Start async operation if we allocated task_udata */
    if (get_udata) {
        tse_task_t *get_task;

        assert(int_req);

        /* Finish filling in task_udata */
        get_udata->req      = int_req;
        get_udata->dset     = dset;
        get_udata->get_type = get_args->op_type;

        /* Create task to get dataset info.  USe empty task for everything but
         * storage size, for the other get types the realize callback will
         * do the actual retrieval. */
        if (H5_daos_create_task(get_args->op_type == H5VL_DATASET_GET_STORAGE_SIZE
                                    ? H5_daos_dataset_get_task
                                    : H5_daos_metatask_autocomplete,
                                0, NULL, NULL, NULL, get_udata, &get_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to perform get operation");

        /* Save task to be scheduled later and give it a reference to req and
         * dset */
        assert(!first_task);
        first_task = get_task;
        dep_task   = get_task;
        dset->obj.item.rc++;
        int_req->rc++;
        get_udata = NULL;
    } /* end if */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &dset->obj.item, H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL,
                             "dataset get operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    assert(!get_udata || ret_value < 0);
    get_udata = DV_free(get_udata);

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_specific
 *
 * Purpose:     Performs a dataset "specific" operation
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
H5_daos_dataset_specific(void *_item, H5VL_dataset_specific_args_t *specific_args, hid_t dxpl_id,
                         void H5VL_DAOS_UNUSED **req)
{
    H5_daos_dset_t *dset       = (H5_daos_dset_t *)_item;
    H5_daos_req_t  *int_req    = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    hbool_t         collective_md_read;
    hbool_t         collective_md_write;
    hbool_t         must_coll_req = FALSE;
    hid_t           dapl_id       = H5P_DATASET_ACCESS_DEFAULT;
    int             ret;
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!specific_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");
    if (H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Determine metadata I/O mode setting (collective vs. independent)
     * for metadata reads and writes according to file-wide setting on FAPL.
     */
    H5_DAOS_GET_METADATA_IO_MODES(dset->obj.item.file, dapl_id, H5P_DATASET_ACCESS_DEFAULT,
                                  collective_md_read, collective_md_write, H5E_DATASET, FAIL);

    switch (specific_args->op_type) {
        case H5VL_DATASET_SET_EXTENT: {
            const hsize_t *size = specific_args->args.set_extent.size;

            if (!size)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size parameter is NULL");

            /* Wait for the dataset to open if necessary */
            if (!dset->obj.item.created && dset->obj.item.open_req->status != 0) {
                if (H5_daos_progress(dset->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dset->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "dataset open failed");
            } /* end if */

            if (H5D_CHUNKED != dset->dcpl_cache.layout)
                D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "dataset storage layout is not chunked");

            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(dset->obj.item.file, "set dataset extent",
                                                      dset->obj.item.open_req, NULL, NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
            /* Start transaction */
            if (0 != (ret = daos_tx_open(dset->obj.item.file->coh, &int_req->th, 0, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't start transaction");
            int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

            /* Call main routine */
            must_coll_req = collective_md_write;
            if (H5_daos_dataset_set_extent(dset, size, collective_md_write, int_req, &first_task, &dep_task) <
                0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "failed to set dataset extent");

            break;
        } /* end block */

        case H5VL_DATASET_FLUSH: {
            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(dset->obj.item.file, "dataset flush",
                                                      dset->obj.item.open_req, NULL, NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            if (H5_daos_dataset_flush(dset, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't flush dataset");

            break;
        } /* end block */

        case H5VL_DATASET_REFRESH: {
            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(dset->obj.item.file, "dataset refresh",
                                                      dset->obj.item.open_req, NULL, NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
            /* Start transaction */
            if (0 !=
                (ret = daos_tx_open(dset->obj.item.file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't start transaction");
            int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

            /* Call main routine */
            if (H5_daos_dataset_refresh(dset, dxpl_id, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "failed to refresh dataset");

            break;
        } /* end block */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported dataset specific operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &dset->obj.item,
                                specific_args->op_type == H5VL_DATASET_SET_EXTENT ||
                                        specific_args->op_type == H5VL_DATASET_FLUSH
                                    ? H5_DAOS_OP_TYPE_WRITE_ORDERED
                                    : H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, must_coll_req, !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL,
                             "dataset specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* else */
    }     /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_close_real
 *
 * Purpose:     Internal version of H5_daos_dataset_close().
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_close_real(H5_daos_dset_t *dset)
{
    size_t i;
    int    ret;
    herr_t ret_value = SUCCEED;

    if (!dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL");
    if (H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset");

    if (--dset->obj.item.rc == 0) {
        /* Free dataset data structures */
        if (dset->obj.item.cur_op_pool)
            H5_daos_op_pool_free(dset->obj.item.cur_op_pool);
        if (dset->obj.item.open_req)
            if (H5_daos_req_free_int(dset->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");
        if (!daos_handle_is_inval(dset->obj.obj_oh))
            if (0 != (ret = daos_obj_close(dset->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "can't close dataset DAOS object: %s",
                             H5_daos_err_to_string(ret));
        if (dset->type_id != H5I_INVALID_HID && H5Idec_ref(dset->type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's datatype");
        if (dset->file_type_id != H5I_INVALID_HID && H5Idec_ref(dset->file_type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's file datatype");
        if (dset->space_id != H5I_INVALID_HID && H5Idec_ref(dset->space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's dataspace");
        if (dset->dcpl_id != H5I_INVALID_HID && dset->dcpl_id != H5P_DATASET_CREATE_DEFAULT)
            if (H5Idec_ref(dset->dcpl_id) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dcpl");
        if (dset->dapl_id != H5I_INVALID_HID && dset->dapl_id != H5P_DATASET_ACCESS_DEFAULT)
            if (H5Idec_ref(dset->dapl_id) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dapl");
        if (dset->fill_val)
            dset->fill_val = DV_free(dset->fill_val);
        /* Clear dataset I/O cache */
        if ((dset->io_cache.file_sel_iter_id > 0) && (H5Ssel_iter_close(dset->io_cache.file_sel_iter_id) < 0))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to close selection iterator");
        if ((dset->io_cache.mem_sel_iter_id > 0) && (H5Ssel_iter_close(dset->io_cache.mem_sel_iter_id) < 0))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to close selection iterator");
        if (dset->io_cache.chunk_info && (dset->io_cache.chunk_info != &dset->io_cache.single_chunk_info)) {
            H5_daos_select_chunk_info_t *chunk_info;

            for (i = 0; i < dset->io_cache.chunk_info_nalloc; i++) {
                chunk_info = &dset->io_cache.chunk_info[i];
                if ((chunk_info->fspace_id >= 0) && (H5Sclose(chunk_info->fspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close chunk file dataspace");
                if ((chunk_info->mspace_id >= 0) && (H5Sclose(chunk_info->mspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close chunk memory dataspace");
            }

            DV_free(dset->io_cache.chunk_info);
        }
        dset = H5FL_FREE(H5_daos_dset_t, dset);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_dataset_close_real() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_close
 *
 * Purpose:     Closes a DAOS HDF5 dataset.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_close(void *_dset, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_dset_t              *dset       = (H5_daos_dset_t *)_dset;
    H5_daos_obj_close_task_ud_t *task_ud    = NULL;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    H5_daos_req_t               *int_req    = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check if the dataset's request queue is NULL, if so we can close it
     * immediately.  Also close if the pool is empty and has no start task (and
     * hence does not depend on anything).  Also close if it is marked to close
     * nonblocking. */
    if (((dset->obj.item.open_req->status == 0 || dset->obj.item.open_req->status < -H5_DAOS_CANCELED) &&
         (!dset->obj.item.cur_op_pool || (dset->obj.item.cur_op_pool->type == H5_DAOS_OP_TYPE_EMPTY &&
                                          !dset->obj.item.cur_op_pool->start_task))) ||
        dset->obj.item.nonblocking_close) {
        if (H5_daos_dataset_close_real(dset) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset");
    } /* end if */
    else {
        tse_task_t *close_task = NULL;

        /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
        if (NULL ==
            (int_req = H5_daos_req_create(dset->obj.item.file, "dataset close", dset->obj.item.open_req, NULL,
                                          NULL, H5P_DATASET_XFER_DEFAULT)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_obj_close_task_ud_t *)DV_calloc(sizeof(H5_daos_obj_close_task_ud_t))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                         "can't allocate space for close task udata struct");
        task_ud->req  = int_req;
        task_ud->item = &dset->obj.item;

        /* Create task to close dataset */
        if (H5_daos_create_task(H5_daos_object_close_task, 0, NULL, NULL, NULL, task_ud, &close_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to close dataset");

        /* Save task to be scheduled later and give it a reference to req and
         * dset */
        assert(!first_task);
        first_task = close_task;
        dep_task   = close_task;
        /* No need to take a reference to dset here since the purpose is to
         * release the API's reference */
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &dset->obj.item, H5_DAOS_OP_TYPE_CLOSE,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't add request to request queue");
        dset = NULL;

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "dataset close failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_dataset_close() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_flush
 *
 * Purpose:     Flushes a DAOS dataset.  Creates a barrier task so all async
 *              ops created before the flush execute before all async ops
 *              created after the flush.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              July, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_flush(H5_daos_dset_t H5VL_DAOS_UNUSED *dset, H5_daos_req_t H5VL_DAOS_UNUSED *req,
                      tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *barrier_task = NULL;
    herr_t      ret_value    = SUCCEED;

    assert(dset);

    /* Create task that does nothing but complete itself.  Only necessary
     * because we can't enqueue a request that has no tasks */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &barrier_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create barrier task for dataset flush");

    /* Schedule barrier task (or save it to be scheduled later)  */
    assert(!*first_task);
    *first_task = barrier_task;
    *dep_task   = barrier_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_dataset_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_refresh_comp_cb
 *
 * Purpose:     Complete callback for asynchronous metadata fetch for
 *              dataset refresh operations.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dataset_refresh_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_omd_fetch_ud_t *udata;
    uint8_t                *p;
    int                     ret;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset dataspace read task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->fetch_metatask);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;
        size_t      daos_info_len = udata->md_rw_cb_ud.iod[0].iod_size;

        assert(udata->md_rw_cb_ud.req->file);
        assert(udata->md_rw_cb_ud.obj);
        assert(udata->md_rw_cb_ud.obj->item.type == H5I_DATASET);

        /* Verify iod size makes sense */
        if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_SPACE_BUF_SIZE)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "buffer length does not match expected value");

        if (udata->bcast_udata) {
            /* Reallocate dataspace buffer if necessary */
            if (daos_info_len > H5_DAOS_SPACE_BUF_SIZE) {
                udata->bcast_udata->bcast_udata.buffer = DV_free(udata->bcast_udata->bcast_udata.buffer);
                if (NULL == (udata->bcast_udata->bcast_udata.buffer =
                                 DV_malloc(daos_info_len + H5_DAOS_ENCODED_UINT64_T_SIZE)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized dataspace info");
                udata->bcast_udata->bcast_udata.buffer_len =
                    (int)daos_info_len + H5_DAOS_ENCODED_UINT64_T_SIZE;
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->bcast_udata->bcast_udata.buffer + H5_DAOS_ENCODED_UINT64_T_SIZE;
        } /* end if */
        else {
            /* Reallocate dataset info buffer if necessary */
            if (daos_info_len > H5_DAOS_SPACE_BUF_SIZE) {
                udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_free(udata->md_rw_cb_ud.sg_iov[0].iov_buf);
                if (NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(daos_info_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized dataspace info");
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->md_rw_cb_ud.sg_iov[0].iov_buf;
        } /* end else */

        /* Set up sgl */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], p, udata->md_rw_cb_ud.iod[0].iod_size);
        udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;

        /* Create task for reissued dataset dataspace read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                     H5_daos_dataset_refresh_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to read dataset dataspace");

        /* Schedule reissued dataset dataspace read task */
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule task to read dataset dataspace: %s",
                         H5_daos_err_to_string(ret));

        /* Relinquish control of the object fetch udata to the task's completion callback */
        udata = NULL;
    } /* end if */
    else {
        /* Handle errors in fetch task.  Only record error in udata->req_status
         * if it does not already contain an error (it could contain an error if
         * another task this task is not dependent on also failed). */
        if (task->dt_result < -H5_DAOS_PRE_ERROR &&
            udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = task->dt_result;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        } /* end if */
        else if (task->dt_result == 0) {
            H5_daos_dset_t *dset = (H5_daos_dset_t *)udata->md_rw_cb_ud.obj;
            hid_t           decoded_space;

            assert(udata->md_rw_cb_ud.req->file);
            assert(udata->md_rw_cb_ud.obj);
            assert(udata->md_rw_cb_ud.obj->item.type == H5I_DATASET);

            if (udata->bcast_udata) {
                /* Encode serialized dataspace length */
                p = udata->bcast_udata->bcast_udata.buffer;
                UINT64ENCODE(p, udata->md_rw_cb_ud.iod[0].iod_size)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Decode dataspace */
            if ((decoded_space = H5Sdecode(udata->md_rw_cb_ud.sg_iov[0].iov_buf)) < 0)
                D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR,
                             "can't deserialize dataspace");

            /* Close dataset's current dataspace ID */
            if (dset->space_id >= 0 && H5Sclose(dset->space_id) < 0) {
                H5Sclose(decoded_space);
                D_GOTO_ERROR(H5E_DATASPACE, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR,
                             "can't close dataset's old dataspace");
            }

            dset->space_id = decoded_space;
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up if this is the last fetch task */
    if (udata) {
        /* Close dataset */
        if (udata->md_rw_cb_ud.obj &&
            H5_daos_dataset_close_real((H5_daos_dset_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        if (udata->bcast_udata) {
            /* Clear broadcast buffer if there was an error */
            if (udata->md_rw_cb_ud.req->status < -H5_DAOS_INCOMPLETE)
                (void)memset(udata->bcast_udata->bcast_udata.buffer, 0,
                             (size_t)udata->bcast_udata->bcast_udata.count);
        } /* end if */
        else
            /* No broadcast, free buffer */
            DV_free(udata->md_rw_cb_ud.sg_iov[0].iov_buf);

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->fetch_metatask) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete fetch metatask */
        tse_task_complete(udata->fetch_metatask, ret_value);

        assert(!udata->md_rw_cb_ud.free_dkey);
        assert(!udata->md_rw_cb_ud.free_akeys);

        /* Free udata */
        DV_free(udata);
    } /* end if */

    return ret_value;
} /* end H5_daos_dataset_refresh_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_refresh
 *
 * Purpose:     Refreshes a DAOS dataset (reads the dataspace)
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              July, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_refresh(H5_daos_dset_t *dset, hid_t H5VL_DAOS_UNUSED dxpl_id, H5_daos_req_t *req,
                        tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_omd_fetch_ud_t *fetch_udata    = NULL;
    tse_task_t             *fetch_task     = NULL;
    uint8_t                *space_buf      = NULL;
    size_t                  space_buf_size = 0;
    int                     ret;
    herr_t                  ret_value = SUCCEED;

    assert(dset);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set initial size for dataspace buffer */
    space_buf_size = H5_DAOS_SPACE_BUF_SIZE;

    /* Allocate argument struct for fetch task */
    if (NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(sizeof(H5_daos_omd_fetch_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for fetch callback arguments");

    /* Set up operation to read dataspace size from dataset */

    /* Setup UD struct */
    fetch_udata->md_rw_cb_ud.obj = &dset->obj;
    fetch_udata->md_rw_cb_ud.req = req;
    fetch_udata->bcast_udata     = NULL;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                       H5_daos_int_md_key_size_g);
    fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

    /* Set up iod */
    daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_space_key_g,
                       H5_daos_space_key_size_g);
    fetch_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
    fetch_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
    fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    fetch_udata->md_rw_cb_ud.free_akeys = FALSE;

    /* Allocate initial dataspace buffer */
    if (NULL == (space_buf = DV_malloc(space_buf_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for serialized dataspace info");

    /* Set up sgl */
    daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], space_buf, H5_DAOS_SPACE_BUF_SIZE);
    fetch_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
    fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
    fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[0];
    fetch_udata->md_rw_cb_ud.free_sg_iov[0]   = TRUE;

    fetch_udata->md_rw_cb_ud.nr = 1u;

    fetch_udata->md_rw_cb_ud.task_name = "dataset refresh (read dataspace)";

    /* Create meta task for dataspace read. This empty task will be
     * completed when the read is finished by H5_daos_dataset_refresh_comp_cb.
     * We can't use fetch_task since it may not be completed by the first
     * fetch. */
    if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &fetch_udata->fetch_metatask) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create meta task for dataset dataspace read");

    /* Create task for dataset dataspace read */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_md_rw_prep_cb, H5_daos_dataset_refresh_comp_cb, fetch_udata,
                                 &fetch_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to read dataset dataspace");

    /* Schedule meta task */
    if (0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                     "can't schedule meta task for dataset dataspace read: %s", H5_daos_err_to_string(ret));

    /* Schedule object fetch task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                         "can't schedule task to fetch dataset dataspace: %s", H5_daos_err_to_string(ret));
    }
    else
        *first_task = fetch_task;
    req->rc++;
    dset->obj.item.rc++;
    *dep_task = fetch_udata->fetch_metatask;

    /* Relinquish control of the object fetch udata and dataspace buffer to the
     * task's completion callback */
    fetch_udata = NULL;
    space_buf   = NULL;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        space_buf   = DV_free(space_buf);
        fetch_udata = DV_free(fetch_udata);
    }

    assert(!space_buf);
    assert(!fetch_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_refresh() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_set_extent_comp_cb
 *
 * Purpose:     Completion callback for H5Dset_extent.  Like
 *              H5_daos_md_update_comp_cb but also updates the dataset
 *              dataspace.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dset_set_extent_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_dset_set_extent_ud_t *udata;
    unsigned                      i;
    int                           ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Clear cached new space ID on dataset */
    if (((H5_daos_dset_t *)udata->md_rw_cb_ud.obj)->cur_set_extent_space_id == udata->new_space_id)
        ((H5_daos_dset_t *)udata->md_rw_cb_ud.obj)->cur_set_extent_space_id = H5I_INVALID_HID;

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        /* Close old space ID */
        if (H5Sclose(((H5_daos_dset_t *)udata->md_rw_cb_ud.obj)->space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataspace");

        /* Set new dataspace in dataset struct */
        ((H5_daos_dset_t *)udata->md_rw_cb_ud.obj)->space_id = udata->new_space_id;
        udata->new_space_id                                  = H5I_INVALID_HID;
    } /* end if */

    /* Close object */
    if (udata->md_rw_cb_ud.obj && H5_daos_object_close(&udata->md_rw_cb_ud.obj->item) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Close new dataspace on error */
    if (udata->new_space_id >= 0) {
        assert(ret_value < 0 || task->dt_result < -H5_DAOS_PRE_ERROR);
        if (H5Sclose(udata->new_space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataspace");
    } /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = ret_value;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    if (udata->md_rw_cb_ud.free_dkey)
        DV_free(udata->md_rw_cb_ud.dkey.iov_buf);
    if (udata->md_rw_cb_ud.free_akeys)
        for (i = 0; i < udata->md_rw_cb_ud.nr; i++)
            DV_free(udata->md_rw_cb_ud.iod[i].iod_name.iov_buf);
    for (i = 0; i < udata->md_rw_cb_ud.nr; i++)
        if (udata->md_rw_cb_ud.free_sg_iov[i])
            DV_free(udata->md_rw_cb_ud.sg_iov[i].iov_buf);
    DV_free(udata);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_dset_set_extent_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_set_extent
 *
 * Purpose:     Changes the extent of a dataset
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              July, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_set_extent(H5_daos_dset_t *dset, const hsize_t *size, hbool_t collective, H5_daos_req_t *req,
                           tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_dset_set_extent_ud_t *update_cb_ud = NULL;
    tse_task_t                   *update_task  = NULL;
    hsize_t                       maxdims[H5S_MAX_RANK];
    int                           ndims;
    void                         *space_buf = NULL;
    int                           i;
    int                           ret;
    herr_t                        ret_value = SUCCEED;

    assert(dset);
    assert(size);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Check for write access */
    if (!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file");

    /* Get dataspace rank */
    if ((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get current dataspace rank");

    /* Get dataspace max dims */
    if (H5Sget_simple_extent_dims(dset->space_id, NULL, maxdims) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get current dataspace maximum dimensions");

    /* Make sure max dims aren't exceeded */
    for (i = 0; i < ndims; i++)
        if ((maxdims[i] != H5S_UNLIMITED) && (size[i] > maxdims[i]))
            D_GOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL,
                         "requested dataset dimensions exceed maximum dimensions");

    /* Allocate task udata */
    if (NULL ==
        (update_cb_ud = (H5_daos_dset_set_extent_ud_t *)DV_calloc(sizeof(H5_daos_dset_set_extent_ud_t))))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                     "can't allocate space for dataset set extent udata struct");
    update_cb_ud->md_rw_cb_ud.req       = req;
    update_cb_ud->md_rw_cb_ud.obj       = &dset->obj;
    update_cb_ud->md_rw_cb_ud.task_name = "dataset set extent";

    /* Copy dataset dataspace */
    if ((update_cb_ud->new_space_id = H5Scopy(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataspace");

    /* Set extent on new dataspace */
    if (H5Sset_extent_simple(update_cb_ud->new_space_id, ndims, size, maxdims) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set dataspace dimensions");

    /* Cache new space id on dataset struct - to be used by H5Dget_space to
     * avoid needing to wait until this set_extent completes */
    dset->cur_set_extent_space_id = update_cb_ud->new_space_id;

    /* Write new dataspace to dataset in file if this process should */
    if (!collective || (dset->obj.item.file->my_rank == 0)) {
        size_t space_size = 0;

        /* Encode dataspace */
        if (H5Sencode2(update_cb_ud->new_space_id, NULL, &space_size, dset->obj.item.file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of dataspace");
        if (NULL == (space_buf = DV_malloc(space_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for serialized dataspace");
        if (H5Sencode2(update_cb_ud->new_space_id, space_buf, &space_size, dset->obj.item.file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, FAIL, "can't serialize dataspace");

        /* Set up operation to write dataspace to dataset */

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        update_cb_ud->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[0].iod_name, H5_daos_space_key_g,
                           H5_daos_space_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_size = (uint64_t)space_size;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        update_cb_ud->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[0], space_buf, (daos_size_t)space_size);
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[0];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[0]   = TRUE;

        update_cb_ud->md_rw_cb_ud.nr = 1u;

        /* Create task to write updated dataspace to dataset */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_md_rw_prep_cb, H5_daos_dset_set_extent_comp_cb, update_cb_ud,
                                     &update_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to update object");
    } /* end if */
    else {
        /* Don't need to write dataspace to file, but still need to create a
         * task to update dset->space_id at the right time */
        /* Create empty task (comp_cb will update dset->space_id) */
        if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, H5_daos_dset_set_extent_comp_cb,
                                update_cb_ud, &update_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to perform get operation");
    } /* end else */

    assert(update_task);

    /* Schedule object update task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to update object: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = update_task;
    req->rc++;
    dset->obj.item.rc++;

    /* Relinquish control of the object update udata and dataspace buffer to the
     * task's completion callback */
    update_cb_ud = NULL;
    space_buf    = NULL;

    *dep_task = update_task;

done:
    if (collective && (dset->obj.item.file->num_procs > 1))
        if (H5_daos_collective_error_check(&dset->obj, req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't perform collective error check");

    /* Cleanup on failure */
    if (ret_value < 0) {
        space_buf = DV_free(space_buf);

        if (update_cb_ud) {
            if (H5Sclose(update_cb_ud->new_space_id) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataspace");
            update_cb_ud = DV_free(update_cb_ud);
        } /* end if */
    }     /* end if */

    assert(!space_buf);
    assert(!update_cb_ud);

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_set_extent() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_point_and_block
 *
 * Purpose:     Intersects the point selection in point_space with the
 *              block specified with start and block, and returns a
 *              dataspace with the intersecting points selected.
 *
 * Return:      Success: Dataspace with the intersecting points selected
 *              Failure: H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5_daos_point_and_block(hid_t point_space, hsize_t rank, hsize_t *dims, hsize_t *start, hsize_t *block)
{
    hsize_t       points_in[H5_DAOS_POINT_BUF_LEN];
    hsize_t       points_out[H5_DAOS_POINT_BUF_LEN];
    const hsize_t npoints_buf = H5_DAOS_POINT_BUF_LEN / rank;
    hssize_t      npoints_in_left;
    hsize_t       npoints_in_done = 0;
    hsize_t       npoints_in_this_iter;
    hsize_t       npoints_out = 0;
    hsize_t       npoints_out_this_iter;
    hid_t         space_out = H5I_INVALID_HID;
    hsize_t       i, j;
    hid_t         ret_value = H5I_INVALID_HID;

    /* Get number of points */
    if ((npoints_in_left = H5Sget_select_elem_npoints(point_space)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "can't get number of points in selection");

    /* Create output space */
    if ((space_out = H5Screate_simple((int)rank, dims, NULL)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5I_INVALID_HID, "can't create output space");

    /* Loop until we've processed all points */
    while (npoints_in_left > 0) {
        /* Calculate number of points to fetch */
        npoints_in_this_iter =
            npoints_in_left > (hssize_t)npoints_buf ? npoints_buf : (hsize_t)npoints_in_left;

        /* Fetch points */
        if (H5Sget_select_elem_pointlist(point_space, npoints_in_done, npoints_in_this_iter, points_in) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "can't get points from selection");

        /* Add intersecting points to points_out */
        npoints_out_this_iter = 0;
        for (i = 0; i < npoints_in_this_iter; i++) {
            for (j = 0; (j < rank) && (points_in[(i * rank) + j] >= start[j]) &&
                        (points_in[(i * rank) + j] < start[j] + block[j]);
                 j++)
                ;
            if (j == rank) {
                memcpy(&points_out[npoints_out_this_iter * rank], &points_in[i * rank],
                       rank * sizeof(points_in[0]));
                npoints_out_this_iter++;
            } /* end if */
        }     /* end for */

        /* Select elements in points_out */
        if (npoints_out_this_iter &&
            H5Sselect_elements(space_out, npoints_out ? H5S_SELECT_APPEND : H5S_SELECT_SET,
                               (size_t)npoints_out_this_iter, points_out) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, H5I_INVALID_HID, "can't select points");

        /* Advance points */
        assert(npoints_in_left >= (hssize_t)npoints_in_this_iter);
        npoints_in_left -= (hssize_t)npoints_in_this_iter;
        npoints_in_done += npoints_in_this_iter;
        npoints_out += npoints_out_this_iter;
    } /* end while */

    /* Set return value */
    ret_value = space_out;

done:
    /* Cleanup on failure */
    if (ret_value < 0)
        if (space_out >= 0 && H5Sclose(space_out) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, H5I_INVALID_HID, "can't close dataspace");

    D_FUNC_LEAVE;
} /* end H5_daos_point_and_block() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_selected_chunk_info
 *
 * Purpose:     Calculates the starting coordinates for the chunks selected
 *              in the file space given by file_space_id and sets up
 *              individual memory and file spaces for each chunk. Selected
 *              chunk's coordinates and dataspaces are returned through the
 *              `chunk_info` struct pointer.
 *
 *              In order to support caching of all of the file and memory
 *              dataspaces that get created for selected chunks, valid
 *              `chunk_info`/`chunk_info_len` pointers that have been
 *              allocated/set by this routine may be passed in. In this
 *              case, the existing dataspaces will be re-used for dataspace
 *              operations rather than creating new ones. Note that this
 *              assumes that the `chunk_info`/`chunk_info_len` pointers
 *              passed in always come from the same dataset object with the
 *              same chunk dimensionality.
 *
 *              NOTE: In several places in this routine, an adjustment is
 *                    calculated in order to move the selection within a
 *                    chunk around by using H5Sselect_adjust. Since this
 *                    API routine accepts an array of signed values, this
 *                    adjustment is calculated by converting unsigned
 *                    coordinates to signed coordinates without a check
 *                    for overflow. In the future, it would be nice if
 *                    HDF5 could support subtracting and adding of
 *                    offsets to selections with routines that accept
 *                    arrays of unsigned offsets so this can be done
 *                    safely.
 *
 *              XXX: Note that performance could be increased by
 *                   calculating all of the chunks in the entire dataset
 *                   and then caching them in the dataset object for
 *                   re-use in subsequent reads/writes
 *
 * Return:      Success: 0
 *              Failure: -1
 *
 * Programmer:  Neil Fortner
 *              May, 2018
 *              Based on H5VL_daosm_get_selected_chunk_info by Jordan
 *              Henderson, May, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_selected_chunk_info(H5_daos_dcpl_cache_t *dcpl_cache, hid_t file_space_id, hid_t mem_space_id,
                                H5_daos_select_chunk_info_t **chunk_info, size_t *chunk_info_len,
                                size_t *nchunks_selected)
{
    H5_daos_select_chunk_info_t *_chunk_info = NULL;
    H5S_sel_type                 file_space_type;
    hssize_t                     num_sel_points;
    hssize_t                     chunk_file_space_adjust[H5O_LAYOUT_NDIMS];
    hsize_t                      file_space_dims[H5S_MAX_RANK], mem_space_dims[H5S_MAX_RANK];
    hsize_t                     *chunk_dims;
    hsize_t                      curr_chunk_dims[H5S_MAX_RANK] = {0};
    hsize_t                      file_sel_start[H5S_MAX_RANK], file_sel_end[H5S_MAX_RANK];
    hsize_t                      mem_sel_start[H5S_MAX_RANK], mem_sel_end[H5S_MAX_RANK];
    hsize_t                      start_coords[H5O_LAYOUT_NDIMS], end_coords[H5O_LAYOUT_NDIMS];
    hsize_t                      selection_start_coords[H5O_LAYOUT_NDIMS] = {0};
    hbool_t                      is_partial_edge_chunk                    = FALSE;
    hbool_t                      file_mem_space_same                      = (file_space_id == mem_space_id);
    htri_t                       space_same_shape                         = FALSE;
    size_t                       chunk_info_nalloc                        = 0;
    ssize_t                      i                                        = -1, j;
    hid_t                        entire_chunk_sel_space_id                = H5I_INVALID_HID;
    int                          fspace_ndims, mspace_ndims;
    int                          increment_dim;
    herr_t                       ret_value = SUCCEED;

    assert(dcpl_cache);
    assert(chunk_info);
    assert(nchunks_selected);

    if ((num_sel_points = H5Sget_select_npoints(file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                     "can't get number of points selected in file dataspace");
    if (num_sel_points == 0)
        D_GOTO_DONE(SUCCEED);

    /* Allocate selected chunk info buffer or use already-allocated buffer */
    if (!*chunk_info) {
        if (NULL == (_chunk_info = (H5_daos_select_chunk_info_t *)DV_calloc(H5_DAOS_DEFAULT_NUM_SEL_CHUNKS *
                                                                            sizeof(*_chunk_info))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                         "can't allocate space for selected chunk info buffer");
        chunk_info_nalloc = H5_DAOS_DEFAULT_NUM_SEL_CHUNKS;

        /* Ensure that every chunk info structure's dataspaces are initialized */
        for (i = 0; i < (ssize_t)chunk_info_nalloc; i++)
            _chunk_info[i].fspace_id = _chunk_info[i].mspace_id = H5I_INVALID_HID;
    } /* end if */
    else {
        assert(chunk_info_len && (*chunk_info_len > 0));
        _chunk_info       = *chunk_info;
        chunk_info_nalloc = *chunk_info_len;
    } /* end else */

    /* Get dataspace ranks */
    if ((fspace_ndims = H5Sget_simple_extent_ndims(file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file space dimensionality");
    if (file_mem_space_same)
        mspace_ndims = fspace_ndims;
    else if ((mspace_ndims = H5Sget_simple_extent_ndims(mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get memory space dimensionality");

    /* Get dataspace dimensionality */
    if (H5Sget_simple_extent_dims(file_space_id, file_space_dims, NULL) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file dataspace dimensions");
    if (file_mem_space_same)
        memcpy(mem_space_dims, file_space_dims, (size_t)fspace_ndims * sizeof(hsize_t));
    else if (H5Sget_simple_extent_dims(mem_space_id, mem_space_dims, NULL) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get memory dataspace dimensions");

    /* Get the bounding box for the current selection in the file and memory spaces */
    if (H5Sget_select_bounds(file_space_id, file_sel_start, file_sel_end) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get bounding box for file selection");
    if (file_mem_space_same) {
        memcpy(mem_sel_start, file_sel_start, (size_t)fspace_ndims * sizeof(hsize_t));
        memcpy(mem_sel_end, file_sel_end, (size_t)fspace_ndims * sizeof(hsize_t));
    } /* end if */
    else if (H5Sget_select_bounds(mem_space_id, mem_sel_start, mem_sel_end) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get bounding box for memory selection");

    /* Set convenience pointer to chunk dimensions */
    chunk_dims = dcpl_cache->chunk_dims;

    /* Initialize the curr_chunk_dims array */
    memcpy(curr_chunk_dims, chunk_dims, (size_t)fspace_ndims * sizeof(hsize_t));

    /* Calculate the coordinates for the initial chunk */
    for (i = 0; i < (ssize_t)fspace_ndims; i++) {
        start_coords[i] = selection_start_coords[i] = (file_sel_start[i] / chunk_dims[i]) * chunk_dims[i];
        end_coords[i]                               = (start_coords[i] + chunk_dims[i]) - 1;
    } /* end for */

    /* Check if the spaces are the same "shape".  For now, reject spaces that
     * have different ranks, until there's a public interface to
     * H5S_select_construct_projection().  See the note in H5D__read().  With
     * the use of H5Sselect_project_intersection() the performance penalty
     * should be much less than with the native library anyways. */
    if (fspace_ndims == mspace_ndims) {
        if (file_mem_space_same)
            space_same_shape = TRUE;
        else if (FAIL == (space_same_shape = H5Sselect_shape_same(file_space_id, mem_space_id)))
            D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                         "can't determine if file and memory dataspaces are the same shape");
    } /* end if */

    if (space_same_shape) {
        /* Calculate the adjustment for the memory selection from the file selection */
        for (i = 0; i < (ssize_t)fspace_ndims; i++) {
            /* H5_CHECK_OVERFLOW(file_sel_start[i], hsize_t, hssize_t); */
            /* H5_CHECK_OVERFLOW(mem_sel_start[i], hsize_t, hssize_t); */
            chunk_file_space_adjust[i] = (hssize_t)file_sel_start[i] - (hssize_t)mem_sel_start[i];
        } /* end for */
    }     /* end if */
    else {
        /* Create temporary dataspace to hold selection of entire chunk */
        if ((entire_chunk_sel_space_id = H5Screate_simple(fspace_ndims, file_space_dims, NULL)) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't create entire chunk selection dataspace");
    } /* end else */

    /* Get file selection type */
    if ((file_space_type = H5Sget_select_type(file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection type");

    /* Iterate through each "chunk" in the dataset */
    for (i = -1;;) {
        htri_t intersect = FALSE;

        /* Check for intersection of file selection and "chunk". If there is
         * an intersection, set up a valid memory and file space for the chunk. */
        if (file_space_type == H5S_SEL_ALL)
            intersect = TRUE;
        else if ((intersect = H5Sselect_intersect_block(file_space_id, start_coords, end_coords)) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                         "cannot determine chunk's intersection with the file dataspace");
        if (TRUE == intersect) {
            hssize_t chunk_space_adjust[H5O_LAYOUT_NDIMS];

            /* Advance index and re-allocate selected chunk info buffer if necessary */
            if (++i == (ssize_t)chunk_info_nalloc) {
                void *tmp_realloc;

                if (NULL ==
                    (tmp_realloc = DV_realloc(_chunk_info, 2 * chunk_info_nalloc * sizeof(*_chunk_info))))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                 "can't reallocate space for selected chunk info buffer");
                _chunk_info = (H5_daos_select_chunk_info_t *)tmp_realloc;

                /* Ensure newly-allocated chunk info structures are initialized */
                memset(&_chunk_info[chunk_info_nalloc], 0, chunk_info_nalloc * sizeof(*_chunk_info));
                for (j = (ssize_t)chunk_info_nalloc; j < (ssize_t)chunk_info_nalloc * 2; j++)
                    _chunk_info[j].fspace_id = _chunk_info[j].mspace_id = H5I_INVALID_HID;

                chunk_info_nalloc *= 2;
            } /* end while */

            assert(i < (ssize_t)chunk_info_nalloc);

            /*
             * Set up the file Dataspace for this chunk.
             */

            /* If this is a partial edge chunk, setup the partial edge chunk dimensions.
             * These will be used to adjust the selection within the edge chunk so that
             * it falls within the dataset's dataspace boundaries. Also setup a selection
             * adjustment that can be used to move the selection within the chunk back to
             * the correct offset within the chunk, if necessary (for point and hyperslab
             * selections).
             */
            for (j = 0; j < (ssize_t)fspace_ndims; j++) {
                if (start_coords[j] + chunk_dims[j] > file_space_dims[j]) {
                    curr_chunk_dims[j]    = file_space_dims[j] - start_coords[j];
                    is_partial_edge_chunk = TRUE;
                } /* end if */
                chunk_space_adjust[j] = (hssize_t)start_coords[j];
            } /* end for */

            switch (file_space_type) {
                case H5S_SEL_POINTS:
                    /* Close file space if cached */
                    if (_chunk_info[i].fspace_id >= 0) {
                        if (H5Sclose(_chunk_info[i].fspace_id) < 0)
                            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL,
                                         "can't close chunk file dataspace");
                        _chunk_info[i].fspace_id = H5I_INVALID_HID;
                    } /* end if */

                    /* Intersect points with block using connector routine */
                    if ((_chunk_info[i].fspace_id =
                             H5_daos_point_and_block(file_space_id, (hsize_t)fspace_ndims, chunk_dims,
                                                     start_coords, curr_chunk_dims)) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't intersect point selection");

                    /* Move selection back to have correct offset in chunk */
                    if (H5Sselect_adjust(_chunk_info[i].fspace_id, chunk_space_adjust) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk selection");

                    break;

                case H5S_SEL_HYPERSLABS: {
                    /* Create chunk file dataspace if one isn't cached */
                    if (_chunk_info[i].fspace_id < 0)
                        if ((_chunk_info[i].fspace_id = H5Screate_simple(fspace_ndims, chunk_dims, NULL)) < 0)
                            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                         "can't create temporary chunk selection");

                    /* Select all elements within this chunk's file dataspace as
                     * a hyperslab (accounting for partial edge chunks) */
                    if (H5Sselect_hyperslab(_chunk_info[i].fspace_id, H5S_SELECT_SET, start_coords, NULL,
                                            curr_chunk_dims, NULL) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                     "can't set selection in chunk file dataspace");

                    /* Refine chunk file dataspace selection with only elements
                     * that are also selected in whole file dataspace */
                    if (H5Smodify_select(_chunk_info[i].fspace_id, H5S_SELECT_AND, file_space_id) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                     "can't refine chunk file dataspace selection");

                    /* Move selection back to have correct offset in chunk */
                    if (H5Sselect_adjust(_chunk_info[i].fspace_id, chunk_space_adjust) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk selection");

                    break;
                } /* case H5S_SEL_HYPERSLABS */

                case H5S_SEL_ALL: {
                    hsize_t zero_offset_start[H5S_MAX_RANK] = {0};

                    if (_chunk_info[i].fspace_id < 0) {
                        /* Create chunk dataspace with full chunk dimensions */
                        if ((_chunk_info[i].fspace_id = H5Screate_simple(fspace_ndims, chunk_dims, NULL)) < 0)
                            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                         "can't create temporary chunk selection");

                        /* Trim selection down to partial edge chunk size if necessary */
                        if (is_partial_edge_chunk &&
                            H5Sselect_hyperslab(_chunk_info[i].fspace_id, H5S_SELECT_SET, zero_offset_start,
                                                NULL, curr_chunk_dims, NULL) < 0)
                            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                         "can't select partial edge chunk from temporary selection");
                    } /* end if */
                    else {
                        /* Reset selection for cached chunk file dataspace to current chunk dimensions */
                        if (H5Sselect_hyperslab(_chunk_info[i].fspace_id, H5S_SELECT_SET, zero_offset_start,
                                                NULL, curr_chunk_dims, NULL) < 0)
                            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                         "can't set chunk file dataspace selection");
                    } /* end else */

                    break;
                } /* case H5S_SEL_ALL */

                case H5S_SEL_NONE:
                case H5S_SEL_ERROR:
                case H5S_SEL_N:
                default:
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "invalid dataspace selection type");
            } /* end switch */

            /* Copy the chunk's coordinates to the selected chunk info buffer */
            memcpy(_chunk_info[i].chunk_coords, start_coords, (size_t)fspace_ndims * sizeof(hsize_t));

            /*
             * Now set up the memory Dataspace for this chunk.
             */
            if (space_same_shape) {
                if (_chunk_info[i].mspace_id < 0) {
                    /* Create new memory dataspace */
                    if ((_chunk_info[i].mspace_id = H5Screate_simple(mspace_ndims, mem_space_dims, NULL)) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL,
                                     "can't create chunk memory dataspace");
                } /* end if */
                else {
                    htri_t extents_equal;

                    /* Check if the cached chunk's memory dataspace extent is the same
                     * as the current memory dataspace's extent; resize the cached
                     * memory dataspace if not.
                     */
                    if ((extents_equal = H5Sextent_equal(_chunk_info[i].mspace_id, mem_space_id)) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                                     "can't check if memory dataspaces have the same extent");

                    if (!extents_equal && H5Sset_extent_simple(_chunk_info[i].mspace_id, mspace_ndims,
                                                               mem_space_dims, NULL) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                     "can't adjust chunk memory dataspace dimensions");
                } /* end else */

                if (H5S_SEL_ALL == file_space_type) {
                    /* Set selection to same shape as chunk's file dataspace selection */
                    if (H5Sselect_hyperslab(_chunk_info[i].mspace_id, H5S_SELECT_SET, start_coords, NULL,
                                            curr_chunk_dims, NULL) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL,
                                     "can't create chunk memory selection");
                } /* end if */
                else {
                    /* Copy the chunk's file space selection to its memory space selection */
                    if (H5Sselect_copy(_chunk_info[i].mspace_id, _chunk_info[i].fspace_id) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL,
                                     "unable to copy selection from temporary chunk's file dataspace to its "
                                     "memory dataspace");

                    /* Compute the adjustment for the chunk */
                    for (j = 0; j < (ssize_t)fspace_ndims; j++) {
                        /* H5_CHECK_OVERFLOW(_chunk_info[i].chunk_coords[j], hsize_t, hssize_t); */
                        chunk_space_adjust[j] =
                            chunk_file_space_adjust[j] - (hssize_t)_chunk_info[i].chunk_coords[j];
                    } /* end for */

                    /* Adjust the selection */
                    if (H5Sselect_adjust(_chunk_info[i].mspace_id, chunk_space_adjust) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                     "can't adjust temporary chunk's memory space selection");
                } /* end else */
            }     /* end if */
            else {
                /* Select this chunk in the temporary chunk selection dataspace.
                 * Shouldn't matter if it goes beyond the extent since we're not
                 * doing I/O with this space */
                if (H5Sselect_hyperslab(entire_chunk_sel_space_id, H5S_SELECT_SET, start_coords, NULL,
                                        chunk_dims, NULL) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't select entire chunk");

                /* Close memory space if cached */
                if (_chunk_info[i].mspace_id >= 0) {
                    if (H5Sclose(_chunk_info[i].mspace_id) < 0)
                        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL,
                                     "can't close chunk memory dataspace");
                    _chunk_info[i].mspace_id = H5I_INVALID_HID;
                } /* end if */

                /* Calculate memory selection for this chunk by projecting
                 * intersection of full file selection and file chunk to full
                 * memory selection */
                if ((_chunk_info[i].mspace_id = H5Sselect_project_intersection(
                         file_space_id, mem_space_id, entire_chunk_sel_space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't project intersection");
            } /* end else */

            /* Determine if there are more chunks to process */
            if ((_chunk_info[i].num_elem_sel_file = H5Sget_select_npoints(_chunk_info[i].fspace_id)) < 0)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                             "can't get number of points selected in chunk file space");

            /* Make sure we didn't process too many points */
            if (_chunk_info[i].num_elem_sel_file > num_sel_points)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                             "processed more elements than present in selection");

            /* Keep track of the number of elements processed */
            num_sel_points -= _chunk_info[i].num_elem_sel_file;

            /* Break out if we're done */
            if (num_sel_points == 0)
                break;

            /* Clean up after partial edge chunk */
            if (is_partial_edge_chunk) {
                memcpy(curr_chunk_dims, chunk_dims, (size_t)fspace_ndims * sizeof(hsize_t));
                is_partial_edge_chunk = FALSE;
            } /* end if */
        }     /* end if */

        /* Set current increment dimension */
        increment_dim = fspace_ndims - 1;

        /* Increment chunk location in fastest changing dimension */
        start_coords[increment_dim] += chunk_dims[increment_dim];
        end_coords[increment_dim] += chunk_dims[increment_dim];

        /* Bring chunk location back into bounds, if necessary */
        if (start_coords[increment_dim] > file_sel_end[increment_dim]) {
            do {
                /* Reset current dimension's location to 0 */
                start_coords[increment_dim] = selection_start_coords[increment_dim];
                end_coords[increment_dim]   = (start_coords[increment_dim] + chunk_dims[increment_dim]) - 1;

                /* Decrement current dimension */
                if (increment_dim == 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                                 "did not find enough elements to process or error traversing chunks");
                increment_dim--;

                /* Increment chunk location in current dimension */
                start_coords[increment_dim] += chunk_dims[increment_dim];
                end_coords[increment_dim] = (start_coords[increment_dim] + chunk_dims[increment_dim]) - 1;
            } while (start_coords[increment_dim] > file_sel_end[increment_dim]);
        } /* end if */
    }     /* end for */

done:
    if (ret_value < 0 && _chunk_info) {
        for (j = 0; j <= i; j++) {
            if ((_chunk_info[j].fspace_id >= 0) && (H5Sclose(_chunk_info[j].fspace_id) < 0))
                D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL,
                             "failed to close chunk file dataspace ID");
            if ((_chunk_info[j].mspace_id >= 0) && (H5Sclose(_chunk_info[j].mspace_id) < 0))
                D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL,
                             "failed to close chunk memory dataspace ID");
        }

        DV_free(_chunk_info);
        if (*chunk_info)
            *chunk_info = NULL;
        if (chunk_info_len)
            *chunk_info_len = 0;
    }
    else {
        *chunk_info = _chunk_info;
        if (chunk_info_len)
            *chunk_info_len = chunk_info_nalloc;

        assert(i + 1 >= 0);
        *nchunks_selected = (size_t)(i + 1);
    }

    if ((entire_chunk_sel_space_id >= 0) && (H5Sclose(entire_chunk_sel_space_id) < 0))
        D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL,
                     "failed to close temporary entire chunk dataspace");

    D_FUNC_LEAVE;
} /* end H5_daos_get_selected_chunk_info() */
