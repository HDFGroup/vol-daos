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
 * library. Dataset routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/****************/
/* Local Macros */
/****************/

/* Definitions for chunking code */
#define H5_DAOS_DEFAULT_NUM_SEL_CHUNKS   64
#define H5O_LAYOUT_NDIMS                 (H5S_MAX_RANK+1)

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Udata type for H5Dscatter callback */
typedef struct H5_daos_scatter_cb_ud_t {
    void *buf;
    size_t len;
} H5_daos_scatter_cb_ud_t;

/* Udata type for memory space H5Diterate callback */
typedef struct {
    daos_iod_t *iods;
    daos_sg_list_t *sgls;
    daos_iov_t *sg_iovs;
    hbool_t is_vl_str;
    size_t base_type_size;
    uint64_t offset;
    uint64_t idx;
} H5_daos_vl_mem_ud_t;

/* Udata type for file space H5Diterate callback */
typedef struct {
    uint8_t **akeys;
    daos_iod_t *iods;
    uint64_t idx;
} H5_daos_vl_file_ud_t;

/* Information about a singular selected chunk during a dataset read/write */
typedef struct H5_daos_select_chunk_info_t {
    uint64_t chunk_coords[H5S_MAX_RANK]; /* The starting coordinates ("upper left corner") of the chunk */
    hid_t    mspace_id;                  /* The memory space corresponding to the
                                            selection in the chunk in memory */
    hid_t    fspace_id;                  /* The file space corresponding to the
                                            selection in the chunk in the file */
} H5_daos_select_chunk_info_t;

/* Enum type for distinguishing between dataset reads and writes. */
typedef enum dset_io_type {
    IO_READ,
    IO_WRITE
} dset_io_type;

/* Typedef for function to perform I/O on a single chunk */
typedef herr_t (*H5_daos_chunk_io_func)(H5_daos_dset_t *dset, daos_key_t *dkey,
    hssize_t num_elem, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
    hid_t dxpl_id, dset_io_type io_type, void *buf, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);

/* Task user data for raw data I/O */
typedef struct H5_daos_chunk_io_ud_t {
    H5_daos_req_t *req;
    H5_daos_dset_t *dset;
    daos_key_t dkey;
    uint8_t akey_buf;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_recx_t recx;
    daos_recx_t *recxs;
    daos_iov_t sg_iov;
    daos_iov_t *sg_iovs;
} H5_daos_chunk_io_ud_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_dset_fill_dcpl_cache(H5_daos_dset_t *dset);
static int H5_daos_fill_val_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_bcast_fill_val(H5_daos_dset_t *dset, H5_daos_req_t *req,
    size_t fill_val_size, tse_task_t **taskp, tse_task_t *dep_task);
static int H5_daos_dset_open_end(H5_daos_dset_t *dset, uint8_t *p,
    uint64_t type_buf_len, uint64_t space_buf_len, uint64_t dcpl_buf_len,
    uint64_t fill_val_len, hid_t dxpl_id);
static int H5_daos_dset_open_bcast_comp_cb(tse_task_t *task, void *args);
static int H5_daos_dset_open_recv_comp_cb(tse_task_t *task, void *args);
static int H5_daos_dinfo_read_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_sel_to_recx_iov(hid_t space_id, size_t type_size,
    void *buf, daos_recx_t **recxs, daos_iov_t **sg_iovs, size_t *list_nused);
static herr_t H5_daos_scatter_cb(const void **src_buf,
    size_t *src_buf_bytes_used, void *_udata);
static int H5_daos_chunk_io_prep_cb(tse_task_t *task, void *args);
static int H5_daos_chunk_io_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_dataset_io_types_equal(H5_daos_dset_t *dset, daos_key_t *dkey,
    hssize_t num_elem, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
    hid_t dxpl_id, dset_io_type io_type, void *buf, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_dataset_io_types_unequal(H5_daos_dset_t *dset, daos_key_t *dkey,
    hssize_t num_elem, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
    hid_t dxpl_id, dset_io_type io_type, void *buf, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_dataset_set_extent(H5_daos_dset_t *dset,
    const hsize_t *size, hid_t dxpl_id, void **req);
static hid_t H5_daos_point_and_block(hid_t point_space, hsize_t rank,
    hsize_t *dims, hsize_t *start, hsize_t *block);
static herr_t H5_daos_get_selected_chunk_info(H5_daos_dcpl_cache_t *dcpl_cache,
    hid_t file_space_id, hid_t mem_space_id,
    H5_daos_select_chunk_info_t **chunk_info, size_t *chunk_info_len);


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
    htri_t is_vl_ref;
    herr_t ret_value = SUCCEED;

    assert(dset);

    /* Retrieve layout */
    if((dset->dcpl_cache.layout = H5Pget_layout(dset->dcpl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get layout property")

    /* Retrieve chunk dimensions */
    if(dset->dcpl_cache.layout == H5D_CHUNKED)
        if(H5Pget_chunk(dset->dcpl_id, H5S_MAX_RANK, dset->dcpl_cache.chunk_dims) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk dimensions")

    /* Retrieve fill status */
    if(H5Pfill_value_defined(dset->dcpl_id, &dset->dcpl_cache.fill_status) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get fill value status")

    /* Check for vlen or reference */
    if((is_vl_ref = H5_daos_detect_vl_vlstr_ref(dset->type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check for vl or reference type")

    /* Retrieve fill time */
    if(H5Pget_fill_time(dset->dcpl_id, &fill_time) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get fill time")

    /* Determine fill method */
    if(fill_time == H5D_FILL_TIME_NEVER) {
        /* Check for fill time never with vl/ref (illegal) */
        if(is_vl_ref)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't use fill time of NEVER with vlen or reference type")

        /* Never write fill values even if defined */
        dset->dcpl_cache.fill_method = H5_DAOS_NO_FILL;
    } /* end if */
    else if(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_UNDEFINED) {
        /* If the fill value is undefined, must still write zeros for vl/ref,
         * otherwise write nothing */
        if(is_vl_ref)
            dset->dcpl_cache.fill_method = H5_DAOS_ZERO_FILL;
        else
            dset->dcpl_cache.fill_method = H5_DAOS_NO_FILL;
    } /* end if */
    else if(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_DEFAULT)
        /* Always fill with zeros */
        dset->dcpl_cache.fill_method = H5_DAOS_ZERO_FILL;
    else {
        /* Always copy the fill value */
        assert(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED);
        dset->dcpl_cache.fill_method = H5_DAOS_COPY_FILL;
    } /* end else */

done:
    D_FUNC_LEAVE
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
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for group info broadcast task")

    assert(udata->req);
    assert(udata->obj);
    assert(udata->obj->item.file);
    assert(!udata->obj->item.file->closed);
    assert(udata->obj->item.type == H5I_DATASET);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast fill value";
    } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Close group */
        if(H5_daos_dataset_close((H5_daos_dset_t *)udata->obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close dataset")

        /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
        if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast fill value completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

        /* Do not free the buffer since it's owned by the dataset struct */

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == H5_DAOS_DAOS_GET_ERROR);

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
H5_daos_bcast_fill_val(H5_daos_dset_t *dset, H5_daos_req_t *req,
    size_t fill_val_size, tse_task_t **taskp, tse_task_t *dep_task)
{
    tse_task_t *bcast_task;
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(dset);
    assert(dset->fill_val);
    assert(req);
    assert(taskp);
    assert(dep_task);

    /* Set up broadcast user data */
    if(NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for MPI broadcast user data")
    bcast_udata->req = req;
    bcast_udata->obj = &dset->obj;
    bcast_udata->bcast_metatask = NULL;
    bcast_udata->buffer = dset->fill_val;
    bcast_udata->buffer_len = fill_val_size;
    bcast_udata->count = fill_val_size;

    /*/* Create task for fill value bcast */
    if(0 != (ret = tse_task_create(H5_daos_mpi_ibcast_task, &dset->obj.item.file->sched, bcast_udata, &bcast_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to broadcast fill value: %s", H5_daos_err_to_string(ret))

    /* Register task dependency */
    if(0 != (ret = tse_task_register_deps(bcast_task, 1, &dep_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependencies for fill value broadcast task: %s", H5_daos_err_to_string(ret))

    /* Set callback functions for fill value bcast */
    if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_fill_val_bcast_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't register callbacks for fill value broadcast: %s", H5_daos_err_to_string(ret))

    /* Schedule bcast task and give it a reference to req and dset */
    if(0 != (ret = tse_task_schedule(bcast_task, false)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task for fill value broadcast: %s", H5_daos_err_to_string(ret))
    req->rc++;
    dset->obj.item.rc++;
    bcast_udata = NULL;

    /* Return bcast_task */
    *taskp = bcast_task;

done:
    bcast_udata = DV_free(bcast_udata);

    D_FUNC_LEAVE
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
H5_daos_dataset_create(void *_item,
    const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
    hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
    hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_dset_t *dset = NULL;
    H5_daos_group_t *target_grp = NULL;
    H5_daos_md_rw_cb_ud_t *update_cb_ud = NULL;
    void *type_buf = NULL;
    void *space_buf = NULL;
    void *dcpl_buf = NULL;
    void *fill_val_buf = NULL;
    hid_t tmp_dcpl_id = H5I_INVALID_HID;
    hbool_t collective;
    size_t fill_val_size;
    htri_t is_vl_ref;
    tse_task_t *finalize_task;
    int finalize_ndeps = 0;
    tse_task_t *finalize_deps[3];
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *open_task = NULL;
    int ret;
    void *ret_value = NULL;

    /* Make sure H5_DAOS_g is set.  Eventually move this to a FUNC_ENTER_API
     * type macro? */
    H5_DAOS_G_INIT(NULL)

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")

    /*
     * Like HDF5, all metadata writes are collective by default. Once independent
     * metadata writes are implemented, we will need to check for this property.
     */
    collective = TRUE;

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(item->file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = H5FL_CALLOC(H5_daos_dset_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.open_req = int_req;
    int_req->rc++;
    dset->obj.item.file = item->file;
    dset->obj.item.rc = 1;
    dset->obj.obj_oh = DAOS_HDL_INVAL;
    dset->type_id = FAIL;
    dset->file_type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    dset->dapl_id = FAIL;

    /* Set up datatypes, dataspace, property list fields.  Do this earlier
     * because we need some of these things */
    if((dset->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((dset->file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to get file datatype")
    if((dset->space_id = H5Scopy(space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dataspace")
    if(H5Sselect_all(dset->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")
    if((dset->dcpl_id = H5Pcopy(dcpl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dcpl")
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dapl")

    /* Fill DCPL cache */
    if(H5_daos_dset_fill_dcpl_cache(dset) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to fill DCPL cache")

    /* Generate dataset oid */
    if(H5_daos_oid_generate(&dset->obj.oid, H5I_DATASET, dcpl_id == H5P_DATASET_CREATE_DEFAULT ? H5P_DEFAULT : dcpl_id, item->file, collective) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't generate object id")

    /* Open dataset object */
    if(H5_daos_obj_open(item->file, int_req, &dset->obj.oid, DAOS_OO_RW, &dset->obj.obj_oh, "dataset object open", &first_task, &open_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset object")

    /* Create dataset and write metadata if this process should */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        size_t type_size = 0;
        size_t space_size = 0;
        size_t dcpl_size = 0;
        tse_task_t *update_task;
        tse_task_t *link_write_task;

        /* Traverse the path */
        if(name) {
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, lcpl_id, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path")

            /* Reject invalid object names during object creation */
            if(!strncmp(target_name, ".", 2))
                D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, NULL, "invalid dataset name - '.'")
        } /* end if */

        /* Create dataset */
        /* Allocate argument struct */
        if(NULL == (update_cb_ud = (H5_daos_md_rw_cb_ud_t *)DV_calloc(sizeof(H5_daos_md_rw_cb_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for update callback arguments")

        /* Encode datatype */
        if(H5Tencode(type_id, NULL, &type_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
        if(NULL == (type_buf = DV_malloc(type_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
        if(H5Tencode(type_id, type_buf, &type_size) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize datatype")

        /* Encode dataspace */
        if(H5Sencode2(space_id, NULL, &space_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataspace")
        if(NULL == (space_buf = DV_malloc(space_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataspace")
        if(H5Sencode2(space_id, space_buf, &space_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dataspace")

        /* If there's a vl or reference type fill value set we must copy the
         * DCPL and unset the fill value.  This is a workaround for the bug that
         * prevents deep copying/flattening of encoded fill values.  Even though
         * the encoded value is never used by the connector, when it is replaced
         * on file open with one converted from the explicitly stored fill,
         * the library tries to free the fill value stored in the property list,
         * causing memory errors. */
        if(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED) {
            if((is_vl_ref = H5_daos_detect_vl_vlstr_ref(type_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't check for vl or reference type")
            if(is_vl_ref) {
                if((tmp_dcpl_id = H5Pcopy(dcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dcpl")
                if(H5Pset_fill_value(tmp_dcpl_id, dset->type_id, NULL) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't unset fill value")
            } /* end if */
        } /* end if */

        /* Encode DCPL */
        if(H5Pencode2(tmp_dcpl_id >= 0 ? tmp_dcpl_id : dcpl_id, NULL, &dcpl_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dcpl")
        if(NULL == (dcpl_buf = DV_malloc(dcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dcpl")
        if(H5Pencode2(tmp_dcpl_id >= 0 ? tmp_dcpl_id : dcpl_id, dcpl_buf, &dcpl_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dcpl")

        /* Set up operation to write datatype, dataspace, and DCPL to dataset */
        /* Point to dset */
        update_cb_ud->obj = &dset->obj;

        /* Point to req */
        update_cb_ud->req = int_req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        update_cb_ud->free_dkey = FALSE;

        /* Set up iod */
        daos_iov_set(&update_cb_ud->iod[0].iod_name, (void *)H5_daos_type_key_g, H5_daos_type_key_size_g);
        update_cb_ud->iod[0].iod_nr = 1u;
        update_cb_ud->iod[0].iod_size = (uint64_t)type_size;
        update_cb_ud->iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&update_cb_ud->iod[1].iod_name, (void *)H5_daos_space_key_g, H5_daos_space_key_size_g);
        update_cb_ud->iod[1].iod_nr = 1u;
        update_cb_ud->iod[1].iod_size = (uint64_t)space_size;
        update_cb_ud->iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&update_cb_ud->iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        update_cb_ud->iod[2].iod_nr = 1u;
        update_cb_ud->iod[2].iod_size = (uint64_t)dcpl_size;
        update_cb_ud->iod[2].iod_type = DAOS_IOD_SINGLE;

        update_cb_ud->free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->sg_iov[0], type_buf, (daos_size_t)type_size);
        update_cb_ud->sgl[0].sg_nr = 1;
        update_cb_ud->sgl[0].sg_nr_out = 0;
        update_cb_ud->sgl[0].sg_iovs = &update_cb_ud->sg_iov[0];
        daos_iov_set(&update_cb_ud->sg_iov[1], space_buf, (daos_size_t)space_size);
        update_cb_ud->sgl[1].sg_nr = 1;
        update_cb_ud->sgl[1].sg_nr_out = 0;
        update_cb_ud->sgl[1].sg_iovs = &update_cb_ud->sg_iov[1];
        daos_iov_set(&update_cb_ud->sg_iov[2], dcpl_buf, (daos_size_t)dcpl_size);
        update_cb_ud->sgl[2].sg_nr = 1;
        update_cb_ud->sgl[2].sg_nr_out = 0;
        update_cb_ud->sgl[2].sg_iovs = &update_cb_ud->sg_iov[2];

        /* Set nr */
        update_cb_ud->nr = 3u;

        /* Encode fill value if necessary.  Note that H5Pget_fill_value()
         * triggers type conversion and therefore writing any VL blobs to the
         * file. */
        /* Could potentially skip this and use value encoded in dcpl for non-vl/
         * ref types, or for all types once H5Pencode/decode works properly with
         * vl/ref fill values.  Latter would require a different code path for
         * filling in read values after conversion instead of before.  -NAF */
        if(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED) {
            if(0 == (fill_val_size = H5Tget_size(dset->file_type_id)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get file type size")
            if(NULL == (dset->fill_val = DV_calloc(fill_val_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fill value")
            if(H5Pget_fill_value(dcpl_id, dset->file_type_id, dset->fill_val) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get fill value")

            /* Copy fill value buffer - only needed because the generic
             * daos_obj_update frees the buffers.  We could avoid this with
             * extra work but this location isn't critical for performance. */
            if(NULL == (fill_val_buf = DV_malloc(fill_val_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fill value")
            (void)memcpy(fill_val_buf, dset->fill_val, fill_val_size);

            /* Set up iod */
            daos_iov_set(&update_cb_ud->iod[update_cb_ud->nr].iod_name, (void *)H5_daos_fillval_key_g, H5_daos_fillval_key_size_g);
            update_cb_ud->iod[update_cb_ud->nr].iod_nr = 1u;
            update_cb_ud->iod[update_cb_ud->nr].iod_size = (uint64_t)fill_val_size;
            update_cb_ud->iod[update_cb_ud->nr].iod_type = DAOS_IOD_SINGLE;

            /* Set up sgl */
            daos_iov_set(&update_cb_ud->sg_iov[update_cb_ud->nr], fill_val_buf, (daos_size_t)fill_val_size);
            update_cb_ud->sgl[update_cb_ud->nr].sg_nr = 1;
            update_cb_ud->sgl[update_cb_ud->nr].sg_nr_out = 0;
            update_cb_ud->sgl[update_cb_ud->nr].sg_iovs = &update_cb_ud->sg_iov[update_cb_ud->nr];

            /* Adjust nr */
            update_cb_ud->nr++;

            /* Broadcast fill value if it contains any vl or reference types and
             * there are other processes that need it.  Needed for vl and
             * reference types because calling H5Pget_fill_value on each process
             * would write a separate vl sequence on each process. */
            if(is_vl_ref && collective && (item->file->num_procs > 1)) {
                if(H5_daos_bcast_fill_val(dset, int_req, fill_val_size, &finalize_deps[finalize_ndeps], open_task) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't broadcast fill value")
                finalize_ndeps++;
            } /* end if */
        } /* end if */

        /* Set task name */
        update_cb_ud->task_name = "dataset metadata write";

        /* Create task for dataset metadata write */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_UPDATE, &item->file->sched, 0, NULL, &update_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to write dataset medadata: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        if(0 != (ret = tse_task_register_deps(update_task, 1, &open_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dependencies for dataset metadata write: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for dataset metadata write */
        if(0 != (ret = tse_task_register_cbs(update_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_md_update_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't register callbacks for task to write dataset medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for dataset metadata write */
        (void)tse_task_set_priv(update_task, update_cb_ud);

        /* Schedule dataset metadata write task and give it a reference to req
         * and the dataset */
        if(0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to write group metadata: %s", H5_daos_err_to_string(ret))
        int_req->rc++;
        dset->obj.item.rc++;
        update_cb_ud = NULL;
        type_buf = NULL;
        space_buf = NULL;
        dcpl_buf = NULL;
        fill_val_buf = NULL;

        /* Add dependency for finalize task */
        finalize_deps[finalize_ndeps] = update_task;
        finalize_ndeps++;

        /* Create link to dataset */
        if(target_grp) {
            H5_daos_link_val_t link_val;

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = dset->obj.oid;
            if(H5_daos_link_write(target_grp, target_name, strlen(target_name), &link_val, int_req, &link_write_task, open_task) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create link to dataset")
            finalize_deps[finalize_ndeps] = link_write_task;
            finalize_ndeps++;
        } /* end if */
    } /* end if */
    else {
        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast though it
         * could only be an issue with dataset reopen so we'll skip it for now.
         * There is probably never an issue with file reopen since all commits
         * are from process 0, same as the dataset create above. */

        /* Handle fill value */
        if(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED) {
            if(0 == (fill_val_size = H5Tget_size(dset->file_type_id)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get file type size")
            if(NULL == (dset->fill_val = DV_malloc(fill_val_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fill value")

            /* If there's a vl or reference type, receive fill value from lead
             * process above (see note above), otherwise just retrieve from DCPL
             */
            if((is_vl_ref = H5_daos_detect_vl_vlstr_ref(type_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't check for vl or reference type")
            if(is_vl_ref) {
                if(H5_daos_bcast_fill_val(dset, int_req, fill_val_size, &finalize_deps[finalize_ndeps], open_task) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTRECV, NULL, "can't broadcast fill value")
                finalize_ndeps++;
            } /* end if */
            else
                if(H5Pget_fill_value(dcpl_id, dset->file_type_id, dset->fill_val) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get fill value")
        } /* end if */

        /* Check for only open_task created, register it as the finalize
         * dependency if so */
        if(open_task && finalize_ndeps == 0) {
            finalize_deps[0] = open_task;
            finalize_ndeps = 1;
        } /* end if */
    } /* end else */

    /* Fill OCPL cache */
    if(H5_daos_fill_ocpl_cache(&dset->obj, dset->dcpl_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "failed to fill OCPL cache")

    /* Set return value */
    ret_value = (void *)dset;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close group")

    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependencies (if any) */
        else if(finalize_ndeps > 0 && 0 != (ret = tse_task_register_deps(finalize_task, finalize_ndeps, finalize_deps)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(item->file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, NULL, "dataset creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't free request")
    } /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value) {
        /* Close dataset */
        if(dset && H5_daos_dataset_close(dset, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset")

        /* Free memory */
        if(update_cb_ud && update_cb_ud->obj && H5_daos_object_close(update_cb_ud->obj, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close object")
        type_buf = DV_free(type_buf);
        space_buf = DV_free(space_buf);
        dcpl_buf = DV_free(dcpl_buf);
        fill_val_buf = DV_free(fill_val_buf);
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    /* Close temporary DCPL */
    if(tmp_dcpl_id >= 0 && H5Pclose(tmp_dcpl_id) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close temporary DCPL")

    assert(!update_cb_ud);
    assert(!type_buf);
    assert(!space_buf);
    assert(!dcpl_buf);
    assert(!fill_val_buf);

    D_FUNC_LEAVE_API
} /* end H5_daos_dataset_create() */


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
H5_daos_dset_open_end(H5_daos_dset_t *dset, uint8_t *p, uint64_t type_buf_len,
    uint64_t space_buf_len, uint64_t dcpl_buf_len, uint64_t fill_val_len,
    hid_t dxpl_id)
{
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    int ret_value = 0;

    assert(dset);
    assert(p);
    assert(type_buf_len > 0);

    /* Decode datatype */
    if((dset->type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, H5_DAOS_H5_DECODE_ERROR, "can't deserialize datatype")
     p += type_buf_len;

    /* Decode dataspace and select all */
    if((dset->space_id = H5Sdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, H5_DAOS_H5_DECODE_ERROR, "can't deserialize dataspace")
    if(H5Sselect_all(dset->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, H5_DAOS_H5_DECODE_ERROR, "can't change selection")
    p += space_buf_len;

    /* Decode DCPL */
    if((dset->dcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, H5_DAOS_H5_DECODE_ERROR, "can't deserialize DCPL")

    /* Finish setting up dataset struct */
    if((dset->file_type_id = H5VLget_file_type(dset->obj.item.file, H5_DAOS_g, dset->type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_H5_TCONV_ERROR, "failed to get file datatype")

    /* Fill DCPL cache */
    if(H5_daos_dset_fill_dcpl_cache(dset) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_CPL_CACHE_ERROR, "failed to fill DCPL cache")

    /* Check for fill value */
    if(fill_val_len > 0) {
        htri_t is_vl_ref;

        /* Copy fill value to dataset struct */
        p += dcpl_buf_len;
        if(NULL == (dset->fill_val = DV_malloc(fill_val_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "can't allocate buffer for fill value")
        (void)memcpy(dset->fill_val, p, fill_val_len);

        /* Set fill value in DCPL if it contains a VL or reference.  This is
         * necessary because the code in H5Pencode/decode for fill values does
         * not deep copy or flatten VL sequeneces, so the pointers stored in the
         * property list are invalid once decoded in a different context.  Note
         * this will cause every process to read the same VL sequence(s).  We
         * could remove this code once this feature is properly supported,
         * though once the library supports flattening VL we should consider
         * fundamentally changing how VL types work in this connector.  -NAF */
        if((is_vl_ref = H5_daos_detect_vl_vlstr_ref(dset->type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5_DAOS_H5_TCONV_ERROR, "can't check for vl or reference type")
        if(is_vl_ref) {
            size_t fill_val_size;
            size_t fill_val_mem_size;
            hbool_t fill_bkg;

            /* Initialize type conversion */
            if(H5_daos_tconv_init(dset->file_type_id, &fill_val_size,
                    dset->type_id, &fill_val_mem_size, 1, FALSE, FALSE,
                    &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_H5_TCONV_ERROR, "can't initialize type conversion")

            /* Sanity check */
            if(fill_val_size != fill_val_len)
                D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "size of stored fill value does not match size of datatype")

            /* Copy file type fill value to tconv_buf */
            (void)memcpy(tconv_buf, dset->fill_val, fill_val_size);

            /* Perform type conversion */
            if(H5Tconvert(dset->file_type_id, dset->type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, H5_DAOS_H5_TCONV_ERROR, "can't perform type conversion")

            /* Set fill value on DCPL */
            if(H5Pset_fill_value(dset->dcpl_id, dset->type_id, tconv_buf) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, H5_DAOS_H5PSET_ERROR, "can't set fill value")

            /* Patch dcpl_cache because the fill value was cleared in dataset
             * create due to a workaround for a different problem caused by the
             * same bug */
            dset->dcpl_cache.fill_status = H5D_FILL_VALUE_USER_DEFINED;
            dset->dcpl_cache.fill_method = H5_DAOS_COPY_FILL;
        } /* end if */
    } /* end if */
    else
        /* Check for missing fill value */
        if(dset->dcpl_cache.fill_status == H5D_FILL_VALUE_USER_DEFINED)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "fill value defined on property list but not found in metadata")

    /* Fill OCPL cache */
    if(H5_daos_fill_ocpl_cache(&dset->obj, dset->dcpl_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache")

done:
    /* Free tconv_buf */
    if(tconv_buf) {
        hid_t scalar_space_id;

        if((scalar_space_id = H5Screate(H5S_SCALAR)) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_H5_CREATE_ERROR, "can't create scalar dataspace")
        else {
            if(H5Treclaim(dset->type_id, scalar_space_id, dxpl_id, tconv_buf) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CANTGC, H5_DAOS_FREE_ERROR, "can't reclaim memory from fill value conversion buffer")
            if(H5Sclose(scalar_space_id) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close scalar dataspace")
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
    H5_daos_mpi_ibcast_ud_t *udata;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for group info broadcast task")

    assert(udata->req);
    assert(udata->obj);
    assert(udata->obj->item.file);
    assert(!udata->obj->item.file->closed);
    assert(udata->obj->item.file->my_rank == 0);
    assert(udata->obj->item.type == H5I_DATASET);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast dataset info";
    } /* end if */
    else
        /* Reissue bcast if necesary */
        if(udata->buffer_len != udata->count) {
            tse_task_t *bcast_task;

            assert(udata->count == H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                    + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                    + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE);
            assert(udata->buffer_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                    + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                    + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE);

            /* Use full buffer this time */
            udata->count = udata->buffer_len;

            /* Create task for second bcast */
            if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, &udata->obj->item.file->sched, udata, &bcast_task)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't create task for second dataset info broadcast")

            /* Set callback functions for second bcast */
            if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_dset_open_bcast_comp_cb, NULL, 0)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't register callbacks for second group info broadcast: %s", H5_daos_err_to_string(ret))

            /* Schedule second bcast and transfer ownership of udata */
            if(0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule task for second group info broadcast: %s", H5_daos_err_to_string(ret))
            udata = NULL;
        } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Close dataset */
        if(H5_daos_dataset_close((H5_daos_dset_t *)udata->obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close dataset")

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast dataset info completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_metatask, ret_value);

        /* Free buffer */
        DV_free(udata->buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == H5_DAOS_DAOS_GET_ERROR);

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
    H5_daos_mpi_ibcast_ud_t *udata;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for group info receive task")

    assert(udata->req);
    assert(udata->obj);
    assert(udata->obj->item.file);
    assert(!udata->req->file->closed);
    assert(udata->obj->item.file->my_rank > 0);
    assert(udata->obj->item.type == H5I_DATASET);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast dataset info";
    } /* end if */
    else {
        uint64_t type_buf_len = 0;
        uint64_t space_buf_len = 0;
        uint64_t dcpl_buf_len = 0;
        uint64_t fill_val_len = 0;
        size_t dinfo_len;
        uint8_t *p = udata->buffer;

        /* Decode oid */
        UINT64DECODE(p, udata->obj->oid.lo)
        UINT64DECODE(p, udata->obj->oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_buf_len)
        UINT64DECODE(p, space_buf_len)
        UINT64DECODE(p, dcpl_buf_len)
        UINT64DECODE(p, fill_val_len)

        /* Check for type_len set to 0 - indicates failure */
        if(type_buf_len == 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_REMOTE_ERROR, "lead process failed to open dataset")

        /* Calculate data length */
        dinfo_len = (size_t)type_buf_len + (size_t)space_buf_len + (size_t)dcpl_buf_len + (size_t)fill_val_len + 6 * sizeof(uint64_t);

        /* Reissue bcast if necesary */
        if(dinfo_len > (size_t)udata->count) {
            tse_task_t *bcast_task;

            assert(udata->buffer_len == H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                    + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                    + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE);
            assert(udata->count == H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                    + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                    + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE);

            /* Realloc buffer */
            DV_free(udata->buffer);
            if(NULL == (udata->buffer = DV_malloc(dinfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "failed to allocate memory for dataset info buffer")
            udata->buffer_len = dinfo_len;
            udata->count = dinfo_len;

            /* Create task for second bcast */
            if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, &udata->obj->item.file->sched, udata, &bcast_task)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't create task for second dataset info broadcast")

            /* Set callback functions for second bcast */
            if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_dset_open_recv_comp_cb, NULL, 0)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't register callbacks for second dataset info broadcast: %s", H5_daos_err_to_string(ret))

            /* Schedule second bcast and transfer ownership of udata */
            if(0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule task for second dataset info broadcast: %s", H5_daos_err_to_string(ret))
            udata = NULL;
        } /* end if */
        else {
            /* Open dataset */
            if(0 != (ret = daos_obj_open(udata->obj->item.file->coh, udata->obj->oid, udata->obj->item.file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &udata->obj->obj_oh, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, ret, "can't open dataset: %s", H5_daos_err_to_string(ret))

            /* Finish building dataset object */
            if(0 != (ret = H5_daos_dset_open_end((H5_daos_dset_t *)udata->obj,
                    p, type_buf_len, space_buf_len, dcpl_buf_len, fill_val_len,
                    udata->req->dxpl_id)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't finish opening dataset")
        } /* end else */
    } /* end else */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Close dataset */
        if(H5_daos_dataset_close((H5_daos_dset_t *)udata->obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close dataset")

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast dataset info completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_metatask, ret_value);

        /* Free buffer */
        DV_free(udata->buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == H5_DAOS_DAOS_GET_ERROR);

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
    uint8_t *p;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for dataset info read task")

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);
    assert(udata->md_rw_cb_ud.obj);
    assert(udata->fetch_metatask);
    assert(!udata->md_rw_cb_ud.req->file->closed);
    assert(udata->md_rw_cb_ud.obj->item.type == H5I_DATASET);

    /* Check for buffer not large enough */
    if(task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;
        size_t daos_info_len = udata->md_rw_cb_ud.iod[0].iod_size
                + udata->md_rw_cb_ud.iod[1].iod_size
                + udata->md_rw_cb_ud.iod[2].iod_size
                + udata->md_rw_cb_ud.iod[3].iod_size;

        /* Verify iod size makes sense */
        if(udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_TYPE_BUF_SIZE
                || udata->md_rw_cb_ud.sg_iov[1].iov_buf_len != H5_DAOS_SPACE_BUF_SIZE
                || udata->md_rw_cb_ud.sg_iov[2].iov_buf_len != H5_DAOS_DCPL_BUF_SIZE
                || udata->md_rw_cb_ud.sg_iov[3].iov_buf_len != H5_DAOS_FILL_VAL_BUF_SIZE)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "buffer length does not match expected value")

        if(udata->bcast_udata) {
            /* Reallocate group info buffer if necessary */
            if(daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                    + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE) {
                udata->bcast_udata->buffer = DV_free(udata->bcast_udata->buffer);
                if(NULL == (udata->bcast_udata->buffer = DV_malloc(daos_info_len + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "can't allocate buffer for serialized dataset info")
                udata->bcast_udata->buffer_len = daos_info_len + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE;
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->bcast_udata->buffer + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE;
        } /* end if */
        else {
            /* Reallocate dataset info buffer if necessary */
            if(daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                    + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE) {
                udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_free(udata->md_rw_cb_ud.sg_iov[0].iov_buf);
                if(NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(daos_info_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "can't allocate buffer for serialized dataset info")
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
        if(udata->md_rw_cb_ud.iod[3].iod_size > 0) {
            p += udata->md_rw_cb_ud.iod[2].iod_size;
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[3], p, udata->md_rw_cb_ud.iod[3].iod_size);
            udata->md_rw_cb_ud.sgl[3].sg_nr_out = 0;
        } /* end if */
        else
            udata->md_rw_cb_ud.nr--;

        /* Create task for reissued dataset metadata read */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_FETCH, &udata->md_rw_cb_ud.obj->item.file->sched, 0, NULL, &fetch_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't create task to read dataset medadata: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for dataset metadata read */
        if(0 != (ret = tse_task_register_cbs(fetch_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_dinfo_read_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't register callbacks for task to read dataset medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for dataset metadata read */
        (void)tse_task_set_priv(fetch_task, udata);

        /* Schedule dataset metadata read task and give it a reference to req
         * and the dataset */
        if(0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule task to read dataset metadata: %s", H5_daos_err_to_string(ret))
        udata = NULL;
    } /* end if */
    else {
        /* Handle errors in fetch task.  Only record error in udata->req_status
         * if it does not already contain an error (it could contain an error if
         * another task this task is not dependent on also failed). */
        if(task->dt_result < H5_DAOS_PRE_ERROR
                && udata->md_rw_cb_ud.req->status >= H5_DAOS_INCOMPLETE) {
            udata->md_rw_cb_ud.req->status = task->dt_result;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        } /* end if */
        else {
            uint64_t type_buf_len = (uint64_t)(udata->md_rw_cb_ud.sg_iov[1].iov_buf
                    - udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            uint64_t space_buf_len = (uint64_t)(udata->md_rw_cb_ud.sg_iov[2].iov_buf
                    - udata->md_rw_cb_ud.sg_iov[1].iov_buf);
            uint64_t dcpl_buf_len = udata->md_rw_cb_ud.nr >= 4 ?
                    (uint64_t)(udata->md_rw_cb_ud.sg_iov[3].iov_buf
                    - udata->md_rw_cb_ud.sg_iov[2].iov_buf)
                    : udata->md_rw_cb_ud.iod[2].iod_size;

            if(udata->bcast_udata) {
                /* Encode oid */
                p = udata->bcast_udata->buffer;
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
            if(0 != (ret = H5_daos_dset_open_end((H5_daos_dset_t *)udata->md_rw_cb_ud.obj,
                    udata->md_rw_cb_ud.sg_iov[0].iov_buf, type_buf_len,
                    space_buf_len, dcpl_buf_len,
                    (uint64_t)udata->md_rw_cb_ud.iod[3].iod_size,
                    udata->md_rw_cb_ud.req->dxpl_id)))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't finish opening dataset")
        } /* end else */
    } /* end else */

done:
    /* Clean up if this is the last fetch task */
    if(udata) {
        /* Close dataset */
        if(H5_daos_dataset_close((H5_daos_dset_t *)udata->md_rw_cb_ud.obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close dataset")

        if(udata->bcast_udata) {
            /* Clear broadcast buffer if there was an error */
            if(udata->md_rw_cb_ud.req->status < H5_DAOS_INCOMPLETE)
                (void)memset(udata->bcast_udata->buffer, 0, udata->bcast_udata->count);
        } /* end if */
        else
            /* No broadcast, free buffer */
            DV_free(udata->md_rw_cb_ud.sg_iov[0].iov_buf);

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < 0 && udata->md_rw_cb_ud.req->status >= H5_DAOS_INCOMPLETE) {
            udata->md_rw_cb_ud.req->status = ret_value;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

        /* Complete fetch metatask */
        tse_task_complete(udata->fetch_metatask, ret_value);

        assert(!udata->md_rw_cb_ud.free_dkey);
        assert(!udata->md_rw_cb_ud.free_akeys);

        /* Free udata */
        DV_free(udata);
    } /* end if */

    return ret_value;
} /* end H5_daos_dinfo_read_comp_cb */


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
H5_daos_dataset_open(void *_item,
    const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
    hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_dset_t *dset = NULL;
    H5_daos_group_t *target_grp = NULL;
    uint8_t *dinfo_buf = NULL;
    size_t dinfo_buf_size = 0;
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    H5_daos_omd_fetch_ud_t *fetch_udata = NULL;
    hbool_t collective;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    void *ret_value = NULL;

    /* Make sure H5_DAOS_g is set.  Eventually move this to a FUNC_ENTER_API
     * type macro? */
    H5_DAOS_G_INIT(NULL)

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    collective = item->file->fapl_cache.is_collective_md_read;
    if(!collective && (H5P_DATASET_ACCESS_DEFAULT != dapl_id))
        if(H5Pget_all_coll_metadata_ops(dapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get collective metadata reads property")

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(item->file, dxpl_id)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = H5FL_CALLOC(H5_daos_dset_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.open_req = NULL;
    dset->obj.item.file = item->file;
    dset->obj.item.rc = 1;
    dset->obj.obj_oh = DAOS_HDL_INVAL;
    dset->type_id = FAIL;
    dset->file_type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "failed to copy dapl");

    /* Set up broadcast user data (if appropriate) and calculate initial dataset
     * info buffer size */
    if(collective && (item->file->num_procs > 1)) {
        if(NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "failed to allocate buffer for MPI broadcast user data")
        bcast_udata->req = int_req;
        bcast_udata->obj = &dset->obj;
        bcast_udata->buffer = NULL;
        bcast_udata->buffer_len = 0;
        bcast_udata->count = 0;

        dinfo_buf_size = H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE;
    } /* end if */
    else
        dinfo_buf_size = H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE;

    /* Check if we're actually opening the group or just receiving the dataset
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        tse_task_t *fetch_task = NULL;
        uint8_t *p;

        /* Check for open by object token */
        if(H5VL_OBJECT_BY_TOKEN == loc_params->type) {
            /* Generate oid from token */
            if(H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &dset->obj.oid) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't convert object token to OID")
        } /* end if */
        else {
            htri_t link_resolved;

            /* Open using name parameter */
            if(H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "unsupported dataset open location parameters type")
            if(!name)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset name is NULL")

            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, dxpl_id,
                    req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path")

            /* Follow link to dataset */
            if((link_resolved = H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &dset->obj.oid)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_TRAVERSE, NULL, "can't follow link to dataset")
            if(!link_resolved)
                D_GOTO_ERROR(H5E_DATASET, H5E_TRAVERSE, NULL, "link to dataset did not resolve")
        } /* end else */

        /* Open dataset object */
        if(H5_daos_obj_open(item->file, int_req, &dset->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &dset->obj.obj_oh, "dataset object open", &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset object")

        /* Allocate argument struct for fetch task */
        if(NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(sizeof(H5_daos_omd_fetch_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fetch callback arguments")

        /* Set up operation to read datatype, dataspace, and DCPL sizes from
         * dataset */
        /* Set up ud struct */
        fetch_udata->md_rw_cb_ud.req = int_req;
        fetch_udata->md_rw_cb_ud.obj = &dset->obj;
        fetch_udata->bcast_udata = bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.iod[0].iod_name, (void *)H5_daos_type_key_g, H5_daos_type_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[0].iod_nr = 1u;
        fetch_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&fetch_udata->md_rw_cb_ud.iod[1].iod_name, (void *)H5_daos_space_key_g, H5_daos_space_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[1].iod_nr = 1u;
        fetch_udata->md_rw_cb_ud.iod[1].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&fetch_udata->md_rw_cb_ud.iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[2].iod_nr = 1u;
        fetch_udata->md_rw_cb_ud.iod[2].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[2].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&fetch_udata->md_rw_cb_ud.iod[3].iod_name, (void *)H5_daos_fillval_key_g, H5_daos_fillval_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[3].iod_nr = 1u;
        fetch_udata->md_rw_cb_ud.iod[3].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[3].iod_type = DAOS_IOD_SINGLE;

        fetch_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Allocate initial dataset info buffer */
        if(NULL == (dinfo_buf = DV_malloc(dinfo_buf_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataset info")

        /* Set up buffer */
        if(bcast_udata) {
            p = dinfo_buf + (6 * sizeof(uint64_t));
            bcast_udata->buffer = dinfo_buf;
            dinfo_buf = NULL;
            bcast_udata->buffer_len = dinfo_buf_size;
            bcast_udata->count = dinfo_buf_size;
        } /* end if */
        else
            p = dinfo_buf;

        /* Set up sgl */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], p, (daos_size_t)H5_DAOS_TYPE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr = 1;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs = &fetch_udata->md_rw_cb_ud.sg_iov[0];
        p += H5_DAOS_TYPE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[1], p, (daos_size_t)H5_DAOS_SPACE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr = 1;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_iovs = &fetch_udata->md_rw_cb_ud.sg_iov[1];
        p += H5_DAOS_SPACE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[2], p, (daos_size_t)H5_DAOS_DCPL_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[2].sg_nr = 1;
        fetch_udata->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[2].sg_iovs = &fetch_udata->md_rw_cb_ud.sg_iov[2];
        p += H5_DAOS_DCPL_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[3], p, (daos_size_t)H5_DAOS_FILL_VAL_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[3].sg_nr = 1;
        fetch_udata->md_rw_cb_ud.sgl[3].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[3].sg_iovs = &fetch_udata->md_rw_cb_ud.sg_iov[3];

        /* Set nr */
        fetch_udata->md_rw_cb_ud.nr = 4u;

        /* Set task name */
        fetch_udata->md_rw_cb_ud.task_name = "dataset metadata read";

        /* Create meta task for dataset metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_dinfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if(0 != (ret = tse_task_create(NULL, &item->file->sched, NULL, &fetch_udata->fetch_metatask)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create meta task for dataset metadata read: %s", H5_daos_err_to_string(ret))

        /* Create task for dataset metadata read */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_FETCH, &item->file->sched, 0, NULL, &fetch_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to read dataset medadata: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        assert(dep_task);
        if(0 != (ret = tse_task_register_deps(fetch_task, 1, &dep_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dependencies for dataset metadata read: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for dataset metadata read */
        if(0 != (ret = tse_task_register_cbs(fetch_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_dinfo_read_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't register callbacks for task to read dataset medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for dataset metadata write */
        (void)tse_task_set_priv(fetch_task, fetch_udata);

        /* Schedule meta task */
        if(0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule meta task for dataset metadata read: %s", H5_daos_err_to_string(ret))

        /* Schedule dataset metadata write task (or save it to be scheduled
         * later) and give it a reference to req and the dataset */
        assert(first_task);
        if(0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to read dataset metadata: %s", H5_daos_err_to_string(ret))
        dep_task = fetch_udata->fetch_metatask;
        int_req->rc++;
        dset->obj.item.rc++;
        fetch_udata = NULL;
        dinfo_buf = NULL;
    } /* end if */
    else {
        assert(bcast_udata);

        /* Allocate buffer for dataset info */
        dinfo_buf_size = H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE;
        if(NULL == (bcast_udata->buffer = DV_malloc(dinfo_buf_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataset info")
        bcast_udata->buffer_len = dinfo_buf_size;
        bcast_udata->count = dinfo_buf_size;
    } /* end else */

    ret_value = dset;

done:
    /* Broadcast dataset info */
    if(bcast_udata) {
        assert(!dinfo_buf);
        assert(dinfo_buf_size == H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE
                + H5_DAOS_DCPL_BUF_SIZE + H5_DAOS_FILL_VAL_BUF_SIZE
                + 6 * H5_DAOS_ENCODED_UINT64_T_SIZE);

        /* Handle failure */
        if(NULL == ret_value) {
            /* Allocate buffer for dataset info if necessary, either way set
             * buffer to 0 to indicate failure */
            if(bcast_udata->buffer) {
                assert((size_t)bcast_udata->buffer_len == dinfo_buf_size);
                assert((size_t)bcast_udata->count == dinfo_buf_size);
                (void)memset(bcast_udata->buffer, 0, dinfo_buf_size);
            } /* end if */
            else {
                if(NULL == (bcast_udata->buffer = DV_calloc(dinfo_buf_size)))
                    D_DONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataset info")
                bcast_udata->buffer_len = dinfo_buf_size;
                bcast_udata->count = dinfo_buf_size;
            } /* end else */
        } /* end if */

        if(bcast_udata->buffer) {
            tse_task_t *bcast_task;

            /* Create meta task for dataset info bcast.  This empty task will be
             * completed when the bcast is finished by the completion callback.
             * We can't use bcast_task since it may not be completed after the
             * first bcast. */
            if(0 != (ret = tse_task_create(NULL, &item->file->sched, NULL, &bcast_udata->bcast_metatask)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create meta task for dataset info broadcast: %s", H5_daos_err_to_string(ret))
            /* Create task for dataset info bcast */
            if(0 != (ret = tse_task_create(H5_daos_mpi_ibcast_task, &item->file->sched, bcast_udata, &bcast_task)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to broadcast dataset info: %s", H5_daos_err_to_string(ret))
            /* Register task dependency if present */
            else if(dep_task && 0 != (ret = tse_task_register_deps(bcast_task, 1, &dep_task)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dependencies for dataset info broadcast task: %s", H5_daos_err_to_string(ret))
            /* Set callback functions for dataset info bcast */
            else if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, item->file->my_rank == 0 ? H5_daos_dset_open_bcast_comp_cb : H5_daos_dset_open_recv_comp_cb, NULL, 0)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't register callbacks for group info broadcast: %s", H5_daos_err_to_string(ret))
            /* Schedule meta task */
            else if(0 != (ret = tse_task_schedule(bcast_udata->bcast_metatask, false)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule meta task for dataset info broadcast: %s", H5_daos_err_to_string(ret))
            /* Schedule bcast and transfer ownership of bcast_udata */
            else {
                if(first_task) {
                    if(0 != (ret = tse_task_schedule(bcast_task, false)))
                        D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task for dataset info broadcast: %s", H5_daos_err_to_string(ret))
                    else {
                        int_req->rc++;
                        dset->obj.item.rc++;
                        dep_task = bcast_udata->bcast_metatask;
                        bcast_udata = NULL;
                    } /* end else */
                } /* end if */
                else {
                    first_task = bcast_task;
                    int_req->rc++;
                    dset->obj.item.rc++;
                    dep_task = bcast_udata->bcast_metatask;
                    bcast_udata = NULL;
                } /* end else */
            } /* end else */
        } /* end if */

        /* Cleanup on failure */
        if(bcast_udata) {
            assert(NULL == ret_value);
            DV_free(bcast_udata->buffer);
            bcast_udata = DV_free(bcast_udata);
        } /* end if */
    } /* end if */

    if(int_req) {
        tse_task_t *finalize_task;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(item->file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, NULL, "dataset open failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't free request")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close group")

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close group */
        if(dset && H5_daos_dataset_close(dset, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset")

        /* Free memory */
        fetch_udata = DV_free(fetch_udata);
        dinfo_buf = DV_free(dinfo_buf);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!fetch_udata);
    assert(!bcast_udata);
    assert(!dinfo_buf);

    D_FUNC_LEAVE_API
} /* end H5_daos_dataset_open() */


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
H5_daos_sel_to_recx_iov(hid_t space_id, size_t type_size, void *buf,
    daos_recx_t **recxs, daos_iov_t **sg_iovs, size_t *list_nused)
{
    size_t nseq;
    size_t nelem;
    hsize_t off[H5_DAOS_SEQ_LIST_LEN];
    size_t len[H5_DAOS_SEQ_LIST_LEN];
    size_t buf_len = 1;
    void *vp_ret;
    size_t szi;
    hid_t sel_iter = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(recxs || sg_iovs);
    assert(!recxs || *recxs);
    assert(!sg_iovs || *sg_iovs);
    assert(list_nused);

    /* Initialize list_nused */
    *list_nused = 0;

    /* Initialize selection iterator We use 1 for the element size here so that
     * the sequence list offsets and lengths are returned in terms of numbers of
     * elements, not bytes.  This way the returned values better match the
     * values DAOS expects to receive, which are also in terms of numbers of
     * elements. */
    if((sel_iter = H5Ssel_iter_create(space_id, 1, H5S_SEL_ITER_SHARE_WITH_DATASPACE)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to create selection iterator")

    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes */
        if(H5Ssel_iter_get_seq_list(sel_iter, (size_t)H5_DAOS_SEQ_LIST_LEN, (size_t)-1, &nseq, &nelem, off, len) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")

        /* Make room for sequences in recxs */
        if((buf_len == 1) && (nseq > 1)) {
            if(recxs)
                if(NULL == (*recxs = (daos_recx_t *)DV_malloc(H5_DAOS_SEQ_LIST_LEN * sizeof(daos_recx_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate memory for records")
            if(sg_iovs)
                if(NULL == (*sg_iovs = (daos_iov_t *)DV_malloc(H5_DAOS_SEQ_LIST_LEN * sizeof(daos_iov_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate memory for sgl iovs")
            buf_len = H5_DAOS_SEQ_LIST_LEN;
        } /* end if */
        else if(*list_nused + nseq > buf_len) {
            if(recxs) {
                if(NULL == (vp_ret = DV_realloc(*recxs, 2 * buf_len * sizeof(daos_recx_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate memory for records")
                *recxs = (daos_recx_t *)vp_ret;
            } /* end if */
            if(sg_iovs) {
                if(NULL == (vp_ret = DV_realloc(*sg_iovs, 2 * buf_len * sizeof(daos_iov_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate memory for sgls")
                *sg_iovs = (daos_iov_t *)vp_ret;
            } /* end if */
            buf_len *= 2;
        } /* end if */
        assert(*list_nused + nseq <= buf_len);

        /* Copy offsets/lengths to recxs and sg_iovs */
        for(szi = 0; szi < nseq; szi++) {
            if(recxs) {
                (*recxs)[szi + *list_nused].rx_idx = (uint64_t)off[szi];
                (*recxs)[szi + *list_nused].rx_nr = (uint64_t)len[szi];
            } /* end if */
            if(sg_iovs)
                daos_iov_set(&(*sg_iovs)[szi + *list_nused],
                        (uint8_t *)buf + (off[szi] * type_size),
                        (daos_size_t)len[szi] * (daos_size_t)type_size);
        } /* end for */
        *list_nused += nseq;
    } while(nseq == H5_DAOS_SEQ_LIST_LEN);

done:
    /* Release selection iterator */
    if(sel_iter >= 0 && H5Ssel_iter_close(sel_iter) < 0)
        D_DONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to close selection iterator")

    D_FUNC_LEAVE
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
H5_daos_scatter_cb(const void **src_buf, size_t *src_buf_bytes_used,
    void *_udata)
{
    H5_daos_scatter_cb_ud_t *udata = (H5_daos_scatter_cb_ud_t *)_udata;
    herr_t ret_value = SUCCEED;

    /* Set src_buf and src_buf_bytes_used to use the entire buffer */
    *src_buf = udata->buf;
    *src_buf_bytes_used = udata->len;

    /* DSINC - This function used to always return SUCCEED without needing an
     * herr_t. Might need an additional FUNC_LEAVE macro to do this, or modify
     * the current one to take in the ret_value.
     */
    D_FUNC_LEAVE
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
    daos_obj_rw_t *update_args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for chunk I/O task")

    assert(udata);
    assert(udata->dset);
    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);

    /* Handle errors */
    if(udata->req->status < H5_DAOS_INCOMPLETE) {
        tse_task_complete(task, H5_DAOS_PRE_ERROR);
        udata = NULL;
        D_GOTO_DONE(H5_DAOS_PRE_ERROR);
    } /* end if */

    /* Set I/O task arguments */
    if(NULL == (update_args = daos_task_get_args(task))) {
        tse_task_complete(task, H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get arguments for chunk I/O task")
    } /* end if */
    update_args->oh = udata->dset->obj.obj_oh;
    update_args->th = DAOS_TX_NONE;
    update_args->flags = 0;
    update_args->dkey = &udata->dkey;
    update_args->nr = 1;
    update_args->iods = &udata->iod;
    update_args->sgls = &udata->sgl;
    update_args->maps = NULL;

done:
    D_FUNC_LEAVE
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
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for chunk I/O task")

    assert(udata);
    assert(udata->req);
    assert(udata->req->file);
    assert(!udata->req->file->closed);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "raw data I/O";
    } /* end if */

    /* Close dataset */
    if(H5_daos_dataset_close(udata->dset, H5I_INVALID_HID, NULL) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close object")

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = ret_value;
        udata->req->failed_task = "raw data I/O completion callback";
    } /* end if */

    /* Release our reference to req */
    if(H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

    /* Free private data */
    DV_free(udata->dkey.iov_buf);
    if(udata->recxs != &udata->recx)
        DV_free(udata->recxs);
    if(udata->sg_iovs != &udata->sg_iov)
        DV_free(udata->sg_iovs);
    DV_free(udata);

done:
    D_FUNC_LEAVE
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
H5_daos_dataset_io_types_equal(H5_daos_dset_t *dset, daos_key_t *dkey, hssize_t H5VL_DAOS_UNUSED num_elem,
    hid_t H5VL_DAOS_UNUSED mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t H5VL_DAOS_UNUSED dxpl_id,
    dset_io_type io_type, void *buf, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_chunk_io_ud_t *chunk_io_ud = NULL;
    size_t tot_nseq;
    size_t file_type_size;
    tse_task_t *io_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(dset);
    assert(dkey);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Get datatype size */
    if((file_type_size = H5Tget_size(dset->file_type_id)) == 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype size for file datatype")

    /* Allocate argument struct */
    if(NULL == (chunk_io_ud = (H5_daos_chunk_io_ud_t *)DV_calloc(sizeof(H5_daos_chunk_io_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O callback arguments")
    chunk_io_ud->recxs = &chunk_io_ud->recx;
    chunk_io_ud->sg_iovs = &chunk_io_ud->sg_iov;

    /* Point to dset */
    chunk_io_ud->dset = dset;

    /* Point to req */
    chunk_io_ud->req = req;

    /* Copy dkey to new buffer */
    if(NULL == (chunk_io_ud->dkey.iov_buf = DV_malloc(dkey->iov_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkey")
    (void)memcpy(chunk_io_ud->dkey.iov_buf, dkey->iov_buf, dkey->iov_len);
    chunk_io_ud->dkey.iov_len = chunk_io_ud->dkey.iov_buf_len = dkey->iov_len;

    /* Set up iod */
    memset(&chunk_io_ud->iod, 0, sizeof(chunk_io_ud->iod));
    chunk_io_ud->akey_buf = H5_DAOS_CHUNK_KEY;
    daos_iov_set(&chunk_io_ud->iod.iod_name, (void *)&chunk_io_ud->akey_buf, (daos_size_t)(sizeof(chunk_io_ud->akey_buf)));
    chunk_io_ud->iod.iod_size = (daos_size_t)file_type_size;
    chunk_io_ud->iod.iod_type = DAOS_IOD_ARRAY;

    /* Check for a memory space of H5S_ALL, use file space in this case */
    if(mem_space_id == H5S_ALL) {
        /* Calculate both recxs and sg_iovs at the same time from file space */
        if(H5_daos_sel_to_recx_iov(file_space_id, file_type_size, buf, &chunk_io_ud->recxs, &chunk_io_ud->sg_iovs, &tot_nseq) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
        chunk_io_ud->iod.iod_nr = (unsigned)tot_nseq;
        chunk_io_ud->sgl.sg_nr = (uint32_t)tot_nseq;
        chunk_io_ud->sgl.sg_nr_out = 0;
    } /* end if */
    else {
        /* Calculate recxs from file space */
        if(H5_daos_sel_to_recx_iov(file_space_id, file_type_size, buf, &chunk_io_ud->recxs, NULL, &tot_nseq) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
        chunk_io_ud->iod.iod_nr = (unsigned)tot_nseq;

        /* Calculate sg_iovs from mem space */
        if(H5_daos_sel_to_recx_iov(mem_space_id, file_type_size, buf, NULL, &chunk_io_ud->sg_iovs, &tot_nseq) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
        chunk_io_ud->sgl.sg_nr = (uint32_t)tot_nseq;
        chunk_io_ud->sgl.sg_nr_out = 0;
    } /* end else */

    /* Point iod and sgl to lists generated above */
    chunk_io_ud->iod.iod_recxs = chunk_io_ud->recxs;
    chunk_io_ud->sgl.sg_iovs = chunk_io_ud->sg_iovs;

    /* No selection in the file */
    if(chunk_io_ud->iod.iod_nr == 0) {
        *dep_task = NULL;
        D_GOTO_DONE(SUCCEED);
    } /* end if */

    if(io_type == IO_READ) {
        /* Handle fill values */
        size_t i;

        if(dset->dcpl_cache.fill_method == H5_DAOS_ZERO_FILL) {
            /* Just set all locations pointed to by sg_iovs to zero */
            for(i = 0; i < tot_nseq; i++)
                (void)memset(chunk_io_ud->sg_iovs[i].iov_buf, 0, chunk_io_ud->sg_iovs[i].iov_len);
        } /* end if */
        else if(dset->dcpl_cache.fill_method == H5_DAOS_COPY_FILL) {
            /* Copy fill value to all locations pointed to by sg_iovs */
            size_t iov_buf_written;

            assert(dset->fill_val);

            for(i = 0; i < tot_nseq; i++) {
                for(iov_buf_written = 0;
                        iov_buf_written < chunk_io_ud->sg_iovs[i].iov_len;
                        iov_buf_written += file_type_size)
                    (void)memcpy((uint8_t *)chunk_io_ud->sg_iovs[i].iov_buf + iov_buf_written,
                            dset->fill_val, file_type_size);
                assert(iov_buf_written == chunk_io_ud->sg_iovs[i].iov_len);
            } /* end for */
        } /* end if */

        /* Create task to read data from dataset */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_FETCH, &dset->obj.item.file->sched, 0, NULL, &io_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to read data: %s", H5_daos_err_to_string(ret))
    } /* end (io_type == IO_READ) */
    else /* (io_type == IO_WRITE) */
        /* Create task to write data to dataset */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_UPDATE, &dset->obj.item.file->sched, 0, NULL, &io_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to write data: %s", H5_daos_err_to_string(ret))

    /* Set callback functions for raw data write */
    if(0 != (ret = tse_task_register_cbs(io_task, H5_daos_chunk_io_prep_cb, NULL, 0, H5_daos_chunk_io_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't register callbacks for data I/O task: %s", H5_daos_err_to_string(ret))

    /* Set private data for raw data write */
    (void)tse_task_set_priv(io_task, chunk_io_ud);

    /* Register task dependency if present */
    if(*dep_task)
        if(0 != (ret = tse_task_register_deps(io_task, 1, dep_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependency for dataset I/O task: %s", H5_daos_err_to_string(ret))

    /* Set first_task and dep_task pointers.  first_task should have been NULL
     * when calling this function.  Do not schedule task. */
    assert(!*first_task);
    *first_task = io_task;
    *dep_task = io_task;

    /* Task will be scheduled, give it a reference to req */
    chunk_io_ud->req->rc++;
    chunk_io_ud->dset->obj.item.rc++;

done:
    /* Cleanup on failure */
    if(ret_value < 0 && chunk_io_ud) {
        if(chunk_io_ud->recxs != &chunk_io_ud->recx)
            DV_free(chunk_io_ud->recxs);
        if(chunk_io_ud->sg_iovs != &chunk_io_ud->sg_iov)
            DV_free(chunk_io_ud->sg_iovs);
        DV_free(chunk_io_ud->dkey.iov_buf);
        chunk_io_ud = DV_free(chunk_io_ud);
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_dataset_io_types_equal() */


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
H5_daos_dataset_io_types_unequal(H5_daos_dset_t *dset, daos_key_t *dkey, hssize_t num_elem,
    hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id,
    dset_io_type io_type, void *buf, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_tconv_reuse_t reuse = H5_DAOS_TCONV_REUSE_NONE;
    daos_sg_list_t sgl;
    daos_recx_t recx;
    daos_recx_t *recxs = &recx;
    daos_iov_t sg_iov;
    daos_iov_t *sg_iovs = &sg_iov;
    daos_iod_t iod;
    uint8_t akey = H5_DAOS_CHUNK_KEY;
    hbool_t contig = FALSE;
    hbool_t fill_bkg = FALSE;
    size_t tot_nseq;
    size_t mem_type_size;
    size_t file_type_size;
    hid_t sel_iter = H5I_INVALID_HID;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(dset);
    assert(dkey);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set first_task and dep_task to NULL DSINC */
    *first_task = NULL;
    *dep_task = NULL;

    if(io_type == IO_READ) {
        size_t nseq_tmp;
        size_t nelem_tmp;
        hsize_t sel_off;
        size_t sel_len;

        /* Initialize selection iterator.  We use 1 for the element size here so
         * that the sequence list offsets and lengths are returned in terms of
         * numbers of elements, not bytes.  In this case it just saves us from
         * needing to multiply num_elem by the type size when checking for a
         * contiguous selection */
        if((sel_iter = H5Ssel_iter_create(mem_space_id, 1, H5S_SEL_ITER_SHARE_WITH_DATASPACE)) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to create selection iterator")

        /* Get the sequence list - only check the first sequence because we only
         * care if it is contiguous and if so where the contiguous selection
         * begins */
        if(H5Ssel_iter_get_seq_list(sel_iter, (size_t)1, (size_t)-1, &nseq_tmp, &nelem_tmp, &sel_off, &sel_len) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
        contig = (sel_len == (size_t)num_elem);

        /* Initialize type conversion */
        if(H5_daos_tconv_init(
                dset->file_type_id,
                &file_type_size,
                mem_type_id,
                &mem_type_size,
                (size_t)num_elem,
                dset->dcpl_cache.fill_method == H5_DAOS_ZERO_FILL,
                FALSE,
                &tconv_buf,
                &bkg_buf,
                contig ? &reuse : NULL,
                &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion")

        /* Reuse buffer as appropriate */
        if(contig) {
            sel_off *= (hsize_t)mem_type_size;
            if(reuse == H5_DAOS_TCONV_REUSE_TCONV)
                tconv_buf = (char *)buf + (size_t)sel_off;
            else if(reuse == H5_DAOS_TCONV_REUSE_BKG)
                bkg_buf = (char *)buf + (size_t)sel_off;
        } /* end if */
    } /* end (io_type == IO_READ) */
    else
        /* Initialize type conversion */
        if(H5_daos_tconv_init(
                mem_type_id,
                &mem_type_size,
                dset->file_type_id,
                &file_type_size,
                (size_t)num_elem,
                FALSE,
                TRUE,
                &tconv_buf,
                &bkg_buf,
                NULL,
                &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion")

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)&akey, (daos_size_t)(sizeof(akey)));
    iod.iod_size = (daos_size_t)file_type_size;
    iod.iod_type = DAOS_IOD_ARRAY;

    /* Build recxs and sg_iovs */

    /* Calculate recxs from file space */
    if(H5_daos_sel_to_recx_iov(file_space_id, file_type_size, buf, &recxs, NULL, &tot_nseq) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
    iod.iod_nr = (unsigned)tot_nseq;
    iod.iod_recxs = recxs;

    /* No selection in the file */
    if(iod.iod_nr == 0)
        D_GOTO_DONE(SUCCEED);

    /* Set up constant sgl info */
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    if(io_type == IO_READ) {
        /* Handle fill values */
        if(dset->dcpl_cache.fill_method == H5_DAOS_ZERO_FILL) {
            /* H5_daos_tconv_init() will have cleared the tconv buf, but not if
             * we're reusing buf as tconv_buf */
            if(reuse == H5_DAOS_TCONV_REUSE_TCONV)
                (void)memset(tconv_buf, 0, (daos_size_t)num_elem * (daos_size_t)file_type_size);
        } /* end if */
        else if(dset->dcpl_cache.fill_method == H5_DAOS_COPY_FILL) {
            hssize_t i;

            assert(dset->fill_val);

            /* Copy the fill value to every element in tconv_buf */
            for(i = 0; i < num_elem; i++)
                (void)memcpy((uint8_t *)tconv_buf + ((size_t)i * file_type_size),
                        dset->fill_val, file_type_size);
        } /* end if */

        /* Set sg_iov to point to tconv_buf */
        daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)num_elem * (daos_size_t)file_type_size);

        /* Read data to tconv_buf */
        if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", H5_daos_err_to_string(ret))

        /* Gather data to background buffer if necessary */
        if(fill_bkg && (reuse != H5_DAOS_TCONV_REUSE_BKG))
            if(H5Dgather(mem_space_id, buf, mem_type_id, (size_t)num_elem * mem_type_size, bkg_buf, NULL, NULL) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to background buffer")

        /* Perform type conversion */
        if(H5Tconvert(dset->file_type_id, mem_type_id, (size_t)num_elem, tconv_buf, bkg_buf, dxpl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

        /* Scatter data to memory buffer if necessary */
        if(reuse != H5_DAOS_TCONV_REUSE_TCONV) {
            H5_daos_scatter_cb_ud_t scatter_cb_ud;

            scatter_cb_ud.buf = tconv_buf;
            scatter_cb_ud.len = (size_t)num_elem * mem_type_size;
            if(H5Dscatter(H5_daos_scatter_cb, &scatter_cb_ud, mem_type_id, mem_space_id, buf) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't scatter data to read buffer")
        } /* end if */
    } /* end (io_type == IO_READ) */
    else {
        /* Check if we need to fill background buffer */
        if(fill_bkg) {
            assert(bkg_buf);

            /* Set sg_iov to point to background buffer */
            daos_iov_set(&sg_iov, bkg_buf, (daos_size_t)num_elem * (daos_size_t)file_type_size);

            /* Read data from dataset to background buffer */
            if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", H5_daos_err_to_string(ret))

            /* Reset iod_size, if the dataset was not allocated then it could
             * have been overwritten by daos_obj_fetch */
            iod.iod_size = file_type_size;
        } /* end if */

        /* Gather data to conversion buffer */
        if(H5Dgather(mem_space_id, buf, mem_type_id, (size_t)num_elem * mem_type_size, tconv_buf, NULL, NULL) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to conversion buffer")

        /* Perform type conversion */
        if(H5Tconvert(mem_type_id, dset->file_type_id, (size_t)num_elem, tconv_buf, bkg_buf, dxpl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

        /* Set sg_iovs to write from tconv_buf */
        daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)num_elem * (daos_size_t)file_type_size);

        /* Write data to dataset */
        if(0 != (ret = daos_obj_update(dset->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, dkey, 1, &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data to dataset: %s", H5_daos_err_to_string(ret))
    } /* end (io_type == IO_WRITE) */

done:
    if(recxs != &recx)
        DV_free(recxs);
    if(sg_iovs != &sg_iov)
        DV_free(sg_iovs);

    if(reuse != H5_DAOS_TCONV_REUSE_TCONV)
        tconv_buf = DV_free(tconv_buf);
    if(reuse != H5_DAOS_TCONV_REUSE_BKG)
        bkg_buf = DV_free(bkg_buf);

    /* Release selection iterator */
    if(sel_iter >= 0 && H5Ssel_iter_close(sel_iter) < 0)
        D_DONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to close selection iterator")

    D_FUNC_LEAVE
} /* end H5_daos_dataset_io_types_unequal() */


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
herr_t
H5_daos_dataset_read(void *_dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t dxpl_id, void *buf, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_select_chunk_info_t *chunk_info = NULL; /* Array of info for each chunk selected in the file */
    H5_daos_chunk_io_func single_chunk_read_func;
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    hssize_t num_elem_file = -1, num_elem_mem;
    uint64_t i;
    uint8_t dkey_buf[1 + (sizeof(uint64_t) * H5S_MAX_RANK)];
    htri_t need_tconv;
    hbool_t close_spaces = FALSE;
    size_t nchunks_sel;
    hid_t real_file_space_id;
    hid_t real_mem_space_id;
    int ndims;
    tse_task_t *finalize_task;
    int ntasks = 0;
    tse_task_t *first_task = NULL;
    tse_task_t **first_tasks = &first_task;
    tse_task_t *finalize_dep = NULL;
    tse_task_t **finalize_deps = &finalize_dep;
    H5_daos_req_t *int_req = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL")
    if(H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")

    /* Get "real" space ids */
    if(file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;
    if(mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else
        real_mem_space_id = mem_space_id;

    /* Get number of elements in selections */
    if((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in file selection")
    if((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in memory selection")

    /* Various sanity and special case checks */
    if(num_elem_file != num_elem_mem)
        D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "number of elements selected in file and memory dataspaces is different")
    if(num_elem_file && !buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "read buffer is NULL but selection has >0 elements")
    if(num_elem_file == 0)
        D_GOTO_DONE(SUCCEED)

    /* Check for the dataset having a chunked storage layout. If it does not,
     * simply set up the dataset as a single "chunk".
     */
    switch(dset->dcpl_cache.layout) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            if(NULL == (chunk_info = (H5_daos_select_chunk_info_t *)DV_malloc(sizeof(H5_daos_select_chunk_info_t))))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate single chunk info buffer")
            nchunks_sel = 1;

            /* Set up "single-chunk dataset", with the "chunk" starting at coordinate 0 */
            chunk_info->fspace_id = real_file_space_id;
            chunk_info->mspace_id = real_mem_space_id;
            memset(chunk_info->chunk_coords, 0, sizeof(chunk_info->chunk_coords));

            break;

        case H5D_CHUNKED:
            /* Get the coordinates of the currently selected chunks in the file, setting up memory and file dataspaces for them */
            if(H5_daos_get_selected_chunk_info(&dset->dcpl_cache, real_file_space_id, real_mem_space_id, &chunk_info, &nchunks_sel) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selected chunk info")

            close_spaces = TRUE;

            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "invalid, unknown or unsupported dataset storage layout type")
    } /* end switch */

    /* Setup the appropriate function for reading the selected chunks */
    /* Check if the type conversion is needed */
    if((need_tconv = H5_daos_need_tconv(dset->file_type_id, mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")
    if(need_tconv)
        /* Type conversion necessary */
        single_chunk_read_func = H5_daos_dataset_io_types_unequal;
    else
        /* No type conversion necessary */
        single_chunk_read_func = H5_daos_dataset_io_types_equal;

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(dset->obj.item.file, need_tconv ? dxpl_id : H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request")

    /* Set up chunk I/O task arrays if there is more than one chunk selected */
    if(nchunks_sel > 1) {
        /* Allocate arrays of tasks */
        if(NULL == (first_tasks = (tse_task_t **)DV_calloc(nchunks_sel * sizeof(tse_task_t *))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate IO first task array")
        if(NULL == (finalize_deps = (tse_task_t **)DV_calloc(nchunks_sel * sizeof(tse_task_t *))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate IO task array")
    } /* end if */

    /* Perform I/O on each chunk selected */
    for(i = 0; i < nchunks_sel; i++) {
        daos_key_t  dkey;
        uint64_t    j;
        uint8_t    *p = dkey_buf;

        /* Encode dkey (chunk coordinates).  Prefix with '\0' to avoid accidental
         * collisions with other d-keys in this object. */
        *p++ = (uint8_t)'\0';
        for(j = 0; j < (uint64_t)ndims; j++)
            UINT64ENCODE(p, chunk_info[i].chunk_coords[j])

        /* Set up dkey */
        daos_iov_set(&dkey, dkey_buf, (daos_size_t)(1 + ((size_t)ndims * sizeof(chunk_info[i].chunk_coords[0]))));

        /* Get number of elements in selection */
        if((num_elem_file = H5Sget_select_npoints(chunk_info[i].fspace_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

        if(single_chunk_read_func(dset, &dkey, num_elem_file, mem_type_id,
                chunk_info[i].mspace_id, chunk_info[i].fspace_id, dxpl_id,
                IO_READ, buf, int_req, &first_tasks[ntasks],
                &finalize_deps[ntasks]) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "dataset read failed")
        if(finalize_deps[ntasks]) /* Remove this check once full async support is implemented or make it an assert DSINC */
            ntasks++;
    } /* end for */

done:
    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &dset->obj.item.file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependencies (if any) */
        else if(ntasks > 0 && 0 != (ret = tse_task_register_deps(finalize_task, ntasks, finalize_deps)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(ret_value < 0)
            int_req->status = H5_DAOS_SETUP_ERROR;

        /* Schedule first tasks */
        for(i = 0; (int)i < ntasks; i++) {
            assert(first_tasks[i]);
            if(0 != (ret = tse_task_schedule(first_tasks[i], false)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))
        } /* end for */

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(dset->obj.item.file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "dataset read failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request")
    } /* end if */

    /* Free memory */
    if(chunk_info) {
        if(close_spaces) {
            for(i = 0; i < nchunks_sel; i++) {
                if((chunk_info[i].mspace_id >= 0) && (H5Sclose(chunk_info[i].mspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close memory space");
                if((chunk_info[i].fspace_id >= 0) && (H5Sclose(chunk_info[i].fspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close file space");
            } /* end for */
        } /* end if */

        DV_free(chunk_info);
    } /* end if */
    if(first_tasks != &first_task) {
        assert(finalize_deps != &finalize_dep);
        DV_free(first_tasks);
        DV_free(finalize_deps);
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5_daos_dataset_read() */


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
herr_t
H5_daos_dataset_write(void *_dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t dxpl_id,
    const void *buf, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_select_chunk_info_t *chunk_info = NULL; /* Array of info for each chunk selected in the file */
    H5_daos_chunk_io_func single_chunk_write_func;
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    hssize_t num_elem_file = -1, num_elem_mem;
    uint64_t i;
    uint8_t dkey_buf[1 + (sizeof(uint64_t) * H5S_MAX_RANK)];
    htri_t need_tconv;
    hbool_t close_spaces = FALSE;
    size_t nchunks_sel;
    hid_t real_file_space_id;
    hid_t real_mem_space_id;
    int ndims;
    tse_task_t *finalize_task;
    int ntasks = 0;
    tse_task_t *first_task = NULL;
    tse_task_t **first_tasks = &first_task;
    tse_task_t *finalize_dep = NULL;
    tse_task_t **finalize_deps = &finalize_dep;
    H5_daos_req_t *int_req = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL")
    if(H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset")

    /* Check for write access */
    if(!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")

    /* Get "real" space ids */
    if(file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;
    if(mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else
        real_mem_space_id = mem_space_id;

    /* Get number of elements in selections */
    if((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in file selection")
    if((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in memory selection")

    /* Various sanity and special case checks */
    if(num_elem_file != num_elem_mem)
        D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "number of elements selected in file and memory dataspaces is different")
    if(num_elem_file && !buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "write buffer is NULL but selection has >0 elements")
    if(num_elem_file == 0)
        D_GOTO_DONE(SUCCEED)

    /* Check for the dataset having a chunked storage layout. If it does not,
     * simply set up the dataset as a single "chunk".
     */
    switch(dset->dcpl_cache.layout) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            if (NULL == (chunk_info = (H5_daos_select_chunk_info_t *) DV_malloc(sizeof(H5_daos_select_chunk_info_t))))
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate single chunk info buffer")
            nchunks_sel = 1;

            /* Set up "single-chunk dataset", with the "chunk" starting at coordinate 0 */
            chunk_info->fspace_id = real_file_space_id;
            chunk_info->mspace_id = real_mem_space_id;
            memset(chunk_info->chunk_coords, 0, sizeof(chunk_info->chunk_coords));

            break;

        case H5D_CHUNKED:
            /* Get the coordinates of the currently selected chunks in the file, setting up memory and file dataspaces for them */
            if(H5_daos_get_selected_chunk_info(&dset->dcpl_cache, real_file_space_id, real_mem_space_id, &chunk_info, &nchunks_sel) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selected chunk info")

            close_spaces = TRUE;

            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            D_GOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "invalid, unknown or unsupported dataset storage layout type")
    } /* end switch */

    /* Setup the appropriate function for reading the selected chunks */
        /* Check if the type conversion is needed */
    if((need_tconv = H5_daos_need_tconv(mem_type_id, dset->file_type_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")
    if(need_tconv)
        /* Type conversion necessary */
        single_chunk_write_func = H5_daos_dataset_io_types_unequal;
    else
        /* No type conversion necessary */
        single_chunk_write_func = H5_daos_dataset_io_types_equal;

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(dset->obj.item.file, need_tconv ? dxpl_id : H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "can't create DAOS request")

    /* Set up chunk I/O task arrays if there is more than one chunk selected */
    if(nchunks_sel > 1) {
        /* Allocate arrays of tasks */
        if(NULL == (first_tasks = (tse_task_t **)DV_calloc(nchunks_sel * sizeof(tse_task_t *))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate IO first task array")
        if(NULL == (finalize_deps = (tse_task_t **)DV_calloc(nchunks_sel * sizeof(tse_task_t *))))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate IO task array")
    } /* end if */


    /* Perform I/O on each chunk selected */
    for(i = 0; i < nchunks_sel; i++) {
        daos_key_t  dkey;
        uint64_t    j;
        uint8_t    *p = dkey_buf;

        /* Encode dkey (chunk coordinates).  Prefix with '\0' to avoid accidental
         * collisions with other d-keys in this object. */
        *p++ = (uint8_t)'\0';
        for(j = 0; j < (uint64_t)ndims; j++)
            UINT64ENCODE(p, chunk_info[i].chunk_coords[j])

        /* Set up dkey */
        daos_iov_set(&dkey, dkey_buf, (daos_size_t)(1 + ((size_t)ndims * sizeof(chunk_info[i].chunk_coords[0]))));

        /* Get number of elements in selection */
        if((num_elem_file = H5Sget_select_npoints(chunk_info[i].fspace_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

        if(single_chunk_write_func(dset, &dkey, num_elem_file, mem_type_id,
                chunk_info[i].mspace_id, chunk_info[i].fspace_id, dxpl_id,
                IO_WRITE, (void *)buf, int_req, &first_tasks[ntasks],
                &finalize_deps[ntasks]) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "dataset write failed")
        if(finalize_deps[ntasks]) /* Remove this check once full async support is implemented or make it an assert DSINC */
            ntasks++;
    } /* end for */

done:
    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &dset->obj.item.file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependencies (if any) */
        else if(ntasks > 0 && 0 != (ret = tse_task_register_deps(finalize_task, ntasks, finalize_deps)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(ret_value < 0)
            int_req->status = H5_DAOS_SETUP_ERROR;

        /* Schedule first tasks */
        for(i = 0; (int)i < ntasks; i++) {
            assert(first_tasks[i]);
            if(0 != (ret = tse_task_schedule(first_tasks[i], false)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))
        } /* end for */

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(dset->obj.item.file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "dataset write failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request")
    } /* end if */

    /* Free memory */
    if(chunk_info) {
        if(close_spaces) {
            for(i = 0; i < nchunks_sel; i++) {
                if((chunk_info[i].mspace_id >= 0) && (H5Sclose(chunk_info[i].mspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close memory space");
                if((chunk_info[i].fspace_id >= 0) && (H5Sclose(chunk_info[i].fspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close file space");
            } /* end for */
        } /* end if */

        DV_free(chunk_info);
    } /* end if */
    if(first_tasks != &first_task) {
        assert(finalize_deps != &finalize_dep);
        DV_free(first_tasks);
        DV_free(finalize_deps);
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5_daos_dataset_write() */


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
H5_daos_dataset_get(void *_dset, H5VL_dataset_get_t get_type,
    hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req, va_list arguments)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    herr_t       ret_value = SUCCEED;    /* Return value */

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")

    switch (get_type) {
        case H5VL_DATASET_GET_DCPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's creation property list */
                if((*plist_id = H5Pcopy(dset->dcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset creation property list")

                /* Set dataset's object class on dcpl */
                if(H5_daos_set_oclass_from_oid(*plist_id, dset->obj.oid) < 0)
                    D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property")

                break;
            } /* end block */
        case H5VL_DATASET_GET_DAPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's access property list */
                if((*plist_id = H5Pcopy(dset->dapl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset access property list")

                break;
            } /* end block */
        case H5VL_DATASET_GET_SPACE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's dataspace */
                if((*ret_id = H5Scopy(dset->space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace ID of dataset");
                break;
            } /* end block */
        case H5VL_DATASET_GET_SPACE_STATUS:
            {
                H5D_space_status_t *allocation = va_arg(arguments, H5D_space_status_t *);

                /* Retrieve the dataset's space status */
                *allocation = H5D_SPACE_STATUS_NOT_ALLOCATED;
                break;
            } /* end block */
        case H5VL_DATASET_GET_TYPE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's datatype */
                if((*ret_id = H5Tcopy(dset->type_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype ID of dataset")
                break;
            } /* end block */
        case H5VL_DATASET_GET_STORAGE_SIZE:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from dataset")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
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
H5_daos_dataset_specific(void *_item, H5VL_dataset_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(H5I_DATASET != dset->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a dataset")

    switch (specific_type) {
        case H5VL_DATASET_SET_EXTENT:
            {
                const hsize_t *size = va_arg(arguments, const hsize_t *);

                if(!size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size parameter is NULL")

                if (H5D_CHUNKED != dset->dcpl_cache.layout)
                    D_GOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "dataset storage layout is not chunked")

                /* Call main routine */
                if(H5_daos_dataset_set_extent(dset, size, dxpl_id, req) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "failed to set dataset extent")

                break;
            } /* end block */

        case H5VL_DATASET_FLUSH:
            {
                if(H5_daos_dataset_flush(dset) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't flush dataset")

                break;
            } /* end block */

        case H5VL_DATASET_REFRESH:
            {
                /* Call main routine */
                if(H5_daos_dataset_refresh(dset, dxpl_id, req) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "failed to refresh dataset")

                break;
            } /* end block */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported dataset specific operation")
    }  /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_dataset_specific() */


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
H5_daos_dataset_close(void *_dset, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL")

    if(--dset->obj.item.rc == 0) {
        /* Free dataset data structures */
        if(dset->obj.item.open_req)
            if(H5_daos_req_free_int(dset->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request")
        if(!daos_handle_is_inval(dset->obj.obj_oh))
            if(0 != (ret = daos_obj_close(dset->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "can't close dataset DAOS object: %s", H5_daos_err_to_string(ret))
        if(dset->type_id != FAIL && H5Idec_ref(dset->type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's datatype")
        if(dset->file_type_id != FAIL && H5Idec_ref(dset->file_type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's file datatype")
        if(dset->space_id != FAIL && H5Idec_ref(dset->space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's dataspace")
        if(dset->dcpl_id != FAIL && H5Idec_ref(dset->dcpl_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dcpl")
        if(dset->dapl_id != FAIL && H5Idec_ref(dset->dapl_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dapl")
        if(dset->fill_val)
            dset->fill_val = DV_free(dset->fill_val);
        dset = H5FL_FREE(H5_daos_dset_t, dset);
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_dataset_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_flush
 *
 * Purpose:     Flushes a DAOS dataset.  Currently a no-op, may create a
 *              snapshot in the future.
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
H5_daos_dataset_flush(H5_daos_dset_t *dset)
{
    herr_t ret_value = SUCCEED;

    assert(dset);

    /* Nothing to do if no write intent */
    if(!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_DONE(SUCCEED);

    /* Progress scheduler until empty? DSINC */

done:
    D_FUNC_LEAVE
} /* end H5_daos_dataset_flush() */


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
H5_daos_dataset_refresh(H5_daos_dset_t *dset, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    uint8_t space_buf_static[H5_DAOS_SPACE_BUF_SIZE];
    uint8_t *space_buf_dyn = NULL;
    uint8_t *space_buf = space_buf_static;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(dset);

    /* Set up operation to read dataspace size from dataset */
    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_space_key_g, H5_daos_space_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Read dataspace size from dataset */
    if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, NULL,
            NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, FAIL, "can't read dataspace size from dataset: %s", H5_daos_err_to_string(ret))

    /* Check for metadata not found */
    if(iod.iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, FAIL, "dataspace not found")

    /* Allocate dataspace buffer if necessary */
    if(iod.iod_size > sizeof(space_buf_static)) {
        if(NULL == (space_buf_dyn = (uint8_t *)DV_malloc(iod.iod_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate dataspace buffer")
        space_buf = space_buf_dyn;
    } /* end if */

    /* Set up sgl */
    daos_iov_set(&sg_iov, space_buf, iod.iod_size);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read dataspace from dataset */
    if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, FAIL, "can't read metadata from dataset: %s", H5_daos_err_to_string(ret))

    /* Decode dataspace */
    if((dset->space_id = H5Sdecode(space_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, FAIL, "can't deserialize dataspace")

done:
    /* Free memory */
    space_buf_dyn = DV_free(space_buf_dyn);

    D_FUNC_LEAVE
} /* end H5_daos_dataset_refresh() */


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
H5_daos_dataset_set_extent(H5_daos_dset_t *dset, const hsize_t *size,
    hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    hsize_t maxdims[H5S_MAX_RANK];
    int ndims;
    void *space_buf = NULL;
    hbool_t collective;
    int i;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(dset);
    assert(size);

    /* Check for write access */
    if(!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /*
     * Like HDF5, all metadata writes are collective by default. Once independent
     * metadata writes are implemented, we will need to check for this property.
     */
    collective = TRUE;

    /* Get dataspace rank */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get current dataspace rank")

    /* Get dataspace max dims */
    if(H5Sget_simple_extent_dims(dset->space_id, NULL, maxdims) <0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get current dataspace maximum dimensions")

    /* Make sure max dims aren't exceeded */
    for(i = 0; i < ndims; i++)
        if((maxdims[i] != H5S_UNLIMITED) && (size[i] > maxdims[i]))
            D_GOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "requested dataset dimensions exceed maximum dimensions")

    /* Change dataspace extent */
    if(H5Sset_extent_simple(dset->space_id, ndims, size, maxdims) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set dataspace dimensions")

    /* Write new dataspace to dataset in file if this process should */
    if(!collective || (dset->obj.item.file->my_rank == 0)) {
        daos_key_t dkey;
        daos_iod_t iod;
        daos_sg_list_t sgl;
        daos_iov_t sg_iov;
        size_t space_size = 0;

        /* Encode dataspace */
        if(H5Sencode2(dset->space_id, NULL, &space_size, dset->obj.item.file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of dataspace")
        if(NULL == (space_buf = DV_malloc(space_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for serialized dataspace")
        if(H5Sencode2(dset->space_id, space_buf, &space_size, dset->obj.item.file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, FAIL, "can't serialize dataspace")

        /* Set up operation to write dataspace to dataset */
        /* Set up dkey */
        daos_iov_set(&dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)H5_daos_space_key_g, H5_daos_space_key_size_g);
        iod.iod_nr = 1u;
        iod.iod_size = (uint64_t)space_size;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, space_buf, (daos_size_t)space_size);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Write updated dataspace to dataset */
        if(0 != (ret = daos_obj_update(dset->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't write metadata to dataset: %s", H5_daos_err_to_string(ret))
    } /* end if */

done:
    /* Free memory */
    space_buf = DV_free(space_buf);

    D_FUNC_LEAVE
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
H5_daos_point_and_block(hid_t point_space, hsize_t rank, hsize_t *dims,
    hsize_t *start, hsize_t *block)
{
    hsize_t points_in[H5_DAOS_POINT_BUF_LEN];
    hsize_t points_out[H5_DAOS_POINT_BUF_LEN];
    const hsize_t npoints_buf = H5_DAOS_POINT_BUF_LEN / rank;
    hssize_t npoints_in_left;
    hsize_t npoints_in_done = 0;
    hsize_t npoints_in_this_iter;
    hsize_t npoints_out = 0;
    hsize_t npoints_out_this_iter;
    hid_t space_out = H5I_INVALID_HID;
    hsize_t i, j;
    hid_t ret_value = H5I_INVALID_HID;

    /* Get number of points */
    if((npoints_in_left = H5Sget_select_elem_npoints(point_space)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "can't get number of points in selection")

    /* Create output space */
    if((space_out = H5Screate_simple(rank, dims, NULL)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5I_INVALID_HID, "can't create output space")

    /* Loop until we've processed all points */
    while(npoints_in_left > 0) {
        /* Calculate number of points to fetch */
        npoints_in_this_iter = npoints_in_left > (hssize_t)npoints_buf ? npoints_buf : (hsize_t)npoints_in_left;

        /* Fetch points */
        if(H5Sget_select_elem_pointlist(point_space, npoints_in_done, npoints_in_this_iter, points_in) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "can't get points from selection")

        /* Add intersecting points to points_out */
        npoints_out_this_iter = 0;
        for(i = 0; i < npoints_in_this_iter; i++) {
            for(j = 0; (j < rank)
                    && (points_in[(i * rank) + j] >= start[j])
                    && (points_in[(i * rank) + j] < start[j] + block[j]); j++);
            if(j == rank) {
                memcpy(&points_out[npoints_out_this_iter * rank],
                        &points_in[i * rank], rank * sizeof(points_in[0]));
                npoints_out_this_iter++;
            } /* end if */
        } /* end for */

        /* Select elements in points_out */
        if(npoints_out_this_iter && H5Sselect_elements(space_out, npoints_out ? H5S_SELECT_APPEND : H5S_SELECT_SET, (size_t)npoints_out_this_iter, points_out) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, H5I_INVALID_HID, "can't select points")

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
    if(ret_value < 0)
        if(space_out >= 0 && H5Sclose(space_out) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, H5I_INVALID_HID, "can't close dataspace")

    D_FUNC_LEAVE
} /* end H5_daos_point_and_block() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_selected_chunk_info
 *
 * Purpose:     Calculates the starting coordinates for the chunks selected
 *              in the file space given by file_space_id and sets up
 *              individual memory and file spaces for each chunk. The chunk
 *              coordinates and dataspaces are returned through the
 *              chunk_info struct pointer.
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
H5_daos_get_selected_chunk_info(H5_daos_dcpl_cache_t *dcpl_cache,
    hid_t file_space_id, hid_t mem_space_id,
    H5_daos_select_chunk_info_t **chunk_info, size_t *chunk_info_len)
{
    H5_daos_select_chunk_info_t *_chunk_info = NULL;
    hssize_t  num_sel_points;
    hssize_t  chunk_file_space_adjust[H5O_LAYOUT_NDIMS];
    hsize_t   file_space_dims[H5S_MAX_RANK];
    hsize_t   *chunk_dims;
    hsize_t   curr_chunk_dims[H5S_MAX_RANK] = {0};
    hsize_t   file_sel_start[H5S_MAX_RANK], file_sel_end[H5S_MAX_RANK];
    hsize_t   mem_sel_start[H5S_MAX_RANK], mem_sel_end[H5S_MAX_RANK];
    hsize_t   start_coords[H5O_LAYOUT_NDIMS], end_coords[H5O_LAYOUT_NDIMS];
    hsize_t   selection_start_coords[H5O_LAYOUT_NDIMS] = {0};
    hsize_t   num_sel_points_cast;
    H5S_sel_type file_space_type;
    hbool_t   is_partial_edge_chunk = FALSE;
    htri_t    space_same_shape = FALSE;
    size_t    chunk_info_nalloc = 0;
    ssize_t   i = -1, j;
    hid_t     entire_chunk_sel_space_id = H5I_INVALID_HID;
    int       fspace_ndims, mspace_ndims;
    int       increment_dim;
    herr_t    ret_value = SUCCEED;

    assert(chunk_info);
    assert(chunk_info_len);

    if ((num_sel_points = H5Sget_select_npoints(file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "can't get number of points selected in file dataspace");
    /* H5_CHECKED_ASSIGN(num_sel_points_cast, hsize_t, num_sel_points, hssize_t); */
    num_sel_points_cast = (hsize_t) num_sel_points;

    if (num_sel_points == 0)
        D_GOTO_DONE(SUCCEED);

    /* Set convenience pointer to chunk dimensions */
    chunk_dims = dcpl_cache->chunk_dims;

    /* Get dataspace ranks */
    if ((fspace_ndims = H5Sget_simple_extent_ndims(file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file space dimensionality");
    if ((mspace_ndims = H5Sget_simple_extent_ndims(mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get memory space dimensionality");

    /* Check if the spaces are the same "shape".  For now, reject spaces that
     * have different ranks, until there's a public interface to
     * H5S_select_construct_projection().  See the note in H5D__read().  With
     * the use of H5Sselect_project_intersection() the performance penalty
     * should be much less than with the native library anyways. */
    if(fspace_ndims == mspace_ndims)
        if(FAIL == (space_same_shape = H5Sselect_shape_same(file_space_id, mem_space_id)))
            D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "can't determine if file and memory dataspaces are the same shape");

    if (H5Sget_simple_extent_dims(file_space_id, file_space_dims, NULL) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file dataspace dimensions")

    /* Get the bounding box for the current selection in the file and memory spaces */
    if (H5Sget_select_bounds(file_space_id, file_sel_start, file_sel_end) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get bounding box for file selection");
    if (H5Sget_select_bounds(mem_space_id, mem_sel_start, mem_sel_end) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get bounding box for memory selection");

    /* Get file selection type */
    if((file_space_type = H5Sget_select_type(file_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection type")

    if(space_same_shape) {
        /* Calculate the adjustment for the memory selection from the file selection */
        for (i = 0; i < (ssize_t)fspace_ndims; i++) {
            /* H5_CHECK_OVERFLOW(file_sel_start[i], hsize_t, hssize_t); */
            /* H5_CHECK_OVERFLOW(mem_sel_start[i], hsize_t, hssize_t); */
            chunk_file_space_adjust[i] = (hssize_t) file_sel_start[i] - (hssize_t) mem_sel_start[i];
        } /* end for */
    } /* end if */
    else {
        /* Create temporary dataspace to hold selection of entire chunk */
        if((entire_chunk_sel_space_id = H5Screate_simple(fspace_ndims, file_space_dims, NULL)) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't create entire chunk selection dataspace")
    } /* end else */

    if (NULL == (_chunk_info = (H5_daos_select_chunk_info_t *) DV_malloc(H5_DAOS_DEFAULT_NUM_SEL_CHUNKS * sizeof(*_chunk_info))))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate space for selected chunk info buffer");
    chunk_info_nalloc = H5_DAOS_DEFAULT_NUM_SEL_CHUNKS;

    /* Calculate the coordinates for the initial chunk */
    for (i = 0; i < (ssize_t)fspace_ndims; i++) {
        start_coords[i] = selection_start_coords[i] = (file_sel_start[i] / chunk_dims[i]) * chunk_dims[i];
        end_coords[i] = (start_coords[i] + chunk_dims[i]) - 1;
    } /* end for */

    /* Initialize the curr_chunk_dims array */
    memcpy(curr_chunk_dims, chunk_dims, (size_t)fspace_ndims * sizeof(hsize_t));

    /* Iterate through each "chunk" in the dataset */
    for(i = -1; ; ) {
        htri_t intersect = FALSE;

        /* Check for intersection of file selection and "chunk". If there is
         * an intersection, set up a valid memory and file space for the chunk. */
        if (file_space_type == H5S_SEL_ALL)
            intersect = TRUE;
        else
            if ((intersect = H5Sselect_intersect_block(file_space_id, start_coords, end_coords)) < 0)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "cannot determine chunk's intersection with the file dataspace");
        if (TRUE == intersect) {
            hssize_t chunk_space_adjust[H5O_LAYOUT_NDIMS];
            hssize_t chunk_sel_npoints;

            /* Advance index and re-allocate selected chunk info buffer if
             * necessary */
            if(++i == (ssize_t)chunk_info_nalloc) {
                if (NULL == (_chunk_info = (H5_daos_select_chunk_info_t *) DV_realloc(_chunk_info, 2 * chunk_info_nalloc * sizeof(*_chunk_info))))
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't reallocate space for selected chunk info buffer");
                chunk_info_nalloc *= 2;
            } /* end while */

            assert(i < (ssize_t)chunk_info_nalloc);

            /* Initialize dataspaces at this index */
            _chunk_info[i].fspace_id = H5I_INVALID_HID;
            _chunk_info[i].mspace_id = H5I_INVALID_HID;

            /*
             * Set up the file Dataspace for this chunk.
             */

            /* If this is a partial edge chunk, setup the partial edge chunk dimensions.
             * These will be used to adjust the selection within the edge chunk so that
             * it falls within the dataset's dataspace boundaries.  Also copy start
             * coords to hssize_t array.
             */
            for(j = 0; j < (ssize_t)fspace_ndims; j++) {
                if(start_coords[j] + chunk_dims[j] > file_space_dims[j]) {
                    curr_chunk_dims[j] = file_space_dims[j] - start_coords[j];
                    is_partial_edge_chunk = TRUE;
                } /* end if */
                chunk_space_adjust[j] = (hssize_t)start_coords[j];
            } /* end for */

            /* Check for point selection */
            if(file_space_type == H5S_SEL_POINTS) {
                /* Intersect points with block using connector routine */
                if((_chunk_info[i].fspace_id = H5_daos_point_and_block(file_space_id, fspace_ndims, chunk_dims, start_coords, curr_chunk_dims)) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't intersect point selection")
            } /* end if */
            else {
                /* Create temporary chunk for selection operations */
                if ((_chunk_info[i].fspace_id = H5Scopy(file_space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy file space");

                /* "AND" temporary chunk and current chunk */
                if (H5Sselect_hyperslab(_chunk_info[i].fspace_id, H5S_SELECT_AND, start_coords, NULL,
                        curr_chunk_dims, NULL) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't create temporary chunk selection");

                /* Resize chunk's dataspace dimensions to size of chunk */
                if (H5Sset_extent_simple(_chunk_info[i].fspace_id, fspace_ndims, chunk_dims, NULL) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk dimensions");
            } /* end else */

            /* Move selection back to have correct offset in chunk */
            if (H5Sselect_adjust(_chunk_info[i].fspace_id, chunk_space_adjust) < 0)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk selection");

            /* Copy the chunk's coordinates to the selected chunk info buffer */
            memcpy(_chunk_info[i].chunk_coords, start_coords, (size_t) fspace_ndims * sizeof(hsize_t));

            /*
             * Now set up the memory Dataspace for this chunk.
             */
            if (space_same_shape) {
                if ((_chunk_info[i].mspace_id = H5Scopy(mem_space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy memory space");

                /* Copy the chunk's file space selection to its memory space selection */
                if (H5Sselect_copy(_chunk_info[i].mspace_id, _chunk_info[i].fspace_id) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy selection from temporary chunk's file dataspace to its memory dataspace");

                /* Compute the adjustment for the chunk */
                for (j = 0; j < (ssize_t)fspace_ndims; j++) {
                    /* H5_CHECK_OVERFLOW(_chunk_info[i].chunk_coords[j], hsize_t, hssize_t); */
                    chunk_space_adjust[j] = chunk_file_space_adjust[j] - (hssize_t) _chunk_info[i].chunk_coords[j];
                } /* end for */

                /* Adjust the selection */
                if (H5Sselect_adjust(_chunk_info[i].mspace_id, chunk_space_adjust) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust temporary chunk's memory space selection");
            } /* end if */
            else {
                /* Select this chunk in the temporary chunk selection dataspace.
                 * Shouldn't matter if it goes beyond the extent since we're not
                 * doing I/O with this space */
                if(H5Sselect_hyperslab(entire_chunk_sel_space_id, H5S_SELECT_SET, start_coords, NULL, chunk_dims, NULL) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't select entire chunk")

                /* Calculate memory selection for this chunk by projecting
                 * intersection of full file selection and file chunk to full
                 * memory selection */
                if((_chunk_info[i].mspace_id = H5Sselect_project_intersection(file_space_id, mem_space_id, entire_chunk_sel_space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't project intersection")
            } /* end else */

            /* Determine if there are more chunks to process */
            if ((chunk_sel_npoints = H5Sget_select_npoints(_chunk_info[i].fspace_id)) < 0)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get number of points selected in chunk file space");

            /* Make sure we didn't process too many points */
            if((hsize_t)chunk_sel_npoints > num_sel_points_cast)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "processed more elements than present in selection")

            /* Keep track of the number of elements processed */
            num_sel_points_cast -= (hsize_t) chunk_sel_npoints;

            /* Break out if we're done */
            if(num_sel_points_cast == 0)
                break;

            /* Clean up after partial edge chunk */
            if(is_partial_edge_chunk) {
                memcpy(curr_chunk_dims, chunk_dims, (size_t)fspace_ndims * sizeof(hsize_t));
                is_partial_edge_chunk = FALSE;
            } /* end if */
        } /* end if */

        /* Set current increment dimension */
        increment_dim = fspace_ndims - 1;

        /* Increment chunk location in fastest changing dimension */
        /* H5_CHECK_OVERFLOW(chunk_dims[increment_dim], hsize_t, hssize_t); */
        start_coords[increment_dim] += chunk_dims[increment_dim];
        end_coords[increment_dim] += chunk_dims[increment_dim];

        /* Bring chunk location back into bounds, if necessary */
        if (start_coords[increment_dim] > file_sel_end[increment_dim]) {
            do {
                /* Reset current dimension's location to 0 */
                start_coords[increment_dim] = selection_start_coords[increment_dim];
                end_coords[increment_dim] = (start_coords[increment_dim] + chunk_dims[increment_dim]) - 1;

                /* Decrement current dimension */
                if(increment_dim == 0)
                    D_GOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "did not find enough elements to process or error traversing chunks")
                increment_dim--;

                /* Increment chunk location in current dimension */
                start_coords[increment_dim] += chunk_dims[increment_dim];
                end_coords[increment_dim] = (start_coords[increment_dim] + chunk_dims[increment_dim]) - 1;
            } while (start_coords[increment_dim] > file_sel_end[increment_dim]);
        } /* end if */
    } /* end for */

done:
    if (ret_value < 0) {
        if (_chunk_info) {
            for (j = 0; j <= i; j++) {
                if ((_chunk_info[j].fspace_id >= 0) && (H5Sclose(_chunk_info[j].fspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "failed to close chunk file dataspace ID")
                if ((_chunk_info[j].mspace_id >= 0) && (H5Sclose(_chunk_info[j].mspace_id) < 0))
                    D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "failed to close chunk memory dataspace ID")
            }

            DV_free(_chunk_info);
        }
    }
    else {
        *chunk_info = _chunk_info;
        assert(i + 1 >= 0);
        *chunk_info_len = (size_t)(i + 1);
    }

    if((entire_chunk_sel_space_id >= 0) &&  (H5Sclose(entire_chunk_sel_space_id) < 0))
        D_DONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "failed to close temporary entire chunk dataspace")

    D_FUNC_LEAVE
} /* end H5_daos_get_selected_chunk_info() */

