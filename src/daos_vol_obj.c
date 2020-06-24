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
 *          library. Generic object routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Task user data for opening a DAOS HDF5 object */
typedef struct H5_daos_object_open_ud_t {
    H5_daos_req_t *req;
    tse_task_t *open_metatask;
    void *loc_obj;
    hid_t lapl_id;
    daos_obj_id_t oid;
    hbool_t collective;
    H5I_type_t *obj_type_out;
    H5_daos_obj_t **obj_out;
} H5_daos_object_open_ud_t;

typedef struct H5_daos_oid_bcast_ud_t {
    H5_daos_mpi_ibcast_ud_t bcast_udata; /* Must be first */
    daos_obj_id_t *oid;
    uint8_t oid_buf[H5_DAOS_ENCODED_OID_SIZE];
} H5_daos_oid_bcast_ud_t;

/* Task user data for retrieving info about an object */
typedef struct H5_daos_object_get_info_ud_t {
    H5_daos_req_t *req;
    tse_task_t *get_info_metatask;
    H5_daos_obj_t *target_obj;
    H5O_info2_t *info_out;
    unsigned fields;
} H5_daos_object_get_info_ud_t;

/* Task user data for copying an object */
typedef struct H5_daos_object_copy_ud_t {
    H5_daos_req_t *req;
    tse_task_t *obj_copy_metatask;
    H5_daos_obj_t *src_obj;
    H5_daos_group_t *dst_grp;
    H5_daos_obj_t *copied_obj;
    char *new_obj_name;
    unsigned obj_copy_options;
    hid_t lcpl_id;
} H5_daos_object_copy_ud_t;

/* Task user data for copying data between
 * datasets during object copying.
 */
typedef struct H5_daos_dataset_copy_data_ud_t {
    H5_daos_req_t *req;
    H5_daos_dset_t *src_dset;
    H5_daos_dset_t *dst_dset;
    void *data_buf;
} H5_daos_dataset_copy_data_ud_t;

/* Task user data for copying attribute from a
 * source object to a target object.
 */
typedef struct H5_daos_object_copy_attributes_ud_t {
    H5_daos_req_t *req;
    H5_daos_obj_t *target_obj;
} H5_daos_object_copy_attributes_ud_t;

/* Task user data for copying a single attribute
 * from source object to a new target object.
 */
typedef struct H5_daos_object_copy_single_attribute_ud_t {
    H5_daos_req_t *req;
    H5_daos_attr_t *src_attr;
    H5_daos_attr_t *new_attr;
    H5_daos_obj_t *target_obj;
} H5_daos_object_copy_single_attribute_ud_t;

/* Task user data for visiting an object */
typedef struct H5_daos_object_visit_ud_t {
    H5_daos_req_t *req;
    tse_task_t *visit_metatask;
    H5_daos_iter_data_t iter_data;
    H5_daos_obj_t *target_obj;
    H5O_info2_t obj_info;
    hid_t target_obj_id;
} H5_daos_object_visit_ud_t;

/* Task user data for retrieving the number of attributes
 * attached to an object
 */
typedef struct H5_daos_object_get_num_attrs_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud; /* Must be first */
    hsize_t *num_attrs_out;
    uint8_t nattrs_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
    hbool_t post_decr;
    herr_t op_ret;
} H5_daos_object_get_num_attrs_ud_t;

/* Task user data for updating the attribute number tracking
 * akey for an object
 */
typedef struct H5_daos_object_update_num_attrs_key_ud_t {
    H5_daos_md_rw_cb_ud_t update_ud;
    hsize_t *new_nattrs;
    uint8_t nattrs_new_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
} H5_daos_object_update_num_attrs_key_ud_t;

/********************/
/* Local Prototypes */
/********************/

static int H5_daos_object_open_task(tse_task_t *task);
static herr_t H5_daos_object_get_oid_by_name(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
    tse_task_t **dep_task);
static herr_t H5_daos_object_get_oid_by_idx(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
    tse_task_t **dep_task);
static herr_t H5_daos_object_get_oid_by_token(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
    tse_task_t **dep_task);
static herr_t H5_daos_object_oid_bcast(H5_daos_file_t *file, daos_obj_id_t *oid,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_object_oid_bcast_prep_cb(tse_task_t *task, void *args);
static int H5_daos_object_oid_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info2_t *info,
    void *op_data, herr_t *op_ret, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info2_t *obj_info_out,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_object_get_info_task(tse_task_t *task);
static int H5_daos_get_num_attrs_prep_cb(tse_task_t *task, void *args);
static int H5_daos_get_num_attrs_comp_cb(tse_task_t *task, void *args);
static int H5_daos_object_update_num_attrs_key_prep_cb(tse_task_t *task, void *args);
static int H5_daos_object_update_num_attrs_key_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_object_copy_helper(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_loc_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, unsigned obj_copy_options, hid_t lcpl_id, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_object_copy_task(tse_task_t *task);
static herr_t H5_daos_object_copy_free_copy_udata(H5_daos_object_copy_ud_t *copy_udata,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_object_copy_free_copy_udata_task(tse_task_t *task);
static herr_t H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_object_copy_attributes_cb(hid_t location_id, const char *attr_name,
    const H5A_info_t H5VL_DAOS_UNUSED *ainfo, void *op_data, herr_t *op_ret,
    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_object_copy_single_attribute(H5_daos_obj_t *src_obj, const char *attr_name,
    H5_daos_obj_t *target_obj, tse_sched_t *sched, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_object_copy_single_attribute_task(tse_task_t *task);
static int H5_daos_object_copy_single_attribute_free_udata_task(tse_task_t *task);
static herr_t H5_daos_group_copy(H5_daos_object_copy_ud_t *obj_copy_udata, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static H5_daos_group_t *H5_daos_group_copy_helper(H5_daos_group_t *src_grp,
    H5_daos_group_t *dst_grp, const char *name, unsigned obj_copy_options,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_group_copy_cb(hid_t group, const char *name, const H5L_info2_t *info,
    void *op_data, herr_t *op_ret, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_datatype_copy(H5_daos_object_copy_ud_t *obj_copy_udata, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_dataset_copy(H5_daos_object_copy_ud_t *obj_copy_udata, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_dataset_copy_data(H5_daos_dset_t *src_dset, H5_daos_dset_t *dst_dset,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_dataset_copy_data_task(tse_task_t *task);
static int H5_daos_object_visit_task(tse_task_t *task);
static herr_t H5_daos_object_visit_free_visit_udata(H5_daos_object_visit_ud_t *visit_udata,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int H5_daos_object_visit_free_visit_udata_task(tse_task_t *task);



/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open
 *
 * Purpose:     Opens a DAOS HDF5 object.
 *
 *              NOTE: not meant to be called internally.
 *
 * Return:      Success:        object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_object_open(void *_item, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_req_t *int_req = NULL;
    H5_daos_obj_t *ret_obj = NULL;
    H5_daos_obj_t tmp_obj;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    hbool_t collective;
    int ret;
    void *ret_value = &tmp_obj; /* Initialize to non-NULL; NULL is used for error checking */

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "parent object is NULL");
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here. If not already set by the
     * file, we then check if collective access has been specified for the given LAPL.
     */
    collective = item->file->fapl_cache.is_collective_md_read;
    if(!collective) {
        hid_t lapl_id = H5P_LINK_ACCESS_DEFAULT;

        if(H5VL_OBJECT_BY_NAME == loc_params->type)
            lapl_id = loc_params->loc_data.loc_by_name.lapl_id;
        else if(H5VL_OBJECT_BY_IDX == loc_params->type)
            lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;

        if(H5P_LINK_ACCESS_DEFAULT != lapl_id)
            if(H5Pget_all_coll_metadata_ops(lapl_id, &collective) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, NULL, "can't get collective metadata reads property");
    } /* end if */

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(item->file, dxpl_id)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    if(H5_daos_object_open_helper(item, loc_params, opened_type,
            collective, &ret_obj, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, NULL, "can't open object");

    /* DSINC - special care will have to be taken with ret_value when explicit async is introduced */

done:
    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &int_req->finalize_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(ret_value == NULL)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(H5_daos_progress(&item->file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't progress scheduler");

        ret_value = ret_obj;

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, NULL, "object open operation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, NULL, "can't free request");
    } /* end if */

    /* Make sure we return something sensible if ret_value never got set */
    if(ret_value == &tmp_obj)
        ret_value = NULL;

    /* Clean up ret_obj if we had a failure and we're not returning ret_obj */
    if(NULL == ret_value && ret_obj && H5_daos_object_close(ret_obj, dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, NULL, "can't close object");

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_helper
 *
 * Purpose:     Internal-use helper routine to create an asynchronous task
 *              for opening a DAOS HDF5 object.
 *
 * Return:      Success:        object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_open_helper(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hbool_t collective, H5_daos_obj_t **ret_obj,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_open_ud_t *open_udata = NULL;
    tse_task_t *open_task = NULL;
    hbool_t open_task_scheduled = FALSE;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(ret_obj);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up user data for object open */
    if(NULL == (open_udata = (H5_daos_object_open_ud_t *)DV_malloc(sizeof(H5_daos_object_open_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for object open user data");
    open_udata->req = req;
    open_udata->collective = collective;
    open_udata->oid = (const daos_obj_id_t){0};
    open_udata->obj_type_out = opened_type;
    open_udata->obj_out = ret_obj;
    open_udata->open_metatask = NULL;

    if(item->type == H5I_FILE)
        open_udata->loc_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
    else
        open_udata->loc_obj = (H5_daos_obj_t *)item;

    open_udata->lapl_id = H5P_LINK_ACCESS_DEFAULT;
    if(H5VL_OBJECT_BY_NAME == loc_params->type) {
        if(H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_name.lapl_id)
            open_udata->lapl_id = H5Pcopy(loc_params->loc_data.loc_by_name.lapl_id);
    }
    else if(H5VL_OBJECT_BY_IDX == loc_params->type) {
        if(H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_idx.lapl_id)
            open_udata->lapl_id = H5Pcopy(loc_params->loc_data.loc_by_idx.lapl_id);
    }

    /* Retrieve the OID of the target object */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_NAME:
            if(H5_daos_object_get_oid_by_name((H5_daos_obj_t *)item, loc_params, &open_udata->oid,
                    collective, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve OID for object by object name");
            break;

        case H5VL_OBJECT_BY_IDX:
            if(H5_daos_object_get_oid_by_idx((H5_daos_obj_t *)item, loc_params, &open_udata->oid,
                    collective, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve OID for object by object index");
            break;

        case H5VL_OBJECT_BY_TOKEN:
            if(H5_daos_object_get_oid_by_token((H5_daos_obj_t *)item, loc_params, &open_udata->oid,
                    collective, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve OID for object by object token");
            break;

        case H5VL_OBJECT_BY_SELF:
        default:
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL, "invalid loc_params type");
    } /* end switch */

    /* Create task for object open */
    if(0 != (ret = tse_task_create(H5_daos_object_open_task, &item->file->sched, open_udata, &open_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to open object: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(open_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for object open task: %s", H5_daos_err_to_string(ret));

    /* Schedule object open task (or save it to be scheduled later) and give it
     * a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(open_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to open object: %s", H5_daos_err_to_string(ret));
    }
    else
        *first_task = open_task;

    open_task_scheduled = TRUE;

    /* Create meta task for object open. This empty task will be completed
     * when the actual asynchronous object open call is finished. This metatask
     * is necessary because the object open task will generate another async
     * task for actually opening the object once it has figured out the type
     * of the object. */
    if(0 != (ret = tse_task_create(H5_daos_metatask_autocomplete, &item->file->sched, NULL, &open_udata->open_metatask)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object open: %s", H5_daos_err_to_string(ret));

    /* Register dependency on object open task for metatask */
    if(0 != (ret = tse_task_register_deps(open_udata->open_metatask, 1, &open_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for object open metatask: %s", H5_daos_err_to_string(ret));

    /* Schedule meta task */
    assert(*first_task);
    if(0 != (ret = tse_task_schedule(open_udata->open_metatask, false)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object open: %s", H5_daos_err_to_string(ret));

    *dep_task = open_udata->open_metatask;
    req->rc++;
    ((H5_daos_item_t *)open_udata->loc_obj)->rc++;

    /* Relinquish control of the object open udata to the
     * task's completion callback */
    open_udata = NULL;

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        if(!open_task_scheduled) {
            open_udata = DV_free(open_udata);
        } /* end if */
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_open_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_task
 *
 * Purpose:     Asynchronous task to open a DAOS HDF5 object.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_open_task(tse_task_t *task)
{
    H5_daos_object_open_ud_t *udata;
    H5_daos_obj_t *obj = NULL;
    H5I_type_t obj_type = H5I_UNINIT;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    hid_t apl_id;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for object open task");

    assert(udata->req);
    assert(udata->loc_obj);
    assert(udata->obj_out);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    if(H5I_BADID == (obj_type = H5_daos_oid_to_type(udata->oid)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, -H5_DAOS_DAOS_GET_ERROR, "can't get object type");

    /* Call object's open function */
    apl_id = udata->lapl_id;
    if(obj_type == H5I_GROUP) {
        if(apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_GROUP_ACCESS_DEFAULT;

        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_group_open_helper(((H5_daos_item_t *)udata->loc_obj)->file,
                apl_id, udata->req, udata->collective, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open group");
    } /* end if */
    else if(obj_type == H5I_DATASET) {
        if(apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_DATASET_ACCESS_DEFAULT;

        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_dataset_open_helper(((H5_daos_item_t *)udata->loc_obj)->file,
                apl_id, udata->collective, udata->req, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open dataset");
    } /* end if */
    else if(obj_type == H5I_DATATYPE) {
        if(apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_DATATYPE_ACCESS_DEFAULT;

        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_datatype_open_helper(((H5_daos_item_t *)udata->loc_obj)->file,
                apl_id, udata->collective, udata->req, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open datatype");
    } /* end if */
    else {
        assert(obj_type == H5I_MAP);

        if(apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_MAP_ACCESS_DEFAULT;

        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_map_open_helper(((H5_daos_item_t *)udata->loc_obj)->file,
                apl_id, udata->collective, udata->req, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open map");
    } /* end else */

    /* Fill in object's OID */
    assert(obj);
    obj->oid = udata->oid;

    /* Register dependency on dep_task for object open metatask */
    if(dep_task && 0 != (ret = tse_task_register_deps(udata->open_metatask, 1, &dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for object open metatask: %s", H5_daos_err_to_string(ret));

    if(udata->obj_type_out)
        *udata->obj_type_out = obj_type;

    *udata->obj_out = obj;

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to open object: %s", H5_daos_err_to_string(ret));

    if(udata) {
        if(udata->loc_obj && H5_daos_object_close(udata->loc_obj, udata->req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTCLOSEOBJ, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");
        if(H5P_LINK_ACCESS_DEFAULT != udata->lapl_id && H5Pclose(udata->lapl_id) < 0)
            D_DONE_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, -H5_DAOS_H5_CLOSE_ERROR, "can't close link access plist");

        if(ret_value < 0) {
            if(obj && H5_daos_object_close(obj, udata->req->dxpl_id, NULL) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

            /* Handle errors in this function */
            /* Do not place any code that can issue errors after this block, except for
             * H5_daos_req_free_int, which updates req->status if it sees an error */
            if(ret_value != -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
                udata->req->status = ret_value;
                udata->req->failed_task = "object open";
            } /* end if */

            /* Complete metatask */
            tse_task_complete(udata->open_metatask, -H5_DAOS_SETUP_ERROR);
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_open_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_oid_by_name
 *
 * Purpose:     Helper function for H5_daos_object_open which retrieves the
 *              OID for an object according to the specified object name.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_oid_by_name(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_obj_t *target_obj = NULL;
    hbool_t leader_must_bcast = FALSE;
    char *path_buf = NULL;
    herr_t ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(oid_out);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(H5VL_OBJECT_BY_NAME == loc_params->type);

    /* Check for simple case of '.' for object name */
    if(!strncmp(loc_params->loc_data.loc_by_name.name, ".", 2)) {
        if(loc_obj->item.type == H5I_FILE)
            *oid_out = ((H5_daos_file_t *)loc_obj)->root_grp->obj.oid;
        else
            *oid_out = loc_obj->oid;

        D_GOTO_DONE(SUCCEED);
    } /* end if */

    /*
     * Check if we're actually retrieving the OID or just receiving it from
     * the leader.
     */
    if(!collective || (loc_obj->item.file->my_rank == 0)) {
        const char *target_name = NULL;
        size_t target_name_len;

        if(collective && (loc_obj->item.file->num_procs > 1))
            leader_must_bcast = TRUE;

        /* Traverse the path */
        if(NULL == (target_obj = H5_daos_group_traverse((H5_daos_item_t *)loc_obj, loc_params->loc_data.loc_by_name.name,
                H5P_LINK_CREATE_DEFAULT, req, collective, &path_buf, &target_name, &target_name_len, first_task, dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't traverse path");

        /* Check for no target_name, in this case just reopen target_obj */
        if(target_name[0] == '\0'
                || (target_name[0] == '.' && target_name[1] == '\0'))
            *oid_out = target_obj->oid;
        else {
            daos_obj_id_t **oid_ptr = NULL;

            /* Check type of target_obj */
            if(target_obj->item.type != H5I_GROUP)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "target object is not a group");

            /* Follow link to object */
            if(H5_daos_link_follow((H5_daos_group_t *)target_obj, target_name, target_name_len, FALSE,
                    req, &oid_ptr, NULL, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't follow link to object");

            /* Retarget *oid_ptr so H5_daos_link_follow fills in the object's oid */
            *oid_ptr = oid_out;
        } /* end else */
    } /* end if */

    /* Signify that all ranks will have called the bcast after this point */
    leader_must_bcast = FALSE;

    /* Broadcast OID if there are other processes that need it */
    if(collective && (loc_obj->item.file->num_procs > 1))
        if(H5_daos_object_oid_bcast(loc_obj->item.file, oid_out, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTSET, FAIL, "can't broadcast OID");

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        if(leader_must_bcast && (loc_obj->item.file->my_rank == 0)) {
            /*
             * Bcast oid as '0' if necessary - this will trigger
             * failures in other processes.
             */
            oid_out->lo = 0;
            oid_out->hi = 0;
            if(H5_daos_object_oid_bcast(loc_obj->item.file, oid_out, req, first_task, dep_task) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_MPI, FAIL, "can't broadcast empty object ID");
        } /* end if */
    } /* end if */

    /* Free path_buf if necessary */
    if(path_buf && H5_daos_free_async(loc_obj->item.file, path_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTFREE, FAIL, "can't free path buffer");

    /* Close target object */
    if(target_obj && H5_daos_object_close(target_obj, req->dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object");

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_oid_by_name() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_oid_by_idx
 *
 * Purpose:     Helper function for H5_daos_object_open which retrieves the
 *              OID for an object according to creation order or
 *              alphabetical order.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_oid_by_idx(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *oid_out, hbool_t H5VL_DAOS_UNUSED collective, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_group_t *container_group = NULL;
    daos_obj_id_t **oid_ptr;
    size_t link_name_size;
    char *path_buf = NULL;
    char *link_name = NULL;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    herr_t ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(oid_out);
    assert(H5VL_OBJECT_BY_IDX == loc_params->type);

    /* Open the group containing the target object */
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
    sub_loc_params.obj_type = H5I_GROUP;
    if(NULL == (container_group = (H5_daos_group_t *)H5_daos_group_open(loc_obj, &sub_loc_params,
            loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.lapl_id, req->dxpl_id, NULL)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open group containing target object");

    /* Retrieve the name of the link at the given index */
    link_name = link_name_buf_static;
    if(H5_daos_link_get_name_by_idx(container_group, loc_params->loc_data.loc_by_idx.idx_type,
            loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
            &link_name_size, link_name, H5_DAOS_LINK_NAME_BUF_SIZE, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get link name");

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(&loc_obj->item.file->sched, req, *first_task, *dep_task,
            H5E_OBJECT, H5E_CANTINIT, FAIL);

    /* Check that buffer was large enough to fit link name */
    if(link_name_size > H5_DAOS_LINK_NAME_BUF_SIZE - 1) {
        if(NULL == (link_name_buf_dyn = DV_malloc((size_t)link_name_size + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link name buffer");
        link_name = link_name_buf_dyn;

        /* Re-issue the call with a larger buffer */
        if(H5_daos_link_get_name_by_idx(container_group, loc_params->loc_data.loc_by_idx.idx_type,
                loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                &link_name_size, link_name, (size_t)link_name_size + 1, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get link name");

        H5_DAOS_WAIT_ON_ASYNC_CHAIN(&loc_obj->item.file->sched, req, *first_task, *dep_task,
                H5E_OBJECT, H5E_CANTINIT, FAIL);
    } /* end if */

    /* Attempt to follow the link */
    if(H5_daos_link_follow(container_group, link_name, link_name_size, FALSE,
            req, &oid_ptr, NULL, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't follow link to object");

    /* Retarget *oid_ptr so H5_daos_link_follow fills in the object's oid */
    *oid_ptr = oid_out;

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(&loc_obj->item.file->sched, req, *first_task, *dep_task,
            H5E_OBJECT, H5E_CANTINIT, FAIL);

done:
    if(link_name_buf_dyn)
        link_name_buf_dyn = DV_free(link_name_buf_dyn);
    if(container_group && H5_daos_group_close(container_group, req->dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");

    /* Free path_buf if necessary */
    if(path_buf && H5_daos_free_async(loc_obj->item.file, path_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "can't free path buffer");

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_oid_by_idx() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_oid_by_token
 *
 * Purpose:     Helper function for H5_daos_object_open which retrieves the
 *              OID for an object according to the specified object token.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_oid_by_token(H5_daos_obj_t H5VL_DAOS_UNUSED *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *oid_out, hbool_t H5VL_DAOS_UNUSED collective, H5_daos_req_t H5VL_DAOS_UNUSED *req,
    tse_task_t H5VL_DAOS_UNUSED **first_task, tse_task_t H5VL_DAOS_UNUSED **dep_task)
{
    daos_obj_id_t oid;
    herr_t ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(oid_out);
    assert(H5VL_OBJECT_BY_TOKEN == loc_params->type);

    /* Generate OID from object token */
    if(H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't convert address token to OID");

    *oid_out = oid;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_get_oid_by_token() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_oid_bcast
 *
 * Purpose:     Creates an asynchronous task for broadcasting an object's
 *              OID after it has been retrieved via a (possibly
 *              asynchronous) call to H5_daos_link_follow().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_oid_bcast(H5_daos_file_t *file, daos_obj_id_t *oid,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_oid_bcast_ud_t *oid_bcast_udata = NULL;
    tse_task_t *bcast_task = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(file);
    assert(oid);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if(NULL == (oid_bcast_udata = (H5_daos_oid_bcast_ud_t *)DV_malloc(sizeof(H5_daos_oid_bcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for MPI broadcast user data");
    oid_bcast_udata->bcast_udata.req = req;
    oid_bcast_udata->bcast_udata.obj = NULL;
    oid_bcast_udata->bcast_udata.bcast_metatask = NULL;
    oid_bcast_udata->bcast_udata.buffer = oid_bcast_udata->oid_buf;
    oid_bcast_udata->bcast_udata.buffer_len = H5_DAOS_ENCODED_OID_SIZE;
    oid_bcast_udata->bcast_udata.count = H5_DAOS_ENCODED_OID_SIZE;
    oid_bcast_udata->oid = oid;

    /* Create task for broadcast */
    if(0 != (ret = tse_task_create(H5_daos_mpi_ibcast_task, &file->sched, oid_bcast_udata, &bcast_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to broadcast OID: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(bcast_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for OID broadcast task: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for OID bcast */
    if(0 != (ret = tse_task_register_cbs(bcast_task, (file->my_rank == 0) ? H5_daos_object_oid_bcast_prep_cb : NULL,
            NULL, 0, H5_daos_object_oid_bcast_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't register callbacks for OID broadcast task: %s", H5_daos_err_to_string(ret));

    /* Schedule OID broadcast task (or save it to be scheduled later) and give it
     * a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(bcast_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to broadcast OID: %s", H5_daos_err_to_string(ret));
    }
    else
        *first_task = bcast_task;
    req->rc++;

    /* Relinquish control of the OID broadcast udata to the
     * task's completion callback */
    oid_bcast_udata = NULL;

    *dep_task = bcast_task;

done:
    /* Cleanup on failure */
    if(oid_bcast_udata) {
        assert(ret_value < 0);
        oid_bcast_udata = DV_free(oid_bcast_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_oid_bcast() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_oid_bcast_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous OID broadcasts.
 *              Currently checks for errors from previous tasks and then
 *              encodes the OID into the broadcast buffer before sending.
 *              Meant only to be called by rank 0.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_oid_bcast_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_oid_bcast_ud_t *udata;
    uint8_t *p;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for OID broadcast task");

    assert(udata->bcast_udata.req);
    assert(udata->bcast_udata.buffer);
    assert(udata->oid);
    assert(H5_DAOS_ENCODED_OID_SIZE == udata->bcast_udata.buffer_len);
    assert(H5_DAOS_ENCODED_OID_SIZE == udata->bcast_udata.count);

    /* Note that we do not handle errors from a previous task here.
     * The broadcast must still proceed on all ranks even if a
     * previous task has failed.
     */

    p = udata->bcast_udata.buffer;
    UINT64ENCODE(p, udata->oid->lo);
    UINT64ENCODE(p, udata->oid->hi);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_oid_bcast_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_oid_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous OID broadcasts.
 *              Currently checks for a failed task, decodes the sent OID
 *              buffer on the ranks that are receiving it and then frees
 *              private data. Meant to be called by all ranks.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_oid_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_oid_bcast_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for OID broadcast task");

    assert(udata->bcast_udata.req);
    assert(udata->bcast_udata.buffer);
    assert(udata->oid);
    assert(H5_DAOS_ENCODED_OID_SIZE == udata->bcast_udata.buffer_len);
    assert(H5_DAOS_ENCODED_OID_SIZE == udata->bcast_udata.count);

    /* Handle errors in OID broadcast task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast OID";
    } /* end if */
    else if(task->dt_result == 0) {
        /* Decode sent OID on receiving ranks */
        if(udata->bcast_udata.req->file->my_rank != 0) {
            uint8_t *p = udata->bcast_udata.buffer;
            UINT64DECODE(p, udata->oid->lo);
            UINT64DECODE(p, udata->oid->hi);

            /* Check for oid.lo set to 0 - indicates failure */
            if(udata->oid->lo == 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTSET, -H5_DAOS_REMOTE_ERROR, "lead process indicated OID broadcast failure");
        }
    } /* end else */

done:
    if(udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast OID completion callback";
        } /* end if */

        H5_daos_file_decref(udata->bcast_udata.req->file);

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_object_oid_bcast_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy
 *
 * Purpose:     Performs an object copy
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
H5_daos_object_copy(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_loc_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)src_loc_obj;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    unsigned obj_copy_options = 0;
    htri_t link_exists;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!src_loc_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source location object is NULL");
    if(!src_loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL");
    if(!src_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object name is NULL");
    if(!dst_loc_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination location object is NULL");
    if(!dst_loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL");
    if(!dst_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object name is NULL");
    if(H5VL_OBJECT_BY_SELF != src_loc_params->type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source location parameters type is invalid");
    if(H5VL_OBJECT_BY_SELF != dst_loc_params->type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination location parameters type is invalid");

    /* Retrieve the object copy options. The following flags are
     * currently supported:
     *
     * H5O_COPY_SHALLOW_HIERARCHY_FLAG
     * H5O_COPY_WITHOUT_ATTR_FLAG
     * H5O_COPY_EXPAND_SOFT_LINK_FLAG
     *
     * DSINC - The following flags are currently unsupported:
     *
     *   H5O_COPY_EXPAND_EXT_LINK_FLAG
     *   H5O_COPY_EXPAND_REFERENCE_FLAG
     *   H5O_COPY_MERGE_COMMITTED_DTYPE_FLAG
     */
    if(H5P_OBJECT_COPY_DEFAULT != ocpypl_id)
        if(H5Pget_copy_object(ocpypl_id, &obj_copy_options) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "failed to retrieve object copy options");

    /* Start H5 operation */
    /* Make work for cross file copies DSINC */
    if(NULL == (int_req = H5_daos_req_create(item->file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /*
     * First, ensure that the object doesn't currently exist at the specified destination
     * location object/destination name pair.
     */
    if((link_exists = H5_daos_link_exists((H5_daos_item_t *) dst_loc_obj, dst_name, int_req, &first_task, &dep_task)) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "couldn't determine if link exists");
    if(link_exists)
        D_GOTO_ERROR(H5E_OBJECT, H5E_ALREADYEXISTS, FAIL, "source object already exists at specified destination location object/destination name pair");

    /* Wait until everything is complete then check for errors
     * (temporary code until the rest of this function is async) */
    /* Needed because swapping between src and dst files causes issues */
    H5_DAOS_WAIT_ON_ASYNC_CHAIN(&item->file->sched, int_req, first_task, dep_task,
            H5E_OBJECT, H5E_CANTINIT, FAIL);

    /* Perform the object copy */
    if(H5_daos_object_copy_helper(src_loc_obj, src_loc_params, src_name,
            dst_loc_obj, dst_loc_params, dst_name, obj_copy_options, lcpl_id,
            &item->file->sched, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, FAIL, "failed to copy object");

done:
    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &int_req->finalize_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependencies (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first_task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule first task: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(H5_daos_progress(&item->file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL, "link creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_helper
 *
 * Purpose:     Internal-use routine to create an asynchronous task for
 *              copying an HDF5 DAOS object.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_helper(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_loc_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, unsigned obj_copy_options, hid_t lcpl_id, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_ud_t *obj_copy_udata = NULL;
    H5VL_loc_params_t sub_loc_params;
    tse_task_t *copy_task = NULL;
    const char *target_name = NULL;
    hbool_t copy_task_scheduled = FALSE;
    size_t target_name_len = 0;
    char *path_buf = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(src_loc_obj);
    assert(src_loc_params);
    assert(src_name);
    assert(dst_loc_obj);
    assert(dst_loc_params);
    assert(dst_name);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up user data for object copy */
    if(NULL == (obj_copy_udata = (H5_daos_object_copy_ud_t *)DV_malloc(sizeof(H5_daos_object_copy_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for object copy user data");
    obj_copy_udata->req = req;
    obj_copy_udata->obj_copy_metatask = NULL;
    obj_copy_udata->src_obj = NULL;
    obj_copy_udata->dst_grp = NULL;
    obj_copy_udata->copied_obj = NULL;
    obj_copy_udata->new_obj_name = NULL;
    obj_copy_udata->obj_copy_options = obj_copy_options;
    obj_copy_udata->lcpl_id = H5P_LINK_CREATE_DEFAULT;
    if(H5P_LINK_CREATE_DEFAULT != lcpl_id)
        if((obj_copy_udata->lcpl_id = H5Pcopy(lcpl_id)) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy LCPL");

    /* Open the source object */
    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.obj_type = src_loc_params->obj_type;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    sub_loc_params.loc_data.loc_by_name.name = src_name;
    if(H5_daos_object_open_helper(src_loc_obj, &sub_loc_params, NULL,
            TRUE, &obj_copy_udata->src_obj, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "failed to open source object");

    /* Traverse path to destination group */
    if(NULL == (obj_copy_udata->dst_grp = (H5_daos_group_t *)H5_daos_group_traverse(dst_loc_obj, dst_name,
            lcpl_id, req, TRUE, &path_buf, &target_name, &target_name_len, first_task, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't traverse path");

    /* Check type of target_obj */
    if(obj_copy_udata->dst_grp->obj.item.type != H5I_GROUP)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "target object is not a group");

    if(target_name_len == 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL, "can't copy new object with same name as destination group");

    /* Copy the name for the new object */
    if(NULL == (obj_copy_udata->new_obj_name = DV_malloc(target_name_len + 1)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for copy of destination object's name");
    strncpy(obj_copy_udata->new_obj_name, target_name, target_name_len + 1);
    obj_copy_udata->new_obj_name[target_name_len] = '\0';

    /* Create task for object copy */
    if(0 != (ret = tse_task_create(H5_daos_object_copy_task, sched, obj_copy_udata, &copy_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to copy object: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(copy_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for object copy task: %s", H5_daos_err_to_string(ret));

    /* Schedule object copy task (or save it to be scheduled later) and give it
     * a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(copy_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to copy object: %s", H5_daos_err_to_string(ret));
    }
    else
        *first_task = copy_task;
    req->rc++;

    copy_task_scheduled = TRUE;

    /* Create meta task for object copy. This task will be completed when the
     * actual asynchronous object copy call chain is finished. This metatask
     * is necessary because the object copy task will generate another async
     * task once the type of the source object is determined. That task might
     * also generate new async tasks depending on the object copy options used.
     */
    if(0 != (ret = tse_task_create(H5_daos_metatask_autocomplete, sched, NULL, &obj_copy_udata->obj_copy_metatask)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object copy: %s", H5_daos_err_to_string(ret));

    /* Register dependency on object copy task for metatask */
    if(0 != (ret = tse_task_register_deps(obj_copy_udata->obj_copy_metatask, 1, &copy_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for object copy metatask: %s", H5_daos_err_to_string(ret));

    /* Schedule meta task */
    assert(*first_task);
    if(0 != (ret = tse_task_schedule(obj_copy_udata->obj_copy_metatask, false)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object copy: %s", H5_daos_err_to_string(ret));

    *dep_task = obj_copy_udata->obj_copy_metatask;

    /* Create final task to free object copy udata after copying has finished */
    if(H5_daos_object_copy_free_copy_udata(obj_copy_udata, sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to free object copying data");

    /* Relinquish control of the object copy udata to task */
    obj_copy_udata = NULL;

done:
    /* Free path_buf if necessary */
    if(path_buf && H5_daos_free_async(req->file, path_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTFREE, FAIL, "can't free path buffer");

    /* Cleanup on failure */
    if(ret_value < 0) {
        if(obj_copy_udata->dst_grp && H5_daos_group_close(obj_copy_udata->dst_grp, req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");

        if(!copy_task_scheduled) {
            if(obj_copy_udata->new_obj_name)
                DV_free(obj_copy_udata->new_obj_name);

            obj_copy_udata = DV_free(obj_copy_udata);
        } /* end if */
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_task
 *
 * Purpose:     Asynchronous task to copy a DAOS HDF5 object.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_copy_task(tse_task_t *task)
{
    H5_daos_object_copy_ud_t *udata;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for object copy task");

    assert(udata->req);
    assert(udata->obj_copy_metatask);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Check if the source object was missing (for example, tried to open
     * the source object through a dangling soft link.
     */
    if(!udata->src_obj)
        D_GOTO_ERROR(H5E_OBJECT, H5E_NOTFOUND, -H5_DAOS_H5_OPEN_ERROR, "failed to open source object to copy");

    /* Determine object copying routine to call */
    switch(udata->src_obj->item.type) {
        case H5I_FILE:
        case H5I_GROUP:
            if(H5_daos_group_copy(udata, &udata->req->file->sched, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "can't copy group");
            break;
        case H5I_DATATYPE:
            if(H5_daos_datatype_copy(udata, &udata->req->file->sched, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "can't copy datatype");
            break;
        case H5I_DATASET:
            if(H5_daos_dataset_copy(udata, &udata->req->file->sched, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "can't copy dataset");
            break;
        case H5I_MAP:
            /* TODO: Add map copying support */
            D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR, "map copying is unsupported");
            break;
        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_NTYPES:
        default:
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, -H5_DAOS_BAD_VALUE, "invalid object type");
    } /* end switch */

    /* Register dependency on new object copying task for metatask */
    if(dep_task && 0 != (ret = tse_task_register_deps(udata->obj_copy_metatask, 1, &dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for object copy metatask: %s", H5_daos_err_to_string(ret));

    /* Relinquish control of the object copying udata to the new task. */
    udata = NULL;

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to copy object: %s", H5_daos_err_to_string(ret));

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_free_copy_udata
 *
 * Purpose:     Creates an asynchronous task for freeing private object
 *              copying data after the copying has completed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_free_copy_udata(H5_daos_object_copy_ud_t *copy_udata,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *free_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(copy_udata);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for freeing udata */
    if(0 != (ret = tse_task_create(H5_daos_object_copy_free_copy_udata_task, sched,
            copy_udata, &free_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to free object copying udata: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(free_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't register dependencies for task to free object copying udata: %s", H5_daos_err_to_string(ret));

    /* Schedule object copying udata free task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(free_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to free object copying udata: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = free_task;

    *dep_task = free_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_free_copy_udata() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_free_copy_udata_task
 *
 * Purpose:     Asynchronous task to free private object copying data
 *              after the copying has completed.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_copy_free_copy_udata_task(tse_task_t *task)
{
    H5_daos_object_copy_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for task to free object copying udata");

    assert(udata->req);

    /* Do not check for previous errors, as we must always free data */

done:
    if(udata) {
        /* Close source object that was opened during copying */
        if(udata->src_obj && H5_daos_object_close(udata->src_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Close destination group that was opened during copying */
        if(udata->dst_grp && H5_daos_group_close(udata->dst_grp, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

        /* Close copied object */
        if(udata->copied_obj && H5_daos_object_close(udata->copied_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close newly-copied object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "object copying udata free task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free copy of name for newly-copied object */
        if(udata->new_obj_name) {
            DV_free(udata->new_obj_name);
            udata->new_obj_name = NULL;
        }

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_free_copy_udata_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_attributes
 *
 * Purpose:     Helper routine to create an asynchronous task for copying
 *              all of the attributes from a given object to the specified
 *              destination object.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_attributes_ud_t *attr_copy_ud = NULL;
    H5_daos_iter_data_t iter_data;
    H5_index_t iter_index_type;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (attr_copy_ud = (H5_daos_object_copy_attributes_ud_t *)DV_malloc(sizeof(H5_daos_object_copy_attributes_ud_t))))
         D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for attribute copy task");
    attr_copy_ud->req = req;
    attr_copy_ud->target_obj = dst_obj;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether attribute creation order is tracked for the object.
     */
    iter_index_type = (src_obj->ocpl_cache.track_acorder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data. Attributes are re-created by creation order if possible */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, iter_index_type, H5_ITER_INC,
            FALSE, NULL, H5I_INVALID_HID, attr_copy_ud, NULL, req->dxpl_id, req, first_task, dep_task);
    iter_data.async_op = TRUE;
    iter_data.u.attr_iter_data.u.attr_iter_op_async = H5_daos_object_copy_attributes_cb;

    if(H5_daos_attribute_iterate(src_obj, &iter_data, sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "can't iterate over object's attributes");

    if(H5_daos_free_async(src_obj->item.file, attr_copy_ud, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to free attribute copying data");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_attributes() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_attributes_cb
 *
 * Purpose:     Attribute iteration callback to copy a single attribute
 *              from one DAOS object to another.
 *
 *              DSINC - currently no provision for dxpl_id or req.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_attributes_cb(hid_t location_id, const char *attr_name,
    const H5A_info_t H5VL_DAOS_UNUSED *ainfo, void *op_data, herr_t *op_ret,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_attributes_ud_t *copy_ud = (H5_daos_object_copy_attributes_ud_t *)op_data;
    H5_daos_obj_t *src_loc_obj = NULL;
    herr_t ret_value = H5_ITER_CONT;

    assert(copy_ud);

    if(NULL == (src_loc_obj = H5VLobject(location_id)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for source location ID");

    if(H5_daos_object_copy_single_attribute(src_loc_obj, attr_name, copy_ud->target_obj,
            &copy_ud->req->file->sched, copy_ud->req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, H5_ITER_ERROR, "can't create task to copy single attribute");

done:
    *op_ret = ret_value;

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_attributes_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_single_attribute
 *
 * Purpose:     Creates an asynchronous task to copy a single attribute
 *              to a target object during object copying.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_single_attribute(H5_daos_obj_t *src_obj, const char *attr_name,
    H5_daos_obj_t *target_obj, tse_sched_t *sched, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_single_attribute_ud_t *attr_copy_ud = NULL;
    H5VL_loc_params_t sub_loc_params;
    tse_task_t *copy_task = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(attr_name);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (attr_copy_ud = (H5_daos_object_copy_single_attribute_ud_t *)DV_malloc(sizeof(H5_daos_object_copy_single_attribute_ud_t))))
         D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for attribute copy task");
    attr_copy_ud->req = req;
    attr_copy_ud->src_attr = NULL;
    attr_copy_ud->target_obj = target_obj;

    sub_loc_params.obj_type = src_obj->item.type;
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;

    if(NULL == (attr_copy_ud->src_attr = H5_daos_attribute_open_helper((H5_daos_item_t *)src_obj, &sub_loc_params,
            attr_name, H5P_ATTRIBUTE_ACCESS_DEFAULT, req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "failed to open attribute");

    if(0 != (ret = tse_task_create(H5_daos_object_copy_single_attribute_task,
            sched, attr_copy_ud, &copy_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to copy attribute: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(copy_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create dependencies for attribute copy task: %s", H5_daos_err_to_string(ret));

    /* Schedule attribute copy task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(copy_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to copy attribute: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = copy_task;
    req->rc++;
    target_obj->item.rc++;
    *dep_task = copy_task;

    /* Relinquish control of attribute copy udata to task. */
    attr_copy_ud = NULL;

done:
    if(ret_value < 0) {
        DV_free(attr_copy_ud);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_single_attribute() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_single_attribute_task
 *
 * Purpose:     Asynchronous task to copy a single attribute to a target
 *              object during object copying.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_copy_single_attribute_task(tse_task_t *task)
{
    H5_daos_object_copy_single_attribute_ud_t *udata;
    H5VL_loc_params_t sub_loc_params;
    tse_task_t *free_task = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for attribute copying task");

    assert(udata->req);
    assert(udata->src_attr);
    assert(udata->target_obj);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Create task for creating new attribute now that the
     * source attribute should be valid.
     */
    sub_loc_params.obj_type = udata->target_obj->item.type;
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
    if(NULL == (udata->new_attr = H5_daos_attribute_create_helper((H5_daos_item_t *)udata->target_obj,
            &sub_loc_params, udata->src_attr->type_id, udata->src_attr->space_id,
            udata->src_attr->acpl_id, H5P_ATTRIBUTE_ACCESS_DEFAULT, udata->src_attr->name,
            TRUE, udata->req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "failed to create new attribute");

    /* Create task to free attribute copying udata after copying is finished */
    if(0 != (ret = tse_task_create(H5_daos_object_copy_single_attribute_free_udata_task,
            &udata->req->file->sched, udata, &free_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't create task to free attribute copying data: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(dep_task && 0 != (ret = tse_task_register_deps(free_task, 1, &dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't create dependencies for attribute copying data free task: %s", H5_daos_err_to_string(ret));

    /* Schedule attribute copying data free task (or save it to be scheduled later) and give it
     * a reference to req */
    assert(first_task);
    if(0 != (ret = tse_task_schedule(free_task, false)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't schedule task to free attribute copying data: %s", H5_daos_err_to_string(ret));

    dep_task = free_task;

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't schedule task to copy attribute: %s", H5_daos_err_to_string(ret));

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_single_attribute_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_single_attribute_free_udata_task
 *
 * Purpose:     Asynchronous task to free private attribute copying data
 *              after copying a single attribute.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_copy_single_attribute_free_udata_task(tse_task_t *task)
{
    H5_daos_object_copy_single_attribute_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for task to free attribute copying udata");

    assert(udata->src_attr);
    assert(udata->req);
    assert(udata->target_obj);

    /* Do not check for previous errors, as we must always free data */

done:
    if(udata) {
        /* Close source attribute */
        if(H5_daos_attribute_close(udata->src_attr, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Close newly-copied attribute */
        if(H5_daos_attribute_close(udata->new_attr, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Close object */
        if(H5_daos_object_close(udata->target_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "attribute copying udata free task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_single_attribute_free_udata_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy
 *
 * Purpose:     Helper routine to create asynchronous tasks for copying a
 *              group and its immediate members to the given location
 *              specified by the dst_obj/dst_name pair. The new group's
 *              name is specified by the base portion of dst_name.
 *
 *              Copying of certain parts of the group, such as its
 *              attributes, is controlled by the passed in object copy
 *              options.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_copy(H5_daos_object_copy_ud_t *obj_copy_udata, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_iter_data_t iter_data;
    H5_daos_group_t *src_grp;
    H5_index_t iter_index_type;
    hid_t target_obj_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(obj_copy_udata);
    assert(obj_copy_udata->req);
    assert(obj_copy_udata->obj_copy_metatask);
    assert(obj_copy_udata->src_obj);
    assert(obj_copy_udata->new_obj_name);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    src_grp = (H5_daos_group_t *)obj_copy_udata->src_obj;

    /* Copy the group */
    if(NULL == (obj_copy_udata->copied_obj = (H5_daos_obj_t *)H5_daos_group_copy_helper(src_grp,
            obj_copy_udata->dst_grp, obj_copy_udata->new_obj_name, obj_copy_udata->obj_copy_options,
            sched, req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "can't copy group");

    /* Now copy the immediate members of the group to the new group. If the
     * H5O_COPY_SHALLOW_HIERARCHY_FLAG flag wasn't specified, this will also
     * recursively copy the members of any subgroups found. */

    /* Register an ID for the group to iterate over */
    if((target_obj_id = H5VLwrap_register(obj_copy_udata->src_obj, H5I_GROUP)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
    obj_copy_udata->src_obj->item.rc++;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether creation order is tracked for the group.
     */
    iter_index_type = (src_grp->gcpl_cache.track_corder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, iter_index_type,
            H5_ITER_INC, FALSE, NULL, target_obj_id, obj_copy_udata, NULL, req->dxpl_id,
            req, first_task, dep_task);
    iter_data.async_op = TRUE;
    iter_data.u.link_iter_data.u.link_iter_op_async = H5_daos_group_copy_cb;

    if(H5_daos_link_iterate(src_grp, &iter_data, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over group's links");

done:
    /* Release reference to group since link iteration task should own it now */
    if(target_obj_id >= 0 && H5Idec_ref(target_obj_id) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object ID");

    D_FUNC_LEAVE;
} /* end H5_daos_group_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy_helper
 *
 * Purpose:     Helper routine to create an asynchronous task to do the
 *              actual group copying. This routine is needed to split the
 *              group copying logic away from the higher-level
 *              H5_daos_group_copy, which also copies the immediate members
 *              of a group during a shallow copy, or the entire hierarchy
 *              during a deep copy.
 *
 *              When a shallow group copy is being done, this routine is
 *              used on each of that group's sub-group members in turn to
 *              copy those sub-groups without their members. During a deep
 *              group copy, H5_daos_group_copy is used on each of that
 *              group's sub-group members in turn instead. This will
 *              recursively copy each of those members along with their own
 *              sub-group members.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static H5_daos_group_t *H5_daos_group_copy_helper(H5_daos_group_t *src_grp,
    H5_daos_group_t *dst_grp, const char *name, unsigned obj_copy_options,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_group_t *copied_group = NULL;
    H5_daos_group_t *ret_value = NULL;

    assert(src_grp);
    assert(dst_grp);
    assert(name);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Copy the group */
    if(NULL == (copied_group = H5_daos_group_create_helper(req->file,
            FALSE, src_grp->gcpl_id, src_grp->gapl_id, dst_grp, name,
            strlen(name), TRUE, req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "can't create new group");

    /* If the "without attribute copying" flag hasn't been specified,
     * create a task to copy the group's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *)src_grp, (H5_daos_obj_t *)copied_group,
                sched, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "can't copy group's attributes");

    ret_value = copied_group;

done:
    if(!ret_value && copied_group)
        if(H5_daos_group_close(copied_group, req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");

    D_FUNC_LEAVE;
} /* end H5_daos_group_copy_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy_cb
 *
 * Purpose:     Helper routine to deal with the copying of a single link
 *              during a group copy. Objects pointed to by hard links will
 *              be copied to the destination (copy) group.
 *
 *              When dealing with soft links, the following will happen:
 *
 *                  - If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was
 *                    specified as part of the object copy options for
 *                    H5Ocopy, soft links will be followed and the objects
 *                    they point to will become new objects in the
 *                    destination (copy) group.
 *                  - If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was not
 *                    specified, soft links will be directly copied to the
 *                    destination group and will have the same link value
 *                    as the original link.
 *
 *              DSINC - Expansion of external links is currently not
 *              supported and they will simply be directly copied to the
 *              destination group, similar to soft link copying when the
 *              H5O_COPY_EXPAND_SOFT_LINK_FLAG flag is not specified.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_copy_cb(hid_t group, const char *name, const H5L_info2_t *info,
    void *op_data, herr_t *op_ret, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_ud_t *obj_copy_udata = (H5_daos_object_copy_ud_t *)op_data;
    H5VL_loc_params_t sub_loc_params;
    H5_daos_group_t *copied_group = NULL;
    herr_t ret_value = H5_ITER_CONT;

    /* Silence compiler for unused parameter */
    (void)group;

    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name = name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    switch (info->type) {
        case H5L_TYPE_HARD:
        {
            daos_obj_id_t oid;
            H5I_type_t obj_type;

            /* Determine the type of object pointed to by the link */
            if(H5_daos_token_to_oid(&info->u.token, &oid) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTDECODE, H5_ITER_ERROR, "can't convert object token to OID");
            obj_type = H5_daos_oid_to_type(oid);
            if(H5I_BADID == obj_type || H5I_UNINIT == obj_type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, H5_ITER_ERROR, "invalid object type");

            /*
             * If performing a shallow group copy, copy the group without its immediate members.
             * Otherwise, continue on with a normal recursive object copy.
             */
            if((obj_type == H5I_GROUP) && (obj_copy_udata->obj_copy_options & H5O_COPY_SHALLOW_HIERARCHY_FLAG)) {
                if(NULL == (copied_group = H5_daos_group_copy_helper((H5_daos_group_t *)obj_copy_udata->src_obj,
                        (H5_daos_group_t *)obj_copy_udata->copied_obj, name, obj_copy_udata->obj_copy_options,
                        &obj_copy_udata->req->file->sched, obj_copy_udata->req, first_task, dep_task)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "failed to perform shallow copy of group");

                /* Close group now that copying task owns it */
                if(H5_daos_group_close(copied_group, obj_copy_udata->req->dxpl_id, NULL) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close group");
            }
            else {
                if(H5_daos_object_copy_helper(obj_copy_udata->src_obj, &sub_loc_params, name,
                        obj_copy_udata->copied_obj, &sub_loc_params, name, obj_copy_udata->obj_copy_options,
                        obj_copy_udata->lcpl_id, &obj_copy_udata->req->file->sched, obj_copy_udata->req,
                        first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy object");
            } /* end else */

            break;
        } /* H5L_TYPE_HARD */

        case H5L_TYPE_SOFT:
        {
            /*
             * If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was specified,
             * expand the soft link into a new object. Otherwise, the link
             * will be copied as-is.
             */
            if(obj_copy_udata->obj_copy_options & H5O_COPY_EXPAND_SOFT_LINK_FLAG) {
                /* Copy the object */
                if(H5_daos_object_copy_helper(obj_copy_udata->src_obj, &sub_loc_params, name,
                        obj_copy_udata->copied_obj, &sub_loc_params, name, obj_copy_udata->obj_copy_options,
                        obj_copy_udata->lcpl_id, &obj_copy_udata->req->file->sched, obj_copy_udata->req,
                        first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy object");
            } /* end if */
            else {
                /* Copy the link as is */
                if(H5_daos_link_copy_int((H5_daos_item_t *)obj_copy_udata->src_obj, &sub_loc_params,
                        (H5_daos_item_t *)obj_copy_udata->copied_obj, &sub_loc_params,
                        obj_copy_udata->lcpl_id, obj_copy_udata->req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy link");
            } /* end else */

            break;
        } /* H5L_TYPE_SOFT */

        case H5L_TYPE_EXTERNAL:
        {
            /*
             * If the H5O_COPY_EXPAND_EXT_LINK_FLAG flag was specified,
             * expand the external link into a new object. Otherwise, the
             * link will be copied as-is.
             */
            if(obj_copy_udata->obj_copy_options & H5O_COPY_EXPAND_EXT_LINK_FLAG) {
                D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, H5_ITER_ERROR, "H5O_COPY_EXPAND_EXT_LINK_FLAG flag is currently unsupported");
            } /* end if */
            else {
                /* Copy the link as is */
                if(H5_daos_link_copy_int((H5_daos_item_t *)obj_copy_udata->src_obj, &sub_loc_params,
                        (H5_daos_item_t *)obj_copy_udata->dst_grp, &sub_loc_params,
                        obj_copy_udata->lcpl_id, obj_copy_udata->req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy link");
            } /* end else */

            break;
        } /* H5L_TYPE_EXTERNAL */

        case H5L_TYPE_MAX:
        case H5L_TYPE_ERROR:
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, H5_ITER_ERROR, "invalid link type");
    } /* end switch */

done:
    if(ret_value < 0 && copied_group)
        if(H5_daos_group_close(copied_group, obj_copy_udata->req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close copied group");

    *op_ret = ret_value;

    D_FUNC_LEAVE;
} /* end H5_daos_group_copy_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_copy
 *
 * Purpose:     Helper routine to create asynchronous tasks for copying a
 *              committed datatype to the given location specified by the
 *              dst_obj/dst_name pair. The new committed datatype's name is
 *              specified by the base portion of dst_name.
 *
 *              Copying of certain parts of the committed datatype, such as
 *              its attributes, is controlled by the passed in object copy
 *              options.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_datatype_copy(H5_daos_object_copy_ud_t *obj_copy_udata, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_dtype_t *src_dtype;
    herr_t ret_value = SUCCEED;

    assert(obj_copy_udata);
    assert(obj_copy_udata->req);
    assert(obj_copy_udata->obj_copy_metatask);
    assert(obj_copy_udata->src_obj);
    assert(obj_copy_udata->new_obj_name);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    src_dtype = (H5_daos_dtype_t *)obj_copy_udata->src_obj;

    /* Copy the datatype */
    if(NULL == (obj_copy_udata->copied_obj = H5_daos_datatype_commit_helper(req->file,
            src_dtype->type_id, src_dtype->tcpl_id, src_dtype->tapl_id, obj_copy_udata->dst_grp,
            obj_copy_udata->new_obj_name, strlen(obj_copy_udata->new_obj_name), TRUE, req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't commit new datatype");

    /* If the "without attribute copying" flag hasn't been specified,
     * create a task to copy the datatype's attributes as well.
     */
    if((obj_copy_udata->obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *)src_dtype, obj_copy_udata->copied_obj,
                sched, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't copy datatype's attributes");

done:
    if(ret_value < 0 && obj_copy_udata->copied_obj)
        if(H5_daos_datatype_close(obj_copy_udata->copied_obj, obj_copy_udata->req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype");

    D_FUNC_LEAVE;
} /* end H5_daos_datatype_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_copy
 *
 * Purpose:     Helper routine to create asynchronous tasks for copying a
 *              dataset to the given location specified by the
 *              dst_obj/dst_name pair. The new dataset's name is specified
 *              by the base portion of dst_name.
 *
 *              Copying of certain parts of the dataset, such as its
 *              attributes, is controlled by the passed in object copy
 *              options.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_copy(H5_daos_object_copy_ud_t *obj_copy_udata, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_dset_t *src_dset;
    herr_t ret_value = SUCCEED;

    assert(obj_copy_udata);
    assert(obj_copy_udata->req);
    assert(obj_copy_udata->obj_copy_metatask);
    assert(obj_copy_udata->src_obj);
    assert(obj_copy_udata->new_obj_name);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    src_dset = (H5_daos_dset_t *)obj_copy_udata->src_obj;

    /* Copy the dataset */
    if(NULL == (obj_copy_udata->copied_obj = H5_daos_dataset_create_helper(req->file,
            src_dset->type_id, src_dset->space_id, src_dset->dcpl_id, src_dset->dapl_id,
            obj_copy_udata->dst_grp, obj_copy_udata->new_obj_name, strlen(obj_copy_udata->new_obj_name),
            TRUE, req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't create new dataset");

    /* Copy all data from the source dataset to the new dataset */
    if(H5_daos_dataset_copy_data(src_dset, (H5_daos_dset_t *)obj_copy_udata->copied_obj,
            sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataset data");

    /* If the "without attribute copying" flag hasn't been specified,
     * create a task to copy the dataset's attributes as well.
     */
    if((obj_copy_udata->obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *)src_dset, obj_copy_udata->copied_obj,
                sched, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataset's attributes");

done:
    if(ret_value < 0 && obj_copy_udata->copied_obj)
        if(H5_daos_dataset_close(obj_copy_udata->copied_obj, obj_copy_udata->req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset");

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_copy_data
 *
 * Purpose:     Creates an asynchronous task for copying data from a source
 *              dataset to a target dataset. Currently just attempts to
 *              read and write the data all at once.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_copy_data(H5_daos_dset_t *src_dset, H5_daos_dset_t *dst_dset,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_dataset_copy_data_ud_t *copy_ud = NULL;
    tse_task_t *copy_task = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(src_dset);
    assert(dst_dset);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (copy_ud = (H5_daos_dataset_copy_data_ud_t *)DV_malloc(sizeof(H5_daos_dataset_copy_data_ud_t))))
         D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for dataset data copy task");
    copy_ud->req = req;
    copy_ud->src_dset = src_dset;
    copy_ud->dst_dset = dst_dset;
    copy_ud->data_buf = NULL;

    /* Create task for dataset data copy */
    if(0 != (ret = tse_task_create(H5_daos_dataset_copy_data_task, sched, copy_ud, &copy_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to copy dataset data: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(copy_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dependencies for dataset data copy task: %s", H5_daos_err_to_string(ret));

    /* Schedule dataset data copy task (or save it to be scheduled later) and give it
     * a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(copy_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to copy dataset data: %s", H5_daos_err_to_string(ret));
    }
    else
        *first_task = copy_task;
    req->rc++;
    src_dset->obj.item.rc++;
    dst_dset->obj.item.rc++;

    *dep_task = copy_task;

done:
    if(ret_value < 0) {
        DV_free(copy_ud);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_copy_data() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_copy_data_task
 *
 * Purpose:     Asynchronous task for copying data from a source dataset to
 *              a target dataset. Currently just attempts to read and write
 *              the data all at once.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dataset_copy_data_task(tse_task_t *task)
{
    H5_daos_dataset_copy_data_ud_t *udata;
    hssize_t fspace_nelements = 0;
    size_t buf_size = 0;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for dataset data copy task");

    assert(udata->req);
    assert(udata->src_dset);
    assert(udata->dst_dset);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Calculate size needed to store entirety of dataset's data */
    if((fspace_nelements = H5Sget_simple_extent_npoints(udata->src_dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get number of elements in source dataset's dataspace");
    if(0 == (buf_size = H5Tget_size(udata->src_dset->type_id)))
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get source dataset's datatype size");
    buf_size *= fspace_nelements;

    if(NULL == (udata->data_buf = DV_malloc(buf_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't allocate data buffer for dataset data copy");

    if(H5_daos_dataset_read(udata->src_dset, udata->src_dset->type_id, H5S_ALL, H5S_ALL,
            udata->req->dxpl_id, udata->data_buf, NULL) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, -H5_DAOS_DAOS_GET_ERROR, "can't read data from source dataset");

    if(H5_daos_dataset_write(udata->dst_dset, udata->src_dset->type_id, H5S_ALL, H5S_ALL,
            udata->req->dxpl_id, udata->data_buf, NULL) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, -H5_DAOS_H5_COPY_ERROR, "can't write data to copied dataset");

done:
    if(udata) {
        if(H5_daos_dataset_close(udata->src_dset, udata->req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        if(H5_daos_dataset_close(udata->dst_dset, udata->req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "dataset data copy task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        if(udata->data_buf)
            DV_free(udata->data_buf);

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_copy_data_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get
 *
 * Purpose:     Performs an object "get" operation
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
H5_daos_object_get(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_object_get_t get_type, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req,
    va_list H5VL_DAOS_UNUSED arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *) _item;
    H5_daos_obj_t  *target_obj = NULL;
    H5_daos_req_t  *int_req = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task = NULL;
    int             ret;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL");

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(item->file, dxpl_id)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    switch (get_type) {
        case H5VL_OBJECT_GET_FILE:
        {
            void **ret_file = va_arg(arguments, void **);

            if(H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type");

            *ret_file = item->file;

            break;
        } /* H5VL_OBJECT_GET_FILE */

        case H5VL_OBJECT_GET_NAME:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported object get operation");
            break;

        case H5VL_OBJECT_GET_TYPE:
        {
            daos_obj_id_t oid;
            H5O_type_t *obj_type = va_arg(arguments, H5O_type_t *);
            H5I_type_t obj_itype;

            if(H5VL_OBJECT_BY_TOKEN != loc_params->type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type");

            /* Retrieve the OID of the referenced object and then determine the object's type */
            if(H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't convert object token to OID");
            if(H5I_BADID == (obj_itype = H5_daos_oid_to_type(oid)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get object type");

            switch(obj_itype) {
                case H5I_FILE:
                case H5I_GROUP:
                    *obj_type = H5O_TYPE_GROUP;
                    break;

                case H5I_DATATYPE:
                    *obj_type = H5O_TYPE_NAMED_DATATYPE;
                    break;

                case H5I_DATASET:
                    *obj_type = H5O_TYPE_DATASET;
                    break;

                case H5I_MAP:
                    *obj_type = H5O_TYPE_MAP;
                    break;

                case H5I_UNINIT:
                case H5I_BADID:
                case H5I_DATASPACE:
                case H5I_ATTR:
                case H5I_VFL:
                case H5I_VOL:
                case H5I_GENPROP_CLS:
                case H5I_GENPROP_LST:
                case H5I_ERROR_CLASS:
                case H5I_ERROR_MSG:
                case H5I_ERROR_STACK:
                case H5I_SPACE_SEL_ITER:
                case H5I_NTYPES:
                default:
                    D_GOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid object type");
            } /* end switch */

            break;
        } /* H5VL_OBJECT_GET_TYPE */

        /* H5Oget_info(_by_name|_by_idx)3 */
        case H5VL_OBJECT_GET_INFO:
        {
            H5O_info2_t *oinfo = va_arg(arguments, H5O_info2_t *);
            unsigned fields = va_arg(arguments, unsigned);

            /* Determine target object */
            switch (loc_params->type) {
                case H5VL_OBJECT_BY_SELF:
                    /* Use item as attribute parent object, or the root group if item is a file */
                    if(item->type == H5I_FILE)
                        target_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
                    else
                        target_obj = (H5_daos_obj_t *)item;

                    target_obj->item.rc++;
                    break;

                case H5VL_OBJECT_BY_NAME:
                case H5VL_OBJECT_BY_IDX:
                    /* Open target object */
                    if(H5_daos_object_open_helper(item, loc_params, NULL, TRUE, &target_obj,
                            int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open object");

                    H5_DAOS_WAIT_ON_ASYNC_CHAIN(&item->file->sched, int_req, first_task, dep_task,
                            H5E_OBJECT, H5E_CANTINIT, FAIL);

                    break;

                default:
                    D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type");
            } /* end switch */

            if(H5_daos_object_get_info(target_obj, fields, oinfo,
                    &item->file->sched, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve info for object");

            break;
        } /* H5VL_OBJECT_GET_INFO */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object get operation");
    } /* end switch */

done:
    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &int_req->finalize_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(H5_daos_progress(&item->file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL, "object get operation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
    } /* end if */

    if(target_obj) {
        if(H5_daos_object_close(target_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object");
        target_obj = NULL;
    } /* end else */

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_specific
 *
 * Purpose:     Performs an object "specific" operation
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
H5_daos_object_specific(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_obj_t *target_obj = NULL;
    char *path_buf = NULL;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    const char *oexists_obj_name = NULL;
    size_t oexists_obj_name_len = 0;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL");

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(item->file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Determine target object */
    if(loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as target object, or the root group if item is a file */
        if(item->type == H5I_FILE)
            target_obj = (H5_daos_obj_t *)item->file->root_grp;
        else
            target_obj = (H5_daos_obj_t *)item;
        target_obj->item.rc++;
    } /* end if */
    else if(loc_params->type == H5VL_OBJECT_BY_NAME) {
        /*
         * Open target_obj. For H5Oexists_by_name, we use H5_daos_group_traverse
         * as the path may point to a soft link that doesn't resolve. In that case,
         * H5_daos_object_open_helper would fail.
         */
        if(H5VL_OBJECT_EXISTS == specific_type) {
#ifdef H5_DAOS_USE_TRANSACTIONS
            /* Start transaction */
            if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't start transaction");
            int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

            /* Open group containing the link in question */
            if(NULL == (target_obj = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name,
                    H5P_LINK_CREATE_DEFAULT, int_req, FALSE, &path_buf, &oexists_obj_name, &oexists_obj_name_len, &first_task, &dep_task)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group");
        }
        else {
            if(H5_daos_object_open_helper(item, loc_params, NULL, TRUE,
                    &target_obj, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open target object");

            H5_DAOS_WAIT_ON_ASYNC_CHAIN(&item->file->sched, int_req, first_task, dep_task,
                    H5E_OBJECT, H5E_CANTINIT, FAIL);
        }
    } /* end else */
    else
        D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type");

    switch (specific_type) {
        /* H5Oincr_refcount/H5Odecr_refcount */
        case H5VL_OBJECT_CHANGE_REF_COUNT:
            D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL, "H5Oincr_refcount/H5Odecr_refcount are unsupported");
            break;

        /* H5Oexists_by_name */
        case H5VL_OBJECT_EXISTS:
        {
            daos_obj_id_t oid;
            daos_obj_id_t **oid_ptr = NULL;
            htri_t *oexists_ret = va_arg(arguments, htri_t *);
            hbool_t link_exists;

            /* Check type of target_obj */
            if(target_obj->item.type != H5I_GROUP)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "target object is not a group");

            /* Check if the link resolves */
            if(H5_daos_link_follow((H5_daos_group_t *)target_obj, oexists_obj_name,
                    oexists_obj_name_len, FALSE, int_req, &oid_ptr, &link_exists,
                    &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't follow link to object");

            /* Retarget *oid_ptr so H5_daos_link_follow fills in the oid */
            *oid_ptr = &oid;

            H5_DAOS_WAIT_ON_ASYNC_CHAIN(&item->file->sched, int_req, first_task, dep_task,
                    H5E_OBJECT, H5E_CANTINIT, FAIL);

            /* Set return value */
            *oexists_ret = (htri_t)link_exists;

            break;
        } /* H5VL_OBJECT_EXISTS */

        case H5VL_OBJECT_LOOKUP:
        {
            H5O_token_t *token = va_arg(arguments, H5O_token_t *);

            if(H5VL_OBJECT_BY_NAME != loc_params->type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL, "invalid loc_params type");

            if(H5_daos_oid_to_token(target_obj->oid, token) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTENCODE, FAIL, "can't convert OID to object token");

            break;
        } /* H5VL_OBJECT_LOOKUP */

        /* H5Ovisit(_by_name) */
        case H5VL_OBJECT_VISIT:
        {
            H5_daos_iter_data_t iter_data;
            H5_index_t idx_type = (H5_index_t) va_arg(arguments, int);
            H5_iter_order_t iter_order = (H5_iter_order_t) va_arg(arguments, int);
            H5O_iterate2_t iter_op = va_arg(arguments, H5O_iterate_t);
            void *op_data = va_arg(arguments, void *);
            unsigned fields = va_arg(arguments, unsigned);

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_OBJ, idx_type, iter_order,
                    FALSE, NULL, H5I_INVALID_HID, op_data, &ret_value, dxpl_id, int_req,
                    &first_task, &dep_task);
            iter_data.u.obj_iter_data.fields = fields;
            iter_data.u.obj_iter_data.u.obj_iter_op = iter_op;
            iter_data.u.obj_iter_data.obj_name = ".";

            if(H5_daos_object_visit(target_obj, &iter_data, &item->file->sched,
                    int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, FAIL, "object visiting failed");

            break;
        } /* H5VL_OBJECT_VISIT */

        /* H5Oflush */
        case H5VL_OBJECT_FLUSH:
        {
            switch(item->type) {
                case H5I_FILE:
                    if(H5_daos_file_flush((H5_daos_file_t *)item) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file");
                    break;
                case H5I_GROUP:
                    if(H5_daos_group_flush((H5_daos_group_t *)item) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't flush group");
                    break;
                case H5I_DATASET:
                    if(H5_daos_dataset_flush((H5_daos_dset_t *)item) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't flush dataset");
                    break;
                case H5I_DATATYPE:
                    if(H5_daos_datatype_flush((H5_daos_dtype_t *)item) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_WRITEERROR, FAIL, "can't flush datatype");
                    break;
                default:
                    D_GOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid object type");
            } /* end switch */

            break;
        } /* H5VL_OBJECT_FLUSH */

        /* H5Orefresh */
        case H5VL_OBJECT_REFRESH:
        {
            switch(item->type) {
                case H5I_FILE:
                    if(H5_daos_group_refresh(item->file->root_grp, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_READERROR, FAIL, "failed to refresh file");
                    break;
                case H5I_GROUP:
                    if(H5_daos_group_refresh((H5_daos_group_t *)item, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_READERROR, FAIL, "failed to refresh group");
                    break;
                case H5I_DATASET:
                    if(H5_daos_dataset_refresh((H5_daos_dset_t *)item, dxpl_id, int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "failed to refresh dataset");
                    break;
                case H5I_DATATYPE:
                    if(H5_daos_datatype_refresh((H5_daos_dtype_t *)item, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_READERROR, FAIL, "failed to refresh datatype");
                    break;
                default:
                    D_GOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid object type");
            } /* end switch */

            break;
        } /* H5VL_OBJECT_REFRESH */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object specific operation");
    } /* end switch */

done:
    if(int_req) {
        /* Free path_buf if necessary */
        if(path_buf && H5_daos_free_async(item->file, path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTFREE, FAIL, "can't free path buffer");

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &int_req->finalize_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(H5_daos_progress(&item->file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL, "object specific operation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
    } /* end if */

    if(target_obj) {
        if(H5_daos_object_close(target_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object");
        target_obj = NULL;
    } /* end else */

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_close
 *
 * Purpose:     Closes a DAOS HDF5 object.
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
H5_daos_object_close(void *_obj, hid_t dxpl_id, void **req)
{
    H5_daos_obj_t *obj = (H5_daos_obj_t *)_obj;
    herr_t ret_value = SUCCEED;

    if(!_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is NULL");

    /* Call type's close function */
    if(obj->item.type == H5I_GROUP) {
        if(H5_daos_group_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");
    } /* end if */
    else if(obj->item.type == H5I_DATASET) {
        if(H5_daos_dataset_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset");
    } /* end if */
    else if(obj->item.type == H5I_DATATYPE) {
        if(H5_daos_datatype_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype");
    } /* end if */
    else if(obj->item.type == H5I_ATTR) {
        if(H5_daos_attribute_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");
    } /* end if */
    else if(obj->item.type == H5I_MAP) {
        if(H5_daos_map_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map");
    } /* end if */
    else
        assert(0 && "Invalid object type");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_fill_ocpl_cache
 *
 * Purpose:     Fills the "ocpl_cache" field of the object struct, using
 *              the object's OCPL.  Assumes obj->ocpl_cache has been
 *              initialized to all zeros.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_fill_ocpl_cache(H5_daos_obj_t *obj, hid_t ocpl_id)
{
    unsigned acorder_flags;
    herr_t ret_value = SUCCEED;

    assert(obj);

    /* Determine if this object is tracking attribute creation order */
    if(H5Pget_attr_creation_order(ocpl_id, &acorder_flags) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't get attribute creation order flags");
    assert(!obj->ocpl_cache.track_acorder);
    if(acorder_flags & H5P_CRT_ORDER_TRACKED)
        obj->ocpl_cache.track_acorder = TRUE;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_fill_ocpl_cache() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit
 *
 * Purpose:     Helper routine to recursively visit the specified object
 *              and all objects accessible from the specified object,
 *              calling the supplied callback function on each object,
 *              when H5Ovisit(_by_name) is called.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_visit(H5_daos_obj_t *target_obj, H5_daos_iter_data_t *iter_data,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_visit_ud_t *visit_udata = NULL;
    tse_task_t *visit_task = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(iter_data);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (visit_udata = (H5_daos_object_visit_ud_t *)DV_malloc(sizeof(H5_daos_object_visit_ud_t))))
         D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for object operator callback function task");
    visit_udata->req = req;
    visit_udata->visit_metatask = NULL;
    visit_udata->target_obj = target_obj;
    visit_udata->iter_data = *iter_data;

    /* Retrieve the info of the target object */
    if(H5_daos_object_get_info(target_obj, iter_data->u.obj_iter_data.fields,
            &visit_udata->obj_info, sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get info for object");

    /* Create task to visit the specified target object first */
    if(0 != (ret = tse_task_create(H5_daos_object_visit_task, sched, visit_udata, &visit_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to call operator callback function: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(visit_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for operator callback function task: %s", H5_daos_err_to_string(ret));

    /* Schedule operator callback function task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(visit_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to call operator callback function: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = visit_task;
    req->rc++;
    target_obj->item.rc++;
    *dep_task = visit_task;

    /* Create meta task for object visiting. This empty task will be completed
     * when the actual asynchronous object visiting is finished. This metatask
     * is necessary because the initial visit task may generate other async
     * tasks for visiting the submembers of a group object. */
    if(0 != (ret = tse_task_create(H5_daos_metatask_autocomplete, sched, NULL, &visit_udata->visit_metatask)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object visiting: %s", H5_daos_err_to_string(ret));

    /* Register dependency on object visiting task for metatask */
    if(*dep_task && 0 != (ret = tse_task_register_deps(visit_udata->visit_metatask, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for object visiting metatask: %s", H5_daos_err_to_string(ret));

    /* Schedule meta task */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(visit_udata->visit_metatask, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object visiting: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = visit_udata->visit_metatask;

    *dep_task = visit_udata->visit_metatask;

    /* Create final task to free visiting udata after visiting has finished */
    visit_udata->target_obj->item.rc++;
    req->rc++;
    if(H5_daos_object_visit_free_visit_udata(visit_udata, sched, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to free object visiting data");

    /* Relinquish control of iteration op udata to task. */
    visit_udata = NULL;

done:
    if(ret_value < 0) {
        visit_udata = DV_free(visit_udata);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit_task
 *
 * Purpose:     Asynchronous task to call a user-supplied operator callback
 *              function during object iteration.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_visit_task(tse_task_t *task)
{
    H5_daos_object_visit_ud_t *udata;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for operator function function task");

    assert(udata->req);
    assert(udata->iter_data.u.obj_iter_data.u.obj_iter_op);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT || *udata->iter_data.op_ret_p < 0)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Determine if short-circuit success was returned in previous tasks */
    if(*udata->iter_data.op_ret_p > 0)
        D_GOTO_DONE(0);

    /* Register ID for object to be visited */
    if((udata->target_obj_id = H5VLwrap_register(udata->target_obj, udata->target_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, -H5_DAOS_SETUP_ERROR, "unable to atomize object handle");
    udata->target_obj->item.rc++;

    /* Make callback */
    if(udata->iter_data.async_op) {
        if(udata->iter_data.u.obj_iter_data.u.obj_iter_op_async(udata->target_obj_id,
                udata->iter_data.u.obj_iter_data.obj_name, &udata->obj_info, udata->iter_data.op_data,
                udata->iter_data.op_ret_p, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR, "operator function returned failure");
    }
    else {
        if((*udata->iter_data.op_ret_p = udata->iter_data.u.obj_iter_data.u.obj_iter_op(
                udata->target_obj_id, udata->iter_data.u.obj_iter_data.obj_name,
                &udata->obj_info, udata->iter_data.op_data)) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR, "operator function returned failure");
    }

    /* Check for short-circuit success */
    if(*udata->iter_data.op_ret_p)
        D_GOTO_DONE(0);

    /* If the object is a group, visit all objects below the group */
    if(H5I_GROUP == udata->target_obj->item.type) {
        H5_daos_iter_data_t sub_iter_data;

        assert(udata->visit_metatask);

        /*
         * Initialize the link iteration data with all of the fields from
         * the previous object iteration data, with the exception that the
         * link iteration data's is_recursive field is set to FALSE. The link
         * iteration data's op_data will be a pointer to the passed in
         * object iteration data so that the correct object iteration callback
         * operator function can be called for each link during H5_daos_link_iterate().
         */
        H5_DAOS_ITER_DATA_INIT(sub_iter_data, H5_DAOS_ITER_TYPE_LINK, udata->iter_data.index_type,
                udata->iter_data.iter_order, FALSE, udata->iter_data.idx_p, udata->target_obj_id,
                udata, NULL, udata->iter_data.dxpl_id, udata->req, udata->iter_data.first_task,
                udata->iter_data.dep_task);
        sub_iter_data.async_op = TRUE;
        sub_iter_data.u.link_iter_data.u.link_iter_op_async = H5_daos_object_visit_link_iter_cb;

        if(H5_daos_link_iterate((H5_daos_group_t *)udata->target_obj, &sub_iter_data,
                &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, -H5_DAOS_H5_ITER_ERROR, "failed to iterate through group's links");
    } /* end if */

    /* Register dependency on dep_task for new link iteration task */
    if(dep_task && 0 != (ret = tse_task_register_deps(udata->visit_metatask, 1, &dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for object visiting metatask: %s", H5_daos_err_to_string(ret));

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to iterate over object's links: %s", H5_daos_err_to_string(ret));

    if(udata) {
        /* Close object */
        if(H5_daos_object_close(udata->target_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "object visiting task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit_link_iter_cb
 *
 * Purpose:     Link iteration callback (H5L_iterate2_t) which is
 *              recursively called for each link in a group during a call
 *              to H5Ovisit(_by_name).
 *
 *              The callback expects to receive an H5_daos_iter_data_t
 *              which contains a pointer to the object iteration operator
 *              callback function (H5O_iterate2_t) to call on the object
 *              which each link points to.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info2_t *info,
    void *op_data, herr_t *op_ret, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_visit_ud_t *visit_udata = (H5_daos_object_visit_ud_t *)op_data;
    H5_daos_group_t *target_grp;
    H5_daos_obj_t *target_obj = NULL;
    hbool_t link_resolves = TRUE;
    int ret;
    herr_t ret_value = H5_ITER_CONT;

    assert(visit_udata);
    assert(H5_DAOS_ITER_TYPE_OBJ == visit_udata->iter_data.iter_type);

    if(NULL == (target_grp = (H5_daos_group_t *) H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for group ID");

    if(H5L_TYPE_SOFT == info->type) {
        /* Temporary hack - create and finalize req within this block.  req
         * should have wider scope once everything is async */
        H5_daos_req_t *int_req = NULL;
        daos_obj_id_t oid;
        daos_obj_id_t **oid_ptr = NULL;
        tse_task_t *first_task_l = NULL;
        tse_task_t *dep_task_l = NULL;

        /* Start H5 operation */
        if(NULL == (int_req = H5_daos_req_create(target_grp->obj.item.file, H5I_INVALID_HID)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, H5_ITER_ERROR, "can't create DAOS request");

        /* Check that the soft link resolves before opening the target object */
        if(H5_daos_link_follow(target_grp, name, strlen(name),
                FALSE, int_req, &oid_ptr, &link_resolves, &first_task_l, &dep_task_l) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_TRAVERSE, H5_ITER_ERROR, "can't follow link");

        /* Retarget *oid_ptr so H5_daos_link_follow fills in the oid */
        /* Use this to open object directly instead of traversing again? */
        *oid_ptr = &oid;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &target_grp->obj.item.file->sched, int_req, &int_req->finalize_task)))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        if(dep_task_l && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task_l)))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* finalize_task now owns a reference to req */
        int_req->rc++;

        /* Schedule first task */
        if(first_task_l && (0 != (ret = tse_task_schedule(first_task_l, false))))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));

        /* Block until operation completes */
        if(H5_daos_progress(&target_grp->obj.item.file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTOPERATE, H5_ITER_ERROR, "group open failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CLOSEERROR, H5_ITER_ERROR, "can't free request");
    } /* end if */

    if(link_resolves) {
        H5VL_loc_params_t loc_params;

        /* Open the target object */
        loc_params.type = H5VL_OBJECT_BY_NAME;
        loc_params.loc_data.loc_by_name.name = name;
        loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
        if(NULL == (target_obj = H5_daos_object_open(target_grp, &loc_params, NULL,
                visit_udata->iter_data.dxpl_id, NULL)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, H5_ITER_ERROR, "can't open object");

        visit_udata->iter_data.u.obj_iter_data.obj_name = name;
        if(H5_daos_object_visit(target_obj, &visit_udata->iter_data, &target_obj->item.file->sched,
                visit_udata->req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, H5_ITER_ERROR, "failed to visit object");

        /* Release reference to target object now that it is owned by object visiting task */
        if(H5_daos_object_close(target_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object");
    } /* end if */

done:
    *op_ret = ret_value;

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_link_iter_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit_free_visit_udata
 *
 * Purpose:     Creates an asynchronous task for freeing private object
 *              visiting data after the visiting has completed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_visit_free_visit_udata(H5_daos_object_visit_ud_t *visit_udata,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *free_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(visit_udata);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for freeing udata */
    if(0 != (ret = tse_task_create(H5_daos_object_visit_free_visit_udata_task, sched,
            visit_udata, &free_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to free object visiting udata: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(free_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't register dependencies for task to free object visiting udata: %s", H5_daos_err_to_string(ret));

    /* Schedule object visiting udata free task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(free_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to free object visiting udata: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = free_task;

    *dep_task = free_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_free_visit_udata() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit_free_visit_udata_task
 *
 * Purpose:     Asynchronous task to free private object visiting data
 *              after the visiting has completed.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_visit_free_visit_udata_task(tse_task_t *task)
{
    H5_daos_object_visit_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for task to free object visiting udata");

    assert(udata->req);
    assert(udata->target_obj);

    /* Do not check for previous errors, as we must always free data */

done:
    if(udata) {
        if(udata->target_obj_id >= 0 && H5Idec_ref(udata->target_obj_id) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object ID");

        /* Close object */
        if(H5_daos_object_close(udata->target_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "object visiting udata free task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_free_visit_udata_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_info
 *
 * Purpose:     Creates an asynchronous task for retrieving the info for
 *              an object.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info2_t *obj_info_out,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_get_info_ud_t *get_info_udata = NULL;
    tse_task_t *get_info_task;
    hbool_t get_info_task_scheduled = FALSE;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(obj_info_out);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if(NULL == (get_info_udata = (H5_daos_object_get_info_ud_t *)DV_malloc(sizeof(H5_daos_object_get_info_ud_t))))
         D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate user data struct for object info retrieval task");
    get_info_udata->req = req;
    get_info_udata->target_obj = target_obj;
    get_info_udata->fields = fields;
    get_info_udata->info_out = obj_info_out;
    get_info_udata->get_info_metatask = NULL;

    /* Create task for retrieving object info */
    if(0 != (ret = tse_task_create(H5_daos_object_get_info_task, sched, get_info_udata, &get_info_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to get object info: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task if present */
    if(*dep_task && 0 != (ret = tse_task_register_deps(get_info_task, 1, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't register dependencies for object info retrieval task: %s", H5_daos_err_to_string(ret));

    /* Schedule object info retrieval task (or save it to be scheduled later) and
     * give it a reference to req */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(get_info_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to get object info: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = get_info_task;
    req->rc++;
    target_obj->item.rc++;
    *dep_task = get_info_task;

    get_info_task_scheduled = TRUE;

    /* Create meta task for object info retrieval. This empty task will be completed
     * when the task for retrieving the object's info is finished. This metatask
     * is necessary because the object info retrieval task will generate another async
     * task for retrieving the number of attributes attached to the object.
     */
    if(0 != (ret = tse_task_create(H5_daos_metatask_autocomplete, sched, NULL, &get_info_udata->get_info_metatask)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object info retrieval: %s", H5_daos_err_to_string(ret));

    /* Register dependency on object info retrieval task for metatask */
    if(0 != (ret = tse_task_register_deps(get_info_udata->get_info_metatask, 1, &get_info_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create dependencies for object info retrieval metatask: %s", H5_daos_err_to_string(ret));

    /* Schedule meta task */
    assert(*first_task);
    if(0 != (ret = tse_task_schedule(get_info_udata->get_info_metatask, false)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object info retrieval: %s", H5_daos_err_to_string(ret));

    *dep_task = get_info_udata->get_info_metatask;

    /* Relinquish control of udata to task's function body */
    get_info_udata = NULL;

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        if(!get_info_task_scheduled) {
            get_info_udata = DV_free(get_info_udata);
        } /* end if */
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_info_task
 *
 * Purpose:     Asynchronous task for retrieving the info for an object.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_get_info_task(tse_task_t *task)
{
    H5_daos_object_get_info_ud_t *udata = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for object info retrieval task");

    assert(udata->req);
    assert(udata->target_obj);
    assert(udata->info_out);

    /* Check for previous errors */
    if(udata->req->status < -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    else if(udata->req->status == -H5_DAOS_SHORT_CIRCUIT)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /*
     * Initialize object info - most fields are not valid and will
     * simply be set to 0.
     */
    memset(udata->info_out, 0, sizeof(*udata->info_out));

    /* Basic fields */
    if(udata->fields & H5O_INFO_BASIC) {
        uint64_t fileno64;
        uint8_t *uuid_p = (uint8_t *)&udata->target_obj->item.file->uuid;

        /* Use the lower <sizeof(unsigned long)> bytes of the file uuid
         * as the fileno.  Ideally we would write separate 32 and 64 bit
         * hash functions but this should work almost as well. */
        UINT64DECODE(uuid_p, fileno64)
        udata->info_out->fileno = (unsigned long)fileno64;

        /* Get token */
        if(H5_daos_oid_to_token(udata->target_obj->oid, &udata->info_out->token) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get object token");

        /* Set object type */
        switch(udata->target_obj->item.type) {
            case H5I_GROUP:
                udata->info_out->type = H5O_TYPE_GROUP;
                break;
            case H5I_DATASET:
                udata->info_out->type = H5O_TYPE_DATASET;
                break;
            case H5I_DATATYPE:
                udata->info_out->type = H5O_TYPE_NAMED_DATATYPE;
                break;
            case H5I_MAP:
                udata->info_out->type = H5O_TYPE_MAP;
                break;
            default:
                udata->info_out->type = H5O_TYPE_UNKNOWN;
                break;
        } /* end switch */

        /* Reference count is always 1 - change this when
         * H5Lcreate_hard() is implemented DSINC */
        udata->info_out->rc = 1;
    } /* end if */

    /* Set the number of attributes. */
    if(udata->fields & H5O_INFO_NUM_ATTRS) {
        if(H5_daos_object_get_num_attrs(udata->target_obj, &udata->info_out->num_attrs, FALSE,
                NULL, NULL, &udata->req->file->sched, udata->req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, -H5_DAOS_SETUP_ERROR, "can't create task to retrieve the number of attributes attached to object");

        if(dep_task && 0 != (ret = tse_task_register_deps(udata->get_info_metatask, 1, &dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't create dependencies for task to retrieve number of attributes attached to object: %s", H5_daos_err_to_string(ret));
    } /* end if */

    /* Investigate collisions with links, etc DSINC */

done:
    /* Schedule first task */
    if(first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to retrieve number of attributes attached to object: %s", H5_daos_err_to_string(ret));

    /* Free private data if we haven't released ownership */
    if(udata) {
        if(H5_daos_object_close(udata->target_obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status = ret_value;
            udata->req->failed_task = "get object info task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_info_task() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_num_attrs
 *
 * Purpose:     Creates an asynchronous task for retrieving the number of
 *              attributes attached to a given object.
 *
 * Return       Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj, hsize_t *num_attrs,
    hbool_t post_decrement, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
    tse_sched_t *sched, H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_get_num_attrs_ud_t *get_num_attr_udata = NULL;
    tse_task_t *get_num_attrs_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(num_attrs);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    if(target_obj->ocpl_cache.track_acorder) {
        /* Allocate argument struct for fetch task */
        if(NULL == (get_num_attr_udata = (H5_daos_object_get_num_attrs_ud_t *)DV_calloc(sizeof(H5_daos_object_get_num_attrs_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for fetch callback arguments");
        get_num_attr_udata->num_attrs_out = num_attrs;
        get_num_attr_udata->post_decr = post_decrement;

        /* Set up main ud struct */
        get_num_attr_udata->md_rw_cb_ud.req = req;
        get_num_attr_udata->md_rw_cb_ud.obj = target_obj;

        /* Set up dkey */
        daos_iov_set(&get_num_attr_udata->md_rw_cb_ud.dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);
        get_num_attr_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod */
        daos_iov_set(&get_num_attr_udata->md_rw_cb_ud.iod[0].iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
        get_num_attr_udata->md_rw_cb_ud.iod[0].iod_nr = 1u;
        get_num_attr_udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
        get_num_attr_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        get_num_attr_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&get_num_attr_udata->md_rw_cb_ud.sg_iov[0], get_num_attr_udata->nattrs_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
        get_num_attr_udata->md_rw_cb_ud.sgl[0].sg_nr = 1;
        get_num_attr_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        get_num_attr_udata->md_rw_cb_ud.sgl[0].sg_iovs = &get_num_attr_udata->md_rw_cb_ud.sg_iov[0];

        get_num_attr_udata->md_rw_cb_ud.nr = 1u;

        get_num_attr_udata->md_rw_cb_ud.task_name = "attribute count retrieval task";

        /* Create task to fetch object's current number of attributes counter */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_FETCH, sched,
                *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, &get_num_attrs_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to get number of attributes attached to object: %s", H5_daos_err_to_string(ret));

        /* Set callback functions for task to read number of attributes */
        if(0 != (ret = tse_task_register_cbs(get_num_attrs_task,
                prep_cb ? prep_cb : H5_daos_get_num_attrs_prep_cb, NULL, 0,
                comp_cb ? comp_cb : H5_daos_get_num_attrs_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't register callbacks for attribute count read task: %s", H5_daos_err_to_string(ret));

        /* Set private data for attribute count read task */
        (void)tse_task_set_priv(get_num_attrs_task, get_num_attr_udata);

        /* Schedule task to read number of attributes attached to object (or save
         * it to be scheduled later) and give it a reference to req.
         */
        if(*first_task) {
            if(0 != (ret = tse_task_schedule(get_num_attrs_task, false)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to read number of attributes attached to object: %s", H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = get_num_attrs_task;
        req->rc++;
        target_obj->item.rc++;
        *dep_task = get_num_attrs_task;

        get_num_attr_udata = NULL;
    } /* end if */
    else {
        H5_daos_iter_data_t iter_data;

        /* Initialize iteration data */
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, H5_ITER_NATIVE,
                FALSE, NULL, H5I_INVALID_HID, num_attrs, NULL, H5P_DATASET_XFER_DEFAULT, req,
                first_task, dep_task);
        iter_data.u.attr_iter_data.u.attr_iter_op = H5_daos_attribute_iterate_count_attrs_cb;

        /* Retrieve the number of attributes attached to the object */
        if(H5_daos_attribute_iterate(target_obj, &iter_data, sched, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed");
    } /* end else */

done:
    if(ret_value < 0) {
        get_num_attr_udata = DV_free(get_num_attr_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_num_attrs() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_num_attrs_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous task to retrieve the
 *              number of attributes attached to an object. Currently
 *              checks for errors from previous tasks and then sets
 *              arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_get_num_attrs_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_object_get_num_attrs_ud_t *udata;
    daos_obj_rw_t *fetch_args;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for attribute count fetch task");

    assert(udata->md_rw_cb_ud.obj);
    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);
    assert(!udata->md_rw_cb_ud.req->file->closed);

    /* Handle errors */
    if(udata->md_rw_cb_ud.req->status < -H5_DAOS_SHORT_CIRCUIT) {
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->md_rw_cb_ud.req->status == -H5_DAOS_SHORT_CIRCUIT) {
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Set update task arguments */
    if(NULL == (fetch_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for metadata I/O task");
    fetch_args->oh = udata->md_rw_cb_ud.obj->obj_oh;
    fetch_args->th = DAOS_TX_NONE;
    fetch_args->flags = 0;
    fetch_args->dkey = &udata->md_rw_cb_ud.dkey;
    fetch_args->nr = udata->md_rw_cb_ud.nr;
    fetch_args->iods = udata->md_rw_cb_ud.iod;
    fetch_args->sgls = udata->md_rw_cb_ud.sgl;

done:
    if(ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_get_num_attrs_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_num_attrs_comp_cb
 *
 * Purpose:     Complete callback for asynchronous task to retrieve the
 *              number of attributes attached to an object. Currently
 *              checks for a failed task then decodes the number of
 *              attributes and frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_get_num_attrs_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_object_get_num_attrs_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for attribute count fetch task");

    assert(!udata->md_rw_cb_ud.req->file->closed);

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if(task->dt_result == 0) {
        /* Check for no num attributes found, in this case it must be 0 */
        if(udata->md_rw_cb_ud.iod[0].iod_size == (uint64_t)0)
            *udata->num_attrs_out = (hsize_t)0;
        else {
            uint64_t nattrs;
            uint8_t *p = udata->nattrs_buf;

            /* Decode num attributes */
            UINT64DECODE(p, nattrs);

            if(udata->post_decr && nattrs > 0)
                nattrs--;

            *udata->num_attrs_out = (hsize_t)nattrs;
        } /* end else */
    } /* end else */

done:
    if(udata) {
        if(H5_daos_object_close(udata->md_rw_cb_ud.obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "attribute count retrieval task completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_get_num_attrs_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_update_num_attrs_key
 *
 * Purpose:     Creates an asynchronous task to update the target object's
 *              attribute number tracking akey by setting its value to the
 *              specified value.
 *
 *              CAUTION: This routine is 'dangerous' in that the attribute
 *              number tracking akey is used in various places. Only call
 *              this routine if it is certain that the number of attributes
 *              attached to the target object has changed to the specified
 *              value.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_update_num_attrs_key(H5_daos_obj_t *target_obj, hsize_t *new_nattrs,
    tse_task_cb_t prep_cb, tse_task_cb_t comp_cb, tse_sched_t *sched,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_update_num_attrs_key_ud_t *update_udata = NULL;
    tse_task_t *update_task;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(new_nattrs);
    assert(sched);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    /* Allocate argument struct for update task */
    if(NULL == (update_udata = (H5_daos_object_update_num_attrs_key_ud_t *)DV_calloc(sizeof(H5_daos_object_update_num_attrs_key_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for update callback arguments");
    update_udata->new_nattrs = new_nattrs;
    update_udata->update_ud.req = req;
    update_udata->update_ud.obj = target_obj;

    /* Set up dkey */
    daos_iov_set(&update_udata->update_ud.dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);
    update_udata->update_ud.free_dkey = FALSE;

    /* Set up iod */
    memset(&update_udata->update_ud.iod[0], 0, sizeof(update_udata->update_ud.iod[0]));
    daos_iov_set(&update_udata->update_ud.iod[0].iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
    update_udata->update_ud.iod[0].iod_nr = 1u;
    update_udata->update_ud.iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
    update_udata->update_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    update_udata->update_ud.free_akeys = FALSE;

    /* Set up sgl */
    daos_iov_set(&update_udata->update_ud.sg_iov[0], update_udata->nattrs_new_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
    update_udata->update_ud.sgl[0].sg_nr = 1;
    update_udata->update_ud.sgl[0].sg_nr_out = 0;
    update_udata->update_ud.sgl[0].sg_iovs = &update_udata->update_ud.sg_iov[0];

    update_udata->update_ud.nr = 1u;

    update_udata->update_ud.task_name = "object attribute number tracking akey update";

    /* Create task for akey update */
    if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_UPDATE, sched, *dep_task ? 1 : 0,
            *dep_task ? dep_task : NULL, &update_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to update object's attribute number tracking akey: %s", H5_daos_err_to_string(ret));

    /* Set callback functions for akey update task*/
    if(0 != (ret = tse_task_register_cbs(update_task,
            prep_cb ? prep_cb : H5_daos_object_update_num_attrs_key_prep_cb, NULL, 0,
            comp_cb ? comp_cb : H5_daos_object_update_num_attrs_key_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't register callbacks for akey update task: %s", H5_daos_err_to_string(ret));

    /* Set private data for akey update task */
    (void)tse_task_set_priv(update_task, update_udata);

    /* Schedule akey update task (or save it to be scheduled later) */
    if(*first_task) {
        if(0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to update object's attribute number tracking akey: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = update_task;
    req->rc++;
    target_obj->item.rc++;
    *dep_task = update_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_update_num_attrs_key() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_update_num_attrs_key_prep_cb
 *
 * Purpose:     Prepare callback for async task to update the attribute
 *              number tracking akey for an object. Currently checks for
 *              errors from previous tasks, then encodes the new value into
 *              a buffer and sets arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_update_num_attrs_key_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_object_update_num_attrs_key_ud_t *udata;
    daos_obj_rw_t *update_args;
    uint8_t *p;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for object attribute number tracking akey update task");

    assert(udata->update_ud.obj);
    assert(udata->update_ud.req);
    assert(udata->update_ud.req->file);
    assert(!udata->update_ud.req->file->closed);

    /* Handle errors */
    if(udata->update_ud.req->status < -H5_DAOS_SHORT_CIRCUIT) {
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    } /* end if */
    else if(udata->update_ud.req->status == -H5_DAOS_SHORT_CIRCUIT) {
        udata = NULL;
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

    /* Encode new value into buffer */
    p = udata->nattrs_new_buf;
    UINT64ENCODE(p, (uint64_t)*udata->new_nattrs);

    /* Set update task arguments */
    if(NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get arguments for metadata I/O task");
    update_args->oh = udata->update_ud.obj->obj_oh;
    update_args->th = DAOS_TX_NONE;
    update_args->flags = 0;
    update_args->dkey = &udata->update_ud.dkey;
    update_args->nr = 1;
    update_args->iods = udata->update_ud.iod;
    update_args->sgls = udata->update_ud.sgl;
    update_args->maps = NULL;

done:
    if(ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_update_num_attrs_key_prep_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_update_num_attrs_key_comp_cb
 *
 * Purpose:     Complete callback for async task to update the attribute
 *              number tracking akey for an object. Currently checks for
 *              a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_update_num_attrs_key_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_object_update_num_attrs_key_ud_t *udata;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for object attribute number tracking akey update task");

    assert(!udata->update_ud.req->file->closed);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < -H5_DAOS_PRE_ERROR
            && udata->update_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->update_ud.req->status = task->dt_result;
        udata->update_ud.req->failed_task = udata->update_ud.task_name;
    } /* end if */

done:
    if(udata) {
        if(H5_daos_object_close(udata->update_ud.obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->update_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->update_ud.req->status = ret_value;
            udata->update_ud.req->failed_task = "completion callback for object attribute number tracking akey update task";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->update_ud.req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_object_update_num_attrs_key_comp_cb() */
