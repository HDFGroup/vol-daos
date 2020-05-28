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

/* Data passed to link iteration callback when performing a group copy */
typedef struct group_copy_op_data {
    H5_daos_group_t *new_group;
    unsigned object_copy_opts;
    hid_t lcpl_id;
    hid_t dxpl_id;
    H5_daos_req_t *req;
    tse_task_t **first_task;
    tse_task_t **dep_task;
} group_copy_op_data;

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
static herr_t H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info2_t *info, void *op_data);
static herr_t H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info_t *obj_info_out);
static herr_t H5_daos_object_copy_helper(H5_daos_obj_t *src_obj, H5I_type_t src_obj_type,
    H5_daos_group_t *dst_obj, const char *dst_name, unsigned obj_copy_options,
    hid_t lcpl_id, H5_daos_req_t *req, tse_task_t **first_task,
    tse_task_t **dep_task);
static herr_t H5_daos_group_copy(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task);
static H5_daos_group_t *H5_daos_group_copy_helper(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj,
    const char *dst_name, unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_datatype_copy(H5_daos_dtype_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_dataset_copy(H5_daos_dset_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
    hid_t dxpl_id, void **req);


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
    H5I_type_t *opened_type, hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
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
    if(NULL == (int_req = H5_daos_req_create(item->file, H5I_INVALID_HID)))
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
H5_daos_object_open_helper(void *_item, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hbool_t collective, H5_daos_obj_t **ret_obj,
    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_open_ud_t *open_udata = NULL;
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
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
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for object open metatask: %s", H5_daos_err_to_string(ret));

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
    if(udata->req->status < -H5_DAOS_INCOMPLETE)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);

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
            if(udata->req->status >= -H5_DAOS_INCOMPLETE) {
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
    ssize_t link_name_size;
    char *path_buf = NULL;
    char *link_name = NULL;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    int ret;
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
    if((link_name_size = H5_daos_link_get_name_by_idx(container_group, loc_params->loc_data.loc_by_idx.idx_type,
            loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
            link_name, H5_DAOS_LINK_NAME_BUF_SIZE, req, first_task, dep_task)) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get link name");

    /* Check that buffer was large enough to fit link name */
    if(link_name_size > H5_DAOS_LINK_NAME_BUF_SIZE - 1) {
        if(NULL == (link_name_buf_dyn = DV_malloc((size_t)link_name_size + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link name buffer");
        link_name = link_name_buf_dyn;

        /* Re-issue the call with a larger buffer */
        if(H5_daos_link_get_name_by_idx(container_group, loc_params->loc_data.loc_by_idx.idx_type,
                loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                link_name, (size_t)link_name_size + 1, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get link name");
    } /* end if */

    /* Attempt to follow the link */
    if(H5_daos_link_follow(container_group, link_name, link_name_size, FALSE,
            req, &oid_ptr, NULL, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't follow link to object");

    /* Retarget *oid_ptr so H5_daos_link_follow fills in the object's oid */
    *oid_ptr = oid_out;

    /* Wait until everything is complete then check for errors
     * (temporary code until the rest of this function is async) */
    if(*first_task && (0 != (ret = tse_task_schedule(*first_task, false))))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));
    if(H5_daos_progress(&loc_obj->item.file->sched, NULL, H5_DAOS_PROGRESS_WAIT) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");
    *first_task = NULL;
    *dep_task = NULL;
    if(req->status < -H5_DAOS_INCOMPLETE)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "asynchronous task failed");

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
            && udata->bcast_udata.req->status >= -H5_DAOS_INCOMPLETE) {
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
        if(ret_value < 0 && udata->bcast_udata.req->status >= -H5_DAOS_INCOMPLETE) {
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
H5_daos_object_copy(void *src_loc_obj, const H5VL_loc_params_t *loc_params1,
    const char *src_name, void *dst_loc_obj, const H5VL_loc_params_t *loc_params2,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t *src_obj = NULL;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    H5I_type_t src_obj_type;
    unsigned obj_copy_options;
    htri_t link_exists;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!src_loc_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source location object is NULL");
    if(!loc_params1)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL");
    if(!src_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object name is NULL");
    if(!dst_loc_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination location object is NULL");
    if(!loc_params2)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL");
    if(!dst_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object name is NULL");

    /* Start H5 operation */
    /* Make work for cross file copies DSINC */
    if(NULL == (int_req = H5_daos_req_create(((H5_daos_item_t *)src_loc_obj)->file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if(0 != (ret = daos_tx_open(((H5_daos_item_t *)src_loc_obj)->file->coh, &int_req->th, NULL /*event*/)))
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
    if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));
    if(H5_daos_progress(&((H5_daos_item_t *)dst_loc_obj)->file->sched, NULL, H5_DAOS_PROGRESS_WAIT) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");
    first_task = NULL;
    dep_task = NULL;
    if(int_req->status < -H5_DAOS_INCOMPLETE)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "asynchronous task failed");

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
    if(H5Pget_copy_object(ocpypl_id, &obj_copy_options) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "failed to retrieve object copy options");

    /*
     * Open the source object
     */
    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.obj_type = loc_params1->obj_type;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    sub_loc_params.loc_data.loc_by_name.name = src_name;
    if(NULL == (src_obj = H5_daos_object_open(src_loc_obj, &sub_loc_params, &src_obj_type, dxpl_id, req)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "failed to open source object");

    /* Perform the object copy */
    if(H5_daos_object_copy_helper(src_obj, src_obj_type, dst_loc_obj, dst_name, obj_copy_options,
            lcpl_id, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, FAIL, "failed to copy object");

done:
    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &((H5_daos_item_t *)src_loc_obj)->file->sched, int_req, &int_req->finalize_task)))
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
        if(H5_daos_progress(&((H5_daos_item_t *)src_loc_obj)->file->sched, int_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL, "link creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status));

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
    } /* end if */

    if(src_obj)
        if(H5_daos_object_close(src_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object");

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_helper
 *
 * Purpose:     Helper routine for H5_daos_object_copy that calls the
 *              appropriate copying routine based upon the object type of
 *              the object being copied. This routine separates out the
 *              copying logic so that recursive group copying can re-use
 *              it.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_helper(H5_daos_obj_t *src_obj, H5I_type_t src_obj_type,
    H5_daos_group_t *dst_obj, const char *dst_name, unsigned obj_copy_options,
    hid_t lcpl_id, H5_daos_req_t *req, tse_task_t **first_task,
    tse_task_t **dep_task)
{
    herr_t ret_value = SUCCEED;

    switch(src_obj_type) {
        case H5I_FILE:
        case H5I_GROUP:
            if(H5_daos_group_copy((H5_daos_group_t *) src_obj, (H5_daos_group_t *) dst_obj, dst_name,
                    obj_copy_options, lcpl_id, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "failed to copy group");
            break;
        case H5I_DATATYPE:
            if(H5_daos_datatype_copy((H5_daos_dtype_t *) src_obj, (H5_daos_group_t *) dst_obj, dst_name,
                    obj_copy_options, lcpl_id, req->dxpl_id, NULL) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "failed to copy datatype");
            break;
        case H5I_DATASET:
            if(H5_daos_dataset_copy((H5_daos_dset_t *) src_obj, (H5_daos_group_t *) dst_obj, dst_name,
                    obj_copy_options, lcpl_id, req->dxpl_id, NULL) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failed to copy dataset");
            break;
        case H5I_MAP:
            /* TODO: Add map copying support */
            D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, H5_ITER_ERROR, "map copying is unsupported");
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
            D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid object type");
    } /* end switch */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_helper() */


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
    H5VL_object_get_t get_type, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req, va_list H5VL_DAOS_UNUSED arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *) _item;
    H5_daos_obj_t *target_obj = NULL;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL");

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
                    if(NULL == (target_obj = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open object");
                    break;

                default:
                    D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type");
            } /* end switch */

            if(H5_daos_object_get_info(target_obj, fields, oinfo) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve info for object");

            break;
        } /* H5VL_OBJECT_GET_INFO */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object get operation");
    } /* end switch */

done:
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
    hid_t target_obj_id = H5I_INVALID_HID;
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
            if(NULL == (target_obj = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open target object");
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

            /* Wait until everything is complete then check for errors
             * (temporary code until the rest of this function is async) */
            if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret));
            if(H5_daos_progress(&item->file->sched, NULL, H5_DAOS_PROGRESS_WAIT) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");
            first_task = NULL;
            dep_task = NULL;
            if(int_req->status < -H5_DAOS_INCOMPLETE)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "asynchronous task failed");

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

            /* Register id for target_obj */
            if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_OBJ, idx_type, iter_order,
                    FALSE, NULL, target_obj_id, op_data, dxpl_id, int_req, &first_task,
                    &dep_task);
            iter_data.u.obj_iter_data.fields = fields;
            iter_data.u.obj_iter_data.obj_iter_op = iter_op;
            iter_data.u.obj_iter_data.obj_name = ".";

            if((ret_value = H5_daos_object_visit(target_obj, &iter_data)) < 0)
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

    if(target_obj_id >= 0) {
        if(H5Idec_ref(target_obj_id) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object ID");
        target_obj_id = H5I_INVALID_HID;
        target_obj = NULL;
    } /* end if */
    else if(target_obj) {
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
H5_daos_object_visit(H5_daos_obj_t *target_obj, H5_daos_iter_data_t *iter_data)
{
    H5O_info_t target_obj_info;
    herr_t op_ret = H5_ITER_CONT;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(iter_data);

    /* Retrieve the info of the target object */
    if(H5_daos_object_get_info(target_obj, iter_data->u.obj_iter_data.fields, &target_obj_info) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get info for object");

    /* Visit the specified target object first */
    if((op_ret = iter_data->u.obj_iter_data.obj_iter_op(iter_data->iter_root_obj, iter_data->u.obj_iter_data.obj_name, &target_obj_info, iter_data->op_data)) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, op_ret, "operator function returned failure");

    /* If the object is a group, visit all objects below the group */
    if(H5I_GROUP == target_obj->item.type) {
        H5_daos_iter_data_t sub_iter_data;

        /*
         * Initialize the link iteration data with all of the fields from
         * the passed in object iteration data, with the exception that the
         * link iteration data's is_recursive field is set to FALSE. The link
         * iteration data's op_data will be a pointer to the passed in
         * object iteration data so that the correct object iteration callback
         * operator function can be called for each link during H5_daos_link_iterate().
         */
        H5_DAOS_ITER_DATA_INIT(sub_iter_data, H5_DAOS_ITER_TYPE_LINK, iter_data->index_type, iter_data->iter_order,
                FALSE, iter_data->idx_p, iter_data->iter_root_obj, iter_data, iter_data->dxpl_id, iter_data->req,
                iter_data->first_task, iter_data->dep_task);
        sub_iter_data.u.link_iter_data.link_iter_op = H5_daos_object_visit_link_iter_cb;

        if(H5_daos_link_iterate((H5_daos_group_t *) target_obj, &sub_iter_data) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "failed to iterate through group's links");
    } /* end if */

    ret_value = op_ret;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_visit() */


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
    void *op_data)
{
    H5_daos_iter_data_t *iter_data = (H5_daos_iter_data_t *)op_data;
    H5_daos_group_t *target_grp;
    H5_daos_obj_t *target_obj = NULL;
    hbool_t link_resolves = TRUE;
    int ret;
    herr_t ret_value = H5_ITER_CONT;

    assert(iter_data);
    assert(H5_DAOS_ITER_TYPE_OBJ == iter_data->iter_type);

    if(NULL == (target_grp = (H5_daos_group_t *) H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for group ID");

    if(H5L_TYPE_SOFT == info->type) {
        /* Temporary hack - create and finalize req within this block.  req
         * should have wider scope once everything is async */
        H5_daos_req_t *int_req = NULL;
        tse_task_t *first_task = NULL;
        tse_task_t *dep_task = NULL;
        daos_obj_id_t oid;
        daos_obj_id_t **oid_ptr = NULL;

        /* Start H5 operation */
        if(NULL == (int_req = H5_daos_req_create(target_grp->obj.item.file, H5I_INVALID_HID)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, H5_ITER_ERROR, "can't create DAOS request");

        /* Check that the soft link resolves before opening the target object */
        if(H5_daos_link_follow(target_grp, name, strlen(name),
                FALSE, int_req, &oid_ptr, &link_resolves, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_TRAVERSE, H5_ITER_ERROR, "can't follow link");

        /* Retarget *oid_ptr so H5_daos_link_follow fills in the oid */
        /* Use this to open object directly instead of traversing again? */
        *oid_ptr = &oid;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &target_grp->obj.item.file->sched, int_req, &int_req->finalize_task)))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Register dependency (if any) */
        if(dep_task && 0 != (ret = tse_task_register_deps(int_req->finalize_task, 1, &dep_task)))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* Schedule finalize task */
        if(0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, H5_ITER_ERROR, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        /* finalize_task now owns a reference to req */
        int_req->rc++;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
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
        if(NULL == (target_obj = H5_daos_object_open(target_grp, &loc_params, NULL, iter_data->dxpl_id, NULL)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, H5_ITER_ERROR, "can't open object");

        iter_data->u.obj_iter_data.obj_name = name;
        if(H5_daos_object_visit(target_obj, iter_data) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, H5_ITER_ERROR, "failed to visit object");

        if(H5_daos_object_close(target_obj, iter_data->dxpl_id, NULL) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object");
        target_obj = NULL;
    } /* end if */

done:
    if(target_obj) {
        if(H5_daos_object_close(target_obj, iter_data->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object");
        target_obj = NULL;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_link_iter_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_info
 *
 * Purpose:     Helper routine to retrieve the info for an object when
 *              H5Oget_info(_by_name/_by_idx) is called.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info2_t *obj_info_out)
{
    hssize_t num_attrs = 0;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(obj_info_out);

    /*
     * Initialize object info - most fields are not valid and will
     * simply be set to 0.
     */
    memset(obj_info_out, 0, sizeof(*obj_info_out));

    /* Fill in fields of object info */

    /* Basic fields */
    if(fields & H5O_INFO_BASIC) {
        uint64_t fileno64;
        uint8_t *uuid_p = (uint8_t *)&target_obj->item.file->uuid;

        /* Use the lower <sizeof(unsigned long)> bytes of the file uuid
         * as the fileno.  Ideally we would write separate 32 and 64 bit
         * hash functions but this should work almost as well. */
        UINT64DECODE(uuid_p, fileno64)
        obj_info_out->fileno = (unsigned long)fileno64;

        /* Get token */
        if(H5_daos_oid_to_token(target_obj->oid, &obj_info_out->token) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get object token");

        /* Set object type */
        switch(target_obj->item.type) {
            case H5I_GROUP:
                obj_info_out->type = H5O_TYPE_GROUP;
                break;
            case H5I_DATASET:
                obj_info_out->type = H5O_TYPE_DATASET;
                break;
            case H5I_DATATYPE:
                obj_info_out->type = H5O_TYPE_NAMED_DATATYPE;
                break;
            case H5I_MAP:
                obj_info_out->type = H5O_TYPE_MAP;
                break;
            default:
                obj_info_out->type = H5O_TYPE_UNKNOWN;
                break;
        }

        /* Reference count is always 1 - change this when
         * H5Lcreate_hard() is implemented DSINC */
        obj_info_out->rc = 1;
    } /* end if */

    /* Set the number of attributes. */
    if(fields & H5O_INFO_NUM_ATTRS) {
        if((num_attrs = H5_daos_object_get_num_attrs(target_obj)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve the number of attributes attached to object");
        obj_info_out->num_attrs = (hsize_t)num_attrs;
    } /* end if */

    /* Investigate collisions with links, etc DSINC */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_num_attrs
 *
 * Purpose:     Helper routine to retrieve the number of attributes
 *              attached to a given object.
 *
 * Return:      Success:        The number of attributes attached to the
 *                              given object.
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
hssize_t
H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj)
{
    uint64_t nattrs = 0;
    hid_t target_obj_id = -1;
    hssize_t ret_value = 0;

    assert(target_obj);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    if(target_obj->ocpl_cache.track_acorder) {
        daos_sg_list_t sgl;
        daos_key_t dkey;
        daos_iod_t iod;
        daos_iov_t sg_iov;
        uint8_t *p;
        uint8_t nattrs_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
        int ret;

        /* Read the "number of attributes" key from the target object */

        /* Set up dkey */
        daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
        iod.iod_nr = 1u;
        iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, nattrs_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Read number of attributes */
        if(0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, (-1), "can't read number of attributes attached to object: %s", H5_daos_err_to_string(ret));

        p = nattrs_buf;
        /* Check for no num attributes found, in this case it must be 0 */
        if(iod.iod_size == (uint64_t)0) {
            nattrs = 0;
        } /* end if */
        else
            /* Decode num attributes */
            UINT64DECODE(p, nattrs);
    } /* end if */
    else {
        H5_daos_iter_data_t iter_data;

        /* Iterate through attributes */

        /* Register id for target object */
        if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
        target_obj->item.rc++;

        /* Initialize iteration data */
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, H5_ITER_NATIVE,
                FALSE, NULL, target_obj_id, &nattrs, H5P_DATASET_XFER_DEFAULT, NULL, NULL, NULL);
        iter_data.u.attr_iter_data.attr_iter_op = H5_daos_attribute_iterate_count_attrs_cb;

        /* Retrieve the number of attributes attached to the object */
        if(H5_daos_attribute_iterate(target_obj, &iter_data) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed");
    } /* end else */

    ret_value = (hssize_t)nattrs; /* DSINC - no check for overflow */

done:
    if((target_obj_id >= 0) && (H5Idec_ref(target_obj_id) < 0))
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object");

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_num_attrs() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_update_num_attrs_key
 *
 * Purpose:     Updates the target object's attribute number tracking akey
 *              by setting its value to the specified value.
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
H5_daos_object_update_num_attrs_key(H5_daos_obj_t *target_obj, uint64_t new_nattrs)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint8_t nattrs_new_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(target_obj->ocpl_cache.track_acorder);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    /* Encode buffer */
    p = nattrs_new_buf;
    UINT64ENCODE(p, new_nattrs);

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, nattrs_new_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Issue write */
    if(0 != (ret = daos_obj_update(target_obj->obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write number of attributes to object: %s", H5_daos_err_to_string(ret));

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_update_num_attrs_key() */


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
H5_daos_group_copy_cb(hid_t group, const char *name,
    const H5L_info2_t *info, void *op_data)
{
    group_copy_op_data *copy_op_data = (group_copy_op_data *) op_data;
    H5VL_loc_params_t sub_loc_params;
    H5_daos_group_t *copied_group = NULL;
    H5_daos_obj_t *grp_obj = NULL;
    H5_daos_obj_t *obj_to_copy = NULL;
    H5I_type_t opened_obj_type;
    herr_t ret_value = H5_ITER_CONT;

    if(NULL == (grp_obj = H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for group");

    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name = name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    switch (info->type) {
        case H5L_TYPE_HARD:
        {
            /* Open the object being copied */
            if(NULL == (obj_to_copy = H5_daos_object_open(grp_obj, &sub_loc_params, &opened_obj_type,
                    copy_op_data->dxpl_id, NULL)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open object");

            /*
             * If performing a shallow group copy, copy the group without its immediate members.
             * Otherwise, continue on with a normal recursive object copy.
             */
            if((opened_obj_type == H5I_GROUP) && (copy_op_data->object_copy_opts & H5O_COPY_SHALLOW_HIERARCHY_FLAG)) {
                if(NULL == (copied_group = H5_daos_group_copy_helper((H5_daos_group_t *) obj_to_copy,
                        copy_op_data->new_group, name, copy_op_data->object_copy_opts,
                        copy_op_data->lcpl_id, copy_op_data->dxpl_id, NULL)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "failed to perform shallow copy of group");
            } /* end if */
            else {
                if(H5_daos_object_copy_helper(obj_to_copy, opened_obj_type, copy_op_data->new_group, name,
                        copy_op_data->object_copy_opts, copy_op_data->lcpl_id,
                        copy_op_data->req, copy_op_data->first_task, copy_op_data->dep_task) < 0)
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
            if (copy_op_data->object_copy_opts & H5O_COPY_EXPAND_SOFT_LINK_FLAG) {
                /* Open the object being copied */
                if(NULL == (obj_to_copy = H5_daos_object_open(grp_obj, &sub_loc_params, &opened_obj_type,
                        copy_op_data->dxpl_id, NULL)))
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open object");

                /* Copy the object */
                if(H5_daos_object_copy_helper(obj_to_copy, opened_obj_type, copy_op_data->new_group, name,
                        copy_op_data->object_copy_opts, copy_op_data->lcpl_id,
                        copy_op_data->req, copy_op_data->first_task,
                        copy_op_data->dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy object");
            } /* end if */
            else {
                /* Copy the link as is */
                if(H5_daos_link_copy(grp_obj, &sub_loc_params, copy_op_data->new_group, &sub_loc_params,
                        copy_op_data->lcpl_id, H5P_LINK_ACCESS_DEFAULT, copy_op_data->dxpl_id, NULL) < 0)
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
            if (copy_op_data->object_copy_opts & H5O_COPY_EXPAND_EXT_LINK_FLAG) {
                /* TODO: Copy the object */
                D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, H5_ITER_ERROR, "H5O_COPY_EXPAND_EXT_LINK_FLAG flag is currently unsupported");
            } /* end if */
            else {
                /* Copy the link as is */
                if(H5_daos_link_copy(grp_obj, &sub_loc_params, copy_op_data->new_group, &sub_loc_params,
                        copy_op_data->lcpl_id, H5P_LINK_ACCESS_DEFAULT, copy_op_data->dxpl_id, NULL) < 0)
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
    if(copied_group)
        if(H5_daos_group_close(copied_group, copy_op_data->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close group");
    if(obj_to_copy)
        if(H5_daos_object_close(obj_to_copy, copy_op_data->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object");

    D_FUNC_LEAVE;
} /* end H5_daos_group_copy_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy
 *
 * Purpose:     Helper routine to copy a specified group to the given
 *              location specified by the dst_obj/dst_name pair. The new
 *              group's name is specified by the base portion of dst_name.
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
H5_daos_group_copy(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, H5_daos_req_t *req,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_iter_data_t iter_data;
    group_copy_op_data copy_op_data;
    H5_daos_group_t *new_group = NULL;
    H5_index_t iter_index_type;
    hid_t target_group_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /* Copy the group */
    if(NULL == (new_group = H5_daos_group_copy_helper(src_obj, dst_obj, dst_name,
            obj_copy_options, lcpl_id, req->dxpl_id, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "failed to copy group");

    /* Register an ID for the group to iterate over */
    if((target_group_id = H5VLwrap_register(src_obj, H5I_GROUP)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
    src_obj->obj.item.rc++;

    /* Setup group copying op_data to pass to the link iteration callback function */
    copy_op_data.new_group = new_group;
    copy_op_data.object_copy_opts = obj_copy_options;
    copy_op_data.lcpl_id = lcpl_id;
    copy_op_data.dxpl_id = req->dxpl_id;
    copy_op_data.req = req;
    copy_op_data.first_task = first_task;
    copy_op_data.dep_task = dep_task;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether creation order is tracked for the group.
     */
    iter_index_type = (src_obj->gcpl_cache.track_corder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, iter_index_type, H5_ITER_INC,
            FALSE, NULL, target_group_id, &copy_op_data, req->dxpl_id, req, first_task, dep_task);
    iter_data.u.link_iter_data.link_iter_op = H5_daos_group_copy_cb;

    /* Copy the immediate members of the group. If the H5O_COPY_SHALLOW_HIERARCHY_FLAG wasn't
     * specified, this will also recursively copy the members of any groups found. */
    if(H5_daos_link_iterate(src_obj, &iter_data) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "failed to iterate over group's links");

done:
    if(new_group) {
        if(H5_daos_group_close(new_group, req->dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");
        new_group = NULL;
    } /* end if */

    if(target_group_id >= 0)
        if(H5Idec_ref(target_group_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group ID");

    D_FUNC_LEAVE;
} /* end H5_daos_group_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy_helper
 *
 * Purpose:     Helper routine for H5_daos_group_copy that actually copies
 *              the specified group. This routine is needed to split the
 *              group copying logic away from the higher-level
 *              H5_daos_group_copy, which also copies the immediate members
 *              of a group during a shallow copy, or the entire hierarchy
 *              during a deep copy.
 *
 *              When a shallow group copy is being done, the group copying
 *              callback for link iteration can simply call this routine to
 *              just copy the group. Otherwise, during a deep copy, it can
 *              call H5_daos_group_copy to copy the group and its members
 *              as well.
 *
 * Return:      Non-negative on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static H5_daos_group_t *
H5_daos_group_copy_helper(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t dest_loc_params;
    H5_daos_group_t *copied_group = NULL;
    H5_daos_group_t *ret_value = NULL;

    /* Copy the group */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (copied_group = H5_daos_group_create(dst_obj, &dest_loc_params, dst_name, lcpl_id,
            src_obj->gcpl_id, src_obj->gapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to create new group");

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the group's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) copied_group, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy group's attributes");

    ret_value = copied_group;

done:
    if(!ret_value && copied_group)
        if(H5_daos_group_close(copied_group, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");

    D_FUNC_LEAVE;
} /* end H5_daos_group_copy_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_copy
 *
 * Purpose:     Helper routine to copy a specified committed datatype to
 *              the given location specified by the dst_obj/dst_name pair.
 *              The new committed datatype's name is specified by the base
 *              portion of dst_name.
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
H5_daos_datatype_copy(H5_daos_dtype_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t dest_loc_params;
    H5_daos_dtype_t *new_dtype = NULL;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /*
     * Copy the datatype
     */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (new_dtype = H5_daos_datatype_commit(dst_obj, &dest_loc_params, dst_name, src_obj->type_id,
            lcpl_id, src_obj->tcpl_id, src_obj->tapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "failed to commit new datatype");

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the datatype's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) new_dtype, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "failed to copy datatype's attributes");

done:
    if(new_dtype) {
        if(H5_daos_datatype_close(new_dtype, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype");
        new_dtype = NULL;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_datatype_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_copy
 *
 * Purpose:     Helper routine to copy a specified dataset to the given
 *              location specified by the dst_obj/dst_name pair. The new
 *              dataset's name is specified by the base portion of
 *              dst_name.
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
H5_daos_dataset_copy(H5_daos_dset_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t dest_loc_params;
    H5_daos_dset_t *new_dset = NULL;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /*
     * Copy the dataset
     */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (new_dset = H5_daos_dataset_create(dst_obj, &dest_loc_params, dst_name, lcpl_id,
            src_obj->type_id, src_obj->space_id, src_obj->dcpl_id, src_obj->dapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failed to create new dataset");

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the dataset's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) new_dset, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failed to copy dataset's attributes");

done:
    if(new_dset) {
        if(H5_daos_dataset_close(new_dset, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset");
        new_dset = NULL;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_copy() */


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
    const H5A_info_t H5VL_DAOS_UNUSED *ainfo, void *op_data)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t *src_loc_obj = NULL;
    H5_daos_obj_t *destination_obj = (H5_daos_obj_t *)op_data;
    H5_daos_attr_t *cur_attr = NULL;
    H5_daos_attr_t *new_attr = NULL;
    herr_t ret_value = H5_ITER_CONT;

    sub_loc_params.obj_type = H5I_ATTR;
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;

    if(NULL == (src_loc_obj = H5VLobject(location_id)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for source location ID");

    if(NULL == (cur_attr = H5_daos_attribute_open(src_loc_obj, &sub_loc_params, attr_name,
            H5P_ATTRIBUTE_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open attribute");

    if(NULL == (new_attr = H5_daos_attribute_create(destination_obj, &sub_loc_params,
            attr_name, cur_attr->type_id, cur_attr->space_id, cur_attr->acpl_id,
            H5P_ATTRIBUTE_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, H5_ITER_ERROR, "failed to create new attribute");

done:
    if(new_attr)
        if(H5_daos_attribute_close(new_attr, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTCLOSEOBJ, H5_ITER_ERROR, "failed to close attribute");
    if(cur_attr)
        if(H5_daos_attribute_close(cur_attr, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTCLOSEOBJ, H5_ITER_ERROR, "failed to close attribute");

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_attributes_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_attributes
 *
 * Purpose:     Helper routine to copy all of the attributes from a given
 *              object to the specified destination object.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
    hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_iter_data_t iter_data;
    H5_index_t iter_index_type;
    hid_t target_obj_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);

    /* Register ID for source object */
    if((target_obj_id = H5VLwrap_register(src_obj, src_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
    src_obj->item.rc++;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether attribute creation order is tracked for the object.
     */
    iter_index_type = (src_obj->ocpl_cache.track_acorder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data. Attributes are re-created by creation order if possible */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, iter_index_type, H5_ITER_INC,
            FALSE, NULL, target_obj_id, dst_obj, dxpl_id, NULL, NULL, NULL);
    iter_data.u.attr_iter_data.attr_iter_op = H5_daos_object_copy_attributes_cb;

    if(H5_daos_attribute_iterate(src_obj, &iter_data) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "failed to iterate over object's attributes");

done:
    if(target_obj_id >= 0)
        if(H5Idec_ref(target_obj_id) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object ID");

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_attributes() */

