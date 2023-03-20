/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 *          library. Generic object routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Task user data for opening a DAOS HDF5 object */
typedef struct H5_daos_object_open_ud_t {
    H5_daos_req_t  *req;
    tse_task_t     *open_metatask;
    void           *loc_obj;
    hid_t           lapl_id;
    daos_obj_id_t   oid;
    hbool_t         collective;
    H5I_type_t     *obj_type_out;
    H5_daos_obj_t **obj_out;
} H5_daos_object_open_ud_t;

typedef struct H5_daos_get_oid_by_idx_ud_t {
    H5_daos_req_t   *req;
    H5_daos_group_t *target_grp;
    daos_obj_id_t   *oid_out;
    tse_task_t      *oid_retrieval_metatask;
    const char      *link_name;
    size_t           link_name_len;
    char            *path_buf;
} H5_daos_get_oid_by_idx_ud_t;

typedef struct H5_daos_oid_bcast_ud_t {
    H5_daos_mpi_ibcast_ud_t bcast_udata; /* Must be first */
    daos_obj_id_t          *oid;
    uint8_t                 oid_buf[H5_DAOS_ENCODED_OID_SIZE];
} H5_daos_oid_bcast_ud_t;

/* Task user data for retrieving info about an object */
typedef struct H5_daos_object_get_info_ud_t {
    H5_daos_req_t  *req;
    tse_task_t     *get_info_task;
    H5_daos_obj_t **target_obj_p;
    H5_daos_obj_t  *target_obj;
    H5O_info2_t    *info_out;
    unsigned        fields;
} H5_daos_object_get_info_ud_t;

/* Task user data for copying an object */
typedef struct H5_daos_object_copy_ud_t {
    H5_daos_req_t               *req;
    tse_task_t                  *obj_copy_metatask;
    H5_daos_obj_t               *src_obj;
    H5_daos_group_t             *dst_grp;
    H5_daos_obj_t               *copied_obj;
    const char                  *new_obj_name;
    size_t                       new_obj_name_len;
    char                        *new_obj_name_path_buf;
    H5_DAOS_ATTR_EXISTS_OUT_TYPE dst_link_exists;
    unsigned                     obj_copy_options;
    hid_t                        lcpl_id;
} H5_daos_object_copy_ud_t;

/* Task user data for copying data between
 * datasets during object copying.
 */
typedef struct H5_daos_dataset_copy_data_ud_t {
    H5_daos_req_t  *req;
    H5_daos_dset_t *src_dset;
    H5_daos_dset_t *dst_dset;
    void           *data_buf;
    tse_task_t     *data_copy_task;
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
    H5_daos_req_t  *req;
    H5_daos_attr_t *src_attr;
    H5_daos_attr_t *new_attr;
    H5_daos_obj_t  *target_obj;
    tse_task_t     *copy_task;
} H5_daos_object_copy_single_attribute_ud_t;

/* Task user data for checking if a particular
 * object exists in a group according to a
 * given link name.
 */
typedef struct H5_daos_object_exists_ud_t {
    H5_daos_req_t   *req;
    H5_daos_group_t *target_grp;
    daos_obj_id_t    oid;
    const char      *link_name;
    size_t           link_name_len;
    hbool_t          link_exists;
    hbool_t         *oexists_ret;
} H5_daos_object_exists_ud_t;

/* Task user data for object token lookup */
typedef struct H5_daos_object_lookup_ud_t {
    H5_daos_req_t *req;
    H5_daos_obj_t *target_obj;
    H5O_token_t   *token;
} H5_daos_object_lookup_ud_t;

/* Task user data for visiting an object */
typedef struct H5_daos_object_visit_ud_t {
    H5_daos_req_t      *req;
    tse_task_t         *visit_metatask;
    H5_daos_iter_data_t iter_data;
    H5_daos_obj_t      *target_obj;
    H5O_info2_t         obj_info;
    hid_t               target_obj_id;
} H5_daos_object_visit_ud_t;

/* Task user data for retrieving the number of attributes
 * attached to an object
 */
typedef struct H5_daos_object_get_num_attrs_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud; /* Must be first */
    hsize_t              *num_attrs_out;
    uint8_t               nattrs_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
    hbool_t               post_decr;
    herr_t                op_ret;
} H5_daos_object_get_num_attrs_ud_t;

/* Task user data for updating the attribute number tracking
 * akey for an object
 */
typedef struct H5_daos_object_update_num_attrs_key_ud_t {
    H5_daos_md_rw_cb_ud_t update_ud;
    hsize_t              *new_nattrs;
    uint8_t               nattrs_new_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
} H5_daos_object_update_num_attrs_key_ud_t;

/* User data struct for object reference count operations */
typedef struct H5_daos_obj_rw_rc_ud_t {
    H5_daos_req_t  *req;
    H5_daos_obj_t **obj_p;
    H5_daos_obj_t  *obj;
    uint64_t       *rc;
    unsigned       *rc_uint;
    int64_t         adjust;
    daos_key_t      dkey;
    daos_iod_t      iod;
    daos_sg_list_t  sgl;
    daos_iov_t      sg_iov;
    uint8_t         rc_buf[H5_DAOS_ENCODED_RC_SIZE];
    tse_task_t     *op_task;
    char           *task_name;
} H5_daos_obj_rw_rc_ud_t;

/********************/
/* Local Prototypes */
/********************/

static int    H5_daos_object_open_by_oid(H5_daos_obj_t **obj_out, H5_daos_file_t *file, daos_obj_id_t oid,
                                         hid_t apl_id, hbool_t collective, H5_daos_req_t *req,
                                         tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_object_open_task(tse_task_t *task);
static herr_t H5_daos_object_get_oid_by_name(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
                                             daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req,
                                             tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_object_get_oid_by_idx(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
                                            daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req,
                                            tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_object_gobi_follow_task(tse_task_t *task);
static int    H5_daos_object_get_oid_by_idx_finish(tse_task_t *task);
static herr_t H5_daos_object_get_oid_by_token(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
                                              daos_obj_id_t *oid_out, hbool_t collective, H5_daos_req_t *req,
                                              tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_object_oid_bcast(H5_daos_file_t *file, daos_obj_id_t *oid, H5_daos_req_t *req,
                                       tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_object_oid_bcast_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_object_oid_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_object_get_info(H5_daos_obj_t ***target_obj_prev_out, H5_daos_obj_t **target_obj_p,
                                      H5_daos_obj_t *target_obj, unsigned fields, H5O_info2_t *obj_info_out,
                                      H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_object_get_info_task(tse_task_t *task);
static int    H5_daos_object_get_info_end(tse_task_t *task);
static int    H5_daos_get_num_attrs_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_get_num_attrs_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_object_update_num_attrs_key_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_object_update_num_attrs_key_comp_cb(tse_task_t *task, void *args);

static herr_t           H5_daos_object_copy_helper(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params,
                                                   const char *src_name, void *dst_loc_obj,
                                                   const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                                                   unsigned obj_copy_options, hid_t lcpl_id,
                                                   H5_DAOS_ATTR_EXISTS_OUT_TYPE **link_exists_p, H5_daos_req_t *req,
                                                   tse_task_t **first_task, tse_task_t **dep_task);
static int              H5_daos_object_copy_task(tse_task_t *task);
static herr_t           H5_daos_object_copy_free_copy_udata(H5_daos_object_copy_ud_t *copy_udata,
                                                            tse_task_t **first_task, tse_task_t **dep_task);
static int              H5_daos_object_copy_free_copy_udata_task(tse_task_t *task);
static herr_t           H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
                                                       H5_daos_req_t *req, tse_task_t **first_task,
                                                       tse_task_t **dep_task);
static herr_t           H5_daos_object_copy_attributes_cb(hid_t location_id, const char *attr_name,
                                                          const H5A_info_t H5VL_DAOS_UNUSED *ainfo, void *op_data,
                                                          herr_t *op_ret, tse_task_t **first_task,
                                                          tse_task_t **dep_task);
static herr_t           H5_daos_object_copy_single_attribute(H5_daos_obj_t *src_obj, const char *attr_name,
                                                             H5_daos_obj_t *target_obj, H5_daos_req_t *req,
                                                             tse_task_t **first_task, tse_task_t **dep_task);
static int              H5_daos_object_copy_single_attribute_task(tse_task_t *task);
static int              H5_daos_object_copy_single_attribute_free_udata_task(tse_task_t *task);
static herr_t           H5_daos_group_copy(H5_daos_object_copy_ud_t *obj_copy_udata, H5_daos_req_t *req,
                                           tse_task_t **first_task, tse_task_t **dep_task);
static H5_daos_group_t *H5_daos_group_copy_helper(H5_daos_group_t *src_grp, H5_daos_group_t *dst_grp,
                                                  const char *name, unsigned obj_copy_options,
                                                  H5_daos_req_t *req, tse_task_t **first_task,
                                                  tse_task_t **dep_task);
static herr_t H5_daos_group_copy_cb(hid_t group, const char *name, const H5L_info2_t *info, void *op_data,
                                    herr_t *op_ret, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_datatype_copy(H5_daos_object_copy_ud_t *obj_copy_udata, H5_daos_req_t *req,
                                    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_dataset_copy(H5_daos_object_copy_ud_t *obj_copy_udata, H5_daos_req_t *req,
                                   tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_dataset_copy_data(H5_daos_dset_t *src_dset, H5_daos_dset_t *dst_dset,
                                        H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_dataset_copy_data_task(tse_task_t *task);
static int    H5_daos_dset_copy_data_end_task(tse_task_t *task);

static int    H5_daos_object_lookup_task(tse_task_t *task);
static herr_t H5_daos_object_exists(H5_daos_group_t *target_grp, const char *link_name, size_t link_name_len,
                                    hbool_t *oexists_ret, H5_daos_req_t *req, tse_task_t **first_task,
                                    tse_task_t **dep_task);
static int    H5_daos_object_exists_finish(tse_task_t *task);
static int    H5_daos_object_visit_task(tse_task_t *task);
static herr_t H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info2_t *info,
                                                void *op_data, herr_t *op_ret, tse_task_t **first_task,
                                                tse_task_t **dep_task);
static int    H5_daos_object_visit_finish(tse_task_t *task);
static int    H5_daos_obj_read_rc_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_obj_read_rc_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_obj_write_rc_task(tse_task_t *task);
static int    H5_daos_obj_write_rc_comp_cb(tse_task_t *task, void *args);

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
H5_daos_object_open(void *_item, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id,
                    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item    = (H5_daos_item_t *)_item;
    H5_daos_req_t  *int_req = NULL;
    H5_daos_obj_t  *ret_obj = NULL;
    H5_daos_obj_t   tmp_obj;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    hbool_t         collective;
    hid_t           lapl_id;
    int             ret;
    void           *ret_value = &tmp_obj; /* Initialize to non-NULL; NULL is used for error checking */

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here. If not already set by the
     * file, we then check if collective or independent access has been specified for the given LAPL.
     */
    lapl_id = (H5VL_OBJECT_BY_NAME == loc_params->type)  ? loc_params->loc_data.loc_by_name.lapl_id
              : (H5VL_OBJECT_BY_IDX == loc_params->type) ? loc_params->loc_data.loc_by_idx.lapl_id
                                                         : H5P_LINK_ACCESS_DEFAULT;
    H5_DAOS_GET_METADATA_READ_MODE(item->file, lapl_id, H5P_LINK_ACCESS_DEFAULT, collective, H5E_OBJECT,
                                   NULL);

    /* Start H5 operation */
    if (NULL ==
        (int_req = H5_daos_req_create(item->file, "object open", item->open_req, NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    if (H5_daos_object_open_helper(item, loc_params, opened_type, collective, NULL, &ret_obj, int_req,
                                   &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, NULL, "can't open object");

    /* DSINC - special care will have to be taken with ret_value when external async is introduced */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value == NULL)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the parent object's request queue.  This will add
         * the dependency on the parent object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_OBJ,
                                collective, !req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async - disabled except for open by token */
        if (req && loc_params->type == H5VL_OBJECT_BY_TOKEN) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, NULL,
                             "object open operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */

        /* Set return value */
        ret_value = ret_obj;
    } /* end if */

    /* Make sure we return something sensible if ret_value never got set */
    if (ret_value == &tmp_obj)
        ret_value = NULL;

    /* Clean up ret_obj if we had a failure and we're not returning ret_obj */
    if (NULL == ret_value && ret_obj && H5_daos_object_close(&ret_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, NULL, "can't close object");

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_helper
 *
 * Purpose:     Internal-use helper routine to create an asynchronous task
 *              for opening a DAOS HDF5 object.  Exactly one of ret_obj_p
 *              and ret_obj must be provided.  If ret_obj_p is provided,
 *              *ret_obj_p will be set to the address of a H5_daos_obj_t**
 *              managed by this function that will be used to place the
 *              output object once it is allocated.  This way, the calling
 *              function can set **ret_obj_p to the address of its
 *              H5_daos_obj_t * and this pointer will be filled in by
 *              tasks created by this function when it is ready.
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
H5_daos_object_open_helper(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type,
                           hbool_t collective, H5_daos_obj_t ****ret_obj_p, H5_daos_obj_t **ret_obj,
                           H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_open_ud_t *open_udata          = NULL;
    tse_task_t               *open_task           = NULL;
    hbool_t                   open_task_scheduled = FALSE;
    H5_daos_obj_t            *loc_obj;
    int                       ret;
    herr_t                    ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(ret_obj_p || ret_obj);
    assert(!(ret_obj_p && ret_obj));
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (item->type == H5I_FILE)
        loc_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
    else
        loc_obj = (H5_daos_obj_t *)item;

    /* Check for open by token, in this case we immediately know the type and
     * can open the object immediately */
    if (loc_params->type == H5VL_OBJECT_BY_TOKEN) {
        daos_obj_id_t oid;

        /* No operations currently use ret_obj_p with BY_TOKEN.  It would be
         * possible to implement, but clumsy, and it would probably be better
         * to modify the clients to not do so if this becomes an issue in the
         * future.  -NAF */
        assert(ret_obj);

        /* Get OID */
        if (H5_daos_object_get_oid_by_token(loc_obj, loc_params, &oid, collective, req, first_task,
                                            dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve OID for object by object token");

        /* Open object */
        if ((ret = H5_daos_object_open_by_oid(ret_obj, item->file, oid, H5P_LINK_ACCESS_DEFAULT, collective,
                                              req, first_task, dep_task)) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open object: %s",
                         H5_daos_err_to_string(ret));

        /* Set opened type */
        assert(*ret_obj);
        if (opened_type)
            *opened_type = (*ret_obj)->item.type;
    } /* end if */
    else {
        /* Set up user data for object open */
        if (NULL == (open_udata = (H5_daos_object_open_ud_t *)DV_malloc(sizeof(H5_daos_object_open_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "failed to allocate buffer for object open user data");
        open_udata->req           = req;
        open_udata->collective    = collective;
        open_udata->oid           = (const daos_obj_id_t){0};
        open_udata->obj_type_out  = opened_type;
        open_udata->obj_out       = ret_obj;
        open_udata->open_metatask = NULL;
        open_udata->loc_obj       = loc_obj;

        /* Set *ret_obj_p to the caller can retarget obj_out */
        if (ret_obj_p)
            *ret_obj_p = &open_udata->obj_out;

        /* Retrieve the OID of the target object and set the lapl id */
        open_udata->lapl_id = H5P_LINK_ACCESS_DEFAULT;
        switch (loc_params->type) {
            case H5VL_OBJECT_BY_NAME:
                if (H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_name.lapl_id)
                    open_udata->lapl_id = H5Pcopy(loc_params->loc_data.loc_by_name.lapl_id);

                if (H5_daos_object_get_oid_by_name((H5_daos_obj_t *)item, loc_params, &open_udata->oid,
                                                   collective, req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL,
                                 "can't retrieve OID for object by object name");
                break;

            case H5VL_OBJECT_BY_IDX:
                if (H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_idx.lapl_id)
                    open_udata->lapl_id = H5Pcopy(loc_params->loc_data.loc_by_idx.lapl_id);

                if (H5_daos_object_get_oid_by_idx((H5_daos_obj_t *)item, loc_params, &open_udata->oid,
                                                  collective, req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL,
                                 "can't retrieve OID for object by object index");
                break;

            case H5VL_OBJECT_BY_TOKEN:
            case H5VL_OBJECT_BY_SELF:
            default:
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL, "invalid loc_params type");
        } /* end switch */

        /* Create task for object open */
        if (H5_daos_create_task(H5_daos_object_open_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                NULL, NULL, open_udata, &open_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to open object");

        /* Schedule object open task (or save it to be scheduled later) and give it
         * a reference to req */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(open_task, false)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to open object: %s",
                             H5_daos_err_to_string(ret));
        }
        else
            *first_task = open_task;

        open_task_scheduled = TRUE;

        /* Create meta task for object open. This empty task will be completed
         * when the actual asynchronous object open call is finished. This metatask
         * is necessary because the object open task will generate another async
         * task for actually opening the object once it has figured out the type
         * of the object. */
        if (H5_daos_create_task(H5_daos_metatask_autocomplete, 1, &open_task, NULL, NULL, NULL,
                                &open_udata->open_metatask) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object open");

        /* Schedule meta task */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(open_udata->open_metatask, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object open: %s",
                         H5_daos_err_to_string(ret));

        *dep_task = open_udata->open_metatask;
        req->rc++;
        ((H5_daos_item_t *)open_udata->loc_obj)->rc++;

        /* Relinquish control of the object open udata to the
         * task's completion callback */
        open_udata = NULL;
    } /* end else */

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (!open_task_scheduled) {
            open_udata = DV_free(open_udata);
        } /* end if */
    }     /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_open_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_by_oid
 *
 * Purpose:     Opens a DAOS HDF5 object, given the OID.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_open_by_oid(H5_daos_obj_t **obj_out, H5_daos_file_t *file, daos_obj_id_t oid, hid_t apl_id,
                           hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
                           tse_task_t **dep_task)
{
    H5_daos_obj_t *obj       = NULL;
    H5I_type_t     obj_type  = H5I_UNINIT;
    int            ret_value = 0;

    assert(obj_out);
    assert(file);
    assert(first_task);
    assert(dep_task);

    /* Get object type */
    if (H5I_BADID == (obj_type = H5_daos_oid_to_type(oid)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, -H5_DAOS_DAOS_GET_ERROR, "can't get object type");

    /* Call object's open function */
    if (obj_type == H5I_GROUP) {
        if (apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_GROUP_ACCESS_DEFAULT;

        /* Allocate the group object that is returned to the user */
        if (NULL == (obj = H5FL_CALLOC(H5_daos_group_t)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_H5_OPEN_ERROR,
                         "can't allocate DAOS group struct");

        if (H5_daos_group_open_helper(file, (H5_daos_group_t *)obj, apl_id, collective, req, first_task,
                                      dep_task) < 0) {
            /* Assumed here that H5_daos_group_open_helper will free "obj" on failure */
            obj = NULL;
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open group");
        }
    } /* end if */
    else if (obj_type == H5I_DATASET) {
        if (apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_DATASET_ACCESS_DEFAULT;

        if (NULL == (obj = (H5_daos_obj_t *)H5_daos_dataset_open_helper(file, apl_id, collective, req,
                                                                        first_task, dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open dataset");
    } /* end if */
    else if (obj_type == H5I_DATATYPE) {
        if (apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_DATATYPE_ACCESS_DEFAULT;

        if (NULL == (obj = (H5_daos_obj_t *)H5_daos_datatype_open_helper(file, apl_id, collective, req,
                                                                         first_task, dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open datatype");
    } /* end if */
    else {
        assert(obj_type == H5I_MAP);

        if (apl_id == H5P_LINK_ACCESS_DEFAULT)
            apl_id = H5P_MAP_ACCESS_DEFAULT;

        if (NULL == (obj = (H5_daos_obj_t *)H5_daos_map_open_helper(file, apl_id, collective, req, first_task,
                                                                    dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open map");
    } /* end else */

    /* Fill in object's OID */
    assert(obj);
    obj->oid = oid;

    /* Return obj */
    *obj_out = obj;
    obj      = NULL;

done:
    if (obj) {
        assert(ret_value < 0);
        if (H5_daos_object_close(&obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_open_by_oid() */

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
    H5_daos_obj_t            *obj        = NULL;
    tse_task_t               *first_task = NULL;
    tse_task_t               *dep_task   = NULL;
    int                       ret;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object open task");

    assert(udata->req);
    assert(udata->loc_obj);
    assert(udata->obj_out);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    /* Open object */
    if ((ret = H5_daos_object_open_by_oid(&obj, ((H5_daos_item_t *)udata->loc_obj)->file, udata->oid,
                                          udata->lapl_id, udata->collective, udata->req, &first_task,
                                          &dep_task)) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, ret, "can't open object: %s", H5_daos_err_to_string(ret));

    /* Register dependency on dep_task for object open metatask */
    if (dep_task && 0 != (ret = tse_task_register_deps(udata->open_metatask, 1, &dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for object open metatask: %s",
                     H5_daos_err_to_string(ret));

    if (udata->obj_type_out)
        *udata->obj_type_out = obj->item.type;

    *udata->obj_out = obj;

done:
    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to open object: %s",
                     H5_daos_err_to_string(ret));

    if (udata) {
        if (udata->loc_obj && H5_daos_object_close(udata->loc_obj) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTCLOSEOBJ, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");
        if (H5P_LINK_ACCESS_DEFAULT != udata->lapl_id && H5Pclose(udata->lapl_id) < 0)
            D_DONE_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, -H5_DAOS_H5_CLOSE_ERROR,
                         "can't close link access plist");

        if (ret_value < 0) {
            if (obj && H5_daos_object_close(&obj->item) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

            /* Handle errors in this function */
            /* Do not place any code that can issue errors after this block, except for
             * H5_daos_req_free_int, which updates req->status if it sees an error */
            if (ret_value != -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
                udata->req->status      = ret_value;
                udata->req->failed_task = "object open";
            } /* end if */

            /* Return task to task list */
            if (H5_daos_task_list_put(H5_daos_task_list_g, udata->open_metatask) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                             "can't return task to task list");

            /* Complete metatask */
            tse_task_complete(udata->open_metatask, ret_value);
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
    H5_daos_obj_t *target_obj        = NULL;
    hbool_t        leader_must_bcast = FALSE;
    char          *path_buf          = NULL;
    herr_t         ret_value         = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(oid_out);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(H5VL_OBJECT_BY_NAME == loc_params->type);

    /* Check for simple case of '.' for object name */
    if (!strncmp(loc_params->loc_data.loc_by_name.name, ".", 2)) {
        if (loc_obj->item.type == H5I_FILE)
            *oid_out = ((H5_daos_file_t *)loc_obj)->root_grp->obj.oid;
        else
            *oid_out = loc_obj->oid;

        D_GOTO_DONE(SUCCEED);
    } /* end if */

    /*
     * Check if we're actually retrieving the OID or just receiving it from
     * the leader.
     */
    if (!collective || (loc_obj->item.file->my_rank == 0)) {
        const char *target_name = NULL;
        size_t      target_name_len;

        if (collective && (loc_obj->item.file->num_procs > 1))
            leader_must_bcast = TRUE;

        /* Traverse the path */
        if (NULL ==
            (target_obj = H5_daos_group_traverse(
                 (H5_daos_item_t *)loc_obj, loc_params->loc_data.loc_by_name.name, H5P_LINK_CREATE_DEFAULT,
                 req, FALSE, &path_buf, &target_name, &target_name_len, first_task, dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't traverse path");

        /* Check for no target_name, in this case just reopen target_obj */
        if (target_name[0] == '\0' || (target_name[0] == '.' && target_name[1] == '\0'))
            *oid_out = target_obj->oid;
        else {
            daos_obj_id_t **oid_ptr = NULL;

            /* Check type of target_obj */
            if (target_obj->item.type != H5I_GROUP)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "target object is not a group");

            /* Follow link to object */
            if (H5_daos_link_follow((H5_daos_group_t *)target_obj, target_name, target_name_len, FALSE, req,
                                    &oid_ptr, NULL, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't follow link to object");

            /* Retarget *oid_ptr so H5_daos_link_follow fills in the object's oid */
            *oid_ptr = oid_out;
        } /* end else */
    }     /* end if */

    /* Signify that all ranks will have called the bcast after this point */
    leader_must_bcast = FALSE;

    /* Broadcast OID if there are other processes that need it */
    if (collective && (loc_obj->item.file->num_procs > 1))
        if (H5_daos_object_oid_bcast(loc_obj->item.file, oid_out, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTSET, FAIL, "can't broadcast OID");

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (leader_must_bcast && (loc_obj->item.file->my_rank == 0)) {
            /*
             * Bcast oid as '0' if necessary - this will trigger
             * failures in other processes.
             */
            oid_out->lo = 0;
            oid_out->hi = 0;
            if (H5_daos_object_oid_bcast(loc_obj->item.file, oid_out, req, first_task, dep_task) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_MPI, FAIL, "can't broadcast empty object ID");
        } /* end if */
    }     /* end if */

    /* Free path_buf if necessary */
    if (path_buf && H5_daos_free_async(path_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTFREE, FAIL, "can't free path buffer");

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
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
    H5_daos_get_oid_by_idx_ud_t *get_oid_udata = NULL;
    H5VL_loc_params_t            sub_loc_params;
    tse_task_t                  *link_follow_task = NULL;
    H5_daos_req_t               *int_int_req      = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(oid_out);
    assert(H5VL_OBJECT_BY_IDX == loc_params->type);

    /* Allocate argument struct for OID retrieval task */
    if (NULL ==
        (get_oid_udata = (H5_daos_get_oid_by_idx_ud_t *)DV_calloc(sizeof(H5_daos_get_oid_by_idx_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for OID retrieval task arguments");
    get_oid_udata->req                    = req;
    get_oid_udata->target_grp             = NULL;
    get_oid_udata->oid_retrieval_metatask = NULL;
    get_oid_udata->oid_out                = oid_out;
    get_oid_udata->link_name              = NULL;
    get_oid_udata->link_name_len          = 0;
    get_oid_udata->path_buf               = NULL;

    /* Start internal H5 operation for target object open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(loc_obj->item.file,
                                                  "target object open within object get oid by index", NULL,
                                                  NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Open the group containing the target object */
    /* We should implement oid bcast here (or in calling function(s)) so all
     * ranks don't need to independently grab the oid -NAF */
    sub_loc_params.type     = H5VL_OBJECT_BY_SELF;
    sub_loc_params.obj_type = H5I_GROUP;
    if (NULL == (get_oid_udata->target_grp = (H5_daos_group_t *)H5_daos_group_open_int(
                     &loc_obj->item, &sub_loc_params, loc_params->loc_data.loc_by_idx.name,
                     loc_params->loc_data.loc_by_idx.lapl_id, int_int_req, FALSE, first_task, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open group containing target object");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = int_int_req->finalize_task;
    *dep_task   = int_int_req->finalize_task;
    int_int_req = NULL;

    /* Retrieve the name of the link at the given index */
    if (H5_daos_link_get_name_by_idx_alloc(
            get_oid_udata->target_grp, loc_params->loc_data.loc_by_idx.idx_type,
            loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
            &get_oid_udata->link_name, &get_oid_udata->link_name_len, &get_oid_udata->path_buf, NULL, req,
            first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get link name");

    /* Create task to follow link once link name is valid */
    if (H5_daos_create_task(H5_daos_object_gobi_follow_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, get_oid_udata, &link_follow_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create link follow task for OID retrieval");

    /* Schedule link follow task */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(link_follow_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                         "can't schedule link follow task for OID retrieval: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = link_follow_task;
    get_oid_udata->target_grp->obj.item.rc++;
    req->rc++;
    *dep_task = link_follow_task;

    /* Create metatask for OID retrieval task to free data after retrieval is finished. This
     * task will be completed when the actual asynchronous OID retrieval is finished.
     */
    if (H5_daos_create_task(H5_daos_object_get_oid_by_idx_finish, *dep_task ? 1 : 0,
                            *dep_task ? dep_task : NULL, NULL, NULL, get_oid_udata,
                            &get_oid_udata->oid_retrieval_metatask) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create metatask for OID retrieval");

    /* Schedule meta task */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(get_oid_udata->oid_retrieval_metatask, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule metatask for OID retrieval: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = get_oid_udata->oid_retrieval_metatask;
    req->rc++;
    *dep_task = get_oid_udata->oid_retrieval_metatask;

    /* Relinquish control of udata to task's function body */
    get_oid_udata = NULL;

done:
    if (get_oid_udata) {
        assert(ret_value < 0);
        get_oid_udata = DV_free(get_oid_udata);
    }

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_oid_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_gobi_follow_task
 *
 * Purpose:     Asynchronous task to call H5_daos_link_follow during
 *              object OID retrieval by an index value. Executes once the
 *              link name for the object is valid.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_gobi_follow_task(tse_task_t *task)
{
    H5_daos_get_oid_by_idx_ud_t *udata;
    daos_obj_id_t              **oid_ptr;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    int                          ret;
    int                          ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for OID retrieval link follow task");

    assert(udata->req);
    assert(udata->target_grp);
    assert(udata->link_name);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    /* Attempt to follow the link */
    if (H5_daos_link_follow(udata->target_grp, udata->link_name, udata->link_name_len, FALSE, udata->req,
                            &oid_ptr, NULL, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, -H5_DAOS_SETUP_ERROR, "can't follow link to object");

    /* Retarget *oid_ptr so H5_daos_link_follow fills in the object's oid */
    *oid_ptr = udata->oid_out;

    /* Register task dependency */
    if (dep_task && 0 != (ret = tse_task_register_deps(udata->oid_retrieval_metatask, 1, &dep_task)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for link follow: %s",
                     H5_daos_err_to_string(ret));

done:
    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to follow link: %s",
                     H5_daos_err_to_string(ret));

    if (udata) {
        /* Close group */
        if (H5_daos_group_close_real(udata->target_grp) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "OID retrieval link follow task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_gobi_follow_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_oid_by_idx_finish
 *
 * Purpose:     Asynchronous task to free OID retrieval udata once OID
 *              retrieval has finished.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_get_oid_by_idx_finish(tse_task_t *task)
{
    H5_daos_get_oid_by_idx_ud_t *udata;
    int                          ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for OID retrieval task");

    assert(udata->req);
    assert(udata->target_grp);
    assert(task == udata->oid_retrieval_metatask);

    if (H5_daos_group_close_real(udata->target_grp) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "OID retrieval udata free task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free path buffer */
    if (udata->path_buf)
        DV_free(udata->path_buf);

    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_oid_by_idx_finish() */

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
                                daos_obj_id_t *oid_out, hbool_t H5VL_DAOS_UNUSED collective,
                                H5_daos_req_t H5VL_DAOS_UNUSED *req, tse_task_t H5VL_DAOS_UNUSED **first_task,
                                tse_task_t H5VL_DAOS_UNUSED **dep_task)
{
    daos_obj_id_t oid;
    herr_t        ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(oid_out);
    assert(H5VL_OBJECT_BY_TOKEN == loc_params->type);

    /* Generate OID from object token */
    if (H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
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
H5_daos_object_oid_bcast(H5_daos_file_t *file, daos_obj_id_t *oid, H5_daos_req_t *req,
                         tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_oid_bcast_ud_t *oid_bcast_udata = NULL;
    tse_task_t             *bcast_task      = NULL;
    int                     ret;
    herr_t                  ret_value = SUCCEED;

    assert(file);
    assert(oid);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up broadcast user data */
    if (NULL == (oid_bcast_udata = (H5_daos_oid_bcast_ud_t *)DV_malloc(sizeof(H5_daos_oid_bcast_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "failed to allocate buffer for MPI broadcast user data");
    oid_bcast_udata->bcast_udata.req            = req;
    oid_bcast_udata->bcast_udata.obj            = NULL;
    oid_bcast_udata->bcast_udata.bcast_metatask = NULL;
    oid_bcast_udata->bcast_udata.buffer         = oid_bcast_udata->oid_buf;
    oid_bcast_udata->bcast_udata.buffer_len     = H5_DAOS_ENCODED_OID_SIZE;
    oid_bcast_udata->bcast_udata.count          = H5_DAOS_ENCODED_OID_SIZE;
    oid_bcast_udata->bcast_udata.comm           = req->file->comm;
    oid_bcast_udata->oid                        = oid;

    /* Create task for broadcast */
    if (H5_daos_create_task(H5_daos_mpi_ibcast_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            (file->my_rank == 0) ? H5_daos_object_oid_bcast_prep_cb : NULL,
                            H5_daos_object_oid_bcast_comp_cb, oid_bcast_udata, &bcast_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to broadcast OID");

    /* Schedule OID broadcast task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(bcast_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to broadcast OID: %s",
                         H5_daos_err_to_string(ret));
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
    if (oid_bcast_udata) {
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
    uint8_t                *p;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for OID broadcast task");

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
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for OID broadcast task");

    assert(udata->bcast_udata.req);
    assert(udata->bcast_udata.buffer);
    assert(udata->oid);
    assert(H5_DAOS_ENCODED_OID_SIZE == udata->bcast_udata.buffer_len);
    assert(H5_DAOS_ENCODED_OID_SIZE == udata->bcast_udata.count);

    /* Handle errors in OID broadcast task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast OID";
    } /* end if */
    else if (task->dt_result == 0) {
        /* Decode sent OID on receiving ranks */
        if (udata->bcast_udata.req->file->my_rank != 0) {
            uint8_t *p = udata->bcast_udata.buffer;
            UINT64DECODE(p, udata->oid->lo);
            UINT64DECODE(p, udata->oid->hi);

            /* Check for oid.lo set to 0 - indicates failure */
            if (udata->oid->lo == 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTSET, -H5_DAOS_REMOTE_ERROR,
                             "lead process indicated OID broadcast failure");
        }
    } /* end else */

done:
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast OID completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
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
H5_daos_object_copy(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name,
                    void *dst_loc_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                    hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t                *item             = (H5_daos_item_t *)src_loc_obj;
    H5_daos_item_t                *dst_item         = (H5_daos_item_t *)dst_loc_obj;
    H5_daos_req_t                 *int_req          = NULL;
    tse_task_t                    *first_task       = NULL;
    tse_task_t                    *dep_task         = NULL;
    unsigned                       obj_copy_options = 0;
    hbool_t                        collective;
    H5_DAOS_ATTR_EXISTS_OUT_TYPE **link_exists_p;
    hid_t                          lapl_id = H5P_LINK_ACCESS_DEFAULT;
    int                            ret;
    herr_t                         ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!src_loc_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source location object is NULL");
    if (!src_loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL");
    if (!src_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object name is NULL");
    if (!dst_loc_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination location object is NULL");
    if (!dst_loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL");
    if (!dst_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object name is NULL");
    if (H5VL_OBJECT_BY_SELF != src_loc_params->type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source location parameters type is invalid");
    if (H5VL_OBJECT_BY_SELF != dst_loc_params->type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination location parameters type is invalid");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Determine metadata I/O mode setting (collective vs. independent)
     * for metadata writes according to file-wide setting on FAPL.
     */
    H5_DAOS_GET_METADATA_WRITE_MODE(item->file, lapl_id, H5P_LINK_ACCESS_DEFAULT, collective, H5E_OBJECT,
                                    FAIL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(
                     item->file, "object copy", item->open_req,
                     item->open_req == dst_item->open_req ? NULL : dst_item->open_req, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, 0, NULL /*event*/)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    if (!collective || (item->file->my_rank == 0)) {
        /*
         * First, ensure that the object doesn't currently exist at the specified destination
         * location object/destination name pair.
         */
        if (H5_daos_link_exists((H5_daos_item_t *)dst_loc_obj, dst_name, &link_exists_p, NULL, int_req,
                                &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "couldn't determine if link exists");

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
        if (H5P_OBJECT_COPY_DEFAULT == ocpypl_id)
            obj_copy_options = H5_daos_plist_cache_g->ocpypl_cache.obj_copy_options;
        else if (H5Pget_copy_object(ocpypl_id, &obj_copy_options) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "failed to retrieve object copy options");

        /* Perform the object copy */
        if (H5_daos_object_copy_helper(src_loc_obj, src_loc_params, src_name, dst_loc_obj, dst_loc_params,
                                       dst_name, obj_copy_options, lcpl_id, link_exists_p, int_req,
                                       &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, FAIL, "failed to copy object");
    } /* end if */

done:
    if (int_req) {
        H5_daos_op_pool_type_t  op_type;
        H5_daos_op_pool_scope_t op_scope;

        /* Perform collective error check */
        if (collective && (item->file->num_procs > 1))
            if (H5_daos_collective_error_check((H5_daos_obj_t *)item, int_req, &first_task, &dep_task) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't perform collective error check");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Determine operation type - we will add the operation to global op
         * pool.  If the dst_loc_obj might have link creation order tracked use
         * H5_DAOS_OP_TYPE_WRITE_ORDERED, otherwise use H5_DAOS_OP_TYPE_WRITE.
         */
        if (!dst_item || dst_item->type != H5I_GROUP ||
            ((dst_item->open_req->status == 0 || dst_item->created) &&
             !((H5_daos_group_t *)dst_item)->gcpl_cache.track_corder))
            op_type = H5_DAOS_OP_TYPE_WRITE;
        else
            op_type = H5_DAOS_OP_TYPE_WRITE_ORDERED;

        /* Determine operation scope - use global if the files are different */
        if (item && dst_item && item->file != dst_item->file)
            op_scope = H5_DAOS_OP_SCOPE_GLOB;
        else
            op_scope = H5_DAOS_OP_SCOPE_FILE;

        /* Add the request to the request queue.  This will add the dependency
         * on the source object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, op_type, op_scope, collective, !req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL, "object copy failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

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
H5_daos_object_copy_helper(void *src_loc_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name,
                           void *dst_loc_obj, const H5VL_loc_params_t H5VL_DAOS_UNUSED *dst_loc_params,
                           const char *dst_name, unsigned obj_copy_options, hid_t lcpl_id,
                           H5_DAOS_ATTR_EXISTS_OUT_TYPE **link_exists_p, H5_daos_req_t *req,
                           tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_ud_t *obj_copy_udata = NULL;
    H5VL_loc_params_t         sub_loc_params;
    tse_task_t               *copy_task           = NULL;
    hbool_t                   copy_task_scheduled = FALSE;
    H5_daos_req_t            *int_int_req         = NULL;
    int                       ret;
    herr_t                    ret_value = SUCCEED;

    assert(src_loc_obj);
    assert(src_loc_params);
    assert(src_name);
    assert(dst_loc_obj);
    assert(dst_loc_params);
    assert(dst_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Set up user data for object copy */
    if (NULL == (obj_copy_udata = (H5_daos_object_copy_ud_t *)DV_malloc(sizeof(H5_daos_object_copy_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "failed to allocate buffer for object copy user data");
    obj_copy_udata->req                   = req;
    obj_copy_udata->obj_copy_metatask     = NULL;
    obj_copy_udata->src_obj               = NULL;
    obj_copy_udata->dst_grp               = NULL;
    obj_copy_udata->copied_obj            = NULL;
    obj_copy_udata->new_obj_name          = NULL;
    obj_copy_udata->new_obj_name_len      = 0;
    obj_copy_udata->new_obj_name_path_buf = NULL;
    obj_copy_udata->obj_copy_options      = obj_copy_options;
    obj_copy_udata->lcpl_id               = H5P_LINK_CREATE_DEFAULT;
    if (H5P_LINK_CREATE_DEFAULT != lcpl_id)
        if ((obj_copy_udata->lcpl_id = H5Pcopy(lcpl_id)) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy LCPL");
    if (link_exists_p) {
        obj_copy_udata->dst_link_exists = TRUE;
        *link_exists_p                  = &obj_copy_udata->dst_link_exists;
    } /* end if */
    else
        obj_copy_udata->dst_link_exists = FALSE;

    /* Start internal H5 operation for source object open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(((H5_daos_item_t *)src_loc_obj)->file,
                                                  "source object open within object copy", NULL, NULL, req,
                                                  H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Open the source object */
    sub_loc_params.type                         = H5VL_OBJECT_BY_NAME;
    sub_loc_params.obj_type                     = src_loc_params->obj_type;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    sub_loc_params.loc_data.loc_by_name.name    = src_name;
    if (H5_daos_object_open_helper(src_loc_obj, &sub_loc_params, NULL, FALSE, NULL, &obj_copy_udata->src_obj,
                                   int_int_req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "failed to open source object");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = int_int_req->finalize_task;
    *dep_task   = int_int_req->finalize_task;
    int_int_req = NULL;

    /* Traverse path to destination group */
    if (NULL == (obj_copy_udata->dst_grp = (H5_daos_group_t *)H5_daos_group_traverse(
                     dst_loc_obj, dst_name, lcpl_id, req, FALSE, &obj_copy_udata->new_obj_name_path_buf,
                     &obj_copy_udata->new_obj_name, &obj_copy_udata->new_obj_name_len, first_task, dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't traverse path");

    /* Check type of target_obj */
    if (obj_copy_udata->dst_grp->obj.item.type != H5I_GROUP)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "target object is not a group");

    if (obj_copy_udata->new_obj_name_len == 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL,
                     "can't copy new object with same name as destination group");

    /* Create task for object copy */
    if (H5_daos_create_task(H5_daos_object_copy_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, obj_copy_udata, &copy_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to copy object");

    /* Schedule object copy task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(copy_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to copy object: %s",
                         H5_daos_err_to_string(ret));
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
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 1, &copy_task, NULL, NULL, NULL,
                            &obj_copy_udata->obj_copy_metatask) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object copy");

    /* Schedule meta task */
    assert(*first_task);
    if (0 != (ret = tse_task_schedule(obj_copy_udata->obj_copy_metatask, false)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object copy: %s",
                     H5_daos_err_to_string(ret));

    *dep_task = obj_copy_udata->obj_copy_metatask;

    /* Create final task to free object copy udata after copying has finished */
    if (H5_daos_object_copy_free_copy_udata(obj_copy_udata, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to free object copying data");

    /* Relinquish control of the object copy udata to task */
    obj_copy_udata = NULL;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (obj_copy_udata->dst_grp && H5_daos_group_close_real(obj_copy_udata->dst_grp) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");

        if (!copy_task_scheduled) {
            if (obj_copy_udata->new_obj_name_path_buf)
                DV_free(obj_copy_udata->new_obj_name_path_buf);
            if (H5P_LINK_CREATE_DEFAULT != obj_copy_udata->lcpl_id)
                if (obj_copy_udata->lcpl_id >= 0 && H5Pclose(obj_copy_udata->lcpl_id) < 0)
                    D_DONE_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "can't close LCPL");

            obj_copy_udata = DV_free(obj_copy_udata);
        } /* end if */

        /* Close internal request for target object open */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_task
 *
 * Purpose:     Asynchronous task to copy a DAOS HDF5 object.  This task
 *              exists in the source object's scheduler.
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
    tse_task_t               *first_task = NULL;
    tse_task_t               *dep_task   = NULL;
    int                       ret;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object copy task");

    assert(udata->req);
    assert(udata->obj_copy_metatask);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    /* Check if the source object was missing (for example, tried to open
     * the source object through a dangling soft link.
     */
    if (!udata->src_obj)
        D_GOTO_ERROR(H5E_OBJECT, H5E_NOTFOUND, -H5_DAOS_H5_OPEN_ERROR,
                     "failed to open source object to copy");

    /* Check for destination object already exists */
    if (udata->dst_link_exists)
        D_GOTO_ERROR(H5E_OBJECT, H5E_ALREADYEXISTS, -H5_DAOS_LINK_EXISTS,
                     "source object/link already exists at specified destination location object/destination "
                     "name pair");

    /* Determine object copying routine to call */
    switch (udata->src_obj->item.type) {
        case H5I_FILE:
        case H5I_GROUP:
            if (H5_daos_group_copy(udata, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "can't copy group");
            break;
        case H5I_DATATYPE:
            if (H5_daos_datatype_copy(udata, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "can't copy datatype");
            break;
        case H5I_DATASET:
            if (H5_daos_dataset_copy(udata, udata->req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "can't copy dataset");
            break;
        case H5I_MAP:
            /* TODO: Add map copying support */
            D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR,
                         "map copying is unsupported");
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
    if (dep_task && 0 != (ret = tse_task_register_deps(udata->obj_copy_metatask, 1, &dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't create dependencies for object copy metatask: %s",
                     H5_daos_err_to_string(ret));

    /* Relinquish control of the object copying udata to the new task. */
    udata = NULL;

done:
    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to copy object: %s",
                     H5_daos_err_to_string(ret));

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (udata && ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "object copy task";
    } /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
H5_daos_object_copy_free_copy_udata(H5_daos_object_copy_ud_t *copy_udata, tse_task_t **first_task,
                                    tse_task_t **dep_task)
{
    tse_task_t *free_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(copy_udata);
    assert(first_task);
    assert(dep_task);

    /* Create task for freeing udata */
    if (H5_daos_create_task(H5_daos_object_copy_free_copy_udata_task, *dep_task ? 1 : 0,
                            *dep_task ? dep_task : NULL, NULL, NULL, copy_udata, &free_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to free object copying udata");

    /* Schedule object copying udata free task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(free_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                         "can't schedule task to free object copying udata: %s", H5_daos_err_to_string(ret));
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
 *              after the copying has completed.  This task exists in the
 *              source object's scheduler.
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
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for task to free object copying udata");

    assert(udata->req);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ_DONE(udata->req);

    /* Close source object that was opened during copying */
    if (udata->src_obj && H5_daos_object_close(&udata->src_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Close destination group that was opened during copying */
    if (udata->dst_grp && H5_daos_group_close_real(udata->dst_grp) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

    /* Close copied object */
    if (udata->copied_obj && H5_daos_object_close(&udata->copied_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close newly-copied object");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "object copying udata free task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free path buffer allocated by H5_daos_group_traverse */
    if (udata->new_obj_name_path_buf) {
        DV_free(udata->new_obj_name_path_buf);
        udata->new_obj_name_path_buf = NULL;
    }

    /* Free private data */
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj, H5_daos_req_t *req,
                               tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_attributes_ud_t *attr_copy_ud = NULL;
    H5_daos_iter_data_t                  iter_data;
    H5_index_t                           iter_index_type;
    herr_t                               ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (attr_copy_ud = (H5_daos_object_copy_attributes_ud_t *)DV_malloc(
                     sizeof(H5_daos_object_copy_attributes_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for attribute copy task");
    attr_copy_ud->req        = req;
    attr_copy_ud->target_obj = dst_obj;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether attribute creation order is tracked for the object.
     */
    iter_index_type = (src_obj->ocpl_cache.track_acorder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data. Attributes are re-created by creation order if possible */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, iter_index_type, H5_ITER_INC, FALSE, NULL,
                           H5I_INVALID_HID, attr_copy_ud, NULL, req);
    iter_data.async_op                              = TRUE;
    iter_data.u.attr_iter_data.u.attr_iter_op_async = H5_daos_object_copy_attributes_cb;

    if (H5_daos_attribute_iterate(src_obj, &iter_data, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "can't iterate over object's attributes");

    if (H5_daos_free_async(attr_copy_ud, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to free attribute copying data");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_attributes() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_attributes_cb
 *
 * Purpose:     Attribute iteration callback to copy a single attribute
 *              from one DAOS object to another.  The current scheduler
 *              will always be the source file's on entry, and must be on
 *              exit.
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
    H5_daos_object_copy_attributes_ud_t *copy_ud     = (H5_daos_object_copy_attributes_ud_t *)op_data;
    H5_daos_obj_t                       *src_loc_obj = NULL;
    herr_t                               ret_value   = H5_ITER_CONT;

    assert(copy_ud);

    if (NULL == (src_loc_obj = H5VLobject(location_id)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR,
                     "failed to retrieve VOL object for source location ID");

    if (H5_daos_object_copy_single_attribute(src_loc_obj, attr_name, copy_ud->target_obj, copy_ud->req,
                                             first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, H5_ITER_ERROR, "can't create task to copy single attribute");

done:
    *op_ret = ret_value;

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_attributes_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_single_attribute
 *
 * Purpose:     Creates an asynchronous task to copy a single attribute
 *              to a target object during object copying.  All tasks are
 *              scheduled in the source scheduler.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_single_attribute(H5_daos_obj_t *src_obj, const char *attr_name, H5_daos_obj_t *target_obj,
                                     H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_single_attribute_ud_t *attr_copy_ud = NULL;
    H5VL_loc_params_t                          sub_loc_params;
    H5_daos_req_t                             *int_int_req = NULL;
    int                                        ret;
    herr_t                                     ret_value = SUCCEED;

    assert(src_obj);
    assert(attr_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (attr_copy_ud = (H5_daos_object_copy_single_attribute_ud_t *)DV_malloc(
                     sizeof(H5_daos_object_copy_single_attribute_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for attribute copy task");
    attr_copy_ud->req        = req;
    attr_copy_ud->src_attr   = NULL;
    attr_copy_ud->target_obj = target_obj;

    sub_loc_params.obj_type = src_obj->item.type;
    sub_loc_params.type     = H5VL_OBJECT_BY_SELF;

    /* Start internal H5 operation for target attribute open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL ==
        (int_int_req = H5_daos_req_create(src_obj->item.file, "source attribute open within object copy",
                                          NULL, NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    if (NULL == (attr_copy_ud->src_attr = H5_daos_attribute_open_helper(
                     (H5_daos_item_t *)src_obj, &sub_loc_params, attr_name, H5P_ATTRIBUTE_ACCESS_DEFAULT,
                     FALSE, int_int_req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "failed to open attribute");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = int_int_req->finalize_task;
    *dep_task   = int_int_req->finalize_task;
    int_int_req = NULL;

    if (H5_daos_create_task(H5_daos_object_copy_single_attribute_task, *dep_task ? 1 : 0,
                            *dep_task ? dep_task : NULL, NULL, NULL, attr_copy_ud,
                            &attr_copy_ud->copy_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to copy attribute");

    /* Schedule attribute copy task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(attr_copy_ud->copy_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to copy attribute: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = attr_copy_ud->copy_task;
    req->rc++;
    target_obj->item.rc++;
    *dep_task = attr_copy_ud->copy_task;

    /* Relinquish control of attribute copy udata to task. */
    attr_copy_ud = NULL;

done:
    if (ret_value < 0) {
        /* Close internal request for target object open */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

        DV_free(attr_copy_ud);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_object_copy_single_attribute() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_single_attribute_task
 *
 * Purpose:     Asynchronous task to copy a single attribute to a target
 *              object during object copying.  This exists within the
 *              source object's scheduler, but all scheduled tasks will
 *              be in the destination file's scheduler.
 *
 * Return:      Success:        0
 *              Failure:        Negative error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_copy_single_attribute_task(tse_task_t *task)
{
    H5_daos_object_copy_single_attribute_ud_t *udata;
    H5VL_loc_params_t                          sub_loc_params;
    H5_daos_req_t                             *req         = NULL;
    tse_task_t                                *free_task   = NULL;
    tse_task_t                                *first_task  = NULL;
    tse_task_t                                *dep_task    = NULL;
    H5_daos_req_t                             *int_int_req = NULL;
    int                                        ret;
    int                                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute copying task");

    assert(udata->req);
    assert(udata->src_attr);
    assert(udata->target_obj);
    assert(udata->copy_task == task);

    /* Assign convenience pointer to req and take a reference to it */
    req = udata->req;
    req->rc++;

    /* Check for previous errors */
    H5_DAOS_PREP_REQ_PROG(req);

    /* Start internal H5 operation for target attribute open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(udata->target_obj->item.file,
                                                  "destination attribute create within object copy", NULL,
                                                  NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't create DAOS request");

    /* Create task for creating new attribute now that the
     * source attribute should be valid.
     */
    sub_loc_params.obj_type = udata->target_obj->item.type;
    sub_loc_params.type     = H5VL_OBJECT_BY_SELF;
    if (NULL == (udata->new_attr = H5_daos_attribute_create_helper(
                     &udata->target_obj->item, &sub_loc_params, udata->src_attr->type_id,
                     udata->src_attr->space_id, udata->src_attr->acpl_id, H5P_ATTRIBUTE_ACCESS_DEFAULT,
                     udata->src_attr->name, FALSE, int_int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, -H5_DAOS_H5_COPY_ERROR, "failed to create new attribute");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                     "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        first_task = int_int_req->finalize_task;
    dep_task    = int_int_req->finalize_task;
    int_int_req = NULL;

done:
    if (udata) {
        /* Create task to free attribute copying udata after copying is finished */
        if (H5_daos_create_task(H5_daos_object_copy_single_attribute_free_udata_task, dep_task ? 1 : 0,
                                dep_task ? &dep_task : NULL, NULL, NULL, udata, &free_task) < 0) {
            tse_task_complete(task, ret_value);
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to free attribute copying data");
        }
        else {
            /* Schedule attribute copying data free task (or save it to be scheduled
             * later) and transfer ownership reference of udata */
            if (first_task) {
                if (0 != (ret = tse_task_schedule(free_task, false))) {
                    /* Return task to task list */
                    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
                        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                     "can't return task to task list");
                    tse_task_complete(task, ret_value);
                    D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                 "can't schedule task to free attribute copying data: %s",
                                 H5_daos_err_to_string(ret));
                } /* end if */
            }     /* end if */
            else
                first_task = free_task;
            dep_task = free_task;
            udata    = NULL;
        } /* end else */

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = "single attribute copy task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */

    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't schedule task to copy attribute: %s",
                     H5_daos_err_to_string(ret));

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

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
    int                                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for task to free attribute copying udata");

    assert(udata->src_attr);
    assert(udata->req);
    assert(udata->target_obj);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ_DONE(udata->req);

    /* Close source attribute */
    if (H5_daos_attribute_close_real(udata->src_attr) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

    /* Close newly-copied attribute */
    if (H5_daos_attribute_close_real(udata->new_attr) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

    /* Close object */
    if (H5_daos_object_close(&udata->target_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "attribute copying udata free task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, udata->copy_task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete copy task */
    tse_task_complete(udata->copy_task, ret_value);

    /* Free private data */
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
H5_daos_group_copy(H5_daos_object_copy_ud_t *obj_copy_udata, H5_daos_req_t *req, tse_task_t **first_task,
                   tse_task_t **dep_task)
{
    H5_daos_iter_data_t iter_data;
    H5_daos_group_t    *src_grp;
    H5_index_t          iter_index_type;
    hid_t               target_obj_id = H5I_INVALID_HID;
    herr_t              ret_value     = SUCCEED;

    assert(obj_copy_udata);
    assert(obj_copy_udata->req);
    assert(obj_copy_udata->obj_copy_metatask);
    assert(obj_copy_udata->src_obj);
    assert(obj_copy_udata->new_obj_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    src_grp = (H5_daos_group_t *)obj_copy_udata->src_obj;

    /* Copy the group */
    if (NULL == (obj_copy_udata->copied_obj = (H5_daos_obj_t *)H5_daos_group_copy_helper(
                     src_grp, obj_copy_udata->dst_grp, obj_copy_udata->new_obj_name,
                     obj_copy_udata->obj_copy_options, req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "can't copy group");

    /* Now copy the immediate members of the group to the new group. If the
     * H5O_COPY_SHALLOW_HIERARCHY_FLAG flag wasn't specified, this will also
     * recursively copy the members of any subgroups found. */

    /* Register an ID for the group to iterate over */
    if ((target_obj_id = H5VLwrap_register(obj_copy_udata->src_obj, H5I_GROUP)) < 0)
        D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
    obj_copy_udata->src_obj->item.rc++;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether creation order is tracked for the group.
     */
    iter_index_type = (src_grp->gcpl_cache.track_corder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, iter_index_type, H5_ITER_INC, FALSE, NULL,
                           target_obj_id, obj_copy_udata, NULL, req);
    iter_data.async_op                              = TRUE;
    iter_data.u.link_iter_data.u.link_iter_op_async = H5_daos_group_copy_cb;

    if (H5_daos_link_iterate(src_grp, &iter_data, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over group's links");

done:
    /* Release reference to group since link iteration task should own it now.
     * No need to mark as nonblocking close since the ID rc shouldn't drop to 0.
     */
    if (target_obj_id >= 0 && H5Idec_ref(target_obj_id) < 0)
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
static H5_daos_group_t *
H5_daos_group_copy_helper(H5_daos_group_t *src_grp, H5_daos_group_t *dst_grp, const char *name,
                          unsigned obj_copy_options, H5_daos_req_t *req, tse_task_t **first_task,
                          tse_task_t **dep_task)
{
    H5_daos_group_t *copied_group = NULL;
    H5_daos_req_t   *int_int_req  = NULL;
    int              ret;
    H5_daos_group_t *ret_value = NULL;

    assert(src_grp);
    assert(dst_grp);
    assert(name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Start internal H5 operation for target object open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req =
                     H5_daos_req_create(dst_grp->obj.item.file, "destination group create within object copy",
                                        NULL, NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request");

    /* Copy the group */
    if (NULL == (copied_group = H5_daos_group_create_helper(dst_grp->obj.item.file, FALSE, src_grp->gcpl_id,
                                                            src_grp->gapl_id, dst_grp, name, strlen(name),
                                                            FALSE, int_int_req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "can't create new group");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = int_int_req->finalize_task;
    *dep_task   = int_int_req->finalize_task;
    int_int_req = NULL;

    /* If the "without attribute copying" flag hasn't been specified,
     * create a task to copy the group's attributes as well.
     */
    if ((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if (H5_daos_object_copy_attributes((H5_daos_obj_t *)src_grp, (H5_daos_obj_t *)copied_group, req,
                                           first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "can't copy group's attributes");

    ret_value = copied_group;

done:
    /* Close internal request for target object create */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't free request");

    if (!ret_value && copied_group)
        if (H5_daos_group_close_real(copied_group) < 0)
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
 *              *dep_task exists within the source scheduler on entry and
 *              must also on exit.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_copy_cb(hid_t group, const char *name, const H5L_info2_t *info, void *op_data, herr_t *op_ret,
                      tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_copy_ud_t *obj_copy_udata = (H5_daos_object_copy_ud_t *)op_data;
    H5VL_loc_params_t         sub_loc_params;
    H5_daos_group_t          *copied_group = NULL;
    herr_t                    ret_value    = H5_ITER_CONT;

    assert(first_task);
    assert(dep_task);

    /* Silence compiler for unused parameter */
    (void)group;

    sub_loc_params.type                         = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name    = name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    switch (info->type) {
        case H5L_TYPE_HARD: {
            daos_obj_id_t oid;
            H5I_type_t    obj_type;

            /* Determine the type of object pointed to by the link */
            if (H5_daos_token_to_oid(&info->u.token, &oid) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTDECODE, H5_ITER_ERROR, "can't convert object token to OID");
            obj_type = H5_daos_oid_to_type(oid);
            if (H5I_BADID == obj_type || H5I_UNINIT == obj_type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, H5_ITER_ERROR, "invalid object type");

            /*
             * If performing a shallow group copy, copy the group without its immediate members.
             * Otherwise, continue on with a normal recursive object copy.
             */
            if ((obj_type == H5I_GROUP) &&
                (obj_copy_udata->obj_copy_options & H5O_COPY_SHALLOW_HIERARCHY_FLAG)) {
                if (NULL ==
                    (copied_group = H5_daos_group_copy_helper((H5_daos_group_t *)obj_copy_udata->src_obj,
                                                              (H5_daos_group_t *)obj_copy_udata->copied_obj,
                                                              name, obj_copy_udata->obj_copy_options,
                                                              obj_copy_udata->req, first_task, dep_task)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR,
                                 "failed to perform shallow copy of group");

                /* Close group now that copying task owns it */
                if (H5_daos_group_close_real(copied_group) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close group");
            }
            else {
                if (H5_daos_object_copy_helper(obj_copy_udata->src_obj, &sub_loc_params, name,
                                               obj_copy_udata->copied_obj, &sub_loc_params, name,
                                               obj_copy_udata->obj_copy_options, obj_copy_udata->lcpl_id,
                                               NULL, obj_copy_udata->req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy object");
            } /* end else */

            break;
        } /* H5L_TYPE_HARD */

        case H5L_TYPE_SOFT: {
            /*
             * If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was specified,
             * expand the soft link into a new object. Otherwise, the link
             * will be copied as-is.
             */
            if (obj_copy_udata->obj_copy_options & H5O_COPY_EXPAND_SOFT_LINK_FLAG) {
                /* Copy the object */
                if (H5_daos_object_copy_helper(obj_copy_udata->src_obj, &sub_loc_params, name,
                                               obj_copy_udata->copied_obj, &sub_loc_params, name,
                                               obj_copy_udata->obj_copy_options, obj_copy_udata->lcpl_id,
                                               NULL, obj_copy_udata->req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy object");
            } /* end if */
            else {
                /* Copy the link as is */
                if (H5_daos_link_copy_move_int((H5_daos_item_t *)obj_copy_udata->src_obj, &sub_loc_params,
                                               (H5_daos_item_t *)obj_copy_udata->copied_obj, &sub_loc_params,
                                               obj_copy_udata->lcpl_id, FALSE, FALSE, obj_copy_udata->req,
                                               first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy link");
            } /* end else */

            break;
        } /* H5L_TYPE_SOFT */

        case H5L_TYPE_EXTERNAL: {
            /*
             * If the H5O_COPY_EXPAND_EXT_LINK_FLAG flag was specified,
             * expand the external link into a new object. Otherwise, the
             * link will be copied as-is.
             */
            if (obj_copy_udata->obj_copy_options & H5O_COPY_EXPAND_EXT_LINK_FLAG) {
                D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, H5_ITER_ERROR,
                             "H5O_COPY_EXPAND_EXT_LINK_FLAG flag is currently unsupported");
            } /* end if */
            else {
                /* Copy the link as is */
                if (H5_daos_link_copy_move_int((H5_daos_item_t *)obj_copy_udata->src_obj, &sub_loc_params,
                                               (H5_daos_item_t *)obj_copy_udata->dst_grp, &sub_loc_params,
                                               obj_copy_udata->lcpl_id, FALSE, FALSE, obj_copy_udata->req,
                                               first_task, dep_task) < 0)
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
    if (ret_value < 0 && copied_group)
        if (H5_daos_group_close_real(copied_group) < 0)
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
H5_daos_datatype_copy(H5_daos_object_copy_ud_t *obj_copy_udata, H5_daos_req_t *req, tse_task_t **first_task,
                      tse_task_t **dep_task)
{
    H5_daos_dtype_t *src_dtype;
    H5_daos_req_t   *int_int_req = NULL;
    int              ret;
    herr_t           ret_value = SUCCEED;

    assert(obj_copy_udata);
    assert(obj_copy_udata->req);
    assert(obj_copy_udata->obj_copy_metatask);
    assert(obj_copy_udata->src_obj);
    assert(obj_copy_udata->new_obj_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    src_dtype = (H5_daos_dtype_t *)obj_copy_udata->src_obj;

    /* Start internal H5 operation for target object open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(obj_copy_udata->dst_grp->obj.item.file,
                                                  "destination datatype create within object copy", NULL,
                                                  NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Copy the datatype */
    if (NULL == (obj_copy_udata->copied_obj = H5_daos_datatype_commit_helper(
                     obj_copy_udata->dst_grp->obj.item.file, src_dtype->type_id, src_dtype->tcpl_id,
                     src_dtype->tapl_id, obj_copy_udata->dst_grp, obj_copy_udata->new_obj_name,
                     strlen(obj_copy_udata->new_obj_name), FALSE, int_int_req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't commit new datatype");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = int_int_req->finalize_task;
    *dep_task   = int_int_req->finalize_task;
    int_int_req = NULL;

    /* If the "without attribute copying" flag hasn't been specified,
     * create a task to copy the datatype's attributes as well.
     */
    if ((obj_copy_udata->obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if (H5_daos_object_copy_attributes((H5_daos_obj_t *)src_dtype, obj_copy_udata->copied_obj, req,
                                           first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't copy datatype's attributes");

done:
    /* Close internal request for target object create */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't free request");

    if (ret_value < 0 && obj_copy_udata->copied_obj)
        if (H5_daos_datatype_close_real((H5_daos_dtype_t *)obj_copy_udata->copied_obj) < 0)
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
H5_daos_dataset_copy(H5_daos_object_copy_ud_t *obj_copy_udata, H5_daos_req_t *req, tse_task_t **first_task,
                     tse_task_t **dep_task)
{
    H5_daos_dset_t *src_dset;
    H5_daos_req_t  *int_int_req = NULL;
    int             ret;
    herr_t          ret_value = SUCCEED;

    assert(obj_copy_udata);
    assert(obj_copy_udata->req);
    assert(obj_copy_udata->obj_copy_metatask);
    assert(obj_copy_udata->src_obj);
    assert(obj_copy_udata->new_obj_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    src_dset = (H5_daos_dset_t *)obj_copy_udata->src_obj;

    /* Start internal H5 operation for target object open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(obj_copy_udata->dst_grp->obj.item.file,
                                                  "destination dataset create within object copy", NULL, NULL,
                                                  req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Copy the dataset */
    if (NULL ==
        (obj_copy_udata->copied_obj = H5_daos_dataset_create_helper(
             obj_copy_udata->dst_grp->obj.item.file, src_dset->type_id, src_dset->space_id, src_dset->dcpl_id,
             src_dset->dapl_id, obj_copy_udata->dst_grp, obj_copy_udata->new_obj_name,
             strlen(obj_copy_udata->new_obj_name), FALSE, int_int_req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't create new dataset");

    /* Create task to finalize internal operation */
    if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL, NULL,
                            int_int_req, &int_int_req->finalize_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

    /* Schedule finalize task (or save it to be scheduled later),
     * give it ownership of int_int_req, and update task pointers */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = int_int_req->finalize_task;
    *dep_task   = int_int_req->finalize_task;
    int_int_req = NULL;

    /* Copy all data from the source dataset to the new dataset */
    if (H5_daos_dataset_copy_data(src_dset, (H5_daos_dset_t *)obj_copy_udata->copied_obj, req, first_task,
                                  dep_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataset data");

    /* If the "without attribute copying" flag hasn't been specified,
     * create a task to copy the dataset's attributes as well.
     */
    if ((obj_copy_udata->obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if (H5_daos_object_copy_attributes((H5_daos_obj_t *)src_dset, obj_copy_udata->copied_obj, req,
                                           first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataset's attributes");

done:
    /* Close internal request for target object create */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free request");

    if (ret_value < 0 && obj_copy_udata->copied_obj)
        if (H5_daos_dataset_close_real((H5_daos_dset_t *)obj_copy_udata->copied_obj) < 0)
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
H5_daos_dataset_copy_data(H5_daos_dset_t *src_dset, H5_daos_dset_t *dst_dset, H5_daos_req_t *req,
                          tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_dataset_copy_data_ud_t *copy_ud = NULL;
    int                             ret;
    herr_t                          ret_value = SUCCEED;

    assert(src_dset);
    assert(dst_dset);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL ==
        (copy_ud = (H5_daos_dataset_copy_data_ud_t *)DV_malloc(sizeof(H5_daos_dataset_copy_data_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for dataset data copy task");
    copy_ud->req      = req;
    copy_ud->src_dset = src_dset;
    copy_ud->dst_dset = dst_dset;
    copy_ud->data_buf = NULL;

    /* Create task for dataset data copy */
    if (H5_daos_create_task(H5_daos_dataset_copy_data_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, copy_ud, &copy_ud->data_copy_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create task to copy dataset data");

    /* Schedule dataset data copy task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(copy_ud->data_copy_task, false)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't schedule task to copy dataset data: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = copy_ud->data_copy_task;
    req->rc++;
    src_dset->obj.item.rc++;
    dst_dset->obj.item.rc++;
    *dep_task = copy_ud->data_copy_task;

done:
    if (ret_value < 0) {
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
 *              This task exists in the source file's scheduler.
 *
 * Return:      Success:        0
 *              Failure:        Negative error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dataset_copy_data_task(tse_task_t *task)
{
    H5_daos_dataset_copy_data_ud_t *udata;
    hssize_t                        fspace_nelements = 0;
    size_t                          buf_size         = 0;
    H5_daos_req_t                  *req              = NULL;
    tse_task_t                     *first_task       = NULL;
    tse_task_t                     *dep_task         = NULL;
    int                             ret;
    int                             ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task))) {
        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");
        tse_task_complete(task, -H5_DAOS_DAOS_GET_ERROR);
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset data copy task");
    } /* end if */

    assert(udata->req);
    assert(udata->src_dset);
    assert(udata->dst_dset);
    assert(task == udata->data_copy_task);

    /* Assign req convenience pointer.  We do this so we can still handle errors
     * after transferring ownership of udata.  This should be safe since we
     * increment the reference count on req when we transfer ownership of udata.
     */
    req = udata->req;

    /* Check for previous errors */
    H5_DAOS_PREP_REQ_PROG(udata->req);

    /* Calculate size needed to store entirety of dataset's data */
    if ((fspace_nelements = H5Sget_simple_extent_npoints(udata->src_dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR,
                     "can't get number of elements in source dataset's dataspace");
    if (0 == (buf_size = H5Tget_size(udata->src_dset->type_id)))
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR,
                     "can't get source dataset's datatype size");
    buf_size *= (size_t)fspace_nelements;

    /* Allocate buffer */
    if (NULL == (udata->data_buf = DV_malloc(buf_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                     "can't allocate data buffer for dataset data copy");

    /* Read data from source */
    if (H5_daos_dataset_read_int(udata->src_dset, udata->src_dset->type_id, H5S_ALL, H5S_ALL, FALSE,
                                 udata->data_buf, NULL, udata->req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, -H5_DAOS_DAOS_GET_ERROR,
                     "can't read data from source dataset");
    assert(dep_task);

    /* Write data to destination */
    if (H5_daos_dataset_write_int(udata->dst_dset, udata->src_dset->type_id, H5S_ALL, H5S_ALL, FALSE,
                                  udata->data_buf, NULL, udata->req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, -H5_DAOS_H5_COPY_ERROR,
                     "can't write data to copied dataset");

done:
    /* Check for tasks scheduled, in this case we need to schedule a task to
     * mark this task complete and free udata */
    if (dep_task) {
        tse_task_t *end_task;

        assert(udata);

        /* Schedule task to finish this operation */
        if (H5_daos_create_task(H5_daos_dset_copy_data_end_task, 1, &dep_task, NULL, NULL, udata, &end_task) <
            0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to finish data copy");
        else {
            /* Schedule end task and give it ownership of udata */
            assert(first_task);
            req->rc++;
            if (0 != (ret = tse_task_schedule(end_task, false)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule task to data copy: %s",
                             H5_daos_err_to_string(ret));
            udata    = NULL;
            dep_task = end_task;
        } /* end else */

        /* Schedule first task */
        assert(first_task);
        if (0 != (ret = tse_task_schedule(first_task, false)))
            D_DONE_ERROR(H5E_DATASET, H5E_CANTINIT, ret, "can't schedule initial task for data copy: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        assert(!first_task);

    if (req) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = "dataset data copy task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete task if we still own udata */
    if (udata) {
        assert(ret_value < 0);
        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");
        tse_task_complete(task, ret_value);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_dataset_copy_data_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dset_copy_data_end_task
 *
 * Purpose:     Asynchronous task to release resources used for dataset
 *              data copy.
 *
 * Return:      Success:        0
 *              Failure:        Negative error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_dset_copy_data_end_task(tse_task_t *task)
{
    H5_daos_dataset_copy_data_ud_t *udata;
    htri_t                          is_vl_ref;
    int                             ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for task");

    /* Check for vlen or reference type */
    if ((is_vl_ref = H5_daos_detect_vl_vlstr_ref(udata->src_dset->type_id)) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CANTGET, -H5_DAOS_H5_TCONV_ERROR,
                     "can't check for vl or reference type");

    /* If there's a vlen or reference type, reclaim any memory in the buffer */
    if (is_vl_ref && H5Treclaim(udata->src_dset->type_id, udata->src_dset->space_id, udata->req->dxpl_id,
                                udata->data_buf) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CANTGC, -H5_DAOS_FREE_ERROR,
                     "can't reclaim memory from fill value conversion buffer");

    /* Close datasets */
    if (H5_daos_dataset_close_real(udata->src_dset) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

    if (H5_daos_dataset_close_real(udata->dst_dset) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close dataset");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except
     * for H5_daos_req_free_int, which updates req->status if it sees an
     * error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "dataset data copy end task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free data buffer */
    if (udata->data_buf)
        DV_free(udata->data_buf);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, udata->data_copy_task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete data copy task */
    tse_task_complete(udata->data_copy_task, ret_value);

    /* Free private data */
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_dset_copy_data_end_task() */

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
H5_daos_object_get(void *_item, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *get_args,
                   hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item        = (H5_daos_item_t *)_item;
    H5_daos_obj_t  *target_obj  = NULL;
    H5_daos_req_t  *int_req     = NULL;
    H5_daos_req_t  *int_int_req = NULL;
    tse_task_t     *first_task  = NULL;
    tse_task_t     *dep_task    = NULL;
    int             ret;
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL");
    if (!get_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "object get", item->open_req, NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    switch (get_args->op_type) {
        case H5VL_OBJECT_GET_FILE: {
            void **ret_file = get_args->args.get_file.file;

            if (H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL,
                             "unsupported object operation location parameters type");

            *ret_file = item->file;

            break;
        } /* H5VL_OBJECT_GET_FILE */

        case H5VL_OBJECT_GET_NAME:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported object get operation");
            break;

        case H5VL_OBJECT_GET_TYPE: {
            daos_obj_id_t oid;
            H5O_type_t   *obj_type = get_args->args.get_type.obj_type;
            H5I_type_t    obj_itype;

            int_req->op_name = "get object type";

            if (H5VL_OBJECT_BY_TOKEN != loc_params->type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL,
                             "unsupported object operation location parameters type");

            /* Retrieve the OID of the referenced object and then determine the object's type */
            if (H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't convert object token to OID");
            if (H5I_BADID == (obj_itype = H5_daos_oid_to_type(oid)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get object type");

            switch (obj_itype) {
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
        case H5VL_OBJECT_GET_INFO: {
            H5O_info2_t     *oinfo        = get_args->args.get_info.oinfo;
            unsigned         fields       = get_args->args.get_info.fields;
            H5_daos_obj_t ***target_obj_p = NULL;

            int_req->op_name = "get object info";

            /* Determine target object */
            switch (loc_params->type) {
                case H5VL_OBJECT_BY_SELF:
                    /* Use item as attribute parent object, or the root group if
                     * item is a file */
                    if (item->type == H5I_FILE)
                        target_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
                    else
                        target_obj = (H5_daos_obj_t *)item;
                    target_obj->item.rc++;

                    break;

                case H5VL_OBJECT_BY_NAME:
                case H5VL_OBJECT_BY_IDX:
                    /* Start internal H5 operation for target object open.  This will
                     * not be visible to the API, will not be added to an operation
                     * pool, and will be integrated into this function's task chain. */
                    if (NULL == (int_int_req = H5_daos_req_create(
                                     item->file, "target object open within object get info by name/index",
                                     NULL, NULL, int_req, H5I_INVALID_HID)))
                        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

                    /* Open target object */
                    if (H5_daos_object_open_helper(item, loc_params, NULL, TRUE, &target_obj_p, NULL,
                                                   int_int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open object");

                    /* Create task to finalize internal operation */
                    if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0,
                                            dep_task ? &dep_task : NULL, NULL, NULL, int_int_req,
                                            &int_int_req->finalize_task) < 0)
                        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                                     "can't create task to finalize internal operation");

                    /* Schedule finalize task (or save it to be scheduled later),
                     * give it ownership of int_int_req, and update task pointers */
                    if (first_task) {
                        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                                         "can't schedule task to finalize H5 operation: %s",
                                         H5_daos_err_to_string(ret));
                    } /* end if */
                    else
                        first_task = int_int_req->finalize_task;
                    dep_task    = int_int_req->finalize_task;
                    int_int_req = NULL;

                    break;

                default:
                    D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL,
                                 "unsupported object operation location parameters type");
            } /* end switch */

            if (H5_daos_object_get_info(target_obj_p, NULL, target_obj, fields, oinfo, int_req, &first_task,
                                        &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't retrieve info for object");

            break;
        } /* H5VL_OBJECT_GET_INFO */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object get operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_OBJ, FALSE,
                                !req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL,
                             "object get operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    if (target_obj) {
        if (H5_daos_object_close(&target_obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object");
        target_obj = NULL;
    } /* end if */

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_lookup_task
 *
 * Purpose:     Asynchronous task for object token lookup.
 *
 * Return:      Success:        0
 *              Failure:        Negative error code
 *
 * Programmer:  Neil Fortner
 *              June, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_lookup_task(tse_task_t *task)
{
    H5_daos_object_lookup_ud_t *udata     = NULL;
    int                         ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object specific operation task");

    assert(udata->req);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    assert(udata->target_obj);
    assert(udata->token);

    /* Get token */
    if (H5_daos_oid_to_token(udata->target_obj->oid, udata->token) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTENCODE, -H5_DAOS_H5_ENCODE_ERROR,
                     "can't convert OID to object token");

done:
    /* Clean up */
    if (udata) {
        if (udata->target_obj) {
            if (H5_daos_object_close(&udata->target_obj->item) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");
            udata->target_obj = NULL;
        } /* end if */

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "object lookup task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_lookup_task() */

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
                        H5VL_object_specific_args_t *specific_args, hid_t dxpl_id, void **req)
{
    H5_daos_item_t             *item                 = (H5_daos_item_t *)_item;
    H5_daos_object_lookup_ud_t *lookup_udata         = NULL;
    H5_daos_obj_t            ***target_obj_p         = NULL;
    H5_daos_obj_t              *target_obj           = NULL;
    char                       *path_buf             = NULL;
    uint64_t                   *rc_buf               = NULL;
    H5_daos_req_t              *int_req              = NULL;
    H5_daos_req_t              *int_int_req          = NULL;
    tse_task_t                 *first_task           = NULL;
    tse_task_t                 *dep_task             = NULL;
    const char                 *oexists_obj_name     = NULL;
    size_t                      oexists_obj_name_len = 0;
    hbool_t                     collective_md_read;
    hbool_t                     collective_md_write;
    hbool_t                     collective;
    hbool_t                     must_coll_err_check = FALSE;
    hbool_t                     must_coll_req       = FALSE;
    hid_t                       lapl_id;
    int                         ret;
    herr_t                      ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL");
    if (!specific_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Determine metadata I/O mode setting (collective vs. independent)
     * for metadata reads and writes according to file-wide setting on
     * FAPL and per-operation setting on LAPL.
     */
    lapl_id = (H5VL_OBJECT_BY_NAME == loc_params->type)  ? loc_params->loc_data.loc_by_name.lapl_id
              : (H5VL_OBJECT_BY_IDX == loc_params->type) ? loc_params->loc_data.loc_by_idx.lapl_id
                                                         : H5P_LINK_ACCESS_DEFAULT;
    if (specific_args->op_type == H5VL_OBJECT_FLUSH)
        collective = FALSE;
    else {
        H5_DAOS_GET_METADATA_IO_MODES(item->file, lapl_id, H5P_LINK_ACCESS_DEFAULT, collective_md_read,
                                      collective_md_write, H5E_OBJECT, FAIL);
        if (specific_args->op_type == H5VL_OBJECT_CHANGE_REF_COUNT)
            collective = collective_md_write;
        else
            collective = collective_md_read;
    } /* end else */

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "object specific", item ? item->open_req : NULL,
                                              NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Determine target object */
    if (loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as target object, or the root group if item is a file */
        if (item->type == H5I_FILE)
            target_obj = (H5_daos_obj_t *)item->file->root_grp;
        else
            target_obj = (H5_daos_obj_t *)item;
        target_obj->item.rc++;
    } /* end if */
    else if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        /*
         * Open target_obj. For H5Oexists_by_name, we use H5_daos_group_traverse
         * as the path may point to a soft link that doesn't resolve. In that case,
         * H5_daos_object_open_helper would fail.
         */
        if (H5VL_OBJECT_EXISTS == specific_args->op_type) {
#ifdef H5_DAOS_USE_TRANSACTIONS
            /* Start transaction */
            if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't start transaction");
            int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

            /* Open group containing the link in question */
            if (NULL == (target_obj = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name,
                                                             H5P_LINK_CREATE_DEFAULT, int_req, collective,
                                                             &path_buf, &oexists_obj_name,
                                                             &oexists_obj_name_len, &first_task, &dep_task)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group");
        } /* end if */
        else {
            /* Start internal H5 operation for target object open.  This will
             * not be visible to the API, will not be added to an operation
             * pool, and will be integrated into this function's task chain. */
            if (NULL == (int_int_req = H5_daos_req_create(
                             item->file, "target object open within object specific operation", NULL, NULL,
                             int_req, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Open the object */
            must_coll_req = collective;
            if (H5_daos_object_open_helper(item, loc_params, NULL, collective, &target_obj_p, NULL,
                                           int_int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, FAIL, "can't open target object");

            /* Create task to finalize internal operation */
            if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL,
                                    NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                             "can't create task to finalize internal operation");

            /* Schedule finalize task (or save it to be scheduled later),
             * give it ownership of int_int_req, and update task pointers */
            if (first_task) {
                if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                                 "can't schedule task to finalize H5 operation: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                first_task = int_int_req->finalize_task;
            dep_task    = int_int_req->finalize_task;
            int_int_req = NULL;
        } /* else */
    }     /* end if */
    else
        D_GOTO_ERROR(H5E_OBJECT, H5E_UNSUPPORTED, FAIL,
                     "unsupported object operation location parameters type");

    switch (specific_args->op_type) {
        /* H5Oincr_refcount/H5Odecr_refcount */
        case H5VL_OBJECT_CHANGE_REF_COUNT: {
            int update_ref = specific_args->args.change_rc.delta;

            assert(loc_params->type == H5VL_OBJECT_BY_SELF);
            assert(target_obj);

            int_req->op_name = "change object reference count";

            must_coll_err_check = collective;
            must_coll_req       = collective;
            if (!collective || (item->file->my_rank == 0)) {
                /* Allocate buffer to hold rc */
                if (NULL == (rc_buf = DV_malloc(sizeof(uint64_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                 "can't allocate user data struct for object reference count adjust task");

                /* Read target object ref count */
                if (0 != (ret = H5_daos_obj_read_rc(NULL, target_obj, rc_buf, NULL, int_req, &first_task,
                                                    &dep_task)))
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't get object ref count: %s",
                                 H5_daos_err_to_string(ret));

                /* Increment and write ref count */
                if (0 != (ret = H5_daos_obj_write_rc(NULL, target_obj, rc_buf, (int64_t)update_ref, int_req,
                                                     &first_task, &dep_task)))
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINC, FAIL, "can't write updated object ref count: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */

            break;
        } /* H5VL_OBJECT_CHANGE_REF_COUNT */

        /* H5Oexists_by_name */
        case H5VL_OBJECT_EXISTS: {
            hbool_t *oexists_ret = specific_args->args.exists.exists;

            assert(target_obj);

            int_req->op_name = "object existence check";

            /* Check type of target_obj */
            if (target_obj->item.type != H5I_GROUP)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "target object is not a group");

            /* Make collective -NAF */
            /*must_coll_err_check = collective;
            must_coll_req = collective;*/
            if (H5_daos_object_exists((H5_daos_group_t *)target_obj, oexists_obj_name, oexists_obj_name_len,
                                      oexists_ret, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't determine if object exists");

            break;
        } /* H5VL_OBJECT_EXISTS */

        case H5VL_OBJECT_LOOKUP: {
            H5O_token_t *token       = specific_args->args.lookup.token_ptr;
            tse_task_t  *lookup_task = NULL;

            int_req->op_name = "object lookup";

            if (H5VL_OBJECT_BY_NAME != loc_params->type)
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL, "invalid loc_params type");

            /* Allocate task udata struct and retarget target_obj_p to fill
             * in target_obj in udata */
            if (NULL ==
                (lookup_udata = (H5_daos_object_lookup_ud_t *)DV_calloc(sizeof(H5_daos_object_lookup_ud_t))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate object specific user data");
            lookup_udata->req   = int_req;
            *target_obj_p       = &lookup_udata->target_obj;
            lookup_udata->token = token;

            /* Create task to finish this operation */
            if (H5_daos_create_task(H5_daos_object_lookup_task, dep_task ? 1 : 0, dep_task ? &dep_task : NULL,
                                    NULL, NULL, lookup_udata, &lookup_task) < 0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to look up object token");

            /* Schedule operator callback function task (or save it to be scheduled
             * later) and give it a reference to req and udata. */
            if (first_task) {
                if (0 != (ret = tse_task_schedule(lookup_task, false)))
                    D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                                 "can't schedule task to look up object token: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                first_task = lookup_task;
            dep_task = lookup_task;
            int_req->rc++;
            lookup_udata = NULL;

            break;
        } /* H5VL_OBJECT_LOOKUP */

        /* H5Ovisit(_by_name) */
        case H5VL_OBJECT_VISIT: {
            H5VL_object_visit_args_t *visit_args = &specific_args->args.visit;
            H5_daos_iter_data_t       iter_data;

            int_req->op_name = "object visit";

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_OBJ, visit_args->idx_type, visit_args->order,
                                   FALSE, NULL, H5I_INVALID_HID, visit_args->op_data, &ret_value, int_req);
            iter_data.u.obj_iter_data.fields        = visit_args->fields;
            iter_data.u.obj_iter_data.u.obj_iter_op = visit_args->op;
            iter_data.u.obj_iter_data.obj_name      = ".";

            if (H5_daos_object_visit(target_obj_p, target_obj, &iter_data, int_req, &first_task, &dep_task) <
                0)
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, FAIL, "object visiting failed");

            break;
        } /* H5VL_OBJECT_VISIT */

        /* H5Oflush */
        case H5VL_OBJECT_FLUSH: {
            assert(loc_params->type == H5VL_OBJECT_BY_SELF);

            int_req->op_name = "object flush";

            switch (item->type) {
                case H5I_FILE:
                    if (H5_daos_file_flush((H5_daos_file_t *)item, int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file");
                    break;
                case H5I_GROUP:
                    if (H5_daos_group_flush((H5_daos_group_t *)item, int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't flush group");
                    break;
                case H5I_DATASET:
                    if (H5_daos_dataset_flush((H5_daos_dset_t *)item, int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't flush dataset");
                    break;
                case H5I_DATATYPE:
                    if (H5_daos_datatype_flush((H5_daos_dtype_t *)item, int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_WRITEERROR, FAIL, "can't flush datatype");
                    break;
                case H5I_MAP:
                    if (H5_daos_map_flush((H5_daos_map_t *)item, int_req, &first_task, &dep_task) < 0)
                        D_GOTO_ERROR(H5E_MAP, H5E_WRITEERROR, FAIL, "can't flush map");
                    break;
                default:
                    D_GOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid object type");
            } /* end switch */

            break;
        } /* H5VL_OBJECT_FLUSH */

        /* H5Orefresh */
        case H5VL_OBJECT_REFRESH: {
            assert(loc_params->type == H5VL_OBJECT_BY_SELF);

            int_req->op_name = "object refresh";

            switch (item->type) {
                case H5I_FILE:
                    if (H5_daos_group_refresh(item->file->root_grp, dxpl_id, NULL) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_READERROR, FAIL, "failed to refresh file");
                    break;
                case H5I_GROUP:
                    if (H5_daos_group_refresh((H5_daos_group_t *)item, dxpl_id, NULL) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_READERROR, FAIL, "failed to refresh group");
                    break;
                case H5I_DATASET:
                    if (H5_daos_dataset_refresh((H5_daos_dset_t *)item, dxpl_id, int_req, &first_task,
                                                &dep_task) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "failed to refresh dataset");
                    break;
                case H5I_DATATYPE:
                    if (H5_daos_datatype_refresh((H5_daos_dtype_t *)item, dxpl_id, NULL) < 0)
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
    if (int_req) {
        /* Free path_buf if necessary */
        if (path_buf && H5_daos_free_async(path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTFREE, FAIL, "can't free path buffer");

        /* Free rc_buf if necessary */
        if (rc_buf && H5_daos_free_async(rc_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTFREE, FAIL, "can't free ref count buffer");

        /* Perform collective error check */
        if (must_coll_err_check && (item->file->num_procs > 1))
            if (H5_daos_collective_error_check((H5_daos_obj_t *)item, int_req, &first_task, &dep_task) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't perform collective error check");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item,
                                specific_args->op_type == H5VL_OBJECT_CHANGE_REF_COUNT ||
                                        specific_args->op_type == H5VL_OBJECT_FLUSH
                                    ? H5_DAOS_OP_TYPE_WRITE_ORDERED
                                    : H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, must_coll_req, !req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async.  Disabled for H5Ovisit for now. */
        if (req && specific_args->op_type != H5VL_OBJECT_VISIT) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CANTOPERATE, FAIL,
                             "object specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    if (target_obj) {
        if (H5_daos_object_close(&target_obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't close object");
        target_obj = NULL;
    } /* end if */

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, FAIL, "can't free request");

    if (lookup_udata) {
        assert(ret_value < 0);
        lookup_udata = DV_free(lookup_udata);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_object_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_close_task
 *
 * Purpose:     Asynchronous task to close a DAOS HDF5 object.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              October, 2020
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_object_close_task(tse_task_t *task)
{
    H5_daos_obj_close_task_ud_t *udata     = NULL;
    int                          ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object close task");

    /* Handle errors in previous tasks */
    /* Probably not necessary here? -NAF */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

done:
    if (udata) {
        /* Always call real close function, even if a previous task failed */
        if (H5_daos_object_close(udata->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "object close task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_close_task() */

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
H5_daos_object_close(H5_daos_item_t *item)
{
    int ret_value = 0;

    /* Call type's close function */
    if (item->type == H5I_GROUP) {
        if (H5_daos_group_close_real((H5_daos_group_t *)item) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");
    } /* end if */
    else if (item->type == H5I_DATASET) {
        if (H5_daos_dataset_close_real((H5_daos_dset_t *)item) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset");
    } /* end if */
    else if (item->type == H5I_DATATYPE) {
        if (H5_daos_datatype_close_real((H5_daos_dtype_t *)item) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype");
    } /* end if */
    else if (item->type == H5I_ATTR) {
        if (H5_daos_attribute_close_real((H5_daos_attr_t *)item) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");
    } /* end if */
    else if (item->type == H5I_MAP) {
        if (H5_daos_map_close_real((H5_daos_map_t *)item) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map");
    } /* end if */
    else
        assert(0 && "Invalid object type");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_obj_close() */

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
    unsigned acorder_flags = 0;
    hbool_t  default_plist;
    herr_t   ret_value = SUCCEED;

    assert(obj);

    default_plist = ocpl_id == H5P_FILE_CREATE_DEFAULT || ocpl_id == H5P_GROUP_CREATE_DEFAULT ||
                    ocpl_id == H5P_DATASET_CREATE_DEFAULT || ocpl_id == H5P_DATATYPE_CREATE_DEFAULT ||
                    ocpl_id == H5P_MAP_CREATE_DEFAULT;

    /* Determine if this object is tracking attribute creation order */
    if (default_plist) {
        switch (obj->item.type) {
            case H5I_FILE:
            case H5I_GROUP:
                acorder_flags = H5_daos_plist_cache_g->gcpl_cache.acorder_flags;
                break;

            case H5I_DATASET:
                acorder_flags = H5_daos_plist_cache_g->dcpl_cache.acorder_flags;
                break;

            case H5I_DATATYPE:
                acorder_flags = H5_daos_plist_cache_g->tcpl_cache.acorder_flags;
                break;

            case H5I_MAP:
                acorder_flags = H5_daos_plist_cache_g->mcpl_cache.acorder_flags;
                break;

            default:
                D_GOTO_ERROR(H5E_OBJECT, H5E_BADVALUE, FAIL, "invalid object type");
        }
    }
    else if (H5Pget_attr_creation_order(ocpl_id, &acorder_flags) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't get attribute creation order flags");
    assert(!obj->ocpl_cache.track_acorder);
    if (acorder_flags & H5P_CRT_ORDER_TRACKED)
        obj->ocpl_cache.track_acorder = TRUE;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_fill_ocpl_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_exists
 *
 * Purpose:     Creates an asynchronous task to check if an object exists
 *              in a group according to a given link name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_exists(H5_daos_group_t *target_grp, const char *link_name, size_t link_name_len,
                      hbool_t *oexists_ret, H5_daos_req_t *req, tse_task_t **first_task,
                      tse_task_t **dep_task)
{
    H5_daos_object_exists_ud_t *exists_udata        = NULL;
    daos_obj_id_t             **oid_ptr             = NULL;
    tse_task_t                 *oexists_finish_task = NULL;
    int                         ret;
    herr_t                      ret_value = SUCCEED;

    assert(target_grp);
    assert(link_name);
    assert(oexists_ret);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate argument struct for object existence checking tasks */
    if (NULL == (exists_udata = (H5_daos_object_exists_ud_t *)DV_calloc(sizeof(H5_daos_object_exists_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for object existence check task arguments");
    exists_udata->req           = req;
    exists_udata->target_grp    = target_grp;
    exists_udata->link_name     = link_name;
    exists_udata->link_name_len = link_name_len;
    exists_udata->oexists_ret   = oexists_ret;

    /* Create task to try to resolve link */
    if (H5_daos_link_follow(exists_udata->target_grp, exists_udata->link_name, exists_udata->link_name_len,
                            FALSE, req, &oid_ptr, &exists_udata->link_exists, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_TRAVERSE, FAIL, "can't follow link to object");

    /* Retarget *oid_ptr so H5_daos_link_follow fills in the oid */
    *oid_ptr = &exists_udata->oid;

    /* Create task to set oexists_ret based on whether link resolves */
    if (H5_daos_create_task(H5_daos_object_exists_finish, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, exists_udata, &oexists_finish_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to check if object exists");

    /* Schedule object existence check task */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(oexists_finish_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                         "can't schedule task to check for object existence: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = oexists_finish_task;
    exists_udata->target_grp->obj.item.rc++;
    req->rc++;
    *dep_task = oexists_finish_task;

    /* Relinquish control of udata to task's function body */
    exists_udata = NULL;

done:
    if (exists_udata) {
        assert(ret_value < 0);
        exists_udata = DV_free(exists_udata);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_object_exists() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_exists_finish
 *
 * Purpose:     Asynchronous task to return whether an object exists in a
 *              group or not, based upon if the link name for that object
 *              resolved. Also frees private data from object existence
 *              check tasks.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_exists_finish(tse_task_t *task)
{
    H5_daos_object_exists_ud_t *udata;
    int                         ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object existence check task");

    assert(udata->req);
    assert(udata->target_grp);
    assert(udata->link_name);
    assert(udata->oexists_ret);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    *udata->oexists_ret = udata->link_exists;

done:
    if (udata) {
        /* Close group */
        if (H5_daos_group_close_real(udata->target_grp) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_exists_finish() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit
 *
 * Purpose:     Creates asynchronous tasks to recursively visit the
 *              specified object and all objects accessible from the
 *              specified object, calling the supplied callback function on
 *              each object.
 *
 *              Exactly one of target_obj_prev_out and target_obj must be
 *              provided.  If target_obj_prev_out is provided it should be
 *              set to the location previously returned by the ret_obj_p
 *              parameter for H5_daos_object_open_helper() (this function
 *              will retarget that function's output to point into this
 *              function's udata).  In this case, this function will own
 *              the object and will close it at the end.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_visit(H5_daos_obj_t ***target_obj_prev_out, H5_daos_obj_t *target_obj,
                     H5_daos_iter_data_t *iter_data, H5_daos_req_t *req, tse_task_t **first_task,
                     tse_task_t **dep_task)
{
    H5_daos_object_visit_ud_t *visit_udata = NULL;
    tse_task_t                *visit_task  = NULL;
    int                        ret;
    herr_t                     ret_value = SUCCEED;

    assert(target_obj_prev_out || target_obj);
    assert(!(target_obj_prev_out && target_obj));
    assert(iter_data);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (visit_udata = (H5_daos_object_visit_ud_t *)DV_malloc(sizeof(H5_daos_object_visit_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for object visit task");
    visit_udata->req            = req;
    visit_udata->visit_metatask = NULL;
    if (target_obj_prev_out)
        *target_obj_prev_out = &visit_udata->target_obj;
    else
        visit_udata->target_obj = target_obj;
    visit_udata->iter_data = *iter_data;

    /* Retrieve the info of the target object */
    if (H5_daos_object_get_info(NULL, &visit_udata->target_obj, NULL, iter_data->u.obj_iter_data.fields,
                                &visit_udata->obj_info, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, FAIL, "can't get info for object");

    /* Create task to visit the specified target object first */
    if (H5_daos_create_task(H5_daos_object_visit_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, visit_udata, &visit_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to call operator callback function");

    /* Schedule operator callback function task (or save it to be scheduled
     * later).  It will share references to req and target_obj with all objects
     * at this level of iteration, these references will be released by
     * H5_daos_object_visit_finish(). */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(visit_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                         "can't schedule task to call operator callback function: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = visit_task;
    *dep_task = visit_task;

    /* Create metatask to free object visiting udata. This task will be completed
     * when the actual asynchronous object visiting is finished. This metatask
     * is necessary because the initial visit task may generate other async
     * tasks for visiting the submembers of a group object. */
    if (H5_daos_create_task(H5_daos_object_visit_finish, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, visit_udata, &visit_udata->visit_metatask) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create meta task for object visiting");

    /* Schedule meta task */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(visit_udata->visit_metatask, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule meta task for object visiting: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = visit_udata->visit_metatask;
    if (target_obj)
        target_obj->item.rc++;
    req->rc++;

    *dep_task = visit_udata->visit_metatask;

    /* Relinquish control of iteration op udata to task. */
    visit_udata = NULL;

done:
    if (ret_value < 0) {
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
    tse_task_t                *first_task = NULL;
    tse_task_t                *dep_task   = NULL;
    int                        ret;
    int                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for operator function function task");

    assert(udata->req);
    assert(udata->iter_data.u.obj_iter_data.u.obj_iter_op);

    /* Check for previous errors */
    if (*udata->iter_data.op_ret_p < 0)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    /* Determine if short-circuit success was returned in previous tasks */
    if (*udata->iter_data.op_ret_p > 0)
        D_GOTO_DONE(0);

    /* Register ID for object to be visited */
    if ((udata->target_obj_id = H5VLwrap_register(udata->target_obj, udata->target_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, -H5_DAOS_SETUP_ERROR, "unable to atomize object handle");
    udata->target_obj->item.rc++;

    /* Make callback */
    if (udata->iter_data.async_op) {
        if (udata->iter_data.u.obj_iter_data.u.obj_iter_op_async(
                udata->target_obj_id, udata->iter_data.u.obj_iter_data.obj_name, &udata->obj_info,
                udata->iter_data.op_data, udata->iter_data.op_ret_p, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR,
                         "operator function returned failure");
    }
    else {
        if ((*udata->iter_data.op_ret_p = udata->iter_data.u.obj_iter_data.u.obj_iter_op(
                 udata->target_obj_id, udata->iter_data.u.obj_iter_data.obj_name, &udata->obj_info,
                 udata->iter_data.op_data)) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR,
                         "operator function returned failure");
    }

    /* Check for short-circuit success */
    if (*udata->iter_data.op_ret_p) {
        udata->iter_data.req->status        = -H5_DAOS_SHORT_CIRCUIT;
        udata->iter_data.short_circuit_init = TRUE;

        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    }

    /* If the object is a group, visit all objects below the group */
    if (H5I_GROUP == udata->target_obj->item.type) {
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
                               udata->iter_data.iter_order, FALSE, udata->iter_data.idx_p,
                               udata->target_obj_id, udata, NULL, udata->req);
        sub_iter_data.async_op                              = TRUE;
        sub_iter_data.u.link_iter_data.u.link_iter_op_async = H5_daos_object_visit_link_iter_cb;

        if (H5_daos_link_iterate((H5_daos_group_t *)udata->target_obj, &sub_iter_data, &first_task,
                                 &dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, -H5_DAOS_H5_ITER_ERROR,
                         "failed to iterate through group's links");
    } /* end if */

    /* Register dependency on dep_task for new link iteration task */
    if (dep_task && 0 != (ret = tse_task_register_deps(udata->visit_metatask, 1, &dep_task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret,
                     "can't create dependencies for object visiting metatask: %s",
                     H5_daos_err_to_string(ret));

done:
    if (udata) {
        /* Close object ID since the iterate task should own it now */
        if (udata->target_obj_id >= 0) {
            udata->target_obj->item.nonblocking_close = TRUE;
            if ((ret = H5Idec_ref(udata->target_obj_id)) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object ID");
            if (ret)
                udata->target_obj->item.nonblocking_close = FALSE;
            udata->target_obj_id = H5I_INVALID_HID;
        } /* end if */
    }     /* end if */

    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task to iterate over object's links: %s",
                     H5_daos_err_to_string(ret));

    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "object visiting task";
        } /* end if */
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info2_t *info, void *op_data,
                                  herr_t *op_ret, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_visit_ud_t *visit_udata = (H5_daos_object_visit_ud_t *)op_data;
    H5_daos_group_t           *target_grp;
    H5_daos_req_t             *int_int_req = NULL;
    int                        ret;
    herr_t                     ret_value = H5_ITER_CONT;

    assert(visit_udata);
    assert(H5_DAOS_ITER_TYPE_OBJ == visit_udata->iter_data.iter_type);

    /* H5Ovisit(_by_name) ignores soft links during visiting */
    if (H5L_TYPE_SOFT == info->type)
        D_GOTO_DONE(H5_ITER_CONT);

    if (NULL == (target_grp = (H5_daos_group_t *)H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for group ID");

    if (H5L_TYPE_HARD == info->type) {
        H5VL_loc_params_t loc_params;
        H5_daos_obj_t  ***target_obj_p = NULL;

        /* Start internal H5 operation for target object open.  This will
         * not be visible to the API, will not be added to an operation
         * pool, and will be integrated into this function's task chain. */
        if (NULL == (int_int_req = H5_daos_req_create(target_grp->obj.item.file,
                                                      "target object open within object visit", NULL, NULL,
                                                      visit_udata->req, H5I_INVALID_HID)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTALLOC, H5_ITER_ERROR, "can't create DAOS request");

        /* Open the target object */
        loc_params.type                         = H5VL_OBJECT_BY_NAME;
        loc_params.loc_data.loc_by_name.name    = name;
        loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
        if (H5_daos_object_open_helper(&target_grp->obj.item, &loc_params, NULL, TRUE, &target_obj_p, NULL,
                                       int_int_req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTOPENOBJ, H5_ITER_ERROR, "can't open object");

        /* Create task to finalize internal operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                                NULL, int_int_req, &int_int_req->finalize_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, H5_ITER_ERROR,
                         "can't create task to finalize internal operation");

        /* Schedule finalize task (or save it to be scheduled later),
         * give it ownership of int_int_req, and update task pointers */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, H5_ITER_ERROR,
                             "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = int_int_req->finalize_task;
        *dep_task   = int_int_req->finalize_task;
        int_int_req = NULL;

        /* Set name for next object and perform recursive object visit */
        visit_udata->iter_data.u.obj_iter_data.obj_name = name;
        if (H5_daos_object_visit(target_obj_p, NULL, &visit_udata->iter_data, visit_udata->req, first_task,
                                 dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_BADITER, H5_ITER_ERROR, "failed to visit object");
    }
    else
        D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, H5_ITER_ERROR, "invalid link type");

done:
    *op_ret = ret_value;

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, H5_ITER_ERROR, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_link_iter_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit_finish
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
H5_daos_object_visit_finish(tse_task_t *task)
{
    H5_daos_object_visit_ud_t *udata;
    int                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object visiting task");

    assert(udata->req);
    assert(udata->target_obj);
    assert(task == udata->visit_metatask);

    /* Iteration is complete, we are no longer short-circuiting (if this
     * iteration caused the short circuit) */
    if (udata->iter_data.short_circuit_init) {
        if (udata->iter_data.req->status == -H5_DAOS_SHORT_CIRCUIT)
            udata->iter_data.req->status = -H5_DAOS_INCOMPLETE;
        udata->iter_data.short_circuit_init = FALSE;
    } /* end if */

    /* Set *op_ret_p if present */
    if (udata->iter_data.op_ret_p)
        *udata->iter_data.op_ret_p = udata->iter_data.op_ret;

    /* Close object */
    if (H5_daos_object_close(&udata->target_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "object visiting udata free task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_visit_finish() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_info
 *
 * Purpose:     Creates an asynchronous task for retrieving the info for
 *              an object.  Exactly one of target_obj_prev_out,
 *              target_obj_p, or target_obj must be provided.
 *
 *              If target_obj_prev_out is provided it should be set to the
 *              location previously  returned by the ret_obj_p parameter
 *              for H5_daos_object_open_helper() (this function will
 *              retarget that function's output to point into this
 *              function's udata).  In this case, this function will own
 *              the object and will close it at the end.
 *
 *              If target_obj_p is provided it should point to the
 *              location of a H5_daos_obj_t * that will be valid for the
 *              target object while tasks scheduled by this function are
 *              executed.  In this case this function will not close the
 *              object.
 *
 *              If target_obj is provided it should be valid immediately
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_info(H5_daos_obj_t ***target_obj_prev_out, H5_daos_obj_t **target_obj_p,
                        H5_daos_obj_t *target_obj, unsigned fields, H5O_info2_t *obj_info_out,
                        H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_get_info_ud_t *get_info_udata = NULL;
    int                           ret;
    herr_t                        ret_value = SUCCEED;

    assert(!!target_obj_prev_out + !!target_obj_p + !!target_obj == 1);
    assert(obj_info_out);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL ==
        (get_info_udata = (H5_daos_object_get_info_ud_t *)DV_calloc(sizeof(H5_daos_object_get_info_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for object info retrieval task");
    get_info_udata->req = req;
    if (target_obj_p)
        get_info_udata->target_obj_p = target_obj_p;
    else {
        get_info_udata->target_obj_p = &get_info_udata->target_obj;
        if (target_obj_prev_out)
            *target_obj_prev_out = get_info_udata->target_obj_p;
        else
            get_info_udata->target_obj = target_obj;
    } /* end else */

    get_info_udata->fields   = fields;
    get_info_udata->info_out = obj_info_out;

    /* Create task for retrieving object info */
    if (H5_daos_create_task(H5_daos_object_get_info_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, get_info_udata, &get_info_udata->get_info_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't create task to get object info");

    /* Schedule object info retrieval task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(get_info_udata->get_info_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL, "can't schedule task to get object info: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = get_info_udata->get_info_task;
    req->rc++;
    if (target_obj)
        target_obj->item.rc++;
    *dep_task = get_info_udata->get_info_task;

    /* Relinquish control of udata to task's function body */
    get_info_udata = NULL;

done:
    /* Cleanup on failure */
    if (get_info_udata) {
        assert(ret_value < 0);
        get_info_udata = DV_free(get_info_udata);
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
    H5_daos_object_get_info_ud_t *udata      = NULL;
    H5_daos_req_t                *req        = NULL;
    tse_task_t                   *first_task = NULL;
    tse_task_t                   *dep_task   = NULL;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object info retrieval task");

    assert(udata->req);
    assert(udata->target_obj_p);
    assert(*udata->target_obj_p);
    assert(udata->info_out);
    assert(task == udata->get_info_task);

    /* Assign req convenience pointer.  We do this so we can still handle errors
     * after transferring ownership of udata.  This should be safe since we
     * increase the ref count on req when we transfer ownership. */
    req = udata->req;

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(req, H5E_OBJECT);

    /*
     * Initialize object info - most fields are not valid and will
     * simply be set to 0.
     */
    memset(udata->info_out, 0, sizeof(*udata->info_out));

    /* Basic fields */
    if (udata->fields & H5O_INFO_BASIC) {
        uint64_t fileno64;
        uuid_t   cuuid;
        uint8_t *uuid_p;

        uuid_parse((*udata->target_obj_p)->item.file->cont, cuuid);
        uuid_p = (uint8_t *)&cuuid;

        /* Use the lower <sizeof(unsigned long)> bytes of the file uuid
         * as the fileno.  Ideally we would write separate 32 and 64 bit
         * hash functions but this should work almost as well. */
        UINT64DECODE(uuid_p, fileno64)
        udata->info_out->fileno = (unsigned long)fileno64;

        /* Get token */
        if (H5_daos_oid_to_token((*udata->target_obj_p)->oid, &udata->info_out->token) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get object token");

        /* Set object type */
        switch ((*udata->target_obj_p)->item.type) {
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

        /* Read target object ref count */
        /* Could run this in parallel with get_num_attrs DSINC */
        if (0 != (ret = H5_daos_obj_read_rc(udata->target_obj_p, NULL, NULL, &udata->info_out->rc, udata->req,
                                            &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't get object ref count: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */

    /* Set the number of attributes. */
    if (udata->fields & H5O_INFO_NUM_ATTRS) {
        if (H5_daos_object_get_num_attrs(*udata->target_obj_p, &udata->info_out->num_attrs, FALSE, udata->req,
                                         &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTGET, -H5_DAOS_SETUP_ERROR,
                         "can't create task to retrieve the number of attributes attached to object");
    } /* end if */

    /* Investigate collisions with links, etc DSINC */

done:
    /* Schedule task to complete this task and free private data */
    if (udata) {
        tse_task_t *end_task;
        /* Schedule task to complete this task and free path buf */
        if (H5_daos_create_task(H5_daos_object_get_info_end, dep_task ? 1 : 0, dep_task ? &dep_task : NULL,
                                NULL, NULL, udata, &end_task) < 0) {
            tse_task_complete(task, ret_value);
            D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to finish getting object info");
        }
        else {
            /* Schedule end task and give it ownership of udata, while
             * keeping a reference to req for ourselves */
            req->rc++;
            if (first_task) {
                if (0 != (ret = tse_task_schedule(end_task, false)))
                    D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret,
                                 "can't schedule task to finish getting object info: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                first_task = end_task;
            udata    = NULL;
            dep_task = end_task;
        } /* end else */
    }     /* end if */

    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_OBJECT, H5E_CANTINIT, ret,
                     "can't schedule task to retrieve number of attributes attached to object: %s",
                     H5_daos_err_to_string(ret));

    /* Free private data if we haven't released ownership */
    if (udata) {
        assert(ret_value < 0);

        if (udata->target_obj && H5_daos_object_close(&udata->target_obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        udata = DV_free(udata);

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        tse_task_complete(task, ret_value);
    } /* end if */

    if (req) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = "get object info task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_info_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_info_end
 *
 * Purpose:     Asynchronous task for finalizing H5_daos_object_get_info.
 *              Cleans up udata and completes the get info task.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              July, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_object_get_info_end(tse_task_t *task)
{
    H5_daos_object_get_info_ud_t *udata     = NULL;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for MPI broadcast task");

    assert(udata->req);
    assert(udata->req->file);

    /* Close target object */
    if (udata->target_obj && H5_daos_object_close(&udata->target_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "object get info end task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, udata->get_info_task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete get info task */
    tse_task_complete(udata->get_info_task, ret_value);

    /* Free private data struct */
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_object_get_info_end() */

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
H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj, hsize_t *num_attrs, hbool_t post_decrement,
                             H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_object_get_num_attrs_ud_t *get_num_attr_udata = NULL;
    tse_task_t                        *get_num_attrs_task;
    int                                ret;
    herr_t                             ret_value = SUCCEED;

    assert(target_obj);
    assert(num_attrs);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    if (target_obj->ocpl_cache.track_acorder) {
        /* Allocate argument struct for fetch task */
        if (NULL == (get_num_attr_udata = (H5_daos_object_get_num_attrs_ud_t *)DV_calloc(
                         sizeof(H5_daos_object_get_num_attrs_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for fetch callback arguments");
        get_num_attr_udata->num_attrs_out = num_attrs;
        get_num_attr_udata->post_decr     = post_decrement;

        /* Set up main ud struct */
        get_num_attr_udata->md_rw_cb_ud.req = req;
        get_num_attr_udata->md_rw_cb_ud.obj = target_obj;

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&get_num_attr_udata->md_rw_cb_ud.dkey, H5_daos_attr_key_g,
                           H5_daos_attr_key_size_g);
        get_num_attr_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod */
        daos_const_iov_set((d_const_iov_t *)&get_num_attr_udata->md_rw_cb_ud.iod[0].iod_name,
                           H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
        get_num_attr_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        get_num_attr_udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
        get_num_attr_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        get_num_attr_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&get_num_attr_udata->md_rw_cb_ud.sg_iov[0], get_num_attr_udata->nattrs_buf,
                     (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
        get_num_attr_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        get_num_attr_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        get_num_attr_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &get_num_attr_udata->md_rw_cb_ud.sg_iov[0];
        get_num_attr_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;

        get_num_attr_udata->md_rw_cb_ud.nr = 1u;

        get_num_attr_udata->md_rw_cb_ud.task_name = "attribute count retrieval task";

        /* Create task to fetch object's current number of attributes counter */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_get_num_attrs_prep_cb, H5_daos_get_num_attrs_comp_cb,
                                     get_num_attr_udata, &get_num_attrs_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                         "can't create task to get number of attributes attached to object");

        /* Schedule task to read number of attributes attached to object (or save
         * it to be scheduled later) and give it a reference to req.
         */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(get_num_attrs_task, false)))
                D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                             "can't schedule task to read number of attributes attached to object: %s",
                             H5_daos_err_to_string(ret));
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
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, H5_ITER_NATIVE, FALSE, NULL,
                               H5I_INVALID_HID, num_attrs, NULL, req);
        iter_data.u.attr_iter_data.u.attr_iter_op = H5_daos_attribute_iterate_count_attrs_cb;

        /* Retrieve the number of attributes attached to the object */
        if (H5_daos_attribute_iterate(target_obj, &iter_data, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed");
    } /* end else */

done:
    if (ret_value < 0) {
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
    daos_obj_rw_t                     *fetch_args;
    int                                ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute count fetch task");

    assert(udata->md_rw_cb_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_OBJECT);

    assert(udata->md_rw_cb_ud.obj);
    assert(udata->md_rw_cb_ud.req->file);

    /* Set update task arguments */
    if (NULL == (fetch_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for metadata I/O task");
    memset(fetch_args, 0, sizeof(*fetch_args));
    fetch_args->oh    = udata->md_rw_cb_ud.obj->obj_oh;
    fetch_args->th    = DAOS_TX_NONE;
    fetch_args->flags = udata->md_rw_cb_ud.flags;
    fetch_args->dkey  = &udata->md_rw_cb_ud.dkey;
    fetch_args->nr    = udata->md_rw_cb_ud.nr;
    fetch_args->iods  = udata->md_rw_cb_ud.iod;
    fetch_args->sgls  = udata->md_rw_cb_ud.sgl;

done:
    if (ret_value < 0)
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
    int                                ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute count fetch task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        /* Check for no num attributes found, in this case it must be 0 */
        if (udata->md_rw_cb_ud.iod[0].iod_size == (uint64_t)0)
            *udata->num_attrs_out = (hsize_t)0;
        else {
            uint64_t nattrs;
            uint8_t *p = udata->nattrs_buf;

            /* Decode num attributes */
            UINT64DECODE(p, nattrs);

            if (udata->post_decr && nattrs > 0)
                nattrs--;

            *udata->num_attrs_out = (hsize_t)nattrs;
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        if (udata->md_rw_cb_ud.obj && H5_daos_object_close(&udata->md_rw_cb_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "attribute count retrieval task completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
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
H5_daos_object_update_num_attrs_key(H5_daos_obj_t *target_obj, hsize_t *new_nattrs, tse_task_cb_t prep_cb,
                                    tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                    tse_task_t **dep_task)
{
    H5_daos_object_update_num_attrs_key_ud_t *update_udata = NULL;
    tse_task_t                               *update_task;
    int                                       ret;
    herr_t                                    ret_value = SUCCEED;

    assert(target_obj);
    assert(new_nattrs);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    /* Allocate argument struct for update task */
    if (NULL == (update_udata = (H5_daos_object_update_num_attrs_key_ud_t *)DV_calloc(
                     sizeof(H5_daos_object_update_num_attrs_key_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for update callback arguments");
    update_udata->new_nattrs    = new_nattrs;
    update_udata->update_ud.req = req;
    update_udata->update_ud.obj = target_obj;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&update_udata->update_ud.dkey, H5_daos_attr_key_g,
                       H5_daos_attr_key_size_g);
    update_udata->update_ud.free_dkey = FALSE;

    /* Set up iod */
    memset(&update_udata->update_ud.iod[0], 0, sizeof(update_udata->update_ud.iod[0]));
    daos_const_iov_set((d_const_iov_t *)&update_udata->update_ud.iod[0].iod_name, H5_daos_nattr_key_g,
                       H5_daos_nattr_key_size_g);
    update_udata->update_ud.iod[0].iod_nr   = 1u;
    update_udata->update_ud.iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
    update_udata->update_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    update_udata->update_ud.free_akeys = FALSE;

    /* Set up sgl */
    daos_iov_set(&update_udata->update_ud.sg_iov[0], update_udata->nattrs_new_buf,
                 (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
    update_udata->update_ud.sgl[0].sg_nr     = 1;
    update_udata->update_ud.sgl[0].sg_nr_out = 0;
    update_udata->update_ud.sgl[0].sg_iovs   = &update_udata->update_ud.sg_iov[0];
    update_udata->update_ud.free_sg_iov[0]   = FALSE;

    update_udata->update_ud.nr = 1u;

    update_udata->update_ud.task_name = "object attribute number tracking akey update";

    /* Create task for akey update */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 prep_cb ? prep_cb : H5_daos_object_update_num_attrs_key_prep_cb,
                                 comp_cb ? comp_cb : H5_daos_object_update_num_attrs_key_comp_cb,
                                 update_udata, &update_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                     "can't create task to update object's number of attributes");

    /* Schedule akey update task (or save it to be scheduled later) */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, FAIL,
                         "can't schedule task to update object's attribute number tracking akey: %s",
                         H5_daos_err_to_string(ret));
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
    daos_obj_rw_t                            *update_args;
    uint8_t                                  *p;
    int                                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object attribute number tracking akey update task");

    assert(udata->update_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->update_ud.req, H5E_OBJECT);

    assert(udata->update_ud.obj);
    assert(udata->update_ud.req->file);

    /* Encode new value into buffer */
    p = udata->nattrs_new_buf;
    UINT64ENCODE(p, (uint64_t)*udata->new_nattrs);

    /* Set update task arguments */
    if (NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for metadata I/O task");
    memset(update_args, 0, sizeof(*update_args));
    update_args->oh    = udata->update_ud.obj->obj_oh;
    update_args->th    = DAOS_TX_NONE;
    update_args->flags = udata->update_ud.flags;
    update_args->dkey  = &udata->update_ud.dkey;
    update_args->nr    = 1;
    update_args->iods  = udata->update_ud.iod;
    update_args->sgls  = udata->update_ud.sgl;

done:
    if (ret_value < 0)
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
    int                                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object attribute number tracking akey update task");

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->update_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->update_ud.req->status      = task->dt_result;
        udata->update_ud.req->failed_task = udata->update_ud.task_name;
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        if (udata->update_ud.obj && H5_daos_object_close(&udata->update_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->update_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->update_ud.req->status = ret_value;
            udata->update_ud.req->failed_task =
                "completion callback for object attribute number tracking akey update task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->update_ud.req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_object_update_num_attrs_key_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_read_rc_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous fetch of object
 *              reference count.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              June, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_obj_read_rc_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_obj_rw_rc_ud_t *udata;
    daos_obj_rw_t          *rw_args;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_OBJECT);

    assert(udata->obj_p);
    assert(udata->req->file);

    /* Set update task arguments */
    if (NULL == (rw_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for object ref count read task");
    memset(rw_args, 0, sizeof(*rw_args));
    rw_args->oh    = (*udata->obj_p)->obj_oh;
    rw_args->th    = udata->req->th;
    rw_args->flags = 0;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&udata->dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
    rw_args->dkey = &udata->dkey;

    /* Set nr */
    rw_args->nr = 1;

    /* Set up iod */
    daos_const_iov_set((d_const_iov_t *)&udata->iod.iod_name, H5_daos_rc_key_g, H5_daos_rc_key_size_g);
    udata->iod.iod_nr   = 1u;
    udata->iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_RC_SIZE;
    udata->iod.iod_type = DAOS_IOD_SINGLE;
    rw_args->iods       = &udata->iod;

    /* Set up sgl */
    daos_iov_set(&udata->sg_iov, udata->rc_buf, (daos_size_t)H5_DAOS_ENCODED_RC_SIZE);
    udata->sgl.sg_nr     = 1;
    udata->sgl.sg_nr_out = 0;
    udata->sgl.sg_iovs   = &udata->sg_iov;
    rw_args->sgls        = &udata->sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_obj_read_rc_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_read_rc_comp_cb
 *
 * Purpose:     Complete callback for asynchronous fetch of object
 *              reference count.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              June, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_obj_read_rc_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_obj_rw_rc_ud_t *udata;
    uint64_t                rc_val;
    uint8_t                *p;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "object fetch ref count";
    } /* end if */
    else if (task->dt_result == 0) {
        /* Set output */
        /* Check for no rc found, in this case the rc is 0 */
        if (udata->iod.iod_size == 0) {
            if (udata->rc)
                *udata->rc = (uint64_t)1;
            if (udata->rc_uint)
                *udata->rc_uint = 1u;
        } /* end if */
        else {
            /* Decode read rc value */
            p = udata->rc_buf;
            UINT64DECODE(p, rc_val);

            if (udata->rc)
                *udata->rc = rc_val;
            if (udata->rc_uint)
                *udata->rc_uint = (unsigned)rc_val;
        } /* end else */
    }     /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Close object if a direct pointer was provided */
        if (udata->obj && H5_daos_object_close(&udata->obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "read object ref count completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_obj_read_rc_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_read_rc
 *
 * Purpose:     Retrieves the object's reference count.  Exactly one of
 *              obj_p or obj must be provided.  Use obj_p if the object
 *              struct is not yet allocated when this function is called.
 *              *obj_p / *obj and *rc do not need to be valid until after
 *              dep_task (as passed to this function) completes.
 *
 * Return:      0 on success/Negative error code on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_obj_read_rc(H5_daos_obj_t **obj_p, H5_daos_obj_t *obj, uint64_t *rc, unsigned *rc_uint,
                    H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_obj_rw_rc_ud_t *fetch_udata = NULL;
    tse_task_t             *fetch_task  = NULL;
    int                     ret;
    int                     ret_value = 0;

    assert(obj || obj_p);
    assert(!(obj_p && obj));
    assert(req);
    assert(req->file);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_RC_SIZE == H5_DAOS_ENCODED_UINT64_T_SIZE);

    /* Allocate task udata struct */
    if (NULL == (fetch_udata = (H5_daos_obj_rw_rc_ud_t *)DV_calloc(sizeof(H5_daos_obj_rw_rc_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                     "can't allocate read ref count user data");
    fetch_udata->req = req;
    if (obj_p)
        fetch_udata->obj_p = obj_p;
    else {
        fetch_udata->obj = obj;
        obj->item.rc++;
        fetch_udata->obj_p = &fetch_udata->obj;
    } /* end else */
    fetch_udata->rc      = rc;
    fetch_udata->rc_uint = rc_uint;

    /* Create task for rc fetch */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_obj_read_rc_prep_cb, H5_daos_obj_read_rc_comp_cb, fetch_udata,
                                 &fetch_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                     "can't create task to read object ref count");

    /* Schedule fetch task (or save it to be scheduled later) and give it a
     * reference to req and udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task for object read ref count: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = fetch_task;
    *dep_task = fetch_task;
    req->rc++;
    fetch_udata = NULL;

done:
    /* Clean up */
    if (fetch_udata) {
        assert(ret_value < 0);
        fetch_udata = DV_free(fetch_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_obj_read_rc() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_write_rc_task
 *
 * Purpose:     Asynchronous task for object reference count write.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              June, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_obj_write_rc_task(tse_task_t *task)
{
    H5_daos_obj_rw_rc_ud_t *udata;
    H5_daos_req_t          *req = NULL;
    uint64_t                cur_rc;
    uint64_t                new_rc;
    int                     ret;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object ref count write task");

    assert(udata->req);
    assert(udata->req->file);
    assert(task == udata->op_task);

    /* Assign req convenience pointer and take a reference to it */
    req = udata->req;
    req->rc++;

    /* Handle errors */
    H5_DAOS_PREP_REQ(req, H5E_OBJECT);

    assert(udata->obj_p);
    assert(*udata->obj_p);

    /* Determine current ref count */
    cur_rc = udata->rc ? *udata->rc : 0;
    assert((int64_t)cur_rc + udata->adjust >= 0);

    /* Check if we're deleting the object */
    new_rc = (uint64_t)((int64_t)cur_rc + udata->adjust);
    if (udata->adjust < 0 && new_rc == 0) {
        tse_task_t       *punch_task;
        daos_obj_punch_t *punch_args;

        /* Create task for object punch */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_PUNCH, 0, NULL, NULL, H5_daos_obj_write_rc_comp_cb, udata,
                                     &punch_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to delete object");

        /* Set punch task arguments */
        if (NULL == (punch_args = daos_task_get_args(punch_task)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                         "can't get arguments for object ref count write task");
        memset(punch_args, 0, sizeof(*punch_args));
        punch_args->oh = (*udata->obj_p)->obj_oh;
        punch_args->th = udata->req->th;

        udata->task_name = "object punch due to ref count dropping to 0";

        /* Schedule punch_task task and transfer ownership of udata */
        if (0 != (ret = tse_task_schedule(punch_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task for object delete: %s",
                         H5_daos_err_to_string(ret));
        udata = NULL;
    } /* end if */
    else {
        tse_task_t    *update_task;
        daos_obj_rw_t *rw_args;
        uint8_t       *p;

        /* Encode rc */
        p = udata->rc_buf;
        UINT64ENCODE(p, new_rc);

        /* Create task for rc update */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, 0, NULL, NULL, H5_daos_obj_write_rc_comp_cb, udata,
                                     &update_task) < 0)
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to write object ref count");

        /* Set update task arguments */
        if (NULL == (rw_args = daos_task_get_args(update_task)))
            D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                         "can't get arguments for object ref count write task");
        memset(rw_args, 0, sizeof(*rw_args));
        rw_args->oh = (*udata->obj_p)->obj_oh;
        rw_args->th = udata->req->th;

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&udata->dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        rw_args->dkey = &udata->dkey;

        /* Set nr */
        rw_args->nr = 1;

        /* Set up iod */
        daos_const_iov_set((d_const_iov_t *)&udata->iod.iod_name, H5_daos_rc_key_g, H5_daos_rc_key_size_g);
        udata->iod.iod_nr   = 1u;
        udata->iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_RC_SIZE;
        udata->iod.iod_type = DAOS_IOD_SINGLE;
        rw_args->iods       = &udata->iod;

        /* Set up sgl */
        daos_iov_set(&udata->sg_iov, udata->rc_buf, (daos_size_t)H5_DAOS_ENCODED_RC_SIZE);
        udata->sgl.sg_nr     = 1;
        udata->sgl.sg_nr_out = 0;
        udata->sgl.sg_iovs   = &udata->sg_iov;
        rw_args->sgls        = &udata->sgl;

        udata->task_name = "object update ref count";

        /* Schedule update_task task and transfer ownership of udata */
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task for object write ref count: %s",
                         H5_daos_err_to_string(ret));
        udata = NULL;
    } /* end else */

done:
    /* Handle errors */
    if (ret_value < 0 && req) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value != -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = "object ref count write task";
        } /* end if */
    }     /* end if */

    /* Release req */
    if (req && H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_LINK, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Cleanup */
    if (udata) {
        assert(ret_value < 0);

        /* Close object if a direct pointer was provided */
        if (udata->obj && H5_daos_object_close(&udata->obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        tse_task_complete(task, ret_value);
        udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_obj_write_rc_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_write_rc_comp_cb
 *
 * Purpose:     Complete callback for asynchronous write of object
 *              reference count.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              June, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_obj_write_rc_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_obj_rw_rc_ud_t *udata;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = udata->task_name;
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Close object if a direct pointer was provided */
        if (udata->obj && H5_daos_object_close(&udata->obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "write object ref count completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Complete op task */
        if (udata->op_task) {
            /* Return task to task list */
            if (H5_daos_task_list_put(H5_daos_task_list_g, udata->op_task) < 0)
                D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                             "can't return task to task list");
            tse_task_complete(udata->op_task, ret_value);
        }

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_obj_write_rc_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_obj_write_rc
 *
 * Purpose:     Writes the provided reference count to the object, after
 *              adding adjust.  Exactly one of obj_p or obj must be
 *              provided.  Use obj_p if the object struct is not yet
 *              allocated when this function is called.  *obj_p / *obj and
 *              *rc do not need to be valid until after dep_task (as
 *              passed to this function) completes.  If rc is passed as
 *              NULL *rc is taken to be 0.  If the object's ref count
 *              drops to 0 it will be deleted from the file.  If *rc and
 *              adjust are both 0 the object will not be deleted (this is
 *              used for anonymous object creation).
 *
 * Return:      0 on success/Negative error code on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_obj_write_rc(H5_daos_obj_t **obj_p, H5_daos_obj_t *obj, uint64_t *rc, int64_t adjust,
                     H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_obj_rw_rc_ud_t *task_udata = NULL;
    int                     ret;
    int                     ret_value = 0;

    assert(obj_p || obj);
    assert(!(obj_p && obj));
    assert(req);
    assert(req->file);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_RC_SIZE == H5_DAOS_ENCODED_UINT64_T_SIZE);

    /* Allocate task udata struct */
    if (NULL == (task_udata = (H5_daos_obj_rw_rc_ud_t *)DV_calloc(sizeof(H5_daos_obj_rw_rc_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                     "can't allocate write ref count user data");
    task_udata->req = req;
    if (obj_p)
        task_udata->obj_p = obj_p;
    else {
        task_udata->obj = obj;
        obj->item.rc++;
        task_udata->obj_p = &task_udata->obj;
    } /* end else */
    task_udata->rc     = rc;
    task_udata->adjust = adjust;

    /* Create task to finish this operation */
    if (H5_daos_create_task(H5_daos_obj_write_rc_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, task_udata, &task_udata->op_task) < 0)
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                     "can't create task for object write ref count");

    /* Schedule task (or save it to be scheduled later) and give it a
     * reference to req and udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(task_udata->op_task, false)))
            D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, ret, "can't schedule task for object write ref count: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = task_udata->op_task;
    *dep_task = task_udata->op_task;
    req->rc++;
    task_udata = NULL;

done:
    /* Clean up */
    if (task_udata) {
        assert(ret_value < 0);
        task_udata = DV_free(task_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_obj_write_rc() */
