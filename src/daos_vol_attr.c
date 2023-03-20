/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library. Attribute routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

/****************/
/* Local Macros */
/****************/

#define H5_DAOS_AINFO_BCAST_BUF_SIZE                                                                         \
    (H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_ACPL_BUF_SIZE +                                \
     3 * H5_DAOS_ENCODED_UINT64_T_SIZE)

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Task user data for creating an attribute */
typedef struct H5_daos_attr_create_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud;
    H5_daos_req_t        *req;
    H5_daos_attr_t       *attr;
    daos_key_t            akeys[4];
    void                 *akeys_buf;
    uint8_t               nattr_new_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
    uint8_t               nattr_old_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE + 1];
    uint8_t               max_corder_old_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t               max_corder_new_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t               flex_buf[];
} H5_daos_attr_create_ud_t;

/* Task user data for opening an attribute */
typedef struct H5_daos_attr_open_ud_t {
    H5_daos_md_rw_cb_ud_t    md_rw_cb_ud; /* Must be first */
    H5_daos_mpi_ibcast_ud_t *bcast_udata;
    tse_task_t              *fetch_metatask;
    H5_daos_attr_t          *attr;
    uint8_t                  flex_buf[];
} H5_daos_attr_open_ud_t;

typedef struct H5_daos_attr_ibcast_ud_t {
    H5_daos_mpi_ibcast_ud_t bcast_ud;
    H5_daos_attr_t         *attr;
    uint8_t                 flex_buf[];
} H5_daos_attr_ibcast_ud_t;

typedef struct H5_daos_attr_io_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud;
    H5_daos_attr_t       *attr;
    uint64_t              attr_nelmts;
    size_t                mem_type_size;
    size_t                file_type_size;
    daos_recx_t           recx;
    hbool_t               collective;
    H5_daos_tconv_reuse_t reuse;
    htri_t                need_tconv;
    hid_t                 mem_type_id;
    daos_key_t            akey;
    void                 *akey_buf;
    H5_daos_io_type_t     io_type;
    union {
        void       *rbuf;
        const void *wbuf;
    } buf;
    void       *tconv_buf;
    void       *bkg_buf;
    tse_task_t *end_task;
} H5_daos_attr_io_ud_t;

/* Task user data for retrieving info about an attribute */
typedef struct H5_daos_attr_get_info_ud_t {
    H5_daos_req_t  *req;
    tse_task_t     *get_info_metatask;
    H5_daos_attr_t *attr;
    H5A_info_t     *info_out;
} H5_daos_attr_get_info_ud_t;

/* Task user data for deleting an attribute */
typedef struct H5_daos_attr_delete_ud_t {
    H5_daos_req_t *req;
    H5_daos_obj_t *attr_parent_obj;
    daos_key_t     dkey;
    daos_key_t     akeys[H5_DAOS_ATTR_NUM_AKEYS];
    const char    *target_attr_name;
    size_t         target_attr_name_len;
    hsize_t        cur_num_attrs;
    void          *akeys_buf;
    char          *attr_name_buf;
} H5_daos_attr_delete_ud_t;

/* User data struct for checking if an attribute exists */
typedef struct H5_daos_attr_exists_ud_t {
    H5_daos_mpi_ibcast_ud_t       bcast_ud; /* Must be first */
    H5_DAOS_ATTR_EXISTS_OUT_TYPE *exists;
    htri_t                        bcast_exists;
    daos_key_t                    dkey;
    daos_key_t                    akeys[H5_DAOS_ATTR_NUM_AKEYS - 1];
    daos_iod_t                    iod[H5_DAOS_ATTR_NUM_AKEYS - 1];
    unsigned                      nr;
    void                         *akeys_buf;
} H5_daos_attr_exists_ud_t;

/* Task user data for iterating over attributes on an object */
typedef struct H5_daos_attr_iterate_ud_t {
    H5_daos_req_t      *req;
    H5_daos_iter_data_t iter_data;
    H5_daos_obj_t      *attr_container_obj;
    tse_task_t         *iterate_metatask;

    union {
        struct {
            H5_daos_md_rw_cb_ud_t md_rw_cb_ud;
            daos_key_desc_t       kds[H5_DAOS_ITER_LEN];
            daos_anchor_t         anchor;
            uint32_t              akey_nr;
        } name_order_data;

        struct {
            hsize_t obj_nattrs;
        } crt_order_data;
    } u;
} H5_daos_attr_iterate_ud_t;

/* Task user data for calling a user-supplied operator
 * callback function during attribute iteration.
 */
typedef struct H5_daos_attr_iterate_op_ud_t {
    H5_daos_attr_get_info_ud_t get_info_ud;
    H5A_info_t                 attr_info;
    H5_daos_iter_data_t       *iter_data;
} H5_daos_attr_iterate_op_ud_t;

/* User data struct for attribute get name by index
 * with automatic asynchronous name buffer allocation */
typedef struct H5_daos_attr_gnbi_alloc_ud_t {
    H5_daos_req_t  *req;
    tse_task_t     *gnbi_task;
    H5_daos_obj_t  *target_obj;
    H5_index_t      index_type;
    H5_iter_order_t iter_order;
    uint64_t        idx;
    const char    **attr_name;
    size_t         *attr_name_size;
    char          **attr_name_buf;
    size_t         *attr_name_buf_size;
    size_t          cur_attr_name_size;
} H5_daos_attr_gnbi_alloc_ud_t;

/* Task user data for retrieving an attribute's name
 * by an index value according to name or creation order.
 */
typedef struct H5_daos_attr_get_name_by_idx_ud_t {
    H5_daos_req_t  *req;
    H5_daos_obj_t  *target_obj;
    H5_index_t      index_type;
    H5_iter_order_t iter_order;
    uint64_t        idx;
    hsize_t         obj_nattrs;
    char           *attr_name_out;
    size_t          attr_name_out_size;
    size_t         *attr_name_size_ret;
    union {
        struct {
            uint64_t cur_attr_idx;
        } by_name_data;
        struct {
            H5_daos_md_rw_cb_ud_t md_rw_cb_ud;
            uint8_t               idx_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1];
        } by_crt_order_data;
    } u;
} H5_daos_attr_get_name_by_idx_ud_t;

/*
 * An attribute iteration callback function data structure. It
 * is passed during attribute iteration when retrieving an
 * attribute's creation order index value by the given attribute's
 * name.
 */
typedef struct H5_daos_attr_crt_idx_iter_ud_t {
    const char *target_attr_name;
    uint64_t   *attr_idx_out;
} H5_daos_attr_crt_idx_iter_ud_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_attribute_get_akeys(const char *attr_name, daos_key_t *datatype_key,
                                          daos_key_t *dataspace_key, daos_key_t *acpl_key,
                                          daos_key_t *acorder_key, daos_key_t *raw_data_key,
                                          void **akey_buf_out);
static int    H5_daos_attribute_md_rw_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_create_helper_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_create_helper_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_create_get_crt_order_info(H5_daos_attr_create_ud_t *create_ud,
                                                          H5_daos_req_t *req, tse_task_t **first_task,
                                                          tse_task_t **dep_task);
static int    H5_daos_attribute_create_get_crt_order_info_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_create_get_crt_order_info_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_open_by_idx_helper(H5_daos_obj_t           *target_obj,
                                                   const H5VL_loc_params_t *loc_params,
                                                   H5_daos_attr_t *attr_out, H5_daos_req_t *req,
                                                   tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_attribute_open_bcast_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_open_recv_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_open_end(H5_daos_attr_t *attr, uint8_t *p, uint64_t type_buf_len,
                                         uint64_t space_buf_len, uint64_t acpl_buf_len);
static int    H5_daos_attr_io_int_task(tse_task_t *task);
static int    H5_daos_attr_io_int_end_task(tse_task_t *task);
static herr_t H5_daos_attribute_read_int(H5_daos_attr_t *attr, hid_t mem_type_id, hbool_t collective,
                                         htri_t need_tconv, void *buf, H5_daos_attr_io_ud_t *_udata,
                                         H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_ainfo_read_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_read_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_read_bcast_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attr_read_tconv(tse_task_t *task);
static herr_t H5_daos_attribute_write_int(H5_daos_attr_t *attr, hid_t mem_type_id, hbool_t collective,
                                          htri_t need_tconv, const void *buf, H5_daos_attr_io_ud_t *_udata,
                                          H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_attribute_read_bkg_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_write_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_get_name(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
                                         char *attr_name_out, size_t attr_name_out_size, size_t *size_ret,
                                         H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_attribute_get_info(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
                                         const char *attr_name, H5A_info_t *attr_info, tse_task_cb_t prep_cb,
                                         tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                         tse_task_t **dep_task);
static herr_t H5_daos_attribute_get_info_inplace(H5_daos_attr_get_info_ud_t *get_info_udata,
                                                 tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                                                 H5_daos_req_t *req, tse_task_t **first_task,
                                                 tse_task_t **dep_task);
static int    H5_daos_attribute_get_info_task(tse_task_t *task);
static int    H5_daos_attribute_get_info_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_delete(H5_daos_obj_t *attr_container_obj, const H5VL_loc_params_t *loc_params,
                                       const H5VL_attr_specific_args_t *delete_args, hbool_t collective,
                                       H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_attribute_delete_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_delete_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_remove_from_crt_idx(H5_daos_obj_t                   *target_obj,
                                                    const H5VL_attr_specific_args_t *delete_args,
                                                    const char *attr_name, H5_daos_req_t *req,
                                                    tse_task_t **first_task, tse_task_t **dep_task);
static herr_t H5_daos_attribute_remove_from_crt_idx_name_cb(hid_t loc_id, const char *attr_name,
                                                            const H5A_info_t *attr_info, void *op_data);
static herr_t H5_daos_attribute_shift_crt_idx_keys_down(H5_daos_obj_t *target_obj, uint64_t idx_begin,
                                                        uint64_t idx_end);
static herr_t H5_daos_attribute_exists(H5_daos_obj_t *attr_container_obj, const char *attr_name,
                                       H5_DAOS_ATTR_EXISTS_OUT_TYPE *exists, hbool_t collective,
                                       H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_attr_exists_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attr_exists_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_attr_exists_bcast_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_iterate_by_name_order(H5_daos_attr_iterate_ud_t *iterate_udata,
                                                      H5_daos_req_t *req, tse_task_t **first_task,
                                                      tse_task_t **dep_task);
static int    H5_daos_attribute_iterate_by_name_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_iterate_by_name_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_iterate_by_crt_order(H5_daos_attr_iterate_ud_t *iterate_udata,
                                                     H5_daos_req_t *req, tse_task_t **first_task,
                                                     tse_task_t **dep_task);
static herr_t H5_daos_attribute_get_iter_op_task(H5_daos_attr_iterate_ud_t *iterate_udata,
                                                 const char *attr_name, H5_daos_req_t *req,
                                                 tse_task_t **first_task, tse_task_t **dep_task);
static int    H5_daos_attribute_iterate_op_task(tse_task_t *task);
static int    H5_daos_attribute_iterate_finish(tse_task_t *task);

static herr_t H5_daos_attribute_rename(H5_daos_obj_t *attr_container_obj, const char *cur_attr_name,
                                       const char *new_attr_name, hbool_t collective, H5_daos_req_t *req,
                                       tse_task_t **first_task, tse_task_t **dep_task);

static int    H5_daos_attr_gnbi_alloc_task(tse_task_t *task);
static herr_t H5_daos_attribute_get_name_by_idx_alloc(H5_daos_obj_t *target_obj, H5_index_t index_type,
                                                      H5_iter_order_t iter_order, uint64_t idx,
                                                      const char **attr_name, size_t *attr_name_size,
                                                      char **attr_name_buf, size_t *attr_name_buf_size,
                                                      H5_daos_req_t *req, tse_task_t **first_task,
                                                      tse_task_t **dep_task);
static herr_t H5_daos_attribute_get_name_by_idx(H5_daos_obj_t *target_obj, H5_index_t index_type,
                                                H5_iter_order_t iter_order, uint64_t idx, char *attr_name_out,
                                                size_t attr_name_out_size, size_t *attr_name_size,
                                                H5_daos_req_t *req, tse_task_t **first_task,
                                                tse_task_t **dep_task);
static herr_t H5_daos_attribute_get_name_by_name_order(H5_daos_attr_get_name_by_idx_ud_t *get_name_udata,
                                                       H5_daos_req_t *req, tse_task_t **first_task,
                                                       tse_task_t **dep_task);
static herr_t H5_daos_attribute_get_name_by_name_order_cb(hid_t loc_id, const char *attr_name,
                                                          const H5A_info_t *attr_info, void *op_data);
static int    H5_daos_attribute_gnbno_no_attrs_check_task(tse_task_t *task);
static herr_t H5_daos_attribute_get_name_by_crt_order(H5_daos_attr_get_name_by_idx_ud_t *get_name_udata,
                                                      H5_daos_req_t *req, tse_task_t **first_task,
                                                      tse_task_t **dep_task);
static int    H5_daos_attribute_get_name_by_crt_order_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_attribute_get_name_by_crt_order_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_attribute_get_name_by_idx_free_udata(H5_daos_attr_get_name_by_idx_ud_t *udata,
                                                           H5_daos_req_t *req, tse_task_t **first_task,
                                                           tse_task_t **dep_task);
static int    H5_daos_attribute_get_name_by_idx_free_udata_task(tse_task_t *task);
static herr_t H5_daos_attribute_get_crt_order_by_name(H5_daos_obj_t *target_obj, const char *attr_name,
                                                      uint64_t *crt_order);

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_akeys
 *
 * Purpose:     Helper routine to generate the DAOS akeys for an HDF5
 *              attribute. The caller is responsible for freeing the buffer
 *              returned in akey_buf_out when finished with the keys.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_akeys(const char *attr_name, daos_key_t *datatype_key, daos_key_t *dataspace_key,
                            daos_key_t *acpl_key, daos_key_t *acorder_key, daos_key_t *raw_data_key,
                            void **akey_buf_out)
{
    size_t total_buf_len = 0;
    size_t attr_name_len = 0;
    void  *akey_buf      = NULL;
    char  *akey_buf_ptr  = NULL;
    herr_t ret_value     = SUCCEED;

    assert(attr_name);
    assert(akey_buf_out);

    attr_name_len = strlen(attr_name);

    /* For each key requested, add enough space for a nul terminator and an akey string
     * of the attribute's name prefixed with specific characters, e.g. 'T-<attribute name>'
     * for the attribute's datatype key.
     */
    if (datatype_key)
        total_buf_len += attr_name_len + 3;
    if (dataspace_key)
        total_buf_len += attr_name_len + 3;
    if (acpl_key)
        total_buf_len += attr_name_len + 3;
    if (acorder_key)
        total_buf_len += attr_name_len + 3;
    if (raw_data_key)
        total_buf_len += attr_name_len + 3;

    /* Allocate a single buffer for all of the akey strings */
    if (NULL == (akey_buf = DV_malloc(total_buf_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey strings");
    akey_buf_ptr = akey_buf;

    /* Generate akey strings into buffer and set relevant positions into buffer
     * and buffer lengths for each daos_key_t requested. The buffer length set
     * for each daos_key_t doesn't include the nul terminator.
     */
    if (datatype_key) {
        daos_iov_set(datatype_key, (void *)akey_buf_ptr, attr_name_len + 2);
        snprintf(akey_buf_ptr, datatype_key->iov_len + 1, "T-%s", attr_name);
        akey_buf_ptr += datatype_key->iov_len + 1;
    }
    if (dataspace_key) {
        daos_iov_set(dataspace_key, (void *)akey_buf_ptr, attr_name_len + 2);
        snprintf(akey_buf_ptr, dataspace_key->iov_len + 1, "S-%s", attr_name);
        akey_buf_ptr += dataspace_key->iov_len + 1;
    }
    if (acpl_key) {
        daos_iov_set(acpl_key, (void *)akey_buf_ptr, attr_name_len + 2);
        snprintf(akey_buf_ptr, acpl_key->iov_len + 1, "P-%s", attr_name);
        akey_buf_ptr += acpl_key->iov_len + 1;
    }
    if (acorder_key) {
        daos_iov_set(acorder_key, (void *)akey_buf_ptr, attr_name_len + 2);
        snprintf(akey_buf_ptr, acorder_key->iov_len + 1, "C-%s", attr_name);
        akey_buf_ptr += acorder_key->iov_len + 1;
    }
    if (raw_data_key) {
        daos_iov_set(raw_data_key, (void *)akey_buf_ptr, attr_name_len + 2);
        snprintf(akey_buf_ptr, raw_data_key->iov_len + 1, "V-%s", attr_name);
        akey_buf_ptr += raw_data_key->iov_len + 1;
    }

    *akey_buf_out = akey_buf;

done:
    if (ret_value < 0 && akey_buf)
        DV_free(akey_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_akeys() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_md_rw_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_update or
 *              daos_obj_fetch for attribute metadata I/O. Similarly to
 *              H5_daos_md_rw_prep_cb, currently checks for errors from
 *              previous tasks then sets arguments for the DAOS operation.
 *              Additionally, sets the attribute's parent object as the
 *              target object for the I/O operation since the attribute's
 *              parent object won't have been opened until previous tasks
 *              have completed.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_md_rw_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_open_ud_t *udata;
    daos_obj_rw_t          *op_args;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute metadata I/O task");

    assert(udata->md_rw_cb_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_ATTR);

    assert(udata->md_rw_cb_ud.req->file);

    /* Now that the attribute's parent object will have been opened,
     * set the target object for the metadata I/O operation.
     */
    assert(udata->attr->parent);
    udata->md_rw_cb_ud.obj = udata->attr->parent;
    if (udata->bcast_udata)
        udata->bcast_udata->obj = udata->attr->parent;

    /* Set task arguments */
    if (NULL == (op_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for attribute metadata I/O task");
    memset(op_args, 0, sizeof(*op_args));
    op_args->oh    = udata->md_rw_cb_ud.obj->obj_oh;
    op_args->th    = DAOS_TX_NONE;
    op_args->flags = udata->md_rw_cb_ud.flags;
    op_args->dkey  = &udata->md_rw_cb_ud.dkey;
    op_args->nr    = udata->md_rw_cb_ud.nr;
    op_args->iods  = udata->md_rw_cb_ud.iod;
    op_args->sgls  = udata->md_rw_cb_ud.sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_md_rw_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create
 *
 * Purpose:     Sends a request to DAOS to create an attribute
 *
 * Return:      Success:        attribute object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_attribute_create(void *_item, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id,
                         hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t H5VL_DAOS_UNUSED dxpl_id,
                         void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item       = (H5_daos_item_t *)_item;
    H5_daos_attr_t *attr       = NULL;
    H5_daos_req_t  *int_req    = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    hbool_t         collective;
    int             ret;
    void           *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");
    if (!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute name is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /* Check for write access */
    if (!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file");

    /*
     * Determine if independent metadata writes have been requested. Otherwise,
     * like HDF5, metadata writes are collective by default.
     */
    H5_DAOS_GET_METADATA_WRITE_MODE(item->file, aapl_id, H5P_ATTRIBUTE_ACCESS_DEFAULT, collective, H5E_ATTR,
                                    NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "attribute create", item->open_req, NULL, NULL,
                                              H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, 0, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Create attribute */
    if (NULL == (attr = (H5_daos_attr_t *)H5_daos_attribute_create_helper(item, loc_params, type_id, space_id,
                                                                          acpl_id, aapl_id, name, collective,
                                                                          int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create attribute");

    /* Set return value */
    ret_value = (void *)attr;

done:
    if (int_req) {
        H5_daos_op_pool_type_t op_type;

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Determine operation type - we will add the operation to item's
         * op pool.  If the attribute is NULL (something failed), use
         * H5_DAOS_OP_TYPE_NOPOOL.  If the attribute's parent object might have
         * attribute creation order tracked and the attribute parent object is
         * not different from item use H5_DAOS_OP_TYPE_WRITE_ORDERED, otherwise
         * use H5_DAOS_OP_TYPE_WRITE.  Add to item's pool because that's where
         * we're creating the attribute.  No need to add to attribute's pool
         * since it's the open request. */
        if (!attr)
            op_type = H5_DAOS_OP_TYPE_NOPOOL;
        else if (!attr->parent || &attr->parent->item != item ||
                 ((attr->parent->item.open_req->status == 0 || attr->parent->item.created) &&
                  !attr->parent->ocpl_cache.track_acorder))
            op_type = H5_DAOS_OP_TYPE_WRITE;
        else
            op_type = H5_DAOS_OP_TYPE_WRITE_ORDERED;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the parent object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, op_type, H5_DAOS_OP_SCOPE_OBJ, collective, !req) <
            0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, NULL, "attribute creation failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value)
        /* Close attribute */
        if (attr && H5_daos_attribute_close_real(attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close attribute");

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create_helper
 *
 * Purpose:     Performs the actual attribute creation.
 *
 * Return:      Success:        attribute object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_attribute_create_helper(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params, hid_t type_id,
                                hid_t space_id, hid_t acpl_id, hid_t H5VL_DAOS_UNUSED aapl_id,
                                const char *attr_name, hbool_t collective, H5_daos_req_t *req,
                                tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_create_ud_t *create_ud = NULL;
    H5_daos_attr_t           *attr      = NULL;
    tse_task_t               *update_task;
    H5_daos_req_t            *int_int_req  = NULL;
    hbool_t                   default_acpl = (acpl_id == H5P_ATTRIBUTE_CREATE_DEFAULT);
    int                       ret;
    void                     *ret_value = NULL;

    assert(item);
    assert(item->file->flags & H5F_ACC_RDWR);
    assert(loc_params);
    assert(attr_name);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    /* Allocate the attribute object that is returned to the user */
    if (NULL == (attr = H5FL_CALLOC(H5_daos_attr_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS attribute struct");
    attr->item.type     = H5I_ATTR;
    attr->item.created  = TRUE;
    attr->item.open_req = req;
    req->rc++;
    attr->item.file    = item->file;
    attr->item.rc      = 1;
    attr->type_id      = H5I_INVALID_HID;
    attr->file_type_id = H5I_INVALID_HID;
    attr->space_id     = H5I_INVALID_HID;
    attr->acpl_id      = H5P_ATTRIBUTE_CREATE_DEFAULT;
    if (NULL == (attr->name = strdup(attr_name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name");

    /* Determine attribute's parent object */
    if (loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as attribute parent object, or the root group if item is a file */
        if (item->type == H5I_FILE)
            attr->parent = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
        else
            attr->parent = (H5_daos_obj_t *)item;
        attr->parent->item.rc++;
    } /* end if */
    else if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        /* Start internal H5 operation for target object open.  This will
         * not be visible to the API, will not be added to an operation
         * pool, and will be integrated into this function's task chain. */
        if (NULL == (int_int_req =
                         H5_daos_req_create(item->file, "target object open within attribute create by name",
                                            NULL, NULL, req, H5I_INVALID_HID)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL, "can't create DAOS request");

        /* Open target_obj */
        if (H5_daos_object_open_helper(item, loc_params, NULL, collective, NULL, &attr->parent, int_int_req,
                                       first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open parent object for attribute");

        /* Create task to finalize internal operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                                NULL, int_int_req, &int_int_req->finalize_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create task to finalize internal operation");

        /* Schedule finalize task (or save it to be scheduled later),
         * give it ownership of int_int_req, and update task pointers */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = int_int_req->finalize_task;
        *dep_task   = int_int_req->finalize_task;
        int_int_req = NULL;
    } /* end else */
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, NULL,
                     "unsupported attribute create location parameters type");

    /* Create attribute and write metadata if this process should */
    if (!collective || (item->file->my_rank == 0)) {
        size_t  type_size  = 0;
        size_t  space_size = 0;
        size_t  acpl_size  = 0;
        void   *type_buf   = NULL;
        void   *space_buf  = NULL;
        void   *acpl_buf   = NULL;
        hbool_t may_track_acorder =
            !attr->parent || (attr->parent->item.open_req->status < 0 && !attr->parent->item.created) ||
            attr->parent->ocpl_cache.track_acorder;

        /* Determine serialized datatype size */
        if (H5Tencode(type_id, NULL, &type_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype");

        /* Determine serialized dataspace size */
        if (H5Sencode2(space_id, NULL, &space_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataspace");

        /* Determine serialized ACPL size if not the default */
        if (!default_acpl)
            if (H5Pencode2(acpl_id, NULL, &acpl_size, item->file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of acpl");

        /* Allocate argument struct */
        if (NULL == (create_ud = (H5_daos_attr_create_ud_t *)DV_calloc(sizeof(H5_daos_attr_create_ud_t) +
                                                                       type_size + space_size + acpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for update callback arguments");
        create_ud->req       = req;
        create_ud->attr      = attr;
        create_ud->akeys_buf = NULL;

        /* Encode datatype */
        type_buf = create_ud->flex_buf;
        if (H5Tencode(type_id, type_buf, &type_size) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTENCODE, NULL, "can't serialize datatype");

        /* Encode dataspace */
        space_buf = create_ud->flex_buf + type_size;
        if (H5Sencode2(space_id, space_buf, &space_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTENCODE, NULL, "can't serialize dataspace");

        /* Encode ACPL if not the default */
        if (!default_acpl) {
            acpl_buf = create_ud->flex_buf + type_size + space_size;
            if (H5Pencode2(acpl_id, acpl_buf, &acpl_size, item->file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTENCODE, NULL, "can't serialize acpl");
        } /* end if */
        else {
            acpl_buf  = item->file->def_plist_cache.acpl_buf;
            acpl_size = item->file->def_plist_cache.acpl_size;
        } /* end else */

        /* Set up operation to write datatype, dataspace and ACPL to attribute's parent object */
        /* obj field is not used */
        create_ud->md_rw_cb_ud.obj = NULL;

        /* Point to req */
        create_ud->md_rw_cb_ud.req = req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&create_ud->md_rw_cb_ud.dkey, H5_daos_attr_key_g,
                           H5_daos_attr_key_size_g);
        create_ud->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up akey strings (attribute name prefixed with 'T-', 'S-' and 'P-' for
         * datatype, dataspace and ACPL, respectively) */
        if (H5_daos_attribute_get_akeys(attr_name, &create_ud->akeys[0], &create_ud->akeys[1],
                                        &create_ud->akeys[2], may_track_acorder ? &create_ud->akeys[3] : NULL,
                                        NULL, &create_ud->akeys_buf) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL, "can't get akey strings");

        /* Set up iod */

        /* iod[0] contains the key for the datatype description */
        daos_iov_set(&create_ud->md_rw_cb_ud.iod[0].iod_name, (void *)create_ud->akeys[0].iov_buf,
                     (daos_size_t)create_ud->akeys[0].iov_len);
        create_ud->md_rw_cb_ud.iod[0].iod_nr    = 1u;
        create_ud->md_rw_cb_ud.iod[0].iod_size  = (uint64_t)type_size;
        create_ud->md_rw_cb_ud.iod[0].iod_type  = DAOS_IOD_SINGLE;
        create_ud->md_rw_cb_ud.iod[0].iod_flags = DAOS_COND_AKEY_INSERT;

        /* iod[1] contains the key for the dataspace description */
        daos_iov_set(&create_ud->md_rw_cb_ud.iod[1].iod_name, (void *)create_ud->akeys[1].iov_buf,
                     (daos_size_t)create_ud->akeys[1].iov_len);
        create_ud->md_rw_cb_ud.iod[1].iod_nr    = 1u;
        create_ud->md_rw_cb_ud.iod[1].iod_size  = (uint64_t)space_size;
        create_ud->md_rw_cb_ud.iod[1].iod_type  = DAOS_IOD_SINGLE;
        create_ud->md_rw_cb_ud.iod[1].iod_flags = DAOS_COND_AKEY_INSERT;

        /* iod[2] contains the key for the ACPL */
        daos_iov_set(&create_ud->md_rw_cb_ud.iod[2].iod_name, (void *)create_ud->akeys[2].iov_buf,
                     (daos_size_t)create_ud->akeys[2].iov_len);
        create_ud->md_rw_cb_ud.iod[2].iod_nr    = 1u;
        create_ud->md_rw_cb_ud.iod[2].iod_size  = (uint64_t)acpl_size;
        create_ud->md_rw_cb_ud.iod[2].iod_type  = DAOS_IOD_SINGLE;
        create_ud->md_rw_cb_ud.iod[2].iod_flags = DAOS_COND_AKEY_INSERT;

        create_ud->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */

        /* sgl[0] contains the serialized datatype description */
        daos_iov_set(&create_ud->md_rw_cb_ud.sg_iov[0], type_buf, (daos_size_t)type_size);
        create_ud->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        create_ud->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        create_ud->md_rw_cb_ud.sgl[0].sg_iovs   = &create_ud->md_rw_cb_ud.sg_iov[0];
        create_ud->md_rw_cb_ud.free_sg_iov[0]   = FALSE;

        /* sgl[1] contains the serialized dataspace description */
        daos_iov_set(&create_ud->md_rw_cb_ud.sg_iov[1], space_buf, (daos_size_t)space_size);
        create_ud->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        create_ud->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        create_ud->md_rw_cb_ud.sgl[1].sg_iovs   = &create_ud->md_rw_cb_ud.sg_iov[1];
        create_ud->md_rw_cb_ud.free_sg_iov[1]   = FALSE;

        /* sgl[2] contains the serialized ACPL */
        daos_iov_set(&create_ud->md_rw_cb_ud.sg_iov[2], acpl_buf, (daos_size_t)acpl_size);
        create_ud->md_rw_cb_ud.sgl[2].sg_nr     = 1;
        create_ud->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        create_ud->md_rw_cb_ud.sgl[2].sg_iovs   = &create_ud->md_rw_cb_ud.sg_iov[2];
        create_ud->md_rw_cb_ud.free_sg_iov[2]   = FALSE;

        /* Set nr */
        create_ud->md_rw_cb_ud.nr = 3u;

        /* Set task name */
        create_ud->md_rw_cb_ud.task_name = "attribute metadata write";

        /* Check for creation order tracking.  If we're not sure if creation
         * order is tracked because the parent object open isn't complete, call
         * the function anyways, the prep callback will check for creation order
         * before actually fetching any info. */
        if (may_track_acorder) {
            if (H5_daos_attribute_create_get_crt_order_info(create_ud, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                             "can't create task to write attribute creation order metadata");
        } /* end if */
        else
            /* Set conditional per-akey insert for the attribute metadata write operation */
            create_ud->md_rw_cb_ud.flags = DAOS_COND_PER_AKEY;

        /* Create task for attribute metadata write */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_attribute_create_helper_prep_cb,
                                     H5_daos_attribute_create_helper_comp_cb, create_ud, &update_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create task to write attribute metadata");

        /* Schedule attribute metadata write task (or save it to be scheduled later)
         * and give it a reference to req and the attribute */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(update_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                             "can't schedule task to write attribute metadata: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = update_task;
        req->rc++;
        attr->item.rc++;
        *dep_task = update_task;

        create_ud = NULL;
    } /* end if */

    /* Finish setting up attribute struct */
    if ((attr->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, NULL, "failed to copy datatype");
    if ((attr->file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, type_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "failed to get file datatype");
    if (0 == (attr->file_type_size = H5Tget_size(attr->file_type_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL, "can't get file datatype size");
    if ((attr->space_id = H5Scopy(space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, NULL, "failed to copy dataspace");
    if (!default_acpl && (attr->acpl_id = H5Pcopy(acpl_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, NULL, "failed to copy ACPL");
    if (H5Sselect_all(attr->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection");

    ret_value = (void *)attr;

done:
    if (collective && (item->file->num_procs > 1))
        if (H5_daos_collective_error_check(attr->parent, req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't perform collective error check");

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value) {
        /* Close attribute */
        if (attr) {
            tse_task_t                  *close_task    = NULL;
            H5_daos_obj_close_task_ud_t *close_task_ud = NULL;

            /* Must create asynchronous task to close attribute so an object open
             * doesn't try to place an object pointer in the attribute struct after
             * it has been freed */
            /* Allocate argument struct */
            if (NULL == (close_task_ud =
                             (H5_daos_obj_close_task_ud_t *)DV_calloc(sizeof(H5_daos_obj_close_task_ud_t))))
                D_DONE_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL,
                             "can't allocate space for close task udata struct");
            close_task_ud->req  = req;
            close_task_ud->item = &attr->item;

            /* Create task to close attribute */
            if (H5_daos_create_task(H5_daos_object_close_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                    NULL, NULL, close_task_ud, &close_task) < 0) {
                close_task_ud = DV_free(close_task_ud);
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create task to close attribute");
            } /* end if */
            else {
                /* Save task to be scheduled later and give it a reference to req and
                 * attr */
                if (*first_task) {
                    if (0 != (ret = tse_task_schedule(close_task, false))) {
                        close_task_ud = DV_free(close_task_ud);
                        D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                                     "can't schedule task to write attribute metadata: %s",
                                     H5_daos_err_to_string(ret));
                    } /* end if */
                }     /* end if */
                else
                    *first_task = close_task;
                if (close_task_ud) {
                    *dep_task = close_task;
                    /* No need to take a reference to attr here since the purpose is to
                     * release the API's reference */
                    req->rc++;
                    close_task_ud = NULL;
                } /* end if */
            }     /* end else */
        }         /* end if */

        /* Close internal request for target object open */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't free request");
        int_int_req = NULL;

        /* Free memory */
        create_ud = DV_free(create_ud);
    } /* end if */

    assert(!create_ud);
    assert(!int_int_req);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_create_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create_helper_prep_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update to write
 *              attribute metadata to an object.  Currently checks for
 *              errors from previous tasks then sets arguments for the
 *              DAOS operation, including the object.
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
H5_daos_attribute_create_helper_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_create_ud_t *udata;
    daos_obj_rw_t            *update_args;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->md_rw_cb_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_ATTR);

    assert(udata->attr);
    assert(udata->attr->parent);
    assert(udata->attr->parent->item.file);

    /* Set update task arguments */
    if (NULL == (update_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for metadata I/O task");
    memset(update_args, 0, sizeof(*update_args));
    update_args->oh    = udata->attr->parent->obj_oh;
    update_args->th    = udata->md_rw_cb_ud.req->th;
    update_args->flags = udata->md_rw_cb_ud.flags;
    update_args->dkey  = &udata->md_rw_cb_ud.dkey;
    update_args->nr    = udata->md_rw_cb_ud.nr;
    update_args->iods  = udata->md_rw_cb_ud.iod;
    update_args->sgls  = udata->md_rw_cb_ud.sgl;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_create_helper_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create_helper_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update to write
 *              attribute metadata to an object.  Currently checks for a
 *              failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_create_helper_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_create_ud_t *udata;
    unsigned                  i;
    int                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */

    /* Close attribute  */
    if (H5_daos_attribute_close_real(udata->attr) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    if (udata->md_rw_cb_ud.free_dkey)
        DV_free(udata->md_rw_cb_ud.dkey.iov_buf);
    if (udata->md_rw_cb_ud.free_akeys)
        for (i = 0; i < udata->md_rw_cb_ud.nr; i++)
            DV_free(udata->md_rw_cb_ud.iod[i].iod_name.iov_buf);
    for (i = 0; i < udata->md_rw_cb_ud.nr; i++)
        if (udata->md_rw_cb_ud.free_sg_iov[i])
            DV_free(udata->md_rw_cb_ud.sg_iov[i].iov_buf);
    DV_free(udata->akeys_buf);
    DV_free(udata);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_create_helper_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create_get_crt_order_info
 *
 * Purpose:     Creates an asynchronous task to retrieve attribute creation
 *              order-related info during attribute creation and then
 *              include updated creation order information in the
 *              subsequent daos_obj_update call that actually writes the
 *              new attribute. To cut down on daos_obj_update calls needed,
 *              this routine shares a udata structure with the attribute
 *              creation helper routine so that all metadata can be written
 *              at once instead of in two phases.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_create_get_crt_order_info(H5_daos_attr_create_ud_t *create_ud, H5_daos_req_t *req,
                                            tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *fetch_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(create_ud);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(create_ud->md_rw_cb_ud.nr == 3u);

    /* Create task to read object's current number of attributes
     * and maximum attribute creation order value
     */

    /* Modify existing iod. iod[3] contains the key for the number of attributes.
     * iod[4] contains the key for the object's max. attribute creation order value.
     * iod[0], iod[1] and iod[2] are assumed to already be set.
     */
    daos_const_iov_set((d_const_iov_t *)&create_ud->md_rw_cb_ud.iod[3].iod_name, H5_daos_nattr_key_g,
                       H5_daos_nattr_key_size_g);
    create_ud->md_rw_cb_ud.iod[3].iod_nr    = 1u;
    create_ud->md_rw_cb_ud.iod[3].iod_size  = (uint64_t)8;
    create_ud->md_rw_cb_ud.iod[3].iod_type  = DAOS_IOD_SINGLE;
    create_ud->md_rw_cb_ud.iod[3].iod_flags = 0;

    daos_const_iov_set((d_const_iov_t *)&create_ud->md_rw_cb_ud.iod[4].iod_name,
                       H5_daos_max_attr_corder_key_g, H5_daos_max_attr_corder_key_size_g);
    create_ud->md_rw_cb_ud.iod[4].iod_nr    = 1u;
    create_ud->md_rw_cb_ud.iod[4].iod_size  = (uint64_t)8;
    create_ud->md_rw_cb_ud.iod[4].iod_type  = DAOS_IOD_SINGLE;
    create_ud->md_rw_cb_ud.iod[4].iod_flags = 0;

    /* Modify existing sgl.
     *
     * sgl[3] contains the read buffer for the number of attributes.
     * We will reuse this buffer in sgl[5] after the read operation.  When it's
     * written to disk it needs to contain a leading 0 byte to guarantee it
     * doesn't conflict with a string akey used in the attribute dkey, so we
     * will read the number of attributes to the last 8 bytes of the buffer.
     *
     * sgl[4] contains the read buffer for the object's max. attribute creation
     * order value. It is used to determine an attribute's permanent creation
     * order value.
     */
    create_ud->nattr_old_buf[0] = 0;
    daos_iov_set(&create_ud->md_rw_cb_ud.sg_iov[3], &create_ud->nattr_old_buf[1], (daos_size_t)8);
    create_ud->md_rw_cb_ud.sgl[3].sg_nr     = 1;
    create_ud->md_rw_cb_ud.sgl[3].sg_nr_out = 0;
    create_ud->md_rw_cb_ud.sgl[3].sg_iovs   = &create_ud->md_rw_cb_ud.sg_iov[3];
    create_ud->md_rw_cb_ud.free_sg_iov[3]   = FALSE;

    daos_iov_set(&create_ud->md_rw_cb_ud.sg_iov[4], create_ud->max_corder_old_buf, (daos_size_t)8);
    create_ud->md_rw_cb_ud.sgl[4].sg_nr     = 1;
    create_ud->md_rw_cb_ud.sgl[4].sg_nr_out = 0;
    create_ud->md_rw_cb_ud.sgl[4].sg_iovs   = &create_ud->md_rw_cb_ud.sg_iov[4];
    create_ud->md_rw_cb_ud.free_sg_iov[4]   = FALSE;

    /* Create task for attribute creation order metadata fetch */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_attribute_create_get_crt_order_info_prep_cb,
                                 H5_daos_attribute_create_get_crt_order_info_comp_cb, create_ud,
                                 &fetch_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                     "can't create task to fetch attribute creation order metadata");

    /* Schedule attribute creation order metadata fetch task (or save it to be scheduled later)
     * and give it a reference to req and the attribute */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't schedule task to fetch attribute creation order metadata: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = fetch_task;
    req->rc++;
    create_ud->attr->item.rc++;
    *dep_task = fetch_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_create_get_crt_order_info() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create_get_crt_order_info_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous task to retrieve
 *              attribute creation order-related info during attribute
 *              creation. Currently checks for errors from previous tasks
 *              then sets arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_create_get_crt_order_info_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_create_ud_t *udata;
    daos_obj_rw_t            *update_args;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->md_rw_cb_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_ATTR);

    assert(udata->attr);
    assert(udata->attr->parent);
    assert(udata->attr->parent->item.file);

    /* Check if creation order is actually tracked */
    if (udata->attr->parent->ocpl_cache.track_acorder) {
        /* Set update task arguments */
        if (NULL == (update_args = daos_task_get_args(task)))
            D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                         "can't get arguments for metadata I/O task");
        memset(update_args, 0, sizeof(*update_args));
        update_args->oh    = udata->attr->parent->obj_oh;
        update_args->th    = DAOS_TX_NONE;
        update_args->flags = udata->md_rw_cb_ud.flags;
        update_args->dkey  = &udata->md_rw_cb_ud.dkey;
        update_args->nr    = 2u;
        update_args->iods  = &udata->md_rw_cb_ud.iod[3];
        update_args->sgls  = &udata->md_rw_cb_ud.sgl[3];
    } /* end if */
    else
        tse_task_complete(task, 0);

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_create_get_crt_order_info_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create_get_crt_order_info_comp_cb
 *
 * Purpose:     Complete callback for asynchronous task to retrieve
 *              attribute creation order-related info during attribute
 *              creation. Currently checks for a failed task, then adjusts
 *              the I/O task parameters for a subsequent daos_obj_update
 *              call.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_create_get_crt_order_info_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_create_ud_t *udata;
    int                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed).  Also skip
     * processing if creaiton order is not tracked. */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0 && udata->attr->parent->ocpl_cache.track_acorder) {
        uint64_t max_corder;
        uint64_t nattr;
        size_t   name_len = strlen(udata->attr->name);
        uint8_t *p;

        p = &udata->nattr_old_buf[1];

        /* Check for no num attributes found, in this case it must be 0 */
        if (udata->md_rw_cb_ud.iod[3].iod_size == (uint64_t)0) {
            nattr = 0;
            UINT64ENCODE(p, nattr);

            /* Reset iod size */
            udata->md_rw_cb_ud.iod[3].iod_size = (uint64_t)8;
        } /* end if */
        else {
            /* Verify the iod size was 8 as expected */
            if (udata->md_rw_cb_ud.iod[3].iod_size != (uint64_t)8)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, -H5_DAOS_BAD_VALUE,
                             "invalid size of number of attributes value");

            /* Decode num attributes */
            UINT64DECODE(p, nattr);
        } /* end else */

        /* Add new attribute to count */
        nattr++;

        p = udata->max_corder_old_buf;

        /* Check for no max creation order record found, in which case it must be 0 */
        if (udata->md_rw_cb_ud.iod[4].iod_size == (uint64_t)0) {
            max_corder = 0;
            UINT64ENCODE(p, max_corder);

            /* Reset iod size */
            udata->md_rw_cb_ud.iod[4].iod_size = (uint64_t)8;
        } /* end if */
        else {
            /* Verify the iod size was 8 as expected */
            if (udata->md_rw_cb_ud.iod[4].iod_size != (uint64_t)8)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, -H5_DAOS_BAD_VALUE,
                             "invalid size of maximum attribute creation order record");

            /* Decode max. attribute creation order */
            UINT64DECODE(p, max_corder);
        } /* end else */

        /* Increase max. creation order value */
        max_corder++;

        /* Add creation order info to write command */
        /* Encode new num attributes and max. creation order */
        p = udata->nattr_new_buf;
        UINT64ENCODE(p, nattr);
        p = udata->max_corder_new_buf;
        UINT64ENCODE(p, max_corder);

        /* Set up iod for subsequent daos_obj_update call.
         * iod[3] contains the key for the number of attributes.
         * Already set up from read operation.
         * iod[4] contains the key for the object's maximum attribute
         * creation order value. Already set up from read operation.
         */

        /* iod[5] contains the creation order of the new attribute, used as
         * an akey for retrieving the attribute name to enable attribute
         * lookup by creation order */
        daos_iov_set(&udata->md_rw_cb_ud.iod[5].iod_name, (void *)udata->nattr_old_buf, 9);
        udata->md_rw_cb_ud.iod[5].iod_nr    = 1u;
        udata->md_rw_cb_ud.iod[5].iod_size  = (uint64_t)name_len;
        udata->md_rw_cb_ud.iod[5].iod_type  = DAOS_IOD_SINGLE;
        udata->md_rw_cb_ud.iod[5].iod_flags = 0;

        /* iod[6] contains the key for the creation order, to enable attribute
         * creation order lookup by name */
        daos_iov_set(&udata->md_rw_cb_ud.iod[6].iod_name, udata->akeys[3].iov_buf,
                     (daos_size_t)udata->akeys[3].iov_len);
        udata->md_rw_cb_ud.iod[6].iod_nr    = 1u;
        udata->md_rw_cb_ud.iod[6].iod_size  = (uint64_t)8;
        udata->md_rw_cb_ud.iod[6].iod_type  = DAOS_IOD_SINGLE;
        udata->md_rw_cb_ud.iod[6].iod_flags = 0;

        /* Set up sgl for subsequent daos_obj_update call. */

        /* sgl[3] contains the number of attributes, updated to include
         * the new attribute */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[3], udata->nattr_new_buf, (daos_size_t)8);
        udata->md_rw_cb_ud.sgl[3].sg_nr     = 1;
        udata->md_rw_cb_ud.sgl[3].sg_nr_out = 0;
        udata->md_rw_cb_ud.sgl[3].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[3];
        udata->md_rw_cb_ud.free_sg_iov[3]   = FALSE;

        /* sgl[4] contains the object's maximum creation order value, updated
         * to include the new attribute
         */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[4], udata->max_corder_new_buf, (daos_size_t)8);
        udata->md_rw_cb_ud.sgl[4].sg_nr     = 1;
        udata->md_rw_cb_ud.sgl[4].sg_nr_out = 0;
        udata->md_rw_cb_ud.sgl[4].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[4];
        udata->md_rw_cb_ud.free_sg_iov[4]   = FALSE;

        /* sgl[5] contains the attribute name, here indexed using the creation
         * order as the akey to enable attribute lookup by creation order */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[5], (void *)udata->attr->name, (daos_size_t)name_len);
        udata->md_rw_cb_ud.sgl[5].sg_nr     = 1;
        udata->md_rw_cb_ud.sgl[5].sg_nr_out = 0;
        udata->md_rw_cb_ud.sgl[5].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[5];
        udata->md_rw_cb_ud.free_sg_iov[5]   = FALSE;

        /* sgl[6] contains the creation order (with no leading 0), to enable
         * attribute creation order lookup by name */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[6], udata->max_corder_old_buf, (daos_size_t)8);
        udata->md_rw_cb_ud.sgl[6].sg_nr     = 1;
        udata->md_rw_cb_ud.sgl[6].sg_nr_out = 0;
        udata->md_rw_cb_ud.sgl[6].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[6];
        udata->md_rw_cb_ud.free_sg_iov[6]   = FALSE;

        /* Update nr for subsequent daos_obj_update call */
        udata->md_rw_cb_ud.nr = 7u;

        /* Set conditional per-akey insert for the attribute metadata write operation */
        udata->md_rw_cb_ud.flags = DAOS_COND_PER_AKEY;
    } /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        if (udata->attr && H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute creation order info fetch task completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_create_get_crt_order_info_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open
 *
 * Purpose:     Sends a request to DAOS to open an attribute
 *
 * Return:      Success:        attribute object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_attribute_open(void *_item, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id,
                       hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item       = (H5_daos_item_t *)_item;
    H5_daos_attr_t *attr       = NULL;
    H5_daos_req_t  *int_req    = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    hbool_t         collective;
    int             ret;
    void           *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");
    if (!name && (H5VL_OBJECT_BY_IDX != loc_params->type))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute name is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has
     * specifically requested collective metadata reads, they will be enabled here.
     */
    H5_DAOS_GET_METADATA_READ_MODE(item->file, aapl_id, H5P_ATTRIBUTE_ACCESS_DEFAULT, collective, H5E_ATTR,
                                   NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "attribute open", item->open_req, NULL, NULL,
                                              H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL, "can't create DAOS request");

    if (NULL == (attr = H5_daos_attribute_open_helper(item, loc_params, name, aapl_id, collective, int_req,
                                                      &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open attribute");

    ret_value = (void *)attr;

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the parent object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_OBJ,
                                collective, !req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, NULL, "attribute opening failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* If we are not returning an attribute we must close it */
    if (ret_value == NULL && attr && H5_daos_attribute_close_real(attr) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close attribute");

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open_helper
 *
 * Purpose:     Internal-use helper routine to create an asynchronous task
 *              for opening a DAOS HDF5 attribute.
 *
 * Return:      Success:        attribute object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5_daos_attr_t *
H5_daos_attribute_open_helper(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
                              const char *attr_name, hid_t H5VL_DAOS_UNUSED aapl_id, hbool_t collective,
                              H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_open_ud_t   *open_udata  = NULL;
    H5_daos_attr_ibcast_ud_t *bcast_udata = NULL;
    H5_daos_attr_t           *attr        = NULL;
    daos_key_t                akeys[3];
    size_t                    ainfo_buf_size = 0;
    void                     *akeys_buf      = NULL;
    H5_daos_req_t            *int_int_req    = NULL;
    int                       ret;
    H5_daos_attr_t           *ret_value = NULL;

    assert(item);
    assert(loc_params);
    assert(req);
    assert(first_task);
    assert(dep_task);
    if (H5VL_OBJECT_BY_IDX != loc_params->type)
        assert(attr_name);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    /* Allocate the attribute object that is returned to the user */
    if (NULL == (attr = H5FL_CALLOC(H5_daos_attr_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS attribute struct");
    attr->item.type     = H5I_ATTR;
    attr->item.open_req = req;
    req->rc++;
    attr->item.file    = item->file;
    attr->item.rc      = 1;
    attr->parent       = NULL;
    attr->name         = NULL;
    attr->type_id      = H5I_INVALID_HID;
    attr->file_type_id = H5I_INVALID_HID;
    attr->space_id     = H5I_INVALID_HID;
    attr->acpl_id      = H5P_ATTRIBUTE_CREATE_DEFAULT;

    /* Set up broadcast user data (if appropriate) and calculate initial attribute
     * info buffer size */
    if (collective && (item->file->num_procs > 1)) {
        if (NULL == (bcast_udata = (H5_daos_attr_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_attr_ibcast_ud_t) +
                                                                         H5_DAOS_AINFO_BCAST_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->bcast_ud.req        = req;
        bcast_udata->bcast_ud.obj        = NULL; /* Set later after parent object is opened */
        bcast_udata->bcast_ud.buffer     = bcast_udata->flex_buf;
        bcast_udata->bcast_ud.buffer_len = H5_DAOS_AINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_ud.count      = H5_DAOS_AINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_ud.comm       = req->file->comm;
        bcast_udata->attr                = attr;

        ainfo_buf_size = H5_DAOS_AINFO_BCAST_BUF_SIZE;
    } /* end if */
    else
        ainfo_buf_size = H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_ACPL_BUF_SIZE;

    /* Determine attribute's name and parent object */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_SELF: {
            /* Use item as attribute parent object, or the root group if item is a file */
            if (item->type == H5I_FILE)
                attr->parent = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
            else
                attr->parent = (H5_daos_obj_t *)item;
            attr->parent->item.rc++;

            /* Set attribute's name */
            if (NULL == (attr->name = strdup(attr_name)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name");

            break;
        } /* H5VL_OBJECT_BY_SELF */

        case H5VL_OBJECT_BY_NAME: {
            /* Start internal H5 operation for target object open.  This will
             * not be visible to the API, will not be added to an operation
             * pool, and will be integrated into this function's task chain. */
            if (NULL == (int_int_req = H5_daos_req_create(item->file,
                                                          "target object open within attribute open by name",
                                                          NULL, NULL, req, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL, "can't create DAOS request");

            /* Open target_obj */
            if (H5_daos_object_open_helper(item, loc_params, NULL, collective, NULL, &attr->parent,
                                           int_int_req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open parent object for attribute");

            /* Create task to finalize internal operation */
            if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                    NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                             "can't create task to finalize internal operation");

            /* Schedule finalize task (or save it to be scheduled later),
             * give it ownership of int_int_req, and update task pointers */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                                 "can't schedule task to finalize H5 operation: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                *first_task = int_int_req->finalize_task;
            *dep_task   = int_int_req->finalize_task;
            int_int_req = NULL;

            /* Set attribute's name */
            if (NULL == (attr->name = strdup(attr_name)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name");

            break;
        } /* H5VL_OBJECT_BY_NAME */

        case H5VL_OBJECT_BY_IDX: {
            if (H5_daos_attribute_open_by_idx_helper((H5_daos_obj_t *)item, loc_params, attr, req, first_task,
                                                     dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL,
                             "can't get attribute's parent object and name by index");

            H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, NULL);

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_TOKEN:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, NULL,
                         "invalid or unsupported attribute open location parameters type");
    } /* end switch */

    if (!collective || (item->file->my_rank == 0)) {
        tse_task_t *fetch_task;
        uint8_t    *p;

        /* Set up akey strings (attribute name prefixed with 'T-', 'S-' and 'P-' for
         * datatype, dataspace and ACPL, respectively) */
        if (H5_daos_attribute_get_akeys(attr->name, &akeys[0], &akeys[1], &akeys[2], NULL, NULL, &akeys_buf) <
            0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL, "can't generate akey strings");

        /* Allocate argument struct for fetch task */
        if (NULL == (open_udata = (H5_daos_attr_open_ud_t *)DV_calloc(sizeof(H5_daos_attr_open_ud_t) +
                                                                      (bcast_udata ? 0 : ainfo_buf_size))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for fetch callback arguments");
        open_udata->attr = attr;

        /* Set up operation to read datatype, dataspace, and ACPL sizes from attribute */
        /* Set up ud struct */
        open_udata->md_rw_cb_ud.req = req;
        open_udata->md_rw_cb_ud.obj = NULL; /* Set later, once attribute's parent object is opened */
        open_udata->bcast_udata     = (H5_daos_mpi_ibcast_ud_t *)bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&open_udata->md_rw_cb_ud.dkey, H5_daos_attr_key_g,
                           H5_daos_attr_key_size_g);
        open_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod */
        daos_iov_set(&open_udata->md_rw_cb_ud.iod[0].iod_name, akeys[0].iov_buf,
                     (daos_size_t)akeys[0].iov_len);
        open_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        open_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
        open_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&open_udata->md_rw_cb_ud.iod[1].iod_name, akeys[1].iov_buf,
                     (daos_size_t)akeys[1].iov_len);
        open_udata->md_rw_cb_ud.iod[1].iod_nr   = 1u;
        open_udata->md_rw_cb_ud.iod[1].iod_size = DAOS_REC_ANY;
        open_udata->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&open_udata->md_rw_cb_ud.iod[2].iod_name, akeys[2].iov_buf,
                     (daos_size_t)akeys[2].iov_len);
        open_udata->md_rw_cb_ud.iod[2].iod_nr   = 1u;
        open_udata->md_rw_cb_ud.iod[2].iod_size = DAOS_REC_ANY;
        open_udata->md_rw_cb_ud.iod[2].iod_type = DAOS_IOD_SINGLE;

        open_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up buffer */
        if (bcast_udata)
            p = bcast_udata->flex_buf + (3 * H5_DAOS_ENCODED_UINT64_T_SIZE);
        else
            p = open_udata->flex_buf;

        /* Set up sgl */
        daos_iov_set(&open_udata->md_rw_cb_ud.sg_iov[0], p, (daos_size_t)H5_DAOS_TYPE_BUF_SIZE);
        open_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        open_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        open_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &open_udata->md_rw_cb_ud.sg_iov[0];
        open_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        p += H5_DAOS_TYPE_BUF_SIZE;
        daos_iov_set(&open_udata->md_rw_cb_ud.sg_iov[1], p, (daos_size_t)H5_DAOS_SPACE_BUF_SIZE);
        open_udata->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        open_udata->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        open_udata->md_rw_cb_ud.sgl[1].sg_iovs   = &open_udata->md_rw_cb_ud.sg_iov[1];
        open_udata->md_rw_cb_ud.free_sg_iov[1]   = FALSE;
        p += H5_DAOS_SPACE_BUF_SIZE;
        daos_iov_set(&open_udata->md_rw_cb_ud.sg_iov[2], p, (daos_size_t)H5_DAOS_ACPL_BUF_SIZE);
        open_udata->md_rw_cb_ud.sgl[2].sg_nr     = 1;
        open_udata->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        open_udata->md_rw_cb_ud.sgl[2].sg_iovs   = &open_udata->md_rw_cb_ud.sg_iov[2];
        open_udata->md_rw_cb_ud.free_sg_iov[2]   = FALSE;

        /* Set conditional akey fetch for attribute metadata read operation */
        open_udata->md_rw_cb_ud.flags = DAOS_COND_AKEY_FETCH;

        /* Set nr */
        open_udata->md_rw_cb_ud.nr = 3u;

        /* Set task name */
        open_udata->md_rw_cb_ud.task_name = "attribute metadata read";

        /* Create meta task for attribute metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_ainfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &open_udata->fetch_metatask) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create meta task for attribute metadata read");

        /* Create task for attribute metadata read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_attribute_md_rw_prep_cb, H5_daos_ainfo_read_comp_cb, open_udata,
                                     &fetch_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't create task to read attribute metadata");

        /* Schedule meta task */
        if (0 != (ret = tse_task_schedule(open_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                         "can't schedule meta task for attribute metadata read: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule attribute metadata read task (or save it to be scheduled
         * later) and give it a reference to req and the attribute */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(fetch_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL,
                             "can't schedule task to read attribute metadata: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = fetch_task;
        *dep_task = open_udata->fetch_metatask;
        req->rc++;
        attr->item.rc++;
        open_udata = NULL;
    } /* end if */
    else
        assert(bcast_udata);

    ret_value = attr;

done:
    /* Broadcast attribute info */
    if (bcast_udata) {
        if (H5_daos_mpi_ibcast((H5_daos_mpi_ibcast_ud_t *)bcast_udata, NULL, ainfo_buf_size,
                               NULL == ret_value ? TRUE : FALSE, NULL,
                               item->file->my_rank == 0 ? H5_daos_attribute_open_bcast_comp_cb
                                                        : H5_daos_attribute_open_recv_comp_cb,
                               req, first_task, dep_task) < 0) {
            DV_free(bcast_udata);
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "failed to broadcast attribute info buffer");
        } /* end if */
        attr->item.rc++;

        bcast_udata = NULL;
    } /* end if */

    /* Free akeys_buf if necessary */
    if (akeys_buf && H5_daos_free_async(akeys_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CANTFREE, NULL, "can't free akey buffer");

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close attribute */
        if (attr && H5_daos_attribute_close_real(attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close attribute");

        /* Close internal request for target object open */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't free request");
        int_int_req = NULL;

        /* Free memory */
        open_udata = DV_free(open_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!open_udata);
    assert(!bcast_udata);
    assert(!int_int_req);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_open_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open_by_idx_helper
 *
 * Purpose:     Helper routine for opening an attribute by index. This
 *              routine first locates the parent object that the attribute
 *              is attached to and then retrieves the target attribute's
 *              name according to the given index type, index iteration
 *              order and index value.
 *
 *              As a side effect, the output attribute's name and parent
 *              object fields are setup.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_open_by_idx_helper(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
                                     H5_daos_attr_t *attr_out, H5_daos_req_t *req, tse_task_t **first_task,
                                     tse_task_t **dep_task)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t    *attr_parent_obj      = NULL;
    const char       *target_attr_name     = NULL;
    size_t            target_attr_name_len = 0;
    char             *attr_name_buf        = NULL;
    H5_daos_req_t    *int_int_req          = NULL;
    int               ret;
    herr_t            ret_value = SUCCEED;

    assert(target_obj);
    assert(loc_params);
    assert(attr_out);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(H5VL_OBJECT_BY_IDX == loc_params->type);

    /* Start internal H5 operation for target object open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(target_obj->item.file,
                                                  "target object open within attribute create by name", NULL,
                                                  NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Open object that the attribute is attached to */
    sub_loc_params.type                         = H5VL_OBJECT_BY_NAME;
    sub_loc_params.obj_type                     = target_obj->item.type;
    sub_loc_params.loc_data.loc_by_name.name    = loc_params->loc_data.loc_by_idx.name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;
    if (H5_daos_object_open_helper((H5_daos_item_t *)target_obj, &sub_loc_params, NULL, TRUE, NULL,
                                   &attr_parent_obj, int_int_req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute's parent object");

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

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

    /* Retrieve the attribute's name by index */
    if (H5_daos_attribute_get_name_by_idx_alloc(
            attr_parent_obj, loc_params->loc_data.loc_by_idx.idx_type, loc_params->loc_data.loc_by_idx.order,
            (uint64_t)loc_params->loc_data.loc_by_idx.n, &target_attr_name, &target_attr_name_len,
            &attr_name_buf, NULL, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name");

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

    /* Setup attribute's parent object and name fields */
    if (NULL == (attr_out->name = strdup(target_attr_name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy attribute name");
    attr_out->parent = attr_parent_obj;

done:
    if (attr_name_buf)
        attr_name_buf = DV_free(attr_name_buf);

    /* Cleanup on failure */
    if (ret_value < 0) {
        /* Close internal request for target object create */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

        if (attr_parent_obj && H5_daos_object_close(&attr_parent_obj->item) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object");
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_open_by_idx_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for attribute
 *              opens (rank 0).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_open_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_ibcast_ud_t *udata;
    int                       ret;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute info broadcast task");

    assert(udata->bcast_ud.req);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_ud.req->status      = task->dt_result;
        udata->bcast_ud.req->failed_task = "MPI_Ibcast attribute info";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->bcast_ud.obj);
        assert(udata->attr);
        assert(udata->bcast_ud.obj->item.file);
        assert(udata->bcast_ud.obj->item.file->my_rank == 0);

        /* Reissue bcast if necessary */
        if (udata->bcast_ud.buffer_len != udata->bcast_ud.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_ud.count == H5_DAOS_AINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_ud.buffer_len > H5_DAOS_AINFO_BCAST_BUF_SIZE);

            /* Use full buffer this time */
            udata->bcast_ud.count = udata->bcast_ud.buffer_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL,
                                    H5_daos_attribute_open_bcast_comp_cb, udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task second attribute info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                             "can't schedule task for second attribute info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close attribute  */
        if (H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_ud.req->status      = ret_value;
            udata->bcast_ud.req->failed_task = "MPI_Ibcast attribute info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_ud.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_ud.bcast_metatask, ret_value);

        /* Free buffer */
        if (udata->bcast_ud.buffer != udata->flex_buf)
            DV_free(udata->bcast_ud.buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_open_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open_recv_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for attribute
 *              opens (rank 1+).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_open_recv_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_ibcast_ud_t *udata;
    int                       ret;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute info receive task");

    assert(udata->bcast_ud.req);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_ud.req->status      = task->dt_result;
        udata->bcast_ud.req->failed_task = "MPI_Ibcast attribute info";
    } /* end if */
    else if (task->dt_result == 0) {
        uint64_t type_buf_len  = 0;
        uint64_t space_buf_len = 0;
        uint64_t acpl_buf_len  = 0;
        size_t   ainfo_len;
        uint8_t *p = udata->bcast_ud.buffer;

        assert(udata->attr);
        assert(udata->attr->parent);
        assert(udata->attr->parent->item.file);
        assert(udata->attr->parent->item.file->my_rank > 0);

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_buf_len)
        UINT64DECODE(p, space_buf_len)
        UINT64DECODE(p, acpl_buf_len)

        /* Check for type_buf_len set to 0 - indicates failure */
        if (type_buf_len == 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR,
                         "lead process failed to open attribute");

        /* Calculate data length */
        ainfo_len =
            (size_t)type_buf_len + (size_t)space_buf_len + (size_t)acpl_buf_len + 3 * sizeof(uint64_t);

        /* Reissue bcast if necessary */
        if (ainfo_len > (size_t)udata->bcast_ud.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_ud.buffer_len == H5_DAOS_AINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_ud.count == H5_DAOS_AINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_ud.buffer == udata->flex_buf);

            /* Realloc buffer */
            if (NULL == (udata->bcast_ud.buffer = DV_malloc(ainfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "failed to allocate memory for attribute info buffer");
            udata->bcast_ud.buffer_len = (int)ainfo_len;
            udata->bcast_ud.count      = (int)ainfo_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL,
                                    H5_daos_attribute_open_recv_comp_cb, udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task second attribute info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                             "can't schedule task for second attribute info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        }
        else {
            /* Finish building attribute object */
            if (0 !=
                (ret = H5_daos_attribute_open_end(udata->attr, p, type_buf_len, space_buf_len, acpl_buf_len)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't finish opening attribute");
        } /* end else */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close attribute  */
        if (H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_ud.req->status      = ret_value;
            udata->bcast_ud.req->failed_task = "MPI_Ibcast attribute info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_ud.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_ud.bcast_metatask, ret_value);

        /* Free buffer */
        if (udata->bcast_ud.buffer != udata->flex_buf)
            DV_free(udata->bcast_ud.buffer);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_open_recv_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open_end
 *
 * Purpose:     Decode serialized attribute info from a buffer and fill
 *              caches.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_open_end(H5_daos_attr_t *attr, uint8_t *p, uint64_t type_buf_len, uint64_t space_buf_len,
                           uint64_t acpl_buf_len)
{
    int ret_value = 0;

    assert(attr);
    assert(attr->parent);
    assert(p);
    assert(type_buf_len > 0);

    /* Decode datatype */
    if ((attr->type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize datatype");
    p += type_buf_len;

    /* Decode dataspace and select all */
    if ((attr->space_id = H5Sdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize dataspace");
    if (H5Sselect_all(attr->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, -H5_DAOS_H5_DECODE_ERROR, "can't change selection");
    p += space_buf_len;

    /* Check if the attribute's ACPL is the default ACPL.
     * Otherwise, decode the attribute's ACPL.
     */
    if ((acpl_buf_len == attr->item.file->def_plist_cache.acpl_size) &&
        !memcmp(p, attr->item.file->def_plist_cache.acpl_buf, attr->item.file->def_plist_cache.acpl_size))
        attr->acpl_id = H5P_ATTRIBUTE_CREATE_DEFAULT;
    else if ((attr->acpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize ACPL");

    /* Finish setting up attribute struct */
    if ((attr->file_type_id = H5VLget_file_type(attr->parent->item.file, H5_DAOS_g, attr->type_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_H5_TCONV_ERROR, "failed to get file datatype");
    if (0 == (attr->file_type_size = H5Tget_size(attr->file_type_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get file datatype size");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_open_end() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_ainfo_read_comp_cb
 *
 * Purpose:     Complete callback for asynchronous metadata fetch for
 *              attribute opens.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_ainfo_read_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_open_ud_t *udata;
    uint8_t                *p;
    int                     ret;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute info read task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->fetch_metatask);
    assert(udata->md_rw_cb_ud.req->file || task->dt_result != 0);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;
        size_t      daos_info_len = udata->md_rw_cb_ud.iod[0].iod_size + udata->md_rw_cb_ud.iod[1].iod_size +
                               udata->md_rw_cb_ud.iod[2].iod_size;

        assert(udata->md_rw_cb_ud.obj);

        /* Verify iod size makes sense */
        if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_TYPE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[1].iov_buf_len != H5_DAOS_SPACE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[2].iov_buf_len != H5_DAOS_ACPL_BUF_SIZE)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "buffer length does not match expected value");

        if (udata->bcast_udata) {
            assert(udata->bcast_udata->buffer == ((H5_daos_attr_ibcast_ud_t *)udata->bcast_udata)->flex_buf);

            /* Reallocate attribute info buffer if necessary */
            if (daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_ACPL_BUF_SIZE) {
                if (NULL == (udata->bcast_udata->buffer =
                                 DV_malloc(daos_info_len + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized attribute info");
                udata->bcast_udata->buffer_len = (int)daos_info_len + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE;
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->bcast_udata->buffer + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE;
        } /* end if */
        else {
            assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf == udata->flex_buf);

            /* Reallocate attribute info buffer if necessary */
            if (daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_SPACE_BUF_SIZE + H5_DAOS_ACPL_BUF_SIZE) {
                if (NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(daos_info_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized attribute info");
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

        /* Create task for reissued attribute metadata read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_attribute_md_rw_prep_cb,
                                     H5_daos_ainfo_read_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to read attribute metadata");

        /* Schedule reissued attribute metadata read task */
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't schedule task to read attribute metadata: %s",
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
            uint64_t acpl_buf_len  = (uint64_t)(udata->md_rw_cb_ud.iod[2].iod_size);

            /* Check for missing metadata */
            if (udata->md_rw_cb_ud.iod[0].iod_size == 0 || udata->md_rw_cb_ud.iod[1].iod_size == 0 ||
                udata->md_rw_cb_ud.iod[2].iod_size == 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, -H5_DAOS_DAOS_GET_ERROR, "internal metadata not found");

            if (udata->bcast_udata) {
                /* Encode serialized info lengths */
                p = udata->bcast_udata->buffer;
                UINT64ENCODE(p, type_buf_len)
                UINT64ENCODE(p, space_buf_len)
                UINT64ENCODE(p, acpl_buf_len)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Finish building attribute object */
            if (0 != (ret = H5_daos_attribute_open_end(udata->attr, udata->md_rw_cb_ud.sg_iov[0].iov_buf,
                                                       type_buf_len, space_buf_len, acpl_buf_len)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't finish opening attribute");
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up if this is the last fetch task */
    if (udata) {
        /* Close attribute  */
        if (H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        if (udata->bcast_udata) {
            /* Clear broadcast buffer if there was an error */
            if (udata->md_rw_cb_ud.req->status < -H5_DAOS_INCOMPLETE)
                (void)memset(udata->bcast_udata->buffer, 0, (size_t)udata->bcast_udata->count);
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
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->fetch_metatask) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete fetch metatask */
        tse_task_complete(udata->fetch_metatask, ret_value);

        assert(!udata->md_rw_cb_ud.free_dkey);
        assert(!udata->md_rw_cb_ud.free_akeys);

        /* Free udata */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_ainfo_read_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_io_int_task
 *
 * Purpose:     Asynchronous version of H5Aread()/H5Awrite().
 *`
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_io_int_task(tse_task_t *task)
{
    H5_daos_attr_io_ud_t *udata      = NULL;
    tse_task_t           *first_task = NULL;
    tse_task_t           *dep_task   = NULL;
    int                   ret;
    int                   ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset I/O task");

    assert(udata->end_task);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_ATTR);

    /* Call actual I/O routine */
    switch (udata->io_type) {
        case IO_READ:
            if (H5_daos_attribute_read_int(udata->attr, udata->mem_type_id, udata->collective,
                                           udata->need_tconv, udata->buf.rbuf, udata, udata->md_rw_cb_ud.req,
                                           &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, -H5_DAOS_H5_GET_ERROR,
                             "failed to read data from attribute");
            break;

        case IO_WRITE:
            if (H5_daos_attribute_write_int(udata->attr, udata->mem_type_id, udata->collective,
                                            udata->need_tconv, udata->buf.rbuf, udata, udata->md_rw_cb_ud.req,
                                            &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, -H5_DAOS_H5_COPY_ERROR,
                             "failed to write data to attribute");
            break;
    } /* end switch */

done:
    if (udata) {
        /* Register dependency for end task */
        if (dep_task && 0 != (ret = tse_task_register_deps(udata->end_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                         "can't create dependencies for task to perform type conversion: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule end task */
        if (0 != (ret = tse_task_schedule(udata->end_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't schedule end task for IO operation: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule first task */
        if (first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret, "can't schedule final task for dataset I/O: %s",
                         H5_daos_err_to_string(ret));

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "attribute I/O task";
        } /* end if */
    }     /* end if */
    else {
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);
        assert(!first_task);
    } /* end else */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attr_io_int_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_io_int_end_task
 *
 * Purpose:     Finalizes an asynchronous attribute I/O task.
 *`
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_io_int_end_task(tse_task_t *task)
{
    H5_daos_attr_io_ud_t *udata     = NULL;
    int                   ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for dataset I/O task");

    assert(task == udata->end_task);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ_DONE(udata->md_rw_cb_ud.req);

    /* Free memory type */
    if (H5Tclose(udata->mem_type_id) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close memory datatype");

    /* Close attribute */
    if (H5_daos_attribute_close_real(udata->attr) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute used for I/O");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = ret_value;
        udata->md_rw_cb_ud.req->failed_task = "attribute I/O end task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    DV_free(udata->akey_buf);
    if (udata->reuse != H5_DAOS_TCONV_REUSE_TCONV)
        DV_free(udata->tconv_buf);
    if (udata->reuse != H5_DAOS_TCONV_REUSE_BKG)
        DV_free(udata->bkg_buf);
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attr_io_int_end_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_read_int
 *
 * Purpose:     Internal version of H5_daos_attribute_read_int().
 *
 * Return:      Success:        0
 *              Failure:        -1, attribute not read.
 *
 * Programmer:  Neil Fortner
 *              November, 2020
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_read_int(H5_daos_attr_t *attr, hid_t mem_type_id, hbool_t collective, htri_t need_tconv,
                           void *buf, H5_daos_attr_io_ud_t *_udata, H5_daos_req_t *req,
                           tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_io_ud_t    *udata       = _udata;
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    tse_task_t              *tconv_task  = NULL;
    int                      ndims;
    hsize_t                  dim[H5S_MAX_RANK];
    uint64_t                 attr_nelmts;
    H5_daos_tconv_reuse_t    reuse = H5_DAOS_TCONV_REUSE_NONE;
    uint64_t                 i;
    size_t                   file_type_size = 0;
    size_t                   mem_type_size  = 0;
    void                    *tconv_buf      = NULL;
    void                    *bkg_buf        = NULL;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    assert(attr);
    assert(attr->item.type == H5I_ATTR);
    assert(buf);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(!udata || udata->md_rw_cb_ud.req == req);
    assert(!udata || udata->attr == attr);
    assert(!udata || udata->mem_type_id == mem_type_id);
    assert(!udata || udata->io_type == IO_READ);
    assert(!udata || udata->collective == collective);
    assert(!udata || udata->need_tconv == need_tconv);
    assert(!udata || udata->buf.rbuf == buf);

    /* Check for a NULL dataspace */
    if (H5S_NULL == H5Sget_simple_extent_type(attr->space_id))
        D_GOTO_DONE(SUCCEED);

    /* Get dataspace extent */
    if ((ndims = H5Sget_simple_extent_ndims(attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of dimensions");
    if (ndims != H5Sget_simple_extent_dims(attr->space_id, dim, NULL))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get dimensions");

    /* Calculate number of elements in attribute dataspace */
    attr_nelmts = (uint64_t)1;
    for (i = 0; i < (uint64_t)ndims; i++)
        attr_nelmts *= (uint64_t)dim[i];

    if (0 == attr_nelmts)
        D_GOTO_DONE(SUCCEED);

    /* Check if the type conversion is needed if we don't already know */
    if (need_tconv < 0)
        if ((need_tconv = H5_daos_need_tconv(attr->file_type_id, mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");

    if (need_tconv) {
        hbool_t fill_bkg = FALSE;

        /* Initialize type conversion */
        if (H5_daos_tconv_init(attr->file_type_id, &file_type_size, mem_type_id, &mem_type_size,
                               (size_t)attr_nelmts, FALSE, FALSE, &tconv_buf, &bkg_buf, &reuse,
                               &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't initialize type conversion");

        /* Reuse buffer as appropriate */
        if (reuse == H5_DAOS_TCONV_REUSE_TCONV)
            tconv_buf = buf;
        else if (reuse == H5_DAOS_TCONV_REUSE_BKG)
            bkg_buf = buf;

        /* Fill background buffer if necessary */
        if (fill_bkg && (reuse != H5_DAOS_TCONV_REUSE_BKG))
            (void)memcpy(bkg_buf, buf, (size_t)attr_nelmts * mem_type_size);
    } /* end if */
    else
        file_type_size = attr->file_type_size;

    /* Set up broadcast user data (if appropriate) */
    if (collective && (attr->item.file->num_procs > 1)) {
        int bcast_buf_size = 0;

        if (need_tconv)
            bcast_buf_size = (int)(attr_nelmts * MAX(file_type_size, mem_type_size));
        else
            bcast_buf_size = (int)(attr_nelmts * file_type_size);

        if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->req        = req;
        bcast_udata->obj        = NULL;
        bcast_udata->buffer     = need_tconv ? tconv_buf : buf;
        bcast_udata->buffer_len = bcast_buf_size;
        bcast_udata->count      = bcast_udata->buffer_len;
        bcast_udata->comm       = req->file->comm;
    } /* end if */

    if (!collective || (attr->item.file->my_rank == 0) || need_tconv) {
        /* Allocate argument struct if not already allocated. If the attribute
         * read is being done collectively and type conversion is needed, other
         * ranks allocate and use this struct to participate in type conversion
         * after the type conversion buffer has been broadcasted. */
        if (!udata) {
            if (NULL == (udata = (H5_daos_attr_io_ud_t *)DV_calloc(sizeof(H5_daos_attr_io_ud_t))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                             "can't allocate buffer for I/O function arguments");
            udata->md_rw_cb_ud.req = req;
            req->rc++;
            udata->attr = attr;
            attr->item.rc++;
            udata->mem_type_id = mem_type_id;
            udata->collective  = collective;
            udata->need_tconv  = need_tconv;
            udata->io_type     = IO_READ;
            udata->buf.rbuf    = buf;
        } /* end if */
        else
            /* Update need_tconv */
            udata->need_tconv = need_tconv;

        /* Fill in remaining fields in udata */
        udata->md_rw_cb_ud.obj = attr->parent;
        udata->attr_nelmts     = attr_nelmts;
        udata->mem_type_size   = mem_type_size;
        udata->file_type_size  = file_type_size;
        udata->reuse           = reuse;
        udata->tconv_buf       = tconv_buf;
        tconv_buf              = NULL;
        udata->bkg_buf         = bkg_buf;
        bkg_buf                = NULL;

        /* Read from attribute if this process should */
        if (!collective || (attr->item.file->my_rank == 0)) {
            tse_task_t *fetch_task = NULL;

            /* Set up dkey */
            daos_const_iov_set((d_const_iov_t *)&udata->md_rw_cb_ud.dkey, H5_daos_attr_key_g,
                               H5_daos_attr_key_size_g);
            udata->md_rw_cb_ud.free_dkey = FALSE;

            /* Type conversion */
            if (need_tconv) {
                /* Set up sgl_iov to point to tconv_buf */
                daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->tconv_buf,
                             (daos_size_t)(attr_nelmts * (uint64_t)udata->file_type_size));
            } /* end if */
            else {
                /* Set up sgl_iov to point to buf */
                daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], buf,
                             (daos_size_t)(attr_nelmts * (uint64_t)udata->file_type_size));
            } /* end else */

            /* Set up operation to read data */

            /* Create akey string (prefix "V-") */
            if (H5_daos_attribute_get_akeys(attr->name, NULL, NULL, NULL, NULL, &udata->akey,
                                            &udata->akey_buf) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get akey string for raw data akey");

            /* Set up recx */
            udata->recx.rx_idx = (uint64_t)0;
            udata->recx.rx_nr  = attr_nelmts;

            /* Set up iod */
            daos_iov_set(&udata->md_rw_cb_ud.iod[0].iod_name, udata->akey.iov_buf, udata->akey.iov_len);
            udata->md_rw_cb_ud.iod[0].iod_nr    = 1u;
            udata->md_rw_cb_ud.iod[0].iod_recxs = &udata->recx;
            udata->md_rw_cb_ud.iod[0].iod_size  = (daos_size_t)file_type_size;
            udata->md_rw_cb_ud.iod[0].iod_type  = DAOS_IOD_ARRAY;

            udata->md_rw_cb_ud.free_akeys = FALSE;

            /* Finish setting up sgl */
            assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
            udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
            udata->md_rw_cb_ud.sgl[0].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[0];

            /* Set nr */
            udata->md_rw_cb_ud.nr = 1u;

            /* Set task name */
            udata->md_rw_cb_ud.task_name = "attribute read";

            /* Create task for attribute read */
            if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                         H5_daos_md_rw_prep_cb, H5_daos_attribute_read_comp_cb, udata,
                                         &fetch_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to read attribute");

            /* Schedule attribute read task or save it to be scheduled later.
             * References to attr and req are already held by udata. */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(fetch_task, false)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                                 "can't schedule fetch task for attribute read: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                *first_task = fetch_task;
            *dep_task = fetch_task;
        } /* end if */

        /* If this read is collective and type conversion is needed,
         * create task to perform type conversion and free udata
         * after conversion buffer has been broadcasted. This task
         * will be scheduled after the broadcast task. */
        if (collective && (attr->item.file->num_procs > 1) && need_tconv)
            if (H5_daos_create_task(H5_daos_attr_read_tconv, 0, NULL, NULL, NULL, udata, &tconv_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to perform type conversion");

        udata = NULL;
    } /* end if */

done:
    /* Broadcast attribute data buffer if necessary */
    if (collective && attr && (attr->item.file->num_procs > 1)) {
        if (bcast_udata &&
            H5_daos_mpi_ibcast(bcast_udata, NULL, (size_t)bcast_udata->buffer_len, FALSE, NULL,
                               H5_daos_attribute_read_bcast_comp_cb, req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't broadcast attribute data buffer");

        if (tconv_task) {
            /* Register dependency */
            if (dep_task && 0 != (ret = tse_task_register_deps(tconv_task, 1, dep_task)))
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                             "can't create dependencies for task to perform type conversion: %s",
                             H5_daos_err_to_string(ret));

            /* Schedule type conversion task */
            if (0 != (ret = tse_task_schedule(tconv_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                             "can't schedule task to perform type conversion: %s",
                             H5_daos_err_to_string(ret));

            /* Update first_task and dep_task */
            assert(*first_task);
            *dep_task = tconv_task;
        } /* end if */
    }     /* end if */

    /* Cleanup on failure */
    if (ret_value < 0) {
        if (reuse != H5_DAOS_TCONV_REUSE_TCONV)
            tconv_buf = DV_free(tconv_buf);
        if (reuse != H5_DAOS_TCONV_REUSE_BKG)
            bkg_buf = DV_free(bkg_buf);

        /* Close udata if end_task won't */
        if (udata && !udata->end_task) {
            /* Close request */
            if (H5_daos_req_free_int(req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

            /* Close attribute */
            if (H5_daos_attribute_close_real(attr) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");

            DV_free(udata->akey_buf);
            if (udata->reuse != H5_DAOS_TCONV_REUSE_TCONV)
                DV_free(udata->tconv_buf);
            if (udata->reuse != H5_DAOS_TCONV_REUSE_BKG)
                DV_free(udata->bkg_buf);
            udata = DV_free(udata);
        } /* end if */
    }     /* end if */
    else
        assert(!udata || udata->end_task);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_read_int() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_read
 *
 * Purpose:     Reads raw data from an attribute into a buffer.
 *
 * Return:      Success:        0
 *              Failure:        -1, attribute not read.
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_read(void *_attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_attr_io_ud_t *attr_read_udata = NULL;
    H5_daos_attr_t       *attr            = (H5_daos_attr_t *)_attr;
    H5_daos_req_t        *int_req         = NULL;
    tse_task_t           *io_task         = NULL;
    tse_task_t           *first_task      = NULL;
    tse_task_t           *dep_task        = NULL;
    hbool_t               collective;
    htri_t                need_tconv;
    hid_t                 req_dxpl_id;
    int                   ret;
    herr_t                ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL");
    if (!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "read buffer is NULL");
    if (H5I_ATTR != attr->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not an attribute");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* If the attribute's datatype is complete, check if type conversion is
     * needed */
    if (attr->item.open_req->status == 0 || attr->item.created) {
        /* Check if datatype conversion is needed */
        if ((need_tconv = H5_daos_need_tconv(attr->file_type_id, mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
        req_dxpl_id = need_tconv ? dxpl_id : H5P_DATASET_XFER_DEFAULT;
    } /* end if */
    else {
        need_tconv  = -1;
        req_dxpl_id = dxpl_id;
    } /* end else */

    /* Start H5 operation.  Currently the DXPL is only copies when we may
     * perform type conversion. */
    if (NULL == (int_req = H5_daos_req_create(attr->item.file, "attribute read", attr->item.open_req, NULL,
                                              NULL, req_dxpl_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /*
     * Like HDF5, metadata reads are independent by default. If the application has
     * specifically requested collective metadata reads, they will be enabled here.
     */
    H5_DAOS_GET_METADATA_READ_MODE(attr->item.file, H5P_ATTRIBUTE_ACCESS_DEFAULT,
                                   H5P_ATTRIBUTE_ACCESS_DEFAULT, collective, H5E_ATTR, FAIL);

    /* Check if we can call the internal routine directly - the attribute open
     * must be complete.  We do not need to check for a compatible operation
     * pool type since there are no operations that can change the dataspace or
     * datatype of the attribute. */
    if (attr->item.open_req->status == 0) {
        /* Call internal routine */
        if (H5_daos_attribute_read_int(attr, mem_type_id, collective, need_tconv, buf, NULL, int_req,
                                       &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "failed to read data from attribute");
    } /* end if */
    else {
        /* Allocate argument struct */
        if (NULL == (attr_read_udata = (H5_daos_attr_io_ud_t *)DV_calloc(sizeof(H5_daos_attr_io_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for I/O function arguments");
        attr_read_udata->md_rw_cb_ud.req = int_req;
        int_req->rc++;
        attr_read_udata->attr = attr;
        attr->item.rc++;
        attr_read_udata->mem_type_id = H5I_INVALID_HID;
        attr_read_udata->collective  = collective;
        attr_read_udata->need_tconv  = need_tconv;
        attr_read_udata->io_type     = IO_READ;
        attr_read_udata->buf.rbuf    = buf;

        /* Copy memory datatype */
        if ((attr_read_udata->mem_type_id = H5Tcopy(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, FAIL, "can't copy memory type ID");

        /* Create end task for reading data (will be scheduled by internal task)
         */
        if (H5_daos_create_task(H5_daos_attr_io_int_end_task, 0, NULL, NULL, NULL, attr_read_udata,
                                &attr_read_udata->end_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create end task for attribute I/O operation");

        /* Create task to read attribute */
        if (H5_daos_create_task(H5_daos_attr_io_int_task, 0, NULL, NULL, NULL, attr_read_udata, &io_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create end task for attribute I/O operation");

        /* Save task to be scheduled later */
        assert(!first_task);
        first_task      = io_task;
        dep_task        = attr_read_udata->end_task;
        attr_read_udata = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the attribute open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &attr->item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_ATTR,
                                collective, !req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL, "attribute read failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (attr_read_udata) {
        assert(ret_value < 0);

        /* Close memory datatype */
        if (attr_read_udata->mem_type_id >= 0 && H5Tclose(attr_read_udata->mem_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close datatype");

        /* Close request */
        if (H5_daos_req_free_int(attr_read_udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

        /* Close attribute */
        if (H5_daos_attribute_close_real(attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");

        /* Free memory */
        attr_read_udata = DV_free(attr_read_udata);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_read() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_read_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch for
 *              attribute data I/O. Currently checks for a failed task,
 *              checks if type conversion is needed and then frees private
 *              data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_read_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_io_ud_t *udata;
    int                   ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file || task->dt_result != 0);
    assert(udata->attr);

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
        if (udata->need_tconv && udata->collective && (udata->attr->item.file->num_procs > 1))
            /* Delay freeing of udata until
             * conversion buffer has been broadcasted.
             */
            udata = NULL;
    } /* end if */
    else if (task->dt_result == 0) {
        /* Check for nothing read, in this case we must clear the read buffer */
        if (udata->md_rw_cb_ud.sgl[0].sg_nr_out == 0)
            (void)memset(udata->md_rw_cb_ud.sg_iov[0].iov_buf, 0,
                         (size_t)udata->attr_nelmts * udata->file_type_size);

        /* Perform type conversion if necessary */
        if (udata->need_tconv) {
            if (udata->collective && (udata->attr->item.file->num_procs > 1))
                /* Delay type conversion and freeing of udata until
                 * conversion buffer has been broadcasted.
                 */
                udata = NULL;
            else {
                /* Type conversion */
                if (H5Tconvert(udata->attr->file_type_id, udata->mem_type_id, udata->attr_nelmts,
                               udata->tconv_buf, udata->bkg_buf, udata->md_rw_cb_ud.req->dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR,
                                 "can't perform type conversion");

                /* Copy to user's buffer if necessary */
                if (udata->reuse != H5_DAOS_TCONV_REUSE_TCONV) {
                    assert(udata->tconv_buf);
                    (void)memcpy(udata->buf.rbuf, udata->tconv_buf,
                                 (size_t)udata->attr_nelmts * udata->mem_type_size);
                } /* end if */
            }
        } /* end if */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        /* Close attribute if there's no end task */
        if (!udata->end_task && H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "attribute read completion callback";
        } /* end if */

        /* Free private data if there's no end task */
        if (!udata->end_task) {
            /* Release our reference to req */
            if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

            DV_free(udata->akey_buf);
            if (udata->reuse != H5_DAOS_TCONV_REUSE_TCONV)
                DV_free(udata->tconv_buf);
            if (udata->reuse != H5_DAOS_TCONV_REUSE_BKG)
                DV_free(udata->bkg_buf);
            DV_free(udata);
        } /* end if */
    }     /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_read_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_read_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for
 *              collective attribute reads.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_read_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute data broadcast task");

    assert(udata->req);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast attribute data";
    } /* end if */

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except
     * for H5_daos_req_free_int, which updates req->status if it sees an
     * error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "MPI_Ibcast attribute data completion callback";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_metatask) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete bcast metatask */
    tse_task_complete(udata->bcast_metatask, ret_value);

    /* Free private data */
    DV_free(udata);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_read_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_read_tconv
 *
 * Purpose:     Asynchronous task to perform datatype conversion and free
 *              private data after the type conversion buffer has been
 *              broadcasted following a collective attribute read.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_read_tconv(tse_task_t *task)
{
    H5_daos_attr_io_ud_t *udata;
    int                   ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);
    assert(udata->collective);
    assert(udata->need_tconv);

    /* Type conversion */
    if (H5Tconvert(udata->attr->file_type_id, udata->mem_type_id, udata->attr_nelmts, udata->tconv_buf,
                   udata->bkg_buf, udata->md_rw_cb_ud.req->dxpl_id) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR, "can't perform type conversion");

    /* Copy to user's buffer if necessary */
    if (udata->reuse != H5_DAOS_TCONV_REUSE_TCONV)
        (void)memcpy(udata->buf.rbuf, udata->tconv_buf, (size_t)udata->attr_nelmts * udata->mem_type_size);

done:
    if (udata) {
        /* Close attribute if there's no end task */
        if (!udata->end_task && H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "attribute read completion callback";
        } /* end if */

        /* Free private data if there's no end task */
        if (!udata->end_task) {
            /* Release our reference to req */
            if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

            DV_free(udata->akey_buf);
            if (udata->reuse != H5_DAOS_TCONV_REUSE_TCONV)
                DV_free(udata->tconv_buf);
            if (udata->reuse != H5_DAOS_TCONV_REUSE_BKG)
                DV_free(udata->bkg_buf);
            DV_free(udata);
        } /* end if */
    }     /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attr_read_tconv() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_write_int
 *
 * Purpose:     Writes raw data from a buffer into an attribute.
 *
 * Return:      Success:        0
 *              Failure:        -1, attribute not written.
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_write_int(H5_daos_attr_t *attr, hid_t mem_type_id, hbool_t collective, htri_t need_tconv,
                            const void *buf, H5_daos_attr_io_ud_t *_udata, H5_daos_req_t *req,
                            tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_io_ud_t *udata       = _udata;
    tse_task_t           *update_task = NULL;
    int                   ndims;
    hsize_t               dim[H5S_MAX_RANK];
    uint64_t              attr_nelmts;
    void                 *tconv_buf      = NULL;
    void                 *bkg_buf        = NULL;
    size_t                mem_type_size  = 0;
    size_t                file_type_size = 0;
    hbool_t               fill_bkg       = FALSE;
    int                   ret;
    uint64_t              i;
    herr_t                ret_value = SUCCEED;

    assert(attr);
    assert(attr->item.type == H5I_ATTR);
    assert(buf);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(!udata || udata->md_rw_cb_ud.req == req);
    assert(!udata || udata->attr == attr);
    assert(!udata || udata->mem_type_id == mem_type_id);
    assert(!udata || udata->io_type == IO_WRITE);
    assert(!udata || udata->collective == collective);
    assert(!udata || udata->need_tconv == need_tconv);
    assert(!udata || udata->buf.wbuf == buf);

    /* Check for a NULL dataspace */
    if (H5S_NULL == H5Sget_simple_extent_type(attr->space_id))
        D_GOTO_DONE(SUCCEED);

    /* Get dataspace extent */
    if ((ndims = H5Sget_simple_extent_ndims(attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of dimensions");
    if (ndims != H5Sget_simple_extent_dims(attr->space_id, dim, NULL))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get dimensions");

    /* Calculate number of elements in attribute dataspace */
    attr_nelmts = (uint64_t)1;
    for (i = 0; i < (uint64_t)ndims; i++)
        attr_nelmts *= (uint64_t)dim[i];

    if (0 == attr_nelmts)
        D_GOTO_DONE(SUCCEED);

    /* Check if the type conversion is needed if we don't already know */
    if (need_tconv < 0)
        if ((need_tconv = H5_daos_need_tconv(mem_type_id, attr->file_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");

    if (need_tconv) {
        /* Initialize type conversion */
        if (H5_daos_tconv_init(mem_type_id, &mem_type_size, attr->file_type_id, &file_type_size,
                               (size_t)attr_nelmts, FALSE, TRUE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't initialize type conversion");
    } /* end if */
    else
        file_type_size = attr->file_type_size;

    /* Allocate argument struct if not already allocated */
    if (!udata) {
        if (NULL == (udata = (H5_daos_attr_io_ud_t *)DV_calloc(sizeof(H5_daos_attr_io_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for I/O function arguments");
        udata->md_rw_cb_ud.req = req;
        req->rc++;
        udata->attr = attr;
        attr->item.rc++;
        udata->mem_type_id = mem_type_id;
        udata->collective  = collective;
        udata->need_tconv  = need_tconv;
        udata->io_type     = IO_READ;
        udata->buf.wbuf    = buf;
    } /* end if */
    else
        /* Update need_tconv */
        udata->need_tconv = need_tconv;

    /* Fill in remaining fields in udata */
    udata->md_rw_cb_ud.obj = attr->parent;
    udata->attr_nelmts     = attr_nelmts;
    udata->mem_type_size   = mem_type_size;
    udata->file_type_size  = file_type_size;
    udata->reuse           = H5_DAOS_TCONV_REUSE_NONE;
    udata->tconv_buf       = tconv_buf;
    tconv_buf              = NULL;
    udata->bkg_buf         = bkg_buf;
    bkg_buf                = NULL;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&udata->md_rw_cb_ud.dkey, H5_daos_attr_key_g,
                       H5_daos_attr_key_size_g);
    udata->md_rw_cb_ud.free_dkey = FALSE;

    /* Set up operation to write data */

    /* Create akey string (prefix "V-") */
    if (H5_daos_attribute_get_akeys(attr->name, NULL, NULL, NULL, NULL, &udata->akey, &udata->akey_buf) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get akey string for raw data akey");

    /* Set up recx */
    udata->recx.rx_idx = (uint64_t)0;
    udata->recx.rx_nr  = attr_nelmts;

    /* Set up iod */
    daos_iov_set(&udata->md_rw_cb_ud.iod[0].iod_name, udata->akey.iov_buf, udata->akey.iov_len);
    udata->md_rw_cb_ud.iod[0].iod_nr    = 1u;
    udata->md_rw_cb_ud.iod[0].iod_recxs = &udata->recx;
    udata->md_rw_cb_ud.iod[0].iod_size  = (daos_size_t)file_type_size;
    udata->md_rw_cb_ud.iod[0].iod_type  = DAOS_IOD_ARRAY;

    udata->md_rw_cb_ud.free_akeys = FALSE;

    /* Set up constant sgl info */
    udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
    udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
    udata->md_rw_cb_ud.sgl[0].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[0];

    /* Set nr */
    udata->md_rw_cb_ud.nr = 1u;

    /* Set task name */
    udata->md_rw_cb_ud.task_name = "attribute write";

    /* Check for type conversion */
    if (need_tconv) {
        /* Copy data to type conversion buffer */
        (void)memcpy(udata->tconv_buf, buf, (size_t)attr_nelmts * mem_type_size);

        /* Check if we need to fill background buffer */
        if (fill_bkg) {
            tse_task_t *bkg_fill_task = NULL;

            assert(udata->bkg_buf);

            /* Set up sgl_iov to point to bkg_buf */
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->bkg_buf,
                         (daos_size_t)(attr_nelmts * (uint64_t)file_type_size));

            /* Create task for reading to background buffer */
            if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                         H5_daos_md_rw_prep_cb, H5_daos_attribute_read_bkg_comp_cb, udata,
                                         &bkg_fill_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to read attribute");

            /* Schedule background buffer read task or save it to be scheduled
             * later.  References to attr and req are already held by udata. */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(bkg_fill_task, false)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                                 "can't schedule fetch task for background buffer read: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                *first_task = bkg_fill_task;
            *dep_task = bkg_fill_task;
        } /* end if */
        else {
            /* No need to read to background buffer, do type conversion now */

            /* Set up sgl_iov to point to tconv_buf */
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->tconv_buf,
                         (daos_size_t)(attr_nelmts * (uint64_t)file_type_size));

            /* Perform type conversion */
            if (H5Tconvert(mem_type_id, attr->file_type_id, attr_nelmts, udata->tconv_buf, udata->bkg_buf,
                           req->dxpl_id) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTCONVERT, FAIL, "can't perform type conversion");

            /* Set up sgl_iov to point to tconv_buf */
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->tconv_buf,
                         (daos_size_t)(attr_nelmts * (uint64_t)udata->file_type_size));
        } /* end if */
    }     /* end if */
    else
        /* Set sgl to write from buf.  Note we use rbuf here to avoid a warning
         * about casting away const. */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->buf.rbuf,
                     (daos_size_t)(attr_nelmts * (uint64_t)file_type_size));

    assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf);

    /* Create task for attribute write */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_md_rw_prep_cb, H5_daos_attribute_write_comp_cb, udata,
                                 &update_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to write attribute");

    /* Schedule attribute write task or save it to be scheduled later.
     * References to attr and req are already held by udata. */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule fetch task for attribute write: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = update_task;
    *dep_task = update_task;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        tconv_buf = DV_free(tconv_buf);
        bkg_buf   = DV_free(bkg_buf);

        /* Close udata if end_task won't */
        if (udata && !udata->end_task) {
            /* Close request */
            if (H5_daos_req_free_int(req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

            /* Close attribute */
            if (H5_daos_attribute_close_real(attr) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");

            DV_free(udata->akey_buf);
            DV_free(udata->tconv_buf);
            DV_free(udata->bkg_buf);
            udata = DV_free(udata);
        } /* end if */
    }     /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_write_int() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_write
 *
 * Purpose:     Reads raw data from an attribute into a buffer.
 *
 * Return:      Success:        0
 *              Failure:        -1, attribute not written.
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_write(void *_attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id,
                        void H5VL_DAOS_UNUSED **req)
{
    H5_daos_attr_io_ud_t *attr_write_udata = NULL;
    H5_daos_attr_t       *attr             = (H5_daos_attr_t *)_attr;
    H5_daos_req_t        *int_req          = NULL;
    tse_task_t           *io_task          = NULL;
    tse_task_t           *first_task       = NULL;
    tse_task_t           *dep_task         = NULL;
    hbool_t               collective;
    htri_t                need_tconv;
    hid_t                 req_dxpl_id;
    int                   ret;
    herr_t                ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL");
    if (!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "read buffer is NULL");
    if (H5I_ATTR != attr->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not an attribute");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check for write access */
    if (!(attr->item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file");

    /* If the attribute's datatype is complete, check if type conversion is
     * needed */
    if (attr->item.open_req->status == 0 || attr->item.created) {
        /* Check if datatype conversion is needed */
        if ((need_tconv = H5_daos_need_tconv(mem_type_id, attr->file_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
        req_dxpl_id = need_tconv ? dxpl_id : H5P_DATASET_XFER_DEFAULT;
    } /* end if */
    else {
        need_tconv  = -1;
        req_dxpl_id = dxpl_id;
    } /* end else */

    /* Start H5 operation.  Currently the DXPL is only copies when we may
     * perform type conversion. */
    if (NULL == (int_req = H5_daos_req_create(attr->item.file, "attribute write", attr->item.open_req, NULL,
                                              NULL, req_dxpl_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /*
     * Determine if independent metadata writes have been requested. Otherwise,
     * like HDF5, metadata writes are collective by default.
     */
    H5_DAOS_GET_METADATA_WRITE_MODE(attr->item.file, H5P_ATTRIBUTE_ACCESS_DEFAULT,
                                    H5P_ATTRIBUTE_ACCESS_DEFAULT, collective, H5E_ATTR, FAIL);

    /* Check if we can call the internal routine directly - the attribute open
     * must be complete.  We do not need to check for a compatible operation
     * pool type since there are no operations that can change the dataspace or
     * datatype of the attribute. */
    if (attr->item.open_req->status == 0) {
        /* Call internal routine */
        if (H5_daos_attribute_write_int(attr, mem_type_id, collective, need_tconv, buf, NULL, int_req,
                                        &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "failed to write data to attribute");
    } /* end if */
    else {
        /* Allocate argument struct */
        if (NULL == (attr_write_udata = (H5_daos_attr_io_ud_t *)DV_calloc(sizeof(H5_daos_attr_io_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for I/O function arguments");
        attr_write_udata->md_rw_cb_ud.req = int_req;
        int_req->rc++;
        attr_write_udata->attr = attr;
        attr->item.rc++;
        attr_write_udata->mem_type_id = H5I_INVALID_HID;
        attr_write_udata->collective  = collective;
        attr_write_udata->need_tconv  = need_tconv;
        attr_write_udata->io_type     = IO_WRITE;
        attr_write_udata->buf.wbuf    = buf;

        /* Copy memory datatype */
        if ((attr_write_udata->mem_type_id = H5Tcopy(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, FAIL, "can't copy memory type ID");

        /* Create end task for writing data (will be scheduled by internal task)
         */
        if (H5_daos_create_task(H5_daos_attr_io_int_end_task, 0, NULL, NULL, NULL, attr_write_udata,
                                &attr_write_udata->end_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create end task for attribute I/O operation");

        /* Create task to write attribute */
        if (H5_daos_create_task(H5_daos_attr_io_int_task, 0, NULL, NULL, NULL, attr_write_udata, &io_task) <
            0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create end task for attribute I/O operation");

        /* Save task to be scheduled later */
        assert(!first_task);
        first_task       = io_task;
        dep_task         = attr_write_udata->end_task;
        attr_write_udata = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Perform collective error check if appropriate */
        if (collective && (attr->item.file->num_procs > 1))
            if (H5_daos_collective_error_check(attr->parent, int_req, &first_task, &dep_task) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't perform collective error check");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the attribute open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &attr->item, H5_DAOS_OP_TYPE_WRITE,
                                H5_DAOS_OP_SCOPE_ATTR, collective, !req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL, "attribute write failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (attr_write_udata) {
        assert(ret_value < 0);

        /* Close memory datatype */
        if (attr_write_udata->mem_type_id >= 0 && H5Tclose(attr_write_udata->mem_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close datatype");

        /* Close request */
        if (H5_daos_req_free_int(attr_write_udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

        /* Close attribute */
        if (H5_daos_attribute_close_real(attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");

        /* Free memory */
        attr_write_udata = DV_free(attr_write_udata);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_write() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_read_bkg_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch for
 *              reading the attribute to the background buffer in
 *              preparation for type conversion before writing the
 *              attribute.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_read_bkg_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_io_ud_t *udata;
    int                   ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file || task->dt_result != 0);
    assert(udata->attr);
    assert(udata->need_tconv);

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        /* Perform type conversion */
        if (H5Tconvert(udata->mem_type_id, udata->attr->file_type_id, udata->attr_nelmts, udata->tconv_buf,
                       udata->bkg_buf, udata->md_rw_cb_ud.req->dxpl_id) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR, "can't perform type conversion");

        /* Reset iod_size, if the attribute was not writted to then it could
         * have been overwritten by daos_obj_fetch */
        udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)udata->file_type_size;

        /* Retarget sg_iov to write from tconv_buf */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->tconv_buf,
                     (daos_size_t)(udata->attr_nelmts * (uint64_t)udata->file_type_size));
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = ret_value;
        udata->md_rw_cb_ud.req->failed_task = "attribute background buffer read completion callback";
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_read_bkg_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_write_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update for
 *              attribute write.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_write_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_io_ud_t *udata;
    int                   ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file || task->dt_result != 0);
    assert(udata->attr);

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */

    /* Close attribute if there's no end task */
    if (!udata->end_task && H5_daos_attribute_close_real(udata->attr) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = ret_value;
        udata->md_rw_cb_ud.req->failed_task = "attribute read completion callback";
    } /* end if */

    /* Free private data if there's no end task */
    if (!udata->end_task) {
        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        DV_free(udata->akey_buf);
        DV_free(udata->tconv_buf);
        DV_free(udata->bkg_buf);
        DV_free(udata);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_write_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get
 *
 * Purpose:     Gets certain information about an attribute
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              May, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_get(void *_item, H5VL_attr_get_args_t *get_args, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_item_t         *item       = (H5_daos_item_t *)_item;
    H5_daos_op_pool_scope_t op_scope   = H5_DAOS_OP_SCOPE_OBJ;
    H5_daos_req_t          *int_req    = NULL;
    tse_task_t             *first_task = NULL;
    tse_task_t             *dep_task   = NULL;
    int                     ret;
    herr_t                  ret_value = SUCCEED; /* Return value */

    H5_daos_inc_api_cnt();

    if (!item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!get_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (get_args->op_type) {
        /* H5Aget_space */
        case H5VL_ATTR_GET_SPACE: {
            H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

            op_scope = H5_DAOS_OP_SCOPE_ATTR;

            /* Wait for the attribute to open if necessary */
            if (!attr->item.created && attr->item.open_req->status != 0) {
                if (H5_daos_progress(attr->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (attr->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "attribute open failed");
            } /* end if */

            /* Retrieve the attribute's dataspace */
            if ((get_args->args.get_space.space_id = H5Scopy(attr->space_id)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get dataspace ID of attribute");
            break;
        } /* end block */
        /* H5Aget_type */
        case H5VL_ATTR_GET_TYPE: {
            H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

            op_scope = H5_DAOS_OP_SCOPE_ATTR;

            /* Wait for the attribute to open if necessary */
            if (!attr->item.created && attr->item.open_req->status != 0) {
                if (H5_daos_progress(attr->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (attr->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "attribute open failed");
            } /* end if */

            /* Retrieve the attribute's datatype */
            if ((get_args->args.get_type.type_id = H5Tcopy(attr->type_id)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype ID of attribute");
            break;
        } /* end block */
        /* H5Aget_create_plist */
        case H5VL_ATTR_GET_ACPL: {
            H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

            op_scope = H5_DAOS_OP_SCOPE_ATTR;

            /* Wait for the attribute to open if necessary */
            if (!attr->item.created && attr->item.open_req->status != 0) {
                if (H5_daos_progress(attr->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (attr->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "attribute open failed");
            } /* end if */

            /* Retrieve the attribute's creation property list */
            if ((get_args->args.get_acpl.acpl_id = H5Pcopy(attr->acpl_id)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute creation property list");
            break;
        } /* end block */
        /* H5Aget_name(_by_idx) */
        case H5VL_ATTR_GET_NAME: {
            H5VL_attr_get_name_args_t *get_name_args = &get_args->args.get_name;

            if (get_name_args->loc_params.type == H5VL_OBJECT_BY_SELF)
                op_scope = H5_DAOS_OP_SCOPE_ATTR;

            /* Wait for the item to open if necessary */
            if (!item->created && item->open_req->status != 0) {
                if (H5_daos_progress(item->open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (item->open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "item open failed");
            } /* end if */

            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(item->file, "get attribute name", item->open_req, NULL,
                                                      NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Pass ret_size as size_t * - this should be fine since if the call
             * fails the HDF5 library will assign -1 to the return value anyways
             */
            if (H5_daos_attribute_get_name((H5_daos_obj_t *)_item, &get_name_args->loc_params,
                                           get_name_args->buf, get_name_args->buf_size,
                                           get_name_args->attr_name_len, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name");

            break;
        } /* end block */
        /* H5Aget_info */
        case H5VL_ATTR_GET_INFO: {
            H5VL_attr_get_info_args_t *get_info_args = &get_args->args.get_info;
            const char                *attr_name =
                (H5VL_OBJECT_BY_NAME == get_info_args->loc_params.type) ? get_info_args->attr_name : NULL;

            if (get_info_args->loc_params.type == H5VL_OBJECT_BY_SELF)
                op_scope = H5_DAOS_OP_SCOPE_ATTR;

            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(item->file, "get attribute info", item->open_req, NULL,
                                                      NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Get attribute info and give the comp_cb a reference to req */
            if (H5_daos_attribute_get_info(_item, &get_info_args->loc_params, attr_name, get_info_args->ainfo,
                                           NULL, H5_daos_attribute_get_info_comp_cb, int_req, &first_task,
                                           &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute info");
            int_req->rc++;

            break;
        } /* H5VL_ATTR_GET_INFO */
        case H5VL_ATTR_GET_STORAGE_SIZE:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from attribute");
    } /* end switch */

done:
    if (int_req) {
        assert(item);

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the attribute open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_READ, op_scope, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL,
                             "attribute get operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_specific
 *
 * Purpose:     Specific operations with attributes
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
H5_daos_attribute_specific(void *_item, const H5VL_loc_params_t *loc_params,
                           H5VL_attr_specific_args_t *specific_args, hid_t H5VL_DAOS_UNUSED dxpl_id,
                           void **req)
{
    H5_daos_item_t          *item       = (H5_daos_item_t *)_item;
    H5_daos_obj_t           *target_obj = NULL;
    H5VL_loc_params_t        sub_loc_params;
    const H5VL_loc_params_t *sub_loc_params_p = NULL;
    H5_daos_req_t           *int_req          = NULL;
    H5_daos_req_t           *int_int_req      = NULL;
    tse_task_t              *first_task       = NULL;
    tse_task_t              *dep_task         = NULL;
    hbool_t                  collective_md_read;
    hbool_t                  collective_md_write;
    H5_daos_op_pool_type_t   op_type    = H5_DAOS_OP_TYPE_READ;
    hbool_t                  collective = FALSE;
    herr_t                   iter_ret   = 0;
    hid_t                    lapl_id;
    int                      ret;
    herr_t                   ret_value = SUCCEED; /* Return value */

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
    H5_DAOS_GET_METADATA_IO_MODES(item->file, lapl_id, H5P_LINK_ACCESS_DEFAULT, collective_md_read,
                                  collective_md_write, H5E_ATTR, FAIL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "attribute specific", item->open_req, NULL, NULL,
                                              H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Determine attribute object and set LAPL if available */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_SELF:
            /* Use item as attribute parent object, or the root group if item is a
             * file */
            if (item->type == H5I_FILE)
                target_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
            else
                target_obj = (H5_daos_obj_t *)item;
            target_obj->item.rc++;
            break;

        case H5VL_OBJECT_BY_NAME:
            /* Target sub_loc_params_p to loc_params */
            sub_loc_params_p = loc_params;

            break;

        case H5VL_OBJECT_BY_IDX: {
            /* Set sub_loc_params for open by index */
            sub_loc_params.type                         = H5VL_OBJECT_BY_NAME;
            sub_loc_params.loc_data.loc_by_name.name    = loc_params->loc_data.loc_by_idx.name;
            sub_loc_params.loc_data.loc_by_name.lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;
            sub_loc_params_p                            = &sub_loc_params;

            break;
        }

        case H5VL_OBJECT_BY_TOKEN:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL,
                         "invalid or unsupported attribute operation location parameters type");
    } /* end switch */

    /* Open target object if necessary */
    if (!target_obj) {
        assert(sub_loc_params_p);

        /* Start internal H5 operation for target object open.  This will
         * not be visible to the API, will not be added to an operation
         * pool, and will be integrated into this function's task chain. */
        if (NULL == (int_int_req = H5_daos_req_create(
                         item->file, "target object open within attribute specific operation", NULL, NULL,
                         int_req, H5I_INVALID_HID)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        /* Open target_obj */
        if (H5_daos_object_open_helper(item, sub_loc_params_p, NULL, collective_md_read, NULL, &target_obj,
                                       int_int_req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open object for attribute");

        /* Create task to finalize internal operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_int_req, &int_int_req->finalize_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

        /* Schedule finalize task (or save it to be scheduled later),
         * give it ownership of int_int_req, and update task pointers */
        if (first_task) {
            if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            first_task = int_int_req->finalize_task;
        dep_task    = int_int_req->finalize_task;
        int_int_req = NULL;

        H5_DAOS_WAIT_ON_ASYNC_CHAIN(int_req, first_task, dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);
    } /* end if */

    switch (specific_args->op_type) {
        /* H5Adelete(_by_name/_by_idx) */
        case H5VL_ATTR_DELETE:
        case H5VL_ATTR_DELETE_BY_IDX: {
            int_req->op_name = "attribute delete";

            /* Wait for the object to open if necessary */
            if (loc_params->type == H5VL_OBJECT_BY_SELF && !target_obj->item.created &&
                target_obj->item.open_req->status != 0) {
                if (H5_daos_progress(target_obj->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (target_obj->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "object open failed");
            } /* end if */

            collective = collective_md_write;
            op_type    = H5_DAOS_OP_TYPE_WRITE;
            if (H5_daos_attribute_delete(target_obj, loc_params, specific_args, collective, int_req,
                                         &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");

            break;
        } /* H5VL_ATTR_DELETE */

        /* H5Aexists(_by_name) */
        case H5VL_ATTR_EXISTS: {
            const char                   *attr_name   = specific_args->args.exists.name;
            H5_DAOS_ATTR_EXISTS_OUT_TYPE *attr_exists = specific_args->args.exists.exists;

            int_req->op_name = "attribute existence check";

            collective = collective_md_read;
            op_type    = H5_DAOS_OP_TYPE_READ;
            if (H5_daos_attribute_exists(target_obj, attr_name, attr_exists, collective, int_req, &first_task,
                                         &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attribute exists");

            break;
        } /* H5VL_ATTR_EXISTS */

        case H5VL_ATTR_ITER: {
            H5VL_attr_iterate_args_t *iter_args = &specific_args->args.iterate;
            H5_daos_iter_data_t       iter_data;

            int_req->op_name = "attribute iterate";

            /* Wait for the object to open if necessary */
            if (loc_params->type == H5VL_OBJECT_BY_SELF && !target_obj->item.created &&
                target_obj->item.open_req->status != 0) {
                if (H5_daos_progress(target_obj->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (target_obj->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "object open failed");
            } /* end if */

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, iter_args->idx_type, iter_args->order,
                                   FALSE, iter_args->idx, H5I_INVALID_HID, iter_args->op_data, &ret_value,
                                   int_req);
            iter_data.u.attr_iter_data.u.attr_iter_op = iter_args->op;

            /* Handle iteration return value (TODO: how to handle if called
             * async? */
            if (!req)
                iter_data.op_ret_p = &iter_ret;

            collective = FALSE;
            op_type    = H5_DAOS_OP_TYPE_READ;
            if (H5_daos_attribute_iterate(target_obj, &iter_data, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "can't iterate over attributes");

            break;
        } /* end block */

        /* H5Arename(_by_name) */
        case H5VL_ATTR_RENAME: {
            const char *cur_attr_name = specific_args->args.rename.old_name;
            const char *new_attr_name = specific_args->args.rename.new_name;

            int_req->op_name = "attribute rename";

            /* Wait for the object to open if necessary */
            if (loc_params->type == H5VL_OBJECT_BY_SELF && !target_obj->item.created &&
                target_obj->item.open_req->status != 0) {
                if (H5_daos_progress(target_obj->item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (target_obj->item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "object open failed");
            } /* end if */

            collective = collective_md_write;
            op_type    = H5_DAOS_OP_TYPE_WRITE;
            if (H5_daos_attribute_rename(target_obj, cur_attr_name, new_attr_name, collective, int_req,
                                         &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute");

            break;
        } /* H5VL_ATTR_RENAME */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid specific operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the parent object open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, op_type, H5_DAOS_OP_SCOPE_OBJ, collective, !req) <
            0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async (disabled for iterate currently) */
        if (req && specific_args->op_type != H5VL_ATTR_ITER) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL,
                             "attribute specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

            /* Set return value for attribute iteration, unless this function failed but
             * the iteration did not */
            if (specific_args->op_type == H5VL_ATTR_ITER && !(ret_value < 0 && iter_ret >= 0))
                ret_value = iter_ret;
        } /* end else */
    }     /* end if */

    /* Close internal request for target object create */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

    if (target_obj) {
        if (H5_daos_object_close(&target_obj->item) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close object");
        target_obj = NULL;
    } /* end else */

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_close_real
 *
 * Purpose:     Internal version of H5_daos_attribute_close()
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
H5_daos_attribute_close_real(H5_daos_attr_t *attr)
{
    herr_t ret_value = SUCCEED;

    if (!attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL");
    if (H5I_ATTR != attr->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not an attribute");

    if (--attr->item.rc == 0) {
        /* Free attribute data structures */
        if (attr->item.cur_op_pool)
            H5_daos_op_pool_free(attr->item.cur_op_pool);
        if (attr->item.open_req)
            if (H5_daos_req_free_int(attr->item.open_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");
        if (attr->parent && H5_daos_object_close(&attr->parent->item))
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object");
        attr->name = DV_free(attr->name);
        if (attr->type_id != H5I_INVALID_HID && H5Idec_ref(attr->type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close attribute's datatype");
        if (attr->file_type_id != H5I_INVALID_HID && H5Idec_ref(attr->file_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close attribute's file datatype");
        if (attr->space_id != H5I_INVALID_HID && H5Idec_ref(attr->space_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close attribute's dataspace");
        if (attr->acpl_id != H5I_INVALID_HID && attr->acpl_id != H5P_ATTRIBUTE_CREATE_DEFAULT)
            if (H5Idec_ref(attr->acpl_id) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close acpl");
        attr = H5FL_FREE(H5_daos_attr_t, attr);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_close_real() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_close
 *
 * Purpose:     Closes a DAOS HDF5 attribute.
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
H5_daos_attribute_close(void *_attr, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_attr_t              *attr       = (H5_daos_attr_t *)_attr;
    H5_daos_obj_close_task_ud_t *task_ud    = NULL;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    H5_daos_req_t               *int_req    = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check if the attribute's request queue is empty and has no start task
     * (and hence does not depend on anything), if so we can close it
     * immediately.  Also close if it is marked to close nonblocking. */
    if (((attr->item.open_req->status == 0 || attr->item.open_req->status < -H5_DAOS_CANCELED) &&
         (!attr->item.cur_op_pool ||
          (attr->item.cur_op_pool->type == H5_DAOS_OP_TYPE_EMPTY && !attr->item.cur_op_pool->start_task))) ||
        attr->item.nonblocking_close) {
        /* Close attribute */
        if (H5_daos_attribute_close_real(attr) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");
    } /* end if */
    else {
        tse_task_t *close_task = NULL;

        /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
        if (NULL == (int_req = H5_daos_req_create(attr->item.file, "attribute close", attr->item.open_req,
                                                  NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_obj_close_task_ud_t *)DV_calloc(sizeof(H5_daos_obj_close_task_ud_t))))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't allocate space for close task udata struct");
        task_ud->req  = int_req;
        task_ud->item = &attr->item;

        /* Create task to close attribute */
        if (H5_daos_create_task(H5_daos_object_close_task, 0, NULL, NULL, NULL, task_ud, &close_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to close attribute");

        /* Save task to be scheduled later and give it a reference to req and
         * attr */
        assert(!first_task);
        first_task = close_task;
        dep_task   = close_task;
        /* No need to take a reference to attr here since the purpose is to
         * release the API's reference */
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the attribute open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &attr->item, H5_DAOS_OP_TYPE_CLOSE,
                                H5_DAOS_OP_SCOPE_ATTR, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't add request to request queue");
        attr = NULL;

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL, "attribute close failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_attribute_close() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name
 *
 * Purpose:     Helper routine to retrieve an HDF5 attribute's name.
 *
 * Return:      Success:        The length of the attribute's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
                           char *attr_name_out, size_t attr_name_out_size, size_t *size_ret,
                           H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_obj_t *parent_obj  = NULL;
    H5_daos_req_t *int_int_req = NULL;
    int            ret;
    herr_t         ret_value = SUCCEED;

    assert(target_obj);
    assert(loc_params);
    assert(req);
    assert(first_task);
    assert(dep_task);

    switch (loc_params->type) {
        /* H5Aget_name */
        case H5VL_OBJECT_BY_SELF: {
            H5_daos_attr_t *attr = (H5_daos_attr_t *)target_obj;
            size_t          copy_len;
            size_t          nbytes;

            nbytes = strlen(attr->name);
            assert((ssize_t)nbytes >= 0); /*overflow, pretty unlikely --rpm*/

            /* compute the string length which will fit into the user's buffer */
            copy_len = (attr_name_out_size > 0) ? MIN(attr_name_out_size - 1, nbytes) : 0;

            /* Copy all/some of the name */
            if (attr_name_out && copy_len > 0) {
                memcpy(attr_name_out, attr->name, copy_len);

                /* Terminate the string */
                attr_name_out[copy_len] = '\0';
            } /* end if */

            *size_ret = nbytes;

            break;
        } /* H5VL_OBJECT_BY_SELF */

        /* H5Aget_name_by_idx */
        case H5VL_OBJECT_BY_IDX: {
            H5VL_loc_params_t sub_loc_params;

            /* Start internal H5 operation for target object open.  This will
             * not be visible to the API, will not be added to an operation
             * pool, and will be integrated into this function's task chain. */
            if (NULL == (int_int_req = H5_daos_req_create(
                             target_obj->item.file, "target object open within attribute get name by index",
                             NULL, NULL, req, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, (-1), "can't create DAOS request");

            /* Open object that the attribute is attached to */
            sub_loc_params.type                         = H5VL_OBJECT_BY_NAME;
            sub_loc_params.obj_type                     = target_obj->item.type;
            sub_loc_params.loc_data.loc_by_name.name    = loc_params->loc_data.loc_by_idx.name;
            sub_loc_params.loc_data.loc_by_name.lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;
            if (H5_daos_object_open_helper((H5_daos_item_t *)target_obj, &sub_loc_params, NULL, TRUE, NULL,
                                           &parent_obj, int_int_req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, (-1), "can't open attribute's parent object");

            /* Create task to finalize internal operation */
            if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                    NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, (-1),
                             "can't create task to finalize internal operation");

            /* Schedule finalize task (or save it to be scheduled later),
             * give it ownership of int_int_req, and update task pointers */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, (-1),
                                 "can't schedule task to finalize H5 operation: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                *first_task = int_int_req->finalize_task;
            *dep_task   = int_int_req->finalize_task;
            int_int_req = NULL;

            H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, (-1));

            if (H5_daos_attribute_get_name_by_idx(
                    parent_obj, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                    attr_name_out, attr_name_out_size, size_ret, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "can't get attribute name by index");

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_NAME:
        case H5VL_OBJECT_BY_TOKEN:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, (-1), "invalid loc_params type");
    } /* end switch */

done:
    if (parent_obj && H5_daos_object_close(&parent_obj->item) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, (-1), "can't close object");

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, (-1), "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_info
 *
 * Purpose:     Helper routine to retrieve info about an HDF5 attribute
 *              stored on a DAOS server. Allocates a
 *              H5_daos_attr_get_info_ud_t structure and passes it to
 *              H5_daos_attribute_get_info_inplace to create the relevant
 *              task.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_info(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params, const char *attr_name,
                           H5A_info_t *attr_info, tse_task_cb_t prep_cb, tse_task_cb_t comp_cb,
                           H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_get_info_ud_t *get_info_udata = NULL;
    H5_daos_req_t              *int_int_req    = NULL;
    int                         ret;
    herr_t                      ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(attr_info);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL ==
        (get_info_udata = (H5_daos_attr_get_info_ud_t *)DV_malloc(sizeof(H5_daos_attr_get_info_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for attribute info retrieval task");
    get_info_udata->req               = req;
    get_info_udata->info_out          = attr_info;
    get_info_udata->attr              = NULL;
    get_info_udata->get_info_metatask = NULL;

    /* Determine the target object */
    switch (loc_params->type) {
        /* H5Aget_info */
        case H5VL_OBJECT_BY_SELF: {
            get_info_udata->attr = (H5_daos_attr_t *)item;
            item->rc++;
            break;
        } /* H5VL_OBJECT_BY_SELF */

        /* H5Aget_info_by_name */
        case H5VL_OBJECT_BY_NAME:
        /* H5Aget_info_by_idx */
        case H5VL_OBJECT_BY_IDX: {
            /* Start internal H5 operation for target attribute create.  This will
             * not be visible to the API, will not be added to an operation
             * pool, and will be integrated into this function's task chain. */
            if (NULL == (int_int_req = H5_daos_req_create(
                             item->file, "target attribute open within attribute get info by index", NULL,
                             NULL, req, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Open the target attribute */
            if (NULL == (get_info_udata->attr = (H5_daos_attr_t *)H5_daos_attribute_open_helper(
                             item, loc_params, attr_name, H5P_ATTRIBUTE_ACCESS_DEFAULT, FALSE, int_int_req,
                             first_task, dep_task)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open target attribute");

            /* Create task to finalize internal operation */
            if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                    NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                             "can't create task to finalize internal operation");

            /* Schedule finalize task (or save it to be scheduled later),
             * give it ownership of int_int_req, and update task pointers */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                                 "can't schedule task to finalize H5 operation: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                *first_task = int_int_req->finalize_task;
            *dep_task   = int_int_req->finalize_task;
            int_int_req = NULL;

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_TOKEN:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "invalid loc_params type");
    } /* end switch */

    if (H5_daos_attribute_get_info_inplace(get_info_udata, prep_cb, comp_cb, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't create task to get attribute info");

    /* Relinquish control of udata to task's function body */
    get_info_udata = NULL;

done:
    get_info_udata = DV_free(get_info_udata);

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_info() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_info_inplace
 *
 * Purpose:     Helper routine to retrieve info about an HDF5 attribute
 *              stored on a DAOS server. Takes a pointer to a
 *              H5_daos_attr_get_info_ud_t struct instead of allocating
 *              one.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_info_inplace(H5_daos_attr_get_info_ud_t *get_info_udata, tse_task_cb_t prep_cb,
                                   tse_task_cb_t comp_cb, H5_daos_req_t *req, tse_task_t **first_task,
                                   tse_task_t **dep_task)
{
    tse_task_t *get_info_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(get_info_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Create task for retrieving attribute info */
    if (H5_daos_create_task(H5_daos_attribute_get_info_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            prep_cb, comp_cb, get_info_udata, &get_info_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to get attribute info");

    /* Schedule attribute info retrieval task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(get_info_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to get attribute info: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = get_info_task;
    req->rc++;
    *dep_task = get_info_task;

    /* Create meta task for attribute info retrieval. This empty task will be completed
     * when the task for retrieving the attribute's info is finished. This metatask
     * is necessary because the attribute info retrieval task may generate another async
     * task for retrieving the attribute creation order value from the attribute's
     * parent object.
     */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 1, &get_info_task, NULL, NULL, NULL,
                            &get_info_udata->get_info_metatask) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create meta task for attribute info retrieval");

    /* Schedule meta task */
    assert(*first_task);
    if (0 != (ret = tse_task_schedule(get_info_udata->get_info_metatask, false)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                     "can't schedule meta task for attribute info retrieval: %s", H5_daos_err_to_string(ret));

    *dep_task = get_info_udata->get_info_metatask;

done:
    D_FUNC_LEAVE;
} /* H5_daos_attribute_get_info_inplace() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_info_task
 *
 * Purpose:     Asynchronous task for retrieving the info for an attribute.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_get_info_task(tse_task_t *task)
{
    H5_daos_attr_get_info_ud_t *udata            = NULL;
    hssize_t                    dataspace_nelmts = 0;
    int                         ret_value        = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute info retrieval task");

    assert(udata->req);
    assert(udata->attr);
    assert(udata->info_out);

    /* Check for previous errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_ATTR);

    /* Retrieve attribute's creation order value */
    if (udata->attr->parent->ocpl_cache.track_acorder) {
        uint64_t attr_crt_order = 0;

        assert(udata->get_info_metatask);

        if (H5_daos_attribute_get_crt_order_by_name(udata->attr->parent, udata->attr->name, &attr_crt_order) <
            0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR,
                         "can't get attribute's creation order value");
        udata->info_out->corder       = (H5O_msg_crt_idx_t)attr_crt_order; /* DSINC - no check for overflow */
        udata->info_out->corder_valid = TRUE;
    } /* end if */
    else {
        udata->info_out->corder       = 0;
        udata->info_out->corder_valid = FALSE;
    } /* end else */

    /* Only ASCII character set is supported currently */
    udata->info_out->cset = H5T_CSET_ASCII;

    if ((dataspace_nelmts = H5Sget_simple_extent_npoints(udata->attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR,
                     "can't retrieve number of elements in attribute's dataspace");

    /* DSINC - data_size will likely be incorrect currently for VLEN types */
    udata->info_out->data_size = udata->attr->file_type_size * (size_t)dataspace_nelmts;

done:
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute info retrieval task callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_info_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_info_comp_cb
 *
 * Purpose:     Complete callback for asynchronous task to retrieve info
 *              about an attribute. Currently checks for a failed task,
 *              then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_get_info_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_get_info_ud_t *udata;
    int                         ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute info retrieval task");

    assert(udata->req);
    assert(udata->get_info_metatask);
    assert(udata->attr || task->dt_result != 0);
    assert(udata->info_out || task->dt_result != 0);

    /* Handle errors in attribute info retrieval task.  Only record error in
     * udata->req_status if it does not already contain an error (it could
     * contain an error if another task this task is not dependent on also
     * failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "attribute info retrieval task";
    } /* end if */

done:
    if (udata) {
        if (udata->attr && H5_daos_attribute_close_real(udata->attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute info retrieval task completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_info_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_delete
 *
 * Purpose:     Creates an asynchronous task for deleting an HDF5 attribute
 *              attached to an object.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_delete(H5_daos_obj_t *attr_container_obj, const H5VL_loc_params_t *loc_params,
                         const H5VL_attr_specific_args_t *delete_args, hbool_t collective, H5_daos_req_t *req,
                         tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_delete_ud_t *delete_udata = NULL;
    tse_task_t               *delete_task;
    int                       ret;
    herr_t                    ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(loc_params);
    assert(delete_args);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (!collective || (attr_container_obj->item.file->my_rank == 0)) {
        /* Allocate argument struct for deletion task */
        if (NULL == (delete_udata = (H5_daos_attr_delete_ud_t *)DV_calloc(sizeof(H5_daos_attr_delete_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for attribute deletion task callback arguments");
        delete_udata->req             = req;
        delete_udata->attr_parent_obj = attr_container_obj;
        delete_udata->akeys_buf       = NULL;

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&delete_udata->dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

        if (H5VL_ATTR_DELETE_BY_IDX == delete_args->op_type) {
            if (H5_daos_attribute_get_name_by_idx_alloc(
                    attr_container_obj, delete_args->args.delete_by_idx.idx_type,
                    delete_args->args.delete_by_idx.order, (uint64_t)delete_args->args.delete_by_idx.n,
                    &delete_udata->target_attr_name, &delete_udata->target_attr_name_len,
                    &delete_udata->attr_name_buf, NULL, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name");

            H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);
        } /* end if */
        else
            delete_udata->target_attr_name = delete_args->args.del.name;

        /* If attribute creation order is tracked for the attribute's parent
         * object, create some extra tasks to do creation order-related
         * bookkeeping before the attribute and its akeys have been removed.
         */
        if (attr_container_obj->ocpl_cache.track_acorder) {
            /* Retrieve the current number of attributes attached to the object and decrement it */
            if (H5_daos_object_get_num_attrs(delete_udata->attr_parent_obj, &delete_udata->cur_num_attrs,
                                             TRUE, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                             "can't create task to retrieve number of attributes attached to object");

            /* Update the "number of attributes" key on the object */
            if (H5_daos_object_update_num_attrs_key(delete_udata->attr_parent_obj,
                                                    &delete_udata->cur_num_attrs, NULL, NULL, req, first_task,
                                                    dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                             "can't create task to update number of attributes attached to object");

            /* Remove the attribute from the object's attribute creation order index */
            if (H5_daos_attribute_remove_from_crt_idx(delete_udata->attr_parent_obj, delete_args,
                                                      delete_udata->target_attr_name, req, first_task,
                                                      dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                             "can't create task to remove attribute from object's creation order index");
        } /* end if */

        /* Create task to punch akeys - DSINC - currently no support for deleting vlen data akeys */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_PUNCH_AKEYS, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_attribute_delete_prep_cb, H5_daos_attribute_delete_comp_cb,
                                     delete_udata, &delete_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to delete attribute");

        /* Schedule task to delete attribute attached to object (or save
         * it to be scheduled later) and give it a reference to req.
         */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(delete_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to delete attribute: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = delete_task;
        req->rc++;
        attr_container_obj->item.rc++;
        *dep_task = delete_task;

        delete_udata = NULL;
    } /* end if */

done:
    if (collective && (attr_container_obj->item.file->num_procs > 1))
        if (H5_daos_collective_error_check(attr_container_obj, req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't perform collective error check");

    if (ret_value < 0) {
        if (delete_udata->attr_name_buf)
            DV_free(delete_udata->attr_name_buf);
        delete_udata = DV_free(delete_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_delete_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_punch_akeys to
 *              delete an attribute attached to an object. Currently
 *              checks for errors from previous tasks and then sets
 *              arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_delete_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_delete_ud_t *udata;
    daos_obj_punch_t         *punch_args;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute deletion task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_ATTR);

    assert(udata->attr_parent_obj);

    /* Set up akeys */
    if (H5_daos_attribute_get_akeys(udata->target_attr_name, &udata->akeys[0], &udata->akeys[1],
                                    &udata->akeys[2], &udata->akeys[3], &udata->akeys[4],
                                    &udata->akeys_buf) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_H5_GET_ERROR, "can't get akey strings");

    /* Set deletion task arguments */
    if (NULL == (punch_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for attribute deletion task");
    memset(punch_args, 0, sizeof(*punch_args));
    punch_args->oh      = udata->attr_parent_obj->obj_oh;
    punch_args->th      = DAOS_TX_NONE;
    punch_args->dkey    = &udata->dkey;
    punch_args->akeys   = udata->akeys;
    punch_args->akey_nr = H5_DAOS_ATTR_NUM_AKEYS;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_delete_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_delete_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_punch_akeys to
 *              delete an attribute attached to an object. Currently checks
 *              for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_delete_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_delete_ud_t *udata;
    int                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute deletion task");

    /* Handle errors in deletion task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "attribute deletion task";
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        if (udata->attr_parent_obj && H5_daos_object_close(&udata->attr_parent_obj->item) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute deletion task completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        if (udata->attr_name_buf)
            DV_free(udata->attr_name_buf);
        DV_free(udata->akeys_buf);
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_delete_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_remove_from_crt_idx
 *
 * Purpose:     Removes the target attribute from the target object's
 *              attribute creation order index by locating the relevant
 *              akeys and then removing them.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_remove_from_crt_idx(H5_daos_obj_t *target_obj, const H5VL_attr_specific_args_t *delete_args,
                                      const char *attr_name, H5_daos_req_t *req, tse_task_t **first_task,
                                      tse_task_t **dep_task)
{
    daos_key_t dkey;
    daos_key_t crt_akey;
    uint64_t   delete_idx = 0;
    uint8_t    idx_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1];
    uint8_t   *p;
    hsize_t    obj_nattrs_remaining;
    int        ret;
    herr_t     ret_value = SUCCEED;

    assert(target_obj);
    assert(delete_args);
    assert(attr_name);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Retrieve the current number of attributes attached to the object */
    if (H5_daos_object_get_num_attrs(target_obj, &obj_nattrs_remaining, FALSE, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get the number of attributes attached to object");

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

    /* Determine the index value of the attribute to be removed */
    if (H5VL_ATTR_DELETE_BY_IDX == delete_args->op_type) {
        /* DSINC - no check for safe cast here */
        /*
         * Note that this assumes this routine is always called after an attribute's
         * akeys are punched during deletion, so the number of attributes attached to
         * the object should reflect the number after the attribute has been removed.
         */
        delete_idx = (H5_ITER_DEC == delete_args->args.delete_by_idx.order)
                         ? (uint64_t)obj_nattrs_remaining - (uint64_t)delete_args->args.delete_by_idx.n
                         : (uint64_t)delete_args->args.delete_by_idx.n;
    } /* end if */
    else {
        H5_daos_attr_crt_idx_iter_ud_t iter_cb_ud;
        H5_daos_iter_data_t            iter_data;

        /* Initialize iteration data */
        iter_cb_ud.target_attr_name = attr_name;
        iter_cb_ud.attr_idx_out     = &delete_idx;
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_CRT_ORDER, H5_ITER_INC, FALSE,
                               NULL, H5I_INVALID_HID, &iter_cb_ud, NULL, req);
        iter_data.u.attr_iter_data.u.attr_iter_op = H5_daos_attribute_remove_from_crt_idx_name_cb;

        /*
         * TODO: Currently, deleting an attribute directly (H5Adelete) or by name (H5Adelete_by_name)
         *       means that we need to iterate through the attribute creation order index until we
         *       find the value corresponding to the attribute being deleted. This is especially
         *       important because the deletion of attributes might cause the target attribute's
         *       index value to shift downwards.
         *
         *       Once iteration restart is supported for attribute iteration, performance can
         *       be improved here by first looking up the original, permanent creation order
         *       value of the attribute using the 'attribute name -> creation order' mapping
         *       and then using that value as the starting point for iteration. In this case,
         *       the iteration order MUST be switched to H5_ITER_DEC or the key will not be
         *       found by the iteration.
         */
        if (H5_daos_attribute_iterate(target_obj, &iter_data, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed");

        H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);
    } /* end else */

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Remove the akey which maps creation order -> attribute name */
    idx_buf[0] = 0;
    p          = &idx_buf[1];
    UINT64ENCODE(p, delete_idx);
    daos_iov_set(&crt_akey, (void *)idx_buf, H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1);

    /* Remove the akey */
    if (0 != (ret = daos_obj_punch_akeys(target_obj->obj_oh, DAOS_TX_NONE, DAOS_COND_PUNCH, &dkey, 1,
                                         &crt_akey, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "failed to punch attribute akey: %s",
                     H5_daos_err_to_string(ret));

    /*
     * If there are still attributes remaining on the object and we didn't delete
     * the attribute currently at the end of the creation order index, shift the
     * indices of all akeys past the removed attribute's akey down by one. This
     * maintains the ability to directly index into the attribute creation order
     * index.
     */
    if ((obj_nattrs_remaining > 0) && (delete_idx < (uint64_t)obj_nattrs_remaining)) {
        if (H5_daos_attribute_shift_crt_idx_keys_down(target_obj, delete_idx + 1,
                                                      (uint64_t)obj_nattrs_remaining) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTMODIFY, FAIL, "failed to update attribute creation order index");
    } /* end if */
    else if (obj_nattrs_remaining == 0) {
        uint8_t        max_corder_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
        daos_iod_t     iod;
        daos_sg_list_t sgl;
        daos_iov_t     sg_iov;

        /* If the last attribute was removed from the object,
         * reset the max. attribute creation order value.
         */

        memset(max_corder_buf, 0, sizeof(max_corder_buf));
        memset(&iod, 0, sizeof(iod));
        daos_const_iov_set((d_const_iov_t *)&iod.iod_name, H5_daos_max_attr_corder_key_g,
                           H5_daos_max_attr_corder_key_size_g);
        iod.iod_nr   = 1u;
        iod.iod_size = (uint64_t)8;
        iod.iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&sg_iov, max_corder_buf, (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
        sgl.sg_nr     = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs   = &sg_iov;

        /* Reset the max. attribute creation order key */
        if (0 != (ret = daos_obj_update(target_obj->obj_oh, DAOS_TX_NONE, DAOS_COND_AKEY_UPDATE, &dkey, 1,
                                        &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL,
                         "failed to reset max. attribute creation order akey: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_remove_from_crt_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_remove_from_crt_idx_name_cb
 *
 * Purpose:     Attribute iteration callback for
 *              H5_daos_attribute_remove_from_crt_idx which iterates
 *              through attributes by creation order until the current
 *              attribute name matches the target attribute name, at which
 *              point the attribute creation order index value for the
 *              target attribute has been found.
 *
 * Return:      Non-negative (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_remove_from_crt_idx_name_cb(hid_t H5VL_DAOS_UNUSED loc_id, const char *attr_name,
                                              const H5A_info_t H5VL_DAOS_UNUSED *attr_info, void *op_data)
{
    H5_daos_attr_crt_idx_iter_ud_t *cb_ud = (H5_daos_attr_crt_idx_iter_ud_t *)op_data;

    if (!strcmp(attr_name, cb_ud->target_attr_name))
        return 1;

    (*cb_ud->attr_idx_out)++;
    return 0;
} /* end H5_daos_attribute_remove_from_crt_idx_name_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_shift_crt_idx_keys_down
 *
 * Purpose:     After an attribute has been deleted from an object, this
 *              routine is used to update the object's attribute creation
 *              order index. All of the index's akeys within the range
 *              specified by the begin and end index parameters are read
 *              and then re-written to the index under new akeys whose
 *              integer 'name' values are one less than the akeys' original
 *              values.
 *
 *              By shifting these indices downward, the creation order
 *              index will not contain any holes and will maintain its
 *              ability to be directly indexed into.
 *
 *              TODO: Currently, this routine attempts to avoid calls to
 *                    the server by allocating buffers for all of the keys
 *                    and then reading/writing them at once. However, this
 *                    leads to several tiny allocations and the potential
 *                    for a very large amount of memory usage, which could
 *                    be improved upon.
 *
 *                    One improvement would be to allocate a single large
 *                    buffer for the key data and then set indices into the
 *                    buffer appropriately in each of the SGLs. This would
 *                    help in avoiding the tiny allocations for the data
 *                    buffers for each key.
 *
 *                    Another improvement would be to pick a sensible upper
 *                    bound on the amount of keys handled at a single time
 *                    and then perform several rounds of reading/writing
 *                    until all of the keys have been processed. This
 *                    should help to minimize the total amount of memory
 *                    that is used at any point in time.
 *
 * Return:      Non-negative on success/negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_shift_crt_idx_keys_down(H5_daos_obj_t *target_obj, uint64_t idx_begin, uint64_t idx_end)
{
    daos_sg_list_t *sgls    = NULL;
    daos_iod_t     *iods    = NULL;
    daos_iov_t     *sg_iovs = NULL;
    daos_key_t      dkey;
    daos_key_t      tail_akey;
    uint64_t        tmp_uint;
    uint8_t        *crt_order_attr_name_buf = NULL;
    uint8_t        *p;
    size_t          nattrs_shift;
    size_t          i;
    char           *tmp_buf = NULL;
    int             ret;
    herr_t          ret_value = SUCCEED;

    assert(target_obj);
    assert(idx_end >= idx_begin);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    nattrs_shift = idx_end - idx_begin + 1;

    /*
     * Allocate space for the 1 akey per attribute: the akey that maps the
     * attribute's creation order value to the attribute's name.
     */
    if (NULL == (iods = DV_calloc(nattrs_shift * sizeof(*iods))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate IOD buffer");
    if (NULL == (sgls = DV_malloc(nattrs_shift * sizeof(*sgls))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate SGL buffer");
    if (NULL == (sg_iovs = DV_calloc(nattrs_shift * sizeof(*sg_iovs))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate IOV buffer");
    if (NULL == (crt_order_attr_name_buf = DV_malloc(nattrs_shift * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate akey data buffer");

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iods */
    for (i = 0; i < nattrs_shift; i++) {
        tmp_uint = idx_begin + i;

        /* Setup the integer 'name' value for the current 'creation order -> attribute name' akey */
        p    = &crt_order_attr_name_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1)];
        *p++ = 0;
        UINT64ENCODE(p, tmp_uint);

        /* Set up iods for the current 'creation order -> attribute name' akey */
        daos_iov_set(&iods[i].iod_name, &crt_order_attr_name_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1)],
                     H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1);
        iods[i].iod_nr   = 1u;
        iods[i].iod_size = DAOS_REC_ANY;
        iods[i].iod_type = DAOS_IOD_SINGLE;
    } /* end for */

    /* Fetch the data size for each akey */
    if (0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, DAOS_COND_AKEY_FETCH, &dkey,
                                   (unsigned)nattrs_shift, iods, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read akey data sizes: %s",
                     H5_daos_err_to_string(ret));

    /* Allocate buffers and setup sgls for each akey */
    for (i = 0; i < nattrs_shift; i++) {
        /* Allocate buffer for the current 'creation order -> attribute name' akey */
        if (iods[i].iod_size == 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADSIZE, FAIL, "invalid iod size - missing metadata");
        if (NULL == (tmp_buf = DV_malloc(iods[i].iod_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey data");

        /* Set up sgls for the current 'creation order -> attribute name' akey */
        daos_iov_set(&sg_iovs[i], tmp_buf, iods[i].iod_size);
        sgls[i].sg_nr     = 1;
        sgls[i].sg_nr_out = 0;
        sgls[i].sg_iovs   = &sg_iovs[i];
    } /* end for */

    /* Read the akey's data */
    if (0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, DAOS_COND_AKEY_FETCH, &dkey,
                                   (unsigned)nattrs_shift, iods, sgls, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read akey data: %s", H5_daos_err_to_string(ret));

    /*
     * Adjust the akeys down by setting their integer 'name' values to
     * one less than their original values
     */
    for (i = 0; i < nattrs_shift; i++) {
        /* Setup the integer 'name' value for the current 'creation order -> attribute name' akey */
        p = &crt_order_attr_name_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1) + 1];
        UINT64DECODE(p, tmp_uint);

        tmp_uint--;
        p = &crt_order_attr_name_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1) + 1];
        UINT64ENCODE(p, tmp_uint);
    } /* end for */

    /* Write the akeys back */
    if (0 != (ret = daos_obj_update(target_obj->obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey,
                                    (unsigned)nattrs_shift, iods, sgls, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write akey data: %s", H5_daos_err_to_string(ret));

    /* Delete the (now invalid) akey at the end of the creation index */
    tmp_uint = idx_end;
    p        = &crt_order_attr_name_buf[1];
    UINT64ENCODE(p, tmp_uint);
    daos_iov_set(&tail_akey, (void *)&crt_order_attr_name_buf[0], H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1);

    if (0 != (ret = daos_obj_punch_akeys(target_obj->obj_oh, DAOS_TX_NONE, DAOS_COND_PUNCH, &dkey, 1,
                                         &tail_akey, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL,
                     "can't trim tail akey from attribute creation order index");

done:
    for (i = 0; i < nattrs_shift; i++) {
        if (sg_iovs[i].iov_buf)
            sg_iovs[i].iov_buf = DV_free(sg_iovs[i].iov_buf);
    } /* end for */
    if (crt_order_attr_name_buf)
        crt_order_attr_name_buf = DV_free(crt_order_attr_name_buf);
    if (sg_iovs)
        sg_iovs = DV_free(sg_iovs);
    if (sgls)
        sgls = DV_free(sgls);
    if (iods)
        iods = DV_free(iods);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_shift_crt_idx_keys_down() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_exists
 *
 * Purpose:     Helper routine to check if an HDF5 attribute exists by
 *              attempting to read from its metadata keys.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_exists(H5_daos_obj_t *attr_container_obj, const char *attr_name,
                         H5_DAOS_ATTR_EXISTS_OUT_TYPE *exists, hbool_t collective, H5_daos_req_t *req,
                         tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_exists_ud_t *attr_exists_ud = NULL;
    tse_task_t               *fetch_task     = NULL;
    hbool_t                   must_bcast     = FALSE;
    int                       ret;
    herr_t                    ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(attr_name);

    /* Allocate user data struct */
    if (NULL == (attr_exists_ud = (H5_daos_attr_exists_ud_t *)DV_calloc(sizeof(H5_daos_attr_exists_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate attribute exists user data");
    attr_exists_ud->bcast_ud.req = req;
    attr_exists_ud->bcast_ud.obj = attr_container_obj;
    attr_exists_ud->exists       = exists;

    /* Set up broadcast user data (if appropriate) */
    if (collective && (attr_container_obj->item.file->num_procs > 1)) {
        attr_exists_ud->bcast_ud.buffer     = (void *)&attr_exists_ud->bcast_exists;
        attr_exists_ud->bcast_ud.buffer_len = (int)sizeof(htri_t);
        attr_exists_ud->bcast_ud.count      = (int)sizeof(htri_t);
        attr_exists_ud->bcast_ud.comm       = req->file->comm;
        must_bcast                          = TRUE;
    } /* end if */

    if (!collective || (attr_container_obj->item.file->my_rank == 0)) {
        hbool_t may_track_acorder =
            (attr_container_obj->item.open_req->status < 0 && !attr_container_obj->item.created) ||
            attr_container_obj->ocpl_cache.track_acorder;

        /* Set number of records.  Do not include creation order key, the prep
         * callback will add it if appropriate. */
        attr_exists_ud->nr = H5_DAOS_ATTR_NUM_AKEYS - 2;

        if (H5_daos_attribute_get_akeys(
                attr_name, &attr_exists_ud->akeys[0], &attr_exists_ud->akeys[1], &attr_exists_ud->akeys[2],
                may_track_acorder ? &attr_exists_ud->akeys[3] : NULL, NULL, &attr_exists_ud->akeys_buf) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get akey strings");

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&attr_exists_ud->dkey, H5_daos_attr_key_g,
                           H5_daos_attr_key_size_g);

        /* Set up iods */
        daos_iov_set(&attr_exists_ud->iod[0].iod_name, attr_exists_ud->akeys[0].iov_buf,
                     (daos_size_t)attr_exists_ud->akeys[0].iov_len);
        attr_exists_ud->iod[0].iod_nr   = 1u;
        attr_exists_ud->iod[0].iod_type = DAOS_IOD_SINGLE;
        attr_exists_ud->iod[0].iod_size = DAOS_REC_ANY;

        daos_iov_set(&attr_exists_ud->iod[1].iod_name, attr_exists_ud->akeys[1].iov_buf,
                     (daos_size_t)attr_exists_ud->akeys[1].iov_len);
        attr_exists_ud->iod[1].iod_nr   = 1u;
        attr_exists_ud->iod[1].iod_type = DAOS_IOD_SINGLE;
        attr_exists_ud->iod[1].iod_size = DAOS_REC_ANY;

        daos_iov_set(&attr_exists_ud->iod[2].iod_name, attr_exists_ud->akeys[2].iov_buf,
                     (daos_size_t)attr_exists_ud->akeys[2].iov_len);
        attr_exists_ud->iod[2].iod_nr   = 1u;
        attr_exists_ud->iod[2].iod_type = DAOS_IOD_SINGLE;
        attr_exists_ud->iod[2].iod_size = DAOS_REC_ANY;

        /* Check for creation order tracking.  If we're not sure if creation
         * order is tracked because the parent object open isn't complete, call
         * the function anyways, the prep callback will check for creation order
         * before actually fetching any info. */
        if (may_track_acorder) {
            daos_iov_set(&attr_exists_ud->iod[3].iod_name, attr_exists_ud->akeys[3].iov_buf,
                         (daos_size_t)attr_exists_ud->akeys[3].iov_len);
            attr_exists_ud->iod[3].iod_nr   = 1u;
            attr_exists_ud->iod[3].iod_type = DAOS_IOD_SINGLE;
            attr_exists_ud->iod[3].iod_size = DAOS_REC_ANY;
        } /* end if */

        /* Create task for fetch */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_attr_exists_prep_cb, H5_daos_attr_exists_comp_cb, attr_exists_ud,
                                     &fetch_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create attribute exists task");

        /* Schedule fetch task (or save it to be scheduled later) and give it a
         * reference to req and udata (transfer ownership of attr_container_obj) */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(fetch_task, false)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule attribute exists task: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = fetch_task;
        *dep_task = fetch_task;
        req->rc++;
        attr_container_obj->item.rc++;
    } /* end if */

    /* Signify that all ranks will have called the bcast after this point */
    must_bcast = FALSE;

    /* Broadcast attribute existence status if there are other processes that need it */
    if (collective && (attr_container_obj->item.file->num_procs > 1))
        if (H5_daos_mpi_ibcast(&attr_exists_ud->bcast_ud, attr_container_obj, sizeof(htri_t), FALSE, NULL,
                               H5_daos_attr_exists_bcast_comp_cb, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't broadcast attribute existence status");
    attr_exists_ud = NULL;

done:
    /* Cleanup on error */
    if (attr_exists_ud) {
        assert(ret_value < 0);

        /* Free akeys_buf if necessary */
        if (attr_exists_ud->akeys_buf)
            attr_exists_ud->akeys_buf = DV_free(attr_exists_ud->akeys_buf);

        attr_exists_ud = DV_free(attr_exists_ud);

        if (fetch_task)
            tse_task_complete(fetch_task, -H5_DAOS_SETUP_ERROR);

        /* Participate in broadcast on failure */
        if (must_bcast) {
            assert(attr_exists_ud->bcast_ud.buffer);
            *((htri_t *)attr_exists_ud->bcast_ud.buffer) = FAIL;
            if (H5_daos_mpi_ibcast(&attr_exists_ud->bcast_ud, attr_container_obj, sizeof(htri_t), FALSE, NULL,
                                   H5_daos_attr_exists_bcast_comp_cb, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL,
                             "can't broadcast attribute existence check failure");
        } /* end if */
    }     /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_exists() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_exists_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous check for attribute
 *              existence.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_exists_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_exists_ud_t *udata;
    daos_obj_rw_t            *rw_args;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->bcast_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->bcast_ud.req, H5E_ATTR);

    assert(udata->bcast_ud.obj);
    assert(udata->bcast_ud.req->file);

    /* Set update task arguments */
    if (NULL == (rw_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for attribute exists task");
    memset(rw_args, 0, sizeof(*rw_args));
    rw_args->oh   = udata->bcast_ud.obj->obj_oh;
    rw_args->th   = udata->bcast_ud.req->th;
    rw_args->dkey = &udata->dkey;
    rw_args->nr   = (uint32_t)udata->nr + (udata->bcast_ud.obj->ocpl_cache.track_acorder ? 1 : 0);
    rw_args->iods = udata->iod;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attr_exists_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_exists_comp_cb
 *
 * Purpose:     Complete callback for asynchronous check for attribute
 *              existence.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_exists_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_exists_ud_t *udata;
    int                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_ud.req->status      = task->dt_result;
        udata->bcast_ud.req->failed_task = "attribute exists fetch";
    } /* end if */
    else if (task->dt_result == 0) {
        hbool_t attr_exists  = FALSE;
        hbool_t attr_missing = FALSE;

        /* Attribute exists if all of its metadata keys are present. */
        attr_exists =
            (udata->iod[0].iod_size != 0) && (udata->iod[1].iod_size != 0) && (udata->iod[2].iod_size != 0);

        /*
         * Conversely, the attribute doesn't exist if all of its
         * metadata keys are missing.
         */
        attr_missing =
            (udata->iod[0].iod_size == 0) && (udata->iod[1].iod_size == 0) && (udata->iod[2].iod_size == 0);

        /*
         * Check for the presence or absence of the attribute creation
         * order key when the attribute's parent object has attribute
         * creation order tracking enabled.
         */
        if (udata->bcast_ud.obj->ocpl_cache.track_acorder) {
            attr_exists  = attr_exists && (udata->iod[3].iod_size != 0);
            attr_missing = attr_missing && (udata->iod[3].iod_size == 0);
        } /* end if */

        assert(udata->exists);
        if (attr_exists)
            *udata->exists = TRUE;
        else if (attr_missing)
            *udata->exists = FALSE;
        else
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, -H5_DAOS_DAOS_GET_ERROR,
                         "attribute exists in inconsistent state (metadata missing)");
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Release our reference to object */
        if (udata->bcast_ud.obj && H5_daos_object_close(&udata->bcast_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_ud.req->status      = ret_value;
            udata->bcast_ud.req->failed_task = "attribute exists completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free akeys_buf */
        DV_free(udata->akeys_buf);

        /* Check for bcast following this */
        if (udata->bcast_ud.bcast_metatask) {
            assert(udata->bcast_ud.buffer == &udata->bcast_exists);

            /* Propagate errors to following tasks */
            if (ret_value < 0) {
                udata->bcast_exists = FAIL;
            } /* end if */
            else if (*udata->exists)
                udata->bcast_exists = TRUE;
            else
                udata->bcast_exists = FALSE;
        } /* end if */
        else
            /* Free udata */
            udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attr_exists_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_exists_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_Ibcast for attribute
 *              existence check.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_exists_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_exists_ud_t *udata;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute existence broadcast task");

    assert(udata->bcast_ud.req);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_ud.req->status      = task->dt_result;
        udata->bcast_ud.req->failed_task = "MPI_Ibcast attribute existence";
    } /* end if */
    else if (task->dt_result == 0) {
        htri_t attr_exists;

        assert(udata->bcast_ud.buffer);
        assert(udata->bcast_ud.obj);
        assert(udata->bcast_ud.obj->item.file);

        attr_exists = *((htri_t *)udata->bcast_ud.buffer);

        /* If this is not the lead process, report the result to the API */
        if (udata->bcast_ud.req->file->my_rank != 0) {
            if (attr_exists < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, -H5_DAOS_REMOTE_ERROR,
                             "lead process failed to determine if attribute exists");
            else if (attr_exists)
                *udata->exists = TRUE;
            else
                *udata->exists = FALSE;
        } /* end if */
    }     /* end if */

done:
    /* Free private data */
    if (udata) {
        /* Close object */
        if (udata->bcast_ud.obj && H5_daos_object_close(&udata->bcast_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_ud.req->status      = ret_value;
            udata->bcast_ud.req->failed_task = "MPI_Ibcast attribute existence completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_ud.req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_ud.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete bcast metatask */
        tse_task_complete(udata->bcast_ud.bcast_metatask, ret_value);

        /* Free private data */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_attr_exists_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate
 *
 * Purpose:     Creates asynchronous tasks for iterating over the
 *              attributes attached to the target object, using the
 *              supplied iter_data struct for the iteration parameters.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner/Jordan Henderson
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_iterate(H5_daos_obj_t *attr_container_obj, H5_daos_iter_data_t *iter_data,
                          H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_iterate_ud_t *iterate_udata = NULL;
    int                        ret;
    herr_t                     ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(iter_data);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(H5_DAOS_ITER_TYPE_ATTR == iter_data->iter_type);

    /* Iteration restart not supported */
    if (iter_data->idx_p && (*iter_data->idx_p != 0))
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)");

    /* Allocate argument struct for iterate task */
    if (NULL == (iterate_udata = (H5_daos_attr_iterate_ud_t *)DV_calloc(sizeof(H5_daos_attr_iterate_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for iterate task arguments");
    iterate_udata->req                = req;
    iterate_udata->iterate_metatask   = NULL;
    iterate_udata->attr_container_obj = attr_container_obj;
    iterate_udata->iter_data          = *iter_data;

    switch (iter_data->index_type) {
        case H5_INDEX_NAME:
            if (H5_daos_attribute_iterate_by_name_order(iterate_udata, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration by name order failed");
            break;
        case H5_INDEX_CRT_ORDER:
            if (H5_daos_attribute_iterate_by_crt_order(iterate_udata, req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration by creation order failed");
            break;

        case H5_INDEX_UNKNOWN:
        case H5_INDEX_N:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "invalid or unsupported index type");
    } /* end switch */

    /* Create meta task for attribute iteration. This task will be completed
     * when the actual asynchronous attribute iteration is finished. This metatask
     * is necessary because the initial iteration task will generate other async
     * tasks for retrieving an attribute's info and making the user-supplied operator
     * function callback. */
    if (H5_daos_create_task(H5_daos_attribute_iterate_finish, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, iterate_udata, &iterate_udata->iterate_metatask) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create meta task for attribute iteration");

    /* Schedule meta task */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(iterate_udata->iterate_metatask, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule meta task for attribute iteration: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = iterate_udata->iterate_metatask;
    iterate_udata->attr_container_obj->item.rc++;
    req->rc++;
    *dep_task = iterate_udata->iterate_metatask;

    /* Relinquish control of the attribute iteration udata to the
     * task's completion callback */
    iterate_udata = NULL;

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        iterate_udata = DV_free(iterate_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_by_name_order
 *
 * Purpose:     Iterates over the attributes attached to the target object
 *              according to their alphabetical order.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_iterate_by_name_order(H5_daos_attr_iterate_ud_t      *iterate_udata,
                                        H5_daos_req_t H5VL_DAOS_UNUSED *req, tse_task_t **first_task,
                                        tse_task_t **dep_task)
{
    tse_task_t *list_akey_task;
    char       *akey_buf = NULL;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(iterate_udata);
    assert(first_task);
    assert(dep_task);
    assert(H5_INDEX_NAME == iterate_udata->iter_data.index_type);
    assert(H5_ITER_NATIVE == iterate_udata->iter_data.iter_order ||
           H5_ITER_INC == iterate_udata->iter_data.iter_order ||
           H5_ITER_DEC == iterate_udata->iter_data.iter_order);

    /* Native iteration order is currently associated with increasing order; decreasing order iteration is not
     * currently supported */
    if (iterate_udata->iter_data.iter_order == H5_ITER_DEC)
        D_GOTO_ERROR(
            H5E_ATTR, H5E_UNSUPPORTED, FAIL,
            "decreasing iteration order not supported (order must be H5_ITER_NATIVE or H5_ITER_INC)");

    /* Initialize anchor */
    memset(&iterate_udata->u.name_order_data.anchor, 0, sizeof(daos_anchor_t));

    iterate_udata->u.name_order_data.md_rw_cb_ud.req = iterate_udata->req;
    iterate_udata->u.name_order_data.md_rw_cb_ud.obj = iterate_udata->attr_container_obj;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&iterate_udata->u.name_order_data.md_rw_cb_ud.dkey,
                       H5_daos_attr_key_g, H5_daos_attr_key_size_g);
    iterate_udata->u.name_order_data.md_rw_cb_ud.free_dkey = FALSE;

    memset(iterate_udata->u.name_order_data.md_rw_cb_ud.iod, 0,
           sizeof(iterate_udata->u.name_order_data.md_rw_cb_ud.iod));
    iterate_udata->u.name_order_data.md_rw_cb_ud.free_akeys = FALSE;

    /* Allocate akey_buf */
    if (NULL == (akey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akeys");

    /* Set up sgl.  Report size as 1 less than buffer size so we
     * always have room for a null terminator. */
    daos_iov_set(&iterate_udata->u.name_order_data.md_rw_cb_ud.sg_iov[0], akey_buf,
                 (daos_size_t)(H5_DAOS_ITER_SIZE_INIT - 1));
    iterate_udata->u.name_order_data.md_rw_cb_ud.sgl[0].sg_nr     = 1;
    iterate_udata->u.name_order_data.md_rw_cb_ud.sgl[0].sg_nr_out = 0;
    iterate_udata->u.name_order_data.md_rw_cb_ud.sgl[0].sg_iovs =
        &iterate_udata->u.name_order_data.md_rw_cb_ud.sg_iov[0];
    iterate_udata->u.name_order_data.md_rw_cb_ud.free_sg_iov[0] = TRUE;

    /* Set nr */
    iterate_udata->u.name_order_data.md_rw_cb_ud.nr = 1u;

    /* Set task name */
    iterate_udata->u.name_order_data.md_rw_cb_ud.task_name = "attribute iterate";

    /* Create task for initial daos_obj_list_akey operation */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_LIST_AKEY, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_attribute_iterate_by_name_prep_cb,
                                 H5_daos_attribute_iterate_by_name_comp_cb, iterate_udata,
                                 &list_akey_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to list object's attribute akeys");

    /* Schedule object akey list task (or save it to be scheduled later). */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(list_akey_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't schedule task to list object's attribute akeys: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = list_akey_task;
    akey_buf = NULL;

    *dep_task = list_akey_task;

done:
    akey_buf = (char *)DV_free(akey_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate_by_name_order() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_by_name_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous attribute iteration by
 *              name order. Currently checks for errors from previous tasks
 *              then sets arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_iterate_by_name_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_iterate_ud_t *udata;
    daos_obj_list_akey_t      *list_args;
    int                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object akey list task");

    assert(udata->req);

    /* Handle errors */
    if (udata->iter_data.op_ret < 0)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    H5_DAOS_PREP_REQ(udata->req, H5E_ATTR);

    assert(udata->attr_container_obj);
    assert(udata->u.name_order_data.md_rw_cb_ud.obj);
    assert(udata->req->file);

    /* Determine if short-circuit success was returned in previous tasks */
    if (udata->iter_data.op_ret > 0)
        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);

    /* Register id for target_obj */
    if (udata->iter_data.iter_root_obj < 0) {
        if ((udata->iter_data.iter_root_obj =
                 H5VLwrap_register(udata->attr_container_obj, udata->attr_container_obj->item.type)) < 0)
            D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, -H5_DAOS_SETUP_ERROR, "unable to atomize object handle");
        udata->attr_container_obj->item.rc++;
    } /* end if */

    /* Reset akey_nr */
    udata->u.name_order_data.akey_nr = H5_DAOS_ITER_LEN;

    /* Set list task arguments */
    if (NULL == (list_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for object akey list task");
    memset(list_args, 0, sizeof(*list_args));
    list_args->oh          = udata->u.name_order_data.md_rw_cb_ud.obj->obj_oh;
    list_args->th          = DAOS_TX_NONE;
    list_args->dkey        = &udata->u.name_order_data.md_rw_cb_ud.dkey;
    list_args->nr          = &udata->u.name_order_data.akey_nr;
    list_args->kds         = udata->u.name_order_data.kds;
    list_args->sgl         = udata->u.name_order_data.md_rw_cb_ud.sgl;
    list_args->type        = DAOS_IOD_NONE;
    list_args->akey_anchor = &udata->u.name_order_data.anchor;
    list_args->incr_order =
        udata->iter_data.iter_order == H5_ITER_INC || udata->iter_data.iter_order == H5_ITER_NATIVE;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate_by_name_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_by_name_comp_cb
 *
 * Purpose:     Complete callback for asynchronous attribute iteration by
 *              name order. Currently checks for a failed task then
 *              creates new iteration tasks for each akey retrieved from
 *              daos_obj_list_akey.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_iterate_by_name_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_iterate_ud_t *udata;
    tse_task_t                *first_task = NULL;
    tse_task_t                *dep_task   = NULL;
    int                        ret;
    int                        ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for object akey list task");

    assert(udata->req);
    assert(udata->req->file || (task->dt_result != 0 && task->dt_result != -DER_KEY2BIG));
    assert(udata->u.name_order_data.md_rw_cb_ud.obj ||
           (task->dt_result != 0 && task->dt_result != -DER_KEY2BIG));
    assert(udata->iterate_metatask);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_KEY2BIG) {
        size_t akey_buf_len;
        char  *tmp_realloc;

        /* Allocate larger buffer */
        akey_buf_len = udata->u.name_order_data.md_rw_cb_ud.sg_iov[0].iov_buf_len * 2;
        if (NULL == (tmp_realloc = (char *)DV_realloc(udata->u.name_order_data.md_rw_cb_ud.sg_iov[0].iov_buf,
                                                      akey_buf_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't reallocate key buffer");

        /* Update SGL */
        daos_iov_set(&udata->u.name_order_data.md_rw_cb_ud.sg_iov[0], tmp_realloc,
                     (daos_size_t)(akey_buf_len - 1));
        udata->u.name_order_data.md_rw_cb_ud.sgl[0].sg_nr_out = 0;

        /* Re-register callback functions for re-initialized akey list task */
        if (0 != (ret = tse_task_register_cbs(task, H5_daos_attribute_iterate_by_name_prep_cb, NULL, 0,
                                              H5_daos_attribute_iterate_by_name_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't register callbacks for task to list object's attribute akeys: %s",
                         H5_daos_err_to_string(ret));

        /* Register dependency on dep_task for attribute iteration metatask */
        if (0 != (ret = tse_task_register_deps(udata->iterate_metatask, 1, &task)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                         "can't create dependencies for attribute iteration metatask: %s",
                         H5_daos_err_to_string(ret));

        if (0 != (ret = tse_task_reinit(task)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't re-initialize task to list object's attribute akeys: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else {
        /* Handle errors in object akey list task.  Only record error in udata->req_status if
         * it does not already contain an error (it could contain an error if
         * another task this task is not dependent on also failed). */
        if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = task->dt_result;
            udata->req->failed_task = udata->u.name_order_data.md_rw_cb_ud.task_name;
        } /* end if */
        else if (task->dt_result == 0) {
            uint32_t i;
            char    *p;

            /* Loop over returned akeys */
            p = udata->u.name_order_data.md_rw_cb_ud.sg_iov[0].iov_buf;
            for (i = 0; (i < udata->u.name_order_data.akey_nr); i++) {
                /* Check for invalid key */
                if (udata->u.name_order_data.kds[i].kd_key_len < 3)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, -H5_DAOS_BAD_VALUE, "attribute akey too short");

                /* Only do callbacks for "S-" (dataspace) keys, to avoid duplication */
                if (p[0] == 'S') {
                    char tmp_char;

                    /* Add null terminator temporarily */
                    tmp_char = p[udata->u.name_order_data.kds[i].kd_key_len];
                    p[udata->u.name_order_data.kds[i].kd_key_len] = '\0';

                    H5_DAOS_WAIT_ON_ASYNC_CHAIN(udata->req, first_task, dep_task, H5E_ATTR, H5E_CANTINIT,
                                                -H5_DAOS_SETUP_ERROR);

                    /* Create task to call user-supplied operator callback function */
                    if (H5_daos_attribute_get_iter_op_task(udata, &p[2], udata->req, &first_task, &dep_task) <
                        0)
                        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                     "can't create task to call operator callback function");

                    /* Replace null terminator */
                    p[udata->u.name_order_data.kds[i].kd_key_len] = tmp_char;
                } /* end if */

                /* Advance to next akey */
                p += udata->u.name_order_data.kds[i].kd_key_len;
            } /* end for */

            /* Register dependency on dep_task for attribute iteration metatask */
            if (dep_task && 0 != (ret = tse_task_register_deps(udata->iterate_metatask, 1, &dep_task)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                             "can't create dependencies for attribute iteration metatask: %s",
                             H5_daos_err_to_string(ret));

            /* If there are more akeys, create a task to repeat the akey list operation */
            if (!daos_anchor_is_eof(&udata->u.name_order_data.anchor)) {
                /* Re-register callback functions for re-initialized akey list task */
                if (0 != (ret = tse_task_register_cbs(task, H5_daos_attribute_iterate_by_name_prep_cb, NULL,
                                                      0, H5_daos_attribute_iterate_by_name_comp_cb, NULL, 0)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                 "can't register callbacks for task to list object's attribute akeys: %s",
                                 H5_daos_err_to_string(ret));

                /* Register dependency on operator callback function tasks for re-initialized akey list task
                 */
                if (dep_task && 0 != (ret = tse_task_register_deps(task, 1, &dep_task)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                                 "can't create dependencies for attribute iteration metatask: %s",
                                 H5_daos_err_to_string(ret));

                /* Register dependency on dep_task for attribute iteration metatask */
                if (0 != (ret = tse_task_register_deps(udata->iterate_metatask, 1, &task)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                                 "can't create dependencies for attribute iteration metatask: %s",
                                 H5_daos_err_to_string(ret));

                if (0 != (ret = tse_task_reinit(task)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                 "can't re-initialize task to list object's attribute akeys: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else {
                /* Return task to task list */
                if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
                    D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                 "can't return task to task list");
            }
        } /* end else */
    }     /* end else */

done:
    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                     "can't schedule task to iterate over object's attributes: %s",
                     H5_daos_err_to_string(ret));

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate_by_name_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_by_crt_order
 *
 * Purpose:     Iterates over the attributes attached to the target object
 *              according to their attribute creation order values.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_iterate_by_crt_order(H5_daos_attr_iterate_ud_t *iterate_udata, H5_daos_req_t *req,
                                       tse_task_t **first_task, tse_task_t **dep_task)
{
    uint64_t    cur_idx;
    const char *target_attr_name     = NULL;
    size_t      target_attr_name_len = 0;
    char       *attr_name_buf        = NULL;
    herr_t      ret_value            = SUCCEED;

    assert(iterate_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);
    assert(H5_INDEX_CRT_ORDER == iterate_udata->iter_data.index_type);
    assert(H5_ITER_NATIVE == iterate_udata->iter_data.iter_order ||
           H5_ITER_INC == iterate_udata->iter_data.iter_order ||
           H5_ITER_DEC == iterate_udata->iter_data.iter_order);

    /* Check that creation order is tracked for the attribute's parent object */
    if (!iterate_udata->attr_container_obj->ocpl_cache.track_acorder)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL,
                     "creation order is not tracked for attribute's parent object");

    /* Retrieve the number of attributes attached to the target object */
    if (H5_daos_object_get_num_attrs(iterate_udata->attr_container_obj,
                                     &iterate_udata->u.crt_order_data.obj_nattrs, FALSE, req, first_task,
                                     dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of attributes attached to object");

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

    /* Check if there are no attributes to process */
    if (iterate_udata->u.crt_order_data.obj_nattrs == 0)
        D_GOTO_DONE(SUCCEED);

    /* Register ID for target obj */
    if ((iterate_udata->iter_data.iter_root_obj = H5VLwrap_register(
             iterate_udata->attr_container_obj, iterate_udata->attr_container_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
    iterate_udata->attr_container_obj->item.rc++;

    for (cur_idx = 0; cur_idx < (uint64_t)iterate_udata->u.crt_order_data.obj_nattrs; cur_idx++) {
        if (H5_daos_attribute_get_name_by_idx_alloc(
                iterate_udata->attr_container_obj, iterate_udata->iter_data.index_type,
                iterate_udata->iter_data.iter_order, cur_idx, &target_attr_name, &target_attr_name_len,
                &attr_name_buf, NULL, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name");

        H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

        /* TODO: Temporarily needed to avoid queuing up user-supplied operator
         * callback function when short-circuiting from a previous operation.
         */
        if (req->status == -H5_DAOS_SHORT_CIRCUIT)
            break;

        /* Create task to call user-supplied operator callback function */
        if (H5_daos_attribute_get_iter_op_task(iterate_udata, target_attr_name, req, first_task, dep_task) <
            0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't create task to call operator callback function");

        if (attr_name_buf)
            attr_name_buf = DV_free(attr_name_buf);
    } /* end for */

done:
    if (attr_name_buf)
        attr_name_buf = DV_free(attr_name_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate_by_crt_order() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_iter_op_task
 *
 * Purpose:     Creates an asynchronous task to call a user-supplied
 *              operator callback function during attribute iteration.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_iter_op_task(H5_daos_attr_iterate_ud_t *iterate_udata, const char *attr_name,
                                   H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_iterate_op_ud_t *op_udata = NULL;
    H5VL_loc_params_t             loc_params;
    tse_task_t                   *iter_op_task;
    H5_daos_req_t                *int_int_req = NULL;
    int                           ret;
    herr_t                        ret_value = SUCCEED;

    assert(iterate_udata);
    assert(attr_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (NULL == (op_udata = (H5_daos_attr_iterate_op_ud_t *)DV_malloc(sizeof(H5_daos_attr_iterate_op_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate user data struct for attribute operator callback function task");
    op_udata->get_info_ud.req               = req;
    op_udata->get_info_ud.attr              = NULL;
    op_udata->get_info_ud.get_info_metatask = NULL;
    op_udata->get_info_ud.info_out          = &op_udata->attr_info;
    op_udata->iter_data                     = &iterate_udata->iter_data;

    /* Start internal H5 operation for target attribute open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(iterate_udata->attr_container_obj->item.file,
                                                  "target attribute open within attribute iteration", NULL,
                                                  NULL, req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Create task to open the target attribute */
    loc_params.obj_type                     = iterate_udata->attr_container_obj->item.type;
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = ".";
    loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    if (NULL == (op_udata->get_info_ud.attr = (H5_daos_attr_t *)H5_daos_attribute_open_helper(
                     (H5_daos_item_t *)iterate_udata->attr_container_obj, &loc_params, attr_name,
                     H5P_ATTRIBUTE_ACCESS_DEFAULT, FALSE, int_int_req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open target attribute");

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

    /* Create task to retrieve attribute's info */
    if (H5_daos_attribute_get_info_inplace(&op_udata->get_info_ud, NULL, NULL, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute info");

    /* Create task to call user-supplied operator callback */
    if (H5_daos_create_task(H5_daos_attribute_iterate_op_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, op_udata, &iter_op_task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to call operator callback function");

    /* Schedule operator callback function task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(iter_op_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't schedule task to call operator callback function: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = iter_op_task;
    req->rc++;
    *dep_task = iter_op_task;

    /* Relinquish control of iteration op udata to task. */
    op_udata = NULL;

done:
    if (ret_value < 0) {
        /* Close internal request for target object open */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

        op_udata = DV_free(op_udata);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_iter_op_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_op_task
 *
 * Purpose:     Asynchronous task to call a user-supplied operator callback
 *              function during attribute iteration.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_iterate_op_task(tse_task_t *task)
{
    H5_daos_attr_iterate_op_ud_t *udata;
    tse_task_t                   *metatask   = NULL;
    tse_task_t                   *first_task = NULL;
    tse_task_t                   *dep_task   = NULL;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_OBJECT, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for operator function task");

    assert(udata->get_info_ud.req);

    /* Check for previous errors */
    if (udata->iter_data->op_ret < 0)
        D_GOTO_DONE(-H5_DAOS_PRE_ERROR);
    H5_DAOS_PREP_REQ_PROG(udata->get_info_ud.req);

    /* Determine if short-circuit success was returned in previous tasks */
    if (udata->iter_data->op_ret > 0)
        D_GOTO_DONE(0);

    /* Make callback */
    if (udata->iter_data->async_op) {
        if (udata->iter_data->u.attr_iter_data.u.attr_iter_op_async(
                udata->iter_data->iter_root_obj, udata->get_info_ud.attr->name, &udata->attr_info,
                udata->iter_data->op_data, &udata->iter_data->op_ret, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR,
                         "operator function returned failure");
    }
    else {
        udata->iter_data->op_ret = udata->iter_data->u.attr_iter_data.u.attr_iter_op(
            udata->iter_data->iter_root_obj, udata->get_info_ud.attr->name, &udata->attr_info,
            udata->iter_data->op_data);
    }

    /* Check for failure from operator return */
    if (udata->iter_data->op_ret < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR, "operator function returned failure");

    /* Check for short-circuit success */
    if (udata->iter_data->op_ret) {
        udata->iter_data->req->status        = -H5_DAOS_SHORT_CIRCUIT;
        udata->iter_data->short_circuit_init = TRUE;

        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    }

done:
    if (udata) {
        /* Advance saved index pointer */
        if (udata->iter_data->idx_p)
            (*udata->iter_data->idx_p)++;

        /* Create metatask to complete this task after dep_task if necessary */
        if (dep_task) {
            /* Create metatask */
            if (H5_daos_create_task(H5_daos_metatask_autocomp_other, 1, &dep_task, NULL, NULL, task,
                                    &metatask) < 0) {
                D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create metatask for attribute iter op task");
                if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
                    D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                                 "can't return task to task list");
                tse_task_complete(task, ret_value);
            }
            else {
                /* Schedule metatask */
                assert(first_task);
                if (0 != (ret = tse_task_schedule(metatask, false)))
                    D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                                 "can't schedule metatask for attribute iter op task: %s",
                                 H5_daos_err_to_string(ret));
            } /* end else */
        }

        /* Schedule first task */
        if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, ret,
                         "can't schedule initial task for attribute iteration op: %s",
                         H5_daos_err_to_string(ret));

        /* Close attribute */
        if (H5_daos_attribute_close_real(udata->get_info_ud.attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close attribute");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->get_info_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->get_info_ud.req->status      = ret_value;
            udata->get_info_ud.req->failed_task = "attribute iteration operator callback function task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->get_info_ud.req) < 0)
            D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Complete task if necessary */
    if (!metatask) {
        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");
        tse_task_complete(task, ret_value);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate_op_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_finish
 *
 * Purpose:     Asynchronous task to free attribute iteration udata once
 *              iteration has finished.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_iterate_finish(tse_task_t *task)
{
    H5_daos_attr_iterate_ud_t *udata;
    int                        ret;
    int                        ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute iteration task");

    assert(udata->req);
    assert(udata->attr_container_obj);
    assert(task == udata->iterate_metatask);

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

    /* Close object.  Use nonblocking close so it doesn't deadlock */
    if (udata->iter_data.iter_root_obj >= 0) {
        udata->attr_container_obj->item.nonblocking_close = TRUE;
        if ((ret = H5Idec_ref(udata->iter_data.iter_root_obj)) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object ID");
        if (ret)
            udata->attr_container_obj->item.nonblocking_close = FALSE;
    } /* end if */
    if (H5_daos_object_close(&udata->attr_container_obj->item) < 0)
        D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = ret_value;
        udata->req->failed_task = "attribute iteration udata free task";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->req) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    if (udata->iter_data.index_type == H5_INDEX_NAME)
        if (udata->u.name_order_data.md_rw_cb_ud.free_sg_iov[0])
            DV_free(udata->u.name_order_data.md_rw_cb_ud.sg_iov[0].iov_buf);

    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_iterate_finish() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_count_attrs_cb
 *
 * Purpose:     A callback for H5_daos_attribute_iterate() that simply
 *              counts the number of attributes attached to the given
 *              object.
 *
 * Return:      0 (can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_iterate_count_attrs_cb(hid_t H5VL_DAOS_UNUSED             loc_id,
                                         const char H5VL_DAOS_UNUSED       *attr_name,
                                         const H5A_info_t H5VL_DAOS_UNUSED *attr_info, void *op_data)
{
    (*((hsize_t *)op_data))++;
    return 0;
} /* end H5_daos_attribute_iterate_count_attrs_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_rename
 *
 * Purpose:     Helper routine to rename an HDF5 attribute by recreating it
 *              under different akeys and deleting the old attribute.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_rename(H5_daos_obj_t *attr_container_obj, const char *cur_attr_name,
                         const char *new_attr_name, hbool_t collective, H5_daos_req_t *req,
                         tse_task_t **first_task, tse_task_t **dep_task)
{
    H5VL_attr_specific_args_t delete_args;
    H5VL_loc_params_t         sub_loc_params;
    H5_daos_attr_t           *cur_attr = NULL;
    H5_daos_attr_t           *new_attr = NULL;
    hssize_t                  attr_space_nelmts;
    size_t                    attr_type_size;
    void                     *attr_data_buf = NULL;
    H5_daos_req_t            *int_int_req   = NULL;
    int                       ret;
    herr_t                    ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(cur_attr_name);
    assert(new_attr_name);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Start internal H5 operation for target attribute open.  This will
     * not be visible to the API, will not be added to an operation
     * pool, and will be integrated into this function's task chain. */
    if (NULL == (int_int_req = H5_daos_req_create(attr_container_obj->item.file,
                                                  "target attribute open within attribute rename", NULL, NULL,
                                                  req, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Open the existing attribute */
    sub_loc_params.type     = H5VL_OBJECT_BY_SELF;
    sub_loc_params.obj_type = H5I_ATTR;
    if (NULL == (cur_attr = (H5_daos_attr_t *)H5_daos_attribute_open_helper(
                     (H5_daos_item_t *)attr_container_obj, &sub_loc_params, cur_attr_name,
                     H5P_ATTRIBUTE_ACCESS_DEFAULT, collective, int_int_req, first_task, dep_task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute");

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

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

    /* Create the new attribute if this process should */
    if (!collective || (attr_container_obj->item.file->my_rank == 0)) {
        /* Start internal H5 operation for target attribute create.  This will
         * not be visible to the API, will not be added to an operation
         * pool, and will be integrated into this function's task chain. */
        if (NULL == (int_int_req = H5_daos_req_create(attr_container_obj->item.file,
                                                      "target attribute create within attribute rename", NULL,
                                                      NULL, req, H5I_INVALID_HID)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        if (NULL == (new_attr = (H5_daos_attr_t *)H5_daos_attribute_create_helper(
                         &attr_container_obj->item, &sub_loc_params, cur_attr->type_id, cur_attr->space_id,
                         cur_attr->acpl_id, H5P_ATTRIBUTE_ACCESS_DEFAULT, new_attr_name, FALSE, int_int_req,
                         first_task, dep_task)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, FAIL, "can't create new attribute");

        /* Create task to finalize internal operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                                NULL, int_int_req, &int_int_req->finalize_task) < 0)
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

        /* Transfer data from the old attribute to the new attribute */
        if (0 == (attr_type_size = H5Tget_size(cur_attr->type_id)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve attribute's datatype size");

        if ((attr_space_nelmts = H5Sget_simple_extent_npoints(cur_attr->space_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL,
                         "can't retrieve number of elements in attribute's dataspace");

        if (NULL == (attr_data_buf = DV_malloc(attr_type_size * (size_t)attr_space_nelmts)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't allocate buffer for attribute data");

        if (H5_daos_attribute_read_int(cur_attr, cur_attr->type_id, FALSE, -1, attr_data_buf, NULL, req,
                                       first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read data from attribute");

        if (H5_daos_attribute_write_int(new_attr, new_attr->type_id, FALSE, -1, attr_data_buf, NULL, req,
                                        first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write data to attribute");
    } /* end if */

    /* Delete the old attribute */
    delete_args.op_type       = H5VL_ATTR_DELETE;
    delete_args.args.del.name = cur_attr_name;
    if (H5_daos_attribute_delete(attr_container_obj, &sub_loc_params, &delete_args, collective, req,
                                 first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "can't delete old attribute");

    H5_DAOS_WAIT_ON_ASYNC_CHAIN(req, *first_task, *dep_task, H5E_ATTR, H5E_CANTINIT, FAIL);

done:
    if (collective && (attr_container_obj->item.file->num_procs > 1))
        if (H5_daos_collective_error_check(attr_container_obj, req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't perform collective error check");

    attr_data_buf = DV_free(attr_data_buf);

    if (new_attr) {
        if (H5_daos_attribute_close_real(new_attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");
        new_attr = NULL;
    }
    if (cur_attr) {
        if (H5_daos_attribute_close_real(cur_attr) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute");
        cur_attr = NULL;
    }

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_rename() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attr_gnbi_alloc_task
 *
 * Purpose:     Asynchronous task for
 *              H5_daos_attribute_get_name_by_idx_alloc(). Executes once
 *              all parameters are valid.
 *
 * Return:      Success:        0
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attr_gnbi_alloc_task(tse_task_t *task)
{
    H5_daos_attr_gnbi_alloc_ud_t *udata      = NULL;
    tse_task_t                   *first_task = NULL;
    tse_task_t                   *dep_task   = NULL;
    int                           ret_value  = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute get name by index task");

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_ATTR);

    /* Check if we need to issue another get operation */
    if (*udata->attr_name_size > udata->cur_attr_name_size - 1) {
        /* Reallocate buffer */
        DV_free(*udata->attr_name_buf);
        if (NULL == (*udata->attr_name_buf = DV_malloc(*udata->attr_name_size + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                         "can't allocate attribute name buffer");
        udata->cur_attr_name_size = *udata->attr_name_size + 1;

        /* Reissue call with larger buffer and transfer ownership of udata */
        if (H5_daos_attribute_get_name_by_idx(udata->target_obj, udata->index_type, udata->iter_order,
                                              udata->idx, *udata->attr_name_buf, udata->cur_attr_name_size,
                                              udata->attr_name_size, udata->req, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get attribute name by index");
        udata = NULL;
    } /* end if */

done:
    /* Finish task if we still own udata */
    if (udata) {
        /* Assign attr_name */
        *udata->attr_name = *udata->attr_name_buf;

        /* Return attr_name_buf_size */
        if (udata->attr_name_buf_size)
            *udata->attr_name_buf_size = udata->cur_attr_name_size;

        /* Close target_obj */
        if (H5_daos_object_close(&udata->target_obj->item) < 0)
            D_DONE_ERROR(H5E_OBJECT, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");
        udata->target_obj = NULL;

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute get name by index end task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Complete main task if different from this task */
        if (udata->gnbi_task != task) {
            /* Return task to task list */
            if (H5_daos_task_list_put(H5_daos_task_list_g, udata->gnbi_task) < 0)
                D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                             "can't return task to task list");
            tse_task_complete(udata->gnbi_task, ret_value);
        }

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

        /* Complete this task */
        tse_task_complete(task, ret_value);

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attr_gnbi_alloc_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_idx_alloc
 *
 * Purpose:     Like H5_daos_attribute_get_name_by_idx, but automatically
 *              allocates the attr_name buffer.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_idx_alloc(H5_daos_obj_t *target_obj, H5_index_t index_type,
                                        H5_iter_order_t iter_order, uint64_t idx, const char **attr_name,
                                        size_t *attr_name_size, char **attr_name_buf,
                                        size_t *attr_name_buf_size, H5_daos_req_t *req,
                                        tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_gnbi_alloc_ud_t *gnbi_udata = NULL;
    int                           ret;
    herr_t                        ret_value = SUCCEED;

    /* Allocate task udata struct */
    if (NULL ==
        (gnbi_udata = (H5_daos_attr_gnbi_alloc_ud_t *)DV_calloc(sizeof(H5_daos_attr_gnbi_alloc_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate get name by index user data");
    gnbi_udata->req                = req;
    gnbi_udata->target_obj         = target_obj;
    gnbi_udata->index_type         = index_type;
    gnbi_udata->iter_order         = iter_order;
    gnbi_udata->idx                = idx;
    gnbi_udata->attr_name          = attr_name;
    gnbi_udata->attr_name_size     = attr_name_size;
    gnbi_udata->attr_name_buf      = attr_name_buf;
    gnbi_udata->attr_name_buf_size = attr_name_buf_size;

    /* Check for preexisting name buffer */
    if (*attr_name_buf) {
        assert(attr_name_buf_size);
        assert(*attr_name_buf_size);
        gnbi_udata->cur_attr_name_size = *attr_name_buf_size;
    } /* end if */
    else {
        /* Allocate initial name buffer */
        if (NULL == (*gnbi_udata->attr_name_buf = DV_malloc(H5_DAOS_ATTR_NAME_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate attribute name buffer");
        gnbi_udata->cur_attr_name_size = H5_DAOS_ATTR_NAME_BUF_SIZE;
    } /* end else */

    /* Call underlying function */
    if (H5_daos_attribute_get_name_by_idx(target_obj, index_type, iter_order, idx, *gnbi_udata->attr_name_buf,
                                          gnbi_udata->cur_attr_name_size, gnbi_udata->attr_name_size, req,
                                          first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get link name by index");

    /* Create task to finish this operation */
    if (H5_daos_create_task(H5_daos_attr_gnbi_alloc_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                            NULL, NULL, gnbi_udata, &gnbi_udata->gnbi_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task for attribute get name by index");

    /* Schedule gnbi task (or save it to be scheduled later) and give it a
     * reference to the group, req and udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(gnbi_udata->gnbi_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't schedule task for attribute get name by index: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = gnbi_udata->gnbi_task;
    *dep_task = gnbi_udata->gnbi_task;
    target_obj->item.rc++;
    req->rc++;
    gnbi_udata = NULL;

done:
    /* Clean up */
    if (gnbi_udata) {
        assert(ret_value < 0);
        gnbi_udata = DV_free(gnbi_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_idx_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_idx
 *
 * Purpose:     Given an index type, index iteration order and index value,
 *              retrieves the name of the nth attribute (as specified by
 *              the index value) within the given index (name index or
 *              creation order index) according to the given order
 *              (increasing, decreasing or native order).
 *
 *              The attr_name_out parameter may be NULL, in which case the
 *              length of the attribute's name is returned through
 *              size_ret. If non-NULL, the attribute's name is stored in
 *              attr_name_out.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_idx(H5_daos_obj_t *target_obj, H5_index_t index_type,
                                  H5_iter_order_t iter_order, uint64_t idx, char *attr_name_out,
                                  size_t attr_name_out_size, size_t *attr_name_size, H5_daos_req_t *req,
                                  tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_attr_get_name_by_idx_ud_t *get_name_udata = NULL;
    herr_t                             ret_value      = SUCCEED;

    assert(target_obj);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate argument struct for name retrieval task */
    if (NULL == (get_name_udata = (H5_daos_attr_get_name_by_idx_ud_t *)DV_calloc(
                     sizeof(H5_daos_attr_get_name_by_idx_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for attribute name retrieval task arguments");
    get_name_udata->req                = req;
    get_name_udata->target_obj         = target_obj;
    get_name_udata->index_type         = index_type;
    get_name_udata->iter_order         = iter_order;
    get_name_udata->idx                = idx;
    get_name_udata->attr_name_out      = attr_name_out;
    get_name_udata->attr_name_out_size = attr_name_out_size;
    get_name_udata->attr_name_size_ret = attr_name_size;

    if (H5_INDEX_NAME == index_type) {
        if (H5_daos_attribute_get_name_by_name_order(get_name_udata, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve attribute name from name order index");
    } /* end if */
    else if (H5_INDEX_CRT_ORDER == index_type) {
        if (H5_daos_attribute_get_name_by_crt_order(get_name_udata, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL,
                         "can't retrieve attribute name from creation order index");
    } /* end else */
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "invalid or unsupported index type");

    /* Create task to free udata after attribute name retrieval has completed */
    if (H5_daos_attribute_get_name_by_idx_free_udata(get_name_udata, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to free attribute name retrieval data");

    /* Relinquish control of the attribute name retrieval udata to the
     * task's completion callback */
    get_name_udata = NULL;

done:
    get_name_udata = DV_free(get_name_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_name_order
 *
 * Purpose:     Creates asynchronous tasks for retrieving an attribute's
 *              name by an index value according to alphabetical order.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_name_order(H5_daos_attr_get_name_by_idx_ud_t *get_name_udata,
                                         H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_iter_data_t iter_data;
    tse_task_t         *no_attrs_check_task = NULL;
    int                 ret;
    herr_t              ret_value = 0;

    assert(get_name_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);

    if (H5_ITER_DEC == get_name_udata->iter_order)
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "decreasing order iteration is unsupported");

    /* Retrieve the current number of attributes attached to the target object */
    if (H5_daos_object_get_num_attrs(get_name_udata->target_obj, &get_name_udata->obj_nattrs, FALSE, req,
                                     first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of attributes attached to object");

    /* Create task to check that requested index value is within range
     * of the number of attributes attached to the object.
     */
    if (H5_daos_create_task(H5_daos_attribute_gnbno_no_attrs_check_task, *dep_task ? 1 : 0,
                            *dep_task ? dep_task : NULL, NULL, NULL, get_name_udata,
                            &no_attrs_check_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to check for no attributes on object");

    /* Schedule attribute count check task (or save it to be scheduled later) and
     * give it a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(no_attrs_check_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't schedule task to check for no attributes on object: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = no_attrs_check_task;
    *dep_task = no_attrs_check_task;
    req->rc++;

    /* Initialize iteration data */
    get_name_udata->u.by_name_data.cur_attr_idx = 0;
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, get_name_udata->iter_order,
                           FALSE, NULL, H5I_INVALID_HID, get_name_udata, NULL, req);
    iter_data.u.attr_iter_data.u.attr_iter_op = H5_daos_attribute_get_name_by_name_order_cb;

    if (H5_daos_attribute_iterate(get_name_udata->target_obj, &iter_data, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_name_order() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_name_order_cb
 *
 * Purpose:     Attribute iteration callback for
 *              H5_daos_attribute_get_name_by_name_order which iterates
 *              through attributes by name order until the specified index
 *              value is reached, at which point the target attribute has
 *              been found and its name is copied back.
 *
 * Return:      Non-negative (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_name_order_cb(hid_t H5VL_DAOS_UNUSED loc_id, const char *attr_name,
                                            const H5A_info_t H5VL_DAOS_UNUSED *attr_info, void *op_data)
{
    H5_daos_attr_get_name_by_idx_ud_t *udata     = (H5_daos_attr_get_name_by_idx_ud_t *)op_data;
    herr_t                             ret_value = H5_ITER_CONT;

    /* Check the index is within range */
    if (udata->u.by_name_data.cur_attr_idx >= (uint64_t)udata->obj_nattrs)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, H5_ITER_ERROR, "index value out of range");

    /*
     * Once the target index has been reached, set the size of the attribute
     * name to be returned and copy the attribute name if the attribute name
     * output buffer is not NULL.
     */
    if (udata->u.by_name_data.cur_attr_idx == udata->idx) {
        size_t attr_name_len = strlen(attr_name);

        if (udata->attr_name_out && udata->attr_name_out_size > 0) {
            size_t copy_len = MIN(attr_name_len, udata->attr_name_out_size - 1);

            memcpy(udata->attr_name_out, attr_name, copy_len);
            udata->attr_name_out[copy_len] = '\0';
        }

        *udata->attr_name_size_ret = attr_name_len;

        D_GOTO_DONE(H5_ITER_STOP);
    }

    udata->u.by_name_data.cur_attr_idx++;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_name_order_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_gnbno_no_attrs_check_task
 *
 * Purpose:     Asynchronous task to check that an object has attributes
 *              attached to it before attempting to retrieve the name of an
 *              attribute on that object according to an index value.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_gnbno_no_attrs_check_task(tse_task_t *task)
{
    H5_daos_attr_get_name_by_idx_ud_t *udata     = NULL;
    int                                ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute count check task");

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_ATTR);

    /* Ensure the index is within range */
    if (udata->idx >= (uint64_t)udata->obj_nattrs)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, -H5_DAOS_BAD_VALUE, "index value out of range");

done:
    if (udata) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < 0) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute count check";
        }

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_gnbno_no_attrs_check_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_crt_order
 *
 * Purpose:     Creates asynchronous tasks for retrieving an attribute's
 *              name by an index value according to creation order.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_crt_order(H5_daos_attr_get_name_by_idx_ud_t *get_name_udata, H5_daos_req_t *req,
                                        tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *fetch_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(get_name_udata);
    assert(req);
    assert(first_task);
    assert(dep_task);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Check that creation order is tracked for the attribute's parent object */
    if (!get_name_udata->target_obj->ocpl_cache.track_acorder)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL,
                     "creation order is not tracked for attribute's parent object");

    /* Retrieve the current number of attributes attached to the target object */
    if (H5_daos_object_get_num_attrs(get_name_udata->target_obj, &get_name_udata->obj_nattrs, FALSE, req,
                                     first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of attributes attached to object");

    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.obj = get_name_udata->target_obj;
    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.req = req;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&get_name_udata->u.by_crt_order_data.md_rw_cb_ud.dkey,
                       H5_daos_attr_key_g, H5_daos_attr_key_size_g);
    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.free_dkey = FALSE;

    /* Set up iod */
    memset(&get_name_udata->u.by_crt_order_data.md_rw_cb_ud.iod[0], 0, sizeof(daos_iod_t));
    daos_iov_set(&get_name_udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_name,
                 (void *)get_name_udata->u.by_crt_order_data.idx_buf, H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1);
    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_nr   = 1u;
    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.free_akeys = FALSE;

    /* Set up sgl if attr_name_out buffer is supplied */
    if (get_name_udata->attr_name_out && get_name_udata->attr_name_out_size > 0) {
        daos_iov_set(&get_name_udata->u.by_crt_order_data.md_rw_cb_ud.sg_iov[0],
                     get_name_udata->attr_name_out, get_name_udata->attr_name_out_size - 1);
        get_name_udata->u.by_crt_order_data.md_rw_cb_ud.sgl[0].sg_nr     = 1;
        get_name_udata->u.by_crt_order_data.md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        get_name_udata->u.by_crt_order_data.md_rw_cb_ud.sgl[0].sg_iovs =
            &get_name_udata->u.by_crt_order_data.md_rw_cb_ud.sg_iov[0];
        get_name_udata->u.by_crt_order_data.md_rw_cb_ud.free_sg_iov[0] = FALSE;
    } /* end if */

    /* Set conditional akey fetch for attribute name read operation */
    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.flags = DAOS_COND_AKEY_FETCH;

    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.nr = 1u;

    get_name_udata->u.by_crt_order_data.md_rw_cb_ud.task_name = "attribute name retrieval";

    /* Create task to fetch attribute's name */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_attribute_get_name_by_crt_order_prep_cb,
                                 H5_daos_attribute_get_name_by_crt_order_comp_cb, get_name_udata,
                                 &fetch_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't create task to retrieve attribute's name");

    /* Schedule attribute name fetch task (or save it to be scheduled later) and give it
     * a reference to req */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't schedule task to retrieve attribute's name: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        *first_task = fetch_task;

    *dep_task = fetch_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_crt_order() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_crt_order_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous task to retrieve an
 *              attribute's name according to a creation order index.
 *              Currently checks for errors from previous tasks, encodes
 *              the index value into the buffer for the fetch operation and
 *              then sets arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_get_name_by_crt_order_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_get_name_by_idx_ud_t *udata;
    daos_obj_rw_t                     *fetch_args;
    uint64_t                           fetch_idx = 0;
    uint8_t                           *p;
    int                                ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute name fetch task");

    assert(udata->u.by_crt_order_data.md_rw_cb_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->u.by_crt_order_data.md_rw_cb_ud.req, H5E_ATTR);

    assert(udata->u.by_crt_order_data.md_rw_cb_ud.obj);
    assert(udata->u.by_crt_order_data.md_rw_cb_ud.req->file);

    /* Ensure the index is within range */
    if (udata->idx >= (uint64_t)udata->obj_nattrs)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, -H5_DAOS_BAD_VALUE, "index value out of range");

    /* Calculate the correct index of the attribute, based upon the iteration order */
    if (H5_ITER_DEC == udata->iter_order)
        fetch_idx = (uint64_t)udata->obj_nattrs - udata->idx - 1;
    else
        fetch_idx = udata->idx;

    p    = udata->u.by_crt_order_data.idx_buf;
    *p++ = 0;
    UINT64ENCODE(p, fetch_idx);

    /* Set fetch task arguments */
    if (NULL == (fetch_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for attribute name fetch task");
    memset(fetch_args, 0, sizeof(*fetch_args));
    fetch_args->oh    = udata->u.by_crt_order_data.md_rw_cb_ud.obj->obj_oh;
    fetch_args->th    = DAOS_TX_NONE;
    fetch_args->flags = udata->u.by_crt_order_data.md_rw_cb_ud.flags;
    fetch_args->dkey  = &udata->u.by_crt_order_data.md_rw_cb_ud.dkey;
    fetch_args->nr    = udata->u.by_crt_order_data.md_rw_cb_ud.nr;
    fetch_args->iods  = udata->u.by_crt_order_data.md_rw_cb_ud.iod;
    fetch_args->sgls  = (udata->attr_name_out) ? udata->u.by_crt_order_data.md_rw_cb_ud.sgl : NULL;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_crt_order_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_crt_order_comp_cb
 *
 * Purpose:     Complete callback for asynchronous task to retrieve an
 *              attribute's name according to a creation order index.
 *              Currently checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_get_name_by_crt_order_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_attr_get_name_by_idx_ud_t *udata;
    int                                ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for attribute name fetch task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR &&
        udata->u.by_crt_order_data.md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->u.by_crt_order_data.md_rw_cb_ud.req->status = task->dt_result;
        udata->u.by_crt_order_data.md_rw_cb_ud.req->failed_task =
            udata->u.by_crt_order_data.md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        /* Check for missing metadata */
        if (udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_size == (daos_size_t)0)
            D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, -H5_DAOS_H5_GET_ERROR, "attribute name record not found");

        if (udata->attr_name_out && udata->attr_name_out_size > 0) {
            size_t nul_term_pos =
                MIN(udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_size, udata->attr_name_out_size - 1);
            udata->attr_name_out[nul_term_pos] = '\0';
        }

        *udata->attr_name_size_ret = (size_t)udata->u.by_crt_order_data.md_rw_cb_ud.iod[0].iod_size;
    } /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_crt_order_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_idx_free_udata
 *
 * Purpose:     Creates an asynchronous task for freeing private data after
 *              retrieval of an attribute's name by an index value has
 *              completed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_idx_free_udata(H5_daos_attr_get_name_by_idx_ud_t *udata, H5_daos_req_t *req,
                                             tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *free_task;
    int         ret;
    herr_t      ret_value = SUCCEED;

    assert(udata);
    assert(first_task);
    assert(dep_task);

    /* Create task for freeing udata */
    if (H5_daos_create_task(H5_daos_attribute_get_name_by_idx_free_udata_task, *dep_task ? 1 : 0,
                            *dep_task ? dep_task : NULL, NULL, NULL, udata, &free_task) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                     "can't create task to free attribute name retrieval udata");

    /* Schedule attribute name retrieval udata free task (or save it to be scheduled later) and
     * give it a reference to req and target_obj */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(free_task, false)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL,
                         "can't schedule task to free attribute name retrieval udata: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = free_task;
    udata->target_obj->item.rc++;
    req->rc++;
    *dep_task = free_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_idx_free_udata() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_idx_free_udata_task
 *
 * Purpose:     Asynchronous task to free private data after retrieval of
 *              an attribute's name by an index value has completed.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_attribute_get_name_by_idx_free_udata_task(tse_task_t *task)
{
    H5_daos_attr_get_name_by_idx_ud_t *udata;
    int                                ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for task to free attribute name retrieval udata");

    assert(udata->req);
    assert(udata->target_obj);

done:
    if (udata) {
        /* Close object */
        if (H5_daos_object_close(&udata->target_obj->item) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close object");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "attribute name retrieval udata free task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        DV_free(udata);
    }
    else
        assert(ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_name_by_idx_free_udata_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_crt_order_by_name
 *
 * Purpose:     Retrieves the creation order value for an attribute given
 *              the attribute's parent object and the attribute's name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_crt_order_by_name(H5_daos_obj_t *target_obj, const char *attr_name, uint64_t *crt_order)
{
    daos_sg_list_t sgl;
    daos_key_t     dkey;
    daos_key_t     acorder_key;
    daos_iod_t     iod;
    daos_iov_t     sg_iov;
    uint64_t       crt_order_val;
    uint8_t        crt_order_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t       *p;
    void          *akeys_buf = NULL;
    int            ret;
    herr_t         ret_value = SUCCEED;

    assert(target_obj);
    assert(attr_name);
    assert(crt_order);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Check that creation order is tracked for the attribute's parent object */
    if (!target_obj->ocpl_cache.track_acorder)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL,
                     "creation order is not tracked for attribute's parent object");

    /* Retrieve akey string for creation order */
    if (H5_daos_attribute_get_akeys(attr_name, NULL, NULL, NULL, &acorder_key, NULL, &akeys_buf) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute creation order akey string");

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, acorder_key.iov_buf, (daos_size_t)acorder_key.iov_len);
    iod.iod_nr   = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, crt_order_buf, (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
    sgl.sg_nr     = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs   = &sg_iov;

    /* Read attribute creation order value */
    if (0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, DAOS_COND_AKEY_FETCH, &dkey, 1, &iod,
                                   &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read attribute's creation order value: %s",
                     H5_daos_err_to_string(ret));

    if (iod.iod_size == 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, FAIL, "attribute creation order value record is missing");

    p = crt_order_buf;
    UINT64DECODE(p, crt_order_val);

    *crt_order = crt_order_val;

done:
    akeys_buf = DV_free(akeys_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_attribute_get_crt_order_by_name() */
