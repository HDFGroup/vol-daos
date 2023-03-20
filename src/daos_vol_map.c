/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library.  Map routines
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

/****************/
/* Local Macros */
/****************/

#define H5_DAOS_MINFO_BCAST_BUF_SIZE                                                                         \
    ((2 * H5_DAOS_TYPE_BUF_SIZE) + H5_DAOS_MCPL_BUF_SIZE + H5_DAOS_ENCODED_OID_SIZE +                        \
     (3 * H5_DAOS_ENCODED_UINT64_T_SIZE))

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Task user data for reading the value of a
 * specified key in a map object or for adding/
 * updating a key-value pair in a map object.
 */
typedef struct H5_daos_map_rw_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud; /* Must be first */
    const void           *key_buf;
    void                 *key_buf_alloc;
    size_t                key_size;
    htri_t                val_need_tconv;
    void                 *tconv_buf;
    void                 *bkg_buf;
    void                 *value_buf;
    hid_t                 val_mem_type_id;
    size_t                val_mem_type_size;
    size_t                val_file_type_size;
} H5_daos_map_rw_ud_t;

/* Task user data for checking if a particular
 * key exists in a map object.
 */
typedef struct H5_daos_map_exists_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud; /* Must be first */
    const void           *key_buf;
    void                 *key_buf_alloc;
    size_t                key_size;
    hbool_t              *exists_ret;
} H5_daos_map_exists_ud_t;

/* A struct used to operate on a single key-value
 * pair during map iteration */
typedef struct H5_daos_map_iter_op_ud_t {
    H5_daos_generic_cb_ud_t generic_ud; /* Must be first */
    H5_daos_iter_ud_t      *iter_ud;
    H5_daos_vl_union_t      vl_union;
    void                   *key_buf;
    void                   *key_buf_alloc;
    size_t                  key_len;
    hid_t                   key_file_type_id;
    hid_t                   key_mem_type_id;
    char                   *char_replace_loc;
    char                    char_replace_char;
    tse_task_t             *op_task;
    /* Fields for querying for existence of Map Record
     * akey for map keys that share dkey with other metadata
     */
    hbool_t    shared_dkey;
    daos_key_t dkey;
    daos_iod_t iod;
} H5_daos_map_iter_op_ud_t;

/* Task user data for deleting a key-value pair from a map */
typedef struct H5_daos_map_delete_key_ud_t {
    H5_daos_req_t *req;
    H5_daos_map_t *map;
    daos_key_t     dkey;
    daos_key_t     akey;
    hbool_t        shared_dkey;
    const void    *key_buf;
    void          *key_buf_alloc;
    size_t         key_size;
} H5_daos_map_delete_key_ud_t;

/********************/
/* Local Prototypes */
/********************/

static int H5_daos_minfo_read_comp_cb(tse_task_t *task, void *args);
static int H5_daos_map_open_bcast_comp_cb(tse_task_t *task, void *args);
static int H5_daos_map_open_recv_comp_cb(tse_task_t *task, void *args);
static int H5_daos_map_open_end(H5_daos_map_t *map, uint8_t *p, uint64_t ktype_buf_len,
                                uint64_t vtype_buf_len, uint64_t mcpl_buf_len, hid_t dxpl_id);

static herr_t H5_daos_map_key_conv(hid_t src_type_id, hid_t dst_type_id, const void *key,
                                   const void **key_buf, size_t *key_size, void **key_buf_alloc,
                                   hid_t dxpl_id);
static herr_t H5_daos_map_key_conv_reverse(hid_t src_type_id, hid_t dst_type_id, void *key, size_t key_size,
                                           void **key_buf, void **key_buf_alloc, H5_daos_vl_union_t *vl_union,
                                           hid_t dxpl_id);
static int    H5_daos_map_get_val_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_map_put_fill_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_map_put_comp_cb(tse_task_t *task, void *args);

static int H5_daos_map_exists_prep_cb(tse_task_t *task, void *args);
static int H5_daos_map_exists_comp_cb(tse_task_t *task, void *args);

static herr_t H5_daos_map_get_count_cb(hid_t map_id, const void *key, void *_int_count);
static herr_t H5_daos_map_iterate(H5_daos_map_t *map, H5_daos_iter_data_t *iter_data, tse_task_t **first_task,
                                  tse_task_t **dep_task);
static int    H5_daos_map_iterate_list_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_map_iterate_query_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_map_iterate_op_task(tse_task_t *task);
static int    H5_daos_map_iter_op_end(tse_task_t *task);

static herr_t H5_daos_map_delete_key(H5_daos_map_t *map, hid_t key_mem_type_id, const void *key,
                                     hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
                                     tse_task_t **dep_task);
static int    H5_daos_map_delete_key_prep_cb(tse_task_t *task, void *args);
static int    H5_daos_map_delete_key_comp_cb(tse_task_t *task, void *args);

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_create
 *
 * Purpose:     Sends a request to DAOS to create a map
 *
 * Return:      Success:        map object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_map_create(void *_item, const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
                   hid_t lcpl_id, hid_t ktype_id, hid_t vtype_id, hid_t mcpl_id, hid_t mapl_id,
                   hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t             *item            = (H5_daos_item_t *)_item;
    H5_daos_map_t              *map             = NULL;
    H5_daos_obj_t              *target_obj      = NULL;
    char                       *path_buf        = NULL;
    const char                 *target_name     = NULL;
    size_t                      target_name_len = 0;
    H5T_class_t                 ktype_class;
    htri_t                      has_vl_vlstr_ref;
    hid_t                       ktype_parent_id = H5I_INVALID_HID;
    hbool_t                     collective;
    H5_daos_md_rw_cb_ud_flex_t *update_cb_ud   = NULL;
    int                         finalize_ndeps = 0;
    tse_task_t                 *finalize_deps[2];
    H5_daos_req_t              *int_req      = NULL;
    tse_task_t                 *first_task   = NULL;
    tse_task_t                 *dep_task     = NULL;
    hbool_t                     default_mcpl = (mcpl_id == H5P_MAP_CREATE_DEFAULT);
    int                         ret;
    void                       *ret_value = NULL;

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map parent object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /* Check validity of key type.  Vlens are only allwed at the top level, no
     * references allowed at all. */
    if (H5T_NO_CLASS == (ktype_class = H5Tget_class(ktype_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTGET, NULL, "can't get key type class");
    if (ktype_class == H5T_VLEN) {
        /* Vlen types must not contain any nested vlens */
        if ((ktype_parent_id = H5Tget_super(ktype_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTGET, NULL, "can't get key type parent");
        if ((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(ktype_parent_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTINIT, NULL, "can't check for vlen or reference type");
        if (has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type contains nested vlen or reference");
    } /* end if */
    else if (ktype_class == H5T_REFERENCE)
        /* References not supported */
        D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type is a reference type");
    else if (ktype_class != H5T_STRING) {
        /* No nested vlens */
        if ((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(ktype_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTINIT, NULL, "can't check for vlen or reference type");
        if (has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type contains nested vlen or reference");
    } /* end if */

    /* Check for write access */
    if (!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file");

    /*
     * Determine if independent metadata writes have been requested. Otherwise,
     * like HDF5, metadata writes are collective by default.
     */
    H5_DAOS_GET_METADATA_WRITE_MODE(item->file, mapl_id, H5P_MAP_ACCESS_DEFAULT, collective, H5E_MAP, NULL);

    /* Start H5 operation */
    if (NULL ==
        (int_req = H5_daos_req_create(item->file, "map create", item->open_req, NULL, NULL, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, NULL, "can't create DAOS request");

    /* Allocate the map object that is returned to the user */
    if (NULL == (map = H5FL_CALLOC(H5_daos_map_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS map struct");
    map->obj.item.type     = H5I_MAP;
    map->obj.item.created  = TRUE;
    map->obj.item.open_req = int_req;
    int_req->rc++;
    map->obj.item.file    = item->file;
    map->obj.item.rc      = 1;
    map->obj.obj_oh       = DAOS_HDL_INVAL;
    map->key_type_id      = H5I_INVALID_HID;
    map->key_file_type_id = H5I_INVALID_HID;
    map->val_type_id      = H5I_INVALID_HID;
    map->val_file_type_id = H5I_INVALID_HID;
    map->mcpl_id          = H5P_MAP_CREATE_DEFAULT;
    map->mapl_id          = H5P_MAP_ACCESS_DEFAULT;

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, 0, NULL /*event*/)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't start transaction");
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
            D_GOTO_ERROR(H5E_MAP, H5E_BADITER, NULL, "can't traverse path");

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        /* Reject invalid object names during object creation - if a name is
         * given it must parse to a link name that can be created */
        if (target_name_len == 0)
            D_GOTO_ERROR(H5E_MAP, H5E_BADVALUE, NULL, "path given does not resolve to a final link name");
    } /* end if */

    /* Generate map oid */
    if (H5_daos_oid_generate(&map->obj.oid, FALSE, 0, H5I_MAP, (default_mcpl ? H5P_DEFAULT : mcpl_id),
                             H5_DAOS_OBJ_CLASS_NAME, item->file, collective, int_req, &first_task,
                             &dep_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't generate object id");

    /* Open map object */
    if (H5_daos_obj_open(item->file, int_req, &map->obj.oid, DAOS_OO_RW, &map->obj.obj_oh, "map object open",
                         &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map object");

    /* Create map and write metadata if this process should */
    if (!collective || (item->file->my_rank == 0)) {
        size_t      mcpl_size  = 0;
        size_t      ktype_size = 0;
        size_t      vtype_size = 0;
        void       *ktype_buf  = NULL;
        void       *vtype_buf  = NULL;
        void       *mcpl_buf   = NULL;
        tse_task_t *update_task;

        /* Determine serialized datatype sizes */
        if (H5Tencode(ktype_id, NULL, &ktype_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype");
        if (H5Tencode(vtype_id, NULL, &vtype_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype");

        /* Determine serialized MCPL size if not the default */
        if (!default_mcpl)
            if (H5Pencode2(mcpl_id, NULL, &mcpl_size, item->file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of mcpl");

        /* Create map */
        /* Allocate argument struct */
        if (NULL == (update_cb_ud = (H5_daos_md_rw_cb_ud_flex_t *)DV_calloc(
                         sizeof(H5_daos_md_rw_cb_ud_flex_t) + ktype_size + vtype_size + mcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for update callback arguments");

        /* Encode datatypes */
        ktype_buf = update_cb_ud->flex_buf;
        if (H5Tencode(ktype_id, ktype_buf, &ktype_size) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize datatype");

        vtype_buf = update_cb_ud->flex_buf + ktype_size;
        if (H5Tencode(vtype_id, vtype_buf, &vtype_size) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize datatype");

        /* Encode MCPL if not the default */
        if (!default_mcpl) {
            mcpl_buf = update_cb_ud->flex_buf + ktype_size + vtype_size;
            if (H5Pencode2(mcpl_id, mcpl_buf, &mcpl_size, item->file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize mcpl");
        } /* end if */
        else {
            mcpl_buf  = item->file->def_plist_cache.mcpl_buf;
            mcpl_size = item->file->def_plist_cache.mcpl_size;
        } /* end else */

        /* Set up operation to write MCPl and datatypes to map */
        /* Point to map */
        update_cb_ud->md_rw_cb_ud.obj = &map->obj;

        /* Point to req */
        update_cb_ud->md_rw_cb_ud.req = int_req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        update_cb_ud->md_rw_cb_ud.free_dkey = FALSE;

        /* The elements in iod and sgl */
        update_cb_ud->md_rw_cb_ud.nr = 3u;

        /* Set up iod */
        /* Key datatype.  Point akey to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[0].iod_name, H5_daos_ktype_g,
                           H5_daos_ktype_size_g);
        update_cb_ud->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_size = (uint64_t)ktype_size;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        /* Value datatype */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[1].iod_name, H5_daos_vtype_g,
                           H5_daos_vtype_size_g);
        update_cb_ud->md_rw_cb_ud.iod[1].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[1].iod_size = (uint64_t)vtype_size;
        update_cb_ud->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        /* MCPL */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[2].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[2].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[2].iod_size = (uint64_t)mcpl_size;
        update_cb_ud->md_rw_cb_ud.iod[2].iod_type = DAOS_IOD_SINGLE;

        /* Do not free global akey buffers */
        update_cb_ud->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[0], ktype_buf, (daos_size_t)ktype_size);
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[0];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[1], vtype_buf, (daos_size_t)vtype_size);
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[1];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[1]   = FALSE;
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[2], mcpl_buf, (daos_size_t)mcpl_size);
        update_cb_ud->md_rw_cb_ud.sgl[2].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[2].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[2];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[2]   = FALSE;

        /* Set task name */
        update_cb_ud->md_rw_cb_ud.task_name = "map metadata write";

        /* Create task for map metadata write */
        assert(dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, 1, &dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_md_update_comp_cb, update_cb_ud, &update_task) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create task to write map metadata");

        /* Schedule map metadata write task and give it a reference to req and
         * the map */
        assert(first_task);
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to write map metadata: %s",
                         H5_daos_err_to_string(ret));
        int_req->rc++;
        map->obj.item.rc++;
        update_cb_ud = NULL;

        /* Add dependency for finalize task */
        finalize_deps[finalize_ndeps] = update_task;
        finalize_ndeps++;

        /* Create link to map */
        if (target_obj) {
            H5_daos_link_val_t link_val;

            link_val.type                 = H5L_TYPE_HARD;
            link_val.target.hard          = map->obj.oid;
            link_val.target_oid_async     = &map->obj.oid;
            finalize_deps[finalize_ndeps] = dep_task;
            if (0 !=
                (ret = H5_daos_link_write((H5_daos_group_t *)target_obj, target_name, target_name_len,
                                          &link_val, int_req, &first_task, &finalize_deps[finalize_ndeps])))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create link to map: %s",
                             H5_daos_err_to_string(ret));
            finalize_ndeps++;
        } /* end if */
        else {
            /* No link to map, write a ref count of 0 */
            finalize_deps[finalize_ndeps] = dep_task;
            if (0 != (ret = H5_daos_obj_write_rc(NULL, &map->obj, NULL, 0, int_req, &first_task,
                                                 &finalize_deps[finalize_ndeps])))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't write object ref count: %s",
                             H5_daos_err_to_string(ret));
            finalize_ndeps++;
        } /* end if */
    }     /* end if */

    /* Finish setting up map struct */
    if ((map->key_type_id = H5Tcopy(ktype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy datatype");
    if ((map->key_file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, ktype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to get file datatype");
    if (0 == (map->key_file_type_size = H5Tget_size(map->key_file_type_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, NULL, "can't get key file datatype size");
    if ((map->val_type_id = H5Tcopy(vtype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy datatype");
    if ((map->val_file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, vtype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to get file datatype");
    if (0 == (map->val_file_type_size = H5Tget_size(map->val_file_type_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, NULL, "can't get value file datatype size");
    if (!default_mcpl && (map->mcpl_id = H5Pcopy(mcpl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy mcpl");
    if ((mapl_id != H5P_MAP_ACCESS_DEFAULT) && (map->mapl_id = H5Pcopy(mapl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy mapl");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&map->obj, map->mcpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to fill OCPL cache");

    /* Set return value */
    ret_value = (void *)map;

done:
    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close object");

    /* Close key type parent type */
    if (ktype_parent_id >= 0 && H5Tclose(ktype_parent_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close key type parent type");

    if (int_req) {
        H5_daos_op_pool_type_t op_type;

        /* Perform collective error check if appropriate */
        if (collective && (item->file->num_procs > 1)) {
            if (finalize_ndeps == 0) {
                /* Only dep_task created, register it as the finalize dependency
                 */
                if (dep_task) {
                    finalize_deps[0] = dep_task;
                    finalize_ndeps   = 1;
                } /* end if */
            }     /* end if */
            else if (finalize_ndeps > 1) {
                tse_task_t *metatask = NULL;

                /* Create metatask for coordination */
                if (H5_daos_create_task(
                        H5_daos_metatask_autocomplete, (finalize_ndeps > 0) ? (unsigned)finalize_ndeps : 0,
                        (finalize_ndeps > 0) ? finalize_deps : NULL, NULL, NULL, NULL, &metatask) < 0)
                    D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create metatask for map create");
                /* Schedule metatask */
                else if (0 != (ret = tse_task_schedule(metatask, false)))
                    D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule metatask for map create: %s",
                                 H5_daos_err_to_string(ret));
                else {
                    finalize_ndeps   = 1;
                    finalize_deps[0] = metatask;
                } /* end if */
            }     /* end if */

            /* At this point we have 0 or 1 finalize deps */
            if (H5_daos_collective_error_check(&map->obj, int_req, &first_task, &finalize_deps[0]) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't perform collective error check");
        } /* end if */

        assert(finalize_ndeps > 0 || !dep_task);

        /* Free path_buf if necessary */
        if (path_buf) {
            assert(finalize_ndeps > 0);
            if (H5_daos_free_async(path_buf, &first_task, &finalize_deps[0]) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTFREE, NULL, "can't free path buffer");
        } /* end if */

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, (finalize_ndeps > 0) ? (unsigned)finalize_ndeps : 0,
                                (finalize_ndeps > 0) ? finalize_deps : NULL, NULL, NULL, int_req,
                                &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
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
         * need to add to map's pool since it's the open request. */
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
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, NULL, "map creation failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value) {
        /* Close map */
        if (map && H5_daos_map_close_real(map) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map");

        /* Free memory */
        if (update_cb_ud && update_cb_ud->md_rw_cb_ud.obj &&
            H5_daos_object_close(&update_cb_ud->md_rw_cb_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close object");
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    assert(!update_cb_ud);

    D_FUNC_LEAVE;
} /* end H5_daos_map_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_open
 *
 * Purpose:     Sends a request to DAOS to open a map
 *
 * Return:      Success:        map object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_map_open(void *_item, const H5VL_loc_params_t *loc_params, const char *name, hid_t mapl_id,
                 hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item       = (H5_daos_item_t *)_item;
    H5_daos_map_t  *map        = NULL;
    H5_daos_req_t  *int_req    = NULL;
    tse_task_t     *first_task = NULL;
    tse_task_t     *dep_task   = NULL;
    hbool_t         collective;
    int             ret;
    void           *ret_value = NULL;

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    H5_DAOS_GET_METADATA_READ_MODE(item->file, mapl_id, H5P_MAP_ACCESS_DEFAULT, collective, H5E_MAP, NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "map open", item->open_req, NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Call internal open routine */
    if (NULL == (map = H5_daos_map_open_int(item, loc_params, name, mapl_id, int_req, collective, &first_task,
                                            &dep_task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map");

    /* Set return value */
    ret_value = (void *)map;

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
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
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, NULL, "map open failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* If we are not returning a map we must close it */
    if (ret_value == NULL && map && H5_daos_map_close_real(map) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map");

    D_FUNC_LEAVE;
} /* end H5_daos_map_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_open_helper
 *
 * Purpose:     Internal-use helper routine to create an asynchronous task
 *              for opening a DAOS HDF5 map. It is the responsibility
 *              of the calling function to make sure that the map's oid
 *              field is filled in before scheduled tasks are allowed to
 *              run.
 *
 * Return:      Success:        map object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5_daos_map_t *
H5_daos_map_open_helper(H5_daos_file_t *file, hid_t mapl_id, hbool_t collective, H5_daos_req_t *req,
                        tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_flex_t *bcast_udata    = NULL;
    H5_daos_omd_fetch_ud_t       *fetch_udata    = NULL;
    H5_daos_map_t                *map            = NULL;
    size_t                        minfo_buf_size = 0;
    int                           ret;
    H5_daos_map_t                *ret_value = NULL;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    /* Allocate the map object that is returned to the user */
    if (NULL == (map = H5FL_CALLOC(H5_daos_map_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS map struct");
    map->obj.item.type     = H5I_MAP;
    map->obj.item.open_req = req;
    req->rc++;
    map->obj.item.file    = file;
    map->obj.item.rc      = 1;
    map->obj.obj_oh       = DAOS_HDL_INVAL;
    map->key_type_id      = H5I_INVALID_HID;
    map->key_file_type_id = H5I_INVALID_HID;
    map->val_type_id      = H5I_INVALID_HID;
    map->val_file_type_id = H5I_INVALID_HID;
    map->mcpl_id          = H5P_MAP_CREATE_DEFAULT;
    map->mapl_id          = H5P_MAP_ACCESS_DEFAULT;
    if ((mapl_id != H5P_MAP_ACCESS_DEFAULT) && (map->mapl_id = H5Pcopy(mapl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy mapl");

    /* Set up broadcast user data (if appropriate) and calculate initial map
     * info buffer size */
    if (collective && (file->num_procs > 1)) {
        if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_flex_t *)DV_malloc(
                         sizeof(H5_daos_mpi_ibcast_ud_flex_t) + H5_DAOS_MINFO_BCAST_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->bcast_udata.req        = req;
        bcast_udata->bcast_udata.obj        = &map->obj;
        bcast_udata->bcast_udata.buffer     = bcast_udata->flex_buf;
        bcast_udata->bcast_udata.buffer_len = H5_DAOS_MINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_udata.count      = H5_DAOS_MINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_udata.comm       = req->file->comm;

        minfo_buf_size = H5_DAOS_MINFO_BCAST_BUF_SIZE;
    } /* end if */
    else
        minfo_buf_size = (2 * H5_DAOS_TYPE_BUF_SIZE) + H5_DAOS_MCPL_BUF_SIZE;

    /* Check if we're actually opening the map or just receiving the map
     * info from the leader */
    if (!collective || (file->my_rank == 0)) {
        tse_task_t *fetch_task = NULL;
        uint8_t    *p;

        /* Open map object */
        if (H5_daos_obj_open(file, req, &map->obj.oid, (file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO),
                             &map->obj.obj_oh, "map object open", first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map object");

        /* Allocate argument struct for fetch task */
        if (NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(sizeof(H5_daos_omd_fetch_ud_t) +
                                                                       (bcast_udata ? 0 : minfo_buf_size))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for fetch callback arguments");

        /* Set up operation to read key/memory datatypes and MCPL sizes from map */

        /* Set up ud struct */
        fetch_udata->md_rw_cb_ud.req = req;
        fetch_udata->md_rw_cb_ud.obj = &map->obj;
        fetch_udata->bcast_udata     = bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_ktype_g,
                           H5_daos_ktype_size_g);
        fetch_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        fetch_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[1].iod_name, H5_daos_vtype_g,
                           H5_daos_vtype_size_g);
        fetch_udata->md_rw_cb_ud.iod[1].iod_nr   = 1u;
        fetch_udata->md_rw_cb_ud.iod[1].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[2].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[2].iod_nr   = 1u;
        fetch_udata->md_rw_cb_ud.iod[2].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[2].iod_type = DAOS_IOD_SINGLE;

        fetch_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up buffer */
        if (bcast_udata)
            p = bcast_udata->flex_buf + (5 * H5_DAOS_ENCODED_UINT64_T_SIZE);
        else
            p = fetch_udata->flex_buf;

        /* Set up sgl */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], p, (daos_size_t)H5_DAOS_TYPE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[0];
        fetch_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        p += H5_DAOS_TYPE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[1], p, (daos_size_t)H5_DAOS_TYPE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[1];
        fetch_udata->md_rw_cb_ud.free_sg_iov[1]   = FALSE;
        p += H5_DAOS_TYPE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[2], p, (daos_size_t)H5_DAOS_MCPL_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[2].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[2].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[2].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[2];
        fetch_udata->md_rw_cb_ud.free_sg_iov[2]   = FALSE;
        p += H5_DAOS_MCPL_BUF_SIZE;

        /* Set conditional akey fetch for map metadata read operation */
        fetch_udata->md_rw_cb_ud.flags = DAOS_COND_AKEY_FETCH;

        /* Set nr */
        fetch_udata->md_rw_cb_ud.nr = 3u;

        /* Set task name */
        fetch_udata->md_rw_cb_ud.task_name = "map metadata read";

        /* Create meta task for map metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_minfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &fetch_udata->fetch_metatask) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create meta task for map metadata read");

        /* Create task for map metadata read */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_minfo_read_comp_cb, fetch_udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create task to read map metadata");

        /* Schedule meta task */
        if (0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule meta task for map metadata read: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule map metadata read task (or save it to be scheduled
         * later) and give it a reference to req and the map */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to read map metadata: %s",
                         H5_daos_err_to_string(ret));
        *dep_task = fetch_udata->fetch_metatask;
        req->rc++;
        map->obj.item.rc++;
        fetch_udata = NULL;
    } /* end if */
    else
        assert(bcast_udata);

    ret_value = map;

done:
    /* Broadcast map info */
    if (bcast_udata) {
        assert(minfo_buf_size == H5_DAOS_MINFO_BCAST_BUF_SIZE);
        if (H5_daos_mpi_ibcast(
                &bcast_udata->bcast_udata, &map->obj, minfo_buf_size, NULL == ret_value ? TRUE : FALSE, NULL,
                file->my_rank == 0 ? H5_daos_map_open_bcast_comp_cb : H5_daos_map_open_recv_comp_cb, req,
                first_task, dep_task) < 0) {
            DV_free(bcast_udata);
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to broadcast map info buffer");
        } /* end if */

        bcast_udata = NULL;
    } /* end if */

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close map */
        if (map && H5_daos_map_close_real(map) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map");

        /* Free memory */
        fetch_udata = DV_free(fetch_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!fetch_udata);
    assert(!bcast_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_map_open_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_open_int
 *
 * Purpose:     Internal version of H5_daos_map_open
 *
 * Return:      Success:        map object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5_daos_map_t *
H5_daos_map_open_int(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params, const char *name,
                     hid_t mapl_id, H5_daos_req_t *req, hbool_t collective, tse_task_t **first_task,
                     tse_task_t **dep_task)
{
    H5_daos_map_t  *map        = NULL;
    H5_daos_obj_t  *target_obj = NULL;
    daos_obj_id_t   oid        = {0, 0};
    daos_obj_id_t **oid_ptr    = NULL;
    char           *path_buf   = NULL;
    hbool_t         must_bcast = FALSE;
    H5_daos_map_t  *ret_value  = NULL;

    assert(item);
    assert(loc_params);
    assert(req);
    assert(req->dxpl_id >= 0);
    assert(first_task);
    assert(dep_task);

    /* Make sure H5_DAOS_g is set. */
    H5_DAOS_G_INIT(NULL);

    /* Check for open by object token */
    if (H5VL_OBJECT_BY_TOKEN == loc_params->type) {
        /* Generate oid from token */
        if (H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't convert object token to OID");
    } /* end if */
    else {
        const char *target_name = NULL;
        size_t      target_name_len;

        /* Open using name parameter */
        if (H5VL_OBJECT_BY_SELF != loc_params->type)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "unsupported map open location parameters type");
        if (!name)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map name is NULL");

        /* At this point we must broadcast on failure */
        if (collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Traverse the path */
        if (NULL == (target_obj = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, req, collective,
                                                         &path_buf, &target_name, &target_name_len,
                                                         first_task, dep_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path");

        /* Check for no target_name, in this case just return target_obj */
        if (target_name_len == 0) {
            /* Check type of target_obj */
            if (target_obj->item.type != H5I_MAP)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a map");

            /* Take ownership of target_obj */
            map        = (H5_daos_map_t *)target_obj;
            target_obj = NULL;

            /* No need to bcast since everyone just opened the already open
             * map */
            must_bcast = FALSE;
        } /* end if */
        else if (!collective || (item->file->my_rank == 0)) {
            /* Check type of target_obj */
            if (target_obj->item.type != H5I_GROUP)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

            /* Follow link to group */
            if (H5_daos_link_follow((H5_daos_group_t *)target_obj, target_name, target_name_len, FALSE, req,
                                    &oid_ptr, NULL, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_TRAVERSE, NULL, "can't follow link to map");
        } /* end else */
    }     /* end else */

    /* Open map if not already open */
    if (!map) {
        must_bcast = FALSE; /* Helper function will handle bcast */
        if (NULL ==
            (map = H5_daos_map_open_helper(item->file, mapl_id, collective, req, first_task, dep_task)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map");

        /* Set map oid */
        if (oid_ptr)
            /* Retarget *oid_ptr to map->obj.oid so H5_daos_link_follow fills in
             * the map's oid */
            *oid_ptr = &map->obj.oid;
        else if (H5VL_OBJECT_BY_TOKEN == loc_params->type)
            /* Just set the static oid from the token */
            map->obj.oid = oid;
        else
            /* We will receive oid from lead process */
            assert(collective && item->file->my_rank > 0);
    } /* end if */

    /* Set return value */
    ret_value = map;

done:
    /* Free path_buf if necessary */
    if (path_buf && H5_daos_free_async(path_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CANTFREE, NULL, "can't free path buffer");

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close object");

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Broadcast failure */
        if (must_bcast && H5_daos_mpi_ibcast(NULL, &map->obj, H5_DAOS_MINFO_BCAST_BUF_SIZE, TRUE, NULL,
                                             item->file->my_rank == 0 ? H5_daos_map_open_bcast_comp_cb
                                                                      : H5_daos_map_open_recv_comp_cb,
                                             req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL,
                         "failed to broadcast empty map info buffer to signal failure");
        must_bcast = FALSE;

        /* Close map to prevent memory leaks since we're not returning it */
        if (map && H5_daos_map_close_real(map) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map");
    } /* end if */

    assert(!must_bcast);

    D_FUNC_LEAVE;
} /* end H5_daos_map_open_int() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_open_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for map
 *              opens (rank 0).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_open_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for map info broadcast task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast map info";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank == 0);
        assert(udata->bcast_udata.obj->item.type == H5I_MAP);

        /* Reissue bcast if necessary */
        if (udata->bcast_udata.buffer_len != udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.count == H5_DAOS_MINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.buffer_len > H5_DAOS_MINFO_BCAST_BUF_SIZE);

            /* Use full buffer this time */
            udata->bcast_udata.count = udata->bcast_udata.buffer_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_map_open_bcast_comp_cb,
                                    udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second map info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret,
                             "can't schedule task for second map info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
    }     /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close map */
        if (udata->bcast_udata.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast map info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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

    D_FUNC_LEAVE;
} /* end H5_daos_map_open_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_open_recv_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for map
 *              opens (rank 1+).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_open_recv_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for map info receive task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast map info";
    } /* end if */
    else if (task->dt_result == 0) {
        uint64_t ktype_buf_len = 0;
        uint64_t vtype_buf_len = 0;
        uint64_t mcpl_buf_len  = 0;
        size_t   minfo_len;
        uint8_t *p = udata->bcast_udata.buffer;

        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank > 0);
        assert(udata->bcast_udata.obj->item.type == H5I_MAP);

        /* Decode oid */
        UINT64DECODE(p, udata->bcast_udata.obj->oid.lo)
        UINT64DECODE(p, udata->bcast_udata.obj->oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, ktype_buf_len)
        UINT64DECODE(p, vtype_buf_len)
        UINT64DECODE(p, mcpl_buf_len)

        /* Check for ktype_buf_len set to 0 - indicates failure */
        if (ktype_buf_len == 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR, "lead process failed to open map");

        /* Calculate data length */
        minfo_len = (size_t)ktype_buf_len + (size_t)vtype_buf_len + (size_t)mcpl_buf_len +
                    H5_DAOS_ENCODED_OID_SIZE + 3 * sizeof(uint64_t);

        /* Reissue bcast if necessary */
        if (minfo_len > (size_t)udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.buffer_len == H5_DAOS_MINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.count == H5_DAOS_MINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.buffer == udata->flex_buf);

            /* Realloc buffer */
            if (NULL == (udata->bcast_udata.buffer = DV_malloc(minfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "failed to allocate memory for map info buffer");
            udata->bcast_udata.buffer_len = (int)minfo_len;
            udata->bcast_udata.count      = (int)minfo_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_map_open_recv_comp_cb,
                                    udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second map info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret,
                             "can't schedule task for second map info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
        else {
            /* Open map */
            if (0 != (ret = daos_obj_open(
                          udata->bcast_udata.obj->item.file->coh, udata->bcast_udata.obj->oid,
                          (udata->bcast_udata.obj->item.file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO),
                          &udata->bcast_udata.obj->obj_oh, NULL /*event*/)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, ret, "can't open map: %s", H5_daos_err_to_string(ret));

            /* Finish building map object */
            if (0 !=
                (ret = H5_daos_map_open_end((H5_daos_map_t *)udata->bcast_udata.obj, p, ktype_buf_len,
                                            vtype_buf_len, mcpl_buf_len, udata->bcast_udata.req->dxpl_id)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't finish opening map");
        } /* end else */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close map */
        if (udata->bcast_udata.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast map info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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

    D_FUNC_LEAVE;
} /* end H5_daos_map_open_recv_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_open_end
 *
 * Purpose:     Decode serialized map info from a buffer and fill caches.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_open_end(H5_daos_map_t *map, uint8_t *p, uint64_t ktype_buf_len, uint64_t vtype_buf_len,
                     uint64_t mcpl_buf_len, hid_t H5VL_DAOS_UNUSED dxpl_id)
{
    H5T_class_t ktype_class;
    htri_t      has_vl_vlstr_ref;
    hid_t       ktype_parent_id = H5I_INVALID_HID;
    int         ret_value       = 0;

    assert(map);
    assert(p);
    assert(ktype_buf_len > 0);
    assert(vtype_buf_len > 0);

    /* Decode datatypes */
    if ((map->key_type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize datatype");
    p += ktype_buf_len;
    if ((map->val_type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize datatype");
    p += vtype_buf_len;

    /* Check if the map's MCPL is the default MCPL.
     * Otherwise, decode the map's MCPL.
     */
    if ((mcpl_buf_len == map->obj.item.file->def_plist_cache.mcpl_size) &&
        !memcmp(p, map->obj.item.file->def_plist_cache.mcpl_buf,
                map->obj.item.file->def_plist_cache.mcpl_size))
        map->mcpl_id = H5P_MAP_CREATE_DEFAULT;
    else if ((map->mcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR,
                     "can't deserialize map creation property list");

    /* Check validity of key type.  Vlens are only allowed at the top level, no
     * references allowed at all. */
    if (H5T_NO_CLASS == (ktype_class = H5Tget_class(map->key_type_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get key type class");
    if (ktype_class == H5T_VLEN) {
        /* Vlen types must not contain any nested vlens */
        if ((ktype_parent_id = H5Tget_super(map->key_type_id)) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get key type parent");
        if ((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(ktype_parent_id)) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_H5_GET_ERROR,
                         "can't check for vlen or reference type");
        if (has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR,
                         "key type contains nested vlen or reference");
    } /* end if */
    else if (ktype_class == H5T_REFERENCE)
        /* References not supported */
        D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR,
                     "key type is a reference type");
    else if (ktype_class != H5T_STRING) {
        /* No nested vlens */
        if ((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(map->key_type_id)) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_H5_GET_ERROR,
                         "can't check for vlen or reference type");
        if (has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, -H5_DAOS_H5_UNSUPPORTED_ERROR,
                         "key type contains nested vlen or reference");
    } /* end if */

    /* Finish setting up map struct */
    if ((map->key_file_type_id = H5VLget_file_type(map->obj.item.file, H5_DAOS_g, map->key_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_H5_TCONV_ERROR, "failed to get file datatype");
    if ((map->val_file_type_id = H5VLget_file_type(map->obj.item.file, H5_DAOS_g, map->val_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_H5_TCONV_ERROR, "failed to get file datatype");
    if (0 == (map->key_file_type_size = H5Tget_size(map->key_file_type_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get key file datatype size");
    if (0 == (map->val_file_type_size = H5Tget_size(map->val_file_type_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, -H5_DAOS_H5_GET_ERROR, "can't get value file datatype size");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&map->obj, map->mcpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache");

done:
    /* Close key type parent type */
    if (ktype_parent_id >= 0 && H5Tclose(ktype_parent_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close key type parent type");

    D_FUNC_LEAVE;
} /* end H5_daos_map_open_end() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_minfo_read_comp_cb
 *
 * Purpose:     Complete callback for asynchronous metadata fetch for
 *              map opens.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_minfo_read_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_omd_fetch_ud_t *udata;
    uint8_t                *p;
    int                     ret;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for map info read task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->fetch_metatask);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;
        size_t      daos_info_len = udata->md_rw_cb_ud.iod[0].iod_size + udata->md_rw_cb_ud.iod[1].iod_size +
                               udata->md_rw_cb_ud.iod[2].iod_size;

        assert(udata->md_rw_cb_ud.req->file);
        assert(udata->md_rw_cb_ud.obj);
        assert(udata->md_rw_cb_ud.obj->item.type == H5I_MAP);

        /* Verify iod size makes sense */
        if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_TYPE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[1].iov_buf_len != H5_DAOS_TYPE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[2].iov_buf_len != H5_DAOS_MCPL_BUF_SIZE)
            D_GOTO_ERROR(H5E_MAP, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "buffer length does not match expected value");

        if (udata->bcast_udata) {
            assert(udata->bcast_udata->bcast_udata.buffer == udata->bcast_udata->flex_buf);

            /* Reallocate map info buffer if necessary */
            if (daos_info_len > (2 * H5_DAOS_TYPE_BUF_SIZE) + H5_DAOS_MCPL_BUF_SIZE) {
                if (NULL ==
                    (udata->bcast_udata->bcast_udata.buffer = DV_malloc(
                         daos_info_len + H5_DAOS_ENCODED_OID_SIZE + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized map info");
                udata->bcast_udata->bcast_udata.buffer_len =
                    (int)(daos_info_len + H5_DAOS_ENCODED_OID_SIZE + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE);
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->bcast_udata->bcast_udata.buffer + H5_DAOS_ENCODED_OID_SIZE +
                3 * H5_DAOS_ENCODED_UINT64_T_SIZE;
        } /* end if */
        else {
            assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf == udata->flex_buf);

            /* Reallocate map info buffer if necessary */
            if (daos_info_len > (2 * H5_DAOS_TYPE_BUF_SIZE) + H5_DAOS_MCPL_BUF_SIZE) {
                if (NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(daos_info_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized map info");
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

        /* Create task for reissued map metadata read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                     H5_daos_minfo_read_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to read map metadata");

        /* Schedule map metadata read task and give it a reference to req
         * and the map */
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't schedule task to read map metadata: %s",
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
            uint64_t ktype_buf_len = (uint64_t)((char *)udata->md_rw_cb_ud.sg_iov[1].iov_buf -
                                                (char *)udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            uint64_t vtype_buf_len = (uint64_t)((char *)udata->md_rw_cb_ud.sg_iov[2].iov_buf -
                                                (char *)udata->md_rw_cb_ud.sg_iov[1].iov_buf);
            uint64_t mcpl_buf_len  = (uint64_t)(udata->md_rw_cb_ud.iod[2].iod_size);

            assert(udata->md_rw_cb_ud.req->file);
            assert(udata->md_rw_cb_ud.obj);
            assert(udata->md_rw_cb_ud.obj->item.type == H5I_MAP);

            /* Check for missing metadata */
            if (udata->md_rw_cb_ud.iod[0].iod_size == 0 || udata->md_rw_cb_ud.iod[1].iod_size == 0 ||
                udata->md_rw_cb_ud.iod[2].iod_size == 0)
                D_GOTO_ERROR(H5E_MAP, H5E_NOTFOUND, -H5_DAOS_DAOS_GET_ERROR, "internal metadata not found");

            if (udata->bcast_udata) {
                /* Encode oid */
                p = udata->bcast_udata->bcast_udata.buffer;
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.lo)
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.hi)

                /* Encode serialized info lengths */
                UINT64ENCODE(p, ktype_buf_len)
                UINT64ENCODE(p, vtype_buf_len)
                UINT64ENCODE(p, mcpl_buf_len)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Finish building map object */
            if (0 != (ret = H5_daos_map_open_end(
                          (H5_daos_map_t *)udata->md_rw_cb_ud.obj, udata->md_rw_cb_ud.sg_iov[0].iov_buf,
                          ktype_buf_len, vtype_buf_len, mcpl_buf_len, udata->md_rw_cb_ud.req->dxpl_id)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't finish opening map");
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up if this is the last fetch task */
    if (udata) {
        /* Close map */
        if (udata->md_rw_cb_ud.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

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
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->fetch_metatask) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

        /* Complete fetch metatask */
        tse_task_complete(udata->fetch_metatask, ret_value);

        assert(!udata->md_rw_cb_ud.free_dkey);
        assert(!udata->md_rw_cb_ud.free_akeys);

        /* Free udata */
        DV_free(udata);
    } /* end if */
    else
        assert(ret_value == 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_minfo_read_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_key_conv
 *
 * Purpose:     Converts the provided key from the source to destination
 *              type.  Handles vlens differently from other conversions in
 *              this connector, flattening them into a buffer.  Does not
 *              support nested vlens.
 *
 * Parameters:  hid_t src_type_id: IN: Type ID that describes the data
 *                  currently in key.
 *              hid_t dst_type_id: IN: Type ID to convert data in key to.
 *              const void *key: IN: Buffer containing the key to be
 *                  converted.
 *              const void **key_buf: OUT: A pointer to a buffer
 *                  containing the converted key will be placed here.
 *              size_t *key_size: OUT: The size in bytes of the data
 *                  pointed to by *key_buf will be placed here.
 *              void **key_buf_alloc: OUT: A pointer to a buffer allocated
 *                  by this function to hold the key, if any, will be
 *                  placed here.  This must be freed by the caller when
 *                  key_buf is no longer needed.
 *              hid_t dxpl_id: IN: Dataset transfer property list ID.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_key_conv(hid_t src_type_id, hid_t dst_type_id, const void *key, const void **key_buf,
                     size_t *key_size, void **key_buf_alloc, hid_t dxpl_id)
{
    htri_t  need_tconv;
    size_t  src_type_size;
    size_t  dst_type_size;
    hid_t   src_parent_type_id = H5I_INVALID_HID;
    hid_t   dst_parent_type_id = H5I_INVALID_HID;
    void   *tconv_buf          = NULL;
    void   *bkg_buf            = NULL;
    hbool_t fill_bkg           = FALSE;
    herr_t  ret_value          = SUCCEED;

    assert(src_type_id >= 0);
    assert(dst_type_id >= 0);
    assert(key);
    assert(key_buf);
    assert(key_size);
    assert(key_buf_alloc);
    assert(!*key_buf_alloc);

    /* Check if type conversion is needed for the key */
    if ((need_tconv = H5_daos_need_tconv(src_type_id, dst_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
    if (need_tconv) {
        H5T_class_t type_class;

        /* Get class */
        if (H5T_NO_CLASS == (type_class = H5Tget_class(src_type_id)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get datatype class");

        /* Check for vlen */
        if (type_class == H5T_VLEN) {
            size_t       src_parent_type_size;
            size_t       dst_parent_type_size;
            const hvl_t *vl = (const hvl_t *)key;
            htri_t       parent_need_tconv;

            /* Check for empty key - currently unsupported */
            if (vl->len == 0)
                D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, FAIL, "vl key cannot have length of 0");

            /* Get parent types */
            if ((src_parent_type_id = H5Tget_super(src_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get source type parent");
            if ((dst_parent_type_id = H5Tget_super(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get destination type parent");

            /* Check if type conversion is needed for the parent type */
            if ((parent_need_tconv = H5_daos_need_tconv(src_parent_type_id, dst_parent_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
            if (parent_need_tconv) {
                /* Initialize type conversion */
                if (H5_daos_tconv_init(src_parent_type_id, &src_parent_type_size, dst_parent_type_id,
                                       &dst_parent_type_size, vl->len, FALSE, TRUE, &tconv_buf, &bkg_buf,
                                       NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion");

                /* If needed, fill the background buffer (with zeros) */
                if (fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, vl->len * dst_parent_type_size);
                } /* end if */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, vl->p, vl->len * src_parent_type_size);

                /* Perform type conversion */
                if (H5Tconvert(src_parent_type_id, dst_parent_type_id, vl->len, tconv_buf, bkg_buf, dxpl_id) <
                    0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion");

                /* Set return values to point to converted buffer */
                *key_buf       = (const void *)tconv_buf;
                *key_size      = vl->len * dst_parent_type_size;
                *key_buf_alloc = tconv_buf;
                tconv_buf      = NULL;
            }
            else {
                /* Just get parent size and set return values to point to vlen
                 * buffer */
                if (0 == (dst_parent_type_size = H5Tget_size(dst_parent_type_id)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype");
                *key_buf  = vl->p;
                *key_size = vl->len * dst_parent_type_size;
            } /* end else */
        }     /* end if */
        else {
            htri_t is_vl_str = FALSE;

            /* Check for VL string */
            if (type_class == H5T_STRING)
                if ((is_vl_str = H5Tis_variable_str(src_type_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't check for variable length string");
            if (is_vl_str) {
                /* Set return values to point to string (exclude null terminator
                 * since it's not needed */
                *key_buf = (const void *)*((const char *const *)key);
                if (*key_buf) {
                    *key_size = strlen(*(const char *const *)key);

                    /* If the key is '\0' (null string), write the null
                     * terminator (to distinguish from NULL pointer) */
                    if (*key_size == 0)
                        *key_size = 1;
                } /* end if */
                else {
                    /* If NULL was passed as the key, set the key to be the
                     * magic value of {'\0', '\0'} */
                    if (NULL == (*key_buf_alloc = DV_calloc(2)))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for NULL key");
                    *key_buf  = (const void *)*key_buf_alloc;
                    *key_size = 2;
                } /* end else */
            }     /* end if */
            else {
                /* Initialize type conversion */
                if (H5_daos_tconv_init(src_type_id, &src_type_size, dst_type_id, &dst_type_size, 1, FALSE,
                                       TRUE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion");

                /* If needed, fill the background buffer (with zeros) */
                if (fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, dst_type_size);
                } /* end if */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, key, src_type_size);

                /* Perform type conversion */
                if (H5Tconvert(src_type_id, dst_type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion");

                /* Set return values to point to converted buffer */
                *key_buf       = (const void *)tconv_buf;
                *key_size      = dst_type_size;
                *key_buf_alloc = tconv_buf;
                tconv_buf      = NULL;
            } /* end else */
        }     /* end else */
    }         /* end if */
    else {
        /* Just get size and return key */
        if (0 == (dst_type_size = H5Tget_size(dst_type_id)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype");
        *key_buf  = key;
        *key_size = dst_type_size;
    } /* end else */

done:
    /* Cleanup */
    if (src_parent_type_id > 0 && H5Tclose(src_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close source type parent type");
    if (dst_parent_type_id > 0 && H5Tclose(dst_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close destination type parent type");
    tconv_buf = DV_free(tconv_buf);
    bkg_buf   = DV_free(bkg_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_map_key_conv() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_key_conv_reverse
 *
 * Purpose:     Converts the provided key from the source to destination
 *              type.  Handles vlens differently from other conversions in
 *              this connector, un-flattening them into buffers and hvl_t
 *              / char * pointers.  Does not support nested vlens.
 *
 * Parameters:  hid_t src_type_id: IN: Type ID that describes the data
 *                  currently in key.
 *              hid_t dst_type_id: IN: Type ID to convert data in key to.
 *              void *key: IN: Buffer containing the key to be converted.
 *                  This function may convert it in place or otherwise
 *                  change the contents of the buffer!
 *              size_t key_size: IN: Size in bytes of the data pointed to
 *                  by key.
 *              void **key_buf: OUT: A pointer to a buffer containing the
 *                  converted key will be placed here.
 *              void **key_buf_alloc: IN/OUT: On entry, optionally
 *                  contains a buffer of size *key_size.  On exit, will
 *                  contain either this buffer or another one that must
 *                  eventually be freed by the caller when key_buf is no
 *                  longer needed.
 *              H5_daos_vl_union_t *vl_union: IN: A pointer to a buffer
 *                  large enough to hold an H5_daos_vl_union_t.  Must not
 *                  be freed or go out of scope until key_buf is no longer
 *                  needed.
 *              hid_t dxpl_id: IN: Dataset transfer property list ID.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_key_conv_reverse(hid_t src_type_id, hid_t dst_type_id, void *key, size_t key_size, void **key_buf,
                             void **key_buf_alloc, H5_daos_vl_union_t *vl_union, hid_t dxpl_id)
{
    htri_t  need_tconv;
    size_t  src_type_size;
    size_t  dst_type_size;
    hid_t   src_parent_type_id = H5I_INVALID_HID;
    hid_t   dst_parent_type_id = H5I_INVALID_HID;
    void   *tconv_buf          = NULL;
    void   *bkg_buf            = NULL;
    hbool_t fill_bkg           = FALSE;
    herr_t  ret_value          = SUCCEED;

    assert(src_type_id >= 0);
    assert(dst_type_id >= 0);
    assert(key);
    assert(key_size > 0);
    assert(key_buf);
    assert(key_buf_alloc);
    assert(vl_union);

    /* Check if type conversion is needed for the key */
    if ((need_tconv = H5_daos_need_tconv(src_type_id, dst_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
    if (need_tconv) {
        H5T_class_t type_class;

        /* Get class */
        if (H5T_NO_CLASS == (type_class = H5Tget_class(src_type_id)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get datatype class");

        /* Check for vlen */
        if (type_class == H5T_VLEN) {
            size_t src_parent_type_size;
            size_t dst_parent_type_size;
            htri_t parent_need_tconv;

            /* Get parent types */
            if ((src_parent_type_id = H5Tget_super(src_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get source type parent");
            if ((dst_parent_type_id = H5Tget_super(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get destination type parent");

            /* Check if type conversion is needed for the parent type */
            if ((parent_need_tconv = H5_daos_need_tconv(src_parent_type_id, dst_parent_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");
            if (parent_need_tconv) {
                /* Get source parent type size */
                if (0 == (src_parent_type_size = H5Tget_size(src_parent_type_id)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype");

                /* Calculate sequence length */
                if (key_size % src_parent_type_size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                 "key size is not a multiple of source datatype size");
                vl_union->vl.len = key_size / src_parent_type_size;

                /* Initialize type conversion */
                if (H5_daos_tconv_init(src_parent_type_id, &src_parent_type_size, dst_parent_type_id,
                                       &dst_parent_type_size, vl_union->vl.len, FALSE, FALSE, &tconv_buf,
                                       &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion");

                /* Note we could reuse buffers here if we change around some of
                 * the logic in H5_daos_tconv_init() to support being passed the
                 * source buffer instead of the destination buffer */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, key, key_size);

                /* If needed, fill the background buffer (with zeros) */
                if (fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, vl_union->vl.len * dst_parent_type_size);
                } /* end if */

                /* Perform type conversion */
                if (H5Tconvert(src_parent_type_id, dst_parent_type_id, vl_union->vl.len, tconv_buf, bkg_buf,
                               dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion");

                /* Set return values to point to converted buffer */
                vl_union->vl.p = tconv_buf;
                *key_buf       = (void *)&(vl_union->vl);
                /*if(tconv_buf != key) {*/
                *key_buf_alloc = tconv_buf;
                tconv_buf      = NULL;
                /*}*/ /* end if */
            }
            else {
                /* Just get parent size and set return values to point to vlen
                 * buffer */
                if (0 == (dst_parent_type_size = H5Tget_size(dst_parent_type_id)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype");

                /* Calculate sequence length */
                if (key_size % dst_parent_type_size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                 "key size is not a multiple of source datatype size");
                vl_union->vl.len = key_size / dst_parent_type_size;
                vl_union->vl.p   = key;
                *key_buf         = (void *)&(vl_union->vl);
            } /* end else */
        }     /* end if */
        else {
            htri_t is_vl_str = FALSE;

            /* Check for VL string */
            if (type_class == H5T_STRING)
                if ((is_vl_str = H5Tis_variable_str(src_type_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't check for variable length string");
            if (is_vl_str) {
                /* Check for magic value indicating key was passed as a NULL
                 * pointer */
                if (((char *)key)[0] == '\0' && key_size == 2)
                    vl_union->vls = NULL;
                else {
                    /* Assign pointer and make sure it's NULL terminated */
                    vl_union->vls = (char *)key;
                    assert(vl_union->vls[key_size] == '\0');
                } /* end if */

                /* Set return value to point to string  */
                *key_buf = (void *)&(vl_union->vls);
            } /* end if */
            else {
                /* Initialize type conversion */
                if (H5_daos_tconv_init(src_type_id, &src_type_size, dst_type_id, &dst_type_size, 1, FALSE,
                                       FALSE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion");

                /* Check size is correct */
                if (key_size != src_type_size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                 "key size does not match source datatype size");

                /* Note we could reuse buffers here if we change around some of
                 * the logic in H5_daos_tconv_init() to support being passed the
                 * source buffer instead of the destination buffer */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, key, src_type_size);

                /* If needed, fill the background buffer (with zeros) */
                if (fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, dst_type_size);
                } /* end if */

                /* Perform type conversion */
                if (H5Tconvert(src_type_id, dst_type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion");

                /* Set return values to point to converted buffer */
                *key_buf = tconv_buf;
                /*if(tconv_buf != key) {*/
                *key_buf_alloc = tconv_buf;
                tconv_buf      = NULL;
                /*}*/ /* end if */
            }         /* end else */
        }             /* end else */
    }                 /* end if */
    else
        /* Just return key */
        *key_buf = key;

done:
    /* Cleanup */
    if (src_parent_type_id > 0 && H5Tclose(src_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close source type parent type");
    if (dst_parent_type_id > 0 && H5Tclose(dst_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close destination type parent type");
    if (tconv_buf && (tconv_buf != key))
        DV_free(tconv_buf);
    if (bkg_buf && (bkg_buf != key))
        DV_free(bkg_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_map_key_conv_reverse() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_get_val
 *
 * Purpose:     Retrieves, from the Map specified by map_id, the value
 *              associated with the provided key.  key_mem_type_id and
 *              val_mem_type_id specify the datatypes for the provided key
 *              and value buffers. If key_mem_type_id is different from
 *              that used to create the Map object the key will be
 *              internally converted to the datatype for the map object
 *              for the query, and if val_mem_type_id is different from
 *              that used to create the Map object the returned value will
 *              be converted to val_mem_type_id before the function
 *              returns. Any further options can be specified through the
 *              property list dxpl_id.
 *
 * Return:      Success:        0
 *              Failure:        -1, value not retrieved.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_map_get_val(void *_map, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, void *value,
                    hid_t dxpl_id, void **req)
{
    H5_daos_map_rw_ud_t  *get_val_udata = NULL;
    H5_daos_tconv_reuse_t reuse         = H5_DAOS_TCONV_REUSE_NONE;
    H5_daos_map_t        *map           = (H5_daos_map_t *)_map;
    H5_daos_req_t        *int_req       = NULL;
    tse_task_t           *first_task    = NULL;
    tse_task_t           *dep_task      = NULL;
    tse_task_t           *get_val_task  = NULL;
    hbool_t               fill_bkg      = FALSE;
    int                   ret;
    herr_t                ret_value = SUCCEED;

    if (!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL");
    if (!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL");
    if (!value)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map value is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(map->obj.item.file, "map get value", map->obj.item.open_req,
                                              NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Wait for the map to open if necessary */
    if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
        if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        if (map->obj.item.open_req->status != 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "map open failed");
    } /* end if */

    /* Allocate argument struct for key value retrieval task */
    if (NULL == (get_val_udata = (H5_daos_map_rw_ud_t *)DV_calloc(sizeof(H5_daos_map_rw_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for map key value retrieval task arguments");
    get_val_udata->md_rw_cb_ud.req = int_req;
    get_val_udata->md_rw_cb_ud.obj = &map->obj;
    get_val_udata->val_mem_type_id = val_mem_type_id;
    get_val_udata->value_buf       = value;

    /* Convert key (if necessary) */
    if (H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &get_val_udata->key_buf,
                             &get_val_udata->key_size, &get_val_udata->key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key");

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&get_val_udata->md_rw_cb_ud.dkey, get_val_udata->key_buf,
                       (daos_size_t)get_val_udata->key_size);
    get_val_udata->md_rw_cb_ud.free_dkey = FALSE;

    /* Check if the type conversion is needed */
    if ((get_val_udata->val_need_tconv = H5_daos_need_tconv(map->val_file_type_id, val_mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");

    /* Type conversion */
    if (get_val_udata->val_need_tconv) {
        /* Initialize type conversion */
        if (H5_daos_tconv_init(map->val_file_type_id, &get_val_udata->val_file_type_size, val_mem_type_id,
                               &get_val_udata->val_mem_type_size, 1, FALSE, FALSE, &get_val_udata->tconv_buf,
                               &get_val_udata->bkg_buf, &reuse, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion");

        /* Reuse buffer as appropriate */
        if (reuse == H5_DAOS_TCONV_REUSE_TCONV)
            get_val_udata->tconv_buf = value;
        else if (reuse == H5_DAOS_TCONV_REUSE_BKG)
            get_val_udata->bkg_buf = value;

        /* Fill background buffer if necessary */
        if (fill_bkg && (get_val_udata->bkg_buf != value))
            (void)memcpy(get_val_udata->bkg_buf, value, get_val_udata->val_mem_type_size);

        /* Set up sgl_iov to point to tconv_buf */
        daos_iov_set(&get_val_udata->md_rw_cb_ud.sg_iov[0], get_val_udata->tconv_buf,
                     (daos_size_t)get_val_udata->val_file_type_size);
        get_val_udata->md_rw_cb_ud.free_sg_iov[0] = TRUE;
    } /* end if */
    else {
        get_val_udata->val_file_type_size = map->val_file_type_size;

        /* Set up sgl_iov to point to value */
        daos_iov_set(&get_val_udata->md_rw_cb_ud.sg_iov[0], value,
                     (daos_size_t)get_val_udata->val_file_type_size);
    } /* end else */

    /* Set up iod */
    memset(&get_val_udata->md_rw_cb_ud.iod[0], 0, sizeof(daos_iod_t));
    daos_const_iov_set((d_const_iov_t *)&get_val_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_map_key_g,
                       H5_daos_map_key_size_g);
    get_val_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
    get_val_udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)get_val_udata->val_file_type_size;
    get_val_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    get_val_udata->md_rw_cb_ud.free_akeys = FALSE;

    /* Set up sgl */
    get_val_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
    get_val_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
    get_val_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &get_val_udata->md_rw_cb_ud.sg_iov[0];

    /* Set nr */
    get_val_udata->md_rw_cb_ud.nr = 1u;

    /* Set task name */
    get_val_udata->md_rw_cb_ud.task_name = "map key value retrieval";

    /* Create task to read map key value */
    assert(!dep_task);
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                 H5_daos_map_get_val_comp_cb, get_val_udata, &get_val_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to read map key value");

    /* Save map key value read task to be scheduled later and give
     * it a reference to req and the map object */
    assert(!first_task);
    first_task = get_val_task;
    dep_task   = get_val_task;
    int_req->rc++;
    map->obj.item.rc++;

    get_val_udata = NULL;

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the map's request queue.  This will add the
         * dependency on the map open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &map->obj.item, H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, FAIL,
                             "map key value read operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (ret_value < 0) {
        if (get_val_udata) {
            if (get_val_udata->tconv_buf && (get_val_udata->tconv_buf != value)) {
                DV_free(get_val_udata->tconv_buf);
                get_val_udata->tconv_buf = NULL;
            }
            if (get_val_udata->bkg_buf && (get_val_udata->bkg_buf != value)) {
                DV_free(get_val_udata->bkg_buf);
                get_val_udata->bkg_buf = NULL;
            }
            get_val_udata->key_buf_alloc = DV_free(get_val_udata->key_buf_alloc);
        }
        get_val_udata = DV_free(get_val_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_map_get_val() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_get_val_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch to read
 *              the value of a specified key in a map object. Currently
 *              checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_get_val_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_rw_ud_t *udata;
    int                  ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = "map key value fetch";
    } /* end if */
    else if (task->dt_result == 0) {
        H5_daos_map_t *map = (H5_daos_map_t *)udata->md_rw_cb_ud.obj;

        assert(map);

        /* Check for missing key-value pair */
        if (udata->md_rw_cb_ud.iod[0].iod_size == (uint64_t)0)
            D_GOTO_ERROR(H5E_MAP, H5E_NOTFOUND, -H5_DAOS_H5_GET_ERROR, "key not found");

        /* Perform type conversion if necessary */
        if (udata->val_need_tconv) {
            /* Type conversion */
            if (H5Tconvert(map->val_file_type_id, udata->val_mem_type_id, 1, udata->tconv_buf, udata->bkg_buf,
                           udata->md_rw_cb_ud.req->dxpl_id) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR,
                             "can't perform type conversion");

            /* Copy to user's buffer if necessary */
            if (udata->value_buf != udata->tconv_buf)
                (void)memcpy(udata->value_buf, udata->tconv_buf, udata->val_mem_type_size);
        } /* end if */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Close map */
        if (udata->md_rw_cb_ud.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "map key value read completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        if (udata->tconv_buf && (udata->tconv_buf != udata->value_buf))
            DV_free(udata->tconv_buf);
        if (udata->bkg_buf && (udata->bkg_buf != udata->value_buf))
            DV_free(udata->bkg_buf);
        if (udata->key_buf_alloc)
            DV_free(udata->key_buf_alloc);
        udata = DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_map_get_val_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_put
 *
 * Purpose:     Adds a key-value pair to the Map specified by map_id, or
 *              updates the value for the specified key if one was set
 *              previously. key_mem_type_id and val_mem_type_id specify
 *              the datatypes for the provided key and value buffers, and
 *              if different from those used to create the Map object, the
 *              key and value will be internally converted to the
 *              datatypes for the map object. Any further options can be
 *              specified through the property list dxpl_id.
 *
 * Return:      Success:        0
 *              Failure:        -1, value not set.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_map_put(void *_map, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, const void *value,
                hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_rw_ud_t *write_udata = NULL;
    H5_daos_map_t       *map         = (H5_daos_map_t *)_map;
    union {
        const void *const_buf;
        void       *buf;
    } safe_value                     = {.const_buf = value};
    H5_daos_req_t *int_req           = NULL;
    tse_task_t    *first_task        = NULL;
    tse_task_t    *dep_task          = NULL;
    tse_task_t    *bkg_buf_fill_task = NULL;
    tse_task_t    *write_task        = NULL;
    hbool_t        fill_bkg          = FALSE;
    int            ret;
    herr_t         ret_value = SUCCEED;

    if (!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL");
    if (!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL");
    if (!value)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map value is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check for write access */
    if (!(map->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file");

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(map->obj.item.file, "map put value", map->obj.item.open_req,
                                              NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Wait for the map to open if necessary */
    if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
        if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        if (map->obj.item.open_req->status != 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "map open failed");
    } /* end if */

    /* Allocate argument struct for key-value pair write task */
    if (NULL == (write_udata = (H5_daos_map_rw_ud_t *)DV_calloc(sizeof(H5_daos_map_rw_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for map key-value write task arguments");
    write_udata->md_rw_cb_ud.req = int_req;
    write_udata->md_rw_cb_ud.obj = &map->obj;
    write_udata->val_mem_type_id = val_mem_type_id;
    write_udata->value_buf       = safe_value.buf;

    /* Convert key (if necessary) */
    if (H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &write_udata->key_buf,
                             &write_udata->key_size, &write_udata->key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key");

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&write_udata->md_rw_cb_ud.dkey, write_udata->key_buf,
                       (daos_size_t)write_udata->key_size);
    write_udata->md_rw_cb_ud.free_dkey = FALSE;

    /* Check if the type conversion is needed */
    if ((write_udata->val_need_tconv = H5_daos_need_tconv(map->val_file_type_id, val_mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed");

    /* Type conversion */
    if (write_udata->val_need_tconv) {
        /* Initialize type conversion */
        if (H5_daos_tconv_init(val_mem_type_id, &write_udata->val_mem_type_size, map->val_file_type_id,
                               &write_udata->val_file_type_size, 1, FALSE, TRUE, &write_udata->tconv_buf,
                               &write_udata->bkg_buf, NULL, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion");
    } /* end if */
    else
        write_udata->val_file_type_size = map->val_file_type_size;

    /* Set up iod */
    memset(&write_udata->md_rw_cb_ud.iod[0], 0, sizeof(daos_iod_t));
    daos_const_iov_set((d_const_iov_t *)&write_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_map_key_g,
                       H5_daos_map_key_size_g);
    write_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
    write_udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)write_udata->val_file_type_size;
    write_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    write_udata->md_rw_cb_ud.free_akeys = FALSE;

    /* Set up constant sgl info */
    write_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
    write_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
    write_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &write_udata->md_rw_cb_ud.sg_iov[0];

    /* Set nr */
    write_udata->md_rw_cb_ud.nr = 1u;

    /* Check for type conversion */
    if (write_udata->val_need_tconv) {
        /* Check if we need to fill background buffer */
        if (fill_bkg) {
            assert(write_udata->bkg_buf);

            /* Read data from map to background buffer */
            daos_iov_set(&write_udata->md_rw_cb_ud.sg_iov[0], write_udata->bkg_buf,
                         (daos_size_t)write_udata->val_file_type_size);

            /* Create task to read data from map to background buffer */
            assert(!dep_task);
            if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                         H5_daos_map_put_fill_comp_cb, write_udata, &bkg_buf_fill_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL,
                             "can't create task to read data from map to background buffer");

            /* Save background buffer data read task to be scheduled later and give
             * it a reference to req and the map object */
            first_task = bkg_buf_fill_task;
            dep_task   = bkg_buf_fill_task;
            int_req->rc++;
            map->obj.item.rc++;
        } /* end if */
        else {
            /* Copy data to type conversion buffer */
            (void)memcpy(write_udata->tconv_buf, value, (size_t)write_udata->val_mem_type_size);

            /* Perform type conversion */
            if (H5Tconvert(val_mem_type_id, map->val_file_type_id, 1, write_udata->tconv_buf,
                           write_udata->bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion");

            /* Set sgl to write from tconv_buf */
            daos_iov_set(&write_udata->md_rw_cb_ud.sg_iov[0], write_udata->tconv_buf,
                         (daos_size_t)write_udata->val_file_type_size);
        } /* end else */

        write_udata->md_rw_cb_ud.free_sg_iov[0] = TRUE;
    } /* end if */
    else
        /* Set sgl to write from value */
        daos_const_iov_set((d_const_iov_t *)&write_udata->md_rw_cb_ud.sg_iov[0], value,
                           (daos_size_t)write_udata->val_file_type_size);

    /* Set task name */
    write_udata->md_rw_cb_ud.task_name = "map key-value write";

    /* Create task to write key-value pair to map */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, dep_task ? 1 : 0, dep_task ? &dep_task : NULL,
                                 H5_daos_md_rw_prep_cb, H5_daos_map_put_comp_cb, write_udata,
                                 &write_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to write key-value pair to map");

    /* Save map key-value write task to be scheduled later and give
     * it a reference to req and the map object */
    if (first_task) {
        if (0 != (ret = tse_task_schedule(write_task, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL,
                         "can't schedule task to write key-value pair to map: %s",
                         H5_daos_err_to_string(ret));
    }
    else
        first_task = write_task;
    dep_task = write_task;
    int_req->rc++;
    map->obj.item.rc++;

    write_udata = NULL;

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the map's request queue.  This will add the
         * dependency on the map open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &map->obj.item, H5_DAOS_OP_TYPE_WRITE,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, FAIL,
                             "map key-value write operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (ret_value < 0) {
        if (write_udata) {
            if (write_udata->tconv_buf && (write_udata->tconv_buf != value)) {
                DV_free(write_udata->tconv_buf);
                write_udata->tconv_buf = NULL;
            }
            if (write_udata->bkg_buf && (write_udata->bkg_buf != value)) {
                DV_free(write_udata->bkg_buf);
                write_udata->bkg_buf = NULL;
            }
            write_udata->key_buf_alloc = DV_free(write_udata->key_buf_alloc);
        }
        write_udata = DV_free(write_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_map_put() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_put_fill_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch to fill
 *              a type conversion background buffer when performing a map
 *              "put" operation that requires type conversion. Currently
 *              checks for a failed task and then performs type conversion.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_put_fill_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_rw_ud_t *udata;
    int                  ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = "map put background buffer fill task";
    } /* end if */
    else if (task->dt_result == 0) {
        H5_daos_map_t *map = (H5_daos_map_t *)udata->md_rw_cb_ud.obj;

        assert(map);

        /* Reset iod_size; if the key was not created then it could have
         * been overwritten by daos_obj_fetch */
        udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)udata->val_file_type_size;

        /* Copy data to type conversion buffer */
        (void)memcpy(udata->tconv_buf, udata->value_buf, (size_t)udata->val_mem_type_size);

        /* Perform type conversion */
        if (H5Tconvert(udata->val_mem_type_id, map->val_file_type_id, 1, udata->tconv_buf, udata->bkg_buf,
                       udata->md_rw_cb_ud.req->dxpl_id) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, -H5_DAOS_H5_TCONV_ERROR, "can't perform type conversion");

        /* Set sgl to write from tconv_buf */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->tconv_buf, (daos_size_t)udata->val_file_type_size);
    } /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Close map */
        if (udata->md_rw_cb_ud.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "map put background buffer fill completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_map_put_fill_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_put_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_update to add
 *              a new key-value pair to a map object or to update an
 *              existing key-value pair in a map object. Currently checks
 *              for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_put_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_rw_ud_t *udata;
    int                  ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for I/O task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);

    /* Close map */
    if (udata->md_rw_cb_ud.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->md_rw_cb_ud.obj) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in update task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = "map key-value write";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free private data */
    if (udata->tconv_buf && (udata->tconv_buf != udata->value_buf))
        DV_free(udata->tconv_buf);
    if (udata->bkg_buf && (udata->bkg_buf != udata->value_buf))
        DV_free(udata->bkg_buf);
    if (udata->key_buf_alloc)
        DV_free(udata->key_buf_alloc);
    udata = DV_free(udata);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_map_put_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_exists
 *
 * Purpose:     Check if the specified key exists in the map. The result
 *              will be returned via the "exists" parameter.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_map_exists(void *_map, hid_t key_mem_type_id, const void *key, hbool_t *exists, hid_t dxpl_id,
                   void **req)
{
    H5_daos_map_exists_ud_t *exists_udata    = NULL;
    H5_daos_map_t           *map             = (H5_daos_map_t *)_map;
    H5_daos_req_t           *int_req         = NULL;
    tse_task_t              *first_task      = NULL;
    tse_task_t              *dep_task        = NULL;
    tse_task_t              *map_exists_task = NULL;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    if (!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL");
    if (!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL");
    if (!exists)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map exists pointer is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(map->obj.item.file, "map key existence check",
                                              map->obj.item.open_req, NULL, NULL, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    /* Wait for the map to open if necessary */
    if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
        if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        if (map->obj.item.open_req->status != 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "map open failed");
    } /* end if */

    /* Allocate argument struct for key existence checking task */
    if (NULL == (exists_udata = (H5_daos_map_exists_ud_t *)DV_calloc(sizeof(H5_daos_map_exists_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate buffer for map key existence check task arguments");
    exists_udata->exists_ret      = exists;
    exists_udata->md_rw_cb_ud.req = int_req;
    exists_udata->md_rw_cb_ud.obj = &map->obj;

    /* Convert key (if necessary) */
    if (H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &exists_udata->key_buf,
                             &exists_udata->key_size, &exists_udata->key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key");

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&exists_udata->md_rw_cb_ud.dkey, exists_udata->key_buf,
                       (daos_size_t)exists_udata->key_size);
    exists_udata->md_rw_cb_ud.free_dkey = FALSE;

    /* Set up iod */
    memset(&exists_udata->md_rw_cb_ud.iod[0], 0, sizeof(daos_iod_t));
    daos_const_iov_set((d_const_iov_t *)&exists_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_map_key_g,
                       H5_daos_map_key_size_g);
    exists_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
    exists_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
    exists_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    exists_udata->md_rw_cb_ud.free_akeys = FALSE;

    /* Set nr */
    exists_udata->md_rw_cb_ud.nr = 1u;

    /* Set task name */
    exists_udata->md_rw_cb_ud.task_name = "map key existence check";

    /* Create task to read map metadata size */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_map_exists_prep_cb,
                                 H5_daos_map_exists_comp_cb, exists_udata, &map_exists_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to read map metadata size");

    /* Save map metadata size read task to be scheduled later and give
     * it a reference to req and the map object */
    assert(!first_task);
    first_task = map_exists_task;
    dep_task   = map_exists_task;
    int_req->rc++;
    map->obj.item.rc++;

    exists_udata = NULL;

done:
    if (int_req) {
        assert(map);

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the map's request queue.  This will add the
         * dependency on the map open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &map->obj.item, H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, FAIL,
                             "map key exists operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    if (ret_value < 0) {
        if (exists_udata)
            exists_udata->key_buf_alloc = DV_free(exists_udata->key_buf_alloc);
        exists_udata = DV_free(exists_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_map_exists() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_exists_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_fetch to check
 *              if a specified key value exists in a map object.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_exists_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_exists_ud_t *udata;
    daos_obj_rw_t           *rw_args;
    int                      ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->md_rw_cb_ud.req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_MAP);

    assert(udata->md_rw_cb_ud.obj);
    assert(udata->md_rw_cb_ud.req->file);

    /* Set fetch task arguments */
    if (NULL == (rw_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for metadata I/O task");
    memset(rw_args, 0, sizeof(*rw_args));
    rw_args->oh    = udata->md_rw_cb_ud.obj->obj_oh;
    rw_args->th    = udata->md_rw_cb_ud.req->th;
    rw_args->flags = udata->md_rw_cb_ud.flags;
    rw_args->dkey  = &udata->md_rw_cb_ud.dkey;
    rw_args->nr    = udata->md_rw_cb_ud.nr;
    rw_args->iods  = udata->md_rw_cb_ud.iod;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_map_exists_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_exists_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch to check
 *              if a specified key value exists in a map object. Currently
 *              checks for a failed task then frees private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_exists_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_exists_ud_t *udata;
    int                      ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for metadata I/O task");

    assert(udata->md_rw_cb_ud.req);

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = "map key exists fetch";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->md_rw_cb_ud.req->file);

        /* Set output */
        *udata->exists_ret = (udata->md_rw_cb_ud.iod[0].iod_size != 0);
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Close map */
        if (udata->md_rw_cb_ud.obj && H5_daos_map_close_real((H5_daos_map_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "map key exists completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        if (udata->key_buf_alloc)
            DV_free(udata->key_buf_alloc);
        udata = DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_map_exists_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_get
 *
 * Purpose:     Gets certain information about a map
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_map_get(void *_map, H5VL_map_args_t *map_args, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map        = (H5_daos_map_t *)_map;
    H5_daos_req_t *int_req    = NULL;
    tse_task_t    *first_task = NULL;
    tse_task_t    *dep_task   = NULL;
    hid_t          map_id     = H5I_INVALID_HID;
    int            ret;
    herr_t         ret_value = SUCCEED; /* Return value */

    assert(map_args);

    if (!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (map_args->get.get_type) {
        case H5VL_MAP_GET_MCPL: {
            hid_t *plist_id = &map_args->get.args.get_mcpl.mcpl_id;

            /* Wait for the map to open if necessary */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            /* Retrieve the map's creation property list */
            if ((*plist_id = H5Pcopy(map->mcpl_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get map creation property list");

            /* Set map's object class on dcpl */
            if (H5_daos_set_oclass_from_oid(*plist_id, map->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");

            break;
        } /* end block */
        case H5VL_MAP_GET_MAPL: {
            hid_t *plist_id = &map_args->get.args.get_mapl.mapl_id;

            /* Wait for the map to open if necessary */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            /* Retrieve the map's access property list */
            if ((*plist_id = H5Pcopy(map->mapl_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get map access property list");

            break;
        } /* end block */
        case H5VL_MAP_GET_KEY_TYPE: {
            hid_t *ret_id = &map_args->get.args.get_key_type.type_id;

            /* Wait for the map to open if necessary */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            /* Retrieve the map's key datatype */
            if ((*ret_id = H5Tcopy(map->key_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get key datatype ID of map");
            break;
        } /* end block */
        case H5VL_MAP_GET_VAL_TYPE: {
            hid_t *ret_id = &map_args->get.args.get_val_type.type_id;

            /* Wait for the map to open if necessary */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            /* Retrieve the map's value datatype */
            if ((*ret_id = H5Tcopy(map->val_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get value datatype ID of map");
            break;
        } /* end block */
        case H5VL_MAP_GET_COUNT: {
            H5_daos_iter_data_t iter_data;
            hsize_t            *count = &map_args->get.args.get_count.count;

            /* Wait for the map to open if necessary */
            /* Needed because below code accesses map->key_type_id */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(map->obj.item.file, "get map key count",
                                                      map->obj.item.open_req, NULL, NULL, dxpl_id)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Initialize counter */
            *count = 0;

            /* Register ID for map */
            if ((map_id = H5VLwrap_register(map, H5I_MAP)) < 0)
                D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");
            map->obj.item.rc++;

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_MAP, H5_INDEX_NAME, H5_ITER_INC, FALSE, NULL,
                                   map_id, count, NULL, int_req);
            iter_data.u.map_iter_data.key_mem_type_id = map->key_type_id;
            iter_data.u.map_iter_data.u.map_iter_op   = H5_daos_map_get_count_cb;

            /* Iterate over the keys, counting them */
            if (H5_daos_map_iterate(map, &iter_data, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, FAIL, "can't iterate over map keys");

            break;
        } /* end block */
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from map");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the map's request queue.  This will add the
         * dependency on the map open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &map->obj.item, H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, FAIL, "map get operation failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    if (map_id >= 0) {
        map->obj.item.nonblocking_close = TRUE;
        if ((ret = H5Idec_ref(map_id)) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map ID");
        if (ret)
            map->obj.item.nonblocking_close = FALSE;
        map_id = H5I_INVALID_HID;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_map_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_get_count_cb
 *
 * Purpose:     Iterate callback for H5Mget_count()
 *
 * Return:      Success:        0 (never fails)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_get_count_cb(hid_t H5VL_DAOS_UNUSED map_id, const void H5VL_DAOS_UNUSED *key, void *_int_count)
{
    hsize_t *int_count = (hsize_t *)_int_count;

    (*int_count)++;

    return 0;
} /* end H5_daos_map_get_count_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_specific
 *
 * Purpose:     Performs a map "specific" operation
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_map_specific(void *_item, H5VL_map_args_t *map_args, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item        = (H5_daos_item_t *)_item;
    H5_daos_map_t  *map         = NULL;
    H5_daos_req_t  *int_req     = NULL;
    H5_daos_req_t  *int_int_req = NULL;
    tse_task_t     *first_task  = NULL;
    tse_task_t     *dep_task    = NULL;
    hbool_t         collective_md_read;
    hbool_t         collective_md_write;
    herr_t          iter_ret = 0;
    hid_t           map_id   = H5I_INVALID_HID;
    hid_t           mapl_id  = H5P_MAP_ACCESS_DEFAULT;
    int             ret;
    herr_t          ret_value = SUCCEED;

    assert(map_args);

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Determine metadata I/O mode setting (collective vs. independent)
     * for metadata reads and writes according to file-wide setting on FAPL.
     */
    H5_DAOS_GET_METADATA_IO_MODES(item->file, mapl_id, H5P_MAP_ACCESS_DEFAULT, collective_md_read,
                                  collective_md_write, H5E_MAP, FAIL);

    /* Start H5 operation */
    if (NULL ==
        (int_req = H5_daos_req_create(item->file, "map specific", item->open_req, NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

    switch (map_args->specific.specific_type) {
        /* H5Miterate(_by_name) */
        case H5VL_MAP_ITER: {
            H5_daos_iter_data_t iter_data;
            H5VL_loc_params_t  *loc_params      = &map_args->specific.args.iterate.loc_params;
            hsize_t            *idx             = &map_args->specific.args.iterate.idx;
            hid_t               key_mem_type_id = map_args->specific.args.iterate.key_mem_type_id;
            H5M_iterate_t       op              = map_args->specific.args.iterate.op;
            void               *op_data         = map_args->specific.args.iterate.op_data;

            int_req->op_name = "map iterate";

            switch (loc_params->type) {
                /* H5Miterate */
                case H5VL_OBJECT_BY_SELF: {
                    /* Use item as the map for iteration */
                    if (item->type != H5I_MAP)
                        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a map");

                    map = (H5_daos_map_t *)item;
                    map->obj.item.rc++;
                    break;
                } /* H5VL_OBJECT_BY_SELF */

                /* H5Miterate_by_name */
                case H5VL_OBJECT_BY_NAME: {
                    H5VL_loc_params_t sub_loc_params;

                    /* Start internal H5 operation for target map open.  This will
                     * not be visible to the API, will not be added to an operation
                     * pool, and will be integrated into this function's task chain. */
                    if (NULL == (int_int_req = H5_daos_req_create(
                                     item->file, "target map open within map iterate by name", NULL, NULL,
                                     int_req, H5I_INVALID_HID)))
                        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

                    /* Open target_map */
                    sub_loc_params.obj_type = item->type;
                    sub_loc_params.type     = H5VL_OBJECT_BY_SELF;
                    if (NULL == (map = H5_daos_map_open_int(
                                     item, &sub_loc_params, loc_params->loc_data.loc_by_name.name,
                                     loc_params->loc_data.loc_by_name.lapl_id, int_int_req,
                                     collective_md_read, &first_task, &dep_task)))
                        D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "can't open map for operation");

                    /* Create task to finalize internal operation */
                    if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0,
                                            dep_task ? &dep_task : NULL, NULL, NULL, int_int_req,
                                            &int_int_req->finalize_task) < 0)
                        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL,
                                     "can't create task to finalize internal operation");

                    /* Schedule finalize task (or save it to be scheduled later),
                     * give it ownership of int_int_req, and update task pointers */
                    if (first_task) {
                        if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL,
                                         "can't schedule task to finalize H5 operation: %s",
                                         H5_daos_err_to_string(ret));
                    } /* end if */
                    else
                        first_task = int_int_req->finalize_task;
                    dep_task    = int_int_req->finalize_task;
                    int_int_req = NULL;

                    break;
                } /* H5VL_OBJECT_BY_NAME */

                case H5VL_OBJECT_BY_IDX:
                case H5VL_OBJECT_BY_TOKEN:
                default:
                    D_GOTO_ERROR(H5E_MAP, H5E_BADVALUE, FAIL, "invalid loc_params type");
            } /* end switch */

            /* Wait for the map to open if necessary */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "map open failed");
            } /* end if */

            /* Register ID for map */
            if ((map_id = H5VLwrap_register(map, H5I_MAP)) < 0)
                D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to atomize object handle");

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_MAP, H5_INDEX_NAME, H5_ITER_INC, FALSE, idx,
                                   map_id, op_data, NULL, int_req);
            iter_data.u.map_iter_data.key_mem_type_id = key_mem_type_id;
            iter_data.u.map_iter_data.u.map_iter_op   = op;

            /* Handle iteration return value TODO: how to handle if called async? */
            if (!req)
                iter_data.op_ret_p = &iter_ret;

            /* Perform map iteration */
            if (H5_daos_map_iterate(map, &iter_data, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, FAIL, "map iteration failed");

            break;
        } /* H5VL_MAP_ITER */

        case H5VL_MAP_DELETE: {
            H5VL_loc_params_t *loc_params      = &map_args->specific.args.del.loc_params;
            hid_t              key_mem_type_id = map_args->specific.args.del.key_mem_type_id;
            const void        *key             = map_args->specific.args.del.key;

            int_req->op_name = "map key delete";

            /* Verify loc_params */
            if (H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL,
                             "unsupported map key delete location parameters type");
            map = (H5_daos_map_t *)item;
            map->obj.item.rc++;

            /* Wait for the map to open if necessary */
            if (!map->obj.item.created && map->obj.item.open_req->status != 0) {
                if (H5_daos_progress(map->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (map->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "map open failed");
            } /* end if */

            /* Perform key delete */
            if ((ret_value = H5_daos_map_delete_key(map, key_mem_type_id, key, collective_md_write, int_req,
                                                    &first_task, &dep_task)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTREMOVE, FAIL, "map key delete failed");

            break;
        } /* H5VL_MAP_DELETE */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported map specific operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the map's request queue.  This will add the
         * dependency on the map open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_WRITE, H5_DAOS_OP_SCOPE_OBJ, FALSE,
                                !req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async.  Disabled for iteration for now. */
        if (req && map_args->specific.specific_type != H5VL_MAP_ITER) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, FAIL,
                             "map specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");

            /* Set return value for map iteration, unless this function failed but
             * the iteration did not */
            if (map_args->specific.specific_type == H5VL_MAP_ITER && !(ret_value < 0 && iter_ret >= 0))
                ret_value = iter_ret;
        } /* end else */
    }     /* end if */

    if (map_id >= 0) {
        map->obj.item.nonblocking_close = TRUE;
        if ((ret = H5Idec_ref(map_id)) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map ID");
        if (ret)
            map->obj.item.nonblocking_close = FALSE;
        map_id = -1;
        map    = NULL;
    } /* end if */
    else if (map) {
        if (H5_daos_map_close_real(map) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map");
        map = NULL;
    } /* end else */

    /* Close internal request for target object open */
    if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");

    D_FUNC_LEAVE;
} /* end H5_daos_map_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_iterate
 *
 * Purpose:     Iterates over all key-value pairs stored in the Map
 *              specified by map_id, making the callback specified by op
 *              for each. The idx parameter is an in/out parameter that
 *              may be used to restart a previously interrupted iteration.
 *              At the start of iteration idx should be set to 0, and to
 *              restart iteration at the same location on a subsequent
 *              call to H5Miterate, idx should be the same value as
 *              returned by the previous call.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_iterate(H5_daos_map_t *map, H5_daos_iter_data_t *iter_data, tse_task_t **first_task,
                    tse_task_t **dep_task)
{
    size_t dkey_prefetch_size = 0;
    size_t dkey_alloc_size    = 0;
    int    ret;
    herr_t ret_value = SUCCEED;

    assert(map);
    assert(iter_data);
    assert(iter_data->req);
    assert(first_task);
    assert(dep_task);
    assert(H5_DAOS_ITER_TYPE_MAP == iter_data->iter_type);
    assert(H5_INDEX_NAME == iter_data->index_type);
    assert(H5_ITER_INC == iter_data->iter_order);

    if (!iter_data->u.map_iter_data.u.map_iter_op)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "operator is NULL");

    /* Iteration restart not supported */
    if (iter_data->idx_p && (*iter_data->idx_p != 0))
        D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)");

    /* Get map iterate hints */
    if (map->mapl_id == H5P_MAP_ACCESS_DEFAULT) {
        dkey_prefetch_size = H5_daos_plist_cache_g->mapl_cache.dkey_prefetch_size;
        dkey_alloc_size    = H5_daos_plist_cache_g->mapl_cache.dkey_alloc_size;
    }
    else if (H5Pget_map_iterate_hints(map->mapl_id, &dkey_prefetch_size, &dkey_alloc_size) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get map iterate hints");

    /* Increment reference count on map ID */
    if (H5Iinc_ref(iter_data->iter_root_obj) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINC, FAIL, "can't increment reference count on iteration base object");

    /* Start iteration */
    if (0 != (ret = H5_daos_list_key_init(iter_data, &map->obj, NULL, DAOS_OPC_OBJ_LIST_DKEY,
                                          H5_daos_map_iterate_list_comp_cb, TRUE, dkey_prefetch_size,
                                          dkey_alloc_size, first_task, dep_task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't start map iteration: %s",
                     H5_daos_err_to_string(ret));

done:
    D_FUNC_LEAVE;
} /* end H5_daos_map_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_iterate_list_comp_cb
 *
 * Purpose:     Completion callback for dkey list for map iteration.
 *              Initiates operation on each key-value pair and reissues
 *              list operation if appropriate.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_iterate_list_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_iter_ud_t        *udata         = NULL;
    H5_daos_map_iter_op_ud_t *iter_op_udata = NULL;
    H5_daos_req_t            *req           = NULL;
    tse_task_t               *query_task    = NULL;
    tse_task_t               *first_task    = NULL;
    tse_task_t               *dep_task      = NULL;
    int                       ret;
    int                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for operation finalize task");

    /* Assign req convenience pointer.  We do this so we can still handle errors
     * after transfering ownership of udata.  This should be safe since the
     * iteration metatask holds a reference to req until all iteration is
     * complete at this level. */
    req = udata->iter_data->req;

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        char  *tmp_realloc = NULL;
        size_t key_buf_len = 2 * (udata->sg_iov.iov_buf_len + 1);

        /* Reallocate larger buffer */
        if (NULL == (tmp_realloc = (char *)DV_realloc(udata->sg_iov.iov_buf, key_buf_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR, "can't reallocate key buffer");

        /* Update sg_iov */
        daos_iov_set(&udata->sg_iov, tmp_realloc, (daos_size_t)(key_buf_len - 1));

        /* Reissue list operation */
        if (0 != (ret = H5_daos_list_key_start(udata, DAOS_OPC_OBJ_LIST_DKEY,
                                               H5_daos_map_iterate_list_comp_cb, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't start iteration");
        udata = NULL;
    } /* end if */
    else {
        /* Handle errors in list task.  Only record error in req->status
         * if it does not already contain an error (it could contain an error if
         * another task this task is not dependent on also failed). */
        if (task->dt_result < -H5_DAOS_PRE_ERROR && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = task->dt_result;
            req->failed_task = "map iterate key list completion callback";
        } /* end if */
        else if (task->dt_result == 0) {
            H5_daos_map_t *map = (H5_daos_map_t *)udata->target_obj;
            uint32_t       i;
            char          *p = udata->sg_iov.iov_buf;

            assert(map);

            /* Loop over returned dkeys */
            for (i = 0; i < udata->nr; i++) {
                /* Allocate iter op udata */
                if (NULL ==
                    (iter_op_udata = (H5_daos_map_iter_op_ud_t *)DV_calloc(sizeof(H5_daos_map_iter_op_ud_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate iteration op user data");
                iter_op_udata->generic_ud.req   = req;
                iter_op_udata->iter_ud          = udata;
                iter_op_udata->key_file_type_id = map->key_file_type_id;
                iter_op_udata->key_mem_type_id  = udata->iter_data->u.map_iter_data.key_mem_type_id;

                /* Check for key sharing dkey with other metadata */
                iter_op_udata->shared_dkey = (udata->kds[i].kd_key_len == H5_daos_int_md_key_size_g &&
                                              !memcmp(p, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g)) ||
                                             (udata->kds[i].kd_key_len == H5_daos_attr_key_size_g &&
                                              !memcmp(p, H5_daos_attr_key_g, H5_daos_attr_key_size_g));
                if (iter_op_udata->shared_dkey) {
                    daos_obj_rw_t *rw_args;

                    iter_op_udata->generic_ud.task_name = "map key record query task";

                    /* Set up dkey */
                    daos_iov_set(&iter_op_udata->dkey, (void *)p, udata->kds[i].kd_key_len);

                    /* Set up iod */
                    memset(&iter_op_udata->iod, 0, sizeof(daos_iod_t));
                    daos_const_iov_set((d_const_iov_t *)&iter_op_udata->iod.iod_name, H5_daos_map_key_g,
                                       H5_daos_map_key_size_g);
                    iter_op_udata->iod.iod_nr   = 1u;
                    iter_op_udata->iod.iod_type = DAOS_IOD_SINGLE;
                    iter_op_udata->iod.iod_size = DAOS_REC_ANY;

                    /* Create task to query record in dkey */
                    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, dep_task ? 1 : 0,
                                                 dep_task ? &dep_task : NULL, H5_daos_generic_prep_cb,
                                                 H5_daos_map_iterate_query_comp_cb, iter_op_udata,
                                                 &query_task) < 0)
                        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                     "can't create task to check for value in map");

                    /* Set fetch task arguments */
                    if (NULL == (rw_args = daos_task_get_args(query_task)))
                        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                                     "can't get arguments for metadata I/O task");
                    memset(rw_args, 0, sizeof(*rw_args));
                    rw_args->oh    = map->obj.obj_oh;
                    rw_args->th    = DAOS_TX_NONE;
                    rw_args->flags = 0;
                    rw_args->dkey  = &iter_op_udata->dkey;
                    rw_args->nr    = 1u;
                    rw_args->iods  = &iter_op_udata->iod;

                    /* Schedule record query task (or save it to be scheduled later) */
                    if (first_task) {
                        if (0 != (ret = tse_task_schedule(query_task, false)))
                            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret,
                                         "can't schedule task to check for value in map: %s",
                                         H5_daos_err_to_string(ret));
                    } /* end if */
                    else
                        first_task = query_task;
                    dep_task = query_task;
                } /* end if */

                iter_op_udata->key_buf = p;
                iter_op_udata->key_len = udata->kds[i].kd_key_len;

                /* Create task for iter op */
                if (H5_daos_create_task(H5_daos_map_iterate_op_task, dep_task ? 1 : 0,
                                        dep_task ? &dep_task : NULL, NULL, NULL, iter_op_udata,
                                        &iter_op_udata->op_task) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                                 "can't create task for iteration op");

                /* Schedule iter op (or save it to be scheduled later) and
                 * transfer ownership of iter_op_udata */
                if (first_task) {
                    if (0 != (ret = tse_task_schedule(iter_op_udata->op_task, false)))
                        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't schedule task for iteration op: %s",
                                     H5_daos_err_to_string(ret));
                } /* end if */
                else
                    first_task = iter_op_udata->op_task;
                dep_task      = iter_op_udata->op_task;
                iter_op_udata = NULL;

                /* Advance to next akey */
                p += udata->kds[i].kd_key_len;
            } /* end for */

            /* Continue iteration if we're not done */
            if (!daos_anchor_is_eof(&udata->anchor) && (req->status == -H5_DAOS_INCOMPLETE)) {
                if (0 !=
                    (ret = H5_daos_list_key_start(udata, DAOS_OPC_OBJ_LIST_DKEY,
                                                  H5_daos_map_iterate_list_comp_cb, &first_task, &dep_task)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't start iteration");
                udata = NULL;
            } /* end if */
        }     /* end else */
    }         /* end else */

done:
    /* If we still own udata then iteration is complete.  Register dependency
     * for metatask and schedule it. */
    if (udata) {
        if (dep_task && 0 != (ret = tse_task_register_deps(udata->iter_metatask, 1, &dep_task)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't create dependencies for iteration metatask: %s",
                         H5_daos_err_to_string(ret));

        if (first_task) {
            if (0 != (ret = tse_task_schedule(udata->iter_metatask, false)))
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't schedule iteration metatask: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            first_task = udata->iter_metatask;
        udata = NULL;
    } /* end if */

    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, ret,
                     "can't schedule initial task for map iteration dkey list comp cb: %s",
                     H5_daos_err_to_string(ret));

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up on error */
    if (ret_value < 0) {
        if (iter_op_udata) {
            if (iter_op_udata->key_buf_alloc)
                iter_op_udata->key_buf_alloc = DV_free(iter_op_udata->key_buf_alloc);
            iter_op_udata = DV_free(iter_op_udata);
        } /* end if */

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value != -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = "map iteration dkey list completion callback";
        } /* end if */
    }     /* end if */

    /* Make sure we cleaned up */
    assert(!udata);
    assert(!iter_op_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_map_iterate_list_key_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_iterate_query_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_fetch to query
 *              whether a key-value pair in a map object is actually a map
 *              record key or if it is a dkey representing other metadata.
 *              Currently just checks for a failed task.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_iterate_query_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_iter_op_ud_t *udata;
    int                       ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for map iteration record query task");

    assert(udata->generic_ud.req);
    assert(udata->generic_ud.req->file);

    /* Handle errors in task.  Only record error in udata->req_status if it does
     * not already contain an error (it could contain an error if another task
     * this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->generic_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->generic_ud.req->status      = task->dt_result;
        udata->generic_ud.req->failed_task = udata->generic_ud.task_name;
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    D_FUNC_LEAVE;
} /* end H5_daos_map_iterate_query_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_iterate_op_task
 *
 * Purpose:     Perform operation on a map key-value pair during iteration.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_iterate_op_task(tse_task_t *task)
{
    H5_daos_map_iter_op_ud_t *udata      = NULL;
    H5_daos_req_t            *req        = NULL;
    tse_task_t               *first_task = NULL;
    tse_task_t               *dep_task   = NULL;
    int                       ret;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for iteration operation task");

    /* Set task name in case it was set for the map key record query task */
    udata->generic_ud.task_name = "map iteration op";

    /* Assign req convenience pointer.  We do this so we can still handle errors
     * after transfering ownership of udata.  This should be safe since the
     * iteration metatask holds a reference to req until all iteration is
     * complete at this level. */
    req = udata->generic_ud.req;

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(req, H5E_MAP);

    /* Check if this key's dkey was a dkey shared with other metadata.
     * If it was, skip processing of this key. */
    if (udata->shared_dkey && udata->iod.iod_size == 0)
        D_GOTO_DONE(0);

    /* Add null terminator temporarily.  Only necessary for VL strings
     * but it would take about as much time to check for VL string again
     * after the callback as it does to just always swap in the null
     * terminator so just do this for simplicity. */
    udata->char_replace_loc  = &((char *)udata->key_buf)[udata->key_len];
    udata->char_replace_char = *udata->char_replace_loc;
    *udata->char_replace_loc = '\0';

    /* Convert key (if necessary) */
    if (H5_daos_map_key_conv_reverse(udata->key_file_type_id, udata->key_mem_type_id, udata->key_buf,
                                     (size_t)udata->key_len, &udata->key_buf, &udata->key_buf_alloc,
                                     &udata->vl_union, req->dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_H5_TCONV_ERROR, "can't convert key");

    /* Call the map iteration callback operator function on the current key-value pair */
    if (udata->iter_ud->iter_data->async_op) {
        if (udata->iter_ud->iter_data->u.map_iter_data.u.map_iter_op_async(
                udata->iter_ud->iter_data->iter_root_obj, udata->key_buf, udata->iter_ud->iter_data->op_data,
                &udata->iter_ud->iter_data->op_ret, &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR, "operator function returned failure");
    } /* end if */
    else
        udata->iter_ud->iter_data->op_ret = udata->iter_ud->iter_data->u.map_iter_data.u.map_iter_op(
            udata->iter_ud->iter_data->iter_root_obj, udata->key_buf, udata->iter_ud->iter_data->op_data);

    /* Check for failure from operator return */
    if (udata->iter_ud->iter_data->op_ret < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR, "operator function returned failure");

    /* Advance idx */
    if (udata->iter_ud->iter_data->idx_p)
        (*udata->iter_ud->iter_data->idx_p)++;

    /* Check for short-circuit success */
    if (udata->iter_ud->iter_data->op_ret) {
        udata->iter_ud->iter_data->req->status        = -H5_DAOS_SHORT_CIRCUIT;
        udata->iter_ud->iter_data->short_circuit_init = TRUE;

        D_GOTO_DONE(-H5_DAOS_SHORT_CIRCUIT);
    } /* end if */

done:
    /* Check for tasks scheduled, in this case we need to schedule a task to
     * mark this task complete and free udata */
    if (dep_task) {
        tse_task_t *op_end_task;

        assert(udata);

        /* Schedule task to complete this task and free udata */
        if (H5_daos_create_task(H5_daos_map_iter_op_end, 1, &dep_task, NULL, NULL, udata, &op_end_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to finish iteration op");
        else {
            /* Schedule map iter op end task and give it ownership of udata */
            assert(first_task);
            if (0 != (ret = tse_task_schedule(op_end_task, false)))
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't schedule task to finish map iteration op: %s",
                             H5_daos_err_to_string(ret));
            udata    = NULL;
            dep_task = op_end_task;
        } /* end else */

        /* Schedule first task */
        assert(first_task);
        if (0 != (ret = tse_task_schedule(first_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, ret, "can't schedule initial task for map iteration op: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        assert(!first_task);

    /* Handle errors */
    if (ret_value < 0 && req) {
        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value != -H5_DAOS_SHORT_CIRCUIT && req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            req->status      = ret_value;
            req->failed_task = udata->generic_ud.task_name;
        } /* end if */
    }     /* end if */

    /* Complete task and free udata if we still own udata */
    if (udata) {
        /* Replace char */
        if (udata->char_replace_loc)
            *udata->char_replace_loc = udata->char_replace_char;

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

        /* Complete this task */
        tse_task_complete(task, ret_value);

        /* Free private data */
        if (udata->key_buf_alloc)
            DV_free(udata->key_buf_alloc);
        udata = DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    /* Make sure we cleaned up */
    assert(!udata);

    D_FUNC_LEAVE;
} /* end H5_daos_map_iterate_op_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_iter_op_end
 *
 * Purpose:     Completes the map iteration op and frees its udata.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_iter_op_end(tse_task_t *task)
{
    H5_daos_map_iter_op_ud_t *udata;
    int                       ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_IO, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR, "can't get private data for task");

    /* Replace char */
    if (udata->char_replace_loc)
        *udata->char_replace_loc = udata->char_replace_char;

    /* Check if we need to update the request status due to an operator return */
    if (udata->iter_ud->iter_data->async_op &&
        udata->iter_ud->iter_data->req->status >= -H5_DAOS_INCOMPLETE) {
        /* Check for failure from operator return */
        if (udata->iter_ud->iter_data->op_ret < 0) {
            udata->iter_ud->iter_data->req->status      = -H5_DAOS_CALLBACK_ERROR;
            udata->iter_ud->iter_data->req->failed_task = "map iteration callback operator function";
            D_DONE_ERROR(H5E_LINK, H5E_BADITER, -H5_DAOS_CALLBACK_ERROR,
                         "operator function returned failure");
        } /* end if */
        else if (udata->iter_ud->iter_data->op_ret) {
            /* Short-circuit success */
            udata->iter_ud->iter_data->req->status        = -H5_DAOS_SHORT_CIRCUIT;
            udata->iter_ud->iter_data->short_circuit_init = TRUE;
        } /* end if */
    }     /* end if */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, udata->op_task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete op task */
    tse_task_complete(udata->op_task, 0);

    /* Free udata */
    if (udata->key_buf_alloc)
        DV_free(udata->key_buf_alloc);
    DV_free(udata);

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete this task */
    tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_map_iter_op_end() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_delete_key
 *
 * Purpose:     Deletes a key-value pair from the Map specified by map_id.
 *              key_mem_type_id specifies the datatype for the provided
 *              key buffers, and if different from that used to create the
 *              Map object, the key will be internally converted to the
 *              datatype for the map object. Any further options can be
 *              specified through the property list dxpl_id.
 *
 * Return:      Success:        0
 *              Failure:        -1, key/value pair not deleted.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_delete_key(H5_daos_map_t *map, hid_t key_mem_type_id, const void *key, hbool_t collective,
                       H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_map_delete_key_ud_t *delete_udata = NULL;
    tse_task_t                  *delete_task;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    assert(req);
    assert(first_task);
    assert(dep_task);

    if (!map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL");
    if (!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL");

    /* Check for write access */
    if (!(map->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file");

    if (!collective || (map->obj.item.file->my_rank == 0)) {
        daos_opc_t daos_op;

        /* Allocate argument struct for deletion task */
        if (NULL ==
            (delete_udata = (H5_daos_map_delete_key_ud_t *)DV_calloc(sizeof(H5_daos_map_delete_key_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                         "can't allocate buffer for map key deletion task callback arguments");
        delete_udata->req = req;
        delete_udata->map = map;

        /* Convert key (if necessary) */
        if (H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &delete_udata->key_buf,
                                 &delete_udata->key_size, &delete_udata->key_buf_alloc, req->dxpl_id) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key");

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&delete_udata->dkey, delete_udata->key_buf,
                           (daos_size_t)delete_udata->key_size);

        /* Check for key sharing dkey with other metadata.  If dkey is shared, only
         * delete akey, otherwise delete dkey. */
        if (((delete_udata->key_size == H5_daos_int_md_key_size_g) &&
             !memcmp(key, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g)) ||
            ((delete_udata->key_size == H5_daos_attr_key_size_g) &&
             !memcmp(key, H5_daos_attr_key_g, H5_daos_attr_key_size_g))) {
            /* Set up akey */
            daos_const_iov_set((d_const_iov_t *)&delete_udata->akey, H5_daos_map_key_g,
                               H5_daos_map_key_size_g);

            delete_udata->shared_dkey = TRUE;

            /* Create task to remove akey */
            daos_op = DAOS_OPC_OBJ_PUNCH_AKEYS;
        } /* end if */
        else {
            /* Create task to remove dkey */
            daos_op = DAOS_OPC_OBJ_PUNCH_DKEYS;
        } /* end else */

        if (H5_daos_create_daos_task(daos_op, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                     H5_daos_map_delete_key_prep_cb, H5_daos_map_delete_key_comp_cb,
                                     delete_udata, &delete_task) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to delete map key");

        /* Schedule task to delete map key (or save it to be scheduled later)
         * and give it a reference to req.
         */
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(delete_task, false)))
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to delete map key: %s",
                             H5_daos_err_to_string(ret));
        } /* end if */
        else
            *first_task = delete_task;
        req->rc++;
        map->obj.item.rc++;
        *dep_task = delete_task;

        delete_udata = NULL;
    } /* end if */

done:
    if (ret_value < 0) {
        if (delete_udata)
            delete_udata->key_buf_alloc = DV_free(delete_udata->key_buf_alloc);
        delete_udata = DV_free(delete_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_map_delete_key() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_delete_key_prep_cb
 *
 * Purpose:     Prepare callback for asynchronous daos_obj_punch_akeys/
 *              daos_obj_punch_dkeys to delete a key-value pair from a map
 *              object. Currently checks for errors from previous tasks and
 *              then sets arguments for the DAOS operation.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_delete_key_prep_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_delete_key_ud_t *udata;
    daos_obj_punch_t            *punch_args;
    int                          ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for map key deletion task");

    assert(udata->req);

    /* Handle errors */
    H5_DAOS_PREP_REQ(udata->req, H5E_MAP);

    assert(udata->map);

    /* Set deletion task arguments */
    if (NULL == (punch_args = daos_task_get_args(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get arguments for attribute deletion task");
    memset(punch_args, 0, sizeof(*punch_args));
    punch_args->oh      = udata->map->obj.obj_oh;
    punch_args->th      = DAOS_TX_NONE;
    punch_args->dkey    = &udata->dkey;
    punch_args->akeys   = udata->shared_dkey ? &udata->akey : NULL;
    punch_args->flags   = DAOS_COND_PUNCH;
    punch_args->akey_nr = udata->shared_dkey ? 1 : 0;

done:
    if (ret_value < 0)
        tse_task_complete(task, ret_value);

    D_FUNC_LEAVE;
} /* end H5_daos_map_delete_key_prep_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_delete_key_comp_cb
 *
 * Purpose:     Complete callback for asynchronous daos_obj_punch_akeys/
 *              daos_obj_punch_dkeys to delete a key-value pair from a map
 *              object. Currently checks for a failed task then frees
 *              private data.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_map_delete_key_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_map_delete_key_ud_t *udata;
    int                          ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for map key deletion task");

    /* Handle errors in deletion task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->req->status      = task->dt_result;
        udata->req->failed_task = "map key deletion task";
    } /* end if */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    if (udata) {
        if (udata->map && H5_daos_map_close_real(udata->map) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close map");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "map key deletion task completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free private data */
        if (udata->key_buf_alloc)
            DV_free(udata->key_buf_alloc);
        DV_free(udata);
    }
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_map_delete_key_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_close_real
 *
 * Purpose:     Internal version of H5_daos_map_close()
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
H5_daos_map_close_real(H5_daos_map_t *map)
{
    int    ret;
    herr_t ret_value = SUCCEED;

    if (!map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL");
    if (H5I_MAP != map->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a map");

    if (--map->obj.item.rc == 0) {
        /* Free map data structures */
        if (map->obj.item.cur_op_pool)
            H5_daos_op_pool_free(map->obj.item.cur_op_pool);
        if (map->obj.item.open_req)
            if (H5_daos_req_free_int(map->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");
        if (!daos_handle_is_inval(map->obj.obj_oh))
            if (0 != (ret = daos_obj_close(map->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_MAP, H5E_CANTCLOSEOBJ, FAIL, "can't close map DAOS object: %s",
                             H5_daos_err_to_string(ret));
        if (map->key_type_id != H5I_INVALID_HID && H5Idec_ref(map->key_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype");
        if (map->key_file_type_id != H5I_INVALID_HID && H5Idec_ref(map->key_file_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype");
        if (map->val_type_id != H5I_INVALID_HID && H5Idec_ref(map->val_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype");
        if (map->val_file_type_id != H5I_INVALID_HID && H5Idec_ref(map->val_file_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype");
        if (map->mcpl_id != H5I_INVALID_HID && map->mcpl_id != H5P_MAP_CREATE_DEFAULT)
            if (H5Idec_ref(map->mcpl_id) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close mcpl");
        if (map->mapl_id != H5I_INVALID_HID && map->mapl_id != H5P_MAP_ACCESS_DEFAULT)
            if (H5Idec_ref(map->mapl_id) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close mapl");
        map = H5FL_FREE(H5_daos_map_t, map);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_map_close_real() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_close
 *
 * Purpose:     Closes a DAOS HDF5 map.
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
H5_daos_map_close(void *_map, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_map_t               *map        = (H5_daos_map_t *)_map;
    H5_daos_obj_close_task_ud_t *task_ud    = NULL;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    H5_daos_req_t               *int_req    = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    if (!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check if the map's request queue is empty, if so we can close it
     * immediately.  Also close if the pool is empty and has no start task (and
     * hence does not depend on anything).  Also close if it is marked to close
     * nonblocking. */
    if (((map->obj.item.open_req->status == 0 || map->obj.item.open_req->status < -H5_DAOS_CANCELED) &&
         (!map->obj.item.cur_op_pool || (map->obj.item.cur_op_pool->type == H5_DAOS_OP_TYPE_EMPTY &&
                                         !map->obj.item.cur_op_pool->start_task))) ||
        map->obj.item.nonblocking_close) {
        if (H5_daos_map_close_real(map) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map");
    } /* end if */
    else {
        tse_task_t *close_task = NULL;

        /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
        if (NULL == (int_req = H5_daos_req_create(map->obj.item.file, "map close", map->obj.item.open_req,
                                                  NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_obj_close_task_ud_t *)DV_calloc(sizeof(H5_daos_obj_close_task_ud_t))))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, FAIL, "can't allocate space for close task udata struct");
        task_ud->req  = int_req;
        task_ud->item = &map->obj.item;

        /* Create task to close map */
        if (H5_daos_create_task(H5_daos_object_close_task, 0, NULL, NULL, NULL, task_ud, &close_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to close map");

        /* Save task to be scheduled later and give it a reference to req and
         * map */
        assert(!first_task);
        first_task = close_task;
        dep_task   = close_task;
        /* No need to take a reference to map here since the purpose is to
         * release the API's reference */
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the map open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &map->obj.item, H5_DAOS_OP_TYPE_CLOSE,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't add request to request queue");
        map = NULL;

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, FAIL, "map close failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_map_close() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_flush
 *
 * Purpose:     Flushes a DAOS map.  Creates a barrier task so all async
 *              ops created before the flush execute before all async ops
 *              created after the flush.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_map_flush(H5_daos_map_t H5VL_DAOS_UNUSED *map, H5_daos_req_t H5VL_DAOS_UNUSED *req,
                  tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *barrier_task = NULL;
    herr_t      ret_value    = SUCCEED; /* Return value */

    assert(map);

    /* Create task that does nothing but complete itself.  Only necessary
     * because we can't enqueue a request that has no tasks */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &barrier_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't create barrier task for group flush");

    /* Schedule barrier task (or save it to be scheduled later)  */
    assert(!*first_task);
    *first_task = barrier_task;
    *dep_task   = barrier_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_map_flush() */
