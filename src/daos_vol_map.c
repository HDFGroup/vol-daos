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
 * library.  Map routines
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_map_key_conv(hid_t src_type_id, hid_t dst_type_id,
    const void *key, const void **key_buf, size_t *key_size,
    void **key_buf_alloc, hid_t dxpl_id);
static herr_t H5_daos_map_key_conv_reverse(hid_t src_type_id, hid_t dst_type_id,
    void *key, size_t key_size, void **key_buf, void **key_buf_alloc,
    H5_daos_vl_union_t *vl_union, hid_t dxpl_id);
static herr_t H5_daos_map_get_count_cb(hid_t map_id, const void *key,
    void *_int_count);
static herr_t H5_daos_map_iterate(H5_daos_map_t *map, hid_t map_id,
    hsize_t *idx, hid_t key_mem_type_id, H5M_iterate_t op, void *op_data,
    hid_t dxpl_id, void **req);
static herr_t  H5_daos_map_delete_key(H5_daos_map_t *map, hid_t key_mem_type_id,
    const void *key, hid_t dxpl_id, void **req);


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
H5_daos_map_create(void *_item,
    const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
    hid_t lcpl_id, hid_t ktype_id, hid_t vtype_id, hid_t mcpl_id, hid_t mapl_id,
    hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_map_t *map = NULL;
    H5_daos_group_t *target_grp = NULL;
    H5T_class_t ktype_class;
    htri_t has_vl_vlstr_ref;
    hid_t ktype_parent_id = H5I_INVALID_HID;
    void *ktype_buf = NULL;
    void *vtype_buf = NULL;
    void *mcpl_buf = NULL;
    hbool_t collective;
    H5_daos_md_rw_cb_ud_t *update_cb_ud = NULL;
    tse_task_t *finalize_task;
    int finalize_ndeps = 0;
    tse_task_t *finalize_deps[2];
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    void *ret_value = NULL;

    /* Make sure H5_DAOS_g is set.  Eventually move this to a FUNC_ENTER_API
     * type macro? */
    H5_DAOS_G_INIT(NULL)

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map parent object is NULL")

    /* Check validity of key type.  Vlens are only allwed at the top level, no
     * references allowed at all. */
    if(H5T_NO_CLASS == (ktype_class = H5Tget_class(ktype_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTGET, NULL, "can't get key type class")
    if(ktype_class == H5T_VLEN) {
        /* Vlen types must not contain any nested vlens */
        if((ktype_parent_id = H5Tget_super(ktype_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTGET, NULL, "can't get key type parent")
        if((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(ktype_parent_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTINIT, NULL, "can't check for vlen or reference type")
        if(has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type contains nested vlen or reference")
    } /* end if */
    else if(ktype_class == H5T_REFERENCE)
        /* References not supported */
        D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type is a reference type")
    else if(ktype_class != H5T_STRING) {
        /* No nested vlens */
        if((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(ktype_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTINIT, NULL, "can't check for vlen or reference type")
        if(has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type contains nested vlen or reference")
    } /* end if */

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
        D_GOTO_ERROR(H5E_MAP, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Allocate the map object that is returned to the user */
    if(NULL == (map = H5FL_CALLOC(H5_daos_map_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS map struct")
    map->obj.item.type = H5I_MAP;
    map->obj.item.open_req = int_req;
    int_req->rc++;
    map->obj.item.file = item->file;
    map->obj.item.rc = 1;
    map->obj.obj_oh = DAOS_HDL_INVAL;
    map->key_type_id = H5I_INVALID_HID;
    map->key_file_type_id = H5I_INVALID_HID;
    map->val_type_id = H5I_INVALID_HID;
    map->val_file_type_id = H5I_INVALID_HID;
    map->mcpl_id = H5I_INVALID_HID;
    map->mapl_id = H5I_INVALID_HID;

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't start transaction")
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Generate map oid */
    if(H5_daos_oid_generate(&map->obj.oid, H5I_MAP,
            (mcpl_id == H5P_MAP_CREATE_DEFAULT ? H5P_DEFAULT : mcpl_id),
            item->file, collective, int_req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't generate object id")

    /* Open map object */
    if(H5_daos_obj_open(item->file, int_req, &map->obj.oid, DAOS_OO_RW,
            &map->obj.obj_oh, "map object open", &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map object")

    /* Create map and write metadata if this process should */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        size_t mcpl_size = 0;
        size_t ktype_size = 0;
        size_t vtype_size = 0;
        tse_task_t *update_task;
        tse_task_t *link_write_task;

        /* Traverse the path */
        if(name) {
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, lcpl_id, dxpl_id,
                    NULL, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, NULL, "can't traverse path")

            /* Reject invalid object names during object creation */
            if(!strncmp(target_name, ".", 2))
                D_GOTO_ERROR(H5E_MAP, H5E_BADVALUE, NULL, "invalid map name - '.'")
        } /* end if */

        /* Create map */
        /* Allocate argument struct */
        if(NULL == (update_cb_ud = (H5_daos_md_rw_cb_ud_t *)DV_calloc(sizeof(H5_daos_md_rw_cb_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for update callback arguments")

        /* Encode datatypes */
        if(H5Tencode(ktype_id, NULL, &ktype_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
        if(NULL == (ktype_buf = DV_malloc(ktype_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
        if(H5Tencode(ktype_id, ktype_buf, &ktype_size) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize datatype")

        if(H5Tencode(vtype_id, NULL, &vtype_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
        if(NULL == (vtype_buf = DV_malloc(vtype_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
        if(H5Tencode(vtype_id, vtype_buf, &vtype_size) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize datatype")

        /* Encode MCPL */
        if(H5Pencode2(mcpl_id, NULL, &mcpl_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of mcpl")
        if(NULL == (mcpl_buf = DV_malloc(mcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized mcpl")
        if(H5Pencode2(mcpl_id, mcpl_buf, &mcpl_size, item->file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize mcpl")

        /* Set up operation to write MCPl and datatypes to map */
        /* Point to map */
        update_cb_ud->obj = &map->obj;

        /* Point to req */
        update_cb_ud->req = int_req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        update_cb_ud->free_dkey = FALSE;

        /* The elements in iod and sgl */
        update_cb_ud->nr = 3u;

        /* Set up iod */
        /* Key datatype.  Point akey to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->iod[0].iod_name, (void *)H5_daos_ktype_g, H5_daos_ktype_size_g);
        update_cb_ud->iod[0].iod_nr = 1u;
        update_cb_ud->iod[0].iod_size = (uint64_t)ktype_size;
        update_cb_ud->iod[0].iod_type = DAOS_IOD_SINGLE;

        /* Value datatype */
        daos_iov_set(&update_cb_ud->iod[1].iod_name, (void *)H5_daos_vtype_g, H5_daos_vtype_size_g);
        update_cb_ud->iod[1].iod_nr = 1u;
        update_cb_ud->iod[1].iod_size = (uint64_t)vtype_size;
        update_cb_ud->iod[1].iod_type = DAOS_IOD_SINGLE;

        /* MCPL */
        daos_iov_set(&update_cb_ud->iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        update_cb_ud->iod[2].iod_nr = 1u;
        update_cb_ud->iod[2].iod_size = (uint64_t)mcpl_size;
        update_cb_ud->iod[2].iod_type = DAOS_IOD_SINGLE;

        /* Do not free global akey buffers */
        update_cb_ud->free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->sg_iov[0], ktype_buf, (daos_size_t)ktype_size);
        update_cb_ud->sgl[0].sg_nr = 1;
        update_cb_ud->sgl[0].sg_nr_out = 0;
        update_cb_ud->sgl[0].sg_iovs = &update_cb_ud->sg_iov[0];
        daos_iov_set(&update_cb_ud->sg_iov[1], vtype_buf, (daos_size_t)vtype_size);
        update_cb_ud->sgl[1].sg_nr = 1;
        update_cb_ud->sgl[1].sg_nr_out = 0;
        update_cb_ud->sgl[1].sg_iovs = &update_cb_ud->sg_iov[1];
        daos_iov_set(&update_cb_ud->sg_iov[2], mcpl_buf, (daos_size_t)mcpl_size);
        update_cb_ud->sgl[2].sg_nr = 1;
        update_cb_ud->sgl[2].sg_nr_out = 0;
        update_cb_ud->sgl[2].sg_iovs = &update_cb_ud->sg_iov[2];

        /* Set task name */
        update_cb_ud->task_name = "map metadata write";

        /* Create task for map metadata write */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_UPDATE, &item->file->sched, 0, NULL, &update_task)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create task to write map medadata: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        if(0 != (ret = tse_task_register_deps(update_task, 1, &dep_task)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create dependencies for dataset metadata write: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for map metadata write */
        if(0 != (ret = tse_task_register_cbs(update_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_md_update_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't register callbacks for task to write map medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for map metadata write */
        (void)tse_task_set_priv(update_task, update_cb_ud);

        /* Schedule map metadata write task and give it a reference to req and
         * the map */
        if(0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to write map metadata: %s", H5_daos_err_to_string(ret))
        int_req->rc++;
        map->obj.item.rc++;
        update_cb_ud = NULL;
        ktype_buf = NULL;
        vtype_buf = NULL;
        mcpl_buf = NULL;

        /* Add dependency for finalize task */
        finalize_deps[finalize_ndeps] = update_task;
        finalize_ndeps++;

        /* Create link to map */
        if(target_grp) {
            H5_daos_link_val_t link_val;

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = map->obj.oid;
            link_val.target_oid_async = &map->obj.oid;
            if(H5_daos_link_write(target_grp, target_name, strlen(target_name),
                    &link_val, int_req, &link_write_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create link to map")
            finalize_deps[finalize_ndeps] = link_write_task;
            finalize_ndeps++;
        } /* end if */
    } /* end if */
    else {
        /* Only dep_task created, register it as the finalize dependency */
        assert(finalize_ndeps == 0);
        assert(dep_task);
        finalize_deps[0] = dep_task;
        finalize_ndeps = 1;

        /* Check for failure of process 0 DSINC */
    } /* end else */

    /* Finish setting up map struct */
    if((map->key_type_id = H5Tcopy(ktype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((map->key_file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, ktype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to get file datatype")
    if((map->val_type_id = H5Tcopy(vtype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((map->val_file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, vtype_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to get file datatype")
    if((map->mcpl_id = H5Pcopy(mcpl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy gcpl");
    if((map->mapl_id = H5Pcopy(mapl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Fill OCPL cache */
    if(H5_daos_fill_ocpl_cache(&map->obj, map->mcpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to fill OCPL cache")

    /* Set return value */
    ret_value = (void *)map;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close group")

    /* Close key type parent type */
    if(ktype_parent_id >= 0 && H5Tclose(ktype_parent_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close key type parent type")

    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependencies (if any) */
        else if(finalize_ndeps > 0 && 0 != (ret = tse_task_register_deps(finalize_task, finalize_ndeps, finalize_deps)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(&item->file->sched, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, NULL, "map creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't free request")
    } /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value) {
        /* Close map */
        if(map && H5_daos_map_close(map, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map");

        /* Free memory */
        if(update_cb_ud && update_cb_ud->obj && H5_daos_object_close(update_cb_ud->obj, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close object")
        ktype_buf = DV_free(ktype_buf);
        vtype_buf = DV_free(vtype_buf);
        mcpl_buf = DV_free(mcpl_buf);
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    assert(!update_cb_ud);
    assert(!ktype_buf);
    assert(!vtype_buf);
    assert(!mcpl_buf);

    D_FUNC_LEAVE
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
H5_daos_map_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t mapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_map_t *map = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    H5T_class_t ktype_class;
    htri_t has_vl_vlstr_ref;
    hid_t ktype_parent_id = H5I_INVALID_HID;
    daos_key_t dkey;
    daos_iod_t iod[3];
    daos_sg_list_t sgl[3];
    daos_iov_t sg_iov[3];
    uint64_t ktype_len = 0;
    uint64_t vtype_len = 0;
    uint64_t mcpl_len = 0;
    uint64_t tot_len;
    uint8_t minfo_buf_static[H5_DAOS_GINFO_BUF_SIZE];
    uint8_t *minfo_buf_dyn = NULL;
    uint8_t *minfo_buf = minfo_buf_static;
    uint8_t *p;
    hbool_t collective;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    /* Make sure H5_DAOS_g is set.  Eventually move this to a FUNC_ENTER_API
     * type macro? */
    H5_DAOS_G_INIT(NULL)

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map parent object is NULL")

    /* Check for collective access, if not already set by the file */
    collective = item->file->fapl_cache.is_collective_md_read;
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(mapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, NULL, "can't get collective access property")

    /* Allocate the map object that is returned to the user */
    if(NULL == (map = H5FL_CALLOC(H5_daos_map_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS map struct")
    map->obj.item.type = H5I_MAP;
    map->obj.item.open_req = NULL;
    map->obj.item.file = item->file;
    map->obj.item.rc = 1;
    map->obj.obj_oh = DAOS_HDL_INVAL;
    map->key_type_id = FAIL;
    map->key_file_type_id = FAIL;
    map->val_type_id = FAIL;
    map->val_file_type_id = FAIL;
    map->mcpl_id = FAIL;
    map->mapl_id = FAIL;

    /* Check if we're actually opening the group or just receiving the map
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by object token */
        if(H5VL_OBJECT_BY_TOKEN == loc_params->type) {
            /* Generate oid from token */
            if(H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &map->obj.oid) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't convert object token to OID")
        } /* end if */
        else {
            htri_t link_resolved;

            /* Open using name parameter */
            if(H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "unsupported map open location parameters type")
            if(!name)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map name is NULL")

            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, dxpl_id,
                    req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, NULL, "can't traverse path")

            /* Follow link to map */
            if((link_resolved = H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &map->obj.oid)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_TRAVERSE, NULL, "can't follow link to map")
            if(!link_resolved)
                D_GOTO_ERROR(H5E_MAP, H5E_TRAVERSE, NULL, "link to map did not resolve")
        } /* end else */

        /* Open map */
        if(0 != (ret = daos_obj_open(item->file->coh, map->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &map->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map: %s", H5_daos_err_to_string(ret))

        /* Set up operation to read datatype and MCPL sizes from map */
        /* Set up dkey */
        daos_iov_set(&dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, (void *)H5_daos_ktype_g, H5_daos_ktype_size_g);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = DAOS_REC_ANY;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, (void *)H5_daos_vtype_g, H5_daos_vtype_size_g);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = DAOS_REC_ANY;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        iod[2].iod_nr = 1u;
        iod[2].iod_size = DAOS_REC_ANY;
        iod[2].iod_type = DAOS_IOD_SINGLE;

        /* Read internal metadata sizes from map */
        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 3, iod, NULL,
                      NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, NULL, "can't read metadata sizes from map: %s", H5_daos_err_to_string(ret))

        /* Check for metadata not found */
        if((iod[0].iod_size == (uint64_t)0) || (iod[1].iod_size == (uint64_t)0)
                || (iod[2].iod_size == (uint64_t)0))
            D_GOTO_ERROR(H5E_MAP, H5E_NOTFOUND, NULL, "internal metadata not found");

        /* Compute map info buffer size */
        ktype_len = iod[0].iod_size;
        vtype_len = iod[1].iod_size;
        mcpl_len = iod[2].iod_size;
        tot_len = ktype_len + vtype_len + mcpl_len;

        /* Allocate map info buffer if necessary */
        if((tot_len + (5 * sizeof(uint64_t))) > sizeof(minfo_buf_static)) {
            if(NULL == (minfo_buf_dyn = (uint8_t *)DV_malloc(tot_len + (5 * sizeof(uint64_t)))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate map info buffer")
            minfo_buf = minfo_buf_dyn;
        } /* end if */

        /* Set up sgl */
        p = minfo_buf + (5 * sizeof(uint64_t));
        daos_iov_set(&sg_iov[0], p, (daos_size_t)ktype_len);
        sgl[0].sg_nr = 1;
        sgl[0].sg_nr_out = 0;
        sgl[0].sg_iovs = &sg_iov[0];
        p += ktype_len;
        daos_iov_set(&sg_iov[1], p, (daos_size_t)vtype_len);
        sgl[1].sg_nr = 1;
        sgl[1].sg_nr_out = 0;
        sgl[1].sg_iovs = &sg_iov[1];
        p += vtype_len;
        daos_iov_set(&sg_iov[2], p, (daos_size_t)mcpl_len);
        sgl[2].sg_nr = 1;
        sgl[2].sg_nr_out = 0;
        sgl[2].sg_iovs = &sg_iov[2];

        /* Read internal metadata from map */
        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 3, iod, sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, NULL, "can't read metadata from map: %s", H5_daos_err_to_string(ret))

        /* Broadcast map info if there are other processes that need it */
        if(collective && (item->file->num_procs > 1)) {
            assert(minfo_buf);
            assert(sizeof(minfo_buf_static) >= 5 * sizeof(uint64_t));

            /* Encode oid */
            p = minfo_buf;
            UINT64ENCODE(p, map->obj.oid.lo)
            UINT64ENCODE(p, map->obj.oid.hi)

            /* Encode serialized info lengths */
            UINT64ENCODE(p, ktype_len)
            UINT64ENCODE(p, vtype_len)
            UINT64ENCODE(p, mcpl_len)

            /* MPI_Bcast minfo_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)minfo_buf, sizeof(minfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_MAP, H5E_MPI, NULL, "can't broadcast map info");

            /* Need a second bcast if it did not fit in the receivers' static
             * buffer */
            if(tot_len + (5 * sizeof(uint64_t)) > sizeof(minfo_buf_static))
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                    D_GOTO_ERROR(H5E_MAP, H5E_MPI, NULL, "can't broadcast map info (second broadcast)")
        } /* end if */
        else
            p = minfo_buf + (5 * sizeof(uint64_t));
    } /* end if */
    else {
        /* Receive map info */
        if(MPI_SUCCESS != MPI_Bcast((char *)minfo_buf, sizeof(minfo_buf_static), MPI_BYTE, 0, item->file->comm))
            D_GOTO_ERROR(H5E_MAP, H5E_MPI, NULL, "can't receive broadcasted map info")

        /* Decode oid */
        p = minfo_buf_static;
        UINT64DECODE(p, map->obj.oid.lo)
        UINT64DECODE(p, map->obj.oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, ktype_len)
        UINT64DECODE(p, vtype_len)
        UINT64DECODE(p, mcpl_len)
        tot_len = ktype_len + vtype_len + mcpl_len;

        /* Check for ktype_len set to 0 - indicates failure */
        if(ktype_len == 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "lead process failed to open map")

        /* Check if we need to perform another bcast */
        if(tot_len + (5 * sizeof(uint64_t)) > sizeof(minfo_buf_static)) {
            /* Allocate a dynamic buffer if necessary */
            if(tot_len > sizeof(minfo_buf_static)) {
                if(NULL == (minfo_buf_dyn = (uint8_t *)DV_malloc(tot_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for map info")
                minfo_buf = minfo_buf_dyn;
            } /* end if */

            /* Receive map info */
            if(MPI_SUCCESS != MPI_Bcast((char *)minfo_buf, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_MAP, H5E_MPI, NULL, "can't receive broadcasted map info (second broadcast)")

            p = minfo_buf;
        } /* end if */

        /* Open map */
        if(0 != (ret = daos_obj_open(item->file->coh, map->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &map->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map: %s", H5_daos_err_to_string(ret))
    } /* end else */

    /* Decode datatypes and MCPL */
    if((map->key_type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += ktype_len;
    if((map->val_type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += vtype_len;
    if((map->mcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTDECODE, NULL, "can't deserialize map creation property list")

    /* Check validity of key type.  Vlens are only allwed at the top level, no
     * references allowed at all. */
    if(H5T_NO_CLASS == (ktype_class = H5Tget_class(map->key_type_id)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, NULL, "can't get key type class")
    if(ktype_class == H5T_VLEN) {
        /* Vlen types must not contain any nested vlens */
        if((ktype_parent_id = H5Tget_super(map->key_type_id)) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, NULL, "can't get key type parent")
        if((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(ktype_parent_id)) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't check for vlen or reference type")
        if(has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, NULL, "key type contains nested vlen or reference")
    } /* end if */
    else if(ktype_class == H5T_REFERENCE)
        /* References not supported */
        D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "key type is a reference type")
    else if(ktype_class != H5T_STRING) {
        /* No nested vlens */
        if((has_vl_vlstr_ref = H5_daos_detect_vl_vlstr_ref(map->key_type_id)) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't check for vlen or reference type")
        if(has_vl_vlstr_ref)
            D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, NULL, "key type contains nested vlen or reference")
    } /* end if */

    /* Finish setting up map struct */
    if((map->key_file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, map->key_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to get file datatype")
    if((map->val_file_type_id = H5VLget_file_type(item->file, H5_DAOS_g, map->val_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to get file datatype")
    if((map->mapl_id = H5Pcopy(mapl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy mapl");

    /* Fill OCPL cache */
    if(H5_daos_fill_ocpl_cache(&map->obj, map->mcpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "failed to fill OCPL cache")

    /* Set return value */
    ret_value = (void *)map;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast minfo_buf as '0' if necessary - this will trigger failures in
         * in other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(minfo_buf_static, 0, sizeof(minfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(minfo_buf_static, sizeof(minfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_DONE_ERROR(H5E_MAP, H5E_MPI, NULL, "can't broadcast empty map info")
        } /* end if */

        /* Close map */
        if(map && H5_daos_map_close(map, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close group")

    /* Close key type parent type */
    if(ktype_parent_id >= 0 && H5Tclose(ktype_parent_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close key type parent type")

    /* Free memory */
    minfo_buf_dyn = (uint8_t *)DV_free(minfo_buf_dyn);

    D_FUNC_LEAVE
} /* end H5_daos_map_open() */


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
H5_daos_map_key_conv(hid_t src_type_id, hid_t dst_type_id, const void *key,
    const void **key_buf, size_t *key_size, void **key_buf_alloc, hid_t dxpl_id)
{
    htri_t need_tconv;
    size_t src_type_size;
    size_t dst_type_size;
    hid_t src_parent_type_id = H5I_INVALID_HID;
    hid_t dst_parent_type_id = H5I_INVALID_HID;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    hbool_t fill_bkg = FALSE;
    herr_t ret_value = SUCCEED;

    assert(src_type_id >= 0);
    assert(dst_type_id >= 0);
    assert(key);
    assert(key_buf);
    assert(key_size);
    assert(key_buf_alloc);
    assert(!*key_buf_alloc);

    /* Check if type conversion is needed for the key */
    if((need_tconv = H5_daos_need_tconv(src_type_id, dst_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")
    if(need_tconv) {
        H5T_class_t type_class;

        /* Get class */
        if(H5T_NO_CLASS == (type_class = H5Tget_class(src_type_id)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get datatype class")

        /* Check for vlen */
        if(type_class == H5T_VLEN) {
            size_t src_parent_type_size;
            size_t dst_parent_type_size;
            const hvl_t *vl = (const hvl_t *)key;
            htri_t parent_need_tconv;

            /* Check for empty key - currently unsupported */
            if(vl->len == 0)
                D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, FAIL, "vl key cannot have length of 0")

            /* Get parent types */
            if((src_parent_type_id = H5Tget_super(src_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get source type parent")
            if((dst_parent_type_id = H5Tget_super(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get destination type parent")

            /* Check if type conversion is needed for the parent type */
            if((parent_need_tconv = H5_daos_need_tconv(src_parent_type_id, dst_parent_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")
            if(parent_need_tconv) {
                /* Initialize type conversion */
                if(H5_daos_tconv_init(src_parent_type_id, &src_parent_type_size, dst_parent_type_id, &dst_parent_type_size, vl->len, FALSE, TRUE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion")

                /* If needed, fill the background buffer (with zeros) */
                if(fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, vl->len * dst_parent_type_size);
                } /* end if */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, vl->p, vl->len * src_parent_type_size);

                /* Perform type conversion */
                if(H5Tconvert(src_parent_type_id, dst_parent_type_id, vl->len, tconv_buf, bkg_buf, dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

                /* Set return values to point to converted buffer */
                *key_buf = (const void *)tconv_buf;
                *key_size = vl->len * dst_parent_type_size;
                *key_buf_alloc = tconv_buf;
                tconv_buf = NULL;
            }
            else {
                /* Just get parent size and set return values to point to vlen
                 * buffer */
                if(0 == (dst_parent_type_size = H5Tget_size(dst_parent_type_id)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype")
                *key_buf = vl->p;
                *key_size = vl->len * dst_parent_type_size;
            } /* end else */
        } /* end if */
        else {
            htri_t is_vl_str = FALSE;

            /* Check for VL string */
            if(type_class == H5T_STRING)
                if((is_vl_str = H5Tis_variable_str(src_type_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't check for variable length string")
            if(is_vl_str) {
                /* Set return values to point to string (exclude null terminator
                 * since it's not needed */
                *key_buf = (const void *)*((const char * const *)key);
                if(*key_buf) {
                    *key_size = strlen(*(const char * const *)key);

                    /* If the key is '\0' (null string), write the null
                     * terminator (to distinguish from NULL pointer) */
                    if(*key_size == 0)
                        *key_size = 1;
                } /* end if */
                else {
                    /* If NULL was passed as the key, set the key to be the
                     * magic value of {'\0', '\0'} */
                    if(NULL == (*key_buf_alloc = DV_calloc(2)))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for NULL key")
                    *key_buf = (const void *)*key_buf_alloc;
                    *key_size = 2;
                } /* end else */
            } /* end if */
            else {
                /* Initialize type conversion */
                if(H5_daos_tconv_init(src_type_id, &src_type_size, dst_type_id, &dst_type_size, 1, FALSE, TRUE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion")

                /* If needed, fill the background buffer (with zeros) */
                if(fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, dst_type_size);
                } /* end if */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, key, src_type_size);

                /* Perform type conversion */
                if(H5Tconvert(src_type_id, dst_type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

                /* Set return values to point to converted buffer */
                *key_buf = (const void *)tconv_buf;
                *key_size = dst_type_size;
                *key_buf_alloc = tconv_buf;
                tconv_buf = NULL;
            } /* end else */
        } /* end else */
    } /* end if */
    else {
        /* Just get size and return key */
        if(0 == (dst_type_size = H5Tget_size(dst_type_id)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype")
        *key_buf = key;
        *key_size = dst_type_size;
    } /* end else */

done:
    /* Cleanup */
    if(src_parent_type_id > 0 && H5Tclose(src_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close source type parent type")
    if(dst_parent_type_id > 0 && H5Tclose(dst_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close destination type parent type")
    tconv_buf = DV_free(tconv_buf);
    bkg_buf = DV_free(bkg_buf);

    D_FUNC_LEAVE
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
H5_daos_map_key_conv_reverse(hid_t src_type_id, hid_t dst_type_id,
    void *key, size_t key_size, void **key_buf, void **key_buf_alloc,
    H5_daos_vl_union_t *vl_union, hid_t dxpl_id)
{
    htri_t need_tconv;
    size_t src_type_size;
    size_t dst_type_size;
    hid_t src_parent_type_id = H5I_INVALID_HID;
    hid_t dst_parent_type_id = H5I_INVALID_HID;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    hbool_t fill_bkg = FALSE;
    herr_t ret_value = SUCCEED;

    assert(src_type_id >= 0);
    assert(dst_type_id >= 0);
    assert(key);
    assert(key_size > 0);
    assert(key_buf);
    assert(key_buf_alloc);
    assert(vl_union);

    /* Check if type conversion is needed for the key */
    if((need_tconv = H5_daos_need_tconv(src_type_id, dst_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")
    if(need_tconv) {
        H5T_class_t type_class;

        /* Get class */
        if(H5T_NO_CLASS == (type_class = H5Tget_class(src_type_id)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get datatype class")

        /* Check for vlen */
        if(type_class == H5T_VLEN) {
            size_t src_parent_type_size;
            size_t dst_parent_type_size;
            htri_t parent_need_tconv;

            /* Get parent types */
            if((src_parent_type_id = H5Tget_super(src_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get source type parent")
            if((dst_parent_type_id = H5Tget_super(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get destination type parent")

            /* Check if type conversion is needed for the parent type */
            if((parent_need_tconv = H5_daos_need_tconv(src_parent_type_id, dst_parent_type_id)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")
            if(parent_need_tconv) {
                /* Get source parent type size */
                if(0 == (src_parent_type_size = H5Tget_size(src_parent_type_id)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype")

                /* Calculate sequence length */
                if(key_size % src_parent_type_size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "key size is not a multiple of source datatype size")
                vl_union->vl.len = key_size / src_parent_type_size;

                /* Initialize type conversion */
                if(H5_daos_tconv_init(src_parent_type_id, &src_parent_type_size, dst_parent_type_id, &dst_parent_type_size, vl_union->vl.len, FALSE, FALSE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion")

                /* Note we could reuse buffers here if we change around some of
                 * the logic in H5_daos_tconv_init() to support being passed the
                 * source buffer instead of the destination buffer */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, key, key_size);

                /* If needed, fill the background buffer (with zeros) */
                if(fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, vl_union->vl.len * dst_parent_type_size);
                } /* end if */

                /* Perform type conversion */
                if(H5Tconvert(src_parent_type_id, dst_parent_type_id, vl_union->vl.len, tconv_buf, bkg_buf, dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

                /* Set return values to point to converted buffer */
                vl_union->vl.p = tconv_buf;
                *key_buf = (void *)&(vl_union->vl);
                /*if(tconv_buf != key) {*/
                    *key_buf_alloc = tconv_buf;
                    tconv_buf = NULL;
                /*}*/ /* end if */
            }
            else {
                /* Just get parent size and set return values to point to vlen
                 * buffer */
                if(0 == (dst_parent_type_size = H5Tget_size(dst_parent_type_id)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't get size of datatype")

                /* Calculate sequence length */
                if(key_size % dst_parent_type_size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "key size is not a multiple of source datatype size")
                vl_union->vl.len = key_size / dst_parent_type_size;
                vl_union->vl.p = key;
                *key_buf = (void *)&(vl_union->vl);
            } /* end else */
        } /* end if */
        else {
            htri_t is_vl_str = FALSE;

            /* Check for VL string */
            if(type_class == H5T_STRING)
                if((is_vl_str = H5Tis_variable_str(src_type_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't check for variable length string")
            if(is_vl_str) {
                /* Check for magic value indicating key was passed as a NULL
                 * pointer */
                if(((char *)key)[0] == '\0' && key_size == 2)
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
                if(H5_daos_tconv_init(src_type_id, &src_type_size, dst_type_id, &dst_type_size, 1, FALSE, FALSE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion")

                /* Check size is correct */
                if(key_size != src_type_size)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "key size does not match source datatype size")

                /* Note we could reuse buffers here if we change around some of
                 * the logic in H5_daos_tconv_init() to support being passed the
                 * source buffer instead of the destination buffer */

                /* Copy data to type conversion buffer */
                (void)memcpy(tconv_buf, key, src_type_size);

                /* If needed, fill the background buffer (with zeros) */
                if(fill_bkg) {
                    assert(bkg_buf);
                    memset(bkg_buf, 0, dst_type_size);
                } /* end if */

                /* Perform type conversion */
                if(H5Tconvert(src_type_id, dst_type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

                /* Set return values to point to converted buffer */
                *key_buf = tconv_buf;
                /*if(tconv_buf != key) {*/
                    *key_buf_alloc = tconv_buf;
                    tconv_buf = NULL;
                /*}*/ /* end if */
            } /* end else */
        } /* end else */
    } /* end if */
    else
        /* Just return key */
        *key_buf = key;

done:
    /* Cleanup */
    if(src_parent_type_id > 0 && H5Tclose(src_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close source type parent type")
    if(dst_parent_type_id > 0 && H5Tclose(dst_parent_type_id) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close destination type parent type")
    if(tconv_buf && (tconv_buf != key))
        DV_free(tconv_buf);
    if(bkg_buf && (bkg_buf != key))
        DV_free(bkg_buf);

    D_FUNC_LEAVE
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
H5_daos_map_get_val(void *_map, hid_t key_mem_type_id, const void *key,
    hid_t val_mem_type_id, void *value, hid_t dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    const void *key_buf = NULL;
    void *key_buf_alloc = NULL;
    size_t key_size = 0;
    htri_t val_need_tconv;
    size_t val_mem_type_size = 0;
    size_t val_file_type_size = 0;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    H5_daos_tconv_reuse_t reuse = H5_DAOS_TCONV_REUSE_NONE;
    hbool_t fill_bkg = FALSE;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL")
    if(!value)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map value is NULL")

    /* Convert key (if necessary) */
    if(H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &key_buf, &key_size, &key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key_buf, (daos_size_t)key_size);

    /* Check if the type conversion is needed */
    if((val_need_tconv = H5_daos_need_tconv(map->val_file_type_id, val_mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")

    /* Type conversion */
    if(val_need_tconv) {
        /* Initialize type conversion */
        if(H5_daos_tconv_init(map->val_file_type_id, &val_file_type_size, val_mem_type_id, &val_mem_type_size, 1, FALSE, FALSE, &tconv_buf, &bkg_buf, &reuse, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion")

        /* Reuse buffer as appropriate */
        if(reuse == H5_DAOS_TCONV_REUSE_TCONV)
            tconv_buf = value;
        else if(reuse == H5_DAOS_TCONV_REUSE_BKG)
            bkg_buf = value;

        /* Fill background buffer if necessary */
        if(fill_bkg && (bkg_buf != value))
            (void)memcpy(bkg_buf, value, val_mem_type_size);

        /* Set up sgl_iov to point to tconv_buf */
        daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)val_file_type_size);
    } /* end if */
    else {
        /* Get datatype size */
        if((val_file_type_size = H5Tget_size(map->val_file_type_id)) == 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get datatype size for value datatype")

        /* Set up sgl_iov to point to value */
        daos_iov_set(&sg_iov, value, (daos_size_t)val_file_type_size);
    } /* end else */

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_map_key_g, H5_daos_map_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)val_file_type_size;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read value */
    if(0 != (ret = daos_obj_fetch(map->obj.obj_oh,
                   DAOS_TX_NONE, 0 /*flags*/, &dkey,
                   1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "MAP get failed: %s", H5_daos_err_to_string(ret));

    /* Check for no key-value pair found */
    if(iod.iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_MAP, H5E_NOTFOUND, FAIL, "key not found")

    /* Perform type conversion if necessary */
    if(val_need_tconv) {
        /* Type conversion */
        if(H5Tconvert(map->val_file_type_id, val_mem_type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

        /* Copy to user's buffer if necessary */
        if(value != tconv_buf)
            (void)memcpy(value, tconv_buf, val_mem_type_size);
    } /* end if */

done:
    /* Free memory */
    key_buf_alloc = DV_free(key_buf_alloc);
    if(tconv_buf && (tconv_buf != value))
        DV_free(tconv_buf);
    if(bkg_buf && (bkg_buf != value))
        DV_free(bkg_buf);

    D_FUNC_LEAVE
} /* end H5_daos_map_get_val() */


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
H5_daos_map_put(void *_map, hid_t key_mem_type_id, const void *key,
    hid_t val_mem_type_id, const void *value, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    const void *key_buf = NULL;
    void *key_buf_alloc = NULL;
    size_t key_size = 0;
    htri_t val_need_tconv;
    size_t val_mem_type_size = 0;
    size_t val_file_type_size = 0;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    hbool_t fill_bkg = FALSE;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL")
    if(!value)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map value is NULL")

    /* Check for write access */
    if(!(map->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Convert key (if necessary) */
    if(H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &key_buf, &key_size, &key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key_buf, (daos_size_t)key_size);

    /* Check if the type conversion is needed */
    if((val_need_tconv = H5_daos_need_tconv(map->val_file_type_id, val_mem_type_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOMPARE, FAIL, "can't check if type conversion is needed")

    /* Type conversion */
    if(val_need_tconv) {
        /* Initialize type conversion */
        if(H5_daos_tconv_init(val_mem_type_id, &val_mem_type_size, map->val_file_type_id, &val_file_type_size, 1, FALSE, TRUE, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't initialize type conversion")
    } /* end if */
    else
        /* Get datatype size */
        if((val_file_type_size = H5Tget_size(map->val_file_type_id)) == 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get datatype size for value datatype")

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_map_key_g, H5_daos_map_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)val_file_type_size;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up constant sgl info */
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Check for type conversion */
    if(val_need_tconv) {
        /* Check if we need to fill background buffer */
        if(fill_bkg) {
            assert(bkg_buf);

            /* Read data from map to background buffer */
            daos_iov_set(&sg_iov, bkg_buf, (daos_size_t)val_file_type_size);

            if(0 != (ret = daos_obj_fetch(map->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
                D_GOTO_ERROR(H5E_MAP, H5E_READERROR, FAIL, "can't read value from map: %s", H5_daos_err_to_string(ret))

            /* Reset iod_size, if the key was not created then it could have
             * been overwritten by daos_obj_fetch */
            iod.iod_size = (daos_size_t)val_file_type_size;
        } /* end if */

        /* Copy data to type conversion buffer */
        (void)memcpy(tconv_buf, value, (size_t)val_mem_type_size);

        /* Perform type conversion */
        if(H5Tconvert(val_mem_type_id, map->val_file_type_id, 1, tconv_buf, bkg_buf, dxpl_id) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

        /* Set sgl to write from tconv_buf */
        daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)val_file_type_size);
    } /* end if */
    else
        /* Set sgl to write from value */
        daos_iov_set(&sg_iov, (void *)value, (daos_size_t)val_file_type_size);

    /* Write key/value pair to map */
    if(0 != (ret = daos_obj_update(map->obj.obj_oh,
                   DAOS_TX_NONE, 0 /*flags*/, &dkey,
                   1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTSET, FAIL, "map put failed: %s", H5_daos_err_to_string(ret));

done:
    /* Free memory */
    key_buf_alloc = DV_free(key_buf_alloc);
    if(tconv_buf && (tconv_buf != value))
        DV_free(tconv_buf);
    if(bkg_buf && (bkg_buf != value))
        DV_free(bkg_buf);

    D_FUNC_LEAVE
} /* end H5_daos_map_put() */


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
H5_daos_map_exists(void *_map, hid_t key_mem_type_id, const void *key,
    hbool_t *exists, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    const void *key_buf = NULL;
    void *key_buf_alloc = NULL;
    size_t key_size = 0;
    daos_key_t dkey;
    daos_iod_t iod;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL")
    if(!exists)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map exists pointer is NULL")

    /* Convert key (if necessary) */
    if(H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &key_buf, &key_size, &key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key_buf, (daos_size_t)key_size);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_map_key_g, H5_daos_map_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    if(0 != (ret = daos_obj_fetch(map->obj.obj_oh,
                   DAOS_TX_NONE, 0 /*flags*/, &dkey,
                   1, &iod, NULL, NULL , NULL)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "MAP get failed: %s", H5_daos_err_to_string(ret));

    if(iod.iod_size != 0)
        *exists = TRUE;
    else
        *exists = FALSE;

done:
    /* Free memory */
    key_buf_alloc = DV_free(key_buf_alloc);

    D_FUNC_LEAVE
} /* end H5_daos_map_exists() */


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
H5_daos_map_get(void *_map, H5VL_map_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    herr_t       ret_value = SUCCEED;    /* Return value */

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")

    switch (get_type) {
        case H5VL_MAP_GET_MCPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the map's creation property list */
                if((*plist_id = H5Pcopy(map->mcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get map creation property list")

                /* Set map's object class on dcpl */
                if(H5_daos_set_oclass_from_oid(*plist_id, map->obj.oid) < 0)
                    D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property")

                break;
            } /* end block */
        case H5VL_MAP_GET_MAPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the map's access property list */
                if((*plist_id = H5Pcopy(map->mapl_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get map access property list")

                break;
            } /* end block */
        case H5VL_MAP_GET_KEY_TYPE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the map's key datatype */
                if((*ret_id = H5Tcopy(map->key_type_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get key datatype ID of map")
                break;
            } /* end block */
        case H5VL_MAP_GET_VAL_TYPE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the map's value datatype */
                if((*ret_id = H5Tcopy(map->val_type_id)) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get value datatype ID of map")
                break;
            } /* end block */
        case H5VL_MAP_GET_COUNT:
            {
                hsize_t *count = va_arg(arguments, hsize_t *);
                hsize_t idx = 0;
                hsize_t int_count = 0;

                /* Iterate over the keys, counting them */
                if(H5_daos_map_iterate(map, 0, &idx, map->key_type_id, H5_daos_map_get_count_cb, &int_count, dxpl_id, req) < 0)
                    D_GOTO_ERROR(H5E_MAP, H5E_BADITER, FAIL, "can't iterate over map keys")

                /* Set return value */
                *count = int_count;
                break;
            } /* end block */
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from map")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
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
H5_daos_map_get_count_cb(hid_t H5VL_DAOS_UNUSED map_id,
    const void H5VL_DAOS_UNUSED *key, void *_int_count)
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
H5_daos_map_specific(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_map_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_map_t *map = NULL;
    hid_t map_id = -1;
    herr_t ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    switch (specific_type) {
        /* H5Miterate(_by_name) */
        case H5VL_MAP_ITER:
        {
            hsize_t *idx = va_arg(arguments, hsize_t *);
            hid_t key_mem_type_id = va_arg(arguments, hid_t);
            H5M_iterate_t op = va_arg(arguments, H5M_iterate_t);
            void *op_data = va_arg(arguments, void *);

            switch (loc_params->type) {
                /* H5Miterate */
                case H5VL_OBJECT_BY_SELF:
                {
                    /* Use item as the map for iteration */
                    if(item->type != H5I_MAP)
                        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a map")

                    map = (H5_daos_map_t *)item;
                    map->obj.item.rc++;
                    break;
                } /* H5VL_OBJECT_BY_SELF */

                /* H5Miterate_by_name */
                case H5VL_OBJECT_BY_NAME:
                {
                    H5VL_loc_params_t sub_loc_params;

                    /* Open target_map */
                    sub_loc_params.obj_type = item->type;
                    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
                    if(NULL == (map = (H5_daos_map_t *)H5_daos_map_open(item, &sub_loc_params, loc_params->loc_data.loc_by_name.name, loc_params->loc_data.loc_by_name.lapl_id, dxpl_id, req)))
                        D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, FAIL, "can't open map for operation")

                    break;
                } /* H5VL_OBJECT_BY_NAME */

                case H5VL_OBJECT_BY_IDX:
                case H5VL_OBJECT_BY_TOKEN:
                default:
                    D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid loc_params type")
            } /* end switch */

            /* Register id for target_map */
            if((map_id = H5VLwrap_register(map, H5I_MAP)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")

            /* Perform map iteration */
            if((ret_value = H5_daos_map_iterate(map, map_id, idx, key_mem_type_id, op, op_data, dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, FAIL, "map iteration failed")

            break;
        } /* H5VL_MAP_ITER */

        case H5VL_MAP_DELETE:
        {
            hid_t key_mem_type_id = va_arg(arguments, hid_t);
            const void *key = va_arg(arguments, const void *);

            /* Verify loc_params */
            if(H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "unsupported map key delete location parameters type")
            map = (H5_daos_map_t *)item;
            map->obj.item.rc++;

            /* Perform key delete */
            if((ret_value = H5_daos_map_delete_key(map, key_mem_type_id, key,
                    dxpl_id, req)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTREMOVE, FAIL, "map key delete failed")

            break;
        } /* H5VL_MAP_DELETE */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported map specific operation")
    } /* end switch */

done:
    if(map_id >= 0) {
        if(H5Idec_ref(map_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map ID")
        map_id = -1;
        map = NULL;
    } /* end if */
    else if(map) {
        if(H5_daos_map_close(map, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map")
        map = NULL;
    } /* end else */

    D_FUNC_LEAVE
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
 * Return:      Success:        Last value returned by op (non-negative)
 *              Failure:        Last value returned by op (negative), or
 *                              -1 (error not caused by op)
 *
 *-------------------------------------------------------------------------
 */
static herr_t 
H5_daos_map_iterate(H5_daos_map_t *map, hid_t map_id, hsize_t *idx,
    hid_t key_mem_type_id, H5M_iterate_t op, void *op_data, hid_t dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    daos_anchor_t anchor;
    uint32_t nr;
    daos_key_desc_t *kds = NULL;
    size_t dkey_prefetch_size = 0;
    void *key_buf = NULL;
    void *key_buf_alloc = NULL;
    H5_daos_vl_union_t vl_union;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    herr_t op_ret;
    char tmp_char;
    char *dkey_buf = NULL;
    size_t dkey_alloc_size = 0;
    char *p;
    int ret;
    uint32_t i;
    herr_t ret_value = SUCCEED;

    assert(map);
    assert(map_id >= 0);
    if(!op)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "operator is NULL")

    /* Iteration restart not supported */
    if(idx && (*idx != 0))
        D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)")

    /* Get map iterate hints */
    if(H5Pget_map_iterate_hints(map->mapl_id, &dkey_prefetch_size, &dkey_alloc_size) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get map iterate hints")

    /* Initialize anchor */
    memset(&anchor, 0, sizeof(anchor));

    /* Allocate kds */
    if(NULL == (kds = (daos_key_desc_t *)DV_malloc(dkey_prefetch_size * sizeof(daos_key_desc_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkeys")

    /* Allocate dkey_buf */
    if(NULL == (dkey_buf = (char *)DV_malloc(dkey_alloc_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkeys")

    /* Set up list_sgl.  Report size as 1 less than buffer size so we
     * always have room for a null terminator. */
    daos_iov_set(&sg_iov, dkey_buf, (daos_size_t)(dkey_alloc_size - 1));
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Loop to retrieve keys and make callbacks */
    do {
        H5_DAOS_RETRIEVE_KEYS_LOOP(dkey_buf, dkey_alloc_size, sg_iov, nr, dkey_prefetch_size, H5E_MAP, daos_obj_list_dkey,
                map->obj.obj_oh, DAOS_TX_NONE, &nr, kds, &sgl, &anchor, NULL /*event*/);

        /* Loop over returned dkeys */
        p = dkey_buf;
        op_ret = 0;
        for(i = 0; (i < nr) && (op_ret == 0); i++) {
            /* Check for key sharing dkey with other metadata */
            if(((kds[i].kd_key_len == H5_daos_int_md_key_size_g)
                    && !memcmp(p, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g))
                    || ((kds[i].kd_key_len == H5_daos_attr_key_size_g)
                    && !memcmp(p, H5_daos_attr_key_g, H5_daos_attr_key_size_g))) {
                /* Set up dkey */
                daos_iov_set(&dkey, (void *)p, kds[i].kd_key_len);

                /* Set up iod */
                memset(&iod, 0, sizeof(iod));
                daos_iov_set(&iod.iod_name, (void *)H5_daos_map_key_g, H5_daos_map_key_size_g);
                iod.iod_nr = 1u;
                iod.iod_type = DAOS_IOD_SINGLE;
                iod.iod_size = DAOS_REC_ANY;

                /* Query map record in dkey */
                if(0 != (ret = daos_obj_fetch(map->obj.obj_oh, DAOS_TX_NONE,
                        0 /*flags*/, &dkey, 1, &iod, NULL, NULL , NULL)))
                    D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't check for value in map: %s", H5_daos_err_to_string(ret));

                /* If there is no value, skip this dkey */
                if(iod.iod_size == 0) {
                    /* Advance to next dkey */
                    p += kds[i].kd_key_len + kds[i].kd_csum_len;

                    continue;
                } /* end if */
            } /* end if */

            /* Add null terminator temporarily.  Only necessary for VL strings
             * but it would take about as much time to check for VL string again
             * after the callback as it does to just always swap in the null
             * terminator so just do this for simplicity. */
            tmp_char = p[kds[i].kd_key_len];
            p[kds[i].kd_key_len] = '\0';

            /* Convert key (if necessary) */
            if(H5_daos_map_key_conv_reverse(map->key_file_type_id, key_mem_type_id, p, (size_t)kds[i].kd_key_len, &key_buf, &key_buf_alloc, &vl_union, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key")

            /* Make callback */
            if((op_ret = op(map_id, key_buf, op_data)) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, op_ret, "operator function returned failure")

            /* Replace null terminator */
            p[kds[i].kd_key_len] = tmp_char;

            /* Free allocated buffer */
            /* While the free/alloc cycle for the key buffer could be a
             * performance problem, this will only happen when the key types
             * require conversion and are not vlen, or if they are vlen and the
             * parent types require conversion (it will never happen for
             * strings), which should be a rare case. */
            if(key_buf_alloc)
                key_buf_alloc = DV_free(key_buf_alloc);

            /* Advance idx */
            if(idx)
                (*idx)++;

            /* Advance to next dkey */
            p += kds[i].kd_key_len + kds[i].kd_csum_len;
        } /* end for */
    } while(!daos_anchor_is_eof(&anchor) && (op_ret == 0));

    ret_value = op_ret;

done:
    kds = (daos_key_desc_t *)DV_free(kds);
    dkey_buf = (char *)DV_free(dkey_buf);
    key_buf_alloc = DV_free(key_buf_alloc);

    D_FUNC_LEAVE
} /* end H5_daos_map_iterate() */


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
H5_daos_map_delete_key(H5_daos_map_t *map, hid_t key_mem_type_id,
    const void *key, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    const void *key_buf = NULL;
    void *key_buf_alloc = NULL;
    size_t key_size;
    daos_key_t dkey;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL")

    /* Check for write access */
    if(!(map->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Convert key (if necessary) */
    if(H5_daos_map_key_conv(key_mem_type_id, map->key_file_type_id, key, &key_buf, &key_size, &key_buf_alloc, dxpl_id) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "can't convert key")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key_buf, (daos_size_t)key_size);

    /* Check for key sharing dkey with other metadata.  If dkey is shared, only
     * delete akey, otherwise delete dkey. */
    if(((key_size == H5_daos_int_md_key_size_g)
            && !memcmp(key, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g))
            || ((key_size == H5_daos_attr_key_size_g)
            && !memcmp(key, H5_daos_attr_key_g, H5_daos_attr_key_size_g))) {
        daos_key_t akey;

        /* Set up akey */
        daos_iov_set(&akey, (void *)H5_daos_map_key_g, H5_daos_map_key_size_g);

        /* Delete key/value pair from map */
        if(0 != (ret = daos_obj_punch_akeys(map->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey,
               1, &akey, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTSET, FAIL, "map akey delete failed: %s", H5_daos_err_to_string(ret));
    } /* end if */
    else
        /* Delete dkey from map */
        if(0 != (ret = daos_obj_punch_dkeys(map->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, 1,
                &dkey, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTSET, FAIL, "map dkey delete failed: %s", H5_daos_err_to_string(ret));

done:
    /* Free memory */
    key_buf_alloc = DV_free(key_buf_alloc);

    D_FUNC_LEAVE
} /* end H5_daos_map_delete_key() */


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
H5_daos_map_close(void *_map, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")

    if(--map->obj.item.rc == 0) {
        /* Free map data structures */
        if(map->obj.item.open_req)
            if(H5_daos_req_free_int(map->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't free request")
        if(!daos_handle_is_inval(map->obj.obj_oh))
            if(0 != (ret = daos_obj_close(map->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_MAP, H5E_CANTCLOSEOBJ, FAIL, "can't close map DAOS object: %s", H5_daos_err_to_string(ret))
        if(map->key_type_id != FAIL && H5Idec_ref(map->key_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(map->key_file_type_id != FAIL && H5Idec_ref(map->key_file_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(map->val_type_id != FAIL && H5Idec_ref(map->val_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(map->val_file_type_id != FAIL && H5Idec_ref(map->val_file_type_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(map->mcpl_id != FAIL && H5Idec_ref(map->mcpl_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close mcpl")
        if(map->mapl_id != FAIL && H5Idec_ref(map->mapl_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close mapl")
        map = H5FL_FREE(H5_daos_map_t, map);
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_map_close() */
