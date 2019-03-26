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
    hid_t mcpl_id, hid_t mapl_id, hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_map_t *map = NULL;
    hid_t ktype_id, vtype_id;
    H5_daos_group_t *target_grp = NULL;
    void *ktype_buf = NULL;
    void *vtype_buf = NULL;
    void *mcpl_buf = NULL;
    hbool_t collective;
    H5_daos_md_update_cb_ud_t *update_cb_ud = NULL;
    hbool_t update_task_scheduled = FALSE;
    tse_task_t *finalize_task;
    int finalize_ndeps = 0;
    tse_task_t *finalize_deps[2];
    H5_daos_req_t *int_req = NULL;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map parent object is NULL")

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")

    /* Check for collective access, if not already set by the file */
    collective = item->file->collective;
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(mapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, NULL, "can't get collective access property")

    /* Get creation properties */
    if(H5Pget(mcpl_id, H5VL_PROP_MAP_KEY_TYPE_ID, &ktype_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for key datatype ID")
    if(H5Pget(mcpl_id, H5VL_PROP_MAP_VAL_TYPE_ID, &vtype_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for value datatype ID")

    /* Start H5 operation */
    if(NULL == (int_req = (H5_daos_req_t *)DV_malloc(sizeof(H5_daos_req_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for request")
    int_req->th = DAOS_TX_NONE;
    int_req->th_open = FALSE;
    int_req->file = item->file;
    int_req->file->item.rc++;
    int_req->rc = 1;
    int_req->status = H5_DAOS_INCOMPLETE;
    int_req->failed_task = NULL;

    /* Allocate the map object that is returned to the user */
    if(NULL == (map = H5FL_CALLOC(H5_daos_map_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS map struct")
    map->obj.item.type = H5I_MAP;
    map->obj.item.open_req = int_req;
    int_req->rc++;
    map->obj.item.file = item->file;
    map->obj.item.rc = 1;
    map->obj.obj_oh = DAOS_HDL_INVAL;
    map->ktype_id = FAIL;
    map->vtype_id = FAIL;
    map->mcpl_id = FAIL;
    map->mapl_id = FAIL;

    /* Generate map oid */
    H5_daos_oid_encode(&map->obj.oid, item->file->max_oid + (uint64_t)1, H5I_MAP);

    /* Create map and write metadata if this process should */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        size_t mcpl_size = 0;
        size_t ktype_size = 0;
        size_t vtype_size = 0;
        tse_task_t *update_task;
        tse_task_t *link_write_task;

        /* Start transaction */
        if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't start transaction")
        int_req->th_open = TRUE;

        /* Traverse the path */
        if(name)
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, NULL, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, NULL, "can't traverse path")

        /* Create map */
        /* Update max_oid */
        item->file->max_oid = H5_daos_oid_to_idx(map->obj.oid);

        /* Write max OID */
        if(H5_daos_write_max_oid(item->file) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't write max OID")

        /* Allocate argument struct */
        if(NULL == (update_cb_ud = (H5_daos_md_update_cb_ud_t *)DV_calloc(sizeof(H5_daos_md_update_cb_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for update callback arguments")

        /* Open map */
        if(0 != (ret = daos_obj_open(item->file->coh, map->obj.oid, DAOS_OO_RW, &map->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map: %s", H5_daos_err_to_string(ret))
        map->obj.item.rc++;

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
        if(H5Pencode(mcpl_id, NULL, &mcpl_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of mcpl")
        if(NULL == (mcpl_buf = DV_malloc(mcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized mcpl")
        if(H5Pencode(mcpl_id, mcpl_buf, &mcpl_size) < 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTENCODE, NULL, "can't serialize mcpl")

        /* Set up operation to write MCPl and datatypes to map */
        /* Point to req */
        update_cb_ud->req = int_req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        update_cb_ud->free_dkey = FALSE;

        /* The elements in iod and sgl */
        update_cb_ud->nr = 3u;

        /* Set up iod */
        /* Key datatype.  Point akey to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->iod[0].iod_name, (void *)H5_daos_ktype_g, H5_daos_ktype_size_g);
        daos_csum_set(&update_cb_ud->iod[0].iod_kcsum, NULL, 0);
        update_cb_ud->iod[0].iod_nr = 1u;
        update_cb_ud->iod[0].iod_size = (uint64_t)ktype_size;
        update_cb_ud->iod[0].iod_type = DAOS_IOD_SINGLE;

        /* Value datatype */
        daos_iov_set(&update_cb_ud->iod[1].iod_name, (void *)H5_daos_vtype_g, H5_daos_vtype_size_g);
        daos_csum_set(&update_cb_ud->iod[1].iod_kcsum, NULL, 0);
        update_cb_ud->iod[1].iod_nr = 1u;
        update_cb_ud->iod[1].iod_size = (uint64_t)vtype_size;
        update_cb_ud->iod[1].iod_type = DAOS_IOD_SINGLE;

        /* MCPL */
        daos_iov_set(&update_cb_ud->iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        daos_csum_set(&update_cb_ud->iod[2].iod_kcsum, NULL, 0);
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

        /* Set private data for group metadata write */
        (void)tse_task_set_priv(update_task, update_cb_ud);

        /* Schedule map metadata write task and give it a reference to req */
        if(0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't schedule task to write map metadata: %s", H5_daos_err_to_string(ret))
        update_task_scheduled = TRUE;
        update_cb_ud->req->rc++;

        /* Add dependency for finalize task */
        finalize_deps[finalize_ndeps] = update_task;
        finalize_ndeps++;

        /* Create link to map */
        if(target_grp) {
            H5_daos_link_val_t link_val;

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = map->obj.oid;
            if(H5_daos_link_write(target_grp, target_name, strlen(target_name), &link_val, int_req, &link_write_task) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't create link to map")
            finalize_deps[finalize_ndeps] = link_write_task;
            finalize_ndeps++;
        } /* end if */
    } /* end if */
    else {
        /* Update max_oid */
        item->file->max_oid = map->obj.oid.lo;

        /* Open map */
        if(0 != (ret = daos_obj_open(item->file->coh, map->obj.oid, DAOS_OO_RW, &map->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map: %s", H5_daos_err_to_string(ret))
    } /* end else */

    /* Finish setting up map struct */
    if((map->ktype_id = H5Tcopy(ktype_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((map->vtype_id = H5Tcopy(vtype_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((map->mcpl_id = H5Pcopy(mcpl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gcpl");
    if((map->mapl_id = H5Pcopy(mapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Set return value */
    ret_value = (void *)map;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close group")

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

        /* Block until operation completes */
        {
            bool is_empty;

            /* Wait for scheduler to be empty *//* Change to custom progress function DSINC */
            if(0 != (ret = daos_progress(&item->file->sched, DAOS_EQ_WAIT, &is_empty)))
                D_DONE_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't progress scheduler: %s", H5_daos_err_to_string(ret))

            /* Check for failure */
            if(int_req->status < 0)
                D_DONE_ERROR(H5E_MAP, H5E_CANTOPERATE, NULL, "map creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))
        } /* end block */

        /* Close internal request */
        H5_daos_req_free_int(int_req);
    } /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value) {
        /* Close map */
        if(map && H5_daos_map_close(map, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CLOSEERROR, NULL, "can't close map");

        /* Free memory */
        if(!update_task_scheduled) {
            if(update_cb_ud && update_cb_ud->obj && H5_daos_object_close(update_cb_ud->obj, dxpl_id, NULL) < 0)
                D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close object")
            ktype_buf = DV_free(ktype_buf);
            vtype_buf = DV_free(vtype_buf);
            mcpl_buf = DV_free(mcpl_buf);
            update_cb_ud = DV_free(update_cb_ud);
        } /* end if */
    } /* end if */
    else
        assert((!ktype_buf && !vtype_buf && !mcpl_buf) || update_task_scheduled);

    /* Free memory */
    ktype_buf = DV_free(ktype_buf);
    vtype_buf = DV_free(vtype_buf);
    mcpl_buf = DV_free(mcpl_buf);

    D_FUNC_LEAVE_API
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
    daos_key_t dkey;
    daos_iod_t iod[3];
    daos_sg_list_t sgl[3];
    daos_iov_t sg_iov[3];
    uint64_t ktype_len = 0;
    uint64_t vtype_len = 0;
    uint64_t mcpl_len = 0;
    uint64_t tot_len;
    uint8_t minfo_buf_static[H5_DAOS_DINFO_BUF_SIZE];
    uint8_t *minfo_buf_dyn = NULL;
    uint8_t *minfo_buf = minfo_buf_static;
    uint8_t *p;
    hbool_t collective;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map parent object is NULL")

    /* Check for collective access, if not already set by the file */
    collective = item->file->collective;
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
    map->ktype_id = FAIL;
    map->vtype_id = FAIL;
    map->mcpl_id = FAIL;
    map->mapl_id = FAIL;

    /* Check if we're actually opening the group or just receiving the map
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by address */
        if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
            /* Generate oid from address */
            H5_daos_oid_generate(&map->obj.oid, (uint64_t)loc_params->loc_data.loc_by_addr.addr, H5I_MAP);
        } /* end if */
        else {
            /* Open using name parameter */
            if(H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "unsupported map open location parameters type")
            if(!name)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "map name is NULL")

            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_MAP, H5E_BADITER, NULL, "can't traverse path")

            /* Follow link to map */
            if(H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &map->obj.oid) < 0)
                D_GOTO_ERROR(H5E_MAP, H5E_CANTINIT, NULL, "can't follow link to map")
        } /* end else */

        /* Open map */
        if(0 != (ret = daos_obj_open(item->file->coh, map->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &map->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, NULL, "can't open map: %s", H5_daos_err_to_string(ret))

        /* Set up operation to read datatype and MCPL sizes from map */
        /* Set up dkey */
        daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, H5_daos_ktype_g, H5_daos_ktype_size_g);
        daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = DAOS_REC_ANY;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, H5_daos_vtype_g, H5_daos_vtype_size_g);
        daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = DAOS_REC_ANY;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
        iod[2].iod_nr = 1u;
        iod[2].iod_size = DAOS_REC_ANY;
        iod[2].iod_type = DAOS_IOD_SINGLE;

        /* Read internal metadata sizes from map */
        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh, DAOS_TX_NONE, &dkey, 3, iod, NULL,
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
        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh, DAOS_TX_NONE, &dkey, 3, iod, sgl, NULL /*maps*/, NULL /*event*/)))
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
    if((map->ktype_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += ktype_len;
    if((map->vtype_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += vtype_len;
    if((map->mcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize map creation property list")

    /* Finish setting up map struct */
    if((map->mapl_id = H5Pcopy(mapl_id)) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTCOPY, NULL, "failed to copy mapl");

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

    /* Free memory */
    minfo_buf_dyn = (uint8_t *)DV_free(minfo_buf_dyn);

    D_FUNC_LEAVE_API
} /* end H5_daos_map_open() */

#ifdef DV_HAVE_MAP

/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_get_size
 *
 * Purpose:     Retrieves the size of a Key or Value binary 
 *              buffer given its datatype and buffer contents.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_get_size(hid_t type_id, const void *buf,
    /*out*/uint64_t H5VL_DAOS_UNUSED *checksum,  /*out*/size_t *size,
    /*out*/H5T_class_t *ret_class)
{
    size_t buf_size = 0;
    H5T_t *dt = NULL;
    H5T_class_t dt_class;
    herr_t ret_value = SUCCEED;

    assert(buf);
    assert(size);

    if(NULL == (dt = (H5T_t *)H5Iobject_verify(type_id, H5I_DATATYPE)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NO_CLASS, "not a datatype")

    dt_class = H5T_get_class(dt, FALSE);

    switch(dt_class) {
        case H5T_STRING:
            /* If this is a variable length string, get the size using strlen(). */
            if(H5T_is_variable_str(dt)) {
                buf_size = strlen((const char*)buf) + 1;

                break;
            }
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_ENUM:
        case H5T_ARRAY:
        case H5T_NO_CLASS:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        case H5T_COMPOUND:
            /* Data is not variable length, so use H5Tget_size() */
            /* MSC - This is not correct. Compound/Array can contian
               VL datatypes, but for now we don't support that. Need
               to check for that too */
            buf_size = H5T_get_size(dt);

            break;

            /* If this is a variable length datatype, iterate over it */
        case H5T_VLEN:
            {
                H5T_t *super = NULL;
                const hvl_t *vl;

                vl = (const hvl_t *)buf;

                if(NULL == (super = H5T_get_super(dt)))
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid super type of VL type");

                buf_size = H5T_get_size(super) * vl->len;
                H5T_close(super);
                break;
            } /* end block */
        default:
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "unsupported datatype");
    } /* end switch */

    *size = buf_size;
    if(ret_class)
        *ret_class = dt_class;

done:
    D_FUNC_LEAVE
} /* end H5_daos_map_get_size */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_map_dtype_info
 *
 * Purpose:     Retrieves information about the datatype of Map Key or
 *              value datatype, whether it's VL or not. If it is not VL
 *              return the size.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_map_dtype_info(hid_t type_id, hbool_t *is_vl, size_t *size,
    H5T_class_t *cls)
{
    size_t buf_size = 0;
    H5T_t *dt = NULL;
    H5T_class_t dt_class;
    herr_t ret_value = SUCCEED;

    assert(is_vl);

    if(NULL == (dt = (H5T_t *)H5Iobject_verify(type_id, H5I_DATATYPE)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NO_CLASS, "not a datatype")

    dt_class = H5T_get_class(dt, FALSE);

    switch(dt_class) {
        case H5T_STRING:
            /* If this is a variable length string, get the size using strlen(). */
            if(H5T_is_variable_str(dt)) {
                *is_vl = TRUE;
                break;
            }
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_ENUM:
        case H5T_ARRAY:
        case H5T_NO_CLASS:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        case H5T_COMPOUND:
            /* Data is not variable length, so use H5Tget_size() */
            /* MSC - This is not correct. Compound/Array can contian
               VL datatypes, but for now we don't support that. Need
               to check for that too */
            buf_size = H5T_get_size(dt);
            *is_vl = FALSE;
            break;

            /* If this is a variable length datatype, iterate over it */
        case H5T_VLEN:
            *is_vl = TRUE;
            break;
        default:
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "unsupported datatype");
    }

    if(size)
        *size = buf_size;
    if(cls)
        *cls = dt_class;
done:
    D_FUNC_LEAVE
} /* end H5_daos_map_dtype_info */


herr_t 
H5_daos_map_set(void *_map, hid_t key_mem_type_id, const void *key,
    hid_t val_mem_type_id, const void *value, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    size_t key_size, val_size;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    H5T_class_t cls;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL")
    if(!value)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map value is NULL")

    /* get the key size and checksum from the provdied key datatype & buffer */
    if(H5_daos_map_get_size(key_mem_type_id, key, NULL, &key_size, NULL) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get key size");

    /* get the val size and checksum from the provdied val datatype & buffer */
    if(H5_daos_map_get_size(val_mem_type_id, value, NULL, &val_size, &cls) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get val size");

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key, (daos_size_t)key_size);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_map_key_g, H5_daos_map_key_size_g);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)val_size;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    if (H5T_VLEN == cls) {
        hvl_t *vl_buf = (hvl_t *)value;

        daos_iov_set(&sg_iov, (void *)vl_buf->p, (daos_size_t)val_size);
    }
    else {
        daos_iov_set(&sg_iov, (void *)value, (daos_size_t)val_size);
    }

    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    if(0 != (ret = daos_obj_update(map->obj.obj_oh,
                   DAOS_TX_NONE, &dkey,
                   1, &iod, &sgl, NULL)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTSET, FAIL, "Map set failed: %s", H5_daos_err_to_string(ret));

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_map_set() */


herr_t 
H5_daos_map_get(void *_map, hid_t key_mem_type_id, const void *key,
    hid_t val_mem_type_id, void *value, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    size_t key_size, val_size;
    hbool_t val_is_vl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    H5T_class_t cls;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!key)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map key is NULL")
    if(!value)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map value is NULL")

    /* get the key size and checksum from the provdied key datatype & buffer */
    if(H5_daos_map_get_size(key_mem_type_id, key, NULL, &key_size, NULL) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get key size");

    /* get information about the datatype of the value. Get the values
       size if it is not VL. val_size will be 0 if it is VL */
    if(H5_daos_map_dtype_info(val_mem_type_id, &val_is_vl, &val_size, &cls) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get key size");

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key, (daos_size_t)key_size);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_map_key_g, H5_daos_map_key_size_g);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_type = DAOS_IOD_SINGLE;

    if (!val_is_vl) {
        iod.iod_size = (daos_size_t)val_size;

        /* Set up sgl */
        daos_iov_set(&sg_iov, value, (daos_size_t)val_size);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh,
                       DAOS_TX_NONE, &dkey,
                       1, &iod, &sgl, NULL , NULL)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "MAP get failed: %s", H5_daos_err_to_string(ret));
    }
    else {
        iod.iod_size = DAOS_REC_ANY;
        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh,
                       DAOS_TX_NONE, &dkey,
                       1, &iod, NULL, NULL , NULL)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "MAP get failed: %s", H5_daos_err_to_string(ret));

        val_size = iod.iod_size;

        if(cls == H5T_STRING) {
            char *val;

            val = (char *)malloc(val_size);
            daos_iov_set(&sg_iov, val, (daos_size_t)val_size);
            (*(void **) value) = val;
        }
        else {
            hvl_t *vl_buf = (hvl_t *)value;

            assert(H5T_VLEN == cls);

            vl_buf->len = val_size;
            vl_buf->p = malloc(val_size);
            daos_iov_set(&sg_iov, vl_buf->p, (daos_size_t)val_size);
        }

        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        if(0 != (ret = daos_obj_fetch(map->obj.obj_oh,
                       DAOS_TX_NONE, &dkey,
                       1, &iod, &sgl, NULL , NULL)))
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "MAP get failed: %s", H5_daos_err_to_string(ret));
    }

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_map_get() */


herr_t 
H5_daos_map_get_types(void *_map, hid_t *key_type_id, hid_t *val_type_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    herr_t ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")

    if(key_type_id)
        if((*key_type_id = H5Tcopy(map->ktype_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get datatype ID of map key");

    if(val_type_id)
        if((*val_type_id = H5Tcopy(map->vtype_id)) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get datatype ID of map val");

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_map_get_types() */


#define ENUM_DESC_BUF   512
#define ENUM_DESC_NR    5


herr_t 
H5_daos_map_get_count(void *_map, hsize_t *count, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    char        *buf;
    daos_key_desc_t  kds[ENUM_DESC_NR];
    daos_anchor_t  anchor;
    uint32_t     number;
    hsize_t      key_nr;
    daos_sg_list_t   sgl;
    daos_iov_t   sg_iov;
    int ret;
    herr_t       ret_value = SUCCEED;

    if(!_map)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map object is NULL")
    if(!count)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "count is NULL")

    memset(&anchor, 0, sizeof(anchor));
    buf = (char *)malloc(ENUM_DESC_BUF);

    daos_iov_set(&sg_iov, buf, ENUM_DESC_BUF);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    for (number = ENUM_DESC_NR, key_nr = 0; !daos_anchor_is_eof(&anchor);
            number = ENUM_DESC_NR) {
        memset(buf, 0, ENUM_DESC_BUF);

        ret = daos_obj_list_dkey(map->obj.obj_oh,
                     DAOS_TX_NONE,
                     &number, kds, &sgl, &anchor, NULL);
        if(ret != 0)
            D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "Map List failed: %s", H5_daos_err_to_string(ret));
        if (number == 0)
            continue; /* loop should break for EOF */

        key_nr += (hsize_t)number;
    }

    /* -1 for MD dkey */
    *count = (hsize_t)(key_nr - 1);

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_map_get_count() */


herr_t 
H5_daos_map_exists(void *_map, hid_t key_mem_type_id, const void *key,
    hbool_t *exists, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_map_t *map = (H5_daos_map_t *)_map;
    size_t key_size;
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

    /* get the key size and checksum from the provdied key datatype & buffer */
    if(H5_daos_map_get_size(key_mem_type_id, key, NULL, &key_size, NULL) < 0)
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get key size");

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)key, (daos_size_t)key_size);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_map_key_g, H5_daos_map_key_size_g);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_type = DAOS_IOD_SINGLE;
    iod.iod_size = DAOS_REC_ANY;

    if(0 != (ret = daos_obj_fetch(map->obj.obj_oh,
                   DAOS_TX_NONE, &dkey,
                   1, &iod, NULL, NULL , NULL)))
        D_GOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "MAP get failed: %s", H5_daos_err_to_string(ret));

    if(iod.iod_size != 0)
        *exists = TRUE;
    else
        *exists = FALSE;

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_map_exists() */
#endif /* DV_HAVE_MAP */


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
            H5_daos_req_free_int(map->obj.item.open_req);
        if(!daos_handle_is_inval(map->obj.obj_oh))
            if(0 != (ret = daos_obj_close(map->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_MAP, H5E_CANTCLOSEOBJ, FAIL, "can't close map DAOS object: %s", H5_daos_err_to_string(ret))
        if(map->ktype_id != FAIL && H5Idec_ref(map->ktype_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(map->vtype_id != FAIL && H5Idec_ref(map->vtype_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(map->mcpl_id != FAIL && H5Idec_ref(map->mcpl_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close mcpl")
        if(map->mapl_id != FAIL && H5Idec_ref(map->mapl_id) < 0)
            D_DONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "failed to close mapl")
        map = H5FL_FREE(H5_daos_map_t, map);
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_map_close() */

