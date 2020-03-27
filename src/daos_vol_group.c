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
 * library. Group routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_group_fill_gcpl_cache(H5_daos_group_t *grp);
static int H5_daos_group_open_bcast_comp_cb(tse_task_t *task, void *args);
static int H5_daos_group_open_recv_comp_cb(tse_task_t *task, void *args);
static herr_t H5_daos_group_get_info(H5_daos_group_t *grp, const H5VL_loc_params_t *loc_params,
    H5G_info_t *group_info, hid_t dxpl_id, void **req);


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_traverse
 *
 * Purpose:     Given a path name and base object, returns the final group
 *              in the path and the object name.  obj_name points into the
 *              buffer given by path, so it does not need to be freed.
 *              The group must be closed with H5_daos_group_close().
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
H5_daos_group_t *
H5_daos_group_traverse(H5_daos_item_t *item, const char *path,
    hid_t lcpl_id, hid_t dxpl_id, void **req, const char **obj_name,
    void **gcpl_buf_out, uint64_t *gcpl_len_out)
{
    H5_daos_group_t *grp = NULL;
    const char *next_obj;
    daos_obj_id_t oid;
    unsigned crt_intermed_grp;
    char *tmp_grp_name = NULL;
    H5_daos_group_t *ret_value = NULL;

    assert(item);
    assert(path);
    assert(obj_name);

    /* Determine if intermediate groups should be created */
    if((H5P_LINK_CREATE_DEFAULT != lcpl_id) && H5Pget_create_intermediate_group(lcpl_id, &crt_intermed_grp) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get intermediate group creation property value")

    /* Initialize obj_name */
    *obj_name = path;

    /* Open starting group */
    if((*obj_name)[0] == '/') {
        grp = item->file->root_grp;
        (*obj_name)++;
    } /* end if */
    else {
        /* Check for the leading './' case */
        if ((*obj_name)[0] == '.' && (*obj_name)[1] == '/') {
            /*
             * Advance past the leading '.' and '/' characters.
             * Note that the case of multiple leading '.' characters
             * is not currently handled.
             */
            (*obj_name)++; (*obj_name)++;
        }

        if(item->type == H5I_GROUP)
            grp = (H5_daos_group_t *)item;
        else if(item->type == H5I_FILE)
            grp = ((H5_daos_file_t *)item)->root_grp;
        else
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "item not a file or group")
    } /* end else */

    grp->obj.item.rc++;

    /* Search for '/' */
    next_obj = strchr(*obj_name, '/');

    /* Traverse path */
    while(next_obj) {
        htri_t link_resolved;

        /* Free gcpl_buf_out */
        if(gcpl_buf_out)
            *gcpl_buf_out = DV_free(*gcpl_buf_out);

        /* Follow link to next group in path */
        assert(next_obj > *obj_name);
        if((link_resolved = H5_daos_link_follow(grp, *obj_name, (size_t)(next_obj - *obj_name), dxpl_id, req, &oid)) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, NULL, "can't follow link to group")
        if(link_resolved) {
            /* Close previous group */
            if(H5_daos_group_close(grp, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")
            grp = NULL;

            /* Open next group in path */
            if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_open_helper(item->file, oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, NULL, gcpl_buf_out, gcpl_len_out)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group")
        } /* end if */
        else {
            H5_daos_group_t *temp_group;
            H5VL_loc_params_t tmp_loc_params;

            /* Link didn't resolve - this is an error unless intermediate group creation is enabled. */
            if(!crt_intermed_grp)
                D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, NULL, "link to group did not resolve")

            /* Setup temporary loc_params needed for group creation */
            tmp_loc_params.type = H5VL_OBJECT_BY_SELF;
            tmp_loc_params.obj_type = H5I_GROUP;

            /* Create a copy of the current object name which only
             * contains the name of the next link in the path.
             */
            if(NULL == (tmp_grp_name = DV_malloc((size_t)((next_obj - *obj_name + 1)))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for intermediate group's name")
            strncpy(tmp_grp_name, *obj_name, (size_t)(next_obj - *obj_name));
            tmp_grp_name[next_obj - *obj_name] = '\0';

            /* Create this intermediate group. */
            if(NULL == (temp_group = H5_daos_group_create(grp, &tmp_loc_params, tmp_grp_name,
                    lcpl_id, grp->gcpl_id, grp->gapl_id, dxpl_id, NULL)))
                D_GOTO_ERROR(H5E_SYM, H5E_PATH, NULL, "failed to create intermediate group")

            tmp_grp_name = DV_free(tmp_grp_name);

            /* Close previous group */
            if(H5_daos_group_close(grp, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

            /* Set current group to newly-created intermediate group */
            grp = temp_group;
        } /* end else */

        /* Advance to next path element */
        *obj_name = next_obj + 1;
        next_obj = strchr(*obj_name, '/');
    } /* end while */

    /* Set return value */
    ret_value = grp;

done:
    tmp_grp_name = DV_free(tmp_grp_name);

    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_group_traverse() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_fill_gcpl_cache
 *
 * Purpose:     Fills the "gcpl_cache" field of the group struct, using
 *              the group's GCPL.  Assumes grp->gcpl_cache has been
 *              initialized to all zeros.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              August, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_fill_gcpl_cache(H5_daos_group_t *grp)
{
    unsigned corder_flags;
    herr_t ret_value = SUCCEED;

    assert(grp);

    /* Determine if this group is tracking link creation order */
    if(H5Pget_link_creation_order(grp->gcpl_id, &corder_flags) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't get link creation order flags")
    assert(!grp->gcpl_cache.track_corder);
    if(corder_flags & H5P_CRT_ORDER_TRACKED)
        grp->gcpl_cache.track_corder = TRUE;

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_fill_gcpl_cache() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_create_helper
 *
 * Purpose:     Performs the actual group creation.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_create_helper(H5_daos_file_t *file, hid_t gcpl_id,
    hid_t gapl_id, hid_t dxpl_id, H5_daos_req_t *req,
    H5_daos_group_t *parent_grp, const char *name, size_t name_len,
    uint64_t oidx, hbool_t collective, tse_task_t **first_task,
    tse_task_t **dep_task)
{
    H5_daos_group_t *grp = NULL;
    void *gcpl_buf = NULL;
    H5_daos_md_rw_cb_ud_t *update_cb_ud = NULL;
    tse_task_t *group_metatask;
    int gmt_ndeps = 0;
    tse_task_t *gmt_deps[2];
    int ret;
    void *ret_value = NULL;

    assert(file);
    assert(file->flags & H5F_ACC_RDWR);
    assert(first_task);
    assert(dep_task);

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.open_req = req;
    req->rc++;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.obj_oh = DAOS_HDL_INVAL;
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Encode group oid */
    if(H5_daos_oid_encode(&grp->obj.oid, oidx, H5I_GROUP, gcpl_id == H5P_GROUP_CREATE_DEFAULT ? H5P_DEFAULT : gcpl_id, H5_DAOS_OBJ_CLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode object ID")

    /* Open group object */
    if(H5_daos_obj_open(file, req, &grp->obj.oid, DAOS_OO_RW, &grp->obj.obj_oh, "group object open", first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group object")

    /* Create group and write metadata if this process should */
    if(!collective || (file->my_rank == 0)) {
        size_t gcpl_size = 0;
        tse_task_t *update_task;
        tse_task_t *link_write_task;

        /* Create group */
        /* Allocate argument struct */
        if(NULL == (update_cb_ud = (H5_daos_md_rw_cb_ud_t *)DV_calloc(sizeof(H5_daos_md_rw_cb_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for update callback arguments")

        /* Encode GCPL */
        if(H5Pencode2(gcpl_id, NULL, &gcpl_size, file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl")
        if(NULL == (gcpl_buf = DV_malloc(gcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
        if(H5Pencode2(gcpl_id, gcpl_buf, &gcpl_size, file->fapl_id) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl")

        /* Set up operation to write GCPL to group */
        /* Point to grp */
        update_cb_ud->obj = &grp->obj;

        /* Point to req */
        update_cb_ud->req = req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        update_cb_ud->free_dkey = FALSE;

        /* Single iod and sgl */
        update_cb_ud->nr = 1u;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_iov_set(&update_cb_ud->iod[0].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        update_cb_ud->iod[0].iod_nr = 1u;
        update_cb_ud->iod[0].iod_size = (uint64_t)gcpl_size;
        update_cb_ud->iod[0].iod_type = DAOS_IOD_SINGLE;
        update_cb_ud->free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->sg_iov[0], gcpl_buf, (daos_size_t)gcpl_size);
        update_cb_ud->sgl[0].sg_nr = 1;
        update_cb_ud->sgl[0].sg_nr_out = 0;
        update_cb_ud->sgl[0].sg_iovs = &update_cb_ud->sg_iov[0];

        /* Set task name */
        update_cb_ud->task_name = "group metadata write";

        /* Create task for group metadata write */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_UPDATE, &file->sched, 0, NULL, &update_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to write group medadata: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        assert(*dep_task);
        if(0 != (ret = tse_task_register_deps(update_task, 1, dep_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for group metadata write: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for group metadata write */
        if(0 != (ret = tse_task_register_cbs(update_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_md_update_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't register callbacks for task to write group medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for group metadata write */
        (void)tse_task_set_priv(update_task, update_cb_ud);

        /* Schedule group metadata write task and give it a reference to req and
         * the group */
        assert(*first_task);
        if(0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to write group metadata: %s", H5_daos_err_to_string(ret))
        req->rc++;
        grp->obj.item.rc++;
        update_cb_ud = NULL;
        gcpl_buf = NULL;

        /* Add dependency for group metatask */
        gmt_deps[gmt_ndeps] = update_task;
        gmt_ndeps++;

        /* Write link to group if requested */
        if(parent_grp) {
            H5_daos_link_val_t link_val;

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = grp->obj.oid;
            if(H5_daos_link_write(parent_grp, name, name_len, &link_val, req, &link_write_task, *dep_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create link to group")
            gmt_deps[gmt_ndeps] = link_write_task;
            gmt_ndeps++;
        } /* end if */
    } /* end if */
    else {
        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast to the
         * calling functions (file_create, group_create) though it could only be
         * an issue with group reopen so we'll skip it for now.  There is
         * probably never an issue with file reopen since all commits are from
         * process 0, same as the group create above. */

        /* Add dependency for group metatask */
        assert(gmt_ndeps == 0);
        assert(*dep_task);
        gmt_deps[gmt_ndeps] = *dep_task;
        gmt_ndeps++;
        /* Check for failure of process 0 DSINC */
    } /* end else */

    /* Finish setting up group struct */
    if((grp->gcpl_id = H5Pcopy(gcpl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gcpl")
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl")

    /* Fill GCPL cache */
    if(H5_daos_group_fill_gcpl_cache(grp) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "failed to fill GCPL cache")

    /* Fill OCPL cache */
    if(H5_daos_fill_ocpl_cache(&grp->obj, grp->gcpl_id) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "failed to fill OCPL cache")

    ret_value = (void *)grp;

done:
    /* Create metatask to use for dependencies on this group create */
    if(0 != (ret = tse_task_create(H5_daos_metatask_autocomplete, &file->sched, NULL, &group_metatask)))
        D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create meta task for group create: %s", H5_daos_err_to_string(ret))
    /* Register dependencies (if any) */
    else if(gmt_ndeps > 0 && 0 != (ret = tse_task_register_deps(group_metatask, gmt_ndeps, gmt_deps)))
        D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for group meta task: %s", H5_daos_err_to_string(ret))
    /* Schedule group metatask (or save it to be scheduled later) */
    else {
        if(*first_task) {
            if(0 != (ret = tse_task_schedule(group_metatask, false)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule group meta task: %s", H5_daos_err_to_string(ret))
            else
                *dep_task = group_metatask;
        } /* end if */
        else {
            *first_task = group_metatask;
            *dep_task = group_metatask;
        } /* end else */
    } /* end else */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value) {
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

        /* Free memory */
        if(update_cb_ud && update_cb_ud->obj && H5_daos_object_close(update_cb_ud->obj, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close object")
        gcpl_buf = DV_free(gcpl_buf);
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    assert(!update_cb_ud);
    assert(!gcpl_buf);

    D_FUNC_LEAVE
} /* end H5_daos_group_create_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_create
 *
 * Purpose:     Sends a request to DAOS to create a group
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_create(void *_item,
    const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
    hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *grp = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    uint64_t oidx;
    hbool_t collective;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "group parent object is NULL")
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
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request")

    if(!collective || (item->file->my_rank == 0)) {
        /* Start transaction */
        if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't start transaction")
        int_req->th_open = TRUE;

        /* Traverse the path */
        if(name) {
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, lcpl_id, dxpl_id,
                    NULL, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

            /* Reject invalid object names during object creation */
            if(!strncmp(target_name, ".", 2))
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, NULL, "invalid group name - '.'")
        } /* end if */
    } /* end if */

    /* Generate object index */
    if(H5_daos_oidx_generate(&oidx, item->file, collective) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't generate object index")

    /* Create group and link to group */
    if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_create_helper(item->file, gcpl_id, gapl_id, dxpl_id,
            int_req, target_grp, target_name, target_name ? strlen(target_name) : 0, oidx, collective, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create group")

    /* Set return value */
    ret_value = (void *)grp;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    if(int_req) {
        tse_task_t *finalize_task;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(item->file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, NULL, "group creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't free request")
    } /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE_API
} /* end H5_daos_group_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for group
 *              opens (rank 0).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 * Programmer:  Neil Fortner
 *              January, 2020
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_group_open_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
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
    assert(udata->obj->item.type == H5I_GROUP);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast group info";
    } /* end if */
    else
        /* Reissue bcast if necesary */
        if(udata->buffer_len != udata->count) {
            tse_task_t *bcast_task;

            assert(udata->count == H5_DAOS_GINFO_BUF_SIZE);
            assert(udata->buffer_len > H5_DAOS_GINFO_BUF_SIZE);

            /* Use full buffer this time */
            udata->count = udata->buffer_len;

            /* Create task for second bcast */
            if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, &udata->obj->item.file->sched, udata, &bcast_task)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't create task for second group info broadcast")

            /* Set callback functions for second bcast */
            if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_group_open_bcast_comp_cb, NULL, 0)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't register callbacks for second group info broadcast: %s", H5_daos_err_to_string(ret))

            /* Schedule second bcast and transfer ownership of udata */
            if(0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule task for second group info broadcast: %s", H5_daos_err_to_string(ret))
            udata = NULL;
        } /* end if */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Close group */
        if(H5_daos_group_close((H5_daos_group_t *)udata->obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close group")

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast group info completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

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
} /* end H5_daos_group_open_bcast_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_recv_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for group
 *              opens (rank 1+).
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
H5_daos_group_open_recv_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_t *udata;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for group info receive task")

    assert(udata->req);
    assert(udata->obj);
    assert(udata->obj->item.file);
    assert(!udata->req->file->closed);
    assert(udata->obj->item.file->my_rank > 0);
    assert(udata->obj->item.type == H5I_GROUP);

    /* Handle errors in bcast task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if(task->dt_result < H5_DAOS_PRE_ERROR
            && udata->req->status >= H5_DAOS_INCOMPLETE) {
        udata->req->status = task->dt_result;
        udata->req->failed_task = "MPI_Ibcast group info";
    } /* end if */
    else {
        uint64_t gcpl_len;
        size_t ginfo_len;
        uint8_t *p = udata->buffer;

        /* Decode oid */
        UINT64DECODE(p, udata->obj->oid.lo)
        UINT64DECODE(p, udata->obj->oid.hi)

        /* Decode GCPL length */
        UINT64DECODE(p, gcpl_len)

        /* Check for gcpl_len set to 0 - indicates failure */
        if(gcpl_len == 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_REMOTE_ERROR, "lead process failed to open group")

        /* Calculate data length */
        ginfo_len = (size_t)gcpl_len + 3 * sizeof(uint64_t);

        /* Reissue bcast if necesary */
        if(ginfo_len > (size_t)udata->count) {
            tse_task_t *bcast_task;

            assert(udata->buffer_len == H5_DAOS_GINFO_BUF_SIZE);
            assert(udata->count == H5_DAOS_GINFO_BUF_SIZE);

            /* Realloc buffer */
            DV_free(udata->buffer);
            if(NULL == (udata->buffer = DV_malloc(ginfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "failed to allocate memory for group info buffer")
            udata->buffer_len = ginfo_len;
            udata->count = ginfo_len;

            /* Create task for second bcast */
            if(0 !=  (ret = tse_task_create(H5_daos_mpi_ibcast_task, &udata->obj->item.file->sched, udata, &bcast_task)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't create task for second group info broadcast")

            /* Set callback functions for second bcast */
            if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, H5_daos_group_open_recv_comp_cb, NULL, 0)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't register callbacks for second group info broadcast: %s", H5_daos_err_to_string(ret))

            /* Schedule second bcast and transfer ownership of udata */
            if(0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule task for second group info broadcast: %s", H5_daos_err_to_string(ret))
            udata = NULL;
        } /* end if */
        else {
            /* Finish building group object */
            /* Open group */
            if(0 != (ret = daos_obj_open(udata->obj->item.file->coh, udata->obj->oid, udata->obj->item.file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &udata->obj->obj_oh, NULL /*event*/)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, ret, "can't open group: %s", H5_daos_err_to_string(ret))

            /* Decode GCPL */
            if((((H5_daos_group_t *)udata->obj)->gcpl_id = H5Pdecode(p)) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTDECODE, H5_DAOS_H5_DECODE_ERROR, "can't deserialize GCPL")

            /* Fill GCPL cache */
            if(H5_daos_group_fill_gcpl_cache((H5_daos_group_t *)udata->obj) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_CPL_CACHE_ERROR, "failed to fill GCPL cache")

            /* Fill OCPL cache */
            if(H5_daos_fill_ocpl_cache(udata->obj, ((H5_daos_group_t *)udata->obj)->gcpl_id) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache")
        } /* end else */
    } /* end else */

done:
    /* Free private data if we haven't released ownership */
    if(udata) {
        /* Close group */
        if(H5_daos_group_close((H5_daos_group_t *)udata->obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close group")

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if(ret_value < 0 && udata->req->status >= H5_DAOS_INCOMPLETE) {
            udata->req->status = ret_value;
            udata->req->failed_task = "MPI_Ibcast group info completion callback";
        } /* end if */

        /* Release our reference to req */
        if(H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

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
} /* end H5_daos_group_open_recv_comp_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_ginfo_read_comp_cb
 *
 * Purpose:     Complete callback for asynchronous metadata fetch for
 *              group opens.
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
H5_daos_ginfo_read_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_omd_fetch_ud_t *udata;
    int ret;
    int ret_value = 0;

    /* Get private data */
    if(NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_DAOS_GET_ERROR, "can't get private data for group info read task")

    assert(udata->md_rw_cb_ud.req);
    assert(udata->md_rw_cb_ud.req->file);
    assert(udata->md_rw_cb_ud.obj);
    assert(udata->fetch_metatask);
    assert(!udata->md_rw_cb_ud.req->file->closed);
    assert(udata->md_rw_cb_ud.obj->item.type == H5I_GROUP);

    /* Check for buffer not large enough */
    if(task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;

        if(udata->bcast_udata) {
            /* Verify iod size makes sense */
            if(udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != (H5_DAOS_GINFO_BUF_SIZE - 3 * H5_DAOS_ENCODED_UINT64_T_SIZE))
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "buffer length does not match expected value")
            if(udata->md_rw_cb_ud.iod[0].iod_size <= (H5_DAOS_GINFO_BUF_SIZE - 3 * H5_DAOS_ENCODED_UINT64_T_SIZE))
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "invalid iod_size returned from DAOS (buffer should have been large enough)")
            
            /* Reallocate group info buffer */
            udata->bcast_udata->buffer = DV_free(udata->bcast_udata->buffer);
            if(NULL == (udata->bcast_udata->buffer = DV_malloc(udata->md_rw_cb_ud.iod[0].iod_size + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "can't allocate buffer for serialized group info")
            udata->bcast_udata->buffer_len = udata->md_rw_cb_ud.iod[0].iod_size + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE;

            /* Set up sgl */
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], (uint8_t *)udata->bcast_udata->buffer + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE, udata->md_rw_cb_ud.iod[0].iod_size);
            udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        } /* end if */
        else {
            /* Verify iod size makes sense */
            if(udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_GINFO_BUF_SIZE)
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "buffer length does not match expected value")
            if(udata->md_rw_cb_ud.iod[0].iod_size <= H5_DAOS_GINFO_BUF_SIZE)
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_DAOS_BAD_VALUE, "invalid iod_size returned from DAOS (buffer should have been large enough)")
            
            /* Reallocate group info buffer */
            udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_free(udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            if(NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(udata->md_rw_cb_ud.iod[0].iod_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, H5_DAOS_ALLOC_ERROR, "can't allocate buffer for serialized group info")

            /* Set up sgl */
            udata->md_rw_cb_ud.sg_iov[0].iov_buf_len = udata->md_rw_cb_ud.iod[0].iod_size;
            udata->md_rw_cb_ud.sg_iov[0].iov_len = udata->md_rw_cb_ud.iod[0].iod_size;
            udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        } /* end else */

        /* Create task for reissued group metadata read */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_FETCH, &udata->md_rw_cb_ud.obj->item.file->sched, 0, NULL, &fetch_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't create task to read group medadata: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for group metadata read */
        if(0 != (ret = tse_task_register_cbs(fetch_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_ginfo_read_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't register callbacks for task to read group medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for group metadata read */
        (void)tse_task_set_priv(fetch_task, udata);

        /* Schedule group metadata read task and give it a reference to req and
         * the group */
        if(0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule task to read group metadata: %s", H5_daos_err_to_string(ret))
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
            if(udata->bcast_udata) {
                uint8_t *p;

                /* Encode oid */
                p = udata->bcast_udata->buffer;
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.lo)
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.hi)

                /* Encode GCPL length */
                UINT64ENCODE(p, udata->md_rw_cb_ud.iod[0].iod_size)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Finish building group object */
            /* Decode GCPL */
            if((((H5_daos_group_t *)udata->md_rw_cb_ud.obj)->gcpl_id = H5Pdecode(udata->md_rw_cb_ud.sg_iov[0].iov_buf)) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTDECODE, H5_DAOS_H5_DECODE_ERROR, "can't deserialize GCPL")

            /* Fill GCPL cache */
            if(H5_daos_group_fill_gcpl_cache((H5_daos_group_t *)udata->md_rw_cb_ud.obj) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_CPL_CACHE_ERROR, "failed to fill GCPL cache")

            /* Fill OCPL cache */
            if(H5_daos_fill_ocpl_cache(udata->md_rw_cb_ud.obj, ((H5_daos_group_t *)udata->md_rw_cb_ud.obj)->gcpl_id) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache")
        } /* end else */
    } /* end else */

done:
    /* Clean up if this is the last fetch task */
    if(udata) {
        /* Close group */
        if(H5_daos_group_close((H5_daos_group_t *)udata->md_rw_cb_ud.obj, H5I_INVALID_HID, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_DAOS_H5_CLOSE_ERROR, "can't close group")

        if(udata->bcast_udata) {
            /* Clear broadcast buffer if there was an error */
            if(udata->md_rw_cb_ud.req->status < H5_DAOS_INCOMPLETE)
                (void)memset(udata->bcast_udata->buffer, 0, H5_DAOS_GINFO_BUF_SIZE);
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
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_DAOS_FREE_ERROR, "can't free request")

        /* Complete fetch metatask */
        tse_task_complete(udata->fetch_metatask, ret_value);

        assert(!udata->md_rw_cb_ud.free_dkey);
        assert(!udata->md_rw_cb_ud.free_akeys);

        /* Free udata */
        DV_free(udata);
    } /* end if */

    return ret_value;
} /* end H5_daos_ginfo_read_comp_cb */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_helper_async
 *
 * Purpose:     Performs the actual group open, given the oid.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
H5_daos_group_t *
H5_daos_group_open_helper_async(H5_daos_file_t *file, daos_obj_id_t oid,
    hid_t gapl_id, hid_t dxpl_id, H5_daos_req_t *req, hbool_t collective,
    tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_group_t *grp = NULL;
    uint8_t *ginfo_buf = NULL;
    H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;
    H5_daos_omd_fetch_ud_t *fetch_udata = NULL;
    int ret;
    H5_daos_group_t *ret_value = NULL;

    assert(file);
    assert(first_task);
    assert(dep_task);

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.open_req = req;
    req->rc++;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.oid = oid;
    grp->obj.obj_oh = DAOS_HDL_INVAL;
    grp->gcpl_id = FAIL;
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Set up broadcast user data */
    if(collective && (file->num_procs > 1)) {
        if(NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "failed to allocate buffer for MPI broadcast user data")
        bcast_udata->req = req;
        bcast_udata->obj = &grp->obj;
        bcast_udata->buffer = NULL;
        bcast_udata->buffer_len = 0;
        bcast_udata->count = 0;
    } /* end if */

    /* Open group and read metadata if this process should */
    if(!collective || (file->my_rank == 0)) {
        tse_task_t *fetch_task = NULL;

        /* Open group object */
        if(H5_daos_obj_open(file, req, &oid, file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &grp->obj.obj_oh, "group object open", first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group object")

        /* Allocate argument struct for fetch task */
        if(NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(sizeof(H5_daos_omd_fetch_ud_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for fetch callback arguments")

        /* Set up operation to read GCPL size from group */
        /* Set up ud struct */
        fetch_udata->md_rw_cb_ud.req = req;
        fetch_udata->md_rw_cb_ud.obj = &grp->obj;
        fetch_udata->bcast_udata = bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);
        fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Single iod and sgl */
        fetch_udata->md_rw_cb_ud.nr = 1u;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.iod[0].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[0].iod_nr = 1u;
        fetch_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;
        fetch_udata->md_rw_cb_ud.free_akeys = FALSE;

        /* Allocate initial group info buffer */
        if(NULL == (ginfo_buf = DV_malloc(H5_DAOS_GINFO_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")

        /* Set up sgl */
        if(bcast_udata) {
            daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], ginfo_buf + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE, (daos_size_t)(H5_DAOS_GINFO_BUF_SIZE - 3 * H5_DAOS_ENCODED_UINT64_T_SIZE));
            bcast_udata->buffer = ginfo_buf;
            ginfo_buf = NULL;
            bcast_udata->buffer_len = H5_DAOS_GINFO_BUF_SIZE;
            bcast_udata->count = H5_DAOS_GINFO_BUF_SIZE;
        } /* end if */
        else
            daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], ginfo_buf, (daos_size_t)(H5_DAOS_GINFO_BUF_SIZE));
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr = 1;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs = &fetch_udata->md_rw_cb_ud.sg_iov[0];

        /* Set task name */
        fetch_udata->md_rw_cb_ud.task_name = "group metadata read";

        /* Create meta task for group metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_ginfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if(0 != (ret = tse_task_create(NULL, &file->sched, NULL, &fetch_udata->fetch_metatask)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create meta task for group metadata read: %s", H5_daos_err_to_string(ret))

        /* Create task for group metadata read */
        if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_FETCH, &file->sched, 0, NULL, &fetch_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to read group medadata: %s", H5_daos_err_to_string(ret))

        /* Register dependency for task */
        assert(*dep_task);
        if(0 != (ret = tse_task_register_deps(fetch_task, 1, dep_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for group metadata read: %s", H5_daos_err_to_string(ret))

        /* Set callback functions for group metadata read */
        if(0 != (ret = tse_task_register_cbs(fetch_task, H5_daos_md_rw_prep_cb, NULL, 0, H5_daos_ginfo_read_comp_cb, NULL, 0)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't register callbacks for task to read group medadata: %s", H5_daos_err_to_string(ret))

        /* Set private data for group metadata write */
        (void)tse_task_set_priv(fetch_task, fetch_udata);

        /* Schedule meta task */
        if(0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule meta task for group metadata read: %s", H5_daos_err_to_string(ret))

        /* Schedule group metadata write task (or save it to be scheduled later)
         * and give it a reference to req and the group */
        assert(*first_task);
        if(0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to read group metadata: %s", H5_daos_err_to_string(ret))
        *dep_task = fetch_udata->fetch_metatask;
        req->rc++;
        grp->obj.item.rc++;
        fetch_udata = NULL;
        ginfo_buf = NULL;
    } /* end if */
    else {
        assert(bcast_udata);

        /* Allocate buffer for group info */
        if(NULL == (bcast_udata->buffer = DV_malloc(H5_DAOS_GINFO_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
        bcast_udata->buffer_len = H5_DAOS_GINFO_BUF_SIZE;
        bcast_udata->count = H5_DAOS_GINFO_BUF_SIZE;
    } /* end else */

    ret_value = grp;

done:
    /* Broadcast group info */
    if(bcast_udata) {
        assert(!ginfo_buf);

        /* Handle failure */
        if(NULL == ret_value) {
            /* Allocate buffer for group info if necessary, either way set
             * buffer to 0 to indicate failure */
            if(bcast_udata->buffer) {
                assert(bcast_udata->buffer_len == H5_DAOS_GINFO_BUF_SIZE);
                assert(bcast_udata->count == H5_DAOS_GINFO_BUF_SIZE);
                (void)memset(bcast_udata->buffer, 0, H5_DAOS_GINFO_BUF_SIZE);
            } /* end if */
            else {
                if(NULL == (bcast_udata->buffer = DV_calloc(H5_DAOS_GINFO_BUF_SIZE)))
                    D_DONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
                bcast_udata->buffer_len = H5_DAOS_GINFO_BUF_SIZE;
                bcast_udata->count = H5_DAOS_GINFO_BUF_SIZE;
            } /* end else */
        } /* end if */

        if(bcast_udata->buffer) {
            tse_task_t *bcast_task;

            /* Create meta task for group info bcast.  This empty task will be
             * completed when the bcast is finished by the completion callback.
             * We can't use bcast_task since it may not be completed after the
             * first bcast. */
            if(0 != (ret = tse_task_create(NULL, &file->sched, NULL, &bcast_udata->bcast_metatask)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create meta task for group info broadcast: %s", H5_daos_err_to_string(ret))
            /* Create task for group info bcast */
            if(0 != (ret = tse_task_create(H5_daos_mpi_ibcast_task, &file->sched, bcast_udata, &bcast_task)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to broadcast group info: %s", H5_daos_err_to_string(ret))
            /* Register task dependency if present */
            else if(*dep_task && 0 != (ret = tse_task_register_deps(bcast_task, 1, dep_task)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for group info broadcast task: %s", H5_daos_err_to_string(ret))
            /* Set callback functions for group info bcast */
            else if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, file->my_rank == 0 ? H5_daos_group_open_bcast_comp_cb : H5_daos_group_open_recv_comp_cb, NULL, 0)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't register callbacks for group info broadcast: %s", H5_daos_err_to_string(ret))
            /* Schedule meta task */
            else if(0 != (ret = tse_task_schedule(bcast_udata->bcast_metatask, false)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule meta task for group info broadcast: %s", H5_daos_err_to_string(ret))
            /* Schedule bcast and transfer ownership of bcast_udata */
            else {
                if(*first_task) {
                    if(0 != (ret = tse_task_schedule(bcast_task, false)))
                        D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task for group info broadcast: %s", H5_daos_err_to_string(ret))
                    else {
                        req->rc++;
                        grp->obj.item.rc++;
                        *dep_task = bcast_udata->bcast_metatask;
                        bcast_udata = NULL;
                    } /* end else */
                } /* end if */
                else {
                    *first_task = bcast_task;
                    req->rc++;
                    grp->obj.item.rc++;
                    *dep_task = bcast_udata->bcast_metatask;
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

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

        /* Free memory */
        fetch_udata = DV_free(fetch_udata);
        ginfo_buf = DV_free(ginfo_buf);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!fetch_udata);
    assert(!bcast_udata);
    assert(!ginfo_buf);

    D_FUNC_LEAVE
} /* end H5_daos_group_open_helper_async() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_helper
 *
 * Purpose:     Performs the actual group open, given the oid.
 *              Synchronous version, will be removed once its use is
 *              eliminated.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_open_helper(H5_daos_file_t *file, daos_obj_id_t oid,
    hid_t gapl_id, hid_t dxpl_id, H5_daos_req_t H5VL_DAOS_UNUSED *req, void **gcpl_buf_out,
    uint64_t *gcpl_len_out)
{
    H5_daos_group_t *grp = NULL;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    void *gcpl_buf = NULL;
    uint64_t gcpl_len;
    int ret;
    void *ret_value = NULL;

    assert(file);

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.open_req = NULL;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.oid = oid;
    grp->obj.obj_oh = DAOS_HDL_INVAL;
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Open group */
    if(0 != (ret = daos_obj_open(file->coh, oid, file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &grp->obj.obj_oh, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open group: %s", H5_daos_err_to_string(ret))

    /* Set up operation to read GCPL size from group */
    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Read internal metadata size from group */
    if(0 != (ret = daos_obj_fetch(grp->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTDECODE, NULL, "can't read metadata size from group: %s", H5_daos_err_to_string(ret))

    /* Check for metadata not found */
    if(iod.iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_SYM, H5E_NOTFOUND, NULL, "internal metadata not found")

    /* Allocate buffer for GCPL */
    gcpl_len = iod.iod_size;
    if(NULL == (gcpl_buf = DV_malloc(gcpl_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")

    /* Set up sgl */
    daos_iov_set(&sg_iov, gcpl_buf, (daos_size_t)gcpl_len);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read internal metadata from group */
    if(0 != (ret = daos_obj_fetch(grp->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTDECODE, NULL, "can't read metadata from group: %s", H5_daos_err_to_string(ret))

    /* Decode GCPL */
    if((grp->gcpl_id = H5Pdecode(gcpl_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize GCPL")

    /* Finish setting up group struct */
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Fill GCPL cache */
    if(H5_daos_group_fill_gcpl_cache(grp) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "failed to fill GCPL cache")

    /* Fill OCPL cache */
    if(H5_daos_fill_ocpl_cache(&grp->obj, grp->gcpl_id) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "failed to fill OCPL cache")

    /* Return GCPL info if requested, relinquish ownership of gcpl_buf if so */
    if(gcpl_buf_out) {
        assert(gcpl_len_out);
        assert(!*gcpl_buf_out);

        *gcpl_buf_out = gcpl_buf;
        gcpl_buf = NULL;

        *gcpl_len_out = gcpl_len;
    } /* end if */

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = DV_free(gcpl_buf);

    D_FUNC_LEAVE
} /* end H5_daos_group_open_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open
 *
 * Purpose:     Sends a request to DAOS to open a group
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *grp = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_obj_id_t oid = {0, 0};
    hbool_t collective;
    hbool_t must_bcast = FALSE;
    H5_daos_req_t *int_req = NULL;
    tse_task_t *first_task = NULL;
    tse_task_t *dep_task = NULL;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "group parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
 
    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    collective = item->file->fapl_cache.is_collective_md_read;
    if(!collective && (H5P_GROUP_ACCESS_DEFAULT != gapl_id))
        if(H5Pget_all_coll_metadata_ops(gapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get collective metadata reads property")

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(item->file, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Check if we're actually opening the group or just receiving the group
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Start transaction */
        if(0 != (ret = daos_tx_open(item->file->coh, &int_req->th, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't start transaction")
        int_req->th_open = TRUE;

        /* Check for open by object token */
        if(H5VL_OBJECT_BY_TOKEN == loc_params->type) {
            /* Generate oid from token */
            if(H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't convert object token to OID")
        } /* end if */
        else {
            /* Open using name parameter */
            if(H5VL_OBJECT_BY_SELF != loc_params->type)
                D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "unsupported group open location parameters type")
            if(!name)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "group name is NULL")

            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, dxpl_id, req,
                    &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

            /* Check for no target_name, in this case just return target_grp */
            if(target_name[0] == '\0'
                    || (target_name[0] == '.' && target_name[1] == '\0')) {
                /* Take ownership of target_grp */
                grp = target_grp;
                target_grp = NULL;
            } /* end if */
            else {
                htri_t link_resolved;

                /* Follow link to group */
                if((link_resolved = H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &oid)) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, NULL, "can't follow link to group")
                if(!link_resolved)
                    D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, NULL, "link to group did not resolve")
            } /* end else */
        } /* end else */
    } /* end if */

    /* Open group if not already open */
    if(!grp) {
        must_bcast = FALSE;     /* Helper function will handle bcast */
        if(NULL == (grp = H5_daos_group_open_helper_async(item->file, oid, gapl_id, dxpl_id, int_req, collective, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group")
    } /* end if */

    /* Set return value */
    ret_value = (void *)grp;

done:
    /* Broadcast group info if needed */
    if(must_bcast) {
        H5_daos_mpi_ibcast_ud_t *bcast_udata = NULL;

        assert(int_req);

        /* Allocate udata struct for broadcast */
        if(NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_t *)DV_malloc(sizeof(H5_daos_mpi_ibcast_ud_t))))
            D_DONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "failed to allocate buffer for MPI broadcast user data")
        else {
            bcast_udata->req = int_req;
            bcast_udata->obj = (H5_daos_obj_t *)grp;

            /* Handle failure */
            if(NULL == ret_value) {
                /* Allocate buffer for group info if necessary, either way set
                 * buffer to 0 to indicate failure */
                if(NULL == (bcast_udata->buffer = DV_calloc(H5_DAOS_GINFO_BUF_SIZE)))
                    D_DONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
                bcast_udata->buffer_len = H5_DAOS_GINFO_BUF_SIZE;
                bcast_udata->count = H5_DAOS_GINFO_BUF_SIZE;
            } /* end if */
            else {
                size_t gcpl_size;
                uint64_t gcpl_len;
                uint8_t *p;

                /* Reset must_bcast, if it isn't set back to TRUE something failed
                 * and we can't bcast */
                must_bcast = FALSE;

                /* Determine length of serialized GCPL */
                if(H5Pencode2(grp->gcpl_id, NULL, &gcpl_size, item->file->fapl_id) < 0)
                    D_DONE_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl")
                else {
                    /* Allocate buffer for group info */
                    bcast_udata->buffer_len = MAX(gcpl_size + 3 * sizeof(uint64_t), H5_DAOS_GINFO_BUF_SIZE);
                    bcast_udata->count = H5_DAOS_GINFO_BUF_SIZE;
                    if(NULL == (bcast_udata->buffer = DV_malloc(bcast_udata->buffer_len)))
                        D_DONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
                    else {
                        /* Encode oid */
                        p = bcast_udata->buffer;
                        UINT64ENCODE(p, oid.lo)
                        UINT64ENCODE(p, oid.hi)

                        /* Encode GCPL length */
                        gcpl_len = (uint64_t)gcpl_size;
                        UINT64ENCODE(p, gcpl_len)

                        /* Serialize GCPL */
                        if(H5Pencode2(grp->gcpl_id, p, &gcpl_size, item->file->fapl_id) < 0)
                            D_DONE_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl")
                        else
                            must_bcast = TRUE;
                    } /* end else */
                } /* end else */
            } /* end else */

            /* Do actual broadcast if we still should */
            if(must_bcast) {
                tse_task_t *bcast_task;

                /* Create meta task for group info bcast.  This empty task will be
                 * completed when the bcast is finished by the completion callback.
                 * We can't use bcast_task since it may not be completed after the
                 * first bcast. */
                if(0 != (ret = tse_task_create(NULL, &item->file->sched, NULL, &bcast_udata->bcast_metatask)))
                    D_DONE_ERROR(H5E_VOL, H5E_CANTINIT, NULL, "can't create meta task for global handle broadcast: %s", H5_daos_err_to_string(ret))
                /* Create task for group info bcast */
                if(0 != (ret = tse_task_create(H5_daos_mpi_ibcast_task, &item->file->sched, bcast_udata, &bcast_task)))
                    D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
                /* Register task dependency if present */
                else if(dep_task && 0 != (ret = tse_task_register_deps(bcast_task, 1, &dep_task)))
                    D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for group info broadcast task: %s", H5_daos_err_to_string(ret))
                /* Set callback functions for group info bcast */
                else if(0 != (ret = tse_task_register_cbs(bcast_task, NULL, NULL, 0, item->file->my_rank == 0 ? H5_daos_group_open_bcast_comp_cb : H5_daos_group_open_recv_comp_cb, NULL, 0)))
                    D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't register callbacks for second group info broadcast: %s", H5_daos_err_to_string(ret))
                /* Schedule meta task */
                else if(0 != (ret = tse_task_schedule(bcast_udata->bcast_metatask, false)))
                    D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule meta task for group info broadcast: %s", H5_daos_err_to_string(ret))
                /* Schedule second bcast and transfer ownership of bcast_udata */
                else {
                    if(first_task) {
                        if(0 != (ret = tse_task_schedule(bcast_task, false)))
                            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task for second group info broadcast: %s", H5_daos_err_to_string(ret))
                        else {
                            int_req->rc++;
                            grp->obj.item.rc++;
                            dep_task = bcast_udata->bcast_metatask;
                            bcast_udata = NULL;
                        } /* end else */
                    } /* end if */
                    else {
                        first_task = bcast_task;
                        int_req->rc++;
                        grp->obj.item.rc++;
                        dep_task = bcast_udata->bcast_metatask;
                        bcast_udata = NULL;
                    } /* end else */
                } /* end else */
            } /* end if */

            /* Cleanup on failure */
            if(bcast_udata) {
                DV_free(bcast_udata->buffer);
                DV_free(bcast_udata);
            } /* end if */
        } /* end else */
    } /* end if */

    if(int_req) {
        tse_task_t *finalize_task;

        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependency (if any) */
        else if(dep_task && 0 != (ret = tse_task_register_deps(finalize_task, 1, &dep_task)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if(NULL == ret_value)
            int_req->status = H5_DAOS_SETUP_ERROR;

        /* Schedule first task */
        if(first_task && (0 != (ret = tse_task_schedule(first_task, false))))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule initial task for H5 operation: %s", H5_daos_err_to_string(ret))

        /* Block until operation completes */
        /* Wait for scheduler to be empty */
        if(H5_daos_progress(item->file, H5_DAOS_PROGRESS_WAIT) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler")

        /* Check for failure */
        if(int_req->status < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, NULL, "group open failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))

        /* Close internal request */
        if(H5_daos_req_free_int(int_req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't free request")
    } /* end if */

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, NULL) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE_API
} /* end H5_daos_group_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_get
 *
 * Purpose:     Performs a group "get" operation
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
H5_daos_group_get(void *_item, H5VL_group_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5_daos_group_t *grp = (H5_daos_group_t *)_item;
    herr_t           ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(H5I_FILE != grp->obj.item.type && H5I_GROUP != grp->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file or group")

    switch (get_type) {
        /* H5Gget_create_plist */
        case H5VL_GROUP_GET_GCPL:
        {
            hid_t *ret_id = va_arg(arguments, hid_t *);

            if((*ret_id = H5Pcopy(grp->gcpl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get group's GCPL")

            /* Set group's object class on gcpl */
            if(H5_daos_set_oclass_from_oid(*ret_id, grp->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property")

            break;
        } /* H5VL_GROUP_GET_GCPL */

        /* H5Gget_info(_by_name/by_idx) */
        case H5VL_GROUP_GET_INFO:
        {
            const H5VL_loc_params_t *loc_params = va_arg(arguments, const H5VL_loc_params_t *);
            H5G_info_t *group_info = va_arg(arguments, H5G_info_t *);

            if(H5_daos_group_get_info(grp, loc_params, group_info, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group's info")

            break;
        } /* H5VL_GROUP_GET_INFO */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported group get operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_group_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_specific
 *
 * Purpose:     Performs a group "specific" operation
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
H5_daos_group_specific(void *_item, H5VL_group_specific_t specific_type,
    hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req, va_list H5VL_DAOS_UNUSED arguments)
{
    H5_daos_group_t *grp = (H5_daos_group_t *)_item;
    herr_t           ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(H5I_FILE != grp->obj.item.type && H5I_GROUP != grp->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file or group")

    switch (specific_type) {
        /* H5Gflush */
        case H5VL_GROUP_FLUSH:
            if(H5_daos_group_flush(grp) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't flush group")
            break;

        /* H5Grefresh */
        case H5VL_GROUP_REFRESH:
            if(H5_daos_group_refresh(grp, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "can't refresh group")
            break;

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported group specific operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_group_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_close
 *
 * Purpose:     Closes a daos HDF5 group.
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
H5_daos_group_close(void *_grp, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    H5_daos_group_t *grp = (H5_daos_group_t *)_grp;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_grp)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group object is NULL")

    if(--grp->obj.item.rc == 0) {
        /* Free group data structures */
        if(grp->obj.item.open_req)
            if(H5_daos_req_free_int(grp->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't free request")
        if(!daos_handle_is_inval(grp->obj.obj_oh))
            if(0 != (ret = daos_obj_close(grp->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close group DAOS object: %s", H5_daos_err_to_string(ret))
        if(grp->gcpl_id != FAIL && H5Idec_ref(grp->gcpl_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close gcpl")
        if(grp->gapl_id != FAIL && H5Idec_ref(grp->gapl_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close gapl")
        grp = H5FL_FREE(H5_daos_group_t, grp);
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_group_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_flush
 *
 * Purpose:     Flushes a DAOS group.  Currently a no-op, may create a
 *              snapshot in the future.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_group_flush(H5_daos_group_t *grp)
{
    herr_t ret_value = SUCCEED;    /* Return value */

    assert(grp);

    /* Nothing to do if no write intent */
    if(!(grp->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_DONE(SUCCEED)

    /* Progress scheduler until empty? DSINC */

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_flush() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_refresh
 *
 * Purpose:     Refreshes a DAOS group (currently a no-op)
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
H5_daos_group_refresh(H5_daos_group_t H5VL_DAOS_UNUSED *grp, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req)
{
    herr_t ret_value = SUCCEED;

    assert(grp);

    D_GOTO_DONE(SUCCEED)

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_refresh() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_get_info
 *
 * Purpose:     Retrieves a group's info, storing the results in the
 *              supplied H5G_info_t.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_get_info(H5_daos_group_t *grp, const H5VL_loc_params_t *loc_params,
    H5G_info_t *group_info, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *target_grp = NULL;
    H5G_info_t local_grp_info;
    uint64_t max_corder;
    ssize_t grp_nlinks;
    herr_t ret_value = SUCCEED;

    assert(grp);
    assert(loc_params);
    assert(group_info);

    /* Determine the target group */
    switch (loc_params->type) {
        /* H5Gget_info */
        case H5VL_OBJECT_BY_SELF:
        {
            /* Use item as group, or the root group if item is a file */
            if(grp->obj.item.type == H5I_FILE)
                target_grp = ((H5_daos_file_t *)grp)->root_grp;
            else if(grp->obj.item.type == H5I_GROUP)
                target_grp = grp;
            else
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a file or group")

            target_grp->obj.item.rc++;
            break;
        } /* H5VL_OBJECT_BY_SELF */

        /* H5Gget_info_by_name */
        case H5VL_OBJECT_BY_NAME:
        {
            H5VL_loc_params_t sub_loc_params;

            /* Open target group */
            sub_loc_params.obj_type = grp->obj.item.type;
            sub_loc_params.type = H5VL_OBJECT_BY_SELF;
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_open(grp, &sub_loc_params,
                    loc_params->loc_data.loc_by_name.name, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group")

            break;
        } /* H5VL_OBJECT_BY_NAME */

        /* H5Gget_info_by_idx */
        case H5VL_OBJECT_BY_IDX:
        {
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_object_open(grp, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group")

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_TOKEN:
        default:
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid loc_params type")
    } /* end switch */

    /* Retrieve the group's info */

    local_grp_info.storage_type = H5G_STORAGE_TYPE_UNKNOWN;
    local_grp_info.nlinks = 0;
    local_grp_info.max_corder = 0;
    local_grp_info.mounted = FALSE; /* DSINC - will file mounting be supported? */

    /* Retrieve group's max creation order value */
    if(target_grp->gcpl_cache.track_corder) {
        if(H5_daos_group_get_max_crt_order(target_grp, &max_corder) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group's max creation order value")
        local_grp_info.max_corder = (int64_t)max_corder; /* DSINC - no check for overflow! */
    }
    else
        local_grp_info.max_corder = -1;

    /* Retrieve the number of links in the group. */
    if((grp_nlinks = H5_daos_group_get_num_links(target_grp)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get the number of links in group")
    local_grp_info.nlinks = (hsize_t)grp_nlinks;

    memcpy(group_info, &local_grp_info, sizeof(*group_info));

done:
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_group_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_get_num_links
 *
 * Purpose:     Retrieves the current number of links within the target
 *              group.
 *
 * Return:      Success:        The number of links within the group
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5_daos_group_get_num_links(H5_daos_group_t *target_grp)
{
    uint64_t nlinks = 0;
    hid_t target_grp_id = -1;
    int ret;
    ssize_t ret_value = 0;

    assert(target_grp);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_LINKS_SIZE == 8);

    if(target_grp->gcpl_cache.track_corder) {
        daos_sg_list_t sgl;
        daos_key_t dkey;
        daos_iod_t iod;
        daos_iov_t sg_iov;
        uint8_t *p;
        uint8_t nlinks_buf[H5_DAOS_ENCODED_NUM_LINKS_SIZE];

        /* Read the "number of links" key from the target group */

        /* Set up dkey */
        daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)H5_daos_nlinks_key_g, H5_daos_nlinks_key_size_g);
        iod.iod_nr = 1u;
        iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, nlinks_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Read num links */
        if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_READERROR, (-1), "can't read number of links in group: %s", H5_daos_err_to_string(ret))

        p = nlinks_buf;
        /* Check for no num links found, in this case it must be 0 */
        if(iod.iod_size == (uint64_t)0) {
            nlinks = 0;
        } /* end if */
        else
            /* Decode num links */
            UINT64DECODE(p, nlinks);
    } /* end if */
    else {
        H5_daos_iter_data_t iter_data;

        /* Iterate through links */

        /* Register id for grp */
        if((target_grp_id = H5VLwrap_register(target_grp, H5I_GROUP)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
        target_grp->obj.item.rc++;

        /* Initialize iteration data */
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, H5_INDEX_NAME, H5_ITER_NATIVE,
                FALSE, NULL, target_grp_id, &nlinks, H5P_DATASET_XFER_DEFAULT, NULL);
        iter_data.u.link_iter_data.link_iter_op = H5_daos_link_iterate_count_links_callback;

        /* Retrieve the number of links in the group. */
        if(H5_daos_link_iterate(target_grp, &iter_data) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve the number of links in group")
    } /* end else */

    ret_value = (ssize_t)nlinks;

done:
    if((target_grp_id >= 0) && (H5Idec_ref(target_grp_id) < 0))
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group ID")

    D_FUNC_LEAVE
} /* end H5_daos_group_get_num_links() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_update_num_links_key
 *
 * Purpose:     Updates the target group's link number tracking akey by
 *              setting its value to the specified value.
 *
 *              CAUTION: This routine is 'dangerous' in that the link
 *              number tracking akey is used in various places. Only call
 *              this routine if it is certain that the number of links in
 *              the group has changed to the specified value.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_group_update_num_links_key(H5_daos_group_t *target_grp, uint64_t new_nlinks)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint8_t nlinks_new_buf[H5_DAOS_ENCODED_NUM_LINKS_SIZE];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_LINKS_SIZE == 8);

    /* Check that creation order is tracked for target group */
    if(!target_grp->gcpl_cache.track_corder)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "creation order is not tracked for group")

    /* Encode buffer */
    p = nlinks_new_buf;
    UINT64ENCODE(p, new_nlinks);

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_nlinks_key_g, H5_daos_nlinks_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, nlinks_new_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Issue write */
    if(0 != (ret = daos_obj_update(target_grp->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't write number of links to group: %s", H5_daos_err_to_string(ret))

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_update_num_links_key() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_get_max_crt_order
 *
 * Purpose:     Retrieves a group's current maximum creation order value.
 *              Note that this value may not match the current number of
 *              links within the group, as some of the links may have been
 *              deleted.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_group_get_max_crt_order(H5_daos_group_t *target_grp, uint64_t *max_corder)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint64_t max_crt_order;
    uint8_t max_corder_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(max_corder);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Check that creation order is tracked for target group */
    if(!target_grp->gcpl_cache.track_corder)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "creation order is not tracked for group")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_max_link_corder_key_g, H5_daos_max_link_corder_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, max_corder_buf, (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read the max. creation order value */
    if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_READERROR, FAIL, "can't read max creation order: %s", H5_daos_err_to_string(ret))

    p = max_corder_buf;
    /* Check for no max creation order found, in this case it must be 0 */
    if(iod.iod_size == (uint64_t)0) {
        max_crt_order = 0;
    } /* end if */
    else
        /* Decode num links */
        UINT64DECODE(p, max_crt_order);

    *max_corder = max_crt_order;

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_get_max_crt_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_update_max_crt_order_key
 *
 * Purpose:     Updates the target group's maximum creation order value
 *              tracking akey by setting its value to the specified value.
 *
 *              CAUTION: This routine is 'dangerous' in that the maximum
 *              creation order tracking akey is used in various places.
 *              Only call this routine if it is certain that the maximum
 *              creation order value for the group has changed to the
 *              specified value. This routine should also never be used to
 *              decrease the group's maximum creation order value; this
 *              value should only ever increase as new links are created
 *              in the group.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_group_update_max_crt_order_key(H5_daos_group_t *target_grp, uint64_t new_max_corder)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint8_t new_max_corder_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Check that creation order is tracked for target group */
    if(!target_grp->gcpl_cache.track_corder)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "creation order is not tracked for group")

    /* Encode buffer */
    p = new_max_corder_buf;
    UINT64ENCODE(p, new_max_corder);

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_max_link_corder_key_g, H5_daos_max_link_corder_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, new_max_corder_buf, (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Issue write */
    if(0 != (ret = daos_obj_update(target_grp->obj.obj_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't write maximum creation order value to group: %s", H5_daos_err_to_string(ret))

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_update_max_crt_order_key() */
