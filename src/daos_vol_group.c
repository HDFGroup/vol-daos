/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library. Group routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* User data struct for group get info */
typedef struct H5_daos_group_get_info_ud_t {
    H5_daos_req_t *req;
    H5G_info_t    *group_info;
    H5_daos_obj_t *target_obj;
    H5I_type_t     opened_type;
} H5_daos_group_get_info_ud_t;

/* User data struct for group get num links */
typedef struct H5_daos_group_gnl_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud; /* Must be first */
    uint8_t               nlinks_buf[H5_DAOS_ENCODED_NUM_LINKS_SIZE];
    tse_task_t           *gnl_task;
    hsize_t              *nlinks;
} H5_daos_group_gnl_ud_t;

/* User data struct for group get max creation order */
typedef struct H5_daos_group_gmco_ud_t {
    H5_daos_md_rw_cb_ud_t md_rw_cb_ud;
    uint8_t               max_corder_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint64_t             *max_corder;
} H5_daos_group_gmco_ud_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5_daos_group_fill_gcpl_cache(H5_daos_group_t *grp);
static int    H5_daos_group_open_end(H5_daos_group_t *grp, uint8_t *p, uint64_t gcpl_buf_len);
static int    H5_daos_group_open_bcast_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_group_open_recv_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_group_get_info_task(tse_task_t *task);
static herr_t H5_daos_group_get_info(H5_daos_group_t *grp, const H5VL_loc_params_t *loc_params,
                                     H5G_info_t *group_info, H5_daos_req_t *req, tse_task_t **first_task,
                                     tse_task_t **dep_task);
static int    H5_daos_group_gnl_task(tse_task_t *task);
static int    H5_daos_group_gnl_comp_cb(tse_task_t *task, void *args);
static int    H5_daos_group_gmco_comp_cb(tse_task_t *task, void *args);

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_traverse
 *
 * Purpose:     Given a path name and base object, returns the final group
 *              in the path and the object name.  obj_name points into the
 *              buffer given by path, so it does not need to be freed.
 *              The group must be closed with H5_daos_group_close_real().
 *
 * Return:      Success:        group object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
H5_daos_obj_t *
H5_daos_group_traverse(H5_daos_item_t *item, const char *path, hid_t lcpl_id, H5_daos_req_t *req,
                       hbool_t collective, char **path_buf, const char **obj_name, size_t *obj_name_len,
                       tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_obj_t *obj          = NULL;
    char          *tmp_path_buf = NULL;
    H5_daos_req_t *int_int_req  = NULL;
    int            ret;
    H5_daos_obj_t *ret_value = NULL;

    assert(item);
    assert(path);
    assert(req);
    assert(path_buf);
    assert(!*path_buf);
    assert(obj_name);
    assert(obj_name_len);
    assert(first_task);
    assert(dep_task);

    /* Initialize obj_name */
    *obj_name = path;

    /* Open starting group */
    if ((*obj_name)[0] == '/') {
        obj = &item->file->root_grp->obj;
        (*obj_name)++;
    } /* end if */
    else {
        if (item->type == H5I_FILE)
            obj = &((H5_daos_file_t *)item)->root_grp->obj;
        else
            obj = (H5_daos_obj_t *)item;
    } /* end else */
    obj->item.rc++;

    /* Strip trailing no-op components of path ("/" and "/.") */
    *obj_name_len = strlen(*obj_name);
    while (*obj_name_len > 0)
        if ((*obj_name)[*obj_name_len - 1] == '/')
            (*obj_name_len)--;
        else if ((*obj_name)[*obj_name_len - 1] == '.') {
            if (*obj_name_len == 1)
                (*obj_name_len) = 0;
            else if ((*obj_name)[*obj_name_len - 2] == '/')
                (*obj_name_len) -= 2;
            else
                break;
        } /* end if */
        else
            break;

    /* Traverse path if this process should */
    if ((!collective || (item->file->my_rank == 0)) && (*obj_name_len > 0)) {
        const char *next_obj;
        unsigned    crt_intermed_grp = 0;

        /* Make sure obj is a group */
        if (obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot initiate traversal from non-group object");

        /* Determine if intermediate groups should be created */
        if (H5P_LINK_CREATE_DEFAULT == lcpl_id)
            crt_intermed_grp = H5_daos_plist_cache_g->lcpl_cache.crt_intermed_grp;
        else if (H5Pget_create_intermediate_group(lcpl_id, &crt_intermed_grp) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL,
                         "can't get intermediate group creation property value");

        /* Create copy of path for use by async tasks and make obj_name point
         * into it */
        if (NULL == (tmp_path_buf = DV_malloc(*obj_name_len + 1)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't allocate space for path");
        memcpy(tmp_path_buf, *obj_name, *obj_name_len);
        tmp_path_buf[*obj_name_len] = '\0';
        *obj_name                   = tmp_path_buf;

        /* Search for '/' */
        next_obj = strchr(*obj_name, '/');

        /* Traverse path */
        while (next_obj) {
            daos_obj_id_t **oid_ptr;
            ptrdiff_t       component_len;

            /* Calculate length of path component */
            component_len = next_obj - *obj_name;

            /* Advance obj_name_len to match next_obj */
            *obj_name_len -= (size_t)(component_len + 1);

            /* Skip past "." path element */
            if (!(component_len == 1 && (*obj_name)[0] == '.')) {
                /* Follow link to next group in path */
                assert(next_obj > *obj_name);
                assert(obj->item.type == H5I_GROUP);
                if (H5_daos_link_follow((H5_daos_group_t *)obj, *obj_name, (size_t)component_len,
                                        (hbool_t)crt_intermed_grp, req, &oid_ptr, NULL, first_task,
                                        dep_task) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, NULL, "can't follow link to group");

                /* Close previous group */
                if (H5_daos_group_close_real((H5_daos_group_t *)obj) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");
                obj = NULL;

                /* Start internal H5 operation for group open.  This will
                 * not be visible to the API, will not be added to an operation
                 * pool, and will be integrated into this function's task chain. */
                if (NULL == (int_int_req = H5_daos_req_create(item->file, "group open within group traversal",
                                                              NULL, NULL, req, H5I_INVALID_HID)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request");

                /* Allocate the group object that is returned to the user */
                if (NULL == (obj = H5FL_CALLOC(H5_daos_group_t)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct");

                /* Open next group in path */
                if (H5_daos_group_open_helper(item->file, (H5_daos_group_t *)obj, H5P_GROUP_ACCESS_DEFAULT,
                                              FALSE, int_int_req, first_task, dep_task) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group");

                /* Create task to finalize internal operation */
                if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                        NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL,
                                 "can't create task to finalize internal operation");

                /* Schedule finalize task (or save it to be scheduled later),
                 * give it ownership of int_int_req, and update task pointers */
                if (*first_task) {
                    if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL,
                                     "can't schedule task to finalize H5 operation: %s",
                                     H5_daos_err_to_string(ret));
                } /* end if */
                else
                    *first_task = int_int_req->finalize_task;
                *dep_task   = int_int_req->finalize_task;
                int_int_req = NULL;

                /* Retarget oid_ptr to grp->obj.oid so H5_daos_link_follow fills in
                 * the group's oid */
                *oid_ptr = &obj->oid;
            } /* end if */

            /* Advance to next path element */
            *obj_name = next_obj + 1;
            next_obj  = strchr(*obj_name, '/');
        } /* end while */

        /* Set path_buf */
        *path_buf    = tmp_path_buf;
        tmp_path_buf = NULL;

        assert(*obj_name_len == strlen(*obj_name));
        assert(*obj_name_len > 0);
    } /* end if */

    /* Set return value */
    ret_value = obj;

done:
    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close group */
        if (obj && H5_daos_object_close(&obj->item) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close object");

        /* Close internal request */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't free request");
        int_int_req = NULL;

        /* Free memory */
        tmp_path_buf = DV_free(tmp_path_buf);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!tmp_path_buf);
    assert(!int_int_req);

    D_FUNC_LEAVE;
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
    herr_t   ret_value = SUCCEED;

    assert(grp);

    /* Determine if this group is tracking link creation order */
    if (grp->gcpl_id == H5P_GROUP_CREATE_DEFAULT || grp->gcpl_id == H5P_FILE_CREATE_DEFAULT)
        corder_flags = H5_daos_plist_cache_g->gcpl_cache.link_corder_flags;
    else if (H5Pget_link_creation_order(grp->gcpl_id, &corder_flags) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't get link creation order flags");
    assert(!grp->gcpl_cache.track_corder);
    if (corder_flags & H5P_CRT_ORDER_TRACKED)
        grp->gcpl_cache.track_corder = TRUE;

done:
    D_FUNC_LEAVE;
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
H5_daos_group_create_helper(H5_daos_file_t *file, hbool_t is_root, hid_t gcpl_id, hid_t gapl_id,
                            H5_daos_group_t *parent_grp, const char *name, size_t name_len,
                            hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
                            tse_task_t **dep_task)
{
    H5_daos_group_t            *grp          = NULL;
    H5_daos_md_rw_cb_ud_flex_t *update_cb_ud = NULL;
    tse_task_t                 *group_metatask;
    int                         gmt_ndeps = 0;
    tse_task_t                 *gmt_deps[2];
    hbool_t default_gcpl = (gcpl_id == H5P_GROUP_CREATE_DEFAULT || gcpl_id == H5P_FILE_CREATE_DEFAULT);
    int     ret;
    void   *ret_value = NULL;

    assert(file);
    assert(file->flags & H5F_ACC_RDWR);
    assert(first_task);
    assert(dep_task);

    /* Allocate the group object that is returned to the user */
    if (NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct");
    grp->obj.item.type     = H5I_GROUP;
    grp->obj.item.created  = TRUE;
    grp->obj.item.open_req = req;
    req->rc++;
    grp->obj.item.file = file;
    grp->obj.item.rc   = 1;
    grp->obj.obj_oh    = DAOS_HDL_INVAL;
    grp->gcpl_id = (gcpl_id == H5P_FILE_CREATE_DEFAULT) ? H5P_FILE_CREATE_DEFAULT : H5P_GROUP_CREATE_DEFAULT;
    grp->gapl_id = H5P_GROUP_ACCESS_DEFAULT;

    if (is_root) {
        /* Generate an oid for the group */
        if (H5_daos_oid_generate(&grp->obj.oid, TRUE, H5_DAOS_OIDX_ROOT, H5I_GROUP,
                                 (default_gcpl ? H5P_DEFAULT : gcpl_id), H5_DAOS_OBJ_CLASS_NAME, file, TRUE,
                                 req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't generate object id");
    }
    else {
        /* Generate an oid for the group */
        if (H5_daos_oid_generate(&grp->obj.oid, FALSE, 0, H5I_GROUP, (default_gcpl ? H5P_DEFAULT : gcpl_id),
                                 H5_DAOS_OBJ_CLASS_NAME, file, collective, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't generate object id");
    }

    /* Open group object */
    if (H5_daos_obj_open(file, req, &grp->obj.oid, DAOS_OO_RW, &grp->obj.obj_oh, "group object open",
                         first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group object");

    /* Create group and write metadata if this process should */
    if (!collective || (file->my_rank == 0)) {
        size_t      gcpl_size = 0;
        void       *gcpl_buf  = NULL;
        tse_task_t *update_task;

        /* Create group */
        /* Determine serialized GCPL size if it is not default */
        if (!default_gcpl)
            if (H5Pencode2(gcpl_id, NULL, &gcpl_size, file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl");

        /* Allocate argument struct */
        if (NULL == (update_cb_ud = (H5_daos_md_rw_cb_ud_flex_t *)DV_calloc(
                         sizeof(H5_daos_md_rw_cb_ud_flex_t) + gcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for update callback arguments");

        /* Encode GCPL if not the default */
        if (!default_gcpl) {
            gcpl_buf = update_cb_ud->flex_buf;
            if (H5Pencode2(gcpl_id, gcpl_buf, &gcpl_size, file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl");
        } /* end if */
        else {
            gcpl_buf  = (gcpl_id == H5P_FILE_CREATE_DEFAULT) ? file->def_plist_cache.fcpl_buf
                                                             : file->def_plist_cache.gcpl_buf;
            gcpl_size = (gcpl_id == H5P_FILE_CREATE_DEFAULT) ? file->def_plist_cache.fcpl_size
                                                             : file->def_plist_cache.gcpl_size;
        } /* end else */

        /* Set up operation to write GCPL to group */
        /* Point to grp */
        update_cb_ud->md_rw_cb_ud.obj = &grp->obj;

        /* Point to req */
        update_cb_ud->md_rw_cb_ud.req = req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        update_cb_ud->md_rw_cb_ud.free_dkey = FALSE;

        /* Single iod and sgl */
        update_cb_ud->md_rw_cb_ud.nr = 1u;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[0].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_size = (uint64_t)gcpl_size;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;
        update_cb_ud->md_rw_cb_ud.free_akeys      = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[0], gcpl_buf, (daos_size_t)gcpl_size);
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[0];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[0]   = FALSE;

        /* Set task name */
        update_cb_ud->md_rw_cb_ud.task_name = "group metadata write";

        /* Create task for group metadata write */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_md_update_comp_cb, update_cb_ud, &update_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to write group metadata");

        /* Schedule group metadata write task and give it a reference to req and
         * the group */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to write group metadata: %s",
                         H5_daos_err_to_string(ret));
        req->rc++;
        grp->obj.item.rc++;
        update_cb_ud = NULL;

        /* Add dependency for group metatask */
        gmt_deps[gmt_ndeps] = update_task;
        gmt_ndeps++;

        /* Write link to group if requested */
        if (parent_grp) {
            H5_daos_link_val_t link_val;

            link_val.type             = H5L_TYPE_HARD;
            link_val.target.hard      = grp->obj.oid;
            link_val.target_oid_async = &grp->obj.oid;
            gmt_deps[gmt_ndeps]       = *dep_task;
            if (0 != (ret = H5_daos_link_write(parent_grp, name, name_len, &link_val, req, first_task,
                                               &gmt_deps[gmt_ndeps])))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create link to group: %s",
                             H5_daos_err_to_string(ret));
            gmt_ndeps++;
        } /* end if */
        else if (!is_root) {
            /* No link to group and it's not the root group, write a ref count
             * of 0 to grp */
            gmt_deps[gmt_ndeps] = *dep_task;
            if (0 !=
                (ret = H5_daos_obj_write_rc(NULL, &grp->obj, NULL, 0, req, first_task, &gmt_deps[gmt_ndeps])))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't write object ref count: %s",
                             H5_daos_err_to_string(ret));
            gmt_ndeps++;
        } /* end if */
    }     /* end if */
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
    } /* end else */

    /* Finish setting up group struct */
    if (!default_gcpl && (grp->gcpl_id = H5Pcopy(gcpl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gcpl");
    if ((gapl_id != H5P_GROUP_ACCESS_DEFAULT) && (grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Fill GCPL cache */
    if (H5_daos_group_fill_gcpl_cache(grp) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "failed to fill GCPL cache");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&grp->obj, grp->gcpl_id) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "failed to fill OCPL cache");

    ret_value = (void *)grp;

done:
    /* Create metatask to use for dependencies on this group create */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, (gmt_ndeps > 0) ? (unsigned)gmt_ndeps : 0,
                            (gmt_ndeps > 0) ? gmt_deps : NULL, NULL, NULL, NULL, &group_metatask) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create meta task for group create");
    /* Schedule group metatask (or save it to be scheduled later) */
    else {
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(group_metatask, false)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule group meta task: %s",
                             H5_daos_err_to_string(ret));
            else
                *dep_task = group_metatask;
        } /* end if */
        else {
            *first_task = group_metatask;
            *dep_task   = group_metatask;
        } /* end else */

        if (collective && (file->num_procs > 1))
            if (H5_daos_collective_error_check(&grp->obj, req, first_task, dep_task) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't perform collective error check");
    } /* end else */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value) {
        /* Close group */
        if (grp && H5_daos_group_close_real(grp) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");

        /* Free memory */
        if (update_cb_ud && update_cb_ud->md_rw_cb_ud.obj &&
            H5_daos_object_close(&update_cb_ud->md_rw_cb_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close object");
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    assert(!update_cb_ud);

    D_FUNC_LEAVE;
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
H5_daos_group_create(void *_item, const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
                     hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_item_t  *item            = (H5_daos_item_t *)_item;
    H5_daos_group_t *grp             = NULL;
    H5_daos_obj_t   *target_obj      = NULL;
    char            *path_buf        = NULL;
    const char      *target_name     = NULL;
    size_t           target_name_len = 0;
    hbool_t          collective;
    H5_daos_req_t   *int_req    = NULL;
    tse_task_t      *first_task = NULL;
    tse_task_t      *dep_task   = NULL;
    int              ret;
    void            *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "group parent object is NULL");
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
    H5_DAOS_GET_METADATA_WRITE_MODE(item->file, gapl_id, H5P_GROUP_ACCESS_DEFAULT, collective, H5E_SYM, NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "group create", item->open_req, NULL, NULL,
                                              H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, 0, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Traverse the path */
    /* Call this on every rank for now so errors are handled correctly.  If/when
     * we add a bcast to check for failure we could only call this on the lead
     * rank. */
    if (name) {
        /* Queue traverse tasks */
        if (NULL ==
            (target_obj = H5_daos_group_traverse(item, name, lcpl_id, int_req, collective, &path_buf,
                                                 &target_name, &target_name_len, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path");

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        /* Reject invalid object names during object creation - if a name is
         * given it must parse to a link name that can be created */
        if (target_name_len == 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, NULL, "path given does not resolve to a final link name");
    } /* end if */

    /* Create group and link to group */
    if (NULL == (grp = (H5_daos_group_t *)H5_daos_group_create_helper(
                     item->file, FALSE, gcpl_id, gapl_id, (H5_daos_group_t *)target_obj, target_name,
                     target_name_len, collective, int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create group");

    /* Set return value */
    ret_value = (void *)grp;

done:
    if (int_req) {
        H5_daos_op_pool_type_t op_type;

        /* Free path_buf if necessary */
        if (path_buf && H5_daos_free_async(path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTFREE, NULL, "can't free path buffer");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
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
         * need to add to new group's pool since it's the open request. */
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
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, NULL, "group creation failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close object");

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value)
        /* Close group */
        if (grp && H5_daos_group_close_real(grp) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");

    D_FUNC_LEAVE_API;
} /* end H5_daos_group_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_end
 *
 * Purpose:     Decode serialized group info from a buffer and fill caches.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_group_open_end(H5_daos_group_t *grp, uint8_t *p, uint64_t gcpl_buf_len)
{
    int ret_value = 0;

    assert(grp);
    assert(p);
    assert(gcpl_buf_len > 0);

    /* Check if the group's GCPL is the default GCPL or FCPL.
     * Otherwise, decode the group's GCPL.
     */
    if ((gcpl_buf_len == grp->obj.item.file->def_plist_cache.gcpl_size) &&
        !memcmp(p, grp->obj.item.file->def_plist_cache.gcpl_buf,
                grp->obj.item.file->def_plist_cache.gcpl_size))
        grp->gcpl_id = H5P_GROUP_CREATE_DEFAULT;
    else if ((gcpl_buf_len == grp->obj.item.file->def_plist_cache.fcpl_size) &&
             !memcmp(p, grp->obj.item.file->def_plist_cache.fcpl_buf,
                     grp->obj.item.file->def_plist_cache.fcpl_size))
        grp->gcpl_id = H5P_FILE_CREATE_DEFAULT;
    else if ((grp->gcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize GCPL");
    p += gcpl_buf_len;

    /* Fill GCPL cache */
    if (H5_daos_group_fill_gcpl_cache(grp) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_CPL_CACHE_ERROR, "failed to fill GCPL cache");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&grp->obj, grp->gcpl_id) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_group_open_end() */

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
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for group info broadcast task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast group info";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank == 0);
        assert(udata->bcast_udata.obj->item.type == H5I_GROUP);

        /* Reissue bcast if necessary */
        if (udata->bcast_udata.buffer_len != udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.count == H5_DAOS_GINFO_BUF_SIZE);
            assert(udata->bcast_udata.buffer_len > H5_DAOS_GINFO_BUF_SIZE);

            /* Use full buffer this time */
            udata->bcast_udata.count = udata->bcast_udata.buffer_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_group_open_bcast_comp_cb,
                                    udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second group info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret,
                             "can't schedule task for second group info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
    }     /* end if */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close group */
        if (udata->bcast_udata.obj && H5_daos_group_close_real((H5_daos_group_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast group info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for group info receive task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast group info";
    } /* end if */
    else if (task->dt_result == 0) {
        uint64_t gcpl_len;
        size_t   ginfo_len;
        uint8_t *p = udata->bcast_udata.buffer;

        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank > 0);
        assert(udata->bcast_udata.obj->item.type == H5I_GROUP);

        /* Decode oid */
        UINT64DECODE(p, udata->bcast_udata.obj->oid.lo)
        UINT64DECODE(p, udata->bcast_udata.obj->oid.hi)

        /* Decode GCPL length */
        UINT64DECODE(p, gcpl_len)

        /* Check for gcpl_len set to 0 - indicates failure */
        if (gcpl_len == 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR, "lead process failed to open group");

        /* Calculate data length */
        ginfo_len = (size_t)gcpl_len + 3 * sizeof(uint64_t);

        /* Reissue bcast if necessary */
        if (ginfo_len > (size_t)udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.buffer_len == H5_DAOS_GINFO_BUF_SIZE);
            assert(udata->bcast_udata.count == H5_DAOS_GINFO_BUF_SIZE);
            assert(udata->bcast_udata.buffer == udata->flex_buf);

            /* Realloc buffer */
            if (NULL == (udata->bcast_udata.buffer = DV_malloc(ginfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "failed to allocate memory for group info buffer");
            udata->bcast_udata.buffer_len = (int)ginfo_len;
            udata->bcast_udata.count      = (int)ginfo_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL, H5_daos_group_open_bcast_comp_cb,
                                    udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second group info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret,
                             "can't schedule task for second group info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
        else {
            /* Open group */
            if (0 != (ret = daos_obj_open(
                          udata->bcast_udata.obj->item.file->coh, udata->bcast_udata.obj->oid,
                          udata->bcast_udata.obj->item.file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO,
                          &udata->bcast_udata.obj->obj_oh, NULL /*event*/)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, ret, "can't open group: %s",
                             H5_daos_err_to_string(ret));

            /* Finish building group object */
            if (0 != (ret = H5_daos_group_open_end((H5_daos_group_t *)udata->bcast_udata.obj, p, gcpl_len)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't finish opening group");
        } /* end else */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close group */
        if (udata->bcast_udata.obj && H5_daos_group_close_real((H5_daos_group_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast group info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
    int                     ret;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for group info read task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->fetch_metatask);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;

        assert(udata->md_rw_cb_ud.req->file);
        assert(udata->md_rw_cb_ud.obj);
        assert(udata->md_rw_cb_ud.obj->item.type == H5I_GROUP);

        if (udata->bcast_udata) {
            assert(udata->bcast_udata->bcast_udata.buffer == udata->bcast_udata->flex_buf);

            /* Verify iod size makes sense */
            if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len !=
                (H5_DAOS_GINFO_BUF_SIZE - 3 * H5_DAOS_ENCODED_UINT64_T_SIZE))
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                             "buffer length does not match expected value");
            if (udata->md_rw_cb_ud.iod[0].iod_size <=
                (H5_DAOS_GINFO_BUF_SIZE - 3 * H5_DAOS_ENCODED_UINT64_T_SIZE))
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                             "invalid iod_size returned from DAOS (buffer should have been large enough)");

            /* Reallocate group info buffer */
            if (NULL == (udata->bcast_udata->bcast_udata.buffer = DV_malloc(
                             udata->md_rw_cb_ud.iod[0].iod_size + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "can't allocate buffer for serialized group info");
            udata->bcast_udata->bcast_udata.buffer_len =
                (int)(udata->md_rw_cb_ud.iod[0].iod_size + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE);

            /* Set up sgl */
            daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0],
                         (uint8_t *)udata->bcast_udata->bcast_udata.buffer +
                             3 * H5_DAOS_ENCODED_UINT64_T_SIZE,
                         udata->md_rw_cb_ud.iod[0].iod_size);
            udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        } /* end if */
        else {
            assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf == udata->flex_buf);

            /* Verify iod size makes sense */
            if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_GINFO_BUF_SIZE)
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                             "buffer length does not match expected value");
            if (udata->md_rw_cb_ud.iod[0].iod_size <= H5_DAOS_GINFO_BUF_SIZE)
                D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                             "invalid iod_size returned from DAOS (buffer should have been large enough)");

            /* Reallocate group info buffer */
            if (NULL ==
                (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(udata->md_rw_cb_ud.iod[0].iod_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "can't allocate buffer for serialized group info");

            /* Set up sgl */
            udata->md_rw_cb_ud.sg_iov[0].iov_buf_len = udata->md_rw_cb_ud.iod[0].iod_size;
            udata->md_rw_cb_ud.sg_iov[0].iov_len     = udata->md_rw_cb_ud.iod[0].iod_size;
            udata->md_rw_cb_ud.sgl[0].sg_nr_out      = 0;
            udata->md_rw_cb_ud.free_sg_iov[0]        = TRUE;
        } /* end else */

        /* Create task for reissued group metadata read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                     H5_daos_ginfo_read_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to read group metadata");

        /* Schedule group metadata read task and transfer ownership of udata */
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule task to read group metadata: %s",
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
            assert(udata->md_rw_cb_ud.req->file);
            assert(udata->md_rw_cb_ud.obj);
            assert(udata->md_rw_cb_ud.obj->item.type == H5I_GROUP);

            /* Check for missing metadata */
            if (udata->md_rw_cb_ud.iod[0].iod_size == 0)
                D_GOTO_ERROR(H5E_SYM, H5E_NOTFOUND, -H5_DAOS_DAOS_GET_ERROR, "internal metadata not found");

            if (udata->bcast_udata) {
                uint8_t *p;

                /* Encode oid */
                p = udata->bcast_udata->bcast_udata.buffer;
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.lo)
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.hi)

                /* Encode GCPL length */
                UINT64ENCODE(p, udata->md_rw_cb_ud.iod[0].iod_size)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Finish building group object */
            if (0 != (ret = H5_daos_group_open_end((H5_daos_group_t *)udata->md_rw_cb_ud.obj,
                                                   udata->md_rw_cb_ud.sg_iov[0].iov_buf,
                                                   (uint64_t)udata->md_rw_cb_ud.iod[0].iod_size)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't finish opening group");
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up if this is the last fetch task */
    if (udata) {
        /* Close group */
        if (udata->md_rw_cb_ud.obj && H5_daos_group_close_real((H5_daos_group_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");

        if (udata->bcast_udata) {
            /* Clear broadcast buffer if there was an error */
            if (udata->md_rw_cb_ud.req->status < -H5_DAOS_INCOMPLETE)
                (void)memset(udata->bcast_udata->bcast_udata.buffer, 0, H5_DAOS_GINFO_BUF_SIZE);
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
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->fetch_metatask) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

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
} /* end H5_daos_ginfo_read_comp_cb */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_helper
 *
 * Purpose:     Performs the actual group open. It is the responsibility
 *              of the calling function to make sure that the group's oid
 *              field is filled in before scheduled tasks are allowed to
 *              run.
 *
 * Return:      Success:        group object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
int
H5_daos_group_open_helper(H5_daos_file_t *file, H5_daos_group_t *grp, hid_t gapl_id, hbool_t collective,
                          H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_flex_t *bcast_udata = NULL;
    H5_daos_omd_fetch_ud_t       *fetch_udata = NULL;
    int                           ret, ret_value = 0;

    assert(file);
    assert(grp);
    assert(first_task);
    assert(dep_task);

    grp->obj.item.type     = H5I_GROUP;
    grp->obj.item.open_req = req;
    req->rc++;
    grp->obj.item.file = file;
    grp->obj.item.rc   = 1;
    grp->obj.obj_oh    = DAOS_HDL_INVAL;
    grp->gcpl_id       = H5P_GROUP_CREATE_DEFAULT;
    grp->gapl_id       = H5P_GROUP_ACCESS_DEFAULT;
    if ((gapl_id != H5P_GROUP_ACCESS_DEFAULT) && (grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, -H5_DAOS_H5_OPEN_ERROR, "failed to copy gapl");

    /* Set up broadcast user data */
    if (collective && (file->num_procs > 1)) {
        if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_flex_t *)DV_malloc(
                         sizeof(H5_daos_mpi_ibcast_ud_flex_t) + H5_DAOS_GINFO_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_H5_OPEN_ERROR,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->bcast_udata.req        = req;
        bcast_udata->bcast_udata.obj        = &grp->obj;
        bcast_udata->bcast_udata.buffer     = bcast_udata->flex_buf;
        bcast_udata->bcast_udata.buffer_len = H5_DAOS_GINFO_BUF_SIZE;
        bcast_udata->bcast_udata.count      = H5_DAOS_GINFO_BUF_SIZE;
        bcast_udata->bcast_udata.comm       = req->file->comm;
    } /* end if */

    /* Open group and read metadata if this process should */
    if (!collective || (file->my_rank == 0)) {
        tse_task_t *fetch_task = NULL;

        /* Open group object */
        if (H5_daos_obj_open(file, req, &grp->obj.oid, file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO,
                             &grp->obj.obj_oh, "group object open", first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, -H5_DAOS_H5_OPEN_ERROR, "can't open group object");

        /* Allocate argument struct for fetch task */
        if (NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(
                         sizeof(H5_daos_omd_fetch_ud_t) + (bcast_udata ? 0 : H5_DAOS_GINFO_BUF_SIZE))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_H5_OPEN_ERROR,
                         "can't allocate buffer for fetch callback arguments");

        /* Set up operation to read GCPL size from group */
        /* Set up ud struct */
        fetch_udata->md_rw_cb_ud.req = req;
        fetch_udata->md_rw_cb_ud.obj = &grp->obj;
        fetch_udata->bcast_udata     = bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Single iod and sgl */
        fetch_udata->md_rw_cb_ud.nr = 1u;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        fetch_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;
        fetch_udata->md_rw_cb_ud.free_akeys      = FALSE;

        /* Set up sgl */
        if (bcast_udata)
            daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0],
                         bcast_udata->flex_buf + 3 * H5_DAOS_ENCODED_UINT64_T_SIZE,
                         (daos_size_t)(H5_DAOS_GINFO_BUF_SIZE - 3 * H5_DAOS_ENCODED_UINT64_T_SIZE));
        else
            daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], fetch_udata->flex_buf,
                         (daos_size_t)(H5_DAOS_GINFO_BUF_SIZE));
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[0];
        fetch_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;

        /* Set conditional akey fetch for group metadata read operation */
        fetch_udata->md_rw_cb_ud.flags = DAOS_COND_AKEY_FETCH;

        /* Set task name */
        fetch_udata->md_rw_cb_ud.task_name = "group metadata read";

        /* Create meta task for group metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_ginfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &fetch_udata->fetch_metatask) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_H5_OPEN_ERROR,
                         "can't create meta task for group metadata read");

        /* Create task for group metadata read */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_ginfo_read_comp_cb, fetch_udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_H5_OPEN_ERROR,
                         "can't create task to read group metadata");

        /* Schedule meta task */
        if (0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_H5_OPEN_ERROR,
                         "can't schedule meta task for group metadata read: %s", H5_daos_err_to_string(ret));

        /* Schedule group metadata read task (or save it to be scheduled later)
         * and give it a reference to req and the group */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_H5_OPEN_ERROR,
                         "can't schedule task to read group metadata: %s", H5_daos_err_to_string(ret));
        *dep_task = fetch_udata->fetch_metatask;
        req->rc++;
        grp->obj.item.rc++;
        fetch_udata = NULL;
    } /* end if */
    else
        assert(bcast_udata);

done:
    /* Broadcast group info */
    if (bcast_udata) {
        if (H5_daos_mpi_ibcast(
                &bcast_udata->bcast_udata, &grp->obj, H5_DAOS_GINFO_BUF_SIZE, 0 != ret_value ? TRUE : FALSE,
                NULL, file->my_rank == 0 ? H5_daos_group_open_bcast_comp_cb : H5_daos_group_open_recv_comp_cb,
                req, first_task, dep_task) < 0) {
            DV_free(bcast_udata);
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_H5_OPEN_ERROR,
                         "failed to broadcast group info buffer");
        } /* end if */

        bcast_udata = NULL;
    } /* end if */

    /* Cleanup on failure */
    if (0 != ret_value) {
        /* Close group */
        if (grp && H5_daos_group_close_real(grp) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_OPEN_ERROR, "can't close group");

        /* Free memory */
        fetch_udata = DV_free(fetch_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!fetch_udata);
    assert(!bcast_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_group_open_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_int
 *
 * Purpose:     Internal version of H5_daos_group_open
 *
 * Return:      Success:        group object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
H5_daos_group_t *
H5_daos_group_open_int(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params, const char *name,
                       hid_t gapl_id, H5_daos_req_t *req, hbool_t collective, tse_task_t **first_task,
                       tse_task_t **dep_task)
{
    H5_daos_group_t *grp        = NULL;
    H5_daos_obj_t   *target_obj = NULL;
    daos_obj_id_t    oid        = {0, 0};
    daos_obj_id_t  **oid_ptr    = NULL;
    char            *path_buf   = NULL;
    hbool_t          must_bcast = FALSE;
    H5_daos_group_t *ret_value  = NULL;

    assert(item);
    assert(loc_params);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Check for open by object token */
    if (H5VL_OBJECT_BY_TOKEN == loc_params->type) {
        /* Generate oid from token */
        if (H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't convert object token to OID");
    } /* end if */
    else {
        const char *target_name = NULL;
        size_t      target_name_len;

        /* Open using name parameter */
        if (H5VL_OBJECT_BY_SELF != loc_params->type)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "unsupported group open location parameters type");
        if (!name)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "group name is NULL");

        /* At this point we must broadcast on failure */
        if (collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Traverse the path */
        if (NULL == (target_obj = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, req, collective,
                                                         &path_buf, &target_name, &target_name_len,
                                                         first_task, dep_task)))
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path");

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        /* Check for no target_name, in this case just return target_grp */
        if (target_name_len == 0) {
            /* Take ownership of target_obj */
            grp        = (H5_daos_group_t *)target_obj;
            target_obj = NULL;

            /* No need to bcast since everyone just opened the already open
             * group */
            must_bcast = FALSE;
        } /* end if */
        else if (!collective || (item->file->my_rank == 0)) {
            /* Follow link to group */
            if (H5_daos_link_follow((H5_daos_group_t *)target_obj, target_name, target_name_len, FALSE, req,
                                    &oid_ptr, NULL, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, NULL, "can't follow link to group");
        } /* end else */
    }     /* end else */

    /* Open group if not already open */
    if (!grp) {
        must_bcast = FALSE; /* Helper function will handle bcast */

        /* Allocate the group object that is returned to the user */
        if (NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct");

        if (H5_daos_group_open_helper(item->file, grp, gapl_id, collective, req, first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group");

        /* Set group oid */
        if (oid_ptr)
            /* Retarget *oid_ptr to grp->obj.oid so H5_daos_link_follow fills in
             * the group's oid */
            *oid_ptr = &grp->obj.oid;
        else if (H5VL_OBJECT_BY_TOKEN == loc_params->type)
            /* Just set the static oid from the token */
            grp->obj.oid = oid;
        else
            /* We will receive oid from lead process */
            assert(collective && item->file->my_rank > 0);
    } /* end if */

    /* Set return value */
    ret_value = grp;

done:
    /* Free path_buf if necessary */
    if (path_buf && H5_daos_free_async(path_buf, first_task, dep_task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTFREE, NULL, "can't free path buffer");

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close object");

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Broadcast failure */
        if (must_bcast && H5_daos_mpi_ibcast(NULL, &grp->obj, H5_DAOS_GINFO_BUF_SIZE, TRUE, NULL,
                                             item->file->my_rank == 0 ? H5_daos_group_open_bcast_comp_cb
                                                                      : H5_daos_group_open_recv_comp_cb,
                                             req, first_task, dep_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL,
                         "failed to broadcast empty group info buffer to signal failure");
        must_bcast = FALSE;

        /* Close group to prevent memory leaks since we're not returning it */
        if (grp && H5_daos_group_close_real(grp) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");
    } /* end if */

    assert(!must_bcast);

    D_FUNC_LEAVE;
} /* end H5_daos_group_open_int() */

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
H5_daos_group_open(void *_item, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id,
                   hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t  *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *grp  = NULL;
    hbool_t          collective;
    H5_daos_req_t   *int_req    = NULL;
    tse_task_t      *first_task = NULL;
    tse_task_t      *dep_task   = NULL;
    int              ret;
    void            *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "group parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    H5_DAOS_GET_METADATA_READ_MODE(item->file, gapl_id, H5P_GROUP_ACCESS_DEFAULT, collective, H5E_SYM, NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "group open", item->open_req, NULL, NULL, dxpl_id)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Call internal open routine */
    if (NULL == (grp = H5_daos_group_open_int(item, loc_params, name, gapl_id, int_req, collective,
                                              &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group");

    /* Set return value */
    ret_value = (void *)grp;

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (NULL == ret_value)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the parent group open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, item, H5_DAOS_OP_TYPE_READ, H5_DAOS_OP_SCOPE_OBJ,
                                collective, !req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, NULL, "group open failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* If we are not returning a group we must close it */
    if (ret_value == NULL && grp && H5_daos_group_close_real(grp) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group");

    D_FUNC_LEAVE_API;
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
H5_daos_group_get(void *_item, H5VL_group_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *grp        = (H5_daos_group_t *)_item;
    H5_daos_req_t   *int_req    = NULL;
    tse_task_t      *first_task = NULL;
    tse_task_t      *dep_task   = NULL;
    int              ret;
    herr_t           ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!get_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");
    if (H5I_FILE != grp->obj.item.type && H5I_GROUP != grp->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file or group");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(grp->obj.item.file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    switch (get_args->op_type) {
        /* H5Gget_create_plist */
        case H5VL_GROUP_GET_GCPL: {
            hid_t *ret_id = &get_args->args.get_gcpl.gcpl_id;

            /* Wait for the group to open if necessary */
            if (!grp->obj.item.created && grp->obj.item.open_req->status != 0) {
                if (H5_daos_progress(grp->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (grp->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            if ((*ret_id = H5Pcopy(grp->gcpl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get group's GCPL");

            /* Set group's object class on gcpl */
            if (H5_daos_set_oclass_from_oid(*ret_id, grp->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");

            break;
        } /* H5VL_GROUP_GET_GCPL */

        /* H5Gget_info(_by_name/by_idx) */
        case H5VL_GROUP_GET_INFO: {
            const H5VL_loc_params_t *loc_params = &get_args->args.get_info.loc_params;
            H5G_info_t              *group_info = get_args->args.get_info.ginfo;

            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(grp->obj.item.file, "group get info",
                                                      grp->obj.item.open_req, NULL, NULL, dxpl_id)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            if (H5_daos_group_get_info(grp, loc_params, group_info, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group's info");

            break;
        } /* H5VL_GROUP_GET_INFO */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported group get operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the group open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &grp->obj.item, H5_DAOS_OP_TYPE_READ,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "group get operation failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    D_FUNC_LEAVE_API;
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
H5_daos_group_specific(void *_item, H5VL_group_specific_args_t *specific_args, hid_t H5VL_DAOS_UNUSED dxpl_id,
                       void **req)
{
    H5_daos_group_t *grp        = (H5_daos_group_t *)_item;
    tse_task_t      *first_task = NULL;
    tse_task_t      *dep_task   = NULL;
    H5_daos_req_t   *int_req    = NULL;
    int              ret;
    herr_t           ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!specific_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");
    if (H5I_FILE != grp->obj.item.type && H5I_GROUP != grp->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file or group");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (specific_args->op_type) {
        /* H5Gflush */
        case H5VL_GROUP_FLUSH: {
            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(grp->obj.item.file, "group flush",
                                                      grp->obj.item.open_req, NULL, NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            if (H5_daos_group_flush(grp, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't flush group");

            break;
        } /* end block */

        /* H5Grefresh */
        case H5VL_GROUP_REFRESH:
            if (H5_daos_group_refresh(grp, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "can't refresh group");
            break;

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported group specific operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the dataset open if necessary.  For flush operations
         * use WRITE_ORDERED so all previous operations complete before the
         * flush and all subsequent operations start after the flush (this is
         * where we implement the barrier semantics for flush). */
        assert(specific_args->op_type == H5VL_GROUP_FLUSH);
        if (H5_daos_req_enqueue(int_req, first_task, &grp->obj.item, H5_DAOS_OP_TYPE_WRITE_ORDERED,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL,
                             "group specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* else */
    }     /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_group_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_close_real
 *
 * Purpose:     Internal version of H5_daos_group_close()
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
H5_daos_group_close_real(H5_daos_group_t *grp)
{
    int    ret;
    herr_t ret_value = SUCCEED;

    if (!grp)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group object is NULL");
    if (H5I_GROUP != grp->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a group");

    if (--grp->obj.item.rc == 0) {
        hbool_t close_gcpl = grp->gcpl_id != H5I_INVALID_HID && grp->gcpl_id != H5P_GROUP_CREATE_DEFAULT &&
                             grp->gcpl_id != H5P_FILE_CREATE_DEFAULT;

        /* Free group data structures */
        if (grp->obj.item.cur_op_pool)
            H5_daos_op_pool_free(grp->obj.item.cur_op_pool);
        if (grp->obj.item.open_req)
            if (H5_daos_req_free_int(grp->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't free request");
        if (!daos_handle_is_inval(grp->obj.obj_oh))
            if (0 != (ret = daos_obj_close(grp->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close group DAOS object: %s",
                             H5_daos_err_to_string(ret));
        if (close_gcpl && H5Idec_ref(grp->gcpl_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close gcpl");
        if (grp->gapl_id != H5I_INVALID_HID && grp->gapl_id != H5P_GROUP_ACCESS_DEFAULT)
            if (H5Idec_ref(grp->gapl_id) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close gapl");
        grp = H5FL_FREE(H5_daos_group_t, grp);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_group_close_real() */

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
H5_daos_group_close(void *_grp, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_group_t             *grp        = (H5_daos_group_t *)_grp;
    H5_daos_obj_close_task_ud_t *task_ud    = NULL;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    H5_daos_req_t               *int_req    = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_grp)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check if the group's request queue is NULL, if so we can close it
     * immediately.  Also close if the pool is empty and has no start task (and
     * hence does not depend on anything).  Also close if it is marked to close
     * nonblocking. */
    if (((grp->obj.item.open_req->status == 0 || grp->obj.item.open_req->status < -H5_DAOS_CANCELED) &&
         (!grp->obj.item.cur_op_pool || (grp->obj.item.cur_op_pool->type == H5_DAOS_OP_TYPE_EMPTY &&
                                         !grp->obj.item.cur_op_pool->start_task))) ||
        grp->obj.item.nonblocking_close) {

        if (H5_daos_group_close_real(grp) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");
        fflush(stdout);
    } /* end if */
    else {
        tse_task_t *close_task = NULL;

        /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
        if (NULL == (int_req = H5_daos_req_create(grp->obj.item.file, "group close", grp->obj.item.open_req,
                                                  NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_obj_close_task_ud_t *)DV_calloc(sizeof(H5_daos_obj_close_task_ud_t))))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't allocate space for close task udata struct");
        task_ud->req  = int_req;
        task_ud->item = &grp->obj.item;

        /* Create task to close group */
        if (H5_daos_create_task(H5_daos_object_close_task, 0, NULL, NULL, NULL, task_ud, &close_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to close group");

        /* Save task to be scheduled later and give it a reference to req and
         * grp */
        assert(!first_task);
        first_task = close_task;
        dep_task   = close_task;
        /* No need to take a reference to grp here since the purpose is to
         * release the API's reference */
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the group open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &grp->obj.item, H5_DAOS_OP_TYPE_CLOSE,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't add request to request queue");
        grp = NULL;

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "group close failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_group_close() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_flush
 *
 * Purpose:     Flushes a DAOS group.  Creates a barrier task so all async
 *              ops created before the flush execute before all async ops
 *              created after the flush.
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
H5_daos_group_flush(H5_daos_group_t H5VL_DAOS_UNUSED *grp, H5_daos_req_t H5VL_DAOS_UNUSED *req,
                    tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *barrier_task = NULL;
    herr_t      ret_value    = SUCCEED; /* Return value */

    assert(grp);

    /* Create task that does nothing but complete itself.  Only necessary
     * because we can't enqueue a request that has no tasks */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &barrier_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create barrier task for group flush");

    /* Schedule barrier task (or save it to be scheduled later)  */
    assert(!*first_task);
    *first_task = barrier_task;
    *dep_task   = barrier_task;

done:
    D_FUNC_LEAVE;
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

    D_GOTO_DONE(SUCCEED);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_group_refresh() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_get_info_task
 *
 * Purpose:     Asynchronous task for H5_daos_group_get_info().  Executes
 *              once target_obj is valid.
 *
 * Return:      Success:        0
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_group_get_info_task(tse_task_t *task)
{
    H5_daos_group_get_info_ud_t *udata      = NULL;
    tse_task_t                  *metatask   = NULL;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    int                          ret;
    int                          ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for group get info task");

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->req, H5E_SYM);

    /* Verify opened object is a group */
    if (udata->opened_type != H5I_GROUP)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, -H5_DAOS_BAD_VALUE, "opened object is not a group");

    /* Retrieve the group's info */
    udata->group_info->storage_type = H5G_STORAGE_TYPE_UNKNOWN;
    udata->group_info->nlinks       = 0;
    udata->group_info->max_corder   = 0;
    udata->group_info->mounted      = FALSE; /* DSINC - will file mounting be supported? */

    /* Retrieve group's max creation order value */
    if (((H5_daos_group_t *)udata->target_obj)->gcpl_cache.track_corder) {
        /* DSINC - no check for overflow for max_corder! */
        if (H5_daos_group_get_max_crt_order((H5_daos_group_t *)udata->target_obj,
                                            (uint64_t *)&udata->group_info->max_corder, udata->req,
                                            &first_task, &dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, -H5_DAOS_DAOS_GET_ERROR,
                         "can't get group's max creation order value");
    } /* end if */
    else
        udata->group_info->max_corder = -1;

    /* Retrieve the number of links in the group. */
    if (H5_daos_group_get_num_links((H5_daos_group_t *)udata->target_obj, &udata->group_info->nlinks,
                                    udata->req, &first_task, &dep_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, -H5_DAOS_DAOS_GET_ERROR, "can't get the number of links in group");

done:
    /* Clean up */
    if (udata) {
        /* Create metatask to complete this task after dep_task if necessary */
        if (dep_task) {
            /* Create metatask */
            if (H5_daos_create_task(H5_daos_metatask_autocomp_other, 1, &dep_task, NULL, NULL, task,
                                    &metatask) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create metatask for group get info");
            else {
                /* Schedule metatask */
                assert(first_task);
                if (0 != (ret = tse_task_schedule(metatask, false)))
                    D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule metatask for group get info: %s",
                                 H5_daos_err_to_string(ret));
            } /* end else */
        }     /* end if */

        /* Schedule first task */
        if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
            D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule initial task for group get info: %s",
                         H5_daos_err_to_string(ret));

        /* Close target_obj */
        if (H5_daos_group_close_real((H5_daos_group_t *)udata->target_obj) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");
        udata->target_obj = NULL;

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->req->status      = ret_value;
            udata->req->failed_task = "group get info task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    /* Complete task if necessary */
    if (!metatask) {
        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");
        tse_task_complete(task, ret_value);
    }

    D_FUNC_LEAVE;
} /* end H5_daos_group_get_info_task() */

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
H5_daos_group_get_info(H5_daos_group_t *grp, const H5VL_loc_params_t *loc_params, H5G_info_t *group_info,
                       H5_daos_req_t *req, tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_group_get_info_ud_t *task_udata    = NULL;
    tse_task_t                  *get_info_task = NULL;
    H5_daos_req_t               *int_int_req   = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    assert(grp);
    assert(loc_params);
    assert(group_info);

    /* Allocate task udata struct */
    if (NULL == (task_udata = (H5_daos_group_get_info_ud_t *)DV_calloc(sizeof(H5_daos_group_get_info_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate group get info user data");
    task_udata->req        = req;
    task_udata->group_info = group_info;

    /* Determine the target group */
    switch (loc_params->type) {
        /* H5Gget_info */
        case H5VL_OBJECT_BY_SELF: {
            /* Use item as group, or the root group if item is a file */
            if (grp->obj.item.type == H5I_FILE)
                task_udata->target_obj = &((H5_daos_file_t *)grp)->root_grp->obj;
            else if (grp->obj.item.type == H5I_GROUP)
                task_udata->target_obj = &grp->obj;
            else
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a file or group");

            task_udata->target_obj->item.rc++;

            task_udata->opened_type = H5I_GROUP;

            break;
        } /* H5VL_OBJECT_BY_SELF */

        /* H5Gget_info_by_name */
        case H5VL_OBJECT_BY_NAME: {
            H5VL_loc_params_t sub_loc_params;

            /* Start internal H5 operation for target object open.  This will
             * not be visible to the API, will not be added to an operation
             * pool, and will be integrated into this function's task chain. */
            if (NULL == (int_int_req = H5_daos_req_create(grp->obj.item.file,
                                                          "target object open within group get info by name",
                                                          NULL, NULL, req, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Open target group */
            sub_loc_params.obj_type = grp->obj.item.type;
            sub_loc_params.type     = H5VL_OBJECT_BY_SELF;
            if (NULL == (task_udata->target_obj = (H5_daos_obj_t *)H5_daos_group_open_int(
                             &grp->obj.item, &sub_loc_params, loc_params->loc_data.loc_by_name.name,
                             H5P_GROUP_ACCESS_DEFAULT, int_int_req, FALSE, first_task, dep_task)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group");

            /* Create task to finalize internal operation */
            if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                    NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

            /* Schedule finalize task (or save it to be scheduled later),
             * give it ownership of int_int_req, and update task pointers */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL,
                                 "can't schedule task to finalize H5 operation: %s",
                                 H5_daos_err_to_string(ret));
            } /* end if */
            else
                *first_task = int_int_req->finalize_task;
            *dep_task   = int_int_req->finalize_task;
            int_int_req = NULL;

            task_udata->opened_type = H5I_GROUP;

            break;
        } /* H5VL_OBJECT_BY_NAME */

        /* H5Gget_info_by_idx */
        case H5VL_OBJECT_BY_IDX: {
            /* Start internal H5 operation for target object open.  This will
             * not be visible to the API, will not be added to an operation
             * pool, and will be integrated into this function's task chain. */
            if (NULL == (int_int_req = H5_daos_req_create(grp->obj.item.file,
                                                          "target object open within group get info by index",
                                                          NULL, NULL, req, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            /* Open target object */
            if (H5_daos_object_open_helper(&grp->obj.item, loc_params, &task_udata->opened_type, FALSE, NULL,
                                           &task_udata->target_obj, int_int_req, first_task, dep_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group");

            /* Create task to finalize internal operation */
            if (H5_daos_create_task(H5_daos_h5op_finalize, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                    NULL, NULL, int_int_req, &int_int_req->finalize_task) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to finalize internal operation");

            /* Schedule finalize task (or save it to be scheduled later),
             * give it ownership of int_int_req, and update task pointers */
            if (*first_task) {
                if (0 != (ret = tse_task_schedule(int_int_req->finalize_task, false)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL,
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
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid loc_params type");
    } /* end switch */

    /* Create task to finish this operation */
    if (H5_daos_create_task(H5_daos_group_get_info_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, task_udata, &get_info_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task for group get info");

    /* Schedule get info task (or save it to be scheduled later) and give it a
     * reference to req and udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(get_info_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't schedule task for group get info: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = get_info_task;
    *dep_task = get_info_task;
    req->rc++;
    task_udata = NULL;

done:
    /* Clean up */
    if (task_udata) {
        assert(ret_value < 0);

        if (task_udata->target_obj && H5_daos_object_close(&task_udata->target_obj->item) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close object");

        /* Close internal request for target object open */
        if (int_int_req && H5_daos_req_free_int(int_int_req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't free request");

        task_udata = DV_free(task_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_group_get_info() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_gnl_task
 *
 * Purpose:     Asynchronous task for H5_daos_group_get_num_links.
 *              Executes once target_grp is valid.
 *
 * Return:      Success:        The number of links within the group
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_group_gnl_task(tse_task_t *task)
{
    H5_daos_group_gnl_ud_t *udata              = NULL;
    hid_t                   target_grp_id      = H5I_INVALID_HID;
    tse_task_t             *first_task         = NULL;
    tse_task_t             *dep_task           = NULL;
    hbool_t                 metatask_scheduled = FALSE;
    int                     ret;
    int                     ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for get num links task");

    assert(udata->md_rw_cb_ud.obj->item.type == H5I_GROUP);

    /* Handle errors in previous tasks */
    H5_DAOS_PREP_REQ(udata->md_rw_cb_ud.req, H5E_SYM);

    /* If creation order is tracked, read number of links directly, otherwise
     * iterate over links, counting them */
    if (((H5_daos_group_t *)udata->md_rw_cb_ud.obj)->gcpl_cache.track_corder) {
        tse_task_t *fetch_task = NULL;

        /* Read the "number of links" key from the target group */

        /* Set up dkey */
        daos_const_iov_set((d_const_iov_t *)&udata->md_rw_cb_ud.dkey, H5_daos_link_corder_key_g,
                           H5_daos_link_corder_key_size_g);

        /* Set nr */
        udata->md_rw_cb_ud.nr = 1;

        /* Set up iod */
        daos_const_iov_set((d_const_iov_t *)&udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_nlinks_key_g,
                           H5_daos_nlinks_key_size_g);
        udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE;
        udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&udata->md_rw_cb_ud.sg_iov[0], udata->nlinks_buf,
                     (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE);
        udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        udata->md_rw_cb_ud.sgl[0].sg_iovs   = &udata->md_rw_cb_ud.sg_iov[0];
        udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;

        /* Do not free buffers */
        udata->md_rw_cb_ud.free_akeys = FALSE;
        udata->md_rw_cb_ud.free_dkey  = FALSE;

        /* Set task name */
        udata->md_rw_cb_ud.task_name = "group get num links fetch";

        /* Create task for num links fetch */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                     H5_daos_group_gnl_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR, "can't create task to read num links");

        /* Save fetch task to be scheduled later and transfer ownership of udata */
        assert(!first_task);
        first_task = fetch_task;
        udata      = NULL;
    } /* end if */
    else {
        tse_task_t         *metatask = NULL;
        H5_daos_iter_data_t iter_data;

        /* Iterate through links */

        /* Register id for grp */
        if ((target_grp_id = H5VLwrap_register((H5_daos_group_t *)udata->md_rw_cb_ud.obj, H5I_GROUP)) < 0)
            D_GOTO_ERROR(H5E_ID, H5E_CANTREGISTER, -H5_DAOS_SETUP_ERROR, "unable to atomize object handle");
        udata->md_rw_cb_ud.obj->item.rc++;

        /* Initialize iteration data */
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, H5_INDEX_NAME, H5_ITER_NATIVE, FALSE, NULL,
                               target_grp_id, udata->nlinks, NULL, udata->md_rw_cb_ud.req);
        iter_data.u.link_iter_data.u.link_iter_op = H5_daos_link_iterate_count_links_callback;

        /* Retrieve the number of links in the group. */
        /* Note that all arguments to H5_daos_link_iterate have ref counts
         * incremented or are copied, so we can free udata in this function
         * without waiting */
        if (H5_daos_link_iterate((H5_daos_group_t *)udata->md_rw_cb_ud.obj, &iter_data, &first_task,
                                 &dep_task) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, -H5_DAOS_SETUP_ERROR,
                         "can't retrieve the number of links in group");

        /* Create metatask to complete this task after dep_task if necessary */
        if (dep_task) {
            /* Create metatask */
            if (H5_daos_create_task(H5_daos_metatask_autocomp_other, 1, &dep_task, NULL, NULL, task,
                                    &metatask) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create metatask for group get num links");

            /* Schedule metatask */
            assert(first_task);
            if (0 != (ret = tse_task_schedule(metatask, false)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, ret,
                             "can't schedule metatask for group get num links: %s",
                             H5_daos_err_to_string(ret));
            metatask_scheduled = TRUE;
        } /* end if */
    }     /* end else */

done:
    /* Close group ID.  No need to mark as nonblocking close since the ID rc
     * shouldn't drop to 0. */
    if ((target_grp_id >= 0) && (H5Idec_ref(target_grp_id) < 0))
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group ID");

    /* Schedule first task */
    if (first_task && 0 != (ret = tse_task_schedule(first_task, false)))
        D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, ret, "can't schedule initial task for group get num links: %s",
                     H5_daos_err_to_string(ret));

    /* Cleanup udata if we still own it */
    if (udata) {
        /* Close target_grp */
        if (H5_daos_group_close_real((H5_daos_group_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");
        udata->md_rw_cb_ud.obj = NULL;

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "group get num links task";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Complete task if it's not being handled by the metatask */
        if (!metatask_scheduled) {
            /* Return task to task list */
            if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                             "can't return task to task list");
            tse_task_complete(task, ret_value);
        }

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */
    else
        assert(ret_value >= 0 || ret_value == -H5_DAOS_DAOS_GET_ERROR);

    D_FUNC_LEAVE;
} /* end H5_daos_group_gnl_task() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_gnl_comp_cb
 *
 * Purpose:     Completion callback for group get num links fetch (from
 *              creation order data)
 *
 * Return:      Success:        0
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_group_gnl_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_group_gnl_ud_t *udata    = NULL;
    uint64_t                nlinks64 = 0;
    uint8_t                *p;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for get num links task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        p = udata->nlinks_buf;

        /* Check for no num links found, in this case it must be 0 */
        if (udata->md_rw_cb_ud.iod[0].iod_size == (uint64_t)0)
            nlinks64 = 0;
        else
            /* Decode num links */
            UINT64DECODE(p, nlinks64);

        /* Set output value */
        *udata->nlinks = (hsize_t)nlinks64;
    } /* end else */

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, udata->gnl_task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Complete main task */
    tse_task_complete(udata->gnl_task, ret_value);

    /* Close target_grp */
    if (H5_daos_group_close_real((H5_daos_group_t *)udata->md_rw_cb_ud.obj) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");
    udata->md_rw_cb_ud.obj = NULL;

    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Handle errors in this function */
    /* Do not place any code that can issue errors after this block, except for
     * H5_daos_req_free_int, which updates req->status if it sees an error */
    if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = ret_value;
        udata->md_rw_cb_ud.req->failed_task = "get group num links completion callback";
    } /* end if */

    /* Release our reference to req */
    if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
        D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

    /* Free udata */
    udata = DV_free(udata);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_group_gnl_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_get_num_links
 *
 * Purpose:     Retrieves the current number of links within the target
 *              group.  The fields within target_grp are not guaranteed to
 *              be valid until after *dep_task (as passed to this
 *              function) is complete.  *nlinks is not guaranteed to be
 *              valid until after *dep_task (as returned from this
 *              function) is complete.
 *
 * Return:      Success:        The number of links within the group
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_group_get_num_links(H5_daos_group_t *target_grp, hsize_t *nlinks, H5_daos_req_t *req,
                            tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_group_gnl_ud_t *gnl_udata = NULL;
    int                     ret;
    herr_t                  ret_value = SUCCEED;

    assert(target_grp);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_LINKS_SIZE == 8);

    /* Allocate task udata struct */
    if (NULL == (gnl_udata = (H5_daos_group_gnl_ud_t *)DV_calloc(sizeof(H5_daos_group_gnl_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate get num links user data");
    gnl_udata->md_rw_cb_ud.req = req;
    gnl_udata->md_rw_cb_ud.obj = &target_grp->obj;
    gnl_udata->nlinks          = nlinks;

    /* Create task to finish this operation */
    if (H5_daos_create_task(H5_daos_group_gnl_task, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL, NULL,
                            NULL, gnl_udata, &gnl_udata->gnl_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task for get num links");

    /* Schedule gnl task (or save it to be scheduled later) and give it a
     * reference to the group, req and udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(gnl_udata->gnl_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't schedule task for get num links: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = gnl_udata->gnl_task;
    *dep_task = gnl_udata->gnl_task;
    target_grp->obj.item.rc++;
    req->rc++;
    gnl_udata = NULL;

done:
    /* Clean up */
    if (gnl_udata) {
        assert(ret_value < 0);
        gnl_udata = DV_free(gnl_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_group_get_num_links() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_gmco_comp_cb
 *
 * Purpose:     Completion callback for asynchronous fetch of group max
 *              creation order.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_group_gmco_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_group_gmco_ud_t *udata     = NULL;
    int                      ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for link get creation order by name task");

    /* Handle errors in fetch task.  Only record error in udata->req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->md_rw_cb_ud.req->status      = task->dt_result;
        udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
    } /* end if */
    else if (task->dt_result == 0) {
        /* Check that creation order is tracked for target group */
        /* Move this check to a custom prep cb? */
        if (!((H5_daos_group_t *)udata->md_rw_cb_ud.obj)->gcpl_cache.track_corder) {
            udata->md_rw_cb_ud.req->status      = -H5_DAOS_BAD_VALUE;
            udata->md_rw_cb_ud.req->failed_task = udata->md_rw_cb_ud.task_name;
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "creation order is not tracked for group");
        } /* end if */

        uint64_t max_corder_val;

        /* Check for no max creation order found, in this case it must be 0 */
        if (udata->md_rw_cb_ud.iod[0].iod_size == 0)
            max_corder_val = (uint64_t)0;
        else {
            uint8_t *p;

            /* Decode max creation order */
            p = udata->max_corder_buf;
            UINT64DECODE(p, max_corder_val);
        } /* end else */

        /* Set output value */
        *udata->max_corder = max_corder_val;
    } /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR, "can't return task to task list");

    /* Clean up */
    if (udata) {
        /* Close target_grp */
        if (udata->md_rw_cb_ud.obj) {
            if (H5_daos_group_close_real((H5_daos_group_t *)udata->md_rw_cb_ud.obj) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close group");
            udata->md_rw_cb_ud.obj = NULL;
        } /* end if */

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except for
         * H5_daos_req_free_int, which updates req->status if it sees an error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->md_rw_cb_ud.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->md_rw_cb_ud.req->status      = ret_value;
            udata->md_rw_cb_ud.req->failed_task = "get link creation order by name completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->md_rw_cb_ud.req) < 0)
            D_DONE_ERROR(H5E_IO, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Free udata */
        udata = DV_free(udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_group_gmco_comp_cb() */

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
H5_daos_group_get_max_crt_order(H5_daos_group_t *target_grp, uint64_t *max_corder, H5_daos_req_t *req,
                                tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_group_gmco_ud_t *fetch_udata = NULL;
    tse_task_t              *fetch_task  = NULL;
    int                      ret;
    herr_t                   ret_value = SUCCEED;

    assert(target_grp);
    assert(max_corder);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Allocate task udata struct */
    if (NULL == (fetch_udata = (H5_daos_group_gmco_ud_t *)DV_calloc(sizeof(H5_daos_group_gmco_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                     "can't allocate get creation order by name user data");
    fetch_udata->md_rw_cb_ud.req = req;
    fetch_udata->md_rw_cb_ud.obj = &target_grp->obj;
    fetch_udata->max_corder      = max_corder;

    /* Set up dkey */
    daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.dkey, H5_daos_link_corder_key_g,
                       H5_daos_link_corder_key_size_g);

    /* Set nr */
    fetch_udata->md_rw_cb_ud.nr = 1;

    /* Set up iod */
    daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[0].iod_name,
                       H5_daos_max_link_corder_key_g, H5_daos_max_link_corder_key_size_g);
    fetch_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
    fetch_udata->md_rw_cb_ud.iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE;
    fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], fetch_udata->max_corder_buf,
                 (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
    fetch_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
    fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
    fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[0];
    fetch_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;

    /* Do not free buffers */
    fetch_udata->md_rw_cb_ud.free_akeys = FALSE;
    fetch_udata->md_rw_cb_ud.free_dkey  = FALSE;

    /* Set task name */
    fetch_udata->md_rw_cb_ud.task_name = "group get max creation order";

    /* Create task for max creation order fetch */
    if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, *dep_task ? 1 : 0, *dep_task ? dep_task : NULL,
                                 H5_daos_md_rw_prep_cb, H5_daos_group_gmco_comp_cb, fetch_udata,
                                 &fetch_task) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to read max creation order");

    /* Schedule fetch task (or save it to be scheduled later) and give it a
     * reference to the group, req and udata */
    if (*first_task) {
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL,
                         "can't schedule task for group get max creation order: %s",
                         H5_daos_err_to_string(ret));
    } /* end if */
    else
        *first_task = fetch_task;
    *dep_task = fetch_task;
    target_grp->obj.item.rc++;
    req->rc++;
    fetch_udata = NULL;

done:
    /* Clean up */
    if (fetch_udata) {
        assert(ret_value < 0);
        fetch_udata = DV_free(fetch_udata);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_group_get_max_crt_order() */
