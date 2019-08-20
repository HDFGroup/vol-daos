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
 * library. Link routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Macros */
#define H5_DAOS_HARD_LINK_VAL_SIZE 17

/* Prototypes */
static herr_t H5_daos_link_read(H5_daos_group_t *grp, const char *name,
    size_t name_len, H5_daos_link_val_t *val);
static herr_t H5_daos_link_get_info(H5_daos_item_t *item, const char *link_path,
    H5L_info_t *link_info, hid_t dxpl_id, void **req);
static herr_t H5_daos_link_delete(H5_daos_item_t *item, const char *link_path,
    hid_t dxpl_id, void **req);


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_read
 *
 * Purpose:     Reads the specified link from the given group.  Note that
 *              if the returned link is a soft link, val->target.soft must
 *              eventually be freed.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_read(H5_daos_group_t *grp, const char *name, size_t name_len,
    H5_daos_link_val_t *val)
{
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    uint8_t *val_buf;
    uint8_t val_buf_static[H5_DAOS_LINK_VAL_BUF_SIZE];
    uint8_t *val_buf_dyn = NULL;
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(grp);
    assert(name);
    assert(val);

    /* Use static link value buffer initially */
    val_buf = val_buf_static;

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)name, (daos_size_t)name_len);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_link_key_g, H5_daos_link_key_size_g);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, val_buf, (daos_size_t)H5_DAOS_LINK_VAL_BUF_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read link */
    if(0 != (ret = daos_obj_fetch(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link: %s", H5_daos_err_to_string(ret))

    /* Check for no link found */
    if(iod.iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "link not found")

    /* Check if val_buf was large enough */
    if(iod.iod_size > (uint64_t)H5_DAOS_LINK_VAL_BUF_SIZE) {
        /* Allocate new value buffer */
        if(NULL == (val_buf_dyn = (uint8_t *)DV_malloc(iod.iod_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link value buffer")

        /* Point to new buffer */
        val_buf = val_buf_dyn;
        daos_iov_set(&sg_iov, val_buf, (daos_size_t)iod.iod_size);

        /* Reissue read */
        if(0 != (ret = daos_obj_fetch(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps */, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link: %s", H5_daos_err_to_string(ret))
    } /* end if */

    /* Decode link type */
    p = val_buf;
    val->type = (H5L_type_t)*p++;

    /* Decode remainder of link value */
    switch(val->type) {
        case H5L_TYPE_HARD:
            /* Decode oid */
            UINT64DECODE(p, val->target.hard.lo)
            UINT64DECODE(p, val->target.hard.hi)

            break;

        case H5L_TYPE_SOFT:
            /* If we had to allocate a buffer to read from daos, it happens to
             * be the exact size (len + 1) we need for the soft link value,
             * take ownership of it and shift the value down one byte.
             * Otherwise, allocate a new buffer. */
            if(val_buf_dyn) {
                val->target.soft = (char *)val_buf_dyn;
                val_buf_dyn = NULL;
                memmove(val->target.soft, val->target.soft + 1, iod.iod_size - 1);
            } /* end if */
            else {
                if(NULL == (val->target.soft = (char *)DV_malloc(iod.iod_size)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link value buffer")
                memcpy(val->target.soft, val_buf + 1, iod.iod_size - 1);
            } /* end else */

            /* Add null terminator */
            val->target.soft[iod.iod_size - 1] = '\0';

            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */

done:
    if(val_buf_dyn) {
        assert(ret_value == FAIL);
        DV_free(val_buf_dyn);
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_read() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_write
 *
 * Purpose:     Writes the specified link to the given group
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_link_write(H5_daos_group_t *grp, const char *name,
    size_t name_len, H5_daos_link_val_t *val, H5_daos_req_t *req,
    tse_task_t **taskp)
{
    H5_daos_md_update_cb_ud_t *update_cb_ud = NULL;
    hbool_t update_task_scheduled = FALSE;
    char *name_buf = NULL;
    uint8_t *iov_buf = NULL;
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(grp);
    assert(name);
    assert(val);

    /* Check for write access */
    if(!(grp->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Allocate argument struct */
    if(NULL == (update_cb_ud = (H5_daos_md_update_cb_ud_t *)DV_calloc(sizeof(H5_daos_md_update_cb_ud_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for update callback arguments")

    /* Copy name */
    if(NULL == (name_buf = (char *)DV_malloc(name_len)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't allocate space for name buffer")
    (void)memcpy(name_buf, name, name_len);

    /* Set up known fields of update_cb_ud */
    update_cb_ud->req = req;
    update_cb_ud->obj = &grp->obj;
    grp->obj.item.rc++;
    update_cb_ud->nr = 1;

    /* Set up dkey */
    daos_iov_set(&update_cb_ud->dkey, (void *)name_buf, (daos_size_t)name_len);
    update_cb_ud->free_dkey = TRUE;

    /* Encode type specific value information */
    switch(val->type) {
         case H5L_TYPE_HARD:
            assert(H5_DAOS_HARD_LINK_VAL_SIZE == sizeof(val->target.hard) + 1);

            /* Allocate iov_buf */
            if(NULL == (iov_buf = (uint8_t *)DV_malloc(H5_DAOS_HARD_LINK_VAL_SIZE)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't allocate space for link target")
            p = iov_buf;

            /* Encode link type */
            *p++ = (uint8_t)H5L_TYPE_HARD;

            /* Encode oid */
            UINT64ENCODE(p, val->target.hard.lo)
            UINT64ENCODE(p, val->target.hard.hi)

            update_cb_ud->iod[0].iod_size = (uint64_t)H5_DAOS_HARD_LINK_VAL_SIZE;

            /* Set up type specific sgl */
            daos_iov_set(&update_cb_ud->sg_iov[0], iov_buf, (daos_size_t)H5_DAOS_HARD_LINK_VAL_SIZE);
            update_cb_ud->sgl[0].sg_nr = 1;
            update_cb_ud->sgl[0].sg_nr_out = 0;

            break;

        case H5L_TYPE_SOFT:
            /* We need an extra byte for the link type (encoded above). */
            update_cb_ud->iod[0].iod_size = (uint64_t)(strlen(val->target.soft) + 1);

            /* Allocate iov_buf */
            if(NULL == (iov_buf = (uint8_t *)DV_malloc((size_t)update_cb_ud->iod[0].iod_size)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't allocate space for link target")
            p = iov_buf;

            /* Encode link type */
            *p++ = (uint8_t)H5L_TYPE_SOFT;

            /* Copy target name */
            (void)memcpy(p, val->target.soft, (size_t)(update_cb_ud->iod[0].iod_size - (uint64_t)1));

            /* Set up type specific sgl.  We use two entries, the first for the
             * link type, the second for the string. */
            daos_iov_set(&update_cb_ud->sg_iov[0], iov_buf, update_cb_ud->iod[0].iod_size);
            update_cb_ud->sgl[0].sg_nr = 1;
            update_cb_ud->sgl[0].sg_nr_out = 0;

            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */

    /* Finish setting up iod */
    daos_iov_set(&update_cb_ud->iod[0].iod_name, H5_daos_link_key_g, H5_daos_link_key_size_g);
    daos_csum_set(&update_cb_ud->iod[0].iod_kcsum, NULL, 0);
    update_cb_ud->iod[0].iod_nr = 1u;
    update_cb_ud->iod[0].iod_type = DAOS_IOD_SINGLE;
    update_cb_ud->free_akeys = FALSE;

    /* Set up general sgl */
    update_cb_ud->sgl[0].sg_iovs = &update_cb_ud->sg_iov[0];

    /* Set task name */
    update_cb_ud->task_name = "link write";

    /* Create task for link write */
    if(0 != (ret = daos_task_create(DAOS_OPC_OBJ_UPDATE, &grp->obj.item.file->sched, 0, NULL, taskp)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create task to write link: %s", H5_daos_err_to_string(ret))

    /* Set callback functions for link write */
    if(0 != (ret = tse_task_register_cbs(*taskp, H5_daos_md_update_prep_cb, NULL, 0, H5_daos_md_update_comp_cb, NULL, 0)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't register callbacks for task to write link: %s", H5_daos_err_to_string(ret))

    /* Set private data for link write */
    (void)tse_task_set_priv(*taskp, update_cb_ud);

    /* Schedule link task and give it a reference to req */
    if(0 != (ret = tse_task_schedule(*taskp, false)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't schedule task to write link: %s", H5_daos_err_to_string(ret))
    update_task_scheduled = TRUE;
    update_cb_ud->req->rc++;

done:
    /* Cleanup on failure */
    if(!update_task_scheduled) {
        assert(ret_value < 0);
        if(update_cb_ud && update_cb_ud->obj && H5_daos_object_close(update_cb_ud->obj, -1, NULL) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close object")
        name_buf = DV_free(name_buf);
        iov_buf = DV_free(iov_buf);
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_write() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_create
 *
 * Purpose:     Creates a hard/soft/UD/external links.
 *              For now, only Soft Links are Supported.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_link_create(H5VL_link_create_type_t create_type, void *_item,
    const H5VL_loc_params_t *loc_params, hid_t H5VL_DAOS_UNUSED lcpl_id,
    hid_t H5VL_DAOS_UNUSED lapl_id, hid_t dxpl_id, void **req, va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *link_grp = NULL;
    H5_daos_obj_t *target_obj = NULL;
    const char *link_name = NULL;
    H5_daos_link_val_t link_val;
    tse_task_t *link_write_task;
    tse_task_t *finalize_task;
    int finalize_ndeps = 0;
    tse_task_t *finalize_deps[2];
    H5_daos_req_t *int_req = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")
    if(loc_params->type != H5VL_OBJECT_BY_NAME)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters type is not H5VL_OBJECT_BY_NAME")

    switch(create_type) {
        case H5VL_LINK_CREATE_HARD:
        {
            H5_daos_obj_t *target_obj_loc = va_arg(arguments, void *);
            H5VL_loc_params_t *target_obj_loc_params = va_arg(arguments, H5VL_loc_params_t *);

            /* Determine the target location object in which to place
             * the new link. If item is NULL here, H5L_SAME_LOC was
             * used as the third parameter to H5Lcreate_hard, so the
             * target location object is actually the object passed
             * in from the va_arg list. */
            if(!item)
                item = (H5_daos_item_t *) target_obj_loc;

            /* Determine the target location object for the object
             * that the hard link is to point to. If target_obj_loc
             * is NULL here, H5L_SAME_LOC was used as the first
             * parameter to H5Lcreate_hard, so the target location
             * object is actually the VOL object that was passed
             * into this callback as a function parameter.
             */
            if(target_obj_loc == NULL)
                target_obj_loc = (H5_daos_obj_t *) item;

            if(H5VL_OBJECT_BY_NAME == target_obj_loc_params->type) {
                /* Attempt to open the hard link's target object */
                if(NULL == (target_obj = H5_daos_object_open((H5_daos_item_t *) target_obj_loc,
                        target_obj_loc_params, NULL, dxpl_id, req)))
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "couldn't open hard link's target object")
            }
            else {
                /* H5Olink */
                assert(H5VL_OBJECT_BY_SELF == target_obj_loc_params->type);
                target_obj = target_obj_loc;
            }

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = target_obj->oid;

            /*
             * TODO: if the link write succeeds, the link ref. count for
             * the target object should be incremented.
             */

            break;
        } /* H5VL_LINK_CREATE_HARD */

        case H5VL_LINK_CREATE_SOFT:
            if(!_item)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype parent object is NULL")

            /* Retrieve target name */
            link_val.type = H5L_TYPE_SOFT;
            link_val.target.soft = va_arg(arguments, const char *);

            break;

        case H5VL_LINK_CREATE_UD:
            D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, FAIL, "UD link creation not supported")
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "invalid link creation call")
    } /* end switch */

    assert(item);

    /* Start H5 operation */
    if(NULL == (int_req = (H5_daos_req_t *)DV_malloc(sizeof(H5_daos_req_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for request")
    int_req->th = DAOS_TX_NONE;
    int_req->th_open = FALSE;
    int_req->file = item->file;
    int_req->file->item.rc++;
    int_req->rc = 1;
    int_req->status = H5_DAOS_INCOMPLETE;
    int_req->failed_task = NULL;

    /* Find target group */
    if(NULL == (link_grp = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name, dxpl_id, req, &link_name, NULL, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't traverse path")

    /* Create link */
    if(H5_daos_link_write(link_grp, link_name, strlen(link_name), &link_val, int_req, &link_write_task) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_WRITEERROR, FAIL, "can't create link")
    finalize_deps[finalize_ndeps] = link_write_task;
    finalize_ndeps++;

done:
    /* Close link group */
    if(link_grp && H5_daos_group_close(link_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
    if(target_obj && H5_daos_object_close(target_obj, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")

    if(int_req) {
        /* Create task to finalize H5 operation */
        if(0 != (ret = tse_task_create(H5_daos_h5op_finalize, &item->file->sched, int_req, &finalize_task)))
            D_DONE_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Register dependencies (if any) */
        else if(finalize_ndeps > 0 && 0 != (ret = tse_task_register_deps(finalize_task, finalize_ndeps, finalize_deps)))
            D_DONE_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "can't create dependencies for task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        /* Schedule finalize task */
        else if(0 != (ret = tse_task_schedule(finalize_task, false)))
            D_DONE_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s", H5_daos_err_to_string(ret))
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* Block until operation completes */
        {
            bool is_empty;

            /* Wait for scheduler to be empty *//* Change to custom progress function DSINC */
            if(0 != (ret = daos_progress(&item->file->sched, DAOS_EQ_WAIT, &is_empty)))
                D_DONE_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "can't progress scheduler: %s", H5_daos_err_to_string(ret))

            /* Check for failure */
            if(int_req->status < 0)
                D_DONE_ERROR(H5E_LINK, H5E_CANTOPERATE, FAIL, "link creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))
        } /* end block */

        /* Close internal request */
        H5_daos_req_free_int(int_req);
    } /* end if */

    D_FUNC_LEAVE_API
} /* end H5_daos_link_create() */


herr_t
H5_daos_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t H5VL_DAOS_UNUSED lcpl,
    hid_t H5VL_DAOS_UNUSED lapl, hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    herr_t ret_value = SUCCEED;

    if(!src_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object location is NULL")
    if(!loc_params1)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL")
    if(!dst_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object location is NULL")
    if(!loc_params2)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL")

    D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "link copying is unsupported")

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_link_copy() */


herr_t
H5_daos_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t H5VL_DAOS_UNUSED lcpl,
    hid_t H5VL_DAOS_UNUSED lapl, hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    herr_t ret_value = SUCCEED;

    if(!src_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object location is NULL")
    if(!loc_params1)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL")
    if(!dst_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object location is NULL")
    if(!loc_params2)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL")

    D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "link moving is unsupported")

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_link_move() */


herr_t
H5_daos_link_get(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    switch (get_type) {
        case H5VL_LINK_GET_INFO:
        {
            H5L_info_t *link_info = va_arg(arguments, H5L_info_t *);

            if(H5VL_OBJECT_BY_NAME != loc_params->type)
                D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, FAIL, "H5Lget_info_by_idx is unsupported")

            if(H5_daos_link_get_info(item, loc_params->loc_data.loc_by_name.name, link_info, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't retrieve link's info")

            break;
        } /* H5VL_LINK_GET_INFO */

        case H5VL_LINK_GET_NAME:
        case H5VL_LINK_GET_VAL:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported link get operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_link_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_specific
 *
 * Purpose:     Specific operations with links
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
H5_daos_link_specific(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *target_grp = NULL;
    hid_t target_grp_id = -1;
    herr_t ret_value = SUCCEED;    /* Return value */

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    switch (specific_type) {
        /* H5Lexists */
        case H5VL_LINK_EXISTS:
            {
                htri_t *lexists_ret = va_arg(arguments, htri_t *);

                assert(H5VL_OBJECT_BY_NAME == loc_params->type);

                if((*lexists_ret = H5_daos_link_exists(item, loc_params->loc_data.loc_by_name.name, dxpl_id, req)) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if link exists")

                break;
            } /* end block */

        /* H5Literate/visit(_by_name) */
        case H5VL_LINK_ITER:
            {
                H5_daos_iter_data_t iter_data;
                int is_recursive = va_arg(arguments, int);
                H5_index_t idx_type = (H5_index_t) va_arg(arguments, int);
                H5_iter_order_t iter_order = (H5_iter_order_t) va_arg(arguments, int);
                hsize_t *idx_p = va_arg(arguments, hsize_t *);
                H5L_iterate_t iter_op = va_arg(arguments, H5L_iterate_t);
                void *op_data = va_arg(arguments, void *);

                switch (loc_params->type) {
                    /* H5Literate/H5Lvisit */
                    case H5VL_OBJECT_BY_SELF:
                    {
                        /* Use item as attribute parent object, or the root group if item is a
                         * file */
                        if(item->type == H5I_GROUP)
                            target_grp = (H5_daos_group_t *)item;
                        else if(item->type == H5I_FILE)
                            target_grp = ((H5_daos_file_t *)item)->root_grp;
                        else
                            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a file or group")

                        target_grp->obj.item.rc++;
                        break;
                    } /* H5VL_OBJECT_BY_SELF */

                    /* H5Literate_by_name/H5Lvisit_by_name */
                    case H5VL_OBJECT_BY_NAME:
                    {
                        H5VL_loc_params_t sub_loc_params;

                        /* Open target_grp */
                        sub_loc_params.obj_type = item->type;
                        sub_loc_params.type = H5VL_OBJECT_BY_SELF;
                        if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_open(item, &sub_loc_params, loc_params->loc_data.loc_by_name.name, loc_params->loc_data.loc_by_name.lapl_id, dxpl_id, req)))
                            D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group for link operation")

                        break;
                    } /* H5VL_OBJECT_BY_NAME */

                    case H5VL_OBJECT_BY_IDX:
                    case H5VL_OBJECT_BY_ADDR:
                    case H5VL_OBJECT_BY_REF:
                    default:
                        D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid loc_params type")
                } /* end switch */

                /* Register id for target_grp */
                if((target_grp_id = H5VLwrap_register(target_grp, H5I_GROUP)) < 0)
                    D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")

                /* Initialize iteration data */
                H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, idx_type, iter_order,
                        is_recursive, idx_p, target_grp_id, op_data, dxpl_id, req);
                iter_data.u.link_iter_data.link_iter_op = iter_op;

                if((ret_value = H5_daos_link_iterate(target_grp, &iter_data)) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration failed")

                break;
            } /* end block */

        /* H5Ldelete(_by_idx) */
        case H5VL_LINK_DELETE:
        {
            if(H5VL_OBJECT_BY_IDX == loc_params->type)
                D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "H5Ldelete_by_idx is unsupported")

            if(H5_daos_link_delete(item, loc_params->loc_data.loc_by_name.name, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTREMOVE, FAIL, "failed to delete link")

            break;
        } /* H5VL_LINK_DELETE */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid specific operation")
    } /* end switch */

done:
    if(target_grp_id >= 0) {
        if(H5Idec_ref(target_grp_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group ID")
        target_grp_id = -1;
        target_grp = NULL;
    } /* end if */
    else if(target_grp) {
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        target_grp = NULL;
    } /* end else */

    D_FUNC_LEAVE_API
} /* end H5_daos_link_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_follow
 *
 * Purpose:     Follows the link in grp identified with name, and returns
 *              in oid the oid of the target object.
 *
 * Return:      Success:        TRUE (for hard links and soft/external
 *                              links which resolve) or FALSE (for soft
 *                              links which do not resolve)
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5_daos_link_follow(H5_daos_group_t *grp, const char *name,
    size_t name_len, hid_t dxpl_id, void **req, daos_obj_id_t *oid)
{
    H5_daos_link_val_t link_val;
    hbool_t link_val_alloc = FALSE;
    H5_daos_group_t *target_grp = NULL;
    htri_t ret_value = TRUE;

    assert(grp);
    assert(name);
    assert(oid);

    /* Read link to group */
   if(H5_daos_link_read(grp, name, name_len, &link_val) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link")

    switch(link_val.type) {
       case H5L_TYPE_HARD:
            /* Simply return the read oid */
            *oid = link_val.target.hard;

            break;

        case H5L_TYPE_SOFT:
            {
                const char *target_name = NULL;

                link_val_alloc = TRUE;

                /* Traverse the soft link path */
                if(NULL == (target_grp = H5_daos_group_traverse(&grp->obj.item, link_val.target.soft, dxpl_id, req, &target_name, NULL, NULL)))
                    D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't traverse path")

                /* Check for no target_name, in this case just return
                 * target_grp's oid */
                if(target_name[0] == '\0'
                        || (target_name[0] == '.' && name_len == (size_t)1))
                    *oid = target_grp->obj.oid;
                else {
                    htri_t link_resolves;

                    /* Attempt to follow the last element in the path */
                    H5E_BEGIN_TRY {
                        link_resolves = H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, oid);
                    } H5E_END_TRY;

                    if(link_resolves < 0)
                        D_GOTO_DONE(FALSE)
                } /* end else */

                break;
            } /* end block */

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
           D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */

done:
    /* Clean up */
    if(link_val_alloc) {
        assert(link_val.type == H5L_TYPE_SOFT);
        DV_free(link_val.target.soft);
    } /* end if */

    if(target_grp)
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_link_follow() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_info
 *
 * Purpose:     Helper routine to retrieve a link's info and populate a
 *              H5L_info_t struct.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_get_info(H5_daos_item_t *item, const char *link_path,
    H5L_info_t *link_info, hid_t dxpl_id, void **req)
{
    H5_daos_link_val_t link_val;
    H5_daos_group_t *target_grp = NULL;
    H5L_info_t local_link_info;
    const char *target_name;
    herr_t ret_value = SUCCEED;

    assert(link_info);

    /* Traverse the path */
    if(NULL == (target_grp = H5_daos_group_traverse(item, link_path, dxpl_id, req, &target_name, NULL, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "failed to traverse path")

    if(H5_daos_link_read(target_grp, target_name, strlen(target_name), &link_val) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "failed to read link")

    local_link_info.type = link_val.type;

    /* TODO */
    if(H5L_TYPE_HARD == link_val.type)
        local_link_info.u.address = HADDR_UNDEF;
    else if(H5L_TYPE_SOFT == link_val.type || H5L_TYPE_EXTERNAL == link_val.type)
        local_link_info.u.val_size = 0;

    /* TODO Retrieve the link's creation order and mark the order as valid */
    local_link_info.corder = -1;
    local_link_info.corder_valid = FALSE;

    /* Only ASCII character set is supported currently */
    local_link_info.cset = H5T_CSET_ASCII;

    memcpy(link_info, &local_link_info, sizeof(*link_info));

done:
    if(target_grp) {
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        target_grp = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_exists
 *
 * Purpose:     Helper routine to determine if a link exists by the given
 *              pathname from the specified object.
 *
 * Return:      Success:        TRUE if the link exists or FALSE if it does
 *                              not exist.
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5_daos_link_exists(H5_daos_item_t *item, const char *link_path, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_key_t dkey;
    daos_iod_t iod;
    int ret;
    htri_t ret_value = FALSE;

    /* Traverse the path */
    if(NULL == (target_grp = H5_daos_group_traverse(item, link_path, dxpl_id, req, &target_name, NULL, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "can't traverse path")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)target_name, strlen(target_name));

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_link_key_g, H5_daos_link_key_size_g);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Read link */
    if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, NULL /*sgl*/, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link: %s", H5_daos_err_to_string(ret))

    /* Set return value */
    ret_value = iod.iod_size != (uint64_t)0;

done:
    if(target_grp) {
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        target_grp = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_exists() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_iterate
 *
 * Purpose:     Iterates over the links in the specified group, using the
 *              supplied iter_data struct for the iteration parameters.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_link_iterate(H5_daos_group_t *target_grp, H5_daos_iter_data_t *iter_data)
{
    H5_daos_obj_t *target_obj = NULL;
    daos_anchor_t anchor;
    uint32_t nr;
    daos_key_desc_t kds[H5_DAOS_ITER_LEN];
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    H5_daos_link_val_t link_val;
    H5L_info_t linfo;
    herr_t op_ret;
    char tmp_char;
    char *dkey_buf = NULL;
    size_t dkey_buf_len = 0;
    char *p;
    int ret;
    uint32_t i;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(iter_data);

    /* Iteration restart not supported */
    if(iter_data->idx_p && (*iter_data->idx_p != 0))
        D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)")

    /* Native iteration order is currently associated with increasing order; decreasing order iteration is not currently supported */
    if(iter_data->iter_order == H5_ITER_DEC)
        D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "decreasing iteration order not supported (order must be H5_ITER_NATIVE or H5_ITER_INC)")

    /* Recursive iteration not supported */
    if(iter_data->is_recursive)
        D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "recursive iteration not supported")

    /* Initialize const linfo info */
    linfo.corder_valid = FALSE;
    linfo.corder = 0;
    linfo.cset = H5T_CSET_ASCII;

    /* Initialize anchor */
    memset(&anchor, 0, sizeof(anchor));

    /* Allocate dkey_buf */
    if(NULL == (dkey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkeys")
    dkey_buf_len = H5_DAOS_ITER_SIZE_INIT;

    /* Set up sgl.  Report size as 1 less than buffer size so we
     * always have room for a null terminator. */
    daos_iov_set(&sg_iov, dkey_buf, (daos_size_t)(dkey_buf_len - 1));
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Loop to retrieve keys and make callbacks */
    do {
        /* Loop to retrieve keys (exit as soon as we get at least 1 key) */
        do {
            /* Reset nr */
            nr = H5_DAOS_ITER_LEN;

            /* Ask daos for a list of dkeys, break out if we succeed
             */
            if(0 == (ret = daos_obj_list_dkey(target_grp->obj.obj_oh, DAOS_TX_NONE, &nr, kds, &sgl, &anchor, NULL /*event*/)))
                break;

            /* Call failed, if the buffer is too small double it and
             * try again, otherwise fail */
            if(ret == -DER_KEY2BIG) {
                char *tmp_realloc;

                /* Allocate larger buffer */
                dkey_buf_len *= 2;
                if(NULL == (tmp_realloc = (char *)DV_realloc(dkey_buf, dkey_buf_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkeys")
                dkey_buf = tmp_realloc;

                /* Update sgl */
                daos_iov_set(&sg_iov, dkey_buf, (daos_size_t)(dkey_buf_len - 1));
            } /* end if */
            else
                D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve attributes: %s", H5_daos_err_to_string(ret))
        } while(1);

        /* Loop over returned dkeys */
        p = dkey_buf;
        op_ret = 0;
        for(i = 0; (i < nr) && (op_ret == 0); i++) {
            /* Check if this key represents a link */
            if(p[0] != '/') {
                /* Add null terminator temporarily */
                tmp_char = p[kds[i].kd_key_len];
                p[kds[i].kd_key_len] = '\0';

                /* Read link */
                if(H5_daos_link_read(target_grp, p, (size_t)kds[i].kd_key_len, &link_val) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link")

                /* Update linfo */
                linfo.type = link_val.type;
                if(link_val.type == H5L_TYPE_HARD)
                    linfo.u.address = (haddr_t)link_val.target.hard.lo;
                else {
                    assert(link_val.type == H5L_TYPE_SOFT);
                    linfo.u.val_size = strlen(link_val.target.soft) + 1;

                    /* Free soft link value */
                    link_val.target.soft = (char *)DV_free(link_val.target.soft);
                } /* end else */

                if(H5_DAOS_ITER_TYPE_LINK == iter_data->iter_type) {
                    /* Make callback */
                    if((op_ret = iter_data->u.link_iter_data.link_iter_op(iter_data->iter_root_obj, p, &linfo, iter_data->op_data)) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, op_ret, "operator function returned failure")
                } /* end if */
                else if(H5_DAOS_ITER_TYPE_OBJ == iter_data->iter_type) {
                    H5VL_loc_params_t sub_loc_params;
                    htri_t link_resolves = TRUE;

                    if(H5L_TYPE_SOFT == link_val.type)
                        /* Check that the soft link resolves before opening the target object */
                        if((link_resolves = H5_daos_link_follow(target_grp, p, strlen(p), iter_data->dxpl_id, NULL, NULL)) < 0)
                            D_GOTO_ERROR(H5E_LINK, H5E_TRAVERSE, FAIL, "can't follow link")

                    if(link_resolves) {
                        /*
                         * Open the target object.
                         */

                        sub_loc_params.type = H5VL_OBJECT_BY_NAME;
                        sub_loc_params.loc_data.loc_by_name.name = p;
                        sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
                        if(NULL == (target_obj = H5_daos_object_open(target_grp, &sub_loc_params, NULL, iter_data->dxpl_id, iter_data->req)))
                            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "can't open object")

                        iter_data->u.obj_iter_data.obj_name = p;
                        if(H5_daos_object_visit(target_obj, iter_data) < 0)
                            D_GOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "failed to visit object")

                        if(H5_daos_object_close(target_obj, iter_data->dxpl_id, iter_data->req) < 0)
                            D_GOTO_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")
                        target_obj = NULL;
                    } /* end if */
                } /* end else */

                /* Replace null terminator */
                p[kds[i].kd_key_len] = tmp_char;

                /* Advance idx */
                if(iter_data->idx_p)
                    (*iter_data->idx_p)++;
            } /* end if */

            /* Advance to next akey */
            p += kds[i].kd_key_len + kds[i].kd_csum_len;
        } /* end for */
    } while(!daos_anchor_is_eof(&anchor) && (op_ret == 0));

    ret_value = op_ret;

done:
    if(target_obj) {
        if(H5_daos_object_close(target_obj, iter_data->dxpl_id, iter_data->req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")
        target_obj = NULL;
    } /* end if */

    dkey_buf = (char *)DV_free(dkey_buf);

    D_FUNC_LEAVE
} /* end H5_daos_link_iterate() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_iterate_count_links_callback
 *
 * Purpose:     A callback for H5_daos_link_iterate() that simply counts
 *              the number of links in the given group.
 *
 * Return:      0 (can't fail)
 *
 * Programmer:  Jordan Henderson
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_link_iterate_count_links_callback(hid_t H5VL_DAOS_UNUSED group, const char H5VL_DAOS_UNUSED *name,
    const H5L_info_t H5VL_DAOS_UNUSED *info, void *op_data)
{
    (*((hsize_t *) op_data))++;
    return 0;
} /* end H5_daos_link_iterate_count_links_callback() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_delete
 *
 * Purpose:     Deletes the link specified by the given link pathname from
 *              the specified object.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_delete(H5_daos_item_t *item, const char *link_path, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_key_t dkey;
    int ret;
    herr_t ret_value = SUCCEED;

    /* Traverse the path */
    if(NULL == (target_grp = H5_daos_group_traverse(item, link_path, dxpl_id, req, &target_name, NULL, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "can't traverse path")

    /* Setup dkey */
    daos_iov_set(&dkey, (void *)target_name, strlen(target_name));

    /* Punch the link's dkey, along with all of its akeys */
    if(0 != (ret = daos_obj_punch_dkeys(target_grp->obj.obj_oh, DAOS_TX_NONE, 1, &dkey, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL, "failed to punch link dkey: %s", H5_daos_err_to_string(ret))

    /* TODO: If no more hard links point to the object in question, it should be
     * removed from the file, or at least marked to be removed.
     */

done:
    if(target_grp) {
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        target_grp = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_delete() */
