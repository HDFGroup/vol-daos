/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Neil Fortner <nfortne2@hdfgroup.org>
 *              September, 2016
 *
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library.  Link routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */
#include "daos_vol_config.h"    /* DAOS connector configuration header     */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Prototypes */
static herr_t H5_daos_link_read(H5_daos_group_t *grp, const char *name,
    size_t name_len, H5_daos_link_val_t *val);


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
    char const_link_key[] = H5_DAOS_LINK_KEY;
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
 
    /* Use static link value buffer initially */
    val_buf = val_buf_static;

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)name, (daos_size_t)name_len);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, const_link_key, (daos_size_t)(sizeof(const_link_key) - 1));
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
                memmove(val->target.soft,  val->target.soft + 1, iod.iod_size - 1);
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
    size_t name_len, H5_daos_link_val_t *val)
{
    char const_link_key[] = H5_DAOS_LINK_KEY;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov[2];
    uint8_t iov_buf[17];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    /* Check for write access */
    if(!(grp->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)name, (daos_size_t)name_len);

    /* Encode link type */
    p = iov_buf;
    *p++ = (uint8_t)val->type;

    /* Initialized iod */
    memset(&iod, 0, sizeof(iod));

    /* Encode type specific value information */
    switch(val->type) {
         case H5L_TYPE_HARD:
            assert(sizeof(iov_buf) == sizeof(val->target.hard) + 1);

            /* Encode oid */
            UINT64ENCODE(p, val->target.hard.lo)
            UINT64ENCODE(p, val->target.hard.hi)

            iod.iod_size = (uint64_t)17;

            /* Set up type specific sgl */
            daos_iov_set(&sg_iov[0], iov_buf, (daos_size_t)sizeof(iov_buf));
            sgl.sg_nr = 1;
            sgl.sg_nr_out = 0;

            break;

        case H5L_TYPE_SOFT:
            /* We need an extra byte for the link type (encoded above). */
            iod.iod_size = (uint64_t)(strlen(val->target.soft) + 1);

            /* Set up type specific sgl.  We use two entries, the first for the
             * link type, the second for the string. */
            daos_iov_set(&sg_iov[0], iov_buf, (daos_size_t)1);
            daos_iov_set(&sg_iov[1], val->target.soft, (daos_size_t)(iod.iod_size - (uint64_t)1));
            sgl.sg_nr = 2;
            sgl.sg_nr_out = 0;

            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */


    /* Finish setting up iod */
    daos_iov_set(&iod.iod_name, const_link_key, (daos_size_t)(sizeof(const_link_key) - 1));
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up general sgl */
    sgl.sg_iovs = sg_iov;

    /* Write link */
    if(0 != (ret = daos_obj_update(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't write link: %s", H5_daos_err_to_string(ret))

done:
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
    const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t DV_ATTR_UNUSED lapl_id,
    hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *link_grp = NULL;
    const char *link_name = NULL;
    H5_daos_link_val_t link_val;
    herr_t ret_value = SUCCEED;

    assert(loc_params->type == H5VL_OBJECT_BY_NAME);

    /* Find target group */
    if(item)
        if(NULL == (link_grp = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name, dxpl_id, req, &link_name, NULL, NULL)))
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't traverse path")

    switch(create_type) {
        case H5VL_LINK_CREATE_HARD:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "hard link creation not supported")

            break;

        case H5VL_LINK_CREATE_SOFT:
            /* Retrieve target name */
            link_val.type = H5L_TYPE_SOFT;
            if(H5Pget(lcpl_id, H5VL_PROP_LINK_TARGET_NAME, &link_val.target.soft) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get property value for soft link target name")

            /* Create soft link */
            if(H5_daos_link_write(link_grp, link_name, strlen(link_name), &link_val) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create soft link")

            break;

        case H5VL_LINK_CREATE_UD:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "UD link creation not supported")
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "invalid link creation call")
    } /* end switch */

done:
    /* Close link group */
    if(link_grp && H5_daos_group_close(link_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_link_create() */


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
    char *dkey_buf = NULL;
    size_t dkey_buf_len = 0;
    int ret;
    herr_t ret_value = SUCCEED;    /* Return value */

    switch (specific_type) {
        /* H5Lexists */
        case H5VL_LINK_EXISTS:
            {
                htri_t *lexists_ret = va_arg(arguments, htri_t *);
                const char *target_name = NULL;
                char const_link_key[] = H5_DAOS_LINK_KEY;
                daos_key_t dkey;
                daos_iod_t iod;

                assert(H5VL_OBJECT_BY_NAME == loc_params->type);

                /* Traverse the path */
                if(NULL == (target_grp = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name, dxpl_id, req, &target_name, NULL, NULL)))
                    D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't traverse path")

                /* Set up dkey */
                daos_iov_set(&dkey, (void *)target_name, strlen(target_name));

                /* Set up iod */
                memset(&iod, 0, sizeof(iod));
                daos_iov_set(&iod.iod_name, const_link_key, (daos_size_t)(sizeof(const_link_key) - 1));
                daos_csum_set(&iod.iod_kcsum, NULL, 0);
                iod.iod_nr = 1u;
                iod.iod_size = DAOS_REC_ANY;
                iod.iod_type = DAOS_IOD_SINGLE;

                /* Read link */
                if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, NULL /*sgl*/, NULL /*maps*/, NULL /*event*/)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link: %s", H5_daos_err_to_string(ret))

                /* Set return value */
                *lexists_ret = iod.iod_size != (uint64_t)0;

                break;
            } /* end block */

#ifdef DV_HAVE_LINK_ITERATION
        case H5VL_LINK_ITER:
            {
                hbool_t recursive = va_arg(arguments, int);
                H5_index_t DV_ATTR_UNUSED idx_type = (H5_index_t)va_arg(arguments, int);
                H5_iter_order_t order = (H5_iter_order_t)va_arg(arguments, int);
                hsize_t *idx = va_arg(arguments, hsize_t *);
                H5L_iterate_t op = va_arg(arguments, H5L_iterate_t);
                void *op_data = va_arg(arguments, void *);
                daos_anchor_t anchor;
                uint32_t nr;
                daos_key_desc_t kds[H5_DAOS_ITER_LEN];
                daos_sg_list_t sgl;
                daos_iov_t sg_iov;
                H5_daos_link_val_t link_val;
                H5L_info_t linfo;
                herr_t op_ret;
                char tmp_char;
                char *p;
                uint32_t i;

                /* Determine the target group */
                if(loc_params->type == H5VL_OBJECT_BY_SELF) {
                    /* Use item as attribute parent object, or the root group if item is a
                     * file */
                    if(item->type == H5I_GROUP)
                        target_grp = (H5_daos_group_t *)item;
                    else if(item->type == H5I_FILE)
                        target_grp = ((H5_daos_file_t *)item)->root_grp;
                    else
                        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a file or group")
                    target_grp->obj.item.rc++;
                } /* end if */
                else if(loc_params->type == H5VL_OBJECT_BY_NAME) {
                    H5VL_loc_params_t sub_loc_params;

                    /* Open target_grp */
                    sub_loc_params.obj_type = item->type;
                    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
                    if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_open(item, &sub_loc_params, loc_params->loc_data.loc_by_name.name, loc_params->loc_data.loc_by_name.lapl_id, dxpl_id, req)))
                        D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group for link operation")
                } /* end else */

                /* Iteration restart not supported */
                if(idx && (*idx != 0))
                    D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)")

                /* Ordered iteration not supported */
                if(order != H5_ITER_NATIVE)
                    D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "ordered iteration not supported (order must be H5_ITER_NATIVE)")

                /* Recursive iteration not supported */
                if(recursive)
                    D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "recursive iteration not supported")

                /* Initialize const linfo info */
                linfo.corder_valid = FALSE;
                linfo.corder = 0;
                linfo.cset = H5T_CSET_ASCII;

                /* Register id for target_grp */
                if((target_grp_id = H5VLregister(H5I_GROUP, target_grp, H5_DAOS_g)) < 0)
                    D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")

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
                    /* Loop to retrieve keys (exit as soon as we get at least 1
                     * key) */
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
                            /* Allocate larger buffer */
                            DV_free(dkey_buf);
                            dkey_buf_len *= 2;
                            if(NULL == (dkey_buf = (char *)DV_malloc(dkey_buf_len)))
                                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkeys")

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

                            /* Make callback */
                            if((op_ret = op(target_grp_id, p, &linfo, op_data)) < 0)
                                D_GOTO_ERROR(H5E_SYM, H5E_BADITER, op_ret, "operator function returned failure")

                            /* Replace null terminator */
                            p[kds[i].kd_key_len] = tmp_char;

                            /* Advance idx */
                            if(idx)
                                (*idx)++;
                        } /* end if */

                        /* Advance to next akey */
                        p += kds[i].kd_key_len + kds[i].kd_csum_len;
                    } /* end for */
                } while(!daos_anchor_is_eof(&anchor) && (op_ret == 0));

                /* Set return value */
                ret_value = op_ret;

                break;
            } /* end block */
#endif

        case H5VL_LINK_DELETE:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported specific operation")
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
    dkey_buf = (char *)DV_free(dkey_buf);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_link_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_follow
 *
 * Purpose:     Follows the link in grp identified with name, and returns
 *              in oid the oid of the target object.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_link_follow(H5_daos_group_t *grp, const char *name,
    size_t name_len, hid_t dxpl_id, void **req, daos_obj_id_t *oid)
{
    H5_daos_link_val_t link_val;
    hbool_t link_val_alloc = FALSE;
    H5_daos_group_t *target_grp = NULL;
    herr_t ret_value = SUCCEED;

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
                else
                    /* Follow the last element in the path */
                    if(H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, oid) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't follow link")

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

