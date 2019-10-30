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
#define H5_DAOS_HARD_LINK_VAL_SIZE (8 + 8 + 1)
#define H5_DAOS_RECURSE_LINK_PATH_BUF_INIT 1024
#define H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE 9

/*
 * Given an H5_daos_link_val_t, uses this to fill out the
 * link type and link object address (hard link) or link
 * value size (soft/external link) fields of an H5L_info_t
 * for a link.
 */
#define H5_DAOS_LINK_VAL_TO_INFO(link_val, link_info)                    \
do {                                                                     \
    link_info.type = link_val.type;                                      \
    if(link_val.type == H5L_TYPE_HARD)                                   \
        link_info.u.address = H5_daos_oid_to_addr(link_val.target.hard); \
    else {                                                               \
        assert(link_val.type == H5L_TYPE_SOFT);                          \
        link_info.u.val_size = strlen(link_val.target.soft) + 1;         \
    }                                                                    \
} while(0)

/* Prototypes */
static herr_t H5_daos_link_read(H5_daos_group_t *grp, const char *name,
    size_t name_len, H5_daos_link_val_t *val);
static herr_t H5_daos_link_get_info(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    H5L_info_t *link_info_out, H5_daos_link_val_t *link_val_out, hid_t dxpl_id, void **req);
static herr_t H5_daos_link_get_val(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    H5_daos_link_val_t *link_val_out, hid_t dxpl_id, void **req);
static herr_t H5_daos_link_iterate_by_name_order(H5_daos_group_t *target_grp, H5_daos_iter_data_t *iter_data);
static herr_t H5_daos_link_iterate_by_crt_order(H5_daos_group_t *target_grp, H5_daos_iter_data_t *iter_data);
static herr_t H5_daos_link_delete(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    hid_t dxpl_id, void **req);
static ssize_t H5_daos_link_get_name_by_crt_order(H5_daos_group_t *target_grp, H5_iter_order_t iter_order,
    uint64_t index, char *link_name_out, size_t link_name_out_size);
static ssize_t H5_daos_link_get_name_by_name_order(H5_daos_group_t *target_grp, H5_iter_order_t iter_order,
    uint64_t index, char *link_name_out, size_t link_name_out_size);
static herr_t H5_daos_link_get_name_by_name_order_cb(hid_t group, const char *name, const H5L_info_t *info, void *op_data);
static herr_t H5_daos_link_remove_from_crt_idx(H5_daos_group_t *target_grp, const H5VL_loc_params_t *loc_params);
static herr_t H5_daos_link_remove_from_crt_idx_name_cb(hid_t group, const char *name, const H5L_info_t *info, void *op_data);
static herr_t H5_daos_link_shift_crt_idx_keys_down(H5_daos_group_t *target_grp, uint64_t idx_begin, uint64_t idx_end);
static uint64_t H5_daos_hash_obj_id(dv_hash_table_key_t obj_id_lo);
static int H5_daos_cmp_obj_id(dv_hash_table_key_t obj_id_lo1, dv_hash_table_key_t obj_id_lo2);
static void H5_daos_free_visited_link_hash_table_key(dv_hash_table_key_t value);

/*
 * A link iteration callback function data structure. It is
 * passed during link iteration when retrieving a link's name
 * by a given creation order index value.
 */
typedef struct H5_daos_link_find_name_by_idx_ud_t {
    char *link_name_out;
    size_t link_name_out_size;
    uint64_t target_link_idx;
    uint64_t cur_link_idx;
} H5_daos_link_find_name_by_idx_ud_t;

/*
 * A link iteration callback function data structure. It is
 * passed during link iteration when retrieving a link's
 * creation order index value by the given link's name.
 */
typedef struct H5_daos_link_crt_idx_iter_ud_t {
    const char *target_link_name;
    uint64_t *link_idx_out;
} H5_daos_link_crt_idx_iter_ud_t;


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
    daos_iov_set(&iod.iod_name, (void *)H5_daos_link_key_g, H5_daos_link_key_size_g);
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
    uint8_t *max_corder_old_buf = NULL; /* Holds the previous max creation order value, which is the creation order for this link */
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(grp);
    assert(name);
    assert(val);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_LINKS_SIZE == 8);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

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
            /* We need an extra byte for the link type */
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
    daos_iov_set(&update_cb_ud->iod[0].iod_name, (void *)H5_daos_link_key_g, H5_daos_link_key_size_g);
    daos_csum_set(&update_cb_ud->iod[0].iod_kcsum, NULL, 0);
    update_cb_ud->iod[0].iod_nr = 1u;
    update_cb_ud->iod[0].iod_type = DAOS_IOD_SINGLE;
    update_cb_ud->free_akeys = FALSE;

    /* Set up general sgl */
    update_cb_ud->sgl[0].sg_iovs = &update_cb_ud->sg_iov[0];

    /* Set task name */
    update_cb_ud->task_name = "link write";

    /* Check for creation order tracking/indexing */
    if(grp->gcpl_cache.track_corder) {
        daos_key_t dkey;
        daos_iod_t iod[4];
        daos_sg_list_t sgl[4];
        daos_iov_t sg_iov[4];
        uint8_t nlinks_old_buf[H5_DAOS_ENCODED_NUM_LINKS_SIZE];
        uint8_t nlinks_new_buf[H5_DAOS_ENCODED_NUM_LINKS_SIZE];
        uint8_t max_corder_new_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
        uint8_t corder_target_buf[H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE];
        ssize_t nlinks;
        uint64_t uint_nlinks;
        uint64_t max_corder;

        /* Allocate buffer for group's current maximum creation order value */
        if(NULL == (max_corder_old_buf = (uint8_t *)DV_malloc(H5_DAOS_ENCODED_CRT_ORDER_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for max. creation order value")

        /* Read group's current maximum creation order value */
        if(H5_daos_group_get_max_crt_order(grp, &max_corder) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group's maximum creation order value")

        p = max_corder_old_buf;
        UINT64ENCODE(p, max_corder);

        /* Add new link to max. creation order value */
        max_corder++;

        /* Read num links */
        if((nlinks = H5_daos_group_get_num_links(grp)) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get number of links in group")
        uint_nlinks = (uint64_t)nlinks;

        p = nlinks_old_buf;
        UINT64ENCODE(p, uint_nlinks);

        assert(H5_daos_nlinks_key_size_g != 8);

        /* Add new link to count */
        uint_nlinks++;

        /* Write new info to creation order index */

        /* Set up dkey */
        daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

        /* Encode buffers */
        p = max_corder_new_buf;
        UINT64ENCODE(p, max_corder);
        p = nlinks_new_buf;
        UINT64ENCODE(p, uint_nlinks);
        memcpy(corder_target_buf, nlinks_old_buf, 8);
        corder_target_buf[8] = 0;

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, (void *)H5_daos_max_link_corder_key_g, H5_daos_max_link_corder_key_size_g);
        daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, (void *)H5_daos_nlinks_key_g, H5_daos_nlinks_key_size_g);
        daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[2].iod_name, (void *)nlinks_old_buf, H5_DAOS_ENCODED_NUM_LINKS_SIZE);
        daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
        iod[2].iod_nr = 1u;
        iod[2].iod_size = (uint64_t)name_len;
        iod[2].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[3].iod_name, (void *)corder_target_buf, H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE);
        daos_csum_set(&iod[3].iod_kcsum, NULL, 0);
        iod[3].iod_nr = 1u;
        iod[3].iod_size = update_cb_ud->iod[0].iod_size;
        iod[3].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov[0], max_corder_new_buf, (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
        sgl[0].sg_nr = 1;
        sgl[0].sg_nr_out = 0;
        sgl[0].sg_iovs = &sg_iov[0];

        daos_iov_set(&sg_iov[1], nlinks_new_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_LINKS_SIZE);
        sgl[1].sg_nr = 1;
        sgl[1].sg_nr_out = 0;
        sgl[1].sg_iovs = &sg_iov[1];

        daos_iov_set(&sg_iov[2], (void *)name_buf, (daos_size_t)name_len);
        sgl[2].sg_nr = 1;
        sgl[2].sg_nr_out = 0;
        sgl[2].sg_iovs = &sg_iov[2];

        daos_iov_set(&sg_iov[3], iov_buf, update_cb_ud->iod[0].iod_size);
        sgl[3].sg_nr = 1;
        sgl[3].sg_nr_out = 0;
        sgl[3].sg_iovs = &sg_iov[3];

        /* Issue write */
        if(0 != (ret = daos_obj_update(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 4, iod, sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't write link creation order information: %s", H5_daos_err_to_string(ret))

        /* Add link name->creation order mapping key-value pair to main write */
        /* Increment number of records */
        update_cb_ud->nr++;

        /* Set up iod */
        daos_iov_set(&update_cb_ud->iod[1].iod_name, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);
        daos_csum_set(&update_cb_ud->iod[1].iod_kcsum, NULL, 0);
        update_cb_ud->iod[1].iod_nr = 1u;
        update_cb_ud->iod[1].iod_size = (uint64_t)8;
        update_cb_ud->iod[1].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->sg_iov[1], (void *)max_corder_old_buf, 8);
        update_cb_ud->sgl[1].sg_nr = 1;
        update_cb_ud->sgl[1].sg_nr_out = 0;
        update_cb_ud->sgl[1].sg_iovs = &update_cb_ud->sg_iov[1];
    } /* end if */

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
        max_corder_old_buf = DV_free(max_corder_old_buf);
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

            link_val.type = H5L_TYPE_HARD;

            if(H5VL_OBJECT_BY_NAME == target_obj_loc_params->type) {
                /* Attempt to open the hard link's target object */
                if(NULL == (target_obj = H5_daos_object_open((H5_daos_item_t *) target_obj_loc,
                        target_obj_loc_params, NULL, dxpl_id, req)))
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "couldn't open hard link's target object")
                link_val.target.hard = target_obj->oid;
            }
            else {
                /* H5Olink */
                if(H5VL_OBJECT_BY_SELF == target_obj_loc_params->type)
                    D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid loc_params type")

                link_val.target.hard = target_obj_loc->oid;
            }

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
    H5_daos_link_val_t  link_val;
    H5_daos_group_t    *target_grp = NULL;
    H5_daos_item_t     *item = (H5_daos_item_t *)_item;
    hbool_t             link_val_alloc = FALSE;
    herr_t              ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    /* Determine group containing link in question */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_SELF:
            /* Use item as link's parent group, or the root group if item is a file */
            if(item->type == H5I_FILE)
                target_grp = ((H5_daos_file_t *)item)->root_grp;
            else if(item->type == H5I_GROUP)
                target_grp = (H5_daos_group_t *)item;
            else
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "item not a file or group")

            target_grp->obj.item.rc++;
            break;

        case H5VL_OBJECT_BY_NAME:
            /* Open target group */
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "can't open target group for link")
            break;

        case H5VL_OBJECT_BY_IDX:
        {
            H5VL_loc_params_t by_name_params;

            /* Setup loc_params for opening target group */
            by_name_params.type = H5VL_OBJECT_BY_NAME;
            by_name_params.obj_type = H5I_GROUP;
            by_name_params.loc_data.loc_by_name.name = loc_params->loc_data.loc_by_idx.name;
            by_name_params.loc_data.loc_by_name.lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;

            /* Open target group */
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_object_open(item, &by_name_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "can't open target group for link")
            break;
        }

        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid loc_params type")
    } /* end switch */

    switch (get_type) {
        case H5VL_LINK_GET_INFO:
        {
            H5L_info_t *link_info = va_arg(arguments, H5L_info_t *);

            if(H5_daos_link_get_info(item, loc_params, link_info, NULL, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't retrieve link's info")

            break;
        } /* H5VL_LINK_GET_INFO */

        case H5VL_LINK_GET_NAME:
        {
            char *name_out = va_arg(arguments, char *);
            size_t name_out_size = va_arg(arguments, size_t);
            ssize_t *ret_size = va_arg(arguments, ssize_t *);

            if((*ret_size = H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n, name_out, name_out_size)) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't retrieve link's name")

            break;
        } /* H5VL_LINK_GET_NAME */

        case H5VL_LINK_GET_VAL:
        {
            void *out_buf = va_arg(arguments, void *);
            size_t out_buf_size = va_arg(arguments, size_t);

            if(H5_daos_link_get_val(item, loc_params, &link_val, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link's value")

            if(H5L_TYPE_SOFT == link_val.type)
                link_val_alloc = TRUE;

            /*
             * H5Lget_val(_by_idx) specifically says that if the size of
             * the buffer given is smaller than the size of the link's
             * value, then the link's value will be truncated to 'size'
             * bytes and will not be null-terminated.
             */
            if(out_buf)
                memcpy(out_buf, link_val.target.soft, out_buf_size);

            break;
        } /* H5VL_LINK_GET_VAL */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported link get operation")
    } /* end switch */

done:
    if(link_val_alloc) {
        assert(H5L_TYPE_SOFT == link_val.type);
        DV_free(link_val.target.soft);
    } /* end if */

    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

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

        /* H5Literate(_by_name)/visit(_by_name) */
        case H5VL_LINK_ITER:
            {
                H5_daos_iter_data_t iter_data;
                int is_recursive = va_arg(arguments, int);
                H5_index_t idx_type = (H5_index_t) va_arg(arguments, int);
                H5_iter_order_t iter_order = (H5_iter_order_t) va_arg(arguments, int);
                hsize_t *idx_p = va_arg(arguments, hsize_t *);
                H5L_iterate_t iter_op = va_arg(arguments, H5L_iterate_t);
                void *op_data = va_arg(arguments, void *);

                /* Determine group containing link in question */
                switch (loc_params->type) {
                    /* H5Literate/H5Lvisit */
                    case H5VL_OBJECT_BY_SELF:
                    {
                        /* Use item as link's parent group, or the root group if item is a file */
                        if(item->type == H5I_FILE)
                            target_grp = ((H5_daos_file_t *)item)->root_grp;
                        else if(item->type == H5I_GROUP)
                            target_grp = (H5_daos_group_t *)item;
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
                        if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_open(item, &sub_loc_params,
                                loc_params->loc_data.loc_by_name.name, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req)))
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
            if(H5_daos_link_delete(item, loc_params, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTREMOVE, FAIL, "failed to delete link")
            break;

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

                    /* Check if the soft link resolved to something */
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
 *              If the link_val_out parameter is non-NULL, the link's value
 *              is returned through it. If the link in question is a soft
 *              link, this means the caller is responsible for freeing the
 *              allocated link value.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_get_info(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    H5L_info_t *link_info_out, H5_daos_link_val_t *link_val_out, hid_t dxpl_id,
    void **req)
{
    H5_daos_link_val_t local_link_val = { 0 };
    H5_daos_group_t *target_grp = NULL;
    H5L_info_t local_link_info;
    const char *target_name = NULL;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    uint64_t link_crt_order;
    herr_t ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(link_info_out);

    /* Determine the target group */
    switch (loc_params->type) {
        /* H5Lget_info */
        case H5VL_OBJECT_BY_NAME:
        {
            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name,
                    dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "failed to traverse path")

            break;
        } /* H5VL_OBJECT_BY_NAME */

        case H5VL_OBJECT_BY_IDX:
        {
            H5VL_loc_params_t sub_loc_params;
            ssize_t link_name_size;

            /* Open the group containing the target link */
            sub_loc_params.type = H5VL_OBJECT_BY_SELF;
            sub_loc_params.obj_type = item->type;
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_open(item, &sub_loc_params,
                    loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.lapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "can't open group containing target link")

            /* Retrieve the link's name length + the link's name if the buffer is large enough */
            if((link_name_size = H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                    link_name_buf_static, H5_DAOS_LINK_NAME_BUF_SIZE)) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

            /* Check that buffer was large enough to fit link name */
            if(link_name_size > H5_DAOS_LINK_NAME_BUF_SIZE - 1) {
                if(NULL == (link_name_buf_dyn = DV_malloc(link_name_size + 1)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link name buffer")

                /* Re-issue the call with a larger buffer */
                if(H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                        loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                        link_name_buf_dyn, link_name_size + 1) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

                target_name = link_name_buf_dyn;
            } /* end if */
            else
                target_name = link_name_buf_static;

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_SELF:
        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid loc_params type")
    } /* end switch */

    if(H5_daos_link_read(target_grp, target_name, strlen(target_name), &local_link_val) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "failed to read link")

    /*
     * Fill in link type and link object address (hard link) or
     * link value size (soft link) fields, then free the link
     * value if this is a soft link and the link's value is not
     * being returned through link_val_out.
     */
    H5_DAOS_LINK_VAL_TO_INFO(local_link_val, local_link_info);
    if(!link_val_out && (H5L_TYPE_SOFT == local_link_val.type))
        local_link_val.target.soft = (char *)DV_free(local_link_val.target.soft);

    if(target_grp->gcpl_cache.track_corder) {
        if(H5_daos_link_get_crt_order_by_name(target_grp, target_name, &link_crt_order) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link's creation order value")
        local_link_info.corder = (int64_t)link_crt_order; /* DSINC - no check for overflow */
        local_link_info.corder_valid = TRUE;
    } /* end if */
    else {
        local_link_info.corder = -1;
        local_link_info.corder_valid = FALSE;
    } /* end else */

    /* Only ASCII character set is supported currently */
    local_link_info.cset = H5T_CSET_ASCII;

    memcpy(link_info_out, &local_link_info, sizeof(*link_info_out));

    if(link_val_out)
        memcpy(link_val_out, &local_link_val, sizeof(*link_val_out));

done:
    /* Only free the link value if it isn't being returned through link_val_out */
    if(!link_val_out && (H5L_TYPE_SOFT == local_link_val.type))
        DV_free(local_link_val.target.soft);

    if(link_name_buf_dyn)
        link_name_buf_dyn = DV_free(link_name_buf_dyn);

    if(target_grp) {
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        target_grp = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_val
 *
 * Purpose:     Helper routine to retrieve a symbolic or user-defined
 *              link's value. The caller must remember to free the link's
 *              value after using it.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_get_val(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    H5_daos_link_val_t *link_val_out, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *target_grp = NULL;
    const char *target_link_name;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    herr_t ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(link_val_out);
    assert(H5VL_OBJECT_BY_NAME == loc_params->type || H5VL_OBJECT_BY_IDX == loc_params->type);

    /* Determine the target group */
    switch (loc_params->type) {
        /* H5Lget_val */
        case H5VL_OBJECT_BY_NAME:
        {
            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name,
                    dxpl_id, req, &target_link_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "failed to traverse path")

            break;
        } /* H5VL_OBJECT_BY_NAME */

        case H5VL_OBJECT_BY_IDX:
        {
            H5VL_loc_params_t sub_loc_params;
            ssize_t link_name_size;

            /* Open the group containing the target link */
            sub_loc_params.type = H5VL_OBJECT_BY_SELF;
            sub_loc_params.obj_type = item->type;
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_open(item, &sub_loc_params,
                    loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.lapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "can't open group containing target link")

            /* Retrieve the link's name length + the link's name if the buffer is large enough */
            if((link_name_size = H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                    link_name_buf_static, H5_DAOS_LINK_NAME_BUF_SIZE)) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

            /* Check that buffer was large enough to fit link name */
            if(link_name_size > H5_DAOS_LINK_NAME_BUF_SIZE - 1) {
                if(NULL == (link_name_buf_dyn = DV_malloc(link_name_size + 1)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link name buffer")

                /* Re-issue the call with a larger buffer */
                if(H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                        loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                        link_name_buf_dyn, link_name_size + 1) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

                target_link_name = link_name_buf_dyn;
            } /* end if */
            else
                target_link_name = link_name_buf_static;

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_SELF:
        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid loc_params type")
    } /* end switch */

    /* Read the link's value */
    if(H5_daos_link_read(target_grp, target_link_name, strlen(target_link_name), link_val_out) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "failed to read link")

    if(H5L_TYPE_HARD == link_val_out->type)
        D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "link value cannot be retrieved from a hard link")

done:
    if(link_name_buf_dyn)
        link_name_buf_dyn = DV_free(link_name_buf_dyn);

    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_link_get_val() */


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

    assert(item);
    assert(link_path);

    /* Traverse the path */
    if(NULL == (target_grp = H5_daos_group_traverse(item, link_path, dxpl_id, req, &target_name, NULL, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "can't traverse path")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)target_name, strlen(target_name));

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_link_key_g, H5_daos_link_key_size_g);
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
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(iter_data);
    assert(H5_DAOS_ITER_TYPE_LINK == iter_data->iter_type);

    /* Iteration restart not supported */
    if(iter_data->idx_p && (*iter_data->idx_p != 0))
        D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)")

    switch (iter_data->index_type) {
        case H5_INDEX_NAME:
            if((ret_value = H5_daos_link_iterate_by_name_order(target_grp, iter_data)) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration by name order failed")
            break;

        case H5_INDEX_CRT_ORDER:
            if((ret_value = H5_daos_link_iterate_by_crt_order(target_grp, iter_data)) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration by creation order failed")
            break;

        case H5_INDEX_UNKNOWN:
        case H5_INDEX_N:
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "invalid or unsupported index type")
    } /* end switch */

done:
    D_FUNC_LEAVE
} /* end H5_daos_link_iterate() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_iterate_by_name_order
 *
 * Purpose:     Iterates over the links in the specified group according to
 *              their alphabetical order. The supplied iter_data struct
 *              contains the iteration parameters.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_iterate_by_name_order(H5_daos_group_t *target_grp, H5_daos_iter_data_t *iter_data)
{
    H5_daos_link_val_t link_val = { 0 };
    H5VL_loc_params_t sub_loc_params;
    H5_daos_group_t *subgroup = NULL;
    daos_anchor_t anchor;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    H5L_info_t linfo;
    char *dkey_buf = NULL;
    size_t dkey_buf_len = 0;
    herr_t op_ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(iter_data);
    assert(H5_INDEX_NAME == iter_data->index_type);
    assert(H5_ITER_NATIVE == iter_data->iter_order || H5_ITER_INC == iter_data->iter_order
            || H5_ITER_DEC == iter_data->iter_order);

    /* Native iteration order is currently associated with increasing order; decreasing order iteration is not currently supported */
    if(iter_data->iter_order == H5_ITER_DEC)
        D_GOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "decreasing iteration order not supported (order must be H5_ITER_NATIVE or H5_ITER_INC)")

    /*
     * If iteration is recursive, setup a hash table to keep track of visited
     * group links so that cyclic links don't result in infinite looping.
     *
     * Also setup the recursive link path buffer, which keeps track of the full
     * path to the current link and is passed to the operator callback function.
     */
    if(iter_data->is_recursive && (iter_data->u.link_iter_data.recurse_depth == 0)) {
        if(NULL == (iter_data->u.link_iter_data.visited_link_table = dv_hash_table_new(H5_daos_hash_obj_id, H5_daos_cmp_obj_id)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate visited links table")

        /*
         * Since the table key values are DV_malloc()ed, register the callback function to
         * call DV_free() on the keys when necessary.
         */
        dv_hash_table_register_free_functions(iter_data->u.link_iter_data.visited_link_table, H5_daos_free_visited_link_hash_table_key, NULL);

        /* Allocate the link path buffer for recursive iteration */
        if(NULL == (iter_data->u.link_iter_data.recursive_link_path = DV_malloc(H5_DAOS_RECURSE_LINK_PATH_BUF_INIT)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate link path buffer")
        iter_data->u.link_iter_data.recursive_link_path_nalloc = H5_DAOS_RECURSE_LINK_PATH_BUF_INIT;
        *iter_data->u.link_iter_data.recursive_link_path = '\0';
    } /* end if */

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
        daos_key_desc_t kds[H5_DAOS_ITER_LEN];
        uint32_t nr;
        uint32_t i;
        char *p;
        int ret;

        /* Loop to retrieve keys (exit as soon as we get at least 1 key) */
        H5_DAOS_RETRIEVE_KEYS_LOOP(dkey_buf, dkey_buf_len, sg_iov, nr, H5E_SYM, daos_obj_list_dkey,
                target_grp->obj.obj_oh, DAOS_TX_NONE, &nr, kds, &sgl, &anchor, NULL /*event*/);

        /* Loop over returned dkeys */
        p = dkey_buf;
        op_ret = 0;
        for(i = 0; (i < nr) && (op_ret == 0); i++) {
            /* Check if this key represents a link */
            if(p[0] != '/') {
                char *link_path = p;
                char *cur_link_path_end = NULL;
                char tmp_char;

                /* Add null terminator temporarily */
                tmp_char = p[kds[i].kd_key_len];
                p[kds[i].kd_key_len] = '\0';

                /* Retrieve link's info and value */
                sub_loc_params.obj_type = target_grp->obj.item.type;
                sub_loc_params.type = H5VL_OBJECT_BY_NAME;
                sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
                sub_loc_params.loc_data.loc_by_name.name = p;
                if(H5_daos_link_get_info((H5_daos_item_t *)target_grp, &sub_loc_params,
                        &linfo, &link_val, iter_data->dxpl_id, iter_data->req) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link info")

                /* Free soft link value if necessary */
                if(H5L_TYPE_SOFT == link_val.type)
                    link_val.target.soft = (char *)DV_free(link_val.target.soft);

                /* If doing recursive iteration, add the current link name to the end of the recursive link path */
                if(iter_data->is_recursive) {
                    size_t cur_link_path_len = strlen(iter_data->u.link_iter_data.recursive_link_path);

                    /*
                     * Save a pointer to the current end of the recursive link path string. This will
                     * be used later to strip the added link name back off of the path string once
                     * processing is done.
                     */
                    cur_link_path_end = &iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len];

                    /*
                     * Reallocate the link path buffer if the current link path + the current
                     * link name and null terminator is larger than what's currently allocated.
                     */
                    while(cur_link_path_len + strlen(p) + 1 > iter_data->u.link_iter_data.recursive_link_path_nalloc) {
                        char *tmp_realloc;

                        iter_data->u.link_iter_data.recursive_link_path_nalloc *= 2;
                        if(NULL == (tmp_realloc = DV_realloc(iter_data->u.link_iter_data.recursive_link_path,
                                iter_data->u.link_iter_data.recursive_link_path_nalloc)))
                            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate link path buffer")

                        iter_data->u.link_iter_data.recursive_link_path = tmp_realloc;
                    } /* end if */

                    /* Append the current link name to the current link path */
                    strncat(&iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len], p,
                            iter_data->u.link_iter_data.recursive_link_path_nalloc - cur_link_path_len - 1);

                    link_path = iter_data->u.link_iter_data.recursive_link_path;
                } /* end if */

                /* Call the link iteration callback operator function on the current link */
                if((op_ret = iter_data->u.link_iter_data.link_iter_op(iter_data->iter_root_obj, link_path, &linfo, iter_data->op_data)) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_BADITER, op_ret, "operator function returned failure")

                /* Check for short-circuit success */
                if(op_ret)
                    D_GOTO_DONE(op_ret)

                if(iter_data->is_recursive) {
                    assert(iter_data->u.link_iter_data.visited_link_table);

                    /* If the current link points to a group that hasn't been visited yet, iterate over its links as well. */
                    if((H5L_TYPE_HARD == link_val.type) && (H5I_GROUP == H5_daos_oid_to_type(link_val.target.hard))
                            && (DV_HASH_TABLE_NULL == dv_hash_table_lookup(iter_data->u.link_iter_data.visited_link_table, &link_val.target.hard.lo))) {
                        uint64_t *oid_lo_copy;
                        size_t cur_link_path_len;
                        herr_t recurse_ret;

                        if(NULL == (oid_lo_copy = DV_malloc(sizeof(*oid_lo_copy))))
                            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate entry for visited link table")
                        *oid_lo_copy = link_val.target.hard.lo;

                        /*
                         * The value chosen for the hash table entry doesn't really matter, as long
                         * as it doesn't match DV_HASH_TABLE_NULL. Later, it only needs to be known
                         * if we inserted the key into the table or not, so the value will not be checked.
                         */
                        ret = dv_hash_table_insert(iter_data->u.link_iter_data.visited_link_table, oid_lo_copy, oid_lo_copy);
                        if(!ret) {
                            DV_free(oid_lo_copy);
                            D_GOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "failed to insert link into visited link table")
                        } /* end if */

                        sub_loc_params.type = H5VL_OBJECT_BY_SELF;
                        sub_loc_params.obj_type = H5I_GROUP;
                        if(NULL == (subgroup = H5_daos_group_open(target_grp, &sub_loc_params,
                                p, H5P_GROUP_ACCESS_DEFAULT, iter_data->dxpl_id, iter_data->req)))
                            D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "failed to open group")

                        /* Add a trailing slash to the link path buffer to denote that this link points to a group */
                        cur_link_path_len = strlen(iter_data->u.link_iter_data.recursive_link_path);
                        while(cur_link_path_len + 2 > iter_data->u.link_iter_data.recursive_link_path_nalloc) {
                            char *tmp_realloc;

                            iter_data->u.link_iter_data.recursive_link_path_nalloc *= 2;
                            if(NULL == (tmp_realloc = DV_realloc(iter_data->u.link_iter_data.recursive_link_path,
                                    iter_data->u.link_iter_data.recursive_link_path_nalloc)))
                                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to reallocate link path buffer")

                            iter_data->u.link_iter_data.recursive_link_path = tmp_realloc;
                        } /* end while */

                        iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len] = '/';
                        iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len + 1] = '\0';

                        /* Recurse on this group */
                        iter_data->u.link_iter_data.recurse_depth++;
                        recurse_ret = H5_daos_link_iterate_by_name_order(subgroup, iter_data);
                        iter_data->u.link_iter_data.recurse_depth--;

                        if(recurse_ret < 0)
                            D_GOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "recursive link iteration failed")
                        else if(recurse_ret)
                            D_GOTO_DONE(recurse_ret) /* Short-circuit success */

                        if(H5_daos_group_close(subgroup, iter_data->dxpl_id, iter_data->req) < 0)
                            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
                        subgroup = NULL;
                    } /* end if */

                    /*
                     * Strip the current link name (and, for groups, any trailing slash)
                     * back off of the recursive link path.
                     */
                    *cur_link_path_end = '\0';
                } /* end if */

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
    if(subgroup) {
        if(H5_daos_group_close(subgroup, iter_data->dxpl_id, iter_data->req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        subgroup = NULL;
    }

    if(H5L_TYPE_SOFT == link_val.type)
        link_val.target.soft = (char *)DV_free(link_val.target.soft);

    dkey_buf = (char *)DV_free(dkey_buf);

    /*
     * Free resources allocated for recursive iteration once we reach the top
     * level of recursion again.
     */
    if(iter_data->is_recursive && (iter_data->u.link_iter_data.recurse_depth == 0)) {
        iter_data->u.link_iter_data.recursive_link_path = DV_free(iter_data->u.link_iter_data.recursive_link_path);

        if(iter_data->u.link_iter_data.visited_link_table) {
            dv_hash_table_free(iter_data->u.link_iter_data.visited_link_table);
            iter_data->u.link_iter_data.visited_link_table = NULL;
        } /* end if */
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_iterate_by_name_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_iterate_by_crt_order
 *
 * Purpose:     Iterates over the links in the specified group according to
 *              their link creation order values. The supplied iter_data
 *              struct contains the iteration parameters.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_iterate_by_crt_order(H5_daos_group_t *target_grp, H5_daos_iter_data_t *iter_data)
{
    H5_daos_link_val_t link_val = { 0 };
    H5_daos_group_t *subgroup = NULL;
    H5L_info_t linfo;
    uint64_t cur_idx;
    ssize_t grp_nlinks;
    herr_t op_ret;
    size_t link_name_buf_size = H5_DAOS_LINK_NAME_BUF_SIZE;
    char *link_name = NULL;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(iter_data);
    assert(H5_INDEX_CRT_ORDER == iter_data->index_type);
    assert(H5_ITER_NATIVE == iter_data->iter_order || H5_ITER_INC == iter_data->iter_order
            || H5_ITER_DEC == iter_data->iter_order);

    /* Check that creation order is tracked for target group */
    if(!target_grp->gcpl_cache.track_corder)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "creation order is not tracked for group")

    /* Retrieve the number of links in the group */
    if((grp_nlinks = H5_daos_group_get_num_links(target_grp)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get number of links in group")

    /* Check if there are no links to process */
    if(grp_nlinks == 0)
        D_GOTO_DONE(SUCCEED);

    /*
     * If iteration is recursive, setup a hash table to keep track of visited
     * group links so that cyclic links don't result in infinite looping.
     *
     * Also setup the recursive link path buffer, which keeps track of the full
     * path to the current link and is passed to the operator callback function.
     */
    if(iter_data->is_recursive && (iter_data->u.link_iter_data.recurse_depth == 0)) {
        if(NULL == (iter_data->u.link_iter_data.visited_link_table = dv_hash_table_new(H5_daos_hash_obj_id, H5_daos_cmp_obj_id)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate visited links table")

        /*
         * Since the table key values are DV_malloc()ed, register the callback function to
         * call DV_free() on the keys when necessary.
         */
        dv_hash_table_register_free_functions(iter_data->u.link_iter_data.visited_link_table, H5_daos_free_visited_link_hash_table_key, NULL);

        /* Allocate the link path buffer for recursive iteration */
        if(NULL == (iter_data->u.link_iter_data.recursive_link_path = DV_malloc(H5_DAOS_RECURSE_LINK_PATH_BUF_INIT)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate link path buffer")
        iter_data->u.link_iter_data.recursive_link_path_nalloc = H5_DAOS_RECURSE_LINK_PATH_BUF_INIT;
        *iter_data->u.link_iter_data.recursive_link_path = '\0';
    } /* end if */

    /* Initialize const link info */
    linfo.corder_valid = TRUE;
    linfo.corder = 0;
    linfo.cset = H5T_CSET_ASCII;

    link_name = link_name_buf_static;
    for(cur_idx = 0; cur_idx < (uint64_t)grp_nlinks; cur_idx++) {
        ssize_t link_name_size;
        htri_t link_exists;

        /* Retrieve the link's name length + the link's name if the buffer is large enough */
        if((link_name_size = H5_daos_link_get_name_by_idx(target_grp, iter_data->index_type,
                iter_data->iter_order, cur_idx, link_name, link_name_buf_size)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

        if((size_t)link_name_size > link_name_buf_size - 1) {
            char *tmp_realloc;

            /*
             * Double the buffer size or re-allocate to fit the current
             * link's name, depending on which allocation is larger.
             */
            link_name_buf_size = ((size_t)link_name_size > (2 * link_name_buf_size)) ?
                    (size_t)link_name_size + 1 : (2 * link_name_buf_size);

            if(NULL == (tmp_realloc = DV_realloc(link_name_buf_dyn, link_name_buf_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate link name buffer")
            link_name = link_name_buf_dyn = tmp_realloc;

            /* Re-issue the call to fetch the link's name with a larger buffer */
            if(H5_daos_link_get_name_by_idx(target_grp, iter_data->index_type,
                    iter_data->iter_order, cur_idx, link_name, link_name_buf_size) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")
        } /* end if */

        /* Check if a link exists for this index value */
        if((link_exists = H5_daos_link_exists((H5_daos_item_t *) target_grp, link_name, iter_data->dxpl_id, iter_data->req)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if link exists")

        /* Process the link */
        if(link_exists) {
            uint64_t link_crt_order;
            char *link_path = link_name;
            char *cur_link_path_end = NULL;

            /* Read the link */
            if(H5_daos_link_read(target_grp, link_name, strlen(link_name), &link_val) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "can't read link")

            /* Update linfo, then free soft link value if necessary */
            H5_DAOS_LINK_VAL_TO_INFO(link_val, linfo);
            if(H5_daos_link_get_crt_order_by_name(target_grp, link_name, &link_crt_order) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link's creation order value")
            linfo.corder = (int64_t)link_crt_order; /* DSINC - no check for overflow */
            if(H5L_TYPE_SOFT == link_val.type)
                link_val.target.soft = (char *)DV_free(link_val.target.soft);

            /* If doing recursive iteration, add the current link name to the end of the recursive link path */
            if(iter_data->is_recursive) {
                size_t cur_link_path_len = strlen(iter_data->u.link_iter_data.recursive_link_path);

                /*
                 * Save a pointer to the current end of the recursive link path string. This will
                 * be used later to strip the added link name back off of the path string once
                 * processing is done.
                 */
                cur_link_path_end = &iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len];

                /*
                 * Reallocate the link path buffer if the current link path + the current
                 * link name and null terminator is larger than what's currently allocated.
                 */
                while(cur_link_path_len + strlen(link_name) + 1 > iter_data->u.link_iter_data.recursive_link_path_nalloc) {
                    char *tmp_realloc;

                    iter_data->u.link_iter_data.recursive_link_path_nalloc *= 2;
                    if(NULL == (tmp_realloc = DV_realloc(iter_data->u.link_iter_data.recursive_link_path,
                            iter_data->u.link_iter_data.recursive_link_path_nalloc)))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate link path buffer")

                    iter_data->u.link_iter_data.recursive_link_path = tmp_realloc;
                } /* end if */

                /* Append the current link name to the current link path */
                strncat(&iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len], link_name,
                        iter_data->u.link_iter_data.recursive_link_path_nalloc - cur_link_path_len - 1);

                link_path = iter_data->u.link_iter_data.recursive_link_path;
            } /* end if */

            /* Call the link iteration callback operator function on the current link */
            if((op_ret = iter_data->u.link_iter_data.link_iter_op(iter_data->iter_root_obj, link_path, &linfo, iter_data->op_data)) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_BADITER, op_ret, "operator function returned failure")

            /* Check for short-circuit success */
            if(op_ret)
                D_GOTO_DONE(op_ret)

            if(iter_data->is_recursive) {
                assert(iter_data->u.link_iter_data.visited_link_table);

                /* If the current link points to a group that hasn't been visited yet, iterate over its links as well. */
                if((H5L_TYPE_HARD == link_val.type) && (H5I_GROUP == H5_daos_oid_to_type(link_val.target.hard))
                        && (DV_HASH_TABLE_NULL == dv_hash_table_lookup(iter_data->u.link_iter_data.visited_link_table, &link_val.target.hard.lo))) {
                    H5VL_loc_params_t sub_loc_params;
                    uint64_t *oid_lo_copy;
                    size_t cur_link_path_len;
                    herr_t recurse_ret;
                    int ret;

                    if(NULL == (oid_lo_copy = DV_malloc(sizeof(*oid_lo_copy))))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate entry for visited link table")
                    *oid_lo_copy = link_val.target.hard.lo;

                    /*
                     * The value chosen for the hash table entry doesn't really matter, as long
                     * as it doesn't match DV_HASH_TABLE_NULL. Later, it only needs to be known
                     * if we inserted the key into the table or not, so the value will not be checked.
                     */
                    ret = dv_hash_table_insert(iter_data->u.link_iter_data.visited_link_table, oid_lo_copy, oid_lo_copy);
                    if(!ret) {
                        DV_free(oid_lo_copy);
                        D_GOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "failed to insert link into visited link table")
                    } /* end if */

                    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
                    sub_loc_params.obj_type = H5I_GROUP;
                    if(NULL == (subgroup = H5_daos_group_open(target_grp, &sub_loc_params,
                            link_name, H5P_GROUP_ACCESS_DEFAULT, iter_data->dxpl_id, iter_data->req)))
                        D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "failed to open group")

                    /* Add a trailing slash to the link path buffer to denote that this link points to a group */
                    cur_link_path_len = strlen(iter_data->u.link_iter_data.recursive_link_path);
                    while(cur_link_path_len + 2 > iter_data->u.link_iter_data.recursive_link_path_nalloc) {
                        char *tmp_realloc;

                        iter_data->u.link_iter_data.recursive_link_path_nalloc *= 2;
                        if(NULL == (tmp_realloc = DV_realloc(iter_data->u.link_iter_data.recursive_link_path,
                                iter_data->u.link_iter_data.recursive_link_path_nalloc)))
                            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to reallocate link path buffer")

                        iter_data->u.link_iter_data.recursive_link_path = tmp_realloc;
                    } /* end while */

                    iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len] = '/';
                    iter_data->u.link_iter_data.recursive_link_path[cur_link_path_len + 1] = '\0';

                    /* Recurse on this group */
                    iter_data->u.link_iter_data.recurse_depth++;
                    recurse_ret = H5_daos_link_iterate_by_crt_order(subgroup, iter_data);
                    iter_data->u.link_iter_data.recurse_depth--;

                    if(recurse_ret < 0)
                        D_GOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "recursive link iteration failed")
                    else if(recurse_ret)
                        D_GOTO_DONE(recurse_ret) /* Short-circuit success */

                    if(H5_daos_group_close(subgroup, iter_data->dxpl_id, iter_data->req) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
                    subgroup = NULL;
                } /* end if */

                /*
                 * Strip the current link name (and, for groups, any trailing slash)
                 * back off of the recursive link path.
                 */
                *cur_link_path_end = '\0';
            } /* end if */
        } /* end if */

        /* Update iteration index */
        if(iter_data->idx_p)
            (*iter_data->idx_p) = (hsize_t)cur_idx; /* TODO handle correct value for decreasing order iteration */
    } /* end for */

    ret_value = op_ret;

done:
    if(subgroup) {
        if(H5_daos_group_close(subgroup, iter_data->dxpl_id, iter_data->req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        subgroup = NULL;
    }

    if(H5L_TYPE_SOFT == link_val.type)
        link_val.target.soft = (char *)DV_free(link_val.target.soft);

    if(link_name_buf_dyn)
        link_name_buf_dyn = DV_free(link_name_buf_dyn);

    /*
     * Free resources allocated for recursive iteration once we reach the top
     * level of recursion again.
     */
    if(iter_data->is_recursive && (iter_data->u.link_iter_data.recurse_depth == 0)) {
        iter_data->u.link_iter_data.recursive_link_path = DV_free(iter_data->u.link_iter_data.recursive_link_path);

        if(iter_data->u.link_iter_data.visited_link_table) {
            dv_hash_table_free(iter_data->u.link_iter_data.visited_link_table);
            iter_data->u.link_iter_data.visited_link_table = NULL;
        } /* end if */
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_link_iterate_by_crt_order() */


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
    (*((uint64_t *) op_data))++;
    return 0;
} /* end H5_daos_link_iterate_count_links_callback() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_delete
 *
 * Purpose:     Deletes the link from the specified group according to
 *              either the given link pathname or the given index into the
 *              group's link creation order/link name index.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_delete(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *target_grp = NULL;
    daos_key_t dkey;
    const char *grp_path = NULL;
    const char *target_link_name = NULL;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    ssize_t grp_nlinks;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(H5VL_OBJECT_BY_NAME == loc_params->type || H5VL_OBJECT_BY_IDX == loc_params->type);

    grp_path = (H5VL_OBJECT_BY_NAME == loc_params->type)
            ? loc_params->loc_data.loc_by_name.name : loc_params->loc_data.loc_by_idx.name;

    /* Traverse the path */
    if(NULL == (target_grp = H5_daos_group_traverse(item, grp_path,
            dxpl_id, req, &target_link_name, NULL, NULL)))
        D_GOTO_ERROR(H5E_SYM, H5E_TRAVERSE, FAIL, "can't traverse path")

    if(H5VL_OBJECT_BY_IDX == loc_params->type) {
        ssize_t link_name_size;

        /* Retrieve the name of the link at the given index */
        if((link_name_size = H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                link_name_buf_static, H5_DAOS_LINK_NAME_BUF_SIZE)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

        /* Check that buffer was large enough to fit link name */
        if(link_name_size > H5_DAOS_LINK_NAME_BUF_SIZE - 1) {
            if(NULL == (link_name_buf_dyn = DV_malloc(link_name_size + 1)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate buffer for link name")

            /* Re-issue the call with a larger buffer */
            if(H5_daos_link_get_name_by_idx(target_grp, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                    link_name_buf_dyn, link_name_size + 1) < 0)
                D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link name")

            target_link_name = link_name_buf_dyn;
        } /* end if */
        else
            target_link_name = link_name_buf_static;
    } /* end if */

    /* Setup dkey */
    daos_iov_set(&dkey, (void *)target_link_name, strlen(target_link_name));

    /* Punch the link's dkey, along with all of its akeys */
    if(0 != (ret = daos_obj_punch_dkeys(target_grp->obj.obj_oh, DAOS_TX_NONE, 1, &dkey, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_CANTREMOVE, FAIL, "failed to punch link dkey: %s", H5_daos_err_to_string(ret))

    /* TODO: If no more hard links point to the object in question, it should be
     * removed from the file, or at least marked to be removed.
     */

    /* If link creation order is tracked, perform some bookkeeping */
    if(target_grp->gcpl_cache.track_corder) {
        /* Update the "number of links" key in the group */
        if((grp_nlinks = H5_daos_group_get_num_links(target_grp)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get number of links in group")
        if(grp_nlinks > 0)
            grp_nlinks--;

        if(H5_daos_group_update_num_links_key(target_grp, (uint64_t)grp_nlinks) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTMODIFY, FAIL, "can't update number of links in group")

        /* Remove the link from the group's creation order index */
        if(H5_daos_link_remove_from_crt_idx(target_grp, loc_params) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTREMOVE, FAIL, "failed to remove link from creation order index")
    } /* end if */

done:
    if(link_name_buf_dyn)
        link_name_buf_dyn = DV_free(link_name_buf_dyn);
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_link_delete() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_remove_from_crt_idx
 *
 * Purpose:     Removes the target link from the target group's link
 *              creation order index by locating the relevant akeys and
 *              then removing them.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_remove_from_crt_idx(H5_daos_group_t *target_grp, const H5VL_loc_params_t *loc_params)
{
    daos_key_t dkey;
    daos_key_t akeys[2];
    uint64_t delete_idx = 0;
    uint8_t crt_order_target_buf[H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE];
    uint8_t idx_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t *p;
    ssize_t grp_nlinks_remaining;
    hid_t target_grp_id = H5I_INVALID_HID;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(loc_params);
    assert(H5VL_OBJECT_BY_NAME == loc_params->type || H5VL_OBJECT_BY_IDX == loc_params->type);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Retrieve the current number of links in the group */
    if((grp_nlinks_remaining = H5_daos_group_get_num_links(target_grp)) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get the number of links in group")

    if(H5VL_OBJECT_BY_IDX == loc_params->type) {
        /* DSINC - no check for safe cast here */
        /*
         * Note that this assumes this routine is always called after a link's
         * dkey is punched during deletion, so the number of links in the group
         * should reflect the number after the link has been removed.
         */
        delete_idx = (H5_ITER_DEC == loc_params->loc_data.loc_by_idx.order) ?
                (uint64_t)grp_nlinks_remaining - (uint64_t)loc_params->loc_data.loc_by_idx.n :
                (uint64_t)loc_params->loc_data.loc_by_idx.n;
    } /* end if */
    else {
        H5_daos_link_crt_idx_iter_ud_t iter_cb_ud;
        H5_daos_iter_data_t iter_data;

        /* Register ID for group for link iteration */
        if((target_grp_id = H5VLwrap_register(target_grp, H5I_GROUP)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
        target_grp->obj.item.rc++;

        /* Initialize iteration data */
        iter_cb_ud.target_link_name = loc_params->loc_data.loc_by_name.name;
        iter_cb_ud.link_idx_out = &delete_idx;
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, H5_INDEX_CRT_ORDER, H5_ITER_INC,
                FALSE, NULL, target_grp_id, &iter_cb_ud, H5P_DATASET_XFER_DEFAULT, NULL);
        iter_data.u.link_iter_data.link_iter_op = H5_daos_link_remove_from_crt_idx_name_cb;

        /*
         * TODO: Currently, deleting a link by name means that we need to iterate through
         *       the link creation order index until we find the value corresponding to
         *       the link being deleted. This is especially important because the deletion
         *       of links might cause the target link's index value to shift downwards.
         *
         *       Once iteration restart is supported for link iteration, performance can
         *       be improved here by first looking up the original, permanent creation order
         *       value of the link using the 'link name -> creation order' mapping and then
         *       using that value as the starting point for iteration. In this case, the
         *       iteration order MUST be switched to H5_ITER_DEC or the key will not be
         *       found by the iteration.
         */
        if(H5_daos_link_iterate(target_grp, &iter_data) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "link iteration failed")
    } /* end else */

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

    /* Set up akeys */

    /* Remove the akey which maps creation order -> link name */
    p = idx_buf;
    UINT64ENCODE(p, delete_idx);
    daos_iov_set(&akeys[0], (void *)idx_buf, H5_DAOS_ENCODED_CRT_ORDER_SIZE);

    /* Remove the akey which maps creation order -> link target */
    p = crt_order_target_buf;
    UINT64ENCODE(p, delete_idx);
    crt_order_target_buf[H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE - 1] = 0;
    daos_iov_set(&akeys[1], (void *)crt_order_target_buf, H5_DAOS_CRT_ORDER_TO_LINK_TRGT_BUF_SIZE);

    /* Remove the akeys */
    if(0 != (ret = daos_obj_punch_akeys(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 2, akeys, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_CANTREMOVE, FAIL, "failed to punch link akeys: %s", H5_daos_err_to_string(ret))

    /*
     * If there are still links remaining in the group and we didn't delete the
     * link currently at the end of the creation order index, shift the indices
     * of all akeys past the removed link's akeys down by one. This maintains
     * the ability to directly index into the link creation order index.
     */
    if((grp_nlinks_remaining > 0) && (delete_idx < (uint64_t)grp_nlinks_remaining))
        if(H5_daos_link_shift_crt_idx_keys_down(target_grp, delete_idx + 1, (uint64_t)grp_nlinks_remaining) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTMODIFY, FAIL, "failed to update link creation order index")

done:
    if((target_grp_id >= 0) && (H5Idec_ref(target_grp_id) < 0))
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group ID")

    D_FUNC_LEAVE
} /* end H5_daos_link_remove_from_crt_idx() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_remove_from_crt_idx_name_cb
 *
 * Purpose:     Link iteration callback for
 *              H5_daos_link_remove_from_crt_idx which iterates through
 *              links by creation order until the current link name matches
 *              the target link name, at which point the creation order
 *              index value for the target link has been found.
 *
 * Return:      Non-negative (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_remove_from_crt_idx_name_cb(hid_t H5VL_DAOS_UNUSED group, const char *name,
    const H5L_info_t H5VL_DAOS_UNUSED *info, void *op_data)
{
    H5_daos_link_crt_idx_iter_ud_t *cb_ud = (H5_daos_link_crt_idx_iter_ud_t *) op_data;

    if(!strcmp(name, cb_ud->target_link_name))
        return 1;

    (*cb_ud->link_idx_out)++;
    return 0;
} /* end H5_daos_link_remove_from_crt_idx_name_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_shift_crt_idx_keys_down
 *
 * Purpose:     After a link has been deleted in a group, this routine
 *              is used to update a group's link creation order index. All
 *              of the index's akeys within the range specified by the
 *              begin and end index parameters are read and then re-written
 *              to the index under new akeys whose integer 'name' values
 *              are one less than the akeys' original values.
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
H5_daos_link_shift_crt_idx_keys_down(H5_daos_group_t *target_grp,
    uint64_t idx_begin, uint64_t idx_end)
{
    daos_sg_list_t *sgls = NULL;
    daos_iod_t *iods = NULL;
    daos_iov_t *sg_iovs = NULL;
    daos_key_t dkey;
    daos_key_t tail_akeys[2];
    uint64_t tmp_uint;
    uint8_t *crt_order_link_name_buf = NULL;
    uint8_t *crt_order_link_trgt_buf = NULL;
    uint8_t *p;
    size_t nlinks_shift;
    size_t i;
    char *tmp_buf = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(idx_end >= idx_begin);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    nlinks_shift = idx_end - idx_begin + 1;

    /*
     * Allocate space for the 2 akeys per link, one akey that maps the link's
     * creation order value to the link's name and one akey that maps the link's
     * creation order value to the link's target.
     */
    if(NULL == (iods = DV_malloc(2 * nlinks_shift * sizeof(*iods))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate IOD buffer")
    if(NULL == (sgls = DV_malloc(2 * nlinks_shift * sizeof(*sgls))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate SGL buffer")
    if(NULL == (sg_iovs = DV_calloc(2 * nlinks_shift * sizeof(*sg_iovs))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate IOV buffer")
    if(NULL == (crt_order_link_name_buf = DV_malloc(nlinks_shift * H5_DAOS_ENCODED_CRT_ORDER_SIZE)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate akey data buffer")
    /*
     * The 'creation order -> link target' akey's integer 'name' value is a 9-byte buffer:
     * 8 bytes for the integer creation order value + 1 0-byte at the end of the buffer.
     */
    if(NULL == (crt_order_link_trgt_buf = DV_malloc(nlinks_shift * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate akey data buffer")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

    /* Set up iods */
    for(i = 0; i < nlinks_shift; i++) {
        tmp_uint = idx_begin + i;

        /* Setup the integer 'name' value for the current 'creation order -> link name' akey */
        p = &crt_order_link_name_buf[i * H5_DAOS_ENCODED_CRT_ORDER_SIZE];
        UINT64ENCODE(p, tmp_uint);

        /* Set up iods for the current 'creation order -> link name' akey */
        memset(&iods[2 * i], 0, sizeof(*iods));
        daos_iov_set(&iods[2 * i].iod_name, &crt_order_link_name_buf[i * H5_DAOS_ENCODED_CRT_ORDER_SIZE], H5_DAOS_ENCODED_CRT_ORDER_SIZE);
        iods[2 * i].iod_nr = 1u;
        iods[2 * i].iod_size = DAOS_REC_ANY;
        iods[2 * i].iod_type = DAOS_IOD_SINGLE;

        /* Setup the integer 'name' value for the current 'creation order -> link target' akey */
        p = &crt_order_link_trgt_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1)];
        UINT64ENCODE(p, tmp_uint);
        *p++ = 0;

        /* Set up iods for the current 'creation order -> link target' akey */
        memset(&iods[(2 * i) + 1], 0, sizeof(*iods));
        daos_iov_set(&iods[(2 * i) + 1].iod_name, &crt_order_link_trgt_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1)], H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1);
        iods[(2 * i) + 1].iod_nr = 1u;
        iods[(2 * i) + 1].iod_size = DAOS_REC_ANY;
        iods[(2 * i) + 1].iod_type = DAOS_IOD_SINGLE;
    } /* end for */

    /* Fetch the data size for each akey */
    if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, (unsigned) 2 * nlinks_shift,
            iods, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "can't read akey data sizes: %s", H5_daos_err_to_string(ret))

    /* Allocate buffers and setup sgls for each akey */
    for(i = 0; i < nlinks_shift; i++) {
        /* Allocate buffer for the current 'creation order -> link name' akey */
        if(iods[2 * i].iod_size == 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADSIZE, FAIL, "invalid iod size - missing metadata")
        if(NULL == (tmp_buf = DV_malloc(iods[2 * i].iod_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey data")

        /* Set up sgls for the current 'creation order -> link name' akey */
        daos_iov_set(&sg_iovs[2 * i], tmp_buf, iods[2 * i].iod_size);
        sgls[2 * i].sg_nr = 1;
        sgls[2 * i].sg_nr_out = 0;
        sgls[2 * i].sg_iovs = &sg_iovs[2 * i];

        /* Allocate buffer for the current 'creation order -> link target' akey */
        if(iods[(2 * i) + 1].iod_size == 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADSIZE, FAIL, "invalid iod size - missing metadata")
        if(NULL == (tmp_buf = DV_malloc(iods[(2 * i) + 1].iod_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey data")

        /* Set up sgls for the current 'creation order -> link target' akey */
        daos_iov_set(&sg_iovs[(2 * i) + 1], tmp_buf, iods[(2 * i) + 1].iod_size);
        sgls[(2 * i) + 1].sg_nr = 1;
        sgls[(2 * i) + 1].sg_nr_out = 0;
        sgls[(2 * i) + 1].sg_iovs = &sg_iovs[(2 * i) + 1];
    } /* end for */

    /* Read the akey's data */
    if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, (unsigned) 2 * nlinks_shift,
            iods, sgls, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "can't read akey data: %s", H5_daos_err_to_string(ret))

    /*
     * Adjust the akeys down by setting their integer 'name' values to
     * one less than their original values
     */
    for(i = 0; i < nlinks_shift; i++) {
        /* Setup the integer 'name' value for the current 'creation order -> link name' akey */
        p = &crt_order_link_name_buf[i * H5_DAOS_ENCODED_CRT_ORDER_SIZE];
        UINT64DECODE(p, tmp_uint);

        tmp_uint--;
        p = &crt_order_link_name_buf[i * H5_DAOS_ENCODED_CRT_ORDER_SIZE];
        UINT64ENCODE(p, tmp_uint);

        /* Setup the integer 'name' value for the current 'creation order -> link target' akey */
        p = &crt_order_link_trgt_buf[i * (H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1)];
        UINT64ENCODE(p, tmp_uint);
        *p++ = 0;
    } /* end for */

    /* Write the akeys back */
    if(0 != (ret = daos_obj_update(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, (unsigned) 2 * nlinks_shift,
            iods, sgls, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_WRITEERROR, FAIL, "can't write akey data: %s", H5_daos_err_to_string(ret))

    /* Delete the (now invalid) akeys at the end of the creation index */
    tmp_uint = idx_end;
    p = &crt_order_link_name_buf[0];
    UINT64ENCODE(p, tmp_uint);
    daos_iov_set(&tail_akeys[0], (void *)crt_order_link_name_buf, H5_DAOS_ENCODED_CRT_ORDER_SIZE);

    p = &crt_order_link_trgt_buf[0];
    UINT64ENCODE(p, tmp_uint);
    *p++ = 0;
    daos_iov_set(&tail_akeys[1], (void *)crt_order_link_trgt_buf, H5_DAOS_ENCODED_CRT_ORDER_SIZE + 1);

    if(0 != (ret = daos_obj_punch_akeys(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey,
            2, tail_akeys, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "can't trim tail akeys from link creation order index")

done:
    for(i = 0; i < nlinks_shift; i++) {
        if(sg_iovs[2 * i].iov_buf)
            sg_iovs[2 * i].iov_buf = DV_free(sg_iovs[2 * i].iov_buf);
        if(sg_iovs[(2 * i) + 1].iov_buf)
            sg_iovs[(2 * i) + 1].iov_buf = DV_free(sg_iovs[(2 * i) + 1].iov_buf);
    } /* end for */
    if(crt_order_link_trgt_buf)
        crt_order_link_trgt_buf = DV_free(crt_order_link_trgt_buf);
    if(crt_order_link_name_buf)
        crt_order_link_name_buf = DV_free(crt_order_link_name_buf);
    if(sg_iovs)
        sg_iovs = DV_free(sg_iovs);
    if(sgls)
        sgls = DV_free(sgls);
    if(iods)
        iods = DV_free(iods);

    D_FUNC_LEAVE
} /* end H5_daos_link_shift_crt_idx_keys_down() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_name_by_idx
 *
 * Purpose:     Given an index type, index iteration order and index value,
 *              retrieves the name of the nth link (as specified by the
 *              index value) within the given index (name index or creation
 *              order index) according to the given order (increasing,
 *              decreasing or native order).
 *
 *              The link_name_out parameter may be NULL, in which case the
 *              length of the link's name is simply returned. If non-NULL,
 *              the link's name is stored in link_name_out.
 *
 * Return:      Success:        The length of the link's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5_daos_link_get_name_by_idx(H5_daos_group_t *target_grp, H5_index_t index_type,
    H5_iter_order_t iter_order, uint64_t idx, char *link_name_out, size_t link_name_out_size)
{
    ssize_t ret_value = 0;

    assert(target_grp);

    if(H5_INDEX_CRT_ORDER == index_type) {
        if((ret_value = H5_daos_link_get_name_by_crt_order(target_grp, iter_order, idx, link_name_out, link_name_out_size)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, (-1), "can't retrieve link name from creation order index")
    } /* end if */
    else if(H5_INDEX_NAME == index_type) {
        if((ret_value = H5_daos_link_get_name_by_name_order(target_grp, iter_order, idx, link_name_out, link_name_out_size)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, (-1), "can't retrieve link name from name order index")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, (-1), "invalid or unsupported index type")

done:
    D_FUNC_LEAVE
} /* end H5_daos_link_get_name_by_idx() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_name_by_crt_order
 *
 * Purpose:     Given an index iteration order and index value, retrieves
 *              the name of the nth link (as specified by the index value)
 *              within the specified group's link creation order index,
 *              according to the given order (increasing, decreasing or
 *              native order).
 *
 *              The link_name_out parameter may be NULL, in which case the
 *              length of the link's name is simply returned. If non-NULL,
 *              the link's name is stored in link_name_out.
 *
 * Return:      Success:        The length of the link's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5_daos_link_get_name_by_crt_order(H5_daos_group_t *target_grp, H5_iter_order_t iter_order,
    uint64_t index, char *link_name_out, size_t link_name_out_size)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint64_t fetch_idx = 0;
    uint8_t idx_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t *p;
    ssize_t grp_nlinks;
    int ret;
    ssize_t ret_value = 0;

    assert(target_grp);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Check that creation order is tracked for target group */
    if(!target_grp->gcpl_cache.track_corder)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, (-1), "creation order is not tracked for group")

    /* Retrieve the current number of links in the group */
    if((grp_nlinks = H5_daos_group_get_num_links(target_grp)) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, (-1), "can't get number of links in group")

    /* Ensure the index is within range */
    if(index >= (uint64_t)grp_nlinks)
        D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, (-1), "index value out of range")

    /* Calculate the correct index of the link, based upon the iteration order */
    if(H5_ITER_DEC == iter_order)
        fetch_idx = grp_nlinks - index - 1;
    else
        fetch_idx = index;

    p = idx_buf;
    UINT64ENCODE(p, fetch_idx);

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)idx_buf, H5_DAOS_ENCODED_CRT_ORDER_SIZE);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl if link_name_out buffer is supplied */
    if(link_name_out && link_name_out_size > 0) {
        daos_iov_set(&sg_iov, link_name_out, link_name_out_size - 1);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;
    } /* end if */

    /* Fetch the size of the link's name + link's name if link_name_out is supplied */
    if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod,
            link_name_out ? &sgl : NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, (-1), "can't fetch%s link name: %s", link_name_out ? "" : " size of", H5_daos_err_to_string(ret))

    if(iod.iod_size == (daos_size_t)0)
        D_GOTO_ERROR(H5E_LINK, H5E_NOTFOUND, (-1), "link name record not found")

    if(link_name_out && link_name_out_size > 0)
        link_name_out[MIN(iod.iod_size, link_name_out_size - 1)] = '\0';

    ret_value = (ssize_t)iod.iod_size;

done:
    D_FUNC_LEAVE
} /* end H5_daos_link_get_name_by_crt_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_name_by_name_order
 *
 * Purpose:     Given an index iteration order and index value, retrieves
 *              the name of the nth link (as specified by the index value)
 *              within the specified group's link name index, according to
 *              the given order (increasing, decreasing or native order).
 *
 *              The link_name_out parameter may be NULL, in which case the
 *              length of the link's name is simply returned. If non-NULL,
 *              the link's name is stored in link_name_out.
 *
 * Return:      Success:        The length of the link's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5_daos_link_get_name_by_name_order(H5_daos_group_t *target_grp, H5_iter_order_t iter_order,
    uint64_t index, char *link_name_out, size_t link_name_out_size)
{
    H5_daos_link_find_name_by_idx_ud_t iter_cb_ud;
    H5_daos_iter_data_t iter_data;
    ssize_t grp_nlinks;
    hid_t target_grp_id = H5I_INVALID_HID;
    ssize_t ret_value = 0;

    assert(target_grp);

    if(H5_ITER_DEC == iter_order)
        D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, (-1), "decreasing order iteration is unsupported")

    /* Retrieve the current number of links in the group */
    if((grp_nlinks = H5_daos_group_get_num_links(target_grp)) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, (-1), "can't get number of links in group")

    /* Ensure the index is within range */
    if(index >= (uint64_t)grp_nlinks)
        D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, (-1), "index value out of range")

    /* Register ID for target group */
    if((target_grp_id = H5VLwrap_register(target_grp, H5I_GROUP)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, (-1), "unable to atomize object handle")
    target_grp->obj.item.rc++;

    /* Initialize iteration data */
    iter_cb_ud.target_link_idx = index;
    iter_cb_ud.cur_link_idx = 0;
    iter_cb_ud.link_name_out = link_name_out;
    iter_cb_ud.link_name_out_size = link_name_out_size;
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, H5_INDEX_NAME, iter_order,
            FALSE, NULL, target_grp_id, &iter_cb_ud, H5P_DATASET_XFER_DEFAULT, NULL);
    iter_data.u.link_iter_data.link_iter_op = H5_daos_link_get_name_by_name_order_cb;

    if(H5_daos_link_iterate(target_grp, &iter_data) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_BADITER, (-1), "link iteration failed")

    ret_value = (ssize_t)iter_cb_ud.link_name_out_size;

done:
    if((target_grp_id >= 0) && (H5Idec_ref(target_grp_id) < 0))
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, (-1), "can't close group ID")

    D_FUNC_LEAVE
} /* end H5_daos_link_get_name_by_name_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_name_by_name_order_cb
 *
 * Purpose:     Link iteration callback for
 *              H5_daos_link_get_name_by_name_order which iterates through
 *              links by name order until the specified index value is
 *              reached, at which point the target link has been found and
 *              its name is copied back.
 *
 * Return:      Non-negative (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_link_get_name_by_name_order_cb(hid_t H5VL_DAOS_UNUSED group, const char *name,
    const H5L_info_t H5VL_DAOS_UNUSED *info, void *op_data)
{
    H5_daos_link_find_name_by_idx_ud_t *cb_ud = (H5_daos_link_find_name_by_idx_ud_t *) op_data;

    if(cb_ud->cur_link_idx == cb_ud->target_link_idx) {
        if(cb_ud->link_name_out && cb_ud->link_name_out_size > 0) {
            memcpy(cb_ud->link_name_out, name, cb_ud->link_name_out_size - 1);
            cb_ud->link_name_out[cb_ud->link_name_out_size - 1] = '\0';
        }

        cb_ud->link_name_out_size = strlen(name);

        return 1;
    }

    cb_ud->cur_link_idx++;
    return 0;
} /* end H5_daos_link_get_name_by_name_order_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_link_get_crt_order_by_name
 *
 * Purpose:     Retrieves the creation order value for a link given the
 *              group containing the link and the link's name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_link_get_crt_order_by_name(H5_daos_group_t *target_grp, const char *link_name,
    uint64_t *crt_order)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint64_t crt_order_val;
    uint8_t crt_order_buf[H5_DAOS_ENCODED_CRT_ORDER_SIZE];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_grp);
    assert(link_name);
    assert(crt_order);
    H5daos_compile_assert(H5_DAOS_ENCODED_CRT_ORDER_SIZE == 8);

    /* Check that creation order is tracked for target group */
    if(!target_grp->gcpl_cache.track_corder)
        D_GOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "creation order is not tracked for group")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)link_name, strlen(link_name));

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_link_corder_key_g, H5_daos_link_corder_key_size_g);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, crt_order_buf, (daos_size_t)H5_DAOS_ENCODED_CRT_ORDER_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read link creation order value */
    if(0 != (ret = daos_obj_fetch(target_grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_LINK, H5E_READERROR, FAIL, "can't read link's creation order value: %s", H5_daos_err_to_string(ret))

    if(iod.iod_size == 0)
        D_GOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "link creation order value record is missing")

    p = crt_order_buf;
    UINT64DECODE(p, crt_order_val);

    *crt_order = crt_order_val;

done:
    D_FUNC_LEAVE
} /* H5_daos_link_get_crt_order_by_name() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_hash_obj_id
 *
 * Purpose:     Helper function to "hash" a DAOS object ID's lower 64 bits
 *              by simply returning the value passed in.
 *
 * Return:      "hashed" DAOS object ID
 *
 *-------------------------------------------------------------------------
 */
static uint64_t
H5_daos_hash_obj_id(dv_hash_table_key_t obj_id_lo)
{
    return *((uint64_t *) obj_id_lo);
} /* end H5_daos_hash_obj_id() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_cmp_obj_id
 *
 * Purpose:     Helper function to compare two "hashed" DAOS object IDs in
 *              a dv_hash_table_t.
 *
 * Return:      Non-zero if the two keys are equal, zero if the keys are
 *              not equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_cmp_obj_id(dv_hash_table_key_t obj_id_lo1, dv_hash_table_key_t obj_id_lo2)
{
    uint64_t val1 = *((uint64_t *) obj_id_lo1);
    uint64_t val2 = *((uint64_t *) obj_id_lo2);

    return (val1 == val2);
} /* H5_daos_cmp_obj_id() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_free_visited_link_hash_table_key
 *
 * Purpose:     Helper function to free keys in the visited link hash table
 *              used by link iteration.
 *
 * Return:      Nothing
 *
 *-------------------------------------------------------------------------
 */
static void
H5_daos_free_visited_link_hash_table_key(dv_hash_table_key_t value)
{
    DV_free(value);
    value = NULL;
} /* end H5_daos_free_visited_link_hash_table_key() */
