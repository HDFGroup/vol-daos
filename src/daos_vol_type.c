/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library. Datatype routines.
 */

#include "daos_vol_private.h" /* DAOS connector                          */

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

/****************/
/* Local Macros */
/****************/

#define H5_DAOS_TINFO_BCAST_BUF_SIZE                                                                         \
    (H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_TCPL_BUF_SIZE + H5_DAOS_ENCODED_OID_SIZE +                              \
     2 * H5_DAOS_ENCODED_UINT64_T_SIZE)

/********************/
/* Local Prototypes */
/********************/

static int H5_daos_tinfo_read_comp_cb(tse_task_t *task, void *args);
static int H5_daos_datatype_open_bcast_comp_cb(tse_task_t *task, void *args);
static int H5_daos_datatype_open_recv_comp_cb(tse_task_t *task, void *args);
static int H5_daos_datatype_open_end(H5_daos_dtype_t *dtype, uint8_t *p, uint64_t type_buf_len,
                                     uint64_t tcpl_buf_len, hid_t dxpl_id);

static htri_t H5_daos_need_bkg(hid_t src_type_id, hid_t dst_type_id, hbool_t dst_file, size_t *dst_type_size,
                               hbool_t *fill_bkg);

/*-------------------------------------------------------------------------
 * Function:    H5_daos_detect_vl_vlstr_ref
 *
 * Purpose:     Determine if datatype conversion is necessary even if the
 *              types are the same.
 *
 * Return:      Success:        1 if conversion needed, 0 otherwise
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5_daos_detect_vl_vlstr_ref(hid_t type_id)
{
    hid_t       memb_type_id = -1;
    H5T_class_t tclass;
    htri_t      ret_value = FALSE;

    /* Get datatype class */
    if (H5T_NO_CLASS == (tclass = H5Tget_class(type_id)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get type class");

    switch (tclass) {
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_ENUM:
            /* No conversion necessary */
            ret_value = FALSE;

            break;

        case H5T_STRING:
            /* Check for vlen string, need conversion if it's vl */
            if ((ret_value = H5Tis_variable_str(type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't check for variable length string");

            break;

        case H5T_COMPOUND: {
            int nmemb;
            int i;

            /* Get number of compound members */
            if ((nmemb = H5Tget_nmembers(type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL,
                             "can't get number of destination compound members");

            /* Iterate over compound members, checking for a member in
             * dst_type_id with no match in src_type_id */
            for (i = 0; i < nmemb; i++) {
                /* Get member type */
                if ((memb_type_id = H5Tget_member_type(type_id, (unsigned)i)) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type");

                /* Recursively check member type, this will fill in the
                 * member size */
                if ((ret_value = H5_daos_detect_vl_vlstr_ref(memb_type_id)) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed");

                /* Close member type */
                if (H5Tclose(memb_type_id) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type");
                memb_type_id = -1;

                /* If any member needs conversion the entire compound does
                 */
                if (ret_value) {
                    ret_value = TRUE;
                    break;
                } /* end if */
            }     /* end for */

            break;
        } /* end block */

        case H5T_ARRAY:
            /* Get parent type */
            if ((memb_type_id = H5Tget_super(type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type");

            /* Recursively check parent type */
            if ((ret_value = H5_daos_detect_vl_vlstr_ref(memb_type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed");

            /* Close parent type */
            if (H5Tclose(memb_type_id) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type");
            memb_type_id = -1;

            break;

        case H5T_REFERENCE:
        case H5T_VLEN:
            /* Always need type conversion for references and vlens */
            ret_value = TRUE;

            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid type class");
    } /* end switch */

done:
    /* Cleanup on failure */
    if (memb_type_id >= 0)
        if (H5Idec_ref(memb_type_id) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close member type");

    D_FUNC_LEAVE;
} /* end H5_daos_detect_vl_vlstr_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_need_tconv
 *
 * Purpose:     Determine if datatype conversion is necessary.
 *
 * Return:      Success:        1 if conversion needed, 0 otherwise
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5_daos_need_tconv(hid_t src_type_id, hid_t dst_type_id)
{
    htri_t types_equal;
    htri_t ret_value;

    /* Check if the types are equal */
    if ((types_equal = H5Tequal(src_type_id, dst_type_id)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOMPARE, FAIL, "can't check if types are equal");

    if (types_equal) {
        /* Check if conversion is needed anyways due to presence of a vlen or
         * reference type */
        if ((ret_value = H5_daos_detect_vl_vlstr_ref(src_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check for vlen or reference type");
    } /* end if */
    else
        ret_value = TRUE;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_need_tconv() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_need_bkg
 *
 * Purpose:     Determine if a background buffer is needed for conversion.
 *
 * Return:      Success:        1 if bkg buffer needed, 0 otherwise
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5_daos_need_bkg(hid_t src_type_id, hid_t dst_type_id, hbool_t dst_file, size_t *dst_type_size,
                 hbool_t *fill_bkg)
{
    hid_t       memb_type_id     = -1;
    hid_t       src_memb_type_id = -1;
    char       *memb_name        = NULL;
    size_t      memb_size;
    H5T_class_t tclass;
    htri_t      ret_value = FALSE;

    assert(dst_type_size);
    assert(fill_bkg);

    /* Get destination type size */
    if ((*dst_type_size = H5Tget_size(dst_type_id)) == 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get source type size");

    /* Get datatype class */
    if (H5T_NO_CLASS == (tclass = H5Tget_class(dst_type_id)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get type class");

    switch (tclass) {
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_ENUM:

            /* No background buffer necessary */
            ret_value = FALSE;

            break;

        case H5T_REFERENCE:
        case H5T_VLEN:

            /* If the destination type is in the the file, the background buffer
             * is necessary so we can delete old sequences. */
            if (dst_file) {
                ret_value = TRUE;
                *fill_bkg = TRUE;
            } /* end if */
            else
                ret_value = FALSE;

            break;

        case H5T_COMPOUND: {
            int    nmemb;
            size_t size_used = 0;
            int    src_i;
            int    i;

            /* We must always provide a background buffer for compound
             * conversions.  Only need to check further to see if it must be
             * filled. */
            ret_value = TRUE;

            /* Get number of compound members */
            if ((nmemb = H5Tget_nmembers(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL,
                             "can't get number of destination compound members");

            /* Iterate over compound members, checking for a member in
             * dst_type_id with no match in src_type_id */
            for (i = 0; i < nmemb; i++) {
                /* Get member type */
                if ((memb_type_id = H5Tget_member_type(dst_type_id, (unsigned)i)) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type");

                /* Get member name */
                if (NULL == (memb_name = H5Tget_member_name(dst_type_id, (unsigned)i)))
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member name");

                /* Check for matching name in source type */
                H5E_BEGIN_TRY
                {
                    src_i = H5Tget_member_index(src_type_id, memb_name);
                }
                H5E_END_TRY

                /* Free memb_name */
                if (H5free_memory(memb_name) < 0)
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't free member name");
                memb_name = NULL;

                /* If no match was found, this type is not being filled in,
                 * so we must fill the background buffer */
                if (src_i < 0) {
                    if (H5Tclose(memb_type_id) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type");
                    memb_type_id = -1;
                    *fill_bkg    = TRUE;
                    D_GOTO_DONE(TRUE);
                } /* end if */

                /* Open matching source type */
                if ((src_memb_type_id = H5Tget_member_type(src_type_id, (unsigned)src_i)) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type");

                /* Recursively check member type, this will fill in the
                 * member size */
                if (H5_daos_need_bkg(src_memb_type_id, memb_type_id, dst_file, &memb_size, fill_bkg) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed");

                /* Close source member type */
                if (H5Tclose(src_memb_type_id) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type");
                src_memb_type_id = -1;

                /* Close member type */
                if (H5Tclose(memb_type_id) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type");
                memb_type_id = -1;

                /* If the source member type needs the background filled, so
                 * does the parent */
                if (*fill_bkg)
                    D_GOTO_DONE(TRUE);

                /* Keep track of the size used in compound */
                size_used += memb_size;
            } /* end for */

            /* Check if all the space in the type is used.  If not, we must
             * fill the background buffer. */
            /* TODO: This is only necessary on read, we don't care about
             * compound gaps in the "file" DSINC */
            assert(size_used <= *dst_type_size);
            if (size_used != *dst_type_size)
                *fill_bkg = TRUE;

            break;
        } /* end block */

        case H5T_ARRAY:
            /* Get parent type */
            if ((memb_type_id = H5Tget_super(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type");

            /* Get source parent type */
            if ((src_memb_type_id = H5Tget_super(src_type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type");

            /* Recursively check parent type */
            if ((ret_value =
                     H5_daos_need_bkg(src_memb_type_id, memb_type_id, dst_file, &memb_size, fill_bkg)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed");

            /* Close source parent type */
            if (H5Tclose(src_memb_type_id) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type");
            src_memb_type_id = -1;

            /* Close parent type */
            if (H5Tclose(memb_type_id) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type");
            memb_type_id = -1;

            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid type class");
    } /* end switch */

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (memb_type_id >= 0)
            if (H5Idec_ref(memb_type_id) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close member type");
        if (src_memb_type_id >= 0)
            if (H5Idec_ref(src_memb_type_id) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close source member type");
        memb_name = (char *)DV_free(memb_name);
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_need_bkg() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_tconv_init
 *
 * Purpose:     DSINC
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_tconv_init(hid_t src_type_id, size_t *src_type_size, hid_t dst_type_id, size_t *dst_type_size,
                   size_t num_elem, hbool_t clear_tconv_buf, hbool_t dst_file, void **tconv_buf,
                   void **bkg_buf, H5_daos_tconv_reuse_t *reuse, hbool_t *fill_bkg)
{
    htri_t need_bkg;
    herr_t ret_value = SUCCEED;

    assert(src_type_size);
    assert(dst_type_size);
    assert(tconv_buf);
    assert(!*tconv_buf);
    assert(bkg_buf);
    assert(!*bkg_buf);
    assert(fill_bkg);
    assert(!*fill_bkg);

    /*
     * If there is no selection in the file dataspace, don't bother
     * trying to allocate any type conversion buffers.
     */
    if (num_elem == 0)
        D_GOTO_DONE(SUCCEED);

    /* Get source type size */
    if ((*src_type_size = H5Tget_size(src_type_id)) == 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get source type size");

    /* Check if we need a background buffer */
    if ((need_bkg = H5_daos_need_bkg(src_type_id, dst_type_id, dst_file, dst_type_size, fill_bkg)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed");

    /* Check for reusable destination buffer */
    if (reuse) {
        assert(*reuse == H5_DAOS_TCONV_REUSE_NONE);

        /* Use dest buffer for type conversion if it large enough, otherwise
         * use it for the background buffer if one is needed. */
        if (*dst_type_size >= *src_type_size)
            *reuse = H5_DAOS_TCONV_REUSE_TCONV;
        else if (need_bkg)
            *reuse = H5_DAOS_TCONV_REUSE_BKG;
    } /* end if */

    /* Allocate conversion buffer if it is not being reused */
    if (!reuse || (*reuse != H5_DAOS_TCONV_REUSE_TCONV)) {
        if (clear_tconv_buf) {
            if (NULL == (*tconv_buf = DV_calloc(
                             num_elem * (*src_type_size > *dst_type_size ? *src_type_size : *dst_type_size))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate type conversion buffer");
        } /* end if */
        else if (NULL ==
                 (*tconv_buf = DV_malloc(
                      num_elem * (*src_type_size > *dst_type_size ? *src_type_size : *dst_type_size))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate type conversion buffer");
    } /* end if */

    /* Allocate background buffer if one is needed and it is not being
     * reused */
    if (need_bkg && (!reuse || (*reuse != H5_DAOS_TCONV_REUSE_BKG)))
        if (NULL == (*bkg_buf = DV_calloc(num_elem * *dst_type_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate background buffer");

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        *tconv_buf = DV_free(*tconv_buf);
        *bkg_buf   = DV_free(*bkg_buf);
        if (reuse)
            *reuse = H5_DAOS_TCONV_REUSE_NONE;
    } /* end if */

    D_FUNC_LEAVE;
} /* end H5_daos_tconv_init() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_commit
 *
 * Purpose:     Commits a datatype inside the container.
 *
 * Return:      Success:        datatype ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              June, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_datatype_commit(void *_item, const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
                        hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
                        hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_item_t  *item            = (H5_daos_item_t *)_item;
    H5_daos_dtype_t *dtype           = NULL;
    H5_daos_obj_t   *target_obj      = NULL;
    char            *path_buf        = NULL;
    const char      *target_name     = NULL;
    size_t           target_name_len = 0;
    hbool_t          collective      = FALSE;
    H5_daos_req_t   *int_req         = NULL;
    tse_task_t      *first_task      = NULL;
    tse_task_t      *dep_task        = NULL;
    int              ret;
    void            *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype parent object is NULL");
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
    H5_DAOS_GET_METADATA_WRITE_MODE(item->file, tapl_id, H5P_DATATYPE_ACCESS_DEFAULT, collective,
                                    H5E_DATATYPE, NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "datatype commit", item->open_req, NULL, NULL,
                                              H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, 0, NULL /*event*/)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't start transaction");
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
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADITER, NULL, "can't traverse path");

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        /* Reject invalid object names during object creation - if a name is
         * given it must parse to a link name that can be created */
        if (target_name_len == 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, NULL,
                         "path given does not resolve to a final link name");
    } /* end if */

    /* Create datatype and link to datatype */
    if (NULL == (dtype = (H5_daos_dtype_t *)H5_daos_datatype_commit_helper(
                     item->file, type_id, tcpl_id, tapl_id, (H5_daos_group_t *)target_obj, target_name,
                     target_name_len, collective, int_req, &first_task, &dep_task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't commit datatype");

    /* Set return value */
    ret_value = (void *)dtype;

done:
    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close object");

    if (int_req) {
        H5_daos_op_pool_type_t op_type;

        /* Free path_buf if necessary */
        if (path_buf && H5_daos_free_async(path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTFREE, NULL, "can't free path buffer");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
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
         * need to add to datatype's pool since it's the open request. */
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
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTOPERATE, NULL,
                             "datatype creation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value)
        /* Close datatype */
        if (dtype && H5_daos_datatype_close_real(dtype) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype");

    D_FUNC_LEAVE_API;
} /* end H5_daos_datatype_commit() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_commit_helper
 *
 * Purpose:     Performs the actual datatype commit operation.
 *
 * Return:      Success:        datatype object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_datatype_commit_helper(H5_daos_file_t *file, hid_t type_id, hid_t tcpl_id, hid_t tapl_id,
                               H5_daos_group_t *parent_grp, const char *name, size_t name_len,
                               hbool_t collective, H5_daos_req_t *req, tse_task_t **first_task,
                               tse_task_t **dep_task)
{
    H5_daos_md_rw_cb_ud_flex_t *update_cb_ud = NULL;
    H5_daos_dtype_t            *dtype        = NULL;
    tse_task_t                 *datatype_metatask;
    tse_task_t                 *finalize_deps[2];
    hbool_t                     default_tcpl   = (tcpl_id == H5P_DATATYPE_CREATE_DEFAULT);
    int                         finalize_ndeps = 0;
    int                         ret;
    void                       *ret_value = NULL;

    assert(file);
    assert(file->flags & H5F_ACC_RDWR);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate the datatype object that is returned to the user */
    if (NULL == (dtype = H5FL_CALLOC(H5_daos_dtype_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS datatype struct");
    dtype->obj.item.type     = H5I_DATATYPE;
    dtype->obj.item.created  = TRUE;
    dtype->obj.item.open_req = req;
    req->rc++;
    dtype->obj.item.file = file;
    dtype->obj.item.rc   = 1;
    dtype->obj.obj_oh    = DAOS_HDL_INVAL;
    dtype->type_id       = H5I_INVALID_HID;
    dtype->tcpl_id       = H5P_DATATYPE_CREATE_DEFAULT;
    dtype->tapl_id       = H5P_DATATYPE_ACCESS_DEFAULT;

    /* Generate datatype oid */
    if (H5_daos_oid_generate(&dtype->obj.oid, FALSE, 0, H5I_DATATYPE, (default_tcpl ? H5P_DEFAULT : tcpl_id),
                             H5_DAOS_OBJ_CLASS_NAME, file, collective, req, first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't generate object id");

    /* Open datatype object */
    if (H5_daos_obj_open(file, req, &dtype->obj.oid, DAOS_OO_RW, &dtype->obj.obj_oh, "datatype object open",
                         first_task, dep_task) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype object");

    /* Create datatype and write metadata if this process should */
    if (!collective || (file->my_rank == 0)) {
        size_t      type_size = 0;
        size_t      tcpl_size = 0;
        void       *type_buf  = NULL;
        void       *tcpl_buf  = NULL;
        tse_task_t *update_task;

        /* Create datatype */
        /* Determine serialized datatype size */
        if (H5Tencode(type_id, NULL, &type_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype");

        /* Determine serialized TCPL size if not the default */
        if (!default_tcpl)
            if (H5Pencode2(tcpl_id, NULL, &tcpl_size, file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of tcpl");

        /* Allocate argument struct */
        if (NULL == (update_cb_ud = (H5_daos_md_rw_cb_ud_flex_t *)DV_calloc(
                         sizeof(H5_daos_md_rw_cb_ud_flex_t) + type_size + tcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for update callback arguments");

        /* Encode datatype */
        type_buf = update_cb_ud->flex_buf;
        if (H5Tencode(type_id, type_buf, &type_size) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, NULL, "can't serialize datatype");

        /* Encode TCPL if not the default */
        if (!default_tcpl) {
            tcpl_buf = update_cb_ud->flex_buf + type_size;
            if (H5Pencode2(tcpl_id, tcpl_buf, &tcpl_size, file->fapl_id) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, NULL, "can't serialize tcpl");
        } /* end if */
        else {
            tcpl_buf  = file->def_plist_cache.tcpl_buf;
            tcpl_size = file->def_plist_cache.tcpl_size;
        } /* end else */

        /* Set up operation to write datatype and TCPL to datatype */
        /* Point to datatype object */
        update_cb_ud->md_rw_cb_ud.obj = &dtype->obj;

        /* Point to req */
        update_cb_ud->md_rw_cb_ud.req = req;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        update_cb_ud->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod */
        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[0].iod_name, H5_daos_type_key_g,
                           H5_daos_type_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_size = (uint64_t)type_size;
        update_cb_ud->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_const_iov_set((d_const_iov_t *)&update_cb_ud->md_rw_cb_ud.iod[1].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        update_cb_ud->md_rw_cb_ud.iod[1].iod_nr   = 1u;
        update_cb_ud->md_rw_cb_ud.iod[1].iod_size = (uint64_t)tcpl_size;
        update_cb_ud->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        update_cb_ud->md_rw_cb_ud.free_akeys = FALSE;

        /* Set up sgl */
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[0], type_buf, (daos_size_t)type_size);
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[0].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[0];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        daos_iov_set(&update_cb_ud->md_rw_cb_ud.sg_iov[1], tcpl_buf, (daos_size_t)tcpl_size);
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        update_cb_ud->md_rw_cb_ud.sgl[1].sg_iovs   = &update_cb_ud->md_rw_cb_ud.sg_iov[1];
        update_cb_ud->md_rw_cb_ud.free_sg_iov[1]   = FALSE;

        /* Set nr */
        update_cb_ud->md_rw_cb_ud.nr = 2u;

        /* Set task name */
        update_cb_ud->md_rw_cb_ud.task_name = "datatype metadata write";

        /* Create task for datatype metadata write */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_UPDATE, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_md_update_comp_cb, update_cb_ud, &update_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create task to write datatype metadata");

        /* Schedule datatype metadata write task and give it a reference to req
         * and the datatype */
        assert(first_task);
        if (0 != (ret = tse_task_schedule(update_task, false)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL,
                         "can't schedule task to write datatype metadata: %s", H5_daos_err_to_string(ret));
        req->rc++;
        dtype->obj.item.rc++;
        update_cb_ud = NULL;

        /* Add dependency for finalize task */
        finalize_deps[finalize_ndeps] = update_task;
        finalize_ndeps++;

        /* Create link to datatype */
        if (parent_grp) {
            H5_daos_link_val_t link_val;

            link_val.type                 = H5L_TYPE_HARD;
            link_val.target.hard          = dtype->obj.oid;
            link_val.target_oid_async     = &dtype->obj.oid;
            finalize_deps[finalize_ndeps] = *dep_task;
            if (0 != (ret = H5_daos_link_write(parent_grp, name, name_len, &link_val, req, first_task,
                                               &finalize_deps[finalize_ndeps])))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create link to datatype: %s",
                             H5_daos_err_to_string(ret));
            finalize_ndeps++;
        } /* end if */
        else {
            /* No link to datatype, write a ref count of 0 */
            finalize_deps[finalize_ndeps] = *dep_task;
            if (0 != (ret = H5_daos_obj_write_rc(NULL, &dtype->obj, NULL, 0, req, first_task,
                                                 &finalize_deps[finalize_ndeps])))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't write object ref count: %s",
                             H5_daos_err_to_string(ret));
            finalize_ndeps++;
        } /* end if */
    }     /* end if */
    else {
        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast though it
         * could only be an issue with datatype reopen so we'll skip it for now.
         * There is probably never an issue with file reopen since all commits
         * are from process 0, same as the datatype create above. */

        /* Only dep_task created, register it as the finalize dependency */
        assert(finalize_ndeps == 0);
        assert(*dep_task);
        finalize_deps[0] = *dep_task;
        finalize_ndeps   = 1;
    } /* end else */

    /* Finish setting up datatype struct */
    if ((dtype->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "failed to copy datatype");
    if (!default_tcpl && (dtype->tcpl_id = H5Pcopy(tcpl_id)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "failed to copy tcpl");
    if ((tapl_id != H5P_DATATYPE_ACCESS_DEFAULT) && (dtype->tapl_id = H5Pcopy(tapl_id)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "failed to copy tapl");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&dtype->obj, dtype->tcpl_id) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "failed to fill OCPL cache");

    ret_value = (void *)dtype;

done:
    /* Create metatask to use for dependencies on this datatype create */
    if (H5_daos_create_task(
            H5_daos_metatask_autocomplete, (finalize_ndeps > 0) ? (unsigned)finalize_ndeps : 0,
            (finalize_ndeps > 0) ? finalize_deps : NULL, NULL, NULL, NULL, &datatype_metatask) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create meta task for datatype create");
    /* Schedule datatype metatask (or save it to be scheduled later) */
    else {
        if (*first_task) {
            if (0 != (ret = tse_task_schedule(datatype_metatask, false)))
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't schedule datatype meta task: %s",
                             H5_daos_err_to_string(ret));
            else
                *dep_task = datatype_metatask;
        } /* end if */
        else {
            *first_task = datatype_metatask;
            *dep_task   = datatype_metatask;
        } /* end else */

        if (collective && (file->num_procs > 1))
            if (H5_daos_collective_error_check(&dtype->obj, req, first_task, dep_task) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't perform collective error check");
    } /* end else */

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if (NULL == ret_value) {
        /* Close datatype */
        if (dtype && H5_daos_datatype_close_real(dtype) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype");

        /* Free memory */
        if (update_cb_ud && update_cb_ud->md_rw_cb_ud.obj &&
            H5_daos_object_close(&update_cb_ud->md_rw_cb_ud.obj->item) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close object");
        update_cb_ud = DV_free(update_cb_ud);
    } /* end if */

    assert(!update_cb_ud);

    D_FUNC_LEAVE;
} /* end H5_daos_datatype_commit_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_open
 *
 * Purpose:     Opens a DAOS HDF5 datatype.
 *
 *              NOTE: not meant to be called internally.
 *
 * Return:      Success:        datatype object.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              April, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_datatype_open(void *_item, const H5VL_loc_params_t H5VL_DAOS_UNUSED *loc_params, const char *name,
                      hid_t tapl_id, hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_item_t  *item       = (H5_daos_item_t *)_item;
    H5_daos_dtype_t *dtype      = NULL;
    H5_daos_obj_t   *target_obj = NULL;
    daos_obj_id_t    oid        = {0, 0};
    daos_obj_id_t  **oid_ptr    = NULL;
    H5_daos_req_t   *int_req    = NULL;
    tse_task_t      *first_task = NULL;
    tse_task_t      *dep_task   = NULL;
    hbool_t          collective = FALSE;
    hbool_t          must_bcast = FALSE;
    char            *path_buf   = NULL;
    int              ret;
    void            *ret_value = NULL;

    H5_daos_inc_api_cnt();

    if (!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype parent object is NULL");
    if (!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(NULL);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    H5_DAOS_GET_METADATA_READ_MODE(item->file, tapl_id, H5P_DATATYPE_ACCESS_DEFAULT, collective, H5E_DATATYPE,
                                   NULL);

    /* Start H5 operation */
    if (NULL == (int_req = H5_daos_req_create(item->file, "committed datatype open", item->open_req, NULL,
                                              NULL, H5I_INVALID_HID)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, NULL, "can't create DAOS request");

#ifdef H5_DAOS_USE_TRANSACTIONS
    /* Start transaction */
    if (0 != (ret = daos_tx_open(item->file->coh, &int_req->th, DAOS_TF_RDONLY, NULL /*event*/)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't start transaction");
    int_req->th_open = TRUE;
#endif /* H5_DAOS_USE_TRANSACTIONS */

    /* Check for open by object token */
    if (H5VL_OBJECT_BY_TOKEN == loc_params->type) {
        /* Generate oid from token */
        if (H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't convert object token to OID");
    } /* end if */
    else {
        const char *target_name = NULL;
        size_t      target_name_len;

        /* Open using name parameter */
        if (H5VL_OBJECT_BY_SELF != loc_params->type)
            D_GOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL,
                         "unsupported datatype open location parameters type");
        if (!name)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype name is NULL");

        /* At this point we must broadcast on failure */
        if (collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Traverse the path */
        if (NULL == (target_obj = H5_daos_group_traverse(item, name, H5P_LINK_CREATE_DEFAULT, int_req,
                                                         collective, &path_buf, &target_name,
                                                         &target_name_len, &first_task, &dep_task)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADITER, NULL, "can't traverse path");

        /* Check for no target_name, in this case just return target_obj */
        if (target_name_len == 0) {
            /* Check type of target_obj */
            if (target_obj->item.type != H5I_DATATYPE)
                D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a datatype");

            /* Take ownership of target_obj */
            dtype      = (H5_daos_dtype_t *)target_obj;
            target_obj = NULL;

            /* No need to bcast since everyone just opened the already open
             * datatype */
            must_bcast = FALSE;

            D_GOTO_DONE(dtype);
        } /* end if */

        /* Check type of target_obj */
        if (target_obj->item.type != H5I_GROUP)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "target object is not a group");

        if (!collective || (item->file->my_rank == 0))
            /* Follow link to datatype */
            if (H5_daos_link_follow((H5_daos_group_t *)target_obj, target_name, target_name_len, FALSE,
                                    int_req, &oid_ptr, NULL, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_TRAVERSE, NULL, "can't follow link to datatype");
    } /* end else */

    must_bcast = FALSE;
    if (NULL == (dtype = H5_daos_datatype_open_helper(item->file, tapl_id, collective, int_req, &first_task,
                                                      &dep_task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype");

    /* Set datatype oid */
    if (oid_ptr)
        /* Retarget *oid_ptr to dtype->obj.oid so H5_daos_link_follow fills in
         * the datatype's oid */
        *oid_ptr = &dtype->obj.oid;
    else if (H5VL_OBJECT_BY_TOKEN == loc_params->type)
        /* Just set the static oid from the token */
        dtype->obj.oid = oid;
    else
        /* We will receive oid from lead process */
        assert(collective && item->file->my_rank > 0);

    /* Set return value */
    ret_value = (void *)dtype;

done:
    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Broadcast datatype info if needed */
        if (must_bcast && H5_daos_mpi_ibcast(NULL, &dtype->obj, H5_DAOS_TINFO_BCAST_BUF_SIZE, TRUE, NULL,
                                             item->file->my_rank == 0 ? H5_daos_datatype_open_bcast_comp_cb
                                                                      : H5_daos_datatype_open_recv_comp_cb,
                                             int_req, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL,
                         "failed to broadcast empty datatype info buffer to signal failure");

        /* Close datatype */
        if (dtype && H5_daos_datatype_close_real(dtype) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype");
    } /* end if */
    else
        assert(!must_bcast);

    if (int_req) {
        /* Free path_buf if necessary */
        if (path_buf && H5_daos_free_async(path_buf, &first_task, &dep_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTFREE, NULL, "can't free path buffer");

        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't schedule task to finalize H5 operation: %s",
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
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTOPERATE, NULL, "datatype open failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Close target object */
    if (target_obj && H5_daos_object_close(&target_obj->item) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close object");

    /* If we are not returning a datatype we must close it */
    if (ret_value == NULL && dtype && H5_daos_datatype_close_real(dtype) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype");

    D_FUNC_LEAVE_API;
} /* end H5_daos_datatype_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_open_helper
 *
 * Purpose:     Internal-use helper routine to create an asynchronous task
 *              for opening a DAOS HDF5 datatype.
 *
 * Return:      Success:        datatype object.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5_daos_dtype_t *
H5_daos_datatype_open_helper(H5_daos_file_t *file, hid_t tapl_id, hbool_t collective, H5_daos_req_t *req,
                             tse_task_t **first_task, tse_task_t **dep_task)
{
    H5_daos_mpi_ibcast_ud_flex_t *bcast_udata    = NULL;
    H5_daos_omd_fetch_ud_t       *fetch_udata    = NULL;
    H5_daos_dtype_t              *dtype          = NULL;
    size_t                        tinfo_buf_size = 0;
    int                           ret;
    H5_daos_dtype_t              *ret_value = NULL;

    assert(file);
    assert(req);
    assert(first_task);
    assert(dep_task);

    /* Allocate the datatype object that is returned to the user */
    if (NULL == (dtype = H5FL_CALLOC(H5_daos_dtype_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS datatype struct");
    dtype->obj.item.type     = H5I_DATATYPE;
    dtype->obj.item.open_req = req;
    req->rc++;
    dtype->obj.item.file = file;
    dtype->obj.item.rc   = 1;
    dtype->obj.obj_oh    = DAOS_HDL_INVAL;
    dtype->type_id       = H5I_INVALID_HID;
    dtype->tcpl_id       = H5P_DATATYPE_CREATE_DEFAULT;
    dtype->tapl_id       = H5P_DATATYPE_ACCESS_DEFAULT;
    if ((tapl_id != H5P_DATATYPE_ACCESS_DEFAULT) && (dtype->tapl_id = H5Pcopy(tapl_id)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "failed to copy tapl");

    /* Set up broadcast user data (if appropriate) and calculate initial datatype
     * info buffer size */
    if (collective && (file->num_procs > 1)) {
        if (NULL == (bcast_udata = (H5_daos_mpi_ibcast_ud_flex_t *)DV_malloc(
                         sizeof(H5_daos_mpi_ibcast_ud_flex_t) + H5_DAOS_TINFO_BCAST_BUF_SIZE)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "failed to allocate buffer for MPI broadcast user data");
        bcast_udata->bcast_udata.req        = req;
        bcast_udata->bcast_udata.obj        = &dtype->obj;
        bcast_udata->bcast_udata.buffer     = bcast_udata->flex_buf;
        bcast_udata->bcast_udata.buffer_len = H5_DAOS_TINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_udata.count      = H5_DAOS_TINFO_BCAST_BUF_SIZE;
        bcast_udata->bcast_udata.comm       = req->file->comm;

        tinfo_buf_size = H5_DAOS_TINFO_BCAST_BUF_SIZE;
    } /* end if */
    else
        tinfo_buf_size = H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_TCPL_BUF_SIZE;

    /* Check if we're actually opening the datatype or just receiving the datatype
     * info from the leader */
    if (!collective || (file->my_rank == 0)) {
        tse_task_t *fetch_task = NULL;
        uint8_t    *p;

        /* Open datatype object */
        if (H5_daos_obj_open(file, req, &dtype->obj.oid,
                             (file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO), &dtype->obj.obj_oh,
                             "datatype object open", first_task, dep_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype object");

        /* Allocate argument struct for fetch task */
        if (NULL == (fetch_udata = (H5_daos_omd_fetch_ud_t *)DV_calloc(sizeof(H5_daos_omd_fetch_ud_t) +
                                                                       (bcast_udata ? 0 : tinfo_buf_size))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL,
                         "can't allocate buffer for fetch callback arguments");

        /* Set up operation to read datatype and TCPL sizes from datatype */

        /* Set up ud struct */
        fetch_udata->md_rw_cb_ud.req = req;
        fetch_udata->md_rw_cb_ud.obj = &dtype->obj;
        fetch_udata->bcast_udata     = bcast_udata;

        /* Set up dkey.  Point to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.dkey, H5_daos_int_md_key_g,
                           H5_daos_int_md_key_size_g);
        fetch_udata->md_rw_cb_ud.free_dkey = FALSE;

        /* Set up iod.  Point akey to global name buffer, do not free. */
        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[0].iod_name, H5_daos_type_key_g,
                           H5_daos_type_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[0].iod_nr   = 1u;
        fetch_udata->md_rw_cb_ud.iod[0].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_const_iov_set((d_const_iov_t *)&fetch_udata->md_rw_cb_ud.iod[1].iod_name, H5_daos_cpl_key_g,
                           H5_daos_cpl_key_size_g);
        fetch_udata->md_rw_cb_ud.iod[1].iod_nr   = 1u;
        fetch_udata->md_rw_cb_ud.iod[1].iod_size = DAOS_REC_ANY;
        fetch_udata->md_rw_cb_ud.iod[1].iod_type = DAOS_IOD_SINGLE;

        /* Set up buffer */
        if (bcast_udata)
            p = bcast_udata->flex_buf + (4 * H5_DAOS_ENCODED_UINT64_T_SIZE);
        else
            p = fetch_udata->flex_buf;

        /* Set up sgl */
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[0], p, (daos_size_t)H5_DAOS_TYPE_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[0].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[0];
        fetch_udata->md_rw_cb_ud.free_sg_iov[0]   = FALSE;
        p += H5_DAOS_TYPE_BUF_SIZE;
        daos_iov_set(&fetch_udata->md_rw_cb_ud.sg_iov[1], p, (daos_size_t)H5_DAOS_TCPL_BUF_SIZE);
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr     = 1;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_nr_out = 0;
        fetch_udata->md_rw_cb_ud.sgl[1].sg_iovs   = &fetch_udata->md_rw_cb_ud.sg_iov[1];
        fetch_udata->md_rw_cb_ud.free_sg_iov[1]   = FALSE;
        p += H5_DAOS_TCPL_BUF_SIZE;

        /* Set conditional akey fetch for datatype metadata read operation */
        fetch_udata->md_rw_cb_ud.flags = DAOS_COND_AKEY_FETCH;

        /* Set nr */
        fetch_udata->md_rw_cb_ud.nr = 2;

        /* Set task name */
        fetch_udata->md_rw_cb_ud.task_name = "datatype metadata read";

        /* Create meta task for datatype metadata read.  This empty task will be
         * completed when the read is finished by H5_daos_tinfo_read_comp_cb.
         * We can't use fetch_task since it may not be completed by the first
         * fetch. */
        if (H5_daos_create_task(NULL, 0, NULL, NULL, NULL, NULL, &fetch_udata->fetch_metatask) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL,
                         "can't create meta task for datatype metadata read");

        /* Create task for datatype metadata read */
        assert(*dep_task);
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 1, dep_task, H5_daos_md_rw_prep_cb,
                                     H5_daos_tinfo_read_comp_cb, fetch_udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create task to read datatype metadata");

        /* Schedule meta task */
        if (0 != (ret = tse_task_schedule(fetch_udata->fetch_metatask, false)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL,
                         "can't schedule meta task for datatype metadata read: %s",
                         H5_daos_err_to_string(ret));

        /* Schedule datatype metadata read task (or save it to be scheduled
         * later) and give it a reference to req and the datatype */
        assert(*first_task);
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL,
                         "can't schedule task to read datatype metadata: %s", H5_daos_err_to_string(ret));
        *dep_task = fetch_udata->fetch_metatask;
        req->rc++;
        dtype->obj.item.rc++;
        fetch_udata = NULL;
    } /* end if */
    else
        assert(bcast_udata);

    ret_value = (void *)dtype;

done:
    /* Broadcast datatype info */
    if (bcast_udata) {
        assert(tinfo_buf_size == H5_DAOS_TINFO_BCAST_BUF_SIZE);
        if (H5_daos_mpi_ibcast(&bcast_udata->bcast_udata, &dtype->obj, tinfo_buf_size,
                               NULL == ret_value ? TRUE : FALSE, NULL,
                               file->my_rank == 0 ? H5_daos_datatype_open_bcast_comp_cb
                                                  : H5_daos_datatype_open_recv_comp_cb,
                               req, first_task, dep_task) < 0) {
            DV_free(bcast_udata);
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "failed to broadcast datatype info buffer");
        } /* end if */

        bcast_udata = NULL;
    } /* end if */

    /* Cleanup on failure */
    if (NULL == ret_value) {
        /* Close datatype */
        if (dtype && H5_daos_datatype_close_real(dtype) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype");

        /* Free memory */
        fetch_udata = DV_free(fetch_udata);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!fetch_udata);
    assert(!bcast_udata);

    D_FUNC_LEAVE;
} /* end H5_daos_datatype_open_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_open_bcast_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for datatype
 *              opens (rank 0).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_datatype_open_bcast_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for datatype info broadcast task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast datatype info";
    } /* end if */
    else if (task->dt_result == 0) {
        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank == 0);
        assert(udata->bcast_udata.obj->item.type == H5I_DATATYPE);

        /* Reissue bcast if necessary */
        if (udata->bcast_udata.buffer_len != udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.count == H5_DAOS_TINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.buffer_len > udata->bcast_udata.count);

            /* Use full buffer this time */
            udata->bcast_udata.count = udata->bcast_udata.buffer_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL,
                                    H5_daos_datatype_open_bcast_comp_cb, udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second datatype info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, ret,
                             "can't schedule task for second datatype info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close datatype */
        if (udata->bcast_udata.obj &&
            H5_daos_datatype_close_real((H5_daos_dtype_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close datatype");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast datatype info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

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
} /* end H5_daos_datatype_open_bcast_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_open_recv_comp_cb
 *
 * Purpose:     Complete callback for asynchronous MPI_ibcast for datatype
 *              opens (rank 1+).
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_datatype_open_recv_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_mpi_ibcast_ud_flex_t *udata;
    int                           ret;
    int                           ret_value = 0;

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for datatype info receive task");

    assert(udata->bcast_udata.req);

    /* Handle errors in bcast task.  Only record error in udata->bcast_udata.req_status if
     * it does not already contain an error (it could contain an error if
     * another task this task is not dependent on also failed). */
    if (task->dt_result < -H5_DAOS_PRE_ERROR && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
        udata->bcast_udata.req->status      = task->dt_result;
        udata->bcast_udata.req->failed_task = "MPI_Ibcast datatype info";
    } /* end if */
    else if (task->dt_result == 0) {
        uint64_t type_buf_len = 0;
        uint64_t tcpl_buf_len = 0;
        size_t   tinfo_len;
        uint8_t *p = udata->bcast_udata.buffer;

        assert(udata->bcast_udata.obj);
        assert(udata->bcast_udata.obj->item.file);
        assert(udata->bcast_udata.obj->item.file->my_rank > 0);
        assert(udata->bcast_udata.obj->item.type == H5I_DATATYPE);

        /* Decode oid */
        UINT64DECODE(p, udata->bcast_udata.obj->oid.lo)
        UINT64DECODE(p, udata->bcast_udata.obj->oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_buf_len)
        UINT64DECODE(p, tcpl_buf_len)

        /* Check for type_buf_len set to 0 - indicates failure */
        if (type_buf_len == 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_REMOTE_ERROR,
                         "lead process failed to open datatype");

        /* Calculate data length */
        tinfo_len =
            (size_t)type_buf_len + (size_t)tcpl_buf_len + H5_DAOS_ENCODED_OID_SIZE + 2 * sizeof(uint64_t);

        /* Reissue bcast if necessary */
        if (tinfo_len > (size_t)udata->bcast_udata.count) {
            tse_task_t *bcast_task;

            assert(udata->bcast_udata.buffer_len == H5_DAOS_TINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.count == H5_DAOS_TINFO_BCAST_BUF_SIZE);
            assert(udata->bcast_udata.buffer == udata->flex_buf);

            /* Realloc buffer */
            if (NULL == (udata->bcast_udata.buffer = DV_malloc(tinfo_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                             "failed to allocate memory for datatype info buffer");
            udata->bcast_udata.buffer_len = (int)tinfo_len;
            udata->bcast_udata.count      = (int)tinfo_len;

            /* Create task for second bcast */
            if (H5_daos_create_task(H5_daos_mpi_ibcast_task, 0, NULL, NULL,
                                    H5_daos_datatype_open_recv_comp_cb, udata, &bcast_task) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                             "can't create task for second datatype info broadcast");

            /* Schedule second bcast and transfer ownership of udata */
            if (0 != (ret = tse_task_schedule(bcast_task, false)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, ret,
                             "can't schedule task for second datatype info broadcast: %s",
                             H5_daos_err_to_string(ret));
            udata = NULL;
        } /* end if */
        else {
            /* Open datatype */
            if (0 != (ret = daos_obj_open(
                          udata->bcast_udata.obj->item.file->coh, udata->bcast_udata.obj->oid,
                          (udata->bcast_udata.obj->item.file->flags & H5F_ACC_RDWR ? DAOS_OO_RW : DAOS_OO_RO),
                          &udata->bcast_udata.obj->obj_oh, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, ret, "can't open datatype: %s",
                             H5_daos_err_to_string(ret));

            /* Finish building datatype object */
            if (0 !=
                (ret = H5_daos_datatype_open_end((H5_daos_dtype_t *)udata->bcast_udata.obj, p, type_buf_len,
                                                 tcpl_buf_len, udata->bcast_udata.req->dxpl_id)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, ret, "can't finish opening datatype");
        } /* end else */
    }     /* end else */

done:
    /* Free private data if we haven't released ownership */
    if (udata) {
        /* Close datatype */
        if (udata->bcast_udata.obj &&
            H5_daos_datatype_close_real((H5_daos_dtype_t *)udata->bcast_udata.obj) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close datatype");

        /* Handle errors in this function */
        /* Do not place any code that can issue errors after this block, except
         * for H5_daos_req_free_int, which updates req->status if it sees an
         * error */
        if (ret_value < -H5_DAOS_SHORT_CIRCUIT && udata->bcast_udata.req->status >= -H5_DAOS_SHORT_CIRCUIT) {
            udata->bcast_udata.req->status      = ret_value;
            udata->bcast_udata.req->failed_task = "MPI_Ibcast datatype info completion callback";
        } /* end if */

        /* Release our reference to req */
        if (H5_daos_req_free_int(udata->bcast_udata.req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->bcast_udata.bcast_metatask) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

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
} /* end H5_daos_datatype_open_recv_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_open_end
 *
 * Purpose:     Decode serialized datatype info from a buffer and fill
 *              caches.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_datatype_open_end(H5_daos_dtype_t *dtype, uint8_t *p, uint64_t type_buf_len, uint64_t tcpl_buf_len,
                          hid_t H5VL_DAOS_UNUSED dxpl_id)
{
    int ret_value = 0;

    assert(dtype);
    assert(p);
    assert(type_buf_len > 0);

    /* Decode datatype */
    if ((dtype->type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR, "can't deserialize datatype");
    p += type_buf_len;

    /* Check if the datatype's TCPL is the default TCPL.
     * Otherwise, decode the datatype's TCPL.
     */
    if ((tcpl_buf_len == dtype->obj.item.file->def_plist_cache.tcpl_size) &&
        !memcmp(p, dtype->obj.item.file->def_plist_cache.tcpl_buf,
                dtype->obj.item.file->def_plist_cache.tcpl_size))
        dtype->tcpl_id = H5P_DATATYPE_CREATE_DEFAULT;
    else if ((dtype->tcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, -H5_DAOS_H5_DECODE_ERROR,
                     "can't deserialize datatype creation property list");

    /* Fill OCPL cache */
    if (H5_daos_fill_ocpl_cache(&dtype->obj, dtype->tcpl_id) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_CPL_CACHE_ERROR, "failed to fill OCPL cache");

done:
    D_FUNC_LEAVE;
} /* end H5_daos_datatype_open_end() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_tinfo_read_comp_cb
 *
 * Purpose:     Complete callback for asynchronous metadata fetch for
 *              datatype opens.
 *
 * Return:      Success:        0
 *              Failure:        Error code
 *
 *-------------------------------------------------------------------------
 */
static int
H5_daos_tinfo_read_comp_cb(tse_task_t *task, void H5VL_DAOS_UNUSED *args)
{
    H5_daos_omd_fetch_ud_t *udata;
    uint8_t                *p;
    int                     ret;
    int                     ret_value = 0;

    assert(H5_daos_task_list_g);

    /* Get private data */
    if (NULL == (udata = tse_task_get_priv(task)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_DAOS_GET_ERROR,
                     "can't get private data for datatype info read task");

    assert(udata->md_rw_cb_ud.req);
    assert(udata->fetch_metatask);

    /* Check for buffer not large enough */
    if (task->dt_result == -DER_REC2BIG) {
        tse_task_t *fetch_task;
        size_t      daos_info_len = udata->md_rw_cb_ud.iod[0].iod_size + udata->md_rw_cb_ud.iod[1].iod_size;

        assert(udata->md_rw_cb_ud.req->file);
        assert(udata->md_rw_cb_ud.obj);
        assert(udata->md_rw_cb_ud.obj->item.type == H5I_DATATYPE);

        /* Verify iod size makes sense */
        if (udata->md_rw_cb_ud.sg_iov[0].iov_buf_len != H5_DAOS_TYPE_BUF_SIZE ||
            udata->md_rw_cb_ud.sg_iov[1].iov_buf_len != H5_DAOS_TCPL_BUF_SIZE)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, -H5_DAOS_BAD_VALUE,
                         "buffer length does not match expected value");

        if (udata->bcast_udata) {
            assert(udata->bcast_udata->bcast_udata.buffer == udata->bcast_udata->flex_buf);

            /* Reallocate datatype info buffer if necessary */
            if (daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_TCPL_BUF_SIZE) {
                if (NULL ==
                    (udata->bcast_udata->bcast_udata.buffer = DV_malloc(
                         daos_info_len + H5_DAOS_ENCODED_OID_SIZE + 2 * H5_DAOS_ENCODED_UINT64_T_SIZE)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized datatype info");
                udata->bcast_udata->bcast_udata.buffer_len =
                    (int)(daos_info_len + H5_DAOS_ENCODED_OID_SIZE + 2 * H5_DAOS_ENCODED_UINT64_T_SIZE);
            } /* end if */

            /* Set starting point for fetch sg_iovs */
            p = (uint8_t *)udata->bcast_udata->bcast_udata.buffer + H5_DAOS_ENCODED_OID_SIZE +
                2 * H5_DAOS_ENCODED_UINT64_T_SIZE;
        } /* end if */
        else {
            assert(udata->md_rw_cb_ud.sg_iov[0].iov_buf == udata->flex_buf);

            /* Reallocate datatype info buffer if necessary */
            if (daos_info_len > H5_DAOS_TYPE_BUF_SIZE + H5_DAOS_TCPL_BUF_SIZE) {
                if (NULL == (udata->md_rw_cb_ud.sg_iov[0].iov_buf = DV_malloc(daos_info_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, -H5_DAOS_ALLOC_ERROR,
                                 "can't allocate buffer for serialized datatype info");
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

        /* Create task for reissued datatype metadata read */
        if (H5_daos_create_daos_task(DAOS_OPC_OBJ_FETCH, 0, NULL, H5_daos_md_rw_prep_cb,
                                     H5_daos_tinfo_read_comp_cb, udata, &fetch_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, -H5_DAOS_SETUP_ERROR,
                         "can't create task to read datatype metadata");

        /* Schedule datatype metadata read task and give it a reference to req
         * and the datatype */
        if (0 != (ret = tse_task_schedule(fetch_task, false)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, ret, "can't schedule task to read datatype metadata: %s",
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
            uint64_t type_buf_len = (uint64_t)((char *)udata->md_rw_cb_ud.sg_iov[1].iov_buf -
                                               (char *)udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            uint64_t tcpl_buf_len = (uint64_t)(udata->md_rw_cb_ud.iod[1].iod_size);

            /* Check for missing metadata */
            if (udata->md_rw_cb_ud.iod[0].iod_size == (uint64_t)0 ||
                udata->md_rw_cb_ud.iod[1].iod_size == (uint64_t)0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, -H5_DAOS_DAOS_GET_ERROR,
                             "internal metadata not found");

            if (udata->bcast_udata) {
                /* Encode oid */
                p = udata->bcast_udata->bcast_udata.buffer;
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.lo)
                UINT64ENCODE(p, udata->md_rw_cb_ud.obj->oid.hi)

                /* Encode serialized info lengths */
                UINT64ENCODE(p, type_buf_len)
                UINT64ENCODE(p, tcpl_buf_len)
                assert(p == udata->md_rw_cb_ud.sg_iov[0].iov_buf);
            } /* end if */

            /* Finish building datatype object */
            if (0 != (ret = H5_daos_datatype_open_end((H5_daos_dtype_t *)udata->md_rw_cb_ud.obj,
                                                      udata->md_rw_cb_ud.sg_iov[0].iov_buf, type_buf_len,
                                                      tcpl_buf_len, udata->md_rw_cb_ud.req->dxpl_id)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, ret, "can't finish opening datatype");
        } /* end else */
    }     /* end else */

done:
    /* Return task to task list */
    if (H5_daos_task_list_put(H5_daos_task_list_g, task) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                     "can't return task to task list");

    /* Clean up if this is the last fetch task */
    if (udata) {
        /* Close datatype */
        if (udata->md_rw_cb_ud.obj &&
            H5_daos_datatype_close_real((H5_daos_dtype_t *)udata->md_rw_cb_ud.obj) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_H5_CLOSE_ERROR, "can't close datatype");

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
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_FREE_ERROR, "can't free request");

        /* Return task to task list */
        if (H5_daos_task_list_put(H5_daos_task_list_g, udata->fetch_metatask) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, -H5_DAOS_TASK_LIST_ERROR,
                         "can't return task to task list");

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
} /* end H5_daos_tinfo_read_comp_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_get
 *
 * Purpose:     Gets certain information about a datatype
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
H5_daos_datatype_get(void *_dtype, H5VL_datatype_get_args_t *get_args, hid_t H5VL_DAOS_UNUSED dxpl_id,
                     void H5VL_DAOS_UNUSED **req)
{
    H5_daos_dtype_t *dtype     = (H5_daos_dtype_t *)_dtype;
    herr_t           ret_value = SUCCEED; /* Return value */

    H5_daos_inc_api_cnt();

    if (!_dtype)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (!get_args)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid operation arguments");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (get_args->op_type) {
        case H5VL_DATATYPE_GET_BINARY_SIZE: {
            size_t *binary_size = get_args->args.get_binary_size.size;

            /* Wait for the datatype to open if necessary */
            if (!dtype->obj.item.created && dtype->obj.item.open_req->status != 0) {
                if (H5_daos_progress(dtype->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dtype->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            if (H5Tencode(dtype->type_id, NULL, binary_size) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL,
                             "can't determine serialized length of datatype");

            break;
        } /* end block */
        case H5VL_DATATYPE_GET_BINARY: {
            void  *buf  = get_args->args.get_binary.buf;
            size_t size = get_args->args.get_binary.buf_size;

            /* Wait for the datatype to open if necessary */
            if (!dtype->obj.item.created && dtype->obj.item.open_req->status != 0) {
                if (H5_daos_progress(dtype->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dtype->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            if (H5Tencode(dtype->type_id, buf, &size) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL,
                             "can't determine serialized length of datatype");

            break;
        } /* end block */
        case H5VL_DATATYPE_GET_TCPL: {
            hid_t *plist_id = &get_args->args.get_tcpl.tcpl_id;

            /* Wait for the datatype to open if necessary */
            if (!dtype->obj.item.created && dtype->obj.item.open_req->status != 0) {
                if (H5_daos_progress(dtype->obj.item.open_req, H5_DAOS_PROGRESS_WAIT) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");
                if (dtype->obj.item.open_req->status != 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "group open failed");
            } /* end if */

            /* Retrieve the datatype's creation property list */
            if ((*plist_id = H5Pcopy(dtype->tcpl_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get dtype creation property list");

            /* Set datatype's object class on tcpl */
            if (H5_daos_set_oclass_from_oid(*plist_id, dtype->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property");

            break;
        } /* end block */
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from datatype");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_datatype_get() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_specific
 *
 * Purpose:     Performs a datatype "specific" operation
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
H5_daos_datatype_specific(void *_item, H5VL_datatype_specific_args_t *specific_args,
                          hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_dtype_t *dtype      = (H5_daos_dtype_t *)_item;
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
    if (H5I_DATATYPE != dtype->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a datatype");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    switch (specific_args->op_type) {
        case H5VL_DATATYPE_FLUSH: {
            /* Start H5 operation */
            if (NULL == (int_req = H5_daos_req_create(dtype->obj.item.file, "committed datatype flush",
                                                      dtype->obj.item.open_req, NULL, NULL, H5I_INVALID_HID)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

            if (H5_daos_datatype_flush(dtype, int_req, &first_task, &dep_task) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_WRITEERROR, FAIL, "can't flush datatype");

            break;
        } /* H5VL_DATATYPE_FLUSH */

        case H5VL_DATATYPE_REFRESH: {
            if (H5_daos_datatype_refresh(dtype, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_READERROR, FAIL, "failed to refresh datatype");

            break;
        } /* H5VL_DATATYPE_REFRESH */
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL,
                         "invalid or unsupported datatype specific operation");
    } /* end switch */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
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
        assert(specific_args->op_type == H5VL_DATATYPE_FLUSH);
        if (H5_daos_req_enqueue(int_req, first_task, &dtype->obj.item, H5_DAOS_OP_TYPE_WRITE_ORDERED,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't add request to request queue");

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTOPERATE, FAIL,
                             "dataset specific operation failed in task \"%s\": %s", int_req->failed_task,
                             H5_daos_err_to_string(int_req->status));

            /* Close internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* else */
    }     /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_datatype_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_close_real
 *
 * Purpose:     Internal version of H5_daos_datatype_close()
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
H5_daos_datatype_close_real(H5_daos_dtype_t *dtype)
{
    int    ret;
    herr_t ret_value = SUCCEED;

    if (!dtype)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype object is NULL");
    if (H5I_DATATYPE != dtype->obj.item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a datatype");

    if (--dtype->obj.item.rc == 0) {
        /* Free datatype data structures */
        if (dtype->obj.item.cur_op_pool)
            H5_daos_op_pool_free(dtype->obj.item.cur_op_pool);
        if (dtype->obj.item.open_req)
            if (H5_daos_req_free_int(dtype->obj.item.open_req) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't free request");
        if (!daos_handle_is_inval(dtype->obj.obj_oh))
            if (0 != (ret = daos_obj_close(dtype->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "can't close datatype DAOS object: %s",
                             H5_daos_err_to_string(ret));
        if (dtype->type_id != H5I_INVALID_HID && H5Idec_ref(dtype->type_id) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close datatype");
        if (dtype->tcpl_id != H5I_INVALID_HID && dtype->tcpl_id != H5P_DATATYPE_CREATE_DEFAULT)
            if (H5Idec_ref(dtype->tcpl_id) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close tcpl");
        if (dtype->tapl_id != H5I_INVALID_HID && dtype->tapl_id != H5P_DATATYPE_ACCESS_DEFAULT)
            if (H5Idec_ref(dtype->tapl_id) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close tapl");
        dtype = H5FL_FREE(H5_daos_dtype_t, dtype);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_datatype_close_real() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_close
 *
 * Purpose:     Closes a DAOS HDF5 datatype.
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
H5_daos_datatype_close(void *_dtype, hid_t H5VL_DAOS_UNUSED dxpl_id, void **req)
{
    H5_daos_dtype_t             *dtype      = (H5_daos_dtype_t *)_dtype;
    H5_daos_obj_close_task_ud_t *task_ud    = NULL;
    tse_task_t                  *first_task = NULL;
    tse_task_t                  *dep_task   = NULL;
    H5_daos_req_t               *int_req    = NULL;
    int                          ret;
    herr_t                       ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (!_dtype)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(FAIL);

    /* Check if the datatype's request queue is empty, if so we can close it
     * immediately.  Also close if the pool is empty and has no start task (and
     * hence does not depend on anything).  Also close if it is marked to close
     * nonblocking. */
    if (((dtype->obj.item.open_req->status == 0 || dtype->obj.item.open_req->status < -H5_DAOS_CANCELED) &&
         (!dtype->obj.item.cur_op_pool || (dtype->obj.item.cur_op_pool->type == H5_DAOS_OP_TYPE_EMPTY &&
                                           !dtype->obj.item.cur_op_pool->start_task))) ||
        dtype->obj.item.nonblocking_close) {
        if (H5_daos_datatype_close_real(dtype) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype");
    } /* end if */
    else {
        tse_task_t *close_task = NULL;

        /* Start H5 operation. Currently, the DXPL is only copied when datatype conversion is needed. */
        if (NULL ==
            (int_req = H5_daos_req_create(dtype->obj.item.file, "committed datatype close",
                                          dtype->obj.item.open_req, NULL, NULL, H5P_DATASET_XFER_DEFAULT)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't create DAOS request");

        /* Allocate argument struct */
        if (NULL == (task_ud = (H5_daos_obj_close_task_ud_t *)DV_calloc(sizeof(H5_daos_obj_close_task_ud_t))))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL,
                         "can't allocate space for close task udata struct");
        task_ud->req  = int_req;
        task_ud->item = &dtype->obj.item;

        /* Create task to close datatype */
        if (H5_daos_create_task(H5_daos_object_close_task, 0, NULL, NULL, NULL, task_ud, &close_task) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't create task to close datatype");

        /* Save task to be scheduled later and give it a reference to req and
         * dtype */
        assert(!first_task);
        first_task = close_task;
        dep_task   = close_task;
        /* No need to take a reference to dtype here since the purpose is to
         * release the API's reference */
        int_req->rc++;
        task_ud = NULL;
    } /* end else */

done:
    if (int_req) {
        /* Create task to finalize H5 operation */
        if (H5_daos_create_task(H5_daos_h5op_finalize, dep_task ? 1 : 0, dep_task ? &dep_task : NULL, NULL,
                                NULL, int_req, &int_req->finalize_task) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't create task to finalize H5 operation");
        /* Schedule finalize task */
        else if (0 != (ret = tse_task_schedule(int_req->finalize_task, false)))
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't schedule task to finalize H5 operation: %s",
                         H5_daos_err_to_string(ret));
        else
            /* finalize_task now owns a reference to req */
            int_req->rc++;

        /* If there was an error during setup, pass it to the request */
        if (ret_value < 0)
            int_req->status = -H5_DAOS_SETUP_ERROR;

        /* Add the request to the object's request queue.  This will add the
         * dependency on the datatype open if necessary. */
        if (H5_daos_req_enqueue(int_req, first_task, &dtype->obj.item, H5_DAOS_OP_TYPE_CLOSE,
                                H5_DAOS_OP_SCOPE_OBJ, FALSE, !req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't add request to request queue");
        dtype = NULL;

        /* Check for external async */
        if (req) {
            /* Return int_req as req */
            *req = int_req;

            /* Kick task engine */
            if (H5_daos_progress(NULL, H5_DAOS_PROGRESS_KICK) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");
        } /* end if */
        else {
            /* Block until operation completes */
            if (H5_daos_progress(int_req, H5_DAOS_PROGRESS_WAIT) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't progress scheduler");

            /* Check for failure */
            if (int_req->status < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTOPERATE, FAIL, "datatype close failed in task \"%s\": %s",
                             int_req->failed_task, H5_daos_err_to_string(int_req->status));

            /* Release our reference to the internal request */
            if (H5_daos_req_free_int(int_req) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't free request");
        } /* end else */
    }     /* end if */

    /* Cleanup on error */
    if (task_ud) {
        assert(ret_value < 0);
        task_ud = DV_free(task_ud);
    } /* end if */

    D_FUNC_LEAVE_API;
} /* end H5_daos_datatype_close() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_flush
 *
 * Purpose:     Flushes a DAOS committed datatype. Currently a no-op, may
 *              create a snapshot in the future.
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
H5_daos_datatype_flush(H5_daos_dtype_t H5VL_DAOS_UNUSED *dtype, H5_daos_req_t H5VL_DAOS_UNUSED *req,
                       tse_task_t **first_task, tse_task_t **dep_task)
{
    tse_task_t *barrier_task = NULL;
    herr_t      ret_value    = SUCCEED;

    assert(dtype);

    /* Create task that does nothing but complete itself.  Only necessary
     * because we can't enqueue a request that has no tasks */
    if (H5_daos_create_task(H5_daos_metatask_autocomplete, 0, NULL, NULL, NULL, NULL, &barrier_task) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't create barrier task for datatype flush");

    /* Schedule barrier task (or save it to be scheduled later)  */
    assert(!*first_task);
    *first_task = barrier_task;
    *dep_task   = barrier_task;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_datatype_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_refresh
 *
 * Purpose:     Refreshes a DAOS committed datatype (currently a no-op)
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
H5_daos_datatype_refresh(H5_daos_dtype_t H5VL_DAOS_UNUSED *dtype, hid_t H5VL_DAOS_UNUSED dxpl_id,
                         void H5VL_DAOS_UNUSED **req)
{
    herr_t ret_value = SUCCEED;

    assert(dtype);

    D_GOTO_DONE(SUCCEED);

done:
    D_FUNC_LEAVE;
} /* end H5_daos_datatype_refresh() */
