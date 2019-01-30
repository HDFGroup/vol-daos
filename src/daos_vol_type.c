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
 * library.  Datatype routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */
#include "daos_vol_config.h"    /* DAOS connector configuration header     */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Prototypes */
static htri_t H5_daos_need_bkg(hid_t src_type_id, hid_t dst_type_id,
    size_t *dst_type_size, hbool_t *fill_bkg);


/*-------------------------------------------------------------------------
 * Function:    H5_daos_need_bkg
 *
 * Purpose:     Determine if a background buffer is needed for conversion.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5_daos_need_bkg(hid_t src_type_id, hid_t dst_type_id, size_t *dst_type_size,
    hbool_t *fill_bkg)
{
    hid_t memb_type_id = -1;
    hid_t src_memb_type_id = -1;
    char *memb_name = NULL;
    size_t memb_size;
    H5T_class_t tclass;
    htri_t ret_value = FALSE;

    assert(dst_type_size);
    assert(fill_bkg);

    /* Get destination type size */
    if((*dst_type_size = H5Tget_size(dst_type_id)) == 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get source type size")

    /* Get datatype class */
    if(H5T_NO_CLASS == (tclass = H5Tget_class(dst_type_id)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get type class")

    switch(tclass) {
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

        case H5T_COMPOUND:
            {
                int nmemb;
                size_t size_used = 0;
                int src_i;
                int i;

                /* We must always provide a background buffer for compound
                 * conversions.  Only need to check further to see if it must be
                 * filled. */
                ret_value = TRUE;

                /* Get number of compound members */
                if((nmemb = H5Tget_nmembers(dst_type_id)) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get number of destination compound members")

                /* Iterate over compound members, checking for a member in
                 * dst_type_id with no match in src_type_id */
                for(i = 0; i < nmemb; i++) {
                    /* Get member type */
                    if((memb_type_id = H5Tget_member_type(dst_type_id, (unsigned)i)) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type")

                    /* Get member name */
                    if(NULL == (memb_name = H5Tget_member_name(dst_type_id, (unsigned)i)))
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member name")

                    /* Check for matching name in source type */
                    H5E_BEGIN_TRY {
                        src_i = H5Tget_member_index(src_type_id, memb_name);
                    } H5E_END_TRY

                    /* Free memb_name */
                    if(H5free_memory(memb_name) < 0)
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't free member name")
                    memb_name = NULL;

                    /* If no match was found, this type is not being filled in,
                     * so we must fill the background buffer */
                    if(src_i < 0) {
                        if(H5Tclose(memb_type_id) < 0)
                            D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type")
                        memb_type_id = -1;
                        *fill_bkg = TRUE;
                        D_GOTO_DONE(TRUE)
                    } /* end if */

                    /* Open matching source type */
                    if((src_memb_type_id = H5Tget_member_type(src_type_id, (unsigned)src_i)) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type")

                    /* Recursively check member type, this will fill in the
                     * member size */
                    if(H5_daos_need_bkg(src_memb_type_id, memb_type_id, &memb_size, fill_bkg) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed")

                    /* Close source member type */
                    if(H5Tclose(src_memb_type_id) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type")
                    src_memb_type_id = -1;

                    /* Close member type */
                    if(H5Tclose(memb_type_id) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type")
                    memb_type_id = -1;

                    /* If the source member type needs the background filled, so
                     * does the parent */
                    if(*fill_bkg)
                        D_GOTO_DONE(TRUE)

                    /* Keep track of the size used in compound */
                    size_used += memb_size;
                } /* end for */

                /* Check if all the space in the type is used.  If not, we must
                 * fill the background buffer. */
                /* TODO: This is only necessary on read, we don't care about
                 * compound gaps in the "file" DSINC */
                assert(size_used <= *dst_type_size);
                if(size_used != *dst_type_size)
                    *fill_bkg = TRUE;

                break;
            } /* end block */

        case H5T_ARRAY:
            /* Get parent type */
            if((memb_type_id = H5Tget_super(dst_type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type")

            /* Get source parent type */
            if((src_memb_type_id = H5Tget_super(src_type_id)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type")

            /* Recursively check parent type */
            if((ret_value = H5_daos_need_bkg(src_memb_type_id, memb_type_id, &memb_size, fill_bkg)) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed")

            /* Close source parent type */
            if(H5Tclose(src_memb_type_id) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type")
            src_memb_type_id = -1;

            /* Close parent type */
            if(H5Tclose(memb_type_id) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type")
            memb_type_id = -1;

            break;

        case H5T_REFERENCE:
        case H5T_VLEN:
            /* Not yet supported */
            D_GOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "reference and vlen types not supported")

            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            D_GOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid type class")
    } /* end switch */

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        if(memb_type_id >= 0)
            if(H5Idec_ref(memb_type_id) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close member type")
        if(src_memb_type_id >= 0)
            if(H5Idec_ref(src_memb_type_id) < 0)
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close source member type")
        memb_name = (char *)DV_free(memb_name);
    } /* end if */

    D_FUNC_LEAVE
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
H5_daos_tconv_init(hid_t src_type_id, size_t *src_type_size,
    hid_t dst_type_id, size_t *dst_type_size, size_t num_elem, void **tconv_buf,
    void **bkg_buf, H5_daos_tconv_reuse_t *reuse, hbool_t *fill_bkg)
{
    htri_t need_bkg;
    htri_t types_equal;
    herr_t ret_value = SUCCEED;

    assert(src_type_size);
    assert(dst_type_size);
    assert(tconv_buf);
    assert(!*tconv_buf);
    assert(bkg_buf);
    assert(!*bkg_buf);
    assert(fill_bkg);
    assert(!*fill_bkg);

    /* Get source type size */
    if((*src_type_size = H5Tget_size(src_type_id)) == 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get source type size")

    /* Check if the types are equal */
    if((types_equal = H5Tequal(src_type_id, dst_type_id)) < 0)
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOMPARE, FAIL, "can't check if types are equal")
    if(types_equal)
        /* Types are equal, no need for conversion, just set dst_type_size */
        *dst_type_size = *src_type_size;
    else {
        /* Check if we need a background buffer */
        if((need_bkg = H5_daos_need_bkg(src_type_id, dst_type_id, dst_type_size, fill_bkg)) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed")

        /* Check for reusable destination buffer */
        if(reuse) {
            assert(*reuse == H5_DAOS_TCONV_REUSE_NONE);

            /* Use dest buffer for type conversion if it large enough, otherwise
             * use it for the background buffer if one is needed. */
            if(dst_type_size >= src_type_size)
                *reuse = H5_DAOS_TCONV_REUSE_TCONV;
            else if(need_bkg)
                *reuse = H5_DAOS_TCONV_REUSE_BKG;
        } /* end if */

        /* Allocate conversion buffer if it is not being reused */
        if(!reuse || (*reuse != H5_DAOS_TCONV_REUSE_TCONV))
            if(NULL == (*tconv_buf = DV_malloc(num_elem * (*src_type_size
                    > *dst_type_size ? *src_type_size : *dst_type_size))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate type conversion buffer")

        /* Allocate background buffer if one is needed and it is not being
         * reused */
        if(need_bkg && (!reuse || (*reuse != H5_DAOS_TCONV_REUSE_BKG)))
            if(NULL == (*bkg_buf = DV_calloc(num_elem * *dst_type_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate background buffer")
    } /* end else */

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        *tconv_buf = DV_free(*tconv_buf);
        *bkg_buf = DV_free(*bkg_buf);
        if(reuse)
            *reuse = H5_DAOS_TCONV_REUSE_NONE;
    } /* end if */

    D_FUNC_LEAVE
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
H5_daos_datatype_commit(void *_item,
    const H5VL_loc_params_t DV_ATTR_UNUSED *loc_params, const char *name,
    hid_t type_id, hid_t DV_ATTR_UNUSED lcpl_id, hid_t tcpl_id, hid_t tapl_id,
    hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_dtype_t *dtype = NULL;
    H5_daos_group_t *target_grp = NULL;
    void *type_buf = NULL;
    void *tcpl_buf = NULL;
    hbool_t collective;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype name is NULL")

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")

    /* Check for collective access, if not already set by the file */
    collective = item->file->collective;
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(tapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, NULL, "can't get collective access property")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dtype = H5FL_CALLOC(H5_daos_dtype_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    dtype->obj.item.type = H5I_DATATYPE;
    dtype->obj.item.open_req = NULL;
    dtype->obj.item.file = item->file;
    dtype->obj.item.rc = 1;
    dtype->obj.obj_oh = DAOS_HDL_INVAL;
    dtype->type_id = FAIL;
    dtype->tcpl_id = FAIL;
    dtype->tapl_id = FAIL;

    /* Generate datatype oid */
    H5_daos_oid_encode(&dtype->obj.oid, item->file->max_oid + (uint64_t)1, H5I_DATATYPE);

    /* Create datatype and write metadata if this process should */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        H5_daos_link_val_t link_val;
        daos_key_t dkey;
        daos_iod_t iod[2];
        daos_sg_list_t sgl[2];
        daos_iov_t sg_iov[2];
        size_t type_size = 0;
        size_t tcpl_size = 0;

        /* Traverse the path */
        if(name)
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_BADITER, NULL, "can't traverse path")

        /* Create datatype */
        /* Update max_oid */
        item->file->max_oid = H5_daos_oid_to_idx(dtype->obj.oid);

        /* Write max OID */
        if(H5_daos_write_max_oid(item->file) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't write max OID")

        /* Open datatype */
        if(0 != (ret = daos_obj_open(item->file->coh, dtype->obj.oid, DAOS_OO_RW, &dtype->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype: %s", H5_daos_err_to_string(ret))

        /* Encode datatype */
        if(H5Tencode(type_id, NULL, &type_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
        if(NULL == (type_buf = DV_malloc(type_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
        if(H5Tencode(type_id, type_buf, &type_size) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, NULL, "can't serialize datatype")

        /* Encode TCPL */
        if(H5Pencode(tcpl_id, NULL, &tcpl_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of tcpl")
        if(NULL == (tcpl_buf = DV_malloc(tcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized tcpl")
        if(H5Pencode(tcpl_id, tcpl_buf, &tcpl_size) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, NULL, "can't serialize tcpl")

        /* Set up operation to write datatype and TCPL to datatype */
        /* Set up dkey */
        daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, H5_daos_type_key_g, H5_daos_type_key_size_g);
        daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = (uint64_t)type_size;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = (uint64_t)tcpl_size;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov[0], type_buf, (daos_size_t)type_size);
        sgl[0].sg_nr = 1;
        sgl[0].sg_nr_out = 0;
        sgl[0].sg_iovs = &sg_iov[0];
        daos_iov_set(&sg_iov[1], tcpl_buf, (daos_size_t)tcpl_size);
        sgl[1].sg_nr = 1;
        sgl[1].sg_nr_out = 0;
        sgl[1].sg_iovs = &sg_iov[1];

        /* Write internal metadata to datatype */
        if(0 != (ret = daos_obj_update(dtype->obj.obj_oh, DAOS_TX_NONE, &dkey, 2, iod, sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't write metadata to datatype: %s", H5_daos_err_to_string(ret))

        /* Create link to datatype */
        if(name) {
            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = dtype->obj.oid;
            if(H5_daos_link_write(target_grp, target_name, strlen(target_name), &link_val) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't create link to datatype")
        } /* end if */
    } /* end if */
    else {
        /* Update max_oid */
        item->file->max_oid = dtype->obj.oid.lo;

        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast though it
         * could only be an issue with datatype reopen so we'll skip it for now.
         * There is probably never an issue with file reopen since all commits
         * are from process 0, same as the datatype create above. */

        /* Open datatype */
        if(0 != (ret = daos_obj_open(item->file->coh, dtype->obj.oid, DAOS_OO_RW, &dtype->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype: %s", H5_daos_err_to_string(ret))
    } /* end else */

    /* Finish setting up datatype struct */
    if((dtype->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((dtype->tcpl_id = H5Pcopy(tcpl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy tcpl")
    if((dtype->tapl_id = H5Pcopy(tapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy tapl")

    /* Set return value */
    ret_value = (void *)dtype;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close group")

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close dataset */
        if(dtype && H5_daos_datatype_close(dtype, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype")

    /* Free memory */
    type_buf = DV_free(type_buf);
    tcpl_buf = DV_free(tcpl_buf);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_datatype_commit() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_open
 *
 * Purpose:     Opens a DAOS HDF5 datatype.
 *
 * Return:      Success:        datatype ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              April, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_datatype_open(void *_item,
    const H5VL_loc_params_t DV_ATTR_UNUSED *loc_params, const char *name,
    hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_dtype_t *dtype = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_key_t dkey;
    daos_iod_t iod[2];
    daos_sg_list_t sgl[2];
    daos_iov_t sg_iov[2];
    uint64_t type_len = 0;
    uint64_t tcpl_len = 0;
    uint64_t tot_len;
    uint8_t tinfo_buf_static[H5_DAOS_TINFO_BUF_SIZE];
    uint8_t *tinfo_buf_dyn = NULL;
    uint8_t *tinfo_buf = tinfo_buf_static;
    uint8_t *p;
    hbool_t collective;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "datatype name is NULL")

    /* Check for collective access, if not already set by the file */
    collective = item->file->collective;
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(tapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, NULL, "can't get collective access property")

    /* Allocate the datatype object that is returned to the user */
    if(NULL == (dtype = H5FL_CALLOC(H5_daos_dtype_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS datatype struct")
    dtype->obj.item.type = H5I_DATATYPE;
    dtype->obj.item.open_req = NULL;
    dtype->obj.item.file = item->file;
    dtype->obj.item.rc = 1;
    dtype->obj.obj_oh = DAOS_HDL_INVAL;
    dtype->type_id = FAIL;
    dtype->tcpl_id = FAIL;
    dtype->tapl_id = FAIL;

    /* Check if we're actually opening the group or just receiving the datatype
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by address */
        if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
            /* Generate oid from address */
            H5_daos_oid_generate(&dtype->obj.oid, (uint64_t)loc_params->loc_data.loc_by_addr.addr, H5I_DATATYPE);
        } /* end if */
        else {
            /* Open using name parameter */
            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_BADITER, NULL, "can't traverse path")

            /* Follow link to datatype */
            if(H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &dtype->obj.oid) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't follow link to datatype")
        } /* end else */

        /* Open datatype */
        if(0 != (ret = daos_obj_open(item->file->coh, dtype->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &dtype->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype: %s", H5_daos_err_to_string(ret))

        /* Set up operation to read datatype and TCPL sizes from datatype */
        /* Set up dkey */
        daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, H5_daos_type_key_g, H5_daos_type_key_size_g);
        daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = DAOS_REC_ANY;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = DAOS_REC_ANY;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        /* Read internal metadata sizes from datatype */
        if(0 != (ret = daos_obj_fetch(dtype->obj.obj_oh, DAOS_TX_NONE, &dkey, 2, iod, NULL,
                      NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTDECODE, NULL, "can't read metadata sizes from datatype: %s", H5_daos_err_to_string(ret))

        /* Check for metadata not found */
        if((iod[0].iod_size == (uint64_t)0) || (iod[1].iod_size == (uint64_t)0))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, NULL, "internal metadata not found")

        /* Compute datatype info buffer size */
        type_len = iod[0].iod_size;
        tcpl_len = iod[1].iod_size;
        tot_len = type_len + tcpl_len;

        /* Allocate datatype info buffer if necessary */
        if((tot_len + (4 * sizeof(uint64_t))) > sizeof(tinfo_buf_static)) {
            if(NULL == (tinfo_buf_dyn = (uint8_t *)DV_malloc(tot_len + (4 * sizeof(uint64_t)))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate datatype info buffer")
            tinfo_buf = tinfo_buf_dyn;
        } /* end if */

        /* Set up sgl */
        p = tinfo_buf + (4 * sizeof(uint64_t));
        daos_iov_set(&sg_iov[0], p, (daos_size_t)type_len);
        sgl[0].sg_nr = 1;
        sgl[0].sg_nr_out = 0;
        sgl[0].sg_iovs = &sg_iov[0];
        p += type_len;
        daos_iov_set(&sg_iov[1], p, (daos_size_t)tcpl_len);
        sgl[1].sg_nr = 1;
        sgl[1].sg_nr_out = 0;
        sgl[1].sg_iovs = &sg_iov[1];

        /* Read internal metadata from datatype */
        if(0 != (ret = daos_obj_fetch(dtype->obj.obj_oh, DAOS_TX_NONE, &dkey, 2, iod, sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTDECODE, NULL, "can't read metadata from datatype: %s", H5_daos_err_to_string(ret))

        /* Broadcast datatype info if there are other processes that need it */
        if(collective && (item->file->num_procs > 1)) {
            assert(tinfo_buf);
            assert(sizeof(tinfo_buf_static) >= 4 * sizeof(uint64_t));

            /* Encode oid */
            p = tinfo_buf;
            UINT64ENCODE(p, dtype->obj.oid.lo)
            UINT64ENCODE(p, dtype->obj.oid.hi)

            /* Encode serialized info lengths */
            UINT64ENCODE(p, type_len)
            UINT64ENCODE(p, tcpl_len)

            /* MPI_Bcast dinfo_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)tinfo_buf, sizeof(tinfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_MPI, NULL, "can't broadcast datatype info")

            /* Need a second bcast if it did not fit in the receivers' static
             * buffer */
            if(tot_len + (4 * sizeof(uint64_t)) > sizeof(tinfo_buf_static))
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_MPI, NULL, "can't broadcast datatype info (second broadcast)")
        } /* end if */
        else
            p = tinfo_buf + (4 * sizeof(uint64_t));
    } /* end if */
    else {
        /* Receive datatype info */
        if(MPI_SUCCESS != MPI_Bcast((char *)tinfo_buf, sizeof(tinfo_buf_static), MPI_BYTE, 0, item->file->comm))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_MPI, NULL, "can't receive broadcasted datatype info")

        /* Decode oid */
        p = tinfo_buf_static;
        UINT64DECODE(p, dtype->obj.oid.lo)
        UINT64DECODE(p, dtype->obj.oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_len)
        UINT64DECODE(p, tcpl_len)
        tot_len = type_len + tcpl_len;

        /* Check for type_len set to 0 - indicates failure */
        if(type_len == 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "lead process failed to open datatype")

        /* Check if we need to perform another bcast */
        if(tot_len + (4 * sizeof(uint64_t)) > sizeof(tinfo_buf_static)) {
            /* Allocate a dynamic buffer if necessary */
            if(tot_len > sizeof(tinfo_buf_static)) {
                if(NULL == (tinfo_buf_dyn = (uint8_t *)DV_malloc(tot_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for datatype info")
                tinfo_buf = tinfo_buf_dyn;
            } /* end if */

            /* Receive datatype info */
            if(MPI_SUCCESS != MPI_Bcast((char *)tinfo_buf, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_DATATYPE, H5E_MPI, NULL, "can't receive broadcasted datatype info (second broadcast)")

            p = tinfo_buf;
        } /* end if */

        /* Open datatype */
        if(0 != (ret = daos_obj_open(item->file->coh, dtype->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &dtype->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "can't open datatype: %s", H5_daos_err_to_string(ret))
    } /* end else */

    /* Decode datatype and TCPL */
    if((dtype->type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += type_len;
    if((dtype->tcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype creation property list")

    /* Finish setting up datatype struct */
    if((dtype->tapl_id = H5Pcopy(tapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy tapl");

    /* Set return value */
    ret_value = (void *)dtype;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast tinfo_buf as '0' if necessary - this will trigger failures in
         * in other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(tinfo_buf_static, 0, sizeof(tinfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(tinfo_buf_static, sizeof(tinfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_DONE_ERROR(H5E_DATATYPE, H5E_MPI, NULL, "can't broadcast empty datatype info")
        } /* end if */

        /* Close datatype */
        if(dtype && H5_daos_datatype_close(dtype, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close datatype")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    tinfo_buf_dyn = (uint8_t *)DV_free(tinfo_buf_dyn);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_datatype_open() */


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
H5_daos_datatype_get(void *_dtype, H5VL_datatype_get_t get_type,
    hid_t DV_ATTR_UNUSED dxpl_id, void DV_ATTR_UNUSED **req, va_list arguments)
{
    H5_daos_dtype_t *dtype = (H5_daos_dtype_t *)_dtype;
    herr_t       ret_value = SUCCEED;    /* Return value */

    if(!_dtype)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")

    switch (get_type) {
        case H5VL_DATATYPE_GET_BINARY:
            {
                ssize_t *nalloc = va_arg(arguments, ssize_t *);
                void *buf = va_arg(arguments, void *);
                size_t size = va_arg(arguments, size_t);

                if(H5Tencode(dtype->type_id, buf, &size) < 0)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't determine serialized length of datatype")

                *nalloc = (ssize_t)size;
                break;
            } /* end block */
        case H5VL_DATATYPE_GET_TCPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the datatype's creation property list */
                if((*plist_id = H5Pcopy(dtype->tcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get dtype creation property list")

                break;
            } /* end block */
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from datatype")
    } /* end switch */

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_datatype_get() */


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
H5_daos_datatype_close(void *_dtype, hid_t DV_ATTR_UNUSED dxpl_id,
    void DV_ATTR_UNUSED **req)
{
    H5_daos_dtype_t *dtype = (H5_daos_dtype_t *)_dtype;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_dtype)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype object is NULL")

    if(--dtype->obj.item.rc == 0) {
        /* Free datatype data structures */
        if(dtype->obj.item.open_req)
            H5_daos_req_free_int(dtype->obj.item.open_req);
        if(!daos_handle_is_inval(dtype->obj.obj_oh))
            if(0 != (ret = daos_obj_close(dtype->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "can't close datatype DAOS object: %s", H5_daos_err_to_string(ret))
        if(dtype->type_id != FAIL && H5Idec_ref(dtype->type_id) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(dtype->tcpl_id != FAIL && H5Idec_ref(dtype->tcpl_id) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close tcpl")
        if(dtype->tapl_id != FAIL && H5Idec_ref(dtype->tapl_id) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close tapl")
        dtype = H5FL_FREE(H5_daos_dtype_t, dtype);
    } /* end if */

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_datatype_close() */

