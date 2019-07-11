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
 * library. Generic object routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open
 *
 * Purpose:     Opens a DAOS HDF5 object.
 *
 * Return:      Success:        object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_object_open(void *_item, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_obj_t *obj = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_obj_id_t oid;
    uint8_t oid_buf[2 * sizeof(uint64_t)];
    uint8_t *p;
    hbool_t collective;
    hbool_t must_bcast = FALSE;
    H5I_type_t obj_type;
    H5VL_loc_params_t sub_loc_params;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")

    /*
     * DSINC - should probably use a major error code other than
     * object headers for H5O calls.
     */
    if(H5VL_OBJECT_BY_IDX == loc_params->type)
        D_GOTO_ERROR(H5E_OHDR, H5E_UNSUPPORTED, NULL, "H5Oopen_by_idx is unsupported")

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    collective = item->file->is_collective_md_read;

    /* Check loc_params type */
    if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
        /* Get object type */
        if(H5I_BADID == (obj_type = H5_daos_addr_to_type((uint64_t)loc_params->loc_data.loc_by_addr.addr)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't get object type")

        /* Generate oid from address */
        memset(&oid, 0, sizeof(oid));
        H5_daos_oid_generate(&oid, (uint64_t)loc_params->loc_data.loc_by_addr.addr, obj_type);
    } /* end if */
    else {
        assert(H5VL_OBJECT_BY_NAME == loc_params->type);

        /* Check for collective access, if not already set by the file */
        if(!collective && (H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_name.lapl_id))
            if(H5Pget_all_coll_metadata_ops(loc_params->loc_data.loc_by_name.lapl_id, &collective) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "can't get collective metadata reads property")

        /* Check if we're actually opening the group or just receiving the group
         * info from the leader */
        if(!collective || (item->file->my_rank == 0)) {
            if(collective && (item->file->num_procs > 1))
                must_bcast = TRUE;

            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_OHDR, H5E_BADITER, NULL, "can't traverse path")

            /* Check for no target_name, in this case just reopen target_grp */
            if(target_name[0] == '\0'
                    || (target_name[0] == '.' && target_name[1] == '\0'))
                oid = target_grp->obj.oid;
            else
                /* Follow link to object */
                if(H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &oid) < 0)
                    D_GOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't follow link to group")

            /* Broadcast group info if there are other processes that need it */
            if(collective && (item->file->num_procs > 1)) {
                /* Encode oid */
                p = oid_buf;
                UINT64ENCODE(p, oid.lo)
                UINT64ENCODE(p, oid.hi)

                /* We are about to bcast so we no longer need to bcast on failure */
                must_bcast = FALSE;

                /* MPI_Bcast oid_buf */
                if(MPI_SUCCESS != MPI_Bcast((char *)oid_buf, sizeof(oid_buf), MPI_BYTE, 0, item->file->comm))
                    D_GOTO_ERROR(H5E_OHDR, H5E_MPI, NULL, "can't broadcast object ID")
            } /* end if */
        } /* end if */
        else {
            /* Receive oid_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)oid_buf, sizeof(oid_buf), MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_OHDR, H5E_MPI, NULL, "can't receive broadcasted object ID")

            /* Decode oid */
            p = oid_buf;
            UINT64DECODE(p, oid.lo)
            UINT64DECODE(p, oid.hi)

            /* Check for oid.lo set to 0 - indicates failure */
            if(oid.lo == 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "lead process failed to open object")
        } /* end else */

        /* Get object type */
        if(H5I_BADID == (obj_type = H5_daos_oid_to_type(oid)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't get object type")
    } /* end else */

    /* Set up sub_loc_params */
    sub_loc_params.obj_type = item->type;
    sub_loc_params.type = H5VL_OBJECT_BY_ADDR;
    sub_loc_params.loc_data.loc_by_addr.addr = (haddr_t)oid.lo;

    /* Call type's open function */
    if(obj_type == H5I_GROUP) {
        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_group_open(item, &sub_loc_params, NULL,
                ((H5VL_OBJECT_BY_NAME == loc_params->type) && (loc_params->loc_data.loc_by_name.lapl_id != H5P_DEFAULT))
                ? loc_params->loc_data.loc_by_name.lapl_id : H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open group")
    } /* end if */
    else if(obj_type == H5I_DATASET) {
        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_dataset_open(item, &sub_loc_params, NULL,
                ((H5VL_OBJECT_BY_NAME == loc_params->type) && (loc_params->loc_data.loc_by_name.lapl_id != H5P_DEFAULT))
                ? loc_params->loc_data.loc_by_name.lapl_id : H5P_DATASET_ACCESS_DEFAULT, dxpl_id, req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open dataset")
    } /* end if */
    else if(obj_type == H5I_DATATYPE) {
        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_datatype_open(item, &sub_loc_params, NULL,
                ((H5VL_OBJECT_BY_NAME == loc_params->type) && (loc_params->loc_data.loc_by_name.lapl_id != H5P_DEFAULT))
                ? loc_params->loc_data.loc_by_name.lapl_id : H5P_DATATYPE_ACCESS_DEFAULT, dxpl_id, req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open datatype")
    } /* end if */
    else {
#ifdef DV_HAVE_MAP
        assert(obj_type == H5I_MAP);
        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_map_open(item, sub_loc_params, NULL,
                ((H5VL_OBJECT_BY_NAME == loc_params.type) && (loc_params.loc_data.loc_by_name.lapl_id != H5P_DEFAULT))
                ? loc_params.loc_data.loc_by_name.lapl_id : H5P_MAP_ACCESS_DEFAULT, dxpl_id, req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open map")
#endif
    } /* end if */

    /* Set return value */
    if(opened_type)
        *opened_type = obj_type;
    ret_value = (void *)obj;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast oid_buf as '0' if necessary - this will trigger failures in
         * other processes */
        if(must_bcast) {
            memset(oid_buf, 0, sizeof(oid_buf));
            if(MPI_SUCCESS != MPI_Bcast(oid_buf, sizeof(oid_buf), MPI_BYTE, 0, item->file->comm))
                D_DONE_ERROR(H5E_OHDR, H5E_MPI, NULL, "can't broadcast empty object ID")
        } /* end if */

        /* Close object */
        if(obj && H5_daos_object_close(obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, NULL, "can't close object")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE_API
} /* end H5_daos_object_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy
 *
 * Purpose:     Performs an object copy
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
H5_daos_object_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *loc_params2,
    const char *dst_name, hid_t H5VL_DAOS_UNUSED ocpypl_id, hid_t H5VL_DAOS_UNUSED lcpl_id,
    hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    herr_t ret_value = SUCCEED;

    if(!src_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object location is NULL")
    if(!loc_params1)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL")
    if(!src_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object name is NULL")
    if(!dst_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object location is NULL")
    if(!loc_params2)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL")
    if(!dst_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object name is NULL")

    D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "object copying is unsupported")

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get
 *
 * Purpose:     Performs an object "get" operation
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
H5_daos_object_get(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_object_get_t get_type, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req, va_list H5VL_DAOS_UNUSED arguments)
{
//    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    switch (get_type) {
        case H5VL_REF_GET_NAME:
        case H5VL_REF_GET_REGION:
        case H5VL_REF_GET_TYPE:
        case H5VL_OBJECT_GET_NAME:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object get operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_object_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_specific
 *
 * Purpose:     Performs an object "specific" operation
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
H5_daos_object_specific(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req, va_list H5VL_DAOS_UNUSED arguments)
{
//    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    switch (specific_type) {
        case H5VL_OBJECT_CHANGE_REF_COUNT:
        case H5VL_OBJECT_EXISTS:
        case H5VL_OBJECT_VISIT:
        case H5VL_REF_CREATE:
        case H5VL_OBJECT_FLUSH:
        case H5VL_OBJECT_REFRESH:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object specific operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_object_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_optional
 *
 * Purpose:     Optional operations with objects
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
H5_daos_object_optional(void *_item, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_obj_t *target_obj = NULL;
    H5VL_object_optional_t optional_type = (H5VL_object_optional_t)va_arg(arguments, int);
    H5VL_loc_params_t *loc_params = va_arg(arguments, H5VL_loc_params_t *);
    char *akey_buf = NULL;
    size_t akey_buf_len = 0;
    int ret;
    herr_t ret_value = SUCCEED;    /* Return value */

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")

    /* Determine target object */
    if(loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as attribute parent object, or the root group if item is a
         * file */
        if(item->type == H5I_FILE)
            target_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
        else
            target_obj = (H5_daos_obj_t *)item;
        target_obj->item.rc++;
    } /* end if */
    else if(loc_params->type == H5VL_OBJECT_BY_NAME) {
        /* Open target_obj */
        if(NULL == (target_obj = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "can't open object")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_OHDR, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type")

    switch (optional_type) {
        /* H5Oget_info / H5Oget_info_by_name / H5Oget_info_by_idx */
        case H5VL_OBJECT_GET_INFO:
            {
                H5O_info_t  *obj_info = va_arg(arguments, H5O_info_t *);
                unsigned fields = va_arg(arguments, unsigned);
                uint64_t fileno64;
                uint8_t *uuid_p = (uint8_t *)&target_obj->item.file->uuid;

                /* Initialize obj_info - most fields are not valid and will
                 * simply be set to 0 */
                memset(obj_info, 0, sizeof(*obj_info));

                /* Fill in valid fields of obj_info */
                /* Basic fields */
                if(fields & H5O_INFO_BASIC) {
                    /* Use the lower <sizeof(unsigned long)> bytes of the file uuid
                     * as the fileno.  Ideally we would write separate 32 and 64 bit
                     * hash functions but this should work almost as well. */
                    UINT64DECODE(uuid_p, fileno64)
                    obj_info->fileno = (unsigned long)fileno64;

                    /* Use lower 64 bits of oid as address - contains encode object
                     * type */
                    obj_info->addr = (haddr_t)target_obj->oid.lo;

                    /* Set object type */
                    if(target_obj->item.type == H5I_GROUP)
                        obj_info->type = H5O_TYPE_GROUP;
                    else if(target_obj->item.type == H5I_DATASET)
                        obj_info->type = H5O_TYPE_DATASET;
                    else if(target_obj->item.type == H5I_DATATYPE)
                        obj_info->type = H5O_TYPE_NAMED_DATATYPE;
                    else {
#ifdef DV_HAVE_MAP
                        assert(target_obj->item.type == H5I_MAP);
                        obj_info->type = H5O_TYPE_MAP;
#else
                        obj_info->type = H5O_TYPE_UNKNOWN;
#endif
                    } /* end else */

                    /* Reference count is always 1 - change this when
                     * H5Lcreate_hard() is implemented */
                    obj_info->rc = 1;
                } /* end if */

                /* Number of attributes. */
                if(fields & H5O_INFO_NUM_ATTRS) {
                    daos_anchor_t anchor;
                    uint32_t nr;
                    daos_key_t dkey;
                    daos_key_desc_t kds[H5_DAOS_ITER_LEN];
                    daos_sg_list_t sgl;
                    daos_iov_t sg_iov;
                    char *p;
                    uint32_t i;

                    /* Initialize anchor */
                    memset(&anchor, 0, sizeof(anchor));

                    /* Set up dkey */
                    daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

                    /* Allocate akey_buf */
                    if(NULL == (akey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for dkeys")
                    akey_buf_len = H5_DAOS_ITER_SIZE_INIT;

                    /* Set up sgl */
                    daos_iov_set(&sg_iov, akey_buf, (daos_size_t)akey_buf_len );
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

                            /* Ask daos for a list of akeys, break out if we succeed
                             */
                            if(0 == (ret = daos_obj_list_akey(target_obj->obj_oh, DAOS_TX_NONE, &dkey, &nr, kds, &sgl, &anchor, NULL /*event*/)))
                                break;

                            /* Call failed, if the buffer is too small double it and
                             * try again, otherwise fail */
                            if(ret == -DER_KEY2BIG) {
                                /* Allocate larger buffer */
                                DV_free(akey_buf);
                                akey_buf_len *= 2;
                                if(NULL == (akey_buf = (char *)DV_malloc(akey_buf_len)))
                                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akeys")

                                /* Update sgl */
                                daos_iov_set(&sg_iov, akey_buf, (daos_size_t)akey_buf_len);
                            } /* end if */
                            else
                                D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't list attributes: %s", H5_daos_err_to_string(ret))
                        } while(1);

                        /* Count number of returned attributes */
                        p = akey_buf;
                        for(i = 0; i < nr; i++) {
                            /* Check for invalid key */
                            if(kds[i].kd_key_len < 3)
                                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "attribute akey too short")
                            if(p[1] != '-')
                                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "invalid attribute akey format")

                            /* Only count for "S-" (dataspace) keys, to avoid
                             * duplication */
                            if(p[0] == 'S')
                                obj_info->num_attrs ++;

                            /* Advance to next akey */
                            p += kds[i].kd_key_len + kds[i].kd_csum_len;
                        } /* end for */
                    } while(!daos_anchor_is_eof(&anchor));
                } /* end if */
                /* Investigate collisions with links, etc DAOSINC */
                break;
            } /* end block */

        case H5VL_OBJECT_GET_COMMENT:
        case H5VL_OBJECT_SET_COMMENT:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported optional operation")
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid optional operation")
    } /* end switch */

done:
    if(target_obj) {
        if(H5_daos_object_close(target_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")
        target_obj = NULL;
    } /* end else */

    akey_buf = (char *)DV_free(akey_buf);

    D_FUNC_LEAVE_API
} /* end H5_daos_object_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_close
 *
 * Purpose:     Closes a DAOS HDF5 object.
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
H5_daos_object_close(void *_obj, hid_t dxpl_id, void **req)
{
    H5_daos_obj_t *obj = (H5_daos_obj_t *)_obj;
    herr_t ret_value = SUCCEED;

    if(!_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is NULL")

    /* Call type's close function */
    if(obj->item.type == H5I_GROUP) {
        if(H5_daos_group_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
    } /* end if */
    else if(obj->item.type == H5I_DATASET) {
        if(H5_daos_dataset_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset")
    } /* end if */
    else if(obj->item.type == H5I_DATATYPE) {
        if(H5_daos_datatype_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype")
    } /* end if */
    else if(obj->item.type == H5I_MAP) {
        if(H5_daos_map_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map")
    } /* end if */
    else
        assert(0 && "Invalid object type");

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_object_close() */

