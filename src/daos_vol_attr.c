/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 * library. Attribute routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_create
 *
 * Purpose:     Sends a request to DAOS to create an attribute
 *
 * Return:      Success:        attribute object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_attribute_create(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t acpl_id, hid_t H5VL_DAOS_UNUSED aapl_id,
    hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_attr_t *attr = NULL;
    size_t akey_len;
    hid_t type_id, space_id;
    daos_key_t dkey;
    char *type_key = NULL;
    char *space_key = NULL;
    daos_iod_t iod[2];
    daos_sg_list_t sgl[2];
    daos_iov_t sg_iov[2];
    size_t type_size = 0;
    size_t space_size = 0;
    void *type_buf = NULL;
    void *space_buf = NULL;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute name is NULL")

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")

    /* get creation properties */
    if(H5Pget(acpl_id, H5VL_PROP_ATTR_TYPE_ID, &type_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for datatype ID")
    if(H5Pget(acpl_id, H5VL_PROP_ATTR_SPACE_ID, &space_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for dataspace ID")

    /* Allocate the attribute object that is returned to the user */
    if(NULL == (attr = H5FL_CALLOC(H5_daos_attr_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    attr->item.type = H5I_ATTR;
    attr->item.open_req = NULL;
    attr->item.file = item->file;
    attr->item.rc = 1;
    attr->type_id = FAIL;
    attr->space_id = FAIL;

    /* Determine attribute object */
    if(loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as attribute parent object, or the root group if item is a file */
        if(item->type == H5I_FILE)
            attr->parent = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
        else
            attr->parent = (H5_daos_obj_t *)item;
        attr->parent->item.rc++;
    } /* end if */
    else if(loc_params->type == H5VL_OBJECT_BY_NAME) {
        /* Open target_obj */
        if(NULL == (attr->parent = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open object for attribute")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, NULL, "unsupported attribute create location parameters type")

    /* Encode datatype */
    if(H5Tencode(type_id, NULL, &type_size) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
    if(NULL == (type_buf = DV_malloc(type_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
    if(H5Tencode(type_id, type_buf, &type_size) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize datatype")

    /* Encode dataspace */
    if(H5Sencode(space_id, NULL, &space_size) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataspace")
    if(NULL == (space_buf = DV_malloc(space_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataspace")
    if(H5Sencode(space_id, space_buf, &space_size) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dataspace")

    /* Set up operation to write datatype and dataspace to attribute */
    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Create akey strings (prefix "S-", "T-") */
    akey_len = strlen(name) + 2;
    if(NULL == (type_key = (char *)DV_malloc(akey_len + 1)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for akey")
    if(NULL == (space_key = (char *)DV_malloc(akey_len + 1)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for akey")
    type_key[0] = 'T';
    type_key[1] = '-';
    space_key[0] = 'S';
    space_key[1] = '-';
    (void)strcpy(type_key + 2, name);
    (void)strcpy(space_key + 2, name);

    /* Set up iod */
    memset(iod, 0, sizeof(iod));
    daos_iov_set(&iod[0].iod_name, (void *)type_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
    iod[0].iod_nr = 1u;
    iod[0].iod_size = (uint64_t)type_size;
    iod[0].iod_type = DAOS_IOD_SINGLE;

    daos_iov_set(&iod[1].iod_name, (void *)space_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
    iod[1].iod_nr = 1u;
    iod[1].iod_size = (uint64_t)space_size;
    iod[1].iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov[0], type_buf, (daos_size_t)type_size);
    sgl[0].sg_nr = 1;
    sgl[0].sg_nr_out = 0;
    sgl[0].sg_iovs = &sg_iov[0];
    daos_iov_set(&sg_iov[1], space_buf, (daos_size_t)space_size);
    sgl[1].sg_nr = 1;
    sgl[1].sg_nr_out = 0;
    sgl[1].sg_iovs = &sg_iov[1];

    /* Write attribute metadata to parent object */
    if(0 != (ret = daos_obj_update(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 2, iod, sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't write attribute metadata: %s", H5_daos_err_to_string(ret))

    /* Finish setting up attribute struct */
    if(NULL == (attr->name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name")
    if((attr->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((attr->space_id = H5Scopy(space_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dataspace")
    if(H5Sselect_all(attr->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")

    ret_value = (void *)attr;

done:
    /* Free memory */
    type_buf = DV_free(type_buf);
    space_buf = DV_free(space_buf);
    type_key = (char *)DV_free(type_key);
    space_key = (char *)DV_free(space_key);

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close attribute */
        if(attr && H5_daos_attribute_close(attr, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close attribute")

    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open
 *
 * Purpose:     Sends a request to DAOS to open an attribute
 *
 * Return:      Success:        attribute object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_attribute_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t H5VL_DAOS_UNUSED aapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_attr_t *attr = NULL;
    size_t akey_len;
    daos_key_t dkey;
    char *type_key = NULL;
    char *space_key = NULL;
    daos_iod_t iod[2];
    daos_sg_list_t sgl[2];
    daos_iov_t sg_iov[2];
    void *type_buf = NULL;
    void *space_buf = NULL;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute name is NULL")

    /* Allocate the attribute object that is returned to the user */
    if(NULL == (attr = H5FL_CALLOC(H5_daos_attr_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    attr->item.type = H5I_ATTR;
    attr->item.open_req = NULL;
    attr->item.file = item->file;
    attr->item.rc = 1;
    attr->type_id = FAIL;
    attr->space_id = FAIL;

    /* Determine attribute object */
    if(loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as attribute parent object, or the root group if item is a file */
        if(item->type == H5I_FILE)
            attr->parent = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
        else
            attr->parent = (H5_daos_obj_t *)item;
        attr->parent->item.rc++;
    } /* end if */
    else if(loc_params->type == H5VL_OBJECT_BY_NAME) {
        /* Open target_obj */
        if(NULL == (attr->parent = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open object for attribute")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, NULL, "unsupported attribute open location parameters type")

    /* Set up operation to write datatype and dataspace to attribute */
    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Create akey strings (prefix "S-", "T-") */
    akey_len = strlen(name) + 2;
    if(NULL == (type_key = (char *)DV_malloc(akey_len + 1)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for akey")
    if(NULL == (space_key = (char *)DV_malloc(akey_len + 1)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for akey")
    type_key[0] = 'T';
    type_key[1] = '-';
    space_key[0] = 'S';
    space_key[1] = '-';
    (void)strcpy(type_key + 2, name);
    (void)strcpy(space_key + 2, name);

    /* Set up iod */
    memset(iod, 0, sizeof(iod));
    daos_iov_set(&iod[0].iod_name, (void *)type_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
    iod[0].iod_nr = 1u;
    iod[0].iod_size = DAOS_REC_ANY;
    iod[0].iod_type = DAOS_IOD_SINGLE;

    daos_iov_set(&iod[1].iod_name, (void *)space_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
    iod[1].iod_nr = 1u;
    iod[1].iod_size = DAOS_REC_ANY;
    iod[1].iod_type = DAOS_IOD_SINGLE;

    /* Read attribute metadata sizes from parent object */
    if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 2, iod, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, NULL, "can't read attribute metadata sizes: %s", H5_daos_err_to_string(ret))

    if(iod[0].iod_size == (uint64_t)0 || iod[1].iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, NULL, "attribute not found")

    /* Allocate buffers for datatype and dataspace */
    if(NULL == (type_buf = DV_malloc(iod[0].iod_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
    if(NULL == (space_buf = DV_malloc(iod[1].iod_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataspace")

    /* Set up sgl */
    daos_iov_set(&sg_iov[0], type_buf, (daos_size_t)iod[0].iod_size);
    sgl[0].sg_nr = 1;
    sgl[0].sg_nr_out = 0;
    sgl[0].sg_iovs = &sg_iov[0];
    daos_iov_set(&sg_iov[1], space_buf, (daos_size_t)iod[1].iod_size);
    sgl[1].sg_nr = 1;
    sgl[1].sg_nr_out = 0;
    sgl[1].sg_iovs = &sg_iov[1];

    /* Read attribute metadata from parent object */
    if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 2, iod, sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, NULL, "can't read attribute metadata: %s", H5_daos_err_to_string(ret))

    /* Decode datatype and dataspace */
    if((attr->type_id = H5Tdecode(type_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    if((attr->space_id = H5Sdecode(space_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    if(H5Sselect_all(attr->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")

    /* Finish setting up attribute struct */
    if(NULL == (attr->name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name")

    ret_value = (void *)attr;

done:
    /* Free memory */
    type_buf = DV_free(type_buf);
    space_buf = DV_free(space_buf);
    type_key = (char *)DV_free(type_key);
    space_key = (char *)DV_free(space_key);

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close attribute */
        if(attr && H5_daos_attribute_close(attr, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close attribute")

    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_read
 *
 * Purpose:     Reads raw data from an attribute into a buffer.
 *
 * Return:      Success:        0
 *              Failure:        -1, attribute not read.
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_read(void *_attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_attr_t *attr = (H5_daos_attr_t *)_attr;
    int ndims;
    hsize_t dim[H5S_MAX_RANK];
    size_t akey_len;
    daos_key_t dkey;
    char *akey = NULL;
    uint8_t **akeys = NULL;
    daos_iod_t *iods = NULL;
    daos_sg_list_t *sgls = NULL;
    daos_iov_t *sg_iovs = NULL;
    hid_t base_type_id = FAIL;
    size_t base_type_size = 0;
    uint64_t attr_size;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    H5T_class_t type_class;
    hbool_t is_vl = FALSE;
    htri_t is_vl_str = FALSE;
    int ret;
    uint64_t i;
    herr_t ret_value = SUCCEED;

    if(!_attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL")
    if(!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "read buffer is NULL")
    if(H5I_ATTR != attr->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not an attribute")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(attr->space_id, dim, NULL))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get dimensions")

    /* Calculate attribute size */
    attr_size = (uint64_t)1;
    for(i = 0; i < (uint64_t)ndims; i++)
        attr_size *= (uint64_t)dim[i];

    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Check for vlen */
    if(H5T_NO_CLASS == (type_class = H5Tget_class(mem_type_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype class")
    if(type_class == H5T_VLEN) {
        is_vl = TRUE;

        /* Calculate base type size */
        if((base_type_id = H5Tget_super(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype base type")
        if(0 == (base_type_size = H5Tget_size(base_type_id)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype base type size")
    } /* end if */
    else if(type_class == H5T_STRING) {
        /* check for vlen string */
        if((is_vl_str = H5Tis_variable_str(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't check for variable length string")
        if(is_vl_str)
            is_vl = TRUE;
    } /* end if */

    /* Check for variable length */
    if(is_vl) {
        size_t akey_str_len;
        uint64_t offset = 0;
        uint8_t *p;

        /* Calculate akey length */
        akey_str_len = strlen(attr->name) + 2;
        akey_len = akey_str_len + sizeof(uint64_t);

        /* Allocate array of akey pointers */
        if(NULL == (akeys = (uint8_t **)DV_calloc(attr_size * sizeof(uint8_t *))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey array")

        /* Allocate array of iods */
        if(NULL == (iods = (daos_iod_t *)DV_calloc(attr_size * sizeof(daos_iod_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O descriptor array")

        /* First loop over elements, set up operation to read vl sizes */
        for(i = 0; i < attr_size; i++) {
            /* Create akey for this element */
            if(NULL == (akeys[i] = (uint8_t *)DV_malloc(akey_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
            akeys[i][0] = 'V';
            akeys[i][1] = '-';
            (void)strcpy((char *)akeys[i] + 2, attr->name);
            p = akeys[i] + akey_str_len;
            UINT64ENCODE(p, i)

            /* Set up iod.  Use "single" records of varying size. */
            daos_iov_set(&iods[i].iod_name, (void *)akeys[i], (daos_size_t)akey_len);
            daos_csum_set(&iods[i].iod_kcsum, NULL, 0);
            iods[i].iod_nr = 1u;
            iods[i].iod_size = DAOS_REC_ANY;
            iods[i].iod_type = DAOS_IOD_SINGLE;
        } /* end for */

        /* Read vl sizes from attribute */
        /* Note cast to unsigned reduces width to 32 bits.  Should eventually
         * check for overflow and iterate over 2^32 size blocks */
        if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, (unsigned)attr_size, iods, NULL, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read vl data sizes from attribute: %s", H5_daos_err_to_string(ret))

        /* Allocate array of sg_iovs */
        if(NULL == (sg_iovs = (daos_iov_t *)DV_malloc(attr_size * sizeof(daos_iov_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list")

        /* Allocate array of sgls */
        if(NULL == (sgls = (daos_sg_list_t *)DV_malloc(attr_size * sizeof(daos_sg_list_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list array")

        /* Second loop over elements, set up operation to read vl data */
        for(i = 0; i < attr_size; i++) {
            /* Set up constant sgl info */
            sgls[i].sg_nr = 1;
            sgls[i].sg_nr_out = 0;
            sgls[i].sg_iovs = &sg_iovs[i];

            /* Check for empty element */
            if(iods[i].iod_size == 0) {
                /* Increment offset, slide down following elements */
                offset++;

                /* Zero out read buffer */
                if(is_vl_str)
                    ((char **)buf)[i] = NULL;
                else
                    memset(&((hvl_t *)buf)[i], 0, sizeof(hvl_t));
            } /* end if */
            else {
                assert(i >= offset);

                /* Check for vlen string */
                if(is_vl_str) {
                    char *elem = NULL;

                    /* Allocate buffer for this vl element */
                    if(NULL == (elem = (char *)malloc((size_t)iods[i].iod_size + 1)))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate vl data buffer")
                    ((char **)buf)[i] = elem;

                    /* Add null terminator */
                    elem[iods[i].iod_size] = '\0';

                    /* Set buffer location in sgl */
                    daos_iov_set(&sg_iovs[i - offset], elem, iods[i].iod_size);
                } /* end if */
                else {
                    /* Standard vlen, find hvl_t struct for this element */
                    hvl_t *elem = &((hvl_t *)buf)[i];

                    assert(base_type_size > 0);

                    /* Allocate buffer for this vl element and set size */
                    elem->len = (size_t)iods[i].iod_size / base_type_size;
                    if(NULL == (elem->p = malloc((size_t)iods[i].iod_size)))
                        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate vl data buffer")

                    /* Set buffer location in sgl */
                    daos_iov_set(&sg_iovs[i - offset], elem->p, iods[i].iod_size);
                } /* end if */

                /* Slide down iod if necessary */
                if(offset)
                    iods[i - offset] = iods[i];
            } /* end else */
        } /* end for */

        /* Read data from attribute */
        /* Note cast to unsigned reduces width to 32 bits.  Should eventually
         * check for overflow and iterate over 2^32 size blocks */
        if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, (unsigned)(attr_size - offset), iods, sgls, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read data from attribute: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else {
        daos_iod_t iod;
        daos_recx_t recx;
        daos_sg_list_t sgl;
        daos_iov_t sg_iov;
        size_t mem_type_size;
        size_t file_type_size;
        H5_daos_tconv_reuse_t reuse = H5_DAOS_TCONV_REUSE_NONE;
        hbool_t fill_bkg = FALSE;

        /* Check for type conversion */
        if(H5_daos_tconv_init(attr->type_id, &file_type_size, mem_type_id, &mem_type_size, (size_t)attr_size, &tconv_buf, &bkg_buf, &reuse, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't initialize type conversion")

        /* Reuse buffer as appropriate */
        if(reuse == H5_DAOS_TCONV_REUSE_TCONV)
            tconv_buf = buf;
        else if(reuse == H5_DAOS_TCONV_REUSE_BKG)
            bkg_buf = buf;

        /* Fill background buffer if necessary */
        if(fill_bkg && (bkg_buf != buf))
            (void)memcpy(bkg_buf, buf, (size_t)attr_size * mem_type_size);

        /* Set up operation to read data */
        /* Create akey string (prefix "V-") */
        akey_len = strlen(attr->name) + 2;
        if(NULL == (akey = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        akey[0] = 'V';
        akey[1] = '-';
        (void)strcpy(akey + 2, attr->name);

        /* Set up recx */
        recx.rx_idx = (uint64_t)0;
        recx.rx_nr = attr_size;

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)akey, (daos_size_t)akey_len);
        daos_csum_set(&iod.iod_kcsum, NULL, 0);
        iod.iod_nr = 1u;
        iod.iod_recxs = &recx;
        iod.iod_size = (uint64_t)file_type_size;
        iod.iod_type = DAOS_IOD_ARRAY;

        /* Set up sgl */
        daos_iov_set(&sg_iov, tconv_buf ? tconv_buf : buf, (daos_size_t)(attr_size * (uint64_t)file_type_size));
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Read data from attribute */
        if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read data from attribute: %s", H5_daos_err_to_string(ret))

        /* Perform type conversion if necessary */
        if(tconv_buf) {
            /* Type conversion */
            if(H5Tconvert(attr->type_id, mem_type_id, attr_size, tconv_buf, bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

            /* Copy to user's buffer if necessary */
            if(buf != tconv_buf)
                (void)memcpy(buf, tconv_buf, (size_t)attr_size * mem_type_size);
        } /* end if */
    } /* end else */

done:
    /* Free memory */
    akey = (char *)DV_free(akey);
    iods = (daos_iod_t *)DV_free(iods);
    sgls = (daos_sg_list_t *)DV_free(sgls);
    sg_iovs = (daos_iov_t *)DV_free(sg_iovs);
    if(tconv_buf && (tconv_buf != buf))
        DV_free(tconv_buf);
    if(bkg_buf && (bkg_buf != buf))
        DV_free(bkg_buf);

    if(akeys) {
        for(i = 0; i < attr_size; i++)
            DV_free(akeys[i]);
        DV_free(akeys);
    } /* end if */

    if(base_type_id != FAIL)
        if(H5Idec_ref(base_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close base type ID")

    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_read() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_write
 *
 * Purpose:     Writes raw data from a buffer into an attribute.
 *
 * Return:      Success:        0
 *              Failure:        -1, attribute not written.
 *
 * Programmer:  Neil Fortner
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_write(void *_attr, hid_t mem_type_id, const void *buf,
    hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    H5_daos_attr_t *attr = (H5_daos_attr_t *)_attr;
    int ndims;
    hsize_t dim[H5S_MAX_RANK];
    size_t akey_len;
    daos_key_t dkey;
    char *akey = NULL;
    uint8_t **akeys = NULL;
    daos_iod_t *iods = NULL;
    daos_sg_list_t *sgls = NULL;
    daos_iov_t *sg_iovs = NULL;
    hid_t base_type_id = FAIL;
    size_t base_type_size = 0;
    uint64_t attr_size;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    H5T_class_t type_class;
    hbool_t is_vl = FALSE;
    htri_t is_vl_str = FALSE;
    int ret;
    uint64_t i;
    herr_t ret_value = SUCCEED;

    if(!_attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL")
    if(!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "write buffer is NULL")
    if(H5I_ATTR != attr->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not an attribute")

    /* Check for write access */
    if(!(attr->item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(attr->space_id, dim, NULL))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get dimensions")

    /* Calculate attribute size */
    attr_size = (uint64_t)1;
    for(i = 0; i < (uint64_t)ndims; i++)
        attr_size *= (uint64_t)dim[i];

    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Check for vlen */
    if(H5T_NO_CLASS == (type_class = H5Tget_class(mem_type_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype class")
    if(type_class == H5T_VLEN) {
        is_vl = TRUE;

        /* Calculate base type size */
        if((base_type_id = H5Tget_super(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype base type")
        if(0 == (base_type_size = H5Tget_size(base_type_id)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype base type size")
    } /* end if */
    else if(type_class == H5T_STRING) {
        /* check for vlen string */
        if((is_vl_str = H5Tis_variable_str(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't check for variable length string")
        if(is_vl_str)
            is_vl = TRUE;
    } /* end if */

    /* Check for variable length */
    if(is_vl) {
        size_t akey_str_len;
        uint8_t *p;

        /* Calculate akey length */
        akey_str_len = strlen(attr->name) + 2;
        akey_len = akey_str_len + sizeof(uint64_t);

        /* Allocate array of akey pointers */
        if(NULL == (akeys = (uint8_t **)DV_calloc(attr_size * sizeof(uint8_t *))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey array")

        /* Allocate array of iods */
        if(NULL == (iods = (daos_iod_t *)DV_calloc(attr_size * sizeof(daos_iod_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O descriptor array")

        /* Allocate array of sg_iovs */
        if(NULL == (sg_iovs = (daos_iov_t *)DV_malloc(attr_size * sizeof(daos_iov_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list")

        /* Allocate array of sgls */
        if(NULL == (sgls = (daos_sg_list_t *)DV_malloc(attr_size * sizeof(daos_sg_list_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list array")

        /* Loop over elements */
        for(i = 0; i < attr_size; i++) {
            /* Create akey for this element */
            if(NULL == (akeys[i] = (uint8_t *)DV_malloc(akey_len)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
            akeys[i][0] = 'V';
            akeys[i][1] = '-';
            (void)strcpy((char *)akeys[i] + 2, attr->name);
            p = akeys[i] + akey_str_len;
            UINT64ENCODE(p, i)

            /* Set up iod, determine size below.  Use "single" records of
             * varying size. */
            daos_iov_set(&iods[i].iod_name, (void *)akeys[i], (daos_size_t)akey_len);
            daos_csum_set(&iods[i].iod_kcsum, NULL, 0);
            iods[i].iod_nr = 1u;
            iods[i].iod_type = DAOS_IOD_SINGLE;

            /* Set up constant sgl info */
            sgls[i].sg_nr = 1;
            sgls[i].sg_nr_out = 0;
            sgls[i].sg_iovs = &sg_iovs[i];

            /* Check for vlen string */
            if(is_vl_str) {
                /* Find string for this element */
                char *elem = ((char * const *)buf)[i];

                /* Set string length in iod and buffer location in sgl.  If we
                 * are writing an empty string ("\0"), increase the size by one
                 * to differentiate it from NULL strings.  Note that this will
                 * cause the read buffer to be one byte longer than it needs to
                 * be in this case.  This should not cause any ill effects. */
                if(elem) {
                    iods[i].iod_size = (daos_size_t)strlen(elem);
                    if(iods[i].iod_size == 0)
                        iods[i].iod_size = 1;
                    daos_iov_set(&sg_iovs[i], (void *)elem, iods[i].iod_size);
                } /* end if */
                else {
                    iods[i].iod_size = 0;
                    daos_iov_set(&sg_iovs[i], NULL, 0);
                } /* end else */
            } /* end if */
            else {
                /* Standard vlen, find hvl_t struct for this element */
                const hvl_t *elem = &((const hvl_t *)buf)[i];

                assert(base_type_size > 0);

                /* Set buffer length in iod and buffer location in sgl */
                if(elem->len > 0) {
                    iods[i].iod_size = (daos_size_t)(elem->len * base_type_size);
                    daos_iov_set(&sg_iovs[i], (void *)elem->p, iods[i].iod_size);
                } /* end if */
                else {
                    iods[i].iod_size = 0;
                    daos_iov_set(&sg_iovs[i], NULL, 0);
                } /* end else */
            } /* end if */
        } /* end for */

        /* Write data to attribute */
        /* Note cast to unsigned reduces width to 32 bits.  Should eventually
         * check for overflow and iterate over 2^32 size blocks */
        if(0 != (ret = daos_obj_update(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, (unsigned)attr_size, iods, sgls, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write data to attribute: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else {
        daos_iod_t iod;
        daos_recx_t recx;
        daos_sg_list_t sgl;
        daos_iov_t sg_iov;
        size_t mem_type_size;
        size_t file_type_size;
        hbool_t fill_bkg = FALSE;

        /* Check for type conversion */
        if(H5_daos_tconv_init(mem_type_id, &mem_type_size, attr->type_id, &file_type_size, (size_t)attr_size, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't initialize type conversion")

        /* Set up operation to write data */
        /* Create akey string (prefix "V-") */
        akey_len = strlen(attr->name) + 2;
        if(NULL == (akey = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        akey[0] = 'V';
        akey[1] = '-';
        (void)strcpy(akey + 2, attr->name);

        /* Set up recx */
        recx.rx_idx = (uint64_t)0;
        recx.rx_nr = attr_size;

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)akey, (daos_size_t)akey_len);
        daos_csum_set(&iod.iod_kcsum, NULL, 0);
        iod.iod_nr = 1u;
        iod.iod_recxs = &recx;
        iod.iod_size = (uint64_t)file_type_size;
        iod.iod_type = DAOS_IOD_ARRAY;

        /* Set up constant sgl info */
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Check for type conversion */
        if(tconv_buf) {
            /* Check if we need to fill background buffer */
            if(fill_bkg) {
                assert(bkg_buf);

                /* Read data from attribute to background buffer */
                daos_iov_set(&sg_iov, bkg_buf, (daos_size_t)(attr_size * (uint64_t)file_type_size));

                if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
                    D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read data from attribute: %s", H5_daos_err_to_string(ret))
            } /* end if */

            /* Copy data to type conversion buffer */
            (void)memcpy(tconv_buf, buf, (size_t)attr_size * mem_type_size);

            /* Perform type conversion */
            if(H5Tconvert(mem_type_id, attr->type_id, attr_size, tconv_buf, bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

            /* Set sgl to write from tconv_buf */
            daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)(attr_size * (uint64_t)file_type_size));
        } /* end if */
        else
            /* Set sgl to write from buf */
            daos_iov_set(&sg_iov, (void *)buf, (daos_size_t)(attr_size * (uint64_t)file_type_size));

        /* Write data to attribute */
        if(0 != (ret = daos_obj_update(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write data to attribute: %s", H5_daos_err_to_string(ret))
    } /* end else */

done:
    /* Free memory */
    akey = (char *)DV_free(akey);
    iods = (daos_iod_t *)DV_free(iods);
    sgls = (daos_sg_list_t *)DV_free(sgls);
    sg_iovs = (daos_iov_t *)DV_free(sg_iovs);
    tconv_buf = DV_free(tconv_buf);
    bkg_buf = DV_free(bkg_buf);

    if(akeys) {
        for(i = 0; i < attr_size; i++)
            DV_free(akeys[i]);
        DV_free(akeys);
    } /* end if */

    if(base_type_id != FAIL)
        if(H5Idec_ref(base_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close base type ID")

    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_write() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get
 *
 * Purpose:     Gets certain information about an attribute
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
H5_daos_attribute_get(void *_item, H5VL_attr_get_t get_type,
    hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req, va_list arguments)
{
    herr_t ret_value = SUCCEED;    /* Return value */

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")

    switch (get_type) {
        /* H5Aget_space */
        case H5VL_ATTR_GET_SPACE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);
                H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

                /* Retrieve the attribute's dataspace */
                if((*ret_id = H5Scopy(attr->space_id)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get dataspace ID of attribute");
                break;
            } /* end block */
        /* H5Aget_type */
        case H5VL_ATTR_GET_TYPE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);
                H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

                /* Retrieve the attribute's datatype */
                if((*ret_id = H5Tcopy(attr->type_id)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get datatype ID of attribute")
                break;
            } /* end block */
        /* H5Aget_create_plist */
        case H5VL_ATTR_GET_ACPL:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);
                /*H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;*/

                /* Retrieve the file's access property list */
                if((*ret_id = H5Pcopy(H5P_ATTRIBUTE_CREATE_DEFAULT)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute creation property list");
                break;
            } /* end block */
        /* H5Aget_name */
        case H5VL_ATTR_GET_NAME:
            {
                H5VL_loc_params_t *loc_params = va_arg(arguments, H5VL_loc_params_t *);
                size_t buf_size = va_arg(arguments, size_t);
                char *buf = va_arg(arguments, char *);
                ssize_t *ret_val = va_arg(arguments, ssize_t *);
                H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

                if(H5VL_OBJECT_BY_SELF == loc_params->type) {
                    size_t copy_len;
                    size_t nbytes;

                    nbytes = strlen(attr->name);
                    assert((ssize_t)nbytes >= 0); /*overflow, pretty unlikely --rpm*/

                    /* compute the string length which will fit into the user's buffer */
                    copy_len = MIN(buf_size - 1, nbytes);

                    /* Copy all/some of the name */
                    if(buf && copy_len > 0) {
                        memcpy(buf, attr->name, copy_len);

                        /* Terminate the string */
                        buf[copy_len]='\0';
                    } /* end if */
                    *ret_val = (ssize_t)nbytes;
                } /* end if */
                else if(H5VL_OBJECT_BY_IDX == loc_params->type) {
                    *ret_val = -1;
                    D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "get attribute name by index unsupported");
                } /* end else */
                break;
            } /* end block */
        /* H5Aget_info */
        case H5VL_ATTR_GET_INFO:
        case H5VL_ATTR_GET_STORAGE_SIZE:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from attribute")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_specific
 *
 * Purpose:     Specific operations with attributes
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
H5_daos_attribute_specific(void *_item, const H5VL_loc_params_t *loc_params,
    H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_obj_t *target_obj = NULL;
    hid_t target_obj_id = FAIL;
    char *akey_buf = NULL;
    size_t akey_buf_len = 0;
    H5_daos_attr_t *attr = NULL;
    int ret;
    herr_t ret_value = SUCCEED;    /* Return value */

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    /* Determine attribute object */
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
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open object for attribute")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "unsupported attribute operation location parameters type")

    switch (specific_type) {
        /* H5Aexists */
        case H5VL_ATTR_DELETE:
        case H5VL_ATTR_EXISTS:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported specific operation")
        case H5VL_ATTR_ITER:
            {
                H5_index_t H5VL_DAOS_UNUSED idx_type = (H5_index_t)va_arg(arguments, int);
                H5_iter_order_t order = (H5_iter_order_t)va_arg(arguments, int);
                hsize_t *idx = va_arg(arguments, hsize_t *);
                H5A_operator2_t op = va_arg(arguments, H5A_operator2_t);
                void *op_data = va_arg(arguments, void *);
                daos_anchor_t anchor;
                daos_key_t dkey;
                uint32_t nr;
                daos_key_desc_t kds[H5_DAOS_ITER_LEN];
                daos_sg_list_t sgl;
                daos_iov_t sg_iov;
                H5VL_loc_params_t sub_loc_params;
                H5A_info_t ainfo;
                herr_t op_ret;
                char tmp_char;
                char *p;
                uint32_t i;

                /* Iteration restart not supported */
                if(idx && (*idx != 0))
                    D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)")

                /* Ordered iteration not supported */
                if(order != H5_ITER_NATIVE)
                    D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "ordered iteration not supported (order must be H5_ITER_NATIVE)")

                /* Initialize sub_loc_params */
                sub_loc_params.obj_type = target_obj->item.type;
                sub_loc_params.type = H5VL_OBJECT_BY_SELF;

                /* Initialize const ainfo info */
                ainfo.corder_valid = FALSE;
                ainfo.corder = 0;
                ainfo.cset = H5T_CSET_ASCII;

                /* Register id for target_obj */
                if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
                    D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")

                /* Initialize anchor */
                memset(&anchor, 0, sizeof(anchor));

                /* Set up dkey */
                daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

                /* Allocate akey_buf */
                if(NULL == (akey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akeys")
                akey_buf_len = H5_DAOS_ITER_SIZE_INIT;

                /* Set up sgl.  Report size as 1 less than buffer size so we
                 * always have room for a null terminator. */
                daos_iov_set(&sg_iov, akey_buf, (daos_size_t)(akey_buf_len - 1));
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
                            daos_iov_set(&sg_iov, akey_buf, (daos_size_t)(akey_buf_len - 1));
                        } /* end if */
                        else
                            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve attributes: %s", H5_daos_err_to_string(ret))
                    } while(1);

                    /* Loop over returned akeys */
                    p = akey_buf;
                    op_ret = 0;
                    for(i = 0; (i < nr) && (op_ret == 0); i++) {
                        /* Check for invalid key */
                        if(kds[i].kd_key_len < 3)
                            D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "attribute akey too short")
                        if(p[1] != '-')
                            D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "invalid attribute akey format")

                        /* Only do callbacks for "S-" (dataspace) keys, to avoid
                         * duplication */
                        if(p[0] == 'S') {
                            hssize_t npoints;
                            size_t type_size;

                            /* Add null terminator temporarily */
                            tmp_char = p[kds[i].kd_key_len];
                            p[kds[i].kd_key_len] = '\0';

                            /* Open attribute */
                            if(NULL == (attr = (H5_daos_attr_t *)H5_daos_attribute_open(target_obj, &sub_loc_params, &p[2], H5P_DEFAULT, dxpl_id, req)))
                                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute")

                            /* Get number of elements in attribute */
                            if((npoints = (H5Sget_simple_extent_npoints(attr->space_id))) < 0)
                                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of elements in attribute dataspace extent")

                            /* Get attribute datatype size */
                            if(0 == (type_size = H5Tget_size(attr->type_id)))
                                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute datatype size")

                            /* Set attribute size */
                            ainfo.data_size = (hsize_t)npoints * (hsize_t)type_size;

                            /* Make callback */
                            if((op_ret = op(target_obj_id, &p[2], &ainfo, op_data)) < 0)
                                D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, op_ret, "operator function returned failure")

                            /* Close attribute */
                            if(H5_daos_attribute_close(attr, dxpl_id, req) < 0)
                                D_GOTO_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute")

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

        case H5VL_ATTR_RENAME:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported specific operation")
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid specific operation")
    } /* end switch */

done:
    if(target_obj_id != FAIL) {
        if(H5Idec_ref(target_obj_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close object ID")
        target_obj_id = FAIL;
        target_obj = NULL;
    } /* end if */
    else if(target_obj) {
        if(H5_daos_object_close(target_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close object")
        target_obj = NULL;
    } /* end else */
    if(attr) {
        if(H5_daos_attribute_close(attr, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute")
        attr = NULL;
    } /* end if */
    akey_buf = (char *)DV_free(akey_buf);

    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_close
 *
 * Purpose:     Closes a DAOS HDF5 attribute.
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
H5_daos_attribute_close(void *_attr, hid_t dxpl_id, void **req)
{
    H5_daos_attr_t *attr = (H5_daos_attr_t *)_attr;
    herr_t ret_value = SUCCEED;

    if(!_attr)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute object is NULL")

    if(--attr->item.rc == 0) {
        /* Free attribute data structures */
        if(attr->item.open_req)
            H5_daos_req_free_int(attr->item.open_req);
        if(attr->parent && H5_daos_object_close(attr->parent, dxpl_id, req))
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object")
        attr->name = DV_free(attr->name);
        if(attr->type_id != FAIL && H5Idec_ref(attr->type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close attribute's datatype")
        if(attr->space_id != FAIL && H5Idec_ref(attr->space_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close attribute's dataspace")
        attr = H5FL_FREE(H5_daos_attr_t, attr);
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_close() */

