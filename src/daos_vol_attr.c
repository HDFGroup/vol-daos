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
 * library. Attribute routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

static herr_t H5_daos_attribute_open_by_idx_helper(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
    H5_daos_attr_t *attr_out, hid_t dxpl_id, void **req);
static ssize_t H5_daos_attribute_get_name(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
    char *attr_name_out, size_t attr_name_out_size, hid_t dxpl_id, void **req);
static herr_t H5_daos_attribute_get_info(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    const char *attr_name, H5A_info_t *attr_info, hid_t dxpl_id, void **req);
static herr_t H5_daos_attribute_delete(H5_daos_obj_t *attr_container_obj, const H5VL_loc_params_t *loc_params, const char *attr_name);
static herr_t H5_daos_attribute_remove_from_crt_idx(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
    const char *attr_name);
static herr_t H5_daos_attribute_remove_from_crt_idx_name_cb(hid_t loc_id, const char *attr_name,
    const H5A_info_t *attr_info, void *op_data);
static herr_t H5_daos_attribute_shift_crt_idx_keys_down(H5_daos_obj_t *target_obj,
    uint64_t idx_begin, uint64_t idx_end);
static htri_t H5_daos_attribute_exists(H5_daos_obj_t *attr_container_obj, const char *attr_name);
static herr_t H5_daos_attribute_iterate_by_name_order(H5_daos_obj_t *attr_container_obj, H5_daos_iter_data_t *iter_data);
static herr_t H5_daos_attribute_iterate_by_crt_order(H5_daos_obj_t *attr_container_obj, H5_daos_iter_data_t *iter_data);
static herr_t H5_daos_attribute_rename(H5_daos_obj_t *attr_container_obj, const char *cur_attr_name,
    const char *new_attr_name);
static ssize_t H5_daos_attribute_get_name_by_idx(H5_daos_obj_t *target_obj, H5_index_t index_type,
    H5_iter_order_t iter_order, uint64_t idx, char *attr_name_out, size_t attr_name_out_size);
static ssize_t H5_daos_attribute_get_name_by_crt_order(H5_daos_obj_t *target_obj, H5_iter_order_t iter_order,
    uint64_t index, char *attr_name_out, size_t attr_name_out_size);
static ssize_t H5_daos_attribute_get_name_by_name_order(H5_daos_obj_t *target_obj, H5_iter_order_t iter_order,
    uint64_t index, char *attr_name_out, size_t attr_name_out_size);
static herr_t H5_daos_attribute_get_crt_order_by_name(H5_daos_obj_t *target_obj, const char *attr_name,
    uint64_t *crt_order);
static herr_t H5_daos_attribute_get_name_by_name_order_cb(hid_t loc_id, const char *attr_name,
    const H5A_info_t *attr_info, void *op_data);
static herr_t H5_daos_attribute_get_akey_strings(const char *attr_name, char **datatype_key_out,
    char **dataspace_key_out, char **acpl_key_out, char **acorder_key_out, char **raw_data_key_out,
    size_t *akey_len_out);

/*
 * An attribute iteration callback function data structure. It
 * is passed during attribute iteration when retrieving an
 * attribute's name by a given creation order index value.
 */
typedef struct H5_daos_attr_find_name_by_idx_ud_t {
    char *attr_name_out;
    size_t attr_name_out_size;
    uint64_t target_attr_idx;
    uint64_t cur_attr_idx;
} H5_daos_attr_find_name_by_idx_ud_t;

/*
 * An attribute iteration callback function data structure. It
 * is passed during attribute iteration when retrieving an
 * attribute's creation order index value by the given attribute's
 * name.
 */
typedef struct H5_daos_attr_crt_idx_iter_ud_t {
    const char *target_attr_name;
    uint64_t *attr_idx_out;
} H5_daos_attr_crt_idx_iter_ud_t;


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
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t H5VL_DAOS_UNUSED aapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_attr_t *attr = NULL;
    size_t akey_len;
    daos_key_t dkey;
    char *type_key = NULL;
    char *space_key = NULL;
    char *acpl_key = NULL;
    char *acorder_key = NULL;
    daos_iod_t iod[7];
    daos_sg_list_t sgl[7];
    daos_iov_t sg_iov[7];
    size_t type_size = 0;
    size_t space_size = 0;
    size_t acpl_size = 0;
    void *type_buf = NULL;
    void *space_buf = NULL;
    void *acpl_buf = NULL;
    int ret;
    /* nattr_new_buf is the write buffer for the number of attributes,
     * updated to include the attribute we're writing now.  Needs to be
     * exactly 8 bytes long because it's filled with UINT64ENCODE. */
    uint8_t nattr_new_buf[8];
    /* nattr_old_buf is the read buffer for the number of attributes, which
     * is also the creation order index for the attribute being created.
     * This buffer is subsequently used as the akey for the creation order
     * -> attribute name mapping key in iod[4]/sgl[4].  Needs to be exactly
     * 9 bytes long to contain a leading 0 followed by the creation order
     * data which is used with UINT64ENCODE/DECODE. */
    uint8_t nattr_old_buf[9];
    uint8_t max_corder_old_buf[8];
    uint8_t max_corder_new_buf[8];
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

    /* Allocate the attribute object that is returned to the user */
    if(NULL == (attr = H5FL_CALLOC(H5_daos_attr_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    attr->item.type = H5I_ATTR;
    attr->item.open_req = NULL;
    attr->item.file = item->file;
    attr->item.rc = 1;
    attr->type_id = FAIL;
    attr->space_id = FAIL;
    attr->acpl_id = FAIL;

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
    if(H5Sencode2(space_id, NULL, &space_size, item->file->fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataspace")
    if(NULL == (space_buf = DV_malloc(space_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataspace")
    if(H5Sencode2(space_id, space_buf, &space_size, item->file->fapl_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dataspace")

    /* Encode ACPL */
    if(H5Pencode2(acpl_id, NULL, &acpl_size, item->file->fapl_id) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of acpl")
    if(NULL == (acpl_buf = DV_malloc(acpl_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized acpl")
    if(H5Pencode2(acpl_id, acpl_buf, &acpl_size, item->file->fapl_id) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize acpl")

    /* Set up operation to write datatype, dataspace and ACPL to attribute */
    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up akey strings (attribute name prefixed with 'T-', 'S-' and 'P-' for datatype, dataspace and ACPL, respectively) */
    if(H5_daos_attribute_get_akey_strings(name, &type_key, &space_key, &acpl_key, attr->parent->ocpl_cache.track_acorder ? &acorder_key : NULL, NULL, &akey_len) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL, "can't generate akey strings")

    /* Set up iod */
    memset(iod, 0, sizeof(iod));

    /* iod[0] contains the key for the datatype description */
    daos_iov_set(&iod[0].iod_name, (void *)type_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
    iod[0].iod_nr = 1u;
    iod[0].iod_size = (uint64_t)type_size;
    iod[0].iod_type = DAOS_IOD_SINGLE;

    /* iod[0] contains the key for the dataspace description */
    daos_iov_set(&iod[1].iod_name, (void *)space_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
    iod[1].iod_nr = 1u;
    iod[1].iod_size = (uint64_t)space_size;
    iod[1].iod_type = DAOS_IOD_SINGLE;

    /* iod[0] contains the key for the ACPL */
    daos_iov_set(&iod[2].iod_name, (void *)acpl_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
    iod[2].iod_nr = 1u;
    iod[2].iod_size = (uint64_t)acpl_size;
    iod[2].iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    /* sgl[0] contains the serialized datatype description */
    daos_iov_set(&sg_iov[0], type_buf, (daos_size_t)type_size);
    sgl[0].sg_nr = 1;
    sgl[0].sg_nr_out = 0;
    sgl[0].sg_iovs = &sg_iov[0];

    /* sgl[1] contains the serialized dataspace description */
    daos_iov_set(&sg_iov[1], space_buf, (daos_size_t)space_size);
    sgl[1].sg_nr = 1;
    sgl[1].sg_nr_out = 0;
    sgl[1].sg_iovs = &sg_iov[1];

    /* sgl[2] contains the serialized ACPL */
    daos_iov_set(&sg_iov[2], acpl_buf, (daos_size_t)acpl_size);
    sgl[2].sg_nr = 1;
    sgl[2].sg_nr_out = 0;
    sgl[2].sg_iovs = &sg_iov[2];

    /* Check for creation order tracking */
    if(attr->parent->ocpl_cache.track_acorder) {
        uint64_t max_corder;
        uint64_t nattr;
        uint8_t *p;
        size_t name_len = strlen(name);

        /* Read object's current number of attributes and maximum attribute creation order value */

        /* Set up iod. iod[3] contains the key for the number of attributes. iod[4] contains the key
         * for the object's max. attribute creation order value.  We use index 3 & 4 here to preserve
         * the data in indices 0-2 set up above.
         */
        daos_iov_set(&iod[3].iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
        daos_csum_set(&iod[3].iod_kcsum, NULL, 0);
        iod[3].iod_nr = 1u;
        iod[3].iod_size = (uint64_t)8;
        iod[3].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[4].iod_name, (void *)H5_daos_max_attr_corder_key_g, H5_daos_max_attr_corder_key_size_g);
        daos_csum_set(&iod[4].iod_kcsum, NULL, 0);
        iod[4].iod_nr = 1u;
        iod[4].iod_size = (uint64_t)8;
        iod[4].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl.
         *
         * sgl[3] contains the read buffer for the number of attributes.
         * We will reuse this buffer in sgl[5] after the read operation.  When it's
         * written to disk it needs to contain a leading 0 byte to guarantee it
         * doesn't conflict with a string akey used in the attribute dkey, so we
         * will read the number of attributes to the last 8 bytes of the buffer.
         *
         * sgl[4] contains the read buffer for the object's max. attribute creation
         * order value. It is used to determine an attribute's permanent creation
         * order value.
         */
        nattr_old_buf[0] = 0;
        daos_iov_set(&sg_iov[3], &nattr_old_buf[1], (daos_size_t)8);
        sgl[3].sg_nr = 1;
        sgl[3].sg_nr_out = 0;
        sgl[3].sg_iovs = &sg_iov[3];

        daos_iov_set(&sg_iov[4], max_corder_old_buf, (daos_size_t)8);
        sgl[4].sg_nr = 1;
        sgl[4].sg_nr_out = 0;
        sgl[4].sg_iovs = &sg_iov[4];

        /* Read num attributes and max creation order */
        if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 2, &iod[3], &sgl[3], NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't read num attributes: %s", H5_daos_err_to_string(ret))

        p = &nattr_old_buf[1];
        /* Check for no num attributes found, in this case it must be 0 */
        if(iod[3].iod_size == (uint64_t)0) {
            nattr = 0;
            UINT64ENCODE(p, nattr);

            /* Reset iod size */
            iod[3].iod_size = (uint64_t)8;
        } /* end if */
        else {
            /* Verify the iod size was 8 as expected */
            if(iod[3].iod_size != (uint64_t)8)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, NULL, "invalid size of number of attributes value")

            /* Decode num attributes */
            UINT64DECODE(p, nattr);
        } /* end else */

        /* Add new attribute to count */
        nattr++;

        /* Check for no max creation order record found, in which case it must be 0 */
        p = max_corder_old_buf;
        if(iod[4].iod_size == (uint64_t)0) {
            max_corder = 0;
            UINT64ENCODE(p, max_corder);

            /* Reset iod size */
            iod[4].iod_size = (uint64_t)8;
        } /* end if */
        else {
            /* Verify the iod size was 8 as expected */
            if(iod[4].iod_size != (uint64_t)8)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, NULL, "invalid size of maximum attribute creation order record")

            /* Decode max. attribute creation order */
            UINT64DECODE(p, max_corder);
        } /* end else */

        /* Increase max. creation order value */
        max_corder++;

        /* Add creation order info to write command */
        /* Encode new num attributes and max. creation order */
        p = nattr_new_buf;
        UINT64ENCODE(p, nattr);
        p = max_corder_new_buf;
        UINT64ENCODE(p, max_corder);

        /* Set up iod */
        /* iod[3] contains the key for the number of attributes.  Already set up
         * from read operation. */

        /* iod[4] contains the key for the object's maximum attribute creation
         * order value. Already set up from read operation.
         */

        /* iod[5] contains the creation order of the new attribute, used as an
         * akey for retrieving the attribute name to enable attribute lookup by
         * creation order */
        daos_iov_set(&iod[5].iod_name, (void *)nattr_old_buf, 9);
        daos_csum_set(&iod[5].iod_kcsum, NULL, 0);
        iod[5].iod_nr = 1u;
        iod[5].iod_size = (uint64_t)name_len;
        iod[5].iod_type = DAOS_IOD_SINGLE;

        /* iod[6] contains the key for the creation order, to enable attribute
         * creation order lookup by name */
        daos_iov_set(&iod[6].iod_name, (void *)acorder_key, (daos_size_t)akey_len);
        daos_csum_set(&iod[6].iod_kcsum, NULL, 0);
        iod[6].iod_nr = 1u;
        iod[6].iod_size = (uint64_t)8;
        iod[6].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        /* sgl[3] contains the number of attributes, updated to include this
         * attribute */
        daos_iov_set(&sg_iov[3], nattr_new_buf, (daos_size_t)8);
        sgl[3].sg_nr = 1;
        sgl[3].sg_nr_out = 0;
        sgl[3].sg_iovs = &sg_iov[3];

        /* sgl[4] contains the object's maximum creation order value, updated
         * to include this attribute
         */
        daos_iov_set(&sg_iov[4], max_corder_new_buf, (daos_size_t)8);
        sgl[4].sg_nr = 1;
        sgl[4].sg_nr_out = 0;
        sgl[4].sg_iovs = &sg_iov[4];

        /* sgl[5] contains the attribute name, here indexed using the creation
         * order as the akey to enable attribute lookup by creation order */
        daos_iov_set(&sg_iov[5], (void *)name, (daos_size_t)name_len);
        sgl[5].sg_nr = 1;
        sgl[5].sg_nr_out = 0;
        sgl[5].sg_iovs = &sg_iov[5];

        /* sgl[6] contains the creation order (with no leading 0), to enable
         * attribute creation order lookup by name */
        daos_iov_set(&sg_iov[6], max_corder_old_buf, (daos_size_t)8);
        sgl[6].sg_nr = 1;
        sgl[6].sg_nr_out = 0;
        sgl[6].sg_iovs = &sg_iov[6];
    } /* end if */

    /* Write attribute metadata to parent object */
    if(0 != (ret = daos_obj_update(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, attr->parent->ocpl_cache.track_acorder ? 7 : 3, iod, sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "can't write attribute metadata: %s", H5_daos_err_to_string(ret))

    /* Finish setting up attribute struct */
    if(NULL == (attr->name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name")
    if((attr->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((attr->space_id = H5Scopy(space_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dataspace")
    if((attr->acpl_id = H5Pcopy(acpl_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, NULL, "failed to copy acpl")
    if(H5Sselect_all(attr->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")

    ret_value = (void *)attr;

done:
    /* Free memory */
    type_buf = DV_free(type_buf);
    space_buf = DV_free(space_buf);
    acpl_buf = DV_free(acpl_buf);
    type_key = (char *)DV_free(type_key);
    space_key = (char *)DV_free(space_key);
    acpl_key = (char *)DV_free(acpl_key);
    acorder_key = (char *)DV_free(acorder_key);

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
    char *acpl_key = NULL;
    daos_iod_t iod[3];
    daos_sg_list_t sgl[3];
    daos_iov_t sg_iov[3];
    void *type_buf = NULL;
    void *space_buf = NULL;
    void *acpl_buf = NULL;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "attribute parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name && (H5VL_OBJECT_BY_IDX != loc_params->type))
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
    attr->acpl_id = FAIL;

    /* Determine attribute's name and parent object */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_SELF:
        {
            /* Use item as attribute parent object, or the root group if item is a file */
            if(item->type == H5I_FILE)
                attr->parent = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
            else
                attr->parent = (H5_daos_obj_t *)item;
            attr->parent->item.rc++;

            /* Set attribute's name */
            if(NULL == (attr->name = strdup(name)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name")

            break;
        } /* H5VL_OBJECT_BY_SELF */

        case H5VL_OBJECT_BY_NAME:
        {
            /* Open target_obj */
            if(NULL == (attr->parent = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open object for attribute")

            /* Set attribute's name */
            if(NULL == (attr->name = strdup(name)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy attribute name")

            break;
        } /* H5VL_OBJECT_BY_NAME */

        case H5VL_OBJECT_BY_IDX:
        {
            if(H5_daos_attribute_open_by_idx_helper((H5_daos_obj_t *)item, loc_params, attr, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL, "can't get attribute's parent object and name by index")
            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, NULL, "invalid or unsupported attribute open location parameters type")
    } /* end switch */

    /* Set up operation to write datatype and dataspace to attribute */
    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up akey strings (attribute name prefixed with 'T-', 'S-' and 'P-' for datatype, dataspace and ACPL, respectively) */
    if(H5_daos_attribute_get_akey_strings(attr->name, &type_key, &space_key, &acpl_key, NULL, NULL, &akey_len) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, NULL, "can't generate akey strings")

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

    daos_iov_set(&iod[2].iod_name, (void *)acpl_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
    iod[2].iod_nr = 1u;
    iod[2].iod_size = DAOS_REC_ANY;
    iod[2].iod_type = DAOS_IOD_SINGLE;

    /* Read attribute metadata sizes from parent object */
    if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 3, iod, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, NULL, "can't read attribute metadata sizes: %s", H5_daos_err_to_string(ret))

    if(iod[0].iod_size == (uint64_t)0 || iod[1].iod_size == (uint64_t)0 || iod[2].iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, NULL, "attribute not found")

    /* Allocate buffers for datatype, dataspace and ACPL */
    if(NULL == (type_buf = DV_malloc(iod[0].iod_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
    if(NULL == (space_buf = DV_malloc(iod[1].iod_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataspace")
    if(NULL == (acpl_buf = DV_malloc(iod[2].iod_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized acpl")

    /* Set up sgl */
    daos_iov_set(&sg_iov[0], type_buf, (daos_size_t)iod[0].iod_size);
    sgl[0].sg_nr = 1;
    sgl[0].sg_nr_out = 0;
    sgl[0].sg_iovs = &sg_iov[0];
    daos_iov_set(&sg_iov[1], space_buf, (daos_size_t)iod[1].iod_size);
    sgl[1].sg_nr = 1;
    sgl[1].sg_nr_out = 0;
    sgl[1].sg_iovs = &sg_iov[1];
    daos_iov_set(&sg_iov[2], acpl_buf, (daos_size_t)iod[2].iod_size);
    sgl[2].sg_nr = 1;
    sgl[2].sg_nr_out = 0;
    sgl[2].sg_iovs = &sg_iov[2];

    /* Read attribute metadata from parent object */
    if(0 != (ret = daos_obj_fetch(attr->parent->obj_oh, DAOS_TX_NONE, &dkey, 3, iod, sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, NULL, "can't read attribute metadata: %s", H5_daos_err_to_string(ret))

    /* Decode datatype and dataspace */
    if((attr->type_id = H5Tdecode(type_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    if((attr->space_id = H5Sdecode(space_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    if((attr->acpl_id = H5Pdecode(acpl_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize acpl")
    if(H5Sselect_all(attr->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")

    ret_value = (void *)attr;

done:
    /* Free memory */
    type_buf = DV_free(type_buf);
    space_buf = DV_free(space_buf);
    acpl_buf = DV_free(acpl_buf);
    type_key = (char *)DV_free(type_key);
    space_key = (char *)DV_free(space_key);
    acpl_key = (char *)DV_free(acpl_key);

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close attribute */
        if(attr && H5_daos_attribute_close(attr, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close attribute")

    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_open_by_idx_helper
 *
 * Purpose:     Helper routine for opening an attribute by index. This
 *              routine first locates the parent object that the attribute
 *              is attached to and then retrieves the target attribute's
 *              name according to the given index type, index iteration
 *              order and index value.
 *
 *              As a side effect, the output attribute's name and parent
 *              object fields are setup.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_open_by_idx_helper(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
    H5_daos_attr_t *attr_out, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t *attr_parent_obj = NULL;
    ssize_t attr_name_size;
    char *target_name = NULL;
    char *attr_name_buf_dyn = NULL;
    char attr_name_buf_static[H5_DAOS_ATTR_NAME_BUF_SIZE];
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(loc_params);
    assert(attr_out);
    assert(H5VL_OBJECT_BY_IDX == loc_params->type);

    /* Open object that the attribute is attached to */
    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.obj_type = target_obj->item.type;
    sub_loc_params.loc_data.loc_by_name.name = loc_params->loc_data.loc_by_idx.name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;
    if(NULL == (attr_parent_obj = (H5_daos_obj_t *)H5_daos_object_open(target_obj, &sub_loc_params,
            NULL, dxpl_id, req)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, (-1), "can't open attribute's parent object")

    /* Retrieve the attribute's name by index */
    if((attr_name_size = H5_daos_attribute_get_name_by_idx(attr_parent_obj, loc_params->loc_data.loc_by_idx.idx_type,
            loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
            attr_name_buf_static, H5_DAOS_ATTR_NAME_BUF_SIZE)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")

    /* Check that buffer was large enough to fit attribute's name */
    if(attr_name_size > H5_DAOS_ATTR_NAME_BUF_SIZE - 1) {
        if(NULL == (attr_name_buf_dyn = DV_malloc(attr_name_size + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate attribute name buffer")

        /* Re-issue the call with a larger buffer */
        if((attr_name_size = H5_daos_attribute_get_name_by_idx(attr_parent_obj, loc_params->loc_data.loc_by_idx.idx_type,
                loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                attr_name_buf_dyn, attr_name_size + 1)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")

        target_name = attr_name_buf_dyn;
    } /* end if */
    else
        target_name = attr_name_buf_static;

    /* Setup attribute's parent object and name fields */
    if(NULL == (attr_out->name = strdup(target_name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy attribute name")
    attr_out->parent = attr_parent_obj;

done:
    if(attr_name_buf_dyn)
        attr_name_buf_dyn = DV_free(attr_name_buf_dyn);

    /* Cleanup on failure */
    if(ret_value < 0)
        if(attr_parent_obj && H5_daos_object_close(attr_parent_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object")

    D_FUNC_LEAVE
} /* end H5_daos_attribute_open_by_idx_helper() */


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
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

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
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

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
                H5_daos_attr_t *attr = (H5_daos_attr_t *)_item;

                /* Retrieve the attribute's creation property list */
                if((*ret_id = H5Pcopy(attr->acpl_id)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute creation property list");
                break;
            } /* end block */
        /* H5Aget_name(_by_idx) */
        case H5VL_ATTR_GET_NAME:
            {
                H5VL_loc_params_t *loc_params = va_arg(arguments, H5VL_loc_params_t *);
                size_t buf_size = va_arg(arguments, size_t);
                char *buf = va_arg(arguments, char *);
                ssize_t *ret_val = va_arg(arguments, ssize_t *);

                if((*ret_val = H5_daos_attribute_get_name((H5_daos_obj_t *)_item, loc_params,
                        buf, buf_size, dxpl_id, req)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")

                break;
            } /* end block */
        /* H5Aget_info */
        case H5VL_ATTR_GET_INFO:
            {
                H5VL_loc_params_t *loc_params = va_arg(arguments, H5VL_loc_params_t *);
                H5A_info_t *attr_info = va_arg(arguments, H5A_info_t *);
                const char *attr_name = (H5VL_OBJECT_BY_NAME == loc_params->type) ?
                        va_arg(arguments, const char *) : NULL;

                if(H5_daos_attribute_get_info(_item, loc_params, attr_name, attr_info, dxpl_id, req) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute info")

                break;
            } /* H5VL_ATTR_GET_INFO */
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
    herr_t ret_value = SUCCEED;    /* Return value */

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    /* Determine attribute object */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_SELF:
            /* Use item as attribute parent object, or the root group if item is a
             * file */
            if(item->type == H5I_FILE)
                target_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
            else
                target_obj = (H5_daos_obj_t *)item;
            target_obj->item.rc++;
            break;

        case H5VL_OBJECT_BY_NAME:
        case H5VL_OBJECT_BY_IDX:
            /* Open target_obj */
            if(NULL == (target_obj = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open object for attribute")
            break;

        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "invalid or unsupported attribute operation location parameters type")
    } /* end switch */

    switch (specific_type) {
        /* H5Adelete(_by_name/_by_idx) */
        case H5VL_ATTR_DELETE:
            {
                const char *attr_name = va_arg(arguments, const char *);

                if(H5_daos_attribute_delete(target_obj, loc_params, attr_name) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute")

                break;
            } /* H5VL_ATTR_DELETE */

        /* H5Aexists(_by_name) */
        case H5VL_ATTR_EXISTS:
            {
                const char *attr_name = va_arg(arguments, const char *);
                htri_t *attr_exists = va_arg(arguments, htri_t *);
                htri_t attr_found = FALSE;

                if((attr_found = H5_daos_attribute_exists(target_obj, attr_name)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attribute exists")

                *attr_exists = attr_found;

                break;
            } /* H5VL_ATTR_EXISTS */

        case H5VL_ATTR_ITER:
            {
                H5_daos_iter_data_t iter_data;
                H5_index_t idx_type = (H5_index_t)va_arg(arguments, int);
                H5_iter_order_t iter_order = (H5_iter_order_t)va_arg(arguments, int);
                hsize_t *idx_p = va_arg(arguments, hsize_t *);
                H5A_operator2_t iter_op = va_arg(arguments, H5A_operator2_t);
                void *op_data = va_arg(arguments, void *);

                /* Register id for target_obj */
                if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
                    D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")

                /* Initialize iteration data */
                H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, idx_type, iter_order,
                        FALSE, idx_p, target_obj_id, op_data, dxpl_id, req);
                iter_data.u.attr_iter_data.attr_iter_op = iter_op;

                if((ret_value = H5_daos_attribute_iterate(target_obj, &iter_data)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "can't iterate over attributes")

                break;
            } /* end block */

        /* H5Arename(_by_name) */
        case H5VL_ATTR_RENAME:
            {
                const char *cur_attr_name = va_arg(arguments, const char *);
                const char *new_attr_name = va_arg(arguments, const char *);

                if(H5_daos_attribute_rename(target_obj, cur_attr_name, new_attr_name) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute")

                break;
            } /* H5VL_ATTR_RENAME */

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
        if(attr->acpl_id != FAIL && H5Idec_ref(attr->acpl_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "failed to close acpl")
        attr = H5FL_FREE(H5_daos_attr_t, attr);
    } /* end if */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_attribute_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name
 *
 * Purpose:     Helper routine to retrieve an HDF5 attribute's name.
 *
 * Return:      Success:        The length of the attribute's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5_daos_attribute_get_name(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
    char *attr_name_out, size_t attr_name_out_size, hid_t dxpl_id, void **req)
{
    H5_daos_obj_t *parent_obj = NULL;
    ssize_t ret_value = 0;

    assert(target_obj);
    assert(loc_params);

    switch (loc_params->type) {
        /* H5Aget_name */
        case H5VL_OBJECT_BY_SELF:
        {
            H5_daos_attr_t *attr = (H5_daos_attr_t *)target_obj;
            size_t copy_len;
            size_t nbytes;

            nbytes = strlen(attr->name);
            assert((ssize_t)nbytes >= 0); /*overflow, pretty unlikely --rpm*/

            /* compute the string length which will fit into the user's buffer */
            copy_len = MIN(attr_name_out_size - 1, nbytes);

            /* Copy all/some of the name */
            if(attr_name_out && copy_len > 0) {
                memcpy(attr_name_out, attr->name, copy_len);

                /* Terminate the string */
                attr_name_out[copy_len] = '\0';
            } /* end if */

            ret_value = (ssize_t)nbytes;

            break;
        } /* H5VL_OBJECT_BY_SELF */

        /* H5Aget_name_by_idx */
        case H5VL_OBJECT_BY_IDX:
        {
            H5VL_loc_params_t sub_loc_params;

            /* Open object that the attribute is attached to */
            sub_loc_params.type = H5VL_OBJECT_BY_NAME;
            sub_loc_params.obj_type = target_obj->item.type;
            sub_loc_params.loc_data.loc_by_name.name = loc_params->loc_data.loc_by_idx.name;
            sub_loc_params.loc_data.loc_by_name.lapl_id = loc_params->loc_data.loc_by_idx.lapl_id;
            if(NULL == (parent_obj = (H5_daos_obj_t *)H5_daos_object_open(target_obj, &sub_loc_params,
                    NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, (-1), "can't open attribute's parent object")

            if((ret_value = H5_daos_attribute_get_name_by_idx(parent_obj, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                    attr_name_out, attr_name_out_size)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "can't get attribute name by index")

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_NAME:
        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, (-1), "invalid loc_params type")
    } /* end switch */

done:
    if(parent_obj && H5_daos_object_close(parent_obj, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, (-1), "can't close object")

    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_name() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_info
 *
 * Purpose:     Helper routine to retrieve info about an HDF5 attribute
 *              stored on a DAOS server.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_info(H5_daos_item_t *item, const H5VL_loc_params_t *loc_params,
    const char *attr_name, H5A_info_t *attr_info, hid_t dxpl_id, void **req)
{
    H5_daos_attr_t *target_attr = NULL;
    H5A_info_t local_attr_info;
    hssize_t dataspace_nelmts = 0;
    size_t datatype_size = 0;
    herr_t ret_value = SUCCEED;

    assert(item);
    assert(loc_params);
    assert(attr_info);

    /* Determine the target object */
    switch (loc_params->type) {
        /* H5Aget_info */
        case H5VL_OBJECT_BY_SELF:
        {
            target_attr = (H5_daos_attr_t *)item;
            item->rc++;
            break;
        } /* H5VL_OBJECT_BY_SELF */

        /* H5Aget_info_by_name */
        case H5VL_OBJECT_BY_NAME:
        /* H5Aget_info_by_idx */
        case H5VL_OBJECT_BY_IDX:
        {
            /* Open the target attribute */
            if(NULL == (target_attr = (H5_daos_attr_t *)H5_daos_attribute_open(item, loc_params,
                    attr_name, H5P_DEFAULT, dxpl_id, req)))
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open target attribute")

            break;
        } /* H5VL_OBJECT_BY_IDX */

        case H5VL_OBJECT_BY_ADDR:
        case H5VL_OBJECT_BY_REF:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "invalid loc_params type")
    } /* end switch */

    /* Retrieve the attribute's info */

    /* Retrieve attribute's creation order value */
    if(target_attr->parent->ocpl_cache.track_acorder) {
        uint64_t attr_crt_order = 0;

        if(H5_daos_attribute_get_crt_order_by_name(target_attr->parent, target_attr->name, &attr_crt_order) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute's creation order value")
        local_attr_info.corder = (H5O_msg_crt_idx_t)attr_crt_order; /* DSINC - no check for overflow */
        local_attr_info.corder_valid = TRUE;
    } /* end if */
    else {
        local_attr_info.corder = 0;
        local_attr_info.corder_valid = FALSE;
    } /* end else */

    /* Only ASCII character set is supported currently */
    local_attr_info.cset = H5T_CSET_ASCII;

    /* Retrieve attribute's data size */
    if(0 == (datatype_size = H5Tget_size(target_attr->type_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve attribute's datatype size")

    if((dataspace_nelmts = H5Sget_simple_extent_npoints(target_attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve number of elements in attribute's dataspace")

    /* DSINC - data_size will likely be incorrect currently for VLEN types */
    local_attr_info.data_size = datatype_size * dataspace_nelmts;

    memcpy(attr_info, &local_attr_info, sizeof(*attr_info));

done:
    if(target_attr && H5_daos_attribute_close(target_attr, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute")

    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_delete
 *
 * Purpose:     Helper routine to delete an HDF5 attribute stored on a DAOS
 *              server.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_delete(H5_daos_obj_t *attr_container_obj, const H5VL_loc_params_t *loc_params,
    const char *attr_name)
{
    const unsigned int nr = H5_DAOS_ATTR_NUM_AKEYS;
    daos_key_t dkey;
    daos_key_t akeys[H5_DAOS_ATTR_NUM_AKEYS];
    size_t akey_len;
    char *target_attr_name = attr_name;
    char *attr_name_buf_dyn = NULL;
    char attr_name_buf_static[H5_DAOS_ATTR_NAME_BUF_SIZE];
    char *type_key = NULL;
    char *space_key = NULL;
    char *acpl_key = NULL;
    char *acorder_key = NULL;
    char *data_key = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(loc_params);
    assert(attr_name);

    if(H5VL_OBJECT_BY_IDX == loc_params->type) {
        ssize_t attr_name_size;

        /* Retrieve the name of the attribute at the given index */
        if((attr_name_size = H5_daos_attribute_get_name_by_idx(attr_container_obj, loc_params->loc_data.loc_by_idx.idx_type,
                loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                attr_name_buf_static, H5_DAOS_ATTR_NAME_BUF_SIZE)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")

        /* Check that the buffer was large enough to fit attribute name */
        if(attr_name_size > H5_DAOS_ATTR_NAME_BUF_SIZE - 1) {
            if(NULL == (attr_name_buf_dyn = DV_malloc(attr_name_size + 1)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for attribute name")

            /* Re-issue the call with a larger buffer */
            if((attr_name_size = H5_daos_attribute_get_name_by_idx(attr_container_obj, loc_params->loc_data.loc_by_idx.idx_type,
                    loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                    attr_name_buf_dyn, attr_name_size + 1)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")

            target_attr_name = attr_name_buf_dyn;
        } /* end if */
        else
            target_attr_name = attr_name_buf_static;
    } /* end if */

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up akey strings (attribute name prefixed with 'T-', 'S-' and 'P-' for datatype, dataspace and ACPL, respectively) */
    if(H5_daos_attribute_get_akey_strings(target_attr_name, &type_key, &space_key, &acpl_key, &acorder_key, &data_key, &akey_len) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't generate akey strings")

    /* Set up akeys */
    memset(akeys, 0, sizeof(akeys));
    daos_iov_set(&akeys[0], (void *)type_key, (daos_size_t)akey_len);
    daos_iov_set(&akeys[1], (void *)space_key, (daos_size_t)akey_len);
    daos_iov_set(&akeys[2], (void *)acpl_key, (daos_size_t)akey_len);
    daos_iov_set(&akeys[3], (void *)acorder_key, (daos_size_t)akey_len);
    daos_iov_set(&akeys[4], (void *)data_key, (daos_size_t)akey_len);

    /* DSINC - currently no support for deleting vlen data akeys */
    if(0 != (ret = daos_obj_punch_akeys(attr_container_obj->obj_oh, DAOS_TX_NONE, &dkey,
            nr, akeys, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute")

    /* If attribute creation order is tracked, perform some bookkeeping */
    if(attr_container_obj->ocpl_cache.track_acorder) {
        hssize_t obj_num_attrs;

        /* Update the "number of attributes" key on the object */
        if((obj_num_attrs = H5_daos_object_get_num_attrs(attr_container_obj)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of attributes attached to object")
        if(obj_num_attrs > 0)
            obj_num_attrs--;

        if(H5_daos_object_update_num_attrs_key(attr_container_obj, (uint64_t)obj_num_attrs) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTMODIFY, FAIL, "can't update number of attributes attached to object")

        /* Remove the attribute from the attribute creation order index */
        if(H5_daos_attribute_remove_from_crt_idx(attr_container_obj, loc_params, target_attr_name) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "can't remove attribute from creation order index")
    } /* end if */

done:
    type_key = (char *)DV_free(type_key);
    space_key = (char *)DV_free(space_key);
    acpl_key = (char *)DV_free(acpl_key);
    acorder_key = (char *)DV_free(acorder_key);
    data_key = (char *)DV_free(data_key);

    if(attr_name_buf_dyn)
        attr_name_buf_dyn = DV_free(attr_name_buf_dyn);

    D_FUNC_LEAVE
} /* end H5_daos_attribute_delete() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_remove_from_crt_idx
 *
 * Purpose:     Removes the target attribute from the target object's
 *              attribute creation order index by locating the relevant
 *              akeys and then removing them.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_remove_from_crt_idx(H5_daos_obj_t *target_obj, const H5VL_loc_params_t *loc_params,
    const char *attr_name)
{
    daos_key_t dkey;
    daos_key_t crt_akey;
    uint64_t delete_idx = 0;
    uint8_t idx_buf[sizeof(uint64_t)];
    uint8_t *p;
    ssize_t obj_nattrs_remaining;
    hid_t target_obj_id = H5I_INVALID_HID;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(loc_params);

    /* Retrieve the current number of attributes attached to the object */
    if((obj_nattrs_remaining = H5_daos_object_get_num_attrs(target_obj)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get the number of attributes attached to object")

    /* Determine the index value of the attribute to be removed */
    if(H5VL_OBJECT_BY_IDX == loc_params->type) {
        /* DSINC - no check for safe cast here */
        /*
         * Note that this assumes this routine is always called after an attribute's
         * akeys are punched during deletion, so the number of attributes attached to
         * the object should reflect the number after the attribute has been removed.
         */
        delete_idx = (H5_ITER_DEC == loc_params->loc_data.loc_by_idx.order) ?
                (uint64_t)obj_nattrs_remaining - (uint64_t)loc_params->loc_data.loc_by_idx.n :
                (uint64_t)loc_params->loc_data.loc_by_idx.n;
    } /* end if */
    else {
        H5_daos_attr_crt_idx_iter_ud_t iter_cb_ud;
        H5_daos_iter_data_t iter_data;

        /* Register ID for object for attribute iteration */
        if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
        target_obj->item.rc++;

        /* Initialize iteration data */
        iter_cb_ud.target_attr_name = attr_name;
        iter_cb_ud.attr_idx_out = &delete_idx;
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_CRT_ORDER, H5_ITER_INC,
                FALSE, NULL, target_obj_id, &iter_cb_ud, H5P_DATASET_XFER_DEFAULT, NULL);
        iter_data.u.attr_iter_data.attr_iter_op = H5_daos_attribute_remove_from_crt_idx_name_cb;

        /*
         * TODO: Currently, deleting an attribute directly (H5Adelete) or by name (H5Adelete_by_name)
         *       means that we need to iterate through the attribute creation order index until we
         *       find the value corresponding to the attribute being deleted. This is especially
         *       important because the deletion of attributes might cause the target attribute's
         *       index value to shift downwards.
         *
         *       Once iteration restart is supported for attribute iteration, performance can
         *       be improved here by first looking up the original, permanent creation order
         *       value of the attribute using the 'attribute name -> creation order' mapping
         *       and then using that value as the starting point for iteration. In this case,
         *       the iteration order MUST be switched to H5_ITER_DEC or the key will not be
         *       found by the iteration.
         */
        if(H5_daos_attribute_iterate(target_obj, &iter_data) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed")
    } /* end else */

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Remove the akey which maps creation order -> attribute name */
    p = idx_buf;
    UINT64ENCODE(p, delete_idx);
    daos_iov_set(&crt_akey, (void *)idx_buf, sizeof(uint64_t));

    /* Remove the akey */
    if(0 != (ret = daos_obj_punch_akeys(target_obj->obj_oh, DAOS_TX_NONE, &dkey, 1, &crt_akey, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "failed to punch attribute akey: %s", H5_daos_err_to_string(ret))

    /*
     * If there are still attributes remaining on the object and we didn't delete
     * the attribute currently at the end of the creation order index, shift the
     * indices of all akeys past the removed attribute's akey down by one. This
     * maintains the ability to directly index into the attribute creation order
     * index.
     */
    if((obj_nattrs_remaining > 0) && (delete_idx < (uint64_t)obj_nattrs_remaining))
        if(H5_daos_attribute_shift_crt_idx_keys_down(target_obj, delete_idx + 1, (uint64_t)obj_nattrs_remaining) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTMODIFY, FAIL, "failed to update attribute creation order index")

done:
    if((target_obj_id >= 0) && (H5Idec_ref(target_obj_id) < 0))
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object ID")

    D_FUNC_LEAVE
} /* end H5_daos_attribute_remove_from_crt_idx() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_remove_from_crt_idx_name_cb
 *
 * Purpose:     Attribute iteration callback for
 *              H5_daos_attribute_remove_from_crt_idx which iterates
 *              through attributes by creation order until the current
 *              attribute name matches the target attribute name, at which
 *              point the attribute creation order index value for the
 *              target attribute has been found.
 *
 * Return:      Non-negative (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_remove_from_crt_idx_name_cb(hid_t H5VL_DAOS_UNUSED loc_id, const char *attr_name,
    const H5A_info_t H5VL_DAOS_UNUSED *attr_info, void *op_data)
{
    H5_daos_attr_crt_idx_iter_ud_t *cb_ud = (H5_daos_attr_crt_idx_iter_ud_t *) op_data;

    if(!strcmp(attr_name, cb_ud->target_attr_name))
        return 1;

    (*cb_ud->attr_idx_out)++;
    return 0;
} /* end H5_daos_attribute_remove_from_crt_idx_name_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_shift_crt_idx_keys_down
 *
 * Purpose:     After an attribute has been deleted from an object, this
 *              routine is used to update the object's attribute creation
 *              order index. All of the index's akeys within the range
 *              specified by the begin and end index parameters are read
 *              and then re-written to the index under new akeys whose
 *              integer 'name' values are one less than the akeys' original
 *              values.
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
H5_daos_attribute_shift_crt_idx_keys_down(H5_daos_obj_t *target_obj,
    uint64_t idx_begin, uint64_t idx_end)
{
    daos_sg_list_t *sgls = NULL;
    daos_iod_t *iods = NULL;
    daos_iov_t *sg_iovs = NULL;
    daos_key_t dkey;
    daos_key_t tail_akey;
    uint64_t tmp_uint;
    uint8_t *crt_order_attr_name_buf = NULL;
    uint8_t *p;
    size_t nattrs_shift;
    size_t i;
    char *tmp_buf = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(idx_end >= idx_begin);

    nattrs_shift = idx_end - idx_begin + 1;

    /*
     * Allocate space for the 1 akey per attribute: the akey that maps the
     * attribute's creation order value to the attribute's name.
     */
    if(NULL == (iods = DV_malloc(nattrs_shift * sizeof(*iods))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate IOD buffer")
    if(NULL == (sgls = DV_malloc(nattrs_shift * sizeof(*sgls))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate SGL buffer")
    if(NULL == (sg_iovs = DV_calloc(nattrs_shift * sizeof(*sg_iovs))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate IOV buffer")
    if(NULL == (crt_order_attr_name_buf = DV_malloc(nattrs_shift * (sizeof(uint64_t) + 1))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate akey data buffer")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iods */
    for(i = 0; i < nattrs_shift; i++) {
        tmp_uint = idx_begin + i;

        /* Setup the integer 'name' value for the current 'creation order -> attribute name' akey */
        p = &crt_order_attr_name_buf[i * (sizeof(uint64_t) + 1)];
        *p++ = 0;
        UINT64ENCODE(p, tmp_uint);

        /* Set up iods for the current 'creation order -> attribute name' akey */
        memset(&iods[i], 0, sizeof(*iods));
        daos_iov_set(&iods[i].iod_name, &crt_order_attr_name_buf[i * (sizeof(uint64_t) + 1)], sizeof(uint64_t) + 1);
        iods[i].iod_nr = 1u;
        iods[i].iod_size = DAOS_REC_ANY;
        iods[i].iod_type = DAOS_IOD_SINGLE;
    } /* end for */

    /* Fetch the data size for each akey */
    if(0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, &dkey, (unsigned) nattrs_shift,
            iods, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read akey data sizes: %s", H5_daos_err_to_string(ret))

    /* Allocate buffers and setup sgls for each akey */
    for(i = 0; i < nattrs_shift; i++) {
        /* Allocate buffer for the current 'creation order -> attribute name' akey */
        if(iods[i].iod_size == 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADSIZE, FAIL, "invalid iod size - missing metadata")
        if(NULL == (tmp_buf = DV_malloc(iods[i].iod_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey data")

        /* Set up sgls for the current 'creation order -> attribute name' akey */
        daos_iov_set(&sg_iovs[i], tmp_buf, iods[i].iod_size);
        sgls[i].sg_nr = 1;
        sgls[i].sg_nr_out = 0;
        sgls[i].sg_iovs = &sg_iovs[i];
    } /* end for */

    /* Read the akey's data */
    if(0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, &dkey, (unsigned) nattrs_shift,
            iods, sgls, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read akey data: %s", H5_daos_err_to_string(ret))

    /*
     * Adjust the akeys down by setting their integer 'name' values to
     * one less than their original values
     */
    for(i = 0; i < nattrs_shift; i++) {
        /* Setup the integer 'name' value for the current 'creation order -> attribute name' akey */
        p = &crt_order_attr_name_buf[i * (sizeof(uint64_t) + 1) + 1];
        UINT64DECODE(p, tmp_uint);

        tmp_uint--;
        p = &crt_order_attr_name_buf[i * (sizeof(uint64_t) + 1) + 1];
        UINT64ENCODE(p, tmp_uint);
    } /* end for */

    /* Write the akeys back */
    if(0 != (ret = daos_obj_update(target_obj->obj_oh, DAOS_TX_NONE, &dkey, (unsigned) nattrs_shift,
            iods, sgls, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write akey data: %s", H5_daos_err_to_string(ret))

    /* Delete the (now invalid) akey at the end of the creation index */
    tmp_uint = idx_end;
    p = &crt_order_attr_name_buf[1];
    UINT64ENCODE(p, tmp_uint);
    daos_iov_set(&tail_akey, (void *)&crt_order_attr_name_buf[0], sizeof(uint64_t) + 1);

    if(0 != (ret = daos_obj_punch_akeys(target_obj->obj_oh, DAOS_TX_NONE, &dkey,
            1, &tail_akey, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "can't trim tail akey from attribute creation order index")

done:
    for(i = 0; i < nattrs_shift; i++) {
        if(sg_iovs[i].iov_buf)
            sg_iovs[i].iov_buf = DV_free(sg_iovs[i].iov_buf);
    } /* end for */
    if(crt_order_attr_name_buf)
        crt_order_attr_name_buf = DV_free(crt_order_attr_name_buf);
    if(sg_iovs)
        sg_iovs = DV_free(sg_iovs);
    if(sgls)
        sgls = DV_free(sgls);
    if(iods)
        iods = DV_free(iods);

    D_FUNC_LEAVE
} /* end H5_daos_attribute_shift_crt_idx_keys_down() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_exists
 *
 * Purpose:     Helper routine to check if an HDF5 attribute exists by
 *              attempting to read from its metadata keys.
 *
 * Return:      Success:        TRUE or FALSE
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5_daos_attribute_exists(H5_daos_obj_t *attr_container_obj, const char *attr_name)
{
    unsigned int nr;
    daos_iod_t iod[H5_DAOS_ATTR_NUM_AKEYS - 1]; /* attribute raw data key is excluded as it may not exist yet */
    daos_key_t dkey;
    hbool_t attr_exists = FALSE;
    hbool_t attr_missing = FALSE;
    size_t akey_len = 0;
    char *type_key = NULL;
    char *space_key = NULL;
    char *acpl_key = NULL;
    char *acorder_key = NULL;
    int ret;
    htri_t ret_value = FALSE;

    assert(attr_container_obj);
    assert(attr_name);

    if(attr_container_obj->ocpl_cache.track_acorder)
        nr = H5_DAOS_ATTR_NUM_AKEYS - 1;
    else
        nr = H5_DAOS_ATTR_NUM_AKEYS - 2;

    if(H5_daos_attribute_get_akey_strings(attr_name, &type_key, &space_key, &acpl_key,
            (attr_container_obj->ocpl_cache.track_acorder) ? &acorder_key : NULL, NULL, &akey_len) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't generate akey strings")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iods */
    memset(iod, 0, sizeof(iod));
    daos_iov_set(&iod[0].iod_name, (void *)type_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
    iod[0].iod_nr = 1u;
    iod[0].iod_type = DAOS_IOD_SINGLE;
    iod[0].iod_size = DAOS_REC_ANY;

    daos_iov_set(&iod[1].iod_name, (void *)space_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
    iod[1].iod_nr = 1u;
    iod[1].iod_type = DAOS_IOD_SINGLE;
    iod[1].iod_size = DAOS_REC_ANY;

    daos_iov_set(&iod[2].iod_name, (void *)acpl_key, (daos_size_t)akey_len);
    daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
    iod[2].iod_nr = 1u;
    iod[2].iod_type = DAOS_IOD_SINGLE;
    iod[2].iod_size = DAOS_REC_ANY;

    if(attr_container_obj->ocpl_cache.track_acorder) {
        daos_iov_set(&iod[3].iod_name, (void *)acorder_key, (daos_size_t)akey_len);
        daos_csum_set(&iod[3].iod_kcsum, NULL, 0);
        iod[3].iod_nr = 1u;
        iod[3].iod_type = DAOS_IOD_SINGLE;
        iod[3].iod_size = DAOS_REC_ANY;
    } /* end if */

    if(0 != (ret = daos_obj_fetch(attr_container_obj->obj_oh, DAOS_TX_NONE, &dkey,
            nr, iod, NULL, NULL, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "akey fetch for attribute '%s' failed: %s", attr_name, H5_daos_err_to_string(ret))

    /* Attribute exists if all of its metadata keys are present. */
    attr_exists = (iod[0].iod_size != 0)
               && (iod[1].iod_size != 0)
               && (iod[2].iod_size != 0);

    /*
     * Conversely, the attribute doesn't exist if all of its
     * metadata keys are missing.
     */
    attr_missing = (iod[0].iod_size == 0)
                && (iod[1].iod_size == 0)
                && (iod[2].iod_size == 0);

    /*
     * Check for the presence or absence of the attribute creation
     * order key when the attribute's parent object has attribute
     * creation order tracking enabled.
     */
    if(attr_container_obj->ocpl_cache.track_acorder) {
        attr_exists = attr_exists && (iod[3].iod_size != 0);
        attr_missing = attr_missing && (iod[3].iod_size == 0);
    } /* end if */

    if(attr_exists)
        D_GOTO_DONE(TRUE)
    else if(attr_missing)
        D_GOTO_DONE(FALSE)
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "attribute exists in inconsistent state (metadata missing)")

done:
    type_key = (char *)DV_free(type_key);
    space_key = (char *)DV_free(space_key);
    acpl_key = (char *)DV_free(acpl_key);
    acorder_key = (char *)DV_free(acorder_key);

    D_FUNC_LEAVE
} /* end H5_daos_attribute_exists() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate
 *
 * Purpose:     Iterates over the attributes attached to the target object,
 *              using the supplied iter_data struct for the iteration
 *              parameters.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner/Jordan Henderson
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_iterate(H5_daos_obj_t *attr_container_obj, H5_daos_iter_data_t *iter_data)
{
    herr_t ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(iter_data);
    assert(H5_DAOS_ITER_TYPE_ATTR == iter_data->iter_type);

    /* Iteration restart not supported */
    if(iter_data->idx_p && (*iter_data->idx_p != 0))
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "iteration restart not supported (must start from 0)")

    switch (iter_data->index_type) {
        case H5_INDEX_NAME:
            if((ret_value = H5_daos_attribute_iterate_by_name_order(attr_container_obj, iter_data)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration by name order failed")
            break;

        case H5_INDEX_CRT_ORDER:
            if((ret_value = H5_daos_attribute_iterate_by_crt_order(attr_container_obj, iter_data)) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration by creation order failed")
            break;

        case H5_INDEX_UNKNOWN:
        case H5_INDEX_N:
        default:
            D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "invalid or unsupported index type")
    } /* end switch */

done:
    D_FUNC_LEAVE
} /* end H5_daos_attribute_iterate() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_by_name_order
 *
 * Purpose:     Iterates over the attributes attached to the target object
 *              according to their alphabetical order. The supplied
 *              iter_data struct contains the iteration parameters.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_iterate_by_name_order(H5_daos_obj_t *attr_container_obj, H5_daos_iter_data_t *iter_data)
{
    H5VL_loc_params_t sub_loc_params;
    daos_key_desc_t kds[H5_DAOS_ITER_LEN];
    daos_anchor_t anchor;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    daos_key_t dkey;
    H5A_info_t ainfo;
    uint32_t nr;
    uint32_t i;
    size_t akey_buf_len = 0;
    herr_t op_ret;
    char *akey_buf = NULL;
    char *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(iter_data);
    assert(H5_INDEX_NAME == iter_data->index_type);
    assert(H5_ITER_NATIVE == iter_data->iter_order || H5_ITER_INC == iter_data->iter_order
            || H5_ITER_DEC == iter_data->iter_order);

    /* Native iteration order is currently associated with increasing order; decreasing order iteration is not currently supported */
    if(iter_data->iter_order == H5_ITER_DEC)
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "decreasing iteration order not supported (order must be H5_ITER_NATIVE or H5_ITER_INC)")

    /* Initialize sub_loc_params */
    sub_loc_params.obj_type = attr_container_obj->item.type;
    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name = ".";
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    /* Initialize const ainfo info */
    ainfo.corder_valid = FALSE;
    ainfo.corder = 0;
    ainfo.cset = H5T_CSET_ASCII;

    /* Initialize anchor */
    memset(&anchor, 0, sizeof(anchor));

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

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
        /* Loop to retrieve keys (exit as soon as we get at least 1 key) */
        H5_DAOS_RETRIEVE_KEYS_LOOP(akey_buf, akey_buf_len, sg_iov, H5E_ATTR, daos_obj_list_akey,
                attr_container_obj->obj_oh, DAOS_TX_NONE, &dkey, &nr, kds, &sgl, &anchor, NULL /*event*/);

        /* Loop over returned akeys */
        p = akey_buf;
        op_ret = 0;
        for(i = 0; (i < nr) && (op_ret == 0); i++) {
            /* Check for invalid key */
            if(kds[i].kd_key_len < 3)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "attribute akey too short")

            /* Only do callbacks for "S-" (dataspace) keys, to avoid duplication */
            if(p[0] == 'S') {
                char tmp_char;

                /* Add null terminator temporarily */
                tmp_char = p[kds[i].kd_key_len];
                p[kds[i].kd_key_len] = '\0';

                /* Retrieve attribute's info */
                if(H5_daos_attribute_get_info((H5_daos_item_t *)attr_container_obj, &sub_loc_params,
                        &p[2], &ainfo, iter_data->dxpl_id, iter_data->req) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute info")

                /* Make callback */
                if((op_ret = iter_data->u.attr_iter_data.attr_iter_op(iter_data->iter_root_obj, &p[2], &ainfo, iter_data->op_data)) < 0)
                    D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, op_ret, "operator function returned failure")

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

    /* Set return value */
    ret_value = op_ret;

done:
    akey_buf = (char *)DV_free(akey_buf);

    D_FUNC_LEAVE
} /* end H5_daos_attribute_iterate_by_name_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_by_crt_order
 *
 * Purpose:     Iterates over the attributes attached to the target object
 *              according to their attribute creation order values. The
 *              supplied iter_data struct contains the iteration
 *              parameters.
 *
 * Return:      Success:        SUCCEED or positive
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_iterate_by_crt_order(H5_daos_obj_t *attr_container_obj, H5_daos_iter_data_t *iter_data)
{
    H5VL_loc_params_t sub_loc_params;
    H5A_info_t ainfo;
    hssize_t obj_nattrs;
    uint64_t cur_idx;
    herr_t op_ret;
    size_t attr_name_buf_size = H5_DAOS_ATTR_NAME_BUF_SIZE;
    char *attr_name = NULL;
    char *attr_name_buf_dyn = NULL;
    char attr_name_buf_static[H5_DAOS_ATTR_NAME_BUF_SIZE];
    herr_t ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(iter_data);
    assert(H5_INDEX_CRT_ORDER == iter_data->index_type);
    assert(H5_ITER_NATIVE == iter_data->iter_order || H5_ITER_INC == iter_data->iter_order
            || H5_ITER_DEC == iter_data->iter_order);

    /* Check that creation order is tracked for the attribute's parent object */
    if(!attr_container_obj->ocpl_cache.track_acorder)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "creation order is not tracked for attribute's parent object")

    /* Retrieve the number of attributes attached to the target object */
    if((obj_nattrs = H5_daos_object_get_num_attrs(attr_container_obj)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get number of attributes attached to object")

    /* Check if there are no attributes to process */
    if(obj_nattrs == 0)
        D_GOTO_DONE(SUCCEED);

    /* Initialize sub_loc_params */
    sub_loc_params.obj_type = attr_container_obj->item.type;
    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name = ".";
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    /* Initialize const attribute info */
    ainfo.corder_valid = FALSE;
    ainfo.corder = 0;
    ainfo.cset = H5T_CSET_ASCII;

    attr_name = attr_name_buf_static;
    for(cur_idx = 0; cur_idx < (uint64_t)obj_nattrs; cur_idx++) {
        ssize_t attr_name_size;

        /* Retrieve the attribute's name length + the attribute's name if the buffer is large enough */
        if((attr_name_size = H5_daos_attribute_get_name_by_idx(attr_container_obj, iter_data->index_type,
                iter_data->iter_order, cur_idx, attr_name, attr_name_buf_size)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")

        /* Check if the buffer was large enough */
        if((size_t)attr_name_size > attr_name_buf_size - 1) {
            char *tmp_realloc;

            /*
             * Double the buffer size or re-allocate to fit the current
             * attribute's name, depending on which allocation is larger.
             */
            attr_name_buf_size = ((size_t)attr_name_size > (2 * attr_name_buf_size)) ?
                    (size_t)attr_name_size + 1 : (2 * attr_name_buf_size);

            if(NULL == (tmp_realloc = DV_realloc(attr_name_buf_dyn, attr_name_buf_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate attribute name buffer")
            attr_name = attr_name_buf_dyn = tmp_realloc;

            /* Re-issue the call to fetch the attribute's name with a larger buffer */
            if(H5_daos_attribute_get_name_by_idx(attr_container_obj, iter_data->index_type,
                    iter_data->iter_order, cur_idx, attr_name, attr_name_buf_size) < 0)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name")
        } /* end if */

        /* Retrieve the attribute's info */
        if(H5_daos_attribute_get_info((H5_daos_item_t *)attr_container_obj, &sub_loc_params,
                attr_name, &ainfo, iter_data->dxpl_id, iter_data->req) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute's info")

        /* Make callback */
        if((op_ret = iter_data->u.attr_iter_data.attr_iter_op(iter_data->iter_root_obj, attr_name, &ainfo, iter_data->op_data)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, op_ret, "operator function returned failure")

        /* Advance idx */
        if(iter_data->idx_p)
            (*iter_data->idx_p)++;
    } /* end for */

    ret_value = op_ret;

done:
    if(attr_name_buf_dyn)
        attr_name_buf_dyn = DV_free(attr_name_buf_dyn);

    D_FUNC_LEAVE
} /* end H5_daos_attribute_iterate_by_crt_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_iterate_count_attrs_cb
 *
 * Purpose:     A callback for H5_daos_attribute_iterate() that simply
 *              counts the number of attributes attached to the given
 *              object.
 *
 * Return:      0 (can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_attribute_iterate_count_attrs_cb(hid_t H5VL_DAOS_UNUSED loc_id, const char H5VL_DAOS_UNUSED *attr_name,
    const H5A_info_t H5VL_DAOS_UNUSED *attr_info, void *op_data)
{
    (*((uint64_t *) op_data))++;
    return 0;
} /* end H5_daos_attribute_iterate_count_attrs_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_rename
 *
 * Purpose:     Helper routine to rename an HDF5 attribute by recreating it
 *              under different akeys and deleting the old attribute.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_rename(H5_daos_obj_t *attr_container_obj, const char *cur_attr_name, const char *new_attr_name)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_attr_t *cur_attr = NULL;
    H5_daos_attr_t *new_attr = NULL;
    hssize_t attr_space_nelmts;
    size_t attr_type_size;
    void *attr_data_buf = NULL;
    herr_t ret_value = SUCCEED;

    assert(attr_container_obj);
    assert(cur_attr_name);
    assert(new_attr_name);

    /* Open the existing attribute */
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
    sub_loc_params.obj_type = H5I_ATTR;
    if(NULL == (cur_attr = (H5_daos_attr_t *)H5_daos_attribute_open(attr_container_obj, &sub_loc_params,
            cur_attr_name, H5P_DEFAULT, H5P_DEFAULT, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute")

    /* Create the new attribute */
    if(NULL == (new_attr = (H5_daos_attr_t *)H5_daos_attribute_create(attr_container_obj, &sub_loc_params,
            new_attr_name, cur_attr->type_id, cur_attr->space_id, cur_attr->acpl_id, H5P_DEFAULT,
            H5P_DEFAULT, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, FAIL, "can't create new attribute")

    /* Transfer data from the old attribute to the new attribute */
    if(0 == (attr_type_size = H5Tget_size(cur_attr->type_id)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve attribute's datatype size")

    if((attr_space_nelmts = H5Sget_simple_extent_npoints(cur_attr->space_id)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve number of elements in attribute's dataspace")

    if(NULL == (attr_data_buf = DV_malloc(attr_type_size * (size_t)attr_space_nelmts)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't allocate buffer for attribute data")

    if(H5_daos_attribute_read(cur_attr, cur_attr->type_id, attr_data_buf, H5P_DEFAULT, NULL) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read data from attribute")

    if(H5_daos_attribute_write(new_attr, new_attr->type_id, attr_data_buf, H5P_DEFAULT, NULL) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write data to attribute")

    /* Delete the old attribute */
    if(H5_daos_attribute_delete(attr_container_obj, &sub_loc_params, cur_attr_name) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "can't delete old attribute")

done:
    attr_data_buf = DV_free(attr_data_buf);

    if(new_attr) {
        if(H5_daos_attribute_close(new_attr, H5P_DEFAULT, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute")
            new_attr = NULL;
    }
    if(cur_attr) {
        if(H5_daos_attribute_close(cur_attr, H5P_DEFAULT, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute")
        cur_attr = NULL;
    }

    D_FUNC_LEAVE
} /* end H5_daos_attribute_rename() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_akey_strings
 *
 * Purpose:     Helper routine to generate the DAOS akey strings for an
 *              HDF5 attribute. The caller is responsible for freeing the
 *              memory allocated for each akey string.
 *
 *              TODO: just allocate one large buffer
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner/Jordan Henderson
 *              February, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_akey_strings(const char *attr_name, char **datatype_key_out, char **dataspace_key_out,
    char **acpl_key_out, char **acorder_key_out, char **raw_data_key_out, size_t *akey_len_out)
{
    size_t akey_len;
    char *type_key = NULL;
    char *space_key = NULL;
    char *acpl_key = NULL;
    char *acorder_key = NULL;
    char *data_key = NULL;
    herr_t ret_value = SUCCEED;

    assert(attr_name);

    akey_len = strlen(attr_name) + 2;

    if(datatype_key_out) {
        if(NULL == (type_key = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        type_key[0] = 'T';
        type_key[1] = '-';
        (void)strcpy(type_key + 2, attr_name);
    }
    if(dataspace_key_out) {
        if(NULL == (space_key = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        space_key[0] = 'S';
        space_key[1] = '-';
        (void)strcpy(space_key + 2, attr_name);
    }
    if(acpl_key_out) {
        if(NULL == (acpl_key = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        acpl_key[0] = 'P';
        acpl_key[1] = '-';
        (void)strcpy(acpl_key + 2, attr_name);
    }
    if(acorder_key_out) {
        if(NULL == (acorder_key = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        acorder_key[0] = 'C';
        acorder_key[1] = '-';
        (void)strcpy(acorder_key + 2, attr_name);
    }
    if(raw_data_key_out) {
        if(NULL == (data_key = (char *)DV_malloc(akey_len + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
        data_key[0] = 'V';
        data_key[1] = '-';
        (void)strcpy(data_key + 2, attr_name);
    }

    if(datatype_key_out)
        *datatype_key_out = type_key;
    if(dataspace_key_out)
        *dataspace_key_out = space_key;
    if(acpl_key_out)
        *acpl_key_out = acpl_key;
    if(acorder_key_out)
        *acorder_key_out = acorder_key;
    if(raw_data_key_out)
        *raw_data_key_out = data_key;
    if(akey_len_out)
        *akey_len_out = akey_len;

done:
    if(ret_value < 0) {
        type_key = (char *)DV_free(type_key);
        space_key = (char *)DV_free(space_key);
        acpl_key = (char *)DV_free(acpl_key);
        acorder_key = (char *)DV_free(acorder_key);
        data_key = (char *)DV_free(data_key);
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_akey_strings() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_idx
 *
 * Purpose:     Given an index type, index iteration order and index value,
 *              retrieves the name of the nth attribute (as specified by
 *              the index value) within the given index (name index or
 *              creation order index) according to the given order
 *              (increasing, decreasing or native order).
 *
 *              The attr_name_out parameter may be NULL, in which case the
 *              length of the attribute's name is simply returned. If
 *              non-NULL, the attribute's name is stored in attr_name_out.
 *
 * Return:      Success:        The length of the attribute's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5_daos_attribute_get_name_by_idx(H5_daos_obj_t *target_obj, H5_index_t index_type,
    H5_iter_order_t iter_order, uint64_t idx, char *attr_name_out, size_t attr_name_out_size)
{
    ssize_t ret_value = 0;

    assert(target_obj);

    if(H5_INDEX_CRT_ORDER == index_type) {
        if((ret_value = H5_daos_attribute_get_name_by_crt_order(target_obj, iter_order, idx, attr_name_out, attr_name_out_size)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "can't retrieve attribute name from creation order index")
    } /* end if */
    else if(H5_INDEX_NAME == index_type) {
        if((ret_value = H5_daos_attribute_get_name_by_name_order(target_obj, iter_order, idx, attr_name_out, attr_name_out_size)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "can't retrieve attribute name from name order index")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, (-1), "invalid or unsupported index type")

done:
    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_name_by_idx() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_crt_order
 *
 * Purpose:     Given an index iteration order and index value, retrieves
 *              the name of the nth attribute (as specified by the index
 *              value) within the specified object's attribute creation
 *              order index, according to the given order (increasing,
 *              decreasing or native order).
 *
 *              The attr_name_out parameter may be NULL, in which case the
 *              length of the attribute's name is simply returned. If
 *              non-NULL, the attribute's name is stored in attr_name_out.
 *
 * Return:      Success:        The length of the attribute's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5_daos_attribute_get_name_by_crt_order(H5_daos_obj_t *target_obj, H5_iter_order_t iter_order,
    uint64_t index, char *attr_name_out, size_t attr_name_out_size)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    hssize_t obj_nattrs;
    uint64_t fetch_idx = 0;
    uint8_t idx_buf[sizeof(uint64_t) + 1];
    uint8_t *p;
    int ret;
    ssize_t ret_value = 0;

    assert(target_obj);

    /* Check that creation order is tracked for the attribute's parent object */
    if(!target_obj->ocpl_cache.track_acorder)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, (-1), "creation order is not tracked for attribute's parent object")

    /* Retrieve the current number of attributes attached to the target object */
    if((obj_nattrs = H5_daos_object_get_num_attrs(target_obj)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "can't get number of attributes attached to object")

    /* Ensure the index is within range */
    if(index >= (uint64_t)obj_nattrs)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, (-1), "index value out of range")

    /* Calculate the correct index of the attribute, based upon the iteration order */
    if(H5_ITER_DEC == iter_order)
        fetch_idx = obj_nattrs - index - 1;
    else
        fetch_idx = index;

    p = idx_buf;
    *p++ = 0;
    UINT64ENCODE(p, fetch_idx);

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)idx_buf, sizeof(uint64_t) + 1);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl if attr_name_out buffer is supplied */
    if(attr_name_out) {
        daos_iov_set(&sg_iov, attr_name_out, attr_name_out_size - 1);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;
    } /* end if */

    /* Fetch the size of the attribute's name + attribute's name if attr_name_out is supplied */
    if(0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod,
            attr_name_out ? &sgl : NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, (-1), "can't fetch%s attribute's name: %s", attr_name_out ? "" : " size of", H5_daos_err_to_string(ret))

    if(iod.iod_size == (daos_size_t)0)
        D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, (-1), "attribute name record not found")

    if(attr_name_out)
        attr_name_out[MIN(iod.iod_size, attr_name_out_size - 1)] = '\0';

    ret_value = (ssize_t)iod.iod_size;

done:
    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_name_by_crt_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_name_order
 *
 * Purpose:     Given an index iteration order and index value, retrieves
 *              the name of the nth attribute (as specified by the index
 *              value) within the specified object's attribute name index,
 *              according to the given order (increasing, decreasing or
 *              native order).
 *
 *              The attr_name_out parameter may be NULL, in which case the
 *              length of the attribute's name is simply returned. If
 *              non-NULL, the attribute's name is stored in attr_name_out.
 *
 * Return:      Success:        The length of the attribute's name
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5_daos_attribute_get_name_by_name_order(H5_daos_obj_t *target_obj, H5_iter_order_t iter_order,
    uint64_t index, char *attr_name_out, size_t attr_name_out_size)
{
    H5_daos_attr_find_name_by_idx_ud_t iter_cb_ud;
    H5_daos_iter_data_t iter_data;
    hssize_t obj_nattrs;
    hid_t target_obj_id = H5I_INVALID_HID;
    ssize_t ret_value = 0;

    assert(target_obj);

    if(H5_ITER_DEC == iter_order)
        D_GOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, (-1), "decreasing order iteration is unsupported")

    /* Retrieve the current number of attributes attached to the target object */
    if((obj_nattrs = H5_daos_object_get_num_attrs(target_obj)) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "can't get number of attributes attached to object")

    /* Ensure the index is within range */
    if(index >= (uint64_t)obj_nattrs)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, (-1), "index value out of range")

    /* Register ID for target object */
    if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, (-1), "unable to atomize object handle")
    target_obj->item.rc++;

    /* Initialize iteration data */
    iter_cb_ud.target_attr_idx = index;
    iter_cb_ud.cur_attr_idx = 0;
    iter_cb_ud.attr_name_out = attr_name_out;
    iter_cb_ud.attr_name_out_size = attr_name_out_size;
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, iter_order,
            FALSE, NULL, target_obj_id, &iter_cb_ud, H5P_DATASET_XFER_DEFAULT, NULL);
    iter_data.u.attr_iter_data.attr_iter_op = H5_daos_attribute_get_name_by_name_order_cb;

    if(H5_daos_attribute_iterate(target_obj, &iter_data) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, (-1), "attribute iteration failed")

    ret_value = (ssize_t)iter_cb_ud.attr_name_out_size;

done:
    if((target_obj_id >= 0) && (H5Idec_ref(target_obj_id) < 0))
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, (-1), "can't close attribute's parent object")

    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_name_by_name_order() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_name_by_name_order_cb
 *
 * Purpose:     Attribute iteration callback for
 *              H5_daos_attribute_get_name_by_name_order which iterates
 *              through attributes by name order until the specified index
 *              value is reached, at which point the target attribute has
 *              been found and its name is copied back.
 *
 * Return:      Non-negative (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_name_by_name_order_cb(hid_t H5VL_DAOS_UNUSED loc_id, const char *attr_name,
    const H5A_info_t H5VL_DAOS_UNUSED *attr_info, void *op_data)
{
    H5_daos_attr_find_name_by_idx_ud_t *cb_ud = (H5_daos_attr_find_name_by_idx_ud_t *) op_data;

    /*
     * Once the target index has been reached, set the size of the attribute
     * name to be returned and copy the attribute name if the attribute name
     * output buffer is not NULL.
     */
    if(cb_ud->cur_attr_idx == cb_ud->target_attr_idx) {
        if(cb_ud->attr_name_out) {
            memcpy(cb_ud->attr_name_out, attr_name, cb_ud->attr_name_out_size - 1);
            cb_ud->attr_name_out[cb_ud->attr_name_out_size - 1] = '\0';
        }

        cb_ud->attr_name_out_size = strlen(attr_name);

        return 1;
    }

    cb_ud->cur_attr_idx++;
    return 0;
} /* end H5_daos_attribute_get_name_by_name_order_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_attribute_get_crt_order_by_name
 *
 * Purpose:     Retrieves the creation order value for an attribute given
 *              the attribute's parent object and the attribute's name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_attribute_get_crt_order_by_name(H5_daos_obj_t *target_obj, const char *attr_name,
    uint64_t *crt_order)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint64_t crt_order_val;
    uint8_t crt_order_buf[sizeof(uint64_t)];
    uint8_t *p;
    size_t akey_len;
    char *acorder_key = NULL;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(attr_name);
    assert(crt_order);

    /* Check that creation order is tracked for the attribute's parent object */
    if(!target_obj->ocpl_cache.track_acorder)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADVALUE, FAIL, "creation order is not tracked for attribute's parent object")

    /* Retrieve akey string for creation order */
    if(H5_daos_attribute_get_akey_strings(attr_name, NULL, NULL, NULL, &acorder_key, NULL, &akey_len) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTALLOC, FAIL, "can't generate creation order akey string")

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)acorder_key, (daos_size_t)akey_len);
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)sizeof(uint64_t);
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, crt_order_buf, (daos_size_t)sizeof(uint64_t));
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read attribute creation order value */
    if(0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read attribute's creation order value: %s", H5_daos_err_to_string(ret))

    if(iod.iod_size == 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, FAIL, "attribute creation order value record is missing")

    p = crt_order_buf;
    UINT64DECODE(p, crt_order_val);

    *crt_order = crt_order_val;

done:
    acorder_key = (char *)DV_free(acorder_key);

    D_FUNC_LEAVE
} /* end H5_daos_attribute_get_crt_order_by_name() */
