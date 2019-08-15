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

static herr_t H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info_t *obj_info_out);
static hssize_t H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj);
static herr_t H5_daos_group_copy(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_datatype_copy(H5_daos_dtype_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_dataset_copy(H5_daos_dset_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
    hid_t dxpl_id, void **req);

/* Data passed to link iteration callback when performing a group copy */
typedef struct group_copy_op_data {
    H5_daos_group_t *new_group;
    unsigned object_copy_opts;
    hid_t lcpl_id;
    hid_t dxpl_id;
    void **req;
} group_copy_op_data;


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
        /*
         * DSINC - This is not correct, but in the H5Acreate_by_name case HDF5 does not set
         * loc_params->loc_data.loc_by_name.lapl_id correctly so this will fail.
         */
        if(!collective /* && (H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_name.lapl_id) */)
            if(H5Pget_all_coll_metadata_ops(/*loc_params->loc_data.loc_by_name.lapl_id*/ H5P_LINK_ACCESS_DEFAULT, &collective) < 0)
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
            else {
                htri_t link_resolved;

                /* Follow link to object */
                if((link_resolved = H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &oid)) < 0)
                    D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, NULL, "can't follow link to group")
                if(!link_resolved)
                    D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, NULL, "link to group did not resolve")
            }

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
H5_daos_object_copy(void *_src_obj, const H5VL_loc_params_t *loc_params1,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *loc_params2,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t *src_obj = NULL;
    H5I_type_t src_obj_type;
    unsigned obj_copy_options;
    htri_t link_exists;
    herr_t ret_value = SUCCEED;

    if(!_src_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source location object is NULL")
    if(!loc_params1)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "first location parameters object is NULL")
    if(!src_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source object name is NULL")
    if(!dst_obj)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination location object is NULL")
    if(!loc_params2)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "second location parameters object is NULL")
    if(!dst_name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination object name is NULL")

    /*
     * First, ensure that the object doesn't currently exist at the specified destination
     * location object/destination name pair.
     */
    if((link_exists = H5_daos_link_exists((H5_daos_item_t *) dst_obj, dst_name, dxpl_id, req)) < 0)
        D_GOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "couldn't determine if link exists")
    if(link_exists)
        D_GOTO_ERROR(H5E_OHDR, H5E_ALREADYEXISTS, FAIL, "source object already exists at specified destination location object/destination name pair")

    /* Retrieve the object copy options. The following flags are
     * currently supported:
     *
     * H5O_COPY_SHALLOW_HIERARCHY_FLAG
     * H5O_COPY_WITHOUT_ATTR_FLAG
     * H5O_COPY_EXPAND_SOFT_LINK_FLAG
     *
     * DSINC - The following flags are currently unsupported:
     *
     *   H5O_COPY_EXPAND_EXT_LINK_FLAG
     *   H5O_COPY_EXPAND_REFERENCE_FLAG
     *   H5O_COPY_MERGE_COMMITTED_DTYPE_FLAG
     */
    if(H5Pget_copy_object(ocpypl_id, &obj_copy_options) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "failed to retrieve object copy options")

    /*
     * Open the source object
     */
    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.obj_type = loc_params1->obj_type;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    sub_loc_params.loc_data.loc_by_name.name = src_name;
    if(NULL == (src_obj = H5_daos_object_open(_src_obj, &sub_loc_params, &src_obj_type, dxpl_id, req)))
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "failed to open source object")

    switch(src_obj_type) {
        case H5I_FILE:
        case H5I_GROUP:
            if(H5_daos_group_copy((H5_daos_group_t *) src_obj, (H5_daos_group_t *) dst_obj, dst_name,
                    obj_copy_options, lcpl_id, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "failed to copy group")
            break;
        case H5I_DATATYPE:
            if(H5_daos_datatype_copy((H5_daos_dtype_t *) src_obj, (H5_daos_group_t *) dst_obj, dst_name,
                    obj_copy_options, lcpl_id, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "failed to copy datatype")
            break;
        case H5I_DATASET:
            if(H5_daos_dataset_copy((H5_daos_dset_t *) src_obj, (H5_daos_group_t *) dst_obj, dst_name,
                    obj_copy_options, lcpl_id, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failed to copy dataset")
            break;

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_DATASPACE:
        case H5I_MAP:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_NTYPES:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid object type")
    } /* end switch */

done:
    if(src_obj)
        if(H5_daos_object_close(src_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")

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
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_obj_t *target_obj = NULL;
    H5_daos_group_t *target_grp = NULL;
    hid_t target_obj_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    /* Determine target object */
    if(loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* Use item as target object, or the root group if item is a file */
        if(item->type == H5I_FILE)
            target_obj = (H5_daos_obj_t *)item->file->root_grp;
        else
            target_obj = (H5_daos_obj_t *)item;
        target_obj->item.rc++;
    } /* end if */
    else if(loc_params->type == H5VL_OBJECT_BY_NAME) {
        /*
         * Open target_obj. If H5Oexists_by_name is being called, skip doing
         * this since the path may point to a soft link that doesn't resolve.
         */
        if(H5VL_OBJECT_EXISTS != specific_type)
            if(NULL == (target_obj = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "can't open target object")
    } /* end else */
    else
        D_GOTO_ERROR(H5E_OHDR, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type")

    switch (specific_type) {
        /* H5Oincr_refcount/H5Odecr_refcount */
        case H5VL_OBJECT_CHANGE_REF_COUNT:
        {
            int mode = va_arg(arguments, int);

            if(mode > 0)
                target_obj->item.rc++;
            else if(mode < 0)
                target_obj->item.rc--;
            else
                D_GOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid reference count change mode")

            break;
        } /* H5VL_OBJECT_CHANGE_REF_COUNT */

        /* H5Oexists_by_name */
        case H5VL_OBJECT_EXISTS:
        {
            daos_obj_id_t oid;
            const char *obj_name;
            htri_t *ret = va_arg(arguments, htri_t *);

            /* Open group containing the link in question */
            if(NULL == (target_grp = (H5_daos_group_t *)H5_daos_group_traverse(item, loc_params->loc_data.loc_by_name.name,
                    dxpl_id, req, &obj_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "can't open group")

            /* Check if the link resolves */
            if((*ret = H5_daos_link_follow(target_grp, obj_name, strlen(obj_name), dxpl_id, req, &oid)) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, FAIL, "can't follow link to object")

            break;
        } /* H5VL_OBJECT_EXISTS */

        /* H5Ovisit(_by_name) */
        case H5VL_OBJECT_VISIT:
        {
            H5_daos_iter_data_t iter_data;
            H5_index_t idx_type = (H5_index_t) va_arg(arguments, int);
            H5_iter_order_t iter_order = (H5_iter_order_t) va_arg(arguments, int);
            H5O_iterate_t iter_op = va_arg(arguments, H5O_iterate_t);
            void *op_data = va_arg(arguments, void *);
            unsigned fields = va_arg(arguments, unsigned);

            /* Register id for target_obj */
            if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
                D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")

            /* Initialize iteration data */
            H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_OBJ, idx_type, iter_order,
                    FALSE, NULL, target_obj_id, op_data, dxpl_id, req);
            iter_data.u.obj_iter_data.fields = fields;
            iter_data.u.obj_iter_data.obj_iter_op = iter_op;
            iter_data.u.obj_iter_data.obj_name = ".";

            if((ret_value = H5_daos_object_visit(target_obj, &iter_data)) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "object visiting failed")

            break;
        } /* H5VL_OBJECT_VISIT */

        case H5VL_REF_CREATE:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object specific operation")

        /* H5Oflush */
        case H5VL_OBJECT_FLUSH:
        {
            switch(item->type) {
                case H5I_FILE:
                    if(H5_daos_file_flush((H5_daos_file_t *)item) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")
                    break;
                case H5I_GROUP:
                    if(H5_daos_group_flush((H5_daos_group_t *)item) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_WRITEERROR, FAIL, "can't flush group")
                    break;
                case H5I_DATASET:
                    if(H5_daos_dataset_flush((H5_daos_dset_t *)item) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't flush dataset")
                    break;
                case H5I_DATATYPE:
                    if(H5_daos_datatype_flush((H5_daos_dtype_t *)item) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_WRITEERROR, FAIL, "can't flush datatype")
                    break;
                default:
                    D_GOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid object type")
            } /* end switch */

            break;
        } /* H5VL_OBJECT_FLUSH */

        /* H5Orefresh */
        case H5VL_OBJECT_REFRESH:
        {
            switch(item->type) {
                case H5I_FILE:
                    if(H5_daos_group_refresh(item->file->root_grp, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_READERROR, FAIL, "failed to refresh file")
                    break;
                case H5I_GROUP:
                    if(H5_daos_group_refresh((H5_daos_group_t *)item, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_SYM, H5E_READERROR, FAIL, "failed to refresh group")
                    break;
                case H5I_DATASET:
                    if(H5_daos_dataset_refresh((H5_daos_dset_t *)item, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "failed to refresh dataset")
                    break;
                case H5I_DATATYPE:
                    if(H5_daos_datatype_refresh((H5_daos_dtype_t *)item, dxpl_id, req) < 0)
                        D_GOTO_ERROR(H5E_DATATYPE, H5E_READERROR, FAIL, "failed to refresh datatype")
                    break;
                default:
                    D_GOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid object type")
            } /* end switch */

            break;
        } /* H5VL_OBJECT_REFRESH */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported object specific operation")
    } /* end switch */

done:
    if(target_grp) {
        if(H5_daos_group_close(target_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        target_grp = NULL;
    } /* end if */

    if(target_obj_id >= 0) {
        if(H5Idec_ref(target_obj_id) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object ID")
        target_obj_id = H5I_INVALID_HID;
        target_obj = NULL;
    } /* end if */
    else if(target_obj) {
        if(H5_daos_object_close(target_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")
        target_obj = NULL;
    } /* end else */

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
                H5O_info_t *obj_info = va_arg(arguments, H5O_info_t *);
                unsigned fields = va_arg(arguments, unsigned);

                if(H5_daos_object_get_info(target_obj, fields, obj_info) < 0)
                    D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't retrieve info for object")

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


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit
 *
 * Purpose:     Helper routine to recursively visit the specified object
 *              and all objects accessible from the specified object,
 *              calling the supplied callback function on each object,
 *              when H5Ovisit(_by_name) is called.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_visit(H5_daos_obj_t *target_obj, H5_daos_iter_data_t *iter_data)
{
    H5O_info_t target_obj_info;
    herr_t op_ret = H5_ITER_CONT;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(iter_data);

    /*
     * Visit the specified target object first.
     */

    /* Retrieve the info of the target object */
    if(H5_daos_object_get_info(target_obj, iter_data->u.obj_iter_data.fields, &target_obj_info) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get info for object")

    /* Visit the object */
    if((op_ret = iter_data->u.obj_iter_data.obj_iter_op(iter_data->iter_root_obj, iter_data->u.obj_iter_data.obj_name, &target_obj_info, iter_data->op_data)) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_BADITER, op_ret, "operator function returned failure")

    /* If the object is a group, visit all objects below the group */
    if(H5I_GROUP == target_obj->item.type)
        if(H5_daos_link_iterate((H5_daos_group_t *) target_obj, iter_data) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "failed to iterate through group's links")

    ret_value = op_ret;

done:
    D_FUNC_LEAVE
} /* end H5_daos_object_visit() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_info
 *
 * Purpose:     Helper routine to retrieve the info for an object when
 *              H5Oget_info(_by_name/_by_idx) is called.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info_t *obj_info_out)
{
    hssize_t num_attrs = 0;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(obj_info_out);

    /*
     * Initialize object info - most fields are not valid and will
     * simply be set to 0.
     */
    memset(obj_info_out, 0, sizeof(*obj_info_out));

    /* Fill in fields of object info */

    /* Basic fields */
    if(fields & H5O_INFO_ALL) {
        uint64_t fileno64;
        uint8_t *uuid_p = (uint8_t *)&target_obj->item.file->uuid;

        /* Use the lower <sizeof(unsigned long)> bytes of the file uuid
         * as the fileno.  Ideally we would write separate 32 and 64 bit
         * hash functions but this should work almost as well. */
        UINT64DECODE(uuid_p, fileno64)
        obj_info_out->fileno = (unsigned long)fileno64;

        /* Use lower 64 bits of oid as address - contains encoded object
         * type */
        obj_info_out->addr = (haddr_t)target_obj->oid.lo;

        /* Set object type */
        switch(target_obj->item.type) {
            case H5I_GROUP:
                obj_info_out->type = H5O_TYPE_GROUP;
                break;
            case H5I_DATASET:
                obj_info_out->type = H5O_TYPE_DATASET;
                break;
            case H5I_DATATYPE:
                obj_info_out->type = H5O_TYPE_NAMED_DATATYPE;
                break;
#ifdef DV_HAVE_MAP
            case H5I_MAP:
                obj_info_out->type = H5O_TYPE_MAP;
                break;
#endif
            default:
                obj_info_out->type = H5O_TYPE_UNKNOWN;
                break;
        }

        /* Reference count is always 1 - change this when
         * H5Lcreate_hard() is implemented */
        obj_info_out->rc = 1;
    } /* end if */

    /* Set the number of attributes. */
    if(fields & H5O_INFO_NUM_ATTRS) {
        if((num_attrs = H5_daos_object_get_num_attrs(target_obj)) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve the number of attributes attached to object")
        obj_info_out->num_attrs = (hsize_t)num_attrs;
    } /* end if */

    /* Investigate collisions with links, etc DSINC */

done:
    D_FUNC_LEAVE
} /* end H5_daos_object_get_info() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_get_num_attrs
 *
 * Purpose:     Helper routine to retrieve the number of attributes
 *              attached to a given object.
 *
 * Return:      Success:        The number of attributes attached to the
 *                              given object.
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static hssize_t
H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj)
{
    daos_key_desc_t kds[H5_DAOS_ITER_LEN];
    daos_sg_list_t sgl;
    daos_anchor_t anchor;
    daos_iov_t sg_iov;
    daos_key_t dkey;
    uint32_t nr;
    uint32_t i;
    size_t akey_buf_len = 0;
    char *akey_buf = NULL;
    char *p;
    hssize_t ret_value = 0;

    /* Initialize anchor */
    memset(&anchor, 0, sizeof(anchor));

    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Allocate akey_buf */
    if(NULL == (akey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akeys")
    akey_buf_len = H5_DAOS_ITER_SIZE_INIT;

    /* Set up sgl */
    daos_iov_set(&sg_iov, akey_buf, (daos_size_t)akey_buf_len);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Loop to retrieve keys and make callbacks */
    do {
        /* Loop to retrieve keys (exit as soon as we get at least 1 key) */
        do {
            int ret;

            /* Reset nr */
            nr = H5_DAOS_ITER_LEN;

            /* Ask daos for a list of akeys, break out if we succeed */
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
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't list attributes: %s", H5_daos_err_to_string(ret))
        } while(1);

        /* Count number of returned attributes */
        p = akey_buf;
        for(i = 0; i < nr; i++) {
            /* Check for invalid key */
            if(kds[i].kd_key_len < 3)
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "attribute akey too short")
            if(p[1] != '-')
                D_GOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "invalid attribute akey format")

            /* Only count for "S-" (dataspace) keys, to avoid duplication */
            if(p[0] == 'S')
                ret_value++;

            /* Advance to next akey */
            p += kds[i].kd_key_len + kds[i].kd_csum_len;
        } /* end for */
    } while(!daos_anchor_is_eof(&anchor));

done:
    akey_buf = (char *)DV_free(akey_buf);

    D_FUNC_LEAVE
} /* end H5_daos_object_get_num_attrs() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy_cb
 *
 * Purpose:     Helper routine to deal with the copying of a single link
 *              during a group copy. Objects pointed to by hard links will
 *              be copied to the destination (copy) group.
 *
 *              When dealing with soft links, the following will happen:
 *
 *                  - If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was
 *                    specified as part of the object copy options for
 *                    H5Ocopy, soft links will be followed and the objects
 *                    they point to will become new objects in the
 *                    destination (copy) group.
 *                  - If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was not
 *                    specified, soft links will be directly copied to the
 *                    destination group and will have the same link value
 *                    as the original link.
 *
 *              DSINC - Expansion of external links is currently not
 *              supported and they will simply be directly copied to the
 *              destination group, similar to soft link copying when the
 *              H5O_COPY_EXPAND_SOFT_LINK_FLAG flag is not specified.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_copy_cb(hid_t group, const char *name,
    const H5L_info_t *info, void *op_data)
{
    group_copy_op_data *copy_op_data = (group_copy_op_data *) op_data;
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t *grp_obj = NULL;
    H5_daos_obj_t *target_obj = NULL;
    herr_t ret_value = H5_ITER_CONT;

    if(NULL == (grp_obj = H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for group")

    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name = name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    switch (info->type) {
        case H5L_TYPE_HARD:
        {
            /* Open the target object */
            if(NULL == (target_obj = H5_daos_object_open(grp_obj, &sub_loc_params, NULL,
                    copy_op_data->dxpl_id, copy_op_data->req)))
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open object")

            /* TODO: Copy the object */

            break;
        } /* H5L_TYPE_HARD */

        case H5L_TYPE_SOFT:
        {
            if (copy_op_data->object_copy_opts & H5O_COPY_EXPAND_SOFT_LINK_FLAG) {
                /* TODO: Copy the object */
            } /* end if */
            else {
                /* Copy the link as is */
                if(H5_daos_link_copy(grp_obj, &sub_loc_params, copy_op_data->new_group, &sub_loc_params,
                        copy_op_data->lcpl_id, H5P_LINK_ACCESS_DEFAULT, copy_op_data->dxpl_id, copy_op_data->req) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy link")
            } /* end else */

            break;
        } /* H5L_TYPE_SOFT */

        case H5L_TYPE_EXTERNAL:
        {
            if (copy_op_data->object_copy_opts & H5O_COPY_EXPAND_EXT_LINK_FLAG) {
                /* TODO: Copy the object */
            } /* end if */
            else {
                /* Copy the link as is */
                if(H5_daos_link_copy(grp_obj, &sub_loc_params, copy_op_data->new_group, &sub_loc_params,
                        copy_op_data->lcpl_id, H5P_LINK_ACCESS_DEFAULT, copy_op_data->dxpl_id, copy_op_data->req) < 0)
                    D_GOTO_ERROR(H5E_LINK, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy link")
            } /* end else */

            break;
        } /* H5L_TYPE_EXTERNAL */

        case H5L_TYPE_MAX:
        case H5L_TYPE_ERROR:
        default:
            D_GOTO_ERROR(H5E_LINK, H5E_BADVALUE, H5_ITER_ERROR, "invalid link type")
    } /* end switch */

done:
    D_FUNC_LEAVE
} /* end H5_daos_group_copy_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_copy
 *
 * Purpose:     Helper routine to copy a specified group to the given
 *              location specified by the dst_obj/dst_name pair. The new
 *              group's name is specified by the base portion of dst_name.
 *
 *              Copying of certain parts of the group, such as its
 *              attributes, is controlled by the passed in object copy
 *              options.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_group_copy(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5_daos_iter_data_t iter_data;
    group_copy_op_data copy_op_data;
    H5VL_loc_params_t dest_loc_params;
    H5_daos_group_t *new_group = NULL;
    hid_t target_group_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /* Copy the group */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (new_group = H5_daos_group_create(dst_obj, &dest_loc_params, dst_name, lcpl_id,
            src_obj->gcpl_id, src_obj->gapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "failed to create new group")

    /* Register an ID for the group to iterate over */
    if((target_group_id = H5VLwrap_register(src_obj, H5I_GROUP)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
    src_obj->obj.item.rc++;

    /* Setup group copying op_data to pass to the link iteration callback function */
    copy_op_data.new_group = new_group;
    copy_op_data.object_copy_opts = obj_copy_options;
    copy_op_data.lcpl_id = lcpl_id;
    copy_op_data.dxpl_id = dxpl_id;
    copy_op_data.req = req;

    /* Initialize iteration data */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, H5_INDEX_CRT_ORDER, H5_ITER_INC,
            (obj_copy_options & H5O_COPY_SHALLOW_HIERARCHY_FLAG) ? 0 : 1, NULL,
            target_group_id, &copy_op_data, dxpl_id, req);
    iter_data.u.link_iter_data.link_iter_op = H5_daos_group_copy_cb;

    /* Copy the immediate members of the group. If the H5O_COPY_SHALLOW_HIERARCHY_FLAG wasn't
     * specified, this will also recursively copy the members of any groups found. */
    if(H5_daos_link_iterate(src_obj, &iter_data) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "failed to iterate over group's links")

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the group's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) new_group, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "failed to copy group's attributes")

done:
    if(new_group) {
        if(H5_daos_group_close(new_group, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")
        new_group = NULL;
    } /* end if */

    if(target_group_id >= 0)
        if(H5Idec_ref(target_group_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group ID")

    D_FUNC_LEAVE
} /* end H5_daos_group_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_datatype_copy
 *
 * Purpose:     Helper routine to copy a specified committed datatype to
 *              the given location specified by the dst_obj/dst_name pair.
 *              The new committed datatype's name is specified by the base
 *              portion of dst_name.
 *
 *              Copying of certain parts of the committed datatype, such as
 *              its attributes, is controlled by the passed in object copy
 *              options.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_datatype_copy(H5_daos_dtype_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t dest_loc_params;
    H5_daos_dtype_t *new_dtype = NULL;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /*
     * Copy the datatype
     */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (new_dtype = H5_daos_datatype_commit(dst_obj, &dest_loc_params, dst_name, src_obj->type_id,
            lcpl_id, src_obj->tcpl_id, src_obj->tapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "failed to commit new datatype")

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the datatype's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) new_dtype, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "failed to copy datatype's attributes")

done:
    if(new_dtype) {
        if(H5_daos_datatype_close(new_dtype, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close datatype")
        new_dtype = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_datatype_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_copy
 *
 * Purpose:     Helper routine to copy a specified dataset to the given
 *              location specified by the dst_obj/dst_name pair. The new
 *              dataset's name is specified by the base portion of
 *              dst_name.
 *
 *              Copying of certain parts of the dataset, such as its
 *              attributes, is controlled by the passed in object copy
 *              options.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_copy(H5_daos_dset_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t dest_loc_params;
    H5_daos_dset_t *new_dset = NULL;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /*
     * Copy the dataset
     */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (new_dset = H5_daos_dataset_create(dst_obj, &dest_loc_params, dst_name, lcpl_id,
            src_obj->type_id, src_obj->space_id, src_obj->dcpl_id, src_obj->dapl_id,
            dxpl_id, req)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failed to create new dataset")

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the dataset's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) new_dset, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failed to copy dataset's attributes")

done:
    if(new_dset) {
        if(H5_daos_dataset_close(new_dset, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataset")
        new_dset = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_dataset_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_attributes_cb
 *
 * Purpose:     Attribute iteration callback to copy a single attribute
 *              from one DAOS object to another.
 *
 *              DSINC - currently no provision for dxpl_id or req.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_attributes_cb(hid_t location_id, const char *attr_name,
    const H5A_info_t *ainfo, void *op_data)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_obj_t *src_loc_obj = NULL;
    H5_daos_obj_t *destination_obj = (H5_daos_obj_t *)op_data;
    H5_daos_attr_t *cur_attr = NULL;
    H5_daos_attr_t *new_attr = NULL;
    herr_t ret_value = H5_ITER_CONT;

    sub_loc_params.obj_type = H5I_ATTR;
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;

    if(NULL == (src_loc_obj = H5VLobject(location_id)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for source location ID")

    if(NULL == (cur_attr = H5_daos_attribute_open(src_loc_obj, &sub_loc_params, attr_name,
            H5P_ATTRIBUTE_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open attribute")

    if(NULL == (new_attr = H5_daos_attribute_create(destination_obj, &sub_loc_params,
            attr_name, cur_attr->type_id, cur_attr->space_id, cur_attr->acpl_id,
            H5P_ATTRIBUTE_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, NULL)))
        D_GOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, H5_ITER_ERROR, "failed to create new attribute")

done:
    if(new_attr)
        if(H5_daos_attribute_close(new_attr, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTCLOSEOBJ, H5_ITER_ERROR, "failed to close attribute")
    if(cur_attr)
        if(H5_daos_attribute_close(cur_attr, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CANTCLOSEOBJ, H5_ITER_ERROR, "failed to close attribute")

    D_FUNC_LEAVE
} /* end H5_daos_object_copy_attributes_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_attributes
 *
 * Purpose:     Helper routine to copy all of the attributes from a given
 *              object to the specified destination object.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_attributes(H5_daos_obj_t *src_obj, H5_daos_obj_t *dst_obj,
    hid_t dxpl_id, void **req)
{
    H5_daos_iter_data_t iter_data;
    hid_t target_obj_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);

    /* Register ID for source object */
    if((target_obj_id = H5VLwrap_register(src_obj, src_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
    src_obj->item.rc++;

    /* Initialize iteration data */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, H5_ITER_INC,
            FALSE, NULL, target_obj_id, dst_obj, dxpl_id, req);
    iter_data.u.attr_iter_data.attr_iter_op = H5_daos_object_copy_attributes_cb;

    if(H5_daos_attribute_iterate(src_obj, &iter_data, dxpl_id, req) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "failed to iterate over object's attributes")

done:
    if(target_obj_id >= 0)
        if(H5Idec_ref(target_obj_id) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object ID")

    D_FUNC_LEAVE
} /* end H5_daos_object_copy_attributes() */
