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
 *
 * DSINC - Major error codes used within this file and for H5O-related calls
 *         should probably be a code other than H5O_OHDR, as this relates to
 *         object headers.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

static herr_t H5_daos_object_open_by_token(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *opened_obj_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_object_open_by_name(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *opened_obj_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_object_open_by_idx(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *opened_obj_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info_t *info, void *op_data);
static herr_t H5_daos_object_get_info(H5_daos_obj_t *target_obj, unsigned fields, H5O_info_t *obj_info_out);
static herr_t H5_daos_object_copy_helper(H5_daos_obj_t *src_obj, H5I_type_t src_obj_type,
    H5_daos_group_t *dst_obj, const char *dst_name, unsigned obj_copy_options,
    hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5_daos_group_copy(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
static H5_daos_group_t *H5_daos_group_copy_helper(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj,
    const char *dst_name, unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req);
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
    H5VL_loc_params_t sub_loc_params;
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_obj_t *obj = NULL;
    daos_obj_id_t oid = {0};
    H5VL_token_t obj_token;
    H5I_type_t obj_type = H5I_UNINIT;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")

    /* Retrieve the OID of the target object */
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_NAME:
            if(H5_daos_object_open_by_name((H5_daos_obj_t *)item, loc_params, &oid, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open object by name")
            break;

        case H5VL_OBJECT_BY_IDX:
            if(H5_daos_object_open_by_idx((H5_daos_obj_t *)item, loc_params, &oid, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open object by index")
            break;

        case H5VL_OBJECT_BY_TOKEN:
            if(H5_daos_object_open_by_token((H5_daos_obj_t *)item, loc_params, &oid, dxpl_id, req) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open object by token")
            break;

        case H5VL_OBJECT_BY_SELF:
        default:
            D_GOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "invalid loc_params type")
    } /* end switch */

    /* Get object type */
    if(H5I_BADID == (obj_type = H5_daos_oid_to_type(oid)))
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "can't get object type")

    /* Setup object token */
    if(H5_daos_oid_to_token(oid, &obj_token) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTENCODE, NULL, "can't convert object OID to token")

    /* Set up sub_loc_params */
    sub_loc_params.obj_type = item->type;
    sub_loc_params.type = H5VL_OBJECT_BY_TOKEN;
    sub_loc_params.loc_data.loc_by_token.token = obj_token;

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
        assert(obj_type == H5I_MAP);
        if(NULL == (obj = (H5_daos_obj_t *)H5_daos_map_open(item, &sub_loc_params, NULL,
                ((H5VL_OBJECT_BY_NAME == loc_params->type) && (loc_params->loc_data.loc_by_name.lapl_id != H5P_DEFAULT))
                ? loc_params->loc_data.loc_by_name.lapl_id : H5P_MAP_ACCESS_DEFAULT, dxpl_id, req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "can't open map")
    } /* end if */

    /* Set return value */
    if(opened_type)
        *opened_type = obj_type;
    ret_value = (void *)obj;

done:
    if(obj && (NULL == ret_value))
        if(H5_daos_object_close(obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, NULL, "can't close object")

    D_FUNC_LEAVE_API
} /* end H5_daos_object_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_by_token
 *
 * Purpose:     Helper function for H5_daos_object_open which opens an
 *              object according to the specified object token.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_open_by_token(H5_daos_obj_t H5VL_DAOS_UNUSED *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *opened_obj_id, hid_t H5VL_DAOS_UNUSED dxpl_id, void H5VL_DAOS_UNUSED **req)
{
    daos_obj_id_t oid;
    herr_t ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(opened_obj_id);
    assert(H5VL_OBJECT_BY_TOKEN == loc_params->type);

    /* Generate OID from object token */
    if(H5_daos_token_to_oid(loc_params->loc_data.loc_by_token.token, &oid) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "can't convert address token to OID")

    *opened_obj_id = oid;

done:
    D_FUNC_LEAVE
} /* end H5_daos_object_open_by_token() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_by_name
 *
 * Purpose:     Helper function for H5_daos_object_open which opens an
 *              object according to the specified object name.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_open_by_name(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *opened_obj_id, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *target_grp = NULL;
    daos_obj_id_t oid;
    const char *target_name = NULL;
    uint8_t oid_buf[2 * sizeof(uint64_t)];
    uint8_t *p;
    hbool_t must_bcast = FALSE;
    hbool_t collective;
    herr_t ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(opened_obj_id);
    assert(H5VL_OBJECT_BY_NAME == loc_params->type);

    /*
     * Like HDF5, metadata reads are independent by default. If the application has specifically
     * requested collective metadata reads, they will be enabled here.
     */
    collective = loc_obj->item.file->fapl_cache.is_collective_md_read;

    /*
     * Check for collective access, if not already set by the file
     *
     * DSINC - In the H5Acreate_by_name case HDF5 does not set
     * loc_params->loc_data.loc_by_name.lapl_id correctly, so this can fail
     * and has therefore been commented out for now.
     */
    if(!collective /* && (H5P_LINK_ACCESS_DEFAULT != loc_params->loc_data.loc_by_name.lapl_id) */)
        if(H5Pget_all_coll_metadata_ops(/*loc_params->loc_data.loc_by_name.lapl_id*/ H5P_LINK_ACCESS_DEFAULT, &collective) < 0)
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get collective metadata reads property")

    /*
     * Check if we're actually opening the group or just receiving the group
     * info from the leader
     */
    if(!collective || (loc_obj->item.file->my_rank == 0)) {
        if(collective && (loc_obj->item.file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for simple case of '.' for object name */
        if(!strncmp(loc_params->loc_data.loc_by_name.name, ".", 2)) {
            if(loc_obj->item.type == H5I_FILE)
                oid = ((H5_daos_file_t *)loc_obj)->root_grp->obj.oid;
            else
                oid = loc_obj->oid;
        } /* end if */
        else {
            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse((H5_daos_item_t *)loc_obj, loc_params->loc_data.loc_by_name.name,
                    dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, FAIL, "can't traverse path")

            /* Check for no target_name, in this case just reopen target_grp */
            if(target_name[0] == '\0'
                    || (target_name[0] == '.' && target_name[1] == '\0'))
                oid = target_grp->obj.oid;
            else {
                htri_t link_resolved;

                /* Follow link to object */
                if((link_resolved = H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &oid)) < 0)
                    D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, FAIL, "can't follow link to group")
                if(!link_resolved)
                    D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, FAIL, "link to group did not resolve")
            } /* end else */
        } /* end else */

        /* Broadcast group info if there are other processes that need it */
        if(collective && (loc_obj->item.file->num_procs > 1)) {
            /* Encode oid */
            p = oid_buf;
            UINT64ENCODE(p, oid.lo)
            UINT64ENCODE(p, oid.hi)

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast oid_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)oid_buf, sizeof(oid_buf), MPI_BYTE, 0, loc_obj->item.file->comm))
                D_GOTO_ERROR(H5E_OHDR, H5E_MPI, FAIL, "can't broadcast object ID")
        } /* end if */
    } /* end if */
    else {
        /* Receive oid_buf */
        if(MPI_SUCCESS != MPI_Bcast((char *)oid_buf, sizeof(oid_buf), MPI_BYTE, 0, loc_obj->item.file->comm))
            D_GOTO_ERROR(H5E_OHDR, H5E_MPI, FAIL, "can't receive broadcasted object ID")

        /* Decode oid */
        p = oid_buf;
        UINT64DECODE(p, oid.lo)
        UINT64DECODE(p, oid.hi)

        /* Check for oid.lo set to 0 - indicates failure */
        if(oid.lo == 0)
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "lead process failed to open object")
    } /* end else */

    *opened_obj_id = oid;

done:
    /* Cleanup on failure */
    if(ret_value < 0 && must_bcast) {
        /*
         * Bcast oid_buf as '0' if necessary - this will trigger
         * failures in other processes.
         */
        memset(oid_buf, 0, sizeof(oid_buf));
        if(MPI_SUCCESS != MPI_Bcast(oid_buf, sizeof(oid_buf), MPI_BYTE, 0, loc_obj->item.file->comm))
            D_DONE_ERROR(H5E_OHDR, H5E_MPI, FAIL, "can't broadcast empty object ID")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_object_open_by_name() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_open_by_idx
 *
 * Purpose:     Helper function for H5_daos_object_open which opens an
 *              object according to creation order or alphabetical order.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_open_by_idx(H5_daos_obj_t *loc_obj, const H5VL_loc_params_t *loc_params,
    daos_obj_id_t *opened_obj_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t sub_loc_params;
    H5_daos_group_t *container_group = NULL;
    daos_obj_id_t oid;
    ssize_t link_name_size;
    htri_t link_resolved;
    char *link_name = NULL;
    char *link_name_buf_dyn = NULL;
    char link_name_buf_static[H5_DAOS_LINK_NAME_BUF_SIZE];
    herr_t ret_value = SUCCEED;

    assert(loc_obj);
    assert(loc_params);
    assert(opened_obj_id);
    assert(H5VL_OBJECT_BY_IDX == loc_params->type);

    /* Open the group containing the target object */
    sub_loc_params.type = H5VL_OBJECT_BY_SELF;
    sub_loc_params.obj_type = H5I_GROUP;
    if(NULL == (container_group = (H5_daos_group_t *)H5_daos_group_open(loc_obj, &sub_loc_params,
            loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.lapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "can't open group containing target object")

    /* Retrieve the name of the link at the given index */
    link_name = link_name_buf_static;
    if((link_name_size = H5_daos_link_get_name_by_idx(container_group, loc_params->loc_data.loc_by_idx.idx_type,
            loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
            link_name, H5_DAOS_LINK_NAME_BUF_SIZE)) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get link name")

    /* Check that buffer was large enough to fit link name */
    if(link_name_size > H5_DAOS_LINK_NAME_BUF_SIZE - 1) {
        if(NULL == (link_name_buf_dyn = DV_malloc((size_t)link_name_size + 1)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link name buffer")
        link_name = link_name_buf_dyn;

        /* Re-issue the call with a larger buffer */
        if(H5_daos_link_get_name_by_idx(container_group, loc_params->loc_data.loc_by_idx.idx_type,
                loc_params->loc_data.loc_by_idx.order, (uint64_t)loc_params->loc_data.loc_by_idx.n,
                link_name, (size_t)link_name_size + 1) < 0)
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get link name")
    } /* end if */

    /* Attempt to follow the link */
    if((link_resolved = H5_daos_link_follow(container_group, link_name, strlen(link_name), dxpl_id, req, &oid)) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, FAIL, "can't follow link to object")
    if(!link_resolved)
        D_GOTO_ERROR(H5E_OHDR, H5E_TRAVERSE, FAIL, "link to object did not resolve")

    *opened_obj_id = oid;

done:
    if(link_name_buf_dyn)
        link_name_buf_dyn = DV_free(link_name_buf_dyn);
    if(container_group && H5_daos_group_close(container_group, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_object_open_by_idx() */


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

    /* Perform the object copy */
    if(H5_daos_object_copy_helper(src_obj, src_obj_type, dst_obj, dst_name, obj_copy_options,
            lcpl_id, dxpl_id, req) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "failed to copy object")

done:
    if(src_obj)
        if(H5_daos_object_close(src_obj, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object")

    D_FUNC_LEAVE_API
} /* end H5_daos_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_copy_helper
 *
 * Purpose:     Helper routine for H5_daos_object_copy that calls the
 *              appropriate copying routine based upon the object type of
 *              the object being copied. This routine separates out the
 *              copying logic so that recursive group copying can re-use
 *              it.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Jordan Henderson
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_copy_helper(H5_daos_obj_t *src_obj, H5I_type_t src_obj_type,
    H5_daos_group_t *dst_obj, const char *dst_name, unsigned obj_copy_options,
    hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    herr_t ret_value = SUCCEED;

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
        case H5I_MAP:
            /* TODO: Add map copying support */
            D_GOTO_ERROR(H5E_MAP, H5E_UNSUPPORTED, H5_ITER_ERROR, "map copying is unsupported")
            break;

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_DATASPACE:
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
    D_FUNC_LEAVE
} /* end H5_daos_object_copy_helper() */


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
    H5_daos_item_t *item = (H5_daos_item_t *) item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location parameters object is NULL")

    switch (get_type) {
        case H5VL_OBJECT_GET_NAME:
        case H5VL_OBJECT_GET_TYPE:
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

        case H5VL_OBJECT_LOOKUP:
        {
            void *token = va_arg(arguments, void *);

            if(H5VL_OBJECT_BY_NAME != loc_params->type)
                D_GOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "invalid loc_params type")

            if(H5_daos_oid_to_token(target_obj->oid, (H5VL_token_t *)&token) < 0)
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTENCODE, FAIL, "can't convert OID to object token")

            break;
        } /* H5VL_OBJECT_LOOKUP */

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
    switch (loc_params->type) {
        case H5VL_OBJECT_BY_SELF:
            /* Use item as attribute parent object, or the root group if item is a file */
            if(item->type == H5I_FILE)
                target_obj = (H5_daos_obj_t *)((H5_daos_file_t *)item)->root_grp;
            else
                target_obj = (H5_daos_obj_t *)item;

            target_obj->item.rc++;
            break;

        case H5VL_OBJECT_BY_NAME:
        case H5VL_OBJECT_BY_IDX:
            /* Open target object */
            if(NULL == (target_obj = (H5_daos_obj_t *)H5_daos_object_open(item, loc_params, NULL, dxpl_id, req)))
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "can't open object")
            break;

        default:
            D_GOTO_ERROR(H5E_OHDR, H5E_UNSUPPORTED, FAIL, "unsupported object operation location parameters type")
    } /* end switch */

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
    else if(obj->item.type == H5I_ATTR) {
        if(H5_daos_attribute_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute")
    } /* end if */
    else if(obj->item.type == H5I_MAP) {
        if(H5_daos_map_close(obj, dxpl_id, req))
            D_GOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "can't close map")
    } /* end if */
    else
        assert(0 && "Invalid object type");

done:
    D_FUNC_LEAVE
} /* end H5_daos_object_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_fill_ocpl_cache
 *
 * Purpose:     Fills the "ocpl_cache" field of the object struct, using
 *              the object's OCPL.  Assumes obj->ocpl_cache has been
 *              initialized to all zeros.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_fill_ocpl_cache(H5_daos_obj_t *obj, hid_t ocpl_id)
{
    unsigned acorder_flags;
    herr_t ret_value = SUCCEED;

    assert(obj);

    /* Determine if this object is tracking attribute creation order */
    if(H5Pget_attr_creation_order(ocpl_id, &acorder_flags) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "can't get attribute creation order flags")
    assert(!obj->ocpl_cache.track_acorder);
    if(acorder_flags & H5P_CRT_ORDER_TRACKED)
        obj->ocpl_cache.track_acorder = TRUE;

done:
    D_FUNC_LEAVE
} /* end H5_daos_fill_ocpl_cache() */


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

    /* Retrieve the info of the target object */
    if(H5_daos_object_get_info(target_obj, iter_data->u.obj_iter_data.fields, &target_obj_info) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get info for object")

    /* Visit the specified target object first */
    if((op_ret = iter_data->u.obj_iter_data.obj_iter_op(iter_data->iter_root_obj, iter_data->u.obj_iter_data.obj_name, &target_obj_info, iter_data->op_data)) < 0)
        D_GOTO_ERROR(H5E_OHDR, H5E_BADITER, op_ret, "operator function returned failure")

    /* If the object is a group, visit all objects below the group */
    if(H5I_GROUP == target_obj->item.type) {
        H5_daos_iter_data_t sub_iter_data;

        /*
         * Initialize the link iteration data with all of the fields from
         * the passed in object iteration data, with the exception that the
         * link iteration data's is_recursive field is set to FALSE. The link
         * iteration data's op_data will be a pointer to the passed in
         * object iteration data so that the correct object iteration callback
         * operator function can be called for each link during H5_daos_link_iterate().
         */
        H5_DAOS_ITER_DATA_INIT(sub_iter_data, H5_DAOS_ITER_TYPE_LINK, iter_data->index_type, iter_data->iter_order,
                FALSE, iter_data->idx_p, iter_data->iter_root_obj, iter_data, iter_data->dxpl_id, iter_data->req);
        sub_iter_data.u.link_iter_data.link_iter_op = H5_daos_object_visit_link_iter_cb;

        if(H5_daos_link_iterate((H5_daos_group_t *) target_obj, &sub_iter_data) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "failed to iterate through group's links")
    } /* end if */

    ret_value = op_ret;

done:
    D_FUNC_LEAVE
} /* end H5_daos_object_visit() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_visit_link_iter_cb
 *
 * Purpose:     Link iteration callback (H5L_iterate_t) which is
 *              recursively called for each link in a group during a call
 *              to H5Ovisit(_by_name).
 *
 *              The callback expects to receive an H5_daos_iter_data_t
 *              which contains a pointer to the object iteration operator
 *              callback function (H5O_iterate_t) to call on the object
 *              which each link points to.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_object_visit_link_iter_cb(hid_t group, const char *name, const H5L_info_t *info,
    void *op_data)
{
    H5_daos_iter_data_t *iter_data = (H5_daos_iter_data_t *)op_data;
    H5_daos_group_t *target_grp;
    H5_daos_obj_t *target_obj = NULL;
    daos_obj_id_t link_target_oid;
    htri_t link_resolves = TRUE;
    herr_t ret_value = H5_ITER_CONT;

    assert(iter_data);
    assert(H5_DAOS_ITER_TYPE_OBJ == iter_data->iter_type);

    if(NULL == (target_grp = (H5_daos_group_t *) H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "failed to retrieve VOL object for group ID")

    if(H5L_TYPE_SOFT == info->type)
        /* Check that the soft link resolves before opening the target object */
        if((link_resolves = H5_daos_link_follow(target_grp, name, strlen(name), iter_data->dxpl_id, NULL, &link_target_oid)) < 0)
            D_GOTO_ERROR(H5E_LINK, H5E_TRAVERSE, H5_ITER_ERROR, "can't follow link")

    if(link_resolves) {
        H5VL_loc_params_t loc_params;

        /* Open the target object */
        loc_params.type = H5VL_OBJECT_BY_NAME;
        loc_params.loc_data.loc_by_name.name = name;
        loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
        if(NULL == (target_obj = H5_daos_object_open(target_grp, &loc_params, NULL, iter_data->dxpl_id, iter_data->req)))
            D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5_ITER_ERROR, "can't open object")

        iter_data->u.obj_iter_data.obj_name = name;
        if(H5_daos_object_visit(target_obj, iter_data) < 0)
            D_GOTO_ERROR(H5E_OHDR, H5E_BADITER, H5_ITER_ERROR, "failed to visit object")

        if(H5_daos_object_close(target_obj, iter_data->dxpl_id, iter_data->req) < 0)
            D_GOTO_ERROR(H5E_OHDR, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object")
        target_obj = NULL;
    } /* end if */

done:
    if(target_obj) {
        if(H5_daos_object_close(target_obj, iter_data->dxpl_id, iter_data->req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object")
        target_obj = NULL;
    } /* end if */

    D_FUNC_LEAVE
} /* end H5_daos_object_visit_link_iter_cb() */


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

        /* Encode oid to address.  Note that if the object index grows beyond 30
         * bits this will return HADDR_UNDEF. */
        obj_info_out->addr = H5_daos_oid_to_addr(target_obj->oid);

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
            case H5I_MAP:
                obj_info_out->type = H5O_TYPE_MAP;
                break;
            default:
                obj_info_out->type = H5O_TYPE_UNKNOWN;
                break;
        }

        /* Reference count is always 1 - change this when
         * H5Lcreate_hard() is implemented DSINC */
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
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
hssize_t
H5_daos_object_get_num_attrs(H5_daos_obj_t *target_obj)
{
    uint64_t nattrs = 0;
    hid_t target_obj_id = -1;
    hssize_t ret_value = 0;

    assert(target_obj);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    if(target_obj->ocpl_cache.track_acorder) {
        daos_sg_list_t sgl;
        daos_key_t dkey;
        daos_iod_t iod;
        daos_iov_t sg_iov;
        uint8_t *p;
        uint8_t nattrs_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
        int ret;

        /* Read the "number of attributes" key from the target object */

        /* Set up dkey */
        daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
        iod.iod_nr = 1u;
        iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, nattrs_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Read number of attributes */
        if(0 != (ret = daos_obj_fetch(target_obj->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, (-1), "can't read number of attributes attached to object: %s", H5_daos_err_to_string(ret))

        p = nattrs_buf;
        /* Check for no num attributes found, in this case it must be 0 */
        if(iod.iod_size == (uint64_t)0) {
            nattrs = 0;
        } /* end if */
        else
            /* Decode num attributes */
            UINT64DECODE(p, nattrs);
    } /* end if */
    else {
        H5_daos_iter_data_t iter_data;

        /* Iterate through attributes */

        /* Register id for target object */
        if((target_obj_id = H5VLwrap_register(target_obj, target_obj->item.type)) < 0)
            D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
        target_obj->item.rc++;

        /* Initialize iteration data */
        H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, H5_INDEX_NAME, H5_ITER_NATIVE,
                FALSE, NULL, target_obj_id, &nattrs, H5P_DATASET_XFER_DEFAULT, NULL);
        iter_data.u.attr_iter_data.attr_iter_op = H5_daos_attribute_iterate_count_attrs_cb;

        /* Retrieve the number of attributes attached to the object */
        if(H5_daos_attribute_iterate(target_obj, &iter_data) < 0)
            D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "attribute iteration failed")
    } /* end else */

    ret_value = (hssize_t)nattrs; /* DSINC - no check for overflow */

done:
    if((target_obj_id >= 0) && (H5Idec_ref(target_obj_id) < 0))
        D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close attribute's parent object")

    D_FUNC_LEAVE
} /* end H5_daos_object_get_num_attrs() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_object_update_num_attrs_key
 *
 * Purpose:     Updates the target object's attribute number tracking akey
 *              by setting its value to the specified value.
 *
 *              CAUTION: This routine is 'dangerous' in that the attribute
 *              number tracking akey is used in various places. Only call
 *              this routine if it is certain that the number of attributes
 *              attached to the target object has changed to the specified
 *              value.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_object_update_num_attrs_key(H5_daos_obj_t *target_obj, uint64_t new_nattrs)
{
    daos_sg_list_t sgl;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_iov_t sg_iov;
    uint8_t nattrs_new_buf[H5_DAOS_ENCODED_NUM_ATTRS_SIZE];
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(target_obj);
    assert(target_obj->ocpl_cache.track_acorder);
    H5daos_compile_assert(H5_DAOS_ENCODED_NUM_ATTRS_SIZE == 8);

    /* Encode buffer */
    p = nattrs_new_buf;
    UINT64ENCODE(p, new_nattrs);

    /* Set up dkey */
    daos_iov_set(&dkey, (void *)H5_daos_attr_key_g, H5_daos_attr_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)H5_daos_nattr_key_g, H5_daos_nattr_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, nattrs_new_buf, (daos_size_t)H5_DAOS_ENCODED_NUM_ATTRS_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Issue write */
    if(0 != (ret = daos_obj_update(target_obj->obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't write number of attributes to object: %s", H5_daos_err_to_string(ret))

done:
    D_FUNC_LEAVE
} /* end H5_daos_object_update_num_attrs_key() */


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
    H5_daos_group_t *copied_group = NULL;
    H5_daos_obj_t *grp_obj = NULL;
    H5_daos_obj_t *obj_to_copy = NULL;
    H5I_type_t opened_obj_type;
    herr_t ret_value = H5_ITER_CONT;

    if(NULL == (grp_obj = H5VLobject(group)))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for group")

    sub_loc_params.type = H5VL_OBJECT_BY_NAME;
    sub_loc_params.loc_data.loc_by_name.name = name;
    sub_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    switch (info->type) {
        case H5L_TYPE_HARD:
        {
            /* Open the object being copied */
            if(NULL == (obj_to_copy = H5_daos_object_open(grp_obj, &sub_loc_params, &opened_obj_type,
                    copy_op_data->dxpl_id, copy_op_data->req)))
                D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open object")

            /*
             * If performing a shallow group copy, copy the group without its immediate members.
             * Otherwise, continue on with a normal recursive object copy.
             */
            if((opened_obj_type == H5I_GROUP) && (copy_op_data->object_copy_opts & H5O_COPY_SHALLOW_HIERARCHY_FLAG)) {
                if(NULL == (copied_group = H5_daos_group_copy_helper((H5_daos_group_t *) obj_to_copy,
                        copy_op_data->new_group, name, copy_op_data->object_copy_opts,
                        copy_op_data->lcpl_id, copy_op_data->dxpl_id, copy_op_data->req)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "failed to perform shallow copy of group")
            } /* end if */
            else {
                if(H5_daos_object_copy_helper(obj_to_copy, opened_obj_type, copy_op_data->new_group, name,
                        copy_op_data->object_copy_opts, copy_op_data->lcpl_id, copy_op_data->dxpl_id,
                        copy_op_data->req) < 0)
                    D_GOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy object")
            } /* end else */

            break;
        } /* H5L_TYPE_HARD */

        case H5L_TYPE_SOFT:
        {
            /*
             * If the H5O_COPY_EXPAND_SOFT_LINK_FLAG flag was specified,
             * expand the soft link into a new object. Otherwise, the link
             * will be copied as-is.
             */
            if (copy_op_data->object_copy_opts & H5O_COPY_EXPAND_SOFT_LINK_FLAG) {
                /* Open the object being copied */
                if(NULL == (obj_to_copy = H5_daos_object_open(grp_obj, &sub_loc_params, &opened_obj_type,
                        copy_op_data->dxpl_id, copy_op_data->req)))
                    D_GOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5_ITER_ERROR, "failed to open object")

                /* Copy the object */
                if(H5_daos_object_copy_helper(obj_to_copy, opened_obj_type, copy_op_data->new_group, name,
                        copy_op_data->object_copy_opts, copy_op_data->lcpl_id, copy_op_data->dxpl_id,
                        copy_op_data->req) < 0)
                    D_GOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, H5_ITER_ERROR, "failed to copy object")
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
            /*
             * If the H5O_COPY_EXPAND_EXT_LINK_FLAG flag was specified,
             * expand the external link into a new object. Otherwise, the
             * link will be copied as-is.
             */
            if (copy_op_data->object_copy_opts & H5O_COPY_EXPAND_EXT_LINK_FLAG) {
                /* TODO: Copy the object */
                D_GOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, H5_ITER_ERROR, "H5O_COPY_EXPAND_EXT_LINK_FLAG flag is currently unsupported")
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
    if(copied_group)
        if(H5_daos_group_close(copied_group, copy_op_data->dxpl_id, copy_op_data->req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close group")
    if(obj_to_copy)
        if(H5_daos_object_close(obj_to_copy, copy_op_data->dxpl_id, copy_op_data->req) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, H5_ITER_ERROR, "can't close object")

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
    H5_daos_group_t *new_group = NULL;
    H5_index_t iter_index_type;
    hid_t target_group_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);
    assert(dst_name);

    /* Copy the group */
    if(NULL == (new_group = H5_daos_group_copy_helper(src_obj, dst_obj, dst_name,
            obj_copy_options, lcpl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "failed to copy group")

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

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether creation order is tracked for the group.
     */
    iter_index_type = (src_obj->gcpl_cache.track_corder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_LINK, iter_index_type, H5_ITER_INC,
            FALSE, NULL, target_group_id, &copy_op_data, dxpl_id, req);
    iter_data.u.link_iter_data.link_iter_op = H5_daos_group_copy_cb;

    /* Copy the immediate members of the group. If the H5O_COPY_SHALLOW_HIERARCHY_FLAG wasn't
     * specified, this will also recursively copy the members of any groups found. */
    if(H5_daos_link_iterate(src_obj, &iter_data) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "failed to iterate over group's links")

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
 * Function:    H5_daos_group_copy_helper
 *
 * Purpose:     Helper routine for H5_daos_group_copy that actually copies
 *              the specified group. This routine is needed to split the
 *              group copying logic away from the higher-level
 *              H5_daos_group_copy, which also copies the immediate members
 *              of a group during a shallow copy, or the entire hierarchy
 *              during a deep copy.
 *
 *              When a shallow group copy is being done, the group copying
 *              callback for link iteration can simply call this routine to
 *              just copy the group. Otherwise, during a deep copy, it can
 *              call H5_daos_group_copy to copy the group and its members
 *              as well.
 *
 * Return:      Non-negative on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static H5_daos_group_t *
H5_daos_group_copy_helper(H5_daos_group_t *src_obj, H5_daos_group_t *dst_obj, const char *dst_name,
    unsigned obj_copy_options, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_loc_params_t dest_loc_params;
    H5_daos_group_t *copied_group = NULL;
    H5_daos_group_t *ret_value = NULL;

    /* Copy the group */
    dest_loc_params.type = H5VL_OBJECT_BY_SELF;
    dest_loc_params.obj_type = H5I_GROUP;
    if(NULL == (copied_group = H5_daos_group_create(dst_obj, &dest_loc_params, dst_name, lcpl_id,
            src_obj->gcpl_id, src_obj->gapl_id, dxpl_id, req)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to create new group")

    /*
     * If the "without attribute copying" flag hasn't been specified,
     * copy the group's attributes as well.
     */
    if((obj_copy_options & H5O_COPY_WITHOUT_ATTR_FLAG) == 0)
        if(H5_daos_object_copy_attributes((H5_daos_obj_t *) src_obj, (H5_daos_obj_t *) copied_group, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy group's attributes")

    ret_value = copied_group;

done:
    if(!ret_value && copied_group)
        if(H5_daos_group_close(copied_group, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_group_copy_helper() */


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
            src_obj->type_id, src_obj->space_id, src_obj->dcpl_id, src_obj->dapl_id, dxpl_id, req)))
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
    const H5A_info_t H5VL_DAOS_UNUSED *ainfo, void *op_data)
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
    H5_index_t iter_index_type;
    hid_t target_obj_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED;

    assert(src_obj);
    assert(dst_obj);

    /* Register ID for source object */
    if((target_obj_id = H5VLwrap_register(src_obj, src_obj->item.type)) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTREGISTER, FAIL, "unable to atomize object handle")
    src_obj->item.rc++;

    /*
     * Determine whether to iterate by name order or creation order, based
     * upon whether attribute creation order is tracked for the object.
     */
    iter_index_type = (src_obj->ocpl_cache.track_acorder) ? H5_INDEX_CRT_ORDER : H5_INDEX_NAME;

    /* Initialize iteration data. Attributes are re-created by creation order if possible */
    H5_DAOS_ITER_DATA_INIT(iter_data, H5_DAOS_ITER_TYPE_ATTR, iter_index_type, H5_ITER_INC,
            FALSE, NULL, target_obj_id, dst_obj, dxpl_id, req);
    iter_data.u.attr_iter_data.attr_iter_op = H5_daos_object_copy_attributes_cb;

    if(H5_daos_attribute_iterate(src_obj, &iter_data) < 0)
        D_GOTO_ERROR(H5E_ATTR, H5E_BADITER, FAIL, "failed to iterate over object's attributes")

done:
    if(target_obj_id >= 0)
        if(H5Idec_ref(target_obj_id) < 0)
            D_DONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, FAIL, "can't close object ID")

    D_FUNC_LEAVE
} /* end H5_daos_object_copy_attributes() */

