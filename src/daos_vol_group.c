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
 * library.  Group routines.
 */

#include "daos_vol.h"           /* DAOS plugin                          */
#include "daos_vol_err.h"       /* DAOS plugin error handling           */
#include "daos_vol_config.h"    /* DAOS plugin configuration header     */

#include "util/daos_vol_mem.h"  /* DAOS plugin memory management        */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_traverse
 *
 * Purpose:     Given a path name and base object, returns the final group
 *              in the path and the object name.  obj_name points into the
 *              buffer given by path, so it does not need to be freed.
 *              The group must be closed with H5_daos_group_close().
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
H5_daos_group_t *
H5_daos_group_traverse(H5_daos_item_t *item, const char *path,
    hid_t dxpl_id, void **req, const char **obj_name, void **gcpl_buf_out,
    uint64_t *gcpl_len_out)
{
    H5_daos_group_t *grp = NULL;
    const char *next_obj;
    daos_obj_id_t oid;
    H5_daos_group_t *ret_value = NULL;

    assert(item);
    assert(path);
    assert(obj_name);

    /* Initialize obj_name */
    *obj_name = path;

    /* Open starting group */
    if((*obj_name)[0] == '/') {
        grp = item->file->root_grp;
        (*obj_name)++;
    } /* end if */
    else {
        if(item->type == H5I_GROUP)
            grp = (H5_daos_group_t *)item;
        else if(item->type == H5I_FILE)
            grp = ((H5_daos_file_t *)item)->root_grp;
        else
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "item not a file or group")
    } /* end else */
        
    grp->obj.item.rc++;

    /* Search for '/' */
    next_obj = strchr(*obj_name, '/');

    /* Traverse path */
    while(next_obj) {
        /* Free gcpl_buf_out */
        if(gcpl_buf_out)
            *gcpl_buf_out = DV_free(*gcpl_buf_out);

        /* Follow link to next group in path */
        assert(next_obj > *obj_name);
        if(H5_daos_link_follow(grp, *obj_name, (size_t)(next_obj - *obj_name), dxpl_id, req, &oid) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't follow link to group")

        /* Close previous group */
        if(H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")
        grp = NULL;

        /* Open group */
        if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_open_helper(item->file, oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req, gcpl_buf_out, gcpl_len_out)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't open group")

        /* Advance to next path element */
        *obj_name = next_obj + 1;
        next_obj = strchr(*obj_name, '/');
    } /* end while */

    /* Set return value */
    ret_value = grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_group_traverse() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_create_helper
 *
 * Purpose:     Performs the actual group creation, but does not create a
 *              link.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_create_helper(H5_daos_file_t *file, hid_t gcpl_id,
    hid_t gapl_id, hid_t dxpl_id, void **req, H5_daos_group_t *parent_grp,
    const char *name, size_t name_len, hbool_t collective)
{
    H5_daos_group_t *grp = NULL;
    void *gcpl_buf = NULL;
    int ret;
    void *ret_value = NULL;

    assert(file->flags & H5F_ACC_RDWR);

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.obj_oh = DAOS_HDL_INVAL;
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Generate group oid */
    H5_daos_oid_encode(&grp->obj.oid, file->max_oid + (uint64_t)1, H5I_GROUP);

    /* Create group and write metadata if this process should */
    if(!collective || (file->my_rank == 0)) {
        daos_key_t dkey;
        daos_iod_t iod;
        daos_sg_list_t sgl;
        daos_iov_t sg_iov;
        size_t gcpl_size = 0;
        char int_md_key[] = H5_DAOS_INT_MD_KEY;
        char gcpl_key[] = H5_DAOS_CPL_KEY;

        /* Create group */
        /* Update max_oid */
        file->max_oid = H5_daos_oid_to_idx(grp->obj.oid);

        /* Write max OID */
        if(H5_daos_write_max_oid(file) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't write max OID")

        /* Open group */
        if(0 != (ret = daos_obj_open(file->coh, grp->obj.oid, DAOS_OO_RW, &grp->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open group: %d", ret)

        /* Encode GCPL */
        if(H5Pencode(gcpl_id, NULL, &gcpl_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl")
        if(NULL == (gcpl_buf = DV_malloc(gcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
        if(H5Pencode(gcpl_id, gcpl_buf, &gcpl_size) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl")

        /* Set up operation to write GCPL to group */
        /* Set up dkey */
        daos_iov_set(&dkey, int_md_key, (daos_size_t)(sizeof(int_md_key) - 1));

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)gcpl_key, (daos_size_t)(sizeof(gcpl_key) - 1));
        daos_csum_set(&iod.iod_kcsum, NULL, 0);
        iod.iod_nr = 1u;
        iod.iod_size = (uint64_t)gcpl_size;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, gcpl_buf, (daos_size_t)gcpl_size);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Write internal metadata to group */
        if(0 != (ret = daos_obj_update(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't write metadata to group: %d", ret)

        /* Write link to group if requested */
        if(parent_grp) {
            H5_daos_link_val_t link_val;

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = grp->obj.oid;
            if(H5_daos_link_write(parent_grp, name, name_len, &link_val) < 0)
                D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create link to group")
        } /* end if */
    } /* end if */
    else {
        /* Update max_oid */
        file->max_oid = grp->obj.oid.lo;

        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast to the
         * calling functions (file_create, group_create) though it could only be
         * an issue with group reopen so we'll skip it for now.  There is
         * probably never an issue with file reopen since all commits are from
         * process 0, same as the group create above. */

        /* Open group */
        if(0 != (ret = daos_obj_open(file->coh, grp->obj.oid, DAOS_OO_RW, &grp->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open group: %d", ret)
    } /* end else */

    /* Finish setting up group struct */
    if((grp->gcpl_id = H5Pcopy(gcpl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gcpl");
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = DV_free(gcpl_buf);

    D_FUNC_LEAVE
} /* end H5_daos_group_create_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_create
 *
 * Purpose:     Sends a request to DAOS to create a group
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_create(void *_item,
    const H5VL_loc_params_t DV_ATTR_UNUSED *loc_params, const char *name,
    hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *grp = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    hbool_t collective = item->file->collective;
    void *ret_value = NULL;

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")
 
    /* Check for collective access, if not already set by the file */
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(gapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get collective access property")

    /* Traverse the path */
    if(name && (!collective || (item->file->my_rank == 0)))
        if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, NULL, NULL)))
            D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

    /* Create group and link to group */
    if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_create_helper(item->file, gcpl_id, gapl_id, dxpl_id, req, target_grp, target_name, target_name ? strlen(target_name) : 0, collective)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create group")

    /* Set return value */
    ret_value = (void *)grp;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_group_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open_helper
 *
 * Purpose:     Performs the actual group open, given the oid.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_open_helper(H5_daos_file_t *file, daos_obj_id_t oid,
    hid_t gapl_id, hid_t dxpl_id, void **req, void **gcpl_buf_out,
    uint64_t *gcpl_len_out)
{
    H5_daos_group_t *grp = NULL;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    void *gcpl_buf = NULL;
    char int_md_key[] = H5_DAOS_INT_MD_KEY;
    char gcpl_key[] = H5_DAOS_CPL_KEY;
    uint64_t gcpl_len;
    int ret;
    void *ret_value = NULL;

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.oid = oid;
    grp->obj.obj_oh = DAOS_HDL_INVAL;
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Open group */
    if(0 != (ret = daos_obj_open(file->coh, oid, file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &grp->obj.obj_oh, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open group: %d", ret)

    /* Set up operation to read GCPL size from group */
    /* Set up dkey */
    daos_iov_set(&dkey, int_md_key, (daos_size_t)(sizeof(int_md_key) - 1));

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, (void *)gcpl_key, (daos_size_t)(sizeof(gcpl_key) - 1));
    daos_csum_set(&iod.iod_kcsum, NULL, 0);
    iod.iod_nr = 1u;
    iod.iod_size = DAOS_REC_ANY;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Read internal metadata size from group */
    if(0 != (ret = daos_obj_fetch(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, NULL, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTDECODE, NULL, "can't read metadata size from group: %d", ret)

    /* Check for metadata not found */
    if(iod.iod_size == (uint64_t)0)
        D_GOTO_ERROR(H5E_SYM, H5E_NOTFOUND, NULL, "internal metadata not found")

    /* Allocate buffer for GCPL */
    gcpl_len = iod.iod_size;
    if(NULL == (gcpl_buf = DV_malloc(gcpl_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")

    /* Set up sgl */
    daos_iov_set(&sg_iov, gcpl_buf, (daos_size_t)gcpl_len);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Read internal metadata from group */
    if(0 != (ret = daos_obj_fetch(grp->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTDECODE, NULL, "can't read metadata from group: %d", ret)

    /* Decode GCPL */
    if((grp->gcpl_id = H5Pdecode(gcpl_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize GCPL")

    /* Finish setting up group struct */
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Return GCPL info if requested, relinquish ownership of gcpl_buf if so */
    if(gcpl_buf_out) {
        assert(gcpl_len_out);
        assert(!*gcpl_buf_out);

        *gcpl_buf_out = gcpl_buf;
        gcpl_buf = NULL;

        *gcpl_len_out = gcpl_len;
    } /* end if */

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = DV_free(gcpl_buf);

    D_FUNC_LEAVE
} /* end H5_daos_group_open_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_reconstitute
 *
 * Purpose:     Reconstitutes a group object opened by another process.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              April, 2017
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_reconstitute(H5_daos_file_t *file, daos_obj_id_t oid,
    uint8_t *gcpl_buf, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_group_t *grp = NULL;
    int ret;
    void *ret_value = NULL;

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5_daos_group_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.oid = oid;
    grp->obj.obj_oh = DAOS_HDL_INVAL;
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Open group */
    if(0 != (ret = daos_obj_open(file->coh, oid, file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &grp->obj.obj_oh, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open group: %d", ret)

    /* Decode GCPL */
    if((grp->gcpl_id = H5Pdecode(gcpl_buf)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize GCPL")

    /* Finish setting up group struct */
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    D_FUNC_LEAVE
} /* end H5_daos_group_reconstitute() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_open
 *
 * Purpose:     Sends a request to DAOS to open a group
 *
 * Return:      Success:        dataset object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_group_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_group_t *grp = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_obj_id_t oid;
    uint8_t *gcpl_buf = NULL;
    uint64_t gcpl_len = 0;
    uint8_t ginfo_buf_static[H5_DAOS_GINFO_BUF_SIZE];
    uint8_t *p;
    hbool_t collective = item->file->collective;
    hbool_t must_bcast = FALSE;
    void *ret_value = NULL;
 
    /* Check for collective access, if not already set by the file */
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(gapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get collective access property")

    /* Check if we're actually opening the group or just receiving the group
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by address */
        if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
            /* Generate oid from address */
            memset(&oid, 0, sizeof(oid));
            H5_daos_oid_generate(&oid, (uint64_t)loc_params->loc_data.loc_by_addr.addr, H5I_GROUP);

            /* Open group */
            if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_open_helper(item->file, oid, gapl_id, dxpl_id, req, (collective && (item->file->num_procs > 1)) ? (void **)&gcpl_buf : NULL, &gcpl_len)))
                D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group")
        } /* end if */
        else {
            /* Open using name parameter */
            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, (collective && (item->file->num_procs > 1)) ? (void **)&gcpl_buf : NULL, &gcpl_len)))
                D_GOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

            /* Check for no target_name, in this case just return target_grp */
            if(target_name[0] == '\0'
                    || (target_name[0] == '.' && target_name[1] == '\0')) {
                size_t gcpl_size;

                /* Take ownership of target_grp */
                grp = target_grp;
                target_grp = NULL;

                /* Encode GCPL */
                if(H5Pencode(grp->gcpl_id, NULL, &gcpl_size) < 0)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl")
                if(NULL == (gcpl_buf = (uint8_t *)DV_malloc(gcpl_size)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
                gcpl_len = (uint64_t)gcpl_size;
                if(H5Pencode(grp->gcpl_id, gcpl_buf, &gcpl_size) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl")
            } /* end if */
            else {
                gcpl_buf = (uint8_t *)DV_free(gcpl_buf);
                gcpl_len = 0;

                /* Follow link to group */
                if(H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &oid) < 0)
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't follow link to group")

                /* Open group */
                if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_open_helper(item->file, oid, gapl_id, dxpl_id, req, (collective && (item->file->num_procs > 1)) ? (void **)&gcpl_buf : NULL, &gcpl_len)))
                    D_GOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group")
            } /* end else */
        } /* end else */

        /* Broadcast group info if there are other processes that need it */
        if(collective && (item->file->num_procs > 1)) {
            assert(gcpl_buf);
            assert(sizeof(ginfo_buf_static) >= 3 * sizeof(uint64_t));

            /* Encode oid */
            p = ginfo_buf_static;
            UINT64ENCODE(p, grp->obj.oid.lo)
            UINT64ENCODE(p, grp->obj.oid.hi)

            /* Encode GCPL length */
            UINT64ENCODE(p, gcpl_len)

            /* Copy GCPL to ginfo_buf_static if it will fit */
            if((gcpl_len + 3 * sizeof(uint64_t)) <= sizeof(ginfo_buf_static))
                (void)memcpy(p, gcpl_buf, gcpl_len);

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast ginfo_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)ginfo_buf_static, sizeof(ginfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast group info")

            /* Need a second bcast if it did not fit in the receivers' static
             * buffer */
            if(gcpl_len + 3 * sizeof(uint64_t) > sizeof(ginfo_buf_static))
                if(MPI_SUCCESS != MPI_Bcast((char *)gcpl_buf, (int)gcpl_len, MPI_BYTE, 0, item->file->comm))
                    D_GOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast GCPL")
        } /* end if */
    } /* end if */
    else {
        /* Receive GCPL */
        if(MPI_SUCCESS != MPI_Bcast((char *)ginfo_buf_static, sizeof(ginfo_buf_static), MPI_BYTE, 0, item->file->comm))
            D_GOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast group info")

        /* Decode oid */
        p = ginfo_buf_static;
        UINT64DECODE(p, oid.lo)
        UINT64DECODE(p, oid.hi)

        /* Decode GCPL length */
        UINT64DECODE(p, gcpl_len)

        /* Check for gcpl_len set to 0 - indicates failure */
        if(gcpl_len == 0)
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "lead process failed to open group")

        /* Check if we need to perform another bcast */
        if(gcpl_len + 3 * sizeof(uint64_t) > sizeof(ginfo_buf_static)) {
            /* Allocate a dynamic buffer if necessary */
            if(gcpl_len > sizeof(ginfo_buf_static)) {
                if(NULL == (gcpl_buf = (uint8_t *)DV_malloc(gcpl_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for global pool handle")
                p = gcpl_buf;
            } /* end if */
            else
                p = ginfo_buf_static;

            /* Receive GCPL */
            if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)gcpl_len, MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast GCPL")
        } /* end if */

        /* Reconstitute group from received oid and GCPL buffer */
        if(NULL == (grp = (H5_daos_group_t *)H5_daos_group_reconstitute(item->file, oid, p, gapl_id, dxpl_id, req)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't reconstitute group")
    } /* end else */

    /* Set return value */
    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast gcpl_buf as '0' if necessary - this will trigger failures in
         * other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(ginfo_buf_static, 0, sizeof(ginfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(ginfo_buf_static, sizeof(ginfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_DONE_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast empty group info")
        } /* end if */

        /* Close group */
        if(grp && H5_daos_group_close(grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = (uint8_t *)DV_free(gcpl_buf);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_group_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_group_close
 *
 * Purpose:     Closes a daos HDF5 group.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_group_close(void *_grp, hid_t DV_ATTR_UNUSED dxpl_id,
    void DV_ATTR_UNUSED **req)
{
    H5_daos_group_t *grp = (H5_daos_group_t *)_grp;
    int ret;
    herr_t ret_value = SUCCEED;

    assert(grp);

    if(--grp->obj.item.rc == 0) {
        /* Free group data structures */
        if(!daos_handle_is_inval(grp->obj.obj_oh))
            if(0 != (ret = daos_obj_close(grp->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close group DAOS object: %d", ret)
        if(grp->gcpl_id != FAIL && H5Idec_ref(grp->gcpl_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close plist")
        if(grp->gapl_id != FAIL && H5Idec_ref(grp->gapl_id) < 0)
            D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close plist")
        grp = H5FL_FREE(H5_daos_group_t, grp);
    } /* end if */

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_group_close() */

