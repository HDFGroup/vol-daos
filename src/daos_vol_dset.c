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
 * library.  Dataset routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */
#include "daos_vol_config.h"    /* DAOS connector configuration header     */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Typedefs */
/* Udata type for H5Dscatter callback */
typedef struct H5_daos_scatter_cb_ud_t {
    void *buf;
    size_t len;
} H5_daos_scatter_cb_ud_t;

/* Udata type for memory space H5Diterate callback */
typedef struct {
    daos_iod_t *iods;
    daos_sg_list_t *sgls;
    daos_iov_t *sg_iovs;
    hbool_t is_vl_str;
    size_t base_type_size;
    uint64_t offset;
    uint64_t idx;
} H5_daos_vl_mem_ud_t;

/* Udata type for file space H5Diterate callback */
typedef struct {
    uint8_t **akeys;
    daos_iod_t *iods;
    uint64_t idx;
} H5_daos_vl_file_ud_t;

/* Prototypes */
static herr_t H5_daos_sel_to_recx_iov(hid_t space_id, size_t type_size,
    void *buf, daos_recx_t **recxs, daos_iov_t **sg_iovs, size_t *list_nused);
static herr_t H5_daos_scatter_cb(const void **src_buf,
    size_t *src_buf_bytes_used, void *_udata);
static herr_t H5_daos_dataset_mem_vl_rd_cb(void *_elem, hid_t type_id,
    unsigned ndim, const hsize_t *point, void *_udata);
static herr_t H5_daos_dataset_file_vl_cb(void *_elem, hid_t type_id,
    unsigned ndim, const hsize_t *point, void *_udata);
static herr_t H5_daos_dataset_mem_vl_wr_cb(void *_elem, hid_t type_id,
    unsigned ndim, const hsize_t *point, void *_udata);


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_create
 *
 * Purpose:     Sends a request to DAOS to create a dataset
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
H5_daos_dataset_create(void *_item,
    const H5VL_loc_params_t DV_ATTR_UNUSED *loc_params, const char *name,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_dset_t *dset = NULL;
    hid_t type_id, space_id;
    H5_daos_group_t *target_grp = NULL;
    void *type_buf = NULL;
    void *space_buf = NULL;
    void *dcpl_buf = NULL;
    hbool_t collective;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset name is NULL")

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")

    /* Check for collective access, if not already set by the file */
    collective = item->file->collective;
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(dapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get collective access property")

    /* get creation properties */
    if(H5Pget(dcpl_id, H5VL_PROP_DSET_TYPE_ID, &type_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for datatype ID")
    if(H5Pget(dcpl_id, H5VL_PROP_DSET_SPACE_ID, &space_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for dataspace ID")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = H5FL_CALLOC(H5_daos_dset_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.open_req = NULL;
    dset->obj.item.file = item->file;
    dset->obj.item.rc = 1;
    dset->obj.obj_oh = DAOS_HDL_INVAL;
    dset->type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    dset->dapl_id = FAIL;

    /* Generate dataset oid */
    H5_daos_oid_encode(&dset->obj.oid, item->file->max_oid + (uint64_t)1, H5I_DATASET);

    /* Create dataset and write metadata if this process should */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        H5_daos_link_val_t link_val;
        daos_key_t dkey;
        daos_iod_t iod[3];
        daos_sg_list_t sgl[3];
        daos_iov_t sg_iov[3];
        size_t type_size = 0;
        size_t space_size = 0;
        size_t dcpl_size = 0;

        /* Traverse the path */
        if(name)
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path")

        /* Create dataset */
        /* Update max_oid */
        item->file->max_oid = H5_daos_oid_to_idx(dset->obj.oid);

        /* Write max OID */
        if(H5_daos_write_max_oid(item->file) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't write max OID")

        /* Open dataset */
        if(0 != (ret = daos_obj_open(item->file->coh, dset->obj.oid, DAOS_OO_RW, &dset->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset: %s", H5_daos_err_to_string(ret))

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

        /* Encode DCPL */
        if(H5Pencode(dcpl_id, NULL, &dcpl_size) < 0)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dcpl")
        if(NULL == (dcpl_buf = DV_malloc(dcpl_size)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dcpl")
        if(H5Pencode(dcpl_id, dcpl_buf, &dcpl_size) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dcpl")

        /* Set up operation to write datatype, dataspace, and DCPL to dataset */
        /* Set up dkey */
        daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, H5_daos_type_key_g, H5_daos_type_key_size_g);
        daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = (uint64_t)type_size;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, H5_daos_space_key_g, H5_daos_space_key_size_g);
        daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = (uint64_t)space_size;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[2].iod_name, H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
        iod[2].iod_nr = 1u;
        iod[2].iod_size = (uint64_t)dcpl_size;
        iod[2].iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov[0], type_buf, (daos_size_t)type_size);
        sgl[0].sg_nr = 1;
        sgl[0].sg_nr_out = 0;
        sgl[0].sg_iovs = &sg_iov[0];
        daos_iov_set(&sg_iov[1], space_buf, (daos_size_t)space_size);
        sgl[1].sg_nr = 1;
        sgl[1].sg_nr_out = 0;
        sgl[1].sg_iovs = &sg_iov[1];
        daos_iov_set(&sg_iov[2], dcpl_buf, (daos_size_t)dcpl_size);
        sgl[2].sg_nr = 1;
        sgl[2].sg_nr_out = 0;
        sgl[2].sg_iovs = &sg_iov[2];

        /* Write internal metadata to dataset */
        if(0 != (ret = daos_obj_update(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 3, iod, sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't write metadata to dataset: %s", H5_daos_err_to_string(ret))

        /* Create link to dataset */
        if(name) {
            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = dset->obj.oid;
            if(H5_daos_link_write(target_grp, target_name, strlen(target_name), &link_val) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create link to dataset")
        } /* end if */
    } /* end if */
    else {
        /* Update max_oid */
        item->file->max_oid = dset->obj.oid.lo;

        /* Note no barrier is currently needed here, daos_obj_open is a local
         * operation and can occur before the lead process writes metadata.  For
         * app-level synchronization we could add a barrier or bcast though it
         * could only be an issue with dataset reopen so we'll skip it for now.
         * There is probably never an issue with file reopen since all commits
         * are from process 0, same as the dataset create above. */

        /* Open dataset */
        if(0 != (ret = daos_obj_open(item->file->coh, dset->obj.oid, DAOS_OO_RW, &dset->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset: %s", H5_daos_err_to_string(ret))
    } /* end else */

    /* Finish setting up dataset struct */
    if((dset->type_id = H5Tcopy(type_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((dset->space_id = H5Scopy(space_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dataspace")
    if(H5Sselect_all(dset->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")
    if((dset->dcpl_id = H5Pcopy(dcpl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dcpl")
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dapl")

    /* Set return value */
    ret_value = (void *)dset;

done:
    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close group")

    /* Cleanup on failure */
    /* Destroy DAOS object if created before failure DSINC */
    if(NULL == ret_value)
        /* Close dataset */
        if(dset && H5_daos_dataset_close(dset, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset")

    /* Free memory */
    type_buf = DV_free(type_buf);
    space_buf = DV_free(space_buf);
    dcpl_buf = DV_free(dcpl_buf);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_dataset_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_open
 *
 * Purpose:     Sends a request to DAOS to open a dataset
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
H5_daos_dataset_open(void *_item,
    const H5VL_loc_params_t DV_ATTR_UNUSED *loc_params, const char *name,
    hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_item_t *item = (H5_daos_item_t *)_item;
    H5_daos_dset_t *dset = NULL;
    H5_daos_group_t *target_grp = NULL;
    const char *target_name = NULL;
    daos_key_t dkey;
    daos_iod_t iod[3];
    daos_sg_list_t sgl[3];
    daos_iov_t sg_iov[3];
    uint64_t type_len = 0;
    uint64_t space_len = 0;
    uint64_t dcpl_len = 0;
    uint64_t tot_len;
    uint8_t dinfo_buf_static[H5_DAOS_DINFO_BUF_SIZE];
    uint8_t *dinfo_buf_dyn = NULL;
    uint8_t *dinfo_buf = dinfo_buf_static;
    uint8_t *p;
    hbool_t collective;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset parent object is NULL")
    if(!loc_params)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL")
    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset name is NULL")

    /* Check for collective access, if not already set by the file */
    collective = item->file->collective;
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(dapl_id, &collective) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get collective access property")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = H5FL_CALLOC(H5_daos_dset_t)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate DAOS dataset struct")
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.open_req = NULL;
    dset->obj.item.file = item->file;
    dset->obj.item.rc = 1;
    dset->obj.obj_oh = DAOS_HDL_INVAL;
    dset->type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    dset->dapl_id = FAIL;

    /* Check if we're actually opening the group or just receiving the dataset
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by address */
        if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
            /* Generate oid from address */
            H5_daos_oid_generate(&dset->obj.oid, (uint64_t)loc_params->loc_data.loc_by_addr.addr, H5I_DATASET);
        } /* end if */
        else {
            /* Open using name parameter */
            /* Traverse the path */
            if(NULL == (target_grp = H5_daos_group_traverse(item, name, dxpl_id, req, &target_name, NULL, NULL)))
                D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path")

            /* Follow link to dataset */
            if(H5_daos_link_follow(target_grp, target_name, strlen(target_name), dxpl_id, req, &dset->obj.oid) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't follow link to dataset")
        } /* end else */

        /* Open dataset */
        if(0 != (ret = daos_obj_open(item->file->coh, dset->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &dset->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset: %s", H5_daos_err_to_string(ret))

        /* Set up operation to read datatype, dataspace, and DCPL sizes from
         * dataset */
        /* Set up dkey */
        daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(iod, 0, sizeof(iod));
        daos_iov_set(&iod[0].iod_name, H5_daos_type_key_g, H5_daos_type_key_size_g);
        daos_csum_set(&iod[0].iod_kcsum, NULL, 0);
        iod[0].iod_nr = 1u;
        iod[0].iod_size = DAOS_REC_ANY;
        iod[0].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[1].iod_name, H5_daos_space_key_g, H5_daos_space_key_size_g);
        daos_csum_set(&iod[1].iod_kcsum, NULL, 0);
        iod[1].iod_nr = 1u;
        iod[1].iod_size = DAOS_REC_ANY;
        iod[1].iod_type = DAOS_IOD_SINGLE;

        daos_iov_set(&iod[2].iod_name, (void *)H5_daos_cpl_key_g, H5_daos_cpl_key_size_g);
        daos_csum_set(&iod[2].iod_kcsum, NULL, 0);
        iod[2].iod_nr = 1u;
        iod[2].iod_size = DAOS_REC_ANY;
        iod[2].iod_type = DAOS_IOD_SINGLE;

        /* Read internal metadata sizes from dataset */
        if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 3, iod, NULL,
                      NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, NULL, "can't read metadata sizes from dataset: %s", H5_daos_err_to_string(ret))

        /* Check for metadata not found */
        if((iod[0].iod_size == (uint64_t)0) || (iod[1].iod_size == (uint64_t)0)
                || (iod[2].iod_size == (uint64_t)0))
            D_GOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, NULL, "internal metadata not found")

        /* Compute dataset info buffer size */
        type_len = iod[0].iod_size;
        space_len = iod[1].iod_size;
        dcpl_len = iod[2].iod_size;
        tot_len = type_len + space_len + dcpl_len;

        /* Allocate dataset info buffer if necessary */
        if((tot_len + (5 * sizeof(uint64_t))) > sizeof(dinfo_buf_static)) {
            if(NULL == (dinfo_buf_dyn = (uint8_t *)DV_malloc(tot_len + (5 * sizeof(uint64_t)))))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate dataset info buffer")
            dinfo_buf = dinfo_buf_dyn;
        } /* end if */

        /* Set up sgl */
        p = dinfo_buf + (5 * sizeof(uint64_t));
        daos_iov_set(&sg_iov[0], p, (daos_size_t)type_len);
        sgl[0].sg_nr = 1;
        sgl[0].sg_nr_out = 0;
        sgl[0].sg_iovs = &sg_iov[0];
        p += type_len;
        daos_iov_set(&sg_iov[1], p, (daos_size_t)space_len);
        sgl[1].sg_nr = 1;
        sgl[1].sg_nr_out = 0;
        sgl[1].sg_iovs = &sg_iov[1];
        p += space_len;
        daos_iov_set(&sg_iov[2], p, (daos_size_t)dcpl_len);
        sgl[2].sg_nr = 1;
        sgl[2].sg_nr_out = 0;
        sgl[2].sg_iovs = &sg_iov[2];

        /* Read internal metadata from dataset */
        if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 3, iod, sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, NULL, "can't read metadata from dataset: %s", H5_daos_err_to_string(ret))

        /* Broadcast dataset info if there are other processes that need it */
        if(collective && (item->file->num_procs > 1)) {
            assert(dinfo_buf);
            assert(sizeof(dinfo_buf_static) >= 5 * sizeof(uint64_t));

            /* Encode oid */
            p = dinfo_buf;
            UINT64ENCODE(p, dset->obj.oid.lo)
            UINT64ENCODE(p, dset->obj.oid.hi)

            /* Encode serialized info lengths */
            UINT64ENCODE(p, type_len)
            UINT64ENCODE(p, space_len)
            UINT64ENCODE(p, dcpl_len)

            /* MPI_Bcast dinfo_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf, sizeof(dinfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't broadcast dataset info")

            /* Need a second bcast if it did not fit in the receivers' static
             * buffer */
            if(tot_len + (5 * sizeof(uint64_t)) > sizeof(dinfo_buf_static))
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                    D_GOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't broadcast dataset info (second broadcast)")
        } /* end if */
        else
            p = dinfo_buf + (5 * sizeof(uint64_t));
    } /* end if */
    else {
        /* Receive dataset info */
        if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf, sizeof(dinfo_buf_static), MPI_BYTE, 0, item->file->comm))
            D_GOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't receive broadcasted dataset info")

        /* Decode oid */
        p = dinfo_buf_static;
        UINT64DECODE(p, dset->obj.oid.lo)
        UINT64DECODE(p, dset->obj.oid.hi)

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_len)
        UINT64DECODE(p, space_len)
        UINT64DECODE(p, dcpl_len)
        tot_len = type_len + space_len + dcpl_len;

        /* Check for type_len set to 0 - indicates failure */
        if(type_len == 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "lead process failed to open dataset")

        /* Check if we need to perform another bcast */
        if(tot_len + (5 * sizeof(uint64_t)) > sizeof(dinfo_buf_static)) {
            /* Allocate a dynamic buffer if necessary */
            if(tot_len > sizeof(dinfo_buf_static)) {
                if(NULL == (dinfo_buf_dyn = (uint8_t *)DV_malloc(tot_len)))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for dataset info")
                dinfo_buf = dinfo_buf_dyn;
            } /* end if */

            /* Receive dataset info */
            if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                D_GOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't receive broadcasted dataset info (second broadcast)")

            p = dinfo_buf;
        } /* end if */

        /* Open dataset */
        if(0 != (ret = daos_obj_open(item->file->coh, dset->obj.oid, item->file->flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &dset->obj.obj_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open dataset: %s", H5_daos_err_to_string(ret))
    } /* end else */

    /* Decode datatype, dataspace, and DCPL */
    if((dset->type_id = H5Tdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += type_len;
    if((dset->space_id = H5Sdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize dataspace")
    if(H5Sselect_all(dset->space_id) < 0)
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")
    p += space_len;
    if((dset->dcpl_id = H5Pdecode(p)) < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize dataset creation property list")

    /* Finish setting up dataset struct */
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dapl");

    /* Set return value */
    ret_value = (void *)dset;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast dinfo_buf as '0' if necessary - this will trigger failures in
         * in other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(dinfo_buf_static, 0, sizeof(dinfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(dinfo_buf_static, sizeof(dinfo_buf_static), MPI_BYTE, 0, item->file->comm))
                D_DONE_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't broadcast empty dataset info")
        } /* end if */

        /* Close dataset */
        if(dset && H5_daos_dataset_close(dset, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset")
    } /* end if */

    /* Close target group */
    if(target_grp && H5_daos_group_close(target_grp, dxpl_id, req) < 0)
        D_DONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    dinfo_buf_dyn = (uint8_t *)DV_free(dinfo_buf_dyn);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_dataset_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_sel_to_recx_iov
 *
 * Purpose:     Given a dataspace with a selection and the datatype
 *              (element) size, build a list of DAOS records (recxs)
 *              and/or scatter/gather list I/O vectors (sg_iovs). *recxs
 *              and *sg_iovs should, if requested, point to a (probably
 *              statically allocated) single element.  Does not release
 *              buffers on error.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_sel_to_recx_iov(hid_t space_id, size_t type_size, void *buf,
    daos_recx_t **recxs, daos_iov_t **sg_iovs, size_t *list_nused)
{
    H5S_sel_iter_t *sel_iter = NULL;
    hbool_t sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    size_t nseq;
    size_t nelem;
    hsize_t off[H5_DAOS_SEQ_LIST_LEN];
    size_t len[H5_DAOS_SEQ_LIST_LEN];
    size_t buf_len = 1;
    void *vp_ret;
    size_t szi;
    herr_t ret_value = SUCCEED;

    assert(recxs || sg_iovs);
    assert(!recxs || *recxs);
    assert(!sg_iovs || *sg_iovs);
    assert(list_nused);

    /* Initialize list_nused */
    *list_nused = 0;

    /* Initialize selection iterator  */
    if(NULL == (sel_iter = H5Sselect_iter_init(space_id, (size_t)1)))
        D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
    sel_iter_init = TRUE;       /* Selection iteration info has been initialized */

    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes */
        if(H5Sselect_get_seq_list(space_id, 0, sel_iter, (size_t)H5_DAOS_SEQ_LIST_LEN, (size_t)-1, &nseq, &nelem, off, len) < 0)
            D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")

        /* Make room for sequences in recxs */
        if((buf_len == 1) && (nseq > 1)) {
            if(recxs)
                if(NULL == (*recxs = (daos_recx_t *)DV_malloc(H5_DAOS_SEQ_LIST_LEN * sizeof(daos_recx_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate memory for records")
            if(sg_iovs)
                if(NULL == (*sg_iovs = (daos_iov_t *)DV_malloc(H5_DAOS_SEQ_LIST_LEN * sizeof(daos_iov_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate memory for sgl iovs")
            buf_len = H5_DAOS_SEQ_LIST_LEN;
        } /* end if */
        else if(*list_nused + nseq > buf_len) {
            if(recxs) {
                if(NULL == (vp_ret = DV_realloc(*recxs, 2 * buf_len * sizeof(daos_recx_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate memory for records")
                *recxs = (daos_recx_t *)vp_ret;
            } /* end if */
            if(sg_iovs) {
                if(NULL == (vp_ret = DV_realloc(*sg_iovs, 2 * buf_len * sizeof(daos_iov_t))))
                    D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate memory for sgls")
                *sg_iovs = (daos_iov_t *)vp_ret;
            } /* end if */
            buf_len *= 2;
        } /* end if */
        assert(*list_nused + nseq <= buf_len);

        /* Copy offsets/lengths to recxs and sg_iovs */
        for(szi = 0; szi < nseq; szi++) {
            if(recxs) {
                (*recxs)[szi + *list_nused].rx_idx = (uint64_t)off[szi];
                (*recxs)[szi + *list_nused].rx_nr = (uint64_t)len[szi];
            } /* end if */
            if(sg_iovs)
                daos_iov_set(&(*sg_iovs)[szi + *list_nused],
                        (uint8_t *)buf + (off[szi] * type_size),
                        (daos_size_t)len[szi] * (daos_size_t)type_size);
        } /* end for */
        *list_nused += nseq;
    } while(nseq == H5_DAOS_SEQ_LIST_LEN);

done:
    /* Release selection iterator */
    if(sel_iter_init && H5Sselect_iter_release(sel_iter) < 0)
        D_DONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")

    D_FUNC_LEAVE
} /* end H5_daos_sel_to_recx_iov() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_scatter_cb
 *
 * Purpose:     Callback function for H5Dscatter.  Simply passes the
 *              entire buffer described by udata to H5Dscatter.
 *
 * Return:      SUCCEED (never fails)
 *
 * Programmer:  Neil Fortner
 *              March, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_scatter_cb(const void **src_buf, size_t *src_buf_bytes_used,
    void *_udata)
{
    H5_daos_scatter_cb_ud_t *udata = (H5_daos_scatter_cb_ud_t *)_udata;
    herr_t ret_value = SUCCEED;

    /* Set src_buf and src_buf_bytes_used to use the entire buffer */
    *src_buf = udata->buf;
    *src_buf_bytes_used = udata->len;

    /* DSINC - This function used to always return SUCCEED without needing an
     * herr_t. Might need an additional FUNC_LEAVE macro to do this, or modify
     * the current one to take in the ret_value.
     */
    D_FUNC_LEAVE
} /* end H5_daos_scatter_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_mem_vl_rd_cb
 *
 * Purpose:     H5Diterate callback for iterating over the memory space
 *              before reading vl data.  Allocates vl read buffers,
 *              up scatter gather lists (sgls), and reshapes iods if
 *              necessary to skip empty elements.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              May, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_mem_vl_rd_cb(void *_elem, hid_t DV_ATTR_UNUSED type_id,
    unsigned DV_ATTR_UNUSED ndim, const hsize_t DV_ATTR_UNUSED *point,
    void *_udata)
{
    H5_daos_vl_mem_ud_t *udata = (H5_daos_vl_mem_ud_t *)_udata;
    herr_t ret_value = SUCCEED;

    /* Set up constant sgl info */
    udata->sgls[udata->idx].sg_nr = 1;
    udata->sgls[udata->idx].sg_nr_out = 0;
    udata->sgls[udata->idx].sg_iovs = &udata->sg_iovs[udata->idx];

    /* Check for empty element */
    if(udata->iods[udata->idx].iod_size == 0) {
        /* Increment offset, slide down following elements */
        udata->offset++;

        /* Zero out read buffer */
        if(udata->is_vl_str)
            *(char **)_elem = NULL;
        else
            memset(_elem, 0, sizeof(hvl_t));
    } /* end if */
    else {
        assert(udata->idx >= udata->offset);

        /* Check for vlen string */
        if(udata->is_vl_str) {
            char *elem = NULL;

            /* Allocate buffer for this vl element */
            if(NULL == (elem = (char *)malloc((size_t)udata->iods[udata->idx].iod_size + 1)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate vl data buffer")
            *(char **)_elem = elem;

            /* Add null terminator */
            elem[udata->iods[udata->idx].iod_size] = '\0';

            /* Set buffer location in sgl */
            daos_iov_set(&udata->sg_iovs[udata->idx - udata->offset], elem, udata->iods[udata->idx].iod_size);
        } /* end if */
        else {
            /* Standard vlen, find hvl_t struct for this element */
            hvl_t *elem = (hvl_t *)_elem;

            assert(udata->base_type_size > 0);

            /* Allocate buffer for this vl element and set size */
            elem->len = (size_t)udata->iods[udata->idx].iod_size / udata->base_type_size;
            if(NULL == (elem->p = malloc((size_t)udata->iods[udata->idx].iod_size)))
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate vl data buffer")

            /* Set buffer location in sgl */
            daos_iov_set(&udata->sg_iovs[udata->idx - udata->offset], elem->p, udata->iods[udata->idx].iod_size);
        } /* end if */

        /* Slide down iod if necessary */
        if(udata->offset)
            udata->iods[udata->idx - udata->offset] = udata->iods[udata->idx];
    } /* end else */

    /* Advance idx */
    udata->idx++;

done:
    D_FUNC_LEAVE
} /* end H5_daos_dataset_mem_vl_rd_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_file_vl_cb
 *
 * Purpose:     H5Diterate callback for iterating over the file space
 *              before vl data I/O.  Sets up akeys and iods (except for
 *              iod record sizes).
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              May, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_file_vl_cb(void DV_ATTR_UNUSED *_elem,
    hid_t DV_ATTR_UNUSED type_id, unsigned ndim, const hsize_t *point,
    void *_udata)
{
    H5_daos_vl_file_ud_t *udata = (H5_daos_vl_file_ud_t *)_udata;
    size_t akey_len = ndim * sizeof(uint64_t);
    uint64_t coordu64;
    uint8_t *p;
    unsigned i;
    herr_t ret_value = SUCCEED;

    /* Create akey for this element */
    if(NULL == (udata->akeys[udata->idx] = (uint8_t *)DV_malloc(akey_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey")
    p = udata->akeys[udata->idx];
    for(i = 0; i < ndim; i++) {
        coordu64 = (uint64_t)point[i];
        UINT64ENCODE(p, coordu64)
    } /* end for */

    /* Set up iod, size was set in memory callback or initialized in main read
     * function.  Use "single" records of varying size. */
    daos_iov_set(&udata->iods[udata->idx].iod_name, (void *)udata->akeys[udata->idx], (daos_size_t)akey_len);
    daos_csum_set(&udata->iods[udata->idx].iod_kcsum, NULL, 0);
    udata->iods[udata->idx].iod_nr = 1u;
    udata->iods[udata->idx].iod_type = DAOS_IOD_SINGLE;

    /* Advance idx */
    udata->idx++;

done:
    D_FUNC_LEAVE
} /* end H5_daos_dataset_file_vl_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_read
 *
 * Purpose:     Reads raw data from a dataset into a buffer.
 *`
 * Return:      Success:        0
 *              Failure:        -1, dataset not read.
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_read(void *_dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t dxpl_id, void *buf, void DV_ATTR_UNUSED **req)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    H5S_sel_iter_t *sel_iter = NULL;
    hbool_t sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    int ndims;
    hsize_t dim[H5S_MAX_RANK];
    hid_t real_file_space_id;
    hid_t real_mem_space_id;
    hssize_t num_elem = -1;
    uint64_t chunk_coords[H5S_MAX_RANK];
    daos_key_t dkey;
    uint8_t **akeys = NULL;
    daos_iod_t *iods = NULL;
    daos_sg_list_t *sgls = NULL;
    daos_recx_t recx;
    daos_recx_t *recxs = &recx;
    daos_iov_t sg_iov;
    daos_iov_t *sg_iovs = &sg_iov;
    uint8_t dkey_buf[1 + H5S_MAX_RANK];
    hid_t base_type_id = FAIL;
    size_t base_type_size = 0;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    H5T_class_t type_class;
    hbool_t is_vl = FALSE;
    htri_t is_vl_str = FALSE;
    H5_daos_tconv_reuse_t reuse = H5_DAOS_TCONV_REUSE_NONE;
    uint8_t *p;
    int ret;
    uint64_t i;
    herr_t ret_value = SUCCEED;

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL")
    if(!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "read buffer is NULL")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")

    /* Get "real" space ids */
    if(file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;
    if(mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else
        real_mem_space_id = mem_space_id;

    /* Encode dkey (chunk coordinates).  Prefix with '\0' to avoid accidental
     * collisions with other d-keys in this object.  For now just 1 chunk,
     * starting at 0. */
    memset(chunk_coords, 0, sizeof(chunk_coords)); /*DSINC*/
    p = dkey_buf;
    *p++ = (uint8_t)'\0';
    for(i = 0; i < (uint64_t)ndims; i++)
        UINT64ENCODE(p, chunk_coords[i])

    /* Set up dkey */
    daos_iov_set(&dkey, dkey_buf, (daos_size_t)(1 + ((size_t)ndims * sizeof(chunk_coords[0]))));

    /* Check for vlen */
    if(H5T_NO_CLASS == (type_class = H5Tget_class(mem_type_id)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype class")
    if(type_class == H5T_VLEN) {
        is_vl = TRUE;

        /* Calculate base type size */
        if((base_type_id = H5Tget_super(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype base type")
        if(0 == (base_type_size = H5Tget_size(base_type_id)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype base type size")
    } /* end if */
    else if(type_class == H5T_STRING) {
        /* check for vlen string */
        if((is_vl_str = H5Tis_variable_str(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check for variable length string")
        if(is_vl_str)
            is_vl = TRUE;
    } /* end if */

    /* Check for variable length */
    if(is_vl) {
        H5_daos_vl_mem_ud_t mem_ud;
        H5_daos_vl_file_ud_t file_ud;

        /* Get number of elements in selection */
        if((num_elem = H5Sget_select_npoints(real_mem_space_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

        /* Allocate array of akey pointers */
        if(NULL == (akeys = (uint8_t **)DV_calloc((size_t)num_elem * sizeof(uint8_t *))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey array")

        /* Allocate array of iods */
        if(NULL == (iods = (daos_iod_t *)DV_calloc((size_t)num_elem * sizeof(daos_iod_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O descriptor array")

        /* Fill in size fields of iod as DAOS_REC_ANY so we can read the vl
         * sizes */
        for(i = 0; i < (uint64_t)num_elem; i++)
            iods[i].iod_size = DAOS_REC_ANY;

        /* Iterate over file selection.  Note the bogus buffer and type_id,
         * these don't matter since the "elem" parameter of the callback is not
         * used. */
        file_ud.akeys = akeys;
        file_ud.iods = iods;
        file_ud.idx = 0;
        if(H5Diterate((void *)buf, mem_type_id, real_file_space_id, H5_daos_dataset_file_vl_cb, &file_ud) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "file selection iteration failed")
        assert(file_ud.idx == (uint64_t)num_elem);

        /* Read vl sizes from dataset */
        /* Note cast to unsigned reduces width to 32 bits.  Should eventually
         * check for overflow and iterate over 2^32 size blocks */
        if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, (unsigned)num_elem, iods, NULL, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't read vl data sizes from dataset: %s", H5_daos_err_to_string(ret))

        /* Allocate array of sg_iovs */
        if(NULL == (sg_iovs = (daos_iov_t *)DV_malloc((size_t)num_elem * sizeof(daos_iov_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list")

        /* Allocate array of sgls */
        if(NULL == (sgls = (daos_sg_list_t *)DV_malloc((size_t)num_elem * sizeof(daos_sg_list_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list array")

        /* Iterate over memory selection */
        mem_ud.iods = iods;
        mem_ud.sgls = sgls;
        mem_ud.sg_iovs = sg_iovs;
        mem_ud.is_vl_str = is_vl_str;
        mem_ud.base_type_size = base_type_size;
        mem_ud.offset = 0;
        mem_ud.idx = 0;
        if(H5Diterate((void *)buf, mem_type_id, real_mem_space_id, H5_daos_dataset_mem_vl_rd_cb, &mem_ud) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "memory selection iteration failed")
        assert(mem_ud.idx == (uint64_t)num_elem);

        /* Read data from dataset */
        /* Note cast to unsigned reduces width to 32 bits.  Should eventually
         * check for overflow and iterate over 2^32 size blocks */
        if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, (unsigned)((uint64_t)num_elem - mem_ud.offset), iods, sgls, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else {
        daos_iod_t iod;
        daos_sg_list_t sgl;
        uint8_t akey = H5_DAOS_CHUNK_KEY;
        size_t tot_nseq;
        size_t file_type_size;
        htri_t types_equal;

        /* Get datatype size */
        if((file_type_size = H5Tget_size(dset->type_id)) == 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype size")

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)&akey, (daos_size_t)(sizeof(akey)));
        daos_csum_set(&iod.iod_kcsum, NULL, 0);
        iod.iod_size = file_type_size;
        iod.iod_type = DAOS_IOD_ARRAY;

        /* Check if the types are equal */
        if((types_equal = H5Tequal(dset->type_id, mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATATYPE, H5E_CANTCOMPARE, FAIL, "can't check if types are equal")
        if(types_equal) {
            /* No type conversion necessary */
            /* Check for memory space is H5S_ALL, use file space in this case */
            if(mem_space_id == H5S_ALL) {
                /* Calculate both recxs and sg_iovs at the same time from file space */
                if(H5_daos_sel_to_recx_iov(real_file_space_id, file_type_size, buf, &recxs, &sg_iovs, &tot_nseq) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
                iod.iod_nr = (unsigned)tot_nseq;
                sgl.sg_nr = (uint32_t)tot_nseq;
                sgl.sg_nr_out = 0;
            } /* end if */
            else {
                /* Calculate recxs from file space */
                if(H5_daos_sel_to_recx_iov(real_file_space_id, file_type_size, buf, &recxs, NULL, &tot_nseq) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
                iod.iod_nr = (unsigned)tot_nseq;

                /* Calculate sg_iovs from mem space */
                if(H5_daos_sel_to_recx_iov(real_mem_space_id, file_type_size, buf, NULL, &sg_iovs, &tot_nseq) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
                sgl.sg_nr = (uint32_t)tot_nseq;
                sgl.sg_nr_out = 0;
            } /* end else */

            /* Point iod and sgl to lists generated above */
            iod.iod_recxs = recxs;
            sgl.sg_iovs = sg_iovs;

            /* Read data from dataset */
            if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", H5_daos_err_to_string(ret))
        } /* end if */
        else {
            size_t nseq_tmp;
            size_t nelem_tmp;
            hsize_t sel_off;
            size_t sel_len;
            size_t mem_type_size;
            hbool_t fill_bkg = FALSE;
            hbool_t contig;

            /* Type conversion necessary */
            /* Get number of elements in selection */
            if((num_elem = H5Sget_select_npoints(real_mem_space_id)) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

            /* Calculate recxs from file space */
            if(H5_daos_sel_to_recx_iov(real_file_space_id, file_type_size, buf, &recxs, NULL, &tot_nseq) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
            iod.iod_nr = (unsigned)tot_nseq;
            iod.iod_recxs = recxs;

            /* Set up constant sgl info */
            sgl.sg_nr = 1;
            sgl.sg_nr_out = 0;
            sgl.sg_iovs = &sg_iov;

            /* Check for contiguous memory buffer */

            /* Initialize selection iterator  */
            if(NULL == (sel_iter = H5Sselect_iter_init(real_mem_space_id, (size_t)1)))
                D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
            sel_iter_init = TRUE;       /* Selection iteration info has been initialized */

            /* Get the sequence list - only check the first sequence because we only
             * care if it is contiguous and if so where the contiguous selection
             * begins */
            if(H5Sselect_get_seq_list(real_mem_space_id, 0, sel_iter, (size_t)1, (size_t)-1, &nseq_tmp, &nelem_tmp, &sel_off, &sel_len) < 0)
                D_GOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
            contig = (sel_len == (size_t)num_elem);

            /* Initialize type conversion */
            if(H5_daos_tconv_init(dset->type_id, &file_type_size, mem_type_id, &mem_type_size, (size_t)num_elem, &tconv_buf, &bkg_buf, contig ? &reuse : NULL, &fill_bkg) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion")

            /* Reuse buffer as appropriate */
            if(contig) {
                sel_off *= (hsize_t)mem_type_size;
                if(reuse == H5_DAOS_TCONV_REUSE_TCONV)
                    tconv_buf = (char *)buf + (size_t)sel_off;
                else if(reuse == H5_DAOS_TCONV_REUSE_BKG)
                    bkg_buf = (char *)buf + (size_t)sel_off;
            } /* end if */

            /* Set sg_iov to point to tconv_buf */
            daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)num_elem * (daos_size_t)file_type_size);

            /* Read data to tconv_buf */
            if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
                D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from attribute: %s", H5_daos_err_to_string(ret))

            /* Gather data to background buffer if necessary */
            if(fill_bkg && (reuse != H5_DAOS_TCONV_REUSE_BKG))
                if(H5Dgather(real_mem_space_id, buf, mem_type_id, (size_t)num_elem * mem_type_size, bkg_buf, NULL, NULL) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to background buffer")

            /* Perform type conversion */
            if(H5Tconvert(dset->type_id, mem_type_id, (size_t)num_elem, tconv_buf, bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

            /* Scatter data to memory buffer if necessary */
            if(reuse != H5_DAOS_TCONV_REUSE_TCONV) {
                H5_daos_scatter_cb_ud_t scatter_cb_ud;

                scatter_cb_ud.buf = tconv_buf;
                scatter_cb_ud.len = (size_t)num_elem * mem_type_size;
                if(H5Dscatter(H5_daos_scatter_cb, &scatter_cb_ud, mem_type_id, real_mem_space_id, buf) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't scatter data to read buffer")
            } /* end if */
        } /* end else */
    } /* end else */

done:
    /* Free memory */
    iods = (daos_iod_t *)DV_free(iods);
    if(recxs != &recx)
        DV_free(recxs);
    sgls = (daos_sg_list_t *)DV_free(sgls);
    if(sg_iovs != &sg_iov)
        DV_free(sg_iovs);
    if(tconv_buf && (reuse != H5_DAOS_TCONV_REUSE_TCONV))
        DV_free(tconv_buf);
    if(bkg_buf && (reuse != H5_DAOS_TCONV_REUSE_BKG))
        DV_free(bkg_buf);

    if(akeys) {
        for(i = 0; i < (uint64_t)num_elem; i++)
            DV_free(akeys[i]);
        DV_free(akeys);
    } /* end if */

    if(base_type_id != FAIL)
        if(H5Idec_ref(base_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close base type ID")

    /* Release selection iterator */
    if(sel_iter_init && H5Sselect_iter_release(sel_iter) < 0)
        D_DONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_dataset_read() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_mem_vl_wr_cb
 *
 * Purpose:     H5Diterate callback for iterating over the memory space
 *              before writing vl data.  Sets up scatter gather lists
 *              (sgls) and sets the record sizes in iods.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              May, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_dataset_mem_vl_wr_cb(void *_elem, hid_t DV_ATTR_UNUSED type_id,
    unsigned DV_ATTR_UNUSED ndim, const hsize_t DV_ATTR_UNUSED *point,
    void *_udata)
{
    H5_daos_vl_mem_ud_t *udata = (H5_daos_vl_mem_ud_t *)_udata;
    herr_t ret_value = SUCCEED;

    /* Set up constant sgl info */
    udata->sgls[udata->idx].sg_nr = 1;
    udata->sgls[udata->idx].sg_nr_out = 0;
    udata->sgls[udata->idx].sg_iovs = &udata->sg_iovs[udata->idx];

    /* Check for vlen string */
    if(udata->is_vl_str) {
        /* Find string for this element */
        char *elem = *(char **)_elem;

        /* Set string length in iod and buffer location in sgl.  If we are
         * writing an empty string ("\0"), increase the size by one to
         * differentiate it from NULL strings.  Note that this will cause the
         * read buffer to be one byte longer than it needs to be in this case.
         * This should not cause any ill effects. */
        if(elem) {
            udata->iods[udata->idx].iod_size = (daos_size_t)strlen(elem);
            if(udata->iods[udata->idx].iod_size == 0)
                udata->iods[udata->idx].iod_size = 1;
            daos_iov_set(&udata->sg_iovs[udata->idx], (void *)elem, udata->iods[udata->idx].iod_size);
        } /* end if */
        else {
            udata->iods[udata->idx].iod_size = 0;
            daos_iov_set(&udata->sg_iovs[udata->idx], NULL, 0);
        } /* end else */
    } /* end if */
    else {
        /* Standard vlen, find hvl_t struct for this element */
        hvl_t *elem = (hvl_t *)_elem;

        assert(udata->base_type_size > 0);

        /* Set buffer length in iod and buffer location in sgl */
        if(elem->len > 0) {
            udata->iods[udata->idx].iod_size = (daos_size_t)(elem->len * udata->base_type_size);
            daos_iov_set(&udata->sg_iovs[udata->idx], (void *)elem->p, udata->iods[udata->idx].iod_size);
        } /* end if */
        else {
            udata->iods[udata->idx].iod_size = 0;
            daos_iov_set(&udata->sg_iovs[udata->idx], NULL, 0);
        } /* end else */
    } /* end else */

    /* Advance idx */
    udata->idx++;

    /* DSINC - This function used to always return SUCCEED without needing an
     * herr_t. Might need an additional FUNC_LEAVE macro to do this, or modify
     * the current one to take in the ret_value.
     */
    D_FUNC_LEAVE
} /* end H5_daos_dataset_mem_vl_wr_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_write
 *
 * Purpose:     Writes raw data from a buffer into a dataset.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              November, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dataset_write(void *_dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t DV_ATTR_UNUSED dxpl_id,
    const void *buf, void DV_ATTR_UNUSED **req)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    int ndims;
    hsize_t dim[H5S_MAX_RANK];
    hid_t real_file_space_id;
    hid_t real_mem_space_id;
    hssize_t num_elem;
    uint64_t chunk_coords[H5S_MAX_RANK];
    daos_key_t dkey;
    uint8_t **akeys = NULL;
    daos_iod_t *iods = NULL;
    daos_sg_list_t *sgls = NULL;
    daos_recx_t recx;
    daos_recx_t *recxs = &recx;
    daos_iov_t sg_iov;
    daos_iov_t *sg_iovs = &sg_iov;
    uint8_t dkey_buf[1 + H5S_MAX_RANK];
    hid_t base_type_id = FAIL;
    size_t base_type_size = 0;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    H5T_class_t type_class;
    hbool_t is_vl = FALSE;
    htri_t is_vl_str = FALSE;
    uint8_t *p;
    int ret;
    uint64_t i;
    herr_t ret_value = SUCCEED;

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL")
    if(!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "write buffer is NULL")

    /* Check for write access */
    if(!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")

    /* Get "real" space ids */
    if(file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;
    if(mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else
        real_mem_space_id = mem_space_id;

    /* Get number of elements in selection */
    if((num_elem = H5Sget_select_npoints(real_mem_space_id)) < 0)
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

    /* Encode dkey (chunk coordinates).  Prefix with '\0' to avoid accidental
     * collisions with other d-keys in this object.  For now just 1 chunk,
     * starting at 0. */
    memset(chunk_coords, 0, sizeof(chunk_coords)); /*DSINC*/
    p = dkey_buf;
    *p++ = (uint8_t)'\0';
    for(i = 0; i < (uint64_t)ndims; i++)
        UINT64ENCODE(p, chunk_coords[i])

    /* Set up dkey */
    daos_iov_set(&dkey, dkey_buf, (daos_size_t)(1 + ((size_t)ndims * sizeof(chunk_coords[0]))));

    /* Check for vlen */
    if(H5T_NO_CLASS == (type_class = H5Tget_class(mem_type_id)))
        D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype class")
    if(type_class == H5T_VLEN) {
        is_vl = TRUE;

        /* Calculate base type size */
        if((base_type_id = H5Tget_super(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype base type")
        if(0 == (base_type_size = H5Tget_size(base_type_id)))
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype base type size")
    } /* end if */
    else if(type_class == H5T_STRING) {
        /* check for vlen string */
        if((is_vl_str = H5Tis_variable_str(mem_type_id)) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check for variable length string")
        if(is_vl_str)
            is_vl = TRUE;
    } /* end if */

    /* Check for variable length */
    if(is_vl) {
        H5_daos_vl_mem_ud_t mem_ud;
        H5_daos_vl_file_ud_t file_ud;

        /* Allocate array of akey pointers */
        if(NULL == (akeys = (uint8_t **)DV_calloc((size_t)num_elem * sizeof(uint8_t *))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for akey array")

        /* Allocate array of iods */
        if(NULL == (iods = (daos_iod_t *)DV_calloc((size_t)num_elem * sizeof(daos_iod_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for I/O descriptor array")

        /* Allocate array of sg_iovs */
        if(NULL == (sg_iovs = (daos_iov_t *)DV_malloc((size_t)num_elem * sizeof(daos_iov_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list")

        /* Allocate array of sgls */
        if(NULL == (sgls = (daos_sg_list_t *)DV_malloc((size_t)num_elem * sizeof(daos_sg_list_t))))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for scatter gather list array")

        /* Iterate over memory selection */
        mem_ud.iods = iods;
        mem_ud.sgls = sgls;
        mem_ud.sg_iovs = sg_iovs;
        mem_ud.is_vl_str = is_vl_str;
        mem_ud.base_type_size = base_type_size;
        mem_ud.idx = 0;
        if(H5Diterate((void *)buf, mem_type_id, real_mem_space_id, H5_daos_dataset_mem_vl_wr_cb, &mem_ud) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "memory selection iteration failed")
        assert(mem_ud.idx == (uint64_t)num_elem);

        /* Iterate over file selection.  Note the bogus buffer and type_id,
         * these don't matter since the "elem" parameter of the callback is not
         * used. */
        file_ud.akeys = akeys;
        file_ud.iods = iods;
        file_ud.idx = 0;
        if(H5Diterate((void *)buf, mem_type_id, real_file_space_id, H5_daos_dataset_file_vl_cb, &file_ud) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "file selection iteration failed")
        assert(file_ud.idx == (uint64_t)num_elem);

        /* Write data to dataset */
        /* Note cast to unsigned reduces width to 32 bits.  Should eventually
         * check for overflow and iterate over 2^32 size blocks */
        if(0 != (ret = daos_obj_update(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, (unsigned)num_elem, iods, sgls, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data to dataset: %s", H5_daos_err_to_string(ret))
    } /* end if */
    else {
        daos_iod_t iod;
        daos_sg_list_t sgl;
        uint8_t akey = H5_DAOS_CHUNK_KEY;
        size_t tot_nseq;
        size_t file_type_size;
        size_t mem_type_size;
        hbool_t fill_bkg = FALSE;

        /* Initialize type conversion */
        if(H5_daos_tconv_init(mem_type_id, &mem_type_size, dset->type_id, &file_type_size, (size_t)num_elem, &tconv_buf, &bkg_buf, NULL, &fill_bkg) < 0)
            D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion")

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)&akey, (daos_size_t)(sizeof(akey)));
        daos_csum_set(&iod.iod_kcsum, NULL, 0);
        iod.iod_size = file_type_size;
        iod.iod_type = DAOS_IOD_ARRAY;

        /* Build recxs and sg_iovs */

        /* Check for type conversion */
        if(tconv_buf) {
            /* Calculate recxs from file space */
            if(H5_daos_sel_to_recx_iov(real_file_space_id, file_type_size, (void *)buf, &recxs, NULL, &tot_nseq) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
            iod.iod_nr = (unsigned)tot_nseq;
            iod.iod_recxs = recxs;

            /* Set up constant sgl info */
            sgl.sg_nr = 1;
            sgl.sg_nr_out = 0;
            sgl.sg_iovs = &sg_iov;

            /* Check if we need to fill background buffer */
            if(fill_bkg) {
                assert(bkg_buf);

                /* Set sg_iov to point to background buffer */
                daos_iov_set(&sg_iov, bkg_buf, (daos_size_t)num_elem * (daos_size_t)file_type_size);

                /* Read data from dataset to background buffer */
                if(0 != (ret = daos_obj_fetch(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
                    D_GOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", H5_daos_err_to_string(ret))

                /* Reset iod_size, if the dataset was not allocated then it could
                 * have been overwritten by daos_obj_fetch */
                iod.iod_size = file_type_size;
            } /* end if */

            /* Gather data to conversion buffer */
            if(H5Dgather(real_mem_space_id, buf, mem_type_id, (size_t)num_elem * mem_type_size, tconv_buf, NULL, NULL) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to conversion buffer")

            /* Perform type conversion */
            if(H5Tconvert(mem_type_id, dset->type_id, (size_t)num_elem, tconv_buf, bkg_buf, dxpl_id) < 0)
                D_GOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

            /* Set sg_iovs to write from tconv_buf */
            daos_iov_set(&sg_iov, tconv_buf, (daos_size_t)num_elem * (daos_size_t)file_type_size);
        } /* end if */
        else {
            /* Check for memory space is H5S_ALL, use file space in this case */
            if(mem_space_id == H5S_ALL) {
                /* Calculate both recxs and sg_iovs at the same time from file space */
                if(H5_daos_sel_to_recx_iov(real_file_space_id, file_type_size, (void *)buf, &recxs, &sg_iovs, &tot_nseq) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
                iod.iod_nr = (unsigned)tot_nseq;
                sgl.sg_nr = (uint32_t)tot_nseq;
                sgl.sg_nr_out = 0;
            } /* end if */
            else {
                /* Calculate recxs from file space */
                if(H5_daos_sel_to_recx_iov(real_file_space_id, file_type_size, (void *)buf, &recxs, NULL, &tot_nseq) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
                iod.iod_nr = (unsigned)tot_nseq;

                /* Calculate sg_iovs from mem space */
                if(H5_daos_sel_to_recx_iov(real_mem_space_id, file_type_size, (void *)buf, NULL, &sg_iovs, &tot_nseq) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate sequence lists for DAOS I/O")
                sgl.sg_nr = (uint32_t)tot_nseq;
                sgl.sg_nr_out = 0;
            } /* end else */

            /* Point iod and sgl to lists generated above */
            iod.iod_recxs = recxs;
            sgl.sg_iovs = sg_iovs;
        } /* end else */

        /* Write data to dataset */
        if(0 != (ret = daos_obj_update(dset->obj.obj_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data to dataset: %s", H5_daos_err_to_string(ret))
    } /* end else */

done:
    /* Free memory */
    iods = (daos_iod_t *)DV_free(iods);
    if(recxs != &recx)
        DV_free(recxs);
    sgls = (daos_sg_list_t *)DV_free(sgls);
    if(sg_iovs && (sg_iovs != &sg_iov))
        DV_free(sg_iovs);
    tconv_buf = DV_free(tconv_buf);
    bkg_buf = DV_free(bkg_buf);

    if(akeys) {
        for(i = 0; i < (uint64_t)num_elem; i++)
            DV_free(akeys[i]);
        DV_free(akeys);
    } /* end if */

    if(base_type_id != FAIL)
        if(H5Idec_ref(base_type_id) < 0)
            D_DONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close base type ID")

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_dataset_write() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_get
 *
 * Purpose:     Gets certain information about a dataset
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
H5_daos_dataset_get(void *_dset, H5VL_dataset_get_t get_type,
    hid_t DV_ATTR_UNUSED dxpl_id, void DV_ATTR_UNUSED **req, va_list arguments)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    herr_t       ret_value = SUCCEED;    /* Return value */

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")

    switch (get_type) {
        case H5VL_DATASET_GET_DCPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's creation property list */
                if((*plist_id = H5Pcopy(dset->dcpl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset creation property list")

                break;
            } /* end block */
        case H5VL_DATASET_GET_DAPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's access property list */
                if((*plist_id = H5Pcopy(dset->dapl_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset access property list")

                break;
            } /* end block */
        case H5VL_DATASET_GET_SPACE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's dataspace */
                if((*ret_id = H5Scopy(dset->space_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace ID of dataset");
                break;
            } /* end block */
        case H5VL_DATASET_GET_SPACE_STATUS:
            {
                H5D_space_status_t *allocation = va_arg(arguments, H5D_space_status_t *);

                /* Retrieve the dataset's space status */
                *allocation = H5D_SPACE_STATUS_NOT_ALLOCATED;
                break;
            } /* end block */
        case H5VL_DATASET_GET_TYPE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's datatype */
                if((*ret_id = H5Tcopy(dset->type_id)) < 0)
                    D_GOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype ID of dataset")
                break;
            } /* end block */
        case H5VL_DATASET_GET_STORAGE_SIZE:
        case H5VL_DATASET_GET_OFFSET:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from dataset")
    } /* end switch */

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_dataset_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_dataset_close
 *
 * Purpose:     Closes a DAOS HDF5 dataset.
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
H5_daos_dataset_close(void *_dset, hid_t DV_ATTR_UNUSED dxpl_id,
    void DV_ATTR_UNUSED **req)
{
    H5_daos_dset_t *dset = (H5_daos_dset_t *)_dset;
    int ret;
    herr_t ret_value = SUCCEED;

    if(!_dset)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataset object is NULL")

    if(--dset->obj.item.rc == 0) {
        /* Free dataset data structures */
        if(dset->obj.item.open_req)
            H5_daos_req_free_int(dset->obj.item.open_req);
        if(!daos_handle_is_inval(dset->obj.obj_oh))
            if(0 != (ret = daos_obj_close(dset->obj.obj_oh, NULL /*event*/)))
                D_DONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "can't close dataset DAOS object: %s", H5_daos_err_to_string(ret))
        if(dset->type_id != FAIL && H5Idec_ref(dset->type_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's datatype")
        if(dset->space_id != FAIL && H5Idec_ref(dset->space_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataset's dataspace")
        if(dset->dcpl_id != FAIL && H5Idec_ref(dset->dcpl_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dcpl")
        if(dset->dapl_id != FAIL && H5Idec_ref(dset->dapl_id) < 0)
            D_DONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dapl")
        dset = H5FL_FREE(H5_daos_dset_t, dset);
    } /* end if */

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_dataset_close() */

