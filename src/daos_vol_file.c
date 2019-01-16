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
 * library.  File routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */
#include "daos_vol_config.h"    /* DAOS connector configuration header     */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Prototypes */
static herr_t H5_daos_file_flush(H5_daos_file_t *file);
static herr_t H5_daos_file_close_helper(H5_daos_file_t *file,
    hid_t dxpl_id, void **req);


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_create
 *
 * Purpose:     Creates a file as a daos HDF5 file.
 *
 * Return:      Success:        the file ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              September, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_file_create(const char *name, unsigned flags, hid_t fcpl_id,
    hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5_daos_fapl_t *fa = NULL;
    H5_daos_file_t *file = NULL;
    daos_iov_t glob;
    uint64_t gh_buf_size;
    char gh_buf_static[H5_DAOS_GH_BUF_SIZE];
    char *gh_buf_dyn = NULL;
    char *gh_buf = gh_buf_static;
    daos_obj_id_t gmd_oid = {0, 0};
    uint8_t *p;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    /*
     * Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if(0==(flags & (H5F_ACC_EXCL|H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL;      /*default*/
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* Get information from the FAPL */
    /*
     * XXX: DSINC - may no longer need to use this VOL info.
     */
    if(H5Pget_vol_info(fapl_id, (void **) &fa) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct")

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct")
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    file->item.rc = 1;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->flags = flags;
    file->max_oid = 0;
    file->max_oid_dirty = FALSE;
    if((file->fcpl_id = H5Pcopy(fcpl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fcpl")
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Duplicate communicator and Info object. */
    /*
     * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
     */
    if(FAIL == H5FDmpi_comm_info_dup(fa ? fa->comm : pool_comm_g, fa ? fa->info : MPI_INFO_NULL, &file->comm, &file->info))
        D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "failed to duplicate MPI communicator and info")

    /* Obtain the process rank and size from the communicator attached to the
     * fapl ID */
    MPI_Comm_rank(fa ? fa->comm : pool_comm_g, &file->my_rank);
    MPI_Comm_size(fa ? fa->comm : pool_comm_g, &file->num_procs);

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Determine if we requested collective object ops for the file */
    if(H5Pget_all_coll_metadata_ops(fapl_id, &file->collective) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get collective access property")

    /* Generate oid for global metadata object */
    daos_obj_generate_id(&gmd_oid, DAOS_OF_DKEY_HASHED | DAOS_OF_AKEY_HASHED, DAOS_OC_TINY_RW);

    if(file->my_rank == 0) {
        /* If there are other processes and we fail we must bcast anyways so they
         * don't hang */
        if(file->num_procs > 1)
            must_bcast = TRUE;

        /* Delete the container if H5F_ACC_TRUNC is set.  This shouldn't cause a
         * problem even if the container doesn't exist. */
        /* Need to handle EXCL correctly DSINC */
        if(flags & H5F_ACC_TRUNC)
            if(0 != (ret = daos_cont_destroy(H5_daos_poh_g, file->uuid, 1, NULL /*event*/)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't destroy container: %s", H5_daos_err_to_string(ret))

        /* Create the container for the file */
        if(0 != (ret = daos_cont_create(H5_daos_poh_g, file->uuid, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't create container: %s", H5_daos_err_to_string(ret))

        /* Open the container */
        if(0 != (ret = daos_cont_open(H5_daos_poh_g, file->uuid, DAOS_COO_RW, &file->coh, NULL /*&file->co_info*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open container: %s", H5_daos_err_to_string(ret))

        /* Open global metadata object */
        if(0 != (ret = daos_obj_open(file->coh, gmd_oid, DAOS_OO_RW, &file->glob_md_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open global metadata object: %s", H5_daos_err_to_string(ret))

        /* Bcast global container handle if there are other processes */
        if(file->num_procs > 1) {
            /* Calculate size of the global container handle */
            glob.iov_buf = NULL;
            glob.iov_buf_len = 0;
            glob.iov_len = 0;
            if(0 != (ret = daos_cont_local2global(file->coh, &glob)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't get global container handle size: %s", H5_daos_err_to_string(ret))
            gh_buf_size = (uint64_t)glob.iov_buf_len;

            /* Check if the global handle won't fit into the static buffer */
            assert(sizeof(gh_buf_static) >= sizeof(uint64_t));
            if(gh_buf_size + sizeof(uint64_t) > sizeof(gh_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (gh_buf_dyn = (char *)DV_malloc(gh_buf_size + sizeof(uint64_t))))
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate space for global container handle")

                /* Use dynamic buffer */
                gh_buf = gh_buf_dyn;
            } /* end if */

            /* Encode handle length */
            p = (uint8_t *)gh_buf;
            UINT64ENCODE(p, gh_buf_size)

            /* Retrieve global container handle */
            glob.iov_buf = (char *)p;
            glob.iov_buf_len = gh_buf_size;
            glob.iov_len = 0;
            if(0 != (ret = daos_cont_local2global(file->coh, &glob)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't get global container handle: %s", H5_daos_err_to_string(ret))
            assert(glob.iov_len == glob.iov_buf_len);

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast gh_buf */
            if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)sizeof(gh_buf_static), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast global container handle")

            /* Need a second bcast if we had to allocate a dynamic buffer */
            if(gh_buf == gh_buf_dyn)
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)gh_buf_size, MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                    D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast global container handle (second broadcast)")
        } /* end if */
    } /* end if */
    else {
        /* Receive global handle */
        if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)sizeof(gh_buf_static), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
            D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't receive broadcasted global container handle")

        /* Decode handle length */
        p = (uint8_t *)gh_buf;
        UINT64DECODE(p, gh_buf_size)

        /* Check for gh_buf_size set to 0 - indicates failure */
        if(gh_buf_size == 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "lead process failed to open file")

        /* Check if we need to perform another bcast */
        if(gh_buf_size + sizeof(uint64_t) > sizeof(gh_buf_static)) {
            /* Check if we need to allocate a dynamic buffer */
            if(gh_buf_size > sizeof(gh_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (gh_buf_dyn = (char *)DV_malloc(gh_buf_size)))
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate space for global pool handle")
                gh_buf = gh_buf_dyn;
            } /* end if */

            /* Receive global handle */
            if(MPI_SUCCESS != MPI_Bcast(gh_buf_dyn, (int)gh_buf_size, MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't receive broadcasted global container handle (second broadcast)")

            p = (uint8_t *)gh_buf;
        } /* end if */

        /* Create local container handle */
        glob.iov_buf = (char *)p;
        glob.iov_buf_len = gh_buf_size;
        glob.iov_len = gh_buf_size;
        if(0 != (ret = daos_cont_global2local(H5_daos_poh_g, glob, &file->coh)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't get local container handle: %s", H5_daos_err_to_string(ret))

        /* Open global metadata object */
        if(0 != (ret = daos_obj_open(file->coh, gmd_oid, DAOS_OO_RW, &file->glob_md_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open global metadata object: %s", H5_daos_err_to_string(ret))
    } /* end else */
 
    /* Create root group */
    if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_create_helper(file, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req, NULL, NULL, 0, TRUE)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group")
    assert(file->root_grp->obj.oid.lo == (uint64_t)1);

    ret_value = (void *)file;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast bcast_buf_64 as '0' if necessary - this will trigger failures
         * in the other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(gh_buf_static, 0, sizeof(gh_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(gh_buf_static, sizeof(gh_buf_static), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                D_DONE_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast empty global container handle")
        } /* end if */

        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    if(fa)
        H5VLfree_connector_info(H5_DAOS_g, fa);

    /* Clean up */
    DV_free(gh_buf_dyn);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_file_create() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_open
 *
 * Purpose:     Opens a file as a daos HDF5 file.
 *
 * Return:      Success:        the file ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
void *
H5_daos_file_open(const char *name, unsigned flags, hid_t fapl_id,
    hid_t dxpl_id, void **req)
{
    H5_daos_fapl_t *fa = NULL;
    H5_daos_file_t *file = NULL;
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    daos_iov_t glob;
    uint64_t gh_len;
    char foi_buf_static[H5_DAOS_FOI_BUF_SIZE];
    char *foi_buf_dyn = NULL;
    char *foi_buf = foi_buf_static;
    void *gcpl_buf = NULL;
    uint64_t gcpl_len;
    daos_obj_id_t gmd_oid = {0, 0};
    daos_obj_id_t root_grp_oid = {0, 0};
    uint8_t *p;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    /* Get information from the FAPL */
    /*
     * XXX: DSINC - may no longer need to use this VOL info.
     */
    if(H5Pget_vol_info(fapl_id, (void **) &fa) < 0)
        D_GOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get DAOS info struct")

#ifdef DV_HAVE_SNAP_OPEN_ID
    if(H5Pget(fapl_id, H5_DAOS_SNAP_OPEN_ID, &snap_id) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for snap ID")

    /* Check for opening a snapshot with write access (disallowed) */
    if((snap_id != H5_DAOS_SNAP_ID_INVAL) && (flags & H5F_ACC_RDWR))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "write access requested to snapshot - disallowed")
#endif

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct")
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    file->item.rc = 1;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->flags = flags;
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Duplicate communicator and Info object. */
    /*
     * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
     */
    if(FAIL == H5FDmpi_comm_info_dup(fa ? fa->comm : pool_comm_g, fa ? fa->info : MPI_INFO_NULL, &file->comm, &file->info))
        D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "failed to duplicate MPI communicator and info")

    /* Obtain the process rank and size from the communicator attached to the
     * fapl ID */
    MPI_Comm_rank(fa ? fa->comm : pool_comm_g, &file->my_rank);
    MPI_Comm_size(fa ? fa->comm : pool_comm_g, &file->num_procs);

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Generate oid for global metadata object */
    daos_obj_generate_id(&gmd_oid, DAOS_OF_DKEY_HASHED | DAOS_OF_AKEY_HASHED, DAOS_OC_TINY_RW);

    /* Generate root group oid */
    H5_daos_oid_encode(&root_grp_oid, (uint64_t)1, H5I_GROUP);

    /* Determine if we requested collective object ops for the file */
    if(H5Pget_all_coll_metadata_ops(fapl_id, &file->collective) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get collective access property")

    if(file->my_rank == 0) {
        daos_key_t dkey;
        daos_iod_t iod;
        daos_sg_list_t sgl;
        daos_iov_t sg_iov;
        char int_md_key[] = H5_DAOS_INT_MD_KEY;
        char max_oid_key[] = H5_DAOS_MAX_OID_KEY;

        /* If there are other processes and we fail we must bcast anyways so they
         * don't hang */
        if(file->num_procs > 1)
            must_bcast = TRUE;

        /* Open the container */
        if(0 != (ret = daos_cont_open(H5_daos_poh_g, file->uuid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->coh, NULL /*&file->co_info*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open container: %s", H5_daos_err_to_string(ret))

        /* If a snapshot was requested, use it as the epoch, otherwise query it
         */
#ifdef DV_HAVE_SNAP_OPEN_ID
        if(snap_id != H5_DAOS_SNAP_ID_INVAL) {
            epoch = (daos_epoch_t)snap_id;

            assert(!(flags & H5F_ACC_RDWR));
        } /* end if */
        else {
#endif
#ifdef DV_HAVE_SNAP_OPEN_ID
        } /* end else */
#endif

        /* Open global metadata object */
        if(0 != (ret = daos_obj_open(file->coh, gmd_oid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->glob_md_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open global metadata object: %s", H5_daos_err_to_string(ret))

        /* Read max OID from gmd obj */
        /* Set up dkey */
        daos_iov_set(&dkey, int_md_key, (daos_size_t)(sizeof(int_md_key) - 1));

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)max_oid_key, (daos_size_t)(sizeof(max_oid_key) - 1));
        daos_csum_set(&iod.iod_kcsum, NULL, 0);
        iod.iod_nr = 1u;
        iod.iod_size = (uint64_t)8;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, &file->max_oid, (daos_size_t)8);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Read max OID from gmd obj */
        if(0 != (ret = daos_obj_fetch(file->glob_md_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*maps*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTDECODE, NULL, "can't read max OID from global metadata object: %s", H5_daos_err_to_string(ret))

        /* Open root group */
        if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_open_helper(file, root_grp_oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req, (file->num_procs > 1) ? &gcpl_buf : NULL, &gcpl_len)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't open root group")

        /* Bcast global handles if there are other processes */
        if(file->num_procs > 1) {
            /* Calculate size of the global container handle */
            glob.iov_buf = NULL;
            glob.iov_buf_len = 0;
            glob.iov_len = 0;
            if(0 != (ret = daos_cont_local2global(file->coh, &glob)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't get global container handle size: %s", H5_daos_err_to_string(ret))
            gh_len = (uint64_t)glob.iov_buf_len;

            /* Check if the file open info won't fit into the static buffer */
            if(gh_len + gcpl_len + 3 * sizeof(uint64_t) > sizeof(foi_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (foi_buf_dyn = (char *)DV_malloc(gh_len + gcpl_len + 3 * sizeof(uint64_t))))
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate space for global container handle")

                /* Use dynamic buffer */
                foi_buf = foi_buf_dyn;
            } /* end if */

            /* Encode handle length */
            p = (uint8_t *)foi_buf;
            UINT64ENCODE(p, gh_len)

            /* Encode GCPL length */
            UINT64ENCODE(p, gcpl_len)

            /* Encode max OID */
            UINT64ENCODE(p, file->max_oid)

            /* Retrieve global container handle */
            glob.iov_buf = (char *)p;
            glob.iov_buf_len = gh_len;
            glob.iov_len = 0;
            if(0 != (ret = daos_cont_local2global(file->coh, &glob)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't get file open info: %s", H5_daos_err_to_string(ret))
            assert(glob.iov_len == glob.iov_buf_len);

            /* Copy GCPL buffer */
            memcpy(p + gh_len, gcpl_buf, gcpl_len);

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast foi_buf */
            if(MPI_SUCCESS != MPI_Bcast(foi_buf, (int)sizeof(foi_buf_static), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast global container handle")

            /* Need a second bcast if we had to allocate a dynamic buffer */
            if(foi_buf == foi_buf_dyn)
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)(gh_len + gcpl_len), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                    D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast file open info (second broadcast)")
        } /* end if */
    } /* end if */
    else {
        /* Receive file open info */
        if(MPI_SUCCESS != MPI_Bcast(foi_buf, (int)sizeof(foi_buf_static), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
            D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't receive broadcasted global container handle")

        /* Decode handle length */
        p = (uint8_t *)foi_buf;
        UINT64DECODE(p, gh_len)

        /* Check for gh_len set to 0 - indicates failure */
        if(gh_len == 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "lead process failed to open file")

        /* Decode GCPL length */
        UINT64DECODE(p, gcpl_len)

        /* Decode max OID */
        UINT64DECODE(p, file->max_oid)

        /* Check if we need to perform another bcast */
        if(gh_len + gcpl_len + 3 * sizeof(uint64_t) > sizeof(foi_buf_static)) {
            /* Check if we need to allocate a dynamic buffer */
            if(gh_len + gcpl_len > sizeof(foi_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (foi_buf_dyn = (char *)DV_malloc(gh_len + gcpl_len)))
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate space for global pool handle")
                foi_buf = foi_buf_dyn;
            } /* end if */

            /* Receive global handle */
            if(MPI_SUCCESS != MPI_Bcast(foi_buf_dyn, (int)(gh_len + gcpl_len), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't receive broadcasted global container handle (second broadcast)")

            p = (uint8_t *)foi_buf;
        } /* end if */

        /* Create local container handle */
        glob.iov_buf = (char *)p;
        glob.iov_buf_len = gh_len;
        glob.iov_len = gh_len;
        if(0 != (ret = daos_cont_global2local(H5_daos_poh_g, glob, &file->coh)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't get local container handle: %s", H5_daos_err_to_string(ret))

        /* Open global metadata object */
        if(0 != (ret = daos_obj_open(file->coh, gmd_oid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->glob_md_oh, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open global metadata object: %s", H5_daos_err_to_string(ret))

        /* Reconstitute root group from revieved GCPL */
        if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_reconstitute(file, root_grp_oid, p + gh_len, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't reconstitute root group")
    } /* end else */

    /* FCPL was stored as root group's GCPL (as GCPL is the parent of FCPL).
     * Point to it. */
    file->fcpl_id = file->root_grp->gcpl_id;
    if(H5Iinc_ref(file->fcpl_id) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTINC, NULL, "can't increment FCPL reference count")

    ret_value = (void *)file;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast bcast_buf_64 as '0' if necessary - this will trigger failures
         * in the other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(foi_buf_static, 0, sizeof(foi_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(foi_buf_static, sizeof(foi_buf_static), MPI_BYTE, 0, fa ? fa->comm : pool_comm_g))
                D_DONE_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast empty global container handle")
        } /* end if */

        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    if(fa)
        H5VLfree_connector_info(H5_DAOS_g, fa);

    /* Clean up buffers */
    foi_buf_dyn = (char *)DV_free(foi_buf_dyn);
    gcpl_buf = DV_free(gcpl_buf);

    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_flush
 *
 * Purpose:     Flushes a DAOS file.  Currently a no-op, may create a
 *              snapshot in the future.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_flush(H5_daos_file_t *file)
{
    int ret;
    herr_t       ret_value = SUCCEED;    /* Return value */

    /* Nothing to do if no write intent */
    if(!(file->flags & H5F_ACC_RDWR))
        D_GOTO_DONE(SUCCEED)

#if 0
    /* Collectively determine if anyone requested a snapshot of the epoch */
    if(MPI_SUCCESS != MPI_Reduce(file->my_rank == 0 ? MPI_IN_PLACE : &file->snap_epoch, &file->snap_epoch, 1, MPI_INT, MPI_LOR, 0, file->comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "failed to determine whether to take snapshot (MPI_Reduce)")

    /* Barrier on all ranks so we don't commit before all ranks are
     * finished writing. H5Fflush must be called collectively. */
    if(MPI_SUCCESS != MPI_Barrier(file->comm))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed")

    /* Commit the epoch */
    if(file->my_rank == 0) {
        /* Save a snapshot of this epoch if requested */
        /* Disabled until snapshots are supported in DAOS DSINC */

        if(file->snap_epoch)
            if(0 != (ret = daos_snap_create(file->coh, file->epoch, NULL /*event*/)))
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't create snapshot: %s", H5_daos_err_to_string(ret))

        /* Commit the epoch.  This should slip previous epochs automatically. */
        if(0 != (ret = daos_epoch_commit(file->coh, file->epoch, NULL /*state*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "failed to commit epoch: %s", H5_daos_err_to_string(ret))
    } /* end if */
#endif

done:
    D_FUNC_LEAVE
} /* end H5_daos_file_flush() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_specific
 *
 * Purpose:     Perform an operation
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_file_specific(void *item, H5VL_file_specific_t specific_type,
    hid_t DV_ATTR_UNUSED dxpl_id, void DV_ATTR_UNUSED **req,
    va_list DV_ATTR_UNUSED arguments)
{
    H5_daos_file_t *file = NULL;
    herr_t          ret_value = SUCCEED;    /* Return value */

    if (item)
        file = ((H5_daos_item_t *)item)->file;

    switch (specific_type) {
        /* H5Fflush */
        case H5VL_FILE_FLUSH:
            if(H5_daos_file_flush(file) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

            break;
        /* H5Fmount */
        case H5VL_FILE_MOUNT:
        /* H5Fmount */
        case H5VL_FILE_UNMOUNT:
        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE:
        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported specific operation")
    } /* end switch */

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_close_helper
 *
 * Purpose:     Closes a daos HDF5 file.
 *
 * Return:      Success:        the file id. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              January, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_file_close_helper(H5_daos_file_t *file, hid_t dxpl_id, void **req)
{
    int ret;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Free file data structures */
    if(file->file_name)
        free(file->file_name);
    if(file->comm || file->info)
        if(H5FDmpi_comm_info_free(&file->comm, &file->info) < 0)
            D_DONE_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to free copy of MPI communicator and info")
    if(file->fapl_id != FAIL && H5Idec_ref(file->fapl_id) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close fapl")
    if(file->fcpl_id != FAIL && H5Idec_ref(file->fcpl_id) < 0)
        D_DONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close fcpl")
    if(!daos_handle_is_inval(file->glob_md_oh))
        if(0 != (ret = daos_obj_close(file->glob_md_oh, NULL /*event*/)))
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close global metadata object: %s", H5_daos_err_to_string(ret))
    if(file->root_grp)
        if(H5_daos_group_close(file->root_grp, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close root group")
    if(!daos_handle_is_inval(file->coh))
        if(0 != (ret = daos_cont_close(file->coh, NULL /*event*/)))
            D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't close container: %s", H5_daos_err_to_string(ret))
    if(file->vol_id >= 0) {
        if(H5VLfree_connector_info(file->vol_id, file->vol_info) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't free VOL connector info")
        if(H5Idec_ref(file->vol_id) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "can't decrement VOL connector ID")
    } /* end if */
    file = H5FL_FREE(H5_daos_file_t, file);

    D_FUNC_LEAVE
} /* end H5_daos_file_close_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_close
 *
 * Purpose:     Closes a daos HDF5 file, committing the epoch if
 *              appropriate.
 *
 * Return:      Success:        the file ID.
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              October, 2016
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_file_close(void *_file, hid_t dxpl_id, void **req)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
#if 0 /* DSINC */
    int ret;
#endif
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Flush the file (barrier, commit epoch, slip epoch) *Update comment DSINC */
    if(H5_daos_file_flush(file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

#if 0 /* DSINC */
    /* Flush the epoch */
    if(0 != (ret = daos_epoch_flush(file->coh, epoch, NULL /*state*/, NULL /*event*/)))
        D_DONE_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "can't flush epoch: %s", H5_daos_err_to_string(ret))
#endif

    /* Close the file */
    if(H5_daos_file_close_helper(file, dxpl_id, req) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close file")

done:
    PRINT_ERROR_STACK

    D_FUNC_LEAVE
} /* end H5_daos_file_close() */

