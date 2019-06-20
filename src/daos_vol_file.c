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
 * library.  File routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */

/* Prototypes */
static herr_t H5_daos_file_flush(H5_daos_file_t *file);
static herr_t H5_daos_file_close_helper(H5_daos_file_t *file,
    hid_t dxpl_id, void **req);
static herr_t H5_daos_get_obj_count_callback(hid_t id, void *udata);
static herr_t H5_daos_get_obj_ids_callback(hid_t id, void *udata);

typedef struct get_obj_count_udata_t {
    uuid_t file_id;
    ssize_t obj_count;
} get_obj_count_udata_t;

typedef struct get_obj_ids_udata_t {
    uuid_t file_id;
    size_t max_objs;
    hid_t *oid_list;
    size_t obj_count;
} get_obj_ids_udata_t;


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
    hbool_t sched_init = FALSE;
    H5_daos_req_t *int_req = NULL;
    int mpi_initialized;
    int ret;
    void *ret_value = NULL;

    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL")

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
    file->item.open_req = NULL;
    file->closed = FALSE;
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;
    file->item.rc = 1;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->flags = flags;
    file->max_oid = 0;
    file->max_oid_dirty = FALSE;
    if((file->fcpl_id = H5Pcopy(fcpl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fcpl")
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Create CART context */
    if(0 != (ret = crt_context_create(&file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create CART context: %s", H5_daos_err_to_string(ret))

    /* Create DAOS task scheduler */
    if(0 != (ret = tse_sched_init(&file->sched, NULL, file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task scheduler: %s", H5_daos_err_to_string(ret))
    sched_init = TRUE;

    if (MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, NULL, "can't determine if MPI has been initialized")
    if (mpi_initialized) {
        /* Duplicate communicator and Info object. */
        /*
         * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
         */
        if(FAIL == H5_daos_comm_info_dup(fa ? fa->comm : H5_daos_pool_comm_g, fa ? fa->info : MPI_INFO_NULL, &file->comm, &file->info))
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "failed to duplicate MPI communicator and info")

        /* Obtain the process rank and size from the communicator attached to the
         * fapl ID */
        MPI_Comm_rank(fa ? fa->comm : H5_daos_pool_comm_g, &file->my_rank);
        MPI_Comm_size(fa ? fa->comm : H5_daos_pool_comm_g, &file->num_procs);
    }
    else {
        file->my_rank = 0;
        file->num_procs = 1;
    }

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Determine if we requested collective object ops for the file */
    if(H5Pget_all_coll_metadata_ops(fapl_id, &file->collective) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get collective access property")

    /* Generate oid for global metadata object */
    daos_obj_generate_id(&gmd_oid, DAOS_OF_DKEY_HASHED | DAOS_OF_AKEY_HASHED, DAOS_OC_TINY_RW);

    /* Start H5 operation */
    if(NULL == (int_req = (H5_daos_req_t *)DV_malloc(sizeof(H5_daos_req_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for request")
    int_req->th = DAOS_TX_NONE;
    int_req->th_open = FALSE;
    int_req->file = file;
    int_req->file->item.rc++;
    int_req->rc = 1;
    int_req->status = H5_DAOS_INCOMPLETE;
    int_req->failed_task = NULL;

    if(file->my_rank == 0) {
        /* If there are other processes and we fail we must bcast anyways so they
         * don't hang */
        if(file->num_procs > 1)
            must_bcast = TRUE;

        /* If the H5F_ACC_EXCL flag was specified, ensure that the container does not exist. */
        if(flags & H5F_ACC_EXCL)
            if(0 == daos_cont_open(H5_daos_poh_g, file->uuid, DAOS_COO_RW, &file->coh, NULL /*&file->co_info*/, NULL /*event*/))
                D_GOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, NULL, "container already existed and H5F_ACC_EXCL flag was used!")

        /* Delete the container if H5F_ACC_TRUNC is set.  This shouldn't cause a
         * problem even if the container doesn't exist. */
        if(flags & H5F_ACC_TRUNC)
            if(0 != (ret = daos_cont_destroy(H5_daos_poh_g, file->uuid, 1, NULL /*event*/)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't destroy container: %s", H5_daos_err_to_string(ret))

        /* Create the container for the file */
        if(0 != (ret = daos_cont_create(H5_daos_poh_g, file->uuid, NULL /* cont_prop */, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't create container: %s", H5_daos_err_to_string(ret))

        /* Open the container */
        if(0 != (ret = daos_cont_open(H5_daos_poh_g, file->uuid, DAOS_COO_RW, &file->coh, NULL /*&file->co_info*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open container: %s", H5_daos_err_to_string(ret))

        /* Start transaction */
        if(0 != (ret = daos_tx_open(file->coh, &int_req->th, NULL /*event*/)))
            D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't start transaction")
        int_req->th_open = TRUE;

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
            if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)sizeof(gh_buf_static), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
                D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast global container handle")

            /* Need a second bcast if we had to allocate a dynamic buffer */
            if(gh_buf == gh_buf_dyn)
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)gh_buf_size, MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
                    D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast global container handle (second broadcast)")
        } /* end if */
    } /* end if */
    else {
        /* Receive global handle */
        if(MPI_SUCCESS != MPI_Bcast(gh_buf, (int)sizeof(gh_buf_static), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
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
            if(MPI_SUCCESS != MPI_Bcast(gh_buf_dyn, (int)gh_buf_size, MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
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
    if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_create_helper(file, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, int_req, NULL, NULL, 0, TRUE)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group")
    assert(file->root_grp->obj.oid.lo == (uint64_t)1);

    ret_value = (void *)file;

done:
    if(int_req) {
        /* Block until operation completes */
        {
            bool is_empty;

            /* Wait for scheduler to be empty *//* Change to custom progress function DSINC */
            if(sched_init && (ret = daos_progress(&file->sched, DAOS_EQ_WAIT, &is_empty)) < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't progress scheduler: %s", H5_daos_err_to_string(ret))

            /* Check for failure */
            if(int_req->status < 0)
                D_DONE_ERROR(H5E_SYM, H5E_CANTOPERATE, NULL, "file creation failed in task \"%s\": %s", int_req->failed_task, H5_daos_err_to_string(int_req->status))
        } /* end block */
    
        /* Close internal request */
        H5_daos_req_free_int(int_req);
    } /* end if */

    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast bcast_buf_64 as '0' if necessary - this will trigger failures
         * in the other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            memset(gh_buf_static, 0, sizeof(gh_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(gh_buf_static, sizeof(gh_buf_static), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
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

    D_FUNC_LEAVE_API
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
    int mpi_initialized;
    int ret;
    void *ret_value = NULL;

    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL")

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

    /* Allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct")
    file->item.open_req = NULL;
    file->closed = FALSE;
    file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;
    file->vol_id = FAIL;
    file->item.rc = 1;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->flags = flags;
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Create CART context */
    if(0 != (ret = crt_context_create(&file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create CART context: %s", H5_daos_err_to_string(ret))

    /* Create DAOS task scheduler */
    if(0 != (ret = tse_sched_init(&file->sched, NULL, file->crt_ctx)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create task scheduler: %s", H5_daos_err_to_string(ret))

    if (MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, NULL, "can't determine if MPI has been initialized")
    if (mpi_initialized) {
        /* Duplicate communicator and Info object. */
        /*
         * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
         */
        if(FAIL == H5_daos_comm_info_dup(fa ? fa->comm : H5_daos_pool_comm_g, fa ? fa->info : MPI_INFO_NULL, &file->comm, &file->info))
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "failed to duplicate MPI communicator and info")

        /* Obtain the process rank and size from the communicator attached to the
         * fapl ID */
        MPI_Comm_rank(fa ? fa->comm : H5_daos_pool_comm_g, &file->my_rank);
        MPI_Comm_size(fa ? fa->comm : H5_daos_pool_comm_g, &file->num_procs);
    }
    else {
        file->my_rank = 0;
        file->num_procs = 1;
    }

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
        daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, H5_daos_max_oid_key_g, H5_daos_max_oid_key_size_g);
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
        if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_open_helper(file, root_grp_oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, NULL, (file->num_procs > 1) ? &gcpl_buf : NULL, &gcpl_len)))
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
            if(MPI_SUCCESS != MPI_Bcast(foi_buf, (int)sizeof(foi_buf_static), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
                D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast global container handle")

            /* Need a second bcast if we had to allocate a dynamic buffer */
            if(foi_buf == foi_buf_dyn)
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)(gh_len + gcpl_len), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
                    D_GOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't broadcast file open info (second broadcast)")
        } /* end if */
    } /* end if */
    else {
        /* Receive file open info */
        if(MPI_SUCCESS != MPI_Bcast(foi_buf, (int)sizeof(foi_buf_static), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
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
            if(MPI_SUCCESS != MPI_Bcast(foi_buf_dyn, (int)(gh_len + gcpl_len), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
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
        if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_reconstitute(file, root_grp_oid, p + gh_len, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, NULL)))
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
            if(MPI_SUCCESS != MPI_Bcast(foi_buf_static, sizeof(foi_buf_static), MPI_BYTE, 0, fa ? fa->comm : H5_daos_pool_comm_g))
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

    D_FUNC_LEAVE_API
} /* end H5_daos_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_get
 *
 * Purpose:     Performs a file "get" operation
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
H5_daos_file_get(void *_item, H5VL_file_get_t get_type, hid_t H5VL_DAOS_UNUSED dxpl_id,
    void H5VL_DAOS_UNUSED **req, va_list H5VL_DAOS_UNUSED arguments)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_item;
    herr_t          ret_value = SUCCEED;

    if(!_item)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL")
    if(get_type == H5VL_FILE_GET_NAME) {
        if(H5I_FILE != file->item.type && H5I_GROUP != file->item.type &&
           H5I_DATATYPE != file->item.type && H5I_DATASET != file->item.type &&
           H5I_ATTR != file->item.type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file, group, datatype, dataset or attribute")
    }
    else
        if(H5I_FILE != file->item.type)
            D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file")

    switch (get_type) {
        /* H5Fget_access_plist */
        case H5VL_FILE_GET_FAPL:
        {
            hid_t *ret_id = va_arg(arguments, hid_t *);

            if((*ret_id = H5Pcopy(file->fapl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FAPL")

            break;
        } /* H5VL_FILE_GET_FAPL */

        /* H5Fget_create_plist */
        case H5VL_FILE_GET_FCPL:
        {
            hid_t *ret_id = va_arg(arguments, hid_t *);

            if((*ret_id = H5Pcopy(file->fcpl_id)) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't get file's FCPL")

            break;
        } /* H5VL_FILE_GET_FCPL */

        /* H5Fget_intent */
        case H5VL_FILE_GET_INTENT:
        {
            unsigned *ret_intent = va_arg(arguments, unsigned *);

            if((file->flags & H5F_ACC_RDWR) == H5F_ACC_RDWR)
                *ret_intent = H5F_ACC_RDWR;
            else
                *ret_intent = H5F_ACC_RDONLY;

            break;
        } /* H5VL_FILE_GET_INTENT */

        /* H5Fget_name */
        case H5VL_FILE_GET_NAME:
        {
            H5I_type_t  obj_type = va_arg(arguments, H5I_type_t);
            size_t      name_buf_size = va_arg(arguments, size_t);
            char       *name_buf = va_arg(arguments, char *);
            ssize_t    *ret_size = va_arg(arguments, ssize_t *);

            if(H5I_FILE != obj_type)
                file = file->item.file;

            *ret_size = (ssize_t) strlen(file->file_name);

            if(name_buf) {
                strncpy(name_buf, file->file_name, name_buf_size - 1);
                name_buf[name_buf_size - 1] = '\0';
            } /* end if */

            break;
        } /* H5VL_FILE_GET_NAME */

        /* H5Fget_obj_count */
        case H5VL_FILE_GET_OBJ_COUNT:
        {
            unsigned obj_types = va_arg(arguments, unsigned);
            ssize_t *ret_val = va_arg(arguments, ssize_t *);
            get_obj_count_udata_t udata;

            udata.obj_count = 0;

            uuid_copy(udata.file_id, file->uuid);

            if(obj_types & H5F_OBJ_FILE)
                if(H5Iiterate(H5I_FILE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs")
            if(obj_types & H5F_OBJ_DATASET)
                if(H5Iiterate(H5I_DATASET, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open dataset IDs")
            if(obj_types & H5F_OBJ_GROUP)
                if(H5Iiterate(H5I_GROUP, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs")
            if(obj_types & H5F_OBJ_DATATYPE)
                if(H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open datatype IDs")
            if(obj_types & H5F_OBJ_ATTR)
                if(H5Iiterate(H5I_ATTR, H5_daos_get_obj_count_callback, &udata) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open attribute IDs")

            *ret_val = udata.obj_count;

            break;
        } /* H5VL_FILE_GET_OBJ_COUNT */

        /* H5Fget_obj_ids */
        case H5VL_FILE_GET_OBJ_IDS:
        {
            unsigned obj_types = va_arg(arguments, unsigned);
            size_t max_ids = va_arg(arguments, size_t);
            hid_t *oid_list = va_arg(arguments, hid_t *);
            ssize_t *ret_val = va_arg(arguments, ssize_t *);
            get_obj_ids_udata_t udata;

            udata.max_objs = max_ids;
            udata.obj_count = 0;
            udata.oid_list = oid_list;

            if(max_ids > 0) {
                uuid_copy(udata.file_id, file->uuid);

                if(obj_types & H5F_OBJ_FILE)
                    if(H5Iiterate(H5I_FILE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open file IDs")
                if(obj_types & H5F_OBJ_DATASET)
                    if(H5Iiterate(H5I_DATASET, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open dataset IDs")
                if(obj_types & H5F_OBJ_GROUP)
                    if(H5Iiterate(H5I_GROUP, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open group IDs")
                if(obj_types & H5F_OBJ_DATATYPE)
                    if(H5Iiterate(H5I_DATATYPE, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open datatype IDs")
                if(obj_types & H5F_OBJ_ATTR)
                    if(H5Iiterate(H5I_ATTR, H5_daos_get_obj_ids_callback, &udata) < 0)
                        D_GOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "failed to iterate over file's open attribute IDs")
            }

            *ret_val = udata.obj_count;

            break;
        } /* H5VL_FILE_GET_OBJ_IDS */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file get operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_file_get() */


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
#if 0
    int ret;
#endif
    herr_t ret_value = SUCCEED;    /* Return value */

    assert(file);

    /* Nothing to do if no write intent */
    if(!(file->flags & H5F_ACC_RDWR))
        D_GOTO_DONE(SUCCEED)

    /* Progress scheduler until empty? DSINC */

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
    hid_t dxpl_id, void **req, va_list H5VL_DAOS_UNUSED arguments)
{
    H5_daos_file_t *file = NULL;
    herr_t          ret_value = SUCCEED;    /* Return value */

    /*
     * TODO: H5Fis_accessible is the only thing that can result in a NULL
     * item pointer.
     */

    if(item)
        file = ((H5_daos_item_t *)item)->file;

    switch (specific_type) {
        /* H5Fflush */
        case H5VL_FILE_FLUSH:
        {
            if(H5_daos_file_flush(file) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

            break;
        } /* H5VL_FILE_FLUSH */

        /* H5Freopen */
        case H5VL_FILE_REOPEN:
        {
            void **ret_file = va_arg(arguments, void **);

            if(NULL == (*ret_file = H5_daos_file_open(file->file_name, file->flags, file->fapl_id, dxpl_id, req)))
                D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "can't reopen file")

            break;
        } /* H5VL_FILE_REOPEN */

        /* H5Fmount */
        case H5VL_FILE_MOUNT:
        /* H5Fmount */
        case H5VL_FILE_UNMOUNT:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation")

        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE:
        {
            hid_t file_fapl = va_arg(arguments, hid_t);
            const char *filename = va_arg(arguments, const char *);
            htri_t *ret_is_accessible = va_arg(arguments, htri_t *);
            void *opened_file = NULL;

            if(NULL == filename)
                D_GOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "filename is NULL")

            H5E_BEGIN_TRY {
                opened_file = H5_daos_file_open(filename, H5F_ACC_RDONLY, file_fapl, dxpl_id, req);
            } H5E_END_TRY;

            *ret_is_accessible = opened_file ? TRUE : FALSE;

            if(opened_file)
                if(H5_daos_file_close(opened_file, dxpl_id, req) < 0)
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "error occurred while closing file")

            break;
        } /* H5VL_FILE_IS_ACCESSIBLE */

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported file specific operation")
    } /* end switch */

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_file_decref
 *
 * Purpose:     Decrements the reference count on an HDF5/DAOS file,
 *              freeing it if the ref count reaches 0.
 *
 * Return:      Success:        the file id. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_file_decref(H5_daos_file_t *file)
{
    assert(file);

    if(--file->item.rc == 0) {
        /* Free file data structure */
        assert(file->closed);
        H5FL_FREE(H5_daos_file_t, file);
    } /* end if */

    return;
} /* end H5_daos_file_decref() */


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
    if(file->item.open_req)
        H5_daos_req_free_int(file->item.open_req);
    file->item.open_req = NULL;
    if(file->file_name)
        file->file_name = DV_free(file->file_name);
    if(file->comm || file->info)
        if(H5_daos_comm_info_free(&file->comm, &file->info) < 0)
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

    /* Finish the scheduler *//* Make this cancel tasks?  Only if flush progresses until empty.  Otherwise change to custom progress function DSINC */
    tse_sched_complete(&file->sched, 0, FALSE);
    tse_sched_fini(&file->sched);

    /* Destroy CART context */
    if(0 != (ret = crt_context_destroy(file->crt_ctx, 1)))
        D_DONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "can't destroy CART context: %s", H5_daos_err_to_string(ret))

    /* File is closed */
    file->closed = TRUE;

    /* Decrement ref count on file struct */
    H5_daos_file_decref(file);

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

    if(!_file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL")

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
    D_FUNC_LEAVE_API
} /* end H5_daos_file_close() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_obj_count_callback
 *
 * Purpose:     A callback for H5Iiterate which increments the passed in
 *              object count only if the current object's file ID matches
 *              the file ID passed in.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_obj_count_callback(hid_t id, void *udata)
{
    get_obj_count_udata_t *count_udata = (get_obj_count_udata_t *)udata;
    H5_daos_obj_t *cur_obj = NULL;
    ssize_t connector_name_len;
    char connector_name[H5_DAOS_VOL_NAME_LEN + 1];
    herr_t ret_value = H5_ITER_CONT;

    /* Ensure that the ID represents a DAOS VOL object */
    H5E_BEGIN_TRY {
        connector_name_len = H5VLget_connector_name(id, connector_name, H5_DAOS_VOL_NAME_LEN + 1);
    } H5E_END_TRY;

    /* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
    if(connector_name_len < 0)
        D_GOTO_DONE(H5_ITER_CONT);

    if(!strncmp(H5_DAOS_VOL_NAME, connector_name, H5_DAOS_VOL_NAME_LEN)) {
        if(NULL == (cur_obj = (H5_daos_obj_t *) H5VLobject(id)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID")

        if(!uuid_compare(cur_obj->item.file->uuid, count_udata->file_id))
            count_udata->obj_count++;
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_get_obj_count_callback() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_get_obj_ids_callback
 *
 * Purpose:     A callback for H5Iiterate which retrieves all of the open
 *              object IDs of the specified types for the given file ID.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 * Programmer:  Jordan Henderson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_get_obj_ids_callback(hid_t id, void *udata)
{
    get_obj_ids_udata_t *id_udata = (get_obj_ids_udata_t *)udata;
    H5_daos_obj_t *cur_obj = NULL;
    ssize_t connector_name_len;
    char connector_name[H5_DAOS_VOL_NAME_LEN + 1];
    herr_t ret_value = H5_ITER_CONT;

    /* Ensure that the ID represents a DAOS VOL object */
    H5E_BEGIN_TRY {
        connector_name_len = H5VLget_connector_name(id, connector_name, H5_DAOS_VOL_NAME_LEN + 1);
    } H5E_END_TRY;

    /* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
    if(connector_name_len < 0)
        D_GOTO_DONE(H5_ITER_CONT);

    if(!strncmp(H5_DAOS_VOL_NAME, connector_name, H5_DAOS_VOL_NAME_LEN)) {
        if(NULL == (cur_obj = (H5_daos_obj_t *) H5VLobject(id)))
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, H5_ITER_ERROR, "can't retrieve VOL object for ID")

        if(!uuid_compare(cur_obj->item.file->uuid, id_udata->file_id))
            id_udata->oid_list[id_udata->obj_count++] = id;

        if(id_udata->obj_count >= id_udata->max_objs)
            ret_value = H5_ITER_STOP;
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_get_obj_ids_callback() */

