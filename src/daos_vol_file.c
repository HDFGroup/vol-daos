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
static H5_daos_req_t *H5_daos_req_create(H5_daos_file_t *file);

static herr_t H5_daos_cont_get_fapl_info(hid_t fapl_id, H5_daos_fapl_t *fa_out);
static herr_t H5_daos_cont_set_mpi_info(H5_daos_file_t *file, H5_daos_fapl_t *fa);
static herr_t H5_daos_cont_create(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *int_req);
static herr_t H5_daos_cont_open(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *int_req);
static herr_t H5_daos_cont_handle_bcast(H5_daos_file_t *file);
static herr_t H5_daos_cont_gcpl_bcast(H5_daos_file_t *file, void **gcpl_buf, uint64_t *gcpl_len);

static herr_t H5_daos_fill_fapl_cache(H5_daos_file_t *file, hid_t fapl_id);
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
 * Function:    H5_daos_req_create
 *
 * Purpose:     Create a request.
 *
 * Return:      Valid pointer on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
/* TODO this should be moved to request module, keep here for now */
static H5_daos_req_t *
H5_daos_req_create(H5_daos_file_t *file)
{
    H5_daos_req_t *ret_value = NULL;

    assert(file);

    if(NULL == (ret_value = (H5_daos_req_t *)DV_malloc(sizeof(H5_daos_req_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for request")
    ret_value->th = DAOS_TX_NONE;
    ret_value->th_open = FALSE;
    ret_value->file = file;
    ret_value->file->item.rc++;
    ret_value->rc = 1;
    ret_value->status = H5_DAOS_INCOMPLETE;
    ret_value->failed_task = NULL;

done:
    D_FUNC_LEAVE
} /* end H5_daos_req_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_get_fapl_info
 *
 * Purpose:     Retrieve needed information from the given FAPL ID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_get_fapl_info(hid_t fapl_id, H5_daos_fapl_t *fa_out)
{
    H5_daos_fapl_t *local_fapl_info = NULL;
    herr_t ret_value = SUCCEED;

    /*
     * First, check to see if any MPI info was set through the use of
     * a H5Pset_fapl_daos() call.
     */
    if(H5Pget_vol_info(fapl_id, (void **) &local_fapl_info) < 0)
        D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL info struct")
    if(local_fapl_info) {
        fa_out->comm = local_fapl_info->comm;
        fa_out->info = local_fapl_info->info;
    }
    else {
        hid_t driver_id;

        /*
         * If no info was set using H5Pset_fapl_daos(), see if the application
         * set any MPI info by using HDF5's H5Pset_fapl_mpio().
         */
        if((driver_id = H5Pget_driver(fapl_id)) < 0)
            D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't determine if a MPI-based HDF5 VFD was requested for file access")
        if(H5FD_MPIO == driver_id) {
            if(H5Pget_fapl_mpio(fapl_id, &fa_out->comm, &fa_out->info) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get HDF5 MPI information")
        }
        else {
            /*
             * If no MPI info was set (as in the case of passing a default FAPL),
             * simply use MPI_COMM_SELF as the communicator.
             */
            fa_out->comm = MPI_COMM_SELF;
            fa_out->info = MPI_INFO_NULL;
        }
    }

done:
    if(local_fapl_info)
        H5VLfree_connector_info(H5_DAOS_g, local_fapl_info);

    D_FUNC_LEAVE
} /* end H5_daos_cont_get_fapl_info() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_set_mpi_info
 *
 * Purpose:     Set MPI info for file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_set_mpi_info(H5_daos_file_t *file, H5_daos_fapl_t *fa)
{
    int mpi_initialized;
    herr_t ret_value = SUCCEED;

    assert(file);
    assert(fa);

    if(MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized")
    if(mpi_initialized) {
        /* Duplicate communicator and Info object. */
        /*
         * XXX: DSINC - Need to pass in MPI Info to VOL connector as well.
         */
        if(FAIL == H5_daos_comm_info_dup(fa->comm, fa->info, &file->comm, &file->info))
            D_GOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "failed to duplicate MPI communicator and info")

        /* Obtain the process rank and size from the communicator attached to the
         * fapl ID */
        MPI_Comm_rank(file->comm, &file->my_rank);
        MPI_Comm_size(file->comm, &file->num_procs);
    } else {
        file->my_rank = 0;
        file->num_procs = 1;
    }

done:
    D_FUNC_LEAVE
} /* end H5_daos_cont_set_mpi_info() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_create
 *
 * Purpose:     Create a DAOS container.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_create(H5_daos_file_t *file, unsigned flags, H5_daos_req_t *int_req)
{
    herr_t ret_value = SUCCEED;
    int ret;

    assert(file);
    assert(int_req);

    /* If the H5F_ACC_EXCL flag was specified, ensure that the container does not exist. */
    if(flags & H5F_ACC_EXCL)
        if(0 == daos_cont_open(H5_daos_poh_g, file->uuid, DAOS_COO_RW, &file->coh, NULL /*&file->co_info*/, NULL /*event*/))
            D_GOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, FAIL, "container already existed and H5F_ACC_EXCL flag was used!")

    /* Delete the container if H5F_ACC_TRUNC is set.  This shouldn't cause a
     * problem even if the container doesn't exist. */
    if(flags & H5F_ACC_TRUNC)
        if(0 != (ret = daos_cont_destroy(H5_daos_poh_g, file->uuid, 1, NULL /*event*/)))
            D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "can't destroy container: %s", H5_daos_err_to_string(ret))

    /* Create the container for the file */
    if(0 != (ret = daos_cont_create(H5_daos_poh_g, file->uuid, NULL /* cont_prop */, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCREATE, FAIL, "can't create container: %s", H5_daos_err_to_string(ret))

    /* Open the container */
    if(0 != (ret = daos_cont_open(H5_daos_poh_g, file->uuid, DAOS_COO_RW, &file->coh, NULL /*&file->co_info*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, FAIL, "can't open container: %s", H5_daos_err_to_string(ret))

    /* Start transaction */
    if(0 != (ret = daos_tx_open(file->coh, &int_req->th, NULL /*event*/)))
        D_GOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't start transaction")
    int_req->th_open = TRUE;

done:
    D_FUNC_LEAVE
} /* end H5_daos_cont_create() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_open
 *
 * Purpose:     Open a DAOS container.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_open(H5_daos_file_t *file, unsigned flags, H5_daos_req_t H5VL_DAOS_UNUSED *int_req)
{
    herr_t ret_value = SUCCEED;
    int ret;

    assert(file);

    /* Open the container */
    if(0 != (ret = daos_cont_open(H5_daos_poh_g, file->uuid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->coh, NULL /*&file->co_info*/, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, FAIL, "can't open container: %s", H5_daos_err_to_string(ret))

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

done:
    D_FUNC_LEAVE
} /* end H5_daos_cont_open() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_handle_bcast
 *
 * Purpose:     Broadcast the container handle.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_handle_bcast(H5_daos_file_t *file)
{
    daos_iov_t glob = {.iov_buf = NULL, .iov_buf_len = 0, .iov_len = 0};
    herr_t ret_value = SUCCEED; /* Return value */
    hbool_t err_occurred = FALSE;
    int ret;

    assert(file);

    /* Calculate size of global cont handle */
    if((file->my_rank == 0) && (0 != (ret = daos_cont_local2global(file->coh, &glob))))
        err_occurred = TRUE;

    /* Bcast size */
    if(MPI_SUCCESS != MPI_Bcast(&glob.iov_buf_len, 1, MPI_UINT64_T, 0, file->comm))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast global container handle size")

    /* Error checking */
    if(err_occurred) {
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get global container handle size: %s", H5_daos_err_to_string(ret))
    } else if(0 == glob.iov_buf_len) {
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "invalid global handle size after bcast")
    }

    /* Allocate buffer */
    if(NULL == (glob.iov_buf = (char *)DV_malloc(glob.iov_buf_len)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for global container handle")
    memset(glob.iov_buf, 0, glob.iov_buf_len);
    glob.iov_len = glob.iov_buf_len;

    /* Get global container handle */
    if((file->my_rank == 0) && (0 != (ret = daos_cont_local2global(file->coh, &glob))))
        err_occurred = TRUE;

    /* Bcast handle */
    if(MPI_SUCCESS != MPI_Bcast(glob.iov_buf, (int)glob.iov_buf_len, MPI_BYTE, 0, file->comm))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast global container handle");

    /* Error checking */
    if(err_occurred) {
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get global container handle: %s", H5_daos_err_to_string(ret))
    } else {
        size_t i;
        hbool_t non_zeros = FALSE;
        for(i = 0; i < glob.iov_buf_len; i++)
            if(0 != ((char *)(glob.iov_buf))[i]) {
                non_zeros = TRUE;
                break; /* Break if not 0 */
            }
        if(!non_zeros)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "invalid global handle size after bcast")
    }

    /* Get container handle */
    if((file->my_rank != 0) && (0 != (ret = daos_cont_global2local(H5_daos_poh_g, glob, &file->coh))))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get global container handle: %s", H5_daos_err_to_string(ret))

done:
    DV_free(glob.iov_buf);

    D_FUNC_LEAVE
} /* end H5_daos_cont_handle_bcast() */

/*-------------------------------------------------------------------------
 * Function:    H5_daos_cont_gcpl_bcast
 *
 * Purpose:     Broadcast the container GCPL + max OID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_cont_gcpl_bcast(H5_daos_file_t *file, void **gcpl_buf, uint64_t *gcpl_len)
{
    herr_t ret_value = SUCCEED; /* Return value */
    char *buf = NULL;
    size_t buf_size;

    assert(file);
    assert(gcpl_buf);
    assert(gcpl_len);

    /* Bcast size */
    buf_size = sizeof(uint64_t) + *gcpl_len;
    if(MPI_SUCCESS != MPI_Bcast(&buf_size, 1, MPI_UINT64_T, 0, file->comm))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast gcpl size")

    /* Allocate buffer */
    if(NULL == (buf = (char *)DV_malloc(buf_size)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for bcast buffer")

    if(file->my_rank == 0) {
        uint8_t *p = (uint8_t *)buf;

        /* Encode GCPL length */
        UINT64ENCODE(p, *gcpl_len)

        /* Copy GCPL buffer */
        memcpy(p, *gcpl_buf, *gcpl_len);
    }

    /* Bcast buffer */
    if(MPI_SUCCESS != MPI_Bcast(buf, (int)buf_size, MPI_BYTE, 0, file->comm))
        D_GOTO_ERROR(H5E_VOL, H5E_MPI, FAIL, "can't broadcast gcpl info buf");

    if(file->my_rank != 0) {
        uint8_t *p = (uint8_t *)buf;

        /* Decode GCPL length */
        UINT64DECODE(p, *gcpl_len)

        /* Copy GCPL buffer */
        if(NULL == (*gcpl_buf = DV_malloc(*gcpl_len)))
            D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for gcpl buffer")
        memcpy(*gcpl_buf, p, *gcpl_len);
    }

done:
    DV_free(buf);

    D_FUNC_LEAVE
} /* end H5_daos_cont_gcpl_bcast() */


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
    H5_daos_file_t *file = NULL;
    H5_daos_fapl_t fapl_info;
    daos_obj_id_t gmd_oid = {0, 0};
    hbool_t sched_init = FALSE;
    H5_daos_req_t *int_req = NULL;
#if 0 /* Needed for storing the root group OID in the global metadata object -
       * see note below */
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    uint8_t root_group_oid_buf[H5_DAOS_ENCODED_OID_SIZE];
    uint8_t *p;
#endif
    int ret;
    void *ret_value = NULL;

    H5daos_compile_assert(H5_DAOS_ENCODED_OID_SIZE
            == H5_DAOS_ENCODED_UINT64_T_SIZE + H5_DAOS_ENCODED_UINT64_T_SIZE);

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
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct")

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5_daos_file_t)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate DAOS file struct")
    file->item.open_req = NULL;
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

    /* Set MPI container info */
    if(H5_daos_cont_set_mpi_info(file, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info")

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Fill FAPL cache */
    if(H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache")

    /* Generate oid for global metadata object */
    if(H5_daos_oid_encode(&gmd_oid, H5_DAOS_OIDX_GMD, H5I_GROUP,
            fcpl_id == H5P_FILE_CREATE_DEFAULT ? H5P_DEFAULT : fcpl_id,
            H5_DAOS_OBJ_CLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID")

    /* Start H5 operation */
    if(NULL == (int_req = H5_daos_req_create(file)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't create DAOS request")

    /* Create container on rank 0 */
    if((file->my_rank == 0) && H5_daos_cont_create(file, flags, int_req) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create DAOS container")

    /* Broadcast container handle to other procs if any */
    if((file->num_procs > 1) && (H5_daos_cont_handle_bcast(file) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, NULL, "can't broadcast DAOS container handle")

    /* Open global metadata object */
    if(0 != (ret = daos_obj_open(file->coh, gmd_oid, DAOS_OO_RW, &file->glob_md_oh, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open global metadata object: %s", H5_daos_err_to_string(ret))

    /* Create root group */
    if(NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_create_helper(file, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, int_req, NULL, NULL, 0, H5_DAOS_OIDX_ROOT, TRUE)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group")

    /* Write root group OID to global metadata object */
    /* Only do this if a non-default object class is used DSINC */
    /* Disabled for now since we don't use it.  Right now, the user must specify
     * the root group's object class through
     * H5daos_set_root_open_object_class(), but eventually we will allow the
     * user to skip this by storing the root group OID in the global metadata
     * object (as below), and, if the intial root group open fails, reading the
     * stored root group OID and using that to open the root group.  DSINC */
#if 0
    /* Encode root group OID */
    p = root_group_oid_buf;
    UINT64ENCODE(p, file->root_grp->obj.oid.lo)
    UINT64ENCODE(p, file->root_grp->obj.oid.hi)

    /* Set up dkey */
    daos_iov_set(&dkey, H5_daos_int_md_key_g, H5_daos_int_md_key_size_g);

    /* Set up iod */
    memset(&iod, 0, sizeof(iod));
    daos_iov_set(&iod.iod_name, H5_daos_root_grp_oid_key_g, H5_daos_root_grp_oid_key_size_g);
    iod.iod_nr = 1u;
    iod.iod_size = (uint64_t)H5_DAOS_ENCODED_OID_SIZE;
    iod.iod_type = DAOS_IOD_SINGLE;

    /* Set up sgl */
    daos_iov_set(&sg_iov, root_group_oid_buf, (daos_size_t)H5_DAOS_ENCODED_OID_SIZE);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;

    /* Write root group OID */
    if(0 != (ret = daos_obj_update(file->glob_md_oh, DAOS_TX_NONE, &dkey, 1, &iod, &sgl, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_WRITEERROR, NULL, "can't write root group OID to global metadata object: %s", H5_daos_err_to_string(ret))
#endif

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
        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

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
    H5_daos_file_t *file = NULL;
    H5_daos_fapl_t fapl_info;
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    void *gcpl_buf = NULL;
    uint64_t gcpl_len = 0;
    daos_obj_id_t gmd_oid = {0, 0};
    daos_obj_id_t root_grp_oid = {0, 0};
    int ret;
    void *ret_value = NULL;

    if(!name)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "file name is NULL")

    /* Get information from the FAPL */
    if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get DAOS info struct")

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

    /* Set MPI container info */
    if(H5_daos_cont_set_mpi_info(file, &fapl_info) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set MPI container info")

    /* Hash file name to create uuid */
    H5_daos_hash128(name, &file->uuid);

    /* Fill FAPL cache */
    if(H5_daos_fill_fapl_cache(file, fapl_id) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "failed to fill FAPL cache")

    /* Generate oid for global metadata object */
    if(H5_daos_oid_encode(&gmd_oid, H5_DAOS_OIDX_GMD, H5I_GROUP,
            fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
            H5_DAOS_ROOT_OPEN_OCLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode global metadata object ID")

    /* Generate root group oid */
    if(H5_daos_oid_encode(&root_grp_oid, H5_DAOS_OIDX_ROOT, H5I_GROUP,
            fapl_id == H5P_FILE_ACCESS_DEFAULT ? H5P_DEFAULT : fapl_id,
            H5_DAOS_ROOT_OPEN_OCLASS_NAME, file) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTENCODE, NULL, "can't encode root group object ID")

    /* Open container on rank 0 */
    if((file->my_rank == 0) && H5_daos_cont_open(file, flags, NULL) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't open DAOS container")

    /* Broadcast container handle to other procs if any */
    if((file->num_procs > 1) && (H5_daos_cont_handle_bcast(file) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, NULL, "can't broadcast DAOS container handle")

    /* Open global metadata object */
    if(0 != (ret = daos_obj_open(file->coh, gmd_oid, flags & H5F_ACC_RDWR ? DAOS_COO_RW : DAOS_COO_RO, &file->glob_md_oh, NULL /*event*/)))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open global metadata object: %s", H5_daos_err_to_string(ret))

    /* Open root group */
    if((file->my_rank == 0) && (NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_open_helper(file, root_grp_oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, NULL, (file->num_procs > 1) ? &gcpl_buf : NULL, &gcpl_len))))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, NULL, "can't open root group")

    /* Broadcast GCPL info to other procs if any */
    if((file->num_procs > 1) && (H5_daos_cont_gcpl_bcast(file, &gcpl_buf, &gcpl_len) < 0))
        D_GOTO_ERROR(H5E_VOL, H5E_CANTSET, NULL, "can't broadcast DAOS container handle")

    /* Reconstitute root group from revived GCPL */
    if((file->my_rank != 0) && (NULL == (file->root_grp = (H5_daos_group_t *)H5_daos_group_reconstitute(file, root_grp_oid, gcpl_buf, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, NULL))))
        D_GOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't reconstitute root group")

    /* FCPL was stored as root group's GCPL (as GCPL is the parent of FCPL).
     * Point to it. */
    file->fcpl_id = file->root_grp->gcpl_id;
    if(H5Iinc_ref(file->fcpl_id) < 0)
        D_GOTO_ERROR(H5E_ATOM, H5E_CANTINC, NULL, "can't increment FCPL reference count")

    ret_value = (void *)file;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Close file */
        if(file && H5_daos_file_close_helper(file, dxpl_id, req) < 0)
            D_DONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    /* Clean up buffers */
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
        /* "get container info" */
        case H5VL_FILE_GET_CONT_INFO:
        {
            H5VL_file_cont_info_t *info = va_arg(arguments, H5VL_file_cont_info_t *);

            /* Verify structure version */
            if(info->version != H5VL_CONTAINER_INFO_VERSION)
                D_GOTO_ERROR(H5E_FILE, H5E_VERSION, FAIL, "wrong container info version number")

            /* Set the container info fields */
            info->feature_flags = 0;            /* None currently defined */
            info->token_size = H5_DAOS_ENCODED_OID_SIZE;
            info->blob_id_size = H5_DAOS_BLOB_ID_SIZE;

            break;
        } /* H5VL_FILE_GET_CONT_INFO */

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

            /* Set root group's object class on fcpl */
            if(H5_daos_set_oclass_from_oid(*ret_id, file->root_grp->obj.oid) < 0)
                D_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object class property")

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
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5_daos_file_t *file = NULL;
    herr_t          ret_value = SUCCEED;    /* Return value */

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

        /* H5Fdelete */
        case H5VL_FILE_DELETE:
        {
            H5_daos_fapl_t fapl_info;
            hid_t fapl_id = va_arg(arguments, hid_t);
            const char *filename = va_arg(arguments, const char *);
            herr_t *delete_ret = va_arg(arguments, herr_t *);
            uuid_t cont_uuid;
            int mpi_rank, mpi_initialized;
            int ret;

            /* Initialize returned value in case we fail */
            *delete_ret = FAIL;

            /* Get information from the FAPL */
            if(H5_daos_cont_get_fapl_info(fapl_id, &fapl_info) < 0)
                D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get DAOS info struct")

            if(MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't determine if MPI has been initialized")
            if(mpi_initialized) {
                MPI_Comm_rank(fapl_info.comm, &mpi_rank);
            } else {
                mpi_rank = 0;
            }

            if(mpi_rank == 0) {
                /* Hash file name to create uuid */
                H5_daos_hash128(filename, &cont_uuid);

                if(0 != (ret = daos_cont_destroy(H5_daos_poh_g, cont_uuid, 1, NULL /*event*/)))
                    D_GOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "can't destroy container: %s", H5_daos_err_to_string(ret))
            } /* end if */

            if(mpi_initialized)
                if(MPI_SUCCESS != MPI_Barrier(fapl_info.comm))
                    D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed during file deletion")

            *delete_ret = SUCCEED;

            break;
        } /* H5VL_FILE_DELETE */

        /* Check if two files are the same */
        case H5VL_FILE_IS_EQUAL:
        {
            H5_daos_file_t *file2 = (H5_daos_file_t *)va_arg(arguments, void *);
            hbool_t *is_equal = va_arg(arguments, hbool_t *);

            if(!file || !file2)
                *is_equal = FALSE;
            else
                *is_equal = (memcmp(&file->uuid, &file2->uuid, sizeof(file->uuid)) == 0);
            break;
        } /* H5VL_FILE_IS_EQUAL */

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

    /*
     * Ensure that all other processes are done with the file before
     * closing the container handle. This is to prevent invalid handle
     * issues due to rank 0 closing the container handle before other
     * ranks are done using it.
     */
    if(file->num_procs > 1 && (MPI_SUCCESS != MPI_Barrier(file->comm)))
        D_GOTO_ERROR(H5E_FILE, H5E_MPI, FAIL, "MPI_Barrier failed during file close")

    /* Close the file */
    if(H5_daos_file_close_helper(file, dxpl_id, req) < 0)
        D_GOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close file")

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_file_close() */


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
herr_t
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
 * Function:    H5_daos_fill_fapl_cache
 *
 * Purpose:     Fills the "fapl_cache" field of the file struct, using the
 *              file's FAPL.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_daos_fill_fapl_cache(H5_daos_file_t *file, hid_t fapl_id)
{
    char *oclass_str = NULL;
    htri_t prop_exists;
    herr_t ret_value = SUCCEED;

    assert(file);

    /* Determine if we requested collective metadata reads for the file */
    file->fapl_cache.is_collective_md_read = FALSE;
    if(H5P_FILE_ACCESS_DEFAULT != fapl_id)
        if(H5Pget_all_coll_metadata_ops(fapl_id, &file->fapl_cache.is_collective_md_read) < 0)
            D_GOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get collective metadata reads property")

    /* Check for file default object class set on fapl_id */
    /* Note we do not copy the oclass_str in the property callbacks (there is no
     * "get" callback, so this is more like an H5P_peek, and we do not need to
     * free oclass_str as it points directly into the plist value */
    file->fapl_cache.default_object_class = OC_UNKNOWN;
    if((prop_exists = H5Pexist(fapl_id, H5_DAOS_OBJ_CLASS_NAME)) < 0)
        D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for object class property")
    if(prop_exists) {
        if(H5Pget(fapl_id, H5_DAOS_OBJ_CLASS_NAME, &oclass_str) < 0)
            D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get object class")
        if(oclass_str && (oclass_str[0] != '\0'))
            if(OC_UNKNOWN == (file->fapl_cache.default_object_class = daos_oclass_name2id(oclass_str)))
                D_GOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unknown object class")
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_fill_fapl_cache() */


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

        if(id_udata->obj_count < id_udata->max_objs) {
            if(!uuid_compare(cur_obj->item.file->uuid, id_udata->file_id))
                id_udata->oid_list[id_udata->obj_count++] = id;
        }
        else
            ret_value = H5_ITER_STOP;
    } /* end if */

done:
    D_FUNC_LEAVE
} /* end H5_daos_get_obj_ids_callback() */

