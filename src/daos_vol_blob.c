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
 * library. Blob routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */



/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_blob_put(void *_file, const void *buf, size_t size, void *blob_id,
    void H5VL_DAOS_UNUSED *_ctx)
{
    uuid_t blob_uuid;                   /* Blob ID */
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    int ret;
    herr_t ret_value = SUCCEED;         /* Return value */

    assert(H5_DAOS_BLOB_ID_SIZE == sizeof(blob_uuid));

    /* Check parameters */
    if(!buf && size > 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer is NULL but size > 0");
    if(!blob_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "blob id is NULL");
    if(!file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(file->sched, FAIL);

    /* Generate blob ID as a UUID */
    uuid_generate(blob_uuid);

    /* Copy uuid to blob_id output buffer */
    (void)memcpy(blob_id, &blob_uuid, H5_DAOS_BLOB_ID_SIZE);

    /* Only write if size > 0 */
    if(size > 0) {
        /* Set up dkey */
        daos_iov_set(&dkey, blob_id, H5_DAOS_BLOB_ID_SIZE);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)H5_daos_blob_key_g, H5_daos_blob_key_size_g);
        iod.iod_nr = 1u;
        iod.iod_size = (uint64_t)size;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, (void *)buf, (daos_size_t)size);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Write blob */
        if(0 != (ret = daos_obj_update(file->glob_md_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*event*/)))
            D_GOTO_ERROR(H5E_VOL, H5E_WRITEERROR, FAIL, "can't write blob to object: %s", H5_daos_err_to_string(ret));
    } /* end if */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_blob_put() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_blob_get(void *_file, const void *blob_id, void *buf, size_t size,
    void H5VL_DAOS_UNUSED *_ctx)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
    daos_key_t dkey;
    daos_iod_t iod;
    daos_sg_list_t sgl;
    daos_iov_t sg_iov;
    int ret;
    herr_t ret_value = SUCCEED;         /* Return value */

    /* Check parameters */
    if(!buf)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer is NULL");
    if(!blob_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "blob id is NULL");
    if(!file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(file->sched, FAIL);

    /* Only read if size > 0 */
    if(size > 0) {
        /* Set up dkey */
        daos_iov_set(&dkey, (void *)blob_id, H5_DAOS_BLOB_ID_SIZE);

        /* Set up iod */
        memset(&iod, 0, sizeof(iod));
        daos_iov_set(&iod.iod_name, (void *)H5_daos_blob_key_g, H5_daos_blob_key_size_g);
        iod.iod_nr = 1u;
        iod.iod_size = (uint64_t)size;
        iod.iod_type = DAOS_IOD_SINGLE;

        /* Set up sgl */
        daos_iov_set(&sg_iov, (void *)buf, (daos_size_t)size);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &sg_iov;

        /* Read blob */
        if(0 != (ret = daos_obj_fetch(file->glob_md_oh, DAOS_TX_NONE, 0 /*flags*/, &dkey, 1, &iod, &sgl, NULL /*ioms*/, NULL /*event*/)))
            D_GOTO_ERROR(H5E_VOL, H5E_READERROR, FAIL, "can't read blob from object: %s", H5_daos_err_to_string(ret));
    } /* end if */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_blob_get() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_blob_specific(void *_file, void *blob_id,
    H5VL_blob_specific_t specific_type, va_list arguments)
{
    H5_daos_file_t *file = (H5_daos_file_t *)_file;
    int ret;
    herr_t ret_value = SUCCEED;         /* Return value */

    /* Check parameters */
    if(!blob_id)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "blob id is NULL");
    if(!file)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file object is NULL");

    H5_DAOS_MAKE_ASYNC_PROGRESS(file->sched, FAIL);

    switch(specific_type) {
        case H5VL_BLOB_GETSIZE:
            {
                /* This callback probably doesn't need to exist DSINC */
                /* If we decide to implement this we must remove the code which
                 * prevents writing 0 size blobs */
                D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported blob specific operation");

                break;
            }

        case H5VL_BLOB_ISNULL:
            {
                hbool_t *isnull = va_arg(arguments, hbool_t *);
                uint8_t nul_buf[H5_DAOS_BLOB_ID_SIZE];

                /* Check parameters */
                if(!isnull)
                    D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "isnull output buffer is NULL");

                /* Initialize comparison buffer */
                (void)memset(nul_buf, 0, sizeof(nul_buf));

                /* Check if blob ID matches NULL ID */
                *isnull = (memcmp(blob_id, nul_buf, H5_DAOS_BLOB_ID_SIZE) == 0);

                break;
            }

        case H5VL_BLOB_SETNULL:
            {
                /* Encode NULL blob ID */
                (void)memset((void *)blob_id, 0, H5_DAOS_BLOB_ID_SIZE);

                break;
            }

        case H5VL_BLOB_DELETE:
            {
                daos_key_t dkey;

                /* Set up dkey */
                daos_iov_set(&dkey, blob_id, H5_DAOS_BLOB_ID_SIZE);

                /* Punch the blob's dkey, along with all of its akeys.  Due to
                 * the (practical) guarantee of uniqueness of UUIDs, we can
                 * assume we won't delete any unintended akeys. */
                if(0 != (ret = daos_obj_punch_dkeys(file->glob_md_oh, DAOS_TX_NONE, 0 /*flags*/, 1, &dkey, NULL /*event*/)))
                    D_GOTO_ERROR(H5E_VOL, H5E_CANTREMOVE, FAIL, "failed to punch blob dkey: %s", H5_daos_err_to_string(ret));

                break;
            }

        default:
            D_GOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid unsupported blob specific operation");
    } /* end switch */

done:
    D_FUNC_LEAVE_API;
} /* end H5_daos_blob_specific() */

