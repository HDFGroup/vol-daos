/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Routines used for testing the DAOS VOL connector from an
 *          external HDF5 application.
 */

#include "daos_vol_private.h"  /* DAOS connector                          */
#include "util/daos_vol_err.h" /* DAOS connector error handling           */

/*-------------------------------------------------------------------------
 * Function:    H5daos_get_poh
 *
 * Purpose:     Internal API function to return the pool object handle
 *              for the recovery test.
 *
 * Return:      Success:    Non-negative.
 *
 *              Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_get_poh(hid_t file_id, daos_handle_t *poh)
{
    H5_daos_file_t *file      = NULL;
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (file_id < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file ID is invalid");
    if (!poh)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "poh pointer is NULL");

    if (NULL == (file = (H5_daos_file_t *)H5VLobject(file_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (H5I_FILE != file->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");

    poh->cookie = file->container_poh.cookie;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_poh() */

/*-------------------------------------------------------------------------
 * Function:    H5daos_get_pool
 *
 * Purpose:     Internal API function to return the pool UUID for a file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_get_pool(hid_t file_id, char *pool)
{
    H5_daos_file_t *file      = NULL;
    herr_t          ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if (file_id < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file ID is invalid");
    if (!pool)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pool pointer is NULL");

    if (NULL == (file = (H5_daos_file_t *)H5VLobject(file_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if (H5I_FILE != file->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");

    strcpy(pool, file->facc_params.pool);

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_pool_uuid() */
