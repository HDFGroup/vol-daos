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
 * Purpose: Routines used for testing the DAOS VOL connector from an
 *          external HDF5 application.
 */

#include "daos_vol.h"           /* DAOS connector                          */
#include "util/daos_vol_err.h"  /* DAOS connector error handling           */


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
    H5_daos_file_t *file = NULL;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if(file_id < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file ID is invalid");
    if(!poh)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "poh pointer is NULL");

    if(NULL == (file = (H5_daos_file_t *)H5VLobject(file_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if(H5I_FILE != file->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");

    poh->cookie = file->container_poh.cookie;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_poh() */


/*-------------------------------------------------------------------------
 * Function:    H5daos_get_pool_uuid
 *
 * Purpose:     Internal API function to return the pool UUID for a file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_get_pool_uuid(hid_t file_id, uuid_t *pool_uuid)
{
    H5_daos_file_t *file = NULL;
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if(file_id < 0)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file ID is invalid");
    if(!pool_uuid)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pool_uuid pointer is NULL");

    if(NULL == (file = (H5_daos_file_t *)H5VLobject(file_id)))
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "VOL object is NULL");
    if(H5I_FILE != file->item.type)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "object is not a file");

    uuid_copy(*pool_uuid, file->puuid);

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_pool_uuid() */


/*-------------------------------------------------------------------------
 * Function:    H5daos_get_global_svcl
 *
 * Purpose:     Internal API function to return the global pool replica
 *              service rank list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_get_global_svcl(d_rank_list_t *svcl)
{
    herr_t ret_value = SUCCEED;

    H5_daos_inc_api_cnt();

    if(!svcl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "svcl pointer is NULL");
    *svcl = H5_daos_pool_svcl_g;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_svcl() */
