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
H5daos_get_poh(daos_handle_t *poh) {
    herr_t ret_value = SUCCEED;

    if(!poh)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "poh pointer is NULL");
    poh->cookie = H5_daos_poh_g.cookie;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_poh() */


/*-------------------------------------------------------------------------
 * Function:    H5daos_get_global_pool_uuid
 *
 * Purpose:     Internal API function to return the global pool's UUID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5daos_get_global_pool_uuid(uuid_t *pool_uuid)
{
    herr_t ret_value = SUCCEED;

    if(!pool_uuid)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pool_uuid pointer is NULL");
    uuid_copy(*pool_uuid, H5_daos_pool_uuid_g);

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

    if(!svcl)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "svcl pointer is NULL");
    *svcl = H5_daos_pool_svcl_g;

done:
    D_FUNC_LEAVE_API;
} /* end H5daos_get_svcl() */
