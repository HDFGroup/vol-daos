/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: The DAOS VOL connector where access is forwarded to the DAOS
 *          library.  Asynchronous request routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_free
 *
 * Purpose:     Decrement the reference count on the request and free it
 *              if the ref count drops to 0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_req_free(void *req)
{
    herr_t     ret_value = SUCCEED;            /* Return value */

    if(!req)
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "request object is NULL")

    H5_daos_req_free_int(req);

done:
    D_FUNC_LEAVE_API
} /* end H5_daos_req_free() */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_free_int
 *
 * Purpose:     Internal version of H5_daos_req_free().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
void
H5_daos_req_free_int(void *_req)
{
    H5_daos_req_t *req = (H5_daos_req_t *)_req;

    assert(req);

    if(--req->rc == 0) {
        H5_daos_file_decref(req->file);
        DV_free(req);
    } /* end if */

    return;
} /* end H5_daos_req_free_int() */

