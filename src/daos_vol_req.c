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
 *          library.  Asynchronous request routines.
 */

#include "daos_vol.h"           /* DAOS connector                          */

#include "util/daos_vol_err.h"  /* DAOS connector error handling           */
#include "util/daos_vol_mem.h"  /* DAOS connector memory management        */


/*-------------------------------------------------------------------------
 * Function:    H5_daos_req_create
 *
 * Purpose:     Create a request.  If the operation will never need to use
 *              the dxpl_id it is OK to pass H5I_INVALID_HID to avoid
 *              H5Pcopy(), even if a DXPL is available.
 *
 * Return:      Valid pointer on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5_daos_req_t *
H5_daos_req_create(H5_daos_file_t *file, hid_t dxpl_id)
{
    H5_daos_req_t *ret_value = NULL;

    assert(file);

    if(NULL == (ret_value = (H5_daos_req_t *)DV_malloc(sizeof(H5_daos_req_t))))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for request");
    ret_value->th = DAOS_TX_NONE;
    ret_value->th_open = FALSE;
    ret_value->file = file;
    if(dxpl_id == H5I_INVALID_HID)
        ret_value->dxpl_id = H5I_INVALID_HID;
    else
        if((ret_value->dxpl_id = H5Pcopy(dxpl_id)) < 0) {
            DV_free(ret_value);
            D_GOTO_ERROR(H5E_VOL, H5E_CANTCOPY, NULL, "can't copy data transfer property list");
        } /* end if */
    ret_value->file->item.rc++;
    ret_value->rc = 1;
    ret_value->status = -H5_DAOS_INCOMPLETE;
    ret_value->failed_task = NULL;

done:
    D_FUNC_LEAVE;
} /* end H5_daos_req_create() */


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
        D_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "request object is NULL");

    if(H5_daos_req_free_int(req) < 0)
        D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't free request");

done:
    D_FUNC_LEAVE_API;
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
herr_t
H5_daos_req_free_int(H5_daos_req_t *req)
{
    herr_t ret_value = SUCCEED;

    assert(req);

    if(--req->rc == 0) {
        if(req->dxpl_id >= 0)
            if(H5Pclose(req->dxpl_id) < 0) {
                /* If H5Pclose failed we must update the request status, since
                 * the calling function can't access the request after calling
                 * this function.  Note this task name isn't very specific.
                 * This should be ok here since this plist isn't visible to the
                 * user and this failure shouldn't be caused by user errors,
                 * only errors in HDF5 and this connector. */
                if(req->status >= -H5_DAOS_INCOMPLETE) {
                    req->status = -H5_DAOS_H5_CLOSE_ERROR;
                    req->failed_task = "request free";
                } /* end if */
                D_DONE_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't close data transfer property list");
            } /* end if */
        H5_daos_file_decref(req->file);
        DV_free(req);
    } /* end if */

done:
    D_FUNC_LEAVE;
} /* end H5_daos_req_free_int() */

