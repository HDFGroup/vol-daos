/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
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

#include "daos_vol.h"
#include "daos_vol_err.h"


/*-------------------------------------------------------------------------
 * Function:    H5_daos_err_to_string
 *
 * Purpose:     Function to convert a DAOS failure return value into a
 *              meaningful string.
 *
 * Return:      String representing given error code (can't fail)
 *
 * Programmer:  Jordan Henderson
 *              January, 2019
 *
 *-------------------------------------------------------------------------
 */
const char*
H5_daos_err_to_string(int ret)
{
    switch (ret) {
        /*
         * GURT errors
         */
        case -DER_NO_PERM:
            return "no permission";
        case -DER_NO_HDL:
            return "invalid handle specified";
        case -DER_INVAL:
            return "invalid parameters specified";
        case -DER_EXIST:
            return "entity already exists";
        case -DER_NONEXIST:
            return "nonexistent entity";
        case -DER_UNREACH:
            return "node is unreachable";
        case -DER_NOSPACE:
            return "no space on storage target";
        case -DER_ALREADY:
            return "already initialized"; /* TODO: may not always refer to library initialization */
        case -DER_NOMEM:
            return "out of memory";
        case -DER_NOSYS:
            return "function not implemented";
        case -DER_TIMEDOUT:
            return "timed out";
        case -DER_BUSY:
            return "busy";
        case -DER_AGAIN:
            return "try again";
        case -DER_PROTO:
            return "incompatible protocol";
        case -DER_UNINIT:
            return "not initialized";
        case -DER_TRUNC:
            return "buffer too short (larger buffer needed)";
        case -DER_OVERFLOW:
            return "data too long for defined data type or buffer size";
        case -DER_CANCELED:
            return "operation canceled";
        case -DER_OOG:
            return "out-of-group or member list";
        case -DER_HG:
            return "transport layer mercury error";
        case -DER_UNREG:
            return "RPC or protocol version not registered";
        case -DER_ADDRSTR_GEN:
            return "failed to generate an address string";
        case -DER_PMIX:
            return "PMIx layer error";
        case -DER_IVCB_FORWARD:
            return "IV callback - cannot handle locally";
        case -DER_MISC:
            return "miscellaneous error";
        case -DER_BADPATH:
            return "bad path name";
        case -DER_NOTDIR:
            return "not a directory";
        case -DER_CORPC_INCOMPLETE:
            return "corpc failure";
        case -DER_NO_RAS_RANK:
            return "no rank is subscribed to RAS";
        case -DER_NOTATTACH:
            return "service group not attached";
        case -DER_MISMATCH:
            return "version mismatch";
        case -DER_EVICTED:
            return "rank has been evicted";
        case -DER_NOREPLY:
            return "user-provided RPC handler didn't send reply back";
        case -DER_DOS:
            return "denial-of-service";

        /*
         * DAOS errors
         */
        case -DER_IO:
            return "generic I/O error";
        case -DER_FREE_MEM:
            return "memory free error";
        case -DER_ENOENT:
            return "entry not found";
        case -DER_NOTYPE:
            return "unknown object type";
        case -DER_NOSCHEMA:
            return "unknown object schema";
        case -DER_NOLOCAL:
            return "object is not local";
        case -DER_STALE:
            return "stale pool map version";
        case -DER_NOTLEADER:
            return "not the service leader";
        case -DER_TGT_CREATE:
            return "target creation error";
        case -DER_EP_RO:
            return "epoch is read-only";
        case -DER_EP_OLD:
            return "epoch is too old, all data has been recycled";
        case -DER_KEY2BIG:
            return "key is too large";
        case -DER_REC2BIG:
            return "record is too large";
        case -DER_IO_INVAL:
            return "IO buffers can't match object extents";
        case -DER_EQ_BUSY:
            return "event queue is busy";
        case -DER_DOMAIN:
            return "Domain of cluster component can't match";
        case -DER_SHUTDOWN:
            return "";
        case -DER_INPROGRESS:
            return "operation in progress";
        case -DER_NOTAPPLICABLE:
            return "not applicable";

        default:
            return "invalid error code or no error";
    }
}
