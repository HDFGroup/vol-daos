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
        case DER_SUCCESS:
            return "no error (DER_SUCCESS)";

        /*
         * Private errors
         */
        case H5_DAOS_INCOMPLETE:
            return "object open incomplete (should not see this)";
        case H5_DAOS_PRE_ERROR:
            return "error in earlier task (should not see this)";
        case H5_DAOS_CLOSE_ERROR:
            return "failed to close HDF5 object";
        /*
         * GURT errors
         */
        case -DER_NO_PERM:
            return "no permission (DER_NO_PERM)";
        case -DER_NO_HDL:
            return "invalid handle specified (DER_NO_HDL)";
        case -DER_INVAL:
            return "invalid parameters specified (DER_INVAL)";
        case -DER_EXIST:
            return "entity already exists (DER_EXIST)";
        case -DER_NONEXIST:
            return "nonexistent entity (DER_NONEXIST)";
        case -DER_UNREACH:
            return "node is unreachable (DER_UNREACH)";
        case -DER_NOSPACE:
            return "no space on storage target (DER_NOSPACE)";
        case -DER_ALREADY:
            return "already initialized (DER_ALREADY)"; /* TODO: may not always refer to library initialization */
        case -DER_NOMEM:
            return "out of memory (DER_NOMEM)";
        case -DER_NOSYS:
            return "function not implemented (DER_NOSYS)";
        case -DER_TIMEDOUT:
            return "timed out (DER_TIMEDOUT)";
        case -DER_BUSY:
            return "busy (DER_BUSY)";
        case -DER_AGAIN:
            return "try again (DER_AGAIN)";
        case -DER_PROTO:
            return "incompatible protocol (DER_PROTO)";
        case -DER_UNINIT:
            return "not initialized (DER_UNINIT)";
        case -DER_TRUNC:
            return "buffer too short (larger buffer needed) (DER_TRUNC)";
        case -DER_OVERFLOW:
            return "data too long for defined data type or buffer size (DER_OVERFLOW)";
        case -DER_CANCELED:
            return "operation canceled (DER_CANCELED)";
        case -DER_OOG:
            return "out-of-group or member list (DER_OOG)";
        case -DER_HG:
            return "transport layer mercury error (DER_HG)";
        case -DER_UNREG:
            return "RPC or protocol version not registered (DER_UNREG)";
        case -DER_ADDRSTR_GEN:
            return "failed to generate an address string (DER_ADDRSTR_GEN)";
        case -DER_PMIX:
            return "PMIx layer error (DER_PMIX)";
        case -DER_IVCB_FORWARD:
            return "IV callback - cannot handle locally (DER_IVCB_FORWARD)";
        case -DER_MISC:
            return "miscellaneous error (DER_MISC)";
        case -DER_BADPATH:
            return "bad path name (DER_BADPATH)";
        case -DER_NOTDIR:
            return "not a directory (DER_NOTDIR)";
        case -DER_CORPC_INCOMPLETE:
            return "corpc failure (DER_CORPC_INCOMPLETE)";
        case -DER_NO_RAS_RANK:
            return "no rank is subscribed to RAS (DER_NO_RAS_RANK)";
        case -DER_NOTATTACH:
            return "service group not attached (DER_NOTATTACH)";
        case -DER_MISMATCH:
            return "version mismatch (DER_MISMATCH)";
        case -DER_EVICTED:
            return "rank has been evicted (DER_EVICTED)";
        case -DER_NOREPLY:
            return "user-provided RPC handler didn't send reply back (DER_NOREPLY)";
        case -DER_DOS:
            return "denial-of-service (DER_DOS)";

        /*
         * DAOS errors
         */
        case -DER_IO:
            return "generic I/O error (DER_IO)";
        case -DER_FREE_MEM:
            return "memory free error (DER_FREE_MEM)";
        case -DER_ENOENT:
            return "entry not found (DER_ENOENT)";
        case -DER_NOTYPE:
            return "unknown object type (DER_NOTYPE)";
        case -DER_NOSCHEMA:
            return "unknown object schema (DER_NOSCHEMA)";
        case -DER_NOLOCAL:
            return "object is not local (DER_NOLOCAL)";
        case -DER_STALE:
            return "stale pool map version (DER_STALE)";
        case -DER_NOTLEADER:
            return "not the service leader (DER_NOTLEADER)";
        case -DER_TGT_CREATE:
            return "target creation error (DER_TGT_CREATE)";
        case -DER_EP_RO:
            return "epoch is read-only (DER_EP_RO)";
        case -DER_EP_OLD:
            return "epoch is too old, all data has been recycled (DER_EP_OLD)";
        case -DER_KEY2BIG:
            return "key is too large (DER_KEY2BIG)";
        case -DER_REC2BIG:
            return "record is too large (DER_REC2BIG)";
        case -DER_IO_INVAL:
            return "IO buffers can't match object extents (DER_IO_INVAL)";
        case -DER_EQ_BUSY:
            return "event queue is busy (DER_EQ_BUSY)";
        case -DER_DOMAIN:
            return "Domain of cluster component can't match (DER_DOMAIN)";
        case -DER_SHUTDOWN:
            return "service should shut down (DER_SHUTDOWN)";
        case -DER_INPROGRESS:
            return "operation in progress (DER_INPROGRESS)";
        case -DER_NOTAPPLICABLE:
            return "not applicable (DER_NOTAPPLICABLE)";

        default:
            return d_errstr(ret);
#if 0
        default:
            return "invalid error code or no error";
#endif
    }
}
