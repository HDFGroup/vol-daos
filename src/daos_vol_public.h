/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
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

/*
 * Programmer:  Neil Fortner <nfortne2@hdfgroup.gov>
 *              December, 2016
 *
 * Purpose:	The public header file for the DAOS VOL plugin.
 */
#ifndef daos_vol_public_H
#define daos_vol_public_H

#define H5_HAVE_EFF 1 /* DSINC */

/* System headers needed by this file */
#include <uuid/uuid.h>

#include "H5PLextern.h"

/* Public headers needed by this file */
#include "H5public.h"
#include "H5Ipublic.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef H5_HAVE_EFF

#define H5_DAOS_SNAP_ID_INVAL (uint64_t)(int64_t)-1

typedef uint64_t H5_daos_snap_id_t;

/* DSINC - need to redefine H5_DLL since this will not work correctly for an external plugin */
H5PLUGIN_DLL herr_t H5daos_init(MPI_Comm pool_comm, uuid_t pool_uuid,
    char *pool_grp);
H5PLUGIN_DLL herr_t H5daos_term(void);
H5PLUGIN_DLL herr_t H5Pset_fapl_daos(hid_t fapl_id, MPI_Comm comm, MPI_Info info);
H5PLUGIN_DLL herr_t H5daos_snap_create(hid_t loc_id,
    H5_daos_snap_id_t *snap_id);
#ifdef DV_HAVE_SNAP_OPEN_ID
H5PLUGIN_DLL herr_t H5Pset_daos_snap_open(hid_t fapl_id,
    H5_daos_snap_id_t snap_id);
#endif
/* H5_DLL herr_t EFF_init(void); DSINC */
/* H5_DLL herr_t EFF_finalize(void); DSINC */

#endif /* H5_HAVE_EFF */

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_public_H */
