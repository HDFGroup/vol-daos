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
 * Purpose:	The public header file for the DAOS VOL connector.
 */

#ifndef daos_vol_public_H
#define daos_vol_public_H

#include "daos_vol_config.h"

/* Public headers needed by this file */
#include <hdf5.h>
#include <mpi.h>
#include <uuid/uuid.h>
#include <daos.h>

/*****************/
/* Public Macros */
/*****************/

#define H5_DAOS_SNAP_ID_INVAL (uint64_t)(int64_t)-1

/*******************/
/* Public Typedefs */
/*******************/

typedef uint64_t H5_daos_snap_id_t;

/********************/
/* Public Variables */
/********************/

/*********************/
/* Public Prototypes */
/*********************/

#ifdef __cplusplus
extern "C" {
#endif

H5VL_DAOS_PUBLIC herr_t H5daos_init(uuid_t pool_uuid, const char *pool_grp, const char *pool_svcl);
H5VL_DAOS_PUBLIC herr_t H5daos_term(void);
H5VL_DAOS_PUBLIC herr_t H5Pset_fapl_daos(hid_t fapl_id, MPI_Comm comm, MPI_Info info);
H5VL_DAOS_PUBLIC herr_t H5daos_set_object_class(hid_t plist_id, char *object_class);
H5VL_DAOS_PUBLIC ssize_t H5daos_get_object_class(hid_t plist_id, char *object_class, size_t size);
H5VL_DAOS_PUBLIC herr_t H5daos_set_root_open_object_class(hid_t fapl_id, char *object_class);
H5VL_DAOS_PUBLIC ssize_t H5daos_get_root_open_object_class(hid_t fapl_id, char *object_class, size_t size);
H5VL_DAOS_PUBLIC herr_t H5daos_set_all_ind_metadata_ops(hid_t accpl_id, hbool_t is_independent);
H5VL_DAOS_PUBLIC herr_t H5daos_get_all_ind_metadata_ops(hid_t accpl_id, hbool_t *is_independent);
#ifdef DSINC
H5VL_DAOS_PUBLIC herr_t H5daos_snap_create(hid_t loc_id,
    H5_daos_snap_id_t *snap_id);
#endif
#ifdef DV_HAVE_SNAP_OPEN_ID
H5VL_DAOS_PUBLIC herr_t H5Pset_daos_snap_open(hid_t fapl_id,
    H5_daos_snap_id_t snap_id);
#endif

H5VL_DAOS_PUBLIC herr_t H5daos_get_poh(hid_t file_id, daos_handle_t *poh);
H5VL_DAOS_PUBLIC herr_t H5daos_get_pool_uuid(hid_t file_id, uuid_t *pool_uuid);
H5VL_DAOS_PUBLIC herr_t H5daos_get_global_svcl(d_rank_list_t *svcl);

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_public_H */
