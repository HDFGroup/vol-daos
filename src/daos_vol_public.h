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

/* These values are used when registering and identifying the DAOS VOL
 * connector. They can be helpful when working with plugins and for passing
 * to the HDF5 command-line tools.
 */
#define H5_DAOS_CONNECTOR_NAME      "daos"
#define H5_DAOS_CONNECTOR_NAME_LEN  4

#define H5_DAOS_CONNECTOR_VALUE     ((H5VL_class_value_t)4004)

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

/**
 * Modify the file access property list to use the DAOS VOL connector.
 *
 * \pool_uuid identifies the UUID of the DAOS pool to connect to.
 *
 * \pool_grp and \pool_svcl respectively identify the server group name
 * and pool service replica rank list to use when connecting to DAOS.
 * These may be NULL, in which case a default group name and service
 * replica rank list are used.
 *
 * \comm and \info identify the communicator and info object used to
 * coordinate actions on file create, open, flush, and close.
 *
 * \param fapl_id   [IN]    File access property list
 * \param pool_uuid [IN]    DAOS pool UUID
 * \param pool_grp  [IN]    Process set name of the DAOS servers managing the pool
 * \param pool_svcl [IN]    Comma-separated list of pool service replica ranks
 * \param comm      [IN]    MPI communicator
 * \param info      [IN]    MPI info
 *
 * \return Non-negative on success/Negative on failure
 */
H5VL_DAOS_PUBLIC herr_t
H5Pset_fapl_daos(hid_t fapl_id, const uuid_t pool_uuid, const char *pool_grp,
        const char *pool_svcl, MPI_Comm file_comm, MPI_Info file_info);

/**
 * Sets the provided DAOS object class on the given property list.
 * Refer to the DAOS documentation for a list of object classes and
 * descriptions of them.
 *
 * If \plist_id is a File, Group, Datatype, Dataset or Map creation
 * property list, this setting will affect objects created using
 * that property list (a File creation property list affects only
 * the root group and global metadata objects).
 *
 * If \plist_id is a File access property list, this setting will
 * affect all objects created in files opened with the property list,
 * except for those with their object class specified via an object
 * creation property list, as above.
 *
 * If the root group of a file is created with a non-default object
 * class, later opens of the file must use a File access property
 * list with the same object class set on it by using
 * H5daos_set_root_open_object_class().
 *
 * \param plist_id     [IN]     Object creation or File access property list
 * \param object_class [IN]     DAOS object class string
 *
 * \return Non-negative on success/Negative on failure
 */
H5VL_DAOS_PUBLIC herr_t
H5daos_set_object_class(hid_t plist_id, char *object_class);

/**
 * Retrieves the DAOS object class set on the given property list.
 *
 * \param plist_id     [IN]     Object creation or File access property list
 * \param object_class [OUT]    Object class string output buffer
 * \param size         [IN]     Size of object class string output buffer
 *
 * \return Length of object class string (excluding null terminator) on success/Negative on failure
 */
H5VL_DAOS_PUBLIC ssize_t
H5daos_get_object_class(hid_t plist_id, char *object_class, size_t size);

/**
 * Sets the DAOS object class to use for opening the root group
 * of a file on the given File access property list. The specified
 * object class should match the object class used to create the
 * root group.
 *
 * \param fapl_id      [IN]     File access property list
 * \param object_class [IN]     DAOS object class string
 *
 * \return Non-negative on success/Negative on failure
 */
H5VL_DAOS_PUBLIC herr_t
H5daos_set_root_open_object_class(hid_t fapl_id, char *object_class);

/**
 * Retrieves the DAOS object class for opening the root group of a file
 * from the given File access property list.
 *
 * \param fapl_id      [IN]     File access property list
 * \param object_class [OUT]    Object class string output buffer
 * \param size         [IN]     Size of object class string output buffer
 *
 * \return Length of object class string (excluding null terminator) on success/Negative on failure
 */
H5VL_DAOS_PUBLIC ssize_t
H5daos_get_root_open_object_class(hid_t fapl_id, char *object_class, size_t size);

/**
 * Modifies the given access property list to indicate that all
 * metadata I/O operations should be performed independently. By
 * default, metadata reads are independent and metadata writes
 * are collective.
 *
 * \param accpl_id       [IN]   File, Link or Reference access property list
 * \param is_independent [IN]   Boolean flag indicating whether all metadata I/O should be independent
 *
 * \return Non-negative on success/Negative on failure
 */
H5VL_DAOS_PUBLIC herr_t
H5daos_set_all_ind_metadata_ops(hid_t accpl_id, hbool_t is_independent);

/**
 * Retrieves the independent metadata I/O setting from the given
 * access property list.
 *
 * \param accpl_id       [IN]   File, Link or Reference access property list
 * \param is_independent [OUT]  Boolean flag indicating whether all metadata I/O is independent
 *
 * \return Non-negative on success/Negative on failure
 */
H5VL_DAOS_PUBLIC herr_t
H5daos_get_all_ind_metadata_ops(hid_t accpl_id, hbool_t *is_independent);

#ifdef DSINC
H5VL_DAOS_PUBLIC herr_t H5daos_snap_create(hid_t loc_id,
    H5_daos_snap_id_t *snap_id);
#endif
#ifdef DV_HAVE_SNAP_OPEN_ID
H5VL_DAOS_PUBLIC herr_t H5Pset_daos_snap_open(hid_t fapl_id,
    H5_daos_snap_id_t snap_id);
#endif

/**
 * API routines for external testing
 */
H5VL_DAOS_PUBLIC herr_t H5daos_get_poh(hid_t file_id, daos_handle_t *poh);
H5VL_DAOS_PUBLIC herr_t H5daos_get_pool_uuid(hid_t file_id, uuid_t *pool_uuid);
H5VL_DAOS_PUBLIC herr_t H5daos_get_global_svcl(d_rank_list_t *svcl);

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_public_H */
