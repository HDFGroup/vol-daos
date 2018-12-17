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
 * Programmer:  Neil Fortner <nfortne2@hdfgroup.org>
 *              September, 2016
 *
 * Purpose:	The private header file for the DAOS VOL plugin.
 */
#ifndef daos_vol_H
#define daos_vol_H

#define H5_HAVE_EFF 1 /* DSINC */

/* Include package's public header */
#include "daos_vol_public.h"

#ifdef H5_HAVE_EFF

#include "daos.h"

#define HDF5_VOL_DAOS_VERSION_1	1	/* Version number of DAOS VOL plugin */

#define H5_VOL_DAOS_CLS_VAL (H5VL_class_value_t) H5_VOL_RESERVED + 2 /* Class value of the DAOS VOL plugin as defined in H5VLpublic.h DSINC */

#define H5_DAOS_VOL_NAME "daos"

#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef H5_HAVE_EFF

/* FAPL property to tell the VOL plugin to open a saved snapshot when opening a
 * file */
#ifdef DV_HAVE_SNAP_OPEN_ID
#define H5_DAOS_SNAP_OPEN_ID "daos_snap_open"
#endif

/* Common object and attribute information */
typedef struct H5_daos_item_t {
    H5I_type_t type;
    struct H5_daos_file_t *file;
    int rc;
} H5_daos_item_t;

/* Common object information */
typedef struct H5_daos_obj_t {
    H5_daos_item_t item; /* Must be first */
    daos_obj_id_t oid;
    daos_handle_t obj_oh;
} H5_daos_obj_t;

/* The file struct */
typedef struct H5_daos_file_t {
    H5_daos_item_t item; /* Must be first */
    daos_handle_t coh;
    daos_epoch_t epoch;
    int snap_epoch;
    char *file_name;
    uuid_t uuid;
    unsigned flags;
    daos_handle_t glob_md_oh;
    struct H5_daos_group_t *root_grp;
    uint64_t max_oid;
    hbool_t max_oid_dirty;
    hid_t fcpl_id;
    hid_t fapl_id;
    MPI_Comm comm;
    MPI_Info info;
    int my_rank;
    int num_procs;
    hbool_t collective;
} H5_daos_file_t;

/* The group struct */
typedef struct H5_daos_group_t {
    H5_daos_obj_t obj; /* Must be first */
    hid_t gcpl_id;
    hid_t gapl_id;
} H5_daos_group_t;

/* The dataset struct */
typedef struct H5_daos_dset_t {
    H5_daos_obj_t obj; /* Must be first */
    hid_t type_id;
    hid_t space_id;
    hid_t dcpl_id;
    hid_t dapl_id;
} H5_daos_dset_t;

/* The datatype struct */
/* Note we could speed things up a bit by caching the serialized datatype.  We
 * may also not need to keep the type_id around.  -NAF */
typedef struct H5_daos_dtype_t {
    H5_daos_obj_t obj; /* Must be first */
    hid_t type_id;
    hid_t tcpl_id;
    hid_t tapl_id;
} H5_daos_dtype_t;

/* The map struct */
typedef struct H5_daos_map_t {
    H5_daos_obj_t obj; /* Must be first */
    hid_t ktype_id;
    hid_t vtype_id;
} H5_daos_map_t;

/* The attribute struct */
typedef struct H5_daos_attr_t {
    H5_daos_item_t item; /* Must be first */
    H5_daos_obj_t *parent;
    char *name;
    hid_t type_id;
    hid_t space_id;
} H5_daos_attr_t;

/* The link value struct */
typedef struct H5_daos_link_val_t {
    H5L_type_t type;
    union {
        daos_obj_id_t hard;
        char *soft;
    } target;
} H5_daos_link_val_t;

/* XXX: The following two definitions are only here until they are
 * moved out of their respective H5Xpkg.h header files and into a
 * more public scope. They are still needed for the DAOS VOL to handle
 * these API calls being made.
 */
typedef enum H5VL_file_optional_t {
    H5VL_FILE_CLEAR_ELINK_CACHE,        /* Clear external link cache               */
    H5VL_FILE_GET_FILE_IMAGE,           /* file image                              */
    H5VL_FILE_GET_FREE_SECTIONS,        /* file free selections                    */
    H5VL_FILE_GET_FREE_SPACE,           /* file freespace                          */
    H5VL_FILE_GET_INFO,                 /* file info                               */
    H5VL_FILE_GET_MDC_CONF,             /* file metadata cache configuration       */
    H5VL_FILE_GET_MDC_HR,               /* file metadata cache hit rate            */
    H5VL_FILE_GET_MDC_SIZE,             /* file metadata cache size                */
    H5VL_FILE_GET_SIZE,                 /* file size                               */
    H5VL_FILE_GET_VFD_HANDLE,           /* file VFD handle                         */
    H5VL_FILE_GET_FILE_ID,              /* retrieve or resurrect file ID of object */
    H5VL_FILE_RESET_MDC_HIT_RATE,       /* get metadata cache hit rate             */
    H5VL_FILE_SET_MDC_CONFIG,           /* set metadata cache configuration        */
    H5VL_FILE_GET_METADATA_READ_RETRY_INFO,
    H5VL_FILE_START_SWMR_WRITE,
    H5VL_FILE_START_MDC_LOGGING,
    H5VL_FILE_STOP_MDC_LOGGING,
    H5VL_FILE_GET_MDC_LOGGING_STATUS,
    H5VL_FILE_FORMAT_CONVERT,
    H5VL_FILE_RESET_PAGE_BUFFERING_STATS,
    H5VL_FILE_GET_PAGE_BUFFERING_STATS,
    H5VL_FILE_GET_MDC_IMAGE_INFO,
    H5VL_FILE_GET_EOA,
    H5VL_FILE_INCR_FILESIZE,
    H5VL_FILE_SET_LIBVER_BOUNDS
} H5VL_file_optional_t;

/* types for object optional VOL operations */
typedef enum H5VL_object_optional_t {
    H5VL_OBJECT_GET_COMMENT,            /* get object comment                   */
    H5VL_OBJECT_GET_INFO,               /* get object info                      */
    H5VL_OBJECT_SET_COMMENT             /* set object comment                   */
} H5VL_object_optional_t;

extern hid_t H5_DAOS_g;

H5_DLL herr_t H5_daos_init(hid_t vipl_id);

H5_DLL void * H5_daos_map_create(void *_item, H5VL_loc_params_t *loc_params, const char *name,
				    hid_t ktype_id, hid_t vtype_id, hid_t mcpl_id, hid_t mapl_id,
				    hid_t dxpl_id, void **req);
H5_DLL void * H5_daos_map_open(void *_item, H5VL_loc_params_t *loc_params, const char *name,
				  hid_t mapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5_daos_map_set(void *_map, hid_t key_mem_type_id, const void *key, 
				 hid_t val_mem_type_id, const void *value, hid_t dxpl_id, void **req);
H5_DLL herr_t H5_daos_map_get(void *_map, hid_t key_mem_type_id, const void *key, 
				 hid_t val_mem_type_id, void *value, hid_t dxpl_id, void **req);
H5_DLL herr_t H5_daos_map_get_types(void *_map, hid_t *key_type_id, hid_t *val_type_id, void **req);
H5_DLL herr_t H5_daos_map_get_count(void *_map, hsize_t *count, void **req);
H5_DLL herr_t H5_daos_map_exists(void *_map, hid_t key_mem_type_id, const void *key, 
				    hbool_t *exists, void **req);
H5_DLL herr_t H5_daos_map_close(void *_map, hid_t dxpl_id, void **req);

#endif /* H5_HAVE_EFF */

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_H */
