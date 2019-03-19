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

#ifndef VOL_OBJECT_TEST_H
#define VOL_OBJECT_TEST_H

#include "vol_test.h"

int vol_object_test(void);

/***********************************************
 *                                             *
 *      VOL connector Object test defines      *
 *                                             *
 ***********************************************/

#define OBJECT_OPEN_TEST_SPACE_RANK 2
#define OBJECT_OPEN_TEST_GROUP_NAME "object_open_test"
#define OBJECT_OPEN_TEST_GRP_NAME   "object_open_test_group"
#define OBJECT_OPEN_TEST_DSET_NAME  "object_open_test_dset"
#define OBJECT_OPEN_TEST_TYPE_NAME  "object_open_test_type"

#define OBJECT_OPEN_INVALID_PARAMS_TEST_GROUP_NAME "object_open_invalid_params_test"
#define OBJECT_OPEN_INVALID_PARAMS_TEST_GRP_NAME   "object_open_invalid_params_test_group"

#define OBJECT_EXISTS_TEST_DSET_SPACE_RANK 2
#define OBJECT_EXISTS_TEST_SUBGROUP_NAME   "object_exists_test"
#define OBJECT_EXISTS_TEST_GRP_NAME        "object_exists_test_group"
#define OBJECT_EXISTS_TEST_TYPE_NAME       "object_exists_test_type"
#define OBJECT_EXISTS_TEST_DSET_NAME       "object_exists_test_dset"

#define OBJECT_EXISTS_INVALID_PARAMS_TEST_SUBGROUP_NAME "object_exists_invalid_params_test"
#define OBJECT_EXISTS_INVALID_PARAMS_TEST_GRP_NAME      "object_exists_invalid_params_test_group"

#define OBJECT_COPY_TEST_SUBGROUP_NAME "object_copy_test"
#define OBJECT_COPY_TEST_SPACE_RANK    2
#define OBJECT_COPY_TEST_GROUP_NAME    "object_copy_test_group"
#define OBJECT_COPY_TEST_GROUP_NAME2   "object_copy_test_group_copy"
#define OBJECT_COPY_TEST_DSET_NAME     "object_copy_test_dset"
#define OBJECT_COPY_TEST_DSET_NAME2    "object_copy_test_dset_copy"
#define OBJECT_COPY_TEST_TYPE_NAME     "object_copy_test_type"
#define OBJECT_COPY_TEST_TYPE_NAME2    "object_copy_test_type_copy"

#define OBJECT_COPY_INVALID_PARAMS_TEST_SUBGROUP_NAME "object_copy_invalid_params_test"
#define OBJECT_COPY_INVALID_PARAMS_TEST_GROUP_NAME    "object_copy_invalid_params_group"
#define OBJECT_COPY_INVALID_PARAMS_TEST_GROUP_NAME2   "object_copy_invalid_params_group_copy"

#define OBJECT_VISIT_TEST_SUBGROUP_NAME "object_visit_test"
#define OBJECT_VISIT_TEST_SPACE_RANK    2
#define OBJECT_VISIT_TEST_GROUP_NAME    "object_visit_test_group"
#define OBJECT_VISIT_TEST_DSET_NAME     "object_visit_test_dset"
#define OBJECT_VISIT_TEST_TYPE_NAME     "object_visit_test_type"

#define OBJECT_VISIT_INVALID_PARAMS_TEST_SUBGROUP_NAME "object_visit_invalid_params_test"
#define OBJECT_VISIT_INVALID_PARAMS_TEST_GROUP_NAME    "object_visit_invalid_params_group"

#define OBJECT_CLOSE_TEST_SPACE_RANK 2
#define OBJECT_CLOSE_TEST_GROUP_NAME "object_close_test"
#define OBJECT_CLOSE_TEST_GRP_NAME   "object_close_test_group"
#define OBJECT_CLOSE_TEST_DSET_NAME  "object_close_test_dset"
#define OBJECT_CLOSE_TEST_TYPE_NAME  "object_close_test_type"

#define OBJ_REF_GET_TYPE_TEST_SUBGROUP_NAME "obj_ref_get_obj_type_test"
#define OBJ_REF_GET_TYPE_TEST_DSET_NAME "ref_dset"
#define OBJ_REF_GET_TYPE_TEST_TYPE_NAME "ref_dtype"
#define OBJ_REF_GET_TYPE_TEST_SPACE_RANK 2

#define OBJ_REF_DATASET_WRITE_TEST_SUBGROUP_NAME  "obj_ref_write_test"
#define OBJ_REF_DATASET_WRITE_TEST_REF_DSET_NAME  "ref_dset"
#define OBJ_REF_DATASET_WRITE_TEST_REF_TYPE_NAME  "ref_dtype"
#define OBJ_REF_DATASET_WRITE_TEST_SPACE_RANK     1
#define OBJ_REF_DATASET_WRITE_TEST_DSET_NAME      "obj_ref_dset"

#define OBJ_REF_DATASET_READ_TEST_SUBGROUP_NAME  "obj_ref_read_test"
#define OBJ_REF_DATASET_READ_TEST_REF_DSET_NAME  "ref_dset"
#define OBJ_REF_DATASET_READ_TEST_REF_TYPE_NAME  "ref_dtype"
#define OBJ_REF_DATASET_READ_TEST_SPACE_RANK     1
#define OBJ_REF_DATASET_READ_TEST_DSET_NAME      "obj_ref_dset"

#define OBJ_REF_DATASET_EMPTY_WRITE_TEST_SUBGROUP_NAME  "obj_ref_empty_write_test"
#define OBJ_REF_DATASET_EMPTY_WRITE_TEST_SPACE_RANK     1
#define OBJ_REF_DATASET_EMPTY_WRITE_TEST_DSET_NAME      "obj_ref_dset"

#endif
