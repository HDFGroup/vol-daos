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

#ifndef VOL_GROUP_TEST_H
#define VOL_GROUP_TEST_H

#include "vol_test.h"

int vol_group_test(void);

/**********************************************
 *                                            *
 *      VOL connector Group test defines      *
 *                                            *
 **********************************************/

#define PARENT_GROUP_UNDER_ROOT_GNAME "/parent_group"
#define CHILD_GROUP_GNAME "child_group"
#define GROUP_WITH_ABSOLUTE_GNAME "/parent_group/child_group/group3"

#define GROUP_CREATE_UNDER_GROUP_REL_GNAME "child_group"
#define GROUP_CREATE_UNDER_GROUP_ABS_GNAME "child_group/grandchild_group"

#define GROUP_CREATE_INVALID_PARAMS_GROUP_NAME "/invalid_params_group"

#define GROUP_CREATE_ANONYMOUS_GROUP_NAME "anon_group"

#define OPEN_NONEXISTENT_GROUP_TEST_GNAME "/nonexistent_group"

#define GROUP_PROPERTY_LIST_TEST_GROUP_NAME1 "property_list_test_group1"
#define GROUP_PROPERTY_LIST_TEST_GROUP_NAME2 "property_list_test_group2"
#define GROUP_PROPERTY_LIST_TEST_DUMMY_VAL   100

#define GROUP_FOR_INFO "group_info"

#define GROUP_FLUSH_GNAME "group_for_flush"

#define GROUP_REFRESH_GNAME "group_for_refresh"

#define NAME_BUF_SIZE 64
#define GROUP_NUMB 16

#define MANY_GROUP_CREATIONS_GNAME "home_for_many_groups"
#define GROUP_NUMB_MANY	10000u

#define DEEP_GROUP_CREATIONS_GNAME "home_for_deep_groups"
#define GROUP_DEPTH 100u

#endif
