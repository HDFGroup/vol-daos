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

#ifndef VOL_FILE_TEST_H
#define VOL_FILE_TEST_H

#include "vol_test.h"

int vol_file_test(void);

/*********************************************
 *                                           *
 *      VOL connector File test defines      *
 *                                           *
 *********************************************/

#define FILE_CREATE_TEST_FILENAME 		"test_file.h5"

#define FILE_CREATE_INVALID_PARAMS_FILE_NAME 	"invalid_params_file.h5"

#define FILE_CREATE_EXCL_FILE_NAME 		"excl_flag_file.h5"

#define NONEXISTENT_FILENAME 			"nonexistent_file.h5"

#define FILE_PERMISSION_FILENAME		"file_permission.h5"
#define DSET_NAME 				"Dataset"
#define DSET2_NAME 				"Dataset2"

#define FILE_FLUSH_FILENAME			"flush_file.h5"

#define FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL 	65536
#define FILE_PROPERTY_LIST_TEST_FNAME1        	"property_list_test_file1.h5"
#define FILE_PROPERTY_LIST_TEST_FNAME2        	"property_list_test_file2.h5"

#define FILE_INTENT_TEST_FILENAME    		"intent_test_file.h5"

#define GET_OBJ_COUNT_FILENAME			"file_obj_count.h5"
#define GRP_NAME 				"/group"

#define FILE_MOUNT_FILENAME			"file_mount.h5"

#define FILESPACE_INFO_FILENAME			"filespace_info.h5"
#define FSP_SIZE512         			(hsize_t)512

#define FILE_GET_ID_TEST_FILENAME 		"test_file_id.h5"

#define FILE_CLOSE_DEGREE_FILENAME 		"test_close_degree.h5"

#define GET_FREE_SECTIONS_FILENAME		"test_free_sections.h5"

#define FILE_SIZE_FILENAME			"file_size.h5"
#define KB              			1024U

#define FILE_INFO_FILENAME			"file_info.h5"

#define DOUBLE_GROUP_OPEN_FILENAME		"double_group_open.h5"

#endif
