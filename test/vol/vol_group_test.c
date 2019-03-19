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

#include "vol_group_test.h"

static int test_create_group_under_root(void);
static int test_create_group_under_existing_group(void);
static int test_create_many_groups(void);
static int test_create_deep_groups(void);
static int test_create_group_invalid_params(void);
static int test_create_anonymous_group(void);
static int test_create_anonymous_group_invalid_params(void);
static int test_open_nonexistent_group(void);
static int test_open_group_invalid_params(void);
static int test_close_group_invalid_id(void);
static int test_group_property_lists(void);
static int test_get_group_info(void);
static int test_get_group_info_invalid_params(void);
static int test_flush_group(void);
static int test_flush_group_invalid_params(void);
static int test_refresh_group(void);
static int test_refresh_group_invalid_params(void);
static int create_group_recursive(hid_t parent_gid, int counter);

/*
 * The array of group tests to be performed.
 */
static int (*group_tests[])(void) = {
        test_create_group_under_root,
        test_create_group_under_existing_group,
        test_create_many_groups,
        test_create_deep_groups,
        test_create_group_invalid_params,
        test_create_anonymous_group,
        test_create_anonymous_group_invalid_params,
        test_open_nonexistent_group,
        test_open_group_invalid_params,
        test_close_group_invalid_id,
        test_group_property_lists,
        test_get_group_info,
        test_get_group_info_invalid_params,
        test_flush_group,
        test_flush_group_invalid_params,
        test_refresh_group,
        test_refresh_group_invalid_params,
};

/*
 * A test to check that a group can be created under the root group.
 */
static int
test_create_group_under_root(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t parent_gid = H5I_INVALID_HID, child_gid = H5I_INVALID_HID,
          absolute_gid = H5I_INVALID_HID;

    TESTING("creation of group under the root group")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Create the group under the root group of the file */
    if ((parent_gid = H5Gcreate2(file_id, PARENT_GROUP_UNDER_ROOT_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", PARENT_GROUP_UNDER_ROOT_GNAME);
        goto error;
    }

    /* Create another group under the first group (relative pathname) */
    if ((child_gid = H5Gcreate2(parent_gid, CHILD_GROUP_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", CHILD_GROUP_GNAME);
        goto error;
    }

    /* Create a group with an absolute pathname */
    if ((absolute_gid = H5Gcreate2(file_id, GROUP_WITH_ABSOLUTE_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GROUP_WITH_ABSOLUTE_GNAME);
        goto error;
    }

    if (H5Gclose(absolute_gid) < 0)
        TEST_ERROR
    if (H5Gclose(child_gid) < 0)
        TEST_ERROR
    if (H5Gclose(parent_gid) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(absolute_gid);
        H5Gclose(child_gid);
        H5Gclose(parent_gid);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a group can be created under an existing
 * group which is not the root group.
 */
static int
test_create_group_under_existing_group(void)
{
    hid_t file_id = H5I_INVALID_HID;
    hid_t parent_group_id = H5I_INVALID_HID, child_group_id = H5I_INVALID_HID,
          grandchild_group_id = H5I_INVALID_HID;
    hid_t fapl_id = H5I_INVALID_HID;

    TESTING("creation of group under existing group using a relative path")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    /* Open the already-existing group (/group_tests) in the file as the parent */
    if ((parent_group_id = H5Gopen2(file_id, GROUP_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group\n");
        goto error;
    }

    /* Create a new group (/group_tests/child_group) under the already-existing parent Group using a relative path */
    if ((child_group_id = H5Gcreate2(parent_group_id, GROUP_CREATE_UNDER_GROUP_REL_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group using relative path: %s\n", GROUP_CREATE_UNDER_GROUP_REL_GNAME);
        goto error;
    }

    /* Create a new group (child_group/grandchild_group) under the already-existing parent Group using an absolute path */
    if ((grandchild_group_id = H5Gcreate2(parent_group_id, GROUP_CREATE_UNDER_GROUP_ABS_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group using absolute path: %s\n", GROUP_CREATE_UNDER_GROUP_ABS_GNAME);
        goto error;
    }

    if (H5Gclose(grandchild_group_id) < 0)
        TEST_ERROR
    if (H5Gclose(child_group_id) < 0)
        TEST_ERROR
    if (H5Gclose(parent_group_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(grandchild_group_id);
        H5Gclose(child_group_id);
        H5Gclose(parent_group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to create many (one million) groups
 */
static int
test_create_many_groups(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t parent_group_id = H5I_INVALID_HID, child_group_id = H5I_INVALID_HID;
    char  group_name[NAME_BUF_SIZE];
    unsigned i;

    TESTING("H5Gcreate many groups")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Create the group under the root group of the file */
    if ((parent_group_id = H5Gcreate2(file_id, MANY_GROUP_CREATIONS_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", MANY_GROUP_CREATIONS_GNAME);
        goto error;
    }

    /* Create multiple groups under the parent group */
    for(i = 0; i < GROUP_NUMB_MANY; i++) {
        sprintf(group_name, "group %02u", i);
        if ((child_group_id = H5Gcreate2(parent_group_id, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create group '%s'\n", group_name);
            goto error;
        }

        if (H5Gclose(child_group_id) < 0)
            TEST_ERROR
    }

    if (H5Gclose(parent_group_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(child_group_id);
        H5Gclose(parent_group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to create groups of the depth GROUP_DEPTH.
 */
static int
test_create_deep_groups(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t group_id = H5I_INVALID_HID;

    TESTING("H5Gcreate groups of great depths")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Create the group under the root group of the file */
    if ((group_id = H5Gcreate2(file_id, DEEP_GROUP_CREATIONS_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", DEEP_GROUP_CREATIONS_GNAME);
        goto error;
    }

    if (create_group_recursive(group_id, 1) < 0)
        TEST_ERROR

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * Recursive function to create groups of the depth GROUP_DEPTH.
 */
static int
create_group_recursive(hid_t parent_gid, int counter)
{
    hid_t child_gid = H5I_INVALID_HID;
    char  gname[NAME_BUF_SIZE];

    sprintf(gname, "%dth_child_group", counter+1);
    if ((child_gid = H5Gcreate2(parent_gid, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", gname);
        goto error;
    }

    if (counter < GROUP_DEPTH) {
        if (create_group_recursive(child_gid, counter+1) < 0)
            TEST_ERROR
    }

    if (H5Gclose(child_gid) < 0)
        TEST_ERROR

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(child_gid);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a group can't be created when H5Gcreate
 * is passed invalid parameters.
 */
static int
test_create_group_invalid_params(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t group_id = H5I_INVALID_HID;

    TESTING("H5Gcreate with invalid parameters"); HDputs("");

    TESTING_2("H5Gcreate with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    H5E_BEGIN_TRY {
        group_id = H5Gcreate2(H5I_INVALID_HID, GROUP_CREATE_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    created group with invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gcreate with an invalid group name")

    H5E_BEGIN_TRY {
        group_id = H5Gcreate2(file_id, NULL, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    created group with invalid name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        group_id = H5Gcreate2(file_id, "", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    created group with invalid name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gcreate with an invalid LCPL")

    H5E_BEGIN_TRY {
        group_id = H5Gcreate2(file_id, GROUP_CREATE_INVALID_PARAMS_GROUP_NAME, H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    created group with invalid LCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gcreate with an invalid GCPL")

    H5E_BEGIN_TRY {
        group_id = H5Gcreate2(file_id, GROUP_CREATE_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    created group with invalid GCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gcreate with an invalid GAPL")

    H5E_BEGIN_TRY {
        group_id = H5Gcreate2(file_id, GROUP_CREATE_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    created group with invalid GAPL!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an anonymous group can be created with
 * H5Gcreate_anon.
 */
static int
test_create_anonymous_group(void)
{
    hid_t file_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, new_group_id = H5I_INVALID_HID;
    hid_t fapl_id = H5I_INVALID_HID;

    TESTING("creation of anonymous group")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, GROUP_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group\n");
        goto error;
    }

    if ((new_group_id = H5Gcreate_anon(file_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create anonymous group\n");
        goto error;
    }

    if (H5Gclose(new_group_id) < 0)
        TEST_ERROR
    if (H5Gclose(container_group) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(new_group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an anonymous group can't be created
 * when H5Gcreate_anon is passed invalid parameters.
 */
static int
test_create_anonymous_group_invalid_params(void)
{
    hid_t file_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, new_group_id = H5I_INVALID_HID;
    hid_t fapl_id = H5I_INVALID_HID;

    TESTING("H5Gcreate_anon with invalid parameters"); HDputs("");

    TESTING_2("H5Gcreate_anon with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, GROUP_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        new_group_id = H5Gcreate_anon(H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (new_group_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous group with invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gcreate_anon with an invalid GCPL")

    H5E_BEGIN_TRY {
        new_group_id = H5Gcreate_anon(container_group, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (new_group_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous group with invalid GCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gcreate_anon with an invalid GAPL")

    H5E_BEGIN_TRY {
        new_group_id = H5Gcreate_anon(container_group, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (new_group_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous group with invalid GAPL!\n");
        goto error;
    }

    if (H5Gclose(container_group) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(new_group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a group which doesn't exist cannot
 * be opened.
 */
static int
test_open_nonexistent_group(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t group_id = H5I_INVALID_HID;

    TESTING("for failure when opening a nonexistent group")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        group_id = H5Gopen2(file_id, OPEN_NONEXISTENT_GROUP_TEST_GNAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    opened non-existent group!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a group can't be opened when H5Gopen
 * is passed invalid parameters.
 */
static int
test_open_group_invalid_params(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t group_id = H5I_INVALID_HID;

    TESTING("H5Gopen with invalid parameters"); HDputs("");

    TESTING_2("H5Gopen with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    H5E_BEGIN_TRY {
        group_id = H5Gopen2(H5I_INVALID_HID, GROUP_TEST_GROUP_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    opened group using an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gopen with an invalid group name")

    H5E_BEGIN_TRY {
        group_id = H5Gopen2(file_id, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    opened group using an invalid name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        group_id = H5Gopen2(file_id, "", H5P_DEFAULT);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    opened group using an invalid name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gopen with an invalid GAPL")

    H5E_BEGIN_TRY {
        group_id = H5Gopen2(file_id, GROUP_TEST_GROUP_NAME, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (group_id >= 0) {
        H5_FAILED();
        HDprintf("    opened group using an invalid GAPL!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Gclose doesn't succeed for an
 * invalid group ID.
 */
static int
test_close_group_invalid_id(void)
{
    herr_t err_ret = -1;
    hid_t  fapl_id = H5I_INVALID_HID;

    TESTING("H5Gclose with an invalid group ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        err_ret = H5Gclose(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    close a group with an invalid ID!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fapl_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a VOL connector stores and can retrieve a valid
 * copy of a GCPL used at group creation time.
 */
static int
test_group_property_lists(void)
{
    size_t dummy_prop_val = GROUP_PROPERTY_LIST_TEST_DUMMY_VAL;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID;
    hid_t  group_id1 = H5I_INVALID_HID, group_id2 = H5I_INVALID_HID;
    hid_t  gcpl_id1 = H5I_INVALID_HID, gcpl_id2 = H5I_INVALID_HID;

    TESTING("group property list operations")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, GROUP_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((gcpl_id1 = H5Pcreate(H5P_GROUP_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create GCPL\n");
        goto error;
    }

    if (H5Pset_local_heap_size_hint(gcpl_id1, dummy_prop_val) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set property on GCPL\n");
        goto error;
    }

    /* Create the group in the file */
    if ((group_id1 = H5Gcreate2(container_group, GROUP_PROPERTY_LIST_TEST_GROUP_NAME1, H5P_DEFAULT, gcpl_id1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group\n");
        goto error;
    }

    /* Create the second group using H5P_DEFAULT for the GCPL */
    if ((group_id2 = H5Gcreate2(container_group, GROUP_PROPERTY_LIST_TEST_GROUP_NAME2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group\n");
        goto error;
    }

    if (H5Pclose(gcpl_id1) < 0)
        TEST_ERROR

    /* Try to retrieve copies of the two property lists, one which has the property set and one which does not */
    if ((gcpl_id1 = H5Gget_create_plist(group_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get GCPL\n");
        goto error;
    }

    if ((gcpl_id2 = H5Gget_create_plist(group_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get GCPL\n");
        goto error;
    }

    /* Ensure that property list 1 has the property set and property list 2 does not */
    dummy_prop_val = 0;

    if (H5Pget_local_heap_size_hint(gcpl_id1, &dummy_prop_val) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve GCPL property value\n");
        goto error;
    }

    if (dummy_prop_val != GROUP_PROPERTY_LIST_TEST_DUMMY_VAL) {
        H5_FAILED();
        HDprintf("    1. retrieved GCPL property value '%llu' did not match expected value '%llu'\n",
                (unsigned long long) dummy_prop_val, (unsigned long long) GROUP_PROPERTY_LIST_TEST_DUMMY_VAL);
        goto error;
    }

    dummy_prop_val = 0;

    if (H5Pget_local_heap_size_hint(gcpl_id2, &dummy_prop_val) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve GCPL property value\n");
        goto error;
    }

    if (dummy_prop_val == GROUP_PROPERTY_LIST_TEST_DUMMY_VAL) {
        H5_FAILED();
        HDprintf("    1. retrieved GCPL property value '%llu' matched control value '%llu' when it shouldn't have\n",
                (unsigned long long) dummy_prop_val, (unsigned long long) GROUP_PROPERTY_LIST_TEST_DUMMY_VAL);
        goto error;
    }

    if (H5Pclose(gcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(gcpl_id2) < 0)
        TEST_ERROR

    /* Now see if we can still retrieve copies of the property lists upon opening
     * (instead of creating) a group. If they were reconstructed properly upon file
     * open, the creation property lists should also have the same test values
     * as set before.
     */
    if (H5Gclose(group_id1) < 0)
        TEST_ERROR
    if (H5Gclose(group_id2) < 0)
        TEST_ERROR

    if ((group_id1 = H5Gopen2(container_group, GROUP_PROPERTY_LIST_TEST_GROUP_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group\n");
        goto error;
    }

    if ((group_id2 = H5Gopen2(container_group, GROUP_PROPERTY_LIST_TEST_GROUP_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group\n");
        goto error;
    }

    if ((gcpl_id1 = H5Gget_create_plist(group_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((gcpl_id2 = H5Gget_create_plist(group_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    /* Re-check the property values */
    dummy_prop_val = 0;

    if (H5Pget_local_heap_size_hint(gcpl_id1, &dummy_prop_val) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve GCPL property value\n");
        goto error;
    }

    if (dummy_prop_val != GROUP_PROPERTY_LIST_TEST_DUMMY_VAL) {
        H5_FAILED();
        HDprintf("    2. retrieved GCPL property value '%llu' did not match expected value '%llu'\n",
                (unsigned long long) dummy_prop_val, (unsigned long long) GROUP_PROPERTY_LIST_TEST_DUMMY_VAL);
        goto error;
    }

    dummy_prop_val = 0;

    if (H5Pget_local_heap_size_hint(gcpl_id2, &dummy_prop_val) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve GCPL property value\n");
        goto error;
    }

    if (dummy_prop_val == GROUP_PROPERTY_LIST_TEST_DUMMY_VAL) {
        H5_FAILED();
        HDprintf("    2. retrieved GCPL property value '%llu' matched control value '%llu' when it shouldn't have\n",
                (unsigned long long) dummy_prop_val, (unsigned long long) GROUP_PROPERTY_LIST_TEST_DUMMY_VAL);
        goto error;
    }

    if (H5Pclose(gcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(gcpl_id2) < 0)
        TEST_ERROR
    if (H5Gclose(group_id1) < 0)
        TEST_ERROR
    if (H5Gclose(group_id2) < 0)
        TEST_ERROR
    if (H5Gclose(container_group) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(gcpl_id1);
        H5Pclose(gcpl_id2);
        H5Gclose(group_id1);
        H5Gclose(group_id2);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for the functionality of H5Gget_info.
 */
static int
test_get_group_info(void)
{
    H5G_info_t 	group_info;
    hid_t      	file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t      	parent_group_id = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    char        group_name[NAME_BUF_SIZE];
    unsigned    i;

    TESTING("retrieval of group info"); HDputs("");

    TESTING_2("retrieval of group info with H5Gget_info")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((parent_group_id = H5Gcreate2(file_id, GROUP_FOR_INFO, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", group_name);
        goto error;
    }

    /* Create multiple groups under the parent group */
    for(i = 0; i < GROUP_NUMB; i++) {
        sprintf(group_name, "group %02u", i);
        if ((group_id = H5Gcreate2(parent_group_id, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create group '%s'\n", group_name);
            goto error;
        }

        if (H5Gclose(group_id) < 0)
            TEST_ERROR
    }

    /* Retrieve information about the parent group */
    if (H5Gget_info(parent_group_id, &group_info) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get group info\n");
        goto error;
    }

    if (group_info.nlinks != GROUP_NUMB) {
        H5_FAILED();
        HDprintf("    the number of groups %llu is different from the expected number %u\n", group_info.nlinks, (unsigned int)GROUP_NUMB);
        goto error;
    }

    if (H5Gclose(parent_group_id) < 0)
        TEST_ERROR

    PASSED();

    TESTING_2("retrieval of group info with H5Gget_info_by_name")

    /* Retrieve information about the parent group */
    if (H5Gget_info_by_name(file_id, GROUP_FOR_INFO, &group_info, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get group info by name\n");
        goto error;
    }

    if (group_info.nlinks != GROUP_NUMB) {
        H5_FAILED();
        HDprintf("    the number of groups %llu is different from the expected number %u\n", group_info.nlinks, (unsigned int)GROUP_NUMB);
        goto error;
    }

    PASSED();

    TESTING_2("retrieval of group info with H5Gget_info_by_idx")

    /* Retrieve information about the first group under the parent group */
    if (H5Gget_info_by_idx(file_id, GROUP_FOR_INFO, H5_INDEX_NAME, H5_ITER_INC, 0, &group_info, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get group info by index\n");
        goto error;
    }

    if (group_info.nlinks != 0) {
        H5_FAILED();
        HDprintf("    the number of groups %llu is different from the expected number 0\n", group_info.nlinks);
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(parent_group_id);
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a group's info can't be retrieved when
 * H5Gget_info(_by_name/_by_idx) is passed invalid parameters.
 */
static int
test_get_group_info_invalid_params(void)
{
    H5G_info_t group_info;
    herr_t     err_ret = -1;
    hid_t      file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Gget_info with invalid parameters"); HDputs("");

    TESTING_2("H5Gget_info with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info(H5I_INVALID_HID, &group_info);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info with an invalid group info pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info(file_id, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info with invalid group info pointer!\n");
        goto error;
    }

    PASSED();

    TESTING("H5Gget_info_by_name with invalid parameters"); HDputs("");

    TESTING_2("H5Gget_info_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_name(H5I_INVALID_HID, ".", &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_name with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_name with an invalid group name")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_name(file_id, NULL, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_name with an invalid name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_name(file_id, "", &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_name with an invalid name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_name with an invalid group info pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_name(file_id, ".", NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_name with an invalid group info pointer!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_name with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_name(file_id, ".", &group_info, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_name with an invalid LAPL!\n");
        goto error;
    }

    PASSED();

    TESTING("H5Gget_info_by_idx with invalid parameters"); HDputs("");

    TESTING_2("H5Gget_info_by_idx with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(H5I_INVALID_HID, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_idx with an invalid group name")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, NULL, H5_INDEX_NAME, H5_ITER_INC, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid group name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, "", H5_INDEX_NAME, H5_ITER_INC, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid group name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_idx with an invalid index type")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, ".", H5_INDEX_UNKNOWN, H5_ITER_INC, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid index type!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, ".", H5_INDEX_N, H5_ITER_INC, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid index type!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_idx with an invalid iteration order")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, ".", H5_INDEX_NAME, H5_ITER_UNKNOWN, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid iteration order!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, ".", H5_INDEX_NAME, H5_ITER_N, 0, &group_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid iteration order!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_idx with an invalid group info pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid group info pointer!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Gget_info_by_idx with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Gget_info_by_idx(file_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &group_info, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved info of group using H5Gget_info_by_idx with an invalid LAPL!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Gflush.
 */
static int
test_flush_group(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t group_id = H5I_INVALID_HID;

    TESTING("H5Gflush")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Create the group under the root group of the file */
    if ((group_id = H5Gcreate2(file_id, GROUP_FLUSH_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GROUP_FLUSH_GNAME);
        goto error;
    }

    /* Flush the group */
    if(H5Gflush(group_id) < 0) {
        H5_FAILED();
        HDprintf("    couldn't flush the group '%s'\n", GROUP_FLUSH_GNAME);
        goto error;
    }

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Gflush fails when it
 * is passed invalid parameters.
 */
static int
test_flush_group_invalid_params(void)
{
    herr_t status;

    TESTING("H5Gflush with invalid parameters")

    H5E_BEGIN_TRY {
        status = H5Gflush(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (status >= 0) {
        H5_FAILED();
        HDprintf("    flushed group with invalid ID!\n");
        goto error;
    }

    PASSED();

    return 0;

error:
    return 1;
}

/*
 * A test for H5Grefresh.
 */
static int
test_refresh_group(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t group_id = H5I_INVALID_HID;

    TESTING("H5Grefresh")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Create the group under the root group of the file */
    if ((group_id = H5Gcreate2(file_id, GROUP_REFRESH_GNAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GROUP_REFRESH_GNAME);
        goto error;
    }

    /* Refresh the group */
    if(H5Grefresh(group_id) < 0) {
        H5_FAILED();
        HDprintf("    couldn't refresh the group '%s'\n", GROUP_REFRESH_GNAME);
        goto error;
    }

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Grefresh fails when it
 * is passed invalid parameters.
 */
static int
test_refresh_group_invalid_params(void)
{
    herr_t status;

    TESTING("H5Grefresh with invalid parameters")

    H5E_BEGIN_TRY {
        status = H5Grefresh(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (status >= 0) {
        H5_FAILED();
        HDprintf("    refreshed group with invalid ID!\n");
        goto error;
    }

    PASSED();

    return 0;

error:
    return 1;
}

int
vol_group_test(void)
{
    size_t i;
    int    nerrors;

    HDprintf("**********************************************\n");
    HDprintf("*                                            *\n");
    HDprintf("*              VOL Group Tests               *\n");
    HDprintf("*                                            *\n");
    HDprintf("**********************************************\n\n");

    for (i = 0, nerrors = 0; i < ARRAY_LENGTH(group_tests); i++) {
        nerrors += (*group_tests[i])() ? 1 : 0;
    }

    HDprintf("\n");

done:
    return nerrors;
}
