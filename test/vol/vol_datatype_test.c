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

#include "vol_datatype_test.h"

static int test_create_committed_datatype(void);
static int test_create_committed_datatype_invalid_params(void);
static int test_create_anonymous_committed_datatype(void);
static int test_create_anonymous_committed_datatype_invalid_params(void);
static int test_open_committed_datatype(void);
static int test_open_committed_datatype_invalid_params(void);
static int test_close_committed_datatype_invalid_id(void);
static int test_datatype_property_lists(void);
static int test_create_dataset_with_committed_type(void);
static int test_create_attribute_with_committed_type(void);
static int test_delete_committed_type(void);
static int test_flush_committed_datatype(void);
static int test_flush_committed_datatype_invalid_params(void);
static int test_refresh_committed_datatype(void);
static int test_refresh_committed_datatype_invalid_params(void);

/*
 * The array of datatype tests to be performed.
 */
static int (*datatype_tests[])(void) = {
        test_create_committed_datatype,
        test_create_committed_datatype_invalid_params,
        test_create_anonymous_committed_datatype,
        test_create_anonymous_committed_datatype_invalid_params,
        test_open_committed_datatype,
        test_open_committed_datatype_invalid_params,
        test_close_committed_datatype_invalid_id,
        test_datatype_property_lists,
        test_create_dataset_with_committed_type,
        test_create_attribute_with_committed_type,
        test_delete_committed_type,
        test_flush_committed_datatype,
        test_flush_committed_datatype_invalid_params,
        test_refresh_committed_datatype,
        test_refresh_committed_datatype_invalid_params,
};

/*
 * A test to check that a committed datatype can be created.
 */
static int
test_create_committed_datatype(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t type_id = H5I_INVALID_HID;

    TESTING("creation of a committed datatype")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_CREATE_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_CREATE_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype to commit\n");
        goto error;
    }

    if (H5Tcommit2(group_id, DATATYPE_CREATE_TEST_TYPE_NAME, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATATYPE_CREATE_TEST_TYPE_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a committed datatype can't be
 * created when H5Tcommit2 is passed invalid parameters.
 */
static int
test_create_committed_datatype_invalid_params(void)
{
    herr_t err_ret = -1;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t  type_id = H5I_INVALID_HID;

    TESTING("H5Tcommit2 with invalid parameters"); HDputs("");

    TESTING_2("H5Tcommit2 with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_CREATE_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_CREATE_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype to commit\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(H5I_INVALID_HID, DATATYPE_CREATE_INVALID_PARAMS_TEST_TYPE_NAME, type_id,
                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit2 with an invalid datatype name");

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(group_id, NULL, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid datatype name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(group_id, "", type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid datatype name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit2 with an invalid datatype ID");

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(group_id, DATATYPE_CREATE_INVALID_PARAMS_TEST_TYPE_NAME, H5I_INVALID_HID,
                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid datatype ID!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit2 with an invalid LCPL");

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(group_id, DATATYPE_CREATE_INVALID_PARAMS_TEST_TYPE_NAME, type_id,
                H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid LCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit2 with an invalid TCPL");

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(group_id, DATATYPE_CREATE_INVALID_PARAMS_TEST_TYPE_NAME, type_id,
                H5P_DEFAULT, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid TCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit2 with an invalid TAPL");

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit2(group_id, DATATYPE_CREATE_INVALID_PARAMS_TEST_TYPE_NAME, type_id,
                H5P_DEFAULT, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit2 succeeded with an invalid TAPL!\n");
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an anonymous committed datatype
 * can be created with H5Tcommit_anon.
 */
static int
test_create_anonymous_committed_datatype(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t type_id = H5I_INVALID_HID;

    TESTING("creation of anonymous committed datatype")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_CREATE_ANONYMOUS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_CREATE_ANONYMOUS_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if (H5Tcommit_anon(group_id, type_id, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit anonymous datatype\n");
        goto error;
    }

    if (H5Olink(type_id, group_id, DATATYPE_CREATE_ANONYMOUS_TYPE_NAME, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't link anonymous datatype into file structure\n");
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a committed datatype can't be
 * created when H5Tcommit_anon is passed invalid parameters.
 */
static int
test_create_anonymous_committed_datatype_invalid_params(void)
{
    herr_t err_ret = -1;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t  type_id = H5I_INVALID_HID;

    TESTING("H5Tcommit_anon with invalid parameters"); HDputs("");

    TESTING_2("H5Tcommit_anon with an invalid loc_id");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_CREATE_ANONYMOUS_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_CREATE_ANONYMOUS_INVALID_PARAMS_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit_anon(H5I_INVALID_HID, type_id, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit_anon succeeded with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit_anon with an invalid datatype ID")

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit_anon(group_id, H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit_anon succeeded with an invalid datatype ID!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit_anon with an invalid TCPL")

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit_anon(group_id, type_id, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit_anon succeeded with an invalid TCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Tcommit_anon with an invalid TAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Tcommit_anon(group_id, type_id, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tcommit_anon succeeded with an invalid TAPL!\n");
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a committed datatype
 * can be opened using H5Topen2.
 */
static int
test_open_committed_datatype(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t type_id = H5I_INVALID_HID;

    TESTING("H5Topen2")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_OPEN_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_OPEN_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype to commit\n");
        goto error;
    }

    if (H5Tcommit2(group_id, DATATYPE_OPEN_TEST_TYPE_NAME, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATATYPE_OPEN_TEST_TYPE_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR

    if ((type_id = H5Topen2(group_id, DATATYPE_OPEN_TEST_TYPE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open committed datatype '%s'\n", DATATYPE_OPEN_TEST_TYPE_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a committed datatype can't
 * be opened when H5Topen2 is passed invalid parameters.
 */
static int
test_open_committed_datatype_invalid_params(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t type_id = H5I_INVALID_HID;

    TESTING("H5Topen2 with invalid parameters"); HDputs("");

    TESTING_2("H5Topen2 with an invalid location ID");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype to commit\n");
        goto error;
    }

    if (H5Tcommit2(group_id, DATATYPE_OPEN_INVALID_PARAMS_TEST_TYPE_NAME, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATATYPE_OPEN_INVALID_PARAMS_TEST_TYPE_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        type_id = H5Topen2(H5I_INVALID_HID, DATATYPE_OPEN_INVALID_PARAMS_TEST_TYPE_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (type_id >= 0) {
        H5_FAILED();
        HDprintf("    opened committed datatype with an invalid location ID!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Topen2 with an invalid datatype name")

    H5E_BEGIN_TRY {
        type_id = H5Topen2(group_id, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (type_id >= 0) {
        H5_FAILED();
        HDprintf("    opened committed datatype with an invalid datatype name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        type_id = H5Topen2(group_id, "", H5P_DEFAULT);
    } H5E_END_TRY;

    if (type_id >= 0) {
        H5_FAILED();
        HDprintf("    opened committed datatype with an invalid datatype name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Topen2 with an invalid TAPL")

    H5E_BEGIN_TRY {
        type_id = H5Topen2(group_id, DATATYPE_OPEN_INVALID_PARAMS_TEST_TYPE_NAME, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (type_id >= 0) {
        H5_FAILED();
        HDprintf("    opened committed datatype with an invalid TAPL!\n");
        goto error;
    }

    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Tclose fails when
 * it is passed an invalid datatype ID.
 */
static int
test_close_committed_datatype_invalid_id(void)
{
    herr_t err_ret = -1;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Tclose with an invalid committed datatype ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Tclose(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Tclose succeeded with an invalid committed datatype ID!\n");
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
 * A test to check that a VOL connector stores and can
 * retrieve a valid copy of a TCPL used during committed
 * datatype creation time.
 */
static int
test_datatype_property_lists(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t type_id1 = H5I_INVALID_HID, type_id2 = H5I_INVALID_HID;
    hid_t tcpl_id1 = H5I_INVALID_HID, tcpl_id2 = H5I_INVALID_HID;

    TESTING("datatype property list operations")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_PROPERTY_LIST_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATATYPE_PROPERTY_LIST_TEST_SUBGROUP_NAME);
        goto error;
    }

    if ((type_id1 = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if ((type_id2 = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if ((tcpl_id1 = H5Pcreate(H5P_DATATYPE_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create TCPL\n");
        goto error;
    }

    /* Currently no TCPL routines are defined */

    if (H5Tcommit2(group_id, DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME1, type_id1, H5P_DEFAULT, tcpl_id1, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME1);
        goto error;
    }

    if (H5Tcommit2(group_id, DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME2, type_id2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0 ) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME2);
        goto error;
    }

    if (H5Pclose(tcpl_id1) < 0)
        TEST_ERROR

    /* Try to receive copies for the two property lists */
    if ((tcpl_id1 = H5Tget_create_plist(type_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((tcpl_id2 = H5Tget_create_plist(type_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    /* Now close the property lists and datatypes and see if we can still retieve copies of
     * the property lists upon opening (instead of creating) a datatype
     */
    if (H5Pclose(tcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(tcpl_id2) < 0)
        TEST_ERROR
    if (H5Tclose(type_id1) < 0)
        TEST_ERROR
    if (H5Tclose(type_id2) < 0)
        TEST_ERROR

    if ((type_id1 = H5Topen2(group_id, DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open datatype '%s'\n", DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME1);
        goto error;
    }

    if ((type_id2 = H5Topen2(group_id, DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open datatype '%s'\n", DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME2);
        goto error;
    }

    if ((tcpl_id1 = H5Tget_create_plist(type_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((tcpl_id2 = H5Tget_create_plist(type_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if (H5Pclose(tcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(tcpl_id2) < 0)
        TEST_ERROR
    if (H5Tclose(type_id1) < 0)
        TEST_ERROR
    if (H5Tclose(type_id2) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Pclose(tcpl_id1);
        H5Pclose(tcpl_id2);
        H5Tclose(type_id1);
        H5Tclose(type_id2);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a dataset can be created using
 * a committed datatype.
 */
static int
test_create_dataset_with_committed_type(void)
{
    hsize_t dims[DATASET_CREATE_WITH_DATATYPE_TEST_DATASET_DIMS];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   type_id = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation with a committed datatype")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_WITH_DATATYPE_TEST_GROUP_NAME,
            H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_CREATE_WITH_DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if (H5Tcommit2(group_id, DATASET_CREATE_WITH_DATATYPE_TEST_TYPE_NAME, type_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATASET_CREATE_WITH_DATATYPE_TEST_TYPE_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Gclose(container_group) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gopen2(container_group, DATASET_CREATE_WITH_DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_CREATE_WITH_DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = H5Topen2(group_id, DATASET_CREATE_WITH_DATATYPE_TEST_TYPE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open committed datatype '%s'\n", DATASET_CREATE_WITH_DATATYPE_TEST_TYPE_NAME);
        goto error;
    }

    for (i = 0; i < DATATYPE_CREATE_TEST_DATASET_DIMS; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATATYPE_CREATE_TEST_DATASET_DIMS, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_CREATE_WITH_DATATYPE_TEST_DSET_NAME, type_id, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s' using committed datatype\n", DATASET_CREATE_WITH_DATATYPE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_CREATE_WITH_DATATYPE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_CREATE_WITH_DATATYPE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Sclose(fspace_id);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can be created
 * using a committed datatype.
 */
static int
test_create_attribute_with_committed_type(void)
{
    hsize_t dims[ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   type_id = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("attribute creation with a committed datatype")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_GROUP_NAME,
            H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if (H5Tcommit2(group_id, ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_DTYPE_NAME, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_DTYPE_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR

    if ((type_id = H5Topen2(group_id, ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_DTYPE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open committed datatype '%s'\n", ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_DTYPE_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME, type_id,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute '%s'\n", ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME);
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute '%s' exists\n", ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME);
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute '%s'\n", ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME);
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Sclose(space_id);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a committed datatype can
 * be deleted.
 */
static int
test_delete_committed_type(void)
{
    htri_t type_exists;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t  type_id = H5I_INVALID_HID;

    TESTING("committed datatype deletion")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATATYPE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATATYPE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATATYPE_DELETE_TEST_GROUP_NAME,
            H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATATYPE_DELETE_TEST_GROUP_NAME);
        goto error;
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if (H5Tcommit2(group_id, DATATYPE_DELETE_TEST_DTYPE_NAME, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype '%s'\n", DATATYPE_DELETE_TEST_DTYPE_NAME);
        goto error;
    }

    if ((type_exists = H5Lexists(group_id, DATATYPE_DELETE_TEST_DTYPE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if datatype '%s' exists\n", DATATYPE_DELETE_TEST_DTYPE_NAME);
        goto error;
    }

    if (!type_exists) {
        H5_FAILED();
        HDprintf("    datatype didn't exist\n");
        goto error;
    }

    if (H5Ldelete(group_id, DATATYPE_DELETE_TEST_DTYPE_NAME, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't delete datatype '%s'\n", DATATYPE_DELETE_TEST_DTYPE_NAME);
        goto error;
    }

    if ((type_exists = H5Lexists(group_id, DATATYPE_DELETE_TEST_DTYPE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if datatype '%s' exists\n", DATATYPE_DELETE_TEST_DTYPE_NAME);
        goto error;
    }

    if (type_exists) {
        H5_FAILED();
        HDprintf("    datatype exists\n");
        goto error;
    }

    if (H5Tclose(type_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id) < 0)
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
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

static int
test_flush_committed_datatype(void)
{
    TESTING("H5Tflush")

    SKIPPED();

    return 0;
}

static int
test_flush_committed_datatype_invalid_params(void)
{
    TESTING("H5Tflush with invalid parameters")

    SKIPPED();

    return 0;
}

static int
test_refresh_committed_datatype(void)
{
    TESTING("H5Trefresh")

    SKIPPED();

    return 0;
}

static int
test_refresh_committed_datatype_invalid_params(void)
{
    TESTING("H5Trefresh with invalid parameters")

    SKIPPED();

    return 0;
}

int
vol_datatype_test(void)
{
    size_t i;
    int    nerrors;

    HDprintf("**********************************************\n");
    HDprintf("*                                            *\n");
    HDprintf("*             VOL Datatype Tests             *\n");
    HDprintf("*                                            *\n");
    HDprintf("**********************************************\n\n");

    for (i = 0, nerrors = 0; i < ARRAY_LENGTH(datatype_tests); i++) {
        nerrors += (*datatype_tests[i])() ? 1 : 0;
    }

    HDprintf("\n");

done:
    return nerrors;
}
