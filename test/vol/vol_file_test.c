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

#include "vol_file_test.h"

static int test_create_file(void);
static int test_create_file_invalid_params(void);
static int test_create_file_excl(void);
static int test_open_file(void);
static int test_open_file_invalid_params(void);
static int test_open_nonexistent_file(void);
static int test_file_permission(void);
static int test_reopen_file(void);
static int test_close_file_invalid_id(void);
static int test_flush_file(void);
static int test_file_is_accessible(void);
static int test_file_property_lists(void);
static int test_get_file_intent(void);
static int test_get_file_obj_count(void);
static int test_file_mounts(void);
static int test_get_file_name(void);
#if 0 /* for native VOL connector test only */
static int test_filespace_info(void);
static int test_get_file_id(void);
static int test_file_close_degree(void);
static int test_get_file_free_sections(void);
static int test_get_file_size(void);
static int test_get_file_image(void);
static int test_get_file_info(void);
static int test_get_file_vfd_handle(void);
static int test_double_group_open(void);
#endif /* for native VOL connector test only */

/*
 * The array of file tests to be performed.
 */
static int (*file_tests[])(void) = {
        test_create_file,
        test_create_file_invalid_params,
        test_create_file_excl,
        test_open_file,
        test_open_file_invalid_params,
        test_open_nonexistent_file,
        test_file_permission,
        test_reopen_file,
        test_close_file_invalid_id,
        test_flush_file,
        test_file_is_accessible,
        test_file_property_lists,
        test_get_file_intent,
        test_get_file_obj_count,
        test_file_mounts,
        test_get_file_name,
#if 0 /* for native VOL connector test only */
        test_filespace_info,
        test_get_file_id,
        test_file_close_degree,
        test_get_file_free_sections,
        test_get_file_size,
        test_get_file_image,
        test_get_file_info,
        test_get_file_vfd_handle,
        test_double_group_open
#endif /* for native VOL connector test only */
};

/*
 * Tests that a file can be created with the VOL connector.
 */
static int
test_create_file(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Fcreate");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_CREATE_TEST_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_CREATE_TEST_FILENAME);
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
 * Tests that a file can't be created when H5Fcreate is passed
 * invalid parameters.
 */
static int
test_create_file_invalid_params(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Fcreate with invalid parameters"); HDputs("");

    TESTING_2("H5Fcreate with invalid file name");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        file_id = H5Fcreate(NULL, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was created with an invalid name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        file_id = H5Fcreate("", H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was created with an invalid name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Fcreate with invalid flags");

    H5E_BEGIN_TRY {
        file_id = H5Fcreate(FILE_CREATE_INVALID_PARAMS_FILE_NAME, H5F_ACC_RDWR, H5P_DEFAULT, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was created with invalid flag H5F_ACC_RDWR!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        file_id = H5Fcreate(FILE_CREATE_INVALID_PARAMS_FILE_NAME, H5F_ACC_CREAT, H5P_DEFAULT, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was created with invalid flag H5F_ACC_CREAT!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        file_id = H5Fcreate(FILE_CREATE_INVALID_PARAMS_FILE_NAME, H5F_ACC_SWMR_READ, H5P_DEFAULT, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was created with invalid flag H5F_ACC_SWMR_READ!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Fcreate with invalid FCPL");

    H5E_BEGIN_TRY {
        file_id = H5Fcreate(FILE_CREATE_INVALID_PARAMS_FILE_NAME, H5F_ACC_TRUNC, H5I_INVALID_HID, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was created with invalid FCPL!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
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
 * Tests that file creation will fail when a file is created
 * using the H5F_ACC_EXCL flag while the file already exists.
 */
static int
test_create_file_excl(void)
{
    hid_t file_id = H5I_INVALID_HID, file_id2 = H5I_INVALID_HID;
    hid_t fapl_id = H5I_INVALID_HID;

    TESTING("H5Fcreate with H5F_ACC_EXCL/H5F_ACC_TRUNC flag");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_CREATE_EXCL_FILE_NAME, H5F_ACC_EXCL, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create first file\n");
        goto error;
    }

    /* Close the file */
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    /* Try again with H5F_ACC_EXCL. This should fail because the file already
     * exists on disk from the previous steps.
     */
    H5E_BEGIN_TRY {
        file_id = H5Fcreate(FILE_CREATE_EXCL_FILE_NAME, H5F_ACC_EXCL, H5P_DEFAULT, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    created already existing file using H5F_ACC_EXCL flag!\n");
        goto error;
    }

    /* Test creating with H5F_ACC_TRUNC. This will truncate the existing file on disk. */
    if((file_id = H5Fcreate(FILE_CREATE_EXCL_FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't truncate the existing file\n");
        goto error;
    }

    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fapl_id);
        H5Fclose(file_id);
        H5Fclose(file_id2);
    } H5E_END_TRY;

    return 1;
}

/*
 * Tests that a file can be opened with the VOL connector.
 */
static int
test_open_file(void)
{
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Fopen");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    unable to open file '%s' in read-only mode\n", vol_test_filename);
        goto error;
    }

    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    unable to open file '%s' in read-write mode\n", vol_test_filename);
        goto error;
    }

    /*
     * XXX: SWMR open flags
     */

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
 * Tests that a file can't be opened when H5Fopen is given
 * invalid parameters.
 */
static int
test_open_file_invalid_params(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Fopen with invalid parameters"); HDputs("");

    TESTING_2("H5Fopen with invalid file name");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        file_id = H5Fopen(NULL, H5F_ACC_RDWR, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was opened with an invalid name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        file_id = H5Fopen("", H5F_ACC_RDWR, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was opened with an invalid name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Fopen with invalid flags");

    H5E_BEGIN_TRY {
        file_id = H5Fopen(vol_test_filename, H5F_ACC_TRUNC, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was opened with invalid flag H5F_ACC_TRUNC!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        file_id = H5Fopen(vol_test_filename, H5F_ACC_EXCL, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    file was opened with invalid flag H5F_ACC_EXCL!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
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
 * A test to ensure that opening a file which doesn't exist will fail.
 */
static int
test_open_nonexistent_file(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    char  test_filename[VOL_TEST_FILENAME_MAX_LENGTH];

    TESTING("for failure when opening a non-existent file")

    HDsnprintf(test_filename, VOL_TEST_FILENAME_MAX_LENGTH, "%s", NONEXISTENT_FILENAME);

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        file_id = H5Fopen(test_filename, H5F_ACC_RDWR, fapl_id);
    } H5E_END_TRY;

    if (file_id >= 0) {
        H5_FAILED();
        HDprintf("    non-existent file was opened!\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
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
 * Tests that a file can be opened read-only or read-write
 * and things are handled appropriately.
 */
static int
test_file_permission(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t dset_id = H5I_INVALID_HID, dspace_id = H5I_INVALID_HID;

    TESTING("for file permission");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_PERMISSION_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_CREATE_TEST_FILENAME);
        goto error;
    }

    if ((dspace_id = H5Screate(H5S_SCALAR)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create data space\n");
        goto error;
    }

    if ((dset_id = H5Dcreate2(file_id, DSET_NAME, H5T_STD_U32LE, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create data set: %s\n", DSET_NAME);
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    /* Open the file (with read-only permission) */
    if ((file_id = H5Fopen(FILE_PERMISSION_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    /* Create a dataset with the read-only file handle (should fail) */
    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(file_id, DSET2_NAME, H5T_STD_U32LE, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    a dataset was created in a read-only file!\n");
        goto error;
    }

    if (H5Sclose(dspace_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Sclose(dspace_id);
        H5Dclose(dset_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a file can be re-opened with H5Freopen.
 */
static int
test_reopen_file(void)
{
    hid_t file_id = H5I_INVALID_HID, file_id2 = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("re-open of a file with H5Freopen")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((file_id2 = H5Freopen(file_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't re-open file\n");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id2) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fapl_id);
        H5Fclose(file_id);
        H5Fclose(file_id2);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Fclose doesn't succeed for an
 * invalid file ID */
static int
test_close_file_invalid_id(void)
{
    herr_t err_ret = -1;
    hid_t  fapl_id = H5I_INVALID_HID;

    TESTING("H5Fclose with an invalid ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        err_ret = H5Fclose(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    closed an invalid file ID!\n");
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
 * A test to check that a file can be flushed using H5Fflush.
 */
static int
test_flush_file(void)
{
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    dspace_id = H5I_INVALID_HID, dset_id = H5I_INVALID_HID;
    char     dset_name[32];
    unsigned u;

    TESTING("H5Fflush")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_FLUSH_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_FLUSH_FILENAME);
        goto error;
    }

    /* Create multiple small datasets in file */
    if ((dspace_id = H5Screate(H5S_SCALAR)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create data space\n");
        goto error;
    }

    for(u = 0; u < 10; u++) {
        HDsprintf(dset_name, "Dataset %u", u);

        if ((dset_id = H5Dcreate2(file_id, dset_name, H5T_STD_U32LE, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create data set: %s\n", dset_name);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
    }

    if (H5Fflush(file_id, H5F_SCOPE_LOCAL) < 0) {
        H5_FAILED();
        HDprintf("    unable to flush file with scope H5F_SCOPE_LOCAL\n");
        goto error;
    }

    if (H5Fflush(file_id, H5F_SCOPE_GLOBAL) < 0) {
        H5_FAILED();
        HDprintf("    unable to flush file with scope H5F_SCOPE_GLOBAL\n");
        goto error;
    }

    if (H5Sclose(dspace_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Sclose(dspace_id);
        H5Dclose(dset_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Fis_accessible.
 */
static int
test_file_is_accessible(void)
{
    htri_t   is_accessible;
    hid_t    fapl_id = H5I_INVALID_HID;

    TESTING("H5Fis_accessible")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((is_accessible = H5Fis_accessible(vol_test_filename, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    file '%s' is not accessible with VOL connector\n", vol_test_filename);
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
 * A test to check that a VOL connector stores and can return a valid copy
 * of a FAPL and FCPL used upon file access and creation time, respectively.
 */
static int
test_file_property_lists(void)
{
    hsize_t prop_val = 0;
    hid_t   file_id1 = H5I_INVALID_HID, file_id2 = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   fcpl_id1 = H5I_INVALID_HID, fcpl_id2 = H5I_INVALID_HID;
    hid_t   fapl_id1 = H5I_INVALID_HID, fapl_id2 = H5I_INVALID_HID;
    char    test_filename1[VOL_TEST_FILENAME_MAX_LENGTH], test_filename2[VOL_TEST_FILENAME_MAX_LENGTH];

    TESTING("file property list operations")

    HDsnprintf(test_filename1, VOL_TEST_FILENAME_MAX_LENGTH, "%s", FILE_PROPERTY_LIST_TEST_FNAME1);
    HDsnprintf(test_filename2, VOL_TEST_FILENAME_MAX_LENGTH, "%s", FILE_PROPERTY_LIST_TEST_FNAME2);

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((fcpl_id1 = H5Pcreate(H5P_FILE_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create FCPL\n");
        goto error;
    }

    if (H5Pset_userblock(fcpl_id1, FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL) < 0) {
        H5_FAILED();
        HDprintf("    failed to set test property on FCPL\n");
        goto error;
    }

    if ((file_id1 = H5Fcreate(test_filename1, H5F_ACC_TRUNC, fcpl_id1, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file\n");
        goto error;
    }

    if ((file_id2 = H5Fcreate(test_filename2, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file\n");
        goto error;
    }

    if (H5Pclose(fcpl_id1) < 0)
        TEST_ERROR

    /* Try to receive copies of the two property lists, one which has the property set and one which does not */
    if ((fcpl_id1 = H5Fget_create_plist(file_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FCPL\n");
        goto error;
    }

    if ((fcpl_id2 = H5Fget_create_plist(file_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FCPL\n");
        goto error;
    }

    /* Ensure that property list 1 has the property set and property list 2 does not */
    if (H5Pget_userblock(fcpl_id1, &prop_val) < 0) {
        H5_FAILED();
        HDprintf("    failed to retrieve test property from FCPL\n");
        goto error;
    }

    if (prop_val != FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL) {
        H5_FAILED();
        HDprintf("    retrieved test property value '%llu' did not match expected value '%llu'\n",
                (long long unsigned) prop_val, (long long unsigned) FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL);
        goto error;
    }

    if (H5Pget_userblock(fcpl_id2, &prop_val) < 0) {
        H5_FAILED();
        HDprintf("    failed to retrieve test property from FCPL\n");
        goto error;
    }

    if (prop_val == FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL) {
        HDprintf("    retrieved test property value '%llu' matched control value '%llu' when it shouldn't have\n",
                        (long long unsigned) prop_val, (long long unsigned) FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL);
        goto error;
    }

    if (H5Pclose(fcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(fcpl_id2) < 0)
        TEST_ERROR


    /* Due to the nature of needing to supply a FAPL with the VOL connector having been set on it to the H5Fcreate()
     * call, we cannot exactly test using H5P_DEFAULT as the FAPL for one of the create calls in this test. However,
     * the use of H5Fget_access_plist() will still be used to check that the FAPL is correct after both creating and
     * opening a file.
     */
    if ((fapl_id1 = H5Fget_access_plist(file_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FAPL\n");
        goto error;
    }

    if ((fapl_id2 = H5Fget_access_plist(file_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FAPL\n");
        goto error;
    }

    if (H5Pclose(fapl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id2) < 0)
        TEST_ERROR

    /* Now see if we can still retrieve copies of the property lists upon opening
     * (instead of creating) a file. If they were reconstructed properly upon file
     * open, the creation property lists should also have the same test values
     * as set before.
     */
    if (H5Fclose(file_id1) < 0)
        TEST_ERROR
    if (H5Fclose(file_id2) < 0)
        TEST_ERROR

    if ((file_id1 = H5Fopen(test_filename1, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((file_id2 = H5Fopen(test_filename2, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((fcpl_id1 = H5Fget_create_plist(file_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FCPL\n");
        goto error;
    }

    if ((fcpl_id2 = H5Fget_create_plist(file_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FCPL\n");
        goto error;
    }

    /* Check the values of the test property */
    if (H5Pget_userblock(fcpl_id1, &prop_val) < 0) {
        H5_FAILED();
        HDprintf("    failed to retrieve test property from FCPL\n");
        goto error;
    }

    if (prop_val != FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL) {
        H5_FAILED();
        HDprintf("    retrieved test property value '%llu' did not match expected value '%llu'\n",
                (long long unsigned) prop_val, (long long unsigned) FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL);
        goto error;
    }

    if (H5Pget_userblock(fcpl_id2, &prop_val) < 0) {
        H5_FAILED();
        HDprintf("    failed to retrieve test property from FCPL\n");
        goto error;
    }

    if (prop_val == FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL) {
        HDprintf("    retrieved test property value '%llu' matched control value '%llu' when it shouldn't have\n",
                (long long unsigned) prop_val, (long long unsigned) FILE_PROPERTY_LIST_TEST_FCPL_PROP_VAL);
        goto error;
    }

    if ((fapl_id1 = H5Fget_access_plist(file_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FAPL\n");
        goto error;
    }

    if ((fapl_id2 = H5Fget_access_plist(file_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get FAPL\n");
        goto error;
    }

    /* XXX: For completeness' sake, check to make sure the VOL connector is set on each of the FAPLs */

    if (H5Pclose(fcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(fcpl_id2) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id2) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id1) < 0)
        TEST_ERROR
    if (H5Fclose(file_id2) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fcpl_id1);
        H5Pclose(fcpl_id2);
        H5Pclose(fapl_id1);
        H5Pclose(fapl_id2);
        H5Pclose(fapl_id);
        H5Fclose(file_id1);
        H5Fclose(file_id2);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that the file intent flags can be retrieved.
 */
static int
test_get_file_intent(void)
{
    unsigned file_intent;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    char     test_filename[VOL_TEST_FILENAME_MAX_LENGTH];

    TESTING("retrieval of file intent with H5Fget_intent")

    HDsnprintf(test_filename, VOL_TEST_FILENAME_MAX_LENGTH, "%s", FILE_INTENT_TEST_FILENAME);

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    /* Test that file intent retrieval works correctly for file create */
    if ((file_id = H5Fcreate(test_filename, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", test_filename);
        goto error;
    }

    if (H5Fget_intent(file_id, &file_intent) < 0)
        TEST_ERROR

    if (H5F_ACC_RDWR != file_intent) {
        H5_FAILED();
        HDprintf("    received incorrect file intent for file creation\n");
        goto error;
    }

    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    /* Test that file intent retrieval works correctly for file open */
    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if (H5Fget_intent(file_id, &file_intent) < 0)
        TEST_ERROR

    if (H5F_ACC_RDONLY != file_intent) {
        H5_FAILED();
        HDprintf("    received incorrect file intent for read-only file open\n");
        goto error;
    }

    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if (H5Fget_intent(file_id, &file_intent) < 0)
        TEST_ERROR

    if (H5F_ACC_RDWR != file_intent) {
        H5_FAILED();
        HDprintf("    received incorrect file intent\n");
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
 * A test to check that the number of open objects and IDs of objects in a file
 * can be retrieved.
 */
static int
test_get_file_obj_count(void)
{
    ssize_t obj_count;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID, object_id = H5I_INVALID_HID;

    TESTING("retrieval of open object number and IDs")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(GET_OBJ_COUNT_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", GET_OBJ_COUNT_FILENAME); 
        goto error;
    }

    if ((group_id = H5Gcreate2(file_id, GRP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GRP_NAME);
        goto error;
    }

    /* Get the number of all open objects */
    if ((obj_count = H5Fget_obj_count(file_id, H5F_OBJ_ALL)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve number of objects in file '%s'\n", vol_test_filename);
        goto error;
    }

    /* One for the file and another for the group */
    if (obj_count != 2) {
        H5_FAILED();
        HDprintf("    1. incorrect object count: obj_count=%ld\n", obj_count);
        goto error;
    }

    /* Get the number of groups */
    if ((obj_count = H5Fget_obj_count(file_id, H5F_OBJ_GROUP)) < 0 || obj_count != 1) {
        H5_FAILED();
        HDprintf("    couldn't get the number of group: obj_count=%ld\n", obj_count);
        goto error;
    }

    if (H5Fget_obj_ids(file_id, H5F_OBJ_GROUP, (size_t)obj_count, &object_id) < 0 || object_id != group_id) {
        H5_FAILED();
        HDprintf("    couldn't get the group ID: object_id=%lld\n", object_id);
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
 * A test to check that file mounting and unmounting works
 * correctly.
 */
static int
test_file_mounts(void)
{
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    child_fid = H5I_INVALID_HID, group_id = H5I_INVALID_HID;

    TESTING("file mounting/unmounting")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_MOUNT_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(file_id, GRP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GRP_NAME);
        goto error;
    }

    if ((child_fid = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Mount one file (child_fid) to the group of another file (file_id) */
    if (H5Fmount(file_id, GRP_NAME, child_fid, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't mount file\n");
        goto error;
    }

    if (H5Funmount(file_id, GRP_NAME) < 0) {
        H5_FAILED();
        HDprintf("    couldn't mount file\n");
        goto error;
    }

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR
    if (H5Fclose(child_fid) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
        H5Fclose(child_fid);
    } H5E_END_TRY;

    return 1;

}

/*
 * A test to ensure that a file's name can be retrieved.
 */
static int
test_get_file_name(void)
{
    ssize_t  file_name_buf_len = 0;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    char    *file_name_buf = NULL;

    TESTING("retrieval of file name")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    /* Retrieve the size of the file name */
    if ((file_name_buf_len = H5Fget_name(file_id, NULL, 0)) < 0)
        TEST_ERROR

    /* Allocate buffer for file name */
    if (NULL == (file_name_buf = (char *) HDmalloc((size_t) file_name_buf_len + 1)))
        TEST_ERROR

    /* Retrieve the actual file name */
    if (H5Fget_name(file_id, file_name_buf, (size_t) file_name_buf_len + 1) < 0)
        TEST_ERROR

    if (HDstrncmp(file_name_buf, vol_test_filename, (size_t) file_name_buf_len)) {
        H5_FAILED();
        HDprintf("    file name '%s' didn't match expected name '%s'\n", file_name_buf, vol_test_filename);
        goto error;
    }

    if (file_name_buf) {
        HDfree(file_name_buf);
        file_name_buf = NULL;
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        if (file_name_buf)
            HDfree(file_name_buf);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

#if 0 /* for native VOL connector test only */
/*
 * Tests that H5Pget/set_file_space_strategy() and H5Pget/set_file_space_page_size()
 * work correctly.
 */
static int
test_filespace_info(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t fcpl_id = H5I_INVALID_HID, fcpl_id2 = H5I_INVALID_HID;
    hsize_t threshold = 1, threshold_out;
    H5F_fspace_strategy_t strategy;     /* File space strategy */
    hbool_t persist;                    /* Persist free-space or not */
    hsize_t fsp_size;                   /* File space page size */

    TESTING("retrieval of filespace info")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((fcpl_id = H5Pcreate(H5P_FILE_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't a file creation property list\n");
        goto error;
    }

    /* Set file space information */
    if (H5Pset_file_space_strategy(fcpl_id, H5F_FSPACE_STRATEGY_FSM_AGGR, TRUE, threshold) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file space strategy\n");
        goto error;
    }

    if (H5Pset_file_space_page_size(fcpl_id, FSP_SIZE512) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file space page size\n");
        goto error;
    }

    if ((file_id = H5Fcreate(FILESPACE_INFO_FILENAME, H5F_ACC_TRUNC, fcpl_id, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_CREATE_TEST_FILENAME);
        goto error;
    }

    if ((fcpl_id2 = H5Fget_create_plist(file_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get file creation property list\n");
        goto error;
    }

    if (H5Pget_file_space_strategy(fcpl_id2, &strategy, &persist, &threshold_out) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get file space strategy\n");
        goto error;
    }

    if (strategy != H5F_FSPACE_STRATEGY_FSM_AGGR || persist != TRUE || threshold_out != threshold) {
        H5_FAILED();
        HDprintf("    wrong file space strategy\n");
        goto error;
    }

    if (H5Pget_file_space_page_size(fcpl_id2, &fsp_size) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get file space page size\n");
        goto error;
    }

    if (fsp_size != FSP_SIZE512) {
        H5_FAILED();
        HDprintf("    wrong file space page size\n");
        goto error;
    }

    if (H5Pclose(fcpl_id) < 0)
        TEST_ERROR
    if (H5Pclose(fcpl_id2) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fcpl_id);
        H5Pclose(fcpl_id2);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check H5Iget_file_id()
 */
static int
test_get_file_id(void)
{
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   new_fid = H5I_INVALID_HID, group_id = H5I_INVALID_HID;

    TESTING("retrieval of file ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_GET_ID_TEST_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(file_id, GRP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GRP_NAME);
        goto error;
    }

    /* Return a duplicated file ID and verify it.  Need to close the new file ID. */
    if ((new_fid = H5Iget_file_id(group_id)) < 0 || new_fid != file_id) {
        H5_FAILED();
        HDprintf("    couldn't get new ID for the file '%s'\n", vol_test_filename);
        goto error;
    }

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR
    if (H5Fclose(new_fid) < 0)
        TEST_ERROR

    /* Open the file again to test H5Iget_file_id() */
    if ((file_id = H5Fopen(FILE_GET_ID_TEST_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((group_id = H5Gopen2(file_id, GRP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group '%s'\n", GRP_NAME);
        goto error;
    }

    /* Return a duplicated file ID and verify it.  Need to close the new file ID. */
    if ((new_fid = H5Iget_file_id(group_id)) < 0 || new_fid != file_id) {
        H5_FAILED();
        HDprintf("    couldn't get new ID for the file '%s'\n", vol_test_filename);
        goto error;
    }

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR
    if (H5Fclose(new_fid) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(new_fid);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check the file close degree
 */
static int
test_file_close_degree(void)
{
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    file_id2 = H5I_INVALID_HID, group_id = H5I_INVALID_HID;

    TESTING("file close degree")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_CLOSE_DEGREE_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    if (H5Pset_fclose_degree(fapl_id, H5F_CLOSE_STRONG) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file close degree\n");
        goto error;
    }

    /* Trying to open the same file should fail */
    H5E_BEGIN_TRY {
        file_id2 = H5Fopen(FILE_CLOSE_DEGREE_FILENAME, H5F_ACC_RDWR, fapl_id);
    } H5E_END_TRY;

    if (file_id2 >= 0) {
        H5_FAILED();
        HDprintf("    file was opened with a wrong close degree!\n");
        goto error;
    }

    /* Allow objects in file remain open after file is closed */
    if (H5Pset_fclose_degree(fapl_id, H5F_CLOSE_WEAK) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file close degree\n");
        goto error;
    }

    /* Trying to open the same file should succeed */
    if ((file_id2 = H5Fopen(FILE_CLOSE_DEGREE_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    /* Close the second opening of the file */
    if (H5Fclose(file_id2) < 0)
        TEST_ERROR

    /* Create a group in the file */
    if ((group_id = H5Gcreate2(file_id, GRP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GRP_NAME);
        goto error;
    }

    /* Close the file */
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    /* Verify the group ID is still valid */
    if (H5Iis_valid(group_id) <= 0) {
        H5_FAILED();
        HDprintf("    group ID is not valid anymore\n");
        goto error;
    }

    /* Verify the ID is a group */
    if (H5Iget_type(group_id) != H5I_GROUP) {
        H5_FAILED();
        HDprintf("    ID is not a group\n");
        goto error;
    }

    /* The group should still be open and can be closed now */
    if (H5Gclose(group_id) < 0)
        TEST_ERROR

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
        H5Fclose(file_id2);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check H5Fget_free_sections and H5Fget_freespace
 */
static int
test_get_file_free_sections(void)
{
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    dcpl_id = H5I_INVALID_HID, dset_id = H5I_INVALID_HID;
    hid_t    fcpl_id = H5I_INVALID_HID, dspace_id = H5I_INVALID_HID;
    hsize_t  dims[1];
    hssize_t free_space = 0;
    ssize_t  numb_sections = 0;
    H5F_sect_info_t *section_info = NULL;  /* buffer to hold free-space information for all types of data */
    hsize_t  total = 0;                    /* sum of the free-space section sizes */
    char     dset_name[32];
    unsigned u;

    TESTING("retrieval of file free sections and free space")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    /* Create file-creation template */
    if ((fcpl_id = H5Pcreate(H5P_FILE_CREATE)) < 0)
        TEST_ERROR

    if (H5Pset_libver_bounds(fapl_id, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) < 0)
        TEST_ERROR

    if (H5Pset_file_space_strategy(fcpl_id, H5F_FSPACE_STRATEGY_PAGE, TRUE, (hsize_t)1) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(GET_FREE_SECTIONS_FILENAME, H5F_ACC_TRUNC, fcpl_id, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset creation property list\n");
        goto error;
    }

    if (H5Pset_alloc_time(dcpl_id, H5D_ALLOC_TIME_EARLY) < 0) {
        H5_FAILED();
        HDprintf("    couldn't allocation time property\n");
        goto error;
    }

    /* Create a large dataset */
    dims[0] = 1200;

    if ((dspace_id = H5Screate_simple(1, dims, NULL)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create data space\n");
        goto error;
    }

    if ((dset_id = H5Dcreate2(file_id, DSET_NAME, H5T_STD_U32LE, dspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create data set: %s\n", DSET_NAME);
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if (H5Sclose(dspace_id) < 0)
        TEST_ERROR

    /* Create multiple small datasets in file */
    if ((dspace_id = H5Screate(H5S_SCALAR)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create data space\n");
        goto error;
    }

    for(u = 0; u < 10; u++) {
        HDsprintf(dset_name, "Dataset %u", u);

        if ((dset_id = H5Dcreate2(file_id, dset_name, H5T_STD_U32LE, dspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create data set: %s\n", dset_name);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
    }

    if (H5Pclose(dcpl_id) < 0)
        TEST_ERROR

    if (H5Sclose(dspace_id) < 0)
        TEST_ERROR

    /* Delete odd-numbered datasets in file */
    for(u = 0; u < 10; u = u + 2) {
        HDsprintf(dset_name, "Dataset %u", u);

        if (H5Ldelete(file_id, dset_name, H5P_DEFAULT) < 0) {
            H5_FAILED();
            HDprintf("    couldn't delete data set: %s\n", dset_name);
            goto error;
        }
    }

    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(GET_FREE_SECTIONS_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    if ((free_space = H5Fget_freespace(file_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get free space\n");
        goto error;
    }

    if ((numb_sections = H5Fget_free_sections(file_id, H5FD_MEM_DEFAULT, (size_t)0, NULL)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get the number of free sections\n");
        goto error;
    }

    section_info = (H5F_sect_info_t *)HDmalloc(numb_sections*sizeof(H5F_sect_info_t));

    if (H5Fget_free_sections(file_id, H5FD_MEM_DEFAULT, (size_t)numb_sections, section_info) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get free sections info\n");
        goto error;
    }

    /* Verify the amount of free-space is correct */
    for(u = 0; u < numb_sections; u++)
        total += section_info[u].size;

    if (free_space != total) {
        H5_FAILED();
        HDprintf("    wrong sum of free section sizes\n");
        goto error;
    }

    if (H5Pclose(fcpl_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if(section_info) {
        HDfree(section_info);
        section_info = NULL;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        if(section_info)
            HDfree(section_info);

        H5Sclose(dset_id);
        H5Sclose(dspace_id);
        H5Pclose(dcpl_id);
        H5Pclose(fcpl_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Fget_filesize.
 */
static int
test_get_file_size(void)
{
    hsize_t file_size;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("retrieval of file size")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(FILE_SIZE_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_CREATE_TEST_FILENAME);
        goto error;
    }

    if (H5Fget_filesize(file_id, &file_size) < 0) {
        H5_FAILED();
        HDprintf("    unable to get file size\n");
        goto error;
    }

    /* There is no garantee the size of metadata in file is constant.
     * Just try to check if it's reasonable.
     *
     * Currently it should be around 2 KB.
     */
    if(file_size < 1 * KB || file_size > 4 * KB) {
        H5_FAILED();
        HDprintf("    wrong file size\n");
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
 * A test for H5Fget_file_image.
 */
static int
test_get_file_image(void)
{
    ssize_t file_image_size;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    void    *buf = NULL;

    TESTING("retrieval of file image")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    /* Get the image size first */
    if ((file_image_size = H5Fget_file_image(file_id, NULL, 0)) < 0) {
        H5_FAILED();
        HDprintf("    unable to get file image size\n");
        goto error;
    }

    if (file_image_size > 0) {
        buf = HDmalloc(file_image_size);

        if (H5Fget_file_image(file_id, buf, (size_t)file_image_size) < 0) {
            H5_FAILED();
            HDprintf("    unable to get file image\n");
            goto error;
        }

        if (buf) {
            HDfree(buf);
            buf = NULL;
        }
    }

    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        if (buf)
            HDfree(buf);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Fget_info.
 */
static int
test_get_file_info(void)
{
    H5F_info2_t file_info;
    hid_t       file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t       fcpl_id = H5I_INVALID_HID;
    hsize_t     threshold = 1;

    TESTING("retrieval of file info with H5Fget_info")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((fcpl_id = H5Pcreate(H5P_FILE_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't a file creation property list\n");
        goto error;
    }

    /* Set a property in the FCPL that will push the superblock version up */
    if (H5Pset_file_space_strategy(fcpl_id, H5F_FSPACE_STRATEGY_FSM_AGGR, TRUE, threshold) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file space strategy\n");
        goto error;
    }

    if (H5Pset_file_space_page_size(fcpl_id, FSP_SIZE512) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file space page size\n");
        goto error;
    }

    if (H5Pset_alignment(fapl_id, (hsize_t)1, (hsize_t)1024) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set file space alignment\n");
        goto error;
    }

    /* Creating a file with the non-default file creation property list should
     * create a version 2 superblock
     */
    if ((file_id = H5Fcreate(FILE_INFO_FILENAME, H5F_ACC_TRUNC, fcpl_id, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_CREATE_TEST_FILENAME);
        goto error;
    }

    /* Get and verify the file's version information */
    if (H5Fget_info2(file_id, &file_info) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get file info\n");
        goto error;
    }

    if (file_info.super.super_ext_size != 152 || file_info.sohm.hdr_size != 0 ||
        file_info.sohm.msgs_info.index_size != 0 || file_info.sohm.msgs_info.heap_size != 0) {
        H5_FAILED();
        HDprintf("    wrong file info\n");
        goto error;
    }


    if (H5Pclose(fcpl_id) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Pclose(fcpl_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that the VFD handle can be retrieved using
 * the native VOL connector.
 */
static int
test_get_file_vfd_handle(void)
{
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    void     *vfd_handle = NULL;

    TESTING("retrieval of VFD handle")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if (H5Fget_vfd_handle(file_id, fapl_id, &vfd_handle) < 0 || !vfd_handle) {
        H5_FAILED();
        HDprintf("    unable to get VFD handle\n");
        goto error;
    }

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
 * A test to check whether opening the same group from
 * two different files works correctly.
 */
static int
test_double_group_open(void)
{
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   file_id2 = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   group_id2 = H5I_INVALID_HID;

    TESTING("double group open")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fcreate(DOUBLE_GROUP_OPEN_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    /* Open the file for the second time for read-only */
    if ((file_id2 = H5Fopen(DOUBLE_GROUP_OPEN_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", FILE_GET_ID_TEST_FILENAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(file_id, GRP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", GRP_NAME);
        goto error;
    }

    /* Open the group in the second file open */
    if ((group_id2 = H5Gopen2(file_id2, GRP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open the group '%s'\n", GRP_NAME);
        goto error;
    }

    if (H5Gclose(group_id) < 0)
        TEST_ERROR
    if (H5Gclose(group_id2) < 0)
        TEST_ERROR
    if (H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR
    if (H5Fclose(file_id2) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(group_id);
        H5Gclose(group_id2);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
        H5Fclose(file_id2);
    } H5E_END_TRY;

    return 1;
}
#endif /* for native VOL connector test only */

/*
 * Cleanup temporary test files
 */
static void
cleanup_files(void)
{
    HDremove(FILE_CREATE_TEST_FILENAME);
    HDremove(FILE_CREATE_EXCL_FILE_NAME);
    HDremove(FILE_CREATE_INVALID_PARAMS_FILE_NAME);
    HDremove(FILE_PERMISSION_FILENAME);
    HDremove(FILE_FLUSH_FILENAME);
    HDremove(FILE_PROPERTY_LIST_TEST_FNAME1);
    HDremove(FILE_PROPERTY_LIST_TEST_FNAME2);
    HDremove(FILE_INTENT_TEST_FILENAME);
    HDremove(GET_OBJ_COUNT_FILENAME);
    HDremove(FILE_MOUNT_FILENAME);
#if 0 /* for native VOL connector test only */
    HDremove(FILESPACE_INFO_FILENAME);
    HDremove(FILE_GET_ID_TEST_FILENAME);
    HDremove(FILE_CLOSE_DEGREE_FILENAME);
    HDremove(GET_FREE_SECTIONS_FILENAME);
    HDremove(FILE_SIZE_FILENAME);
    HDremove(FILE_INFO_FILENAME);
    HDremove(DOUBLE_GROUP_OPEN_FILENAME);
#endif /* for native VOL connector test only */
}

int
vol_file_test(void)
{
    size_t i;
    int    nerrors;

    HDprintf("**********************************************\n");
    HDprintf("*                                            *\n");
    HDprintf("*               VOL File Tests               *\n");
    HDprintf("*                                            *\n");
    HDprintf("**********************************************\n\n");

    for (i = 0, nerrors = 0; i < ARRAY_LENGTH(file_tests); i++) {
        nerrors += (*file_tests[i])() ? 1 : 0;
    }

    HDprintf("\n");

    cleanup_files();

done:
    return nerrors;
}
