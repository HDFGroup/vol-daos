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

#include "vol_misc_test.h"

static int test_open_link_without_leading_slash(void);
static int test_object_creation_by_absolute_path(void);
static int test_absolute_vs_relative_path(void);
static int test_symbols_in_compound_field_name(void);
static int test_double_init_term(void);

/*
 * The array of miscellaneous tests to be performed.
 */
static int (*misc_tests[])(void) = {
        test_open_link_without_leading_slash,
        test_object_creation_by_absolute_path,
        test_absolute_vs_relative_path,
        test_symbols_in_compound_field_name,
        test_double_init_term,
};

static int
test_open_link_without_leading_slash(void)
{
    hsize_t dims[OPEN_LINK_WITHOUT_SLASH_DSET_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("opening a link without a leading slash")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, MISCELLANEOUS_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    for (i = 0; i < OPEN_LINK_WITHOUT_SLASH_DSET_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(OPEN_LINK_WITHOUT_SLASH_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(container_group, OPEN_LINK_WITHOUT_SLASH_DSET_NAME, dset_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Gclose(container_group) < 0)
        TEST_ERROR
    if (H5Fclose(file_id) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((group_id = H5Gopen2(file_id, "/", H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open root group\n");
        goto error;
    }

    if ((dset_id = H5Dopen2(group_id, MISCELLANEOUS_TEST_GROUP_NAME "/" OPEN_LINK_WITHOUT_SLASH_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset\n");
        goto error;
    }

    if ((space_id = H5Dget_space(dset_id)) < 0)
        TEST_ERROR

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
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
        H5Sclose(space_id);
        H5Tclose(dset_dtype);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

static int
test_object_creation_by_absolute_path(void)
{
    hsize_t dims[OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_SPACE_RANK];
    htri_t  link_exists;
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID, sub_group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;
    hid_t   dtype_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;

    TESTING("object creation by absolute path")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, MISCELLANEOUS_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    /* Start by creating a group to hold all the objects for this test */
    if ((group_id = H5Gcreate2(container_group, OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group\n");
        goto error;
    }

    /* Next try to create a group under the container group by using an absolute pathname */
    if ((sub_group_id = H5Gcreate2(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create subgroup by absolute pathname\n");
        goto error;
    }

    /* Next try to create a dataset nested at the end of this group chain by using an absolute pathname */
    for (i = 0; i < OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_SPACE_RANK, dims, NULL)) <0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_NAME,
            dset_dtype, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    /* Next try to create a committed datatype in the same fashion as the preceding dataset */
    if ((dtype_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if (H5Tcommit2(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DTYPE_NAME,
            dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype\n");
        goto error;
    }

    /* Finally try to verify that all of the previously-created objects exist in the correct location */
    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    container group didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    subgroup didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    dataset didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME "/" OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DTYPE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    datatype didn't exist at the correct location\n");
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Tclose(dtype_id) < 0)
        TEST_ERROR
    if (H5Gclose(sub_group_id) < 0)
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
        H5Sclose(fspace_id);
        H5Tclose(dset_dtype);
        H5Dclose(dset_id);
        H5Tclose(dtype_id);
        H5Gclose(sub_group_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

static int
test_absolute_vs_relative_path(void)
{
    hsize_t dims[ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET_SPACE_RANK];
    htri_t  link_exists;
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id1 = H5I_INVALID_HID, dset_id2 = H5I_INVALID_HID, dset_id3 = H5I_INVALID_HID, dset_id4 = H5I_INVALID_HID, dset_id5 = H5I_INVALID_HID, dset_id6 = H5I_INVALID_HID;
    hid_t   dset_dtype1 = H5I_INVALID_HID, dset_dtype2 = H5I_INVALID_HID, dset_dtype3 = H5I_INVALID_HID, dset_dtype4 = H5I_INVALID_HID, dset_dtype5 = H5I_INVALID_HID, dset_dtype6 = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("absolute vs. relative pathnames")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, MISCELLANEOUS_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    /* Start by creating a group to be used during some of the dataset creation operations */
    if ((group_id = H5Gcreate2(container_group, ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group\n");
        goto error;
    }

    for (i = 0; i < ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype1 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype2 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype3 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype4 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype5 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype6 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    /* Create a dataset by absolute path in the form "/group/dataset" starting from the root group */
    if ((dset_id1 = H5Dcreate2(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET1_NAME,
            dset_dtype1, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset by absolute path from root\n");
        goto error;
    }

    /* Create a dataset by relative path in the form "group/dataset" starting from the container group */
    if ((dset_id2 = H5Dcreate2(container_group, ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET2_NAME,
            dset_dtype2, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset by relative path from root\n");
        goto error;
    }

    /* Create a dataset by relative path in the form "./group/dataset" starting from the root group */
    if ((dset_id3 = H5Dcreate2(file_id, "./" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET3_NAME,
            dset_dtype3, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset by relative path from root with leading '.'\n");
        goto error;
    }

    /* Create a dataset by absolute path in the form "/group/dataset" starting from the container group */
    if ((dset_id4 = H5Dcreate2(container_group, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET4_NAME,
            dset_dtype4, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset by absolute path from container group\n");
        goto error;
    }

    /* Create a dataset by relative path in the form "dataset" starting from the container group */
    if ((dset_id5 = H5Dcreate2(group_id, ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET5_NAME, dset_dtype5,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset by relative path from container group\n");
        goto error;
    }

    /* Create a dataset by relative path in the form "./dataset" starting from the container group */
    if ((dset_id6 = H5Dcreate2(group_id, "./" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET6_NAME, dset_dtype6,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset by relative path from container group with leading '.'\n");
        goto error;
    }

    /* Verify that all of the previously-created datasets exist in the correct locations */
    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET1_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET2_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET3_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET4_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET5_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    didn't exist at the correct location\n");
        goto error;
    }

    if ((link_exists = H5Lexists(file_id, "/" MISCELLANEOUS_TEST_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "/" ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET6_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if link exists\n");
        goto error;
    }

    if (!link_exists) {
        H5_FAILED();
        HDprintf("    didn't exist at the correct location\n");
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype1) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype2) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype3) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype4) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype5) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype6) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id1) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id3) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id4) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id5) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id6) < 0)
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
        H5Sclose(fspace_id);
        H5Tclose(dset_dtype1);
        H5Tclose(dset_dtype2);
        H5Tclose(dset_dtype3);
        H5Tclose(dset_dtype4);
        H5Tclose(dset_dtype5);
        H5Tclose(dset_dtype6);
        H5Dclose(dset_id1);
        H5Dclose(dset_id2);
        H5Dclose(dset_id3);
        H5Dclose(dset_id4);
        H5Dclose(dset_id5);
        H5Dclose(dset_id6);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that the initialization and termination
 * functions of a VOL connector can be called multiple times
 * in a row.
 *
 * XXX: Not sure if this test can be done from public APIs
 * at the moment.
 */
static int
test_double_init_term(void)
{
    hid_t fapl_id = H5I_INVALID_HID;

    TESTING("double init/term correctness")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

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

static int
test_symbols_in_compound_field_name(void)
{
    hsize_t  dims[COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_RANK];
    size_t   i;
    size_t   total_type_size;
    size_t   next_offset;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    compound_type = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    hid_t    type_pool[COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES];
    char     member_names[COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES][256];

    TESTING("usage of '{', '}' and '\\\"' symbols in compound type\'s field name")

    for (i = 0; i < COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES; i++)
        type_pool[i] = H5I_INVALID_HID;

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, MISCELLANEOUS_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_SUBGROUP_NAME,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group\n");
        goto error;
    }

    for (i = 0, total_type_size = 0; i < COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES; i++) {
        type_pool[i] = generate_random_datatype(H5T_NO_CLASS);
        total_type_size += H5Tget_size(type_pool[i]);
    }

    HDsnprintf(member_names[0], 256, "{{{ member0");
    HDsnprintf(member_names[1], 256, "member1 }}}");
    HDsnprintf(member_names[2], 256, "{{{ member2 }}");
    HDsnprintf(member_names[3], 256, "{{ member3 }}}");
    HDsnprintf(member_names[4], 256, "\\\"member4");
    HDsnprintf(member_names[5], 256, "member5\\\"");
    HDsnprintf(member_names[6], 256, "mem\\\"ber6");
    HDsnprintf(member_names[7], 256, "{{ member7\\\" }");
    HDsnprintf(member_names[8], 256, "{{ member8\\\\");

    if ((compound_type = H5Tcreate(H5T_COMPOUND, total_type_size)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create compound datatype\n");
        goto error;
    }

    for (i = 0, next_offset = 0; i < COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES; i++) {
        if (H5Tinsert(compound_type, member_names[i], next_offset, type_pool[i]) < 0) {
            H5_FAILED();
            HDprintf("    couldn't insert compound member %zu\n", i);
            goto error;
        }

        next_offset += H5Tget_size(type_pool[i]);
    }

    if (H5Tpack(compound_type) < 0)
        TEST_ERROR

    for (i = 0; i < COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_NAME, compound_type,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset\n");
        goto error;
    }

    for (i = 0; i < COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES; i++)
        if (type_pool[i] >= 0 && H5Tclose(type_pool[i]) < 0)
            TEST_ERROR

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(compound_type) < 0)
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
        for (i = 0; i < COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES; i++)
            H5Tclose(type_pool[i]);
        H5Sclose(fspace_id);
        H5Tclose(compound_type);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

int
vol_misc_test(void)
{
    size_t i;
    int    nerrors;

    HDprintf("**********************************************\n");
    HDprintf("*                                            *\n");
    HDprintf("*          VOL Miscellaneous Tests           *\n");
    HDprintf("*                                            *\n");
    HDprintf("**********************************************\n\n");

    for (i = 0, nerrors = 0; i < ARRAY_LENGTH(misc_tests); i++) {
        nerrors += (*misc_tests[i])() ? 1 : 0;
    }

    HDprintf("\n");

done:
    return nerrors;
}
